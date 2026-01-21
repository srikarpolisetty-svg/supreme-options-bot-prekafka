# ibkr_execution_engine.py
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as ecals
from ib_insync import IB, Contract, MarketOrder, StopOrder, Order
from datetime import date, timedelta

def third_friday_of_month(d: date) -> date:
    """
    Return the 3rd Friday of the month for date d.
    """
    first = d.replace(day=1)
    days_to_friday = (4 - first.weekday()) % 7  # Friday = 4
    first_friday = first + timedelta(days=days_to_friday)
    return first_friday + timedelta(days=14)


def is_third_friday_week(d: date) -> tuple[bool, date]:
    """
    Returns (True, third_friday_date) if d is in the Monday–Sunday
    week that contains the 3rd Friday of the month.
    """
    tf = third_friday_of_month(d)
    week_start = tf - timedelta(days=tf.weekday())  # Monday
    week_end = week_start + timedelta(days=6)       # Sunday
    return week_start <= d <= week_end, tf


# =========================
# Config
# =========================
HOST = "127.0.0.1"
PORT = 4002
DB_PATH = "options_data.db"

# Global kill switch (entries + exits)
EXECUTE_TRADES_DEFAULT = True

# Optional: allow exits even when kill switch is off (recommended safety behavior)
ALLOW_EXITS_WHEN_KILLED = True


@dataclass
class RiskConfig:
    per_trade_risk_pct: float = 0.02
    per_day_risk_pct: float = 0.04
    max_open_orders: int = 5
    min_order_age_seconds: int = 15 * 60
    trail_pct: float = 0.20  # 0.20 = 20%
    trail_tif: str = "GTC"   # "DAY" or "GTC"
    entry_qty: int = 2       # default entry size


class IBKRExecutionEngine:
    """
    Execution-only engine:
      - Uses conId from DuckDB for entries (no qualification)
      - Places server-side IB trailing stops (no Python trailing loop)
      - Entries can be disabled by gates; exits/management can still run (configurable)
      - Dedupe entries per run (no repeated buys for same conId)
      - Avoids re-placing BE/Trail if open orders already exist
    """

    def __init__(
        self,
        client_id: int,
        host=HOST,
        port=PORT,
        db_path=DB_PATH,
        execute_trades_default=EXECUTE_TRADES_DEFAULT,
        allow_exits_when_killed: bool = ALLOW_EXITS_WHEN_KILLED,
        risk: RiskConfig = RiskConfig(),
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.db_path = db_path
        self.execute_trades_default = execute_trades_default
        self.allow_exits_when_killed = allow_exits_when_killed
        self.risk = risk

        self.ib = IB()
        self.NY_TZ = ZoneInfo("America/New_York")
        self.XNYS = ecals.get_calendar("XNYS")

        # Track submit times so you can enforce the 15-min gate
        self.order_submit_time: dict[int, datetime] = {}

    # -------------------------
    # Connection
    # -------------------------
    def connect(self):
        if not self.ib.isConnected():
            self.ib.connect(self.host, self.port, clientId=self.client_id)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    # -------------------------
    # Contract from conId (DB -> IB)
    # -------------------------
    def opt_contract_from_conid(self, conid: int) -> Contract:
        return Contract(conId=int(conid), secType="OPT", exchange="SMART", currency="USD")

    # -------------------------
    # Minimal mark price (ONLY for managing OPEN positions)
    # -------------------------
    def get_mark_price_snapshot(self, contract: Contract, wait_s: float = 0.8) -> float | None:
        """
        Only used for exits/management logic (PnL/return %).
        Entries do NOT request market data.
        """
        t = self.ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)
        self.ib.sleep(wait_s)

        bid = t.bid if (t.bid and t.bid > 0) else None
        ask = t.ask if (t.ask and t.ask > 0) else None
        last = t.last if (t.last and t.last > 0) else None
        close = t.close if (t.close and t.close > 0) else None

        if bid is not None and ask is not None:
            return float((bid + ask) / 2.0)
        if last is not None:
            return float(last)
        if close is not None:
            return float(close)
        return None

    # -------------------------
    # Positions / PnL (options only)
    # -------------------------
    def get_positions(self):
        return list(self.ib.positions())

    def _entry_from_position(self, p) -> float | None:
        """
        IB reports avgCost in account currency for the position.
        For options it is commonly the premium * 100 (but not always).
        We make this explicit + conservative: if avgCost looks like it's scaled, divide by 100.
        """
        if p.avgCost is None:
            return None
        try:
            ac = float(p.avgCost)
        except Exception:
            return None
        if ac <= 0:
            return None

        # Common case: options show avgCost as premium*100, so > 20 is often a tell.
        # Still heuristic, but centralized here for easier validation.
        if ac > 20.0:
            return ac / 100.0
        return ac

    def compute_unrealized_pnl_options(self) -> float:
        pnl = 0.0
        for p in self.get_positions():
            if p.contract.secType != "OPT" or p.position == 0:
                continue

            mark = self.get_mark_price_snapshot(p.contract)
            if mark is None:
                continue

            entry = self._entry_from_position(p)
            if entry is None or entry <= 0:
                continue

            pnl += (mark - entry) * float(p.position) * 100.0

        return float(pnl)

    def position_return_pct(self, p, mark: float) -> float | None:
        entry = self._entry_from_position(p)
        if entry is None or entry <= 0:
            return None
        return (mark - entry) / entry * 100.0

    # -------------------------
    # Orders
    # -------------------------
    def _track_trade(self, trade):
        try:
            oid = trade.order.orderId
            self.order_submit_time[oid] = datetime.now(timezone.utc)
        except Exception:
            pass

    def place_market(self, contract: Contract, side: str, qty: int, allow_orders: bool):
        if not allow_orders:
            return None
        trade = self.ib.placeOrder(contract, MarketOrder(side.upper(), int(qty)))
        self._track_trade(trade)
        self.ib.sleep(0.2)
        return trade

    def place_stop_close_sell(self, contract: Contract, qty: int, stop_price: float, allow_orders: bool):
        if not allow_orders:
            return None
        trade = self.ib.placeOrder(contract, StopOrder("SELL", int(qty), float(stop_price)))
        self._track_trade(trade)
        self.ib.sleep(0.2)
        return trade

    def place_trailing_stop_pct_sell(self, contract: Contract, qty: int, trailing_pct: float, allow_orders: bool):
        """
        Server-side trailing stop (SELL).
        trailing_pct: 20.0 means trail by 20% from best price after activation.
        """
        if not allow_orders:
            return None

        o = Order(
            action="SELL",
            orderType="TRAIL",
            totalQuantity=int(qty),
            trailingPercent=float(trailing_pct),
            tif=str(self.risk.trail_tif),
        )
        trade = self.ib.placeOrder(contract, o)
        self._track_trade(trade)
        self.ib.sleep(0.2)
        return trade

    # -------------------------
    # DB helpers
    # -------------------------
    def load_recent_signal_rows(self, symbol: str, limit: int = 6):
        con = duckdb.connect(self.db_path, read_only=True)
        df = con.execute(
            """
            SELECT *
            FROM option_snapshots_enriched
            WHERE symbol = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [symbol, limit],
        ).df()
        con.close()
        return df


    def _row_conid(self, row) -> int | None:
        for name in ("conId", "conid", "con_id", "option_conId", "option_conid", "contract_id", "contractId"):
            v = getattr(row, name, None)
            if v is not None:
                try:
                    return int(v)
                except Exception:
                    pass
        return None

    def _row_est_cost(self, row, qty: int) -> float | None:
        """
        Estimate total dollars for `qty` contracts from DB columns.
        Uses mid/price/last/close first, then bid/ask mid.
        """
        for name in ("mid", "price", "option_price", "last", "close"):
            v = getattr(row, name, None)
            if v is None:
                continue
            try:
                px = float(v)
                if px > 0:
                    return px * 100.0 * qty
            except Exception:
                pass

        bid = getattr(row, "bid", None)
        ask = getattr(row, "ask", None)
        try:
            bidf = float(bid) if bid is not None else None
            askf = float(ask) if ask is not None else None
            if bidf is not None and askf is not None and bidf > 0 and askf > 0:
                return ((bidf + askf) / 2.0) * 100.0 * qty
        except Exception:
            pass

        return None

    # -------------------------
    # Existing orders / dedupe
    # -------------------------
    def _open_orders_for_conid(self, conid: int):
        """
        Returns open trades whose contract matches conId and status is still working.
        """
        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        out = []
        for t in self.ib.trades():
            try:
                if t.contract and int(t.contract.conId) == int(conid) and t.orderStatus.status in working_status:
                    out.append(t)
            except Exception:
                continue
        return out

    def _has_working_trailing_sell(self, conid: int) -> bool:
        for t in self._open_orders_for_conid(conid):
            try:
                if t.order and t.order.action == "SELL" and t.order.orderType == "TRAIL":
                    return True
            except Exception:
                pass
        return False

    def _has_working_stop_sell(self, conid: int) -> bool:
        for t in self._open_orders_for_conid(conid):
            try:
                if t.order and t.order.action == "SELL" and t.order.orderType in {"STP", "STOP"}:
                    return True
            except Exception:
                pass
        return False

    def _already_long_conid(self, conid: int) -> bool:
        for p in self.get_positions():
            try:
                if p.contract.secType == "OPT" and int(p.contract.conId) == int(conid) and float(p.position) > 0:
                    return True
            except Exception:
                continue
        return False
    def cancel_all_option_orders(self):
        """
        Cancel all working orders for options (safety before liquidation).
        """
        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        for t in self.ib.trades():
            try:
                if (
                    t.contract is not None
                    and t.contract.secType == "OPT"
                    and t.orderStatus is not None
                    and t.orderStatus.status in working_status
                ):
                    self.ib.cancelOrder(t.order)
            except Exception:
                continue

        self.ib.sleep(0.2)

    def close_all_option_positions_market(self, allow_exits: bool):
        """
        Force-liquidate all OPT positions with market sells.
        """
        if not allow_exits:
            return

        for p in self.get_positions():
            try:
                if p.contract is None or p.contract.secType != "OPT":
                    continue

                qty = float(p.position)
                if qty <= 0:
                    continue

                self.place_market(p.contract, "SELL", int(qty), allow_orders=True)
            except Exception:
                continue

    def enforce_daily_loss_forced_liquidation(self, max_day_risk: float, allow_exits: bool) -> bool:
        """
        If daily unrealized loss breaches max_day_risk:
          - cancel working option orders
          - market-close all option positions
          - return False (disable new entries)
        Otherwise return True.
        """
        daily_pnl = float(self.compute_unrealized_pnl_options())

        if daily_pnl < 0 and abs(daily_pnl) >= float(max_day_risk):
            print(
                f"[EXEC][KILL] daily_unrealized_pnl={daily_pnl:.2f} breached max_day_risk={max_day_risk:.2f} -> LIQUIDATE",
                flush=True
            )

            # stop stacking conflicting orders, then liquidate
            self.cancel_all_option_orders()
            self.close_all_option_positions_market(allow_exits=allow_exits)

            return False

        return True
# =========================
# FIX 1) Robust _row_conid (works reliably with df.itertuples())
# Put this INSIDE the IBKRExecutionEngine class (replace your current _row_conid)
# =========================

    def _row_conid(self, row) -> int | None:
        """
        Robust conId extraction from a pandas itertuples() row.
        Handles attribute name mangling + None/NaN.
        """
        d = row._asdict() if hasattr(row, "_asdict") else {}

        for name in (
            "conId", "conid", "con_id",
            "option_conId", "option_conid",
            "contract_id", "contractId",
        ):
            v = d.get(name, getattr(row, name, None))
            if v is None:
                continue
            try:
                # handle NaN (float)
                import math
                if isinstance(v, float) and math.isnan(v):
                    continue
            except Exception:
                pass

            try:
                return int(v)
            except Exception:
                # strings like "123.0"
                try:
                    return int(float(str(v).strip()))
                except Exception:
                    continue

        return None
# =========================
# FIX 2) Exclude protective sells (TRAIL/STP/etc) from open-order gates
# Add this helper INSIDE the class, then update run()
# =========================

    def _is_entry_order_trade(self, t) -> bool:
        """
        True only for ENTRY orders (BUY). Excludes protective sells like TRAIL/STP.
        """
        try:
            o = getattr(t, "order", None)
            if o is None:
                return False

            if getattr(o, "action", None) != "BUY":
                return False

            # exclude trailing (and any other protective types you might use)
            if getattr(o, "orderType", None) in {"TRAIL", "STP", "STOP"}:
                return False

            return True
        except Exception:
            return False

    # -------------------------
    # Main loop
    # -------------------------
    def run(self, symbol: str):
        # market-hours gate
        today = datetime.now(self.NY_TZ).date()
        is_tf_week, tf_date = is_third_friday_week(today)

        if is_tf_week:
            print(
                f"[EXEC][SKIP] Third-Friday week detected. "
                f"third_friday={tf_date} today={today}",
                flush=True
            )
            return

        # -------------------------
        # Market hours gate
        # -------------------------
        now = datetime.now(self.NY_TZ)
        if not self.XNYS.is_open_on_minute(now, ignore_breaks=True):
            return

        self.connect()

        # Global kill switch
        allow_orders = bool(self.execute_trades_default)

        # Exits can optionally remain enabled even when kill switch is off
        allow_exits = bool(allow_orders or self.allow_exits_when_killed)
        allow_entries = bool(allow_orders)  # entries only when explicitly enabled

        # Buying power / budgets
        acct = {}
        for r in self.ib.accountSummary():
            try:
                v = r.value
                if v is None or v == "":
                    continue
                acct[r.tag] = float(str(v).replace(",", ""))
            except Exception:
                continue

        buying_power = acct.get("BuyingPower") or acct.get("AvailableFunds") or 0.0
        max_trade_risk = float(buying_power) * self.risk.per_trade_risk_pct
        max_day_risk = float(buying_power) * self.risk.per_day_risk_pct

        # -------------------------
        # Entry gates
        # -------------------------
        # -------------------------
        # Entry gates (ENTRY orders only)
        # -------------------------
        open_trades = [
            t for t in self.ib.trades()
            if getattr(getattr(t, "orderStatus", None), "status", None) in {"Submitted", "PreSubmitted", "ApiPending"}
        ]

        open_entry_trades = [t for t in open_trades if self._is_entry_order_trade(t)]

        if len(open_entry_trades) >= self.risk.max_open_orders:
            allow_entries = False

        now_utc = datetime.now(timezone.utc)
        for t in open_entry_trades:
            try:
                oid = t.order.orderId
            except Exception:
                continue
            ts = self.order_submit_time.get(oid)
            if ts and (now_utc - ts).total_seconds() <= self.risk.min_order_age_seconds:
                allow_entries = False
                break


        # Daily loss gate: stops NEW entries, but still allows exits/management.
        # Daily loss kill-switch: FORCE liquidation + disable NEW entries
        allow_entries = bool(allow_entries) and self.enforce_daily_loss_forced_liquidation(
            max_day_risk=max_day_risk,
            allow_exits=allow_exits
        )


        # -------------------------
        # Entries from DB signals (NO market data)
        # -------------------------
        df = self.load_recent_signal_rows(symbol)
        signal_cols = [
            "atm_call_signal", "atm_put_signal",
            "otm1_call_signal", "otm1_put_signal",
            "otm2_call_signal", "otm2_put_signal",
        ]

        seen_conids: set[int] = set()

        for row in df.itertuples():
            if not any(bool(getattr(row, c, False)) for c in signal_cols):
                continue

            conid = self._row_conid(row)
            if conid is None:
                continue

            # Dedupe per run (avoid multiple buys from multiple recent rows)
            if conid in seen_conids:
                continue
            seen_conids.add(conid)

            # Don't re-enter if already long this exact contract
            if self._already_long_conid(conid):
                continue

            contract = self.opt_contract_from_conid(conid)

            qty = int(self.risk.entry_qty)
            est_cost = self._row_est_cost(row, qty=qty)
            if est_cost is None:
                continue

            if allow_entries and est_cost <= max_trade_risk:
                tr = self.place_market(contract, "BUY", qty, allow_orders=allow_entries)
                if tr is not None:
                    # Attach trailing stop if not already working
                    if not self._has_working_trailing_sell(conid):
                        self.place_trailing_stop_pct_sell(
                            contract=contract,
                            qty=qty,
                            trailing_pct=self.risk.trail_pct * 100.0,  # 0.20 -> 20.0
                            allow_orders=allow_exits,
                        )

        # -------------------------
        # Position management (only touches OPEN positions)
        # -------------------------
        for p in self.get_positions():
            if p.contract.secType != "OPT" or p.position <= 0:
                continue

            conid = int(p.contract.conId)
            qty = int(abs(p.position))

            mark = self.get_mark_price_snapshot(p.contract)
            if mark is None:
                continue

            ret = self.position_return_pct(p, mark)
            if ret is None:
                continue

            # +25%: set breakeven stop (only if no working stop sell exists)
            if ret >= 25 and not self._has_working_stop_sell(conid):
                entry = self._entry_from_position(p)
                if entry and entry > 0:
                    self.place_stop_close_sell(p.contract, qty, float(entry), allow_orders=allow_exits)

            # +50%: take 1 off (only if qty>=2, and only once by checking working/filled state is hard;
            # simplest is: don't do it if you already have a SELL market working for this conId)
            if ret >= 50 and qty >= 2:
                # Avoid stacking multiple take-profit sells
                has_working_sell_mkt = any(
                    (t.order.action == "SELL" and t.order.orderType == "MKT")
                    for t in self._open_orders_for_conid(conid)
                    if getattr(t, "order", None) is not None
                )
                if not has_working_sell_mkt:
                    self.place_market(p.contract, "SELL", 1, allow_orders=allow_exits)

            # Ensure a trailing stop exists (safety net)
            if not self._has_working_trailing_sell(conid):
                self.place_trailing_stop_pct_sell(
                    contract=p.contract,
                    qty=qty,
                    trailing_pct=self.risk.trail_pct * 100.0,
                    allow_orders=allow_exits,
                )


def open_connection(client_id: int) -> IBKRExecutionEngine:
    eng = IBKRExecutionEngine(client_id=client_id)
    eng.connect()
    return eng


def main_execution(client_id: int, symbols):
    eng = open_connection(client_id)
    try:
        symbols = [str(s).upper() for s in symbols if s]
        for i, symbol in enumerate(symbols, start=1):
            try:
                print(f"[EXEC] ({i}/{len(symbols)}) {symbol}", flush=True)
                eng.run(symbol)
            except Exception as e:
                print(f"[EXEC] skip {symbol}: {e}", flush=True)
            finally:
                time.sleep(0.15)
    finally:
        eng.disconnect()
