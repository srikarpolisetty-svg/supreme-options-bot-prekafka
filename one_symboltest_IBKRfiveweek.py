from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import datetime
import pandas as pd

HOST, PORT, CLIENT_ID = "127.0.0.1", 4002, 7


def make_stock(symbol: str) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def make_option(symbol: str, exp_yyyymmdd: str, strike: float, right: str) -> Contract:
    c = Contract()
    c.symbol = symbol
    c.secType = "OPT"
    c.exchange = "SMART"
    c.currency = "USD"
    c.lastTradeDateOrContractMonth = exp_yyyymmdd
    c.strike = float(strike)
    c.right = right
    c.multiplier = "100"
    return c


class App(EWrapper, EClient):
    def __init__(self, symbol: str):
        EClient.__init__(self, self)
        self.symbol = symbol

        self.underlying_conId = None
        self.last_price = None

        self.expirations = set()
        self.strikes = set()

        self._next_req_id = 1
        self._next_ticker_id = 5000

        self.reqid_to_conid = {}
        self.quote_by_conid = {}

        self._got_underlying_price = threading.Event()
        self._got_chain = threading.Event()

        self._pending_opt_qualify = {}       # reqId -> (right, strike, exp)
        self._qualified_opt_contracts = {}   # (right, strike, exp) -> Contract(with conId)
        self._pending_snapshot = {}          # tickerId -> threading.Event

    # ---------- IB callbacks ----------
    def nextValidId(self, orderId: int):
        self.reqContractDetails(self._new_req_id(), make_stock(self.symbol))
        self.reqMarketDataType(3)

    def contractDetails(self, reqId, cd):
        con = cd.contract

        # Underlying contract details
        if con.secType == "STK" and self.underlying_conId is None:
            self.underlying_conId = con.conId

            # underlying snapshot
            self.request_market_data(con, snapshot=True)

            # option chain definition
            self.reqSecDefOptParams(
                reqId=self._new_req_id(),
                underlyingSymbol=self.symbol,
                futFopExchange="",
                underlyingSecType="STK",
                underlyingConId=self.underlying_conId,
            )
            return

        # Option qualification details
        if con.secType == "OPT":
            meta = self._pending_opt_qualify.pop(reqId, None)
            if meta is not None:
                right, strike, exp = meta
                key = (right, float(strike), exp)
                self._qualified_opt_contracts[key] = con

    def securityDefinitionOptionParameter(
        self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes
    ):
        self.expirations.update(expirations)
        self.strikes.update(strikes)

    def securityDefinitionOptionParameterEnd(self, reqId):
        self._got_chain.set()

    def tickPrice(self, reqId, tickType, price, attrib):
        if reqId in self.reqid_to_conid:
            conId = self.reqid_to_conid[reqId]
            q = self.quote_by_conid.setdefault(conId, {})

            if tickType == 1:
                q["bid"] = price
            elif tickType == 2:
                q["ask"] = price
            elif tickType == 4:
                q["last"] = price
            elif tickType == 9:
                q["close"] = price

            # underlying last
            if self.underlying_conId is not None and conId == self.underlying_conId:
                if price is not None and price > 0:
                    if tickType == 4:
                        self.last_price = price
                        self._got_underlying_price.set()
                    elif tickType == 9 and self.last_price is None:
                        self.last_price = price
                        self._got_underlying_price.set()

            done_event = self._pending_snapshot.get(reqId)
            if done_event is not None:
                got_bid_ask = ("bid" in q and "ask" in q and q["bid"] is not None and q["ask"] is not None)
                got_last = ("last" in q and q["last"] is not None and q["last"] > 0)
                if got_bid_ask or got_last:
                    done_event.set()

    def tickSize(self, reqId, tickType, size):
        if reqId in self.reqid_to_conid:
            conId = self.reqid_to_conid[reqId]
            q = self.quote_by_conid.setdefault(conId, {})

            if tickType == 0:
                q["bidSize"] = size
            elif tickType == 3:
                q["askSize"] = size
            elif tickType == 5:
                q["lastSize"] = size
            elif tickType == 8:
                q["volume"] = size
            elif tickType == 27:
                q["oi"] = size

    def tickOptionComputation(
        self, reqId, tickType, impliedVol, delta, optPrice, pvDividend, gamma, vega, theta, undPrice
    ):
        if tickType != 13:  # MODEL
            return
        if reqId not in self.reqid_to_conid:
            return
        if impliedVol is None or impliedVol < 0:
            return

        conId = self.reqid_to_conid[reqId]
        q = self.quote_by_conid.setdefault(conId, {})
        q["iv"] = impliedVol

    # ---------- helpers ----------
    def _new_req_id(self) -> int:
        rid = self._next_req_id
        self._next_req_id += 1
        return rid

    def get_closest_strike_ibkr(self, target: float) -> float:
        if not self.strikes:
            raise RuntimeError("No strikes loaded yet.")
        return float(min(self.strikes, key=lambda s: abs(float(s) - float(target))))

    def get_friday_within_4_days(self) -> str | None:
        if not self.expirations:
            return None

        now = datetime.date.today()
        for exp in sorted(self.expirations):
            d = datetime.datetime.strptime(exp, "%Y%m%d").date()

            is_friday = (d.weekday() == 4)
            is_within_4_days = 0 <= (d - now).days <= 4
            is_third_friday = is_friday and 15 <= d.day <= 21

            if is_friday and is_within_4_days and not is_third_friday:
                return exp
        return None

    def qualify_option(self, opt: Contract, right: str, strike: float, exp: str):
        reqId = self._new_req_id()
        self._pending_opt_qualify[reqId] = (right, strike, exp)
        self.reqContractDetails(reqId, opt)

    def request_market_data(self, contract: Contract, snapshot: bool = True) -> int:
        if not getattr(contract, "conId", 0):
            raise ValueError("Contract must be qualified (conId set) before requesting market data.")

        reqId = self._next_ticker_id
        self._next_ticker_id += 1

        self.reqid_to_conid[reqId] = contract.conId
        self.quote_by_conid.setdefault(contract.conId, {})

        if snapshot:
            self._pending_snapshot[reqId] = threading.Event()

        # 106 => option model computations / greek-ish stream (IV via tickOptionComputation MODEL)
        generic_ticks = "106" if contract.secType == "OPT" else ""

        self.reqMktData(reqId, contract, generic_ticks, snapshot, False, [])
        return reqId

    def get_option_quote_ibkr(self, opt_contract: Contract, timeout: float = 5.0) -> dict:
        tickerId = self.request_market_data(opt_contract, snapshot=True)
        ev = self._pending_snapshot[tickerId]
        ev.wait(timeout=timeout)

        conId = opt_contract.conId
        q = dict(self.quote_by_conid.get(conId, {}))

        bid = q.get("bid")
        ask = q.get("ask")
        if bid is not None and ask is not None and ask > 0:
            q["mid"] = (bid + ask) / 2.0
            q["spread"] = ask - bid
            q["spread_pct"] = (ask - bid) / ((ask + bid) / 2.0) if (ask + bid) else None

        return q

    # ---------- main workflow ----------
    def run_sequence(self):
        if not self._got_underlying_price.wait(timeout=10.0):
            if self.last_price is None:
                raise RuntimeError("Did not receive underlying price snapshot in time.")

        if not self._got_chain.wait(timeout=15.0):
            raise RuntimeError("Did not receive option chain in time.")

        exp = self.get_friday_within_4_days()
        if exp is None:
            raise RuntimeError("No suitable Friday expiration within 4 days found.")

        atm = float(self.last_price)

        otm_call_1_target = atm * 1.015
        otm_put_1_target = atm * 0.985
        otm_call_2_target = atm * 1.035
        otm_put_2_target = atm * 0.965

        atm_strike = self.get_closest_strike_ibkr(atm)
        c1 = self.get_closest_strike_ibkr(otm_call_1_target)
        p1 = self.get_closest_strike_ibkr(otm_put_1_target)
        c2 = self.get_closest_strike_ibkr(otm_call_2_target)
        p2 = self.get_closest_strike_ibkr(otm_put_2_target)

        # Build + qualify
        to_qualify = [
            ("C", atm_strike, exp),
            ("P", atm_strike, exp),
            ("C", c1, exp),
            ("P", p1, exp),
            ("C", c2, exp),
            ("P", p2, exp),
        ]
        for right, strike, exp_ in to_qualify:
            self.qualify_option(make_option(self.symbol, exp_, strike, right), right, strike, exp_)

        # Wait for all qualifications
        deadline = time.time() + 10.0
        needed = set(to_qualify)
        while time.time() < deadline:
            if needed.issubset(set(self._qualified_opt_contracts.keys())):
                break
            time.sleep(0.05)
        if not needed.issubset(set(self._qualified_opt_contracts.keys())):
            missing = needed.difference(set(self._qualified_opt_contracts.keys()))
            raise RuntimeError(f"Did not qualify all option contracts in time. Missing: {missing}")

        # Pull qualified contracts
        q_atm_c = self._qualified_opt_contracts[("C", atm_strike, exp)]
        q_atm_p = self._qualified_opt_contracts[("P", atm_strike, exp)]
        qc1 = self._qualified_opt_contracts[("C", c1, exp)]
        qp1 = self._qualified_opt_contracts[("P", p1, exp)]
        qc2 = self._qualified_opt_contracts[("C", c2, exp)]
        qp2 = self._qualified_opt_contracts[("P", p2, exp)]

        # Quotes
        atm_c_q = self.get_option_quote_ibkr(q_atm_c, timeout=5.0)
        atm_p_q = self.get_option_quote_ibkr(q_atm_p, timeout=5.0)
        c1_q = self.get_option_quote_ibkr(qc1, timeout=5.0)
        p1_q = self.get_option_quote_ibkr(qp1, timeout=5.0)
        c2_q = self.get_option_quote_ibkr(qc2, timeout=5.0)
        p2_q = self.get_option_quote_ibkr(qp2, timeout=5.0)

        print(f"\nUnderlying {self.symbol} last_price: {self.last_price}")
        print("EXP:", exp)
        print("ATM_C", atm_strike, atm_c_q)
        print("ATM_P", atm_strike, atm_p_q)
        print("C1", c1, c1_q)
        print("P1", p1, p1_q)
        print("C2", c2, c2_q)
        print("P2", p2, p2_q)

        # Time / buckets
        now = datetime.datetime.now()
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_id = f"{self.symbol}_{timestamp}"

        exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()
        now_date = now.date()
        days_till_expiry = (exp_date - now_date).days

        if days_till_expiry <= 1:
            time_decay_bucket = "EXTREME"
        elif days_till_expiry <= 3:
            time_decay_bucket = "HIGH"
        elif days_till_expiry <= 7:
            time_decay_bucket = "MEDIUM"
        else:
            time_decay_bucket = "LOW"

        # helper to build row
        def row_for(con, strike, cp, bucket, q):
            return [
                con.conId,
                snapshot_id,
                timestamp,
                self.symbol,
                strike,
                cp,
                days_till_expiry,
                exp_date,
                bucket,
                q.get("bid"),
                q.get("ask"),
                q.get("mid"),
                q.get("volume"),
                q.get("oi"),
                q.get("iv"),
                q.get("spread"),
                q.get("spread_pct"),
                time_decay_bucket,
            ]

        cols = [
            "con_id",
            "snapshot_id",
            "timestamp",
            "symbol",
            "strike",
            "call_put",
            "days_to_expiry",
            "expiration_date",
            "moneyness_bucket",
            "bid",
            "ask",
            "mid",
            "volume",
            "open_interest",
            "iv",
            "spread",
            "spread_pct",
            "time_decay_bucket",
        ]

        rows = [
            row_for(q_atm_c, atm_strike, "C", "ATM", atm_c_q),
            row_for(q_atm_p, atm_strike, "P", "ATM", atm_p_q),
            row_for(qc1, c1, "C", "OTM_1", c1_q),
            row_for(qp1, p1, "P", "OTM_1", p1_q),
            row_for(qc2, c2, "C", "OTM_2", c2_q),
            row_for(qp2, p2, "P", "OTM_2", p2_q),
        ]

        df1 = pd.DataFrame(rows, columns=cols)
        print("\nDF1 (raw-ish):")
        print(df1)

        # For test version, just clone df1 into df2/df3 with empty extras
        df2 = df1.copy()
        for c in ["mid_z", "volume_z", "iv_z", "opt_ret_10m", "opt_ret_1h", "opt_ret_eod", "opt_ret_next_open", "opt_ret_1d", "opt_ret_exp"]:
            df2[c] = None

        df3 = df2.copy()
        for c in ["atm_call_signal", "atm_put_signal", "otm1_call_signal", "otm1_put_signal", "otm2_call_signal", "otm2_put_signal"]:
            df3[c] = None

        print("\nDF2 (enriched stub):")
        print(df2)
        print("\nDF3 (signals stub):")
        print(df3)


def main():
    app = App("AAPL")
    app.connect(HOST, PORT, clientId=CLIENT_ID)

    t = threading.Thread(target=app.run, daemon=True)
    t.start()

    try:
        app.run_sequence()
    finally:
        app.disconnect()


if __name__ == "__main__":
    main()
