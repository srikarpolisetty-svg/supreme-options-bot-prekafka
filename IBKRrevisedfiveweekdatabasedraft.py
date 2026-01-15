from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import datetime
import pandas as pd
import os
import pytz
import duckdb
from ibapi.contract import Contract as IBContract
from databasefunctions import compute_z_scores_for_bucket_5w

HOST, PORT = "127.0.0.1", 4002


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
    def __init__(self, symbol: str = ""):
        EClient.__init__(self, self)

        self.symbol = symbol
        self.connected_event = threading.Event()

        self._next_req_id = 1
        self._next_ticker_id = 5000

        self.reqid_to_conid = {}
        self.quote_by_conid = {}

        self.reset_for_symbol(symbol)
        self._req_errors = {}
        self._pending_snapshot = {}
        self._pending_contract_details = {}


    def nextValidId(self, orderId: int):
        self.reqMarketDataType(3)  # delayed-ok
        self.connected_event.set()

    def reset_for_symbol(self, symbol: str):
        self.symbol = symbol

        self.underlying_conId = None
        self.last_price = None

        self.expirations = set()
        self.strikes = set()

        self._got_underlying_price = threading.Event()
        self._got_chain = threading.Event()

        self._pending_opt_qualify = {}
        self._qualified_opt_contracts = {}
        self._pending_snapshot = {}

    def start_symbol(self, symbol: str):
        self.reset_for_symbol(symbol)
        self.reqContractDetails(self._new_req_id(), make_stock(symbol))

    def contractDetails(self, reqId, cd):
        con = cd.contract

        if con.secType == "STK" and self.underlying_conId is None:
            self.underlying_conId = con.conId
            self.request_market_data(con, snapshot=True)

            self.reqSecDefOptParams(
                reqId=self._new_req_id(),
                underlyingSymbol=self.symbol,
                futFopExchange="",
                underlyingSecType="STK",
                underlyingConId=self.underlying_conId,
            )
            return

        if con.secType == "OPT":
            meta = self._pending_opt_qualify.pop(reqId, None)
            if meta is not None:
                right, strike, exp = meta
                self._qualified_opt_contracts[(right, float(strike), exp)] = con

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print(f"ERROR {reqId} {errorCode} {errorString}")

        if errorCode == 200:
            # remember it failed
            self._req_errors[reqId] = (errorCode, errorString)

            # unblock anything waiting on this reqId
            ev = self._pending_contract_details.get(reqId)
            if ev:
                ev.set()

            ev = self._pending_snapshot.get(reqId)
            if ev:
                ev.set()

    def securityDefinitionOptionParameter(
        self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes
    ):
        self.expirations.update(expirations)
        self.strikes.update(strikes)

    def securityDefinitionOptionParameterEnd(self, reqId):
        self._got_chain.set()

    def tickPrice(self, reqId, tickType, price, attrib):
        if reqId not in self.reqid_to_conid:
            return

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

        if conId == self.underlying_conId and price and price > 0:
            if tickType in (4, 9):
                self.last_price = price
                self._got_underlying_price.set()
            else:
                b = q.get("bid")
                a = q.get("ask")
                if b is not None and a is not None and b > 0 and a > 0 and self.last_price is None:
                    self.last_price = (b + a) / 2.0
                    self._got_underlying_price.set()

        done_event = self._pending_snapshot.get(reqId)
        if done_event:
            if ("bid" in q and "ask" in q) or ("last" in q):
                done_event.set()


    def tickSize(self, reqId, tickType, size):
        if reqId not in self.reqid_to_conid:
            return

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
        self, reqId, tickType, tickAttrib, impliedVol, delta, optPrice,
        pvDividend, gamma, vega, theta, undPrice
    ):
        if tickType != 13 or impliedVol is None or impliedVol < 0:
            return

        conId = self.reqid_to_conid.get(reqId)
        if conId is None:
            return

        self.quote_by_conid.setdefault(conId, {})["iv"] = impliedVol

    def _new_req_id(self) -> int:
        rid = self._next_req_id
        self._next_req_id += 1
        return rid

    def get_friday_within_4_days(self) -> str | None:
        if not self.expirations:
            return None

        now = datetime.date.today()

        for exp in sorted(self.expirations):
            d = datetime.datetime.strptime(exp, "%Y%m%d").date()

            is_friday = (d.weekday() == 4)
            is_within_4_days = 0 <= (d - now).days <= 4

            if is_friday and is_within_4_days:
                return exp

        return None

    def get_closest_strike_ibkr(self, target: float) -> float:
        if not self.strikes:
            raise RuntimeError("No strikes loaded yet.")
        return float(min(self.strikes, key=lambda s: abs(float(s) - float(target))))
    
    def qualify_option(self, opt: Contract, right: str, strike: float, exp: str):
        reqId = self._new_req_id()

        # track what this request is for (you already had this)
        self._pending_opt_qualify[reqId] = (right, strike, exp)

        # create an Event to wait on
        ev = threading.Event()
        self._pending_contract_details[reqId] = ev

        # send request
        self.reqContractDetails(reqId, opt)

        # wait (DO NOT hang forever)
        ev.wait(timeout=0.6)

        # ---- CRITICAL PART ----
        # if IB said "no security definition", skip safely
        if reqId in self._req_errors:
            self._pending_contract_details.pop(reqId, None)
            self._pending_opt_qualify.pop(reqId, None)
            return None

        # cleanup pending event
        self._pending_contract_details.pop(reqId, None)

        # whatever you already do to retrieve the qualified contract
        # (example — adjust to your storage)
        return True



    def get_option_quote_ibkr(self, opt_contract: Contract, timeout: float = 5.0) -> dict:
        tickerId = self.request_market_data(opt_contract, snapshot=True)
        ev = self._pending_snapshot[tickerId]
        ev.wait(timeout=timeout)

        conId = opt_contract.conId
        q = dict(self.quote_by_conid.get(conId, {}))

        # normalize IB "no quote" sentinels → None
        for k in ("bid", "ask", "last", "close", "iv"):
            if q.get(k) == -1.0:
                q[k] = None

        # sizes / counts: keep None if missing
        for k in ("volume", "oi", "bidSize", "askSize", "lastSize"):
            if k not in q:
                q[k] = None

        bid = q.get("bid")
        ask = q.get("ask")

        # derived fields only if both sides exist and are positive
        if bid is not None and ask is not None and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            spread = ask - bid
            spread_pct = spread / mid if mid else None
        else:
            mid = None
            spread = None
            spread_pct = None

        q["mid"] = mid
        q["spread"] = spread
        q["spread_pct"] = spread_pct

        return q

    def request_market_data(self, contract: Contract, snapshot=True) -> int:
        reqId = self._next_ticker_id
        self._next_ticker_id += 1

        self.reqid_to_conid[reqId] = contract.conId
        self.quote_by_conid.setdefault(contract.conId, {})

        if snapshot:
            self._pending_snapshot[reqId] = threading.Event()
            generic_ticks = ""          # ✅ NO generic ticks for snapshots
        else:
            generic_ticks = "106" if contract.secType == "OPT" else ""

        self.reqMktData(reqId, contract, generic_ticks, snapshot, False, [])
        return reqId


    def run_sequence(self, run_id: str, shard_id: int):

                # Wait for underlying last_price (max ~10s)
        # Wait for underlying last_price (max ~10s)
        if not self._got_underlying_price.wait(timeout=5.0):
            # If we didn't get last, try to proceed if close came in
            if self.last_price is None:
                print(f"skip {self.symbol}: did not receive underlying price snapshot in time.")
                return None


        # Wait for chain (max ~15s)
        if not self._got_chain.wait(timeout=8.0):
            print(f"skip {self.symbol}: did not receive option chain in time.")
            return None


        exp = self.get_friday_within_4_days()
        if exp is None:
            print(f"skip {self.symbol}: no Friday expiration within 4 days")
            return None


        atm = float(self.last_price)

        # targets
        otm_call_1_target = atm * 1.015
        otm_put_1_target = atm * 0.985
        otm_call_2_target = atm * 1.035
        otm_put_2_target = atm * 0.965

        # closest strikes
        atm_strike = self.get_closest_strike_ibkr(atm)
        c1 = self.get_closest_strike_ibkr(otm_call_1_target)
        p1 = self.get_closest_strike_ibkr(otm_put_1_target)
        c2 = self.get_closest_strike_ibkr(otm_call_2_target)
        p2 = self.get_closest_strike_ibkr(otm_put_2_target)

        # Build option contracts
        opt_atm_c = make_option(self.symbol, exp, atm_strike, "C")
        opt_atm_p = make_option(self.symbol, exp, atm_strike, "P")
        opt_c1 = make_option(self.symbol, exp, c1, "C")
        opt_p1 = make_option(self.symbol, exp, p1, "P")
        opt_c2 = make_option(self.symbol, exp, c2, "C")
        opt_p2 = make_option(self.symbol, exp, p2, "P")

        # 5) Qualify options (get conIds)
        qc_atm_c = self.qualify_option(opt_atm_c, "C", atm_strike, exp)
        qc_atm_p = self.qualify_option(opt_atm_p, "P", atm_strike, exp)
        qc_c1    = self.qualify_option(opt_c1,    "C", c1,         exp)
        qc_p1    = self.qualify_option(opt_p1,    "P", p1,         exp)
        qc_c2    = self.qualify_option(opt_c2,    "C", c2,         exp)
        qc_p2    = self.qualify_option(opt_p2,    "P", p2,         exp)

        qualified = [qc_atm_c, qc_atm_p, qc_c1, qc_p1, qc_c2, qc_p2]

        # if you require all 6, skip symbol safely
        if any(qc is None for qc in qualified):
            return None  # caller: treat as "skip symbol"



        # Pull qualified contracts back out
        # qualified contracts (Contracts)
        atm_c_con = self._qualified_opt_contracts[("C", atm_strike, exp)]
        atm_p_con = self._qualified_opt_contracts[("P", atm_strike, exp)]
        c1_con    = self._qualified_opt_contracts[("C", c1, exp)]
        p1_con    = self._qualified_opt_contracts[("P", p1, exp)]
        c2_con    = self._qualified_opt_contracts[("C", c2, exp)]
        p2_con    = self._qualified_opt_contracts[("P", p2, exp)]

        # quotes (dicts)
        atm_c_q = self.get_option_quote_ibkr(atm_c_con, timeout=2.0)
        atm_p_q = self.get_option_quote_ibkr(atm_p_con, timeout=2.0)
        c1_q    = self.get_option_quote_ibkr(c1_con, timeout=2.0)
        p1_q    = self.get_option_quote_ibkr(p1_con, timeout=2.0)
        c2_q    = self.get_option_quote_ibkr(c2_con, timeout=2.0)
        p2_q    = self.get_option_quote_ibkr(p2_con, timeout=2.0)


        print(f"Underlying {self.symbol} last_price:", self.last_price)
        print("EXP:", exp)
        print("ATM_C", atm_strike, atm_c_q)
        print("ATM_P", atm_strike, atm_p_q)
        print("C1", c1, c1_q)
        print("P1", p1, p1_q)
        print("C2", c2, c2_q)
        print("P2", p2, p2_q)

        # ---------- ATM CALL ----------
        atm_call_bid        = atm_c_q.get("bid")
        atm_call_ask        = atm_c_q.get("ask")
        atm_call_mid        = atm_c_q.get("mid")
        atm_call_volume     = atm_c_q.get("volume")
        atm_call_iv         = atm_c_q.get("iv")
        atm_call_oi         = atm_c_q.get("oi")
        atm_call_spread     = atm_c_q.get("spread")
        atm_call_spread_pct = atm_c_q.get("spread_pct")

        # ---------- ATM PUT ----------
        atm_put_bid         = atm_p_q.get("bid")
        atm_put_ask         = atm_p_q.get("ask")
        atm_put_mid         = atm_p_q.get("mid")
        atm_put_volume      = atm_p_q.get("volume")
        atm_put_iv          = atm_p_q.get("iv")
        atm_put_oi          = atm_p_q.get("oi")
        atm_put_spread      = atm_p_q.get("spread")
        atm_put_spread_pct  = atm_p_q.get("spread_pct")

        # ---------- OTM1 CALL (C1) ----------
        otm_call_1_bid        = c1_q.get("bid")
        otm_call_1_ask        = c1_q.get("ask")
        otm_call_1_mid        = c1_q.get("mid")
        otm_call_1_volume     = c1_q.get("volume")
        otm_call_1_iv         = c1_q.get("iv")
        otm_call_1_oi         = c1_q.get("oi")
        otm_call_1_spread     = c1_q.get("spread")
        otm_call_1_spread_pct = c1_q.get("spread_pct")

        # ---------- OTM1 PUT (P1) ----------
        otm_put_1_bid         = p1_q.get("bid")
        otm_put_1_ask         = p1_q.get("ask")
        otm_put_1_mid         = p1_q.get("mid")
        otm_put_1_volume      = p1_q.get("volume")
        otm_put_1_oi          = p1_q.get("oi")
        otm_put_1_spread      = p1_q.get("spread")
        otm_put_1_spread_pct  = p1_q.get("spread_pct")
        otm_put_1_iv          = p1_q.get("iv")

        # ---------- OTM2 CALL (C2) ----------
        otm_call_2_bid        = c2_q.get("bid")
        otm_call_2_ask        = c2_q.get("ask")
        otm_call_2_mid        = c2_q.get("mid")
        otm_call_2_volume     = c2_q.get("volume")
        otm_call_2_iv         = c2_q.get("iv")
        otm_call_2_oi         = c2_q.get("oi")
        otm_call_2_spread     = c2_q.get("spread")
        otm_call_2_spread_pct = c2_q.get("spread_pct")

        # ---------- OTM2 PUT (P2) ----------
        otm_put_2_bid         = p2_q.get("bid")
        otm_put_2_ask         = p2_q.get("ask")
        otm_put_2_mid         = p2_q.get("mid")
        otm_put_2_volume      = p2_q.get("volume")
        otm_put_2_iv          = p2_q.get("iv")
        otm_put_2_oi          = p2_q.get("oi")
        otm_put_2_spread      = p2_q.get("spread")
        otm_put_2_spread_pct  = p2_q.get("spread_pct")

        # conIds (from Contracts, not quote dicts)
        atm_call_conid = atm_c_con.conId
        atm_put_conid  = atm_p_con.conId
        otm1_call_conid = c1_con.conId
        otm1_put_conid  = p1_con.conId
        otm2_call_conid = c2_con.conId
        otm2_put_conid  = p2_con.conId


        import datetime
        import pytz

        est = pytz.timezone("US/Eastern")

        now_est = datetime.datetime.now(est)
        timestamp = now_est.strftime("%Y-%m-%d %H:%M:%S")
        symbol = self.symbol
        snapshot_id = f"{self.symbol}_{timestamp}"

        exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()
        now_date = now_est.date()

        days_till_expiry = (exp_date - now_date).days

        if days_till_expiry <= 1:
            time_decay_bucket = "EXTREME"
        elif days_till_expiry <= 3:
            time_decay_bucket = "HIGH"
        elif days_till_expiry <= 7:
            time_decay_bucket = "MEDIUM"
        else:
            time_decay_bucket = "LOW"

        # closest strikes
        atm_strike = self.get_closest_strike_ibkr(atm)
        c1 = self.get_closest_strike_ibkr(otm_call_1_target)
        p1 = self.get_closest_strike_ibkr(otm_put_1_target)
        c2 = self.get_closest_strike_ibkr(otm_call_2_target)
        p2 = self.get_closest_strike_ibkr(otm_put_2_target)
  
        cols1 = [
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

        rows1 = [
            # ===== ATM CALL =====
            [
                atm_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                atm_strike,
                "C",
                days_till_expiry,
                exp_date,
                "ATM",
                atm_call_bid,
                atm_call_ask,
                atm_call_mid,
                atm_call_volume,
                atm_call_oi,
                atm_call_iv,
                atm_call_spread,
                atm_call_spread_pct,
                time_decay_bucket,
            ],
            # ===== ATM PUT =====
            [
                atm_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                atm_strike,
                "P",
                days_till_expiry,
                exp_date,
                "ATM",
                atm_put_bid,
                atm_put_ask,
                atm_put_mid,
                atm_put_volume,
                atm_put_oi,
                atm_put_iv,
                atm_put_spread,
                atm_put_spread_pct,
                time_decay_bucket,
            ],
            # ===== OTM CALL 1 =====
            [
                otm1_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                c1,
                "C",
                days_till_expiry,
                exp_date,
                "OTM_1",
                otm_call_1_bid,
                otm_call_1_ask,
                otm_call_1_mid,
                otm_call_1_volume,
                otm_call_1_oi,
                otm_call_1_iv,
                otm_call_1_spread,
                otm_call_1_spread_pct,
                time_decay_bucket,
            ],
            # ===== OTM PUT 1 =====
            [
                otm1_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                p1,
                "P",
                days_till_expiry,
                exp_date,
                "OTM_1",
                otm_put_1_bid,
                otm_put_1_ask,
                otm_put_1_mid,
                otm_put_1_volume,
                otm_put_1_oi,
                otm_put_1_iv,
                otm_put_1_spread,
                otm_put_1_spread_pct,
                time_decay_bucket,
            ],
            # ===== OTM CALL 2 =====
            [
                otm2_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                c2,
                "C",
                days_till_expiry,
                exp_date,
                "OTM_2",
                otm_call_2_bid,
                otm_call_2_ask,
                otm_call_2_mid,
                otm_call_2_volume,
                otm_call_2_oi,
                otm_call_2_iv,
                otm_call_2_spread,
                otm_call_2_spread_pct,
                time_decay_bucket,
            ],
            # ===== OTM PUT 2 =====
            [
                otm2_put_conid, 
                snapshot_id,
                timestamp,
                symbol,
                p2,
                "P",
                days_till_expiry,
                exp_date,
                "OTM_2",
                otm_put_2_bid,
                otm_put_2_ask,
                otm_put_2_mid,
                otm_put_2_volume,
                otm_put_2_oi,
                otm_put_2_iv,
                otm_put_2_spread,
                otm_put_2_spread_pct,
                time_decay_bucket,
            ],
        ]


        df1 = pd.DataFrame(rows1, columns=cols1)

        out_dir = f"runs/{run_id}/option_snapshots_raw_5w"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
        df1.to_parquet(out_path, index=False)

        atm_call_z, atm_call_vol_z, atm_call_iv_z = compute_z_scores_for_bucket_5w(
            symbol=symbol,
            bucket="ATM",
            call_put="C",
            time_decay_bucket=time_decay_bucket,
            current_mid=atm_call_mid,
            current_volume=atm_call_volume,
            current_iv=atm_call_iv,
        )
        atm_put_z, atm_put_vol_z, atm_put_iv_z = compute_z_scores_for_bucket_5w(
            symbol=symbol,
            bucket="ATM",
            call_put="P",
            time_decay_bucket=time_decay_bucket,
            current_mid=atm_put_mid,
            current_volume=atm_put_volume,
            current_iv=atm_put_iv,
        )
        otm_call_1_z, otm_call_1_vol_z, otm_call_1_iv_z = compute_z_scores_for_bucket_5w(
            symbol=symbol,
            bucket="OTM_1",
            call_put="C",
            time_decay_bucket=time_decay_bucket,
            current_mid=otm_call_1_mid,
            current_volume=otm_call_1_volume,
            current_iv=otm_call_1_iv,
        )
        otm_put_1_z, otm_put_1_vol_z, otm_put_1_iv_z = compute_z_scores_for_bucket_5w(
            symbol=symbol,
            bucket="OTM_1",
            call_put="P",
            time_decay_bucket=time_decay_bucket,
            current_mid=otm_put_1_mid,
            current_volume=otm_put_1_volume,
            current_iv=otm_put_1_iv,
        )
        otm_call_2_z, otm_call_2_vol_z, otm_call_2_iv_z = compute_z_scores_for_bucket_5w(
            symbol=symbol,
            bucket="OTM_2",
            call_put="C",
            time_decay_bucket=time_decay_bucket,
            current_mid=otm_call_2_mid,
            current_volume=otm_call_2_volume,
            current_iv=otm_call_2_iv,
        )
        otm_put_2_z, otm_put_2_vol_z, otm_put_2_iv_z = compute_z_scores_for_bucket_5w(
            symbol=symbol,
            bucket="OTM_2",
            call_put="P",
            time_decay_bucket=time_decay_bucket,
            current_mid=otm_put_2_mid,
            current_volume=otm_put_2_volume,
            current_iv=otm_put_2_iv,
        )

        # ======================
        # ENRICHED (5W)
        # ======================        atm_call_conid = q_atm_c.conId


        cols2 = [
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
            "mid_z",
            "volume_z",
            "iv_z",
            "opt_ret_10m",
            "opt_ret_1h",
            "opt_ret_eod",
            "opt_ret_next_open",
            "opt_ret_1d",
            "opt_ret_exp",
        ]

        rows2 = [
            [
                atm_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                atm_strike,
                "C",
                days_till_expiry,
                exp_date,
                "ATM",
                atm_call_bid,
                atm_call_ask,
                atm_call_mid,
                atm_call_volume,
                atm_call_oi,
                atm_call_iv,
                atm_call_spread,
                atm_call_spread_pct,
                time_decay_bucket,
                atm_call_z,
                atm_call_vol_z,
                atm_call_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                atm_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                atm_strike,
                "P",
                days_till_expiry,
                exp_date,
                "ATM",
                atm_put_bid,
                atm_put_ask,
                atm_put_mid,
                atm_put_volume,
                atm_put_oi,
                atm_put_iv,
                atm_put_spread,
                atm_put_spread_pct,
                time_decay_bucket,
                atm_put_z,
                atm_put_vol_z,
                atm_put_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm1_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                c1,
                "C",
                days_till_expiry,
                exp_date,
                "OTM_1",
                otm_call_1_bid,
                otm_call_1_ask,
                otm_call_1_mid,
                otm_call_1_volume,
                otm_call_1_oi,
                otm_call_1_iv,
                otm_call_1_spread,
                otm_call_1_spread_pct,
                time_decay_bucket,
                otm_call_1_z,
                otm_call_1_vol_z,
                otm_call_1_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm1_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                p1,
                "P",
                days_till_expiry,
                exp_date,
                "OTM_1",
                otm_put_1_bid,
                otm_put_1_ask,
                otm_put_1_mid,
                otm_put_1_volume,
                otm_put_1_oi,
                otm_put_1_iv,
                otm_put_1_spread,
                otm_put_1_spread_pct,
                time_decay_bucket,
                otm_put_1_z,
                otm_put_1_vol_z,
                otm_put_1_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm2_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                c2,
                "C",
                days_till_expiry,
                exp_date,
                "OTM_2",
                otm_call_2_bid,
                otm_call_2_ask,
                otm_call_2_mid,
                otm_call_2_volume,
                otm_call_2_oi,
                otm_call_2_iv,
                otm_call_2_spread,
                otm_call_2_spread_pct,
                time_decay_bucket,
                otm_call_2_z,
                otm_call_2_vol_z,
                otm_call_2_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm2_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                p2,
                "P",
                days_till_expiry,
                exp_date,
                "OTM_2",
                otm_put_2_bid,
                otm_put_2_ask,
                otm_put_2_mid,
                otm_put_2_volume,
                otm_put_2_oi,
                otm_put_2_iv,
                otm_put_2_spread,
                otm_put_2_spread_pct,
                time_decay_bucket,
                otm_put_2_z,
                otm_put_2_vol_z,
                otm_put_2_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ]

        df2 = pd.DataFrame(rows2, columns=cols2)

        out_dir = f"runs/{run_id}/option_snapshots_enriched_5w"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
        df2.to_parquet(out_path, index=False)

        # ======================
        # EXECUTION SIGNALS (5W)
        # ======================        atm_call_conid = q_atm_c.conId

        cols3 = [
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
            "mid_z",
            "volume_z",
            "iv_z",
            "opt_ret_10m",
            "opt_ret_1h",
            "opt_ret_eod",
            "opt_ret_next_open",
            "opt_ret_1d",
            "opt_ret_exp",
            "atm_call_signal",
            "atm_put_signal",
            "otm1_call_signal",
            "otm1_put_signal",
            "otm2_call_signal",
            "otm2_put_signal",
        ]
        
        rows3 = [
            [
                atm_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                atm_strike,
                "C",
                days_till_expiry,
                exp_date,
                "ATM",
                atm_call_bid,
                atm_call_ask,
                atm_call_mid,
                atm_call_volume,
                atm_call_oi,
                atm_call_iv,
                atm_call_spread,
                atm_call_spread_pct,
                time_decay_bucket,
                atm_call_z,
                atm_call_vol_z,
                atm_call_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                atm_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                atm_strike,
                "P",
                days_till_expiry,
                exp_date,
                "ATM",
                atm_put_bid,
                atm_put_ask,
                atm_put_mid,
                atm_put_volume,
                atm_put_oi,
                atm_put_iv,
                atm_put_spread,
                atm_put_spread_pct,
                time_decay_bucket,
                atm_put_z,
                atm_put_vol_z,
                atm_put_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm1_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                c1,
                "C",
                days_till_expiry,
                exp_date,
                "OTM_1",
                otm_call_1_bid,
                otm_call_1_ask,
                otm_call_1_mid,
                otm_call_1_volume,
                otm_call_1_oi,
                otm_call_1_iv,
                otm_call_1_spread,
                otm_call_1_spread_pct,
                time_decay_bucket,
                otm_call_1_z,
                otm_call_1_vol_z,
                otm_call_1_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm1_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                p1,
                "P",
                days_till_expiry,
                exp_date,
                "OTM_1",
                otm_put_1_bid,
                otm_put_1_ask,
                otm_put_1_mid,
                otm_put_1_volume,
                otm_put_1_oi,
                otm_put_1_iv,
                otm_put_1_spread,
                otm_put_1_spread_pct,
                time_decay_bucket,
                otm_put_1_z,
                otm_put_1_vol_z,
                otm_put_1_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm2_call_conid,
                snapshot_id,
                timestamp,
                symbol,
                c2,
                "C",
                days_till_expiry,
                exp_date,
                "OTM_2",
                otm_call_2_bid,
                otm_call_2_ask,
                otm_call_2_mid,
                otm_call_2_volume,
                otm_call_2_oi,
                otm_call_2_iv,
                otm_call_2_spread,
                otm_call_2_spread_pct,
                time_decay_bucket,
                otm_call_2_z,
                otm_call_2_vol_z,
                otm_call_2_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            [
                otm2_put_conid,
                snapshot_id,
                timestamp,
                symbol,
                p2,
                "P",
                days_till_expiry,
                exp_date,
                "OTM_2",
                otm_put_2_bid,
                otm_put_2_ask,
                otm_put_2_mid,
                otm_put_2_volume,
                otm_put_2_oi,
                otm_put_2_iv,
                otm_put_2_spread,
                otm_put_2_spread_pct,
                time_decay_bucket,
                otm_put_2_z,
                otm_put_2_vol_z,
                otm_put_2_iv_z,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ],
        ]

        df3 = pd.DataFrame(rows3, columns=cols3)

        out_dir = f"runs/{run_id}/option_snapshots_execution_signals_5w"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
        df3.to_parquet(out_path, index=False)
        pass


def open_connection(client_id: int) -> App:
    app = App()
    app.connect(HOST, PORT, clientId=client_id)

    threading.Thread(target=app.run, daemon=True).start()

    if not app.connected_event.wait(timeout=10):
        raise RuntimeError("Failed to connect")

    return app



def main_parquet(client_id: int, shard: int, run_id: str, symbols):
    app = open_connection(client_id)
    try:
        for symbol in symbols:
            app.start_symbol(symbol)
            res = app.run_sequence(run_id=run_id, shard_id=shard)
            if res is None:
                continue
            time.sleep(0.3)
    finally:
        app.disconnect()


