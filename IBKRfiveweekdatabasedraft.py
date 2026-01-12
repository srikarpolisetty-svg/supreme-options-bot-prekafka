from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import threading
import time
import datetime
import pandas as pd
import os
from databasefunctions import compute_z_scores_for_bucket_5w

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
    c.lastTradeDateOrContractMonth = exp_yyyymmdd  # IB format: YYYYMMDD
    c.strike = float(strike)
    c.right = right  # "C" or "P"
    c.multiplier = "100"
    return c


class App(EWrapper, EClient):
    def __init__(self, symbol: str):
        EClient.__init__(self, self)

        self.symbol = symbol

        # underlying
        self.underlying_conId = None
        self.last_price = None

        # chain
        self.expirations = set()
        self.strikes = set()
        self.got_chain = False

        # reqId bookkeeping
        self._next_req_id = 1
        self._next_ticker_id = 5000

        self.reqid_to_conid = {}
        self.quote_by_conid = {}

        # workflow flags
        self._got_underlying_price = threading.Event()
        self._got_chain = threading.Event()

        # option qualification + quote snapshot waits
        self._pending_opt_qualify = {}  # reqId -> ("C"/"P", strike, exp)
        self._qualified_opt_contracts = {}  # key -> Contract(with conId)
        self._pending_snapshot = {}  # tickerId -> threading.Event
        self.exp = None

    # ---------- IB callbacks ----------
    def nextValidId(self, orderId: int):
        # 1) Request underlying details (to get conId)
        self.reqContractDetails(self._new_req_id(), make_stock(self.symbol))

    def contractDetails(self, reqId, cd):
        con = cd.contract

        # Underlying contract details
        if con.secType == "STK" and self.underlying_conId is None:
            self.underlying_conId = con.conId

            # 2) Request underlying market data snapshot (to get last)
            self.request_market_data(con, snapshot=True)

            # 3) Request option chain definition (needs underlying conId)
            self.reqSecDefOptParams(
                reqId=self._new_req_id(),
                underlyingSymbol=self.symbol,
                futFopExchange="",
                underlyingSecType="STK",
                underlyingConId=self.underlying_conId,
            )
            return

        # Option contract details (qualification)
        if con.secType == "OPT":
            meta = self._pending_opt_qualify.pop(reqId, None)
            if meta is not None:
                right, strike, exp = meta
                key = (right, float(strike), exp)
                self._qualified_opt_contracts[key] = con

    def securityDefinitionOptionParameter(
        self, reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes
    ):
        # collecting strikes and expirations over multiple iterations
        self.expirations.update(expirations)
        self.strikes.update(strikes)

    def securityDefinitionOptionParameterEnd(self, reqId):  # done getting all exiprations and strikes
        self.got_chain = True
        self._got_chain.set()

    def tickPrice(self, reqId, tickType, price, attrib):  # assembling the quote piece by piece
        # Underlying snapshot "last" is tickType=4, close is 9.
        if reqId in self.reqid_to_conid:
            conId = self.reqid_to_conid[reqId]  # identifying what tick it belongs to
            q = self.quote_by_conid.setdefault(conId, {})

            if tickType == 1:  # bid
                q["bid"] = price
            elif tickType == 2:  # ask
                q["ask"] = price
            elif tickType == 4:  # last
                q["last"] = price
            elif tickType == 9:  # close
                q["close"] = price

            # If this is the underlying, latch a usable last_price
            if self.underlying_conId is not None and conId == self.underlying_conId:
                if price is not None and price > 0:
                    # Prefer last, fall back to close if needed
                    if tickType == 4:
                        self.last_price = price
                        self._got_underlying_price.set()
                    elif tickType == 9 and self.last_price is None:
                        self.last_price = price
                        self._got_underlying_price.set()  # prefers to get the last price if possible

            # once I get all the data , I am done with this so I can set it as done
            done_event = self._pending_snapshot.get(reqId)
            if done_event is not None:
                got_bid_ask = ("bid" in q and "ask" in q and q["bid"] is not None and q["ask"] is not None)
                got_last = ("last" in q and q["last"] is not None and q["last"] > 0)
                if got_bid_ask or got_last:
                    done_event.set()

    def tickSize(self, reqId, tickType, size):  # getting other information
        if reqId in self.reqid_to_conid:
            conId = self.reqid_to_conid[reqId]
            q = self.quote_by_conid.setdefault(conId, {})

            if tickType == 0:  # bid size
                q["bidSize"] = size
            elif tickType == 3:  # ask size
                q["askSize"] = size
            elif tickType == 5:  # last size
                q["lastSize"] = size
            elif tickType == 8:  # volume
                q["volume"] = size
            elif tickType == 27:  # open interest (often arrives via tickOptionComputation/other ticks depending on subscription)
                q["oi"] = size

    def tickOptionComputation(
        self,
        reqId,
        tickType,
        impliedVol,
        delta,
        optPrice,
        pvDividend,
        gamma,
        vega,
        theta,
        undPrice,
    ):
        # We only care about MODEL Greeks/IV
        if tickType != 13:  # 13 = MODEL
            return

        if reqId not in self.reqid_to_conid:
            return

        if impliedVol is None or impliedVol < 0:
            return

        conId = self.reqid_to_conid[reqId]
        q = self.quote_by_conid.setdefault(conId, {})
        q["iv"] = impliedVol

    # ---------- helpers ---------- # every request needs a new separate id
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
            is_third_friday = is_friday and 15 <= d.day <= 21  # monthly exp

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

        generic_ticks = "106" if contract.secType == "OPT" else ""

        self.reqMktData(
            reqId,
            contract,
            generic_ticks,  # <-- changed
            snapshot,
            False,
            [],
        )
        return reqId

    def get_option_quote_ibkr(self, opt_contract: Contract, timeout: float = 5.0) -> dict:
        # Snapshot request and wait for at least bid/ask or last
        tickerId = self.request_market_data(opt_contract, snapshot=True)
        ev = self._pending_snapshot[tickerId]

        ev.wait(timeout=timeout)

        conId = opt_contract.conId
        q = dict(self.quote_by_conid.get(conId, {}))

        # Compute mids/spread if possible
        bid = q.get("bid")
        ask = q.get("ask")
        if bid is not None and ask is not None and ask > 0:
            q["mid"] = (bid + ask) / 2.0
            q["spread"] = ask - bid
            q["spread_pct"] = (ask - bid) / ((ask + bid) / 2.0) if (ask + bid) else None

        return q

    # ---------- main workflow ----------
    def run_sequence(self):
        # Wait for underlying last_price (max ~10s)
        if not self._got_underlying_price.wait(timeout=10.0):
            # If we didn't get last, try to proceed if close came in
            if self.last_price is None:
                raise RuntimeError("Did not receive underlying price snapshot in time.")

        # Wait for chain (max ~15s)
        if not self._got_chain.wait(timeout=15.0):
            raise RuntimeError("Did not receive option chain in time.")

        exp = self.get_friday_within_4_days()
        if exp is None:
            raise RuntimeError("No suitable Friday expiration within 4 days found.")

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
        self.qualify_option(opt_atm_c, "C", atm_strike, exp)
        self.qualify_option(opt_atm_p, "P", atm_strike, exp)
        self.qualify_option(opt_c1, "C", c1, exp)
        self.qualify_option(opt_p1, "P", p1, exp)
        self.qualify_option(opt_c2, "C", c2, exp)
        self.qualify_option(opt_p2, "P", p2, exp)

        # Give IB a moment to return contractDetails (or you can wait loop)
        deadline = time.time() + 10.0
        needed = {
            ("C", atm_strike, exp),
            ("P", atm_strike, exp),
            ("C", c1, exp),
            ("P", p1, exp),
            ("C", c2, exp),
            ("P", p2, exp),
        }
        while time.time() < deadline:
            if needed.issubset(set(self._qualified_opt_contracts.keys())):
                break
            time.sleep(0.05)

        if not needed.issubset(set(self._qualified_opt_contracts.keys())):
            raise RuntimeError("Did not qualify all option contracts in time.")

        # Pull qualified contracts back out
        q_atm_c = self._qualified_opt_contracts[("C", atm_strike, exp)]
        q_atm_p = self._qualified_opt_contracts[("P", atm_strike, exp)]
        qc1 = self._qualified_opt_contracts[("C", c1, exp)]
        qp1 = self._qualified_opt_contracts[("P", p1, exp)]
        qc2 = self._qualified_opt_contracts[("C", c2, exp)]
        q_p2 = self._qualified_opt_contracts[("P", p2, exp)]

        # 6) Request market data snapshots for each option & collect quotes
        q_atm_c_quote = self.get_option_quote_ibkr(q_atm_c, timeout=5.0)
        q_atm_p_quote = self.get_option_quote_ibkr(q_atm_p, timeout=5.0)
        q_c1 = self.get_option_quote_ibkr(qc1, timeout=5.0)
        q_p1 = self.get_option_quote_ibkr(qp1, timeout=5.0)
        q_c2 = self.get_option_quote_ibkr(qc2, timeout=5.0)
        q_p2 = self.get_option_quote_ibkr(q_p2, timeout=5.0)

        # Example prints (replace with your DB write)
        print(f"Underlying {self.symbol} last_price:", self.last_price)
        print("EXP:", exp)
        print("ATM_C", atm_strike, q_atm_c_quote)
        print("ATM_P", atm_strike, q_atm_p_quote)
        print("C1", c1, q_c1)
        print("P1", p1, q_p1)
        print("C2", c2, q_c2)
        print("P2", p2, q_p2)

        # ---------- ATM CALL ----------
        atm_call_bid = q_atm_c_quote.get("bid")
        atm_call_ask = q_atm_c_quote.get("ask")
        atm_call_mid = q_atm_c_quote.get("mid")
        atm_call_volume = q_atm_c_quote.get("volume")
        atm_call_iv = q_atm_c_quote.get("iv")
        atm_call_oi = q_atm_c_quote.get("oi")
        atm_call_spread = q_atm_c_quote.get("spread")
        atm_call_spread_pct = q_atm_c_quote.get("spread_pct")

        # ---------- ATM PUT ----------
        atm_put_bid = q_atm_p_quote.get("bid")
        atm_put_ask = q_atm_p_quote.get("ask")
        atm_put_mid = q_atm_p_quote.get("mid")
        atm_put_volume = q_atm_p_quote.get("volume")
        atm_put_iv = q_atm_p_quote.get("iv")
        atm_put_oi = q_atm_p_quote.get("oi")
        atm_put_spread = q_atm_p_quote.get("spread")
        atm_put_spread_pct = q_atm_p_quote.get("spread_pct")

        # ---------- OTM1 CALL (C1) ----------
        otm_call_1_bid = q_c1.get("bid")
        otm_call_1_ask = q_c1.get("ask")
        otm_call_1_mid = q_c1.get("mid")
        otm_call_1_volume = q_c1.get("volume")
        otm_call_1_iv = q_c1.get("iv")
        otm_call_1_oi = q_c1.get("oi")
        otm_call_1_spread = q_c1.get("spread")
        otm_call_1_spread_pct = q_c1.get("spread_pct")

        # ---------- OTM1 PUT (P1) ----------
        otm_put_1_bid = q_p1.get("bid")
        otm_put_1_ask = q_p1.get("ask")
        otm_put_1_mid = q_p1.get("mid")
        otm_put_1_volume = q_p1.get("volume")
        otm_put_1_oi = q_p1.get("oi")
        otm_put_1_spread = q_p1.get("spread")
        otm_put_1_spread_pct = q_p1.get("spread_pct")
        otm_put_1_iv = q_c1.get("iv")

        # ---------- OTM2 CALL (C2) ----------
        otm_call_2_bid = q_c2.get("bid")
        otm_call_2_ask = q_c2.get("ask")
        otm_call_2_mid = q_c2.get("mid")
        otm_call_2_volume = q_c2.get("volume")
        otm_call_2_iv = q_c2.get("iv")
        otm_call_2_oi = q_c2.get("oi")
        otm_call_2_spread = q_c2.get("spread")
        otm_call_2_spread_pct = q_c2.get("spread_pct")

        # ---------- OTM2 PUT (P2) ----------
        otm_put_2_bid = q_p2.get("bid")
        otm_put_2_ask = q_p2.get("ask")
        otm_put_2_mid = q_p2.get("mid")
        otm_put_2_volume = q_p2.get("volume")
        otm_put_2_iv = q_p2.get("iv")
        otm_put_2_oi = q_p2.get("oi")
        otm_put_2_spread = q_p2.get("spread")
        otm_put_2_spread_pct = q_p2.get("spread_pct")

        atm_call_conid = q_atm_c.conId
        atm_put_conid = q_atm_p.conId
        otm1_call_conid = qc1.conId
        otm1_put_conid = qp1.conId
        otm2_call_conid = qc2.conId
        otm2_put_conid = q_p2.conId

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
            "option_symbol",
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
            "con_id"
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
        atm_put_conid = q_atm_p.conId
        otm1_call_conid = qc1.conId
        otm1_put_conid = qp1.conId
        otm2_call_conid = qc2.conId
        otm2_put_conid = q_p2.conId

        cols3 = [
            "con_id"
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

        # ======================
        # MASTER INGEST (5W) - embedded, no params
        # ======================


import duckdb

import duckdb


def master_ingest_5w(run_id: str, db_path: str = "options_data.db"):
    """
    Master ingest for a single 5W options run_id.
    Reads parquet written by shards and inserts into existing tables.
    """

    raw_dir = f"runs/{run_id}/option_snapshots_raw_5w"
    enriched_dir = f"runs/{run_id}/option_snapshots_enriched_5w"
    signals_dir = f"runs/{run_id}/option_snapshots_execution_signals_5w"

    con = duckdb.connect(db_path)

    try:
        con.execute("BEGIN;")

        con.execute(
            """
            INSERT INTO option_snapshots_execution_signals_5w
            SELECT * FROM read_parquet(?)
        """,
            [f"{signals_dir}/shard_*.parquet"],
        )

        con.execute(
            """
            INSERT INTO option_snapshots_enriched_5w
            SELECT * FROM read_parquet(?)
        """,
            [f"{enriched_dir}/shard_*.parquet"],
        )

        con.execute(
            """
            INSERT INTO option_snapshots_raw_5w
            SELECT * FROM read_parquet(?)
        """,
            [f"{raw_dir}/shard_*.parquet"],
        )

        con.execute("COMMIT;")

    except Exception:
        con.execute("ROLLBACK;")
        raise

    finally:
        con.close()


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
