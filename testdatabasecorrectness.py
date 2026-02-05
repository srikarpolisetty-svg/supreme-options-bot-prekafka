import databento as db
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import DATABENTO_API_KEY
import duckdb
import math

# ==============================
# CONFIG
# ==============================
SYMBOL = "AAPL"
DB_PATH = "options_data.db"
DAYS_BACK = 35

EVERY_NTH_TIMESTAMP = 20
MAX_CHECKS = 50

# tolerances for quotes
BIDASK_ABS_TOL = 0.05
BIDASK_PCT_TOL = 0.10
SPREAD_ABS_TOL = 0.05
SPREAD_PCT_TOL = 0.20
SPREADPCT_ABS_TOL = 0.10

# tolerances for volume / OI
VOLUME_ABS_TOL = 50
VOLUME_PCT_TOL = 0.50
OI_ABS_TOL = 0  # usually exact

# tolerances for IV
IV_ABS_TOL = 0.05
IV_PCT_TOL = 0.25

client = db.Historical(DATABENTO_API_KEY)

# ==============================
# TIME / CAST HELPERS
# ==============================
def db_end_utc_day() -> datetime:
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

def to_utc_ts(x) -> pd.Timestamp:
    ts = pd.Timestamp(x)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")

def to_utc_naive_dt(x) -> datetime:
    ts = to_utc_ts(x)
    return ts.tz_convert("UTC").tz_localize(None).to_pydatetime()

def to_int_or_none(x):
    if x is None or pd.isna(x):
        return None
    return int(x)

def to_float_or_none(x):
    if x is None or pd.isna(x):
        return None
    return float(x)

def _ensure_utc_col(df: pd.DataFrame, col: str) -> None:
    if df is None or df.empty or col not in df.columns:
        return
    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

# ==============================
# NUM CHECKS
# ==============================
def pct_diff(a: float, b: float) -> float:
    if a is None or b is None:
        return float("inf")
    denom = (abs(a) + abs(b)) / 2.0
    if denom == 0:
        return 0.0 if a == b else float("inf")
    return abs(a - b) / denom

def ok_num(db_v, live_v, abs_tol, pct_tol) -> bool:
    if db_v is None or live_v is None:
        return True
    if abs(db_v - live_v) <= abs_tol:
        return True
    return pct_diff(db_v, live_v) <= pct_tol

def ok_int(db_v, live_v, abs_tol, pct_tol) -> bool:
    if db_v is None or live_v is None:
        return True
    db_v = int(db_v)
    live_v = int(live_v)
    if abs(db_v - live_v) <= abs_tol:
        return True
    denom = max(1, (abs(db_v) + abs(live_v)) / 2.0)
    return abs(db_v - live_v) / denom <= pct_tol

# ==============================
# DEFINITIONS MAP
# ==============================
def build_def_map(symbol: str, start: datetime, end: datetime) -> dict:
    """
    (exp_date, strike, side) -> raw_symbol
    """
    defs = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols=[f"{symbol}.OPT"],
        start=start,
        end=end - timedelta(hours=24),
    )
    df = defs.to_df()
    if df is None or df.empty:
        return {}

    df = df.copy()
    df["exp_date"] = pd.to_datetime(df["expiration"]).dt.date
    df["strike_f"] = df["strike_price"].astype(float)
    df["side"] = df["instrument_class"].astype(str)
    df["raw_symbol"] = df["raw_symbol"].astype(str)

    df = df.dropna(subset=["exp_date", "strike_f", "side", "raw_symbol"])
    df = df.drop_duplicates(subset=["exp_date", "strike_f", "side"], keep="last")

    keys = list(zip(df["exp_date"].tolist(), df["strike_f"].tolist(), df["side"].tolist()))
    vals = df["raw_symbol"].tolist()
    return dict(zip(keys, vals))

# ==============================
# API PULLS (NO BATCH FILES)
# ==============================
def pull_cbbo_last_le_many(raw_symbols: list[str], ts: pd.Timestamp) -> dict:
    """
    raw_symbol -> (bid, ask) from last quote <= ts
    """
    if not raw_symbols:
        return {}

    ts = to_utc_ts(ts)
    start = (ts - pd.Timedelta(minutes=10)).to_pydatetime()
    end = (ts + pd.Timedelta(minutes=1)).to_pydatetime()

    safe_end = db_end_utc_day()
    if end.replace(tzinfo=timezone.utc) > safe_end:
        end = safe_end

    data = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="cbbo-1m",
        stype_in="raw_symbol",
        symbols=raw_symbols,
        start=start,
        end=end,
    )
    df = data.to_df()
    if df is None or df.empty:
        return {}

    tcol = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if tcol is None:
        return {}

    _ensure_utc_col(df, tcol)
    df = df[df[tcol] <= ts]
    if df.empty:
        return {}

    df = df.sort_values(["symbol", tcol])
    last_rows = df.groupby("symbol", as_index=False).tail(1)

    out = {}
    for _, r in last_rows.iterrows():
        rs = str(r["symbol"])
        bid = float(r.get("bid_px", 0) or 0)
        ask = float(r.get("ask_px", 0) or 0)
        out[rs] = (bid if bid > 0 else None, ask if ask > 0 else None)
    return out

def pull_trades_volume_window_many(raw_symbols: list[str], ts: pd.Timestamp) -> dict:
    """
    raw_symbol -> volume in ±5 minutes around ts (sum size)
    """
    if not raw_symbols:
        return {}

    ts = to_utc_ts(ts)
    start = (ts - pd.Timedelta(minutes=5)).to_pydatetime()
    end = (ts + pd.Timedelta(minutes=5)).to_pydatetime()

    safe_end = db_end_utc_day()
    if end.replace(tzinfo=timezone.utc) > safe_end:
        end = safe_end

    data = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="trades",
        stype_in="raw_symbol",
        symbols=raw_symbols,
        start=start,
        end=end,
    )
    df = data.to_df()
    if df is None or df.empty:
        return {}

    tcol = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if tcol is None or "size" not in df.columns:
        return {}

    _ensure_utc_col(df, tcol)
    g = df.groupby("symbol")["size"].sum()
    return {str(k): float(v) for k, v in g.items()}

def pull_oi_last_le_many(raw_symbols: list[str], ts: pd.Timestamp) -> dict:
    """
    raw_symbol -> open_interest from last stats row <= ts
    """
    if not raw_symbols:
        return {}

    ts = to_utc_ts(ts)
    start = (ts - pd.Timedelta(days=3)).to_pydatetime()
    end = (ts + pd.Timedelta(minutes=1)).to_pydatetime()

    safe_end = db_end_utc_day()
    if end.replace(tzinfo=timezone.utc) > safe_end:
        end = safe_end

    data = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="statistics",
        stype_in="raw_symbol",
        symbols=raw_symbols,
        start=start,
        end=end,
    )
    df = data.to_df()
    if df is None or df.empty:
        return {}

    tcol = "ts_event" if "ts_event" in df.columns else ("timestamp" if "timestamp" in df.columns else None)
    if tcol is None:
        return {}

    _ensure_utc_col(df, tcol)
    df = df[df[tcol] <= ts]
    if df.empty:
        return {}

    df = df.sort_values(["symbol", tcol])
    last_rows = df.groupby("symbol", as_index=False).tail(1)

    out = {}
    for _, r in last_rows.iterrows():
        rs = str(r["symbol"])
        oi = r.get("open_interest", None)
        out[rs] = None if (oi is None or pd.isna(oi)) else int(oi)
    return out

# ==============================
# IV RECOMPUTE
# ==============================
def bs_iv_from_mid(S: float, K: float, T_years: float, r: float, mid: float, side: str) -> float | None:
    if S <= 0 or K <= 0 or T_years <= 0 or mid is None or mid <= 0:
        return None

    def N(x):
        return 0.5 * (1 + math.erf(x / math.sqrt(2)))

    def price(sigma: float) -> float:
        d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T_years) / (sigma * math.sqrt(T_years))
        d2 = d1 - sigma * math.sqrt(T_years)
        if side == "C":
            return S * N(d1) - K * math.exp(-r * T_years) * N(d2)
        else:
            return K * math.exp(-r * T_years) * N(-d2) - S * N(-d1)

    lo, hi = 1e-6, 5.0
    for _ in range(60):
        m = 0.5 * (lo + hi)
        if price(m) > mid:
            hi = m
        else:
            lo = m
    return 0.5 * (lo + hi)

# ==============================
# MAIN
# ==============================
def main():
    symbol = SYMBOL.strip().upper()

    end_utc = db_end_utc_day()
    start_utc = end_utc - timedelta(days=DAYS_BACK)

    print(f"[INFO] validate symbol={symbol} days_back={DAYS_BACK}")
    print(f"[INFO] time window (UTC): start={start_utc.isoformat()} end={end_utc.isoformat()}")

    start_db = start_utc.replace(tzinfo=None)
    end_db = end_utc.replace(tzinfo=None)

    def_map = build_def_map(symbol, start_utc, end_utc)
    if not def_map:
        print("[FAIL] no OPRA definitions -> cannot map raw_symbol")
        return
    print(f"[INFO] def_map keys={len(def_map):,}")

    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        ts_df = con.execute(
            """
            WITH t AS (
              SELECT DISTINCT timestamp
              FROM option_snapshots_raw
              WHERE symbol = ?
                AND timestamp >= ?
                AND timestamp < ?
              ORDER BY timestamp
            ),
            u AS (
              SELECT timestamp, row_number() OVER (ORDER BY timestamp) AS rn
              FROM t
            )
            SELECT timestamp
            FROM u
            WHERE (rn - 1) % ? = 0
            """,
            [symbol, start_db, end_db, EVERY_NTH_TIMESTAMP],
        ).df()

        if ts_df is None or ts_df.empty:
            print("[FAIL] no timestamps found in DB for this symbol in this window")
            return

        timestamps = [to_utc_ts(x) for x in ts_df["timestamp"].tolist()]
        if MAX_CHECKS is not None:
            timestamps = timestamps[:MAX_CHECKS]
        print(f"[INFO] timestamps_to_check={len(timestamps)} (every {EVERY_NTH_TIMESTAMP}th)")

        pass_ct = 0
        fail_ct = 0

        for i, ts in enumerate(timestamps, start=1):
            ts = to_utc_ts(ts)
            ts_db = to_utc_naive_dt(ts)

            rows = con.execute(
                """
                SELECT
                  timestamp,
                  underlying_price,
                  strike,
                  call_put,
                  days_to_expiry,
                  expiration_date,
                  bid,
                  ask,
                  mid,
                  volume,
                  open_interest,
                  iv,
                  spread,
                  spread_pct
                FROM option_snapshots_raw
                WHERE symbol = ? AND timestamp = ?
                ORDER BY strike, call_put
                """,
                [symbol, ts_db],
            ).df()

            if rows is None or rows.empty:
                print(f"[{i:02d}] {ts.isoformat()} -> FAIL (no DB rows)")
                fail_ct += 1
                continue

            row_meta = []
            raw_symbols = []
            for _, r in rows.iterrows():
                exp_date = pd.Timestamp(r["expiration_date"]).date()
                strike = float(r["strike"])
                side = str(r["call_put"])
                rs = def_map.get((exp_date, strike, side))
                row_meta.append((rs, r))
                if rs is not None:
                    raw_symbols.append(rs)

            raw_symbols = sorted(set(raw_symbols))

            # API pulls for this timestamp
            live_quotes = pull_cbbo_last_le_many(raw_symbols, ts)
            live_vols = pull_trades_volume_window_many(raw_symbols, ts)
            live_ois = pull_oi_last_le_many(raw_symbols, ts)

            ok_all = True
            bad = []

            for rs, r in row_meta:
                if rs is None:
                    ok_all = False
                    bad.append(f"no_raw_symbol(strike={r['strike']} side={r['call_put']} exp={r['expiration_date']})")
                    continue

                lbid, lask = live_quotes.get(rs, (None, None))

                db_bid = to_float_or_none(r["bid"]) or None
                db_ask = to_float_or_none(r["ask"]) or None
                db_mid = to_float_or_none(r["mid"]) or None
                db_spr = to_float_or_none(r["spread"])
                db_sprp = to_float_or_none(r["spread_pct"])

                if db_bid == 0: db_bid = None
                if db_ask == 0: db_ask = None
                if db_mid == 0: db_mid = None

                lmid = lspr = lsprp = None
                if lbid is not None and lask is not None and lbid > 0 and lask > 0:
                    lmid = (lbid + lask) / 2.0
                    lspr = lask - lbid
                    lsprp = (lspr / lmid) if lmid else None

                if not (
                    ok_num(db_bid, lbid, BIDASK_ABS_TOL, BIDASK_PCT_TOL) and
                    ok_num(db_ask, lask, BIDASK_ABS_TOL, BIDASK_PCT_TOL) and
                    ok_num(db_mid, lmid, BIDASK_ABS_TOL, BIDASK_PCT_TOL) and
                    ok_num(db_spr, lspr, SPREAD_ABS_TOL, SPREAD_PCT_TOL) and
                    ok_num(db_sprp, lsprp, SPREADPCT_ABS_TOL, 10.0)
                ):
                    ok_all = False
                    bad.append(
                        f"quote_mismatch(rs={rs} db(bid,ask,mid)=({db_bid},{db_ask},{db_mid}) "
                        f"live(bid,ask,mid)=({lbid},{lask},{lmid}))"
                    )

                lvol = live_vols.get(rs, None)
                db_vol = to_int_or_none(r["volume"])
                if db_vol == 0:
                    db_vol = None
                if not ok_int(db_vol, lvol, VOLUME_ABS_TOL, VOLUME_PCT_TOL):
                    ok_all = False
                    bad.append(f"vol_mismatch(rs={rs} db={db_vol} live={lvol})")

                loi = live_ois.get(rs, None)
                db_oi = to_int_or_none(r["open_interest"])
                if not ok_int(db_oi, loi, OI_ABS_TOL, 0.0):
                    ok_all = False
                    bad.append(f"oi_mismatch(rs={rs} db={db_oi} live={loi})")

                db_iv = to_float_or_none(r["iv"])
                if db_iv == 0:
                    db_iv = None

                S = to_float_or_none(r["underlying_price"])
                K = float(r["strike"])
                dte = to_int_or_none(r["days_to_expiry"])
                side = str(r["call_put"])

                liv = None
                if S is not None and dte is not None and lmid is not None:
                    liv = bs_iv_from_mid(S=S, K=K, T_years=dte / 365.0, r=0.01, mid=lmid, side=side)

                if not ok_num(db_iv, liv, IV_ABS_TOL, IV_PCT_TOL):
                    ok_all = False
                    bad.append(f"iv_mismatch(rs={rs} db={db_iv} live={liv})")

            if ok_all:
                print(f"[{i:02d}] {ts.isoformat()} -> PASS")
                pass_ct += 1
            else:
                print(f"[{i:02d}] {ts.isoformat()} -> FAIL")
                for msg in bad[:5]:
                    print(f"      {msg}")
                if len(bad) > 5:
                    print(f"      ... +{len(bad)-5} more")
                fail_ct += 1

        print("\n========== DATABENTO VALIDATION SUMMARY ==========")
        print(f"symbol={symbol}")
        print(f"checked={len(timestamps)} pass={pass_ct} fail={fail_ct}")
        print("===============================================\n")

    finally:
        con.close()

if __name__ == "__main__":
    main()
