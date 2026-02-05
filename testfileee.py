import databento as db
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import DATABENTO_API_KEY
import math
import duckdb
import time
import operator
import pathlib

# ==============================
# CONFIG (edit these)
# ==============================
SYMBOL = "AAPL"
DAYS_BACK = 35
DB_PATH = "options_data.db"
BATCH_DIR = pathlib.Path("batch_downloads")

# Optional speed clips for testing:
MAX_HOURS = None
MAX_TS = None
SKIP_DB_INSERT = False  # True = perf-only run

POLL_S = 2.0            # batch job polling interval

# Databento batch symbol cap (hard limit)
MAX_SYMBOLS_PER_JOB = 2000

client = db.Historical(DATABENTO_API_KEY)

# ==============================
# TIME RANGE (avoid Databento "end after available end")
# ==============================
def db_end_utc_day() -> datetime:
    """
    Databento historical often seals availability at 00:00:00 UTC boundaries.
    Using 'now()' can exceed the available end and trigger 422.
    This clamps end to the start of the current UTC day.
    """
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

# ==============================
# TIMING UTIL
# ==============================
class Timer:
    def __init__(self):
        self.t0 = time.perf_counter()
        self.marks = []

    def mark(self, name: str):
        now = time.perf_counter()
        self.marks.append((name, now))
        return now

    def report(self):
        out = []
        last = self.t0
        for name, t in self.marks:
            out.append((name, t - last, t - self.t0))
            last = t
        return out

def fmt_s(x: float) -> str:
    if x < 1:
        return f"{x*1000:.1f}ms"
    return f"{x:.2f}s"

def _to_iso(x) -> str:
    return pd.Timestamp(x).to_pydatetime().isoformat()

def _ensure_utc_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns or df.empty:
        return
    s = df[col]
    # Always parse/convert -> UTC-aware
    df[col] = pd.to_datetime(s, utc=True, errors="coerce")

# ==============================
# DB
# ==============================
def ensure_table():
    con = duckdb.connect(DB_PATH)
    try:
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw (
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                underlying_price DOUBLE,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT
            );
        """)
    finally:
        con.close()

# ==============================
# BATCH DOWNLOAD
# ==============================
def batch_get_df(
    dataset: str,
    schema: str,
    symbols: list[str],
    start,
    end,
    *,
    stype_in: str,
    split_duration: str = "day",
    poll_s: float = 2.0,
) -> pd.DataFrame:
    """
    One batch job -> wait -> download -> load dbn -> to_df
    """
    if not symbols:
        return pd.DataFrame()

    print(f"[BATCH] submit dataset={dataset} schema={schema} symbols={len(symbols)} start={start} end={end}")
    job = client.batch.submit_job(
        dataset=dataset,
        start=_to_iso(start),
        end=_to_iso(end),
        symbols=symbols,
        schema=schema,
        split_duration=split_duration,
        stype_in=stype_in,
    )
    job_id = job["id"]
    print(f"[BATCH] job_id={job_id} submitted")

    while True:
        done_ids = set(map(operator.itemgetter("id"), client.batch.list_jobs("done")))
        if job_id in done_ids:
            break
        time.sleep(poll_s)

    out_dir = BATCH_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    files = client.batch.download(job_id=job_id, output_dir=out_dir)

    dbn_files = [f for f in files if str(f).endswith(".dbn.zst")]
    print(f"[BATCH] downloaded files={len(files)} dbn_zst={len(dbn_files)} -> {out_dir}")

    dfs = []
    for f in sorted(dbn_files):
        store = db.DBNStore.from_file(f)
        dfs.append(store.to_df())

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def chunk_symbols(symbols: list[str], chunk_size: int = MAX_SYMBOLS_PER_JOB):
    """Yield symbols in the biggest chunks allowed (minimal chunking)."""
    for i in range(0, len(symbols), chunk_size):
        yield symbols[i:i + chunk_size]

def batch_get_df_chunked(
    dataset: str,
    schema: str,
    symbols: list[str],
    start,
    end,
    *,
    stype_in: str,
    split_duration: str = "day",
    poll_s: float = 2.0,
) -> pd.DataFrame:
    """
    Minimal chunking:
    - If <= 2000 symbols: exactly one batch job
    - If > 2000: run ceil(n/2000) jobs, then concat
    """
    if not symbols:
        return pd.DataFrame()

    if len(symbols) <= MAX_SYMBOLS_PER_JOB:
        return batch_get_df(
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            start=start,
            end=end,
            stype_in=stype_in,
            split_duration=split_duration,
            poll_s=poll_s,
        )

    n_chunks = (len(symbols) + MAX_SYMBOLS_PER_JOB - 1) // MAX_SYMBOLS_PER_JOB
    print(f"[BATCH] symbol list too large ({len(symbols)}). Splitting into {n_chunks} chunk(s) of {MAX_SYMBOLS_PER_JOB}...")

    parts = []
    for idx, sym_chunk in enumerate(chunk_symbols(symbols, MAX_SYMBOLS_PER_JOB), start=1):
        print(f"[BATCH] chunk {idx}/{n_chunks}: symbols={len(sym_chunk)}")
        df_part = batch_get_df(
            dataset=dataset,
            schema=schema,
            symbols=sym_chunk,
            start=start,
            end=end,
            stype_in=stype_in,
            split_duration=split_duration,
            poll_s=poll_s,
        )
        parts.append(df_part)

    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

# ==============================
# HELPERS
# ==============================
def append_row(
    results, snapshot_id, ts, symbol, underlying_price, days_till_expiry, exp_date,
    time_decay_bucket, strike, bucket, side, bid, ask, mid, vol, oi, iv, spread, spread_pct
):
    results.append({
        "snapshot_id": snapshot_id,
        "timestamp": ts,
        "symbol": symbol,
        "underlying_price": underlying_price,
        "strike": strike,
        "call_put": side,
        "days_to_expiry": days_till_expiry,
        "expiration_date": exp_date,
        "moneyness_bucket": bucket,
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "volume": int(vol) if vol is not None else 0,
        "open_interest": int(oi) if oi is not None else None,
        "iv": iv,
        "spread": spread,
        "spread_pct": spread_pct,
        "time_decay_bucket": time_decay_bucket,
    })

def get_closest_strike(target: float, strikes: list[float]) -> float:
    if not strikes:
        raise RuntimeError("No strikes available.")
    return float(min(strikes, key=lambda s: abs(float(s) - float(target))))

def is_third_friday(d):
    return d.weekday() == 4 and (15 <= d.day <= 21)

def has_any_weekly_expiration(expirations: list[str]) -> bool:
    for exp in expirations:
        d = datetime.strptime(exp, "%Y%m%d").date()
        if d.weekday() == 4 and not is_third_friday(d):
            return True
    return False

def get_friday_within_4_days(expirations: list[str], now_date):
    for exp in sorted(expirations):
        d = datetime.strptime(exp, "%Y%m%d").date()
        if d.weekday() == 4 and 0 <= (d - now_date).days <= 4 and not is_third_friday(d):
            return exp
    return None

# ==============================
# ✅ NEW: Build only the raw_symbols you actually need
# ==============================
def build_needed_raw_symbols(
    data_10m: pd.DataFrame,
    df_defs: pd.DataFrame,
    strikes: list[float],
    expirations: list[str],
    *,
    max_ts: int | None = None,
) -> list[str]:
    """
    Pre-pass over your 10m timestamps using the SAME selection logic (ATM/±1.5/±3.5 + weekly expiry within 4 days).
    Map (strike, side, exp_date) -> raw_symbol via definitions.
    Returns a deduped sorted list of raw_symbols actually needed.
    """
    if data_10m is None or data_10m.empty:
        return []

    if max_ts is not None:
        data_10m = data_10m.head(max_ts)

    d = df_defs.copy()
    d["strike_f"] = d["strike_price"].astype(float)
    d["exp_date"] = pd.to_datetime(d["expiration"]).dt.date

    needed: set[str] = set()

    for ts, row in data_10m.iterrows():
        underlying_price = float(row["Close"])
        now_date = ts.date()

        exp = get_friday_within_4_days(expirations, now_date)
        if exp is None:
            continue

        exp_date = datetime.strptime(exp, "%Y%m%d").date()
        dte = (exp_date - now_date).days
        if dte <= 0:
            continue

        atm = get_closest_strike(underlying_price, strikes)
        c1 = get_closest_strike(underlying_price * 1.015, strikes)
        p1 = get_closest_strike(underlying_price * 0.985, strikes)
        c2 = get_closest_strike(underlying_price * 1.035, strikes)
        p2 = get_closest_strike(underlying_price * 0.965, strikes)

        strike_sides = [
            (atm, "C"), (atm, "P"),
            (c1, "C"), (p1, "P"),
            (c2, "C"), (p2, "P"),
        ]

        for strike, side in strike_sides:
            m = (
                (d["strike_f"] == float(strike)) &
                (d["instrument_class"] == side) &
                (d["exp_date"] == exp_date)
            )
            hit = d.loc[m, "raw_symbol"].head(1)
            if not hit.empty:
                needed.add(str(hit.iloc[0]))

    return sorted(needed)

# ==============================
# CORE QUOTE/TRADE/OI RESOLUTION
# ==============================
def get_contract_data_from_dfs(
    strike_sides, days_to_expiry, df_defs, exp_date, ts, underlying_price,
    mkt_df, trd_df, oi_df
):
    symbols = {}
    for strike, side in strike_sides:
        contract_row = df_defs[
            (df_defs["strike_price"].astype(float) == float(strike)) &
            (df_defs["instrument_class"] == side) &
            (pd.to_datetime(df_defs["expiration"]).dt.date == exp_date)
        ].head(1)
        symbols[(strike, side)] = None if contract_row.empty else contract_row.iloc[0]["raw_symbol"]

    mkt_tcol = "ts_event" if "ts_event" in mkt_df.columns else ("timestamp" if "timestamp" in mkt_df.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df.columns else ("timestamp" if "timestamp" in trd_df.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df.columns else ("timestamp" if "timestamp" in oi_df.columns else None)

    if mkt_tcol: _ensure_utc_col(mkt_df, mkt_tcol)
    if trd_tcol: _ensure_utc_col(trd_df, trd_tcol)
    if oi_tcol:  _ensure_utc_col(oi_df, oi_tcol)

    mkt_groups = dict(tuple(mkt_df.groupby("symbol"))) if not mkt_df.empty else {}
    trd_groups = dict(tuple(trd_df.groupby("symbol"))) if not trd_df.empty else {}
    oi_groups  = dict(tuple(oi_df.groupby("symbol")))  if not oi_df.empty  else {}

    out = {}

    for strike, side in strike_sides:
        rs = symbols.get((strike, side))

        bid = ask = mid = spread = spread_pct = iv = None
        volume = 0.0
        open_interest = None

        if rs is None:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        g_oi = oi_groups.get(rs)
        if g_oi is not None and not g_oi.empty:
            if oi_tcol:
                g_oi2 = g_oi[g_oi[oi_tcol] <= ts]
                if not g_oi2.empty:
                    open_interest = g_oi2.iloc[-1].get("open_interest", None)
            else:
                open_interest = g_oi.iloc[-1].get("open_interest", None)

        g_trd = trd_groups.get(rs)
        if g_trd is not None and not g_trd.empty and "size" in g_trd.columns and trd_tcol:
            mask = (g_trd[trd_tcol] >= ts - pd.Timedelta(minutes=5)) & (g_trd[trd_tcol] <= ts + pd.Timedelta(minutes=5))
            volume = float(g_trd.loc[mask, "size"].sum())
        else:
            volume = 0.0

        g_mkt = mkt_groups.get(rs)
        if g_mkt is None or g_mkt.empty or not mkt_tcol:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue

        last_row = g_mkt[g_mkt[mkt_tcol] <= ts].tail(1)
        if last_row.empty:
            out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)
            continue
        last = last_row.iloc[0]

        bid = float(last.get("bid_px", 0) or 0)
        ask = float(last.get("ask_px", 0) or 0)

        if bid and ask:
            mid = (bid + ask) / 2.0
            spread = ask - bid
            spread_pct = spread / mid if mid else None

        # IV via bisection on BS mid
        T = days_to_expiry / 365.0
        if mid and T > 0:
            S = float(underlying_price)
            K = float(strike)
            r = 0.01
            lo, hi = 1e-6, 5.0

            def N(x):
                return 0.5 * (1 + math.erf(x / math.sqrt(2)))

            def bs_price(sigma):
                d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
                d2 = d1 - sigma * math.sqrt(T)
                if side == "C":
                    return S * N(d1) - K * math.exp(-r * T) * N(d2)
                else:
                    return K * math.exp(-r * T) * N(-d2) - S * N(-d1)

            for _ in range(60):
                mid_sigma = 0.5 * (lo + hi)
                if bs_price(mid_sigma) > mid:
                    hi = mid_sigma
                else:
                    lo = mid_sigma
            iv = 0.5 * (lo + hi)

        out[(strike, side)] = (bid, ask, mid, open_interest, volume, iv, spread, spread_pct)

    return out

# ==============================
# UNDERLYING
# ==============================
def fetch_last_days(symbol: str, days: int):
    # Use same end boundary as Databento to keep timelines aligned
    end = db_end_utc_day()
    start = end - timedelta(days=days)

    df = yf.download(symbol, start=start, end=end, interval="5m", progress=False)
    if df is None or df.empty:
        return df

    # force UTC-aware index (UTC-only pipeline)
    df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

    # market-hours filter (temporary ET), then back to UTC
    df = df.tz_convert("US/Eastern")
    df = df.between_time("09:30", "16:00")
    df = df.tz_convert("UTC")
    return df

def quality_report(df: pd.DataFrame):
    if df is None or df.empty:
        print("[Q] no rows")
        return

    def pct(col):
        return 100.0 * df[col].notna().mean() if col in df.columns else 0.0

    print("[Q] non-null % "
          f"bid={pct('bid'):.1f} "
          f"ask={pct('ask'):.1f} "
          f"mid={pct('mid'):.1f} "
          f"iv={pct('iv'):.1f} "
          f"open_interest={pct('open_interest'):.1f}")

    if "spread_pct" in df.columns and df["spread_pct"].notna().any():
        s = df["spread_pct"].dropna()
        qs = s.quantile([0.1, 0.5, 0.9]).to_dict()
        print(f"[Q] spread_pct p10={qs.get(0.1):.4f} p50={qs.get(0.5):.4f} p90={qs.get(0.9):.4f}")

# ==============================
# MAIN TEST RUN
# ==============================
def main():
    ensure_table()

    T = Timer()
    symbol = SYMBOL.strip().upper()

    print(f"[INFO] TEST RUN symbol={symbol} days_back={DAYS_BACK}")
    print(f"[INFO] clips: MAX_HOURS={MAX_HOURS} MAX_TS={MAX_TS} SKIP_DB_INSERT={SKIP_DB_INSERT}")

    # Align end/start to Databento daily boundary (UTC-aware)
    end = db_end_utc_day()
    start = end - timedelta(days=DAYS_BACK)

    # 1) Underlying
    data = fetch_last_days(symbol, days=DAYS_BACK)
    T.mark("underlying_download")

    if data is None or data.empty:
        print(f"⏭️ {symbol}: no underlying data")
        return

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    # ensure UTC-aware
    data.index = pd.to_datetime(data.index, utc=True, errors="coerce")
    data_10m = data.resample("10min").last().dropna()
    T.mark("underlying_resample_10m")

    if data_10m.empty:
        print(f"⏭️ {symbol}: no 10m bars")
        return

    if MAX_TS is not None:
        data_10m = data_10m.head(MAX_TS)
        print(f"[INFO] clipped 10m bars -> {len(data_10m):,}")

    # 2) Definitions
    defs = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols=[f"{symbol}.OPT"],
        start=start,
        end=end - timedelta(hours=24),
    )
    df_defs = defs.to_df()
    T.mark("defs_download")

    if df_defs is None or df_defs.empty:
        print(f"⏭️ {symbol}: no options definitions")
        return

    strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
    strikes.sort()

    expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()
    if not has_any_weekly_expiration(expirations):
        print(f"⏭️ {symbol}: only monthly expirations -> skip")
        return

    raw_symbols_all = build_needed_raw_symbols(
        data_10m=data_10m,
        df_defs=df_defs,
        strikes=strikes,
        expirations=expirations,
        max_ts=MAX_TS,
    )

    if not raw_symbols_all:
        print(f"⏭️ {symbol}: no needed raw_symbols produced")
        return

    print(f"[INFO] defs rows={len(df_defs):,} strikes={len(strikes):,} expirations={len(expirations):,}")
    print(f"[INFO] raw_symbols_needed={len(raw_symbols_all):,} (filtered from full chain)")
    print(f"[INFO] databento window start={start.isoformat()} end={end.isoformat()}")

    # 3) Batch downloads (MINIMAL CHUNKING if needed)
    print(f"[INFO] batch downloading {DAYS_BACK}d for {symbol} ...")

    mkt_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="cbbo-1m",
        symbols=raw_symbols_all,
        start=start,
        end=end,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )
    T.mark("batch_cbbo_1m")

    trd_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="trades",
        symbols=raw_symbols_all,
        start=start,
        end=end,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )
    T.mark("batch_trades")

    oi_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="statistics",
        symbols=raw_symbols_all,
        start=start - pd.Timedelta(days=1),
        end=end,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )
    T.mark("batch_statistics")

    print(f"[INFO] downloaded rows: cbbo={len(mkt_df_all):,} trades={len(trd_df_all):,} stats={len(oi_df_all):,}")

    # normalize timestamp cols once (UTC-aware)
    mkt_tcol = "ts_event" if "ts_event" in mkt_df_all.columns else ("timestamp" if "timestamp" in mkt_df_all.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df_all.columns else ("timestamp" if "timestamp" in trd_df_all.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df_all.columns else ("timestamp" if "timestamp" in oi_df_all.columns else None)

    if mkt_tcol: _ensure_utc_col(mkt_df_all, mkt_tcol)
    if trd_tcol: _ensure_utc_col(trd_df_all, trd_tcol)
    if oi_tcol:  _ensure_utc_col(oi_df_all, oi_tcol)
    T.mark("timestamp_normalize_all")

    # 4) Main loop (hour windows)
    results = []
    hour_groups = list(data_10m.groupby(pd.Grouper(freq="1h")))
    if MAX_HOURS is not None:
        hour_groups = hour_groups[:MAX_HOURS]
        print(f"[INFO] clipped hours -> {len(hour_groups):,}")

    n_windows = 0
    n_ts_seen = 0
    n_ts_used = 0
    n_skips_no_exp = 0

    for window_start, window_df in hour_groups:
        if window_df.empty:
            continue
        window_end = window_start + pd.Timedelta(hours=1)
        n_windows += 1
        n_ts_seen += len(window_df)

        per_ts = {}
        for ts, row in window_df.iterrows():
            underlying_price = float(row["Close"])

            atm_strike = get_closest_strike(underlying_price, strikes)
            c1 = get_closest_strike(underlying_price * 1.015, strikes)
            p1 = get_closest_strike(underlying_price * 0.985, strikes)
            c2 = get_closest_strike(underlying_price * 1.035, strikes)
            p2 = get_closest_strike(underlying_price * 0.965, strikes)

            now_date = ts.date()
            exp = get_friday_within_4_days(expirations, now_date)
            if exp is None:
                n_skips_no_exp += 1
                continue

            exp_date = datetime.strptime(exp, "%Y%m%d").date()
            days_till_expiry = (exp_date - now_date).days
            if days_till_expiry <= 0:
                continue

            strike_sides = [
                (atm_strike, "C"), (atm_strike, "P"),
                (c1, "C"), (p1, "P"),
                (c2, "C"), (p2, "P"),
            ]
            per_ts[ts] = (underlying_price, days_till_expiry, exp_date, strike_sides)

        if not per_ts:
            continue
        n_ts_used += len(per_ts)

        # filter big dfs down to this hour
        if mkt_tcol:
            mkt_df_win = mkt_df_all[(mkt_df_all[mkt_tcol] >= window_start) & (mkt_df_all[mkt_tcol] < window_end)]
        else:
            mkt_df_win = pd.DataFrame()

        if trd_tcol:
            trd_df_win = trd_df_all[(trd_df_all[trd_tcol] >= window_start) & (trd_df_all[trd_tcol] < window_end)]
        else:
            trd_df_win = pd.DataFrame()

        if oi_tcol:
            oi_start = window_start - pd.Timedelta(days=1)
            oi_df_win = oi_df_all[(oi_df_all[oi_df_all.columns[0]] >= oi_start) & (oi_df_all[oi_tcol] < window_end)]
        else:
            oi_df_win = pd.DataFrame()

        for ts, (underlying_price, days_till_expiry, exp_date, strike_sides) in per_ts.items():
            if days_till_expiry <= 1:
                time_decay_bucket = "EXTREME"
            elif days_till_expiry <= 3:
                time_decay_bucket = "HIGH"
            elif days_till_expiry <= 7:
                time_decay_bucket = "MEDIUM"
            else:
                time_decay_bucket = "LOW"

            # UTC-only snapshot_id (explicit Z)
            ts_utc = pd.Timestamp(ts).tz_convert("UTC")
            snapshot_id = f"{symbol}_{ts_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"

            out = get_contract_data_from_dfs(
                strike_sides,
                days_till_expiry,
                df_defs,
                exp_date,
                ts,
                underlying_price,
                mkt_df_win,
                trd_df_win,
                oi_df_win,
            )

            for (strike, side) in strike_sides:
                bid, ask, mid, oi, vol, iv, spread, spread_pct = out.get(
                    (strike, side), (None, None, None, None, 0.0, None, None, None)
                )

                if strike == strike_sides[0][0]:
                    bucket = "ATM"
                elif strike in (strike_sides[2][0], strike_sides[3][0]):
                    bucket = "OTM_1"
                else:
                    bucket = "OTM_2"

                append_row(
                    results,
                    snapshot_id, ts, symbol, underlying_price,
                    days_till_expiry, exp_date, time_decay_bucket,
                    strike, bucket, side,
                    bid, ask, mid, vol, oi, iv, spread, spread_pct
                )

    T.mark("main_loop_done")

    if not results:
        print(f"⏭️ {symbol}: no rows produced")
        return

    df = pd.DataFrame(results)

    # FORCE UTC-only storage: DuckDB TIMESTAMP is tz-naive; store UTC-naive
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)

    inserted = 0
    if not SKIP_DB_INSERT:
        con = duckdb.connect(DB_PATH)
        try:
            con.register("df_view", df)
            cols = ",".join(df.columns)
            con.execute(f"INSERT INTO option_snapshots_raw({cols}) SELECT {cols} FROM df_view")
            con.unregister("df_view")
            inserted = len(df)
        finally:
            con.close()
    T.mark("db_insert")

    # SUMMARY
    total_s = time.perf_counter() - T.t0

    print("\n========== TEST SUMMARY ==========")
    print(f"symbol={symbol}")
    print(f"underlying_10m_bars={len(data_10m):,} hour_windows={n_windows:,} ts_seen={n_ts_seen:,} ts_used={n_ts_used:,} skips_no_exp={n_skips_no_exp:,}")
    print(f"downloaded_rows cbbo={len(mkt_df_all):,} trades={len(trd_df_all):,} stats={len(oi_df_all):,}")
    print(f"produced_rows={len(df):,} inserted_rows={inserted:,}")
    quality_report(df)

    print("\n[Timing]")
    for name, delta, since0 in T.report():
        print(f"  {name:26s}  +{fmt_s(delta):>8s}   total={fmt_s(since0):>8s}")
    print(f"  {'TOTAL':26s}  {'':>8s}   total={fmt_s(total_s):>8s}")
    print("==================================\n")

if __name__ == "__main__":
    main()
