import databento as db
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import DATABENTO_API_KEY
import math
import duckdb
import argparse
import zlib
import time
import operator
import pathlib

from databasefunctions import get_sp500_symbols

client = db.Historical(DATABENTO_API_KEY)

DB_PATH = "options_data.db"
BATCH_DIR = pathlib.Path("batch_downloads")

# Databento batch symbol cap (hard limit)
MAX_SYMBOLS_PER_JOB = 2000
POLL_S = 2.0


# ---------- TIME RANGE (Databento-safe end boundary) ----------
def db_end_utc_day() -> datetime:
    """
    Databento historical often seals availability at 00:00:00 UTC boundaries.
    Using 'now()' can exceed the available end and trigger 422.
    Clamp end to the start of the current UTC day.
    """
    return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


# ---------- DB ----------
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


# ---------- SHARDING ----------
def stable_shard(symbol: str, n_shards: int) -> int:
    return zlib.crc32(symbol.encode("utf-8")) % n_shards


# ---------- BATCH DOWNLOAD ----------
def _to_iso(x) -> str:
    return pd.Timestamp(x).to_pydatetime().isoformat()

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
    submit batch job -> wait -> download -> load dbn -> to_df
    """
    if not symbols:
        return pd.DataFrame()

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

    while True:
        done_ids = set(map(operator.itemgetter("id"), client.batch.list_jobs("done")))
        if job_id in done_ids:
            break
        time.sleep(poll_s)

    out_dir = BATCH_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)
    files = client.batch.download(job_id=job_id, output_dir=out_dir)

    dfs = []
    for f in sorted(files):
        if str(f).endswith(".dbn.zst"):
            store = db.DBNStore.from_file(f)
            dfs.append(store.to_df())

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def chunk_symbols(symbols: list[str], chunk_size: int = MAX_SYMBOLS_PER_JOB):
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


# ---------- HELPERS ----------
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

def _ensure_utc_col(df: pd.DataFrame, col: str) -> None:
    if df is None or df.empty or col not in df.columns:
        return
    # Always parse/convert -> UTC-aware
    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

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
        if (
            d.weekday() == 4 and
            0 <= (d - now_date).days <= 4 and
            not is_third_friday(d)
        ):
            return exp
    return None


# ---------- ✅ NEW: Build only the raw_symbols you actually need ----------
def build_needed_raw_symbols(
    data_10m: pd.DataFrame,
    df_defs: pd.DataFrame,
    strikes: list[float],
    expirations: list[str],
) -> list[str]:
    """
    Pre-pass over your 10m timestamps using the SAME selection logic (ATM/±1.5/±3.5 + weekly expiry within 4 days).
    Map (strike, side, exp_date) -> raw_symbol via definitions.
    Returns a deduped sorted list of raw_symbols actually needed.
    """
    if data_10m is None or data_10m.empty:
        return []

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


# ---------- CORE QUOTE/TRADE/OI RESOLUTION ----------
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
    oi_groups = dict(tuple(oi_df.groupby("symbol"))) if not oi_df.empty else {}
    dict(tuple(oi_df.groupby("symbol"))) if not oi_df.empty else {}

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
        if g_oi is not None and not g_oi.empty and oi_tcol:
            g_oi2 = g_oi[g_oi[oi_tcol] <= ts]
            if not g_oi2.empty:
                open_interest = g_oi2.iloc[-1].get("open_interest", None)

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


# ---------- UNDERLYING ----------
def fetch_last_days(symbol: str, days: int):
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


# ---------- ONE SYMBOL ----------
def run_symbol(symbol: str, days_back: int = 35):
    data = fetch_last_days(symbol, days_back)
    if data is None or data.empty:
        print(f"⏭️ {symbol}: no underlying data")
        return

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    # ensure UTC-aware
    data.index = pd.to_datetime(data.index, utc=True, errors="coerce")
    data_10m = data.resample("10min").last().dropna()
    if data_10m.empty:
        print(f"⏭️ {symbol}: no 10m bars")
        return

    end = db_end_utc_day()
    start = end - timedelta(days=days_back)

    # Definitions
    defs = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="definition",
        stype_in="parent",
        symbols=[f"{symbol}.OPT"],
        start=start,
        end=end - timedelta(hours=24),
    )
    df_defs = defs.to_df()
    if df_defs is None or df_defs.empty:
        print(f"⏭️ {symbol}: no options definitions")
        return

    strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
    strikes.sort()

    expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()
    if not has_any_weekly_expiration(expirations):
        print(f"⏭️ {symbol}: only monthly expirations -> skip")
        return

    # ✅ IMPORTANT: only download raw_symbols we will actually use
    raw_symbols_needed = build_needed_raw_symbols(
        data_10m=data_10m,
        df_defs=df_defs,
        strikes=strikes,
        expirations=expirations,
    )
    if not raw_symbols_needed:
        print(f"⏭️ {symbol}: no needed raw_symbols produced")
        return

    print(f"[INFO] {symbol}: batch downloading {days_back}d | raw_symbols_needed={len(raw_symbols_needed):,}")

    # ✅ chunked batch downloads
    mkt_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="cbbo-1m",
        symbols=raw_symbols_needed,
        start=start,
        end=end,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )

    trd_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="trades",
        symbols=raw_symbols_needed,
        start=start,
        end=end,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )

    oi_df_all = batch_get_df_chunked(
        dataset="OPRA.PILLAR",
        schema="statistics",
        symbols=raw_symbols_needed,
        start=start - pd.Timedelta(days=1),
        end=end,
        stype_in="raw_symbol",
        split_duration="day",
        poll_s=POLL_S,
    )

    # timestamp columns
    mkt_tcol = "ts_event" if "ts_event" in mkt_df_all.columns else ("timestamp" if "timestamp" in mkt_df_all.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df_all.columns else ("timestamp" if "timestamp" in trd_df_all.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df_all.columns else ("timestamp" if "timestamp" in oi_df_all.columns else None)

    if mkt_tcol: _ensure_utc_col(mkt_df_all, mkt_tcol)
    if trd_tcol: _ensure_utc_col(trd_df_all, trd_tcol)
    if oi_tcol:  _ensure_utc_col(oi_df_all, oi_tcol)

    results = []

    for window_start, window_df in data_10m.groupby(pd.Grouper(freq="1h")):
        if window_df.empty:
            continue
        window_end = window_start + pd.Timedelta(hours=1)

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
            # ✅ IMPORTANT: filter using the ACTUAL oi_tcol (not oi_df_all.columns[0])
            oi_df_win = oi_df_all[(oi_df_all[oi_tcol] >= oi_start) & (oi_df_all[oi_tcol] < window_end)]
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

            # ✅ UTC-only snapshot_id with explicit Z
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

    if not results:
        print(f"⏭️ {symbol}: no rows produced")
        return

    df = pd.DataFrame(results)

    # ✅ FORCE UTC-only storage: DuckDB TIMESTAMP is tz-naive; store UTC-naive
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)

    con = duckdb.connect(DB_PATH)
    try:
        con.register("df_view", df)
        cols = ",".join(df.columns)
        con.execute(f"INSERT INTO option_snapshots_raw({cols}) SELECT {cols} FROM df_view")
        con.unregister("df_view")
    finally:
        con.close()

    print(f"✅ {symbol}: inserted {len(df):,} rows")


# ---------- MAIN ----------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--n-shards", type=int, required=True)
    parser.add_argument("--days-back", type=int, default=35)
    args = parser.parse_args()

    ensure_table()

    symbols = get_sp500_symbols()
    symbols = [s.strip().upper() for s in symbols if s and isinstance(s, str)]
    my_symbols = [s for s in symbols if stable_shard(s, args.n_shards) == args.shard_id]

    print(f"[INFO] shard={args.shard_id}/{args.n_shards} symbols={len(my_symbols)} days_back={args.days_back}")

    for sym in my_symbols:
        try:
            run_symbol(sym, days_back=args.days_back)
        except Exception as e:
            print(f"❌ {sym}: error -> {e}")


if __name__ == "__main__":
    main()
