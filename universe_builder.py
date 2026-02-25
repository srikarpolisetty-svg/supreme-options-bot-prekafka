# universe_builder.py
# Builds the raw_symbol universe + (optional) strike_rows, and writes the raw_symbol list
# to a newline cache file atomically so the streamer can consume it without DB concurrency.

from __future__ import annotations

import time
import os
import datetime as dt
import pytz
from typing import Iterable

import pandas as pd
import yfinance as yf
import duckdb

from databasefunctions import get_sp500_symbols


# -------------------------
# CONFIG
# -------------------------
DEF_DB_PATH = "definitioncache.duckdb"
UNIVERSE_CACHE_PATH = "state/raw_universe_cache.txt"

BATCH_SIZE_YF = 40
SLEEP_SEC_YF = 5
INTERVAL_YF = "15m"

LOOKAHEAD_DAYS = 4

C1_MULT = 1.015
P1_MULT = 0.985
C2_MULT = 1.035
P2_MULT = 0.965


# -------------------------
# HELPERS
# -------------------------
def chunks(lst: list[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def utc_now_naive() -> dt.datetime:
    return dt.datetime.now(tz=pytz.UTC).replace(tzinfo=None)


def normalize_parent_symbol(sym: str) -> str:
    return sym.replace("-", "").replace(".", "")


def normalize_expiration_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def get_friday_within_lookahead(exp_yyyymmdd: list[str], today_utc: dt.date) -> str | None:
    for exp in sorted(exp_yyyymmdd):
        d = dt.datetime.strptime(exp, "%Y%m%d").date()
        if d.weekday() == 4 and 0 <= (d - today_utc).days <= LOOKAHEAD_DAYS and not (15 <= d.day <= 21):
            return exp
    return None


def closest_strike(target: float, strikes: list[float]) -> float:
    return float(min(strikes, key=lambda s: abs(float(s) - float(target))))


def pick_strikes(underlying_price: float, strikes: list[float]) -> dict[str, float]:
    atm = float(underlying_price)
    return {
        "ATM": closest_strike(atm, strikes),
        "C1": closest_strike(atm * C1_MULT, strikes),
        "P1": closest_strike(atm * P1_MULT, strikes),
        "C2": closest_strike(atm * C2_MULT, strikes),
        "P2": closest_strike(atm * P2_MULT, strikes),
    }


def pick_raw_symbol(sub: pd.DataFrame, strike: float, cp: str) -> str | None:
    tmp = sub[
        (sub["strike_price"].astype(float) == float(strike))
        & (sub["instrument_class"] == cp)
    ]["raw_symbol"]
    if tmp.empty:
        return None
    return str(tmp.iloc[0])


def write_universe_cache(desired_raws: list[str], path: str = UNIVERSE_CACHE_PATH) -> None:
    # Ensure parent directory exists
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        for r in desired_raws:
            f.write(r + "\n")

    os.replace(tmp_path, path)  # atomic swap on Linux


# -------------------------
# 1) UNDERLYING PRICES (yfinance, batched)
# -------------------------
def batch_underlying_last_close(
    symbols: list[str],
    start: dt.datetime,
    end: dt.datetime,
) -> dict[str, float]:
    out: dict[str, float] = {}

    for batch in chunks(symbols, BATCH_SIZE_YF):
        try:
            df = yf.download(
                tickers=batch,
                start=start,
                end=end,
                interval=INTERVAL_YF,
                group_by="ticker",
                progress=False,
                auto_adjust=False,
            )
        except Exception:
            time.sleep(15)
            continue

        if df is None or df.empty:
            time.sleep(SLEEP_SEC_YF)
            continue

        close = None
        try:
            close = df.xs("Close", axis=1, level=1)
        except Exception:
            try:
                close = df.xs("Close", axis=1, level=0)
            except Exception:
                time.sleep(SLEEP_SEC_YF)
                continue

        for sym in batch:
            if close is None or sym not in close.columns:
                continue
            s = close[sym].dropna()
            if len(s) == 0:
                continue
            out[sym] = float(s.iloc[-1])

        time.sleep(SLEEP_SEC_YF)

    return out


# -------------------------
# 2) UNIVERSE BUILDER (from definition_cache)
# -------------------------
def build_raw_symbol_universe_from_cache(
    symbols: list[str],
    underlying_px: dict[str, float],
    def_db_path: str = DEF_DB_PATH,
) -> tuple[list[str], list[tuple], dict[str, dict]]:
    today_utc = dt.datetime.now(tz=pytz.UTC).date()
    ts_refresh = utc_now_naive()

    con = duckdb.connect(def_db_path)

    all_raws: list[str] = []
    strike_rows: list[tuple] = []
    meta: dict[str, dict] = {}

    for sym in symbols:
        px = underlying_px.get(sym)
        if px is None or px <= 0:
            continue

        parent_sym = normalize_parent_symbol(sym)

        df = con.execute(
            """
            SELECT raw_symbol, expiration, instrument_class, strike_price
            FROM definition_cache
            WHERE symbol = ?
              AND underlying = ?
            """,
            [sym, parent_sym],
        ).fetchdf()

        if df is None or df.empty:
            continue

        df["expiration"] = normalize_expiration_utc_naive(df["expiration"])
        df = df.dropna(subset=["expiration", "raw_symbol", "instrument_class", "strike_price"])
        if df.empty:
            continue

        df["exp_yyyymmdd"] = df["expiration"].dt.strftime("%Y%m%d")
        expirations = sorted(df["exp_yyyymmdd"].unique().tolist())

        exp = get_friday_within_lookahead(expirations, today_utc)
        if exp is None:
            continue

        sub = df[df["exp_yyyymmdd"] == exp]
        if sub.empty:
            continue

        strikes = sorted(sub["strike_price"].astype(float).unique().tolist())
        if not strikes:
            continue

        strike_map = pick_strikes(float(px), strikes)

        atm = strike_map["ATM"]
        c1 = strike_map["C1"]
        p1 = strike_map["P1"]
        c2 = strike_map["C2"]
        p2 = strike_map["P2"]

        raw_atm_c = pick_raw_symbol(sub, atm, "C")
        raw_atm_p = pick_raw_symbol(sub, atm, "P")
        raw_c1 = pick_raw_symbol(sub, c1, "C")
        raw_p1 = pick_raw_symbol(sub, p1, "P")
        raw_c2 = pick_raw_symbol(sub, c2, "C")
        raw_p2 = pick_raw_symbol(sub, p2, "P")

        raws = [raw_atm_c, raw_atm_p, raw_c1, raw_p1, raw_c2, raw_p2]
        if any(x is None for x in raws):
            continue

        exp_date = dt.datetime.strptime(exp, "%Y%m%d").date()

        strike_rows.extend(
            [
                (sym, "ATM", "C", exp, exp_date, float(atm), raw_atm_c, float(px), ts_refresh),
                (sym, "ATM", "P", exp, exp_date, float(atm), raw_atm_p, float(px), ts_refresh),
                (sym, "C1", "C", exp, exp_date, float(c1), raw_c1, float(px), ts_refresh),
                (sym, "P1", "P", exp, exp_date, float(p1), raw_p1, float(px), ts_refresh),
                (sym, "C2", "C", exp, exp_date, float(c2), raw_c2, float(px), ts_refresh),
                (sym, "P2", "P", exp, exp_date, float(p2), raw_p2, float(px), ts_refresh),
            ]
        )

        all_raws.extend([r for r in raws if r is not None])
        meta[sym] = {
            "parent_sym": parent_sym,
            "exp": exp,
            "expiration_date": exp_date,
            "underlying_price": float(px),
            "strike_map": strike_map,
            "raws": raws,
        }

    con.close()

    seen = set()
    desired_raws: list[str] = []
    for r in all_raws:
        if r not in seen:
            seen.add(r)
            desired_raws.append(r)

    return desired_raws, strike_rows, meta


# -------------------------
# CLI / TEST RUN
# -------------------------
def main():
    now = dt.datetime.now(tz=pytz.UTC)
    safe_end = now - dt.timedelta(minutes=10)
    safe_start = safe_end - dt.timedelta(minutes=180)

    symbols = sorted(get_sp500_symbols())
    print("Total symbols:", len(symbols))

    print("Batch fetching underlying prices (yfinance)...")
    underlying_px = batch_underlying_last_close(symbols, start=safe_start, end=safe_end)
    print("Underlying prices found:", len(underlying_px))

    print("Building raw_symbol universe (from definition_cache)...")
    raw_symbols, strike_rows, meta = build_raw_symbol_universe_from_cache(symbols, underlying_px)
    print("Good underlyings:", len(meta))
    print("Total raw_symbols:", len(raw_symbols))
    print("Total strike_rows:", len(strike_rows))

    # NEW: write raw universe cache for streamer
    write_universe_cache(raw_symbols)
    print("Wrote universe cache:", UNIVERSE_CACHE_PATH)

    # NOTE: strike_rows/meta are still returned for any other use (e.g., live_strikes6_latest upsert elsewhere)


if __name__ == "__main__":
    main()