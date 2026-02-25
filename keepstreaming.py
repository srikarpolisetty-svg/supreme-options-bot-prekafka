# live_ingest_latest.py
# One file, two modes:
#   1) streamer: 24/7 single-writer that maintains latest state in LIVE DuckDB
#   2) snapshot: read-only job that reads LIVE DuckDB and writes Parquet outputs
#
# Streamer is kept lean:
#   - NO z-scores / NO parquet / NO IV / NO OI cache / NO dbfunctions except get_sp500_symbols
#   - Only maintains: live_quotes_latest, live_vol10m_latest, live_strikes6_latest, live_oi_latest,
#                     live_sixpack_ready view

from __future__ import annotations

import time
import argparse
import datetime as dt
import datetime
import pytz
from collections import deque

import pandas as pd
import databento as db
import duckdb

from config import DATABENTO_API_KEY


# -------------------------
# CONFIG
# -------------------------
SUB_BATCH_SIZE = 200
SUB_SLEEP_SEC = 0.25

VOL_WINDOW_SEC = 10 * 60
FLUSH_EVERY_SEC = 1.0

# universe refresh cadence (reload raw_symbol cache file, subscribe new)
REFRESH_SEC = 10 * 60

RESTART_HOUR_UTC = 21
RESTART_MIN_UTC = 10

DUCKDB_LIVE_PATH = "live_state.duckdb"

# raw_symbol universe cache written by universe_builder (newline-delimited)
RAW_UNIVERSE_CACHE_PATH = "state/raw_universe_cache.txt"

# snapshot freshness gates (used by snapshot mode; harmless to keep here)
STRIKES_MAX_AGE_MIN = 20


# -------------------------
# DUCKDB (LIVE) TABLES
# -------------------------
DDL_LIVE_QUOTES = """
CREATE TABLE IF NOT EXISTS live_quotes_latest (
    raw_symbol TEXT PRIMARY KEY,
    ts_event   TIMESTAMP,
    bid        DOUBLE,
    ask        DOUBLE,
    mid        DOUBLE,
    spread     DOUBLE,
    spread_pct DOUBLE
);
"""

DDL_LIVE_VOL = """
CREATE TABLE IF NOT EXISTS live_vol10m_latest (
    raw_symbol TEXT PRIMARY KEY,
    ts_calc    TIMESTAMP,
    vol10m     BIGINT
);
"""

DDL_LIVE_STRIKES6 = """
CREATE TABLE IF NOT EXISTS live_strikes6_latest (
    parent_symbol    TEXT,
    label            TEXT,       -- ATM, C1, P1, C2, P2
    instrument_class TEXT,       -- C or P
    exp_yyyymmdd     TEXT,
    expiration_date  DATE,
    strike_price     DOUBLE,
    raw_symbol       TEXT,
    underlying_price DOUBLE,
    ts_refresh       TIMESTAMP,
    PRIMARY KEY(parent_symbol, label, instrument_class)
);
"""

DDL_LIVE_OI = """
CREATE TABLE IF NOT EXISTS live_oi_latest (
    raw_symbol     TEXT PRIMARY KEY,
    oi_date        DATE,        -- as-of date for OI (typically yesterday UTC)
    ts_oi          TIMESTAMP,
    open_interest  BIGINT
);
"""

DDL_VIEW_SIXPACK_READY = """
CREATE VIEW IF NOT EXISTS live_sixpack_ready AS
SELECT
  s.parent_symbol AS symbol,
  s.label,
  s.instrument_class,
  s.exp_yyyymmdd,
  s.expiration_date,
  s.strike_price,
  s.raw_symbol,
  s.underlying_price,
  s.ts_refresh,

  q.ts_event,
  q.bid, q.ask, q.mid, q.spread, q.spread_pct,

  v.ts_calc,
  v.vol10m,

  o.oi_date,
  o.ts_oi,
  o.open_interest
FROM live_strikes6_latest s
LEFT JOIN live_quotes_latest q ON q.raw_symbol = s.raw_symbol
LEFT JOIN live_vol10m_latest v ON v.raw_symbol = s.raw_symbol
LEFT JOIN live_oi_latest o ON o.raw_symbol = s.raw_symbol;
"""


def init_live_duckdb():
    con = duckdb.connect(DUCKDB_LIVE_PATH)
    con.execute(DDL_LIVE_QUOTES)
    con.execute(DDL_LIVE_VOL)
    con.execute(DDL_LIVE_STRIKES6)
    con.execute(DDL_LIVE_OI)
    con.execute(DDL_VIEW_SIXPACK_READY)
    con.close()


# -------------------------
# HELPERS
# -------------------------
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def utc_now_naive() -> datetime.datetime:
    return dt.datetime.now(tz=pytz.UTC).replace(tzinfo=None)


def to_utc_naive(ts) -> datetime.datetime | None:
    if ts is None:
        return None
    try:
        t = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.isna(t):
            return None
        return t.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def compute_quote_fields(bid: float | None, ask: float | None):
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
        spread = ask - bid
        spread_pct = (spread / mid) if mid > 0 else None
        return mid, spread, spread_pct
    return None, None, None


def load_raw_universe_cache(path: str) -> list[str]:
    """
    Reads newline-delimited raw_symbol universe written atomically by universe_builder.
    Returns [] if missing/empty.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            raws = [ln.strip() for ln in f if ln.strip()]
        # de-dupe preserving order
        seen = set()
        out = []
        for r in raws:
            if r not in seen:
                seen.add(r)
                out.append(r)
        return out
    except FileNotFoundError:
        return []
    except Exception:
        return []


# -------------------------
# STREAMER: SUBSCRIBE + STREAM -> UPSERT LIVE TABLES
# -------------------------
def subscribe_raws(live: db.Live, raws: list[str]):
    for batch in chunks(raws, SUB_BATCH_SIZE):
        live.subscribe(dataset="OPRA.PILLAR", schema="cbbo-1m", symbols=batch, stype_in="raw_symbol")
        live.subscribe(dataset="OPRA.PILLAR", schema="trades", symbols=batch, stype_in="raw_symbol")
        time.sleep(SUB_SLEEP_SEC)


def stream_to_duckdb_latest(initial_raw_symbols: list[str]):
    quote_latest: dict[str, dict] = {}
    vol_deques: dict[str, deque] = {}
    vol_latest: dict[str, dict] = {}

    con = duckdb.connect(DUCKDB_LIVE_PATH)

    upsert_quote = """
    INSERT INTO live_quotes_latest (raw_symbol, ts_event, bid, ask, mid, spread, spread_pct)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_event=excluded.ts_event,
      bid=excluded.bid,
      ask=excluded.ask,
      mid=excluded.mid,
      spread=excluded.spread,
      spread_pct=excluded.spread_pct;
    """

    upsert_vol = """
    INSERT INTO live_vol10m_latest (raw_symbol, ts_calc, vol10m)
    VALUES (?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_calc=excluded.ts_calc,
      vol10m=excluded.vol10m;
    """

    live = db.Live(key=DATABENTO_API_KEY)

    # --- CHANGE: use callback queue (Databento Live has no next_record) ---
    record_q = deque(maxlen=200_000)
    live.add_callback(lambda rec: record_q.append(rec))

    subscribed: set[str] = set()
    last_refresh = 0.0
    last_flush = time.time()

    print("Subscribing initial raw_symbols:", len(initial_raw_symbols))
    if initial_raw_symbols:
        subscribe_raws(live, initial_raw_symbols)
        subscribed.update(initial_raw_symbols)
    else:
        print(f"WARNING: initial_raw_symbols empty (cache={RAW_UNIVERSE_CACHE_PATH}).")

    live.start()
    print("Live streaming started. Ctrl+C to stop.")

    try:
        while True:
            now_utc = dt.datetime.now(dt.timezone.utc)
            if (
                now_utc.hour > RESTART_HOUR_UTC
                or (now_utc.hour == RESTART_HOUR_UTC and now_utc.minute >= RESTART_MIN_UTC)
            ):
                print("Nightly UTC restart time reached. Exiting loop...")
                break

            if (time.time() - last_refresh) >= REFRESH_SEC:
                last_refresh = time.time()

                desired_raws = load_raw_universe_cache(RAW_UNIVERSE_CACHE_PATH)
                if not desired_raws:
                    print(f"Universe cache empty/missing: {RAW_UNIVERSE_CACHE_PATH}")
                else:
                    to_add = [r for r in desired_raws if r not in subscribed]
                    if to_add:
                        print(f"Subscribing NEW raws from cache: {len(to_add)}")
                        subscribe_raws(live, to_add)
                        subscribed.update(to_add)
                    else:
                        print("No new raws to add from cache.")

            # --- CHANGE: pop from callback queue instead of live.next_record() ---
            rec = record_q.popleft() if record_q else None
            now_sec = time.time()

            if rec is not None:
                raw = getattr(rec, "symbol", None) or getattr(rec, "raw_symbol", None)
                if raw:
                    ts_event = to_utc_naive(getattr(rec, "ts_event", None))

                    bid = getattr(rec, "bid_px_00", None)
                    ask = getattr(rec, "ask_px_00", None)

                    if bid is not None or ask is not None:
                        bid_f = float(bid) if bid is not None and bid > 0 else None
                        ask_f = float(ask) if ask is not None and ask > 0 else None
                        mid, spread, spread_pct = compute_quote_fields(bid_f, ask_f)

                        quote_latest[raw] = {
                            "ts_event": ts_event,
                            "bid": bid_f,
                            "ask": ask_f,
                            "mid": mid,
                            "spread": spread,
                            "spread_pct": spread_pct,
                        }
                    else:
                        size = getattr(rec, "size", None)
                        if size is not None:
                            try:
                                sz = int(size)
                            except Exception:
                                sz = 0
                            dq = vol_deques.get(raw)
                            if dq is None:
                                dq = deque()
                                vol_deques[raw] = dq
                            dq.append((now_sec, sz))

            cutoff = now_sec - VOL_WINDOW_SEC
            for raw, dq in list(vol_deques.items()):
                while dq and dq[0][0] < cutoff:
                    dq.popleft()
                vol10m = int(sum(sz for _, sz in dq)) if dq else 0
                vol_latest[raw] = {"ts_calc": utc_now_naive(), "vol10m": vol10m}

            if (time.time() - last_flush) >= FLUSH_EVERY_SEC:
                last_flush = time.time()

                if quote_latest:
                    rows = [
                        (
                            raw,
                            q["ts_event"] or utc_now_naive(),
                            q["bid"],
                            q["ask"],
                            q["mid"],
                            q["spread"],
                            q["spread_pct"],
                        )
                        for raw, q in quote_latest.items()
                    ]
                    con.executemany(upsert_quote, rows)

                if vol_latest:
                    rows = [
                        (raw, v["ts_calc"] or utc_now_naive(), int(v["vol10m"]))
                        for raw, v in vol_latest.items()
                    ]
                    con.executemany(upsert_vol, rows)

                print(f"flush quotes={len(quote_latest)} vol={len(vol_latest)} subscribed={len(subscribed)}")

    except KeyboardInterrupt:
        print("Stopping (KeyboardInterrupt)...")
    finally:
        try:
            live.stop()
        except Exception:
            pass
        try:
            live.close()
        except Exception:
            pass
        try:
            con.close()
        except Exception:
            pass


# -------------------------
# SNAPSHOT MODE (placeholder)
# -------------------------


# -------------------------
# MAIN
# -------------------------
def main():
    init_live_duckdb()

    raw_symbols = load_raw_universe_cache(RAW_UNIVERSE_CACHE_PATH)
    print("Initial raw_symbols from cache:", len(raw_symbols))

    stream_to_duckdb_latest(raw_symbols)


if __name__ == "__main__":
    main()