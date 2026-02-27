# live_ingest_latest.py
# One file, two modes:
#   1) streamer: 24/7 single-writer that maintains latest state in LIVE DuckDB
#   2) snapshot: read-only job that reads LIVE DuckDB and writes Parquet outputs
#
# Streamer is kept lean:
#   - NO z-scores / NO parquet / NO IV / NO OI cache / NO dbfunctions except get_sp500_symbols
#   - Maintains ONE table: live_contract_latest (metadata + quote + rolling vol + optional OI)

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
SUB_BATCH_SIZE = 10
SUB_SLEEP_SEC = 0.25

VOL_WINDOW_SEC = 10 * 60
FLUSH_EVERY_SEC = 1.0

# universe refresh cadence (reload raw_symbol cache file, subscribe new)
REFRESH_SEC = 10 * 60

# NOTE: 24 is not a real hour. Keep this valid to avoid weird exits.
RESTART_HOUR_UTC = 23
RESTART_MIN_UTC = 10

DUCKDB_LIVE_PATH = "/home/ubuntu/supreme-options-bot-prekafka/live_state.duckdb"
RAW_UNIVERSE_CACHE_PATH = "/home/ubuntu/supreme-options-bot-prekafka/state/raw_universe_cache.txt"

# snapshot freshness gates (used by snapshot mode; harmless to keep here)
STRIKES_MAX_AGE_MIN = 20

# -------------------------
# TEST / DEBUG PRINTS
# -------------------------
DEBUG_PRINT_EVERY_SEC = 5.0  # how often to print queue/counters
DEBUG_PRINT_FIRST_N_RECORDS = 3  # print repr/dir of first N records to verify fields
DEBUG_DRAIN_MAX_PER_LOOP = 500  # process up to N records per loop tick

# -------------------------
# DATABENTO PRINTS (NEW)
# -------------------------
PRINT_DB_RECORDS = True            # master switch
PRINT_DB_ERRORS_ONLY = False       # if True, prints only ERROR/System-ish messages
PRINT_DB_EVERY_N = 1               # print 1 out of N records (set 1 to print everything)


# -------------------------
# DUCKDB (LIVE) TABLE (SINGLE)
# -------------------------
DDL_LIVE_CONTRACT_LATEST = """
CREATE TABLE IF NOT EXISTS live_contract_latest (
    raw_symbol       TEXT PRIMARY KEY,

    -- contract identity / routing (from universe cache)
    parent_symbol    TEXT,
    label            TEXT,         -- ATM, C1, P1, C2, P2
    instrument_class TEXT,         -- C or P
    exp_yyyymmdd     TEXT,
    expiration_date  DATE,
    strike_price     DOUBLE,
    underlying_price DOUBLE,
    ts_refresh       TIMESTAMP,

    -- latest quote (from cbbo-1m)
    ts_quote   TIMESTAMP,
    bid        DOUBLE,
    ask        DOUBLE,
    mid        DOUBLE,
    spread     DOUBLE,
    spread_pct DOUBLE,

    -- rolling volume (from trades)
    ts_vol     TIMESTAMP,
    vol10m     BIGINT,

    -- open interest (optional; if/when you stream it)
    oi_date        DATE,
    ts_oi          TIMESTAMP,
    open_interest  BIGINT
);
"""


def init_live_duckdb():
    con = duckdb.connect(DUCKDB_LIVE_PATH)
    con.execute(DDL_LIVE_CONTRACT_LATEST)
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


def load_universe_cache(path: str):
    """
    Expected per line (space-separated):
      parent_symbol label instrument_class exp_yyyymmdd strike_price root_symbol raw_symbol underlying_price ts_refresh...

    Example:
      BAC ATM C 20260227 52.0 BAC 260227C00052000 51.89 2026-02-26 17:55:30.757915
    """
    try:
        raws: list[str] = []
        strike_rows: list[tuple] = []
        seen_raw = set()
        seen_key = set()  # (parent, label, cp)

        with open(path, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln:
                    continue

                parts = ln.split()
                # Need at least up to underlying_price; ts may be 2 tokens (date time)
                if len(parts) < 8:
                    continue

                parent = parts[0]
                label = parts[1]
                cp = parts[2]  # "C" or "P"
                exp_yyyymmdd = parts[3]

                # strike
                try:
                    strike = float(parts[4])
                except Exception:
                    continue

                # parts[5] is root/underlying symbol
                root = parts[5]
                short_raw = parts[6]

                # ✅ FIX: Databento OPRA raw_symbol is root left-justified to width 6 + 15-char option code
                #    AAPL -> "AAPL  " + short_raw
                #    ABT  -> "ABT   " + short_raw
                raw = f"{root:<6}{short_raw}"

                try:
                    underlying_px = float(parts[7])
                except Exception:
                    underlying_px = None

                # ts_refresh: if present, parse; else set now
                ts_refresh = utc_now_naive()
                if len(parts) >= 10:
                    ts_refresh = to_utc_naive(parts[8] + " " + parts[9]) or ts_refresh
                elif len(parts) >= 9:
                    ts_refresh = to_utc_naive(parts[8]) or ts_refresh

                # expiration_date from exp_yyyymmdd
                expiration_date = None
                try:
                    expiration_date = dt.datetime.strptime(exp_yyyymmdd, "%Y%m%d").date()
                except Exception:
                    expiration_date = None

                if raw and raw not in seen_raw:
                    seen_raw.add(raw)
                    raws.append(raw)

                key = (parent, label, cp)
                if key in seen_key:
                    continue
                seen_key.add(key)

                # row aligns with upsert_meta VALUES order below
                strike_rows.append(
                    (
                        parent,
                        label,
                        cp,
                        exp_yyyymmdd,
                        expiration_date,
                        strike,
                        raw,
                        underlying_px,
                        ts_refresh,
                    )
                )

        return raws, strike_rows

    except FileNotFoundError:
        return [], []
    except Exception:
        return [], []


# -------------------------
# STREAMER: SUBSCRIBE + STREAM -> UPSERT LIVE TABLE
# -------------------------
def subscribe_raws(live: db.Live, raws: list[str]):
    for batch in chunks(raws, SUB_BATCH_SIZE):
        live.subscribe(dataset="OPRA.PILLAR", schema="cbbo-1m", symbols=batch, stype_in="raw_symbol")
        live.subscribe(dataset="OPRA.PILLAR", schema="trades", symbols=batch, stype_in="raw_symbol")
        time.sleep(SUB_SLEEP_SEC)


def stream_to_duckdb_latest(initial_raw_symbols: list[str], initial_strike_rows: list[tuple]):
    quote_latest: dict[str, dict] = {}
    vol_deques: dict[str, deque] = {}
    vol_latest: dict[str, dict] = {}

    # map instrument_id -> raw_symbol (SymbolMappingMsg carries it)
    inst_to_raw: dict[int, str] = {}

    con = duckdb.connect(DUCKDB_LIVE_PATH)

    upsert_meta = """
    INSERT INTO live_contract_latest (
      parent_symbol, label, instrument_class,
      exp_yyyymmdd, expiration_date, strike_price,
      raw_symbol, underlying_price, ts_refresh
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      parent_symbol=excluded.parent_symbol,
      label=excluded.label,
      instrument_class=excluded.instrument_class,
      exp_yyyymmdd=excluded.exp_yyyymmdd,
      expiration_date=excluded.expiration_date,
      strike_price=excluded.strike_price,
      underlying_price=excluded.underlying_price,
      ts_refresh=excluded.ts_refresh;
    """

    upsert_quote = """
    INSERT INTO live_contract_latest (raw_symbol, ts_quote, bid, ask, mid, spread, spread_pct)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_quote=excluded.ts_quote,
      bid=excluded.bid,
      ask=excluded.ask,
      mid=excluded.mid,
      spread=excluded.spread,
      spread_pct=excluded.spread_pct;
    """

    upsert_vol = """
    INSERT INTO live_contract_latest (raw_symbol, ts_vol, vol10m)
    VALUES (?, ?, ?)
    ON CONFLICT(raw_symbol) DO UPDATE SET
      ts_vol=excluded.ts_vol,
      vol10m=excluded.vol10m;
    """

    clear_old_labels = """
    UPDATE live_contract_latest
    SET
      parent_symbol=NULL,
      label=NULL,
      instrument_class=NULL,
      exp_yyyymmdd=NULL,
      expiration_date=NULL,
      strike_price=NULL,
      underlying_price=NULL,
      ts_refresh=NULL
    WHERE parent_symbol = ?
      AND label IN ('ATM','C1','P1','C2','P2')
      AND instrument_class IN ('C','P');
    """

    if initial_strike_rows:
        print(f"[DEBUG] Initial meta upsert rows={len(initial_strike_rows)}")
        con.executemany(upsert_meta, initial_strike_rows)

    live = db.Live(key=DATABENTO_API_KEY)

    record_q = deque(maxlen=200_000)

    callback_state = {"count": 0, "last_ts": time.time()}

    def _cb(rec):
        callback_state["count"] += 1
        callback_state["last_ts"] = time.time()
        record_q.append(rec)

        # ---- PRINT incoming Databento messages/errors
        if not PRINT_DB_RECORDS:
            return

        n = callback_state["count"]
        rtype = getattr(rec, "rtype", None)
        rs = str(rtype) if rtype is not None else ""

        # ErrorMsg details (print always)
        if type(rec).__name__ in ("ErrorMsg", "ErrMsg") or rs.endswith("ERROR"):
            try:
                print("[DB_ERROR]", rec)
                print("  msg =", getattr(rec, "msg", None) or getattr(rec, "text", None))
                print("  err =", getattr(rec, "err", None))
                print("  code =", getattr(rec, "code", None))
                print("  instrument_id =", getattr(rec, "instrument_id", None))
                print(
                    "  symbol =",
                    getattr(rec, "stype_in_symbol", None)
                    or getattr(rec, "stype_out_symbol", None),
                )
            except Exception:
                pass
            return

        # System-ish messages are useful
        is_system_like = rs.endswith("SYSTEM") or type(rec).__name__ == "SystemMsg"

        if PRINT_DB_ERRORS_ONLY:
            if is_system_like:
                print("[DB_MSG]", rec)
            return

        # Normal printing: 1 out of N, OR always print system messages
        if (PRINT_DB_EVERY_N <= 1) or ((n % PRINT_DB_EVERY_N) == 0) or is_system_like:
            print("[DB_MSG]", rec)

    def _cb_exc(exc: Exception):
        print("[DB_CALLBACK_EXCEPTION]", repr(exc))

    # IMPORTANT: match your working file
    live.add_callback(record_callback=_cb, exception_callback=_cb_exc)

    # -------------------------
    # DEBUG STATE
    # -------------------------
    last_debug_print = time.time()
    seen_records = 0
    seen_quotes = 0
    seen_trades = 0
    printed_records = 0

    subscribed: set[str] = set()
    last_refresh = 0.0
    last_flush = time.time()

    print("Subscribing initial raw_symbols:", len(initial_raw_symbols))
    print("[DEBUG] first few raws:", initial_raw_symbols[:5])
    if initial_raw_symbols:
        subscribe_raws(live, initial_raw_symbols)
        subscribed.update(initial_raw_symbols)
    else:
        print(f"WARNING: initial_raw_symbols empty (cache={RAW_UNIVERSE_CACHE_PATH}).")

    live.start()
    print("Live streaming started. Ctrl+C to stop.")
    print("[DEBUG] subscriptions active; waiting for records...")

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

                desired_raws, strike_rows = load_universe_cache(RAW_UNIVERSE_CACHE_PATH)
                print(f"[DEBUG] refresh: cache_raws={len(desired_raws)} cache_rows={len(strike_rows)} subscribed={len(subscribed)}")

                if strike_rows:
                    parents = sorted({row[0] for row in strike_rows})
                    print(f"[DEBUG] refresh: clearing old labels for parents={len(parents)} (sample={parents[:5]})")
                    con.executemany(clear_old_labels, [(p,) for p in parents])

                    print(f"[DEBUG] refresh: upserting meta rows={len(strike_rows)}")
                    con.executemany(upsert_meta, strike_rows)

                if not desired_raws:
                    print(f"Universe cache empty/missing: {RAW_UNIVERSE_CACHE_PATH}")
                else:
                    to_add = [r for r in desired_raws if r not in subscribed]
                    if to_add:
                        print(f"Subscribing NEW raws from cache: {len(to_add)} (sample={to_add[:5]})")
                        subscribe_raws(live, to_add)
                        subscribed.update(to_add)
                    else:
                        print("No new raws to add from cache.")

            now_sec = time.time()

            if (now_sec - last_debug_print) >= DEBUG_PRINT_EVERY_SEC:
                cb_count = callback_state["count"]
                cb_age = now_sec - callback_state["last_ts"]
                print(
                    f"[DEBUG] queue={len(record_q)} "
                    f"cb_count={cb_count} cb_age_sec={cb_age:.2f} "
                    f"seen_records={seen_records} quotes={seen_quotes} trades={seen_trades} "
                    f"quote_cache={len(quote_latest)} vol_cache={len(vol_latest)} "
                    f"subscribed={len(subscribed)}"
                )
                last_debug_print = now_sec

            drained = 0
            while record_q and drained < DEBUG_DRAIN_MAX_PER_LOOP:
                rec = record_q.popleft()
                drained += 1

                seen_records += 1

                if printed_records < DEBUG_PRINT_FIRST_N_RECORDS:
                    printed_records += 1
                    try:
                        raw_dbg = getattr(rec, "symbol", None) or getattr(rec, "raw_symbol", None)
                        print(f"[DEBUG] record#{printed_records} type={type(rec)} raw={raw_dbg}")
                        attrs = [a for a in dir(rec) if "px" in a or a in ("symbol", "raw_symbol", "ts_event", "size")]
                        print(f"[DEBUG] record#{printed_records} attrs_sample={attrs[:40]}")
                    except Exception:
                        pass

                # capture mapping (instrument_id -> raw_symbol) from SymbolMappingMsg
                rtype = getattr(rec, "rtype", None)
                if rtype is not None and str(rtype).endswith("SYMBOL_MAPPING"):
                    inst = getattr(rec, "instrument_id", None)
                    sym = getattr(rec, "stype_in_symbol", None) or getattr(rec, "stype_out_symbol", None)
                    if inst is not None and sym:
                        inst_to_raw[int(inst)] = str(sym)
                    continue

                # resolve raw_symbol via instrument_id mapping (cbbo/trades usually only have instrument_id)
                inst = getattr(rec, "instrument_id", None)
                raw = inst_to_raw.get(int(inst)) if inst is not None else None

                if raw:
                    ts_event = to_utc_naive(getattr(rec, "ts_event", None))

                    # use pretty_* fields so bid/ask are real floats
                    bid = getattr(rec, "pretty_bid_px_00", None)
                    ask = getattr(rec, "pretty_ask_px_00", None)

                    # Quote record (cbbo-1m)
                    if bid is not None or ask is not None:
                        seen_quotes += 1
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
                        # Trade record (trades) -> rolling vol
                        seen_trades += 1
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

                print(
                    f"flush quotes={len(quote_latest)} vol={len(vol_latest)} subscribed={len(subscribed)} "
                    f"(seen_records={seen_records} quotes={seen_quotes} trades={seen_trades} cb_count={callback_state['count']})"
                )

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

    raw_symbols, strike_rows = load_universe_cache(RAW_UNIVERSE_CACHE_PATH)
    print("Initial raw_symbols from cache:", len(raw_symbols))
    print("[DEBUG] Initial strike_rows:", len(strike_rows))
    if strike_rows:
        print("[DEBUG] First strike_row:", strike_rows[0])

    stream_to_duckdb_latest(raw_symbols, strike_rows)


if __name__ == "__main__":
    main()