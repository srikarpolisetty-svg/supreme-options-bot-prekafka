import databento as db
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone
from config import DATABENTO_API_KEY
import math
import duckdb
import time

client = db.Historical(DATABENTO_API_KEY)
symbol = "AAPL"


def append(
    results, snapshot_id, ts, symbol, underlying_price, days_till_expiry, exp_date, time_decay_bucket,
    strike, bucket, side, bid, ask, mid, vol, oi, iv, spread, spread_pct
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
        "volume": vol,
        "open_interest": oi,
        "iv": iv,
        "spread": spread,
        "spread_pct": spread_pct,
        "time_decay_bucket": time_decay_bucket,
    })


def _ensure_utc_col(df: pd.DataFrame, col: str) -> None:
    """Make df[col] tz-aware UTC if it exists and isn't already."""
    if col not in df.columns or df.empty:
        return
    s = df[col]
    try:
        if getattr(s.dt, "tz", None) is not None:
            return
    except Exception:
        pass
    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")


def get_contract_data_from_dfs(
    strike_sides, days_to_expiry, df_defs, exp_date, ts, underlying_price,
    mkt_df, trd_df, oi_df
):
    # Resolve strike/side -> raw_symbol (no API)
    symbols = {}
    for strike, side in strike_sides:
        contract_row = df_defs[
            (df_defs["strike_price"].astype(float) == float(strike)) &
            (df_defs["instrument_class"] == side) &
            (pd.to_datetime(df_defs["expiration"]).dt.date == exp_date)
        ].head(1)

        symbols[(strike, side)] = None if contract_row.empty else contract_row.iloc[0]["raw_symbol"]

    # Choose timestamp columns + normalize to UTC for comparisons
    mkt_tcol = "ts_event" if "ts_event" in mkt_df.columns else ("timestamp" if "timestamp" in mkt_df.columns else None)
    trd_tcol = "ts_event" if "ts_event" in trd_df.columns else ("timestamp" if "timestamp" in trd_df.columns else None)
    oi_tcol  = "ts_event" if "ts_event" in oi_df.columns else ("timestamp" if "timestamp" in oi_df.columns else None)

    if mkt_tcol:
        _ensure_utc_col(mkt_df, mkt_tcol)
    if trd_tcol:
        _ensure_utc_col(trd_df, trd_tcol)
    if oi_tcol:
        _ensure_utc_col(oi_df, oi_tcol)

    # Group once per window
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

        # --- OI: last known <= ts ---
        g_oi = oi_groups.get(rs)
        if g_oi is not None and not g_oi.empty:
            if oi_tcol:
                g_oi2 = g_oi[g_oi[oi_tcol] <= ts]
                if not g_oi2.empty:
                    open_interest = g_oi2.iloc[-1].get("open_interest", None)
            else:
                open_interest = g_oi.iloc[-1].get("open_interest", None)

        # --- Volume: sum trades in ±5 min window around ts ---
        g_trd = trd_groups.get(rs)
        if g_trd is not None and not g_trd.empty and "size" in g_trd.columns and trd_tcol:
            mask = (g_trd[trd_tcol] >= ts - pd.Timedelta(minutes=5)) & (g_trd[trd_tcol] <= ts + pd.Timedelta(minutes=5))
            volume = float(g_trd.loc[mask, "size"].sum())
        else:
            volume = 0.0

        # --- Quote: last known quote <= ts ---
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

        # --- IV (unchanged) ---
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

def fetch_last_35_days():
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=35)

    df = yf.download(symbol, start=start, end=end, interval="5m", progress=False)

    # ensure timezone
    df.index = pd.to_datetime(df.index, utc=True)

    # convert to US/Eastern (market hours are in ET)
    df = df.tz_convert("US/Eastern")

    # keep only 9:30–16:00
    df = df.between_time("09:30", "16:00")

    # convert back to UTC so rest of your code stays consistent
    df = df.tz_convert("UTC")

    return df



# -------------------- Underlying bars --------------------
t_all0 = time.perf_counter()

t0 = time.perf_counter()
data = fetch_last_35_days()
print(f"[TIMING] yfinance fetch: {time.perf_counter() - t0:.2f}s | rows={len(data)}")

if isinstance(data.columns, pd.MultiIndex):
    data.columns = data.columns.get_level_values(0)

data.index = pd.to_datetime(data.index, utc=True)
data_10m = data.resample("10min").last().dropna()
print(f"[INFO] data_10m rows={len(data_10m)}")

def get_closest_strike(target: float, strikes: list[float]) -> float:
    if not strikes:
        raise RuntimeError("No strikes available.")
    return float(min(strikes, key=lambda s: abs(float(s) - float(target))))


# -------------------- Definitions --------------------
t0 = time.perf_counter()
end = datetime.now(timezone.utc)
start = end - timedelta(days=35)

defs = client.timeseries.get_range(
    dataset="OPRA.PILLAR",
    schema="definition",
    stype_in="parent",
    symbols=[f"{symbol}.OPT"],
    start=start,
    end=end - timedelta(hours=24),
)

df_defs = defs.to_df()
print(f"[TIMING] definition fetch+to_df: {time.perf_counter() - t0:.2f}s | rows={len(df_defs)}")

strikes = df_defs["strike_price"].dropna().astype(float).unique().tolist()
strikes.sort()

expirations = pd.to_datetime(df_defs["expiration"]).dt.strftime("%Y%m%d").dropna().unique().tolist()


def get_friday_within_4_days(expirations: list[str], now_date):
    for exp in sorted(expirations):
        d = datetime.strptime(exp, "%Y%m%d").date()
        if (
            d.weekday() == 4 and
            0 <= (d - now_date).days <= 4 and
            not (15 <= d.day <= 21)
        ):
            return exp
    return None


# -------------------- Main (batched by hour) --------------------
results = []

window_idx = 0
t_main0 = time.perf_counter()

for window_start, window_df in data_10m.groupby(pd.Grouper(freq="1h")):
    if window_df.empty:
        continue

    window_idx += 1
    w0 = time.perf_counter()
    window_end = window_start + pd.Timedelta(hours=1)

    # PASS 1: collect needed raw_symbols for this hour
    t1 = time.perf_counter()
    per_ts = {}
    raw_symbols_set = set()

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

        for strike, side in strike_sides:
            contract_row = df_defs[
                (df_defs["strike_price"].astype(float) == float(strike)) &
                (df_defs["instrument_class"] == side) &
                (pd.to_datetime(df_defs["expiration"]).dt.date == exp_date)
            ].head(1)
            if not contract_row.empty:
                raw_symbols_set.add(contract_row.iloc[0]["raw_symbol"])

        per_ts[ts] = (underlying_price, days_till_expiry, exp_date, strike_sides)

    raw_symbols = list(raw_symbols_set)
    pass1_s = time.perf_counter() - t1

    if not raw_symbols:
        if window_idx <= 5 or window_idx % 25 == 0:
            print(f"[WIN {window_idx}] {window_start} | PASS1 {pass1_s:.2f}s | raw_symbols=0 | ts={len(window_df)} | SKIP")
        continue

    # ONE FETCH PER HOUR
    t2 = time.perf_counter()
    mkt_df = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="cbbo-1m",
        symbols=raw_symbols,
        start=window_start,
        end=window_end,
    ).to_df()
    mkt_s = time.perf_counter() - t2

    t3 = time.perf_counter()
    trd_df = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="trades",
        symbols=raw_symbols,
        start=window_start,
        end=window_end,
    ).to_df()
    trd_s = time.perf_counter() - t3

    t4 = time.perf_counter()
    oi_df = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="statistics",
        symbols=raw_symbols,
        start=window_start - pd.Timedelta(days=1),
        end=window_end,
    ).to_df()
    oi_s = time.perf_counter() - t4

    # PASS 2: produce rows
    t5 = time.perf_counter()
    for ts, (underlying_price, days_till_expiry, exp_date, strike_sides) in per_ts.items():
        if days_till_expiry <= 1:
            time_decay_bucket = "EXTREME"
        elif days_till_expiry <= 3:
            time_decay_bucket = "HIGH"
        elif days_till_expiry <= 7:
            time_decay_bucket = "MEDIUM"
        else:
            time_decay_bucket = "LOW"

        snapshot_id = f"{symbol}_{ts.isoformat()}"

        out = get_contract_data_from_dfs(
            strike_sides,
            days_till_expiry,
            df_defs,
            exp_date,
            ts,
            underlying_price,
            mkt_df,
            trd_df,
            oi_df,
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

            append(
                results, snapshot_id, ts, symbol, underlying_price,
                days_till_expiry, exp_date, time_decay_bucket,
                strike, bucket, side,
                bid, ask, mid, vol, oi, iv, spread, spread_pct
            )

    pass2_s = time.perf_counter() - t5
    w_s = time.perf_counter() - w0

    # print first few windows + then every 25 windows

    print(
            f"[WIN {window_idx}] {window_start} | "
            f"PASS1 {pass1_s:.2f}s (ts={len(window_df)}, syms={len(raw_symbols)}) | "
            f"mkt {mkt_s:.2f}s rows={len(mkt_df)} | "
            f"trd {trd_s:.2f}s rows={len(trd_df)} | "
            f"oi {oi_s:.2f}s rows={len(oi_df)} | "
            f"PASS2 {pass2_s:.2f}s | WIN {w_s:.2f}s"
    )

print(f"[TIMING] main loop total: {time.perf_counter() - t_main0:.2f}s | results rows={len(results)}")


# -------------------- Save --------------------
t0 = time.perf_counter()
df = pd.DataFrame(results)
print(f"[TIMING] build df: {time.perf_counter() - t0:.2f}s | rows={len(df)}")

t0 = time.perf_counter()
df.to_parquet("options_snapshots.parquet", index=False, compression="snappy", engine="pyarrow")
print(f"[TIMING] parquet write: {time.perf_counter() - t0:.2f}s")

t0 = time.perf_counter()
con = duckdb.connect("options_data.db")
con.register("df_view", df)
cols = ",".join(df.columns)
con.execute(f"INSERT INTO option_snapshots_raw({cols}) SELECT {cols} FROM df_view")
con.unregister("df_view")
con.close()
print(f"[TIMING] duckdb insert: {time.perf_counter() - t0:.2f}s")

print(f"[TIMING] TOTAL RUN: {time.perf_counter() - t_all0:.2f}s")
