def get_option_quote(chain, call_put: str, strike: float):
    """
    chain: yfinance option_chain(exp)
    call_put: "C" for calls, "P" for puts
    strike: target strike price (float)

    Returns a dict with:
      last_price, bid, ask, mid,
      volume, iv, oi,
      spread, spread_pct
    """
    df = chain.calls if call_put == "C" else chain.puts

    row = df[df["strike"] == strike].iloc[0]

    bid   = row["bid"]
    ask   = row["ask"]
    mid   = (bid + ask) / 2
    spread = ask - bid
    spread_pct = (spread / mid) * 100 if mid != 0 else 0.0

    return {
        "last_price": row["lastPrice"],
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "volume": row["volume"],
        "iv": row["impliedVolatility"],
        "oi": row["openInterest"],
        "spread": spread,
        "spread_pct": spread_pct,
    }











def get_closest_strike(chain, call_put, target):
    df = chain.calls if call_put == "C" else chain.puts
    strikes = df["strike"].values

    # find strike with minimum absolute distance from target
    return min(strikes, key=lambda s: abs(s - target))







import duckdb

DB_PATH = "/home/ubuntu/supreme-options-bot/options_data.db"

def compute_z_scores_for_bucket(
    symbol: str,
    bucket: str,
    call_put: str,
    time_decay_bucket: str,
    current_mid: float,
    current_volume: float,
    current_iv: float,
):
    """
    Compute z-scores for mid, volume, iv for a given
    (symbol, bucket, call_put, time_decay_bucket),
    using the historical rows in option_snapshots_raw.
    """
    with duckdb.connect(DB_PATH, read_only=True) as con:
        df = con.execute(
            """
            SELECT mid, volume, iv
            FROM option_snapshots_raw
            WHERE symbol = ?
              AND moneyness_bucket = ?
              AND call_put = ?
              AND time_decay_bucket = ?
            """,
            [symbol, bucket, call_put, time_decay_bucket],
        ).df()

    if df.empty:
        return 0.0, 0.0, 0.0

    mid_mean = df["mid"].mean()
    mid_std  = df["mid"].std()

    vol_mean = df["volume"].mean()
    vol_std  = df["volume"].std()

    iv_mean  = df["iv"].mean()
    iv_std   = df["iv"].std()

    mid_z = (current_mid - mid_mean) / mid_std if mid_std else 0.0
    vol_z = (current_volume - vol_mean) / vol_std if vol_std else 0.0
    iv_z  = (current_iv - iv_mean) / iv_std if iv_std else 0.0

    return mid_z, vol_z, iv_z







import duckdb

DB_PATH = "/home/ubuntu/supreme-options-bot/options_data.db"

def compute_z_scores_for_bucket_5w(
    symbol: str,
    bucket: str,
    call_put: str,
    time_decay_bucket: str,
    current_mid: float,
    current_volume: float,
    current_iv: float,
):
    with duckdb.connect(DB_PATH, read_only=True) as con:
        df = con.execute(
            """
            SELECT mid, volume, iv
            FROM option_snapshots_raw_5w
            WHERE symbol = ?
              AND moneyness_bucket = ?
              AND call_put = ?
              AND time_decay_bucket = ?
            """,
            [symbol, bucket, call_put, time_decay_bucket],
        ).df()

    if df.empty:
        return 0.0, 0.0, 0.0

    mid_mean = df["mid"].mean()
    mid_std  = df["mid"].std()

    vol_mean = df["volume"].mean()
    vol_std  = df["volume"].std()

    iv_mean  = df["iv"].mean()
    iv_std   = df["iv"].std()

    mid_z = (current_mid - mid_mean) / mid_std if mid_std else 0.0
    vol_z = (current_volume - vol_mean) / vol_std if vol_std else 0.0
    iv_z  = (current_iv - iv_mean) / iv_std if iv_std else 0.0

    return mid_z, vol_z, iv_z



