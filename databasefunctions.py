


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

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/options_data.db"

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





import os
import time
import tempfile
from typing import List, Optional

import requests
import pandas as pd




SP500_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
CACHE_PATH = "/home/ubuntu/supreme-options-bot/sp500_constituents.csv"

def _atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    d = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, suffix=".tmp") as f:
        tmp_path = f.name
        df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)  # atomic on POSIX

def _normalize_symbol(sym: str) -> str:
    # yfinance compatibility: BRK.B -> BRK-B, BF.B -> BF-B
    return sym.replace(".", "-").strip().upper()

def get_sp500_symbols(retries: int = 3, backoff_sec: float = 2.0, timeout_sec: float = 10.0) -> List[str]:
    last_err: Optional[Exception] = None

    for i in range(retries):
        try:
            r = requests.get(SP500_URL, timeout=timeout_sec)
            r.raise_for_status()

            # Parse CSV from text content
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))

            if "Symbol" not in df.columns:
                raise ValueError(f"Expected 'Symbol' column, got columns={list(df.columns)}")

            syms = df["Symbol"].dropna().astype(str).map(_normalize_symbol).tolist()

            # sanity check: avoid caching nonsense
            if len(syms) < 400:
                raise ValueError(f"Too few symbols ({len(syms)}). Possible bad response.")

            _atomic_write_csv(df, CACHE_PATH)
            return syms

        except Exception as e:
            last_err = e
            time.sleep(backoff_sec * (2 ** i))

    # Fallback cache
    if os.path.exists(CACHE_PATH):
        df = pd.read_csv(CACHE_PATH)
        if "Symbol" not in df.columns:
            raise RuntimeError(f"Cache exists but missing 'Symbol' column: {CACHE_PATH}")
        return df["Symbol"].dropna().astype(str).map(_normalize_symbol).tolist()

    raise RuntimeError(f"Failed to fetch S&P 500 symbols and no cache found at {CACHE_PATH}.") from last_err









def master_ingest(run_id: str, db_path: str = "options_data.db"):
    """
    Master ingest for a single options run_id.
    Tables already exist.
    Handles BOTH 10-min and 5-week tables.
    """

    raw_dir = f"runs/{run_id}/option_snapshots_raw"
    enriched_dir = f"runs/{run_id}/option_snapshots_enriched"
    signals_dir = f"runs/{run_id}/option_snapshots_execution_signals"

    con = duckdb.connect(db_path)

    try:
        con.execute("BEGIN;")

        # ============================
        # 10 MIN TABLES
        # ============================

        con.execute(
            """
            INSERT INTO option_snapshots_execution_signals
            SELECT * FROM read_parquet(?)
            """,
            [f"{signals_dir}/shard_*.parquet"],
        )

        con.execute(
            """
            INSERT INTO option_snapshots_enriched
            SELECT * FROM read_parquet(?)
            """,
            [f"{enriched_dir}/shard_*.parquet"],
        )

        con.execute(
            """
            INSERT INTO option_snapshots_raw
            SELECT * FROM read_parquet(?)
            """,
            [f"{raw_dir}/shard_*.parquet"],
        )

        # ============================
        # 5 WEEK TABLES
        # ============================

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
