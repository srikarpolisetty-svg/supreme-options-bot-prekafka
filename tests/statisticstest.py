import time
import pathlib
import operator
import databento as db
from config import DATABENTO_API_KEY

client = db.Historical(DATABENTO_API_KEY)

START = "2024-01-02"
END   = "2024-01-03"

BATCH_DIR = pathlib.Path("batch_downloads")
POLL_S = 2.0


def batch_get_df(
    dataset: str,
    schema: str,
    symbols,
    start,
    end,
    *,
    stype_in: str,
    split_duration: str = "day",
    poll_s: float = 2.0,
):
    """
    Batch job -> wait -> download -> load .dbn.zst -> concat to df
    """
    if not symbols:
        raise ValueError("symbols is empty")

    job = client.batch.submit_job(
        dataset=dataset,
        start=start,
        end=end,
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
        # `f` can be a Path, so cast to str before endswith
        if str(f).endswith(".dbn.zst"):
            store = db.DBNStore.from_file(f)
            dfs.append(store.to_df())

    if not dfs:
        # empty result is still valid
        import pandas as pd
        return pd.DataFrame()

    import pandas as pd
    return pd.concat(dfs, ignore_index=True)


# ----------------------------
# 1) Definition (batch)
# ----------------------------
defs = batch_get_df(
    dataset="OPRA.PILLAR",
    schema="definition",
    symbols=["NVDA.OPT"],      # parent symbol (options root)
    start=START,
    end=END,
    stype_in="parent",
    split_duration="day",
    poll_s=POLL_S,
)

print("DEFINITION COLUMNS:", list(defs.columns))
print(defs[["raw_symbol", "instrument_class", "expiration", "strike_price"]].head(10))

raw_contract = defs["raw_symbol"].dropna().astype(str).iloc[0]
print("\nUsing raw_symbol:", raw_contract)

# ----------------------------
# 2) Statistics (batch)
# ----------------------------
stats = batch_get_df(
    dataset="OPRA.PILLAR",
    schema="statistics",
    symbols=[raw_contract],     # contract-level
    start=START,
    end=END,
    stype_in="raw_symbol",
    split_duration="day",
    poll_s=POLL_S,
)

print("\nSTATS COLUMNS:", list(stats.columns))

# ----------------------------
# 3) Filter OI
# ----------------------------
oi = stats[stats["stat_type"] == db.StatType.OPEN_INTEREST]
print("\nOPEN INTEREST ROWS:")
print(oi[["symbol", "quantity", "ts_event"]])