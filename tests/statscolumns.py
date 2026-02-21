import databento as db
from config import DATABENTO_API_KEY
from datetime import datetime, timedelta, timezone
import pandas as pd
import time
import operator

client = db.Historical(DATABENTO_API_KEY)

end = datetime.now(timezone.utc) - timedelta(days=3)
end = end.replace(hour=0, minute=0, second=0, microsecond=0)
start = end - timedelta(days=5)

job = client.batch.submit_job(
    dataset="OPRA.PILLAR",
    schema="statistics",
    symbols=["AAPL.OPT"],
    start=start.isoformat(),
    end=end.isoformat(),
    stype_in="parent",
    split_duration="day",
)

job_id = job["id"]

while True:
    done_ids = set(map(operator.itemgetter("id"), client.batch.list_jobs("done")))
    if job_id in done_ids:
        break
    time.sleep(2)

files = client.batch.download(job_id=job_id, output_dir="debug_stats")

dfs = []
for f in files:
    if f.suffix == ".zst":
        store = db.DBNStore.from_file(f)
        dfs.append(store.to_df())

df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

print(df.columns)
print(df.head())
print(df["stat_type"].unique())