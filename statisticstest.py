import databento as db
import datetime as dt
import pytz
from config import DATABENTO_API_KEY

client = db.Historical(DATABENTO_API_KEY)

now = dt.datetime.now(tz=pytz.UTC)

# Clamp `end` so it’s always <= available dataset range (avoids your error)
end = now - dt.timedelta(minutes=10000)
start = end - dt.timedelta(days=240)
 
symbol = "AAPL"
today = end.date()

# 1) Get a REAL raw_symbol from definition (no guessing)
defs = client.timeseries.get_range(
    dataset="OPRA.PILLAR",
    schema="definition",
    symbols=f"{symbol}.OPT",
    stype_in="parent",
    start=today,
).to_df()

if defs.empty:
    raise RuntimeError("No definition rows returned.")

raw_symbol = defs["raw_symbol"].iloc[0]
print("RAW SYMBOL:", raw_symbol)

# 2) Pull statistics for that raw_symbol
stats = client.timeseries.get_range(
    dataset="OPRA.PILLAR",
    schema="statistics",
    symbols=[raw_symbol],
    stype_in="raw_symbol",
    start=start,
    end=end,
).to_df()

print(stats.head())
print(stats.columns)
print("stat_type uniques:", sorted(stats["stat_type"].dropna().unique()))

# 3) Extract open interest rows (stat_type == 9)
oi = stats[stats["stat_type"] == 9].copy()
print("OI rows:", len(oi))
print(oi[["ts_event", "ts_ref", "quantity", "symbol"]].tail(10))
