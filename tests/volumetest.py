import databento as db
import pandas as pd
from config import DATABENTO_API_KEY

client = db.Historical(DATABENTO_API_KEY)

START = "2024-01-02"
END   = "2024-01-03"

# 1) Get one contract first (definition)
defs = client.timeseries.get_range(
    dataset="OPRA.PILLAR",
    schema="definition",
    symbols=["NVDA.OPT"],
    stype_in="parent",
    start=START,
    end=END,
).to_df()

raw_contract = defs["raw_symbol"].dropna().astype(str).iloc[0]
print("Using raw:", raw_contract)

# 2) Pull TRADES for that single contract
trades = client.timeseries.get_range(
    dataset="OPRA.PILLAR",
    schema="trades",
    symbols=[raw_contract],
    stype_in="raw_symbol",
    start=START,
    end=END,
).to_df()

print("\nTRADES COLUMNS:")
print(sorted(trades.columns))

print("\nSAMPLE ROWS:")
print(trades.head(10))

if "size" in trades.columns:
    print("\nNON-ZERO TRADE SIZES:")
    print(trades[trades["size"] > 0].head(10))

if "ts_event" in trades.columns:
    print("\nTIMESTAMP RANGE:")
    print(trades["ts_event"].min(), "->", trades["ts_event"].max())
