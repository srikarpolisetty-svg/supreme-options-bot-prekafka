import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

symbol = "SPY"

end = datetime.now(timezone.utc)
start = end - timedelta(days=1)

df = yf.download(
    symbol,
    start=start,
    end=end,
    interval="5m",
    progress=False
)

if df is None or df.empty:
    print("No data returned.")
else:
    df.index = pd.to_datetime(df.index, utc=True)
    print("Last 5 rows:")
    print(df.tail())

    print("\nLatest timestamp:", df.index[-1])
    print("Now (UTC):", datetime.now(timezone.utc))
