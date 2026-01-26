import duckdb
import databento as db
from datetime import datetime, timedelta, timezone

from execution_functions import get_all_symbols
from message import send_text


DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/options_data.db"


def check_db_accuracy(symbols):
    client = db.Historical()

    for symbol in symbols:
        # ---- get latest underlying price from DB ----
        with duckdb.connect(DB_PATH, read_only=True) as con:
            df = con.execute(
                """
                SELECT underlying_price
                FROM option_snapshots_raw
                WHERE symbol = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                [symbol],
            ).df()

        if df.empty:
            continue

        ib_price = df.iloc[0]["underlying_price"]
        if ib_price is None:
            continue

        # ---- get latest underlying price from Databento ----
        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=5)

        df_db = client.timeseries.get_range(
            dataset="EQUS.MINI",
            schema="ohlcv-1m",
            symbols=[symbol],
            start=start.isoformat(),
            end=now.isoformat(),
        ).to_df()

        if df_db.empty:
            continue

        row = df_db.sort_values("ts_event").iloc[-1]
        db_price = float(row["close"])

        # ---- compare ----
        if abs(float(ib_price) - float(db_price)) > 10:
            send_text(
                f"DB CORRUPTION ALERT: {symbol} price mismatch. "
                f"IBKR={ib_price}, Databento={db_price}"
            )


# -------------------------
# run
# -------------------------
symbols = get_all_symbols()
sample_symbols = symbols[::20]

check_db_accuracy(sample_symbols)
