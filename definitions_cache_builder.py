# definition_cache_builder.py
# Pull OPRA.PILLAR definition data once/day and store as rows in DuckDB (not JSON blobs).

from __future__ import annotations

import time
import datetime as dt
import pytz
import pandas as pd
import databento as db
import duckdb

from databasefunctions import get_sp500_symbols
from config import DATABENTO_API_KEY


DB_PATH = "definitioncache.duckdb"
SLEEP_SEC = 0.10  # small throttle so you don't slam the API


def build_definition_cache():
    hist = db.Historical(DATABENTO_API_KEY)

    today_utc = dt.datetime.now(tz=pytz.UTC).date()
    start = today_utc - dt.timedelta(days=1)
    end = today_utc  # end is exclusive-ish in many APIs; this avoids "start >= end"

    symbols = list(get_sp500_symbols())
    print(f"Total symbols: {len(symbols)}")
    print(f"Definition query range: start={start} end={end}")

    con = duckdb.connect(DB_PATH)
        # Replace old cache each run
    con.execute("DROP TABLE IF EXISTS definition_cache;")


    created_table = False
    inserted_total = 0
    skipped = 0
    errors = 0

    for i, sym in enumerate(symbols, 1):
        # Databento parent symbology fix: remove '-' and '.' (e.g. BF-B -> BFB)
        parent_sym = sym.replace("-", "").replace(".", "")

        try:
            chain_df = hist.timeseries.get_range(
                dataset="OPRA.PILLAR",
                schema="definition",
                symbols=f"{parent_sym}.OPT",
                stype_in="parent",
                start=start,
                end=end,
            ).to_df()
        except Exception as e:
            errors += 1
            print(f"[{i}/{len(symbols)}] ERROR {sym} ({parent_sym}): {e}")
            time.sleep(SLEEP_SEC)
            continue

        if chain_df is None or chain_df.empty:
            skipped += 1
            time.sleep(SLEEP_SEC)
            continue

        # tag rows with the original equity symbol (so you can query by it later)
        chain_df["symbol"] = sym

        # create table on first successful df (stores real columns, not JSON)
        if not created_table:
            con.register("tmp_def", chain_df)
            con.execute("""
                CREATE TABLE IF NOT EXISTS definition_cache AS
                SELECT * FROM tmp_def WHERE 0=1
            """)
            con.unregister("tmp_def")
            created_table = True

        # append rows
        con.append("definition_cache", chain_df)
        inserted_total += len(chain_df)

        if i % 25 == 0:
            print(f"[{i}/{len(symbols)}] inserted_total={inserted_total} skipped={skipped} errors={errors}")

        time.sleep(SLEEP_SEC)

    con.close()
    print("DONE")
    print(f"Inserted rows: {inserted_total}")
    print(f"Skipped (empty): {skipped}")
    print(f"Errors: {errors}")


if __name__ == "__main__":
    build_definition_cache()
