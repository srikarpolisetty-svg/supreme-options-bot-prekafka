from __future__ import annotations

import argparse
import datetime as dt
import pytz
import databento as db

from config import DATABENTO_API_KEY
from databentodatabase import run_databento_option_snapshot
from databasefunctions import get_sp500_symbols

# now expects (rec, quote_cache, vol_cache)
from databentodatabase import handle_live_record  # adjust import if needed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    symbols = sorted(get_sp500_symbols())
    my_symbols = symbols[args.shard::args.shards]

    print(
        f"Shard {args.shard}/{args.shards} processing {len(my_symbols)} symbols (run_id={args.run_id})"
    )

    # shared caches for entire shard (quotes + rolling volume)
    quote_cache: dict = {}
    vol_cache: dict = {}

    # one live connection for the whole shard
    live = db.Live(key=DATABENTO_API_KEY)

    try:
        for symbol in my_symbols:
            print(f"--- {symbol} (shard {args.shard}) ---")

            # subscribe to BOTH quotes + trades so we have mid + live volume
            live.subscribe(
                dataset="OPRA.PILLAR",
                schema="mbp-1",
                symbols=[f"{symbol}.OPT"],
                stype_in="parent",
            )
            live.subscribe(
                dataset="OPRA.PILLAR",
                schema="trades",
                symbols=[f"{symbol}.OPT"],
                stype_in="parent",
            )

            # warm up for ~2 seconds (fills some quotes + some volume)
            warmup_end = dt.datetime.now(tz=pytz.UTC) + dt.timedelta(seconds=2)
            while dt.datetime.now(tz=pytz.UTC) < warmup_end:
                rec = live.next_record(timeout=0.5)
                if rec is not None:
                    handle_live_record(rec, quote_cache, vol_cache)

            # run snapshot using BOTH caches
            try:
                run_databento_option_snapshot(
                    args.run_id,
                    symbol,
                    args.shard,
                    quote_cache,
                    vol_cache,
                )
            except Exception as e:
                print(f"ERROR on {symbol}: {e}")

    finally:
        try:
            live.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
