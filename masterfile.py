from __future__ import annotations
from IBKRdatabase import main_parquet

from databasefunctions import get_sp500_symbols


import argparse







import argparse

def main():
    # ---- shard args ----
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--client_id_base", type=int, default=1000)  # optional
    args = parser.parse_args()

    # ---- load + shard symbols ----
    symbols = sorted(get_sp500_symbols())  # stable ordering
    my_symbols = symbols[args.shard::args.shards]

    print(f"[5W] Shard {args.shard}/{args.shards} processing {len(my_symbols)} symbols (run_id={args.run_id})")

    # ---- unique client id per shard ----
    client_id = args.client_id_base + args.shard

    # ---- process (parquet only) ----
    try:
        main_parquet(
            client_id=client_id,
            shard=args.shard,
            run_id=args.run_id,
            symbols=my_symbols,
        )
    except Exception as e:
        print(f"Shard {args.shard} failed: {e}")

if __name__ == "__main__":
    main()





