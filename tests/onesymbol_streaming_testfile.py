import time
from datetime import datetime, timedelta, timezone

import databento as db
from config import DATABENTO_API_KEY

API_KEY = DATABENTO_API_KEY
DATASET = "OPRA.PILLAR"
LIVE_SCHEMA = "cbbo-1m"

ROOT = "AAPL"
PARENT_OPT = f"{ROOT}.OPT"


def pick_one_raw_symbol():
    hist = db.Historical(API_KEY)

    start = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()

    data = hist.timeseries.get_range(
        dataset=DATASET,
        schema="definition",
        symbols=PARENT_OPT,
        stype_in="parent",
        start=start,
    )

    df = data.to_df()
    if df is None or df.empty:
        raise RuntimeError(f"No definition rows returned for {PARENT_OPT}. Widen start window.")

    if "raw_symbol" not in df.columns:
        raise RuntimeError(f"Definition missing raw_symbol. Columns: {list(df.columns)}")

    df = df[df["raw_symbol"].notna()].copy()

    # If expiration exists, prefer non-expired contracts
    if "expiration" in df.columns:
        now = datetime.now(timezone.utc)
        try:
            df = df[df["expiration"] > now]
        except Exception:
            pass

    if df.empty:
        raise RuntimeError("No usable raw_symbol found from definition.")

    return str(df["raw_symbol"].iloc[0])


def on_record(record):
    # This callback receives ALL records, including ErrorMsg
    rtype = getattr(record, "rtype", None)

    # Print gateway errors clearly
    if str(rtype).endswith("ERROR"):
        print("GATEWAY ERROR RECORD:", record)
        return

    # Try to read cbbo bid/ask
    bid = getattr(record, "bid_px_00", None)
    ask = getattr(record, "ask_px_00", None)

    if bid is not None and ask is not None:
        try:
            bid_f = float(bid)
            ask_f = float(ask)
            mid = (bid_f + ask_f) / 2.0 if (bid_f > 0 and ask_f > 0) else None
            print(f"CBBO: bid={bid_f} ask={ask_f} mid={mid}")
        except Exception:
            print("CBBO (non-float bid/ask):", record)
    else:
        # For debugging: show first few non-cbbo records
        print("RECORD:", record)


def on_exception(exc: Exception):
    # Exceptions raised inside on_record will land here
    print("CALLBACK EXCEPTION:", repr(exc))


def main():
    print("Step 1) Resolve real option raw_symbol via definition for:", PARENT_OPT)
    raw_symbol = pick_one_raw_symbol()
    print("Picked raw_symbol:", repr(raw_symbol))

    print("\nStep 2) Start LIVE cbbo-1m stream")
    client = db.Live(key=API_KEY)

    client.subscribe(
        dataset=DATASET,
        schema=LIVE_SCHEMA,
        symbols=[raw_symbol],
    )

    # ✅ correct API from your screenshot
    client.add_callback(record_callback=on_record, exception_callback=on_exception)

    print("Streaming... waiting for data. (cbbo-1m can take up to ~1 minute)")
    print("Press CTRL+C to stop.\n")

    client.start()

    # Keep alive forever
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        client.stop()


if __name__ == "__main__":
    main()