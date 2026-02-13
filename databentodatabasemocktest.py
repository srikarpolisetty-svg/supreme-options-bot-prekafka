import uuid
from  databentodatabase import run_databento_option_snapshot

def mock_test_run():
    # 10 liquid symbols so definition + options almost always exist
    symbols = [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "GOOGL",
        "META",
        "TSLA",
        "SPY",
        "QQQ",
        "AMD",
    ]

    run_id = f"mock_{uuid.uuid4().hex[:8]}"

    print(f"Starting mock run_id={run_id}")

    for shard_id, symbol in enumerate(symbols):
        try:
            print(f"\n--- {symbol} (shard {shard_id}) ---")
            result = run_databento_option_snapshot(
                run_id=run_id,
                symbol=symbol,
                shard_id=shard_id,
            )
            print("Result:", result)
        except Exception as e:
            print(f"ERROR on {symbol}: {e}")

    print("\nMock run complete.")

if __name__ == "__main__":
    mock_test_run()
