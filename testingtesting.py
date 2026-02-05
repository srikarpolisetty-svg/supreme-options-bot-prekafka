import duckdb
import pandas as pd

DB_PATH = "options_data.db"
SYMBOL = "AAPL"

def main():
    con = duckdb.connect(DB_PATH, read_only=True)

    try:
        df = con.execute("""
            SELECT DISTINCT timestamp
            FROM option_snapshots_raw
            WHERE symbol = ?
            ORDER BY timestamp
        """, [SYMBOL]).df()
    finally:
        con.close()

    if df is None or df.empty:
        print("No timestamps found.")
        return

    print(f"Total timestamps: {len(df)}")
    print("-----")

    # print all timestamps
    for ts in df["timestamp"]:
        print(pd.Timestamp(ts).isoformat())

if __name__ == "__main__":
    main()
