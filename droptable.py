import duckdb

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/options_data.db"

TABLES = [
    # 10 MIN TABLES
    "option_snapshots_raw",
    "option_snapshots_enriched",
    "option_snapshots_execution_signals",

    # 5 WEEK TABLES
    "option_snapshots_raw_5w",
    "option_snapshots_enriched_5w",
    "option_snapshots_execution_signals_5w",
]

def main():
    con = duckdb.connect(DB_PATH)
    try:
        for table in TABLES:
            print(f"Dropping table: {table}")
            con.execute(f"DROP TABLE IF EXISTS {table}")
        print("✅ All tables dropped successfully.")
    finally:
        con.close()

if __name__ == "__main__":
    main()
