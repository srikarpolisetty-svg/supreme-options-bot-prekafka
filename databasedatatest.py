import duckdb
import pandas as pd

# SHOW EVERYTHING
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

DB_PATH = "options_data.db"

con = duckdb.connect(DB_PATH)

symbols_df = con.execute("""
SELECT DISTINCT symbol
FROM option_snapshots_raw
ORDER BY symbol
""").df()
print("\n=== DISTINCT SYMBOLS ===")
print(symbols_df)

exp_df = con.execute("""
SELECT DISTINCT expiration_date
FROM option_snapshots_raw
WHERE symbol = 'AMD'
ORDER BY expiration_date
""").df()
print("\n=== AMD EXPIRATIONS ===")
print(exp_df)

sample_df = con.execute("""
SELECT *
FROM option_snapshots_raw
WHERE symbol IN (
    SELECT DISTINCT symbol
    FROM option_snapshots_raw
    LIMIT 5
)
ORDER BY symbol, timestamp
LIMIT 50
""").df()

print("\n=== SAMPLE ROWS (MULTI-SYMBOL) ===")
print(sample_df)

con.close()