import duckdb 
import pandas as pd



pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/definitioncache.duckdb"

con = duckdb.connect(DB_PATH)


df = con.execute("""
    SELECT *
    FROM definition_cache
    WHERE symbol = ?
""", ["AAPL"]).fetchdf()

print(df.columns.tolist())

