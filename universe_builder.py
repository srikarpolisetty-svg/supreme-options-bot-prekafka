import duckdb 




DB_PATH = "definitioncache.duckdb"
con = duckdb.connect(DB_PATH)

symbols = con.execute("""
SELECT DISTINCT symbol
FROM definition_cache
""").fetchdf()["symbol"].tolist()

print(len(symbols))

for symbol in symbols:
    df = con.execute("""
    SELECT raw_symbol 
    FROM definition_cache
    WHERE symbol = ?
    """, [symbol]).fetchdf()

    symbols  = df["raw_symbol"].tolist()
    print(symbols)


