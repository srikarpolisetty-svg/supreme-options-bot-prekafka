import duckdb 





DB_PATH = "definitioncache.duckdb"

con = duckdb.connect(DB_PATH)


df = con.execute("""
    SELECT *
    FROM definition_cache
    WHERE symbol = ?
""", ["AAPL"]).fetchdf()

print(df)


con = duckdb.connect(DB_PATH)

df1 = con.execute("""
    SELECT DISTINCT symbol
    FROM definition_cache
    ORDER BY symbol
""").fetchdf()

print(df1)