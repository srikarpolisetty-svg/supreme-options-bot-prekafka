import duckdb 
import yfinance as yf




DB_PATH = "definitioncache.duckdb"
con = duckdb.connect(DB_PATH)

symbols = con.execute("""
SELECT DISTINCT symbol
FROM definition_cache
""").fetchdf()["symbol"].tolist()

print(len(symbols))

for symbol in symbols:
    df = con.execute("""
    SELECT raw_symbol, expiration 
    FROM definition_cache
    WHERE symbol = ?
    """, [symbol]).fetchdf()

    symbols  = df["raw_symbol"].tolist()
    expirations = df["expiration"]
    print(symbols)
    print (expirations)



hist = yf.download(
    symbol,
    period="1d",
    interval="5m",
    progress=False
)

underlying_price = hist["Close"].iloc[-1]


atm = underlying_price 

c1 = atm * 1.015
p1 = atm * 0.985

c2 = atm * 1.035

p2 = atm * 0.965

'VICI  270115P00040000'


