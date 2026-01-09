from execution_functions import get_all_symbols
import duckdb
from execution import run_execution_engine

con = duckdb.connect("options_data.db")


symbols = get_all_symbols(con)


for symbol in symbols:
    run_execution_engine(symbol)




con.close()