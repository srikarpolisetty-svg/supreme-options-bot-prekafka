import duckdb
import pandas as pd

from backtesting_functions import backtest_signal
from backtesting_functions import backtest_returns
from backtesting_functions import backtest_returns_5w

# --- pandas: don't truncate prints ---
pd.set_option("display.max_rows", None)        # show all rows
pd.set_option("display.max_columns", None)     # show all columns
pd.set_option("display.width", None)           # auto-detect width
pd.set_option("display.max_colwidth", None)    # don't cut long strings
pd.set_option("display.expand_frame_repr", False)  # keep wide df on one line if possible

con = duckdb.connect("options_data.db", read_only=True)

#Atmcalldf = backtest_signal(con, moneyness="ATM", call_put="C", signal_col="atm_call_signal")
#print(Atmcalldf)

#Atmputdf = backtest_signal(con, moneyness="ATM", call_put="P", signal_col="atm_put_signal")
#print(Atmputdf)

Atmput_general_df = backtest_returns(con, moneyness="ATM", call_put="P")
print(Atmput_general_df)

Atmput_general_df_5w = backtest_returns_5w(con, moneyness="ATM", call_put="P")
print(Atmput_general_df_5w)


