import duckdb
import pandas as pd

import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)





con = duckdb.connect("options_data.db", read_only=True)

df = con.execute("""
    SELECT *
    FROM option_snapshots_enriched
    ORDER BY timestamp ASC
""").df()



print(df)