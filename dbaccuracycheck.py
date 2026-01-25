from execution_functions import get_all_symbols, get_underlying_price_ibkr
from databento_functions import get_underlying_price_databento
from message import send_text

# get symbols (CALL THE FUNCTION)
symbols = get_all_symbols()

# pick 1 out of every 20
sample_symbols = symbols[::20]

for symbol in sample_symbols:
    ib_price = get_underlying_price_ibkr(symbol)
    db_price = get_underlying_price_databento(symbol)

    # skip if missing data
    if ib_price is None or db_price is None:
        continue

    if abs(ib_price - db_price) > 10:
        send_text(
            f"DB CORRUPTION ALERT: {symbol} price mismatch. "
            f"IBKR={ib_price}, Databento={db_price}"
        )
