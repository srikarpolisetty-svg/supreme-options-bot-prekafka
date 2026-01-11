# run: python3 testconnectionIBKR.py
#
# Tests connection to IB Gateway (Paper) and logs all events.
# Adjustments:
# - Treat IB "status" codes (2104/2106/2107/2158) as INFO, not ERROR
# - Keep the connection open briefly (default 30s) before disconnecting

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time
import logging

HOST = "127.0.0.1"
PORT = 4002          # IB Gateway: paper=4002, live=4001 | TWS: paper=7497, live=7496
CLIENT_ID = 7        # use a non-1 clientId to avoid conflicts
LOG_FILE = "ibkr_connection.log"
HOLD_SECONDS = 30    # keep connection alive for a bit after nextValidId


# ---------- logging ----------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)


class App(EWrapper, EClient):
    # IB "error" callback also carries info/status messages; treat these as INFO.
    INFO_CODES = {2104, 2106, 2107, 2158}

    def __init__(self):
        EClient.__init__(self, self)
        self.connected_ok = False

    def nextValidId(self, orderId: int):
        self.connected_ok = True
        logging.info(f"CONNECTED ✅ nextValidId={orderId}")

        # Hold connection briefly so you can observe ESTABLISHED sockets, farm messages, etc.
        time.sleep(HOLD_SECONDS)

        self.disconnect()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in self.INFO_CODES:
            logging.info(f"INFO reqId={reqId} code={errorCode} msg={errorString}")
            return

        logging.error(f"ERROR reqId={reqId} code={errorCode} msg={errorString}")
        if advancedOrderRejectJson:
            logging.error(f"advancedOrderRejectJson={advancedOrderRejectJson}")

    def connectionClosed(self):
        logging.warning("Connection closed by IB.")


def main():
    logging.info("Starting IBKR Gateway connection test")
    app = App()

    logging.info(f"Connecting to {HOST}:{PORT} (clientId={CLIENT_ID})")
    app.connect(HOST, PORT, clientId=CLIENT_ID)

    t = threading.Thread(target=app.run, daemon=True)
    t.start()

    deadline = time.time() + 10
    while time.time() < deadline and not app.connected_ok:
        time.sleep(0.1)

    if not app.connected_ok:
        logging.error("❌ Did not receive nextValidId within timeout")
        logging.error("Common causes:")
        logging.error("- Gateway not fully logged in")
        logging.error("- API not enabled or wrong port")
        logging.error("- Duplicate clientId")
        logging.error("- Script not running on same machine as Gateway")
        app.disconnect()

    logging.info("Test finished")


if __name__ == "__main__":
    main()
