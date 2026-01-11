# run: python3 testconnectionIBKR.py
#
# Tests connection to IB Gateway (Paper) and logs all events.
# - Treat IB "status" codes (2104/2106/2107/2158) as INFO, not ERROR
# - Wait for nextValidId to confirm connection
# - Then hold for HOLD_SECONDS (or forever if HOLD_FOREVER=True)

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time
import logging

HOST = "127.0.0.1"
PORT = 4002          # IB Gateway: paper=4002, live=4001 | TWS: paper=7497, live=7496
CLIENT_ID = 7        # use a non-1 clientId to avoid conflicts
LOG_FILE = "ibkr_connection.log"

HOLD_SECONDS = 30
HOLD_FOREVER = True     # set True to hold indefinitely once connected
RECONNECT = True         # attempt reconnect if IB drops the socket
RECONNECT_DELAY = 5      # seconds


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
    INFO_CODES = {2104, 2106, 2107, 2158}

    def __init__(self):
        EClient.__init__(self, self)
        self.connected_ok = False
        self._stop = False

    def nextValidId(self, orderId: int):
        self.connected_ok = True
        logging.info(f"CONNECTED ✅ nextValidId={orderId}")

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in self.INFO_CODES:
            logging.info(f"INFO reqId={reqId} code={errorCode} msg={errorString}")
            return

        logging.error(f"ERROR reqId={reqId} code={errorCode} msg={errorString}")
        if advancedOrderRejectJson:
            logging.error(f"advancedOrderRejectJson={advancedOrderRejectJson}")

    def connectionClosed(self):
        self.connected_ok = False
        logging.warning("Connection closed by IB.")
        # If RECONNECT is enabled, we'll reconnect in the run loop.

    def run_loop(self):
        """
        Runs IB API network loop, and optionally reconnects if disconnected.
        """
        while not self._stop:
            try:
                # This blocks until disconnected / error
                self.run()
            except Exception as e:
                logging.exception(f"Exception in app.run(): {e}")

            if self._stop:
                break

            if RECONNECT:
                logging.warning(f"Connection lost. Reconnecting in {RECONNECT_DELAY}s...")
                time.sleep(RECONNECT_DELAY)
                try:
                    self.connect(HOST, PORT, clientId=CLIENT_ID)
                except Exception as e:
                    logging.exception(f"Reconnect failed: {e}")
                    time.sleep(RECONNECT_DELAY)
            else:
                break

    def stop(self):
        self._stop = True
        try:
            self.disconnect()
        except Exception:
            pass


def main():
    logging.info("Starting IBKR Gateway connection test")

    app = App()
    logging.info(f"Connecting to {HOST}:{PORT} (clientId={CLIENT_ID})")
    app.connect(HOST, PORT, clientId=CLIENT_ID)

    t = threading.Thread(target=app.run_loop, daemon=True)
    t.start()

    # Wait for nextValidId
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
        app.stop()
        return

    # Connected
    if HOLD_FOREVER:
        logging.info("Connection established. Holding indefinitely.")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            logging.info("KeyboardInterrupt: exiting.")
        finally:
            app.stop()
        return

    logging.info(f"Connection established. Holding for {HOLD_SECONDS}s...")
    time.sleep(HOLD_SECONDS)
    logging.info("Done. Disconnecting.")
    app.stop()


if __name__ == "__main__":
    main()

