# save as: ib_ping.py
# run: python3 ib_ping.py
#
# This verifies your Ubuntu box can connect to IB Gateway/TWS API and receive nextValidId.

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import threading
import time


HOST = "127.0.0.1"
PORT = 7497  # Paper = 7497, Live = 7496
CLIENT_ID = 1


class App(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.connected_ok = False

    def nextValidId(self, orderId: int):
        self.connected_ok = True
        print(f"CONNECTED ✅ nextValidId = {orderId}")
        self.disconnect()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        # You'll often see harmless info messages too; still useful for debugging.
        print(f"ERROR reqId={reqId} code={errorCode} msg={errorString}")
        if advancedOrderRejectJson:
            print("advancedOrderRejectJson:", advancedOrderRejectJson)


def main():
    app = App()

    # Start the IB network loop
    t = threading.Thread(target=app.run, daemon=True)
    t.start()

    print(f"Connecting to {HOST}:{PORT} (clientId={CLIENT_ID}) ...")
    app.connect(HOST, PORT, clientId=CLIENT_ID)

    # Wait up to 10 seconds for nextValidId
    deadline = time.time() + 10
    while time.time() < deadline and not app.connected_ok:
        time.sleep(0.1)

    if not app.connected_ok:
        print("❌ Did not receive nextValidId. Common causes:")
        print("  - Wrong port (Paper 7497 vs Live 7496)")
        print("  - API not enabled in Gateway (API -> Settings)")
        print("  - Gateway not fully logged in")
        print("  - Another app already using the same clientId")
        app.disconnect()


if __name__ == "__main__":
    main()
