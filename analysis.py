import duckdb
from message import send_text
from analysis_functions import load_all_groups, get_option_metrics, update_signal

import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import exchange_calendars as ecals


def run_option_signals(symbol: str):
    NY_TZ = ZoneInfo("America/New_York")
    XNYS = ecals.get_calendar("XNYS")  # NYSE

    now = datetime.now(NY_TZ)

    # True only if the exchange is actually open right now (includes holidays/early closes)
    if not XNYS.is_open_on_minute(now, ignore_breaks=True):
        print(f"Market closed (holiday/after-hours) — skipping insert. now={now}")
        sys.exit(0)

    now1 = datetime.now()
    print(f"Run time: {now1.strftime('%Y-%m-%d %H:%M')}")

    con = duckdb.connect("options_data.db")

    # NOTE: load_all_groups now returns { "ATM_CALL": df, "ATM_PUT": df, ... } (single table)
    groups = load_all_groups(con, symbol)

    if groups is None:
        con.close()
        return f"no data {symbol}"

    # =========================
    # Helper: pull metrics safely
    # =========================
    def M(key: str):
        m = get_option_metrics(groups, key)  # single-table metrics dict
        if m is None:
            return None
        return m

    # ============================================================
    # ATM, OTM_1 and OTM_2 blocks are identical structure → helper
    # ============================================================
    def handle_bucket(bucket: str):
        """
        bucket: "ATM", "OTM_1", or "OTM_2"
        Updates signal columns:
          - f"{bucket.lower()}_call_signal"  (e.g. atm_call_signal)
          - f"{bucket.lower()}_put_signal"   (e.g. atm_put_signal)
        """
        call_key = f"{bucket}_CALL"
        put_key  = f"{bucket}_PUT"

        call_m = M(call_key)
        put_m  = M(put_key)

        call_signal = False
        put_signal  = False

        if call_m is not None:
            call_signal = (
                call_m["z_price_5w"]  > 1.5 and
                call_m["z_volume_5w"] > 1.5 and
                call_m["z_iv_5w"]     > 1.5 and
                call_m["z_price_3d"]  > 1.5 and
                call_m["z_volume_3d"] > 1.5 and
                call_m["z_iv_3d"]     > 1.5
            )

        if put_m is not None:
            put_signal = (
                put_m["z_price_5w"]  > 1.5 and
                put_m["z_volume_5w"] > 1.5 and
                put_m["z_iv_5w"]     > 1.5 and
                put_m["z_price_3d"]  > 1.5 and
                put_m["z_volume_3d"] > 1.5 and
                put_m["z_iv_3d"]     > 1.5
            )

        # Decision
        if call_signal and not put_signal and call_m is not None:
            send_text(
                f"🚀 STRONG {bucket} CALL SIGNAL\n\n"
                f"Symbol: {call_m['symbol']}\n"
                f"Strike: {call_m['strike']}\n"
                f"Option Price (mid): {call_m['price']}\n\n"
                f"All price/volume/IV Z-scores > 1.5 in BOTH 3-day and 5-week windows."
            )
            print(f"ALERT SENT ({bucket} CALL)")

            update_signal(
                con,
                symbol=call_m["symbol"],
                snapshot_id=call_m["snapshot_id"],
                call_put="C",
                bucket=bucket,
                signal_column=f"{bucket.lower()}_call_signal",
            )

        elif put_signal and not call_signal and put_m is not None:
            send_text(
                f"⚠️ STRONG {bucket} PUT SIGNAL\n\n"
                f"Symbol: {put_m['symbol']}\n"
                f"Strike: {put_m['strike']}\n"
                f"Option Price (mid): {put_m['price']}\n\n"
                f"All price/volume/IV Z-scores > 1.5 in BOTH 3-day and 5-week windows."
            )
            print(f"ALERT SENT ({bucket} PUT)")

            update_signal(
                con,
                symbol=put_m["symbol"],
                snapshot_id=put_m["snapshot_id"],
                call_put="P",
                bucket=bucket,
                signal_column=f"{bucket.lower()}_put_signal",
            )

        elif call_signal and put_signal:
            print(f"{bucket} CALL & PUT both elevated → volatility spike, no directional {bucket} signal.")
        else:
            print(f"No {bucket} directional signal. Z-scores not all > 1.5.")

    # Run buckets (ATM included now)
    handle_bucket("ATM")
    handle_bucket("OTM_1")
    handle_bucket("OTM_2")

    con.close()
