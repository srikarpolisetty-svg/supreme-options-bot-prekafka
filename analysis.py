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

    groups = load_all_groups(con, symbol)

    if groups is None:
     return f"no data {symbol}"

    # ===== ATM CALL =====
    atm_call = get_option_metrics(groups, "ATM_CALL")
    atm_call_short = atm_call["short"]

    z_price_atm_2_3day  = atm_call_short["z_price"]
    z_volume_atm_2_3day = atm_call_short["z_volume"]
    z_iv_atm_2_3day     = atm_call_short["z_iv"]
    strike_atm          = atm_call_short["strike"]
    price_atm           = atm_call_short["price"]
    symbol_atm          = atm_call_short["symbol"]
    snapshot_id_atmcall = atm_call_short["snapshot_id"]

    atm_call_long = atm_call["long"]
    z_price_atm_5w         = atm_call_long["z_price"]
    z_volume_atm_5w        = atm_call_long["z_volume"]
    z_iv_atm_5w            = atm_call_long["z_iv"]
    snapshot_id_atmcall_5w = atm_call_long["snapshot_id"]

    atm_call_signal = (
        z_price_atm_5w  > 1.5 and
        z_volume_atm_5w > 1.5 and
        z_iv_atm_5w     > 1.5 and
        z_price_atm_2_3day  > 1.5 and
        z_volume_atm_2_3day > 1.5 and
        z_iv_atm_2_3day     > 1.5
    )

    # ===== ATM PUT =====
    atm_put = get_option_metrics(groups, "ATM_PUT")
    atm_put_short = atm_put["short"]

    z_price_atm_put_2_3day  = atm_put_short["z_price"]
    z_volume_atm_put_2_3day = atm_put_short["z_volume"]
    z_iv_atm_put_2_3day     = atm_put_short["z_iv"]
    strike_atm_put          = atm_put_short["strike"]
    price_atm_put           = atm_put_short["price"]
    symbol_atm_put          = atm_put_short["symbol"]
    snapshot_id_atmput      = atm_put_short["snapshot_id"]

    atm_put_long = atm_put["long"]
    z_price_atm_put_5w    = atm_put_long["z_price"]
    z_volume_atm_put_5w   = atm_put_long["z_volume"]
    z_iv_atm_put_5w       = atm_put_long["z_iv"]
    snapshot_id_atmput_5w = atm_put_long["snapshot_id"]

    atm_put_signal = (
        z_price_atm_put_5w  > 1.5 and
        z_volume_atm_put_5w > 1.5 and
        z_iv_atm_put_5w     > 1.5 and
        z_price_atm_put_2_3day  > 1.5 and
        z_volume_atm_put_2_3day > 1.5 and
        z_iv_atm_put_2_3day     > 1.5
    )

    # ===== DECISION LOGIC (ATM) =====
    if atm_call_signal and not atm_put_signal:
        send_text(
            f"🚀 STRONG ATM CALL SIGNAL\n\n"
            f"Symbol: {symbol_atm}\n"
            f"Strike: {strike_atm}\n"
            f"Option Price (mid): {price_atm}\n\n"
            f"All price/volume/IV Z-scores > 1.5 in BOTH 2–3 day and 5-week windows.\n"
            f"Strong UPWARD pressure likely."
        )
        print("ALERT SENT (ATM CALL)")

        update_signal(
            con,
            symbol=symbol_atm,
            short_snapshot_id=snapshot_id_atmcall,
            long_snapshot_id=snapshot_id_atmcall_5w,
            call_put="C",
            bucket="ATM",
            signal_column="atm_call_signal"
        )

    elif atm_put_signal and not atm_call_signal:
        send_text(
            f"⚠️ STRONG ATM PUT SIGNAL\n\n"
            f"Symbol: {symbol_atm_put}\n"
            f"Strike: {strike_atm_put}\n"
            f"Option Price (mid): {price_atm_put}\n\n"
            f"All price/volume/IV Z-scores > 1.5 in BOTH 2–3 day and 5-week windows.\n"
            f"Strong DOWNWARD pressure likely."
        )
        print("ALERT SENT (ATM PUT)")

        update_signal(
            con,
            symbol=symbol_atm_put,
            short_snapshot_id=snapshot_id_atmput,
            long_snapshot_id=snapshot_id_atmput_5w,
            call_put="P",
            bucket="ATM",
            signal_column="atm_put_signal"
        )

    elif atm_call_signal and atm_put_signal:
        print("ATM CALL & PUT both elevated → volatility spike, no directional ATM signal.")
    else:
        print("No ATM directional signal. Z-scores not all > 1.5.")

    # ===== OTM_1 CALL =====
    otm1_call = get_option_metrics(groups, "OTM_1_CALL")
    otm1_call_short = otm1_call["short"]

    z_price_otm1_2_3day   = otm1_call_short["z_price"]
    z_volume_otm1_2_3day  = otm1_call_short["z_volume"]
    z_iv_otm1_2_3day      = otm1_call_short["z_iv"]
    strike_otm1           = otm1_call_short["strike"]
    price_otm1            = otm1_call_short["price"]
    symbol_otm1           = otm1_call_short["symbol"]
    snapshot_id_otm1_call = otm1_call_short["snapshot_id"]

    otm1_call_long = otm1_call["long"]
    z_price_otm1_5w         = otm1_call_long["z_price"]
    z_volume_otm1_5w        = otm1_call_long["z_volume"]
    z_iv_otm1_5w            = otm1_call_long["z_iv"]
    snapshot_id_5w_otm1call = otm1_call_long["snapshot_id"]

    otm1_call_signal = (
        z_price_otm1_5w  > 1.5 and
        z_volume_otm1_5w > 1.5 and
        z_iv_otm1_5w     > 1.5 and
        z_price_otm1_2_3day  > 1.5 and
        z_volume_otm1_2_3day > 1.5 and
        z_iv_otm1_2_3day     > 1.5
    )

    # ===== OTM_1 PUT =====
    otm1_put = get_option_metrics(groups, "OTM_1_PUT")
    otm1_put_short = otm1_put["short"]

    z_price_otm1_put_2_3day  = otm1_put_short["z_price"]
    z_volume_otm1_put_2_3day = otm1_put_short["z_volume"]
    z_iv_otm1_put_2_3day     = otm1_put_short["z_iv"]
    strike_otm1_put          = otm1_put_short["strike"]
    price_otm1_put           = otm1_put_short["price"]
    symbol_otm1_put          = otm1_put_short["symbol"]
    snapshot_id_otm1_put2_3  = otm1_put_short["snapshot_id"]

    otm1_put_long = otm1_put["long"]
    z_price_otm1_put_5w    = otm1_put_long["z_price"]
    z_volume_otm1_put_5w   = otm1_put_long["z_volume"]
    z_iv_otm1_put_5w       = otm1_put_long["z_iv"]
    snapshot_id_5w_otmput1 = otm1_put_long["snapshot_id"]

    otm1_put_signal = (
        z_price_otm1_put_5w  > 1.5 and
        z_volume_otm1_put_5w > 1.5 and
        z_iv_otm1_put_5w     > 1.5 and
        z_price_otm1_put_2_3day  > 1.5 and
        z_volume_otm1_put_2_3day > 1.5 and
        z_iv_otm1_put_2_3day     > 1.5
    )

    # ===== OTM_1 DECISION =====
    if otm1_call_signal and not otm1_put_signal:
        send_text(
            f"🚀 STRONG OTM_1 CALL SIGNAL\n\n"
            f"Symbol: {symbol_otm1}\n"
            f"Strike: {strike_otm1}\n"
            f"Option Price (mid): {price_otm1}\n\n"
            f"All price/volume/IV Z-scores > 1.5 in BOTH 2–3 day and 5-week windows."
        )
        print("ALERT SENT (OTM_1 CALL)")

        update_signal(
            con,
            symbol=symbol_otm1,
            short_snapshot_id=snapshot_id_otm1_call,
            long_snapshot_id=snapshot_id_5w_otm1call,
            call_put="C",
            bucket="OTM_1",
            signal_column="otm1_call_signal"
        )

    elif otm1_put_signal and not otm1_call_signal:
        send_text(
            f"⚠️ STRONG OTM_1 PUT SIGNAL\n\n"
            f"Symbol: {symbol_otm1_put}\n"
            f"Strike: {strike_otm1_put}\n"
            f"Option Price (mid): {price_otm1_put}\n\n"
            f"All price/volume/IV Z-scores > 1.5 in BOTH 2–3 day and 5-week windows."
        )
        print("ALERT SENT (OTM_1 PUT)")

        update_signal(
            con,
            symbol=symbol_otm1_put,
            short_snapshot_id=snapshot_id_otm1_put2_3,
            long_snapshot_id=snapshot_id_5w_otmput1,
            call_put="P",
            bucket="OTM_1",
            signal_column="otm1_put_signal"
        )

    elif otm1_call_signal and otm1_put_signal:
        print("OTM_1 CALL & PUT both elevated → volatility spike, no directional OTM_1 signal.")
    else:
        print("No OTM_1 directional signal. Z-scores not all > 1.5.")

    # ===== OTM_2 CALL =====
    otm2_call = get_option_metrics(groups, "OTM_2_CALL")
    otm2_call_short = otm2_call["short"]

    z_price_otm2_2_3day  = otm2_call_short["z_price"]
    z_volume_otm2_2_3day = otm2_call_short["z_volume"]
    z_iv_otm2_2_3day     = otm2_call_short["z_iv"]
    strike_otm2          = otm2_call_short["strike"]
    price_otm2           = otm2_call_short["price"]
    symbol_otm2          = otm2_call_short["symbol"]
    snapshot_id_otm2call = otm2_call_short["snapshot_id"]

    otm2_call_long = otm2_call["long"]
    z_price_otm2_5w          = otm2_call_long["z_price"]
    z_volume_otm2_5w         = otm2_call_long["z_volume"]
    z_iv_otm2_5w             = otm2_call_long["z_iv"]
    snapshot_id_otm2call_5w  = otm2_call_long["snapshot_id"]

    otm2_call_signal = (
        z_price_otm2_5w  > 1.5 and
        z_volume_otm2_5w > 1.5 and
        z_iv_otm2_5w     > 1.5 and
        z_price_otm2_2_3day  > 1.5 and
        z_volume_otm2_2_3day > 1.5 and
        z_iv_otm2_2_3day     > 1.5
    )

    # ===== OTM_2 PUT =====
    otm2_put = get_option_metrics(groups, "OTM_2_PUT")
    otm2_put_short = otm2_put["short"]

    z_price_otm2_put_2_3day  = otm2_put_short["z_price"]
    z_volume_otm2_put_2_3day = otm2_put_short["z_volume"]
    z_iv_otm2_put_2_3day     = otm2_put_short["z_iv"]
    strike_otm2_put          = otm2_put_short["strike"]
    price_otm2_put           = otm2_put_short["price"]
    symbol_otm2_put          = otm2_put_short["symbol"]
    snapshot_id_otm2put      = otm2_put_short["snapshot_id"]

    otm2_put_long = otm2_put["long"]
    z_price_otm2_put_5w    = otm2_put_long["z_price"]
    z_volume_otm2_put_5w   = otm2_put_long["z_volume"]
    z_iv_otm2_put_5w       = otm2_put_long["z_iv"]
    snapshot_id_otm2put_5w = otm2_put_long["snapshot_id"]

    otm2_put_signal = (
        z_price_otm2_put_5w  > 1.5 and
        z_volume_otm2_put_5w > 1.5 and
        z_iv_otm2_put_5w     > 1.5 and
        z_price_otm2_put_2_3day  > 1.5 and
        z_volume_otm2_put_2_3day > 1.5 and
        z_iv_otm2_put_2_3day     > 1.5
    )

    # ===== OTM_2 DECISION =====
    if otm2_call_signal and not otm2_put_signal:
        send_text(
            f"🚀 STRONG OTM_2 CALL SIGNAL\n\n"
            f"Symbol: {symbol_otm2}\n"
            f"Strike: {strike_otm2}\n"
            f"Option Price (mid): {price_otm2}\n\n"
            f"All price/volume/IV Z-scores > 1.5 in BOTH 2–3 day and 5-week windows."
        )
        print("ALERT SENT (OTM_2 CALL)")

        update_signal(
            con,
            symbol=symbol_otm2,
            short_snapshot_id=snapshot_id_otm2call,
            long_snapshot_id=snapshot_id_otm2call_5w,
            call_put="C",
            bucket="OTM_2",
            signal_column="otm2_call_signal"
        )

    elif otm2_put_signal and not otm2_call_signal:
        send_text(
            f"⚠️ STRONG OTM_2 PUT SIGNAL\n\n"
            f"Symbol: {symbol_otm2_put}\n"
            f"Strike: {strike_otm2_put}\n"
            f"Option Price (mid): {price_otm2_put}\n\n"
            f"All price/volume/IV Z-scores > 1.5 in BOTH 2–3 day and 5-week windows."
        )
        print("ALERT SENT (OTM_2 PUT)")

        update_signal(
            con,
            symbol=symbol_otm2_put,
            short_snapshot_id=snapshot_id_otm2put,
            long_snapshot_id=snapshot_id_otm2put_5w,
            call_put="P",
            bucket="OTM_2",
            signal_column="otm2_put_signal"
        )

    elif otm2_call_signal and otm2_put_signal:
        print("OTM_2 CALL & PUT both elevated → volatility spike, no directional OTM_2 signal.")
    else:
        print("No OTM_2 directional signal. Z-scores not all > 1.5.")

    con.close()
