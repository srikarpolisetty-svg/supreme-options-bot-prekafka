import yfinance as yf
import datetime
import duckdb
import numpy as np
import pytz
import sys
from zoneinfo import ZoneInfo
import exchange_calendars as ecals
import pandas as pd
from databasefunctions import compute_z_scores_for_bucket_5w
from databasefunctions import get_closest_strike, get_option_quote
import os




def ingest_option_snapshot_5w(symbol: str, shard_id: int, run_id: str):
    NY_TZ = ZoneInfo("America/New_York")
    XNYS = ecals.get_calendar("XNYS")  # NYSE

    now1 = datetime.datetime.now(NY_TZ)

    # True only if the exchange is actually open right now (includes holidays/early closes)
    if not XNYS.is_open_on_minute(now1, ignore_breaks=True):
        print(f"Market closed (holiday/after-hours) — skipping insert. now={now1}")
        sys.exit(0)

    est = pytz.timezone("America/New_York")

    def py(x):
        # convert numpy numbers → plain Python types
        if isinstance(x, (np.integer,)):
            return int(x)
        if isinstance(x, (np.floating,)):
            return float(x)
        return x

    # ---------- STEP 1: Get this week's Friday chain ----------
    now = datetime.datetime.now()
    print(now.strftime("%Y-%m-%d %H:%M"))
    now_dateobject = now.date()

    stock = yf.Ticker(symbol)
    expirations = stock.options

    def get_friday_within_4_days():
        for exp in expirations:
            d = datetime.datetime.strptime(exp, "%Y-%m-%d").date()

            is_friday = d.weekday() == 4
            is_within_4_days = (d - now_dateobject).days <= 4
            is_third_friday = is_friday and 15 <= d.day <= 21  # monthly exp

            if is_friday and is_within_4_days and not is_third_friday:
                return exp

        return None

    exp = get_friday_within_4_days()

    if exp is None:
        print(
            f"{symbol}: no valid weekly Friday expiration (skipping 3rd Friday / monthly-only)."
        )
        return None # <-- do NOT sys.exit

    chain = stock.option_chain(exp)

    # current price
    atm = stock.info["currentPrice"]

    # 1–2% and 3–4% OTM targets
    otm_call_1_strike = atm * 1.015
    otm_put_1_strike = atm * 0.985

    otm_call_2_strike = atm * 1.035
    otm_put_2_strike = atm * 0.965

    # ---------- STEP 2: Find closest strikes ----------
    closest_atm_call = get_closest_strike(chain, "C", atm)
    closest_atm_put = get_closest_strike(chain, "P", atm)

    otm_call_1_closest = get_closest_strike(chain, "C", otm_call_1_strike)
    otm_put_1_closest = get_closest_strike(chain, "P", otm_put_1_strike)

    otm_call_2_closest = get_closest_strike(chain, "C", otm_call_2_strike)
    otm_put_2_closest = get_closest_strike(chain, "P", otm_put_2_strike)

    # OCC strike formatting
    atm_call_option_strike_OCC = f"{int(closest_atm_call * 1000):08d}"
    atm_put_option_strike_OCC = f"{int(closest_atm_put * 1000):08d}"

    otm1_call_option_strike_OCC = f"{int(otm_call_1_closest * 1000):08d}"
    otm1_put_option_strike_OCC = f"{int(otm_put_1_closest * 1000):08d}"

    otm2_call_option_strike_OCC = f"{int(otm_call_2_closest * 1000):08d}"
    otm2_put_option_strike_OCC = f"{int(otm_put_2_closest * 1000):08d}"

    # ---------- STEP 3: Quotes ----------
    atm_call_q = get_option_quote(chain, "C", closest_atm_call)
    atm_call_bid = atm_call_q["bid"]
    atm_call_ask = atm_call_q["ask"]
    atm_call_mid = atm_call_q["mid"]
    atm_call_volume = atm_call_q["volume"]
    atm_call_iv = atm_call_q["iv"]
    atm_call_oi = atm_call_q["oi"]
    atm_call_spread = atm_call_q["spread"]
    atm_call_spread_pct = atm_call_q["spread_pct"]

    atm_put_q = get_option_quote(chain, "P", closest_atm_put)
    atm_put_bid = atm_put_q["bid"]
    atm_put_ask = atm_put_q["ask"]
    atm_put_mid = atm_put_q["mid"]
    atm_put_volume = atm_put_q["volume"]
    atm_put_iv = atm_put_q["iv"]
    atm_put_oi = atm_put_q["oi"]
    atm_put_spread = atm_put_q["spread"]
    atm_put_spread_pct = atm_put_q["spread_pct"]

    otm1_call_q = get_option_quote(chain, "C", otm_call_1_closest)
    otm_call_1_bid = otm1_call_q["bid"]
    otm_call_1_ask = otm1_call_q["ask"]
    otm_call_1_mid = otm1_call_q["mid"]
    otm_call_1_volume = otm1_call_q["volume"]
    otm_call_1_iv = otm1_call_q["iv"]
    otm_call_1_oi = otm1_call_q["oi"]
    otm_call_1_spread = otm1_call_q["spread"]
    otm_call_1_spread_pct = otm1_call_q["spread_pct"]

    otm1_put_q = get_option_quote(chain, "P", otm_put_1_closest)
    otm_put_1_bid = otm1_put_q["bid"]
    otm_put_1_ask = otm1_put_q["ask"]
    otm_put_1_mid = otm1_put_q["mid"]
    otm_put_1_volume = otm1_put_q["volume"]
    otm_put_1_iv = otm1_put_q["iv"]
    otm_put_1_oi = otm1_put_q["oi"]
    otm_put_1_spread = otm1_put_q["spread"]
    otm_put_1_spread_pct = otm1_put_q["spread_pct"]

    otm2_call_q = get_option_quote(chain, "C", otm_call_2_closest)
    otm_call_2_bid = otm2_call_q["bid"]
    otm_call_2_ask = otm2_call_q["ask"]
    otm_call_2_mid = otm2_call_q["mid"]
    otm_call_2_volume = otm2_call_q["volume"]
    otm_call_2_iv = otm2_call_q["iv"]
    otm_call_2_oi = otm2_call_q["oi"]
    otm_call_2_spread = otm2_call_q["spread"]
    otm_call_2_spread_pct = otm2_call_q["spread_pct"]

    otm2_put_q = get_option_quote(chain, "P", otm_put_2_closest)
    otm_put_2_bid = otm2_put_q["bid"]
    otm_put_2_ask = otm2_put_q["ask"]
    otm_put_2_mid = otm2_put_q["mid"]
    otm_put_2_volume = otm2_put_q["volume"]
    otm_put_2_iv = otm2_put_q["iv"]
    otm_put_2_oi = otm2_put_q["oi"]
    otm_put_2_spread = otm2_put_q["spread"]
    otm_put_2_spread_pct = otm2_put_q["spread_pct"]

    # ---------- STEP 4: Timestamp / buckets ----------
    now_est = datetime.datetime.now(est)
    timestamp = now_est.strftime("%Y-%m-%d %H:%M:%S")

    snapshot_id = f"{symbol}_{timestamp}"
    print(snapshot_id)

    expiration = exp
    exp_date = datetime.datetime.strptime(expiration, "%Y-%m-%d").date()
    days_till_expiry = (exp_date - now_dateobject).days

    if days_till_expiry <= 1:
        time_decay_bucket = "EXTREME"
    elif days_till_expiry <= 3:
        time_decay_bucket = "HIGH"
    elif days_till_expiry <= 7:
        time_decay_bucket = "MEDIUM"
    else:
        time_decay_bucket = "LOW"

    option_symbol_atm_call = f"{symbol}{exp_date}C{atm_call_option_strike_OCC}"
    option_symbol_atm_put = f"{symbol}{exp_date}P{atm_put_option_strike_OCC}"
    option_symbol_otm1_call = f"{symbol}{exp_date}C{otm1_call_option_strike_OCC}"
    option_symbol_otm1_put = f"{symbol}{exp_date}P{otm1_put_option_strike_OCC}"
    option_symbol_otm2_call = f"{symbol}{exp_date}C{otm2_call_option_strike_OCC}"
    option_symbol_otm2_put = f"{symbol}{exp_date}P{otm2_put_option_strike_OCC}"

    # ======================
    # DB
    # ======================





    cols1 = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "option_symbol",
        "strike",
        "call_put",
        "days_to_expiry",
        "expiration_date",
        "moneyness_bucket",
        "bid",
        "ask",
        "mid",
        "volume",
        "open_interest",
        "iv",
        "spread",
        "spread_pct",
        "time_decay_bucket",
    ]

    rows1 = [
        # ===== ATM CALL =====
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_atm_call,
            closest_atm_call,
            "C",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_call_bid,
            atm_call_ask,
            atm_call_mid,
            atm_call_volume,
            atm_call_oi,
            atm_call_iv,
            atm_call_spread,
            atm_call_spread_pct,
            time_decay_bucket,
        ],
        # ===== ATM PUT =====
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_atm_put,
            closest_atm_put,
            "P",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_put_bid,
            atm_put_ask,
            atm_put_mid,
            atm_put_volume,
            atm_put_oi,
            atm_put_iv,
            atm_put_spread,
            atm_put_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM CALL 1 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm1_call,
            otm_call_1_closest,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_call_1_bid,
            otm_call_1_ask,
            otm_call_1_mid,
            otm_call_1_volume,
            otm_call_1_oi,
            otm_call_1_iv,
            otm_call_1_spread,
            otm_call_1_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM PUT 1 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm1_put,
            otm_put_1_closest,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_put_1_bid,
            otm_put_1_ask,
            otm_put_1_mid,
            otm_put_1_volume,
            otm_put_1_oi,
            otm_put_1_iv,
            otm_put_1_spread,
            otm_put_1_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM CALL 2 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm2_call,
            otm_call_2_closest,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_call_2_bid,
            otm_call_2_ask,
            otm_call_2_mid,
            otm_call_2_volume,
            otm_call_2_oi,
            otm_call_2_iv,
            otm_call_2_spread,
            otm_call_2_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM PUT 2 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm2_put,
            otm_put_2_closest,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_put_2_bid,
            otm_put_2_ask,
            otm_put_2_mid,
            otm_put_2_volume,
            otm_put_2_oi,
            otm_put_2_iv,
            otm_put_2_spread,
            otm_put_2_spread_pct,
            time_decay_bucket,
        ],
    ]

    df1 = pd.DataFrame(rows1, columns=cols1)


    out_dir = f"runs/{run_id}/option_snapshots_raw_5w"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df1.to_parquet(out_path, index=False)

    # ===== Z-SCORES (5W) =====
    atm_call_z, atm_call_vol_z, atm_call_iv_z = compute_z_scores_for_bucket_5w(
        symbol=symbol,
        bucket="ATM",
        call_put="C",
        time_decay_bucket=time_decay_bucket,
        current_mid=atm_call_mid,
        current_volume=atm_call_volume,
        current_iv=atm_call_iv,
    )
    atm_put_z, atm_put_vol_z, atm_put_iv_z = compute_z_scores_for_bucket_5w(
        symbol=symbol,
        bucket="ATM",
        call_put="P",
        time_decay_bucket=time_decay_bucket,
        current_mid=atm_put_mid,
        current_volume=atm_put_volume,
        current_iv=atm_put_iv,
    )
    otm_call_1_z, otm_call_1_vol_z, otm_call_1_iv_z = compute_z_scores_for_bucket_5w(
        symbol=symbol,
        bucket="OTM_1",
        call_put="C",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_call_1_mid,
        current_volume=otm_call_1_volume,
        current_iv=otm_call_1_iv,
    )
    otm_put_1_z, otm_put_1_vol_z, otm_put_1_iv_z = compute_z_scores_for_bucket_5w(
        symbol=symbol,
        bucket="OTM_1",
        call_put="P",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_put_1_mid,
        current_volume=otm_put_1_volume,
        current_iv=otm_put_1_iv,
    )
    otm_call_2_z, otm_call_2_vol_z, otm_call_2_iv_z = compute_z_scores_for_bucket_5w(
        symbol=symbol,
        bucket="OTM_2",
        call_put="C",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_call_2_mid,
        current_volume=otm_call_2_volume,
        current_iv=otm_call_2_iv,
    )
    otm_put_2_z, otm_put_2_vol_z, otm_put_2_iv_z = compute_z_scores_for_bucket_5w(
        symbol=symbol,
        bucket="OTM_2",
        call_put="P",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_put_2_mid,
        current_volume=otm_put_2_volume,
        current_iv=otm_put_2_iv,
    )

    # ======================
    # ENRICHED (5W)
    # ======================
    


    


    cols2 = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "option_symbol",
        "strike",
        "call_put",
        "days_to_expiry",
        "expiration_date",
        "moneyness_bucket",
        "bid",
        "ask",
        "mid",
        "volume",
        "open_interest",
        "iv",
        "spread",
        "spread_pct",
        "time_decay_bucket",
        "mid_z",
        "volume_z",
        "iv_z",
        "opt_ret_10m",
        "opt_ret_1h",
        "opt_ret_eod",
        "opt_ret_next_open",
        "opt_ret_1d",
        "opt_ret_exp",
    ]

    rows2 = [
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_atm_call,
            closest_atm_call,
            "C",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_call_bid,
            atm_call_ask,
            atm_call_mid,
            atm_call_volume,
            atm_call_oi,
            atm_call_iv,
            atm_call_spread,
            atm_call_spread_pct,
            time_decay_bucket,
            atm_call_z,
            atm_call_vol_z,
            atm_call_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_atm_put,
            closest_atm_put,
            "P",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_put_bid,
            atm_put_ask,
            atm_put_mid,
            atm_put_volume,
            atm_put_oi,
            atm_put_iv,
            atm_put_spread,
            atm_put_spread_pct,
            time_decay_bucket,
            atm_put_z,
            atm_put_vol_z,
            atm_put_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm1_call,
            otm_call_1_closest,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_call_1_bid,
            otm_call_1_ask,
            otm_call_1_mid,
            otm_call_1_volume,
            otm_call_1_oi,
            otm_call_1_iv,
            otm_call_1_spread,
            otm_call_1_spread_pct,
            time_decay_bucket,
            otm_call_1_z,
            otm_call_1_vol_z,
            otm_call_1_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm1_put,
            otm_put_1_closest,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_put_1_bid,
            otm_put_1_ask,
            otm_put_1_mid,
            otm_put_1_volume,
            otm_put_1_oi,
            otm_put_1_iv,
            otm_put_1_spread,
            otm_put_1_spread_pct,
            time_decay_bucket,
            otm_put_1_z,
            otm_put_1_vol_z,
            otm_put_1_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm2_call,
            otm_call_2_closest,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_call_2_bid,
            otm_call_2_ask,
            otm_call_2_mid,
            otm_call_2_volume,
            otm_call_2_oi,
            otm_call_2_iv,
            otm_call_2_spread,
            otm_call_2_spread_pct,
            time_decay_bucket,
            otm_call_2_z,
            otm_call_2_vol_z,
            otm_call_2_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm2_put,
            otm_put_2_closest,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_put_2_bid,
            otm_put_2_ask,
            otm_put_2_mid,
            otm_put_2_volume,
            otm_put_2_oi,
            otm_put_2_iv,
            otm_put_2_spread,
            otm_put_2_spread_pct,
            time_decay_bucket,
            otm_put_2_z,
            otm_put_2_vol_z,
            otm_put_2_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    ]

    df2 = pd.DataFrame(rows2, columns=cols2)

    out_dir = f"runs/{run_id}/option_snapshots_enriched_5w"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df2.to_parquet(out_path, index=False)

    # ======================
    # EXECUTION SIGNALS (5W)
    # ======================


    cols3 = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "option_symbol",
        "strike",
        "call_put",
        "days_to_expiry",
        "expiration_date",
        "moneyness_bucket",
        "bid",
        "ask",
        "mid",
        "volume",
        "open_interest",
        "iv",
        "spread",
        "spread_pct",
        "time_decay_bucket",
        "mid_z",
        "volume_z",
        "iv_z",
        "opt_ret_10m",
        "opt_ret_1h",
        "opt_ret_eod",
        "opt_ret_next_open",
        "opt_ret_1d",
        "opt_ret_exp",
        "atm_call_signal",
        "atm_put_signal",
        "otm1_call_signal",
        "otm1_put_signal",
        "otm2_call_signal",
        "otm2_put_signal",
    ]

    rows3 = [
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_atm_call,
            closest_atm_call,
            "C",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_call_bid,
            atm_call_ask,
            atm_call_mid,
            atm_call_volume,
            atm_call_oi,
            atm_call_iv,
            atm_call_spread,
            atm_call_spread_pct,
            time_decay_bucket,
            atm_call_z,
            atm_call_vol_z,
            atm_call_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_atm_put,
            closest_atm_put,
            "P",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_put_bid,
            atm_put_ask,
            atm_put_mid,
            atm_put_volume,
            atm_put_oi,
            atm_put_iv,
            atm_put_spread,
            atm_put_spread_pct,
            time_decay_bucket,
            atm_put_z,
            atm_put_vol_z,
            atm_put_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm1_call,
            otm_call_1_closest,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_call_1_bid,
            otm_call_1_ask,
            otm_call_1_mid,
            otm_call_1_volume,
            otm_call_1_oi,
            otm_call_1_iv,
            otm_call_1_spread,
            otm_call_1_spread_pct,
            time_decay_bucket,
            otm_call_1_z,
            otm_call_1_vol_z,
            otm_call_1_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm1_put,
            otm_put_1_closest,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_put_1_bid,
            otm_put_1_ask,
            otm_put_1_mid,
            otm_put_1_volume,
            otm_put_1_oi,
            otm_put_1_iv,
            otm_put_1_spread,
            otm_put_1_spread_pct,
            time_decay_bucket,
            otm_put_1_z,
            otm_put_1_vol_z,
            otm_put_1_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm2_call,
            otm_call_2_closest,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_call_2_bid,
            otm_call_2_ask,
            otm_call_2_mid,
            otm_call_2_volume,
            otm_call_2_oi,
            otm_call_2_iv,
            otm_call_2_spread,
            otm_call_2_spread_pct,
            time_decay_bucket,
            otm_call_2_z,
            otm_call_2_vol_z,
            otm_call_2_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
        [
            snapshot_id,
            timestamp,
            symbol,
            option_symbol_otm2_put,
            otm_put_2_closest,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_put_2_bid,
            otm_put_2_ask,
            otm_put_2_mid,
            otm_put_2_volume,
            otm_put_2_oi,
            otm_put_2_iv,
            otm_put_2_spread,
            otm_put_2_spread_pct,
            time_decay_bucket,
            otm_put_2_z,
            otm_put_2_vol_z,
            otm_put_2_iv_z,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        ],
    ]

    df3 = pd.DataFrame(rows3, columns=cols3)

    out_dir = f"runs/{run_id}/option_snapshots_execution_signals_5w"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df3.to_parquet(out_path, index=False)
    


    
    # ======================
    # MASTER INGEST (5W) - embedded, no params
    # ======================



import duckdb

import duckdb


def master_ingest_5w(run_id: str, db_path: str = "options_data.db"):
    """
    Master ingest for a single 5W options run_id.
    Reads parquet written by shards and inserts into existing tables.
    """

    raw_dir = f"runs/{run_id}/option_snapshots_raw_5w"
    enriched_dir = f"runs/{run_id}/option_snapshots_enriched_5w"
    signals_dir = f"runs/{run_id}/option_snapshots_execution_signals_5w"

    con = duckdb.connect(db_path)

    try:
        con.execute("BEGIN;")

        con.execute("""
            INSERT INTO option_snapshots_execution_signals_5w
            SELECT * FROM read_parquet(?)
        """, [f"{signals_dir}/shard_*.parquet"])

        con.execute("""
            INSERT INTO option_snapshots_enriched_5w
            SELECT * FROM read_parquet(?)
        """, [f"{enriched_dir}/shard_*.parquet"])

        con.execute("""
            INSERT INTO option_snapshots_raw_5w
            SELECT * FROM read_parquet(?)
        """, [f"{raw_dir}/shard_*.parquet"])

        con.execute("COMMIT;")

    except Exception:
        con.execute("ROLLBACK;")
        raise

    finally:
        con.close()

