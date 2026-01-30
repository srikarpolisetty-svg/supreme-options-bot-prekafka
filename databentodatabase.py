import os
import math
import datetime as dt
import datetime
import pytz
import pandas as pd
import databento as db

from databasefunctions import compute_z_scores_for_bucket
from databasefunctions import stabilize_schema
from config import DATABENTO_API_KEY



def run_databento_option_snapshot(run_id: str, symbol: str, shard_id: int):
    client = db.Historical(DATABENTO_API_KEY)  # uses DATABENTO_API_KEY env var

    now = dt.datetime.now(tz=pytz.UTC)

    data = client.timeseries.get_range(
        dataset="EQUS.MINI",  # common US equities dataset
        schema="ohlcv-1m",
        symbols=[symbol],
        start=(now - dt.timedelta(minutes=2)).isoformat(),
        end=now.isoformat(),
    )

    df = data.to_df()
    underlying_price = float(df["close"].iloc[-1])

    chain_df = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="definition",
        symbols=f"{symbol}.OPT",  # all {symbol} options under the parent
        stype_in="parent",        # parent symbology
        start=dt.date.today(),    # "as-of" day; can be a date
    ).to_df()

    # Useful columns typically include: raw_symbol, expiration, strike_price, instrument_class (C/P)
    chain_df["exp_yyyymmdd"] = chain_df["expiration"].dt.strftime("%Y%m%d")
    expirations = sorted(chain_df["exp_yyyymmdd"].unique())
    strikes = sorted(chain_df["strike_price"].astype(float).unique())

    def get_friday_within_4_days(expirations: list[str]) -> str | None:
        if not expirations:
            return None

        today = datetime.date.today()

        for exp in sorted(expirations):
            d = datetime.datetime.strptime(exp, "%Y%m%d").date()

            if (
                d.weekday() == 4                      # Friday
                and 0 <= (d - today).days <= 4        # within 4 days
                and not (15 <= d.day <= 21)           # not 3rd Friday
            ):
                return exp

        return None

    exp = get_friday_within_4_days(expirations)

    if exp is None:
        return "no expiration this friday"

    atm = underlying_price
    otm_call_1_target = atm * 1.015
    otm_put_1_target = atm * 0.985
    otm_call_2_target = atm * 1.035
    otm_put_2_target = atm * 0.965

    def get_closest_strike(target: float, strikes: list[float]) -> float:
        if not strikes:
            raise RuntimeError("No strikes available.")
        return float(min(strikes, key=lambda s: abs(float(s) - float(target))))

    atm_strike = get_closest_strike(atm, strikes)

    c1 = get_closest_strike(otm_call_1_target, strikes)
    p1 = get_closest_strike(otm_put_1_target, strikes)

    c2 = get_closest_strike(otm_call_2_target, strikes)
    p2 = get_closest_strike(otm_put_2_target, strikes)

    sub = chain_df[chain_df["exp_yyyymmdd"] == exp]

    raw_atm_c = sub[(sub["strike_price"] == atm_strike) & (sub["instrument_class"] == "C")]["raw_symbol"].iloc[0]
    raw_atm_p = sub[(sub["strike_price"] == atm_strike) & (sub["instrument_class"] == "P")]["raw_symbol"].iloc[0]

    raw_c1 = sub[(sub["strike_price"] == c1) & (sub["instrument_class"] == "C")]["raw_symbol"].iloc[0]
    raw_p1 = sub[(sub["strike_price"] == p1) & (sub["instrument_class"] == "P")]["raw_symbol"].iloc[0]

    raw_c2 = sub[(sub["strike_price"] == c2) & (sub["instrument_class"] == "C")]["raw_symbol"].iloc[0]
    raw_p2 = sub[(sub["strike_price"] == p2) & (sub["instrument_class"] == "P")]["raw_symbol"].iloc[0]

    df_quotes = client.timeseries.get_range(
        dataset="OPRA.PILLAR",
        schema="mbp-1",
        symbols=[raw_atm_c, raw_atm_p, raw_c1, raw_p1, raw_c2, raw_p2],
        stype_in="raw_symbol",
        start=(now - dt.timedelta(seconds=10)).isoformat(),
        end=now.isoformat(),
    ).to_df()

    def get_quote(raw_symbol):
        df = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="mbp-1",
            symbols=[raw_symbol],
            stype_in="raw_symbol",
            start=(now - dt.timedelta(seconds=10)).isoformat(),
            end=now.isoformat(),
        ).to_df()

        if df.empty:
            return None

        row = df.sort_values("ts_event").iloc[-1]

        bid = row["bid_px_00"]
        ask = row["ask_px_00"]

        if bid and ask and bid > 0 and ask > 0:
            mid = (bid + ask) / 2
            spread = ask - bid
            spread_pct = spread / mid
        else:
            mid = spread = spread_pct = None

        return {
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "spread": spread,
            "spread_pct": spread_pct,
        }

    # ===== ATM CALL =====
    q_atm_c = get_quote(raw_atm_c)
    atm_call_bid = q_atm_c["bid"]
    atm_call_ask = q_atm_c["ask"]
    atm_call_mid = q_atm_c["mid"]
    atm_call_spread = q_atm_c["spread"]
    atm_call_spread_pct = q_atm_c["spread_pct"]

    # ===== ATM PUT =====
    q_atm_p = get_quote(raw_atm_p)
    atm_put_bid = q_atm_p["bid"]
    atm_put_ask = q_atm_p["ask"]
    atm_put_mid = q_atm_p["mid"]
    atm_put_spread = q_atm_p["spread"]
    atm_put_spread_pct = q_atm_p["spread_pct"]

    # ===== OTM CALL 1 =====
    q_c1 = get_quote(raw_c1)
    otm_call_1_bid = q_c1["bid"]
    otm_call_1_ask = q_c1["ask"]
    otm_call_1_mid = q_c1["mid"]
    otm_call_1_spread = q_c1["spread"]
    otm_call_1_spread_pct = q_c1["spread_pct"]

    # ===== OTM PUT 1 =====
    q_p1 = get_quote(raw_p1)
    otm_put_1_bid = q_p1["bid"]
    otm_put_1_ask = q_p1["ask"]
    otm_put_1_mid = q_p1["mid"]
    otm_put_1_spread = q_p1["spread"]
    otm_put_1_spread_pct = q_p1["spread_pct"]

    # ===== OTM CALL 2 =====
    q_c2 = get_quote(raw_c2)
    otm_call_2_bid = q_c2["bid"]
    otm_call_2_ask = q_c2["ask"]
    otm_call_2_mid = q_c2["mid"]
    otm_call_2_spread = q_c2["spread"]
    otm_call_2_spread_pct = q_c2["spread_pct"]

    # ===== OTM PUT 2 =====
    q_p2 = get_quote(raw_p2)
    otm_put_2_bid = q_p2["bid"]
    otm_put_2_ask = q_p2["ask"]
    otm_put_2_mid = q_p2["mid"]
    otm_put_2_spread = q_p2["spread"]
    otm_put_2_spread_pct = q_p2["spread_pct"]

    def get_volume(raw_symbol):
        df = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="trades",
            symbols=[raw_symbol],
            stype_in="raw_symbol",
            start=(now - dt.timedelta(minutes=10)).isoformat(),
            end=now.isoformat(),
        ).to_df()
        return df["size"].sum() if not df.empty else 0

    vol_atm_c = get_volume(raw_atm_c)
    vol_atm_p = get_volume(raw_atm_p)

    vol_c1 = get_volume(raw_c1)
    vol_p1 = get_volume(raw_p1)

    vol_c2 = get_volume(raw_c2)
    vol_p2 = get_volume(raw_p2)

    def get_open_interest(raw_symbol):
        df = client.timeseries.get_range(
            dataset="OPRA.PILLAR",
            schema="open_interest",
            symbols=[raw_symbol],
            stype_in="raw_symbol",
            start=(dt.date.today() - dt.timedelta(days=1)),
            end=(dt.date.today() - dt.timedelta(days=1)),
        ).to_df()
        return df["open_interest"].iloc[0] if not df.empty else None

    oi_atm_c = get_open_interest(raw_atm_c)
    oi_atm_p = get_open_interest(raw_atm_p)

    oi_c1 = get_open_interest(raw_c1)
    oi_p1 = get_open_interest(raw_p1)

    oi_c2 = get_open_interest(raw_c2)
    oi_p2 = get_open_interest(raw_p2)

    def get_iv(mid, S, K, days_to_expiry, call_put):
        if mid is None or mid <= 0:
            return None

        T = days_to_expiry / 365.0
        if T <= 0:
            return None

        r = 0.01
        lo, hi = 1e-6, 5.0  # 0% → 500%

        def N(x):
            return 0.5 * (1 + math.erf(x / math.sqrt(2)))

        def bs_price(sigma):
            d1 = (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))
            d2 = d1 - sigma * math.sqrt(T)
            if call_put == "C":
                return S * N(d1) - K * math.exp(-r * T) * N(d2)
            else:
                return K * math.exp(-r * T) * N(-d2) - S * N(-d1)

        for _ in range(60):
            mid_sigma = 0.5 * (lo + hi)
            price = bs_price(mid_sigma)
            if price > mid:
                hi = mid_sigma
            else:
                lo = mid_sigma

        return 0.5 * (lo + hi)

    est = pytz.timezone("US/Eastern")
    now_est = datetime.datetime.now(est)

    timestamp = now_est.strftime("%Y-%m-%d %H:%M:%S")  # VALID TIMESTAMP
    snapshot_id = f"{symbol}_{timestamp}"

    exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()
    now_date = now_est.date()

    days_till_expiry = (exp_date - now_date).days

    if days_till_expiry <= 1:
        time_decay_bucket = "EXTREME"
    elif days_till_expiry <= 3:
        time_decay_bucket = "HIGH"
    elif days_till_expiry <= 7:
        time_decay_bucket = "MEDIUM"
    else:
        time_decay_bucket = "LOW"

    iv_atm_c = get_iv(q_atm_c["mid"], underlying_price, atm_strike, days_till_expiry, "C")
    iv_atm_p = get_iv(q_atm_p["mid"], underlying_price, atm_strike, days_till_expiry, "P")

    iv_c1 = get_iv(q_c1["mid"], underlying_price, c1, days_till_expiry, "C")
    iv_p1 = get_iv(q_p1["mid"], underlying_price, p1, days_till_expiry, "P")

    iv_c2 = get_iv(q_c2["mid"], underlying_price, c2, days_till_expiry, "C")
    iv_p2 = get_iv(q_p2["mid"], underlying_price, p2, days_till_expiry, "P")

    cols1 = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "underlying_price",
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
            underlying_price,
            atm_strike,
            "C",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_call_bid,
            atm_call_ask,
            atm_call_mid,
            vol_atm_c,
            oi_atm_c,
            iv_atm_c,
            atm_call_spread,
            atm_call_spread_pct,
            time_decay_bucket,
        ],
        # ===== ATM PUT =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            atm_strike,
            "P",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_put_bid,
            atm_put_ask,
            atm_put_mid,
            vol_atm_p,
            oi_atm_p,
            iv_atm_p,
            atm_put_spread,
            atm_put_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM CALL 1 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            c1,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_call_1_bid,
            otm_call_1_ask,
            otm_call_1_mid,
            vol_c1,
            oi_c1,
            iv_c1,
            otm_call_1_spread,
            otm_call_1_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM PUT 1 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            p1,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_put_1_bid,
            otm_put_1_ask,
            otm_put_1_mid,
            vol_p1,
            oi_p1,
            iv_p1,
            otm_put_1_spread,
            otm_put_1_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM CALL 2 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            c2,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_call_2_bid,
            otm_call_2_ask,
            otm_call_2_mid,
            vol_c2,
            oi_c2,
            iv_c2,
            otm_call_2_spread,
            otm_call_2_spread_pct,
            time_decay_bucket,
        ],
        # ===== OTM PUT 2 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            p2,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_put_2_bid,
            otm_put_2_ask,
            otm_put_2_mid,
            vol_p2,
            oi_p2,
            iv_p2,
            otm_put_2_spread,
            otm_put_2_spread_pct,
            time_decay_bucket,
        ],
    ]

    # ===== Raw =====
    df1 = pd.DataFrame(rows1, columns=cols1)
    df1 = stabilize_schema(df1)
    out_dir = f"runs/{run_id}/option_snapshots_raw"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df1.to_parquet(out_path, index=False)

    (
        atm_call_mid_z_3d,
        atm_call_vol_z_3d,
        atm_call_iv_z_3d,
        atm_call_mid_z_5w,
        atm_call_vol_z_5w,
        atm_call_iv_z_5w,
    ) = compute_z_scores_for_bucket(
        symbol=symbol,
        bucket="ATM",
        call_put="C",
        time_decay_bucket=time_decay_bucket,
        current_mid=atm_call_mid,
        current_volume=vol_atm_c,
        current_iv=iv_atm_c,
    )

    (
        atm_put_mid_z_3d,
        atm_put_vol_z_3d,
        atm_put_iv_z_3d,
        atm_put_mid_z_5w,
        atm_put_vol_z_5w,
        atm_put_iv_z_5w,
    ) = compute_z_scores_for_bucket(
        symbol=symbol,
        bucket="ATM",
        call_put="P",
        time_decay_bucket=time_decay_bucket,
        current_mid=atm_put_mid,
        current_volume=vol_atm_p,
        current_iv=iv_atm_p,
    )

    (
        otm_call_1_mid_z_3d,
        otm_call_1_vol_z_3d,
        otm_call_1_iv_z_3d,
        otm_call_1_mid_z_5w,
        otm_call_1_vol_z_5w,
        otm_call_1_iv_z_5w,
    ) = compute_z_scores_for_bucket(
        symbol=symbol,
        bucket="OTM_1",
        call_put="C",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_call_1_mid,
        current_volume=vol_c1,
        current_iv=iv_c1,
    )

    (
        otm_put_1_mid_z_3d,
        otm_put_1_vol_z_3d,
        otm_put_1_iv_z_3d,
        otm_put_1_mid_z_5w,
        otm_put_1_vol_z_5w,
        otm_put_1_iv_z_5w,
    ) = compute_z_scores_for_bucket(
        symbol=symbol,
        bucket="OTM_1",
        call_put="P",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_put_1_mid,
        current_volume=vol_p1,
        current_iv=iv_p1,
    )

    (
        otm_call_2_mid_z_3d,
        otm_call_2_vol_z_3d,
        otm_call_2_iv_z_3d,
        otm_call_2_mid_z_5w,
        otm_call_2_vol_z_5w,
        otm_call_2_iv_z_5w,
    ) = compute_z_scores_for_bucket(
        symbol=symbol,
        bucket="OTM_2",
        call_put="C",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_call_2_mid,
        current_volume=vol_c2,
        current_iv=iv_c2,
    )

    (
        otm_put_2_mid_z_3d,
        otm_put_2_vol_z_3d,
        otm_put_2_iv_z_3d,
        otm_put_2_mid_z_5w,
        otm_put_2_vol_z_5w,
        otm_put_2_iv_z_5w,
    ) = compute_z_scores_for_bucket(
        symbol=symbol,
        bucket="OTM_2",
        call_put="P",
        time_decay_bucket=time_decay_bucket,
        current_mid=otm_put_2_mid,
        current_volume=vol_p2,
        current_iv=iv_p2,
    )

    # ======================
    # ENRICHED (5W)
    # ======================
    cols2 = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "underlying_price",  # ← added to match RAW
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
        "mid_z_3d",
        "volume_z_3d",
        "iv_z_3d",
        "mid_z_5w",
        "volume_z_5w",
        "iv_z_5w",
        "opt_ret_10m",
        "opt_ret_1h",
        "opt_ret_eod",
        "opt_ret_next_open",
        "opt_ret_1d",
        "opt_ret_exp",
    ]

    rows2 = [
        # ===== ATM CALL =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            atm_strike,
            "C",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_call_bid,
            atm_call_ask,
            atm_call_mid,
            vol_atm_c,
            oi_atm_c,
            iv_atm_c,
            atm_call_spread,
            atm_call_spread_pct,
            time_decay_bucket,
            atm_call_mid_z_3d,
            atm_call_vol_z_3d,
            atm_call_iv_z_3d,
            atm_call_mid_z_5w,
            atm_call_vol_z_5w,
            atm_call_iv_z_5w,
            None, None, None, None, None, None,
        ],
        # ===== ATM PUT =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            atm_strike,
            "P",
            days_till_expiry,
            exp_date,
            "ATM",
            atm_put_bid,
            atm_put_ask,
            atm_put_mid,
            vol_atm_p,
            oi_atm_p,
            iv_atm_p,
            atm_put_spread,
            atm_put_spread_pct,
            time_decay_bucket,
            atm_put_mid_z_3d,
            atm_put_vol_z_3d,
            atm_put_iv_z_3d,
            atm_put_mid_z_5w,
            atm_put_vol_z_5w,
            atm_put_iv_z_5w,
            None, None, None, None, None, None,
        ],
        # ===== OTM CALL 1 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            c1,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_call_1_bid,
            otm_call_1_ask,
            otm_call_1_mid,
            vol_c1,
            oi_c1,
            iv_c1,
            otm_call_1_spread,
            otm_call_1_spread_pct,
            time_decay_bucket,
            otm_call_1_mid_z_3d,
            otm_call_1_vol_z_3d,
            otm_call_1_iv_z_3d,
            otm_call_1_mid_z_5w,
            otm_call_1_vol_z_5w,
            otm_call_1_iv_z_5w,
            None, None, None, None, None, None,
        ],
        # ===== OTM PUT 1 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            p1,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_1",
            otm_put_1_bid,
            otm_put_1_ask,
            otm_put_1_mid,
            vol_p1,
            oi_p1,
            iv_p1,
            otm_put_1_spread,
            otm_put_1_spread_pct,
            time_decay_bucket,
            otm_put_1_mid_z_3d,
            otm_put_1_vol_z_3d,
            otm_put_1_iv_z_3d,
            otm_put_1_mid_z_5w,
            otm_put_1_vol_z_5w,
            otm_put_1_iv_z_5w,
            None, None, None, None, None, None,
        ],
        # ===== OTM CALL 2 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            c2,
            "C",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_call_2_bid,
            otm_call_2_ask,
            otm_call_2_mid,
            vol_c2,
            oi_c2,
            iv_c2,
            otm_call_2_spread,
            otm_call_2_spread_pct,
            time_decay_bucket,
            otm_call_2_mid_z_3d,
            otm_call_2_vol_z_3d,
            otm_call_2_iv_z_3d,
            otm_call_2_mid_z_5w,
            otm_call_2_vol_z_5w,
            otm_call_2_iv_z_5w,
            None, None, None, None, None, None,
        ],
        # ===== OTM PUT 2 =====
        [
            snapshot_id,
            timestamp,
            symbol,
            underlying_price,
            p2,
            "P",
            days_till_expiry,
            exp_date,
            "OTM_2",
            otm_put_2_bid,
            otm_put_2_ask,
            otm_put_2_mid,
            vol_p2,
            oi_p2,
            iv_p2,
            otm_put_2_spread,
            otm_put_2_spread_pct,
            time_decay_bucket,
            otm_put_2_mid_z_3d,
            otm_put_2_vol_z_3d,
            otm_put_2_iv_z_3d,
            otm_put_2_mid_z_5w,
            otm_put_2_vol_z_5w,
            otm_put_2_iv_z_5w,
            None, None, None, None, None, None,
        ],
    ]

    # ===== Enriched =====
    df2 = pd.DataFrame(rows2, columns=cols2)
    df2 = stabilize_schema(df2)
    out_dir = f"runs/{run_id}/option_snapshots_enriched"
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
        "underlying_price",  # ← added
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
        "mid_z_3d",
        "volume_z_3d",
        "iv_z_3d",
        "mid_z_5w",
        "volume_z_5w",
        "iv_z_5w",
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
        # ===== ATM CALL =====
        [
            snapshot_id, timestamp, symbol, underlying_price,
            atm_strike, "C",
            days_till_expiry, exp_date, "ATM",
            atm_call_bid, atm_call_ask, atm_call_mid,
            vol_atm_c, oi_atm_c, iv_atm_c,
            atm_call_spread, atm_call_spread_pct,
            time_decay_bucket,
            atm_call_mid_z_3d, atm_call_vol_z_3d, atm_call_iv_z_3d,
            atm_call_mid_z_5w, atm_call_vol_z_5w, atm_call_iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ],
        # ===== ATM PUT =====
        [
            snapshot_id, timestamp, symbol, underlying_price,
            atm_strike, "P",
            days_till_expiry, exp_date, "ATM",
            atm_put_bid, atm_put_ask, atm_put_mid,
            vol_atm_p, oi_atm_p, iv_atm_p,
            atm_put_spread, atm_put_spread_pct,
            time_decay_bucket,
            atm_put_mid_z_3d, atm_put_vol_z_3d, atm_put_iv_z_3d,
            atm_put_mid_z_5w, atm_put_vol_z_5w, atm_put_iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ],
        # ===== OTM CALL 1 =====
        [
            snapshot_id, timestamp, symbol, underlying_price,
            c1, "C",
            days_till_expiry, exp_date, "OTM_1",
            otm_call_1_bid, otm_call_1_ask, otm_call_1_mid,
            vol_c1, oi_c1, iv_c1,
            otm_call_1_spread, otm_call_1_spread_pct,
            time_decay_bucket,
            otm_call_1_mid_z_3d, otm_call_1_vol_z_3d, otm_call_1_iv_z_3d,
            otm_call_1_mid_z_5w, otm_call_1_vol_z_5w, otm_call_1_iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ],
        # ===== OTM PUT 1 =====
        [
            snapshot_id, timestamp, symbol, underlying_price,
            p1, "P",
            days_till_expiry, exp_date, "OTM_1",
            otm_put_1_bid, otm_put_1_ask, otm_put_1_mid,
            vol_p1, oi_p1, iv_p1,
            otm_put_1_spread, otm_put_1_spread_pct,
            time_decay_bucket,
            otm_put_1_mid_z_3d, otm_put_1_vol_z_3d, otm_put_1_iv_z_3d,
            otm_put_1_mid_z_5w, otm_put_1_vol_z_5w, otm_put_1_iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ],
        # ===== OTM CALL 2 =====
        [
            snapshot_id, timestamp, symbol, underlying_price,
            c2, "C",
            days_till_expiry, exp_date, "OTM_2",
            otm_call_2_bid, otm_call_2_ask, otm_call_2_mid,
            vol_c2, oi_c2, iv_c2,
            otm_call_2_spread, otm_call_2_spread_pct,
            time_decay_bucket,
            otm_call_2_mid_z_3d, otm_call_2_vol_z_3d, otm_call_2_iv_z_3d,
            otm_call_2_mid_z_5w, otm_call_2_vol_z_5w, otm_call_2_iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ],
        # ===== OTM PUT 2 =====
        [
            snapshot_id, timestamp, symbol, underlying_price,
            p2, "P",
            days_till_expiry, exp_date, "OTM_2",
            otm_put_2_bid, otm_put_2_ask, otm_put_2_mid,
            vol_p2, oi_p2, iv_p2,
            otm_put_2_spread, otm_put_2_spread_pct,
            time_decay_bucket,
            otm_put_2_mid_z_3d, otm_put_2_vol_z_3d, otm_put_2_iv_z_3d,
            otm_put_2_mid_z_5w, otm_put_2_vol_z_5w, otm_put_2_iv_z_5w,
            None, None, None, None, None, None,
            None, None, None, None, None, None,
        ],
    ]

    # ===== Execution Signals =====
    df3 = pd.DataFrame(rows3, columns=cols3)
    df3 = stabilize_schema(df3)
    out_dir = f"runs/{run_id}/option_snapshots_execution_signals"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df3.to_parquet(out_path, index=False)


