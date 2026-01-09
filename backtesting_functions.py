
import duckdb




#tests how signal is doing in database 3 
def backtest_signal(
    con,
    moneyness: str,
    call_put: str,
    signal_col: str
):
    query = f"""
        SELECT
            timestamp,
            symbol,
            strike,
            call_put,
            moneyness_bucket,

            opt_ret_10m,
            opt_ret_1h,
            opt_ret_eod,
            opt_ret_next_open,
            opt_ret_1d,
            opt_ret_exp
        FROM option_snapshots_execution_signals
        WHERE moneyness_bucket = '{moneyness}'
          AND call_put = '{call_put}'
          AND {signal_col} = TRUE
    """
    return con.execute(query).df()






#general backtesting function 


def backtest_returns(
    con,
    moneyness: str,
    call_put: str,

    # numeric filters (min/max)
    strike_min: float | None = None,
    strike_max: float | None = None,

    dte_min: int | None = None,
    dte_max: int | None = None,

    bid_min: float | None = None,
    bid_max: float | None = None,
    ask_min: float | None = None,
    ask_max: float | None = None,
    mid_min: float | None = None,
    mid_max: float | None = None,

    volume_min: int | None = None,
    volume_max: int | None = None,
    open_interest_min: int | None = None,
    open_interest_max: int | None = None,

    iv_min: float | None = None,
    iv_max: float | None = None,

    spread_min: float | None = None,
    spread_max: float | None = None,
    spread_pct_min: float | None = None,
    spread_pct_max: float | None = None,

    # bucket / categorical filters
    time_decay_bucket: str | None = None,

    # z-score filters
    mid_z_min: float | None = None,
    mid_z_max: float | None = None,
    volume_z_min: float | None = None,
    volume_z_max: float | None = None,
    iv_z_min: float | None = None,
    iv_z_max: float | None = None,
):
    query = f"""
        SELECT
            timestamp,
            symbol,
            strike,
            call_put,
            moneyness_bucket,

            opt_ret_10m,
            opt_ret_1h,
            opt_ret_eod,
            opt_ret_next_open,
            opt_ret_1d,
            opt_ret_exp
        FROM option_snapshots_enriched
        WHERE moneyness_bucket = '{moneyness}'
          AND call_put = '{call_put}'

          {f"AND strike >= {strike_min}" if strike_min is not None else ""}
          {f"AND strike <= {strike_max}" if strike_max is not None else ""}

          {f"AND days_to_expiry >= {dte_min}" if dte_min is not None else ""}
          {f"AND days_to_expiry <= {dte_max}" if dte_max is not None else ""}

          {f"AND bid >= {bid_min}" if bid_min is not None else ""}
          {f"AND bid <= {bid_max}" if bid_max is not None else ""}

          {f"AND ask >= {ask_min}" if ask_min is not None else ""}
          {f"AND ask <= {ask_max}" if ask_max is not None else ""}

          {f"AND mid >= {mid_min}" if mid_min is not None else ""}
          {f"AND mid <= {mid_max}" if mid_max is not None else ""}

          {f"AND volume >= {volume_min}" if volume_min is not None else ""}
          {f"AND volume <= {volume_max}" if volume_max is not None else ""}

          {f"AND open_interest >= {open_interest_min}" if open_interest_min is not None else ""}
          {f"AND open_interest <= {open_interest_max}" if open_interest_max is not None else ""}

          {f"AND iv >= {iv_min}" if iv_min is not None else ""}
          {f"AND iv <= {iv_max}" if iv_max is not None else ""}

          {f"AND spread >= {spread_min}" if spread_min is not None else ""}
          {f"AND spread <= {spread_max}" if spread_max is not None else ""}

          {f"AND spread_pct >= {spread_pct_min}" if spread_pct_min is not None else ""}
          {f"AND spread_pct <= {spread_pct_max}" if spread_pct_max is not None else ""}

          {f"AND time_decay_bucket = '{time_decay_bucket}'" if time_decay_bucket else ""}

          {f"AND mid_z >= {mid_z_min}" if mid_z_min is not None else ""}
          {f"AND mid_z <= {mid_z_max}" if mid_z_max is not None else ""}

          {f"AND volume_z >= {volume_z_min}" if volume_z_min is not None else ""}
          {f"AND volume_z <= {volume_z_max}" if volume_z_max is not None else ""}

          {f"AND iv_z >= {iv_z_min}" if iv_z_min is not None else ""}
          {f"AND iv_z <= {iv_z_max}" if iv_z_max is not None else ""}
    """
    return con.execute(query).df()





def backtest_returns_5w(
    con,
    moneyness: str,
    call_put: str,

    # numeric filters (min/max)
    strike_min: float | None = None,
    strike_max: float | None = None,

    dte_min: int | None = None,
    dte_max: int | None = None,

    bid_min: float | None = None,
    bid_max: float | None = None,
    ask_min: float | None = None,
    ask_max: float | None = None,
    mid_min: float | None = None,
    mid_max: float | None = None,

    volume_min: int | None = None,
    volume_max: int | None = None,
    open_interest_min: int | None = None,
    open_interest_max: int | None = None,

    iv_min: float | None = None,
    iv_max: float | None = None,

    spread_min: float | None = None,
    spread_max: float | None = None,
    spread_pct_min: float | None = None,
    spread_pct_max: float | None = None,

    # bucket / categorical filters
    time_decay_bucket: str | None = None,

    # z-score filters
    mid_z_min: float | None = None,
    mid_z_max: float | None = None,
    volume_z_min: float | None = None,
    volume_z_max: float | None = None,
    iv_z_min: float | None = None,
    iv_z_max: float | None = None,
):
    query = f"""
        SELECT
            timestamp,
            symbol,
            strike,
            call_put,
            moneyness_bucket,

            opt_ret_10m,
            opt_ret_1h,
            opt_ret_eod,
            opt_ret_next_open,
            opt_ret_1d,
            opt_ret_exp
        FROM option_snapshots_enriched_5w
        WHERE moneyness_bucket = '{moneyness}'
          AND call_put = '{call_put}'

          {f"AND strike >= {strike_min}" if strike_min is not None else ""}
          {f"AND strike <= {strike_max}" if strike_max is not None else ""}

          {f"AND days_to_expiry >= {dte_min}" if dte_min is not None else ""}
          {f"AND days_to_expiry <= {dte_max}" if dte_max is not None else ""}

          {f"AND bid >= {bid_min}" if bid_min is not None else ""}
          {f"AND bid <= {bid_max}" if bid_max is not None else ""}

          {f"AND ask >= {ask_min}" if ask_min is not None else ""}
          {f"AND ask <= {ask_max}" if ask_max is not None else ""}

          {f"AND mid >= {mid_min}" if mid_min is not None else ""}
          {f"AND mid <= {mid_max}" if mid_max is not None else ""}

          {f"AND volume >= {volume_min}" if volume_min is not None else ""}
          {f"AND volume <= {volume_max}" if volume_max is not None else ""}

          {f"AND open_interest >= {open_interest_min}" if open_interest_min is not None else ""}
          {f"AND open_interest <= {open_interest_max}" if open_interest_max is not None else ""}

          {f"AND iv >= {iv_min}" if iv_min is not None else ""}
          {f"AND iv <= {iv_max}" if iv_max is not None else ""}

          {f"AND spread >= {spread_min}" if spread_min is not None else ""}
          {f"AND spread <= {spread_max}" if spread_max is not None else ""}

          {f"AND spread_pct >= {spread_pct_min}" if spread_pct_min is not None else ""}
          {f"AND spread_pct <= {spread_pct_max}" if spread_pct_max is not None else ""}

          {f"AND time_decay_bucket = '{time_decay_bucket}'" if time_decay_bucket else ""}

          {f"AND mid_z >= {mid_z_min}" if mid_z_min is not None else ""}
          {f"AND mid_z <= {mid_z_max}" if mid_z_max is not None else ""}

          {f"AND volume_z >= {volume_z_min}" if volume_z_min is not None else ""}
          {f"AND volume_z <= {volume_z_max}" if volume_z_max is not None else ""}

          {f"AND iv_z >= {iv_z_min}" if iv_z_min is not None else ""}
          {f"AND iv_z <= {iv_z_max}" if iv_z_max is not None else ""}
    """
    return con.execute(query).df()