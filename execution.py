from datetime import datetime, timezone
import sys
from zoneinfo import ZoneInfo
import exchange_calendars as ecals
import duckdb

from config import SECRET_KEY, ACCOUNT_ID
from execution_functions import (
    get_access_token,
    get_portfolio,
    to_float,
    get_daily_unrealized_pnl,
    preflight_single_leg_option,
    place_equity_order,
    place_close_order,
    place_stop_close_order,
    trail_exit_signals,
)


def run_execution_engine(symbol: str):
    NY_TZ = ZoneInfo("America/New_York")
    XNYS = ecals.get_calendar("XNYS")  # NYSE

    now1 = datetime.now(NY_TZ)

    # True only if the exchange is actually open right now (includes holidays/early closes)
    if not XNYS.is_open_on_minute(now1, ignore_breaks=True):
        print(f"Market closed (holiday/after-hours) — skipping insert. now={now1}")
        sys.exit(0)

    token_response = get_access_token(SECRET_KEY)
    data = get_portfolio(ACCOUNT_ID, token_response)

    options_bp = to_float(data["buyingPower"]["optionsBuyingPower"])

    positions = [
        p for p in data.get("positions", [])
        if (p.get("instrument", {}) or {}).get("type") == "OPTION"
    ]

    if not positions:
        print("No positions to close")

    orders = [
        o for o in data.get("orders", [])
        if (o.get("instrument", {}) or {}).get("type") == "OPTION"
    ]

    PER_TRADE_RISK_PCT = 0.02
    PER_DAY_RISK_PCT = 0.04

    max_risk_per_trade = options_bp * PER_TRADE_RISK_PCT
    max_risk_per_day = options_bp * PER_DAY_RISK_PCT

    execute_trades = False

    ACTIVE_STATUSES = {"NEW", "PARTIALLY_FILLED"}

    active_buy_open_market = [
        o for o in orders
        if o.get("side") == "BUY"
        and o.get("openCloseIndicator") == "OPEN"
        and o.get("type") == "MARKET"
        and o.get("status") in ACTIVE_STATUSES
    ]

    num_buy_open_market = len(active_buy_open_market)

    if num_buy_open_market >= 5:
        execute_trades = False
    else:
        execute_trades = True

    now = datetime.now(timezone.utc)

    for order in active_buy_open_market:
        created_at_str = order.get("createdAt")
        if not created_at_str:
            continue

        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))

        if (now - created_at).total_seconds() <= 15 * 60:
            execute_trades = False
            continue

    daily_unrealized = get_daily_unrealized_pnl(data)

    if daily_unrealized < 0 and abs(daily_unrealized) >= max_risk_per_day:
        for pos in positions:
            place_close_order(
                ACCOUNT_ID,
                token_response,
                order_id=pos["orderId"],
                symbol=pos["instrument"]["symbol"],
                side="SELL",          # closing a long
                order_type="MARKET",
                quantity=pos["quantity"],
            )

    con = duckdb.connect("options_data.db")

    df = con.execute(
        """
        SELECT *
        FROM options_snapshots_enriched
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT 6
        """,
        [symbol],
    ).df()

    con.close()

    signal_cols = [
        "atm_call_signal",
        "atm_put_signal",
        "otm1_call_signal",
        "otm1_put_signal",
        "otm2_call_signal",
        "otm2_put_signal",
    ]

    for row in df.itertuples(index=False):
        fired_signals = [col for col in signal_cols if getattr(row, col)]

        if not fired_signals:
            continue

        # Preflight risk check
        flight = preflight_single_leg_option(
            account_id=ACCOUNT_ID,
            access_token=token_response,
            symbol=row.option_symbol,
            quantity=1,
        )

        if flight.get("estimatedCost", float("inf")) <= max_risk_per_trade:
            place_equity_order(
                account_id=ACCOUNT_ID,
                access_token=token_response,
                symbol=row.option_symbol,
                side="BUY",
                quantity=2,
                execute=False,  # keep as your current behavior
            )

    # ================
    # PROFIT / RISK MGMT
    # ================
    positions = data.get("positions", [])

    if positions:
        for pos in positions:
            # ✅ only options
            if (pos.get("instrument", {}) or {}).get("type") != "OPTION":
                continue

            option_return = to_float((pos.get("costBasis") or {}).get("gainPercentage"))

            if option_return is None:
                continue

            if option_return >= 25:
                entry_price = to_float((pos.get("costBasis") or {}).get("unitCost"))
                if entry_price is None:
                    continue

                stop_price = entry_price  # breakeven stop

                place_stop_close_order(
                    ACCOUNT_ID,
                    token_response,
                    symbol=pos["instrument"]["symbol"],
                    side="SELL",
                    quantity=pos["quantity"],
                    stop_price=stop_price,
                )

    positions = data.get("positions", [])

    if positions:
        for pos in positions:
            # ✅ only option positions
            if (pos.get("instrument") or {}).get("type") != "OPTION":
                continue

            option_return = to_float((pos.get("costBasis") or {}).get("gainPercentage"))
            if option_return is None:
                continue

            quantity = int(to_float(pos.get("quantity") or 0))

            pos_symbol = (pos.get("instrument") or {}).get("symbol")
            if not pos_symbol:
                continue

            if option_return >= 50 and quantity >= 2:
                place_close_order(
                    ACCOUNT_ID,
                    token_response,
                    pos_symbol,
                    quantity=1,
                )

    trail_exit_signals(data=data, token_response=token_response)

