from returns_script_functions import fill_return_label
import datetime
from returns_script_functions import fill_return_label_5w
from returns_script_functions import fill_return_label_executionsignals
from returns_script_functions import fill_return_label_executionsignals_5w
import sys
from zoneinfo import ZoneInfo
import exchange_calendars as ecals

NY_TZ = ZoneInfo("America/New_York")
XNYS = ecals.get_calendar("XNYS")  # NYSE

now1 = datetime.datetime.now(NY_TZ)

# True only if the exchange is actually open right now (includes holidays/early closes)
if not XNYS.is_open_on_minute(now1, ignore_breaks=True):
    print(f"Market closed (holiday/after-hours) — skipping insert. now={now1}")
    sys.exit(0)




# db 2 short term database 
fill_return_label(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)


fill_return_label(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)


fill_return_label(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE",
    order_dir="DESC"
)





fill_return_label(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)




fill_return_label(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)


fill_return_label(
    "opt_ret_exp",
    "DATE(f.timestamp) = base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE",
    order_dir="DESC"
)




# db 2 long term database 
fill_return_label_5w(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)

fill_return_label_5w(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)

fill_return_label_5w(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE",
    order_dir="DESC"
)


fill_return_label_5w(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)

fill_return_label_5w(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)

fill_return_label_5w(
    "opt_ret_exp",
    "DATE(f.timestamp) = base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE",
    order_dir="DESC"
)




# db 3 short term database 
fill_return_label_executionsignals(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)

fill_return_label_executionsignals(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)

fill_return_label_executionsignals(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE",
    order_dir="DESC"
)

fill_return_label_executionsignals(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)

fill_return_label_executionsignals(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)

fill_return_label_executionsignals(
    "opt_ret_exp",
    "DATE(f.timestamp) = base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE",
    order_dir="DESC"
)



# db 3 long term database 
fill_return_label_executionsignals_5w(
    "opt_ret_10m",
    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
)

fill_return_label_executionsignals_5w(
    "opt_ret_1h",
    "f.timestamp >= base.timestamp + INTERVAL 1 HOUR"
)

fill_return_label_executionsignals_5w(
    "opt_ret_eod",
    "DATE(f.timestamp) = DATE(base.timestamp)",
    "AND DATE(base.timestamp) < CURRENT_DATE",
    order_dir="DESC"
)

fill_return_label_executionsignals_5w(
    "opt_ret_next_open",
    "DATE(f.timestamp) > DATE(base.timestamp)"
)

fill_return_label_executionsignals_5w(
    "opt_ret_1d",
    "f.timestamp >= base.timestamp + INTERVAL 1 DAY"
)


fill_return_label_executionsignals_5w(
    "opt_ret_exp",
    "DATE(f.timestamp) = base.expiration_date",
    "AND base.expiration_date <= CURRENT_DATE",
    order_dir="DESC"
)













now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M"))



















