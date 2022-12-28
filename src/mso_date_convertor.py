from datetime import datetime, timedelta
from pytz import timezone


def get_date(n_day):
    prague_tz = timezone('Europe/Prague')
    loc_dt = datetime.now(prague_tz)
    n_day_positive = abs(n_day)
    td = loc_dt - timedelta(days=n_day_positive)
    return td.strftime("%Y-%m-%d")
