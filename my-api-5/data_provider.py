import calendar
import logging
import random
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Tuple

# default fallback; overridden dynamically by app.py
flip_rate = 0.3
log = logging.getLogger(__name__)

_SESSIONS = ("asia", "europe", "us", "off")


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Return the date of the nth weekday (0=Monday) for a given month/year."""
    if n <= 0:
        raise ValueError("n must be >= 1")
    cal = calendar.Calendar()
    count = 0
    for day, wd in ((d, d.weekday()) for d in cal.itermonthdates(year, month) if d.month == month):
        if wd == weekday:
            count += 1
            if count == n:
                return day
    raise ValueError("Invalid nth weekday request")


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Return the date of the last weekday (0=Monday) for a given month/year."""
    cal = calendar.Calendar()
    last = None
    for day, wd in ((d, d.weekday()) for d in cal.itermonthdates(year, month) if d.month == month):
        if wd == weekday:
            last = day
    if last is None:
        raise ValueError("Invalid weekday/month combination")
    return last


@lru_cache(maxsize=16)
def _holiday_cache(year: int) -> set[date]:
    """Compute major US federal holidays plus Halloween and Black Friday."""
    holidays: set[date] = set()
    holidays.add(date(year, 1, 1))   # New Year's Day
    holidays.add(_nth_weekday_of_month(year, 1, calendar.MONDAY, 3))  # MLK Day
    holidays.add(_nth_weekday_of_month(year, 2, calendar.MONDAY, 3))  # Presidents' Day
    holidays.add(_last_weekday_of_month(year, 5, calendar.MONDAY))    # Memorial Day
    holidays.add(date(year, 6, 19))  # Juneteenth
    holidays.add(date(year, 7, 4))   # Independence Day
    holidays.add(_nth_weekday_of_month(year, 9, calendar.MONDAY, 1))  # Labor Day
    holidays.add(_nth_weekday_of_month(year, 10, calendar.MONDAY, 2)) # Columbus Day
    holidays.add(date(year, 10, 31))  # Halloween
    holidays.add(date(year, 11, 11))  # Veterans Day
    thanksgiving = _nth_weekday_of_month(year, 11, calendar.THURSDAY, 4)
    holidays.add(thanksgiving)
    holidays.add(thanksgiving + timedelta(days=1))  # Black Friday
    holidays.add(date(year, 12, 25))  # Christmas
    return holidays


def is_day_off(dt: datetime) -> bool:
    """Return True for weekends or major US holidays (UTC)."""
    if dt.weekday() >= 5:
        return True
    return dt.date() in _holiday_cache(dt.year)


def get_session_label(dt: datetime) -> str:
    """
    Map a UTC datetime to a coarse "session" bucket, aligned with normalize script.

    Sessions (UTC):
      - "asia"   : 00–06
      - "europe" : 07–12
      - "us"     : 13–20
      - "off"    : 21–23
    """
    h = dt.hour
    if 0 <= h <= 6:
        return "asia"
    if 7 <= h <= 12:
        return "europe"
    if 13 <= h <= 20:
        return "us"
    return "off"


def _bucket_for_datetime(dt: datetime) -> Tuple[str, str]:
    """
    Convert a datetime into (workday/day_off, session) bucket labels.
    """
    day_type = "day_off" if is_day_off(dt) else "workday"
    session = get_session_label(dt)
    return day_type, session


def _select_bucket(
    crypto: str,
    dt: datetime,
    data_ready: Dict[str, Any],
) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Resolve the appropriate nested bucket for a given asset & datetime.

    Returns:
        (ok, bucket_key_str, bucket_dict_or_empty)
    """
    if data_ready is None:
        log.critical("data_ready not provided")
        return False, "", {}

    day_type, session = _bucket_for_datetime(dt)
    bucket_key = f"{day_type}:{session}"

    try:
        asset_data = data_ready[crypto]
        bucket = asset_data[day_type][session]
        return True, bucket_key, bucket
    except KeyError as e:
        log.critical("Missing bucket %s for asset %s: %s", bucket_key, crypto, e)
        return False, bucket_key, {}


def get_randomized_point(
    crypto: str = "btc",
    dt: datetime | None = None,
    data_ready: Dict[str, Any] | None = None,
    picker=None,
    scheme=None,
):
    """
    Retrieve a random point for a given asset & datetime.

    Uses weekday/weekend + session (asia/europe/us/off) buckets.
    """
    if dt is None:
        log.critical("dt not provided to get_randomized_point")
        return False, 0.0

    ok, bucket_key, bucket = _select_bucket(crypto, dt, data_ready or {})
    if not ok:
        return False, 0.0

    try:
        point_data = bucket[0]
        if not point_data:
            log.critical("No point data available for %s %s", crypto, bucket_key)
            return False, 0.0
        point = random.choice(point_data)
        if random.random() < flip_rate:
            point = -point
        return True, point
    except Exception as e:
        log.exception("Error in get_randomized_point(%s, %s): %s", crypto, bucket_key, e)
        return False, 0.0


def get_randomized_interval(
    crypto: str = "btc",
    dt: datetime | None = None,
    data_ready: Dict[str, Any] | None = None,
    picker=None,
    scheme=None,
):
    """
    Retrieve a random interval for a given asset & datetime.

    Uses weekday/weekend + session (asia/europe/us/off) buckets.
    """
    if dt is None:
        log.critical("dt not provided to get_randomized_interval")
        return False, [0.0, 0.0]

    ok, bucket_key, bucket = _select_bucket(crypto, dt, data_ready or {})
    if not ok:
        return False, [0.0, 0.0]

    try:
        interval_data = bucket[1]
        if not interval_data:
            log.critical("No interval data available for %s %s", crypto, bucket_key)
            return False, [0.0, 0.0]
        interval = random.choice(interval_data)
        if random.random() < flip_rate:
            interval = [-interval[1], -interval[0]]
        return True, interval
    except Exception as e:
        log.exception("Error in get_randomized_interval(%s, %s): %s", crypto, bucket_key, e)
        return False, [0.0, 0.0]


def get_rnd_point_interval(
    crypto: str = "btc",
    dt: datetime | None = None,
    data_ready: Dict[str, Any] | None = None,
    picker=None,
    scheme=None,
):
    """
    Retrieve a random (point, interval) pair for a given asset & datetime.

    Uses weekday/weekend + session (asia/europe/us/off) buckets.
    """
    if dt is None:
        log.critical("dt not provided to get_rnd_point_interval")
        return False, 0.0, [0.0, 0.0]

    ok, bucket_key, bucket = _select_bucket(crypto, dt, data_ready or {})
    if not ok:
        return False, 0.0, [0.0, 0.0]

    try:
        point_data = bucket[0]
        interval_data = bucket[1]
        if not point_data or not interval_data:
            log.critical("Empty point/interval bucket for %s %s", crypto, bucket_key)
            return False, 0.0, [0.0, 0.0]
        if len(point_data) != len(interval_data):
            log.critical("Point vs. Interval data length mismatch for %s %s", crypto, bucket_key)
            return False, 0.0, [0.0, 0.0]
        n = len(point_data)
        idx = random.randrange(n)
        point = point_data[idx]
        interval = interval_data[idx]
        if random.random() < flip_rate:
            point = -point
            interval = [-interval[1], -interval[0]]
        return True, point, interval
    except Exception as e:
        log.exception("Error in get_rnd_point_interval(%s, %s): %s", crypto, bucket_key, e)
        return False, 0.0, [0.0, 0.0]

