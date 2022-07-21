from datetime import datetime, timedelta, timezone
from typing import Optional


class TimeUnit:
    SECONDS = 10
    MINUTES = 20
    HOURS = 30
    DAYS = 40
    WEEKS = 50

    MAX = WEEKS


# computes timedelta from dt to current time
# negative if dt in the past, positive if in future
def get_delta_to_now(dt: datetime):
    # create string representing time left until next run
    # datetime.now() returns a naive datetime equal to the current system time
    # to get aware current system time: first get as UTC, then cast to local time zone
    # TODO: remove timezone.utc ?
    curr_time = datetime.now(timezone.utc).astimezone()
    return dt - curr_time


# convert duration to combination of units
# WARN: handling of negative durations is equal to timedelta.__str__, i.e. the largest unit becomes negative and the smaller ones positive
# this might be unexpected
def convert_timedelta(delta: int | timedelta, max_unit: int = TimeUnit.MAX) -> tuple[int,...]:
    if isinstance(delta, timedelta):
        delta = int(delta.total_seconds())

    seconds = delta
    if max_unit == TimeUnit.SECONDS:
        return (seconds,)

    seconds %= 60
    minutes = delta // 60
    if max_unit == TimeUnit.MINUTES:
        return (seconds, minutes)

    minutes %= 60
    hours = delta // (60*60)
    if max_unit == TimeUnit.HOURS:
        return (seconds, minutes, hours)
    
    hours %= 24
    days = delta // (60*60*24)
    if max_unit == TimeUnit.DAYS:
        return (seconds, minutes, hours, days)

    days %= 7
    weeks = delta // (60*60*24*7)
    return (seconds, minutes, hours, days, weeks)


# generate human-readable string for a time duration
# computes absolute value of duration and appends sign to result
# appends optional prefix to beginning
# returns now argument if duration is zero
def format_timedelta(delta: int | timedelta, max_unit: int = TimeUnit.MAX, prefix: str = '', now: str='') -> str:
    if isinstance(delta, timedelta):
        delta = int(delta.total_seconds())
    is_pos: bool = delta >= 0
    delta = abs(delta)

    if delta == 0:
        return now

    values = convert_timedelta(delta, max_unit)
    # only take relevant part of list
    names = ['second', 'minute', 'hour', 'day', 'week'][:len(values)]

    # iterate in reverse order because most relevant unit should come first
    res = ''
    for name, value in reversed(list(zip(names, values))):
        if value > 1:
            res += f'{value} {name}s '
        elif value == 1:
            res += f'{value} {name} '
        else:
            # do not append if value is zero
            pass
    res = res[:-1]

    # insert sign
    if not is_pos:
        res = '- ' + res

    # append prefix
    if prefix:
        res = f'{prefix} {res}'
    
    return res


# formats a datetime to human-readable string
# datetime must be aware
def format_datetime(dt: datetime) -> str:
    # retrieve UTC offset
    utc_offset: Optional[timedelta] = dt.utcoffset()
    if utc_offset is None:
        raise ValueError('Invalid UTC offset')

    # create UTC offset string
    sign = '+' if utc_offset >= timedelta(0) else '-'
    _, minutes, hours = convert_timedelta(abs(utc_offset), TimeUnit.HOURS)
    utc_offset_str = '%s%02d:%02d' % (sign, hours, minutes)

    return dt.strftime(f'%Y-%m-%d %H:%M:%S UTC{utc_offset_str}')