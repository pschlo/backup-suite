from datetime import timedelta


class TimeUnit:
    SECONDS = 10
    MINUTES = 20
    HOURS = 30
    DAYS = 40
    WEEKS = 50

    MAX = WEEKS


# convert duration to combination of units
# WARN: handling of negative durations is equal to timedelta.__str__, i.e. the largest unit becomes negative and the smaller ones positive
# this might be unexpected
def convert_duration(duration: int | timedelta, max_unit: int = TimeUnit.MAX) -> tuple[int,...]:
    if isinstance(duration, timedelta):
        duration = int(duration.total_seconds())

    seconds = duration
    if max_unit == TimeUnit.SECONDS:
        return (seconds,)

    seconds %= 60
    minutes = duration // 60
    if max_unit == TimeUnit.MINUTES:
        return (seconds, minutes)

    minutes %= 60
    hours = duration // (60*60)
    if max_unit == TimeUnit.HOURS:
        return (seconds, minutes, hours)
    
    hours %= 24
    days = duration // (60*60*24)
    if max_unit == TimeUnit.DAYS:
        return (seconds, minutes, hours, days)

    days %= 7
    weeks = duration // (60*60*24*7)
    return (seconds, minutes, hours, days, weeks)


# generate human-readable string for a time duration
# computes absolute value of duration and appends sign to result
def format_duration(duration: int | timedelta, max_unit: int = TimeUnit.MAX) -> str:
    if isinstance(duration, timedelta):
        duration = int(duration.total_seconds())
    is_pos: bool = duration >= 0
    duration = abs(duration)

    values = convert_duration(duration, max_unit)
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
    return res if is_pos else '- ' + res
