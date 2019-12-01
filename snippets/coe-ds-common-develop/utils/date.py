"""
Date Utilities
"""

from datetime import datetime, timedelta
from dateutil.rrule import rrule, DAILY


def date_range(dt_start, until, in_format=None, out_format=None, by=None, freq=DAILY):
    """

    :param dt_start:
    :type dt_start:
    :param until:
    :type until:
    :param in_format:
    :type in_format:
    :param out_format:
    :type out_format:
    :param by:
    :type by:
    :return:
    :rtype:
    """
    if in_format:
        dt_start = datetime.strptime(dt_start, in_format)
        until = datetime.strptime(until, in_format)
    reverse = False
    if dt_start > until:
        reverse = True
        newstart = until
        until = dt_start
        dt_start = newstart
    dt_range = rrule(freq, dtstart=dt_start, until=until)
    if out_format:
        dt_range = [dt.strftime(out_format) for dt in dt_range]
    else:
        dt_range = [dt for dt in dt_range]
    # Omit duplicates in case the out_format gives less unique values than daily
    dt_range = list(set(dt_range))
    dt_range.sort(reverse=reverse)
    if by:
        dt_range = [dt_range[pos:pos + by] for pos in range(0, len(dt_range), by)]
    return dt_range


def extend_date_range(dt_range, delta=None, in_format=None):
    if not delta:
        return list(set(dt_range))
    if in_format:
        dt_range = [datetime.strptime(dt, in_format) for dt in dt_range]
    if isinstance(delta, int):
        delta = [delta]
    ext_range = dt_range
    for d in delta:
        new_range = [dt + timedelta(days=d) for dt in dt_range]
        ext_range = list(set(ext_range) & set(new_range))
    if in_format:
        ext_range = [dt.strftime(in_format) for dt in ext_range]
    return ext_range
