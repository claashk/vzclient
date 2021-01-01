from datetime import datetime, timedelta

EPOCH = datetime(year=1970, month=1, day=1)
 
def time(t):
    """Convert timestamp to datetime object

    Arguments:
        t (int): Timestamp [ms since EPOCH]

    Return:
        datetime.datetime: Datetime object representing the same time as `t`
    """
    return EPOCH + timedelta(milliseconds=t)


def timestamp(t):
    """Convert datetime object to timestamp

    Arguments:
        t (datetime.datetime): Datetime object

    Return:
        int: Timestamp [ms since EPOCH]
    """
    return int(1000. * (t - EPOCH).total_seconds() + 0.5)


def now():
    return timestamp(datetime.utcnow())
