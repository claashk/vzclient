from datetime import datetime


class TimeDerivative(object):
    """Calculate the derivative based on two consecutive measurements
    
    Arguments:
        last (tuple): Tuple containing two float values. The first float is a
            timestamp of the measurement in seconds since the epoch as would
            be returned by :meth:`Derivative.now` at the time of observation.
            The second value is the observed value (measurement).
    """
    def __init__(self, last=(None, None)):
        self._last = last #timestamp and reading

    @property
    def last_timestamp(self):
        return self._last[0]

    @property
    def last_value(self):
        return self._last[1]

    def __call__(self, value, timestamp=None):
        """Calculate the derivative
        
        Arguments:
            value (float): Observation (measured value)
            timestamp (float): Timestamp at which the observation was made.
                If ``None``, the timestamp will be generated at the time of
                invocation by a call to :meth:`Derivative.now`
        
        Return:
            float: Change between last measurement and the current one.
        """
        dy_dt = None
        t0, y0 = self._last
        
        if value is not None:
            y1 = value
        else:
            return dy_dt

        if timestamp is not None:
            t1 = timestamp
        else:
            t1 = self.now()

        self._last = (t1, y1)

        if t0 is not None and y0 is not None:
            dt = t1 - t0
            dy = y1 - y0

            if dt > 0.:
                dy_dt = dy / dt
        return dy_dt
            
    @staticmethod
    def now():
        """Get the current time as timestamp

        Equivalent to ``datetime.datetime.utcnow().timestamp()``

        Return:
            float: Current UTC time as timestamp
        """
        return datetime.utcnow().timestamp()

