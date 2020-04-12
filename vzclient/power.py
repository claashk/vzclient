from .time_derivative import TimeDerivative


class Power(object):
    """Convert energy readings into power
    
    Calculates the mean power between two readings of an energy meter.
    
    Arguments:
        last (tuple): Last reading and timestamp of last reading. Defaults to
            ``(None, None)``
    """
    def __init__(self, last=(None, None)):
        self.dE_dt = TimeDerivative(last=last)

    def __call__(self, reading=-1., timestamp=None):
        return self.update(reading, timestamp)
    
    @property
    def last_update(self):
        """Get last reading and timestamp of last reading
        
        Return:
            tuple: reading and timestamp
        """
        return self.dE_dt._last
    
    @property
    def timestamp(self):
        """Timestamp of last reading
        
        Return:
            float: seconds since the epoch at last update
        """
        return self.dE_dt.last_timestamp

    def update(self, reading=-1., timestamp=None):
        """Calculate mean power between last reading and new reading
        
        
        Arguments:
            reading (float): Latest meter reading
            timestamp (float): Timestamp of latest reading
            
        Return:
            float: Mean power between the latest reading and the reading before.
        """
        if timestamp is not None:
            t = 0.001 * timestamp  # timestamp [ms] -> t[s]
        else:
            t = None

        s = reading if reading != -1. else None
        p = self.dE_dt(s, t)

        if p is not None:
            # ds [Wh], dt [s]: 1 Wh = 3600 Ws = 3.6 kWs
            p *= 3.6
 
        return p
