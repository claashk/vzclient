from datetime import datetime
import logging
import asyncio
from vzclient.constants import now, timestamp

logger = logging.getLogger("vzclient")


class DeviceReader(object):
    """Sample input from a device at given time intervals

    Arguments:
        reader (coroutine): Executed by the device reader in regular intervals
            to retrieve new values. Shall return a tuple containing a timestamp
            and the device reading at this timestamp. Instead of the timestamp
            a :class:`datetime.datetime` object or ``None`` are permissible, too.
        use_device_time(bool): If ``True``, the wrapper uses the native time read
            from the device, if not ``None``. Otherwise the local server utc time
            is used.
        sampling_interval (int): Desired sampling interval [ms].
        interpolate (bool): If ``True``, sampled values will be interpolated at
            integer multiples of the sampling interval. If ``False``, values
            will be returned exactly as read.
        allowed_errors (int): Maximum number of allowed read errors. Any positive
            number n will cause the reader to abort on the n-th error. Zero
            causes abort on the first error. A negative value causes the reader
            to ignore all errors.
        **kwarger: Keyword arguments passed verbatim to reader on each call
    """
    def __init__(self,
                 reader,
                 use_device_time=True,
                 sampling_interval=10000,
                 interpolate=True,
                 allowed_errors=-1,
                 name="<unknown>",
                 **kwargs):
        self.read_device = reader
        self.use_device_time = use_device_time
        self.sampling_interval = int(sampling_interval)
        self.interpolate = interpolate
        self.allowed_errors = allowed_errors
        self.name = name
        self._reader_args = kwargs

        self._t0 = None
        self._t1 = None
        self._y0 = None
        self._y1 = None
        self._exec_time = 10 * [0.]
        self._pos = 0
        self._stop = True

    @property
    def mean_exec_time(self):
        """Mean execution time (excluding sleep) [s]"""
        return sum(self._exec_time) / len(self._exec_time)

    async def __aiter__(self):
        """Iterate over readings from the device

        Yield:
            tuple: Timestamp and reading
        """
        if not self._stop:
            raise RuntimeError(f"Reader for device {self.name} already active")
        self._stop = False

        #initialize
        sleep_sec = 0.001 * self.sampling_interval
        is_ok = False
        while not self._stop and not is_ok:
            is_ok = await self.update()
            if not self.interpolate and is_ok:
                yield self.get_value()
            await asyncio.sleep(sleep_sec)

        # read out loop
        while not self._stop:
            is_ok = await self.update()
            if is_ok:
                self.update_exec_time(sleep_sec)
                if self.interpolate:
                    i = (self._t1 // self.sampling_interval)
                    t = i * self.sampling_interval
                    yield self.get_value(t)
                    # buffer to avoid sampling too early (5% of mean exec time)
                    dtmin = 0.05 * self.mean_exec_time
                    sleep_sec = dtmin + 0.001 * (
                                (i + 1) * self.sampling_interval - self._t1)
                else:
                    sleep_sec = 0.001 * self.sampling_interval
                    yield self.get_value()

                sleep_sec -= self.mean_exec_time
                if sleep_sec < 0.:
                    logger.warning(f"Mean execution time "
                                   f"({self.mean_exec_time:.3f} s) for device "
                                   f"{self.name} exceeds sampling interval by "
                                   f"{-1000 * sleep_sec:.0f} ms")
                    sleep_sec = 0.
            await asyncio.sleep(sleep_sec)
        self._stop = False

    async def stop(self, max_retries=5):
        """Stop the reader and wait until the loop is complete"""
        logger.debug(f"Stopping device reader {self.name} ...")
        if self._stop:
            return
        self._stop = True
        while max_retries >= 0:
            max_retries -= 1
            await asyncio.sleep(0.0005 * self.sampling_interval)
            if not self._stop:
                return True
        return False

    async def update(self):
        """Update readings from device"""
        try:
            logger.debug(f"Reading from device '{self.name}'")
            t, y = await self.read_device(**self._reader_args)
        except Exception as ex:
            logger.error(f"While reading from '{self.name}': {ex}")
            logger.debug(f"Errors remaining: {self.allowed_errors}")
            if self.allowed_errors == 0:
                raise
            self.allowed_errors -= 1
            return False

        if self.use_device_time and t is not None:
            if isinstance(t, datetime):
                t = timestamp(t)
            else:
                t = int(t)
        else:
            t = now()
        self._t0, self._y0 = self._t1, self._y1
        self._t1, self._y1 = t, y
        return True

    def update_exec_time(self, sleep_time=0.):
        """Update execution time estimate

        Arguments:
            sleep_time (float): Total sleep time between the last two
                measurements. This value is subtracted from the measured time
                between the last two update calls. Defaults to zero.
        """
        if self._t1 is None or self._t0 is None:
            return
        dt = 0.001 * (self._t1 - self._t0) - sleep_time
        self._exec_time[self._pos] = dt
        self._pos += 1
        if self._pos == len(self._exec_time):
            self._pos = 0

    def get_value(self, t=None):
        """Get last measurement

        Arguments:
            t (timestamp): Timestamp at which to interpolate the device reading.
                If ``None``, the last reading will be returned

        Return:
            tuple: Timestamp and value of measurement at time `t`
        """
        if self._t1 is None or self._y1 is None:
            raise ValueError("Insufficient data")

        if t is None:
            return self._t1, self._y1

        if self._t0 is None or self._y0 is None:
            raise ValueError("Missing one data point for linear interpolation")

        if t < self._t0:
             logger.warning("Extrapolating {} ms in the past"
                            .format(self._t0 - t))
        elif t > self._t1:
            logger.warning("Extrapolating {} ms into the future"
                           .format(t - self._t1))
        # Linear interpolation
        w = float(t - self._t0) / (self._t1 - self._t0)
        return t, (1. - w) * self._y0 + w * self._y1
