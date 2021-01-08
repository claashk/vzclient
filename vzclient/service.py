import signal
import asyncio
from inspect import iscoroutinefunction


class Service(object):
    """Context manager to catch signals
    Allows to easily create services compatible with systemd

    Use as
      with Service() as service:
          while service.run:
              print("I am working")
          print("And now I am finished")
    """
    def __init__(self, signals=None, callback=None, **kwargs):
        self.cancel = False
        self.signum = None
        self.frame = None

        self._saved_signals = dict()
        self._sigterm = None
        if signals is None:
            self._signals = [signal.SIGINT, signal.SIGTERM]
        else:
            self._signals = signals

        self._callback = callback
        self._args = kwargs
        self._callback_result = None

    @property
    def callback_result(self):
        return self._callback_result

    def __enter__(self):
        self.enable()
        return self

    async def __aenter__(self):
        self.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.__exit__(exc_type, exc_val, exc_tb)
        if self._callback_result is not None:
            self._callback_result = await self._callback_result

    def enable(self):
        self.disable()
        for s in self._signals:
            self._saved_signals[s] = signal.signal(s, self.handle_signal)

    def disable(self):
        while self._saved_signals:
            s, f = self._saved_signals.popitem()
            signal.signal(s, f)

    def handle_signal(self, signum, frame):
        self.signum = signum
        self.frame = frame
        self.cancel = True

        if self._callback is not None:
            if iscoroutinefunction(self._callback):
                self._callback_result = asyncio.ensure_future(
                                            self._callback(**self._args))
            else:
                self._callback_result = self._callback(**self._args)



