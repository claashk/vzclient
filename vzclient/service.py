import signal


class Service(object):
    """Context manager to catch signals
    Allows to easily create services compatible with systemd

    Use as
      with Service() as service:
          while service.run:
              print("I am working")
          print("And now I am finished")
    """
    def __init__(self):
        self.run = True
        self.signum = None
        self.frame = None

        self._sigint = None
        self._sigterm = None

    def __enter__(self):
        self.enable()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()

    def enable(self):
        self._sigint = signal.signal(signal.SIGINT, self.handle_signal)
        self._sigterm = signal.signal(signal.SIGTERM, self.handle_signal)

    def disable(self):
        if self._sigint is not None:
            signal.signal(signal.SIGINT, self._sigint)
            self._sigint = None

        if self._sigterm is not None:
            signal.signal(signal.SIGINT, self._sigterm)
            self._sigterm = None

    def handle_signal(self, signum, frame):
        self.signum = signum
        self.frame = frame
        self.run = False
