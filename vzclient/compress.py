
class Compressor(object):
    """Compresses const phases within a time series

    Reduces the number of nodes in constant phases of a time series

    Arguments:
         max_gap: Maximum allowed gap in time (or x-coordinate)
         x0: Initial x value.  Defaults to ``None``. For expert users only.
         y0: Initial y value.  Defaults to ``None``. For expert users only.
    """
    def __init__(self, max_gap=None, x0=None, y0=None):
        self.max_gap = max_gap
        self._x0, self._y0 = (x0, y0)
        self._xn, self._yn = self._x0, self._y0

    def __call__(self, iterable):
        """Compress values in an iterable

        Arguments:
            iterable (iterable): Input time series. Iterable of (x, y) values
                to compress.

        Yield:
             tuple: Tuple of compressed time series
        """
        try:
            it = self.iter(iterable)
        except StopIteration:
            return

        yield from self.compress(it)
        yield from self.finalize()
        return

    def iter(self, iterable):
        """Initialize compressor from iterable

        Arguments:
            iterable (iterable): Input time series. Iterable of (x, y) values
                to compress.

        Return:
            Iterator: Iterator to pass to compress
        """
        it = iter(iterable)
        self._x0, self._y0 = next(it)
        self._xn, self._yn = self._x0, self._y0
        return it

    def compress(self, chunk):
        """Compress a chunk of values

        Arguments:
            chunk (iterable): Input time series. Iterable of (x, y) values
                to compress.

        Yield:
            tuple: Elements of the compressed time series
        """
        for x, y in chunk:
            if x == self._xn:
                continue

            if y == self._yn:
                if self.max_gap is not None and x - self._x0 > self.max_gap:
                    yield self._x0, self._y0
                    self._x0, self._y0 = self._xn, self._yn
                self._xn = x
                continue

            yield self._x0, self._y0
            if self._xn != self._x0:
                yield self._xn, self._yn

            self._x0, self._y0 = x, y
            self._xn, self._yn = self._x0, self._y0

    def finalize(self):
        """Finalize compression

        Call this in iterative mode after after time series has been read

        Yields:
            tuple: Remaining elements of the time series
        """
        yield self._x0, self._y0
        if self._xn != self._x0:
            yield self._xn, self._yn


def compress_const(iterable, max_gap=None):
    """Compresses a time series by reducing number of nodes for const periods

    Eliminates nodes where the measurement does not change. If ``max_gap`` is
    specified, nodes are not deleted, if they
    
    Arguments:
        max_gap (int): Maximum distance between neighbouring nodes.

    Yield:
        int: Compressed items of iterable with constant nodes removed
    """
    compressor = Compressor(max_gap=max_gap)
    yield from compressor(iterable)
