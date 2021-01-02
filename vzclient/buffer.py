import logging

logger = logging.getLogger("vzclient")


class Buffer(object):
    """Simple binary buffer implementation


    Arguments:
        capacity (int): Maximum number of bytes in buffer
        high_water_mark (int): High water mark used to check if buffer is full
    """
    def __init__(self, capacity, high_water_mark=None):
        self._storage = bytearray(capacity)
        self._bytes_in_buffer = 0
        self._high_water_mark = 0

        if high_water_mark is None:
            self._high_water_mark = int(0.9 * self.capacity)
        else:
            self.high_water_mark = high_water_mark

    @property
    def capacity(self):
        """Get capacity of the buffer

        Return:
            int: Maximum number of bytes this buffer can store
        """
        return len(self._storage)

    @property
    def high_water_mark(self):
        """Get the current high water mark of the buffer

        Return:
            int: Number of bytes beyond which the buffer is assumed to be full
            and a flush should occur
        """
        return self._high_water_mark

    @high_water_mark.setter
    def high_water_mark(self, n):
        m = int(n)
        if m > self.capacity:
            raise ValueError(f"High water mark ({m}) exceeds capacity")
        self._high_water_mark = m

    def __len__(self):
        """Get number of bytes in buffer"""
        return self._bytes_in_buffer

    def data(self):
        """Get a view of the data

        Return:
            view: View of the data
        """
        return self._storage[:self._bytes_in_buffer]

    def is_full(self):
        """Check if buffer content is below water mark

        Return:
            bool: True if and only if buffer is filled up to or beyond the high
            water mark.
        """
        return self._bytes_in_buffer >= self._high_water_mark

    def write(self, *args):
        """Write data to buffer

        Arguments:
            args: Bytes objects to write to the buffer

        Return:
            int: Total number of bytes written to buffer
        """
        offset = self._bytes_in_buffer
        for x in args:
            begin = self._bytes_in_buffer
            end = begin + len(x)
            if end > self.capacity:
                raise BufferError("Overflow")
            self._storage[begin:end] = x
            self._bytes_in_buffer = end
        return self._bytes_in_buffer - offset

