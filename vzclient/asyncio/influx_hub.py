import logging
import asyncio

from ..buffer import Buffer
from .influx_driver import InfluxDriver

logger = logging.getLogger("vzclient")


class InfluxHub(object):
    """Relays information from various sources to an influx DB client

    The influx hub receives information from various readers (sources) and
    buffers them in an internal output queue until one or more writers relay the
    buffered information to an influx DB client.

    Arguments:
        buffer_size (int): Buffer size in bytes
        max_buffer_age (int): Maximum time allowed for messages to remain in
            buffer before they are flushed [ms].
        max_retries (int): Maximum number of write attempts on error. Setting
            this value to -1 will lead to infinite retries.
    """
    def __init__(self, buffer_size=1000000, max_buffer_age=30000, max_retries=5):
        self.max_buffer_age = int(max_buffer_age)
        self.max_retries = int(max_retries)

        self._output_queue = asyncio.Queue()
        self._buffer = Buffer(buffer_size)
        self._t_buffer = None
        self._readers = []
        self._writers = []
        self._future = None

    def __await__(self):
        if self._future is None:
            loop = asyncio.get_event_loop()
            self._future = loop.create_future()
            self._future.set_exception(RuntimeError("Hub was never started"))
        return self._future.__await__()

    async def stop(self, timeout=300):
        """Stop this relay instance

        Stops all readers, flushes remaining data to output queue and writes
        data from the queue to Influx before stopping the writer(s).

        The timeout argument is split between readers and writers. If only one
        value is used, 20 % of the timeout will be allowed for readers and the
        remaining 80% will be used for the writers. To override this behaviour
        a tuple can be specified, in which case the first value of the tuple
        will be used for readers and the second value will be used for the
        writers. The total timeout is thus the sum of these values.

        Arguments:
             timeout (float or tuple): Timeout [s] before cancelling readers and
                 writers.  If a tuple of two values is specified, then the first
                 element is used as reader timeout and the second timeout is
                 used for the writers.
        """
        logger.info("Stopping Influx Hub ...")

        if not isinstance(timeout, (tuple, list)):
            t1 = 0.2 * timeout
            t2 = timeout - t1
            timeout = (t1, t2)

        errors = []
        for t, name in zip(timeout, ("readers", "writers")):
            tasks = getattr(self, f"_{name}")
            for task in tasks:
                task.cancel()
            logger.debug(f"Closing {name} ...")
            done, pending = await asyncio.wait(tasks, timeout=t)
            for task in done:
                try:
                    await task
                except Exception as ex:
                    logger.error(f"{ex}")
            if pending:
                msg = f"Failed to close {len(pending)} {name}"
                logger.warning(msg)
                errors.append(msg)

        if self._future is not None:
            if errors:
                self._future.set_exception(TimeoutError("\n".join(errors)))
            elif not self._future.cancelled():
                self._future.set_result(True)

        return

    def connect_reader(self, reader, **kwargs):
        """Connect a reader delivering input to write to database

        Arguments:
            reader (async generator): Asynchronous generator yielding pairs of
                (timestamp, value) pairs. See
                :class:`~vzclient.asyncio.DeviceReader` for an example.
            **kwargs: Keyword arguments passed verbatim to
                :meth:`DeviceReader.wrap_reader`
        """
        task = asyncio.ensure_future(self.wrap_reader(reader, **kwargs))
        self._readers.append(task)

    def connect_writer(self, host, **kwargs):
        """Start a writer task

        Starts a writer which continuously writes chunks of data from the
        internal output queue to the Influx database. At least one writer is
        required to consume the queue. More writer instances may be useful, if
        the readers produce more input than one writer can consume.

        Arguments:
            host (str): Hostname of influx db
            **kwargs: Keyword arguments passed verbatim to
                :class:`~vzclient.asyncio.InfluxDriver`
        """
        if self._future is None or self._future.done():
            loop = asyncio.get_event_loop()
            self._future = loop.create_future()
        task = asyncio.ensure_future(self.wrap_writer(host, **kwargs))
        self._writers.append(task)

    async def wrap_reader(self,
                          reader,
                          measurement,
                          tags=None,
                          field_name="value"):
        """Reader implementation run for every device reader

        Do not execute this directly. Use :meth:`connect_reader` instead.

        Arguments:
            reader (async generator): Asynchronous generator yielding
                (timestamp, value) pairs. See
                :class:`~vzclient.asyncio.DeviceReader` for an example.
            measurement (str): Measurement name to use by influx DB
            tags (dict): Additional tags to add to influx DB. Defaults to
                ``None``.
            field_name (str): Field name to use for Influx DB entries. Defaults
                to ``'value'``.
        """
        prefix = InfluxDriver.get_prefix(measurement=measurement,
                                         tags=tags,
                                         field_name=field_name)
        name = tags.get("title", "<unknown>")
        logger.info(f"Started {name} reader for measurement {measurement} ...")

        try:
            async for t, x in reader:
                if self._t_buffer is None:
                    self._t_buffer = t
                self._buffer.write(prefix, f"{x} {t}\n".encode())
                logger.debug(f"Buffer: ({len(self._buffer)} / "
                             f"{self._buffer.capacity} bytes) used")

                if self._buffer.is_full():
                    self.flush_buffer()
                elif t - self._t_buffer > self.max_buffer_age:
                    self.flush_buffer()
            logger.error(f"{name} reader died unexpectedly")
        except asyncio.CancelledError:
            self.flush_buffer()
            logger.debug(f"{name} reader cancelled.")

    async def wrap_writer(self, host, bucket='volkszaehler', **kwargs):
        """Task executed for each data base writer

        Do not execute this directly. Use :meth:`start_writer` instead.

        Arguments:
            host (str): Hostname of Influx DB Server to connect to
            bucket (str): Bucket to write to. Defaults to 'volkszaehler'
            **kwargs: Keyword arguments passed verbatim to
                :class:`~vzclient.asyncio.InfluxDriver`
        """
        logger.debug(f"Starting influx writer for bucket {bucket} on {host} ...")
        data = None
        retry = 0
        cancel = False
        while not cancel or not self._output_queue.empty() or data is not None:
            try:
                if data is None:
                    retry = 0
                    logger.debug(f"Waiting for next chunk ...")
                    data = await self._output_queue.get()

                try:
                    async with InfluxDriver(host,
                                            bucket=bucket,
                                            **kwargs) as client:
                        logger.debug(f"Writing {len(data)} bytes of data to "
                                     f"bucket {bucket} ...")
                        await client.insert(data)
                        data = None
                        self._output_queue.task_done()
                except asyncio.CancelledError:
                    raise
                except Exception as ex:
                    # TODO we need finer grained error control here
                    # Maybe no retry on 401 and different behaviour on connection
                    # problems ?
                    if self.max_retries < 0 or retry < self.max_retries:
                        retry += 1
                        logger.error(f"While writing: {ex}. "
                                     f"Starting retry ({retry}) ...")
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"While writing: {ex}. Ignoring {len(data)}"
                                     f" bytes of data")
                        data = None
                        self._output_queue.task_done()
            except asyncio.CancelledError:
                logger.info(f"Writer for bucket {bucket} cancelled. Closing...")
                cancel = True
        logger.debug(f"Writer for bucket {bucket} on {host} closed")

    def flush_buffer(self):
        """Copy buffer content to output queue and clear buffer
        """
        self._t_buffer = None
        if self._buffer:
            logger.debug(f"Flushing {len(self._buffer)} bytes to output queue")
            data = bytes(self._buffer.data())
            self._buffer.clear()
            self._output_queue.put_nowait(data)

