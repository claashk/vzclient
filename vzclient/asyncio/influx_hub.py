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
        self._stop = False
        self._tasks = []

    async def stop(self, timeout=300):
        """Stop this relay instance

        Stops all readers, flushes remaining data to output queue and writes
        data from the queue to Influx before stopping the writer.

        Arguments:
             timeout (int): Timeout [s] before cancelling readers and writers.
        """
        logger.debug("Stopping Influx writer ...")
        self._stop = True
        done, pending = await asyncio.wait(self._tasks, timeout=timeout)
        if pending:
            logger.debug(f"Cancelling {len(pending)} pending tasks ...")
            for t in pending:
                t.cancel()
            done, pending = await asyncio.wait(pending)
            logger.warning(f"Unable to cancel {len(pending)} tasks")
        return

    def connect_reader(self, reader, **kwargs):
        """Connect a reader delivering input to write to database

        Arguments:
            reader (async generator): Asynchronous generator yielding pairs of
                (timestamp, value) pairs. See
                :class:`~vzclient.asyncio.DeviceReader` for an example.
            measurement (str): Measurement name to use by influx DB
            tags (dict): Additional tags to add to influx DB. Defaults to
                ``None``.
            field_name (str): Field name to use for Influx DB entries. Defaults
                to ``'value'``.
        """
        task = asyncio.ensure_future(self._read(reader, **kwargs))
        self._tasks.append(task)

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
        task = asyncio.ensure_future(self._write(host, **kwargs))
        self._tasks.append(task)

    async def _read(self, reader, measurement, tags=None, field_name="value"):
        """Reader implementation run for every device reader

        Do not execute this directly. Use :meth:`connect_reader` instead.

        Arguments:
            reader (async generator): Asynchronous generator yielding pairs of
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
        logger.debug(f"Started reader for measurement {measurement} with tags "
                     f"{tags} ...")
        async for t, x in reader:
            if self._stop:
                logger.debug(f"Reader ({tags}) closing ...")
                self.flush_buffer()
                return

            if self._t_buffer is None:
                self._t_buffer = t
            self._buffer.write(prefix, f"{x} {t}\n".encode())
            logger.debug(f"Buffer: ({len(self._buffer)} / "
                         f"{self._buffer.capacity} bytes) used")

            if self._buffer.is_full():
                self.flush_buffer()
            elif t - self._t_buffer > self.max_buffer_age:
                self.flush_buffer()

    async def _write(self, host, bucket='volkszaehler', **kwargs):
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
        while not self._stop or not self._output_queue.empty():
            if data is None:
                logger.debug(f"Waiting for next chunk ...")
                data = await self._output_queue.get()

            try:
                async with InfluxDriver(host, bucket=bucket, **kwargs) as client:
                    logger.debug(f"Writing {len(data)} bytes of data to bucket "
                                 f"{bucket} ...")
                    await client.insert(data)
            except Exception as ex:
                # TODO we need finer grained error control here
                # Maybe no retry on 401 and different behaviour on connection
                # problems ?
                if self.max_retries < 0 or retry < self.max_retries:
                    retry += 1
                    logger.error(f"While writing: {ex}. Will retry ({retry}) ...")
                    await asyncio.sleep(2)
                    continue
                else:
                    logger.error(f"While writing: {ex}. Max retries exceeded."
                                 f"Ignoring {len(data)} bytes of data")
            data = None
            retry = 0
            self._output_queue.task_done()
        logger.debug(f"Writer for bucket {bucket} closed")

    def flush_buffer(self):
        """Copy buffer content to output queue and clear buffer
        """
        self._t_buffer = None
        if self._buffer:
            logger.debug(f"Flushing {len(self._buffer)} bytes to output queue")
            data = bytes(self._buffer.data())
            self._buffer.clear()
            self._output_queue.put_nowait(data)

