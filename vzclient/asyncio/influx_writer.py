import logging
import asyncio

from ..buffer import Buffer
from .influx_driver import InfluxDriver

logger = logging.getLogger(__name__)


class InfluxWriter(object):
    """Buffer input from a reader and transmit it in chunks to a consumer

    Arguments:
        buffer_size (int): Buffer size in bytes
        max_buffer_age (int): Maximum time allowed for messages to remain in
            buffer before they are flushed [ms].
    """
    def __init__(self, buffer_size=1000000, max_buffer_age=30000):
        self._output_queue = asyncio.Queue()
        self._buffer = Buffer(buffer_size)
        self.max_buffer_age = int(max_buffer_age)
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
            logger.debug(f"Killing {len(pending)} pending tasks ...")
            cancel_all = [asyncio.ensure_future(t.cancel()) for t in pending]
            done, pending = await asyncio.wait(cancel_all)
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

    def start_writer(self, host, **kwargs):
        """Start a writer task

        Starts a writer which continuously writes chunks of data from the
        internal output queue to the Influx database. At least one writer should
        be started.

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
                logger.error(f"While writing: {ex}. Retrying ...")
                await asyncio.sleep(2)
                continue

            data = None
            self._output_queue.task_done()
        logger.debug("Writer for bucket {bucket} closed")

    def flush_buffer(self):
        """Copy buffer content to output queue and clear buffer
        """
        self._t_buffer = None
        if self._buffer:
            logger.debug(f"Flushing {len(self._buffer)} bytes to output queue")
            data = bytes(self._buffer.data())
            self._buffer.clear()
            self._output_queue.put_nowait(data)
