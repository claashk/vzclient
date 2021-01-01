import logging
from aioinflux import InfluxDB2Client
from aioinflux.serialization.common import escape, tag_escape
from aioinflux.serialization.common import measurement_escape, key_escape

logger = logging.getLogger("vzclient")


class InfluxDriver(object):
    """Asynchronous InfluxDB client for the Volkszaehler database

    Can be used in an asynchronous with block.

    Arguments:
        host (str): Hostname of mysql server
        user (str): MySQL user name
        token (str): MySQL password for ``user``
        db (str): Database identifier
        **kwargs: Keyword arguments passed verbatim to mysql.connect
    """
    def __init__(self,
                 host,
                 secret=None,
                 org="volkszaehler",
                 bucket="volkszaehler",
                 ssl=True,
                 **kwargs):
        if secret is None:
            secret = kwargs.pop("token", None)
        self._client_cfg = dict(host=host,
                                bucket=bucket,
                                org=org,
                                token=secret,
                                ssl=ssl,
                                **kwargs)
        self._connection = None
        self._prefix = b""
        self._buffer = None
        self._high_water_mark = 0
        self._bytes_in_buffer = 0

    async def __aenter__(self):
        if self.is_connected:
            raise RuntimeError("Client already in use")
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.is_connected:
            try:
                await self.flush_buffer()
            except Exception as ex:
                logger.error(f"While flushing buffer: {ex}")
            await self.disconnect()

    @property
    def is_connected(self):
        """Check if client is connected

        Return:
            bool: True if and only if client is connected
        """
        return self._connection is not None

    async def connect(self, **kwargs):
        """Connect to a database

        Arguments:
            **kwargs: Keyword arguments used for the connection. These
                arguments will override any arguments passed to init.
        """
        self._client_cfg.update(kwargs)
        await self.disconnect()
        self._connection = InfluxDB2Client(**self._client_cfg)
        await self._connection.create_session()

    async def disconnect(self):
        """Disconnect client"""
        if self.is_connected:
            await self._connection.close()
            self._connection = None

    async def assert_connected(self):
        """Connect to the database if the client is not already connected"""
        if not self.is_connected:
            await self.connect()

    async def query(self, query, **kwargs):
        """Query the database

        Arguments:
            query (str): Query string
            **kwargs : Keyword arguments passed verbatim to InfluxDB
        """
        await self.assert_connected()
        result = await self._connection.query(query, **kwargs)
        return result

    async def insert(self, data, precision="ms", **kwargs):
        await self.assert_connected()
        r = await self._connection.write(data, precision=precision, **kwargs)
        return r

    def get_reader(self):
        client = self.get_client()
        client.init_reader()
        return client

    def get_writer(self,
                   measurement="volkszaehler",
                   tags=None,
                   field_name="value",
                   buffer_size=8192*1024):
        client = self.get_client()
        client.init_writer(measurement=measurement,
                           tags=tags,
                           field_name=field_name,
                           buffer_size=buffer_size)
        return client

    def get_client(self):
        if self.is_connected:
            return InfluxDriver(**self._client_cfg)
        return self

    def init_writer(self, measurement, tags, field_name, buffer_size):
        self._prefix = self.get_prefix(measurement=measurement,
                                       tags=tags,
                                       field_name=field_name)
        self._buffer = bytearray(buffer_size)
        max_line_len = len(self._prefix) + 2 * 32  # assume 32 char max per field
        self._high_water_mark = buffer_size - max_line_len
        self._bytes_in_buffer = 0

    def init_reader(self):
        raise NotImplementedError("Reader")

    async def write_chunk(self, chunk):
        for t, x in chunk:
            first = self._bytes_in_buffer
            last = first + len(self._prefix)
            self._buffer[first:last] = self._prefix
            self._bytes_in_buffer = last
            first = last
            rest = f"{x} {t}\n".encode()
            last = first + len(rest)
            self._buffer[first:last] = rest
            self._bytes_in_buffer = last
            if self._bytes_in_buffer > self._high_water_mark:
                await self.flush_buffer()

    async def iter_chunks(self,
                          channel,
                          begin=None,
                          end=None,
                          max_gap=None,
                          chunk_size=8192):
        raise NotImplementedError("read chunks for Influx driver")

    async def flush_buffer(self):
        if self._bytes_in_buffer > 0:
            logger.debug(f"Flushing {self._bytes_in_buffer} bytes to influx ...")
            data = self._buffer[:self._bytes_in_buffer]
            await self.insert(data=data)
            self._bytes_in_buffer = 0

    @staticmethod
    def get_prefix(measurement, tags=None, field_name="value"):
        # https: // github.com / influxdata / influxdb / issues / 3069
        if tags is None:
            tags = dict()
        tag_str = ",".join("=".join((escape(key, tag_escape),
                                     escape(tags[key], tag_escape)))
                           for key in sorted(tags.keys()))
        meas = escape(measurement, measurement_escape)
        field = escape(field_name, key_escape)
        return f"{meas},{tag_str} {field}=".encode("utf-8")
