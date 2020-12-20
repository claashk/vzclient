import aiomysql as sql
from collections import namedtuple
from datetime import datetime, timedelta
from ..compress import Compressor

Entity = namedtuple("Entity", ("id", "uuid", "type", "cls"))
Entity.__doc__ = """Namedtuple for rows of the 'entities' table

Arguments:
    id (int): MySQL Row number
    uuid (str): UUID
    type (str): Type of the channel or entity
    cls (str): Class column.
"""

Measurement = namedtuple("Measurement", ("id", "channel_id", "timestamp", "value"))
Property = namedtuple("Property", ("id", "entity_id", "key", "value"))

EPOCH = datetime(year=1970, month=1, day=1)


def time(t):
    """Convert timestamp to datetime object

    Arguments:
        t (int): Timestamp [ms since EPOCH]

    Return:
        datetime.datetime: Datetime object representing the same time as `t`
    """
    return EPOCH + timedelta(milliseconds=t)


def timestamp(t):
    """Convert datetime object to timestamp

    Arguments:
        t (datetime.datetime): Datetime object

    Return:
        int: Timestamp [ms since EPOCH]
    """
    return int(1000. * (t - EPOCH).total_seconds() + 0.5)


class MySqlClient(object):
    """Asynchronous MySQL client for the Volkszaehler database

    Can be used in an asynchronous with block.

    Arguments:
        host (str): Hostname of mysql server
        user (str): MySQL user name
        password (str): MySQL password for ``user``
        db (str): Database identifier
        charset (str): Character set to use. Defaults to "utf-8"
        **kwargs (dict): Keyword arguments passed verbatim to mysql.connect
    """
    def __init__(self, host, user, password, db, charset="utf8", **kwargs):
        self._parms=dict()
        self._parms['host'] = host
        self._parms['user'] = user
        self._parms['password'] = password
        self._parms['db'] = db
        self._parms['charset'] = charset
        self._parms.update(kwargs)
        self._connection = None
        self._cursor = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    @property
    def is_connected(self):
        """Check if client is connected

        Return:
            bool: True if and only if client is connected
        """
        return self._connection is not None and not self._connection.closed

    async def connect(self, **kwargs):
        """Connect to a database

        Arguments:
            **kwargs (dict): Keyword arguments used for the connection. These
                arguments will override any arguments passed to init.
        """
        self._parms.update(kwargs)
        await self.disconnect()
        self._connection = await sql.connect(**self._parms)
        self._cursor = await self._connection.cursor()

    async def disconnect(self):
        """Disconnect client"""
        if self.is_connected:
            if self._cursor is not None and not self._cursor.closed:
                await self._cursor.close()
            await self._connection.ensure_closed()

    async def assert_connected(self):
        """Connect to the database if the client is not already connected"""
        if not self.is_connected:
            await self.connect()

    async def query(self, query, *args):
        """Query the database

        Arguments:
            query (str): Query string
            *args : Arguments used to fill the query
        """
        await self.assert_connected()
        nrows = await self._cursor.execute(query, args)
        result = await self._cursor.fetchall()
        return result

    async def select(self, table, where=None, limit=None, offset=None, order=None):
        """Select items from a specific table

        This is a convenience method to query information from a table.

        Arguments:
            table (str): Table name
            where (str): WHERE clause. If ``None``, no WHERE is inserted into the
                query. Defaults to ``None``.
            limit (int): LIMIT clause. If ``None``, no LIMIT is used in query.
                Defaults to ``None``.
            offset (int): OFFSET clause. If ``None``, no OFFSET is used in query.
                Defaults to ``None``.
            order (str): Column name used in combination with ORDER statement.
                If ``None``, no ORDER statement is used. Defaults to ``None``.

        Return:
            list: Query result
        """
        query = ["SELECT * FROM {}".format(table)]
        if where is not None:
            query.append("WHERE {}".format(where))

        if order:
            query.append("ORDER BY {}".format(order))

        if limit is not None:
            query.append("LIMIT {:d}".format(limit))

        if offset is not None:
            query.append("OFFSET {:d}".format(offset))

        res = await self.query(" ".join(query))
        return res

    async def entities(self, **kwargs):
        """Query entities table

        This is a convenience method to query information from the entities
        table.

        Arguments:
            **kwargs: Keyword arguments passed verbatim to :meth:`select`

        Return:
            list: Query result containing :class:`Entity` objects
        """
        lst = await self.select("entities", **kwargs)
        return [Entity(*e) for e in lst]

    async def properties(self, **kwargs):
        """Query properties table

        This is a convenience method to query the properties table.

        Arguments:
            **kwargs: Keyword arguments passed verbatim to :meth:`select`

        Return:
            list: Query result containing :class:`Property` objects
        """
        lst = await self.select("properties", **kwargs)
        return [Property(*e) for e in lst]

    async def data(self, **kwargs):
        """Query data table

        This is a convenience method to query the data table.

        Arguments:
            **kwargs: Keyword arguments passed verbatim to :meth:`select`

        Return:
            list: Query result containing :class:`Measurement` objects
        """
        lst = await self.select("data", **kwargs)
        return [Measurement(*e) for e in lst]

    async def channel_info(self):
        """Get information about all channels

        Returns:
            dict: Dictionary with channel title as key and channel information
            as :class:`Entity` instance.
        """
        channels = {}
        entities = await self.entities(where="class = 'channel'")
        for e in entities:
            where = "entity_id = {} and pkey = 'title'".format(e.id)
            properties = await self.properties(where=where)
            assert len(properties) == 1
            channels[properties[0].value] = e
        return channels

    async def iter_measurements(self, channel, begin=None, end=None, limit=512):
        """Iterate over measurements from a specific channel in chunks

        Arguments:
            channel (int): ID of channel (as defined in entities table)
            begin (int): Timestamp [ms since EPOCH]. If not ``None`` only
               measurements with timestamp greater or equal this value are
               returned. Defaults to ``None``.
            end (int):   Timestamp [ms since EPOCH]. If not ``None`` only
               measurements with timestamp smaller than this value are
               returned. Defalts to ``None``.
            limit (int): Chunk size. Determines the maximum number of
                measurements returned per query. Defaults to 512.

        Yields:
            list: One tuple containing timestamp and associated value per
            matching row.
        """
        where = ["channel_id = {}".format(channel)]
        if begin is not None:
            where.append("AND timestamp >= {}".format(timestamp(begin)))
        if end is not None:
            where.append("AND timestamp < {}".format(timestamp(end)))

        where = " ".join(where)
        offset = 0
        while True:
            chunk = await self.data(where=where,
                                    limit=limit,
                                    offset=offset,
                                    order="timestamp ASC")
            n = len(chunk)
            if n == 0:
                return
            offset += n
            yield [(m.timestamp, m.value) for m in chunk]

    async def compress_measurements(self,
                                    channel,
                                    begin=None,
                                    end=None,
                                    limit=1024,
                                    max_gap=None):
        """Compress measurements of a channel

        Iterates over chunks of compressed measurements from a channel.

        Arguments:
            channel (int): ID of channel (as defined in entities table)
            begin (int): Timestamp [ms since EPOCH]. If not ``None`` only
               measurements with timestamp greater or equal this value are
               returned. Defaults to ``None``.
            end (int):   Timestamp [ms since EPOCH]. If not ``None`` only
               measurements with timestamp smaller than this value are
               returned. Defalts to ``None``.
            limit (int): Chunk size. Determines the maximum number of
                measurements returned per query. Defaults to 512.
            max_gap (int): Maximum gap allowed between nodes in compressed
               stream [ms]

        Yields:
            list: One tuple containing timestamp and associated value per
            matching row in compressed time series.
        """
        compressor = Compressor(max_gap=max_gap)
        ait = self.iter_measurements(channel, begin=begin, end=end, limit=limit)
        try:
            chunk = await ait.__anext__()
        except StopAsyncIteration:
            return
        yield list(compressor.compress(compressor.iter(chunk)))
        async for chunk in ait:
            yield list(compressor.compress(chunk))
        yield list(compressor.finalize())