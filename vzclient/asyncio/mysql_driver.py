import aiomysql as sql
from collections import namedtuple
from datetime import datetime, timedelta
import logging

Entity = namedtuple("Entity", ("id", "uuid", "type", "cls"))
Entity.__doc__ = """Namedtuple for rows of the 'entities' table

Arguments:
    id (int): MySQL Row number
    uuid (str): UUID
    type (str): Type of the channel or entity
    cls (str): Class column.
"""

logger = logging.getLogger(__name__)

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


class MySqlDriver(object):
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
    def __init__(self,
                 host,
                 user,
                 secret=None,
                 database=None,
                 charset="utf8",
                 **kwargs):
        if secret is None:
            secret = kwargs.pop("password", None)
        if database is None:
            database = kwargs.pop("db", "volkszaehler")
        self._client_cfg = dict(host=host,
                                user=user,
                                password=secret,
                                db=database,
                                charset=charset,
                                **kwargs)
        self._connection = None
        self._cursor = None

    async def __aenter__(self):
        if self.is_connected:
            raise RuntimeError("Client already in use")
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
        self._client_cfg.update(kwargs)
        await self.disconnect()
        self._connection = await sql.connect(**self._client_cfg)
        self._cursor = await self._connection.cursor()

    async def disconnect(self):
        """Disconnect client"""
        if self.is_connected:
            if self._cursor is not None and not self._cursor.closed:
                await self._cursor.close()
            await self._connection.ensure_closed()
        self._connection = None
        self._cursor = None

    async def assert_connected(self):
        """Connect to the database if the client is not already connected"""
        if not self.is_connected:
            await self.connect()

    def get_reader(self):
        return self.get_client()

    def get_writer(self):
        client = self.get_client()
        client.init_writer()
        return client

    def get_client(self):
        if self.is_connected:
            return MySqlDriver(**self._client_cfg)
        return self

    def init_writer(self):
        raise NotImplementedError("Writer")

    async def iter_chunks(self,
                          channel,
                          begin=None,
                          end=None,
                          chunk_size=8192):
        if not self.is_connected:
            raise RuntimeError("Reader not initialized")
        channel_id = channel['id']
        async for chunk in self.iter_measurements(channel_id,
                                                  begin=begin,
                                                  end=end,
                                                  limit=chunk_size):
            yield chunk
        return

    async def write_chunk(self, **kwargs):
        raise NotImplementedError("write_chunks for mysql driver")

    async def get_channels(self):
        if not self.is_connected:
            raise RuntimeError("Reader not initialized")
        entities = await self.entities()
        properties = []
        for entity in entities:
            p = await self.channel_properties(entity.id)
            p.update(uuid=entity.uuid,
                     cls=entity.cls,
                     type=entity.type,
                     id=entity.id)
            properties.append(p)
        return properties

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
        query = [f"SELECT * FROM {table}"]
        if where is not None:
            query.append(f"WHERE {where}")

        if order:
            query.append(f"ORDER BY {order}")

        if limit is not None:
            query.append(f"LIMIT {limit:d}")

        if offset is not None:
            query.append(f"OFFSET {offset:d}")
        query = " ".join(query)
        logger.debug(f"Posting query: {query}")
        res = await self.query(query)
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

    async def channel_properties(self, entity_id):
        """Get information about a channel's properties

        Returns:
            dict: Dictionary with property title as key and associated property
            as key.
        """
        i = int(entity_id)
        properties = await self.properties(where=f"entity_id = {i}")
        return {p.key: p.value for p in properties}

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
        if end is not None:
            t1 = timestamp(end) if isinstance(end, datetime) else int(end)
            end_query = f" AND timestamp < {t1}"
        else:
            end_query = ""
        where_const = f"channel_id = {channel}{end_query}"

        # Do not use offset, as it is incredibly slow:
        # https://www.eversql.com/faster-pagination-in-mysql-why-order-by-with-limit-and-offset-is-slow/
        # channel_id, timestamp combination should be unique (forms an index)

        if begin is not None:
            t0 = timestamp(begin) if isinstance(begin, datetime) else int(end)
            where = f"{where_const} AND timestamp >= {t0}"
        else:
            where = where_const

        while True:
            chunk = await self.data(where=where,
                                    limit=limit,
                                    order="timestamp ASC")
            logger.debug("Got chunk of size {}".format(len(chunk)))
            if not chunk:
                return

            yield [(m.timestamp, m.value) for m in chunk]
            where = f"{where_const} AND timestamp > {chunk[-1].timestamp}"
