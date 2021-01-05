#!/usr/bin/env python3
import logging
import uuid
from .device_reader import DeviceReader
from .compress import compress_const
from ..constants import now, CHANNEL_TYPES


SENSOR_TYPES = {s['name'] for s in CHANNEL_TYPES}
logger = logging.getLogger("vzclient")


class ModbusLogger:
    """Log data from modbus clients to influx DB

    Arguments:
        client_type (type): Client type to use for the reader. A type like
            :class:`modbusclient.asyncio.Client` or any derived type.
        hub (:class:`vzclient.asyncio.InfluxHub`): Hub relaying information from
            readers to the database
        host (str): Modbus host name
        device_id (str): Unique device ID used to create UUID. Defaults to
            `host`. The UUID will be created from the string
            "<modbus address>.<device_id>" via uuid3 and NAMESPACE_DNS.
        sampling_interval (float): Sampling interval of the device [s]. Defaults
            to 30 seconds.
        interpolate (bool): If ``True`` interpolate to timestamps which are
            integer multiples of `sampling_interval`.
        max_gap (float): Omit repetition of identical consecutive values unless
            the resulting time gap is larger than `max_gap` seconds. If ``None``
            no compression is applied. Defaults to ``None``.
        precision (int or str): Either an integer defining the number of
           digits to use or ``'auto'`` to use the default precision
           defined by `default_precision` for the sensor_type. Defaults to
               ``'auto'``.
        default_precision (dict): Dictionary containing a default precision per
            message address.
        measurement (str): Measurement identification to use for Influx DB.
            Defaults to ``'volkszaehler'``.
           tags (dict): Tags to use for Influx Entries. Besides any key value
               pair, the following are set unless specified otherwise:

            - ``'title'``: Will be set to `name` attribute of message as
                defined by `api`.
            - ``'type'``: Will be set to `sensor_type` attribute of message
                as defined by `api`.
            - ``'unit'``: Will be set to `units` attribute of message as
                defined by `api`.
            - ``'uuid'``: Will be set to ``'auto'``, causing the UUID to be
                created from message address and `device_id`
        api (dict): Modbus API. Defaults to MESSAGE_BY_ADDRESS.
    """
    def __init__(self,
                 client_type,
                 hub,
                 host,
                 device_id=None,
                 sampling_interval=30.,
                 interpolate=True,
                 max_gap=None,
                 precision="auto",
                 default_precision=None,
                 measurement="volkszaehler",
                 tags=None,
                 api=None):
        if default_precision is None:
            default_precision = dict()
        self.client_type = client_type
        self.hub = hub
        self.host = host
        self.device_id = device_id if device_id is not None else str(host)
        self.sampling_interval = float(sampling_interval)
        self.interpolate = interpolate
        self.max_gap = max_gap
        self.precision = precision
        d = default_precision if default_precision is not None else dict()
        self.default_precision = d
        self.measurement = measurement
        self.api = api
        self.tags = tags if tags is not None else dict()

    def connect_writer(self, **kwargs):
        """Connect InfluxDB writer to hub

        Spawns are writer task and connects it to the hub.

        Arguments
            **kwargs: Keyword arguments passed verbatim to
                ``self.hub.connect_writer``.
        """
        self.hub.connect_writer(**kwargs)

    def __call__(self, message, message_id=None, **kwargs):
        """Connect reader for a modbus message

        Arguments:
            message (int): Modbus message address
            message_id (int or str): Message ID used to form uuid. Defaults to
                `message`.
            **kwargs: Additional keyword arguments overriding any of the default
                values passed upon initialization of this class.
        """
        host = str(kwargs.pop('host', self.host))
        tags = kwargs.pop('tags', self.tags.copy())
        payload = self.api[message]
        name = tags.setdefault('title', payload.name)
        sensor_type = tags.setdefault('type', payload.sensor_type)
        if sensor_type not in SENSOR_TYPES:
            logger.warning(f"In message {name}: unknown sensor type "
                           f"'{sensor_type}'")
        tags.setdefault('unit', payload.units)

        # Set uuid if required
        uid = tags.pop("uuid", "auto")
        if uid == "auto":
            device_id = kwargs.pop('device_id', self.device_id)
            if message_id is None:
                message_id = str(message)
            uid = uuid.uuid3(uuid.NAMESPACE_DNS, f"{message_id}.{device_id}")
        if uid is not None:
            tags['uuid'] = uid

        precision = kwargs.pop('precision', self.precision)
        if precision == "auto":
            precision = self.default_precision.get(sensor_type, None)

        sampling_interval = kwargs.pop('sampling_interval',
                                       self.sampling_interval)
        interpolate = kwargs.pop('interpolate', self.interpolate)
        measurement = str(kwargs.pop('measurement', self.measurement))
        max_gap = kwargs.pop('max_gap', self.max_gap)

        for k in kwargs.keys():
            logger.warning(f"While connecting to {name} on modbus host {host}: "
                           f"Ignoring unrecognized option {k}.")

        gen = DeviceReader(self.modbus_read,
                           sampling_interval=int(1000 * sampling_interval),
                           interpolate=interpolate,
                           name=name,
                           host=self.host,
                           message=message,
                           precision=precision)

        if max_gap is not None:
            gen = compress_const(gen, int(1000 * max_gap))  # max_gap[s] -> ms

        logger.info(f"Connecting modbus reader {name} ({message}) at {host} ...")
        self.hub.connect_reader(gen, measurement=measurement, tags=tags)

    async def modbus_read(self, host, message=None, precision=None, **kwargs):
        """Wrapper function reading a modbus register

            This function can be passed as `reader` argument to
            :class:`vzclient.asyncio.DeviceReader`.

            Arguments:
                host (str): Host address
                message (int): Modbus message address
                precision (int): Number of digits to round to. If ``None``, no
                    rounding takes place. Defaults to ``None``.
            **kwargs: Keyword arguments passed to the client.

            Return:
                tuple: timestamp (local UTC [ms since EPOCH]) and value read from
                host.
        """
        logger.info(f"Connecting to {host} ...")
        name = self.api[message].name
        async with self.client_type(host=host, api=self.api, **kwargs) as client:
            if not client.is_connected():
                logger.error(f"Modbus connection to {host} failed")
            value = await client.get(message)
            t = now()
            if precision is not None:
                value = round(value, precision)
        logger.debug(f"Got {name} ({message}) of {value} from {host}")
        return t, value
