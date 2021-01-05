#!/usr/bin/env python3
import logging
import uuid
from .device_reader import DeviceReader
from .compress import compress_const
from ..constants import now, CHANNEL_TYPES


SENSOR_TYPES = {s['name'] for s in CHANNEL_TYPES}
logger = logging.getLogger("vzclient")


async def modbus_read(client,
                      host,
                      api,
                      message=None,
                      precision=None,
                      **kwargs):
    """Wrapper function reading a modbus register

    This function can be passed as `reader` argument to
    :class:`vzclient.asyncio.DeviceReader`.

    Arguments:
        client (type): Client type to use for the reader. A type like
            :class:`modbusclient.asyncio.Client` or any derived type.
        host (str): Host address
        api (dict): Modbus API.
        message (int): Modbus message address
        precision (int): Number of digits to round to. If ``None``, no
            rounding takes place. Defaults to ``None``.
        **kwargs: Keyword arguments passed to the client.

    Return:
        tuple: timestamp (local UTC [ms since EPOCH]) and value read from
        host.
    """
    logger.info(f"Connecting to {host} ...")
    name = api[message].name
    async with client(host=host, api=api, **kwargs) as cli:
        if not cli.is_connected():
            logger.error(f"Modbus connection to {host} failed")
        value = await cli.get(message)
        t = now()
        if precision is not None:
            value = round(value, precision)
    logger.debug(f"Got {name} ({message}) of {value} from {host}")
    return t, value


def log_modbus(hub,
               client,
               host,
               api,
               message,
               message_id=None,
               device_id=None,
               sampling_interval=30.,
               interpolate=True,
               max_gap=None,
               precision="auto",
               measurement="volkszaehler",
               tags=None,
               field_name="value"):
    """Connect logger for modbus messages to a hub.

    Arguments:
        hub (:class:`vzclient.asyncio.InfluxHub`): Hub to send log messages to.
        client (type): Client type to use for the reader. A type like
            :class:`modbusclient.asyncio.Client` or any derived type.
        host (str): Host address
        api (dict): Modbus API.
        message (int): Modbus message address
        message_id (int or str): Message ID used to form uuid. Defaults to
            `message`.
        device_id (str): Unique device ID used to create UUID. Defaults to
            `host`. The UUID will be created from the string
            "<modbus address>.<device_id>" via uuid3 and NAMESPACE_DNS.
        sampling_interval (float): Sampling interval of the device [s].
            Defaults to 30 seconds.
        interpolate (bool): If ``True`` interpolate to timestamps which are
            integer multiples of `sampling_interval`.
        max_gap (float): Omit repetition of identical consecutive values
            unless the resulting time gap is larger than `max_gap` seconds.
            If ``None``, no compression is applied. Defaults to ``None``.
        precision (int): Number of decimal digits to round value to or
            ``None`` to disable rounding. Defaults to ``None``.
        measurement (str): Measurement identification to use for Influx DB.
            Defaults to ``'volkszaehler'``.
        tags (dict): Tags to use for Influx Entries. Besides any key value pair,
           the following are set unless specified otherwise:

            - ``'title'``: Will be set to `name` attribute of message as
               defined by `api`.
            - ``'type'``: Will be set to `sensor_type` attribute of message
              as defined by `api`.
            - ``'unit'``: Will be set to `units` attribute of message as
                defined by `api`.
            - ``'uuid'``: Will be set to ``'auto'``, causing the UUID to be
                created from message address and `device_id`
        field_name (str): Field name to use in InfluxDB. Defaults to
            ``'value'``
    """
    tags = tags if tags is not None else dict()
    payload = api[message]
    name = tags.pop('title', 'auto')
    if name == 'auto':
        name = payload.name
    if name:
        tags['title'] = name

    sensor_type = tags.pop('type', 'auto')
    if sensor_type == 'auto':
        sensor_type = payload.sensor_type
    if sensor_type:
        if sensor_type not in SENSOR_TYPES:
            logger.warning(f"In message {name}: unknown sensor type "
                           f"'{sensor_type}'")
            tags['type'] = sensor_type
    unit = tags.pop('unit', 'auto')
    if unit == "auto":
        unit = payload.units
    if unit:
        tags['unit'] = unit

    # Set uuid if required
    uid = tags.pop("uuid", "auto")
    if uid == "auto":
        if message_id is None:
            message_id = str(message)
        device_id = device_id if device_id is not None else str(host)
        uid = str(uuid.uuid3(uuid.NAMESPACE_DNS, f"{message_id}.{device_id}"))
    if uid:
        tags['uuid'] = uid

    gen = DeviceReader(modbus_read,
                       sampling_interval=int(1000 * sampling_interval),
                       interpolate=interpolate,
                       name=name,
                       client=client,
                       host=host,
                       api=api,
                       message=message,
                       precision=precision)

    if max_gap is not None:
        gen = compress_const(gen, int(1000 * max_gap))  # max_gap[s] -> ms

    logger.info(f"Connecting modbus reader for {name} ({message}) at {host} ...")
    hub.connect_reader(gen,
                       measurement=measurement,
                       tags=tags,
                       field_name=field_name)
