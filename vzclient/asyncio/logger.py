import logging
from uuid import uuid3, NAMESPACE_DNS
from copy import deepcopy

from .device_reader import DeviceReader
from .compress import compress_const
from .influx_hub import InfluxHub
from ..tool_base import ToolBase
from ..service import Service
from ..constants import CHANNEL_TYPES


SENSOR_TYPES = {s['name'] for s in CHANNEL_TYPES}
logger = logging.getLogger("vzclient")


async def modbus_read(client,
                      host,
                      api,
                      message=None,
                      message_name="message",
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
        message_name (str): Message name. Only used in log messages.
        precision (int): Number of digits to round to. If ``None``, no
            rounding takes place. Defaults to ``None``.
        **kwargs: Keyword arguments passed to the client.

    Return:
        tuple: timestamp (local UTC [ms since EPOCH]) and value read from
        host.
    """
    logger.info(f"Connecting to {host} ...")
    async with client(host=host, api=api, **kwargs) as cli:
        if not cli.is_connected():
            logger.error(f"Modbus connection to {host} failed")
        value = await cli.get(message)
        if precision is not None:
            value = round(value, precision)
    logger.debug(f"Got {message_name} ({message}) of {value} from {host}")
    t = None
    return t, value


class InfluxLogger(ToolBase):
    """Base class for logging to influx database

    Arguments:
        drivers (dict): Dictionary containing a driver name as key and a tuple
            as value. The tuple shall contain the ModbusClient type and the API
            dictionary.
        default_
    """
    third_party_libs = ["aioinflux"]

    def __init__(self, logger, description):
        super().__init__(logger=logger, description=description)
        self.influx_config = dict()
        self.hub_config = dict()
        self._logs = [] #

    async def main(self, **kwargs):
        """Main implementation"""
        super().main()
        hub = InfluxHub(**self.hub_config)

        async with Service(callback=hub.stop) as service:
            hub.connect_writer(**self.influx_config)
            self.connect_to_hub(hub)
            await hub
        return 0

    def configure_logging(self, **kwargs):
        third_party_log_level = super().configure_logging(**kwargs)
        for lib in self.third_party_libs:
            logging.getLogger(lib).setLevel(third_party_log_level)

    def parse_config_file(self, **kwargs):
        super().parse_config_file(**kwargs)

        influx_cfg = self.config.pop('destination', dict())
        driver = influx_cfg.pop('driver', 'influx').lower()

        buffer_size = influx_cfg.pop('buffer_size', 100000)
        max_buffer_age = int(1000 * influx_cfg.pop('max_buffer_age'))
        max_retries = int(influx_cfg.pop('max_retries'))
        self.hub_config.update(buffer_size=buffer_size,
                               max_buffer_age=max_buffer_age,
                               max_retries=max_retries)

        if driver != "influx":
            raise ValueError(f"Invalid destination driver: {driver}")
        self.influx_config.update(influx_cfg)

        # We create one config dict per message, where default options are copied
        # from defaults and the log blocks unless specified per message

        defaults = self.config.pop('defaults', dict())
        logs = self.config.pop('logs', ['*'])

        self._logs.clear()
        nerr = 0
        for i, log in enumerate(logs, 1):
            if isinstance(log, dict):
                cfg = self.unify_sectionwise(log, defaults)
                try:
                    channels = cfg.pop("channel")
                except KeyError:
                    nerr += 1
                    self.log.error(f"In log {i}: "
                                   f"Missing mandatory keyword 'channel'")
                    continue
            else:
                channels = log
                cfg = defaults
            log_cfg = deepcopy(cfg)
            try:
                src_cfg = log_cfg.pop("source")
            except KeyError:
                self.log.error(
                    f"In log {i}: Missing mandatory source section")
                nerr += 1
                continue
            try:
                driver = src_cfg.pop("driver")
            except KeyError:
                self.log.error(f"In log {i}, source section: "
                               f"Missing mandatory driver keyword")
                nerr += 1
                continue
            try:
                self.add_channels(driver=driver,
                                  channels=channels,
                                  source=src_cfg,
                                  **log_cfg)
            except Exception as ex:
                self.log.error(f"In log {i}: {ex}")
                nerr += 1
        if nerr:
            raise RuntimeError(f"Encountered {nerr} error(s) in log file")

    def add_channels(self,
                     driver,
                     channels,
                     source,
                     use_device_time=True,
                     sampling_interval=30,
                     interpolate=True,
                     max_gap=None,
                     precision="auto",
                     **kwargs):
        """Add a channel

        This is a very simple default implementation, which should be
        re-implemented by derived classes.

        Arguments:
            driver (str): Driver. If ``None``, it simply forwards the
                preprocessed options. If it starts with ``'modbus'``, then the
                modbus reader will be invoked with the preprocessed options.
            channels: Single channel or list of channels to add
            source (dict): Source related information depending on driver.
            use_device_time(bool): If ``True`` use native device time.
            sampling_interval (float): Sampling interval of the device [s].
                Defaults to 30 seconds.
            interpolate (bool): If ``True`` interpolate to timestamps which are
                integer multiples of `sampling_interval`.
            max_gap (float): Omit repetition of identical consecutive values
                unless the resulting time gap is larger than `max_gap` seconds.
                If ``None``, no compression is applied. Defaults to ``None``.
            precision (int): Number of decimal digits to round value to or
                ``None`` to disable rounding. Defaults to ``'auto'``.
            **kwargs: Additional driver dependent keyword arguments
        """
        if max_gap is not None:
            max_gap = int(max_gap)
            if max_gap < 0:
                max_gap = None

        opts = dict(driver=driver,
                    channels=channels,
                    source=source,
                    max_gap=max_gap,
                    use_device_time=bool(use_device_time),
                    sampling_interval=int(1000 * sampling_interval),
                    interpolate=bool(interpolate),
                    precision=precision,
                    **kwargs)

        if driver is None:
            return opts

        if driver.startswith("modbus"):
            self.add_modbus_channels(**opts)
            return None

        raise NotImplementedError(f"Unsupported driver: '{driver}'")

    def add_modbus_channels(self, driver, channels, source, **kwargs):
        """Add modbus channels to this logger

        Arguments:
            driver (str): Driver.
            channels (str): One or more channels.
            source (dict): Source specific information
            **kwargs: Keyword arguments passed verbatim to
                :meth:`add_modbus_reader`.
        """
        payloads = set(self.iter_channels(driver, channels))
        api = self.get_modbus_api(driver)
        client = self.get_client(driver)
        kwargs.update(source)

        for payload in payloads:
            self.add_modbus_reader(driver=driver,
                                   payload=payload,
                                   api=api,
                                   client=client,
                                   **kwargs)

    def add_modbus_reader(self,
                          driver,
                          payload,
                          api,
                          client,
                          max_gap=None,
                          device_id=None,
                          host="localhost",
                          precision="auto",
                          measurement="volkszaehler",
                          tags=None,
                          field_name="value",
                          **kwargs):
        """Add messages to log

        Arguments:
            sampling_interval (float): Sampling interval of the device [s].
                Defaults to 30 seconds.
            interpolate (bool): If ``True`` interpolate to timestamps which are
                integer multiples of `sampling_interval`.
            max_gap (float): Omit repetition of identical consecutive values
                unless the resulting time gap is larger than `max_gap` seconds.
                If ``None``, no compression is applied. Defaults to ``None``.
            precision (int): Number of decimal digits to round value to or
                ``None`` to disable rounding. Defaults to ``None``.
            messages(Payload, int, str, iterable): Messages to add. If the value
                can be converted to an integer, then it is converted to a
                Payload using the api inferred from driver. If it is a string not
                convertible to an integer, the string is interpreted as regular
                expression run through ``run_by_name(api, messages)``. For an
                iterable the rules above will be applied elementwise
        """
        if precision == "auto":
            precision = self.get_precision(driver, payload)
        elif precision is not None:
            precision = int(precision)

        name = self.get_name(driver, payload)

        gen = DeviceReader(modbus_read,
                           name=name,
                           client=client,
                           host=host,
                           api=api,
                           message=payload.address,
                           message_name=name,
                           precision=precision,
                           **kwargs)

        if max_gap is not None:
            # TODO This does not WORK
            # gen = compress_const(gen, int(1000 * max_gap))  # max_gap[s] -> ms
            raise NotImplementedError("Not yet implemented")

        if tags is None:
            tags = dict()
        else:
            tags = deepcopy(tags)

        self._logs.append(dict(reader=gen,
                               measurement=str(measurement),
                               tags=self.create_tags(
                                   driver=driver,
                                   channel=payload,
                                   message_id=str(payload.address),
                                   device_id=str(device_id),
                                   host=host,
                                   **tags),
                               field_name=field_name))

        self.log.info(f"Configured modbus channel {name} ({payload.address})")

    def iter_channels(self, driver, channels):
        """Iterate over all channels for a specific driver

        Arguments:
            driver (str): Driver name.
            channels (iterable): Iterable of channels

        Yield:
        """
        yield from channels

    def get_modbus_api(self, driver):
        """Get Modbus API definition for a driver

        Needs to be implemented by derived class

        Arguments:
            driver (str): Driver name

        Return:
            dict: Modbus API definition
        """
        raise NotImplementedError

    def get_client(self, driver):
        """Get Modbus API definition for a driver

        Arguments:
            driver (str): Driver name

        Return:
            dict: Modbus API definition
        """
        raise NotImplementedError

    def get_precision(self, driver, channel):
        """Get default precision for a channel depending on the driver

        Arguments:
            driver (str): Driver
            channel: Channel information. Type depends on driver.

        Return:
            int: Default channel precision
        """
        return None

    def get_name(self, driver, channel):
        return channel.name

    def get_sensor_type(self, driver, channel):
        return channel.sensor_type

    def get_units(self, driver, channel):
        return channel.units

    def create_tags(self,
                    driver,
                    channel,
                    message_id=None,
                    device_id=None,
                    host="localhost",
                    title="auto",
                    type="auto",
                    unit="auto",
                    uuid="auto",
                    **kwargs):
        """Create tags dictionary for a payload

        Arguments:
            payload (): payload object.
            message_id (int or str): Message ID used to form uuid. Defaults to
                ``str(payload.address)``.
            device_id (str): Unique device ID used to create UUID. Defaults to
                `host`. The UUID will be created from the string
                ``"<modbus address>.<device_id>"`` via uuid3 and NAMESPACE_DNS.
            host (str): Hostname. Will be used to create device ID, if not
                specified.
            title (str): Will be set to `payload` name as delivered by
                ``self.get_name(payload)``.
            type (str): Will be set to `sensor_type` attribute of message
                as defined by `api`.
            unit (str): Will be set to `units` attribute of message as defined by
                `api`.
            uuid (str): Will be set to ``'auto'``, causing the UUID to be
                created from message address and `device_id`
            **kwargs: Additional keyword arguments added verbatim as tags

        Return:
            dict: Tags dictionary
        """
        tags = dict()
        if title == 'auto':
            title = self.get_name(driver, channel)
        if title:
            tags['title'] = title

        if type == 'auto':
            type = self.get_sensor_type(driver, channel)
        if type:
            if type not in SENSOR_TYPES:
                logger.warning(f"In message {title}: unknown sensor type "
                               f"'{type}'")
                tags['type'] = type

        if unit == "auto":
             unit = self.get_units(driver, channel)
        if unit:
            tags['unit'] = unit

        # Set uuid if required
        if uuid == "auto":
            if message_id is None:
                message_id = self.get_name(driver, channel)
            device_id = device_id if device_id is not None else str(host)
            uuid = str(uuid3(NAMESPACE_DNS, f"{message_id}.{device_id}"))
        if uuid:
            tags['uuid'] = uuid

        tags.update({k: str(v) for k, v in kwargs.items()})
        return tags

    def connect_to_hub(self, hub):
        for log in self._logs:
            self.log.info(f"Connecting reader {log['reader'].name} ...")
            hub.connect_reader(**log)
