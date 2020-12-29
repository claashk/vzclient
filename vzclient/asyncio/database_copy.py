import logging
import asyncio
from copy import deepcopy
from datetime import datetime
import yaml
import re

from .influx_driver import InfluxDriver
from .mysql_driver import MySqlDriver
from .compress import compress_const
from . import transform


logger = logging.getLogger("vzclient")


class DatabaseCopy:
    """Copy implementation for channel information

    Copies data from a source to a sink. Source and sink are abstracted in terms
    of their respective drivers. A source driver has to provide a ``get_reader``
    method and a ``get_writer`` method. The returned reader should at least
    provide the following asynchronous methods:
    - ``get_channels``
    - ``iter_chunks``
    while a writer should expose a ``write_chunk`` method.

    Arguments:
        src (dict): Source driver configuration
        dest (dict): Sink / destination driver configuration
        includes (iterable): Iterable of dict-like objects. Each dict describes
            the input option of one channel to copy.
        excludes (dict): Dictionary containing various exclusion criteria.
            Excludes take precedence over includes.
    """
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S:"

    def __init__(self,
                 source,
                 destination,
                 includes=None,
                 excludes=None,
                 begin=None,
                 end=None,
                 max_gap=None,
                 chunk_size=8192,
                 **kwargs):
        self._includes = list()
        self._excludes = dict()
        self._default_src = source

        self.init_includes(includes,
                           source=source,
                           destination=destination,
                           begin=begin,
                           end=end,
                           max_gap=int(max_gap) if max_gap is not None else None,
                           chunk_size=chunk_size,
                           **kwargs)
        if excludes is not None:
            self.init_excludes(**excludes)

    async def copy(self):
        channels = await self.get_channels()
        jobs = []
        for channel in channels:
            if self.exclude(channel):
                logger.debug("Channel {} explicitly excluded for copy process"
                             .format(self.get_name(channel)))
                continue
            copy, opts = self.include(channel)
            if copy:
                jobs.append(self.copy_channel(channel, **opts))
                logger.debug("Will copy channel {}"
                             .format(self.get_name(channel)))
        logger.debug("Started {} copy jobs".format(len(jobs)))
        await asyncio.gather(*jobs)

    async def get_channels(self, source=None):
        """Get information about available channels from source

        Return:
            list: List containing one dictionary with channel information per
                channel.
        """
        if source is None:
            source = deepcopy(self._default_src)
        src, sopt, opts = self.get_io_helper(None, "source", source)
        async with src.get_reader(**sopt) as reader:
            channels = await reader.get_channels()
            return channels

    async def copy_channel(self,
                           channel,
                           source,
                           destination,
                           begin,
                           end,
                           max_gap,
                           chunk_size,
                           transform=None,
                           **kwargs):
        name = self.get_name(channel)
        logger.info(f"Copying channel '{name}' ...")
        start = datetime.utcnow()
        src, sopt, opts = self.get_io_helper(channel,
                                             "source",
                                             source,
                                             **kwargs)
        dest, dopt, opts = self.get_io_helper(channel,
                                              "destination",
                                              destination,
                                              **opts)
        for k in opts.keys():
            logger.warning(f"Ignoring unsupported option {k} for channel {name}")

        async with src.get_reader(**sopt) as reader, dest.get_writer(**dopt) as writer:
            gen = reader.iter_chunks(channel,
                                     begin=begin,
                                     end=end,
                                     chunk_size=chunk_size)
            if transform is not None:
                trafo, trafo_args = self.get_transform(channel, **transform)
                if trafo is not None:
                    gen = trafo(gen, **trafo_args)
            if max_gap is not None:
                gen = compress_const(gen, max_gap)

            nmeas = 0
            async for chunk in gen:
                if chunk:
                    n = len(chunk)
                    logger.debug(f"Copying {n} measurements for channel {name}")
                    await writer.write_chunk(chunk)
                    nmeas += n

        elapsed_time = (datetime.utcnow() - start).total_seconds()
        logger.info(f"Copied {nmeas} measurements for channel '{name}' in {elapsed_time}s")
        return nmeas

    def exclude(self, channel):
        """Check if a channel shall be excluded

        Arguments:
            channel (dict): Channel information

        Return:
            bool: True if and only if channel shall be excluded from copy
        """
        for attr in ("title", "type", "class", "id"):
            for pattern in self._excludes.get(attr, []):
                if pattern.match(channel.get(attr, "").strip()):
                    return True
        return False

    def include(self, channel):
        """Check if a channel shall be included

        Arguments:
            channel (dict): Channel information

        Return:
            bool: True if and only if channel shall be excluded from copy
        """
        name = self.get_name(channel)
        for pattern, info in self._includes:
            if pattern.match(name):
                return True, info
        return False, dict()

    def get_io_helper(self, channel, type, opts, **kwargs):
        if type == "source":
            t = "reader"
        elif type == "destination":
            t = "writer"
        else:
            ValueError("Invalid IO type: '{type}'")
        driver = opts.pop("driver")
        try:
            f = getattr(self, f"get_{driver}_{t}")
        except AttributeError:
            raise ValueError(f"{driver} {t} currently not supported")
        return f(channel, opts, **kwargs)

    def get_mysql_reader(self, channel, opts, **kwargs):
        driver = MySqlDriver(**opts)
        reader_args = {}
        return driver, reader_args, kwargs

    def get_influx_writer(self,
                          channel,
                          opts,
                          measurement="volkszaehler",
                          copy_tags=None,
                          add_tags=None,
                          field_name="value",
                          buffer_size=500000,
                          **kwargs):
        driver = InfluxDriver(**opts)
        tags = dict()
        if copy_tags is None:
            copy_tags = {}

        for key in copy_tags:
            if key == "unit":
                value = self.get_unit(channel)
            elif key == "uuid":
                value = channel.get('uuid', "<none>")
            elif key in ("title", "name"):
                key = "title"
                value = self.get_name(channel)
            else:
                value = channel[key]
            tags[key] = value
        if add_tags is not None:
            tags.update(**add_tags)
        writer_args = dict(measurement=measurement,
                           tags=tags if tags else None,
                           field_name=field_name,
                           buffer_size=buffer_size)
        return driver, writer_args, kwargs

    def init_excludes(self, titles=None, types=None, classes=None, ids=None):
        self._excludes.clear()
        excludes = dict()
        attrs = {'title': titles, 'type': types, 'class': classes, 'id': ids}
        for key, val in attrs.items():
            if not val:
                continue
            if isinstance(val, str):
                val = [val]
            excludes[key] = [self.make_re(s) for s in val]
        self._excludes.update(excludes)

    def init_includes(self,
                      includes=None,
                      source=None,
                      destination=None,
                      **kwargs):
        """Configure channels to copy including copy properties

        Arguments:
            includes (list): List containing either a string or a dictionary
                for each channel to copy. The string should contain a regular
                expression pattern matching one or more channel names. The
                dictionary can be used to pass additional channel specific copy
                options. Allowed are all values included in the 'defaults'
                section of the config file ('source', 'destination', ...)
            source (dict): Dictionary with default options for the source. Values
                which cannot be retrieved from ``includes['source']`` are filled
                from this dictionary.
            destination (dict): Dictionary with default options for the source.
                Values which cannot be retrieved from ``includes['destination']``
                are filled from this dictionary.
            **kwargs: Additional default values for copy options. Values
                which cannot be retrieved from ``includes`` are filled with
                values specified here.
        """
        if includes is None:
            includes = ['*']

        incs = []
        for inc in includes:
            print("Creating ", inc)
            src = deepcopy(source) if source is not None else dict()
            dest = deepcopy(destination) if destination is not None else dict()
            opts = dict(source=src, destination=dest, **deepcopy(kwargs))
            if isinstance(inc, str):
                pattern = self.make_re(inc)
            else:
                pattern = self.make_re(inc.pop("channel"))
                opts['source'].update(inc.pop("source", dict()))
                opts['destination'].update(inc.pop("destination", dict()))
                opts.update(inc)
            incs.append((pattern, opts))
        self._includes = incs

    @classmethod
    def from_yaml(cls, path, default_config=None):
        if default_config is not None:
            cfg = deepcopy(default_config)
        else:
            cfg = dict()
        with open(path) as cfg_file:
            yaml_cfg = yaml.load(cfg_file, Loader=yaml.SafeLoader)

        defaults = cfg.pop('defaults', dict())
        yaml_defaults = yaml_cfg.pop('defaults', dict())

        src = defaults.pop("source", dict())
        src.update(yaml_defaults.pop("source", dict()))

        dest = defaults.pop('destination', dict())
        dest.update(yaml_defaults.pop("destination", dict()))

        defaults.update(yaml_defaults)

        includes = yaml_cfg.pop("include", cfg.pop('include', ['*']))

        excludes = cfg.pop("exclude", dict())
        excludes.update(yaml_cfg.pop('exclude', dict()))

        assert not cfg

        for key in ['begin', 'end']:
            val = defaults.pop(key, "")
            if isinstance(val, str) and val:
                val = datetime.strptime(val, cls.TIME_FORMAT)
            if isinstance(val, datetime):
                defaults[key] = val

        max_gap = defaults.pop("max_gap", "")
        if max_gap:
            defaults['max_gap'] = int(max_gap)

        if isinstance(includes, str):
            includes = [includes]
        for key in yaml_cfg.keys():
            logger.warning(f"Ignored unknown section {key} in {path}")
        return cls(source=src,
                   destination=dest,
                   includes=includes,
                   excludes=excludes,
                   **defaults)

    @staticmethod
    def make_re(pattern):
        """Create a regular expression from a pattern string

        Replaces wildcards with regular expression patterns

        Arguments:
            pattern (str): Pattern including wildcards

        Return:
            re.Pattern: Regular Expression Object
        """
        return re.compile(pattern.replace("*", ".*").replace("?", "."))

    @staticmethod
    def get_name(channel):
        try:
            name = channel['title']
        except KeyError:
            name = channel.get('id', "<unknown>")
        return name.strip()

    @staticmethod
    def get_unit(channel, default=None):
        default_units = {
            "electric meter": "kWh",
            "temperature": "°C",
            "current": "A",
            "voltage": "V"
        }
        try:
            return channel['unit']
        except KeyError:
            pass
        try:
            return default_units[channel['type']]
        except KeyError:
            if default is None:
                raise
        return default

    @staticmethod
    def get_transform(channel, type="linear", **kwargs):
        if type == "linear":
            f = transform.chunk_trafo
            fargs = dict(trafo=transform.linear,
                         scale=float(kwargs.pop('scale', 1.)),
                         offset=float(kwargs.pop('offset', 0.)))
        elif type == "auto-resolution":
            scale = 1. / float(channel.get('resolution', 1.))
            if scale != 1.:
                f = transform.chunk_trafo
                fargs = dict(trafo=transform.linear, scale=scale)
            else:
                f, fargs = None, dict()
        else:
            raise ValueError(f"Invalid transformation type: '{type}'")

        for key in kwargs.keys():
            name = DatabaseCopy.get_name(channel)
            logger.warning(f"Channel {name}: Ignored unsupported argument "
                           f"{key} for {type} transform")
        return f, fargs
