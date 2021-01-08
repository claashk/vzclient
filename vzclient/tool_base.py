import logging, asyncio, argparse, io
from inspect import iscoroutinefunction
from copy import deepcopy
import yaml


class ToolBase:
    """Helper function for command line tools reading yaml configuration files

    Implements basic support for command line tools which are intended to be
    run as service and which are configured via a (yaml) log file.
    """
    default_config = dict()
    def __init__(self,
                 logger=None,
                 description=""):
        self.log = logger if logger is not None else logging.getLogger()
        self.parser = argparse.ArgumentParser(description=description)
        self.config = dict()
        self.loop = None
        self.debug = False

    @property
    def log_level(self):
        if self.log is None:
            return None
        return self.log.getEffectiveLevel()

    def main(self, cmd_line_args=None):
        """Main function. Override this in derived tools"""
        self.configure_parser()
        cfg = self.parse_cmd_line_args(cmd_line_args)
        self.configure_logging(verbose_mode=cfg.verbose, log_file=cfg.logfile)
        self.parse_config_file(cfg.path[0], self.default_config)
        return 0

    def run(self, func=None, **kwargs):
        """Run a function or coroutine as main

        Arguments:
            func (callable or coroutine): Function to be called as main. If
                ``None`` it defaults to :meth:`ToolBase.main`.
            **kwargs: Keyword arguments passed to func.

        Return:
            int: Return value of func
        """
        if func is None:
            func = self.main
        if iscoroutinefunction(func):
            return self.main_wrapper(self.async_wrapper, func, **kwargs)
        return self.main_wrapper(func, **kwargs)

    def configure_parser(self):
        self.parser.add_argument(
            "--logfile", "-l",
            help="Path to logfile. If not specified, log goes to stderr"
        )
        self.parser.add_argument(
            "--verbose", "-v",
            help="Set verbose mode. Use more often to increase log level",
            action='count',
            default=0
        )
        self.parser.add_argument(
            "path",
            help="Path to yaml configuration file",
            nargs=1
        )

    def parse_cmd_line_args(self, args):
        config = self.parser.parse_args(args)
        return config

    def configure_logging(self, verbose_mode=0, log_level=None, log_file=None):
        log_levels = {
            0: logging.WARNING,
            1: logging.INFO,
            2: logging.DEBUG
        }

        third_party_log_levels = {
            0: logging.WARNING,
            1: logging.WARNING,
            2: logging.INFO
        }

        if log_level is None:
            log_level = log_levels.get(verbose_mode, logging.DEBUG)

        third_party_log_level = third_party_log_levels.get(verbose_mode,
                                                           logging.DEBUG)
        if verbose_mode > 3:
            self.debug = True

        self.log.setLevel(log_level)
        if log_file:
            h = logging.FileHandler(log_file, 'a')
            for handler in self.log.handlers:
                self.log.removeHandler(handler)
            self.log.addHandler(h)

        return third_party_log_level

    def parse_config_file(self, path, default_config=None):
        """Parse yaml configuration file

        Arguments:
            path (str): Path to config file

        Return:
            tuple: Three dictionaries containing default options, default source
            options and
        """
        if default_config is None:
            default_cfg = dict()
        else:
            default_cfg = deepcopy(default_config)

        with open(path) as cfg_file:
            yaml_cfg = yaml.load(cfg_file, Loader=yaml.SafeLoader)

        self.config = self.unify_sectionwise(yaml_cfg, default_cfg)

    def main_wrapper(self, main, *args, **kwargs):
        exit_code = 1
        try:
            exit_code = main(*args, **kwargs)
            if exit_code is None:
                exit_code = 0
        except Exception as ex:
            self.log.exception(f"{ex}")
        except SystemExit as ex:
            exit_code = ex.code

        if self.loop is not None and not self.loop.is_closed():
            self.loop.close()
            self.loop = None

        if exit_code:
            self.log.error("Program terminated abnormally")
        else:
            self.log.info("Program terminated successfully")
        return exit_code

    def async_wrapper(self, main, *args, **kwargs):
        if self.loop is None:
            self.loop = asyncio.get_event_loop()

        if self.loop.is_closed():
            self.log.debug("Event Loop closed. Creating a new one ...")
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

        if self.debug:
            self.loop.set_debug(True)
        retval = self.loop.run_until_complete(main(*args, **kwargs))
        return retval

    @staticmethod
    def get_sections(dictionary):
        """Get all sections from a dictionary

        A section is defined as a key which has a dictionary as value.

        Arguments:
            dictionary (dict): Dictionary containing configuration options

        Return:
            set: Set containing names of all sections
        """
        d = dictionary if dictionary is not None else dict()
        return {k for k, v in d.items() if isinstance(v, dict)}

    @staticmethod
    def unify_sectionwise(options, default=None):
        """Unify to dictionaries section wise

        Unify the input dictionaries to one dictionary, where each section of
        `options` is filled with values from `default`, if the respective key
        is missing in `options`.

        Arguments:
            options (dict): (Possibly nested) dictionary with options
            default (dict): Possibly nested dictionary with default options

        Return:
            dict: Merged dictionary with options
        """
        sections = ToolBase.get_sections(options)
        sections.update(ToolBase.get_sections(default))

        retval = dict()
        for section in sections:
            opts = options.get(section, dict())
            if default is not None:
                default_opts = default.get(section, dict())
            else:
                default_opts = dict()
            retval[section] = ToolBase.unify_sectionwise(opts, default_opts)

        for d in (default, options):
            if d is not None:
                retval.update({k: v for k, v in d.items() if k not in sections})

        return retval
