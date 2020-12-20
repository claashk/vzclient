#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
from vzclient import read_vzlogger_config
from vzclient.config_parser import remove_json_comments
import unittest

SHARE = Path(__file__).parent / "share"


class ConfigParserTestCase(unittest.TestCase):

    def test_remove_json_comments(self):
        self.skipTest("For debugging only")
        cfg_path = SHARE / "vzlogger.conf"
        with open(cfg_path) as src:
            text = src.read()
            for i, line in enumerate(remove_json_comments(text).split("\n")):
                print("{}: {}".format(i + 1, line))

    def test_parse_vzlogger_config(self):
        cfg_path = SHARE / "vzlogger.conf"
        cfg = read_vzlogger_config(cfg_path)

        ids = set()
        for meter in cfg['meters']:
            try:
                channels = meter['channels']
            except KeyError:
                try:
                    channels = [meter['channel']]
                except KeyError:
                    channels = []

            for ch in channels:
                self.assertEqual(ch.get('api', 'volkszaehler'), "volkszaehler")
                self.assertEqual(ch['middleware'], "http://localhost/middleware.php")
                ids.add(ch.get('identifier', None))

        for key in ["power", "sensor0/power", "counter"]:
            self.assertIn(key, ids)

        # cfg['mqtt'] contains host, port, topic, enabled.
        # channel names are: topic.replace("/", ".") + "chn{}".format(channel_number) + "raw" / "agg"


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(ConfigParserTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )
