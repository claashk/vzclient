#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
from datetime import datetime
import unittest
import logging
from io import StringIO

from vzclient.asyncio import DeviceReader
from vzclient.asyncio.device_reader import logger
from vzclient.constants import time

DEFAULT_VALUES = [float(x) for x in range(10)]

async def stub_reader(sleep=0.1, values=iter(DEFAULT_VALUES)):
    await asyncio.sleep(sleep)
    t = datetime.utcnow()
    return t, next(values)


class DeviceReaderTestCase(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)
        self.stream = StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.log = logger
        self.log.setLevel(logging.DEBUG)
        for handler in self.log.handlers:
            self.log.removeHandler(handler)
        self.log.addHandler(self.handler)
        self.reader = None
        self.result = []
        self.raw = []

    @property
    def x(self):
        return [x for t, x in self.result]

    @property
    def t(self):
        return [t for t, x in self.result]

    def _wrap(self, count=5, **kwargs):
        async def t(r, c):
            result = []
            raw = []
            async for ti, x in r:
                result.append((ti, x))
                c -= 1
                raw.append(r.get_value())
                # print(count, ":", time(t), x)
                if c == 0:
                    await r.stop()
            return result, raw
        self.reader = DeviceReader(stub_reader, **kwargs)
        self.result, self.raw = self.loop.run_until_complete(t(self.reader,
                                                               count))

    def test_raw_read(self):
        it = iter(DEFAULT_VALUES)
        self._wrap(sampling_interval=1000,
                   interpolate=False,
                   allowed_errors=0,
                   count=4,
                   name="test_raw_read_1",
                   values=it)
        self.assertEqual([0., 1., 2., 3.], self.x)
        self.assertEqual(self.raw, self.result)

        self._wrap(sampling_interval=500,
                   interpolate=False,
                   allowed_errors=0,
                   count=5,
                   sleep=0.01,
                   name="test_raw_read_2",
                   values=it)
        output = self.stream.getvalue()
        self.assertEqual([4., 5., 6., 7., 8.], self.x)
        self.assertEqual(4,
                         output.count("Reading from device 'test_raw_read_1'"))
        self.assertEqual(5,
                         output.count("Reading from device 'test_raw_read_2'"))

    def test_interpolate(self):
        it = iter(DEFAULT_VALUES)
        self._wrap(sampling_interval=500,
                   interpolate=True,
                   allowed_errors=0,
                   count=8,
                   sleep=0.01,
                   name="test_interpolate",
                   values=it)
        t0, x0 = None, None
        for i, (t, x) in enumerate(self.result):
            self.assertEqual(0, t % 500)
            tr, xr = self.raw[i]
            self.assertLessEqual(t, tr)
            self.assertLessEqual(x, xr)
            # print(t, tr - t, 500 * (xr - x))
            self.assertGreaterEqual(x, i)
            self.assertLessEqual(x, i + 1)
            if t0 is not None:
                self.assertAlmostEqual(500, t - t0)
                self.assertGreaterEqual(x, x0)
            t0 = t
            x0 = xr

        output = self.stream.getvalue()
        self.assertEqual(9,  # interpolation requires one additional read
                         output.count("Reading from device 'test_interpolate'"))


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(DeviceReaderTestCase)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )
