#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest
import asyncio, time
import signal
from vzclient import Service


class Main:
    def __init__(self):
        self._stop = False
        self._count = 0

    @property
    def count(self):
        return self._count

    async def arun(self):
        self._count = 0
        while not self._stop:
            self._count += 1
            await asyncio.sleep(1)

    def run(self):
        self._count = 0
        while not self._stop:
            self._count += 1
            time.sleep(1)

    def stop(self):
        self._stop = True

    async def astop(self):
        self._stop = True
        await asyncio.sleep(2)
        return -self._count


async def main_wrapper():
    main = Main()
    async with Service(signals=[signal.SIGALRM], callback=main.astop) as service:
        await main.arun()
        return service


class ServiceTestCase(unittest.TestCase):
    def test_service(self):
        # For testing purpose we use 5-second alarm with SIGALARM
        main = Main()
        with Service(signals=[signal.SIGALRM], callback=main.stop) as service:
            self.assertFalse(service.cancel)
            signal.alarm(2)
            main.run()
        self.assertEqual(main.count, 2)
        self.assertTrue(service.cancel)

    def test_async_service(self):
        loop = asyncio.get_event_loop()
        main = Main()
        with Service(signals=[signal.SIGALRM], callback=main.stop) as service:
            self.assertFalse(service.cancel)
            signal.alarm(3)
            loop.run_until_complete(main.arun())
        self.assertEqual(main.count, 3)
        self.assertTrue(service.cancel)

    def test_async_service_with_cleanup(self):
        loop = asyncio.get_event_loop()
        signal.alarm(2)
        service = loop.run_until_complete(main_wrapper())
        self.assertEqual(service.result, -2)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(ServiceTestCase)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )
