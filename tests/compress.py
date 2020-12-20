#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from vzclient import compress_const
import unittest

class CompressTestCase(unittest.TestCase):

    def test_compress_const(self):
        series = [(1.1, 1.1), (1.2, 1.2), (1.3, 1.2), (5, 1.2), (6, 1.3)]
        expected = [(1.1, 1.1), (1.2, 1.2), (5, 1.2), (6, 1.3)]
        result = list(compress_const(series))
        self.assertEqual(expected, result)

        series = [(1, 1.1), (2, 1.2), (3, 1.2), (5, 50), (6, 10)]
        result = list(compress_const(series))
        self.assertEqual(series, result)

        series = [(1, 1), (2, 1), (3, 1), (5, 1), (6, 1)]
        expected = [(1, 1), (6, 1)]
        result = list(compress_const(series))
        self.assertEqual(expected, result)

        series = [(1, 1), (2, 1), (3, 1), (5, 1), (6, 1)]
        expected = [(1, 1), (5, 1), (6, 1)]
        result = list(compress_const(series, max_gap=4))
        self.assertEqual(expected, result)

        series = [(1, 1), (2, 1), (3, 1), (5, 1), (6, 1), (7, 1)]
        expected = [(1, 1), (3, 1), (6, 1), (7, 1)]
        result = list(compress_const(series, max_gap=3))
        self.assertEqual(expected, result)

        series = [(1, 1), (2, 1), (3, 1), (4, 1), (6, 1), (7, 1)]
        expected = [(1, 1), (4, 1), (7, 1)]
        result = list(compress_const(series, max_gap=3))
        self.assertEqual(expected, result)

        series = [(1, 1), (2, 1), (2, 2), (4, 2), (6, 2), (7, 2)]
        expected = [(1, 1), (2, 1), (4, 2), (7, 2)]
        result = list(compress_const(series, max_gap=3))
        self.assertEqual(expected, result)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(CompressTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )
