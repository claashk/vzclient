#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from vzclient import Buffer
import unittest

class BufferTestCase(unittest.TestCase):

    def test_init(self):
        buf = Buffer(20)
        self.assertEqual(20, buf.capacity)
        self.assertEqual(18, buf.high_water_mark)
        self.assertEqual(0, len(buf))

        buf = Buffer(30, 29)
        self.assertEqual(30, buf.capacity)
        self.assertEqual(29, buf.high_water_mark)
        self.assertEqual(0, len(buf))

        self.assertRaises(ValueError, Buffer, 30, 31)

    def test_write(self):
        buf = Buffer(20)
        n = buf.write(b"Hello")
        self.assertEqual(5, n)
        self.assertEqual(n, len(buf))
        n = buf.write(b" World", b"!")
        self.assertEqual(7, n)
        self.assertEqual(12, len(buf))

        self.assertEqual(b"Hello World!", buf.data())
        self.assertFalse(buf.is_full())

        buf.write(b"876543")
        self.assertTrue(buf.is_full())
        buf.write(b"21")
        self.assertTrue(buf.is_full())
        self.assertEqual(buf.capacity, len(buf))
        self.assertRaises(BufferError, buf.write, b"Overflow")


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(BufferTestCase)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )
