#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import unittest
from copy import deepcopy
import asyncio, logging, io
from contextlib import redirect_stderr
from pathlib import Path

from vzclient import ToolBase

TEST_DIR = Path(__file__).parent


class SyncTool(ToolBase):
    def main(self, throw=False, **kwargs):
        super().main(**kwargs)
        if throw:
            raise RuntimeError("There you go")
        return self.config['return_value']


class AsyncTool(SyncTool):
    async def main(self, **kwargs):
        await asyncio.sleep(1)
        super().main(**kwargs)


SyncTool.default_config.update(
    return_value=3,
    other_options={"key1": 1, "key2": 2, "key3": "val3"}
)


class ToolBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.cfg_path = TEST_DIR / "tool_base_test.config.yml"
        self.stream = io.StringIO()
        self.handler = logging.StreamHandler(self.stream)
        self.log = logging.getLogger()
        for handler in self.log.handlers:
            self.log.removeHandler(handler)
        self.log.addHandler(self.handler)

        with open(self.cfg_path, mode='w') as cfg:
            cfg.write(
                "return_value: 0\n"
                "other_options:\n"
                "  key1: 2\n"
                "\n"
            )

    def tearDown(self):
        if self.cfg_path.exists():
            self.cfg_path.unlink()

    def test_unify_opts(self):
        d = {"one": 1, "two": 2, "three": None}
        self.assertDictEqual(d, ToolBase.unify_sectionwise(d))

        defaults = {"one": 2, "four": 4}
        expected = d.copy()
        expected.update(four=4)
        self.assertDictEqual(expected, ToolBase.unify_sectionwise(d, defaults))

        d['section1'] = d.copy()
        defaults['section1'] = defaults.copy()
        expected['section1'] = expected.copy()
        self.assertDictEqual(expected, ToolBase.unify_sectionwise(d, defaults))

        defaults['section2'] = {
            "name1": "value1",
            "name2": "value2",
            "sub_section": {
                "name1.1": "value1.1",
                "name1.2": "value1.2"
            }
        }
        expected['section2'] = deepcopy(defaults['section2'])
        self.assertDictEqual(expected, ToolBase.unify_sectionwise(d, defaults))

        d['section2'] = {"name1": "new_value1"}
        expected['section2']['name1'] = "new_value1"
        self.assertDictEqual(expected, ToolBase.unify_sectionwise(d, defaults))

    def test_run(self):
        tool = ToolBase()
        def main(exit_code=0):
            return exit_code

        exit_code = tool.run(main, exit_code=5)
        self.assertEqual(5, exit_code)

        async def amain(exit_code=0):
            await asyncio.sleep(1)
            return exit_code

        exit_code = tool.run(amain, exit_code=3)
        self.assertEqual(3, exit_code)

    def test_missing_args(self):
        tool = SyncTool(logger=self.log)

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            exit_code = tool.run()

        output = self.stream.getvalue()
        self.assertEqual(2, exit_code)
        self.assertIn("Program terminated abnormally", output)
        buf = stderr.getvalue()
        self.assertIn("error: the following arguments are required: path", buf)

    def test_main(self):
        tool = SyncTool(logger=self.log)

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            exit_code = tool.run(cmd_line_args=['-v', str(self.cfg_path)])

        self.assertEqual(tool.log_level, logging.INFO)
        output = self.stream.getvalue()
        self.assertEqual(0, exit_code)
        self.assertIn("Program terminated successfully", output)
        buf = stderr.getvalue()
        self.assertEqual("", buf)

    def test_main_throw(self):
        tool = SyncTool(logger=self.log)

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            exit_code = tool.run(throw=True,
                                 cmd_line_args=['-v', str(self.cfg_path)])

        self.assertEqual(tool.log_level, logging.INFO)
        output = self.stream.getvalue()
        self.assertEqual(1, exit_code)
        self.assertIn("RuntimeError", output)
        self.assertIn("Traceback", output)
        self.assertIn("There you go", output)
        buf = stderr.getvalue()
        self.assertEqual("", buf)

    def test_amain_throw(self):
        tool = AsyncTool(logger=self.log)

        stderr = io.StringIO()
        with redirect_stderr(stderr):
            exit_code = tool.run(throw=True,
                                 cmd_line_args=['-v', str(self.cfg_path)])

        self.assertEqual(tool.log_level, logging.INFO)
        output = self.stream.getvalue()
        self.assertEqual(1, exit_code)
        self.assertIn("RuntimeError", output)
        self.assertIn("Traceback", output)
        self.assertIn("There you go", output)
        buf = stderr.getvalue()
        self.assertEqual("", buf)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(ToolBaseTestCase)

if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run( suite() )
