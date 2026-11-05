#!/usr/bin/env python3
"""Check CTest restart binary selection without requiring built FDB binaries."""

import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fdb_test_runner import TestRunner


class RestartTestPolicyTest(unittest.TestCase):
    def setUp(self):
        logger = patch.object(TestRunner, "_logger", logging.getLogger(__name__))
        logger.start()
        self.addCleanup(logger.stop)

    def assert_selection(self, directory, version, expected):
        version_output = f"FoundationDB (v{version})\n".encode()
        with patch.object(
            TestRunner.subprocess, "check_output", return_value=version_output
        ) as check_output:
            policy = TestRunner.RestartTestPolicy(
                f"/source/tests/restarting/{directory}/Restart-1.toml", "old", "new"
            )
        check_output.assert_called_once_with(["old", "--version"])
        self.assertEqual((policy.first_binary(), policy.second_binary()), expected)

    def test_bounded_versions(self):
        for direction, selected in (("from", ("old", "new")), ("to", ("new", "old"))):
            for version, eligible in (
                ("7.4.2", False),
                ("7.4.3", True),
                ("7.4.4", False),
                ("7.4.8", False),
                ("8.0.0", False),
            ):
                with self.subTest(direction=direction, version=version):
                    self.assert_selection(
                        f"{direction}_7.4.3_until_7.4.4",
                        version,
                        selected if eligible else ("new", "new"),
                    )

    def test_unbounded_versions(self):
        for direction, selected in (("from", ("old", "new")), ("to", ("new", "old"))):
            for version, eligible in (
                ("7.3.99", False),
                ("7.4.0", True),
                ("7.4.8", True),
                ("8.0.0", True),
            ):
                with self.subTest(direction=direction, version=version):
                    self.assert_selection(
                        f"{direction}_7.4.0",
                        version,
                        selected if eligible else ("new", "new"),
                    )

    def test_bounds_compare_numeric_components(self):
        for direction, selected in (("from", ("old", "new")), ("to", ("new", "old"))):
            for version, eligible in (("7.4.9", True), ("7.4.10", False)):
                with self.subTest(direction=direction, version=version):
                    self.assert_selection(
                        f"{direction}_7.4.3_until_7.4.10",
                        version,
                        selected if eligible else ("new", "new"),
                    )

    def test_unversioned_restarts(self):
        self.assert_selection("same_version", "7.4.3", ("new", "new"))

    def test_no_old_binary(self):
        policy = TestRunner.RestartTestPolicy(
            "/source/tests/restarting/from_7.3.0_until_7.4.0/Restart-1.toml",
            None,
            "new",
        )
        self.assertEqual(
            (policy.first_binary(), policy.second_binary()), ("new", "new")
        )


if __name__ == "__main__":
    unittest.main()
