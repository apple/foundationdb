#!/usr/bin/env python3
"""Exercise unit-suite registration and build opt-outs without compiling FDB."""

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

CMAKE, CTEST, module_path = sys.argv[1:4]
del sys.argv[1:4]
MODULE = Path(module_path).resolve()


class UnitTestRegistration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "CMakeLists.txt").write_text(
            f"""
cmake_minimum_required(VERSION 3.24)
project(UnitRegistration NONE)
include(CTest)
set(SANITIZER_OPTIONS "FIRST=one;SECOND=two")
include("{MODULE.as_posix()}")
foreach(suite native_only simulated)
  add_executable(${{suite}} IMPORTED)
  set_target_properties(${{suite}} PROPERTIES IMPORTED_LOCATION "${{CMAKE_COMMAND}}")
  add_custom_target(build_${{suite}}
    COMMAND ${{CMAKE_COMMAND}} -E touch "${{CMAKE_BINARY_DIR}}/${{suite}}.built")
  add_dependencies(${{suite}} build_${{suite}})
endforeach()
register_fdb_unit_tests(native_only)
register_fdb_unit_tests(simulated SIMULATION)
"""
        )

    def run_command(self, *args, success=True):
        result = subprocess.run(
            args, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        if success:
            self.assertEqual(result.returncode, 0, result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result.stdout

    def configure(self, *flags, success=True):
        return self.run_command(
            CMAKE,
            "-S",
            str(self.root),
            "-B",
            str(self.root / "build"),
            "-DUNIT_TEST_SEED=17",
            "-DUNIT_TEST_TIMEOUT=93",
            *flags,
            success=success,
        )

    def inventory(self):
        return json.loads(
            self.run_command(
                CTEST,
                "--test-dir",
                str(self.root / "build"),
                "--show-only=json-v1",
            )
        )["tests"]

    def assert_built(self, expected):
        for suite in ("native_only", "simulated"):
            self.assertEqual(
                (self.root / "build" / f"{suite}.built").exists(), expected
            )

    def test_modes_metadata_and_default_build(self):
        self.configure("-DAUTO_DISCOVER_UNIT_TESTS=ON")
        tests = {test["name"]: test for test in self.inventory()}
        self.assertEqual(
            set(tests),
            {
                "unit/native_only/native",
                "unit/simulated/native",
                "unit/simulated/simulation",
            },
        )
        for name, test in tests.items():
            _, suite, mode = name.split("/")
            expected_args = ["--seed", "17"]
            if mode == "simulation":
                expected_args.append("--simulation")
            self.assertEqual(test["command"][1:], expected_args)
            properties = {p["name"]: p["value"] for p in test["properties"]}
            self.assertEqual(set(properties["LABELS"]), {"unit", suite, mode})
            self.assertEqual(properties["TIMEOUT"], 93)
            self.assertEqual(properties["ENVIRONMENT"], ["FIRST=one", "SECOND=two"])
        self.run_command(CMAKE, "--build", str(self.root / "build"))
        self.assert_built(True)

    def test_opt_out_preserves_explicit_aggregate(self):
        for flag in (
            "BUILD_TESTING=OFF",
            "ENABLE_UNIT_TESTS=OFF",
            "OPEN_FOR_IDE=ON",
            "FOUNDATIONDB_CROSS_COMPILING=ON",
        ):
            with self.subTest(flag=flag):
                self.configure(
                    "-DBUILD_TESTING=ON",
                    "-DENABLE_UNIT_TESTS=ON",
                    "-DOPEN_FOR_IDE=OFF",
                    "-DFOUNDATIONDB_CROSS_COMPILING=OFF",
                    f"-D{flag}",
                )
                self.assertEqual(self.inventory(), [])
                self.run_command(CMAKE, "--build", str(self.root / "build"))
                self.assert_built(False)
                self.run_command(
                    CMAKE, "--build", str(self.root / "build"), "--target", "unit_tests"
                )
                self.assert_built(True)
                for stamp in (self.root / "build").glob("*.built"):
                    stamp.unlink()

    def test_invalid_configuration(self):
        for setting in (
            "UNIT_TEST_SEED=0x10",
            "UNIT_TEST_SEED=-1",
            "UNIT_TEST_TIMEOUT=0",
        ):
            with self.subTest(setting=setting):
                self.configure(f"-D{setting}", success=False)


if __name__ == "__main__":
    unittest.main()
