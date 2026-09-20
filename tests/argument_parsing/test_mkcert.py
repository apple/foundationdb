#!/usr/bin/env python3
#
# test_mkcert.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class MkcertTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)

    def run_mkcert(self, *args):
        return subprocess.run(
            [str(self.binary), *args],
            cwd=self.directory,
            capture_output=True,
            text=True,
            timeout=20,
        )

    def test_zero_client_chain_clears_existing_credentials(self):
        result = self.run_mkcert(
            "--server-chain-length", "1", "--client-chain-length", "1"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for side in ("server", "client"):
            for suffix in ("cert", "key", "ca"):
                self.assertGreater(
                    (self.directory / f"{side}_{suffix}.pem").stat().st_size, 0
                )

        result = self.run_mkcert(
            "--server-chain-length", "1", "--client-chain-length", "0"
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for suffix in ("cert", "key", "ca"):
            self.assertEqual(
                (self.directory / f"client_{suffix}.pem").read_bytes(), b""
            )
            self.assertGreater(
                (self.directory / f"server_{suffix}.pem").stat().st_size, 0
            )

    def test_zero_server_chain_is_rejected(self):
        result = self.run_mkcert("--server-chain-length", "0")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Certificate chain length must be positive", result.stderr)
        self.assertEqual(list(self.directory.iterdir()), [])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test mkcert certificate chain options."
    )
    parser.add_argument("binary", type=Path, help="Path to the mkcert executable")
    args = parser.parse_args()
    MkcertTest.binary = args.binary.resolve()
    unittest.main(argv=[sys.argv[0]])
