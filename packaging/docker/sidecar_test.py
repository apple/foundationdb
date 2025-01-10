#!/usr/bin/env python3

# sidecar_test.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2018-2022 Apple Inc. and the FoundationDB project authors
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

import os
import shutil
import socket
import tempfile
import unittest
from functools import partial
from http.server import HTTPServer
from threading import Thread
from unittest.mock import MagicMock

import requests

from sidecar import SidecarHandler


# This test suite starts a real server with a mocked configuration and will do some requests against it.
class TestSidecar(unittest.TestCase):
    def setUp(self):
        super(TestSidecar, self).setUp()
        self.get_free_port()
        self.server_url = f"http://localhost:{self.test_server_port}"
        self.mock_config = MagicMock()
        # We don't want to use TLS for the local tests for now.
        self.mock_config.enable_tls = False
        self.mock_config.output_dir = tempfile.mkdtemp()

        handler = partial(
            SidecarHandler,
            self.mock_config,
        )
        self.mock_server = HTTPServer(("localhost", self.test_server_port), handler)

        # Start running mock server in a separate thread.
        # Daemon threads automatically shut down when the main process exits.
        self.mock_server_thread = Thread(target=self.mock_server.serve_forever)
        self.mock_server_thread.setDaemon(True)
        self.mock_server_thread.start()

    def tearDown(self):
        shutil.rmtree(self.mock_config.output_dir)
        super(TestSidecar, self).tearDown()

    # Helper method to get a free port
    def get_free_port(self):
        s = socket.socket(socket.AF_INET, type=socket.SOCK_STREAM)
        s.bind(("localhost", 0))
        __, port = s.getsockname()
        s.close()
        self.test_server_port = port

    def test_get_ready(self):
        r = requests.get(f"{self.server_url}/ready")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text, "OK\n")

    def test_get_substitutions(self):
        expected = {"key": "value"}
        self.mock_config.substitutions = expected
        r = requests.get(f"{self.server_url}/substitutions")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), expected)

    def test_get_check_hash_no_found(self):
        r = requests.get(f"{self.server_url}/check_hash/foobar")
        self.assertEqual(r.status_code, 404)
        self.assertRegex(r.text, "foobar not found")

    def test_get_check_hash(self):
        with open(os.path.join(self.mock_config.output_dir, "foobar"), "w") as f:
            f.write("hello world")
        r = requests.get(f"{self.server_url}/check_hash/foobar")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(
            r.text, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )

    def test_get_check_hash_nested(self):
        test_path = os.path.join(self.mock_config.output_dir, "nested/foobar")
        os.makedirs(os.path.dirname(test_path), exist_ok=True)
        with open(test_path, "w") as f:
            f.write("hello world")
        r = requests.get(f"{self.server_url}/check_hash/nested/foobar")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(
            r.text, "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        )

    def test_get_is_present_no_found(self):
        r = requests.get(f"{self.server_url}/is_present/foobar")
        self.assertEqual(r.status_code, 404)
        self.assertRegex(r.text, "foobar not found")

    def test_get_is_present(self):
        with open(os.path.join(self.mock_config.output_dir, "foobar"), "w") as f:
            f.write("hello world")
        r = requests.get(f"{self.server_url}/is_present/foobar")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text, "OK\n")

    def test_get_is_present_nested(self):
        test_path = os.path.join(self.mock_config.output_dir, "nested/foobar")
        os.makedirs(os.path.dirname(test_path), exist_ok=True)
        with open(test_path, "w") as f:
            f.write("hello world")
        r = requests.get(f"{self.server_url}/is_present/nested/foobar")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.text, "OK\n")

    def test_get_not_found(self):
        r = requests.get(f"{self.server_url}/foobar")
        self.assertEqual(r.status_code, 404)
        self.assertRegex(r.text, "Path /foobar not found")


# TODO(johscheuer): Add test cases for post requests.
# TODO(johscheuer): Add test cases for TLS.
if __name__ == "__main__":
    unittest.main()
