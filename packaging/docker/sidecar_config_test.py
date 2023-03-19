#!/usr/bin/env python3

# sidecar_config_test.py
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

import unittest

from sidecar import Config

class TestSidecarConfig(unittest.TestCase):
    def test_filter_label_1(self):
        LABEL_NAME = 'topology.kubernetes.io/zone'
        labels = {
            'failure-domain.beta.kubernetes.io/zone': 'fdl',
            'topology.kubernetes.io/zone': 'tl'
        }
        got = Config.filter_label(Config, labels, LABEL_NAME)
        want = labels['topology.kubernetes.io/zone']
        self.assertEqual(got, want)

    def test_filter_label_2(self):
        LABEL_NAME = 'failure-domain.beta.kubernetes.io/zone'
        labels = {
            'failure-domain.beta.kubernetes.io/zone': 'fdl'
        }
        got = Config.filter_label(Config, labels, LABEL_NAME)
        want = labels['failure-domain.beta.kubernetes.io/zone']
        self.assertEqual(got, want)

    def test_filter_label3(self):
        LABEL_NAME = 'failure-domain.beta.kubernetes.io/zone'
        labels = {}
        want = ValueError("K8s node labels cannot be empty")
        try:
            got = Config.filter_label(Config, labels, LABEL_NAME)
        except ValueError as e:
            got = e
        self.assertEqual(got, want)

    def test_filter_label4(self):
        LABEL_NAME = 'test_host'
        labels = None
        want = ValueError("K8s node labels cannot be empty")
        try:
            got = Config.filter_label(Config, labels, LABEL_NAME)
        except ValueError as e:
            got = e
        self.assertEqual(got, want)

    def test_filter_label5(self):
        LABEL_NAME = 'label 3'
        labels = {
            "label 1": "1",
            "label 2": "2"
        }
        want = ValueError(f"K8s node label {LABEL_NAME} not found")
        try:
            got = Config.filter_label(Config, labels, LABEL_NAME)
        except ValueError as e:
            got = e
        self.assertEqual(got, want)


if __name__ == '__main__':
    unittest.main()
