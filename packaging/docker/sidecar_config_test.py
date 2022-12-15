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
    def test_get_topology_label_1(self):
        HOSTNAME = 'test_host'
        labels = {
            'failure-domain.beta.kubernetes.io/zone' : 'fdl',
            'topology.kubernetes.io/zone' : 'tl'
        }
        got = Config.get_topology_label(Config,labels,HOSTNAME)
        want = 'tl'
        self.assertEqual(got,want)

    def test_get_topology_label_2(self):
        HOSTNAME = 'test_host'
        labels = {
            'failure-domain.beta.kubernetes.io/zone' : 'fdl'
        }
        got = Config.get_topology_label(Config,labels,HOSTNAME)
        want = 'fdl'
        self.assertEqual(got,want)

    def test_get_topology_label_3(self):
        HOSTNAME = 'test_host'
        labels = {}
        got = Config.get_topology_label(Config,labels,HOSTNAME)
        want = HOSTNAME
        self.assertEqual(got,want)

    def test_get_topology_label_4(self):
        HOSTNAME = 'test_host'
        labels = None
        got = Config.get_topology_label(Config,labels,HOSTNAME)
        want = HOSTNAME
        self.assertEqual(got,want)

    def test_get_topology_label_5(self):
        HOSTNAME = 'test_host'
        labels = {
            "label 1" : "1",
            "label 2" : "2"
        }
        got = Config.get_topology_label(Config,labels,HOSTNAME)
        want = HOSTNAME
        self.assertEqual(got,want)


if __name__ == '__main__':
    unittest.main()
