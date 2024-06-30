#
# known_testers.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

from bindingtester import FDB_API_VERSION

MAX_API_VERSION = FDB_API_VERSION
COMMON_TYPES = [
    "null",
    "bytes",
    "string",
    "int",
    "uuid",
    "bool",
    "float",
    "double",
    "tuple",
]
ALL_TYPES = COMMON_TYPES + ["versionstamp"]


class Tester:
    def __init__(
        self,
        name,
        cmd,
        max_int_bits=64,
        min_api_version=0,
        max_api_version=MAX_API_VERSION,
        threads_enabled=True,
        types=COMMON_TYPES,
        directory_snapshot_ops_enabled=True,
        tenants_enabled=False,
    ):
        self.name = name
        self.cmd = cmd
        self.max_int_bits = max_int_bits
        self.min_api_version = min_api_version
        self.max_api_version = max_api_version
        self.threads_enabled = threads_enabled
        self.types = types
        self.directory_snapshot_ops_enabled = directory_snapshot_ops_enabled
        self.tenants_enabled = tenants_enabled

    def supports_api_version(self, api_version):
        return (
            api_version >= self.min_api_version and api_version <= self.max_api_version
        )

    @classmethod
    def get_test(cls, test_name_or_args):
        if test_name_or_args in testers:
            return testers[test_name_or_args]
        else:
            return Tester(test_name_or_args.split(" ")[0], test_name_or_args)


def _absolute_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", path)


_java_cmd = "java -ea -cp %s:%s com.apple.foundationdb.test." % (
    _absolute_path("java/foundationdb-client.jar"),
    _absolute_path("java/foundationdb-tests.jar"),
)

# We could set min_api_version lower on some of these if the testers were updated to support them
testers = {
    "python": Tester(
        "python",
        "python3 " + _absolute_path("python/tests/tester.py"),
        2040,
        23,
        MAX_API_VERSION,
        types=ALL_TYPES,
        tenants_enabled=True,
    ),
    "ruby": Tester(
        "ruby", _absolute_path("ruby/tests/tester.rb"), 2040, 23, MAX_API_VERSION
    ),
    "java": Tester(
        "java",
        _java_cmd + "StackTester",
        2040,
        510,
        MAX_API_VERSION,
        types=ALL_TYPES,
        tenants_enabled=True,
    ),
    "java_async": Tester(
        "java",
        _java_cmd + "AsyncStackTester",
        2040,
        510,
        MAX_API_VERSION,
        types=ALL_TYPES,
        tenants_enabled=True,
    ),
    "go": Tester(
        "go",
        _absolute_path("go/build/bin/_stacktester"),
        2040,
        200,
        MAX_API_VERSION,
        types=ALL_TYPES,
    ),
    "flow": Tester(
        "flow",
        _absolute_path("flow/bin/fdb_flow_tester"),
        63,
        500,
        MAX_API_VERSION,
        directory_snapshot_ops_enabled=False,
    ),
}
