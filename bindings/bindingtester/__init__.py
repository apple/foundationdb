#
# __init__.py
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

import sys
import os

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]

import util

FDB_API_VERSION = 520

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'simple': {
            'format': '%(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'NOTSET',
            'class': 'logging.StreamHandler',
            'stream': sys.stdout,
            'formatter': 'simple'
        }
    },
    'loggers': {
        'foundationdb.bindingtester': {
            'level': 'INFO',
            'handlers': ['console']
        }
    }
}


class Result:
    def __init__(self, subspace, key, values):
        self.subspace_tuple = util.subspace_to_tuple(subspace)
        self.key_tuple = subspace.unpack(key)
        self.values = values

    def matches(self, rhs, specification):
        if not isinstance(rhs, Result):
            return False

        left_key = self.key_tuple[specification.key_start_index:]
        right_key = self.key_tuple[specification.key_start_index:]

        if len(left_key) != len(right_key) or left_key != right_key:
            return False

        for value in self.values:
            for rValue in rhs.values:
                if value == rValue:
                    return True

        return False

    def matches_global_error_filter(self, specification):
        return any([specification.matches_global_error_filter(v) for v in self.values])

    # A non-unique sequence of numbers used to align results from different testers
    def sequence_num(self, specification):
        if specification.ordering_index is not None:
            return self.key_tuple[specification.ordering_index]

        return None

    def __str__(self):
        if len(self.values) == 1:
            value_str = repr(self.values[0])
        else:
            value_str = repr(self.values)

        return '%s = %s' % (repr(self.subspace_tuple + self.key_tuple), value_str)
