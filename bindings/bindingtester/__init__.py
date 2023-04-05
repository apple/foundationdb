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

import math
import sys
import os

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]

import util

FDB_API_VERSION = 720

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

    def key(self, specification):
        return self.key_tuple[specification.key_start_index:]

    @staticmethod
    def elements_equal(el1, el2):
        if type(el1) != type(el2):
            return False

        if isinstance(el1, tuple):
            return Result.tuples_match(el1, el2)

        if isinstance(el1, float) and math.isnan(el1):
            return math.isnan(el2)

        return el1 == el2

    @staticmethod
    def tuples_match(t1, t2):
        if len(t1) != len(t2):
            return False

        return all([Result.elements_equal(x,y) for x,y in zip(t1, t2)])        

    def matches_key(self, rhs, specification):
        if not isinstance(rhs, Result):
            return False

        return Result.tuples_match(self.key(specification), rhs.key(specification))

    def matches(self, rhs, specification):
        if not self.matches_key(rhs, specification):
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
