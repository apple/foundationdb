#
# subspace_impl.py
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

# FoundationDB Python API

import fdb.tuple


class Subspace (object):

    def __init__(self, prefixTuple=tuple(), rawPrefix=b''):
        self.rawPrefix = fdb.tuple.pack(prefixTuple, prefix=rawPrefix)

    def __repr__(self):
        return 'Subspace(rawPrefix=' + repr(self.rawPrefix) + ')'

    def __getitem__(self, name):
        return Subspace((name,), self.rawPrefix)

    def key(self):
        return self.rawPrefix

    def pack(self, t=tuple()):
        return fdb.tuple.pack(t, prefix=self.rawPrefix)

    def pack_with_versionstamp(self, t=tuple()):
        return fdb.tuple.pack_with_versionstamp(t, prefix=self.rawPrefix)

    def unpack(self, key):
        if not self.contains(key):
            raise ValueError('Cannot unpack key that is not in subspace.')

        return fdb.tuple.unpack(key, prefix_len=len(self.rawPrefix))

    def range(self, t=tuple()):
        p = fdb.tuple.range(t)
        return slice(self.rawPrefix + p.start, self.rawPrefix + p.stop)

    def contains(self, key):
        return key.startswith(self.rawPrefix)

    def as_foundationdb_key(self):
        return self.rawPrefix

    def subspace(self, tuple):
        return Subspace(tuple, self.rawPrefix)
