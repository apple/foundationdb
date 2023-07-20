#
# micro_multi.py
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

import struct

import fdb
fdb.api_version(300)
db = fdb.open()


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


multi = fdb.Subspace(('M',))
clear_subspace(db, multi)


# Multimaps with multiset values


@fdb.transactional
def multi_add(tr, index, value):
    tr.add(multi[index][value], struct.pack('<q', 1))


@fdb.transactional
def multi_subtract(tr, index, value):
    v = tr[multi[index][value]]
    if v.present() and struct.unpack('<q', str(v))[0] > 1:
        tr.add(multi[index][value], struct.pack('<q', -1))
    else:
        del tr[multi[index][value]]


@fdb.transactional
def multi_get(tr, index):
    return [multi.unpack(k)[1] for k, v in tr[multi[index].range()]]


@fdb.transactional
def multi_get_counts(tr, index):
    return {multi.unpack(k)[1]: struct.unpack('<q', v)[0]
            for k, v in tr[multi[index].range()]}


@fdb.transactional
def multi_is_element(tr, index, value):
    return tr[multi[index][value]].present()


import time

N = 10000


@fdb.transactional
def time_atomic_add(tr):
    for i in xrange(N):
        multi_add(db, 'foo', 'bar')


@fdb.transactional
def time_atomic_subtract(tr):
    start = time.time()
    for i in xrange(N):
        multi_subtract(tr, 'foo', 'bar')
    end = time.time()
    print "{} seconds for atomic subtract".format(end - start)


if __name__ == '__main__':
    time_atomic_add(db)
    time_atomic_subtract(db)
