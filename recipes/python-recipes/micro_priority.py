#
# micro_priority.py
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
import fdb
fdb.api_version(300)

pq = fdb.Subspace(('P',))


@fdb.transactional
def push(tr, value, priority):
    tr[pq[priority][_next_count(tr, priority)][os.urandom(20)]] = value


@fdb.transactional
def _next_count(tr, priority):
    r = pq[priority].range()
    for key, value in tr.snapshot.get_range(r.start, r.stop, limit=1, reverse=True):
        return pq[priority].unpack(key)[0] + 1
    return 0


@fdb.transactional
def pop(tr, max=False):
    r = pq.range()
    for item in tr.get_range(r.start, r.stop, limit=1, reverse=max):
        del tr[item.key]
        return item.value


@fdb.transactional
def peek(tr, max=False):
    r = pq.range()
    for item in tr.get_range(r.start, r.stop, limit=1, reverse=max):
        return item.value


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


def smoke_test():
    print "Peek none:", peek(db)
    push(db, 'a', 1)
    push(db, 'b', 5)
    push(db, 'c', 2)
    push(db, 'd', 4)
    push(db, 'e', 3)
    print "peek in min order"
    print peek(db)
    print peek(db)
    print "pop in min order"
    print pop(db)
    print pop(db)
    print pop(db)
    print pop(db)
    print pop(db)
    print "Peek none:", peek(db, max=True)
    push(db, 'a1', 1)
    push(db, 'a2', 1)
    push(db, 'a3', 1)
    push(db, 'a4', 1)
    push(db, 'b', 5)
    push(db, 'c', 2)
    push(db, 'd', 4)
    push(db, 'e', 3)
    print "peek in max order"
    print peek(db, max=True)
    print peek(db, max=True)
    print "pop in max order"
    print pop(db, max=True)
    print pop(db, max=True)
    print pop(db, max=True)
    print pop(db, max=True)
    print pop(db, max=True)
    print pop(db, max=True)


if __name__ == "__main__":
    db = fdb.open()
    clear_subspace(db, pq)
    smoke_test()
