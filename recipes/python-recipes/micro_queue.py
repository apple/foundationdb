#
# micro_queue.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
db = fdb.open()


@fdb.transactional
def clear_subspace(tr, subspace):
    tr.clear_range_startswith(subspace.key())


queue = fdb.Subspace(('Q',))
clear_subspace(db, queue)


@fdb.transactional
def dequeue(tr):
    item = first_item(tr)
    if item is None:
        return None
    del tr[item.key]
    return item.value


@fdb.transactional
def enqueue(tr, value):
    tr[queue[last_index(tr) + 1][os.urandom(20)]] = value


@fdb.transactional
def last_index(tr):
    r = queue.range()
    for key, _ in tr.snapshot.get_range(r.start, r.stop, limit=1, reverse=True):
        return queue.unpack(key)[0]
    return 0


@fdb.transactional
def first_item(tr):
    r = queue.range()
    for kv in tr.get_range(r.start, r.stop, limit=1):
        return kv


def smoke_test():
    enqueue(db, 'a')
    enqueue(db, 'b')
    enqueue(db, 'c')
    enqueue(db, 'd')
    enqueue(db, 'e')
    print "dequeue"
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)
    enqueue(db, 'a1')
    enqueue(db, 'a2')
    enqueue(db, 'a3')
    enqueue(db, 'a4')
    enqueue(db, 'b')
    enqueue(db, 'c')
    enqueue(db, 'd')
    enqueue(db, 'e')
    print "dequeue"
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)
    print dequeue(db)


if __name__ == "__main__":
    db = fdb.open()
    clear_subspace(db, queue)
    smoke_test()
