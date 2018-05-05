#
# set.py
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

import fdb
import fdb.tuple

fdb.api_version(16)


def nextStopToNone(gen):
    try:
        return gen.next()
    except StopIteration:
        return None


class FdbSet (object):
    def __init__(self, path):
        self._path = path

    @fdb.transactional
    def length(self, tr):
        setLength = 0
        for k, v in tr[fdb.tuple.range((self._path,))]:
            setLength += 1
        return setLength

    @fdb.transactional
    def iterate(self, tr):
        for k, v in tr[fdb.tuple.range((self._path,))]:
            yield fdb.tuple.unpack(k)[1]

    @fdb.transactional
    def contains(self, tr, x):
        return tr[fdb.tuple.pack((self._path, x))].present()

    @fdb.transactional
    def issubset(self, tr, t):  # s <= t
        for k, v in tr[fdb.tuple.range((self._path,))]:
            if not t.contains(tr, fdb.tuple.unpack(k)[1]):
                return False
        return True

    @fdb.transactional
    def issuperset(self, tr, t):  # s >= t
        return t.issubset(tr, self)

    @fdb.transactional
    def union(self, tr, t):  # s | t
        s_gen = self.iterate(tr)
        t_gen = t.iterate(tr)
        s_key = nextStopToNone(s_gen)
        t_key = nextStopToNone(t_gen)

        while True:
            if t_key == None and s_key == None:
                return
            elif t_key == None or (s_key != None and s_key < t_key):
                yield s_key
                s_key = nextStopToNone(s_gen)
            elif s_key == None or s_key > t_key:
                yield t_key
                t_key = nextStopToNone(t_gen)
            else:
                yield s_key
                s_key = nextStopToNone(s_gen)
                t_key = nextStopToNone(t_gen)

    @fdb.transactional
    def intersection(self, tr, t):  # s & t
        s_key = self.first_greater_or_equal(tr, "")
        t_key = t.first_greater_or_equal(tr, "")

        while True:
            if t_key == None or s_key == None:
                return
            elif s_key < t_key:
                s_key = self.first_greater_or_equal(tr, t_key)
            elif s_key > t_key:
                t_key = t.first_greater_or_equal(tr, s_key)
            else:
                yield s_key
                s_key = self.first_greater_than(tr, s_key)
                t_key = t.first_greater_than(tr, t_key)

    @fdb.transactional
    def difference(self, tr, t):  # s - t
        s_gen = self.iterate(tr)
        s_key = nextStopToNone(s_gen)
        t_key = t.first_greater_or_equal(tr, "")

        while True:
            if s_key == None:
                return
            elif t_key == None or s_key < t_key:
                yield s_key
                s_key = nextStopToNone(s_gen)
            elif s_key > t_key:
                t_key = t.first_greater_or_equal(tr, s_key)
            else:
                s_key = nextStopToNone(s_gen)

    @fdb.transactional
    def symmetric_difference(self, tr, t):  # s ^ t
        s_gen = self.iterate(tr)
        t_gen = t.iterate(tr)
        s_key = nextStopToNone(s_gen)
        t_key = nextStopToNone(t_gen)

        while True:
            if t_key == None and s_key == None:
                return
            elif t_key == None or (s_key != None and s_key < t_key):
                yield s_key
                s_key = nextStopToNone(s_gen)
            elif s_key == None or s_key > t_key:
                yield t_key
                t_key = nextStopToNone(t_gen)
            else:
                s_key = nextStopToNone(s_gen)
                t_key = nextStopToNone(t_gen)

    @fdb.transactional
    def update(self, tr, t):  # s |= t T
        for k in t.iterate(tr):
            self.add(tr, k)

    @fdb.transactional
    def intersection_update(self, tr, t):  # s &= t
        lastValue = fdb.tuple.pack((self._path,))
        for k in self.intersection(tr, t):
            if k != lastValue:
                del tr[lastValue + '\x00':fdb.tuple.pack((self._path, k))]
            lastValue = fdb.tuple.pack((self._path, k))
        del tr[lastValue + '\x00':fdb.tuple.pack((self._path + chr(0),))]

    @fdb.transactional
    def difference_update(self, tr, t):  # s -= t
        for k in self.intersection(tr, t):
            del tr[fdb.tuple.pack((self._path, k))]

    @fdb.transactional
    def symmetric_difference_update(self, tr, t):  # s ^ t
        s_gen = self.iterate(tr)
        t_gen = t.iterate(tr)
        s_key = nextStopToNone(s_gen)
        t_key = nextStopToNone(t_gen)

        while True:
            if t_key == None and s_key == None:
                return
            elif t_key == None or (s_key != None and s_key < t_key):
                s_key = nextStopToNone(s_gen)
            elif s_key == None or s_key > t_key:
                self.add(tr, t_key)
                t_key = nextStopToNone(t_gen)
            else:
                remove_key = s_key
                s_key = nextStopToNone(s_gen)
                t_key = nextStopToNone(t_gen)
                self.remove(tr, remove_key)

    @fdb.transactional
    def add(self, tr, x):
        tr[fdb.tuple.pack((self._path, x))] = ""

    @fdb.transactional
    def remove(self, tr, x):
        if tr[fdb.tuple.pack((self._path, x))] == None:
            raise KeyError
        del tr[fdb.tuple.pack((self._path, x))]

    @fdb.transactional
    def discard(self, tr, x):
        del tr[fdb.tuple.pack((self._path, x))]

    @fdb.transactional
    def pop(self, tr):
        key = tr.get_key(fdb.KeySelector.first_greater_or_equal(self._path))
        if self._keyInRange(key):
            del tr[key]
            return fdb.tuple.unpack(key)[1]
        raise KeyError

    @fdb.transactional
    def clear(self, tr):
        tr.clear_range_startswith(self._path)

    @fdb.transactional
    def first_greater_than(self, tr, x):
        key = tr.get_key(fdb.KeySelector.first_greater_than(fdb.tuple.pack((self._path, x))))
        if self._keyInRange(key):
            return fdb.tuple.unpack(key)[1]
        return None

    @fdb.transactional
    def first_greater_or_equal(self, tr, x):
        key = tr.get_key(fdb.KeySelector.first_greater_or_equal(fdb.tuple.pack((self._path, x))))
        if self._keyInRange(key):
            return fdb.tuple.unpack(key)[1]
        return None

    def _keyInRange(self, key):
        return key < fdb.tuple.pack((self._path + chr(0),))


def test(db):
    print "starting set test"
    tr = db.create_transaction()

    del tr[:]

    a = FdbSet("a")
    a.add(tr, "apple")
    a.add(tr, "banana")
    a.add(tr, "orange")

    b = FdbSet("b")
    b.add(tr, "banana")
    b.add(tr, "grape")
    b.add(tr, "strawberry")

    c = FdbSet("c")
    c.add(tr, "grape")

    print "set a:"
    for k in a.iterate(tr):
        print k

    print "set b:"
    for k in b.iterate(tr):
        print k

    print "set c:"
    for k in c.iterate(tr):
        print k

    print "b contains strawberry: {0}".format(b.contains(tr, "strawberry"))
    print "remove strawberry from b"

    b.remove(tr, "strawberry")

    print "b contains strawberry: {0}".format(b.contains(tr, "strawberry"))

    try:
        b.remove(tr, "strawberry")
        print "survived second remove of strawberry"
    except KeyError:
        print "failed second remove of strawberry"

    print "insert strawberry into b"

    b.add(tr, "strawberry")

    print "b contains strawberry: {0}".format(b.contains(tr, "strawberry"))

    print "discard strawberry from b"

    b.discard(tr, "strawberry")

    print "b contains strawberry: {0}".format(b.contains(tr, "strawberry"))

    b.discard(tr, "strawberry")

    print "b length: {0}".format(b.length(tr))

    print "c issubset a: {0}".format(c.issubset(tr, a))

    print "c issubset b: {0}".format(c.issubset(tr, b))

    print "a union b:"
    for k in a.union(tr, b):
        print k

    print "a intersection b:"
    for k in a.intersection(tr, b):
        print k

    print "a difference b:"
    for k in a.difference(tr, b):
        print k

    print "b difference a:"
    for k in b.difference(tr, a):
        print k

    print "a symmetric_difference b:"
    for k in a.symmetric_difference(tr, b):
        print k

    print "a update c:"
    a.update(tr, c)
    for k in a.iterate(tr):
        print k

    print "b intersection_update a:"
    b.intersection_update(tr, a)
    for k in b.iterate(tr):
        print k

    print "a difference_update c"
    a.difference_update(tr, c)
    for k in a.iterate(tr):
        print k

    print "b symmetric_difference_update a"
    b.symmetric_difference_update(tr, a)
    for k in b.iterate(tr):
        print k

    print "popping items from a"
    try:
        while True:
            print "popped {0}".format(a.pop(tr))
    except KeyError:
        print "finished popping"

    print "a length: {0}".format(a.length(tr))

    print "clearing b"

    b.clear(tr)

    print "b length: {0}".format(b.length(tr))


db = fdb.open()
test(db)
