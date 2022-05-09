#
# queue.py
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

"""FoundationDB Queue Layer.

Provides a high-contention Queue() class.

It has two operating modes. The high contention mode (default) is
designed for environments where many clients will be popping the queue
simultaneously. Pop operations in this mode are slower when performed in
isolation, but their performance scales much better with the number of
popping clients.

If high contention mode is off, then no attempt will be made to avoid
transaction conflicts in pop operations. This mode performs well with
only one popping client, but will not scale well to many popping clients.
"""

import time
import os

import fdb
import fdb.tuple

fdb.api_version(22)


###################################
# This defines a Subspace of keys #
###################################


class Subspace(object):
    def __init__(self, prefix_tuple, raw_prefix=""):
        self.rawPrefix = raw_prefix + fdb.tuple.pack(prefix_tuple)

    def __getitem__(self, name):
        return Subspace((name,), self.rawPrefix)

    def key(self):
        return self.rawPrefix

    def pack(self, tuple):
        return self.rawPrefix + fdb.tuple.pack(tuple)

    def unpack(self, key):
        assert key.startswith(self.rawPrefix)
        return fdb.tuple.unpack(key[len(self.rawPrefix):])

    def range(self, tuple=()):
        p = fdb.tuple.range(tuple)
        return slice(self.rawPrefix + p.start, self.rawPrefix + p.stop)


#########
# Queue #
#########


class Queue:
    # Public functions
    def __init__(self, subspace, high_contention=True):
        self.subspace = subspace
        self.highContention = high_contention

        self._conflictedPop = self.subspace["pop"]
        self._conflictedItem = self.subspace["conflict"]
        self._queueItem = self.subspace["item"]

    @fdb.transactional
    def clear(self, tr):
        """Remove all items from the queue."""
        del tr[self.subspace.range()]

    @fdb.transactional
    def push(self, tr, value):
        """Push a single item onto the queue."""
        index = self._get_next_index(tr.snapshot, self._queueItem)
        self._push_at(tr, self._encode_value(value), index)

    def pop(self, db):
        """Pop the next item from the queue. Cannot be composed with other functions in a single transaction."""

        if self.highContention:
            result = self._pop_high_contention(db)
        else:
            result = self._pop_simple(db)

        if result is None:
            return result

        return self._decode_value(result)

    @fdb.transactional
    def empty(self, tr):
        """Test whether the queue is empty."""
        return self._get_first_item(tr) is None

    @fdb.transactional
    def peek(self, tr):
        """Get the value of the next item in the queue without popping it."""
        first_item = self._get_first_item(tr)
        if first_item is None:
            return None
        else:
            return self._decode_value(first_item.value)

    # Private functions

    def _conflicted_item_key(self, sub_key):
        return self._conflictedItem.pack((sub_key,))

    def _rand_id(self):
        return os.urandom(
            20
        )  # this relies on good random data from the OS to avoid collisions

    def _encode_value(self, value):
        return fdb.tuple.pack((value,))

    def _decode_value(self, value):
        return fdb.tuple.unpack(value)[0]

    # Items are pushed on the queue at an (index, randomID) pair. Items pushed at the
    # same time will have the same index, and so their ordering will be random.
    # This makes pushes fast and usually conflict free (unless the queue becomes empty
    # during the push)
    def _push_at(self, tr, value, index):
        key = self._queueItem.pack((index, self._rand_id()))
        tr[key] = value

    def _get_next_index(self, tr, subspace):
        last_key = tr.get_key(fdb.KeySelector.last_less_than(subspace.range().stop))
        if last_key < subspace.range().start:
            return 0

        return subspace.unpack(last_key)[0] + 1

    def _get_first_item(self, tr):
        r = self._queueItem.range()
        for kv in tr.get_range(r.start, r.stop, 1):
            return kv

        return None

    # This implementation of pop does not attempt to avoid conflicts. If many clients
    # are trying to pop simultaneously, only one will be able to succeed at a time.
    @fdb.transactional
    def _pop_simple(self, tr):

        firstItem = self._get_first_item(tr)
        if firstItem is None:
            return None

        del tr[firstItem.key]
        return firstItem.value

    @fdb.transactional
    def _add_conflicted_pop(self, tr, forced=False):
        index = self._get_next_index(tr.snapshot, self._conflictedPop)

        if index == 0 and not forced:
            return None

        wait_key = self._conflictedPop.pack((index, self._rand_id()))
        tr[wait_key] = ""
        return wait_key

    def _get_waiting_pops(self, tr, num_pops):
        r = self._conflictedPop.range()
        return tr.get_range(r.start, r.stop, num_pops)

    def _get_items(self, tr, num_items):
        r = self._queueItem.range()
        return tr.get_range(r.start, r.stop, num_items)

    def _fulfill_conflicted_pops(self, db):
        num_pops = 100

        tr = db.create_transaction()
        pops = self._get_waiting_pops(tr.snapshot, num_pops)
        items = self._get_items(tr.snapshot, num_pops)

        i = 0
        pops = list(pops)
        for pop, (k, v) in zip(pops, items):
            key = self._conflictedPop.unpack(pop.key)
            storage_key = self._conflicted_item_key(key[1])
            tr[storage_key] = v
            del tr[pop.key]
            del tr[k]
            i = i + 1

        for pop in pops[i:]:
            del tr[pop.key]

        tr.commit().wait()
        return len(pops) < num_pops

    # This implementation of pop attempts to avoid collisions by registering
    # itself in a semi-ordered set of poppers if it doesn't initially succeed.
    # It then enters a polling loop where it attempts to fulfill outstanding pops
    # and then checks to see if it has been fulfilled.
    def _pop_high_contention(self, db):

        backoff = 0.01

        tr = db.create_transaction()

        try:
            # Check if there are other people waiting to be popped. If so, we
            # cannot pop before them.
            wait_key = self._add_conflicted_pop(tr)
            if wait_key is None:
                # No one else was waiting to be popped
                item = self._pop_simple(tr)
                tr.commit().wait()
                return item
            else:
                tr.commit().wait()

        except fdb.FDBError:
            # If we didn't succeed, then register our pop request
            wait_key = self._add_conflicted_pop(db, True)

        # The result of the pop will be stored at this key once it has been fulfilled
        result_key = self._conflicted_item_key(self._conflictedPop.unpack(wait_key)[1])

        tr.reset()

        # Attempt to fulfill outstanding pops and then poll the database
        # checking if we have been fulfilled
        while 1:
            try:
                while not self._fulfill_conflicted_pops(db):
                    pass
            except fdb.FDBError as e:
                # If the error is 1020 (not_committed), then there is a good chance
                # that somebody else has managed to fulfill some outstanding pops. In
                # that case, we proceed to check whether our request has been fulfilled.
                # Otherwise, we handle the error in the usual fashion.
                if e.code != 1020:
                    tr.on_error(e.code).wait()
                    continue

            try:
                tr.reset()
                value = tr[wait_key]
                result = tr[result_key]

                # If wait_key is present, then we have not been fulfilled
                if value.present():
                    time.sleep(backoff)
                    backoff = min(1, backoff * 2)
                    continue

                if not result.present():
                    return None

                del tr[result_key]
                tr.commit().wait()
                return result

            except fdb.FDBError as e:
                tr.on_error(e.code).wait()


##################
# Internal tests #
##################


def queue_test(db):
    queue = Queue(Subspace(("queue_test",)), False)
    print("Clear Queue")
    queue.clear(db)
    print("Empty? %s" % queue.empty(db))
    print("Push 10, 8, 6")
    queue.push(db, 10)
    queue.push(db, 8)
    queue.push(db, 6)
    print("Empty? %s" % queue.empty(db))
    print("Pop item: %d" % queue.pop(db))
    print("Next item: %d" % queue.peek(db))
    print("Pop item: %d" % queue.pop(db))
    print("Pop item: %d" % queue.pop(db))
    print("Empty? %s" % queue.empty(db))
    print("Push 5")
    queue.push(db, 5)
    print("Clear Queue")
    queue.clear(db)
    print("Empty? %s" % queue.empty(db))


######################
# Queue sample usage #
######################


# caution: modifies the database!
def queue_single_client_example(db):
    queue = Queue(Subspace(("queue_example",)), False)
    queue.clear(db)

    for i in range(10):
        queue.push(db, i)

    for i in range(10):
        print(queue.pop(db))


def push_thread(queue, db, id, num):
    for i in range(num):
        queue.push(db, "%d.%d" % (id, i))


def pop_thread(queue, db, id, num):
    for i in range(num):
        queue.pop(db)

    print("Finished pop thread %d" % id)


import threading


def queue_multi_client_example(db):
    descriptions = ["simple queue", "high contention queue"]

    for highContention in range(2):
        print("Starting %s test" % descriptions[highContention])
        queue = Queue(Subspace(("queue_example",)), highContention > 0)
        queue.clear(db)

        pushThreads = [
            threading.Thread(target=push_thread, args=(queue, db, i, 100))
            for i in range(10)
        ]
        popThreads = [
            threading.Thread(target=pop_thread, args=(queue, db, i, 100))
            for i in range(10)
        ]

        start = time.time()

        for push in pushThreads:
            push.start()
        for pop in popThreads:
            pop.start()
        for push in pushThreads:
            push.join()
        for pop in popThreads:
            pop.join()

        end = time.time()
        print("Finished %s in %f seconds" % (descriptions[highContention], end - start))


def queue_example(db):
    print("Running single client example:")
    queue_single_client_example(db)

    print("\nRunning multi-client example:")
    queue_multi_client_example(db)


# caution: modifies the database!
if __name__ == "__main__":
    db = fdb.open()

    queue_example(db)
    # queue_test(db)
