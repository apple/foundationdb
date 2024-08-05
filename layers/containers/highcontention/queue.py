#
# queue.py
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


class Subspace (object):
    def __init__(self, prefixTuple, rawPrefix=""):
        self.rawPrefix = rawPrefix + fdb.tuple.pack(prefixTuple)

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
    def __init__(self, subspace, highContention=True):
        self.subspace = subspace
        self.highContention = highContention

        self._conflictedPop = self.subspace['pop']
        self._conflictedItem = self.subspace['conflict']
        self._queueItem = self.subspace['item']

    @fdb.transactional
    def clear(self, tr):
        """Remove all items from the queue."""
        del tr[self.subspace.range()]

    @fdb.transactional
    def push(self, tr, value):
        """Push a single item onto the queue."""
        index = self._getNextIndex(tr.snapshot, self._queueItem)
        self._pushAt(tr, self._encodeValue(value), index)

    def pop(self, db):
        """Pop the next item from the queue. Cannot be composed with other functions in a single transaction."""

        if self.highContention:
            result = self._popHighContention(db)
        else:
            result = self._popSimple(db)

        if result is None:
            return result

        return self._decodeValue(result)

    @fdb.transactional
    def empty(self, tr):
        """Test whether the queue is empty."""
        return self._getFirstItem(tr) is None

    @fdb.transactional
    def peek(self, tr):
        """Get the value of the next item in the queue without popping it."""
        firstItem = self._getFirstItem(tr)
        if firstItem is None:
            return None
        else:
            return self._decodeValue(firstItem.value)

    # Private functions

    def _conflictedItemKey(self, subKey):
        return self._conflictedItem.pack((subKey,))

    def _randID(self):
        return os.urandom(20)  # this relies on good random data from the OS to avoid collisions

    def _encodeValue(self, value):
        return fdb.tuple.pack((value,))

    def _decodeValue(self, value):
        return fdb.tuple.unpack(value)[0]

    # Items are pushed on the queue at an (index, randomID) pair. Items pushed at the
    # same time will have the same index, and so their ordering will be random.
    # This makes pushes fast and usually conflict free (unless the queue becomes empty
    # during the push)
    def _pushAt(self, tr, value, index):
        key = self._queueItem.pack((index, self._randID()))
        read = tr[key]
        tr[key] = value

    def _getNextIndex(self, tr, subspace):
        lastKey = tr.get_key(fdb.KeySelector.last_less_than(subspace.range().stop))
        if lastKey < subspace.range().start:
            return 0

        return subspace.unpack(lastKey)[0] + 1

    def _getFirstItem(self, tr):
        r = self._queueItem.range()
        for kv in tr.get_range(r.start, r.stop, 1):
            return kv

        return None

    # This implementation of pop does not attempt to avoid conflicts. If many clients
    # are trying to pop simultaneously, only one will be able to succeed at a time.
    @fdb.transactional
    def _popSimple(self, tr):

        firstItem = self._getFirstItem(tr)
        if firstItem is None:
            return None

        del tr[firstItem.key]
        return firstItem.value

    @fdb.transactional
    def _addConflictedPop(self, tr, forced=False):
        index = self._getNextIndex(tr.snapshot, self._conflictedPop)

        if index == 0 and not forced:
            return None

        waitKey = self._conflictedPop.pack((index, self._randID()))
        read = tr[waitKey]
        tr[waitKey] = ''
        return waitKey

    def _getWaitingPops(self, tr, numPops):
        r = self._conflictedPop.range()
        return tr.get_range(r.start, r.stop, numPops)

    def _getItems(self, tr, numItems):
        r = self._queueItem.range()
        return tr.get_range(r.start, r.stop, numItems)

    def _fulfillConflictedPops(self, db):
        numPops = 100

        tr = db.create_transaction()
        pops = self._getWaitingPops(tr.snapshot, numPops)
        items = self._getItems(tr.snapshot, numPops)

        i = 0
        pops = list(pops)
        for pop, (k, v) in zip(pops, items):
            key = self._conflictedPop.unpack(pop.key)
            storageKey = self._conflictedItemKey(key[1])
            tr[storageKey] = v
            read = tr[k]
            read = tr[pop.key]
            del tr[pop.key]
            del tr[k]
            i = i + 1

        for pop in pops[i:]:
            read = tr[pop.key]
            del tr[pop.key]

        tr.commit().wait()
        return len(pops) < numPops

    # This implementation of pop attempts to avoid collisions by registering
    # itself in a semi-ordered set of poppers if it doesn't initially succeed.
    # It then enters a polling loop where it attempts to fulfill outstanding pops
    # and then checks to see if it has been fulfilled.
    def _popHighContention(self, db):

        backoff = 0.01

        tr = db.create_transaction()

        try:
            # Check if there are other people waiting to be popped. If so, we
            # cannot pop before them.
            waitKey = self._addConflictedPop(tr)
            if waitKey is None:
                # No one else was waiting to be popped
                item = self._popSimple(tr)
                tr.commit().wait()
                return item
            else:
                tr.commit().wait()

        except fdb.FDBError as e:
            # If we didn't succeed, then register our pop request
            waitKey = self._addConflictedPop(db, True)

        # The result of the pop will be stored at this key once it has been fulfilled
        resultKey = self._conflictedItemKey(self._conflictedPop.unpack(waitKey)[1])

        tr.reset()

        # Attempt to fulfill outstanding pops and then poll the database
        # checking if we have been fulfilled
        while 1:
            try:
                while not self._fulfillConflictedPops(db):
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
                value = tr[waitKey]
                result = tr[resultKey]

                # If waitKey is present, then we have not been fulfilled
                if value.present():
                    time.sleep(backoff)
                    backoff = min(1, backoff * 2)
                    continue

                if not result.present():
                    return None

                del tr[resultKey]
                tr.commit().wait()
                return result

            except fdb.FDBError as e:
                tr.on_error(e.code).wait()


##################
# Internal tests #
##################


def queue_test(db):
    queue = Queue(Subspace(('queue_test',)), False)
    print 'Clear Queue'
    queue.clear(db)
    print 'Empty? %s' % queue.empty(db)
    print 'Push 10, 8, 6'
    queue.push(db, 10)
    queue.push(db, 8)
    queue.push(db, 6)
    print 'Empty? %s' % queue.empty(db)
    print 'Pop item: %d' % queue.pop(db)
    print 'Next item: %d' % queue.peek(db)
    print 'Pop item: %d' % queue.pop(db)
    print 'Pop item: %d' % queue.pop(db)
    print 'Empty? %s' % queue.empty(db)
    print 'Push 5'
    queue.push(db, 5)
    print 'Clear Queue'
    queue.clear(db)
    print 'Empty? %s' % queue.empty(db)


######################
# Queue sample usage #
######################


# caution: modifies the database!
def queue_single_client_example(db):
    queue = Queue(Subspace(('queue_example',)), False)
    queue.clear(db)

    for i in range(10):
        queue.push(db, i)

    for i in range(10):
        print queue.pop(db)


def push_thread(queue, db, id, num):
    for i in range(num):
        queue.push(db, '%d.%d' % (id, i))


def pop_thread(queue, db, id, num):
    for i in range(num):
        queue.pop(db)

    print 'Finished pop thread %d' % id


import threading


def queue_multi_client_example(db):
    descriptions = ["simple queue", "high contention queue"]

    for highContention in range(2):
        print 'Starting %s test' % descriptions[highContention]
        queue = Queue(Subspace(('queue_example',)), highContention > 0)
        queue.clear(db)

        pushThreads = [threading.Thread(target=push_thread, args=(queue, db, i, 100)) for i in range(10)]
        popThreads = [threading.Thread(target=pop_thread, args=(queue, db, i, 100)) for i in range(10)]

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
        print 'Finished %s in %f seconds' % (descriptions[highContention], end - start)


def queue_example(db):
    print "Running single client example:"
    queue_single_client_example(db)

    print "\nRunning multi-client example:"
    queue_multi_client_example(db)


# caution: modifies the database!
if __name__ == '__main__':
    db = fdb.open()

    queue_example(db)
    # queue_test(db)
