#
# __init__.py
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

# FoundationDB TaskBucket layer

import random
import uuid
import time
import struct
import fdb
import fdb.tuple
fdb.api_version(200)

# TODO: Make this fdb.tuple.subspace() or similar?


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


def random_key():
    return uuid.uuid4().bytes


def _pack_value(v):
    if hasattr(v, 'pack'):
        return v.pack()
    else:
        return v


class TaskTimedOutException(Exception):
    pass


class TaskBucket (object):
    """A TaskBucket represents an unordered collection of tasks, stored
    in a database and available to any number of clients.  A program may
    use one or many TaskBuckets.  TaskBucket represents a task as a
    key/value dictionary.  See TaskDispatcher for an easy way to define
    tasks as functions."""

    def __init__(self, subspace, system_access=False):
        self.prefix = subspace
        self.active = self.prefix["ac"]
        self.available = self.prefix["av"]
        self.timeouts = self.prefix["to"]
        self.timeout = 30000
        self.system_access = system_access

    @fdb.transactional
    def clear(self, tr):
        """Removes all tasks, whether or not locked, from the bucket."""
        if self.system_access:
            tr.options.set_access_system_keys()
        del tr[self.prefix.range(())]

    @fdb.transactional
    def add(self, tr, taskDict):
        """Adds the task specified by taskDict to the bucket for any participant to do."""
        if self.system_access:
            tr.options.set_access_system_keys()
        assert taskDict
        key = random_key()
        for k, v in taskDict.items():
            tr[self.available.pack((key, k))] = _pack_value(v)
        taskDict["__task_key"] = key
        return key

    @fdb.transactional
    def addIdle(self, tr):
        """Adds an idle task for any participant to remove."""
        if self.system_access:
            tr.options.set_access_system_keys()
        key = random_key()
        tr[self.available.pack((key, "type"))] = ""
        return key

    @fdb.transactional
    def get_one(self, tr):
        """Gets a single task from the bucket, locks it so that only the
        caller will work on it for a while, and returns its taskDict.
        If there are no tasks in the bucket, returns None."""
        if self.system_access:
            tr.options.set_access_system_keys()
        k = tr.snapshot.get_key(fdb.KeySelector.last_less_or_equal(self.available.pack((random_key(),))))
        if not k or k < self.available.pack(("",)):
            k = tr.snapshot.get_key(fdb.KeySelector.last_less_or_equal(self.available.pack((chr(255) * 16,))))
            if not k or k < self.available.pack(("",)):
                if self.check_timeouts(tr):
                    return self.get_one(tr)
                return None
        key = self.available.unpack(k)[0]
        avail = self.available[key]
        timeout = tr.get_read_version().wait() + long(self.timeout * (0.9 + 0.2 * random.random()))

        taskDict = {}
        for k, v in tr[avail.range(())]:
            tk, = avail.unpack(k)
            taskDict[tk] = v

            if tk != "type" or v != "":
                tr[self.timeouts.pack((timeout, key, tk))] = v
        del tr[avail.range(())]
        tr[self.active.key()] = random_key()

        taskDict["__task_key"] = key
        taskDict["__task_timeout"] = timeout
        return taskDict

    @fdb.transactional
    def is_empty(self, tr):
        if self.system_access:
            tr.options.set_read_system_keys()
        k = tr.get_key(fdb.KeySelector.last_less_or_equal(self.available.pack((chr(255) * 16,))))
        if k and k >= self.available.pack(("",)):
            return False
        return not bool(next(iter(tr[self.timeouts.range()]), False))

    @fdb.transactional
    def is_busy(self, tr):
        if self.system_access:
            tr.options.set_read_system_keys()
        k = tr.get_key(fdb.KeySelector.last_less_or_equal(self.available.pack((chr(255) * 16,))))
        return k and k >= self.available.pack(("",))

    @fdb.transactional
    def finish(self, tr, taskDict):
        """taskDict is a task previously locked by get_one.  Removes it permanently
        from the bucket.  If the task has already timed out, raises TaskTimedOutException."""
        if self.system_access:
            tr.options.set_access_system_keys()
        rng = self.timeouts.range((taskDict["__task_timeout"], taskDict["__task_key"]))
        if next(iter(tr[rng]), False):
            del tr[rng]
        else:
            raise TaskTimedOutException()

    @fdb.transactional
    def is_finished(self, tr, taskDict):
        # print "checking if the task was finished at version: {0}".format(tr.get_read_version().wait())
        if self.system_access:
            tr.options.set_read_system_keys()
        rng = self.timeouts.range((taskDict["__task_timeout"], taskDict["__task_key"]))
        return not bool(next(iter(tr[rng]), False))

    def check_active(self, db):
        @fdb.transactional
        def get_starting_value(tr):
            if self.system_access:
                tr.options.set_read_system_keys()
            if not self.is_busy(tr):
                self.addIdle(tr)
            return tr[self.active.key()]

        starting_value = get_starting_value(db)

        @fdb.transactional
        def get_active_key(tr):
            if self.system_access:
                tr.options.set_read_system_keys()
            if tr[self.active.key()] != starting_value:
                return True
            return False

        for i in range(10):
            time.sleep(0.5)
            if get_active_key(db):
                return True
        return False

    @fdb.transactional
    def extend(self, tr, taskDict):
        """Extends the lock on a task previously locked by get_one for another self.timeout seconds.
        If the task has already timed out, raises TaskTimedOutException."""
        pass

    def check_timeouts(self, tr):
        """Looks for tasks that have timed out and returns them to be available tasks.  Returns True
        iff any tasks were affected."""
        end = tr.get_read_version().wait()
        rng = slice(self.timeouts.range((0,)).start, self.timeouts.range((end,)).stop)
        anyTimeouts = False
        for k, v in tr.get_range(rng.start, rng.stop, streaming_mode=fdb.StreamingMode.want_all):
            timeout, taskKey, param = self.timeouts.unpack(k)
            anyTimeouts = True
            tr.set(self.available.pack((taskKey, param)), v)
        del tr[rng]

        return anyTimeouts


class TaskDispatcher (object):
    def __init__(self):
        self.taskTypes = {}

    def taskType(self, f):
        """Given a function, defines a task type with the same name as the
        function that calls the function.  The function should accept the
        task dictionary as keyword parameters.

        May be used as a decorator."""
        self.taskTypes[f.__name__] = f
        return f

    def makeTask(self, f, **kw):
        """Return a taskDict suitable for TaskBucket.add() based on a function
        which has already been passed to self.taskType() and keyword parameters
        for it."""
        assert self.taskTypes.get(f.__name__) == f
        assert 'type' not in kw
        d = dict(kw)
        d["type"] = f.__name__
        return d

    def dispatch(self, taskDict):
        """Calls the function associated with the type of the taskDict passed
        in, passing the taskDict as keyword arguments.  Does not finish or
        extend the task or otherwise interact with a TaskBucket."""
        if taskDict["type"] != "":
            self.taskTypes[taskDict["type"]](**taskDict)

    def do_one(self, db, taskBucket):
        """Gets one task (if any) from the task bucket, executes it, and finishes it.
        Returns True if a task was executed, False if none was available."""
        task = taskBucket.get_one(db)
        if not task:
            return False
        self.dispatch(task)
        return True


class FutureBucket (object):
    """A factory for Futures, and a location in the database to store them."""

    def __init__(self, subspace, system_access=False):
        self.prefix = subspace
        self.dispatcher = TaskDispatcher()
        self.dispatcher.taskType(self._add_task)
        self.dispatcher.taskType(self._unblock_future)
        self.system_access = system_access

    @fdb.transactional
    def future(self, tr):
        """Returns a new Future which is unset."""
        if self.system_access:
            tr.options.set_access_system_keys()
        f = Future(self)
        f._add_block(tr, "")
        return f

    def unpack(self, packed_future):
        """Returns a Future such that Future.pack()==packed_future."""
        return Future(self, packed_future)

    @fdb.transactional
    def join(self, tr, *futures):
        """Returns a Future which is set when all of the given futures are set."""
        if self.system_access:
            tr.options.set_access_system_keys()
        joined = Future(self)
        joined._join(tr, futures)
        return joined

    def _add_task(self, tr, taskType, taskBucket, **task):
        bucket = TaskBucket(Subspace((), rawPrefix=taskBucket), system_access=self.system_access)
        task["type"] = taskType
        bucket.add(tr, task)

    def _unblock_future(self, tr, future, blockid, **task):
        if self.system_access:
            tr.options.set_access_system_keys()
        future = self.unpack(future)
        del tr[future.prefix["bl"][blockid].key()]
        if future.is_set(tr):
            future.perform_all_actions(tr)


class Future (object):
    """Represents a state which will become true ("set") at some point, and a set
    of actions to perform as soon as the state is true.  The state can change
    only once."""

    def __init__(self, bucket, key=None):
        """Not for direct use.  Call FutureBucket.future() instead."""
        if key is None:
            key = random_key()
        self.key = key
        self.bucket = bucket
        self.prefix = bucket.prefix[self.key]
        self.dispatcher = bucket.dispatcher
        self.system_access = bucket.system_access

    def pack(self):
        return self.key

    @fdb.transactional
    def is_set(self, tr):
        if self.system_access:
            tr.options.set_read_system_keys()
        return not any(tr[self.prefix["bl"].range(())])

    @fdb.transactional
    def on_set_add_task(self, tr, taskBucket, taskDict):
        """When the Future is set, the given task will be added to the given taskBucket."""
        td = dict(taskDict)
        td["taskBucket"] = taskBucket.prefix.key()
        td["taskType"] = td["type"]
        td["type"] = "_add_task"
        self.on_set(tr, td)

    @fdb.transactional
    def on_set(self, tr, taskDict):
        """When the Future is set, the given task will be executed immediately
        as part of the same transaction.  Consider using onSetAddTask to defer
        the execution of the task instead."""
        if self.system_access:
            tr.options.set_access_system_keys()
        if self.is_set(tr):
            self.perform_action(tr, taskDict)
        else:
            cb_key = random_key()
            for k, v in taskDict.items():
                tr[self.prefix["cb"][cb_key][k].key()] = _pack_value(v)

    @fdb.transactional
    def set(self, tr):
        """Sets the Future immediately if it has not already been set.  Any
        actions that are to take place when it is set take place."""
        if self.system_access:
            tr.options.set_access_system_keys()
        # if self.is_set(tr): return
        del tr[self.prefix.range(("bl",))]  # Remove all blocks
        self.perform_all_actions(tr)

    @fdb.transactional
    def join(self, tr, *futures):
        """This Future will be set when all of the given futures have been set.
        If this is called more than once on a Future, it is cumulative - the
        Future will be set when all of the Futures passed to any .join() are set."""
        if self.system_access:
            tr.options.set_access_system_keys()
        assert futures
        if self.is_set(tr):
            return
        del tr[self.prefix["bl"][""].key()]
        self._join(tr, futures)

    @fdb.transactional
    def joined_future(self, tr):
        """Returns a new unset future which this future is join()ed to."""
        if self.system_access:
            tr.options.set_access_system_keys()
        f = self.bucket.future(tr)
        self.join(tr, f)
        return f

    def _join(self, tr, futures):
        ids = [random_key() for f in futures]
        for blockid in ids:
            self._add_block(tr, blockid)
        for f, blockid in zip(futures, ids):
            f.on_set(tr, self.dispatcher.makeTask(self.bucket._unblock_future, future=self.pack(), blockid=blockid))

    def _add_block(self, tr, blockid):
        tr[self.prefix["bl"][blockid].key()] = ""

    def perform_all_actions(self, tr):
        cb = self.prefix["cb"]
        callbacks = list(tr[cb.range()])
        del tr[cb.range()]  # Remove all callbacks
        # Now actually perform the callbacks
        taskDict = {}
        taskKey = None
        for k, v in callbacks:
            cb_key, k = cb.unpack(k)
            if cb_key != taskKey:
                self.perform_action(tr, taskDict)
                taskDict = {}
                taskKey = cb_key
            taskDict[k] = v
        self.perform_action(tr, taskDict)

    def perform_action(self, tr, taskDict):
        if taskDict:
            taskDict["tr"] = tr
            self.dispatcher.dispatch(taskDict)
