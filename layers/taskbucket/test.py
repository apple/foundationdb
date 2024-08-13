#!/usr/bin/python
#
# test.py
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


import sys
import os
sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]
sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'layers')]

import fdb
import taskbucket
import time
import sys
fdb.api_version(200)

from taskbucket import Subspace, TaskTimedOutException

taskDispatcher = taskbucket.TaskDispatcher()
testSubspace = Subspace((), "backup-agent")
taskBucket = taskbucket.TaskBucket(testSubspace["tasks"])
futureBucket = taskbucket.FutureBucket(testSubspace["futures"])


@taskDispatcher.taskType
def say_hello(name, done, **task):
    done = futureBucket.unpack(done)

    print "Hello,", name

    @fdb.transactional
    def say_hello_tx(tr):
        done.set(tr)
        taskBucket.finish(tr, task)
    say_hello_tx(db)


@taskDispatcher.taskType
def say_hello_to_everyone(done, **task):
    done = futureBucket.unpack(done)

    @fdb.transactional
    def say_hello_to_everyone_tx(tr):
        futures = []
        for name in range(20):
            name_done = futureBucket.future(tr)
            futures.append(name_done)
            taskBucket.add(tr, taskDispatcher.makeTask(say_hello, name=str(name), done=name_done))
            done.join(tr, *futures)
        taskBucket.finish(tr, task)
    say_hello_to_everyone_tx(db)


@taskDispatcher.taskType
def said_hello(**task):
    print "Said hello to everyone."
    taskBucket.finish(db, task)


if len(sys.argv) == 2:
    clusterFile = sys.argv[1]
db = fdb.open()
del db["":"\xff"]

print "adding tasks"
all_done = futureBucket.future(db)

taskBucket.clear(db)
taskBucket.add(db, taskDispatcher.makeTask(say_hello_to_everyone, done=all_done))
all_done.on_set_add_task(db, taskBucket, taskDispatcher.makeTask(said_hello))

while True:
    try:
        while True:
            if not taskDispatcher.do_one(db, taskBucket):
                if taskBucket.is_empty(db):
                    time.sleep(5)
                else:
                    time.sleep(1)
        break
    except TaskTimedOutException as e:
        print "task timed out"

print "No tasks remain."

if len(sys.argv) == 2:
    print "all_done:", all_done.is_set(db)
