#!/usr/bin/python -u
#
# remotesend.py
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
import sys

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]
import fdb
import argparse
import random
import gevent

from gevent import monkey
monkey.patch_thread()

from pubsub_bigdoc import PubSub

parser = argparse.ArgumentParser()
parser.add_argument("--zkAddr")
parser.add_argument("--database")
parser.add_argument("--totalUsers", type=int)
parser.add_argument("--messages", type=int)
parser.add_argument("--threads", type=int)
args = parser.parse_args()

# zkAddr = '10.0.3.1:2181/bbc'
# database = 'TwitDB'
# users = 1000
# followers = 10

db = fdb.open(args.zkAddr, args.database, event_model="gevent")

ps = PubSub(db)
name = os.uname()[1]

print 'sending messages',


def message_client():
    gevent.sleep(random.random())
    messages_sent = 0
    while messages_sent < args.messages / args.threads:
        user = random.randint(0, args.totalUsers)
        if random.random() < 0.1:
            ps.post_message(ps.get_feed_by_name('%09d' % user), 'Message %d from %s' % (i, name))
            messages_sent += 1
        else:
            ps.get_inbox_messages(ps.get_inbox_by_name('%09d' % user), 10)


jobs = [gevent.spawn(message_client) for i in range(0, args.threads)]
gevent.joinall(jobs)

print 'done'
