#!/usr/bin/python -i
#
# ps_test.py
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
import sys

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]
import fdb
from pubsub_bigdoc import PubSub

fdb.api_version(14)
db = fdb.open('/home/bbc/fdb.cluster', 'DB')

del db[:]

ps = PubSub(db)

feed_a = ps.create_feed('alice')
ps.print_feed_stats(feed_a)

feed_b = ps.create_feed('bob')
feed_x = ps.create_feed('bieber')

inbox_a = ps.create_inbox('alice')
inbox_b = ps.create_inbox('bob')
inbox_x = ps.create_inbox('bieber')

assert ps.create_subscription(feed_b, inbox_a)
assert ps.create_subscription(feed_a, inbox_b)
assert ps.create_subscription(feed_x, inbox_a)
assert ps.create_subscription(feed_x, inbox_b)

ps.post_message(feed_a, 'hi from alice')
ps.post_message(feed_b, 'hi from bob')
ps.post_message(feed_x, 'hi from your favorite singer')
ps.post_message(feed_a, 'I''m off to the store!')

ps.print_feed_stats(feed_a)
ps.print_feed_stats(feed_b)
ps.print_feed_stats(feed_x)

ps.list_inbox_messages(inbox_a)
ps.list_inbox_messages(inbox_b)
ps.list_inbox_messages(inbox_x)

print "Messages by bieber:"
for m in ps.get_feed_messages(feed_x):
    print " ->", m

print "Messages to Bob:"
for m in ps.get_inbox_messages(inbox_b):
    print " ->", m

print "Messages to Bob:"
for m in ps.get_inbox_messages(inbox_b):
    print " ->", m
