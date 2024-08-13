#!/usr/bin/python -i
#
# ps_tutorial.py
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


"""
This document is a brief tutorial of the publish/subscription layer.

The key concepts are:
 - Feeds: A feed is a source of messages.
 - Inbox: An inbox receives messages posted to feeds
 - Subscriptions: A subscription connects an inbox to a feed

Simply put, an inbox receives receives all messages posted to the feeds it subscribes to.

The pub/sub layer allows management of feeds, inboxes, and subscriptions, as well as the
actual delivery of messages.
"""

# Prerequisites
import os
import sys
import fdb
from pubsub_bigdoc import PubSub
fdb.api_version(14)

# Start by opening the default FoundationDB database (see documentation)
db = fdb.open()

# Clear all data from the database, so that this simple test starts fresh each time
# del db[:]

# Create a "new" PubSub layer that will use the opened database
ps = PubSub(db)

# Create a single feed (for Alice)
feed_a = ps.create_feed('alice_feed')

# Create an inbox for Bob
inbox_b = ps.create_inbox('bob_inbox')

# Subscribe bob to alice's feed
assert ps.create_subscription(feed_a, inbox_b)

# Post a couple of messages to alice's feed
ps.post_message(feed_a, 'hi from alice')
ps.post_message(feed_a, 'hi from alice again!')

# Print all the messages in bob's inbox (which subscribes to alice's feed)
print "Messages to Bob:"
for m in ps.get_inbox_messages(inbox_b):
    print " ->", m
