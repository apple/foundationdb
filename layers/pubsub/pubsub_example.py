#
# pubsub_example.py
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

###################
# PubSub Example  #
###################

# This example generates a simple topology with specified numbers of feeds and
# inboxes. Inboxes are randomly subscribed to feeds. Each feed and inbox is then
# run in its own thread. Feeds post a specified number of messages, waiting a
# random interval between messages. Each inbox is polled for messages received,
# terminating when no messages are received for a wait limit.

import random
import threading
import time

import fdb

from pubsub import PubSub

fdb.api_version(22)
db = fdb.open()

ps = PubSub(db)
ps.clear_all_messages()


# Create the specified numbers of feeds and inboxes. Subscribe each inbox to a
# randomly selected subset of feeds.
def setup_topology(feeds, inboxes):
    feed_map = {f: ps.create_feed('Alice ' + str(f)) for f in range(feeds)}
    inbox_map = {}
    for i in range(inboxes):
        inbox_map[i] = ps.create_inbox('Bob ' + str(i))
        for f in random.sample(xrange(feeds), random.randint(1, feeds)):
            ps.create_subscription(inbox_map[i], feed_map[f])
    return feed_map, inbox_map


# Post a fixed number of messages, waiting a random interval under 1 sec
# between each message
def feed_driver(feed, messages):
    for i in range(messages):
        ps.post_message(feed, 'Message {} from {}'.format(i, feed.get_name()))
        time.sleep(random.random())


def get_and_print_inbox_messages(inbox, limit=10):
    print "\nMessages to {}:".format(inbox.get_name())
    for m in ps.get_inbox_messages(inbox, limit):
        print " ->", m


# Poll the inbox every 0.1 sec, getting and printing messages received,
# until no messages have been received for 1.1 sec
def inbox_driver(inbox):
    wait_limit = 1.1
    wait_inc = 0.1
    waited = 0.0
    changed = False
    latest = None
    while True:
        get_and_print_inbox_messages(inbox)
        changed = (latest != inbox.latest_message)
        latest = inbox.latest_message
        if not changed and waited > wait_limit:
            break
        waited += wait_inc
        time.sleep(wait_inc)


# Generate and run a thread for each feed and each inbox.
def run_threads(feed_map, inbox_map, messages):
    feed_threads = [threading.Thread(target=feed_driver, args=(feed_map[id], messages))
                    for id in feed_map]
    inbox_threads = [threading.Thread(target=inbox_driver, args=(inbox_map[id],))
                     for id in inbox_map]
    for f in feed_threads:
        f.start()
    for i in inbox_threads:
        i.start()
    for f in feed_threads:
        f.join()
    for i in inbox_threads:
        i.join()


def sample_pubsub(feeds, inboxes, messages):
    feed_map, inbox_map = setup_topology(feeds, inboxes)
    run_threads(feed_map, inbox_map, messages)


if __name__ == "__main__":
    sample_pubsub(3, 3, 3)
