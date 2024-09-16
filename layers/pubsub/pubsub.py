#
# pubsub.py
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

"""FoundationDB PubSub Layer.

Provides the PubSub class for message passing according to the publish-subscribe
pattern. PubSub methods supports operations on:

     - Feeds, which publish messages
     - Inboxes, which subscribe to feeds and receive messages from them
     - Messages

The layer allows management of feeds and inboxes as well as message delivery. An
inbox can subscribe to any number of feeds. The inbox will then receive all
messages posted by each feed to which it subscribes. Subscriptions are
retroactive, so messages posted prior to subscription will become available once
a subscription is established.

"""

import simpledoc

feed_messages = simpledoc.OrderedIndex("messages.?", "fromfeed")

feeds = simpledoc.root.feeds
inboxes = simpledoc.root.inboxes
messages = simpledoc.root.messages


#####################################
# Transactions for SimpleDoc Access #
#####################################

@simpledoc.transactional
def _create_feed(name):
    feed = feeds[name]
    feed.set_value(name)
    return feed


@simpledoc.transactional
def _create_inbox(name):
    inbox = inboxes[name]
    inbox.set_value(name)
    return inbox


@simpledoc.transactional
def _create_feed_and_inbox(name):
    feed = _create_feed(name)
    inbox = _create_inbox(name)
    return feed, inbox


# When creating a subscription, initialize the state of the feed as dirty in
# relation to the inbox as if there have been prior posts by the feed. There may
# in fact have been such posts, but this is the correct initial state regardless.
@simpledoc.transactional
def _create_subscription(inbox, feed):
    inbox.subs[feed.get_name()] = ""
    inbox.dirtyfeeds[feed.get_name()] = "1"
    return True


@simpledoc.transactional
def _post_message(feed, contents):
    message = messages.prepend()
    message.set_value(contents)
    message.fromfeed = feed.get_name()
    # Mark the feed as dirty in relation to each watching inbox, setting the
    # inbox to copy the feed the next time it gets messages
    for inbox in feed.watchinginboxes.get_children():
        inboxes[inbox.get_name()].dirtyfeeds[feed.get_name()] = "1"
    # Clear all watching inboxes so the feed will not need to touch a subscribed
    # inbox upon subsequent posts until the inbox has gotten its messages
    feed.watchinginboxes.clear_all()


# Print without other side-effects.
@simpledoc.transactional
def _list_messages(inbox):
    print "Messages in {}'s inbox:".format(inbox.get_value())
    for feed in inbox.subs.get_children():
        print " from {}:".format(feeds[feed.get_name()].get_value())
        for message in feed_messages.find_all(feed.get_name()):
            print "   ", message.get_value()


# Read-only operation without side-effects.
@simpledoc.transactional
def _get_feed_messages(feed, limit):
    message_list = []
    counter = 0
    for message in feed_messages.find_all(feed.get_name()):
        if counter == limit:
            break
        message_list.append(message.get_value())
        counter += 1
    return message_list


# Read-only operation without side-effects.
@simpledoc.transactional
def _get_inbox_subscriptions(inbox, limit):
    subscriptions = []
    for message in inbox.subs.get_children():
        subscriptions.append(message.get_name())
    return subscriptions


# For each of an inbox's dirty feeds, copy the feed's new messages to the inbox
# and mark the inbox as watching the feed. Then unmark the feeds as dirty.
@simpledoc.transactional
def _copy_dirty_feeds(inbox):
    changed = False
    latest_id = inbox.latest_message.get_value()
    for feed in inbox.dirtyfeeds.get_children():
        for message in feed_messages.find_all(feed.get_name()):
            if latest_id != None and message.get_name() >= latest_id:
                break
            changed = True
            inbox.messages[message.get_name()] = feed.get_name()
        feeds[feed.get_name()].watchinginboxes[inbox.get_name()] = "1"
    inbox.dirtyfeeds.clear_all()
    return changed


# Copy messages from an inbox's dirty feeds and update state accordingly. Return
# the most recent messages up to limit.
@simpledoc.transactional
def _get_inbox_messages(inbox, limit):
    inbox_changed = _copy_dirty_feeds(inbox)
    message_ids = []
    for message in inbox.messages.get_children():
        if len(message_ids) >= limit:
            break
        message_ids.append(message.get_name())
    if inbox_changed and len(message_ids) > 0:
        inbox.latest_message = message_ids[0]
    return [messages[mid].get_value() for mid in message_ids]


@simpledoc.transactional
def _clear_all_messages():
    simpledoc.root.clear_all()


@simpledoc.transactional
def _print_feed_stats(feed):
    count = len(list(feed_messages.find_all(feed.get_name())))
    print "{} messages in feed {}".format(count, feed.get_name())


# Pretty print the entire PubSub database from SimpleDoc
@simpledoc.transactional
def _print_pubsub():
    print simpledoc.root.get_json(pretty=True)


################
# PubSub Class #
################

class PubSub(object):
    def __init__(self, db):
        self.db = db

    def create_feed(self, name):
        return _create_feed(self.db, name)

    def create_inbox(self, name):
        return _create_inbox(self.db, name)

    def create_inbox_and_feed(self, name):
        return _create_feed_and_inbox(self.db, name)

    def get_feed_by_name(self, name):
        return feeds[name]

    def get_inbox_by_name(self, name):
        return inboxes[name]

    def create_subscription(self, feed, inbox):
        return _create_subscription(self.db, feed, inbox)

    def post_message(self, feed, contents):
        return _post_message(self.db, feed, contents)

    def list_inbox_messages(self, inbox):
        return _list_messages(self.db, inbox)

    def get_feed_messages(self, feed, limit=10):
        return _get_feed_messages(self.db, feed, limit)

    def get_inbox_subscriptions(self, inbox, limit=10):
        return _get_inbox_subscriptions(self.db, inbox, limit)

    def get_inbox_messages(self, inbox, limit=10):
        return _get_inbox_messages(self.db, inbox, limit)

    def clear_all_messages(self):
        _clear_all_messages(self.db)

    def print_feed_stats(self, feed):
        _print_feed_stats(self.db, feed)

    def print_pubsub(self):
        _print_pubsub(self.db)


###################
# PubSub Example  #
###################

# This example generates a simple topology with specified numbers of feeds and
# inboxes. Inboxes are randomly subscribed to feeds. Each feed and inbox is then
# run in its own thread. Feeds post a specified number of messages, waiting a
# random interval between messages. Each inbox is polled for messages received,
# terminating when no messages are received for a wait limit.


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
    import random
    import threading
    import time
    import fdb

    fdb.api_version(22)
    db = fdb.open()

    ps = PubSub(db)
    ps.clear_all_messages()
    sample_pubsub(3, 3, 3)
