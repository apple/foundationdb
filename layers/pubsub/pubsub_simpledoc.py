#
# pubsub_simpledoc.py
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

import fdb
import simpledoc

feed_messages = simpledoc.OrderedIndex("messages.?", "fromfeed")
feed_watching_inboxes = simpledoc.OrderedIndex("messages.?", "fromfeed")

feeds = simpledoc.root.feeds
inboxes = simpledoc.root.inboxes
messages = simpledoc.root.messages


@simpledoc.transactional
def _create_feed_internal(metadata):
    feed = feeds[metadata]
    feed.set_value(metadata)
    return feed


@simpledoc.transactional
def _create_inbox_internal(metadata):
    inbox = inboxes[metadata]
    inbox.set_value(metadata)
    return inbox


@simpledoc.transactional
def _create_feed_and_inbox_internal(metadata):
    _create_feed_internal(metadata)
    _create_inbox_internal(metadata)
    return True


@simpledoc.transactional
def _create_subscription_internal(feed, inbox):
    inbox.subs[feed.get_name()] = ""
    inbox.dirtyfeeds[feed.get_name()] = "1"
    return True


@simpledoc.transactional
def _post_message_internal(feed, message_id, contents):
    message = messages.prepend()
    message.set_value(contents)
    message.fromfeed = feed.get_name()

    # mark all the "watching inboxes" as dirty, so they know to re-copy
    for inbox in feed.watchinginboxes.get_children():
        inboxes[inbox.get_name()].dirtyfeeds[feed.get_name()] = "1"
    feed.watchinginboxes.clear_all()


@simpledoc.transactional
def _list_messages_internal(inbox):
    print "messages in %s's inbox:" % inbox.get_value()
    for feed in inbox.subs.get_children():
        print " from %s:" % feeds[feed.get_name()].get_value()
        for message in feed_messages.find_all(feed.get_name()):
            print "   ", message.get_value()


@simpledoc.transactional
def _get_feed_messages_internal(feed, limit):
    message_list = []
    counter = 0
    for message in feed_messages.find_all(feed.get_name()):
        if counter == limit:
            break
        message_list.append(message.get_value())
        counter += 1
    return message_list


@simpledoc.transactional
def _copy_dirty_feeds(inbox):
    changed = False
    latest_id = inbox.latest_message.get_value()
    # print "latest message is", latest_id
    for feed in inbox.dirtyfeeds.get_children():
        # print "working on dirty feed", feed.get_name()
        for message in feed_messages.find_all(feed.get_name()):
            # print "found message", message.get_name()
            if latest_id != None and message.get_name() >= latest_id:
                break
            changed = True
            inbox.messages[message.get_name()] = feed.get_name()
            # print "copied message", message.get_name()

        # now that we have copied, mark this inbox as watching the feed
        feeds[feed.get_name()].watchinginboxes[inbox.get_name()] = "1"

    inbox.dirtyfeeds.clear_all()
    return changed


@simpledoc.transactional
def _get_inbox_subscriptions_internal(inbox, limit):
    subscriptions = []

    for message in inbox.subs.get_children():
        subscriptions.append(message.get_name())

    return subscriptions


@simpledoc.transactional
def _get_inbox_messages_internal(inbox, limit):
    inbox_changed = _copy_dirty_feeds(inbox)

    message_ids = []
    for message in inbox.messages.get_children():
        if len(message_ids) >= limit:
            break
        message_ids.append(message.get_name())

    # As simpledoc returns children in order, I do not think this is needed (the above depends on it NOT being needed)
    # message_ids.sort()
    if inbox_changed and len(message_ids) > 0:
        inbox.latest_message = message_ids[0]
    message_ids = message_ids[:max(limit, len(message_ids))]

    return [messages[mid].get_value() for mid in message_ids]


@simpledoc.transactional
def _clear_all_messages():
    simpledoc.root.clear_all()


@simpledoc.transactional
def _print_internals(feed_or_inbox=None):
    if feed_or_inbox is None:
        print simpledoc.root.get_json(False)
    else:
        print feed_or_inbox.get_json(False)


class PubSub(object):
    def __init__(self, db):
        self.db = db

    def create_feed(self, metadata):
        return _create_feed_internal(self.db, metadata)

    def create_inbox(self, metadata):
        return _create_inbox_internal(self.db, metadata)

    def create_inbox_and_feed(self, metadata):
        _create_feed_and_inbox_internal(self.db, metadata)

    def get_feed_by_name(self, metadata):
        return feeds[metadata]

    def get_inbox_by_name(self, metadata):
        return inboxes[metadata]

    def create_subscription(self, feed, inbox):
        return _create_subscription_internal(self.db, feed, inbox)

    def post_message(self, feed, contents):
        message_id = os.urandom(8)
        return _post_message_internal(self.db, feed, message_id, contents)

    def list_inbox_messages(self, inbox):
        return _list_messages_internal(self.db, inbox)

    def get_feed_messages(self, feed, limit=10):
        return _get_feed_messages_internal(self.db, feed, limit)

    def get_inbox_subscriptions(self, inbox, limit=10):
        return _get_inbox_subscriptions_internal(self.db, inbox, limit)

    def get_inbox_messages(self, inbox, limit=10):
        return _get_inbox_messages_internal(self.db, inbox, limit)

    def print_feed_stats(self, feed):
        _print_internals(self.db, feed_or_inbox=feed)

    def print_internals(self):
        _print_internals(self.db)
