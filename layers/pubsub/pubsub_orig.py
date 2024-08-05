#
# pubsub_orig.py
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

'''
Created on May 14, 2012

* General FDB data model of Pub/Sub model *
     Feeds, Inboxes, Messages

 Message - publisher (feed), data

 Inbox - message cache, dirty list, list of subscribed-to feeds

 Feed - messages, list of inboxes to notify

 Structures for scalability: global list of dispatching feeds, feeds to consider
  dirty for all operation


* Basic Processes: post message, list messages *

 Post (in one transaction):
  1. Add message to list of message in feed history
  2. For each Inbox of list of watching inboxes
     a. remove Inbox from list of watchers
     b. mark self as 'dirty' in the Inbox's dirty list

 List recent message in Inbox (in one transaction), up to n messages:
  1. For each dirty Feed, update cache with latest message
  2. Triage list by getting latest m messages from top m feeds
      note: these messages can be cached in memory on the server, and on paging
      operations reused, so long as step 1 is repeated, IF there is no message deletion.


* Assumptions: *
  1. Subscriptions are "retroactive".  If a subscription is in place, the messages
      from that feed will start to appear in the listing of that inboxes messages
      and appear in historical lists as well.  This could lead to odd behavior
      if paging through the contents of an inbox while a new subscription was added

'''

import os
import sys

sys.path[:0] = [os.path.join(os.path.dirname(__file__), '..', '..', 'bindings', 'python')]
import fdb
import struct


def key_for_feed(feed):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed))


def key_for_feed_subscriber_count(feed):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'subCount')


def key_for_feed_message_count(feed):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'messCount')


def key_for_feed_subscriber(feed, inbox):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'subs', struct.pack('>Q', inbox))


def prefix_for_feed_subscribers(feed):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'subs')


def key_for_feed_watcher(feed, inbox):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'watchers', struct.pack('>Q', inbox))


def key_for_feed_message(feed, message):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'message', struct.pack('>Q', message))


def prefix_for_feed_messages(feed):
    return fdb.tuple_to_key('f', struct.pack('>Q', feed), 'message')


def key_for_inbox(inbox):
    return fdb.tuple_to_key('i', struct.pack('>Q', inbox))


def key_for_inbox_subscription_count(inbox):
    return fdb.tuple_to_key('i', struct.pack('>Q', inbox), 'subCount')


def key_for_inbox_subscription(inbox, feed):
    return fdb.tuple_to_key('i', struct.pack('>Q', inbox), 'subs', struct.pack('>Q', feed))


def prefix_for_inbox_subscriptions(inbox):
    return fdb.tuple_to_key('i', struct.pack('>Q', inbox), 'subs')


def key_for_inbox_stale_feed(inbox, feed):
    return fdb.tuple_to_key('i', struct.pack('>Q', inbox), 'stale', struct.pack('>Q', feed))


def key_for_message(message):
    return fdb.tuple_to_key('m', struct.pack('>Q', message))


@fdb.transactional
def _create_feed_internal(tr, feed, metadata):
    key = key_for_feed(feed)
    if tr[key] != None:
        print 'Feed', feed, 'already exists'
        return feed

    tr[key] = metadata
    tr[key_for_feed_subscriber_count(feed)] = struct.pack('>Q', 0)
    tr[key_for_feed_message_count(feed)] = struct.pack('>Q', 0)

    return feed


@fdb.transactional
def _create_inbox_internal(tr, inbox, metadata):
    key = key_for_inbox(inbox)
    if tr[key] != None:
        return inbox

    tr[key] = metadata
    tr[key_for_inbox_subscription_count(inbox)] = struct.pack('>Q', 0)

    return inbox


@fdb.transactional
def _create_subscription_internal(tr, feed, inbox):
    key = key_for_inbox_subscription(inbox, feed)
    if tr[key] != None:
        return True  # This subscription exists

    # print 'Feed, inbox:', feed, ",", inbox

    # Retrieve the current counts for the feed and inbox, this serves as an existence "proof"
    f_count = tr[key_for_feed_subscriber_count(feed)]
    i_count = tr[key_for_inbox_subscription_count(inbox)]
    # print f_count, 'and', i_count
    if f_count == None or i_count == None:
        print 'There is not a feed or inbox'
        return False  # Either the inbox or the feed do not exist

    # Update the subscriptions of the inbox
    tr[key] = ''
    tr[key_for_feed_subscriber_count(feed)] = \
        struct.pack('>Q', struct.unpack('>Q', f_count + '')[0] + 1)

    # Update the subscribers of the feed
    tr[key_for_feed_subscriber(feed, inbox)] = ''
    tr[key_for_inbox_subscription_count(inbox)] = \
        struct.pack('>Q', struct.unpack('>Q', i_count + '')[0] + 1)

    # Add inbox as watcher of feed.
    tr[key_for_feed_watcher(feed, inbox)] = ''

    return True


@fdb.transactional
def _post_message_internal(tr, feed, contents):
    if tr[key_for_feed(feed)] == None:
        return False  # this feed does not exist!

    # Get globally latest message, set our ID to that less one
    zero_key = key_for_message(0)
    last_key = key_for_message(sys.maxint)
    first_key = tr.get_key(fdb.KeySelector.first_greater_than(zero_key))
    last_message = tr[last_key]
    if last_message == None:
        print 'There are not other messages in the database'
        message_id = sys.maxint
    else:
        print fdb.key_to_tuple(first_key)
        message_id = struct.unpack('>Q', fdb.key_to_tuple(first_key)[1])[0] - 1
    print "MessageID", message_id

    tr[key_for_message(message_id)] = contents
    tr[key_for_feed_message(feed, message_id)] = ''
    f_count = tr[key_for_feed_message_count(feed)]
    tr[key_for_feed_message_count(feed)] = \
        struct.pack('>Q', struct.unpack('>Q', f_count + '')[0] + 1)

    # update the watchers on the feed to mark those inboxes as stale
    # prefix = fdb.tuple_to_key(key_for_feed(feed), 'watchers')
    # for k,v in tr.get_range_startswith(prefix):
    #     stale_inbox = fdb.key_to_tuple(k)[3]
    #     tr[key_for_inbox_stale_feed(stale_inbox, feed)] = ''
    #     del tr[k]

    return True


@fdb.transactional
def _list_messages_internal(tr, inbox):
    messages = []
    if tr[key_for_inbox(inbox)] == None:
        return messages  # this inbox does not exist!

    print 'Messages in %s''s inbox' % tr[key_for_inbox(inbox)]
    prefix = prefix_for_inbox_subscriptions(inbox)
    for k, _ in tr.get_range_startswith(prefix):
        # print "inbox sub:", fdb.key_to_tuple(k)
        feed = struct.unpack('>Q', fdb.key_to_tuple(k)[3])[0]
        # print "messages from feed:", feed
        print ' from %s:' % tr[key_for_feed(feed)]
        feed_prefix = prefix_for_feed_messages(feed)
        for key, _ in tr.get_range_startswith(feed_prefix):
            # print "feed message:", fdb.key_to_tuple(key)
            message_id = struct.unpack('>Q', fdb.key_to_tuple(key)[3])[0]
            message = tr[key_for_message(message_id)]
            print "  ", message
            messages.append(message)

    return messages


@fdb.transactional
def _print_internal(tr, feed):
    countKey = key_for_feed_message_count(feed)
    f_count = tr[countKey]
    print 'Messages in feed', feed, ':', struct.unpack('>Q', f_count + '')[0]


class PubSub(object):
    def __init__(self, db):
        self.db = db

    def create_feed(self, metadata):
        feed_id = struct.unpack('>Q', os.urandom(8))[0]
        return _create_feed_internal(self.db, feed_id, metadata)

    def create_inbox(self, metadata):
        inbox_id = struct.unpack('>Q', os.urandom(8))[0]
        return _create_inbox_internal(self.db, inbox_id, metadata)

    def create_subscription(self, feed, inbox):
        return _create_subscription_internal(self.db, feed, inbox)

    def post_message(self, feed, contents):
        return _post_message_internal(self.db, feed, contents)

    def list_inbox_messages(self, inbox):
        return _list_messages_internal(self.db, inbox)

    def list_feed_messages(self, feed):
        pass

    def print_feed_stats(self, feed):
        _print_internal(self.db, feed)
