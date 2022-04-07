/*
 * pubsub.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cinttypes>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/pubsub.h"
#include "flow/actorcompiler.h" // This must be the last #include.

Value uInt64ToValue(uint64_t v) {
	return StringRef(format("%016llx", v));
}
uint64_t valueToUInt64(const StringRef& v) {
	uint64_t x = 0;
	sscanf(v.toString().c_str(), "%" SCNx64, &x);
	return x;
}

Key keyForInbox(uint64_t inbox) {
	return StringRef(format("i/%016llx", inbox));
}
Key keyForInboxSubscription(uint64_t inbox, uint64_t feed) {
	return StringRef(format("i/%016llx/subs/%016llx", inbox, feed));
}
Key keyForInboxSubscriptionCount(uint64_t inbox) {
	return StringRef(format("i/%016llx/subsCnt", inbox));
}
Key keyForInboxStalePrefix(uint64_t inbox) {
	return StringRef(format("i/%016llx/stale/", inbox));
}
Key keyForInboxStaleFeed(uint64_t inbox, uint64_t feed) {
	return StringRef(format("i/%016llx/stale/%016llx", inbox, feed));
}
Key keyForInboxCacheByIDPrefix(uint64_t inbox) {
	return StringRef(format("i/%016llx/cid/", inbox));
}
Key keyForInboxCacheByID(uint64_t inbox, uint64_t messageId) {
	return StringRef(format("i/%016llx/cid/%016llx", inbox, messageId));
}
Key keyForInboxCacheByFeedPrefix(uint64_t inbox) {
	return StringRef(format("i/%016llx/cf/", inbox));
}
Key keyForInboxCacheByFeed(uint64_t inbox, uint64_t feed) {
	return StringRef(format("i/%016llx/cf/%016llx", inbox, feed));
}

Key keyForFeed(uint64_t feed) {
	return StringRef(format("f/%016llx", feed));
}
Key keyForFeedSubcriber(uint64_t feed, uint64_t inbox) {
	return StringRef(format("f/%016llx/subs/%016llx", feed, inbox));
}
Key keyForFeedSubcriberCount(uint64_t feed) {
	return StringRef(format("f/%016llx/subscCnt", feed));
}
Key keyForFeedMessage(uint64_t feed, uint64_t message) {
	return StringRef(format("f/%016llx/m/%016llx", feed, message));
}
Key keyForFeedMessagePrefix(uint64_t feed) {
	return StringRef(format("f/%016llx/m/", feed));
}
Key keyForFeedMessageCount(uint64_t feed) {
	return StringRef(format("f/%016llx/messCount", feed));
}
// the following should go at some point: change over to range query of count 1 from feed message list
Key keyForFeedLatestMessage(uint64_t feed) {
	return StringRef(format("f/%016llx/latestMessID", feed));
}
Key keyForFeedWatcherPrefix(uint64_t feed) {
	return StringRef(format("f/%016llx/watchers/", feed));
}
Key keyForFeedWatcher(uint64_t feed, uint64_t inbox) {
	return StringRef(format("f/%016llx/watchers/%016llx", feed, inbox));
}

Standalone<StringRef> messagePrefix(LiteralStringRef("m/"));

Key keyForMessage(uint64_t message) {
	return StringRef(format("m/%016llx", message));
}

Key keyForDisptchEntry(uint64_t message) {
	return StringRef(format("d/%016llx", message));
}

PubSub::PubSub(Database _cx) : cx(_cx) {}

ACTOR Future<uint64_t> _createFeed(Database cx, Standalone<StringRef> metadata) {
	state uint64_t id(deterministicRandom()->randomUniqueID().first()); // SOMEDAY: this should be an atomic increment
	TraceEvent("PubSubCreateFeed").detail("Feed", id);
	state Transaction tr(cx);
	loop {
		try {
			state Optional<Value> val = wait(tr.get(keyForFeed(id)));
			while (val.present()) {
				id = id + deterministicRandom()->randomInt(1, 100);
				Optional<Value> v = wait(tr.get(keyForFeed(id)));
				val = v;
			}
			tr.set(keyForFeed(id), metadata);
			tr.set(keyForFeedSubcriberCount(id), uInt64ToValue(0));
			tr.set(keyForFeedMessageCount(id), uInt64ToValue(0));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return id;
}

Future<uint64_t> PubSub::createFeed(Standalone<StringRef> metadata) {
	return _createFeed(cx, metadata);
}

ACTOR Future<uint64_t> _createInbox(Database cx, Standalone<StringRef> metadata) {
	state uint64_t id = deterministicRandom()->randomUniqueID().first();
	TraceEvent("PubSubCreateInbox").detail("Inbox", id);
	state Transaction tr(cx);
	loop {
		try {
			state Optional<Value> val = wait(tr.get(keyForInbox(id)));
			while (val.present()) {
				id += deterministicRandom()->randomInt(1, 100);
				Optional<Value> v = wait(tr.get(keyForFeed(id)));
				val = v;
			}
			tr.set(keyForInbox(id), metadata);
			tr.set(keyForInboxSubscriptionCount(id), uInt64ToValue(0));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return id;
}

Future<uint64_t> PubSub::createInbox(Standalone<StringRef> metadata) {
	return _createInbox(cx, metadata);
}

ACTOR Future<bool> _createSubscription(Database cx, uint64_t feed, uint64_t inbox) {
	state Transaction tr(cx);
	TraceEvent("PubSubCreateSubscription").detail("Feed", feed).detail("Inbox", inbox);
	loop {
		try {
			Optional<Value> subscription = wait(tr.get(keyForInboxSubscription(inbox, feed)));
			if (subscription.present()) {
				// For idempotency, this could exist from a previous transaction from us that succeeded
				return true;
			}
			Optional<Value> inboxVal = wait(tr.get(keyForInbox(inbox)));
			if (!inboxVal.present()) {
				return false;
			}
			Optional<Value> feedVal = wait(tr.get(keyForFeed(feed)));
			if (!feedVal.present()) {
				return false;
			}

			// Update the subscriptions of the inbox
			Optional<Value> subscriptionCountVal = wait(tr.get(keyForInboxSubscriptionCount(inbox)));
			uint64_t subscriptionCount = valueToUInt64(subscriptionCountVal.get()); // throws if count not present
			tr.set(keyForInboxSubscription(inbox, feed), StringRef());
			tr.set(keyForInboxSubscriptionCount(inbox), uInt64ToValue(subscriptionCount + 1));

			// Update the subcribers of the feed
			Optional<Value> subcriberCountVal = wait(tr.get(keyForFeedSubcriberCount(feed)));
			uint64_t subcriberCount = valueToUInt64(subcriberCountVal.get()); // throws if count not present
			tr.set(keyForFeedSubcriber(feed, inbox), StringRef());
			tr.set(keyForFeedSubcriberCount(inbox), uInt64ToValue(subcriberCount + 1));

			// Add inbox as watcher of feed.
			tr.set(keyForFeedWatcher(feed, inbox), StringRef());

			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return true;
}

Future<bool> PubSub::createSubscription(uint64_t feed, uint64_t inbox) {
	return _createSubscription(cx, feed, inbox);
}

// Since we are not relying on "read-your-own-writes", we need to keep track of
//  the highest-numbered inbox that we've cleared from the watchers list and
//  make sure that further requests start after this inbox.
ACTOR Future<Void> updateFeedWatchers(Transaction* tr, uint64_t feed) {
	state StringRef watcherPrefix = keyForFeedWatcherPrefix(feed);
	state uint64_t highestInbox;
	state bool first = true;
	loop {
		// Grab watching inboxes in swaths of 100
		state RangeResult watchingInboxes =
		    wait((*tr).getRange(firstGreaterOrEqual(keyForFeedWatcher(feed, first ? 0 : highestInbox + 1)),
		                        firstGreaterOrEqual(keyForFeedWatcher(feed, UINT64_MAX)),
		                        100)); // REVIEW: does 100 make sense?
		if (!watchingInboxes.size())
			// If there are no watchers, return.
			return Void();
		first = false;
		state int idx = 0;
		for (; idx < watchingInboxes.size(); idx++) {
			KeyRef key = watchingInboxes[idx].key;
			StringRef inboxStr = key.removePrefix(watcherPrefix);
			uint64_t inbox = valueToUInt64(inboxStr);
			// add this feed to the stale list of inbox
			(*tr).set(keyForInboxStaleFeed(inbox, feed), StringRef());
			// remove the inbox from the list of watchers on this feed
			(*tr).clear(key);
			highestInbox = inbox;
		}
		if (watchingInboxes.size() < 100)
			// If there were fewer watchers returned that we asked for, we're done.
			return Void();
	}
}

/*
 * Posts a message to a feed.  This updates the list of stale feeds to all watchers.
 * Return: a per-feed (non-global) message ID.
 *
 * This needs many additions to make it "real"
 * SOMEDAY: create better global message table to enforce cross-feed ordering.
 * SOMEDAY: create a global "dispatching" list for feeds that have yet to fully update inboxes.
 *        Move feed in and remove watchers in one transaction, possibly
 * SOMEDAY: create a global list of the most-subscribed-to feeds that all inbox reads check
 */
ACTOR Future<uint64_t> _postMessage(Database cx, uint64_t feed, Standalone<StringRef> data) {
	state Transaction tr(cx);
	state uint64_t messageId = UINT64_MAX - (uint64_t)now();
	TraceEvent("PubSubPost").detail("Feed", feed).detail("Message", messageId);
	loop {
		try {
			Optional<Value> feedValue = wait(tr.get(keyForFeed(feed)));
			if (!feedValue.present()) {
				// No such feed!!
				return uint64_t(0);
			}

			// Get globally latest message, set our ID to that less one
			state RangeResult latestMessage = wait(
			    tr.getRange(firstGreaterOrEqual(keyForMessage(0)), firstGreaterOrEqual(keyForMessage(UINT64_MAX)), 1));
			if (!latestMessage.size()) {
				messageId = UINT64_MAX - 1;
			} else {
				StringRef messageStr = latestMessage[0].key.removePrefix(messagePrefix);
				messageId = valueToUInt64(messageStr) - 1;
			}

			tr.set(keyForMessage(messageId), StringRef());
			tr.set(keyForDisptchEntry(messageId), StringRef());

			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	tr = Transaction(cx);
	loop {
		try {
			// Record this ID as the "latest message" for a feed
			tr.set(keyForFeedLatestMessage(feed), uInt64ToValue(messageId));

			// Store message in list of feed's messages
			tr.set(keyForFeedMessage(feed, messageId), StringRef());

			// Update the count of message that this feed has published
			Optional<Value> cntValue = wait(tr.get(keyForFeedMessageCount(feed)));
			uint64_t messageCount(valueToUInt64(cntValue.get()) + 1);
			tr.set(keyForFeedMessageCount(feed), uInt64ToValue(messageCount));

			// Go through the list of watching inboxes
			wait(updateFeedWatchers(&tr, feed));

			// Post the real message data; clear the "dispatching" entry
			tr.set(keyForMessage(messageId), data);
			tr.clear(keyForDisptchEntry(messageId));

			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return messageId;
}

Future<uint64_t> PubSub::postMessage(uint64_t feed, Standalone<StringRef> data) {
	return _postMessage(cx, feed, data);
}

ACTOR Future<int> singlePassInboxCacheUpdate(Database cx, uint64_t inbox, int swath) {
	state Transaction tr(cx);
	loop {
		try {
			// For each stale feed, update cache with latest message id
			state RangeResult staleFeeds =
			    wait(tr.getRange(firstGreaterOrEqual(keyForInboxStaleFeed(inbox, 0)),
			                     firstGreaterOrEqual(keyForInboxStaleFeed(inbox, UINT64_MAX)),
			                     swath)); // REVIEW: does 100 make sense?
			// printf("  --> stale feeds list size: %d\n", staleFeeds.size());
			if (!staleFeeds.size())
				// If there are no stale feeds, return.
				return 0;
			state StringRef stalePrefix = keyForInboxStalePrefix(inbox);
			state int idx = 0;
			for (; idx < staleFeeds.size(); idx++) {
				StringRef feedStr = staleFeeds[idx].key.removePrefix(stalePrefix);
				// printf("  --> clearing stale entry: %s\n", feedStr.toString().c_str());
				state uint64_t feed = valueToUInt64(feedStr);

				// SOMEDAY: change this to be a range query for the highest #'ed message
				Optional<Value> v = wait(tr.get(keyForFeedLatestMessage(feed)));
				state Value latestMessageValue = v.get();
				// printf("  --> latest message from feed: %s\n", latestMessageValue.toString().c_str());

				// find the messageID which is currently cached for this feed
				Optional<Value> lastCachedValue = wait(tr.get(keyForInboxCacheByFeed(inbox, feed)));
				if (lastCachedValue.present()) {
					uint64_t lastCachedId = valueToUInt64(lastCachedValue.get());
					// clear out the cache entry in the "by-ID" list for this feed
					// SOMEDAY: should we leave this in there in some way, or should we pull a better/more recent cache?
					tr.clear(keyForInboxCacheByID(inbox, lastCachedId));
				}
				// printf("  --> caching message by ID: %s\n", keyForInboxCacheByID(inbox,
				// valueToUInt64(latestMessageValue)).toString().c_str());
				tr.set(keyForInboxCacheByID(inbox, valueToUInt64(latestMessageValue)), uInt64ToValue(feed));

				// set the latest message
				tr.set(keyForInboxCacheByFeed(inbox, feed), latestMessageValue);
				tr.clear(staleFeeds[idx].key);
				// place watch back on feed
				tr.set(keyForFeedWatcher(feed, inbox), StringRef());
				// printf("  --> adding watch to feed: %s\n", keyForFeedWatcher(feed, inbox).toString().c_str());
			}
			wait(tr.commit());
			return staleFeeds.size();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// SOMEDAY: evaluate if this could lead to painful loop if there's one frequent feed.
ACTOR Future<Void> updateInboxCache(Database cx, uint64_t inbox) {
	state int swath = 100;
	state int updatedEntries = swath;
	while (updatedEntries >= swath) {
		int retVal = wait(singlePassInboxCacheUpdate(cx, inbox, swath));
		updatedEntries = retVal;
	}
	return Void();
}

ACTOR Future<MessageId> getFeedLatestAtOrAfter(Transaction* tr, Feed feed, MessageId position) {
	state RangeResult lastMessageRange = wait((*tr).getRange(firstGreaterOrEqual(keyForFeedMessage(feed, position)),
	                                                         firstGreaterOrEqual(keyForFeedMessage(feed, UINT64_MAX)),
	                                                         1));
	if (!lastMessageRange.size())
		return uint64_t(0);
	KeyValueRef m = lastMessageRange[0];
	StringRef prefix = keyForFeedMessagePrefix(feed);
	StringRef mIdStr = m.key.removePrefix(prefix);
	return valueToUInt64(mIdStr);
}

ACTOR Future<Message> getMessage(Transaction* tr, Feed feed, MessageId id) {
	state Message m;
	m.originatorFeed = feed;
	m.messageId = id;
	Optional<Value> data = wait(tr->get(keyForMessage(id)));
	m.data = data.get();
	return m;
}

ACTOR Future<std::vector<Message>> _listInboxMessages(Database cx, uint64_t inbox, int count, uint64_t cursor);

// inboxes with MANY fast feeds may be punished by the following checks
// SOMEDAY: add a check on global lists (or on dispatching list)
ACTOR Future<std::vector<Message>> _listInboxMessages(Database cx, uint64_t inbox, int count, uint64_t cursor) {
	TraceEvent("PubSubListInbox").detail("Inbox", inbox).detail("Count", count).detail("Cursor", cursor);
	wait(updateInboxCache(cx, inbox));
	state StringRef perIdPrefix = keyForInboxCacheByIDPrefix(inbox);
	loop {
		state Transaction tr(cx);
		state std::vector<Message> messages;
		state std::map<MessageId, Feed> feedLatest;
		try {
			// Fetch all cached entries for all the feeds to which we are subscribed
			Optional<Value> cntValue = wait(tr.get(keyForInboxSubscriptionCount(inbox)));
			uint64_t subscriptions = valueToUInt64(cntValue.get());
			state RangeResult feeds = wait(tr.getRange(firstGreaterOrEqual(keyForInboxCacheByID(inbox, 0)),
			                                           firstGreaterOrEqual(keyForInboxCacheByID(inbox, UINT64_MAX)),
			                                           subscriptions));
			if (!feeds.size())
				return messages;

			// read cache into map, replace entries newer than cursor with the newest older than cursor
			state int idx = 0;
			for (; idx < feeds.size(); idx++) {
				StringRef mIdStr = feeds[idx].key.removePrefix(perIdPrefix);
				MessageId messageId = valueToUInt64(mIdStr);
				state Feed feed = valueToUInt64(feeds[idx].value);
				// printf(" -> cached message %016llx from feed %016llx\n", messageId, feed);
				if (messageId >= cursor) {
					// printf(" -> entering message %016llx from feed %016llx\n", messageId, feed);
					feedLatest.emplace(messageId, feed);
				} else {
					// replace this with the first message older than the cursor
					MessageId mId = wait(getFeedLatestAtOrAfter(&tr, feed, cursor));
					if (mId) {
						feedLatest.emplace(mId, feed);
					}
				}
			}
			// There were some cached feeds, but none with messages older than "cursor"
			if (!feedLatest.size())
				return messages;

			// Check the list of dispatching messages to make sure there are no older ones than ours
			state MessageId earliestMessage = feedLatest.begin()->first;
			RangeResult dispatching = wait(tr.getRange(firstGreaterOrEqual(keyForDisptchEntry(earliestMessage)),
			                                           firstGreaterOrEqual(keyForDisptchEntry(UINT64_MAX)),
			                                           1));
			// If there are messages "older" than ours, try this again
			//  (with a new transaction and a flush of the "stale" feeds
			if (dispatching.size()) {
				std::vector<Message> r = wait(_listInboxMessages(cx, inbox, count, earliestMessage));
				return r;
			}

			while (messages.size() < count && feedLatest.size() > 0) {
				std::map<MessageId, Feed>::iterator latest = feedLatest.begin();
				state MessageId id = latest->first;
				state Feed f = latest->second;
				feedLatest.erase(latest);

				Message m = wait(getMessage(&tr, f, id));
				messages.push_back(m);

				MessageId nextMessage = wait(getFeedLatestAtOrAfter(&tr, f, id + 1));
				if (nextMessage) {
					feedLatest.emplace(nextMessage, f);
				}
			}

			return messages;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

Future<std::vector<Message>> PubSub::listInboxMessages(uint64_t inbox, int count, uint64_t cursor) {
	return _listInboxMessages(cx, inbox, count, cursor);
}

ACTOR Future<std::vector<Message>> _listFeedMessages(Database cx, Feed feed, int count, uint64_t cursor) {
	state std::vector<Message> messages;
	state Transaction tr(cx);
	TraceEvent("PubSubListFeed").detail("Feed", feed).detail("Count", count).detail("Cursor", cursor);
	loop {
		try {
			state RangeResult messageIds = wait(tr.getRange(firstGreaterOrEqual(keyForFeedMessage(feed, cursor)),
			                                                firstGreaterOrEqual(keyForFeedMessage(feed, UINT64_MAX)),
			                                                count));
			if (!messageIds.size())
				return messages;

			state int idx = 0;
			for (; idx < messageIds.size(); idx++) {
				StringRef mIdStr = messageIds[idx].key.removePrefix(keyForFeedMessagePrefix(feed));
				MessageId messageId = valueToUInt64(mIdStr);
				Message m = wait(getMessage(&tr, feed, messageId));
				messages.push_back(m);
			}
			return messages;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

Future<std::vector<Message>> PubSub::listFeedMessages(Feed feed, int count, uint64_t cursor) {
	return _listFeedMessages(cx, feed, count, cursor);
}
