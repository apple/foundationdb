/*
 * pubsub.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "pubsub.h"

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
Key keyForFeedSubscriber(uint64_t feed, uint64_t inbox) {
	return StringRef(format("f/%016llx/subs/%016llx", feed, inbox));
}
Key keyForFeedSubscriberCount(uint64_t feed) {
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

Standalone<StringRef> messagePrefix("m/"_sr);

Key keyForMessage(uint64_t message) {
	return StringRef(format("m/%016llx", message));
}

Key keyForDisptchEntry(uint64_t message) {
	return StringRef(format("d/%016llx", message));
}

PubSub::PubSub(Database _cx) : cx(_cx) {}

Future<uint64_t> _createFeed(Database cx, Standalone<StringRef> metadata) {
	uint64_t id(deterministicRandom()->randomUniqueID().first()); // SOMEDAY: this should be an atomic increment
	TraceEvent("PubSubCreateFeed").detail("Feed", id);
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			Optional<Value> val = co_await tr.get(keyForFeed(id));
			while (val.present()) {
				id = id + deterministicRandom()->randomInt(1, 100);
				Optional<Value> v = co_await tr.get(keyForFeed(id));
				val = v;
			}
			tr.set(keyForFeed(id), metadata);
			tr.set(keyForFeedSubscriberCount(id), uInt64ToValue(0));
			tr.set(keyForFeedMessageCount(id), uInt64ToValue(0));
			co_await tr.commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	co_return id;
}

Future<uint64_t> PubSub::createFeed(Standalone<StringRef> metadata) {
	return _createFeed(cx, metadata);
}

Future<uint64_t> _createInbox(Database cx, Standalone<StringRef> metadata) {
	uint64_t id = deterministicRandom()->randomUniqueID().first();
	TraceEvent("PubSubCreateInbox").detail("Inbox", id);
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			Optional<Value> val = co_await tr.get(keyForInbox(id));
			while (val.present()) {
				id += deterministicRandom()->randomInt(1, 100);
				Optional<Value> v = co_await tr.get(keyForFeed(id));
				val = v;
			}
			tr.set(keyForInbox(id), metadata);
			tr.set(keyForInboxSubscriptionCount(id), uInt64ToValue(0));
			co_await tr.commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	co_return id;
}

Future<uint64_t> PubSub::createInbox(Standalone<StringRef> metadata) {
	return _createInbox(cx, metadata);
}

Future<bool> _createSubscription(Database cx, uint64_t feed, uint64_t inbox) {
	Transaction tr(cx);
	TraceEvent("PubSubCreateSubscription").detail("Feed", feed).detail("Inbox", inbox);
	while (true) {
		Error err;
		try {
			Optional<Value> subscription = co_await tr.get(keyForInboxSubscription(inbox, feed));
			if (subscription.present()) {
				// For idempotency, this could exist from a previous transaction from us that succeeded
				co_return true;
			}
			Optional<Value> inboxVal = co_await tr.get(keyForInbox(inbox));
			if (!inboxVal.present()) {
				co_return false;
			}
			Optional<Value> feedVal = co_await tr.get(keyForFeed(feed));
			if (!feedVal.present()) {
				co_return false;
			}

			// Update the subscriptions of the inbox
			Optional<Value> subscriptionCountVal = co_await tr.get(keyForInboxSubscriptionCount(inbox));
			uint64_t subscriptionCount = valueToUInt64(subscriptionCountVal.get()); // throws if count not present
			tr.set(keyForInboxSubscription(inbox, feed), StringRef());
			tr.set(keyForInboxSubscriptionCount(inbox), uInt64ToValue(subscriptionCount + 1));

			// Update the subscribers of the feed
			Optional<Value> subscriberCountVal = co_await tr.get(keyForFeedSubscriberCount(feed));
			uint64_t subscriberCount = valueToUInt64(subscriberCountVal.get()); // throws if count not present
			tr.set(keyForFeedSubscriber(feed, inbox), StringRef());
			tr.set(keyForFeedSubscriberCount(inbox), uInt64ToValue(subscriberCount + 1));

			// Add inbox as watcher of feed.
			tr.set(keyForFeedWatcher(feed, inbox), StringRef());

			co_await tr.commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	co_return true;
}

Future<bool> PubSub::createSubscription(uint64_t feed, uint64_t inbox) {
	return _createSubscription(cx, feed, inbox);
}

// Since we are not relying on "read-your-own-writes", we need to keep track of
//  the highest-numbered inbox that we've cleared from the watchers list and
//  make sure that further requests start after this inbox.
Future<Void> updateFeedWatchers(Transaction* tr, uint64_t feed) {
	StringRef watcherPrefix = keyForFeedWatcherPrefix(feed);
	uint64_t highestInbox{ 0 };
	bool first = true;
	while (true) {
		// Grab watching inboxes in swaths of 100
		RangeResult watchingInboxes =
		    co_await (*tr).getRange(firstGreaterOrEqual(keyForFeedWatcher(feed, first ? 0 : highestInbox + 1)),
		                            firstGreaterOrEqual(keyForFeedWatcher(feed, UINT64_MAX)),
		                            100); // REVIEW: does 100 make sense?
		if (watchingInboxes.empty())
			// If there are no watchers, return.
			co_return;
		first = false;
		for (int idx = 0; idx < watchingInboxes.size(); idx++) {
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
			co_return;
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
Future<uint64_t> _postMessage(Database cx, uint64_t feed, Standalone<StringRef> data) {
	Transaction tr(cx);
	uint64_t messageId = UINT64_MAX - (uint64_t)now();
	TraceEvent("PubSubPost").detail("Feed", feed).detail("Message", messageId);
	while (true) {
		Error err;
		try {
			Optional<Value> feedValue = co_await tr.get(keyForFeed(feed));
			if (!feedValue.present()) {
				// No such feed!!
				co_return uint64_t(0);
			}

			// Get globally latest message, set our ID to that less one
			RangeResult latestMessage = co_await tr.getRange(
			    firstGreaterOrEqual(keyForMessage(0)), firstGreaterOrEqual(keyForMessage(UINT64_MAX)), 1);
			if (latestMessage.empty()) {
				messageId = UINT64_MAX - 1;
			} else {
				StringRef messageStr = latestMessage[0].key.removePrefix(messagePrefix);
				messageId = valueToUInt64(messageStr) - 1;
			}

			tr.set(keyForMessage(messageId), StringRef());
			tr.set(keyForDisptchEntry(messageId), StringRef());

			co_await tr.commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	tr = Transaction(cx);
	while (true) {
		Error err;
		try {
			// Record this ID as the "latest message" for a feed
			tr.set(keyForFeedLatestMessage(feed), uInt64ToValue(messageId));

			// Store message in list of feed's messages
			tr.set(keyForFeedMessage(feed, messageId), StringRef());

			// Update the count of message that this feed has published
			Optional<Value> cntValue = co_await tr.get(keyForFeedMessageCount(feed));
			uint64_t messageCount(valueToUInt64(cntValue.get()) + 1);
			tr.set(keyForFeedMessageCount(feed), uInt64ToValue(messageCount));

			// Go through the list of watching inboxes
			co_await updateFeedWatchers(&tr, feed);

			// Post the real message data; clear the "dispatching" entry
			tr.set(keyForMessage(messageId), data);
			tr.clear(keyForDisptchEntry(messageId));

			co_await tr.commit();
			break;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
	co_return messageId;
}

Future<uint64_t> PubSub::postMessage(uint64_t feed, Standalone<StringRef> data) {
	return _postMessage(cx, feed, data);
}

Future<int> singlePassInboxCacheUpdate(Database cx, uint64_t inbox, int swath) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			// For each stale feed, update cache with latest message id
			RangeResult staleFeeds = co_await tr.getRange(firstGreaterOrEqual(keyForInboxStaleFeed(inbox, 0)),
			                                              firstGreaterOrEqual(keyForInboxStaleFeed(inbox, UINT64_MAX)),
			                                              swath); // REVIEW: does 100 make sense?
			// printf("  --> stale feeds list size: %d\n", staleFeeds.size());
			if (staleFeeds.empty())
				// If there are no stale feeds, return.
				co_return 0;
			StringRef stalePrefix = keyForInboxStalePrefix(inbox);
			for (int idx = 0; idx < staleFeeds.size(); idx++) {
				StringRef feedStr = staleFeeds[idx].key.removePrefix(stalePrefix);
				// printf("  --> clearing stale entry: %s\n", feedStr.toString().c_str());
				uint64_t feed = valueToUInt64(feedStr);

				// SOMEDAY: change this to be a range query for the highest #'ed message
				Optional<Value> v = co_await tr.get(keyForFeedLatestMessage(feed));
				Value latestMessageValue = v.get();
				// printf("  --> latest message from feed: %s\n", latestMessageValue.toString().c_str());

				// find the messageID which is currently cached for this feed
				Optional<Value> lastCachedValue = co_await tr.get(keyForInboxCacheByFeed(inbox, feed));
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
			co_await tr.commit();
			co_return staleFeeds.size();
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// SOMEDAY: evaluate if this could lead to painful loop if there's one frequent feed.
Future<Void> updateInboxCache(Database cx, uint64_t inbox) {
	int swath = 100;
	int updatedEntries = swath;
	while (updatedEntries >= swath) {
		updatedEntries = co_await singlePassInboxCacheUpdate(cx, inbox, swath);
	}
}

Future<MessageId> getFeedLatestAtOrAfter(Transaction* tr, Feed feed, MessageId position) {
	RangeResult lastMessageRange = co_await (*tr).getRange(firstGreaterOrEqual(keyForFeedMessage(feed, position)),
	                                                       firstGreaterOrEqual(keyForFeedMessage(feed, UINT64_MAX)),
	                                                       1);
	if (lastMessageRange.empty())
		co_return uint64_t(0);
	KeyValueRef m = lastMessageRange[0];
	StringRef prefix = keyForFeedMessagePrefix(feed);
	StringRef mIdStr = m.key.removePrefix(prefix);
	co_return valueToUInt64(mIdStr);
}

Future<Message> getMessage(Transaction* tr, Feed feed, MessageId id) {
	Message m;
	m.originatorFeed = feed;
	m.messageId = id;
	Optional<Value> data = co_await tr->get(keyForMessage(id));
	m.data = data.get();
	co_return m;
}

Future<std::vector<Message>> _listInboxMessages(Database cx, uint64_t inbox, int count, uint64_t cursor);

// inboxes with MANY fast feeds may be punished by the following checks
// SOMEDAY: add a check on global lists (or on dispatching list)
Future<std::vector<Message>> _listInboxMessages(Database cx, uint64_t inbox, int count, uint64_t cursor) {
	TraceEvent("PubSubListInbox").detail("Inbox", inbox).detail("Count", count).detail("Cursor", cursor);
	co_await updateInboxCache(cx, inbox);
	StringRef perIdPrefix = keyForInboxCacheByIDPrefix(inbox);
	while (true) {
		Transaction tr(cx);
		std::vector<Message> messages;
		std::map<MessageId, Feed> feedLatest;
		Error err;
		try {
			// Fetch all cached entries for all the feeds to which we are subscribed
			Optional<Value> cntValue = co_await tr.get(keyForInboxSubscriptionCount(inbox));
			uint64_t subscriptions = valueToUInt64(cntValue.get());
			RangeResult feeds = co_await tr.getRange(firstGreaterOrEqual(keyForInboxCacheByID(inbox, 0)),
			                                         firstGreaterOrEqual(keyForInboxCacheByID(inbox, UINT64_MAX)),
			                                         subscriptions);
			if (feeds.empty())
				co_return messages;

			// read cache into map, replace entries newer than cursor with the newest older than cursor
			for (int idx = 0; idx < feeds.size(); idx++) {
				StringRef mIdStr = feeds[idx].key.removePrefix(perIdPrefix);
				MessageId messageId = valueToUInt64(mIdStr);
				Feed feed = valueToUInt64(feeds[idx].value);
				// printf(" -> cached message %016llx from feed %016llx\n", messageId, feed);
				if (messageId >= cursor) {
					// printf(" -> entering message %016llx from feed %016llx\n", messageId, feed);
					feedLatest.emplace(messageId, feed);
				} else {
					// replace this with the first message older than the cursor
					MessageId mId = co_await getFeedLatestAtOrAfter(&tr, feed, cursor);
					if (mId) {
						feedLatest.emplace(mId, feed);
					}
				}
			}
			// There were some cached feeds, but none with messages older than "cursor"
			if (feedLatest.empty())
				co_return messages;

			// Check the list of dispatching messages to make sure there are no older ones than ours
			MessageId earliestMessage = feedLatest.begin()->first;
			RangeResult dispatching = co_await tr.getRange(firstGreaterOrEqual(keyForDisptchEntry(earliestMessage)),
			                                               firstGreaterOrEqual(keyForDisptchEntry(UINT64_MAX)),
			                                               1);
			// If there are messages "older" than ours, try this again
			//  (with a new transaction and a flush of the "stale" feeds
			if (!dispatching.empty()) {
				std::vector<Message> r = co_await _listInboxMessages(cx, inbox, count, earliestMessage);
				co_return r;
			}

			while (messages.size() < count && !feedLatest.empty()) {
				auto latest = feedLatest.begin();
				MessageId id = latest->first;
				Feed f = latest->second;
				feedLatest.erase(latest);

				Message m = co_await getMessage(&tr, f, id);
				messages.push_back(m);

				MessageId nextMessage = co_await getFeedLatestAtOrAfter(&tr, f, id + 1);
				if (nextMessage) {
					feedLatest.emplace(nextMessage, f);
				}
			}

			co_return messages;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<std::vector<Message>> PubSub::listInboxMessages(uint64_t inbox, int count, uint64_t cursor) {
	return _listInboxMessages(cx, inbox, count, cursor);
}

Future<std::vector<Message>> _listFeedMessages(Database cx, Feed feed, int count, uint64_t cursor) {
	std::vector<Message> messages;
	Transaction tr(cx);
	TraceEvent("PubSubListFeed").detail("Feed", feed).detail("Count", count).detail("Cursor", cursor);
	while (true) {
		Error err;
		try {
			RangeResult messageIds = co_await tr.getRange(firstGreaterOrEqual(keyForFeedMessage(feed, cursor)),
			                                              firstGreaterOrEqual(keyForFeedMessage(feed, UINT64_MAX)),
			                                              count);
			if (messageIds.empty())
				co_return messages;

			for (int idx = 0; idx < messageIds.size(); idx++) {
				StringRef mIdStr = messageIds[idx].key.removePrefix(keyForFeedMessagePrefix(feed));
				MessageId messageId = valueToUInt64(mIdStr);
				Message m = co_await getMessage(&tr, feed, messageId);
				messages.push_back(m);
			}
			co_return messages;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<std::vector<Message>> PubSub::listFeedMessages(Feed feed, int count, uint64_t cursor) {
	return _listFeedMessages(cx, feed, count, cursor);
}
