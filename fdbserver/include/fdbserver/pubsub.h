/*
 * pubsub.h
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

#include "fdbclient/NativeAPI.actor.h"

/*
 * ///////////////
 * General FDB data model of Pub/Sub model
 * Feeds, Inboxes, Messages
 *
 * Message - publisher (feed), data
 *
 * Inbox - message cache, dirty list, list of subscribed-to feeds
 *
 * Feed - messages, list of inboxes to notify
 *
 * Structures for scalability: global list of dispatching feeds, feeds to consider
 *  dirty for all operation
 *
 * ///////////////
 * Basic Processes: post message, list messages
 *
 * Post (in one transaction):
 *  1. Add message to list of message in feed history
 *  2. For each Inbox of list of watching inboxes
 *    a. remove Inbox from list of watchers
 *    b. mark self as 'dirty' in the Inbox's dirty list
 *
 * List recent message in Inbox (in one transaction), up to n messages:
 *  1. For each dirty Feed, update cache with latest message
 *  2. Triage list by getting latest m messages from top m feeds
 *    note: these messages can be cached in memory on the server, and on paging
 *     operations reused, so long as step 1 is repeated, IF there is no message deletion.
 *
 * ////////////////
 * Assumptions:
 *  1. Subscriptions are "retroactive".  If a subscription is in place, the messages
 *    from that feed will start to appear in the listing of that inboxes messages
 *    and appear in historical lists as well.  This could lead to odd behaviour
 *    if paging through the contents of an inbox while a new subscrption was added
 */

typedef uint64_t Feed;
typedef uint64_t Inbox;
typedef uint64_t MessageId;

class Message {
public:
	Feed originatorFeed;
	MessageId messageId;
	Standalone<StringRef> data;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, originatorFeed, messageId, data, data.arena());
	}
};

class PubSub {
public:
	PubSub(Database cx);

	Future<Feed> createFeed(Standalone<StringRef> metadata);

	Future<Inbox> createInbox(Standalone<StringRef> metadata);

	Future<bool> createSubscription(Feed feed, Inbox inbox);

	Future<MessageId> postMessage(Feed feed, Standalone<StringRef> data);

	Future<std::vector<Message>> listFeedMessages(Feed feed, int count, MessageId cursor = 0);

	Future<std::vector<Message>> listInboxMessages(Inbox inbox, int count, MessageId cursor = 0);

private:
	Database cx;
};
