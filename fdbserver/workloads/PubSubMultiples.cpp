/*
 * PubSubMultiples.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "pubsub.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct PubSubMultiplesWorkload : TestWorkload {
	static constexpr auto NAME = "PubSubMultiples";

	double testDuration, messagesPerSecond;
	int actorCount, inboxesPerActor;

	std::vector<Future<Void>> inboxWatchers;
	PerfIntCounter messages;

	explicit PubSubMultiplesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), messages("Messages") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		messagesPerSecond = getOption(options, "messagesPerSecond"_sr, 500.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, 20);
		inboxesPerActor = getOption(options, "inboxesPerActor"_sr, 20);
	}

	Future<Void> setup(Database const& cx) override { return createNodes(this, cx); }
	Future<Void> start(Database const& cx) override {
		Future<Void> _ = startTests(this, cx);
		return delay(testDuration);
	}
	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override { m.push_back(messages.getMetric()); }

	Key keyForFeed(int i) { return StringRef(format("/PSM/feeds/%d", i)); }
	Key keyForInbox(int i) { return StringRef(format("/PSM/inbox/%d", i)); }
	Value valueForUInt(uint64_t i) { return StringRef(format("%llx", i)); }

	Future<Void> createNodeSwath(PubSubMultiplesWorkload* self, int actor, Database cx) {
		PubSub ps(cx);
		std::vector<uint64_t> feeds;
		std::vector<uint64_t> inboxes;
		for (int idx = 0; idx < self->inboxesPerActor; idx++) {
			uint64_t feedIdx = co_await ps.createFeed(StringRef());
			feeds.push_back(feedIdx);
			uint64_t inboxIdx = co_await ps.createInbox(StringRef());
			inboxes.push_back(inboxIdx);
		}
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				for (int idx = 0; idx < self->inboxesPerActor; idx++) {
					int offset = (self->clientId * self->clientCount * self->actorCount * self->inboxesPerActor) +
					             (actor * self->actorCount * self->inboxesPerActor) + idx;
					tr.set(self->keyForFeed(offset), self->valueForUInt(feeds[idx]));
					tr.set(self->keyForInbox(offset), self->valueForUInt(inboxes[idx]));
				}
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> createNodes(PubSubMultiplesWorkload* self, Database cx) {
		PubSub ps(cx);
		std::vector<Future<Void>> actors;
		actors.reserve(self->actorCount);
		for (int i = 0; i < self->actorCount; i++)
			actors.push_back(self->createNodeSwath(self, i, cx->clone()));
		co_await waitForAll(actors);
		TraceEvent("PSMNodesCreated").detail("ClientIdx", self->clientId);
	}

	Future<Void> createSubscriptions(PubSubMultiplesWorkload* self, int actor, Database cx) {
		// create the "multiples" subscriptions for each owned inbox
		return Void();
	}

	Future<Void> messageSender(PubSubMultiplesWorkload* self, Database cx) {
		// use a possion loop and post messages to feeds
		return Void();
	}

	Future<Void> startTests(PubSubMultiplesWorkload* self, Database cx) {
		std::vector<Future<Void>> subscribers;
		subscribers.reserve(self->actorCount);
		for (int i = 0; i < self->actorCount; i++)
			subscribers.push_back(self->createSubscriptions(self, i, cx));
		co_await waitForAll(subscribers);

		Future<Void> sender = self->messageSender(self, cx);
	}
};

WorkloadFactory<PubSubMultiplesWorkload> PubSubMultiplesWorkloadFactory;
