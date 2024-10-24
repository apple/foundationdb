/*
 * WatchAndWait.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct WatchAndWaitWorkload : TestWorkload {
	static constexpr auto NAME = "WatchAndWait";

	uint64_t nodeCount, watchCount;
	int64_t nodePrefix;
	int keyBytes;
	double testDuration;
	bool triggerWatches;
	std::vector<Future<Void>> clients;
	PerfIntCounter triggers, retries;

	WatchAndWaitWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), triggers("Triggers"), retries("Retries") {
		testDuration = getOption(options, "testDuration"_sr, 600.0);
		watchCount = getOption(options, "watchCount"_sr, (uint64_t)10000);
		nodeCount = getOption(options, "nodeCount"_sr, (uint64_t)100000);
		nodePrefix = getOption(options, "nodePrefix"_sr, (int64_t)-1);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 4);
		triggerWatches = getOption(options, "triggerWatches"_sr, false);

		if (watchCount > nodeCount) {
			watchCount = nodeCount;
		}
		if (nodePrefix > 0) {
			keyBytes += 16;
		}

		if (!triggerWatches) {
			keyBytes++; // watches are on different keys than the ones being modified by the workload
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	Key keyForIndex(uint64_t index) const {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		int idx = 0;
		if (nodePrefix > 0) {
			emplaceIndex(data, 0, nodePrefix);
			idx += 16;
		}

		double d = double(index) / nodeCount;
		emplaceIndex(data, idx, *(int64_t*)&d);

		return result;
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = testDuration;
		m.emplace_back("Triggers/sec", triggers.getValue() / duration, Averaged::False);
		m.push_back(triggers.getMetric());
		m.push_back(retries.getMetric());
	}

	ACTOR Future<Void> _start(Database cx, WatchAndWaitWorkload* self) {
		state std::vector<Future<Void>> watches;
		uint64_t endNode = (self->nodeCount * (self->clientId + 1)) / self->clientCount;
		uint64_t startNode = (self->nodeCount * self->clientId) / self->clientCount;
		uint64_t NodesPerWatch = self->nodeCount / self->watchCount;
		TraceEvent("WatchAndWaitExpect")
		    .detail("Duration", self->testDuration)
		    .detail("ExpectedCount", (endNode - startNode) / NodesPerWatch)
		    .detail("End", endNode)
		    .detail("Start", startNode)
		    .detail("Npw", NodesPerWatch);
		for (uint64_t i = startNode; i < endNode; i += NodesPerWatch) {
			watches.push_back(self->watchAndWait(cx, self, i));
		}
		wait(delay(self->testDuration)); // || waitForAll( watches )
		TraceEvent("WatchAndWaitEnd").detail("Duration", self->testDuration);
		return Void();
	}

	ACTOR Future<Void> watchAndWait(Database cx, WatchAndWaitWorkload* self, int index) {
		try {
			state ReadYourWritesTransaction tr(cx);
			loop {
				cx->maxOutstandingWatches = 1e6;
				try {
					state Future<Void> watch = tr.watch(self->keyForIndex(index));
					wait(tr.commit());
					wait(watch);
					++self->triggers;
				} catch (Error& e) {
					++self->retries;
					wait(tr.onError(e));
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "WatchAndWaitError").error(e);
			throw e;
		}
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory;
