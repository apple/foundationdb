/*
 * WatchAndWait.cpp
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
#include "fdbserver/core/TesterInterface.h"
#include "BulkSetup.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/workloads.actor.h"

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

	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> watches;
		uint64_t endNode = (nodeCount * (clientId + 1)) / clientCount;
		uint64_t startNode = (nodeCount * clientId) / clientCount;
		uint64_t NodesPerWatch = nodeCount / watchCount;
		TraceEvent("WatchAndWaitExpect")
		    .detail("Duration", testDuration)
		    .detail("ExpectedCount", (endNode - startNode) / NodesPerWatch)
		    .detail("End", endNode)
		    .detail("Start", startNode)
		    .detail("Npw", NodesPerWatch);
		for (uint64_t i = startNode; i < endNode; i += NodesPerWatch) {
			watches.push_back(watchAndWait(cx, this, i));
		}
		co_await delay(testDuration); // || waitForAll( watches )
		TraceEvent("WatchAndWaitEnd").detail("Duration", testDuration);
	}

	Future<Void> watchAndWait(Database cx, WatchAndWaitWorkload* self, int index) {
		try {
			ReadYourWritesTransaction tr(cx);
			while (true) {
				cx->maxOutstandingWatches = 1e6;
				Error err;
				try {
					Future<Void> watch = tr.watch(self->keyForIndex(index));
					co_await tr.commit();
					co_await watch;
					++self->triggers;
				} catch (Error& e) {
					err = e;
				}
				++self->retries;
				co_await tr.onError(err);
			}
		} catch (Error& e) {
			TraceEvent(SevError, "WatchAndWaitError").error(e);
			throw e;
		}
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory;
