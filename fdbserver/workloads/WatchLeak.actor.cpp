/*
 * WatchLeak.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

Key getKey(int i) {
	return Key(format("watchKey/%d", i));
}

ACTOR static Future<Void> getWatch(Database cx, int index) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
	state Future<Void> watchFuture;
	loop {
		try {
			watchFuture = tr->watch(getKey(index));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
	wait(watchFuture);
	return Void();
}

} // namespace

class WatchLeakWorkload : public TestWorkload {
	int numBatches;
	int numActiveWatches;
	double delayBetweenBatches;

	ACTOR static Future<Void> _start(WatchLeakWorkload const* self, Database cx) {
		state std::vector<Future<Void>> activeWatches;
		state int batchIndex = 0;
		for (; batchIndex < self->numBatches; ++batchIndex) {
			for (int i = 0; i < self->numActiveWatches; ++i) {
				activeWatches.push_back(getWatch(cx, batchIndex * self->numActiveWatches + i));
			}
			wait(delay(self->delayBetweenBatches));
			// All outstanding watches should be cancelled here:
			activeWatches.clear();
		}
		return Void();
	}

public:
	static constexpr auto NAME = "WatchLeak";

	WatchLeakWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numBatches = getOption(options, LiteralStringRef("numBatches"), 100);
		numActiveWatches = getOption(options, LiteralStringRef("numActiveWatches"), 100);
		delayBetweenBatches = getOption(options, LiteralStringRef("delayBetweenBatches"), 5.0);
	}

	Future<Void> setup(Database const&) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(this, cx); }

	Future<bool> check(Database const&) override { return true; }

	void getMetrics(std::vector<PerfMetric>&) override {}

	std::string description() const override { return NAME; }
};

WorkloadFactory<WatchLeakWorkload> WatchLeakWorkloadFactory(WatchLeakWorkload::NAME);
