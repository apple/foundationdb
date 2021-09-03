/*
 * ChangeFeeds.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/serialize.h"
#include <cstring>

struct ChangeFeedsWorkload : TestWorkload {
	double testDuration;

	ChangeFeedsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
	}

	std::string description() const override { return "ChangeFeedsWorkload"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return Void(); /*changeFeedClient(cx->clone(), this, testDuration);*/ }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(vector<PerfMetric>& m) override {}

	/*
	ACTOR Future<Void> changeFeedClient(Database cx, ChangeFeedsWorkload* self, double duration) {
		// Enable change feed for a key range
		state UID rangeUID = deterministicRandom()->randomUniqueID();
		state Key rangeID = StringRef(rangeUID.toString());
		state Key logPath = LiteralStringRef("\xff\x02/cftest/");
		state Transaction tr(cx);
		loop {
			try {
				wait(tr.registerChangeFeed(rangeID, normalKeys));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// Periodically read from both and compare results (merging clears)
		state PromiseStream<Standalone<VectorRef<MutationsAndVersionRef>>> results;
		cx->getChangeFeedStream(results, rangeID);

		// Pop from both
		return Void(); 
	}*/
};

WorkloadFactory<ChangeFeedsWorkload> ChangeFeedsWorkloadFactory("ChangeFeeds");
