/*
 * Fuzz.cpp
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

#include "fdbrpc/ActorFuzz.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"

struct ActorFuzzWorkload : TestWorkload {
	bool enabled;
	std::pair<int, int> fuzzResults;

	ActorFuzzWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), fuzzResults(std::make_pair(0, 0)) {
		enabled = !clientId; // only do this on the "first" client
	}

	std::string description() const override { return "ActorFuzzWorkload"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (enabled) {
			// Only include this test outside of Windows because of MSVC compiler bug
			fuzzResults.second = 0;

			// Only include this test outside of Windows because of MSVC compiler bug
#ifndef WIN32
			fuzzResults = actorFuzzTests();
#endif
			if (fuzzResults.second == 0)
				// if there are no total tests, then mark this as "non-passing"
				fuzzResults.first = 1;
		}
		return Void();
	}
	Future<bool> check(Database const& cx) override { return fuzzResults.first == fuzzResults.second; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ActorFuzzWorkload> ActorFuzzWorkloadFactory("ActorFuzz");
