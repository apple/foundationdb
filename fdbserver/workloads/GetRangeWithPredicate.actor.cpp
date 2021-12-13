/*
 * GetRangeWithPredicate.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct GetRangeWithPredicateWorkload : TestWorkload {
	bool enabled;

	GetRangeWithPredicateWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
	}

	std::string description() const override { return "GetRangeWithPredicate"; }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			// return _start(cx, this);
		}
		return Void();
	}

	// ACTOR Future<Void> _start(Database cx, GetRangeWithPredicateWorkload* self) { return Void(); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GetRangeWithPredicateWorkload> GetRangeWithPredicateWorkloadFactory("GetRangeWithPredicate");
