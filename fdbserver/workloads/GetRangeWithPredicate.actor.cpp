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
#include "fdbclient/NativeAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct GetRangeWithPredicateWorkload : TestWorkload {
	bool enabled;

	GetRangeWithPredicateWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
	}

	std::string description() const override { return "GetRangeWithPredicate"; }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return _start(cx, this);
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, GetRangeWithPredicateWorkload* self) {
		wait(populateData(cx, 200));
		wait(testGetRangeSubstrMatch(cx));
		wait(testGetRangeWithLimits(cx));
		return Void();
	}

	ACTOR static Future<Void> populateData(Database cx, int n) {
		state std::vector<StringRef> testValues = {
			"one test value"_sr, "test second value"_sr, "third value"_sr, "something unrelated"_sr, "one more"_sr,
		};

		loop {
			state Transaction tr(cx);
			try {
				tr.reset();
				for (int i = 0; i < 1000; i++) {
					tr.set(format("rangea%06d", i), testValues[i % testValues.size()]);
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> testGetRangeSubstrMatch(Database cx) {
		Key rangeBegin = "rangea"_sr;
		Key rangeEnd = "rangeb"_sr;

		state Transaction tr(cx);
		try {
			tr.reset();
			RangeResult result =
			    wait(tr.getRangeWithPredicate(KeySelector(firstGreaterOrEqual(rangeBegin)),
			                                  KeySelector(firstGreaterOrEqual(rangeEnd)),
			                                  GetRangeLimits(),
			                                  GetRangePredicate(PRED_FIND_IN_VALUE).addArg("test"_sr)));
			ASSERT(result.size() == 400);
			showResult(result);
		} catch (Error& e) {
			wait(tr.onError(e));
		}
		return Void();
	}

	ACTOR static Future<Void> testGetRangeWithLimits(Database cx) {
		Key rangeBegin = "rangea"_sr;
		Key rangeEnd = "rangeb"_sr;

		state Transaction tr(cx);
		try {
			tr.reset();
			RangeResult result =
			    wait(tr.getRangeWithPredicate(KeySelector(firstGreaterOrEqual(rangeBegin)),
			                                  KeySelector(firstGreaterOrEqual(rangeEnd)),
			                                  GetRangeLimits(100),
			                                  GetRangePredicate(PRED_FIND_IN_VALUE).addArg("test"_sr)));
			ASSERT(result.size() == 100);
			showResult(result);
		} catch (Error& e) {
			wait(tr.onError(e));
		}
		return Void();
	}

	static void showResult(const RangeResult& result) {
		std::cout << "result size: " << result.size() << std::endl;
		int cnt = 0;
		for (const KeyValueRef* it = result.begin(); it != result.end(); it++) {
			std::cout << "key=" << it->key.printable() << ", value=" << it->value.printable() << std::endl;
			if (cnt++ >= 5) {
				return;
			}
		}
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GetRangeWithPredicateWorkload> GetRangeWithPredicateWorkloadFactory("GetRangeWithPredicate");
