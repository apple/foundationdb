/*
 * TimeKeeperCorrectness.cpp
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

#include "fdbserver/tester/workloads.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbserver/core/Knobs.h"

struct TimeKeeperCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "TimeKeeperCorrectness";

	double testDuration;
	std::map<int64_t, Version> inMemTimeKeeper;

	explicit TimeKeeperCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 20.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> start(Database const& cx) override {
		TraceEvent(SevInfo, "TKCorrectness_Start").log();

		double start = now();
		while (now() - start > testDuration) {
			Transaction tr(cx);

			while (true) {
				Error err;
				try {
					int64_t curTime = now();
					Version v = co_await tr.getReadVersion();
					inMemTimeKeeper[curTime] = v;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}

			// For every sample from Timekeeper collect two samples here.
			co_await delay(std::min(SERVER_KNOBS->TIME_KEEPER_DELAY / 10, (int64_t)1L));
		}

		TraceEvent(SevInfo, "TKCorrectness_Completed").log();
	}

	Future<bool> check(Database const& cx) override {
		KeyBackedMap<int64_t, Version> dbTimeKeeper = KeyBackedMap<int64_t, Version>(timeKeeperPrefixRange.begin);
		auto tr = makeReference<ReadYourWritesTransaction>(cx);

		TraceEvent(SevInfo, "TKCorrectness_CheckStart")
		    .detail("TimeKeeperMaxEntries", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES)
		    .detail("TimeKeeperDelay", SERVER_KNOBS->TIME_KEEPER_DELAY);

		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				KeyBackedRangeResult<std::pair<int64_t, Version>> allItems =
				    co_await dbTimeKeeper.getRange(tr, 0, Optional<int64_t>(), inMemTimeKeeper.size() + 2);

				if (allItems.results.size() > SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES + 1) {
					TraceEvent(SevError, "TKCorrectness_TooManyEntries")
					    .detail("Expected", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES + 1)
					    .detail("Found", allItems.results.size());
					co_return false;
				}

				if (allItems.results.size() < testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY) {
					TraceEvent(SevWarnAlways, "TKCorrectness_TooFewEntries")
					    .detail("Expected", testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY)
					    .detail("Found", allItems.results.size());
				}

				for (auto item : allItems.results) {
					auto it = inMemTimeKeeper.lower_bound(item.first);
					if (it == inMemTimeKeeper.end()) {
						continue;
					}

					if (item.second >= it->second) {
						TraceEvent(SevError, "TKCorrectness_VersionIncorrectBounds")
						    .detail("ClusterTime", item.first)
						    .detail("ClusterVersion", item.second)
						    .detail("LocalTime", it->first)
						    .detail("LocalVersion", it->second);
						co_return false;
					}
				}

				TraceEvent(SevInfo, "TKCorrectness_Passed").log();
				co_return true;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	}
};

WorkloadFactory<TimeKeeperCorrectnessWorkload> TimeKeeperCorrectnessWorkloadFactory;
