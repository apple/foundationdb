/*
 * TimeKeeperCorrectness.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // has to be last include

struct TimeKeeperCorrectnessWorkload : TestWorkload {
	double testDuration;
	std::map<int64_t, Version> inMemTimeKeeper;

	TimeKeeperCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 20.0);
	}

	std::string description() const override { return "TimeKeeperCorrectness"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> _start(Database cx, TimeKeeperCorrectnessWorkload* self) {
		TraceEvent(SevInfo, "TKCorrectness_Start").log();

		state double start = now();
		while (now() - start > self->testDuration) {
			state Transaction tr(cx);

			loop {
				try {
					state int64_t curTime = now();
					Version v = wait(tr.getReadVersion());
					self->inMemTimeKeeper[curTime] = v;
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			// For every sample from Timekeeper collect two samples here.
			wait(delay(std::min(SERVER_KNOBS->TIME_KEEPER_DELAY / 10, (int64_t)1L)));
		}

		TraceEvent(SevInfo, "TKCorrectness_Completed").log();
		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR static Future<bool> _check(Database cx, TimeKeeperCorrectnessWorkload* self) {
		state KeyBackedMap<int64_t, Version> dbTimeKeeper = KeyBackedMap<int64_t, Version>(timeKeeperPrefixRange.begin);
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);

		TraceEvent(SevInfo, "TKCorrectness_CheckStart")
		    .detail("TimeKeeperMaxEntries", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES)
		    .detail("TimeKeeperDelay", SERVER_KNOBS->TIME_KEEPER_DELAY);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				std::vector<std::pair<int64_t, Version>> allItems =
				    wait(dbTimeKeeper.getRange(tr, 0, Optional<int64_t>(), self->inMemTimeKeeper.size() + 2));

				if (allItems.size() > SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES + 1) {
					TraceEvent(SevError, "TKCorrectness_TooManyEntries")
					    .detail("Expected", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES + 1)
					    .detail("Found", allItems.size());
					return false;
				}

				if (allItems.size() < self->testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY) {
					TraceEvent(SevWarnAlways, "TKCorrectness_TooFewEntries")
					    .detail("Expected", self->testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY)
					    .detail("Found", allItems.size());
				}

				for (auto item : allItems) {
					auto it = self->inMemTimeKeeper.lower_bound(item.first);
					if (it == self->inMemTimeKeeper.end()) {
						continue;
					}

					if (item.second >= it->second) {
						TraceEvent(SevError, "TKCorrectness_VersionIncorrectBounds")
						    .detail("ClusterTime", item.first)
						    .detail("ClusterVersion", item.second)
						    .detail("LocalTime", it->first)
						    .detail("LocalVersion", it->second);
						return false;
					}
				}

				TraceEvent(SevInfo, "TKCorrectness_Passed").log();
				return true;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	Future<bool> check(Database const& cx) override { return _check(cx, this); }
};

WorkloadFactory<TimeKeeperCorrectnessWorkload> TimeKeeperCorrectnessWorkloadFactory("TimeKeeperCorrectness");
