/*
 * TimeKeeperCorrectness.actor.cpp
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

#include "workloads.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbserver/Knobs.h"

struct TimeKeeperCorrectnessWorkload : TestWorkload {
	double testDuration;
	std::map<int64_t, Version> inMemTimeKeeper;

	TimeKeeperCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption( options, LiteralStringRef("testDuration"), 20.0 );
	}

	virtual std::string description() {
		return "TimeKeeperCorrectness";
	}

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	ACTOR static Future<Void> _start(Database cx, TimeKeeperCorrectnessWorkload *self) {
		TraceEvent(SevInfo, "TKCorrectness_start");

		state double start = now();
		while (now() - start > self->testDuration) {
			state Transaction tr(cx);

			loop {
				try {
					state int64_t curTime = now();
					Version v = wait(tr.getReadVersion());
					self->inMemTimeKeeper[curTime] = v;
					break;
				} catch (Error &e) {
					Void _ = wait(tr.onError(e));
				}
			}

			// For every sample from Timekeeper collect two samples here.
			Void _ = wait(delay(std::min(SERVER_KNOBS->TIME_KEEPER_DELAY / 10, (int64_t)1L)));
		}

		TraceEvent(SevInfo, "TKCorrectness_completed");
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		return _start(cx, this);
	}

	ACTOR static Future<bool> _check(Database cx, TimeKeeperCorrectnessWorkload *self) {
		state KeyBackedMap<int64_t, Version> dbTimeKeeper = KeyBackedMap<int64_t, Version>(timeKeeperPrefixRange.begin);
		state Reference<ReadYourWritesTransaction> tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));

		TraceEvent(SevInfo, "TKCorrectness_checkStart")
				.detail("TIME_KEPER_MAX_ENTRIES", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES)
				.detail("TIME_KEEPER_DELAY", SERVER_KNOBS->TIME_KEEPER_DELAY);

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				std::vector<std::pair<int64_t, Version>> allItems = wait(
						dbTimeKeeper.getRange(tr, 0, Optional<int64_t>(), self->inMemTimeKeeper.size() + 2));

				if (allItems.size() > SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES + 1) {
					TraceEvent(SevError, "TKCorrectness_tooManyEntries")
							.detail("expected", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES + 1)
							.detail("found", allItems.size());
					return false;
				}

				if (allItems.size() < self->testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY) {
					TraceEvent(SevWarnAlways, "TKCorrectness_tooFewEntries")
							.detail("expected", self->testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY)
							.detail("found", allItems.size());
				}

				for (auto item : allItems) {
					auto it = self->inMemTimeKeeper.lower_bound(item.first);
					if (it == self->inMemTimeKeeper.end()) {
						continue;
					}

					if (item.second >= it->second) {
						TraceEvent(SevError, "TKCorrectness_versionIncorrectBounds")
								.detail("clusterTime", item.first)
								.detail("clusterVersion", item.second)
								.detail("localTime", it->first)
								.detail("localVersion", it->second);
						return false;
					}
				}

				TraceEvent(SevInfo, "TKCorrectness_passed");
				return true;
			} catch (Error & e) {
				Void _ = wait(tr->onError(e));
			}
		}
	}

	virtual Future<bool> check( Database const& cx ) {
		return _check(cx, this);
	}
};

WorkloadFactory<TimeKeeperCorrectnessWorkload> TimeKeeperCorrectnessWorkloadFactory("TimeKeeperCorrectness");
