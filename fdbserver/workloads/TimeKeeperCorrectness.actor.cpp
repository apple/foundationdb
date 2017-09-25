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
		state Future<Void> testCompleted = delay(self->testDuration);

		TraceEvent(SevInfo, "TKCorrectness start");

		while (!testCompleted.isReady()) {
			state Transaction tr(cx);

			try {
				Version v = wait(tr.getReadVersion());
				self->inMemTimeKeeper[(int64_t) now()] = v;
			} catch (Error & e) {
				Void _ = wait(tr.onError(e));
			}

			// For every sample from Timekeeper collect two samples here.
			Void _ = wait(delay(SERVER_KNOBS->TIME_KEEPER_DELAY / 2));
		}

		TraceEvent(SevInfo, "TKCorrectness completed");
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		return _start(cx, this);
	}

	ACTOR static Future<bool> _check(Database cx, TimeKeeperCorrectnessWorkload *self) {
		state KeyBackedMap<int64_t, Version> dbTimeKeeper = KeyBackedMap<int64_t, Version>(timeKeeperPrefixRange.begin);
		state Reference<ReadYourWritesTransaction> tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));

		TraceEvent(SevInfo, "TKCorrectness check start")
				.detail("TIME_KEPER_MAX_ENTRIES", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES)
				.detail("TIME_KEEPER_DELAY", SERVER_KNOBS->TIME_KEEPER_DELAY);

		if (SERVER_KNOBS->TIME_KEEPER_DELAY < 2) {
			TraceEvent(SevError, "TKCorrectness too small delay").detail("found", SERVER_KNOBS->TIME_KEEPER_DELAY);
			return false;
		}

		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				std::vector<std::pair<int64_t, Version>> futureItems = wait(
						dbTimeKeeper.getRange(tr, ((int64_t) now()) + 1, Optional<int64_t>(), 1));
				if (!futureItems.empty()) {
					TraceEvent(SevError, "TKCorrectness FutureMappings").detail("count", futureItems.empty());
					return false;
				}

				std::vector<std::pair<int64_t, Version>> allItems = wait(
						dbTimeKeeper.getRange(tr, 0, Optional<int64_t>(), self->inMemTimeKeeper.size() + 2));
				for (auto item : allItems) {
					self->inMemTimeKeeper[item.first] = item.second;
				}

				if (allItems.size() > SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES) {
					TraceEvent(SevError, "TKCorrectness too many entries")
							.detail("expected", SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES)
							.detail("found", allItems.size());
					return false;
				}

				if (allItems.size() < self->testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY) {
					TraceEvent(SevError, "TKCorrectness too few entries")
							.detail("expected", self->testDuration / SERVER_KNOBS->TIME_KEEPER_DELAY)
							.detail("found", allItems.size());
					return false;
				}

				bool first = true;
				std::pair<int64_t, Version> prevItem;
				for (auto item : self->inMemTimeKeeper) {
					if (first) {
						first = false;
					} else if (prevItem.second > item.second) {
						TraceEvent(SevError, "TKCorrectness time mismatch")
								.detail("prevVersion", prevItem.second)
								.detail("currentVersion", item.second);
						return false;
					}

					prevItem = item;
				}

				TraceEvent(SevInfo, "TKCorrectness passed");
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
