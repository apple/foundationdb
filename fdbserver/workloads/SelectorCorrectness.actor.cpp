/*
 * SelectorCorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SelectorCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "SelectorCorrectness";

	int minOperationsPerTransaction, maxOperationsPerTransaction, maxKeySpace, maxOffset;
	bool testReadYourWrites;
	double testDuration;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;

	SelectorCorrectnessWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries") {

		minOperationsPerTransaction = getOption(options, "minOperationsPerTransaction"_sr, 10);
		maxOperationsPerTransaction = getOption(options, "minOperationsPerTransaction"_sr, 50);
		maxKeySpace = getOption(options, "maxKeySpace"_sr, 10);
		maxOffset = getOption(options, "maxOffset"_sr, 20);
		testReadYourWrites = getOption(options, "testReadYourWrites"_sr, true);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override { return SelectorCorrectnessSetup(cx->clone(), this); }

	Future<Void> start(Database const& cx) override {
		clients.push_back(timeout(SelectorCorrectnessClient(cx->clone(), this), testDuration, Void()));
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
	}

	ACTOR Future<Void> SelectorCorrectnessSetup(Database cx, SelectorCorrectnessWorkload* self) {
		state Value myValue = StringRef(format("%010d", deterministicRandom()->randomInt(0, 10000000)));
		state Transaction tr(cx);

		if (!self->testReadYourWrites) {
			loop {
				try {
					for (int i = 0; i < self->maxKeySpace; i += 2)
						tr.set(StringRef(format("%010d", i)), myValue);

					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		} else {
			loop {
				try {
					for (int i = 0; i < self->maxKeySpace; i += 4)
						tr.set(StringRef(format("%010d", i)), myValue);
					for (int i = 2; i < self->maxKeySpace; i += 4)
						if (deterministicRandom()->random01() > 0.5)
							tr.set(StringRef(format("%010d", i)), myValue);

					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		return Void();
	}

	ACTOR Future<Void> SelectorCorrectnessClient(Database cx, SelectorCorrectnessWorkload* self) {
		state int i;
		state int j;
		state std::string myKeyA;
		state std::string myKeyB;
		state Value myValue;
		state bool onEqualA;
		state bool onEqualB;
		state int offsetA;
		state int offsetB;
		state Standalone<StringRef> maxKey;
		state Reverse reverse = Reverse::False;

		maxKey = Standalone<StringRef>(format("%010d", self->maxKeySpace + 1));

		loop {

			state Transaction tr(cx);
			state ReadYourWritesTransaction trRYOW(cx);

			if (self->testReadYourWrites) {
				myValue = StringRef(format("%010d", deterministicRandom()->randomInt(0, 10000000)));
				for (int i = 2; i < self->maxKeySpace; i += 4)
					trRYOW.set(StringRef(format("%010d", i)), myValue);
				for (int i = 0; i < self->maxKeySpace; i += 4)
					if (deterministicRandom()->random01() > 0.5)
						trRYOW.set(StringRef(format("%010d", i)), myValue);
			}

			try {
				for (i = 0; i < deterministicRandom()->randomInt(self->minOperationsPerTransaction,
				                                                 self->maxOperationsPerTransaction + 1);
				     i++) {
					j = deterministicRandom()->randomInt(0, 2);
					if (j < 1) {
						state int searchInt = deterministicRandom()->randomInt(0, self->maxKeySpace);
						myKeyA = format("%010d", searchInt);

						if (self->testReadYourWrites) {
							Optional<Value> getTest = wait(trRYOW.get(StringRef(myKeyA)));
							if ((searchInt % 2 == 0 && !getTest.present()) ||
							    (searchInt % 2 == 1 && getTest.present())) {
								TraceEvent(SevError, "RanSelTestFailure")
								    .detail("Reason", "Value not present")
								    .detail("KeyA", myKeyA);
							}
						} else {
							Optional<Value> getTest = wait(tr.get(StringRef(myKeyA)));
							if ((searchInt % 2 == 0 && !getTest.present()) ||
							    (searchInt % 2 == 1 && getTest.present())) {
								TraceEvent(SevError, "RanSelTestFailure")
								    .detail("Reason", "Value not present")
								    .detail("KeyA", myKeyA);
							}
						}
					} else {
						int a = deterministicRandom()->randomInt(2, self->maxKeySpace);
						int b = deterministicRandom()->randomInt(2, 2 * self->maxKeySpace);
						int abmax = std::max(a, b);
						int abmin = std::min(a, b) - 1;
						myKeyA = format("%010d", abmin);
						myKeyB = format("%010d", abmax);
						onEqualA = deterministicRandom()->randomInt(0, 2) != 0;
						onEqualB = deterministicRandom()->randomInt(0, 2) != 0;
						offsetA = 1; //-1*deterministicRandom()->randomInt( 0, self->maxOffset );
						offsetB = deterministicRandom()->randomInt(1, self->maxOffset);
						reverse.set(deterministicRandom()->coinflip());

						//TraceEvent("RYOWgetRange").detail("KeyA", myKeyA).detail("KeyB", myKeyB).detail("OnEqualA",onEqualA).detail("OnEqualB",onEqualB).detail("OffsetA",offsetA).detail("OffsetB",offsetB).detail("Direction",direction);
						state int expectedSize =
						    (std::min(abmax + 2 * offsetB - (abmax % 2 == 1 ? 1 : (onEqualB ? 0 : 2)),
						              self->maxKeySpace) -
						     (std::max(abmin + 2 * offsetA - (abmin % 2 == 1 ? 1 : (onEqualA ? 0 : 2)), 0))) /
						    2;

						if (self->testReadYourWrites) {
							RangeResult getRangeTest =
							    wait(trRYOW.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
							                         KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
							                         2 * (self->maxKeySpace + self->maxOffset),
							                         Snapshot::False,
							                         reverse));

							int trueSize = 0;
							while (trueSize < getRangeTest.size() &&
							       getRangeTest[!reverse ? trueSize : getRangeTest.size() - trueSize - 1].key < maxKey)
								trueSize++;

							if (trueSize != expectedSize) {
								std::string outStr = "";
								for (int k = 0; k < trueSize; k++) {
									std::string keyStr =
									    printable(getRangeTest[!reverse ? k : getRangeTest.size() - k - 1].key);
									outStr = outStr + keyStr + " ";
								}

								TraceEvent(SevError, "RanSelTestFailure")
								    .detail("Reason", "The getRange results did not match expected size")
								    .detail("Size", trueSize)
								    .detail("Expected", expectedSize)
								    .detail("Data", outStr)
								    .detail("DataSize", getRangeTest.size());
							}
						} else {
							RangeResult getRangeTest =
							    wait(tr.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
							                     KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
							                     2 * (self->maxKeySpace + self->maxOffset),
							                     Snapshot::False,
							                     reverse));

							int trueSize = 0;
							while (trueSize < getRangeTest.size() &&
							       getRangeTest[!reverse ? trueSize : getRangeTest.size() - trueSize - 1].key < maxKey)
								trueSize++;

							if (trueSize != expectedSize) {
								std::string outStr = "";
								for (int k = 0; k < trueSize; k++) {
									std::string keyStr =
									    printable(getRangeTest[!reverse ? k : getRangeTest.size() - k - 1].key);
									outStr = outStr + keyStr + " ";
								}

								TraceEvent(SevError, "RanSelTestFailure")
								    .detail("Reason", "The getRange results did not match expected size")
								    .detail("Size", trueSize)
								    .detail("Expected", expectedSize)
								    .detail("Data", outStr)
								    .detail("DataSize", getRangeTest.size());
							}
						}
					}
				}

				tr.reset();
				trRYOW.reset();
				++self->transactions;
			} catch (Error& e) {
				wait(trRYOW.onError(e));
				trRYOW.reset();
				++self->retries;
			}
		}
	}
};

WorkloadFactory<SelectorCorrectnessWorkload> SelectorCorrectnessWorkloadFactory;
