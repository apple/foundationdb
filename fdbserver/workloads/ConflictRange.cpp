/*
 * ConflictRange.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbclient/ManagementAPI.h"

// For this test to report properly buggify must be disabled (flow.h) , and failConnection must be disabled in
// (sim2.cpp)

struct ConflictRangeWorkload : TestWorkload {
	static constexpr auto NAME = "ConflictRange";
	int minOperationsPerTransaction, maxOperationsPerTransaction, maxKeySpace, maxOffset, minInitialAmount,
	    maxInitialAmount;
	double testDuration;
	bool testReadYourWrites;

	std::vector<Future<Void>> clients;
	PerfIntCounter withConflicts, withoutConflicts, retries;

	// This workload is not compatible with RandomRangeLock workload because RangeLock transaction triggers conflicts
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomRangeLock" });
	}

	ConflictRangeWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), withConflicts("WithConflicts"), withoutConflicts("withoutConflicts"), retries("Retries") {
		minOperationsPerTransaction = getOption(options, "minOperationsPerTransaction"_sr, 2);
		maxOperationsPerTransaction = getOption(options, "minOperationsPerTransaction"_sr, 4);
		maxKeySpace = getOption(options, "maxKeySpace"_sr, 100);
		maxOffset = getOption(options, "maxOffset"_sr, 5);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		testReadYourWrites = getOption(options, "testReadYourWrites"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(withConflicts.getMetric());
		m.push_back(withoutConflicts.getMetric());
		m.push_back(retries.getMetric());
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			co_await timeout(conflictRangeClient(cx, this), testDuration, Void());
	}

	Future<Void> conflictRangeClient(Database cx, ConflictRangeWorkload* self) {
		std::string clientID;
		std::string myKeyA;
		std::string myKeyB;
		std::string myValue;
		bool onEqualA{ false };
		bool onEqualB{ false };
		int offsetA{ 0 };
		int offsetB{ 0 };
		int randomLimit{ 0 };
		Reverse reverse = Reverse::False;
		bool randomSets = false;
		std::set<int> insertedSet;
		RangeResult originalResults;
		Standalone<StringRef> firstElement;

		std::set<int> clearedSet;
		int clearedBegin{ 0 };
		int clearedEnd{ 0 };

		if (g_network->isSimulated()) {
			co_await timeKeeperSetDisable(cx);
		}

		// Set one key after the end of the tested range. If this key is included in the result, then
		// we may have drifted into the system key-space and cannot evaluate the result.
		Key sentinelKey = StringRef(format("%010d", self->maxKeySpace));

		while (true) {
			randomSets = !randomSets;

			// Initialize the database with random values.
			while (true) {
				Transaction tr0(cx);
				Error err;
				try {
					TraceEvent("ConflictRangeReset").log();
					insertedSet.clear();

					if (self->testReadYourWrites) {
						clearedSet.clear();
						int clearedA = deterministicRandom()->randomInt(0, self->maxKeySpace - 1);
						int clearedB = deterministicRandom()->randomInt(0, self->maxKeySpace - 1);
						clearedBegin = std::min(clearedA, clearedB);
						clearedEnd = std::max(clearedA, clearedB) + 1;
						TraceEvent("ConflictRangeClear").detail("Begin", clearedBegin).detail("End", clearedEnd);
					}

					tr0.clear(
					    KeyRangeRef(StringRef(format("%010d", 0)), StringRef(format("%010d", self->maxKeySpace))));
					for (int i = 0; i < self->maxKeySpace; i++) {
						if (deterministicRandom()->random01() > 0.5) {
							TraceEvent("ConflictRangeInit").detail("Key", i);
							if (self->testReadYourWrites && i >= clearedBegin && i < clearedEnd)
								clearedSet.insert(i);
							else {
								insertedSet.insert(i);
								tr0.set(StringRef(format("%010d", i)),
								        deterministicRandom()->randomUniqueID().toString());
							}
						}
					}

					tr0.set(sentinelKey, deterministicRandom()->randomUniqueID().toString());

					co_await tr0.commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr0.onError(err);
			}

			firstElement = Key(StringRef(format("%010d", *(insertedSet.begin()))));

			Transaction tr1(cx);
			Transaction tr2(cx);
			Transaction tr3(cx);
			Transaction tr4(cx);
			ReadYourWritesTransaction trRYOW(cx);

			Error err;
			try {
				// Generate a random getRange operation and execute it, if it produces results, save them, otherwise
				// retry.
				while (true) {
					myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace));
					myKeyB = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace));
					onEqualA = deterministicRandom()->randomInt(0, 2) != 0;
					onEqualB = deterministicRandom()->randomInt(0, 2) != 0;
					offsetA = deterministicRandom()->randomInt(-1 * self->maxOffset, self->maxOffset);
					offsetB = deterministicRandom()->randomInt(-1 * self->maxOffset, self->maxOffset);
					randomLimit = deterministicRandom()->randomInt(1, self->maxKeySpace);
					reverse.set(deterministicRandom()->coinflip());

					RangeResult res = co_await tr1.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
					                                        KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
					                                        randomLimit,
					                                        Snapshot::False,
					                                        reverse);
					if (!res.empty()) {
						originalResults = res;
						break;
					}
					tr1 = Transaction(cx);
				}

				if (self->testReadYourWrites) {
					for (auto iter = clearedSet.begin(); iter != clearedSet.end(); ++iter)
						tr1.set(StringRef(format("%010d", (*iter))),
						        deterministicRandom()->randomUniqueID().toString());
					co_await tr1.commit();
					tr1 = Transaction(cx);
				}

				// Create two transactions with the same read version
				Version readVersion = co_await tr2.getReadVersion();

				if (self->testReadYourWrites) {
					trRYOW.setVersion(readVersion);
				} else
					tr3.setVersion(readVersion);

				// Do random operations in one of the transactions and commit.
				// Either do all sets in locations without existing data or all clears in locations with data.
				for (int i = 0; i < deterministicRandom()->randomInt(self->minOperationsPerTransaction,
				                                                     self->maxOperationsPerTransaction + 1);
				     i++) {
					if (randomSets) {
						for (int j = 0; j < 5; j++) {
							int proposedKey = deterministicRandom()->randomInt(0, self->maxKeySpace);
							if (!insertedSet.contains(proposedKey)) {
								TraceEvent("ConflictRangeSet").detail("Key", proposedKey);
								insertedSet.insert(proposedKey);
								tr2.set(StringRef(format("%010d", proposedKey)),
								        deterministicRandom()->randomUniqueID().toString());
								break;
							}
						}
					} else {
						for (int j = 0; j < 5; j++) {
							int proposedKey = deterministicRandom()->randomInt(0, self->maxKeySpace);
							if (insertedSet.contains(proposedKey)) {
								TraceEvent("ConflictRangeClear").detail("Key", proposedKey);
								insertedSet.erase(proposedKey);
								tr2.clear(StringRef(format("%010d", proposedKey)));
								break;
							}
						}
					}
				}

				co_await tr2.commit();

				bool foundConflict = false;
				try {
					// Do the generated getRange in the other transaction and commit.
					if (self->testReadYourWrites) {
						trRYOW.clear(KeyRangeRef(StringRef(format("%010d", clearedBegin)),
						                         StringRef(format("%010d", clearedEnd))));
						RangeResult res = co_await trRYOW.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
						                                           KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
						                                           randomLimit,
						                                           Snapshot::False,
						                                           reverse);
						co_await trRYOW.commit();
					} else {
						tr3.clear(StringRef(format("%010d", self->maxKeySpace + 1)));
						RangeResult res = co_await tr3.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
						                                        KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
						                                        randomLimit,
						                                        Snapshot::False,
						                                        reverse);
						co_await tr3.commit();
					}
				} catch (Error& e) {
					if (e.code() != error_code_not_committed)
						throw e;
					foundConflict = true;
				}

				if (foundConflict) {
					// If the commit fails, do the getRange again and check that the results are different from the
					// first execution.
					if (self->testReadYourWrites) {
						tr1.clear(KeyRangeRef(StringRef(format("%010d", clearedBegin)),
						                      StringRef(format("%010d", clearedEnd))));
						co_await tr1.commit();
						tr1 = Transaction(cx);
					}

					RangeResult res = co_await tr4.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
					                                        KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
					                                        randomLimit,
					                                        Snapshot::False,
					                                        reverse);
					++self->withConflicts;

					if (res.size() == originalResults.size()) {
						for (int i = 0; i < res.size(); i++)
							if (res[i] != originalResults[i])
								throw not_committed();

						// Discard known cases where conflicts do not change the results
						if (originalResults.size() == randomLimit &&
						    ((offsetB <= 0 && !reverse) || (offsetA > 1 && reverse))) {
							// Hit limit but end offset goes into the range, so changes could effect results even
							// though in this instance they did not
							throw not_committed();
						}

						KeyRef smallestResult = originalResults[0].key;
						KeyRef largestResult = originalResults[originalResults.size() - 1].key;
						if (reverse) {
							std::swap(smallestResult, largestResult);
						}

						if (largestResult >= sentinelKey) {
							// Results go into server keyspace, so if a key selector does not fully resolve offset,
							// a change won't effect results
							throw not_committed();
						}

						if ((smallestResult == firstElement ||
						     smallestResult == StringRef(format("%010d", *(insertedSet.begin())))) &&
						    offsetA < 0) {
							// Results return the first element, and the begin offset is negative, so if a key
							// selector does not fully resolve the offset, a change won't effect results
							throw not_committed();
						}

						if ((myKeyA > myKeyB || (myKeyA == myKeyB && onEqualA && !onEqualB)) &&
						    originalResults.size() == randomLimit) {
							// The begin key is less than the end key, so changes in this range only effect the end
							// key selector, but because we hit the limit this does not change the results
							throw not_committed();
						}

						std::string keyStr1 = "";
						for (int i = 0; i < res.size(); i++) {
							keyStr1 += printable(res[i].key) + " ";
						}

						std::string keyStr2 = "";
						for (int i = 0; i < originalResults.size(); i++) {
							keyStr2 += printable(originalResults[i].key) + " ";
						}

						TraceEvent(SevError, "ConflictRangeError")
						    .detail("Info", "Conflict returned, however results are the same")
						    .detail("RandomSets", randomSets)
						    .detail("MyKeyA", myKeyA)
						    .detail("MyKeyB", myKeyB)
						    .detail("OnEqualA", onEqualA)
						    .detail("OnEqualB", onEqualB)
						    .detail("OffsetA", offsetA)
						    .detail("OffsetB", offsetB)
						    .detail("RandomLimit", randomLimit)
						    .detail("Reverse", reverse)
						    .detail("Size", originalResults.size())
						    .detail("Results", keyStr1)
						    .detail("Original", keyStr2);

						tr4 = Transaction(cx);
						RangeResult res = co_await tr4.getRange(
						    KeyRangeRef(StringRef(format("%010d", 0)), StringRef(format("%010d", self->maxKeySpace))),
						    200);
						std::string allKeyEntries = "";
						for (int i = 0; i < res.size(); i++) {
							allKeyEntries += printable(res[i].key) + " ";
						}

						TraceEvent("ConflictRangeDump").setMaxFieldLength(10000).detail("Keys", allKeyEntries);
					}
					throw not_committed();
				} else {
					// If the commit is successful, check that the result matches the first execution.
					RangeResult res = co_await tr4.getRange(KeySelectorRef(StringRef(myKeyA), onEqualA, offsetA),
					                                        KeySelectorRef(StringRef(myKeyB), onEqualB, offsetB),
					                                        randomLimit,
					                                        Snapshot::False,
					                                        reverse);
					++self->withoutConflicts;

					if (res.size() == originalResults.size()) {
						for (int i = 0; i < res.size(); i++) {
							if (res[i] != originalResults[i] &&
							    !(res[i].key.startsWith("\xff"_sr) && originalResults[i].key.startsWith("\xff"_sr))) {
								TraceEvent(SevError, "ConflictRangeError")
								    .detail("Info", "No conflict returned, however results do not match")
								    .detail("Original",
								            printable(originalResults[i].key) + " " +
								                printable(originalResults[i].value))
								    .detail("New", printable(res[i].key) + " " + printable(res[i].value));
							}
						}
					} else {
						std::string keyStr1 = "";
						for (int i = 0; i < res.size(); i++) {
							keyStr1 += printable(res[i].key) + " ";
						}

						std::string keyStr2 = "";
						for (int i = 0; i < originalResults.size(); i++) {
							keyStr2 += printable(originalResults[i].key) + " ";
						}

						TraceEvent(SevError, "ConflictRangeError")
						    .detail("Info", "No conflict returned, however result sizes do not match")
						    .detail("OriginalSize", originalResults.size())
						    .detail("NewSize", res.size())
						    .detail("RandomSets", randomSets)
						    .detail("MyKeyA", myKeyA)
						    .detail("MyKeyB", myKeyB)
						    .detail("OnEqualA", onEqualA)
						    .detail("OnEqualB", onEqualB)
						    .detail("OffsetA", offsetA)
						    .detail("OffsetB", offsetB)
						    .detail("RandomLimit", randomLimit)
						    .detail("Reverse", reverse)
						    .detail("Size", originalResults.size())
						    .detail("Results", keyStr1)
						    .detail("Original", keyStr2);
					}
				}
			} catch (Error& e) {
				err = e;
			}
			if (!err.isValid()) {
				continue;
			}
			if (err.code() != error_code_not_committed)
				++self->retries;

			co_await tr1.onError(err);
			co_await tr2.onError(err);
			co_await tr3.onError(err);
			co_await tr4.onError(err);
			co_await trRYOW.onError(err);
		}
	}
};

WorkloadFactory<ConflictRangeWorkload> ConflictRangeWorkloadFactory;
