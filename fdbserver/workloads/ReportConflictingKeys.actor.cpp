/*
 * ReportConflictingKeys.actor.cpp
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
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// For this test to report properly buggify must be disabled (flow.h) , and failConnection must be disabled in
// (sim2.actor.cpp)
struct ReportConflictingKeysWorkload : TestWorkload {
	static constexpr auto NAME = "ReportConflictingKeys";

	double testDuration, transactionsPerSecond, addReadConflictRangeProb, addWriteConflictRangeProb;
	Key keyPrefix;

	int nodeCount, actorCount, keyBytes, valueBytes, readConflictRangeCount, writeConflictRangeCount;

	PerfIntCounter invalidReports, commits, conflicts, xacts;

	ReportConflictingKeysWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), invalidReports("InvalidReports"), commits("commits"), conflicts("Conflicts"),
	    xacts("Transactions") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		// transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 5000.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, 1);
		keyPrefix = unprintable(getOption(options, "keyPrefix"_sr, "ReportConflictingKeysWorkload"_sr).toString());
		keyBytes = getOption(options, "keyBytes"_sr, 64);

		readConflictRangeCount = getOption(options, "readConflictRangeCountPerTx"_sr, 1);
		writeConflictRangeCount = getOption(options, "writeConflictRangeCountPerTx"_sr, 1);
		ASSERT(readConflictRangeCount >= 1 && writeConflictRangeCount >= 1);
		// modeled by geometric distribution: (1 - prob) / prob = mean - 1, where we add at least one conflictRange to
		// each tx
		addReadConflictRangeProb = (readConflictRangeCount - 1.0) / readConflictRangeCount;
		addWriteConflictRangeProb = (writeConflictRangeCount - 1.0) / writeConflictRangeCount;
		ASSERT(keyPrefix.size() + 8 <= keyBytes); // make sure the string format is valid
		nodeCount = getOption(options, "nodeCount"_sr, 100);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(const Database& cx) override { return _start(cx->clone(), this); }

	ACTOR Future<Void> _start(Database cx, ReportConflictingKeysWorkload* self) {
		wait(timeout(self->conflictingClient(cx, self), self->testDuration, Void()));
		return Void();
	}

	Future<bool> check(Database const& cx) override { return invalidReports.getValue() == 0; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Measured Duration", testDuration, Averaged::True);
		m.push_back(xacts.getMetric());
		m.emplace_back("Transactions/sec", xacts.getValue() / testDuration, Averaged::True);
		m.push_back(commits.getMetric());
		m.emplace_back("Commits/sec", commits.getValue() / testDuration, Averaged::True);
		m.push_back(conflicts.getMetric());
		m.emplace_back("Conflicts/sec", conflicts.getValue() / testDuration, Averaged::True);
	}

	// disable the default timeout setting
	double getCheckTimeout() const override { return std::numeric_limits<double>::max(); }

	// Copied from tester.actor.cpp, added parameter to determine the key's length
	Key keyForIndex(int n) {
		double p = (double)n / nodeCount;
		// 8 bytes for Cid_* suffix of each client
		int paddingLen = keyBytes - 8 - keyPrefix.size();
		// left padding by zero, each client has different prefix
		Key prefixWithClientId = StringRef(format("Cid_%04d", clientId)).withPrefix(keyPrefix);
		return StringRef(format("%0*llx", paddingLen, *(uint64_t*)&p)).withPrefix(prefixWithClientId);
	}

	void addRandomReadConflictRange(ReadYourWritesTransaction* tr, std::vector<KeyRange>* readConflictRanges) {
		int startIdx, endIdx;
		Key startKey, endKey;
		do { // add at least one non-empty range
			startIdx = deterministicRandom()->randomInt(0, nodeCount);
			endIdx = deterministicRandom()->randomInt(startIdx + 1, nodeCount + 1);
			startKey = keyForIndex(startIdx);
			endKey = keyForIndex(endIdx);
			ASSERT(startKey < endKey);
			tr->addReadConflictRange(KeyRangeRef(startKey, endKey));
			if (readConflictRanges)
				readConflictRanges->push_back(KeyRangeRef(startKey, endKey));
		} while (deterministicRandom()->random01() < addReadConflictRangeProb);
	}

	void addRandomWriteConflictRange(ReadYourWritesTransaction* tr, std::vector<KeyRange>* writeConflictRanges) {
		int startIdx, endIdx;
		Key startKey, endKey;
		do { // add at least one non-empty range
			startIdx = deterministicRandom()->randomInt(0, nodeCount);
			endIdx = deterministicRandom()->randomInt(startIdx + 1, nodeCount + 1);
			startKey = keyForIndex(startIdx);
			endKey = keyForIndex(endIdx);
			ASSERT(startKey < endKey);
			tr->addWriteConflictRange(KeyRangeRef(startKey, endKey));
			if (writeConflictRanges)
				writeConflictRanges->push_back(KeyRangeRef(startKey, endKey));
		} while (deterministicRandom()->random01() < addWriteConflictRangeProb);
	}

	void emptyConflictingKeysTest(const Reference<ReadYourWritesTransaction>& ryw) {
		// This test is called when you want to make sure there is no conflictingKeys,
		// which means you will get an empty result form getRange(\xff\xff/transaction/conflicting_keys/,
		// \xff\xff/transaction/conflicting_keys0)
		auto resultFuture = ryw->getRange(conflictingKeysRange, CLIENT_KNOBS->TOO_MANY);
		auto result = resultFuture.get();
		ASSERT(!result.more && result.size() == 0);
	}

	ACTOR Future<Void> conflictingClient(Database cx, ReportConflictingKeysWorkload* self) {

		state Reference<ReadYourWritesTransaction> tr1(new ReadYourWritesTransaction(cx));
		state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(cx));
		state std::vector<KeyRange> readConflictRanges;
		state std::vector<KeyRange> writeConflictRanges;

		loop {
			try {
				// set the flag for empty key range testing
				tr1->setOption(FDBTransactionOptions::REPORT_CONFLICTING_KEYS);
				// tr1 should never have conflicting keys, the result should always be empty
				self->emptyConflictingKeysTest(tr1);

				tr2->setOption(FDBTransactionOptions::REPORT_CONFLICTING_KEYS);
				// If READ_YOUR_WRITES_DISABLE set, it behaves like native transaction object
				// where overlapped conflict ranges are not merged.
				if (deterministicRandom()->coinflip())
					tr1->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				if (deterministicRandom()->coinflip())
					tr2->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				// We have the two tx with same grv, then commit the first
				// If the second one is not able to commit due to conflicts, verify the returned conflicting keys
				// Otherwise, there is no conflicts between tr1's writeConflictRange and tr2's readConflictRange
				Version readVersion = wait(tr1->getReadVersion());
				tr2->setVersion(readVersion);
				self->addRandomReadConflictRange(tr1.getPtr(), nullptr);
				self->addRandomWriteConflictRange(tr1.getPtr(), &writeConflictRanges);
				++self->commits;
				wait(tr1->commit());
				++self->xacts;
				// tr1 should never have conflicting keys, test again after the commit
				self->emptyConflictingKeysTest(tr1);

				state bool foundConflict = false;
				try {
					self->addRandomReadConflictRange(tr2.getPtr(), &readConflictRanges);
					self->addRandomWriteConflictRange(tr2.getPtr(), nullptr);
					++self->commits;
					wait(tr2->commit());
					++self->xacts;
				} catch (Error& e) {
					if (e.code() != error_code_not_committed)
						throw e;
					foundConflict = true;
					++self->conflicts;
				}
				// These two conflict sets should not be empty
				ASSERT(readConflictRanges.size());
				ASSERT(writeConflictRanges.size());
				// check API correctness
				if (foundConflict) {
					// \xff\xff/transaction/conflicting_keys is always initialized to false, skip it here
					state KeyRange ckr = KeyRangeRef(keyAfter(""_sr.withPrefix(conflictingKeysRange.begin)),
					                                 "\xff\xff"_sr.withPrefix(conflictingKeysRange.begin));
					// The getRange here using the special key prefix "\xff\xff/transaction/conflicting_keys/" happens
					// locally Thus, the error handling is not needed here
					state Future<RangeResult> conflictingKeyRangesFuture = tr2->getRange(ckr, CLIENT_KNOBS->TOO_MANY);
					ASSERT(conflictingKeyRangesFuture.isReady());

					wait(validateSpecialSubrangeRead(tr2.getPtr(),
					                                 firstGreaterOrEqual(ckr.begin),
					                                 firstGreaterOrEqual(ckr.end),
					                                 GetRangeLimits(),
					                                 Reverse::False,
					                                 conflictingKeyRangesFuture.get()));

					tr2 = makeReference<ReadYourWritesTransaction>(cx);

					const RangeResult conflictingKeyRanges = conflictingKeyRangesFuture.get();
					ASSERT(conflictingKeyRanges.size() &&
					       (conflictingKeyRanges.size() <= readConflictRanges.size() * 2));
					ASSERT(conflictingKeyRanges.size() % 2 == 0);
					ASSERT(!conflictingKeyRanges.more);
					for (int i = 0; i < conflictingKeyRanges.size(); i += 2) {
						KeyValueRef startKeyWithPrefix = conflictingKeyRanges[i];
						ASSERT(startKeyWithPrefix.key.startsWith(conflictingKeysRange.begin));
						ASSERT(startKeyWithPrefix.value == conflictingKeysTrue);
						KeyValueRef endKeyWithPrefix = conflictingKeyRanges[i + 1];
						ASSERT(endKeyWithPrefix.key.startsWith(conflictingKeysRange.begin));
						ASSERT(endKeyWithPrefix.value == conflictingKeysFalse);
						// Remove the prefix of returning keys
						Key startKey = startKeyWithPrefix.key.removePrefix(conflictingKeysRange.begin);
						Key endKey = endKeyWithPrefix.key.removePrefix(conflictingKeysRange.begin);
						KeyRangeRef kr = KeyRangeRef(startKey, endKey);
						if (!std::any_of(readConflictRanges.begin(), readConflictRanges.end(), [&kr](KeyRange rCR) {
							    // Read_conflict_range remains same in the resolver.
							    // Thus, the returned keyrange is either the original read_conflict_range or merged
							    // by several overlapped ones in either cases, it contains at least one original
							    // read_conflict_range
							    return kr.contains(rCR);
						    })) {
							++self->invalidReports;
							std::string allReadConflictRanges = "";
							for (int i = 0; i < readConflictRanges.size(); i++) {
								allReadConflictRanges += "Begin:" + printable(readConflictRanges[i].begin) +
								                         ", End:" + printable(readConflictRanges[i].end) + "; ";
							}
							TraceEvent(SevError, "TestFailure")
							    .detail("Reason",
							            "Returned conflicting keys are not original or merged readConflictRanges")
							    .detail("ConflictingKeyRange", kr.toString())
							    .detail("ReadConflictRanges", allReadConflictRanges);
						} else if (!std::any_of(
						               writeConflictRanges.begin(), writeConflictRanges.end(), [&kr](KeyRange wCR) {
							               // Returned key range should be conflicting with at least one
							               // writeConflictRange
							               return kr.intersects(wCR);
						               })) {
							++self->invalidReports;
							std::string allWriteConflictRanges = "";
							for (int i = 0; i < writeConflictRanges.size(); i++) {
								allWriteConflictRanges += "Begin:" + printable(writeConflictRanges[i].begin) +
								                          ", End:" + printable(writeConflictRanges[i].end) + "; ";
							}
							TraceEvent(SevError, "TestFailure")
							    .detail("Reason", "Returned keyrange is not conflicting with any writeConflictRange")
							    .detail("ConflictingKeyRange", kr.toString())
							    .detail("WriteConflictRanges", allWriteConflictRanges);
						}
					}
				} else {
					// make sure no conflicts between tr2's readConflictRange and tr1's writeConflictRange
					for (const KeyRange& rCR : readConflictRanges) {
						if (std::any_of(writeConflictRanges.begin(), writeConflictRanges.end(), [&rCR](KeyRange wCR) {
							    bool result = wCR.intersects(rCR);
							    if (result)
								    TraceEvent(SevError, "TestFailure")
								        .detail("Reason", "No conflicts returned but it should")
								        .detail("WriteConflictRangeInTr1", wCR.toString())
								        .detail("ReadConflictRangeInTr2", rCR.toString());
							    return result;
						    })) {
							++self->invalidReports;
							std::string allReadConflictRanges = "";
							for (int i = 0; i < readConflictRanges.size(); i++) {
								allReadConflictRanges += "Begin:" + printable(readConflictRanges[i].begin) +
								                         ", End:" + printable(readConflictRanges[i].end) + "; ";
							}
							std::string allWriteConflictRanges = "";
							for (int i = 0; i < writeConflictRanges.size(); i++) {
								allWriteConflictRanges += "Begin:" + printable(writeConflictRanges[i].begin) +
								                          ", End:" + printable(writeConflictRanges[i].end) + "; ";
							}
							TraceEvent(SevError, "TestFailure")
							    .detail("Reason", "No conflicts returned but it should")
							    .detail("ReadConflictRanges", allReadConflictRanges)
							    .detail("WriteConflictRanges", allWriteConflictRanges);
							break;
						}
					}
				}
			} catch (Error& e) {
				state Error e2 = e;
				wait(tr1->onError(e2));
				wait(tr2->onError(e2));
			}
			readConflictRanges.clear();
			writeConflictRanges.clear();
			tr1->reset();
			tr2->reset();
		}
	}
};

WorkloadFactory<ReportConflictingKeysWorkload> ReportConflictingKeysWorkload;
