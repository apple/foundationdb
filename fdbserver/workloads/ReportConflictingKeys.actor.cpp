/*
 * ReportConflictingKeys.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ReportConflictingKeysWorkload : TestWorkload {

	double testDuration, transactionsPerSecond, addReadConflictRangeProb, addWriteConflictRangeProb;
	Key keyPrefix;

	int nodeCountPerPrefix, actorCount, keyBytes, valueBytes, readConflictRangeCount, writeConflictRangeCount;
	bool reportConflictingKeys, skipCorrectnessCheck;
	uint64_t keyPrefixBytes, prefixCount;

	PerfIntCounter invalidReports, commits, conflicts, retries, xacts;

	ReportConflictingKeysWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), invalidReports("InvalidReports"), conflicts("Conflicts"), retries("Retries"),
	    commits("Commits"), xacts("Transactions") {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 5000.0) / clientCount;
		actorCount = getOption(options, LiteralStringRef("actorsPerClient"), transactionsPerSecond / 5);
		keyPrefix = unprintable(
		    getOption(options, LiteralStringRef("keyPrefix"), LiteralStringRef("ReportConflictingKeysWorkload"))
		        .toString());
		keyBytes = getOption(options, LiteralStringRef("keyBytes"), 16);

		readConflictRangeCount = getOption(options, LiteralStringRef("readConflictRangeCountPerTx"), 1);
		writeConflictRangeCount = getOption(options, LiteralStringRef("writeConflictRangeCountPerTx"), 1);
		// modeled by geometric distribution: (1 - prob) / prob = mean
		addReadConflictRangeProb = readConflictRangeCount / (readConflictRangeCount + 1.0);
		addWriteConflictRangeProb = writeConflictRangeCount / (writeConflictRangeCount + 1.0);
		// If true, store key ranges conflicting with other txs
		reportConflictingKeys = getOption(options, LiteralStringRef("reportConflictingKeys"), false);
		skipCorrectnessCheck = getOption(options, LiteralStringRef("skipCorrectnessCheck"), false);
		// used for generating keyPrefix
		keyPrefixBytes = getOption(options, LiteralStringRef("keyPrefixBytes"), 0);
		if (keyPrefixBytes) {
			prefixCount = 255 * std::round(std::exp2(8 * (keyPrefixBytes - 1)));
			ASSERT(keyPrefixBytes + 16 <= keyBytes);
		} else {
			ASSERT(keyPrefix.size() + 16 <= keyBytes); // make sure the string format is valid
		}
		nodeCountPerPrefix = getOption(options, LiteralStringRef("nodeCountPerPrefix"), 100);
	}

	std::string description() override { return "ReportConflictingKeysWorkload"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(const Database& cx) override { return _start(cx->clone(), this); }

	ACTOR Future<Void> _start(Database cx, ReportConflictingKeysWorkload* self) {
		std::vector<Future<Void>> clients;
		for (int c = 0; c < self->actorCount; ++c) {
			clients.push_back(self->conflictingClient(cx, self, self->actorCount / self->transactionsPerSecond, c));
		}

		wait(timeout(waitForAll(clients), self->testDuration, Void()));
		return Void();
	}

	Future<bool> check(Database const& cx) override { return invalidReports.getValue() == 0; }

	void getMetrics(vector<PerfMetric>& m) override {
		m.push_back(PerfMetric("Measured Duration", testDuration, true));
		m.push_back(xacts.getMetric());
		m.push_back(PerfMetric("Transactions/sec", xacts.getValue() / testDuration, true));
		m.push_back(commits.getMetric());
		m.push_back(PerfMetric("Commits/sec", commits.getValue() / testDuration, true));
		m.push_back(conflicts.getMetric());
		m.push_back(PerfMetric("Conflicts/sec", conflicts.getValue() / testDuration, true));
		m.push_back(retries.getMetric());
		m.push_back(PerfMetric("Retries/sec", retries.getValue() / testDuration, true));
	}

	// disable the default timeout setting
	double getCheckTimeout() override { return std::numeric_limits<double>::max(); }

	// Copied from tester.actor.cpp, added parameter to determine the key's length
	Key keyForIndex(int prefixIdx, int n) {
		double p = (double)n / nodeCountPerPrefix;
		int paddingLen = keyBytes - 16 - keyPrefixBytes;
		// left padding by zero
		return StringRef(format("%0*llx", paddingLen, *(uint64_t*)&p))
		    .withPrefix(prefixIdx >= 0 ? keyPrefixForIndex(prefixIdx) : keyPrefix);
	}

	Key keyPrefixForIndex(uint64_t n) {
		Key prefix = makeString(keyPrefixBytes);
		uint8_t* head = mutateString(prefix);
		memset(head, 0, keyPrefixBytes);
		int offset = keyPrefixBytes - 1;
		while (n) {
			*(head + offset) = static_cast<uint8_t>(n % 256);
			n /= 256;
			offset -= 1;
		}
		return prefix;
	}

	void addRandomReadConflictRange(ReadYourWritesTransaction* tr, std::vector<KeyRange>& readConflictRanges) {
		int startIdx, endIdx, startPrefixIdx, endPrefixIdx;
		Key startKey, endKey;
		while (deterministicRandom()->random01() < addReadConflictRangeProb) {
			startPrefixIdx = keyPrefixBytes ? deterministicRandom()->randomInt(0, prefixCount) : -1;
			endPrefixIdx = keyPrefixBytes ? deterministicRandom()->randomInt(startPrefixIdx, prefixCount) : -1;
			startIdx = deterministicRandom()->randomInt(0, nodeCountPerPrefix);
			endIdx = deterministicRandom()->randomInt(startPrefixIdx < endPrefixIdx ? 0 : startIdx, nodeCountPerPrefix);
			startKey = keyForIndex(startPrefixIdx, startIdx);
			endKey = keyForIndex(endPrefixIdx, endIdx);
			tr->addReadConflictRange(KeyRangeRef(startKey, endKey));
			readConflictRanges.push_back(KeyRangeRef(startKey, endKey));
		}
	}

	void addRandomWriteConflictRange(ReadYourWritesTransaction* tr) {
		int startIdx, endIdx, startPrefixIdx, endPrefixIdx;
		Key startKey, endKey;
		while (deterministicRandom()->random01() < addWriteConflictRangeProb) {
			startPrefixIdx = keyPrefixBytes ? deterministicRandom()->randomInt(0, prefixCount) : -1;
			endPrefixIdx = keyPrefixBytes ? deterministicRandom()->randomInt(startPrefixIdx, prefixCount) : -1;
			startIdx = deterministicRandom()->randomInt(0, nodeCountPerPrefix);
			endIdx = deterministicRandom()->randomInt(startPrefixIdx < endPrefixIdx ? 0 : startIdx, nodeCountPerPrefix);
			startKey = keyForIndex(startPrefixIdx, startIdx);
			endKey = keyForIndex(endPrefixIdx, endIdx);
			tr->addWriteConflictRange(KeyRangeRef(startKey, endKey));
		}
	}

	ACTOR Future<Void> conflictingClient(Database cx, ReportConflictingKeysWorkload* self, double delay,
	                                     int actorIndex) {

		state ReadYourWritesTransaction tr(cx);
		state double lastTime = now();
		state std::vector<KeyRange> readConflictRanges;

		loop {
			try {
				// used for throttling
				wait(poisson(&lastTime, delay));
				if (self->reportConflictingKeys) tr.setOption(FDBTransactionOptions::REPORT_CONFLICTING_KEYS);
				// If READ_YOUR_WRITES_DISABLE set, it behaves like native transaction object
				// where overlapped conflict ranges are not merged.
				if (deterministicRandom()->random01() < 0.5)
					tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				self->addRandomReadConflictRange(&tr, readConflictRanges);
				self->addRandomWriteConflictRange(&tr);
				++self->commits;
				wait(tr.commit());
				++self->xacts;
			} catch (Error& e) {
				TraceEvent("FailedToCommitTx").error(e);
				state bool isConflict = false;
				if (e.code() == error_code_operation_cancelled)
					throw;
				else if (e.code() == error_code_not_committed) {
					++self->conflicts;
					isConflict = true;
				}
				wait(tr.onError(e));
				// check API correctness
				if (!self->skipCorrectnessCheck && self->reportConflictingKeys && isConflict) {
					// \xff\xff/transaction/conflicting_keys is always false, we skip it here for simplicity
					state KeyRange ckr = KeyRangeRef(keyAfter(LiteralStringRef("").withPrefix(conflictingKeysAbsolutePrefix)),
					                                 LiteralStringRef("\xff\xff").withPrefix(conflictingKeysAbsolutePrefix));
					// The getRange here using the special key prefix "\xff\xff/transaction/conflicting_keys/" happens
					// locally Thus, the error handling is not needed here
					Future<Standalone<RangeResultRef>> conflictingKeyRangesFuture =
					    tr.getRange(ckr, readConflictRanges.size() * 2);
					ASSERT(conflictingKeyRangesFuture.isReady());
					const Standalone<RangeResultRef> conflictingKeyRanges = conflictingKeyRangesFuture.get();
					ASSERT(conflictingKeyRanges.size() && (conflictingKeyRanges.size() % 2 == 0));
					for (int i = 0; i < conflictingKeyRanges.size(); i += 2) {
						KeyValueRef startKeyWithPrefix = conflictingKeyRanges[i];
						ASSERT(startKeyWithPrefix.value == conflictingKeysTrue);
						KeyValueRef endKeyWithPrefix = conflictingKeyRanges[i + 1];
						ASSERT(endKeyWithPrefix.value == conflictingKeysFalse);
						// Remove the prefix of returning keys
						Key startKey = startKeyWithPrefix.key.removePrefix(conflictingKeysAbsolutePrefix);
						Key endKey = endKeyWithPrefix.key.removePrefix(conflictingKeysAbsolutePrefix);
						KeyRangeRef kr = KeyRangeRef(startKey, endKey);
						if (!std::any_of(readConflictRanges.begin(), readConflictRanges.end(), [&kr](KeyRange rCR) {
							    // Read_conflict_range remains same in the resolver.
							    // Thus, the returned keyrange is either the original read_conflict_range or merged
							    // by several overlapped ones In either case, it contains at least one original
							    // read_conflict_range
							    return kr.contains(rCR);
						    })) {
							++self->invalidReports;
							TraceEvent(SevError, "TestFailure").detail("Reason", "InvalidKeyRangeReturned");
						}
					}
				}
				++self->retries;
			}
			readConflictRanges.clear();
			tr.reset();
		}
	}
};

WorkloadFactory<ReportConflictingKeysWorkload> ReportConflictingKeysWorkload("ReportConflictingKeys");
