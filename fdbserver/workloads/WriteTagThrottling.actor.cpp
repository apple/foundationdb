/*
 * WriteThrottling.actor.cpp
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

#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TagThrottle.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// workload description:
// This workload aims to test whether we can throttling some bad clients that doing penetrating write on write hot-spot
// range. There are several good clientActor just randomly do read and write ops in transaction. Also, some bad
// clientActor has high probability to read and write a particular hot-spot. If the tag-based throttling works right on
// write-heavy tags, it will only limit the bad clientActor without influence other normal actors. This workload also
// output TPS and latency of read/set/clear operations to do eyeball check. We want this new feature would not cause
// more load to the cluster (Maybe some visualization tools is needed to show the trend of metrics)
struct WriteTagThrottlingWorkload : KVWorkload {
	// Performance metrics
	int goodActorTxNum = 0, goodActorRetries = 0, goodActorTooOldRetries = 0, goodActorCommitFailedRetries = 0;
	int badActorTxNum = 0, badActorRetries = 0, badActorTooOldRetries = 0, badActorCommitFailedRetries = 0;
	double badActorTotalLatency = 0.0, goodActorTotalLatency = 0.0;
	std::unique_ptr<ContinuousSample<double>> badActorReadLatency, goodActorReadLatency;
	std::unique_ptr<ContinuousSample<double>> badActorCommitLatency, goodActorCommitLatency;

	// Test configuration
	// KVWorkload::actorCount
	int goodActorPerClient, badActorPerClient;
	int numWritePerTx, numReadPerTx, numClearPerTx;
	int keyCount, hotKeyCount;
	int sampleSize;
	double badOpRate;
	double testDuration;
	bool writeThrottle;

	// internal states
	double txBackoffDelay;
	TransactionTag badReadTag, badWriteTag, goodTag;
	static constexpr const char* NAME = "WriteTagThrottling";

	WriteTagThrottlingWorkload(WorkloadContext const& wcx) : KVWorkload(wcx) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);
		badOpRate = getOption(options, LiteralStringRef("badOpRate"), 0.9);
		numWritePerTx = getOption(options, LiteralStringRef("numWritePerTx"), 1);
		numReadPerTx = getOption(options, LiteralStringRef("numReadPerTx"), 1);
		numClearPerTx = getOption(options, LiteralStringRef("numClearPerTx"), 1);
		sampleSize = getOption(options, LiteralStringRef("sampleSize"), 10000);
		badActorReadLatency.reset(new ContinuousSample<double>(sampleSize));
		goodActorReadLatency.reset(new ContinuousSample<double>(sampleSize));
		badActorCommitLatency.reset(new ContinuousSample<double>(sampleSize));
		goodActorCommitLatency.reset(new ContinuousSample<double>(sampleSize));

		writeThrottle = getOption(options, LiteralStringRef("writeThrottle"), false);
		badActorPerClient = getOption(options, LiteralStringRef("badActorPerClient"), 1);
		goodActorPerClient = getOption(options, LiteralStringRef("goodActorPerClient"), 1);
		actorCount = goodActorPerClient + badActorPerClient;

		keyCount = getOption(options, LiteralStringRef("keyCount"), 3000);
		txBackoffDelay = actorCount * 1.0 / getOption(options, LiteralStringRef("txPerSecond"), 1000);

		badReadTag = TransactionTag(std::string("badReadTag"));
		badWriteTag = TransactionTag(std::string("badWriteTag"));
		goodTag = TransactionTag(std::string("goodTag"));
	}

	virtual std::string description() { return WriteTagThrottlingWorkload::NAME; }

	// choose a tag
	ACTOR static Future<Void> _setup(Database cx, WriteTagThrottlingWorkload* self) {
		state TransactionPriority priority = deterministicRandom()->randomChoice(allTransactionPriorities);
		state double rate = deterministicRandom()->random01() * 20;
		state TagSet tagSet1;
		state TagSet tagSet2;
		state TagSet tagSet3;
		tagSet1.addTag(self->badWriteTag);
		tagSet2.addTag(self->badReadTag);
		tagSet3.addTag(self->goodTag);

		wait(ThrottleApi::throttleTags(cx, tagSet1, rate, self->testDuration + 120, TagThrottleType::MANUAL, priority));
		wait(ThrottleApi::throttleTags(cx, tagSet2, rate, self->testDuration + 120, TagThrottleType::MANUAL, priority));
		wait(ThrottleApi::throttleTags(cx, tagSet3, rate, self->testDuration + 120, TagThrottleType::MANUAL, priority));

		return Void();
	}
	Future<Void> setup(const Database& cx) override { return _setup(cx, this); }
	ACTOR static Future<Void> _start(Database cx, WriteTagThrottlingWorkload* self) {
		state vector<Future<Void>> clientActors;
		state int actorId;
		for (actorId = 0; actorId < self->goodActorPerClient; ++actorId) {
			clientActors.push_back(clientActor(false, actorId, 0, cx, self));
		}
		for (actorId = 0; actorId < self->badActorPerClient; ++actorId) {
			clientActors.push_back(clientActor(true, actorId, self->badOpRate, cx, self));
		}
		wait(delay(self->testDuration));
		return Void();
	}
	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) {
		if (badActorTxNum == 0 && goodActorTxNum == 0) {
			TraceEvent(SevError, "NoTransactionsCommitted");
			return false;
		}
		// TODO check whether write throttling work correctly after enabling that feature
		return true;
	}
	virtual void getMetrics(vector<PerfMetric>& m) {
		m.push_back(PerfMetric("Transactions (badActor)", badActorTxNum, true));
		m.push_back(PerfMetric("Transactions (goodActor)", goodActorTxNum, true));
		m.push_back(PerfMetric("Avg Latency (ms, badActor)", 1000 * badActorTotalLatency / badActorTxNum, true));
		m.push_back(PerfMetric("Avg Latency (ms, goodActor)", 1000 * goodActorTotalLatency / goodActorTxNum, true));

		m.push_back(PerfMetric("Retries (badActor)", badActorRetries, true));
		m.push_back(PerfMetric("Retries (goodActor)", goodActorRetries, true));

		m.push_back(PerfMetric("Retries.too_old (badActor)", badActorTooOldRetries, true));
		m.push_back(PerfMetric("Retries.too_old (goodActor)", goodActorTooOldRetries, true));

		m.push_back(PerfMetric("Retries.commit_failed (badActor)", badActorCommitFailedRetries, true));
		m.push_back(PerfMetric("Retries.commit_failed (goodActor)", goodActorCommitFailedRetries, true));

		// Read Sampleing
		m.push_back(PerfMetric("Avg Read Latency (ms, badActor)", 1000 * badActorReadLatency->mean(), false));
		m.push_back(PerfMetric("Avg Read Latency (ms, goodActor)", 1000 * goodActorReadLatency->mean(), false));
		m.push_back(PerfMetric("95% Read Latency (ms, badActor)", 1000 * badActorReadLatency->percentile(0.95), false));
		m.push_back(
		    PerfMetric("95% Read Latency (ms, goodActor)", 1000 * goodActorReadLatency->percentile(0.95), false));
		m.push_back(PerfMetric("50% Read Latency (ms, badActor)", 1000 * badActorReadLatency->median(), false));
		m.push_back(PerfMetric("50% Read Latency (ms, goodActor)", 1000 * goodActorReadLatency->median(), false));

		// Commit Sampleing
		m.push_back(PerfMetric("Avg Commit Latency (ms, badActor)", 1000 * badActorCommitLatency->mean(), false));
		m.push_back(PerfMetric("Avg Commit Latency (ms, goodActor)", 1000 * goodActorCommitLatency->mean(), false));
		m.push_back(
		    PerfMetric("95% Commit Latency (ms, badActor)", 1000 * badActorCommitLatency->percentile(0.95), false));
		m.push_back(
		    PerfMetric("95% Commit Latency (ms, goodActor)", 1000 * goodActorCommitLatency->percentile(0.95), false));
		m.push_back(PerfMetric("50% Commit Latency (ms, badActor)", 1000 * badActorCommitLatency->median(), false));
		m.push_back(PerfMetric("50% Commit Latency (ms, goodActor)", 1000 * goodActorCommitLatency->median(), false));
	}

	// return a key based on useReadKey
	Key generateKey(bool useReadKey, int idx) {
		if (useReadKey) {
			return keyForIndex(idx, false);
		}
		return getRandomKey();
	}
	// return a range based on useClearKey
	KeyRange generateRange(bool useClearKey, int idx) {
		int a = deterministicRandom()->randomInt(0, keyCount / 3);
		if (useClearKey) {
			if (a < idx) {
				return KeyRange(KeyRangeRef(keyForIndex(a, false), keyForIndex(idx, false)));
			} else if (a > idx) {
				return KeyRange(KeyRangeRef(keyForIndex(idx, false), keyForIndex(a, false)));
			} else
				return KeyRange(KeyRangeRef(keyForIndex(idx, false), keyForIndex(a + 1, false)));
		}
		int b = deterministicRandom()->randomInt(0, keyCount / 3);
		if (a > b) std::swap(a, b);
		if (a == b) return KeyRange(KeyRangeRef(keyForIndex(a, false), keyForIndex(a + 1, false)));
		return KeyRange(KeyRangeRef(keyForIndex(a, false), keyForIndex(b, false)));
	}
	Value generateVal() { return Value(deterministicRandom()->randomAlphaNumeric(maxValueBytes)); }

	// read and write value on particular/random Key
	ACTOR static Future<Void> clientActor(bool isBadActor, int actorId, double badOpRate, Database cx,
	                                      WriteTagThrottlingWorkload* self) {
		state double lastTime = now();
		state double opStart;
		state StringRef key;
		try {
			loop {
				wait(poisson(&lastTime, self->txBackoffDelay));
				state double txStart;
				state Transaction tx(cx);
				state int i;
				// give tag to client
				if (self->writeThrottle) {
					if (isBadActor) {
						tx.options.tags.addTag(self->badWriteTag);
						tx.options.tags.addTag(self->badReadTag);
					} else if (deterministicRandom()->coinflip()) {
						tx.options.tags.addTag(self->goodTag);
					}
				}
				txStart = now();
				while (true) {
					try {
						for (i = 0; i < self->numClearPerTx; ++i) {
							bool useClearKey = deterministicRandom()->random01() < badOpRate;
							tx.clear(self->generateRange(useClearKey, actorId));
						}
						for (i = 0; i < self->numWritePerTx; ++i) {
							bool useReadKey = deterministicRandom()->random01() < badOpRate;
							key = self->generateKey(useReadKey, actorId + self->keyCount / 3);
							tx.set(key, self->generateVal());
						}
						for (i = 0; i < self->numReadPerTx; ++i) {
							bool useReadKey = deterministicRandom()->random01() < badOpRate;
							key = self->generateKey(useReadKey, self->keyCount - actorId);
							opStart = now();
							Optional<Value> v = wait(tx.get(key));
							double duration = now() - opStart;
							isBadActor ? self->badActorReadLatency->addSample(duration)
							           : self->goodActorReadLatency->addSample(duration);
						}
						opStart = now();
						wait(tx.commit());
						double duration = now() - opStart;
						isBadActor ? self->badActorCommitLatency->addSample(duration)
						           : self->goodActorCommitLatency->addSample(duration);
						break;
					} catch (Error& e) {
						if (e.code() == error_code_transaction_too_old) {
							isBadActor ? ++self->badActorTooOldRetries : ++self->goodActorTooOldRetries;
						} else if (e.code() == error_code_not_committed) {
							isBadActor ? ++self->badActorCommitFailedRetries : ++self->goodActorCommitFailedRetries;
						}
						wait(tx.onError(e));
					}
					isBadActor ? ++self->badActorRetries : ++self->goodActorRetries;
				}
				double duration = now() - txStart;
				if (isBadActor) {
					++self->badActorTxNum;
					self->badActorTotalLatency += duration;
				} else {
					++self->goodActorTxNum;
					self->goodActorTotalLatency += duration;
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "WriteThrottling").error(e);
			throw;
		}
	}
	// collect health metrics
	//   ACTOR static Future<Void> healthMetricsChecker(Database cx, WriteTagThrottlingWorkload* self) {
	//
	//	}
};

WorkloadFactory<WriteTagThrottlingWorkload> WriteTagThrottlingWorkloadFactory(WriteTagThrottlingWorkload::NAME);
