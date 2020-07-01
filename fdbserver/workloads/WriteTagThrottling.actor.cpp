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
//
// Created by sfc-gh-xwang on 6/29/20.
//
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// workload description:
// This workload aims to test whether we can throttling some bad clients that doing penetrating write on write hot-spot range.
// There are several good clientActor just randomly do read and write ops in transaction.
// Also, some bad clientActor has high probability to read and write a particular hot-spot.
// If the tag-based throttling works right on write-heavy tags, it will only limit the bad clientActor without influence other normal actors.
// This workload also output TPS and latency of read/set/clear operations to do eyeball check. We want this new feature would not cause more load to the cluster
// (Maybe some visualization tools is needed to show the trend of metrics)
struct WriteTagThrottlingWorkload : TestWorkload {
    // Performance metrics
	PerfIntCounter transactions, retries, tooOldRetries, commitFailedRetries;
    PerfDoubleCounter totalLatency, readOpLatency, commitOpLatency;
	double clientReadLatency, worstReadLatency;
	double clientCommitLatency, worstCommitLatency;

	// Test configuration
	int actorPerClient, goodActorPerClient, badActorPerClient;
    int numWritePerTx, numReadPerTx;
    int keyCount;
	double badOpRate;
	double testDuration;
	bool writeThrottle;

	// internal states
	Key readKey;
    double txBackoffDelay;

    WriteTagThrottlingWorkload(WorkloadContext const& wcx)
	    : TestWorkload(wcx),
	    transactions("Transactions"), retries("Retries"), totalLatency("Latency"),
	    tooOldRetries("Retries.too_old"), commitFailedRetries("Retires.commit_failed"),
	    readOpLatency("ReadOpLatency"), commitOpLatency("CommitOpLatency")
    {
        testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);
        badOpRate = getOption(options, LiteralStringRef("badOpRate"), 0.9);
		numWritePerTx = getOption(options, LiteralStringRef("numWritePerTx"), 1);
        numReadPerTx = getOption(options, LiteralStringRef("numReadPerTx"), 1);
        writeThrottle = getOption(options, LiteralStringRef("writeThrottle"), false);
        actorPerClient = getOption(options, LiteralStringRef("actorPerClient"), 10);
        badActorPerClient = getOption(options, LiteralStringRef("badActorPerClient"), 1);
		goodActorPerClient = actorPerClient - badActorPerClient;
        keyCount = getOption(options, LiteralStringRef("keyCount"), 100);
        readKey = StringRef(format("testkey%08x", deterministicRandom()->randomInt(0, keyCount)));
        txBackoffDelay = actorPerClient * 1.0 / getOption(options, LiteralStringRef("txPerSecond"), 1000);
    }

	virtual std::string description() {
		return "WriteThrottlingWorkload";
	}

	ACTOR static Future<Void> _start(Database cx, WriteTagThrottlingWorkload* self) {
        state vector<Future<Void>> clientActors;
		state int actorId;
		for(actorId = 0; actorId < self->goodActorPerClient; ++actorId) {
			clientActors.push_back(clientActor(0, cx, self));
		}
		for(actorId = 0; actorId < self->badActorPerClient; ++actorId) {
			clientActors.push_back(clientActor(self->badOpRate, cx, self));
		}
        wait(delay(self->testDuration));
		return Void();
	}
    virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
    virtual Future<bool> check(Database const& cx) {
        if (transactions.getValue() == 0) {
            TraceEvent(SevError, "NoTransactionsCommitted");
            return false;
        }
		// TODO check whether write throttling work correctly after enabling that feature
        return true;
	}
    virtual void getMetrics(vector<PerfMetric>& m) {
        m.push_back( transactions.getMetric() );
        m.push_back( retries.getMetric() );
        m.push_back( tooOldRetries.getMetric() );
        m.push_back( commitFailedRetries.getMetric() );
        m.push_back( PerfMetric( "Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), true ) );
        m.push_back( PerfMetric( "Read rows/simsec (approx)", transactions.getValue() * 3 / testDuration, false ) );
        m.push_back( PerfMetric( "Write rows/simsec (approx)", transactions.getValue() * 4 / testDuration, false ) );
	}

	// return a key based on useReadKey
	static StringRef generateKey(bool useReadKey, WriteTagThrottlingWorkload* self) {
		if(useReadKey) return self->readKey;
		return StringRef((format("testKey%08x", deterministicRandom()->randomInt(0, self->keyCount))));
	}
    static ValueRef generateVal() {
        int64_t n = deterministicRandom()->randomInt64(0, LONG_LONG_MAX);
        ValueRef val((uint8_t*)&n, sizeof(int64_t));
		return val;
	}
	// read and write value on particular/random Key
    ACTOR static Future<Void> clientActor(double badOpRate, Database cx, WriteTagThrottlingWorkload* self) {
        state double lastTime = now();
        state double tstart;
        state double tmp_start;
		state StringRef key;
		try{
            loop {
                wait(poisson(&lastTime, self->txBackoffDelay));
                tstart = now();
                state ReadYourWritesTransaction tx(cx);
                state int i;
				// TODO open writeThrottle tag
//				if(self->writeThrottle) {
//
//				}
                while(true) {
                    try {
                        for (i = 0; i < self->numWritePerTx; ++i) {
                            bool useReadKey = deterministicRandom()->random01() < badOpRate;
                            key = generateKey(useReadKey, self);
		                    tmp_start = now();
                            Optional<Value> v = wait(tx.get(key));
                            double a = now() - tmp_start;
                            self->clientReadLatency = a;
                            self->readOpLatency += a;
							tx.set(key, generateVal());
                        }
                        for (i = 0; i < self->numReadPerTx - 1; ++i) {
                            bool useReadKey = deterministicRandom()->random01() < badOpRate;
                            key = generateKey(useReadKey, self);
                            tmp_start = now();
							Optional<Value> v = wait(tx.get(key));
							double a = now() - tmp_start;
							self->clientReadLatency = a;
							self->readOpLatency += a;
                        }
						tmp_start = now();
                        wait(tx.commit());
						double a = now() - tmp_start;
						self->clientCommitLatency = a;
						self->commitOpLatency += a;
                        break;
                    } catch (Error& e) {
                        if (e.code() == error_code_transaction_too_old)
                            ++self->tooOldRetries;
                        else if (e.code() == error_code_not_committed)
                            ++self->commitFailedRetries;
                        wait(tx.onError(e));
                    }
                    ++ self->retries;
                }
                ++self->transactions;
                self->totalLatency += now() - tstart;
            }
		}
		catch (Error& e) {
			TraceEvent(SevError, "WriteThrottling").error(e);
			throw;
		}
    }
	// collect health metrics
//    ACTOR static Future<Void> healthMetricsChecker(Database cx, WriteTagThrottlingWorkload* self) {
//
//	}
};

WorkloadFactory<WriteTagThrottlingWorkload> WriteTagThrottlingWorkloadFactory("WriteTagThrottling");


