/*
 * AtomicOps.actor.cpp
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

#include "flow/actorcompiler.h"
#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "workloads.h"

struct AtomicOpsWorkload : TestWorkload {
	int opNum, actorCount, nodeCount;
	uint32_t opType;
	bool apiVersion500 = false;

	double testDuration, transactionsPerSecond;
	vector<Future<Void>> clients;

	AtomicOpsWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), opNum(0)
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 600.0 );
		transactionsPerSecond = getOption( options, LiteralStringRef("transactionsPerSecond"), 5000.0 ) / clientCount;
		actorCount = getOption( options, LiteralStringRef("actorsPerClient"), transactionsPerSecond / 5 );
		opType = getOption( options, LiteralStringRef("opType"), -1 );
		nodeCount = getOption( options, LiteralStringRef("nodeCount"), 1000 );
		// Atomic OPs Min and And have modified behavior from api version 510. Hence allowing testing for older version (500) with a 10% probability
		// Actual change of api Version happens in setup
		apiVersion500 = ((sharedRandomNumber % 10) == 0);
		TraceEvent("AtomicOpsApiVersion500").detail("apiVersion500", apiVersion500);

		int64_t randNum = sharedRandomNumber / 10;
		if(opType == -1)
			opType = randNum % 8;

		switch(opType) {
		case 0:
			TEST(true); //Testing atomic AddValue
			opType = MutationRef::AddValue;
			break;
		case 1:
			TEST(true); //Testing atomic And
			opType = MutationRef::And;
			break;
		case 2:
			TEST(true); //Testing atomic Or
			opType = MutationRef::Or;
			break;
		case 3:
			TEST(true); //Testing atomic Xor
			opType = MutationRef::Xor;
			break;
		case 4:
			TEST(true); //Testing atomic Max
			opType = MutationRef::Max;
			break;
		case 5:
			TEST(true); //Testing atomic Min
			opType = MutationRef::Min;
			break;
		case 6:
			TEST(true); //Testing atomic ByteMin
			opType = MutationRef::ByteMin;
			break;
		case 7:
			TEST(true); //Testing atomic ByteMax
			opType = MutationRef::ByteMax;
			break;
		default:
			ASSERT(false);
		}
		TraceEvent("AtomicWorkload").detail("opType", opType);
	}

	virtual std::string description() { return "AtomicOps"; }

	virtual Future<Void> setup( Database const& cx ) {
		if (apiVersion500)
			cx->cluster->apiVersion = 500;

		if(clientId != 0)
			return Void();
		return _setup( cx, this );
	}

	virtual Future<Void> start( Database const& cx ) {
		for(int c=0; c<actorCount; c++)
		clients.push_back(
			timeout(
				atomicOpWorker( cx->clone(), this, actorCount / transactionsPerSecond ), testDuration, Void()) );
		return delay(testDuration);
	}

	virtual Future<bool> check( Database const& cx ) {
		if(clientId != 0)
			return true;
		return _check( cx, this );
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	Key logKey( int group ) { return StringRef(format("log%08x%08x%08x",group,clientId,opNum++));}

	ACTOR Future<Void> _setup( Database cx, AtomicOpsWorkload* self ) {
		state int g = 0;
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "AtomicOps");
		for(; g < 100; g++) {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					for(int i = 0; i < self->nodeCount/100; i++) {
						uint64_t intValue = 0;
						tr.set(StringRef(format("ops%08x%08x",g,i)), StringRef((const uint8_t*) &intValue, sizeof(intValue)));
					}
					Void _ = wait( tr.commit() );
					break;
				} catch( Error &e ) {
					Void _ = wait( tr.onError(e) );
				}
			}
		}
		return Void();
	}

	ACTOR Future<Void> atomicOpWorker( Database cx, AtomicOpsWorkload* self, double delay ) {
		state double lastTime = now();
		loop {
			Void _ = wait( poisson( &lastTime, delay ) );
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					int group = g_random->randomInt(0,100);
					uint64_t intValue = g_random->randomInt( 0, 10000000 );
					Key val = StringRef((const uint8_t*) &intValue, sizeof(intValue));
					tr.set(self->logKey(group), val);
					tr.atomicOp(StringRef(format("ops%08x%08x",group,g_random->randomInt(0,self->nodeCount/100))), val, self->opType);
					Void _ = wait( tr.commit() );
					break;
				} catch( Error &e ) {
					Void _ = wait( tr.onError(e) );
				}
			}
		}
	}

	ACTOR Future<bool> _check( Database cx, AtomicOpsWorkload* self ) {
		state int g = 0;
		for(; g < 100; g++) {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					Key begin(format("log%08x", g));
					state Standalone<RangeResultRef> log = wait( tr.getRange(KeyRangeRef(begin, strinc(begin)), CLIENT_KNOBS->TOO_MANY) );
					uint64_t zeroValue = 0;
					tr.set(LiteralStringRef("xlogResult"), StringRef((const uint8_t*) &zeroValue, sizeof(zeroValue)));
					for(auto& kv : log) {
						uint64_t intValue = 0;
						memcpy(&intValue, kv.value.begin(), kv.value.size());
						tr.atomicOp(LiteralStringRef("xlogResult"), kv.value, self->opType);
					}

					Key begin(format("ops%08x", g));
					Standalone<RangeResultRef> ops = wait( tr.getRange(KeyRangeRef(begin, strinc(begin)), CLIENT_KNOBS->TOO_MANY) );
					uint64_t zeroValue = 0;
					tr.set(LiteralStringRef("xopsResult"), StringRef((const uint8_t*) &zeroValue, sizeof(zeroValue)));
					for(auto& kv : ops) {
						uint64_t intValue = 0;
						memcpy(&intValue, kv.value.begin(), kv.value.size());
						tr.atomicOp(LiteralStringRef("xopsResult"), kv.value, self->opType);
					}

					if(tr.get(LiteralStringRef("xlogResult")).get() != tr.get(LiteralStringRef("xopsResult")).get()) {
						TraceEvent(SevError, "LogMismatch").detail("logResult", printable(tr.get(LiteralStringRef("xlogResult")).get())).detail("opsResult",  printable(tr.get(LiteralStringRef("xopsResult")).get().get()));
					}

					if( self->opType == MutationRef::AddValue ) {
						uint64_t opsResult=0;
						Key opsResultStr = tr.get(LiteralStringRef("xopsResult")).get().get();
						memcpy(&opsResult, opsResultStr.begin(), opsResultStr.size());
						uint64_t logResult=0;
						for(auto& kv : log) {
							uint64_t intValue = 0;
							memcpy(&intValue, kv.value.begin(), kv.value.size());
							logResult += intValue;
						}
						if(logResult != opsResult) {
							TraceEvent(SevError, "LogAddMismatch").detail("logResult", logResult).detail("opResult", opsResult).detail("opsResultStr", printable(opsResultStr)).detail("size", opsResultStr.size());
						}
					}
					break;
				} catch( Error &e ) {
					Void _ = wait( tr.onError(e) );
				}
			}
		}
		return true;
	}
};

WorkloadFactory<AtomicOpsWorkload> AtomicOpsWorkloadFactory("AtomicOps");
