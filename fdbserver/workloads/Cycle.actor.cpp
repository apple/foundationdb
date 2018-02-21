/*
 * Cycle.actor.cpp
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
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "workloads.h"
#include "BulkSetup.actor.h"

struct CycleWorkload : TestWorkload {
	int actorCount, nodeCount;
	double testDuration, transactionsPerSecond, minExpectedTransactionsPerSecond;
	Key		keyPrefix;

	vector<Future<Void>> clients;
	PerfIntCounter transactions, retries, pastVersionRetries, commitFailedRetries;
	PerfDoubleCounter totalLatency;

	CycleWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx),
		transactions("Transactions"), retries("Retries"), totalLatency("Latency"),
		pastVersionRetries("Retries.past_version"), commitFailedRetries("Retries.commit_failed")
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		transactionsPerSecond = getOption( options, LiteralStringRef("transactionsPerSecond"), 5000.0 ) / clientCount;
		actorCount = getOption( options, LiteralStringRef("actorsPerClient"), transactionsPerSecond / 5 );
		nodeCount = getOption(options, LiteralStringRef("nodeCount"), transactionsPerSecond * clientCount);
		keyPrefix = getOption(options, LiteralStringRef("keyPrefix"), LiteralStringRef(""));
		minExpectedTransactionsPerSecond = transactionsPerSecond * getOption(options, LiteralStringRef("expectedRate"), 0.7);
	}

	virtual std::string description() { return "CycleWorkload"; }
	virtual Future<Void> setup( Database const& cx ) {
		return bulkSetup( cx, this, nodeCount, Promise<double>() );
	}
	virtual Future<Void> start( Database const& cx ) {
		for(int c=0; c<actorCount; c++)
			clients.push_back(
				timeout(
					cycleClient( cx->clone(), this, actorCount / transactionsPerSecond ), testDuration, Void()) );
		return delay(testDuration);
	}
	virtual Future<bool> check( Database const& cx ) {
		int errors = 0;
		for(int c=0; c<clients.size(); c++)
			errors += clients[c].isError();
		if (errors)
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were client errors.");
		clients.clear();
		return cycleCheck( cx->clone(), this, !errors );
	}
	virtual void getMetrics( vector<PerfMetric>& m ) {
		m.push_back( transactions.getMetric() );
		m.push_back( retries.getMetric() );
		m.push_back( pastVersionRetries.getMetric() );
		m.push_back( commitFailedRetries.getMetric() );
		m.push_back( PerfMetric( "Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), true ) );
		m.push_back( PerfMetric( "Read rows/simsec (approx)", transactions.getValue() * 3 / testDuration, false ) );
		m.push_back( PerfMetric( "Write rows/simsec (approx)", transactions.getValue() * 4 / testDuration, false ) );
	}

	Key keyForIndex( int n ) { return key( n ); }
	Key key(int n) { return doubleToTestKey((double)n / nodeCount, keyPrefix); }
	Value value(int n) { return doubleToTestKey(n, keyPrefix); }
	int fromValue(const ValueRef& v) { return testKeyToDouble(v, keyPrefix); }

	Standalone<KeyValueRef> operator()( int n ) {
		return KeyValueRef( key( n ), value( (n+1) % nodeCount ) );
	}

	void badRead(const char *name, int r, Transaction& tr) {
		TraceEvent(SevError, "CycleBadRead").detail(name, r).detail("Key", printable(key(r))).detail("Version", tr.getReadVersion().get()).detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken);
	}

	ACTOR Future<Void> cycleClient( Database cx, CycleWorkload* self, double delay ) {
		state double lastTime = now();
		try {
			loop {
				Void _ = wait( poisson( &lastTime, delay ) );

				state double tstart = now();
				state int r = g_random->randomInt(0, self->nodeCount);
				state Transaction tr(cx);
				while (true) {
					try {
						// Reverse next and next^2 node
						Optional<Value> v = wait( tr.get( self->key(r) ) );
						if (!v.present()) self->badRead("r", r, tr);
						state int r2 = self->fromValue(v.get());
						Optional<Value> v2 = wait( tr.get( self->key(r2) ) );
						if (!v2.present()) self->badRead("r2", r2, tr);
						state int r3 = self->fromValue(v2.get());
						Optional<Value> v3 = wait( tr.get( self->key(r3) ) );
						if (!v3.present()) self->badRead("r3", r3, tr);
						int r4 = self->fromValue(v3.get());

						tr.clear( self->key(r) );	//< Shouldn't have an effect, but will break with wrong ordering
						tr.set( self->key(r), self->value(r3) );
						tr.set( self->key(r2), self->value(r4) );
						tr.set( self->key(r3), self->value(r2) );

						Void _ = wait( tr.commit() );
						//TraceEvent("CycleCommit");
						break;
					} catch (Error& e) {
						if (e.code() == error_code_transaction_too_old) ++self->pastVersionRetries;
						else if (e.code() == error_code_not_committed) ++self->commitFailedRetries;
						Void _ = wait( tr.onError(e) );
					}
					++self->retries;
				}
				++self->transactions;
				self->totalLatency += now() - tstart;
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				TraceEvent(SevError, "CycleClient").error(e);
			throw;
		}
	}
	bool cycleCheckData( const VectorRef<KeyValueRef>& data, Version v ) {
		if (data.size() != nodeCount) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "Node count changed").detail("Before", nodeCount).detail("After", data.size()).detail("Version", v).detail("KeyPrefix", keyPrefix.printable());
			return false;
		}
		int i=0;
		for(int c=0; c<nodeCount; c++) {
			if (c && !i) {
				TraceEvent(SevError, "TestFailure").detail("Reason", "Cycle got shorter").detail("Before", nodeCount).detail("After", c).detail("KeyPrefix", keyPrefix.printable());
				return false;
			}
			if (data[i].key != key(i)) {
				TraceEvent(SevError, "TestFailure").detail("Reason", "Key changed").detail("KeyPrefix", keyPrefix.printable());
				return false;
			}
			double d = testKeyToDouble(data[i].value, keyPrefix);
			i = (int)d;
			if ( i != d || i<0 || i>=nodeCount) {
				TraceEvent(SevError, "TestFailure").detail("Reason", "Invalid value").detail("KeyPrefix", keyPrefix.printable());
				return false;
			}
		}
		if (i != 0) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "Cycle got longer").detail("KeyPrefix", keyPrefix.printable());
			return false;
		}
		return true;
	}
	ACTOR Future<bool> cycleCheck( Database cx, CycleWorkload* self, bool ok ) {
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "cycleCheck");
		if (self->transactions.getMetric().value() < self->testDuration * self->minExpectedTransactionsPerSecond) {
			TraceEvent(SevWarnAlways, "TestFailure").detail("Reason", "Rate below desired rate").detail("Details", format("%.2f", self->transactions.getMetric().value() / (self->transactionsPerSecond * self->testDuration)))
				.detail("TransactionsAchieved", self->transactions.getMetric().value())
				.detail("MinTransactionsExpected", self->testDuration * self->minExpectedTransactionsPerSecond)
				.detail("TransactionGoal", self->transactionsPerSecond * self->testDuration);
			ok = false;
		}
		if (!self->clientId) {
			// One client checks the validity of the cycle
			state Transaction tr(cx);
			state int retryCount = 0;
			loop {
				try {
					state Version v = wait( tr.getReadVersion() );
					Standalone<RangeResultRef> data = wait(tr.getRange(firstGreaterOrEqual(doubleToTestKey(0.0, self->keyPrefix)), firstGreaterOrEqual(doubleToTestKey(1.0, self->keyPrefix)), self->nodeCount + 1));
					ok = self->cycleCheckData( data, v ) && ok;
					break;
				} catch (Error& e) {
					retryCount++;
					TraceEvent(retryCount > 20 ? SevWarnAlways : SevWarn, "CycleCheckError").error(e);
					Void _ = wait(tr.onError(e));
				}
			}
		}
		return ok;
	}
};

WorkloadFactory<CycleWorkload> CycleWorkloadFactory("Cycle");
