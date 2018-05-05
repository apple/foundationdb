/*
 * LowLatency.actor.cpp
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
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/Knobs.h"
#include "workloads.h"

struct LowLatencyWorkload : TestWorkload {
	double testDuration;
	double maxLatency;
	double checkDelay;
	PerfIntCounter operations, retries;
	bool ok;

	LowLatencyWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), operations("Operations"), retries("Retries") , ok(true)
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 600.0 );
		maxLatency = getOption( options, LiteralStringRef("maxLatency"), 20.0 );
		checkDelay = getOption( options, LiteralStringRef("checkDelay"), 1.0 );
	}

	virtual std::string description() { return "LowLatency"; }

	virtual Future<Void> setup( Database const& cx ) {
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		if( clientId == 0 )
			return _start( cx, this );
		return Void();
	}

	ACTOR static Future<Void> _start( Database cx, LowLatencyWorkload* self ) {
		state double testStart = now();
		try {
			loop {
				Void _ = wait( delay( self->checkDelay ) );
				state Transaction tr( cx );
				state double operationStart = now();
				++self->operations;
				loop {
					try {
						tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
						tr.setOption(FDBTransactionOptions::LOCK_AWARE);
						Version _ = wait(tr.getReadVersion());
						break;
					} catch( Error &e ) {
						Void _ = wait( tr.onError(e) );
						++self->retries;
					}
				}
				if(now() - operationStart > self->maxLatency) {
					TraceEvent(SevError, "LatencyTooLarge").detail("maxLatency", self->maxLatency).detail("observedLatency", now() - operationStart);
					self->ok = false;
				}
				if( now() - testStart > self->testDuration )
					break;
			}
			return Void();
		} catch( Error &e ) {
			TraceEvent(SevError, "LowLatencyError").error(e,true);
			throw;
		}
	}

	virtual Future<bool> check( Database const& cx ) {
		return ok;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
		double duration = testDuration;
		m.push_back( PerfMetric( "Operations/sec", operations.getValue() / duration, false ) );
		m.push_back( operations.getMetric() );
		m.push_back( retries.getMetric() );
	}
};

WorkloadFactory<LowLatencyWorkload> LowLatencyWorkloadFactory("LowLatency");