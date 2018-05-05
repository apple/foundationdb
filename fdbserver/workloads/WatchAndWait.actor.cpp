/*
 * WatchAndWait.actor.cpp
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

struct WatchAndWaitWorkload : TestWorkload {
	uint64_t nodeCount, watchCount;
	int64_t nodePrefix;
	int keyBytes;
	double testDuration;
	bool triggerWatches;
	vector<Future<Void>> clients;
	PerfIntCounter triggers, retries;

	WatchAndWaitWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), triggers("Triggers"), retries("Retries") 
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 600.0 );
		watchCount = getOption( options, LiteralStringRef("watchCount"), (uint64_t)10000 );
		nodeCount = getOption( options, LiteralStringRef("nodeCount"), (uint64_t)100000 );
		nodePrefix = getOption( options, LiteralStringRef("nodePrefix"), (int64_t)-1 );
		keyBytes = std::max( getOption( options, LiteralStringRef("keyBytes"), 16 ), 4 );
		triggerWatches = getOption( options, LiteralStringRef("triggerWatches"), false );
		
		if( watchCount > nodeCount ) {
			watchCount = nodeCount;
		}
		if( nodePrefix > 0 ) {
			keyBytes += 16;
		}
		
		if( !triggerWatches ) {
			keyBytes++; //watches are on different keys than the ones being modified by the workload
		}
	}

	virtual std::string description() { return "WatchAndWait"; }

	virtual Future<Void> setup( Database const& cx ) { return Void(); }

	virtual Future<Void> start( Database const& cx ) {
		return _start( cx, this );
	}

	Key keyForIndex( uint64_t index ) {
		Key result = makeString( keyBytes );
		uint8_t* data = mutateString( result );
		memset(data, '.', keyBytes);

		int idx = 0;
		if( nodePrefix > 0 ) {
			emplaceIndex( data, 0, nodePrefix );
			idx += 16;
		}

		double d = double(index) / nodeCount;
		emplaceIndex( data, idx, *(int64_t*)&d );

		return result;
	}

	virtual Future<bool> check( Database const& cx ) {
		return true;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
		double duration = testDuration;
		m.push_back( PerfMetric( "Triggers/sec", triggers.getValue() / duration, false ) );
		m.push_back( triggers.getMetric() );
		m.push_back( retries.getMetric() );
	}

	ACTOR Future<Void> _start( Database cx, WatchAndWaitWorkload* self ) {
		state std::vector<Future<Void>> watches;
		int watchCounter = 0;
		uint64_t endNode = (self->nodeCount * (self->clientId+1)) / self->clientCount;
		uint64_t startNode = (self->nodeCount * self->clientId) / self->clientCount;
		uint64_t NodesPerWatch = self->nodeCount / self->watchCount;
		TraceEvent("WatchAndWaitExpect").detail("duration", self->testDuration).detail("expectedCount", (endNode - startNode) / NodesPerWatch).detail("end", endNode).detail("start", startNode).detail("npw", NodesPerWatch);
		for( uint64_t i = startNode; i < endNode; i += NodesPerWatch ) {
			watches.push_back( self->watchAndWait( cx, self, i ) );
			watchCounter++;
		}
		Void _ = wait( delay( self->testDuration )); // || waitForAll( watches )
		TraceEvent("WatchAndWaitEnd").detail("duration", self->testDuration);
		return Void();
	}

	ACTOR Future<Void> watchAndWait( Database cx, WatchAndWaitWorkload* self, int index ) {
		try {
			state ReadYourWritesTransaction tr(cx);
			loop {
				cx->maxOutstandingWatches = 1e6;
				try {
					state Future<Void> watch = tr.watch( self->keyForIndex( index ) );
					Void _ = wait( tr.commit() );
					Void _ = wait( watch );
					++self->triggers;
				} catch( Error &e ) {
					++self->retries;
					Void _ = wait( tr.onError(e) );
				}
			}
		} catch( Error &e ) {
			TraceEvent(SevError, "watchAndWaitError").error(e);
			throw e;
		}
	}
};

WorkloadFactory<WatchAndWaitWorkload> WatchAndWaitWorkloadFactory("WatchAndWait");
