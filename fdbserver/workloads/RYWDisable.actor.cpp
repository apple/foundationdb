/*
 * RYWDisable.actor.cpp
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
#include "workloads.h"

struct RYWDisableWorkload : TestWorkload {
	int nodes, keyBytes;
	double testDuration;
	vector<Future<Void>> clients;

	RYWDisableWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 600.0 );
		nodes = getOption( options, LiteralStringRef("nodes"), 100 );
		keyBytes = std::max( getOption( options, LiteralStringRef("keyBytes"), 16 ), 16 );
	}

	virtual std::string description() { return "RYWDisable"; }

	virtual Future<Void> setup( Database const& cx ) {
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		if( clientId == 0 )
			return _start( cx, this );
		return Void();
	}

	ACTOR static Future<Void> _start( Database cx, RYWDisableWorkload* self ) {
		state double testStart = now();
		
		loop {
			state ReadYourWritesTransaction tr( cx );
			loop {
				try {
					//do some operations
					state int opType = g_random->randomInt(0,4);
					state bool shouldError = true;

					if( opType == 0 ) {
						//TraceEvent("RYWSetting");
						tr.set( self->keyForIndex(g_random->randomInt(0, self->nodes)), StringRef());
					} else if( opType == 1 ) {
						//TraceEvent("RYWGetNoWait");
						Future<Optional<Value>> _ = tr.get( self->keyForIndex(g_random->randomInt(0, self->nodes)));
					} else if( opType == 2 ) {
						//TraceEvent("RYWGetAndWait");
						Optional<Value> _ = wait( tr.get( self->keyForIndex(g_random->randomInt(0, self->nodes))) );
					} else {
						//TraceEvent("RYWNoOp");
						shouldError = false;
					}

					//set ryw disable, check that it fails
					try {
						tr.setOption( FDBTransactionOptions::READ_YOUR_WRITES_DISABLE );
						if( shouldError )
							ASSERT(false);
					} catch( Error &e ) {
						if( !shouldError )
							ASSERT(false);
						ASSERT(e.code() == error_code_client_invalid_operation);
					}

					Void _ = wait( delay(0.1) );
					
					if( now() - testStart > self->testDuration )
						return Void();

					if( g_random->random01() < 0.5 )
						break;

					tr.reset();
				} catch( Error &e ) {
					Void _ = wait( tr.onError(e) );
				}
			}
		}
	}

	virtual Future<bool> check( Database const& cx ) {
		bool ok = true;
		for( int i = 0; i < clients.size(); i++ )
			if( clients[i].isError() )
				ok = false;
		clients.clear();
		return ok;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
	}

	Key keyForIndex( uint64_t index ) {
		Key result = makeString( keyBytes );
		uint8_t* data = mutateString( result );
		memset(data, '.', keyBytes);

		double d = double(index) / nodes;
		emplaceIndex( data, 0, *(int64_t*)&d );

		return result;
	}
};

WorkloadFactory<RYWDisableWorkload> RYWDisableWorkloadFactory("RYWDisable");