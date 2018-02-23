/*
 * QueuePush.actor.cpp
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
#include "workloads.h"

const int keyBytes = 16;

struct QueuePushWorkload : TestWorkload {
	int actorCount, valueBytes;
	double testDuration;
	bool forward;
	std::string valueString;
	Key endingKey, startingKey;

	vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;
	ContinuousSample<double> commitLatencies, GRVLatencies;

	QueuePushWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx),
		commitLatencies( 2000 ), GRVLatencies( 2000 ), transactions("Transactions"), retries("Retries")
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		actorCount = getOption( options, LiteralStringRef("actorCount"), 50 );

		valueBytes = getOption( options, LiteralStringRef("valueBytes"), 96 );
		valueString = std::string( valueBytes, 'x' );

		forward = getOption( options, LiteralStringRef("forward"), true );

		endingKey = LiteralStringRef("9999999900000001");
		startingKey = LiteralStringRef("0000000000000001");
	}

	virtual std::string description() { return "QueuePush"; }
	virtual Future<Void> start( Database const& cx ) { return _start( cx, this ); }

	virtual Future<bool> check( Database const& cx ) { return true; }

	virtual void getMetrics( vector<PerfMetric>& m ) {
		double duration = testDuration;
		int writes = transactions.getValue();
		m.push_back( PerfMetric( "Measured Duration", duration, true ) );
		m.push_back( PerfMetric( "Operations/sec", writes / duration, false ) );
		m.push_back( transactions.getMetric() );
		m.push_back( retries.getMetric() );

		m.push_back( PerfMetric( "Mean GRV Latency (ms)", 1000 * GRVLatencies.mean(), true ) );
		m.push_back( PerfMetric( "Median GRV Latency (ms, averaged)", 1000 * GRVLatencies.median(), true ) );
		m.push_back( PerfMetric( "90% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile( 0.90 ), true ) );
		m.push_back( PerfMetric( "98% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile( 0.98 ), true ) );

		m.push_back( PerfMetric( "Mean Commit Latency (ms)", 1000 * commitLatencies.mean(), true ) );
		m.push_back( PerfMetric( "Median Commit Latency (ms, averaged)", 1000 * commitLatencies.median(), true ) );
		m.push_back( PerfMetric( "90% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile( 0.90 ), true ) );
		m.push_back( PerfMetric( "98% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile( 0.98 ), true ) );

		m.push_back( PerfMetric( "Bytes written/sec", (writes * (keyBytes + valueBytes)) / duration, false ) );
	}

	static Key keyForIndex( int base, int offset ) { return StringRef( format( "%08x%08x", base, offset ) ); }

	static std::pair<int, int> valuesForKey( KeyRef value ) {
		int base, offset;
		ASSERT(value.size() == 16);

		if( sscanf( value.substr(0,8).toString().c_str(), "%x", &base ) && sscanf( value.substr(8,8).toString().c_str(), "%x", &offset ) ) {
			return std::make_pair( base, offset );
		}
		else
			// SOMEDAY: what should this really be?  Should we rely on exceptions for control flow here?
			throw client_invalid_operation();
	}

	ACTOR Future<Void> _start( Database cx, QueuePushWorkload *self ) {
		for( int i = 0; i < self->actorCount; i++ ) {
			self->clients.push_back( self->writeClient( cx, self ) );
		}

		Void _ = wait( timeout( waitForAll( self->clients ), self->testDuration, Void() ) );
		self->clients.clear();
		return Void();
	}

	ACTOR Future<Void> writeClient( Database cx, QueuePushWorkload *self ) {
		loop {
			state Transaction tr( cx );
			loop {
				try {
					state double start = now();
					Version v = wait( tr.getReadVersion() );
					self->GRVLatencies.addSample( now() - start );

					// Get the last key in the database with a snapshot read
					state Key lastKey;

					if( self->forward ) {
						Key _lastKey = wait( tr.getKey( lastLessThan( self->endingKey ), true ) );
						lastKey = _lastKey;
						if( lastKey == StringRef() )
							lastKey = self->startingKey;
					} else {
						Key _lastKey = wait( tr.getKey( firstGreaterThan( self->startingKey ), true ) );
						lastKey = _lastKey;
						if( !normalKeys.contains( lastKey ) )
							lastKey = self->endingKey;
					}
					
					pair<int, int> unpacked = valuesForKey( lastKey );

					if( self->forward )
						tr.set( keyForIndex( unpacked.first + unpacked.second, g_random->randomInt(1, 1000) ), 
								StringRef(self->valueString) );
					else
						tr.set( keyForIndex( unpacked.first - unpacked.second, g_random->randomInt(1, 1000) ), 
								StringRef(self->valueString) );

					start = now();
					Void _ = wait( tr.commit() );
					self->commitLatencies.addSample( now() - start );
					break;
				} catch( Error& e ) {
					Void _ = wait( tr.onError( e ) );
					++self->retries;
				}
			}
			++self->transactions;
		}
	}
};

WorkloadFactory<QueuePushWorkload> QueuePushWorkloadFactory("QueuePush");
