/*
 * WriteBandwidth.actor.cpp
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
#include "fdbserver/WorkerInterface.h"
#include "workloads.h"
#include "BulkSetup.actor.h"

#include <boost/lexical_cast.hpp>

struct WriteBandwidthWorkload : KVWorkload {
	int keysPerTransaction;
	double testDuration, warmingDelay, loadTime, maxInsertRate;
	std::string valueString;

	vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;
	ContinuousSample<double> commitLatencies, GRVLatencies;

	WriteBandwidthWorkload(WorkloadContext const& wcx)
		: KVWorkload(wcx),
		commitLatencies( 2000 ), GRVLatencies( 2000 ),
		loadTime( 0.0 ), transactions("Transactions"), retries("Retries")
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		keysPerTransaction = getOption( options, LiteralStringRef("keysPerTransaction"), 100 );
		valueString = std::string( maxValueBytes, '.' );

		warmingDelay = getOption( options, LiteralStringRef("warmingDelay"), 0.0 );
		maxInsertRate = getOption( options, LiteralStringRef("maxInsertRate"), 1e12 );
	}
		
	virtual std::string description() { return "WriteBandwidth"; }
	virtual Future<Void> setup( Database const& cx ) { return _setup( cx, this ); }
	virtual Future<Void> start( Database const& cx ) { return _start( cx, this ); }

	virtual Future<bool> check( Database const& cx ) { return true; }

	virtual void getMetrics( vector<PerfMetric>& m ) {
		double duration = testDuration;
		int writes = transactions.getValue() * keysPerTransaction;
		m.push_back( PerfMetric( "Measured Duration", duration, true ) );
		m.push_back( PerfMetric( "Transactions/sec", transactions.getValue() / duration, false ) );
		m.push_back( PerfMetric( "Operations/sec", writes / duration, false ) );
		m.push_back( transactions.getMetric() );
		m.push_back( retries.getMetric() );
		m.push_back( PerfMetric( "Mean load time (seconds)", loadTime, true ) );
		m.push_back( PerfMetric( "Write rows", writes, false ) );

		m.push_back( PerfMetric( "Mean GRV Latency (ms)", 1000 * GRVLatencies.mean(), true ) );
		m.push_back( PerfMetric( "Median GRV Latency (ms, averaged)", 1000 * GRVLatencies.median(), true ) );
		m.push_back( PerfMetric( "90% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile( 0.90 ), true ) );
		m.push_back( PerfMetric( "98% GRV Latency (ms, averaged)", 1000 * GRVLatencies.percentile( 0.98 ), true ) );

		m.push_back( PerfMetric( "Mean Commit Latency (ms)", 1000 * commitLatencies.mean(), true ) );
		m.push_back( PerfMetric( "Median Commit Latency (ms, averaged)", 1000 * commitLatencies.median(), true ) );
		m.push_back( PerfMetric( "90% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile( 0.90 ), true ) );
		m.push_back( PerfMetric( "98% Commit Latency (ms, averaged)", 1000 * commitLatencies.percentile( 0.98 ), true ) );

		m.push_back( PerfMetric( "Write rows/sec", writes / duration, false ) );
		m.push_back( PerfMetric( "Bytes written/sec", (writes * (keyBytes + (minValueBytes+maxValueBytes)*0.5)) / duration, false ) );
	}

	Value randomValue() { return StringRef( (uint8_t*)valueString.c_str(), g_random->randomInt(minValueBytes, maxValueBytes+1) );	}

	Standalone<KeyValueRef> operator()( uint64_t n ) {
		return KeyValueRef( keyForIndex( n, false ), randomValue() );
	}

	ACTOR Future<Void> _setup( Database cx, WriteBandwidthWorkload *self ) {
		state Promise<double> loadTime;
		state Promise<vector<pair<uint64_t, double> > > ratesAtKeyCounts;

		Void _ = wait( bulkSetup( cx, self, self->nodeCount, loadTime, true, self->warmingDelay, self->maxInsertRate ) );
		self->loadTime = loadTime.getFuture().get();
		return Void();
	}

	ACTOR Future<Void> _start( Database cx, WriteBandwidthWorkload *self ) {
		for( int i = 0; i < self->actorCount; i++ ) {
			self->clients.push_back( self->writeClient( cx, self ) );
		}

		Void _ = wait( timeout( waitForAll( self->clients ), self->testDuration, Void() ) );
		self->clients.clear();
		return Void();
	}

	ACTOR Future<Void> writeClient( Database cx, WriteBandwidthWorkload *self ) {
		loop {
			state Transaction tr( cx );
			state uint64_t startIdx = g_random->random01() * (self->nodeCount - self->keysPerTransaction);
			loop {
				try {
					state double start = now();
					Version v = wait( tr.getReadVersion() );
					self->GRVLatencies.addSample( now() - start );

					// Predefine a single large write conflict range over the whole key space
					tr.addWriteConflictRange( KeyRangeRef( 
							self->keyForIndex( startIdx, false ), 
							keyAfter( self->keyForIndex( startIdx + self->keysPerTransaction - 1, false ) ) ) );

					for( int i = 0; i < self->keysPerTransaction; i++ )
						tr.set( self->keyForIndex( startIdx + i, false ), self->randomValue(), false );

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

WorkloadFactory<WriteBandwidthWorkload> WriteBandwidthWorkloadFactory("WriteBandwidth");
