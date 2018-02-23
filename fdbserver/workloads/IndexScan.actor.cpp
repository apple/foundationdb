/*
 * IndexScan.actor.cpp
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
#include "BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"

struct IndexScanWorkload : KVWorkload {
	uint64_t rowsRead, chunks;
	int bytesPerRead, failedTransactions, scans;
	double totalTimeFetching, testDuration, transactionDuration;
	bool singleProcess, readYourWrites;

	IndexScanWorkload(WorkloadContext const& wcx)
		: KVWorkload(wcx), failedTransactions( 0 ),
		rowsRead( 0 ), chunks( 0 ), scans( 0 )
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		bytesPerRead = getOption( options, LiteralStringRef("bytesPerRead"), 80000 );
		transactionDuration = getOption( options, LiteralStringRef("transactionDuration"), 1.0 );
		singleProcess = getOption( options, LiteralStringRef("singleProcess"), true );
		readYourWrites = getOption( options, LiteralStringRef("readYourWrites"), true );
	}

	virtual std::string description() { return "SimpleRead"; }

	virtual Future<Void> setup( Database const& cx ) {
		// this will be set up by and external force!
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		if( singleProcess && clientId != 0 ) {
			return Void();
		}
		return _start( cx, this );
	}

	virtual Future<bool> check(const Database&) { return true; }

	virtual void getMetrics( vector<PerfMetric>& m ) {
		if( singleProcess && clientId != 0 )
			return;

		m.push_back( PerfMetric( "FailedTransactions", failedTransactions, false ) );
		m.push_back( PerfMetric( "RowsRead", rowsRead, false ) );
		m.push_back( PerfMetric( "Scans", scans, false ) );
		m.push_back( PerfMetric( "Chunks", chunks, false ) );
		m.push_back( PerfMetric( "TimeFetching", totalTimeFetching, true ) );
		m.push_back( PerfMetric( "Rows/sec", totalTimeFetching == 0 ? 0 : rowsRead / totalTimeFetching, true ) );
		m.push_back( PerfMetric( "Rows/chunk", chunks == 0 ? 0 : rowsRead / (double)chunks, true ) );
	}

	ACTOR Future<Void> _start( Database cx, IndexScanWorkload *self ) {
		// Boilerplate: "warm" the location cache so that the location of all keys is known before test starts
		state double startTime = now();
		loop {
			state Transaction tr(cx);
			try {
				Void _ = wait( tr.warmRange( cx, allKeys ) );
				break;
			} catch( Error& e ) {
				Void _ = wait( tr.onError( e ) );
			}
		}

		// Wait some small amount of time for things to "settle". Maybe this is historical?
		Void _ = wait( delay( std::max(0.1, 1.0 - (now() - startTime) ) ) );

		Void _ = wait( timeout( serialScans( cx, self ), self->testDuration, Void() ) );
		return Void();
	}

	ACTOR static Future<Void> serialScans( Database cx, IndexScanWorkload *self ) {
		state double start = now();
		try {
			loop {
				Void _  = wait( scanDatabase( cx, self ) );
			}
		} catch( ... ) {
			self->totalTimeFetching = now() - start;
			throw;
		}
	}

	ACTOR static Future<Void> scanDatabase( Database cx, IndexScanWorkload *self ) {
		state int startNode = g_random->randomInt(0, self->nodeCount / 2); //start in the first half of the database
		state KeySelector begin = firstGreaterOrEqual( self->keyForIndex( startNode ) );
		state KeySelector end = firstGreaterThan( self->keyForIndex( self->nodeCount ) );
		state GetRangeLimits limits( CLIENT_KNOBS->ROW_LIMIT_UNLIMITED, self->bytesPerRead );

		state int rowsRead;
		state int chunks;
		state double startTime;
		loop {
			state ReadYourWritesTransaction tr(cx);
			if (!self->readYourWrites) tr.setOption( FDBTransactionOptions::READ_YOUR_WRITES_DISABLE );
			startTime = now();
			rowsRead = 0;
			chunks = 0;

			try {
				loop {
					Standalone<RangeResultRef> r = wait( tr.getRange( begin, end, limits ) );
					chunks++;
					rowsRead += r.size();
					if( !r.size() || !r.more || (now() - startTime) > self->transactionDuration) {
						break;
					}
					begin = firstGreaterThan( r[ r.size() - 1].key );
				}

				break;
			} catch( Error& e ) {
				if( e.code() != error_code_actor_cancelled )
					++self->failedTransactions;
				Void _ = wait( tr.onError( e ) );
			}
		}

		self->rowsRead += rowsRead;
		self->chunks += chunks;
		self->scans++;
		return Void();
	}
};

WorkloadFactory<IndexScanWorkload> IndexScanWorkloadFactory("IndexScan");
