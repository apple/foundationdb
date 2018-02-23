/*
 * ConflictRange.actor.cpp
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
#include "fdbclient/ReadYourWrites.h"
#include "workloads.h"
#include "fdbclient/ManagementAPI.h"

//For this test to report properly buggify must be disabled (flow.h) , and failConnection must be disabled in (sim2.actor.cpp)

struct ConflictRangeWorkload : TestWorkload {
	int minOperationsPerTransaction,maxOperationsPerTransaction,maxKeySpace,maxOffset,minInitialAmount,maxInitialAmount;
	double testDuration;
	bool testReadYourWrites;

	vector<Future<Void>> clients;
	PerfIntCounter withConflicts, withoutConflicts, retries;

	ConflictRangeWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), withConflicts("WithConflicts"), withoutConflicts("withoutConflicts"), retries("Retries")
	{
		minOperationsPerTransaction = getOption( options, LiteralStringRef("minOperationsPerTransaction"), 2 );
		maxOperationsPerTransaction = getOption( options, LiteralStringRef("minOperationsPerTransaction"), 4 );
		maxKeySpace = getOption( options, LiteralStringRef("maxKeySpace"), 100 );
		maxOffset = getOption( options, LiteralStringRef("maxOffset"), 5 );
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
		testReadYourWrites = getOption( options, LiteralStringRef("testReadYourWrites"), false );
	}

	virtual std::string description() { return "ConflictRange"; }

	virtual Future<Void> setup( Database const& cx ) {
		return Void();
	}

	virtual Future<Void> start( Database const& cx ) {
		return _start( cx, this );
	}

	virtual Future<bool> check( Database const& cx ) {
		clients.clear();
		return true;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
		m.push_back( withConflicts.getMetric() );
		m.push_back( withoutConflicts.getMetric() );
		m.push_back( retries.getMetric() );
	}

	ACTOR Future<Void> _start(Database cx, ConflictRangeWorkload *self) {
		if( self->clientId == 0 )
			Void _ = wait( timeout( self->conflictRangeClient( cx, self), self->testDuration, Void() ) );
		return Void();
	}

	ACTOR Future<Void> conflictRangeClient(Database cx, ConflictRangeWorkload *self) {
		state int i;
		state int j;
		state std::string clientID;
		state std::string myKeyA;
		state std::string myKeyB;
		state std::string myValue;
		state bool onEqualA;
		state bool onEqualB;
		state int offsetA;
		state int offsetB;
		state int randomLimit;
		state bool randomSets = false;
		state std::set<int> insertedSet;
		state Standalone<RangeResultRef> originalResults;
		state Standalone<StringRef> firstElement;

		state std::set<int> clearedSet;
		state int clearedBegin;
		state int clearedEnd;

		if(g_network->isSimulated()) {
			Void _ = wait( timeKeeperSetDisable(cx) );
		}

		loop {
			randomSets = !randomSets;

			//Initialize the database with random values.
			loop {
				state Transaction tr0(cx);
				try {
					TraceEvent("ConflictRangeReset");
					insertedSet.clear();

					if( self->testReadYourWrites ) {
						clearedSet.clear();
						int clearedA = g_random->randomInt(0, self->maxKeySpace-1);
						int clearedB = g_random->randomInt(0, self->maxKeySpace-1);
						clearedBegin = std::min(clearedA, clearedB);
						clearedEnd = std::max(clearedA, clearedB)+1;
						TraceEvent("ConflictRangeClear").detail("begin",clearedBegin).detail("end",clearedEnd);
					}

					tr0.clear( KeyRangeRef( StringRef( format( "%010d", 0 ) ), StringRef( format( "%010d", self->maxKeySpace ) ) ) );
					for(int i = 0; i < self->maxKeySpace; i++) {
						if( g_random->random01() > 0.5) {
							TraceEvent("ConflictRangeInit").detail("Key",i);
							if( self->testReadYourWrites && i >= clearedBegin && i < clearedEnd )
								clearedSet.insert( i );
							else {
								insertedSet.insert( i );
								tr0.set(StringRef( format( "%010d", i ) ),g_random->randomUniqueID().toString());
							}
						}
					}

					Void _ = wait( tr0.commit() );
					break;
				} catch (Error& e) {
					Void _ = wait( tr0.onError(e) );
				}
			}

			firstElement = Key( StringRef( format( "%010d", *(insertedSet.begin()) ) ) );

			state Transaction tr1(cx);
			state Transaction tr2(cx);
			state Transaction tr3(cx);
			state Transaction tr4(cx);
			state ReadYourWritesTransaction trRYOW(cx);

			try {
				//Generate a random getRange operation and execute it, if it produces results, save them, otherwise retry.
				loop {
					myKeyA = format( "%010d", g_random->randomInt( 0, self->maxKeySpace ) );
					myKeyB = format( "%010d", g_random->randomInt( 0, self->maxKeySpace ) );
					onEqualA = g_random->randomInt( 0, 2 ) != 0;
					onEqualB = g_random->randomInt( 0, 2 ) != 0;
					offsetA = g_random->randomInt( -1*self->maxOffset, self->maxOffset );
					offsetB = g_random->randomInt( -1*self->maxOffset, self->maxOffset );
					randomLimit = g_random->randomInt( 1, self->maxKeySpace );

					Standalone<RangeResultRef> res = wait( tr1.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB),randomLimit) );
					if( res.size() ) {
						originalResults = res;
						break;
					}
					tr1 = Transaction(cx);
				}

				if( self->testReadYourWrites ) {
					for( auto iter = clearedSet.begin(); iter != clearedSet.end(); ++iter )
						tr1.set(StringRef( format( "%010d", (*iter) ) ),g_random->randomUniqueID().toString());
					Void _ = wait( tr1.commit() );
					tr1 = Transaction(cx);
				}

				//Create two transactions with the same read version
				Version readVersion = wait( tr2.getReadVersion() );

				if( self->testReadYourWrites ) {
					trRYOW.setVersion( readVersion );
					trRYOW.setOption( FDBTransactionOptions::READ_SYSTEM_KEYS );
				} else
					tr3.setVersion( readVersion );

				//Do random operations in one of the transactions and commit.
				//Either do all sets in locations without existing data or all clears in locations with data.
				for(i = 0; i < g_random->randomInt(self->minOperationsPerTransaction,self->maxOperationsPerTransaction+1); i++) {
					if( randomSets ) {
						for( int j = 0; j < 5; j++) {
							int proposedKey = g_random->randomInt( 0, self->maxKeySpace );
							if( !insertedSet.count( proposedKey ) ) {
								TraceEvent("ConflictRangeSet").detail("Key",proposedKey);
								insertedSet.insert( proposedKey );
								tr2.set(StringRef(format( "%010d", proposedKey )),g_random->randomUniqueID().toString());
								break;
							}
						}
					}
					else {
						for( int j = 0; j < 5; j++) {
							int proposedKey = g_random->randomInt( 0, self->maxKeySpace );
							if( insertedSet.count( proposedKey ) ) {
								TraceEvent("ConflictRangeClear").detail("Key",proposedKey);
								insertedSet.erase( proposedKey );
								tr2.clear(StringRef(format( "%010d", proposedKey )));
								break;
							}
						}
					}
				}

				Void _ = wait( tr2.commit() );

				state bool foundConflict = false;
				try {
					//Do the generated getRange in the other transaction and commit.
					if( self->testReadYourWrites ) {
						trRYOW.clear( KeyRangeRef( StringRef( format( "%010d", clearedBegin ) ), StringRef( format( "%010d", clearedEnd ) ) ) );
						Standalone<RangeResultRef> res = wait( trRYOW.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB),randomLimit) );
						Void _ = wait( trRYOW.commit() );
					} else {
						tr3.clear( StringRef( format( "%010d", self->maxKeySpace + 1 ) ) );
						Standalone<RangeResultRef> res = wait( tr3.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB),randomLimit) );
						Void _ = wait( tr3.commit() );
					}
				} catch( Error &e ) {
					if( e.code() != error_code_not_committed )
						throw e;
					foundConflict = true;
				}

				if( foundConflict ) {
					//If the commit fails, do the getRange again and check that the results are different from the first execution.
					if( self->testReadYourWrites ) {
						tr1.clear( KeyRangeRef( StringRef( format( "%010d", clearedBegin ) ), StringRef( format( "%010d", clearedEnd ) ) ) );
						Void _ = wait( tr1.commit() );
						tr1 = Transaction(cx);
					}

					Standalone<RangeResultRef> res = wait( tr4.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB),randomLimit) );
					++self->withConflicts;

					if( res.size() == originalResults.size() ) {
						for( int i = 0; i < res.size(); i++ )
							if( res[i] != originalResults[i] )
								throw not_committed();

						//Discard known cases where conflicts do not change the results
						if( originalResults.size() == randomLimit && offsetB <= 0 ) {
							//Hit limit but end offset goes backwards, so changes could effect results even though in this instance they did not
							throw not_committed();
						}

						if( originalResults[originalResults.size()-1].key >= LiteralStringRef("\xff") ) {
							//Results go into server keyspace, so if a key selector does not fully resolve offset, a change won't effect results
							throw not_committed();
						}

						if( (originalResults[0].key == firstElement || originalResults[0].key == StringRef( format( "%010d", *(insertedSet.begin()) ) ) ) && offsetA < 0 ) {
							//Results return the first element, and the begin offset is negative, so if a key selector does not fully resolve the offset, a change won't effect results
							throw not_committed();
						}

						if( (myKeyA > myKeyB || (myKeyA == myKeyB && onEqualA && !onEqualB)) && originalResults.size() == randomLimit ) {
							//The begin key is less than the end key, so changes in this range only effect the end key selector, but because we hit the limit this does not change the results
							throw not_committed();
						}

						std::string keyStr1 = "";
						for( int i = 0; i < res.size(); i++) {
							keyStr1 += printable(res[i].key) + " ";
						}

						std::string keyStr2 = "";
						for( int i = 0; i < originalResults.size(); i++) {
							keyStr2 += printable(originalResults[i].key) + " ";
						}

						TraceEvent(SevError, "ConflictRangeError").detail("Info", "Conflict returned, however results are the same")
							.detail("randomSets",randomSets).detail("myKeyA",myKeyA).detail("myKeyB",myKeyB).detail("onEqualA",onEqualA).detail("onEqualB",onEqualB)
							.detail("offsetA",offsetA).detail("offsetB",offsetB).detail("randomLimit",randomLimit).detail("size",originalResults.size()).detail("results", keyStr1).detail("original", keyStr2);

						tr4 = Transaction(cx);
						Standalone<RangeResultRef> res = wait( tr4.getRange( KeyRangeRef( StringRef( format( "%010d", 0 ) ), StringRef( format( "%010d", self->maxKeySpace ) ) ), 200 ) );
						std::string allKeyEntries = "";
						for(int i = 0; i < res.size(); i++) {
							allKeyEntries += printable( res[i].key ) + " ";
						}

						TraceEvent("ConflictRangeDump").detail("keys", allKeyEntries);
					}
					throw not_committed();
				} else {
					//If the commit is successful, check that the result matches the first execution.
					Standalone<RangeResultRef> res = wait( tr4.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB),randomLimit) );
					++self->withoutConflicts;

					if( res.size() == originalResults.size() ) {
						for( int i = 0; i < res.size(); i++ ) {
							if( res[i] != originalResults[i] ) {
								TraceEvent(SevError, "ConflictRangeError").detail("Info", "No conflict returned, however results do not match")
									.detail("Original", printable(originalResults[i].key) + " " + printable(originalResults[i].value))
									.detail("New", printable(res[i].key) + " " + printable(res[i].value));
							}
						}
					} else {
						std::string keyStr1 = "";
						for( int i = 0; i < res.size(); i++) {
							keyStr1 += printable(res[i].key) + " ";
						}

						std::string keyStr2 = "";
						for( int i = 0; i < originalResults.size(); i++) {
							keyStr2 += printable(originalResults[i].key) + " ";
						}

						TraceEvent(SevError, "ConflictRangeError").detail("Info", "No conflict returned, however result sizes do not match")
							.detail("OriginalSize", originalResults.size()).detail("NewSize", res.size())
							.detail("randomSets",randomSets).detail("myKeyA",myKeyA).detail("myKeyB",myKeyB).detail("onEqualA",onEqualA).detail("onEqualB",onEqualB)
							.detail("offsetA",offsetA).detail("offsetB",offsetB).detail("randomLimit",randomLimit).detail("size",originalResults.size()).detail("results", keyStr1).detail("original", keyStr2);
					}
				}
			} catch (Error& e) {
				state Error e2 = e;
				if( e2.code() != error_code_not_committed )
					++self->retries;

				Void _ = wait( tr1.onError(e2) );
				Void _ = wait( tr2.onError(e2) );
				Void _ = wait( tr3.onError(e2) );
				Void _ = wait( tr4.onError(e2) );
				Void _ = wait( trRYOW.onError(e2) );
			}
		}
	}
};

WorkloadFactory<ConflictRangeWorkload> ConflictRangeWorkloadFactory("ConflictRange");
