/*
 * SelectorCorrectness.actor.cpp
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

struct SelectorCorrectnessWorkload : TestWorkload {
	int minOperationsPerTransaction,maxOperationsPerTransaction,maxKeySpace,maxOffset;
	bool testReadYourWrites;
	double testDuration;

	vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;

	SelectorCorrectnessWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx), transactions("Transactions"), retries("Retries") {
		
		minOperationsPerTransaction = getOption( options, LiteralStringRef("minOperationsPerTransaction"), 10 );
		maxOperationsPerTransaction = getOption( options, LiteralStringRef("minOperationsPerTransaction"), 50 );
		maxKeySpace = getOption( options, LiteralStringRef("maxKeySpace"), 10 );
		maxOffset = getOption( options, LiteralStringRef("maxOffset"), 20 );
		testReadYourWrites = getOption( options, LiteralStringRef("testReadYourWrites"), true );
		testDuration = getOption( options, LiteralStringRef("testDuration"), 10.0 );
	}

	virtual std::string description() { return "SelectorCorrectness"; }

	virtual Future<Void> setup( Database const& cx ) { 
		return SelectorCorrectnessSetup( cx->clone(), this );
	}

	virtual Future<Void> start( Database const& cx ) { 
		clients.push_back(
			timeout(
			SelectorCorrectnessClient( cx->clone(), this), testDuration, Void()) );
		return delay(testDuration);
	}

	virtual Future<bool> check( Database const& cx ) { 
		clients.clear();
		return true;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
		m.push_back( transactions.getMetric() );
		m.push_back( retries.getMetric() );
	}

	ACTOR Future<Void> SelectorCorrectnessSetup( Database cx, SelectorCorrectnessWorkload* self ) {
		state Future<Void> disabler = disableConnectionFailuresAfter(300, "SelectorCorrectness");
		state Value myValue = StringRef(format( "%010d", g_random->randomInt( 0, 10000000 ) ));
		state Transaction tr(cx);

		if(!self->testReadYourWrites) {
			loop {
				try {
					for(int i = 0; i < self->maxKeySpace; i+=2) tr.set(StringRef(format( "%010d", i ) ),myValue);

					Void _ = wait( tr.commit() );
					break;
				} catch (Error& e) {
					Void _ = wait( tr.onError(e) );
				}
			}
		} else {
			loop {
				try {
					for(int i = 0; i < self->maxKeySpace; i+=4) 
						tr.set(StringRef(format( "%010d", i ) ),myValue);
					for(int i = 2; i < self->maxKeySpace; i+=4) 
						if(g_random->random01() > 0.5)
							tr.set(StringRef(format( "%010d", i ) ),myValue);

					Void _ = wait( tr.commit() );
					break;
				} catch (Error& e) {
					Void _ = wait( tr.onError(e) );
				}
			}
		}
		
		return Void();
	}

	ACTOR Future<Void> SelectorCorrectnessClient( 
		Database cx, SelectorCorrectnessWorkload *self) {
		state int i;
		state int j;
		state std::string myKeyA;
		state std::string myKeyB;
		state Value myValue;
		state bool onEqualA;
		state bool onEqualB;
		state int offsetA;
		state int offsetB;
		state Standalone<StringRef> maxKey;
		state bool reverse;

		maxKey = Standalone<StringRef>(format( "%010d", self->maxKeySpace + 1 ));

		loop {
			
			state Transaction tr(cx);
			state ReadYourWritesTransaction trRYOW(cx);

			trRYOW.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			if( self->testReadYourWrites ) {
				myValue = StringRef(format( "%010d", g_random->randomInt( 0, 10000000 ) ));
				for(int i = 2; i < self->maxKeySpace; i+=4)
					trRYOW.set(StringRef(format( "%010d", i ) ),myValue);
				for(int i = 0; i < self->maxKeySpace; i+=4)
					if(g_random->random01() > 0.5)
						trRYOW.set(StringRef(format( "%010d", i ) ),myValue);
			}

			try {
				for(i = 0; i < g_random->randomInt(self->minOperationsPerTransaction,self->maxOperationsPerTransaction+1); i++) {
					j = g_random->randomInt(0,2);
					if( j < 1 ) {
						state int searchInt = g_random->randomInt( 0, self->maxKeySpace );
						myKeyA = format( "%010d", searchInt );

						if(self->testReadYourWrites) {
							Optional<Value> getTest = wait(trRYOW.get(StringRef(myKeyA)));
							if( (searchInt%2==0 && !getTest.present()) || (searchInt%2==1 && getTest.present())) {
									TraceEvent(SevError, "RanSelTestFailure").detail("Reason", "Value not present").detail("KeyA",myKeyA);
							}
						} else {
							Optional<Value> getTest = wait(tr.get(StringRef(myKeyA)));
							if( (searchInt%2==0 && !getTest.present()) || (searchInt%2==1 && getTest.present())) {
									TraceEvent(SevError, "RanSelTestFailure").detail("Reason", "Value not present").detail("KeyA",myKeyA);
							}
						}
					} else {
						int a = g_random->randomInt( 2, self->maxKeySpace );
						int b = g_random->randomInt( 2, 2*self->maxKeySpace );
						int abmax = std::max(a,b);
						int abmin = std::min(a,b)-1;
						myKeyA = format( "%010d", abmin );
						myKeyB = format( "%010d", abmax );
						onEqualA = g_random->randomInt( 0, 2 ) != 0;
						onEqualB = g_random->randomInt( 0, 2 ) != 0;
						offsetA = 1;//-1*g_random->randomInt( 0, self->maxOffset );
						offsetB = g_random->randomInt( 1, self->maxOffset );
						reverse = g_random->random01() > 0.5 ? false : true;

						//TraceEvent("RYOWgetRange").detail("KeyA", myKeyA).detail("KeyB", myKeyB).detail("onEqualA",onEqualA).detail("onEqualB",onEqualB).detail("offsetA",offsetA).detail("offsetB",offsetB).detail("direction",direction);
						state int expectedSize = (std::min( abmax + 2*offsetB - (abmax%2==1 ? 1 : (onEqualB ? 0 : 2)), self->maxKeySpace ) - ( std::max( abmin + 2*offsetA - (abmin%2==1 ? 1 : (onEqualA ? 0 : 2)), 0 ) ))/2;

						if(self->testReadYourWrites) {
							Standalone<RangeResultRef> getRangeTest = wait( trRYOW.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB), 2*(self->maxKeySpace+self->maxOffset), false, reverse ) );

							int trueSize = 0;
							while(trueSize < getRangeTest.size() && getRangeTest[ !reverse ? trueSize : getRangeTest.size() - trueSize - 1 ].key < maxKey) trueSize++;

							if( trueSize != expectedSize ) {
								std::string outStr = "";
								for(int k = 0; k < trueSize; k++) {
									std::string keyStr = printable(getRangeTest[!reverse ? k : getRangeTest.size() - k - 1].key);
									outStr = outStr + keyStr + " ";
								}

								TraceEvent(SevError, "RanSelTestFailure").detail("Reason", "The getRange results did not match expected size").detail("size", trueSize).detail("expected",expectedSize).detail("data",outStr).detail("dataSize", getRangeTest.size());
							}
						} else {
							Standalone<RangeResultRef> getRangeTest = wait( tr.getRange(KeySelectorRef(StringRef(myKeyA),onEqualA,offsetA),KeySelectorRef(StringRef(myKeyB),onEqualB,offsetB), 2*(self->maxKeySpace+self->maxOffset), false, reverse ) );
							
							int trueSize = 0;
							while(trueSize < getRangeTest.size() && getRangeTest[ !reverse ? trueSize : getRangeTest.size() - trueSize - 1 ].key < maxKey) trueSize++;
							
							if( trueSize != expectedSize ) {
								std::string outStr = "";
								for(int k = 0; k < trueSize; k++) {
									std::string keyStr = printable(getRangeTest[!reverse ? k : getRangeTest.size() - k - 1].key);
									outStr = outStr + keyStr + " ";
								}
								
								TraceEvent(SevError, "RanSelTestFailure").detail("Reason", "The getRange results did not match expected size").detail("size", trueSize).detail("expected",expectedSize).detail("data",outStr).detail("dataSize", getRangeTest.size());
							}
						}
					}
				}

				tr.reset();
				trRYOW.reset();
				++self->transactions;
			} catch (Error& e) {
				Void _ = wait( trRYOW.onError(e) );
				trRYOW.reset();
				++self->retries;
			}
		}
	}
};

WorkloadFactory<SelectorCorrectnessWorkload> SelectorCorrectnessWorkloadFactory("SelectorCorrectness");
