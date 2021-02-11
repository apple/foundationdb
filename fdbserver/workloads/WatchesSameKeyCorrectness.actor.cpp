/*
 * WatchesSameKeyCorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "flow/DeterministicRandom.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct WatchesSameKeyWorkload : TestWorkload {
	int numWatches;
	std::vector<Future<Void>> cases;

	WatchesSameKeyWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		numWatches = getOption( options, LiteralStringRef("numWatches"), 3 );
	}

	std::string description() const override { return "WatchesSameKeyCorrectness"; }

	Future<Void> setup(Database const& cx) override { 
		if ( clientId == 0 ) {
			cases.push_back( case1(cx, LiteralStringRef("foo1"), this) );
			cases.push_back( case2(cx, LiteralStringRef("foo2"), this) );
			cases.push_back( case3(cx, LiteralStringRef("foo3"), this) );
			cases.push_back( case4(cx, LiteralStringRef("foo4"), this) );
			cases.push_back( case5(cx, LiteralStringRef("foo5"), this) );
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override { 
		if (clientId == 0) return waitForAll( cases );
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if ( clientId != 0 ) return true;
		bool ok = true;
		for( int i = 0; i < cases.size(); i++ ) {
			if ( cases[i].isError() ) ok = false;
		}
		cases.clear();
		return ok;
	}

	ACTOR static Future<Void> setKeyRandomValue(Database cx, Key key, Optional<Value> val) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				if (!val.present()) val = Value(deterministicRandom()->randomUniqueID().toString());
				tr.set(key, val.get());
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Optional<Value>> getValue(Database cx, Key key) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				Optional<Value> val = wait(tr.get(key));
				return val;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Future<Void>> watchKey(Database cx, Key key) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				state Future<Void> watchFuture = tr.watch(key);
				wait(tr.commit());
				return watchFuture;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> case1(Database cx, Key key, WatchesSameKeyWorkload* self) {
		/**
		 * Tests case 2 in the design doc:
		 *  - we get a watch that has the same value as a key in the watch map 
		 * */
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				state std::vector<Future<Void>> watchFutures;
				state int i;

				for ( i = 0; i < self->numWatches; i++ ) {
					watchFutures.push_back(tr.watch(key));
				}
				wait(tr.commit());

				wait( setKeyRandomValue(cx, key, Optional<Value>()) );
				for ( i = 0; i < watchFutures.size(); i++) {
					wait( watchFutures[i] );
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> case2(Database cx, Key key, WatchesSameKeyWorkload* self) {
		/**
		 * Tests case 3 in the design doc:
		 * 	- we get a watch that has a different value than the key in the map but the version is larger
		 * */
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				state std::vector<Future<Void>> watchFutures;
				state Future<Void> watch1 = wait(watchKey(cx, key));
				state int i;
				
				state Value val = Value( deterministicRandom()->randomUniqueID().toString() );
				tr.set(key, val);
				for ( i = 0; i < self->numWatches; i++ ) {
					watchFutures.push_back(tr.watch(key));
				}
				wait ( tr.commit() );
				wait( watch1 );
				wait( setKeyRandomValue(cx, key, Optional<Value>()) );
				for ( i = 0; i < watchFutures.size(); i++) {
					wait( watchFutures[i] );
				}
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> case3(Database cx, Key key, WatchesSameKeyWorkload* self) {
		/**
		 * Tests case 2 for the storage server response:
		 * 	- i.e ABA but when the storage server responds the future count == 1 so we do nothing (no refire)
		 * */
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				wait ( setKeyRandomValue(cx, key, Optional<Value>()) );
				state Optional<Value> val = wait( getValue(cx, key) );
				state Future<Void> watch1 = wait(watchKey(cx, key));
				wait ( setKeyRandomValue(cx, key, Optional<Value>()) );
				
				tr.set(key, val.get());
				state Future<Void> watch2 = tr.watch(key);
				wait( tr.commit() );
				
				watch1.cancel();
				watch2.cancel();
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> case4(Database cx, Key key, WatchesSameKeyWorkload* self) {
		/**
		 * Tests case 3 for the storage server response:
		 * 	- i.e ABA but when the storage server responds the future count > 1 so we refire request to SS
		 * */
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				wait ( setKeyRandomValue(cx, key, Optional<Value>()) );
				state Optional<Value> val = wait( getValue(cx, key) );
				state Future<Void> watch1 = wait(watchKey(cx, key));
				wait ( setKeyRandomValue(cx, key, Optional<Value>()) );
				
				tr.set(key, val.get());
				state Future<Void> watch2 = tr.watch(key);
				wait( tr.commit() );

				wait( setKeyRandomValue(cx, key, Optional<Value>()) );
				wait( watch1 );
				wait( watch2 );
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> case5(Database cx, Key key, WatchesSameKeyWorkload* self) {
		/**
		 * Tests case 5 in the design doc:
		 * 	- i.e values of watches are different but versions are the same
		 * */
		state ReadYourWritesTransaction tr1(cx);
		state ReadYourWritesTransaction tr2(cx);
		loop {
			try {
				state Value val1 = Value( deterministicRandom()->randomUniqueID().toString() );
				state Value val2 = Value( deterministicRandom()->randomUniqueID().toString() );
				tr1.setOption( FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE );
				tr2.setOption( FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE );
				tr1.set(key, val1);
				tr2.set(key, val2);
				state Future<Void> watch1 = tr1.watch(key);
				state Future<Void> watch2 = tr2.watch(key);
				wait( tr1.commit() && tr2.commit() );

				wait( watch1 || watch2 ); // since we enter case 5 at least one of the watches should be fired
				wait( setKeyRandomValue(cx, key, Optional<Value>()) );
				wait( watch1 && watch2 );

				return Void();
			} catch (Error& e) {
				wait(tr1.onError(e) && tr2.onError(e));
			}
		}
	}

	void getMetrics(vector<PerfMetric>& m) override {}
};

WorkloadFactory<WatchesSameKeyWorkload> WatchesSameKeyWorkloadFactory("WatchesSameKeyCorrectness");
