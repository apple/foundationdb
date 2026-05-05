/*
 * WatchesSameKeyCorrectness.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "flow/DeterministicRandom.h"
#include "fdbserver/tester/workloads.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct WatchesSameKeyWorkload : TestWorkload {
	static constexpr auto NAME = "WatchesSameKeyCorrectness";
	int numWatches;
	std::vector<Future<Void>> cases;

	explicit WatchesSameKeyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numWatches = getOption(options, "numWatches"_sr, 3);
	}

	Future<Void> setup(Database const& cx) override {
		cases.push_back(case1(cx, "foo1"_sr));
		cases.push_back(case2(cx, "foo2"_sr));
		cases.push_back(case3(cx, "foo3"_sr));
		cases.push_back(case4(cx, "foo4"_sr));
		cases.push_back(case5(cx, "foo5"_sr));
		return Void();
	}

	Future<Void> start(Database const& cx) override { return waitForAll(cases); }

	Future<bool> check(Database const& cx) override {
		bool ok = true;
		for (int i = 0; i < cases.size(); i++) {
			if (cases[i].isError())
				ok = false;
		}
		cases.clear();
		return ok;
	}

	static Future<Void> setKeyRandomValue(Database cx, Key key, Optional<Value> val) {
		// set value at key to val if provided (random otherwise)
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error err;
			try {
				Value valS;
				if (!val.present())
					valS = Value(deterministicRandom()->randomUniqueID().toString());
				else
					valS = val.get();
				tr.set(key, valS);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Future<Void>> watchKey(Database cx, Key key) {
		// sets a watch on a key and returns future
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error err;
			try {
				Future<Void> watchFuture = tr.watch(key);
				co_await tr.commit();
				co_return watchFuture;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> case1(Database cx, Key key) {
		/**
		 * Tests case 2 in the design doc:
		 *  - we get a watch that has the same value as a key in the watch map
		 * */
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error err;
			try {
				std::vector<Future<Void>> watchFutures;
				int i{ 0 };

				tr.set(key, Value(deterministicRandom()->randomUniqueID().toString()));
				for (i = 0; i < numWatches; i++) { // set watches for a given k/v pair set above
					watchFutures.push_back(tr.watch(key));
				}
				co_await tr.commit();

				co_await setKeyRandomValue(cx, key, Optional<Value>()); // trigger all watches created above
				for (i = 0; i < watchFutures.size(); i++) {
					co_await watchFutures[i];
				}
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> case2(Database cx, Key key) {
		/**
		 * Tests case 3 in the design doc:
		 * 	- we get a watch that has a different value than the key in the map but the version is larger
		 * */
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error err;
			try {
				std::vector<Future<Void>> watchFutures;
				Future<Void> watch1 = co_await watchKey(cx, key);
				int i{ 0 };

				tr.set(key, Value(deterministicRandom()->randomUniqueID().toString()));
				for (i = 0; i < numWatches; i++) { // set watches for a given k/v pair set above
					watchFutures.push_back(tr.watch(key));
				}
				co_await tr.commit();
				co_await watch1;
				co_await setKeyRandomValue(cx, key, Optional<Value>()); // trigger remaining watches
				for (i = 0; i < watchFutures.size(); i++) {
					co_await watchFutures[i];
				}
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> case3(Database cx, Key key) {
		/**
		 * Tests case 2 for the storage server response:
		 * 	- i.e ABA but when the storage server responds the future count == 1 so we do nothing (no refire)
		 * */
		ReadYourWritesTransaction tr(cx);
		ReadYourWritesTransaction tr2(cx);
		Value val;
		while (true) {
			Error err;
			try {
				val = deterministicRandom()->randomUniqueID().toString();
				tr2.set(key, val);
				Future<Void> watch1 = tr2.watch(key);
				co_await tr2.commit();
				co_await setKeyRandomValue(cx, key, Optional<Value>());

				tr.set(key, val);
				Future<Void> watch2 = tr.watch(key);
				co_await tr.commit();

				watch1.cancel();
				watch2.cancel();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await (tr.onError(err) && tr2.onError(err));
		}
	}

	Future<Void> case4(Database cx, Key key) {
		/**
		 * Tests case 3 for the storage server response:
		 * 	- i.e ABA but when the storage server responds the future count > 1 so we refire request to SS
		 * */
		ReadYourWritesTransaction tr(cx);
		ReadYourWritesTransaction tr2(cx);
		while (true) {
			Error err;
			try {
				// watch1 and watch2 are set on the same k/v pair
				Value val(deterministicRandom()->randomUniqueID().toString());
				tr2.set(key, val);
				Future<Void> watch1 = tr2.watch(key);
				co_await tr2.commit();
				co_await setKeyRandomValue(cx, key, Optional<Value>());
				tr.set(key, val); // trigger ABA (line above changes value and this line changes it back)
				Future<Void> watch2 = tr.watch(key);
				co_await tr.commit();

				co_await setKeyRandomValue(
				    cx,
				    key,
				    Optional<Value>()); // since ABA has occurred we need to trigger the watches with a new value
				co_await watch1;
				co_await watch2;
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await (tr.onError(err) && tr2.onError(err));
		}
	}

	Future<Void> case5(Database cx, Key key) {
		/**
		 * Tests case 5 in the design doc:
		 * 	- i.e values of watches are different but versions are the same
		 * */
		ReadYourWritesTransaction tr1(cx);
		ReadYourWritesTransaction tr2(cx);
		while (true) {
			Error err;
			try {
				tr1.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
				tr2.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
				tr1.set(key, Value(deterministicRandom()->randomUniqueID().toString()));
				tr2.set(key, Value(deterministicRandom()->randomUniqueID().toString()));
				Future<Void> watch1 = tr1.watch(key);
				Future<Void> watch2 = tr2.watch(key);
				// each watch commits with a different value but (hopefully) the same version since there is no
				// write conflict range
				co_await (tr1.commit() && tr2.commit());

				co_await (watch1 || watch2); // since we enter case 5 at least one of the watches should be fired
				co_await setKeyRandomValue(cx, key, Optional<Value>()); // fire the watch that possibly wasn't triggered
				co_await (watch1 && watch2);

				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await (tr1.onError(err) && tr2.onError(err));
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<WatchesSameKeyWorkload> WatchesSameKeyWorkloadFactory;
