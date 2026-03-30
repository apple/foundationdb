/*
 * MemoryLifetime.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.actor.h"
#include "BulkSetup.h"
#include "fdbclient/ReadYourWrites.h"

struct MemoryLifetime : KVWorkload {
	static constexpr auto NAME = "MemoryLifetime";
	double testDuration;
	std::vector<Future<Void>> clients;

	MemoryLifetime(WorkloadContext const& wcx) : KVWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
	}

	KeySelector getRandomKeySelector() const {
		return KeySelectorRef(getRandomKey(),
		                      deterministicRandom()->random01() < 0.5,
		                      deterministicRandom()->randomInt(-nodeCount, nodeCount));
	}

	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n, false), randomValue()); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> setup(Database const& cx) override {
		Promise<double> loadTime;
		co_await bulkSetup(cx, this, nodeCount, loadTime);
	}

	Future<Void> start(Database const& cx) override {
		double startTime = now();
		ReadYourWritesTransaction tr(cx);
		Reverse reverse = Reverse::False;
		Snapshot snapshot = Snapshot::False;
		while (true) {
			Error err;
			try {
				tr = ReadYourWritesTransaction(cx);
				int op = deterministicRandom()->randomInt(0, 4);
				if (op == 0) {
					reverse.set(deterministicRandom()->coinflip());
					Key getRange_startKey = getRandomKey();
					KeyRange getRange_queryRange = reverse ? KeyRangeRef(normalKeys.begin, keyAfter(getRange_startKey))
					                                       : KeyRangeRef(getRange_startKey, normalKeys.end);
					bool getRange_randomStart = deterministicRandom()->random01();
					Value getRange_newValue = randomValue();
					snapshot.set(deterministicRandom()->coinflip());

					//TraceEvent("MemoryLifetimeCheck").detail("IsReverse", reverse).detail("StartKey", printable(getRange_startKey)).detail("RandomStart", getRange_randomStart).detail("NewValue", getRange_newValue.size()).detail("IsSnapshot", snapshot);
					if (getRange_randomStart)
						tr.set(getRange_startKey, getRange_newValue);
					RangeResult getRange_res1 =
					    co_await tr.getRange(getRange_queryRange, GetRangeLimits(4000), snapshot, reverse);
					tr = ReadYourWritesTransaction(cx);
					co_await delay(0.01);
					if (getRange_randomStart)
						tr.set(getRange_startKey, getRange_newValue);
					RangeResult getRange_res2 =
					    co_await tr.getRange(getRange_queryRange, GetRangeLimits(4000), snapshot, reverse);
					ASSERT(getRange_res1.size() == getRange_res2.size());
					for (int i = 0; i < getRange_res1.size(); i++) {
						if (getRange_res1[i].key != getRange_res2[i].key) {
							TraceEvent(SevError, "MemoryLifetimeCheckKeyError")
							    .detail("Key1", printable(getRange_res1[i].key))
							    .detail("Key2", printable(getRange_res2[i].key))
							    .detail("Value1", getRange_res1[i].value.size())
							    .detail("Value2", getRange_res2[i].value.size())
							    .detail("I", i)
							    .detail("Size", getRange_res2.size());
							ASSERT(false);
						}
						if (getRange_res1[i].value != getRange_res2[i].value) {
							TraceEvent(SevError, "MemoryLifetimeCheckValueError")
							    .detail("Key1", printable(getRange_res1[i].key))
							    .detail("Key2", printable(getRange_res2[i].key))
							    .detail("Value1", getRange_res1[i].value.size())
							    .detail("Value2", getRange_res2[i].value.size())
							    .detail("I", i)
							    .detail("Size", getRange_res2.size());
							ASSERT(false);
						}
					}
				} else if (op == 1) {
					Key get_startKey = getRandomKey();
					bool get_randomStart = deterministicRandom()->random01();
					Value get_newValue = randomValue();
					snapshot.set(deterministicRandom()->coinflip());

					if (get_randomStart)
						tr.set(get_startKey, get_newValue);
					Optional<Value> get_res1 = co_await tr.get(get_startKey, snapshot);
					tr = ReadYourWritesTransaction(cx);
					co_await delay(0.01);
					if (get_randomStart)
						tr.set(get_startKey, get_newValue);
					Optional<Value> get_res2 = co_await tr.get(get_startKey, snapshot);
					ASSERT(get_res1 == get_res2);
				} else if (op == 2) {
					KeySelector getKey_selector = getRandomKeySelector();
					bool getKey_randomStart = deterministicRandom()->random01();
					Value getKey_newValue = randomValue();
					snapshot.set(deterministicRandom()->coinflip());

					if (getKey_randomStart)
						tr.set(getKey_selector.getKey(), getKey_newValue);
					Key getKey_res1 = co_await tr.getKey(getKey_selector, snapshot);
					tr = ReadYourWritesTransaction(cx);
					co_await delay(0.01);
					if (getKey_randomStart)
						tr.set(getKey_selector.getKey(), getKey_newValue);
					Key getKey_res2 = co_await tr.getKey(getKey_selector, snapshot);
					ASSERT(getKey_res1 == getKey_res2);
				} else if (op == 3) {
					Key getAddress_startKey = getRandomKey();
					Standalone<VectorRef<const char*>> getAddress_res1 =
					    co_await tr.getAddressesForKey(getAddress_startKey);
					tr = ReadYourWritesTransaction(cx);
					co_await delay(0.01);
					// we cannot check the contents like other operations so just touch all the values to make sure
					// we dont crash
					for (int i = 0; i < getAddress_res1.size(); i++) {
						ASSERT(NetworkAddress::parseOptional(getAddress_res1[i]).present());
					}
				}
				if (now() - startTime > testDuration)
					co_return;
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
		}
	}
};

WorkloadFactory<MemoryLifetime> MemoryLifetimeWorkloadFactory;
