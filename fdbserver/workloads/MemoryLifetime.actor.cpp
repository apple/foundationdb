/*
 * MemoryLifetime.actor.cpp
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

#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "flow/DeterministicRandom.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct MemoryLifetime : KVWorkload {
	double testDuration;
	vector<Future<Void>> clients;

	std::string valueString;

	MemoryLifetime(WorkloadContext const& wcx)
		: KVWorkload(wcx)
	{
		testDuration = getOption( options, LiteralStringRef("testDuration"), 60.0 );
		valueString = std::string( maxValueBytes, '.' );
	}

	virtual std::string description() { return "MemoryLifetime"; }

	Value randomValue() { return StringRef( (uint8_t*)valueString.c_str(), deterministicRandom()->randomInt(minValueBytes, maxValueBytes+1) );	}

	KeySelector getRandomKeySelector() {
		return KeySelectorRef( getRandomKey(), deterministicRandom()->random01() < 0.5, deterministicRandom()->randomInt(-nodeCount, nodeCount) );
	}

	Standalone<KeyValueRef> operator()( uint64_t n ) {
		return KeyValueRef( keyForIndex( n, false ), randomValue() );
	}

	virtual Future<Void> setup( Database const& cx ) {
		return _setup(cx, this);
	}

	virtual Future<Void> start( Database const& cx ) {
		return _start(cx, this);
	}

	virtual Future<bool> check( Database const& cx ) {
		return true;
	}

	virtual void getMetrics( vector<PerfMetric>& m ) {
	}


	ACTOR Future<Void> _setup( Database cx, MemoryLifetime* self) {
		state Promise<double> loadTime;
		wait( bulkSetup( cx, self, self->nodeCount, loadTime ) );
		return Void();
	}

	ACTOR Future<Void> _start( Database cx, MemoryLifetime* self) {
		state double startTime = now();
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				int op = deterministicRandom()->randomInt(0,4);
				if(op==0) {
					state bool getRange_isReverse = deterministicRandom()->random01() < 0.5;
					state Key getRange_startKey = self->getRandomKey();
					state KeyRange getRange_queryRange = getRange_isReverse ? KeyRangeRef(normalKeys.begin, keyAfter(getRange_startKey)) : KeyRangeRef(getRange_startKey, normalKeys.end);
					state bool getRange_randomStart = deterministicRandom()->random01();
					state Value getRange_newValue = self->randomValue();
					state bool getRange_isSnapshot = deterministicRandom()->random01() < 0.5;

					//TraceEvent("MemoryLifetimeCheck").detail("IsReverse", getRange_isReverse).detail("StartKey", printable(getRange_startKey)).detail("RandomStart", getRange_randomStart).detail("NewValue", getRange_newValue.size()).detail("IsSnapshot", getRange_isSnapshot);
					if(getRange_randomStart)
						tr.set(getRange_startKey, getRange_newValue);
					state Standalone<RangeResultRef> getRange_res1 = wait( tr.getRange(getRange_queryRange, GetRangeLimits(4000), getRange_isSnapshot, getRange_isReverse) );
					tr = ReadYourWritesTransaction(cx);
					wait( delay(0.01) );
					if(getRange_randomStart)
						tr.set(getRange_startKey, getRange_newValue);
					Standalone<RangeResultRef> getRange_res2 = wait( tr.getRange(getRange_queryRange, GetRangeLimits(4000), getRange_isSnapshot, getRange_isReverse) );
					ASSERT(getRange_res1.size() == getRange_res2.size());
					for(int i = 0; i < getRange_res1.size(); i++) {
						if(getRange_res1[i].key != getRange_res2[i].key) {
							TraceEvent(SevError, "MemoryLifetimeCheckKeyError")
								.detail("Key1", printable(getRange_res1[i].key)).detail("Key2", printable(getRange_res2[i].key))
								.detail("Value1", getRange_res1[i].value.size()).detail("Value2", getRange_res2[i].value.size())
								.detail("I", i).detail("Size", getRange_res2.size());
							ASSERT(false);
						}
						if(getRange_res1[i].value != getRange_res2[i].value) {
							TraceEvent(SevError, "MemoryLifetimeCheckValueError")
								.detail("Key1", printable(getRange_res1[i].key)).detail("Key2", printable(getRange_res2[i].key))
								.detail("Value1", getRange_res1[i].value.size()).detail("Value2", getRange_res2[i].value.size())
								.detail("I", i).detail("Size", getRange_res2.size());
							ASSERT(false);
						}
					}
				} else if(op==1) {
					state Key get_startKey = self->getRandomKey();
					state bool get_randomStart = deterministicRandom()->random01();
					state Value get_newValue = self->randomValue();
					state bool get_isSnapshot = deterministicRandom()->random01() < 0.5;

					if(get_randomStart)
						tr.set(get_startKey, get_newValue);
					state Optional<Value> get_res1 = wait( tr.get(get_startKey, get_isSnapshot) );
					tr = ReadYourWritesTransaction(cx);
					wait( delay(0.01) );
					if(get_randomStart)
						tr.set(get_startKey, get_newValue);
					Optional<Value> get_res2 = wait( tr.get(get_startKey, get_isSnapshot) );
					ASSERT(get_res1 == get_res2);
				} else if(op==2) {
					state KeySelector getKey_selector = self->getRandomKeySelector();
					state bool getKey_randomStart = deterministicRandom()->random01();
					state Value getKey_newValue = self->randomValue();
					state bool getKey_isSnapshot = deterministicRandom()->random01() < 0.5;

					if(getKey_randomStart)
						tr.set(getKey_selector.getKey(), getKey_newValue);
					state Key getKey_res1 = wait( tr.getKey(getKey_selector, getKey_isSnapshot) );
					tr = ReadYourWritesTransaction(cx);
					wait( delay(0.01) );
					if(getKey_randomStart)
						tr.set(getKey_selector.getKey(), getKey_newValue);
					Key getKey_res2 = wait( tr.getKey(getKey_selector, getKey_isSnapshot) );
					ASSERT(getKey_res1 == getKey_res2);
				} else if(op==3) {
					state Key getAddress_startKey = self->getRandomKey();
					state Standalone<VectorRef<const char*>> getAddress_res1 = wait( tr.getAddressesForKey(getAddress_startKey) );
					tr = ReadYourWritesTransaction(cx);
					wait( delay(0.01) );
					//we cannot check the contents like other operations so just touch all the values to make sure we dont crash
					for (int i = 0; i < getAddress_res1.size(); i++) {
						ASSERT(NetworkAddress::parseOptional(getAddress_res1[i]).present());
					}
				}
				if(now() - startTime > self->testDuration)
					return Void();
			} catch(Error &e) {
				wait( tr.onError(e) );
			}
		}
	}
};

WorkloadFactory<MemoryLifetime> MemoryLifetimeWorkloadFactory("MemoryLifetime");
