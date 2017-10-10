/*
 * AtomicOpsApiCorrectness.actor.cpp
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
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "workloads.h"

struct AtomicOpsApiCorrectnessWorkload : TestWorkload {
	bool testFailed = false;
	uint32_t opType;

private:
	static int getApiVersion(const Database &cx) {
		return cx->cluster->apiVersion;
	}

	static void setApiVersion(Database *cx, int version) {
		(*cx)->cluster->apiVersion = version;
	}

	Key getTestKey(std::string prefix) {
		std::string key = prefix + std::to_string(clientId);
		return StringRef(key);
	}

public:
	AtomicOpsApiCorrectnessWorkload(WorkloadContext const& wcx)
		: TestWorkload(wcx)
	{
		opType = getOption(options, LiteralStringRef("opType"), -1);
	}

	virtual std::string description() { return "AtomicOpsApiCorrectness"; }

	virtual Future<Void> setup(Database const& cx) {
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (opType == -1)
			opType = sharedRandomNumber % 4;

		switch (opType) {
		case 0:
			TEST(true); //Testing atomic Min
			return testMin(cx->clone(), this);
		case 1:
			TEST(true); //Testing atomic And
			return testAnd(cx->clone(), this);
		case 2:
			TEST(true); //Testing atomic ByteMin
			return testByteMin(cx->clone(), this);
		case 3:
			TEST(true); //Testing atomic ByteMax
			return testByteMax(cx->clone(), this);
		default:
			ASSERT(false);
		}

		return Void();
	}

	virtual Future<bool> check(Database const& cx) {
		return !testFailed;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

	// Test Atomic ops on non existing keys that results in a set
	ACTOR Future<Void> testAtomicOpSetOnNonExistingKey(Database cx, AtomicOpsApiCorrectnessWorkload* self, uint32_t opType, Key key) {
		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { tr->clear(key); return Void(); }));
		state uint64_t intValue = g_random->randomInt(0, 10000000);
		Value val = StringRef((const uint8_t*)&intValue, sizeof(intValue));
		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { tr->atomicOp(key, val, opType); return Void(); }));
		Optional<Value> outputVal = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
		uint64_t output = 0;
		ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
		memcpy(&output, outputVal.get().begin(), outputVal.get().size());
		if (output != intValue) {
			TraceEvent(SevError, "AtomicOpSetOnNonExistingKeyUnexpectedOutput").detail("ExpectedOutput", intValue).detail("ActualOutput", output);
			self->testFailed = true;
			return Void();
		}
		return Void();
	}

	// Test Atomic ops on non existing keys that results in a unset
	ACTOR Future<Void> testAtomicOpUnsetOnNonExistingKey(Database cx, AtomicOpsApiCorrectnessWorkload* self, uint32_t opType, Key key) {
		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { tr->clear(key); return Void(); }));
		state uint64_t intValue = g_random->randomInt(0, 10000000);
		Value val = StringRef((const uint8_t*)&intValue, sizeof(intValue));
		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { tr->atomicOp(key, val, opType); return Void(); }));
		Optional<Value> outputVal = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
		uint64_t output = 0;
		ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
		memcpy(&output, outputVal.get().begin(), outputVal.get().size());
		if (output != 0) {
			TraceEvent(SevError, "AtomicOpUnsetOnNonExistingKeyUnexpectedOutput").detail("ExpectedOutput", 0).detail("ActualOutput", output);
			self->testFailed = true;
			return Void();
		}
		return Void();
	}

	typedef std::function<uint64_t(uint64_t, uint64_t)> DoAtomicOpFunction;

	ACTOR Future<Void> testAtomicOpApi(Database cx, AtomicOpsApiCorrectnessWorkload* self, uint32_t opType, Key key, DoAtomicOpFunction opFunc) {
		state uint64_t intValue1 = g_random->randomInt(0, 10000000);
		Value val1 = StringRef((const uint8_t*)&intValue1, sizeof(intValue1));
		// Set the key to a random value
		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { tr->set(key, val1); return Void(); }));

		// Do atomic op
		state uint64_t intValue2 = g_random->randomInt(0, 10000000);
		Value val2 = StringRef((const uint8_t *)&intValue2, sizeof(intValue2));
		Void _ = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> { tr->atomicOp(key, val2, opType); return Void(); }));

		// Compare result
		Optional<Value> outputVal = wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
		uint64_t output = 0;
		ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
		memcpy(&output, outputVal.get().begin(), outputVal.get().size());
		if (output != opFunc(intValue1, intValue2)) {
			TraceEvent(SevError, "AtomicOpApiCorrectnessUnexpectedOutput").detail("InValue1", intValue1).detail("InValue2", intValue2).detail("AtomicOp", opType).detail("ExpectedOutput", opFunc(intValue1, intValue2)).detail("ActualOutput", output);
			self->testFailed = true;
		}
		return Void();
	}

	ACTOR Future<Void> testMin(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state int currentApiVersion = getApiVersion(cx);
		state Key key = self->getTestKey("test_key_min_");

		TraceEvent("AtomicOpCorrectnessApiWorkload").detail("opType", "MIN");
		// API Version 500
		setApiVersion(&cx, 500);
		TraceEvent(SevInfo, "Running Atomic Op Min Correctness Test Api Version 500");
		Void _ = wait(self->testAtomicOpUnsetOnNonExistingKey(cx, self, MutationRef::Min, key));
		Void _ = wait(self->testAtomicOpApi(cx, self, MutationRef::Min, key, [](uint64_t val1, uint64_t val2) { return val1 < val2 ? val1 : val2;  }));

		// Current API Version 
		setApiVersion(&cx, 510); // TODO: Remove 510
		TraceEvent(SevInfo, "Running Atomic Op Min Correctness Current Api Version").detail("Version", currentApiVersion);
		Void _ = wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::Min, key));
		Void _ = wait(self->testAtomicOpApi(cx, self, MutationRef::Min, key, [](uint64_t val1, uint64_t val2) { return val1 < val2 ? val1 : val2;  }));

		return Void();
	}

	ACTOR Future<Void> testAnd(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state int currentApiVersion = getApiVersion(cx);
		state Key key = self->getTestKey("test_key_and_");

		TraceEvent("AtomicOpCorrectnessApiWorkload").detail("opType", "AND");
		// API Version 500
		setApiVersion(&cx, 500);
		TraceEvent(SevInfo, "Running Atomic Op AND Correctness Test Api Version 500");
		Void _ = wait(self->testAtomicOpUnsetOnNonExistingKey(cx, self, MutationRef::And, key));
		Void _ = wait(self->testAtomicOpApi(cx, self, MutationRef::And, key, [](uint64_t val1, uint64_t val2) { return val1 & val2;  }));

		// Current API Version 
		setApiVersion(&cx, 510); // TODO: Remove 510
		TraceEvent(SevInfo, "Running Atomic Op AND Correctness Current Api Version").detail("Version", currentApiVersion);
		Void _ = wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::And, key));
		Void _ = wait(self->testAtomicOpApi(cx, self, MutationRef::And, key, [](uint64_t val1, uint64_t val2) { return val1 & val2;  }));

		return Void();
	}

	ACTOR Future<Void> testByteMin(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state int currentApiVersion = getApiVersion(cx);
		state Key key = self->getTestKey("test_key_byte_min_");

		TraceEvent(SevInfo, "Running Atomic Op BYTE_MIN Correctness Current Api Version").detail("Version", currentApiVersion);
		Void _ = wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::ByteMin, key));
		Void _ = wait(self->testAtomicOpApi(cx, self, MutationRef::ByteMin, key, [](uint64_t val1, uint64_t val2) { return StringRef((const uint8_t *)&val1, sizeof(val1)) < StringRef((const uint8_t *)&val2, sizeof(val2)) ? val1 : val2; }));

		return Void();
	}

	ACTOR Future<Void> testByteMax(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state int currentApiVersion = getApiVersion(cx);
		state Key key = self->getTestKey("test_key_byte_max_");

		TraceEvent(SevInfo, "Running Atomic Op BYTE_MAX Correctness Current Api Version").detail("Version", currentApiVersion);
		Void _ = wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::ByteMax, key));
		Void _ = wait(self->testAtomicOpApi(cx, self, MutationRef::ByteMax, key, [](uint64_t val1, uint64_t val2) { return StringRef((const uint8_t *)&val1, sizeof(val1)) > StringRef((const uint8_t *)&val2, sizeof(val2)) ? val1 : val2; }));

		return Void();
	}
};

WorkloadFactory<AtomicOpsApiCorrectnessWorkload> AtomicOpsApiCorrectnessWorkloadFactory("AtomicOpsApiCorrectness");
