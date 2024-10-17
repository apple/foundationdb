/*
 * AtomicOpsApiCorrectness.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct AtomicOpsApiCorrectnessWorkload : TestWorkload {
	static constexpr auto NAME = "AtomicOpsApiCorrectness";
	bool testFailed = false;
	uint32_t opType;

private:
	static int getApiVersion(const Database& cx) { return cx->apiVersion.version(); }

	static void setApiVersion(Database* cx, int version) { (*cx)->apiVersion = ApiVersion(version); }

	Key getTestKey(std::string prefix) {
		std::string key = prefix + std::to_string(clientId);
		return StringRef(key);
	}

public:
	AtomicOpsApiCorrectnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		opType = getOption(options, "opType"_sr, -1);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (opType == -1)
			opType = sharedRandomNumber % 9;

		switch (opType) {
		case 0:
			CODE_PROBE(true, "Testing atomic Min");
			return testMin(cx->clone(), this);
		case 1:
			CODE_PROBE(true, "Testing atomic And");
			return testAnd(cx->clone(), this);
		case 2:
			CODE_PROBE(true, "Testing atomic ByteMin");
			return testByteMin(cx->clone(), this);
		case 3:
			CODE_PROBE(true, "Testing atomic ByteMax");
			return testByteMax(cx->clone(), this);
		case 4:
			CODE_PROBE(true, "Testing atomic Or");
			return testOr(cx->clone(), this);
		case 5:
			CODE_PROBE(true, "Testing atomic Max");
			return testMax(cx->clone(), this);
		case 6:
			CODE_PROBE(true, "Testing atomic Xor");
			return testXor(cx->clone(), this);
		case 7:
			CODE_PROBE(true, "Testing atomic Add");
			return testAdd(cx->clone(), this);
		case 8:
			CODE_PROBE(true, "Testing atomic CompareAndClear");
			return testCompareAndClear(cx->clone(), this);
		default:
			ASSERT(false);
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return !testFailed; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Test Atomic ops on non existing keys that results in a set
	ACTOR Future<Void> testAtomicOpSetOnNonExistingKey(Database cx,
	                                                   AtomicOpsApiCorrectnessWorkload* self,
	                                                   uint32_t opType,
	                                                   Key key) {
		state uint64_t intValue = deterministicRandom()->randomInt(0, 10000000);
		state Value val = StringRef((const uint8_t*)&intValue, sizeof(intValue));

		// Do operation on Storage Server
		loop {
			try {
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->clear(key);
					return Void();
				}));
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->atomicOp(key, val, opType);
					return Void();
				}));
				break;
			} catch (Error& e) {
				TraceEvent(SevInfo, "AtomicOpApiThrow").detail("ErrCode", e.code());
				wait(delay(1));
			}
		}
		{
			Optional<Value> outputVal = wait(runRYWTransaction(
			    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
			uint64_t output = 0;
			ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
			memcpy(&output, outputVal.get().begin(), outputVal.get().size());
			if (output != intValue) {
				TraceEvent(SevError, "AtomicOpSetOnNonExistingKeyUnexpectedOutput")
				    .detail("OpOn", "StorageServer")
				    .detail("Op", opType)
				    .detail("ExpectedOutput", intValue)
				    .detail("ActualOutput", output);
				self->testFailed = true;
			}
		}

		{
			// Do operation on RYW Layer
			Optional<Value> outputVal =
			    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
				    tr->clear(key);
				    tr->atomicOp(key, val, opType);
				    return tr->get(key);
			    }));
			uint64_t output = 0;
			ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
			memcpy(&output, outputVal.get().begin(), outputVal.get().size());
			if (output != intValue) {
				TraceEvent(SevError, "AtomicOpSetOnNonExistingKeyUnexpectedOutput")
				    .detail("OpOn", "RYWLayer")
				    .detail("Op", opType)
				    .detail("ExpectedOutput", intValue)
				    .detail("ActualOutput", output);
				self->testFailed = true;
			}
		}
		return Void();
	}

	// Test Atomic ops on non existing keys that results in a unset
	ACTOR Future<Void> testAtomicOpUnsetOnNonExistingKey(Database cx,
	                                                     AtomicOpsApiCorrectnessWorkload* self,
	                                                     uint32_t opType,
	                                                     Key key) {
		state uint64_t intValue = deterministicRandom()->randomInt(0, 10000000);
		state Value val = StringRef((const uint8_t*)&intValue, sizeof(intValue));

		// Do operation on Storage Server
		loop {
			try {
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->clear(key);
					return Void();
				}));
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->atomicOp(key, val, opType);
					return Void();
				}));
				break;
			} catch (Error& e) {
				TraceEvent(SevInfo, "AtomicOpApiThrow").detail("ErrCode", e.code());
				wait(delay(1));
			}
		}
		{
			Optional<Value> outputVal = wait(runRYWTransaction(
			    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
			uint64_t output = 0;
			ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
			memcpy(&output, outputVal.get().begin(), outputVal.get().size());
			if (output != 0) {
				TraceEvent(SevError, "AtomicOpUnsetOnNonExistingKeyUnexpectedOutput")
				    .detail("OpOn", "StorageServer")
				    .detail("Op", opType)
				    .detail("ExpectedOutput", 0)
				    .detail("ActualOutput", output);
				self->testFailed = true;
			}
		}

		{
			// Do operation on RYW Layer
			Optional<Value> outputVal =
			    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
				    tr->clear(key);
				    tr->atomicOp(key, val, opType);
				    return tr->get(key);
			    }));
			uint64_t output = 0;
			ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
			memcpy(&output, outputVal.get().begin(), outputVal.get().size());
			if (output != 0) {
				TraceEvent(SevError, "AtomicOpUnsetOnNonExistingKeyUnexpectedOutput")
				    .detail("OpOn", "RYWLayer")
				    .detail("Op", opType)
				    .detail("ExpectedOutput", 0)
				    .detail("ActualOutput", output);
				self->testFailed = true;
			}
		}
		return Void();
	}

	typedef std::function<Value(Value, Value)> DoAtomicOpOnEmptyValueFunction;

	// Test Atomic Ops when one of the value is empty
	ACTOR Future<Void> testAtomicOpOnEmptyValue(Database cx,
	                                            AtomicOpsApiCorrectnessWorkload* self,
	                                            uint32_t opType,
	                                            Key key,
	                                            DoAtomicOpOnEmptyValueFunction opFunc) {
		state Value existingVal;
		state Value otherVal;
		state uint64_t val = deterministicRandom()->randomInt(0, 10000000);
		if (deterministicRandom()->random01() < 0.5) {
			existingVal = StringRef((const uint8_t*)&val, sizeof(val));
			otherVal = StringRef();
		} else {
			otherVal = StringRef((const uint8_t*)&val, sizeof(val));
			existingVal = StringRef();
		}
		// Do operation on Storage Server
		loop {
			try {
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->set(key, existingVal);
					return Void();
				}));
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->atomicOp(key, otherVal, opType);
					return Void();
				}));
				break;
			} catch (Error& e) {
				TraceEvent(SevInfo, "AtomicOpApiThrow").detail("ErrCode", e.code());
				wait(delay(1));
			}
		}
		{
			Optional<Value> outputVal = wait(runRYWTransaction(
			    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
			ASSERT(outputVal.present());
			Value output = outputVal.get();
			if (output != opFunc(existingVal, otherVal)) {
				TraceEvent(SevError, "AtomicOpOnEmptyValueUnexpectedOutput")
				    .detail("OpOn", "StorageServer")
				    .detail("Op", opType)
				    .detail("ExpectedOutput", opFunc(existingVal, otherVal).toString())
				    .detail("ActualOutput", output.toString());
				self->testFailed = true;
			}
		}

		{
			// Do operation on RYW Layer
			Optional<Value> outputVal =
			    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
				    tr->set(key, existingVal);
				    tr->atomicOp(key, otherVal, opType);
				    return tr->get(key);
			    }));
			ASSERT(outputVal.present());
			Value output = outputVal.get();
			if (output != opFunc(existingVal, otherVal)) {
				TraceEvent(SevError, "AtomicOpOnEmptyValueUnexpectedOutput")
				    .detail("OpOn", "RYWLayer")
				    .detail("Op", opType)
				    .detail("ExpectedOutput", opFunc(existingVal, otherVal).toString())
				    .detail("ActualOutput", output.toString());
				self->testFailed = true;
			}
		}
		return Void();
	}

	typedef std::function<uint64_t(uint64_t, uint64_t)> DoAtomicOpFunction;

	// Test atomic ops in the normal case when the existing value is present
	ACTOR Future<Void> testAtomicOpApi(Database cx,
	                                   AtomicOpsApiCorrectnessWorkload* self,
	                                   uint32_t opType,
	                                   Key key,
	                                   DoAtomicOpFunction opFunc) {
		state uint64_t intValue1 = deterministicRandom()->randomInt(0, 10000000);
		state uint64_t intValue2 = deterministicRandom()->randomInt(0, 10000000);
		state Value val1 = StringRef((const uint8_t*)&intValue1, sizeof(intValue1));
		state Value val2 = StringRef((const uint8_t*)&intValue2, sizeof(intValue2));

		// Do operation on Storage Server
		loop {
			try {
				// Set the key to a random value
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->set(key, val1);
					return Void();
				}));
				// Do atomic op
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->atomicOp(key, val2, opType);
					return Void();
				}));
				break;
			} catch (Error& e) {
				TraceEvent(SevInfo, "AtomicOpApiThrow").detail("ErrCode", e.code());
				wait(delay(1));
			}
		}
		{
			// Compare result
			Optional<Value> outputVal = wait(runRYWTransaction(
			    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
			uint64_t output = 0;
			ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
			memcpy(&output, outputVal.get().begin(), outputVal.get().size());
			if (output != opFunc(intValue1, intValue2)) {
				TraceEvent(SevError, "AtomicOpApiCorrectnessUnexpectedOutput")
				    .detail("OpOn", "StorageServer")
				    .detail("InValue1", intValue1)
				    .detail("InValue2", intValue2)
				    .detail("AtomicOp", opType)
				    .detail("ExpectedOutput", opFunc(intValue1, intValue2))
				    .detail("ActualOutput", output);
				self->testFailed = true;
			}
		}

		{
			// Do operation at RYW layer
			Optional<Value> outputVal =
			    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
				    tr->set(key, val1);
				    tr->atomicOp(key, val2, opType);
				    return tr->get(key);
			    }));
			// Compare result
			uint64_t output = 0;
			ASSERT(outputVal.present() && outputVal.get().size() == sizeof(uint64_t));
			memcpy(&output, outputVal.get().begin(), outputVal.get().size());
			if (output != opFunc(intValue1, intValue2)) {
				TraceEvent(SevError, "AtomicOpApiCorrectnessUnexpectedOutput")
				    .detail("OpOn", "RYWLayer")
				    .detail("InValue1", intValue1)
				    .detail("InValue2", intValue2)
				    .detail("AtomicOp", opType)
				    .detail("ExpectedOutput", opFunc(intValue1, intValue2))
				    .detail("ActualOutput", output);
				self->testFailed = true;
			}
		}

		return Void();
	}

	ACTOR Future<Void> testCompareAndClearAtomicOpApi(Database cx,
	                                                  AtomicOpsApiCorrectnessWorkload* self,
	                                                  Key key,
	                                                  bool keySet) {
		state uint64_t opType = MutationRef::CompareAndClear;
		state uint64_t intValue1 = deterministicRandom()->randomInt(0, 10000000);
		state uint64_t intValue2 =
		    deterministicRandom()->coinflip() ? intValue1 : deterministicRandom()->randomInt(0, 10000000);

		state Value val1 = StringRef((const uint8_t*)&intValue1, sizeof(intValue1));
		state Value val2 = StringRef((const uint8_t*)&intValue2, sizeof(intValue2));
		state std::function<Optional<uint64_t>(uint64_t, uint64_t)> opFunc = [keySet](uint64_t val1, uint64_t val2) {
			if (!keySet || val1 == val2) {
				return Optional<uint64_t>();
			} else {
				return Optional<uint64_t>(val1);
			}
		};

		// Do operation on Storage Server
		loop {
			try {
				// Set the key to a random value
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					if (keySet) {
						tr->set(key, val1);
					} else {
						tr->clear(key);
					}
					return Void();
				}));

				// Do atomic op
				wait(runRYWTransactionNoRetry(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
					tr->atomicOp(key, val2, opType);
					return Void();
				}));
				break;
			} catch (Error& e) {
				TraceEvent(SevInfo, "AtomicOpApiThrow").detail("ErrCode", e.code());
				wait(delay(1));
			}
		}

		state Optional<uint64_t> expectedOutput;
		{
			// Compare result
			Optional<Value> outputVal = wait(runRYWTransaction(
			    cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> { return tr->get(key); }));
			Optional<uint64_t> expectedOutput_ = opFunc(intValue1, intValue2);
			expectedOutput = expectedOutput_;

			ASSERT(outputVal.present() == expectedOutput.present());
			if (outputVal.present()) {
				uint64_t output = 0;
				ASSERT(outputVal.get().size() == sizeof(uint64_t));
				memcpy(&output, outputVal.get().begin(), outputVal.get().size());
				if (output != expectedOutput.get()) {
					TraceEvent(SevError, "AtomicOpApiCorrectnessUnexpectedOutput")
					    .detail("OpOn", "StorageServer")
					    .detail("InValue1", intValue1)
					    .detail("InValue2", intValue2)
					    .detail("AtomicOp", opType)
					    .detail("ExpectedOutput", expectedOutput.get())
					    .detail("ActualOutput", output);
					self->testFailed = true;
				}
			}
		}

		{
			// Do operation at RYW layer
			Optional<Value> outputVal =
			    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
				    if (keySet) {
					    tr->set(key, val1);
				    } else {
					    tr->clear(key);
				    }
				    tr->atomicOp(key, val2, opType);
				    return tr->get(key);
			    }));

			// Compare result
			ASSERT(outputVal.present() == expectedOutput.present());
			if (outputVal.present()) {
				uint64_t output = 0;
				ASSERT(outputVal.get().size() == sizeof(uint64_t));
				memcpy(&output, outputVal.get().begin(), outputVal.get().size());
				if (output != expectedOutput.get()) {
					TraceEvent(SevError, "AtomicOpApiCorrectnessUnexpectedOutput")
					    .detail("OpOn", "RYWLayer")
					    .detail("InValue1", intValue1)
					    .detail("InValue2", intValue2)
					    .detail("AtomicOp", opType)
					    .detail("ExpectedOutput", expectedOutput.get())
					    .detail("ActualOutput", output);
					self->testFailed = true;
				}
			}
		}

		return Void();
	}

	ACTOR Future<Void> testMin(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state int currentApiVersion = getApiVersion(cx);
		state Key key = self->getTestKey("test_key_min_");

		TraceEvent("AtomicOpCorrectnessApiWorkload").detail("OpType", "MIN");
		// API Version 500
		setApiVersion(&cx, 500);
		TraceEvent(SevInfo, "Running Atomic Op Min Correctness Test Api Version 500").log();
		wait(self->testAtomicOpUnsetOnNonExistingKey(cx, self, MutationRef::Min, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::Min, key, [](uint64_t val1, uint64_t val2) { return val1 < val2 ? val1 : val2; }));
		wait(self->testAtomicOpOnEmptyValue(cx, self, MutationRef::Min, key, [](Value v1, Value v2) -> Value {
			uint64_t zeroVal = 0;
			if (v2.size() == 0)
				return StringRef();
			else
				return StringRef((const uint8_t*)&zeroVal, sizeof(zeroVal));
		}));

		// Current API Version
		setApiVersion(&cx, currentApiVersion);
		TraceEvent(SevInfo, "Running Atomic Op Min Correctness Current Api Version")
		    .detail("Version", currentApiVersion);
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::Min, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::Min, key, [](uint64_t val1, uint64_t val2) { return val1 < val2 ? val1 : val2; }));
		wait(self->testAtomicOpOnEmptyValue(cx, self, MutationRef::Min, key, [](Value v1, Value v2) -> Value {
			uint64_t zeroVal = 0;
			if (v2.size() == 0)
				return StringRef();
			else
				return StringRef((const uint8_t*)&zeroVal, sizeof(zeroVal));
		}));

		return Void();
	}

	ACTOR Future<Void> testMax(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_max_");

		TraceEvent(SevInfo, "Running Atomic Op MAX Correctness Current Api Version").log();
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::Max, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::Max, key, [](uint64_t val1, uint64_t val2) { return val1 > val2 ? val1 : val2; }));
		wait(self->testAtomicOpOnEmptyValue(
		    cx, self, MutationRef::Max, key, [](Value v1, Value v2) -> Value { return v2.size() ? v2 : StringRef(); }));

		return Void();
	}

	ACTOR Future<Void> testAnd(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state int currentApiVersion = getApiVersion(cx);
		state Key key = self->getTestKey("test_key_and_");

		TraceEvent("AtomicOpCorrectnessApiWorkload").detail("OpType", "AND");
		// API Version 500
		setApiVersion(&cx, 500);
		TraceEvent(SevInfo, "Running Atomic Op AND Correctness Test Api Version 500").log();
		wait(self->testAtomicOpUnsetOnNonExistingKey(cx, self, MutationRef::And, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::And, key, [](uint64_t val1, uint64_t val2) { return val1 & val2; }));
		wait(self->testAtomicOpOnEmptyValue(cx, self, MutationRef::And, key, [](Value v1, Value v2) -> Value {
			uint64_t zeroVal = 0;
			if (v2.size() == 0)
				return StringRef();
			else
				return StringRef((const uint8_t*)&zeroVal, sizeof(zeroVal));
		}));

		// Current API Version
		setApiVersion(&cx, currentApiVersion);
		TraceEvent(SevInfo, "Running Atomic Op AND Correctness Current Api Version")
		    .detail("Version", currentApiVersion);
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::And, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::And, key, [](uint64_t val1, uint64_t val2) { return val1 & val2; }));
		wait(self->testAtomicOpOnEmptyValue(cx, self, MutationRef::And, key, [](Value v1, Value v2) -> Value {
			uint64_t zeroVal = 0;
			if (v2.size() == 0)
				return StringRef();
			else
				return StringRef((const uint8_t*)&zeroVal, sizeof(zeroVal));
		}));

		return Void();
	}

	ACTOR Future<Void> testOr(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_or_");

		TraceEvent(SevInfo, "Running Atomic Op OR Correctness Current Api Version").log();
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::Or, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::Or, key, [](uint64_t val1, uint64_t val2) { return val1 | val2; }));
		wait(self->testAtomicOpOnEmptyValue(
		    cx, self, MutationRef::Or, key, [](Value v1, Value v2) -> Value { return v2.size() ? v2 : StringRef(); }));

		return Void();
	}

	ACTOR Future<Void> testXor(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_xor_");

		TraceEvent(SevInfo, "Running Atomic Op XOR Correctness Current Api Version").log();
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::Xor, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::Xor, key, [](uint64_t val1, uint64_t val2) { return val1 ^ val2; }));
		wait(self->testAtomicOpOnEmptyValue(
		    cx, self, MutationRef::Xor, key, [](Value v1, Value v2) -> Value { return v2.size() ? v2 : StringRef(); }));

		return Void();
	}

	ACTOR Future<Void> testAdd(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_add_");
		TraceEvent(SevInfo, "Running Atomic Op ADD Correctness Current Api Version").log();
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::AddValue, key));
		wait(self->testAtomicOpApi(
		    cx, self, MutationRef::AddValue, key, [](uint64_t val1, uint64_t val2) { return val1 + val2; }));
		wait(self->testAtomicOpOnEmptyValue(cx, self, MutationRef::AddValue, key, [](Value v1, Value v2) -> Value {
			return v2.size() ? v2 : StringRef();
		}));

		return Void();
	}

	ACTOR Future<Void> testCompareAndClear(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_compare_and_clear_");
		TraceEvent(SevInfo, "Running Atomic Op COMPARE_AND_CLEAR Correctness Current Api Version").log();
		wait(self->testCompareAndClearAtomicOpApi(cx, self, key, true));
		wait(self->testCompareAndClearAtomicOpApi(cx, self, key, false));
		return Void();
	}

	ACTOR Future<Void> testByteMin(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_byte_min_");

		TraceEvent(SevInfo, "Running Atomic Op BYTE_MIN Correctness Current Api Version").log();
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::ByteMin, key));
		wait(self->testAtomicOpApi(cx, self, MutationRef::ByteMin, key, [](uint64_t val1, uint64_t val2) {
			return StringRef((const uint8_t*)&val1, sizeof(val1)) < StringRef((const uint8_t*)&val2, sizeof(val2))
			           ? val1
			           : val2;
		}));
		wait(self->testAtomicOpOnEmptyValue(
		    cx, self, MutationRef::ByteMin, key, [](Value v1, Value v2) -> Value { return StringRef(); }));

		return Void();
	}

	ACTOR Future<Void> testByteMax(Database cx, AtomicOpsApiCorrectnessWorkload* self) {
		state Key key = self->getTestKey("test_key_byte_max_");

		TraceEvent(SevInfo, "Running Atomic Op BYTE_MAX Correctness Current Api Version").log();
		wait(self->testAtomicOpSetOnNonExistingKey(cx, self, MutationRef::ByteMax, key));
		wait(self->testAtomicOpApi(cx, self, MutationRef::ByteMax, key, [](uint64_t val1, uint64_t val2) {
			return StringRef((const uint8_t*)&val1, sizeof(val1)) > StringRef((const uint8_t*)&val2, sizeof(val2))
			           ? val1
			           : val2;
		}));
		wait(self->testAtomicOpOnEmptyValue(
		    cx, self, MutationRef::ByteMax, key, [](Value v1, Value v2) -> Value { return v1.size() ? v1 : v2; }));

		return Void();
	}
};

WorkloadFactory<AtomicOpsApiCorrectnessWorkload> AtomicOpsApiCorrectnessWorkloadFactory;
