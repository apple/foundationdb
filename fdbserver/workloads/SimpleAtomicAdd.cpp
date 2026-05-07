/*
 * SimpleAtomicAdd.cpp
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
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/workloads.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

struct SimpleAtomicAddWorkload : TestWorkload {
	static constexpr auto NAME = "SimpleAtomicAdd";

	int addValue;
	int iterations;
	bool initialize;
	int initialValue;
	Key sumKey;
	double testDuration;
	std::vector<Future<Void>> clients;

	explicit SimpleAtomicAddWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		addValue = getOption(options, "addValue"_sr, 1);
		iterations = getOption(options, "iterations"_sr, 100);
		initialize = getOption(options, "initialize"_sr, false);
		initialValue = getOption(options, "initialValue"_sr, 0);
		sumKey = getOption(options, "sumKey"_sr, "sumKey"_sr);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId) {
			return Void();
		}
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId) {
			return true;
		}
		return _check(cx);
	}

	Future<Void> _start(Database cx) {
		if (initialize) {
			co_await setInitialValue(cx);
		}
		for (int i = 0; i < iterations; ++i) {
			clients.push_back(timeout(applyAtomicAdd(cx), testDuration, Void()));
		}
		waitForAll(clients);
	}

	Future<Void> setInitialValue(Database cx) {
		ReadYourWritesTransaction tr(cx);
		Value val = StringRef((const uint8_t*)&initialValue, sizeof(initialValue));
		while (true) {
			Error err;
			try {
				TraceEvent("SAASetInitialValue").detail("Key", sumKey).detail("Value", val);
				tr.set(sumKey, val);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("SAASetInitialValueError").error(err);
			co_await tr.onError(err);
		}
	}

	Future<Void> applyAtomicAdd(Database cx) {
		ReadYourWritesTransaction tr(cx);
		Value val = StringRef((const uint8_t*)&addValue, sizeof(addValue));
		while (true) {
			Error err;
			try {
				TraceEvent("SAABegin").detail("Key", sumKey).detail("Value", val);
				tr.atomicOp(sumKey, val, MutationRef::AddValue);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("SAABeginError").error(err);
			co_await tr.onError(err);
		}
	}

	Future<bool> _check(Database cx) {
		ReadYourWritesTransaction tr(cx);
		uint64_t expectedValue = addValue * iterations;
		if (initialize) {
			expectedValue += initialValue;
		}
		while (true) {
			Error err;
			try {
				TraceEvent("SAACheckKey").log();
				Optional<Value> actualValue = co_await tr.get(sumKey);
				uint64_t actualValueInt = 0;
				if (actualValue.present()) {
					memcpy(&actualValueInt, actualValue.get().begin(), actualValue.get().size());
				}
				TraceEvent("SAACheckEqual")
				    .detail("ExpectedValue", expectedValue)
				    .detail("ActualValue", actualValueInt);
				co_return (expectedValue == actualValueInt);
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("SAACheckError").error(err);
			co_await tr.onError(err);
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SimpleAtomicAddWorkload> SimpleAtomicAddWorkloadFactory;
