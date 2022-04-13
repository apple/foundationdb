/*
 * SimpleAtomicAdd.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SimpleAtomicAddWorkload : TestWorkload {
	int addValue;
	int iterations;
	bool initialize;
	int initialValue;
	Key sumKey;
	double testDuration;
	std::vector<Future<Void>> clients;

	SimpleAtomicAddWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		addValue = getOption(options, LiteralStringRef("addValue"), 1);
		iterations = getOption(options, LiteralStringRef("iterations"), 100);
		initialize = getOption(options, LiteralStringRef("initialize"), false);
		initialValue = getOption(options, LiteralStringRef("initialValue"), 0);
		sumKey = getOption(options, LiteralStringRef("sumKey"), LiteralStringRef("sumKey"));
	}

	std::string description() const override { return "SimpleAtomicAdd"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId) {
			return Void();
		}
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId) {
			return true;
		}
		return _check(cx, this);
	}

	ACTOR static Future<Void> _start(Database cx, SimpleAtomicAddWorkload* self) {
		if (self->initialize) {
			wait(setInitialValue(cx, self));
		}
		for (int i = 0; i < self->iterations; ++i) {
			self->clients.push_back(timeout(applyAtomicAdd(cx, self), self->testDuration, Void()));
		}
		waitForAll(self->clients);
		return Void();
	}

	ACTOR static Future<Void> setInitialValue(Database cx, SimpleAtomicAddWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		state Value val = StringRef((const uint8_t*)&self->initialValue, sizeof(self->initialValue));
		loop {
			try {
				TraceEvent("SAASetInitialValue").detail("Key", self->sumKey).detail("Value", val);
				tr.set(self->sumKey, val);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				TraceEvent("SAASetInitialValueError").error(e);
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> applyAtomicAdd(Database cx, SimpleAtomicAddWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		state Value val = StringRef((const uint8_t*)&self->addValue, sizeof(self->addValue));
		loop {
			try {
				TraceEvent("SAABegin").detail("Key", self->sumKey).detail("Value", val);
				tr.atomicOp(self->sumKey, val, MutationRef::AddValue);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				TraceEvent("SAABeginError").error(e);
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<bool> _check(Database cx, SimpleAtomicAddWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		state uint64_t expectedValue = self->addValue * self->iterations;
		if (self->initialize) {
			expectedValue += self->initialValue;
		}
		loop {
			try {
				TraceEvent("SAACheckKey").log();
				Optional<Value> actualValue = wait(tr.get(self->sumKey));
				uint64_t actualValueInt = 0;
				if (actualValue.present()) {
					memcpy(&actualValueInt, actualValue.get().begin(), actualValue.get().size());
				}
				TraceEvent("SAACheckEqual")
				    .detail("ExpectedValue", expectedValue)
				    .detail("ActualValue", actualValueInt);
				return (expectedValue == actualValueInt);
			} catch (Error& e) {
				TraceEvent("SAACheckError").error(e);
				wait(tr.onError(e));
			}
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SimpleAtomicAddWorkload> SimpleAtomicAddWorkloadFactory("SimpleAtomicAdd");
