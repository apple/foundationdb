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
	static constexpr auto NAME = "SimpleAtomicAdd";

	int addValue;
	int iterations;
	int completedIterations = 0;
	int potentialIterations = 0;
	bool initialize;
	int initialValue;
	Key sumKey;
	double testDuration;
	std::vector<Future<Void>> clients;

	SimpleAtomicAddWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		addValue = getOption(options, "addValue"_sr, 1);
		iterations = getOption(options, "iterations"_sr, 100);
		initialize = getOption(options, "initialize"_sr, false);
		initialValue = getOption(options, "initialValue"_sr, 0);
		sumKey = getOption(options, "sumKey"_sr, "sumKey"_sr);
	}

	Future<Void> setup(Database const& cx) override {
		if (initialize) {
			return setInitialValue(cx, this);
		}
		return Void();
	}

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
		for (int i = 0; i < self->iterations; ++i) {
			self->clients.push_back(timeout(applyAtomicAdd(cx, self), self->testDuration, Void()));
		}
		wait(waitForAll(self->clients));
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
				TraceEvent("SAASetInitialValueDone").detail("Key", self->sumKey).detail("Value", val);
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
			// FIXME: only count potential iteration if it's an error where it's unknown if the commit applied or not,
			// like commit_unknown_result
			self->potentialIterations++;
			try {
				TraceEvent("SAABegin").detail("Key", self->sumKey).detail("Value", val);
				tr.atomicOp(self->sumKey, val, MutationRef::AddValue);
				wait(tr.commit());
				self->completedIterations++;
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
		// Any iteration that committed is guaranteed to be committed and counted.
		// Any iteration that did not finish may or may not have committed and thus may or may not count
		state uint64_t expectedMaxValue = self->addValue * self->potentialIterations;
		state uint64_t expectedMinValue = self->addValue * self->completedIterations;
		CODE_PROBE(expectedMaxValue != expectedMinValue, "SimpleAtomicAdd had failures and/or incomplete operations");
		if (self->initialize) {
			expectedMaxValue += self->initialValue;
			expectedMinValue += self->initialValue;
		}
		loop {
			try {
				TraceEvent("SAACheckKey").log();
				ValueReadResult actualValue = wait(tr.get(self->sumKey));
				uint64_t actualValueInt = 0;
				if (actualValue.present()) {
					memcpy(&actualValueInt, actualValue.get().begin(), actualValue.get().size());
				}
				bool correct = (actualValueInt >= expectedMinValue && actualValueInt <= expectedMaxValue);
				TraceEvent("SAACheck")
				    .detail("InitialValue", self->initialize ? self->initialValue : 0)
				    .detail("AddValue", self->addValue)
				    .detail("PotentialIterations", self->potentialIterations)
				    .detail("CompletedIterations", self->completedIterations)
				    .detail("ExpectedMinValue", expectedMinValue)
				    .detail("ExpectedMaxValue", expectedMaxValue)
				    .detail("ActualValue", actualValueInt)
				    .detail("Correct", correct);
				return correct;
			} catch (Error& e) {
				TraceEvent("SAACheckError").error(e);
				wait(tr.onError(e));
			}
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SimpleAtomicAddWorkload> SimpleAtomicAddWorkloadFactory;
