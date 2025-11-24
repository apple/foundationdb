/*
 * SimpleReadWrite.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A simple workload that writes a key-value pair and then reads it back
struct SimpleReadWriteWorkload : TestWorkload {
	static constexpr auto NAME = "SimpleReadWrite";

	Key testKey;
	Value testValue;
	bool writeSuccess;
	bool readSuccess;

	SimpleReadWriteWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), writeSuccess(false), readSuccess(false) {
		// All clients use the same key so they can all read what client 0 writes
		testKey = "SimpleReadWrite/TestKey"_sr;
		testValue = "HelloFoundationDB"_sr;
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0) {
			// Only client 0 writes the test data
			return _setup(this, cx);
		}
		return Void();
	}

	ACTOR static Future<Void> _setup(SimpleReadWriteWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.set(self->testKey, self->testValue);
				wait(tr.commit());
				self->writeSuccess = true;
				TraceEvent("SimpleReadWriteSetup").detail("Key", self->testKey).detail("Value", self->testValue);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		// All clients read the value
		return _start(this, cx);
	}

	ACTOR static Future<Void> _start(SimpleReadWriteWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> value = wait(tr.get(self->testKey));
				if (value.present() && value.get() == self->testValue) {
					self->readSuccess = true;
					TraceEvent("SimpleReadWriteRead")
					    .detail("Key", self->testKey)
					    .detail("Value", value.get())
					    .detail("Expected", self->testValue);
				} else {
					TraceEvent(SevError, "SimpleReadWriteReadFailed")
					    .detail("Key", self->testKey)
					    .detail("ValuePresent", value.present())
					    .detail("Value", value.present() ? value.get() : Standalone<StringRef>("NOT_PRESENT"_sr))
					    .detail("Expected", self->testValue);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		// Verify the data is still correct
		return _check(this, cx);
	}

	ACTOR static Future<bool> _check(SimpleReadWriteWorkload* self, Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> value = wait(tr.get(self->testKey));
				if (self->clientId == 0) {
					// Client 0 should have written successfully
					if (!self->writeSuccess) {
						TraceEvent(SevError, "SimpleReadWriteCheckFailed").detail("Reason", "Write did not succeed");
						return false;
					}
				}
				// All clients should have read successfully
				if (!self->readSuccess) {
					TraceEvent(SevError, "SimpleReadWriteCheckFailed").detail("Reason", "Read did not succeed");
					return false;
				}
				// Verify the value is still correct
				if (!value.present() || value.get() != self->testValue) {
					TraceEvent(SevError, "SimpleReadWriteCheckFailed")
					    .detail("Reason", "Value mismatch")
					    .detail("ValuePresent", value.present())
					    .detail("Value", value.present() ? value.get() : Standalone<StringRef>("NOT_PRESENT"_sr))
					    .detail("Expected", self->testValue);
					return false;
				}
				TraceEvent("SimpleReadWriteCheck").detail("Key", self->testKey).detail("Value", value.get());
				return true;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("WriteSuccess", writeSuccess ? 1.0 : 0.0, Averaged::False);
		m.emplace_back("ReadSuccess", readSuccess ? 1.0 : 0.0, Averaged::False);
	}
};

WorkloadFactory<SimpleReadWriteWorkload> SimpleReadWriteWorkloadFactory;

