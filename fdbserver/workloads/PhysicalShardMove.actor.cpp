/*
 *PhysicalShardMove.actor.cpp
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

#include <cstdint>
#include <limits>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
std::string printValue(const ErrorOr<Optional<Value>>& value) {
	if (value.isError()) {
		return value.getError().name();
	}
	return value.get().present() ? value.get().get().toString() : "Value Not Found.";
}
} // namespace

struct SSCheckpointWorkload : TestWorkload {
	const bool enabled;
	bool pass;

	SSCheckpointWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(!clientId), pass(true) {}

	void validationFailed(ErrorOr<Optional<Value>> expectedValue, ErrorOr<Optional<Value>> actualValue) {
		TraceEvent(SevError, "TestFailed")
		    .detail("ExpectedValue", printValue(expectedValue))
		    .detail("ActualValue", printValue(actualValue));
		pass = false;
	}

	std::string description() const override { return "SSCheckpoint"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (!enabled) {
			return Void();
		}
		return _start(this, cx);
	}

	ACTOR Future<Void> _start(SSCheckpointWorkload* self, Database cx) {
		state Key key = "TestKey"_sr;
		state Key endKey = "TestKey0"_sr;
		state Value oldValue = "TestValue"_sr;
		state Value newValue = "TestNewValue"_sr;

		wait(self->writeAndVerify(self, cx, key, oldValue));

		// Disable DD to avoid DD undoing of our move.
		int ignore = wait(setDDMode(cx, 0));
		CheckpointRecord checkpoint = wait(createCheckpoint(cx, normalKeys, 1));
        std::cout << checkpoint.toString() << std::endl;
		int ignore = wait(setDDMode(cx, 1));
		return Void();
	}

	ACTOR Future<Void> readAndVerify(SSCheckpointWorkload* self,
	                                 Database cx,
	                                 Key key,
	                                 ErrorOr<Optional<Value>> expectedValue) {
		state Transaction tr(cx);

		loop {
			try {
				state Optional<Value> res = wait(timeoutError(tr.get(key), 30.0));
				const bool equal = !expectedValue.isError() && res == expectedValue.get();
				if (!equal) {
					self->validationFailed(expectedValue, ErrorOr<Optional<Value>>(res));
				}
				break;
			} catch (Error& e) {
				if (expectedValue.isError() && expectedValue.getError().code() == e.code()) {
					break;
				}
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	ACTOR Future<Void> writeAndVerify(SSCheckpointWorkload* self, Database cx, Key key, Optional<Value> value) {
		state Transaction tr(cx);
		loop {
			try {
				if (value.present()) {
					tr.set(key, value.get());
				} else {
					tr.clear(key);
				}
				wait(timeoutError(tr.commit(), 30.0));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		wait(self->readAndVerify(self, cx, key, value));

		return Void();
	}

	Future<bool> check(Database const& cx) override { return pass; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SSCheckpointWorkload> SSCheckpointWorkloadFactory("SSCheckpointWorkload");