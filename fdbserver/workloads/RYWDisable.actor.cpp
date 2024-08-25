/*
 * RYWDisable.actor.cpp
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
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RYWDisableWorkload : TestWorkload {
	static constexpr auto NAME = "RYWDisable";

	int nodes, keyBytes;
	double testDuration;
	std::vector<Future<Void>> clients;

	RYWDisableWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 600.0);
		nodes = getOption(options, "nodes"_sr, 100);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx, this);
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx, RYWDisableWorkload* self) {
		state double testStart = now();

		loop {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					// do some operations
					state int opType = deterministicRandom()->randomInt(0, 4);
					state bool shouldError = true;

					if (opType == 0) {
						//TraceEvent("RYWSetting");
						tr.set(self->keyForIndex(deterministicRandom()->randomInt(0, self->nodes)), StringRef());
					} else if (opType == 1) {
						//TraceEvent("RYWGetNoWait");
						Future<Optional<Value>> _ =
						    tr.get(self->keyForIndex(deterministicRandom()->randomInt(0, self->nodes)));
					} else if (opType == 2) {
						//TraceEvent("RYWGetAndWait");
						wait(success(tr.get(self->keyForIndex(deterministicRandom()->randomInt(0, self->nodes)))));
					} else {
						//TraceEvent("RYWNoOp");
						shouldError = false;
					}

					// set ryw disable, check that it fails
					try {
						tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
						if (shouldError)
							ASSERT(false);
					} catch (Error& e) {
						if (!shouldError)
							ASSERT(false);
						ASSERT(e.code() == error_code_client_invalid_operation);
					}

					wait(delay(0.1));

					if (now() - testStart > self->testDuration)
						return Void();

					if (deterministicRandom()->random01() < 0.5)
						break;

					tr.reset();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	Future<bool> check(Database const& cx) override {
		bool ok = true;
		for (int i = 0; i < clients.size(); i++)
			if (clients[i].isError())
				ok = false;
		clients.clear();
		return ok;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Key keyForIndex(uint64_t index) {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodes;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}
};

WorkloadFactory<RYWDisableWorkload> RYWDisableWorkloadFactory;
