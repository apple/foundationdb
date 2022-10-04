/*
 * AutomaticIdempotencyWorkload.actor.cpp
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

#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct AutomaticIdempotencyWorkload : TestWorkload {
	int64_t countTo;
	Key key;

	AutomaticIdempotencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		countTo = getOption(options, "countTo"_sr, 1000);
		key = KeyRef(getOption(options, "key"_sr, "automaticIdempotencyKey"_sr));
	}

	std::string description() const override { return "AutomaticIdempotency"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	ACTOR static Future<Void> _start(AutomaticIdempotencyWorkload* self, Database cx) {
		state int i = 0;
		for (; i < self->countTo; ++i) {
			wait(runRYWTransaction(cx, [self = self](Reference<ReadYourWritesTransaction> tr) {
				tr->setOption(FDBTransactionOptions::AUTOMATIC_IDEMPOTENCY);
				uint64_t oneInt = 1;
				Value oneVal = StringRef(reinterpret_cast<const uint8_t*>(&oneInt), sizeof(oneInt));
				tr->atomicOp(self->key, oneVal, MutationRef::AddValue);
				return Future<Void>(Void());
			}));
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		return runRYWTransaction(cx, [this](Reference<ReadYourWritesTransaction> tr) { return _check(this, tr); });
	}

	ACTOR static Future<bool> _check(AutomaticIdempotencyWorkload* self, Reference<ReadYourWritesTransaction> tr) {
		Optional<Value> val = wait(tr->get(self->key));
		ASSERT(val.present());
		uint64_t result;
		ASSERT(val.get().size() == sizeof(result));
		memcpy(&result, val.get().begin(), val.get().size());
		ASSERT_EQ(result, self->clientCount * self->countTo);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<AutomaticIdempotencyWorkload> AutomaticIdempotencyWorkloadFactory("AutomaticIdempotencyCorrectness");
