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

// This tests launches a bunch of transactions with idempotency ids (so they should be idempotent automatically). Each
// transaction sets a version stamped key, and then we check that the right number of transactions were committed.
// If a transaction commits multiple times or doesn't commit, that probably indicates a problem with
// `determineCommitStatus` in NativeAPI.
struct AutomaticIdempotencyWorkload : TestWorkload {
	static constexpr auto NAME = "AutomaticIdempotencyCorrectness";
	int64_t numTransactions;
	Key keyPrefix;

	AutomaticIdempotencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numTransactions = getOption(options, "numTransactions"_sr, 1000);
		keyPrefix = KeyRef(getOption(options, "keyPrefix"_sr, "automaticIdempotencyKeyPrefix"_sr));
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	ACTOR static Future<Void> _start(AutomaticIdempotencyWorkload* self, Database cx) {
		state int i = 0;
		for (; i < self->numTransactions; ++i) {
			// Half direct representation, half indirect representation
			int length = deterministicRandom()->coinflip() ? 16 : deterministicRandom()->randomInt(17, 256);
			state Value idempotencyId = makeString(length);
			deterministicRandom()->randomBytes(mutateString(idempotencyId), length);
			TraceEvent("IdempotencyIdWorkloadTransaction").detail("Id", idempotencyId);
			wait(runRYWTransaction(
			    cx, [self = self, idempotencyId = idempotencyId](Reference<ReadYourWritesTransaction> tr) {
				    tr->setOption(FDBTransactionOptions::IDEMPOTENCY_ID, idempotencyId);
				    uint32_t index = self->keyPrefix.size();
				    Value suffix = makeString(14);
				    memset(mutateString(suffix), 0, 10);
				    memcpy(mutateString(suffix) + 10, &index, 4);
				    tr->atomicOp(self->keyPrefix.withSuffix(suffix), idempotencyId, MutationRef::SetVersionstampedKey);
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
		RangeResult result = wait(tr->getRange(prefixRange(self->keyPrefix), CLIENT_KNOBS->TOO_MANY));
		ASSERT(!result.more);
		std::unordered_set<Value> ids;
		// Make sure they're all unique - ie no transaction committed twice
		for (const auto& [k, v] : result) {
			ids.emplace(v);
		}
		if (ids.size() != self->clientCount * self->numTransactions) {
			for (const auto& [k, v] : result) {
				TraceEvent("IdempotencyIdWorkloadTransactionCommitted").detail("Id", v);
			}
		}
		ASSERT_EQ(ids.size(), self->clientCount * self->numTransactions);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<AutomaticIdempotencyWorkload> AutomaticIdempotencyWorkloadFactory;
