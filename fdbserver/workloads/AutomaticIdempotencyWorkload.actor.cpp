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

namespace {
struct ValueType {
	static constexpr FileIdentifier file_identifier = 9556754;

	Value idempotencyId;
	int64_t createdTime;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, idempotencyId, createdTime);
	}
};
} // namespace

// This tests launches a bunch of transactions with idempotency ids (so they should be idempotent automatically). Each
// transaction sets a version stamped key, and then we check that the right number of transactions were committed.
// If a transaction commits multiple times or doesn't commit, that probably indicates a problem with
// `determineCommitStatus` in NativeAPI.
struct AutomaticIdempotencyWorkload : TestWorkload {
	static constexpr auto NAME = "AutomaticIdempotencyCorrectness";
	int64_t numTransactions;
	Key keyPrefix;
	double automaticPercentage;

	bool ok = true;

	AutomaticIdempotencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numTransactions = getOption(options, "numTransactions"_sr, 2500);
		keyPrefix = KeyRef(getOption(options, "keyPrefix"_sr, "/autoIdempotency/"_sr));
		automaticPercentage = getOption(options, "automaticPercentage"_sr, 0.1);
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
				    // If we don't set AUTOMATIC_IDEMPOTENCY the idempotency id won't automatically get cleaned up, so
				    // it should create work for the cleaner.
				    tr->setOption(FDBTransactionOptions::IDEMPOTENCY_ID, idempotencyId);
				    if (deterministicRandom()->random01() < self->automaticPercentage) {
					    // We also want to exercise the automatic idempotency code path.
					    tr->setOption(FDBTransactionOptions::AUTOMATIC_IDEMPOTENCY);
				    }
				    uint32_t index = self->keyPrefix.size();
				    Value suffix = makeString(14);
				    memset(mutateString(suffix), 0, 10);
				    memcpy(mutateString(suffix) + 10, &index, 4);
				    tr->atomicOp(self->keyPrefix.withSuffix(suffix),
				                 ObjectWriter::toValue(ValueType{ idempotencyId, int64_t(now()) }, Unversioned()),
				                 MutationRef::SetVersionstampedKey);
				    return Future<Void>(Void());
			    }));
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		return testAll(this, cx);
	}

	ACTOR static Future<bool> testAll(AutomaticIdempotencyWorkload* self, Database db) {
		wait(runRYWTransaction(db,
		                       [=](Reference<ReadYourWritesTransaction> tr) { return logIdempotencyIds(self, tr); }));
		wait(runRYWTransaction(db, [=](Reference<ReadYourWritesTransaction> tr) { return testIdempotency(self, tr); }));
		return self->ok;
	}

	ACTOR static Future<Void> logIdempotencyIds(AutomaticIdempotencyWorkload* self,
	                                            Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		RangeResult result = wait(tr->getRange(idempotencyIdKeys, CLIENT_KNOBS->TOO_MANY));
		ASSERT(!result.more);
		for (const auto& [k, v] : result) {
			BinaryReader reader(k, Unversioned());
			reader.readBytes(idempotencyIdKeys.begin.size());
			Version commitVersion;
			reader >> commitVersion;
			commitVersion = bigEndian64(commitVersion);
			uint8_t highOrderBatchIndex;
			reader >> highOrderBatchIndex;
			BinaryReader valReader(v, IncludeVersion());
			int64_t timestamp; // ignored
			valReader >> timestamp;
			while (!valReader.empty()) {
				uint8_t length;
				valReader >> length;
				StringRef id{ reinterpret_cast<const uint8_t*>(valReader.readBytes(length)), length };
				uint8_t lowOrderBatchIndex;
				valReader >> lowOrderBatchIndex;
				TraceEvent("IdempotencyIdWorkloadIdCommitted")
				    .detail("CommitVersion", commitVersion)
				    .detail("HighOrderBatchIndex", highOrderBatchIndex)
				    .detail("Id", id);
			}
		}
		return Void();
	}

	// Check that each transaction committed exactly once.
	ACTOR static Future<Void> testIdempotency(AutomaticIdempotencyWorkload* self,
	                                          Reference<ReadYourWritesTransaction> tr) {
		RangeResult result = wait(tr->getRange(prefixRange(self->keyPrefix), CLIENT_KNOBS->TOO_MANY));
		ASSERT(!result.more);
		std::unordered_set<Value> ids;
		// Make sure they're all unique - ie no transaction committed twice
		for (const auto& [k, v] : result) {
			ids.emplace(v);
		}
		for (const auto& [k, rawValue] : result) {
			auto v = ObjectReader::fromStringRef<ValueType>(rawValue, Unversioned());
			BinaryReader reader(k, Unversioned());
			reader.readBytes(self->keyPrefix.size());
			Version commitVersion;
			reader >> commitVersion;
			commitVersion = bigEndian64(commitVersion);
			uint8_t highOrderBatchIndex;
			reader >> highOrderBatchIndex;
			TraceEvent("IdempotencyIdWorkloadTransactionCommitted")
			    .detail("CommitVersion", commitVersion)
			    .detail("HighOrderBatchIndex", highOrderBatchIndex)
			    .detail("Key", k)
			    .detail("Id", v.idempotencyId)
			    .detail("CreatedTime", v.createdTime);
		}
		if (ids.size() != self->clientCount * self->numTransactions) {
			self->ok = false;
		}
		ASSERT_EQ(ids.size(), self->clientCount * self->numTransactions);
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<AutomaticIdempotencyWorkload> AutomaticIdempotencyWorkloadFactory;
