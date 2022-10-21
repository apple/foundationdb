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
	int64_t minByteTarget;
	int64_t minMinAgeSeconds;
	double automaticPercentage;
	double slop;
	double pollingInterval;

	bool ok = true;

	AutomaticIdempotencyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		numTransactions = getOption(options, "numTransactions"_sr, 2500);
		keyPrefix = KeyRef(getOption(options, "keyPrefix"_sr, "/autoIdempotency/"_sr));
		minByteTarget = getOption(options, "minByteTarget"_sr, 50000);
		minMinAgeSeconds = getOption(options, "minMinAgeSeconds"_sr, 15);
		automaticPercentage = getOption(options, "automaticPercentage"_sr, 0.1);
		slop = getOption(options, "slop"_sr, 1.2);
		pollingInterval = getOption(options, "pollingInterval"_sr, 1.0);
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
		wait(runRYWTransaction(db, [=](Reference<ReadYourWritesTransaction> tr) { return testIdempotency(self, tr); }));
		wait(testCleaner(self, db));
		return self->ok;
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
		if (ids.size() != self->clientCount * self->numTransactions) {
			for (const auto& [k, rawValue] : result) {
				auto v = ObjectReader::fromStringRef<ValueType>(rawValue, Unversioned());
				TraceEvent("IdempotencyIdWorkloadTransactionCommitted")
				    .detail("Id", v.idempotencyId)
				    .detail("CreatedTime", v.createdTime);
			}
			self->ok = false;
		}
		ASSERT_EQ(ids.size(), self->clientCount * self->numTransactions);
		return Void();
	}

	ACTOR static Future<int64_t> getIdmpKeySize(Database db) {
		state ReadYourWritesTransaction tr(db);
		loop {
			try {
				int64_t size = wait(tr.getEstimatedRangeSizeBytes(idempotencyIdKeys));
				return size;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<int64_t> getMaxAge(AutomaticIdempotencyWorkload* self, Database db) {
		state ReadYourWritesTransaction tr(db);
		state RangeResult result;
		state Key key;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				wait(store(result, tr.getRange(idempotencyIdKeys, /*limit*/ 1)));
				if (result.empty()) {
					return 0;
				}
				for (const auto& [k, v] : result) {
					BinaryReader keyReader(k.begin(), k.size(), Unversioned());
					keyReader.readBytes(idempotencyIdKeys.begin.size());
					Version commitVersion;
					keyReader >> commitVersion;
					commitVersion = bigEndian64(commitVersion);
					uint8_t highOrderBatchIndex;
					keyReader >> highOrderBatchIndex;
					BinaryReader valReader(v.begin(), v.size(), IncludeVersion());
					uint8_t length;
					valReader >> length;
					StringRef id{ reinterpret_cast<const uint8_t*>(valReader.readBytes(length)), length };
					uint8_t lowOrderBatchIndex;
					valReader >> lowOrderBatchIndex;
					BinaryWriter keyWriter(Unversioned());
					keyWriter.serializeBytes(self->keyPrefix);
					keyWriter.serializeBinaryItem(bigEndian64(commitVersion));
					keyWriter.serializeBinaryItem(highOrderBatchIndex);
					keyWriter.serializeBinaryItem(lowOrderBatchIndex);
					key = keyWriter.toValue();
					Optional<Value> entry = wait(tr.get(key));
					if (!entry.present()) {
						TraceEvent(SevError, "AutomaticIdempotencyKeyMissing").detail("Key", key);
					}
					ASSERT(entry.present());
					auto e = ObjectReader::fromStringRef<ValueType>(entry.get(), Unversioned());
					return int64_t(now()) - e.createdTime;
				}
				ASSERT(false);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> testCleanerOneIteration(AutomaticIdempotencyWorkload* self,
	                                                  Database db,
	                                                  ActorCollection* actors,
	                                                  int64_t minAgeSeconds,
	                                                  int64_t byteTarget) {
		state Future<Void> cleaner = idempotencyIdsCleaner(db, minAgeSeconds, byteTarget, self->pollingInterval);
		state int64_t size;
		state int64_t maxAge;
		state int64_t successes = 0;
		actors->add(cleaner);
		loop {
			wait(store(size, getIdmpKeySize(db)) && store(maxAge, getMaxAge(self, db)));
			if (size > byteTarget * self->slop && maxAge > minAgeSeconds * self->slop) {
				CODE_PROBE(true, "Idempotency cleaner more to clean");
				TraceEvent("AutomaticIdempotencyCleanerMoreToClean")
				    .detail("Size", size)
				    .detail("ByteTarget", byteTarget)
				    .detail("MaxActualAge", maxAge)
				    .detail("MinAgePolicy", minAgeSeconds);
				successes = 0;
				// Cleaning should happen eventually
				wait(delay(self->pollingInterval));
			} else if (size < byteTarget / self->slop || maxAge < minAgeSeconds / self->slop) {
				TraceEvent(SevError, "AutomaticIdempotencyCleanedTooMuch")
				    .detail("Size", size)
				    .detail("ByteTarget", byteTarget)
				    .detail("MaxActualAge", maxAge)
				    .detail("MinAgePolicy", minAgeSeconds);
				self->ok = false;
				ASSERT(false);
			} else {
				++successes;
				TraceEvent("AutomaticIdempotencyCleanerSuccess")
				    .detail("Size", size)
				    .detail("ByteTarget", byteTarget)
				    .detail("MaxActualAge", maxAge)
				    .detail("MinAgePolicy", minAgeSeconds)
				    .detail("Successes", successes);
				if (successes >= 10) {
					break;
				}
				wait(delay(self->pollingInterval));
			}
		}
		cleaner.cancel();
		return Void();
	}

	// Check that min age and byte target are respected. Also test that we can tolerate concurrent cleaners.
	ACTOR static Future<Void> testCleaner(AutomaticIdempotencyWorkload* self, Database db) {
		state ActorCollection actors;
		state int64_t minAgeSeconds;
		state int64_t byteTarget;

		// Initialize the byteTarget and minAgeSeconds to match the current status
		wait(store(byteTarget, getIdmpKeySize(db)) && store(minAgeSeconds, getMaxAge(self, db)));

		// Slowly and somewhat randomly allow the cleaner to do more cleaning. Observe that it cleans some, but not too
		// much.
		loop {
			if (deterministicRandom()->coinflip()) {
				minAgeSeconds *= 1 / (self->slop * 2);
			} else {
				byteTarget *= 1 / (self->slop * 2);
			}
			if (minAgeSeconds < self->minMinAgeSeconds) {
				break;
			}
			if (byteTarget < self->minByteTarget) {
				break;
			}
			choose {
				when(wait(testCleanerOneIteration(self, db, &actors, minAgeSeconds, byteTarget))) {}
				when(wait(actors.getResult())) { ASSERT(false); }
			}
		}
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<AutomaticIdempotencyWorkload> AutomaticIdempotencyWorkloadFactory;
