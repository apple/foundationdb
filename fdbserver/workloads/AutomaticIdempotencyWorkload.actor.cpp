/*
 * AutomaticIdempotencyWorkload.actor.cpp
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

#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
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
	int64_t minMinAgeSeconds;
	double automaticPercentage;
	constexpr static double slop = 2.0;
	double pollingInterval;
	bool disableAutomaticIdempotency = false;

	bool ok = true;

	struct SharedConfiguration {
		bool disableAutomaticIdempotency = false;

		Tuple pack() const { return Tuple::makeTuple(disableAutomaticIdempotency); }

		static SharedConfiguration unpack(Tuple const& tuple) {
			SharedConfiguration config;
			config.disableAutomaticIdempotency = tuple.getBool(0);
			return config;
		}
	};

	KeyBackedProperty<SharedConfiguration, TupleCodec<SharedConfiguration>, false> sharedConfigProperty;
	SharedConfiguration sharedConfig;

	AutomaticIdempotencyWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), sharedConfigProperty("/testConfig"_sr) {
		numTransactions = getOption(options, "numTransactions"_sr, 500);
		keyPrefix = KeyRef(getOption(options, "keyPrefix"_sr, "/autoIdempotency/"_sr));
		minMinAgeSeconds = getOption(options, "minMinAgeSeconds"_sr, 15);
		automaticPercentage = getOption(options, "automaticPercentage"_sr, 0.1);
		pollingInterval = getOption(options, "pollingInterval"_sr, 5.0);

		// Disable use of automatic idempotency most of the time so we do more extensive cleanup validation
		if (clientId == 0 && deterministicRandom()->random01() < 0.9) {
			sharedConfig.disableAutomaticIdempotency = true;
		}
	}

	ACTOR static Future<Void> _setup(AutomaticIdempotencyWorkload* self, Database cx) {
		if (self->clientId == 0) {
			wait(self->sharedConfigProperty.set(cx.getReference(), self->sharedConfig));
		} else {
			loop {
				Optional<SharedConfiguration> sharedConfig = wait(self->sharedConfigProperty.get(cx.getReference()));
				if (sharedConfig.present()) {
					self->sharedConfig = sharedConfig.get();
					break;
				}
				wait(delay(1.0));
			}
		}

		if (self->sharedConfig.disableAutomaticIdempotency) {
			self->automaticPercentage = 0;
		}

		return Void();
	}

	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

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
		wait(testCleaner(self, db));
		return self->ok;
	}

	ACTOR static Future<Void> logIdempotencyIds(AutomaticIdempotencyWorkload* self,
	                                            Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		RangeResult result = wait(tr->getRange(idempotencyIdKeys, CLIENT_KNOBS->TOO_MANY));
		ASSERT(!result.more);
		for (const auto& [k, v] : result) {
			Version commitVersion;
			uint8_t highOrderBatchIndex;
			decodeIdempotencyKey(k, commitVersion, highOrderBatchIndex);
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

	std::vector<Key> idempotencyKeyValueToTestKeys(KeyValueRef kv, Version* commitVersion, int64_t* timestamp) {
		uint8_t highOrderBatchIndex;
		decodeIdempotencyKey(kv.key, *commitVersion, highOrderBatchIndex);

		BinaryReader valReader(kv.value.begin(), kv.value.size(), IncludeVersion());
		valReader >> *timestamp;

		std::vector<Key> keys;
		while (!valReader.empty()) {
			uint8_t length;
			valReader >> length;
			StringRef id{ reinterpret_cast<const uint8_t*>(valReader.readBytes(length)), length };
			uint8_t lowOrderBatchIndex;
			valReader >> lowOrderBatchIndex;

			// Recover the key written in the transaction associated with this idempotency id
			BinaryWriter keyWriter(Unversioned());
			keyWriter.serializeBytes(keyPrefix);
			keyWriter.serializeBinaryItem(bigEndian64(*commitVersion));
			keyWriter.serializeBinaryItem(highOrderBatchIndex);
			keyWriter.serializeBinaryItem(lowOrderBatchIndex);

			keys.push_back(keyWriter.toValue());
		}

		ASSERT(!keys.empty());
		return keys;
	}

	// Returns the largest gap between createdTime and the idempotency timestamp
	ACTOR static Future<int64_t> getMaxTimestampDelta(AutomaticIdempotencyWorkload* self,
	                                                  Database db,
	                                                  int64_t numCreatedTimes) {
		state std::vector<int64_t> timestamps;
		state std::vector<Key> keys;

		RangeResult result = wait(runRYWTransaction(db, [](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return tr->getRange(idempotencyIdKeys, CLIENT_KNOBS->TOO_MANY);
		}));

		ASSERT(!result.more);

		for (const auto& kv : result) {
			Version commitVersion;
			int64_t timestamp;
			std::vector<Key> decodedKeys = self->idempotencyKeyValueToTestKeys(kv, &commitVersion, &timestamp);
			for (auto const& key : decodedKeys) {
				timestamps.push_back(timestamp);
				keys.push_back(key);
			}
		}

		state ReadYourWritesTransaction tr(db);
		loop {
			try {
				state std::vector<Future<Optional<Value>>> futures;

				for (auto const& key : keys) {
					futures.push_back(tr.get(key));
				}

				wait(waitForAll(futures));

				int64_t maxCreatedTimeDelta = 0;
				for (int i = 0; i < futures.size(); ++i) {
					auto entry = futures[i].get();
					ASSERT(entry.present());
					auto e = ObjectReader::fromStringRef<ValueType>(entry.get(), Unversioned());
					maxCreatedTimeDelta = std::max(timestamps[i] - e.createdTime, maxCreatedTimeDelta);
				}

				if (self->automaticPercentage == 0) {
					ASSERT_EQ(futures.size(), numCreatedTimes);
				}

				return maxCreatedTimeDelta;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<int64_t> getOldestCreatedTime(AutomaticIdempotencyWorkload* self, Database db) {
		state ReadYourWritesTransaction tr(db);
		state Key key;
		state Version commitVersion;

		state RangeResult result = wait(runRYWTransaction(db, [](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return tr->getRange(idempotencyIdKeys, 1);
		}));

		if (result.empty()) {
			TraceEvent("AutomaticIdempotencyNoIdsLeft").log();
			return -1;
		}

		int64_t timestamp;
		key = self->idempotencyKeyValueToTestKeys(result[0], &commitVersion, &timestamp)[0];

		// We need to use a different transaction because we set READ_SYSTEM_KEYS on this one, and we might
		// be using a tenant.
		Optional<Value> entry =
		    wait(runRYWTransaction(db, [key = key](Reference<ReadYourWritesTransaction> tr) { return tr->get(key); }));

		if (!entry.present()) {
			TraceEvent(SevError, "AutomaticIdempotencyKeyMissing")
			    .detail("Key", key)
			    .detail("CommitVersion", commitVersion)
			    .detail("ReadVersion", tr.getReadVersion().get());
		}
		ASSERT(entry.present());

		auto e = ObjectReader::fromStringRef<ValueType>(entry.get(), Unversioned());
		return e.createdTime;
	}

	ACTOR static Future<bool> testCleanerOneIteration(AutomaticIdempotencyWorkload* self,
	                                                  Database db,
	                                                  ActorCollection* actors,
	                                                  int64_t minAgeSeconds,
	                                                  int64_t maxTimestampDelta,
	                                                  const std::vector<int64_t>* createdTimes) {
		state Future<Void> cleaner = recurringAsync(
		    [db = db, minAgeSeconds = minAgeSeconds]() { return cleanIdempotencyIds(db, minAgeSeconds); },
		    self->pollingInterval,
		    true,
		    self->pollingInterval);

		state int64_t oldestCreatedTime;
		state int64_t successes = 0;
		actors->add(cleaner);
		loop {
			// Oldest created time of a transaction from the workload which still has an idempotency id
			wait(store(oldestCreatedTime, getOldestCreatedTime(self, db)));
			if (oldestCreatedTime == -1) {
				return true; // Test can't make meaningful progress anymore
			}

			// oldestCreatedTime could seem too high if there's a large gap in the age
			// of entries, so account for this by making oldestCreatedTime one more than
			// the youngest entry that actually got deleted.
			//
			// Because of some uncertainty around what the most recently deleted entry
			// was, we subtract out the largest observed gap between idempotency timestamp
			// and created timestamp.
			int64_t initialOldestCreatedTime = oldestCreatedTime;
			auto iter =
			    std::lower_bound(createdTimes->begin(), createdTimes->end(), oldestCreatedTime - maxTimestampDelta);
			if (iter != createdTimes->begin()) {
				--iter;
				oldestCreatedTime = *iter + 1;
			}
			auto maxActualAge = int64_t(now()) - oldestCreatedTime;
			if (maxActualAge > minAgeSeconds * self->slop) {
				CODE_PROBE(true, "Idempotency cleaner more to clean");
				TraceEvent("AutomaticIdempotencyCleanerMoreToClean")
				    .detail("MaxActualAge", maxActualAge)
				    .detail("MinAgePolicy", minAgeSeconds);
				successes = 0;
				// Cleaning should happen eventually
			} else if (maxActualAge < minAgeSeconds / self->slop) {
				bool ok = self->automaticPercentage == 0;
				TraceEvent(ok ? SevInfo : SevError, "AutomaticIdempotencyCleanedTooMuch")
				    .detail("MaxActualAge", maxActualAge)
				    .detail("MinAgePolicy", minAgeSeconds)
				    .detail("InitialOldestCreatedTime", initialOldestCreatedTime)
				    .detail("OldestCreatedTime", oldestCreatedTime)
				    .detail("MaxTimestampDelta", maxTimestampDelta);
				if (!ok) {
					self->ok = false;
				}
				ASSERT(ok);
				successes = 0;
			} else {
				++successes;
				TraceEvent("AutomaticIdempotencyCleanerSuccess")
				    .detail("MaxActualAge", maxActualAge)
				    .detail("MinAgePolicy", minAgeSeconds)
				    .detail("Successes", successes);
				if (successes >= 10) {
					break;
				}
			}
			wait(delay(self->pollingInterval));
		}
		cleaner.cancel();
		return false;
	}

	ACTOR static Future<std::vector<int64_t>> getCreatedTimes(AutomaticIdempotencyWorkload* self,
	                                                          Reference<ReadYourWritesTransaction> tr) {
		RangeResult result = wait(tr->getRange(prefixRange(self->keyPrefix), CLIENT_KNOBS->TOO_MANY));
		ASSERT(!result.more);
		std::vector<int64_t> createdTimes;
		for (const auto& [k, v] : result) {
			auto e = ObjectReader::fromStringRef<ValueType>(v, Unversioned());
			createdTimes.emplace_back(e.createdTime);
		}
		std::sort(createdTimes.begin(), createdTimes.end());
		return createdTimes;
	}

	// Check that min age is respected. Also test that we can tolerate concurrent cleaners.
	ACTOR static Future<Void> testCleaner(AutomaticIdempotencyWorkload* self, Database db) {
		state ActorCollection actors;
		state int64_t minAgeSeconds;
		state std::vector<int64_t> createdTimes;

		// Initialize minAgeSeconds to match the current status
		wait(store(minAgeSeconds, fmap([](int64_t t) { return int64_t(now()) - t; }, getOldestCreatedTime(self, db))) &&
		     store(createdTimes, runRYWTransaction(db, [self = self](Reference<ReadYourWritesTransaction> tr) {
			           return getCreatedTimes(self, tr);
		           })));

		state int64_t maxTimestampDelta = wait(getMaxTimestampDelta(self, db, createdTimes.size()));

		// Slowly and somewhat randomly allow the cleaner to do more cleaning. Observe that it cleans some, but not too
		// much.
		loop {
			minAgeSeconds *= 1 / (self->slop * 2);
			if (minAgeSeconds < self->minMinAgeSeconds) {
				break;
			}
			choose {
				when(bool done = wait(
				         testCleanerOneIteration(self, db, &actors, minAgeSeconds, maxTimestampDelta, &createdTimes))) {
					if (done) {
						break;
					}
				}
				when(wait(actors.getResult())) {
					ASSERT(false);
				}
			}
		}
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<AutomaticIdempotencyWorkload> AutomaticIdempotencyWorkloadFactory;
