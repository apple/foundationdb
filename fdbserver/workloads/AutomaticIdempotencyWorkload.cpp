/*
 * AutomaticIdempotencyWorkload.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/KeyBackedTypes.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/CoroUtils.h"

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

	Future<Void> _setup(Database cx) {
		if (clientId == 0) {
			co_await sharedConfigProperty.set(cx.getReference(), sharedConfig);
		} else {
			while (true) {
				Optional<SharedConfiguration> sharedConfig = co_await sharedConfigProperty.get(cx.getReference());
				if (sharedConfig.present()) {
					this->sharedConfig = sharedConfig.get();
					break;
				}
				co_await delay(1.0);
			}
		}

		if (sharedConfig.disableAutomaticIdempotency) {
			automaticPercentage = 0;
		}
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx); }

	Future<Void> start(Database const& cx) override { return _start(cx); }

	Future<Void> _start(Database cx) {
		for (int i = 0; i < numTransactions; ++i) {
			// Half direct representation, half indirect representation
			int length = deterministicRandom()->coinflip() ? 16 : deterministicRandom()->randomInt(17, 256);
			Value idempotencyId = makeString(length);
			deterministicRandom()->randomBytes(mutateString(idempotencyId), length);
			TraceEvent("IdempotencyIdWorkloadTransaction").detail("Id", idempotencyId);
			co_await runRYWTransaction(
			    cx, [this, idempotencyId = idempotencyId](Reference<ReadYourWritesTransaction> tr) {
				    // If we don't set AUTOMATIC_IDEMPOTENCY the idempotency id won't automatically get cleaned up, so
				    // it should create work for the cleaner.
				    tr->setOption(FDBTransactionOptions::IDEMPOTENCY_ID, idempotencyId);
				    if (deterministicRandom()->random01() < automaticPercentage) {
					    // We also want to exercise the automatic idempotency code path.
					    tr->setOption(FDBTransactionOptions::AUTOMATIC_IDEMPOTENCY);
				    }
				    uint32_t index = keyPrefix.size();
				    Value suffix = makeString(14);
				    memset(mutateString(suffix), 0, 10);
				    memcpy(mutateString(suffix) + 10, &index, 4);
				    tr->atomicOp(keyPrefix.withSuffix(suffix),
				                 ObjectWriter::toValue(ValueType{ idempotencyId, int64_t(now()) }, Unversioned()),
				                 MutationRef::SetVersionstampedKey);
				    return Future<Void>(Void());
			    });
		}
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		return testAll(cx);
	}

	Future<bool> testAll(Database db) {
		co_await runRYWTransaction(db,
		                           [this](Reference<ReadYourWritesTransaction> tr) { return logIdempotencyIds(tr); });
		co_await runRYWTransaction(db, [this](Reference<ReadYourWritesTransaction> tr) { return testIdempotency(tr); });
		co_await testCleaner(db);
		co_return ok;
	}

	Future<Void> logIdempotencyIds(Reference<ReadYourWritesTransaction> tr) {
		tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		RangeResult result = co_await tr->getRange(idempotencyIdKeys, CLIENT_KNOBS->TOO_MANY);
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
	}

	// Check that each transaction committed exactly once.
	Future<Void> testIdempotency(Reference<ReadYourWritesTransaction> tr) {
		RangeResult result = co_await tr->getRange(prefixRange(keyPrefix), CLIENT_KNOBS->TOO_MANY);
		ASSERT(!result.more);
		std::unordered_set<Value> ids;
		// Make sure they're all unique - ie no transaction committed twice
		for (const auto& [k, v] : result) {
			ids.emplace(v);
		}
		for (const auto& [k, rawValue] : result) {
			auto v = ObjectReader::fromStringRef<ValueType>(rawValue, Unversioned());
			BinaryReader reader(k, Unversioned());
			reader.readBytes(keyPrefix.size());
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
		if (ids.size() != clientCount * numTransactions) {
			ok = false;
		}
		ASSERT_EQ(ids.size(), clientCount * numTransactions);
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
	Future<int64_t> getMaxTimestampDelta(Database db, int64_t numCreatedTimes) {
		std::vector<int64_t> timestamps;
		std::vector<Key> keys;

		RangeResult result = co_await runRYWTransaction(db, [](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return tr->getRange(idempotencyIdKeys, CLIENT_KNOBS->TOO_MANY);
		});

		ASSERT(!result.more);

		for (const auto& kv : result) {
			Version commitVersion;
			int64_t timestamp;
			std::vector<Key> decodedKeys = idempotencyKeyValueToTestKeys(kv, &commitVersion, &timestamp);
			for (auto const& key : decodedKeys) {
				timestamps.push_back(timestamp);
				keys.push_back(key);
			}
		}

		ReadYourWritesTransaction tr(db);
		while (true) {
			Error err;
			try {
				std::vector<Future<Optional<Value>>> futures;

				for (auto const& key : keys) {
					futures.push_back(tr.get(key));
				}

				co_await waitForAll(futures);

				int64_t maxCreatedTimeDelta = 0;
				for (int i = 0; i < futures.size(); ++i) {
					auto entry = futures[i].get();
					ASSERT(entry.present());
					auto e = ObjectReader::fromStringRef<ValueType>(entry.get(), Unversioned());
					maxCreatedTimeDelta = std::max(timestamps[i] - e.createdTime, maxCreatedTimeDelta);
				}

				if (automaticPercentage == 0) {
					ASSERT_EQ(futures.size(), numCreatedTimes);
				}

				co_return maxCreatedTimeDelta;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<int64_t> getOldestCreatedTime(Database db) {
		ReadYourWritesTransaction tr(db);
		Key key;
		Version commitVersion{ 0 };

		RangeResult result = co_await runRYWTransaction(db, [](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			return tr->getRange(idempotencyIdKeys, 1);
		});

		if (result.empty()) {
			TraceEvent("AutomaticIdempotencyNoIdsLeft").log();
			co_return -1;
		}

		int64_t timestamp;
		key = idempotencyKeyValueToTestKeys(result[0], &commitVersion, &timestamp)[0];

		// We need to use a different transaction because we set READ_SYSTEM_KEYS on this one, and we might
		// be using a tenant.
		Optional<Value> entry = co_await runRYWTransaction(
		    db, [key = key](Reference<ReadYourWritesTransaction> tr) { return tr->get(key); });

		if (!entry.present()) {
			TraceEvent(SevError, "AutomaticIdempotencyKeyMissing")
			    .detail("Key", key)
			    .detail("CommitVersion", commitVersion)
			    .detail("ReadVersion", tr.getReadVersion().get());
		}
		ASSERT(entry.present());

		auto e = ObjectReader::fromStringRef<ValueType>(entry.get(), Unversioned());
		co_return e.createdTime;
	}

	Future<bool> testCleanerOneIteration(Database db,
	                                     ActorCollection* actors,
	                                     int64_t minAgeSeconds,
	                                     int64_t maxTimestampDelta,
	                                     const std::vector<int64_t>* createdTimes) {
		Future<Void> cleaner = recurringAsync(
		    [db = db, minAgeSeconds = minAgeSeconds]() { return cleanIdempotencyIds(db, minAgeSeconds); },
		    pollingInterval,
		    true,
		    pollingInterval);

		int64_t oldestCreatedTime{ 0 };
		int64_t successes = 0;
		actors->add(cleaner);
		while (true) {
			// Oldest created time of a transaction from the workload which still has an idempotency id
			oldestCreatedTime = co_await getOldestCreatedTime(db);
			if (oldestCreatedTime == -1) {
				co_return true; // Test can't make meaningful progress anymore
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
			if (maxActualAge > minAgeSeconds * slop) {
				CODE_PROBE(true, "Idempotency cleaner more to clean");
				TraceEvent("AutomaticIdempotencyCleanerMoreToClean")
				    .detail("MaxActualAge", maxActualAge)
				    .detail("MinAgePolicy", minAgeSeconds);
				successes = 0;
				// Cleaning should happen eventually
			} else if (maxActualAge < minAgeSeconds / slop) {
				bool ok = automaticPercentage == 0;
				TraceEvent(ok ? SevInfo : SevError, "AutomaticIdempotencyCleanedTooMuch")
				    .detail("MaxActualAge", maxActualAge)
				    .detail("MinAgePolicy", minAgeSeconds)
				    .detail("InitialOldestCreatedTime", initialOldestCreatedTime)
				    .detail("OldestCreatedTime", oldestCreatedTime)
				    .detail("MaxTimestampDelta", maxTimestampDelta);
				if (!ok) {
					this->ok = false;
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
			co_await delay(pollingInterval);
		}
		cleaner.cancel();
		co_return false;
	}

	Future<std::vector<int64_t>> getCreatedTimes(Reference<ReadYourWritesTransaction> tr) {
		RangeResult result = co_await tr->getRange(prefixRange(keyPrefix), CLIENT_KNOBS->TOO_MANY);
		ASSERT(!result.more);
		std::vector<int64_t> createdTimes;
		for (const auto& [k, v] : result) {
			auto e = ObjectReader::fromStringRef<ValueType>(v, Unversioned());
			createdTimes.emplace_back(e.createdTime);
		}
		std::sort(createdTimes.begin(), createdTimes.end());
		co_return createdTimes;
	}

	// Check that min age is respected. Also test that we can tolerate concurrent cleaners.
	Future<Void> testCleaner(Database db) {
		ActorCollection actors;
		int64_t minAgeSeconds{ 0 };
		std::vector<int64_t> createdTimes;

		// Initialize minAgeSeconds to match the current status
		co_await (store(minAgeSeconds, fmap([](int64_t t) { return int64_t(now()) - t; }, getOldestCreatedTime(db))) &&
		          store(createdTimes, runRYWTransaction(db, [this](Reference<ReadYourWritesTransaction> tr) {
			                return getCreatedTimes(tr);
		                })));

		int64_t maxTimestampDelta = co_await getMaxTimestampDelta(db, createdTimes.size());

		// Slowly and somewhat randomly allow the cleaner to do more cleaning. Observe that it cleans some, but not too
		// much.
		while (true) {
			minAgeSeconds *= 1 / (slop * 2);
			if (minAgeSeconds < minMinAgeSeconds) {
				break;
			}
			auto choice =
			    co_await race(testCleanerOneIteration(db, &actors, minAgeSeconds, maxTimestampDelta, &createdTimes),
			                  actors.getResult());
			if (choice.index() == 0) {
				bool done = std::get<0>(std::move(choice));

				if (done) {
					break;
				}
			} else if (choice.index() == 1) {
				ASSERT(false);
			} else {
				UNREACHABLE();
			}
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<AutomaticIdempotencyWorkload> AutomaticIdempotencyWorkloadFactory;
