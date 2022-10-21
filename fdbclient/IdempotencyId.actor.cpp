/*
 * IdempotencyId.actor.cpp
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

#include "fdbclient/IdempotencyId.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // this has to be the last include

struct IdempotencyIdKVBuilderImpl {
	Optional<Version> commitVersion;
	Optional<uint8_t> batchIndexHighOrderByte;
	BinaryWriter value{ IncludeVersion() };
};

IdempotencyIdKVBuilder::IdempotencyIdKVBuilder() : impl(PImpl<IdempotencyIdKVBuilderImpl>::create()) {}

void IdempotencyIdKVBuilder::setCommitVersion(Version commitVersion) {
	impl->commitVersion = commitVersion;
}

void IdempotencyIdKVBuilder::add(const IdempotencyIdRef& id, uint16_t batchIndex) {
	ASSERT(id.valid());
	if (impl->batchIndexHighOrderByte.present()) {
		ASSERT((batchIndex >> 8) == impl->batchIndexHighOrderByte.get());
	} else {
		impl->batchIndexHighOrderByte = batchIndex >> 8;
	}
	StringRef s = id.asStringRefUnsafe();
	impl->value << uint8_t(s.size());
	impl->value.serializeBytes(s);
	impl->value << uint8_t(batchIndex); // Low order byte of batchIndex
}

Optional<KeyValue> IdempotencyIdKVBuilder::buildAndClear() {
	ASSERT(impl->commitVersion.present());
	if (!impl->batchIndexHighOrderByte.present()) {
		return {};
	}

	BinaryWriter key{ Unversioned() };
	key.serializeBytes(idempotencyIdKeys.begin);
	key << bigEndian64(impl->commitVersion.get());
	key << impl->batchIndexHighOrderByte.get();

	Value v = impl->value.toValue();

	impl->value = BinaryWriter(IncludeVersion());
	impl->batchIndexHighOrderByte = Optional<uint8_t>();

	Optional<KeyValue> result = KeyValue();
	result.get().arena() = v.arena();
	result.get().key = key.toValue(result.get().arena());
	result.get().value = v;
	return result;
}

IdempotencyIdKVBuilder::~IdempotencyIdKVBuilder() = default;

Optional<CommitResult> kvContainsIdempotencyId(const KeyValueRef& kv, const IdempotencyIdRef& id) {
	ASSERT(id.valid());
	StringRef needle = id.asStringRefUnsafe();
	StringRef haystack = kv.value;

#ifndef _WIN32
	// The common case is that the kv does not contain the idempotency id, so early return if memmem is available
	if (memmem(haystack.begin(), haystack.size(), needle.begin(), needle.size()) == nullptr) {
		return {};
	}
#endif

	// Even if id is a substring of value, it may still not actually contain it.
	BinaryReader reader(kv.value.begin(), kv.value.size(), IncludeVersion());
	while (!reader.empty()) {
		uint8_t length;
		reader >> length;
		StringRef candidate{ reinterpret_cast<const uint8_t*>(reader.readBytes(length)), length };
		uint8_t lowOrderBatchIndex;
		reader >> lowOrderBatchIndex;
		if (candidate == needle) {
			BinaryReader reader(kv.key.begin(), kv.key.size(), Unversioned());
			reader.readBytes(idempotencyIdKeys.begin.size());
			Version commitVersion;
			reader >> commitVersion;
			commitVersion = bigEndian64(commitVersion);
			uint8_t highOrderBatchIndex;
			reader >> highOrderBatchIndex;
			return CommitResult{ commitVersion,
				                 static_cast<uint16_t>((uint16_t(highOrderBatchIndex) << 8) |
				                                       uint16_t(lowOrderBatchIndex)) };
		}
	}
	return {};
}

void forceLinkIdempotencyIdTests() {}

namespace {
IdempotencyIdRef generate(Arena& arena) {
	int length = deterministicRandom()->coinflip() ? deterministicRandom()->randomInt(16, 256) : 16;
	StringRef id = makeString(length, arena);
	deterministicRandom()->randomBytes(mutateString(id), length);
	return IdempotencyIdRef(id);
}
} // namespace

TEST_CASE("/fdbclient/IdempotencyId/basic") {
	Arena arena;
	uint16_t firstBatchIndex = deterministicRandom()->randomUInt32();
	firstBatchIndex &= 0xff7f; // ensure firstBatchIndex+5 won't change the higher order byte
	uint16_t batchIndex = firstBatchIndex;
	Version commitVersion = deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max());
	std::vector<IdempotencyIdRef> idVector; // Reference
	std::unordered_set<IdempotencyIdRef> idSet; // Make sure hash+equals works
	IdempotencyIdKVBuilder builder; // Check kv data format
	builder.setCommitVersion(commitVersion);

	for (int i = 0; i < 5; ++i) {
		auto id = generate(arena);
		idVector.emplace_back(id);
		idSet.emplace(id);
		builder.add(id, batchIndex++);
	}

	batchIndex = firstBatchIndex;
	Optional<KeyValue> kvOpt = builder.buildAndClear();
	ASSERT(kvOpt.present());
	const auto& kv = kvOpt.get();

	ASSERT(idSet.size() == idVector.size());
	for (const auto& id : idVector) {
		auto commitResult = kvContainsIdempotencyId(kv, id);
		ASSERT(commitResult.present());
		ASSERT(commitResult.get().commitVersion == commitVersion);
		ASSERT(commitResult.get().batchIndex == batchIndex++);
		ASSERT(idSet.find(id) != idSet.end());
		idSet.erase(id);
		ASSERT(idSet.find(id) == idSet.end());
	}
	ASSERT(idSet.size() == 0);

	ASSERT(!kvContainsIdempotencyId(kv, generate(arena)).present());

	return Void();
}

TEST_CASE("/fdbclient/IdempotencyId/serialization") {
	ASSERT(ObjectReader::fromStringRef<IdempotencyIdRef>(ObjectWriter::toValue(IdempotencyIdRef(), Unversioned()),
	                                                     Unversioned()) == IdempotencyIdRef());
	for (int i = 0; i < 1000; ++i) {
		Arena arena;
		auto id = generate(arena);
		auto serialized = ObjectWriter::toValue(id, Unversioned());
		IdempotencyIdRef t;
		ObjectReader reader(serialized.begin(), Unversioned());
		reader.deserialize(t);
		ASSERT(t == id);
	}
	return Void();
}

ACTOR static Future<Version> timeKeeperVersionFromUnixEpoch(int64_t time, Reference<ReadYourWritesTransaction> tr) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);
	state KeyBackedRangeResult<std::pair<int64_t, Version>> rangeResult =
	    wait(versionMap.getRange(tr, 0, time, 1, Snapshot::False, Reverse::True));
	if (time < 0) {
		return 0;
	}
	if (rangeResult.results.size() != 1) {
		// No key less than time was found in the database
		// Look for a key >= time.
		wait(store(rangeResult, versionMap.getRange(tr, time, std::numeric_limits<int64_t>::max(), 1)));

		if (rangeResult.results.size() != 1) {
			// No timekeeper entries? Use current version and time to guess
			Version version = wait(tr->getReadVersion());
			rangeResult.results.emplace_back(static_cast<int64_t>(g_network->now()), version);
		}
	}

	// Adjust version found by the delta between time and the time found and return at minimum 0
	auto& result = rangeResult.results[0];
	return std::max<Version>(0, result.second + (time - result.first) * CLIENT_KNOBS->CORE_VERSIONSPERSECOND);
}

ACTOR static Future<int64_t> timeKeeperUnixEpochFromVersion(Version v, Reference<ReadYourWritesTransaction> tr) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);

	// Binary search to find the closest date with a version <= v
	state int64_t min = 0;
	state int64_t max = (int64_t)now();
	state int64_t mid;
	state std::pair<int64_t, Version> found;
	state std::pair<int64_t, Version> best;
	Version version = wait(tr->getReadVersion());

	best = std::make_pair(static_cast<int64_t>(g_network->now()), version); // Worst case we use this for our guess

	loop {
		mid = (min + max + 1) / 2; // ceiling

		// Find the highest time < mid
		state KeyBackedRangeResult<std::pair<int64_t, Version>> rangeResult =
		    wait(versionMap.getRange(tr, min, mid, 1, Snapshot::False, Reverse::True));

		if (rangeResult.results.size() != 1) {
			if (min == mid) {
				break;
			}
			min = mid;
			break;
		}

		found = rangeResult.results[0];

		if (v < found.second) {
			if (max == found.first) {
				break;
			}
			max = found.first;
		} else {
			best = found;
			if (min == found.first) {
				break;
			}
			min = found.first;
		}
	}

	auto result = best.first + (v - best.second) / CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
	return result;
}

const static int64_t kIdmpKeySize =
    /* idempotencyIdKeys.begin.size() */ 8 + /* commit version */ 8 + /* high order batch index */ 1;
const static int64_t kMinIdmpValSize =
    /* protocol version */ 8 + /* length */ 1 + /* smallest id */ 16 + /* low order batch index */ 1;

// Based on byte sample in storage server
const static double kMinSampleProbability = double(kIdmpKeySize + kMinIdmpValSize) / (kIdmpKeySize + 100) / 250;
const static double kMinSampleSize = (kIdmpKeySize + kMinIdmpValSize) / kMinSampleProbability;

// Assuming that there are n idempotency ids, each stored in a separate kv pair, the distribution of
// getEstimatedRangeSizeBytes is kMinSampleSize * B(n, kMinSampleProbability), where B is the binomial distribution.
// https://en.wikipedia.org/wiki/Binomial_distribution#Expected_value_and_variance

ACTOR static Future<int64_t> idempotencyIdsEstimateSize(Reference<ReadYourWritesTransaction> tr) {
	
	int64_t size = wait(tr->getEstimatedRangeSizeBytes(idempotencyIdKeys));
	return size;
}

ACTOR Future<Void> idempotencyIdsCleaner(Database db,
                                         int64_t minAgeSeconds,
                                         int64_t byteTarget,
                                         int64_t pollingInterval) {
	state int64_t idmpKeySize;
	state int64_t candidateDeleteSize;
	state int64_t lastCandidateDeleteSize;
	state KeyRange candidateRangeToClean;
	state KeyRange finalRange;
	state Reference<ReadYourWritesTransaction> tr;
	state Version expiredVersion;
	state Key firstKey;
	state Version firstVersion;
	state Version canDeleteLowerThan;
	loop {
		tr = makeReference<ReadYourWritesTransaction>(db);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				wait(store(idmpKeySize, idempotencyIdsEstimateSize(tr)));
				if (idmpKeySize <= byteTarget) {
					break;
				}

				RangeResult result = wait(tr->getRange(idempotencyIdKeys, /*limit*/ 1));
				if (!result.size()) {
					break;
				}
				firstKey = result.front().key;
				{
					BinaryReader rd(firstKey, Unversioned());
					rd.readBytes(idempotencyIdKeys.begin.size());
					rd >> firstVersion;
					firstVersion = bigEndian64(firstVersion);
				}

				wait(store(canDeleteLowerThan,
				           timeKeeperVersionFromUnixEpoch(static_cast<int64_t>(g_network->now()) - minAgeSeconds, tr)));
				if (firstVersion >= canDeleteLowerThan) {
					break;
				}

				// Keep dividing the candidate range until clearing it would (probably) not take us under the byte
				// target
				lastCandidateDeleteSize = std::numeric_limits<int64_t>::max();
				loop {
					candidateRangeToClean =
					    KeyRangeRef(firstKey,
					                BinaryWriter::toValue(bigEndian64(canDeleteLowerThan), Unversioned())
					                    .withPrefix(idempotencyIdKeys.begin));
					wait(store(candidateDeleteSize, tr->getEstimatedRangeSizeBytes(candidateRangeToClean)));
					if (candidateDeleteSize == lastCandidateDeleteSize) {
						break;
					}
					TraceEvent("IdempotencyIdsCleanerCandidateDelete")
					    .detail("IdmpKeySizeEstimate", idmpKeySize)
					    .detail("ClearRangeSizeEstimate", candidateDeleteSize)
					    .detail("Range", candidateRangeToClean.toString());
					if (idmpKeySize - candidateDeleteSize >= byteTarget) {
						break;
					}
					lastCandidateDeleteSize = candidateDeleteSize;
					canDeleteLowerThan = (firstVersion + canDeleteLowerThan) / 2;
				}
				finalRange = KeyRangeRef(idempotencyIdKeys.begin, candidateRangeToClean.end);
				RangeResult finalRangeEnd = wait(tr->getRange(finalRange, /*limit*/ 1, Snapshot::False, Reverse::True));
				if (finalRangeEnd.size()) {
					tr->addReadConflictRange(finalRange);
					tr->clear(finalRange);
					BinaryReader rd(finalRangeEnd.front().key, Unversioned());
					rd.readBytes(idempotencyIdKeys.begin.size());
					rd >> expiredVersion;
					expiredVersion = bigEndian64(expiredVersion);
					tr->set(idempotencyIdsExpiredVersion,
					        ObjectWriter::toValue(IdempotencyIdsExpiredVersion{ expiredVersion }, Unversioned()));
					int64_t expiredTime = wait(timeKeeperUnixEpochFromVersion(expiredVersion, tr));
					TraceEvent("IdempotencyIdsCleanerAttempt")
					    .detail("IdmpKeySizeEstimate", idmpKeySize)
					    .detail("ClearRangeSizeEstimate", candidateDeleteSize)
					    .detail("ExpiredVersion", expiredVersion)
					    .detail("ExpiredVersionAgeEstimate", static_cast<int64_t>(g_network->now()) - expiredTime);
				}
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
		wait(delay(pollingInterval));
	}
}

ACTOR Future<Void> deleteIdempotencyKV(Database db, Version version, uint8_t highOrderBatchIndex) {
	state Transaction tr(db);
	state Key key;

	BinaryWriter wr{ Unversioned() };
	wr.serializeBytes(idempotencyIdKeys.begin);
	wr << bigEndian64(version);
	wr << highOrderBatchIndex;
	key = wr.toValue();

	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.clear(key);
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}
