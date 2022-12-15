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
#include "flow/BooleanParam.h"
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
		impl->value << int64_t(now());
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

	Value v = impl->value.toValue();

	KeyRef key =
	    makeIdempotencySingleKeyRange(v.arena(), impl->commitVersion.get(), impl->batchIndexHighOrderByte.get()).begin;

	impl->value = BinaryWriter(IncludeVersion());
	impl->batchIndexHighOrderByte = Optional<uint8_t>();

	Optional<KeyValue> result = KeyValue();
	result.get().arena() = v.arena();
	result.get().key = key;
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
	int64_t timestamp; // ignored
	reader >> timestamp;
	while (!reader.empty()) {
		uint8_t length;
		reader >> length;
		StringRef candidate{ reinterpret_cast<const uint8_t*>(reader.readBytes(length)), length };
		uint8_t lowOrderBatchIndex;
		reader >> lowOrderBatchIndex;
		if (candidate == needle) {
			Version commitVersion;
			uint8_t highOrderBatchIndex;
			decodeIdempotencyKey(kv.key, commitVersion, highOrderBatchIndex);
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

KeyRangeRef makeIdempotencySingleKeyRange(Arena& arena, Version version, uint8_t highOrderBatchIndex) {
	static const auto size =
	    idempotencyIdKeys.begin.size() + sizeof(version) + sizeof(highOrderBatchIndex) + /*\x00*/ 1;

	StringRef second = makeString(size, arena);
	auto* dst = mutateString(second);

	memcpy(dst, idempotencyIdKeys.begin.begin(), idempotencyIdKeys.begin.size());
	dst += idempotencyIdKeys.begin.size();

	version = bigEndian64(version);
	memcpy(dst, &version, sizeof(version));
	dst += sizeof(version);

	*dst++ = highOrderBatchIndex;

	*dst++ = 0;

	ASSERT_EQ(dst - second.begin(), size);

	return KeyRangeRef(second.removeSuffix("\x00"_sr), second);
}

void decodeIdempotencyKey(KeyRef key, Version& commitVersion, uint8_t& highOrderBatchIndex) {
	BinaryReader reader(key, Unversioned());
	reader.readBytes(idempotencyIdKeys.begin.size());
	reader >> commitVersion;
	commitVersion = bigEndian64(commitVersion);
	reader >> highOrderBatchIndex;
}

FDB_BOOLEAN_PARAM(Oldest);

// Find the youngest or oldest idempotency id key in `range` (depending on `oldest`)
// Write the timestamp to `*time` and the version to `*version` when non-null.
ACTOR static Future<Optional<Key>> getBoundary(Reference<ReadYourWritesTransaction> tr,
                                               KeyRange range,
                                               Oldest oldest,
                                               Version* version,
                                               int64_t* time) {
	RangeResult result =
	    wait(tr->getRange(range, /*limit*/ 1, Snapshot::False, oldest ? Reverse::False : Reverse::True));
	if (!result.size()) {
		return Optional<Key>();
	}
	if (version != nullptr) {
		BinaryReader rd(result.front().key, Unversioned());
		rd.readBytes(idempotencyIdKeys.begin.size());
		rd >> *version;
		*version = bigEndian64(*version);
	}
	if (time != nullptr) {
		BinaryReader rd(result.front().value, IncludeVersion());
		rd >> *time;
	}
	return result.front().key;
}

ACTOR Future<JsonBuilderObject> getIdmpKeyStatus(Database db) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	state int64_t size;
	state IdempotencyIdsExpiredVersion expired;
	state KeyBackedObjectProperty<IdempotencyIdsExpiredVersion, _Unversioned> expiredKey(idempotencyIdsExpiredVersion,
	                                                                                     Unversioned());
	state int64_t oldestIdVersion = 0;
	state int64_t oldestIdTime = 0;
	loop {
		try {
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

			wait(store(size, tr->getEstimatedRangeSizeBytes(idempotencyIdKeys)) &&
			     store(expired, expiredKey.getD(tr)) &&
			     success(getBoundary(tr, idempotencyIdKeys, Reverse::False, &oldestIdVersion, &oldestIdTime)));
			JsonBuilderObject result;
			result["size_bytes"] = size;
			if (expired.expired != 0) {
				result["expired_version"] = expired.expired;
			}
			if (expired.expiredTime != 0) {
				result["expired_age"] = int64_t(now()) - expired.expiredTime;
			}
			if (oldestIdVersion != 0) {
				result["oldest_id_version"] = oldestIdVersion;
			}
			if (oldestIdTime != 0) {
				result["oldest_id_age"] = int64_t(now()) - oldestIdTime;
			}
			return result;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> cleanIdempotencyIds(Database db, double minAgeSeconds) {
	state int64_t idmpKeySize;
	state int64_t candidateDeleteSize;
	state KeyRange finalRange;
	state Reference<ReadYourWritesTransaction> tr;

	// Only assigned to once
	state Key oldestKey;
	state Version oldestVersion;
	state int64_t oldestTime;

	// Assigned to multiple times looking for a suitable range
	state Version candidateDeleteVersion;
	state int64_t candidateDeleteTime;
	state KeyRange candidateRangeToClean;

	tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			// Check if any keys are older than minAgeSeconds
			Optional<Key> oldestKey_ =
			    wait(getBoundary(tr, idempotencyIdKeys, Oldest::True, &oldestVersion, &oldestTime));
			if (!oldestKey_.present()) {
				break;
			}
			oldestKey = oldestKey_.get();
			if (int64_t(now()) - oldestTime < minAgeSeconds) {
				break;
			}

			// Only used for a trace event
			wait(store(idmpKeySize, tr->getEstimatedRangeSizeBytes(idempotencyIdKeys)));

			// Get the version of the most recent idempotency ID
			wait(success(
			    getBoundary(tr, idempotencyIdKeys, Oldest::False, &candidateDeleteVersion, &candidateDeleteTime)));

			// Keep dividing the candidate range until clearing it would not delete something younger than
			// minAgeSeconds
			loop {

				candidateRangeToClean =
				    KeyRangeRef(oldestKey,
				                BinaryWriter::toValue(bigEndian64(candidateDeleteVersion + 1), Unversioned())
				                    .withPrefix(idempotencyIdKeys.begin));

				// We know that we're okay deleting oldestVersion at this point. Go ahead and do that.
				if (oldestVersion == candidateDeleteVersion) {
					break;
				}

				// Find the youngest key in candidate range
				wait(success(getBoundary(
				    tr, candidateRangeToClean, Oldest::False, &candidateDeleteVersion, &candidateDeleteTime)));

				// Update the range so that it ends at an idempotency id key. Since we're binary searching, the
				// candidate range was probably too large before.
				candidateRangeToClean =
				    KeyRangeRef(oldestKey,
				                BinaryWriter::toValue(bigEndian64(candidateDeleteVersion + 1), Unversioned())
				                    .withPrefix(idempotencyIdKeys.begin));

				wait(store(candidateDeleteSize, tr->getEstimatedRangeSizeBytes(candidateRangeToClean)));

				int64_t youngestAge = int64_t(now()) - candidateDeleteTime;
				TraceEvent("IdempotencyIdsCleanerCandidateDelete")
				    .detail("Range", candidateRangeToClean.toString())
				    .detail("IdmpKeySizeEstimate", idmpKeySize)
				    .detail("YoungestIdAge", youngestAge)
				    .detail("MinAgeSeconds", minAgeSeconds)
				    .detail("ClearRangeSizeEstimate", candidateDeleteSize);
				if (youngestAge > minAgeSeconds) {
					break;
				}
				candidateDeleteVersion = (oldestVersion + candidateDeleteVersion) / 2;
			}
			finalRange = KeyRangeRef(idempotencyIdKeys.begin, candidateRangeToClean.end);
			if (!finalRange.empty()) {
				tr->addReadConflictRange(finalRange);
				tr->clear(finalRange);
				tr->set(idempotencyIdsExpiredVersion,
				        ObjectWriter::toValue(IdempotencyIdsExpiredVersion{ candidateDeleteVersion }, Unversioned()));
				TraceEvent("IdempotencyIdsCleanerAttempt")
				    .detail("Range", finalRange.toString())
				    .detail("IdmpKeySizeEstimate", idmpKeySize)
				    .detail("ClearRangeSizeEstimate", candidateDeleteSize)
				    .detail("ExpiredVersion", candidateDeleteVersion)
				    .detail("ExpiredVersionAgeEstimate", static_cast<int64_t>(now()) - candidateDeleteTime);
				wait(tr->commit());
			}
			break;
		} catch (Error& e) {
			TraceEvent("IdempotencyIdsCleanerError").error(e);
			wait(tr->onError(e));
		}
	}
	return Void();
}
