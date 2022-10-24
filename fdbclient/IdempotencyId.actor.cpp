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
	int64_t timestamp; // ignored
	reader >> timestamp;
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

/*
import math
import gmpy2
from gmpy2 import mpfr

kIdmpKeySize = 17
# Assume the worst case - one id per value
kIdmpValSize = 8 + 8 + 1 * 18
kIdmpKVSize = kIdmpKeySize + kIdmpValSize
kSampleProbability = kIdmpKVSize / (kIdmpKeySize + 100) / 250
kSampleSize = int(kIdmpKVSize / min(1, kSampleProbability))
kOverestimateFactor = 2

gmpy2.get_context().precision=100

# We want to compute a small threshold such that if the estimate is >=
# threshold, the probability of an overestimate of at least a factor of 2 is <=
# 1e-9

# Given that the estimate is threshold, what is the probability that the actual value < threshold / 2 ?
def evalThreshold(threshold):
    successes = threshold // kSampleSize
    accum = mpfr(0)
    minNumKeys = successes
    k = successes
    maxNumKeys = math.floor(threshold / kOverestimateFactor / kIdmpKVSize)
    for numKeys in range(minNumKeys, maxNumKeys + 1):
        accum += (
            mpfr(math.comb(numKeys, k))
            * (mpfr(kSampleProbability)**k)
            * (mpfr(1 - kSampleProbability) ** (numKeys - k))
        )
    accum /= mpfr(maxNumKeys - minNumKeys)
    return accum

def search():
    low = 0
    high = 10000000
    best = None
    tolerance = mpfr(1e-9)
    while True:
        mid = (low + high) // 2
        if abs(low - high) <= 1:
            return mid, best
        p = evalThreshold(mid)
        if p <= tolerance:
            best = p
            high = mid
        else:
            low = mid

print(search())
*/

// In tests we want to assert that the cleaner does not clean more than half its target bytes
// 2193749 is chosen such that the probability of the actual size being less than half the estimate is less than 1e-9
// See above for the python program used to calculate 2193749
static constexpr auto byteEstimateCutoff = 2193749;

// range should always be a subset of the range from the last call
ACTOR static Future<int64_t> idempotencyIdsEstimateSize(Reference<ReadYourWritesTransaction> tr,
                                                        KeyRange range,
                                                        Optional<KeyRange>* cached,
                                                        int64_t byteTarget) {
	state int64_t size = 0;
	if (!cached->present()) {
		wait(store(size, tr->getEstimatedRangeSizeBytes(range)));
	}
	if (cached->present() || (size <= byteEstimateCutoff && byteTarget <= 2 * byteEstimateCutoff)) {
		if (!cached->present()) {
			*cached = range;
		} else {
			ASSERT(cached->get().contains(range));
		}
		RangeResult result = wait(tr->getRange(range, CLIENT_KNOBS->TOO_MANY));
		ASSERT(!result.more);
		return result.logicalSize();
	}
	CODE_PROBE(true, "Idempotency ids cleaner using byte estimate");
	return size;
}

ACTOR static Future<Optional<Key>> getBoundary(Reference<ReadYourWritesTransaction> tr,
                                               KeyRange range,
                                               Reverse reverse,
                                               Version* version,
                                               int64_t* time) {
	RangeResult result = wait(tr->getRange(range, /*limit*/ 1, Snapshot::False, reverse));
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

ACTOR Future<Void> idempotencyIdsCleaner(Database db,
                                         int64_t minAgeSeconds,
                                         int64_t byteTarget,
                                         int64_t pollingInterval) {
	state int64_t idmpKeySize;
	state int64_t candidateDeleteSize;
	state KeyRange candidateRangeToClean;
	state KeyRange finalRange;
	state Reference<ReadYourWritesTransaction> tr;
	state Key firstKey;
	state Version firstVersion;
	state int64_t firstTime;
	state Version candidateDeleteVersion;
	state int64_t candidateDeleteTime;
	state Optional<KeyRange> cached;
	loop {
		tr = makeReference<ReadYourWritesTransaction>(db);
		loop {
			try {
				cached = Optional<KeyRange>();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);

				// Check if any keys are older than minAgeSeconds
				Optional<Key> firstKey_ =
				    wait(getBoundary(tr, idempotencyIdKeys, Reverse::False, &firstVersion, &firstTime));
				if (!firstKey_.present()) {
					break;
				}
				firstKey = firstKey_.get();
				if (int64_t(now()) - firstTime < minAgeSeconds) {
					break;
				}

				// Check if we're using more than our byte target
				wait(store(idmpKeySize, idempotencyIdsEstimateSize(tr, idempotencyIdKeys, &cached, byteTarget)));
				if (idmpKeySize <= byteTarget) {
					break;
				}

				// Get the version of the most recent idempotency ID
				wait(success(
				    getBoundary(tr, idempotencyIdKeys, Reverse::True, &candidateDeleteVersion, &candidateDeleteTime)));

				// Keep dividing the candidate range until clearing it would (probably) not take us under the byte
				// target, or delete something younger than minAgeSeconds
				loop {
					if (firstVersion == candidateDeleteVersion) {
						candidateRangeToClean = KeyRangeRef(idempotencyIdKeys.begin, idempotencyIdKeys.begin);
						break;
					}
					candidateRangeToClean =
					    KeyRangeRef(firstKey,
					                BinaryWriter::toValue(bigEndian64(candidateDeleteVersion + 1), Unversioned())
					                    .withPrefix(idempotencyIdKeys.begin));
					wait(store(candidateDeleteSize,
					           idempotencyIdsEstimateSize(tr, candidateRangeToClean, &cached, byteTarget)) &&
					     success(getBoundary(
					         tr, candidateRangeToClean, Reverse::True, &candidateDeleteVersion, &candidateDeleteTime)));
					candidateRangeToClean =
					    KeyRangeRef(firstKey,
					                BinaryWriter::toValue(bigEndian64(candidateDeleteVersion + 1), Unversioned())
					                    .withPrefix(idempotencyIdKeys.begin));
					int64_t youngestAge = int64_t(now()) - candidateDeleteTime;
					TraceEvent("IdempotencyIdsCleanerCandidateDelete")
					    .detail("Range", candidateRangeToClean.toString())
					    .detail("IdmpKeySizeEstimate", idmpKeySize)
					    .detail("YoungestIdAge", youngestAge)
					    .detail("MinAgeSeconds", minAgeSeconds)
					    .detail("ByteTarget", byteTarget)
					    .detail("ClearRangeSizeEstimate", candidateDeleteSize);
					if (idmpKeySize - candidateDeleteSize > byteTarget && youngestAge > minAgeSeconds) {
						break;
					}
					candidateDeleteVersion = (firstVersion + candidateDeleteVersion) / 2;
				}
				finalRange = KeyRangeRef(idempotencyIdKeys.begin, candidateRangeToClean.end);
				if (!finalRange.empty()) {
					tr->addReadConflictRange(finalRange);
					tr->clear(finalRange);
					tr->set(
					    idempotencyIdsExpiredVersion,
					    ObjectWriter::toValue(IdempotencyIdsExpiredVersion{ candidateDeleteVersion }, Unversioned()));
					TraceEvent("IdempotencyIdsCleanerAttempt")
					    .detail("IdmpKeySizeEstimate", idmpKeySize)
					    .detail("ClearRangeSizeEstimate", candidateDeleteSize)
					    .detail("ExpiredVersion", candidateDeleteVersion)
					    .detail("ExpiredVersionAgeEstimate", static_cast<int64_t>(now()) - candidateDeleteTime);
					wait(tr->commit());
				}
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
