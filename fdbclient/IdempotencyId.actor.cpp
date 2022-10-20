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
#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"

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