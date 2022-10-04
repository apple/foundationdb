#include "fdbclient/IdempotencyId.h"
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

void IdempotencyIdKVBuilder::add(const IdempotencyId& id, uint16_t batchIndex) {
	if (impl->batchIndexHighOrderByte.present()) {
		ASSERT((batchIndex >> 8) == impl->batchIndexHighOrderByte.get());
	} else {
		impl->batchIndexHighOrderByte = batchIndex >> 8;
	}
	StringRef s = id.asStringRef();
	impl->value << uint8_t(s.size());
	impl->value.serializeBytes(s);
	impl->value << uint8_t(batchIndex); // Low order byte of batchIndex
}

KeyValue IdempotencyIdKVBuilder::buildAndClear() {
	ASSERT(impl->commitVersion.present());
	ASSERT(impl->batchIndexHighOrderByte.present());

	BinaryWriter key{ Unversioned() };
	key.serializeBytes(idempotencyIdKeys.begin);
	key << bigEndian64(impl->commitVersion.get());
	key << impl->batchIndexHighOrderByte.get();

	Value v = impl->value.toValue();

	*impl = IdempotencyIdKVBuilderImpl{};

	KeyValue result;
	result.arena() = v.arena();
	result.key = key.toValue(result.arena());
	result.value = v;
	return result;
}

Optional<CommitResult> kvContainsIdempotencyId(const KeyValueRef& kv, const IdempotencyId& id) {
	// The common case is that the kv does not contain the idempotency id
	StringRef needle = id.asStringRef();
	StringRef haystack = kv.value;
	if (memmem(haystack.begin(), haystack.size(), needle.begin(), needle.size()) == nullptr) {
		return {};
	}

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

TEST_CASE("/fdbclient/IdempotencyId/basic") {
	Arena arena;
	uint16_t firstBatchIndex = deterministicRandom()->randomUInt32();
	uint16_t batchIndex = firstBatchIndex;
	Version commitVersion = deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max());
	std::vector<IdempotencyId> idVector; // Reference
	std::unordered_set<IdempotencyId> idSet; // Make sure hash+equals works
	IdempotencyIdKVBuilder builder; // Check kv data format
	builder.setCommitVersion(commitVersion);

	for (int i = 0; i < 5; ++i) {
		UID id = deterministicRandom()->randomUniqueID();
		idVector.emplace_back(id);
		idSet.emplace(id);
		builder.add(IdempotencyId(id), batchIndex++);
	}
	for (int i = 0; i < 5; ++i) {
		int length = deterministicRandom()->randomInt(16, 256);
		StringRef id = makeString(length, arena);
		deterministicRandom()->randomBytes(mutateString(id), length);
		idVector.emplace_back(id);
		idSet.emplace(id);
		builder.add(IdempotencyId(id), batchIndex++);
	}

	batchIndex = firstBatchIndex;
	KeyValue kv = builder.buildAndClear();

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

	ASSERT(!kvContainsIdempotencyId(kv, IdempotencyId(deterministicRandom()->randomUniqueID())).present());

	return Void();
}