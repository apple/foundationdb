/*
 * RangeLockEncodingTests.cpp
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

#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"

#include <cstring>
#include <limits>
#include <map>
#include <string>
#include <vector>

namespace {

// Wire-shaped fixtures can represent old/additive schemas and invalid ranges
// without calling RangeLockState or KeyRange constructors that assert.
struct EncodingTestRange {
	std::string begin;
	std::string end;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end);
	}
};

template <bool IncludeLockId = true, bool IncludeFutureField = false>
struct EncodingTestState {
	std::string owner = "owner";
	uint8_t type = static_cast<uint8_t>(RangeLockType::ExclusiveReadLock);
	EncodingTestRange range{ "a", "z" };
	std::string lockId;
	std::string futureField = "ignored future field";

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (IncludeFutureField) {
			serializer(ar, owner, type, range, lockId, futureField);
		} else if constexpr (IncludeLockId) {
			serializer(ar, owner, type, range, lockId);
		} else {
			serializer(ar, owner, type, range);
		}
	}
};

template <class State = EncodingTestState<>>
struct EncodingTestSet {
	std::map<std::string, State> locks;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locks);
	}
};

template <class State>
Value encodeTestSet(const EncodingTestSet<State>& locks) {
	ObjectWriter writer(IncludeVersion());
	writer.serialize(RangeLockStateSet::file_identifier, locks);
	return writer.toString();
}

template <class State>
Value encodeTestState(const State& state, const std::string& key = "legacy map key") {
	EncodingTestSet<State> locks;
	locks.locks.emplace(key, state);
	return encodeTestSet(locks);
}

void expectRejected(const ValueRef& value) {
	bool rejected = false;
	try {
		decodeRangeLockStateSetSafe(value);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_range_lock_not_ready);
		rejected = true;
	}
	ASSERT(rejected);
}

class EncodingTestBuffer {
public:
	explicit EncodingTestBuffer(const ValueRef& value) : data_(value.toString()) {}

	StringRef value() const { return StringRef(data_); }

	template <class T>
	T read(size_t offset) const {
		ASSERT(offset <= data_.size() && sizeof(T) <= data_.size() - offset);
		T result;
		memcpy(&result, data_.data() + offset, sizeof(T));
		return result;
	}

	template <class T>
	void write(size_t offset, T value) {
		ASSERT(offset <= data_.size() && sizeof(T) <= data_.size() - offset);
		memcpy(data_.data() + offset, &value, sizeof(T));
	}

	size_t indirect(size_t offset) const { return offset + read<uint32_t>(offset); }
	size_t vtable(size_t object) const { return static_cast<int64_t>(object) - read<int32_t>(object); }
	size_t field(size_t object, size_t index) const {
		const size_t table = vtable(object);
		ASSERT((index + 2) * sizeof(uint16_t) < read<uint16_t>(table));
		const uint16_t offset = read<uint16_t>(table + (index + 2) * sizeof(uint16_t));
		ASSERT(offset >= sizeof(int32_t));
		return object + offset;
	}

	template <class T>
	void expectCorrupt(size_t offset, T replacement) const {
		EncodingTestBuffer corrupted(*this);
		corrupted.write<T>(offset, replacement);
		expectRejected(corrupted.value());
	}

private:
	std::string data_;
};

Value encodedLock(const RangeLockState& lock) {
	RangeLockStateSet locks;
	locks.insertIfNotExist(lock);
	return rangeLockStateSetValue(locks);
}

} // namespace

TEST_CASE("/RangeLock/Encoding/Compatibility") {
	ASSERT(decodeRangeLockStateSetSafe(ValueRef()).empty());
	ASSERT(decodeRangeLockStateSet(ValueRef()).empty());
	ASSERT(decodeRangeLockStateSetSafe(rangeLockStateSetValue(RangeLockStateSet())).empty());

	const std::vector<RangeLockState> locks = {
		RangeLockState(RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("a"_sr, "z"_sr)),
		RangeLockState(RangeLockType::ExclusiveReadLock,
		               std::string("owner\0legacy", 12),
		               normalKeys,
		               std::string("token\0id", 8)),
		RangeLockState(RangeLockType::ExclusiveReadLock, "single", singleKeyRange("key"_sr), "fenced"),
		RangeLockState(RangeLockType::ExclusiveReadLock, "A", KeyRangeRef("XExclusiveReadLock{ begin=a"_sr, "z"_sr)),
		RangeLockState(RangeLockType::ExclusiveReadLock, "AExclusiveReadLock{ begin=X", KeyRangeRef("a"_sr, "z"_sr)),
	};
	for (const auto& lock : locks) {
		const Value encoded = encodedLock(lock);
		const RangeLockStateSet decoded = decodeRangeLockStateSetSafe(encoded);
		ASSERT(decoded.containsExactLock(lock));
		ASSERT(decoded.getLocks().begin()->first == lock.getLockUniqueString());
		ASSERT(decodeRangeLockStateSet(encoded) == decoded);
		ASSERT(rangeLockStateSetValue(decoded) == encoded);

		Value prefixed = makeAlignedString(alignof(uint32_t), encoded.size() + 1);
		mutateString(prefixed)[0] = 0;
		memcpy(mutateString(prefixed) + 1, encoded.begin(), encoded.size());
		const ValueRef unaligned = prefixed.substr(1);
		ASSERT(reinterpret_cast<uintptr_t>(unaligned.begin()) % alignof(uint32_t) != 0);
		ASSERT(decodeRangeLockStateSetSafe(unaligned) == decoded);

		// The old format has no total-length field. Preserve its existing
		// acceptance of unreferenced trailing bytes instead of requiring a
		// canonical re-encoding, which would reject compatible older layouts.
		std::string withTrailingData = encoded.toString();
		withTrailingData.append("\0\xffunused", 8);
		ASSERT(decodeRangeLockStateSetSafe(StringRef(withTrailingData)) == decoded);
	}

	const std::string originalMapKey("historical\0map-key", 18);
	const Value legacy = encodeTestState(EncodingTestState<false>(), originalMapKey);
	const RangeLockStateSet decodedLegacy = decodeRangeLockStateSetSafe(legacy);
	ASSERT_EQ(decodedLegacy.getLocks().size(), 1);
	ASSERT(decodedLegacy.getLocks().begin()->first == originalMapKey);
	ASSERT(decodedLegacy.getLocks().begin()->second.getLockId().empty());
	ASSERT(decodedLegacy.getLocks().begin()->second.hasSameAcquisition(locks.front()));

	EncodingTestState<true, true> extended;
	extended.lockId = std::string("new\0token", 9);
	const auto decodedExtended = decodeRangeLockStateSetSafe(encodeTestState(extended, originalMapKey));
	ASSERT(decodedExtended.getLocks().begin()->first == originalMapKey);
	ASSERT(decodedExtended.getLocks().begin()->second.getLockId() == extended.lockId);
	return Void();
}

TEST_CASE("/RangeLock/Encoding/Truncation") {
	const std::vector<Value> values = {
		rangeLockStateSetValue(RangeLockStateSet()),
		encodedLock(RangeLockState(RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("a"_sr, "z"_sr))),
		encodedLock(RangeLockState(RangeLockType::ExclusiveReadLock, "single", singleKeyRange("key"_sr), "token")),
		encodeTestState(EncodingTestState<false>()),
	};
	for (const auto& value : values) {
		// The zero-byte KRM value deliberately means an empty state set.
		for (int length = 1; length < value.size(); ++length) {
			expectRejected(value.substr(0, length));
		}
		decodeRangeLockStateSetSafe(value);
	}
	return Void();
}

TEST_CASE("/RangeLock/Encoding/InvalidLayout") {
	const Value encoded = encodedLock(
	    RangeLockState(RangeLockType::ExclusiveReadLock, "owner", KeyRangeRef("begin"_sr, "end"_sr), "token"));
	const EncodingTestBuffer buffer(encoded);
	const size_t rootOffset = sizeof(uint64_t);
	const size_t root = buffer.indirect(rootOffset);
	const size_t stateSet = buffer.indirect(buffer.field(root, 0));
	const size_t vector = buffer.indirect(buffer.field(stateSet, 0));
	const size_t entryOffset = vector + sizeof(uint32_t);
	const size_t entry = buffer.indirect(entryOffset);
	const size_t state = buffer.indirect(buffer.field(entry, 1));
	const size_t range = buffer.indirect(buffer.field(state, 2));
	const uint32_t hugeOffset = std::numeric_limits<uint32_t>::max();

	buffer.expectCorrupt<uint64_t>(0, 0);
	buffer.expectCorrupt<uint64_t>(0, minInvalidProtocolVersion.versionWithFlags());
	buffer.expectCorrupt<FileIdentifier>(rootOffset + sizeof(uint32_t), RangeLockOwner::file_identifier);
	buffer.expectCorrupt<uint32_t>(vector, hugeOffset);

	const std::vector<size_t> offsets = { rootOffset,
		                                  buffer.field(root, 0),
		                                  buffer.field(stateSet, 0),
		                                  entryOffset,
		                                  buffer.field(entry, 0),
		                                  buffer.field(entry, 1),
		                                  buffer.field(state, 0),
		                                  buffer.field(state, 2),
		                                  buffer.field(state, 3),
		                                  buffer.field(range, 0),
		                                  buffer.field(range, 1) };
	for (const auto offset : offsets) {
		buffer.expectCorrupt<uint32_t>(offset, 0);
		buffer.expectCorrupt<uint32_t>(offset, 1);
		buffer.expectCorrupt<uint32_t>(offset, hugeOffset);
	}
	for (const auto object : { root, stateSet, entry, state, range }) {
		const size_t vtable = buffer.vtable(object);
		buffer.expectCorrupt<int32_t>(object, std::numeric_limits<int32_t>::max());
		buffer.expectCorrupt<int32_t>(object, std::numeric_limits<int32_t>::min());
		buffer.expectCorrupt<int32_t>(object, 0);
		buffer.expectCorrupt<uint16_t>(vtable, 3);
		buffer.expectCorrupt<uint16_t>(vtable, std::numeric_limits<uint16_t>::max() - 1);
		buffer.expectCorrupt<uint16_t>(vtable + sizeof(uint16_t), 3);
		buffer.expectCorrupt<uint16_t>(vtable + sizeof(uint16_t), std::numeric_limits<uint16_t>::max());
		buffer.expectCorrupt<uint16_t>(vtable + 2 * sizeof(uint16_t), 1);
		buffer.expectCorrupt<uint16_t>(vtable + 2 * sizeof(uint16_t), std::numeric_limits<uint16_t>::max());
	}
	for (const auto field : { buffer.field(entry, 0),
	                          buffer.field(state, 0),
	                          buffer.field(state, 3),
	                          buffer.field(range, 0),
	                          buffer.field(range, 1) }) {
		buffer.expectCorrupt<uint32_t>(buffer.indirect(field), hugeOffset);
	}
	buffer.expectCorrupt<uint8_t>(buffer.field(state, 1), 0);
	buffer.expectCorrupt<uint8_t>(buffer.field(state, 1), 255);

	// The ordinary client decoder must share the same wrong-file-ID guard.
	EncodingTestBuffer wrongType(buffer);
	wrongType.write<FileIdentifier>(rootOffset + sizeof(uint32_t), RangeLockOwner::file_identifier);
	bool rejected = false;
	try {
		decodeRangeLockStateSet(wrongType.value());
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_range_lock_not_ready);
		rejected = true;
	}
	ASSERT(rejected);
	return Void();
}

TEST_CASE("/RangeLock/Encoding/InvalidState") {
	EncodingTestState<> state;
	state.owner.clear();
	expectRejected(encodeTestState(state));
	state.owner = "owner";
	state.type = 0;
	expectRejected(encodeTestState(state));
	state.type = 255;
	expectRejected(encodeTestState(state));
	state.type = static_cast<uint8_t>(RangeLockType::ExclusiveReadLock);

	const std::vector<EncodingTestRange> invalidRanges = {
		{ "", "" }, { "a", "a" }, { "z", "a" }, { "not-compressed", "" }, { "a", "\xff\xff" },
	};
	for (const auto& range : invalidRanges) {
		state.range = range;
		expectRejected(encodeTestState(state));
	}

	// A bounded vector count alone does not bound allocations if all entries
	// point to the same large state. The type-specific decoder also limits the
	// total bytes that its subsequent ObjectReader can materialize.
	EncodingTestSet<> repeated;
	EncodingTestState<> large;
	large.owner.assign(4096, 'o');
	large.lockId.assign(4096, 't');
	repeated.locks.emplace("a" + std::string(2048, 'k'), large);
	repeated.locks.emplace("z", EncodingTestState<>());
	EncodingTestBuffer buffer(encodeTestSet(repeated));
	const size_t root = buffer.indirect(sizeof(uint64_t));
	const size_t stateSet = buffer.indirect(buffer.field(root, 0));
	const size_t vector = buffer.indirect(buffer.field(stateSet, 0));
	ASSERT_EQ(buffer.read<uint32_t>(vector), 2);
	const size_t firstEntryOffset = vector + sizeof(uint32_t);
	const size_t secondEntryOffset = firstEntryOffset + sizeof(uint32_t);
	const size_t firstEntry = buffer.indirect(firstEntryOffset);
	ASSERT(firstEntry > secondEntryOffset);
	buffer.write<uint32_t>(secondEntryOffset, static_cast<uint32_t>(firstEntry - secondEntryOffset));
	expectRejected(buffer.value());
	return Void();
}
