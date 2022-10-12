/*
 * IdempotencyId.h
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

#ifndef FDBCLIENT_IDEMPOTENCYID_H
#define FDBCLIENT_IDEMPOTENCYID_H

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/PImpl.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"

struct CommitResult {
	Version commitVersion;
	uint16_t batchIndex;
};

// See design/idempotency_ids.md for more information. Designed so that the common case of a random 16 byte id does not
// usually require indirection. Either invalid or an id with length >= 16 and < 256.
struct IdempotencyIdRef {
	static constexpr auto file_identifier = 3858470;

	// Create an invalid IdempotencyIdRef
	IdempotencyIdRef() : first(0) {}

	// Borrows memory from the StringRef
	explicit IdempotencyIdRef(StringRef id) {
		if (id.empty()) {
			first = 0;
			return;
		}
		ASSERT(id.size() >= 16);
		ASSERT(id.size() < 256);
		if (id.size() == 16 &&
		    /* If it's 16 bytes but first < 256 we still need to use an indirection to avoid ambiguity. */
		    reinterpret_cast<const uint64_t*>(id.begin())[0] >= 256) {
			first = reinterpret_cast<const uint64_t*>(id.begin())[0];
			second.id = reinterpret_cast<const uint64_t*>(id.begin())[1];
		} else {
			first = id.size();
			second.ptr = id.begin();
		}
	}

	IdempotencyIdRef(Arena& arena, IdempotencyIdRef t)
	  : IdempotencyIdRef(t.valid() && t.indirect() ? StringRef(arena, t.asStringRefUnsafe()) : t.asStringRefUnsafe()) {}

	int expectedSize() const {
		if (valid() && indirect()) {
			return first;
		}
		return 0;
	}

	bool operator==(const IdempotencyIdRef& other) const { return asStringRefUnsafe() == other.asStringRefUnsafe(); }

	IdempotencyIdRef(IdempotencyIdRef&& other) = default;
	IdempotencyIdRef& operator=(IdempotencyIdRef&& other) = default;
	IdempotencyIdRef(const IdempotencyIdRef& other) = default;
	IdempotencyIdRef& operator=(const IdempotencyIdRef& other) = default;

	template <class Archive>
	void serialize(Archive& ar) {
		// Only support network messages/object serializer for now
		ASSERT(false);
	}

	bool valid() const { return first != 0; }

	// Result may reference this, so *this must outlive result.
	StringRef asStringRefUnsafe() const {
		if (!valid()) {
			return StringRef();
		}
		if (indirect()) {
			return StringRef(second.ptr, first);
		} else {
			return StringRef(reinterpret_cast<const uint8_t*>(this), sizeof(*this));
		}
	}

private:
	bool indirect() const { return first < 256; }
	// first == 0 means this id is invalid. This representation is not ambiguous
	// because if first < 256, then first is the length of the id, but a valid
	// id as at least 16 bytes long.
	uint64_t first;
	union {
		uint64_t id;
		const uint8_t* ptr;
	} second; // If first < 256, then ptr is valid. Otherwise id is valid.
};

using IdempotencyId = Standalone<IdempotencyIdRef>;

namespace std {
template <>
struct hash<IdempotencyIdRef> {
	std::size_t operator()(const IdempotencyIdRef& id) const { return std::hash<StringRef>{}(id.asStringRefUnsafe()); }
};
template <>
struct hash<IdempotencyId> {
	std::size_t operator()(const IdempotencyId& id) const { return std::hash<StringRef>{}(id.asStringRefUnsafe()); }
};
} // namespace std

template <>
struct dynamic_size_traits<IdempotencyIdRef> : std::true_type {
	template <class Context>
	static size_t size(const IdempotencyIdRef& t, Context&) {
		return t.asStringRefUnsafe().size();
	}
	template <class Context>
	static void save(uint8_t* out, const IdempotencyIdRef& t, Context&) {
		StringRef s = t.asStringRefUnsafe();
		std::copy(s.begin(), s.end(), out);
	}

	template <class Context>
	static void load(const uint8_t* ptr, size_t sz, IdempotencyIdRef& id, Context& context) {
		id = IdempotencyIdRef(StringRef(context.tryReadZeroCopy(ptr, sz), sz));
	}
};

// The plan is to use this as a key in a potentially large hashtable, so it should be compact.
static_assert(sizeof(IdempotencyIdRef) == 16);

// Use in the commit proxy to construct a kv pair according to the format described in design/idempotency_ids.md
struct IdempotencyIdKVBuilder : NonCopyable {
	IdempotencyIdKVBuilder();
	void setCommitVersion(Version commitVersion);
	// All calls to add must share the same high order byte of batchIndex (until the next call to buildAndClear)
	void add(const IdempotencyIdRef& id, uint16_t batchIndex);
	// Must call setCommitVersion before calling buildAndClear. After calling buildAndClear, this object is ready to
	// start a new kv pair for the high order byte of batchIndex.
	Optional<KeyValue> buildAndClear();

	~IdempotencyIdKVBuilder();

private:
	PImpl<struct IdempotencyIdKVBuilderImpl> impl;
};

// Check if id is present in kv, and if so return the commit version and batchIndex
Optional<CommitResult> kvContainsIdempotencyId(const KeyValueRef& kv, const IdempotencyIdRef& id);

#endif