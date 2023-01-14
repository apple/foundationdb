/*
 * WriteMap.h
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

#ifndef FDBCLIENT_WRITEMAP_H
#define FDBCLIENT_WRITEMAP_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/VersionedMap.h"
#include "fdbclient/SnapshotCache.h"
#include "fdbclient/Atomic.h"

struct RYWMutation {
	Optional<ValueRef> value;
	enum MutationRef::Type type;
	int compare(WriteMapEntry const& r) const { return key.compare(r.key); }
	RYWMutation(Optional<ValueRef> const& entry, MutationRef::Type type) : value(entry), type(type) {}
	RYWMutation() : value(), type(MutationRef::NoOp) {}

	bool operator==(const RYWMutation& r) const { return value == r.value && type == r.type; }
	bool operator!=(const RYWMutation& r) const { return !(*this == r); }
};

class OperationStack {
private:
	RYWMutation singletonOperation;
	Optional<std::vector<RYWMutation>> optionalOperations;
	bool hasVector() const { return optionalOperations.present(); }
	bool defaultConstructed;

public:
	OperationStack() { defaultConstructed = true; } // Don't use this!
	explicit OperationStack(RYWMutation initialEntry) {
		defaultConstructed = false;
		singletonOperation = initialEntry;
	}
	void reset(RYWMutation initialEntry);
	void poppush(RYWMutation entry);
	void push(RYWMutation entry);
	bool isDependent() const;

	const RYWMutation& top() const { return hasVector() ? optionalOperations.get().back() : singletonOperation; }
	RYWMutation& operator[](int n) { return (n == 0) ? singletonOperation : optionalOperations.get()[n - 1]; }

	const RYWMutation& at(int n) const { return (n == 0) ? singletonOperation : optionalOperations.get()[n - 1]; }

	int size() const { return defaultConstructed ? 0 : hasVector() ? optionalOperations.get().size() + 1 : 1; }

	bool operator==(const OperationStack& r) const;
};

struct WriteMapEntry {
	KeyRef key;
	OperationStack stack;
	bool following_keys_cleared;
	bool following_keys_conflict;
	bool is_conflict;
	bool following_keys_unreadable;
	bool is_unreadable;

	WriteMapEntry(KeyRef const& key,
	              OperationStack&& stack,
	              bool following_keys_cleared,
	              bool following_keys_conflict,
	              bool is_conflict,
	              bool following_keys_unreadable,
	              bool is_unreadable)
	  : key(key), stack(std::move(stack)), following_keys_cleared(following_keys_cleared),
	    following_keys_conflict(following_keys_conflict), is_conflict(is_conflict),
	    following_keys_unreadable(following_keys_unreadable), is_unreadable(is_unreadable) {}

	int compare(StringRef const& r) const { return key.compare(r); }

	int compare(ExtStringRef const& r) const { return -r.compare(key); }

	std::string toString() const { return printable(key); }
};

inline int compare(StringRef const& l, WriteMapEntry const& r) {
	return l.compare(r.key);
}

inline int compare(ExtStringRef const& l, WriteMapEntry const& r) {
	return l.compare(r.key);
}

inline bool operator<(const WriteMapEntry& lhs, const WriteMapEntry& rhs) {
	return lhs.key < rhs.key;
}
inline bool operator<(const WriteMapEntry& lhs, const StringRef& rhs) {
	return lhs.key < rhs;
}
inline bool operator<(const StringRef& lhs, const WriteMapEntry& rhs) {
	return lhs < rhs.key;
}
inline bool operator<(const WriteMapEntry& lhs, const ExtStringRef& rhs) {
	return rhs.compare(lhs.key) > 0;
}
inline bool operator<(const ExtStringRef& lhs, const WriteMapEntry& rhs) {
	return lhs.compare(rhs.key) < 0;
}

class WriteMap {
private:
	typedef PTreeImpl::PTree<WriteMapEntry> PTreeT;
	typedef PTreeImpl::PTreeFinger<WriteMapEntry> PTreeFingerT;
	typedef Reference<PTreeT> Tree;

public:
	explicit WriteMap(Arena* arena) : arena(arena), writeMapEmpty(true), ver(-1), scratch_iterator(this) {
		PTreeImpl::insert(
		    writes, ver, WriteMapEntry(allKeys.begin, OperationStack(), false, false, false, false, false));
		PTreeImpl::insert(writes, ver, WriteMapEntry(allKeys.end, OperationStack(), false, false, false, false, false));
		PTreeImpl::insert(
		    writes, ver, WriteMapEntry(afterAllKeys, OperationStack(), false, false, false, false, false));
	}

	WriteMap(WriteMap&& r) noexcept
	  : arena(r.arena), writeMapEmpty(r.writeMapEmpty), writes(std::move(r.writes)), ver(r.ver),
	    scratch_iterator(std::move(r.scratch_iterator)) {}

	WriteMap& operator=(WriteMap&& r) noexcept;

	// a write with addConflict false on top of an existing write with a conflict range will not remove the conflict
	void mutate(KeyRef key, MutationRef::Type operation, ValueRef param, bool addConflict);

	void clear(KeyRangeRef keys, bool addConflict);

	void addUnmodifiedAndUnreadableRange(KeyRangeRef keys);

	void addConflictRange(KeyRangeRef keys);

	struct iterator {
		// Iterates over three types of segments: unmodified ranges, cleared ranges, and modified keys.
		// Modified keys may be dependent (need to be collapsed with a snapshot value) or independent (value is known
		// regardless of the snapshot value) Every key will belong to exactly one segment.  The first segment begins at
		// "" and the last segment ends at \xff\xff.

		explicit iterator(WriteMap* map) : tree(map->writes), at(map->ver), offset(false) { ++map->ver; }
		// Creates an iterator which is conceptually before the beginning of map (you may essentially only call skip()
		// or ++ on it) This iterator also represents a snapshot (will be unaffected by future writes)

		enum SEGMENT_TYPE { UNMODIFIED_RANGE, CLEARED_RANGE, INDEPENDENT_WRITE, DEPENDENT_WRITE };

		SEGMENT_TYPE type() const;
		bool is_cleared_range() const { return offset && entry().following_keys_cleared; }
		bool is_unmodified_range() const { return offset && !entry().following_keys_cleared; }
		bool is_operation() const { return !offset; }
		bool is_conflict_range() const { return offset ? entry().following_keys_conflict : entry().is_conflict; }
		bool is_unreadable() const { return offset ? entry().following_keys_unreadable : entry().is_unreadable; }

		bool is_independent() const {
			ASSERT(is_operation());
			return entry().following_keys_cleared || !entry().stack.isDependent();
		}

		ExtStringRef beginKey() const { return ExtStringRef(entry().key, offset && entry().stack.size()); }
		ExtStringRef endKey() const { return offset ? nextEntry().key : ExtStringRef(entry().key, 1); }

		OperationStack const& op() const {
			ASSERT(is_operation());
			return entry().stack;
		}

		iterator& operator++();
		iterator& operator--();
		bool operator==(const iterator& r) const {
			return offset == r.offset && beginLen == r.beginLen && finger[beginLen - 1] == r.finger[beginLen - 1];
		}
		void skip(KeyRef key);

	private:
		friend class WriteMap;
		void reset(Tree const& tree, Version ver);

		WriteMapEntry const& entry() const { return finger[beginLen - 1]->data; }
		WriteMapEntry const& nextEntry() const { return finger[endLen - 1]->data; }

		bool keyAtBegin() { return !offset || !entry().stack.size(); }

		Tree tree;
		Version at;
		int beginLen, endLen;
		PTreeFingerT finger;
		bool offset; // false-> the operation stack at entry(); true-> the following cleared or unmodified range
	};

	bool empty() const { return writeMapEmpty; }

	static RYWMutation coalesce(RYWMutation existingEntry, RYWMutation newEntry, Arena& arena);
	static void coalesceOver(OperationStack& stack, RYWMutation newEntry, Arena& arena);
	static RYWMutation coalesceUnder(OperationStack const& stack, Optional<ValueRef> const& value, Arena& arena);

private:
	friend class ReadYourWritesTransaction;
	Arena* arena;
	bool writeMapEmpty;
	Tree writes;
	// an internal version number for the tree - no connection to database versions!  Currently this is
	// incremented after reads, so that consecutive writes have the same version and those separated by
	// reads have different versions.
	Version ver;
	iterator scratch_iterator; // Avoid unnecessary memory allocation in write operations

	void dump();

	// SOMEDAY: clearNoConflict replaces cleared sets with two map entries for everyone one item cleared
	void clearNoConflict(KeyRangeRef keys);
};

/*

    for write in writes:   # write.type in [ 'none', 'clear', 'independent', 'dependent' ]
        for read in reads[ write.begin : write.end ]:   # read.type in [ 'unknown', 'empty', 'value' ]
            if write.type == "none":
                yield read
            elif write.type == "clear":
                yield empty()
            elif write.type == "independent":
                yield value( write )
            else:  # Dependent write
                if read.type == "unknown":
                    yield read
                else:
                    yield value( collapse( read, write ) )

*/

#endif
