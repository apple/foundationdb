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
	void reset(RYWMutation initialEntry) {
		defaultConstructed = false;
		singletonOperation = initialEntry;
		optionalOperations = Optional<std::vector<RYWMutation>>();
	}
	void poppush(RYWMutation entry) {
		if (hasVector()) {
			optionalOperations.get().pop_back();
			optionalOperations.get().push_back(entry);
		} else
			singletonOperation = entry;
	}
	void push(RYWMutation entry) {
		if (defaultConstructed) {
			singletonOperation = entry;
			defaultConstructed = false;
		} else if (hasVector())
			optionalOperations.get().push_back(entry);
		else {
			optionalOperations = std::vector<RYWMutation>();
			optionalOperations.get().push_back(entry);
		}
	}
	bool isDependent() const {
		if (!size())
			return false;
		return singletonOperation.type != MutationRef::SetValue && singletonOperation.type != MutationRef::ClearRange &&
		       singletonOperation.type != MutationRef::SetVersionstampedValue &&
		       singletonOperation.type != MutationRef::SetVersionstampedKey;
	}
	const RYWMutation& top() const { return hasVector() ? optionalOperations.get().back() : singletonOperation; }
	RYWMutation& operator[](int n) { return (n == 0) ? singletonOperation : optionalOperations.get()[n - 1]; }

	const RYWMutation& at(int n) const { return (n == 0) ? singletonOperation : optionalOperations.get()[n - 1]; }

	int size() const { return defaultConstructed ? 0 : hasVector() ? optionalOperations.get().size() + 1 : 1; }

	bool operator==(const OperationStack& r) const {
		if (size() != r.size())
			return false;

		if (size() == 0)
			return true;

		if (singletonOperation != r.singletonOperation)
			return false;

		if (size() == 1)
			return true;

		for (int i = 0; i < optionalOperations.get().size(); i++) {
			if (optionalOperations.get()[i] != r.optionalOperations.get()[i])
				return false;
		}

		return true;
	}
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
	WriteMap& operator=(WriteMap&& r) noexcept {
		writeMapEmpty = r.writeMapEmpty;
		writes = std::move(r.writes);
		ver = r.ver;
		scratch_iterator = std::move(r.scratch_iterator);
		arena = r.arena;
		return *this;
	}

	// a write with addConflict false on top of an existing write with a conflict range will not remove the conflict
	void mutate(KeyRef key, MutationRef::Type operation, ValueRef param, bool addConflict) {
		writeMapEmpty = false;
		auto& it = scratch_iterator;
		it.reset(writes, ver);
		it.skip(key);

		bool is_cleared = it.entry().following_keys_cleared;
		bool following_conflict = it.entry().following_keys_conflict;
		bool is_conflict = addConflict || it.is_conflict_range();
		bool following_unreadable = it.entry().following_keys_unreadable;
		bool is_unreadable = it.is_unreadable() || operation == MutationRef::SetVersionstampedValue ||
		                     operation == MutationRef::SetVersionstampedKey;
		bool is_dependent = operation != MutationRef::SetValue && operation != MutationRef::SetVersionstampedValue &&
		                    operation != MutationRef::SetVersionstampedKey;

		if (it.entry().key != key) {
			if (it.is_cleared_range() && is_dependent) {
				it.tree.clear();
				OperationStack op(RYWMutation(Optional<StringRef>(), MutationRef::SetValue));
				coalesceOver(op, RYWMutation(param, operation), *arena);
				PTreeImpl::insert(writes,
				                  ver,
				                  WriteMapEntry(key,
				                                std::move(op),
				                                true,
				                                following_conflict,
				                                is_conflict,
				                                following_unreadable,
				                                is_unreadable));
			} else {
				it.tree.clear();
				PTreeImpl::insert(writes,
				                  ver,
				                  WriteMapEntry(key,
				                                OperationStack(RYWMutation(param, operation)),
				                                is_cleared,
				                                following_conflict,
				                                is_conflict,
				                                following_unreadable,
				                                is_unreadable));
			}
		} else {
			if (!it.is_unreadable() && operation == MutationRef::SetValue) {
				it.tree.clear();
				PTreeImpl::remove(writes, ver, key);
				PTreeImpl::insert(writes,
				                  ver,
				                  WriteMapEntry(key,
				                                OperationStack(RYWMutation(param, operation)),
				                                is_cleared,
				                                following_conflict,
				                                is_conflict,
				                                following_unreadable,
				                                is_unreadable));
			} else {
				WriteMapEntry e(it.entry());
				e.is_conflict = is_conflict;
				e.is_unreadable = is_unreadable;
				if (e.stack.size() == 0 && it.is_cleared_range() && is_dependent) {
					e.stack.push(RYWMutation(Optional<StringRef>(), MutationRef::SetValue));
					coalesceOver(e.stack, RYWMutation(param, operation), *arena);
				} else if (!is_unreadable && e.stack.size() > 0)
					coalesceOver(e.stack, RYWMutation(param, operation), *arena);
				else
					e.stack.push(RYWMutation(param, operation));

				it.tree.clear();
				PTreeImpl::remove(
				    writes,
				    ver,
				    e.key); // FIXME: Make PTreeImpl::insert do this automatically (see also VersionedMap.h FIXME)
				PTreeImpl::insert(writes, ver, std::move(e));
			}
		}
	}

	void clear(KeyRangeRef keys, bool addConflict) {
		writeMapEmpty = false;
		if (!addConflict) {
			clearNoConflict(keys);
			return;
		}

		auto& it = scratch_iterator;
		it.reset(writes, ver);
		it.skip(keys.begin);

		bool insert_begin = !it.is_cleared_range() || !it.is_conflict_range() || it.is_unreadable();

		if (it.endKey() == keys.end) {
			++it;
		} else if (it.endKey() < keys.end) {
			it.skip(keys.end);
		}

		bool insert_end = (it.is_unmodified_range() || !it.is_conflict_range() || it.is_unreadable()) &&
		                  (!it.keyAtBegin() || it.beginKey() != keys.end);
		bool end_coalesce_clear =
		    it.is_cleared_range() && it.beginKey() == keys.end && it.is_conflict_range() && !it.is_unreadable();
		bool end_conflict = it.is_conflict_range();
		bool end_cleared = it.is_cleared_range();
		bool end_unreadable = it.is_unreadable();

		it.tree.clear();

		PTreeImpl::remove(writes,
		                  ver,
		                  ExtStringRef(keys.begin, !insert_begin ? 1 : 0),
		                  ExtStringRef(keys.end, end_coalesce_clear ? 1 : 0));

		if (insert_begin)
			PTreeImpl::insert(writes, ver, WriteMapEntry(keys.begin, OperationStack(), true, true, true, false, false));

		if (insert_end)
			PTreeImpl::insert(writes,
			                  ver,
			                  WriteMapEntry(keys.end,
			                                OperationStack(),
			                                end_cleared,
			                                end_conflict,
			                                end_conflict,
			                                end_unreadable,
			                                end_unreadable));
	}

	void addUnmodifiedAndUnreadableRange(KeyRangeRef keys) {
		auto& it = scratch_iterator;
		it.reset(writes, ver);
		it.skip(keys.begin);

		bool insert_begin = !it.is_unmodified_range() || it.is_conflict_range() || !it.is_unreadable();

		if (it.endKey() == keys.end) {
			++it;
		} else if (it.endKey() < keys.end) {
			it.skip(keys.end);
		}

		bool insert_end = (it.is_cleared_range() || it.is_conflict_range() || !it.is_unreadable()) &&
		                  (!it.keyAtBegin() || it.beginKey() != keys.end);
		bool end_coalesce_unmodified =
		    it.is_unmodified_range() && it.beginKey() == keys.end && !it.is_conflict_range() && it.is_unreadable();
		bool end_conflict = it.is_conflict_range();
		bool end_cleared = it.is_cleared_range();
		bool end_unreadable = it.is_unreadable();

		it.tree.clear();

		PTreeImpl::remove(writes,
		                  ver,
		                  ExtStringRef(keys.begin, !insert_begin ? 1 : 0),
		                  ExtStringRef(keys.end, end_coalesce_unmodified ? 1 : 0));

		if (insert_begin)
			PTreeImpl::insert(
			    writes, ver, WriteMapEntry(keys.begin, OperationStack(), false, false, false, true, true));

		if (insert_end)
			PTreeImpl::insert(writes,
			                  ver,
			                  WriteMapEntry(keys.end,
			                                OperationStack(),
			                                end_cleared,
			                                end_conflict,
			                                end_conflict,
			                                end_unreadable,
			                                end_unreadable));
	}

	void addConflictRange(KeyRangeRef keys) {
		writeMapEmpty = false;
		auto& it = scratch_iterator;
		it.reset(writes, ver);
		it.skip(keys.begin);

		std::vector<ExtStringRef> removals;
		std::vector<WriteMapEntry> insertions;

		if (!it.entry().following_keys_conflict || !it.entry().is_conflict) {
			if (it.keyAtBegin() && it.beginKey() == keys.begin) {
				removals.push_back(keys.begin);
			}
			insertions.push_back(WriteMapEntry(keys.begin,
			                                   it.is_operation() ? OperationStack(it.op()) : OperationStack(),
			                                   it.entry().following_keys_cleared,
			                                   true,
			                                   true,
			                                   it.entry().following_keys_unreadable,
			                                   it.entry().is_unreadable));
		}

		while (it.endKey() < keys.end) {
			++it;
			if (it.keyAtBegin() && (!it.entry().following_keys_conflict || !it.entry().is_conflict)) {
				WriteMapEntry e(it.entry());
				e.following_keys_conflict = true;
				e.is_conflict = true;
				removals.push_back(e.key);
				insertions.push_back(std::move(e));
			}
		}

		ASSERT(it.beginKey() != keys.end);
		if (!it.entry().following_keys_conflict || !it.entry().is_conflict) {
			bool isCleared = it.entry().following_keys_cleared;
			bool isUnreadable = it.entry().is_unreadable;
			bool followingUnreadable = it.entry().following_keys_unreadable;
			++it;

			if (!it.keyAtBegin() || it.beginKey() != keys.end) {
				insertions.push_back(WriteMapEntry(
				    keys.end, OperationStack(), isCleared, false, false, followingUnreadable, isUnreadable));
			}
		}

		it.tree.clear();

		// SOMEDAY: optimize this code by having a PTree removal/insertion that takes and returns an iterator
		for (int i = 0; i < removals.size(); i++) {
			PTreeImpl::remove(
			    writes,
			    ver,
			    removals[i]); // FIXME: Make PTreeImpl::insert do this automatically (see also VersionedMap.h FIXME)
		}

		for (int i = 0; i < insertions.size(); i++) {
			PTreeImpl::insert(writes, ver, std::move(insertions[i]));
		}
	}

	struct iterator {
		// Iterates over three types of segments: unmodified ranges, cleared ranges, and modified keys.
		// Modified keys may be dependent (need to be collapsed with a snapshot value) or independent (value is known
		// regardless of the snapshot value) Every key will belong to exactly one segment.  The first segment begins at
		// "" and the last segment ends at \xff\xff.

		explicit iterator(WriteMap* map) : tree(map->writes), at(map->ver), offset(false) { ++map->ver; }
		// Creates an iterator which is conceptually before the beginning of map (you may essentially only call skip()
		// or ++ on it) This iterator also represents a snapshot (will be unaffected by future writes)

		enum SEGMENT_TYPE { UNMODIFIED_RANGE, CLEARED_RANGE, INDEPENDENT_WRITE, DEPENDENT_WRITE };

		SEGMENT_TYPE type() const {
			if (offset)
				return entry().following_keys_cleared ? CLEARED_RANGE : UNMODIFIED_RANGE;
			else
				return entry().stack.isDependent() ? DEPENDENT_WRITE : INDEPENDENT_WRITE;
		}
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

		iterator& operator++() {
			if (!offset && !equalsKeyAfter(entry().key, nextEntry().key)) {
				offset = true;
			} else {
				beginLen = endLen;
				finger.resize(beginLen);
				endLen = PTreeImpl::halfNext(at, finger);
				offset = !entry().stack.size();
			}
			return *this;
		}
		iterator& operator--() {
			if (offset && entry().stack.size()) {
				offset = false;
			} else {
				endLen = beginLen;
				finger.resize(endLen);
				beginLen = PTreeImpl::halfPrevious(at, finger);
				offset = !entry().stack.size() || !equalsKeyAfter(entry().key, nextEntry().key);
			}
			return *this;
		}
		bool operator==(const iterator& r) const {
			return offset == r.offset && beginLen == r.beginLen && finger[beginLen - 1] == r.finger[beginLen - 1];
		}

		void skip(
		    KeyRef key) { // Changes *this to the segment containing key (so that beginKey()<=key && key < endKey())
			finger.clear();

			if (key == allKeys.end)
				PTreeImpl::last(tree, at, finger);
			else
				PTreeImpl::upper_bound(tree, at, key, finger);
			endLen = finger.size();
			beginLen = PTreeImpl::halfPrevious(at, finger);

			offset = !entry().stack.size() || (entry().key != key);
		}

	private:
		friend class WriteMap;
		void reset(Tree const& tree, Version ver) {
			this->tree = tree;
			this->at = ver;
			this->finger.clear();
			beginLen = endLen = 0;
			offset = false;
		}

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

	static RYWMutation coalesce(RYWMutation existingEntry, RYWMutation newEntry, Arena& arena) {
		ASSERT(newEntry.value.present());

		if (newEntry.type == MutationRef::SetValue)
			return newEntry;
		else if (newEntry.type == MutationRef::AddValue) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doLittleEndianAdd(existingEntry.value, newEntry.value.get(), arena),
				                   MutationRef::SetValue);
			case MutationRef::AddValue:
				return RYWMutation(doLittleEndianAdd(existingEntry.value, newEntry.value.get(), arena),
				                   MutationRef::AddValue);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::CompareAndClear) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				if (doCompareAndClear(existingEntry.value, newEntry.value.get(), arena).present()) {
					return existingEntry;
				} else {
					return RYWMutation(Optional<ValueRef>(), MutationRef::SetValue);
				}
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::AppendIfFits) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doAppendIfFits(existingEntry.value, newEntry.value.get(), arena),
				                   MutationRef::SetValue);
			case MutationRef::AppendIfFits:
				return RYWMutation(doAppendIfFits(existingEntry.value, newEntry.value.get(), arena),
				                   MutationRef::AppendIfFits);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::And) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doAnd(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::And:
				return RYWMutation(doAnd(existingEntry.value, newEntry.value.get(), arena), MutationRef::And);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::Or) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doOr(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::Or:
				return RYWMutation(doOr(existingEntry.value, newEntry.value.get(), arena), MutationRef::Or);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::Xor) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doXor(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::Xor:
				return RYWMutation(doXor(existingEntry.value, newEntry.value.get(), arena), MutationRef::Xor);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::Max) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doMax(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::Max:
				return RYWMutation(doMax(existingEntry.value, newEntry.value.get(), arena), MutationRef::Max);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::Min) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doMin(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::Min:
				return RYWMutation(doMin(existingEntry.value, newEntry.value.get(), arena), MutationRef::Min);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::ByteMin) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doByteMin(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::ByteMin:
				return RYWMutation(doByteMin(existingEntry.value, newEntry.value.get(), arena), MutationRef::ByteMin);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::ByteMax) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doByteMax(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::ByteMax:
				return RYWMutation(doByteMax(existingEntry.value, newEntry.value.get(), arena), MutationRef::ByteMax);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::MinV2) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doMinV2(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::MinV2:
				return RYWMutation(doMinV2(existingEntry.value, newEntry.value.get(), arena), MutationRef::MinV2);
			default:
				throw operation_failed();
			}
		} else if (newEntry.type == MutationRef::AndV2) {
			switch (existingEntry.type) {
			case MutationRef::SetValue:
				return RYWMutation(doAndV2(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
			case MutationRef::AndV2:
				return RYWMutation(doAndV2(existingEntry.value, newEntry.value.get(), arena), MutationRef::AndV2);
			default:
				throw operation_failed();
			}
		} else
			throw operation_failed();
	}

	static void coalesceOver(OperationStack& stack, RYWMutation newEntry, Arena& arena) {
		RYWMutation existingEntry = stack.top();
		if (existingEntry.type == newEntry.type && newEntry.type != MutationRef::CompareAndClear) {
			if (isNonAssociativeOp(existingEntry.type) && existingEntry.value.present() &&
			    existingEntry.value.get().size() != newEntry.value.get().size()) {
				stack.push(newEntry);
			} else {
				stack.poppush(coalesce(existingEntry, newEntry, arena));
			}
		} else {
			if (isAtomicOp(newEntry.type) && isAtomicOp(existingEntry.type)) {
				stack.push(newEntry);
			} else {
				stack.poppush(coalesce(existingEntry, newEntry, arena));
			}
		}
	}

	static RYWMutation coalesceUnder(OperationStack const& stack, Optional<ValueRef> const& value, Arena& arena) {
		if (!stack.isDependent() && stack.size() == 1)
			return stack.at(0);

		RYWMutation currentEntry = RYWMutation(value, MutationRef::SetValue);
		for (int i = 0; i < stack.size(); ++i) {
			currentEntry = coalesce(currentEntry, stack.at(i), arena);
		}

		return currentEntry;
	}

private:
	friend class ReadYourWritesTransaction;
	Arena* arena;
	bool writeMapEmpty;
	Tree writes;
	Version ver; // an internal version number for the tree - no connection to database versions!  Currently this is
	             // incremented after reads, so that consecutive writes have the same version and those separated by
	             // reads have different versions.
	iterator scratch_iterator; // Avoid unnecessary memory allocation in write operations

	void dump() {
		iterator it(this);
		it.skip(allKeys.begin);
		while (it.beginKey() < allKeys.end) {
			TraceEvent("WriteMapDump")
			    .detail("Begin", it.beginKey().toStandaloneStringRef())
			    .detail("End", it.endKey())
			    .detail("Cleared", it.is_cleared_range())
			    .detail("Conflicted", it.is_conflict_range())
			    .detail("Operation", it.is_operation())
			    .detail("Unmodified", it.is_unmodified_range())
			    .detail("Independent", it.is_operation() && it.is_independent())
			    .detail("StackSize", it.is_operation() ? it.op().size() : 0);
			++it;
		}
	}

	// SOMEDAY: clearNoConflict replaces cleared sets with two map entries for everyone one item cleared
	void clearNoConflict(KeyRangeRef keys) {
		auto& it = scratch_iterator;
		it.reset(writes, ver);

		// Find all write conflict ranges within the cleared range
		it.skip(keys.begin);

		bool insert_begin = !it.is_cleared_range() || it.is_unreadable();

		bool lastConflicted = it.is_conflict_range();

		bool conflicted = lastConflicted;
		std::vector<ExtStringRef> conflict_ranges;

		if (insert_begin) {
			conflict_ranges.push_back(keys.begin);
		} else {
			conflicted = !conflicted;
		}

		while (it.endKey() < keys.end) {
			++it;
			if (lastConflicted != it.is_conflict_range()) {
				conflict_ranges.push_back(it.beginKey());
				lastConflicted = it.is_conflict_range();
			}
		}

		if (it.endKey() == keys.end)
			++it;

		ASSERT(it.beginKey() <= keys.end && keys.end < it.endKey());

		bool insert_end =
		    ((it.is_unmodified_range() || it.is_unreadable()) && (!it.keyAtBegin() || it.beginKey() != keys.end)) ||
		    (it.entry().is_conflict && !it.entry().following_keys_conflict && it.beginKey() == keys.end &&
		     !it.keyAtBegin());
		bool end_cleared = it.is_cleared_range();
		bool end_coalesce_clear = it.is_cleared_range() && it.beginKey() == keys.end &&
		                          it.is_conflict_range() == lastConflicted && !it.is_unreadable();
		bool end_conflict = it.is_conflict_range();
		bool end_unreadable = it.is_unreadable();

		TEST(it.is_conflict_range() != lastConflicted); // not last conflicted

		it.tree.clear();

		PTreeImpl::remove(writes,
		                  ver,
		                  ExtStringRef(keys.begin, !insert_begin ? 1 : 0),
		                  ExtStringRef(keys.end, end_coalesce_clear ? 1 : 0));

		for (int i = 0; i < conflict_ranges.size(); i++) {
			PTreeImpl::insert(writes,
			                  ver,
			                  WriteMapEntry(conflict_ranges[i].toArenaOrRef(*arena),
			                                OperationStack(),
			                                true,
			                                conflicted,
			                                conflicted,
			                                false,
			                                false));
			conflicted = !conflicted;
		}

		ASSERT(conflicted != lastConflicted);

		if (insert_end)
			PTreeImpl::insert(writes,
			                  ver,
			                  WriteMapEntry(keys.end,
			                                OperationStack(),
			                                end_cleared,
			                                end_conflict,
			                                end_conflict,
			                                end_unreadable,
			                                end_unreadable));
	}
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
