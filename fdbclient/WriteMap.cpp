/*
 * WriteMap.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/WriteMap.h"

void OperationStack::reset(RYWMutation initialEntry) {
	defaultConstructed = false;
	singletonOperation = initialEntry;
	optionalOperations = Optional<std::vector<RYWMutation>>();
}

void OperationStack::poppush(RYWMutation entry) {
	if (hasVector()) {
		optionalOperations.get().pop_back();
		optionalOperations.get().push_back(entry);
	} else
		singletonOperation = entry;
}

void OperationStack::push(RYWMutation entry) {
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

bool OperationStack::isDependent() const {
	if (!size())
		return false;
	return singletonOperation.type != MutationRef::SetValue && singletonOperation.type != MutationRef::ClearRange &&
	       singletonOperation.type != MutationRef::SetVersionstampedValue &&
	       singletonOperation.type != MutationRef::SetVersionstampedKey;
}

bool OperationStack::operator==(const OperationStack& r) const {
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

WriteMap& WriteMap::operator=(WriteMap&& r) noexcept {
	writeMapEmpty = r.writeMapEmpty;
	writes = std::move(r.writes);
	ver = r.ver;
	scratch_iterator = std::move(r.scratch_iterator);
	arena = r.arena;
	return *this;
}

void WriteMap::mutate(KeyRef key, MutationRef::Type operation, ValueRef param, bool addConflict) {
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
			PTreeImpl::insert(
			    writes,
			    ver,
			    WriteMapEntry(
			        key, std::move(op), true, following_conflict, is_conflict, following_unreadable, is_unreadable));
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
		if (!it.is_unreadable() &&
		    (operation == MutationRef::SetValue || operation == MutationRef::SetVersionstampedValue)) {
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

void WriteMap::clear(KeyRangeRef keys, bool addConflict) {
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
		PTreeImpl::insert(
		    writes,
		    ver,
		    WriteMapEntry(
		        keys.end, OperationStack(), end_cleared, end_conflict, end_conflict, end_unreadable, end_unreadable));
}

void WriteMap::addUnmodifiedAndUnreadableRange(KeyRangeRef keys) {
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
		PTreeImpl::insert(writes, ver, WriteMapEntry(keys.begin, OperationStack(), false, false, false, true, true));

	if (insert_end)
		PTreeImpl::insert(
		    writes,
		    ver,
		    WriteMapEntry(
		        keys.end, OperationStack(), end_cleared, end_conflict, end_conflict, end_unreadable, end_unreadable));
}

void WriteMap::addConflictRange(KeyRangeRef keys) {
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
			insertions.push_back(
			    WriteMapEntry(keys.end, OperationStack(), isCleared, false, false, followingUnreadable, isUnreadable));
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

WriteMap::iterator::SEGMENT_TYPE WriteMap::iterator::type() const {
	if (offset)
		return entry().following_keys_cleared ? CLEARED_RANGE : UNMODIFIED_RANGE;
	else
		return entry().stack.isDependent() ? DEPENDENT_WRITE : INDEPENDENT_WRITE;
}

WriteMap::iterator& WriteMap::iterator::operator++() {
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
WriteMap::iterator& WriteMap::iterator::operator--() {
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

void WriteMap::iterator::skip(
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

void WriteMap::iterator::reset(Tree const& tree, Version ver) {
	this->tree = tree;
	this->at = ver;
	this->finger.clear();
	beginLen = endLen = 0;
	offset = false;
}

RYWMutation WriteMap::coalesce(RYWMutation existingEntry, RYWMutation newEntry, Arena& arena) {
	ASSERT(newEntry.value.present());

	if (newEntry.type == MutationRef::SetValue || newEntry.type == MutationRef::SetVersionstampedValue) {
		// independent mutations
		return newEntry;
	} else if (newEntry.type == MutationRef::AddValue) {
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
			return RYWMutation(doAppendIfFits(existingEntry.value, newEntry.value.get(), arena), MutationRef::SetValue);
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

void WriteMap::coalesceOver(OperationStack& stack, RYWMutation newEntry, Arena& arena) {
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

RYWMutation WriteMap::coalesceUnder(OperationStack const& stack, Optional<ValueRef> const& value, Arena& arena) {
	if (!stack.isDependent() && stack.size() == 1)
		return stack.at(0);

	RYWMutation currentEntry = RYWMutation(value, MutationRef::SetValue);
	for (int i = 0; i < stack.size(); ++i) {
		currentEntry = coalesce(currentEntry, stack.at(i), arena);
	}

	return currentEntry;
}

void WriteMap::dump() {
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

void WriteMap::clearNoConflict(KeyRangeRef keys) {
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

	CODE_PROBE(it.is_conflict_range() != lastConflicted, "not last conflicted");

	it.tree.clear();

	PTreeImpl::remove(writes,
	                  ver,
	                  ExtStringRef(keys.begin, !insert_begin ? 1 : 0),
	                  ExtStringRef(keys.end, end_coalesce_clear ? 1 : 0));

	for (int i = 0; i < conflict_ranges.size(); i++) {
		PTreeImpl::insert(
		    writes,
		    ver,
		    WriteMapEntry(
		        conflict_ranges[i].toArenaOrRef(*arena), OperationStack(), true, conflicted, conflicted, false, false));
		conflicted = !conflicted;
	}

	ASSERT(conflicted != lastConflicted);

	if (insert_end)
		PTreeImpl::insert(
		    writes,
		    ver,
		    WriteMapEntry(
		        keys.end, OperationStack(), end_cleared, end_conflict, end_conflict, end_unreadable, end_unreadable));
}
