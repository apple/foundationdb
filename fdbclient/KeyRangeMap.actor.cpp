/*
 * KeyRangeMap.actor.cpp
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

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // has to be last include

void KeyRangeActorMap::getRangesAffectedByInsertion(const KeyRangeRef& keys, std::vector<KeyRange>& affectedRanges) {
	auto s = map.rangeContaining(keys.begin);
	if (s.begin() != keys.begin && s.value().isValid() && !s.value().isReady())
		affectedRanges.push_back(KeyRangeRef(s.begin(), keys.begin));
	affectedRanges.push_back(keys);
	auto e = map.rangeContaining(keys.end);
	if (e.begin() != keys.end && e.value().isValid() && !e.value().isReady())
		affectedRanges.push_back(KeyRangeRef(keys.end, e.end()));
}

RangeResult krmDecodeRanges(KeyRef mapPrefix, KeyRange keys, RangeResult kv, bool align) {
	ASSERT(!kv.more || kv.size() > 1);
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());

	RangeResult result;
	result.arena().dependsOn(kv.arena());
	result.arena().dependsOn(keys.arena());

	// Always push a kv pair <= keys.begin.
	KeyRef beginKey = keys.begin;
	if (!align && !kv.empty() && kv.front().key.startsWith(mapPrefix) && kv.front().key < withPrefix.begin) {
		beginKey = kv[0].key.removePrefix(mapPrefix);
	}
	ValueRef beginValue;
	if (!kv.empty() && kv.front().key.startsWith(mapPrefix) && kv.front().key <= withPrefix.begin) {
		beginValue = kv.front().value;
	}
	result.push_back(result.arena(), KeyValueRef(beginKey, beginValue));

	for (int i = 0; i < kv.size(); i++) {
		if (kv[i].key > withPrefix.begin && kv[i].key < withPrefix.end) {
			KeyRef k = kv[i].key.removePrefix(mapPrefix);
			result.push_back(result.arena(), KeyValueRef(k, kv[i].value));
		} else if (kv[i].key >= withPrefix.end) {
			kv.more = false;
			// There should be at most 1 value past mapPrefix + keys.end.
			ASSERT(i == kv.size() - 1);
			break;
		}
	}

	if (!kv.more) {
		KeyRef endKey = keys.end;
		if (!align && !kv.empty() && kv.back().key.startsWith(mapPrefix) && kv.back().key >= withPrefix.end) {
			endKey = kv.back().key.removePrefix(mapPrefix);
		}
		ValueRef endValue;
		if (!kv.empty()) {
			// In the aligned case, carry the last value to be the end value.
			if (align && kv.back().key.startsWith(mapPrefix) && kv.back().key > withPrefix.end) {
				endValue = result.back().value;
			} else {
				endValue = kv.back().value;
			}
		}
		result.push_back(result.arena(), KeyValueRef(endKey, endValue));
	}
	result.more = kv.more;

	return result;
}

// Returns keys.begin, all transitional points in keys, and keys.end, and their values
ACTOR Future<RangeResult> krmGetRanges(Transaction* tr, Key mapPrefix, KeyRange keys, int limit, int limitBytes) {
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());

	state GetRangeLimits limits(limit, limitBytes);
	limits.minRows = 2;
	RangeResult kv = wait(tr->getRange(lastLessOrEqual(withPrefix.begin), firstGreaterThan(withPrefix.end), limits));

	return krmDecodeRanges(mapPrefix, keys, kv);
}

ACTOR Future<RangeResult> krmGetRanges(Reference<ReadYourWritesTransaction> tr,
                                       Key mapPrefix,
                                       KeyRange keys,
                                       int limit,
                                       int limitBytes) {
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());

	state GetRangeLimits limits(limit, limitBytes);
	limits.minRows = 2;
	RangeResult kv = wait(tr->getRange(lastLessOrEqual(withPrefix.begin), firstGreaterThan(withPrefix.end), limits));

	return krmDecodeRanges(mapPrefix, keys, kv);
}

// Returns keys.begin, all transitional points in keys, and keys.end, and their values
ACTOR Future<RangeResult> krmGetRangesUnaligned(Transaction* tr,
                                                Key mapPrefix,
                                                KeyRange keys,
                                                int limit,
                                                int limitBytes) {
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());

	state GetRangeLimits limits(limit, limitBytes);
	limits.minRows = 2;
	// wait to include the next highest row >= keys.end in the result, so since end is exclusive, we need +2 and
	// !orEqual
	RangeResult kv =
	    wait(tr->getRange(lastLessOrEqual(withPrefix.begin), KeySelectorRef(withPrefix.end, false, +2), limits));

	return krmDecodeRanges(mapPrefix, keys, kv, false);
}

ACTOR Future<RangeResult> krmGetRangesUnaligned(Reference<ReadYourWritesTransaction> tr,
                                                Key mapPrefix,
                                                KeyRange keys,
                                                int limit,
                                                int limitBytes) {
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());

	state GetRangeLimits limits(limit, limitBytes);
	limits.minRows = 2;
	// wait to include the next highest row >= keys.end in the result, so since end is exclusive, we need +2 and
	// !orEqual
	RangeResult kv =
	    wait(tr->getRange(lastLessOrEqual(withPrefix.begin), KeySelectorRef(withPrefix.end, false, +2), limits));

	return krmDecodeRanges(mapPrefix, keys, kv, false);
}

void krmSetPreviouslyEmptyRange(Transaction* tr,
                                const KeyRef& mapPrefix,
                                const KeyRangeRef& keys,
                                const ValueRef& newValue,
                                const ValueRef& oldEndValue) {
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());
	tr->set(withPrefix.begin, newValue);
	tr->set(withPrefix.end, oldEndValue);
}

void krmSetPreviouslyEmptyRange(CommitTransactionRef& tr,
                                Arena& trArena,
                                const KeyRef& mapPrefix,
                                const KeyRangeRef& keys,
                                const ValueRef& newValue,
                                const ValueRef& oldEndValue) {
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());
	tr.set(trArena, withPrefix.begin, newValue);
	tr.set(trArena, withPrefix.end, oldEndValue);
}

ACTOR Future<Void> krmSetRange(Transaction* tr, Key mapPrefix, KeyRange range, Value value) {
	state KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + range.begin.toString(), mapPrefix.toString() + range.end.toString());
	RangeResult old =
	    wait(tr->getRange(lastLessOrEqual(withPrefix.end), firstGreaterThan(withPrefix.end), 1, Snapshot::True));

	Value oldValue;
	bool hasResult = old.size() > 0 && old[0].key.startsWith(mapPrefix);
	if (hasResult)
		oldValue = old[0].value;

	KeyRange conflictRange = KeyRangeRef(hasResult ? old[0].key : mapPrefix.toString(), keyAfter(withPrefix.end));
	if (!conflictRange.empty())
		tr->addReadConflictRange(conflictRange);

	tr->clear(withPrefix);
	tr->set(withPrefix.begin, value);
	tr->set(withPrefix.end, oldValue);

	return Void();
}

ACTOR Future<Void> krmSetRange(Reference<ReadYourWritesTransaction> tr, Key mapPrefix, KeyRange range, Value value) {
	state KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + range.begin.toString(), mapPrefix.toString() + range.end.toString());
	RangeResult old =
	    wait(tr->getRange(lastLessOrEqual(withPrefix.end), firstGreaterThan(withPrefix.end), 1, Snapshot::True));

	Value oldValue;
	bool hasResult = old.size() > 0 && old[0].key.startsWith(mapPrefix);
	if (hasResult)
		oldValue = old[0].value;

	KeyRange conflictRange = KeyRangeRef(hasResult ? old[0].key : mapPrefix.toString(), keyAfter(withPrefix.end));
	if (!conflictRange.empty())
		tr->addReadConflictRange(conflictRange);

	tr->clear(withPrefix); // clear [keyVersionMap/begin, keyVersionMap/end)
	tr->set(withPrefix.begin, value); // set keyVersionMap/begin to value(input version)
	// set keyVersionMap/end to oldValue: because end is exclusive here,
	// but starting from end it might be covered by another range file, so set it to old value
	tr->set(withPrefix.end, oldValue);

	return Void();
}

// Sets a range of keys in a key range map, coalescing with adjacent regions if the values match
// Ranges outside of maxRange will not be coalesced
// CAUTION: use care when attempting to coalesce multiple ranges in the same prefix in a single transaction
ACTOR template <class Transaction>
static Future<Void> krmSetRangeCoalescing_(Transaction* tr,
                                           Key mapPrefix,
                                           KeyRange range,
                                           KeyRange maxRange,
                                           Value value) {
	ASSERT(maxRange.contains(range));

	state KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + range.begin.toString(), mapPrefix.toString() + range.end.toString());
	state KeyRange maxWithPrefix =
	    KeyRangeRef(mapPrefix.toString() + maxRange.begin.toString(), mapPrefix.toString() + maxRange.end.toString());

	state std::vector<Future<RangeResult>> keys;
	keys.push_back(
	    tr->getRange(lastLessThan(withPrefix.begin), firstGreaterOrEqual(withPrefix.begin), 1, Snapshot::True));
	keys.push_back(
	    tr->getRange(lastLessOrEqual(withPrefix.end), firstGreaterThan(withPrefix.end) + 1, 2, Snapshot::True));
	wait(waitForAll(keys));

	// Determine how far to extend this range at the beginning
	auto beginRange = keys[0].get();
	bool hasBegin = beginRange.size() > 0 && beginRange[0].key.startsWith(mapPrefix);
	Value beginValue = hasBegin ? beginRange[0].value : ""_sr;

	state Key beginKey = withPrefix.begin;
	if (beginValue == value) {
		bool outsideRange = !hasBegin || beginRange[0].key < maxWithPrefix.begin;
		beginKey = outsideRange ? maxWithPrefix.begin : beginRange[0].key;
	}

	// Determine how far to extend this range at the end
	auto endRange = keys[1].get();
	bool hasEnd = endRange.size() >= 1 && endRange[0].key.startsWith(mapPrefix) && endRange[0].key <= withPrefix.end;
	bool hasNext = (endRange.size() == 2 && endRange[1].key.startsWith(mapPrefix)) ||
	               (endRange.size() == 1 && withPrefix.end < endRange[0].key && endRange[0].key.startsWith(mapPrefix));
	Value existingValue = hasEnd ? endRange[0].value : ""_sr;
	bool valueMatches = value == existingValue;

	KeyRange conflictRange = KeyRangeRef(hasBegin ? beginRange[0].key : mapPrefix, withPrefix.begin);
	if (!conflictRange.empty())
		tr->addReadConflictRange(conflictRange);

	conflictRange = KeyRangeRef(hasEnd ? endRange[0].key : mapPrefix,
	                            hasNext ? keyAfter(endRange.end()[-1].key) : strinc(mapPrefix));
	if (!conflictRange.empty())
		tr->addReadConflictRange(conflictRange);

	state Key endKey;
	state Value endValue;

	// Case 1: Coalesce completely with the following range
	if (hasNext && endRange.end()[-1].key <= maxWithPrefix.end && valueMatches) {
		endKey = endRange.end()[-1].key;
		endValue = endRange.end()[-1].value;
	}

	// Case 2: Coalesce with the following range only up to the end of maxRange
	else if (valueMatches) {
		endKey = maxWithPrefix.end;
		endValue = existingValue;
	}

	// Case 3: Don't coalesce
	else {
		endKey = withPrefix.end;
		endValue = existingValue;
	}

	tr->clear(KeyRangeRef(beginKey, endKey));

	ASSERT(value != endValue || endKey == maxWithPrefix.end);
	tr->set(beginKey, value);
	tr->set(endKey, endValue);

	return Void();
}
Future<Void> krmSetRangeCoalescing(Transaction* const& tr,
                                   Key const& mapPrefix,
                                   KeyRange const& range,
                                   KeyRange const& maxRange,
                                   Value const& value) {
	return krmSetRangeCoalescing_(tr, mapPrefix, range, maxRange, value);
}
Future<Void> krmSetRangeCoalescing(Reference<ReadYourWritesTransaction> const& tr,
                                   Key const& mapPrefix,
                                   KeyRange const& range,
                                   KeyRange const& maxRange,
                                   Value const& value) {
	return holdWhile(tr, krmSetRangeCoalescing_(tr.getPtr(), mapPrefix, range, maxRange, value));
}

TEST_CASE("/keyrangemap/decoderange/aligned") {
	Arena arena;
	Key prefix = "/prefix/"_sr;
	StringRef fullKeyA = StringRef(arena, "/prefix/a"_sr);
	StringRef fullKeyB = StringRef(arena, "/prefix/b"_sr);
	StringRef fullKeyC = StringRef(arena, "/prefix/c"_sr);
	StringRef fullKeyD = StringRef(arena, "/prefix/d"_sr);

	StringRef keyA = StringRef(arena, "a"_sr);
	StringRef keyB = StringRef(arena, "b"_sr);
	StringRef keyC = StringRef(arena, "c"_sr);
	StringRef keyD = StringRef(arena, "d"_sr);
	StringRef keyE = StringRef(arena, "e"_sr);
	StringRef keyAB = StringRef(arena, "ab"_sr);
	StringRef keyAC = StringRef(arena, "ac"_sr);
	StringRef keyCD = StringRef(arena, "cd"_sr);

	// Fake getRange() call.
	RangeResult kv;
	kv.push_back(arena, KeyValueRef(fullKeyA, keyA));
	kv.push_back(arena, KeyValueRef(fullKeyB, keyB));

	// [A, AB(start), AC(start), B]
	RangeResult decodedRanges = krmDecodeRanges(prefix, KeyRangeRef(keyAB, keyAC), kv);
	ASSERT(decodedRanges.size() == 2);
	ASSERT(decodedRanges.front().key == keyAB);
	ASSERT(decodedRanges.front().value == keyA);
	ASSERT(decodedRanges.back().key == keyAC);
	ASSERT(decodedRanges.back().value == keyA);

	kv.push_back(arena, KeyValueRef(fullKeyC, keyC));
	kv.push_back(arena, KeyValueRef(fullKeyD, keyD));

	// [A, AB(start), B, C, CD(end), D]
	decodedRanges = krmDecodeRanges(prefix, KeyRangeRef(keyAB, keyCD), kv);
	ASSERT(decodedRanges.size() == 4);
	ASSERT(decodedRanges.front().key == keyAB);
	ASSERT(decodedRanges.front().value == keyA);
	ASSERT(decodedRanges.back().key == keyCD);
	ASSERT(decodedRanges.back().value == keyC);

	// [""(start), A, B, C, D, E(end)]
	decodedRanges = krmDecodeRanges(prefix, KeyRangeRef(StringRef(), keyE), kv);
	ASSERT(decodedRanges.size() == 6);
	ASSERT(decodedRanges.front().key == StringRef());
	ASSERT(decodedRanges.front().value == StringRef());
	ASSERT(decodedRanges.back().key == keyE);
	ASSERT(decodedRanges.back().value == keyD);

	return Void();
}

TEST_CASE("/keyrangemap/decoderange/unaligned") {
	Arena arena;
	Key prefix = "/prefix/"_sr;
	StringRef fullKeyA = StringRef(arena, "/prefix/a"_sr);
	StringRef fullKeyB = StringRef(arena, "/prefix/b"_sr);
	StringRef fullKeyC = StringRef(arena, "/prefix/c"_sr);
	StringRef fullKeyD = StringRef(arena, "/prefix/d"_sr);

	StringRef keyA = StringRef(arena, "a"_sr);
	StringRef keyB = StringRef(arena, "b"_sr);
	StringRef keyC = StringRef(arena, "c"_sr);
	StringRef keyD = StringRef(arena, "d"_sr);
	StringRef keyE = StringRef(arena, "e"_sr);
	StringRef keyAB = StringRef(arena, "ab"_sr);
	StringRef keyAC = StringRef(arena, "ac"_sr);
	StringRef keyCD = StringRef(arena, "cd"_sr);

	// Fake getRange() call.
	RangeResult kv;
	kv.push_back(arena, KeyValueRef(fullKeyA, keyA));
	kv.push_back(arena, KeyValueRef(fullKeyB, keyB));

	// [A, AB(start), AC(start), B]
	RangeResult decodedRanges = krmDecodeRanges(prefix, KeyRangeRef(keyAB, keyAC), kv, false);
	ASSERT(decodedRanges.size() == 2);
	ASSERT(decodedRanges.front().key == keyA);
	ASSERT(decodedRanges.front().value == keyA);
	ASSERT(decodedRanges.back().key == keyB);
	ASSERT(decodedRanges.back().value == keyB);

	kv.push_back(arena, KeyValueRef(fullKeyC, keyC));
	kv.push_back(arena, KeyValueRef(fullKeyD, keyD));

	// [A, AB(start), B, C, CD(end), D]
	decodedRanges = krmDecodeRanges(prefix, KeyRangeRef(keyAB, keyCD), kv, false);
	ASSERT(decodedRanges.size() == 4);
	ASSERT(decodedRanges.front().key == keyA);
	ASSERT(decodedRanges.front().value == keyA);
	ASSERT(decodedRanges.back().key == keyD);
	ASSERT(decodedRanges.back().value == keyD);

	// [""(start), A, B, C, D, E(end)]
	decodedRanges = krmDecodeRanges(prefix, KeyRangeRef(StringRef(), keyE), kv, false);
	ASSERT(decodedRanges.size() == 6);
	ASSERT(decodedRanges.front().key == StringRef());
	ASSERT(decodedRanges.front().value == StringRef());
	ASSERT(decodedRanges.back().key == keyE);
	ASSERT(decodedRanges.back().value == keyD);

	return Void();
}