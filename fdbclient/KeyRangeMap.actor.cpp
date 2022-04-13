/*
 * KeyRangeMap.actor.cpp
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

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
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

RangeResult krmDecodeRanges(KeyRef mapPrefix, KeyRange keys, RangeResult kv) {
	ASSERT(!kv.more || kv.size() > 1);
	KeyRange withPrefix =
	    KeyRangeRef(mapPrefix.toString() + keys.begin.toString(), mapPrefix.toString() + keys.end.toString());

	ValueRef beginValue, endValue;
	if (kv.size() && kv[0].key.startsWith(mapPrefix))
		beginValue = kv[0].value;
	if (kv.size() && kv.end()[-1].key.startsWith(mapPrefix))
		endValue = kv.end()[-1].value;

	RangeResult result;
	result.arena().dependsOn(kv.arena());
	result.arena().dependsOn(keys.arena());

	result.push_back(result.arena(), KeyValueRef(keys.begin, beginValue));
	for (int i = 0; i < kv.size(); i++) {
		if (kv[i].key > withPrefix.begin && kv[i].key < withPrefix.end) {
			KeyRef k = kv[i].key.removePrefix(mapPrefix);
			result.push_back(result.arena(), KeyValueRef(k, kv[i].value));
		} else if (kv[i].key >= withPrefix.end)
			kv.more = false;
	}

	if (!kv.more)
		result.push_back(result.arena(), KeyValueRef(keys.end, endValue));
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

	tr->clear(withPrefix);
	tr->set(withPrefix.begin, value);
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
	Value beginValue = hasBegin ? beginRange[0].value : LiteralStringRef("");

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
	Value existingValue = hasEnd ? endRange[0].value : LiteralStringRef("");
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
