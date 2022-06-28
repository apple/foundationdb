/*
 * KeyRangeMap.h
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

#ifndef FLOW_KEYRANGEMAP_H
#define FLOW_KEYRANGEMAP_H
#pragma once

#include "flow/flow.h"
#include "fdbclient/FDBTypes.h"
#include "boost/range.hpp"
#include "flow/IndexedSet.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/RangeMap.h"
#include "fdbclient/Knobs.h"

using boost::iterator_range;

template <class Val, class Metric = int, class MetricFunc = ConstantMetric<Metric>>
class KeyRangeMap : public RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>,
                    NonCopyable,
                    public ReferenceCounted<KeyRangeMap<Val>> {
public:
	explicit KeyRangeMap(Val v = Val(), Key endKey = allKeys.end)
	  : RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>(endKey, v), mapEnd(endKey) {}
	void operator=(KeyRangeMap&& r) noexcept {
		mapEnd = std::move(r.mapEnd);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::operator=(std::move(r));
	}
	void insert(const KeyRangeRef& keys, const Val& value) {
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::insert(keys, value);
	}
	void insert(const KeyRef& key, const Val& value) {
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::insert(singleKeyRange(key), value);
	}
	std::vector<KeyRangeWith<Val>> getAffectedRangesAfterInsertion(const KeyRangeRef& keys,
	                                                               const Val& insertionValue = Val());
	typename RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::Ranges modify(
	    const KeyRangeRef&
	        keys) // Returns ranges, the first of which begins at keys.begin and the last of which ends at keys.end
	{
		MapPair<Key, Val> valueBeforeRange(
		    keys.begin, RangeMap<Key, Val, KeyRangeRef, Metric>::rangeContaining(keys.begin).value());
		MapPair<Key, Val> valueAfterRange(keys.end,
		                                  RangeMap<Key, Val, KeyRangeRef, Metric>::rangeContaining(keys.end).value());

		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(std::move(valueBeforeRange));
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(std::move(valueAfterRange));
		return RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::intersectingRanges(keys);
	}

	void rawErase(KeyRange const& range) {
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.erase(
		    RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(range.begin),
		    RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(range.end));
	}
	void rawInsert(Key const& key, Val const& value) {
		MapPair<Key, Val> pair(key, value);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    pair, true, RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::mf(pair));
	}
	void rawInsert(const std::vector<std::pair<MapPair<Key, Val>, Metric>>& pairs) {
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(pairs);
	}
	Key mapEnd;
};

template <class Val, class Metric = int, class MetricFunc = ConstantMetric<Metric>>
class CoalescedKeyRefRangeMap : public RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>, NonCopyable {
public:
	explicit CoalescedKeyRefRangeMap(Val v = Val(), Key endKey = allKeys.end)
	  : RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>(endKey, v), mapEnd(endKey) {}
	void operator=(CoalescedKeyRefRangeMap&& r) noexcept {
		mapEnd = std::move(r.mapEnd);
		RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::operator=(std::move(r));
	}
	void insert(const KeyRangeRef& keys, const Val& value);
	void insert(const KeyRef& key, const Val& value, Arena& arena);
	Key mapEnd;
};

template <class Val, class Metric = int, class MetricFunc = ConstantMetric<Metric>>
class CoalescedKeyRangeMap : public RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>, NonCopyable {
public:
	explicit CoalescedKeyRangeMap(Val v = Val(), Key endKey = allKeys.end)
	  : RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>(endKey, v), mapEnd(endKey) {}
	void operator=(CoalescedKeyRangeMap&& r) noexcept {
		mapEnd = std::move(r.mapEnd);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::operator=(std::move(r));
	}
	void insert(const KeyRangeRef& keys, const Val& value);
	void insert(const KeyRef& key, const Val& value);
	Key mapEnd;
};

class KeyRangeActorMap {
public:
	void getRangesAffectedByInsertion(const KeyRangeRef& keys, std::vector<KeyRange>& affectedRanges);
	void insert(const KeyRangeRef& keys, const Future<Void>& value) { map.insert(keys, value); }
	void cancel(const KeyRangeRef& keys) { insert(keys, Future<Void>()); }
	bool liveActorAt(const KeyRef& key) {
		Future<Void> actorAt = map[key];
		return actorAt.isValid() && !actorAt.isReady();
	}

private:
	KeyRangeMap<Future<Void>> map;
};

// krm*(): KeyRangeMap-like abstraction stored in the database, accessed through Transactions
class Transaction;
class ReadYourWritesTransaction;
Future<RangeResult> krmGetRanges(Transaction* const& tr,
                                 Key const& mapPrefix,
                                 KeyRange const& keys,
                                 int const& limit = CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
                                 int const& limitBytes = CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES);
Future<RangeResult> krmGetRanges(Reference<ReadYourWritesTransaction> const& tr,
                                 Key const& mapPrefix,
                                 KeyRange const& keys,
                                 int const& limit = CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
                                 int const& limitBytes = CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES);
void krmSetPreviouslyEmptyRange(Transaction* tr,
                                const KeyRef& mapPrefix,
                                const KeyRangeRef& keys,
                                const ValueRef& newValue,
                                const ValueRef& oldEndValue);
void krmSetPreviouslyEmptyRange(struct CommitTransactionRef& tr,
                                Arena& trArena,
                                const KeyRef& mapPrefix,
                                const KeyRangeRef& keys,
                                const ValueRef& newValue,
                                const ValueRef& oldEndValue);
Future<Void> krmSetRange(Transaction* const& tr, Key const& mapPrefix, KeyRange const& range, Value const& value);
Future<Void> krmSetRange(Reference<ReadYourWritesTransaction> const& tr,
                         Key const& mapPrefix,
                         KeyRange const& range,
                         Value const& value);
Future<Void> krmSetRangeCoalescing(Transaction* const& tr,
                                   Key const& mapPrefix,
                                   KeyRange const& range,
                                   KeyRange const& maxRange,
                                   Value const& value);
Future<Void> krmSetRangeCoalescing(Reference<ReadYourWritesTransaction> const& tr,
                                   Key const& mapPrefix,
                                   KeyRange const& range,
                                   KeyRange const& maxRange,
                                   Value const& value);
RangeResult krmDecodeRanges(KeyRef mapPrefix, KeyRange keys, RangeResult kv);

template <class Val, class Metric, class MetricFunc>
std::vector<KeyRangeWith<Val>> KeyRangeMap<Val, Metric, MetricFunc>::getAffectedRangesAfterInsertion(
    const KeyRangeRef& keys,
    const Val& insertionValue) {
	std::vector<KeyRangeWith<Val>> affectedRanges;

	{ // possible first range if no exact alignment
		auto r = RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::rangeContaining(keys.begin);
		if (r->begin() != keys.begin)
			affectedRanges.push_back(KeyRangeWith<Val>(KeyRangeRef(r->begin(), keys.begin), r->value()));
	}

	affectedRanges.push_back(KeyRangeWith<Val>(keys, insertionValue));

	{ // possible last range if no exact alignment
		auto r = RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::rangeContaining(keys.end);
		if (r->begin() != keys.end)
			affectedRanges.push_back(KeyRangeWith<Val>(KeyRangeRef(keys.end, r->end()), r->value()));
	}

	return affectedRanges;
}

template <class Val, class Metric, class MetricFunc>
void CoalescedKeyRangeMap<Val, Metric, MetricFunc>::insert(const KeyRangeRef& keys, const Val& value) {
	ASSERT(keys.end <= mapEnd);

	if (keys.empty())
		return;

	auto begin = RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(keys.begin);
	auto end = RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(keys.end);
	bool insertEnd = false;
	bool insertBegin = false;
	Val endVal;

	if (keys.end != mapEnd) {
		if (end->key != keys.end) {
			auto before_end = end;
			before_end.decrementNonEnd();
			if (value != before_end->value) {
				insertEnd = true;
				endVal = before_end->value;
			}
		}

		if (!insertEnd && end->value == value && end->key != mapEnd) {
			++end;
		}
	}

	if (keys.begin == allKeys.begin) {
		insertBegin = true;
	} else {
		auto before_begin = begin;
		before_begin.decrementNonEnd();
		if (before_begin->value != value)
			insertBegin = true;
	}

	RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.erase(begin, end);
	if (insertEnd) {
		MapPair<Key, Val> p(keys.end, endVal);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
	if (insertBegin) {
		MapPair<Key, Val> p(keys.begin, value);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
}

template <class Val, class Metric, class MetricFunc>
void CoalescedKeyRangeMap<Val, Metric, MetricFunc>::insert(const KeyRef& key, const Val& value) {
	ASSERT(key < mapEnd);

	auto begin = RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(key);
	auto end = begin;
	if (end->key == key)
		++end;

	bool insertEnd = false;
	bool insertBegin = false;
	Val endVal;

	if (!equalsKeyAfter(key, end->key)) {
		auto before_end = end;
		before_end.decrementNonEnd();
		if (value != before_end->value) {
			insertEnd = true;
			endVal = before_end->value;
		}
	}

	if (!insertEnd && end->value == value && end->key != mapEnd) {
		++end;
	}

	if (key == allKeys.begin) {
		insertBegin = true;
	} else {
		auto before_begin = begin;
		before_begin.decrementNonEnd();
		if (before_begin->value != value)
			insertBegin = true;
	}

	RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.erase(begin, end);
	if (insertEnd) {
		MapPair<Key, Val> p(keyAfter(key), endVal);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
	if (insertBegin) {
		MapPair<Key, Val> p(key, value);
		RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<Key, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
}

template <class Val, class Metric, class MetricFunc>
void CoalescedKeyRefRangeMap<Val, Metric, MetricFunc>::insert(const KeyRangeRef& keys, const Val& value) {
	ASSERT(keys.end <= mapEnd);

	if (keys.empty())
		return;

	auto begin = RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(keys.begin);
	auto end = RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(keys.end);
	bool insertEnd = false;
	bool insertBegin = false;
	Val endVal;

	if (keys.end != mapEnd) {
		if (end->key != keys.end) {
			auto before_end = end;
			before_end.decrementNonEnd();
			if (value != before_end->value) {
				insertEnd = true;
				endVal = before_end->value;
			}
		}

		if (!insertEnd && end->value == value && end->key != mapEnd) {
			++end;
		}
	}

	if (keys.begin == allKeys.begin) {
		insertBegin = true;
	} else {
		auto before_begin = begin;
		before_begin.decrementNonEnd();
		if (before_begin->value != value)
			insertBegin = true;
	}

	RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.erase(begin, end);
	if (insertEnd) {
		MapPair<KeyRef, Val> p(keys.end, endVal);
		RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
	if (insertBegin) {
		MapPair<KeyRef, Val> p(keys.begin, value);
		RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
}

template <class Val, class Metric, class MetricFunc>
void CoalescedKeyRefRangeMap<Val, Metric, MetricFunc>::insert(const KeyRef& key, const Val& value, Arena& arena) {
	ASSERT(key < mapEnd);

	auto begin = RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.lower_bound(key);
	auto end = begin;
	if (end->key == key)
		++end;

	bool insertEnd = false;
	bool insertBegin = false;
	Val endVal;

	if (!equalsKeyAfter(key, end->key)) {
		auto before_end = end;
		before_end.decrementNonEnd();
		if (value != before_end->value) {
			insertEnd = true;
			endVal = before_end->value;
		}
	}

	if (!insertEnd && end->value == value && end->key != mapEnd) {
		++end;
	}

	if (key == allKeys.begin) {
		insertBegin = true;
	} else {
		auto before_begin = begin;
		before_begin.decrementNonEnd();
		if (before_begin->value != value)
			insertBegin = true;
	}

	RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.erase(begin, end);
	if (insertEnd) {
		MapPair<KeyRef, Val> p(keyAfter(key, arena), endVal);
		RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
	if (insertBegin) {
		MapPair<KeyRef, Val> p(key, value);
		RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::map.insert(
		    p, true, RangeMap<KeyRef, Val, KeyRangeRef, Metric, MetricFunc>::mf(p));
	}
}

#endif
