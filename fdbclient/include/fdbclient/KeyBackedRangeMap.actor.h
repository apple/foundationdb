/*
 * KeyBackedRangeMap.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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
#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_KEYBACKEDRANGEMAP_ACTOR_G_H)
#define FDBCLIENT_KEYBACKEDRANGEMAP_ACTOR_G_H
#include "fdbclient/KeyBackedRangeMap.actor.g.h"
#elif !defined(FDBCLIENT_KEYBACKEDRANGEMAP_ACTOR_H)
#define FDBCLIENT_KEYBACKEDRANGEMAP_ACTOR_H

#include "fdbclient/KeyBackedTypes.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A local in-memory representation of a KeyBackedRangeMap snapshot
// It is invalid to look up a range in this map which not within the range of
// [first key of map, last key of map)
template <typename KeyType, typename ValueType>
struct KeyRangeMapSnapshot {
	typedef std::map<KeyType, ValueType> Map;

	// A default constructed map snapshot can't be used to look anything up because no ranges are covered.
	KeyRangeMapSnapshot() {}

	// Initialize map with a single range from min to max with a given ValueType
	KeyRangeMapSnapshot(const KeyType& min, const KeyType& max, const ValueType& val = {}) {
		map[min] = val;
		map[max] = val;
	}

	struct RangeValue {
		TypedRange<KeyType> range;
		ValueType value;
	};

	// Iterator for ranges in the map.  Ranges are represented by a key, its value, and the next key in the map.
	struct RangeIter {
		typename Map::const_iterator impl;

		using iterator_category = std::bidirectional_iterator_tag;
		using value_type = RangeIter;
		using pointer = RangeIter*;
		using reference = RangeIter&;

		TypedRange<const KeyType&> range() const { return { impl->first, std::next(impl)->first }; }
		const ValueType& value() const { return impl->second; }

		const RangeIter& operator*() const { return *this; }
		const RangeIter* operator->() const { return this; }

		RangeIter operator++(int) { return { impl++ }; }
		RangeIter operator--(int) { return { impl-- }; }
		RangeIter& operator++() {
			++impl;
			return *this;
		}
		RangeIter& operator--() {
			--impl;
			return *this;
		}

		bool operator==(const RangeIter& rhs) const { return impl == rhs.impl; }
		bool operator!=(const RangeIter& rhs) const { return impl == rhs.impl; }
	};

	// Range-for compatible object representing a list of contiguous ranges.
	struct Ranges {
		RangeIter iBegin, iEnd;
		RangeIter begin() const { return iBegin; }
		RangeIter end() const { return iEnd; }
	};

	RangeIter rangeContaining(const KeyType& begin) const {
		ASSERT(map.size() >= 2);
		auto i = map.upper_bound(begin);
		ASSERT(i != map.begin());
		ASSERT(i != map.end());
		return { --i };
	}

	// Get a set of Ranges which cover [begin, end)
	Ranges intersectingRanges(const KeyType& begin, const KeyType& end) const {
		return { rangeContaining(begin), { map.lower_bound(end) } };
	}

	Ranges ranges() const { return { { map.begin() }, { std::prev(map.end()) } }; }

	Map map;
};

// KeyBackedKeyRangeMap is similar to KeyRangeMap but without a Metric
// It is assumed that any range not covered by the map is set to a default ValueType()
// The ValueType must have
//     // Return a copy of *this updated with properties in value
//     ValueType update(ValueType const& value) const;
//
//     // Return true if adjacent ranges with values *this and value could be merged to a single
//     // range represented by *this and retain identical meaning
//     bool canMerge(ValueType const& value) const;
template <typename KeyType,
          typename ValueType,
          typename KeyCodec = TupleCodec<KeyType>,
          typename ValueCodec = TupleCodec<ValueType>>
class KeyBackedRangeMap {
public:
	typedef KeyBackedMap<KeyType, ValueType, KeyCodec, ValueCodec> Map;
	typedef typename Map::KeySelector KeySelector;
	typedef typename Map::RangeResultType RangeResultType;
	typedef KeyRangeMapSnapshot<KeyType, ValueType> LocalSnapshot;
	typedef typename LocalSnapshot::RangeValue RangeValue;

	KeyBackedRangeMap(KeyRef prefix = invalidKey, Optional<WatchableTrigger> trigger = {}, ValueCodec valueCodec = {})
	  : kvMap(prefix, trigger, valueCodec) {}

	// Get the RangeValue for the range that contains key, if there is a begin and end in the map which contain key
	template <class Transaction>
	Future<Optional<RangeValue>> getRangeForKey(Transaction tr, KeyType const& key) const {
		GetRangeLimits limits(2);
		limits.minRows = 2;

		return map(
		    kvMap.getRange(tr, KeySelector::lastLessOrEqual(key), KeySelector::firstGreaterThan(key) + 1, limits),
		    [](RangeResultType const& bvs) -> Optional<RangeValue> {
			    // Result set is (boundary, value) pairs
			    if (bvs.results.size() != 2) {
				    return {};
			    }
			    return RangeValue{ { bvs.results.front().first, bvs.results.back().first },
				                   bvs.results.front().second };
		    });
	}

	// Update the range from begin to end by applying valueUpdate to it
	// Since the transaction type may not be RYW, this method must take care to not rely on reading its own updates.
	ACTOR template <class Transaction>
	Future<Void> updateRangeActor(KeyBackedRangeMap self,
	                              Transaction tr,
	                              KeyType begin,
	                              KeyType end,
	                              ValueType valueUpdate) {

		state Future<Optional<RangeValue>> beginRange = self.getRangeForKey(tr, begin);
		state Future<Optional<RangeValue>> endRange = self.getRangeForKey(tr, end);
		state Future<RangeResultType> middleBoundaries = self.kvMap.getRange(
		    tr, KeySelector::firstGreaterThan(begin), KeySelector::firstGreaterOrEqual(end), GetRangeLimits(1e4));

		wait(success(beginRange) && success(endRange));

		if (beginRange.get().present()) {
			// Begin is in a range that exists, so set begin = the current value updated with valueUpdate.
			// Note that begin may already be the start of the range it exists in, which also works.
			self.kvMap.set(tr, begin, beginRange.get()->value.update(valueUpdate));
		} else {
			// Begin is not in any range that exists, so initialize the begin boundary to valueUpdate
			self.kvMap.set(tr, begin, valueUpdate);
		}

		if (endRange.get().present()) {
			// End is in a range that exists, so if that range does not start with end split it at end by setting
			// end to the range's value.
			if (endRange.get()->range.begin != end) {
				self.kvMap.set(tr, end, endRange.get()->value.split());
			}
		} else {
			// End is not in a range that exists, so set end to a new ValueType to terminate it
			self.kvMap.set(tr, end, ValueType());
		}

		loop {
			RangeResultType boundaries = wait(middleBoundaries);
			for (auto const& bv : boundaries.results) {
				self.kvMap.set(tr, bv.first, bv.second.update(valueUpdate));
			}
			if (!boundaries.more) {
				break;
			}
			middleBoundaries = self.kvMap.getRange(tr,
			                                       KeySelector::firstGreaterThan(boundaries.results.back().first),
			                                       KeySelector::firstGreaterOrEqual(end),
			                                       GetRangeLimits(1e4));
		}

		return Void();
	}

	template <class Transaction>
	Future<Void> updateRange(Transaction* tr, KeyType const& begin, KeyType const& end, ValueType const& valueUpdate) {
		return updateRangeActor(*this, tr, begin, end, valueUpdate);
	}

	// Return a LocalSnapshot of all ranges from the map which cover the range of begin through end.
	// If the map in the database does not have boundaries <=begin or >=end then these boundaries will be
	// added to the returned snapshot with a default ValueType.
	ACTOR template <class Transaction>
	static Future<LocalSnapshot> getSnapshotActor(KeyBackedRangeMap self, Transaction tr, KeyType begin, KeyType end) {
		// Start reading the range of of key boundaries which would cover begin through end using key selectors
		state Future<RangeResultType> boundariesFuture = self.kvMap.getRange(
		    tr, KeySelector::lastLessOrEqual(begin), KeySelector::firstGreaterThan(end), GetRangeLimits(1e4));

		state LocalSnapshot result;
		loop {
			RangeResultType boundaries = wait(boundariesFuture);
			for (auto const& bv : boundaries.results) {
				result.map[bv.first] = bv.second;
			}
			if (!boundaries.more) {
				break;
			}
			// Continue reading starting from the first key after the last key read.
			boundariesFuture = self.kvMap.getRange(tr,
			                                       KeySelector::firstGreaterThan(boundaries.results.back().first),
			                                       KeySelector::firstGreaterThan(end),
			                                       GetRangeLimits(1e4));
		}

		// LocalSnapshot requires initialization of the widest range it will be queried with, so we must ensure that has
		// been done now. If the map does not start at or before begin then add begin with a default value type
		if (result.map.empty() || result.map.begin()->first > begin) {
			result.map[begin] = ValueType();
		}
		// The map is no longer empty, so if the last key is not >= end then add end with a default value type
		if (result.map.rbegin()->first < end) {
			result.map[end] = ValueType();
		}
		return result;
	}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, Future<LocalSnapshot>>::type
	getSnapshot(Transaction tr, KeyType begin, KeyType end) const {
		return getSnapshotActor(*this, tr, begin, end);
	}

	template <class DB>
	typename std::enable_if<is_transaction_creator<DB>, Future<LocalSnapshot>>::type getSnapshot(Reference<DB> db,
	                                                                                             KeyType begin,
	                                                                                             KeyType end) const {
		return runTransaction(db, [self = *this, begin, end](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getSnapshot(tr, begin, end);
		});
	}

private:
	Map kvMap;
};

#include "flow/unactorcompiler.h"

#endif
