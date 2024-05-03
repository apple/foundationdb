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

#include "flow/FastRef.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A local in-memory representation of a KeyBackedRangeMap snapshot
// It is invalid to look up a range in this map which not within the range of
// [first key of snapshot, last key of snapshot)
// This is ReferenceCounted as it can be large and there is no reason to copy it as
// it should not be modified locally.
template <typename KeyType, typename ValueType>
struct KeyRangeMapSnapshot : public ReferenceCounted<KeyRangeMapSnapshot<KeyType, ValueType>> {
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
		bool operator!=(const RangeIter& rhs) const { return impl != rhs.impl; }
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
//     ValueType apply(ValueType const& value) const;
//
//     // Return true if the two values are identical in meaning so adjacent ranges using either value can be merged
//     bool operator==(ValueType const& value) const;
// For debug output, KeyType and ValueType must both be supported by fmt::formatter<>
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

	KeyBackedRangeMap(KeyRef prefix, Optional<WatchableTrigger> trigger, ValueCodec valueCodec)
	  : kvMap(prefix, trigger, valueCodec) {}

	// Get the RangeValue for the range that contains key, if there is a begin and end in the map which contain key
	ACTOR template <class Transaction>
	static Future<Optional<RangeValue>> getRangeForKey(KeyBackedRangeMap self,
	                                                   Transaction tr,
	                                                   KeyType key,
	                                                   Snapshot snapshot = Snapshot::False) {
		state Future<Optional<typename Map::KVType>> begin = self.kvMap.seekLessOrEqual(tr, key, snapshot);
		state Future<Optional<typename Map::KVType>> end = self.kvMap.seekGreaterThan(tr, key, snapshot);
		wait(success(begin) && success(end));

		if (begin.get().present() && end.get().present()) {
			return RangeValue{ { begin.get()->key, end.get()->key }, begin.get()->value };
		}
		return Optional<RangeValue>();
	}

	template <class Transaction>
	Future<Optional<RangeValue>> getRangeForKey(Transaction tr,
	                                            KeyType const& key,
	                                            Snapshot snapshot = Snapshot::False) const {
		return getRangeForKey(*this, tr, key, snapshot);
	}

	// Update the range from begin to end by either applying valueUpdate to it, or if replace is true then replace
	// the the range with the given value.
	// Adjacent ranges that are identical will be coalesced in the update transaction.
	// Since the transaction type may not be RYW, this method must take care to not rely on reading its own updates.
	ACTOR template <class Transaction>
	static Future<Void> updateRangeActor(KeyBackedRangeMap self,
	                                     Transaction tr,
	                                     KeyType begin,
	                                     KeyType end,
	                                     ValueType valueUpdate,
	                                     bool replace) {
		kbt_debug("RANGEMAP updateRange start {} to {} value {}\n", begin, end, valueUpdate);

		// In order to update and coalesce the range, we need all range boundaries from
		//   lastLessThan(begin) inclusive
		// through
		//   firstGreaterOrEqual(end) inclusive, which is firstGreaterThan(end) exclusive
		// so we can
		//   - compare each modified range boundary to the previous boundary to see if the modified boundary
		//     can be removed, which is the case if its value matches the previous boundary's value
		//   - compare the boundary after the last modified boundary to see if it can be removed because
		//     its value matches the last modified boundary's value
		Optional<typename Map::KVType> beginKV = wait(self.kvMap.seekLessThan(tr, begin));
		state KeySelector rangeBegin = KeySelector::firstGreaterOrEqual(beginKV.present() ? beginKV->key : begin);

		// rangeEnd is past end so the result will include end if it exists
		state KeySelector rangeEnd = KeySelector::firstGreaterThan(end);

		state int readSize = BUGGIFY ? 1 : 100000;
		state Future<RangeResultType> boundariesFuture =
		    self.kvMap.getRange(tr, rangeBegin, rangeEnd, GetRangeLimits(readSize));

		// As we walk through the range boundaries, keep two values about the last visited boundary
		state ValueType original; // value prior to modification
		state Optional<ValueType> previous; // value after modification
		// Indicates that begin has been either seen/updated or created.
		state bool beginDone = false;
		// Indicates that end was found
		state bool endFound = false;

		// The results will contain
		//   A) 0 or 1 key < begin    Use this to initialize previous.
		//   B) 0 or 1 key == begin   Create/update this value if it doesn't exist.
		//                            Possibly delete it if update matches previous range.
		//   C) >=0 keys > begin and < end   Update these, delete if they match previous
		//   D) 0 or 1 key == end.  Delete this key if it matches previous. Set to default if it doesn't exist.
		loop {
			kbt_debug("RANGEMAP updateRange loop\n");
			RangeResultType boundaries = wait(boundariesFuture);
			for (auto const& bv : boundaries.results) {
				kbt_debug("RANGEMAP updateRange   result key={} value={}\n", bv.first, bv.second);
				// Should never see a result past the end key
				ASSERT(!endFound);

				original = bv.second;

				if (bv.first > begin) {
					if (!beginDone) {
						kbt_debug("RANGEMAP updateRange !beginDone\n");
						// Begin was not found and we've passed it.
						ValueType val = replace ? valueUpdate : previous.orDefault(ValueType()).apply(valueUpdate);
						// Set begin if the new val is different from the previous range value if it exists
						if (previous != val) {
							self.kvMap.set(tr, begin, val);
							previous = val;
						}
						beginDone = true;
					}
					if (bv.first < end) {
						// Case C
						kbt_debug("RANGEMAP updateRange Case C\n");
						ValueType val = replace ? valueUpdate : bv.second.apply(valueUpdate);
						if (previous == val) {
							self.kvMap.erase(tr, bv.first);
						} else {
							self.kvMap.set(tr, bv.first, val);
							previous = val;
						}
					} else {
						if (bv.first == end) {
							// Case D
							kbt_debug("RANGEMAP updateRange Case D\n");
							if (previous == bv.second) {
								self.kvMap.erase(tr, end);
							}
							endFound = true;
						}
					}
				} else if (bv.first == begin) {
					// Case B
					kbt_debug("RANGEMAP updateRange Case B\n");
					ValueType val = replace ? valueUpdate : bv.second.apply(valueUpdate);
					if (previous == val) {
						self.kvMap.erase(tr, begin);
					} else {
						self.kvMap.set(tr, begin, val);
					}
					beginDone = true;
					previous = val;
				} else {
					// Case A
					kbt_debug("RANGEMAP updateRange Case A\n");
					previous = bv.second;
				}
			}
			if (!boundaries.more) {
				break;
			}
			ASSERT(!boundaries.results.empty());

			// Continue reading starting from the first key after the last key read.
			rangeBegin = KeySelector::firstGreaterThan(boundaries.results.back().first);
			boundariesFuture = self.kvMap.getRange(tr, rangeBegin, rangeEnd, GetRangeLimits(readSize));
		}

		kbt_debug("RANGEMAP updateRange beginDone {} endFound {}  previous {}  default {}\n",
		          beginDone,
		          endFound,
		          previous,
		          ValueType());

		// If begin was never found or passed/created
		if (!beginDone) {
			ValueType val = replace ? valueUpdate : previous.orDefault(ValueType()).apply(valueUpdate);
			if (previous != val) {
				kbt_debug("RANGEMAP updateRange set begin\n");
				self.kvMap.set(tr, begin, valueUpdate);
				previous = val;
			}
		}

		// If end was not found, we may need to set it to restore the value of the range that begin-end has split.
		// The value that the range starting at end should have is in original, the unmodified value of the last
		// range boundary visited.  If the previous range value leading up to end is not the same as original, then
		// set end to original to restore the range >= end to what it was before this update.
		if (!endFound && previous != original) {
			kbt_debug("RANGEMAP updateRange set end\n");
			self.kvMap.set(tr, end, original);
		}

		return Void();
	}

	template <class Transaction>
	Future<Void> updateRange(Transaction tr,
	                         KeyType const& begin,
	                         KeyType const& end,
	                         ValueType const& valueUpdate,
	                         bool replace = false) const {
		if constexpr (is_transaction_creator<Transaction>) {
			return runTransaction(tr, [=, self = *this](decltype(tr->createTransaction()) tr) {
				return self.updateRange(tr, begin, end, valueUpdate, replace);
			});
		} else {
			return updateRangeActor(*this, tr, begin, end, valueUpdate, replace);
		}
	}

	ACTOR template <class Transaction>
	static Future<Reference<LocalSnapshot>> getSnapshotActor(KeyBackedRangeMap self,
	                                                         Transaction tr,
	                                                         KeyType begin,
	                                                         KeyType end) {
		kbt_debug("RANGEMAP snapshot start\n");

		// The range read should start at KeySelector::lastLessOrEqual(begin) to get the range which covers begin.
		// However this could touch a key outside of the map subspace which can lead to various errors.
		// Use seekLessOrEqual() to find a begin key safely.
		Optional<typename Map::KVType> beginKV = wait(self.kvMap.seekLessOrEqual(tr, begin));
		state KeySelector rangeBegin = KeySelector::firstGreaterOrEqual(beginKV.present() ? beginKV->key : begin);

		// rangeEnd is past end so the result will include end if it exists
		state KeySelector rangeEnd = KeySelector::firstGreaterThan(end);

		state int readSize = BUGGIFY ? 1 : 100000;
		state Future<RangeResultType> boundariesFuture =
		    self.kvMap.getRange(tr, rangeBegin, rangeEnd, GetRangeLimits(readSize));

		state Reference<LocalSnapshot> result = makeReference<LocalSnapshot>();
		loop {
			kbt_debug("RANGEMAP snapshot loop\n");
			RangeResultType boundaries = wait(boundariesFuture);
			for (auto const& bv : boundaries.results) {
				result->map[bv.first] = bv.second;
			}
			if (!boundaries.more) {
				break;
			}
			ASSERT(!boundaries.results.empty());

			// Continue reading starting from the first key after the last key read.
			rangeBegin = KeySelector::firstGreaterThan(boundaries.results.back().first);
			boundariesFuture = self.kvMap.getRange(tr, rangeBegin, rangeEnd, GetRangeLimits(readSize));
		}

		// LocalSnapshot requires initialization of the widest range it will be queried with, so we must ensure that has
		// been done now. If the map does not start at or before begin then add begin with a default value type
		if (result->map.empty() || result->map.begin()->first > begin) {
			result->map[begin] = ValueType();
		}
		// The map is no longer empty, so if the last key is not >= end then add end with a default value type
		if (result->map.rbegin()->first < end) {
			result->map[end] = ValueType();
		}

		kbt_debug("RANGEMAP snapshot end\n");
		return result;
	}

	// Return a LocalSnapshot of all ranges from the map which cover the range of begin through end.
	// If the map in the database does not have boundaries <=begin or >=end then these boundaries will be
	// added to the returned snapshot with a default ValueType.
	template <class Transaction>
	Future<Reference<LocalSnapshot>> getSnapshot(Transaction tr, KeyType const& begin, KeyType const& end) const {
		if constexpr (is_transaction_creator<Transaction>) {
			return runTransaction(tr, [=, self = *this](decltype(tr->createTransaction()) tr) {
				return self.getSnapshot(tr, begin, end);
			});
		} else {
			return getSnapshotActor(*this, tr, begin, end);
		}
	}

private:
	Map kvMap;
};

#include "flow/unactorcompiler.h"

#endif
