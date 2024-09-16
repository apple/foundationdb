/*
 * ReadYourWrites.actor.cpp
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

#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Atomic.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/Util.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class RYWImpl {
public:
	template <class Iter>
	static void dump(Iter it) {
		it.skip(allKeys.begin);
		Arena arena;
		while (true) {
			Optional<StringRef> key = StringRef();
			if (it.is_kv()) {
				auto kv = it.kv(arena);
				if (kv)
					key = kv->key;
			}
			TraceEvent("RYWDump")
			    .detail("Begin", it.beginKey())
			    .detail("End", it.endKey())
			    .detail("Unknown", it.is_unknown_range())
			    .detail("Empty", it.is_empty_range())
			    .detail("KV", it.is_kv())
			    .detail("Key", key.get());
			if (it.endKey() == allKeys.end)
				break;
			++it;
		}
	}

	struct GetValueReq {
		explicit GetValueReq(Key key) : key(key) {}
		Key key;
		typedef Optional<Value> Result;
	};

	struct GetKeyReq {
		explicit GetKeyReq(KeySelector key) : key(key) {}
		KeySelector key;
		typedef Key Result;
	};

	template <bool reverse>
	struct GetRangeReq {
		GetRangeReq(KeySelector begin, KeySelector end, GetRangeLimits limits)
		  : begin(begin), end(end), limits(limits) {}
		KeySelector begin, end;
		GetRangeLimits limits;
		using Result = RangeResult;
	};

	template <bool reverse>
	struct GetMappedRangeReq {
		GetMappedRangeReq(KeySelector begin, KeySelector end, Key mapper, GetRangeLimits limits)
		  : begin(begin), end(end), mapper(mapper), limits(limits) {}
		KeySelector begin, end;
		Key mapper;
		GetRangeLimits limits;
		using Result = MappedRangeResult;
	};

	// read() Performs a read (get, getKey, getRange, etc), in the context of the given transaction.  Snapshot or RYW
	// reads are distinguished by the type Iter being SnapshotCache::iterator or RYWIterator. Fills in the snapshot
	// cache as a side effect but does not affect conflict ranges. Some (indicated) overloads of read are required
	// to update the given *it to point to the key that was read, so that the corresponding overload of
	// addConflictRange() can make use of it.

	ACTOR template <class Iter>
	static Future<Optional<Value>> read(ReadYourWritesTransaction* ryw, GetValueReq read, Iter* it) {
		// This overload is required to provide postcondition: it->extractWriteMapIterator().segmentContains(read.key)

		if (ryw->options.bypassUnreadable) {
			it->bypassUnreadableProtection();
		}
		it->skip(read.key);
		state bool dependent = it->is_dependent();
		if (it->is_kv()) {
			const KeyValueRef* result = it->kv(ryw->arena);
			if (result != nullptr) {
				return result->value;
			} else {
				return Optional<Value>();
			}
		} else if (it->is_empty_range()) {
			return Optional<Value>();
		} else {
			Optional<Value> res = wait(ryw->tr.get(read.key, Snapshot::True));
			KeyRef k(ryw->arena, read.key);

			if (res.present()) {
				if (ryw->cache.insert(k, res.get()))
					ryw->arena.dependsOn(res.get().arena());
				if (!dependent)
					return res;
			} else {
				ryw->cache.insert(k, Optional<ValueRef>());
				if (!dependent)
					return Optional<Value>();
			}

			// There was a dependent write at the key, so we need to lookup the iterator again
			it->skip(k);

			ASSERT(it->is_kv());
			const KeyValueRef* result = it->kv(ryw->arena);
			if (result != nullptr) {
				return result->value;
			} else {
				return Optional<Value>();
			}
		}
	}

	ACTOR template <class Iter>
	static Future<Key> read(ReadYourWritesTransaction* ryw, GetKeyReq read, Iter* it) {
		if (read.key.offset > 0) {
			RangeResult result =
			    wait(getRangeValue(ryw, read.key, firstGreaterOrEqual(ryw->getMaxReadKey()), GetRangeLimits(1), it));
			if (result.readToBegin)
				return allKeys.begin;
			if (result.readThroughEnd || !result.size())
				return ryw->getMaxReadKey();
			return result[0].key;
		} else {
			read.key.offset++;
			RangeResult result =
			    wait(getRangeValueBack(ryw, firstGreaterOrEqual(allKeys.begin), read.key, GetRangeLimits(1), it));
			if (result.readThroughEnd)
				return ryw->getMaxReadKey();
			if (result.readToBegin || !result.size())
				return allKeys.begin;
			return result[0].key;
		}
	};

	template <class Iter>
	static Future<RangeResult> read(ReadYourWritesTransaction* ryw, GetRangeReq<false> read, Iter* it) {
		return getRangeValue(ryw, read.begin, read.end, read.limits, it);
	};

	template <class Iter>
	static Future<RangeResult> read(ReadYourWritesTransaction* ryw, GetRangeReq<true> read, Iter* it) {
		return getRangeValueBack(ryw, read.begin, read.end, read.limits, it);
	};

	// readThrough() performs a read in the RYW disabled case, passing it on relatively directly to the underlying
	// transaction. Responsible for clipping results to the non-system keyspace when appropriate, since NativeAPI
	// doesn't do that.

	static Future<Optional<Value>> readThrough(ReadYourWritesTransaction* ryw, GetValueReq read, Snapshot snapshot) {
		return ryw->tr.get(read.key, snapshot);
	}

	ACTOR static Future<Key> readThrough(ReadYourWritesTransaction* ryw, GetKeyReq read, Snapshot snapshot) {
		Key key = wait(ryw->tr.getKey(read.key, snapshot));
		if (ryw->getMaxReadKey() < key)
			return ryw->getMaxReadKey(); // Filter out results in the system keys if they are not accessible
		return key;
	}

	ACTOR template <bool backwards>
	static Future<RangeResult> readThrough(ReadYourWritesTransaction* ryw,
	                                       GetRangeReq<backwards> read,
	                                       Snapshot snapshot) {
		if (backwards && read.end.offset > 1) {
			// FIXME: Optimistically assume that this will not run into the system keys, and only reissue if the result
			// actually does.
			Key key = wait(ryw->tr.getKey(read.end, snapshot));
			if (key > ryw->getMaxReadKey())
				read.end = firstGreaterOrEqual(ryw->getMaxReadKey());
			else
				read.end = KeySelector(firstGreaterOrEqual(key), key.arena());
		}

		RangeResult v = wait(
		    ryw->tr.getRange(read.begin, read.end, read.limits, snapshot, backwards ? Reverse::True : Reverse::False));
		KeyRef maxKey = ryw->getMaxReadKey();
		if (v.size() > 0) {
			if (!backwards && v[v.size() - 1].key >= maxKey) {
				state RangeResult _v = v;
				int i = _v.size() - 2;
				for (; i >= 0 && _v[i].key >= maxKey; --i) {
				}
				return RangeResult(RangeResultRef(VectorRef<KeyValueRef>(&_v[0], i + 1), false), _v.arena());
			}
		}

		return v;
	}

	// addConflictRange(ryw,read,result) is called after a serializable read and is responsible for adding the relevant
	// conflict range

	template <bool mustUnmodified = false>
	static void addConflictRange(ReadYourWritesTransaction* ryw,
	                             GetValueReq read,
	                             WriteMap::iterator& it,
	                             Optional<Value> result) {
		// it will already point to the right segment (see the calling code in read()), so we don't need to skip
		// read.key will be copied into ryw->arena inside of updateConflictMap if it is being added
		updateConflictMap<mustUnmodified>(ryw, read.key, it);
	}

	static void addConflictRange(ReadYourWritesTransaction* ryw, GetKeyReq read, WriteMap::iterator& it, Key result) {
		KeyRangeRef readRange;
		if (read.key.offset <= 0)
			readRange = KeyRangeRef(KeyRef(ryw->arena, result),
			                        read.key.orEqual ? keyAfter(read.key.getKey(), ryw->arena)
			                                         : KeyRef(ryw->arena, read.key.getKey()));
		else
			readRange = KeyRangeRef(read.key.orEqual ? keyAfter(read.key.getKey(), ryw->arena)
			                                         : KeyRef(ryw->arena, read.key.getKey()),
			                        keyAfter(result, ryw->arena));

		it.skip(readRange.begin);
		ryw->updateConflictMap(readRange, it);
	}

	template <bool mustUnmodified = false, class RangeResultFamily = RangeResult>
	static void addConflictRange(ReadYourWritesTransaction* ryw,
	                             GetRangeReq<false> read,
	                             WriteMap::iterator& it,
	                             RangeResultFamily& result) {
		KeyRef rangeBegin, rangeEnd;
		bool endInArena = false;

		if (read.begin.getKey() < read.end.getKey()) {
			rangeBegin = read.begin.getKey();
			// If the end offset is 1 (first greater than / first greater or equal) or more, then no changes to the
			// range after the returned results can change the outcome.
			rangeEnd = read.end.offset > 0 && result.more ? read.begin.getKey() : read.end.getKey();
		} else {
			rangeBegin = read.end.getKey();
			rangeEnd = read.begin.getKey();
		}

		if (result.readToBegin && read.begin.offset <= 0)
			rangeBegin = allKeys.begin;
		if (result.readThroughEnd && read.end.offset > 0)
			rangeEnd = ryw->getMaxReadKey();

		if (result.size()) {
			if (read.begin.offset <= 0)
				rangeBegin = std::min(rangeBegin, result[0].key);
			if (rangeEnd <= result.end()[-1].key) {
				rangeEnd = keyAfter(result.end()[-1].key, ryw->arena);
				endInArena = true;
			}
		}

		KeyRangeRef readRange =
		    KeyRangeRef(KeyRef(ryw->arena, rangeBegin), endInArena ? rangeEnd : KeyRef(ryw->arena, rangeEnd));
		it.skip(readRange.begin);
		updateConflictMap<mustUnmodified>(ryw, readRange, it);
	}

	// In the case where RangeResultFamily is MappedRangeResult, it only adds the primary range to conflict.
	template <bool mustUnmodified = false, class RangeResultFamily = RangeResult>
	static void addConflictRange(ReadYourWritesTransaction* ryw,
	                             GetRangeReq<true> read,
	                             WriteMap::iterator& it,
	                             RangeResultFamily& result) {
		KeyRef rangeBegin, rangeEnd;
		bool endInArena = false;

		if (read.begin.getKey() < read.end.getKey()) {
			// If the begin offset is 1 (first greater than / first greater or equal) or less, then no changes to the
			// range prior to the returned results can change the outcome.
			rangeBegin = read.begin.offset <= 1 && result.more ? read.end.getKey() : read.begin.getKey();
			rangeEnd = read.end.getKey();
		} else {
			rangeBegin = read.end.getKey();
			rangeEnd = read.begin.getKey();
		}

		if (result.readToBegin && read.begin.offset <= 0)
			rangeBegin = allKeys.begin;
		if (result.readThroughEnd && read.end.offset > 0)
			rangeEnd = ryw->getMaxReadKey();

		if (result.size()) {
			rangeBegin = std::min(rangeBegin, result.end()[-1].key);
			if (read.end.offset > 0 && rangeEnd <= result[0].key) {
				rangeEnd = keyAfter(result[0].key, ryw->arena);
				endInArena = true;
			}
		}

		KeyRangeRef readRange =
		    KeyRangeRef(KeyRef(ryw->arena, rangeBegin), endInArena ? rangeEnd : KeyRef(ryw->arena, rangeEnd));
		it.skip(readRange.begin);
		updateConflictMap<mustUnmodified>(ryw, readRange, it);
	}

	template <bool mustUnmodified = false>
	static void updateConflictMap(ReadYourWritesTransaction* ryw, KeyRef const& key, WriteMap::iterator& it) {
		// it.skip( key );
		// ASSERT( it.beginKey() <= key && key < it.endKey() );
		if (mustUnmodified && !it.is_unmodified_range()) {
			throw get_mapped_range_reads_your_writes();
		}
		if (it.is_unmodified_range() || (it.is_operation() && !it.is_independent())) {
			ryw->approximateSize += 2 * key.expectedSize() + 1 + sizeof(KeyRangeRef);
			ryw->readConflicts.insert(singleKeyRange(key, ryw->arena), true);
		}
	}

	template <bool mustUnmodified = false>
	static void updateConflictMap(ReadYourWritesTransaction* ryw, KeyRangeRef const& keys, WriteMap::iterator& it) {
		// it.skip( keys.begin );
		// ASSERT( it.beginKey() <= keys.begin && keys.begin < it.endKey() );
		for (; it.beginKey() < keys.end; ++it) {
			if (mustUnmodified && !it.is_unmodified_range()) {
				throw get_mapped_range_reads_your_writes();
			}
			if (it.is_unmodified_range() || (it.is_operation() && !it.is_independent())) {
				KeyRangeRef insert_range = KeyRangeRef(std::max(keys.begin, it.beginKey().toArenaOrRef(ryw->arena)),
				                                       std::min(keys.end, it.endKey().toArenaOrRef(ryw->arena)));
				if (!insert_range.empty()) {
					ryw->approximateSize += keys.expectedSize() + sizeof(KeyRangeRef);
					ryw->readConflicts.insert(insert_range, true);
				}
			}
		}
	}

	ACTOR template <class Req>
	static Future<typename Req::Result> readWithConflictRangeThrough(ReadYourWritesTransaction* ryw,
	                                                                 Req req,
	                                                                 Snapshot snapshot) {
		choose {
			when(typename Req::Result result = wait(readThrough(ryw, req, snapshot))) {
				return result;
			}
			when(wait(ryw->resetPromise.getFuture())) {
				throw internal_error();
			}
		}
	}
	ACTOR template <class Req>
	static Future<typename Req::Result> readWithConflictRangeSnapshot(ReadYourWritesTransaction* ryw, Req req) {
		state SnapshotCache::iterator it(&ryw->cache, &ryw->writes);
		choose {
			when(typename Req::Result result = wait(read(ryw, req, &it))) {
				return result;
			}
			when(wait(ryw->resetPromise.getFuture())) {
				throw internal_error();
			}
		}
	}
	ACTOR template <class Req>
	static Future<typename Req::Result> readWithConflictRangeRYW(ReadYourWritesTransaction* ryw,
	                                                             Req req,
	                                                             Snapshot snapshot) {
		state RYWIterator it(&ryw->cache, &ryw->writes);
		choose {
			when(typename Req::Result result = wait(read(ryw, req, &it))) {
				// Some overloads of addConflictRange() require it to point to the "right" key and others don't.  The
				// corresponding overloads of read() have to provide that guarantee!
				if (!snapshot)
					addConflictRange(ryw, req, it.extractWriteMapIterator(), result);
				return result;
			}
			when(wait(ryw->resetPromise.getFuture())) {
				throw internal_error();
			}
		}
	}
	template <class Req>
	static inline Future<typename Req::Result> readWithConflictRange(ReadYourWritesTransaction* ryw,
	                                                                 Req const& req,
	                                                                 Snapshot snapshot) {
		if (ryw->options.readYourWritesDisabled) {
			return readWithConflictRangeThrough(ryw, req, snapshot);
		} else if (snapshot && ryw->options.snapshotRywEnabled <= 0) {
			return readWithConflictRangeSnapshot(ryw, req);
		}
		return readWithConflictRangeRYW(ryw, req, snapshot);
	}

	template <class Iter>
	static void resolveKeySelectorFromCache(KeySelector& key,
	                                        Iter& it,
	                                        KeyRef const& maxKey,
	                                        bool* readToBegin,
	                                        bool* readThroughEnd,
	                                        int* actualOffset) {
		// If the key indicated by `key` can be determined without reading unknown data from the snapshot, then
		// it.kv().key is the resolved key. If the indicated key is determined to be "off the beginning or end" of the
		// database, it points to the first or last segment in the DB,
		//   and key is an equivalent key selector relative to the beginning or end of the database.
		// Otherwise it points to an unknown segment, and key is an equivalent key selector whose base key is in or
		// adjoining the segment.

		key.removeOrEqual(key.arena());

		bool alreadyExhausted = key.offset == 1;

		it.skip(key.getKey()); // TODO: or precondition?

		if (key.offset <= 0 && it.beginKey() == key.getKey() && key.getKey() != allKeys.begin)
			--it;

		ExtStringRef keykey = key.getKey();
		bool keyNeedsCopy = false;

		// Invariant: it.beginKey() <= keykey && keykey <= it.endKey() && (key.isBackward() ? it.beginKey() != keykey :
		// it.endKey() != keykey) Maintaining this invariant, we transform the key selector toward firstGreaterOrEqual
		// form until we reach an unknown range or the result
		while (key.offset > 1 && !it.is_unreadable() && !it.is_unknown_range() && it.endKey() < maxKey) {
			if (it.is_kv())
				--key.offset;
			++it;
			keykey = it.beginKey();
			keyNeedsCopy = true;
		}
		while (key.offset < 1 && !it.is_unreadable() && !it.is_unknown_range() && it.beginKey() != allKeys.begin) {
			if (it.is_kv()) {
				++key.offset;
				if (key.offset == 1) {
					keykey = it.beginKey();
					keyNeedsCopy = true;
					break;
				}
			}
			--it;
			keykey = it.endKey();
			keyNeedsCopy = true;
		}

		if (!alreadyExhausted) {
			*actualOffset = key.offset;
		}

		if (!it.is_unreadable() && !it.is_unknown_range() && key.offset < 1) {
			*readToBegin = true;
			key.setKey(allKeys.begin);
			key.offset = 1;
			return;
		}

		if (!it.is_unreadable() && !it.is_unknown_range() && key.offset > 1) {
			*readThroughEnd = true;
			key.setKey(maxKey); // maxKey is a KeyRef, but points to a literal. TODO: how can we ASSERT this?
			key.offset = 1;
			return;
		}

		while (!it.is_unreadable() && it.is_empty_range() && it.endKey() < maxKey) {
			++it;
			keykey = it.beginKey();
			keyNeedsCopy = true;
		}

		if (keyNeedsCopy) {
			key.setKey(keykey.toArena(key.arena()));
		}
	}

	static KeyRangeRef getKnownKeyRange(RangeResultRef data, KeySelector begin, KeySelector end, Arena& arena) {
		StringRef beginKey = begin.offset <= 1 ? begin.getKey() : allKeys.end;
		ExtStringRef endKey = !data.more && end.offset >= 1 ? end.getKey() : allKeys.begin;

		if (data.readToBegin)
			beginKey = allKeys.begin;
		if (data.readThroughEnd)
			endKey = allKeys.end;

		if (data.size()) {
			beginKey = std::min(beginKey, data[0].key);
			if (data.readThrough.present()) {
				endKey = std::max<ExtStringRef>(endKey, data.readThrough.get());
			} else {
				endKey = !data.more && data.end()[-1].key < endKey ? endKey : ExtStringRef(data.end()[-1].key, 1);
			}
		}
		if (beginKey >= endKey)
			return KeyRangeRef();

		return KeyRangeRef(StringRef(arena, beginKey), endKey.toArena(arena));
	}

	// Pre: it points to an unknown range
	// Increments it to point to the unknown range just before the next nontrivial known range (skips over trivial known
	// ranges), but not more than iterationLimit ranges away
	template <class Iter>
	static int skipUncached(Iter& it, Iter const& end, int iterationLimit) {
		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;

		ASSERT(!it.is_unreadable() && it.is_unknown_range());

		// b is the beginning of the most recent contiguous *empty* range
		// e is it.endKey()
		while (it != end && --iterationLimit >= 0) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { // Assumes no degenerate ranges
					while (it.is_unreadable() || !it.is_unknown_range())
						--it;
					return singleEmpty;
				}
				singleEmpty++;
			} else
				b = e;
			++it;
			e = it.endKey();
		}
		while (it.is_unreadable() || !it.is_unknown_range())
			--it;
		return singleEmpty;
	}

	// Pre: it points to an unknown range
	// Returns the number of following empty single-key known ranges between it and the next nontrivial known range, but
	// no more than maxClears Leaves `it` in an indeterminate state
	template <class Iter>
	static int countUncached(Iter&& it, KeyRef maxKey, int maxClears) {
		if (maxClears <= 0)
			return 0;

		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;

		while (e < maxKey) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { // Assumes no degenerate ranges
					return singleEmpty;
				}
				singleEmpty++;
				if (singleEmpty >= maxClears)
					return maxClears;
			} else
				b = e;
			++it;
			e = it.endKey();
		}
		return singleEmpty;
	}

	static void setRequestLimits(GetRangeLimits& requestLimit, int64_t additionalRows, int offset, int requestCount) {
		requestLimit.minRows =
		    (int)std::min(std::max(1 + additionalRows, (int64_t)offset), (int64_t)std::numeric_limits<int>::max());
		if (requestLimit.hasRowLimit()) {
			requestLimit.rows =
			    (int)std::min(std::max(std::max(1, requestLimit.rows) + additionalRows, (int64_t)offset),
			                  (int64_t)std::numeric_limits<int>::max());
		}

		// Calculating request byte limit
		if (requestLimit.bytes == 0) {
			requestLimit.bytes = GetRangeLimits::BYTE_LIMIT_UNLIMITED;
			if (!requestLimit.hasRowLimit()) {
				requestLimit.rows =
				    (int)std::min(std::max(std::max(1, requestLimit.rows) + additionalRows, (int64_t)offset),
				                  (int64_t)std::numeric_limits<int>::max());
			}
		} else if (requestLimit.hasByteLimit()) {
			requestLimit.bytes = std::min(int64_t(requestLimit.bytes) << std::min(requestCount, 20),
			                              (int64_t)CLIENT_KNOBS->REPLY_BYTE_LIMIT);
		}
	}

	// TODO: read to begin, read through end flags for result
	ACTOR template <class Iter>
	static Future<RangeResult> getRangeValue(ReadYourWritesTransaction* ryw,
	                                         KeySelector begin,
	                                         KeySelector end,
	                                         GetRangeLimits limits,
	                                         Iter* pit) {
		state Iter& it(*pit);
		state Iter itEnd(*pit);
		state RangeResult result;
		state int64_t additionalRows = 0;
		state int itemsPastEnd = 0;
		state int requestCount = 0;
		state bool readToBegin = false;
		state bool readThroughEnd = false;
		state int actualBeginOffset = begin.offset;
		state int actualEndOffset = end.offset;
		// state UID randomID = nondeterministicRandom()->randomUniqueID();

		resolveKeySelectorFromCache(begin, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset);
		resolveKeySelectorFromCache(end, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset);

		if (actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
			return RangeResultRef(false, false);
		} else if ((begin.isFirstGreaterOrEqual() && begin.getKey() == ryw->getMaxReadKey()) ||
		           (end.isFirstGreaterOrEqual() && end.getKey() == allKeys.begin)) {
			return RangeResultRef(readToBegin, readThroughEnd);
		}

		if (!end.isFirstGreaterOrEqual() && begin.getKey() > end.getKey()) {
			Key resolvedEnd = wait(read(ryw, GetKeyReq(end), pit));
			if (resolvedEnd == allKeys.begin)
				readToBegin = true;
			if (resolvedEnd == ryw->getMaxReadKey())
				readThroughEnd = true;

			if (begin.getKey() >= resolvedEnd && !begin.isBackward()) {
				return RangeResultRef(false, false);
			} else if (resolvedEnd == allKeys.begin) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			resolveKeySelectorFromCache(
			    begin, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset);
			resolveKeySelectorFromCache(
			    end, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset);
		}

		//TraceEvent("RYWSelectorsStartForward", randomID).detail("ByteLimit", limits.bytes).detail("RowLimit", limits.rows);

		loop {
			/*TraceEvent("RYWSelectors", randomID).detail("Begin", begin.toString())
			    .detail("End", end.toString())
			    .detail("Reached", limits.isReached())
			    .detail("ItemsPastEnd", itemsPastEnd)
			    .detail("EndOffset", -end.offset)
			    .detail("ItBegin", it.beginKey())
			    .detail("ItEnd", itEnd.beginKey())
			    .detail("Unknown", it.is_unknown_range())
			    .detail("Requests", requestCount);*/

			if (!result.size() && actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
				return RangeResultRef(false, false);
			}

			if (end.offset <= 1 && end.getKey() == allKeys.begin) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if ((begin.offset >= end.offset && begin.getKey() >= end.getKey()) ||
			    (begin.offset >= 1 && begin.getKey() >= ryw->getMaxReadKey())) {
				if (end.isFirstGreaterOrEqual())
					break;
				if (!result.size())
					break;
				Key resolvedEnd =
				    wait(read(ryw,
				              GetKeyReq(end),
				              pit)); // do not worry about iterator invalidation, because we are breaking for the loop
				if (resolvedEnd == allKeys.begin)
					readToBegin = true;
				if (resolvedEnd == ryw->getMaxReadKey())
					readThroughEnd = true;
				end = firstGreaterOrEqual(resolvedEnd);
				break;
			}

			if (!it.is_unreadable() && !it.is_unknown_range() && it.beginKey() > itEnd.beginKey()) {
				if (end.isFirstGreaterOrEqual())
					break;
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if (limits.isReached() && itemsPastEnd >= 1 - end.offset)
				break;

			if (it == itEnd && ((!it.is_unreadable() && !it.is_unknown_range()) ||
			                    (begin.offset > 0 && end.isFirstGreaterOrEqual() && end.getKey() == it.beginKey())))
				break;

			if (it.is_unknown_range()) {
				if (limits.hasByteLimit() && limits.hasSatisfiedMinRows() && result.size() &&
				    itemsPastEnd >= 1 - end.offset) {
					result.more = true;
					break;
				}

				Iter ucEnd(it);
				int singleClears = 0;
				int clearLimit = requestCount ? 1 << std::min(requestCount, 20) : 0;
				if (it.beginKey() < itEnd.beginKey())
					singleClears = std::min(skipUncached(ucEnd, itEnd, BUGGIFY ? 0 : clearLimit + 100), clearLimit);

				state KeySelector read_end;
				if (ucEnd != itEnd) {
					Key k = ucEnd.endKey().toStandaloneStringRef();
					read_end = KeySelector(firstGreaterOrEqual(k), k.arena());
					if (end.offset < 1)
						additionalRows += 1 - end.offset; // extra for items past end
				} else if (end.offset < 1) {
					read_end = KeySelector(firstGreaterOrEqual(end.getKey()), end.arena());
					additionalRows += 1 - end.offset;
				} else {
					read_end = end;
					if (end.offset > 1) {
						singleClears +=
						    countUncached(std::move(ucEnd), ryw->getMaxReadKey(), clearLimit - singleClears);
						read_end.offset += singleClears;
					}
				}

				additionalRows += singleClears;

				state KeySelector read_begin;
				if (begin.isFirstGreaterOrEqual()) {
					Key k = it.beginKey() > begin.getKey() ? it.beginKey().toStandaloneStringRef()
					                                       : Key(begin.getKey(), begin.arena());
					begin = KeySelector(firstGreaterOrEqual(k), k.arena());
					read_begin = begin;
				} else if (begin.offset > 1) {
					read_begin = KeySelector(firstGreaterOrEqual(begin.getKey()), begin.arena());
					additionalRows += begin.offset - 1;
				} else {
					read_begin = begin;
					ucEnd = it;

					singleClears = countUncachedBack(std::move(ucEnd), clearLimit);
					read_begin.offset -= singleClears;
					additionalRows += singleClears;
				}

				if (read_end.getKey() < read_begin.getKey()) {
					read_end.setKey(read_begin.getKey());
					read_end.arena().dependsOn(read_begin.arena());
				}

				state GetRangeLimits requestLimit = limits;
				setRequestLimits(requestLimit, additionalRows, 2 - read_begin.offset, requestCount);
				requestCount++;

				ASSERT(!requestLimit.hasRowLimit() || requestLimit.rows > 0);
				ASSERT(requestLimit.hasRowLimit() || requestLimit.hasByteLimit());

				//TraceEvent("RYWIssuing", randomID).detail("Begin", read_begin.toString()).detail("End", read_end.toString()).detail("Bytes", requestLimit.bytes).detail("Rows", requestLimit.rows).detail("Limits", limits.bytes).detail("Reached", limits.isReached()).detail("RequestCount", requestCount).detail("SingleClears", singleClears).detail("UcEnd", ucEnd.beginKey()).detail("MinRows", requestLimit.minRows);

				additionalRows = 0;
				RangeResult snapshot_read =
				    wait(ryw->tr.getRange(read_begin, read_end, requestLimit, Snapshot::True, Reverse::False));
				KeyRangeRef range = getKnownKeyRange(snapshot_read, read_begin, read_end, ryw->arena);

				//TraceEvent("RYWCacheInsert", randomID).detail("Range", range).detail("ExpectedSize", snapshot_read.expectedSize()).detail("Rows", snapshot_read.size()).detail("Results", snapshot_read).detail("More", snapshot_read.more).detail("ReadToBegin", snapshot_read.readToBegin).detail("ReadThroughEnd", snapshot_read.readThroughEnd).detail("ReadThrough", snapshot_read.readThrough);

				if (ryw->cache.insert(range, snapshot_read))
					ryw->arena.dependsOn(snapshot_read.arena());

				// TODO: Is there a more efficient way to deal with invalidation?
				resolveKeySelectorFromCache(
				    begin, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset);
				resolveKeySelectorFromCache(
				    end, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset);
			} else if (it.is_kv()) {
				KeyValueRef const* start = it.kv(ryw->arena);
				if (start == nullptr) {
					++it;
					continue;
				}
				it.skipContiguous(end.isFirstGreaterOrEqual()
				                      ? end.getKey()
				                      : ryw->getMaxReadKey()); // not technically correct since this would add
				                                               // end.getKey(), but that is protected above

				int maxCount = it.kv(ryw->arena) - start + 1;
				int count = 0;
				for (; count < maxCount && !limits.isReached(); count++) {
					limits.decrement(start[count]);
				}

				itemsPastEnd += maxCount - count;

				//TraceEvent("RYWaddKV", randomID).detail("Key", it.beginKey()).detail("Count", count).detail("MaxCount", maxCount).detail("ItemsPastEnd", itemsPastEnd);
				if (count)
					result.append(result.arena(), start, count);
				++it;
			} else
				++it;
		}

		result.more = result.more || limits.isReached();

		if (end.isFirstGreaterOrEqual()) {
			int keepItems = std::lower_bound(result.begin(), result.end(), end.getKey(), KeyValueRef::OrderByKey()) -
			                result.begin();
			if (keepItems < result.size())
				result.more = false;
			result.resize(result.arena(), keepItems);
		}

		result.readToBegin = readToBegin;
		result.readThroughEnd = !result.more && readThroughEnd;
		result.arena().dependsOn(ryw->arena);

		return result;
	}

	static KeyRangeRef getKnownKeyRangeBack(RangeResultRef data, KeySelector begin, KeySelector end, Arena& arena) {
		StringRef beginKey = !data.more && begin.offset <= 1 ? begin.getKey() : allKeys.end;
		ExtStringRef endKey = end.offset >= 1 ? end.getKey() : allKeys.begin;

		if (data.readToBegin)
			beginKey = allKeys.begin;
		if (data.readThroughEnd)
			endKey = allKeys.end;

		if (data.size()) {
			if (data.readThrough.present()) {
				beginKey = std::min(data.readThrough.get(), beginKey);
			} else {
				beginKey = !data.more && data.end()[-1].key > beginKey ? beginKey : data.end()[-1].key;
			}

			endKey = data[0].key < endKey ? endKey : ExtStringRef(data[0].key, 1);
		}
		if (beginKey >= endKey)
			return KeyRangeRef();

		return KeyRangeRef(StringRef(arena, beginKey), endKey.toArena(arena));
	}

	// Pre: it points to an unknown range
	// Decrements it to point to the unknown range just before the last nontrivial known range (skips over trivial known
	// ranges), but not more than iterationLimit ranges away Returns the number of single-key empty ranges skipped
	template <class Iter>
	static int skipUncachedBack(Iter& it, Iter const& end, int iterationLimit) {
		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;
		ASSERT(!it.is_unreadable() && it.is_unknown_range());

		// b == it.beginKey()
		// e is the end of the contiguous empty range containing it
		while (it != end && --iterationLimit >= 0) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { // Assumes no degenerate ranges
					while (it.is_unreadable() || !it.is_unknown_range())
						++it;
					return singleEmpty;
				}
				singleEmpty++;
			} else
				e = b;
			--it;
			b = it.beginKey();
		}
		while (it.is_unreadable() || !it.is_unknown_range())
			++it;
		return singleEmpty;
	}

	// Pre: it points to an unknown range
	// Returns the number of preceding empty single-key known ranges between it and the previous nontrivial known range,
	// but no more than maxClears Leaves it in an indeterminate state
	template <class Iter>
	static int countUncachedBack(Iter&& it, int maxClears) {
		if (maxClears <= 0)
			return 0;
		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;
		while (b > allKeys.begin) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { // Assumes no degenerate ranges
					return singleEmpty;
				}
				singleEmpty++;
				if (singleEmpty >= maxClears)
					return maxClears;
			} else
				e = b;
			--it;
			b = it.beginKey();
		}
		return singleEmpty;
	}

	ACTOR template <class Iter>
	static Future<RangeResult> getRangeValueBack(ReadYourWritesTransaction* ryw,
	                                             KeySelector begin,
	                                             KeySelector end,
	                                             GetRangeLimits limits,
	                                             Iter* pit) {
		state Iter& it(*pit);
		state Iter itEnd(*pit);
		state RangeResult result;
		state int64_t additionalRows = 0;
		state int itemsPastBegin = 0;
		state int requestCount = 0;
		state bool readToBegin = false;
		state bool readThroughEnd = false;
		state int actualBeginOffset = begin.offset;
		state int actualEndOffset = end.offset;
		// state UID randomID = nondeterministicRandom()->randomUniqueID();

		resolveKeySelectorFromCache(end, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset);
		resolveKeySelectorFromCache(
		    begin, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset);

		if (actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
			return RangeResultRef(false, false);
		} else if ((begin.isFirstGreaterOrEqual() && begin.getKey() == ryw->getMaxReadKey()) ||
		           (end.isFirstGreaterOrEqual() && end.getKey() == allKeys.begin)) {
			return RangeResultRef(readToBegin, readThroughEnd);
		}

		if (!begin.isFirstGreaterOrEqual() && begin.getKey() > end.getKey()) {
			Key resolvedBegin = wait(read(ryw, GetKeyReq(begin), pit));
			if (resolvedBegin == allKeys.begin)
				readToBegin = true;
			if (resolvedBegin == ryw->getMaxReadKey())
				readThroughEnd = true;

			if (resolvedBegin >= end.getKey() && end.offset <= 1) {
				return RangeResultRef(false, false);
			} else if (resolvedBegin == ryw->getMaxReadKey()) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			resolveKeySelectorFromCache(end, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset);
			resolveKeySelectorFromCache(
			    begin, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset);
		}

		//TraceEvent("RYWSelectorsStartReverse", randomID).detail("ByteLimit", limits.bytes).detail("RowLimit", limits.rows);

		loop {
			/*TraceEvent("RYWSelectors", randomID).detail("Begin", begin.toString())
			    .detail("End", end.toString())
			    .detail("Reached", limits.isReached())
			    .detail("ItemsPastBegin", itemsPastBegin)
			    .detail("EndOffset", end.offset)
			    .detail("ItBegin", it.beginKey())
			    .detail("ItEnd", itEnd.beginKey())
			    .detail("Unknown", it.is_unknown_range())
			    .detail("Kv", it.is_kv())
			    .detail("Requests", requestCount);*/

			if (!result.size() && actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
				return RangeResultRef(false, false);
			}

			if (!begin.isBackward() && begin.getKey() >= ryw->getMaxReadKey()) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if ((begin.offset >= end.offset && begin.getKey() >= end.getKey()) ||
			    (end.offset <= 1 && end.getKey() == allKeys.begin)) {
				if (begin.isFirstGreaterOrEqual())
					break;
				if (!result.size())
					break;
				Key resolvedBegin =
				    wait(read(ryw,
				              GetKeyReq(begin),
				              pit)); // do not worry about iterator invalidation, because we are breaking for the loop
				if (resolvedBegin == allKeys.begin)
					readToBegin = true;
				if (resolvedBegin == ryw->getMaxReadKey())
					readThroughEnd = true;
				begin = firstGreaterOrEqual(resolvedBegin);
				break;
			}

			if (itemsPastBegin >= begin.offset - 1 && !it.is_unreadable() && !it.is_unknown_range() &&
			    it.beginKey() < itEnd.beginKey()) {
				if (begin.isFirstGreaterOrEqual())
					break;
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if (limits.isReached() && itemsPastBegin >= begin.offset - 1)
				break;

			if (end.isFirstGreaterOrEqual() && end.getKey() == it.beginKey()) {
				if (itemsPastBegin >= begin.offset - 1 && it == itEnd)
					break;
				--it;
			}

			if (it.is_unknown_range()) {
				if (limits.hasByteLimit() && result.size() && itemsPastBegin >= begin.offset - 1) {
					result.more = true;
					break;
				}

				Iter ucEnd(it);
				int singleClears = 0;
				int clearLimit = requestCount ? 1 << std::min(requestCount, 20) : 0;
				if (it.beginKey() > itEnd.beginKey())
					singleClears = std::min(skipUncachedBack(ucEnd, itEnd, BUGGIFY ? 0 : clearLimit + 100), clearLimit);

				state KeySelector read_begin;
				if (ucEnd != itEnd) {
					Key k = ucEnd.beginKey().toStandaloneStringRef();
					read_begin = KeySelector(firstGreaterOrEqual(k), k.arena());
					if (begin.offset > 1)
						additionalRows += begin.offset - 1; // extra for items past end
				} else if (begin.offset > 1) {
					read_begin = KeySelector(firstGreaterOrEqual(begin.getKey()), begin.arena());
					additionalRows += begin.offset - 1;
				} else {
					read_begin = begin;
					if (begin.offset < 1) {
						singleClears += countUncachedBack(std::move(ucEnd), clearLimit - singleClears);
						read_begin.offset -= singleClears;
					}
				}

				additionalRows += singleClears;

				state KeySelector read_end;
				if (end.isFirstGreaterOrEqual()) {
					Key k = it.endKey() < end.getKey() ? it.endKey().toStandaloneStringRef() : end.getKey();
					end = KeySelector(firstGreaterOrEqual(k), k.arena());
					read_end = end;
				} else if (end.offset < 1) {
					read_end = KeySelector(firstGreaterOrEqual(end.getKey()), end.arena());
					additionalRows += 1 - end.offset;
				} else {
					read_end = end;
					ucEnd = it;

					singleClears = countUncached(std::move(ucEnd), ryw->getMaxReadKey(), clearLimit);
					read_end.offset += singleClears;
					additionalRows += singleClears;
				}

				if (read_begin.getKey() > read_end.getKey()) {
					read_begin.setKey(read_end.getKey());
					read_begin.arena().dependsOn(read_end.arena());
				}

				state GetRangeLimits requestLimit = limits;
				setRequestLimits(requestLimit, additionalRows, read_end.offset, requestCount);
				requestCount++;

				ASSERT(!requestLimit.hasRowLimit() || requestLimit.rows > 0);
				ASSERT(requestLimit.hasRowLimit() || requestLimit.hasByteLimit());

				//TraceEvent("RYWIssuing", randomID).detail("Begin", read_begin.toString()).detail("End", read_end.toString()).detail("Bytes", requestLimit.bytes).detail("Rows", requestLimit.rows).detail("Limits", limits.bytes).detail("Reached", limits.isReached()).detail("RequestCount", requestCount).detail("SingleClears", singleClears).detail("UcEnd", ucEnd.beginKey()).detail("MinRows", requestLimit.minRows);

				additionalRows = 0;
				RangeResult snapshot_read =
				    wait(ryw->tr.getRange(read_begin, read_end, requestLimit, Snapshot::True, Reverse::True));
				KeyRangeRef range = getKnownKeyRangeBack(snapshot_read, read_begin, read_end, ryw->arena);

				//TraceEvent("RYWCacheInsert", randomID).detail("Range", range).detail("ExpectedSize", snapshot_read.expectedSize()).detail("Rows", snapshot_read.size()).detail("Results", snapshot_read).detail("More", snapshot_read.more).detail("ReadToBegin", snapshot_read.readToBegin).detail("ReadThroughEnd", snapshot_read.readThroughEnd).detail("ReadThrough", snapshot_read.readThrough);

				RangeResultRef reversed;
				reversed.resize(ryw->arena, snapshot_read.size());
				for (int i = 0; i < snapshot_read.size(); i++) {
					reversed[snapshot_read.size() - i - 1] = snapshot_read[i];
				}

				if (ryw->cache.insert(range, reversed))
					ryw->arena.dependsOn(snapshot_read.arena());

				// TODO: Is there a more efficient way to deal with invalidation?
				resolveKeySelectorFromCache(
				    end, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset);
				resolveKeySelectorFromCache(
				    begin, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset);
			} else {
				KeyValueRef const* end = it.is_kv() ? it.kv(ryw->arena) : nullptr;
				if (end != nullptr) {
					it.skipContiguousBack(begin.isFirstGreaterOrEqual() ? begin.getKey() : allKeys.begin);
					KeyValueRef const* start = it.kv(ryw->arena);
					ASSERT(start != nullptr);

					int maxCount = end - start + 1;
					int count = 0;
					for (; count < maxCount && !limits.isReached(); count++) {
						limits.decrement(start[maxCount - count - 1]);
					}

					itemsPastBegin += maxCount - count;
					//TraceEvent("RYWaddKV", randomID).detail("Key", it.beginKey()).detail("Count", count).detail("MaxCount", maxCount).detail("ItemsPastBegin", itemsPastBegin);
					if (count) {
						int size = result.size();
						result.resize(result.arena(), size + count);
						for (int i = 0; i < count; i++) {
							result[size + i] = start[maxCount - i - 1];
						}
					}
				}
				if (it == itEnd)
					break;
				--it;
			}
		}

		result.more = result.more || limits.isReached();

		if (begin.isFirstGreaterOrEqual()) {
			int keepItems = result.rend() -
			                std::lower_bound(result.rbegin(), result.rend(), begin.getKey(), KeyValueRef::OrderByKey());
			if (keepItems < result.size())
				result.more = false;

			result.resize(result.arena(), keepItems);
		}

		result.readToBegin = !result.more && readToBegin;
		result.readThroughEnd = readThroughEnd;
		result.arena().dependsOn(ryw->arena);

		return result;
	}

#ifndef __INTEL_COMPILER
#pragma region GetMappedRange
#endif

	template <class Iter>
	static Future<MappedRangeResult> read(ReadYourWritesTransaction* ryw, GetMappedRangeReq<false> read, Iter* it) {
		return getMappedRangeValue(ryw, read.begin, read.end, read.mapper, read.limits, it);
	};

	template <class Iter>
	static Future<MappedRangeResult> read(ReadYourWritesTransaction* ryw, GetMappedRangeReq<true> read, Iter* it) {
		throw unsupported_operation();
		// TODO: Support reverse. return getMappedRangeValueBack(ryw, read.begin, read.end, read.mapper,
		// read.limits, it);
	};

	ACTOR template <bool backwards>
	static Future<MappedRangeResult> readThrough(ReadYourWritesTransaction* ryw,
	                                             GetMappedRangeReq<backwards> read,
	                                             Snapshot snapshot) {
		if (backwards && read.end.offset > 1) {
			// FIXME: Optimistically assume that this will not run into the system keys, and only reissue if the result
			// actually does.
			Key key = wait(ryw->tr.getKey(read.end, snapshot));
			if (key > ryw->getMaxReadKey())
				read.end = firstGreaterOrEqual(ryw->getMaxReadKey());
			else
				read.end = KeySelector(firstGreaterOrEqual(key), key.arena());
		}
		MappedRangeResult v = wait(ryw->tr.getMappedRange(
		    read.begin, read.end, read.mapper, read.limits, snapshot, backwards ? Reverse::True : Reverse::False));
		return v;
	}

	template <bool backwards>
	static void addConflictRangeAndMustUnmodified(ReadYourWritesTransaction* ryw,
	                                              GetMappedRangeReq<backwards> read,
	                                              WriteMap::iterator& it,
	                                              MappedRangeResult result) {
		// Primary getRange.
		addConflictRange<true, MappedRangeResult>(
		    ryw, GetRangeReq<backwards>(read.begin, read.end, read.limits), it, result);

		// Secondary getValue/getRanges.
		for (const auto& mappedKeyValue : result) {
			const auto& reqAndResult = mappedKeyValue.reqAndResult;
			if (std::holds_alternative<GetValueReqAndResultRef>(reqAndResult)) {
				auto getValue = std::get<GetValueReqAndResultRef>(reqAndResult);
				// GetValueReq variation of addConflictRange require it to point at the right segment.
				it.skip(getValue.key);
				// The result is not used in GetValueReq variation of addConflictRange. Let's just pass in a
				// placeholder.
				addConflictRange<true>(ryw, GetValueReq(getValue.key), it, Optional<Value>());
			} else if (std::holds_alternative<GetRangeReqAndResultRef>(reqAndResult)) {
				auto getRange = std::get<GetRangeReqAndResultRef>(reqAndResult);
				// We only support forward scan for secondary getRange requests.
				// The limits are not used in addConflictRange. Let's just pass in a placeholder.
				addConflictRange<true>(
				    ryw, GetRangeReq<false>(getRange.begin, getRange.end, GetRangeLimits()), it, getRange.result);
			} else {
				throw internal_error();
			}
		}
	}

	// For Snapshot::True and NOT readYourWritesDisabled.
	ACTOR template <bool backwards>
	static Future<MappedRangeResult> readWithConflictRangeRYW(ReadYourWritesTransaction* ryw,
	                                                          GetMappedRangeReq<backwards> req,
	                                                          Snapshot snapshot) {
		choose {
			when(MappedRangeResult result = wait(readThrough(ryw, req, Snapshot::True))) {
				// Insert read conflicts (so that it supported Snapshot::True) and check it is not modified (so it masks
				// sure not break RYW semantic while not implementing RYW) for both the primary getRange and all
				// underlying getValue/getRanges.
				WriteMap::iterator writes(&ryw->writes);
				addConflictRangeAndMustUnmodified<backwards>(ryw, req, writes, result);
				return result;
			}
			when(wait(ryw->resetPromise.getFuture())) {
				throw internal_error();
			}
		}
	}

	template <bool backwards>
	static inline Future<MappedRangeResult> readWithConflictRangeForGetMappedRange(
	    ReadYourWritesTransaction* ryw,
	    GetMappedRangeReq<backwards> const& req,
	    Snapshot snapshot) {
		// For now, getMappedRange requires serializable isolation. (Technically it is trivial to add snapshot
		// isolation support. But it is not default and is rarely used. So we disallow it until we have thorough test
		// coverage for it.)
		if (snapshot) {
			CODE_PROBE(true, "getMappedRange not supported for snapshot.", probe::decoration::rare);
			throw unsupported_operation();
		}
		// For now, getMappedRange requires read-your-writes being NOT disabled. But the support of RYW is limited
		// to throwing get_mapped_range_reads_your_writes error when getMappedRange actually reads your own writes.
		// Applications should fall back in their own ways. This is different from what is usually expected from RYW,
		// which returns the written value transparently. In another word, it makes sure not break RYW semantics without
		// actually implementing reading from the writes.
		if (ryw->options.readYourWritesDisabled) {
			CODE_PROBE(true, "getMappedRange not supported for read-your-writes disabled.", probe::decoration::rare);
			throw unsupported_operation();
		}

		return readWithConflictRangeRYW(ryw, req, snapshot);
	}

#ifndef __INTEL_COMPILER
#pragma endregion
#endif

	static void triggerWatches(ReadYourWritesTransaction* ryw,
	                           KeyRangeRef range,
	                           Optional<ValueRef> val,
	                           bool valueKnown = true) {
		for (auto it = ryw->watchMap.lower_bound(range.begin); it != ryw->watchMap.end() && it->key < range.end;) {
			auto itCopy = it;
			++it;

			ASSERT(itCopy->value.size());
			CODE_PROBE(itCopy->value.size() > 1, "Multiple watches on the same key triggered by RYOW");

			for (int i = 0; i < itCopy->value.size(); i++) {
				if (itCopy->value[i]->onChangeTrigger.isSet()) {
					swapAndPop(&itCopy->value, i--);
				} else if (!valueKnown ||
				           (itCopy->value[i]->setPresent &&
				            (itCopy->value[i]->setValue.present() != val.present() ||
				             (val.present() && itCopy->value[i]->setValue.get() != val.get()))) ||
				           (itCopy->value[i]->valuePresent &&
				            (itCopy->value[i]->value.present() != val.present() ||
				             (val.present() && itCopy->value[i]->value.get() != val.get())))) {
					itCopy->value[i]->onChangeTrigger.send(Void());
					swapAndPop(&itCopy->value, i--);
				} else {
					itCopy->value[i]->setPresent = true;
					itCopy->value[i]->setValue = val.castTo<Value>();
				}
			}

			if (itCopy->value.size() == 0)
				ryw->watchMap.erase(itCopy);
		}
	}

	static void triggerWatches(ReadYourWritesTransaction* ryw,
	                           KeyRef key,
	                           Optional<ValueRef> val,
	                           bool valueKnown = true) {
		triggerWatches(ryw, singleKeyRange(key), val, valueKnown);
	}

	ACTOR static Future<Void> watch(ReadYourWritesTransaction* ryw, Key key) {
		state Future<Optional<Value>> val;
		state Future<Void> watchFuture;
		state Reference<Watch> watch(new Watch(key));
		state Promise<Void> done;

		ryw->reading.add(done.getFuture());

		if (!ryw->options.readYourWritesDisabled) {
			ryw->watchMap[key].push_back(watch);
			val = readWithConflictRange(ryw, GetValueReq(key), Snapshot::False);
		} else {
			ryw->approximateSize += 2 * key.expectedSize() + 1;
			val = ryw->tr.get(key);
		}

		try {
			wait(ryw->resetPromise.getFuture() || success(val) || watch->onChangeTrigger.getFuture());
		} catch (Error& e) {
			done.send(Void());
			throw;
		}

		if (watch->onChangeTrigger.getFuture().isReady()) {
			done.send(Void());
			if (watch->onChangeTrigger.getFuture().isError())
				throw watch->onChangeTrigger.getFuture().getError();
			return Void();
		}

		watch->valuePresent = true;
		watch->value = val.get();

		if (watch->setPresent && (watch->setValue.present() != watch->value.present() ||
		                          (watch->value.present() && watch->setValue.get() != watch->value.get()))) {
			watch->onChangeTrigger.send(Void());
			done.send(Void());
			return Void();
		}

		try {
			watchFuture = ryw->tr.watch(watch); // throws if there are too many outstanding watches
		} catch (Error& e) {
			done.send(Void());
			throw;
		}
		done.send(Void());

		wait(watchFuture);

		return Void();
	}

	ACTOR static void simulateTimeoutInFlightCommit(ReadYourWritesTransaction* ryw_) {
		state Reference<ReadYourWritesTransaction> ryw = Reference<ReadYourWritesTransaction>::addRef(ryw_);
		ASSERT(ryw->options.timeoutInSeconds > 0);
		// An actual in-flight commit (i.e. one that's past the point where cancelling the transaction would stop it)
		// would already have a read version. We need to get a read version too, otherwise committing a conflicting
		// transaction may not ensure this transaction is no longer in-flight, since this transaction could get a read
		// version _after_.
		wait(success(ryw->getReadVersion()));
		if (!ryw->resetPromise.isSet())
			ryw->resetPromise.sendError(transaction_timed_out());
		wait(delay(deterministicRandom()->random01() * 5));
		TraceEvent("ClientBuggifyInFlightCommit").log();
		wait(ryw->tr.commit());
	}

	ACTOR static Future<Void> commit(ReadYourWritesTransaction* ryw) {
		try {
			ryw->commitStarted = true;

			if (ryw->options.specialKeySpaceChangeConfiguration)
				wait(ryw->getDatabase()->specialKeySpace->commit(ryw));

			Future<Void> ready = ryw->reading;
			wait(ryw->resetPromise.getFuture() || ready);

			if (ryw->options.readYourWritesDisabled) {

				// Stash away conflict ranges to read after commit
				ryw->nativeReadRanges = ryw->tr.readConflictRanges();
				ryw->nativeWriteRanges = ryw->tr.writeConflictRanges();
				for (const auto& f : ryw->tr.getExtraReadConflictRanges()) {
					if (f.isReady() && f.get().first < f.get().second)
						ryw->nativeReadRanges.push_back(
						    ryw->nativeReadRanges.arena(),
						    KeyRangeRef(f.get().first, f.get().second)
						        .withPrefix(readConflictRangeKeysRange.begin, ryw->nativeReadRanges.arena()));
				}

				if (ryw->resetPromise.isSet())
					throw ryw->resetPromise.getFuture().getError();
				if (CLIENT_BUGGIFY && ryw->options.timeoutInSeconds > 0) {
					simulateTimeoutInFlightCommit(ryw);
					throw transaction_timed_out();
				}
				wait(ryw->resetPromise.getFuture() || ryw->tr.commit());

				ryw->debugLogRetries();

				if (!ryw->tr.apiVersionAtLeast(410)) {
					ryw->reset();
				}

				return Void();
			}

			ryw->writeRangeToNativeTransaction(KeyRangeRef(StringRef(), allKeys.end));

			auto conflictRanges = ryw->readConflicts.ranges();
			for (auto iter = conflictRanges.begin(); iter != conflictRanges.end(); ++iter) {
				if (iter->value()) {
					ryw->tr.addReadConflictRange(iter->range());
				}
			}

			if (CLIENT_BUGGIFY && ryw->options.timeoutInSeconds > 0) {
				simulateTimeoutInFlightCommit(ryw);
				throw transaction_timed_out();
			}
			wait(ryw->resetPromise.getFuture() || ryw->tr.commit());

			ryw->debugLogRetries();
			if (!ryw->tr.apiVersionAtLeast(410)) {
				ryw->reset();
			}

			return Void();
		} catch (Error& e) {
			if (!ryw->tr.apiVersionAtLeast(410)) {
				ryw->commitStarted = false;
				if (!ryw->resetPromise.isSet()) {
					ryw->tr.reset();
					ryw->resetRyow();
				}
			}

			throw;
		}
	}

	// This function must not block unless a non-empty commitFuture is passed in
	// If commitFuture is specified, this will wait for the future and report the result of
	// the future in the output. If commitFuture isn't specified, then the transaction will
	// be reported uncommitted. In that case, an optional error can be provided to indicate
	// why the transaction was uncommitted.
	ACTOR static Future<Void> printDebugMessages(ReadYourWritesTransaction* self,
	                                             Optional<Future<Void>> commitFuture,
	                                             Optional<Error> error = Optional<Error>()) {
		state std::string prefix;
		state std::string commitResult;
		state ErrorOr<Void> result;
		state std::vector<BaseTraceEvent> debugTraces = std::move(self->debugTraces);
		state std::vector<std::string> debugMessages = std::move(self->debugMessages);

		self->debugTraces.clear();
		self->debugMessages.clear();

		if (commitFuture.present()) {
			try {
				wait(store(result, errorOr(commitFuture.get())));
			} catch (Error& e) {
				result = e;
			}
		}

		Version readVersion = self->getReadVersion().canGet() ? self->getReadVersion().get() : -1;
		Version commitVersion = result.present() ? self->getCommittedVersion() : -1;

		if (result.present()) {
			commitResult = "Committed";
		} else if (result.getError().code() == error_code_commit_unknown_result ||
		           result.getError().code() == error_code_operation_cancelled ||
		           result.getError().code() == error_code_transaction_timed_out) {
			commitResult = "Maybe committed";
		} else if (commitFuture.present()) {
			commitResult = "Not committed";
		} else {
			commitResult = "Uncommitted";
		}

		for (auto& event : debugTraces) {
			event.detail("CommitResult", commitResult).detail("ReadVersion", readVersion);

			if (result.present()) {
				event.detail("CommitVersion", commitVersion);
			} else if (commitFuture.present()) {
				event.errorUnsuppressed(result.getError());
			} else if (error.present()) {
				event.errorUnsuppressed(error.get());
			}

			event.log();
		}

		for (auto message : debugMessages) {
			std::string cvString = result.present() ? fmt::format(" cv={}", commitVersion) : "";
			std::string errorString;
			if (commitFuture.present() && result.isError()) {
				errorString = fmt::format(" error={}", result.getError().name());
			} else if (error.present()) {
				errorString = fmt::format(" error={}", error.get().name());
			}

			fmt::print("[{} rv={}{}{}] {}\n", commitResult, readVersion, cvString, errorString, message);
		}

		if (result.isError()) {
			throw result.getError();
		}

		return Void();
	}

	ACTOR static Future<Void> onError(ReadYourWritesTransaction* ryw, Error e) {
		if (ryw->debugTraces.size() > 0 || ryw->debugMessages.size() > 0) {
			// printDebugMessages returns a future but will not block if called with an empty second argument
			ASSERT(printDebugMessages(ryw, {}, e).isReady());
		}

		try {
			if (ryw->resetPromise.isSet()) {
				throw ryw->resetPromise.getFuture().getError();
			}

			bool retry_limit_hit = ryw->options.maxRetries != -1 && ryw->retries >= ryw->options.maxRetries;
			if (ryw->retries < std::numeric_limits<int>::max())
				ryw->retries++;
			if (retry_limit_hit) {
				throw e;
			}

			wait(ryw->resetPromise.getFuture() || ryw->tr.onError(e));

			ryw->debugLogRetries(e);

			ryw->resetRyow();
			return Void();
		} catch (Error& e) {
			if (!ryw->resetPromise.isSet()) {
				if (ryw->tr.apiVersionAtLeast(610)) {
					ryw->resetPromise.sendError(transaction_cancelled());
				} else {
					ryw->resetRyow();
				}
			}
			if (e.code() == error_code_broken_promise)
				throw transaction_cancelled();
			throw;
		}
	}

	ACTOR static Future<Version> getReadVersion(ReadYourWritesTransaction* ryw) {
		choose {
			when(Version v = wait(ryw->tr.getReadVersion())) {
				return v;
			}

			when(wait(ryw->resetPromise.getFuture())) {
				throw internal_error();
			}
		}
	}
};

ReadYourWritesTransaction::ReadYourWritesTransaction(Database const& cx, Optional<Reference<Tenant>> const& tenant)
  : ISingleThreadTransaction(cx->deferredError), tr(cx, tenant), cache(&arena), writes(&arena), retries(0),
    approximateSize(0), creationTime(now()), commitStarted(false), versionStampFuture(tr.getVersionstamp()),
    specialKeySpaceWriteMap(std::make_pair(false, Optional<Value>()), specialKeys.end), options(tr) {
	std::copy(
	    cx.getTransactionDefaults().begin(), cx.getTransactionDefaults().end(), std::back_inserter(persistentOptions));
	applyPersistentOptions();
}

void ReadYourWritesTransaction::construct(Database const& cx) {
	*this = ReadYourWritesTransaction(cx);
}

void ReadYourWritesTransaction::construct(Database const& cx, Reference<Tenant> const& tenant) {
	*this = ReadYourWritesTransaction(cx, tenant);
}

ACTOR Future<Void> timebomb(double endTime, Promise<Void> resetPromise) {
	while (now() < endTime) {
		wait(delayUntil(std::min(endTime + 0.0001, now() + CLIENT_KNOBS->TRANSACTION_TIMEOUT_DELAY_INTERVAL)));
	}
	if (!resetPromise.isSet())
		resetPromise.sendError(transaction_timed_out());
	throw transaction_timed_out();
}

void ReadYourWritesTransaction::resetTimeout() {
	timeoutActor =
	    options.timeoutInSeconds == 0.0 ? Void() : timebomb(options.timeoutInSeconds + creationTime, resetPromise);
}

Future<Version> ReadYourWritesTransaction::getReadVersion() {
	if (tr.apiVersionAtLeast(101)) {
		if (resetPromise.isSet())
			return resetPromise.getFuture().getError();
		return RYWImpl::getReadVersion(this);
	}
	return tr.getReadVersion();
}

Optional<Value> getValueFromJSON(StatusObject statusObj) {
	try {
		Value output =
		    StringRef(json_spirit::write_string(json_spirit::mValue(statusObj), json_spirit::Output_options::none));
		return output;
	} catch (std::exception& e) {
		TraceEvent(SevError, "UnableToUnparseStatusJSON").detail("What", e.what());
		throw internal_error();
	}
}

ACTOR Future<Optional<Value>> getJSON(Database db, std::string jsonField = "") {
	StatusObject statusObj = wait(StatusClient::statusFetcher(db, jsonField));
	return getValueFromJSON(statusObj);
}

ACTOR Future<RangeResult> getWorkerInterfaces(Reference<IClusterConnectionRecord> connRecord) {
	state Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface(new AsyncVar<Optional<ClusterInterface>>);
	state Future<Void> leaderMon = monitorLeader<ClusterInterface>(connRecord, clusterInterface);

	loop {
		choose {
			when(std::vector<ClientWorkerInterface> workers =
			         wait(clusterInterface->get().present()
			                  ? brokenPromiseToNever(
			                        clusterInterface->get().get().getClientWorkers.getReply(GetClientWorkersRequest()))
			                  : Never())) {
				RangeResult result;
				for (auto& it : workers) {
					result.push_back_deep(
					    result.arena(),
					    KeyValueRef(it.address().toString(), BinaryWriter::toValue(it, IncludeVersion())));
				}

				return result;
			}
			when(wait(clusterInterface->onChange())) {}
		}
	}
}

Future<Optional<Value>> ReadYourWritesTransaction::get(const Key& key, Snapshot snapshot) {
	CODE_PROBE(true, "ReadYourWritesTransaction::get");

	if (getDatabase()->apiVersionAtLeast(630)) {
		if (specialKeys.contains(key)) {
			CODE_PROBE(true, "Special keys get");
			return getDatabase()->specialKeySpace->get(this, key);
		}
	} else {
		if (key == "\xff\xff/status/json"_sr) {
			if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionRecord()) {
				++tr.getDatabase()->transactionStatusRequests;
				return getJSON(tr.getDatabase());
			} else {
				return Optional<Value>();
			}
		}

		if (key == "\xff\xff/cluster_file_path"_sr) {
			try {
				if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionRecord()) {
					Optional<Value> output = StringRef(tr.getDatabase()->getConnectionRecord()->getLocation());
					return output;
				}
			} catch (Error& e) {
				return e;
			}
			return Optional<Value>();
		}

		if (key == "\xff\xff/connection_string"_sr) {
			try {
				if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionRecord()) {
					Reference<IClusterConnectionRecord> f = tr.getDatabase()->getConnectionRecord();
					Optional<Value> output = StringRef(f->getConnectionString().toString());
					return output;
				}
			} catch (Error& e) {
				return e;
			}
			return Optional<Value>();
		}
	}

	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	if (key >= getMaxReadKey() && key != metadataVersionKey)
		return key_outside_legal_range();

	// There are no keys in the database with size greater than the max key size
	if (key.size() > getMaxReadKeySize(key)) {
		return Optional<Value>();
	}

	Future<Optional<Value>> result = RYWImpl::readWithConflictRange(this, RYWImpl::GetValueReq(key), snapshot);
	reading.add(success(result));
	return result;
}

Future<Key> ReadYourWritesTransaction::getKey(const KeySelector& key, Snapshot snapshot) {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	if (key.getKey() > getMaxReadKey())
		return key_outside_legal_range();

	Future<Key> result = RYWImpl::readWithConflictRange(this, RYWImpl::GetKeyReq(key), snapshot);
	reading.add(success(result));
	return result;
}

Future<RangeResult> ReadYourWritesTransaction::getRange(KeySelector begin,
                                                        KeySelector end,
                                                        GetRangeLimits limits,
                                                        Snapshot snapshot,
                                                        Reverse reverse) {
	if (getDatabase()->apiVersionAtLeast(630)) {
		if (specialKeys.contains(begin.getKey()) && specialKeys.begin <= end.getKey() &&
		    end.getKey() <= specialKeys.end) {
			CODE_PROBE(true, "Special key space get range");
			return getDatabase()->specialKeySpace->getRange(this, begin, end, limits, reverse);
		}
	} else {
		if (begin.getKey() == "\xff\xff/worker_interfaces"_sr) {
			if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionRecord()) {
				return getWorkerInterfaces(tr.getDatabase()->getConnectionRecord());
			} else {
				return RangeResult();
			}
		}
	}

	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	KeyRef maxKey = getMaxReadKey();
	if (begin.getKey() > maxKey || end.getKey() > maxKey)
		return key_outside_legal_range();

	// This optimization prevents nullptr operations from being added to the conflict range
	if (limits.isReached()) {
		CODE_PROBE(true, "RYW range read limit 0");
		return RangeResult();
	}

	if (!limits.isValid())
		return range_limits_invalid();

	if (begin.orEqual)
		begin.removeOrEqual(begin.arena());

	if (end.orEqual)
		end.removeOrEqual(end.arena());

	if (begin.offset >= end.offset && begin.getKey() >= end.getKey()) {
		CODE_PROBE(true, "RYW range inverted");
		return RangeResult();
	}

	Future<RangeResult> result =
	    reverse ? RYWImpl::readWithConflictRange(this, RYWImpl::GetRangeReq<true>(begin, end, limits), snapshot)
	            : RYWImpl::readWithConflictRange(this, RYWImpl::GetRangeReq<false>(begin, end, limits), snapshot);

	reading.add(success(result));
	return result;
}

Future<RangeResult> ReadYourWritesTransaction::getRange(const KeySelector& begin,
                                                        const KeySelector& end,
                                                        int limit,
                                                        Snapshot snapshot,
                                                        Reverse reverse) {
	return getRange(begin, end, GetRangeLimits(limit), snapshot, reverse);
}

Future<MappedRangeResult> ReadYourWritesTransaction::getMappedRange(KeySelector begin,
                                                                    KeySelector end,
                                                                    Key mapper,
                                                                    GetRangeLimits limits,
                                                                    Snapshot snapshot,
                                                                    Reverse reverse) {
	if (getDatabase()->apiVersionAtLeast(630)) {
		if (specialKeys.contains(begin.getKey()) && specialKeys.begin <= end.getKey() &&
		    end.getKey() <= specialKeys.end) {
			CODE_PROBE(true, "Special key space get range (getMappedRange)", probe::decoration::rare);
			throw client_invalid_operation(); // Not support special keys.
		}
	} else {
		if (begin.getKey() == "\xff\xff/worker_interfaces"_sr) {
			throw client_invalid_operation(); // Not support special keys.
		}
	}

	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	KeyRef maxKey = getMaxReadKey();
	if (begin.getKey() > maxKey || end.getKey() > maxKey)
		return key_outside_legal_range();

	// This optimization prevents nullptr operations from being added to the conflict range
	if (limits.isReached()) {
		CODE_PROBE(true, "RYW range read limit 0 (getMappedRange)", probe::decoration::rare);
		return MappedRangeResult();
	}

	if (!limits.isValid())
		return range_limits_invalid();

	if (begin.orEqual)
		begin.removeOrEqual(begin.arena());

	if (end.orEqual)
		end.removeOrEqual(end.arena());

	if (begin.offset >= end.offset && begin.getKey() >= end.getKey()) {
		CODE_PROBE(true, "RYW range inverted (getMappedRange)", probe::decoration::rare);
		return MappedRangeResult();
	}

	Future<MappedRangeResult> result =
	    reverse ? RYWImpl::readWithConflictRangeForGetMappedRange(
	                  this, RYWImpl::GetMappedRangeReq<true>(begin, end, mapper, limits), snapshot)
	            : RYWImpl::readWithConflictRangeForGetMappedRange(
	                  this, RYWImpl::GetMappedRangeReq<false>(begin, end, mapper, limits), snapshot);

	return result;
}

Future<Standalone<VectorRef<const char*>>> ReadYourWritesTransaction::getAddressesForKey(const Key& key) {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	// If key >= allKeys.end, then our resulting address vector will be empty.

	Future<Standalone<VectorRef<const char*>>> result =
	    waitOrError(tr.getAddressesForKey(key), resetPromise.getFuture());
	reading.add(success(result));
	return result;
}

Future<int64_t> ReadYourWritesTransaction::getEstimatedRangeSizeBytes(const KeyRange& keys) {
	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}
	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	// Pass in the TransactionState only if tenant is present
	Optional<Reference<TransactionState>> trState =
	    tr.trState->hasTenant() ? tr.trState : Optional<Reference<TransactionState>>();
	return map(waitOrError(tr.getDatabase()->getStorageMetrics(keys, -1, trState), resetPromise.getFuture()),
	           [](const StorageMetrics& m) { return m.bytes; });
}

Future<Standalone<VectorRef<KeyRef>>> ReadYourWritesTransaction::getRangeSplitPoints(const KeyRange& range,
                                                                                     int64_t chunkSize) {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}
	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	KeyRef maxKey = getMaxReadKey();
	if (range.begin > maxKey || range.end > maxKey)
		return key_outside_legal_range();

	return waitOrError(tr.getRangeSplitPoints(range, chunkSize), resetPromise.getFuture());
}

Future<Standalone<VectorRef<KeyRangeRef>>> ReadYourWritesTransaction::getBlobGranuleRanges(const KeyRange& range,
                                                                                           int rangeLimit) {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}
	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	KeyRef maxKey = getMaxReadKey();
	if (range.begin > maxKey || range.end > maxKey)
		return key_outside_legal_range();

	return waitOrError(tr.getBlobGranuleRanges(range, rangeLimit), resetPromise.getFuture());
}

Future<Standalone<VectorRef<BlobGranuleChunkRef>>> ReadYourWritesTransaction::readBlobGranules(
    const KeyRange& range,
    Version begin,
    Optional<Version> readVersion,
    Version* readVersionOut) {
	if (!options.readYourWritesDisabled) {
		return blob_granule_no_ryw();
	}

	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	KeyRef maxKey = getMaxReadKey();
	if (range.begin > maxKey || range.end > maxKey)
		return key_outside_legal_range();

	return waitOrError(tr.readBlobGranules(range, begin, readVersion, readVersionOut), resetPromise.getFuture());
}

Future<Standalone<VectorRef<BlobGranuleSummaryRef>>> ReadYourWritesTransaction::summarizeBlobGranules(
    const KeyRange& range,
    Optional<Version> summaryVersion,
    int rangeLimit) {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	KeyRef maxKey = getMaxReadKey();
	if (range.begin > maxKey || range.end > maxKey)
		return key_outside_legal_range();

	return waitOrError(tr.summarizeBlobGranules(range, summaryVersion, rangeLimit), resetPromise.getFuture());
}

void ReadYourWritesTransaction::addGranuleMaterializeStats(const GranuleMaterializeStats& stats) {
	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}
	tr.addGranuleMaterializeStats(stats);
}

void ReadYourWritesTransaction::addReadConflictRange(KeyRangeRef const& keys) {
	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (tr.apiVersionAtLeast(300)) {
		if ((keys.begin > getMaxReadKey() || keys.end > getMaxReadKey()) &&
		    (keys.begin != metadataVersionKey || keys.end != metadataVersionKeyEnd)) {
			throw key_outside_legal_range();
		}
	}

	// There aren't any keys in the database with size larger than max key size, so if range contains large keys
	// we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	int64_t beginMaxSize = getMaxReadKeySize(begin);
	int64_t endMaxSize = getMaxReadKeySize(end);
	if (begin.size() > beginMaxSize) {
		begin = begin.substr(0, beginMaxSize + 1);
	}
	if (end.size() > endMaxSize) {
		end = end.substr(0, endMaxSize + 1);
	}

	KeyRangeRef r = KeyRangeRef(begin, end);

	if (r.empty()) {
		return;
	}

	if (options.readYourWritesDisabled) {
		approximateSize += r.expectedSize() + sizeof(KeyRangeRef);
		tr.addReadConflictRange(r);
		return;
	}

	WriteMap::iterator it(&writes);
	KeyRangeRef readRange(arena, r);
	it.skip(readRange.begin);
	updateConflictMap(readRange, it);
}

void ReadYourWritesTransaction::updateConflictMap(KeyRef const& key, WriteMap::iterator& it) {
	RYWImpl::updateConflictMap(this, key, it);
}

void ReadYourWritesTransaction::updateConflictMap(KeyRangeRef const& keys, WriteMap::iterator& it) {
	RYWImpl::updateConflictMap(this, keys, it);
}

void ReadYourWritesTransaction::writeRangeToNativeTransaction(KeyRangeRef const& keys) {
	WriteMap::iterator it(&writes);
	it.skip(keys.begin);

	bool inClearRange = false;
	ExtStringRef clearBegin;

	// Clear ranges must be done first because of keys that are both cleared and set to a new value
	for (; it.beginKey() < keys.end; ++it) {
		if (it.is_cleared_range() && !inClearRange) {
			clearBegin = std::max(ExtStringRef(keys.begin), it.beginKey());
			inClearRange = true;
		} else if (!it.is_cleared_range() && inClearRange) {
			tr.clear(KeyRangeRef(clearBegin.toArenaOrRef(arena), it.beginKey().toArenaOrRef(arena)),
			         AddConflictRange::False);
			inClearRange = false;
		}
	}

	if (inClearRange) {
		tr.clear(KeyRangeRef(clearBegin.toArenaOrRef(arena), keys.end), AddConflictRange::False);
	}

	it.skip(keys.begin);

	bool inConflictRange = false;
	ExtStringRef conflictBegin;

	for (; it.beginKey() < keys.end; ++it) {
		if (it.is_conflict_range() && !inConflictRange) {
			conflictBegin = std::max(ExtStringRef(keys.begin), it.beginKey());
			inConflictRange = true;
		} else if (!it.is_conflict_range() && inConflictRange) {
			tr.addWriteConflictRange(KeyRangeRef(conflictBegin.toArenaOrRef(arena), it.beginKey().toArenaOrRef(arena)));
			inConflictRange = false;
		}

		// SOMEDAY: make atomicOp take set to avoid switch
		if (it.is_operation()) {
			auto op = it.op();
			for (int i = 0; i < op.size(); ++i) {
				switch (op[i].type) {
				case MutationRef::SetValue:
					if (op[i].value.present()) {
						tr.set(it.beginKey().assertRef(), op[i].value.get(), AddConflictRange::False);
					} else {
						tr.clear(it.beginKey().assertRef(), AddConflictRange::False);
					}
					break;
				case MutationRef::AddValue:
				case MutationRef::AppendIfFits:
				case MutationRef::And:
				case MutationRef::Or:
				case MutationRef::Xor:
				case MutationRef::Max:
				case MutationRef::Min:
				case MutationRef::SetVersionstampedKey:
				case MutationRef::SetVersionstampedValue:
				case MutationRef::ByteMin:
				case MutationRef::ByteMax:
				case MutationRef::MinV2:
				case MutationRef::AndV2:
				case MutationRef::CompareAndClear:
					tr.atomicOp(it.beginKey().assertRef(), op[i].value.get(), op[i].type, AddConflictRange::False);
					break;
				default:
					break;
				}
			}
		}
	}

	if (inConflictRange) {
		tr.addWriteConflictRange(KeyRangeRef(conflictBegin.toArenaOrRef(arena), keys.end));
	}
}

ReadYourWritesTransactionOptions::ReadYourWritesTransactionOptions(Transaction const& tr) {
	reset(tr);
}

void ReadYourWritesTransactionOptions::reset(Transaction const& tr) {
	memset(this, 0, sizeof(*this));
	timeoutInSeconds = 0.0;
	maxRetries = -1;
	snapshotRywEnabled = tr.getDatabase()->snapshotRywEnabled;
}

bool ReadYourWritesTransactionOptions::getAndResetWriteConflictDisabled() {
	bool disabled = nextWriteDisableConflictRange;
	nextWriteDisableConflictRange = false;
	return disabled;
}

void ReadYourWritesTransaction::getWriteConflicts(KeyRangeMap<bool>* result) {
	WriteMap::iterator it(&writes);
	it.skip(allKeys.begin);

	bool inConflictRange = false;
	ExtStringRef conflictBegin;

	for (; it.beginKey() < getMaxWriteKey(); ++it) {
		if (it.is_conflict_range() && !inConflictRange) {
			conflictBegin = it.beginKey();
			inConflictRange = true;
		} else if (!it.is_conflict_range() && inConflictRange) {
			result->insert(KeyRangeRef(conflictBegin.toArenaOrRef(arena), it.beginKey().toArenaOrRef(arena)), true);
			inConflictRange = false;
		}
	}

	if (inConflictRange) {
		result->insert(KeyRangeRef(conflictBegin.toArenaOrRef(arena), getMaxWriteKey()), true);
	}
}

void ReadYourWritesTransaction::setTransactionID(UID id) {
	tr.setTransactionID(id);
}

void ReadYourWritesTransaction::setToken(uint64_t token) {
	tr.setToken(token);
}

RangeResult ReadYourWritesTransaction::getReadConflictRangeIntersecting(KeyRangeRef kr) {
	CODE_PROBE(true, "Special keys read conflict range");
	ASSERT(readConflictRangeKeysRange.contains(kr));
	ASSERT(!tr.trState->options.checkWritesEnabled);
	RangeResult result;
	if (!options.readYourWritesDisabled) {
		kr = kr.removePrefix(readConflictRangeKeysRange.begin);
		auto iter = readConflicts.rangeContainingKeyBefore(kr.begin);
		if (iter->begin() == allKeys.begin && !iter->value()) {
			++iter; // Conventionally '' is missing from the result range if it's not part of a read conflict
		}
		for (; iter->begin() < kr.end; ++iter) {
			if (kr.begin <= iter->begin() && iter->begin() < kr.end) {
				result.push_back(result.arena(),
				                 KeyValueRef(iter->begin().withPrefix(readConflictRangeKeysRange.begin, result.arena()),
				                             iter->value() ? "1"_sr : "0"_sr));
			}
		}
	} else {
		CoalescedKeyRefRangeMap<ValueRef> readConflicts{ "0"_sr, specialKeys.end };
		for (const auto& range : tr.readConflictRanges())
			readConflicts.insert(range.withPrefix(readConflictRangeKeysRange.begin, result.arena()), "1"_sr);
		for (const auto& range : nativeReadRanges)
			readConflicts.insert(range.withPrefix(readConflictRangeKeysRange.begin, result.arena()), "1"_sr);
		for (const auto& f : tr.getExtraReadConflictRanges()) {
			if (f.isReady() && f.get().first < f.get().second)
				readConflicts.insert(KeyRangeRef(f.get().first, f.get().second)
				                         .withPrefix(readConflictRangeKeysRange.begin, result.arena()),
				                     "1"_sr);
		}
		auto beginIter = readConflicts.rangeContaining(kr.begin);
		if (beginIter->begin() != kr.begin)
			++beginIter;
		for (auto it = beginIter; it->begin() < kr.end; ++it) {
			result.push_back(result.arena(), KeyValueRef(it->begin(), it->value()));
		}
	}
	return result;
}

RangeResult ReadYourWritesTransaction::getWriteConflictRangeIntersecting(KeyRangeRef kr) {
	CODE_PROBE(true, "Special keys write conflict range");
	ASSERT(writeConflictRangeKeysRange.contains(kr));
	RangeResult result;

	// Memory owned by result
	CoalescedKeyRefRangeMap<ValueRef> writeConflicts{ "0"_sr, specialKeys.end };

	if (!options.readYourWritesDisabled) {
		KeyRangeRef strippedWriteRangePrefix = kr.removePrefix(writeConflictRangeKeysRange.begin);
		WriteMap::iterator it(&writes);
		it.skip(strippedWriteRangePrefix.begin);
		if (it.beginKey() > allKeys.begin)
			--it;
		for (; it.beginKey() < strippedWriteRangePrefix.end; ++it) {
			if (it.is_conflict_range())
				writeConflicts.insert(
				    KeyRangeRef(it.beginKey().toArena(result.arena()), it.endKey().toArena(result.arena()))
				        .withPrefix(writeConflictRangeKeysRange.begin, result.arena()),
				    "1"_sr);
		}
	} else {
		for (const auto& range : tr.writeConflictRanges())
			writeConflicts.insert(range.withPrefix(writeConflictRangeKeysRange.begin, result.arena()), "1"_sr);
		for (const auto& range : nativeWriteRanges)
			writeConflicts.insert(range.withPrefix(writeConflictRangeKeysRange.begin, result.arena()), "1"_sr);
	}

	for (const auto& k : versionStampKeys) {
		KeyRange range;
		if (versionStampFuture.isValid() && versionStampFuture.isReady() && !versionStampFuture.isError()) {
			const auto& stamp = versionStampFuture.get();
			StringRef key(range.arena(), k); // Copy
			ASSERT(k.size() >= 4);
			int32_t pos;
			memcpy(&pos, k.end() - sizeof(int32_t), sizeof(int32_t));
			pos = littleEndian32(pos);
			ASSERT(pos >= 0 && pos + stamp.size() <= key.size());
			memcpy(mutateString(key) + pos, stamp.begin(), stamp.size());
			*(mutateString(key) + key.size() - 4) = '\x00';
			// singleKeyRange, but share begin and end's memory
			range = KeyRangeRef(key.substr(0, key.size() - 4), key.substr(0, key.size() - 3));
		} else {
			range = getVersionstampKeyRange(result.arena(), k, tr.getCachedReadVersion(), getMaxReadKey());
		}
		writeConflicts.insert(range.withPrefix(writeConflictRangeKeysRange.begin, result.arena()), "1"_sr);
	}

	auto beginIter = writeConflicts.rangeContaining(kr.begin);
	if (beginIter->begin() != kr.begin)
		++beginIter;
	for (auto it = beginIter; it->begin() < kr.end; ++it) {
		result.push_back(result.arena(), KeyValueRef(it->begin(), it->value()));
	}

	return result;
}

void ReadYourWritesTransaction::atomicOp(const KeyRef& key, const ValueRef& operand, uint32_t operationType) {
	AddConflictRange addWriteConflict{ !options.getAndResetWriteConflictDisabled() };

	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (key == metadataVersionKey) {
		if (operationType != MutationRef::SetVersionstampedValue || operand != metadataVersionRequiredValue) {
			throw client_invalid_operation();
		}
	} else if (key >= getMaxWriteKey()) {
		throw key_outside_legal_range();
	}

	if (!isValidMutationType(operationType) || !isAtomicOp((MutationRef::Type)operationType))
		throw invalid_mutation_type();

	if (key.size() > getMaxWriteKeySize(key, getTransactionState()->options.rawAccess)) {
		throw key_too_large();
	}
	if (operand.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	if (tr.apiVersionAtLeast(510)) {
		if (operationType == MutationRef::Min)
			operationType = MutationRef::MinV2;
		else if (operationType == MutationRef::And)
			operationType = MutationRef::AndV2;
	}

	KeyRef k;
	if (!tr.apiVersionAtLeast(520) && operationType == MutationRef::SetVersionstampedKey) {
		k = key.withSuffix("\x00\x00"_sr, arena);
	} else {
		k = KeyRef(arena, key);
	}
	ValueRef v;
	if (!tr.apiVersionAtLeast(520) && operationType == MutationRef::SetVersionstampedValue) {
		v = operand.withSuffix("\x00\x00\x00\x00"_sr, arena);
	} else {
		v = ValueRef(arena, operand);
	}

	if (operationType == MutationRef::SetVersionstampedKey) {
		CODE_PROBE(options.readYourWritesDisabled, "SetVersionstampedKey without ryw enabled");
		// this does validation of the key and needs to be performed before the readYourWritesDisabled path
		KeyRangeRef range = getVersionstampKeyRange(arena, k, tr.getCachedReadVersion(), getMaxReadKey());
		versionStampKeys.push_back(arena, k);
		addWriteConflict = AddConflictRange::False;
		if (!options.readYourWritesDisabled) {
			writeRangeToNativeTransaction(range);
			writes.addUnmodifiedAndUnreadableRange(range);
		}
		// k is the unversionstamped key provided by the user.  If we've filled in a minimum bound
		// for the versionstamp, we need to make sure that's reflected when we insert it into the
		// WriteMap below.
		transformVersionstampKey(k, tr.getCachedReadVersion().map([](Version v) { return v + 1; }).orDefault(0), 0);
	}

	if (operationType == MutationRef::SetVersionstampedValue) {
		if (v.size() < 4)
			throw client_invalid_operation();
		int32_t pos;
		memcpy(&pos, v.end() - sizeof(int32_t), sizeof(int32_t));
		pos = littleEndian32(pos);
		if (pos < 0 || pos + 10 > v.size() - 4)
			throw client_invalid_operation();
	}

	approximateSize += k.expectedSize() + v.expectedSize() + sizeof(MutationRef) +
	                   (addWriteConflict ? sizeof(KeyRangeRef) + 2 * key.expectedSize() + 1 : 0);
	if (options.readYourWritesDisabled) {
		return tr.atomicOp(k, v, (MutationRef::Type)operationType, addWriteConflict);
	}

	writes.mutate(k, (MutationRef::Type)operationType, v, addWriteConflict);
	RYWImpl::triggerWatches(this, k, Optional<ValueRef>(), false);
}

void ReadYourWritesTransaction::set(const KeyRef& key, const ValueRef& value) {
	if (key == metadataVersionKey) {
		throw client_invalid_operation();
	}

	if (specialKeys.contains(key)) {
		if (getDatabase()->apiVersionAtLeast(700)) {
			return getDatabase()->specialKeySpace->set(this, key, value);
		} else {
			// These three special keys are deprecated in 7.0 and an alternative C API is added
			// TODO : Rewrite related code using C api
			if (key == "\xff\xff/reboot_worker"_sr) {
				BinaryReader::fromStringRef<ClientWorkerInterface>(value, IncludeVersion())
				    .reboot.send(RebootRequest());
				return;
			}
			if (key == "\xff\xff/suspend_worker"_sr) {
				BinaryReader::fromStringRef<ClientWorkerInterface>(value, IncludeVersion())
				    .reboot.send(RebootRequest(false, false, options.timeoutInSeconds));
				return;
			}
			if (key == "\xff\xff/reboot_and_check_worker"_sr) {
				BinaryReader::fromStringRef<ClientWorkerInterface>(value, IncludeVersion())
				    .reboot.send(RebootRequest(false, true));
				return;
			}
		}
	}

	AddConflictRange addWriteConflict{ !options.getAndResetWriteConflictDisabled() };

	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (key >= getMaxWriteKey())
		throw key_outside_legal_range();

	approximateSize += key.expectedSize() + value.expectedSize() + sizeof(MutationRef) +
	                   (addWriteConflict ? sizeof(KeyRangeRef) + 2 * key.expectedSize() + 1 : 0);
	if (options.readYourWritesDisabled) {
		return tr.set(key, value, addWriteConflict);
	}

	// TODO: check transaction size here
	if (key.size() > getMaxWriteKeySize(key, getTransactionState()->options.rawAccess)) {
		throw key_too_large();
	}
	if (value.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	KeyRef k = KeyRef(arena, key);
	ValueRef v = ValueRef(arena, value);

	writes.mutate(k, MutationRef::SetValue, v, addWriteConflict);
	RYWImpl::triggerWatches(this, key, value);
}

void ReadYourWritesTransaction::clear(const KeyRangeRef& range) {
	AddConflictRange addWriteConflict{ !options.getAndResetWriteConflictDisabled() };

	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (specialKeys.contains(range)) {
		if (getDatabase()->apiVersionAtLeast(700)) {
			return getDatabase()->specialKeySpace->clear(this, range);
		}
	}

	KeyRef maxKey = getMaxWriteKey();
	if (range.begin > maxKey || range.end > maxKey)
		throw key_outside_legal_range();

	approximateSize += range.expectedSize() + sizeof(MutationRef) +
	                   (addWriteConflict ? sizeof(KeyRangeRef) + range.expectedSize() : 0);
	if (options.readYourWritesDisabled) {
		return tr.clear(range, addWriteConflict);
	}

	// There aren't any keys in the database with size larger than the max key size, so if range contains large keys
	// we can translate it to an equivalent one with smaller keys
	KeyRef begin = range.begin;
	KeyRef end = range.end;

	int64_t beginMaxSize = getMaxClearKeySize(begin);
	int64_t endMaxSize = getMaxClearKeySize(end);
	if (begin.size() > beginMaxSize) {
		begin = begin.substr(0, beginMaxSize + 1);
	}
	if (end.size() > endMaxSize) {
		end = end.substr(0, endMaxSize + 1);
	}

	KeyRangeRef r = KeyRangeRef(begin, end);

	if (r.empty()) {
		return;
	}

	r = KeyRangeRef(arena, r);

	writes.clear(r, addWriteConflict);
	RYWImpl::triggerWatches(this, r, Optional<ValueRef>());
}

void ReadYourWritesTransaction::clear(const KeyRef& key) {
	AddConflictRange addWriteConflict{ !options.getAndResetWriteConflictDisabled() };

	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (specialKeys.contains(key)) {
		if (getDatabase()->apiVersionAtLeast(700)) {
			return getDatabase()->specialKeySpace->clear(this, key);
		}
	}

	if (key >= getMaxWriteKey())
		throw key_outside_legal_range();

	if (key.size() > getMaxClearKeySize(key)) {
		return;
	}

	if (options.readYourWritesDisabled) {
		return tr.clear(key, addWriteConflict);
	}

	KeyRangeRef r = singleKeyRange(key, arena);
	approximateSize +=
	    r.expectedSize() + sizeof(KeyRangeRef) + (addWriteConflict ? sizeof(KeyRangeRef) + r.expectedSize() : 0);

	// SOMEDAY: add an optimized single key clear to write map
	writes.clear(r, addWriteConflict);

	RYWImpl::triggerWatches(this, r, Optional<ValueRef>());
}

Future<Void> ReadYourWritesTransaction::watch(const Key& key) {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if (resetPromise.isSet())
		return resetPromise.getFuture().getError();

	if (options.readYourWritesDisabled)
		return watches_disabled();

	if (key >= allKeys.end || (key >= getMaxReadKey() && key != metadataVersionKey && tr.apiVersionAtLeast(300)))
		return key_outside_legal_range();

	if (key.size() > getMaxWriteKeySize(key, getTransactionState()->options.rawAccess)) {
		return key_too_large();
	}

	return RYWImpl::watch(this, key);
}

void ReadYourWritesTransaction::addWriteConflictRange(KeyRangeRef const& keys) {
	if (checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (tr.apiVersionAtLeast(300)) {
		if (keys.begin > getMaxWriteKey() || keys.end > getMaxWriteKey()) {
			throw key_outside_legal_range();
		}
	}

	// There aren't any keys in the database with size larger than the max key size, so if range contains large keys
	// we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	int64_t beginMaxSize = getMaxKeySize(begin);
	int64_t endMaxSize = getMaxKeySize(end);
	if (begin.size() > beginMaxSize) {
		begin = begin.substr(0, beginMaxSize + 1);
	}
	if (end.size() > endMaxSize) {
		end = end.substr(0, endMaxSize + 1);
	}

	KeyRangeRef r = KeyRangeRef(begin, end);

	if (r.empty()) {
		return;
	}

	approximateSize += r.expectedSize() + sizeof(KeyRangeRef);
	if (options.readYourWritesDisabled) {
		tr.addWriteConflictRange(r);
		return;
	}

	r = KeyRangeRef(arena, r);
	writes.addConflictRange(r);
}

Future<Void> ReadYourWritesTransaction::commit() {
	Future<Void> result;
	if (checkUsedDuringCommit()) {
		result = used_during_commit();
	} else if (resetPromise.isSet()) {
		result = resetPromise.getFuture().getError();
	} else {
		result = RYWImpl::commit(this);
	}

	return debugMessages.size() > 0 || debugTraces.size() > 0 ? RYWImpl::printDebugMessages(this, result) : result;
}

Future<Standalone<StringRef>> ReadYourWritesTransaction::getVersionstamp() {
	if (checkUsedDuringCommit()) {
		return used_during_commit();
	}

	return waitOrError(tr.getVersionstamp(), resetPromise.getFuture());
}

void ReadYourWritesTransaction::setOption(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	setOptionImpl(option, value);
	auto const& opt = FDBTransactionOptions::optionInfo.getMustExist(option);
	if (opt.persistent) {
		if (opt.sensitive)
			sensitivePersistentOptions.emplace_back(option, value.castTo<WipedString>());
		else
			persistentOptions.emplace_back(option, value.castTo<Standalone<StringRef>>());
	}
}

void ReadYourWritesTransaction::setOptionImpl(FDBTransactionOptions::Option option, Optional<StringRef> value) {
	TraceEvent(SevDebug, "TransactionSetOption").detail("Option", option).detail("Value", value);
	switch (option) {
	case FDBTransactionOptions::READ_YOUR_WRITES_DISABLE:
		validateOptionValueNotPresent(value);

		if (reading.getFutureCount() > 0 || !cache.empty() || !writes.empty())
			throw client_invalid_operation();

		options.readYourWritesDisabled = true;
		break;

	case FDBTransactionOptions::READ_AHEAD_DISABLE:
		validateOptionValueNotPresent(value);

		options.readAheadDisabled = true;
		break;

	case FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE:
		validateOptionValueNotPresent(value);

		options.nextWriteDisableConflictRange = true;
		break;

	case FDBTransactionOptions::ACCESS_SYSTEM_KEYS:
		validateOptionValueNotPresent(value);

		options.readSystemKeys = true;
		options.writeSystemKeys = true;
		break;

	case FDBTransactionOptions::READ_SYSTEM_KEYS:
		validateOptionValueNotPresent(value);

		options.readSystemKeys = true;
		break;

	case FDBTransactionOptions::TIMEOUT:
		options.timeoutInSeconds = extractIntOption(value, 0, std::numeric_limits<int>::max()) / 1000.0;
		TraceEvent(SevDebug, "TransactionTimeout").detail("TimeoutInSeconds", options.timeoutInSeconds);
		resetTimeout();
		break;

	case FDBTransactionOptions::RETRY_LIMIT:
		options.maxRetries = (int)extractIntOption(value, -1, std::numeric_limits<int>::max());
		TraceEvent(SevDebug, "TransactionRetryLimit").detail("MaxRetries", options.maxRetries);
		break;

	case FDBTransactionOptions::DEBUG_RETRY_LOGGING:
		options.debugRetryLogging = true;
		if (!transactionDebugInfo) {
			transactionDebugInfo = Reference<TransactionDebugInfo>::addRef(new TransactionDebugInfo());
			transactionDebugInfo->lastRetryLogTime = creationTime;
		}

		transactionDebugInfo->transactionName = value.present() ? value.get().toString() : "";
		break;
	case FDBTransactionOptions::SNAPSHOT_RYW_ENABLE:
		validateOptionValueNotPresent(value);

		options.snapshotRywEnabled++;
		break;
	case FDBTransactionOptions::SNAPSHOT_RYW_DISABLE:
		validateOptionValueNotPresent(value);

		options.snapshotRywEnabled--;
		break;
	case FDBTransactionOptions::USED_DURING_COMMIT_PROTECTION_DISABLE:
		validateOptionValueNotPresent(value);

		options.disableUsedDuringCommitProtection = true;
		break;
	case FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED:
		validateOptionValueNotPresent(value);
		options.specialKeySpaceRelaxed = true;
		break;
	case FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES:
		validateOptionValueNotPresent(value);
		options.specialKeySpaceChangeConfiguration = true;
		break;
	case FDBTransactionOptions::BYPASS_UNREADABLE:
		validateOptionValueNotPresent(value);
		options.bypassUnreadable = true;
		break;
	default:
		break;
	}

	tr.setOption(option, value);
}

void ReadYourWritesTransaction::operator=(ReadYourWritesTransaction&& r) noexcept {
	cache = std::move(r.cache);
	writes = std::move(r.writes);
	arena = std::move(r.arena);
	tr = std::move(r.tr);
	readConflicts = std::move(r.readConflicts);
	watchMap = std::move(r.watchMap);
	reading = std::move(r.reading);
	resetPromise = std::move(r.resetPromise);
	r.resetPromise = Promise<Void>();
	deferredError = std::move(r.deferredError);
	retries = r.retries;
	approximateSize = r.approximateSize;
	timeoutActor = r.timeoutActor;
	creationTime = r.creationTime;
	commitStarted = r.commitStarted;
	options = r.options;
	transactionDebugInfo = r.transactionDebugInfo;
	cache.arena = &arena;
	writes.arena = &arena;
	persistentOptions = std::move(r.persistentOptions);
	sensitivePersistentOptions = std::move(r.sensitivePersistentOptions);
	nativeReadRanges = std::move(r.nativeReadRanges);
	nativeWriteRanges = std::move(r.nativeWriteRanges);
	versionStampKeys = std::move(r.versionStampKeys);
	specialKeySpaceWriteMap = std::move(r.specialKeySpaceWriteMap);
	debugTraces = std::move(r.debugTraces);
	debugMessages = std::move(r.debugMessages);
}

ReadYourWritesTransaction::ReadYourWritesTransaction(ReadYourWritesTransaction&& r) noexcept
  : ISingleThreadTransaction(std::move(r.deferredError)), arena(std::move(r.arena)), cache(std::move(r.cache)),
    writes(std::move(r.writes)), resetPromise(std::move(r.resetPromise)), reading(std::move(r.reading)),
    retries(r.retries), approximateSize(r.approximateSize), timeoutActor(std::move(r.timeoutActor)),
    creationTime(r.creationTime), commitStarted(r.commitStarted), transactionDebugInfo(r.transactionDebugInfo),
    options(r.options) {
	cache.arena = &arena;
	writes.arena = &arena;
	tr = std::move(r.tr);
	readConflicts = std::move(r.readConflicts);
	watchMap = std::move(r.watchMap);
	r.resetPromise = Promise<Void>();
	persistentOptions = std::move(r.persistentOptions);
	sensitivePersistentOptions = std::move(r.sensitivePersistentOptions);
	nativeReadRanges = std::move(r.nativeReadRanges);
	nativeWriteRanges = std::move(r.nativeWriteRanges);
	versionStampKeys = std::move(r.versionStampKeys);
	specialKeySpaceWriteMap = std::move(r.specialKeySpaceWriteMap);
	debugTraces = std::move(r.debugTraces);
	debugMessages = std::move(r.debugMessages);
}

Future<Void> ReadYourWritesTransaction::onError(Error const& e) {
	return RYWImpl::onError(this, e);
}

void ReadYourWritesTransaction::applyPersistentOptions() {
	Optional<StringRef> timeout;
	for (auto const& option : persistentOptions) {
		if (option.first == FDBTransactionOptions::TIMEOUT) {
			timeout = option.second.castTo<StringRef>();
		} else {
			setOptionImpl(option.first, option.second.castTo<StringRef>());
		}
	}
	for (auto const& option : sensitivePersistentOptions) {
		setOptionImpl(option.first, option.second.castTo<StringRef>());
	}

	// Setting a timeout can immediately cause a transaction to fail. The only timeout
	// that matters is the one most recently set, so we ignore any earlier set timeouts
	// that might inadvertently fail the transaction.
	if (timeout.present()) {
		setOptionImpl(FDBTransactionOptions::TIMEOUT, timeout);
	}
}

void ReadYourWritesTransaction::resetRyow() {
	Promise<Void> oldReset = resetPromise;
	resetPromise = Promise<Void>();

	timeoutActor.cancel();
	arena = Arena();
	cache = SnapshotCache(&arena);
	writes = WriteMap(&arena);
	readConflicts = CoalescedKeyRefRangeMap<bool>();
	versionStampKeys = VectorRef<KeyRef>();
	nativeReadRanges = Standalone<VectorRef<KeyRangeRef>>();
	nativeWriteRanges = Standalone<VectorRef<KeyRangeRef>>();
	specialKeySpaceWriteMap =
	    KeyRangeMap<std::pair<bool, Optional<Value>>>(std::make_pair(false, Optional<Value>()), specialKeys.end);
	specialKeySpaceErrorMsg.reset();
	watchMap.clear();
	reading = AndFuture();
	approximateSize = 0;
	commitStarted = false;

	deferredError = Error();

	if (tr.apiVersionAtLeast(16)) {
		options.reset(tr);
		applyPersistentOptions();
	}

	if (!oldReset.isSet())
		oldReset.sendError(transaction_cancelled());
}

void ReadYourWritesTransaction::cancel() {
	if (!resetPromise.isSet())
		resetPromise.sendError(transaction_cancelled());
}

void ReadYourWritesTransaction::reset() {
	if (debugTraces.size() > 0 || debugMessages.size() > 0) {
		// printDebugMessages returns a future but will not block if called with an empty second argument
		ASSERT(RYWImpl::printDebugMessages(this, {}).isReady());
	}

	retries = 0;
	approximateSize = 0;
	creationTime = now();
	timeoutActor.cancel();
	persistentOptions.clear();
	sensitivePersistentOptions.clear();
	options.reset(tr);
	transactionDebugInfo.clear();
	tr.fullReset();
	versionStampFuture = tr.getVersionstamp();
	std::copy(tr.getDatabase().getTransactionDefaults().begin(),
	          tr.getDatabase().getTransactionDefaults().end(),
	          std::back_inserter(persistentOptions));
	resetRyow();
}

KeyRef ReadYourWritesTransaction::getMaxReadKey() {
	if (options.readSystemKeys)
		return systemKeys.end;
	else
		return normalKeys.end;
}

KeyRef ReadYourWritesTransaction::getMaxWriteKey() {
	if (options.writeSystemKeys)
		return systemKeys.end;
	else
		return normalKeys.end;
}

ReadYourWritesTransaction::~ReadYourWritesTransaction() {
	if (!resetPromise.isSet())
		resetPromise.sendError(transaction_cancelled());

	if (debugTraces.size() || debugMessages.size()) {
		// printDebugMessages returns a future but will not block if called with an empty second argument
		[[maybe_unused]] Future<Void> f = RYWImpl::printDebugMessages(this, {});
	}
}

bool ReadYourWritesTransaction::checkUsedDuringCommit() {
	if (commitStarted && !resetPromise.isSet() && !options.disableUsedDuringCommitProtection) {
		resetPromise.sendError(used_during_commit());
	}

	return commitStarted;
}

void ReadYourWritesTransaction::debugLogRetries(Optional<Error> error) {
	bool committed = !error.present();
	if (options.debugRetryLogging) {
		double timeSinceLastLog = now() - transactionDebugInfo->lastRetryLogTime;
		double elapsed = now() - creationTime;
		if (timeSinceLastLog >= 1 || (committed && elapsed > 1)) {
			std::string transactionNameStr = "";
			if (!transactionDebugInfo->transactionName.empty())
				transactionNameStr =
				    format(" in transaction '%s'", printable(StringRef(transactionDebugInfo->transactionName)).c_str());
			if (!g_network->isSimulated()) // Fuzz workload turns this on, but we do not want stderr output in
			                               // simulation
				fprintf(stderr,
				        "fdb WARNING: long transaction (%.2fs elapsed%s, %d retries, %s)\n",
				        elapsed,
				        transactionNameStr.c_str(),
				        retries,
				        committed ? "committed" : error.get().what());
			{
				TraceEvent trace = TraceEvent("LongTransaction");
				if (error.present())
					trace.errorUnsuppressed(error.get());
				if (!transactionDebugInfo->transactionName.empty())
					trace.detail("TransactionName", transactionDebugInfo->transactionName);
				trace.detail("Elapsed", elapsed).detail("Retries", retries).detail("Committed", committed);
			}
			transactionDebugInfo->lastRetryLogTime = now();
		}
	}
}

void ReadYourWritesTransaction::debugTrace(BaseTraceEvent&& event) {
	if (event.isEnabled()) {
		debugTraces.emplace_back(std::move(event));
	}
}

void ReadYourWritesTransaction::debugPrint(std::string const& message) {
	debugMessages.push_back(message);
}
