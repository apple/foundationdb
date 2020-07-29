/*
 * ReadYourWrites.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/Atomic.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbclient/StatusClient.h"
#include "fdbclient/MonitorLeader.h"
#include "flow/Util.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

class RYWImpl {
public:
	template<class Iter> static void dump( Iter it ) {
		it.skip(allKeys.begin);
		Arena arena;
		while( true ) {
			Optional<StringRef> key = StringRef();
			if (it.is_kv()) {
				auto kv = it.kv(arena);
				if (kv) key = kv->key;
			}
			TraceEvent("RYWDump")
			    .detail("Begin", it.beginKey())
			    .detail("End", it.endKey())
			    .detail("Unknown", it.is_unknown_range())
			    .detail("Empty", it.is_empty_range())
			    .detail("KV", it.is_kv())
			    .detail("Key", key.get());
			if( it.endKey() == allKeys.end )
				break;
			++it;
		}
	}

	struct GetValueReq {
		explicit GetValueReq( Key key ) : key(key) {}
		Key key;
		typedef Optional<Value> Result;
	};

	struct GetKeyReq {
		explicit GetKeyReq( KeySelector key ) : key(key) {}
		KeySelector key;
		typedef Key Result;
	};

	template <bool Reverse>
	struct GetRangeReq {
		GetRangeReq( KeySelector begin, KeySelector end, GetRangeLimits limits ) : begin(begin), end(end), limits(limits) {}
		KeySelector begin, end;
		GetRangeLimits limits;
		typedef Standalone<RangeResultRef> Result;
	};

	// read() Performs a read (get, getKey, getRange, etc), in the context of the given transaction.  Snapshot or RYW reads are distingushed by the type Iter being SnapshotCache::iterator or RYWIterator.
	// Fills in the snapshot cache as a side effect but does not affect conflict ranges.
	// Some (indicated) overloads of read are required to update the given *it to point to the key that was read, so that the corresponding overload of addConflictRange() can make use of it.

	ACTOR template<class Iter> static Future< Optional<Value> > read( ReadYourWritesTransaction *ryw, GetValueReq read, Iter* it ) {
		// This overload is required to provide postcondition: it->extractWriteMapIterator().segmentContains(read.key)

		it->skip(read.key);
		state bool dependent = it->is_dependent();
		if( it->is_kv() ) {
			const KeyValueRef* result = it->kv(ryw->arena);
			if (result != nullptr) {
				return result->value;
			} else {
				return Optional<Value>();
			}
		} else if( it->is_empty_range() ) {
			return Optional<Value>();
		} else {
			Optional<Value> res = wait( ryw->tr.get( read.key, true ) );
			KeyRef k( ryw->arena, read.key );

			if( res.present() ) {
				if( ryw->cache.insert( k, res.get() ) )
					ryw->arena.dependsOn(res.get().arena());
				if( !dependent )
					return res;
			} else {
				ryw->cache.insert( k, Optional<ValueRef>() );
				if( !dependent )
					return Optional<Value>();
			}

			//There was a dependent write at the key, so we need to lookup the iterator again
			it->skip(k);

			ASSERT( it->is_kv() );
			const KeyValueRef* result = it->kv(ryw->arena);
			if (result != nullptr) {
				return result->value;
			} else {
				return Optional<Value>();
			}
		}
	}

	ACTOR template<class Iter> static Future< Key > read( ReadYourWritesTransaction* ryw, GetKeyReq read, Iter* it ) {
		if( read.key.offset > 0 ) {
			Standalone<RangeResultRef> result = wait( getRangeValue( ryw, read.key, firstGreaterOrEqual(ryw->getMaxReadKey()), GetRangeLimits(1), it ) );
			if( result.readToBegin )
				return allKeys.begin;
			if( result.readThroughEnd || !result.size() )
				return ryw->getMaxReadKey();
			return result[0].key;
		} else {
			read.key.offset++;
			Standalone<RangeResultRef> result = wait( getRangeValueBack( ryw, firstGreaterOrEqual(allKeys.begin), read.key, GetRangeLimits(1), it ) );
			if( result.readThroughEnd )
				return ryw->getMaxReadKey();
			if( result.readToBegin || !result.size() )
				return allKeys.begin;
			return result[0].key;
		}
	};

	template <class Iter> static Future< Standalone<RangeResultRef> > read( ReadYourWritesTransaction* ryw, GetRangeReq<false> read, Iter* it ) {
		return getRangeValue( ryw, read.begin, read.end, read.limits, it );
	};

	template <class Iter> static Future< Standalone<RangeResultRef> > read( ReadYourWritesTransaction* ryw, GetRangeReq<true> read, Iter* it ) {
		return getRangeValueBack( ryw, read.begin, read.end, read.limits, it );
	};

	// readThrough() performs a read in the RYW disabled case, passing it on relatively directly to the underlying transaction.
	// Responsible for clipping results to the non-system keyspace when appropriate, since NativeAPI doesn't do that.

	static Future<Optional<Value>> readThrough( ReadYourWritesTransaction *ryw, GetValueReq read, bool snapshot ) {
		return ryw->tr.get( read.key, snapshot );
	}

	ACTOR static Future<Key> readThrough( ReadYourWritesTransaction *ryw, GetKeyReq read, bool snapshot ) {
		Key key = wait( ryw->tr.getKey( read.key, snapshot ) );
		if (ryw->getMaxReadKey() < key) return ryw->getMaxReadKey();  // Filter out results in the system keys if they are not accessible
		return key;
	}

	ACTOR template <bool Reverse> static Future<Standalone<RangeResultRef>> readThrough( ReadYourWritesTransaction *ryw, GetRangeReq<Reverse> read, bool snapshot ) {
		if(Reverse && read.end.offset > 1) {
			// FIXME: Optimistically assume that this will not run into the system keys, and only reissue if the result actually does.
			Key key = wait( ryw->tr.getKey(read.end, snapshot) );
			if(key > ryw->getMaxReadKey())
				read.end = firstGreaterOrEqual(ryw->getMaxReadKey());
			else
				read.end = KeySelector(firstGreaterOrEqual(key), key.arena());
		}

		Standalone<RangeResultRef> v = wait( ryw->tr.getRange(read.begin, read.end, read.limits, snapshot, Reverse) );
		KeyRef maxKey = ryw->getMaxReadKey();
		if(v.size() > 0) {
			if(!Reverse && v[v.size()-1].key >= maxKey) {
				state Standalone<RangeResultRef> _v = v;
				int i = _v.size() - 2;
				for(; i >= 0 && _v[i].key >= maxKey; --i) { }
				return Standalone<RangeResultRef>(RangeResultRef( VectorRef<KeyValueRef>(&_v[0], i+1), false ), _v.arena());
			}
		}
					
		return v;
	}

	// addConflictRange(ryw,read,result) is called after a serializable read and is responsible for adding the relevant conflict range

	static void addConflictRange( ReadYourWritesTransaction* ryw, GetValueReq read, WriteMap::iterator& it, Optional<Value> result ) {
		// it will already point to the right segment (see the calling code in read()), so we don't need to skip
		// read.key will be copied into ryw->arena inside of updateConflictMap if it is being added
		ryw->updateConflictMap(read.key, it);
	}

	static void addConflictRange( ReadYourWritesTransaction* ryw, GetKeyReq read, WriteMap::iterator& it, Key result ) {
		KeyRangeRef readRange;
		if( read.key.offset <= 0 )
			readRange = KeyRangeRef( KeyRef( ryw->arena, result ), read.key.orEqual ? keyAfter( read.key.getKey(), ryw->arena ) : KeyRef( ryw->arena, read.key.getKey() ) );
		else
			readRange = KeyRangeRef( read.key.orEqual ? keyAfter( read.key.getKey(), ryw->arena ) : KeyRef( ryw->arena, read.key.getKey() ), keyAfter( result, ryw->arena ) );

		it.skip( readRange.begin );
		ryw->updateConflictMap(readRange, it);
	}

	static void addConflictRange( ReadYourWritesTransaction* ryw, GetRangeReq<false> read, WriteMap::iterator &it, Standalone<RangeResultRef> const& result ) {
		KeyRef rangeBegin, rangeEnd;
		bool endInArena = false;

		if( read.begin.getKey() < read.end.getKey() ) {
			rangeBegin = read.begin.getKey();
			rangeEnd = read.end.offset > 0 && result.more ? read.begin.getKey() : read.end.getKey();
		} 
		else {
			rangeBegin = read.end.getKey();
			rangeEnd = read.begin.getKey();
		}

		if( result.readToBegin && read.begin.offset <= 0 ) rangeBegin = allKeys.begin;
		if( result.readThroughEnd && read.end.offset > 0 ) rangeEnd = ryw->getMaxReadKey();

		if ( result.size() ) {
			if( read.begin.offset <= 0 ) rangeBegin = std::min( rangeBegin, result[0].key );
			if( rangeEnd <= result.end()[-1].key ) {
				rangeEnd = keyAfter( result.end()[-1].key, ryw->arena );
				endInArena = true;
			}
		}

		KeyRangeRef readRange = KeyRangeRef( KeyRef( ryw->arena, rangeBegin ), endInArena ? rangeEnd : KeyRef( ryw->arena, rangeEnd ) );
		it.skip( readRange.begin );
		ryw->updateConflictMap(readRange, it);
	}

	static void addConflictRange( ReadYourWritesTransaction* ryw, GetRangeReq<true> read, WriteMap::iterator& it, Standalone<RangeResultRef> const& result ) {
		KeyRef rangeBegin, rangeEnd;
		bool endInArena = false;

		if( read.begin.getKey() < read.end.getKey() ) {
			rangeBegin = read.begin.offset <= 0 && result.more ? read.end.getKey() : read.begin.getKey();
			rangeEnd = read.end.getKey();
		} 
		else {
			rangeBegin = read.end.getKey();
			rangeEnd = read.begin.getKey();
		}

		if( result.readToBegin && read.begin.offset <= 0 ) rangeBegin = allKeys.begin;
		if( result.readThroughEnd && read.end.offset > 0 ) rangeEnd = ryw->getMaxReadKey();

		if ( result.size() ) {
			rangeBegin = std::min( rangeBegin, result.end()[-1].key );
			if( read.end.offset > 0 && rangeEnd <= result[0].key ) {
				rangeEnd = keyAfter( result[0].key, ryw->arena );
				endInArena = true;
			}
		}

		KeyRangeRef readRange = KeyRangeRef( KeyRef( ryw->arena, rangeBegin ), endInArena ? rangeEnd : KeyRef( ryw->arena, rangeEnd ) );
		it.skip( readRange.begin );
		ryw->updateConflictMap(readRange, it);
	}

	ACTOR template <class Req> static Future<typename Req::Result> readWithConflictRangeThrough( ReadYourWritesTransaction* ryw, Req req, bool snapshot ) {
		choose {
			when (typename Req::Result result = wait( readThrough( ryw, req, snapshot ) )) {
				return result;
			}
			when (wait(ryw->resetPromise.getFuture())) { throw internal_error(); }
		}
	}
	ACTOR template <class Req> static Future<typename Req::Result> readWithConflictRangeSnapshot( ReadYourWritesTransaction* ryw, Req req ) {
		state SnapshotCache::iterator it(&ryw->cache, &ryw->writes);
		choose {
			when (typename Req::Result result = wait( read( ryw, req, &it ) )) {
				return result;
			}
			when (wait(ryw->resetPromise.getFuture())) { throw internal_error(); }
		}
	}
	ACTOR template <class Req> static Future<typename Req::Result> readWithConflictRangeRYW( ReadYourWritesTransaction* ryw, Req req, bool snapshot ) {
		state RYWIterator it( &ryw->cache, &ryw->writes );
		choose {
			when (typename Req::Result result = wait( read( ryw, req, &it ) )) {
				// Some overloads of addConflictRange() require it to point to the "right" key and others don't.  The corresponding overloads of read() have to provide that guarantee!
				if(!snapshot)
					addConflictRange( ryw, req, it.extractWriteMapIterator(), result );
				return result;
			}
			when (wait(ryw->resetPromise.getFuture())) { throw internal_error(); }
		}
	}
	template <class Req> static inline Future<typename Req::Result> readWithConflictRange( ReadYourWritesTransaction* ryw, Req const& req, bool snapshot ) {
		if (ryw->options.readYourWritesDisabled) {
			return readWithConflictRangeThrough(ryw, req, snapshot);
		} else if (snapshot && ryw->options.snapshotRywEnabled <= 0) {
			return readWithConflictRangeSnapshot(ryw, req);
		}
		return readWithConflictRangeRYW(ryw, req, snapshot);
	}

	template<class Iter> static void resolveKeySelectorFromCache( KeySelector& key, Iter& it, KeyRef const& maxKey, bool* readToBegin, bool* readThroughEnd, int* actualOffset ) {
		// If the key indicated by `key` can be determined without reading unknown data from the snapshot, then it.kv().key is the resolved key.
		// If the indicated key is determined to be "off the beginning or end" of the database, it points to the first or last segment in the DB,
		//   and key is an equivalent key selector relative to the beginning or end of the database.
		// Otherwise it points to an unknown segment, and key is an equivalent key selector whose base key is in or adjoining the segment.

		key.removeOrEqual(key.arena());

		bool alreadyExhausted = key.offset == 1;

		it.skip( key.getKey() );  // TODO: or precondition?

		if ( key.offset <= 0 && it.beginKey() == key.getKey() && key.getKey() != allKeys.begin )
			--it;

		ExtStringRef keykey = key.getKey();
		bool keyNeedsCopy = false;

		// Invariant: it.beginKey() <= keykey && keykey <= it.endKey() && (key.isBackward() ? it.beginKey() != keykey : it.endKey() != keykey)
		// Maintaining this invariant, we transform the key selector toward firstGreaterOrEqual form until we reach an unknown range or the result
		while (key.offset > 1 && !it.is_unreadable() && !it.is_unknown_range() && it.endKey() < maxKey ) {
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

		if(!alreadyExhausted) {
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
			key.setKey(maxKey); // maxKey is a KeyRef, but points to a LiteralStringRef. TODO: how can we ASSERT this?
			key.offset = 1;
			return;
		}

		while (!it.is_unreadable() && it.is_empty_range() && it.endKey() < maxKey) {
			++it;
			keykey = it.beginKey();
			keyNeedsCopy = true;
		}

		if(keyNeedsCopy) {
			key.setKey(keykey.toArena(key.arena()));
		}
	}

	static KeyRangeRef getKnownKeyRange( RangeResultRef data, KeySelector begin, KeySelector end, Arena& arena ) {
		StringRef beginKey = begin.offset<=1 ? begin.getKey() : allKeys.end;
		ExtStringRef endKey = !data.more && end.offset>=1 ? end.getKey() : allKeys.begin;

		if (data.readToBegin) beginKey = allKeys.begin;
		if (data.readThroughEnd) endKey = allKeys.end;

		if( data.size() ) {
			beginKey = std::min( beginKey, data[0].key );
			if( data.readThrough.present() ) {
				endKey = std::max<ExtStringRef>( endKey, data.readThrough.get() );
			}
			else {
				endKey = !data.more && data.end()[-1].key < endKey ? endKey : ExtStringRef( data.end()[-1].key, 1 );
			}
		}
		if (beginKey >= endKey) return KeyRangeRef();


		return KeyRangeRef( StringRef(arena, beginKey), endKey.toArena(arena));
	}

	// Pre: it points to an unknown range
	// Increments it to point to the unknown range just before the next nontrivial known range (skips over trivial known ranges), but not more than iterationLimit ranges away
	template<class Iter> static int skipUncached( Iter& it, Iter const& end, int iterationLimit ) {
		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;

		ASSERT( !it.is_unreadable() && it.is_unknown_range() );

		// b is the beginning of the most recent contiguous *empty* range
		// e is it.endKey()
		while( it != end && --iterationLimit>=0 ) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { //Assumes no degenerate ranges
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
	// Returns the number of following empty single-key known ranges between it and the next nontrivial known range, but no more than maxClears
	// Leaves `it` in an indeterminate state
	template<class Iter> static int countUncached( Iter&& it, KeyRef maxKey, int maxClears ) {
		if (maxClears<=0) return 0;

		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;

		while( e < maxKey ) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { //Assumes no degenerate ranges
					return singleEmpty;
				}
				singleEmpty++;
				if( singleEmpty >= maxClears )
					return maxClears;
			} else
				b = e;
			++it;
			e = it.endKey();
		}
		return singleEmpty;
	}
	
	static void setRequestLimits(GetRangeLimits &requestLimit, int64_t additionalRows, int offset, int requestCount) {
		requestLimit.minRows = (int)std::min(std::max(1 + additionalRows, (int64_t)offset), (int64_t)std::numeric_limits<int>::max());
		if(requestLimit.hasRowLimit()) {
			requestLimit.rows = (int)std::min(std::max(std::max(1,requestLimit.rows) + additionalRows, (int64_t)offset), (int64_t)std::numeric_limits<int>::max());
		}

		// Calculating request byte limit
		if(requestLimit.bytes==0) {
			requestLimit.bytes = GetRangeLimits::BYTE_LIMIT_UNLIMITED;
			if(!requestLimit.hasRowLimit()) {
				requestLimit.rows = (int)std::min(std::max(std::max(1,requestLimit.rows) + additionalRows, (int64_t)offset), (int64_t)std::numeric_limits<int>::max());
			}
		}
		else if(requestLimit.hasByteLimit()) {
			requestLimit.bytes = std::min(int64_t(requestLimit.bytes)<<std::min(requestCount, 20), (int64_t)CLIENT_KNOBS->REPLY_BYTE_LIMIT);
		}
	}

	//TODO: read to begin, read through end flags for result
	ACTOR template<class Iter> static Future< Standalone<RangeResultRef> > getRangeValue( ReadYourWritesTransaction *ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, Iter* pit ) {
		state Iter& it(*pit);
		state Iter itEnd(*pit);
		state Standalone<RangeResultRef> result;
		state int64_t additionalRows = 0;
		state int itemsPastEnd = 0;
		state int requestCount = 0;
		state bool readToBegin = false;
		state bool readThroughEnd = false;
		state int actualBeginOffset = begin.offset;
		state int actualEndOffset = end.offset;
		//state UID randomID = nondeterministicRandom()->randomUniqueID();

		resolveKeySelectorFromCache( begin, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset );
		resolveKeySelectorFromCache( end, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset );

		if( actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey() ) {
			return RangeResultRef(false, false);
		}
		else if( ( begin.isFirstGreaterOrEqual() && begin.getKey() == ryw->getMaxReadKey() ) 
			|| ( end.isFirstGreaterOrEqual() && end.getKey() == allKeys.begin ) )
		{
			return RangeResultRef(readToBegin, readThroughEnd);
		}

		if( !end.isFirstGreaterOrEqual() && begin.getKey() > end.getKey() ) {
			Key resolvedEnd = wait( read( ryw, GetKeyReq(end), pit ) );
			if( resolvedEnd == allKeys.begin )
				readToBegin = true;
			if( resolvedEnd == ryw->getMaxReadKey() )
				readThroughEnd = true;

			if( begin.getKey() >= resolvedEnd && !begin.isBackward() ) {
				return RangeResultRef(false, false);
			}
			else if( resolvedEnd == allKeys.begin ) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}
			
			resolveKeySelectorFromCache( begin, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset );
			resolveKeySelectorFromCache( end, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset );
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

			if( !result.size() && actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey() ) {
				return RangeResultRef(false, false);
			}

			if( end.offset <= 1 && end.getKey() == allKeys.begin ) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}
			
			if( ( begin.offset >= end.offset && begin.getKey() >= end.getKey() ) ||
				( begin.offset >= 1 && begin.getKey() >= ryw->getMaxReadKey() ) ) {
				if( end.isFirstGreaterOrEqual() ) break;
				if( !result.size() ) break;
				Key resolvedEnd = wait( read( ryw, GetKeyReq(end), pit ) ); //do not worry about iterator invalidation, because we are breaking for the loop
				if( resolvedEnd == allKeys.begin )
					readToBegin = true;
				if( resolvedEnd == ryw->getMaxReadKey() )
					readThroughEnd = true;
				end = firstGreaterOrEqual( resolvedEnd );
				break;
			}

			if( !it.is_unreadable() && !it.is_unknown_range() && it.beginKey() > itEnd.beginKey() ) {
				if( end.isFirstGreaterOrEqual() ) break;
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if( limits.isReached() && itemsPastEnd >= 1-end.offset ) break;

			if (it == itEnd && ((!it.is_unreadable() && !it.is_unknown_range()) || (begin.offset > 0 && end.isFirstGreaterOrEqual() && end.getKey() == it.beginKey()))) break;

			if (it.is_unknown_range()) {
				if( limits.hasByteLimit() && result.size() && itemsPastEnd >= 1-end.offset ) {
					result.more = true;
					break;
				}

				Iter ucEnd(it);
				int singleClears = 0;
				int clearLimit = requestCount ? 1 << std::min(requestCount, 20) : 0;
				if( it.beginKey() < itEnd.beginKey() )
					singleClears = std::min(skipUncached(ucEnd, itEnd, BUGGIFY ? 0 : clearLimit + 100), clearLimit);
				
				state KeySelector read_end;
				if ( ucEnd!=itEnd ) {
					Key k = ucEnd.endKey().toStandaloneStringRef();
					read_end = KeySelector(firstGreaterOrEqual(k), k.arena());
					if( end.offset < 1 ) additionalRows += 1 - end.offset; // extra for items past end
				} else if( end.offset < 1 ) {
					read_end = KeySelector(firstGreaterOrEqual(end.getKey()), end.arena());
					additionalRows += 1 - end.offset;
				} else {
					read_end = end;
					if( end.offset > 1 ) {
						singleClears += countUncached( std::move(ucEnd), ryw->getMaxReadKey(), clearLimit-singleClears);
						read_end.offset += singleClears;
					}
				}

				additionalRows += singleClears;

				state KeySelector read_begin;
				if (begin.isFirstGreaterOrEqual()) {
					Key k = it.beginKey() > begin.getKey() ? it.beginKey().toStandaloneStringRef() : Key(begin.getKey(), begin.arena());
					begin = KeySelector(firstGreaterOrEqual(k), k.arena());
					read_begin = begin;
				} else if( begin.offset > 1 ) {
					read_begin = KeySelector(firstGreaterOrEqual(begin.getKey()), begin.arena());
					additionalRows += begin.offset - 1;
				} else {
					read_begin = begin;
					ucEnd = it;

					singleClears = countUncachedBack(std::move(ucEnd), clearLimit);
					read_begin.offset -= singleClears;
					additionalRows += singleClears;
				}

				if(read_end.getKey() < read_begin.getKey()) {
					read_end.setKey(read_begin.getKey());
					read_end.arena().dependsOn(read_begin.arena());
				}

				state GetRangeLimits requestLimit = limits;
				setRequestLimits(requestLimit, additionalRows, 2-read_begin.offset, requestCount);
				requestCount++;

				ASSERT( !requestLimit.hasRowLimit() || requestLimit.rows > 0 );
				ASSERT( requestLimit.hasRowLimit() || requestLimit.hasByteLimit() );
				
				//TraceEvent("RYWIssuing", randomID).detail("Begin", read_begin.toString()).detail("End", read_end.toString()).detail("Bytes", requestLimit.bytes).detail("Rows", requestLimit.rows).detail("Limits", limits.bytes).detail("Reached", limits.isReached()).detail("RequestCount", requestCount).detail("SingleClears", singleClears).detail("UcEnd", ucEnd.beginKey()).detail("MinRows", requestLimit.minRows);

				additionalRows = 0;
				Standalone<RangeResultRef> snapshot_read = wait( ryw->tr.getRange( read_begin, read_end, requestLimit, true, false ) );
				KeyRangeRef range = getKnownKeyRange( snapshot_read, read_begin, read_end, ryw->arena );
				
				//TraceEvent("RYWCacheInsert", randomID).detail("Range", range).detail("ExpectedSize", snapshot_read.expectedSize()).detail("Rows", snapshot_read.size()).detail("Results", snapshot_read).detail("More", snapshot_read.more).detail("ReadToBegin", snapshot_read.readToBegin).detail("ReadThroughEnd", snapshot_read.readThroughEnd).detail("ReadThrough", snapshot_read.readThrough);

				if( ryw->cache.insert( range, snapshot_read ) )
					ryw->arena.dependsOn(snapshot_read.arena());

				// TODO: Is there a more efficient way to deal with invalidation?
				resolveKeySelectorFromCache( begin, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset );
				resolveKeySelectorFromCache( end, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset );
			} else if (it.is_kv()) {
				KeyValueRef const* start = it.kv(ryw->arena);
				if (start == nullptr) {
					++it;
					continue;
				}
				it.skipContiguous( end.isFirstGreaterOrEqual() ? end.getKey() : ryw->getMaxReadKey() ); //not technically correct since this would add end.getKey(), but that is protected above

				int maxCount = it.kv(ryw->arena) - start + 1;
				int count = 0;
				for(; count < maxCount && !limits.isReached(); count++ ) {
					limits.decrement(start[count]);
				}

				itemsPastEnd += maxCount - count;
				
				//TraceEvent("RYWaddKV", randomID).detail("Key", it.beginKey()).detail("Count", count).detail("MaxCount", maxCount).detail("ItemsPastEnd", itemsPastEnd);
				if( count ) result.append( result.arena(), start, count );
				++it;
			} else
				++it;
		}

		result.more = result.more || limits.isReached();

		if( end.isFirstGreaterOrEqual() ) {
			int keepItems = std::lower_bound( result.begin(), result.end(), end.getKey(), KeyValueRef::OrderByKey() ) - result.begin();
			if( keepItems < result.size() )
				result.more = false;
			result.resize( result.arena(), keepItems );
		}

		result.readToBegin = readToBegin;
		result.readThroughEnd = !result.more && readThroughEnd;
		result.arena().dependsOn( ryw->arena );
		
		return result;
	}

	static KeyRangeRef getKnownKeyRangeBack( RangeResultRef data, KeySelector begin, KeySelector end, Arena& arena ) {
		StringRef beginKey = !data.more && begin.offset<=1 ? begin.getKey() : allKeys.end;
		ExtStringRef endKey = end.offset>=1 ? end.getKey() : allKeys.begin;

		if (data.readToBegin) beginKey = allKeys.begin;
		if (data.readThroughEnd) endKey = allKeys.end;

		if( data.size() ) {
			if( data.readThrough.present() ) {
				beginKey = std::min( data.readThrough.get(), beginKey );
			}
			else {
				beginKey = !data.more && data.end()[-1].key > beginKey ? beginKey : data.end()[-1].key;
			}
			
			endKey = data[0].key < endKey ? endKey : ExtStringRef( data[0].key, 1 );
		}
		if (beginKey >= endKey) return KeyRangeRef();

		return KeyRangeRef( StringRef(arena, beginKey), endKey.toArena(arena));
	}

	// Pre: it points to an unknown range
	// Decrements it to point to the unknown range just before the last nontrivial known range (skips over trivial known ranges), but not more than iterationLimit ranges away
	// Returns the number of single-key empty ranges skipped
	template<class Iter> static int skipUncachedBack( Iter& it, Iter const& end, int iterationLimit ) {
		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;
		ASSERT(!it.is_unreadable() && it.is_unknown_range());

		// b == it.beginKey()
		// e is the end of the contiguous empty range containing it
		while( it != end && --iterationLimit>=0) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { //Assumes no degenerate ranges
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
	// Returns the number of preceding empty single-key known ranges between it and the previous nontrivial known range, but no more than maxClears
	// Leaves it in an indeterminate state
	template<class Iter> static int countUncachedBack( Iter&& it, int maxClears ) {
		if (maxClears <= 0) return 0;
		ExtStringRef b = it.beginKey();
		ExtStringRef e = it.endKey();
		int singleEmpty = 0;
		while( b > allKeys.begin ) {
			if (it.is_unreadable() || it.is_empty_range()) {
				if (it.is_unreadable() || !e.isKeyAfter(b)) { //Assumes no degenerate ranges
					return singleEmpty;
				}
				singleEmpty++;
				if( singleEmpty >= maxClears )
					return maxClears;
			} else
				e = b;
			--it;
			b = it.beginKey();
		}
		return singleEmpty;
	}

	ACTOR template<class Iter> static Future< Standalone<RangeResultRef> > getRangeValueBack( ReadYourWritesTransaction *ryw, KeySelector begin, KeySelector end, GetRangeLimits limits, Iter* pit ) {
		state Iter& it(*pit);
		state Iter itEnd(*pit);
		state Standalone<RangeResultRef> result;
		state int64_t additionalRows = 0;
		state int itemsPastBegin = 0;
		state int requestCount = 0;
		state bool readToBegin = false;
		state bool readThroughEnd = false;
		state int actualBeginOffset = begin.offset;
		state int actualEndOffset = end.offset;
		//state UID randomID = nondeterministicRandom()->randomUniqueID();

		resolveKeySelectorFromCache( end, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset );
		resolveKeySelectorFromCache( begin, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset );

		if( actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey() ) {
			return RangeResultRef(false, false);
		}
		else if( ( begin.isFirstGreaterOrEqual() && begin.getKey() == ryw->getMaxReadKey() ) 
			|| ( end.isFirstGreaterOrEqual() && end.getKey() == allKeys.begin ) )
		{
			return RangeResultRef(readToBegin, readThroughEnd);
		}

		if( !begin.isFirstGreaterOrEqual() && begin.getKey() > end.getKey() ) {
			Key resolvedBegin = wait( read( ryw, GetKeyReq(begin), pit ) );
			if( resolvedBegin == allKeys.begin )
				readToBegin = true;
			if( resolvedBegin == ryw->getMaxReadKey() )
				readThroughEnd = true;

			if( resolvedBegin >= end.getKey() && end.offset <= 1 ) {
				return RangeResultRef(false, false);
			}
			else if( resolvedBegin == ryw->getMaxReadKey() ) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}
			
			resolveKeySelectorFromCache( end, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset );
			resolveKeySelectorFromCache( begin, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset );
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

			if(!result.size() && actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
				return RangeResultRef(false, false);
			}
			
			if( !begin.isBackward() && begin.getKey() >= ryw->getMaxReadKey() ) {
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if( ( begin.offset >= end.offset && begin.getKey() >= end.getKey() ) ||
				( end.offset <= 1 && end.getKey() == allKeys.begin ) ) {
				if( begin.isFirstGreaterOrEqual() ) break;
				if( !result.size() ) break;
				Key resolvedBegin = wait( read( ryw, GetKeyReq(begin), pit ) ); //do not worry about iterator invalidation, because we are breaking for the loop
				if( resolvedBegin == allKeys.begin )
					readToBegin = true;
				if( resolvedBegin == ryw->getMaxReadKey() )
					readThroughEnd = true;
				begin = firstGreaterOrEqual( resolvedBegin );
				break;
			}
			
			if (itemsPastBegin >= begin.offset - 1 && !it.is_unreadable() && !it.is_unknown_range() && it.beginKey() < itEnd.beginKey()) {
				if( begin.isFirstGreaterOrEqual() ) break;
				return RangeResultRef(readToBegin, readThroughEnd);
			}

			if( limits.isReached() && itemsPastBegin >= begin.offset-1 ) break;

			if( end.isFirstGreaterOrEqual() && end.getKey() == it.beginKey() ) {
				if( itemsPastBegin >= begin.offset-1 && it == itEnd) break;
				--it;
			}

			if (it.is_unknown_range()) {
				if( limits.hasByteLimit() && result.size() && itemsPastBegin >= begin.offset-1 ) {
					result.more = true;
					break;
				}

				Iter ucEnd(it);
				int singleClears = 0;
				int clearLimit = requestCount ? 1 << std::min(requestCount, 20) : 0;
				if( it.beginKey() > itEnd.beginKey() )
					singleClears = std::min(skipUncachedBack(ucEnd, itEnd, BUGGIFY ? 0 : clearLimit+100), clearLimit);
				
				state KeySelector read_begin;
				if ( ucEnd!=itEnd ) {
					Key k = ucEnd.beginKey().toStandaloneStringRef();
					read_begin = KeySelector(firstGreaterOrEqual(k), k.arena());
					if( begin.offset > 1 ) additionalRows += begin.offset - 1; // extra for items past end
				} else if( begin.offset > 1 ) {
					read_begin = KeySelector(firstGreaterOrEqual( begin.getKey() ), begin.arena());
					additionalRows += begin.offset - 1;
				} else {
					read_begin = begin;
					if( begin.offset < 1 ) {
						singleClears += countUncachedBack(std::move(ucEnd), clearLimit-singleClears);
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

				if(read_begin.getKey() > read_end.getKey()) {
					read_begin.setKey(read_end.getKey());
					read_begin.arena().dependsOn(read_end.arena());
				}

				state GetRangeLimits requestLimit = limits;
				setRequestLimits(requestLimit, additionalRows, read_end.offset, requestCount);
				requestCount++;

				ASSERT( !requestLimit.hasRowLimit() || requestLimit.rows > 0 );
				ASSERT( requestLimit.hasRowLimit() || requestLimit.hasByteLimit() );

				//TraceEvent("RYWIssuing", randomID).detail("Begin", read_begin.toString()).detail("End", read_end.toString()).detail("Bytes", requestLimit.bytes).detail("Rows", requestLimit.rows).detail("Limits", limits.bytes).detail("Reached", limits.isReached()).detail("RequestCount", requestCount).detail("SingleClears", singleClears).detail("UcEnd", ucEnd.beginKey()).detail("MinRows", requestLimit.minRows);

				additionalRows = 0;
				Standalone<RangeResultRef> snapshot_read = wait( ryw->tr.getRange( read_begin, read_end, requestLimit, true, true ) );
				KeyRangeRef range = getKnownKeyRangeBack( snapshot_read, read_begin, read_end, ryw->arena );

				//TraceEvent("RYWCacheInsert", randomID).detail("Range", range).detail("ExpectedSize", snapshot_read.expectedSize()).detail("Rows", snapshot_read.size()).detail("Results", snapshot_read).detail("More", snapshot_read.more).detail("ReadToBegin", snapshot_read.readToBegin).detail("ReadThroughEnd", snapshot_read.readThroughEnd).detail("ReadThrough", snapshot_read.readThrough);
				
				RangeResultRef reversed;
				reversed.resize(ryw->arena, snapshot_read.size());
				for( int i = 0; i < snapshot_read.size(); i++ ) {
					reversed[snapshot_read.size()-i-1] = snapshot_read[i];
				}
				
				if( ryw->cache.insert( range, reversed ) )
					ryw->arena.dependsOn(snapshot_read.arena());

				// TODO: Is there a more efficient way to deal with invalidation?
				resolveKeySelectorFromCache( end, it, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualEndOffset );
				resolveKeySelectorFromCache( begin, itEnd, ryw->getMaxReadKey(), &readToBegin, &readThroughEnd, &actualBeginOffset );
			} else {
				KeyValueRef const* end = it.is_kv() ? it.kv(ryw->arena) : nullptr;
				if (end != nullptr) {
					it.skipContiguousBack( begin.isFirstGreaterOrEqual() ? begin.getKey() : allKeys.begin );
					KeyValueRef const* start = it.kv(ryw->arena);
					ASSERT(start != nullptr);

					int maxCount = end - start + 1;
					int count = 0;
					for(; count < maxCount && !limits.isReached(); count++ ) {
						limits.decrement(start[maxCount-count-1]);
					}

					itemsPastBegin += maxCount - count;
					//TraceEvent("RYWaddKV", randomID).detail("Key", it.beginKey()).detail("Count", count).detail("MaxCount", maxCount).detail("ItemsPastBegin", itemsPastBegin);
					if( count ) {
						int size = result.size();
						result.resize(result.arena(),size+count);
						for( int i = 0; i < count; i++ ) {
							result[size + i] = start[maxCount-i-1];
						}
					}
				}
				if (it == itEnd) break;
				--it;
			}
		}

		result.more = result.more || limits.isReached();

		if( begin.isFirstGreaterOrEqual() ) {
			int keepItems = result.rend() - std::lower_bound( result.rbegin(), result.rend(), begin.getKey(), KeyValueRef::OrderByKey());
			if( keepItems < result.size() )
				result.more = false;

			result.resize( result.arena(), keepItems );
		}

		result.readToBegin = !result.more && readToBegin;
		result.readThroughEnd = readThroughEnd;
		result.arena().dependsOn( ryw->arena );

		return result;
	}

	static void triggerWatches(ReadYourWritesTransaction *ryw, KeyRangeRef range, Optional<ValueRef> val, bool valueKnown = true) {
		for(auto it = ryw->watchMap.lower_bound(range.begin); it != ryw->watchMap.end() && it->key < range.end; ) {
			auto itCopy = it;
			++it;

			ASSERT( itCopy->value.size() );
			TEST( itCopy->value.size() > 1 ); //Multiple watches on the same key triggered by RYOW
			
			for( int i = 0; i < itCopy->value.size(); i++ ) {
				if(itCopy->value[i]->onChangeTrigger.isSet()) {
					swapAndPop(&itCopy->value, i--);
				} else if( !valueKnown || 
						   (itCopy->value[i]->setPresent && (itCopy->value[i]->setValue.present() != val.present() || (val.present() && itCopy->value[i]->setValue.get() != val.get()))) ||
						   (itCopy->value[i]->valuePresent && (itCopy->value[i]->value.present() != val.present() || (val.present() && itCopy->value[i]->value.get() != val.get()))) ) {
					itCopy->value[i]->onChangeTrigger.send(Void());
					swapAndPop(&itCopy->value, i--);
				} else {
					itCopy->value[i]->setPresent = true;
					itCopy->value[i]->setValue = val.castTo<Value>();
				}
			}

			if( itCopy->value.size() == 0 )
				ryw->watchMap.erase(itCopy);
		}
	}

	static void triggerWatches(ReadYourWritesTransaction *ryw, KeyRef key, Optional<ValueRef> val, bool valueKnown = true) {
		triggerWatches(ryw, singleKeyRange(key), val, valueKnown);
	}

	ACTOR static Future<Void> watch( ReadYourWritesTransaction *ryw, Key key ) {
		state Future<Optional<Value>> val;
		state Future<Void> watchFuture;
		state Reference<Watch> watch(new Watch(key));
		state Promise<Void> done;

		ryw->reading.add( done.getFuture() );

		if(!ryw->options.readYourWritesDisabled) {
			ryw->watchMap[key].push_back(watch);
			val = readWithConflictRange( ryw, GetValueReq(key), false );
		} else {
			ryw->approximateSize += 2 * key.expectedSize() + 1;
			val = ryw->tr.get(key);
		}

		try {
			wait(ryw->resetPromise.getFuture() || success(val) || watch->onChangeTrigger.getFuture());
		} catch( Error &e ) {
			done.send(Void());
			throw;
		}
		
		if( watch->onChangeTrigger.getFuture().isReady() ) {
			done.send(Void());
			if( watch->onChangeTrigger.getFuture().isError() )
				throw watch->onChangeTrigger.getFuture().getError();
			return Void();
		}

		watch->valuePresent = true;
		watch->value = val.get();

		if( watch->setPresent && ( watch->setValue.present() != watch->value.present() || (watch->value.present() && watch->setValue.get() != watch->value.get()) ) ) {
			watch->onChangeTrigger.send(Void());
			done.send(Void());
			return Void();
		}

		try {
			watchFuture = ryw->tr.watch(watch); // throws if there are too many outstanding watches	
		} catch( Error &e ) {
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
		if (!ryw->resetPromise.isSet()) ryw->resetPromise.sendError(transaction_timed_out());
		wait(delay(deterministicRandom()->random01() * 5));
		TraceEvent("ClientBuggifyInFlightCommit");
		wait(ryw->tr.commit());
	}

	ACTOR static Future<Void> commit( ReadYourWritesTransaction *ryw ) {
		try {
			ryw->commitStarted = true;
			
			Future<Void> ready = ryw->reading;
			wait( ryw->resetPromise.getFuture() || ready );

			if( ryw->options.readYourWritesDisabled ) {

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
				wait( ryw->resetPromise.getFuture() || ryw->tr.commit() );

				ryw->debugLogRetries();

				if(!ryw->tr.apiVersionAtLeast(410)) {
					ryw->reset();
				}

				return Void();
			}

			ryw->writeRangeToNativeTransaction(KeyRangeRef(StringRef(), allKeys.end));

			auto conflictRanges = ryw->readConflicts.ranges();
			for( auto iter = conflictRanges.begin(); iter != conflictRanges.end(); ++iter ) {
				if( iter->value() ) {
					ryw->tr.addReadConflictRange( iter->range() );
				}
			}

			if (CLIENT_BUGGIFY && ryw->options.timeoutInSeconds > 0) {
				simulateTimeoutInFlightCommit(ryw);
				throw transaction_timed_out();
			}
			wait( ryw->resetPromise.getFuture() || ryw->tr.commit() );

			ryw->debugLogRetries();
			if(!ryw->tr.apiVersionAtLeast(410)) {
				ryw->reset();
			}
			
			return Void();
		} catch( Error &e ) {
			if(!ryw->tr.apiVersionAtLeast(410)) {
				ryw->commitStarted = false;
				if( !ryw->resetPromise.isSet() ) {
					ryw->tr.reset();
					ryw->resetRyow();
				}
			}

			throw;
		}
	}

	ACTOR static Future<Void> onError( ReadYourWritesTransaction *ryw, Error e ) {
		try {
			if ( ryw->resetPromise.isSet() ) {
				throw ryw->resetPromise.getFuture().getError();
			}

			bool retry_limit_hit = ryw->options.maxRetries != -1 && ryw->retries >= ryw->options.maxRetries;
			if (ryw->retries < std::numeric_limits<int>::max()) ryw->retries++;
			if(retry_limit_hit) {
				throw e;
			}

			wait( ryw->resetPromise.getFuture() || ryw->tr.onError(e) );

			ryw->debugLogRetries(e);

			ryw->resetRyow();
			return Void();
		} catch( Error &e ) {
			if ( !ryw->resetPromise.isSet() ) {
				if(ryw->tr.apiVersionAtLeast(610)) {
					ryw->resetPromise.sendError(transaction_cancelled());
				}
				else {
					ryw->resetRyow();
				}
			}
			if( e.code() == error_code_broken_promise )
				throw transaction_cancelled();
			throw;
		}
	}

	ACTOR static Future<Version> getReadVersion(ReadYourWritesTransaction* ryw) {
		choose{
			when(Version v = wait(ryw->tr.getReadVersion())) {
				return v;
			}

			when(wait(ryw->resetPromise.getFuture())) {
				throw internal_error();
			}
		}
	}
};

ReadYourWritesTransaction::ReadYourWritesTransaction(Database const& cx)
  : cache(&arena), writes(&arena), tr(cx), retries(0), approximateSize(0), creationTime(now()), commitStarted(false),
    options(tr), deferredError(cx->deferredError), versionStampFuture(tr.getVersionstamp()) {
	std::copy(cx.getTransactionDefaults().begin(), cx.getTransactionDefaults().end(),
	          std::back_inserter(persistentOptions));
	applyPersistentOptions();
}

ACTOR Future<Void> timebomb(double endTime, Promise<Void> resetPromise) {
	while(now() < endTime) {
		wait( delayUntil( std::min(endTime + 0.0001, now() + CLIENT_KNOBS->TRANSACTION_TIMEOUT_DELAY_INTERVAL) ) );
	}
	if( !resetPromise.isSet() )
		resetPromise.sendError(transaction_timed_out());
	throw transaction_timed_out();	
}

void ReadYourWritesTransaction::resetTimeout() {
	timeoutActor = options.timeoutInSeconds == 0.0 ? Void() : timebomb(options.timeoutInSeconds + creationTime, resetPromise);
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
		Value output = StringRef(json_spirit::write_string(json_spirit::mValue(statusObj), json_spirit::Output_options::none));
		return output;
	}
	catch (std::exception& e){
		TraceEvent(SevError, "UnableToUnparseStatusJSON").detail("What", e.what());
		throw internal_error();
	}
}

ACTOR Future<Optional<Value>> getJSON(Database db) {
	StatusObject statusObj = wait(StatusClient::statusFetcher(db));
	return getValueFromJSON(statusObj);
}

ACTOR Future<Standalone<RangeResultRef>> getWorkerInterfaces (Reference<ClusterConnectionFile> clusterFile){
	state Reference<AsyncVar<Optional<ClusterInterface>>> clusterInterface(new AsyncVar<Optional<ClusterInterface>>);
	state Future<Void> leaderMon = monitorLeader<ClusterInterface>(clusterFile, clusterInterface);

	loop{
		choose {
			when( vector<ClientWorkerInterface> workers = wait( clusterInterface->get().present() ? brokenPromiseToNever( clusterInterface->get().get().getClientWorkers.getReply( GetClientWorkersRequest() ) ) : Never() ) ) {
				Standalone<RangeResultRef> result;
				for(auto& it : workers) {
					result.push_back_deep(result.arena(), KeyValueRef(it.address().toString(), BinaryWriter::toValue(it, IncludeVersion())));
				}
			
				return result;
			}
			when( wait(clusterInterface->onChange()) ) {}	
		}
	}
}

Future< Optional<Value> > ReadYourWritesTransaction::get( const Key& key, bool snapshot ) {
	TEST(true);

	if (getDatabase()->apiVersionAtLeast(630)) {
		if (specialKeys.contains(key)) {
			TEST(true); // Special keys get
			return getDatabase()->specialKeySpace->get(this, key);
		}
	} else {
		if (key == LiteralStringRef("\xff\xff/status/json")) {
			if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionFile()) {
				return getJSON(tr.getDatabase());
			} else {
				return Optional<Value>();
			}
		}

		if (key == LiteralStringRef("\xff\xff/cluster_file_path")) {
			try {
				if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionFile()) {
					Optional<Value> output = StringRef(tr.getDatabase()->getConnectionFile()->getFilename());
					return output;
				}
			} catch (Error& e) {
				return e;
			}
			return Optional<Value>();
		}

		if (key == LiteralStringRef("\xff\xff/connection_string")) {
			try {
				if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionFile()) {
					Reference<ClusterConnectionFile> f = tr.getDatabase()->getConnectionFile();
					Optional<Value> output = StringRef(f->getConnectionString().toString());
					return output;
				}
			} catch (Error& e) {
				return e;
			}
			return Optional<Value>();
		}
	}

	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();
	
	if(key >= getMaxReadKey() && key != metadataVersionKey)
		return key_outside_legal_range();

	//There are no keys in the database with size greater than KEY_SIZE_LIMIT
	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		return Optional<Value>();

	Future< Optional<Value> > result = RYWImpl::readWithConflictRange( this, RYWImpl::GetValueReq(key), snapshot );
	reading.add( success( result ) );
	return result;
}

Future< Key > ReadYourWritesTransaction::getKey( const KeySelector& key, bool snapshot ) {
	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();
	
	if(key.getKey() > getMaxReadKey())
		return key_outside_legal_range();

	Future< Key > result = RYWImpl::readWithConflictRange(this, RYWImpl::GetKeyReq(key), snapshot);
	reading.add( success( result ) );
	return result;
}

Future< Standalone<RangeResultRef> > ReadYourWritesTransaction::getRange( 
	KeySelector begin, 
	KeySelector end, 
	GetRangeLimits limits,
	bool snapshot,
	bool reverse )
{
	if (getDatabase()->apiVersionAtLeast(630)) {
		if (specialKeys.contains(begin.getKey()) && end.getKey() <= specialKeys.end) {
			TEST(true); // Special key space get range
			return getDatabase()->specialKeySpace->getRange(this, begin, end, limits, reverse);
		}
	} else {
		if (begin.getKey() == LiteralStringRef("\xff\xff/worker_interfaces")) {
			if (tr.getDatabase().getPtr() && tr.getDatabase()->getConnectionFile()) {
				return getWorkerInterfaces(tr.getDatabase()->getConnectionFile());
			} else {
				return Standalone<RangeResultRef>();
			}
		}
	}

	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();
	
	KeyRef maxKey = getMaxReadKey();
	if(begin.getKey() > maxKey || end.getKey() > maxKey)
		return key_outside_legal_range();

	//This optimization prevents NULL operations from being added to the conflict range
	if( limits.isReached() ) {
		TEST(true); // RYW range read limit 0
		return Standalone<RangeResultRef>();
	}

	if( !limits.isValid() )
		return range_limits_invalid();

	if( begin.orEqual )
		begin.removeOrEqual(begin.arena());

	if( end.orEqual )
		end.removeOrEqual(end.arena());

	if( begin.offset >= end.offset && begin.getKey() >= end.getKey() ) {
		TEST(true); // RYW range inverted
		return Standalone<RangeResultRef>();
	}

	Future< Standalone<RangeResultRef> > result = reverse 
		? RYWImpl::readWithConflictRange( this, RYWImpl::GetRangeReq<true>(begin, end, limits), snapshot )
		: RYWImpl::readWithConflictRange( this, RYWImpl::GetRangeReq<false>(begin, end, limits), snapshot );

	reading.add( success( result ) );
	return result;
}

Future< Standalone<RangeResultRef> > ReadYourWritesTransaction::getRange( 
	const KeySelector& begin, 
	const KeySelector& end, 
	int limit,
	bool snapshot,
	bool reverse )
{
	return getRange( begin, end, GetRangeLimits( limit ), snapshot, reverse );
}

Future< Standalone<VectorRef<const char*> >> ReadYourWritesTransaction::getAddressesForKey( const Key& key ) {
	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}
	
	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();

	// If key >= allKeys.end, then our resulting address vector will be empty.
	
	Future< Standalone<VectorRef<const char*> >> result = waitOrError(tr.getAddressesForKey(key), resetPromise.getFuture());
	reading.add( success( result ) ); 
	return result;
}

Future<int64_t> ReadYourWritesTransaction::getEstimatedRangeSizeBytes(const KeyRangeRef& keys) {
	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}
	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();

	return map(waitOrError(tr.getStorageMetrics(keys, -1), resetPromise.getFuture()), [](const StorageMetrics& m) { return m.bytes; });
}

void ReadYourWritesTransaction::addReadConflictRange( KeyRangeRef const& keys ) {
	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}
	
	if (tr.apiVersionAtLeast(300)) {
		if ((keys.begin > getMaxReadKey() || keys.end > getMaxReadKey()) && (keys.begin != metadataVersionKey || keys.end != metadataVersionKeyEnd)) {
			throw key_outside_legal_range();
		}
	}

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT, so if range contains large keys
	//we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	if(begin.size() > (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		begin = begin.substr(0, (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);
	if(end.size() > (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		end = end.substr(0, (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);

	KeyRangeRef r = KeyRangeRef(begin, end);

	if(r.empty()) {
		return;
	}

	if(options.readYourWritesDisabled) {
		approximateSize += r.expectedSize() + sizeof(KeyRangeRef);
		tr.addReadConflictRange(r);
		return;
	}

	WriteMap::iterator it( &writes );
	KeyRangeRef readRange( arena, r );
	it.skip( readRange.begin );
	updateConflictMap(readRange, it);
}

void ReadYourWritesTransaction::updateConflictMap( KeyRef const& key, WriteMap::iterator& it ) {
	//it.skip( key );
	//ASSERT( it.beginKey() <= key && key < it.endKey() );
	if( it.is_unmodified_range() || ( it.is_operation() && !it.is_independent() ) ) {
		approximateSize += 2 * key.expectedSize() + 1 + sizeof(KeyRangeRef);
		readConflicts.insert( singleKeyRange( key, arena ), true );
	}
}

void ReadYourWritesTransaction::updateConflictMap( KeyRangeRef const& keys, WriteMap::iterator& it ) {
	//it.skip( keys.begin );
	//ASSERT( it.beginKey() <= keys.begin && keys.begin < it.endKey() );
	for(; it.beginKey() < keys.end; ++it ) {
		if( it.is_unmodified_range() || ( it.is_operation() && !it.is_independent() ) ) {
			KeyRangeRef insert_range = KeyRangeRef( std::max( keys.begin, it.beginKey().toArenaOrRef( arena ) ), std::min( keys.end, it.endKey().toArenaOrRef( arena ) ) );
			if (!insert_range.empty()) {
				approximateSize += keys.expectedSize() + sizeof(KeyRangeRef);
				readConflicts.insert( insert_range, true );
			}
		}
	}
}

void ReadYourWritesTransaction::writeRangeToNativeTransaction(KeyRangeRef const& keys) {
	WriteMap::iterator it( &writes );
	it.skip(keys.begin);

	bool inClearRange = false;
	ExtStringRef clearBegin;

	//Clear ranges must be done first because of keys that are both cleared and set to a new value
	for(; it.beginKey() < keys.end; ++it) {
		if( it.is_cleared_range() && !inClearRange ) {
			clearBegin = std::max(ExtStringRef(keys.begin), it.beginKey());
			inClearRange = true;
		} else if( !it.is_cleared_range() && inClearRange ) {
			tr.clear(KeyRangeRef(clearBegin.toArenaOrRef(arena), it.beginKey().toArenaOrRef(arena)), false);
			inClearRange = false;
		}
	}

	if (inClearRange) {
		tr.clear(KeyRangeRef(clearBegin.toArenaOrRef(arena), keys.end), false);
	}

	it.skip(keys.begin);

	bool inConflictRange = false;
	ExtStringRef conflictBegin;

	for(; it.beginKey() < keys.end; ++it) {
		if( it.is_conflict_range() && !inConflictRange ) {
			conflictBegin = std::max(ExtStringRef(keys.begin), it.beginKey());
			inConflictRange = true;
		} else if( !it.is_conflict_range() && inConflictRange ) {
			tr.addWriteConflictRange(KeyRangeRef(conflictBegin.toArenaOrRef(arena), it.beginKey().toArenaOrRef(arena)));
			inConflictRange = false;
		}

		//SOMEDAY: make atomicOp take set to avoid switch
		if( it.is_operation() ) {
			auto op = it.op();
			for( int i = 0; i < op.size(); ++i) {
				switch(op[i].type) {
					case MutationRef::SetValue:
						if (op[i].value.present()) {
							tr.set(it.beginKey().assertRef(), op[i].value.get(), false);
						} else {
							tr.clear(it.beginKey().assertRef(), false);
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
						tr.atomicOp(it.beginKey().assertRef(), op[i].value.get(), op[i].type, false);
						break;
					default:
						break;
				}
			}
		}
	}

	if( inConflictRange ) {
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

void ReadYourWritesTransaction::getWriteConflicts( KeyRangeMap<bool> *result ) {
	WriteMap::iterator it( &writes );
	it.skip(allKeys.begin);

	bool inConflictRange = false;
	ExtStringRef conflictBegin;

	for(; it.beginKey() < getMaxWriteKey(); ++it) {
		if( it.is_conflict_range() && !inConflictRange ) {
			conflictBegin = it.beginKey();
			inConflictRange = true;
		} else if( !it.is_conflict_range() && inConflictRange ) {
			result->insert(  KeyRangeRef( conflictBegin.toArenaOrRef(arena), it.beginKey().toArenaOrRef(arena) ), true );
			inConflictRange = false;
		}
	}

	if( inConflictRange ) {
		result->insert(  KeyRangeRef( conflictBegin.toArenaOrRef(arena), getMaxWriteKey() ), true );
	}
}

Standalone<RangeResultRef> ReadYourWritesTransaction::getReadConflictRangeIntersecting(KeyRangeRef kr) {
	TEST(true); // Special keys read conflict range
	ASSERT(readConflictRangeKeysRange.contains(kr));
	ASSERT(!tr.options.checkWritesEnabled);
	Standalone<RangeResultRef> result;
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
				                             iter->value() ? LiteralStringRef("1") : LiteralStringRef("0")));
			}
		}
	} else {
		CoalescedKeyRefRangeMap<ValueRef> readConflicts{ LiteralStringRef("0"), specialKeys.end };
		for (const auto& range : tr.readConflictRanges())
			readConflicts.insert(range.withPrefix(readConflictRangeKeysRange.begin, result.arena()),
			                     LiteralStringRef("1"));
		for (const auto& range : nativeReadRanges)
			readConflicts.insert(range.withPrefix(readConflictRangeKeysRange.begin, result.arena()),
			                     LiteralStringRef("1"));
		for (const auto& f : tr.getExtraReadConflictRanges()) {
			if (f.isReady() && f.get().first < f.get().second)
				readConflicts.insert(KeyRangeRef(f.get().first, f.get().second)
				                         .withPrefix(readConflictRangeKeysRange.begin, result.arena()),
				                     LiteralStringRef("1"));
		}
		auto beginIter = readConflicts.rangeContaining(kr.begin);
		if (beginIter->begin() != kr.begin) ++beginIter;
		for (auto it = beginIter; it->begin() < kr.end; ++it) {
			result.push_back(result.arena(), KeyValueRef(it->begin(), it->value()));
		}
	}
	return result;
}

Standalone<RangeResultRef> ReadYourWritesTransaction::getWriteConflictRangeIntersecting(KeyRangeRef kr) {
	TEST(true); // Special keys write conflict range
	ASSERT(writeConflictRangeKeysRange.contains(kr));
	Standalone<RangeResultRef> result;

	// Memory owned by result
	CoalescedKeyRefRangeMap<ValueRef> writeConflicts{ LiteralStringRef("0"), specialKeys.end };

	if (!options.readYourWritesDisabled) {
		KeyRangeRef strippedWriteRangePrefix = kr.removePrefix(writeConflictRangeKeysRange.begin);
		WriteMap::iterator it(&writes);
		it.skip(strippedWriteRangePrefix.begin);
		if (it.beginKey() > allKeys.begin) --it;
		for (; it.beginKey() < strippedWriteRangePrefix.end; ++it) {
			if (it.is_conflict_range())
				writeConflicts.insert(
				    KeyRangeRef(it.beginKey().toArena(result.arena()), it.endKey().toArena(result.arena()))
				        .withPrefix(writeConflictRangeKeysRange.begin, result.arena()),
				    LiteralStringRef("1"));
		}
	} else {
		for (const auto& range : tr.writeConflictRanges())
			writeConflicts.insert(range.withPrefix(writeConflictRangeKeysRange.begin, result.arena()),
			                      LiteralStringRef("1"));
		for (const auto& range : nativeWriteRanges)
			writeConflicts.insert(range.withPrefix(writeConflictRangeKeysRange.begin, result.arena()),
			                      LiteralStringRef("1"));
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
			range = getVersionstampKeyRange(result.arena(), k, tr.getCachedReadVersion().orDefault(0), getMaxReadKey());
		}
		writeConflicts.insert(range.withPrefix(writeConflictRangeKeysRange.begin, result.arena()),
		                      LiteralStringRef("1"));
	}

	auto beginIter = writeConflicts.rangeContaining(kr.begin);
	if (beginIter->begin() != kr.begin) ++beginIter;
	for (auto it = beginIter; it->begin() < kr.end; ++it) {
		result.push_back(result.arena(), KeyValueRef(it->begin(), it->value()));
	}

	return result;
}

void ReadYourWritesTransaction::atomicOp( const KeyRef& key, const ValueRef& operand, uint32_t operationType ) {
	bool addWriteConflict = !options.getAndResetWriteConflictDisabled();

	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (key == metadataVersionKey) {
		if(operationType != MutationRef::SetVersionstampedValue || operand != metadataVersionRequiredValue) {
			throw client_invalid_operation();
		}
	}
	else if(key >= getMaxWriteKey()) {
		throw key_outside_legal_range();
	}

	if(!isValidMutationType(operationType) || !isAtomicOp((MutationRef::Type) operationType))
		throw invalid_mutation_type();

	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		throw key_too_large();
	if(operand.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();

	if (tr.apiVersionAtLeast(510)) {
		if (operationType == MutationRef::Min)
			operationType = MutationRef::MinV2;
		else if (operationType == MutationRef::And)
			operationType = MutationRef::AndV2;
	}

	KeyRef k;
	if(!tr.apiVersionAtLeast(520) && operationType == MutationRef::SetVersionstampedKey) {
		k = key.withSuffix( LiteralStringRef("\x00\x00"), arena );
	} else {
		k = KeyRef( arena, key );
	}
	ValueRef v;
	if(!tr.apiVersionAtLeast(520) && operationType == MutationRef::SetVersionstampedValue) {
		v = operand.withSuffix( LiteralStringRef("\x00\x00\x00\x00"), arena );
	} else {
		v = ValueRef( arena, operand );
	}

	if(operationType == MutationRef::SetVersionstampedKey) {
		TEST(options.readYourWritesDisabled); // SetVersionstampedKey without ryw enabled
		// this does validation of the key and needs to be performed before the readYourWritesDisabled path
		KeyRangeRef range = getVersionstampKeyRange(arena, k, tr.getCachedReadVersion().orDefault(0), getMaxReadKey());
		versionStampKeys.push_back(arena, k);
		addWriteConflict = false;
		if(!options.readYourWritesDisabled) {
			writeRangeToNativeTransaction(range);
			writes.addUnmodifiedAndUnreadableRange(range);
		}
		// k is the unversionstamped key provided by the user.  If we've filled in a minimum bound
		// for the versionstamp, we need to make sure that's reflected when we insert it into the
		// WriteMap below.
		transformVersionstampKey( k, tr.getCachedReadVersion().orDefault(0), 0 );
	}

	if(operationType == MutationRef::SetVersionstampedValue) {
		if(v.size() < 4)
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
		return tr.atomicOp(k, v, (MutationRef::Type) operationType, addWriteConflict);
	}

	writes.mutate(k, (MutationRef::Type) operationType, v, addWriteConflict);
	RYWImpl::triggerWatches(this, k, Optional<ValueRef>(), false);
}

void ReadYourWritesTransaction::set( const KeyRef& key, const ValueRef& value ) {
	if (key == LiteralStringRef("\xff\xff/reboot_worker")){
		BinaryReader::fromStringRef<ClientWorkerInterface>(value, IncludeVersion()).reboot.send( RebootRequest() );
		return;
	}
	if (key == LiteralStringRef("\xff\xff/reboot_and_check_worker")){
		BinaryReader::fromStringRef<ClientWorkerInterface>(value, IncludeVersion()).reboot.send( RebootRequest(false, true) );
		return;
	}
	if (key == metadataVersionKey) {
		throw client_invalid_operation();
	}

	bool addWriteConflict = !options.getAndResetWriteConflictDisabled();

	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if(key >= getMaxWriteKey())
		throw key_outside_legal_range();

	approximateSize += key.expectedSize() + value.expectedSize() + sizeof(MutationRef) +
	                   (addWriteConflict ? sizeof(KeyRangeRef) + 2 * key.expectedSize() + 1 : 0);
	if (options.readYourWritesDisabled) {
		return tr.set(key, value, addWriteConflict);
	}

	//TODO: check transaction size here
	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		throw key_too_large();
	if(value.size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT)
		throw value_too_large();
	
	KeyRef k = KeyRef( arena, key );
	ValueRef v = ValueRef( arena, value );

	writes.mutate(k, MutationRef::SetValue, v, addWriteConflict);
	RYWImpl::triggerWatches(this, key, value);
}
	
void ReadYourWritesTransaction::clear( const KeyRangeRef& range ) {
	bool addWriteConflict = !options.getAndResetWriteConflictDisabled();

	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	KeyRef maxKey = getMaxWriteKey();
	if(range.begin > maxKey || range.end > maxKey)
		throw key_outside_legal_range();

	approximateSize += range.expectedSize() + sizeof(MutationRef) +
	                   (addWriteConflict ? sizeof(KeyRangeRef) + range.expectedSize() : 0);
	if (options.readYourWritesDisabled) {
		return tr.clear(range, addWriteConflict);
	}

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT, so if range contains large keys
	//we can translate it to an equivalent one with smaller keys
	KeyRef begin = range.begin;
	KeyRef end = range.end;

	if(begin.size() > (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		begin = begin.substr(0, (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);
	if(end.size() > (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		end = end.substr(0, (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);

	KeyRangeRef r = KeyRangeRef(begin, end);

	if(r.empty()) {
		return;
	}

	r = KeyRangeRef( arena, r );

	writes.clear(r, addWriteConflict);
	RYWImpl::triggerWatches(this, r, Optional<ValueRef>());
}

void ReadYourWritesTransaction::clear( const KeyRef& key ) {
	bool addWriteConflict = !options.getAndResetWriteConflictDisabled();

	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if(key >= getMaxWriteKey())
		throw key_outside_legal_range();

	if(key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		return;

	if( options.readYourWritesDisabled ) {
		return tr.clear(key, addWriteConflict);
	}
	
	KeyRangeRef r = singleKeyRange( key, arena );
	approximateSize += r.expectedSize() + sizeof(KeyRangeRef) +
	                   (addWriteConflict ? sizeof(KeyRangeRef) + r.expectedSize() : 0);

	//SOMEDAY: add an optimized single key clear to write map
	writes.clear(r, addWriteConflict);

	RYWImpl::triggerWatches(this, r, Optional<ValueRef>());
}

Future<Void> ReadYourWritesTransaction::watch(const Key& key) {
	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();
	
	if( options.readYourWritesDisabled )
		return watches_disabled();

	if(key >= allKeys.end || (key >= getMaxReadKey() && key != metadataVersionKey && tr.apiVersionAtLeast(300)))
		return key_outside_legal_range();

	if (key.size() > (key.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		return key_too_large();

	return RYWImpl::watch(this, key);
}

void ReadYourWritesTransaction::addWriteConflictRange(KeyRangeRef const& keys) {
	if(checkUsedDuringCommit()) {
		throw used_during_commit();
	}

	if (tr.apiVersionAtLeast(300)) {
		if (keys.begin > getMaxWriteKey() || keys.end > getMaxWriteKey()) {
			throw key_outside_legal_range();
		}
	}

	//There aren't any keys in the database with size larger than KEY_SIZE_LIMIT, so if range contains large keys
	//we can translate it to an equivalent one with smaller keys
	KeyRef begin = keys.begin;
	KeyRef end = keys.end;

	if(begin.size() > (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		begin = begin.substr(0, (begin.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);
	if(end.size() > (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT))
		end = end.substr(0, (end.startsWith(systemKeys.begin) ? CLIENT_KNOBS->SYSTEM_KEY_SIZE_LIMIT : CLIENT_KNOBS->KEY_SIZE_LIMIT)+1);

	KeyRangeRef r = KeyRangeRef(begin, end);

	if(r.empty()) {
		return;
	}

	approximateSize += r.expectedSize() + sizeof(KeyRangeRef);
	if(options.readYourWritesDisabled) {
		tr.addWriteConflictRange(r);
		return;
	}

	r = KeyRangeRef( arena, r );
	writes.addConflictRange(r);
}

Future<Void> ReadYourWritesTransaction::commit() { 
	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}

	if( resetPromise.isSet() )
		return resetPromise.getFuture().getError();

	return RYWImpl::commit( this );
}

Future<Standalone<StringRef>> ReadYourWritesTransaction::getVersionstamp() {
	if(checkUsedDuringCommit()) {
		return used_during_commit();
	}

	return waitOrError(tr.getVersionstamp(), resetPromise.getFuture());
}

void ReadYourWritesTransaction::setOption( FDBTransactionOptions::Option option, Optional<StringRef> value ) {
	setOptionImpl(option, value);

	if (FDBTransactionOptions::optionInfo.getMustExist(option).persistent) {
		persistentOptions.emplace_back(option, value.castTo<Standalone<StringRef>>());
	}
}

void ReadYourWritesTransaction::setOptionImpl( FDBTransactionOptions::Option option, Optional<StringRef> value ) { 
	switch(option) {
		case FDBTransactionOptions::READ_YOUR_WRITES_DISABLE:
			validateOptionValue(value, false);

			if (!reading.isReady() || !cache.empty() || !writes.empty())
				throw client_invalid_operation();

			options.readYourWritesDisabled = true;
			break;

		case FDBTransactionOptions::READ_AHEAD_DISABLE:
			validateOptionValue(value, false);

			options.readAheadDisabled = true;
			break;

		case FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE:
			validateOptionValue(value, false);
			
			options.nextWriteDisableConflictRange = true;
			break;

		case FDBTransactionOptions::ACCESS_SYSTEM_KEYS:
			validateOptionValue(value, false);

			options.readSystemKeys = true;
			options.writeSystemKeys = true;
			break;

		case FDBTransactionOptions::READ_SYSTEM_KEYS:
			validateOptionValue(value, false);

			options.readSystemKeys = true;
			break;

		case FDBTransactionOptions::TIMEOUT:
			options.timeoutInSeconds = extractIntOption(value, 0, std::numeric_limits<int>::max())/1000.0;
			resetTimeout();
			break;

		case FDBTransactionOptions::RETRY_LIMIT:
			options.maxRetries = (int)extractIntOption(value, -1, std::numeric_limits<int>::max());
			break;

		case FDBTransactionOptions::DEBUG_RETRY_LOGGING:
			options.debugRetryLogging = true;
			if(!transactionDebugInfo) {
				transactionDebugInfo = Reference<TransactionDebugInfo>::addRef(new TransactionDebugInfo());
				transactionDebugInfo->lastRetryLogTime = creationTime;
			}

			transactionDebugInfo->transactionName = value.present() ? value.get().toString() : "";
			break;
		case FDBTransactionOptions::SNAPSHOT_RYW_ENABLE:
			validateOptionValue(value, false);
			
			options.snapshotRywEnabled++;
			break;
		case FDBTransactionOptions::SNAPSHOT_RYW_DISABLE:
			validateOptionValue(value, false);
			
			options.snapshotRywEnabled--;
			break;
		case FDBTransactionOptions::USED_DURING_COMMIT_PROTECTION_DISABLE:
			validateOptionValue(value, false);

			options.disableUsedDuringCommitProtection = true;
			break;
		case FDBTransactionOptions::SPECIAL_KEY_SPACE_RELAXED:
			validateOptionValue(value, false);
			options.specialKeySpaceRelaxed = true;
		default:
			break;
	}

	tr.setOption( option, value );
}

void ReadYourWritesTransaction::operator=(ReadYourWritesTransaction&& r) noexcept {
	cache = std::move( r.cache );
	writes = std::move( r.writes );
	arena = std::move( r.arena );
	tr = std::move( r.tr );
	readConflicts = std::move( r.readConflicts );
	watchMap = std::move( r.watchMap );
	reading = std::move( r.reading );
	resetPromise = std::move( r.resetPromise );
	r.resetPromise = Promise<Void>();
	deferredError = std::move( r.deferredError );
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
	nativeReadRanges = std::move(r.nativeReadRanges);
	nativeWriteRanges = std::move(r.nativeWriteRanges);
	versionStampKeys = std::move(r.versionStampKeys);
}

ReadYourWritesTransaction::ReadYourWritesTransaction(ReadYourWritesTransaction&& r) noexcept
  : cache(std::move(r.cache)), writes(std::move(r.writes)), arena(std::move(r.arena)), reading(std::move(r.reading)),
    retries(r.retries), approximateSize(r.approximateSize), creationTime(r.creationTime),
    deferredError(std::move(r.deferredError)), timeoutActor(std::move(r.timeoutActor)),
    resetPromise(std::move(r.resetPromise)), commitStarted(r.commitStarted), options(r.options),
    transactionDebugInfo(r.transactionDebugInfo) {
	cache.arena = &arena;
	writes.arena = &arena;
	tr = std::move( r.tr );
	readConflicts = std::move(r.readConflicts);
	watchMap = std::move( r.watchMap );
	r.resetPromise = Promise<Void>();
	persistentOptions = std::move(r.persistentOptions);
	nativeReadRanges = std::move(r.nativeReadRanges);
	nativeWriteRanges = std::move(r.nativeWriteRanges);
	versionStampKeys = std::move(r.versionStampKeys);
}

Future<Void> ReadYourWritesTransaction::onError(Error const& e) {
	return RYWImpl::onError( this, e );
}

void ReadYourWritesTransaction::applyPersistentOptions() {
	Optional<StringRef> timeout;
	for (auto option : persistentOptions) {
		if(option.first == FDBTransactionOptions::TIMEOUT) {
			timeout = option.second.castTo<StringRef>();
		}
		else {
			setOptionImpl(option.first, option.second.castTo<StringRef>());
		}
	}

	// Setting a timeout can immediately cause a transaction to fail. The only timeout 
	// that matters is the one most recently set, so we ignore any earlier set timeouts
	// that might inadvertently fail the transaction.
	if(timeout.present()) {
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
	watchMap.clear();
	reading = AndFuture();
	approximateSize = 0;
	commitStarted = false;

	deferredError = Error();

	if(tr.apiVersionAtLeast(16)) {
		options.reset(tr);
		applyPersistentOptions();
	}

	if ( !oldReset.isSet() )
		oldReset.sendError(transaction_cancelled());
}

void ReadYourWritesTransaction::cancel() {
	if(!resetPromise.isSet() )
		resetPromise.sendError(transaction_cancelled());
}

void ReadYourWritesTransaction::reset() {
	retries = 0;
	approximateSize = 0;
	creationTime = now();
	timeoutActor.cancel();
	persistentOptions.clear();
	options.reset(tr);
	transactionDebugInfo.clear();
	tr.fullReset();
	versionStampFuture = tr.getVersionstamp();
	std::copy(tr.getDatabase().getTransactionDefaults().begin(), tr.getDatabase().getTransactionDefaults().end(), std::back_inserter(persistentOptions));
	resetRyow();
}

KeyRef ReadYourWritesTransaction::getMaxReadKey() {
	if(options.readSystemKeys)
		return systemKeys.end;
	else
		return normalKeys.end;
}

KeyRef ReadYourWritesTransaction::getMaxWriteKey() {
	if(options.writeSystemKeys)
		return systemKeys.end;
	else
		return normalKeys.end;
}

ReadYourWritesTransaction::~ReadYourWritesTransaction() {
	if( !resetPromise.isSet() )
		resetPromise.sendError(transaction_cancelled());
}

bool ReadYourWritesTransaction::checkUsedDuringCommit() {
	if(commitStarted && !resetPromise.isSet() && !options.disableUsedDuringCommitProtection) {
		resetPromise.sendError(used_during_commit());
	}

	return commitStarted;
}

void ReadYourWritesTransaction::debugLogRetries(Optional<Error> error) {
	bool committed = !error.present();
	if(options.debugRetryLogging) {
		double timeSinceLastLog = now() - transactionDebugInfo->lastRetryLogTime;
		double elapsed = now() - creationTime;
		if(timeSinceLastLog >= 1 || (committed && elapsed > 1)) {
			std::string transactionNameStr = "";
			if(!transactionDebugInfo->transactionName.empty())
				transactionNameStr = format(" in transaction '%s'", printable(StringRef(transactionDebugInfo->transactionName)).c_str());
			if(!g_network->isSimulated()) //Fuzz workload turns this on, but we do not want stderr output in simulation
				fprintf(stderr, "fdb WARNING: long transaction (%.2fs elapsed%s, %d retries, %s)\n", elapsed, transactionNameStr.c_str(), retries, committed ? "committed" : error.get().what());
			{
				TraceEvent trace = TraceEvent("LongTransaction");
				if(error.present())
					trace.error(error.get(), true);
				if(!transactionDebugInfo->transactionName.empty())
					trace.detail("TransactionName", transactionDebugInfo->transactionName);
				trace.detail("Elapsed", elapsed).detail("Retries", retries).detail("Committed", committed);
			}
			transactionDebugInfo->lastRetryLogTime = now();
		}
	}
}
