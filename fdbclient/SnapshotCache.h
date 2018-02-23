/*
 * SnapshotCache.h
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

#ifndef FDBCLIENT_SNAPSHOTCACHE_H
#define FDBCLIENT_SNAPSHOTCACHE_H
#pragma once

#include "FDBTypes.h"
#include "NativeAPI.h"
#include "SystemData.h"
#include "flow/IndexedSet.h"

struct ExtStringRef {
	ExtStringRef() : extra_zero_bytes(0) {}
	ExtStringRef( StringRef const& s, int extra_zero_bytes=0 ) : base( s ), extra_zero_bytes(extra_zero_bytes) {}

	Standalone<StringRef> toStandaloneStringRef() {
		auto s = makeString( size() );
		memcpy( mutateString( s ), base.begin(), base.size() );
		memset( mutateString( s ) + base.size(), 0, extra_zero_bytes );
		return s;
	};

	StringRef toArenaOrRef( Arena& a ) {
		if (extra_zero_bytes) {
			StringRef dest = StringRef( new(a) uint8_t[ size() ], size() );
			memcpy( mutateString(dest), base.begin(), base.size() );
			memset( mutateString(dest)+base.size(), 0, extra_zero_bytes );
			return dest;
		} else
			return base;
	}

	StringRef assertRef() {
		ASSERT( extra_zero_bytes == 0 );
		return base;
	}

	StringRef toArena( Arena& a ) {
		if (extra_zero_bytes) {
			StringRef dest = StringRef( new(a) uint8_t[ size() ], size() );
			memcpy( mutateString(dest), base.begin(), base.size() );
			memset( mutateString(dest)+base.size(), 0, extra_zero_bytes );
			return dest;
		} else
			return StringRef(a, base);
	}

	int size() const { return base.size() + extra_zero_bytes; }

	int cmp( ExtStringRef const& rhs ) const {
		int cbl = std::min(base.size(), rhs.base.size());
		int c = memcmp( base.begin(), rhs.base.begin(), cbl );
		if (c!=0) return c;

		for(int i=cbl; i<base.size(); i++)
			if (base[i]) return 1;
		for(int i=cbl; i<rhs.base.size(); i++)
			if (rhs.base[i]) return -1;
		return size() - rhs.size();
	}

	bool startsWith( const ExtStringRef& s ) const { 
		if (size() < s.size())
			return false;
		int cbl = std::min(base.size(), s.base.size());
		for(int i=cbl; i < std::min( s.size(),base.size() ); i++)
			if (base[i]) return false;
		for(int i=cbl; i < s.base.size(); i++)
			if (s.base[i]) return false;
		return !memcmp(base.begin(), s.base.begin(), cbl);
	}

	bool isKeyAfter( ExtStringRef const& s ) const {
		if( size() != s.size() + 1 )
			return false;
		if( extra_zero_bytes == 0 && base[base.size()-1] != 0 )
			return false;
		return startsWith( s ); 
	}

	ExtStringRef keyAfter() const {
		return ExtStringRef( base, extra_zero_bytes + 1 );
	}

private:
	StringRef base;
	int extra_zero_bytes;
};
inline bool operator == (const ExtStringRef& lhs, const ExtStringRef& rhs ) {
	return lhs.size() == rhs.size() && !lhs.cmp(rhs);
}
inline bool operator != (const ExtStringRef& lhs, const ExtStringRef& rhs ) { return !(lhs==rhs); }
inline bool operator < ( const ExtStringRef& lhs, const ExtStringRef& rhs ) { return lhs.cmp(rhs)<0; }
inline bool operator > ( const ExtStringRef& lhs, const ExtStringRef& rhs ) { return lhs.cmp(rhs)>0; }
inline bool operator <= ( const ExtStringRef& lhs, const ExtStringRef& rhs ) { return lhs.cmp(rhs)<=0; }
inline bool operator >= ( const ExtStringRef& lhs, const ExtStringRef& rhs ) { return lhs.cmp(rhs)>=0; }

class SnapshotCache {
private:
	struct Entry {
		// An entry represents a range of keys which are known.  The keys which are not present in `values` are implicitly empty.
		KeyRef beginKey;
		ExtStringRef endKey;
		VectorRef<KeyValueRef> values;

		Entry( KeyRef const& beginKey, ExtStringRef const& endKey, VectorRef<KeyValueRef> const& values ) 
		 : beginKey(beginKey), endKey(endKey), values(values) 
		{
		}
		Entry( KeyValueRef const& kv, Arena& arena )
			: beginKey(kv.key), endKey( kv.key, 1 )
		{
			values.push_back( arena, kv );
		}
		bool operator < (Entry const& r) const {
			return beginKey < r.beginKey;
		}
		bool operator < (StringRef const& r) const {
			return beginKey < r;
		}

		int segments() const { return 2*(values.size()+1); }
	};

	friend class ReadYourWritesTransaction;
	Arena* arena;
	IndexedSet< Entry, NoMetric > entries;

public:
	struct iterator {
		// Iterates over three types of segments: individual key/value pairs, empty ranges, and ranges which are not known.
		// Every key will belong to exactly one segment.  The first segment begins at "" and the last segment ends at \xff\xff.

		// Note that an uncached range stops at the next individual key that is known, even though we might want to read through that key.  In RYWIterator, it might also stop at a dependent write which is not known at all!

		iterator( SnapshotCache* cache ) : parent(cache), it( cache->entries.begin() ), offset( 0 ) {
			//++*this;  // gives begin
		}

		iterator( SnapshotCache* cache, class WriteMap* writes ) : parent(cache), it( cache->entries.begin() ), offset( 0 ) {} //for RYW to use the same constructor for snapshot cache and RYWIterator

		enum SEGMENT_TYPE { UNKNOWN_RANGE, EMPTY_RANGE, KV };

		SEGMENT_TYPE type() {
			if (!offset) return UNKNOWN_RANGE;
			if (offset&1) return EMPTY_RANGE;
			return KV;
		}

		bool is_kv() { return type() == KV; }
		bool is_unknown_range() { return type() == UNKNOWN_RANGE; }
		bool is_empty_range() { return type() == EMPTY_RANGE; }
		bool is_dependent() { return false; }
		bool is_unreadable() { return false; }

		ExtStringRef beginKey() {
			if (offset == 0) {
				auto prev = it;
				prev.decrementNonEnd();
				return prev->endKey;
			} else if (offset == 1)
				return it->beginKey;
			else
				return ExtStringRef( it->values[ (offset-2)>>1 ].key, offset&1 );
		}
		ExtStringRef endKey() {
			if (offset == 0)
				return it->beginKey;
			else if (offset == it->segments()-1)
				return it->endKey;
			else
				return ExtStringRef( it->values[ (offset-1)>>1 ].key, 1-(offset&1) );
		}

		KeyValueRef const& kv( Arena& arena ){  // only if is_kv()
			return it->values[ (offset-2)>>1 ];
		}

		iterator& operator++() {
			ExtStringRef originalEnd = endKey();
			do {
				offset++;
				if (offset == it->segments()) {
					offset = 0;
					++it;
				}
			} while ( endKey() == originalEnd );   // TODO: pointer only comparison; maintain equality of pointers to keys around degenerate segments
			return *this;
		}
		iterator& operator--() {
			ExtStringRef originalBegin = beginKey();
			do {
				offset--;
				if (offset<0) {
					it.decrementNonEnd();
					offset = it->segments()-1;
				}
			} while (beginKey() == originalBegin);
			return *this;
		}

		bool operator == ( const iterator& r ) const { return it == r.it && offset == r.offset; }

		void skip( KeyRef key ) {      // Changes *this to the segment containing key (so that beginKey()<=key && key < endKey())
			if( key == allKeys.end ) {
				it = parent->entries.lastItem();
				offset = 1;
				return;
			}
			
			it = parent->entries.lastLessOrEqual( Entry( key, key, VectorRef<KeyValueRef>() ) );  // TODO: finger query?
			if (key >= it->endKey) {
				offset = 0;
				++it;
			} else {
				int idx = std::lower_bound( it->values.begin(), it->values.end(), key, KeyValueRef::OrderByKey() ) - it->values.begin();
				offset = idx*2 + 1 + (idx < it->values.size() && it->values[idx].key==key);
			}
		}
		void skipContiguous( ExtStringRef key ) {   // Changes *this to be the last iterator i | the elements e of array [&*this, &*i] all have e->key < key
			offset = 2*(std::lower_bound( it->values.begin() + offset/2, it->values.end(), key, KeyValueRef::OrderByKey() ) - it->values.begin());
		}
		void skipContiguousBack( ExtStringRef key ) {   // Changes *this to be the first iterator i | the elements e of array [&*i, &*this] all have e->key >= key
			offset = 2*(std::lower_bound( it->values.begin(), it->values.begin() + offset/2 - 1, key, KeyValueRef::OrderByKey() ) - it->values.begin()) + 2;
		}

		void _nextUnknown() {  // For internal use only - can return a degenerate segment
			++it;
			offset = 0;
		}
		void _prevUnknown() {  // For internal use only - can return a degenerate segment
			offset = 0;
		}

		void dbg() {}

	private:
		friend class SnapshotCache;
		SnapshotCache* parent;
		IndexedSet<Entry,NoMetric>::iterator it;
		int offset;   // 0 <= offset < it->segments()
	};

	explicit SnapshotCache(Arena* arena) : arena(arena) {
		// Degenerate entries at the beginning and end reduce edge cases
		entries.insert( Entry( allKeys.begin, allKeys.begin, VectorRef<KeyValueRef>() ), NoMetric(), true );
		entries.insert( Entry( allKeys.end, afterAllKeys, VectorRef<KeyValueRef>() ), NoMetric(), true );
	}
	// Visual Studio refuses to generate these, apparently despite the standard
	SnapshotCache(SnapshotCache&& r) noexcept(true) : entries(std::move(r.entries)), arena(r.arena) {}
	SnapshotCache& operator=(SnapshotCache&& r) noexcept(true) { entries = std::move(r.entries); arena = r.arena; return *this; }

	bool empty() const {
		// Returns true iff anything is known about the contents of the snapshot
		for(auto i = entries.begin(); i != entries.end(); ++i)
			if (i->beginKey != i->endKey && i->beginKey < allKeys.end)
				return false;
		return true;
	}

	bool insert( KeyRef key, Optional<ValueRef> value ) {
		// Asserts that, in the snapshot, the given key has the given value (or is not present, if !value.present())

		iterator it( this );
		it.skip( key );

		if (it.is_unknown_range()) {
			if (value.present())
				entries.insert( Entry( KeyValueRef( key, value.get() ), *arena ), NoMetric(), true );
			else
				entries.insert( Entry( key, ExtStringRef(key,1), VectorRef<KeyValueRef>() ), NoMetric(), true );
			return true;
		}
		return false;
	}

	bool insert( KeyRangeRef keys, VectorRef<KeyValueRef> values ) {
		// Asserts that, in the snapshot, the given ranges of keys contains (only) the given key/value pairs
		// The returned iterator points to the first key in the range, or after the range if !values.size()
		if( keys.empty() )
			return false;

		iterator itb( this );
		itb.skip( keys.begin );
		iterator ite = itb;
		ite.skip( keys.end );

		StringRef begin = keys.begin;
		if (!itb.is_unknown_range() && itb.it->beginKey != keys.begin) {
			begin = itb.it->endKey.toArenaOrRef(*arena);
			auto i = std::lower_bound( values.begin(), values.end(), begin, KeyValueRef::OrderByKey() );
			values = VectorRef<KeyValueRef>( i, values.end()-i );
			itb._nextUnknown();
		}

		ExtStringRef end = keys.end;
		if (!ite.is_unknown_range()) {
			ite._prevUnknown();
			end = ite.endKey();
			values.resize( *arena, std::lower_bound( values.begin(), values.end(), end, KeyValueRef::OrderByKey() ) - values.begin() );
		}
		
		if (begin < end ) {
			bool addBegin = begin != allKeys.begin && itb.it->beginKey == allKeys.begin;
			entries.erase( itb.it, ite.it );
			entries.insert( Entry( begin, end, values ), NoMetric(), true );
			if( addBegin )
				entries.insert( Entry( allKeys.begin, allKeys.begin, VectorRef<KeyValueRef>() ), NoMetric(), true );
			return true;
		}
		return false;
	}

	void dump() {
		for( auto it = entries.begin(); it != entries.end(); ++it ) {
			TraceEvent("CacheDump").detail("begin", printable(it->beginKey)).detail("end", printable(it->endKey.toStandaloneStringRef())).detail("values", printable(it->values));
		}
	}

	void promise( iterator const& segment, KeyRangeRef keys, Future<Void> onReady );
		// Asserts that the caller is reading the contents of the given range of keys and will later call insert(keys, ?)
		//   and then set the given onReady future.
		// ? Also asserts that segment.uncached_range().begin() <= keys.begin && segment.uncached_range() >= keys.end
		// segment is not invalidated
};

#endif