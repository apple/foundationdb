/*
 * RYWIterator.cpp
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

#include "fdbclient/RYWIterator.h"
#include "fdbclient/KeyRangeMap.h"
#include "flow/UnitTest.h"

const RYWIterator::SEGMENT_TYPE RYWIterator::typeMap[12] = {
	// UNMODIFIED_RANGE
	RYWIterator::UNKNOWN_RANGE,
	RYWIterator::EMPTY_RANGE,
	RYWIterator::KV,
	// CLEARED_RANGE
	RYWIterator::EMPTY_RANGE,
	RYWIterator::EMPTY_RANGE,
	RYWIterator::EMPTY_RANGE,
	// INDEPENDENT_WRITE
	RYWIterator::KV,
	RYWIterator::KV,
	RYWIterator::KV,
	// DEPENDENT_WRITE
	RYWIterator::UNKNOWN_RANGE,
	RYWIterator::KV,
	RYWIterator::KV
};

RYWIterator::SEGMENT_TYPE RYWIterator::type() const {
	if (is_unreadable() && !bypassUnreadable)
		throw accessed_unreadable();

	return typeMap[writes.type() * 3 + cache.type()];
}

bool RYWIterator::is_kv() const {
	return type() == KV;
}
bool RYWIterator::is_unknown_range() const {
	return type() == UNKNOWN_RANGE;
}
bool RYWIterator::is_empty_range() const {
	return type() == EMPTY_RANGE;
}
bool RYWIterator::is_dependent() const {
	return writes.type() == WriteMap::iterator::DEPENDENT_WRITE;
}
bool RYWIterator::is_unreadable() const {
	return writes.is_unreadable();
}

ExtStringRef RYWIterator::beginKey() {
	return begin_key_cmp <= 0 ? writes.beginKey() : cache.beginKey();
}
ExtStringRef RYWIterator::endKey() {
	return end_key_cmp <= 0 ? cache.endKey() : writes.endKey();
}

const KeyValueRef* RYWIterator::kv(Arena& arena) {
	if (is_unreadable() && !bypassUnreadable)
		throw accessed_unreadable();

	if (writes.is_unmodified_range()) {
		return cache.kv(arena);
	}

	auto result = (writes.is_independent() || cache.is_empty_range())
	                  ? WriteMap::coalesceUnder(writes.op(), Optional<ValueRef>(), arena)
	                  : WriteMap::coalesceUnder(writes.op(), cache.kv(arena)->value, arena);

	if (!result.value.present()) {
		// Key is now deleted, which can happen because of CompareAndClear.
		return nullptr;
	}
	temp = KeyValueRef(writes.beginKey().assertRef(), result.value.get());
	return &temp;
}

RYWIterator& RYWIterator::operator++() {
	if (end_key_cmp <= 0)
		++cache;
	if (end_key_cmp >= 0)
		++writes;
	begin_key_cmp = -end_key_cmp;
	end_key_cmp = cache.endKey().compare(writes.endKey());
	return *this;
}

RYWIterator& RYWIterator::operator--() {
	if (begin_key_cmp >= 0)
		--cache;
	if (begin_key_cmp <= 0)
		--writes;
	end_key_cmp = -begin_key_cmp;
	begin_key_cmp = cache.beginKey().compare(writes.beginKey());
	return *this;
}

bool RYWIterator::operator==(const RYWIterator& r) const {
	return cache == r.cache && writes == r.writes;
}
bool RYWIterator::operator!=(const RYWIterator& r) const {
	return !(*this == r);
}

void RYWIterator::skip(
    KeyRef key) { // Changes *this to the segment containing key (so that beginKey()<=key && key < endKey())
	cache.skip(key);
	writes.skip(key);
	updateCmp();
}

void RYWIterator::skipContiguous(KeyRef key) {
	if (is_kv() && writes.is_unmodified_range()) {
		cache.skipContiguous(std::min(ExtStringRef(key), writes.endKey()));
		updateCmp();
	}
}

void RYWIterator::skipContiguousBack(KeyRef key) {
	if (is_kv() && writes.is_unmodified_range()) {
		cache.skipContiguousBack(std::max(ExtStringRef(key), writes.beginKey().keyAfter()));
		updateCmp();
	}
}

WriteMap::iterator& RYWIterator::extractWriteMapIterator() {
	return writes;
}

void RYWIterator::dbg() {
	fprintf(stderr,
	        "cache: %d begin: '%s' end: '%s'\n",
	        cache.type(),
	        printable(cache.beginKey().toStandaloneStringRef()).c_str(),
	        printable(cache.endKey().toStandaloneStringRef()).c_str());
	fprintf(stderr,
	        "writes: %d begin: '%s' end: '%s'\n",
	        writes.type(),
	        printable(writes.beginKey().toStandaloneStringRef()).c_str(),
	        printable(writes.endKey().toStandaloneStringRef()).c_str());
	// fprintf(stderr, "summary - offset: %d cleared: %d size: %d\n", writes.offset,
	// writes.entry().following_keys_cleared, writes.entry().stack.size());
}

void RYWIterator::updateCmp() {
	begin_key_cmp = cache.beginKey().compare(writes.beginKey());
	end_key_cmp = cache.endKey().compare(writes.endKey());
}

void testESR() {
	printf("testESR\n");

	int chars[] = { 0, 1, 127, 128, 255 };
	int nchars = sizeof(chars) / sizeof(chars[0]);

	std::vector<std::string> bases;
	bases.push_back(std::string());
	for (int i = 0; i < nchars; i++) {
		bases.push_back(std::string(1, chars[i]));
		for (int j = 0; j < nchars; j++) {
			bases.push_back(std::string(1, chars[i]) + std::string(1, chars[j]));
			for (int k = 0; k < nchars; k++)
				bases.push_back(std::string(1, chars[i]) + std::string(1, chars[j]) + std::string(1, chars[k]));
		}
	}

	printf("1\n");
	std::vector<ExtStringRef> srs;
	std::vector<Standalone<StringRef>> ssrs;
	for (int e = 0; e < 3; e++)
		for (auto b = bases.begin(); b != bases.end(); ++b) {
			srs.push_back(ExtStringRef(*b, e));
			ssrs.push_back(StringRef(*b + std::string(e, 0)));
		}
	ASSERT(srs.size() == ssrs.size());
	printf("2\n");
	for (int i = 0; i < srs.size(); i++)
		for (int j = 0; j < srs.size(); j++) {
			bool c = ssrs[i] != ssrs[j];
			bool c2 = srs[i] != srs[j];
			if (c != c2) {
				printf("Error: '%s' cmp '%s' = %d\n", printable(ssrs[i]).c_str(), printable(ssrs[j]).c_str(), c2);
				return;
			}

			/*
			int c = ssrs[i] < ssrs[j] ? -1 : ssrs[i] == ssrs[j] ? 0 : 1;
			int c2 = srs[i].compare(srs[j]);
			if ( c != (0<c2)-(c2<0) ) {
			    printf("Error: '%s' cmp '%s' = %d\n", printable(ssrs[i]).c_str(), printable(ssrs[j]).c_str(), c2);
			    return;
			}*/

			/*
			bool c = ssrs[i].startsWith( ssrs[j] );
			int c2 = srs[i].startsWith( srs[j] );
			if ( c != c2 ) {
			    printf("Error: '%s' + %d cmp '%s' + %d = %d\n", printable(srs[i].base).c_str(), srs[i].extra_zero_bytes,
			printable(srs[j].base).c_str(), srs[j].extra_zero_bytes, c2); return;
			}
			bool c = equalsKeyAfter( ssrs[j], ssrs[i] );
			int c2 = srs[i].isKeyAfter( srs[j] );
			if ( c != c2 ) {
			    printf("Error: '%s' + %d cmp '%s' + %d = %d\n", printable(srs[i].base).c_str(), srs[i].extra_zero_bytes,
			printable(srs[j].base).c_str(), srs[j].extra_zero_bytes, c2); return;
			}
			*/
		}
	printf("OK\n");
}

void testSnapshotCache() {
	Arena arena;
	SnapshotCache cache(&arena);
	WriteMap writes(&arena);

	Standalone<VectorRef<KeyValueRef>> keys;
	keys.push_back_deep(keys.arena(), KeyValueRef(LiteralStringRef("d"), LiteralStringRef("doo")));
	keys.push_back_deep(keys.arena(), KeyValueRef(LiteralStringRef("e"), LiteralStringRef("eoo")));
	keys.push_back_deep(keys.arena(), KeyValueRef(LiteralStringRef("e\x00"), LiteralStringRef("zoo")));
	keys.push_back_deep(keys.arena(), KeyValueRef(LiteralStringRef("f"), LiteralStringRef("foo")));
	cache.insert(KeyRangeRef(LiteralStringRef("d"), LiteralStringRef("f\x00")), keys);

	cache.insert(KeyRangeRef(LiteralStringRef("g"), LiteralStringRef("h")), Standalone<VectorRef<KeyValueRef>>());

	Standalone<VectorRef<KeyValueRef>> keys2;
	keys2.push_back_deep(keys2.arena(), KeyValueRef(LiteralStringRef("k"), LiteralStringRef("koo")));
	keys2.push_back_deep(keys2.arena(), KeyValueRef(LiteralStringRef("l"), LiteralStringRef("loo")));
	cache.insert(KeyRangeRef(LiteralStringRef("j"), LiteralStringRef("m")), keys2);

	writes.mutate(LiteralStringRef("c"), MutationRef::SetValue, LiteralStringRef("c--"), true);
	writes.clear(KeyRangeRef(LiteralStringRef("c\x00"), LiteralStringRef("e")), true);
	writes.mutate(LiteralStringRef("c\x00"), MutationRef::SetValue, LiteralStringRef("c00--"), true);
	WriteMap::iterator it3(&writes);
	writes.mutate(LiteralStringRef("d"), MutationRef::SetValue, LiteralStringRef("d--"), true);
	writes.mutate(LiteralStringRef("e"), MutationRef::SetValue, LiteralStringRef("e++"), true);
	writes.mutate(LiteralStringRef("i"), MutationRef::SetValue, LiteralStringRef("i--"), true);

	KeyRange searchKeys = KeyRangeRef(LiteralStringRef("a"), LiteralStringRef("z"));

	RYWIterator it(&cache, &writes);
	it.skip(searchKeys.begin);
	while (true) {
		fprintf(stderr,
		        "b: '%s' e: '%s' type: %s value: '%s'\n",
		        printable(it.beginKey().toStandaloneStringRef()).c_str(),
		        printable(it.endKey().toStandaloneStringRef()).c_str(),
		        it.is_empty_range() ? "empty" : (it.is_kv() ? "keyvalue" : "unknown"),
		        it.is_kv() ? printable(it.kv(arena)->value).c_str() : "");
		if (it.endKey() >= searchKeys.end)
			break;
		++it;
	}
	fprintf(stderr, "end\n");

	it.skip(searchKeys.end);
	while (true) {
		fprintf(stderr,
		        "b: '%s' e: '%s' type: %s value: '%s'\n",
		        printable(it.beginKey().toStandaloneStringRef()).c_str(),
		        printable(it.endKey().toStandaloneStringRef()).c_str(),
		        it.is_empty_range() ? "empty" : (it.is_kv() ? "keyvalue" : "unknown"),
		        it.is_kv() ? printable(it.kv(arena)->value).c_str() : "");
		if (it.beginKey() <= searchKeys.begin)
			break;
		--it;
	}
	fprintf(stderr, "end\n");

	/*
	SnapshotCache::iterator it(&cache);

	it.skip(searchKeys.begin);
	while (true) {
	    if( it.is_kv() ) {
	        Standalone<VectorRef<KeyValueRef>> result;
	        KeyValueRef const& start = it.kv();
	        it.skipContiguous(searchKeys.end);
	        result.append( result.arena(), &start, &it.kv() - &start + 1 );
	        fprintf(stderr, "%s\n", printable(result).c_str());

	    }
	    if (it.endKey() >= searchKeys.end) break;
	    ++it;
	}
	fprintf(stderr, "end\n");


	it.skip(searchKeys.begin);
	while (true) {
	    fprintf(stderr, "b: '%s' e: '%s' type: %s value: '%s'\n",
	printable(it.beginKey().toStandaloneStringRef()).c_str(), printable(it.endKey().toStandaloneStringRef()).c_str(),
	it.is_empty_range() ? "empty" : ( it.is_kv() ? "keyvalue" : "unknown" ), it.is_kv() ?
	printable(it.kv().value).c_str() : ""); if (it.endKey() >= searchKeys.end) break;
	    ++it;
	}
	fprintf(stderr, "end\n");

	it.skip(searchKeys.end);
	while (true) {
	    fprintf(stderr, "b: '%s' e: '%s' type: %s value: '%s'\n",
	printable(it.beginKey().toStandaloneStringRef()).c_str(), printable(it.endKey().toStandaloneStringRef()).c_str(),
	it.is_empty_range() ? "empty" : ( it.is_kv() ? "keyvalue" : "unknown" ), it.is_kv() ?
	printable(it.kv().value).c_str() : "" ); if (it.beginKey() <= searchKeys.begin) break;
	    --it;
	}
	fprintf(stderr, "end\n");

	WriteMap::iterator it2(&writes);
	it2.skip(searchKeys.begin);
	while (true) {
	    fprintf(stderr, "b: '%s' e: '%s' type: %s value: '%s'\n",
	printable(it2.beginKey().toStandaloneStringRef()).c_str(), printable(it2.endKey().toStandaloneStringRef()).c_str(),
	it2.is_cleared_range() ? "cleared" : ( it2.is_unmodified_range() ? "unmodified" : "operation" ), it2.is_operation()
	? printable(it2.op().top().value).c_str() : ""); if (it2.endKey() >= searchKeys.end) break;
	    ++it2;
	}
	fprintf(stderr, "end\n");


	it3.skip(searchKeys.begin);
	while (true) {
	    fprintf(stderr, "b: '%s' e: '%s' type: %s value: '%s'\n",
	printable(it3.beginKey().toStandaloneStringRef()).c_str(), printable(it3.endKey().toStandaloneStringRef()).c_str(),
	it3.is_cleared_range() ? "cleared" : ( it3.is_unmodified_range() ? "unmodified" : "operation" ), it3.is_operation()
	? printable(it3.op().top().value).c_str() : ""); if (it3.endKey() >= searchKeys.end) break;
	    ++it3;
	}
	fprintf(stderr, "end\n");
	*/
}

/*
ACTOR RangeResult getRange( Transaction* tr, KeySelector begin, KeySelector end, SnapshotCache* cache,
WriteMap* writes, GetRangeLimits limits ) {
    RYWIterator it(cache, writes); RYWIterator itEnd(cache, writes);
    resolveKeySelectorFromCache( begin, it );
    resolveKeySelectorFromCache( end, itEnd );

    Standalone<VectorRef<KeyValueRef>> result;

    while ( !limits.isReached() ) {
        if ( it == itEnd && !it.is_unknown_range() ) break;

        if (it.is_unknown_range()) {
            RYWIterator ucEnd(it);
            ucEnd.skipUncached(itEnd);

            state KeySelector read_end = ucEnd==itEnd ? end :
firstGreaterOrEqual(ucEnd.endKey().toStandaloneStringRef()); RangeResult snapshot_read = wait(tr->getRange( begin,
read_end, limits, false, false ) ); cache->insert( getKnownKeyRange( snapshot_read, begin, read_end), snapshot_read );

            // TODO: Is there a more efficient way to deal with invalidation?
            it = itEnd = RYWIterator( cache, writes );
            resolveKeySelectorFromCache( begin, it );
            resolveKeySelectorFromCache( end, itEnd );
        } else if (it.is_kv()) {
            KeyValueRef const& start = it.kv();
            it.skipContiguous( end.isFirstGreaterOrEqual() ? end.key : allKeys.end );
            result.append( result.arena(), &start, &it.kv() - &start + 1 );
            limits;

            ++it;
        } else
            ++it;
    }

    ASSERT( end.isFirstGreaterOrEqual() );
    result.resize( result.arena(), std::lower_bound( result.begin(), result.end(), end.key ) - result.begin() );
}*/

// static void printWriteMap(WriteMap *p) {
//	WriteMap::iterator it(p);
//	for (it.skip(allKeys.begin); it.beginKey() < allKeys.end; ++it) {
//		if (it.is_cleared_range()) {
//			printf("CLEARED ");
//		}
//		if (it.is_conflict_range()) {
//			printf("CONFLICT ");
//		}
//		if (it.is_operation()) {
//			printf("OPERATION ");
//			printf(it.is_independent() ? "INDEPENDENT " : "DEPENDENT ");
//		}
//		if (it.is_unmodified_range()) {
//			printf("UNMODIFIED ");
//		}
//		if (it.is_unreadable()) {
//			printf("UNREADABLE ");
//		}
//		printf(": \"%s\" -> \"%s\"\n",
//			printable(it.beginKey().toStandaloneStringRef()).c_str(),
//			printable(it.endKey().toStandaloneStringRef()).c_str());
//	}
//	printf("\n");
//}

static int getWriteMapCount(WriteMap* p) {
	//	printWriteMap(p);
	int count = 0;
	WriteMap::iterator it(p);
	for (it.skip(allKeys.begin); it.beginKey() < allKeys.end; ++it) {
		count += 1;
	}
	return count;
}

TEST_CASE("/fdbclient/WriteMap/emptiness") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	writes.mutate(LiteralStringRef("apple"), MutationRef::SetValue, LiteralStringRef("red"), true);
	ASSERT(!writes.empty());
	return Void();
}

TEST_CASE("/fdbclient/WriteMap/clear") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	writes.mutate(LiteralStringRef("apple"), MutationRef::SetValue, LiteralStringRef("red"), true);
	ASSERT(!writes.empty());
	ASSERT(getWriteMapCount(&writes) == 3);

	KeyRangeRef range = KeyRangeRef(LiteralStringRef("a"), LiteralStringRef("j"));
	writes.clear(range, true);
	ASSERT(getWriteMapCount(&writes) == 3);

	return Void();
}

TEST_CASE("/fdbclient/WriteMap/setVersionstampedKey") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	writes.mutate(LiteralStringRef("stamp:XXXXXXXX\x06\x00\x00\x00"),
	              MutationRef::SetVersionstampedKey,
	              LiteralStringRef("1"),
	              true);
	ASSERT(!writes.empty());
	ASSERT(getWriteMapCount(&writes) == 3);

	writes.mutate(LiteralStringRef("stamp:ZZZZZZZZZZ"), MutationRef::AddValue, LiteralStringRef("2"), true);
	ASSERT(getWriteMapCount(&writes) == 5);

	WriteMap::iterator it(&writes);
	it.skip(allKeys.begin);

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp:XXXXXXXX\x06\x00\x00\x00")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp:XXXXXXXX\x06\x00\x00\x00")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp:XXXXXXXX\x06\x00\x00\x00\x00")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp:XXXXXXXX\x06\x00\x00\x00\x00")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp:ZZZZZZZZZZ")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp:ZZZZZZZZZZ")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp:ZZZZZZZZZZ\x00")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(!it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp:ZZZZZZZZZZ\x00")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("\xff\xff")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() >= allKeys.end);

	return Void();
}

TEST_CASE("/fdbclient/WriteMap/setVersionstampedValue") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	writes.mutate(LiteralStringRef("stamp"),
	              MutationRef::SetVersionstampedValue,
	              LiteralStringRef("XXXXXXXX\x00\x00\x00\x00\x00\x00"),
	              true);
	ASSERT(!writes.empty());
	ASSERT(getWriteMapCount(&writes) == 3);

	writes.mutate(LiteralStringRef("stamp123"), MutationRef::AddValue, LiteralStringRef("1"), true);
	ASSERT(getWriteMapCount(&writes) == 5);

	WriteMap::iterator it(&writes);
	it.skip(allKeys.begin);

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp\x00")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp\x00")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp123")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp123")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("stamp123\x00")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(!it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(LiteralStringRef("stamp123\x00")) == 0);
	ASSERT(it.endKey().compare(LiteralStringRef("\xff\xff")) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() >= allKeys.end);

	return Void();
}

TEST_CASE("/fdbclient/WriteMap/addValue") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	writes.mutate(LiteralStringRef("apple123"), MutationRef::SetValue, LiteralStringRef("17"), true);
	ASSERT(getWriteMapCount(&writes) == 3);

	writes.mutate(LiteralStringRef("apple123"), MutationRef::AddValue, LiteralStringRef("1"), true);
	ASSERT(getWriteMapCount(&writes) == 3);

	return Void();
}

TEST_CASE("/fdbclient/WriteMap/random") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	std::map<KeyRef, OperationStack> setMap;
	KeyRangeMap<bool> conflictMap;
	KeyRangeMap<bool> clearMap;
	KeyRangeMap<bool> unreadableMap;

	for (int i = 0; i < 100; i++) {
		int r = deterministicRandom()->randomInt(0, 10);
		if (r == 0) {
			KeyRangeRef range = RandomTestImpl::getRandomRange(arena);
			writes.addConflictRange(range);
			conflictMap.insert(range, true);
			TraceEvent("RWMT_AddConflictRange").detail("Range", range);
		} else if (r == 1) {
			KeyRangeRef range = RandomTestImpl::getRandomRange(arena);
			writes.addUnmodifiedAndUnreadableRange(range);
			setMap.erase(setMap.lower_bound(range.begin), setMap.lower_bound(range.end));
			conflictMap.insert(range, false);
			clearMap.insert(range, false);
			unreadableMap.insert(range, true);
			TraceEvent("RWMT_AddUnmodifiedAndUnreadableRange").detail("Range", range);
		} else if (r == 2) {
			bool addConflict = deterministicRandom()->random01() < 0.5;
			KeyRangeRef range = RandomTestImpl::getRandomRange(arena);
			writes.clear(range, addConflict);
			setMap.erase(setMap.lower_bound(range.begin), setMap.lower_bound(range.end));
			if (addConflict)
				conflictMap.insert(range, true);
			clearMap.insert(range, true);
			unreadableMap.insert(range, false);
			TraceEvent("RWMT_Clear").detail("Range", range).detail("AddConflict", addConflict);
		} else if (r == 3) {
			bool addConflict = deterministicRandom()->random01() < 0.5;
			KeyRef key = RandomTestImpl::getRandomKey(arena);
			ValueRef value = RandomTestImpl::getRandomValue(arena);
			writes.mutate(key, MutationRef::SetVersionstampedValue, value, addConflict);
			setMap[key].push(RYWMutation(value, MutationRef::SetVersionstampedValue));
			if (addConflict)
				conflictMap.insert(key, true);
			clearMap.insert(key, false);
			unreadableMap.insert(key, true);
			TraceEvent("RWMT_SetVersionstampedValue")
			    .detail("Key", key)
			    .detail("Value", value.size())
			    .detail("AddConflict", addConflict);
		} else if (r == 4) {
			bool addConflict = deterministicRandom()->random01() < 0.5;
			KeyRef key = RandomTestImpl::getRandomKey(arena);
			ValueRef value = RandomTestImpl::getRandomValue(arena);
			writes.mutate(key, MutationRef::SetVersionstampedKey, value, addConflict);
			setMap[key].push(RYWMutation(value, MutationRef::SetVersionstampedKey));
			if (addConflict)
				conflictMap.insert(key, true);
			clearMap.insert(key, false);
			unreadableMap.insert(key, true);
			TraceEvent("RWMT_SetVersionstampedKey")
			    .detail("Key", key)
			    .detail("Value", value.size())
			    .detail("AddConflict", addConflict);
		} else if (r == 5) {
			bool addConflict = deterministicRandom()->random01() < 0.5;
			KeyRef key = RandomTestImpl::getRandomKey(arena);
			ValueRef value = RandomTestImpl::getRandomValue(arena);
			writes.mutate(key, MutationRef::And, value, addConflict);

			auto& stack = setMap[key];
			if (clearMap[key]) {
				stack = OperationStack(RYWMutation(StringRef(), MutationRef::SetValue));
				WriteMap::coalesceOver(stack, RYWMutation(value, MutationRef::And), arena);
			} else if (!unreadableMap[key] && stack.size() > 0)
				WriteMap::coalesceOver(stack, RYWMutation(value, MutationRef::And), arena);
			else
				stack.push(RYWMutation(value, MutationRef::And));

			if (addConflict)
				conflictMap.insert(key, true);
			clearMap.insert(key, false);
			TraceEvent("RWMT_And").detail("Key", key).detail("Value", value.size()).detail("AddConflict", addConflict);
		} else {
			bool addConflict = deterministicRandom()->random01() < 0.5;
			KeyRef key = RandomTestImpl::getRandomKey(arena);
			ValueRef value = RandomTestImpl::getRandomValue(arena);
			writes.mutate(key, MutationRef::SetValue, value, addConflict);
			if (unreadableMap[key])
				setMap[key].push(RYWMutation(value, MutationRef::SetValue));
			else
				setMap[key] = OperationStack(RYWMutation(value, MutationRef::SetValue));
			if (addConflict)
				conflictMap.insert(key, true);
			clearMap.insert(key, false);
			TraceEvent("RWMT_Set").detail("Key", key).detail("Value", value.size()).detail("AddConflict", addConflict);
		}
	}

	WriteMap::iterator it(&writes);
	it.skip(allKeys.begin);
	auto setIter = setMap.begin();
	auto setEnd = setMap.end();

	for (; it.beginKey() < allKeys.end; ++it) {
		if (it.is_operation()) {
			ASSERT(setIter != setEnd);
			TraceEvent("RWMT_CheckOperation")
			    .detail("WmKey", it.beginKey())
			    .detail("WmSize", it.op().size())
			    .detail("WmValue",
			            it.op().top().value.present() ? std::to_string(it.op().top().value.get().size()) : "Not Found")
			    .detail("WmType", (int)it.op().top().type)
			    .detail("SmKey", setIter->first)
			    .detail("SmSize", setIter->second.size())
			    .detail("SmValue",
			            setIter->second.top().value.present() ? std::to_string(setIter->second.top().value.get().size())
			                                                  : "Not Found")
			    .detail("SmType", (int)setIter->second.top().type);
			ASSERT(it.beginKey() == setIter->first && it.op() == setIter->second);
			++setIter;
		}
	}

	TraceEvent("RWMT_CheckOperationFinal").detail("WmKey", it.beginKey()).detail("SmIter", setIter == setEnd);

	ASSERT(it.beginKey() >= allKeys.end && setIter == setEnd);

	it.skip(allKeys.begin);
	auto conflictRanges = conflictMap.ranges();
	auto conflictIter = conflictRanges.begin();
	auto conflictEnd = conflictRanges.end();

	while (it.beginKey() < allKeys.end && conflictIter != conflictEnd) {
		ASSERT(conflictIter.value() == it.is_conflict_range());
		if (conflictIter.range().end < it.endKey()) {
			++conflictIter;
		} else if (conflictIter.range().end > it.endKey()) {
			++it;
		} else {
			++it;
			++conflictIter;
		}
	}

	it.skip(allKeys.begin);
	auto clearRanges = clearMap.ranges();
	auto clearIter = clearRanges.begin();
	auto clearEnd = clearRanges.end();

	while (it.beginKey() < allKeys.end && clearIter != clearEnd) {
		ASSERT(clearIter.value() == it.is_cleared_range());
		if (clearIter.range().end < it.endKey()) {
			++clearIter;
		} else if (clearIter.range().end > it.endKey()) {
			++it;
		} else {
			++it;
			++clearIter;
		}
	}

	it.skip(allKeys.begin);
	auto unreadableRanges = unreadableMap.ranges();
	auto unreadableIter = unreadableRanges.begin();
	auto unreadableEnd = unreadableRanges.end();

	while (it.beginKey() < allKeys.end && unreadableIter != unreadableEnd) {
		TraceEvent("RWMT_CheckUnreadable")
		    .detail("WriteMapRange",
		            KeyRangeRef(it.beginKey().toStandaloneStringRef(), it.endKey().toStandaloneStringRef()))
		    .detail("UnreadableMapRange", unreadableIter.range())
		    .detail("WriteMapValue", it.is_unreadable())
		    .detail("UnreadableMapValue", unreadableIter.value());
		ASSERT(unreadableIter.value() == it.is_unreadable());
		if (unreadableIter.range().end < it.endKey()) {
			++unreadableIter;
		} else if (unreadableIter.range().end > it.endKey()) {
			++it;
		} else {
			++it;
			++unreadableIter;
		}
	}

	//	printWriteMap(&writes);

	return Void();
}
