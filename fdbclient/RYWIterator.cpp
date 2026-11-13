/*
 * RYWIterator.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "RYWIterator.h"
#include "fdbclient/RandomTestUtils.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/Knobs.h"
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
	for (int e = 0; e < 3; e++) {
		for (auto b = bases.begin(); b != bases.end(); ++b) {
			srs.push_back(ExtStringRef(*b, e));
			ssrs.push_back(StringRef(*b + std::string(e, 0)));
		}
	}
	ASSERT(srs.size() == ssrs.size());
	printf("2\n");
	for (int i = 0; i < srs.size(); i++) {
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
	}
	printf("OK\n");
}

void testSnapshotCache() {
	Arena arena;
	SnapshotCache cache(&arena);
	WriteMap writes(&arena);

	Standalone<VectorRef<KeyValueRef>> keys;
	keys.push_back_deep(keys.arena(), KeyValueRef("d"_sr, "doo"_sr));
	keys.push_back_deep(keys.arena(), KeyValueRef("e"_sr, "eoo"_sr));
	keys.push_back_deep(keys.arena(), KeyValueRef("e\x00"_sr, "zoo"_sr));
	keys.push_back_deep(keys.arena(), KeyValueRef("f"_sr, "foo"_sr));
	cache.insert(KeyRangeRef("d"_sr, "f\x00"_sr), keys);

	cache.insert(KeyRangeRef("g"_sr, "h"_sr), Standalone<VectorRef<KeyValueRef>>());

	Standalone<VectorRef<KeyValueRef>> keys2;
	keys2.push_back_deep(keys2.arena(), KeyValueRef("k"_sr, "koo"_sr));
	keys2.push_back_deep(keys2.arena(), KeyValueRef("l"_sr, "loo"_sr));
	cache.insert(KeyRangeRef("j"_sr, "m"_sr), keys2);

	writes.mutate("c"_sr, MutationRef::SetValue, "c--"_sr, true);
	writes.clear(KeyRangeRef("c\x00"_sr, "e"_sr), true);
	writes.mutate("c\x00"_sr, MutationRef::SetValue, "c00--"_sr, true);
	WriteMap::iterator it3(&writes);
	writes.mutate("d"_sr, MutationRef::SetValue, "d--"_sr, true);
	writes.mutate("e"_sr, MutationRef::SetValue, "e++"_sr, true);
	writes.mutate("i"_sr, MutationRef::SetValue, "i--"_sr, true);

	KeyRange searchKeys = KeyRangeRef("a"_sr, "z"_sr);

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

TEST_CASE("/fdbclient/ExtStringRef/materialization") {
	struct MaterializationCase {
		StringRef base;
		int padding;
		StringRef expected;
	};
	const MaterializationCase cases[] = {
		{ ""_sr, 0, ""_sr },
		{ ""_sr, 3, "\x00\x00\x00"_sr },
		{ "a\x00z"_sr, 0, "a\x00z"_sr },
		{ "a\x00z"_sr, 1, "a\x00z\x00"_sr },
		{ "a\x00z"_sr, 3, "a\x00z\x00\x00\x00"_sr },
	};
	for (const auto& test : cases) {
		Standalone<StringRef> base(test.base);
		ExtStringRef extended(base, test.padding);
		Arena ownedArena;
		Arena borrowArena;
		StringRef owned = extended.toArena(ownedArena);
		StringRef borrowedOrOwned = extended.toArenaOrRef(borrowArena);
		Standalone<StringRef> standalone = extended.toStandaloneStringRef();
		ASSERT(owned == test.expected);
		ASSERT(borrowedOrOwned == test.expected);
		ASSERT(standalone == test.expected);
		if (test.padding == 0) {
			ASSERT(borrowedOrOwned.begin() == base.begin());
			ASSERT(borrowArena.getSize() == 0);
		}
		if (!base.empty()) {
			mutateString(base)[0] = 'b';
			ASSERT(owned == test.expected);
			ASSERT(standalone == test.expected);
			if (test.padding == 0) {
				ASSERT(borrowedOrOwned == base);
			} else {
				ASSERT(borrowedOrOwned == test.expected);
			}
		}
	}
	return Void();
}

TEST_CASE("/fdbclient/WriteMap/emptiness") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	writes.mutate("apple"_sr, MutationRef::SetValue, "red"_sr, true);
	ASSERT(!writes.empty());
	return Void();
}

TEST_CASE("/fdbclient/WriteMap/VersionstampedvalueAfterSet") {
	Arena arena = Arena();
	SnapshotCache cache(&arena);
	WriteMap writes = WriteMap(&arena);

	ASSERT(writes.empty());
	writes.mutate("apple"_sr, MutationRef::SetValue, "red"_sr, true);
	writes.mutate("apple"_sr, MutationRef::SetVersionstampedValue, metadataVersionRequiredValue, true);

	RYWIterator it(&cache, &writes);
	it.bypassUnreadableProtection();
	it.skip("apple"_sr);
	ASSERT(it.is_unreadable());
	ASSERT(it.is_kv());
	const KeyValueRef* kv = it.kv(arena);
	ASSERT(kv->key == "apple"_sr);
	ASSERT(kv->value == metadataVersionRequiredValue);

	return Void();
}

TEST_CASE("/fdbclient/WriteMap/clear") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	writes.mutate("apple"_sr, MutationRef::SetValue, "red"_sr, true);
	ASSERT(!writes.empty());
	ASSERT(getWriteMapCount(&writes) == 3);

	KeyRangeRef range = KeyRangeRef("a"_sr, "j"_sr);
	writes.clear(range, true);
	ASSERT(getWriteMapCount(&writes) == 3);

	return Void();
}

TEST_CASE("/fdbclient/WriteMap/setVersionstampedKey") {
	Arena arena = Arena();
	WriteMap writes = WriteMap(&arena);
	ASSERT(writes.empty());
	ASSERT(getWriteMapCount(&writes) == 1);

	writes.mutate("stamp:XXXXXXXX\x06\x00\x00\x00"_sr, MutationRef::SetVersionstampedKey, "1"_sr, true);
	ASSERT(!writes.empty());
	ASSERT(getWriteMapCount(&writes) == 3);

	writes.mutate("stamp:ZZZZZZZZZZ"_sr, MutationRef::AddValue, "2"_sr, true);
	ASSERT(getWriteMapCount(&writes) == 5);

	WriteMap::iterator it(&writes);
	it.skip(allKeys.begin);

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(""_sr) == 0);
	ASSERT(it.endKey().compare("stamp:XXXXXXXX\x06\x00\x00\x00"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp:XXXXXXXX\x06\x00\x00\x00"_sr) == 0);
	ASSERT(it.endKey().compare("stamp:XXXXXXXX\x06\x00\x00\x00\x00"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp:XXXXXXXX\x06\x00\x00\x00\x00"_sr) == 0);
	ASSERT(it.endKey().compare("stamp:ZZZZZZZZZZ"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp:ZZZZZZZZZZ"_sr) == 0);
	ASSERT(it.endKey().compare("stamp:ZZZZZZZZZZ\x00"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(!it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp:ZZZZZZZZZZ\x00"_sr) == 0);
	ASSERT(it.endKey().compare("\xff\xff"_sr) == 0);
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

	writes.mutate("stamp"_sr, MutationRef::SetVersionstampedValue, "XXXXXXXX\x00\x00\x00\x00\x00\x00"_sr, true);
	ASSERT(!writes.empty());
	ASSERT(getWriteMapCount(&writes) == 3);

	writes.mutate("stamp123"_sr, MutationRef::AddValue, "1"_sr, true);
	ASSERT(getWriteMapCount(&writes) == 5);

	WriteMap::iterator it(&writes);
	it.skip(allKeys.begin);

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare(""_sr) == 0);
	ASSERT(it.endKey().compare("stamp"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp"_sr) == 0);
	ASSERT(it.endKey().compare("stamp\x00"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp\x00"_sr) == 0);
	ASSERT(it.endKey().compare("stamp123"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(!it.is_conflict_range());
	ASSERT(!it.is_operation());
	ASSERT(it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp123"_sr) == 0);
	ASSERT(it.endKey().compare("stamp123\x00"_sr) == 0);
	ASSERT(!it.is_cleared_range());
	ASSERT(it.is_conflict_range());
	ASSERT(it.is_operation());
	ASSERT(!it.is_independent());
	ASSERT(!it.is_unmodified_range());
	ASSERT(!it.is_unreadable());
	++it;

	ASSERT(it.beginKey() < allKeys.end);
	ASSERT(it.beginKey().compare("stamp123\x00"_sr) == 0);
	ASSERT(it.endKey().compare("\xff\xff"_sr) == 0);
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

	writes.mutate("apple123"_sr, MutationRef::SetValue, "17"_sr, true);
	ASSERT(getWriteMapCount(&writes) == 3);

	writes.mutate("apple123"_sr, MutationRef::AddValue, "1"_sr, true);
	ASSERT(getWriteMapCount(&writes) == 3);

	return Void();
}

// AppendIfFits is not associative under truncation. Two equal-sized appends must stay
// on the OperationStack and be applied sequentially against the base value; coalescing
// the operands first can reject work that would have succeeded one-at-a-time (#13828).
TEST_CASE("/fdbclient/WriteMap/appendIfFitsNonAssociative") {
	Arena arena = Arena();
	const int baseSize = 70000;
	const int appendSize = 20000;
	ASSERT(baseSize + appendSize <= CLIENT_KNOBS->VALUE_SIZE_LIMIT);
	ASSERT(baseSize + 2 * appendSize > CLIENT_KNOBS->VALUE_SIZE_LIMIT);

	ValueRef firstAppend = StringRef(arena, std::string(appendSize, 'b'));
	ValueRef secondAppend = StringRef(arena, std::string(appendSize, 'b'));
	ValueRef base = StringRef(arena, std::string(baseSize, 'a'));

	OperationStack stack(RYWMutation(firstAppend, MutationRef::AppendIfFits));
	WriteMap::coalesceOver(stack, RYWMutation(secondAppend, MutationRef::AppendIfFits), arena);
	ASSERT(stack.size() == 2);
	ASSERT(stack.at(0).type == MutationRef::AppendIfFits);
	ASSERT(stack.at(1).type == MutationRef::AppendIfFits);

	RYWMutation under = WriteMap::coalesceUnder(stack, base, arena);
	ASSERT(under.type == MutationRef::SetValue);
	ASSERT(under.value.present());
	ASSERT(under.value.get().size() == baseSize + appendSize);

	// Dependent writes (no prior SetValue in the transaction): RYW must still yield 90KB.
	WriteMap writes = WriteMap(&arena);
	writes.mutate("k"_sr, MutationRef::AppendIfFits, firstAppend, true);
	writes.mutate("k"_sr, MutationRef::AppendIfFits, secondAppend, true);
	WriteMap::iterator it(&writes);
	it.skip("k"_sr);
	ASSERT(it.is_operation());
	ASSERT(it.op().size() == 2);
	RYWMutation visible = WriteMap::coalesceUnder(it.op(), base, arena);
	ASSERT(visible.value.present());
	ASSERT(visible.value.get().size() == baseSize + appendSize);

	return Void();
}

// Pending AppendIfFits operands over an unknown base must give the same value as applying them one at a time.
TEST_CASE("/fdbclient/WriteMap/appendIfFitsSequentialEquivalence") {
	const int limit = CLIENT_KNOBS->VALUE_SIZE_LIMIT;
	for (int iter = 0; iter < 200; iter++) {
		Arena arena;
		WriteMap writes = WriteMap(&arena);
		auto randomSize = [&]() {
			switch (deterministicRandom()->randomInt(0, 4)) {
			case 0:
				return 0;
			case 1:
				return deterministicRandom()->randomInt(1, 100);
			case 2:
				return deterministicRandom()->randomInt(1, limit / 3);
			default:
				return deterministicRandom()->randomInt(1, limit + 1);
			}
		};
		Optional<ValueRef> base;
		if (deterministicRandom()->coinflip()) {
			base = StringRef(arena, std::string(randomSize(), 'a'));
		}

		Optional<ValueRef> expected = base;
		int n = deterministicRandom()->randomInt(1, 12);
		for (int i = 0; i < n; i++) {
			ValueRef operand = StringRef(arena, std::string(randomSize(), 'b' + i));
			writes.mutate("k"_sr, MutationRef::AppendIfFits, operand, true);
			expected = doAppendIfFits(expected, operand, arena);
		}

		WriteMap::iterator it(&writes);
		it.skip("k"_sr);
		ASSERT(it.op().size() == n);
		RYWMutation actual = WriteMap::coalesceUnder(it.op(), base, arena);
		ASSERT(actual.type == MutationRef::SetValue);
		ASSERT(actual.value.present() && actual.value.get() == expected.get());
	}
	return Void();
}

// Pushing onto a key's pending operations must not copy the earlier ones, and iterators created before later writes
// must still see the operations as they were when the iterator was created.
TEST_CASE("/fdbclient/WriteMap/manyPendingOperationsOnOneKey") {
	Arena arena;
	WriteMap writes = WriteMap(&arena);
	const int total = 30000;
	const std::vector<int> snapshotAt = { 1, 2, 1000, total / 2 };
	std::vector<WriteMap::iterator> snapshots;
	std::vector<ValueRef> operands;
	int nextSnapshot = 0;
	for (int i = 0; i < total; i++) {
		operands.push_back(StringRef(arena, format("%d", i)));
		writes.mutate("k"_sr, MutationRef::AppendIfFits, operands.back(), true);
		if (nextSnapshot < snapshotAt.size() && i + 1 == snapshotAt[nextSnapshot]) {
			snapshots.emplace_back(&writes);
			++nextSnapshot;
		}
	}

	WriteMap::iterator it(&writes);
	it.skip("k"_sr);
	ASSERT(it.op().size() == total);
	for (int i = 0; i < total; i++) {
		ASSERT(it.op().at(i).value.get() == operands[i]);
	}
	for (int s = 0; s < snapshots.size(); s++) {
		snapshots[s].skip("k"_sr);
		ASSERT(snapshots[s].op().size() == snapshotAt[s]);
		for (int i = 0; i < snapshotAt[s]; i++) {
			ASSERT(snapshots[s].op().at(i).value.get() == operands[i]);
		}
	}

	// Replacing the top of a stack that an older snapshot shares must not change what that snapshot sees.
	WriteMap::iterator beforeAdds(&writes);
	writes.mutate("k"_sr, MutationRef::AddValue, "\x01"_sr, true);
	writes.mutate("k"_sr, MutationRef::AddValue, "\x01"_sr, true);
	beforeAdds.skip("k"_sr);
	ASSERT(beforeAdds.op().size() == total);
	ASSERT(beforeAdds.op().top().type == MutationRef::AppendIfFits);
	ASSERT(beforeAdds.op().top().value.get() == operands.back());
	WriteMap::iterator afterAdds(&writes);
	afterAdds.skip("k"_sr);
	ASSERT(afterAdds.op().size() == total + 1);
	ASSERT(afterAdds.op().top().type == MutationRef::AddValue);
	ASSERT(afterAdds.op().top().value.get() == "\x02"_sr);
	return Void();
}

// Reading a key with a single pending append over an absent or empty value must return the operand itself rather
// than allocating a copy in the transaction arena on every read.
TEST_CASE("/fdbclient/WriteMap/singleAppendReadDoesNotAllocate") {
	Arena arena;
	WriteMap writes = WriteMap(&arena);
	ValueRef operand = StringRef(arena, std::string(CLIENT_KNOBS->VALUE_SIZE_LIMIT, 'x'));
	writes.mutate("k"_sr, MutationRef::AppendIfFits, operand, true);

	WriteMap::iterator it(&writes);
	it.skip("k"_sr);
	ASSERT(it.op().size() == 1);
	Arena readArena;
	size_t sizeBefore = readArena.getSize();
	for (int i = 0; i < 100; i++) {
		for (Optional<ValueRef> base : { Optional<ValueRef>(), Optional<ValueRef>(StringRef()) }) {
			RYWMutation visible = WriteMap::coalesceUnder(it.op(), base, readArena);
			ASSERT(visible.value.present());
			ASSERT(visible.value.get().begin() == operand.begin());
			ASSERT(visible.value.get().size() == operand.size());
		}
	}
	ASSERT(readArena.getSize() == sizeBefore);
	return Void();
}

static Optional<ValueRef> applyAtomicOpSequentially(MutationRef::Type t, Optional<ValueRef> e, ValueRef o, Arena& a) {
	switch (t) {
	case MutationRef::AddValue:
		return doLittleEndianAdd(e, o, a);
	case MutationRef::And:
		return doAnd(e, o, a);
	case MutationRef::AndV2:
		return doAndV2(e, o, a);
	case MutationRef::Or:
		return doOr(e, o, a);
	case MutationRef::Xor:
		return doXor(e, o, a);
	case MutationRef::AppendIfFits:
		return doAppendIfFits(e, o, a);
	case MutationRef::Max:
		return doMax(e, o, a);
	case MutationRef::Min:
		return doMin(e, o, a);
	case MutationRef::MinV2:
		return doMinV2(e, o, a);
	case MutationRef::ByteMin:
		return doByteMin(e, o, a);
	case MutationRef::ByteMax:
		return doByteMax(e, o, a);
	case MutationRef::CompareAndClear:
		return doCompareAndClear(e, o, a);
	default:
		ASSERT(false);
		return Optional<ValueRef>();
	}
}

// Pending atomic ops over an unknown base must read back the same as applying them one at a time against it.
TEST_CASE("/fdbclient/WriteMap/atomicOpsSequentialEquivalence") {
	std::vector<MutationRef::Type> types = {
		MutationRef::AddValue, MutationRef::And,          MutationRef::AndV2,   MutationRef::Or,
		MutationRef::Xor,      MutationRef::AppendIfFits, MutationRef::Max,     MutationRef::Min,
		MutationRef::MinV2,    MutationRef::ByteMin,      MutationRef::ByteMax, MutationRef::CompareAndClear
	};
	for (int mode = -1; mode < (int)types.size(); mode++) { // -1 = mixed
		for (int iter = 0; iter < 2000; iter++) {
			Arena arena;
			WriteMap writes = WriteMap(&arena);
			auto rv = [&](bool allowEmpty) {
				int len = deterministicRandom()->randomInt(allowEmpty ? 0 : 1, 5);
				std::string str;
				for (int i = 0; i < len; i++)
					str.push_back((char)(deterministicRandom()->coinflip() ? deterministicRandom()->randomInt(0, 3)
					                                                       : deterministicRandom()->randomInt(0, 256)));
				return StringRef(arena, str);
			};
			Optional<ValueRef> base;
			if (deterministicRandom()->coinflip())
				base = rv(true);
			Optional<ValueRef> expected = base;
			int n = deterministicRandom()->randomInt(2, 7);
			for (int i = 0; i < n; i++) {
				MutationRef::Type t = mode < 0 ? deterministicRandom()->randomChoice(types) : types[mode];
				ValueRef o = rv(true);
				writes.mutate("k"_sr, t, o, true);
				expected = applyAtomicOpSequentially(t, expected, o, arena);
			}
			WriteMap::iterator it(&writes);
			it.skip("k"_sr);
			RYWMutation actual = WriteMap::coalesceUnder(it.op(), base, arena);
			ASSERT(actual.value.present() == expected.present());
			ASSERT(!expected.present() || actual.value.get() == expected.get());
		}
	}
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
			if (unreadableMap[key])
				setMap[key].push(RYWMutation(value, MutationRef::SetVersionstampedValue));
			else
				setMap[key] = OperationStack(RYWMutation(value, MutationRef::SetVersionstampedValue));
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
			} else if (!unreadableMap[key] && stack.size() > 0) {
				WriteMap::coalesceOver(stack, RYWMutation(value, MutationRef::And), arena);
			} else {
				stack.push(RYWMutation(value, MutationRef::And));
			}

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
