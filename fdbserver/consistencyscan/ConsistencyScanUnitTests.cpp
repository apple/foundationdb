/*
 * ConsistencyScanUnitTests.cpp
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

#include "../../fdbclient/include/fdbclient/StorageServerInterface.h"

#include <algorithm>

#include "fdbserver/consistencyscan/ConsistencyScan.h"
#include "flow/UnitTest.h"

namespace {
// Builds a StorageServerInterface for passing into the unit tests.
StorageServerInterface makeTestSSI() {
	StorageServerInterface ssi;
	ssi.initEndpoints();
	return ssi;
}

GetKeyValuesReply makeReply(std::vector<std::pair<KeyRef, ValueRef>> const& kvs, bool more) {
	GetKeyValuesReply reply;
	reply.data.reserve(reply.arena, kvs.size());
	for (auto const& kv : kvs) {
		reply.data.push_back(reply.arena, KeyValueRef(kv.first, kv.second));
	}
	reply.more = more;
	reply.version = 1;
	return reply;
}

// Simulates a single storage server answering page read, starting from firstGreaterThan(lastReadKey) if set or
// from the beggining if absent.
GetKeyValuesReply pageFrom(std::vector<KeyValueRef> const& data, Optional<KeyRef> const& lastReadKey, size_t pageSize) {
	size_t startIdx = 0;
	if (lastReadKey.present()) {
		while (startIdx < data.size() && data[startIdx].key <= lastReadKey.get()) {
			startIdx++;
		}
	}
	const size_t endIdx = std::min(data.size(), startIdx + pageSize);
	GetKeyValuesReply reply;
	for (size_t i = startIdx; i < endIdx; i++) {
		reply.data.push_back(reply.arena, data[i]);
	}
	reply.more = endIdx < data.size();
	reply.version = 1;
	return reply;
}

// Reverse counterpart of pageFrom() above
GetKeyValuesReply pageFromReverse(std::vector<KeyValueRef> const& data,
                                  Optional<KeyRef> const& lastReadKey,
                                  size_t pageSize) {
	size_t endIdx = data.size(); // exclusive
	if (lastReadKey.present()) {
		while (endIdx > 0 && data[endIdx - 1].key >= lastReadKey.get()) {
			endIdx--;
		}
	}
	const size_t startIdx = endIdx >= pageSize ? endIdx - pageSize : 0;
	GetKeyValuesReply reply;
	for (size_t i = endIdx; i > startIdx; i--) {
		reply.data.push_back(reply.arena, data[i - 1]);
	}
	reply.more = startIdx > 0;
	reply.version = 1;
	return reply;
}

// Drives checkRangeReplies() across multiple rounds the way checkDataConsistency() does: each round
// pages every server's ground-truth data independently (so the two servers' page boundaries drift out
// of sync with each other, exactly like real, differently-sized replies would), feeds the replies
// through checkRangeReplies(), and resumes the next round from the returned nextKey. This validates
// that the *cumulative* unique/mismatch counts across all rounds land exactly on the errors that were
// planted below, neither over- nor undercounting.
void simulateConsistencyScan(const std::vector<KeyValueRef>& serverAData,
                             const std::vector<KeyValueRef>& serverBData,
                             const size_t pageSize,
                             const Reverse reverse,
                             const int uniqueA,
                             const int uniqueB,
                             const int mismatched) {
	std::vector servers = { makeTestSSI(), makeTestSSI() };
	KeyRangeRef range(""_sr, "\xff\xff"_sr);

	std::vector<int64_t> totalUniqueRefKeys(servers.size(), 0);
	std::vector<int64_t> totalUniqueCmpKeys(servers.size(), 0);
	std::vector<int64_t> totalMismatchedValues(servers.size(), 0);

	Optional<KeyRef> nextKey;
	int maxExpectedRounds = serverAData.size() + serverBData.size() + 1;
	int round = 0;
	for (; round < maxExpectedRounds; round++) {
		KeySelector begin;
		std::vector<ErrorOr<GetKeyValuesReply>> replies;
		replies.reserve(servers.size());
		if (reverse) {
			begin = nextKey.present() ? firstGreaterOrEqual(nextKey.get()) : firstGreaterOrEqual(range.end);
			replies.push_back(ErrorOr<GetKeyValuesReply>(pageFromReverse(serverAData, nextKey, pageSize)));
			replies.push_back(ErrorOr<GetKeyValuesReply>(pageFromReverse(serverBData, nextKey, pageSize)));
		} else {
			begin = nextKey.present() ? firstGreaterThan(nextKey.get()) : firstGreaterOrEqual(range.begin);
			replies.push_back(ErrorOr<GetKeyValuesReply>(pageFrom(serverAData, nextKey, pageSize)));
			replies.push_back(ErrorOr<GetKeyValuesReply>(pageFrom(serverBData, nextKey, pageSize)));
		}

		RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin, reverse);
		for (int i = 0; i < result.uniqueRefKeys.size(); i++) {
			totalUniqueRefKeys[i] += result.uniqueRefKeys[i];
			totalUniqueCmpKeys[i] += result.uniqueCmpKeys[i];
			totalMismatchedValues[i] += result.mismatchedValues[i];
		}

		if (!result.nextKey.present()) {
			break;
		}
		nextKey = result.nextKey;
	}
	ASSERT(round <
	       maxExpectedRounds); // otherwise pagination never converged -- likely a bug in the test, or a real regression

	ASSERT_EQ(totalUniqueRefKeys[1], uniqueA);
	ASSERT_EQ(totalUniqueCmpKeys[1], uniqueB);
	ASSERT_EQ(totalMismatchedValues[1], mismatched);
}

// Run simulateConsistencyScan() across all meaningful configurations. This means scanning in both forward
// and reverse order, with either servers as the reference, and with as many page sizes as make sense
void simulateAllConsistencyScans(const std::vector<KeyValueRef>& serverAData,
                                 const std::vector<KeyValueRef>& serverBData,
                                 const int uniqueA,
                                 const int uniqueB,
                                 const int mismatched) {

	// Check all the different page sizes and both forward and reverse
	for (size_t pageSize = 1; pageSize < serverAData.size() || pageSize < serverBData.size(); pageSize++) {
		simulateConsistencyScan(serverAData, serverBData, pageSize, Reverse(false), uniqueA, uniqueB, mismatched);
		simulateConsistencyScan(serverBData, serverAData, pageSize, Reverse(false), uniqueB, uniqueA, mismatched);
		simulateConsistencyScan(serverAData, serverBData, pageSize, Reverse(true), uniqueA, uniqueB, mismatched);
		simulateConsistencyScan(serverBData, serverAData, pageSize, Reverse(true), uniqueB, uniqueA, mismatched);
	}
}

} // namespace

// Two servers agree on every key they both actually read ("b" and "c"), but server 0's reply happened to
// paginate further ("d", "e") before server 1's did. checkRangeReplies() should bound its comparison at
// result.nextKey (the minimum last-read key among servers still reporting more data) rather than treating
// server 1's shorter raw reply as containing missing/unique keys.
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/MisalignedPaginationNotOverreported") {
	const std::vector servers = { makeTestSSI(), makeTestSSI() };
	const KeyRangeRef range("a"_sr, "z"_sr);
	const KeySelector begin = firstGreaterOrEqual(range.begin);

	std::vector<ErrorOr<GetKeyValuesReply>> replies;
	replies.push_back(ErrorOr<GetKeyValuesReply>(
	    makeReply({ { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr }, { "d"_sr, "1"_sr }, { "e"_sr, "1"_sr } },
	              /*more=*/true)));
	replies.push_back(ErrorOr<GetKeyValuesReply>(makeReply({ { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr } }, /*more=*/true)));

	RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin);

	ASSERT_EQ(result.firstValidServer, 0);
	ASSERT(result.nextKey.present());
	ASSERT(result.nextKey.get() == "c"_sr);
	ASSERT_EQ(result.uniqueRefKeys[1], 0);
	ASSERT_EQ(result.uniqueCmpKeys[1], 0);
	ASSERT_EQ(result.mismatchedValues[1], 0);
	ASSERT(!result.mismatchedMoreReplies[1]);
	ASSERT(result.success);
	ASSERT(result.allSucceeded);
	ASSERT(!result.readFailed);
	ASSERT(!result.foundInjected);

	co_return;
}

// Positive control for the test above: within the bounded comparison window, server 1 is actually missing "c"
// (a genuine gap, not just an artifact of pagination), so checkRangeReplies() should still catch it.
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/GenuineMissingKeyStillDetected") {
	const std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	const KeyRangeRef range("a"_sr, "z"_sr);
	const KeySelector begin = firstGreaterOrEqual(range.begin);

	std::vector<ErrorOr<GetKeyValuesReply>> replies;
	replies.push_back(ErrorOr<GetKeyValuesReply>(
	    makeReply({ { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr }, { "d"_sr, "1"_sr } }, /*more=*/false)));
	replies.push_back(
	    ErrorOr<GetKeyValuesReply>(makeReply({ { "b"_sr, "1"_sr }, { "d"_sr, "1"_sr } }, /*more=*/false)));

	const RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin);

	ASSERT_EQ(result.firstValidServer, 0);
	ASSERT(!result.nextKey.present()); // neither server reported more data
	ASSERT_EQ(result.uniqueRefKeys[1], 1); // "c" is unique to the reference server (server 0)
	ASSERT_EQ(result.uniqueCmpKeys[1], 0);
	ASSERT_EQ(result.mismatchedValues[1], 0);

	co_return;
}

// Both servers report more=false: they've each fully exhausted the requested range, so there's nothing
// left to page through, even though server 1's raw reply is shorter than server 0's. checkRangeReplies()
// should conclude that "d", "e", "f" are genuinely unique to server 0 (not just a pagination artifact,
// since server 1 truly has no more data coming), and that the range is complete.
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/TrailingUniqueKeysMarkRangeComplete") {
	const std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	const KeyRangeRef range("a"_sr, "z"_sr);
	const KeySelector begin = firstGreaterOrEqual(range.begin);

	std::vector<ErrorOr<GetKeyValuesReply>> replies;
	replies.push_back(ErrorOr<GetKeyValuesReply>(makeReply({ { "a"_sr, "1"_sr },
	                                                         { "b"_sr, "1"_sr },
	                                                         { "c"_sr, "1"_sr },
	                                                         { "d"_sr, "1"_sr },
	                                                         { "e"_sr, "1"_sr },
	                                                         { "f"_sr, "1"_sr } },
	                                                       /*more=*/false)));
	replies.push_back(ErrorOr<GetKeyValuesReply>(
	    makeReply({ { "a"_sr, "1"_sr }, { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr } }, /*more=*/false)));

	const RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin);

	ASSERT_EQ(result.uniqueRefKeys[1], 3); // d, e, f: really gone, not just unread, since server 1 is done
	ASSERT_EQ(result.uniqueCmpKeys[1], 0);
	ASSERT_EQ(result.mismatchedValues[1], 0);
	// Neither server reported more data, so there's nothing left to page through. In particular, the
	// implementation must not fall back to server 1's own last key ("c") as a next-page cursor just
	// because its raw reply happened to be shorter than server 0's -- that would erroneously suggest more
	// data remains to compare, when in fact the range is already fully accounted for.
	ASSERT(!result.nextKey.present());
	ASSERT(!result.success); // a real, permanent discrepancy -- this should still be reported as a failure

	co_return;
}

TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/OneUniqueValue") {
	const std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                           { "j"_sr, "v"_sr } };
	// Server B is missing "d" (unique to A). Everything else is the same
	const std::vector<KeyValueRef> serverBData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr }, { "g"_sr, "v"_sr },
		                                           { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr }, { "j"_sr, "v"_sr } };

	simulateAllConsistencyScans(serverAData, serverBData, 1, 0, 0);
	co_return;
}

TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/OneMismatchedValue") {
	const std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                           { "j"_sr, "v"_sr } };
	// Server B disagrees on "h". Everything else is the same
	const std::vector<KeyValueRef> serverBData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "h"_sr, "X"_sr }, { "i"_sr, "v"_sr },
		                                           { "j"_sr, "v"_sr } };

	simulateAllConsistencyScans(serverAData, serverBData, 0, 0, 1);
	co_return;
}

TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/MissingMiddleKeys") {
	const std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                           { "j"_sr, "v"_sr } };
	// Server B is missing keys d, e, and f.
	const std::vector<KeyValueRef> serverBData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                           { "j"_sr, "v"_sr } };

	simulateAllConsistencyScans(serverAData, serverBData, 3, 0, 0);
	co_return;
}

TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/TruncatedKeys") {
	const std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                           { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                           { "j"_sr, "v"_sr } };
	// Server B is missing everything after "e".
	const std::vector<KeyValueRef> serverBData = {
		{ "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr }, { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }
	};

	simulateAllConsistencyScans(serverAData, serverBData, 5, 0, 0);
	co_return;
}

TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/EntirelyDistinctKeys") {
	// Servers A and B alternate, and they each have a completely distinct set of keys
	const std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "c"_sr, "v"_sr }, { "e"_sr, "v"_sr },
		                                           { "g"_sr, "v"_sr }, { "i"_sr, "v"_sr }, { "j"_sr, "v"_sr } };
	const std::vector<KeyValueRef> serverBData = {
		{ "b"_sr, "v"_sr }, { "d"_sr, "v"_sr }, { "f"_sr, "v"_sr }, { "h"_sr, "X"_sr }, { "k"_sr, "v"_sr }
	};

	simulateAllConsistencyScans(serverAData, serverBData, serverAData.size(), serverBData.size(), 0);
	co_return;
}

TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/TupleEncodedKeys") {
	const std::vector<KeyValueRef> serverAData = { { ""_sr, "v"_sr },
		                                           { "\x01 bytes\x00"_sr, "v"_sr },
		                                           { "\x02 unicode\x00"_sr, "v"_sr },
		                                           { "\x13\xff"_sr, "v"_sr },
		                                           { "\x14"_sr, "v"_sr },
		                                           { "\x15\x01"_sr, "v"_sr },
		                                           { "\x15\x02\x01 abc\x00"_sr, "v"_sr },
		                                           { "\x15\x02\x01 abc\x00\x14"_sr, "v"_sr },
		                                           { "\xff/keyServers/a"_sr, "v"_sr },
		                                           { "\xff/keyServers/b"_sr, "v"_sr } };
	// Different value for \x01bytes\x00. \x02unicode\x00 is spelled wrong (missing "i"), leading to unique keys on both
	// servers. Extra key \x15\x01\x14, but missing \x15\x02\x01abc\x00 and \xff/keyServers/b. That means 1 mismatched
	// value, 3 unique keys on A, and 2 unique keys on B
	const std::vector<KeyValueRef> serverBData = { { ""_sr, "v"_sr },
		                                           { "\x01 bytes\x00"_sr, "X"_sr },
		                                           { "\x02 uncode\x00"_sr, "v"_sr },
		                                           { "\x13\xff"_sr, "v"_sr },
		                                           { "\x14"_sr, "v"_sr },
		                                           { "\x15\x01"_sr, "v"_sr },
		                                           { "\x15\x01\x04"_sr, "v"_sr },
		                                           { "\x15\x02\x01 abc\x00\x14"_sr, "v"_sr },
		                                           { "\xff/keyServers/a"_sr, "v"_sr } };

	simulateAllConsistencyScans(serverAData, serverBData, 3, 2, 1);
	co_return;
}
