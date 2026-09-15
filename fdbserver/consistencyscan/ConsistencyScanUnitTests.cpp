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

#include <algorithm>

#include "fdbserver/consistencyscan/ConsistencyScan.h"
#include "flow/UnitTest.h"

namespace {

// Builds a StorageServerInterface with real, registered endpoints. checkRangeReplies() consults
// g_simulator->getProcessByAddress(interface.address()) when running in simulation, which asserts if the
// address isn't a known simulated process; initEndpoints() binds the interface to whatever process/transport
// is actually running the test (real or simulated), so it's safe to use in either mode.
StorageServerInterface makeTestSSI() {
	StorageServerInterface ssi;
	ssi.initEndpoints();
	return ssi;
}

GetKeyValuesReply makeReply(std::vector<std::pair<KeyRef, ValueRef>> const& kvs, bool more) {
	GetKeyValuesReply reply;
	for (auto const& kv : kvs) {
		reply.data.push_back(reply.arena, KeyValueRef(kv.first, kv.second));
	}
	reply.more = more;
	reply.version = 1;
	return reply;
}

// Simulates a single storage server answering a firstGreaterThan(cursor) (or, if cursor is absent,
// firstGreaterOrEqual(range.begin)) request against its own ground-truth data, returning up to
// pageSize entries and setting `more` according to whether any of its own data is left unread.
GetKeyValuesReply pageFrom(std::vector<KeyValueRef> const& data, Optional<KeyRef> const& cursor, size_t pageSize) {
	size_t startIdx = 0;
	if (cursor.present()) {
		while (startIdx < data.size() && data[startIdx].key <= cursor.get()) {
			startIdx++;
		}
	}
	size_t endIdx = std::min(data.size(), startIdx + pageSize);
	GetKeyValuesReply reply;
	for (size_t i = startIdx; i < endIdx; i++) {
		reply.data.push_back(reply.arena, data[i]);
	}
	reply.more = endIdx < data.size();
	reply.version = 1;
	return reply;
}

// Reverse counterpart of pageFrom() above: `cursor`, if present, is an exclusive upper bound (only keys
// strictly less than it are eligible), matching how readFromAllStorageServers() resumes a backward read.
// Entries are returned in descending order, as a real storage server does for a negative-limit read.
GetKeyValuesReply pageFromReverse(std::vector<KeyValueRef> const& data,
                                  Optional<KeyRef> const& cursor,
                                  size_t pageSize) {
	size_t endIdx = data.size(); // exclusive
	if (cursor.present()) {
		while (endIdx > 0 && data[endIdx - 1].key >= cursor.get()) {
			endIdx--;
		}
	}
	size_t startIdx = endIdx >= pageSize ? endIdx - pageSize : 0;
	GetKeyValuesReply reply;
	for (size_t i = endIdx; i > startIdx; i--) {
		reply.data.push_back(reply.arena, data[i - 1]);
	}
	reply.more = startIdx > 0;
	reply.version = 1;
	return reply;
}

} // namespace

// Two servers agree on every key they both actually read ("b" and "c"), but server 0's reply happened to
// paginate further ("d", "e") before server 1's did. checkRangeReplies() should bound its comparison at
// result.nextKey (the minimum last-read key among servers still reporting more data) rather than treating
// server 1's shorter raw reply as containing missing/unique keys.
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/MisalignedPaginationNotOverreported") {
	std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	KeyRangeRef range("a"_sr, "z"_sr);
	KeySelector begin = firstGreaterOrEqual(range.begin);

	std::vector<ErrorOr<GetKeyValuesReply>> replies;
	replies.push_back(ErrorOr<GetKeyValuesReply>(
	    makeReply({ { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr }, { "d"_sr, "1"_sr }, { "e"_sr, "1"_sr } },
	              /*more=*/true)));
	replies.push_back(ErrorOr<GetKeyValuesReply>(makeReply({ { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr } }, /*more=*/true)));

	RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin, /*performQuiescentChecks=*/false);

	ASSERT_EQ(result.firstValidServer, 0);
	ASSERT(result.nextKey.present());
	ASSERT(result.nextKey.get() == "c"_sr);
	ASSERT(result.lastReadKey.present());
	ASSERT(result.lastReadKey.get() == "c"_sr);
	ASSERT_EQ(result.uniqueRefKeys[1], 0);
	ASSERT_EQ(result.uniqueCmpKeys[1], 0);
	ASSERT_EQ(result.mismatchedValues[1], 0);
	ASSERT(result.success);

	co_return;
}

// Positive control for the test above: within the bounded comparison window, server 1 is actually missing "c"
// (a genuine gap, not just an artifact of pagination), so checkRangeReplies() should still catch it.
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/GenuineMissingKeyStillDetected") {
	std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	KeyRangeRef range("a"_sr, "z"_sr);
	KeySelector begin = firstGreaterOrEqual(range.begin);

	std::vector<ErrorOr<GetKeyValuesReply>> replies;
	replies.push_back(ErrorOr<GetKeyValuesReply>(
	    makeReply({ { "b"_sr, "1"_sr }, { "c"_sr, "1"_sr }, { "d"_sr, "1"_sr } }, /*more=*/false)));
	replies.push_back(
	    ErrorOr<GetKeyValuesReply>(makeReply({ { "b"_sr, "1"_sr }, { "d"_sr, "1"_sr } }, /*more=*/false)));

	RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin, /*performQuiescentChecks=*/false);

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
	std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	KeyRangeRef range("a"_sr, "z"_sr);
	KeySelector begin = firstGreaterOrEqual(range.begin);

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

	RangeConsistencyResult result = checkRangeReplies(servers, replies, range, begin, /*performQuiescentChecks=*/false);

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

// Drives checkRangeReplies() across multiple rounds the way checkDataConsistency() does: each round
// pages every server's ground-truth data independently (so the two servers' page boundaries drift out
// of sync with each other, exactly like real, differently-sized replies would), feeds the replies
// through checkRangeReplies(), and resumes the next round from the returned nextKey. This validates
// that the *cumulative* unique/mismatch counts across all rounds land exactly on the errors that were
// planted below -- no more (the overreporting bug this refactor fixed) and no less (silently swallowing
// a real error).
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/MultiRoundPaginationMatchesPlantedErrors") {
	std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                     { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                     { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                     { "j"_sr, "v"_sr } };
	// Server B is missing "d" (unique to A), has an extra "ee" that A doesn't have (unique to B), and
	// disagrees with A on the value of "h". Everything else matches exactly. Since B is missing one key
	// and has one extra key on either side of "d"/"ee", its page boundaries desync from and then resync
	// with A's as pagination proceeds -- the scenario this refactor was meant to handle correctly.
	std::vector<KeyValueRef> serverBData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr },  { "c"_sr, "v"_sr },
		                                     { "e"_sr, "v"_sr }, { "ee"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                     { "g"_sr, "v"_sr }, { "h"_sr, "X"_sr },  { "i"_sr, "v"_sr },
		                                     { "j"_sr, "v"_sr } };
	std::vector<std::vector<KeyValueRef>> serverData = { serverAData, serverBData };
	KeyRangeRef range("a"_sr, "z"_sr);
	constexpr size_t pageSize = 3;

	std::vector<int64_t> totalUniqueRefKeys(servers.size(), 0);
	std::vector<int64_t> totalUniqueCmpKeys(servers.size(), 0);
	std::vector<int64_t> totalMismatchedValues(servers.size(), 0);

	Optional<KeyRef> cursor;
	int round = 0;
	for (; round < 20; round++) {
		KeySelector begin = cursor.present() ? firstGreaterThan(cursor.get()) : firstGreaterOrEqual(range.begin);
		std::vector<ErrorOr<GetKeyValuesReply>> replies;
		replies.reserve(serverData.size());
		for (auto const& data : serverData) {
			replies.push_back(ErrorOr<GetKeyValuesReply>(pageFrom(data, cursor, pageSize)));
		}

		RangeConsistencyResult result =
		    checkRangeReplies(servers, replies, range, begin, /*performQuiescentChecks=*/false);
		for (int i = 0; i < result.uniqueRefKeys.size(); i++) {
			totalUniqueRefKeys[i] += result.uniqueRefKeys[i];
			totalUniqueCmpKeys[i] += result.uniqueCmpKeys[i];
			totalMismatchedValues[i] += result.mismatchedValues[i];
		}

		if (!result.nextKey.present()) {
			break;
		}
		cursor = result.nextKey;
	}
	ASSERT(round < 20); // otherwise pagination never converged -- likely a bug in the test, or a real regression

	ASSERT_EQ(totalUniqueRefKeys[1], 1); // "d"
	ASSERT_EQ(totalUniqueCmpKeys[1], 1); // "ee"
	ASSERT_EQ(totalMismatchedValues[1], 1); // "h"

	co_return;
}

// Reverse counterpart of the test above: same planted errors, but paginated backward (descending), the way
// readFromAllStorageServers()/checkRangeReplies() page when called with Reverse::True -- e.g. by
// ConsistencyCheckUrgent when CONSISTENCY_CHECK_BACKWARD_READ is set. Validates that the cumulative
// unique/mismatch counts land exactly on the planted errors when merging descending-sorted replies, which
// exercises the reverse-specific comparisons in diffReplies()/checkRangeReplies() (bound direction, "least
// progress" server selection, etc.) that the forward test above can't reach.
TEST_CASE("/fdbserver/consistencyscan/checkRangeReplies/ReverseMultiRoundPaginationMatchesPlantedErrors") {
	std::vector<StorageServerInterface> servers = { makeTestSSI(), makeTestSSI() };
	std::vector<KeyValueRef> serverAData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr }, { "c"_sr, "v"_sr },
		                                     { "d"_sr, "v"_sr }, { "e"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                     { "g"_sr, "v"_sr }, { "h"_sr, "v"_sr }, { "i"_sr, "v"_sr },
		                                     { "j"_sr, "v"_sr } };
	// Same planted errors as the forward test: B is missing "d", has an extra "ee", and disagrees on "h".
	std::vector<KeyValueRef> serverBData = { { "a"_sr, "v"_sr }, { "b"_sr, "v"_sr },  { "c"_sr, "v"_sr },
		                                     { "e"_sr, "v"_sr }, { "ee"_sr, "v"_sr }, { "f"_sr, "v"_sr },
		                                     { "g"_sr, "v"_sr }, { "h"_sr, "X"_sr },  { "i"_sr, "v"_sr },
		                                     { "j"_sr, "v"_sr } };
	std::vector<std::vector<KeyValueRef>> serverData = { serverAData, serverBData };
	KeyRangeRef range("a"_sr, "z"_sr);
	constexpr size_t pageSize = 3;

	std::vector<int64_t> totalUniqueRefKeys(servers.size(), 0);
	std::vector<int64_t> totalUniqueCmpKeys(servers.size(), 0);
	std::vector<int64_t> totalMismatchedValues(servers.size(), 0);

	Optional<KeyRef> cursor; // exclusive upper bound; absent means "start from range.end"
	int round = 0;
	for (; round < 20; round++) {
		KeySelector begin = cursor.present() ? firstGreaterOrEqual(cursor.get()) : firstGreaterOrEqual(range.end);
		std::vector<ErrorOr<GetKeyValuesReply>> replies;
		replies.reserve(serverData.size());
		for (auto const& data : serverData) {
			replies.push_back(ErrorOr<GetKeyValuesReply>(pageFromReverse(data, cursor, pageSize)));
		}

		RangeConsistencyResult result =
		    checkRangeReplies(servers, replies, range, begin, /*performQuiescentChecks=*/false, Reverse::True);
		for (int i = 0; i < result.uniqueRefKeys.size(); i++) {
			totalUniqueRefKeys[i] += result.uniqueRefKeys[i];
			totalUniqueCmpKeys[i] += result.uniqueCmpKeys[i];
			totalMismatchedValues[i] += result.mismatchedValues[i];
		}

		if (!result.nextKey.present()) {
			break;
		}
		cursor = result.nextKey;
	}
	ASSERT(round < 20); // otherwise pagination never converged -- likely a bug in the test, or a real regression

	ASSERT_EQ(totalUniqueRefKeys[1], 1); // "d"
	ASSERT_EQ(totalUniqueCmpKeys[1], 1); // "ee"
	ASSERT_EQ(totalMismatchedValues[1], 1); // "h"

	co_return;
}
