/*
 * RangeCompare.h
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

#pragma once

#ifndef FDBSERVER_RANGE_COMPARE_H
#define FDBSERVER_RANGE_COMPARE_H

#include <algorithm>
#include <string>
#include <vector>

#include "fdbclient/NativeAPI.h"
#include "flow/flow.h"
#include "flow/genericactors.h"

// Comparison of the key-values of two key ranges that hold the same keys under two different prefixes, by reading both
// from the client in parallel shards. Both ranges must not be written while they are compared: the pages of the two
// sides are read at different versions.

struct RangeCompareReport {
	int64_t leftKeys = 0;
	int64_t rightKeys = 0;
	int64_t leftBytes = 0;
	int64_t rightBytes = 0;
	// Keys present on one side only, and keys whose values differ.
	int64_t differences = 0;
	int shards = 0;
	// The first differences, as text, at most the number asked for.
	std::vector<std::string> firstDifferences;

	void add(RangeCompareReport const& other, int maxReported) {
		leftKeys += other.leftKeys;
		rightKeys += other.rightKeys;
		leftBytes += other.leftBytes;
		rightBytes += other.rightBytes;
		differences += other.differences;
		shards += other.shards;
		for (auto const& difference : other.firstDifferences) {
			if (static_cast<int>(firstDifferences.size()) < maxReported) {
				firstDifferences.push_back(difference);
			}
		}
	}
};

namespace RangeCompare {

inline constexpr int defaultPageRows = 5000;
inline constexpr int pageBytes = 1000000;

inline Future<RangeResult> readPage(Database cx, KeyRange range, int pageRows) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			RangeResult page = co_await tr.getRange(range, GetRangeLimits(pageRows, pageBytes));
			co_return page;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

inline void addDifference(RangeCompareReport* report, int maxReported, std::string const& what, StringRef key) {
	++report->differences;
	if (static_cast<int>(report->firstDifferences.size()) < maxReported) {
		report->firstDifferences.push_back(what + " " + printable(key.substr(0, std::min<int>(key.size(), 100))));
	}
}

// Compares [left.begin, left.end) with [right.begin, right.end), whose keys are the left keys with the prefix of
// `leftPrefixSize` bytes replaced by the prefix of `rightPrefixSize` bytes.
inline Future<Void> compareShard(Database cx,
                                 KeyRange left,
                                 size_t leftPrefixSize,
                                 KeyRange right,
                                 size_t rightPrefixSize,
                                 RangeCompareReport* report,
                                 int maxReported,
                                 int pageRows) {
	Key leftNext = left.begin;
	Key rightNext = right.begin;
	RangeResult leftPage;
	RangeResult rightPage;
	size_t li = 0;
	size_t ri = 0;
	bool leftMore = true;
	bool rightMore = true;
	while (true) {
		if (li == leftPage.size() && leftMore) {
			leftPage = co_await readPage(cx, KeyRangeRef(leftNext, left.end), pageRows);
			li = 0;
			leftMore = leftPage.more;
			if (!leftPage.empty()) {
				leftNext = keyAfter(leftPage.back().key);
			}
		}
		if (ri == rightPage.size() && rightMore) {
			rightPage = co_await readPage(cx, KeyRangeRef(rightNext, right.end), pageRows);
			ri = 0;
			rightMore = rightPage.more;
			if (!rightPage.empty()) {
				rightNext = keyAfter(rightPage.back().key);
			}
		}
		bool haveLeft = li < leftPage.size();
		bool haveRight = ri < rightPage.size();
		if (!haveLeft && !haveRight) {
			break;
		}
		StringRef leftKey = haveLeft ? leftPage[li].key.substr(leftPrefixSize) : StringRef();
		StringRef rightKey = haveRight ? rightPage[ri].key.substr(rightPrefixSize) : StringRef();
		if (haveLeft && (!haveRight || leftKey < rightKey)) {
			++report->leftKeys;
			report->leftBytes += leftPage[li].key.size() + leftPage[li].value.size();
			addDifference(report, maxReported, "only in left:", leftKey);
			++li;
		} else if (haveRight && (!haveLeft || rightKey < leftKey)) {
			++report->rightKeys;
			report->rightBytes += rightPage[ri].key.size() + rightPage[ri].value.size();
			addDifference(report, maxReported, "only in right:", rightKey);
			++ri;
		} else {
			++report->leftKeys;
			++report->rightKeys;
			report->leftBytes += leftPage[li].key.size() + leftPage[li].value.size();
			report->rightBytes += rightPage[ri].key.size() + rightPage[ri].value.size();
			if (leftPage[li].value != rightPage[ri].value) {
				addDifference(report, maxReported, "different value:", leftKey);
			}
			++li;
			++ri;
		}
	}
}

inline Future<Void> compareShardLimited(FlowLock* lock,
                                        Database cx,
                                        KeyRange left,
                                        size_t leftPrefixSize,
                                        KeyRange right,
                                        size_t rightPrefixSize,
                                        RangeCompareReport* report,
                                        int maxReported,
                                        int pageRows) {
	co_await lock->take();
	FlowLock::Releaser releaser(*lock);
	co_await compareShard(cx, left, leftPrefixSize, right, rightPrefixSize, report, maxReported, pageRows);
}

} // namespace RangeCompare

// Compares the keys of `range` under `leftPrefix` with the same keys under `rightPrefix`: the key-values of
// [leftPrefix + range.begin, leftPrefix + range.end) against those of [rightPrefix + range.begin,
// rightPrefix + range.end), with the prefixes removed. The ranges are cut into shards of about `shardBytes` along the
// split points of the left side, `parallelism` shards are compared at a time, and each side is read `pageRows` rows at
// a time.
inline Future<RangeCompareReport> compareRanges(Database cx,
                                                KeyRange range,
                                                Key leftPrefix,
                                                Key rightPrefix,
                                                int parallelism = 8,
                                                int64_t shardBytes = 50000000,
                                                int maxReported = 10,
                                                int pageRows = RangeCompare::defaultPageRows) {
	KeyRange left(KeyRangeRef(range.begin.withPrefix(leftPrefix), range.end.withPrefix(leftPrefix)));
	Standalone<VectorRef<KeyRef>> splits;
	{
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				splits = co_await tr.getRangeSplitPoints(left, shardBytes);
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}
	std::vector<Key> bounds;
	bounds.push_back(left.begin);
	for (auto const& split : splits) {
		if (split > bounds.back() && split < left.end) {
			bounds.push_back(split);
		}
	}
	bounds.push_back(left.end);

	RangeCompareReport report;
	FlowLock lock(parallelism);
	std::vector<Future<Void>> shards;
	for (size_t i = 0; i + 1 < bounds.size(); ++i) {
		KeyRange shardLeft(KeyRangeRef(bounds[i], bounds[i + 1]));
		KeyRange shardRight(KeyRangeRef(bounds[i].removePrefix(leftPrefix).withPrefix(rightPrefix),
		                                bounds[i + 1].removePrefix(leftPrefix).withPrefix(rightPrefix)));
		shards.push_back(RangeCompare::compareShardLimited(
		    &lock, cx, shardLeft, leftPrefix.size(), shardRight, rightPrefix.size(), &report, maxReported, pageRows));
	}
	report.shards = static_cast<int>(shards.size());
	co_await waitForAll(shards);
	co_return report;
}

#endif
