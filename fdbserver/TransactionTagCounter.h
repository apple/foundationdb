/*
 * TransactionTagCounter.h
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

#pragma once

#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/Knobs.h"

class TopKTags {
public:
	struct TagAndCount {
		TransactionTag tag;
		int64_t count;
		bool operator<(TagAndCount const& other) const { return count < other.count; }
		explicit TagAndCount(TransactionTag tag, int64_t count) : tag(tag), count(count) {}
	};

private:
	// Because the number of tracked is expected to be small, they can be tracked
	// in a simple vector. If the number of tracked tags increases, a more sophisticated
	// data structure will be required.
	std::vector<TagAndCount> topTags;
	int limit;

public:
	explicit TopKTags(int limit) : limit(limit) { ASSERT_GT(limit, 0); }
	void incrementTagCount(TransactionTag tag, int previousCount, int increase);

	std::vector<StorageQueuingMetricsReply::TagInfo> getBusiestTags(double elapsed, double totalSampleCount) const;

	void clear() { topTags.clear(); }
};

class TransactionTagCounter {
	UID thisServerID;
	TransactionTagMap<int64_t> intervalCounts;
	int64_t intervalTotalSampledCount = 0;
	TopKTags topTags;
	double intervalStart = 0;

	std::vector<StorageQueuingMetricsReply::TagInfo> previousBusiestTags;
	Reference<EventCacheHolder> busiestReadTagEventHolder;

public:
	TransactionTagCounter(UID thisServerID);
	static int64_t costFunction(int64_t bytes) { return bytes / SERVER_KNOBS->READ_COST_BYTE_FACTOR + 1; }
	void addRequest(Optional<TagSet> const& tags, int64_t bytes);
	void startNewInterval();
	std::vector<StorageQueuingMetricsReply::TagInfo> const& getBusiestTags() const { return previousBusiestTags; }
};
