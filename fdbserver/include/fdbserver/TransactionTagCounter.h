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

class TransactionTagCounter {
	TransactionTagMap<int64_t> intervalCounts;
	int64_t intervalTotalSampledCount = 0;
	TransactionTag busiestTag;
	int64_t busiestTagCount = 0;
	double intervalStart = 0;

	std::vector<StorageQueuingMetricsReply::TagInfo> previousBusiestTags;
	UID thisServerID;
	Reference<EventCacheHolder> busiestReadTagEventHolder;

public:
	TransactionTagCounter(UID thisServerID);
	static int64_t costFunction(int64_t bytes) { return bytes / SERVER_KNOBS->READ_COST_BYTE_FACTOR + 1; }
	void addRequest(Optional<TagSet> const& tags, int64_t bytes);
	void startNewInterval();
	std::vector<StorageQueuingMetricsReply::TagInfo> const& getBusiestTags() const { return previousBusiestTags; }
};
