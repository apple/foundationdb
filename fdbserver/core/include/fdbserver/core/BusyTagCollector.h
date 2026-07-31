/*
 * BusyTagCollector.h
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

#include <functional>
#include <queue>
#include <vector>

#include "fdbclient/StorageServerInterface.h"

class BusyTagCollector {
	std::priority_queue<BusyTagInfo, std::vector<BusyTagInfo>, std::greater<BusyTagInfo>> busiestTags;
	size_t maxTagsTracked;
	double minRateTracked;

public:
	BusyTagCollector(int maxTagsTracked, double minRateTracked)
	  : maxTagsTracked(maxTagsTracked), minRateTracked(minRateTracked) {}

	bool add(const TransactionTag& tag, double rate, double fractionalBusyness) {
		if (rate < minRateTracked) {
			return false;
		}
		if (busiestTags.size() < maxTagsTracked) {
			busiestTags.emplace(tag, rate, fractionalBusyness);
		} else if (busiestTags.top().rate < rate) {
			busiestTags.pop();
			busiestTags.emplace(tag, rate, fractionalBusyness);
		}
		return true;
	}

	void drainInto(std::vector<BusyTagInfo>& result) {
		while (!busiestTags.empty()) {
			result.push_back(busiestTags.top());
			busiestTags.pop();
		}
	}
};
