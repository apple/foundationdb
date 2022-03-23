/*
 * TransactionTagCounter.cpp
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

#include "fdbserver/TransactionTagCounter.h"
#include "flow/Trace.h"

TransactionTagCounter::TransactionTagCounter(UID thisServerID)
  : thisServerID(thisServerID),
    busiestReadTagEventHolder(makeReference<EventCacheHolder>(thisServerID.toString() + "/BusiestReadTag")) {}

void TransactionTagCounter::addRequest(Optional<TagSet> const& tags, int64_t bytes) {
	if (tags.present()) {
		TEST(true); // Tracking transaction tag in counter
		double cost = costFunction(bytes);
		for (auto& tag : tags.get()) {
			int64_t& count = intervalCounts[TransactionTag(tag, tags.get().getArena())];
			count += cost;
			if (count > busiestTagCount) {
				busiestTagCount = count;
				busiestTag = tag;
			}
		}

		intervalTotalSampledCount += cost;
	}
}

void TransactionTagCounter::startNewInterval() {
	double elapsed = now() - intervalStart;
	previousBusiestTags.clear();
	if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
		double rate = busiestTagCount / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE / elapsed;
		if (rate > SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE) {
			previousBusiestTags.emplace_back(busiestTag, rate, (double)busiestTagCount / intervalTotalSampledCount);
		}

		TraceEvent("BusiestReadTag", thisServerID)
		    .detail("Elapsed", elapsed)
		    .detail("Tag", printable(busiestTag))
		    .detail("TagCost", busiestTagCount)
		    .detail("TotalSampledCost", intervalTotalSampledCount)
		    .detail("Reported", !previousBusiestTags.empty())
		    .trackLatest(busiestReadTagEventHolder->trackingKey);
	}

	intervalCounts.clear();
	intervalTotalSampledCount = 0;
	busiestTagCount = 0;
	intervalStart = now();
}
