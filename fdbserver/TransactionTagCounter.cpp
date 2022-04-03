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

void TopKTags::incrementCount(TransactionTag tag, int previousCount, int increase) {
	auto iter = std::find_if(topTags.begin(), topTags.end(), [tag](const auto& tc) { return tc.tag == tag; });
	if (iter != topTags.end()) {
		ASSERT_EQ(previousCount, iter->count);
		iter->count += increase;
	} else if (topTags.size() < limit) {
		ASSERT_EQ(previousCount, 0);
		topTags.emplace_back(tag, increase);
	} else {
		auto toReplace = std::min_element(topTags.begin(), topTags.end());
		ASSERT_GE(toReplace->count, previousCount);
		if (toReplace->count < previousCount + increase) {
			toReplace->tag = tag;
			toReplace->count = previousCount + increase;
		}
	}
}

std::vector<StorageQueuingMetricsReply::TagInfo> TopKTags::getBusiestTags(double elapsed,
                                                                          double totalSampleCount) const {
	std::vector<StorageQueuingMetricsReply::TagInfo> result;
	for (auto const& tagAndCounter : topTags) {
		auto rate = (tagAndCounter.count / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE) / elapsed;
		if (rate > SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE) {
			result.emplace_back(tagAndCounter.tag, rate, tagAndCounter.count / totalSampleCount);
		}
	}
	return result;
}

TransactionTagCounter::TransactionTagCounter(UID thisServerID)
  : thisServerID(thisServerID), topTags(1), // TODO: Make this a knob
    busiestReadTagEventHolder(makeReference<EventCacheHolder>(thisServerID.toString() + "/BusiestReadTag")) {}

void TransactionTagCounter::addRequest(Optional<TagSet> const& tags, int64_t bytes) {
	if (tags.present()) {
		TEST(true); // Tracking transaction tag in counter
		double cost = costFunction(bytes);
		for (auto& tag : tags.get()) {
			int64_t& count = intervalCounts[TransactionTag(tag, tags.get().getArena())];
			topTags.incrementCount(tag, count, cost);
			count += cost;
		}

		intervalTotalSampledCount += cost;
	}
}

void TransactionTagCounter::startNewInterval() {
	double elapsed = now() - intervalStart;
	previousBusiestTags.clear();
	if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
		previousBusiestTags = topTags.getBusiestTags(elapsed, intervalTotalSampledCount);

		TraceEvent("BusiestReadTag", thisServerID)
		    .detail("Elapsed", elapsed)
		    //.detail("Tag", printable(busiestTag))
		    //.detail("TagCost", busiestTagCount)
		    .detail("TotalSampledCost", intervalTotalSampledCount)
		    .detail("Reported", previousBusiestTags.size())
		    .trackLatest(busiestReadTagEventHolder->trackingKey);
	}

	intervalCounts.clear();
	intervalTotalSampledCount = 0;
	topTags.clear();
	intervalStart = now();
}

TEST_CASE("/TransactionTagCounter/TopKTags") {
	TopKTags topTags(2);

	// Ensure that costs are larger enough to show up
	auto const costMultiplier =
	    std::max<double>(1.0, 2 * SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE * CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);

	ASSERT_EQ(topTags.getBusiestTags(1.0, 0).size(), 0);
	topTags.incrementCount("a"_sr, 0, 1 * costMultiplier);
	{
		auto const busiestTags = topTags.getBusiestTags(1.0, 1 * costMultiplier);
		ASSERT_EQ(busiestTags.size(), 1);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "a"_sr; }),
		          1);
	}
	topTags.incrementCount("b"_sr, 0, 2 * costMultiplier);
	topTags.incrementCount("c"_sr, 0, 3 * costMultiplier);
	{
		auto busiestTags = topTags.getBusiestTags(1.0, 6 * costMultiplier);
		ASSERT_EQ(busiestTags.size(), 2);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "a"_sr; }),
		          0);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "b"_sr; }),
		          1);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "c"_sr; }),
		          1);
	}
	topTags.incrementCount("a"_sr, 1 * costMultiplier, 3 * costMultiplier);
	{
		auto busiestTags = topTags.getBusiestTags(1.0, 9 * costMultiplier);
		ASSERT_EQ(busiestTags.size(), 2);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "a"_sr; }),
		          1);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "b"_sr; }),
		          0);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "c"_sr; }),
		          1);
	}
	topTags.clear();
	ASSERT_EQ(topTags.getBusiestTags(1.0, 0).size(), 0);
	return Void();
}
