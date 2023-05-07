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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TransactionTagCounter.h"
#include "flow/Trace.h"

namespace {

class TopKTags {
public:
	struct TagAndCost {
		TransactionTag tag;
		double cost;
		bool operator<(TagAndCost const& other) const { return cost < other.cost; }
		explicit TagAndCost(TransactionTag tag, double cost) : tag(tag), cost(cost) {}
	};

private:
	// Because the number of tracked is expected to be small, they can be tracked
	// in a simple vector. If the number of tracked tags increases, a more sophisticated
	// data structure will be required.
	std::vector<TagAndCost> topTags;
	int limit;

public:
	explicit TopKTags(int limit) : limit(limit) {
		ASSERT_GT(limit, 0);
		topTags.reserve(limit);
	}

	void incrementCost(TransactionTag tag, double previousCost, double increase) {
		auto iter = std::find_if(topTags.begin(), topTags.end(), [tag](const auto& tc) { return tc.tag == tag; });
		if (iter != topTags.end()) {
			ASSERT_EQ(previousCost, iter->cost);
			iter->cost += increase;
		} else if (topTags.size() < limit) {
			ASSERT_EQ(previousCost, 0);
			topTags.emplace_back(tag, increase);
		} else {
			auto toReplace = std::min_element(topTags.begin(), topTags.end());
			ASSERT_GE(toReplace->cost, previousCost);
			if (toReplace->cost < previousCost + increase) {
				toReplace->tag = tag;
				toReplace->cost = previousCost + increase;
			}
		}
	}

	std::vector<BusyTagInfo> getBusiestTags(double elapsed, double totalCost) const {
		std::vector<BusyTagInfo> result;
		for (auto const& tagAndCost : topTags) {
			auto rate = tagAndCost.cost / elapsed;
			if (rate > SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE) {
				result.emplace_back(tagAndCost.tag, rate, std::min(1.0, tagAndCost.cost / totalCost));
			}
		}
		return result;
	}

	void clear() { topTags.clear(); }
};

} // namespace

class TransactionTagCounterImpl {
	UID thisServerID;
	TransactionTagMap<double> intervalCosts;
	double intervalTotalCost = 0;
	TopKTags topTags;
	double intervalStart = 0;

	std::vector<BusyTagInfo> previousBusiestTags;
	Reference<EventCacheHolder> busiestReadTagEventHolder;

	void updateTagCost(TransactionTag tag, double additionalCost) {
		double& tagCost = intervalCosts[tag];
		topTags.incrementCost(tag, tagCost, additionalCost);
		tagCost += additionalCost;
	}

public:
	TransactionTagCounterImpl(UID thisServerID)
	  : thisServerID(thisServerID), topTags(SERVER_KNOBS->SS_THROTTLE_TAGS_TRACKED),
	    busiestReadTagEventHolder(makeReference<EventCacheHolder>(thisServerID.toString() + "/BusiestReadTag")) {}

	void addRequest(Optional<TagSet> const& tags, Optional<TenantGroupName> const& tenantGroup, int64_t bytes) {
		auto const cost = getReadOperationCost(bytes);
		intervalTotalCost += cost;
		if (tags.present()) {
			for (auto const& tag : tags.get()) {
				CODE_PROBE(true, "Tracking transaction tag in TransactionTagCounter");
				updateTagCost(TransactionTag(tag, tags.get().getArena()), cost / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
			}
		}
		if (tenantGroup.present()) {
			CODE_PROBE(true, "Tracking tenant group in TransactionTagCounter");
			updateTagCost(tenantGroup.get(), cost);
		}
	}

	void startNewInterval() {
		double elapsed = now() - intervalStart;
		previousBusiestTags.clear();
		if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
			previousBusiestTags = topTags.getBusiestTags(elapsed, intervalTotalCost);

			// For status, report the busiest tag:
			if (previousBusiestTags.empty()) {
				TraceEvent("BusiestReadTag", thisServerID).detail("TagCost", 0);
			} else {
				auto busiestTagInfo = previousBusiestTags[0];
				for (int i = 1; i < previousBusiestTags.size(); ++i) {
					auto const& tagInfo = previousBusiestTags[i];
					if (tagInfo.rate > busiestTagInfo.rate) {
						busiestTagInfo = tagInfo;
					}
				}
				TraceEvent("BusiestReadTag", thisServerID)
				    .detail("Tag", printable(busiestTagInfo.tag))
				    .detail("TagCost", busiestTagInfo.rate)
				    .detail("FractionalBusyness", busiestTagInfo.fractionalBusyness);
			}

			for (const auto& tagInfo : previousBusiestTags) {
				TraceEvent("BusyReadTag", thisServerID)
				    .detail("Tag", printable(tagInfo.tag))
				    .detail("TagCost", tagInfo.rate)
				    .detail("FractionalBusyness", tagInfo.fractionalBusyness);
			}
		}

		intervalCosts.clear();
		intervalTotalCost = 0;
		topTags.clear();
		intervalStart = now();
	}

	std::vector<BusyTagInfo> const& getBusiestTags() const { return previousBusiestTags; }
};

TransactionTagCounter::TransactionTagCounter(UID thisServerID)
  : impl(PImpl<TransactionTagCounterImpl>::create(thisServerID)) {}

TransactionTagCounter::~TransactionTagCounter() = default;

void TransactionTagCounter::addRequest(Optional<TagSet> const& tags,
                                       Optional<TenantGroupName> const& tenantGroup,
                                       int64_t bytes) {
	return impl->addRequest(tags, tenantGroup, bytes);
}

void TransactionTagCounter::startNewInterval() {
	return impl->startNewInterval();
}

std::vector<BusyTagInfo> const& TransactionTagCounter::getBusiestTags() const {
	return impl->getBusiestTags();
}

TEST_CASE("/TransactionTagCounter/TopKTags") {
	TopKTags topTags(2);

	// Ensure that costs are larger enough to show up
	auto const costMultiplier =
	    std::max<double>(1.0, 2 * SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);

	ASSERT_EQ(topTags.getBusiestTags(1.0, 0).size(), 0);
	topTags.incrementCost("a"_sr, 0, 1 * costMultiplier);
	{
		auto const busiestTags = topTags.getBusiestTags(1.0, 1 * costMultiplier);
		ASSERT_EQ(busiestTags.size(), 1);
		ASSERT_EQ(std::count_if(busiestTags.begin(),
		                        busiestTags.end(),
		                        [](auto const& tagInfo) { return tagInfo.tag == "a"_sr; }),
		          1);
	}
	topTags.incrementCost("b"_sr, 0, 2 * costMultiplier);
	topTags.incrementCost("c"_sr, 0, 3 * costMultiplier);
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
	topTags.incrementCost("a"_sr, 1 * costMultiplier, 3 * costMultiplier);
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
