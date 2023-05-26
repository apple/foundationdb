/*
 * TransactionTagCounter.actor.cpp
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
#include "flow/actorcompiler.h"

class TransactionTagCounterImpl {
	UID thisServerID;
	TransactionTagMap<double> intervalCosts;
	double intervalTotalCost = 0;
	double intervalStart = 0;
	int maxTagsTracked;
	double minRateTracked;

	std::vector<BusyTagInfo> previousBusiestTags;
	Reference<EventCacheHolder> busiestReadTagEventHolder;

	std::vector<BusyTagInfo> getBusiestTagsFromLastInterval(double elapsed) const {
		std::priority_queue<BusyTagInfo, std::vector<BusyTagInfo>, std::greater<BusyTagInfo>> topKTags;
		for (auto const& [tag, cost] : intervalCosts) {
			auto const rate = cost / elapsed;
			auto const fractionalBusyness = std::min(1.0, cost / intervalTotalCost);
			if (rate < minRateTracked) {
				continue;
			} else if (topKTags.size() < maxTagsTracked) {
				topKTags.emplace(tag, rate, fractionalBusyness);
			} else if (topKTags.top().rate < rate) {
				topKTags.pop();
				topKTags.emplace(tag, rate, fractionalBusyness);
			}
		}
		std::vector<BusyTagInfo> result;
		while (!topKTags.empty()) {
			result.push_back(std::move(topKTags.top()));
			topKTags.pop();
		}
		return result;
	}

public:
	TransactionTagCounterImpl(UID thisServerID, int maxTagsTracked, double minRateTracked)
	  : thisServerID(thisServerID), maxTagsTracked(maxTagsTracked), minRateTracked(minRateTracked),
	    busiestReadTagEventHolder(makeReference<EventCacheHolder>(thisServerID.toString() + "/BusiestReadTag")) {}

	void addRequest(Optional<TagSet> const& tags, int64_t bytes) {
		auto const cost = getReadOperationCost(bytes);
		intervalTotalCost += cost;
		if (tags.present()) {
			for (auto const& tag : tags.get()) {
				CODE_PROBE(true, "Tracking transaction tag in TransactionTagCounter");
				intervalCosts[TransactionTag(tag, tags.get().getArena())] += cost / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE;
			}
		}
	}

	void startNewInterval() {
		double elapsed = now() - intervalStart;
		previousBusiestTags.clear();
		if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
			previousBusiestTags = getBusiestTagsFromLastInterval(elapsed);

			// For status, report the busiest tag:
			if (previousBusiestTags.empty()) {
				TraceEvent("BusiestReadTag", thisServerID).detail("TagCost", 0.0);
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
		intervalStart = now();
	}

	std::vector<BusyTagInfo> const& getBusiestTags() const { return previousBusiestTags; }
};

TransactionTagCounter::TransactionTagCounter(UID thisServerID, int maxTagsTracked, double minRateTracked)
  : impl(PImpl<TransactionTagCounterImpl>::create(thisServerID, maxTagsTracked, minRateTracked)) {}

TransactionTagCounter::~TransactionTagCounter() = default;

void TransactionTagCounter::addRequest(Optional<TagSet> const& tags, int64_t bytes) {
	return impl->addRequest(tags, bytes);
}

void TransactionTagCounter::startNewInterval() {
	return impl->startNewInterval();
}

std::vector<BusyTagInfo> const& TransactionTagCounter::getBusiestTags() const {
	return impl->getBusiestTags();
}

namespace {

bool containsTag(std::vector<BusyTagInfo> const& busyTags, TransactionTagRef tag) {
	return std::count_if(busyTags.begin(), busyTags.end(), [tag](auto const& tagInfo) { return tagInfo.tag == tag; }) ==
	       1;
}

TagSet getTagSet(TransactionTagRef tag) {
	TagSet result;
	result.addTag(tag);
	return result;
}

} // namespace

TEST_CASE("/fdbserver/TransactionTagCounter/IgnoreBeyondMaxTags") {
	state TransactionTagCounter counter(UID(),
	                                    /*maxTagsTracked=*/2,
	                                    /*minRateTracked=*/10.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE /
	                                        CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
	counter.startNewInterval();
	ASSERT_EQ(counter.getBusiestTags().size(), 0);
	{
		wait(delay(1.0));
		counter.addRequest(getTagSet("tagA"_sr), 10 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.addRequest(getTagSet("tagA"_sr), 10 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.addRequest(getTagSet("tagB"_sr), 15 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.addRequest(getTagSet("tagC"_sr), 20 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.startNewInterval();
		auto const busiestTags = counter.getBusiestTags();
		ASSERT_EQ(busiestTags.size(), 2);
		ASSERT(containsTag(busiestTags, "tagA"_sr));
		ASSERT(!containsTag(busiestTags, "tagB"_sr));
		ASSERT(containsTag(busiestTags, "tagC"_sr));
	}
	return Void();
}

TEST_CASE("/fdbserver/TransactionTagCounter/IgnoreBelowMinRate") {
	state TransactionTagCounter counter(UID(),
	                                    /*maxTagsTracked=*/2,
	                                    /*minRateTracked=*/10.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE /
	                                        CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
	counter.startNewInterval();
	ASSERT_EQ(counter.getBusiestTags().size(), 0);
	{
		wait(delay(1.0));
		counter.addRequest(getTagSet("tagA"_sr), 5 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.startNewInterval();
		auto const busiestTags = counter.getBusiestTags();
		ASSERT_EQ(busiestTags.size(), 0);
	}
	return Void();
}
