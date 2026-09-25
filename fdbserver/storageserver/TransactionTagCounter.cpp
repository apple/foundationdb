/*
 * TransactionTagCounter.cpp
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

#include "fdbclient/NativeAPI.h"
#include "fdbserver/core/BusyTagCollector.h"
#include "fdbserver/core/Knobs.h"
#include "TransactionTagCounter.h"
#include "flow/Coroutines.h"
#include "flow/Trace.h"

class TransactionTagCounterImpl {
	UID thisServerID;
	TransactionTagMap<double> intervalCosts;
	double intervalTotalCost = 0;
	double intervalStart = 0;
	int maxTagsTracked;
	double minRateTracked;

	std::vector<BusyTagInfo> previousBusiestTags;
	Reference<EventCacheHolder> busiestReadTagEventHolder;
	// True once an idle (TagCost: 0) interval has already been reported, so we don't repeat it
	// every interval while nothing changes. Cleared as soon as a real busy tag is seen again.
	bool wasIdleLastInterval = false;

	std::vector<BusyTagInfo> getBusiestTagsFromLastInterval(double elapsed) const {
		BusyTagCollector busiestTags(maxTagsTracked, minRateTracked);
		for (auto const& [tag, cost] : intervalCosts) {
			auto const rate = cost / elapsed;
			auto const fractionalBusyness = std::min(1.0, cost / intervalTotalCost);
			busiestTags.add(tag, rate, fractionalBusyness);
		}
		std::vector<BusyTagInfo> result;
		busiestTags.drainInto(result);
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
				if (!wasIdleLastInterval) {
					TraceEvent("BusiestReadTag", thisServerID)
					    .detail("TagCost", 0.0)
					    .trackLatest(busiestReadTagEventHolder->trackingKey);
				}
				wasIdleLastInterval = true;
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
				    .detail("FractionalBusyness", busiestTagInfo.fractionalBusyness)
				    .trackLatest(busiestReadTagEventHolder->trackingKey);
				wasIdleLastInterval = false;
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
	UID const thisServerID(1, 1);
	TransactionTagCounter counter(thisServerID,
	                              /*maxTagsTracked=*/2,
	                              /*minRateTracked=*/10.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE /
	                                  CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
	counter.startNewInterval();
	ASSERT_EQ(counter.getBusiestTags().size(), 0);
	co_await delay(1.0);
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
	// BusiestReadTag must reach latestEventCache (and from there, fdbcli status's
	// "busiest_read_tag" field via EventLogRequest) -- not just be computed in memory.
	ASSERT(latestEventCache.get(thisServerID.toString() + "/BusiestReadTag").size() > 0);
	co_return;
}

TEST_CASE("/fdbserver/TransactionTagCounter/IgnoreBelowMinRate") {
	UID const thisServerID(2, 2);
	TransactionTagCounter counter(thisServerID,
	                              /*maxTagsTracked=*/2,
	                              /*minRateTracked=*/10.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE /
	                                  CLIENT_KNOBS->READ_TAG_SAMPLE_RATE);
	counter.startNewInterval();
	ASSERT_EQ(counter.getBusiestTags().size(), 0);
	co_await delay(1.0);
	counter.addRequest(getTagSet("tagA"_sr), 5 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	counter.startNewInterval();
	auto const busiestTags = counter.getBusiestTags();
	ASSERT_EQ(busiestTags.size(), 0);
	// Even with no busy tag this interval, BusiestReadTag still logs (TagCost: 0) and that
	// zero-cost report must still reach latestEventCache -- this is the actual production
	// case: every real cluster we checked reports TagCost=0 100% of the time.
	TraceEventFields const& latest = latestEventCache.get(thisServerID.toString() + "/BusiestReadTag");
	ASSERT(latest.size() > 0);
	ASSERT_EQ(latest.getInt64("TagCost"), 0);
	co_return;
}
