/*
 * ThrottlingCounter.actor.cpp
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
#include "fdbserver/ThrottlingCounter.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h"

class ThrottlingCounterImpl {
	UID thisServerID;
	ThrottlingIdMap<double> intervalCosts;
	double intervalTotalCost = 0;
	double intervalStart = 0;
	int maxReadersTracked;
	double minRateTracked;

	std::vector<BusyThrottlingIdInfo> previousBusiestReaders;
	Reference<EventCacheHolder> busiestReaderEventHolder;

	std::vector<BusyThrottlingIdInfo> getBusiestReadersFromLastInterval(double elapsed) const {
		std::priority_queue<BusyThrottlingIdInfo, std::vector<BusyThrottlingIdInfo>, std::greater<BusyThrottlingIdInfo>>
		    topKReaders;
		for (auto const& [readerId, cost] : intervalCosts) {
			auto const rate = cost / elapsed;
			auto const fractionalBusyness = std::min(1.0, cost / intervalTotalCost);
			if (rate < minRateTracked) {
				continue;
			} else if (topKReaders.size() < maxReadersTracked) {
				topKReaders.emplace(readerId, rate, fractionalBusyness);
			} else if (topKReaders.top().rate < rate) {
				topKReaders.pop();
				topKReaders.emplace(readerId, rate, fractionalBusyness);
			}
		}
		std::vector<BusyThrottlingIdInfo> result;
		while (!topKReaders.empty()) {
			result.push_back(std::move(topKReaders.top()));
			topKReaders.pop();
		}
		return result;
	}

public:
	ThrottlingCounterImpl(UID thisServerID, int maxReadersTracked, double minRateTracked)
	  : thisServerID(thisServerID), maxReadersTracked(maxReadersTracked), minRateTracked(minRateTracked),
	    busiestReaderEventHolder(makeReference<EventCacheHolder>(thisServerID.toString() + "/BusiestReader")) {}

	void addRequest(Optional<TagSet> const& tags, Optional<TenantGroupName> const& tenantGroup, int64_t bytes) {
		auto const cost = getReadOperationCost(bytes);
		intervalTotalCost += cost;
		if (tags.present()) {
			for (auto const& tag : tags.get()) {
				CODE_PROBE(true, "Tracking transaction tag in ThrottlingCounter");
				intervalCosts[ThrottlingIdRef::fromTag(tag)] += cost / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE;
			}
		}
		if (tenantGroup.present()) {
			CODE_PROBE(true, "Tracking tenant group in ThrottlingCounter");
			intervalCosts[ThrottlingIdRef::fromTenantGroup(tenantGroup.get())] += cost;
		}
	}

	void startNewInterval() {
		double elapsed = now() - intervalStart;
		previousBusiestReaders.clear();
		if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
			previousBusiestReaders = getBusiestReadersFromLastInterval(elapsed);

			// For status, report the busiest tag:
			if (previousBusiestReaders.empty()) {
				TraceEvent("BusiestReader", thisServerID).detail("Cost", 0.0);
			} else {
				auto busiestReader = previousBusiestReaders[0];
				for (int i = 1; i < previousBusiestReaders.size(); ++i) {
					auto const& reader = previousBusiestReaders[i];
					if (reader.rate > busiestReader.rate) {
						busiestReader = reader;
					}
				}
				TraceEvent("BusiestReader", thisServerID)
				    .detail("ThrottlingId", busiestReader.throttlingId)
				    .detail("Cost", busiestReader.rate)
				    .detail("FractionalBusyness", busiestReader.fractionalBusyness);
			}

			for (const auto& busyReader : previousBusiestReaders) {
				TraceEvent("BusyReader", thisServerID)
				    .detail("ThrottlingId", busyReader.throttlingId)
				    .detail("Cost", busyReader.rate)
				    .detail("FractionalBusyness", busyReader.fractionalBusyness);
			}
		}

		intervalCosts.clear();
		intervalTotalCost = 0;
		intervalStart = now();
	}

	std::vector<BusyThrottlingIdInfo> const& getBusiestReaders() const { return previousBusiestReaders; }
};

ThrottlingCounter::ThrottlingCounter(UID thisServerID, int maxReadersTracked, double minRateTracked)
  : impl(PImpl<ThrottlingCounterImpl>::create(thisServerID, maxReadersTracked, minRateTracked)) {}

ThrottlingCounter::~ThrottlingCounter() = default;

void ThrottlingCounter::addRequest(Optional<TagSet> const& tags,
                                   Optional<TenantGroupName> const& tenantGroup,
                                   int64_t bytes) {
	return impl->addRequest(tags, tenantGroup, bytes);
}

void ThrottlingCounter::startNewInterval() {
	return impl->startNewInterval();
}

std::vector<BusyThrottlingIdInfo> const& ThrottlingCounter::getBusiestReaders() const& {
	return impl->getBusiestReaders();
}

namespace {

bool containsTenantGroup(std::vector<BusyThrottlingIdInfo> const& busyReaders, TenantGroupNameRef tenantGroup) {
	for (auto const& reader : busyReaders) {
		if (!reader.throttlingId.isTag() && reader.throttlingId.getTenantGroup() == tenantGroup) {
			return true;
		}
	}
	return false;
}

} // namespace

TEST_CASE("/fdbserver/ThrottlingCounter/IgnoreBeyondMaxReaders") {
	state ThrottlingCounter counter(
	    UID(), /*maxReadersTracked=*/2, /*minRateTracked=*/10.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	counter.startNewInterval();
	ASSERT_EQ(counter.getBusiestReaders().size(), 0);
	{
		wait(delay(1.0));
		counter.addRequest({}, "tenantGroupA"_sr, 10 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.addRequest({}, "tenantGroupA"_sr, 10 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.addRequest({}, "tenantGroupB"_sr, 15 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.addRequest({}, "tenantGroupC"_sr, 20 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.startNewInterval();
		auto const busiestReaders = counter.getBusiestReaders();
		ASSERT_EQ(busiestReaders.size(), 2);
		ASSERT(containsTenantGroup(busiestReaders, "tenantGroupA"_sr));
		ASSERT(!containsTenantGroup(busiestReaders, "tenantGroupB"_sr));
		ASSERT(containsTenantGroup(busiestReaders, "tenantGroupC"_sr));
	}
	return Void();
}

TEST_CASE("/fdbserver/ThrottlingCounter/IgnoreBelowMinRate") {
	state ThrottlingCounter counter(
	    UID(), /*maxReadersTracked=*/2, /*minRateTracked=*/10.0 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	counter.startNewInterval();
	ASSERT_EQ(counter.getBusiestReaders().size(), 0);
	{
		wait(delay(1.0));
		counter.addRequest({}, "tenantGroupA"_sr, 5 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		counter.startNewInterval();
		auto const busiestReaders = counter.getBusiestReaders();
		ASSERT_EQ(busiestReaders.size(), 0);
	}
	return Void();
}
