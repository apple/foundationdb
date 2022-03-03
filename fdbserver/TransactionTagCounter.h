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

struct TransactionTagCounter {
	struct TagInfo {
		TransactionTag tag;
		double rate;
		double fractionalBusyness;

		TagInfo(TransactionTag const& tag, double rate, double fractionalBusyness)
		  : tag(tag), rate(rate), fractionalBusyness(fractionalBusyness) {}
	};

	TransactionTagMap<int64_t> intervalCounts;
	int64_t intervalTotalSampledCount = 0;
	TransactionTag busiestTag;
	int64_t busiestTagCount = 0;
	double intervalStart = 0;

	Optional<TagInfo> previousBusiestTag;

	UID thisServerID;

	Reference<EventCacheHolder> busiestReadTagEventHolder;

	TransactionTagCounter(UID thisServerID)
	  : thisServerID(thisServerID),
	    busiestReadTagEventHolder(makeReference<EventCacheHolder>(thisServerID.toString() + "/BusiestReadTag")) {}

	int64_t costFunction(int64_t bytes) const { return bytes / SERVER_KNOBS->READ_COST_BYTE_FACTOR + 1; }

	void addRequest(Optional<TagSet> const& tags, int64_t bytes) {
		if (tags.present()) {
			TEST(true); // Tracking tag on storage server
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

	void startNewInterval() {
		double elapsed = now() - intervalStart;
		previousBusiestTag.reset();
		if (intervalStart > 0 && CLIENT_KNOBS->READ_TAG_SAMPLE_RATE > 0 && elapsed > 0) {
			double rate = busiestTagCount / CLIENT_KNOBS->READ_TAG_SAMPLE_RATE / elapsed;
			if (rate > SERVER_KNOBS->MIN_TAG_READ_PAGES_RATE) {
				previousBusiestTag = TagInfo(busiestTag, rate, (double)busiestTagCount / intervalTotalSampledCount);
			}

			TraceEvent("BusiestReadTag", thisServerID)
			    .detail("Elapsed", elapsed)
			    .detail("Tag", printable(busiestTag))
			    .detail("TagCost", busiestTagCount)
			    .detail("TotalSampledCost", intervalTotalSampledCount)
			    .detail("Reported", previousBusiestTag.present())
			    .trackLatest(busiestReadTagEventHolder->trackingKey);
		}

		intervalCounts.clear();
		intervalTotalSampledCount = 0;
		busiestTagCount = 0;
		intervalStart = now();
	}

	Optional<TagInfo> getBusiestTag() const { return previousBusiestTag; }
};
