/*
 * ServerThroughputTracker.cpp
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

#include "fdbserver/ServerThroughputTracker.h"

namespace {

template <class K, class V, class H>
static Optional<V> tryGet(std::unordered_map<K, V, H> const& m, K const& k) {
	auto it = m.find(k);
	if (it == m.end()) {
		return {};
	} else {
		return it->second;
	}
}

} // namespace

ServerThroughputTracker::ThroughputCounters::ThroughputCounters()
  : readThroughput(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME),
    writeThroughput(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}

std::vector<TransactionTag> ServerThroughputTracker::getTagsAffectingStorageServer(UID storageServerId) const {
	std::vector<TransactionTag> result;
	auto const tagToThroughputCounters = tryGet(throughput, storageServerId);
	if (!tagToThroughputCounters.present()) {
		return {};
	} else {
		result.reserve(tagToThroughputCounters.get().size());
		for (const auto& [tag, _] : tagToThroughputCounters.get()) {
			result.push_back(tag);
		}
	}
	return result;
}

void ServerThroughputTracker::ThroughputCounters::updateThroughput(double newThroughput, OpType opType) {
	if (opType == OpType::READ) {
		readThroughput.setTotal(newThroughput);
	} else {
		writeThroughput.setTotal(newThroughput);
	}
}

double ServerThroughputTracker::ThroughputCounters::getThroughput() const {
	return readThroughput.smoothTotal() + writeThroughput.smoothTotal();
}

ServerThroughputTracker::~ServerThroughputTracker() = default;

// Exponentially decay throughput statistics for tags that were not reported.
// If reported throughput for a particular tag is too low, stop tracking this
// tag.
void ServerThroughputTracker::cleanupUnseenTags(TransactionTagMap<ThroughputCounters>& tagToThroughputCounters,
                                                std::unordered_set<TransactionTag> const& seenReadTags,
                                                std::unordered_set<TransactionTag> const& seenWriteTags) {
	auto it = tagToThroughputCounters.begin();
	while (it != tagToThroughputCounters.end()) {
		auto& [tag, throughputCounters] = *it;
		bool seen = false;
		if (seenReadTags.count(tag)) {
			seen = true;
		} else {
			throughputCounters.updateThroughput(0, OpType::READ);
		}
		if (seenWriteTags.count(tag)) {
			seen = true;
		} else {
			throughputCounters.updateThroughput(0, OpType::WRITE);
		}
		if (!seen && throughputCounters.getThroughput() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_SS_THRESHOLD) {
			it = tagToThroughputCounters.erase(it);
		} else {
			++it;
		}
	}
}

// For unseen storage servers, exponentially decay throughput statistics for every tag.
// If the reported throughput for a particular tag is too low, stop tracking this
// tag. If no more tags are being tracked for a particular storage server,
// stop tracking this storage server altogether.
void ServerThroughputTracker::cleanupUnseenStorageServers(std::unordered_set<UID> const& seen) {
	auto it1 = throughput.begin();
	while (it1 != throughput.end()) {
		auto& [ssId, tagToThroughputCounters] = *it1;
		if (seen.count(ssId)) {
			++it1;
		} else {
			auto it2 = tagToThroughputCounters.begin();
			while (it2 != tagToThroughputCounters.end()) {
				auto& throughputCounters = it2->second;
				throughputCounters.updateThroughput(0, OpType::READ);
				throughputCounters.updateThroughput(0, OpType::WRITE);
				if (throughputCounters.getThroughput() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_SS_THRESHOLD) {
					it2 = tagToThroughputCounters.erase(it2);
				} else {
					++it2;
				}
			}
			if (tagToThroughputCounters.empty()) {
				it1 = throughput.erase(it1);
			} else {
				++it1;
			}
		}
	}
}

void ServerThroughputTracker::update(Map<UID, StorageQueueInfo> const& sqInfos) {
	std::unordered_set<UID> seenStorageServerIds;
	for (auto it = sqInfos.begin(); it != sqInfos.end(); ++it) {
		auto const& ss = it->value;
		if (!ss.valid) {
			continue;
		}
		seenStorageServerIds.insert(ss.id);
		std::unordered_set<TransactionTag> seenReadTags, seenWriteTags;
		auto& tagToThroughputCounters = throughput[ss.id];
		for (const auto& busyReader : ss.busiestReadTags) {
			seenReadTags.insert(busyReader.tag);
			tagToThroughputCounters[busyReader.tag].updateThroughput(busyReader.rate, OpType::READ);
		}
		for (const auto& busyWriter : ss.busiestWriteTags) {
			seenWriteTags.insert(busyWriter.tag);
			tagToThroughputCounters[busyWriter.tag].updateThroughput(busyWriter.rate, OpType::WRITE);
		}

		cleanupUnseenTags(tagToThroughputCounters, seenReadTags, seenWriteTags);
	}
	cleanupUnseenStorageServers(seenStorageServerIds);
}

Optional<double> ServerThroughputTracker::getThroughput(UID storageServerId, TransactionTag const& tag) const {
	auto const tagToThroughputCounters = tryGet(throughput, storageServerId);
	if (!tagToThroughputCounters.present()) {
		return {};
	}
	auto const throughputCounter = tryGet(tagToThroughputCounters.get(), tag);
	if (!throughputCounter.present()) {
		return {};
	}
	return throughputCounter.get().getThroughput();
}

double ServerThroughputTracker::getThroughput(TransactionTag const& tag) const {
	double result{ 0.0 };
	for (auto const& [ssId, _] : throughput) {
		result += getThroughput(ssId, tag).orDefault(0);
	}
	return result;
}

Optional<double> ServerThroughputTracker::getThroughput(UID storageServerId) const {
	auto tagToThroughputCounters = tryGet(throughput, storageServerId);
	if (!tagToThroughputCounters.present()) {
		return {};
	}
	double result = 0;
	for (const auto& [_, throughputCounters] : tagToThroughputCounters.get()) {
		result += throughputCounters.getThroughput();
	}
	return result;
}

void ServerThroughputTracker::removeTag(TransactionTag const& tag) {
	for (auto& [ss, tagToCounters] : throughput) {
		tagToCounters.erase(tag);
	}
}

int ServerThroughputTracker::storageServersTracked() const {
	return throughput.size();
}
