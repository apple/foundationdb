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

		for (auto& [tag, throughputCounters] : tagToThroughputCounters) {
			if (!seenReadTags.count(tag)) {
				throughputCounters.updateThroughput(0, OpType::READ);
			}
			if (!seenWriteTags.count(tag)) {
				throughputCounters.updateThroughput(0, OpType::WRITE);
			}
		}
	}
	for (auto& [ssId, tagToThroughputCounters] : throughput) {
		if (!seenStorageServerIds.count(ssId)) {
			for (auto& [_, throughputCounters] : tagToThroughputCounters) {
				throughputCounters.updateThroughput(0, OpType::READ);
				throughputCounters.updateThroughput(0, OpType::WRITE);
			}
		}
	}
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
