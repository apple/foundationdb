/**
 * RKThroughputTracker.cpp
 */

#include "fdbserver/Knobs.h"
#include "fdbserver/IRKThroughputTracker.h"

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

std::vector<ThrottlingId> ServerThroughputTracker::getThrottlingIdsAffectingStorageServer(UID storageServerId) const {
	std::vector<ThrottlingId> result;
	auto const throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
	if (!throttlingIdToThroughputCounters.present()) {
		return {};
	} else {
		result.reserve(throttlingIdToThroughputCounters.get().size());
		for (const auto& [throttlingId, _] : throttlingIdToThroughputCounters.get()) {
			result.push_back(throttlingId);
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

// Exponentially decay throughput statistics for throttlingIds that were not reported.
// If reported throughput for a particular throttlingId is too low, stop tracking this
// throttlingId.
void ServerThroughputTracker::cleanupUnseenThrottlingIds(
    ThrottlingIdMap<ThroughputCounters>& throttlingIdToThroughputCounters,
    std::unordered_set<ThrottlingId> const& seenReadThrottlingIds,
    std::unordered_set<ThrottlingId> const& seenWriteThrottlingIds) {
	auto it = throttlingIdToThroughputCounters.begin();
	while (it != throttlingIdToThroughputCounters.end()) {
		auto& [throttlingId, throughputCounters] = *it;
		bool seen = false;
		if (seenReadThrottlingIds.count(throttlingId)) {
			seen = true;
		} else {
			throughputCounters.updateThroughput(0, OpType::READ);
		}
		if (seenWriteThrottlingIds.count(throttlingId)) {
			seen = true;
		} else {
			throughputCounters.updateThroughput(0, OpType::WRITE);
		}
		if (!seen && throughputCounters.getThroughput() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_SS_THRESHOLD) {
			it = throttlingIdToThroughputCounters.erase(it);
		} else {
			++it;
		}
	}
}

// For unseen storage servers, exponentially decay throughput statistics for every throttlingId.
// If the reported throughput for a particular throttlingId is too low, stop tracking this
// throttlingId. If no more throttlingIds are being tracked for a particular storage server,
// stop tracking this storage server altogether.
void ServerThroughputTracker::cleanupUnseenStorageServers(std::unordered_set<UID> const& seen) {
	auto it1 = throughput.begin();
	while (it1 != throughput.end()) {
		auto& [ssId, throttlingIdToThroughputCounters] = *it1;
		if (seen.count(ssId)) {
			++it1;
		} else {
			auto it2 = throttlingIdToThroughputCounters.begin();
			while (it2 != throttlingIdToThroughputCounters.end()) {
				auto& throughputCounters = it2->second;
				throughputCounters.updateThroughput(0, OpType::READ);
				throughputCounters.updateThroughput(0, OpType::WRITE);
				if (throughputCounters.getThroughput() < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FORGET_SS_THRESHOLD) {
					it2 = throttlingIdToThroughputCounters.erase(it2);
				} else {
					++it2;
				}
			}
			if (throttlingIdToThroughputCounters.empty()) {
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
		std::unordered_set<ThrottlingId> seenReadThrottlingIds, seenWriteThrottlingIds;
		auto& throttlingIdToThroughputCounters = throughput[ss.id];
		for (const auto& busyReader : ss.busiestReaders) {
			seenReadThrottlingIds.insert(busyReader.throttlingId);
			throttlingIdToThroughputCounters[busyReader.throttlingId].updateThroughput(busyReader.rate, OpType::READ);
		}
		for (const auto& busyWriter : ss.busiestWriters) {
			seenWriteThrottlingIds.insert(busyWriter.throttlingId);
			throttlingIdToThroughputCounters[busyWriter.throttlingId].updateThroughput(busyWriter.rate, OpType::WRITE);
		}

		cleanupUnseenThrottlingIds(throttlingIdToThroughputCounters, seenReadThrottlingIds, seenWriteThrottlingIds);
	}
	cleanupUnseenStorageServers(seenStorageServerIds);
}

Optional<double> ServerThroughputTracker::getThroughput(UID storageServerId, ThrottlingId const& throttlingId) const {
	auto const throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
	if (!throttlingIdToThroughputCounters.present()) {
		return {};
	}
	auto const throughputCounter = tryGet(throttlingIdToThroughputCounters.get(), throttlingId);
	if (!throughputCounter.present()) {
		return {};
	}
	return throughputCounter.get().getThroughput();
}

double ServerThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	double result{ 0.0 };
	for (auto const& [ssId, _] : throughput) {
		result += getThroughput(ssId, throttlingId).orDefault(0);
	}
	return result;
}

Optional<double> ServerThroughputTracker::getThroughput(UID storageServerId) const {
	auto throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
	if (!throttlingIdToThroughputCounters.present()) {
		return {};
	}
	double result = 0;
	for (const auto& [_, throughputCounters] : throttlingIdToThroughputCounters.get()) {
		result += throughputCounters.getThroughput();
	}
	return result;
}

void ServerThroughputTracker::removeThrottlingId(ThrottlingId const& throttlingId) {
	for (auto& [ss, throttlingIdToCounters] : throughput) {
		throttlingIdToCounters.erase(throttlingId);
	}
}

int ServerThroughputTracker::storageServersTracked() const {
	return throughput.size();
}

ClientThroughputTracker::~ClientThroughputTracker() = default;

double ClientThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	if (it == throughput.end()) {
		return 0.0;
	} else {
		return it->second.smoother.smoothRate();
	}
}

void ClientThroughputTracker::update(ThrottlingIdMap<uint64_t> const& newThroughput) {
	for (auto const& [throttlingId, newPerThrottlingIdThroughput] : newThroughput) {
		throughput[throttlingId].smoother.addDelta(newPerThrottlingIdThroughput);
	}
}

void ClientThroughputTracker::removeThrottlingId(ThrottlingId const& throttlingId) {
	throughput.erase(throttlingId);
}
