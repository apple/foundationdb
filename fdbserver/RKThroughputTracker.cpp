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

void ServerThroughputTracker::update(Map<UID, StorageQueueInfo> const& sqInfos) {
	std::unordered_set<UID> seenStorageServerIds;
	for (auto it = sqInfos.begin(); it != sqInfos.end(); ++it) {
		auto const& ss = it->value;
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

		for (auto& [throttlingId, throughputCounters] : throttlingIdToThroughputCounters) {
			if (!seenReadThrottlingIds.count(throttlingId)) {
				throughputCounters.updateThroughput(0, OpType::READ);
			}
			if (!seenWriteThrottlingIds.count(throttlingId)) {
				throughputCounters.updateThroughput(0, OpType::WRITE);
			}
		}
	}
	for (auto& [ssId, throttlingIdToThroughputCounters] : throughput) {
		if (!seenStorageServerIds.count(ssId)) {
			for (auto& [_, throughputCounters] : throttlingIdToThroughputCounters) {
				throughputCounters.updateThroughput(0, OpType::READ);
				throughputCounters.updateThroughput(0, OpType::WRITE);
			}
		}
	}
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
