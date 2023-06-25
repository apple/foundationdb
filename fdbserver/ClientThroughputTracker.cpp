/**
 * ClientThroughputTracker.cpp
 */

#include "fdbserver/IRKThroughputTracker.h"

ClientThroughputTracker::~ClientThroughputTracker() = default;

double ClientThroughputTracker::getThroughput(ThrottlingId const& throttlingId) const {
	auto it = throughput.find(throttlingId);
	if (it == throughput.end()) {
		return 0.0;
	} else {
		return it->second.smoother.smoothRate();
	}
}

void ClientThroughputTracker::update(ThrottlingIdMap<uint64_t>&& newThroughput) {
	for (auto const& [throttlingId, newPerThrottlingIdThroughput] : newThroughput) {
		throughput[throttlingId].smoother.addDelta(newPerThrottlingIdThroughput);
	}
}
