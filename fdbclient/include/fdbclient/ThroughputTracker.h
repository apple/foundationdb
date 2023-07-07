/**
 * ThroughputTracker.h
 */

#pragma once

#include "fdbclient/ThrottlingId.h"

// The ThroughputTracker class is responsible for periodically reporting each
// throttlingId's throughput to GRV proxies.
//
// TODO: Add the future responsibility of tracking average transaction cost
// for each throttlingId.
class ThroughputTracker {
	friend class ThroughputTrackerImpl;
	ThrottlingIdMap<uint64_t> throughput;

public:
	Future<Void> run(class DatabaseContext& cx);
	void addCost(ThrottlingId const&, uint64_t cost);
};
