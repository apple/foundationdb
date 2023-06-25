/**
 * ThroughputTrackers.h
 */

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ThrottlingId.h"

// The ThroughputTracker class is responsible for periodically reporting each
// throttlingId's throughput to GRV proxies.
//
// TODO: Add the future responsibility of tracking average transaction cost
// for each throttlingId.
class ThroughputTracker {
	friend class ThroughputTrackerImpl;

	Future<Void> reporter;
	ThrottlingIdMap<uint64_t> throughput;

public:
	ThroughputTracker(Database cx);
	void addCost(ThrottlingId const&, uint64_t cost);
};
