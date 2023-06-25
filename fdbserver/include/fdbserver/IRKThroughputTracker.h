/**
 * IRKThroughputTracker.h
 */

#pragma once

#include "fdbclient/ThrottlingId.h"
#include "fdbserver/IRKMetricsTracker.h"

// Tracks the cluster-wide throughput (in bytes/second) of each throttlingId
class IRKThroughputTracker {
public:
	virtual ~IRKThroughputTracker() = default;

	virtual double getThroughput(ThrottlingId const&) const = 0;
};

class ClientThroughputTracker : public IRKThroughputTracker {
public:
	~ClientThroughputTracker();

	// TODO: Implement
	double getThroughput(ThrottlingId const&) const override { throw not_implemented(); }

	// TODO: Implement
	void update(ThrottlingIdMap<int64_t>&&) { throw not_implemented(); }
};
