/**
 * IRKThroughputTracker.h
 */

#pragma once

#include "fdbclient/ThrottlingId.h"
#include "fdbserver/IRKMetricsTracker.h"

// Tracks the cluster-wide throughput of each throttlingId
class IRKThroughputTracker {
public:
	virtual ~IRKThroughputTracker() = default;

	virtual double getThroughput(ThrottlingId const&) const = 0;

	virtual void update(StorageQueueInfo const& ss) = 0;

	virtual void update(ThrottlingIdMap<int64_t>&&) = 0;
};

class ClientThroughputTracker : public IRKThroughputTracker {
public:
	~ClientThroughputTracker();

	// TODO: Implement
	double getThroughput(ThrottlingId const&) const override { throw not_implemented(); }

	// This is a noop
	void update(StorageQueueInfo const&) override {}

	// TODO: Implement
	void update(ThrottlingIdMap<int64_t>&&) override { throw not_implemented(); }
};
