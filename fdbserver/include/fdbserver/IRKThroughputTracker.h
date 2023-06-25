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
	struct ThroughputSmoother {
		HoltLinearSmoother smoother;
		ThroughputSmoother() : smoother(1.0, 1.0) {}
	};

	ThrottlingIdMap<ThroughputSmoother> throughput;

public:
	~ClientThroughputTracker();
	double getThroughput(ThrottlingId const&) const override;
	void update(ThrottlingIdMap<uint64_t>&&);
};
