/**
 * IRKRateUpdater.h
 */

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbserver/RatekeeperLimits.h"
#include "flow/Deque.h"
#include "flow/TDMetric.actor.h"

struct RKVersionInfo {
	int64_t totalTransactions;
	int64_t batchTransactions;
	double created;

	RKVersionInfo(int64_t totalTransactions, int64_t batchTransactions, double created)
	  : totalTransactions(totalTransactions), batchTransactions(batchTransactions), created(created) {}

	RKVersionInfo() : totalTransactions(0), batchTransactions(0), created(0.0) {}
};

class IRKRateUpdater {
public:
	virtual ~IRKRateUpdater() = default;
	virtual HealthMetrics const& getHealthMetrics() const& = 0;
	virtual double getTpsLimit() const = 0;
	virtual limitReason_t getLimitReason() const = 0;
	virtual void update(class IRKMetricsTracker const&,
	                    class IRKRateServer const&,
	                    class IRKConfigurationMonitor const&,
	                    class IRKRecoveryTracker const&,
	                    Deque<double> const& actualTpsHistory,
	                    class IRKBlobMonitor&,
	                    int throttledTags) = 0;

	// In simulation, after a certain period of time has passed,
	// we expect blob worker lag to be low, and fail an assertion
	// if this is not the case.
	static bool requireSmallBlobVersionLag();
};

class RKRateUpdater : public IRKRateUpdater {
	RatekeeperLimits limits;
	HealthMetrics healthMetrics;
	std::map<Version, RKVersionInfo> version_transactions;
	double lastWarning;
	double blobWorkerTime;
	UID ratekeeperId;

	// Returns the actual rate at which transactions being released,
	// with some special handling of edge cases.
	static double getActualTps(IRKRateServer const&, IRKMetricsTracker const&);

	// Returns sum of storage bytes across all tlogs and storage servers
	// in the cluster.
	static int64_t getTotalDiskUsageBytes(IRKMetricsTracker const&);

	// If verbose tracing is enabled, randomly determine if the
	// tracing for a particular rate calculation should be verbose
	static bool shouldBeVerbose();

	void updateHealthMetricsStorageStats(IRKMetricsTracker const&);

public:
	explicit RKRateUpdater(UID ratekeeperId, RatekeeperLimits const&);

	~RKRateUpdater();

	double getTpsLimit() const override;

	limitReason_t getLimitReason() const override;

	void update(class IRKMetricsTracker const&,
	            class IRKRateServer const&,
	            class IRKConfigurationMonitor const&,
	            class IRKRecoveryTracker const&,
	            Deque<double> const& actualTpsHistory,
	            class IRKBlobMonitor&,
	            int throttledTags) override;

	HealthMetrics const& getHealthMetrics() const& override;
};
