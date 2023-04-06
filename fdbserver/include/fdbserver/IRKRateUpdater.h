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
	virtual HealthMetrics const& getHealthMetrics() const = 0;
	virtual double getTpsLimit() const = 0;
	virtual void update(class IRKMetricsTracker const&,
	                    class IRKRateServer const&,
	                    PromiseStream<Future<Void>> addActor,
	                    class ITagThrottler&,
	                    class IRKConfigurationMonitor const&,
	                    class IRKRecoveryTracker const&,
	                    Deque<double> const& actualTpsHistory,
	                    bool anyBlobRanges,
	                    Deque<std::pair<double, Version>> const& blobWorkerVersionHistory,
	                    double& blobWorkerTime,
	                    Int64MetricHandle& actualTpsMetric,
	                    double& unblockedAssignmentTime) = 0;
};

class RKRateUpdater : public IRKRateUpdater {
	RatekeeperLimits limits;
	HealthMetrics healthMetrics;
	std::map<Version, RKVersionInfo> version_transactions;
	double lastWarning;
	UID ratekeeperId;

public:
	explicit RKRateUpdater(UID ratekeeperId, RatekeeperLimits const&);

	~RKRateUpdater();

	double getTpsLimit() const override;

	void update(class IRKMetricsTracker const&,
	            class IRKRateServer const&,
	            PromiseStream<Future<Void>> addActor,
	            class ITagThrottler&,
	            class IRKConfigurationMonitor const&,
	            class IRKRecoveryTracker const&,
	            Deque<double> const& actualTpsHistory,
	            bool anyBlobRanges,
	            Deque<std::pair<double, Version>> const& blobWorkerVersionHistory,
	            double& blobWorkerTime,
	            Int64MetricHandle& actualTpsMetric,
	            double& unblockedAssignmentTime) override;

	HealthMetrics const& getHealthMetrics() const override;
};
