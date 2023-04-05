/**
 * IRKRateServer.h
 */

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKRecoveryTracker.h"
#include "fdbserver/RatekeeperInterface.h"

struct RatekeeperLimits {
	double tpsLimit;
	Int64MetricHandle tpsLimitMetric;
	Int64MetricHandle reasonMetric;

	int64_t storageTargetBytes;
	int64_t storageSpringBytes;
	int64_t logTargetBytes;
	int64_t logSpringBytes;
	double maxVersionDifference;

	int64_t durabilityLagTargetVersions;
	int64_t lastDurabilityLag;
	double durabilityLagLimit;

	double bwLagTarget;

	TransactionPriority priority;
	std::string context;

	Reference<EventCacheHolder> rkUpdateEventCacheHolder;

	RatekeeperLimits(TransactionPriority priority,
	                 std::string context,
	                 int64_t storageTargetBytes,
	                 int64_t storageSpringBytes,
	                 int64_t logTargetBytes,
	                 int64_t logSpringBytes,
	                 double maxVersionDifference,
	                 int64_t durabilityLagTargetVersions,
	                 double bwLagTarget);
};

struct RKGrvProxyInfo {
	int64_t totalTransactions{ 0 };
	int64_t batchTransactions{ 0 };
	uint64_t lastThrottledTagChangeId{ 0 };

	double lastUpdateTime{ 0.0 };
	double lastTagPushTime{ 0.0 };
	Version version{ 0 };
};

class IRKRateServer {
public:
	virtual ~IRKRateServer() = default;
	virtual double getSmoothReleasedTransactionRate() const = 0;
	virtual double getSmoothBatchReleasedTransactionRate() const = 0;
	virtual std::map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const = 0;
	virtual void cleanupExpiredGrvProxies() = 0;
	virtual Future<Void> run(HealthMetrics const&,
	                         RatekeeperLimits const& normalLimits,
	                         RatekeeperLimits const& batchLimits,
	                         class ITagThrottler&,
	                         IRKRecoveryTracker&,
	                         bool const& lastLimited) = 0;
};

class RKRateServer : public IRKRateServer {
	friend class RKRateServerImpl;
	FutureStream<GetRateInfoRequest> getRateInfo;
	Smoother smoothReleasedTransactions;
	Smoother smoothBatchReleasedTransactions;
	std::map<UID, RKGrvProxyInfo> grvProxyInfo;

public:
	explicit RKRateServer(FutureStream<GetRateInfoRequest>);
	~RKRateServer();
	double getSmoothReleasedTransactionRate() const override;
	double getSmoothBatchReleasedTransactionRate() const override;
	std::map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const override;
	void cleanupExpiredGrvProxies() override;
	Future<Void> run(HealthMetrics const&,
	                 RatekeeperLimits const& normalLimits,
	                 RatekeeperLimits const& batchLimits,
	                 class ITagThrottler&,
	                 IRKRecoveryTracker&,
	                 bool const& lastLimited) override;
};
