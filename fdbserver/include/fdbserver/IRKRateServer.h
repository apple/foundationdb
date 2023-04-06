/**
 * IRKRateServer.h
 */

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Smoother.h"
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

	// Returns smooth rate at which transactions are being released.
	virtual double getSmoothReleasedTransactionRate() const = 0;

	// Returns smooth rate at which batch-priority transactions
	// are being released.
	virtual double getSmoothBatchReleasedTransactionRate() const = 0;

	// Returns a map of GRV proxy IDs to throttling-relevant GRV proxy
	// statistics.
	virtual std::map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const = 0;

	// Remove GRV proxies that have not sent messages recently from
	// internal state
	virtual void cleanupExpiredGrvProxies() = 0;

	// Based on the provided batch TPS limit and the current rate at which
	// transactions are being released, update the lastLimited field to be
	// included in health metrics
	virtual void updateLastLimited(double batchTpsLimit) = 0;

	// Serve the getRateInfo interface for GRV proxies. Respond by providing
	// TPS limits per-tag and per-priority, as well as health metrics.
	//
	// Also updates the tag throttler with statistics about the rate of incoming
	// requests per tag.
	//
	// Also updates the recovery tracker by reporting the versions received
	// from GRV proxies.
	virtual Future<Void> run(HealthMetrics const&,
	                         RatekeeperLimits const& normalLimits,
	                         RatekeeperLimits const& batchLimits,
	                         class ITagThrottler&,
	                         class IRKRecoveryTracker&) = 0;
};

class RKRateServer : public IRKRateServer {
	friend class RKRateServerImpl;
	FutureStream<GetRateInfoRequest> getRateInfo;
	Smoother smoothReleasedTransactions;
	Smoother smoothBatchReleasedTransactions;
	std::map<UID, RKGrvProxyInfo> grvProxyInfo;
	bool lastLimited;

public:
	explicit RKRateServer(FutureStream<GetRateInfoRequest>);
	~RKRateServer();
	double getSmoothReleasedTransactionRate() const override;
	double getSmoothBatchReleasedTransactionRate() const override;
	std::map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const override;
	void cleanupExpiredGrvProxies() override;
	void updateLastLimited(double batchTpsLimit) override;
	Future<Void> run(HealthMetrics const&,
	                 RatekeeperLimits const& normalLimits,
	                 RatekeeperLimits const& batchLimits,
	                 class ITagThrottler&,
	                 class IRKRecoveryTracker&) override;
};
