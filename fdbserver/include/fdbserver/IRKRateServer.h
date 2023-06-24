/**
 * IRKRateServer.h
 */

#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/RatekeeperLimits.h"
#include "fdbserver/RatekeeperInterface.h"

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
	virtual std::unordered_map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const& = 0;

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
	virtual Future<Void> run(class IRKRateUpdater const& normalRateUpdater,
	                         class IRKRateUpdater const& batchRateUpdater,
	                         class GlobalTagThrottler&,
	                         class IRKRecoveryTracker&) = 0;
};

class RKRateServer : public IRKRateServer {
	friend class RKRateServerImpl;
	FutureStream<GetRateInfoRequest> getRateInfo;
	Smoother smoothReleasedTransactions;
	Smoother smoothBatchReleasedTransactions;
	std::unordered_map<UID, RKGrvProxyInfo> grvProxyInfo;
	bool lastLimited{ false };

public:
	explicit RKRateServer(FutureStream<GetRateInfoRequest>);
	~RKRateServer();
	double getSmoothReleasedTransactionRate() const override;
	double getSmoothBatchReleasedTransactionRate() const override;
	std::unordered_map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const& override;
	void cleanupExpiredGrvProxies() override;
	void updateLastLimited(double batchTpsLimit) override;
	Future<Void> run(class IRKRateUpdater const& normalRateUpdater,
	                 class IRKRateUpdater const& batchRateUpdater,
	                 class GlobalTagThrottler&,
	                 class IRKRecoveryTracker&) override;
};

class MockRKRateServer : public IRKRateServer {
	std::unordered_map<UID, RKGrvProxyInfo> grvProxyInfo;
	double rate;

public:
	explicit MockRKRateServer(double rate) : rate(rate) {}
	double getSmoothReleasedTransactionRate() const override { return rate; }
	double getSmoothBatchReleasedTransactionRate() const override { return 0; }
	std::unordered_map<UID, RKGrvProxyInfo> const& getGrvProxyInfo() const& override { return grvProxyInfo; }
	void cleanupExpiredGrvProxies() override {}
	void updateLastLimited(double batchTpsLimit) override {}
	Future<Void> run(class IRKRateUpdater const& normalRateUpdater,
	                 class IRKRateUpdater const& batchRateUpdater,
	                 class GlobalTagThrottler&,
	                 class IRKRecoveryTracker&) override {
		return Never();
	}

	void updateProxy(UID proxyId, Version v, int newTotalTransactions);
};
