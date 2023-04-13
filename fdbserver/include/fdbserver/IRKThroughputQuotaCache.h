/**
 * IRKThrougputQuotaCache.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TagThrottle.actor.h"
#include "flow/flow.h"
#include "flow/Optional.h"

class IRKThroughputQuotaCache {
public:
	virtual ~IRKThroughputQuotaCache() = default;
	virtual Optional<int64_t> getTotalQuota(TransactionTag) const = 0;
	virtual Optional<int64_t> getReservedQuota(TransactionTag) const = 0;
	virtual Future<Void> run() = 0;
};

class RKThroughputQuotaCache : public IRKThroughputQuotaCache {
	friend class RKThroughputQuotaCacheImpl;
	TransactionTagMap<ThrottleApi::TagQuotaValue> quotas;
	UID id;
	Database db;

	void removeUnseenQuotas(std::unordered_set<TransactionTag> const& tagsWithQuota);

public:
	RKThroughputQuotaCache(UID id, Database db);
	~RKThroughputQuotaCache();
	Optional<int64_t> getTotalQuota(TransactionTag) const override;
	Optional<int64_t> getReservedQuota(TransactionTag) const override;
	Future<Void> run() override;
};

class MockRKThroughputQuotaCache : public IRKThroughputQuotaCache {
	TransactionTagMap<ThrottleApi::TagQuotaValue> quotas;

public:
	~MockRKThroughputQuotaCache();
	Optional<int64_t> getTotalQuota(TransactionTag) const override;
	Optional<int64_t> getReservedQuota(TransactionTag) const override;
	Future<Void> run() override;

	void setQuota(TransactionTag tag, int64_t totalQuota, int64_t reservedQuota);
	void removeQuota(TransactionTag tag);
};
