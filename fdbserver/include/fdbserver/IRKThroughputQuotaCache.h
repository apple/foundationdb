/**
 * IRKThrougputQuotaCache.h
 */

#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TagThrottle.actor.h"
#include "flow/flow.h"
#include "flow/Optional.h"

// Responsible for maintaining a cache of per-tag throughput quotas
class IRKThroughputQuotaCache {
public:
	virtual ~IRKThroughputQuotaCache() = default;

	// Returns the cached value for the total throughput quota for
	// the provided tag (in bytes/second)
	virtual Optional<int64_t> getTotalQuota(TransactionTag) const = 0;

	// Returns the cached value for the reserved throughput quota
	// for the provided tag (in bytes/second)
	virtual Optional<int64_t> getReservedQuota(TransactionTag) const = 0;

	// Responsible for updating the quota cache. The returned future
	// should never be ready, but can be used for propagating errors.
	virtual Future<Void> run() = 0;
};

// Uses the system keyspace to populate a cache of per-tenant throughput quotas
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

// Cache is updated by a test client that manually sets and removes quotas
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
