/**
 * ThroughputQuotaCache.actor.cpp
 */

#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // must be the last include

// Tests the functionality of the RKThroughputQuotaCache class
class ThroughputQuotaCacheWorkload : public TestWorkload {
	static int64_t testReservedQuota() { return 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	static int64_t testTotalQuota() { return 1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE; }

	ACTOR static Future<Void> setTagQuota(Database cx, TransactionTag tag, int64_t reservedQuota, int64_t totalQuota) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ThrottleApi::setTagQuota(tr, tag, reservedQuota, totalQuota);
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> removeTagQuota(Database cx, TransactionTag tag) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->clear(ThrottleApi::getTagQuotaKey(tag));
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> setTenantGroupQuota(Database cx,
	                                              TenantGroupName tenantGroup,
	                                              int64_t reservedQuota,
	                                              int64_t totalQuota) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				ThrottleApi::TagQuotaValue tagQuotaValue;
				tagQuotaValue.reservedQuota = reservedQuota;
				tagQuotaValue.totalQuota = totalQuota;
				if (!tagQuotaValue.isValid()) {
					throw invalid_throttle_quota_value();
				}
				TenantMetadata::throughputQuota().set(tr, tenantGroup, tagQuotaValue);
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> removeTenantGroupQuota(Database cx, TenantGroupName tenantGroup) {
		state Reference<ReadYourWritesTransaction> tr = cx->createTransaction();
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				TenantMetadata::throughputQuota().erase(tr, tenantGroup);
				wait(tr->commit());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> testTagQuota(Database cx, TransactionTag tag, RKThroughputQuotaCache const* quotaCache) {
		ASSERT_EQ(quotaCache->size(), 0);
		wait(setTagQuota(cx, tag, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(tag).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(tag).get(), testTotalQuota());
		wait(removeTagQuota(cx, tag));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		ASSERT(!quotaCache->getReservedQuota(tag).present());
		ASSERT(!quotaCache->getTotalQuota(tag).present());
		return Void();
	}

	ACTOR static Future<Void> testTenantGroupQuota(Database cx,
	                                               TenantGroupName tenantGroup,
	                                               RKThroughputQuotaCache const* quotaCache) {
		ASSERT_EQ(quotaCache->size(), 0);
		wait(setTenantGroupQuota(cx, tenantGroup, testReservedQuota(), testTotalQuota()));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(tenantGroup).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(tenantGroup).get(), testTotalQuota());
		wait(removeTenantGroupQuota(cx, tenantGroup));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		ASSERT(!quotaCache->getReservedQuota(tenantGroup).present());
		ASSERT(!quotaCache->getTotalQuota(tenantGroup).present());
		return Void();
	}

	// When tenant groups and tags have conflicting names, tenant groups take priority
	// TODO: Change this behaviour in the future, to allow for complete separation of
	// tenant group quotas and tag quotas.
	ACTOR static Future<Void> testConflictingNames(Database cx,
	                                               Standalone<StringRef> sharedName,
	                                               RKThroughputQuotaCache const* quotaCache) {
		ASSERT_EQ(quotaCache->size(), 0);
		wait(setTenantGroupQuota(cx, sharedName, testReservedQuota(), testTotalQuota()));
		wait(setTagQuota(cx, sharedName, testReservedQuota() * 2, testTotalQuota() * 2));
		while (quotaCache->size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->getReservedQuota(sharedName).get(), testReservedQuota());
		ASSERT_EQ(quotaCache->getTotalQuota(sharedName).get(), testTotalQuota());
		wait(removeTenantGroupQuota(cx, sharedName));
		while (quotaCache->getReservedQuota(sharedName).get() != testReservedQuota() * 2) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache->size(), 1);
		ASSERT_EQ(quotaCache->getTotalQuota(sharedName).get(), testTotalQuota() * 2);
		wait(removeTagQuota(cx, sharedName));
		while (quotaCache->size() != 0) {
			wait(delay(1.0));
		}
		return Void();
	}

	ACTOR static Future<Void> _start(Database cx) {
		state RKThroughputQuotaCache quotaCache(deterministicRandom()->randomUniqueID(), cx);
		state Future<Void> runFuture = quotaCache.run();
		wait(testTagQuota(cx, "testTag"_sr, &quotaCache));
		wait(testTenantGroupQuota(cx, "testTenantGroup"_sr, &quotaCache));
		wait(testConflictingNames(cx, "testConflictingName"_sr, &quotaCache));
		return Void();
	}

public:
	static constexpr auto NAME = "ThroughputQuotaCache";
	explicit ThroughputQuotaCacheWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ThroughputQuotaCacheWorkload> ThroughputQuotaCacheWorkloadFactory;
