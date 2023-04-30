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

	ACTOR static Future<Void> _start(Database cx) {
		state RKThroughputQuotaCache quotaCache(deterministicRandom()->randomUniqueID(), cx);
		state Future<Void> runFuture = quotaCache.run();
		ASSERT_EQ(quotaCache.size(), 0);
		wait(setTagQuota(cx, "testTag"_sr, testReservedQuota(), testTotalQuota()));
		while (quotaCache.size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache.getReservedQuota("testTag"_sr).get(), testReservedQuota());
		ASSERT_EQ(quotaCache.getTotalQuota("testTag"_sr).get(), testTotalQuota());
		wait(removeTagQuota(cx, "testTag"_sr));
		while (quotaCache.size() != 0) {
			wait(delay(1.0));
		}
		ASSERT(!quotaCache.getReservedQuota("testTag"_sr).present());
		ASSERT(!quotaCache.getTotalQuota("testTag"_sr).present());

		wait(setTenantGroupQuota(cx, "testTenantGroup"_sr, testReservedQuota(), testTotalQuota()));
		while (quotaCache.size() != 1) {
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache.getReservedQuota("testTenantGroup"_sr).get(), testReservedQuota());
		ASSERT_EQ(quotaCache.getTotalQuota("testTenantGroup"_sr).get(), testTotalQuota());
		wait(removeTenantGroupQuota(cx, "testTenantGroup"_sr));
		while (quotaCache.size() != 0) {
			wait(delay(1.0));
		}
		ASSERT(!quotaCache.getReservedQuota("testTenantGroup"_sr).present());
		ASSERT(!quotaCache.getTotalQuota("testTenantGroup"_sr).present());

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
