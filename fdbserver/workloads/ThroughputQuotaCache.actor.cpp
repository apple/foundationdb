/**
 * ThroughputQuotaCache.actor.cpp
 */

#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // must be the last include

// Tests the functionality of the RKThroughputQuotaCache class
class ThroughputQuotaCacheWorkload : public TestWorkload {
	ACTOR static Future<Void> setQuota(Database cx, TransactionTag tag, int64_t reservedQuota, int64_t totalQuota) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
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

	ACTOR static Future<Void> removeQuota(Database cx, TransactionTag tag) {
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

	ACTOR static Future<Void> _start(Database cx) {
		state RKThroughputQuotaCache quotaCache(deterministicRandom()->randomUniqueID(), cx);
		state Future<Void> runFuture = quotaCache.run();
		ASSERT_EQ(quotaCache.size(), 0);
		wait(setQuota(cx,
		              "testTag"_sr,
		              100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE,
		              1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE));
		while (quotaCache.size() != 1) { 
			wait(delay(1.0));
		}
		ASSERT_EQ(quotaCache.getReservedQuota("testTag"_sr).get(), 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		ASSERT_EQ(quotaCache.getTotalQuota("testTag"_sr).get(), 1000 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
		wait(removeQuota(cx, "testTag"_sr));
		loop {
			if (quotaCache.size() == 0) {
				break;
			}
			wait(delay(1.0));
		}
		ASSERT(!quotaCache.getReservedQuota("testTag"_sr).present());
		ASSERT(!quotaCache.getTotalQuota("testTag"_sr).present());
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
