/**
 * RKThroughputQuotaCache.actor.cpp
 */

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "flow/actorcompiler.h" // must be last include

class RKThroughputQuotaCacheImpl {
public:
	ACTOR static Future<Void> run(RKThroughputQuotaCache* self) {
		loop {
			state Reference<ReadYourWritesTransaction> tr = self->db->createTransaction();
			loop {
				try {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr->setOption(FDBTransactionOptions::READ_LOCK_AWARE);

					state RangeReadResult tagQuotas = wait(tr->getRange(tagQuotaKeys, CLIENT_KNOBS->TOO_MANY));
					state KeyBackedRangeResult<std::pair<TenantGroupName, ThrottleApi::ThroughputQuotaValue>>
					    tenantGroupQuotas = wait(TenantMetadata::throughputQuota().getRange(
					        tr, {}, {}, CLIENT_KNOBS->MAX_TENANTS_PER_CLUSTER));
					TraceEvent("GlobalTagThrottler_ReadCurrentQuotas", self->id)
					    .detail("TagQuotasSize", tagQuotas.size())
					    .detail("TenantGroupQuotasSize", tenantGroupQuotas.results.size());
					self->quotas.clear();
					for (auto const kv : tagQuotas) {
						auto const tag = kv.key.removePrefix(tagQuotaPrefix);
						self->quotas[ThrottlingId::fromTag(tag)] =
						    ThrottleApi::ThroughputQuotaValue::unpack(Tuple::unpack(kv.value));
					}
					for (auto const& [groupName, quota] : tenantGroupQuotas.results) {
						self->quotas[ThrottlingId::fromTenantGroup(groupName)] = quota;
					}
					wait(delay(5.0));
					break;
				} catch (Error& e) {
					TraceEvent("GlobalTagThrottler_MonitoringChangesError", self->id).error(e);
					wait(tr->onError(e));
				}
			}
		}
	}
}; // class RKThroughputQuotaCacheImpl

RKThroughputQuotaCache::RKThroughputQuotaCache(UID id, Database db) : id(id), db(db) {}

RKThroughputQuotaCache::~RKThroughputQuotaCache() = default;

Optional<int64_t> RKThroughputQuotaCache::getTotalQuota(ThrottlingId const& tag) const {
	auto it = quotas.find(tag);
	if (it == quotas.end()) {
		return {};
	} else {
		return it->second.totalQuota;
	}
}

Optional<int64_t> RKThroughputQuotaCache::getReservedQuota(ThrottlingId const& tag) const {
	auto it = quotas.find(tag);
	if (it == quotas.end()) {
		return {};
	} else {
		return it->second.reservedQuota;
	}
}

int RKThroughputQuotaCache::size() const {
	return quotas.size();
}

Future<Void> RKThroughputQuotaCache::run() {
	return RKThroughputQuotaCacheImpl::run(this);
}

MockRKThroughputQuotaCache::~MockRKThroughputQuotaCache() = default;

Optional<int64_t> MockRKThroughputQuotaCache::getTotalQuota(ThrottlingId const& tag) const {
	auto it = quotas.find(tag);
	if (it == quotas.end()) {
		return {};
	} else {
		return it->second.totalQuota;
	}
}

Optional<int64_t> MockRKThroughputQuotaCache::getReservedQuota(ThrottlingId const& tag) const {
	auto it = quotas.find(tag);
	if (it == quotas.end()) {
		return {};
	} else {
		return it->second.reservedQuota;
	}
}

int MockRKThroughputQuotaCache::size() const {
	return quotas.size();
}

void MockRKThroughputQuotaCache::setQuota(ThrottlingId const& tag, int64_t totalQuota, int64_t reservedQuota) {
	quotas[tag].totalQuota = totalQuota;
	quotas[tag].reservedQuota = reservedQuota;
}

void MockRKThroughputQuotaCache::removeQuota(ThrottlingId const& tag) {
	quotas.erase(tag);
}

Future<Void> MockRKThroughputQuotaCache::run() {
	return Never();
}
