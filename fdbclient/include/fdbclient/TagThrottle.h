/**
 * TagThrottle.h
 */

#pragma once

#include "fmt/format.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/ThreadHelper.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Tuple.h"

typedef StringRef TransactionTagRef;
typedef Standalone<TransactionTagRef> TransactionTag;

struct ClientTrCommitCostEstimation {
	int opsCount = 0;
	uint64_t writeCosts = 0;
	std::deque<std::pair<int, uint64_t>> clearIdxCosts;
	uint32_t expensiveCostEstCount = 0;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, opsCount, writeCosts, clearIdxCosts, expensiveCostEstCount);
	}
};

namespace ThrottleApi {

class ThroughputQuotaValue {
public:
	int64_t reservedQuota{ 0 };
	int64_t totalQuota{ 0 };
	bool isValid() const;
	Tuple pack() const;
	static ThroughputQuotaValue unpack(Tuple const& val);
	bool operator==(ThroughputQuotaValue const&) const;
};

Key getTagQuotaKey(TransactionTagRef);

template <class Tr>
void setTagQuota(Reference<Tr> tr, TransactionTagRef tag, int64_t reservedQuota, int64_t totalQuota) {
	ThroughputQuotaValue tagQuotaValue;
	tagQuotaValue.reservedQuota = reservedQuota;
	tagQuotaValue.totalQuota = totalQuota;
	if (!tagQuotaValue.isValid()) {
		throw invalid_throttle_quota_value();
	}
	tr->set(getTagQuotaKey(tag), tagQuotaValue.pack().pack());
}

}; // namespace ThrottleApi

template <class Value>
using TransactionTagMap = std::unordered_map<TransactionTag, Value, std::hash<TransactionTagRef>>;

template <class Value>
using UIDTransactionTagMap = std::unordered_map<UID, TransactionTagMap<Value>>;
