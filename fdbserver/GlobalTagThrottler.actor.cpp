/*
 * GlobalTagThrottler.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbclient/FDBTypes.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/TagThrottler.h"

#include <limits>

#include "flow/actorcompiler.h" // must be last include

// In the function names below, several terms are used repeatedly. The context-specific are defined here:
//
// Cost: Every read or write operation has an associated cost, determined by the number of bytes accessed.
//       Global tag throttling quotas are specified in terms of the amount of this cost that can be consumed
//       per second. In the global tag throttler, cost refers to the per second rate of cost consumption.
//
// TPS: Transactions per second. Quotas are not specified in terms of TPS, but the limits given to clients must
//      be specified in terms of TPS because throttling is performed at the front end of transactions (before costs are
//      known).
//
// Total: Refers to the total quota specified by clients through the global tag throttling API. The sum of the
//        costs of all operations (cluster-wide) with a particular tag cannot exceed the tag's specified total quota,
//        even if the cluster has no saturated processes.
//
// Desired TPS: Assuming that a tag is able to achieve its total quota, this is the TPS it would be able to perform.
//
// Reserved: Refers to the reserved quota specified by clients through the global tag throttling API. As long as the
//           sum of the costs of all operations (cluster-wide) with a particular tag are not above the tag's
//           specified reserved quota, the tag should not experience any throttling from the global tag throttler.
//
// Current [Cost|TPS]: Measuring the current throughput on the cluster, independent of any specified quotas.
//
// ThrottlingRatio: Based on the health of each storage server, a throttling ratio is provided,
//                  informing the global tag throttler what ratio of the current throughput can be maintained.
//
// Limiting [Cost|TPS]: Based on the health of storage servers, a limiting throughput may be enforced.
//
// Target [Cost|TPS]: Based on reserved, limiting, and desired throughputs, this is the target throughput
//                    that the global tag throttler aims to achieve (across all clients).
//
// PerClient TPS: Because the target throughput must be shared across multiple clients, and all clients must
//           be given the same limits, a per-client limit is calculated based on the current and target throughputs.

class GlobalTagThrottlerImpl {
	template <class K, class V>
	static Optional<V> tryGet(std::unordered_map<K, V> const& m, K const& k) {
		auto it = m.find(k);
		if (it == m.end()) {
			return {};
		} else {
			return it->second;
		}
	}

	static Optional<double> getMin(Optional<double> a, Optional<double> b) {
		if (a.present() && b.present()) {
			return std::min(a.get(), b.get());
		} else if (a.present()) {
			return a;
		} else {
			return b;
		}
	}

	static Optional<double> getMax(Optional<double> a, Optional<double> b) {
		if (a.present() && b.present()) {
			return std::max(a.get(), b.get());
		} else if (a.present()) {
			return a;
		} else {
			return b;
		}
	}

	enum class LimitType { RESERVED, TOTAL };
	enum class OpType { READ, WRITE };

	class ThroughputCounters {
		Smoother readCost;
		Smoother writeCost;

	public:
		ThroughputCounters()
		  : readCost(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    writeCost(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME) {}

		void updateCost(double newCost, OpType opType) {
			if (opType == OpType::READ) {
				readCost.setTotal(newCost);
			} else {
				writeCost.setTotal(CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * newCost);
			}
		}

		double getCost() const { return readCost.smoothTotal() + writeCost.smoothTotal(); }
	};

	// Track various statistics per tag, aggregated across all storage servers
	class PerTagStatistics {
		Optional<ThrottleApi::TagQuotaValue> quota;
		Smoother transactionCounter;
		Smoother perClientRate;
		Smoother targetRate;
		double transactionsLastAdded;

	public:
		explicit PerTagStatistics()
		  : transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    perClientRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    targetRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME), transactionsLastAdded(now()) {}

		Optional<ThrottleApi::TagQuotaValue> getQuota() const { return quota; }

		void setQuota(ThrottleApi::TagQuotaValue quota) { this->quota = quota; }

		void clearQuota() { quota = {}; }

		void addTransactions(int count) {
			transactionsLastAdded = now();
			transactionCounter.addDelta(count);
		}

		double getTransactionRate() const { return transactionCounter.smoothRate(); }

		ClientTagThrottleLimits updateAndGetPerClientLimit(double targetTps) {
			auto newPerClientRate = std::max(
			    SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE,
			    std::min(targetTps, (targetTps / transactionCounter.smoothRate()) * perClientRate.smoothTotal()));
			perClientRate.setTotal(newPerClientRate);
			return ClientTagThrottleLimits(
			    std::max(perClientRate.smoothTotal(), SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE),
			    ClientTagThrottleLimits::NO_EXPIRATION);
		}

		double updateAndGetTargetLimit(double targetTps) {
			targetRate.setTotal(targetTps);
			return targetRate.smoothTotal();
		}

		bool recentTransactionsAdded() const {
			return now() - transactionsLastAdded < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER;
		}
	};

	struct StorageServerInfo {
		Optional<Standalone<StringRef>> zoneId;
		Optional<double> throttlingRatio;
	};

	Database db;
	UID id;
	int maxFallingBehind{ 0 };
	uint64_t throttledTagChangeId{ 0 };
	uint32_t lastBusyTagCount{ 0 };

	std::unordered_map<UID, StorageServerInfo> ssInfos;
	std::unordered_map<TransactionTag, PerTagStatistics> tagStatistics;
	std::unordered_map<UID, std::unordered_map<TransactionTag, ThroughputCounters>> throughput;

	// Returns the cost rate for the given tag on the given storage server
	Optional<double> getCurrentCost(UID storageServerId, TransactionTag tag) const {
		auto const tagToThroughputCounters = tryGet(throughput, storageServerId);
		if (!tagToThroughputCounters.present()) {
			return {};
		}
		auto const throughputCounter = tryGet(tagToThroughputCounters.get(), tag);
		if (!throughputCounter.present()) {
			return {};
		}
		return throughputCounter.get().getCost();
	}

	// Return the cost rate on the given storage server, summed across all tags
	Optional<double> getCurrentCost(UID storageServerId) const {
		auto tagToPerTagThroughput = tryGet(throughput, storageServerId);
		if (!tagToPerTagThroughput.present()) {
			return {};
		}
		double result = 0;
		for (const auto& [tag, perTagThroughput] : tagToPerTagThroughput.get()) {
			result += perTagThroughput.getCost();
		}
		return result;
	}

	// Return the cost rate for the given tag, summed across all storage servers
	double getCurrentCost(TransactionTag tag) const {
		double result{ 0.0 };
		for (const auto& [id, _] : throughput) {
			result += getCurrentCost(id, tag).orDefault(0);
		}
		// FIXME: Disabled due to noisy trace events. Fix the noise and reenabled
		//TraceEvent("GlobalTagThrottler_GetCurrentCost").detail("Tag", printable(tag)).detail("Cost", result);

		return result;
	}

	// For transactions with the provided tag, returns the average cost that gets associated with the provided storage
	// server
	Optional<double> getAverageTransactionCost(TransactionTag tag, UID storageServerId) const {
		auto const cost = getCurrentCost(storageServerId, tag);
		if (!cost.present()) {
			return {};
		}
		auto const stats = tryGet(tagStatistics, tag);
		if (!stats.present()) {
			return {};
		}
		auto const transactionRate = stats.get().getTransactionRate();
		if (transactionRate == 0.0) {
			return {};
		} else {
			return std::max(1.0, cost.get() / transactionRate);
		}
	}

	// For transactions with the provided tag, returns the average cost of all transactions
	// accross the cluster. The minimum cost is 1.
	double getAverageTransactionCost(TransactionTag tag) const {
		auto const cost = getCurrentCost(tag);
		auto const stats = tryGet(tagStatistics, tag);
		if (!stats.present()) {
			return 1.0;
		}
		auto const transactionRate = stats.get().getTransactionRate();
		// FIXME: Disabled due to noisy trace events. Fix the noise and reenabled
		/*
		TraceEvent("GlobalTagThrottler_GetAverageTransactionCost")
		    .detail("Tag", tag)
		    .detail("TransactionRate", transactionRate)
		    .detail("Cost", cost);
		*/
		if (transactionRate == 0.0) {
			return 1.0;
		} else {
			return std::max(1.0, cost / transactionRate);
		}
	}

	// Returns the list of all tags performing meaningful work on the given storage server
	std::vector<TransactionTag> getTagsAffectingStorageServer(UID storageServerId) const {
		std::vector<TransactionTag> result;
		auto const tagToThroughputCounters = tryGet(throughput, storageServerId);
		if (!tagToThroughputCounters.present()) {
			return {};
		} else {
			result.reserve(tagToThroughputCounters.get().size());
			for (const auto& [t, _] : tagToThroughputCounters.get()) {
				result.push_back(t);
			}
		}
		return result;
	}

	Optional<double> getQuota(TransactionTag tag, LimitType limitType) const {
		auto const stats = tryGet(tagStatistics, tag);
		if (!stats.present()) {
			return {};
		}
		auto const quota = stats.get().getQuota();
		if (!quota.present()) {
			return {};
		}
		if (limitType == LimitType::TOTAL) {
			return quota.get().totalQuota;
		} else {
			return quota.get().reservedQuota;
		}
	}

	// Of all tags meaningfully performing workload on the given storage server,
	// returns the ratio of total quota allocated to the specified tag
	double getQuotaRatio(TransactionTagRef tag, UID storageServerId) const {
		double sumQuota{ 0.0 };
		double tagQuota{ 0.0 };
		auto const tagsAffectingStorageServer = getTagsAffectingStorageServer(storageServerId);
		for (const auto& t : tagsAffectingStorageServer) {
			auto const tQuota = getQuota(t, LimitType::TOTAL);
			sumQuota += tQuota.orDefault(0);
			if (t.compare(tag) == 0) {
				tagQuota = tQuota.orDefault(0);
			}
		}
		if (tagQuota == 0.0) {
			return 0;
		}
		ASSERT_GT(sumQuota, 0.0);
		return tagQuota / sumQuota;
	}

	// Returns the desired cost for a storage server, based on its current
	// cost and throttling ratio
	Optional<double> getLimitingCost(UID storageServerId) const {
		auto const ssInfo = tryGet(ssInfos, storageServerId);
		Optional<double> const throttlingRatio = ssInfo.present() ? ssInfo.get().throttlingRatio : Optional<double>{};
		Optional<double> const currentCost = getCurrentCost(storageServerId);
		if (!throttlingRatio.present() || !currentCost.present()) {
			return {};
		}
		return throttlingRatio.get() * currentCost.get();
	}

	// For a given storage server and tag combination, return the limiting transaction rate.
	Optional<double> getLimitingTps(UID storageServerId, TransactionTag tag) const {
		auto const quotaRatio = getQuotaRatio(tag, storageServerId);
		Optional<double> const limitingCost = getLimitingCost(storageServerId);
		Optional<double> const averageTransactionCost = getAverageTransactionCost(tag, storageServerId);
		if (!limitingCost.present() || !averageTransactionCost.present()) {
			return {};
		}

		auto const limitingCostForTag = limitingCost.get() * quotaRatio;
		return limitingCostForTag / averageTransactionCost.get();
	}

	// Return the limiting transaction rate, aggregated across all storage servers.
	// The limits from the worst maxFallingBehind zones are
	// ignored, because we do not non-workload related issues (e.g. slow disks)
	// to affect tag throttling. If more than maxFallingBehind zones are at
	// or near saturation, this indicates that throttling should take place.
	Optional<double> getLimitingTps(TransactionTag tag) const {
		// TODO: The algorithm for ignoring the worst zones can be made more efficient
		std::unordered_map<Optional<Standalone<StringRef>>, double> zoneIdToLimitingTps;
		for (const auto& [id, ssInfo] : ssInfos) {
			auto const limitingTpsForSS = getLimitingTps(id, tag);
			if (limitingTpsForSS.present()) {
				auto it = zoneIdToLimitingTps.find(ssInfo.zoneId);
				if (it != zoneIdToLimitingTps.end()) {
					auto& limitingTpsForZone = it->second;
					limitingTpsForZone = std::min<double>(limitingTpsForZone, limitingTpsForSS.get());
				} else {
					zoneIdToLimitingTps[ssInfo.zoneId] = limitingTpsForSS.get();
				}
			}
		}
		if (zoneIdToLimitingTps.size() <= maxFallingBehind) {
			return {};
		} else {
			std::vector<double> zoneLimits;
			for (const auto& [_, limit] : zoneIdToLimitingTps) {
				zoneLimits.push_back(limit);
			}
			std::nth_element(zoneLimits.begin(), zoneLimits.begin() + maxFallingBehind, zoneLimits.end());
			return zoneLimits[maxFallingBehind];
		}
	}

	Optional<double> getTps(TransactionTag tag, LimitType limitType, double averageTransactionCost) const {
		auto const cost = getQuota(tag, limitType);
		if (!cost.present()) {
			return {};
		} else {
			return cost.get() / averageTransactionCost;
		}
	}

	void removeUnseenQuotas(std::unordered_set<TransactionTag> const& tagsWithQuota) {
		for (auto& [tag, stats] : tagStatistics) {
			if (!tagsWithQuota.count(tag)) {
				stats.clearQuota();
			}
		}
	}

	ACTOR static Future<Void> monitorThrottlingChanges(GlobalTagThrottlerImpl* self) {
		state std::unordered_set<TransactionTag> tagsWithQuota;

		loop {
			state ReadYourWritesTransaction tr(self->db);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					tagsWithQuota.clear();
					state RangeResult currentQuotas = wait(tr.getRange(tagQuotaKeys, CLIENT_KNOBS->TOO_MANY));
					TraceEvent("GlobalTagThrottler_ReadCurrentQuotas", self->id).detail("Size", currentQuotas.size());
					for (auto const kv : currentQuotas) {
						auto const tag = kv.key.removePrefix(tagQuotaPrefix);
						auto const quota = ThrottleApi::TagQuotaValue::fromValue(kv.value);
						self->tagStatistics[tag].setQuota(quota);
						tagsWithQuota.insert(tag);
					}
					self->removeUnseenQuotas(tagsWithQuota);
					self->removeExpiredTags();
					++self->throttledTagChangeId;
					wait(delay(5.0));
					break;
				} catch (Error& e) {
					TraceEvent("GlobalTagThrottler_MonitoringChangesError", self->id).error(e);
					wait(tr.onError(e));
				}
			}
		}
	}

	Optional<double> getTargetTps(TransactionTag tag, bool& isBusy, TraceEvent& te) {
		auto const limitingTps = getLimitingTps(tag);
		auto const averageTransactionCost = getAverageTransactionCost(tag);
		auto const desiredTps = getTps(tag, LimitType::TOTAL, averageTransactionCost);
		if (!desiredTps.present()) {
			return {};
		}
		auto reservedTps = getTps(tag, LimitType::RESERVED, averageTransactionCost);
		auto targetTps = getMax(reservedTps, getMin(desiredTps, limitingTps));

		isBusy = limitingTps.present() && limitingTps.get() < desiredTps.orDefault(0);

		te.detail("Tag", printable(tag))
		    .detail("TargetTps", targetTps)
		    .detail("AverageTransactionCost", averageTransactionCost)
		    .detail("LimitingTps", limitingTps)
		    .detail("ReservedTps", reservedTps)
		    .detail("DesiredTps", desiredTps)
		    .detail("NumStorageServers", throughput.size());

		return targetTps;
	}

public:
	GlobalTagThrottlerImpl(Database db, UID id, int maxFallingBehind)
	  : db(db), id(id), maxFallingBehind(maxFallingBehind) {}
	Future<Void> monitorThrottlingChanges() { return monitorThrottlingChanges(this); }
	void addRequests(TransactionTag tag, int count) {
		auto it = tagStatistics.find(tag);
		if (it == tagStatistics.end()) {
			if (tagStatistics.size() == SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED) {
				CODE_PROBE(true,
				           "Global tag throttler ignoring transactions because maximum number of trackable tags has "
				           "been reached");
				TraceEvent("GlobalTagThrottler_IgnoringRequests")
				    .suppressFor(1.0)
				    .detail("Tag", printable(tag))
				    .detail("Count", count);
			} else {
				tagStatistics[tag].addTransactions(static_cast<double>(count));
			}
		} else {
			it->second.addTransactions(static_cast<double>(count));
		}
	}
	uint64_t getThrottledTagChangeId() const { return throttledTagChangeId; }

	TransactionTagMap<double> getProxyRates(int numProxies) {
		TransactionTagMap<double> result;
		lastBusyTagCount = 0;

		for (auto& [tag, stats] : tagStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			TraceEvent te("GlobalTagThrottler_GotRate", id);
			bool isBusy{ false };
			auto const targetTps = getTargetTps(tag, isBusy, te);
			if (isBusy) {
				++lastBusyTagCount;
			}
			if (targetTps.present()) {
				auto const smoothedTargetTps = stats.updateAndGetTargetLimit(targetTps.get());
				result[tag] = smoothedTargetTps / numProxies;
			} else {
				te.disable();
			}
		}

		return result;
	}

	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() {
		PrioritizedTransactionTagMap<ClientTagThrottleLimits> result;
		lastBusyTagCount = 0;

		for (auto& [tag, stats] : tagStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			bool isBusy{ false };
			TraceEvent te("GlobalTagThrottler_GotClientRate", id);
			auto const targetTps = getTargetTps(tag, isBusy, te);

			if (isBusy) {
				++lastBusyTagCount;
			}

			if (targetTps.present()) {
				auto const clientRate = stats.updateAndGetPerClientLimit(targetTps.get());
				result[TransactionPriority::BATCH][tag] = result[TransactionPriority::DEFAULT][tag] = clientRate;
				te.detail("ClientTps", clientRate.tpsRate);
			} else {
				te.disable();
			}
		}
		return result;
	}

	int64_t autoThrottleCount() const {
		int64_t result{ 0 };
		for (const auto& [tag, stats] : tagStatistics) {
			if (stats.getQuota().present()) {
				++result;
			}
		}
		return result;
	}
	uint32_t busyReadTagCount() const { return lastBusyTagCount; }
	uint32_t busyWriteTagCount() const { return lastBusyTagCount; }
	int64_t manualThrottleCount() const { return 0; }

	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
		auto& ssInfo = ssInfos[ss.id];
		ssInfo.throttlingRatio = ss.getTagThrottlingRatio(SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
		                                                  SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER);
		ssInfo.zoneId = ss.locality.zoneId();

		for (const auto& busyReadTag : ss.busiestReadTags) {
			if (tagStatistics.find(busyReadTag.tag) != tagStatistics.end()) {
				throughput[ss.id][busyReadTag.tag].updateCost(busyReadTag.rate, OpType::READ);
			}
		}
		for (const auto& busyWriteTag : ss.busiestWriteTags) {
			if (tagStatistics.find(busyWriteTag.tag) != tagStatistics.end()) {
				throughput[ss.id][busyWriteTag.tag].updateCost(busyWriteTag.rate, OpType::WRITE);
			}
		}
		return Void();
	}

	void setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
		tagStatistics[tag].setQuota(tagQuotaValue);
	}

	void removeQuota(TransactionTagRef tag) { tagStatistics[tag].clearQuota(); }

	void removeExpiredTags() {
		for (auto it = tagStatistics.begin(); it != tagStatistics.end();) {
			const auto& [tag, stats] = *it;
			if (!stats.recentTransactionsAdded()) {
				for (auto& [ss, tagToCounters] : throughput) {
					tagToCounters.erase(tag);
				}
				it = tagStatistics.erase(it);
			} else {
				++it;
			}
		}
	}

	uint32_t tagsTracked() const { return tagStatistics.size(); }
};

GlobalTagThrottler::GlobalTagThrottler(Database db, UID id, int maxFallingBehind)
  : impl(PImpl<GlobalTagThrottlerImpl>::create(db, id, maxFallingBehind)) {}

GlobalTagThrottler::~GlobalTagThrottler() = default;

Future<Void> GlobalTagThrottler::monitorThrottlingChanges() {
	return impl->monitorThrottlingChanges();
}
void GlobalTagThrottler::addRequests(TransactionTag tag, int count) {
	return impl->addRequests(tag, count);
}
uint64_t GlobalTagThrottler::getThrottledTagChangeId() const {
	return impl->getThrottledTagChangeId();
}
PrioritizedTransactionTagMap<ClientTagThrottleLimits> GlobalTagThrottler::getClientRates() {
	return impl->getClientRates();
}
TransactionTagMap<double> GlobalTagThrottler::getProxyRates(int numProxies) {
	return impl->getProxyRates(numProxies);
}
int64_t GlobalTagThrottler::autoThrottleCount() const {
	return impl->autoThrottleCount();
}
uint32_t GlobalTagThrottler::busyReadTagCount() const {
	return impl->busyReadTagCount();
}
uint32_t GlobalTagThrottler::busyWriteTagCount() const {
	return impl->busyWriteTagCount();
}
int64_t GlobalTagThrottler::manualThrottleCount() const {
	return impl->manualThrottleCount();
}
bool GlobalTagThrottler::isAutoThrottlingEnabled() const {
	return true;
}
Future<Void> GlobalTagThrottler::tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
	return impl->tryUpdateAutoThrottling(ss);
}

void GlobalTagThrottler::setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
	return impl->setQuota(tag, tagQuotaValue);
}

void GlobalTagThrottler::removeQuota(TransactionTagRef tag) {
	return impl->removeQuota(tag);
}

uint32_t GlobalTagThrottler::tagsTracked() const {
	return impl->tagsTracked();
}

void GlobalTagThrottler::removeExpiredTags() {
	return impl->removeExpiredTags();
}

namespace {

enum class LimitType { RESERVED, TOTAL };
enum class OpType { READ, WRITE };

Optional<double> getTPSLimit(GlobalTagThrottler& globalTagThrottler, TransactionTag tag) {
	auto clientRates = globalTagThrottler.getClientRates();
	auto it1 = clientRates.find(TransactionPriority::DEFAULT);
	if (it1 != clientRates.end()) {
		auto it2 = it1->second.find(tag);
		if (it2 != it1->second.end()) {
			return it2->second.tpsRate;
		}
	}
	return {};
}

class MockStorageServer {
	class Cost {
		Smoother smoother;

	public:
		Cost() : smoother(5.0) {}
		Cost& operator+=(double delta) {
			smoother.addDelta(delta);
			return *this;
		}
		double smoothRate() const { return smoother.smoothRate(); }
	};

	UID id;
	// bytes/second that this storage server can handle
	double capacity;
	std::map<TransactionTag, Cost> readCosts, writeCosts;
	Cost totalReadCost, totalWriteCost;

public:
	explicit MockStorageServer(UID id, double capacity) : id(id), capacity(capacity) { ASSERT_GT(capacity, 0); }
	void addReadCost(TransactionTag tag, double cost) {
		readCosts[tag] += cost;
		totalReadCost += cost;
	}
	void addWriteCost(TransactionTag tag, double cost) {
		writeCosts[tag] += cost;
		totalWriteCost += cost;
	}

	void setCapacity(double value) { capacity = value; }

	StorageQueueInfo getStorageQueueInfo() const {
		StorageQueueInfo result(id, LocalityData({}, Value(id.toString()), {}, {}));
		for (const auto& [tag, readCost] : readCosts) {
			double fractionalBusyness{ 0.0 }; // unused for global tag throttling
			result.busiestReadTags.emplace_back(tag, readCost.smoothRate(), fractionalBusyness);
		}
		for (const auto& [tag, writeCost] : writeCosts) {
			double fractionalBusyness{ 0.0 }; // unused for global tag throttling
			result.busiestWriteTags.emplace_back(tag, writeCost.smoothRate(), fractionalBusyness);
		}
		result.lastReply.bytesInput = ((totalReadCost.smoothRate() + totalWriteCost.smoothRate()) / capacity) *
		                              SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER;
		return result;
	}
};

class StorageServerCollection {
	std::vector<MockStorageServer> storageServers;

public:
	StorageServerCollection(size_t size, double targetCost) {
		ASSERT_GT(size, 0);
		storageServers.reserve(size);
		for (int i = 0; i < size; ++i) {
			storageServers.emplace_back(UID(i, i), targetCost);
		}
	}

	void addCost(TransactionTag tag, double cost, std::vector<int> const& storageServerIndices, OpType opType) {
		if (storageServerIndices.empty()) {
			auto const costPerSS = cost / storageServers.size();
			for (auto& storageServer : storageServers) {
				if (opType == OpType::READ) {
					storageServer.addReadCost(tag, costPerSS);
				} else {
					storageServer.addWriteCost(tag, costPerSS);
				}
			}
		} else {
			auto const costPerSS = cost / storageServerIndices.size();
			for (auto i : storageServerIndices) {
				if (opType == OpType::READ) {
					storageServers[i].addReadCost(tag, costPerSS);
				} else {
					storageServers[i].addWriteCost(tag, costPerSS);
				}
			}
		}
	}

	void setCapacity(int index, double value) { storageServers[index].setCapacity(value); }

	std::vector<StorageQueueInfo> getStorageQueueInfos() const {
		std::vector<StorageQueueInfo> result;
		result.reserve(storageServers.size());
		for (const auto& storageServer : storageServers) {
			result.push_back(storageServer.getStorageQueueInfo());
		}
		return result;
	}
};

ACTOR Future<Void> runClient(GlobalTagThrottler* globalTagThrottler,
                             StorageServerCollection* storageServers,
                             TransactionTag tag,
                             double tpsRate,
                             double costPerTransaction,
                             OpType opType,
                             std::vector<int> storageServerIndices = std::vector<int>()) {
	loop {
		auto tpsLimit = getTPSLimit(*globalTagThrottler, tag);
		state double enforcedRate = tpsLimit.present() ? std::min<double>(tpsRate, tpsLimit.get()) : tpsRate;
		wait(delay(1 / enforcedRate));
		storageServers->addCost(tag, costPerTransaction, storageServerIndices, opType);
		globalTagThrottler->addRequests(tag, 1);
	}
}

ACTOR template <class Check>
Future<Void> monitorActor(GlobalTagThrottler* globalTagThrottler, Check check) {
	state int successes = 0;
	loop {
		wait(delay(1.0));
		if (check(*globalTagThrottler)) {
			// Wait for 10 consecutive successes so we're certain
			// than a stable equilibrium has been reached
			if (++successes == 10) {
				return Void();
			}
		} else {
			successes = 0;
		}
	}
}

bool isNear(double a, double b) {
	return abs(a - b) < 3.0;
}

bool isNear(Optional<double> a, Optional<double> b) {
	if (a.present()) {
		return b.present() && isNear(a.get(), b.get());
	} else {
		return !b.present();
	}
}

bool targetRateIsNear(GlobalTagThrottler& globalTagThrottler, TransactionTag tag, Optional<double> expected) {
	Optional<double> rate;
	auto targetRates = globalTagThrottler.getProxyRates(1);
	auto it = targetRates.find(tag);
	if (it != targetRates.end()) {
		rate = it->second;
	}
	TraceEvent("GlobalTagThrottling_RateMonitor")
	    .detail("Tag", tag)
	    .detail("CurrentTPSRate", rate)
	    .detail("ExpectedTPSRate", expected);
	return isNear(rate, expected);
}

bool clientRateIsNear(GlobalTagThrottler& globalTagThrottler, TransactionTag tag, Optional<double> expected) {
	Optional<double> rate;
	auto clientRates = globalTagThrottler.getClientRates();
	auto it1 = clientRates.find(TransactionPriority::DEFAULT);
	if (it1 != clientRates.end()) {
		auto it2 = it1->second.find(tag);
		if (it2 != it1->second.end()) {
			rate = it2->second.tpsRate;
		}
	}
	TraceEvent("GlobalTagThrottling_ClientRateMonitor")
	    .detail("Tag", tag)
	    .detail("CurrentTPSRate", rate)
	    .detail("ExpectedTPSRate", expected);
	return isNear(rate, expected);
}

ACTOR Future<Void> updateGlobalTagThrottler(GlobalTagThrottler* globalTagThrottler,
                                            StorageServerCollection const* storageServers) {
	loop {
		wait(delay(1.0));
		auto const storageQueueInfos = storageServers->getStorageQueueInfos();
		for (const auto& sq : storageQueueInfos) {
			globalTagThrottler->tryUpdateAutoThrottling(sq);
		}
	}
}

} // namespace

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second.
// Client attempts 5 6-byte read transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/Simple") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second.
// Client attempts 5 6-byte write transactions per second.
// Limit should adjust to allow 100/(6*<fungibility_ratio>) transactions per second.
TEST_CASE("/GlobalTagThrottler/WriteThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::WRITE);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 100.0 / (6.0 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO));
	});

	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second for each tag.
// 2 clients each attempt 5 6-byte read transactions per second.
// Both limits should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiTagThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 5.0, 6.0, OpType::READ));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 5.0, 6.0, OpType::READ));
	futures.push_back(updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, 100.0 / 6.0) && targetRateIsNear(gtt, testTag2, 100.0 / 6.0);
	});
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second.
// Client attempts 20 10-byte read transactions per second.
// Limit should adjust to allow 100/10 transactions per second.
TEST_CASE("/GlobalTagThrottler/AttemptWorkloadAboveQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 10.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second.
// 2 clients each attempt 5 6-byte transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> client2 = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 100.0 / 6.0) && clientRateIsNear(gtt, testTag, 100.0 / 6.0);
	});
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || client2 || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second.
// 2 clients each attempt 20 10-byte transactions per second.
// Target rate should adjust to allow 100/10 transactions per second.
// Each client is throttled to only perform 100/20 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiClientThrottling2") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, OpType::READ);
	state Future<Void> client2 = runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 10.0) && clientRateIsNear(gtt, testTag, 5.0);
	});
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota set to 100 bytes/second.
// One client attempts 5 5-byte read transactions per second.
// Another client attempts 25 5-byte read transactions per second.
// Target rate should adjust to allow 100/5 transactions per second.
// This 20 transactions/second limit is split with a distribution of (5, 15) between the 2 clients.
TEST_CASE("/GlobalTagThrottler/SkewedMultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 5.0, OpType::READ);
	state Future<Void> client2 = runClient(&globalTagThrottler, &storageServers, testTag, 25.0, 5.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 20.0) && clientRateIsNear(gtt, testTag, 15.0);
	});
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota is initially set to 100 bytes/second.
// Client attempts 5 6-byte transactions per second.
// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
// Target rate should adjust to allow 100/6 transactions per second.
// Total quota is modified to 50 bytes/second.
// Target rate should adjust to allow 50/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/UpdateQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	tagQuotaValue.totalQuota = 50.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	monitor =
	    monitorActor(&globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 50.0 / 6.0); });
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 bytes/second each.
// Total quota is initially set to 100 bytes/second.
// Client attempts 5 6-byte read transactions per second.
// Target limit adjusts to allow 100/6 transactions per second.
// Then Quota is removed.
// Target limit is removed as a result.
TEST_CASE("/GlobalTagThrottler/RemoveQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	globalTagThrottler.removeQuota(testTag);
	monitor = monitorActor(&globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, {}); });
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 bytes/second each.
// Total quota is set to 100 bytes/second.
// Client attempts 10 6-byte transactions per second
// Target is adjusted to 50/6 transactions per second, to match the total capacity all storage servers.
TEST_CASE("/GlobalTagThrottler/ActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 50 / 6.0) && gtt.busyReadTagCount() == 1;
	});
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 bytes/second each.
// Total quota is set to 50 bytes/second for one tag, 100 bytes/second for another.
// For each tag, a client attempts to execute 10 6-byte read transactions per second.
// Target rates are adjusted to utilize the full 50 bytes/second capacity of the
//   add storage servers. The two tags receive this capacity with a 2:1 ratio,
//   matching the ratio of their total quotas.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalQuota = 50.0;
	tagQuotaValue2.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, (50 / 6.0) / 3) && targetRateIsNear(gtt, testTag2, 2 * (50 / 6.0) / 3) &&
		       gtt.busyReadTagCount() == 2;
	});
	futures.push_back(updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 bytes/second each.
// Total quota is set to 100 bytes/second for each tag.
// Each client attempts 10 6-byte read transactions per second.
// This workload is sent to 2 storage servers per client (with an overlap of one storage server).
// Target rates for both tags are adjusted to 50/6 transactions per second to match the throughput
//   that the busiest server can handle.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling2") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(3, 50);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalQuota = 100.0;
	tagQuotaValue2.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0, 1 }));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, 50 / 6.0) && targetRateIsNear(gtt, testTag2, 50 / 6.0) &&
		       gtt.busyReadTagCount() == 2;
	});
	futures.push_back(updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 bytes/second each.
// Total quota is set to 100 bytes/second for each tag.
// One client attempts 10 6-byte read transactions per second, all directed towards a single storage server.
// Another client, using a different tag, attempts 10 6-byte read transactions split across the other two storage
// servers. Target rates adjust to 50/6 and 100/6 transactions per second for the two clients, based on the capacities
// of the
//   storage servers being accessed.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling3") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(3, 50);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalQuota = 100.0;
	tagQuotaValue2.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0 }));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, 50 / 6.0) && targetRateIsNear(gtt, testTag2, 100 / 6.0) &&
		       gtt.busyReadTagCount() == 1;
	});
	futures.push_back(updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can serve 5 bytes/second each.
// Total quota is set to 100 bytes/second.
// Reserved quota is set to 70 bytes/second.
// A client attempts to execute 10 6-byte read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
//   the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/ReservedQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100.0;
	tagQuotaValue.reservedQuota = 70.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 70 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// Test that tags are expired iff a sufficient amount of time has passed since the
// last transaction with that tag
TEST_CASE("/GlobalTagThrottler/ExpireTags") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	TransactionTag testTag = "sampleTag1"_sr;

	state Future<Void> client =
	    timeout(runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ), 60.0, Void());
	state Future<Void> updater = timeout(updateGlobalTagThrottler(&globalTagThrottler, &storageServers), 60.0, Void());
	wait(client && updater);
	client.cancel();
	updater.cancel();
	ASSERT_EQ(globalTagThrottler.tagsTracked(), 1);
	globalTagThrottler.removeExpiredTags();
	ASSERT_EQ(globalTagThrottler.tagsTracked(), 1);
	wait(delay(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER + 1.0));
	ASSERT_EQ(globalTagThrottler.tagsTracked(), 1);
	globalTagThrottler.removeExpiredTags();
	ASSERT_EQ(globalTagThrottler.tagsTracked(), 0);
	return Void();
}

// Test that the number of tags tracked does not grow beyond SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED
TEST_CASE("/GlobalTagThrottler/TagLimit") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	std::vector<Future<Void>> futures;
	for (int i = 0; i < 2 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED; ++i) {
		Arena arena;
		TransactionTag tag = makeString(8, arena);
		deterministicRandom()->randomBytes(mutateString(tag), tag.size());
		futures.push_back(runClient(&globalTagThrottler, &storageServers, tag, 1.0, 6.0, OpType::READ));
	}
	wait(timeout(waitForAll(futures), 60.0, Void()));
	ASSERT_EQ(globalTagThrottler.tagsTracked(), SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED);
	return Void();
}

// 9 storage servers can handle 100 bytes/second each.
// 1 unhealthy storage server can only handle 1 byte/second.
// Total quota is set to 100 bytes/second.
// Client attempts 5 6-byte transactions per second.
// Target rate adjusts to 100/6 transactions per second, ignoring the worst storage server.
// Then, a second storage server becomes unhealthy and can only handle 1 byte/second.
// Target rate adjusts down to 1/6 transactions per second, because only one bad zone can be ignored.
TEST_CASE("/GlobalTagThrottler/IgnoreWorstZone") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{}, 1);
	state StorageServerCollection storageServers(10, 100);
	state TransactionTag testTag = "sampleTag1"_sr;
	storageServers.setCapacity(0, 1);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	tagQuotaValue.totalQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	storageServers.setCapacity(1, 1);
	monitor =
	    monitorActor(&globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 1.0 / 6.0); });
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}
