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
#include "fdbclient/TagThrottle.h"
#include "fdbclient/ThrottlingId.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/Knobs.h"
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

class GlobalTagThrottlerImpl {
	template <class K, class V, class H>
	static Optional<V> tryGet(std::unordered_map<K, V, H> const& m, K const& k) {
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
		  : readCost(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME),
		    writeCost(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}

		void updateCost(double newCost, OpType opType) {
			if (opType == OpType::READ) {
				readCost.setTotal(newCost);
			} else {
				writeCost.setTotal(newCost);
			}
		}

		double getCost() const { return readCost.smoothTotal() + writeCost.smoothTotal(); }
	};

	// Track various statistics per throttling ID, aggregated across all storage servers
	class PerThrottlingIdStatistics {
		HoltLinearSmoother transactionCounter;
		Smoother targetRate;
		double transactionsLastAdded;
		double lastLogged;

	public:
		explicit PerThrottlingIdStatistics()
		  : transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_COUNT_FOLDING_TIME,
		                       SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_RATE_FOLDING_TIME),
		    targetRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME), transactionsLastAdded(now()),
		    lastLogged(0) {}

		void addTransactions(int count) {
			transactionsLastAdded = now();
			transactionCounter.addDelta(count);
		}

		double getTransactionRate() const { return transactionCounter.smoothRate(); }

		double updateAndGetTargetLimit(double targetTps) {
			targetRate.setTotal(targetTps);
			return targetRate.smoothTotal();
		}

		bool recentTransactionsAdded() const {
			return now() - transactionsLastAdded < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER;
		}

		bool canLog() const { return now() - lastLogged > SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRACE_INTERVAL; }

		void updateLastLogged() { lastLogged = now(); }
	};

	IRKMetricsTracker const* metricsTracker;
	IRKThroughputQuotaCache const* quotaCache;
	UID id;
	int maxFallingBehind{ 0 };
	uint32_t lastBusyThrottlingIdCount{ 0 };

	ThrottlingIdMap<PerThrottlingIdStatistics> throttlingStatistics;
	std::unordered_map<UID, ThrottlingIdMap<ThroughputCounters>> throughput;

	// Returns the cost rate for the given throttling ID on the given storage server
	Optional<double> getCurrentCost(UID storageServerId, ThrottlingId throttlingId) const {
		auto const throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
		if (!throttlingIdToThroughputCounters.present()) {
			return {};
		}
		auto const throughputCounter = tryGet(throttlingIdToThroughputCounters.get(), throttlingId);
		if (!throughputCounter.present()) {
			return {};
		}
		return throughputCounter.get().getCost();
	}

	// Return the cost rate on the given storage server, summed across all throttling IDs
	Optional<double> getCurrentCost(UID storageServerId) const {
		auto throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
		if (!throttlingIdToThroughputCounters.present()) {
			return {};
		}
		double result = 0;
		for (const auto& [_, throughputCounters] : throttlingIdToThroughputCounters.get()) {
			result += throughputCounters.getCost();
		}
		return result;
	}

	// Return the cost rate for the given throttlingId, summed across all storage servers
	double getCurrentCost(ThrottlingId throttlingId) const {
		double result{ 0.0 };
		for (const auto& [ssId, _] : throughput) {
			result += getCurrentCost(ssId, throttlingId).orDefault(0);
		}

		return result;
	}

	// For transactions with the provided throttlingId, returns the average cost that gets associated with the provided
	// storage server
	Optional<double> getAverageTransactionCost(ThrottlingId throttlingId, UID storageServerId) const {
		auto const cost = getCurrentCost(storageServerId, throttlingId);
		if (!cost.present()) {
			return {};
		}
		auto const stats = tryGet(throttlingStatistics, throttlingId);
		if (!stats.present()) {
			return {};
		}
		auto const transactionRate = stats.get().getTransactionRate();
		// If there is less than GLOBAL_TAG_THROTTLING_MIN_RATE transactions per second, we do not have enough data
		// to accurately compute an average transaction cost.
		if (transactionRate < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE) {
			return {};
		} else {
			return cost.get() / transactionRate;
		}
	}

	// For transactions with the provided throttling ID, returns the average cost of all transactions
	// accross the cluster. The minimum cost is one page. If the transaction rate is too low,
	// return an empty Optional, because no accurate estimation can be made.
	Optional<double> getAverageTransactionCost(ThrottlingId throttlingId, TraceEvent& te) const {
		auto const cost = getCurrentCost(throttlingId);
		auto const stats = tryGet(throttlingStatistics, throttlingId);
		if (!stats.present()) {
			return CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
		}
		auto const transactionRate = stats.get().getTransactionRate();
		te.detail("TransactionRate", transactionRate);
		te.detail("Cost", cost);
		if (transactionRate < SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE) {
			return {};
		} else {
			return std::max(static_cast<double>(CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE), cost / transactionRate);
		}
	}

	// Returns the list of all throttling IDs performing meaningful work on the given storage server
	std::vector<ThrottlingId> getThrottlingIdsAffectingStorageServer(UID storageServerId) const {
		std::vector<ThrottlingId> result;
		auto const throttlingIdToThroughputCounters = tryGet(throughput, storageServerId);
		if (!throttlingIdToThroughputCounters.present()) {
			return {};
		} else {
			result.reserve(throttlingIdToThroughputCounters.get().size());
			for (const auto& [throttlingId, _] : throttlingIdToThroughputCounters.get()) {
				result.push_back(throttlingId);
			}
		}
		return result;
	}

	// Of all throttling IDs meaningfully performing workload on the given storage server,
	// returns the ratio of total quota allocated to the specified throttling ID
	double getQuotaRatio(ThrottlingIdRef throttlingId, UID storageServerId) const {
		double sumQuota{ 0.0 };
		double throttlingIdQuota{ 0.0 };
		auto const throttlingIdsAffectingStorageServer = getThrottlingIdsAffectingStorageServer(storageServerId);
		for (const auto& t : throttlingIdsAffectingStorageServer) {
			auto const tQuota = quotaCache->getTotalQuota(t);
			sumQuota += tQuota.orDefault(0);
			if (t == throttlingId) {
				throttlingIdQuota = tQuota.orDefault(0);
			}
		}
		if (throttlingIdQuota == 0.0) {
			return 0;
		}
		ASSERT_GT(sumQuota, 0.0);
		return throttlingIdQuota / sumQuota;
	}

	Optional<double> getThrottlingRatio(UID storageServerId) const {
		auto const& storageQueueInfo = metricsTracker->getStorageQueueInfo();
		auto it = storageQueueInfo.find(storageServerId);
		if (it == storageQueueInfo.end()) {
			return {};
		}
		auto const& [_, ssInfo] = *it;
		return ssInfo.getTagThrottlingRatio(SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,
		                                    SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER);
	}

	// Returns the desired cost for a storage server, based on its current
	// cost and throttling ratio
	Optional<double> getLimitingCost(UID storageServerId) const {
		Optional<double> const throttlingRatio = getThrottlingRatio(storageServerId);
		Optional<double> const currentCost = getCurrentCost(storageServerId);
		if (!throttlingRatio.present() || !currentCost.present()) {
			return {};
		}
		return throttlingRatio.get() * currentCost.get();
	}

	// For a given storage server and throttling ID combination, return the limiting transaction rate.
	Optional<double> getLimitingTps(UID storageServerId, ThrottlingId throttlingId) const {
		auto const quotaRatio = getQuotaRatio(throttlingId, storageServerId);
		Optional<double> const limitingCost = getLimitingCost(storageServerId);
		Optional<double> const averageTransactionCost = getAverageTransactionCost(throttlingId, storageServerId);
		if (!limitingCost.present() || !averageTransactionCost.present()) {
			return {};
		}

		auto const limitingCostForThrottlingId = limitingCost.get() * quotaRatio;
		return limitingCostForThrottlingId / averageTransactionCost.get();
	}

	// Return the limiting transaction rate, aggregated across all storage servers.
	// The limits from the worst maxFallingBehind zones are
	// ignored, because we do not non-workload related issues (e.g. slow disks)
	// to affect tag throttling. If more than maxFallingBehind zones are at
	// or near saturation, this indicates that throttling should take place.
	Optional<double> getLimitingTps(ThrottlingId throttlingId) const {
		// TODO: The algorithm for ignoring the worst zones can be made more efficient
		std::unordered_map<Optional<Standalone<StringRef>>, double> zoneIdToLimitingTps;
		auto const& storageQueueInfo = metricsTracker->getStorageQueueInfo();
		for (auto const& [id, ssInfo] : storageQueueInfo) {
			auto const limitingTpsForSS = getLimitingTps(id, throttlingId);
			if (limitingTpsForSS.present()) {
				auto it = zoneIdToLimitingTps.find(ssInfo.locality.zoneId());
				if (it != zoneIdToLimitingTps.end()) {
					auto& limitingTpsForZone = it->second;
					limitingTpsForZone = std::min<double>(limitingTpsForZone, limitingTpsForSS.get());
				} else {
					zoneIdToLimitingTps[ssInfo.locality.zoneId()] = limitingTpsForSS.get();
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

	Optional<double> getTargetTps(ThrottlingId throttlingId, bool& isBusy, TraceEvent& te) {
		auto const limitingTps = getLimitingTps(throttlingId);
		auto const averageTransactionCost = getAverageTransactionCost(throttlingId, te);
		auto const totalQuota = quotaCache->getTotalQuota(throttlingId);
		auto const reservedQuota = quotaCache->getReservedQuota(throttlingId);
		if (!averageTransactionCost.present() || !totalQuota.present() || !reservedQuota.present()) {
			return {};
		}
		auto const desiredTps = totalQuota.get() / averageTransactionCost.get();
		auto const reservedTps = reservedQuota.get() / averageTransactionCost.get();
		auto const targetTps = getMax(reservedTps, getMin(desiredTps, limitingTps));

		isBusy = limitingTps.present() && limitingTps.get() < desiredTps;

		te.detail("ThrottlingId", throttlingId)
		    .detail("TargetTps", targetTps)
		    .detail("AverageTransactionCost", averageTransactionCost)
		    .detail("LimitingTps", limitingTps)
		    .detail("ReservedTps", reservedTps)
		    .detail("DesiredTps", desiredTps)
		    .detail("NumStorageServers", throughput.size())
		    .detail("TotalQuota", totalQuota)
		    .detail("ReservedQuota", reservedQuota);

		return targetTps;
	}

public:
	GlobalTagThrottlerImpl(IRKMetricsTracker const& metricsTracker,
	                       IRKThroughputQuotaCache const& quotaCache,
	                       UID id,
	                       int maxFallingBehind)
	  : metricsTracker(&metricsTracker), quotaCache(&quotaCache), id(id), maxFallingBehind(maxFallingBehind) {}
	void addRequests(ThrottlingId throttlingId, int count) {
		auto it = throttlingStatistics.find(throttlingId);
		if (it == throttlingStatistics.end()) {
			if (throttlingStatistics.size() == SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED) {
				CODE_PROBE(
				    true,
				    "Global tag throttler ignoring transactions because maximum number of trackable throttling IDs has "
				    "been reached");
				TraceEvent("GlobalTagThrottler_IgnoringRequests")
				    .suppressFor(60.0)
				    .detail("ThrottlingId", throttlingId)
				    .detail("Count", count);
			} else {
				throttlingStatistics[throttlingId].addTransactions(static_cast<double>(count));
			}
		} else {
			it->second.addTransactions(static_cast<double>(count));
		}
	}

	ThrottlingIdMap<double> getProxyRates(int numProxies) {
		ThrottlingIdMap<double> result;
		lastBusyThrottlingIdCount = 0;

		for (auto& [throttlingId, stats] : throttlingStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			TraceEvent te("GlobalTagThrottler_GotRate", id);
			bool const traceEnabled = stats.canLog();
			if (!traceEnabled) {
				te.disable();
			}
			bool isBusy{ false };
			auto const targetTps = getTargetTps(throttlingId, isBusy, te);
			if (isBusy) {
				++lastBusyThrottlingIdCount;
			}
			if (targetTps.present()) {
				auto const smoothedTargetTps = stats.updateAndGetTargetLimit(targetTps.get());
				te.detail("SmoothedTargetTps", smoothedTargetTps).detail("NumProxies", numProxies);
				result[throttlingId] = std::max(1.0, smoothedTargetTps / numProxies);
				if (traceEnabled) {
					stats.updateLastLogged();
				}
			} else {
				te.disable();
			}
		}

		return result;
	}

	int64_t throttleCount() const { return lastBusyThrottlingIdCount; }
	int64_t manualThrottleCount() const { return 0; }

	void updateThrottling(StorageQueueInfo const& ss) {
		auto& throttlingIdToThroughputCounters = throughput[ss.id];
		std::unordered_set<ThrottlingId, HashThrottlingId> busyReaders, busyWriters;
		for (const auto& busyReader : ss.busiestReaders) {
			busyReaders.insert(busyReader.throttlingId);
			if (throttlingStatistics.find(busyReader.throttlingId) != throttlingStatistics.end()) {
				throttlingIdToThroughputCounters[busyReader.throttlingId].updateCost(busyReader.rate, OpType::READ);
			}
		}
		for (const auto& busyWriter : ss.busiestWriters) {
			busyWriters.insert(busyWriter.throttlingId);
			if (throttlingStatistics.find(busyWriter.throttlingId) != throttlingStatistics.end()) {
				throttlingIdToThroughputCounters[busyWriter.throttlingId].updateCost(busyWriter.rate, OpType::WRITE);
			}
		}

		for (auto& [throttlingId, throughputCounters] : throttlingIdToThroughputCounters) {
			if (!busyReaders.count(throttlingId)) {
				throughputCounters.updateCost(0.0, OpType::READ);
			}
			if (!busyWriters.count(throttlingId)) {
				throughputCounters.updateCost(0.0, OpType::WRITE);
			}
		}
	}

	void removeExpiredThrottlingIds() {
		for (auto it = throttlingStatistics.begin(); it != throttlingStatistics.end();) {
			const auto& [throttlingId, stats] = *it;
			if (!stats.recentTransactionsAdded()) {
				for (auto& [ss, throttlingIdToCounters] : throughput) {
					throttlingIdToCounters.erase(throttlingId);
				}
				it = throttlingStatistics.erase(it);
			} else {
				++it;
			}
		}
	}

	Future<Void> removeExpiredThrottlingIdsActor() {
		return recurring([this] { removeExpiredThrottlingIds(); }, 5.0);
	}

	uint32_t throttlingIdsTracked() const { return throttlingStatistics.size(); }
};

GlobalTagThrottler::GlobalTagThrottler(IRKMetricsTracker const& metricsTracker,
                                       IRKThroughputQuotaCache const& quotaCache,
                                       UID id,
                                       int maxFallingBehind)
  : impl(PImpl<GlobalTagThrottlerImpl>::create(metricsTracker, quotaCache, id, maxFallingBehind)) {}

GlobalTagThrottler::~GlobalTagThrottler() = default;

Future<Void> GlobalTagThrottler::monitorThrottlingChanges() {
	return impl->removeExpiredThrottlingIdsActor();
}

void GlobalTagThrottler::addRequests(ThrottlingId throttlingId, int count) {
	return impl->addRequests(throttlingId, count);
}
ThrottlingIdMap<double> GlobalTagThrottler::getProxyRates(int numProxies) {
	return impl->getProxyRates(numProxies);
}
int64_t GlobalTagThrottler::throttleCount() const {
	return impl->throttleCount();
}
void GlobalTagThrottler::updateThrottling(StorageQueueInfo const& ss) {
	return impl->updateThrottling(ss);
}

uint32_t GlobalTagThrottler::throttlingIdsTracked() const {
	return impl->throttlingIdsTracked();
}

void GlobalTagThrottler::removeExpiredThrottlingIds() {
	return impl->removeExpiredThrottlingIds();
}

namespace {

enum class LimitType { RESERVED, TOTAL };
enum class OpType { READ, WRITE };

Optional<double> getTPSLimit(GlobalTagThrottler& globalTagThrottler, ThrottlingId throttlingId) {
	auto rates = globalTagThrottler.getProxyRates(/*numProxies=*/1);
	auto it = rates.find(throttlingId);
	if (it != rates.end()) {
		return it->second;
	}
	return {};
}

class MockStorageServer {
	class Cost {
		HoltLinearSmoother smoother;

	public:
		Cost() : smoother(1.0) {}
		Cost& operator+=(double delta) {
			smoother.addDelta(delta);
			return *this;
		}
		double smoothRate() const { return smoother.smoothRate(); }
	};

	UID id;
	// pages/second that this storage server can handle
	double capacity;
	ThrottlingIdMap<Cost> readCosts, writeCosts;
	Cost totalReadCost, totalWriteCost;

public:
	explicit MockStorageServer(UID id, double capacity) : id(id), capacity(capacity) { ASSERT_GT(capacity, 0); }
	void addReadCost(ThrottlingId throttlingId, double cost) {
		readCosts[throttlingId] += cost;
		totalReadCost += cost;
	}
	void addWriteCost(ThrottlingId throttlingId, double cost) {
		writeCosts[throttlingId] += cost;
		totalWriteCost += cost;
	}

	void setCapacity(double value) { capacity = value; }

	StorageQueueInfo getStorageQueueInfo() const {
		StorageQueueInfo result(id, LocalityData({}, Value(id.toString()), {}, {}));
		for (const auto& [throttlingId, readCost] : readCosts) {
			double fractionalBusyness{ 0.0 }; // unused for global tag throttling
			result.busiestReaders.emplace_back(throttlingId, readCost.smoothRate(), fractionalBusyness);
		}
		for (const auto& [throttlingId, writeCost] : writeCosts) {
			double fractionalBusyness{ 0.0 }; // unused for global tag throttling
			result.busiestWriters.emplace_back(throttlingId, writeCost.smoothRate(), fractionalBusyness);
		}
		result.lastReply.bytesInput = ((totalReadCost.smoothRate() + totalWriteCost.smoothRate()) /
		                               (capacity * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE)) *
		                              SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES;
		return result;
	}
};

class StorageServerCollection {
	std::vector<MockStorageServer> storageServers;

public:
	StorageServerCollection(size_t size, double capacity) {
		ASSERT_GT(size, 0);
		storageServers.reserve(size);
		for (int i = 0; i < size; ++i) {
			storageServers.emplace_back(UID(i, i), capacity);
		}
	}

	void addCost(ThrottlingId throttlingId,
	             double pagesPerSecond,
	             std::vector<int> const& storageServerIndices,
	             OpType opType) {
		if (storageServerIndices.empty()) {
			auto costPerSS = CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (pagesPerSecond / storageServers.size());
			if (opType == OpType::WRITE) {
				costPerSS *= CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO;
			}
			for (auto& storageServer : storageServers) {
				if (opType == OpType::READ) {
					storageServer.addReadCost(throttlingId, costPerSS);
				} else {
					storageServer.addWriteCost(throttlingId, costPerSS);
				}
			}
		} else {
			auto const costPerSS =
			    CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (pagesPerSecond / storageServerIndices.size());
			for (auto i : storageServerIndices) {
				if (opType == OpType::READ) {
					storageServers[i].addReadCost(throttlingId, costPerSS);
				} else {
					storageServers[i].addWriteCost(throttlingId, costPerSS);
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
                             ThrottlingId throttlingId,
                             double tpsRate,
                             double costPerTransaction,
                             OpType opType,
                             std::vector<int> storageServerIndices = std::vector<int>()) {
	loop {
		auto tpsLimit = getTPSLimit(*globalTagThrottler, throttlingId);
		state double enforcedRate = tpsLimit.present() ? std::min<double>(tpsRate, tpsLimit.get()) : tpsRate;
		wait(delay(1 / enforcedRate));
		storageServers->addCost(throttlingId, costPerTransaction, storageServerIndices, opType);
		globalTagThrottler->addRequests(throttlingId, 1);
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
	return abs(a - b) < 0.1 * a;
}

bool isNear(Optional<double> a, Optional<double> b) {
	if (a.present()) {
		return b.present() && isNear(a.get(), b.get());
	} else {
		return !b.present();
	}
}

bool targetRateIsNear(GlobalTagThrottler& globalTagThrottler, ThrottlingId throttlingId, Optional<double> expected) {
	Optional<double> rate;
	auto targetRates = globalTagThrottler.getProxyRates(1);
	auto it = targetRates.find(throttlingId);
	if (it != targetRates.end()) {
		rate = it->second;
	}
	TraceEvent("GlobalTagThrottling_RateMonitor")
	    .detail("ThrottlingId", throttlingId)
	    .detail("CurrentTPSRate", rate)
	    .detail("ExpectedTPSRate", expected);
	return isNear(rate, expected);
}

ACTOR Future<Void> updateGlobalTagThrottler(MockRKMetricsTracker* metricsTracker,
                                            GlobalTagThrottler* globalTagThrottler,
                                            StorageServerCollection const* storageServers) {
	loop {
		wait(delay(1.0));
		auto const storageQueueInfos = storageServers->getStorageQueueInfos();
		for (const auto& ss : storageQueueInfos) {
			globalTagThrottler->updateThrottling(ss);
			metricsTracker->updateStorageQueueInfo(ss);
		}
	}
}

} // namespace

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte read transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/Simple") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte write transactions per second.
// Limit should adjust to allow 100/(6*<fungibility_ratio>) transactions per second.
TEST_CASE("/GlobalTagThrottler/WriteThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::WRITE);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 100.0 / (6.0 * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO));
	});

	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second for each tag.
// 2 clients each attempt 5 6-byte read transactions per second.
// Both limits should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiTagThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 5.0, 6.0, OpType::READ));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 5.0, 6.0, OpType::READ));
	futures.push_back(updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, 100.0 / 6.0) && targetRateIsNear(gtt, testTag2, 100.0 / 6.0);
	});
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 20 10-byte read transactions per second.
// Limit should adjust to allow 100/10 transactions per second.
TEST_CASE("/GlobalTagThrottler/AttemptWorkloadAboveQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 10.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
// Target rate should adjust to allow 100/6 transactions per second.
// Total quota is modified to 50 pages/second.
// Target rate should adjust to allow 50/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/UpdateQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [](auto& gtt) {
		return targetRateIsNear(gtt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 100.0 / 6.0);
	});
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	quotaCache.setQuota(testTag, 50 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	monitor = monitorActor(&globalTagThrottler, [](auto& gtt) {
		return targetRateIsNear(gtt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 50.0 / 6.0);
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page read transactions per second.
// Target limit adjusts to allow 100/6 transactions per second.
// Then Quota is removed.
// Target limit is removed as a result.
TEST_CASE("/GlobalTagThrottler/RemoveQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [](auto& gtt) {
		return targetRateIsNear(gtt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 100.0 / 6.0);
	});
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	quotaCache.removeQuota(testTag);
	monitor = monitorActor(&globalTagThrottler, [](auto& gtt) {
		return targetRateIsNear(gtt, ThrottlingIdRef::fromTag("sampleTag1"_sr), {});
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 100 pages/second.
// Client attempts 10 6-page transactions per second
// Target is adjusted to 50/6 transactions per second, to match the total capacity all storage servers.
TEST_CASE("/GlobalTagThrottler/ActiveThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 50 / 6.0) && gtt.throttleCount() == 1;
	});
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 50 pages/second for one tag, 100 pages/second for another.
// For each tag, a client attempts to execute 10 6-page read transactions per second.
// Target rates are adjusted to utilize the full 50 pages/second capacity of the
//   add storage servers. The two tags receive this capacity with a 2:1 ratio,
//   matching the ratio of their total quotas.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 50 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, (50 / 6.0) / 3) && targetRateIsNear(gtt, testTag2, 2 * (50 / 6.0) / 3) &&
		       gtt.throttleCount() == 2;
	});
	futures.push_back(updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// Each client attempts 10 6-page read transactions per second.
// This workload is sent to 2 storage servers per client (with an overlap of one storage server).
// Target rates for both tags are adjusted to 50/6 transactions per second to match the throughput
//   that the busiest server can handle.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling2") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(3, 50);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0, 1 }));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, 50 / 6.0) && targetRateIsNear(gtt, testTag2, 50 / 6.0) &&
		       gtt.throttleCount() == 2;
	});
	futures.push_back(updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// One client attempts 10 6-page read transactions per second, all directed towards a single storage server.
// Another client, using a different tag, attempts 10 6-page read transactions split across the other two storage
// servers. Target rates adjust to 50/6 and 100/6 transactions per second for the two clients, based on the capacities
// of the
//   storage servers being accessed.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling3") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(3, 50);
	auto testTag1 = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	auto testTag2 = ThrottlingIdRef::fromTag("sampleTag2"_sr);
	quotaCache.setQuota(testTag1, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	quotaCache.setQuota(testTag2, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0 }));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return targetRateIsNear(gtt, testTag1, 50 / 6.0) && targetRateIsNear(gtt, testTag2, 100 / 6.0) &&
		       gtt.throttleCount() == 1;
	});
	futures.push_back(updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 10 storage servers can serve 5 pages/second each.
// Total quota is set to 100 pages/second.
// Reserved quota is set to 70 pages/second.
// A client attempts to execute 10 6-page read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
//   the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/ReservedQuota") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	quotaCache.setQuota(
	    testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 70 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 70 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// Test that tags are expired iff a sufficient amount of time has passed since the
// last transaction with that tag
TEST_CASE("/GlobalTagThrottler/ExpireTags") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	auto testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);

	state Future<Void> client =
	    timeout(runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ), 60.0, Void());
	state Future<Void> updater =
	    timeout(updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers), 60.0, Void());
	wait(client && updater);
	client.cancel();
	updater.cancel();
	ASSERT_EQ(globalTagThrottler.throttlingIdsTracked(), 1);
	globalTagThrottler.removeExpiredThrottlingIds();
	ASSERT_EQ(globalTagThrottler.throttlingIdsTracked(), 1);
	wait(delay(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TAG_EXPIRE_AFTER + 1.0));
	ASSERT_EQ(globalTagThrottler.throttlingIdsTracked(), 1);
	globalTagThrottler.removeExpiredThrottlingIds();
	ASSERT_EQ(globalTagThrottler.throttlingIdsTracked(), 0);
	return Void();
}

// Test that the number of tags tracked does not grow beyond SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED
TEST_CASE("/GlobalTagThrottler/TagLimit") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 0);
	state StorageServerCollection storageServers(10, 5);
	std::vector<Future<Void>> futures;
	for (int i = 0; i < 2 * SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED; ++i) {
		Arena arena;
		TransactionTag tag = makeString(8, arena);
		deterministicRandom()->randomBytes(mutateString(tag), tag.size());
		futures.push_back(
		    runClient(&globalTagThrottler, &storageServers, ThrottlingIdRef::fromTag(tag), 1.0, 6.0, OpType::READ));
	}
	wait(timeout(waitForAll(futures), 60.0, Void()));
	ASSERT_EQ(globalTagThrottler.throttlingIdsTracked(), SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED);
	return Void();
}

// 9 storage servers can handle 100 pages/second each.
// 1 unhealthy storage server can only handle 1 page/second.
// Total quota is set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Target rate adjusts to 100/6 transactions per second, ignoring the worst storage server.
// Then, a second storage server becomes unhealthy and can only handle 1 page/second.
// Target rate adjusts down to 10/6 transactions per second, because only one bad zone can be ignored.
TEST_CASE("/GlobalTagThrottler/IgnoreWorstZone") {
	state MockRKMetricsTracker metricsTracker;
	state MockRKThroughputQuotaCache quotaCache;
	state GlobalTagThrottler globalTagThrottler(metricsTracker, quotaCache, UID{}, 1);
	state StorageServerCollection storageServers(10, 100);
	state ThrottlingId testTag = ThrottlingIdRef::fromTag("sampleTag1"_sr);
	storageServers.setCapacity(0, 1);
	quotaCache.setQuota(testTag, 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE, 0);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [](auto& gtt) {
		return targetRateIsNear(gtt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 100.0 / 6.0);
	});
	state Future<Void> updater = updateGlobalTagThrottler(&metricsTracker, &globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	storageServers.setCapacity(1, 1);
	monitor = monitorActor(&globalTagThrottler, [](auto& gtt) {
		return targetRateIsNear(gtt, ThrottlingIdRef::fromTag("sampleTag1"_sr), 10.0 / 6.0);
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}
