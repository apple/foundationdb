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
#include "fdbserver/ServerThroughputTracker.h"
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

	// Track various statistics per tag, aggregated across all storage servers
	class PerTagStatistics {
		Optional<ThrottleApi::TagQuotaValue> quota;
		HoltLinearSmoother transactionCounter;
		Smoother perClientRate;
		Smoother targetRate;
		double transactionsLastAdded;
		double lastLogged;

	public:
		explicit PerTagStatistics()
		  : transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_COUNT_FOLDING_TIME,
		                       SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRANSACTION_RATE_FOLDING_TIME),
		    perClientRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME),
		    targetRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TARGET_RATE_FOLDING_TIME), transactionsLastAdded(now()),
		    lastLogged(0) {}

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

		bool canLog() const { return now() - lastLogged > SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRACE_INTERVAL; }

		void updateLastLogged() { lastLogged = now(); }
	};

	struct StorageServerInfo {
		Optional<Standalone<StringRef>> zoneId;
		Optional<double> throttlingRatio;
	};

	Database db;
	UID id;
	int maxFallingBehind{ 0 };
	double limitingThreshold{ 0.0 };
	uint64_t throttledTagChangeId{ 0 };
	uint32_t lastBusyTagCount{ 0 };

	std::unordered_map<UID, StorageServerInfo> ssInfos;
	std::unordered_map<TransactionTag, PerTagStatistics> tagStatistics;
	ServerThroughputTracker throughputTracker;

	// For transactions with the provided tag, returns the average cost of all transactions
	// across the cluster. The minimum cost is one page. If the transaction rate is too low,
	// return an empty Optional, because no accurate estimation can be made.
	Optional<double> getAverageTransactionCost(TransactionTag tag, TraceEvent& te) const {
		auto const cost = throughputTracker.getThroughput(tag);
		auto const stats = tryGet(tagStatistics, tag);
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

	Optional<double> getLimitingThrottlingRatio(TransactionTag tag) const {
		std::unordered_map<Optional<Standalone<StringRef>>, double> zoneThrottlingRatios;
		for (auto const& [id, ssInfo] : ssInfos) {
			// Ignore storage servers where throttlingId is not significantly contributing
			auto const throughput = throughputTracker.getThroughput(id, tag);
			if (!throughput.present() || throughput.get() < limitingThreshold) {
				continue;
			}

			if (ssInfo.throttlingRatio.present()) {
				auto const [it, inserted] =
				    zoneThrottlingRatios.try_emplace(ssInfo.zoneId, ssInfo.throttlingRatio.get());
				if (!inserted) {
					auto& zoneThrottlingRatio = it->second;
					zoneThrottlingRatio = std::min<double>(zoneThrottlingRatio, ssInfo.throttlingRatio.get());
				}
			}
		}
		if (zoneThrottlingRatios.size() <= maxFallingBehind) {
			return {};
		} else {
			std::priority_queue<double, std::vector<double>> maxHeap;
			ASSERT_GE(maxFallingBehind, 0);
			for (auto const& [_, throttlingRatio] : zoneThrottlingRatios) {
				if (maxHeap.size() < maxFallingBehind + 1) {
					maxHeap.push(throttlingRatio);
				} else if (maxHeap.top() > throttlingRatio) {
					maxHeap.pop();
					maxHeap.push(throttlingRatio);
				}
			}
			return maxHeap.top();
		}
	}

	// Return the limiting transaction rate, aggregated across all storage servers.
	// The limits from the worst maxFallingBehind zones are
	// ignored, because we do not non-workload related issues (e.g. slow disks)
	// to affect tag throttling. If more than maxFallingBehind zones are at
	// or near saturation, this indicates that throttling should take place.
	Optional<double> getLimitingTps(TransactionTag tag) const {
		auto const stats = tryGet(tagStatistics, tag);
		if (!stats.present()) {
			return {};
		}
		auto const limitingThrottlingRatio = getLimitingThrottlingRatio(tag);
		if (!limitingThrottlingRatio.present()) {
			return {};
		}
		return stats.get().getTransactionRate() * limitingThrottlingRatio.get();
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
		auto const averageTransactionCost = getAverageTransactionCost(tag, te);
		auto const totalQuota = getQuota(tag, LimitType::TOTAL);
		auto const reservedQuota = getQuota(tag, LimitType::RESERVED);
		if (!averageTransactionCost.present() || !totalQuota.present() || !reservedQuota.present()) {
			return {};
		}
		auto const desiredTps = totalQuota.get() / averageTransactionCost.get();
		auto const reservedTps = reservedQuota.get() / averageTransactionCost.get();
		auto const targetTps = getMax(reservedTps, getMin(desiredTps, limitingTps));

		isBusy = limitingTps.present() && limitingTps.get() < desiredTps;

		te.detail("Tag", tag)
		    .detail("TargetTps", targetTps)
		    .detail("AverageTransactionCost", averageTransactionCost)
		    .detail("LimitingTps", limitingTps)
		    .detail("ReservedTps", reservedTps)
		    .detail("DesiredTps", desiredTps)
		    .detail("NumStorageServers", throughputTracker.storageServersTracked())
		    .detail("TotalQuota", totalQuota)
		    .detail("ReservedQuota", reservedQuota);

		return targetTps;
	}

public:
	GlobalTagThrottlerImpl(Database db, UID id, int maxFallingBehind, double limitingThreshold)
	  : db(db), id(id), maxFallingBehind(maxFallingBehind), limitingThreshold(limitingThreshold) {}
	Future<Void> monitorThrottlingChanges() { return monitorThrottlingChanges(this); }
	void addRequests(TransactionTag tag, int count) {
		auto it = tagStatistics.find(tag);
		if (it == tagStatistics.end()) {
			if (tagStatistics.size() == SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MAX_TAGS_TRACKED) {
				CODE_PROBE(true,
				           "Global tag throttler ignoring transactions because maximum number of trackable tags has "
				           "been reached");
				TraceEvent("GlobalTagThrottler_IgnoringRequests")
				    .suppressFor(60.0)
				    .detail("Tag", tag)
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
			bool const traceEnabled = stats.canLog();
			if (!traceEnabled) {
				te.disable();
			}
			bool isBusy{ false };
			auto const targetTps = getTargetTps(tag, isBusy, te);
			if (isBusy) {
				++lastBusyTagCount;
			}
			if (targetTps.present()) {
				auto const smoothedTargetTps = stats.updateAndGetTargetLimit(targetTps.get());
				te.detail("SmoothedTargetTps", smoothedTargetTps).detail("NumProxies", numProxies);
				result[tag] = std::max(1.0, smoothedTargetTps / numProxies);
				if (traceEnabled) {
					stats.updateLastLogged();
				}
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
			bool const traceEnabled = stats.canLog();
			if (!traceEnabled) {
				te.disable();
			}
			auto const targetTps = getTargetTps(tag, isBusy, te);

			if (isBusy) {
				++lastBusyTagCount;
			}

			if (targetTps.present()) {
				auto const clientRate = stats.updateAndGetPerClientLimit(targetTps.get());
				result[TransactionPriority::BATCH][tag] = result[TransactionPriority::DEFAULT][tag] = clientRate;
				te.detail("ClientTps", clientRate.tpsRate);
				if (traceEnabled) {
					stats.updateLastLogged();
				}
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

	void updateThrottling(Map<UID, StorageQueueInfo> const& sqInfos) {
		for (auto it = sqInfos.begin(); it != sqInfos.end(); ++it) {
			auto& ss = it->value;
			auto& ssInfo = ssInfos[ss.id];
			ssInfo.throttlingRatio =
			    ss.getTagThrottlingRatio(SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES,
			                             SERVER_KNOBS->AUTO_TAG_THROTTLE_SPRING_BYTES_STORAGE_SERVER);
			ssInfo.zoneId = ss.locality.zoneId();
		}
		throughputTracker.update(sqInfos);
	}

	void setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
		tagStatistics[tag].setQuota(tagQuotaValue);
	}

	void removeQuota(TransactionTagRef tag) { tagStatistics[tag].clearQuota(); }

	void removeExpiredTags() {
		for (auto it = tagStatistics.begin(); it != tagStatistics.end();) {
			const auto& [tag, stats] = *it;
			if (!stats.recentTransactionsAdded()) {
				throughputTracker.removeTag(tag);
				it = tagStatistics.erase(it);
			} else {
				++it;
			}
		}
	}

	uint32_t tagsTracked() const { return tagStatistics.size(); }
};

GlobalTagThrottler::GlobalTagThrottler(Database db, UID id, int maxFallingBehind, double limitingThreshold)
  : impl(PImpl<GlobalTagThrottlerImpl>::create(db, id, maxFallingBehind, limitingThreshold)) {}

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
void GlobalTagThrottler::updateThrottling(Map<UID, StorageQueueInfo> const& sqInfos) {
	return impl->updateThrottling(sqInfos);
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
		Cost() : smoother(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_COST_FOLDING_TIME) {}
		Cost& operator+=(double delta) {
			smoother.addDelta(delta);
			return *this;
		}
		double smoothRate() const { return smoother.smoothRate(); }
	};

	UID id;
	// pages/second that this storage server can handle
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
		result.lastReply.bytesInput = ((totalReadCost.smoothRate() + totalWriteCost.smoothRate()) /
		                               (capacity * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE)) *
		                              SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES;
		result.valid = true;
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

	void addCost(TransactionTag tag,
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
					storageServer.addReadCost(tag, costPerSS);
				} else {
					storageServer.addWriteCost(tag, costPerSS);
				}
			}
		} else {
			auto const costPerSS =
			    CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE * (pagesPerSecond / storageServerIndices.size());
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

	Map<UID, StorageQueueInfo> getStorageQueueInfos() const {
		Map<UID, StorageQueueInfo> result;
		for (const auto& storageServer : storageServers) {
			auto const sqInfo = storageServer.getStorageQueueInfo();
			result.insert(mapPair(sqInfo.id, sqInfo));
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
	return abs(a - b) < 0.1 * a;
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
	auto const targetRates = globalTagThrottler.getProxyRates(1);
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

bool totalTargetRateIsNear(GlobalTagThrottler& globalTagThrottler, double expected) {
	auto const targetRates = globalTagThrottler.getProxyRates(1);
	double targetRateSum = 0.0;
	for (auto const& [_, targetRate] : targetRates) {
		targetRateSum += targetRate;
	}
	TraceEvent("GlobalTagThrottling_TotalRateMonitor")
	    .detail("NumTags", targetRates.size())
	    .detail("CurrentTotalTPSRate", targetRateSum)
	    .detail("ExpectedTotalTPSRate", expected);
	return isNear(targetRateSum, expected);
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
		globalTagThrottler->updateThrottling(storageServers->getStorageQueueInfos());
	}
}

GlobalTagThrottler getTestGlobalTagThrottler(int maxFallingBehind = 0) {
	return GlobalTagThrottler(
	    Database{}, UID{}, maxFallingBehind, /*limitingThreshold=*/CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE);
}

} // namespace

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte read transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/Simple") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 5 6-byte write transactions per second.
// Limit should adjust to allow 100/(6*<fungibility_ratio>) transactions per second.
TEST_CASE("/GlobalTagThrottler/WriteThrottling") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::WRITE);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 100.0 / (6.0 * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO));
	});

	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second for each tag.
// 2 clients each attempt 5 6-byte read transactions per second.
// Both limits should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiTagThrottling") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// Client attempts 20 10-byte read transactions per second.
// Limit should adjust to allow 100/10 transactions per second.
TEST_CASE("/GlobalTagThrottler/AttemptWorkloadAboveQuota") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, OpType::READ);
	state Future<Void> monitor =
	    monitorActor(&globalTagThrottler, [testTag](auto& gtt) { return targetRateIsNear(gtt, testTag, 10.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// 2 clients each attempt 5 6-byte transactions per second.
// Limit should adjust to allow 100/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// 2 clients each attempt 20 10-page transactions per second.
// Target rate should adjust to allow 100/10 transactions per second.
// Each client is throttled to only perform (100/10)/2 transactions per second.
TEST_CASE("/GlobalTagThrottler/MultiClientThrottling2") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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

// 10 storage servers can handle 100 pages/second each.
// Total quota set to 100 pages/second.
// One client attempts 5 5-page read transactions per second.
// Another client attempts 25 5-page read transactions per second.
// Target rate should adjust to allow 100/5 transactions per second.
// This 20 transactions/second limit is split with a distribution of (5, 15) between the 2 clients.
TEST_CASE("/GlobalTagThrottler/SkewedMultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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

// 10 storage servers can handle 100 pages/second each.
// Total quota is initially set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
// Target rate should adjust to allow 100/6 transactions per second.
// Total quota is modified to 50 pages/second.
// Target rate should adjust to allow 50/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/UpdateQuota") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	tagQuotaValue.totalQuota = 50 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	monitor =
	    monitorActor(&globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 50.0 / 6.0); });
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
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 100 pages/second.
// Client attempts 10 6-page transactions per second
// Target is adjusted to 50/6 transactions per second, to match the total capacity all storage servers.
TEST_CASE("/GlobalTagThrottler/ActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag](auto& gtt) {
		return targetRateIsNear(gtt, testTag, 50 / 6.0) && gtt.busyReadTagCount() == 1;
	});
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// 10 storage servers can handle 5 pages/second each.
// Total quota is set to 50 pages/second for one tag, 100 pages/second for another.
// For each tag, a client attempts to execute 10 6-page read transactions per second.
// Target rates are adjusted to utilize the full 50 pages/second capacity of the
// storage servers.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling1") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalQuota = 50 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	tagQuotaValue2.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return totalTargetRateIsNear(gtt, 50 / 6.0) && gtt.busyReadTagCount() == 2;
	});
	futures.push_back(updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// Each client attempts 10 6-page read transactions per second.
// This workload is sent to 2 storage servers per client (with an overlap of one storage server).
// The total target rate summed across both tags is adjusted to match the throughput that the
// busiest server can handle (50/3 transactions per second).
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling2") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(3, 50);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	tagQuotaValue2.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, OpType::READ, { 0, 1 }));
	futures.push_back(runClient(&globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, OpType::READ, { 1, 2 }));
	state Future<Void> monitor = monitorActor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		return totalTargetRateIsNear(gtt, 50 / 3.0) && gtt.busyReadTagCount() == 2;
	});
	futures.push_back(updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

// 3 storage servers can handle 50 pages/second each.
// Total quota is set to 100 pages/second for each tag.
// One client attempts 10 6-page read transactions per second, all directed towards a single storage server.
// Another client, using a different tag, attempts 10 6-page read transactions split across the other two storage
// servers. Target rates adjust to 50/6 and 100/6 transactions per second for the two clients, based on the capacities
// of the storage servers being accessed.
TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling3") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(3, 50);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	tagQuotaValue2.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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

// 10 storage servers can serve 5 pages/second each.
// Total quota is set to 100 pages/second.
// Reserved quota is set to 70 pages/second.
// A client attempts to execute 10 6-page read transactions per second.
// Despite the storage server only having capacity to serve 50/6 transactions per second,
//   the reserved quota will ensure the target rate adjusts to 70/6 transactions per second.
TEST_CASE("/GlobalTagThrottler/ReservedQuota") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
	state StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	tagQuotaValue.reservedQuota = 70 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
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
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
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
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler();
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

// 9 storage servers can handle 100 pages/second each.
// 1 unhealthy storage server can only handle 1 page/second.
// Total quota is set to 100 pages/second.
// Client attempts 5 6-page transactions per second.
// Target rate adjusts to 100/6 transactions per second, ignoring the worst storage server.
// Then, a second storage server becomes unhealthy and can only handle 1 page/second.
// Target rate adjusts down to 10/6 transactions per second, because only one bad zone can be ignored.
TEST_CASE("/GlobalTagThrottler/IgnoreWorstZone") {
	state GlobalTagThrottler globalTagThrottler = getTestGlobalTagThrottler(/*maxFallingBehind=*/1);
	state StorageServerCollection storageServers(10, 100);
	state TransactionTag testTag = "sampleTag1"_sr;
	storageServers.setCapacity(0, 1);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	tagQuotaValue.totalQuota = 100 * CLIENT_KNOBS->TAG_THROTTLING_PAGE_SIZE;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, OpType::READ);
	state Future<Void> monitor = monitorActor(
	    &globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0); });
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	storageServers.setCapacity(1, 1);
	monitor =
	    monitorActor(&globalTagThrottler, [](auto& gtt) { return targetRateIsNear(gtt, "sampleTag1"_sr, 10.0 / 6.0); });
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}
