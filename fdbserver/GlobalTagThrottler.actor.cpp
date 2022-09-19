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
				writeCost.setTotal(newCost);
			}
		}

		double getCost(OpType opType) const {
			if (opType == OpType::READ) {
				return readCost.smoothTotal();
			} else {
				return writeCost.smoothTotal();
			}
		}
	};

	// Track various statistics per tag, aggregated across all storage servers
	class PerTagStatistics {
		Optional<ThrottleApi::TagQuotaValue> quota;
		Smoother transactionCounter;
		Smoother perClientRate;
		Smoother targetRate;

	public:
		explicit PerTagStatistics()
		  : transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    perClientRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    targetRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME) {}

		Optional<ThrottleApi::TagQuotaValue> getQuota() const { return quota; }

		void setQuota(ThrottleApi::TagQuotaValue quota) { this->quota = quota; }

		void clearQuota() { quota = {}; }

		void addTransactions(int count) { transactionCounter.addDelta(count); }

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
	};

	Database db;
	UID id;
	uint64_t throttledTagChangeId{ 0 };
	uint32_t lastBusyReadTagCount{ 0 };
	uint32_t lastBusyWriteTagCount{ 0 };

	std::unordered_map<UID, Optional<double>> throttlingRatios;
	std::unordered_map<TransactionTag, PerTagStatistics> tagStatistics;
	std::unordered_map<UID, std::unordered_map<TransactionTag, ThroughputCounters>> throughput;

	// Returns the cost rate for the given tag on the given storage server
	Optional<double> getCurrentCost(UID storageServerId, TransactionTag tag, OpType opType) const {
		auto const tagToThroughputCounters = tryGet(throughput, storageServerId);
		if (!tagToThroughputCounters.present()) {
			return {};
		}
		auto const throughputCounter = tryGet(tagToThroughputCounters.get(), tag);
		if (!throughputCounter.present()) {
			return {};
		}
		return throughputCounter.get().getCost(opType);
	}

	// Return the cost rate on the given storage server, summed across all tags
	Optional<double> getCurrentCost(UID storageServerId, OpType opType) const {
		auto tagToPerTagThroughput = tryGet(throughput, storageServerId);
		if (!tagToPerTagThroughput.present()) {
			return {};
		}
		double result = 0;
		for (const auto& [tag, perTagThroughput] : tagToPerTagThroughput.get()) {
			result += perTagThroughput.getCost(opType);
		}
		return result;
	}

	// Return the cost rate for the given tag, summed across all storage servers
	double getCurrentCost(TransactionTag tag, OpType opType) const {
		double result{ 0.0 };
		for (const auto& [id, _] : throughput) {
			result += getCurrentCost(id, tag, opType).orDefault(0);
		}
		TraceEvent("GlobalTagThrottler_GetCurrentCost")
		    .detail("Tag", printable(tag))
		    .detail("Op", (opType == OpType::READ) ? "Read" : "Write")
		    .detail("Cost", result);

		return result;
	}

	// For transactions with the provided tag, returns the average cost that gets associated with the provided storage
	// server
	Optional<double> getAverageTransactionCost(TransactionTag tag, UID storageServerId, OpType opType) const {
		auto const cost = getCurrentCost(storageServerId, tag, opType);
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
	double getAverageTransactionCost(TransactionTag tag, OpType opType) const {
		auto const cost = getCurrentCost(tag, opType);
		auto const stats = tryGet(tagStatistics, tag);
		if (!stats.present()) {
			return 1.0;
		}
		auto const transactionRate = stats.get().getTransactionRate();
		TraceEvent("GlobalTagThrottler_GetAverageTransactionCost")
		    .detail("Tag", tag)
		    .detail("OpType", (opType == OpType::READ) ? "Read" : "Write")
		    .detail("TransactionRate", transactionRate)
		    .detail("Cost", cost);
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

	Optional<double> getQuota(TransactionTag tag, OpType opType, LimitType limitType) const {
		auto const stats = tryGet(tagStatistics, tag);
		if (!stats.present()) {
			return {};
		}
		auto const quota = stats.get().getQuota();
		if (!quota.present()) {
			return {};
		}
		if (limitType == LimitType::TOTAL) {
			return (opType == OpType::READ) ? quota.get().totalReadQuota : quota.get().totalWriteQuota;
		} else {
			return (opType == OpType::READ) ? quota.get().reservedReadQuota : quota.get().reservedWriteQuota;
		}
	}

	// Of all tags meaningfully performing workload on the given storage server,
	// returns the ratio of total quota allocated to the specified tag
	double getQuotaRatio(TransactionTagRef tag, UID storageServerId, OpType opType) const {
		double sumQuota{ 0.0 };
		double tagQuota{ 0.0 };
		auto const tagsAffectingStorageServer = getTagsAffectingStorageServer(storageServerId);
		for (const auto& t : tagsAffectingStorageServer) {
			auto const tQuota = getQuota(t, opType, LimitType::TOTAL);
			sumQuota += tQuota.orDefault(0);
			if (tag.compare(tag) == 0) {
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
	Optional<double> getLimitingCost(UID storageServerId, OpType opType) const {
		auto const throttlingRatio = tryGet(throttlingRatios, storageServerId);
		auto const currentCost = getCurrentCost(storageServerId, opType);
		if (!throttlingRatio.present() || !currentCost.present() || !throttlingRatio.get().present()) {
			return {};
		}
		return throttlingRatio.get().get() * currentCost.get();
	}

	// For a given storage server and tag combination, return the limiting transaction rate.
	Optional<double> getLimitingTps(UID storageServerId, TransactionTag tag, OpType opType) const {
		auto const quotaRatio = getQuotaRatio(tag, storageServerId, opType);
		auto const limitingCost = getLimitingCost(storageServerId, opType);
		auto const averageTransactionCost = getAverageTransactionCost(tag, storageServerId, opType);
		if (!limitingCost.present() || !averageTransactionCost.present()) {
			return {};
		}

		auto const limitingCostForTag = limitingCost.get() * quotaRatio;
		return limitingCostForTag / averageTransactionCost.get();
	}

	// Return the limiting transaction rate, aggregated across all storage servers
	Optional<double> getLimitingTps(TransactionTag tag, OpType opType) const {
		Optional<double> result;
		for (const auto& [id, _] : throttlingRatios) {
			auto const targetTpsForSS = getLimitingTps(id, tag, opType);
			result = getMin(result, targetTpsForSS);
		}
		return result;
	}

	Optional<double> getTps(TransactionTag tag,
	                        OpType opType,
	                        LimitType limitType,
	                        double averageTransactionCost) const {
		auto const cost = getQuota(tag, opType, limitType);
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

	Optional<double> getTargetTps(TransactionTag tag, bool& isReadBusy, bool& isWriteBusy, TraceEvent& te) {
		auto const readLimitingTps = getLimitingTps(tag, OpType::READ);
		auto const writeLimitingTps = getLimitingTps(tag, OpType::WRITE);
		Optional<double> limitingTps;
		limitingTps = getMin(readLimitingTps, writeLimitingTps);

		auto const averageTransactionReadCost = getAverageTransactionCost(tag, OpType::READ);
		auto const averageTransactionWriteCost = getAverageTransactionCost(tag, OpType::WRITE);
		auto const readDesiredTps = getTps(tag, OpType::READ, LimitType::TOTAL, averageTransactionReadCost);
		auto const writeDesiredTps = getTps(tag, OpType::WRITE, LimitType::TOTAL, averageTransactionWriteCost);
		Optional<double> desiredTps;
		desiredTps = getMin(readDesiredTps, writeDesiredTps);

		if (!desiredTps.present()) {
			return {};
		}

		isReadBusy = readLimitingTps.present() && readLimitingTps.get() < readDesiredTps.orDefault(0);
		isWriteBusy = writeLimitingTps.present() && writeLimitingTps.get() < writeDesiredTps.orDefault(0);

		auto const readReservedTps = getTps(tag, OpType::READ, LimitType::RESERVED, averageTransactionReadCost);
		auto const writeReservedTps = getTps(tag, OpType::WRITE, LimitType::RESERVED, averageTransactionWriteCost);
		Optional<double> reservedTps;
		reservedTps = getMax(readReservedTps, writeReservedTps);

		auto targetTps = getMax(reservedTps, getMin(desiredTps, limitingTps));

		te.detail("Tag", printable(tag))
		    .detail("TargetTps", targetTps)
		    .detail("AverageTransactionReadCost", averageTransactionReadCost)
		    .detail("AverageTransactionWriteCost", averageTransactionWriteCost)
		    .detail("LimitingTps", limitingTps)
		    .detail("ReservedTps", reservedTps)
		    .detail("DesiredTps", desiredTps)
		    .detail("NumStorageServers", throughput.size());

		return targetTps;
	}

public:
	GlobalTagThrottlerImpl(Database db, UID id) : db(db), id(id) {}
	Future<Void> monitorThrottlingChanges() { return monitorThrottlingChanges(this); }
	void addRequests(TransactionTag tag, int count) { tagStatistics[tag].addTransactions(static_cast<double>(count)); }
	uint64_t getThrottledTagChangeId() const { return throttledTagChangeId; }

	PrioritizedTransactionTagMap<double> getProxyRates(int numProxies) {
		PrioritizedTransactionTagMap<double> result;
		lastBusyReadTagCount = lastBusyWriteTagCount = 0;

		for (auto& [tag, stats] : tagStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			TraceEvent te("GlobalTagThrottler_GotRate", id);
			bool isReadBusy = false, isWriteBusy = false;
			auto const targetTps = getTargetTps(tag, isReadBusy, isWriteBusy, te);
			if (isReadBusy) {
				++lastBusyReadTagCount;
			}
			if (isWriteBusy) {
				++lastBusyWriteTagCount;
			}
			if (targetTps.present()) {
				auto const smoothedTargetTps = stats.updateAndGetTargetLimit(targetTps.get());
				result[TransactionPriority::BATCH][tag] = result[TransactionPriority::DEFAULT][tag] =
				    smoothedTargetTps / numProxies;
			} else {
				te.disable();
			}
		}

		return result;
	}

	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() {
		PrioritizedTransactionTagMap<ClientTagThrottleLimits> result;
		lastBusyReadTagCount = lastBusyWriteTagCount = 0;

		for (auto& [tag, stats] : tagStatistics) {
			// Currently there is no differentiation between batch priority and default priority transactions
			bool isReadBusy = false, isWriteBusy = false;
			TraceEvent te("GlobalTagThrottler_GotClientRate", id);
			auto const targetTps = getTargetTps(tag, isReadBusy, isWriteBusy, te);

			if (isReadBusy) {
				++lastBusyReadTagCount;
			}
			if (isWriteBusy) {
				++lastBusyWriteTagCount;
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
	uint32_t busyReadTagCount() const { return lastBusyReadTagCount; }
	uint32_t busyWriteTagCount() const { return lastBusyWriteTagCount; }
	int64_t manualThrottleCount() const { return 0; }

	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
		throttlingRatios[ss.id] = ss.getThrottlingRatio(SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
		                                                SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER);
		for (const auto& busyReadTag : ss.busiestReadTags) {
			throughput[ss.id][busyReadTag.tag].updateCost(busyReadTag.rate, OpType::READ);
		}
		for (const auto& busyWriteTag : ss.busiestWriteTags) {
			throughput[ss.id][busyWriteTag.tag].updateCost(busyWriteTag.rate, OpType::WRITE);
		}
		return Void();
	}

	void setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
		tagStatistics[tag].setQuota(tagQuotaValue);
	}

	void removeQuota(TransactionTagRef tag) { tagStatistics[tag].clearQuota(); }
};

GlobalTagThrottler::GlobalTagThrottler(Database db, UID id) : impl(PImpl<GlobalTagThrottlerImpl>::create(db, id)) {}

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
PrioritizedTransactionTagMap<double> GlobalTagThrottler::getProxyRates(int numProxies) {
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

namespace GlobalTagThrottlerTesting {

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
	double targetCost;
	std::map<TransactionTag, Cost> readCosts, writeCosts;
	Cost totalReadCost, totalWriteCost;

public:
	explicit MockStorageServer(UID id, double targetCost) : id(id), targetCost(targetCost) { ASSERT_GT(targetCost, 0); }
	void addReadCost(TransactionTag tag, double cost) {
		readCosts[tag] += cost;
		totalReadCost += cost;
	}
	void addWriteCost(TransactionTag tag, double cost) {
		writeCosts[tag] += cost;
		totalWriteCost += cost;
	}

	StorageQueueInfo getStorageQueueInfo() const {
		StorageQueueInfo result(id, LocalityData{});
		for (const auto& [tag, readCost] : readCosts) {
			double fractionalBusyness{ 0.0 }; // unused for global tag throttling
			result.busiestReadTags.emplace_back(tag, readCost.smoothRate(), fractionalBusyness);
		}
		for (const auto& [tag, writeCost] : writeCosts) {
			double fractionalBusyness{ 0.0 }; // unused for global tag throttling
			result.busiestWriteTags.emplace_back(tag, writeCost.smoothRate(), fractionalBusyness);
		}
		result.lastReply.bytesInput = ((totalReadCost.smoothRate() + totalWriteCost.smoothRate()) / targetCost) *
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
Future<Void> monitor(GlobalTagThrottler* globalTagThrottler, Check check) {
	state int successes = 0;
	loop {
		wait(delay(1.0));
		if (check(*globalTagThrottler)) {
			if (++successes == 3) {
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
	auto it1 = targetRates.find(TransactionPriority::DEFAULT);
	if (it1 != targetRates.end()) {
		auto it2 = it1->second.find(tag);
		if (it2 != it1->second.end()) {
			rate = it2->second;
		}
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

} // namespace GlobalTagThrottlerTesting

TEST_CASE("/GlobalTagThrottler/Simple") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 100.0 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/WriteThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::WRITE);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 100.0 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiTagThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag1, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ));
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag2, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ));
	futures.push_back(GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		    return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag1, 100.0 / 6.0) &&
		           GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag2, 100.0 / 6.0);
	    });
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/AttemptWorkloadAboveQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 20.0, 10.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 10.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> client2 = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 100.0 / 6.0) &&
		       GlobalTagThrottlerTesting::clientRateIsNear(gtt, testTag, 100.0 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || client2 || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiClientThrottling2") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 20.0, 10.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> client2 = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 20.0, 10.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 10.0) &&
		       GlobalTagThrottlerTesting::clientRateIsNear(gtt, testTag, 5.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// Global transaction rate should be 20.0, with a distribution of (5, 15) between the 2 clients
TEST_CASE("/GlobalTagThrottler/SkewedMultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 5.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> client2 = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 25.0, 5.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 20.0) &&
		       GlobalTagThrottlerTesting::clientRateIsNear(gtt, testTag, 15.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
TEST_CASE("/GlobalTagThrottler/UpdateQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	tagQuotaValue.totalReadQuota = 50.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, "sampleTag1"_sr, 50.0 / 6.0);
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/RemoveQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 5.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, "sampleTag1"_sr, 100.0 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	globalTagThrottler.removeQuota(testTag);
	monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, "sampleTag1"_sr, {});
	});
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/ActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 50 / 6.0) && gtt.busyReadTagCount() == 1;
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalReadQuota = tagQuotaValue1.totalWriteQuota = 50.0;
	tagQuotaValue2.totalReadQuota = tagQuotaValue2.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ));
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ));
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		    return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag1, (50 / 6.0) / 3) &&
		           GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag2, 2 * (50 / 6.0) / 3) &&
		           gtt.busyReadTagCount() == 2;
	    });
	futures.push_back(GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling2") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(3, 50);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalReadQuota = tagQuotaValue1.totalWriteQuota = 100.0;
	tagQuotaValue2.totalReadQuota = tagQuotaValue2.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ, { 0, 1 }));
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ, { 1, 2 }));
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		    return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag1, 50 / 6.0) &&
		           GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag2, 50 / 6.0) && gtt.busyReadTagCount() == 2;
	    });
	futures.push_back(GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiTagActiveThrottling3") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(3, 50);
	state ThrottleApi::TagQuotaValue tagQuotaValue1;
	state ThrottleApi::TagQuotaValue tagQuotaValue2;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue1.totalReadQuota = tagQuotaValue1.totalWriteQuota = 100.0;
	tagQuotaValue2.totalReadQuota = tagQuotaValue2.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue1);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue2);
	std::vector<Future<Void>> futures;
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag1, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ, { 0 }));
	futures.push_back(GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag2, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ, { 1, 2 }));
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag1, testTag2](auto& gtt) {
		    return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag1, 50 / 6.0) &&
		           GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag2, 100 / 6.0) && gtt.busyReadTagCount() == 1;
	    });
	futures.push_back(GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	wait(timeoutError(waitForAny(futures) || monitor, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/ReservedReadQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	tagQuotaValue.reservedReadQuota = 70.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::READ);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 70 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/ReservedWriteQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = tagQuotaValue.totalWriteQuota = 100.0;
	tagQuotaValue.reservedWriteQuota = 70.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = GlobalTagThrottlerTesting::runClient(
	    &globalTagThrottler, &storageServers, testTag, 10.0, 6.0, GlobalTagThrottlerTesting::OpType::WRITE);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitor(&globalTagThrottler, [testTag](auto& gtt) {
		return GlobalTagThrottlerTesting::targetRateIsNear(gtt, testTag, 70 / 6.0);
	});
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 600.0));
	return Void();
}
