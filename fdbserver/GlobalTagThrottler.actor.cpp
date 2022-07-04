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

class GlobalTagThrottlerImpl {

	enum class LimitType { RESERVED, TOTAL };
	enum class OpType { READ, WRITE };

	class QuotaAndCounters {
		Optional<ThrottleApi::TagQuotaValue> quota;
		std::unordered_map<UID, double> ssToReadCostRate;
		std::unordered_map<UID, double> ssToWriteCostRate;
		Smoother totalReadCostRate;
		Smoother totalWriteCostRate;
		Smoother transactionCounter;
		Smoother perClientRate;

		Optional<double> getReadTPSLimit(Optional<double> maxDesiredCost) const {
			if (totalReadCostRate.smoothTotal() > 0) {
				auto const desiredReadCost = maxDesiredCost.present() ? std::min(maxDesiredCost.get(), quota.get().totalReadQuota) : quota.get().totalReadQuota;
				auto const averageCostPerTransaction =
				    totalReadCostRate.smoothTotal() / transactionCounter.smoothRate();
				return desiredReadCost / averageCostPerTransaction;
			} else {
				return {};
			}
		}

		Optional<double> getWriteTPSLimit(Optional<double> maxDesiredCost) const {
			if (totalWriteCostRate.smoothTotal() > 0) {
				auto const desiredWriteCost = maxDesiredCost.present() ? std::min(maxDesiredCost.get(), quota.get().totalWriteQuota) : quota.get().totalWriteQuota;
				auto const averageCostPerTransaction = transactionCounter.smoothRate() / totalWriteCostRate.smoothTotal();
				return desiredWriteCost * averageCostPerTransaction;
			} else {
				return {};
			}
		}

	public:
		QuotaAndCounters()
		  : totalReadCostRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    totalWriteCostRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    perClientRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME) {}

		void setQuota(ThrottleApi::TagQuotaValue const& quota) { this->quota = quota; }

		Optional<ThrottleApi::TagQuotaValue> const &getQuota() const {
			return quota;
		}

		double getReadCostRate() const {
			return totalReadCostRate.smoothTotal();
		}

		double getWriteCostRate() const {
			return totalWriteCostRate.smoothTotal();
		}

		void updateReadCostRate(UID ssId, double newReadCostRate) {
			auto& currentReadCostRate = ssToReadCostRate[ssId];
			auto diff = newReadCostRate - currentReadCostRate;
			currentReadCostRate += diff;
			totalReadCostRate.addDelta(diff);
		}

		void updateWriteCostRate(UID ssId, double newWriteCostRate) {
			auto& currentWriteCostRate = ssToWriteCostRate[ssId];
			auto diff = newWriteCostRate - currentWriteCostRate;
			currentWriteCostRate += diff;
			totalWriteCostRate.addDelta(diff);
		}

		void addTransactions(int count) { transactionCounter.addDelta(count); }

		Optional<double> getTargetTotalTPSLimit(Optional<double> maxReadCostRate, Optional<double> maxWriteCostRate) const {
			if (!quota.present())
				return {};
			auto readLimit = getReadTPSLimit(maxReadCostRate);
			auto writeLimit = getWriteTPSLimit(maxWriteCostRate);

			if (!readLimit.present() && !writeLimit.present()) {
				return {};
			} else {
				if (!readLimit.present()) {
					return writeLimit.get();
				} else if (!writeLimit.present()) {
					return readLimit.get();
				} else {
					return std::min(readLimit.get(), writeLimit.get());
				}
			}
		}

		Optional<ClientTagThrottleLimits> updateAndGetPerClientLimit(Optional<double> maxReadCostRate, Optional<double> maxWriteCostRate) {
			auto targetRate = getTargetTotalTPSLimit(maxReadCostRate, maxWriteCostRate);
			if (targetRate.present() && transactionCounter.smoothRate() > 0) {
				auto newPerClientRate = std::max(
				    SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE,
				    std::min(targetRate.get(),
				             (targetRate.get() / transactionCounter.smoothRate()) * perClientRate.smoothTotal()));
				perClientRate.setTotal(newPerClientRate);
				return ClientTagThrottleLimits(perClientRate.getTotal(), ClientTagThrottleLimits::NO_EXPIRATION);
			} else {
				return {};
			}
		}

		void processTraceEvent(TraceEvent& te) const {
			if (quota.present()) {
				te.detail("ProvidedReadTPSLimit", getReadTPSLimit({}))
				    .detail("ProvidedWriteTPSLimit", getWriteTPSLimit({}))
				    .detail("ReadCostRate", totalReadCostRate.smoothTotal())
				    .detail("WriteCostRate", totalWriteCostRate.smoothTotal())
				    .detail("TotalReadQuota", quota.get().totalReadQuota)
				    .detail("ReservedReadQuota", quota.get().reservedReadQuota)
				    .detail("TotalWriteQuota", quota.get().totalWriteQuota)
				    .detail("ReservedWriteQuota", quota.get().reservedWriteQuota);
			}
		}
	};

	Database db;
	UID id;
	std::map<TransactionTag, QuotaAndCounters> trackedTags;
	uint64_t throttledTagChangeId{ 0 };
	Future<Void> traceActor;
	Optional<double> throttlingRatio;

	double getQuotaRatio(TransactionTagRef tag, OpType opType, LimitType limitType) const {
		int64_t sumQuota{ 0 };
		int64_t tagQuota{ 0 };
		for (const auto &[tag2, quotaAndCounters] : trackedTags) {
			if (!quotaAndCounters.getQuota().present()) {
				continue;
			}
			int64_t quota{ 0 };
			if (opType == OpType::READ) {
				if (limitType == LimitType::RESERVED) {
					quota = quotaAndCounters.getQuota().get().reservedReadQuota;
				} else {
					quota = quotaAndCounters.getQuota().get().totalReadQuota;
				}
			} else {
				if (limitType == LimitType::RESERVED) {
					quota = quotaAndCounters.getQuota().get().reservedWriteQuota;
				} else {
					quota = quotaAndCounters.getQuota().get().totalWriteQuota;
				}
			}
			sumQuota += quota;
			if (tag == tag2) {
				tagQuota = quota;
			}
		}
		if (tagQuota == 0) return 0;
		ASSERT_GT(sumQuota, 0);
		return tagQuota / sumQuota;
	}

	// Returns the total cost rate (summed across all tags)
	double getTotalCostRate(OpType opType) const {
		double result{ 0 };
		for (const auto &[tag, quotaAndCounters] : trackedTags) {
			result +=
			    (opType == OpType::READ) ? quotaAndCounters.getReadCostRate() : quotaAndCounters.getWriteCostRate();
		}
		return result;
	}

	ACTOR static Future<Void> tracer(GlobalTagThrottlerImpl const* self) {
		loop {
			for (const auto& [tag, quotaAndCounters] : self->trackedTags) {
				TraceEvent te("GlobalTagThrottling");
				te.detail("Tag", tag);
				quotaAndCounters.processTraceEvent(te);
			}
			wait(delay(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_TRACE_INTERVAL));
		}
	}

	void removeUnseenTags(std::unordered_set<TransactionTag> const& seenTags) {
		std::map<TransactionTag, QuotaAndCounters>::iterator it = trackedTags.begin();
		while (it != trackedTags.end()) {
			auto current = it++;
			auto const tag = current->first;
			if (seenTags.find(tag) == seenTags.end()) {
				trackedTags.erase(current);
			}
		}
	}

	ACTOR static Future<Void> monitorThrottlingChanges(GlobalTagThrottlerImpl* self) {
		state std::unordered_set<TransactionTag> seenTags;

		loop {
			state ReadYourWritesTransaction tr(self->db);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					seenTags.clear();
					state RangeResult currentQuotas = wait(tr.getRange(tagQuotaKeys, CLIENT_KNOBS->TOO_MANY));
					TraceEvent("GlobalTagThrottler_ReadCurrentQuotas").detail("Size", currentQuotas.size());
					for (auto const kv : currentQuotas) {
						auto const tag = kv.key.removePrefix(tagQuotaPrefix);
						auto const quota = ThrottleApi::TagQuotaValue::fromValue(kv.value);
						self->trackedTags[tag].setQuota(quota);
						seenTags.insert(tag);
					}
					self->removeUnseenTags(seenTags);
					++self->throttledTagChangeId;
					wait(delay(5.0));
					TraceEvent("GlobalTagThrottler_ChangeSignaled");
					CODE_PROBE(true, "Global tag throttler detected quota changes");
					break;
				} catch (Error& e) {
					TraceEvent("GlobalTagThrottlerMonitoringChangesError", self->id).error(e);
					wait(tr.onError(e));
				}
			}
		}
	}

	Optional<double> getMaxCostRate(TransactionTagRef tag, OpType opType, LimitType limitType) const {
		if (throttlingRatio.present()) {
			auto const desiredTotalCost = throttlingRatio.get() * getTotalCostRate(opType);
			return desiredTotalCost * getQuotaRatio(tag, opType, limitType);
		} else {
			return {};
		}
	}

public:
	GlobalTagThrottlerImpl(Database db, UID id) : db(db), id(id) { traceActor = tracer(this); }
	Future<Void> monitorThrottlingChanges() { return monitorThrottlingChanges(this); }
	void addRequests(TransactionTag tag, int count) { trackedTags[tag].addTransactions(count); }
	uint64_t getThrottledTagChangeId() const { return throttledTagChangeId; }
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() {
		// TODO: For now, only enforce total throttling rates.
		// We should use reserved quotas as well.
		PrioritizedTransactionTagMap<ClientTagThrottleLimits> result;
		for (auto& [tag, quotaAndCounters] : trackedTags) {
			// Currently there is no differentiation between batch priority and default priority transactions
			auto const limit =
			    quotaAndCounters.updateAndGetPerClientLimit(getMaxCostRate(tag, OpType::READ, LimitType::TOTAL),
			                                                getMaxCostRate(tag, OpType::WRITE, LimitType::TOTAL));
			if (limit.present()) {
				result[TransactionPriority::BATCH][tag] = result[TransactionPriority::DEFAULT][tag] = limit.get();
			}
		}
		return result;
	}
	int64_t autoThrottleCount() const { return trackedTags.size(); }
	uint32_t busyReadTagCount() const {
		// TODO: Implement
		return 0;
	}
	uint32_t busyWriteTagCount() const {
		// TODO: Implement
		return 0;
	}
	int64_t manualThrottleCount() const { return trackedTags.size(); }

	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const& ss) {
		for (const auto& busyReadTag : ss.busiestReadTags) {
			trackedTags[busyReadTag.tag].updateReadCostRate(ss.id, busyReadTag.rate);
		}
		for (const auto& busyWriteTag : ss.busiestWriteTags) {
			trackedTags[busyWriteTag.tag].updateWriteCostRate(ss.id, busyWriteTag.rate);
		}
		return Void();
	}

	void setThrottlingRatio(Optional<double> ratio) {
		throttlingRatio = ratio;
	}

	void setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
		trackedTags[tag].setQuota(tagQuotaValue);
	}

	void removeQuota(TransactionTagRef tag) { trackedTags.erase(tag); }
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
void GlobalTagThrottler::setThrottlingRatio(Optional<double> ratio) {
	return impl->setThrottlingRatio(ratio);
}

void GlobalTagThrottler::setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
	return impl->setQuota(tag, tagQuotaValue);
}

void GlobalTagThrottler::removeQuota(TransactionTagRef tag) {
	return impl->removeQuota(tag);
}

namespace GlobalTagThrottlerTesting {

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
	double targetCostRate;
	std::map<TransactionTag, Cost> readCosts, writeCosts;
	Cost totalReadCost, totalWriteCost;

public:
	explicit MockStorageServer(UID id, double targetCostRate) : id(id), targetCostRate(targetCostRate) {
		ASSERT_GT(targetCostRate, 0);
	}
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
		return result;
	}

	Optional<double> getThrottlingRatio() const {
		auto const springCostRate = 0.2 * targetCostRate;
		auto const currentCostRate = totalReadCost.smoothRate() + totalWriteCost.smoothRate();
		if (currentCostRate < targetCostRate - springCostRate) {
			return {};
		} else {
			return std::max(0.0, ((targetCostRate + springCostRate) - currentCostRate) / springCostRate);
		}
	}
};

class StorageServerCollection {
	std::vector<MockStorageServer> storageServers;

public:
	StorageServerCollection(size_t size, double targetCostRate) {
		ASSERT_GT(size, 0);
		storageServers.reserve(size);
		for (int i = 0; i < size; ++i) {
			storageServers.emplace_back(UID(i, i), targetCostRate);
		}
	}

	void addReadCost(TransactionTag tag, double cost) {
		auto const costPerSS = cost / storageServers.size();
		for (auto& storageServer : storageServers) {
			storageServer.addReadCost(tag, costPerSS);
		}
	}

	void addWriteCost(TransactionTag tag, double cost) {
		auto const costPerSS = cost / storageServers.size();
		for (auto& storageServer : storageServers) {
			storageServer.addWriteCost(tag, costPerSS);
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

	Optional<double> getWorstThrottlingRatio() const {
		Optional<double> result;
		for (const auto& storageServer : storageServers) {
			auto const throttlingRatio = storageServer.getThrottlingRatio();
			if (result.present() && throttlingRatio.present()) {
				result = std::max(result.get(), throttlingRatio.get());
			} else if (throttlingRatio.present()) {
				result = throttlingRatio.get();
			}
		}
		return result;
	}
};

ACTOR static Future<Void> runClient(GlobalTagThrottler* globalTagThrottler,
                                    StorageServerCollection* storageServers,
                                    TransactionTag tag,
                                    double desiredTpsRate,
                                    double costPerTransaction,
                                    bool write) {
	loop {
		auto tpsLimit = getTPSLimit(*globalTagThrottler, tag);
		state double tpsRate = tpsLimit.present() ? std::min<double>(desiredTpsRate, tpsLimit.get()) : desiredTpsRate;
		wait(delay(1 / tpsRate));
		if (write) {
			storageServers->addWriteCost(tag, costPerTransaction);
		} else {
			storageServers->addReadCost(tag, costPerTransaction);
		}
		globalTagThrottler->addRequests(tag, 1);
	}
}

ACTOR static Future<Void> monitorClientRates(GlobalTagThrottler* globalTagThrottler,
                                             TransactionTag tag,
                                             Optional<double> desiredTPSLimit) {
	state int successes = 0;
	loop {
		wait(delay(1.0));
		auto currentTPSLimit = getTPSLimit(*globalTagThrottler, tag);
		if (currentTPSLimit.present()) {
			TraceEvent("GlobalTagThrottling_RateMonitor")
			    .detail("Tag", tag)
			    .detail("CurrentTPSRate", currentTPSLimit.get())
			    .detail("DesiredTPSRate", desiredTPSLimit);
			if (desiredTPSLimit.present() && abs(currentTPSLimit.get() - desiredTPSLimit.get()) < 1.0) {
				if (++successes == 3) {
					return Void();
				}
			} else {
				successes = 0;
			}
		} else {
			TraceEvent("GlobalTagThrottling_RateMonitor")
			    .detail("Tag", tag)
			    .detail("CurrentTPSRate", currentTPSLimit)
			    .detail("DesiredTPSRate", desiredTPSLimit);
			if (desiredTPSLimit.present()) {
				successes = 0;
			} else {
				if (++successes == 3) {
					return Void();
				}
			}
		}
	}
}

ACTOR static Future<Void> updateGlobalTagThrottler(GlobalTagThrottler* globalTagThrottler,
                                                   StorageServerCollection const* storageServers) {
	loop {
		wait(delay(1.0));
		auto const storageQueueInfos = storageServers->getStorageQueueInfos();
		for (const auto& sq : storageQueueInfos) {
			globalTagThrottler->tryUpdateAutoThrottling(sq);
		}
		globalTagThrottler->setThrottlingRatio(storageServers->getWorstThrottlingRatio());
	}
}

} // namespace GlobalTagThrottlerTesting

TEST_CASE("/GlobalTagThrottler/Simple") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, false);
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 100.0 / 6.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/WriteThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalWriteQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, true);
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 100.0 / 6.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiTagThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag1 = "sampleTag1"_sr;
	TransactionTag testTag2 = "sampleTag2"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag1, tagQuotaValue);
	globalTagThrottler.setQuota(testTag2, tagQuotaValue);
	state std::vector<Future<Void>> futures;
	state std::vector<Future<Void>> monitorFutures;
	futures.push_back(
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag1, 5.0, 6.0, false));
	futures.push_back(
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag2, 5.0, 6.0, false));
	futures.push_back(GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers));
	monitorFutures.push_back(GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag1, 100.0 / 6.0));
	monitorFutures.push_back(GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag2, 100.0 / 6.0));
	wait(timeoutError(waitForAny(futures) || waitForAll(monitorFutures), 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/AttemptWorkloadAboveQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, false);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 10.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiClientThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, false);
	state Future<Void> client2 =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, false);
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 100.0 / 6.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/MultiClientActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, false);
	state Future<Void> client2 =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 20.0, 10.0, false);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 5.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

// Global transaction rate should be 20.0, with a distribution of (5, 15) between the 2 clients
TEST_CASE("/GlobalTagThrottler/SkewedMultiClientActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 5.0, false);
	state Future<Void> client2 =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 25.0, 5.0, false);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 15.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

// Test that the tag throttler can reach equilibrium, then adjust to a new equilibrium once the quota is changed
TEST_CASE("/GlobalTagThrottler/UpdateQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, false);
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 100.0 / 6.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	tagQuotaValue.totalReadQuota = 50.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	monitor = GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 50.0 / 6.0);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/RemoveQuota") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 100);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 5.0, 6.0, false);
	state Future<Void> monitor =
	    GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 100.0 / 6.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	globalTagThrottler.removeQuota(testTag);
	monitor = GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, {});
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/ActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state GlobalTagThrottlerTesting::StorageServerCollection storageServers(10, 5);
	state ThrottleApi::TagQuotaValue tagQuotaValue;
	state TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client =
	    GlobalTagThrottlerTesting::runClient(&globalTagThrottler, &storageServers, testTag, 10.0, 6.0, false);
	state Future<Void> monitor = GlobalTagThrottlerTesting::monitorClientRates(&globalTagThrottler, testTag, 50 / 6.0);
	state Future<Void> updater =
	    GlobalTagThrottlerTesting::updateGlobalTagThrottler(&globalTagThrottler, &storageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}
