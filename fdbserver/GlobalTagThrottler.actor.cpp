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
	class QuotaAndCounters {
		Optional<ThrottleApi::TagQuotaValue> quota;
		std::unordered_map<UID, double> ssToReadCostRate;
		std::unordered_map<UID, double> ssToWriteCostRate;
		Smoother totalReadCostRate;
		Smoother totalWriteCostRate;
		Smoother transactionCounter;

		Optional<double> getReadTPSLimit() const {
			if (totalReadCostRate.smoothTotal() > 0) {
				return quota.get().totalReadQuota * transactionCounter.smoothRate() / totalReadCostRate.smoothTotal();
			} else {
				return {};
			}
		}

		Optional<double> getWriteTPSLimit() const {
			if (totalWriteCostRate.smoothTotal() > 0) {
				return quota.get().totalWriteQuota * transactionCounter.smoothRate() / totalWriteCostRate.smoothTotal();
			} else {
				return {};
			}
		}

	public:
		QuotaAndCounters()
		  : totalReadCostRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    totalWriteCostRate(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME),
		    transactionCounter(SERVER_KNOBS->GLOBAL_TAG_THROTTLING_FOLDING_TIME) {}

		void setQuota(ThrottleApi::TagQuotaValue const& quota) { this->quota = quota; }

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

		Optional<ClientTagThrottleLimits> getTotalLimit() const {
			if (!quota.present())
				return {};
			auto readLimit = getReadTPSLimit();
			auto writeLimit = getWriteTPSLimit();

			// TODO: Implement expiration logic
			if (!readLimit.present() && !writeLimit.present()) {
				return {};
			} else {
				auto totalLimit = SERVER_KNOBS->GLOBAL_TAG_THROTTLING_MIN_RATE;
				if (!readLimit.present()) {
					totalLimit = std::max(totalLimit, writeLimit.get());
				} else if (!writeLimit.present()) {
					totalLimit = std::max(totalLimit, readLimit.get());
				} else {
					totalLimit = std::max(totalLimit, std::min(readLimit.get(), writeLimit.get()));
				}
				return ClientTagThrottleLimits(totalLimit, ClientTagThrottleLimits::NO_EXPIRATION);
			}
		}

		void processTraceEvent(TraceEvent& te) const {
			if (quota.present()) {
				te.detail("ProvidedReadTPSLimit", getReadTPSLimit())
				    .detail("ProvidedWriteTPSLimit", getWriteTPSLimit())
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

	ACTOR static Future<Void> monitorThrottlingChanges(GlobalTagThrottlerImpl* self) {
		loop {
			state ReadYourWritesTransaction tr(self->db);

			loop {
				// TODO: Clean up quotas that have been removed
				try {
					tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					state RangeResult currentQuotas = wait(tr.getRange(tagQuotaKeys, CLIENT_KNOBS->TOO_MANY));
					TraceEvent("GlobalTagThrottler_ReadCurrentQuotas").detail("Size", currentQuotas.size());
					for (auto const kv : currentQuotas) {
						auto const tag = kv.key.removePrefix(tagQuotaPrefix);
						auto const quota = ThrottleApi::TagQuotaValue::fromValue(kv.value);
						self->trackedTags[tag].setQuota(quota);
					}

					++self->throttledTagChangeId;
					// FIXME: Should wait on watch instead
					// wait(tr.watch(tagThrottleSignalKey));
					wait(delay(5.0));
					TraceEvent("GlobalTagThrottler_ChangeSignaled");
					TEST(true); // Global tag throttler detected quota changes
					break;
				} catch (Error& e) {
					TraceEvent("GlobalTagThrottlerMonitoringChangesError", self->id).error(e);
					wait(tr.onError(e));
				}
			}
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
		for (const auto& [tag, quotaAndCounters] : trackedTags) {
			// Currently there is no differentiation between batch priority and default priority transactions
			auto const limit = quotaAndCounters.getTotalLimit();
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
		// TODO: Call ThrottleApi::throttleTags
		return Void();
	}

	void setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
		trackedTags[tag].setQuota(tagQuotaValue);
	}
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

void GlobalTagThrottler::setQuota(TransactionTagRef tag, ThrottleApi::TagQuotaValue const& tagQuotaValue) {
	return impl->setQuota(tag, tagQuotaValue);
}

Optional<double> testGetTPSLimit(GlobalTagThrottler& globalTagThrottler, TransactionTag tag) {
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

class TestStorageServers {
	class ReadCost {
		Smoother smoother;

	public:
		ReadCost() : smoother(5.0) {}
		ReadCost& operator+=(double delta) {
			smoother.addDelta(delta);
			return *this;
		}
		double smoothRate() const { return smoother.smoothRate(); }
	};

	std::vector<std::map<TransactionTag, ReadCost>> readCosts;

public:
	TestStorageServers(size_t size) : readCosts(size) { ASSERT_GT(size, 0); }

	void addReadCost(TransactionTag tag, double cost) {
		auto const costPerSS = cost / readCosts.size();
		for (auto& readCost : readCosts) {
			readCost[tag] += costPerSS;
		}
	}

	std::vector<StorageQueueInfo> getStorageQueueInfos() const {
		std::vector<StorageQueueInfo> result;
		result.reserve(readCosts.size());
		for (int i = 0; i < readCosts.size(); ++i) {
			StorageQueueInfo sqInfo(UID(i, i), LocalityData{});
			for (const auto& [tag, readCost] : readCosts[i]) {
				double fractionalBusyness{ 0.0 }; // unused for global tag throttling
				sqInfo.busiestReadTags.emplace_back(tag, readCost.smoothRate(), fractionalBusyness);
			}
			result.push_back(sqInfo);
		}
		return result;
	}
};

ACTOR static Future<Void> testClient(GlobalTagThrottler* globalTagThrottler,
                                     TestStorageServers* testStorageServers,
                                     TransactionTag tag,
                                     double desiredTpsRate,
                                     double costPerTransaction) {
	loop {
		auto tpsLimit = testGetTPSLimit(*globalTagThrottler, tag);
		state double tpsRate = tpsLimit.present() ? std::min<double>(desiredTpsRate, tpsLimit.get()) : desiredTpsRate;
		wait(delay(1 / tpsRate));
		testStorageServers->addReadCost(tag, costPerTransaction);
		globalTagThrottler->addRequests(tag, 1);
	}
}

ACTOR static Future<Void> monitorClientRates(GlobalTagThrottler* globalTagThrottler,
                                             TransactionTag tag,
                                             double desiredTPSLimit) {
	state int successes = 0;
	loop {
		wait(delay(1.0));
		auto currentTPSLimit = testGetTPSLimit(*globalTagThrottler, tag);
		if (currentTPSLimit.present()) {
			TraceEvent("GlobalTagThrottling_RateMonitor")
			    .detail("Tag", tag)
			    .detail("CurrentTPSRate", currentTPSLimit.get())
			    .detail("DesiredTPSRate", desiredTPSLimit);
			if (abs(currentTPSLimit.get() - desiredTPSLimit) < 0.1) {
				if (++successes == 3) {
					return Void();
				}
			} else {
				successes = 0;
			}
		} else {
			successes = 0;
		}
	}
}

ACTOR static Future<Void> updateGlobalTagThrottler(GlobalTagThrottler* globalTagThrottler,
                                                   TestStorageServers const* testStorageServers) {
	loop {
		wait(delay(1.0));
		auto const storageQueueInfos = testStorageServers->getStorageQueueInfos();
		for (const auto& sq : storageQueueInfos) {
			globalTagThrottler->tryUpdateAutoThrottling(sq);
		}
	}
}

TEST_CASE("/GlobalTagThrottler/NoActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state TestStorageServers testStorageServers(10);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = testClient(&globalTagThrottler, &testStorageServers, testTag, 5.0, 6.0);
	state Future<Void> monitor = monitorClientRates(&globalTagThrottler, testTag, 100.0 / 6.0);
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &testStorageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}

TEST_CASE("/GlobalTagThrottler/ActiveThrottling") {
	state GlobalTagThrottler globalTagThrottler(Database{}, UID{});
	state TestStorageServers testStorageServers(10);
	ThrottleApi::TagQuotaValue tagQuotaValue;
	TransactionTag testTag = "sampleTag1"_sr;
	tagQuotaValue.totalReadQuota = 100.0;
	globalTagThrottler.setQuota(testTag, tagQuotaValue);
	state Future<Void> client = testClient(&globalTagThrottler, &testStorageServers, testTag, 20.0, 10.0);
	state Future<Void> monitor = monitorClientRates(&globalTagThrottler, testTag, 10.0);
	state Future<Void> updater = updateGlobalTagThrottler(&globalTagThrottler, &testStorageServers);
	wait(timeoutError(monitor || client || updater, 300.0));
	return Void();
}
