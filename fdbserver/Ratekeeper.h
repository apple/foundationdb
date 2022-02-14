/*
 * RatekeeperData.h
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

// TODO: Trim down these includes
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/IndexedSet.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Smoother.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"

#pragma once

enum limitReason_t {
	unlimited, // TODO: rename to workload?
	storage_server_write_queue_size, // 1
	storage_server_write_bandwidth_mvcc,
	storage_server_readable_behind,
	log_server_mvcc_write_bandwidth,
	log_server_write_queue, // 5
	storage_server_min_free_space, // a storage server's normal limits are being reduced by low free space
	storage_server_min_free_space_ratio, // a storage server's normal limits are being reduced by a low free space ratio
	log_server_min_free_space,
	log_server_min_free_space_ratio,
	storage_server_durability_lag, // 10
	storage_server_list_fetch_failed,
	limitReason_t_end
};

struct StorageQueueInfo {
	bool valid;
	UID id;
	LocalityData locality;
	StorageQueuingMetricsReply lastReply;
	StorageQueuingMetricsReply prevReply;
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothDurableVersion, smoothLatestVersion;
	Smoother smoothFreeSpace;
	Smoother smoothTotalSpace;
	limitReason_t limitReason;

	Optional<TransactionTag> busiestReadTag, busiestWriteTag;
	double busiestReadTagFractionalBusyness = 0, busiestWriteTagFractionalBusyness = 0;
	double busiestReadTagRate = 0, busiestWriteTagRate = 0;

	Reference<EventCacheHolder> busiestWriteTagEventHolder;

	// refresh periodically
	TransactionTagMap<TransactionCommitCostEstimation> tagCostEst;
	uint64_t totalWriteCosts = 0;
	int totalWriteOps = 0;

	StorageQueueInfo(UID id, LocalityData locality)
	  : valid(false), id(id), locality(locality), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
	    smoothDurableVersion(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothLatestVersion(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    limitReason(limitReason_t::unlimited),
	    busiestWriteTagEventHolder(makeReference<EventCacheHolder>(id.toString() + "/BusiestWriteTag")) {
		// FIXME: this is a tacky workaround for a potential uninitialized use in trackStorageServerQueueInfo
		lastReply.instanceID = -1;
	}
};

struct TLogQueueInfo {
	bool valid;
	UID id;
	TLogQueuingMetricsReply lastReply;
	TLogQueuingMetricsReply prevReply;
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothFreeSpace;
	Smoother smoothTotalSpace;
	TLogQueueInfo(UID id)
	  : valid(false), id(id), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
	    smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT) {
		// FIXME: this is a tacky workaround for a potential uninitialized use in trackTLogQueueInfo (copied from
		// storageQueueInfO)
		lastReply.instanceID = -1;
	}
};

class RkTagThrottleCollection : NonCopyable {
private:
	struct RkTagData {
		Smoother requestRate;
		RkTagData() : requestRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {}
	};

	struct RkTagThrottleData {
		ClientTagThrottleLimits limits;
		Smoother clientRate;

		// Only used by auto-throttles
		double created = now();
		double lastUpdated = 0;
		double lastReduced = now();
		bool rateSet = false;

		RkTagThrottleData() : clientRate(CLIENT_KNOBS->TAG_THROTTLE_SMOOTHING_WINDOW) {}

		double getTargetRate(Optional<double> requestRate) {
			if (limits.tpsRate == 0.0 || !requestRate.present() || requestRate.get() == 0.0 || !rateSet) {
				return limits.tpsRate;
			} else {
				return std::min(limits.tpsRate, (limits.tpsRate / requestRate.get()) * clientRate.smoothTotal());
			}
		}

		Optional<double> updateAndGetClientRate(Optional<double> requestRate) {
			if (limits.expiration > now()) {
				double targetRate = getTargetRate(requestRate);
				if (targetRate == std::numeric_limits<double>::max()) {
					rateSet = false;
					return targetRate;
				}
				if (!rateSet) {
					rateSet = true;
					clientRate.reset(targetRate);
				} else {
					clientRate.setTotal(targetRate);
				}

				double rate = clientRate.smoothTotal();
				ASSERT(rate >= 0);
				return rate;
			} else {
				TEST(true); // Get throttle rate for expired throttle
				rateSet = false;
				return Optional<double>();
			}
		}
	};

	void initializeTag(TransactionTag const& tag) { tagData.try_emplace(tag); }

public:
	RkTagThrottleCollection() {}

	RkTagThrottleCollection(RkTagThrottleCollection&& other) {
		autoThrottledTags = std::move(other.autoThrottledTags);
		manualThrottledTags = std::move(other.manualThrottledTags);
		tagData = std::move(other.tagData);
	}

	void operator=(RkTagThrottleCollection&& other) {
		autoThrottledTags = std::move(other.autoThrottledTags);
		manualThrottledTags = std::move(other.manualThrottledTags);
		tagData = std::move(other.tagData);
	}

	double computeTargetTpsRate(double currentBusyness, double targetBusyness, double requestRate) {
		ASSERT(currentBusyness > 0);

		if (targetBusyness < 1) {
			double targetFraction = targetBusyness * (1 - currentBusyness) / ((1 - targetBusyness) * currentBusyness);
			return requestRate * targetFraction;
		} else {
			return std::numeric_limits<double>::max();
		}
	}

	// Returns the TPS rate if the throttle is updated, otherwise returns an empty optional
	Optional<double> autoThrottleTag(UID id,
	                                 TransactionTag const& tag,
	                                 double fractionalBusyness,
	                                 Optional<double> tpsRate = Optional<double>(),
	                                 Optional<double> expiration = Optional<double>()) {
		ASSERT(!tpsRate.present() || tpsRate.get() >= 0);
		ASSERT(!expiration.present() || expiration.get() > now());

		auto itr = autoThrottledTags.find(tag);
		bool present = (itr != autoThrottledTags.end());
		if (!present) {
			if (autoThrottledTags.size() >= SERVER_KNOBS->MAX_AUTO_THROTTLED_TRANSACTION_TAGS) {
				TEST(true); // Reached auto-throttle limit
				return Optional<double>();
			}

			itr = autoThrottledTags.try_emplace(tag).first;
			initializeTag(tag);
		} else if (itr->second.limits.expiration <= now()) {
			TEST(true); // Re-throttling expired tag that hasn't been cleaned up
			present = false;
			itr->second = RkTagThrottleData();
		}

		auto& throttle = itr->second;

		if (!tpsRate.present()) {
			if (now() <= throttle.created + SERVER_KNOBS->AUTO_TAG_THROTTLE_START_AGGREGATION_TIME) {
				tpsRate = std::numeric_limits<double>::max();
				if (present) {
					return Optional<double>();
				}
			} else if (now() <= throttle.lastUpdated + SERVER_KNOBS->AUTO_TAG_THROTTLE_UPDATE_FREQUENCY) {
				TEST(true); // Tag auto-throttled too quickly
				return Optional<double>();
			} else {
				tpsRate = computeTargetTpsRate(fractionalBusyness,
				                               SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS,
				                               tagData[tag].requestRate.smoothRate());

				if (throttle.limits.expiration > now() && tpsRate.get() >= throttle.limits.tpsRate) {
					TEST(true); // Tag auto-throttle rate increase attempt while active
					return Optional<double>();
				}

				throttle.lastUpdated = now();
				if (tpsRate.get() < throttle.limits.tpsRate) {
					throttle.lastReduced = now();
				}
			}
		}
		if (!expiration.present()) {
			expiration = now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION;
		}

		ASSERT(tpsRate.present() && tpsRate.get() >= 0);

		throttle.limits.tpsRate = tpsRate.get();
		throttle.limits.expiration = expiration.get();

		Optional<double> clientRate = throttle.updateAndGetClientRate(getRequestRate(tag));

		TraceEvent("RkSetAutoThrottle", id)
		    .detail("Tag", tag)
		    .detail("TargetRate", tpsRate.get())
		    .detail("Expiration", expiration.get() - now())
		    .detail("ClientRate", clientRate)
		    .detail("Created", now() - throttle.created)
		    .detail("LastUpdate", now() - throttle.lastUpdated)
		    .detail("LastReduced", now() - throttle.lastReduced);

		if (tpsRate.get() != std::numeric_limits<double>::max()) {
			return tpsRate.get();
		} else {
			return Optional<double>();
		}
	}

	void manualThrottleTag(UID id,
	                       TransactionTag const& tag,
	                       TransactionPriority priority,
	                       double tpsRate,
	                       double expiration,
	                       Optional<ClientTagThrottleLimits> const& oldLimits) {
		ASSERT(tpsRate >= 0);
		ASSERT(expiration > now());

		auto& priorityThrottleMap = manualThrottledTags[tag];
		auto result = priorityThrottleMap.try_emplace(priority);
		initializeTag(tag);
		ASSERT(result.second); // Updating to the map is done by copying the whole map

		result.first->second.limits.tpsRate = tpsRate;
		result.first->second.limits.expiration = expiration;

		if (!oldLimits.present()) {
			TEST(true); // Transaction tag manually throttled
			TraceEvent("RatekeeperAddingManualThrottle", id)
			    .detail("Tag", tag)
			    .detail("Rate", tpsRate)
			    .detail("Priority", transactionPriorityToString(priority))
			    .detail("SecondsToExpiration", expiration - now());
		} else if (oldLimits.get().tpsRate != tpsRate || oldLimits.get().expiration != expiration) {
			TEST(true); // Manual transaction tag throttle updated
			TraceEvent("RatekeeperUpdatingManualThrottle", id)
			    .detail("Tag", tag)
			    .detail("Rate", tpsRate)
			    .detail("Priority", transactionPriorityToString(priority))
			    .detail("SecondsToExpiration", expiration - now());
		}

		Optional<double> clientRate = result.first->second.updateAndGetClientRate(getRequestRate(tag));
		ASSERT(clientRate.present());
	}

	Optional<ClientTagThrottleLimits> getManualTagThrottleLimits(TransactionTag const& tag,
	                                                             TransactionPriority priority) {
		auto itr = manualThrottledTags.find(tag);
		if (itr != manualThrottledTags.end()) {
			auto priorityItr = itr->second.find(priority);
			if (priorityItr != itr->second.end()) {
				return priorityItr->second.limits;
			}
		}

		return Optional<ClientTagThrottleLimits>();
	}

	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates(bool autoThrottlingEnabled) {
		PrioritizedTransactionTagMap<ClientTagThrottleLimits> clientRates;

		for (auto tagItr = tagData.begin(); tagItr != tagData.end();) {
			bool tagPresent = false;

			double requestRate = tagItr->second.requestRate.smoothRate();
			auto manualItr = manualThrottledTags.find(tagItr->first);
			if (manualItr != manualThrottledTags.end()) {
				Optional<ClientTagThrottleLimits> manualClientRate;
				for (auto priority = allTransactionPriorities.rbegin(); !(priority == allTransactionPriorities.rend());
				     ++priority) {
					auto priorityItr = manualItr->second.find(*priority);
					if (priorityItr != manualItr->second.end()) {
						Optional<double> priorityClientRate = priorityItr->second.updateAndGetClientRate(requestRate);
						if (!priorityClientRate.present()) {
							TEST(true); // Manual priority throttle expired
							priorityItr = manualItr->second.erase(priorityItr);
						} else {
							if (!manualClientRate.present() ||
							    manualClientRate.get().tpsRate > priorityClientRate.get()) {
								manualClientRate = ClientTagThrottleLimits(priorityClientRate.get(),
								                                           priorityItr->second.limits.expiration);
							} else {
								TEST(true); // Manual throttle overriden by higher priority
							}

							++priorityItr;
						}
					}

					if (manualClientRate.present()) {
						tagPresent = true;
						TEST(true); // Using manual throttle
						clientRates[*priority][tagItr->first] = manualClientRate.get();
					}
				}

				if (manualItr->second.empty()) {
					TEST(true); // All manual throttles expired
					manualThrottledTags.erase(manualItr);
					break;
				}
			}

			auto autoItr = autoThrottledTags.find(tagItr->first);
			if (autoItr != autoThrottledTags.end()) {
				Optional<double> autoClientRate = autoItr->second.updateAndGetClientRate(requestRate);
				if (autoClientRate.present()) {
					double adjustedRate = autoClientRate.get();
					double rampStartTime = autoItr->second.lastReduced + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION -
					                       SERVER_KNOBS->AUTO_TAG_THROTTLE_RAMP_UP_TIME;
					if (now() >= rampStartTime && adjustedRate != std::numeric_limits<double>::max()) {
						TEST(true); // Tag auto-throttle ramping up

						double targetBusyness = SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS;
						if (targetBusyness == 0) {
							targetBusyness = 0.01;
						}

						double rampLocation = (now() - rampStartTime) / SERVER_KNOBS->AUTO_TAG_THROTTLE_RAMP_UP_TIME;
						adjustedRate =
						    computeTargetTpsRate(targetBusyness, pow(targetBusyness, 1 - rampLocation), adjustedRate);
					}

					tagPresent = true;
					if (autoThrottlingEnabled) {
						auto result = clientRates[TransactionPriority::DEFAULT].try_emplace(
						    tagItr->first, adjustedRate, autoItr->second.limits.expiration);
						if (!result.second && result.first->second.tpsRate > adjustedRate) {
							result.first->second =
							    ClientTagThrottleLimits(adjustedRate, autoItr->second.limits.expiration);
						} else {
							TEST(true); // Auto throttle overriden by manual throttle
						}
						clientRates[TransactionPriority::BATCH][tagItr->first] =
						    ClientTagThrottleLimits(0, autoItr->second.limits.expiration);
					}
				} else {
					ASSERT(autoItr->second.limits.expiration <= now());
					TEST(true); // Auto throttle expired
					if (BUGGIFY) { // Temporarily extend the window between expiration and cleanup
						tagPresent = true;
					} else {
						autoThrottledTags.erase(autoItr);
					}
				}
			}

			if (!tagPresent) {
				TEST(true); // All tag throttles expired
				tagItr = tagData.erase(tagItr);
			} else {
				++tagItr;
			}
		}

		return clientRates;
	}

	void addRequests(TransactionTag const& tag, int requests) {
		if (requests > 0) {
			TEST(true); // Requests reported for throttled tag

			auto tagItr = tagData.try_emplace(tag);
			tagItr.first->second.requestRate.addDelta(requests);

			double requestRate = tagItr.first->second.requestRate.smoothRate();

			auto autoItr = autoThrottledTags.find(tag);
			if (autoItr != autoThrottledTags.end()) {
				autoItr->second.updateAndGetClientRate(requestRate);
			}

			auto manualItr = manualThrottledTags.find(tag);
			if (manualItr != manualThrottledTags.end()) {
				for (auto priorityItr = manualItr->second.begin(); priorityItr != manualItr->second.end();
				     ++priorityItr) {
					priorityItr->second.updateAndGetClientRate(requestRate);
				}
			}
		}
	}

	Optional<double> getRequestRate(TransactionTag const& tag) {
		auto itr = tagData.find(tag);
		if (itr != tagData.end()) {
			return itr->second.requestRate.smoothRate();
		}
		return Optional<double>();
	}

	int64_t autoThrottleCount() const { return autoThrottledTags.size(); }

	int64_t manualThrottleCount() const {
		int64_t count = 0;
		for (auto itr = manualThrottledTags.begin(); itr != manualThrottledTags.end(); ++itr) {
			count += itr->second.size();
		}

		return count;
	}

	TransactionTagMap<RkTagThrottleData> autoThrottledTags;
	TransactionTagMap<std::map<TransactionPriority, RkTagThrottleData>> manualThrottledTags;
	TransactionTagMap<RkTagData> tagData;
	uint32_t busyReadTagCount = 0, busyWriteTagCount = 0;
};

struct RatekeeperLimits {
	double tpsLimit;
	Int64MetricHandle tpsLimitMetric;
	Int64MetricHandle reasonMetric;

	int64_t storageTargetBytes;
	int64_t storageSpringBytes;
	int64_t logTargetBytes;
	int64_t logSpringBytes;
	double maxVersionDifference;

	int64_t durabilityLagTargetVersions;
	int64_t lastDurabilityLag;
	double durabilityLagLimit;

	TransactionPriority priority;
	std::string context;

	Reference<EventCacheHolder> rkUpdateEventCacheHolder;

	RatekeeperLimits(TransactionPriority priority,
	                 std::string context,
	                 int64_t storageTargetBytes,
	                 int64_t storageSpringBytes,
	                 int64_t logTargetBytes,
	                 int64_t logSpringBytes,
	                 double maxVersionDifference,
	                 int64_t durabilityLagTargetVersions)
	  : tpsLimit(std::numeric_limits<double>::infinity()), tpsLimitMetric(StringRef("Ratekeeper.TPSLimit" + context)),
	    reasonMetric(StringRef("Ratekeeper.Reason" + context)), storageTargetBytes(storageTargetBytes),
	    storageSpringBytes(storageSpringBytes), logTargetBytes(logTargetBytes), logSpringBytes(logSpringBytes),
	    maxVersionDifference(maxVersionDifference),
	    durabilityLagTargetVersions(
	        durabilityLagTargetVersions +
	        SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS), // The read transaction life versions are expected to not
	                                                           // be durable on the storage servers
	    lastDurabilityLag(0), durabilityLagLimit(std::numeric_limits<double>::infinity()), priority(priority),
	    context(context), rkUpdateEventCacheHolder(makeReference<EventCacheHolder>("RkUpdate" + context)) {}
};

struct RatekeeperData {
	// Differentiate from GrvProxyInfo in DatabaseContext.h
	struct GrvProxyInfo {
		int64_t totalTransactions;
		int64_t batchTransactions;
		uint64_t lastThrottledTagChangeId;

		double lastUpdateTime;
		double lastTagPushTime;

		GrvProxyInfo()
		  : totalTransactions(0), batchTransactions(0), lastThrottledTagChangeId(0), lastUpdateTime(0),
		    lastTagPushTime(0) {}
	};

	UID id;
	Database db;

	Map<UID, StorageQueueInfo> storageQueueInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;

	std::map<UID, RatekeeperData::GrvProxyInfo> grvProxyInfo;
	Smoother smoothReleasedTransactions, smoothBatchReleasedTransactions, smoothTotalDurableBytes;
	HealthMetrics healthMetrics;
	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	Int64MetricHandle actualTpsMetric;

	double lastWarning;
	double lastSSListFetchedTimestamp;
	double lastBusiestCommitTagPick;

	RkTagThrottleCollection throttledTags;
	uint64_t throttledTagChangeId;

	RatekeeperLimits normalLimits;
	RatekeeperLimits batchLimits;

	Deque<double> actualTpsHistory;
	Optional<Key> remoteDC;

	Future<Void> expiredTagThrottleCleanup;

	bool autoThrottlingEnabled;

	RatekeeperData(UID id, Database db);

	Future<Void> configurationMonitor();
	void updateCommitCostEstimation(UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation);
	void updateRate(RatekeeperLimits* limits);
	Future<Void> refreshStorageServerCommitCost();
	Future<Void> monitorServerListChange(PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges);
	Future<Void> trackEachStorageServer(FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges);

	// SOMEDAY: template trackStorageServerQueueInfo and trackTLogQueueInfo into one function
	Future<Void> trackStorageServerQueueInfo(StorageServerInterface);
	Future<Void> trackTLogQueueInfo(TLogInterface);

	void tryAutoThrottleTag(TransactionTag, double rate, double busyness, TagThrottledReason);
	void tryAutoThrottleTag(StorageQueueInfo&, int64_t storageQueue, int64_t storageDurabilityLag);
	Future<Void> monitorThrottlingChanges();

	static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
};
