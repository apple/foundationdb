/*
 * Ratekeeper.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/WorkerInterface.actor.h"
#include "flow/IndexedSet.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Smoother.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Tuple.h" // TODO REMOVE
#include "fdbclient/S3BlobStore.h" // TODO REMOVE
#include "fdbclient/AsyncFileS3BlobStore.actor.h" // TODO REMOVE
#include "fdbclient/BlobWorkerInterface.h" // TODO REMOVE
#include "fdbserver/BlobManagerInterface.h" // TODO REMOVE
#include "fdbclient/TagThrottle.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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

int limitReasonEnd = limitReason_t_end;

const char* limitReasonName[] = { "workload",
	                              "storage_server_write_queue_size",
	                              "storage_server_write_bandwidth_mvcc",
	                              "storage_server_readable_behind",
	                              "log_server_mvcc_write_bandwidth",
	                              "log_server_write_queue",
	                              "storage_server_min_free_space",
	                              "storage_server_min_free_space_ratio",
	                              "log_server_min_free_space",
	                              "log_server_min_free_space_ratio",
	                              "storage_server_durability_lag",
	                              "storage_server_list_fetch_failed" };
static_assert(sizeof(limitReasonName) / sizeof(limitReasonName[0]) == limitReason_t_end, "limitReasonDesc table size");

// NOTE: This has a corresponding table in Script.cs (see RatekeeperReason graph)
// IF UPDATING THIS ARRAY, UPDATE SCRIPT.CS!
const char* limitReasonDesc[] = { "Workload or read performance.",
	                              "Storage server performance (storage queue).",
	                              "Storage server MVCC memory.",
	                              "Storage server version falling behind.",
	                              "Log server MVCC memory.",
	                              "Storage server performance (log queue).",
	                              "Storage server running out of space (approaching 100MB limit).",
	                              "Storage server running out of space (approaching 5% limit).",
	                              "Log server running out of space (approaching 100MB limit).",
	                              "Log server running out of space (approaching 5% limit).",
	                              "Storage server durable version falling behind.",
	                              "Unable to fetch storage server list." };

static_assert(sizeof(limitReasonDesc) / sizeof(limitReasonDesc[0]) == limitReason_t_end, "limitReasonDesc table size");

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

	// refresh periodically
	TransactionTagMap<TransactionCommitCostEstimation> tagCostEst;
	uint64_t totalWriteCosts = 0;
	int totalWriteOps = 0;

	StorageQueueInfo(UID id, LocalityData locality)
	  : valid(false), id(id), locality(locality), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
	    smoothDurableVersion(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothLatestVersion(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    limitReason(limitReason_t::unlimited) {
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

	RatekeeperLimits(TransactionPriority priority,
	                 std::string context,
	                 int64_t storageTargetBytes,
	                 int64_t storageSpringBytes,
	                 int64_t logTargetBytes,
	                 int64_t logSpringBytes,
	                 double maxVersionDifference,
	                 int64_t durabilityLagTargetVersions)
	  : priority(priority), tpsLimit(std::numeric_limits<double>::infinity()),
	    tpsLimitMetric(StringRef("Ratekeeper.TPSLimit" + context)),
	    reasonMetric(StringRef("Ratekeeper.Reason" + context)), storageTargetBytes(storageTargetBytes),
	    storageSpringBytes(storageSpringBytes), logTargetBytes(logTargetBytes), logSpringBytes(logSpringBytes),
	    maxVersionDifference(maxVersionDifference),
	    durabilityLagTargetVersions(
	        durabilityLagTargetVersions +
	        SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS), // The read transaction life versions are expected to not
	                                                           // be durable on the storage servers
	    durabilityLagLimit(std::numeric_limits<double>::infinity()), lastDurabilityLag(0), context(context) {}
};

struct GrvProxyInfo {
	int64_t totalTransactions;
	int64_t batchTransactions;
	uint64_t lastThrottledTagChangeId;

	double lastUpdateTime;
	double lastTagPushTime;

	GrvProxyInfo()
	  : totalTransactions(0), batchTransactions(0), lastUpdateTime(0), lastThrottledTagChangeId(0), lastTagPushTime(0) {
	}
};

struct RatekeeperData {
	UID id;
	Database db;

	Map<UID, StorageQueueInfo> storageQueueInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;

	std::map<UID, GrvProxyInfo> grvProxyInfo;
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

	RatekeeperData(UID id, Database db)
	  : id(id), db(db), smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothBatchReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothTotalDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
	    actualTpsMetric(LiteralStringRef("Ratekeeper.ActualTPS")), lastWarning(0), lastSSListFetchedTimestamp(now()),
	    throttledTagChangeId(0), lastBusiestCommitTagPick(0),
	    normalLimits(TransactionPriority::DEFAULT,
	                 "",
	                 SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
	                 SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER,
	                 SERVER_KNOBS->TARGET_BYTES_PER_TLOG,
	                 SERVER_KNOBS->SPRING_BYTES_TLOG,
	                 SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE,
	                 SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS),
	    batchLimits(TransactionPriority::BATCH,
	                "Batch",
	                SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER_BATCH,
	                SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER_BATCH,
	                SERVER_KNOBS->TARGET_BYTES_PER_TLOG_BATCH,
	                SERVER_KNOBS->SPRING_BYTES_TLOG_BATCH,
	                SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE_BATCH,
	                SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS_BATCH),
	    autoThrottlingEnabled(false) {
		expiredTagThrottleCleanup =
		    recurring([this]() { ThrottleApi::expire(this->db); }, SERVER_KNOBS->TAG_THROTTLE_EXPIRED_CLEANUP_INTERVAL);
	}
};

// SOMEDAY: template trackStorageServerQueueInfo and trackTLogQueueInfo into one function
ACTOR Future<Void> trackStorageServerQueueInfo(RatekeeperData* self, StorageServerInterface ssi) {
	self->storageQueueInfo.insert(mapPair(ssi.id(), StorageQueueInfo(ssi.id(), ssi.locality)));
	state Map<UID, StorageQueueInfo>::iterator myQueueInfo = self->storageQueueInfo.find(ssi.id());
	TraceEvent("RkTracking", self->id).detail("StorageServer", ssi.id()).detail("Locality", ssi.locality.toString());
	try {
		loop {
			ErrorOr<StorageQueuingMetricsReply> reply = wait(ssi.getQueuingMetrics.getReplyUnlessFailedFor(
			    StorageQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
			if (reply.present()) {
				myQueueInfo->value.valid = true;
				myQueueInfo->value.prevReply = myQueueInfo->value.lastReply;
				myQueueInfo->value.lastReply = reply.get();
				if (myQueueInfo->value.prevReply.instanceID != reply.get().instanceID) {
					myQueueInfo->value.smoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.reset(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.reset(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.reset(reply.get().storageBytes.total);
					myQueueInfo->value.smoothDurableVersion.reset(reply.get().durableVersion);
					myQueueInfo->value.smoothLatestVersion.reset(reply.get().version);
				} else {
					self->smoothTotalDurableBytes.addDelta(reply.get().bytesDurable -
					                                       myQueueInfo->value.prevReply.bytesDurable);
					myQueueInfo->value.smoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.setTotal(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.setTotal(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.setTotal(reply.get().storageBytes.total);
					myQueueInfo->value.smoothDurableVersion.setTotal(reply.get().durableVersion);
					myQueueInfo->value.smoothLatestVersion.setTotal(reply.get().version);
				}

				myQueueInfo->value.busiestReadTag = reply.get().busiestTag;
				myQueueInfo->value.busiestReadTagFractionalBusyness = reply.get().busiestTagFractionalBusyness;
				myQueueInfo->value.busiestReadTagRate = reply.get().busiestTagRate;
			} else {
				if (myQueueInfo->value.valid) {
					TraceEvent("RkStorageServerDidNotRespond", self->id).detail("StorageServer", ssi.id());
				}
				myQueueInfo->value.valid = false;
			}

			wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
			     IFailureMonitor::failureMonitor().onStateEqual(ssi.getQueuingMetrics.getEndpoint(),
			                                                    FailureStatus(false)));
		}
	} catch (...) {
		// including cancellation
		self->storageQueueInfo.erase(myQueueInfo);
		throw;
	}
}

ACTOR Future<Void> trackTLogQueueInfo(RatekeeperData* self, TLogInterface tli) {
	self->tlogQueueInfo.insert(mapPair(tli.id(), TLogQueueInfo(tli.id())));
	state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
	TraceEvent("RkTracking", self->id).detail("TransactionLog", tli.id());
	try {
		loop {
			ErrorOr<TLogQueuingMetricsReply> reply = wait(tli.getQueuingMetrics.getReplyUnlessFailedFor(
			    TLogQueuingMetricsRequest(), 0, 0)); // SOMEDAY: or tryGetReply?
			if (reply.present()) {
				myQueueInfo->value.valid = true;
				myQueueInfo->value.prevReply = myQueueInfo->value.lastReply;
				myQueueInfo->value.lastReply = reply.get();
				if (myQueueInfo->value.prevReply.instanceID != reply.get().instanceID) {
					myQueueInfo->value.smoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.reset(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.reset(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.reset(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.reset(reply.get().storageBytes.total);
				} else {
					self->smoothTotalDurableBytes.addDelta(reply.get().bytesDurable -
					                                       myQueueInfo->value.prevReply.bytesDurable);
					myQueueInfo->value.smoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.setTotal(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.setTotal(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.setTotal(reply.get().storageBytes.total);
				}
			} else {
				if (myQueueInfo->value.valid) {
					TraceEvent("RkTLogDidNotRespond", self->id).detail("TransactionLog", tli.id());
				}
				myQueueInfo->value.valid = false;
			}

			wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) &&
			     IFailureMonitor::failureMonitor().onStateEqual(tli.getQueuingMetrics.getEndpoint(),
			                                                    FailureStatus(false)));
		}
	} catch (...) {
		// including cancellation
		self->tlogQueueInfo.erase(myQueueInfo);
		throw;
	}
}

ACTOR Future<Void> splitError(Future<Void> in, Promise<Void> errOut) {
	try {
		wait(in);
		return Void();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !errOut.isSet())
			errOut.sendError(e);
		throw;
	}
}

ACTOR Future<Void> trackEachStorageServer(
    RatekeeperData* self,
    FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	state Map<UID, Future<Void>> actors;
	state Promise<Void> err;
	loop choose {
		when(state std::pair<UID, Optional<StorageServerInterface>> change = waitNext(serverChanges)) {
			wait(delay(0)); // prevent storageServerTracker from getting cancelled while on the call stack
			if (change.second.present()) {
				if (!change.second.get().isTss()) {
					auto& a = actors[change.first];
					a = Future<Void>();
					a = splitError(trackStorageServerQueueInfo(self, change.second.get()), err);
				}
			} else
				actors.erase(change.first);
		}
		when(wait(err.getFuture())) {}
	}
}

ACTOR Future<Void> monitorServerListChange(
    RatekeeperData* self,
    PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges) {
	state std::map<UID, StorageServerInterface> oldServers;
	state Transaction tr(self->db);

	loop {
		try {
			if (now() - self->lastSSListFetchedTimestamp > 2 * SERVER_KNOBS->SERVER_LIST_DELAY) {
				TraceEvent(SevWarnAlways, "RatekeeperGetSSListLongLatency", self->id)
				    .detail("Latency", now() - self->lastSSListFetchedTimestamp);
			}
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			vector<std::pair<StorageServerInterface, ProcessClass>> results = wait(getServerListAndProcessClasses(&tr));
			self->lastSSListFetchedTimestamp = now();

			std::map<UID, StorageServerInterface> newServers;
			for (const auto& [ssi, _] : results) {
				const UID serverId = ssi.id();
				newServers[serverId] = ssi;

				if (oldServers.count(serverId)) {
					if (ssi.getValue.getEndpoint() != oldServers[serverId].getValue.getEndpoint()) {
						serverChanges.send(std::make_pair(serverId, Optional<StorageServerInterface>(ssi)));
					}
					oldServers.erase(serverId);
				} else {
					serverChanges.send(std::make_pair(serverId, Optional<StorageServerInterface>(ssi)));
				}
			}

			for (const auto& it : oldServers) {
				serverChanges.send(std::make_pair(it.first, Optional<StorageServerInterface>()));
			}

			oldServers.swap(newServers);
			tr = Transaction(self->db);
			wait(delay(SERVER_KNOBS->SERVER_LIST_DELAY));
		} catch (Error& e) {
			TraceEvent("RatekeeperGetSSListError", self->id).error(e).suppressFor(1.0);
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> monitorThrottlingChanges(RatekeeperData* self) {
	state bool committed = false;
	loop {
		state ReadYourWritesTransaction tr(self->db);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				state Future<RangeResult> throttledTagKeys = tr.getRange(tagThrottleKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<Optional<Value>> autoThrottlingEnabled = tr.get(tagThrottleAutoEnabledKey);

				if (!committed) {
					BinaryWriter limitWriter(Unversioned());
					limitWriter << SERVER_KNOBS->MAX_MANUAL_THROTTLED_TRANSACTION_TAGS;
					tr.set(tagThrottleLimitKey, limitWriter.toValue());
				}

				wait(success(throttledTagKeys) && success(autoThrottlingEnabled));

				if (autoThrottlingEnabled.get().present() &&
				    autoThrottlingEnabled.get().get() == LiteralStringRef("0")) {
					TEST(true); // Auto-throttling disabled
					if (self->autoThrottlingEnabled) {
						TraceEvent("AutoTagThrottlingDisabled", self->id);
					}
					self->autoThrottlingEnabled = false;
				} else if (autoThrottlingEnabled.get().present() &&
				           autoThrottlingEnabled.get().get() == LiteralStringRef("1")) {
					TEST(true); // Auto-throttling enabled
					if (!self->autoThrottlingEnabled) {
						TraceEvent("AutoTagThrottlingEnabled", self->id);
					}
					self->autoThrottlingEnabled = true;
				} else {
					TEST(true); // Auto-throttling unspecified
					if (autoThrottlingEnabled.get().present()) {
						TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue", self->id)
						    .detail("Value", autoThrottlingEnabled.get().get());
					}
					self->autoThrottlingEnabled = SERVER_KNOBS->AUTO_TAG_THROTTLING_ENABLED;
					if (!committed)
						tr.set(tagThrottleAutoEnabledKey, LiteralStringRef(self->autoThrottlingEnabled ? "1" : "0"));
				}

				RkTagThrottleCollection updatedTagThrottles;

				TraceEvent("RatekeeperReadThrottledTags", self->id)
				    .detail("NumThrottledTags", throttledTagKeys.get().size());
				for (auto entry : throttledTagKeys.get()) {
					TagThrottleKey tagKey = TagThrottleKey::fromKey(entry.key);
					TagThrottleValue tagValue = TagThrottleValue::fromValue(entry.value);

					ASSERT(tagKey.tags.size() == 1); // Currently, only 1 tag per throttle is supported

					if (tagValue.expirationTime == 0 || tagValue.expirationTime > now() + tagValue.initialDuration) {
						TEST(true); // Converting tag throttle duration to absolute time
						tagValue.expirationTime = now() + tagValue.initialDuration;
						BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagThrottleValueReason()));
						wr << tagValue;
						state Value value = wr.toValue();

						tr.set(entry.key, value);
					}

					if (tagValue.expirationTime > now()) {
						TransactionTag tag = *tagKey.tags.begin();
						Optional<ClientTagThrottleLimits> oldLimits =
						    self->throttledTags.getManualTagThrottleLimits(tag, tagKey.priority);

						if (tagKey.throttleType == TagThrottleType::AUTO) {
							updatedTagThrottles.autoThrottleTag(
							    self->id, tag, 0, tagValue.tpsRate, tagValue.expirationTime);
							if (tagValue.reason == TagThrottledReason::BUSY_READ) {
								updatedTagThrottles.busyReadTagCount++;
							} else if (tagValue.reason == TagThrottledReason::BUSY_WRITE) {
								updatedTagThrottles.busyWriteTagCount++;
							}
						} else {
							updatedTagThrottles.manualThrottleTag(
							    self->id, tag, tagKey.priority, tagValue.tpsRate, tagValue.expirationTime, oldLimits);
						}
					}
				}

				self->throttledTags = std::move(updatedTagThrottles);
				++self->throttledTagChangeId;

				state Future<Void> watchFuture = tr.watch(tagThrottleSignalKey);
				wait(tr.commit());
				committed = true;

				wait(watchFuture);
				TraceEvent("RatekeeperThrottleSignaled", self->id);
				TEST(true); // Tag throttle changes detected
				break;
			} catch (Error& e) {
				TraceEvent("RatekeeperMonitorThrottlingChangesError", self->id).error(e);
				wait(tr.onError(e));
			}
		}
	}
}

Future<Void> refreshStorageServerCommitCost(RatekeeperData* self) {
	if (self->lastBusiestCommitTagPick == 0) { // the first call should be skipped
		self->lastBusiestCommitTagPick = now();
		return Void();
	}
	double elapsed = now() - self->lastBusiestCommitTagPick;
	// for each SS, select the busiest commit tag from ssTrTagCommitCost
	for (auto it = self->storageQueueInfo.begin(); it != self->storageQueueInfo.end(); ++it) {
		it->value.busiestWriteTag.reset();
		TransactionTag busiestTag;
		TransactionCommitCostEstimation maxCost;
		double maxRate = 0, maxBusyness = 0;
		for (const auto& [tag, cost] : it->value.tagCostEst) {
			double rate = cost.getCostSum() / elapsed;
			if (rate > maxRate) {
				busiestTag = tag;
				maxRate = rate;
				maxCost = cost;
			}
		}
		if (maxRate > SERVER_KNOBS->MIN_TAG_WRITE_PAGES_RATE) {
			it->value.busiestWriteTag = busiestTag;
			// TraceEvent("RefreshSSCommitCost").detail("TotalWriteCost", it->value.totalWriteCost).detail("TotalWriteOps",it->value.totalWriteOps);
			ASSERT(it->value.totalWriteCosts > 0);
			maxBusyness = double(maxCost.getCostSum()) / it->value.totalWriteCosts;
			it->value.busiestWriteTagFractionalBusyness = maxBusyness;
			it->value.busiestWriteTagRate = maxRate;
		}

		TraceEvent("BusiestWriteTag", it->key)
		    .detail("Elapsed", elapsed)
		    .detail("Tag", printable(busiestTag))
		    .detail("TagOps", maxCost.getOpsSum())
		    .detail("TagCost", maxCost.getCostSum())
		    .detail("TotalCost", it->value.totalWriteCosts)
		    .detail("Reported", it->value.busiestWriteTag.present())
		    .trackLatest(it->key.toString() + "/BusiestWriteTag");

		// reset statistics
		it->value.tagCostEst.clear();
		it->value.totalWriteOps = 0;
		it->value.totalWriteCosts = 0;
	}
	self->lastBusiestCommitTagPick = now();
	return Void();
}

void tryAutoThrottleTag(RatekeeperData* self,
                        TransactionTag tag,
                        double rate,
                        double busyness,
                        TagThrottledReason reason) {
	// NOTE: before the comparison with MIN_TAG_COST, the busiest tag rate also compares with MIN_TAG_PAGES_RATE
	// currently MIN_TAG_PAGES_RATE > MIN_TAG_COST in our default knobs.
	if (busyness > SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS && rate > SERVER_KNOBS->MIN_TAG_COST) {
		TEST(true); // Transaction tag auto-throttled
		Optional<double> clientRate = self->throttledTags.autoThrottleTag(self->id, tag, busyness);
		if (clientRate.present()) {
			TagSet tags;
			tags.addTag(tag);

			self->addActor.send(ThrottleApi::throttleTags(self->db,
			                                              tags,
			                                              clientRate.get(),
			                                              SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION,
			                                              TagThrottleType::AUTO,
			                                              TransactionPriority::DEFAULT,
			                                              now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION,
			                                              reason));
		}
	}
}

void tryAutoThrottleTag(RatekeeperData* self,
                        StorageQueueInfo& ss,
                        int64_t storageQueue,
                        int64_t storageDurabilityLag) {
	// NOTE: we just keep it simple and don't differentiate write-saturation and read-saturation at the moment. In most
	// of situation, this works. More indicators besides queue size and durability lag could be investigated in the
	// future
	if (storageQueue > SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES ||
	    storageDurabilityLag > SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS) {
		if (ss.busiestWriteTag.present()) {
			tryAutoThrottleTag(self,
			                   ss.busiestWriteTag.get(),
			                   ss.busiestWriteTagRate,
			                   ss.busiestWriteTagFractionalBusyness,
			                   TagThrottledReason::BUSY_WRITE);
		}
		if (ss.busiestReadTag.present()) {
			tryAutoThrottleTag(self,
			                   ss.busiestReadTag.get(),
			                   ss.busiestReadTagRate,
			                   ss.busiestReadTagFractionalBusyness,
			                   TagThrottledReason::BUSY_READ);
		}
	}
}

void updateRate(RatekeeperData* self, RatekeeperLimits* limits) {
	// double controlFactor = ;  // dt / eFoldingTime

	double actualTps = self->smoothReleasedTransactions.smoothRate();
	self->actualTpsMetric = (int64_t)actualTps;
	// SOMEDAY: Remove the max( 1.0, ... ) since the below calculations _should_ be able to recover back up from this
	// value
	actualTps = std::max(std::max(1.0, actualTps),
	                     self->smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT);

	if (self->actualTpsHistory.size() > SERVER_KNOBS->MAX_TPS_HISTORY_SAMPLES) {
		self->actualTpsHistory.pop_front();
	}
	self->actualTpsHistory.push_back(actualTps);

	limits->tpsLimit = std::numeric_limits<double>::infinity();
	UID reasonID = UID();
	limitReason_t limitReason = limitReason_t::unlimited;

	int sscount = 0;

	int64_t worstFreeSpaceStorageServer = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueStorageServer = 0;
	int64_t limitingStorageQueueStorageServer = 0;
	int64_t worstDurabilityLag = 0;

	std::multimap<double, StorageQueueInfo*> storageTpsLimitReverseIndex;
	std::multimap<int64_t, StorageQueueInfo*> storageDurabilityLagReverseIndex;

	std::map<UID, limitReason_t> ssReasons;

	// Look at each storage server's write queue and local rate, compute and store the desired rate ratio
	for (auto i = self->storageQueueInfo.begin(); i != self->storageQueueInfo.end(); ++i) {
		auto& ss = i->value;
		if (!ss.valid || (self->remoteDC.present() && ss.locality.dcId() == self->remoteDC))
			continue;
		++sscount;

		limitReason_t ssLimitReason = limitReason_t::unlimited;

		int64_t minFreeSpace =
		    std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		             (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * ss.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceStorageServer =
		    std::min(worstFreeSpaceStorageServer, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(
		    1, std::min<int64_t>(limits->storageSpringBytes, (ss.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(
		    1, std::min(limits->storageTargetBytes, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace));
		if (targetBytes != limits->storageTargetBytes) {
			if (minFreeSpace == SERVER_KNOBS->MIN_AVAILABLE_SPACE) {
				ssLimitReason = limitReason_t::storage_server_min_free_space;
			} else {
				ssLimitReason = limitReason_t::storage_server_min_free_space_ratio;
			}
		}

		int64_t storageQueue = ss.lastReply.bytesInput - ss.smoothDurableBytes.smoothTotal();
		worstStorageQueueStorageServer = std::max(worstStorageQueueStorageServer, storageQueue);

		int64_t storageDurabilityLag = ss.smoothLatestVersion.smoothTotal() - ss.smoothDurableVersion.smoothTotal();
		worstDurabilityLag = std::max(worstDurabilityLag, storageDurabilityLag);

		storageDurabilityLagReverseIndex.insert(std::make_pair(-1 * storageDurabilityLag, &ss));

		auto& ssMetrics = self->healthMetrics.storageStats[ss.id];
		ssMetrics.storageQueue = storageQueue;
		ssMetrics.storageDurabilityLag = storageDurabilityLag;
		ssMetrics.cpuUsage = ss.lastReply.cpuUsage;
		ssMetrics.diskUsage = ss.lastReply.diskUsage;

		double targetRateRatio = std::min((storageQueue - targetBytes + springBytes) / (double)springBytes, 2.0);

		if (limits->priority == TransactionPriority::DEFAULT) {
			tryAutoThrottleTag(self, ss, storageQueue, storageDurabilityLag);
		}

		double inputRate = ss.smoothInputBytes.smoothRate();
		// inputRate = std::max( inputRate, actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );

		/*if( deterministicRandom()->random01() < 0.1 ) {
		    std::string name = "RatekeeperUpdateRate" + limits.context;
		    TraceEvent(name, ss.id)
		        .detail("MinFreeSpace", minFreeSpace)
		        .detail("SpringBytes", springBytes)
		        .detail("TargetBytes", targetBytes)
		        .detail("SmoothTotalSpaceTotal", ss.smoothTotalSpace.smoothTotal())
		        .detail("SmoothFreeSpaceTotal", ss.smoothFreeSpace.smoothTotal())
		        .detail("LastReplyBytesInput", ss.lastReply.bytesInput)
		        .detail("SmoothDurableBytesTotal", ss.smoothDurableBytes.smoothTotal())
		        .detail("TargetRateRatio", targetRateRatio)
		        .detail("SmoothInputBytesRate", ss.smoothInputBytes.smoothRate())
		        .detail("ActualTPS", actualTps)
		        .detail("InputRate", inputRate)
		        .detail("VerySmoothDurableBytesRate", ss.verySmoothDurableBytes.smoothRate())
		        .detail("B", b);
		}*/

		// Don't let any storage server use up its target bytes faster than its MVCC window!
		double maxBytesPerSecond =
		    (targetBytes - springBytes) /
		    ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0);
		double limitTps = std::min(actualTps * maxBytesPerSecond / std::max(1.0e-8, inputRate),
		                           maxBytesPerSecond * SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
		if (ssLimitReason == limitReason_t::unlimited)
			ssLimitReason = limitReason_t::storage_server_write_bandwidth_mvcc;

		if (targetRateRatio > 0 && inputRate > 0) {
			ASSERT(inputRate != 0);
			double smoothedRate =
			    std::max(ss.verySmoothDurableBytes.smoothRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
			double x = smoothedRate / (inputRate * targetRateRatio);
			double lim = actualTps * x;
			if (lim < limitTps) {
				limitTps = lim;
				if (ssLimitReason == limitReason_t::unlimited ||
				    ssLimitReason == limitReason_t::storage_server_write_bandwidth_mvcc) {
					ssLimitReason = limitReason_t::storage_server_write_queue_size;
				}
			}
		}

		storageTpsLimitReverseIndex.insert(std::make_pair(limitTps, &ss));

		if (limitTps < limits->tpsLimit && (ssLimitReason == limitReason_t::storage_server_min_free_space ||
		                                    ssLimitReason == limitReason_t::storage_server_min_free_space_ratio)) {
			reasonID = ss.id;
			limits->tpsLimit = limitTps;
			limitReason = ssLimitReason;
		}

		ssReasons[ss.id] = ssLimitReason;
	}

	std::set<Optional<Standalone<StringRef>>> ignoredMachines;
	for (auto ss = storageTpsLimitReverseIndex.begin();
	     ss != storageTpsLimitReverseIndex.end() && ss->first < limits->tpsLimit;
	     ++ss) {
		if (ignoredMachines.size() <
		    std::min(self->configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingStorageQueueStorageServer =
		    ss->second->lastReply.bytesInput - ss->second->smoothDurableBytes.smoothTotal();
		limits->tpsLimit = ss->first;
		reasonID = storageTpsLimitReverseIndex.begin()->second->id; // Although we aren't controlling based on the worst
		                                                            // SS, we still report it as the limiting process
		limitReason = ssReasons[reasonID];
		break;
	}

	// Calculate limited durability lag
	int64_t limitingDurabilityLag = 0;

	std::set<Optional<Standalone<StringRef>>> ignoredDurabilityLagMachines;
	for (auto ss = storageDurabilityLagReverseIndex.begin(); ss != storageDurabilityLagReverseIndex.end(); ++ss) {
		if (ignoredDurabilityLagMachines.size() <
		    std::min(self->configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredDurabilityLagMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredDurabilityLagMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingDurabilityLag = -1 * ss->first;
		if (limitingDurabilityLag > limits->durabilityLagTargetVersions &&
		    self->actualTpsHistory.size() > SERVER_KNOBS->NEEDED_TPS_HISTORY_SAMPLES) {
			if (limits->durabilityLagLimit == std::numeric_limits<double>::infinity()) {
				double maxTps = 0;
				for (int i = 0; i < self->actualTpsHistory.size(); i++) {
					maxTps = std::max(maxTps, self->actualTpsHistory[i]);
				}
				limits->durabilityLagLimit = SERVER_KNOBS->INITIAL_DURABILITY_LAG_MULTIPLIER * maxTps;
			}
			if (limitingDurabilityLag > limits->lastDurabilityLag) {
				limits->durabilityLagLimit = SERVER_KNOBS->DURABILITY_LAG_REDUCTION_RATE * limits->durabilityLagLimit;
			}
			if (limits->durabilityLagLimit < limits->tpsLimit) {
				limits->tpsLimit = limits->durabilityLagLimit;
				limitReason = limitReason_t::storage_server_durability_lag;
			}
		} else if (limits->durabilityLagLimit != std::numeric_limits<double>::infinity() &&
		           limitingDurabilityLag >
		               limits->durabilityLagTargetVersions - SERVER_KNOBS->DURABILITY_LAG_UNLIMITED_THRESHOLD) {
			limits->durabilityLagLimit = SERVER_KNOBS->DURABILITY_LAG_INCREASE_RATE * limits->durabilityLagLimit;
		} else {
			limits->durabilityLagLimit = std::numeric_limits<double>::infinity();
		}
		limits->lastDurabilityLag = limitingDurabilityLag;
		break;
	}

	self->healthMetrics.worstStorageQueue = worstStorageQueueStorageServer;
	self->healthMetrics.limitingStorageQueue = limitingStorageQueueStorageServer;
	self->healthMetrics.worstStorageDurabilityLag = worstDurabilityLag;
	self->healthMetrics.limitingStorageDurabilityLag = limitingDurabilityLag;

	double writeToReadLatencyLimit = 0;
	Version worstVersionLag = 0;
	Version limitingVersionLag = 0;

	{
		Version minSSVer = std::numeric_limits<Version>::max();
		Version minLimitingSSVer = std::numeric_limits<Version>::max();
		for (const auto& it : self->storageQueueInfo) {
			auto& ss = it.value;
			if (!ss.valid || (self->remoteDC.present() && ss.locality.dcId() == self->remoteDC))
				continue;

			minSSVer = std::min(minSSVer, ss.lastReply.version);

			// Machines that ratekeeper isn't controlling can fall arbitrarily far behind
			if (ignoredMachines.count(it.value.locality.zoneId()) == 0) {
				minLimitingSSVer = std::min(minLimitingSSVer, ss.lastReply.version);
			}
		}

		Version maxTLVer = std::numeric_limits<Version>::min();
		for (const auto& it : self->tlogQueueInfo) {
			auto& tl = it.value;
			if (!tl.valid)
				continue;
			maxTLVer = std::max(maxTLVer, tl.lastReply.v);
		}

		if (minSSVer != std::numeric_limits<Version>::max() && maxTLVer != std::numeric_limits<Version>::min()) {
			// writeToReadLatencyLimit: 0 = infinte speed; 1 = TL durable speed ; 2 = half TL durable speed
			writeToReadLatencyLimit =
			    ((maxTLVer - minLimitingSSVer) - limits->maxVersionDifference / 2) / (limits->maxVersionDifference / 4);
			worstVersionLag = std::max((Version)0, maxTLVer - minSSVer);
			limitingVersionLag = std::max((Version)0, maxTLVer - minLimitingSSVer);
		}
	}

	int64_t worstFreeSpaceTLog = std::numeric_limits<int64_t>::max();
	int64_t worstStorageQueueTLog = 0;
	int tlcount = 0;
	for (auto& it : self->tlogQueueInfo) {
		auto& tl = it.value;
		if (!tl.valid)
			continue;
		++tlcount;

		limitReason_t tlogLimitReason = limitReason_t::log_server_write_queue;

		int64_t minFreeSpace =
		    std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE,
		             (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * tl.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceTLog = std::min(worstFreeSpaceTLog, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(
		    1, std::min<int64_t>(limits->logSpringBytes, (tl.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(
		    1, std::min(limits->logTargetBytes, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace));
		if (targetBytes != limits->logTargetBytes) {
			if (minFreeSpace == SERVER_KNOBS->MIN_AVAILABLE_SPACE) {
				tlogLimitReason = limitReason_t::log_server_min_free_space;
			} else {
				tlogLimitReason = limitReason_t::log_server_min_free_space_ratio;
			}
		}

		int64_t queue = tl.lastReply.bytesInput - tl.smoothDurableBytes.smoothTotal();
		self->healthMetrics.tLogQueue[tl.id] = queue;
		int64_t b = queue - targetBytes;
		worstStorageQueueTLog = std::max(worstStorageQueueTLog, queue);

		if (tl.lastReply.bytesInput - tl.lastReply.bytesDurable > tl.lastReply.storageBytes.free - minFreeSpace / 2) {
			if (now() - self->lastWarning > 5.0) {
				self->lastWarning = now();
				TraceEvent(SevWarnAlways, "RkTlogMinFreeSpaceZero", self->id).detail("ReasonId", tl.id);
			}
			reasonID = tl.id;
			limitReason = limitReason_t::log_server_min_free_space;
			limits->tpsLimit = 0.0;
		}

		double targetRateRatio = std::min((b + springBytes) / (double)springBytes, 2.0);

		if (writeToReadLatencyLimit > targetRateRatio) {
			targetRateRatio = writeToReadLatencyLimit;
			tlogLimitReason = limitReason_t::storage_server_readable_behind;
		}

		double inputRate = tl.smoothInputBytes.smoothRate();

		if (targetRateRatio > 0) {
			double smoothedRate =
			    std::max(tl.verySmoothDurableBytes.smoothRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
			double x = smoothedRate / (inputRate * targetRateRatio);
			if (targetRateRatio < .75) //< FIXME: KNOB for 2.0
				x = std::max(x, 0.95);
			double lim = actualTps * x;
			if (lim < limits->tpsLimit) {
				limits->tpsLimit = lim;
				reasonID = tl.id;
				limitReason = tlogLimitReason;
			}
		}
		if (inputRate > 0) {
			// Don't let any tlogs use up its target bytes faster than its MVCC window!
			double x =
			    ((targetBytes - springBytes) /
			     ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
			      2.0)) /
			    inputRate;
			double lim = actualTps * x;
			if (lim < limits->tpsLimit) {
				limits->tpsLimit = lim;
				reasonID = tl.id;
				limitReason = limitReason_t::log_server_mvcc_write_bandwidth;
			}
		}
	}

	self->healthMetrics.worstTLogQueue = worstStorageQueueTLog;

	limits->tpsLimit = std::max(limits->tpsLimit, 0.0);

	if (g_network->isSimulated() && g_simulator.speedUpSimulation) {
		limits->tpsLimit = std::max(limits->tpsLimit, 100.0);
	}

	int64_t totalDiskUsageBytes = 0;
	for (auto& t : self->tlogQueueInfo) {
		if (t.value.valid) {
			totalDiskUsageBytes += t.value.lastReply.storageBytes.used;
		}
	}
	for (auto& s : self->storageQueueInfo) {
		if (s.value.valid) {
			totalDiskUsageBytes += s.value.lastReply.storageBytes.used;
		}
	}

	if (now() - self->lastSSListFetchedTimestamp > SERVER_KNOBS->STORAGE_SERVER_LIST_FETCH_TIMEOUT) {
		limits->tpsLimit = 0.0;
		limitReason = limitReason_t::storage_server_list_fetch_failed;
		reasonID = UID();
		TraceEvent(SevWarnAlways, "RkSSListFetchTimeout", self->id).suppressFor(1.0);
	} else if (limits->tpsLimit == std::numeric_limits<double>::infinity()) {
		limits->tpsLimit = SERVER_KNOBS->RATEKEEPER_DEFAULT_LIMIT;
	}

	limits->tpsLimitMetric = std::min(limits->tpsLimit, 1e6);
	limits->reasonMetric = limitReason;

	if (deterministicRandom()->random01() < 0.1) {
		std::string name = "RkUpdate" + limits->context;
		TraceEvent(name.c_str(), self->id)
		    .detail("TPSLimit", limits->tpsLimit)
		    .detail("Reason", limitReason)
		    .detail("ReasonServerID", reasonID == UID() ? std::string() : Traceable<UID>::toString(reasonID))
		    .detail("ReleasedTPS", self->smoothReleasedTransactions.smoothRate())
		    .detail("ReleasedBatchTPS", self->smoothBatchReleasedTransactions.smoothRate())
		    .detail("TPSBasis", actualTps)
		    .detail("StorageServers", sscount)
		    .detail("GrvProxies", self->grvProxyInfo.size())
		    .detail("TLogs", tlcount)
		    .detail("WorstFreeSpaceStorageServer", worstFreeSpaceStorageServer)
		    .detail("WorstFreeSpaceTLog", worstFreeSpaceTLog)
		    .detail("WorstStorageServerQueue", worstStorageQueueStorageServer)
		    .detail("LimitingStorageServerQueue", limitingStorageQueueStorageServer)
		    .detail("WorstTLogQueue", worstStorageQueueTLog)
		    .detail("TotalDiskUsageBytes", totalDiskUsageBytes)
		    .detail("WorstStorageServerVersionLag", worstVersionLag)
		    .detail("LimitingStorageServerVersionLag", limitingVersionLag)
		    .detail("WorstStorageServerDurabilityLag", worstDurabilityLag)
		    .detail("LimitingStorageServerDurabilityLag", limitingDurabilityLag)
		    .detail("TagsAutoThrottled", self->throttledTags.autoThrottleCount())
		    .detail("TagsAutoThrottledBusyRead", self->throttledTags.busyReadTagCount)
		    .detail("TagsAutoThrottledBusyWrite", self->throttledTags.busyWriteTagCount)
		    .detail("TagsManuallyThrottled", self->throttledTags.manualThrottleCount())
		    .detail("AutoThrottlingEnabled", self->autoThrottlingEnabled)
		    .trackLatest(name);
	}
}

static void updateCommitCostEstimation(RatekeeperData* self,
                                       UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation) {
	for (auto it = self->storageQueueInfo.begin(); it != self->storageQueueInfo.end(); ++it) {
		auto tagCostIt = costEstimation.find(it->key);
		if (tagCostIt == costEstimation.end())
			continue;
		for (const auto& [tagName, cost] : tagCostIt->second) {
			it->value.tagCostEst[tagName] += cost;
			it->value.totalWriteCosts += cost.getCostSum();
			it->value.totalWriteOps += cost.getOpsSum();
		}
	}
}

ACTOR Future<Void> configurationMonitor(RatekeeperData* self) {
	loop {
		state ReadYourWritesTransaction tr(self->db);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				RangeResult results = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

				self->configuration.fromKeyValues((VectorRef<KeyValueRef>)results);

				state Future<Void> watchFuture =
				    tr.watch(moveKeysLockOwnerKey) || tr.watch(excludedServersVersionKey) ||
				    tr.watch(failedServersVersionKey) || tr.watch(excludedLocalityVersionKey) ||
				    tr.watch(failedLocalityVersionKey);
				wait(tr.commit());
				wait(watchFuture);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

//  |-------------------------------------|
//  |         Blob Granule Stuff          |
//  |-------------------------------------|

// TODO might need to use IBackupFile instead of blob store interface to support non-s3 things like azure?
struct GranuleMetadata : NonCopyable, ReferenceCounted<GranuleMetadata> {
	std::deque<std::pair<Version, std::string>> snapshotFiles;
	std::deque<std::pair<Version, std::string>> deltaFiles;
	GranuleDeltas currentDeltas;
	uint64_t bytesInNewDeltaFiles = 0;
	Version lastWriteVersion = 0;
	uint64_t currentDeltaBytes = 0;
	Arena deltaArena;

	KeyRange keyRange;
	Future<Void> rangeFeedFuture;
	Future<Void> fileUpdaterFuture;
	PromiseStream<MutationAndVersion> rangeFeed;
	PromiseStream<Version> snapshotVersions;

	// FIXME: right now there is a dependency because this contains both the actual file/delta data as well as the
	// metadata (worker futures), so removing this reference from the map doesn't actually cancel the workers. It'd be
	// better to have this in 2 separate objects, where the granule metadata map has the futures, but the read
	// queries/file updater/range feed only copy the reference to the file/delta data.
	void cancel() {
		rangeFeedFuture = Never();
		fileUpdaterFuture = Never();
	}

	~GranuleMetadata() {
		// only print for "active" metadata
		if (lastWriteVersion != 0) {
			printf("Destroying granule metadata for [%s - %s)\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str());
		}
	}
};

struct BlobWorkerData {
	UID id;
	LocalityData locality;

	Reference<S3BlobStoreEndpoint> bstore;
	std::string bucket;

	KeyRangeMap<Reference<GranuleMetadata>> granuleMetadata;
	PromiseStream<KeyRange> assignRange;
	PromiseStream<KeyRange> revokeRange;

	~BlobWorkerData() { printf("Destroying blob worker data for %s\n", id.toString().c_str()); }
};

// TODO add granule locks
ACTOR Future<std::pair<Version, std::string>> writeDeltaFile(RatekeeperData* rkData,
                                                             BlobWorkerData* bwData,
                                                             KeyRange keyRange,
                                                             GranuleDeltas const* deltasToWrite) {

	// TODO some sort of directory structure would be useful?
	state Version lastVersion = deltasToWrite->back().v;
	state std::string fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
	                          std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(lastVersion) +
	                          ".delta";

	state Value serialized = ObjectWriter::toValue(*deltasToWrite, Unversioned());

	// write to s3 using multi part upload
	state Reference<AsyncFileS3BlobStoreWrite> objectFile =
	    makeReference<AsyncFileS3BlobStoreWrite>(bwData->bstore, bwData->bucket, fname);
	wait(objectFile->write(serialized.begin(), serialized.size(), 0));
	wait(objectFile->sync());

	// TODO read file back here if still problems

	// update FDB with new file
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(rkData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	loop {
		try {
			Tuple deltaFileKey;
			deltaFileKey.append(keyRange.begin).append(keyRange.end);
			deltaFileKey.append(LiteralStringRef("delta")).append(lastVersion);
			tr->set(deltaFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin), fname);

			wait(tr->commit());
			printf("blob worker updated fdb with delta file %s of size %d at version %lld\n",
			       fname.c_str(),
			       serialized.size(),
			       lastVersion);
			return std::pair<Version, std::string>(lastVersion, fname);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<std::pair<Version, std::string>> dumpSnapshotFromFDB(RatekeeperData* rkData,
                                                                  BlobWorkerData* bwData,
                                                                  KeyRange keyRange) {
	printf("Dumping snapshot from FDB for [%s - %s)\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str());
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(rkData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

	loop {
		state std::string fname = "";
		try {
			state Version readVersion = wait(tr->getReadVersion());
			fname = deterministicRandom()->randomUniqueID().toString() + "_T" +
			        std::to_string((uint64_t)(1000.0 * now())) + "_V" + std::to_string(readVersion) + ".snapshot";

			// TODO some sort of directory structure would be useful?
			state Arena arena;
			state GranuleSnapshot allRows;

			// TODO would be fairly easy to change this to a promise stream, and optionally build from blobGranuleReader
			// instead
			state Key beginKey = keyRange.begin;
			loop {
				// TODO knob for limit?
				RangeResult res = wait(tr->getRange(KeyRangeRef(beginKey, keyRange.end), 1000));
				/*printf("granule [%s - %s) read %d%s rows\n",
				       keyRange.begin.printable().c_str(),
				       keyRange.end.printable().c_str(),
				       res.size(),
				       res.more ? "+" : "");*/
				arena.dependsOn(res.arena());
				allRows.append(arena, res.begin(), res.size());
				if (res.more) {
					beginKey = keyAfter(res.back().key);
				} else {
					break;
				}
			}

			printf("Granule [%s- %s) read %d snapshot rows from fdb\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       allRows.size());
			if (allRows.size() < 10) {
				for (auto& row : allRows) {
					printf("  %s=%s\n", row.key.printable().c_str(), row.value.printable().c_str());
				}
			}
			// TODO REMOVE sanity check!

			for (int i = 0; i < allRows.size() - 1; i++) {
				if (allRows[i].key >= allRows[i + 1].key) {
					printf("SORT ORDER VIOLATION IN SNAPSHOT FILE: %s, %s\n",
					       allRows[i].key.printable().c_str(),
					       allRows[i + 1].key.printable().c_str());
				}
				ASSERT(allRows[i].key < allRows[i + 1].key);
			}

			// TODO is this easy to read as a flatbuffer from reader? Need to be sure about this data format
			state Value serialized = ObjectWriter::toValue(allRows, Unversioned());

			// write to s3 using multi part upload
			state Reference<AsyncFileS3BlobStoreWrite> objectFile =
			    makeReference<AsyncFileS3BlobStoreWrite>(bwData->bstore, bwData->bucket, fname);
			wait(objectFile->write(serialized.begin(), serialized.size(), 0));
			wait(objectFile->sync());

			// TODO could move this into separate txn to avoid the timeout, it'll need to be separate later anyway
			// object uploaded successfully, save it to system key space (TODO later - and memory file history)
			// TODO add conflict range for writes?
			Tuple snapshotFileKey;
			snapshotFileKey.append(keyRange.begin).append(keyRange.end);
			snapshotFileKey.append(LiteralStringRef("snapshot")).append(readVersion);
			tr->set(snapshotFileKey.getDataAsStandalone().withPrefix(blobGranuleFileKeys.begin), fname);
			wait(tr->commit());
			printf("Granule [%s - %s) committed new snapshot file %s with %d bytes for range\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       fname.c_str(),
			       serialized.size());
			return std::pair<Version, std::string>(readVersion, fname);
		} catch (Error& e) {
			// TODO REMOVE
			printf("dump range txn got error %s\n", e.name());
			if (fname != "") {
				// TODO delete unsuccessfully written file
				bwData->bstore->deleteObject(bwData->bucket, fname);
				printf("deleting s3 object %s\n", fname.c_str());
			}
			wait(tr->onError(e));
		}
	}
}

// updater for a single granule
ACTOR Future<Void> blobGranuleUpdateFiles(RatekeeperData* rkData,
                                          BlobWorkerData* bwData,
                                          Reference<GranuleMetadata> metadata) {

	try {
		std::pair<Version, std::string> newSnapshotFile = wait(dumpSnapshotFromFDB(rkData, bwData, metadata->keyRange));
		metadata->snapshotFiles.push_back(newSnapshotFile);
		metadata->lastWriteVersion = newSnapshotFile.first;
		metadata->snapshotVersions.send(newSnapshotFile.first);

		loop {
			MutationAndVersion delta = waitNext(metadata->rangeFeed.getFuture());
			metadata->currentDeltas.push_back(metadata->deltaArena, delta);
			// 8 for version, 1 for type, 4 for each param length then actual param size
			metadata->currentDeltaBytes += 17 + delta.m.param1.size() + delta.m.param2.size();

			if (metadata->currentDeltaBytes >= SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES &&
			    metadata->currentDeltas.back().v > metadata->lastWriteVersion) {
				printf("Granule [%s - %s) flushing delta file after %d bytes\n",
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       metadata->currentDeltaBytes);
				std::pair<Version, std::string> newDeltaFile =
				    wait(writeDeltaFile(rkData, bwData, metadata->keyRange, &metadata->currentDeltas));

				// add new delta file
				metadata->deltaFiles.push_back(newDeltaFile);
				metadata->lastWriteVersion = newDeltaFile.first;
				metadata->bytesInNewDeltaFiles += metadata->currentDeltaBytes;

				// reset current deltas
				metadata->deltaArena = Arena();
				metadata->currentDeltas = GranuleDeltas();
				metadata->currentDeltaBytes = 0;
			}

			if (metadata->bytesInNewDeltaFiles >= SERVER_KNOBS->BG_DELTA_BYTES_BEFORE_COMPACT) {
				printf("Granule [%s - %s) compacting after %d bytes\n",
				       metadata->keyRange.begin.printable().c_str(),
				       metadata->keyRange.end.printable().c_str(),
				       metadata->bytesInNewDeltaFiles);
				// FIXME: instead of just doing new snapshot, it should offer shard back to blob manager and get
				// reassigned
				// FIXME: this should read previous snapshot + delta files instead, unless it knows it's really small or
				// there was a huge clear or something
				std::pair<Version, std::string> newSnapshotFile =
				    wait(dumpSnapshotFromFDB(rkData, bwData, metadata->keyRange));

				// add new snapshot file
				metadata->snapshotFiles.push_back(newSnapshotFile);
				metadata->lastWriteVersion = newSnapshotFile.first;
				metadata->snapshotVersions.send(newSnapshotFile.first);

				// reset metadata
				metadata->bytesInNewDeltaFiles = 0;
			}
		}
	} catch (Error& e) {
		printf("Granule file updater for [%s - %s) got error %s, exiting\n",
		       metadata->keyRange.begin.printable().c_str(),
		       metadata->keyRange.end.printable().c_str(),
		       e.name());
		throw e;
	}
}

static void handleBlobGranuleFileRequest(BlobWorkerData* wkData, const BlobGranuleFileRequest& req) {
	BlobGranuleFileReply rep;

	auto checkRanges = wkData->granuleMetadata.intersectingRanges(req.keyRange);
	// check for gaps as errors before doing actual data copying
	KeyRef lastRangeEnd = req.keyRange.begin;
	for (auto& r : checkRanges) {
		if (lastRangeEnd < r.begin()) {
			printf("No blob data for [%s - %s) in request range [%s - %s), skipping request\n",
			       lastRangeEnd.printable().c_str(),
			       r.begin().printable().c_str(),
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str());
			req.reply.sendError(transaction_too_old());
			return;
		}
		if (!r.value().isValid()) {
			printf("No valid blob data for [%s - %s) in request range [%s - %s), skipping request\n",
			       lastRangeEnd.printable().c_str(),
			       r.begin().printable().c_str(),
			       req.keyRange.begin.printable().c_str(),
			       req.keyRange.end.printable().c_str());
			req.reply.sendError(transaction_too_old());
			return;
		}
		lastRangeEnd = r.end();
	}
	if (lastRangeEnd < req.keyRange.end) {
		printf("No blob data for [%s - %s) in request range [%s - %s), skipping request\n",
		       lastRangeEnd.printable().c_str(),
		       req.keyRange.end.printable().c_str(),
		       req.keyRange.begin.printable().c_str(),
		       req.keyRange.end.printable().c_str());
		req.reply.sendError(transaction_too_old());
		return;
	}

	printf("BW processing blob granule request for [%s - %s)\n",
	       req.keyRange.begin.printable().c_str(),
	       req.keyRange.end.printable().c_str());

	// do work for each range
	auto requestRanges = wkData->granuleMetadata.intersectingRanges(req.keyRange);
	for (auto& r : requestRanges) {
		Reference<GranuleMetadata> metadata = r.value();
		// FIXME: eventually need to handle waiting for granule's committed version to catch up to the request version
		// before copying mutations into reply's arena, to ensure read isn't stale
		BlobGranuleChunk chunk;
		chunk.keyRange =
		    KeyRangeRef(StringRef(rep.arena, metadata->keyRange.begin), StringRef(rep.arena, r.value()->keyRange.end));

		// handle snapshot files
		int i = metadata->snapshotFiles.size() - 1;
		while (i >= 0 && metadata->snapshotFiles[i].first > req.readVersion) {
			i--;
		}
		// if version is older than oldest snapshot file (or no snapshot files), throw too old
		// FIXME: probably want a dedicated exception like blob_range_too_old or something instead
		if (i < 0) {
			req.reply.sendError(transaction_too_old());
			return;
		}
		chunk.snapshotFileName = StringRef(rep.arena, metadata->snapshotFiles[i].second);
		Version snapshotVersion = metadata->snapshotFiles[i].first;

		// handle delta files
		i = metadata->deltaFiles.size() - 1;
		// skip delta files that are too new
		while (i >= 0 && metadata->deltaFiles[i].first > req.readVersion) {
			i--;
		}
		if (i < metadata->deltaFiles.size() - 1) {
			i++;
		}
		// only include delta files after the snapshot file
		int j = i;
		while (j >= 0 && metadata->deltaFiles[j].first > snapshotVersion) {
			j--;
		}
		j++;
		while (j <= i) {
			chunk.deltaFileNames.push_back_deep(rep.arena, metadata->deltaFiles[j].second);
			j++;
		}

		// new deltas (if version is larger than version of last delta file)
		// FIXME: do trivial key bounds here if key range is not fully contained in request key range
		if (!metadata->deltaFiles.size() || req.readVersion >= metadata->deltaFiles.back().first) {
			rep.arena.dependsOn(metadata->deltaArena);
			for (auto& delta : metadata->currentDeltas) {
				if (delta.v <= req.readVersion) {
					chunk.newDeltas.push_back_deep(rep.arena, delta);
				}
			}
		}

		rep.chunks.push_back(rep.arena, chunk);

		// TODO yield?
	}
	req.reply.send(rep);
}

// dumb series of mutations that just sets/clears same key that is unrelated to actual db transactions
ACTOR Future<Void> fakeRangeFeed(PromiseStream<MutationAndVersion> mutationStream,
                                 PromiseStream<Version> snapshotVersions,
                                 KeyRange keyRange) {
	state Version version = waitNext(snapshotVersions.getFuture());
	state uint32_t targetKbPerSec = (uint32_t)(SERVER_KNOBS->BG_DELTA_FILE_TARGET_BYTES) / 10;
	state Arena arena;
	printf("Fake range feed got initial version %lld\n", version);
	loop {
		// dumb series of mutations that just sets/clears same key that is unrelated to actual db transactions
		state uint32_t bytesGenerated = 0;
		state uint32_t targetKbThisSec =
		    targetKbPerSec / 2 + (uint32_t)(deterministicRandom()->random01() * targetKbPerSec);
		state uint32_t mutationsGenerated = 0;
		while (bytesGenerated < targetKbThisSec) {
			MutationAndVersion update;
			update.v = version;
			update.m.param1 = keyRange.begin;
			update.m.param2 = keyRange.begin;
			if (deterministicRandom()->random01() < 0.5) {
				// clear start key
				update.m.type = MutationRef::Type::ClearRange;
			} else {
				// set
				update.m.type = MutationRef::Type::SetValue;
			}
			mutationsGenerated++;
			bytesGenerated += 17 + 2 * keyRange.begin.size();
			mutationStream.send(update);

			// simulate multiple mutations with same version (TODO: this should be possible right)
			if (deterministicRandom()->random01() < 0.4) {
				version++;
			}
			if (mutationsGenerated % 1000 == 0) {
				wait(yield());
			}
		}

		// printf("Fake range feed generated %d mutations at version %lld\n", mutationsGenerated, version);

		choose {
			when(wait(delay(1.0))) {
				// slightly slower than real versions, to try to ensure it doesn't get ahead
				version += 950000;
			}
			when(Version _v = waitNext(snapshotVersions.getFuture())) {
				if (_v > version) {
					printf("updating fake range feed from %lld to snapshot version %lld\n", version, _v);
					version = _v;
				} else {
					printf("snapshot version %lld was ahead of fake range feed version %lld, keeping fake version\n",
					       _v,
					       version);
				}
			}
		}
	}
}

static Reference<GranuleMetadata> constructNewBlobRange(RatekeeperData* rkData,
                                                        BlobWorkerData* wkData,
                                                        KeyRange keyRange) {
	printf("Creating new worker metadata for range [%s - %s)\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str());
	Reference<GranuleMetadata> newMetadata = makeReference<GranuleMetadata>();
	newMetadata->keyRange = keyRange;
	newMetadata->rangeFeedFuture = fakeRangeFeed(newMetadata->rangeFeed, newMetadata->snapshotVersions, keyRange);
	newMetadata->fileUpdaterFuture = blobGranuleUpdateFiles(rkData, wkData, newMetadata);

	return newMetadata;
}

// Any ranges that were purely contained by this range are cancelled. If this intersects but does not fully contain
// any existing range(s), it will restart them at the new cutoff points
static void changeBlobRange(RatekeeperData* rkData,
                            BlobWorkerData* wkData,
                            KeyRange keyRange,
                            Reference<GranuleMetadata> newMetadata) {
	printf("Changing range for [%s - %s): %s\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str(),
	       newMetadata.isValid() ? "T" : "F");

	// if any of these ranges are active, cancel them.
	// if the first or last range was set, and this key range insertion truncates but does not completely replace them,
	// restart the truncated ranges
	// tricker because this is an iterator, so we don't know size up front
	int i = 0;
	Key firstRangeStart;
	bool firstRangeActive = false;
	Key lastRangeEnd;
	bool lastRangeActive = false;

	auto ranges = wkData->granuleMetadata.intersectingRanges(keyRange);
	for (auto& r : ranges) {
		lastRangeEnd = r.end();
		lastRangeActive = r.value().isValid();
		if (i == 0) {
			firstRangeStart = r.begin();
			firstRangeActive = lastRangeActive;
		}
		if (lastRangeActive) {
			// cancel actors for old range and clear reference
			printf("  [%s - %s): T (cancelling)\n", r.begin().printable().c_str(), r.end().printable().c_str());
			r.value()->cancel();
			r.value().clear();
		} else {
			printf("  [%s - %s):F\n", r.begin().printable().c_str(), r.end().printable().c_str());
		}
		i++;
	}

	wkData->granuleMetadata.insert(keyRange, newMetadata);

	if (firstRangeActive && firstRangeStart < keyRange.begin) {
		printf("    Truncated first range [%s - %s)\n",
		       firstRangeStart.printable().c_str(),
		       keyRange.begin.printable().c_str());
		// this should modify exactly one range so it's not so bad. A bug here could lead to infinite recursion
		// though
		KeyRangeRef newRange = KeyRangeRef(firstRangeStart, keyRange.begin);
		changeBlobRange(rkData, wkData, newRange, constructNewBlobRange(rkData, wkData, newRange));
	}

	if (lastRangeActive && keyRange.end < lastRangeEnd) {
		printf(
		    "    Truncated last range [%s - %s)\n", keyRange.end.printable().c_str(), lastRangeEnd.printable().c_str());
		// this should modify exactly one range so it's not so bad. A bug here could lead to infinite recursion
		// though
		KeyRangeRef newRange = KeyRangeRef(keyRange.end, lastRangeEnd);
		changeBlobRange(rkData, wkData, newRange, constructNewBlobRange(rkData, wkData, newRange));
	}

	printf("Final result after inserting [%s - %s): %s\n",
	       keyRange.begin.printable().c_str(),
	       keyRange.end.printable().c_str(),
	       newMetadata.isValid() ? "T" : "F");

	auto rangesFinal = wkData->granuleMetadata.intersectingRanges(keyRange);
	for (auto& r : rangesFinal) {
		printf("  [%s - %s): %s\n",
		       r.begin().printable().c_str(),
		       r.end().printable().c_str(),
		       r.value().isValid() ? "T" : "F");
	}

	// don't do this in above loop because modifying range map changes iterator

	// auto ranges = wkData->granuleMetadata.getAffectedRangesAfterInsertion(keyRange, Reference<GranuleMetadata>());
	// auto rangeIt = wkData->granuleMetadata.intersectingRanges(keyRange);
	// copy these because inserting/updating ranges will change the range map
	/*std::vector<KeyRange> ranges;
	printf("Intersecting ranges:\n");
	for (auto& r : rangeIt) {
	    printf("  [%s - %s): %s\n",
	           r.begin().printable().c_str(),
	           r.end().printable().c_str(),
	           r.value().isValid() ? "T" : "F");
	    ranges.push_back(KeyRange(KeyRangeRef(r.begin(), r.end())));
	}
	for (auto& r : ranges) {
	    if (r.value().isValid()) {
	        // cancel actors for old range
	        printf("  Cancelling range [%s - %s)\n", r.begin().printable().c_str(), r.end().printable().c_str());
	        r.value()->cancel();
	        // delete old granule metadata
	        wkData->granuleMetadata.insert(r.range(), Reference<GranuleMetadata>());

	        if (r.begin() < keyRange.begin) {
	            printf("  Truncating granule range [%s - %s) to [%s - %s)\n",
	                   r.begin().printable().c_str(),
	                   r.end().printable().c_str(),
	                   r.begin().printable().c_str(),
	                   keyRange.begin.printable().c_str());
	            // this should modify exactly one range so it's not so bad. A bug here could lead to infinite recursion
	            // though
	            KeyRangeRef newRange = KeyRangeRef(r.begin(), keyRange.begin);
	            changeBlobRange(rkData, wkData, newRange, constructNewBlobRange(rkData, wkData, newRange));
	        }

	        if (keyRange.end < r.end()) {
	            printf("  Truncating granule range [%s - %s) to [%s - %s)\n",
	                   r.begin().printable().c_str(),
	                   r.end().printable().c_str(),
	                   r.end().printable().c_str(),
	                   r.end().printable().c_str());
	            // this should modify exactly one range so it's not so bad. A bug here could lead to infinite recursion
	            // though
	            KeyRangeRef newRange = KeyRangeRef(keyRange.end, r.end());
	            changeBlobRange(rkData, wkData, newRange, constructNewBlobRange(rkData, wkData, newRange));
	        }

	    } else {
	        printf("  Ignoring non-set worker range [%s - %s)\n",
	               r.begin().printable().c_str(),
	               r.end().printable().c_str());
	    }
	}
	wkData->granuleMetadata.insert(keyRange, newMetadata);*/
}

static void handleAssignedRange(RatekeeperData* rkData, BlobWorkerData* wkData, KeyRange keyRange) {
	changeBlobRange(rkData, wkData, keyRange, constructNewBlobRange(rkData, wkData, keyRange));
}

static void handleRevokedRange(RatekeeperData* rkData, BlobWorkerData* wkData, KeyRange keyRange) {
	changeBlobRange(rkData, wkData, keyRange, Reference<GranuleMetadata>());
}

ACTOR Future<Void> registerBlobWorker(RatekeeperData* rkData, BlobWorkerInterface interf) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(rkData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {
			Key blobWorkerListKey = blobWorkerListKeyFor(interf.id());
			tr->addReadConflictRange(singleKeyRange(blobWorkerListKey));
			tr->set(blobWorkerListKey, blobWorkerListValue(interf));

			wait(tr->commit());

			printf("Registered blob worker %s\n", interf.id().toString().c_str());
			return Void();
		} catch (Error& e) {
			printf("Registering blob worker %s got error %s\n", interf.id().toString().c_str(), e.name());
			wait(tr->onError(e));
		}
	}
}

// TODO list of key ranges in the future to batch
ACTOR Future<Void> persistAssignWorkerRange(RatekeeperData* rkData, KeyRange keyRange, UID workerID) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(rkData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {

			wait(krmSetRangeCoalescing(
			    tr, blobGranuleMappingKeys.begin, keyRange, KeyRange(allKeys), blobGranuleMappingValueFor(workerID)));

			wait(tr->commit());

			printf("Blob worker %s persisted key range [%s - %s)\n",
			       workerID.toString().c_str(),
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str());
			return Void();
		} catch (Error& e) {
			printf("Persisting key range [%s - %s) for blob worker %s got error %s\n",
			       keyRange.begin.printable().c_str(),
			       keyRange.end.printable().c_str(),
			       workerID.toString().c_str(),
			       e.name());
			wait(tr->onError(e));
		}
	}
}

// TODO need to version assigned ranges with read version of txn the range was read and use that in
// handleAssigned/Revoked from to prevent out-of-order assignments/revokes from the blob manager from getting ranges in
// an incorrect state
ACTOR Future<Void> blobWorkerCore(RatekeeperData* rkData, BlobWorkerData* bwData) {
	printf("Blob worker starting for bucket %s\n", bwData->bucket.c_str());

	state BlobWorkerInterface interf(bwData->locality, bwData->id);
	interf.initEndpoints();

	wait(registerBlobWorker(rkData, interf));

	state PromiseStream<Future<Void>> addActor;
	state Future<Void> collection = actorCollection(addActor.getFuture());

	addActor.send(waitFailureServer(interf.waitFailure.getFuture()));

	try {
		loop choose {
			when(BlobGranuleFileRequest req = waitNext(interf.blobGranuleFileRequest.getFuture())) {
				printf("Got blob granule request [%s - %s)\n",
				       req.keyRange.begin.printable().c_str(),
				       req.keyRange.end.printable().c_str());
				handleBlobGranuleFileRequest(bwData, req);
			}
			when(KeyRange _rangeToAdd = waitNext(bwData->assignRange.getFuture())) {
				state KeyRange rangeToAdd = _rangeToAdd;
				printf("Worker %s assigned range [%s - %s)\n",
				       bwData->id.toString().c_str(),
				       rangeToAdd.begin.printable().c_str(),
				       rangeToAdd.end.printable().c_str());

				wait(persistAssignWorkerRange(rkData, rangeToAdd, bwData->id));

				handleAssignedRange(rkData, bwData, rangeToAdd);
			}
			when(KeyRange rangeToRemove = waitNext(bwData->revokeRange.getFuture())) {
				printf("Worker %s revoked range [%s - %s)\n",
				       bwData->id.toString().c_str(),
				       rangeToRemove.begin.printable().c_str(),
				       rangeToRemove.end.printable().c_str());
				handleRevokedRange(rkData, bwData, rangeToRemove);
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& e) {
		printf("Blob worker got error %s, exiting\n", e.name());
		TraceEvent("BlobWorkerDied", interf.id()).error(e, true);
	}

	return Void();
}

// TODO REMOVE eventually
ACTOR Future<Void> nukeBlobWorkerData(RatekeeperData* rkData) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(rkData->db);
	tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	loop {
		try {
			tr->clear(blobWorkerListKeys);
			tr->clear(blobGranuleMappingKeys);

			return Void();
		} catch (Error& e) {
			printf("Nuking blob worker data got error %s\n", e.name());
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Standalone<VectorRef<KeyRef>>> splitNewRange(Reference<ReadYourWritesTransaction> tr, KeyRange range) {
	// TODO is it better to just pass empty metrics to estimated?
	// TODO handle errors here by pulling out into its own transaction instead of the main loop's transaction, and
	// retrying
	printf("Splitting new range [%s - %s)\n", range.begin.printable().c_str(), range.end.printable().c_str());
	StorageMetrics estimated = wait(tr->getTransaction().getStorageMetrics(range, CLIENT_KNOBS->TOO_MANY));

	printf("Estimated bytes for [%s - %s): %lld\n",
	       range.begin.printable().c_str(),
	       range.end.printable().c_str(),
	       estimated.bytes);

	if (estimated.bytes > SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES) {
		printf("  Splitting range\n");
		// only split on bytes
		StorageMetrics splitMetrics;
		splitMetrics.bytes = SERVER_KNOBS->BG_SNAPSHOT_FILE_TARGET_BYTES;
		splitMetrics.bytesPerKSecond = splitMetrics.infinity;
		splitMetrics.iosPerKSecond = splitMetrics.infinity;
		splitMetrics.bytesReadPerKSecond = splitMetrics.infinity; // Don't split by readBandwidth

		Standalone<VectorRef<KeyRef>> keys =
		    wait(tr->getTransaction().splitStorageMetrics(range, splitMetrics, estimated));
		return keys;
	} else {
		printf("  Not splitting range\n");
		Standalone<VectorRef<KeyRef>> keys;
		keys.push_back_deep(keys.arena(), range.begin);
		keys.push_back_deep(keys.arena(), range.end);
		return keys;
	}
}

// TODO MOVE ELSEWHERE
ACTOR Future<Void> blobManagerPoc(RatekeeperData* rkData, LocalityData locality) {
	state KeyRangeMap<bool> knownBlobRanges(false, normalKeys.end);

	state BlobWorkerData bwData;
	bwData.id = deterministicRandom()->randomUniqueID();
	bwData.locality = locality;
	bwData.bucket = SERVER_KNOBS->BG_BUCKET;

	printf("Initializing blob manager s3 stuff\n");
	try {
		printf("constructing s3blobstoreendpoint from %s\n", SERVER_KNOBS->BG_URL.c_str());
		bwData.bstore = S3BlobStoreEndpoint::fromString(SERVER_KNOBS->BG_URL);
		printf("checking if bucket %s exists\n", bwData.bucket.c_str());
		bool bExists = wait(bwData.bstore->bucketExists(bwData.bucket));
		if (!bExists) {
			printf("Bucket %s does not exist!\n", bwData.bucket.c_str());
			return Void();
		}
	} catch (Error& e) {
		printf("Blob manager got s3 init error %s\n", e.name());
		return Void();
	}

	// TODO remove once we have persistence + failure detection
	printf("Blob manager nuking previous workers and range assignments on startup\n");
	wait(nukeBlobWorkerData(rkData));
	printf("Blob manager nuked previous workers and range assignments\n");

	state Future<Void> blobWorker = blobWorkerCore(rkData, &bwData);

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(rkData->db);

		printf("Blob manager checking for range updates\n");
		loop {
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				// TODO probably knobs here? This should always be pretty small though
				RangeResult results = wait(krmGetRanges(
				    tr, blobRangeKeys.begin, KeyRange(normalKeys), 10000, GetRangeLimits::BYTE_LIMIT_UNLIMITED));
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

				state Arena ar;
				ar.dependsOn(results.arena());
				VectorRef<KeyRangeRef> rangesToAdd;
				VectorRef<KeyRangeRef> rangesToRemove;
				// TODO hack for simulation
				if (g_network->isSimulated()) {
					printf("Hacking blob ranges!\n");
					RangeResult fakeResults;
					KeyValueRef one =
					    KeyValueRef(StringRef(ar, LiteralStringRef("\x01")), StringRef(ar, LiteralStringRef("1")));
					KeyValueRef two = KeyValueRef(StringRef(ar, LiteralStringRef("\x02")), StringRef());
					fakeResults.push_back(fakeResults.arena(), one);
					fakeResults.push_back(fakeResults.arena(), two);
					updateClientBlobRanges(&knownBlobRanges, fakeResults, ar, &rangesToAdd, &rangesToRemove);
				} else {
					updateClientBlobRanges(&knownBlobRanges, results, ar, &rangesToAdd, &rangesToRemove);
				}

				for (KeyRangeRef range : rangesToRemove) {
					printf("BM Got range to revoke [%s - %s)\n",
					       range.begin.printable().c_str(),
					       range.end.printable().c_str());
					bwData.revokeRange.send(KeyRange(range));
				}

				state std::vector<Future<Standalone<VectorRef<KeyRef>>>> splitFutures;
				// Divide new ranges up into equal chunks by using SS byte sample
				for (KeyRangeRef range : rangesToAdd) {
					printf("BM Got range to add [%s - %s)\n",
					       range.begin.printable().c_str(),
					       range.end.printable().c_str());
					splitFutures.push_back(splitNewRange(tr, range));
				}

				for (auto f : splitFutures) {
					Standalone<VectorRef<KeyRef>> splits = wait(f);
					printf("Split client range [%s - %s) into %d ranges:\n",
					       splits[0].printable().c_str(),
					       splits[splits.size() - 1].printable().c_str(),
					       splits.size() - 1);
					for (int i = 0; i < splits.size() - 1; i++) {
						KeyRange range = KeyRange(KeyRangeRef(splits[i], splits[i + 1]));
						printf("    [%s - %s)\n", range.begin.printable().c_str(), range.end.printable().c_str());
						bwData.assignRange.send(range);
					}
				}

				state Future<Void> watchFuture = tr->watch(blobRangeChangeKey);
				wait(tr->commit());
				printf("Blob manager done processing client ranges, awaiting update\n");
				wait(watchFuture);
				break;
			} catch (Error& e) {
				printf("Blob manager got error looking for range updates %s\n", e.name());
				wait(tr->onError(e));
			}
		}
	}
}

ACTOR Future<Void> ratekeeper(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	state RatekeeperData self(rkInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));
	state Future<Void> timeout = Void();
	state std::vector<Future<Void>> tlogTrackers;
	state std::vector<TLogInterface> tlogInterfs;
	state Promise<Void> err;
	state Future<Void> collection = actorCollection(self.addActor.getFuture());

	TraceEvent("RatekeeperStarting", rkInterf.id());
	self.addActor.send(waitFailureServer(rkInterf.waitFailure.getFuture()));
	self.addActor.send(configurationMonitor(&self));

	PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges;
	self.addActor.send(monitorServerListChange(&self, serverChanges));
	self.addActor.send(trackEachStorageServer(&self, serverChanges.getFuture()));
	self.addActor.send(traceRole(Role::RATEKEEPER, rkInterf.id()));

	self.addActor.send(monitorThrottlingChanges(&self));
	RatekeeperData* selfPtr = &self; // let flow compiler capture self
	self.addActor.send(
	    recurring([selfPtr]() { refreshStorageServerCommitCost(selfPtr); }, SERVER_KNOBS->TAG_MEASUREMENT_INTERVAL));

	// TODO MOVE eventually
	if (SERVER_KNOBS->BG_URL == "" || SERVER_KNOBS->BG_BUCKET == "") {
		printf("not starting blob manager poc, no url/bucket configured\n");
	} else {
		printf("Starting blob manager with url=%s and bucket=%s\n",
		       SERVER_KNOBS->BG_URL.c_str(),
		       SERVER_KNOBS->BG_BUCKET.c_str());
		self.addActor.send(blobManagerPoc(&self, rkInterf.locality));
	}

	TraceEvent("RkTLogQueueSizeParameters", rkInterf.id())
	    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_TLOG)
	    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_TLOG)
	    .detail("Rate",
	            (SERVER_KNOBS->TARGET_BYTES_PER_TLOG - SERVER_KNOBS->SPRING_BYTES_TLOG) /
	                ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
	                 2.0));

	TraceEvent("RkStorageServerQueueSizeParameters", rkInterf.id())
	    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER)
	    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER)
	    .detail("EBrake", SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES)
	    .detail("Rate",
	            (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER - SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER) /
	                ((((double)SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) / SERVER_KNOBS->VERSIONS_PER_SECOND) +
	                 2.0));

	tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
	tlogTrackers.reserve(tlogInterfs.size());
	for (int i = 0; i < tlogInterfs.size(); i++) {
		tlogTrackers.push_back(splitError(trackTLogQueueInfo(&self, tlogInterfs[i]), err));
	}

	self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();

	try {
		state bool lastLimited = false;
		loop choose {
			when(wait(timeout)) {
				updateRate(&self, &self.normalLimits);
				updateRate(&self, &self.batchLimits);

				lastLimited = self.smoothReleasedTransactions.smoothRate() >
				              SERVER_KNOBS->LAST_LIMITED_RATIO * self.batchLimits.tpsLimit;
				double tooOld = now() - 1.0;
				for (auto p = self.grvProxyInfo.begin(); p != self.grvProxyInfo.end();) {
					if (p->second.lastUpdateTime < tooOld)
						p = self.grvProxyInfo.erase(p);
					else
						++p;
				}
				timeout = delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE);
			}
			when(GetRateInfoRequest req = waitNext(rkInterf.getRateInfo.getFuture())) {
				GetRateInfoReply reply;

				auto& p = self.grvProxyInfo[req.requesterID];
				//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.totalTransactions).detail("Delta", req.totalReleasedTransactions - p.totalTransactions);
				if (p.totalTransactions > 0) {
					self.smoothReleasedTransactions.addDelta(req.totalReleasedTransactions - p.totalTransactions);

					for (auto tag : req.throttledTagCounts) {
						self.throttledTags.addRequests(tag.first, tag.second);
					}
				}
				if (p.batchTransactions > 0) {
					self.smoothBatchReleasedTransactions.addDelta(req.batchReleasedTransactions - p.batchTransactions);
				}

				p.totalTransactions = req.totalReleasedTransactions;
				p.batchTransactions = req.batchReleasedTransactions;
				p.lastUpdateTime = now();

				reply.transactionRate = self.normalLimits.tpsLimit / self.grvProxyInfo.size();
				reply.batchTransactionRate = self.batchLimits.tpsLimit / self.grvProxyInfo.size();
				reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;

				if (p.lastThrottledTagChangeId != self.throttledTagChangeId ||
				    now() > p.lastTagPushTime + SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL) {
					p.lastThrottledTagChangeId = self.throttledTagChangeId;
					p.lastTagPushTime = now();

					reply.throttledTags = self.throttledTags.getClientRates(self.autoThrottlingEnabled);
					bool returningTagsToProxy = reply.throttledTags.present() && reply.throttledTags.get().size() > 0;
					TEST(returningTagsToProxy); // Returning tag throttles to a proxy
				}

				reply.healthMetrics.update(self.healthMetrics, true, req.detailed);
				reply.healthMetrics.tpsLimit = self.normalLimits.tpsLimit;
				reply.healthMetrics.batchLimited = lastLimited;

				req.reply.send(reply);
			}
			when(HaltRatekeeperRequest req = waitNext(rkInterf.haltRatekeeper.getFuture())) {
				req.reply.send(Void());
				TraceEvent("RatekeeperHalted", rkInterf.id()).detail("ReqID", req.requesterID);
				break;
			}
			when(ReportCommitCostEstimationRequest req = waitNext(rkInterf.reportCommitCostEstimation.getFuture())) {
				updateCommitCostEstimation(&self, req.ssTrTagCommitCost);
				req.reply.send(Void());
			}
			when(wait(err.getFuture())) {}
			when(wait(dbInfo->onChange())) {
				if (tlogInterfs != dbInfo->get().logSystemConfig.allLocalLogs()) {
					tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
					tlogTrackers = std::vector<Future<Void>>();
					for (int i = 0; i < tlogInterfs.size(); i++)
						tlogTrackers.push_back(splitError(trackTLogQueueInfo(&self, tlogInterfs[i]), err));
				}
				self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();
			}
			when(wait(collection)) {
				ASSERT(false);
				throw internal_error();
			}
		}
	} catch (Error& err) {
		TraceEvent("RatekeeperDied", rkInterf.id()).error(err, true);
	}
	return Void();
}
