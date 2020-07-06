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
#include "fdbclient/TagThrottle.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

enum limitReason_t {
	unlimited,  // TODO: rename to workload?
	storage_server_write_queue_size,
	storage_server_write_bandwidth_mvcc,
	storage_server_readable_behind,
	log_server_mvcc_write_bandwidth,
	log_server_write_queue,
	storage_server_min_free_space,  // a storage server's normal limits are being reduced by low free space
	storage_server_min_free_space_ratio,  // a storage server's normal limits are being reduced by a low free space ratio
	log_server_min_free_space,
	log_server_min_free_space_ratio,
	storage_server_durability_lag,
	storage_server_list_fetch_failed,
	limitReason_t_end
};

int limitReasonEnd = limitReason_t_end;

const char* limitReasonName[] = {
	"workload",
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
	"storage_server_list_fetch_failed"
};
static_assert(sizeof(limitReasonName) / sizeof(limitReasonName[0]) == limitReason_t_end, "limitReasonDesc table size");

// NOTE: This has a corresponding table in Script.cs (see RatekeeperReason graph)
// IF UPDATING THIS ARRAY, UPDATE SCRIPT.CS!
const char* limitReasonDesc[] = {
	"Workload or read performance.",
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
	"Unable to fetch storage server list."
};

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

	Optional<TransactionTag> busiestTag;
	double busiestTagFractionalBusyness;
	double busiestTagRate;

	StorageQueueInfo(UID id, LocalityData locality)
	  : valid(false), id(id), locality(locality), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
	    smoothDurableVersion(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothLatestVersion(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT), limitReason(limitReason_t::unlimited), busiestTagFractionalBusyness(0),
		busiestTagRate(0) {
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
	TLogQueueInfo(UID id) : valid(false), id(id), smoothDurableBytes(SERVER_KNOBS->SMOOTHING_AMOUNT), smoothInputBytes(SERVER_KNOBS->SMOOTHING_AMOUNT),
		verySmoothDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT), smoothFreeSpace(SERVER_KNOBS->SMOOTHING_AMOUNT),
		smoothTotalSpace(SERVER_KNOBS->SMOOTHING_AMOUNT) {
		// FIXME: this is a tacky workaround for a potential uninitialized use in trackTLogQueueInfo (copied from storageQueueInfO)
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
			if(limits.tpsRate == 0.0 || !requestRate.present() || requestRate.get() == 0.0) {
				return limits.tpsRate;
			}
			else {
				return std::min(limits.tpsRate, (limits.tpsRate / requestRate.get()) * clientRate.smoothTotal());
			}
		}

		Optional<double> updateAndGetClientRate(Optional<double> requestRate) {
			if(limits.expiration > now()) {
				double targetRate = getTargetRate(requestRate);
				if(targetRate == std::numeric_limits<double>::max()) {
					rateSet = false;
					return Optional<double>();
				}
				if(!rateSet) {
					rateSet = true;
					clientRate.reset(targetRate);
				}
				else {
					clientRate.setTotal(targetRate);
				}

				double rate = clientRate.smoothTotal();
				ASSERT(rate >= 0);
				return rate;
			}
			else {
				TEST(true); // Get throttle rate for expired throttle
				rateSet = false;
				return Optional<double>();
			}
		}
	};

public:
	RkTagThrottleCollection() {}

	RkTagThrottleCollection(RkTagThrottleCollection &&other) {
		autoThrottledTags = std::move(other.autoThrottledTags);
		manualThrottledTags = std::move(other.manualThrottledTags);
		tagData = std::move(other.tagData);
	}
	
	void operator=(RkTagThrottleCollection &&other) {
		autoThrottledTags = std::move(other.autoThrottledTags);
		manualThrottledTags = std::move(other.manualThrottledTags);
		tagData = std::move(other.tagData);
	}

	double computeTargetTpsRate(double currentBusyness, double targetBusyness, double requestRate) {
		ASSERT(currentBusyness > 0);

		if(targetBusyness < 1) {
			double targetFraction = targetBusyness * (1-currentBusyness) / ((1-targetBusyness) * currentBusyness);
			return requestRate * targetFraction;
		}
		else {
			return std::numeric_limits<double>::max();
		}
	}

	// Returns the TPS rate if the throttle is updated, otherwise returns an empty optional
	Optional<double> autoThrottleTag(UID id, TransactionTag const& tag, double fractionalBusyness, Optional<double> tpsRate = Optional<double>(), Optional<double> expiration = Optional<double>()) {
		ASSERT(!tpsRate.present() || tpsRate.get() >= 0);
		ASSERT(!expiration.present() || expiration.get() > now());

		auto itr = autoThrottledTags.find(tag);
		if(itr == autoThrottledTags.end() && autoThrottledTags.size() >= SERVER_KNOBS->MAX_AUTO_THROTTLED_TRANSACTION_TAGS) {
			TEST(true); // Reached auto-throttle limit
			return Optional<double>();
		}

		itr = autoThrottledTags.try_emplace(itr, tag);
		auto &throttle = itr->second;

		if(!tpsRate.present()) {
			if(now() <= throttle.created + SERVER_KNOBS->AUTO_TAG_THROTTLE_START_AGGREGATION_TIME) {
				tpsRate = std::numeric_limits<double>::max();
			}
			else if(now() <= throttle.lastUpdated + SERVER_KNOBS->AUTO_TAG_THROTTLE_UPDATE_FREQUENCY) {
				TEST(true); // Tag auto-throttled too quickly
				return Optional<double>();
			}
			else {
				tpsRate = computeTargetTpsRate(fractionalBusyness, SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS, tagData[tag].requestRate.smoothRate());

				if(throttle.limits.expiration > now() && tpsRate.get() >= throttle.limits.tpsRate) {
					TEST(true); // Tag auto-throttle rate increase attempt while active
					return Optional<double>();
				}

				throttle.lastUpdated = now();
				if(tpsRate.get() < throttle.limits.tpsRate) {
					throttle.lastReduced = now();
				}
			}
		}
		if(!expiration.present()) {
			expiration = now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION;
		}

		ASSERT(tpsRate.present() && tpsRate.get() >= 0);

		TraceEvent("RkSetAutoThrottle", id)
			.detail("Tag", tag)
			.detail("TargetRate", tpsRate.get())
			.detail("Expiration", expiration.get() - now());

		throttle.limits.tpsRate = tpsRate.get();
		throttle.limits.expiration = expiration.get();

		throttle.updateAndGetClientRate(getRequestRate(tag));

		if(tpsRate.get() != std::numeric_limits<double>::max()) {
			return tpsRate.get();
		}
		else {
			return Optional<double>();
		}
	}

	void manualThrottleTag(UID id, TransactionTag const& tag, TransactionPriority priority, double tpsRate,
	                       double expiration, Optional<ClientTagThrottleLimits> const& oldLimits) {
		ASSERT(tpsRate >= 0);
		ASSERT(expiration > now());

		auto &priorityThrottleMap = manualThrottledTags[tag];
		auto result = priorityThrottleMap.try_emplace(priority);
		ASSERT(result.second); // Updating to the map is done by copying the whole map
		
		result.first->second.limits.tpsRate = tpsRate;
		result.first->second.limits.expiration = expiration;

		if(!oldLimits.present()) {
			TEST(true); // Transaction tag manually throttled
			TraceEvent("RatekeeperAddingManualThrottle", id)
				.detail("Tag", tag)
				.detail("Rate", tpsRate)
				.detail("Priority", transactionPriorityToString(priority))
				.detail("SecondsToExpiration", expiration - now());
		}
		else if(oldLimits.get().tpsRate != tpsRate || oldLimits.get().expiration != expiration) {
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

	Optional<ClientTagThrottleLimits> getManualTagThrottleLimits(TransactionTag const& tag, TransactionPriority priority) {
		auto itr = manualThrottledTags.find(tag);
		if(itr != manualThrottledTags.end()) {
			auto priorityItr = itr->second.find(priority);
			if(priorityItr != itr->second.end()) {
				return priorityItr->second.limits;
			}
		}

		return Optional<ClientTagThrottleLimits>();
	}

	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() {
		PrioritizedTransactionTagMap<ClientTagThrottleLimits> clientRates;

		for(auto tagItr = tagData.begin(); tagItr != tagData.end();) {
			bool tagPresent = false;

			double requestRate = tagItr->second.requestRate.smoothRate();
			auto manualItr = manualThrottledTags.find(tagItr->first);
			if(manualItr != manualThrottledTags.end()) {
				Optional<ClientTagThrottleLimits> manualClientRate;
				for(auto priority = allTransactionPriorities.rbegin(); !(priority == allTransactionPriorities.rend()); ++priority) {
					auto priorityItr = manualItr->second.find(*priority);
					if(priorityItr != manualItr->second.end()) {
						Optional<double> priorityClientRate = priorityItr->second.updateAndGetClientRate(requestRate);
						if(!priorityClientRate.present()) {
							TEST(true); // Manual priority throttle expired
							priorityItr = manualItr->second.erase(priorityItr);
						}
						else {
							if(!manualClientRate.present() || manualClientRate.get().tpsRate > priorityClientRate.get()) {
								manualClientRate = ClientTagThrottleLimits(priorityClientRate.get(), priorityItr->second.limits.expiration);
							}
							else {
								TEST(true); // Manual throttle overriden by higher priority
							}

							++priorityItr;
						}
					}

					if(manualClientRate.present()) {
						tagPresent = true;
						TEST(true); // Using manual throttle
						clientRates[*priority][tagItr->first] = manualClientRate.get();
					}
				}

				if(manualItr->second.empty()) {
					TEST(true); // All manual throttles expired
					manualThrottledTags.erase(manualItr);
					break;
				}
			}

			auto autoItr = autoThrottledTags.find(tagItr->first);
			if(autoItr != autoThrottledTags.end()) {
				Optional<double> autoClientRate = autoItr->second.updateAndGetClientRate(requestRate);
				if(autoClientRate.present()) {
					double adjustedRate = autoClientRate.get();
					if(now() >= autoItr->second.lastReduced + SERVER_KNOBS->AUTO_TAG_THROTTLE_RAMP_UP_TIME) {
						TEST(true); // Tag auto-throttle ramping up

						double targetBusyness = SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS;
						if(targetBusyness == 0) {
							targetBusyness = 0.01;
						}

						double rampLocation = (now() - autoItr->second.lastReduced - SERVER_KNOBS->AUTO_TAG_THROTTLE_RAMP_UP_TIME) / targetBusyness;
						adjustedRate = computeTargetTpsRate(targetBusyness, pow(targetBusyness, 1 - rampLocation), adjustedRate);
					}

					tagPresent = true;
					auto result = clientRates[TransactionPriority::DEFAULT].try_emplace(tagItr->first, adjustedRate, autoItr->second.limits.expiration);
					if(!result.second && result.first->second.tpsRate > adjustedRate) {
						result.first->second = ClientTagThrottleLimits(adjustedRate, autoItr->second.limits.expiration);
					}
					else {
						TEST(true); // Auto throttle overriden by manual throttle
					}
					clientRates[TransactionPriority::BATCH][tagItr->first] = ClientTagThrottleLimits(0, autoItr->second.limits.expiration);
				}
				else {
					TEST(true); // Auto throttle expired
					autoThrottledTags.erase(autoItr);
				}
			}

			if(!tagPresent) {
				TEST(true); // All tag throttles expired
				tagItr = tagData.erase(tagItr);
			}
			else {
				++tagItr;
			}
		}

		return clientRates;
	}

	void addRequests(TransactionTag const& tag, int requests) {
		TEST(true); // Requests reported for throttled tag
		ASSERT(requests > 0);

		auto tagItr = tagData.try_emplace(tag);
		tagItr.first->second.requestRate.addDelta(requests);

		double requestRate = tagItr.first->second.requestRate.smoothRate();
		
		auto autoItr = autoThrottledTags.find(tag);
		if(autoItr != autoThrottledTags.end()) {
			autoItr->second.updateAndGetClientRate(requestRate);
		}

		auto manualItr = manualThrottledTags.find(tag);
		if(manualItr != manualThrottledTags.end()) {
			for(auto priorityItr = manualItr->second.begin(); priorityItr != manualItr->second.end(); ++priorityItr) {
				priorityItr->second.updateAndGetClientRate(requestRate);
			}
		}
	}

	Optional<double> getRequestRate(TransactionTag const& tag) { 
		auto itr = tagData.find(tag);
		if(itr != tagData.end()) {
			return itr->second.requestRate.smoothRate();
		}
		return Optional<double>();
	}

	int64_t autoThrottleCount() const {
		return autoThrottledTags.size();
	}

	int64_t manualThrottleCount() const {
		int64_t count = 0;
		for(auto itr = manualThrottledTags.begin(); itr != manualThrottledTags.end(); ++itr) {
			count += itr->second.size();
		}

		return count;
	}

	TransactionTagMap<RkTagThrottleData> autoThrottledTags;
	TransactionTagMap<std::map<TransactionPriority, RkTagThrottleData>> manualThrottledTags;
	TransactionTagMap<RkTagData> tagData;
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

	RatekeeperLimits(TransactionPriority priority, std::string context, int64_t storageTargetBytes,
	                 int64_t storageSpringBytes, int64_t logTargetBytes, int64_t logSpringBytes,
	                 double maxVersionDifference, int64_t durabilityLagTargetVersions, Version readTxnLifetime)
	  : priority(priority), tpsLimit(std::numeric_limits<double>::infinity()),
	    tpsLimitMetric(StringRef("Ratekeeper.TPSLimit" + context)),
	    reasonMetric(StringRef("Ratekeeper.Reason" + context)), storageTargetBytes(storageTargetBytes),
	    storageSpringBytes(storageSpringBytes), logTargetBytes(logTargetBytes), logSpringBytes(logSpringBytes),
	    maxVersionDifference(maxVersionDifference),
	    durabilityLagTargetVersions(durabilityLagTargetVersions +
	                                readTxnLifetime), // The read transaction life versions are expected to not be
	                                                  // durable on the storage servers
	    durabilityLagLimit(std::numeric_limits<double>::infinity()), lastDurabilityLag(0), context(context) {}
};

struct ProxyInfo {
	int64_t totalTransactions;
	int64_t batchTransactions;
	uint64_t lastThrottledTagChangeId;

	double lastUpdateTime;
	double lastTagPushTime;

	ProxyInfo() : totalTransactions(0), batchTransactions(0), lastUpdateTime(0), lastThrottledTagChangeId(0), lastTagPushTime(0) {}
};

struct RatekeeperData {
	UID id;
	Database db;

	Map<UID, StorageQueueInfo> storageQueueInfo;
	Map<UID, TLogQueueInfo> tlogQueueInfo;

	std::map<UID, ProxyInfo> proxyInfo;
	Smoother smoothReleasedTransactions, smoothBatchReleasedTransactions, smoothTotalDurableBytes;
	HealthMetrics healthMetrics;
	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	Int64MetricHandle actualTpsMetric;

	double lastWarning;
	double lastSSListFetchedTimestamp;

	RkTagThrottleCollection throttledTags;
	uint64_t throttledTagChangeId;

	RatekeeperLimits normalLimits;
	RatekeeperLimits batchLimits;

	Deque<double> actualTpsHistory;
	Optional<Key> remoteDC;

	Future<Void> expiredTagThrottleCleanup;

	bool autoThrottlingEnabled;

	Version readTxnLifetime;

	RatekeeperData(UID id, Database db, Version readTxnLifetime)
	  : id(id), db(db), readTxnLifetime(readTxnLifetime), smoothReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothBatchReleasedTransactions(SERVER_KNOBS->SMOOTHING_AMOUNT),
	    smoothTotalDurableBytes(SERVER_KNOBS->SLOW_SMOOTHING_AMOUNT),
	    actualTpsMetric(LiteralStringRef("Ratekeeper.ActualTPS")), lastWarning(0), lastSSListFetchedTimestamp(now()),
	    throttledTagChangeId(0),
	    normalLimits(TransactionPriority::DEFAULT, "", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER,
	                 SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER, SERVER_KNOBS->TARGET_BYTES_PER_TLOG,
	                 SERVER_KNOBS->SPRING_BYTES_TLOG, SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE,
	                 SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS, readTxnLifetime),
	    batchLimits(TransactionPriority::BATCH, "Batch", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER_BATCH,
	                SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER_BATCH, SERVER_KNOBS->TARGET_BYTES_PER_TLOG_BATCH,
	                SERVER_KNOBS->SPRING_BYTES_TLOG_BATCH, SERVER_KNOBS->MAX_TL_SS_VERSION_DIFFERENCE_BATCH,
	                SERVER_KNOBS->TARGET_DURABILITY_LAG_VERSIONS_BATCH, readTxnLifetime),
	    autoThrottlingEnabled(false) {
		expiredTagThrottleCleanup = recurring([this](){ ThrottleApi::expire(this->db); }, SERVER_KNOBS->TAG_THROTTLE_EXPIRED_CLEANUP_INTERVAL);
		ASSERT_WE_THINK(this->readTxnLifetime > 0);
	}
};

//SOMEDAY: template trackStorageServerQueueInfo and trackTLogQueueInfo into one function
ACTOR Future<Void> trackStorageServerQueueInfo( RatekeeperData* self, StorageServerInterface ssi ) {
	self->storageQueueInfo.insert( mapPair(ssi.id(), StorageQueueInfo(ssi.id(), ssi.locality) ) );
	state Map<UID, StorageQueueInfo>::iterator myQueueInfo = self->storageQueueInfo.find(ssi.id());
	TraceEvent("RkTracking", self->id)
		.detail("StorageServer", ssi.id());
	try {
		loop {
			ErrorOr<StorageQueuingMetricsReply> reply = wait( ssi.getQueuingMetrics.getReplyUnlessFailedFor( StorageQueuingMetricsRequest(), 0, 0 ) ); // SOMEDAY: or tryGetReply?
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
					self->smoothTotalDurableBytes.addDelta( reply.get().bytesDurable - myQueueInfo->value.prevReply.bytesDurable );
					myQueueInfo->value.smoothDurableBytes.setTotal( reply.get().bytesDurable );
					myQueueInfo->value.verySmoothDurableBytes.setTotal( reply.get().bytesDurable );
					myQueueInfo->value.smoothInputBytes.setTotal( reply.get().bytesInput );
					myQueueInfo->value.smoothFreeSpace.setTotal( reply.get().storageBytes.available );
					myQueueInfo->value.smoothTotalSpace.setTotal( reply.get().storageBytes.total );
					myQueueInfo->value.smoothDurableVersion.setTotal(reply.get().durableVersion);
					myQueueInfo->value.smoothLatestVersion.setTotal(reply.get().version);
				}

				myQueueInfo->value.busiestTag = reply.get().busiestTag;
				myQueueInfo->value.busiestTagFractionalBusyness = reply.get().busiestTagFractionalBusyness;
				myQueueInfo->value.busiestTagRate = reply.get().busiestTagRate;
			} else {
				if(myQueueInfo->value.valid) {
					TraceEvent("RkStorageServerDidNotRespond", self->id)
						.detail("StorageServer", ssi.id());
				}
				myQueueInfo->value.valid = false;
			}

			wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) && IFailureMonitor::failureMonitor().onStateEqual(ssi.getQueuingMetrics.getEndpoint(), FailureStatus(false)));
		}
	} catch (...) {
		// including cancellation
		self->storageQueueInfo.erase( myQueueInfo );
		throw;
	}
}

ACTOR Future<Void> trackTLogQueueInfo( RatekeeperData* self, TLogInterface tli ) {
	self->tlogQueueInfo.insert( mapPair(tli.id(), TLogQueueInfo(tli.id()) ) );
	state Map<UID, TLogQueueInfo>::iterator myQueueInfo = self->tlogQueueInfo.find(tli.id());
	TraceEvent("RkTracking", self->id)
		.detail("TransactionLog", tli.id());
	try {
		loop {
			ErrorOr<TLogQueuingMetricsReply> reply = wait( tli.getQueuingMetrics.getReplyUnlessFailedFor( TLogQueuingMetricsRequest(), 0, 0 ) );  // SOMEDAY: or tryGetReply?
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
					self->smoothTotalDurableBytes.addDelta( reply.get().bytesDurable - myQueueInfo->value.prevReply.bytesDurable );
					myQueueInfo->value.smoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.verySmoothDurableBytes.setTotal(reply.get().bytesDurable);
					myQueueInfo->value.smoothInputBytes.setTotal(reply.get().bytesInput);
					myQueueInfo->value.smoothFreeSpace.setTotal(reply.get().storageBytes.available);
					myQueueInfo->value.smoothTotalSpace.setTotal(reply.get().storageBytes.total);
				}
			} else {
				if(myQueueInfo->value.valid) {
					TraceEvent("RkTLogDidNotRespond", self->id)
						.detail("TransactionLog", tli.id());
				}
				myQueueInfo->value.valid = false;
			}

			wait(delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE) && IFailureMonitor::failureMonitor().onStateEqual(tli.getQueuingMetrics.getEndpoint(), FailureStatus(false)));
		}
	} catch (...) {
		// including cancellation
		self->tlogQueueInfo.erase( myQueueInfo );
		throw;
	}
}

ACTOR Future<Void> splitError( Future<Void> in, Promise<Void> errOut ) {
	try {
		wait( in );
		return Void();
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && !errOut.isSet())
			errOut.sendError(e);
		throw;
	}
}

ACTOR Future<Void> trackEachStorageServer(
	RatekeeperData* self,
	FutureStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges )
{
	state Map<UID, Future<Void>> actors;
	state Promise<Void> err;
	loop choose {
		when (state std::pair< UID, Optional<StorageServerInterface> > change = waitNext(serverChanges) ) {
			wait(delay(0)); // prevent storageServerTracker from getting cancelled while on the call stack
			if (change.second.present()) {
				auto& a = actors[ change.first ];
				a = Future<Void>();
				a = splitError( trackStorageServerQueueInfo(self, change.second.get()), err );
			} else
				actors.erase( change.first );
		}
		when (wait(err.getFuture())) {}
	}
}

ACTOR Future<Void> monitorServerListChange(
		RatekeeperData* self,
		PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges) {
	state std::map<UID, StorageServerInterface> oldServers;
	state Transaction tr(self->db);

	loop {
		try {
			tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
			vector<std::pair<StorageServerInterface, ProcessClass>> results = wait(getServerListAndProcessClasses(&tr));
			self->lastSSListFetchedTimestamp = now();

			std::map<UID, StorageServerInterface> newServers;
			for (int i = 0; i < results.size(); i++) {
				const StorageServerInterface& ssi = results[i].first;
				const UID serverId = ssi.id();
				newServers[serverId] = ssi;

				if (oldServers.count(serverId)) {
					if (ssi.getValue.getEndpoint() != oldServers[serverId].getValue.getEndpoint()) {
						serverChanges.send( std::make_pair(serverId, Optional<StorageServerInterface>(ssi)) );
					}
					oldServers.erase(serverId);
				} else {
					serverChanges.send( std::make_pair(serverId, Optional<StorageServerInterface>(ssi)) );
				}
			}

			for (const auto& it : oldServers) {
				serverChanges.send( std::make_pair(it.first, Optional<StorageServerInterface>()) );
			}

			oldServers.swap(newServers);
			tr = Transaction(self->db);
			wait(delay(SERVER_KNOBS->SERVER_LIST_DELAY));
		} catch(Error& e) {
			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> monitorThrottlingChanges(RatekeeperData *self) {
	state bool committed = false;
	loop {
		state ReadYourWritesTransaction tr(self->db);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				state Future<Standalone<RangeResultRef>> throttledTagKeys = tr.getRange(tagThrottleKeys, CLIENT_KNOBS->TOO_MANY);
				state Future<Optional<Value>> autoThrottlingEnabled = tr.get(tagThrottleAutoEnabledKey);

				if(!committed) {
					BinaryWriter limitWriter(Unversioned());
					limitWriter << SERVER_KNOBS->MAX_MANUAL_THROTTLED_TRANSACTION_TAGS;
					tr.set(tagThrottleLimitKey, limitWriter.toValue());
				}

				wait(success(throttledTagKeys) && success(autoThrottlingEnabled));

				if(autoThrottlingEnabled.get().present() && autoThrottlingEnabled.get().get() == LiteralStringRef("0")) {
					TEST(true); // Auto-throttling disabled
					if(self->autoThrottlingEnabled) {
						TraceEvent("AutoTagThrottlingDisabled", self->id);
					}
					self->autoThrottlingEnabled = false;
				}
				else if(autoThrottlingEnabled.get().present() && autoThrottlingEnabled.get().get() == LiteralStringRef("1")) {
					TEST(true); // Auto-throttling enabled
					if(!self->autoThrottlingEnabled) {
						TraceEvent("AutoTagThrottlingEnabled", self->id);
					}
					self->autoThrottlingEnabled = true;
				}
				else {
					TEST(true); // Auto-throttling unspecified
					if(autoThrottlingEnabled.get().present()) {
						TraceEvent(SevWarnAlways, "InvalidAutoTagThrottlingValue", self->id).detail("Value", autoThrottlingEnabled.get().get());
					}
					self->autoThrottlingEnabled = SERVER_KNOBS->AUTO_TAG_THROTTLING_ENABLED;
				}

				RkTagThrottleCollection updatedTagThrottles;

				TraceEvent("RatekeeperReadThrottledTags", self->id).detail("NumThrottledTags", throttledTagKeys.get().size());
				for(auto entry : throttledTagKeys.get()) {
					TagThrottleKey tagKey = TagThrottleKey::fromKey(entry.key);
					TagThrottleValue tagValue = TagThrottleValue::fromValue(entry.value);

					ASSERT(tagKey.tags.size() == 1); // Currently, only 1 tag per throttle is supported

					if(tagValue.expirationTime == 0 || tagValue.expirationTime > now() + tagValue.initialDuration) {
						TEST(true); // Converting tag throttle duration to absolute time
						tagValue.expirationTime = now() + tagValue.initialDuration;
						BinaryWriter wr(IncludeVersion(ProtocolVersion::withTagThrottleValue()));
						wr << tagValue;
						state Value value = wr.toValue();

						tr.set(entry.key, value);
					}

					if(tagValue.expirationTime > now()) {
						TransactionTag tag = *tagKey.tags.begin();
						Optional<ClientTagThrottleLimits> oldLimits = self->throttledTags.getManualTagThrottleLimits(tag, tagKey.priority);

						if(tagKey.throttleType == TagThrottleType::AUTO) {
							updatedTagThrottles.autoThrottleTag(self->id, tag, 0, tagValue.tpsRate, tagValue.expirationTime);
						}
						else {
							updatedTagThrottles.manualThrottleTag(self->id, tag, tagKey.priority, tagValue.tpsRate, tagValue.expirationTime, oldLimits);
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

void tryAutoThrottleTag(RatekeeperData *self, StorageQueueInfo const& ss, RkTagThrottleCollection& throttledTags) {
	if(ss.busiestTag.present() && ss.busiestTagFractionalBusyness > SERVER_KNOBS->AUTO_THROTTLE_TARGET_TAG_BUSYNESS && ss.busiestTagRate > SERVER_KNOBS->MIN_TAG_COST) {
		TEST(true); // Transaction tag auto-throttled

		Optional<double> clientRate = self->throttledTags.autoThrottleTag(self->id, ss.busiestTag.get(), ss.busiestTagFractionalBusyness);
		if(clientRate.present()) {
			TagSet tags;
			tags.addTag(ss.busiestTag.get());

			self->addActor.send(ThrottleApi::throttleTags(self->db, tags, clientRate.get(), SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION, TagThrottleType::AUTO, TransactionPriority::DEFAULT, now() + SERVER_KNOBS->AUTO_TAG_THROTTLE_DURATION));
		}
	}
}

void updateRate(RatekeeperData* self, RatekeeperLimits* limits, RkTagThrottleCollection& throttledTags) {
	//double controlFactor = ;  // dt / eFoldingTime

	double actualTps = self->smoothReleasedTransactions.smoothRate();
	self->actualTpsMetric = (int64_t)actualTps;
	// SOMEDAY: Remove the max( 1.0, ... ) since the below calculations _should_ be able to recover back up from this value
	actualTps = std::max( std::max( 1.0, actualTps ), self->smoothTotalDurableBytes.smoothRate() / CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT );
	
	if(self->actualTpsHistory.size() > SERVER_KNOBS->MAX_TPS_HISTORY_SAMPLES) {
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
	for(auto i = self->storageQueueInfo.begin(); i != self->storageQueueInfo.end(); ++i) {
		auto& ss = i->value;
		if (!ss.valid || (self->remoteDC.present() && ss.locality.dcId() == self->remoteDC)) continue;
		++sscount;

		limitReason_t ssLimitReason = limitReason_t::unlimited;

		int64_t minFreeSpace = std::max(SERVER_KNOBS->MIN_AVAILABLE_SPACE, (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * ss.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceStorageServer = std::min(worstFreeSpaceStorageServer, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(1, std::min<int64_t>(limits->storageSpringBytes, (ss.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(1, std::min(limits->storageTargetBytes, (int64_t)ss.smoothFreeSpace.smoothTotal() - minFreeSpace));
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

		storageDurabilityLagReverseIndex.insert(std::make_pair(-1*storageDurabilityLag, &ss));

		auto& ssMetrics = self->healthMetrics.storageStats[ss.id];
		ssMetrics.storageQueue = storageQueue;
		ssMetrics.storageDurabilityLag = storageDurabilityLag;
		ssMetrics.cpuUsage = ss.lastReply.cpuUsage;
		ssMetrics.diskUsage = ss.lastReply.diskUsage;

		double targetRateRatio = std::min(( storageQueue - targetBytes + springBytes ) / (double)springBytes, 2.0);

		if(limits->priority == TransactionPriority::DEFAULT && (storageQueue > SERVER_KNOBS->AUTO_TAG_THROTTLE_STORAGE_QUEUE_BYTES || storageDurabilityLag > SERVER_KNOBS->AUTO_TAG_THROTTLE_DURABILITY_LAG_VERSIONS)) {
			tryAutoThrottleTag(self, ss, throttledTags);
		}

		double inputRate = ss.smoothInputBytes.smoothRate();
		//inputRate = std::max( inputRate, actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );

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
		// What if we do, what will happen? Client will not get transaction too old error although it should not?
		double maxBytesPerSecond =
		    (targetBytes - springBytes) / ((((double)self->readTxnLifetime) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0);
		double limitTps = std::min(actualTps * maxBytesPerSecond / std::max(1.0e-8, inputRate), maxBytesPerSecond * SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE);
		if (ssLimitReason == limitReason_t::unlimited)
			ssLimitReason = limitReason_t::storage_server_write_bandwidth_mvcc;

		if (targetRateRatio > 0 && inputRate > 0) {
			ASSERT(inputRate != 0);
			double smoothedRate = std::max( ss.verySmoothDurableBytes.smoothRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );
			double x =  smoothedRate / (inputRate * targetRateRatio);
			double lim = actualTps * x;
			if (lim < limitTps) {
				limitTps = lim;
				if (ssLimitReason == limitReason_t::unlimited || ssLimitReason == limitReason_t::storage_server_write_bandwidth_mvcc) {
					ssLimitReason = limitReason_t::storage_server_write_queue_size;
				}
			}
		}

		storageTpsLimitReverseIndex.insert(std::make_pair(limitTps, &ss));

		if (limitTps < limits->tpsLimit && (ssLimitReason == limitReason_t::storage_server_min_free_space || ssLimitReason == limitReason_t::storage_server_min_free_space_ratio)) {
			reasonID = ss.id;
			limits->tpsLimit = limitTps;
			limitReason = ssLimitReason;
		}

		ssReasons[ss.id] = ssLimitReason;
	}

	std::set<Optional<Standalone<StringRef>>> ignoredMachines;
	for (auto ss = storageTpsLimitReverseIndex.begin(); ss != storageTpsLimitReverseIndex.end() && ss->first < limits->tpsLimit; ++ss) {
		if (ignoredMachines.size() < std::min(self->configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingStorageQueueStorageServer = ss->second->lastReply.bytesInput - ss->second->smoothDurableBytes.smoothTotal();
		limits->tpsLimit = ss->first;
		reasonID = storageTpsLimitReverseIndex.begin()->second->id; // Although we aren't controlling based on the worst SS, we still report it as the limiting process
		limitReason = ssReasons[reasonID];

		break;
	}

	int64_t limitingDurabilityLag = 0;

	std::set<Optional<Standalone<StringRef>>> ignoredDurabilityLagMachines;
	for (auto ss = storageDurabilityLagReverseIndex.begin(); ss != storageDurabilityLagReverseIndex.end(); ++ss) {
		if (ignoredDurabilityLagMachines.size() < std::min(self->configuration.storageTeamSize - 1, SERVER_KNOBS->MAX_MACHINES_FALLING_BEHIND)) {
			ignoredDurabilityLagMachines.insert(ss->second->locality.zoneId());
			continue;
		}
		if (ignoredDurabilityLagMachines.count(ss->second->locality.zoneId()) > 0) {
			continue;
		}

		limitingDurabilityLag = -1*ss->first;
		if(limitingDurabilityLag > limits->durabilityLagTargetVersions && self->actualTpsHistory.size() > SERVER_KNOBS->NEEDED_TPS_HISTORY_SAMPLES) {
			if(limits->durabilityLagLimit == std::numeric_limits<double>::infinity()) {
				double maxTps = 0;
				for(int i = 0; i < self->actualTpsHistory.size(); i++) {
					maxTps = std::max(maxTps, self->actualTpsHistory[i]);
				}
				limits->durabilityLagLimit = SERVER_KNOBS->INITIAL_DURABILITY_LAG_MULTIPLIER*maxTps;
			}
			if( limitingDurabilityLag > limits->lastDurabilityLag ) {
				limits->durabilityLagLimit = SERVER_KNOBS->DURABILITY_LAG_REDUCTION_RATE*limits->durabilityLagLimit;
			}
			if(limits->durabilityLagLimit < limits->tpsLimit) {
				limits->tpsLimit = limits->durabilityLagLimit;
				limitReason = limitReason_t::storage_server_durability_lag;
			}
		} else if(limits->durabilityLagLimit != std::numeric_limits<double>::infinity() && limitingDurabilityLag > limits->durabilityLagTargetVersions - SERVER_KNOBS->DURABILITY_LAG_UNLIMITED_THRESHOLD) {
			limits->durabilityLagLimit = SERVER_KNOBS->DURABILITY_LAG_INCREASE_RATE*limits->durabilityLagLimit;
		} else {
			limits->durabilityLagLimit = std::numeric_limits<double>::infinity();
		}
		limits->lastDurabilityLag = limitingDurabilityLag;
		break;
	}

	self->healthMetrics.worstStorageQueue = worstStorageQueueStorageServer;
	self->healthMetrics.worstStorageDurabilityLag = worstDurabilityLag;

	double writeToReadLatencyLimit = 0;
	Version worstVersionLag = 0;
	Version limitingVersionLag = 0;

	{
		Version minSSVer = std::numeric_limits<Version>::max();
		Version minLimitingSSVer = std::numeric_limits<Version>::max();
		for (const auto& it : self->storageQueueInfo) {
			auto& ss = it.value;
			if (!ss.valid || (self->remoteDC.present() && ss.locality.dcId() == self->remoteDC)) continue;

			minSSVer = std::min(minSSVer, ss.lastReply.version);

			// Machines that ratekeeper isn't controlling can fall arbitrarily far behind
			if (ignoredMachines.count(it.value.locality.zoneId()) == 0) {
				minLimitingSSVer = std::min(minLimitingSSVer, ss.lastReply.version);
			}
		}

		Version maxTLVer = std::numeric_limits<Version>::min();
		for(const auto& it : self->tlogQueueInfo) {
			auto& tl = it.value;
			if (!tl.valid) continue;
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
		if (!tl.valid) continue;
		++tlcount;

		limitReason_t tlogLimitReason = limitReason_t::log_server_write_queue;

		int64_t minFreeSpace = std::max( SERVER_KNOBS->MIN_AVAILABLE_SPACE, (int64_t)(SERVER_KNOBS->MIN_AVAILABLE_SPACE_RATIO * tl.smoothTotalSpace.smoothTotal()));

		worstFreeSpaceTLog = std::min(worstFreeSpaceTLog, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace);

		int64_t springBytes = std::max<int64_t>(1, std::min<int64_t>(limits->logSpringBytes, (tl.smoothFreeSpace.smoothTotal() - minFreeSpace) * 0.2));
		int64_t targetBytes = std::max<int64_t>(1, std::min(limits->logTargetBytes, (int64_t)tl.smoothFreeSpace.smoothTotal() - minFreeSpace));
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

		if( tl.lastReply.bytesInput - tl.lastReply.bytesDurable > tl.lastReply.storageBytes.free - minFreeSpace / 2 ) {
			if(now() - self->lastWarning > 5.0) {
				self->lastWarning = now();
				TraceEvent(SevWarnAlways, "RkTlogMinFreeSpaceZero", self->id).detail("ReasonId", tl.id);
			}
			reasonID = tl.id;
			limitReason = limitReason_t::log_server_min_free_space;
			limits->tpsLimit = 0.0;
		}

		double targetRateRatio = std::min( ( b + springBytes ) / (double)springBytes, 2.0 );

		if (writeToReadLatencyLimit > targetRateRatio){
			targetRateRatio = writeToReadLatencyLimit;
			tlogLimitReason = limitReason_t::storage_server_readable_behind;
		}

		double inputRate = tl.smoothInputBytes.smoothRate();

		if (targetRateRatio > 0) {
			double smoothedRate = std::max( tl.verySmoothDurableBytes.smoothRate(), actualTps / SERVER_KNOBS->MAX_TRANSACTIONS_PER_BYTE );
			double x = smoothedRate / (inputRate * targetRateRatio);
			if (targetRateRatio < .75)  //< FIXME: KNOB for 2.0
				x = std::max(x, 0.95);
			double lim = actualTps * x;
			if (lim < limits->tpsLimit){
				limits->tpsLimit = lim;
				reasonID = tl.id;
				limitReason = tlogLimitReason;
			}
		}
		if (inputRate > 0) {
			// Don't let any tlogs use up its target bytes faster than its MVCC window!
			double x = ((targetBytes - springBytes) /
			            ((((double)self->readTxnLifetime) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0)) /
			           inputRate;
			double lim = actualTps * x;
			if (lim < limits->tpsLimit){
				limits->tpsLimit = lim;
				reasonID = tl.id;
				limitReason = limitReason_t::log_server_mvcc_write_bandwidth;
			}
		}
	}

	self->healthMetrics.worstTLogQueue = worstStorageQueueTLog;

	limits->tpsLimit = std::max(limits->tpsLimit, 0.0);

	if(g_network->isSimulated() && g_simulator.speedUpSimulation) {
		limits->tpsLimit = std::max(limits->tpsLimit, 100.0);
	}

	int64_t totalDiskUsageBytes = 0;
	for(auto & t : self->tlogQueueInfo)
		if (t.value.valid)
			totalDiskUsageBytes += t.value.lastReply.storageBytes.used;
	for(auto & s : self->storageQueueInfo)
		if (s.value.valid)
			totalDiskUsageBytes += s.value.lastReply.storageBytes.used;

	if (now() - self->lastSSListFetchedTimestamp > SERVER_KNOBS->STORAGE_SERVER_LIST_FETCH_TIMEOUT) {
		limits->tpsLimit = 0.0;
		limitReason = limitReason_t::storage_server_list_fetch_failed;
		reasonID = UID();
		TraceEvent(SevWarnAlways, "RkSSListFetchTimeout", self->id).suppressFor(1.0);
	}
	else if(limits->tpsLimit == std::numeric_limits<double>::infinity()) {
		limits->tpsLimit = SERVER_KNOBS->RATEKEEPER_DEFAULT_LIMIT;
	}

	limits->tpsLimitMetric = std::min(limits->tpsLimit, 1e6);
	limits->reasonMetric = limitReason;

	if (deterministicRandom()->random01() < 0.1) {
		std::string name = "RkUpdate" + limits->context;
		TraceEvent(name.c_str(), self->id)
			.detail("TPSLimit", limits->tpsLimit)
			.detail("Reason", limitReason)
			.detail("ReasonServerID", reasonID==UID() ? std::string() : Traceable<UID>::toString(reasonID))
			.detail("ReleasedTPS", self->smoothReleasedTransactions.smoothRate())
			.detail("ReleasedBatchTPS", self->smoothBatchReleasedTransactions.smoothRate())
			.detail("TPSBasis", actualTps)
			.detail("StorageServers", sscount)
			.detail("Proxies", self->proxyInfo.size())
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
			.detail("TagsAutoThrottled", throttledTags.autoThrottleCount())
			.detail("TagsManuallyThrottled", throttledTags.manualThrottleCount())
			.trackLatest(name);
	}
}

ACTOR Future<Void> configurationMonitor(RatekeeperData *self) {
	loop {
		state ReadYourWritesTransaction tr(self->db);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Standalone<RangeResultRef> results = wait( tr.getRange( configKeys, CLIENT_KNOBS->TOO_MANY ) );
				ASSERT( !results.more && results.size() < CLIENT_KNOBS->TOO_MANY );

				self->configuration.fromKeyValues( (VectorRef<KeyValueRef>) results );

				// Note: We may not need to monitor readTxnLifetimeKey because it will trigger recovery and kill the RK
				state Future<Void> watchFuture = tr.watch(moveKeysLockOwnerKey) ||
				                                 tr.watch(excludedServersVersionKey) ||
				                                 tr.watch(failedServersVersionKey) || tr.watch(readTxnLifetimeKey);
				wait( tr.commit() );
				wait( watchFuture );
				break;
			} catch (Error& e) {
				wait( tr.onError(e) );
			}
		}
	}
}

ACTOR Future<Void> ratekeeper(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	state RatekeeperData self(rkInterf.id(), openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, true, true),
	                          dbInfo->get().readTxnLifetime);
	state Future<Void> timeout = Void();
	state std::vector<Future<Void>> tlogTrackers;
	state std::vector<TLogInterface> tlogInterfs;
	state Promise<Void> err;
	state Future<Void> collection = actorCollection( self.addActor.getFuture() );

	TraceEvent("RatekeeperStarting", rkInterf.id());
	self.addActor.send( waitFailureServer(rkInterf.waitFailure.getFuture()) );
	self.addActor.send( configurationMonitor(&self) );

	PromiseStream< std::pair<UID, Optional<StorageServerInterface>> > serverChanges;
	self.addActor.send( monitorServerListChange(&self, serverChanges) );
	self.addActor.send( trackEachStorageServer(&self, serverChanges.getFuture()) );
	self.addActor.send( traceRole(Role::RATEKEEPER, rkInterf.id()) );

	self.addActor.send(monitorThrottlingChanges(&self));

	ASSERT_WE_THINK(self.readTxnLifetime > 0);
	TraceEvent("RkTLogQueueSizeParameters", rkInterf.id())
	    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_TLOG)
	    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_TLOG)
	    .detail("ReadTransactionLifetime", self.readTxnLifetime)
	    .detail("Rate", (SERVER_KNOBS->TARGET_BYTES_PER_TLOG - SERVER_KNOBS->SPRING_BYTES_TLOG) /
	                        ((((double)self.readTxnLifetime) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0));

	TraceEvent("RkStorageServerQueueSizeParameters", rkInterf.id())
	    .detail("Target", SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER)
	    .detail("Spring", SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER)
	    .detail("EBrake", SERVER_KNOBS->STORAGE_HARD_LIMIT_BYTES)
	    .detail("Rate", (SERVER_KNOBS->TARGET_BYTES_PER_STORAGE_SERVER - SERVER_KNOBS->SPRING_BYTES_STORAGE_SERVER) /
	                        ((((double)self.readTxnLifetime) / SERVER_KNOBS->VERSIONS_PER_SECOND) + 2.0));

	tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
	for( int i = 0; i < tlogInterfs.size(); i++ )
		tlogTrackers.push_back( splitError( trackTLogQueueInfo(&self, tlogInterfs[i]), err ) );

	self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();

	try {
		state bool lastLimited = false;
		loop choose {
			when (wait( timeout )) {
				updateRate(&self, &self.normalLimits, self.throttledTags);
				updateRate(&self, &self.batchLimits, self.throttledTags);

				lastLimited = self.smoothReleasedTransactions.smoothRate() > SERVER_KNOBS->LAST_LIMITED_RATIO * self.batchLimits.tpsLimit;
				double tooOld = now() - 1.0;
				for(auto p=self.proxyInfo.begin(); p!=self.proxyInfo.end(); ) {
					if (p->second.lastUpdateTime < tooOld)
						p = self.proxyInfo.erase(p);
					else
						++p;
				}
				timeout = delayJittered(SERVER_KNOBS->METRIC_UPDATE_RATE);
			}
			when (GetRateInfoRequest req = waitNext(rkInterf.getRateInfo.getFuture())) {
				GetRateInfoReply reply;

				auto& p = self.proxyInfo[ req.requesterID ];
				//TraceEvent("RKMPU", req.requesterID).detail("TRT", req.totalReleasedTransactions).detail("Last", p.first).detail("Delta", req.totalReleasedTransactions - p.first);
				if (p.totalTransactions > 0) {
					self.smoothReleasedTransactions.addDelta( req.totalReleasedTransactions - p.totalTransactions );

					for(auto tag : req.throttledTagCounts) {
						self.throttledTags.addRequests(tag.first, tag.second);
					}
				}
				if(p.batchTransactions > 0) {
					self.smoothBatchReleasedTransactions.addDelta( req.batchReleasedTransactions - p.batchTransactions );
				}

				p.totalTransactions = req.totalReleasedTransactions;
				p.batchTransactions = req.batchReleasedTransactions;
				p.lastUpdateTime = now();

				reply.transactionRate = self.normalLimits.tpsLimit / self.proxyInfo.size();
				reply.batchTransactionRate = self.batchLimits.tpsLimit / self.proxyInfo.size();
				reply.leaseDuration = SERVER_KNOBS->METRIC_UPDATE_RATE;

				if(p.lastThrottledTagChangeId != self.throttledTagChangeId || now() < p.lastTagPushTime + SERVER_KNOBS->TAG_THROTTLE_PUSH_INTERVAL) {
					p.lastThrottledTagChangeId = self.throttledTagChangeId;
					p.lastTagPushTime = now();

					reply.throttledTags = self.throttledTags.getClientRates();
					TEST(reply.throttledTags.present() && reply.throttledTags.get().size() > 0); // Returning tag throttles to a proxy
				}

				reply.healthMetrics.update(self.healthMetrics, true, req.detailed);
				reply.healthMetrics.tpsLimit = self.normalLimits.tpsLimit;
				reply.healthMetrics.batchLimited = lastLimited;

				req.reply.send( reply );
			}
			when (HaltRatekeeperRequest req = waitNext(rkInterf.haltRatekeeper.getFuture())) {
				req.reply.send(Void());
				TraceEvent("RatekeeperHalted", rkInterf.id()).detail("ReqID", req.requesterID);
				break;
			}
			when (wait(err.getFuture())) {}
			when (wait(dbInfo->onChange())) {
				if( tlogInterfs != dbInfo->get().logSystemConfig.allLocalLogs() ) {
					tlogInterfs = dbInfo->get().logSystemConfig.allLocalLogs();
					tlogTrackers = std::vector<Future<Void>>();
					for( int i = 0; i < tlogInterfs.size(); i++ )
						tlogTrackers.push_back( splitError( trackTLogQueueInfo(&self, tlogInterfs[i]), err ) );
				}
				self.remoteDC = dbInfo->get().logSystemConfig.getRemoteDcId();
			}
			when ( wait(collection) ) {
				ASSERT(false);
				throw internal_error();
			}
		}
	}
	catch (Error& err) {
		TraceEvent("RatekeeperDied", rkInterf.id()).error(err, true);
	}
	return Void();
}
