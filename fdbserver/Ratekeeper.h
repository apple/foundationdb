/*
 * Ratekeeper.h
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

#pragma once

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"

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

class Ratekeeper {
	friend class RatekeeperImpl;

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

	std::map<UID, Ratekeeper::GrvProxyInfo> grvProxyInfo;
	Smoother smoothReleasedTransactions, smoothBatchReleasedTransactions, smoothTotalDurableBytes;
	HealthMetrics healthMetrics;
	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	Int64MetricHandle actualTpsMetric;

	double lastWarning;
	double lastSSListFetchedTimestamp;

	std::unique_ptr<class TagThrottler> tagThrottler;

	RatekeeperLimits normalLimits;
	RatekeeperLimits batchLimits;

	Deque<double> actualTpsHistory;
	Optional<Key> remoteDC;

	Future<Void> expiredTagThrottleCleanup;

	double lastBusiestCommitTagPick;

	Ratekeeper(UID id, Database db);

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

public:
	static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
};
