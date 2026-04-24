/*
 * Ratekeeper.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_RATEKEEPER_H
#define FDBSERVER_RATEKEEPER_H

#pragma once

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/TagThrottle.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/core/RatekeeperLimitReasons.h"
#include "fdbserver/core/RatekeeperInterface.h"
#include "fdbserver/core/TLogInterface.h"

struct ServerDBInfo;

class StorageQueueInfo {
	uint64_t totalWriteCosts{ 0 };
	int totalWriteOps{ 0 };

	TransactionTagMap<TransactionCommitCostEstimation> tagCostEst;

	UID ratekeeperID;
	Smoother smoothFreeSpace, smoothTotalSpace;
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothDurableVersion, smoothLatestVersion;

public:
	bool valid;
	UID id;
	LocalityData locality;
	StorageQueuingMetricsReply lastReply;
	bool acceptingRequests;
	limitReason_t limitReason;
	std::vector<BusyTagInfo> busiestReadTags, busiestWriteTags;

	StorageQueueInfo(const UID& id, const LocalityData& locality);
	StorageQueueInfo(const UID& rateKeeperID, const UID& id, const LocalityData& locality);
	UpdateCommitCostRequest refreshCommitCost(double elapsed);
	int64_t getStorageQueueBytes() const { return lastReply.bytesInput - smoothDurableBytes.smoothTotal(); }
	int64_t getDurabilityLag() const { return smoothLatestVersion.smoothTotal() - smoothDurableVersion.smoothTotal(); }
	void update(StorageQueuingMetricsReply const&, Smoother& smoothTotalDurableBytes);
	void addCommitCost(TransactionTagRef tagName, TransactionCommitCostEstimation const& cost);

	double getSmoothFreeSpace() const { return smoothFreeSpace.smoothTotal(); }
	double getSmoothTotalSpace() const { return smoothTotalSpace.smoothTotal(); }
	double getSmoothDurableBytes() const { return smoothDurableBytes.smoothTotal(); }
	double getSmoothInputBytesRate() const { return smoothInputBytes.smoothRate(); }
	double getVerySmoothDurableBytesRate() const { return verySmoothDurableBytes.smoothRate(); }

	Version getLatestVersion() const { return lastReply.version; }
	Optional<double> getTagThrottlingRatio(int64_t storageTargetBytes, int64_t storageSpringBytes) const;
};

class TLogQueueInfo {
	Smoother smoothDurableBytes, smoothInputBytes, verySmoothDurableBytes;
	Smoother smoothFreeSpace;
	Smoother smoothTotalSpace;

public:
	TLogQueuingMetricsReply lastReply;
	bool valid;
	UID id;

	double getSmoothFreeSpace() const { return smoothFreeSpace.smoothTotal(); }
	double getSmoothTotalSpace() const { return smoothTotalSpace.smoothTotal(); }
	double getSmoothDurableBytes() const { return smoothDurableBytes.smoothTotal(); }
	double getSmoothInputBytesRate() const { return smoothInputBytes.smoothRate(); }
	double getVerySmoothDurableBytesRate() const { return verySmoothDurableBytes.smoothRate(); }

	explicit TLogQueueInfo(UID id);
	Version getLastCommittedVersion() const { return lastReply.v; }
	void update(TLogQueuingMetricsReply const& reply, Smoother& smoothTotalDurableBytes);
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

	double bwLagTarget;

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
	                 int64_t durabilityLagTargetVersions,
	                 double bwLagTarget);
};

class Ratekeeper {
	struct GrvProxyInfo {
		int64_t totalTransactions{ 0 };
		int64_t batchTransactions{ 0 };
		uint64_t lastThrottledTagChangeId{ 0 };

		double lastUpdateTime{ 0.0 };
		double lastTagPushTime{ 0.0 };
		Version version{ 0 };
	};

	struct VersionInfo {
		int64_t totalTransactions;
		int64_t batchTransactions;
		double created;

		VersionInfo(int64_t totalTransactions, int64_t batchTransactions, double created)
		  : totalTransactions(totalTransactions), batchTransactions(batchTransactions), created(created) {}

		VersionInfo() : totalTransactions(0), batchTransactions(0), created(0.0) {}
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

	std::unique_ptr<class ITagThrottler> tagThrottler;

	std::unordered_map<UID, StorageServerInterface> storageServerInterfaces;

	RatekeeperLimits normalLimits;
	RatekeeperLimits batchLimits;

	Deque<double> actualTpsHistory;
	Version maxVersion;
	double blobWorkerTime;
	double unblockedAssignmentTime;
	std::map<Version, Ratekeeper::VersionInfo> version_transactions;
	std::map<Version, std::pair<double, Optional<double>>> version_recovery;
	Deque<std::pair<double, Version>> blobWorkerVersionHistory;
	bool anyBlobRanges;
	Optional<Key> remoteDC;
	Optional<UID> ssHighWriteQueue;

	double getRecoveryDuration(Version ver) const {
		auto it = version_recovery.lower_bound(ver);
		double recoveryDuration = 0;
		while (it != version_recovery.end()) {
			if (it->second.second.present()) {
				recoveryDuration += it->second.second.get() - it->second.first;
			} else {
				recoveryDuration += now() - it->second.first;
			}
			++it;
		}
		return recoveryDuration;
	}

	Ratekeeper(UID id, Database db);

	Future<Void> configurationMonitor();
	void updateCommitCostEstimation(UIDTransactionTagMap<TransactionCommitCostEstimation> const& costEstimation);
	void updateRate(RatekeeperLimits* limits);
	Future<Void> refreshStorageServerCommitCosts();
	Future<Void> monitorServerListChange(PromiseStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges);
	Future<Void> trackStorageServerQueueInfo(StorageServerInterface);
	Future<Void> monitorStorageServerQueueSizeInSimulation();
	Future<Void> trackTLogQueueInfo(TLogInterface);
	Future<Void> trackEachStorageServer(FutureStream<std::pair<UID, Optional<StorageServerInterface>>> serverChanges);

	void tryAutoThrottleTag(TransactionTag, double rate, double busyness, TagThrottledReason);
	void tryAutoThrottleTag(StorageQueueInfo&, int64_t storageQueue, int64_t storageDurabilityLag);
	Future<Void> monitorThrottlingChanges();
	Future<Void> monitorBlobWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo);
	Future<Void> monitorHotShards(Reference<AsyncVar<ServerDBInfo> const> dbInfo);
	Future<Void> handleReportCommitCostEstimationReqs(RatekeeperInterface rkInterf);
	Future<Void> handleGetSSVersionLagReqs(RatekeeperInterface rkInterf);
	Future<Void> handleGetRateInfoReqs(RatekeeperInterface rkInterf, Version* recoveryVersion, bool* lastLimited);
	Future<Void> handleDBInfoChanges(Reference<AsyncVar<ServerDBInfo> const> dbInfo,
	                                 bool* recovering,
	                                 Version* recoveryVersion,
	                                 std::vector<TLogInterface>* tlogInterfs,
	                                 std::vector<Future<Void>>* tlogTrackers,
	                                 Promise<Void>* err);
	Future<Void> rateUpdater(bool* lastLimited);

	void getSSVersionLag(Version& maxSSPrimaryVersion, Version& maxSSRemoteVersion);

public:
	static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
};

#endif // FDBSERVER_RATEKEEPER_H
