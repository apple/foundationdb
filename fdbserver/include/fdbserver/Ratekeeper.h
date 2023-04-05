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

#ifndef FDBSERVER_RATEKEEPER_H
#define FDBSERVER_RATEKEEPER_H

#pragma once

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TLogInterface.h"

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

/**
 * The Ratekeeper class is responsible for:
 *
 * - Fetching metrics from storage servers, tlogs, and commit proxies.
 *   This responsiblity is managed through the metricsTracker object.
 *
 * - Calculating cluster-wide rates for each priority and tag. The
 *   responsibility of calculating per-tag rates is handled through
 *   the tagThrottler object.
 *
 * - Serving the RatekeeperInterface. This interface is used to distribute
 *   transaction rates and health metrics to GRV proxies. Commit proxies also
 *   use this interface to send commit cost estimations to the metricsTracker.
 */
class Ratekeeper {
	friend class RatekeeperImpl;

	// Differentiate from GrvProxyInfo in DatabaseContext.h
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

	std::unique_ptr<IRKMetricsTracker> metricsTracker;

	std::map<UID, Ratekeeper::GrvProxyInfo> grvProxyInfo;
	Smoother smoothReleasedTransactions, smoothBatchReleasedTransactions;
	HealthMetrics healthMetrics;
	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	Int64MetricHandle actualTpsMetric;

	double lastWarning;

	std::unique_ptr<class ITagThrottler> tagThrottler;

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

	Ratekeeper(UID, Database, Reference<AsyncVar<ServerDBInfo> const>, RatekeeperInterface);

	Future<Void> configurationMonitor();
	void updateRate(RatekeeperLimits* limits);

	void tryAutoThrottleTag(TransactionTag, double rate, double busyness, TagThrottledReason);
	void tryAutoThrottleTag(StorageQueueInfo&, int64_t storageQueue, int64_t storageDurabilityLag);
	Future<Void> monitorBlobWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo);

public:
	static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
};

#endif // FDBSERVER_RATEKEEPER_H
