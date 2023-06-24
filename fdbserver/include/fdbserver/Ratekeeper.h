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
#include "fdbclient/TagThrottle.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/IRKBlobMonitor.h"
#include "fdbserver/IRKConfigurationMonitor.h"
#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/IRKRateServer.h"
#include "fdbserver/IRKRateUpdater.h"
#include "fdbserver/IRKRecoveryTracker.h"
#include "fdbserver/IRKThroughputQuotaCache.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/TagThrottler.h"
#include "fdbserver/TLogInterface.h"

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

	UID id;
	Database db;

	RKMetricsTracker metricsTracker;
	RKConfigurationMonitor configurationMonitor;
	RKRecoveryTracker recoveryTracker;
	RKBlobMonitor blobMonitor;
	RKRateServer rateServer;
	RKRateUpdater normalRateUpdater, batchRateUpdater;
	RKThroughputQuotaCache quotaCache;
	GlobalTagThrottler tagThrottler;

	PromiseStream<Future<Void>> addActor;

	Deque<double> actualTpsHistory;

	Ratekeeper(UID, Database, Reference<AsyncVar<ServerDBInfo> const>, RatekeeperInterface);

	void tryUpdateAutoTagThrottling();

public:
	static Future<Void> run(RatekeeperInterface rkInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo);
};

#endif // FDBSERVER_RATEKEEPER_H
