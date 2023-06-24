/*
 * TagThrottler.h
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

#include "fdbclient/PImpl.h"
#include "fdbclient/ThrottlingId.h"
#include "fdbserver/IRKMetricsTracker.h"
#include "fdbserver/IRKThroughputQuotaCache.h"

class GlobalTagThrottler {
	PImpl<class GlobalTagThrottlerImpl> impl;

public:
	GlobalTagThrottler(IRKMetricsTracker const&, IRKThroughputQuotaCache const&, UID id, int maxFallingBehind);
	~GlobalTagThrottler();

	// Poll the system keyspace looking for updates made through the tag throttling API
	Future<Void> monitorThrottlingChanges();

	// Increment the number of known requests associated with the specified throttling ID
	void addRequests(ThrottlingId, int count);

	// Number of throttling IDs for which throttling information is a rate is being
	// sent to GRV proxies
	int64_t throttleCount() const;

	// Based on the busiest readers and writers in the provided storage queue info, update
	// throttling limits.
	void updateThrottling(StorageQueueInfo const&);

	// For each throttling ID and priority combination, return the throughput limit for the cluster
	// (to be shared across all GRV proxies)
	ThrottlingIdMap<double> getProxyRates(int numProxies);

	// Testing only:
public:
	void removeExpiredThrottlingIds();
	uint32_t throttlingIdsTracked() const;
};
