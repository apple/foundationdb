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
#include "fdbserver/Ratekeeper.h"

class TagThrottler {
	PImpl<class TagThrottlerImpl> impl;

public:
	TagThrottler(Database db, UID id);
	~TagThrottler();

	// Poll the system keyspace looking for updates made through the tag throttling API
	Future<Void> monitorThrottlingChanges();

	// Increment the number of known requests associated with the specified tag
	void addRequests(TransactionTag tag, int count);

	// This throttled tag change ID is used to coordinate updates with the GRV proxies
	uint64_t getThrottledTagChangeId() const;

	// For each tag and priority combination, return the throughput limit and expiration time
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates();

	int64_t autoThrottleCount() const;
	uint32_t busyReadTagCount() const;
	uint32_t busyWriteTagCount() const;
	int64_t manualThrottleCount() const;
	bool isAutoThrottlingEnabled() const;

	// Based on the busiest read and write tags in the provided storage queue info, update
	// tag throttling limits.
	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const&);
};
