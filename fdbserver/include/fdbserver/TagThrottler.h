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

class ITagThrottler {
public:
	virtual ~ITagThrottler() = default;

	// Poll the system keyspace looking for updates made through the tag throttling API
	virtual Future<Void> monitorThrottlingChanges() = 0;

	// Increment the number of known requests associated with the specified tag
	virtual void addRequests(TransactionTag tag, int count) = 0;

	// This throttled tag change ID is used to coordinate updates with the GRV proxies
	virtual uint64_t getThrottledTagChangeId() const = 0;

	// For each tag and priority combination, return the throughput limit and expiration time
	// Also, erase expired tags
	virtual PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() = 0;

	// For each tag and priority combination, return the throughput limit for the cluster
	// (to be shared across all GRV proxies)
	virtual TransactionTagMap<double> getProxyRates(int numProxies) = 0;

	virtual int64_t autoThrottleCount() const = 0;
	virtual uint32_t busyReadTagCount() const = 0;
	virtual uint32_t busyWriteTagCount() const = 0;
	virtual int64_t manualThrottleCount() const = 0;
	virtual bool isAutoThrottlingEnabled() const = 0;

	// Based on the busiest read and write tags in the provided storage queue info, these methods
	// update tag throttling limits. Unfortunately, the two effective interfaces of the two
	// implementations of ITagThrottler (GlobalTagThrottler and TagThrottler) have diveraged over
	// time. As a result, exactly one of the below methods is a noop for each implementation.
	virtual Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const&) = 0;
	virtual void updateThrottling(Map<UID, StorageQueueInfo> const&) = 0;
};

class TagThrottler : public ITagThrottler {
	PImpl<class TagThrottlerImpl> impl;

public:
	TagThrottler(Database db, UID id);
	~TagThrottler();

	Future<Void> monitorThrottlingChanges() override;
	void addRequests(TransactionTag tag, int count) override;
	uint64_t getThrottledTagChangeId() const override;
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() override;
	TransactionTagMap<double> getProxyRates(int numProxies) override { throw not_implemented(); }
	int64_t autoThrottleCount() const override;
	uint32_t busyReadTagCount() const override;
	uint32_t busyWriteTagCount() const override;
	int64_t manualThrottleCount() const override;
	bool isAutoThrottlingEnabled() const override;
	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const&) override;
	void updateThrottling(Map<UID, StorageQueueInfo> const&) override {}
};

class GlobalTagThrottler : public ITagThrottler {
	PImpl<class GlobalTagThrottlerImpl> impl;

public:
	GlobalTagThrottler(Database db, UID id, int maxFallingBehind, double limitingThreshold);
	~GlobalTagThrottler();

	Future<Void> monitorThrottlingChanges() override;
	void addRequests(TransactionTag tag, int count) override;
	uint64_t getThrottledTagChangeId() const override;

	int64_t autoThrottleCount() const override;
	uint32_t busyReadTagCount() const override;
	uint32_t busyWriteTagCount() const override;
	int64_t manualThrottleCount() const override;
	bool isAutoThrottlingEnabled() const override;

	Future<Void> tryUpdateAutoThrottling(StorageQueueInfo const&) override { return Void(); }
	void updateThrottling(Map<UID, StorageQueueInfo> const&) override;
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> getClientRates() override;
	TransactionTagMap<double> getProxyRates(int numProxies) override;

	// Testing only:
public:
	void setQuota(TransactionTagRef, ThrottleApi::TagQuotaValue const&);
	void removeQuota(TransactionTagRef);
	void removeExpiredTags();
	uint32_t tagsTracked() const;
};
