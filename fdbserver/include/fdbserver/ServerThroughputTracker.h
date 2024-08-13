/*
 * ServerThroughputTracker.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/TagThrottle.actor.h"
#include "fdbrpc/Smoother.h"
#include "fdbserver/Ratekeeper.h"

// The ServerThroughputTracker class is responsible for tracking
// every tag's reported throughput across all storage servers
class ServerThroughputTracker {
	enum class OpType {
		READ,
		WRITE,
	};

	class ThroughputCounters {
		Smoother readThroughput;
		Smoother writeThroughput;

	public:
		ThroughputCounters();
		void updateThroughput(double newThroughput, OpType);
		double getThroughput() const;
	};

	std::unordered_map<UID, TransactionTagMap<ThroughputCounters>> throughput;

	void cleanupUnseenStorageServers(std::unordered_set<UID> const& seen);

	static void cleanupUnseenTags(TransactionTagMap<ThroughputCounters>&,
	                              std::unordered_set<TransactionTag> const& seenReadTags,
	                              std::unordered_set<TransactionTag> const& seenWriteTags);

public:
	~ServerThroughputTracker();

	// Returns all tags running significant workload on the specified storage server.
	std::vector<TransactionTag> getTagsAffectingStorageServer(UID storageServerId) const;

	// Updates throughput statistics based on new storage queue info
	void update(Map<UID, StorageQueueInfo> const&);

	// Returns the current throughput for the provided tag on the
	// provided storage server
	Optional<double> getThroughput(UID storageServerId, TransactionTag const&) const;

	// Returns the current cluster-wide throughput for the provided tag
	double getThroughput(TransactionTag const&) const;

	// Returns the current throughput on the provided storage server, summed
	// across all throttling IDs
	Optional<double> getThroughput(UID storageServerId) const;

	// Used to remove a tag which has expired
	void removeTag(TransactionTag const&);

	// Returns the number of storage servers currently being tracked
	int storageServersTracked() const;
};
