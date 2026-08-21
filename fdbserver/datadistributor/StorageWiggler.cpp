/*
 * StorageWiggler.cpp
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

#include "StorageWiggler.h"

#include "DDTeamCollection.h"
#include "fdbclient/RunRYWTransaction.h"
#include "fdbserver/core/Knobs.h"
#include "flow/UnitTest.h"
#include "flow/genericactors.h"

#include <iostream>

Future<Void> StorageWiggler::onCheck() const {
	return delay(MIN_ON_CHECK_DELAY_SEC);
}

// add server to wiggling queue
void StorageWiggler::addServer(const UID& serverId, const StorageMetadataType& metadata) {
	// std::cout << "size: " << pq_handles.size() << " add " << serverId.toString() << " DC: "
	//           << teamCollection->isPrimary() << std::endl;
	ASSERT(!pq_handles.contains(serverId));
	pq_handles[serverId] = wiggle_pq.emplace(metadata, serverId);
}

void StorageWiggler::removeServer(const UID& serverId) {
	// std::cout << "size: " << pq_handles.size() << " remove " << serverId.toString() << " DC: "
	//           << teamCollection->isPrimary() << std::endl;
	if (contains(serverId)) { // server haven't been popped
		auto handle = pq_handles.at(serverId);
		pq_handles.erase(serverId);
		wiggle_pq.erase(handle);
	}
}

void StorageWiggler::updateMetadata(const UID& serverId, const StorageMetadataType& metadata) {
	//	std::cout << "size: " << pq_handles.size() << " update " << serverId.toString()
	//	          << " DC: " << teamCollection->isPrimary() << std::endl;
	auto handle = pq_handles.at(serverId);
	if ((*handle).first == metadata) {
		return;
	}
	wiggle_pq.update(handle, std::make_pair(metadata, serverId));
}

bool StorageWiggler::necessary(const UID& serverId, const StorageMetadataType& metadata) const {
	return metadata.wrongConfiguredForWiggle ||
	       (now() - metadata.createdTime > SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC);
}

Optional<UID> StorageWiggler::getNextServerId(bool necessaryOnly) {
	if (!wiggle_pq.empty()) {
		auto [metadata, id] = wiggle_pq.top();
		if (necessaryOnly && !necessary(id, metadata)) {
			return {};
		}
		wiggle_pq.pop();
		pq_handles.erase(id);
		return Optional<UID>(id);
	}
	return Optional<UID>();
}

Future<Void> StorageWiggler::resetStats() {
	metrics.reset();
	return runRYWTransaction(
	    teamCollection->dbContext(), [this](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		    return wiggleData.resetStorageWiggleMetrics(tr, PrimaryRegion(teamCollection->isPrimary()), metrics);
	    });
}

Future<Void> StorageWiggler::restoreStats() {
	auto readFuture = wiggleData.storageWiggleMetrics(PrimaryRegion(teamCollection->isPrimary()))
	                      .getD(teamCollection->dbContext().getReference(), Snapshot::False, metrics);
	return store(metrics, readFuture);
}

Future<Void> StorageWiggler::startWiggle() {
	metrics.last_wiggle_start = StorageMetadataType::currentTime();
	if (shouldStartNewRound()) {
		metrics.last_round_start = metrics.last_wiggle_start;
	}
	return runRYWTransaction(
	    teamCollection->dbContext(), [this](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		    return wiggleData.updateStorageWiggleMetrics(tr, metrics, PrimaryRegion(teamCollection->isPrimary()));
	    });
}

Future<Void> StorageWiggler::finishWiggle() {
	updateFinishWiggleMetrics(StorageMetadataType::currentTime());
	return runRYWTransaction(
	    teamCollection->dbContext(), [this](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		    return wiggleData.updateStorageWiggleMetrics(tr, metrics, PrimaryRegion(teamCollection->isPrimary()));
	    });
}

void StorageWiggler::updateFinishWiggleMetrics(double finishTime) {
	metrics.last_wiggle_finish = finishTime;
	metrics.finished_wiggle += 1;
	auto duration = metrics.last_wiggle_finish - metrics.last_wiggle_start;
	metrics.smoothed_wiggle_duration.setTotal((double)duration);

	if (shouldFinishRound()) {
		metrics.last_round_finish = metrics.last_wiggle_finish;
		metrics.finished_round += 1;
		duration = metrics.last_round_finish - metrics.last_round_start;
		metrics.smoothed_round_duration.setTotal((double)duration);
	}
}

TEST_CASE("/DataDistribution/StorageWiggler/Order") {
	StorageWiggler wiggler(nullptr);
	double startTime = now() - SERVER_KNOBS->DD_STORAGE_WIGGLE_MIN_SS_AGE_SEC - 0.4;
	wiggler.addServer(UID(1, 0), StorageMetadataType(startTime, KeyValueStoreType::SSD_BTREE_V2));
	wiggler.addServer(UID(2, 0), StorageMetadataType(startTime + 0.1, KeyValueStoreType::MEMORY, true));
	wiggler.addServer(UID(3, 0), StorageMetadataType(startTime + 0.2, KeyValueStoreType::SSD_ROCKSDB_V1, true));
	wiggler.addServer(UID(4, 0), StorageMetadataType(startTime + 0.3, KeyValueStoreType::SSD_BTREE_V2));

	std::vector<UID> correctOrder{ UID(2, 0), UID(3, 0), UID(1, 0), UID(4, 0) };
	for (int i = 0; i < correctOrder.size(); ++i) {
		auto id = wiggler.getNextServerId();
		std::cout << "Get " << id.get().shortString() << "\n";
		ASSERT(id == correctOrder[i]);
	}
	ASSERT(!wiggler.getNextServerId().present());
	return Void();
}

TEST_CASE("/DataDistribution/StorageWiggler/FinishUpdatesMetrics") {
	for (bool finishRound : { false, true }) {
		StorageWiggler wiggler(nullptr);
		wiggler.metrics.last_wiggle_start = 100;
		wiggler.metrics.last_round_start = 80;
		wiggler.metrics.last_round_finish = 70;
		wiggler.metrics.finished_wiggle = 2;
		wiggler.metrics.finished_round = 1;
		wiggler.metrics.smoothed_wiggle_duration.reset(11);
		wiggler.metrics.smoothed_round_duration.reset(17);
		if (!finishRound) {
			wiggler.addServer(UID(1, 0), StorageMetadataType(79, KeyValueStoreType::SSD_BTREE_V2));
		}

		wiggler.updateFinishWiggleMetrics(130);
		ASSERT_EQ(wiggler.metrics.last_wiggle_finish, 130);
		ASSERT_EQ(wiggler.metrics.finished_wiggle, 3);
		ASSERT_EQ(wiggler.metrics.smoothed_wiggle_duration.getTotal(), 30);
		ASSERT_EQ(wiggler.metrics.last_round_finish, finishRound ? 130 : 70);
		ASSERT_EQ(wiggler.metrics.finished_round, finishRound ? 2 : 1);
		ASSERT_EQ(wiggler.metrics.smoothed_round_duration.getTotal(), finishRound ? 50 : 17);
	}
	return Void();
}
