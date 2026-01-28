/*
 * RangePartitionBackupManager.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BackupProgress.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Range Partition Backup Manager
// Manages the configuration and assignment of key ranges to backup workers
// for range-partitioned backup mutation logs

class RangePartitionBackupManager {
public:
	RangePartitionBackupManager(Database cx, UID backupID) : cx(cx), backupID(backupID) {}

	// Configure range partitioning for a backup
	ACTOR static Future<Void> configureRangePartitioning(RangePartitionBackupManager* self,
	                                                      std::vector<KeyRange> ranges,
	                                                      int maxRangesPerWorker,
	                                                      bool enableRangePartitioning) {
		state Transaction tr(self->cx);
		state Key configKey = backupRangeConfigKeyFor(self->backupID);
		state RangePartitionConfig config(self->backupID, ranges, maxRangesPerWorker, enableRangePartitioning);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				tr.set(configKey, backupRangeConfigValue(config));
				wait(tr.commit());

				TraceEvent("RangePartitionBackupConfigured")
				    .detail("BackupID", self->backupID)
				    .detail("RangeCount", ranges.size())
				    .detail("MaxRangesPerWorker", maxRangesPerWorker)
				    .detail("EnableRangePartitioning", enableRangePartitioning);

				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Assign ranges to backup workers
	ACTOR static Future<Void> assignRangesToWorkers(RangePartitionBackupManager* self,
	                                                 std::vector<UID> workerIDs,
	                                                 std::vector<KeyRange> ranges) {
		state Transaction tr(self->cx);
		state int rangeIndex = 0;
		state int workerIndex = 0;

		// Get configuration to determine max ranges per worker
		state Optional<Value> configValue = wait(tr.get(backupRangeConfigKeyFor(self->backupID)));
		if (!configValue.present()) {
			TraceEvent(SevWarn, "RangePartitionConfigNotFound").detail("BackupID", self->backupID);
			return Void();
		}

		state RangePartitionConfig config = decodeBackupRangeConfigValue(configValue.get());
		if (!config.enableRangePartitioning) {
			TraceEvent("RangePartitioningDisabled").detail("BackupID", self->backupID);
			return Void();
		}

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				// Distribute ranges among workers
				std::map<UID, std::vector<KeyRange>> workerAssignments;
				for (int i = 0; i < ranges.size(); i++) {
					UID workerID = workerIDs[i % workerIDs.size()];
					workerAssignments[workerID].push_back(ranges[i]);
				}

				// Save assignments to system keyspace
				for (const auto& [workerID, assignedRanges] : workerAssignments) {
					Key assignmentKey = backupRangeAssignmentKeyFor(workerID);
					Value assignmentValue = backupRangeAssignmentValue(assignedRanges);
					tr.set(assignmentKey, assignmentValue);

					TraceEvent("RangePartitionAssignment")
					    .detail("WorkerID", workerID)
					    .detail("AssignedRanges", assignedRanges.size())
					    .detail("BackupID", self->backupID);
				}

				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Get range assignments for a specific worker
	ACTOR static Future<std::vector<KeyRange>> getWorkerRangeAssignments(RangePartitionBackupManager* self,
	                                                                      UID workerID) {
		state Transaction tr(self->cx);
		state Key assignmentKey = backupRangeAssignmentKeyFor(workerID);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				Optional<Value> assignmentValue = wait(tr.get(assignmentKey));
				if (!assignmentValue.present()) {
					return std::vector<KeyRange>();
				}

				std::vector<KeyRange> ranges = decodeBackupRangeAssignmentValue(assignmentValue.get());
				return ranges;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Monitor range partition backup progress
	ACTOR static Future<Void> monitorRangePartitionProgress(RangePartitionBackupManager* self) {
		state Transaction tr(self->cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

				// Get all range progress entries
				RangeResult results = wait(tr.getRange(backupRangeProgressKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

				std::map<UID, std::vector<RangeBackupStatus>> workerProgress;
				for (auto& it : results) {
					auto [workerID, beginKey] = decodeBackupRangeProgressKey(it.key);
					RangeBackupStatus status = decodeBackupRangeProgressValue(it.value);
					workerProgress[workerID].push_back(status);
				}

				// Log progress summary
				for (const auto& [workerID, statuses] : workerProgress) {
					Version minVersion = invalidVersion;
					Version maxVersion = 0;
					for (const auto& status : statuses) {
						if (minVersion == invalidVersion || status.version < minVersion) {
							minVersion = status.version;
						}
						if (status.version > maxVersion) {
							maxVersion = status.version;
						}
					}

					TraceEvent("RangePartitionProgressSummary")
					    .detail("WorkerID", workerID)
					    .detail("RangeCount", statuses.size())
					    .detail("MinVersion", minVersion)
					    .detail("MaxVersion", maxVersion)
					    .detail("BackupID", self->backupID);
				}

				wait(delay(30.0)); // Monitor every 30 seconds
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

private:
	Database cx;
	UID backupID;
};

// Helper function to create and configure range partition backup
ACTOR Future<Void> setupRangePartitionedBackup(Database cx,
                                                UID backupID,
                                                std::vector<KeyRange> ranges,
                                                std::vector<UID> workerIDs,
                                                int maxRangesPerWorker) {
	state RangePartitionBackupManager manager(cx, backupID);

	// Configure range partitioning
	wait(RangePartitionBackupManager::configureRangePartitioning(&manager, ranges, maxRangesPerWorker, true));

	// Assign ranges to workers
	wait(RangePartitionBackupManager::assignRangesToWorkers(&manager, workerIDs, ranges));

	// Start monitoring progress
	wait(RangePartitionBackupManager::monitorRangePartitionProgress(&manager));

	return Void();
}