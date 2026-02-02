/*
 * BackupPartitionManager.actor.cpp
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

#include "fdbserver/BackupPartitionManager.actor.h"
#include "fdbserver/BackupPartitionMap.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/Knobs.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Actor that monitors for backup partition requests and creates partition maps
ACTOR Future<Void> backupPartitionManager(Reference<DataDistributor> self) {
	state Transaction tr(self->txnProcessor->context());
	
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			
			// Check if backup partitioning is requested
			Optional<Value> partitionRequired = wait(tr.get(backupPartitionRequiredKey));
			
			if (partitionRequired.present()) {
				TraceEvent("BackupPartitionRequested", self->ddId).log();
				
				// Get current version for the partition map
				Version currentVersion = wait(tr.getReadVersion());
				
				// Create backup worker tags (using backup locality)
				std::vector<Tag> backupTags;
				for (int i = 0; i < SERVER_KNOBS->BACKUP_NUM_OF_PARTITIONS; i++) {
					backupTags.push_back(Tag(tagLocalityBackup, i));
				}
				
				// Create the partition map
				BackupPartitionMap partitionMap = createPartitionMap(
					SERVER_KNOBS->BACKUP_NUM_OF_PARTITIONS, 
					backupTags, 
					currentVersion
				);
				
				// Store the partition map in the system keyspace
				Key partitionMapKey = backupPartitionMapKeyFor(currentVersion);
				Value partitionMapValue = backupPartitionMapValue(partitionMap);
				tr.set(partitionMapKey, partitionMapValue);
				
				// Clear the partition request key
				tr.clear(backupPartitionRequiredKey);
				
				wait(tr.commit());
				
				TraceEvent("BackupPartitionMapCreated", self->ddId)
					.detail("Version", currentVersion)
					.detail("NumPartitions", SERVER_KNOBS->BACKUP_NUM_OF_PARTITIONS);
				
				// Broadcast the partition map to CommitProxies
				wait(broadcastPartitionMapToProxies(self, partitionMap));
				
			} else {
				// No partition request, wait for changes
				Future<Void> watchFuture = tr.watch(backupPartitionRequiredKey);
				wait(tr.commit());
				wait(watchFuture);
			}
			
			tr.reset();
			
		} catch (Error& e) {
			TraceEvent("BackupPartitionManagerError", self->ddId).error(e);
			wait(tr.onError(e));
		}
	}
}

// Actor that broadcasts partition map to all CommitProxies
ACTOR Future<Void> broadcastPartitionMapToProxies(Reference<DataDistributor> self, BackupPartitionMap partitionMap) {
	state std::vector<Future<Void>> broadcastFutures;
	
	// Get current database info to find CommitProxies
	ServerDBInfo dbInfo = wait(self->db->onChange());
	
	// Send partition map to all CommitProxies
	for (const auto& proxy : dbInfo.client.commitProxies) {
		BackupPartitionMapRequest req;
		req.partitionMap = partitionMap;
		req.version = partitionMap.getVersion();
		
		broadcastFutures.push_back(proxy.updateBackupPartitionMap.getReply(req));
	}
	
	// Wait for all proxies to acknowledge
	wait(waitForAll(broadcastFutures));
	
	TraceEvent("BackupPartitionMapBroadcast", self->ddId)
		.detail("Version", partitionMap.getVersion())
		.detail("NumProxies", dbInfo.client.commitProxies.size());
	
	return Void();
}

// Actor that periodically cleans up old partition maps
ACTOR Future<Void> cleanupOldPartitionMaps(Reference<DataDistributor> self) {
	loop {
		try {
			state Transaction tr(self->txnProcessor->context());
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			
			// Get current version
			Version currentVersion = wait(tr.getReadVersion());
			Version cutoffVersion = currentVersion - (SERVER_KNOBS->BACKUP_PARTITION_MAP_CACHE_TIMEOUT * SERVER_KNOBS->VERSIONS_PER_SECOND);
			
			// Read all partition map keys
			RangeResult partitionMaps = wait(tr.getRange(backupPartitionMapKeys, CLIENT_KNOBS->TOO_MANY));
			
			// Delete old partition maps
			int deletedCount = 0;
			for (const auto& kv : partitionMaps) {
				Version mapVersion = BinaryReader::fromStringRef<Version>(
					kv.key.removePrefix(backupPartitionMapPrefix), Unversioned());
				
				if (mapVersion < cutoffVersion) {
					tr.clear(kv.key);
					deletedCount++;
				}
			}
			
			if (deletedCount > 0) {
				wait(tr.commit());
				TraceEvent("BackupPartitionMapCleanup", self->ddId)
					.detail("DeletedCount", deletedCount)
					.detail("CutoffVersion", cutoffVersion);
			}
			
		} catch (Error& e) {
			TraceEvent("BackupPartitionMapCleanupError", self->ddId).error(e);
		}
		
		// Wait before next cleanup
		wait(delay(SERVER_KNOBS->BACKUP_PARTITION_MAP_CACHE_TIMEOUT));
	}
}