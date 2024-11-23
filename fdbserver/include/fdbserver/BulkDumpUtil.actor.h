/*
 * BulkDumpUtil.actor.h
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

#include <cstdint>
#include <string>
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULKDUMPUTIL_ACTOR_G_H)
#define FDBSERVER_BULKDUMPUTIL_ACTOR_G_H
#include "fdbserver/BulkDumpUtil.actor.g.h"
#elif !defined(FDBSERVER_BULKDUMPUTIL_ACTOR_H)
#define FDBSERVER_BULKDUMPUTIL_ACTOR_H
#pragma once

#include "fdbclient/BulkDumping.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/actorcompiler.h" // has to be last include

struct SSBulkDumpTask {
	SSBulkDumpTask(const StorageServerInterface& targetServer,
	               const std::vector<UID>& checksumServers,
	               const BulkDumpState& bulkDumpState)
	  : targetServer(targetServer), checksumServers(checksumServers), bulkDumpState(bulkDumpState){};

	std::string toString() const {
		return "[BulkDumpState]: " + bulkDumpState.toString() + ", [TargetServer]: " + targetServer.toString() +
		       ", [ChecksumServers]: " + describe(checksumServers);
	}

	StorageServerInterface targetServer;
	std::vector<UID> checksumServers;
	BulkDumpState bulkDumpState;
};

struct BulkDumpJobManifestFileSet {
	UID jobId;
	std::string manifestPath;
	std::vector<std::string> taskManifestFiles;

	BulkDumpJobManifestFileSet() = default;

	BulkDumpJobManifestFileSet(const UID& jobId,
	                           const std::string& manifestPath,
	                           const std::vector<std::string>& taskManifestFiles)
	  : jobId(jobId), manifestPath(manifestPath), taskManifestFiles(taskManifestFiles) {
		ASSERT(isValid());
	}

	bool isValid() const {
		if (!jobId.isValid()) {
			return false;
		}
		if (manifestPath.size() == 0) {
			return false;
		}
		for (const auto& taskManifestFile : taskManifestFiles) {
			if (!taskManifestFile.empty()) {
				return false;
			}
		}
		return true;
	}
};

// Define the configuration of bytes sampling
// Use for setting manifest file
struct ByteSampleSetting {
	int version = 0;
	std::string method;
	int factor = 0;
	int overhead = 0;
	double minimalProbability = 0.0;
	ByteSampleSetting(int version, const std::string& method, int factor, int overhead, double minimalProbability)
	  : version(version), method(method), factor(factor), overhead(overhead), minimalProbability(minimalProbability) {
		ASSERT(isValid());
	}
	bool isValid() const {
		if (method.size() == 0) {
			return false;
		}
		return true;
	}
	std::string toString() const {
		return "[ByteSampleVersion]: " + std::to_string(version) + ", [ByteSampleMethod]: " + method +
		       ", [ByteSampleFactor]: " + std::to_string(factor) +
		       ", [ByteSampleOverhead]: " + std::to_string(overhead) +
		       ", [ByteSampleMinimalProbability]: " + std::to_string(minimalProbability);
	}
};

// Define the metadata of bulkdump manifest file
// The file is uploaded along with the data files
struct BulkDumpManifest {
	BulkDumpFileSet fileSet;
	Key beginKey;
	Key endKey;
	Version version;
	std::string checksum;
	int64_t bytes;
	ByteSampleSetting byteSampleSetting;

	BulkDumpManifest(const BulkDumpFileSet& fileSet,
	                 const Key& beginKey,
	                 const Key& endKey,
	                 const Version& version,
	                 const std::string& checksum,
	                 int64_t bytes,
	                 const ByteSampleSetting& byteSampleSetting)
	  : fileSet(fileSet), beginKey(beginKey), endKey(endKey), version(version), checksum(checksum), bytes(bytes),
	    byteSampleSetting(byteSampleSetting) {}

	// Generating human readable string to stored in the manifest file
	std::string toString() const {
		return fileSet.toString() + ", [BeginKey]: " + beginKey.toHexString() + ", [EndKey]: " + endKey.toHexString() +
		       ", [Version]: " + std::to_string(version) + ", [Checksum]: " + checksum +
		       ", [Bytes]: " + std::to_string(bytes) + ", " + byteSampleSetting.toString();
	}
};

// Used by DD to generate a SSBulkDumpTask and send to SS
// SS dumps the data based on the configuration of the SSBulkDumpTask
SSBulkDumpTask getSSBulkDumpTask(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                 const BulkDumpState& bulkDumpState);

std::string generateRandomBulkDumpDataFileName(Version version);

// Return two file settings: first: LocalFilePaths; Second: RemoteFilePaths.
// The local file path:
//	<rootLocal>/<relativeFolder>/<dumpVersion>-manifest.sst
//	<rootLocal>/<relativeFolder>/<dumpVersion>-data.sst
//	<rootLocal>/<relativeFolder>/<dumpVersion>-sample.sst
// The remote file path:
//	<rootRemote>/<relativeFolder>/<dumpVersion>-manifest.sst
//	<rootRemote>/<relativeFolder>/<dumpVersion>-data.sst
//	<rootRemote>/<relativeFolder>/<dumpVersion>-sample.sst
std::pair<BulkDumpFileSet, BulkDumpFileSet> getLocalRemoteFileSetSetting(Version dumpVersion,
                                                                         const std::string& relativeFolder,
                                                                         const std::string& rootLocal,
                                                                         const std::string& rootRemote);

// The size of sortedKVS is defined at the place of generating the data (getRangeDataToDump).
// The size is configured by MOVE_SHARD_KRM_ROW_LIMIT.
BulkDumpFileSet dumpDataFileToLocalDirectory(UID logId,
                                             const std::map<Key, Value>& sortedKVS,
                                             const BulkDumpFileSet& localFileSet,
                                             const BulkDumpFileSet& remoteFileSet,
                                             const ByteSampleSetting& byteSampleSetting,
                                             Version dumpVersion,
                                             const KeyRange& dumpRange,
                                             int64_t dumpBytes);

ACTOR Future<Void> uploadFiles(BulkDumpTransportMethod transportMethod,
                               BulkDumpFileSet sourceFileSet,
                               BulkDumpFileSet destinationFileSet,
                               UID logId);

// Persist the complete progress of bulkDump by writing the metadata with Complete phase
// to the bulk dump system key space
ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState);

class ParallelismLimitor {
public:
	ParallelismLimitor(int maxParallelism) : maxParallelism(maxParallelism) {}

	inline void decrementTaskCounter() {
		ASSERT(numRunningTasks.get() <= maxParallelism);
		numRunningTasks.set(numRunningTasks.get() - 1);
		ASSERT(numRunningTasks.get() >= 0);
	}

	// return true if succeed
	inline bool tryIncrementTaskCounter() {
		if (numRunningTasks.get() < maxParallelism) {
			numRunningTasks.set(numRunningTasks.get() + 1);
			return true;
		} else {
			return false;
		}
	}

	inline Future<Void> waitUntilCounterChanged() const { return numRunningTasks.onChange(); }

private:
	AsyncVar<int> numRunningTasks;
	int maxParallelism;
};

#include "flow/unactorcompiler.h"
#endif
