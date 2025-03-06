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

// Add this declaration
ACTOR Future<Void> bulkDumpTransportCP_impl(Reference<BulkLoadFileSet> srcFileSet,
                                            Reference<BulkLoadFileSet> destFileSet,
                                            size_t fileBytesMax,
                                            UID logId);

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

// Used by DD to generate a SSBulkDumpTask and send to SS
// SS dumps the data based on the configuration of the SSBulkDumpTask
SSBulkDumpTask getSSBulkDumpTask(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                 const BulkDumpState& bulkDumpState);

std::string generateRandomBulkDumpDataFileName(Version version);

// Return two file settings: first: LocalFilePaths; Second: RemoteFilePaths.
// The local file path:
//	<rootLocal>/<relativeFolder>/<dumpVersion>-manifest.txt (must have)
//	<rootLocal>/<relativeFolder>/<dumpVersion>-data.sst (omitted for empty range)
//	<rootLocal>/<relativeFolder>/<dumpVersion>-sample.sst (omitted if data size is too small to have a sample)
// The remote file path:
//	<rootRemote>/<relativeFolder>/<dumpVersion>-manifest.txt (must have)
//	<rootRemote>/<relativeFolder>/<dumpVersion>-data.sst (omitted for empty range)
//	<rootRemote>/<relativeFolder>/<dumpVersion>-sample.sst (omitted if data size is too small to have a sample)
std::pair<BulkLoadFileSet, BulkLoadFileSet> getLocalRemoteFileSetSetting(Version dumpVersion,
                                                                         const std::string& relativeFolder,
                                                                         const std::string& rootLocal,
                                                                         const std::string& rootRemote);

// Persist the complete progress of bulkDump by writing the metadata with Complete phase
// to the bulk dump system key space.
ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState);

// Define bulk dump job folder. Job is set by user. At most one job at a time globally.
std::string generateBulkDumpJobFolder(const UID& jobId);

// Define task folder name.
std::string getBulkDumpJobTaskFolder(const UID& jobId, const UID& taskId);

// Define job root folder.
std::string getBulkLoadJobRoot(const std::string& root, const UID& jobId);

// Define job manifest file content based on job's all BulkLoadManifest.
// Each row is a range sorted by the beginKey. Any two ranges do not have overlapping.
// Col: beginKey, endKey, dataVersion, dataBytes, manifestPath.
// dataVersion should be always valid. dataBytes can be 0 in case of an empty range.
std::string generateBulkLoadJobManifestFileContent(const std::map<Key, BulkLoadManifest>& manifests);

// Generate key-value data, byte sampling data, and manifest file.
// Return BulkLoadManifest metadata (equivalent to content of the manifest file).
// TODO(BulkDump): can cause slow tasks, do the task in a separate thread in the future.
// The size of sortedData is defined at the place of generating the data (getRangeDataToDump).
// The size is configured by MOVE_SHARD_KRM_ROW_LIMIT.
ACTOR Future<BulkLoadManifest> dumpDataFileToLocalDirectory(UID logId,
                                                            std::map<Key, Value> sortedData,
                                                            std::map<Key, Value> sortedSample,
                                                            BulkLoadFileSet localFileSet,
                                                            BulkLoadFileSet remoteFileSet,
                                                            BulkLoadByteSampleSetting byteSampleSetting,
                                                            Version dumpVersion,
                                                            KeyRange dumpRange,
                                                            int64_t dumpBytes,
                                                            int64_t dumpKeyCount,
                                                            BulkLoadType dumpType,
                                                            BulkLoadTransportMethod transportMethod);

ACTOR Future<Void> generateBulkDumpJobManifestFile(std::string workFolder,
                                                   std::string localJobManifestFilePath,
                                                   StringRef content,
                                                   UID logId);

// Upload manifest file for bulkdump job
// Each job has one manifest file including manifest paths of all tasks.
// The local file path:	<localRootLocal>/<jobId>-manifest.txt
// The remote file folder and the name of the file in the remote folder.
ACTOR Future<Void> uploadBulkDumpJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                 std::string localJobManifestFilePath,
                                                 std::string remoteFolder,
                                                 std::string jobManifestFileName,
                                                 UID logId);

// Upload file for each task. Each task is spawned by bulkdump job according to the shard boundary
ACTOR Future<Void> uploadBulkDumpFileSet(BulkLoadTransportMethod transportMethod,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet,
                                         UID logId);

class ParallelismLimitor {
public:
	ParallelismLimitor(int maxParallelism) : maxParallelism(maxParallelism) {}

	inline void decrementTaskCounter() {
		ASSERT(numRunningTasks.get() <= maxParallelism);
		numRunningTasks.set(numRunningTasks.get() - 1);
		ASSERT(numRunningTasks.get() >= 0);
	}

	inline void incrementTaskCounter() {
		ASSERT(numRunningTasks.get() < maxParallelism);
		numRunningTasks.set(numRunningTasks.get() + 1);
		ASSERT(numRunningTasks.get() <= maxParallelism);
	}

	// return true if succeed
	inline bool canStart() {
		if (numRunningTasks.get() < maxParallelism) {
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
