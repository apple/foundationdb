/*
 * BulkLoadAndDumpUtil.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULKLOADANDDUMPUTIL_ACTOR_G_H)
#define FDBSERVER_BULKLOADANDDUMPUTIL_ACTOR_G_H
#include "fdbserver/BulkLoadAndDumpUtil.actor.g.h"
#elif !defined(FDBSERVER_BULKLOADANDDUMPUTIL_ACTOR_H)
#define FDBSERVER_BULKLOADANDDUMPUTIL_ACTOR_H
#pragma once

#include "fdbclient/BulkLoadAndDump.h"
#include "flow/actorcompiler.h" // has to be last include

class ParallelismLimitor {
public:
	ParallelismLimitor(int maxParallelism, const std::string& context)
	  : maxParallelism(maxParallelism), context(context) {}

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
	std::string context;
};

struct SSBulkLoadFileSet {
	std::unordered_set<std::string> dataFileList;
	Optional<std::string> bytesSampleFile;
	std::string folder;
	SSBulkLoadFileSet() = default;
	std::string toString() {
		std::string res = "SSBulkLoadFileSet: [DataFiles]: " + describe(dataFileList);
		if (bytesSampleFile.present()) {
			res = res + ", [BytesSampleFile]: " + bytesSampleFile.get();
		}
		res = res + ", [Folder]: " + folder;
		return res;
	}
};

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

ACTOR Future<Optional<BulkLoadTaskState>> getBulkLoadTaskStateFromDataMove(Database cx, UID dataMoveId, UID logId);

ACTOR Future<Optional<std::string>> getBytesSamplingFromSSTFiles(std::string folderToGenerate,
                                                                 std::unordered_set<std::string> dataFiles,
                                                                 UID logId);

// Used by DD to generate a SSBulkDumpTask and send to SS
// SS dumps the data based on the configuration of the SSBulkDumpTask
SSBulkDumpTask getSSBulkDumpTask(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                 const BulkDumpState& bulkDumpState);

// Used by bulk dumping
// The size of sortedData is defined at the place of generating the data (getRangeDataToDump).
// The size is configured by MOVE_SHARD_KRM_ROW_LIMIT.
BulkLoadManifest dumpDataFileToLocal(UID logId,
                                     const std::map<Key, Value>& sortedData,
                                     const std::map<Key, Value>& sortedSample,
                                     const BulkLoadFileSet& localFileSet,
                                     const BulkLoadFileSet& remoteFileSet,
                                     const BulkLoadByteSampleSetting& byteSampleSetting,
                                     Version dumpVersion,
                                     const KeyRange& dumpRange,
                                     int64_t dumpBytes);

// Upload file for each task. Each task is spawned by bulkdump job according to the shard boundary
ACTOR Future<Void> uploadBulkLoadFileSet(BulkLoadTransportMethod transportMethod,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet,
                                         UID logId);

// Download file for each task
ACTOR Future<SSBulkLoadFileSet> downloadBulkLoadFileSet(BulkLoadTransportMethod transportMethod,
                                                        std::string dir,
                                                        BulkLoadTaskState bulkLoadTaskState,
                                                        size_t fileBytesMax,
                                                        UID logId);

// Download job manifest file which is generated when dumping the data
ACTOR Future<Void> downloadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                   std::string localJobManifestFilePath,
                                                   std::string remoteJobManifestFilePath,
                                                   UID logId);

// Upload job manifest file when dumping the data
ACTOR Future<Void> uploadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                 std::string localJobManifestFilePath,
                                                 std::string remoteJobManifestFilePath,
                                                 std::map<Key, BulkLoadManifest> manifests,
                                                 UID logId);

// Extract manifests from job manifest file with the input range
ACTOR Future<std::vector<BulkLoadManifest>> getBulkLoadManifestMetadataFromFiles(
    std::string localJobManifestFilePath,
    KeyRange range,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    UID logId);

// Persist the complete progress of bulkDump by writing the metadata with Complete phase
// to the bulk dump system key space.
ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState);

#include "flow/unactorcompiler.h"
#endif
