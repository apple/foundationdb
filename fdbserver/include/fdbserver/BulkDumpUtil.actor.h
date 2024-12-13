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

#include "flow/Trace.h"
#include <cstddef>
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
#include "flow/TDMetric.actor.h"
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
std::pair<BulkDumpFileSet, BulkDumpFileSet> getLocalRemoteFileSetSetting(Version dumpVersion,
                                                                         const std::string& relativeFolder,
                                                                         const std::string& rootLocal,
                                                                         const std::string& rootRemote);

// Persist the complete progress of bulkDump by writing the metadata with Complete phase
// to the bulk dump system key space.
ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState);

// Define bulk dump job folder. Job is set by user. At most one job at a time globally.
std::string generateBulkDumpJobFolder(const UID& jobId);

// Define job manifest file name.
std::string getJobManifestFileName(const UID& jobId);

// Define task folder name.
std::string getBulkDumpTaskFolder(const UID& taskId);

// Define job root folder.
std::string getBulkDumpJobRoot(const std::string& root, const UID& jobId);

// Define job manifest file content based on job's all BulkDumpManifest.
// Each row is a range sorted by the beginKey. Any two ranges do not have overlapping.
// Col: beginKey, endKey, dataVersion, dataBytes, manifestPath.
// dataVersion should be always valid. dataBytes can be 0 in case of an empty range.
std::string generateJobManifestFileContent(const std::map<Key, BulkDumpManifest>& manifests);

// The size of sortedData is defined at the place of generating the data (getRangeDataToDump).
// The size is configured by MOVE_SHARD_KRM_ROW_LIMIT.
BulkDumpManifest dumpDataFileToLocalDirectory(UID logId,
                                              const std::map<Key, Value>& sortedData,
                                              const std::map<Key, Value>& sortedSample,
                                              const BulkDumpFileSet& localFileSet,
                                              const BulkDumpFileSet& remoteFileSet,
                                              const ByteSampleSetting& byteSampleSetting,
                                              Version dumpVersion,
                                              const KeyRange& dumpRange,
                                              int64_t dumpBytes);

void generateBulkDumpJobManifestFile(const std::string& workFolder,
                                     const std::string& localJobManifestFilePath,
                                     const std::string& content,
                                     const UID& logId);

// Upload manifest file for bulkdump job
// Each job has one manifest file including manifest paths of all tasks.
// The local file path:
//	<rootLocal>/<jobId>-manifest.txt
// The remote file path:
//	<rootRemote>/<jobId>-manifest.txt
void uploadBulkDumpJobManifestFile(BulkDumpTransportMethod transportMethod,
                                   const std::string& localJobManifestFilePath,
                                   const std::string& remoteJobManifestFilePath,
                                   UID logId);

// Upload file for each task. Each task is spawned by bulkdump job according to the shard boundary
ACTOR Future<Void> uploadBulkDumpFileSet(BulkDumpTransportMethod transportMethod,
                                         BulkDumpFileSet sourceFileSet,
                                         BulkDumpFileSet destinationFileSet,
                                         UID logId);

// Erase file folder
void clearFileFolder(const std::string& folderPath);

// Define the bulkdump job manifest header
struct BulkDumpJobManifestHeader {
	BulkDumpJobManifestHeader() = default;

	BulkDumpJobManifestHeader(const std::string& rawString) {
		std::vector<std::string> parts = splitString(rawString, ", ");
		if (parts.size() != 2) {
			TraceEvent(SevError, "ParseBulkDumpJobManifestHeaderError").detail("RawString", rawString);
			ASSERT(false);
		}
		manifestCount = std::stoull(stringRemovePrefix(parts[0], "Manifest count: "));
		rootFolder = stringRemovePrefix(parts[1], "Root: ");
		ASSERT(!rootFolder.empty());
	}

	std::string toString() {
		return "[BulkDumpJobManifestHeader]: [ManifestCount]: " + std::to_string(manifestCount) +
		       ", [RootFolder]: " + rootFolder;
	}

	size_t manifestCount = 0;
	std::string rootFolder = "";
};

// Define the bulkdump job manifest entry per range
struct BulkDumpJobManifestEntry {
	BulkDumpJobManifestEntry(const std::string& rawString) {
		std::vector<std::string> parts = splitString(rawString, ", ");
		if (parts.size() != 5) {
			TraceEvent(SevError, "ParseBulkDumpJobManifestEntryError").detail("RawString", rawString);
			ASSERT(false);
		}
		beginKey = getKeyFromHexString(parts[0]);
		endKey = getKeyFromHexString(parts[1]);
		version = std::stoll(parts[2]);
		bytes = std::stoull(parts[3]);
		relativePath = parts[4];
	}

	KeyRange getRange() const { return Standalone(KeyRangeRef(beginKey, endKey)); }

	std::string toString() const {
		return "[BulkDumpJobManifestEntry]: [BeginKey]: " + beginKey.toFullHexStringPlain() +
		       ", [EndKey]: " + endKey.toFullHexStringPlain() + ", [RelativePath]: " + relativePath +
		       ", [Version]: " + std::to_string(version) + ", [Bytes]: " + std::to_string(bytes);
	}

	Key beginKey;
	Key endKey;
	std::string relativePath;
	Version version;
	size_t bytes;
};

// Download bulkdump job manifest file which is generated when dumping the data
ACTOR Future<Void> downloadBulkDumpJobManifestFile(BulkDumpTransportMethod transportMethod,
                                                   std::string localJobManifestFilePath,
                                                   std::string remoteJobManifestFilePath,
                                                   UID logId);

// Extract manifests from files
ACTOR Future<std::vector<BulkDumpManifest>> extractBulkDumpJobManifests(std::string localJobManifestFilePath,
                                                                        std::vector<KeyRange> ranges,
                                                                        std::string localFolder,
                                                                        BulkDumpTransportMethod transportMethod,
                                                                        UID logId);

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
