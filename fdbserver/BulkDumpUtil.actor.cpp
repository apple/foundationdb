/*
 * BulkDumpUtils.actor.cpp
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

#include "fdbclient/BulkDumping.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/Optional.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/flow.h"

SSBulkDumpTask getSSBulkDumpTask(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                 const BulkDumpState& bulkDumpState) {
	StorageServerInterface targetServer;
	std::vector<UID> checksumServers;
	int dcid = 0;
	for (const auto& [_, dcServers] : locations) {
		if (dcid == 0) {
			const int idx = deterministicRandom()->randomInt(0, dcServers.size());
			targetServer = dcServers[idx];
		}
		for (int i = 0; i < dcServers.size(); i++) {
			if (dcServers[i].id() == targetServer.id()) {
				ASSERT_WE_THINK(dcid == 0);
			} else {
				checksumServers.push_back(dcServers[i].id());
			}
		}
		dcid++;
	}
	return SSBulkDumpTask(targetServer, checksumServers, bulkDumpState);
}

std::string generateBulkDumpManifestFileName(Version version) {
	return std::to_string(version) + "-manifest.txt";
}

std::string generateBulkDumpDataFileName(Version version) {
	return std::to_string(version) + "-data.sst";
}

std::string generateBulkDumpByteSampleFileName(Version version) {
	return std::to_string(version) + "-sample.sst";
}

std::string generateBulkDumpJobFolder(const UID& jobId) {
	return jobId.toString();
}

std::string generateBulkDumpTaskFolder(const UID& jobId, const UID& taskId) {
	return joinPath(jobId.toString(), taskId.toString());
}

std::string getJobManifestFileName(const UID& jobId) {
	return jobId.toString() + "-job-manifest.txt";
}

std::pair<BulkDumpFileSet, BulkDumpFileSet> getLocalRemoteFileSetSetting(Version dumpVersion,
                                                                         const std::string& relativeFolder,
                                                                         const std::string& rootLocal,
                                                                         const std::string& rootRemote) {
	// Generate file names based on data version
	const std::string manifestFileName = generateBulkDumpManifestFileName(dumpVersion);
	const std::string dataFileName = generateBulkDumpDataFileName(dumpVersion);
	const std::string byteSampleFileName = generateBulkDumpByteSampleFileName(dumpVersion);

	// Generate local file path to dump
	const std::string dumpFolderLocal = joinPath(rootLocal, relativeFolder);
	const std::string manifestFilePathLocal = joinPath(dumpFolderLocal, manifestFileName);
	const std::string dataFilePathLocal = joinPath(dumpFolderLocal, dataFileName);
	const std::string byteSampleFilePathLocal = joinPath(dumpFolderLocal, byteSampleFileName);

	// Generate remote file path used to fill manifest file content
	const std::string dumpFolderRemote = joinPath(rootRemote, relativeFolder);
	const std::string manifestFilePathRemote = joinPath(dumpFolderRemote, manifestFileName);
	const std::string dataFilePathRemote = joinPath(dumpFolderRemote, dataFileName);
	const std::string byteSampleFilePathRemote = joinPath(dumpFolderRemote, byteSampleFileName);

	BulkDumpFileSet fileSetLocal(
	    rootLocal, dumpFolderLocal, manifestFilePathLocal, dataFilePathLocal, byteSampleFilePathLocal);
	BulkDumpFileSet fileSetRemote(
	    rootRemote, dumpFolderRemote, manifestFilePathRemote, dataFilePathRemote, byteSampleFilePathRemote);
	return std::make_pair(fileSetLocal, fileSetRemote);
}

// Generate SST file given the input sortedKVS to the input filePath
void writeKVSToSSTFile(std::string filePath, const std::map<Key, Value>& sortedKVS, UID logId) {
	const std::string absFilePath = abspath(filePath);
	// Check file
	if (fileExists(absFilePath)) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old File when writeKVSToSSTFile")
		    .detail("DataFilePathLocal", absFilePath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
	// Dump data to file
	std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
	sstWriter->open(absFilePath);
	for (const auto& [key, value] : sortedKVS) {
		sstWriter->write(key, value); // assuming sorted
	}
	if (!sstWriter->finish()) {
		// Unexpected: having data but failed to finish
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "failed to finish data sst writer when writeKVSToSSTFile")
		    .detail("DataFilePath", absFilePath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
	return;
}

void writeStringToFile(const std::string& path, const std::string& content) {
	return writeFile(abspath(path), content);
}

void clearFileFolder(const std::string& folderPath) {
	platform::eraseDirectoryRecursive(abspath(folderPath));
	return;
}

void resetFileFolder(const std::string& folderPath, const UID& logId) {
	clearFileFolder(abspath(folderPath));
	platform::createDirectory(abspath(folderPath));
	return;
}

void bulkDumpFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax, UID logId) {
	const std::string content = readFileBytes(abspath(fromFile), fileBytesMax);
	writeStringToFile(toFile, content);
	TraceEvent(SevInfo, "SSBulkDumpSSTFileCopied", logId)
	    .detail("FromFile", abspath(fromFile))
	    .detail("ToFile", abspath(toFile))
	    .detail("ContentSize", content.size());
	return;
}

// Generate key-value data, byte sampling data, and manifest file given a range at a version with a certain bytes
// Return BulkDumpManifest metadata (equivalent to content of the manifest file)
BulkDumpManifest dumpDataFileToLocalDirectory(UID logId,
                                              const std::map<Key, Value>& sortedData,
                                              const std::map<Key, Value>& sortedSample,
                                              const BulkDumpFileSet& localFileSetConfig,
                                              const BulkDumpFileSet& remoteFileSetConfig,
                                              const ByteSampleSetting& byteSampleSetting,
                                              Version dumpVersion,
                                              const KeyRange& dumpRange,
                                              int64_t dumpBytes) {
	// Step 1: Clean up local folder
	resetFileFolder(localFileSetConfig.folderPath, logId);

	// Step 2: Dump data to file
	bool containDataFile = false;
	if (sortedData.size() > 0) {
		writeKVSToSSTFile(localFileSetConfig.dataPath, sortedData, logId);
		containDataFile = true;
	} else {
		ASSERT(sortedSample.empty());
		containDataFile = false;
	}

	// Step 3: Dump sample to file
	bool containByteSampleFile = false;
	if (sortedSample.size() > 0) {
		writeKVSToSSTFile(localFileSetConfig.byteSamplePath, sortedSample, logId);
		ASSERT(containDataFile);
		containByteSampleFile = true;
	} else {
		containByteSampleFile = false;
	}

	// Step 4: Generate manifest file
	if (fileExists(abspath(localFileSetConfig.manifestPath))) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old manifestFile")
		    .detail("ManifestFilePathLocal", localFileSetConfig.manifestPath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
	BulkDumpFileSet fileSetRemote(remoteFileSetConfig.rootPath,
	                              remoteFileSetConfig.folderPath,
	                              remoteFileSetConfig.manifestPath,
	                              containDataFile ? remoteFileSetConfig.dataPath : "",
	                              containByteSampleFile ? remoteFileSetConfig.byteSamplePath : "");
	BulkDumpManifest manifest(
	    fileSetRemote, dumpRange.begin, dumpRange.end, dumpVersion, "", dumpBytes, byteSampleSetting);
	writeStringToFile(localFileSetConfig.manifestPath, manifest.toString());
	return manifest;
}

// Validate the invariant of filenames. Source is the file stored locally. Destination is the file going to move to.
bool validateSourceDestinationFileSets(const BulkDumpFileSet& source, const BulkDumpFileSet& destination) {
	// Manifest file must be present
	if (source.manifestPath.empty() || destination.manifestPath.empty()) {
		return false;
	}
	// Source data file and destination data file must present at same time
	// If data file not present, byte sampling file must not present
	if (source.dataPath.empty() && (!destination.dataPath.empty() || !source.byteSamplePath.empty())) {
		return false;
	}
	if (destination.dataPath.empty() && (!source.dataPath.empty() || !source.byteSamplePath.empty())) {
		return false;
	}
	// Data file path and byte sampling file path must have the same basename between source and destination
	if (!source.dataPath.empty() && basename(source.dataPath) != basename(destination.dataPath)) {
		return false;
	}
	if (!source.byteSamplePath.empty() && basename(source.byteSamplePath) != basename(destination.byteSamplePath)) {
		return false;
	}
	return true;
}

// Copy files between local file folders, used to mock blobstore in the test.
void bulkDumpTransportCP_impl(BulkDumpFileSet sourceFileSet,
                              BulkDumpFileSet destinationFileSet,
                              size_t fileBytesMax,
                              UID logId) {
	// Clear existing folder
	resetFileFolder(destinationFileSet.folderPath, logId);
	// Copy bulk dump files to the target folder
	bulkDumpFileCopy(sourceFileSet.manifestPath, destinationFileSet.manifestPath, fileBytesMax, logId);
	if (sourceFileSet.dataPath.size() > 0) {
		bulkDumpFileCopy(sourceFileSet.dataPath, destinationFileSet.dataPath, fileBytesMax, logId);
	}
	if (sourceFileSet.byteSamplePath.size() > 0) {
		ASSERT(sourceFileSet.dataPath.size() > 0);
		bulkDumpFileCopy(sourceFileSet.byteSamplePath, destinationFileSet.byteSamplePath, fileBytesMax, logId);
	}
	return;
}

ACTOR Future<Void> uploadBulkDumpFileSet(BulkDumpTransportMethod transportMethod,
                                         BulkDumpFileSet sourceFileSet,
                                         BulkDumpFileSet destinationFileSet,
                                         UID logId) {
	// Upload to blobstore or mock file copy
	if (transportMethod != BulkDumpTransportMethod::CP) {
		TraceEvent(SevWarnAlways, "SSBulkDumpUploadFilesError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	if (!validateSourceDestinationFileSets(sourceFileSet, destinationFileSet)) {
		TraceEvent(SevWarnAlways, "SSBulkDumpUploadFilesError", logId)
		    .detail("SourceFileSet", sourceFileSet.toString())
		    .detail("DestinationFileSet", destinationFileSet.toString());
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	bulkDumpTransportCP_impl(sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
	return Void();
}

void generateBulkDumpJobManifestFile(const std::string& workFolder,
                                     const std::string& localJobManifestFilePath,
                                     const std::string& content,
                                     const UID& logId) {
	resetFileFolder(workFolder, logId);
	writeStringToFile(localJobManifestFilePath, content);
	TraceEvent(SevInfo, "UploadBulkDumpJobManifestWriteLocal", logId)
	    .detail("LocalJobManifestFilePath", localJobManifestFilePath)
	    .detail("Content", content);
	return;
}

void uploadBulkDumpJobManifestFile(BulkDumpTransportMethod transportMethod,
                                   const std::string& localJobManifestFilePath,
                                   const std::string& remoteJobManifestFilePath,
                                   UID logId) {
	if (transportMethod != BulkDumpTransportMethod::CP) {
		TraceEvent(SevWarnAlways, "UploadBulkDumpJobManifestFileError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	TraceEvent(SevWarn, "UploadBulkDumpJobManifestWriteLocal", logId)
	    .detail("RemoteJobManifestFilePath", remoteJobManifestFilePath);
	bulkDumpFileCopy(abspath(localJobManifestFilePath),
	                 abspath(remoteJobManifestFilePath),
	                 SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX,
	                 logId);
	// TODO(BulkDump): check uploaded file exist
	return;
}

ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState) {
	state Transaction tr(cx);
	state Key beginKey = bulkDumpState.getRange().begin;
	state Key endKey = bulkDumpState.getRange().end;
	state KeyRange rangeToPersist;
	state RangeResult result;
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			rangeToPersist = Standalone(KeyRangeRef(beginKey, endKey));
			wait(store(result, krmGetRanges(&tr, bulkDumpPrefix, rangeToPersist)));
			bool anyNew = false;
			for (int i = 0; i < result.size() - 1; i++) {
				if (result[i].value.empty()) {
					throw bulkdump_task_outdated();
				}
				BulkDumpState currentBulkDumpState = decodeBulkDumpState(result[i].value);
				if (currentBulkDumpState.getJobId() != bulkDumpState.getJobId()) {
					throw bulkdump_task_outdated();
				}
				ASSERT(bulkDumpState.getTaskId().present());
				if (currentBulkDumpState.getTaskId().present() &&
				    currentBulkDumpState.getTaskId().get() != bulkDumpState.getTaskId().get()) {
					throw bulkdump_task_outdated();
				}
				if (!anyNew && currentBulkDumpState.getPhase() == BulkDumpPhase::Submitted) {
					anyNew = true;
				}
			}
			if (!anyNew) {
				throw bulkdump_task_outdated();
			}
			wait(krmSetRange(&tr, bulkDumpPrefix, bulkDumpState.getRange(), bulkDumpStateValue(bulkDumpState)));
			wait(tr.commit());
			beginKey = result[result.size() - 1].key;
			if (beginKey >= endKey) {
				break;
			} else {
				tr.reset();
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
	return Void();
}

std::string generateJobManifestFileContent(const std::map<Key, BulkDumpManifest>& manifests) {
	std::string res;
	for (const auto& [beginKey, manifest] : manifests) {
		res = res + manifest.getBeginKeyString() + ", " + manifest.getEndKeyString() + ", " +
		      std::to_string(manifest.version) + ", " + std::to_string(manifest.bytes) + ", " +
		      manifest.fileSet.manifestPath + "\n";
	}
	return res;
}
