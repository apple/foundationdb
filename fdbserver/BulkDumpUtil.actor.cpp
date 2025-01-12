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
#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Optional.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "fdbclient/S3Client.actor.h" // include the header for S3Client
#include "flow/flow.h"
#include <stdexcept>
#include <string>
#include "flow/actorcompiler.h" // has to be last include

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

std::string getBulkDumpJobTaskFolder(const UID& jobId, const UID& taskId) {
	return joinPath(jobId.toString(), taskId.toString());
}

std::pair<BulkLoadFileSet, BulkLoadFileSet> getLocalRemoteFileSetSetting(Version dumpVersion,
                                                                         const std::string& relativeFolder,
                                                                         const std::string& rootLocal,
                                                                         const std::string& rootRemote) {
	// Generate file names based on data version
	const std::string manifestFileName = generateBulkDumpManifestFileName(dumpVersion);
	const std::string dataFileName = generateBulkDumpDataFileName(dumpVersion);
	const std::string byteSampleFileName = generateBulkDumpByteSampleFileName(dumpVersion);
	BulkLoadFileSet fileSetLocal(
	    rootLocal, relativeFolder, manifestFileName, dataFileName, byteSampleFileName, BulkLoadChecksum());
	BulkLoadFileSet fileSetRemote(
	    rootRemote, relativeFolder, manifestFileName, dataFileName, byteSampleFileName, BulkLoadChecksum());
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
// Return BulkLoadManifest metadata (equivalent to content of the manifest file)
// TODO(BulkDump): can cause slow tasks, do the task in a separate thread in the future.
BulkLoadManifest dumpDataFileToLocalDirectory(UID logId,
                                              const std::map<Key, Value>& sortedData,
                                              const std::map<Key, Value>& sortedSample,
                                              const BulkLoadFileSet& localFileSetConfig,
                                              const BulkLoadFileSet& remoteFileSetConfig,
                                              const BulkLoadByteSampleSetting& byteSampleSetting,
                                              Version dumpVersion,
                                              const KeyRange& dumpRange,
                                              int64_t dumpBytes,
                                              int64_t dumpKeyCount,
                                              BulkLoadType dumpType,
                                              BulkLoadTransportMethod transportMethod) {
	// Step 1: Clean up local folder
	resetFileFolder((abspath(localFileSetConfig.getFolder())), logId);

	// Step 2: Dump data to file
	bool containDataFile = false;
	if (sortedData.size() > 0) {
		writeKVSToSSTFile(abspath(localFileSetConfig.getDataFileFullPath()), sortedData, logId);
		containDataFile = true;
	} else {
		ASSERT(sortedSample.empty());
		containDataFile = false;
	}

	// Step 3: Dump sample to file
	bool containByteSampleFile = false;
	if (sortedSample.size() > 0) {
		writeKVSToSSTFile(abspath(localFileSetConfig.getBytesSampleFileFullPath()), sortedSample, logId);
		ASSERT(containDataFile);
		containByteSampleFile = true;
	} else {
		containByteSampleFile = false;
	}

	// Step 4: Generate manifest file
	if (fileExists(abspath(localFileSetConfig.getManifestFileFullPath()))) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old manifestFile")
		    .detail("ManifestFilePathLocal", abspath(localFileSetConfig.getManifestFileFullPath()));
		ASSERT_WE_THINK(false);
		throw retry();
	}
	BulkLoadFileSet fileSetRemote(remoteFileSetConfig.getRootPath(),
	                              remoteFileSetConfig.getRelativePath(),
	                              remoteFileSetConfig.getManifestFileName(),
	                              containDataFile ? remoteFileSetConfig.getDataFileName() : "",
	                              containByteSampleFile ? remoteFileSetConfig.getByteSampleFileName() : "",
	                              BulkLoadChecksum());
	BulkLoadManifest manifest(fileSetRemote,
	                          dumpRange.begin,
	                          dumpRange.end,
	                          dumpVersion,
	                          dumpBytes,
	                          dumpKeyCount,
	                          byteSampleSetting,
	                          dumpType,
	                          transportMethod);
	writeStringToFile(abspath(localFileSetConfig.getManifestFileFullPath()), manifest.toString());
	return manifest;
}

// Validate the invariant of filenames. Source is the file stored locally. Destination is the file going to move to.
bool validateSourceDestinationFileSets(const BulkLoadFileSet& source, const BulkLoadFileSet& destination) {
	// Manifest file must be present
	if (!source.hasManifestFile() || !destination.hasManifestFile()) {
		return false;
	}
	// Source data file and destination data file must present at same time
	// If data file not present, byte sampling file must not present
	if (!source.hasDataFile() && (destination.hasDataFile() || source.hasByteSampleFile())) {
		return false;
	}
	if (!destination.hasDataFile() && (source.hasDataFile() || destination.hasByteSampleFile())) {
		return false;
	}
	// Data file path and byte sampling file path must have the same basename between source and destination
	if (source.hasDataFile() && source.getDataFileName() != destination.getDataFileName()) {
		return false;
	}
	if (source.hasByteSampleFile() && source.getByteSampleFileName() != destination.getByteSampleFileName()) {
		return false;
	}
	return true;
}

// Copy files between local file folders, used to mock blobstore in the test.
void bulkDumpTransportCP_impl(const BulkLoadFileSet& sourceFileSet,
                              const BulkLoadFileSet& destinationFileSet,
                              size_t fileBytesMax,
                              UID logId) {
	// Clear remote existing folder
	resetFileFolder(abspath(destinationFileSet.getFolder()), logId);
	// Copy bulk dump files to the remote folder
	ASSERT(sourceFileSet.hasManifestFile() && destinationFileSet.hasManifestFile());
	bulkDumpFileCopy(abspath(sourceFileSet.getManifestFileFullPath()),
	                 abspath(destinationFileSet.getManifestFileFullPath()),
	                 fileBytesMax,
	                 logId);
	if (sourceFileSet.hasDataFile()) {
		ASSERT(destinationFileSet.hasDataFile());
		bulkDumpFileCopy(abspath(sourceFileSet.getDataFileFullPath()),
		                 abspath(destinationFileSet.getDataFileFullPath()),
		                 fileBytesMax,
		                 logId);
	}
	if (sourceFileSet.hasByteSampleFile()) {
		ASSERT(sourceFileSet.hasDataFile() && destinationFileSet.hasByteSampleFile());
		bulkDumpFileCopy(abspath(sourceFileSet.getBytesSampleFileFullPath()),
		                 abspath(destinationFileSet.getBytesSampleFileFullPath()),
		                 fileBytesMax,
		                 logId);
	}
	return;
}

// Dump files to blobstore.
ACTOR Future<Void> bulkDumpTransportBlobstore_impl(BulkLoadFileSet sourceFileSet,
                                                   BulkLoadFileSet destinationFileSet,
                                                   size_t fileBytesMax,
                                                   UID logId) {
	// TODO(BulkDump): Make use of fileBytesMax
	wait(copyUpBulkDumpFileSet(destinationFileSet.getRootPath(), sourceFileSet, destinationFileSet));
	return Void();
}

ACTOR Future<Void> uploadBulkDumpFileSet(BulkLoadTransportMethod transportMethod,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet,
                                         UID logId) {
	// Validate file names of source and destination
	if (!validateSourceDestinationFileSets(sourceFileSet, destinationFileSet)) {
		TraceEvent(SevWarnAlways, "SSBulkDumpUploadFilesError", logId)
		    .detail("SourceFileSet", sourceFileSet.toString())
		    .detail("DestinationFileSet", destinationFileSet.toString());
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	// Upload to blobstore or mock file copy
	if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
		wait(bulkDumpTransportBlobstore_impl(
		    sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
	} else if (transportMethod == BulkLoadTransportMethod::CP) {
		bulkDumpTransportCP_impl(sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
	} else {
		TraceEvent(SevError, "SSBulkDumpUploadFilesError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT(false);
	}
	return Void();
}

void generateBulkDumpJobManifestFile(const std::string& workFolder,
                                     const std::string& localJobManifestFilePath,
                                     const std::string& content,
                                     const UID& logId) {
	resetFileFolder(workFolder, logId);
	writeStringToFile(localJobManifestFilePath, content);
	TraceEvent(SevInfo, "GenerateBulkDumpJobManifestWriteLocal", logId)
	    .detail("LocalJobManifestFilePath", localJobManifestFilePath)
	    .detail("Content", content);
	return;
}

ACTOR Future<Void> uploadBulkDumpJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                 std::string localJobManifestFilePath,
                                                 std::string remoteFolder,
                                                 std::string remoteJobManifestFileName,
                                                 UID logId) {
	auto remoteJobManifestFilePath = appendToPath(remoteFolder, remoteJobManifestFileName);
	TraceEvent(SevInfo, "UploadBulkDumpJobManifest", logId)
	    .detail("RemoteJobManifestFilePath", remoteJobManifestFilePath);
	if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
		wait(copyUpFile(localJobManifestFilePath, remoteJobManifestFilePath));
	} else if (transportMethod == BulkLoadTransportMethod::CP) {
		bulkDumpFileCopy(abspath(localJobManifestFilePath),
		                 abspath(remoteJobManifestFilePath),
		                 SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX,
		                 logId);
	} else {
		TraceEvent(SevError, "UploadBulkDumpJobManifestFileError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT(false);
	}
	// TODO(BulkDump): check uploaded file exist
	return Void();
}

ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState) {
	state Transaction tr(cx);
	ASSERT(bulkDumpState.isValid());
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
				if (result[i].value.empty()) { // has been cancelled
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
