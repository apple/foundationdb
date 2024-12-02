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
#include <string>

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
	std::string manifestFileName = generateBulkDumpManifestFileName(dumpVersion);
	std::string dataFileName = generateBulkDumpDataFileName(dumpVersion);
	std::string byteSampleFileName = generateBulkDumpByteSampleFileName(dumpVersion);

	// Generate local file path to dump
	const std::string dumpFolderLocal = abspath(joinPath(rootLocal, relativeFolder));
	std::string manifestFilePathLocal = abspath(joinPath(dumpFolderLocal, manifestFileName));
	std::string dataFilePathLocal = abspath(joinPath(dumpFolderLocal, dataFileName));
	std::string byteSampleFilePathLocal = abspath(joinPath(dumpFolderLocal, byteSampleFileName));

	// Generate remote file path used to fill manifest file content
	const std::string dumpFolderRemote = joinPath(rootRemote, relativeFolder);
	std::string manifestFilePathRemote = joinPath(dumpFolderRemote, manifestFileName);
	std::string dataFilePathRemote = joinPath(dumpFolderRemote, dataFileName);
	std::string byteSampleFilePathRemote = joinPath(dumpFolderRemote, byteSampleFileName);

	BulkDumpFileSet fileSetLocal(
	    rootLocal, dumpFolderLocal, manifestFilePathLocal, dataFilePathLocal, byteSampleFilePathLocal);
	BulkDumpFileSet fileSetRemote(
	    rootRemote, dumpFolderRemote, manifestFilePathRemote, dataFilePathRemote, byteSampleFilePathRemote);
	return std::make_pair(fileSetLocal, fileSetRemote);
}

// Generate SST file given the input sortedKVS to the input filePath
void writeKVSToSSTFile(std::string filePath, const std::map<Key, Value>& sortedKVS, UID logId) {
	// Check file
	if (fileExists(filePath)) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old File when writeKVSToSSTFile")
		    .detail("DataFilePathLocal", filePath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
	// Dump data to file
	std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
	sstWriter->open(filePath);
	for (const auto& [key, value] : sortedKVS) {
		sstWriter->write(key, value); // assuming sorted
	}
	if (!sstWriter->finish()) {
		// Unexpected: having data but failed to finish
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "failed to finish data sst writer when writeKVSToSSTFile")
		    .detail("DataFilePath", filePath);
		ASSERT_WE_THINK(false);
		throw retry();
	}
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
	platform::eraseDirectoryRecursive(localFileSetConfig.folderPath);
	if (!platform::createDirectory(localFileSetConfig.folderPath)) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "failed to re-create director")
		    .detail("DumpFolderLocal", localFileSetConfig.folderPath);
		throw retry();
	}

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
	if (fileExists(localFileSetConfig.manifestPath)) {
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
	writeFile(localFileSetConfig.manifestPath, manifest.toString());
	return manifest;
}

void bulkDumpFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax, UID logId) {
	std::string content = readFileBytes(fromFile, fileBytesMax);
	writeFile(toFile, content);
	TraceEvent(SevInfo, "SSBulkDumpSSTFileCopied", logId)
	    .detail("FromFile", fromFile)
	    .detail("ToFile", toFile)
	    .detail("ContentSize", content.size());
	return;
}

bool validateSourceDestinationFileSets(const BulkDumpFileSet& source, const BulkDumpFileSet& destination) {
	if (source.manifestPath.empty() || destination.manifestPath.empty()) {
		return false;
	}
	if (source.dataPath.empty() && (!destination.dataPath.empty() || !source.byteSamplePath.empty())) {
		return false;
	}
	if (destination.dataPath.empty() && (!source.dataPath.empty() || !source.byteSamplePath.empty())) {
		return false;
	}
	if (!source.dataPath.empty() && basename(source.dataPath) != basename(destination.dataPath)) {
		return false;
	}
	if (!source.byteSamplePath.empty() && basename(source.byteSamplePath) != basename(destination.byteSamplePath)) {
		return false;
	}
	return true;
}

// Copy files between local file folders, used to mock blobstore in the test
ACTOR Future<Void> bulkDumpTransportCP_impl(BulkDumpFileSet sourceFileSet,
                                            BulkDumpFileSet destinationFileSet,
                                            size_t fileBytesMax,
                                            UID logId) {
	loop {
		try {
			// Clear existing folder
			platform::eraseDirectoryRecursive(abspath(destinationFileSet.folderPath));
			if (!platform::createDirectory(abspath(destinationFileSet.folderPath))) {
				throw retry();
			}
			// Move bulk dump files to the target folder
			bulkDumpFileCopy(
			    abspath(sourceFileSet.manifestPath), abspath(destinationFileSet.manifestPath), fileBytesMax, logId);
			if (sourceFileSet.dataPath.size() > 0) {
				bulkDumpFileCopy(
				    abspath(sourceFileSet.dataPath), abspath(destinationFileSet.dataPath), fileBytesMax, logId);
				if (sourceFileSet.byteSamplePath.size() > 0) {
					bulkDumpFileCopy(abspath(sourceFileSet.byteSamplePath),
					                 abspath(destinationFileSet.byteSamplePath),
					                 fileBytesMax,
					                 logId);
				}
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevInfo, "SSBulkDumpSSTFileCopyError", logId)
			    .errorUnsuppressed(e)
			    .detail("SourceFileSet", sourceFileSet.toString())
			    .detail("DestinationFileSet", destinationFileSet.toString());
			if (!g_network->isSimulated()) {
				wait(delay(5.0));
			}
		}
	}
	return Void();
}

ACTOR Future<Void> uploadBulkDumpFileSet(BulkDumpTransportMethod transportMethod,
                                         BulkDumpFileSet sourceFileSet,
                                         BulkDumpFileSet destinationFileSet,
                                         UID logId) {
	// Upload to S3 or mock file copy
	if (transportMethod != BulkDumpTransportMethod::CP) {
		TraceEvent(SevWarnAlways, "SSBulkDumpUploadFilesError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	if (!validateSourceDestinationFileSets(sourceFileSet, destinationFileSet)) {
		TraceEvent(SevInfo, "SSBulkDumpUploadFilesError", logId)
		    .detail("SourceFileSet", sourceFileSet.toString())
		    .detail("DestinationFileSet", destinationFileSet.toString());
		throw bulkdump_task_failed();
	}
	wait(bulkDumpTransportCP_impl(sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
	return Void();
}

void uploadBulkDumpJobManifestFile(BulkDumpTransportMethod transportMethod,
                                   const std::string& content,
                                   const std::string& localRoot,
                                   const std::string& jobFolder,
                                   const std::string& localJobManifestFilePath,
                                   const std::string& remoteJobManifestFilePath,
                                   UID logId) {
	platform::eraseDirectoryRecursive(abspath(localRoot));
	if (!platform::createDirectory(abspath(joinPath(localRoot, jobFolder)))) {
		TraceEvent(SevWarn, "UploadBulkDumpJobManifestFileError", logId)
		    .detail("Reason", "failed to re-create director")
		    .detail("LocalRoot", localRoot);
		throw retry();
	}
	TraceEvent(SevWarn, "UploadBulkDumpJobManifestWriteLocal", logId)
	    .detail("LocalRoot", localRoot)
	    .detail("LocalJobManifestFilePath", localJobManifestFilePath)
	    .detail("Content", content);
	writeFile(localJobManifestFilePath, content);
	// Upload to S3 or mock file copy
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
	platform::eraseDirectoryRecursive(abspath(localRoot)); // clean up
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
