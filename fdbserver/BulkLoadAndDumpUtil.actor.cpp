/*
 * BulkLoadAndDumpUtils.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbserver/BulkLoadAndDumpUtil.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include <fmt/format.h>
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // has to be last include

void doFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	std::string content = readFileBytes(fromFile, fileBytesMax);
	writeFile(toFile, content);
	return;
}

ACTOR Future<Optional<BulkLoadTaskState>> getBulkLoadTaskStateFromDataMove(Database cx, UID dataMoveId, UID logId) {
	loop {
		state Transaction tr(cx);
		try {
			Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
			if (!val.present()) {
				TraceEvent(SevWarn, "SSBulkLoadDataMoveIdNotExist", logId).detail("DataMoveID", dataMoveId);
				return Optional<BulkLoadTaskState>();
			}
			DataMoveMetaData dataMoveMetaData = decodeDataMoveValue(val.get());
			return dataMoveMetaData.bulkLoadTaskState;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

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

ACTOR Future<Optional<std::string>> getBytesSamplingFromSSTFiles(std::string folderToGenerate,
                                                                 std::unordered_set<std::string> dataFiles,
                                                                 UID logId) {
	loop {
		try {
			// TODO(BulkLoad): generate filename at first
			std::string bytesSampleFile = abspath(
			    joinPath(folderToGenerate, deterministicRandom()->randomUniqueID().toString() + "-byteSample.sst"));
			std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
			sstWriter->open(bytesSampleFile);
			bool anySampled = false;
			for (const auto& filePath : dataFiles) {
				std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader();
				reader->open(filePath);
				while (reader->hasNext()) {
					KeyValue kv = reader->next();
					ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
					if (sampleInfo.inSample) {
						sstWriter->write(kv.key, kv.value); // TODO(BulkLoad): validate if kvs are sorted
						anySampled = true;
					}
				}
			}
			// It is possible that no key is sampled
			// This can happen when the data to sample is small
			// In this case, no SST sample byte file is generated
			if (anySampled) {
				ASSERT(sstWriter->finish());
				return bytesSampleFile;
			} else {
				return Optional<std::string>();
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadTaskSamplingError", logId).errorUnsuppressed(e);
			wait(delay(5.0));
		}
	}
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

// Generate key-value data, byte sampling data, and manifest file given a range at a version with a certain bytes
// Return BulkLoadManifest metadata (equivalent to content of the manifest file)
// TODO(BulkDump): can cause slow tasks, do the task in a separate thread in the future.
BulkLoadManifest dumpDataFileToLocal(UID logId,
                                     const std::map<Key, Value>& sortedData,
                                     const std::map<Key, Value>& sortedSample,
                                     const BulkLoadFileSet& localFileSetConfig,
                                     const BulkLoadFileSet& remoteFileSetConfig,
                                     const BulkLoadByteSampleSetting& byteSampleSetting,
                                     Version dumpVersion,
                                     const KeyRange& dumpRange,
                                     int64_t dumpBytes) {
	BulkDumpFileFullPathSet localFiles(localFileSetConfig);

	// Step 1: Clean up local folder
	platform::eraseDirectoryRecursive(abspath(localFiles.folder));
	platform::createDirectory(abspath(localFiles.folder));

	// Step 2: Dump data to file
	bool containDataFile = false;
	if (sortedData.size() > 0) {
		writeKVSToSSTFile(abspath(localFiles.dataFilePath), sortedData, logId);
		containDataFile = true;
	} else {
		ASSERT(sortedSample.empty());
		containDataFile = false;
	}

	// Step 3: Dump sample to file
	bool containByteSampleFile = false;
	if (sortedSample.size() > 0) {
		writeKVSToSSTFile(abspath(localFiles.byteSampleFilePath), sortedSample, logId);
		ASSERT(containDataFile);
		containByteSampleFile = true;
	} else {
		containByteSampleFile = false;
	}

	// Step 4: Generate manifest file
	if (fileExists(abspath(localFiles.manifestFilePath))) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old manifestFile")
		    .detail("ManifestFilePathLocal", abspath(localFiles.manifestFilePath));
		ASSERT_WE_THINK(false);
		throw retry();
	}
	BulkLoadFileSet fileSetRemote(remoteFileSetConfig.rootPath,
	                              remoteFileSetConfig.relativePath,
	                              remoteFileSetConfig.manifestFileName,
	                              containDataFile ? remoteFileSetConfig.dataFileName : "",
	                              containByteSampleFile ? remoteFileSetConfig.byteSampleFileName : "");
	BulkLoadManifest manifest(
	    fileSetRemote, dumpRange.begin, dumpRange.end, dumpVersion, "", dumpBytes, byteSampleSetting);
	writeFile(abspath(localFiles.manifestFilePath), manifest.toString());
	return manifest;
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

ACTOR Future<Void> uploadSingleFile(BulkLoadTransportMethod transportMethod,
                                    std::string fromLocalPath,
                                    std::string toRemotePath,
                                    UID logId) {
	state int retryCount = 0;
	loop {
		try {
			if (transportMethod == BulkLoadTransportMethod::CP) {
				TraceEvent(SevInfo, "UploadSingleFile", logId)
				    .detail("FromLocalPath", fromLocalPath)
				    .detail("ToRemotePath", toRemotePath);
				doFileCopy(abspath(fromLocalPath), abspath(toRemotePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
				wait(delay(0.1));
			} else {
				TraceEvent(SevError, "UploadSingleFileError", logId)
				    .detail("Reason", "Transport method is not implemented")
				    .detail("TransportMethod", transportMethod)
				    .detail("FromLocalPath", fromLocalPath)
				    .detail("ToRemotePath", toRemotePath);
				UNREACHABLE();
			}
			// TODO(BulkDump): check uploaded file exist
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			retryCount++;
			if (retryCount > 10) {
				TraceEvent(SevWarnAlways, "UploadSingleFileError", logId)
				    .errorUnsuppressed(e)
				    .detail("TransportMethod", transportMethod)
				    .detail("FromLocalPath", fromLocalPath)
				    .detail("ToRemotePath", toRemotePath);
				throw e;
			}
			wait(delay(5.0));
		}
	}
	return Void();
}

ACTOR Future<Void> downloadSingleFile(BulkLoadTransportMethod transportMethod,
                                      std::string fromRemotePath,
                                      std::string toLocalPath,
                                      UID logId) {
	state int retryCount = 0;
	loop {
		try {
			if (transportMethod == BulkLoadTransportMethod::CP) {
				TraceEvent(SevInfo, "DownloadSingleFile", logId)
				    .detail("FromRemotePath", fromRemotePath)
				    .detail("ToLocalPath", toLocalPath);
				doFileCopy(abspath(fromRemotePath), abspath(toLocalPath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
				wait(delay(0.1));
			} else {
				TraceEvent(SevError, "DownloadSingleFileError", logId)
				    .detail("Reason", "Transport method is not implemented")
				    .detail("TransportMethod", transportMethod)
				    .detail("FromRemotePath", fromRemotePath)
				    .detail("ToLocalPath", toLocalPath);
				UNREACHABLE();
			}
			if (!fileExists(abspath(toLocalPath))) {
				throw retry();
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			retryCount++;
			if (retryCount > 10) {
				TraceEvent(SevWarnAlways, "DownloadSingleFileError", logId)
				    .errorUnsuppressed(e)
				    .detail("TransportMethod", transportMethod)
				    .detail("FromRemotePath", fromRemotePath)
				    .detail("ToLocalPath", toLocalPath);
				throw e;
			}
			wait(delay(5.0));
		}
	}
	return Void();
}

SSBulkLoadFileSet bulkLoadTransportCP_impl(std::string dir,
                                           BulkLoadTaskState bulkLoadTaskState,
                                           size_t fileBytesMax,
                                           UID logId) {
	ASSERT(bulkLoadTaskState.getTransportMethod() == BulkLoadTransportMethod::CP);
	std::string toFile;
	std::string fromFile;

	SSBulkLoadFileSet fileSet;
	fileSet.folder = abspath(joinPath(dir, bulkLoadTaskState.getFolder()));
	// Clear existing folder
	platform::eraseDirectoryRecursive(fileSet.folder);
	ASSERT(platform::createDirectory(fileSet.folder));

	// Move bulk load files to loading folder
	for (const auto& filePath : bulkLoadTaskState.getDataFiles()) {
		fromFile = abspath(filePath);
		toFile = abspath(joinPath(fileSet.folder, basename(fromFile)));
		if (fileSet.dataFileList.find(toFile) != fileSet.dataFileList.end()) {
			ASSERT_WE_THINK(false);
			throw retry();
		}
		doFileCopy(fromFile, toFile, fileBytesMax);
		fileSet.dataFileList.insert(toFile);
		TraceEvent(SevInfo, "SSBulkLoadSSTFileCopied", logId)
		    .detail("BulkLoadTask", bulkLoadTaskState.toString())
		    .detail("FromFile", fromFile)
		    .detail("ToFile", toFile);
	}
	if (bulkLoadTaskState.getBytesSampleFile().present()) {
		fromFile = abspath(bulkLoadTaskState.getBytesSampleFile().get());
		if (fileExists(fromFile)) {
			toFile = abspath(joinPath(fileSet.folder, basename(fromFile)));
			doFileCopy(fromFile, toFile, fileBytesMax);
			fileSet.bytesSampleFile = toFile;
			TraceEvent(SevInfo, "SSBulkLoadSSTFileCopied", logId)
			    .detail("BulkLoadTask", bulkLoadTaskState.toString())
			    .detail("FromFile", fromFile)
			    .detail("ToFile", toFile);
		}
	}
	return fileSet;
}

// Copy files between local file folders, used to mock blobstore in the test.
void doFileSetCopy(BulkLoadFileSet fromFileSet, BulkLoadFileSet toFileSet, size_t fileBytesMax, UID logId) {
	BulkDumpFileFullPathSet localFiles(fromFileSet);
	BulkDumpFileFullPathSet remoteFiles(toFileSet);

	// Clear remote existing folder
	platform::eraseDirectoryRecursive(abspath(remoteFiles.folder));
	platform::createDirectory(abspath(remoteFiles.folder));
	// Copy bulk dump files to the remote folder
	doFileCopy(abspath(localFiles.manifestFilePath), abspath(remoteFiles.manifestFilePath), fileBytesMax);
	if (fromFileSet.dataFileName.size() > 0) {
		doFileCopy(abspath(localFiles.dataFilePath), abspath(remoteFiles.dataFilePath), fileBytesMax);
	}
	if (fromFileSet.byteSampleFileName.size() > 0) {
		ASSERT(fromFileSet.dataFileName.size() > 0);
		doFileCopy(abspath(localFiles.byteSampleFilePath), abspath(remoteFiles.byteSampleFilePath), fileBytesMax);
	}
	return;
}

// Validate the invariant of filenames. Source is the file stored locally. Destination is the file going to move to.
bool validateSourceDestinationFileSets(const BulkLoadFileSet& source, const BulkLoadFileSet& destination) {
	// Manifest file must be present
	if (source.manifestFileName.empty() || destination.manifestFileName.empty()) {
		return false;
	}
	// Source data file and destination data file must present at same time
	// If data file not present, byte sampling file must not present
	if (source.dataFileName.empty() && (!destination.dataFileName.empty() || !source.byteSampleFileName.empty())) {
		return false;
	}
	if (destination.dataFileName.empty() && (!source.dataFileName.empty() || !source.byteSampleFileName.empty())) {
		return false;
	}
	// Data file path and byte sampling file path must have the same basename between source and destination
	if (!source.dataFileName.empty() && source.dataFileName != destination.dataFileName) {
		return false;
	}
	if (!source.byteSampleFileName.empty() && source.byteSampleFileName != destination.byteSampleFileName) {
		return false;
	}
	return true;
}

ACTOR Future<SSBulkLoadFileSet> downloadBulkLoadFileSet(BulkLoadTransportMethod transportMethod,
                                                        std::string dir,
                                                        BulkLoadTaskState bulkLoadTaskState,
                                                        size_t fileBytesMax,
                                                        UID logId) {
	if (transportMethod != BulkLoadTransportMethod::CP) {
		TraceEvent(SevWarnAlways, "SSDownloadBulkLoadFileSetError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	wait(delay(0.1));
	SSBulkLoadFileSet res = bulkLoadTransportCP_impl(dir, bulkLoadTaskState, fileBytesMax, logId);
	return res;
}

ACTOR Future<Void> uploadBulkLoadFileSet(BulkLoadTransportMethod transportMethod,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet,
                                         UID logId) {
	if (transportMethod != BulkLoadTransportMethod::CP) {
		TraceEvent(SevWarnAlways, "SSBulkDumpuploadSingleFilesError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	if (!validateSourceDestinationFileSets(sourceFileSet, destinationFileSet)) {
		TraceEvent(SevWarnAlways, "SSBulkDumpuploadSingleFilesError", logId)
		    .detail("SourceFileSet", sourceFileSet.toString())
		    .detail("DestinationFileSet", destinationFileSet.toString());
		ASSERT_WE_THINK(false);
		throw bulkdump_task_failed();
	}
	wait(delay(0.1));
	doFileSetCopy(sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
	return Void();
}

// Get manifest within the input range
ACTOR Future<std::vector<BulkLoadManifest>> getBulkLoadManifestMetadataFromFiles(
    std::string localJobManifestFilePath,
    KeyRange range,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    UID logId) {
	ASSERT(fileExists(abspath(localJobManifestFilePath)));
	state std::vector<BulkLoadManifest> res;
	const std::string jobManifestRawString =
	    readFileBytes(abspath(localJobManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
	state std::vector<std::string> lines = splitString(jobManifestRawString, "\n");
	state BulkDumpJobManifestHeader header(lines[0]);
	state size_t lineIdx = 1; // skip the first line which is the header
	while (lineIdx < lines.size()) {
		if (lines[lineIdx].empty()) {
			ASSERT(lineIdx == lines.size() - 1);
			break;
		}
		BulkDumpJobManifestEntry manifestEntry(lines[lineIdx]);
		KeyRange overlappingRange = range & manifestEntry.getRange();
		if (overlappingRange.empty()) {
			// Ignore the manifest entry if no overlapping range
			lineIdx = lineIdx + 1;
			continue;
		}
		state std::string remoteManifestFilePath =
		    joinPath(header.getRootFolder(), manifestEntry.getRelativePath()); // wrong
		platform::eraseDirectoryRecursive(abspath(manifestLocalTempFolder));
		platform::createDirectory(abspath(manifestLocalTempFolder));
		state std::string localManifestFilePath = joinPath(manifestLocalTempFolder, basename(remoteManifestFilePath));
		// Download the manifest file
		wait(downloadSingleFile(transportMethod, remoteManifestFilePath, localManifestFilePath, logId));
		const std::string manifestRawString =
		    readFileBytes(abspath(localManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
		ASSERT(!manifestRawString.empty());
		BulkLoadManifest manifest(manifestRawString);
		res.push_back(manifest);
		wait(delay(1.0));
		lineIdx = lineIdx + 1;
	}
	platform::eraseDirectoryRecursive(abspath(manifestLocalTempFolder));
	return res;
}

// Download job manifest file
// Each job has one manifest file including manifest paths of all tasks
ACTOR Future<Void> downloadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                   std::string localJobManifestFilePath,
                                                   std::string remoteJobManifestFilePath,
                                                   UID logId) {
	wait(downloadSingleFile(transportMethod, remoteJobManifestFilePath, localJobManifestFilePath, logId));
	return Void();
}

// Upload job manifest file
// Each job has one manifest file including manifest paths of all tasks
ACTOR Future<Void> uploadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                 std::string localJobManifestFilePath,
                                                 std::string remoteJobManifestFilePath,
                                                 std::map<Key, BulkLoadManifest> manifests,
                                                 UID logId) {
	// Step 1: Generate manifest file content
	std::string content = generateBulkLoadJobManifestFileContent(manifests);
	ASSERT(!content.empty());

	// Step 2: Generate the manifest file locally
	writeFile(abspath(localJobManifestFilePath), content);
	TraceEvent(SevInfo, "UploadBulkLoadJobManifestFile", logId)
	    .detail("LocalJobManifestFilePath", localJobManifestFilePath)
	    .detail("Content", content);

	// Step 3: Upload the manifest file
	wait(uploadSingleFile(transportMethod, localJobManifestFilePath, remoteJobManifestFilePath, logId));
	return Void();
}
