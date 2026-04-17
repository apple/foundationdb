/*
 * BulkDumpUtils.cpp
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

#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/S3Client.h"
#include "fdbserver/core/BulkDumpUtil.h"
#include "fdbserver/core/BulkLoadUtil.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/RocksDBCheckpointUtils.h"
#include "fdbserver/core/StorageMetrics.h"
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

// Generate SST file given the input sortedKVS to the input filePath.
// TODO(BulkDump): This copy of sortedKVS can be a slow task if data is large.
void writeKVSToSSTFile(std::string filePath, std::map<Key, Value>& sortedKVS, UID logId) {
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

Future<BulkLoadManifest> dumpDataFileToLocalDirectory(UID logId,
                                                      std::shared_ptr<RangeDumpRawData> rangeDumpRawData,
                                                      BulkLoadFileSet localFileSet,
                                                      BulkLoadFileSet remoteFileSet,
                                                      BulkLoadByteSampleSetting byteSampleSetting,
                                                      Version dumpVersion,
                                                      KeyRange dumpRange,
                                                      BulkLoadType dumpType,
                                                      BulkLoadTransportMethod transportMethod) {
	// Step 1: Clean up local folder
	resetFileFolder((abspath(localFileSet.getFolder())));

	// Step 2: Dump data to file
	bool containDataFile = false;
	if (rangeDumpRawData->kvs.size() > 0) {
		writeKVSToSSTFile(abspath(localFileSet.getDataFileFullPath()), rangeDumpRawData->kvs, logId);
		containDataFile = true;
	} else {
		ASSERT(rangeDumpRawData->sampled.empty());
		containDataFile = false;
	}

	// Step 3: Dump sample to file
	bool containByteSampleFile = false;
	if (rangeDumpRawData->sampled.size() > 0) {
		writeKVSToSSTFile(abspath(localFileSet.getBytesSampleFileFullPath()), rangeDumpRawData->sampled, logId);
		containByteSampleFile = true;
	} else {
		containByteSampleFile = false;
	}

	// Step 4: Generate manifest file
	if (fileExists(abspath(localFileSet.getManifestFileFullPath()))) {
		TraceEvent(SevWarn, "SSBulkDumpRetriableError", logId)
		    .detail("Reason", "exist old manifestFile")
		    .detail("ManifestFilePathLocal", abspath(localFileSet.getManifestFileFullPath()));
		ASSERT_WE_THINK(false);
		throw retry();
	}
	BulkLoadFileSet fileSetRemote(remoteFileSet.getRootPath(),
	                              remoteFileSet.getRelativePath(),
	                              remoteFileSet.getManifestFileName(),
	                              containDataFile ? remoteFileSet.getDataFileName() : std::string(),
	                              containByteSampleFile ? remoteFileSet.getByteSampleFileName() : std::string(),
	                              BulkLoadChecksum());
	BulkLoadManifest manifestMetadata(fileSetRemote,
	                                  dumpRange.begin,
	                                  dumpRange.end,
	                                  dumpVersion,
	                                  rangeDumpRawData->kvsBytes,
	                                  rangeDumpRawData->kvs.size(),
	                                  byteSampleSetting,
	                                  dumpType,
	                                  transportMethod);
	std::string manifestStr = manifestMetadata.toString();
	std::shared_ptr<std::string> manifest = std::make_shared<std::string>(std::move(manifestStr));
	co_await writeBulkFileBytes(abspath(localFileSet.getManifestFileFullPath()), manifest);
	co_return manifestMetadata;
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
Future<Void> bulkDumpTransportCP_impl(BulkLoadFileSet srcFileSet,
                                      BulkLoadFileSet destFileSet,
                                      size_t fileBytesMax,
                                      UID logId) {
	// Clear remote existing folder
	resetFileFolder(abspath(destFileSet.getFolder()));
	// Copy bulk dump files to the remote folder
	ASSERT(srcFileSet.hasManifestFile() && destFileSet.hasManifestFile());
	co_await copyBulkFile(
	    abspath(srcFileSet.getManifestFileFullPath()), abspath(destFileSet.getManifestFileFullPath()), fileBytesMax);
	if (srcFileSet.hasDataFile()) {
		ASSERT(destFileSet.hasDataFile());
		co_await copyBulkFile(
		    abspath(srcFileSet.getDataFileFullPath()), abspath(destFileSet.getDataFileFullPath()), fileBytesMax);
	}
	if (srcFileSet.hasByteSampleFile()) {
		ASSERT(srcFileSet.hasDataFile() && destFileSet.hasByteSampleFile());
		co_await copyBulkFile(abspath(srcFileSet.getBytesSampleFileFullPath()),
		                      abspath(destFileSet.getBytesSampleFileFullPath()),
		                      fileBytesMax);
	}
}

// Dump files to blobstore.
Future<Void> bulkDumpTransportBlobstore_impl(BulkLoadFileSet sourceFileSet,
                                             BulkLoadFileSet destinationFileSet,
                                             size_t fileBytesMax,
                                             UID logId) {
	// TODO(BulkDump): Make use of fileBytesMax
	co_await copyUpBulkDumpFileSet(destinationFileSet.getRootPath(), sourceFileSet, destinationFileSet);
}

Future<Void> uploadBulkDumpFileSet(BulkLoadTransportMethod transportMethod,
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
		co_await bulkDumpTransportBlobstore_impl(
		    sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
	} else if (transportMethod == BulkLoadTransportMethod::CP) {
		co_await bulkDumpTransportCP_impl(
		    sourceFileSet, destinationFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
	} else {
		TraceEvent(SevError, "SSBulkDumpUploadFilesError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT(false);
	}
}

Future<Void> uploadBulkDumpJobManifestFile(BulkLoadTransportMethod transportMethod,
                                           std::string localJobManifestFilePath,
                                           std::string remoteFolder,
                                           std::string remoteJobManifestFileName,
                                           UID logId) {
	auto remoteJobManifestFilePath = appendToPath(remoteFolder, remoteJobManifestFileName);
	TraceEvent(SevInfo, "UploadBulkDumpJobManifest", logId)
	    .detail("RemoteJobManifestFilePath", remoteJobManifestFilePath);
	if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
		co_await copyUpFile(localJobManifestFilePath, remoteJobManifestFilePath);
	} else if (transportMethod == BulkLoadTransportMethod::CP) {
		co_await copyBulkFile(abspath(localJobManifestFilePath),
		                      abspath(remoteJobManifestFilePath),
		                      SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
	} else {
		TraceEvent(SevError, "UploadBulkDumpJobManifestFileError", logId)
		    .detail("Reason", "Transport method is not implemented")
		    .detail("TransportMethod", transportMethod);
		ASSERT(false);
	}
	// TODO(BulkDump): check uploaded file exist
}

Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState) {
	Transaction tr(cx);
	ASSERT(bulkDumpState.isMetadataValid());
	Key beginKey = bulkDumpState.getRange().begin;
	Key endKey = bulkDumpState.getRange().end;
	KeyRange rangeToPersist;
	RangeResult result;
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			rangeToPersist = Standalone(KeyRangeRef(beginKey, endKey));
			result = co_await krmGetRanges(&tr, bulkDumpPrefix, rangeToPersist);
			if (result.empty()) {
				throw bulkdump_task_outdated();
			}
			bool anyNew = false;
			for (int i = 0; i < static_cast<int>(result.size()) - 1; i++) {
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
			co_await krmSetRange(&tr, bulkDumpPrefix, bulkDumpState.getRange(), bulkDumpStateValue(bulkDumpState));
			co_await tr.commit();
			beginKey = result[result.size() - 1].key;
			if (beginKey >= endKey) {
				break;
			} else {
				tr.reset();
			}
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}
