/*
 * BulkLoadUtils.actor.cpp
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

#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include <cstddef>
#include <fmt/format.h>
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/flow.h"

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

// Return true if generated the byte sampling file. Otherwise, return false.
// TODO(BulkDump): directly read from special key space.
ACTOR Future<bool> doBytesSamplingOnDataFile(std::string dataFileFullPath, // input file
                                             std::string byteSampleFileFullPath, // output file
                                             UID logId) {
	state int counter = 0;
	loop {
		try {
			state std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
			sstWriter->open(abspath(byteSampleFileFullPath));
			state bool anySampled = false;
			state std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader();
			reader->open(abspath(dataFileFullPath));
			while (reader->hasNext()) {
				KeyValue kv = reader->next();
				ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
				if (sampleInfo.inSample) {
					sstWriter->write(kv.key, BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned()));
					anySampled = true;
					counter++;
					if (counter > SERVER_KNOBS->BULKLOAD_BYTE_SAMPLE_BATCH_KEY_COUNT) {
						wait(yield());
						counter = 0;
					}
				}
			}
			// It is possible that no key is sampled
			// This can happen when the data to sample is small
			// In this case, no SST sample byte file is generated
			if (anySampled) {
				ASSERT(sstWriter->finish());
				return true;
			} else {
				ASSERT(!sstWriter->finish());
				deleteFile(abspath(byteSampleFileFullPath));
				return false;
			}
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadTaskSamplingError", logId).errorUnsuppressed(e);
			wait(delay(5.0));
			deleteFile(abspath(byteSampleFileFullPath));
		}
	}
}

void bulkLoadFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	std::string content = readFileBytes(abspath(fromFile), fileBytesMax);
	writeFile(abspath(toFile), content);
	return;
}

void bulkLoadTransportCP_impl(BulkLoadFileSet fromRemoteFileSet,
                              BulkLoadFileSet toLocalFileSet,
                              size_t fileBytesMax,
                              UID logId) {
	// Clear existing local folder
	platform::eraseDirectoryRecursive(abspath(toLocalFileSet.getFolder()));
	ASSERT(platform::createDirectory(abspath(toLocalFileSet.getFolder())));
	// Copy data file
	bulkLoadFileCopy(
	    abspath(fromRemoteFileSet.getDataFileFullPath()), abspath(toLocalFileSet.getDataFileFullPath()), fileBytesMax);
	// Copy byte sample file if exists
	if (fromRemoteFileSet.hasByteSampleFile()) {
		bulkLoadFileCopy(abspath(fromRemoteFileSet.getBytesSampleFileFullPath()),
		                 abspath(toLocalFileSet.getBytesSampleFileFullPath()),
		                 fileBytesMax);
	}
	// TODO(BulkLoad): Throw error if the date/bytesample file does not exist while the filename is not empty
	return;
}

ACTOR Future<BulkLoadFileSet> bulkLoadDownloadTaskFileSet(BulkLoadTransportMethod transportMethod,
                                                          BulkLoadFileSet fromRemoteFileSet,
                                                          std::string toLocalRoot,
                                                          UID logId) {
	ASSERT(transportMethod != BulkLoadTransportMethod::Invalid);
	loop {
		try {
			// Step 1: Generate local file set based on remote file set by replacing the remote root to the local root.
			state BulkLoadFileSet toLocalFileSet(toLocalRoot,
			                                     fromRemoteFileSet.getRelativePath(),
			                                     fromRemoteFileSet.getManifestFileName(),
			                                     fromRemoteFileSet.getDataFileName(),
			                                     fromRemoteFileSet.getByteSampleFileName(),
			                                     BulkLoadChecksum());

			// Step 2: Download remote file set to local folder
			if (transportMethod == BulkLoadTransportMethod::CP) {
				ASSERT(fromRemoteFileSet.hasDataFile());
				// Copy the data file and the sample file from remote folder to a local folder specified by
				// fromRemoteFileSet.
				bulkLoadTransportCP_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
			} else {
				ASSERT(false);
			}
			// TODO(BulkLoad): Check file checksum

			return toLocalFileSet;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadDownloadTaskFileSetError", logId).errorUnsuppressed(e);
			wait(delay(5.0));
		}
	}
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
				bulkLoadFileCopy(abspath(fromRemotePath), abspath(toLocalPath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
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

// Download job manifest file
// Each job has one manifest file including manifest paths of all tasks
ACTOR Future<Void> downloadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                   std::string localJobManifestFilePath,
                                                   std::string remoteJobManifestFilePath,
                                                   UID logId) {
	wait(downloadSingleFile(transportMethod, remoteJobManifestFilePath, localJobManifestFilePath, logId));
	return Void();
}

// Get manifest within the input range
ACTOR Future<std::unordered_map<Key, BulkLoadManifest>> getBulkLoadManifestMetadataFromFiles(
    std::string localJobManifestFilePath,
    KeyRange range,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    std::string jobRoot,
    UID logId) {
	ASSERT(fileExists(abspath(localJobManifestFilePath)));
	state std::unordered_map<Key, BulkLoadManifest> res;
	const std::string jobManifestRawString =
	    readFileBytes(abspath(localJobManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
	state std::vector<std::string> lines = splitString(jobManifestRawString, "\n");
	state BulkLoadJobManifestFileHeader header(lines[0]);
	state size_t lineIdx = 1; // skip the first line which is the header
	while (lineIdx < lines.size()) {
		if (lines[lineIdx].empty()) {
			ASSERT(lineIdx == lines.size() - 1);
			break;
		}
		BulkLoadJobFileManifestEntry manifestEntry(lines[lineIdx]);
		KeyRange overlappingRange = range & manifestEntry.getRange();
		if (overlappingRange.empty()) {
			// Ignore the manifest entry if no overlapping range
			lineIdx = lineIdx + 1;
			continue;
		}
		state std::string remoteManifestFilePath = joinPath(jobRoot, manifestEntry.getManifestRelativePath());
		platform::eraseDirectoryRecursive(abspath(manifestLocalTempFolder));
		ASSERT(platform::createDirectory(abspath(manifestLocalTempFolder)));
		state std::string localManifestFilePath = joinPath(manifestLocalTempFolder, basename(remoteManifestFilePath));
		// Download the manifest file
		// TODO(BulkLoad): to maximize the throughput, we want to do downloadSingleFile() in parallel.
		wait(downloadSingleFile(transportMethod, remoteManifestFilePath, localManifestFilePath, logId));
		const std::string manifestRawString =
		    readFileBytes(abspath(localManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
		ASSERT(!manifestRawString.empty());
		BulkLoadManifest manifest(manifestRawString);
		auto returnV = res.insert({ manifest.getBeginKey(), manifest });
		ASSERT(returnV.second);
		wait(delay(1.0));
		lineIdx = lineIdx + 1;
	}
	platform::eraseDirectoryRecursive(abspath(manifestLocalTempFolder));
	return res;
}
