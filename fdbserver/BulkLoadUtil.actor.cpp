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
#include "fdbclient/S3Client.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include <cstddef>
#include <fmt/format.h>
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<BulkLoadTaskState> getBulkLoadTaskStateFromDataMove(Database cx,
                                                                 UID dataMoveId,
                                                                 Version atLeastVersion,
                                                                 UID logId) {
	state Transaction tr(cx);
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	loop {
		try {
			state Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
			ASSERT(tr.getReadVersion().isReady());
			if (tr.getReadVersion().get() < atLeastVersion) {
				wait(delay(0.1));
				tr.reset();
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				continue;
			}
			if (val.present()) {
				state DataMoveMetaData dataMoveMetaData = decodeDataMoveValue(val.get());
				if (dataMoveMetaData.bulkLoadTaskState.present()) {
					return dataMoveMetaData.bulkLoadTaskState.get();
				} else {
					wait(delay(0.1));
					tr.reset();
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					// DD moveShard progressively persists dataMoveMetadata based on range.
					// Therefore, it is possible that the dataMoveMetadata is persisted at a subrange partially.
					// Note that DD moveShard only persist the bulkload metadata in the dataMoveMetadata when all
					// subranges have been persisted. Therefore, if the data move id indicating the move is for bulk
					// loading, the SS should wait until the bulkLoad metadata is included in the dataMoveMetadata.
					continue;
				}
			}
			TraceEvent(SevWarnAlways, "SSBulkLoadDataMoveIdNotExist", logId)
			    .detail("Message", "This fetchKey is blocked and will be cancelled later")
			    .detail("DataMoveID", dataMoveId)
			    .detail("ReadVersion", tr.getReadVersion().get())
			    .detail("AtLeastVersion", atLeastVersion);
			wait(Never());
			throw internal_error(); // does not happen
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
	state bool res = false;
	state int retryCount = 0;
	state double startTime = now();
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
				res = true;
			} else {
				ASSERT(!sstWriter->finish());
				deleteFile(abspath(byteSampleFileFullPath));
			}
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadTaskSamplingError", logId)
			    .errorUnsuppressed(e)
			    .detail("DataFileFullPath", dataFileFullPath)
			    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);
			wait(delay(5.0));
			deleteFile(abspath(byteSampleFileFullPath));
			retryCount++;
		}
	}
	TraceEvent(SevInfo, "SSBulkLoadTaskSamplingComplete", logId)
	    .detail("DataFileFullPath", dataFileFullPath)
	    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
	    .detail("Duration", now() - startTime)
	    .detail("RetryCount", retryCount);
	return res;
}

void bulkLoadFileCopy(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	std::string content = readFileBytes(abspath(fromFile), fileBytesMax);
	writeFile(abspath(toFile), content);
	return;
}

// TODO(BulkLoad): slow task
void clearFileFolder(const std::string& folderPath, const UID& logId, bool ignoreError) {
	try {
		platform::eraseDirectoryRecursive(abspath(folderPath));
	} catch (Error& e) {
		if (logId.isValid()) {
			TraceEvent(ignoreError ? SevWarn : SevWarnAlways, "ClearFileFolderError", logId)
			    .error(e)
			    .detail("FolderPath", folderPath);
		}
		if (ignoreError) {
			return;
		}
		throw e;
	}
	return;
}

// TODO(BulkLoad): slow task
void resetFileFolder(const std::string& folderPath) {
	clearFileFolder(abspath(folderPath));
	ASSERT(platform::createDirectory(abspath(folderPath)));
	return;
}

void bulkLoadTransportCP_impl(BulkLoadFileSet fromRemoteFileSet,
                              BulkLoadFileSet toLocalFileSet,
                              size_t fileBytesMax,
                              UID logId) {
	// Clear existing local folder
	resetFileFolder(abspath(toLocalFileSet.getFolder()));
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

ACTOR Future<Void> bulkLoadTransportBlobstore_impl(BulkLoadFileSet fromRemoteFileSet,
                                                   BulkLoadFileSet toLocalFileSet,
                                                   size_t fileBytesMax,
                                                   UID logId) {
	// Clear existing local folder
	resetFileFolder(abspath(toLocalFileSet.getFolder()));
	// TODO(BulkLoad): Make use of fileBytesMax
	// TODO: File-at-a-time costs because we make connection for each.
	wait(copyDownFile(fromRemoteFileSet.getDataFileFullPath(), abspath(toLocalFileSet.getDataFileFullPath())));
	// Copy byte sample file if exists
	if (fromRemoteFileSet.hasByteSampleFile()) {
		wait(copyDownFile(fromRemoteFileSet.getBytesSampleFileFullPath(),
		                  abspath(toLocalFileSet.getBytesSampleFileFullPath())));
	}
	// TODO(BulkLoad): Throw error if the date/bytesample file does not exist while the filename is not empty
	return Void();
}

ACTOR Future<BulkLoadFileSet> bulkLoadDownloadTaskFileSet(BulkLoadTransportMethod transportMethod,
                                                          BulkLoadFileSet fromRemoteFileSet,
                                                          std::string toLocalRoot,
                                                          UID logId) {
	state int retryCount = 0;
	state double startTime = now();
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
			} else if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
				// Copy the data file and the sample file from remote folder to a local folder specified by
				// fromRemoteFileSet.
				wait(bulkLoadTransportBlobstore_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
			} else {
				UNREACHABLE();
			}
			// TODO(BulkLoad): Check file checksum
			TraceEvent(SevInfo, "SSBulkLoadDownloadTaskFileSet", logId)
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
			    .detail("ToLocalRoot", toLocalRoot)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);

			return toLocalFileSet;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadDownloadTaskFileSetError", logId)
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .errorUnsuppressed(e)
			    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
			    .detail("ToLocalRoot", toLocalRoot)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);
			retryCount++;
			wait(delay(5.0));
		}
	}
}

ACTOR Future<Void> downloadManifestFile(BulkLoadTransportMethod transportMethod,
                                        std::string fromRemotePath,
                                        std::string toLocalPath,
                                        UID logId) {
	state int retryCount = 0;
	state double startTime = now();
	loop {
		try {
			if (transportMethod == BulkLoadTransportMethod::CP) {
				bulkLoadFileCopy(abspath(fromRemotePath), abspath(toLocalPath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
				wait(delay(0.1));
			} else if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
				// TODO: Make use of fileBytesMax
				wait(copyDownFile(fromRemotePath, abspath(toLocalPath)));
			} else {
				UNREACHABLE();
			}
			if (!fileExists(abspath(toLocalPath))) {
				throw retry();
			}
			TraceEvent(SevInfo, "DownloadManifestFile", logId)
			    .detail("FromRemotePath", fromRemotePath)
			    .detail("ToLocalPath", toLocalPath)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarnAlways, "DownloadManifestFileError", logId)
			    .errorUnsuppressed(e)
			    .detail("TransportMethod", transportMethod)
			    .detail("FromRemotePath", fromRemotePath)
			    .detail("ToLocalPath", toLocalPath)
			    .detail("RetryCount", retryCount)
			    .detail("Duration", now() - startTime);
			retryCount++;
			if (retryCount > 10) {
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
	wait(downloadManifestFile(transportMethod, remoteJobManifestFilePath, localJobManifestFilePath, logId));
	return Void();
}

// Get manifest within the input range
ACTOR Future<std::unordered_map<Key, BulkLoadJobFileManifestEntry>>
getBulkLoadJobFileManifestEntryFromJobManifestFile(std::string localJobManifestFilePath, KeyRange range, UID logId) {
	ASSERT(fileExists(abspath(localJobManifestFilePath)));
	state std::unordered_map<Key, BulkLoadJobFileManifestEntry> res;
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
			// Here we do not do break here because we always scan the entire file assuming the manifest entry is not
			// sorted by beginKey in the file.
			continue;
		}
		auto returnV = res.insert({ manifestEntry.getBeginKey(), manifestEntry });
		ASSERT(returnV.second);
		if (lineIdx % 1000 == 0) {
			wait(delay(0.1)); // yield per batch
		}
		lineIdx = lineIdx + 1;
	}
	return res;
}

ACTOR Future<BulkLoadManifest> getBulkLoadManifestMetadataFromEntry(BulkLoadJobFileManifestEntry manifestEntry,
                                                                    std::string manifestLocalTempFolder,
                                                                    BulkLoadTransportMethod transportMethod,
                                                                    std::string jobRoot,
                                                                    UID logId) {
	state std::string remoteManifestFilePath = appendToPath(jobRoot, manifestEntry.getManifestRelativePath());
	state std::string localManifestFilePath =
	    joinPath(manifestLocalTempFolder,
	             deterministicRandom()->randomUniqueID().toString() + "-" + basename(getPath(remoteManifestFilePath)));
	wait(downloadManifestFile(transportMethod, remoteManifestFilePath, localManifestFilePath, logId));
	const std::string manifestRawString =
	    readFileBytes(abspath(localManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
	ASSERT(!manifestRawString.empty());
	return BulkLoadManifest(manifestRawString);
}
