/*
 * BulkLoadUtils.actor.cpp
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

#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/S3Client.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<Void> readBulkFileBytes(std::string path, int64_t maxLength, std::shared_ptr<std::string> output) {
	try {
		output->clear();
		state int64_t chunkSize = SERVER_KNOBS->BULKLOAD_ASYNC_READ_WRITE_BLOCK_SIZE;
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    abspath(path), IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));
		state int64_t fileSize = wait(file->size());
		if (fileSize > maxLength) {
			TraceEvent(SevError, "ReadBulkFileBytesTooLarge")
			    .detail("FileSize", fileSize)
			    .detail("MaxLength", maxLength);
			throw file_too_large();
		}
		output->reserve(fileSize); // Pre-allocate the full size

		// Read in chunks to avoid memory pressure
		state int64_t offset = 0;
		state int64_t remaining = fileSize;
		state std::shared_ptr<std::string> chunk = std::make_shared<std::string>();
		while (remaining > 0) {
			state int64_t bytesToRead = std::min(chunkSize, remaining);
			chunk->clear();
			chunk->resize(bytesToRead);
			state int bytesRead = wait(uncancellable(holdWhile(chunk, file->read(chunk->data(), bytesToRead, offset))));
			if (bytesRead != bytesToRead) {
				TraceEvent(SevError, "ReadBulkFileBytesError")
				    .detail("BytesRead", bytesRead)
				    .detail("BytesExpected", bytesToRead);
				throw io_error();
			}
			output->append(*chunk);
			offset += bytesRead;
			remaining -= bytesRead;
		}
		return Void();
	} catch (Error& e) {
		TraceEvent(SevWarn, "ReadBulkFileBytesError").error(e).detail("Path", path).detail("MaxLength", maxLength);
		throw e;
	}
}

ACTOR Future<Void> writeBulkFileBytes(std::string path, std::shared_ptr<std::string> content) {
	try {
		state int64_t chunkSize = SERVER_KNOBS->BULKLOAD_ASYNC_READ_WRITE_BLOCK_SIZE;
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    abspath(path),
		    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
		    0644));

		// For large files, write in chunks to avoid memory pressure
		state int64_t offset = 0;
		state int64_t remaining = content->size();

		while (remaining > 0) {
			state int64_t bytesToWrite = std::min(chunkSize, remaining);
			wait(uncancellable(holdWhile(content, file->write(content->data() + offset, bytesToWrite, offset))));
			offset += bytesToWrite;
			remaining -= bytesToWrite;
		}

		// Ensure the file size is correct and data is synced
		wait(file->truncate(content->size()));
		wait(file->sync());
		return Void();
	} catch (Error& e) {
		TraceEvent(SevWarn, "WriteBulkFileBytesError").error(e).detail("Path", path);
		throw e;
	}
}

ACTOR Future<Void> copyBulkFile(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	state std::shared_ptr<std::string> content = std::make_shared<std::string>();
	wait(readBulkFileBytes(abspath(fromFile), fileBytesMax, /*output=*/content));
	wait(writeBulkFileBytes(toFile, content));
	return Void();
}

ACTOR Future<BulkLoadTaskState> getBulkLoadTaskStateFromDataMove(Database cx,
                                                                 UID dataMoveId,
                                                                 Version atLeastVersion,
                                                                 UID logId) {
	state Transaction tr(cx);
	state int retryCount = 0;
	state int metadataRetryCount = 0;
	state double startTime = now();
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	loop {
		try {
			state Optional<Value> val = wait(tr.get(dataMoveKeyFor(dataMoveId)));
			ASSERT(tr.getReadVersion().isReady());
			if (tr.getReadVersion().get() < atLeastVersion) {
				retryCount++;
				if (retryCount % 100 == 0) {
					TraceEvent(SevWarn, "SSBulkLoadTaskWaitingForVersion", logId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("ReadVersion", tr.getReadVersion().get())
					    .detail("AtLeastVersion", atLeastVersion)
					    .detail("RetryCount", retryCount)
					    .detail("ElapsedSec", now() - startTime);
				}
				wait(delay(0.1));
				tr.reset();
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				continue;
			}
			if (val.present()) {
				state DataMoveMetaData dataMoveMetaData = decodeDataMoveValue(val.get());
				if (dataMoveMetaData.bulkLoadTaskState.present()) {
					if (metadataRetryCount > 0 || retryCount > 0) {
						TraceEvent(SevInfo, "SSBulkLoadTaskGotMetadata", logId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("MetadataRetryCount", metadataRetryCount)
						    .detail("VersionRetryCount", retryCount)
						    .detail("ElapsedSec", now() - startTime);
					}
					return dataMoveMetaData.bulkLoadTaskState.get();
				} else {
					metadataRetryCount++;
					if (metadataRetryCount % 100 == 0) {
						TraceEvent(SevWarn, "SSBulkLoadTaskWaitingForMetadata", logId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("DataMovePhase", static_cast<int>(dataMoveMetaData.getPhase()))
						    .detail("MetadataRetryCount", metadataRetryCount)
						    .detail("ElapsedSec", now() - startTime)
						    .detail("Message", "DataMove exists but BulkLoadTaskState not yet written");
					}
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
			TraceEvent(SevWarnAlways, "SSBulkLoadTaskDataMoveIdNotExist", logId)
			    .detail("Message", "This fetchKey is blocked and will be cancelled later")
			    .detail("DataMoveID", dataMoveId)
			    .detail("ReadVersion", tr.getReadVersion().get())
			    .detail("AtLeastVersion", atLeastVersion)
			    .detail("ElapsedSec", now() - startTime);
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
	TraceEvent(bulkLoadVerboseEventSev(), "SSBulkLoadTaskSamplingComplete", logId)
	    .detail("DataFileFullPath", dataFileFullPath)
	    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
	    .detail("Duration", now() - startTime)
	    .detail("RetryCount", retryCount);
	return res;
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

ACTOR Future<Void> bulkLoadTransportCP_impl(BulkLoadFileSet fromRemoteFileSet,
                                            BulkLoadFileSet toLocalFileSet,
                                            size_t fileBytesMax,
                                            UID logId) {
	// Clear existing local folder
	resetFileFolder(abspath(toLocalFileSet.getFolder()));
	// Copy data file
	wait(copyBulkFile(
	    abspath(fromRemoteFileSet.getDataFileFullPath()), abspath(toLocalFileSet.getDataFileFullPath()), fileBytesMax));
	// Copy byte sample file if exists
	if (fromRemoteFileSet.hasByteSampleFile()) {
		wait(copyBulkFile(abspath(fromRemoteFileSet.getBytesSampleFileFullPath()),
		                  abspath(toLocalFileSet.getBytesSampleFileFullPath()),
		                  fileBytesMax));
	}
	// TODO(BulkLoad): Throw error if the date/bytesample file does not exist while the filename is not empty
	return Void();
}

ACTOR Future<Void> bulkLoadTransportBlobstore_impl(BulkLoadFileSet fromRemoteFileSet,
                                                   BulkLoadFileSet toLocalFileSet,
                                                   size_t fileBytesMax,
                                                   UID logId) {
	// Clear existing local folder
	resetFileFolder(abspath(toLocalFileSet.getFolder()));
	TraceEvent(SevDebug, "BulkLoadBlobstoreTransportStart", logId)
	    .detail("FromRemote", fromRemoteFileSet.toString())
	    .detail("ToLocal", toLocalFileSet.toString())
	    .detail("HasDataFile", fromRemoteFileSet.hasDataFile());
	// TODO(BulkLoad): Make use of fileBytesMax
	// TODO: File-at-a-time costs because we make connection for each.
	// Skip data file download if the range is empty (no data file)
	if (fromRemoteFileSet.hasDataFile()) {
		TraceEvent(SevDebug, "BulkLoadBlobstoreBeforeCopyDataFile", logId)
		    .detail("FromPath", fromRemoteFileSet.getDataFileFullPath())
		    .detail("ToPath", abspath(toLocalFileSet.getDataFileFullPath()));
		wait(copyDownFile(fromRemoteFileSet.getDataFileFullPath(), abspath(toLocalFileSet.getDataFileFullPath())));
		TraceEvent(SevDebug, "BulkLoadBlobstoreAfterCopyDataFile", logId);
	} else {
		TraceEvent("BulkLoadBlobstoreSkipEmptyRange", logId).detail("Reason", "No data file for empty range");
	}
	// Copy byte sample file if exists
	if (fromRemoteFileSet.hasByteSampleFile()) {
		TraceEvent(SevDebug, "BulkLoadBlobstoreBeforeCopySampleFile", logId)
		    .detail("FromPath", fromRemoteFileSet.getBytesSampleFileFullPath())
		    .detail("ToPath", abspath(toLocalFileSet.getBytesSampleFileFullPath()));
		wait(copyDownFile(fromRemoteFileSet.getBytesSampleFileFullPath(),
		                  abspath(toLocalFileSet.getBytesSampleFileFullPath())));
		TraceEvent(SevDebug, "BulkLoadBlobstoreAfterCopySampleFile", logId);
	}
	// TODO(BulkLoad): Throw error if the date/bytesample file does not exist while the filename is not empty
	TraceEvent(SevDebug, "BulkLoadBlobstoreTransportEnd", logId);
	return Void();
}

ACTOR Future<BulkLoadFileSet> bulkLoadDownloadTaskFileSet(BulkLoadTransportMethod transportMethod,
                                                          BulkLoadFileSet fromRemoteFileSet,
                                                          std::string toLocalRoot,
                                                          UID logId) {
	state int retryCount = 0;
	state double startTime = now();
	ASSERT(transportMethod != BulkLoadTransportMethod::Invalid);
	TraceEvent(SevDebug, "BulkLoadDownloadTaskFileSetStart", logId)
	    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
	    .detail("ToLocalRoot", toLocalRoot)
	    .detail("TransportMethod", transportMethod);
	loop {
		try {
			TraceEvent(SevDebug, "BulkLoadDownloadTaskFileSetAttempt", logId)
			    .detail("RetryCount", retryCount)
			    .detail("Elapsed", now() - startTime);
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
				TraceEvent(SevDebug, "BulkLoadDownloadBeforeCP", logId).detail("Elapsed", now() - startTime);
				// Copy the data file and the sample file from remote folder to a local folder specified by
				// fromRemoteFileSet.
				wait(bulkLoadTransportCP_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
				TraceEvent(SevDebug, "BulkLoadDownloadAfterCP", logId).detail("Elapsed", now() - startTime);
			} else if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
				TraceEvent(SevDebug, "BulkLoadDownloadBeforeBlobstore", logId).detail("Elapsed", now() - startTime);
				// Copy the data file and the sample file from remote folder to a local folder specified by
				// fromRemoteFileSet.
				wait(bulkLoadTransportBlobstore_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
				TraceEvent(SevDebug, "BulkLoadDownloadAfterBlobstore", logId).detail("Elapsed", now() - startTime);
			} else {
				UNREACHABLE();
			}
			TraceEvent(bulkLoadVerboseEventSev(), "SSBulkLoadTaskDownloadFileSet", logId)
			    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
			    .detail("ToLocalRoot", toLocalRoot)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);

			return toLocalFileSet;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarn, "SSBulkLoadTaskDownloadFileSetError", logId)
			    .errorUnsuppressed(e)
			    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
			    .detail("ToLocalRoot", toLocalRoot)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);
			retryCount++;
			if (retryCount > SERVER_KNOBS->BULKLOAD_DOWNLOAD_MAX_RETRIES) {
				TraceEvent(SevError, "SSBulkLoadTaskDownloadFileSetMaxRetriesExceeded", logId)
				    .errorUnsuppressed(e)
				    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
				    .detail("ToLocalRoot", toLocalRoot)
				    .detail("Duration", now() - startTime)
				    .detail("RetryCount", retryCount)
				    .detail("MaxRetries", SERVER_KNOBS->BULKLOAD_DOWNLOAD_MAX_RETRIES)
				    .detail("OriginalError", e.code())
				    .detail("OriginalErrorName", e.name());
				// Throw bulkload_task_failed to signal fetchKeys that this is a retryable bulk load error
				// This allows the data movement to be retried at the DD level instead of killing the SS
				throw bulkload_task_failed();
			}
			wait(delay(SERVER_KNOBS->BULKLOAD_DOWNLOAD_RETRY_DELAY));
		}
	}
}

ACTOR Future<Void> bulkLoadDownloadTaskFileSets(BulkLoadTransportMethod transportMethod,
                                                std::shared_ptr<BulkLoadFileSetKeyMap> fromRemoteFileSets,
                                                std::shared_ptr<BulkLoadFileSetKeyMap> localFileSets,
                                                std::string toLocalRoot,
                                                UID logId) {
	state BulkLoadFileSetKeyMap::iterator iter = fromRemoteFileSets->begin();
	state KeyRange keys;
	for (; iter != fromRemoteFileSets->end(); iter++) {
		keys = iter->first;
		if (!iter->second.hasDataFile()) {
			// For empty ranges (no data file), create an empty local fileSet entry so FetchKeys knows this range was
			// processed
			TraceEvent("BulkLoadDownloadSkipEmptyRange", logId)
			    .detail("Keys", keys)
			    .detail("Reason", "No data file for empty range");
			// Create a local fileSet with the same structure but no data/sample files (empty range marker)
			BulkLoadFileSet emptyLocalFileSet(toLocalRoot,
			                                  iter->second.getRelativePath(),
			                                  iter->second.getManifestFileName(),
			                                  "", // Empty data file name
			                                  "", // Empty sample file name
			                                  BulkLoadChecksum());
			localFileSets->push_back(std::make_pair(keys, emptyLocalFileSet));
			continue;
		}
		BulkLoadFileSet localFileSet =
		    wait(bulkLoadDownloadTaskFileSet(transportMethod, iter->second, toLocalRoot, logId));
		localFileSets->push_back(std::make_pair(keys, localFileSet));
	}
	return Void();
}

ACTOR Future<Void> downloadManifestFile(BulkLoadTransportMethod transportMethod,
                                        std::string fromRemotePath,
                                        std::string toLocalPath,
                                        UID logId) {
	state int retryCount = 0;
	state double startTime = now();
	TraceEvent(SevDebug, "BulkLoadDownloadManifestStart", logId)
	    .detail("FromRemotePath", fromRemotePath)
	    .detail("ToLocalPath", toLocalPath)
	    .detail("TransportMethod", transportMethod);
	loop {
		try {
			TraceEvent(SevDebug, "BulkLoadDownloadManifestAttempt", logId)
			    .detail("RetryCount", retryCount)
			    .detail("Elapsed", now() - startTime);
			if (transportMethod == BulkLoadTransportMethod::CP) {
				wait(
				    copyBulkFile(abspath(fromRemotePath), abspath(toLocalPath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX));
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
			TraceEvent(bulkLoadVerboseEventSev(), "BulkLoadDownloadManifestFile", logId)
			    .detail("FromRemotePath", fromRemotePath)
			    .detail("ToLocalPath", toLocalPath)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);
			break;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			}
			TraceEvent(SevWarnAlways, "BulkLoadDownloadManifestFileError", logId)
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
			wait(delay(SERVER_KNOBS->BULKLOAD_DOWNLOAD_RETRY_DELAY));
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

// Update manifestEntryMap and expanding mapRange if the input line overlaps with the job range.
KeyRange updateManifestEntryMap(const std::string& line,
                                const KeyRange& jobRange,
                                const KeyRange& mapRange,
                                std::shared_ptr<BulkLoadManifestFileMap> manifestEntryMap) {
	BulkLoadJobFileManifestEntry manifestEntry(line);
	KeyRange overlappingRange = jobRange & manifestEntry.getRange();
	if (!overlappingRange.empty()) {
		auto returnV = manifestEntryMap->insert({ overlappingRange.begin, manifestEntry });
		ASSERT(returnV.second);
		if (mapRange.empty()) {
			return KeyRangeRef(overlappingRange.begin, overlappingRange.end);
		} else {
			if (mapRange.begin > overlappingRange.begin) {
				return KeyRangeRef(overlappingRange.begin, mapRange.end);
			}
			if (mapRange.end < overlappingRange.end) {
				return KeyRangeRef(mapRange.begin, overlappingRange.end);
			}
		}
	}
	return mapRange;
}

// Get manifest within the input range.
// manifestEntryMap is the output.
// Return value is the range of the manifestEntryMap.
ACTOR Future<KeyRange> getBulkLoadJobFileManifestEntryFromJobManifestFile(
    std::string localJobManifestFilePath,
    KeyRange range,
    UID logId,
    std::shared_ptr<BulkLoadManifestFileMap> manifestEntryMap) {
	state KeyRange mapRange;
	state double startTime = now();
	ASSERT(fileExists(abspath(localJobManifestFilePath)));
	ASSERT(manifestEntryMap->empty());

	state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
	    abspath(localJobManifestFilePath),
	    IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED,
	    0644));

	state int64_t fileSize = wait(file->size());
	if (fileSize > SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX) {
		TraceEvent(SevError, "ManifestFileTooBig", logId)
		    .detail("FileSize", fileSize)
		    .detail("MaxSize", SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
		throw file_too_large();
	}

	state int64_t chunkSize = 64 * 1024; // 64KB chunks
	state std::string buffer;
	state int64_t offset = 0;
	state std::string leftover;
	state bool headerProcessed = false;

	try {
		while (offset < fileSize) {
			state int64_t bytesToRead = std::min(chunkSize, fileSize - offset);
			buffer.resize(bytesToRead);

			state int bytesRead = wait(file->read(&buffer[0], bytesToRead, offset));
			if (bytesRead != bytesToRead) {
				TraceEvent(SevError, "ReadFileError", logId)
				    .detail("BytesRead", bytesRead)
				    .detail("BytesExpected", bytesToRead);
				throw io_error();
			}

			// Process the chunk line by line
			state std::string chunk = leftover + buffer;
			state size_t pos = 0;
			state size_t lineStart = 0;

			while ((pos = chunk.find(bulkLoadJobManifestLineTerminator, lineStart)) != std::string::npos) {
				state std::string line = chunk.substr(lineStart, pos - lineStart);
				if (!line.empty()) {
					if (!headerProcessed) {
						// First line is header
						BulkLoadJobManifestFileHeader header(line);
						headerProcessed = true;
					} else {
						mapRange = updateManifestEntryMap(line, range, mapRange, /*output=*/manifestEntryMap);
					}
				}
				lineStart = pos + 1;

				// Yield every 100 entries
				if (manifestEntryMap->size() % 100 == 0) {
					wait(yield());
				}
			}

			// Save any partial line for the next chunk
			leftover = chunk.substr(lineStart);
			offset += bytesRead;
			wait(yield());
		}

		// Process the last line if it didn't end with a newline
		if (!leftover.empty()) {
			if (!headerProcessed) {
				// If we somehow only have one line and it's the header
				BulkLoadJobManifestFileHeader header(leftover);
			} else {
				mapRange = updateManifestEntryMap(leftover, range, mapRange, /*output=*/manifestEntryMap);
			}
		}
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "BulkLoadJobFileManifestEntryError", logId)
		    .errorUnsuppressed(e)
		    .detail("LocalJobManifestFilePath", localJobManifestFilePath)
		    .detail("Duration", now() - startTime)
		    .detail("Offset", offset);
		throw e;
	}

	return mapRange;
}

ACTOR Future<BulkLoadManifestSet> getBulkLoadManifestMetadataFromEntry(
    std::vector<BulkLoadJobFileManifestEntry> manifestEntries,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    std::string jobRoot,
    UID logId) {
	state BulkLoadManifestSet manifests(SERVER_KNOBS->MANIFEST_COUNT_MAX_PER_BULKLOAD_TASK);
	state int i = 0;
	for (; i < manifestEntries.size(); i++) {
		state std::string remoteManifestFilePath = appendToPath(jobRoot, manifestEntries[i].getManifestRelativePath());
		state std::string localManifestFilePath = joinPath(manifestLocalTempFolder,
		                                                   deterministicRandom()->randomUniqueID().toString() + "-" +
		                                                       basename(getPath(remoteManifestFilePath)));
		wait(downloadManifestFile(transportMethod, remoteManifestFilePath, localManifestFilePath, logId));
		state std::shared_ptr<std::string> manifestRawString = std::make_shared<std::string>();
		wait(readBulkFileBytes(
		    abspath(localManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, manifestRawString));
		ASSERT(!manifestRawString->empty());
		BulkLoadManifest manifest(*manifestRawString);
		ASSERT(manifests.addManifest(manifest));
	}
	return manifests;
}
