/*
 * BulkLoadUtil.cpp
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
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/S3Client.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/BulkLoadUtil.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/RocksDBCheckpointUtils.h"
#include "fdbserver/core/StorageMetrics.h"
#include "flow/genericactors.actor.h"

Future<Void> readBulkFileBytes(std::string path, int64_t maxLength, std::shared_ptr<std::string> output) {
	try {
		output->clear();
		int64_t chunkSize = SERVER_KNOBS->BULKLOAD_ASYNC_READ_WRITE_BLOCK_SIZE;
		Reference<IAsyncFile> file = co_await IAsyncFileSystem::filesystem()->open(
		    abspath(path), IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644);
		int64_t fileSize = co_await file->size();
		if (fileSize > maxLength) {
			TraceEvent(SevError, "ReadBulkFileBytesTooLarge")
			    .detail("FileSize", fileSize)
			    .detail("MaxLength", maxLength);
			throw file_too_large();
		}
		output->reserve(fileSize); // Pre-allocate the full size

		// Read in chunks to avoid memory pressure
		int64_t offset = 0;
		int64_t remaining = fileSize;
		auto chunk = std::make_shared<std::string>();
		while (remaining > 0) {
			int64_t bytesToRead = std::min(chunkSize, remaining);
			chunk->clear();
			chunk->resize(bytesToRead);
			int bytesRead = co_await uncancellable(holdWhile(chunk, file->read(chunk->data(), bytesToRead, offset)));
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
	} catch (Error& e) {
		TraceEvent(SevWarn, "ReadBulkFileBytesError").error(e).detail("Path", path).detail("MaxLength", maxLength);
		throw e;
	}
}

Future<Void> writeBulkFileBytes(std::string path, std::shared_ptr<std::string> content) {
	try {
		int64_t chunkSize = SERVER_KNOBS->BULKLOAD_ASYNC_READ_WRITE_BLOCK_SIZE;
		Reference<IAsyncFile> file = co_await IAsyncFileSystem::filesystem()->open(
		    abspath(path),
		    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
		    0644);

		// For large files, write in chunks to avoid memory pressure
		int64_t offset = 0;
		int64_t remaining = content->size();

		while (remaining > 0) {
			int64_t bytesToWrite = std::min(chunkSize, remaining);
			co_await uncancellable(holdWhile(content, file->write(content->data() + offset, bytesToWrite, offset)));
			offset += bytesToWrite;
			remaining -= bytesToWrite;
		}

		// Ensure the file size is correct and data is synced
		co_await file->truncate(content->size());
		co_await file->sync();
	} catch (Error& e) {
		TraceEvent(SevWarn, "WriteBulkFileBytesError").error(e).detail("Path", path);
		throw e;
	}
}

Future<Void> copyBulkFile(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	auto content = std::make_shared<std::string>();
	co_await readBulkFileBytes(abspath(fromFile), fileBytesMax, /*output=*/content);
	co_await writeBulkFileBytes(toFile, content);
}

Future<BulkLoadTaskState> getBulkLoadTaskStateFromDataMove(Database cx,
                                                           UID dataMoveId,
                                                           Version atLeastVersion,
                                                           UID logId) {
	Transaction tr(cx);
	int retryCount = 0;
	int metadataRetryCount = 0;
	double startTime = now();
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	while (true) {
		Error err;
		try {
			Optional<Value> val = co_await tr.get(dataMoveKeyFor(dataMoveId));
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
				co_await delay(0.1);
				tr.reset();
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				continue;
			}
			if (val.present()) {
				DataMoveMetaData dataMoveMetaData = decodeDataMoveValue(val.get());
				if (dataMoveMetaData.bulkLoadTaskState.present()) {
					if (metadataRetryCount > 0 || retryCount > 0) {
						TraceEvent(SevInfo, "SSBulkLoadTaskGotMetadata", logId)
						    .detail("DataMoveID", dataMoveId)
						    .detail("MetadataRetryCount", metadataRetryCount)
						    .detail("VersionRetryCount", retryCount)
						    .detail("ElapsedSec", now() - startTime);
					}
					co_return dataMoveMetaData.bulkLoadTaskState.get();
				}

				metadataRetryCount++;
				if (metadataRetryCount % 100 == 0) {
					TraceEvent(SevWarn, "SSBulkLoadTaskWaitingForMetadata", logId)
					    .detail("DataMoveID", dataMoveId)
					    .detail("DataMovePhase", static_cast<int>(dataMoveMetaData.getPhase()))
					    .detail("MetadataRetryCount", metadataRetryCount)
					    .detail("ElapsedSec", now() - startTime)
					    .detail("Message", "DataMove exists but BulkLoadTaskState not yet written");
				}
				co_await delay(0.1);
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
			TraceEvent(SevWarnAlways, "SSBulkLoadTaskDataMoveIdNotExist", logId)
			    .detail("Message", "This fetchKey is blocked and will be cancelled later")
			    .detail("DataMoveID", dataMoveId)
			    .detail("ReadVersion", tr.getReadVersion().get())
			    .detail("AtLeastVersion", atLeastVersion)
			    .detail("ElapsedSec", now() - startTime);
			co_await Future<Void>(Never());
			throw internal_error(); // does not happen
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Look up BulkLoadTaskState directly from bulkLoadTaskKeys by range, without going through DataMoveMetaData.
// This allows bulk load to work without SHARD_ENCODE_LOCATION_METADATA / dataMoveId.
// The function retries until the read version >= atLeastVersion and a valid task is found.
// If no task exists for the range, blocks forever (caller is expected to cancel via fetchKeys cancellation).
Future<BulkLoadTaskState> getBulkLoadTaskStateByRange(Database cx, KeyRange range, Version atLeastVersion, UID logId) {
	Transaction tr(cx);
	int retryCount = 0;
	int metadataRetryCount = 0;
	double startTime = now();
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	while (true) {
		Error err;
		try {
			RangeResult result = co_await krmGetRanges(&tr, bulkLoadTaskPrefix, range);
			ASSERT(tr.getReadVersion().isReady());
			if (tr.getReadVersion().get() < atLeastVersion) {
				retryCount++;
				if (retryCount % 100 == 0) {
					TraceEvent(SevWarn, "SSBulkLoadTaskByRangeWaitingForVersion", logId)
					    .detail("Range", range)
					    .detail("ReadVersion", tr.getReadVersion().get())
					    .detail("AtLeastVersion", atLeastVersion)
					    .detail("RetryCount", retryCount)
					    .detail("ElapsedSec", now() - startTime);
				}
				co_await delay(0.1);
				tr.reset();
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				continue;
			}
			if (result.size() >= 2 && !result[0].value.empty()) {
				BulkLoadTaskState bulkLoadTaskState = decodeBulkLoadTaskState(result[0].value);
				if (bulkLoadTaskState.isValid()) {
					if (metadataRetryCount > 0 || retryCount > 0) {
						TraceEvent(SevInfo, "SSBulkLoadTaskByRangeGotMetadata", logId)
						    .detail("Range", range)
						    .detail("TaskID", bulkLoadTaskState.getTaskId())
						    .detail("TaskRange", bulkLoadTaskState.getRange())
						    .detail("MetadataRetryCount", metadataRetryCount)
						    .detail("VersionRetryCount", retryCount)
						    .detail("ElapsedSec", now() - startTime);
					}
					// Log if the task range doesn't match the requested range
					if (bulkLoadTaskState.getRange() != range) {
						TraceEvent(SevWarn, "SSBulkLoadTaskByRangeRangeMismatch", logId)
						    .detail("RequestedRange", range)
						    .detail("TaskRange", bulkLoadTaskState.getRange())
						    .detail("TaskID", bulkLoadTaskState.getTaskId());
					}
					co_return bulkLoadTaskState;
				}
			}

			metadataRetryCount++;
			if (metadataRetryCount % 100 == 0) {
				TraceEvent(SevWarn, "SSBulkLoadTaskByRangeWaitingForMetadata", logId)
				    .detail("Range", range)
				    .detail("MetadataRetryCount", metadataRetryCount)
				    .detail("ElapsedSec", now() - startTime)
				    .detail("Message", "BulkLoadTaskState not yet written for this range");
			}
			co_await delay(0.1);
			tr.reset();
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Return true if generated the byte sampling file. Otherwise, return false.
// TODO(BulkDump): directly read from special key space.
Future<bool> doBytesSamplingOnDataFile(std::string dataFileFullPath, // input file
                                       std::string byteSampleFileFullPath, // output file
                                       UID logId) {
	int counter = 0;
	bool res = false;
	int retryCount = 0;
	double startTime = now();
	while (true) {
		Error err;
		try {
			std::unique_ptr<IRocksDBSstFileWriter> sstWriter = newRocksDBSstFileWriter();
			sstWriter->open(abspath(byteSampleFileFullPath));
			bool anySampled = false;
			std::unique_ptr<IRocksDBSstFileReader> reader = newRocksDBSstFileReader();
			reader->open(abspath(dataFileFullPath));
			while (reader->hasNext()) {
				KeyValue kv = reader->next();
				ByteSampleInfo sampleInfo = isKeyValueInSample(kv);
				if (sampleInfo.inSample) {
					sstWriter->write(kv.key, BinaryWriter::toValue(sampleInfo.sampledSize, Unversioned()));
					anySampled = true;
					counter++;
					if (counter > SERVER_KNOBS->BULKLOAD_BYTE_SAMPLE_BATCH_KEY_COUNT) {
						co_await yield();
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
			err = e;
		}
		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}
		TraceEvent(SevWarn, "SSBulkLoadTaskSamplingError", logId)
		    .errorUnsuppressed(err)
		    .detail("DataFileFullPath", dataFileFullPath)
		    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
		    .detail("Duration", now() - startTime)
		    .detail("RetryCount", retryCount);
		co_await delay(5.0);
		deleteFile(abspath(byteSampleFileFullPath));
		retryCount++;
	}
	TraceEvent(bulkLoadVerboseEventSev(), "SSBulkLoadTaskSamplingComplete", logId)
	    .detail("DataFileFullPath", dataFileFullPath)
	    .detail("ByteSampleFileFullPath", byteSampleFileFullPath)
	    .detail("Duration", now() - startTime)
	    .detail("RetryCount", retryCount);
	co_return res;
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

Future<Void> bulkLoadTransportCP_impl(BulkLoadFileSet fromRemoteFileSet,
                                      BulkLoadFileSet toLocalFileSet,
                                      size_t fileBytesMax,
                                      UID logId) {
	// Clear existing local folder
	resetFileFolder(abspath(toLocalFileSet.getFolder()));
	// Copy data file
	co_await copyBulkFile(
	    abspath(fromRemoteFileSet.getDataFileFullPath()), abspath(toLocalFileSet.getDataFileFullPath()), fileBytesMax);
	// Copy byte sample file if exists
	if (fromRemoteFileSet.hasByteSampleFile()) {
		co_await copyBulkFile(abspath(fromRemoteFileSet.getBytesSampleFileFullPath()),
		                      abspath(toLocalFileSet.getBytesSampleFileFullPath()),
		                      fileBytesMax);
	}
	// TODO(BulkLoad): Throw error if the date/bytesample file does not exist while the filename is not empty
}

Future<Void> bulkLoadTransportBlobstore_impl(BulkLoadFileSet fromRemoteFileSet,
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
		co_await copyDownFile(fromRemoteFileSet.getDataFileFullPath(), abspath(toLocalFileSet.getDataFileFullPath()));
		TraceEvent(SevDebug, "BulkLoadBlobstoreAfterCopyDataFile", logId);
	} else {
		TraceEvent("BulkLoadBlobstoreSkipEmptyRange", logId).detail("Reason", "No data file for empty range");
	}
	// Copy byte sample file if exists
	if (fromRemoteFileSet.hasByteSampleFile()) {
		TraceEvent(SevDebug, "BulkLoadBlobstoreBeforeCopySampleFile", logId)
		    .detail("FromPath", fromRemoteFileSet.getBytesSampleFileFullPath())
		    .detail("ToPath", abspath(toLocalFileSet.getBytesSampleFileFullPath()));
		co_await copyDownFile(fromRemoteFileSet.getBytesSampleFileFullPath(),
		                      abspath(toLocalFileSet.getBytesSampleFileFullPath()));
		TraceEvent(SevDebug, "BulkLoadBlobstoreAfterCopySampleFile", logId);
	}
	// TODO(BulkLoad): Throw error if the date/bytesample file does not exist while the filename is not empty
	TraceEvent(SevDebug, "BulkLoadBlobstoreTransportEnd", logId);
}

Future<BulkLoadFileSet> bulkLoadDownloadTaskFileSet(BulkLoadTransportMethod transportMethod,
                                                    BulkLoadFileSet fromRemoteFileSet,
                                                    std::string toLocalRoot,
                                                    UID logId) {
	int retryCount = 0;
	double startTime = now();
	ASSERT(transportMethod != BulkLoadTransportMethod::Invalid);
	TraceEvent(SevDebug, "BulkLoadDownloadTaskFileSetStart", logId)
	    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
	    .detail("ToLocalRoot", toLocalRoot)
	    .detail("TransportMethod", transportMethod);
	while (true) {
		Error err;
		try {
			TraceEvent(SevDebug, "BulkLoadDownloadTaskFileSetAttempt", logId)
			    .detail("RetryCount", retryCount)
			    .detail("Elapsed", now() - startTime);
			// Step 1: Generate local file set based on remote file set by replacing the remote root to the local root.
			BulkLoadFileSet toLocalFileSet(toLocalRoot,
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
				co_await bulkLoadTransportCP_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
				TraceEvent(SevDebug, "BulkLoadDownloadAfterCP", logId).detail("Elapsed", now() - startTime);
			} else if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
				TraceEvent(SevDebug, "BulkLoadDownloadBeforeBlobstore", logId).detail("Elapsed", now() - startTime);
				// Copy the data file and the sample file from remote folder to a local folder specified by
				// fromRemoteFileSet.
				co_await bulkLoadTransportBlobstore_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId);
				TraceEvent(SevDebug, "BulkLoadDownloadAfterBlobstore", logId).detail("Elapsed", now() - startTime);
			} else {
				UNREACHABLE();
			}
			TraceEvent(bulkLoadVerboseEventSev(), "SSBulkLoadTaskDownloadFileSet", logId)
			    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
			    .detail("ToLocalRoot", toLocalRoot)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount);
			co_return toLocalFileSet;
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}
		TraceEvent(SevWarn, "SSBulkLoadTaskDownloadFileSetError", logId)
		    .errorUnsuppressed(err)
		    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
		    .detail("ToLocalRoot", toLocalRoot)
		    .detail("Duration", now() - startTime)
		    .detail("RetryCount", retryCount);
		retryCount++;
		if (retryCount > SERVER_KNOBS->BULKLOAD_DOWNLOAD_MAX_RETRIES) {
			TraceEvent(SevError, "SSBulkLoadTaskDownloadFileSetMaxRetriesExceeded", logId)
			    .errorUnsuppressed(err)
			    .detail("FromRemoteFileSet", fromRemoteFileSet.toString())
			    .detail("ToLocalRoot", toLocalRoot)
			    .detail("Duration", now() - startTime)
			    .detail("RetryCount", retryCount)
			    .detail("MaxRetries", SERVER_KNOBS->BULKLOAD_DOWNLOAD_MAX_RETRIES)
			    .detail("OriginalError", err.code())
			    .detail("OriginalErrorName", err.name());
			// Throw bulkload_task_failed to signal fetchKeys that this is a retryable bulk load error
			// This allows the data movement to be retried at the DD level instead of killing the SS
			throw bulkload_task_failed();
		}
		co_await delay(SERVER_KNOBS->BULKLOAD_DOWNLOAD_RETRY_DELAY);
	}
}

Future<Void> bulkLoadDownloadTaskFileSets(BulkLoadTransportMethod transportMethod,
                                          std::shared_ptr<BulkLoadFileSetKeyMap> fromRemoteFileSets,
                                          std::shared_ptr<BulkLoadFileSetKeyMap> localFileSets,
                                          std::string toLocalRoot,
                                          UID logId) {
	KeyRange keys;
	for (auto iter = fromRemoteFileSets->begin(); iter != fromRemoteFileSets->end(); iter++) {
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
		    co_await bulkLoadDownloadTaskFileSet(transportMethod, iter->second, toLocalRoot, logId);
		localFileSets->push_back(std::make_pair(keys, localFileSet));
	}
}

Future<Void> downloadManifestFile(BulkLoadTransportMethod transportMethod,
                                  std::string fromRemotePath,
                                  std::string toLocalPath,
                                  UID logId) {
	int retryCount = 0;
	double startTime = now();
	TraceEvent(SevDebug, "BulkLoadDownloadManifestStart", logId)
	    .detail("FromRemotePath", fromRemotePath)
	    .detail("ToLocalPath", toLocalPath)
	    .detail("TransportMethod", transportMethod);
	while (true) {
		Error err;
		try {
			TraceEvent(SevDebug, "BulkLoadDownloadManifestAttempt", logId)
			    .detail("RetryCount", retryCount)
			    .detail("Elapsed", now() - startTime);
			if (transportMethod == BulkLoadTransportMethod::CP) {
				co_await copyBulkFile(
				    abspath(fromRemotePath), abspath(toLocalPath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
				co_await delay(0.1);
			} else if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
				// TODO: Make use of fileBytesMax
				co_await copyDownFile(fromRemotePath, abspath(toLocalPath));
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
			err = e;
		}
		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}
		TraceEvent(SevWarnAlways, "BulkLoadDownloadManifestFileError", logId)
		    .errorUnsuppressed(err)
		    .detail("TransportMethod", transportMethod)
		    .detail("FromRemotePath", fromRemotePath)
		    .detail("ToLocalPath", toLocalPath)
		    .detail("RetryCount", retryCount)
		    .detail("Duration", now() - startTime);
		retryCount++;
		if (retryCount > 10) {
			throw err;
		}
		co_await delay(SERVER_KNOBS->BULKLOAD_DOWNLOAD_RETRY_DELAY);
	}
}

// Download job manifest file
// Each job has one manifest file including manifest paths of all tasks
Future<Void> downloadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                             std::string localJobManifestFilePath,
                                             std::string remoteJobManifestFilePath,
                                             UID logId) {
	co_await downloadManifestFile(transportMethod, remoteJobManifestFilePath, localJobManifestFilePath, logId);
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
Future<KeyRange> getBulkLoadJobFileManifestEntryFromJobManifestFile(
    std::string localJobManifestFilePath,
    KeyRange range,
    UID logId,
    std::shared_ptr<BulkLoadManifestFileMap> manifestEntryMap) {
	KeyRange mapRange;
	double startTime = now();
	ASSERT(fileExists(abspath(localJobManifestFilePath)));
	ASSERT(manifestEntryMap->empty());

	Reference<IAsyncFile> file = co_await IAsyncFileSystem::filesystem()->open(
	    abspath(localJobManifestFilePath),
	    IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED,
	    0644);

	int64_t fileSize = co_await file->size();
	if (fileSize > SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX) {
		TraceEvent(SevError, "ManifestFileTooBig", logId)
		    .detail("FileSize", fileSize)
		    .detail("MaxSize", SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
		throw file_too_large();
	}

	int64_t chunkSize = 64 * 1024; // 64KB chunks
	std::string buffer;
	int64_t offset = 0;
	std::string leftover;
	bool headerProcessed = false;

	try {
		while (offset < fileSize) {
			int64_t bytesToRead = std::min(chunkSize, fileSize - offset);
			buffer.resize(bytesToRead);

			int bytesRead = co_await file->read(&buffer[0], bytesToRead, offset);
			if (bytesRead != bytesToRead) {
				TraceEvent(SevError, "ReadFileError", logId)
				    .detail("BytesRead", bytesRead)
				    .detail("BytesExpected", bytesToRead);
				throw io_error();
			}

			// Process the chunk line by line
			std::string chunk = leftover + buffer;
			size_t pos = 0;
			size_t lineStart = 0;

			while ((pos = chunk.find(bulkLoadJobManifestLineTerminator, lineStart)) != std::string::npos) {
				std::string line = chunk.substr(lineStart, pos - lineStart);
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
					co_await yield();
				}
			}

			// Save any partial line for the next chunk
			leftover = chunk.substr(lineStart);
			offset += bytesRead;
			co_await yield();
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

	co_return mapRange;
}

Future<BulkLoadManifestSet> getBulkLoadManifestMetadataFromEntry(
    std::vector<BulkLoadJobFileManifestEntry> manifestEntries,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    std::string jobRoot,
    UID logId) {
	BulkLoadManifestSet manifests(SERVER_KNOBS->MANIFEST_COUNT_MAX_PER_BULKLOAD_TASK);
	for (int i = 0; i < manifestEntries.size(); ++i) {
		std::string remoteManifestFilePath = appendToPath(jobRoot, manifestEntries[i].getManifestRelativePath());
		std::string localManifestFilePath = joinPath(manifestLocalTempFolder,
		                                             deterministicRandom()->randomUniqueID().toString() + "-" +
		                                                 basename(getPath(remoteManifestFilePath)));
		co_await downloadManifestFile(transportMethod, remoteManifestFilePath, localManifestFilePath, logId);
		auto manifestRawString = std::make_shared<std::string>();
		co_await readBulkFileBytes(
		    abspath(localManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, manifestRawString);
		ASSERT(!manifestRawString->empty());
		BulkLoadManifest manifest(*manifestRawString);
		ASSERT(manifests.addManifest(manifest));
	}
	co_return manifests;
}
