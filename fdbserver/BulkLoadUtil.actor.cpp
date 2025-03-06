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
#include "flow/IAsyncFile.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/flow.h"

ACTOR Future<std::string> readBulkFileBytes(std::string path, int64_t maxLength) {
	try {
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    abspath(path), IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0644));

		state int64_t fileSize = wait(file->size());
		if (fileSize > maxLength) {
			TraceEvent(SevError, "ReadBulkFileBytesTooLarge")
			    .detail("FileSize", fileSize)
			    .detail("MaxLength", maxLength);
			throw file_too_large();
		}

		state std::string content;
		content.reserve(fileSize); // Pre-allocate the full size

		// For small files (< 1MB), do a single read
		if (fileSize < 1024 * 1024) {
			content.resize(fileSize);
			int bytesRead = wait(file->read(&content[0], fileSize, 0));
			if (bytesRead != fileSize) {
				TraceEvent(SevError, "ReadBulkFileBytesError")
				    .detail("BytesRead", bytesRead)
				    .detail("BytesExpected", fileSize);
				throw io_error();
			}
		} else {
			// For large files, read in chunks to avoid memory pressure
			state int64_t chunkSize = 1024 * 1024; // 1MB chunks
			state int64_t offset = 0;
			state int64_t remaining = fileSize;

			while (remaining > 0) {
				state int64_t bytesToRead = std::min(chunkSize, remaining);
				state std::string chunk;
				chunk.resize(bytesToRead);

				state int bytesRead = wait(file->read(&chunk[0], bytesToRead, offset));
				if (bytesRead != bytesToRead) {
					TraceEvent(SevError, "ReadBulkFileBytesError")
					    .detail("BytesRead", bytesRead)
					    .detail("BytesExpected", bytesToRead);
					throw io_error();
				}

				content.append(chunk);
				offset += bytesRead;
				remaining -= bytesRead;

				// Yield every 4MB to allow other actors to run
				if (offset % (4 * 1024 * 1024) == 0) {
					wait(yield());
				}
			}
		}

		return content;
	} catch (Error& e) {
		TraceEvent(SevWarn, "ReadBulkFileBytesError").error(e).detail("Path", path).detail("MaxLength", maxLength);
		throw;
	}
}

ACTOR Future<Void> writeBulkFileBytes(std::string path, StringRef content) {
	try {
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    abspath(path),
		    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE,
		    0644));

		// For small files (< 1MB), do a single write
		if (content.size() < 1024 * 1024) {
			wait(file->write(content.begin(), content.size(), 0));
		} else {
			// For large files, write in chunks to avoid memory pressure
			state int64_t chunkSize = 1024 * 1024; // 1MB chunks
			state int64_t offset = 0;
			state int64_t remaining = content.size();

			while (remaining > 0) {
				state int64_t bytesToWrite = std::min(chunkSize, remaining);
				wait(file->write(content.begin() + offset, bytesToWrite, offset));
				offset += bytesToWrite;
				remaining -= bytesToWrite;

				// Yield every 4MB to allow other actors to run
				if (offset % (4 * 1024 * 1024) == 0) {
					wait(yield());
				}
			}
		}

		// Ensure the file size is correct and data is synced
		wait(file->truncate(content.size()));
		wait(file->sync());

		return Void();
	} catch (Error& e) {
		TraceEvent(SevWarn, "WriteBulkFileBytesError")
		    .error(e)
		    .detail("Path", path)
		    .detail("ContentSize", content.size());
		throw;
	}
}

ACTOR Future<Void> copyBulkFile(std::string fromFile, std::string toFile, size_t fileBytesMax) {
	state std::string content = wait(readBulkFileBytes(abspath(fromFile), fileBytesMax));
	wait(writeBulkFileBytes(toFile, StringRef(content)));
	return Void();
}

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
			TraceEvent(SevWarnAlways, "SSBulkLoadTaskDataMoveIdNotExist", logId)
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
				wait(bulkLoadTransportCP_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
			} else if (transportMethod == BulkLoadTransportMethod::BLOBSTORE) {
				// Copy the data file and the sample file from remote folder to a local folder specified by
				// fromRemoteFileSet.
				wait(bulkLoadTransportBlobstore_impl(
				    fromRemoteFileSet, toLocalFileSet, SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX, logId));
			} else {
				UNREACHABLE();
			}
			// TODO(BulkLoad): Check file checksum
			TraceEvent(SevInfo, "SSBulkLoadTaskDownloadFileSet", logId)
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
			TraceEvent(SevWarn, "SSBulkLoadTaskDownloadFileSetError", logId)
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
			TraceEvent(SevInfo, "BulkLoadDownloadManifestFile", logId)
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
	state double startTime = now();
	ASSERT(fileExists(abspath(localJobManifestFilePath)));
	state std::unordered_map<Key, BulkLoadJobFileManifestEntry> res;

	state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
	    abspath(localJobManifestFilePath),
	    IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED,
	    0644));

	state int64_t fileSize = wait(file->size());
	if (fileSize > SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX) {
		TraceEvent(SevError, "ManifestFileTooBig", logId)
		    .detail("FileSize", fileSize)
		    .detail("MaxSize", SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX);
		throw file_too_large(); // TODO(BulkLoad): handle this unretrievable error
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
				throw io_error(); // TODO(BulkLoad): handle this unretrievable error
			}

			// Process the chunk line by line
			state std::string chunk = leftover + buffer;
			state size_t pos = 0;
			state size_t lineStart = 0;

			while ((pos = chunk.find('\n', lineStart)) != std::string::npos) {
				state std::string line = chunk.substr(lineStart, pos - lineStart);
				if (!line.empty()) {
					if (!headerProcessed) {
						// First line is header
						BulkLoadJobManifestFileHeader header(line);
						headerProcessed = true;
					} else {
						BulkLoadJobFileManifestEntry manifestEntry(line);
						KeyRange overlappingRange = range & manifestEntry.getRange();
						if (!overlappingRange.empty()) {
							auto returnV = res.insert({ manifestEntry.getBeginKey(), manifestEntry });
							ASSERT(returnV.second);
						}
					}
				}
				lineStart = pos + 1;

				// Yield every 100 entries
				if (res.size() % 100 == 0) {
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
				BulkLoadJobFileManifestEntry manifestEntry(leftover);
				KeyRange overlappingRange = range & manifestEntry.getRange();
				if (!overlappingRange.empty()) {
					auto returnV = res.insert({ manifestEntry.getBeginKey(), manifestEntry });
					ASSERT(returnV.second);
				}
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
	std::string manifestRawString =
	    wait(readBulkFileBytes(abspath(localManifestFilePath), SERVER_KNOBS->BULKLOAD_FILE_BYTES_MAX));
	ASSERT(!manifestRawString.empty());
	return BulkLoadManifest(manifestRawString);
}
