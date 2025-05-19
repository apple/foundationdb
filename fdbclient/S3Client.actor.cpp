/*
 * S3Client.actor.cpp
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

#include <string>
#include <vector>
#include <unordered_set>
#include <algorithm>

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/S3Client.actor.h"
#include "flow/IAsyncFile.h"
#include "flow/Trace.h"
#include "flow/Traceable.h"
#include "flow/flow.h"
#include "flow/xxhash.h"
#include "flow/Error.h"
#include "rapidxml/rapidxml.hpp"

#include "flow/actorcompiler.h" // has to be last include

#define S3_CHECKSUM_TAG_NAME "xxhash64"

typedef XXH64_state_t XXHashState;

// State for a part of a multipart upload.
struct PartState {
	int partNumber = 0;
	std::string etag;
	int64_t offset = 0;
	int64_t size = 0;
	std::string md5;
	bool completed = false;

	PartState() = default; // Add explicit default constructor

	PartState(int pNum, int64_t off, int64_t sz, std::string m = "")
	  : partNumber(pNum), offset(off), size(sz), md5(m) {}
};

// Config for a part of a multipart upload.
struct PartConfig {
	// Let this be the minimum configured part size.
	int64_t partSizeBytes = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	// Retry delay for multipart uploads
	int retryDelayMs = CLIENT_KNOBS->BLOBSTORE_MULTIPART_RETRY_DELAY_MS;
};

// Calculate hash of a file.
// Uses xxhash library because it's fast (supposedly) and used elsewhere in fdb.
// If size is -1, the function will determine the file size automatically.
// Returns a hex string representation of the xxhash64 checksum.
ACTOR Future<std::string> calculateFileChecksum(Reference<IAsyncFile> file, int64_t size) {
	state int64_t pos = 0;
	state XXH64_state_t* hashState = XXH64_createState();
	state std::vector<uint8_t> buffer(65536);
	state int readSize;

	XXH64_reset(hashState, 0);

	try {
		if (size == -1) {
			int64_t s = wait(file->size());
			size = s;
		}

		while (pos < size) {
			readSize = std::min<int64_t>(buffer.size(), size - pos);
			int bytesRead = wait(file->read(buffer.data(), readSize, pos));
			if (bytesRead != readSize) {
				XXH64_freeState(hashState);
				TraceEvent(SevError, "S3ClientCalculateChecksumReadError")
				    .detail("Expected", readSize)
				    .detail("Actual", bytesRead)
				    .detail("Position", pos);
				throw io_error();
			}
			XXH64_update(hashState, buffer.data(), bytesRead);
			pos += bytesRead;
		}

		uint64_t hash = XXH64_digest(hashState);
		XXH64_freeState(hashState);
		return format("%016llx", hash);
	} catch (Error& e) {
		XXH64_freeState(hashState);
		throw;
	}
}

// Get the endpoint for the given s3url.
// Populates parameters and resource with parse of s3url.
Reference<S3BlobStoreEndpoint> getEndpoint(const std::string& s3url,
                                           std::string& resource,
                                           S3BlobStoreEndpoint::ParametersT& parameters) {
	try {
		std::string error;
		Optional<std::string> proxy;
		auto res = g_network->global(INetwork::enProxy);
		if (res) {
			proxy = *static_cast<Optional<std::string>*>(res);
		}
		Reference<S3BlobStoreEndpoint> endpoint =
		    S3BlobStoreEndpoint::fromString(s3url, proxy, &resource, &error, &parameters);

		if (!endpoint) {
			TraceEvent(SevError, "S3ClientGetEndpointNullEndpoint").detail("URL", s3url).detail("Error", error);
			throw backup_invalid_url();
		}

		// Let empty resource path be valid - it means list root of bucket

		// Validate bucket parameter exists
		if (parameters.find("bucket") == parameters.end()) {
			TraceEvent(SevError, "S3ClientGetEndpointMissingBucket").detail("URL", s3url);
			throw backup_invalid_url();
		}

		// Validate resource path characters
		for (char c : resource) {
			if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/') {
				TraceEvent(SevError, "S3ClientGetEndpointIllegalCharacter")
				    .detail("URL", s3url)
				    .detail("Character", std::string(1, c));
				throw backup_invalid_url();
			}
		}

		if (!error.empty()) {
			TraceEvent(SevError, "S3ClientGetEndpointError").detail("URL", s3url).detail("Error", error);
			throw backup_invalid_url();
		}

		return endpoint;

	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientGetEndpointFailed").detail("URL", s3url).detail("Error", e.what());
		throw;
	}
}

// Upload a part of a multipart upload with retry logic.
ACTOR static Future<PartState> uploadPart(Reference<S3BlobStoreEndpoint> endpoint,
                                          std::string bucket,
                                          std::string objectName,
                                          std::string uploadID,
                                          PartState part, // Pass by value for state management
                                          std::string partData,
                                          int retryDelayMs) {
	state double startTime = now();
	state PartState resultPart = part;
	state UnsentPacketQueue packets;
	TraceEvent(SevDebug, "S3ClientUploadPartStart")
	    .detail("Bucket", bucket)
	    .detail("Object", objectName)
	    .detail("PartNumber", part.partNumber)
	    .detail("Offset", resultPart.offset)
	    .detail("Size", resultPart.size);

	try {
		PacketWriter pw(packets.getWriteBuffer(partData.size()), nullptr, Unversioned());
		pw.serializeBytes(partData);

		std::string etag = wait(endpoint->uploadPart(
		    bucket, objectName, uploadID, resultPart.partNumber, &packets, partData.size(), resultPart.md5));

		resultPart.etag = etag;
		resultPart.completed = true;
		TraceEvent(SevDebug, "S3ClientUploadPartEnd")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("Offset", resultPart.offset)
		    .detail("Duration", now() - startTime)
		    .detail("Size", resultPart.size);
		return resultPart;
	} catch (Error& e) {
		TraceEvent(SevWarn, "S3ClientUploadPartError")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("ErrorCode", e.code());
		throw;
	}
}

// Copy filepath to bucket at resource in s3.
ACTOR static Future<Void> copyUpFile(Reference<S3BlobStoreEndpoint> endpoint,
                                     std::string bucket,
                                     std::string objectName,
                                     std::string filepath,
                                     PartConfig config = PartConfig()) {
	state double startTime = now();
	state Reference<IAsyncFile> file;
	state std::string uploadID;
	state std::vector<Future<PartState>> uploadFutures;
	state std::vector<PartState> parts;
	state std::vector<std::string> partDatas;
	state int64_t size = fileSize(filepath);

	try {
		TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpFileStart")
		    .detail("Filepath", filepath)
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("FileSize", size);

		// Open file once with UNCACHED for both checksum and upload.
		// TODO(BulkLoad): Optimize this to avoid double reading the file. Consider:
		// 1. Using memory-mapped files if available
		// 2. Caching the file contents in memory
		// 3. Using the same file handle for both checksum and upload
		Reference<IAsyncFile> f = wait(
		    IAsyncFileSystem::filesystem()->open(filepath, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0));
		file = f;

		// Calculate checksum using the same file handle
		state std::string checksum = wait(calculateFileChecksum(file, size));

		// Start multipart upload
		std::string id = wait(endpoint->beginMultiPartUpload(bucket, objectName));
		uploadID = id;

		state int64_t offset = 0;
		state int partNumber = 1;

		// Prepare parts
		while (offset < size) {
			state int64_t partSize = std::min(config.partSizeBytes, size - offset);

			// Store part data in our vector to keep it alive
			partDatas.emplace_back();
			partDatas.back().resize(partSize);

			int bytesRead = wait(file->read(&partDatas.back()[0], partSize, offset));
			if (bytesRead != partSize) {
				TraceEvent(SevError, "S3ClientCopyUpFileReadError")
				    .detail("Expected", partSize)
				    .detail("Actual", bytesRead)
				    .detail("Offset", offset)
				    .detail("FilePath", filepath);
				throw io_error();
			}

			std::string md5 = HTTP::computeMD5Sum(partDatas.back());
			state PartState part;
			part.partNumber = partNumber;
			part.offset = offset;
			part.size = partSize;
			part.md5 = md5;
			parts.push_back(part);

			uploadFutures.push_back(
			    uploadPart(endpoint, bucket, objectName, uploadID, part, partDatas.back(), config.retryDelayMs));

			offset += partSize;
			partNumber++;
		}

		// Wait for all uploads to complete and collect results
		std::vector<PartState> p = wait(getAll(uploadFutures));
		parts = p;

		// Clear upload futures to ensure they're done
		uploadFutures.clear();

		// Only close the file after all uploads are complete
		file = Reference<IAsyncFile>();

		// Verify all parts completed and prepare etag map
		std::map<int, std::string> etagMap;
		for (const auto& part : parts) {
			if (!part.completed) {
				TraceEvent(SevWarnAlways, "S3ClientCopyUpFilePartNotCompleted")
				    .detail("PartNumber", part.partNumber)
				    .detail("Offset", part.offset)
				    .detail("Size", part.size);
				throw operation_failed();
			}
			etagMap[part.partNumber] = part.etag;
		}

		wait(endpoint->finishMultiPartUpload(bucket, objectName, uploadID, etagMap));

		// Clear data after successful upload
		parts.clear();
		partDatas.clear();

		// TODO(BulkLoad): Consider returning a map of part numbers to their checksums
		// This would allow for more granular integrity verification during downloads
		// and could help identify which specific part failed if there's an issue.

		// Add the checksum as a tag after successful upload
		state std::map<std::string, std::string> tags;
		tags[S3_CHECKSUM_TAG_NAME] = checksum;
		wait(endpoint->putObjectTags(bucket, objectName, tags));

		TraceEvent(s3PerfEventSev(), "S3ClientCopyUpFileEnd")
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("FileSize", size)
		    .detail("Parts", parts.size())
		    .detail("Checksum", checksum)
		    .detail("Duration", now() - startTime);

		return Void();
	} catch (Error& e) {
		state Error err = e;
		TraceEvent(SevWarnAlways, "S3ClientCopyUpFileError")
		    .detail("Filepath", filepath)
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("Error", err.what());
		// Close file before abort attempt
		file = Reference<IAsyncFile>();
		// Attempt to abort the upload but don't wait for it
		try {
			wait(endpoint->abortMultiPartUpload(bucket, objectName, uploadID));
		} catch (Error& abortError) {
			// Log abort failure but throw original error
			TraceEvent(SevWarnAlways, "S3ClientCopyUpFileAbortError")
			    .error(abortError)
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("OriginalError", err.what());
		}
		throw err;
	}
}

ACTOR Future<Void> copyUpFile(std::string filepath, std::string s3url) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	wait(copyUpFile(endpoint, parameters["bucket"], resource, filepath));
	return Void();
}

ACTOR Future<Void> copyUpDirectory(std::string dirpath, std::string s3url) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	state Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	state std::vector<std::string> files;
	platform::findFilesRecursively(dirpath, files);
	TraceEvent(s3VerboseEventSev(), "S3ClientUploadDirStart")
	    .detail("Filecount", files.size())
	    .detail("Bucket", bucket)
	    .detail("Resource", resource);
	for (const auto& file : files) {
		std::string filepath = file;
		std::string s3path = resource + "/" + file.substr(dirpath.size() + 1);
		wait(copyUpFile(endpoint, bucket, s3path, filepath));
	}
	TraceEvent(s3VerboseEventSev(), "S3ClientUploadDirEnd").detail("Bucket", bucket).detail("Resource", resource);
	return Void();
}

ACTOR Future<Void> copyUpBulkDumpFileSet(std::string s3url,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	state Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpBulkDumpFileSetStart")
	    .detail("Bucket", bucket)
	    .detail("SourceFileSet", sourceFileSet.toString())
	    .detail("DestinationFileSet", destinationFileSet.toString());
	state int pNumDeleted = 0;
	state int64_t pBytesDeleted = 0;
	state std::string batch_dir = joinPath(getPath(s3url), destinationFileSet.getRelativePath());
	// Delete the batch dir if it exists already (need to check bucket exists else 404 and s3blobstore errors out).
	bool exists = wait(endpoint->bucketExists(bucket));
	if (exists) {
		wait(endpoint->deleteRecursively(bucket, batch_dir, &pNumDeleted, &pBytesDeleted));
	}
	// Destination for manifest file.
	auto destinationManifestPath = joinPath(batch_dir, destinationFileSet.getManifestFileName());
	wait(copyUpFile(endpoint, bucket, destinationManifestPath, sourceFileSet.getManifestFileFullPath()));
	if (sourceFileSet.hasDataFile()) {
		auto destinationDataPath = joinPath(batch_dir, destinationFileSet.getDataFileName());
		wait(copyUpFile(endpoint, bucket, destinationDataPath, sourceFileSet.getDataFileFullPath()));
	}
	if (sourceFileSet.hasByteSampleFile()) {
		ASSERT(sourceFileSet.hasDataFile());
		auto destinationByteSamplePath = joinPath(batch_dir, destinationFileSet.getByteSampleFileName());
		wait(copyUpFile(endpoint, bucket, destinationByteSamplePath, sourceFileSet.getBytesSampleFileFullPath()));
	}
	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpBulkDumpFileSetEnd")
	    .detail("BatchDir", batch_dir)
	    .detail("NumDeleted", pNumDeleted)
	    .detail("BytesDeleted", pBytesDeleted);
	return Void();
}

ACTOR static Future<PartState> downloadPartWithRetry(Reference<S3BlobStoreEndpoint> endpoint,
                                                     std::string bucket,
                                                     std::string objectName,
                                                     Reference<IAsyncFile> file,
                                                     PartState part,
                                                     int retryDelayMs) {
	state std::vector<uint8_t> buffer;
	state PartState resultPart = part;

	try {
		TraceEvent(SevDebug, "S3ClientDownloadPartStart")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("Offset", resultPart.offset)
		    .detail("Size", resultPart.size);

		buffer.resize(resultPart.size);

		// Add range validation
		if (resultPart.offset < 0 || resultPart.size <= 0) {
			TraceEvent(SevError, "S3ClientDownloadPartInvalidRange")
			    .detail("Offset", resultPart.offset)
			    .detail("Size", resultPart.size);
			throw operation_failed();
		}

		int bytesRead =
		    wait(endpoint->readObject(bucket, objectName, buffer.data(), resultPart.size, resultPart.offset));

		if (bytesRead != resultPart.size) {
			TraceEvent(SevError, "S3ClientDownloadPartSizeMismatch")
			    .detail("Expected", resultPart.size)
			    .detail("Actual", bytesRead)
			    .detail("Offset", resultPart.offset)
			    .detail("FilePath", file->getFilename());
			throw io_error();
		}

		// Verify MD5 checksum if provided
		if (!resultPart.md5.empty()) {
			std::string calculatedMD5 = HTTP::computeMD5Sum(std::string((char*)buffer.data(), bytesRead));
			if (resultPart.md5 != calculatedMD5) {
				TraceEvent(SevWarnAlways, "S3ClientDownloadPartMD5Mismatch")
				    .detail("Expected", resultPart.md5)
				    .detail("Calculated", calculatedMD5);
				throw checksum_failed();
			}
		}

		wait(file->write(buffer.data(), bytesRead, resultPart.offset));

		resultPart.completed = true;
		TraceEvent(SevDebug, "S3ClientDownloadPartEnd")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("Offset", resultPart.offset)
		    .detail("Size", resultPart.size);
		return resultPart;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "S3ClientDownloadPartError")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("Error", e.what())
		    .detail("PartNumber", part.partNumber);
		throw;
	}
}

// Copy down file from s3 to filepath.
ACTOR static Future<Void> copyDownFile(Reference<S3BlobStoreEndpoint> endpoint,
                                       std::string bucket,
                                       std::string objectName,
                                       std::string filepath,
                                       PartConfig config = PartConfig()) {
	state double startTime = now();
	state Reference<IAsyncFile> file;
	state std::vector<Future<PartState>> downloadFutures;
	state std::vector<PartState> parts;
	state int64_t fileSize = 0;
	state int64_t offset = 0;
	state int partNumber = 1;
	state int64_t partSize;
	state std::map<std::string, std::string> tags;
	state std::string expectedChecksum;

	try {
		TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownFileStart")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("FilePath", filepath);

		int64_t s = wait(endpoint->objectSize(bucket, objectName));
		if (s <= 0) {
			TraceEvent(SevWarnAlways, "S3ClientCopyDownFileEmptyFile")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName);
			throw file_not_found();
		}
		fileSize = s;

		// Create parent directory if it doesn't exist
		std::string dirPath = filepath.substr(0, filepath.find_last_of("/"));
		if (!dirPath.empty()) {
			platform::createDirectory(dirPath);
		}

		// Pre-allocate vectors to avoid reallocations
		int numParts = (fileSize + config.partSizeBytes - 1) / config.partSizeBytes;
		parts.reserve(numParts);
		downloadFutures.reserve(numParts);
		Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
		    filepath, IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED, 0644));
		file = f;
		// Pre-allocate file size to avoid fragmentation
		wait(file->truncate(fileSize));

		while (offset < fileSize) {
			partSize = std::min(config.partSizeBytes, fileSize - offset);

			parts.emplace_back(partNumber, offset, partSize, "");

			downloadFutures.push_back(
			    downloadPartWithRetry(endpoint, bucket, objectName, file, parts.back(), config.retryDelayMs));

			offset += partSize;
			partNumber++;
		}
		std::vector<PartState> completedParts = wait(getAll(downloadFutures));

		// Clear futures to free memory
		downloadFutures.clear();

		// Verify all parts completed
		for (const auto& part : completedParts) {
			if (!part.completed) {
				TraceEvent(SevError, "S3ClientCopyDownFilePartNotCompleted").detail("PartNumber", part.partNumber);
				throw operation_failed();
			}
		}

		// Set exact file size: without this, file has padding on the end.
		wait(file->truncate(fileSize));
		// Ensure all data is written to disk
		wait(file->sync());

		// Get and verify checksum before closing the file
		std::map<std::string, std::string> t = wait(endpoint->getObjectTags(bucket, objectName));
		tags = t;
		auto it = tags.find(S3_CHECKSUM_TAG_NAME);
		if (it != tags.end()) {
			expectedChecksum = it->second;
			if (!expectedChecksum.empty()) {
				state std::string actualChecksum = wait(calculateFileChecksum(file));
				if (actualChecksum != expectedChecksum) {
					TraceEvent(SevError, "S3ClientCopyDownFileChecksumMismatch")
					    .detail("Expected", expectedChecksum)
					    .detail("Calculated", actualChecksum)
					    .detail("FileSize", fileSize)
					    .detail("FilePath", filepath);
					// TODO(BulkLoad): Consider making this a non-retryable error since
					// retrying is unlikely to help if the checksum doesn't match.
					// This would require adding a new error type and updating callers.
					throw checksum_failed();
				}
			}
		}

		// Close file properly
		file = Reference<IAsyncFile>();

		TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownFileEnd")
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("FileSize", fileSize)
		    .detail("Duration", now() - startTime)
		    .detail("Checksum", expectedChecksum)
		    .detail("Parts", parts.size());

		return Void();
	} catch (Error& e) {
		state Error err = e;
		TraceEvent(SevWarnAlways, "S3ClientCopyDownFileError")
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("Error", err.what())
		    .detail("FilePath", filepath)
		    .detail("FileSize", fileSize);

		// Clean up the file in case of error
		if (file) {
			try {
				wait(file->sync());
				file = Reference<IAsyncFile>();
				IAsyncFileSystem::filesystem()->deleteFile(filepath, false);
			} catch (Error& e) {
				TraceEvent(SevWarnAlways, "S3ClientCopyDownFileCleanupError")
				    .detail("FilePath", filepath)
				    .detail("Error", e.what());
			}
		}
		throw err;
	}
}

ACTOR Future<Void> copyDownFile(std::string s3url, std::string filepath) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	wait(copyDownFile(endpoint, parameters["bucket"], resource, filepath));
	return Void();
}

ACTOR Future<Void> copyDownDirectory(std::string s3url, std::string dirpath) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	state Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	S3BlobStoreEndpoint::ListResult items = wait(endpoint->listObjects(bucket, resource));
	state std::vector<S3BlobStoreEndpoint::ObjectInfo> objects = items.objects;
	TraceEvent(s3VerboseEventSev(), "S3ClientDownDirectoryStart")
	    .detail("Filecount", objects.size())
	    .detail("Bucket", bucket)
	    .detail("Resource", resource);
	for (const auto& object : objects) {
		std::string filepath = dirpath + "/" + object.name.substr(resource.size());
		std::string s3path = object.name;
		wait(copyDownFile(endpoint, bucket, s3path, filepath));
	}
	TraceEvent(s3VerboseEventSev(), "S3ClientDownDirectoryEnd").detail("Bucket", bucket).detail("Resource", resource);
	return Void();
}

ACTOR Future<Void> deleteResource(std::string s3url) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	wait(endpoint->deleteRecursively(bucket, resource));
	return Void();
}

ACTOR Future<Void> listFiles(std::string s3url, int maxDepth) {
	try {
		state std::string resource;
		state std::string error;
		state S3BlobStoreEndpoint::ParametersT parameters;
		state Reference<S3BlobStoreEndpoint> bstore = getEndpoint(s3url, resource, parameters);

		if (!bstore) {
			TraceEvent(SevError, "S3ClientListingFailed").detail("Error", error);
			throw backup_invalid_url();
		}

		// Get bucket directly from parameters
		state std::string bucket = parameters["bucket"];

		// Check if bucket exists first
		bool exists = wait(bstore->bucketExists(bucket));
		if (!exists) {
			std::cerr << "ERROR: Bucket '" << bucket << "' does not exist" << std::endl;
			throw http_request_failed();
		}

		// Let S3BlobStoreEndpoint handle the resource path construction
		state Optional<char> delimiter;
		if (maxDepth <= 1) {
			delimiter = Optional<char>('/');
		}

		// Use listObjects with the resource path directly, letting S3BlobStoreEndpoint handle URL construction
		state S3BlobStoreEndpoint::ListResult result =
		    wait(bstore->listObjects(bucket, resource, delimiter, maxDepth > 1));

		// Format and display the objects
		std::cout << "Contents of " << s3url << ":" << std::endl;

		// Track directories to avoid duplicates
		std::set<std::string> directories;

		// Helper function to format size in human-readable format
		auto formatSize = [](int64_t size) -> std::string {
			const char* units[] = { "B", "KB", "MB", "GB", "TB", "PB" };
			int unit = 0;
			double value = static_cast<double>(size);
			while (value >= 1024.0 && unit < 5) {
				value /= 1024.0;
				unit++;
			}
			char buffer[32];
			snprintf(buffer, sizeof(buffer), "%.2f %s", value, units[unit]);
			return std::string(buffer);
		};

		// First print common prefixes (directories)
		for (const auto& prefix : result.commonPrefixes) {
			std::string dirName = prefix;
			// Remove trailing slash if present
			if (!dirName.empty() && dirName.back() == '/') {
				dirName.pop_back();
			}
			directories.insert(dirName);
			std::cout << "  " << dirName << "/" << std::endl;
		}

		// Then print objects, skipping those in directories we've already printed
		for (const auto& object : result.objects) {
			std::string objectName = object.name;
			// Skip if this object is in a directory we've already printed
			bool skip = false;
			for (const auto& dir : directories) {
				if (objectName.find(dir + "/") == 0) {
					skip = true;
					break;
				}
			}
			if (!skip) {
				std::cout << " " << objectName << " " << formatSize(object.size) << std::endl;
			}
		}
	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientListFilesError").detail("URL", s3url).detail("Error", e.what());
		if (e.code() == error_code_backup_invalid_url) {
			std::cerr << "ERROR: Invalid blobstore URL: " << s3url << std::endl;
		} else if (e.code() == error_code_backup_auth_missing) {
			std::cerr << "ERROR: Authentication information missing from URL" << std::endl;
		} else if (e.code() == error_code_backup_auth_unreadable) {
			std::cerr << "ERROR: Could not read authentication information" << std::endl;
		} else if (e.code() == error_code_http_request_failed) {
			// Check if the error is due to a non-existent bucket
			if (e.what() && strstr(e.what(), "NoSuchBucket") != nullptr) {
				std::cerr << "ERROR: Bucket does not exist" << std::endl;
			} else if (e.what() && strstr(e.what(), "NoSuchKey") != nullptr) {
				std::cerr << "ERROR: Resource does not exist in bucket" << std::endl;
				throw resource_not_found();
			} else {
				std::cerr << "ERROR: HTTP request to blobstore failed" << std::endl;
			}
		} else {
			std::cerr << "ERROR: " << e.what() << std::endl;
		}
		throw;
	}
	return Void();
}

ACTOR Future<std::vector<std::string>> listFiles_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                      std::string bucket,
                                                      std::string path) {
	wait(bstore->requestRateRead->getAllowance(1));

	state std::string resource = bstore->constructResourcePath(bucket, path);
	state HTTP::Headers headers;
	state std::string fullResource = resource + "?list-type=2&prefix=" + path;

	Reference<HTTP::IncomingResponse> r =
	    wait(bstore->doRequest("GET", fullResource, headers, nullptr, 0, { 200, 404 }));

	if (r->code == 404) {
		TraceEvent(SevWarn, "S3ClientListFilesNotFound").detail("Bucket", bucket).detail("Path", path);
		throw file_not_found();
	}

	try {
		rapidxml::xml_document<> doc;
		std::string content = r->data.content;
		doc.parse<0>((char*)content.c_str());

		rapidxml::xml_node<>* result = doc.first_node();
		if (result == nullptr || strcmp(result->name(), "ListBucketResult") != 0) {
			TraceEvent(SevWarn, "S3ClientListFilesInvalidResponse")
			    .detail("NodeName", result ? result->name() : "null");
			throw http_bad_response();
		}

		std::vector<std::string> files;
		rapidxml::xml_node<>* n = result->first_node();
		while (n != nullptr) {
			const char* name = n->name();
			if (strcmp(name, "Contents") == 0) {
				rapidxml::xml_node<>* key = n->first_node("Key");
				if (key == nullptr) {
					TraceEvent(SevWarn, "S3ClientListFilesMissingKey").detail("NodeName", name);
					throw http_bad_response();
				}
				std::string file = key->value();
				if (file.size() > path.size() && file.substr(0, path.size()) == path) {
					file = file.substr(path.size());
				}
				files.push_back(file);
			}
			n = n->next_sibling();
		}

		return files;
	} catch (Error& e) {
		TraceEvent(SevWarn, "S3ClientListFilesError").error(e).detail("Bucket", bucket).detail("Path", path);
		throw;
	}
}
