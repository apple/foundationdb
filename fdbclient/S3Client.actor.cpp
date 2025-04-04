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

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/S3Client.actor.h"
#include "fdbclient/S3TransferManagerWrapper.actor.h"
#include "flow/IAsyncFile.h"
#include "flow/Trace.h"
#include "flow/Traceable.h"
#include "flow/flow.h"
#include "flow/xxhash.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/URI.h"
#include "fdbclient/Knobs.h"
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/Object.h>
#include <aws/core/Aws.h>
#include <aws/core/outcome/Outcome.h>

#include "flow/actorcompiler.h" // has to be last include
//
#define S3_CHECKSUM_TAG_NAME "xxhash64"

typedef XXH64_state_t XXHashState;

namespace {
// Helper function to parse s3://bucket/key URL
bool parseS3BucketAndKey(const std::string& s3url, std::string& bucket, std::string& key) {
	const std::string prefix = "s3://";
	if (s3url.compare(0, prefix.length(), prefix) != 0) {
		TraceEvent(SevError, "ParseS3UrlError")
		    .detail(StringRef("URL"), s3url)
		    .detail(StringRef("Reason"), "Prefix missing");
		return false;
	}
	std::string path = s3url.substr(prefix.length());
	size_t firstSlash = path.find('/');
	if (firstSlash == std::string::npos || firstSlash == 0 || firstSlash == path.length() - 1) {
		TraceEvent(SevError, "ParseS3UrlError")
		    .detail(StringRef("URL"), s3url)
		    .detail(StringRef("Reason"), "Invalid format after prefix");
		return false; // Needs bucket and key
	}
	bucket = path.substr(0, firstSlash);
	key = path.substr(firstSlash + 1);
	// Remove trailing slash from key if present
	if (!key.empty() && key.back() == '/') {
		key.pop_back();
	}
	if (key.empty()) {
		TraceEvent(SevError, "ParseS3UrlError")
		    .detail(StringRef("URL"), s3url)
		    .detail(StringRef("Reason"), "Key part is empty");
		return false;
	}
	return true;
}
} // anonymous namespace

// Define these trace helpers *once* in the global scope
inline Severity s3VerboseEventSev() {
	return !g_network->isSimulated() && CLIENT_KNOBS->S3CLIENT_VERBOSE_LEVEL >= 10 ? SevInfo : SevDebug;
}
inline Severity s3PerfEventSev() {
	return !g_network->isSimulated() && CLIENT_KNOBS->S3CLIENT_VERBOSE_LEVEL >= 5 ? SevInfo : SevDebug;
}

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
	// TODO: Make this settable via knobs.
	int retryDelayMs = 1000;
};

// Calculate hash of a file.
// Uses xxhash library because it's fast (supposedly) and used elsewhere in fdb.
ACTOR static Future<std::string> calculateFileChecksum(Reference<IAsyncFile> file, int64_t size = -1) {
	state int64_t pos = 0;
	state XXH64_state_t* hashState = XXH64_createState();
	state std::vector<uint8_t> buffer(65536);

	XXH64_reset(hashState, 0);

	if (size == -1) {
		int64_t s = wait(file->size());
		size = s;
	}

	while (pos < size) {
		int readSize = std::min<int64_t>(buffer.size(), size - pos);
		int bytesRead = wait(file->read(buffer.data(), readSize, pos));
		XXH64_update(hashState, buffer.data(), bytesRead);
		pos += bytesRead;
	}

	uint64_t hash = XXH64_digest(hashState);
	XXH64_freeState(hashState);
	return format("%016llx", hash);
}

// Get the endpoint for the given s3url.
// Populates parameters and resource with parse of s3url.
Reference<S3BlobStoreEndpoint> getEndpoint(const std::string& s3url,
                                           std::string& resource,
                                           S3BlobStoreEndpoint::ParametersT& parameters) {
	try {
		std::string error;
		Reference<S3BlobStoreEndpoint> endpoint =
		    S3BlobStoreEndpoint::fromString(s3url, {}, &resource, &error, &parameters);

		if (!endpoint) {
			TraceEvent(SevError, "S3ClientGetEndpointNullEndpoint").detail("URL", s3url).detail("Error", error);
			throw backup_invalid_url();
		}

		if (resource.empty()) {
			TraceEvent(SevError, "S3ClientGetEndpointEmptyResource").detail("URL", s3url);
			throw backup_invalid_url();
		}

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
ACTOR static Future<Void> copyUpFile(std::string filepath, std::string s3url) {
	state std::string bucket;
	state std::string key;
	state double startTime = now();

	if (!parseS3BucketAndKey(s3url, bucket, key)) {
		throw backup_invalid_url();
	}

	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpFileStart")
	    .detail(StringRef("Filepath"), filepath)
	    .detail(StringRef("Bucket"), bucket)
	    .detail(StringRef("ObjectKey"), key)
	    .detail(StringRef("S3Url"), s3url);

	try {
		wait(uploadFileWithTransferManager(filepath, bucket, key));

		TraceEvent(s3PerfEventSev(), "S3ClientCopyUpFileEnd")
		    .detail(StringRef("Filepath"), filepath)
		    .detail(StringRef("Bucket"), bucket)
		    .detail(StringRef("ObjectKey"), key)
		    .detail(StringRef("Duration"), now() - startTime);
		return Void();
	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientCopyUpFileError")
		    .detail(StringRef("Filepath"), filepath)
		    .detail(StringRef("Bucket"), bucket)
		    .detail(StringRef("ObjectKey"), key)
		    .error(e);
		throw;
	}
}

// Copy the directory content from the local filesystem up to s3.
ACTOR Future<Void> copyUpDirectory(std::string dirpath, std::string s3url) {
	state std::string baseBucket;
	state std::string baseKeyPrefix;
	state double startTime = now();
	state std::vector<std::string> files;
	state std::vector<Future<Void>> copyFutures;

	// Parse the base S3 URL to get the bucket and the starting key prefix
	// The key from parseS3BucketAndKey will act as the prefix for files in the directory.
	if (!parseS3BucketAndKey(s3url, baseBucket, baseKeyPrefix)) {
		throw backup_invalid_url();
	}
	// Ensure the base key prefix ends with a slash if it's not empty
	if (!baseKeyPrefix.empty() && baseKeyPrefix.back() != '/') {
		baseKeyPrefix += "/";
	}

	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpDirectoryStart")
	    .detail("DirectoryPath", dirpath)
	    .detail("Bucket", baseBucket)
	    .detail("BaseKeyPrefix", baseKeyPrefix)
	    .detail("S3Url", s3url);

	try {
		platform::findFilesRecursively(dirpath, files);
		TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpDirectoryListFiles")
		    .detail("DirectoryPath", dirpath)
		    .detail("FileCount", files.size());

		if (files.empty()) {
			TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpDirectoryEmpty").detail("DirectoryPath", dirpath);
			return Void();
		}

		// Normalize dirpath to ensure it ends with a separator for correct relative path calculation
		std::string normalizedDirpath = dirpath;
		if (!normalizedDirpath.empty() && normalizedDirpath.back() != platform::pathSeparator) {
			normalizedDirpath += platform::pathSeparator;
		}
		int prefixLen = normalizedDirpath.length();

		for (const std::string& fullLocalPath : files) {
			if (fullLocalPath.length() <= prefixLen)
				continue; // Should not happen with findFilesRecursively

			// Calculate relative path and construct target S3 key
			std::string relativePath = fullLocalPath.substr(prefixLen);
			std::string targetKey = baseKeyPrefix + relativePath;
			std::string targetS3Url = "s3://" + baseBucket + "/" + targetKey;

			// Launch copyUpFile actor for each file
			copyFutures.push_back(copyUpFile(fullLocalPath, targetS3Url));
		}

		// Wait for all file copies to complete
		wait(waitForAll(copyFutures));

		TraceEvent(s3PerfEventSev(), "S3ClientCopyUpDirectoryEnd")
		    .detail("DirectoryPath", dirpath)
		    .detail("Bucket", baseBucket)
		    .detail("BaseKeyPrefix", baseKeyPrefix)
		    .detail("FileCount", files.size())
		    .detail("Duration", now() - startTime);
		return Void();
	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientCopyUpDirectoryError")
		    .detail("DirectoryPath", dirpath)
		    .detail("Bucket", baseBucket)
		    .detail("BaseKeyPrefix", baseKeyPrefix)
		    .error(e);
		// Note: This doesn't automatically clean up partially uploaded files on error.
		// A more robust implementation might attempt cleanup or use S3 lifecycle policies.
		throw;
	}
}

// Upload the source file set after clearing any existing files at the destination. (NEW IMPLEMENTATION)
ACTOR Future<Void> copyUpBulkDumpFileSet(std::string s3url,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet) {
	state std::string baseBucket;
	state std::string baseKeyPrefix;
	state double startTime = now();

	// Parse the base S3 URL. Key part is the base prefix for the bulk dump.
	if (!parseS3BucketAndKey(s3url, baseBucket, baseKeyPrefix)) {
		throw backup_invalid_url();
	}
	// Ensure the base key prefix ends with a slash
	if (!baseKeyPrefix.empty() && baseKeyPrefix.back() != '/') {
		baseKeyPrefix += "/";
	}

	// Construct the S3 path for the specific batch directory within the base prefix
	std::string batchRelativePath = destinationFileSet.getRelativePath();
	if (!batchRelativePath.empty() && batchRelativePath.back() != '/') {
		batchRelativePath += "/";
	}
	std::string batchS3KeyPrefix = baseKeyPrefix + batchRelativePath;
	std::string batchS3Url = "s3://" + baseBucket + "/" + batchS3KeyPrefix;

	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpBulkDumpFileSetStart")
	    .detail("BaseS3Url", s3url)
	    .detail("BatchS3Url", batchS3Url)
	    .detail("SourceFileSet", sourceFileSet.toString())
	    .detail("DestinationFileSet", destinationFileSet.toString());

	try {
		// Delete the batch directory if it exists already
		// This uses the refactored deleteResource, which currently has placeholders for async calls.
		TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpBulkDumpFileSetDelete").detail("Target", batchS3Url);
		wait(deleteResource(batchS3Url));

		// Upload Manifest
		std::string destManifestKey = batchS3KeyPrefix + destinationFileSet.getManifestFileName();
		std::string destManifestUrl = "s3://" + baseBucket + "/" + destManifestKey;
		wait(copyUpFile(sourceFileSet.getManifestFileFullPath(), destManifestUrl));

		// Upload Data File (if exists)
		if (sourceFileSet.hasDataFile()) {
			std::string destDataKey = batchS3KeyPrefix + destinationFileSet.getDataFileName();
			std::string destDataUrl = "s3://" + baseBucket + "/" + destDataKey;
			wait(copyUpFile(sourceFileSet.getDataFileFullPath(), destDataUrl));
		}

		// Upload Byte Sample File (if exists)
		if (sourceFileSet.hasByteSampleFile()) {
			ASSERT(sourceFileSet.hasDataFile()); // Should always have data file if byte sample exists
			std::string destByteSampleKey = batchS3KeyPrefix + destinationFileSet.getByteSampleFileName();
			std::string destByteSampleUrl = "s3://" + baseBucket + "/" + destByteSampleKey;
			wait(copyUpFile(sourceFileSet.getBytesSampleFileFullPath(), destByteSampleUrl));
		}

		TraceEvent(s3PerfEventSev(), "S3ClientCopyUpBulkDumpFileSetEnd")
		    .detail("BatchS3Url", batchS3Url)
		    .detail("Duration", now() - startTime);
		return Void();
	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientCopyUpBulkDumpFileSetError")
		    .detail("BaseS3Url", s3url)
		    .detail("BatchS3Url", batchS3Url)
		    .error(e);
		throw;
	}
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
			    .detail("Offset", resultPart.offset);
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
ACTOR static Future<Void> copyDownFile(std::string s3url, std::string filepath) {
	state std::string bucket;
	state std::string key;
	state double startTime = now();

	if (!parseS3BucketAndKey(s3url, bucket, key)) {
		throw backup_invalid_url();
	}

	TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownFileStart")
	    .detail(StringRef("S3Url"), s3url)
	    .detail(StringRef("Bucket"), bucket)
	    .detail(StringRef("ObjectKey"), key)
	    .detail(StringRef("Filepath"), filepath);

	try {
		std::string dir = parentDirectory(filepath);
		if (!dir.empty()) {
			platform::createDirectory(dir);
		}

		wait(downloadFileWithTransferManager(bucket, key, filepath));

		TraceEvent(s3PerfEventSev(), "S3ClientCopyDownFileEnd")
		    .detail(StringRef("S3Url"), s3url)
		    .detail(StringRef("Bucket"), bucket)
		    .detail(StringRef("ObjectKey"), key)
		    .detail(StringRef("Filepath"), filepath)
		    .detail(StringRef("Duration"), now() - startTime);
		return Void();
	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientCopyDownFileError")
		    .detail(StringRef("S3Url"), s3url)
		    .detail(StringRef("Bucket"), bucket)
		    .detail(StringRef("ObjectKey"), key)
		    .detail(StringRef("Filepath"), filepath)
		    .error(e);
		throw;
	}
}

// Copy down the directory content from s3 to the local filesystem. (IMPLEMENTED W/ ASYNC CALLS)
ACTOR Future<Void> copyDownDirectory(std::string s3url, std::string dirpath) {
	state std::string bucket;
	state std::string keyPrefix;
	state double startTime = now();
	state std::vector<Future<Void>> downloadFutures;
	// No need for s3Client state variable here, called within actor
	state Aws::S3::Model::ListObjectsV2Request request;
	state bool isTruncated = true;
	state int fileCount = 0;

	if (!parseS3BucketAndKey(s3url, bucket, keyPrefix)) {
		throw backup_invalid_url();
	}
	if (!keyPrefix.empty() && keyPrefix.back() != '/') {
		keyPrefix += "/";
	}

	TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownDirectoryStart")
	    .detail("DirectoryPath", dirpath)
	    .detail("Bucket", bucket)
	    .detail("KeyPrefix", keyPrefix)
	    .detail("S3Url", s3url);

	try {
		request.SetBucket(Aws::String(bucket.c_str()));
		request.SetPrefix(Aws::String(keyPrefix.c_str()));

		while (isTruncated) {
			// Call the asynchronous ListObjectsV2 actor
			state Aws::S3::Model::ListObjectsV2Outcome outcome = wait(listObjectsV2Actor(request));

			if (!outcome.IsSuccess()) {
				TraceEvent(SevError, "S3ClientCopyDownDirectoryListError")
				    .detail("Bucket", bucket)
				    .detail("KeyPrefix", keyPrefix)
				    .detail("AWSError", outcome.GetError().GetMessage().c_str());
				throw backup_error(); // Convert to FDB error
			}

			const auto& result = outcome.GetResult();
			const auto& objects = result.GetContents();
			TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownDirectoryListResult")
			    .detail("Bucket", bucket)
			    .detail("KeyPrefix", keyPrefix)
			    .detail("Count", objects.size())
			    .detail("IsTruncated", result.GetIsTruncated());

			for (const auto& object : objects) {
				std::string objectKey = object.GetKey().c_str();
				if (objectKey == keyPrefix || object.GetSize() == 0)
					continue;

				if (objectKey.rfind(keyPrefix, 0) != 0) {
					TraceEvent(SevWarn, "S3ClientCopyDownDirectorySkipKey")
					    .detail("Key", objectKey)
					    .detail("Prefix", keyPrefix);
					continue;
				}
				std::string relativePath = objectKey.substr(keyPrefix.length());
				std::replace(relativePath.begin(), relativePath.end(), '/', platform::pathSeparator);
				std::string targetLocalPath = joinPath(dirpath, relativePath);
				std::string objectS3Url = "s3://" + bucket + "/" + objectKey;

				std::string localDir = parentDirectory(targetLocalPath);
				if (!localDir.empty()) {
					platform::createDirectory(localDir);
				}
				downloadFutures.push_back(copyDownFile(objectS3Url, targetLocalPath));
				fileCount++;
			}

			if (result.GetIsTruncated()) {
				request.SetContinuationToken(result.GetNextContinuationToken());
				isTruncated = true;
			} else {
				isTruncated = false;
			}
		}

		wait(waitForAll(downloadFutures));

		TraceEvent(s3PerfEventSev(), "S3ClientCopyDownDirectoryEnd")
		    .detail("DirectoryPath", dirpath)
		    .detail("Bucket", bucket)
		    .detail("KeyPrefix", keyPrefix)
		    .detail("FileCount", fileCount)
		    .detail("Duration", now() - startTime);
		return Void();
	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientCopyDownDirectoryError")
		    .detail("DirectoryPath", dirpath)
		    .detail("Bucket", bucket)
		    .detail("KeyPrefix", keyPrefix)
		    .error(e);
		throw;
	} catch (...) {
		// Catch logic moved inside the actor to handle Outcome exceptions
		TraceEvent(SevError, "S3ClientCopyDownDirectoryError")
		    .detail("DirectoryPath", dirpath)
		    .detail("Bucket", bucket)
		    .detail("KeyPrefix", keyPrefix)
		    .detail("Error", "Unknown exception");
		throw unknown_error();
	}
}

// Delete the file or directory at s3url -- recursively. (IMPLEMENTED W/ ASYNC CALLS)
ACTOR Future<Void> deleteResource(std::string s3url) {
	state std::string bucket;
	state std::string keyOrPrefix;
	state double startTime = now();
	// No s3Client state needed
	state bool isDirectory = false;
	state int deletedCount = 0;

	if (!parseS3BucketAndKey(s3url, bucket, keyOrPrefix)) {
		throw backup_invalid_url();
	}

	if (keyOrPrefix.empty() || keyOrPrefix.back() == '/') {
		isDirectory = true;
	} else {
		isDirectory = false;
	}
	if (isDirectory && !keyOrPrefix.empty() && keyOrPrefix.back() != '/') {
		keyOrPrefix += "/";
	}

	TraceEvent(s3VerboseEventSev(), "S3ClientDeleteResourceStart")
	    .detail("S3Url", s3url)
	    .detail("Bucket", bucket)
	    .detail("KeyOrPrefix", keyOrPrefix)
	    .detail("IsDirectory", isDirectory);

	try {
		if (!isDirectory) {
			// --- Delete Single Object ---
			TraceEvent(s3VerboseEventSev(), "S3ClientDeleteResourceSingle").detail("Key", keyOrPrefix);
			state Aws::S3::Model::DeleteObjectRequest delRequest;
			delRequest.SetBucket(bucket.c_str());
			delRequest.SetKey(keyOrPrefix.c_str());

			state Aws::S3::Model::DeleteObjectOutcome delOutcome = wait(deleteObjectActor(delRequest));

			if (!delOutcome.IsSuccess()) {
				// Log specific S3 error
				TraceEvent(SevError, "S3ClientDeleteResourceSingleError")
				    .detail("Bucket", bucket)
				    .detail("Key", keyOrPrefix)
				    .detail("AWSError", delOutcome.GetError().GetMessage().c_str());
				throw backup_error(); // Convert to FDB error
			}
			deletedCount = 1;
		} else {
			// --- Delete Directory (List + Batch Delete) ---
			state Aws::S3::Model::ListObjectsV2Request listRequest;
			state Aws::Vector<Aws::S3::Model::ObjectIdentifier> objectsToDelete;
			state bool isTruncated = true;

			listRequest.SetBucket(bucket.c_str());
			if (!keyOrPrefix.empty()) {
				listRequest.SetPrefix(keyOrPrefix.c_str());
			}

			while (isTruncated) {
				// Call async ListObjectsV2 actor
				state Aws::S3::Model::ListObjectsV2Outcome listOutcome = wait(listObjectsV2Actor(listRequest));

				if (!listOutcome.IsSuccess()) {
					TraceEvent(SevError, "S3ClientDeleteResourceListError")
					    .detail("Bucket", bucket)
					    .detail("KeyPrefix", keyOrPrefix)
					    .detail("AWSError", listOutcome.GetError().GetMessage().c_str());
					throw backup_error();
				}

				const auto& listResult = listOutcome.GetResult();
				const auto& objects = listResult.GetContents();
				TraceEvent(s3VerboseEventSev(), "S3ClientDeleteResourceListResult")
				    .detail("Bucket", bucket)
				    .detail("KeyPrefix", keyOrPrefix)
				    .detail("Count", objects.size())
				    .detail("IsTruncated", listResult.GetIsTruncated());

				for (const auto& object : objects) {
					Aws::S3::Model::ObjectIdentifier objId;
					objId.SetKey(object.GetKey());
					objectsToDelete.push_back(std::move(objId));

					if (objectsToDelete.size() == 1000) {
						TraceEvent(s3VerboseEventSev(), "S3ClientDeleteResourceBatch")
						    .detail("Count", objectsToDelete.size());
						state Aws::S3::Model::DeleteObjectsRequest delRequest;
						state Aws::S3::Model::Delete delInfo;
						delInfo.SetObjects(objectsToDelete);
						delInfo.SetQuiet(true);
						delRequest.SetBucket(bucket.c_str());
						delRequest.SetDelete(delInfo);

						state Aws::S3::Model::DeleteObjectsOutcome delOutcome = wait(deleteObjectsActor(delRequest));

						// Check outcome and outcome.GetResult().GetDeleted() / .GetErrors()
						if (!delOutcome.IsSuccess()) {
							TraceEvent(SevError, "S3ClientDeleteResourceBatchError")
							    .detail("Bucket", bucket)
							    .detail("KeyPrefix", keyOrPrefix)
							    .detail("AWSError", delOutcome.GetError().GetMessage().c_str());
							// Decide: throw or just log and continue? Throwing is safer.
							throw backup_error();
						}
						// TODO: Check delOutcome.GetResult().GetErrors() for partial failures?
						deletedCount += delOutcome.GetResult().GetDeleted().size();
						objectsToDelete.clear();
					}
				}

				if (listResult.GetIsTruncated()) {
					listRequest.SetContinuationToken(listResult.GetNextContinuationToken());
					isTruncated = true;
				} else {
					isTruncated = false;
				}
			}

			// Delete any remaining objects
			if (!objectsToDelete.empty()) {
				TraceEvent(s3VerboseEventSev(), "S3ClientDeleteResourceBatch").detail("Count", objectsToDelete.size());
				state Aws::S3::Model::DeleteObjectsRequest delRequest;
				state Aws::S3::Model::Delete delInfo;
				delInfo.SetObjects(objectsToDelete);
				delInfo.SetQuiet(true);
				delRequest.SetBucket(bucket.c_str());
				delRequest.SetDelete(delInfo);

				state Aws::S3::Model::DeleteObjectsOutcome delOutcome = wait(deleteObjectsActor(delRequest));
				// Delete any remaining objects (less than 1000)
				if (!objectsToDelete.empty()) {
					// TODO: Implement async wrapper for DeleteObjects
					TraceEvent(s3VerboseEventSev(), "S3ClientDeleteResourceBatch")
					    .detail("Count", objectsToDelete.size());
					// Placeholder (same as above)
					wait(delay(0.0));
					deletedCount += objectsToDelete.size();
					// --- End Placeholder ---
				}
			}

			TraceEvent(s3PerfEventSev(), "S3ClientDeleteResourceEnd")
			    .detail("S3Url", s3url)
			    .detail("Bucket", bucket)
			    .detail("KeyOrPrefix", keyOrPrefix)
			    .detail("IsDirectory", isDirectory)
			    .detail("DeletedCount", deletedCount)
			    .detail("Duration", now() - startTime);
			return Void();
		}
		catch (Error& e) {
			TraceEvent(SevError, "S3ClientDeleteResourceError")
			    .detail("S3Url", s3url)
			    .detail("Bucket", bucket)
			    .detail("KeyOrPrefix", keyOrPrefix)
			    .error(e);
			throw;
		}
		catch (const Aws::Client::AWSError<Aws::S3::S3Errors>& s3Error) {
			TraceEvent(SevError, "S3ClientDeleteResourceError")
			    .detail("S3Url", s3url)
			    .detail("Bucket", bucket)
			    .detail("KeyOrPrefix", keyOrPrefix)
			    .detail("AWSError", s3Error.GetMessage().c_str());
			throw backup_error();
		}
		catch (const std::exception& e) {
			TraceEvent(SevError, "S3ClientDeleteResourceError")
			    .detail("S3Url", s3url)
			    .detail("Bucket", bucket)
			    .detail("KeyOrPrefix", keyOrPrefix)
			    .detail("StdError", e.what());
			throw unknown_error();
		}
	}
