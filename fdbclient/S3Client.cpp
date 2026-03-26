/*
 * S3Client.cpp
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

#include <string>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <sstream>
#include <iomanip>

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/S3Client.h"
#include "flow/IAsyncFile.h"
#include "flow/Trace.h"
#include "flow/Traceable.h"
#include "flow/flow.h"
#include "flow/xxhash.h"
#include "flow/Error.h"
#include "rapidxml/rapidxml.hpp"
#include <openssl/sha.h>
#include "libb64/encode.h"

// Configuration constants
#define S3_CHECKSUM_TAG_NAME "xxhash64"
#define S3_CHECKSUM_FILE_SUFFIX ".checksum"

using XXHashState = XXH64_state_t;

using ::format; // Use FoundationDB's format, not std::format

// State for a part of a multipart upload.
struct PartState {
	int partNumber = 0;
	std::string etag;
	int64_t offset = 0;
	int64_t size = 0;
	std::string checksum; // MD5 or SHA256 depending on integrity check setting
	bool completed = false;
	std::string partData; // Part data kept for sequential XXH64 checksum calculation after upload

	PartState() = default; // Add explicit default constructor

	PartState(int pNum, int64_t off, int64_t sz, std::string checksum = "")
	  : partNumber(pNum), offset(off), size(sz), checksum(checksum) {}
};

// Config for S3 operations with configurable parameters
struct PartConfig {
	// Basic part configuration
	int64_t partSizeBytes = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	int baseRetryDelayMs = CLIENT_KNOBS->BLOBSTORE_MULTIPART_RETRY_DELAY_MS;

	// Retry configuration - now configurable instead of magic numbers
	// TODO: Add these to CLIENT_KNOBS for runtime configuration
	int maxPartRetries = 3; // Default: 3 retries per part
	int maxFileRetries = 3; // Default: 3 retries per file
	int maxRetryDelayMs = 30000; // Default: 30 second cap on retry delay

	// Checksum configuration
	// TODO: Add these to CLIENT_KNOBS for runtime configuration
	bool enableChecksumValidation = true; // Default: enable checksum validation
};

// Calculate hash of a file.
// Uses xxhash library because it's fast (supposedly) and used elsewhere in fdb.
// If size is -1, the function will determine the file size automatically.
// Returns a hex string representation of the xxhash64 checksum.
AsyncResult<std::string> calculateFileChecksum(Reference<IAsyncFile> file, int64_t size) {
	int64_t pos = 0;
	XXH64_state_t* hashState = XXH64_createState();
	std::shared_ptr<std::vector<uint8_t>> buffer = std::make_shared<std::vector<uint8_t>>(65536);
	int readSize{ 0 };

	XXH64_reset(hashState, 0);

	try {
		if (size == -1) {
			int64_t s = co_await file->size();
			size = s;
		}

		while (pos < size) {
			readSize = std::min<int64_t>(buffer->size(), size - pos);
			int bytesRead = co_await uncancellable(holdWhile(buffer, file->read(buffer->data(), readSize, pos)));
			if (bytesRead != readSize) {
				XXH64_freeState(hashState);
				TraceEvent(SevError, "S3ClientCalculateChecksumReadError")
				    .detail("Expected", readSize)
				    .detail("Actual", bytesRead)
				    .detail("Position", pos);
				throw io_error();
			}
			XXH64_update(hashState, buffer->data(), bytesRead);
			pos += bytesRead;
		}

		uint64_t hash = XXH64_digest(hashState);
		XXH64_freeState(hashState);
		co_return format("%016llx", hash);
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
			TraceEvent(SevError, "S3ClientGetEndpointMissingBucket").detail("URL", s3url).detail("Error", error);
			throw backup_invalid_url();
		}

		// Validate resource path characters
		for (char c : resource) {
			if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/') {
				TraceEvent(SevError, "S3ClientGetEndpointIllegalCharacter")
				    .detail("URL", s3url)
				    .detail("Character", std::string(1, c))
				    .detail("Error", error);
				throw backup_invalid_url();
			}
		}

		if (!error.empty()) {
			TraceEvent(SevError, "S3ClientGetEndpointError").detail("URL", s3url).detail("Error", error);
			throw backup_invalid_url();
		}

		return endpoint;

	} catch (Error& e) {
		TraceEvent(SevError, "S3ClientGetEndpointFailed").detail("URL", StringRef(s3url)).detail("Error", e.what());
		throw;
	}
}

// Helper function to determine if an error is retryable
bool isRetryableError(int errorCode) {
	return errorCode == error_code_http_bad_response || errorCode == error_code_connection_failed ||
	       errorCode == error_code_lookup_failed || errorCode == error_code_http_request_failed ||
	       errorCode == error_code_io_error || errorCode == error_code_platform_error;
}

// Write checksum with configurable fallback strategy
static Future<Void> writeChecksumWithFallback(Reference<S3BlobStoreEndpoint> endpoint,
                                              std::string bucket,
                                              std::string objectName,
                                              std::string checksum,
                                              PartConfig config) {
	if (!config.enableChecksumValidation) {
		TraceEvent(SevWarn, "S3ClientChecksumValidationDisabled")
		    .suppressFor(60)
		    .detail("Bucket", bucket)
		    .detail("Object", objectName);
		co_return; // Skip checksum storage if disabled
	}

	// Always try tags first
	try {
		std::map<std::string, std::string> tags;
		tags[S3_CHECKSUM_TAG_NAME] = checksum;
		co_await endpoint->putObjectTags(bucket, objectName, tags);
		TraceEvent(SevDebug, "S3ClientChecksumStoredAsTags")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("Checksum", checksum);
		co_return;
	} catch (Error& e) {
		if (e.code() != error_code_http_bad_response && e.code() != error_code_file_not_found) {
			throw;
		}
		TraceEvent(s3VerboseEventSev(), "S3ClientTaggingFallback")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("Reason", "Tagging not supported, using companion file");
	}

	// Use companion file (either by preference or as fallback)
	co_await endpoint->writeEntireFile(bucket, objectName + S3_CHECKSUM_FILE_SUFFIX, checksum);
	TraceEvent(SevDebug, "S3ClientChecksumStoredAsFile")
	    .detail("Bucket", bucket)
	    .detail("Object", objectName)
	    .detail("ChecksumFile", objectName + S3_CHECKSUM_FILE_SUFFIX)
	    .detail("Checksum", checksum);
}

// Read checksum with configurable fallback strategy
static AsyncResult<Optional<std::string>> readChecksumWithFallback(Reference<S3BlobStoreEndpoint> endpoint,
                                                                   std::string bucket,
                                                                   std::string objectName,
                                                                   PartConfig config) {
	if (!config.enableChecksumValidation) {
		TraceEvent(SevDebug, "S3ClientChecksumValidationDisabled")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName);
		co_return Optional<std::string>(); // Skip checksum validation if disabled
	}

	// Always try tags first
	try {
		std::map<std::string, std::string> tags = co_await endpoint->getObjectTags(bucket, objectName);
		auto it = tags.find(S3_CHECKSUM_TAG_NAME);
		if (it != tags.end() && !it->second.empty()) {
			TraceEvent(SevDebug, "S3ClientChecksumFoundInTags")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("Checksum", it->second);
			co_return Optional<std::string>(it->second);
		}
	} catch (Error& e) {
		if (e.code() != error_code_http_bad_response && e.code() != error_code_file_not_found) {
			throw;
		}
		TraceEvent(s3VerboseEventSev(), "S3ClientTagsNotAvailable")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("FallingBackToFile", "true");
	}

	// Try companion file
	try {
		std::string checksum = co_await endpoint->readEntireFile(bucket, objectName + S3_CHECKSUM_FILE_SUFFIX);
		TraceEvent(SevDebug, "S3ClientChecksumFoundInFile")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("ChecksumFile", objectName + S3_CHECKSUM_FILE_SUFFIX)
		    .detail("Checksum", checksum);
		co_return Optional<std::string>(checksum);
	} catch (Error& e) {
		if (e.code() != error_code_file_not_found) {
			throw;
		}
	}

	TraceEvent(SevDebug, "S3ClientNoChecksumFound")
	    .detail("Bucket", bucket)
	    .detail("Object", objectName)
	    .detail("ChecksumValidationEnabled", config.enableChecksumValidation ? "true" : "false");
	co_return Optional<std::string>();
}

// Upload a part of a multipart upload with configurable retry logic.

static Future<PartState> uploadPart(Reference<S3BlobStoreEndpoint> endpoint,
                                    std::string bucket,
                                    std::string objectName,
                                    std::string uploadID,
                                    Reference<IAsyncFile> file,
                                    PartState part,
                                    PartConfig config) {
	double startTime = now();
	PartState resultPart = part;
	int attempt = 0;
	int maxRetries = config.maxPartRetries;
	int delayMs = config.baseRetryDelayMs;
	UnsentPacketQueue packets;

	TraceEvent(SevDebug, "S3ClientUploadPartStart")
	    .detail("Bucket", StringRef(bucket))
	    .detail("Object", StringRef(objectName))
	    .detail("PartNumber", part.partNumber)
	    .detail("Offset", resultPart.offset)
	    .detail("Size", resultPart.size)
	    .detail("MaxRetries", maxRetries);

	while (true) {
		Error err;
		try {
			// Read part data from file (automatic memory management)
			std::shared_ptr<std::string> partData = std::make_shared<std::string>(resultPart.size, '\0');

			int bytesRead = co_await uncancellable(
			    holdWhile(partData, file->read(&(*partData)[0], resultPart.size, resultPart.offset)));
			if (bytesRead != resultPart.size) {
				TraceEvent(SevError, "S3ClientUploadPartReadError")
				    .detail("Expected", resultPart.size)
				    .detail("Actual", bytesRead)
				    .detail("Offset", resultPart.offset);
				throw io_error();
			}

			// Store part data for sequential XXH64 checksum calculation after concurrent uploads complete
			// to avoid race condition where multiple concurrent uploadPart actors all call
			// XXH64_update(hashState, ...) on the same hash state simultaneously, corrupting it.
			resultPart.partData = std::move(*partData);

			// Calculate hash for this part - use SHA256 if integrity check enabled, otherwise MD5
			std::string checksum;
			if (CLIENT_KNOBS->BLOBSTORE_ENABLE_OBJECT_INTEGRITY_CHECK) {
				// Calculate SHA256 hash - inline implementation to avoid include issues
				unsigned char hash[SHA256_DIGEST_LENGTH];
				SHA256_CTX sha256;
				SHA256_Init(&sha256);
				SHA256_Update(&sha256, resultPart.partData.data(), resultPart.partData.size());
				SHA256_Final(hash, &sha256);
				std::string hashAsStr = std::string((char*)hash, SHA256_DIGEST_LENGTH);
				std::string sig = base64::encoder::from_string(hashAsStr);
				// base64 encoded blocks end in \n so remove last character.
				sig.resize(sig.size() - 1);
				checksum = sig;
			} else {
				// Calculate MD5 hash (original behavior)
				checksum = HTTP::computeMD5Sum(resultPart.partData);
			}

			// Store the checksum (MD5 or SHA256 depending on integrity check setting)
			resultPart.checksum = checksum;

			// Reset the packet queue for each retry attempt
			packets.discardAll();
			PacketWriter pw(packets.getWriteBuffer(resultPart.partData.size()), nullptr, Unversioned());
			pw.serializeBytes(resultPart.partData);

			std::string etag = co_await endpoint->uploadPart(bucket,
			                                                 objectName,
			                                                 uploadID,
			                                                 resultPart.partNumber,
			                                                 &packets,
			                                                 resultPart.partData.size(),
			                                                 resultPart.checksum);

			resultPart.etag = etag;
			resultPart.completed = true;
			TraceEvent(SevDebug, "S3ClientUploadPartEnd")
			    .detail("Bucket", StringRef(bucket))
			    .detail("Object", StringRef(objectName))
			    .detail("PartNumber", part.partNumber)
			    .detail("Offset", resultPart.offset)
			    .detail("Duration", now() - startTime)
			    .detail("Size", resultPart.size)
			    .detail("Attempts", attempt + 1);
			co_return resultPart;
		} catch (Error& e) {
			err = e;
		}

		attempt++;
		if (attempt >= maxRetries || !isRetryableError(err.code())) {
			TraceEvent(SevWarnAlways, "S3ClientUploadPartFailed")
			    .detail("Bucket", StringRef(bucket))
			    .detail("Object", StringRef(objectName))
			    .detail("PartNumber", part.partNumber)
			    .detail("ErrorCode", err.code())
			    .detail("Attempts", attempt)
			    .detail("MaxRetries", maxRetries)
			    .detail("FinalError", err.what());
			throw err;
		}

		TraceEvent(SevDebug, "S3ClientUploadPartRetry")
		    .detail("Bucket", StringRef(bucket))
		    .detail("Object", StringRef(objectName))
		    .detail("PartNumber", part.partNumber)
		    .detail("Attempt", attempt)
		    .detail("Error", err.what())
		    .detail("DelayMs", delayMs);

		co_await delay(delayMs / 1000.0);
		delayMs = std::min(delayMs * 2, config.maxRetryDelayMs);
	}
}

// Copy filepath to bucket at resource in s3.
static Future<Void> copyUpFile(Reference<S3BlobStoreEndpoint> endpoint,
                               std::string bucket,
                               std::string objectName,
                               std::string filepath,
                               PartConfig config = PartConfig()) {
	double startTime = now();
	Reference<IAsyncFile> file;
	std::string uploadID;
	std::vector<PartState> parts;
	int64_t size{ 0 };
	XXH64_state_t* hashState = XXH64_createState();
	int retries = 0;
	int64_t offset{ 0 };
	int partNumber{ 0 };
	int maxConcurrentUploads{ 0 };
	std::vector<Future<PartState>> activeFutures;
	std::vector<int> activePartIndices;
	int numParts{ 0 };
	std::string checksum;

	while (true) {
		Error err;
		try {
			TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpFileStart")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("FilePath", filepath)
			    .detail("Attempt", retries);

			// At the top of the loop, before any use of hashState
			if (!hashState) {
				hashState = XXH64_createState();
			}
			XXH64_reset(hashState, 0);

			Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
			    filepath, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO, 0644);
			file = f;

			int64_t fileSize = co_await file->size();
			size = fileSize;
			// Start multipart upload
			std::string id = co_await endpoint->beginMultiPartUpload(bucket, objectName);
			uploadID = id;

			offset = 0;
			partNumber = 1;
			maxConcurrentUploads = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
			activeFutures.clear();
			activePartIndices.clear();

			// Process parts in batches with concurrency limit
			while (offset < size) {
				// Fill up to maxConcurrentUploads active uploads
				while (activeFutures.size() < maxConcurrentUploads && offset < size) {
					const int64_t currentPartSize = std::min(config.partSizeBytes, size - offset);
					PartState part(partNumber, offset, currentPartSize);
					parts.push_back(part);

					activeFutures.push_back(uploadPart(endpoint, bucket, objectName, uploadID, file, part, config));
					activePartIndices.push_back(partNumber - 1); // Store index into parts array

					offset += currentPartSize;
					partNumber++;
				}

				// Wait for all active uploads to complete
				if (!activeFutures.empty()) {
					std::vector<PartState> completedParts = co_await getAll(activeFutures);
					// Update parts with completion status
					for (int i = 0; i < completedParts.size(); i++) {
						parts[activePartIndices[i]] = completedParts[i];
					}
					// Memory is automatically freed when uploadPart actors complete
					activeFutures.clear();
					activePartIndices.clear();
				}
			}

			// Verify all parts completed and prepare etag map
			std::map<int, S3BlobStoreEndpoint::PartInfo> etagMap;
			for (const auto& part : parts) {
				if (!part.completed) {
					TraceEvent(SevWarnAlways, "S3ClientCopyUpFilePartNotCompleted")
					    .detail("PartNumber", part.partNumber)
					    .detail("Offset", part.offset)
					    .detail("Size", part.size);
					XXH64_freeState(hashState);
					throw http_bad_response();
				}
				etagMap[part.partNumber] = S3BlobStoreEndpoint::PartInfo(part.etag, part.checksum);
			}

			Optional<std::string> s3Checksum =
			    co_await endpoint->finishMultiPartUpload(bucket, objectName, uploadID, etagMap);

			// Log the S3 checksum if present
			if (s3Checksum.present()) {
				TraceEvent(SevDebug, "S3ClientMultipartUploadChecksum")
				    .detail("Bucket", bucket)
				    .detail("Object", objectName)
				    .detail("S3ChecksumSHA256", s3Checksum.get());
			}

			// Calculate XXH64 checksum sequentially after all parts have completed.
			// Parts are sorted by partNumber, ensuring checksum is calculated in correct order.
			TraceEvent(SevDebug, "S3ClientCalculatingFileChecksum")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("NumParts", parts.size());

			for (const auto& part : parts) {
				if (!part.partData.empty()) {
					XXH64_update(hashState, part.partData.data(), part.partData.size());
				}
			}

			// Clear data after successful upload
			numParts = parts.size();
			parts.clear();

			// Finalize checksum
			uint64_t hash = XXH64_digest(hashState);
			XXH64_freeState(hashState);
			checksum = format("%016llx", hash);

			// Only close the file after all uploads are complete
			file = Reference<IAsyncFile>();

			// Store the checksum using configurable fallback strategy
			co_await writeChecksumWithFallback(endpoint, bucket, objectName, checksum, config);

			TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpFileEnd")
			    .detail("Bucket", bucket)
			    .detail("ObjectName", objectName)
			    .detail("FileSize", size)
			    .detail("Parts", numParts)
			    .detail("Checksum", checksum)
			    .detail("Duration", now() - startTime)
			    .detail("Attempts", retries + 1);

			break; // Success - exit retry loop
		} catch (Error& e) {
			err = e;
		}

		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}
		// File-level retry for specific errors, matching download behavior
		if ((err.code() == error_code_file_not_found || err.code() == error_code_http_request_failed ||
		     err.code() == error_code_io_error) &&
		    retries < config.maxFileRetries) { // Use configurable retry limit
			Error retryError = err;
			TraceEvent(SevWarn, "S3ClientCopyUpFileRetry")
			    .errorUnsuppressed(retryError)
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("FilePath", filepath)
			    .detail("Retries", retries);
			retries++;

			// Cleanup before retry
			XXH64_freeState(hashState);
			hashState = nullptr;
			parts.clear();
			activeFutures.clear();
			activePartIndices.clear();

			if (file) {
				file = Reference<IAsyncFile>();
			}

			// Attempt to abort the upload but only if we have a valid uploadID
			if (!uploadID.empty()) {
				try {
					co_await endpoint->abortMultiPartUpload(bucket, objectName, uploadID);
				} catch (Error& abortError) {
					TraceEvent(SevWarn, "S3ClientCopyUpFileAbortError")
					    .error(abortError)
					    .detail("Bucket", bucket)
					    .detail("Object", objectName)
					    .detail("UploadID", uploadID)
					    .detail("OriginalError", retryError.what());
				}
				uploadID = "";
			}

			if (g_network->isSimulated()) {
				co_await delay(0);
				continue;
			}
			co_await delay(1.0 * retries); // Linear backoff like download
		} else {
			XXH64_freeState(hashState);
			hashState = nullptr;
			TraceEvent(SevWarnAlways, "S3ClientCopyUpFileError")
			    .detail("Filepath", filepath)
			    .detail("Bucket", bucket)
			    .detail("ObjectName", objectName)
			    .detail("Error", err.what())
			    .detail("Attempts", retries + 1);

			// Close file before abort attempt
			file = Reference<IAsyncFile>();

			// Attempt to abort the upload but do not wait for it
			if (!uploadID.empty()) {
				try {
					co_await endpoint->abortMultiPartUpload(bucket, objectName, uploadID);
				} catch (Error& abortError) {
					// Log abort failure but throw original error
					TraceEvent(SevWarnAlways, "S3ClientCopyUpFileAbortError")
					    .error(abortError)
					    .detail("Bucket", bucket)
					    .detail("Object", objectName)
					    .detail("OriginalError", err.what());
				}
			}
		}
	}
}

Future<Void> copyUpFile(std::string filepath, std::string s3url) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	co_await copyUpFile(endpoint, parameters["bucket"], resource, filepath);
}

Future<Void> copyUpDirectory(std::string dirpath, std::string s3url) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	std::string bucket = parameters["bucket"];
	std::vector<std::string> files;
	platform::findFilesRecursively(dirpath, files);
	TraceEvent(s3VerboseEventSev(), "S3ClientUploadDirStart")
	    .detail("Filecount", files.size())
	    .detail("Bucket", bucket)
	    .detail("Resource", resource);
	for (const auto& file : files) {
		std::string filepath = file;
		std::string s3path = resource + "/" + file.substr(dirpath.size() + 1);
		co_await copyUpFile(endpoint, bucket, s3path, filepath);
	}
	TraceEvent(s3VerboseEventSev(), "S3ClientUploadDirEnd").detail("Bucket", bucket).detail("Resource", resource);
}

Future<Void> copyUpBulkDumpFileSet(std::string s3url,
                                   BulkLoadFileSet sourceFileSet,
                                   BulkLoadFileSet destinationFileSet) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	std::string bucket = parameters["bucket"];

	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpBulkDumpFileSetStart")
	    .detail("Bucket", bucket)
	    .detail("SourceFileSet", sourceFileSet.toString())
	    .detail("DestinationFileSet", destinationFileSet.toString());
	int pNumDeleted = 0;
	int64_t pBytesDeleted = 0;
	std::string batch_dir = joinPath(getPath(s3url), destinationFileSet.getRelativePath());

	// Delete the batch dir if it exists already (need to check bucket exists else 404 and s3blobstore errors out).
	bool exists = co_await endpoint->bucketExists(bucket);
	if (exists) {
		co_await endpoint->deleteRecursively(bucket, batch_dir, &pNumDeleted, &pBytesDeleted);
	}
	// Destination for manifest file.
	auto destinationManifestPath = joinPath(batch_dir, destinationFileSet.getManifestFileName());
	co_await copyUpFile(endpoint, bucket, destinationManifestPath, sourceFileSet.getManifestFileFullPath());
	if (sourceFileSet.hasDataFile()) {
		auto destinationDataPath = joinPath(batch_dir, destinationFileSet.getDataFileName());
		co_await copyUpFile(endpoint, bucket, destinationDataPath, sourceFileSet.getDataFileFullPath());
	}
	if (sourceFileSet.hasByteSampleFile()) {
		ASSERT(sourceFileSet.hasDataFile());
		auto destinationByteSamplePath = joinPath(batch_dir, destinationFileSet.getByteSampleFileName());
		co_await copyUpFile(endpoint, bucket, destinationByteSamplePath, sourceFileSet.getBytesSampleFileFullPath());
	}
	TraceEvent(s3VerboseEventSev(), "S3ClientCopyUpBulkDumpFileSetEnd")
	    .detail("BatchDir", batch_dir)
	    .detail("NumDeleted", pNumDeleted)
	    .detail("BytesDeleted", pBytesDeleted);
}

static Future<PartState> downloadPart(Reference<S3BlobStoreEndpoint> endpoint,
                                      std::string bucket,
                                      std::string objectName,
                                      Reference<IAsyncFile> file,
                                      PartState part,
                                      PartConfig config) {
	PartState resultPart = part;
	int attempt = 0;
	int maxRetries = config.maxPartRetries;
	int delayMs = config.baseRetryDelayMs;

	TraceEvent(SevDebug, "S3ClientDownloadPartStart")
	    .detail("Bucket", bucket)
	    .detail("Object", objectName)
	    .detail("PartNumber", part.partNumber)
	    .detail("Offset", resultPart.offset)
	    .detail("Size", resultPart.size);

	while (true) {
		Error err;
		try {
			std::vector<uint8_t> buffer;
			int64_t totalBytesRead = 0;
			buffer.resize(resultPart.size);

			// Add range validation
			if (resultPart.offset < 0 || resultPart.size <= 0) {
				TraceEvent(SevError, "S3ClientDownloadPartInvalidRange")
				    .detail("Offset", resultPart.offset)
				    .detail("Size", resultPart.size);
				throw http_bad_response();
			}

			while (totalBytesRead < resultPart.size) {
				int bytesRead = co_await endpoint->readObject(bucket,
				                                              objectName,
				                                              buffer.data() + totalBytesRead,
				                                              resultPart.size - totalBytesRead,
				                                              resultPart.offset + totalBytesRead);
				if (bytesRead == 0) {
					// Avoid infinite loop if server closes connection prematurely
					TraceEvent(SevError, "S3ClientDownloadPartUnexpectedEOF")
					    .detail("Expected", resultPart.size)
					    .detail("Actual", totalBytesRead);
					throw io_error();
				}
				totalBytesRead += bytesRead;
			}

			if (totalBytesRead != resultPart.size) {
				TraceEvent(SevError, "S3ClientDownloadPartSizeMismatch")
				    .detail("Expected", resultPart.size)
				    .detail("Actual", totalBytesRead)
				    .detail("Offset", resultPart.offset)
				    .detail("FilePath", file->getFilename());
				throw io_error();
			}

			// Verify checksum if provided (currently only MD5 is used for download verification)
			if (!resultPart.checksum.empty()) {
				std::string calculatedMD5 = HTTP::computeMD5Sum(std::string((char*)buffer.data(), totalBytesRead));
				if (resultPart.checksum != calculatedMD5) {
					TraceEvent(SevWarnAlways, "S3ClientDownloadPartChecksumMismatch")
					    .detail("Expected", resultPart.checksum)
					    .detail("Calculated", calculatedMD5);
					throw checksum_failed();
				}
			}

			co_await file->write(buffer.data(), totalBytesRead, resultPart.offset);

			resultPart.completed = true;
			TraceEvent(SevDebug, "S3ClientDownloadPartEnd")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("PartNumber", part.partNumber)
			    .detail("Offset", resultPart.offset)
			    .detail("Size", resultPart.size)
			    .detail("Attempts", attempt + 1);
			co_return resultPart;
		} catch (Error& e) {
			err = e;
		}

		attempt++;
		if (attempt >= maxRetries || !isRetryableError(err.code())) {
			TraceEvent(SevWarnAlways, "S3ClientDownloadPartFailed")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("PartNumber", part.partNumber)
			    .detail("ErrorCode", err.code())
			    .detail("Attempts", attempt)
			    .detail("FinalError", err.what());
			throw err;
		}

		TraceEvent(SevInfo, "S3ClientDownloadPartRetry")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("Attempt", attempt)
		    .detail("Error", err.what())
		    .detail("DelayMs", delayMs);

		co_await delay(delayMs / 1000.0);
		delayMs = std::min(delayMs * 2, config.maxRetryDelayMs); // Use configurable cap
	}
}

static AsyncResult<Optional<std::string>> getExpectedChecksum(Reference<S3BlobStoreEndpoint> endpoint,
                                                              std::string bucket,
                                                              std::string objectName) {
	PartConfig config; // Use default configuration
	Optional<std::string> result = co_await readChecksumWithFallback(endpoint, bucket, objectName, config);
	co_return result;
}

// Copy down file from s3 to filepath.
static Future<Void> copyDownFile(Reference<S3BlobStoreEndpoint> endpoint,
                                 std::string bucket,
                                 std::string objectName,
                                 std::string filepath,
                                 PartConfig config = PartConfig()) {
	Reference<IAsyncFile> file;
	std::vector<PartState> parts;
	int64_t fileSize = 0;
	int64_t offset = 0;
	int partNumber = 1;
	int64_t partSize{ 0 };
	std::string expectedChecksum;
	int retries = 0;
	int maxConcurrentDownloads{ 0 };
	std::vector<Future<PartState>> activeDownloadFutures;
	std::vector<int> activePartIndices;

	while (true) {
		Error err;
		try {
			TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownFileStart")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("FilePath", filepath)
			    .detail("Attempt", retries);

			TraceEvent(SevDebug, "S3ClientCopyDownFileBeforeObjectSize")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName);
			int64_t s = co_await endpoint->objectSize(bucket, objectName);
			TraceEvent(SevDebug, "S3ClientCopyDownFileAfterObjectSize")
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("Size", s);
			if (s <= 0) {
				TraceEvent(SevWarnAlways, "S3ClientCopyDownFileEmptyFile")
				    .detail("Bucket", bucket)
				    .detail("Object", objectName);
				throw file_not_found();
			}
			fileSize = s;

			std::string dirPath = filepath.substr(0, filepath.find_last_of("/"));
			if (!dirPath.empty()) {
				platform::createDirectory(dirPath);
			}

			int numParts = (fileSize + config.partSizeBytes - 1) / config.partSizeBytes;
			parts.reserve(numParts);
			Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
			    filepath,
			    IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED |
			        IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_NO_AIO,
			    0644);
			file = f;

			co_await file->truncate(fileSize);
			co_await file->truncate(fileSize);

			offset = 0;
			partNumber = 1;
			maxConcurrentDownloads = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_READS_PER_FILE;
			activeDownloadFutures.clear();
			activePartIndices.clear();

			// Process parts in batches with concurrency limit
			while (offset < fileSize) {
				// Fill up to maxConcurrentDownloads active downloads
				while (activeDownloadFutures.size() < maxConcurrentDownloads && offset < fileSize) {
					partSize = std::min(config.partSizeBytes, fileSize - offset);
					parts.emplace_back(partNumber, offset, partSize, "");
					activeDownloadFutures.push_back(
					    downloadPart(endpoint, bucket, objectName, file, parts.back(), config));
					activePartIndices.push_back(partNumber - 1); // Store index into parts array
					offset += partSize;
					partNumber++;
				}

				// Wait for all active downloads to complete
				if (!activeDownloadFutures.empty()) {
					std::vector<PartState> completedParts = co_await getAll(activeDownloadFutures);
					// Update parts with completion status
					for (int i = 0; i < completedParts.size(); i++) {
						parts[activePartIndices[i]] = completedParts[i];
					}
					// Memory is automatically freed when downloadPart actors complete
					activeDownloadFutures.clear();
					activePartIndices.clear();
				}
			}

			// Verify all parts completed
			for (const auto& part : parts) {
				if (!part.completed) {
					TraceEvent(SevError, "S3ClientCopyDownFilePartNotCompleted").detail("PartNumber", part.partNumber);
					throw http_bad_response();
				}
			}

			co_await file->truncate(fileSize);
			co_await file->sync();

			// Get and verify checksum using the helper
			Optional<std::string> cs = co_await getExpectedChecksum(endpoint, bucket, objectName);

			if (cs.present()) {
				expectedChecksum = cs.get();
				std::string actualChecksum = co_await calculateFileChecksum(file, fileSize);
				if (actualChecksum != expectedChecksum) {
					TraceEvent(SevWarnAlways, "S3ClientCopyDownFileChecksumMismatch")
					    .detail("Expected", expectedChecksum)
					    .detail("Calculated", actualChecksum);
					throw checksum_failed();
				}
			}

			file = Reference<IAsyncFile>(); // Close file

			TraceEvent(s3VerboseEventSev(), "S3ClientCopyDownFileEnd")
			    .detail("Bucket", bucket)
			    .detail("ObjectName", objectName)
			    .detail("FileSize", fileSize)
			    .detail("Checksum", expectedChecksum)
			    .detail("Parts", parts.size());

			break; // Success
		} catch (Error& e) {
			err = e;
		}

		if ((err.code() == error_code_file_not_found || err.code() == error_code_http_request_failed ||
		     err.code() == error_code_io_error) &&
		    retries < config.maxFileRetries) {
			TraceEvent(SevWarn, "S3ClientCopyDownFileRetry")
			    .errorUnsuppressed(err)
			    .detail("Bucket", bucket)
			    .detail("Object", objectName)
			    .detail("FilePath", filepath)
			    .detail("Retries", retries);
			retries++;

			// Cleanup state for retry
			parts.clear();
			activeDownloadFutures.clear();
			activePartIndices.clear();

			if (file) {
				try {
					file = Reference<IAsyncFile>();
					IAsyncFileSystem::filesystem()->deleteFile(filepath, true);
				} catch (Error& cleanupError) {
					TraceEvent(SevWarnAlways, "S3ClientCopyDownFileCleanupError")
					    .detail("FilePath", filepath)
					    .errorUnsuppressed(cleanupError);
				}
			}
			if (g_network->isSimulated()) {
				co_await delay(0);
				continue;
			}
			co_await delay(1.0 * retries);
		} else {
			TraceEvent(SevWarnAlways, "S3ClientCopyDownFileError")
			    .detail("Bucket", bucket)
			    .detail("ObjectName", objectName)
			    .errorUnsuppressed(err)
			    .detail("FilePath", filepath)
			    .detail("FileSize", fileSize);

			if (file) {
				try {
					co_await file->sync();
					file = Reference<IAsyncFile>();
					IAsyncFileSystem::filesystem()->deleteFile(filepath, false);
				} catch (Error& e2) {
					TraceEvent(SevWarnAlways, "S3ClientCopyDownFileCleanupError")
					    .detail("FilePath", filepath)
					    .errorUnsuppressed(e2);
				}
			}
			throw err;
		}
	}
}

Future<Void> copyDownFile(std::string s3url, std::string filepath) {
	TraceEvent(SevDebug, "S3ClientCopyDownFileWrapperStart").detail("S3URL", s3url).detail("FilePath", filepath);
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	TraceEvent(SevDebug, "S3ClientCopyDownFileBeforeGetEndpoint").detail("S3URL", s3url);
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	TraceEvent(SevDebug, "S3ClientCopyDownFileAfterGetEndpoint")
	    .detail("S3URL", s3url)
	    .detail("Resource", resource)
	    .detail("Bucket", parameters["bucket"]);
	co_await copyDownFile(endpoint, parameters["bucket"], resource, filepath);
	TraceEvent(SevDebug, "S3ClientCopyDownFileWrapperEnd").detail("S3URL", s3url).detail("FilePath", filepath);
}

Future<Void> copyDownDirectory(std::string s3url, std::string dirpath) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	std::string bucket = parameters["bucket"];
	S3BlobStoreEndpoint::ListResult items = co_await endpoint->listObjects(bucket, resource);
	std::vector<S3BlobStoreEndpoint::ObjectInfo> objects = items.objects;
	TraceEvent(s3VerboseEventSev(), "S3ClientDownDirectoryStart")
	    .detail("Filecount", objects.size())
	    .detail("Bucket", bucket)
	    .detail("Resource", resource);
	for (const auto& object : objects) {
		std::string filepath = dirpath + "/" + object.name.substr(resource.size());
		std::string s3path = object.name;
		co_await copyDownFile(endpoint, bucket, s3path, filepath);
	}
	TraceEvent(s3VerboseEventSev(), "S3ClientDownDirectoryEnd").detail("Bucket", bucket).detail("Resource", resource);
}

Future<Void> deleteResource(std::string s3url) {
	std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	std::string bucket = parameters["bucket"];
	co_await endpoint->deleteRecursively(bucket, resource);
}

Future<Void> listFiles(std::string s3url, int maxDepth) {
	try {
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT parameters;
		Reference<S3BlobStoreEndpoint> bstore = getEndpoint(s3url, resource, parameters);

		if (!bstore) {
			TraceEvent(SevError, "S3ClientListingFailed").detail("Error", error);
			throw backup_invalid_url();
		}

		// Get bucket directly from parameters
		std::string bucket = parameters["bucket"];

		// Check if bucket exists first
		bool exists = co_await bstore->bucketExists(bucket);
		if (!exists) {
			std::cerr << "ERROR: Bucket '" << bucket << "' does not exist" << std::endl;
			throw http_request_failed();
		}

		// Let S3BlobStoreEndpoint handle the resource path construction
		Optional<char> delimiter;
		if (maxDepth <= 1) {
			delimiter = Optional<char>('/');
		}

		// Use listObjects with the resource path directly, letting S3BlobStoreEndpoint handle URL construction
		S3BlobStoreEndpoint::ListResult result = co_await bstore->listObjects(bucket, resource, delimiter, maxDepth);

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
}

AsyncResult<std::vector<std::string>> listFiles_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                     std::string bucket,
                                                     std::string path) {
	co_await bstore->requestRateRead->getAllowance(1);

	std::string resource = bstore->constructResourcePath(bucket, path);
	HTTP::Headers headers;
	std::string fullResource = resource + "?list-type=2&prefix=" + path;

	Reference<HTTP::IncomingResponse> r =
	    co_await bstore->doRequest("GET", fullResource, headers, nullptr, 0, { 200, 404 });

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

		co_return files;
	} catch (Error& e) {
		TraceEvent(SevWarn, "S3ClientListFilesError").error(e).detail("Bucket", bucket).detail("Path", path);
		throw;
	}
}
