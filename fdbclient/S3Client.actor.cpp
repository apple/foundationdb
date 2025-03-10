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

#include "fdbclient/ClientKnobs.h"
#include "fdbclient/Knobs.h"
#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BackupTLSConfig.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/S3Client.actor.h"
#include "fdbclient/BulkLoading.h"
#include "flow/Platform.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include <boost/url/url.hpp>
#include <boost/url/parse.hpp>
#include "flow/TLSConfig.actor.h"
#include "flow/IAsyncFile.h"

#include "flow/actorcompiler.h" // has to be last include

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
		TraceEvent(SevError, "S3ClientGetEndpointFailed").error(e).detail("URL", s3url);
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
	TraceEvent("S3ClientUploadPartStart")
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
		TraceEvent("S3ClientUploadPartEnd")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("Offset", resultPart.offset)
		    .detail("Duration", now() - startTime)
		    .detail("Size", resultPart.size);
		return resultPart;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "S3ClientUploadPartError")
		    .error(e)
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber);
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
		TraceEvent("S3ClientCopyUpFileStart")
		    .detail("Filepath", filepath)
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("FileSize", size);

		// Start multipart upload
		std::string id = wait(endpoint->beginMultiPartUpload(bucket, objectName));
		uploadID = id;

		Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
		    filepath, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0666));
		file = f;

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
				    .detail("expected", partSize)
				    .detail("actual", bytesRead);
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
				TraceEvent(SevWarnAlways, "S3ClientCopyUpFilePartNotCompleted").detail("partNumber", part.partNumber);
				throw operation_failed();
			}
			etagMap[part.partNumber] = part.etag;
		}

		wait(endpoint->finishMultiPartUpload(bucket, objectName, uploadID, etagMap));
		// TODO(BulkLoad): Return map of part numbers to md5 or other checksumming so we can save
		// aside and check integrity of downloaded file

		// Clear data after successful upload
		parts.clear();
		partDatas.clear();

		TraceEvent("S3ClientCopyUpFileEnd")
		    .detail("Bucket", bucket)
		    .detail("ObjectName", objectName)
		    .detail("FileSize", size)
		    .detail("Parts", parts.size())
		    .detail("Duration", now() - startTime);

		return Void();
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "S3ClientCopyUpFileError")
		    .error(e)
		    .detail("Bucket", bucket)
		    .detail("Object", objectName);
		state Error err = e;

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
	TraceEvent("S3ClientUploadDirStart")
	    .detail("Filecount", files.size())
	    .detail("Bucket", bucket)
	    .detail("Resource", resource);
	for (const auto& file : files) {
		std::string filepath = file;
		std::string s3path = resource + "/" + file.substr(dirpath.size() + 1);
		wait(copyUpFile(endpoint, bucket, s3path, filepath));
	}
	TraceEvent("S3ClientUploadDirEnd").detail("Bucket", bucket).detail("Resource", resource);
	return Void();
}

ACTOR Future<Void> copyUpBulkDumpFileSet(std::string s3url,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet) {
	state std::string resource;
	S3BlobStoreEndpoint::ParametersT parameters;
	state Reference<S3BlobStoreEndpoint> endpoint = getEndpoint(s3url, resource, parameters);
	state std::string bucket = parameters["bucket"];
	TraceEvent("S3ClientCopyUpBulkDumpFileSetStart")
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
	TraceEvent("S3ClientCopyUpBulkDumpFileSetEnd")
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
		TraceEvent("S3ClientDownloadPartStart")
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
		TraceEvent("S3ClientDownloadPartEnd")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("PartNumber", part.partNumber)
		    .detail("Offset", resultPart.offset)
		    .detail("Size", resultPart.size);
		return resultPart;
	} catch (Error& e) {
		TraceEvent(SevWarnAlways, "S3ClientDownloadPartError")
		    .error(e)
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
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
	state Optional<std::string> md5;

	try {
		TraceEvent("S3ClientCopyDownFileStart")
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
		Reference<IAsyncFile> f = wait(
		    IAsyncFileSystem::filesystem()->open(filepath, IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0644));
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

		// Close file properly
		file = Reference<IAsyncFile>();

		// TODO(Bulkload): Check integrity of downloaded file.
		TraceEvent("S3ClientCopyDownFileEnd")
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("FileSize", fileSize)
		    .detail("Duration", now() - startTime)
		    .detail("Parts", parts.size());

		return Void();
	} catch (Error& e) {
		state Error err = e;
		TraceEvent(SevWarnAlways, "S3ClientCopyDownFileError")
		    .error(e)
		    .detail("Bucket", bucket)
		    .detail("Object", objectName)
		    .detail("FilePath", filepath);

		// Clean up the file in case of error
		if (file) {
			try {
				wait(file->sync());
				file = Reference<IAsyncFile>();
				IAsyncFileSystem::filesystem()->deleteFile(filepath, false);
			} catch (Error& e) {
				TraceEvent(SevWarnAlways, "S3ClientCopyDownFileCleanupError").error(e).detail("FilePath", filepath);
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
	TraceEvent("S3ClientDownDirecotryStart")
	    .detail("Filecount", objects.size())
	    .detail("Bucket", bucket)
	    .detail("Resource", resource);
	for (const auto& object : objects) {
		std::string filepath = dirpath + "/" + object.name.substr(resource.size());
		std::string s3path = object.name;
		wait(copyDownFile(endpoint, bucket, s3path, filepath));
	}
	TraceEvent("S3ClientDownDirectoryEnd").detail("Bucket", bucket).detail("Resource", resource);
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
