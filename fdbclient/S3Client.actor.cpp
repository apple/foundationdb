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

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <fcntl.h>
#include <filesystem>

#ifdef _WIN32
#include <io.h>
#endif

#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BackupTLSConfig.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/IKnobCollection.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/versions.h"
#include "fdbclient/S3Client.actor.h"
#include "flow/Platform.h"
#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include <boost/url/url.hpp>
#include <boost/url/parse.hpp>
#include "flow/TLSConfig.actor.h"

#include "flow/actorcompiler.h" // has to be last include

// Get the endpoint for the given s3url.
// Populates parameters and resource with parse of s3url.
Reference<S3BlobStoreEndpoint> getEndpoint(const std::string& s3url,
                                           std::string& resource,
                                           S3BlobStoreEndpoint::ParametersT& parameters) {
	std::string error;
	Reference<S3BlobStoreEndpoint> endpoint =
	    S3BlobStoreEndpoint::fromString(s3url, {}, &resource, &error, &parameters);
	if (resource.empty()) {
		TraceEvent(SevError, "S3ClientEmptyResource").detail("s3url", s3url);
		throw backup_invalid_url();
	}
	for (auto c : resource) {
		if (!isalnum(c) && c != '_' && c != '-' && c != '.' && c != '/') {
			TraceEvent(SevError, "S3ClientIllegalCharacter").detail("s3url", s3url);
			throw backup_invalid_url();
		}
	}
	if (error.size()) {
		TraceEvent(SevError, "S3ClientGetEndpointError").detail("s3url", s3url).detail("error", error);
		throw backup_invalid_url();
	}
	return endpoint;
}

// Copy filepath to bucket at resource in s3.
ACTOR static Future<Void> copyUpFile(Reference<S3BlobStoreEndpoint> endpoint,
                                     std::string bucket,
                                     std::string resource,
                                     std::string filepath) {
	// Reading an SST file fully into memory is pretty obnoxious. They are about 16MB on
	// average. Streaming would require changing this s3blobstore interface.
	// Make 32MB the max size for now even though its arbitrary and way to big.
	state std::string content = readFileBytes(filepath, 1024 * 1024 * 32);
	TraceEvent("S3ClientUploadStart")
	    .detail("filepath", filepath)
	    .detail("bucket", bucket)
	    .detail("resource", resource)
	    .detail("size", content.size());
	wait(endpoint->writeEntireFile(bucket, resource, content));
	TraceEvent("S3ClientUpload")
	    .detail("filepath", filepath)
	    .detail("bucket", bucket)
	    .detail("resource", resource)
	    .detail("size", content.size());
	return Void();
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
	    .detail("filecount", files.size())
	    .detail("bucket", bucket)
	    .detail("resource", resource);
	for (const auto& file : files) {
		std::string filepath = file;
		std::string s3path = resource + "/" + file.substr(dirpath.size() + 1);
		wait(copyUpFile(endpoint, bucket, s3path, filepath));
	}
	TraceEvent("S3ClientUploadDirEnd").detail("bucket", bucket).detail("resource", resource);
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
	    .detail("bucket", bucket)
	    .detail("sourceFileSet", sourceFileSet.toString())
	    .detail("destinationFileSet", destinationFileSet.toString());
	state int pNumDeleted = 0;
	state int64_t pBytesDeleted = 0;
	// Throws error if s3url is invalid.
	boost::urls::url url = boost::urls::parse_uri(s3url).value();
	// Get path to the batch dir.
	state std::string batch_dir = joinPath(url.path(), destinationFileSet.getRelativePath());
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

// Copy down file from s3 to filepath.
ACTOR static Future<Void> copyDownFile(Reference<S3BlobStoreEndpoint> endpoint,
                                       std::string bucket,
                                       std::string resource,
                                       std::string filepath) {
	std::string content = wait(endpoint->readEntireFile(bucket, resource));
	auto parent = std::filesystem::path(filepath).parent_path();
	if (parent != "" && !std::filesystem::exists(parent)) {
		std::filesystem::create_directories(parent);
	}
	writeFile(filepath, content);
	TraceEvent("S3ClientDownload")
	    .detail("filepath", filepath)
	    .detail("bucket", bucket)
	    .detail("resource", resource)
	    .detail("size", content.size());
	return Void();
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
	TraceEvent("S3ClientDownloadDirStart")
	    .detail("filecount", objects.size())
	    .detail("bucket", bucket)
	    .detail("resource", resource);
	for (const auto& object : objects) {
		std::string filepath = dirpath + "/" + object.name.substr(resource.size());
		std::string s3path = object.name;
		wait(copyDownFile(endpoint, bucket, s3path, filepath));
	}
	TraceEvent("S3ClientDownloadDirEnd").detail("bucket", bucket).detail("resource", resource);
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