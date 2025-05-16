/*
 * S3Client.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_S3CLIENT_ACTOR_G_H)
#define FDBCLIENT_S3CLIENT_ACTOR_G_H
#include "fdbclient/S3Client.actor.g.h"
#elif !defined(FDBCLIENT_S3CLIENT_ACTOR_H)
#define FDBCLIENT_S3CLIENT_ACTOR_H

#include <string>
#include "fdbclient/S3BlobStore.h"
#include "fdbclient/BulkDumping.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// FDB S3 Client. Includes copying files and directories to and from s3.
// Uses the S3BlobStoreEndpoint to interact with s3. The s3url is of the form
// expected by S3BlobStoreEndpoint:
//   blobstore://<access_key>:<secret_key>@<endpoint>/resource?bucket=<bucket>, etc.
// See the section 'Backup URls' in the backup documentation,
// https://apple.github.io/foundationdb/backups.html, for more information.
//
// Features:
// - Multipart uploads and downloads for large files
// - Automatic retries with configurable delays
// - Checksum verification using xxhash64
// - Configurable part sizes for multipart operations
// - Recursive directory operations
//
// Error Handling:
// - Detailed error reporting with trace events
// - Automatic cleanup on failure
// - Checksum verification for data integrity
//
// Performance:
// - Configurable verbosity levels for trace events
// - UNCACHED file operations for large files
// - Parallel part uploads/downloads
//
// TODO: Handle prefix as a parameter on the URL so can strip the first part
// of the resource from the blobstore URL.

// For all trace events for s3 client operations
// Returns SevInfo if S3CLIENT_VERBOSE_LEVEL >= 10 in non-simulated environment,
// otherwise returns SevDebug.
inline Severity s3VerboseEventSev() {
	return !g_network->isSimulated() && CLIENT_KNOBS->S3CLIENT_VERBOSE_LEVEL >= 10 ? SevInfo : SevDebug;
}

// For all trace events measuring the performance of s3 client operations
// Returns SevInfo if S3CLIENT_VERBOSE_LEVEL >= 5 in non-simulated environment,
// otherwise returns SevDebug.
inline Severity s3PerfEventSev() {
	return !g_network->isSimulated() && CLIENT_KNOBS->S3CLIENT_VERBOSE_LEVEL >= 5 ? SevInfo : SevDebug;
}

// Prefix used to identify blobstore URLs
const std::string BLOBSTORE_PREFIX = "blobstore://";

// Copy the directory content from the local filesystem up to s3.
// Recursively copies all files and subdirectories.
// dirpath: Local directory path to copy from
// s3url: S3 URL to copy to (must include bucket parameter)
// Returns a Future that completes when the operation is done
ACTOR Future<Void> copyUpDirectory(std::string dirpath, std::string s3url);

// Copy filepath to bucket at resource in s3.
// Uses multipart upload for large files.
// filepath: Local file path to copy from
// s3url: S3 URL to copy to (must include bucket parameter)
// Returns a Future that completes when the operation is done
ACTOR Future<Void> copyUpFile(std::string filepath, std::string s3url);

// Copy the file from s3 down to the local filesystem.
// Overwrites existing file. Uses multipart download for large files.
// s3url: S3 URL to copy from (must include bucket parameter)
// filepath: Local file path to copy to
// Returns a Future that completes when the operation is done
ACTOR Future<Void> copyDownFile(std::string s3url, std::string filepath);

// Copy down the directory content from s3 to the local filesystem.
// Recursively copies all files and subdirectories.
// s3url: S3 URL to copy from (must include bucket parameter)
// dirpath: Local directory path to copy to
// Returns a Future that completes when the operation is done
ACTOR Future<Void> copyDownDirectory(std::string s3url, std::string dirpath);

// Upload the source file set after clearing any existing files at the destination.
// Used for bulk operations like backup and restore.
// s3url: S3 URL to copy to (must include bucket parameter)
// sourceFileSet: Source file set to copy
// destinationFileSet: Destination file set to create
// Returns a Future that completes when the operation is done
ACTOR Future<Void> copyUpBulkDumpFileSet(std::string s3url,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet);

// Delete the file or directory at s3url -- recursively.
// s3url: S3 URL to delete (must include bucket parameter)
// Returns a Future that completes when the operation is done
ACTOR Future<Void> deleteResource(std::string s3url);

// Calculate the xxhash64 checksum of a file.
// Used for verifying data integrity during uploads and downloads.
// file: File handle to calculate checksum for
// size: Size of the file in bytes, or -1 to determine automatically
// Returns a Future that completes with the hex string representation of the checksum
ACTOR Future<std::string> calculateFileChecksum(Reference<IAsyncFile> file, int64_t size = -1);

// List files and directories at the given S3 URL
// s3url: S3 URL to list (must include bucket parameter)
// maxDepth: Maximum depth to recurse (default: 1)
// Returns a Future that completes when the operation is done
ACTOR Future<Void> listFiles(std::string s3url, int maxDepth = 1);

// Get the endpoint for the given s3url.
// Populates parameters and resource with parse of s3url.
// s3url: S3 URL to parse
// resource: Output parameter for the resource path
// parameters: Output parameter for the URL parameters
// Returns a Reference to the S3BlobStoreEndpoint
Reference<S3BlobStoreEndpoint> getEndpoint(const std::string& s3url,
                                           std::string& resource,
                                           S3BlobStoreEndpoint::ParametersT& parameters);

#include "flow/unactorcompiler.h"
#endif
