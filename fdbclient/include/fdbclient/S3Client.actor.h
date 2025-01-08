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
#include "flow/actorcompiler.h" // This must be the last #include.

// FDB S3 Client. Includes copying files and directories to and from s3.
// Uses the S3BlobStoreEndpoint to interact with s3. The s3url is of the form
// expected by S3BlobStoreEndpoint:
//   blobstore://<access_key>:<secret_key>@<endpoint>/resource?bucket=<bucket>, etc.
// See the section 'Backup URls' in the backup documentation,
// https://apple.github.io/foundationdb/backups.html, for more information.
// TODO: Handle prefix as a parameter on the URL so can strip the first part
// of the resource from the blobstore URL.

const std::string BLOBSTORE_PREFIX = "blobstore://";

// Copy the directory content from the local filesystem up to s3.
ACTOR Future<Void> copyUpDirectory(std::string dirpath, std::string s3url);

// Copy filepath to bucket at resource in s3.
ACTOR Future<Void> copyUpFile(std::string filepath, std::string s3url);

// Copy the file from s3 down to the local filesystem.
// Overwrites existing file.
ACTOR Future<Void> copyDownFile(std::string s3url, std::string filepath);

// Copy down the directory content from s3 to the local filesystem.
ACTOR Future<Void> copyDownDirectory(std::string s3url, std::string dirpath);

// Upload the source file set after clearing any existing files at the destination.
ACTOR Future<Void> copyUpBulkDumpFileSet(std::string s3url,
                                         BulkLoadFileSet sourceFileSet,
                                         BulkLoadFileSet destinationFileSet);

// Delete the file or directory at s3url -- recursively.
ACTOR Future<Void> deleteResource(std::string s3url);

#include "flow/unactorcompiler.h"
#endif
