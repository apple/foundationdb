/*
 * BulkLoadUtil.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULKLOADUTIL_ACTOR_G_H)
#define FDBSERVER_BULKLOADUTIL_ACTOR_G_H
#include "fdbserver/BulkLoadUtil.actor.g.h"
#elif !defined(FDBSERVER_BULKLOADUTIL_ACTOR_H)
#define FDBSERVER_BULKLOADUTIL_ACTOR_H
#pragma once

#include "fdbclient/BulkLoading.h"
#include "flow/actorcompiler.h" // has to be last include

// Erase file folder
void clearFileFolder(const std::string& folderPath, const UID& logId = UID(), bool ignoreError = false);

// Erase and recreate file folder
void resetFileFolder(const std::string& folderPath);

// Asynchronously copy file from one path to another.
ACTOR Future<Void> copyBulkFile(std::string fromFile, std::string toFile, size_t fileBytesMax);

// Asynchronously read file bytes from local file.
ACTOR Future<std::string> readBulkFileBytes(std::string path, int64_t maxLength);

// Asynchronously write file bytes to local file.
ACTOR Future<Void> writeBulkFileBytes(std::string path, StringRef content);

// Get the bulkLoadTask metadata of the dataMoveMetadata since the atLeastVersion given the dataMoveId
// This actor is stuck if the actor is failed to read the dataMoveMetadata.
ACTOR Future<BulkLoadTaskState> getBulkLoadTaskStateFromDataMove(Database cx,
                                                                 UID dataMoveId,
                                                                 Version atLeastVersion,
                                                                 UID logId);

ACTOR Future<BulkLoadFileSet> bulkLoadDownloadTaskFileSet(BulkLoadTransportMethod transportMethod,
                                                          BulkLoadFileSet fromRemoteFileSet,
                                                          std::string toLocalRoot,
                                                          UID logId);

ACTOR Future<bool> doBytesSamplingOnDataFile(std::string dataFileFullPath,
                                             std::string byteSampleFileFullPath,
                                             UID logId);

// Download job manifest file which is generated when dumping the data
ACTOR Future<Void> downloadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                                   std::string localJobManifestFilePath,
                                                   std::string remoteJobManifestFilePath,
                                                   UID logId);

// Extract manifest entries from job manifest file with the input range
ACTOR Future<std::unordered_map<Key, BulkLoadJobFileManifestEntry>>
getBulkLoadJobFileManifestEntryFromJobManifestFile(std::string localJobManifestFilePath, KeyRange range, UID logId);

// Get BulkLoad manifest metadata from the entry in the job manifest file
ACTOR Future<BulkLoadManifest> getBulkLoadManifestMetadataFromEntry(BulkLoadJobFileManifestEntry manifestEntry,
                                                                    std::string manifestLocalTempFolder,
                                                                    BulkLoadTransportMethod transportMethod,
                                                                    std::string jobRoot,
                                                                    UID logId);

#include "flow/unactorcompiler.h"
#endif
