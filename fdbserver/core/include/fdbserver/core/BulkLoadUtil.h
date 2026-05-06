/*
 * BulkLoadUtil.h
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
#pragma once

#include <memory>

#include "fdbclient/BulkLoading.h"

// Erase file folder
void clearFileFolder(const std::string& folderPath, const UID& logId = UID(), bool ignoreError = false);

// Erase and recreate file folder
void resetFileFolder(const std::string& folderPath);

// Asynchronously copy file from one path to another.
Future<Void> copyBulkFile(std::string fromFile, std::string toFile, size_t fileBytesMax);

// Asynchronously read file bytes from local file.
Future<Void> readBulkFileBytes(std::string path, int64_t maxLength, std::shared_ptr<std::string> output);

// Asynchronously write file bytes to local file.
Future<Void> writeBulkFileBytes(std::string path, std::shared_ptr<std::string> content);

// Get the bulkLoadTask metadata of the dataMoveMetadata since the atLeastVersion given the dataMoveId
// This actor is stuck if the actor is failed to read the dataMoveMetadata.
Future<BulkLoadTaskState> getBulkLoadTaskStateFromDataMove(Database cx,
                                                           UID dataMoveId,
                                                           Version atLeastVersion,
                                                           UID logId);

// Look up BulkLoadTaskState directly from bulkLoadTaskKeys by range.
// Does not require DataMoveMetaData or a valid dataMoveId.
Future<BulkLoadTaskState> getBulkLoadTaskStateByRange(Database cx, KeyRange range, Version atLeastVersion, UID logId);

Future<BulkLoadFileSet> bulkLoadDownloadTaskFileSet(BulkLoadTransportMethod transportMethod,
                                                    BulkLoadFileSet fromRemoteFileSet,
                                                    std::string toLocalRoot,
                                                    UID logId);

Future<Void> bulkLoadDownloadTaskFileSets(BulkLoadTransportMethod transportMethod,
                                          std::shared_ptr<BulkLoadFileSetKeyMap> fromRemoteFileSets,
                                          std::shared_ptr<BulkLoadFileSetKeyMap> localFileSets,
                                          std::string toLocalRoot,
                                          UID logId);

Future<bool> doBytesSamplingOnDataFile(std::string dataFileFullPath, std::string byteSampleFileFullPath, UID logId);

// Download job manifest file which is generated when dumping the data
Future<Void> downloadBulkLoadJobManifestFile(BulkLoadTransportMethod transportMethod,
                                             std::string localJobManifestFilePath,
                                             std::string remoteJobManifestFilePath,
                                             UID logId);

// Extract manifest entries from job manifest file with the input range
Future<KeyRange> getBulkLoadJobFileManifestEntryFromJobManifestFile(
    std::string localJobManifestFilePath,
    KeyRange range,
    UID logId,
    std::shared_ptr<BulkLoadManifestFileMap> manifestMap);

// Get BulkLoad manifest metadata from the entry in the job manifest file
Future<BulkLoadManifestSet> getBulkLoadManifestMetadataFromEntry(
    std::vector<BulkLoadJobFileManifestEntry> manifestEntries,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    std::string jobRoot,
    UID logId);
