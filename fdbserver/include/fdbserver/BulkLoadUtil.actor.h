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

ACTOR Future<Optional<BulkLoadTaskState>> getBulkLoadTaskStateFromDataMove(Database cx, UID dataMoveId, UID logId);

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

// Extract manifests from job manifest file with the input range
ACTOR Future<std::unordered_map<Key, BulkLoadManifest>> getBulkLoadManifestMetadataFromFiles(
    std::string localJobManifestFilePath,
    KeyRange range,
    std::string manifestLocalTempFolder,
    BulkLoadTransportMethod transportMethod,
    std::string remoteRoot,
    UID logId);

#include "flow/unactorcompiler.h"
#endif
