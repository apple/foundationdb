/*
 * BulkSstFiles.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/core/BulkDumpUtil.h"

// Generate key-value data, byte sampling data, and manifest file.
// Return BulkLoadManifest metadata (equivalent to content of the manifest file).
// TODO(BulkDump): can cause slow tasks, do the task in a separate thread in the future.
// The size of sortedData is defined at the place of generating the data (getRangeDataToDump).
// The size is configured by MOVE_SHARD_KRM_ROW_LIMIT.
Future<BulkLoadManifest> dumpDataFileToLocalDirectory(UID logId,
                                                      std::shared_ptr<RangeDumpRawData> rangeDumpRawData,
                                                      BulkLoadFileSet localFileSet,
                                                      BulkLoadFileSet remoteFileSet,
                                                      BulkLoadByteSampleSetting byteSampleSetting,
                                                      Version dumpVersion,
                                                      KeyRange dumpRange,
                                                      BulkLoadType dumpType,
                                                      BulkLoadTransportMethod transportMethod);

Future<bool> doBytesSamplingOnDataFile(std::string dataFileFullPath, std::string byteSampleFileFullPath, UID logId);
