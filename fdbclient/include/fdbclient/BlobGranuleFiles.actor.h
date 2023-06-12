/*
 * BlobGranuleFiles.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(BLOB_GRANULE_FILES_ACTOR_CLIENT_G_H)
#define BLOB_GRANULE_FILES_ACTOR_CLIENT_G_H
#include "fdbclient/BlobGranuleFiles.actor.g.h"
#elif !defined(BLOB_GRANULE_FILES_ACTOR_CLIENT_H)
#define BLOB_GRANULE_FILES_ACTOR_CLIENT_H

// This file contains functions for readers who want to materialize blob granules from the underlying files

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/SystemData.h"
#include "flow/CompressionUtils.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// for performance reasons, the serializeChunked* actors do not copy/take ownership of the data to be serialized and
// rely on the caller to keep it in scope
ACTOR Future<Value> serializeChunkedSnapshot(Standalone<StringRef> fileNameRef,
                                             Standalone<GranuleSnapshot> const* snapshot,
                                             int chunkSize,
                                             Optional<CompressionFilter> compressFilter,
                                             Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
                                             bool isSnapshotSorted);

ACTOR Future<Value> serializeChunkedDeltaFile(Standalone<StringRef> fileNameRef,
                                              Standalone<GranuleDeltas> const* deltas,
                                              KeyRangeRef fileRange,
                                              int chunkSize,
                                              Optional<CompressionFilter> compressFilter,
                                              Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx);

// TODO: add future
RangeResult materializeBlobGranule(const BlobGranuleChunkRef& chunk,
                                   KeyRangeRef keyRange,
                                   Version beginVersion,
                                   Version readVersion,
                                   Optional<StringRef> snapshotData,
                                   const std::vector<StringRef>& deltaFileData,
                                   GranuleMaterializeStats& stats);

std::string randomBGFilename(UID blobWorkerID, UID granuleID, Version version, std::string suffix);

// For benchmark testing only. It should never be called in prod.
ACTOR Future<Void> sortDeltasByKey(Standalone<GranuleDeltas> const* deltasByVersion, KeyRange fileRange);

// just for client passthrough. reads all key-value pairs from a snapshot file, and all mutations from a delta file
RangeResult bgReadSnapshotFile(const StringRef& data,
                               Optional<KeyRef> tenantPrefix,
                               Optional<BlobGranuleCipherKeysCtx> encryptionCtx,
                               const KeyRangeRef& keys = normalKeys);
Standalone<VectorRef<GranuleMutationRef>> bgReadDeltaFile(const StringRef& data,
                                                          Optional<KeyRef> tenantPrefix,
                                                          Optional<BlobGranuleCipherKeysCtx> encryptionCtx);

#endif
