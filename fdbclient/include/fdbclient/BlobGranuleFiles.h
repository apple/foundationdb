/*
 * BlobGranuleFiles.h
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

#ifndef FDBCLIENT_BLOBGRANULEFILES_H
#define FDBCLIENT_BLOBGRANULEFILES_H

// This file contains functions for readers who want to materialize blob granules from the underlying files

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/SystemData.h"
#include "flow/CompressionUtils.h"

Value serializeChunkedSnapshot(const Standalone<StringRef>& fileNameRef,
                               const Standalone<GranuleSnapshot>& snapshot,
                               int chunkSize,
                               Optional<CompressionFilter> compressFilter,
                               Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx = {},
                               bool isSnapshotSorted = true);

Value serializeChunkedDeltaFile(const Standalone<StringRef>& fileNameRef,
                                const Standalone<GranuleDeltas>& deltas,
                                const KeyRangeRef& fileRange,
                                int chunkSize,
                                Optional<CompressionFilter> compressFilter,
                                Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx = {});

ErrorOr<RangeResult> loadAndMaterializeBlobGranules(const Standalone<VectorRef<BlobGranuleChunkRef>>& files,
                                                    const KeyRangeRef& keyRange,
                                                    Version beginVersion,
                                                    Version readVersion,
                                                    ReadBlobGranuleContext granuleContext,
                                                    GranuleMaterializeStats& stats);

RangeResult materializeBlobGranule(const BlobGranuleChunkRef& chunk,
                                   KeyRangeRef keyRange,
                                   Version beginVersion,
                                   Version readVersion,
                                   Optional<StringRef> snapshotData,
                                   const std::vector<StringRef>& deltaFileData,
                                   GranuleMaterializeStats& stats);

std::string randomBGFilename(UID blobWorkerID, UID granuleID, Version version, std::string suffix);

// For benchmark testing only. It should never be called in prod.
void sortDeltasByKey(const Standalone<GranuleDeltas>& deltasByVersion, const KeyRangeRef& fileRange);

// just for client passthrough. reads all key-value pairs from a snapshot file, and all mutations from a delta file
RangeResult bgReadSnapshotFile(const StringRef& data, const KeyRangeRef& keys = normalKeys);
Standalone<VectorRef<GranuleMutationRef>> bgReadDeltaFile(const StringRef& data);

#endif
