/*
 * BlobGranuleReader.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(BLOB_GRANULE_READER_CLIENT_G_H)
#define BLOB_GRANULE_READER_CLIENT_G_H
#include "fdbclient/BlobGranuleReader.actor.g.h"
#elif !defined(BLOB_GRANULE_READER_CLIENT_H)
#define BLOB_GRANULE_READER_CLIENT_H

#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobGranuleFiles.h"
#include "fdbclient/BlobWorkerInterface.h"
#include "fdbclient/BlobWorkerCommon.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// Reads the fileset in the reply using the provided blob store, and filters data and mutations by key + version from
// the request
ACTOR Future<RangeResult> readBlobGranule(BlobGranuleChunkRef chunk,
                                          KeyRangeRef keyRange,
                                          Version beginVersion,
                                          Version readVersion,
                                          Reference<BackupContainerFileSystem> bstore,
                                          Optional<BlobWorkerStats*> stats = Optional<BlobWorkerStats*>());

ACTOR Future<Void> readBlobGranules(BlobGranuleFileRequest request,
                                    BlobGranuleFileReply reply,
                                    Reference<BackupContainerFileSystem> bstore,
                                    PromiseStream<RangeResult> results);

#include "flow/unactorcompiler.h"
#endif
