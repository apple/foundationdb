/*
 * NativeCdcOrderedLifecycle.h
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

#ifndef FDBCLIENT_NATIVECDCORDEREDLIFECYCLE_H
#define FDBCLIENT_NATIVECDCORDEREDLIFECYCLE_H
#pragma once

#include "NativeCdcOrderedMetadata.h"
#include "fdbclient/NativeAPI.h"

struct NativeCdcOrderedSnapshot {
	NativeCdcOrderedMetadata metadata;
	Version minVersion;
};

// The transaction overload neither sets options nor retries. Its caller enables system-key and lock-aware reads.
// An absent group returns none; inconsistent child metadata or unequal child watermarks fails with
// serialization_failed.
Future<Optional<NativeCdcOrderedSnapshot>> readNativeCdcOrderedSnapshot(Transaction* tr, CDCStreamId logicalId);
Future<Optional<NativeCdcOrderedSnapshot>> readNativeCdcOrderedSnapshot(Database cx, CDCStreamId logicalId);

// knownAvailableThrough must be proven across every partition of expectedMetadata. All child watermarks advance in
// one transaction; calling the physical-stream acknowledgement helper for individual children violates this invariant.
Future<Version> acknowledgeNativeCdcOrderedStream(Database cx,
                                                  CDCStreamId logicalId,
                                                  NativeCdcOrderedMetadata expectedMetadata,
                                                  Version consumedThrough,
                                                  Version knownAvailableThrough = invalidVersion);

// False delegates a stream with no ordered metadata to ordinary removal. True also covers a missing/replaced name or
// a completed retry; only an exact logical identity match can remove the group and its children.
Future<bool> removeNativeCdcOrderedStream(Database cx, Key name, CDCStreamId expectedId);

#endif // FDBCLIENT_NATIVECDCORDEREDLIFECYCLE_H
