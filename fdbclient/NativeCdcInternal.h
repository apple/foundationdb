/*
 * NativeCdcInternal.h
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

#ifndef FDBCLIENT_NATIVECDCINTERNAL_H
#define FDBCLIENT_NATIVECDCINTERNAL_H
#pragma once

#include <vector>

#include "fdbclient/NativeCdc.h"

struct NativeCdcRemovedStreamInfo {
	Version removalVersion = invalidVersion;
	std::vector<Tag> tags;
};

// Durable metadata operations used by CDC server roles. Registration is
// feature gated; drain and cleanup operations remain available for streams
// persisted before native CDC is disabled.
Future<CDCStreamId> registerNativeCdcStream(Database cx,
                                            Key name,
                                            KeyRange keys,
                                            Optional<UID> proxyId = Optional<UID>());
// Persists per-tag final-pop watermarks before removing stream metadata.
Future<Optional<NativeCdcRemovedStreamInfo>> removeNativeCdcStream(Database cx,
                                                                   Key name,
                                                                   Optional<UID> proxyId = Optional<UID>());
Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreams(Database cx);
// Atomically moves any streams assigned to a failed proxy to its replacement.
Future<Void> reassignNativeCdcStreams(Database cx, UID oldProxyId, UID newProxyId);
// Persists the exclusive unpopped watermark after consuming through a version.
// knownAvailableThrough permits a consumer to acknowledge log data it has
// already received before that version is visible at a transaction read version.
Future<Version> acknowledgeNativeCdcStream(Database cx,
                                           CDCStreamId streamId,
                                           Version consumedThrough,
                                           Version knownAvailableThrough = invalidVersion);

#endif // FDBCLIENT_NATIVECDCINTERNAL_H
