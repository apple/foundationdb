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

#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"

// A durable snapshot used to fence a sampled balancing decision.
struct NativeCdcTagState {
	CDCStreamId streamId = 0;
	KeyRange keys;
	Key historyKey;
	CDCTagHistoryEntry assignment;
	UID proxyId;
	Version minVersion = invalidVersion;
	bool pending = false;
};

// An absent result means the bounded snapshot is incomplete; it must not be used
// as an empty or zero-load configuration.
Future<Optional<NativeCdcTagState>> readNativeCdcTagState(Transaction* tr, CDCStreamId streamId);
Future<Optional<std::vector<NativeCdcTagState>>> readNativeCdcTagStates(Transaction* tr, int maxStreams);
// These helpers revalidate the durable identity and prepare mutations without
// committing. The caller must fence its controller ownership in this transaction.
Future<bool> retagNativeCdcStream(Transaction* tr, NativeCdcTagState expected, Tag destination);
Future<bool> finishNativeCdcRetag(Transaction* tr, NativeCdcTagState expected);

// Durable metadata operations used by CDC server roles. Registration is
// feature gated; drain and cleanup operations remain available for streams
// persisted before native CDC is disabled.
Future<CDCStreamId> registerNativeCdcStream(Database cx, Key name, KeyRange keys, UID proxyId);
// Persists per-tag final-pop watermarks before removing stream metadata.
Future<bool> removeNativeCdcStream(Database cx, Key name, CDCStreamId streamId, UID proxyId);
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
