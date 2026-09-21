/*
 * NativeCdcMetadata.h
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

#ifndef FDBSERVER_CORE_NATIVECDCMETADATA_H
#define FDBSERVER_CORE_NATIVECDCMETADATA_H
#pragma once

#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"

// A durable snapshot used to validate tag transitions and their finalization.
struct NativeCdcTagState {
	CDCStreamId streamId = 0;
	std::vector<KeyRange> ranges;
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

struct NativeCdcRegistrationResult {
	CDCStreamId streamId;
	// Describes only mutations prepared by this helper, not unrelated caller mutations.
	bool requiresCommit;
};

// Prepares one registration without committing or retrying. The caller sets LOCK_AWARE and ACCESS_SYSTEM_KEYS
// and owns commit and retry handling. Transaction does not read its own writes: prepare at most one registration
// per transaction, without earlier mutations to the CDC metadata this operation reads.
Future<NativeCdcRegistrationResult> prepareNativeCdcStreamRegistration(Transaction* tr,
                                                                       Key name,
                                                                       std::vector<KeyRange> ranges,
                                                                       UID proxyId);

// Durable metadata operations used by CDC server roles. Registration is
// feature gated; drain and cleanup operations remain available for streams
// persisted before native CDC is disabled.
Future<CDCStreamId> registerNativeCdcStream(Database cx, Key name, std::vector<KeyRange> ranges, UID proxyId);
// Persists per-tag final-pop watermarks before removing stream metadata.
Future<bool> removeNativeCdcStream(Database cx, Key name, CDCStreamId streamId, UID proxyId);
// Atomically moves any streams assigned to a failed proxy to its replacement.
Future<Void> reassignNativeCdcStreams(Database cx, UID oldProxyId, UID newProxyId);

#endif // FDBSERVER_CORE_NATIVECDCMETADATA_H
