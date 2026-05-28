/*
 * NativeCdc.h
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

#ifndef FDBCLIENT_NATIVECDC_H
#define FDBCLIENT_NATIVECDC_H
#pragma once

#include <vector>

#include "fdbclient/CDCProxyInterface.h"
#include "fdbclient/NativeAPI.actor.h"

struct NativeCdcStreamInfo {
	Key name;
	CDCStreamId streamId = 0;
	KeyRange keys;
	Version minVersion = invalidVersion;
};

struct NativeCdcRemovedStreamInfo {
	Version removalVersion = invalidVersion;
	std::vector<Tag> tags;
};

class NativeCdcConsumer : public ReferenceCounted<NativeCdcConsumer> {
public:
	NativeCdcConsumer(Database cx, CDCCursor position) : cx(cx), currentPosition(position) {}

	Future<CDCConsumeReply> consume();
	Future<Void> acknowledge();
	const CDCCursor& position() const { return currentPosition; }

private:
	static Future<CDCConsumeReply> consumeImpl(Reference<NativeCdcConsumer> self);
	static Future<Void> acknowledgeImpl(Reference<NativeCdcConsumer> self);

	Database cx;
	CDCCursor currentPosition;
	Version knownAvailableThrough = invalidVersion;
};

// These durable metadata operations back CDCProxyInterface lifecycle requests.
// Registration is knob-protected; draining and cleanup remain available for
// streams persisted while native CDC was enabled.
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

// Client-facing CDC operations. Registration is feature gated; the remaining
// operations stay available so existing durable streams can be drained after
// the feature is disabled. Requests retry when stream ownership changes.
Future<CDCStreamId> registerNativeCdcStreamClient(Database cx, Key name, KeyRange keys);
Future<Void> removeNativeCdcStreamClient(Database cx, Key name);
Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreamsClient(Database cx);
// Uses the range registered for this name; consumers do not respecify it. A
// CDCCursor remains a serializable position token and does not hold Database.
Future<Reference<NativeCdcConsumer>> createNativeCdcConsumer(Database cx, Key name);
Reference<NativeCdcConsumer> resumeNativeCdcConsumer(Database cx, CDCCursor position);

#endif // FDBCLIENT_NATIVECDC_H
