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

class NativeCdcConsumer : public ReferenceCounted<NativeCdcConsumer> {
	static Future<CDCConsumeReply> consumeImpl(Reference<NativeCdcConsumer> self);
	static Future<Void> acknowledgeImpl(Reference<NativeCdcConsumer> self);

	Database cx;
	CDCCursor currentPosition;
	Version knownAvailableThrough = invalidVersion;
	bool operationOutstanding = false;

public:
	NativeCdcConsumer(Database cx, CDCCursor position) : cx(cx), currentPosition(position) {}

	// Operations advance shared delivery state; only one may be outstanding.
	Future<CDCConsumeReply> consume();
	Future<Void> acknowledge();
	const CDCCursor& position() const { return currentPosition; }
};

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
