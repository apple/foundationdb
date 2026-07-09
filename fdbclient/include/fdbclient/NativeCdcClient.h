/*
 * NativeCdcClient.h
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

#ifndef FDBCLIENT_NATIVECDCCLIENT_H
#define FDBCLIENT_NATIVECDCCLIENT_H
#pragma once

#include <cstdint>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "flow/ThreadHelper.actor.h"

// Native CDC value types shared by thread-safe client surfaces and language
// bindings. Keep this header independent from NativeAPI so multi-version
// client plumbing does not depend on the native client implementation.
struct NativeCdcStreamInfo {
	Key name;
	CDCStreamId streamId = 0;
	KeyRange keys;
	Version minVersion = invalidVersion;
};

struct NativeCdcCursor {
	CDCStreamId streamId = 0;
	Version lastConsumedVersion = invalidVersion;
};

struct NativeCdcMutation {
	uint8_t type = 0;
	Key param1;
	Value param2;
};

struct NativeCdcVersionedMutations {
	Version version = invalidVersion;
	std::vector<NativeCdcMutation> mutations;
};

struct NativeCdcConsumeResult {
	std::vector<NativeCdcVersionedMutations> mutations;
	NativeCdcCursor cursor;
};

// A thread-safe, reference-counted CDC consumer surface for language bindings.
// Implementations own any native consumer state and must keep returned values
// alive independently of the originating native reply arena.
class INativeCdcConsumer {
public:
	virtual ~INativeCdcConsumer() = default;

	virtual ThreadFuture<NativeCdcConsumeResult> consume() = 0;
	virtual ThreadFuture<Void> acknowledge() = 0;
	virtual NativeCdcCursor getPosition() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

#endif // FDBCLIENT_NATIVECDCCLIENT_H
