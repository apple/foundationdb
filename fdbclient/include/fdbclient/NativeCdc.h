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

#include "fdbclient/NativeAPI.actor.h"

struct NativeCdcStreamInfo {
	Key name;
	CDCStreamId streamId = 0;
	KeyRange keys;
	Version minVersion = invalidVersion;
};

// These durable metadata operations are intended to back CDCProxyInterface
// lifecycle requests once CDC proxies are recruited.
Future<CDCStreamId> registerNativeCdcStream(Database cx,
                                            Key name,
                                            KeyRange keys,
                                            Optional<UID> proxyId = Optional<UID>());
Future<Void> removeNativeCdcStream(Database cx, Key name, Optional<UID> proxyId = Optional<UID>());
Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreams(Database cx);
// Persists the exclusive unpopped watermark after consuming through a version.
// Removed streams remain acknowledgeable while retained CDC log data is drained.
Future<Version> acknowledgeNativeCdcStream(Database cx, CDCStreamId streamId, Version consumedThrough);

#endif // FDBCLIENT_NATIVECDC_H
