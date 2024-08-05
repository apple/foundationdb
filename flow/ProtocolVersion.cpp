/*
 * ProtocolVersion.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "flow/ProtocolVersion.h"

namespace {

ProtocolVersion g_currentProtocolVersion(defaultProtocolVersionValue);

}

ProtocolVersion currentProtocolVersion() {
	static ProtocolVersion firstReturnedProtocolVersion = g_currentProtocolVersion;
	// Make sure the protocol version is not changed, once it is already in use
	ASSERT(firstReturnedProtocolVersion == g_currentProtocolVersion);
	return g_currentProtocolVersion;
}

void useFutureProtocolVersion() {
	g_currentProtocolVersion = ProtocolVersion(futureProtocolVersionValue);
}