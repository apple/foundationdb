/*
 * VersionedBTreeDebug.cpp
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

#include "fdbserver/VersionedBTreeDebug.h"

// Whether to enable debug log
static bool g_debugEnabled = true;
// Only print debug info for a specific address
static NetworkAddress g_debugAddress = NetworkAddress::parse("0.0.0.0:0");
// Only print debug info between specific time range
static double g_debugStart = 0;
static double g_debugEnd = std::numeric_limits<double>::max();

bool enableRedwoodDebug() {
	return g_debugEnabled && now() >= g_debugStart && now() < g_debugEnd &&
	       (!g_network->getLocalAddress().isValid() || !g_debugAddress.isValid() ||
	        g_network->getLocalAddress() == g_debugAddress);
}