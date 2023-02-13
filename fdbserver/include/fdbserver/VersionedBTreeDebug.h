/*
 * VersionedBTreeDebug.h
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

#ifndef FDBSERVER_VERSIONEDBTREEDEBUG_H
#define FDBSERVER_VERSIONEDBTREEDEBUG_H
#pragma once

#include "flow/flow.h"

#define REDWOOD_DEBUG 0

// Debug output stream
extern FILE* g_debugStream;

// Knob to disable XOR encryption for unit tests that aren't compatible with XOR encryption.
extern bool g_allowXOREncryptionInSimulation;

bool enableRedwoodDebug();

#define debug_printf_always(...)                                                                                       \
	if (enableRedwoodDebug()) {                                                                                        \
		std::string prefix = format("%s %f %04d ", g_network->getLocalAddress().toString().c_str(), now(), __LINE__);  \
		std::string msg = format(__VA_ARGS__);                                                                         \
		fputs(addPrefix(prefix, msg).c_str(), g_debugStream);                                                          \
		fflush(g_debugStream);                                                                                         \
	}

#define debug_print(str) debug_printf("%s\n", str.c_str())
#define debug_print_always(str) debug_printf_always("%s\n", str.c_str())
#define debug_printf_noop(...)

#if defined(NO_INTELLISENSE)
#if REDWOOD_DEBUG
#define debug_printf debug_printf_always
#else
#define debug_printf debug_printf_noop
#endif
#else
// To get error-checking on debug_printf statements in IDE
#define debug_printf printf
#endif

#define BEACON debug_printf_always("HERE\n")
#define TRACE                                                                                                          \
	debug_printf_always("%s: %s line %d %s\n", __FUNCTION__, __FILE__, __LINE__, platform::get_backtrace().c_str());

#endif // FDBSERVER_VERSIONEDBTREEDEBUG_H