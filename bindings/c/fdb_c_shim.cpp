/*
 * fdb_c_shim.cpp
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

#if (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__))

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>

static const char* FDB_C_CLIENT_LIBRARY_PATH = "FDB_C_CLIENT_LIBRARY_PATH";

// Callback that tries different library names
extern "C" void* fdb_shim_dlopen_callback(const char* libName) {
	std::string libPath;
	char* val = getenv(FDB_C_CLIENT_LIBRARY_PATH);
	if (val) {
		libPath = val;
	} else {
		libPath = libName;
	}
	return dlopen(libPath.c_str(), RTLD_LAZY | RTLD_GLOBAL);
}

#else
#error Port me!
#endif