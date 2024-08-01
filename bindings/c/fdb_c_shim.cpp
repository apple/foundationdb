/*
 * fdb_c_shim.cpp
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

#if (defined(__linux__) || defined(__APPLE__) || defined(__FreeBSD__))

#define DLLEXPORT __attribute__((visibility("default")))

#include "foundationdb/fdb_c_shim.h"

#include <dlfcn.h>
#include <string>

namespace {

const char* FDB_LOCAL_CLIENT_LIBRARY_PATH_ENVVAR = "FDB_LOCAL_CLIENT_LIBRARY_PATH";
std::string g_fdbLocalClientLibraryPath;

} // namespace

extern "C" DLLEXPORT void fdb_shim_set_local_client_library_path(const char* filePath) {
	g_fdbLocalClientLibraryPath = filePath;
}

/* The callback of the fdb_c_shim layer that determines the path
   of the fdb_c library to be dynamically loaded
 */
extern "C" void* fdb_shim_dlopen_callback(const char* libName) {
	std::string libPath;
	if (!g_fdbLocalClientLibraryPath.empty()) {
		libPath = g_fdbLocalClientLibraryPath;
	} else {
		char* val = getenv(FDB_LOCAL_CLIENT_LIBRARY_PATH_ENVVAR);
		if (val) {
			libPath = val;
		} else {
			libPath = libName;
		}
	}
	return dlopen(libPath.c_str(), RTLD_LAZY | RTLD_GLOBAL);
}

#else
#error Port me!
#endif
