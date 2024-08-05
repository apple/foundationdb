/*
 * fdb_shim_c.h
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

#ifndef FDB_SHIM_C_H
#define FDB_SHIM_C_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Specify the path of the local libfdb_c.so library to be dynamically loaded by the shim layer
 *
 * This enables running the same application code with different client library versions,
 * e.g. using the latest development build for testing new features, but still using the latest
 * stable release in production deployments.
 *
 * The given path overrides the environment variable FDB_LOCAL_CLIENT_LIBRARY_PATH
 */
DLLEXPORT void fdb_shim_set_local_client_library_path(const char* filePath);

#ifdef __cplusplus
}
#endif
#endif
