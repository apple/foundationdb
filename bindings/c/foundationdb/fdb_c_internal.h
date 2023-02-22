/*
 * fdb_c_internal.h
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

#ifndef FDB_C_INTERNAL_H
#define FDB_C_INTERNAL_H
#include "flow/ProtocolVersion.h"
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#ifndef WARN_UNUSED_RESULT
#define WARN_UNUSED_RESULT
#endif

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// forward declaration and typedef
typedef struct DatabaseSharedState DatabaseSharedState;

DLLEXPORT const unsigned char* fdb_retrieve_build_id();

DLLEXPORT FDBFuture* fdb_database_create_shared_state(FDBDatabase* db);

DLLEXPORT void fdb_database_set_shared_state(FDBDatabase* db, DatabaseSharedState* p);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_shared_state(FDBFuture* f, DatabaseSharedState** outPtr);

DLLEXPORT void fdb_use_future_protocol_version();

// the logical read_blob_granules is broken out (at different points depending on the client type) into the asynchronous
// start() that happens on the fdb network thread, and synchronous finish() that happens off it
DLLEXPORT FDBFuture* fdb_transaction_read_blob_granules_start(FDBTransaction* tr,
                                                              uint8_t const* begin_key_name,
                                                              int begin_key_name_length,
                                                              uint8_t const* end_key_name,
                                                              int end_key_name_length,
                                                              int64_t beginVersion,
                                                              int64_t readVersion,
                                                              int64_t* readVersionOut);

DLLEXPORT FDBResult* fdb_transaction_read_blob_granules_finish(FDBTransaction* tr,
                                                               FDBFuture* f,
                                                               uint8_t const* begin_key_name,
                                                               int begin_key_name_length,
                                                               uint8_t const* end_key_name,
                                                               int end_key_name_length,
                                                               int64_t beginVersion,
                                                               int64_t readVersion,
                                                               FDBReadBlobGranuleContext* granuleContext);

#ifdef __cplusplus
}
#endif
#endif
