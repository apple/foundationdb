/*
 * fdb_c_internal.h
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

#ifndef FDB_C_INTERNAL_H
#define FDB_C_INTERNAL_H
#include "flow/ProtocolVersion.h"
#pragma once
#include "fdb_c.h"

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#ifndef WARN_UNUSED_RESULT
#define WARN_UNUSED_RESULT
#endif

#ifdef __cplusplus
extern "C" {
#endif

// Keep typedef syntax because this header is shared with C API implementation code.
typedef struct DatabaseSharedState DatabaseSharedState;

DLLEXPORT FDBFuture* fdb_database_create_shared_state(FDBDatabase* db);

DLLEXPORT void fdb_database_set_shared_state(FDBDatabase* db, DatabaseSharedState* p);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_shared_state(FDBFuture* f, DatabaseSharedState** outPtr);

DLLEXPORT void fdb_use_future_protocol_version();

// Older multiversion clients look up these names during setup even when the
// application does not use blob granules. Retaining the exports allows setup.
// Each invocation prints the function name and aborts because the operation
// was removed in 8.0.

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_granule_summary_array(FDBFuture* f,
                                                                              FDBGranuleSummary const** out_summaries,
                                                                              int* out_count);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_purge_blob_granules(FDBDatabase* db,
                                                                         uint8_t const* begin_key_name,
                                                                         int begin_key_name_length,
                                                                         uint8_t const* end_key_name,
                                                                         int end_key_name_length,
                                                                         int64_t purge_version,
                                                                         fdb_bool_t force);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_wait_purge_granules_complete(FDBDatabase* db,
                                                                                  uint8_t const* purge_key_name,
                                                                                  int purge_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_blobbify_range(FDBDatabase* db,
                                                                    uint8_t const* begin_key_name,
                                                                    int begin_key_name_length,
                                                                    uint8_t const* end_key_name,
                                                                    int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_blobbify_range_blocking(FDBDatabase* db,
                                                                             uint8_t const* begin_key_name,
                                                                             int begin_key_name_length,
                                                                             uint8_t const* end_key_name,
                                                                             int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_unblobbify_range(FDBDatabase* db,
                                                                      uint8_t const* begin_key_name,
                                                                      int begin_key_name_length,
                                                                      uint8_t const* end_key_name,
                                                                      int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_list_blobbified_ranges(FDBDatabase* db,
                                                                            uint8_t const* begin_key_name,
                                                                            int begin_key_name_length,
                                                                            uint8_t const* end_key_name,
                                                                            int end_key_name_length,
                                                                            int rangeLimit);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_verify_blob_range(FDBDatabase* db,
                                                                       uint8_t const* begin_key_name,
                                                                       int begin_key_name_length,
                                                                       uint8_t const* end_key_name,
                                                                       int end_key_name_length,
                                                                       int64_t version);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_flush_blob_range(FDBDatabase* db,
                                                                      uint8_t const* begin_key_name,
                                                                      int begin_key_name_length,
                                                                      uint8_t const* end_key_name,
                                                                      int end_key_name_length,
                                                                      fdb_bool_t compact,
                                                                      int64_t version);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_purge_blob_granules(FDBTenant* db,
                                                                       uint8_t const* begin_key_name,
                                                                       int begin_key_name_length,
                                                                       uint8_t const* end_key_name,
                                                                       int end_key_name_length,
                                                                       int64_t purge_version,
                                                                       fdb_bool_t force);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_wait_purge_granules_complete(FDBTenant* db,
                                                                                uint8_t const* purge_key_name,
                                                                                int purge_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_blobbify_range(FDBTenant* tenant,
                                                                  uint8_t const* begin_key_name,
                                                                  int begin_key_name_length,
                                                                  uint8_t const* end_key_name,
                                                                  int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_blobbify_range_blocking(FDBTenant* tenant,
                                                                           uint8_t const* begin_key_name,
                                                                           int begin_key_name_length,
                                                                           uint8_t const* end_key_name,
                                                                           int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_unblobbify_range(FDBTenant* tenant,
                                                                    uint8_t const* begin_key_name,
                                                                    int begin_key_name_length,
                                                                    uint8_t const* end_key_name,
                                                                    int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_list_blobbified_ranges(FDBTenant* tenant,
                                                                          uint8_t const* begin_key_name,
                                                                          int begin_key_name_length,
                                                                          uint8_t const* end_key_name,
                                                                          int end_key_name_length,
                                                                          int rangeLimit);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_verify_blob_range(FDBTenant* tenant,
                                                                     uint8_t const* begin_key_name,
                                                                     int begin_key_name_length,
                                                                     uint8_t const* end_key_name,
                                                                     int end_key_name_length,
                                                                     int64_t version);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_flush_blob_range(FDBTenant* tenant,
                                                                    uint8_t const* begin_key_name,
                                                                    int begin_key_name_length,
                                                                    uint8_t const* end_key_name,
                                                                    int end_key_name_length,
                                                                    fdb_bool_t compact,
                                                                    int64_t version);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_blob_granule_ranges(FDBTransaction* tr,
                                                                                uint8_t const* begin_key_name,
                                                                                int begin_key_name_length,
                                                                                uint8_t const* end_key_name,
                                                                                int end_key_name_length,
                                                                                int rangeLimit);

DLLEXPORT WARN_UNUSED_RESULT FDBResult* fdb_transaction_read_blob_granules(FDBTransaction* tr,
                                                                           uint8_t const* begin_key_name,
                                                                           int begin_key_name_length,
                                                                           uint8_t const* end_key_name,
                                                                           int end_key_name_length,
                                                                           int64_t beginVersion,
                                                                           int64_t readVersion,
                                                                           FDBReadBlobGranuleContext granuleContext);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_summarize_blob_granules(FDBTransaction* tr,
                                                                                uint8_t const* begin_key_name,
                                                                                int begin_key_name_length,
                                                                                uint8_t const* end_key_name,
                                                                                int end_key_name_length,
                                                                                int64_t summaryVersion,
                                                                                int rangeLimit);

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
