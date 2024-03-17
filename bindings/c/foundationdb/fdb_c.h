/*
 * fdb_c.h
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

#ifndef FDB_C_H
#define FDB_C_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#include "fdb_c_apiversion.g.h"
#if (defined FDB_USE_LATEST_API_VERSION)
#define FDB_API_VERSION FDB_LATEST_API_VERSION
#elif (defined FDB_USE_LATEST_BINDINGS_API_VERSION)
#define FDB_API_VERSION FDB_LATEST_BINDINGS_API_VERSION
#endif

#if !defined(FDB_API_VERSION)
#error You must #define FDB_API_VERSION prior to including fdb_c.h (the latest version is defined as FDB_LATEST_API_VERSION)
#elif FDB_API_VERSION < 13
#error API version no longer supported (upgrade to 13)
#elif FDB_API_VERSION > FDB_LATEST_API_VERSION
#error Requested API version requires a newer version of this header
#endif

#if FDB_API_VERSION >= 23 && !defined(WARN_UNUSED_RESULT)
#ifdef __GNUG__
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
#define WARN_UNUSED_RESULT
#endif
#else
#define WARN_UNUSED_RESULT
#endif

/*
 * With default settings, gcc will not warn about unprototyped functions being
 * called, so it is easy to erroneously call a function which is not available
 * at FDB_API_VERSION and then get an error only at runtime.  These macros
 * ensure a compile error in such cases, and attempt to make the compile error
 * slightly informative.
 */
#define This_FoundationDB_API_function_is_removed_at_this_FDB_API_VERSION()                                            \
	{ == == = }
#define FDB_REMOVED_FUNCTION This_FoundationDB_API_function_is_removed_at_this_FDB_API_VERSION(0)

#include <stdint.h>

#include "fdb_c_options.g.h"
#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

DLLEXPORT const char* fdb_get_error(fdb_error_t code);

DLLEXPORT fdb_bool_t fdb_error_predicate(int predicate_test, fdb_error_t code);

#define /* fdb_error_t */ fdb_select_api_version(v) fdb_select_api_version_impl(v, FDB_API_VERSION)

/*
 * A variant of fdb_select_api_version that caps the header API version by the maximum API version
 * supported by the client library. It is intended mainly for use in combination with the shim
 * layer, which loads the client library dynamically.
 */
#define /* fdb_error_t */ fdb_select_api_version_capped(v)                                                             \
	fdb_select_api_version_impl(                                                                                       \
	    v, FDB_API_VERSION < fdb_get_max_api_version() ? FDB_API_VERSION : fdb_get_max_api_version())

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_network_set_option(FDBNetworkOption option,
                                                                uint8_t const* value,
                                                                int value_length);

#if FDB_API_VERSION >= 14
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_setup_network(void);
#endif

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_run_network(void);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_stop_network(void);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_add_network_thread_completion_hook(void (*hook)(void*),
                                                                                void* hook_parameter);

#pragma pack(push, 4)
typedef struct key {
	const uint8_t* key;
	int key_length;
} FDBKey;
#if FDB_API_VERSION >= 630
typedef struct keyvalue {
	const uint8_t* key;
	int key_length;
	const uint8_t* value;
	int value_length;
} FDBKeyValue;
#else
typedef struct keyvalue {
	const void* key;
	int key_length;
	const void* value;
	int value_length;
} FDBKeyValue;
#endif

#pragma pack(pop)

/* Memory layout of KeySelectorRef. */
typedef struct keyselector {
	FDBKey key;
	/* orEqual and offset have not be tested in C binding. Just a placeholder. */
	fdb_bool_t orEqual;
	int offset;
} FDBKeySelector;

/* Memory layout of GetRangeReqAndResultRef. */
typedef struct getrangereqandresult {
	FDBKeySelector begin;
	FDBKeySelector end;
	FDBKeyValue* data;
	int m_size, m_capacity;
} FDBGetRangeReqAndResult;

/* Memory layout of MappedKeyValueRef.

Total 112 bytes
- key (12 bytes)
:74:8F:8E:5F:AE:7F:00:00
:4A:00:00:00
- value (12 bytes)
:70:8F:8E:5F:AE:7F:00:00
:00:00:00:00
- begin selector (20 bytes)
:30:8F:8E:5F:AE:7F:00:00
:2D:00:00:00
:00:7F:00:00
:01:00:00:00
- end selector (20 bytes)
:EC:8E:8E:5F:AE:7F:00:00
:2D:00:00:00
:00:2B:3C:60
:01:00:00:00
- vector (16 bytes)
:74:94:8E:5F:AE:7F:00:00
:01:00:00:00
:01:00:00:00
- buffer (32 bytes)
:00:20:D1:61:00:00:00:00
:00:00:00:00:00:00:00:00
:00:00:00:00:00:00:00:00
:01:00:00:00:AE:7F:00:00
*/
typedef struct mappedkeyvalue {
	FDBKey key;
	FDBKey value;
	/* It's complicated to map a std::variant to C. For now we assume the underlying requests are always getRange
	 * and take the shortcut. */
	FDBGetRangeReqAndResult getRange;
	unsigned char buffer[32];
} FDBMappedKeyValue;

#pragma pack(push, 4)
typedef struct keyrange {
	const uint8_t* begin_key;
	int begin_key_length;
	const uint8_t* end_key;
	int end_key_length;
} FDBKeyRange;

typedef struct granulesummary {
	FDBKeyRange key_range;
	int64_t snapshot_version;
	int64_t snapshot_size;
	int64_t delta_version;
	int64_t delta_size;
} FDBGranuleSummary;
#pragma pack(pop)

typedef struct readgranulecontext {
	/* User context to pass along to functions */
	void* userContext;

	/* Returns a unique id for the load. Asynchronous to support queueing multiple in parallel. */
	int64_t (*start_load_f)(const char* filename,
	                        int filenameLength,
	                        int64_t offset,
	                        int64_t length,
	                        int64_t fullFileLength,
	                        void* context);

	/* Returns data for the load. Pass the loadId returned by start_load_f */
	uint8_t* (*get_load_f)(int64_t loadId, void* context);

	/* Frees data from load. Pass the loadId returned by start_load_f */
	void (*free_load_f)(int64_t loadId, void* context);

	/* Set this to true for testing if you don't want to read the granule files,
	   just do the request to the blob workers */
	fdb_bool_t debugNoMaterialize;

	/* Number of granules to load in parallel */
	int granuleParallelism;
} FDBReadBlobGranuleContext;

typedef enum { FDB_BG_MUTATION_TYPE_SET_VALUE = 0, FDB_BG_MUTATION_TYPE_CLEAR_RANGE = 1 } FDBBGMutationType;

#pragma pack(push, 4)

typedef struct bgtenantprefix {
	fdb_bool_t present;
	FDBKey prefix;
} FDBBGTenantPrefix;

/* encryption structs correspond to similar ones in BlobGranuleCommon.h */
typedef struct bgencryptionkey {
	int64_t domain_id;
	uint64_t base_key_id;
	uint32_t base_kcv;
	uint64_t random_salt;
	FDBKey base_key;
} FDBBGEncryptionKey;

typedef struct bgencryptionctx {
	fdb_bool_t present;
	FDBBGEncryptionKey textKey;
	uint32_t textKCV;
	FDBBGEncryptionKey headerKey;
	uint32_t headerKCV;
	FDBKey iv;
} FDBBGEncryptionCtx;

typedef struct bgfilepointer {
	const uint8_t* filename_ptr;
	int filename_length;
	int64_t file_offset;
	int64_t file_length;
	int64_t full_file_length;
	int64_t file_version;
	FDBBGEncryptionCtx encryption_ctx;
} FDBBGFilePointer;

typedef struct bgmutation {
	/* FDBBGMutationType */ uint8_t type;
	int64_t version;
	const uint8_t* param1_ptr;
	int param1_length;
	const uint8_t* param2_ptr;
	int param2_length;
} FDBBGMutation;

typedef struct bgfiledescription {
	FDBKeyRange key_range;
	fdb_bool_t snapshot_present;
	FDBBGFilePointer snapshot_file_pointer;
	int delta_file_count;
	FDBBGFilePointer* delta_files;
	int memory_mutation_count;
	FDBBGMutation* memory_mutations;
	FDBBGTenantPrefix tenant_prefix;
} FDBBGFileDescription;
#pragma pack(pop)

DLLEXPORT void fdb_future_cancel(FDBFuture* f);

DLLEXPORT void fdb_future_release_memory(FDBFuture* f);

DLLEXPORT void fdb_future_destroy(FDBFuture* f);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_block_until_ready(FDBFuture* f);

DLLEXPORT fdb_bool_t fdb_future_is_ready(FDBFuture* f);

typedef void (*FDBCallback)(FDBFuture* future, void* callback_parameter);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_set_callback(FDBFuture* f,
                                                                 FDBCallback callback,
                                                                 void* callback_parameter);

#if FDB_API_VERSION >= 23
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_error(FDBFuture* f);
#endif

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_bool(FDBFuture* f, fdb_bool_t* out);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_int64(FDBFuture* f, int64_t* out);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_uint64(FDBFuture* f, uint64_t* out);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_double(FDBFuture* f, double* out);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_key(FDBFuture* f, uint8_t const** out_key, int* out_key_length);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_value(FDBFuture* f,
                                                              fdb_bool_t* out_present,
                                                              uint8_t const** out_value,
                                                              int* out_value_length);

#if FDB_API_VERSION >= 14
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_keyvalue_array(FDBFuture* f,
                                                                       FDBKeyValue const** out_kv,
                                                                       int* out_count,
                                                                       fdb_bool_t* out_more);
#endif

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_mappedkeyvalue_array(FDBFuture* f,
                                                                             FDBMappedKeyValue const** out_kv,
                                                                             int* out_count,
                                                                             fdb_bool_t* out_more);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_key_array(FDBFuture* f,
                                                                  FDBKey const** out_key_array,
                                                                  int* out_count);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_string_array(FDBFuture* f,
                                                                     const char*** out_strings,
                                                                     int* out_count);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_keyrange_array(FDBFuture* f,
                                                                       FDBKeyRange const** out_ranges,
                                                                       int* out_count);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_granule_summary_array(FDBFuture* f,
                                                                              FDBGranuleSummary const** out_summaries,
                                                                              int* out_count);

/* all for using future result from read_blob_granules_description */
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_readbg_get_descriptions(FDBFuture* f,
                                                                            FDBBGFileDescription** out,
                                                                            int* desc_count);

DLLEXPORT WARN_UNUSED_RESULT FDBResult* fdb_readbg_parse_snapshot_file(const uint8_t* file_data,
                                                                       int file_len,
                                                                       FDBBGTenantPrefix const* tenant_prefix,
                                                                       FDBBGEncryptionCtx const* encryption_ctx);

DLLEXPORT WARN_UNUSED_RESULT FDBResult* fdb_readbg_parse_delta_file(const uint8_t* file_data,
                                                                    int file_len,
                                                                    FDBBGTenantPrefix const* tenant_prefix,
                                                                    FDBBGEncryptionCtx const* encryption_ctx);

/* FDBResult is a synchronous computation result, as opposed to a future that is asynchronous. */
DLLEXPORT void fdb_result_destroy(FDBResult* r);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_result_get_keyvalue_array(FDBResult* r,
                                                                       FDBKeyValue const** out_kv,
                                                                       int* out_count,
                                                                       fdb_bool_t* out_more);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_result_get_bg_mutations_array(FDBResult* r,
                                                                           FDBBGMutation const** out_mutations,
                                                                           int* out_count);

/* TODO: add other return types as we need them */

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_create_database(const char* cluster_file_path, FDBDatabase** out_database);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_create_database_from_connection_string(const char* connection_string,
                                                                                    FDBDatabase** out_database);

DLLEXPORT void fdb_database_destroy(FDBDatabase* d);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_database_set_option(FDBDatabase* d,
                                                                 FDBDatabaseOption option,
                                                                 uint8_t const* value,
                                                                 int value_length);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_database_open_tenant(FDBDatabase* d,
                                                                  uint8_t const* tenant_name,
                                                                  int tenant_name_length,
                                                                  FDBTenant** out_tenant);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_database_create_transaction(FDBDatabase* d,
                                                                         FDBTransaction** out_transaction);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_reboot_worker(FDBDatabase* db,
                                                                   uint8_t const* address,
                                                                   int address_length,
                                                                   fdb_bool_t check,
                                                                   int duration);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_force_recovery_with_data_loss(FDBDatabase* db,
                                                                                   uint8_t const* dcid,
                                                                                   int dcid_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_create_snapshot(FDBDatabase* db,
                                                                     uint8_t const* uid,
                                                                     int uid_length,
                                                                     uint8_t const* snap_command,
                                                                     int snap_command_length);

DLLEXPORT WARN_UNUSED_RESULT double fdb_database_get_main_thread_busyness(FDBDatabase* db);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_get_server_protocol(FDBDatabase* db, uint64_t expected_version);

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

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_database_get_client_status(FDBDatabase* db);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_tenant_create_transaction(FDBTenant* tenant,
                                                                       FDBTransaction** out_transaction);

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

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_tenant_get_id(FDBTenant* tenant);

DLLEXPORT void fdb_tenant_destroy(FDBTenant* tenant);

DLLEXPORT void fdb_transaction_destroy(FDBTransaction* tr);

DLLEXPORT void fdb_transaction_cancel(FDBTransaction* tr);

#if FDB_API_VERSION >= 14
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_transaction_set_option(FDBTransaction* tr,
                                                                    FDBTransactionOption option,
                                                                    uint8_t const* value,
                                                                    int value_length);
#endif

DLLEXPORT void fdb_transaction_set_read_version(FDBTransaction* tr, int64_t version);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_read_version(FDBTransaction* tr);

#if FDB_API_VERSION >= 14
DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get(FDBTransaction* tr,
                                                            uint8_t const* key_name,
                                                            int key_name_length,
                                                            fdb_bool_t snapshot);
#endif

#if FDB_API_VERSION >= 14
DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_key(FDBTransaction* tr,
                                                                uint8_t const* key_name,
                                                                int key_name_length,
                                                                fdb_bool_t or_equal,
                                                                int offset,
                                                                fdb_bool_t snapshot);
#endif

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_addresses_for_key(FDBTransaction* tr,
                                                                              uint8_t const* key_name,
                                                                              int key_name_length);

#if FDB_API_VERSION >= 14
DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range(FDBTransaction* tr,
                                                                  uint8_t const* begin_key_name,
                                                                  int begin_key_name_length,
                                                                  fdb_bool_t begin_or_equal,
                                                                  int begin_offset,
                                                                  uint8_t const* end_key_name,
                                                                  int end_key_name_length,
                                                                  fdb_bool_t end_or_equal,
                                                                  int end_offset,
                                                                  int limit,
                                                                  int target_bytes,
                                                                  FDBStreamingMode mode,
                                                                  int iteration,
                                                                  fdb_bool_t snapshot,
                                                                  fdb_bool_t reverse);
#endif

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_mapped_range(FDBTransaction* tr,
                                                                         uint8_t const* begin_key_name,
                                                                         int begin_key_name_length,
                                                                         fdb_bool_t begin_or_equal,
                                                                         int begin_offset,
                                                                         uint8_t const* end_key_name,
                                                                         int end_key_name_length,
                                                                         fdb_bool_t end_or_equal,
                                                                         int end_offset,
                                                                         uint8_t const* mapper_name,
                                                                         int mapper_name_length,
                                                                         int limit,
                                                                         int target_bytes,
                                                                         FDBStreamingMode mode,
                                                                         int iteration,
                                                                         fdb_bool_t snapshot,
                                                                         fdb_bool_t reverse);

DLLEXPORT void fdb_transaction_set(FDBTransaction* tr,
                                   uint8_t const* key_name,
                                   int key_name_length,
                                   uint8_t const* value,
                                   int value_length);

DLLEXPORT void fdb_transaction_atomic_op(FDBTransaction* tr,
                                         uint8_t const* key_name,
                                         int key_name_length,
                                         uint8_t const* param,
                                         int param_length,
                                         FDBMutationType operation_type);

DLLEXPORT void fdb_transaction_clear(FDBTransaction* tr, uint8_t const* key_name, int key_name_length);

DLLEXPORT void fdb_transaction_clear_range(FDBTransaction* tr,
                                           uint8_t const* begin_key_name,
                                           int begin_key_name_length,
                                           uint8_t const* end_key_name,
                                           int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_watch(FDBTransaction* tr,
                                                              uint8_t const* key_name,
                                                              int key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_commit(FDBTransaction* tr);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_transaction_get_committed_version(FDBTransaction* tr,
                                                                               int64_t* out_version);

/*
 * These functions intentionally return an FDBFuture instead of a numeric value
 * directly, so that calling the API can see the effect of previous
 * mutations on the transaction. Specifically, mutations are applied
 * asynchronously by the main thread. In order to see them, this call has to
 * be serviced by the main thread too.
 */
DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_tag_throttled_duration(FDBTransaction* tr);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_total_cost(FDBTransaction* tr);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_approximate_size(FDBTransaction* tr);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_versionstamp(FDBTransaction* tr);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_on_error(FDBTransaction* tr, fdb_error_t error);

DLLEXPORT void fdb_transaction_reset(FDBTransaction* tr);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_transaction_add_conflict_range(FDBTransaction* tr,
                                                                            uint8_t const* begin_key_name,
                                                                            int begin_key_name_length,
                                                                            uint8_t const* end_key_name,
                                                                            int end_key_name_length,
                                                                            FDBConflictRangeType type);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_estimated_range_size_bytes(FDBTransaction* tr,
                                                                                       uint8_t const* begin_key_name,
                                                                                       int begin_key_name_length,
                                                                                       uint8_t const* end_key_name,
                                                                                       int end_key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range_split_points(FDBTransaction* tr,
                                                                               uint8_t const* begin_key_name,
                                                                               int begin_key_name_length,
                                                                               uint8_t const* end_key_name,
                                                                               int end_key_name_length,
                                                                               int64_t chunk_size);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_blob_granule_ranges(FDBTransaction* tr,
                                                                                uint8_t const* begin_key_name,
                                                                                int begin_key_name_length,
                                                                                uint8_t const* end_key_name,
                                                                                int end_key_name_length,
                                                                                int rangeLimit);

/* LatestVersion (-2) for readVersion means get read version from transaction
   Separated out as optional because BG reads can support longer-lived reads than normal FDB transactions */
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

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_read_blob_granules_description(FDBTransaction* tr,
                                                                                       uint8_t const* begin_key_name,
                                                                                       int begin_key_name_length,
                                                                                       uint8_t const* end_key_name,
                                                                                       int end_key_name_length,
                                                                                       int64_t begin_version,
                                                                                       int64_t read_version,
                                                                                       int64_t* read_version_out);

#define FDB_KEYSEL_LAST_LESS_THAN(k, l) k, l, 0, 0
#define FDB_KEYSEL_LAST_LESS_OR_EQUAL(k, l) k, l, 1, 0
#define FDB_KEYSEL_FIRST_GREATER_THAN(k, l) k, l, 1, 1
#define FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(k, l) k, l, 0, 1

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_select_api_version_impl(int runtime_version, int header_version);

DLLEXPORT int fdb_get_max_api_version(void);
DLLEXPORT const char* fdb_get_client_version(void);

/* LEGACY API VERSIONS */

#if FDB_API_VERSION < 620
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_version(FDBFuture* f, int64_t* out_version);
#else
#define fdb_future_get_version(f, ov) FDB_REMOVED_FUNCTION
#endif

#if FDB_API_VERSION < 610 || defined FDB_INCLUDE_LEGACY_TYPES
typedef struct FDB_cluster FDBCluster;

typedef enum {
	/* This option is only a placeholder for C compatibility and should not be used */
	FDB_CLUSTER_OPTION_DUMMY_DO_NOT_USE = -1
} FDBClusterOption;
#endif

#if FDB_API_VERSION < 610
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_cluster(FDBFuture* f, FDBCluster** out_cluster);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_database(FDBFuture* f, FDBDatabase** out_database);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_create_cluster(const char* cluster_file_path);

DLLEXPORT void fdb_cluster_destroy(FDBCluster* c);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_cluster_set_option(FDBCluster* c,
                                                                FDBClusterOption option,
                                                                uint8_t const* value,
                                                                int value_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_cluster_create_database(FDBCluster* c,
                                                                    uint8_t const* db_name,
                                                                    int db_name_length);
#else
#define fdb_future_get_cluster(f, oc) FDB_REMOVED_FUNCTION
#define fdb_future_get_database(f, od) FDB_REMOVED_FUNCTION
#define fdb_create_cluster(cfp) FDB_REMOVED_FUNCTION
#define fdb_cluster_destroy(c) FDB_REMOVED_FUNCTION
#define fdb_cluster_set_option(c, o, v, vl) FDB_REMOVED_FUNCTION
#define fdb_cluster_create_database(c, dn, dnl) FDB_REMOVED_FUNCTION
#endif

#if FDB_API_VERSION < 23
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_error(FDBFuture* f, const char** out_description /* = NULL */);

DLLEXPORT fdb_bool_t fdb_future_is_error(FDBFuture* f);
#else
#define fdb_future_is_error(x) FDB_REMOVED_FUNCTION
#endif

#if FDB_API_VERSION < 14
DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_keyvalue_array(FDBFuture* f,
                                                                       FDBKeyValue const** out_kv,
                                                                       int* out_count);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get(FDBTransaction* tr,
                                                            uint8_t const* key_name,
                                                            int key_name_length);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_key(FDBTransaction* tr,
                                                                uint8_t const* key_name,
                                                                int key_name_length,
                                                                fdb_bool_t or_equal,
                                                                int offset);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_setup_network(const char* local_address);

DLLEXPORT void fdb_transaction_set_option(FDBTransaction* tr, FDBTransactionOption option);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range(FDBTransaction* tr,
                                                                  uint8_t const* begin_key_name,
                                                                  int begin_key_name_length,
                                                                  uint8_t const* end_key_name,
                                                                  int end_key_name_length,
                                                                  int limit);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range_selector(FDBTransaction* tr,
                                                                           uint8_t const* begin_key_name,
                                                                           int begin_key_name_length,
                                                                           fdb_bool_t begin_or_equal,
                                                                           int begin_offset,
                                                                           uint8_t const* end_key_name,
                                                                           int end_key_name_length,
                                                                           fdb_bool_t end_or_equal,
                                                                           int end_offset,
                                                                           int limit);

#else
#define fdb_transaction_get_range_selector(tr, bkn, bknl, boe, bo, ekn, eknl, eoe, eo, lim) FDB_REMOVED_FUNCTION
#endif

#ifdef __cplusplus
}
#endif
#endif
