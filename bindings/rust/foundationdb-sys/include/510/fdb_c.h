/*
 * fdb_c.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#if !defined(FDB_API_VERSION)
#error You must #define FDB_API_VERSION prior to including fdb_c.h (current version is 510)
#elif FDB_API_VERSION < 13
#error API version no longer supported (upgrade to 13)
#elif FDB_API_VERSION > 510
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

// With default settings, gcc will not warn about unprototyped functions being called, so it
// is easy to erroneously call a function which is not available at FDB_API_VERSION and then
// get an error only at runtime.  These macros ensure a compile error in such cases, and
// attempt to make the compile error slightly informative.
#define This_FoundationDB_API_function_is_removed_at_this_FDB_API_VERSION() [=====]
#define FDB_REMOVED_FUNCTION This_FoundationDB_API_function_is_removed_at_this_FDB_API_VERSION(0)

#include <stdint.h>

#include "fdb_c_options.g.h"

#ifdef __cplusplus
extern "C" {
#endif

    /* Pointers to these opaque types represent objects in the FDB API */
    typedef struct future FDBFuture;
    typedef struct cluster FDBCluster;
    typedef struct database FDBDatabase;
    typedef struct transaction FDBTransaction;

    typedef int fdb_error_t;
    typedef int fdb_bool_t;

    DLLEXPORT const char*
    fdb_get_error( fdb_error_t code );

    DLLEXPORT fdb_bool_t
    fdb_error_predicate( int predicate_test, fdb_error_t code );

    #define /* fdb_error_t */ fdb_select_api_version(v) fdb_select_api_version_impl(v, FDB_API_VERSION)

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_network_set_option( FDBNetworkOption option, uint8_t const* value,
                            int value_length );

#if FDB_API_VERSION >= 14
    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_setup_network();
#endif

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_run_network();

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_stop_network();

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_add_network_thread_completion_hook(void (*hook)(void*), void *hook_parameter);

#pragma pack(push, 4)
    typedef struct keyvalue {
        const void* key;
        int key_length;
        const void* value;
        int value_length;
    } FDBKeyValue;
#pragma pack(pop)

    DLLEXPORT void fdb_future_cancel( FDBFuture *f );

    DLLEXPORT void fdb_future_release_memory( FDBFuture* f );

    DLLEXPORT void fdb_future_destroy( FDBFuture* f );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_block_until_ready( FDBFuture* f );

    DLLEXPORT fdb_bool_t fdb_future_is_ready( FDBFuture* f );

    typedef void (*FDBCallback)(FDBFuture* future, void* callback_parameter);

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_set_callback( FDBFuture* f, FDBCallback callback,
                             void* callback_parameter );

#if FDB_API_VERSION >= 23
    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_error( FDBFuture* f );
#endif

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_version( FDBFuture* f, int64_t* out_version );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_key( FDBFuture* f, uint8_t const** out_key,
                        int* out_key_length );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_cluster( FDBFuture* f, FDBCluster** out_cluster );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_database( FDBFuture* f, FDBDatabase** out_database );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_value( FDBFuture* f, fdb_bool_t *out_present,
                          uint8_t const** out_value,
                          int* out_value_length );

#if FDB_API_VERSION >= 14
    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_keyvalue_array( FDBFuture* f, FDBKeyValue const** out_kv,
                                   int* out_count, fdb_bool_t* out_more );
#endif

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_string_array(FDBFuture* f,
                            const char*** out_strings, int* out_count);

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_create_cluster( const char* cluster_file_path );

    DLLEXPORT void fdb_cluster_destroy( FDBCluster* c );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_cluster_set_option( FDBCluster* c, FDBClusterOption option,
                            uint8_t const* value, int value_length );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture*
    fdb_cluster_create_database( FDBCluster* c, uint8_t const* db_name,
                                 int db_name_length );

    DLLEXPORT void fdb_database_destroy( FDBDatabase* d );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_database_set_option( FDBDatabase* d, FDBDatabaseOption option,
                             uint8_t const* value, int value_length );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_database_create_transaction( FDBDatabase* d,
                                     FDBTransaction** out_transaction );

    DLLEXPORT void fdb_transaction_destroy( FDBTransaction* tr);

    DLLEXPORT void fdb_transaction_cancel( FDBTransaction* tr);

#if FDB_API_VERSION >= 14
    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_transaction_set_option( FDBTransaction* tr, FDBTransactionOption option,
                                uint8_t const* value, int value_length );
#endif

    DLLEXPORT void
    fdb_transaction_set_read_version( FDBTransaction* tr, int64_t version );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_read_version( FDBTransaction* tr );

#if FDB_API_VERSION >= 14
    DLLEXPORT WARN_UNUSED_RESULT FDBFuture*
    fdb_transaction_get( FDBTransaction* tr, uint8_t const* key_name,
                         int key_name_length, fdb_bool_t snapshot );
#endif

#if FDB_API_VERSION >= 14
    DLLEXPORT WARN_UNUSED_RESULT FDBFuture*
    fdb_transaction_get_key( FDBTransaction* tr, uint8_t const* key_name,
                             int key_name_length, fdb_bool_t or_equal,
                             int offset, fdb_bool_t snapshot );
#endif

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture*
    fdb_transaction_get_addresses_for_key(FDBTransaction* tr, uint8_t const* key_name,
                            int key_name_length);

#if FDB_API_VERSION >= 14
    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range(
        FDBTransaction* tr, uint8_t const* begin_key_name,
        int begin_key_name_length, fdb_bool_t begin_or_equal, int begin_offset,
        uint8_t const* end_key_name, int end_key_name_length,
        fdb_bool_t end_or_equal, int end_offset, int limit, int target_bytes,
        FDBStreamingMode mode, int iteration, fdb_bool_t snapshot,
        fdb_bool_t reverse );
#endif

    DLLEXPORT void
    fdb_transaction_set( FDBTransaction* tr, uint8_t const* key_name,
                         int key_name_length, uint8_t const* value,
                         int value_length );

    DLLEXPORT void
    fdb_transaction_atomic_op( FDBTransaction* tr, uint8_t const* key_name,
                               int key_name_length, uint8_t const* param,
                               int param_length, FDBMutationType operation_type );

    DLLEXPORT void
    fdb_transaction_clear( FDBTransaction* tr, uint8_t const* key_name,
                           int key_name_length );

    DLLEXPORT void fdb_transaction_clear_range(
        FDBTransaction* tr, uint8_t const* begin_key_name,
        int begin_key_name_length, uint8_t const* end_key_name,
        int end_key_name_length );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_watch( FDBTransaction *tr,
                                                uint8_t const* key_name,
                                                int key_name_length);

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_commit( FDBTransaction* tr );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_transaction_get_committed_version( FDBTransaction* tr,
                                           int64_t* out_version );

	DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_versionstamp( FDBTransaction* tr );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture*
    fdb_transaction_on_error( FDBTransaction* tr, fdb_error_t error );

    DLLEXPORT void fdb_transaction_reset( FDBTransaction* tr );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_transaction_add_conflict_range(FDBTransaction *tr,
                                       uint8_t const* begin_key_name,
                                       int begin_key_name_length,
                                       uint8_t const* end_key_name,
                                       int end_key_name_length,
                                       FDBConflictRangeType type);

    #define FDB_KEYSEL_LAST_LESS_THAN(k, l) k, l, 0, 0
    #define FDB_KEYSEL_LAST_LESS_OR_EQUAL(k, l) k, l, 1, 0
    #define FDB_KEYSEL_FIRST_GREATER_THAN(k, l) k, l, 1, 1
    #define FDB_KEYSEL_FIRST_GREATER_OR_EQUAL(k, l) k, l, 0, 1

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_select_api_version_impl( int runtime_version, int header_version );

    DLLEXPORT int fdb_get_max_api_version();
    DLLEXPORT const char* fdb_get_client_version();

    /* LEGACY API VERSIONS */

#if FDB_API_VERSION < 23
    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
    fdb_future_get_error( FDBFuture* f,
                          const char** out_description /* = NULL */ );

    DLLEXPORT fdb_bool_t fdb_future_is_error( FDBFuture* f );
#else
	#define fdb_future_is_error(x) FDB_REMOVED_FUNCTION
#endif

#if FDB_API_VERSION < 14
    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_keyvalue_array(
        FDBFuture* f, FDBKeyValue const** out_kv, int* out_count );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get(
        FDBTransaction* tr, uint8_t const* key_name, int key_name_length );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_key(
        FDBTransaction* tr, uint8_t const* key_name, int key_name_length,
        fdb_bool_t or_equal, int offset );

    DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_setup_network( const char* local_address );

    DLLEXPORT void fdb_transaction_set_option(
        FDBTransaction* tr, FDBTransactionOption option );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range(
        FDBTransaction* tr, uint8_t const* begin_key_name,
        int begin_key_name_length, uint8_t const* end_key_name,
        int end_key_name_length, int limit );

    DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_transaction_get_range_selector(
        FDBTransaction* tr, uint8_t const* begin_key_name,
        int begin_key_name_length, fdb_bool_t begin_or_equal,
        int begin_offset, uint8_t const* end_key_name,
        int end_key_name_length, fdb_bool_t end_or_equal, int end_offset,
        int limit );
#else
	#define fdb_transaction_get_range_selector(tr,bkn,bknl,boe,bo,ekn,eknl,eoe,eo,lim) FDB_REMOVED_FUNCTION
#endif

#ifdef __cplusplus
}
#endif
#endif
