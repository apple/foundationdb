/*
 * fdb_c_types.h
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

#ifndef FDB_C_TYPES_H
#define FDB_C_TYPES_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
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

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Pointers to these opaque types represent objects in the FDB API */
typedef struct FDB_future FDBFuture;
typedef struct FDBResult_ FDBResult;
typedef struct FDB_cluster FDBCluster;
typedef struct FDB_database FDBDatabase;
typedef struct FDB_tenant FDBTenant;
typedef struct FDB_transaction FDBTransaction;

typedef int fdb_error_t;
typedef int fdb_bool_t;

#pragma pack(push, 4)

typedef struct FDBByteString_ {
	const uint8_t* begin;
	int length;
} FDBByteString;

typedef struct FDBKey_ {
	const uint8_t* key;
	int key_length;
} FDBKey;

#if FDB_API_VERSION >= 630

typedef struct FDBKeyValue_ {
	const uint8_t* key;
	int key_length;
	const uint8_t* value;
	int value_length;
} FDBKeyValue;

#else

typedef struct FDBKeyValue_ {
	const void* key;
	int key_length;
	const void* value;
	int value_length;
} FDBKeyValue;

#endif

typedef struct FDBKeyRange_ {
	const uint8_t* begin_key;
	int begin_key_length;
	const uint8_t* end_key;
	int end_key_length;
} FDBKeyRange;

typedef struct FDBGranuleSummary_ {
	FDBKeyRange key_range;
	int64_t snapshot_version;
	int64_t snapshot_size;
	int64_t delta_version;
	int64_t delta_size;
} FDBGranuleSummary;

typedef struct FDBBGTenantPrefix_ {
	fdb_bool_t present;
	FDBKey prefix;
} FDBBGTenantPrefix;

/* encryption structs correspond to similar ones in BlobGranuleCommon.h */
typedef struct FDBBGEncryptionKey_ {
	int64_t domain_id;
	uint64_t base_key_id;
	uint32_t base_kcv;
	uint64_t random_salt;
	FDBKey base_key;
} FDBBGEncryptionKey;

typedef enum { FDB_BG_MUTATION_TYPE_SET_VALUE = 0, FDB_BG_MUTATION_TYPE_CLEAR_RANGE = 1 } FDBBGMutationType;

typedef struct FDBBGMutation_ {
	/* FDBBGMutationType */ uint8_t type;
	int64_t version;
	const uint8_t* param1_ptr;
	int param1_length;
	const uint8_t* param2_ptr;
	int param2_length;
} FDBBGMutation;

/* ---------------------------------------------------------------------------------
 *	Legacy blob granule API structures, to be deprecated in the next major release
 */

typedef struct FDBBGEncryptionCtxV1_ {
	fdb_bool_t present;
	FDBBGEncryptionKey textKey;
	uint32_t textKCV;
	FDBBGEncryptionKey headerKey;
	uint32_t headerKCV;
	FDBKey iv;
} FDBBGEncryptionCtxV1;

typedef struct FDBBGFilePointerV1_ {
	const uint8_t* filename_ptr;
	int filename_length;
	int64_t file_offset;
	int64_t file_length;
	int64_t full_file_length;
	int64_t file_version;
	FDBBGEncryptionCtxV1 encryption_ctx;
} FDBBGFilePointerV1;

typedef struct FDBBGFileDescriptionV1_ {
	FDBKeyRange key_range;
	fdb_bool_t snapshot_present;
	FDBBGFilePointerV1 snapshot_file_pointer;
	int delta_file_count;
	FDBBGFilePointerV1* delta_files;
	int memory_mutation_count;
	FDBBGMutation* memory_mutations;
	FDBBGTenantPrefix tenant_prefix;
} FDBBGFileDescriptionV1;

/* ---------------------------------------------------------------------------------
 *	New blob granule API structures
 *  (Changed to referecence extensible data structures by pointers)
 */

// contains metadata necessary to derive encryption keys (for passing back to FDB)
typedef struct FDBBGEncryptionCtxV2_ {
	FDBBGEncryptionKey* textKey;
	uint32_t textKCV;
	FDBBGEncryptionKey* headerKey;
	uint32_t headerKCV;
	FDBKey iv;
} FDBBGEncryptionCtxV2;

// contains actual encryption keys (for external use if necessary)
typedef struct FDBBGEncryptionKeys_ {
	FDBKey headerKey;
	FDBKey textKey;
} FDBBGEncryptionKeys;

typedef struct FDBBGFilePointerV2_ {
	const uint8_t* filename_ptr;
	int filename_length;
	int64_t file_offset;
	int64_t file_length;
	int64_t full_file_length;
	int64_t file_version;
	FDBBGEncryptionCtxV2* encryption_ctx;
	FDBBGEncryptionKeys* encryption_keys;
} FDBBGFilePointerV2;

typedef struct FDBBGFileDescriptionV2_ {
	FDBKeyRange key_range;
	FDBBGFilePointerV2* snapshot_file_pointer;
	int delta_file_count;
	FDBBGFilePointerV2** delta_files;
	int memory_mutation_count;
	FDBBGMutation** memory_mutations;
	FDBBGTenantPrefix tenant_prefix;
} FDBBGFileDescriptionV2;

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

#ifdef __cplusplus
}
#endif
#endif
