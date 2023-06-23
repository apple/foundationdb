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
#pragma once

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------------------------
 *  Database state sharing among multiple client threads
 */

// forward declaration and typedef
typedef struct DatabaseSharedState DatabaseSharedState;

DLLEXPORT FDBFuture* fdb_database_create_shared_state(FDBDatabase* db);

DLLEXPORT void fdb_database_set_shared_state(FDBDatabase* db, DatabaseSharedState* p);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t fdb_future_get_shared_state(FDBFuture* f, DatabaseSharedState** outPtr);

DLLEXPORT void fdb_use_future_protocol_version();

/* -------------------------------------------------------------------------------------------
 *  Internal evolvable API
 */

// Each FDB client instance provides an allocation interface. It provides a way to allocate
// memory and hold it by allocator (aka arena) handles. The memory is never deallocated explicilty,
// but is rather released automatically when the last reference to it is released.
//
// allocate: Allocates memory block of a given size and returns a reference to it. If passed handle
//           is null, creates a new area, otherwise attaches memory to the existing arena. The pointer
//           is changed if a new arena block is allocated. The allocation is not thread safe, so it is
//           intended that all allocations on a certain arena are made in one thread. After that, the
//           arena must stay immutable.
//
// addref/delref: Thread-safe functions for resp. increasing and decreasing reference count to the arena
//
typedef struct FDBAllocatorIfc_ {
	void* (*allocate)(void** handle, uint64_t sz);
	void (*addref)(void* handle);
	void (*delref)(void* handle);
} FDBAllocatorIfc;

// Allocator reference consisting of a handle and an interface to work with that handle
typedef struct FDBAllocator_ {
	void* handle;
	FDBAllocatorIfc* ifc;
} FDBAllocator;

typedef struct FDBRequestHeader_ {
	FDBAllocator allocator;
	int32_t request_type;
} FDBRequestHeader;

// A generic reference to an API request
// It can be casted to a specific request structure matching the request_type (see fdb_c_requests.h)
// The request owns its memory by holding a reference to its allocator
typedef struct FDBRequest_ {
	FDBRequestHeader* header;
} FDBRequest;

typedef struct FDBResultHeader_ {
	FDBAllocator allocator;
	int32_t result_type;
} FDBResultHeader;

// A generic reference to an API result. Synchronous API functions return it directly, while
// asynchronous API functions return futures providing FDBResult
// It can be casted to a specific result structure matching the result_type (see fdb_c_requests.h)
// The result owns its memory by holding a reference to its allocator
typedef struct FDBResult_ {
	FDBResultHeader* header;
} FDBResult;

// Get the allocator interface of the client instance
DLLEXPORT FDBAllocatorIfc* fdb_get_allocator_interface();

// Execute an asynchronous request on a FDB transaction. Returns a future providing FDBResult
// The request must be allocated using the allocator interface of the same client instance
DLLEXPORT FDBFuture* fdb_transaction_exec_async(FDBTransaction* tx, FDBRequest* request);

// Extract FDBResult from the future. Returns an error if the future is not ready
// or resulted in an error. The returned FDBResult maintains a reference to its memory,
// so it can be accessed also after the future is destroyed. It must be destroyed
// using fdb_result_destroy
DLLEXPORT fdb_error_t fdb_future_get_result(FDBFuture* f, FDBResult** result);

#ifdef __cplusplus
}
#endif

#endif
