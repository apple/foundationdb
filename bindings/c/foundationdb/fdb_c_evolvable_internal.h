/*
 * fdb_c_evolvable_internal.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDB_C_EVOLVABLE_INTERNAL_H
#define FDB_C_EVOLVABLE_INTERNAL_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FDBAllocatorIfc_ {
	void* (*allocate)(void** handle, uint64_t sz);
	void (*addref)(void* handle);
	void (*delref)(void* handle);
} FDBAllocatorIfc;

typedef struct FDBAllocator_ {
	void* handle;
	FDBAllocatorIfc* ifc;
} FDBAllocator;

typedef struct FDBRequestHeader_ {
	FDBAllocator allocator;
	int32_t request_type;
} FDBRequestHeader;

typedef struct FDBRequest_ {
	FDBRequestHeader* header;
} FDBRequest;

typedef struct FDBResponseHeader_ {
	FDBAllocator allocator;
	int32_t request_type;
} FDBResponseHeader;

typedef struct FDBResponse_ {
	FDBResponseHeader* header;
} FDBResponse;

DLLEXPORT FDBAllocatorIfc* fdb_get_allocator_interface();

DLLEXPORT FDBFuture* fdb_transaction_exec_async(FDBTransaction* tx, FDBRequest* request);

DLLEXPORT fdb_error_t fdb_future_get_response(FDBFuture* f, FDBResponse** response);

#ifdef __cplusplus
}
#endif
#endif