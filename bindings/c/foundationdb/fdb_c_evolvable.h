/*
 * fdb_c_evolvable.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDB_C_EVOLVABLE_H
#define FDB_C_EVOLVABLE_H
#pragma once

#ifndef DLLEXPORT
#define DLLEXPORT
#endif

#ifdef __GNUG__
#define WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
#define WARN_UNUSED_RESULT
#endif

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define FDB_API_INVALID_REQUEST (0)
#define FDB_API_READ_BG_DESCRIPTION_REQUEST (1)

typedef struct FDBRequestHeader_ FDBRequestHeader;
typedef struct FDBResponseHeader_ FDBResponseHeader;

typedef struct FDBReadBGDescriptionRequest_ {
	FDBRequestHeader* header;
	FDBKeyRange key_range;
	int64_t begin_version;
	int64_t read_version;
} FDBReadBGDescriptionRequest;

typedef struct FDBReadBGDescriptionResponse_ {
	FDBResponseHeader* header;
	FDBBGFileDescription** desc_arr;
	int desc_count;
	int64_t read_version;
} FDBReadBGDescriptionResponse;

DLLEXPORT WARN_UNUSED_RESULT void* fdb_copy_to_request(FDBRequestHeader* request_header,
                                                       const void* begin,
                                                       uint64_t length);

DLLEXPORT WARN_UNUSED_RESULT FDBReadBGDescriptionRequest* fdb_create_read_bg_description_request(FDBTransaction* tx);

DLLEXPORT WARN_UNUSED_RESULT FDBFuture* fdb_execute_read_bg_description_request(FDBTransaction* tx,
                                                                                FDBReadBGDescriptionRequest* request);

DLLEXPORT WARN_UNUSED_RESULT fdb_error_t
fdb_future_get_read_bg_description_response(FDBFuture* f, FDBReadBGDescriptionResponse** out);

#ifdef __cplusplus
}
#endif
#endif