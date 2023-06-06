/*
 * fdb_c_requests.h
 *
 * Copyright (c) 2023 Snowflake Computing
 */

#ifndef FDB_C_REQUESTS_H
#define FDB_C_REQUESTS_H
#pragma once

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

enum FDBApiRequestType {
	FDBApiRequest_Invalid = 0, //
	FDBApiRequest_ReadBGDescriptionRequest = 1 //
};

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

#ifdef __cplusplus
}
#endif
#endif