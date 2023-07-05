/*
 * fdb_c_requests.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#ifndef FDB_C_REQUESTS_H
#define FDB_C_REQUESTS_H
#pragma once

#include "fdb_c_types.h"

#ifdef __cplusplus
extern "C" {
#endif

enum FDBApiRequestType {
	FDBApiRequest_Invalid = 0, //
	FDBApiRequest_ReadBGDescription = 1 //
};

enum FDBApiResultType {
	FDBApiResult_Invalid = 0, //
	FDBApiResult_Error = -1, //
	FDBApiResult_ReadBGDescription = 1, //
	FDBApiResult_ReadRange = 2, //
	FDBApiResult_ReadBGMutations = 3, //
};

typedef struct FDBRequestHeader_ FDBRequestHeader;
typedef struct FDBResultHeader_ FDBResultHeader;

typedef struct FDBReadBGDescriptionRequest_ {
	FDBRequestHeader* header;
	FDBKeyRange key_range;
	int64_t begin_version;
	int64_t read_version;
} FDBReadBGDescriptionRequest;

typedef struct FDBErrorResult_ {
	FDBResultHeader* header;
	fdb_error_t error;
} FDBErrorResult;

typedef struct FDBReadBGDescriptionResult_ {
	FDBResultHeader* header;
	FDBBGFileDescriptionV2** desc_arr;
	int desc_count;
	int64_t read_version;
} FDBReadBGDescriptionResult;

typedef struct FDBReadRangeResult_ {
	FDBResultHeader* header;
	FDBKeyValue* kv_arr;
	int kv_count;
	fdb_bool_t more;
} FDBReadRangeResult;

typedef struct FDBReadBGMutationsResult_ {
	FDBResultHeader* header;
	FDBBGMutation* mutation_arr;
	int mutation_count;
} FDBReadBGMutationsResult;

#ifdef __cplusplus
}
#endif
#endif