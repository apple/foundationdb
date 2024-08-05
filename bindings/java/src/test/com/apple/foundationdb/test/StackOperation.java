/*
 * StackOperation.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.test;

enum StackOperation {
	PUSH,
	POP,
	DUP,
	EMPTY_STACK,
	SWAP,
	WAIT_EMPTY,
	START_THREAD,
	WAIT_FUTURE,
	NEW_TRANSACTION,
	USE_TRANSACTION,
	SET,
	CLEAR,
	CLEAR_RANGE,
	CLEAR_RANGE_STARTS_WITH,
	ATOMIC_OP,

	// explicit conflict ranges...
	READ_CONFLICT_RANGE,
	WRITE_CONFLICT_RANGE,
	READ_CONFLICT_KEY,
	WRITE_CONFLICT_KEY,
	DISABLE_WRITE_CONFLICT,

	COMMIT,
	RESET,
	CANCEL,
	GET,
	GET_RANGE,
	GET_RANGE_SELECTOR,
	GET_RANGE_STARTS_WITH,
	GET_KEY,
	GET_READ_VERSION,
	GET_COMMITTED_VERSION,
	GET_APPROXIMATE_SIZE,
	GET_VERSIONSTAMP,
	GET_ESTIMATED_RANGE_SIZE,
	GET_RANGE_SPLIT_POINTS,
	SET_READ_VERSION,
	ON_ERROR,
	SUB,
	CONCAT,
	TUPLE_PACK,
	TUPLE_PACK_WITH_VERSIONSTAMP,
	TUPLE_UNPACK,
	TUPLE_RANGE,
	TUPLE_SORT,
	ENCODE_FLOAT,
	ENCODE_DOUBLE,
	DECODE_FLOAT,
	DECODE_DOUBLE,
	UNIT_TESTS, /* Possibly unimplemented */

	// Tenants
	TENANT_CREATE,
	TENANT_DELETE,
	TENANT_LIST,
	TENANT_SET_ACTIVE,
	TENANT_CLEAR_ACTIVE,
	TENANT_GET_ID,

	LOG_STACK
}
