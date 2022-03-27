/*
 * txn_size_test.c
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "test.h"

#include <assert.h>
#include <stdio.h>
#include <pthread.h>

#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

pthread_t netThread;
const int numKeys = 100;
uint8_t** keys = NULL;

#define KEY_SIZE 16
#define VALUE_SIZE 100
uint8_t valueStr[VALUE_SIZE];

fdb_error_t getSize(struct ResultSet* rs, FDBTransaction* tr, int64_t* out_size) {
	fdb_error_t e;
	FDBFuture* future = fdb_transaction_get_approximate_size(tr);

	e = maybeLogError(fdb_future_block_until_ready(future), "waiting for get future", rs);
	if (e) {
		fdb_future_destroy(future);
		return e;
	}

	e = maybeLogError(fdb_future_get_int64(future, out_size), "getting future value", rs);
	if (e) {
		fdb_future_destroy(future);
		return e;
	}

	fdb_future_destroy(future);
	return 0;
}

void runTests(struct ResultSet* rs) {
	int64_t sizes[numKeys];
	int i = 0, j = 0;
	FDBDatabase* db = openDatabase(rs, &netThread);
	FDBTransaction* tr = NULL;
	fdb_error_t e = fdb_database_create_transaction(db, &tr);
	checkError(e, "create transaction", rs);
	memset(sizes, 0, numKeys * sizeof(uint32_t));

	fdb_transaction_set(tr, keys[i], KEY_SIZE, valueStr, VALUE_SIZE);
	e = getSize(rs, tr, sizes + i);
	checkError(e, "transaction get size", rs);
	printf("size %d: %" PRId64 "\n", i, sizes[i]);
	i++;

	fdb_transaction_set(tr, keys[i], KEY_SIZE, valueStr, VALUE_SIZE);
	e = getSize(rs, tr, sizes + i);
	checkError(e, "transaction get size", rs);
	printf("size %d: %" PRId64 "\n", i, sizes[i]);
	i++;

	fdb_transaction_clear(tr, keys[i], KEY_SIZE);
	e = getSize(rs, tr, sizes + i);
	checkError(e, "transaction get size", rs);
	printf("size %d: %" PRId64 "\n", i, sizes[i]);
	i++;

	fdb_transaction_clear_range(tr, keys[i], KEY_SIZE, keys[i + 1], KEY_SIZE);
	e = getSize(rs, tr, sizes + i);
	checkError(e, "transaction get size", rs);
	printf("size %d: %" PRId64 "\n", i, sizes[i]);
	i++;

	for (j = 0; j + 1 < i; j++) {
		assert(sizes[j] < sizes[j + 1]);
	}
	printf("Test passed!\n");
}

int main(int argc, char** argv) {
	srand(time(NULL));
	struct ResultSet* rs = newResultSet();
	checkError(fdb_select_api_version(710), "select API version", rs);
	printf("Running performance test at client version: %s\n", fdb_get_client_version());

	keys = generateKeys(numKeys, KEY_SIZE);
	runTests(rs);

	freeResultSet(rs);
	freeKeys(keys, numKeys);

	return 0;
}
