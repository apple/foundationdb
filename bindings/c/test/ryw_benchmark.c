/*
 * ryw_benchmark.c
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

#include "test.h"
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

pthread_t netThread;

int numKeys = 10000;
int keySize = 16;
uint8_t** keys;

void insertData(FDBTransaction* tr) {
	fdb_transaction_clear_range(tr, (uint8_t*)"", 0, (uint8_t*)"\xff", 1);

	uint8_t* v = (uint8_t*)"foo";
	uint32_t i;
	for (i = 0; i <= numKeys; ++i) {
		fdb_transaction_set(tr, keys[i], keySize, v, 3);
	}
}

int runTest(int (*testFxn)(FDBTransaction*, struct ResultSet*),
            FDBTransaction* tr,
            struct ResultSet* rs,
            const char* kpiName) {
	int numRuns = 25;
	int* results = malloc(sizeof(int) * numRuns);
	int i = 0;
	for (; i < numRuns; ++i) {
		results[i] = testFxn(tr, rs);
		if (results[i] < 0) {
			free(results);
			return -1;
		}
	}

	int result = median(results, numRuns);
	free(results);

	addKpi(rs, kpiName, result, "keys/s");

	return result;
}

int getSingle(FDBTransaction* tr, struct ResultSet* rs) {
	int present;
	uint8_t const* value;
	int length;
	int i;

	double start = getTime();
	for (i = 0; i < numKeys; ++i) {
		FDBFuture* f = fdb_transaction_get(tr, keys[5001], keySize, 0);
		if (getError(fdb_future_block_until_ready(f), "GetSingle (block for get)", rs))
			return -1;
		if (getError(fdb_future_get_value(f, &present, &value, &length), "GetSingle (get result)", rs))
			return -1;
		fdb_future_destroy(f);
	}
	double end = getTime();

	return numKeys / (end - start);
}

int getManySequential(FDBTransaction* tr, struct ResultSet* rs) {
	int present;
	uint8_t const* value;
	int length;
	int i;

	double start = getTime();
	for (i = 0; i < numKeys; ++i) {
		FDBFuture* f = fdb_transaction_get(tr, keys[i], keySize, 0);
		if (getError(fdb_future_block_until_ready(f), "GetManySequential (block for get)", rs))
			return -1;
		if (getError(fdb_future_get_value(f, &present, &value, &length), "GetManySequential (get result)", rs))
			return -1;
		fdb_future_destroy(f);
	}
	double end = getTime();

	return numKeys / (end - start);
}

int getRangeBasic(FDBTransaction* tr, struct ResultSet* rs) {
	int count;
	const FDBKeyValue* kvs;
	int more;
	int i;

	double start = getTime();
	for (i = 0; i < 100; ++i) {
		FDBFuture* f = fdb_transaction_get_range(tr,
		                                         FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[0], keySize),
		                                         FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[numKeys], keySize),
		                                         numKeys,
		                                         0,
		                                         0,
		                                         1,
		                                         0,
		                                         0);

		if (getError(fdb_future_block_until_ready(f), "GetRangeBasic (block for get range)", rs))
			return -1;
		if (getError(fdb_future_get_keyvalue_array(f, &kvs, &count, &more), "GetRangeBasic (get range results)", rs))
			return -1;

		if (count != numKeys) {
			fprintf(stderr, "Bad count %d (expected %d)\n", count, numKeys);
			addError(rs, "GetRangeBasic bad count");
			return -1;
		}
	}
	double end = getTime();

	return 100 * numKeys / (end - start);
}

int singleClearGetRange(FDBTransaction* tr, struct ResultSet* rs) {
	int count;
	const FDBKeyValue* kvs;
	int more;
	int i;

	for (i = 0; i < numKeys; i += 2) {
		fdb_transaction_clear(tr, keys[i], keySize);
	}

	double start = getTime();
	for (i = 0; i < 100; ++i) {
		FDBFuture* f = fdb_transaction_get_range(tr,
		                                         FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[0], keySize),
		                                         FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[numKeys], keySize),
		                                         numKeys,
		                                         0,
		                                         0,
		                                         1,
		                                         0,
		                                         0);

		if (getError(fdb_future_block_until_ready(f), "SingleClearGetRange (block for get range)", rs))
			return -1;
		if (getError(
		        fdb_future_get_keyvalue_array(f, &kvs, &count, &more), "SingleClearGetRange (get range results)", rs))
			return -1;

		fdb_future_destroy(f);

		if (count != numKeys / 2) {
			fprintf(stderr, "Bad count %d (expected %d)\n", count, numKeys);
			addError(rs, "SingleClearGetRange bad count");
			return -1;
		}
	}
	double end = getTime();

	insertData(tr);
	return 100 * numKeys / 2 / (end - start);
}

int clearRangeGetRange(FDBTransaction* tr, struct ResultSet* rs) {
	int count;
	const FDBKeyValue* kvs;
	int more;
	int i;

	for (i = 0; i < numKeys; i += 4) {
		fdb_transaction_clear_range(tr, keys[i], keySize, keys[i + 1], keySize);
	}

	double start = getTime();
	for (i = 0; i < 100; ++i) {
		FDBFuture* f = fdb_transaction_get_range(tr,
		                                         FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[0], keySize),
		                                         FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[numKeys], keySize),
		                                         numKeys,
		                                         0,
		                                         0,
		                                         1,
		                                         0,
		                                         0);

		if (getError(fdb_future_block_until_ready(f), "ClearRangeGetRange (block for get range)", rs))
			return -1;
		if (getError(
		        fdb_future_get_keyvalue_array(f, &kvs, &count, &more), "ClearRangeGetRange (get range results)", rs))
			return -1;

		fdb_future_destroy(f);

		if (count != numKeys * 3 / 4) {
			fprintf(stderr, "Bad count %d (expected %d)\n", count, numKeys * 3 / 4);
			addError(rs, "ClearRangeGetRange bad count");
			return -1;
		}
	}
	double end = getTime();

	insertData(tr);
	return 100 * numKeys * 3 / 4 / (end - start);
}

int interleavedSetsGets(FDBTransaction* tr, struct ResultSet* rs) {
	int present;
	uint8_t const* value;
	int length;
	int i;

	uint8_t* k = (uint8_t*)"foo";
	uint8_t v[10];
	int num = 1;

	double start = getTime();
	sprintf((char*)v, "%d", num);
	fdb_transaction_set(tr, k, 3, v, strlen((char*)v));

	for (i = 0; i < 10000; ++i) {
		FDBFuture* f = fdb_transaction_get(tr, k, 3, 0);
		if (getError(fdb_future_block_until_ready(f), "InterleavedSetsGets (block for get)", rs))
			return -1;
		if (getError(fdb_future_get_value(f, &present, &value, &length), "InterleavedSetsGets (get result)", rs))
			return -1;
		fdb_future_destroy(f);

		sprintf((char*)v, "%d", ++num);
		fdb_transaction_set(tr, k, 3, v, strlen((char*)v));
	}
	double end = getTime();

	return 10000 / (end - start);
}

void runTests(struct ResultSet* rs) {
	FDBDatabase* db = openDatabase(rs, &netThread);

	FDBTransaction* tr;
	checkError(fdb_database_create_transaction(db, &tr), "create transaction", rs);

	FDBFuture* f = fdb_transaction_get_read_version(tr);
	checkError(fdb_future_block_until_ready(f), "block for read version", rs);

	int64_t version;
	checkError(fdb_future_get_int64(f, &version), "get version", rs);
	fdb_future_destroy(f);

	insertData(tr);

	runTest(&getSingle, tr, rs, "C: get single cached value throughput");
	runTest(&getManySequential, tr, rs, "C: get sequential cached values throughput");
	runTest(&getRangeBasic, tr, rs, "C: get range cached values throughput");
	runTest(&singleClearGetRange, tr, rs, "C: get range cached values with clears throughput");
	runTest(&clearRangeGetRange, tr, rs, "C: get range cached values with clear ranges throughput");
	runTest(&interleavedSetsGets, tr, rs, "C: interleaved sets and gets on a single key throughput");

	fdb_transaction_destroy(tr);
	fdb_database_destroy(db);
	fdb_stop_network();
}

int main(int argc, char** argv) {
	srand(time(NULL));
	struct ResultSet* rs = newResultSet();
	checkError(fdb_select_api_version(720), "select API version", rs);
	printf("Running RYW Benchmark test at client version: %s\n", fdb_get_client_version());

	keys = generateKeys(numKeys, keySize);
	runTests(rs);
	writeResultSet(rs);
	freeResultSet(rs);
	freeKeys(keys, numKeys);

	return 0;
}
