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

#define FDB_API_VERSION 500
#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

pthread_t netThread;

void preload(FDBTransaction *tr, int numKeys) {
	fdb_transaction_clear_range(tr, (uint8_t*)"", 0, (uint8_t*)"\xff", 1);

	uint32_t i;
	for(i = 0; i < numKeys; ++i) {
		uint32_t k = htonl(i);
		fdb_transaction_set(tr, (uint8_t*)&k, 4, (uint8_t*)&k, 4);
	}
}

void* runNetwork() {
	checkError(fdb_run_network(), "run network");
	return NULL;
}

FDBDatabase* openDatabase() {
	checkError(fdb_setup_network(), "setup network");
	pthread_create(&netThread, NULL, &runNetwork, NULL);

	FDBFuture *f = fdb_create_cluster(NULL);
	checkError(fdb_future_block_until_ready(f), "block for cluster");

	FDBCluster *cluster;
	checkError(fdb_future_get_cluster(f, &cluster), "get cluster");

	fdb_future_destroy(f);

	f = fdb_cluster_create_database(cluster, (uint8_t*)"DB", 2);
	checkError(fdb_future_block_until_ready(f), "block for database");

	FDBDatabase *db;
	checkError(fdb_future_get_database(f, &db), "get database");

	fdb_future_destroy(f);
	fdb_cluster_destroy(cluster);

	return db;
}

int numKeys = 10000;
int keySize = 16;
uint8_t** keys;

void populateKeys() {
	keys = (uint8_t**)malloc(sizeof(uint8_t*)*(numKeys+1));

	uint32_t i;
	for(i = 0; i <= numKeys; ++i) {
		keys[i] = malloc(keySize);
		sprintf((char*)keys[i], "%0*d", keySize, i);
	}
}

void insertData(FDBTransaction *tr) {
	fdb_transaction_clear_range(tr, (uint8_t*)"", 0, (uint8_t*)"\xff", 1);

	uint8_t *v = (uint8_t*)"foo";
	uint32_t i;
	for(i = 0; i <= numKeys; ++i) {
		fdb_transaction_set(tr, keys[i], keySize, v, 3);
	}
}

int runTest(int (*testFxn)(FDBTransaction*), FDBTransaction *tr, struct ResultSet *rs, const char *kpiName) {
	int numRuns = 1;
	int *results = malloc(sizeof(int)*numRuns);
	int i = 0;
	for(; i < numRuns; ++i) {
		results[i] = testFxn(tr);
	}

	int result = median(results, numRuns);
	free(results);

	addKpi(rs, kpiName, result, "keys/s");

	return result;
}

int getSingle(FDBTransaction *tr) {
	int present;
	uint8_t const *value;
	int length;
	int i;

	double start = getTime();
	for(i = 0; i < numKeys; ++i) {
		FDBFuture *f = fdb_transaction_get(tr, keys[5001], keySize, 0);
		checkError(fdb_future_block_until_ready(f), "block for get");
		checkError(fdb_future_get_value(f, &present, &value, &length), "get result");
		fdb_future_destroy(f);
	}
	double end = getTime();

	return numKeys / (end - start);
}

int getManySequential(FDBTransaction *tr) {
	int present;
	uint8_t const *value;
	int length;
	int i;

	double start = getTime();
	for(i = 0; i < numKeys; ++i) {
		FDBFuture *f = fdb_transaction_get(tr, keys[i], keySize, 0);
		checkError(fdb_future_block_until_ready(f), "block for get");
		checkError(fdb_future_get_value(f, &present, &value, &length), "get result");
		fdb_future_destroy(f);
	}
	double end = getTime();

	return numKeys / (end - start);
}

int getRangeBasic(FDBTransaction *tr) {
	int count;
	const FDBKeyValue *kvs;
	int more;
	int i;

	double start = getTime();
	for(i = 0; i < 100; ++i) {
		FDBFuture *f = fdb_transaction_get_range(tr, FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[0], keySize), FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[numKeys], keySize), numKeys, 0, 0, 1, 0, 0);

		checkError(fdb_future_block_until_ready(f), "block for get range");
		checkError(fdb_future_get_keyvalue_array(f, &kvs, &count, &more), "get range results");

		if(count != numKeys) {
			fprintf(stderr, "Bad count %d (expected %d)\n", count, numKeys);
		}
	}
	double end = getTime();

	return 100 * numKeys / (end - start);
}

int singleClearGetRange(FDBTransaction *tr) {
	int count;
	const FDBKeyValue *kvs;
	int more;
	int i;

	for(i = 0; i < numKeys; i+=2) {
		fdb_transaction_clear(tr, keys[i], keySize);
	}

	double start = getTime();
	for(i = 0; i < 100; ++i) {
		FDBFuture *f = fdb_transaction_get_range(tr, FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[0], keySize), FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[numKeys], keySize), numKeys, 0, 0, 1, 0, 0);

		checkError(fdb_future_block_until_ready(f), "block for get range");
		checkError(fdb_future_get_keyvalue_array(f, &kvs, &count, &more), "get range results");

		fdb_future_destroy(f);

		if(count != numKeys/2) {
			fprintf(stderr, "Bad count %d (expected %d)\n", count, numKeys);
		}
	}
	double end = getTime();

	insertData(tr);
	return 100 * numKeys / 2 / (end - start);
}

int clearRangeGetRange(FDBTransaction *tr) {
	int count;
	const FDBKeyValue *kvs;
	int more;
	int i;

	for(i = 0; i < numKeys; i+=4) {
		fdb_transaction_clear_range(tr, keys[i], keySize, keys[i+1], keySize);
	}

	double start = getTime();
	for(i = 0; i < 100; ++i) {
		FDBFuture *f = fdb_transaction_get_range(tr, FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[0], keySize), FDB_KEYSEL_LAST_LESS_OR_EQUAL(keys[numKeys], keySize), numKeys, 0, 0, 1, 0, 0);

		checkError(fdb_future_block_until_ready(f), "block for get range");
		checkError(fdb_future_get_keyvalue_array(f, &kvs, &count, &more), "get range results");

		fdb_future_destroy(f);

		if(count != numKeys*3/4) {
			fprintf(stderr, "Bad count %d (expected %d)\n", count, numKeys*3/4);
		}
	}
	double end = getTime();

	insertData(tr);
	return 100 * numKeys * 3 / 4 / (end - start);
}

int interleavedSetsGets(FDBTransaction *tr) {
	int present;
	uint8_t const *value;
	int length;
	int i;

	uint8_t *k = (uint8_t*)"foo";
	uint8_t v[10];
	int num = 1;

	double start = getTime();
	sprintf((char*)v, "%d", num);
	fdb_transaction_set(tr, k, 3, v, strlen((char*)v));

	for(i = 0; i < 10000; ++i) {
		FDBFuture *f = fdb_transaction_get(tr, k, 3, 0);
		checkError(fdb_future_block_until_ready(f), "block for get");
		checkError(fdb_future_get_value(f, &present, &value, &length), "get result");
		fdb_future_destroy(f);

		sprintf((char*)v, "%d", ++num);
		fdb_transaction_set(tr, k, 3, v, strlen((char*)v));
	}
	double end = getTime();

	return 10000 / (end - start);
}

struct ResultSet* runTests() {
	struct ResultSet *rs = newResultSet();
	FDBDatabase *db = openDatabase();

	FDBTransaction *tr;
	checkError(fdb_database_create_transaction(db, &tr), "create transaction");

	FDBFuture *f = fdb_transaction_get_read_version(tr);
	checkError(fdb_future_block_until_ready(f), "block for read version");

	int64_t version;
	checkError(fdb_future_get_version(f, &version), "get version");
	fdb_future_destroy(f);

	insertData(tr);

	runTest(&getSingle, tr, rs, "C: get single cached value throughput");
	runTest(&getManySequential, tr, rs, "C: get sequential cached values throughput");
	runTest(&getRangeBasic, tr, rs, "C: get range cached values throughput");
	runTest(&singleClearGetRange, tr, rs, "C: get range cached values with clears throughput");
	runTest(&clearRangeGetRange, tr, rs, "C: get range cached values with clear ranges throughput");
	runTest(&interleavedSetsGets, tr, rs, "C: interleaved sets and gets on a single key throughput");

	fdb_database_destroy(db);
	fdb_stop_network();

	return rs;
}

int main(int argc, char **argv) {
	checkError(fdb_select_api_version(500), "select API version");
	printf("Running RYW Benchmark test at client version: %s\n", fdb_get_client_version());

	populateKeys();
	struct ResultSet *rs = runTests();
	writeResultSet(rs);
}

