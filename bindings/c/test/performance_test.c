/*
 * performance_test.c
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

#include <stdio.h>
#include <pthread.h>

pthread_t netThread;

int numKeys = 1000000;
int keySize = 16;
uint8_t** keys = NULL;
int valueSize = 100;
uint8_t* valueStr = NULL;

fdb_error_t waitError(FDBFuture* f) {
	fdb_error_t blockError = fdb_future_block_until_ready(f);
	if (!blockError) {
		return fdb_future_get_error(f);
	} else {
		return blockError;
	}
}

struct RunResult run(struct ResultSet* rs,
                     FDBDatabase* db,
                     struct RunResult (*func)(struct ResultSet*, FDBTransaction*)) {
	FDBTransaction* tr = NULL;
	fdb_error_t e = fdb_database_create_transaction(db, &tr);
	checkError(e, "create transaction", rs);

	while (1) {
		struct RunResult r = func(rs, tr);
		e = r.e;
		if (!e) {
			FDBFuture* f = fdb_transaction_commit(tr);
			e = waitError(f);
			fdb_future_destroy(f);
		}

		if (e) {
			FDBFuture* f = fdb_transaction_on_error(tr, e);
			fdb_error_t retryE = waitError(f);
			fdb_future_destroy(f);
			if (retryE) {
				fdb_transaction_destroy(tr);
				return (struct RunResult){ 0, retryE };
			}
		} else {
			fdb_transaction_destroy(tr);
			return r;
		}
	}

	return RES(0, 4100); // internal_error ; we should never get here
}

int runTest(struct RunResult (*testFxn)(struct ResultSet*, FDBTransaction*),
            FDBDatabase* db,
            struct ResultSet* rs,
            const char* kpiName) {
	int numRuns = 25;
	int* results = malloc(sizeof(int) * numRuns);
	int i = 0;
	for (; i < numRuns; ++i) {
		struct RunResult res = run(rs, db, testFxn);
		if (res.e) {
			logError(res.e, kpiName, rs);
			free(results);
			return 0;
		}
		results[i] = res.res;
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

int runTestDb(struct RunResult (*testFxn)(struct ResultSet*, FDBDatabase*),
              FDBDatabase* db,
              struct ResultSet* rs,
              const char* kpiName) {
	int numRuns = 25;
	int* results = malloc(sizeof(int) * numRuns);
	int i = 0;
	for (; i < numRuns; ++i) {
		struct RunResult res = testFxn(rs, db);
		if (res.e) {
			logError(res.e, kpiName, rs);
			free(results);
			return 0;
		}
		results[i] = res.res;
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

struct RunResult clearAll(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_transaction_clear_range(tr, (uint8_t*)"", 0, (uint8_t*)"\xff", 1);
	return RES(0, 0);
}

uint32_t start = 0;
uint32_t stop = 0;
struct RunResult insertRange(struct ResultSet* rs, FDBTransaction* tr) {
	int i;
	for (i = start; i < stop; i++) {
		fdb_transaction_set(tr, keys[i], keySize, valueStr, valueSize);
	}
	return RES(0, 0);
}

void insertData(struct ResultSet* rs, FDBDatabase* db) {
	checkError(run(rs, db, &clearAll).e, "clearing database", rs);

	// TODO: Do this asynchronously.
	start = 0;
	while (start < numKeys) {
		stop = start + 1000;
		if (stop > numKeys)
			stop = numKeys;
		checkError(run(rs, db, &insertRange).e, "inserting data range", rs);
		start = stop;
	}
}

fdb_error_t setRetryLimit(struct ResultSet* rs, FDBTransaction* tr, uint64_t limit) {
	return fdb_transaction_set_option(tr, FDB_TR_OPTION_RETRY_LIMIT, (const uint8_t*)&limit, sizeof(uint64_t));
}

uint32_t FUTURE_LATENCY_COUNT = 100000;
const char* FUTURE_LATENCY_KPI = "C future throughput (local client)";
struct RunResult futureLatency(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	FDBFuture* f = fdb_transaction_get_read_version(tr);
	e = waitError(f);
	fdb_future_destroy(f);
	maybeLogError(e, "getting initial read version", rs);
	if (e)
		return RES(0, e);

	double start = getTime();
	int i;
	for (i = 0; i < FUTURE_LATENCY_COUNT; i++) {
		FDBFuture* f = fdb_transaction_get_read_version(tr);
		e = waitError(f);
		fdb_future_destroy(f);
		maybeLogError(e, "getting read version", rs);
		if (e)
			return RES(0, e);
	}
	double end = getTime();

	return RES(FUTURE_LATENCY_COUNT / (end - start), 0);
}

uint32_t CLEAR_COUNT = 100000;
const char* CLEAR_KPI = "C clear throughput (local client)";
struct RunResult clear(struct ResultSet* rs, FDBTransaction* tr) {
	double start = getTime();
	int i;
	for (i = 0; i < CLEAR_COUNT; i++) {
		int k = ((uint64_t)rand()) % numKeys;
		fdb_transaction_clear(tr, keys[k], keySize);
	}
	double end = getTime();

	fdb_transaction_reset(tr); // Don't actually clear things.
	return RES(CLEAR_COUNT / (end - start), 0);
}

uint32_t CLEAR_RANGE_COUNT = 100000;
const char* CLEAR_RANGE_KPI = "C clear range throughput (local client)";
struct RunResult clearRange(struct ResultSet* rs, FDBTransaction* tr) {
	double start = getTime();
	int i;
	for (i = 0; i < CLEAR_RANGE_COUNT; i++) {
		int k = ((uint64_t)rand()) % (numKeys - 1);
		fdb_transaction_clear_range(tr, keys[k], keySize, keys[k + 1], keySize);
	}
	double end = getTime();

	fdb_transaction_reset(tr); // Don't actually clear things.
	return RES(CLEAR_RANGE_COUNT / (end - start), 0);
}

uint32_t SET_COUNT = 100000;
const char* SET_KPI = "C set throughput (local client)";
struct RunResult set(struct ResultSet* rs, FDBTransaction* tr) {
	double start = getTime();
	int i;
	for (i = 0; i < SET_COUNT; i++) {
		int k = ((uint64_t)rand()) % numKeys;
		fdb_transaction_set(tr, keys[k], keySize, valueStr, valueSize);
	}
	double end = getTime();

	fdb_transaction_reset(tr); // Don't actually set things.
	return RES(SET_COUNT / (end - start), 0);
}

uint32_t PARALLEL_GET_COUNT = 10000;
const char* PARALLEL_GET_KPI = "C parallel get throughput (local client)";
struct RunResult parallelGet(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	FDBFuture** futures = (FDBFuture**)malloc((sizeof(FDBFuture*)) * PARALLEL_GET_COUNT);

	double start = getTime();

	int i;
	for (i = 0; i < PARALLEL_GET_COUNT; i++) {
		int k = ((uint64_t)rand()) % numKeys;
		futures[i] = fdb_transaction_get(tr, keys[k], keySize, 0);
	}

	fdb_bool_t present;
	uint8_t const* outValue;
	int outValueLength;

	for (i = 0; i < PARALLEL_GET_COUNT; i++) {
		e = maybeLogError(fdb_future_block_until_ready(futures[i]), "waiting for get future", rs);
		if (e) {
			fdb_future_destroy(futures[i]);
			return RES(0, e);
		}

		e = maybeLogError(
		    fdb_future_get_value(futures[i], &present, &outValue, &outValueLength), "getting future value", rs);
		if (e) {
			fdb_future_destroy(futures[i]);
			return RES(0, e);
		}

		fdb_future_destroy(futures[i]);
	}

	double end = getTime();

	free(futures);
	return RES(PARALLEL_GET_COUNT / (end - start), 0);
}

uint32_t ALTERNATING_GET_SET_COUNT = 2000;
const char* ALTERNATING_GET_SET_KPI = "C alternating get set throughput (local client)";
struct RunResult alternatingGetSet(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	FDBFuture** futures = (FDBFuture**)malloc((sizeof(FDBFuture*)) * ALTERNATING_GET_SET_COUNT);

	double start = getTime();

	int i;
	for (i = 0; i < ALTERNATING_GET_SET_COUNT; i++) {
		int k = ((uint64_t)rand()) % numKeys;
		fdb_transaction_set(tr, keys[k], keySize, valueStr, valueSize);
		futures[i] = fdb_transaction_get(tr, keys[k], keySize, 0);
	}

	fdb_bool_t present;
	uint8_t const* outValue;
	int outValueLength;

	for (i = 0; i < ALTERNATING_GET_SET_COUNT; i++) {
		e = maybeLogError(fdb_future_block_until_ready(futures[i]), "waiting for get future", rs);
		if (e) {
			fdb_future_destroy(futures[i]);
			return RES(0, e);
		}

		e = maybeLogError(
		    fdb_future_get_value(futures[i], &present, &outValue, &outValueLength), "getting future value", rs);
		if (e) {
			fdb_future_destroy(futures[i]);
			return RES(0, e);
		}

		fdb_future_destroy(futures[i]);
	}

	double end = getTime();

	free(futures);
	return RES(ALTERNATING_GET_SET_COUNT / (end - start), 0);
}

uint32_t SERIAL_GET_COUNT = 2000;
const char* SERIAL_GET_KPI = "C serial get throughput (local client)";
struct RunResult serialGet(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	int i;
	uint32_t* keyIndices = (uint32_t*)malloc((sizeof(uint32_t)) * SERIAL_GET_COUNT);

	if (SERIAL_GET_COUNT > numKeys / 2) {
		for (i = 0; i < SERIAL_GET_COUNT; i++) {
			keyIndices[i] = ((uint64_t)rand()) % numKeys;
		}
	} else {
		for (i = 0; i < SERIAL_GET_COUNT; i++) {
			while (1) {
				// Yes, this is a linear scan. This happens outside
				// the part we are measuring.
				uint32_t index = ((uint64_t)rand()) % numKeys;
				int j;
				fdb_bool_t found = 0;
				for (j = 0; j < i; j++) {
					if (keyIndices[j] == index) {
						found = 1;
						break;
					}
				}

				if (!found) {
					keyIndices[i] = index;
					break;
				}
			}
		}
	}

	double start = getTime();

	fdb_bool_t present;
	uint8_t const* outValue;
	int outValueLength;

	for (i = 0; i < SERIAL_GET_COUNT; i++) {
		FDBFuture* f = fdb_transaction_get(tr, keys[keyIndices[i]], keySize, 0);
		fdb_error_t e = maybeLogError(fdb_future_block_until_ready(f), "getting key in serial", rs);
		if (e) {
			free(keyIndices);
			fdb_future_destroy(f);
			return RES(0, e);
		}

		e = maybeLogError(fdb_future_get_value(f, &present, &outValue, &outValueLength), "getting future value", rs);
		fdb_future_destroy(f);
		if (e) {
			free(keyIndices);
			return RES(0, e);
		}
	}

	double end = getTime();

	free(keyIndices);
	return RES(SERIAL_GET_COUNT / (end - start), 0);
}

uint32_t GET_RANGE_COUNT = 100000;
const char* GET_RANGE_KPI = "C get range throughput (local client)";
struct RunResult getRange(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	uint32_t startKey = ((uint64_t)rand()) % (numKeys - GET_RANGE_COUNT - 1);

	double start = getTime();

	const FDBKeyValue* outKv;
	int outCount;
	fdb_bool_t outMore = 1;
	int totalOut = 0;
	int iteration = 0;

	FDBFuture* f = fdb_transaction_get_range(tr,
	                                         keys[startKey],
	                                         keySize,
	                                         1,
	                                         0,
	                                         keys[startKey + GET_RANGE_COUNT],
	                                         keySize,
	                                         1,
	                                         0,
	                                         0,
	                                         0,
	                                         FDB_STREAMING_MODE_WANT_ALL,
	                                         ++iteration,
	                                         0,
	                                         0);

	while (outMore) {
		e = maybeLogError(fdb_future_block_until_ready(f), "getting range", rs);
		if (e) {
			fdb_future_destroy(f);
			return RES(0, e);
		}

		e = maybeLogError(fdb_future_get_keyvalue_array(f, &outKv, &outCount, &outMore), "reading range array", rs);
		if (e) {
			fdb_future_destroy(f);
			return RES(0, e);
		}

		totalOut += outCount;

		if (outMore) {
			FDBFuture* f2 = fdb_transaction_get_range(tr,
			                                          outKv[outCount - 1].key,
			                                          outKv[outCount - 1].key_length,
			                                          1,
			                                          1,
			                                          keys[startKey + GET_RANGE_COUNT],
			                                          keySize,
			                                          1,
			                                          0,
			                                          0,
			                                          0,
			                                          FDB_STREAMING_MODE_WANT_ALL,
			                                          ++iteration,
			                                          0,
			                                          0);
			fdb_future_destroy(f);
			f = f2;
		}
	}

	if (totalOut != GET_RANGE_COUNT) {
		char* msg = (char*)malloc((sizeof(char)) * 200);
		sprintf(msg, "verifying out count (%d != %d)", totalOut, GET_RANGE_COUNT);
		logError(4100, msg, rs);
		free(msg);
		fdb_future_destroy(f);
		return RES(0, 4100);
	}
	if (outMore) {
		logError(4100, "verifying no more in range", rs);
		fdb_future_destroy(f);
		return RES(0, 4100);
	}
	fdb_future_destroy(f);

	double end = getTime();

	return RES(GET_RANGE_COUNT / (end - start), 0);
}

uint32_t GET_KEY_COUNT = 2000;
const char* GET_KEY_KPI = "C get key throughput (local client)";
struct RunResult getKey(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	double start = getTime();

	fdb_bool_t present;
	uint8_t const* outValue;
	int outValueLength;

	int i;
	for (i = 0; i < GET_KEY_COUNT; i++) {
		int key = ((uint64_t)rand()) % numKeys;
		int offset = (((uint64_t)rand()) % 21) - 10;
		FDBFuture* f = fdb_transaction_get_key(tr, keys[key], keySize, 1, offset, 0);

		e = maybeLogError(fdb_future_block_until_ready(f), "waiting for get key", rs);
		if (e) {
			fdb_future_destroy(f);
			return RES(0, e);
		}

		e = maybeLogError(fdb_future_get_value(f, &present, &outValue, &outValueLength), "getting future value", rs);
		fdb_future_destroy(f);
		if (e) {
			return RES(0, e);
		}
	}

	double end = getTime();

	return RES(GET_KEY_COUNT / (end - start), 0);
}

uint32_t GET_SINGLE_KEY_RANGE_COUNT = 2000;
const char* GET_SINGLE_KEY_RANGE_KPI = "C get_single_key_range throughput (local client)";
struct RunResult getSingleKeyRange(struct ResultSet* rs, FDBTransaction* tr) {
	fdb_error_t e = maybeLogError(setRetryLimit(rs, tr, 5), "setting retry limit", rs);
	if (e)
		return RES(0, e);

	double start = getTime();

	const FDBKeyValue* outKv;
	int outCount;
	fdb_bool_t outMore;

	int i;
	for (i = 0; i < GET_SINGLE_KEY_RANGE_COUNT; i++) {
		int key = ((uint64_t)rand()) % (numKeys - 1);
		FDBFuture* f = fdb_transaction_get_range(
		    tr, keys[key], keySize, 1, 0, keys[key + 1], keySize, 1, 0, 2, 0, FDB_STREAMING_MODE_EXACT, 1, 0, 0);

		e = maybeLogError(fdb_future_block_until_ready(f), "waiting for single key range", rs);
		if (e) {
			fdb_future_destroy(f);
			return RES(0, e);
		}

		e = maybeLogError(
		    fdb_future_get_keyvalue_array(f, &outKv, &outCount, &outMore), "reading single key range array", rs);
		if (e) {
			fdb_future_destroy(f);
			return RES(0, e);
		}

		if (outCount != 1) {
			logError(4100, "more than one key returned in single key range read", rs);
			fdb_future_destroy(f);
			return RES(0, 4100);
		}
		if (outMore) {
			logError(4100, "more keys to read in single key range read", rs);
			fdb_future_destroy(f);
			return RES(0, 4100);
		}

		fdb_future_destroy(f);
	}

	double end = getTime();

	return RES(GET_SINGLE_KEY_RANGE_COUNT / (end - start), 0);
}

struct RunResult singleKey(struct ResultSet* rs, FDBTransaction* tr) {
	int k = ((uint64_t)rand()) % numKeys;
	fdb_transaction_set(tr, keys[k], keySize, valueStr, valueSize);
	return RES(0, 0);
}

uint32_t WRITE_TRANSACTION_COUNT = 1000;
const char* WRITE_TRANSACTION_KPI = "C write_transaction throughput (local client)";
struct RunResult writeTransaction(struct ResultSet* rs, FDBDatabase* db) {
	double start = getTime();

	int i;
	for (i = 0; i < WRITE_TRANSACTION_COUNT; i++) {
		struct RunResult res = run(rs, db, &singleKey);
		if (res.e)
			return res;
	}

	double end = getTime();

	return RES(WRITE_TRANSACTION_COUNT / (end - start), 0);
}

void runTests(struct ResultSet* rs) {
	FDBDatabase* db = openDatabase(rs, &netThread);

	printf("Loading database...\n");
	insertData(rs, db);

	printf("future_latency\n");
	runTest(&futureLatency, db, rs, FUTURE_LATENCY_KPI);

	printf("clear\n");
	runTest(&clear, db, rs, CLEAR_KPI);

	printf("clear_range\n");
	runTest(&clearRange, db, rs, CLEAR_RANGE_KPI);

	printf("set\n");
	runTest(&set, db, rs, SET_KPI);

	printf("parallel_get\n");
	runTest(&parallelGet, db, rs, PARALLEL_GET_KPI);

	printf("alternating_get_set\n");
	runTest(&alternatingGetSet, db, rs, ALTERNATING_GET_SET_KPI);

	printf("serial_get\n");
	runTest(&serialGet, db, rs, SERIAL_GET_KPI);

	printf("get_range\n");
	runTest(&getRange, db, rs, GET_RANGE_KPI);

	printf("get_key\n");
	runTest(&getKey, db, rs, GET_KEY_KPI);

	printf("get_single_key_range\n");
	runTest(&getSingleKeyRange, db, rs, GET_SINGLE_KEY_RANGE_KPI);

	printf("write_transaction\n");
	runTestDb(&writeTransaction, db, rs, WRITE_TRANSACTION_KPI);

	fdb_database_destroy(db);
	fdb_stop_network();
}

int main(int argc, char** argv) {
	srand(time(NULL));
	struct ResultSet* rs = newResultSet();
	checkError(fdb_select_api_version(720), "select API version", rs);
	printf("Running performance test at client version: %s\n", fdb_get_client_version());

	valueStr = (uint8_t*)malloc((sizeof(uint8_t)) * valueSize);
	int i;
	for (i = 0; i < valueSize; i++) {
		valueStr[i] = (uint8_t)'x';
	}

	keys = generateKeys(numKeys, keySize);
	runTests(rs);
	writeResultSet(rs);

	free(valueStr);
	freeResultSet(rs);
	freeKeys(keys, numKeys);

	return 0;
}
