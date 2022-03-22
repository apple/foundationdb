/*
 * test.h
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

#include <sys/time.h>
#include <arpa/inet.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include <inttypes.h>

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 710
#endif

#include <foundationdb/fdb_c.h>
#include <foundationdb/fdb_c_options.g.h>

double getTime() {
	static struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_usec / 1000000.0 + tv.tv_sec;
}

void writeKey(uint8_t** dest, int key, int keySize) {
	*dest = (uint8_t*)malloc((sizeof(uint8_t)) * keySize);
	sprintf((char*)*dest, "%0*d", keySize, key);
}

uint8_t** generateKeys(int numKeys, int keySize) {
	uint8_t** keys = (uint8_t**)malloc(sizeof(uint8_t*) * (numKeys + 1));

	uint32_t i;
	for (i = 0; i <= numKeys; ++i) {
		writeKey(keys + i, i, keySize);
	}

	return keys;
}
void freeKeys(uint8_t** keys, int numKeys) {
	uint32_t i;
	for (i = 0; i < numKeys; i++) {
		free(keys[i]);
	}
	free(keys);
}

int cmpfunc(const void* a, const void* b) {
	return (*(int*)a - *(int*)b);
}

int median(int* values, int length) {
	qsort(values, length, sizeof(int), cmpfunc);
	return values[length / 2];
}

struct RunResult {
	int res;
	fdb_error_t e;
};
#define RES(x, y)                                                                                                      \
	(struct RunResult) { x, y }

struct Kpi {
	const char* name;
	int value;
	const char* units;

	struct Kpi* next;
};

struct Error {
	char* message;

	struct Error* next;
};

struct ResultSet {
	struct Kpi* kpis;
	struct Error* errors;
};

struct ResultSet* newResultSet() {
	struct ResultSet* rs = malloc(sizeof(struct ResultSet));

	rs->kpis = NULL;
	rs->errors = NULL;

	return rs;
}

void addKpi(struct ResultSet* rs, const char* name, int value, const char* units) {
	struct Kpi* k = malloc(sizeof(struct Kpi));
	k->name = name;
	k->value = value;
	k->units = units;
	k->next = rs->kpis;
	rs->kpis = k;
}

void addError(struct ResultSet* rs, const char* message) {
	struct Error* e = malloc(sizeof(struct Error));
	e->message = (char*)malloc(strlen(message) + 1);
	strcpy(e->message, message);
	e->next = rs->errors;
	rs->errors = e;
}

void writeResultSet(struct ResultSet* rs) {
	uint64_t id = ((uint64_t)rand() << 32) + rand();
	char name[100];
	sprintf(name, "fdb-c_result-%" SCNu64 ".json", id);
	FILE* fp = fopen(name, "w");
	if (!fp) {
		fprintf(stderr, "Could not open results file %s\n", name);
		exit(1);
	}

	fprintf(fp, "{\n");
	fprintf(fp, "\t\"kpis\": {\n");

	struct Kpi* k = rs->kpis;
	while (k != NULL) {
		fprintf(fp, "\t\t\"%s\": { \"units\": \"%s\", \"value\": %d }", k->name, k->units, k->value);
		if (k->next != NULL) {
			fprintf(fp, ",");
		}
		fprintf(fp, "\n");
		k = k->next;
	}

	fprintf(fp, "\t},\n");
	fprintf(fp, "\t\"errors\": [\n");

	struct Error* e = rs->errors;
	while (e != NULL) {
		fprintf(fp, "\t\t\"%s\"", e->message);
		if (e->next != NULL) {
			fprintf(fp, ",");
		}
		fprintf(fp, "\n");
		e = e->next;
	}

	fprintf(fp, "\t]\n");
	fprintf(fp, "}\n");

	fclose(fp);
}

void freeResultSet(struct ResultSet* rs) {
	struct Kpi* k = rs->kpis;
	while (k != NULL) {
		struct Kpi* next = k->next;
		free(k);
		k = next;
	}

	struct Error* e = rs->errors;
	while (e != NULL) {
		struct Error* next = e->next;
		free(e->message);
		free(e);
		e = next;
	}

	free(rs);
}

fdb_error_t getError(fdb_error_t err, const char* context, struct ResultSet* rs) {
	if (err) {
		char* msg = (char*)malloc(strlen(context) + 100);
		sprintf(msg, "Error in %s: %s", context, fdb_get_error(err));
		fprintf(stderr, "%s\n", msg);
		if (rs != NULL) {
			addError(rs, msg);
		}

		free(msg);
	}

	return err;
}

void checkError(fdb_error_t err, const char* context, struct ResultSet* rs) {
	if (getError(err, context, rs)) {
		if (rs != NULL) {
			writeResultSet(rs);
			freeResultSet(rs);
		}
		exit(1);
	}
}

fdb_error_t logError(fdb_error_t err, const char* context, struct ResultSet* rs) {
	char* msg = (char*)malloc(strlen(context) + 100);
	sprintf(msg, "Error in %s: %s", context, fdb_get_error(err));
	fprintf(stderr, "%s\n", msg);
	if (rs != NULL) {
		addError(rs, msg);
	}

	free(msg);
	return err;
}

fdb_error_t maybeLogError(fdb_error_t err, const char* context, struct ResultSet* rs) {
	if (err && !fdb_error_predicate(FDB_ERROR_PREDICATE_RETRYABLE, err)) {
		return logError(err, context, rs);
	}
	return err;
}

void* runNetwork() {
	checkError(fdb_run_network(), "run network", NULL);
	return NULL;
}

FDBDatabase* openDatabase(struct ResultSet* rs, pthread_t* netThread) {
	checkError(fdb_setup_network(), "setup network", rs);
	pthread_create(netThread, NULL, (void*)(&runNetwork), NULL);

	FDBDatabase* db;
	checkError(fdb_create_database(NULL, &db), "create database", rs);

	return db;
}
