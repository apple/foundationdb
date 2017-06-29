/*
 * test.h
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

#include <sys/time.h>
#include <arpa/inet.h>

#include <stdio.h>
#include <stdlib.h>

double getTime() {
	static struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_usec/1000000.0 + tv.tv_sec;
}

int getError(int err, const char* context) {
	if(err) {
		fprintf(stderr, "Error in %s: %d\n", context, err);
	}

	return err;
}

void checkError(int err, const char* context) {
	if(getError(err, context)) {
		exit(1);
	}
}

int cmpfunc(const void* a, const void* b) {
	return (*(int*)a - *(int*)b);
}

int median(int *values, int length) {
	qsort(values, length, sizeof(int), cmpfunc);
	return values[length/2];
}

struct Kpi {
	const char *name;
	int value;
	const char *units;

	struct Kpi *next;
};

struct Error {
	const char *message;

	struct Error *next;
};

struct ResultSet {
	struct Kpi *kpis;
	struct Error *errors;
};

struct ResultSet* newResultSet() {
	struct ResultSet *rs = malloc(sizeof(struct ResultSet));

	rs->kpis = NULL;
	rs->errors = NULL;

	return rs;
}

void addKpi(struct ResultSet *rs, const char *name, int value, const char *units) {
	struct Kpi *k = malloc(sizeof(struct Kpi));
	k->name = name;
	k->value = value;
	k->units = units;
	k->next = rs->kpis;
	rs->kpis = k;
}

void addError(struct ResultSet *rs, const char *message) {
	struct Error *e = malloc(sizeof(struct Error));
	e->message = message;
	e->next = rs->errors;
	rs->errors = e;
}

void writeResultSet(struct ResultSet *rs) {
	srand(time(NULL)); // TODO: move this?
	uint64_t id = ((uint64_t)rand() << 32) + rand();
	char name[100];
	sprintf(name, "fdb-c_result-%llu.json", id);
	FILE *fp = fopen(name, "w");
	if(!fp) {
		fprintf(stderr, "Could not open results file %s\n", name);
		exit(1);
	}

	fprintf(fp, "{\n");
	fprintf(fp, "\t\"kpis\": {\n");

	struct Kpi *k = rs->kpis;
	while(k != NULL) {
		fprintf(fp, "\t\t\"%s\": { \"units\": \"%s\", \"value\": %d }", k->name, k->units, k->value);
		if(k->next != NULL) {
			fprintf(fp, ",");
		}
		fprintf(fp, "\n");
		k = k->next;
	}

	fprintf(fp, "\t},\n");
	fprintf(fp, "\t\"errors\": [\n");

	struct Error *e = rs->errors;
	while(e != NULL) {
		fprintf(fp, "\t\t%s", e->message);
		if(e->next != NULL) {
			fprintf(fp, ",");
		}
		fprintf(fp, "\n");
		e = e->next;
	}

	fprintf(fp, "\t]\n");
	fprintf(fp, "}\n");

	fclose(fp);
}

