/*
 * CWorkload.c
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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "foundationdb/CWorkload.h"

typedef struct CWorkload {
	char* name;
	int cliend_id;
	FDBWorkloadContext context;
} CWorkload;

#define BIND(W) CWorkload* this = (CWorkload*)W
#define WITH(C, M, ...) (C).M((C).inner, ##__VA_ARGS__)
#define EXPORT extern __attribute__((visibility("default")))

static void workload_setup(OpaqueWorkload* raw_workload, FDBDatabase* db, FDBPromise done) {
	BIND(raw_workload);
	printf("c_setup(%s_%d)\n", this->name, this->cliend_id);
	FDBStringPair details[2] = {
		{ .key = "Layer", .val = "C" },
		{ .key = "Stage", .val = "setup" },
	};
	WITH(this->context, trace, FDBSeverity_Debug, "Test", details, 2);
	WITH(done, send, true);
	WITH(done, free);
}
static void workload_start(OpaqueWorkload* raw_workload, FDBDatabase* db, FDBPromise done) {
	BIND(raw_workload);
	printf("c_start(%s_%d)\n", this->name, this->cliend_id);
	FDBStringPair details[2] = {
		{ .key = "Layer", .val = "C" },
		{ .key = "Stage", .val = "start" },
	};
	WITH(this->context, trace, FDBSeverity_Debug, "Test", details, 2);
	WITH(done, send, true);
	WITH(done, free);
}
static void workload_check(OpaqueWorkload* raw_workload, FDBDatabase* db, FDBPromise done) {
	BIND(raw_workload);
	printf("c_check(%s_%d)\n", this->name, this->cliend_id);
	FDBStringPair details[2] = {
		{ .key = "Layer", .val = "C" },
		{ .key = "Stage", .val = "check" },
	};
	WITH(this->context, trace, FDBSeverity_Debug, "Test", details, 2);
	WITH(done, send, true);
	WITH(done, free);
}
static void workload_getMetrics(OpaqueWorkload* raw_workload, FDBMetrics out) {
	BIND(raw_workload);
	printf("c_getMetrics(%s_%d)\n", this->name, this->cliend_id);
	WITH(out, reserve, 8);
	WITH(out, push, (FDBMetric){ .key = "test", .val = 42., .avg = false });
}
static double workload_getCheckTimeout(OpaqueWorkload* raw_workload) {
	BIND(raw_workload);
	printf("c_getCheckTimeout(%s_%d)\n", this->name, this->cliend_id);
	return 3000.;
};
static void workload_free(OpaqueWorkload* raw_workload) {
	BIND(raw_workload);
	printf("c_free(%s_%d)\n", this->name, this->cliend_id);
	free(this->name);
	free(this);
}

EXPORT FDBWorkload workloadCFactory(const char* borrow_name, FDBWorkloadContext context) {
	int len = strlen(borrow_name) + 1;
	char* name = (char*)malloc(len);
	memcpy(name, borrow_name, len);

	int client_id = WITH(context, clientId);
	int client_count = WITH(context, clientCount);
	printf("workloadCFactory(%s)[%d/%d]\n", name, client_id, client_count);

	FDBString my_c_option;
	my_c_option = WITH(context, getOption, "my_c_option", "null");
	printf("my_c_option: \"%s\"\n", my_c_option.inner);
	WITH(my_c_option, free);
	my_c_option = WITH(context, getOption, "my_c_option", "null");
	printf("my_c_option: \"%s\"\n", my_c_option.inner);
	WITH(my_c_option, free);

	CWorkload* workload = (CWorkload*)malloc(sizeof(CWorkload));
	workload->name = name;
	workload->cliend_id = client_id;
	workload->context = context;

	return (FDBWorkload){
		.inner = (OpaqueWorkload*)workload,
		.setup = workload_setup,
		.start = workload_start,
		.check = workload_check,
		.getMetrics = workload_getMetrics,
		.getCheckTimeout = workload_getCheckTimeout,
		.free = workload_free,
	};
}
