/*
 * CWorkload.h
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

#pragma once
#ifndef C_WORKLOAD_H
#define C_WORKLOAD_H

#include <stdint.h>
#include <stdbool.h>

typedef struct FDB_database FDBDatabase;
typedef struct Opaque_promise OpaquePromise;
typedef struct Opaque_workload OpaqueWorkload;
typedef struct Opaque_workloadContext OpaqueWorkloadContext;
typedef struct Opaque_metrics OpaqueMetrics;

typedef enum FDBSeverity {
	FDBSeverity_Debug,
	FDBSeverity_Info,
	FDBSeverity_Warn,
	FDBSeverity_WarnAlways,
	FDBSeverity_Error,
} FDBSeverity;

typedef struct FDBStringPair {
	const char* key;
	const char* val;
} FDBStringPair;

typedef struct FDBMetric {
	const char* key;
	const char* fmt;
	double val;
	bool avg;
} FDBMetric;

typedef struct FDBString {
	const char* inner;
	void (*free)(const char* inner);
} FDBString;

typedef struct FDBMetrics {
	OpaqueMetrics* inner;
	void (*reserve)(OpaqueMetrics* inner, int n);
	void (*push)(OpaqueMetrics* inner, FDBMetric val);
} FDBMetrics;

typedef struct FDBPromise {
	OpaquePromise* inner;
	void (*send)(OpaquePromise* inner, bool val);
	void (*free)(OpaquePromise* inner);
} FDBPromise;

typedef struct FDBWorkloadContext {
	OpaqueWorkloadContext* inner;
	void (*trace)(OpaqueWorkloadContext* inner, FDBSeverity sev, const char* name, const FDBStringPair* details, int n);
	uint64_t (*getProcessID)(OpaqueWorkloadContext* inner);
	void (*setProcessID)(OpaqueWorkloadContext* inner, uint64_t processID);
	double (*now)(OpaqueWorkloadContext* inner);
	uint32_t (*rnd)(OpaqueWorkloadContext* inner);
	FDBString (*getOption)(OpaqueWorkloadContext* inner, const char* name, const char* defaultValue);
	int (*clientId)(OpaqueWorkloadContext* inner);
	int (*clientCount)(OpaqueWorkloadContext* inner);
	int64_t (*sharedRandomNumber)(OpaqueWorkloadContext* inner);
} FDBWorkloadContext;

typedef struct FDBWorkload {
	OpaqueWorkload* inner;
	void (*setup)(OpaqueWorkload* inner, FDBDatabase* db, FDBPromise done);
	void (*start)(OpaqueWorkload* inner, FDBDatabase* db, FDBPromise done);
	void (*check)(OpaqueWorkload* inner, FDBDatabase* db, FDBPromise done);
	void (*getMetrics)(OpaqueWorkload* inner, FDBMetrics out);
	double (*getCheckTimeout)(OpaqueWorkload* inner);
	void (*free)(OpaqueWorkload* inner);
} FDBWorkload;

#endif
