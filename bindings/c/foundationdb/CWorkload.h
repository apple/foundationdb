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

#define FDB_WORKLOAD_API_VERSION 1

typedef struct FDB_future FDBFuture;
typedef struct FDB_database FDBDatabase;

typedef struct Opaque_promise OpaquePromise;
typedef struct Opaque_workload OpaqueWorkload;
typedef struct Opaque_workloadContext OpaqueWorkloadContext;
typedef struct Opaque_metrics OpaqueMetrics;

// Pure C bindings, equivalent to the C++ bindings in `CppWorkload.h`
// documented here: https://apple.github.io/foundationdb/client-testing.html.

// API changes may add pointers at the end of the virtual tables (_VT)
// It is advised to never dereference or copy these structures, only using them through a pointer.

// Log severity levels.
// FDBSeverity_Error automatically stops the simulation.
typedef enum FDBSeverity {
	FDBSeverity_Debug,
	FDBSeverity_Info,
	FDBSeverity_Warn,
	FDBSeverity_WarnAlways,
	FDBSeverity_Error,
} FDBSeverity;

// Key-value pair for logging additional details.
typedef struct FDBStringPair {
	const char* key;
	const char* val;
} FDBStringPair;

// Metric entry for simulation statistics.
// Values are either averaged or summed across clients based on the `avg` flag.
// `fmt` is used to format the value, defaults to "%.3g" if null.
typedef struct FDBMetric {
	const char* key;
	const char* fmt;
	double val;
	bool avg;
} FDBMetric;

// Wrapper around an owned C++ `string`.
// The `free` function must be called on `inner` before releasing the string.
typedef struct FDBString {
	const char* inner;
	struct FDBString_VT {
		void (*free)(const char* inner);
	}* vt;
} FDBString;

// Wrapper around a borrowed C++ `vector<FDBMetric>`.
typedef struct FDBMetrics {
	OpaqueMetrics* inner;
	struct FDBMetrics_VT {
		void (*reserve)(OpaqueMetrics* inner, int n);
		void (*push)(OpaqueMetrics* inner, FDBMetric val);
	}* vt;
} FDBMetrics;

// Wrapper around an owned C++ `GenericPromise<bool>`.
// Calling `send` resolves the promise (the value is meaningless).
// Calling `free` before resolving the promise triggers a "broken promise" error.
typedef struct FDBPromise {
	OpaquePromise* inner;
	struct FDBPromise_VT {
		void (*free)(OpaquePromise* inner);
		void (*send)(OpaquePromise* inner, bool val);
	}* vt;
} FDBPromise;

// Wrapper around a borrowed `ExternalWorkload`'s context.
// All pointer-based arguments are borrowed and managed by the workload.
typedef struct FDBWorkloadContext {
	int api_version;
	OpaqueWorkloadContext* inner;
	struct FDBWorkloadContext_VT {
		// Log a message with severity and optional details.
		// `details` is an array of key-value pairs, and `n` specifies the array size.
		void (*trace)(OpaqueWorkloadContext* inner,
		              FDBSeverity sev,
		              const char* name,
		              const FDBStringPair* details,
		              int n);
		uint64_t (*getProcessID)(OpaqueWorkloadContext* inner);
		void (*setProcessID)(OpaqueWorkloadContext* inner, uint64_t processID);
		// Return the current simulated time in seconds (starts at zero).
		double (*now)(OpaqueWorkloadContext* inner);
		// Return a random number (different each time and for all clients).
		uint32_t (*rnd)(OpaqueWorkloadContext* inner);
		// Return an option by name, returning `defaultValue` if the option is not found.
		// Return an option, consumming it, querying it again returns the empty string.
		FDBString (*getOption)(OpaqueWorkloadContext* inner, const char* name, const char* defaultValue);
		int (*clientId)(OpaqueWorkloadContext* inner);
		int (*clientCount)(OpaqueWorkloadContext* inner);
		// Return a random seed (same each time and for all clients).
		int64_t (*sharedRandomNumber)(OpaqueWorkloadContext* inner);
		// Return a future that will be ready after a given time. This internally uses TaskPriority::DefaultDelay.
		FDBFuture* (*delay)(OpaqueWorkloadContext* inner, double seconds);
	}* vt;
} FDBWorkloadContext;

// Interface for a workload implementation in C.
//
// Simulation stages (setup, start, and check) are executed sequentially.
// A stage finishes when its associated promise is resolved. If a promise
// is not resolved or freed, the simulation hangs.
//
// Workload functions should not block. If an operation must wait for database interaction,
// it should initiate the action, register a callback, and return.
typedef struct FDBWorkload {
	int api_version;
	OpaqueWorkload* inner;
	struct FDBWorkload_VT {
		void (*free)(OpaqueWorkload* inner);
		void (*setup)(OpaqueWorkload* inner, FDBDatabase* db, FDBPromise done);
		void (*start)(OpaqueWorkload* inner, FDBDatabase* db, FDBPromise done);
		void (*check)(OpaqueWorkload* inner, FDBDatabase* db, FDBPromise done);
		void (*getMetrics)(OpaqueWorkload* inner, FDBMetrics out);
		// The timeout in simulated seconds for the check stage
		double (*getCheckTimeout)(OpaqueWorkload* inner);
	}* vt;
} FDBWorkload;

// The C workload entrypoint.
// This function must return a valid instance of the `FDBWorkload` struct.
// A specific implementation can be chosen based on the name passed.
// The client C workload must be allocated, initialized and passed as pointer
// in `inner`. It is advised to store the context alongside the workload as
// it can't be retrieved later. No function pointer can be left null.
FDBWorkload workloadCFactory(const char* name, FDBWorkloadContext context);

#endif
