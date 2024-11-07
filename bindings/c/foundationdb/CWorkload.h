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

typedef struct FDB_future FDBFuture;
typedef struct FDB_result FDBResult;
typedef struct FDB_database FDBDatabase;
typedef struct FDB_transaction FDBTransaction;

typedef struct FDB_promise FDBPromise;
typedef struct FDB_workload FDBWorkload;
typedef struct FDB_workloadContext FDBWorkloadContext;

typedef enum FDBSeverity {
	FDBSeverity_Debug,
	FDBSeverity_Info,
	FDBSeverity_Warn,
	FDBSeverity_WarnAlways,
	FDBSeverity_Error,
} FDBSeverity;

typedef struct CVector {
	int n;
	void* elements;
} CVector;

typedef struct CStringPair {
	const char* key;
	const char* val;
} CStringPair;

typedef struct CMetric {
	const char* name;
	const char* format_code;
	double value;
	bool averaged;
} CMetric;

/**
 * BridgeToClient - A structure the client has to pass to the fdbserver so it
 * can interact with the workload it defines.
 *
 * This structure includes:
 * - An opaque pointer to the client-defined workload.
 * - A set of function pointers that operate on the workload, with the workload
 *   itself as the first parameter (emulating C++ method calls).
 *
 * All methods, except `free`, directly map to corresponding C++ methods in
 * `CppWorkload.h`, documented here: https://apple.github.io/foundationdb/client-testing.html.
 *
 * Notes:
 * - `FDBWorkload::init` is not defined in this struct, initialization should be
 *   done during workload creation.
 * - The `FDBWorkload` pointer passed in each method corresponds to the pointer
 *   the client sets in `BridgeToClient::workload`, allowing it to be safely cast
 *   back to its original type.
 */
typedef struct BridgeToClient {
	FDBWorkload* workload;
	void (*setup)(FDBWorkload*, FDBDatabase*, FDBPromise*);
	void (*start)(FDBWorkload*, FDBDatabase*, FDBPromise*);
	void (*check)(FDBWorkload*, FDBDatabase*, FDBPromise*);
	CVector (*getMetrics)(FDBWorkload*);
	double (*getCheckTimeout)(FDBWorkload*);
	void (*free)(FDBWorkload*);
} BridgeToClient;

/**
 * BridgeToServer - A structure the client will receive at initialisation
 * to interact with the FDBContext and FDBPromise.
 *
 * Each C workload must define the following entry point:
 * extern BridgeToClient workloadInstantiate(char* name, FDBWorkloadContext* ctx, BridgeToServer bridge);
 * This allows the fdbserver to pass to the workload's name, context and bridge.
 * A workload instance must be created, initialized and returned as a pointer
 * in the BridgeToClient structure. The context and bridge should be stored, as
 * they cannot be accessed otherwise
 *
 * The ContextImpl and PromiseImpl sub-structures store the function pointers
 * corresponding to the C++ methods of FDBContext and FDBPromise respectively,
 * defined in CppWorkload.h
 */
typedef struct BridgeToServer {
	struct ContextImpl {
		void (*trace)(FDBWorkloadContext* context, FDBSeverity severity, const char* name, CVector vec);
		uint64_t (*getProcessID)(FDBWorkloadContext* context);
		void (*setProcessID)(FDBWorkloadContext* context, uint64_t processID);
		double (*now)(FDBWorkloadContext* context);
		uint32_t (*rnd)(FDBWorkloadContext* context);
		char* (*getOption)(FDBWorkloadContext* context, const char* name, const char* defaultValue);
		int (*clientId)(FDBWorkloadContext* context);
		int (*clientCount)(FDBWorkloadContext* context);
		int64_t (*sharedRandomNumber)(FDBWorkloadContext* context);
	} context;

	struct PromiseImpl {
		void (*send)(FDBPromise* promise, bool value);
		void (*free)(FDBPromise* promise);
	} promise;
} BridgeToServer;

#endif
