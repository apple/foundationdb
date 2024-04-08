/*
 * CWorkload.c
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

#define FDB_USE_LATEST_API_VERSION
#include <stdlib.h>
#include "foundationdb/fdb_c.h"
#include "foundationdb/ClienWorkload.h"

typedef struct CWorkload {
    BridgeToServer cpp;
    FDBWorkloadContext* context;
} CWorkload;

void workload_setup(CWorkload* workload, FDBDatabase* db, FDBPromise* done) {
    CStringPair pairs[2] = {
        {.key = "Layer", .val = "C"},
        {.key = "Stage", .val = "setup"},
    };
    CVector details = { .n = 2, .elements = pairs };
    workload->cpp.context.trace(workload->context, FDBSeverity_Debug, "Test", details);
    workload->cpp.promise.send(done, true);
    workload->cpp.promise.free(done);
}
void workload_start(CWorkload* workload, FDBDatabase* db, FDBPromise* done) {
    CStringPair pairs[2] = {
        {.key = "Layer", .val = "C"},
        {.key = "Stage", .val = "start"},
    };
    CVector details = { .n = 2, .elements = pairs };
    workload->cpp.context.trace(workload->context, FDBSeverity_Debug, "Test", details);
    workload->cpp.promise.send(done, true);
    workload->cpp.promise.free(done);
}
void workload_check(CWorkload* workload, FDBDatabase* db, FDBPromise* done) {
    CStringPair pairs[2] = {
        {.key = "Layer", .val = "C"},
        {.key = "Stage", .val = "check"},
    };
    CVector details = { .n = 2, .elements = pairs };
    workload->cpp.context.trace(workload->context, FDBSeverity_Debug, "Test", details);
    workload->cpp.promise.send(done, true);
    workload->cpp.promise.free(done);
}
CVector workload_getMetrics(CWorkload* workload) {
    CVector metrics = { 0 };
    return metrics;
}
double workload_getCheckTimeout(CWorkload* workload) {
    return 3000.;
};
void workload_free(CWorkload* workload) {
    free(workload);
}

BridgeToClient workloadInstantiate(const char* name, FDBWorkloadContext* context, BridgeToServer bridgeToServer) {
    int status = fdb_select_api_version(FDB_API_VERSION);
    // if (status != 0) {
    //     fdb_get_error(status);
    // }

    CWorkload* workload = (CWorkload*)malloc(sizeof(CWorkload));
    workload->cpp = bridgeToServer;
    workload->context = context;

    BridgeToClient bridgeToClient = {
        .workload = workload,
        .setup = workload_setup,
        .start = workload_start,
        .check = workload_check,
        .getMetrics = workload_getMetrics,
        .getCheckTimeout = workload_getCheckTimeout,
        .free = workload_free,
    };
    return bridgeToClient;
}
