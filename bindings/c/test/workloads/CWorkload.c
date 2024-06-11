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

#include <stdlib.h>
#include <stdio.h>
// #define FDB_USE_LATEST_API_VERSION
// #include "foundationdb/fdb_c.h"
#include "foundationdb/CWorkload.h"

typedef struct CWorkload {
    char* name;
    int cliendId;
    BridgeToServer cpp;
    FDBWorkloadContext* context;
} CWorkload;

#define GET_CWORKLOAD(NAME, W) CWorkload* NAME = (CWorkload*)W

static void workload_setup(FDBWorkload* raw_workload, FDBDatabase* db, FDBPromise* done) {
    GET_CWORKLOAD(workload, raw_workload);
    printf("workload_setup(%s_%d)\n", workload->name, workload->cliendId);
    CStringPair pairs[2] = {
        {.key = "Layer", .val = "C"},
        {.key = "Stage", .val = "setup"},
    };
    CVector details = { .n = 2, .elements = pairs };
    workload->cpp.context.trace(workload->context, FDBSeverity_Debug, "Test", details);
    workload->cpp.promise.send(done, true);
    workload->cpp.promise.free(done);
}
static void workload_start(FDBWorkload* raw_workload, FDBDatabase* db, FDBPromise* done) {
    GET_CWORKLOAD(workload, raw_workload);
    printf("workload_start(%s_%d)\n", workload->name, workload->cliendId);
    CStringPair pairs[2] = {
        {.key = "Layer", .val = "C"},
        {.key = "Stage", .val = "start"},
    };
    CVector details = { .n = 2, .elements = pairs };
    workload->cpp.context.trace(workload->context, FDBSeverity_Debug, "Test", details);
    workload->cpp.promise.send(done, true);
    workload->cpp.promise.free(done);
}
static void workload_check(FDBWorkload* raw_workload, FDBDatabase* db, FDBPromise* done) {
    GET_CWORKLOAD(workload, raw_workload);
    printf("workload_check(%s_%d)\n", workload->name, workload->cliendId);
    CStringPair pairs[2] = {
        {.key = "Layer", .val = "C"},
        {.key = "Stage", .val = "check"},
    };
    CVector details = { .n = 2, .elements = pairs };
    workload->cpp.context.trace(workload->context, FDBSeverity_Debug, "Test", details);
    workload->cpp.promise.send(done, true);
    workload->cpp.promise.free(done);
}
static CVector workload_getMetrics(FDBWorkload* raw_workload) {
    GET_CWORKLOAD(workload, raw_workload);
    printf("workload_getMetrics(%s_%d)\n", workload->name, workload->cliendId);
    CVector metrics = { 0 };
    return metrics;
}
static double workload_getCheckTimeout(FDBWorkload* raw_workload) {
    GET_CWORKLOAD(workload, raw_workload);
    printf("workload_getCheckTimeout(%s_%d)\n", workload->name, workload->cliendId);
    return 3000.;
};
static void workload_free(FDBWorkload* raw_workload) {
    GET_CWORKLOAD(workload, raw_workload);
    printf("workload_free(%s_%d)\n", workload->name, workload->cliendId);
    free(workload->name);
    free(workload);
}

extern BridgeToClient workloadInstantiate(char* name, FDBWorkloadContext* context, BridgeToServer bridgeToServer) {
    // int status = fdb_select_api_version(FDB_API_VERSION);
    // if (status != 0) {
    //     fdb_get_error(status);
    // }

    char* configValue = bridgeToServer.context.getOption(context, "my_c_option", "null");
    int clientId = bridgeToServer.context.clientId(context);
    int clientCount = bridgeToServer.context.clientCount(context);

    printf("workloadInstantiate(%s, %p)[%d/%d]\n", name, context, client_id, clientCount);
    printf("my_c_option: %s\n", configValue);
    free(configValue);

    CWorkload* workload = (CWorkload*)malloc(sizeof(CWorkload));
    workload->name = name;
    workload->cliendId = clientId;
    workload->cpp = bridgeToServer;
    workload->context = context;

    BridgeToClient bridgeToClient = {
        .workload = (FDBWorkload*)workload,
        .setup = workload_setup,
        .start = workload_start,
        .check = workload_check,
        .getMetrics = workload_getMetrics,
        .getCheckTimeout = workload_getCheckTimeout,
        .free = workload_free,
    };
    return bridgeToClient;
}
