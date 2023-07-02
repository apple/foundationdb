#ifndef FDB_C_APIVERSION_G_H
#define FDB_C_APIVERSION_G_H
#pragma once

/*
 * fdb_c_apiversion.g.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * Do not include this file directly.
 */

/* The latest FDB C API version */
#define FDB_LATEST_API_VERSION @FDB_AV_LATEST_VERSION@

/* The latest FDB API version supported by bindings. It may lag behind the latest C API version */
#define FDB_LATEST_BINDINGS_API_VERSION @FDB_AV_LATEST_BINDINGS_VERSION@

/* API version introducing client_tmp_dir option */
#define FDB_API_VERSION_CLIENT_TMP_DIR @FDB_AV_CLIENT_TMP_DIR@

/* API version introducing disable_client_bypass option */
#define FDB_API_VERSION_DISABLE_CLIENT_BYPASS @FDB_AV_DISABLE_CLIENT_BYPASS@

/* API version with multitenancy API released */
#define FDB_API_VERSION_TENANT_API_RELEASED @FDB_AV_TENANT_API_RELEASED@

#endif
