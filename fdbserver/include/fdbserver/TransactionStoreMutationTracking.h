/*
 * TxnMutationTracking.h
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

#ifndef _FDBSERVER_TRANSACTION_STORE_TRACKING_H_
#define _FDBSERVER_TRANSACTION_STORE_TRACKING_H_
#pragma once

#include "fdbclient/FDBTypes.h"

// The keys to track are defined in the .cpp file to limit recompilation.
#define DEBUG_TRANSACTION_STATE_STORE_ENABLED 0

#define DEBUG_TRANSACTION_STATE_STORE(...)                                                                             \
	DEBUG_TRANSACTION_STATE_STORE_ENABLED&& transactionStoreDebugMutation(__VA_ARGS__)
TraceEvent transactionStoreDebugMutation(const char* context,
                                         StringRef const& mutation,
                                         const UID id,
                                         const std::string loc = "");

#endif
