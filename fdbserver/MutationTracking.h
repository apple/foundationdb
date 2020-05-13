/*
 * MutationTracking.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef _FDBSERVER_MUTATIONTRACKING_H_
#define _FDBSERVER_MUTATIONTRACKING_H_
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"

#define MUTATION_TRACKING_ENABLED 0


#define DEBUG_MUTATION(context, version, mutation) MUTATION_TRACKING_ENABLED && debugMutation(context, version, mutation)
TraceEvent debugMutation( const char* context, Version version, MutationRef const& mutation );

#define DEBUG_KEY_RANGE(context, version, keys) MUTATION_TRACKING_ENABLED && debugKeyRange(context, version, keys)
TraceEvent debugKeyRange( const char* context, Version version, KeyRangeRef const& keys );

#define DEBUG_TAGS_AND_MESSAGE(context, version, commitBlob) MUTATION_TRACKING_ENABLED && debugTagsAndMessage(context, version, commitBlob)
TraceEvent debugTagsAndMessage( const char* context, Version version, StringRef commitBlob );

#endif
