/*
* fdbserver_stream_support.h
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

#ifndef FOUNDATIONDB_FDBSERVER_STREAM_SUPPORT_H
#define FOUNDATIONDB_FDBSERVER_STREAM_SUPPORT_H

#include "flow/swift.h"
#include "flow/flow.h"
#include "flow/unsafe_swift_compat.h"
#include "flow/swift_stream_support.h"
#include "pthread.h"
#include <stdint.h>

#include "MasterInterface.h"
#include "SwiftModules/FDBServer_CxxTypeConformances.h"

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases

#define SWIFT_FUTURE_STREAM(TYPE)									\
using CONCAT3(FutureStream, _, TYPE) = FutureStream<struct TYPE>;	\
using CONCAT3(FlowSingleCallbackForSwiftContinuation, _, TYPE) =	\
    FlowSingleCallbackForSwiftContinuation<TYPE>;

#define SWIFT_REQUEST_STREAM(TYPE)									\
using CONCAT3(RequestStream, _, TYPE) = RequestStream<struct TYPE>;

SWIFT_FUTURE_STREAM(UpdateRecoveryDataRequest)
SWIFT_REQUEST_STREAM(UpdateRecoveryDataRequest)

SWIFT_FUTURE_STREAM(GetCommitVersionRequest)
SWIFT_REQUEST_STREAM(GetCommitVersionRequest)

SWIFT_FUTURE_STREAM(GetRawCommittedVersionRequest)
SWIFT_REQUEST_STREAM(GetRawCommittedVersionRequest)

SWIFT_FUTURE_STREAM(ReportRawCommittedVersionRequest)
SWIFT_REQUEST_STREAM(ReportRawCommittedVersionRequest)

#endif // FOUNDATIONDB_FDBSERVER_STREAM_SUPPORT_H
