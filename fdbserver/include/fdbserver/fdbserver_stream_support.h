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
#include "pthread.h"
#include <stdint.h>

#include "MasterInterface.h"
#include "SwiftModules/FDBServer_CxxTypeConformances.h"

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases

using FutureStream_UpdateRecoveryDataRequest = FutureStream<struct UpdateRecoveryDataRequest>;
using RequestStream_UpdateRecoveryDataRequest = RequestStream<struct UpdateRecoveryDataRequest>;

using FutureStream_GetCommitVersionRequest = FutureStream<struct GetCommitVersionRequest>;

using RequestStream_GetRawCommittedVersionRequest = RequestStream<struct GetRawCommittedVersionRequest>;
using FutureStream_GetRawCommittedVersionRequest = FutureStream<struct GetRawCommittedVersionRequest>;

using RequestStream_ReportRawCommittedVersionRequest = RequestStream<struct ReportRawCommittedVersionRequest>;
using FutureStream_ReportRawCommittedVersionRequest = FutureStream<struct ReportRawCommittedVersionRequest>;

// ==== ----------------------------------------------------------------------------------------------------------------

template<class T>
class FlowSingleCallbackForSwiftContinuation : SingleCallback<T> {
	using SwiftCC = flow_swift::FlowCheckedContinuation<T>;
	SwiftCC continuationInstance;
public:
	void set(const void * _Nonnull pointerToContinuationInstance,
	         FutureStream<T> fs,
	         const void * _Nonnull thisPointer) {
		// Verify Swift did not make a copy of the `self` value for this method
		// call.
		assert(this == thisPointer);

		// FIXME: Propagate `SwiftCC` to Swift using forward
		// interop, without relying on passing it via a `void *`
		// here. That will let us avoid this hack.
		const void *_Nonnull opaqueStorage = pointerToContinuationInstance;
		static_assert(sizeof(SwiftCC) == sizeof(const void *));
		const SwiftCC ccCopy(*reinterpret_cast<const SwiftCC *>(&opaqueStorage));
		// Set the continuation instance.
		continuationInstance.set(ccCopy);
		// Add this callback to the future.
		fs.addCallbackAndClear(this);
	}

	FlowSingleCallbackForSwiftContinuation(): continuationInstance(SwiftCC::init()) {
	}

	void fire(T const& value) {
		SingleCallback<T>::remove();
		SingleCallback<T>::next = 0;
		continuationInstance.resume(value);
	}

	void fire(T&& value) {
		SingleCallback<T>::remove();
		SingleCallback<T>::next = 0;
		auto copy = value;
		continuationInstance.resume(copy);
	}

	void error(Error error) {
		SingleCallback<T>::remove();
		SingleCallback<T>::next = 0;
		continuationInstance.resumeThrowing(error);
	}

	void unwait() {
		// TODO(swift): implement
	}
};

using FlowSingleCallbackForSwiftContinuation_UpdateRecoveryDataRequest =
    FlowSingleCallbackForSwiftContinuation<UpdateRecoveryDataRequest>;

using FlowSingleCallbackForSwiftContinuation_GetCommitVersionRequest =
    FlowSingleCallbackForSwiftContinuation<GetCommitVersionRequest>;

using FlowSingleCallbackForSwiftContinuation_GetRawCommittedVersionRequest =
    FlowSingleCallbackForSwiftContinuation<GetRawCommittedVersionRequest>;

using FlowSingleCallbackForSwiftContinuation_ReportRawCommittedVersionRequest =
    FlowSingleCallbackForSwiftContinuation<ReportRawCommittedVersionRequest>;


#endif // FOUNDATIONDB_FDBSERVER_STREAM_SUPPORT_H
