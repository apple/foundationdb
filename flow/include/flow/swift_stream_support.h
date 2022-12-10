/*
 * swift_stream_support.h
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

#ifndef SWIFT_STREAM_SUPPORT_H
#define SWIFT_STREAM_SUPPORT_H

#include "swift.h"
#include "flow.h"
#include "unsafe_swift_compat.h"
#include "SwiftModules/Flow_CheckedContinuation.h"
#include "pthread.h"
#include <stdint.h>

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases, since we cannot work with templates yet in Swift

using PromiseStreamCInt = PromiseStream<int>;
using FutureStreamCInt = FutureStream<int>;

// ==== ----------------------------------------------------------------------------------------------------------------

// TODO: Implements `FlowSingleCallbackForSwiftContinuationProtocol`
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
		// 		  interop, without relying on passing it via a `void *`
		// 	 	  here. That will let us avoid this hack.
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

// ==== ----------------------------------------------------------------------------------------------------------------

/// TODO(swift): Conform this to FlowSingleCallbackForSwiftContinuationProtocol from C++ already
using FlowSingleCallbackForSwiftContinuation_CInt =
    FlowSingleCallbackForSwiftContinuation<int>;

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: SingleCallback types
// 		 Used for waiting on FutureStreams, which don't support multiple callbacks.

// FIXME(swift): either implement in Swift, or manage lifetime properly
struct UNSAFE_SWIFT_CXX_IMMORTAL_REF SwiftContinuationSingleCallbackCInt : SingleCallback<int> {
private:
	void* _Nonnull continuationBox;
	void (*_Nonnull resumeWithValue)(void* _Nonnull /*context*/, /*value*/ int);
	void (*_Nonnull resumeWithError)(void* _Nonnull /*context*/, /*value*/ Error);

	SwiftContinuationSingleCallbackCInt(void* continuationBox,
	                              void (*_Nonnull returning)(void* _Nonnull, int),
	                              void (*_Nonnull throwing)(void* _Nonnull, Error))
	  : continuationBox(continuationBox),
	    resumeWithValue(returning),
	    resumeWithError(throwing) {}

public:
	static SwiftContinuationSingleCallbackCInt* _Nonnull make(void* continuationBox,
	                                                    void (*_Nonnull returning)(void* _Nonnull, int),
	                                                    void (*_Nonnull throwing)(void* _Nonnull, Error)) {
		return new SwiftContinuationSingleCallbackCInt(continuationBox, returning, throwing);
	}

	void addCallbackAndClearTo(FutureStreamCInt f) {
		f.addCallbackAndClear(this);
	}

	void fire(int const& value) {
		SingleCallback<int>::remove();
		SingleCallback<int>::next = 0;
		resumeWithValue(continuationBox, value);
	}

	void fire(int&& value) {
		SingleCallback<int>::remove();
		SingleCallback<int>::next = 0;
		auto copy = value;
		resumeWithValue(continuationBox, copy);
	}

	void error(Error error) {
		printf("[c++][%s:%d](%s) [stream] cb=%p, ERROR: code=%d\n", __FILE_NAME__, __LINE__, __FUNCTION__, this, error.code());

		if (error.code() == error_code_end_of_stream) {
			printf("[c++][%s:%d](%s) [stream] cb=%p, ERROR: END OF STREAM\n", __FILE_NAME__, __LINE__, __FUNCTION__, this);
		}

		SingleCallback<int>::remove();
		SingleCallback<int>::next = 0;
		resumeWithError(continuationBox, error);
	}

	void unwait() {
		// TODO(swift): implement
	}
};

#endif
