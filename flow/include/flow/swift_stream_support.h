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
#include "pthread.h"
#include "unsafe_swift_compat.h"
#include <stdint.h>

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases, since we cannot work with templates yet in Swift

using PromiseStreamCInt = PromiseStream<int>;
using FutureStreamCInt = FutureStream<int>;

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

	// TODO(swift): virtual is an issue
	void fire(int const& value) {
		printf("[c++][%s:%d](%s) [stream] cb=%p, fire: %d\n", __FILE_NAME__, __LINE__, __FUNCTION__, this, value);
		SingleCallback<int>::remove();
		SingleCallback<int>::next = 0;
		resumeWithValue(continuationBox, value);
	}

	// TODO(swift): virtual is an issue
	void fire(int&& value) {
		printf("[c++][%s:%d](%s) [stream] cb=%p, fire&&: %d\n", __FILE_NAME__, __LINE__, __FUNCTION__, this, value);
		SingleCallback<int>::remove();
		SingleCallback<int>::next = 0;
		auto copy = value;
		resumeWithValue(continuationBox, copy);
	}

	// TODO(swift): virtual is an issue
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
