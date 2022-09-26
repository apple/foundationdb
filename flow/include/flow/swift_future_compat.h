/*
 * network.h
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

#ifndef SWIFT_FUTURE_COMPAT_H
#define SWIFT_FUTURE_COMPAT_H

#include "swift.h"
#include "flow.h"
#include "pthread.h"
#include <stdint.h>

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases, since we cannot work with templates yet in Swift

using PromiseCInt = Promise<int>;
using FutureCInt = Future<int>;
using CallbackInt = Callback<int>;

using PromiseVoid = Promise<Void>;
using FutureVoid = Future<Void>;
using CallbackVoid = Callback<Void>;

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Callback types

struct SWIFT_CXX_REF_IMMORTAL CCResumeCInt {
	void* cc;
	void (*resumeWithValue)(void*, int);

	explicit CCResumeCInt(void* cc, void (*resumeWithValue)(void*, int)) : cc(cc), resumeWithValue(resumeWithValue) {}

	void resume(int value) { resumeWithValue(this->cc, value); }
};

struct SWIFT_CXX_REF_IMMORTAL SwiftContinuationCallbackCInt : Callback<int> {
private:
	void* continuationBox;
	void (*resumeWithValue)(void* _Nonnull /*context*/, /*value*/ int);
	void (*resumeWithError)(void* _Nonnull /*context*/, /*value*/ Error);

	SwiftContinuationCallbackCInt(void* continuationBox,
	                             void (*_Nonnull returning)(void*, int),
	                             void (*_Nonnull throwing)(void*, Error))
	  : continuationBox(continuationBox), resumeWithValue(returning), resumeWithError(throwing) {}

public:
	static SwiftContinuationCallbackCInt* _Nonnull make(void* continuationBox,
	                                                   void (*_Nonnull returning)(void*, int),
	                                                   void (*_Nonnull throwing)(void*, Error)) {
		return new SwiftContinuationCallbackCInt(continuationBox, returning, throwing);
	}

	CallbackInt* _Nonnull cast() { return this; }

	void addCallbackAndClearTo(FutureCInt f) {
    f.addCallbackAndClear(this);
  }

	// TODO: virtual is an issue
	void fire(int const& value) {
		printf("[c++][%s:%d](%s) cb:%p\n", __FILE_NAME__, __LINE__, __FUNCTION__, this);
		Callback<int>::remove();
		Callback<int>::next = 0;
		resumeWithValue(continuationBox, value);
	}

	// TODO: virtual is an issue
	void error(Error error) {
		printf("[c++][%s:%d](%s) \n", __FILE_NAME__, __LINE__, __FUNCTION__);
		Callback<int>::remove();
		Callback<int>::next = 0;
		resumeWithError(continuationBox, error);
	}
	void unwait() {
		printf("[c++][%s:%d](%s) \n", __FILE_NAME__, __LINE__, __FUNCTION__);
		// TODO: implement
	}
};

#endif