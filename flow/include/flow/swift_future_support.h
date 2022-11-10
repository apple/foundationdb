/*
 * swift_future_support.h
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

#ifndef SWIFT_FUTURE_SUPPORT_H
#define SWIFT_FUTURE_SUPPORT_H

#include "swift.h"
#include "flow.h"
#include "swift_stream_support.h"
#include "pthread.h"
#include "unsafe_swift_compat.h"
#include "SwiftModules/Flow"
#include <stdint.h>

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: type aliases, since we cannot work with templates yet in Swift

using PromiseCInt = Promise<int>;
using FutureCInt = Future<int>;
using CallbackInt = Callback<int>;
using PromiseStreamCInt = PromiseStream<int>;
using FutureStreamCInt = FutureStream<int>;

using PromiseVoid = Promise<Void>;
using FutureVoid = Future<Void>;
using CallbackVoid = Callback<Void>;

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Callback types

template<class T>
class FlowCallbackForSwiftContinuation : Callback<T> {
    using SwiftCC = flow_swift::FlowCheckedContinuation<T>;
    SwiftCC continuationInstance;
public:
    void set(const void * _Nonnull pointerToContinuationInstance,
             Future<T> f,
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
        f.addCallbackAndClear(this);
    }

    FlowCallbackForSwiftContinuation() : continuationInstance(SwiftCC::init()) {
    }

    void fire(const T &value) override {
        printf("[c++][%s:%d](%s) cb:%p\n", __FILE_NAME__, __LINE__, __FUNCTION__, this);
        Callback<T>::remove();
        Callback<T>::next = nullptr;
        continuationInstance.resume(value);
    }
    void error(Error error) override {
        printf("[c++][%s:%d](%s) \n", __FILE_NAME__, __LINE__, __FUNCTION__);
        Callback<T>::remove();
        Callback<T>::next = nullptr;
        continuationInstance.resumeThrowing(error);
    }
    void unwait() override {
        printf("[c++][%s:%d](%s) \n", __FILE_NAME__, __LINE__, __FUNCTION__);
        // TODO(swift): implement
    }
};

using FlowCallbackForSwiftContinuationCInt = FlowCallbackForSwiftContinuation<int>;
using FlowCallbackForSwiftContinuationVoid = FlowCallbackForSwiftContinuation<Void>;

#endif
