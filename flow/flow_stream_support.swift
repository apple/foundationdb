/*
 * stream_support.swift
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

import Flow

// ==== FutureStream: Conformances -------------------------------------------------------------------------------------

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: CInt

extension FutureStreamCInt: FlowStreamOps {
    public typealias Element = CInt
    public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_CInt
    public typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>
}

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowSingleCallbackForSwiftContinuation_CInt:
        FlowSingleCallbackForSwiftContinuationProtocol {
    public typealias AssociatedFutureStream = FutureStreamCInt
}

//// ==== ----------------------------------------------------------------------------------------------------------------
//// MARK: Void
//
//extension FutureStreamCInt: FlowStreamOps {
//    public typealias Element = Flow.Void
//    public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_Void
//    public typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>
//}
//
//// This is a C++ type that we add the conformance to; so no easier way around it currently
//extension FlowSingleCallbackForSwiftContinuation_CInt:
//        FlowSingleCallbackForSwiftContinuationProtocol {
//    public typealias AssociatedFutureStream = FutureStreamCInt
//}

// ==== ---------------------------------------------------------------------------------- ==== //
// ==== Add further conformances here that should be shared for all Flow importing modules ==== //
// ==== ---------------------------------------------------------------------------------- ==== //
