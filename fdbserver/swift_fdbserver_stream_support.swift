/*
 * swift_fdbserver_strem_support.swift
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
import flow_swift
import FDBClient
import FDBServer
import Cxx

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: UpdateRecoveryDataRequest

//extension RequestStream_UpdateRecoveryDataRequest: _FlowStreamOps {
//    public typealias Element = UpdateRecoveryDataRequest
//    public typealias AsyncIterator = FutureStream_UpdateRecoveryDataRequest.AsyncIterator
//    typealias SingleCB = FlowSingleCallbackForSwiftContinuation_UpdateRecoveryDataRequest
//}

extension FutureStream_UpdateRecoveryDataRequest: FlowStreamOps {
	public typealias Element = UpdateRecoveryDataRequest
	public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_UpdateRecoveryDataRequest
	public typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>
}

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowSingleCallbackForSwiftContinuation_UpdateRecoveryDataRequest:
		FlowSingleCallbackForSwiftContinuationProtocol {
	public typealias AssociatedFutureStream = FutureStream_UpdateRecoveryDataRequest
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: GetRawCommittedVersionRequest

extension FutureStream_GetRawCommittedVersionRequest: FlowStreamOps {
	public typealias SelfStream = Self
	public typealias Element = GetRawCommittedVersionRequest
	public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_GetRawCommittedVersionRequest
	public typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>
}

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowSingleCallbackForSwiftContinuation_GetRawCommittedVersionRequest: FlowSingleCallbackForSwiftContinuationProtocol {
	public typealias AssociatedFutureStream = FutureStream_GetRawCommittedVersionRequest
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: GetCommitVersionRequest

extension FutureStream_GetCommitVersionRequest: FlowStreamOps {
	public typealias SelfStream = Self
	public typealias Element = GetCommitVersionRequest
	public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_GetCommitVersionRequest
	public typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>
}

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowSingleCallbackForSwiftContinuation_GetCommitVersionRequest: FlowSingleCallbackForSwiftContinuationProtocol {
	public typealias AssociatedFutureStream = FutureStream_GetCommitVersionRequest
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: ReportRawCommittedVersionRequest

extension FutureStream_ReportRawCommittedVersionRequest: FlowStreamOps {
	public typealias SelfStream = Self
	public typealias Element = ReportRawCommittedVersionRequest
	public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_ReportRawCommittedVersionRequest
	public typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>
}

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowSingleCallbackForSwiftContinuation_ReportRawCommittedVersionRequest: FlowSingleCallbackForSwiftContinuationProtocol {
	public typealias AssociatedFutureStream = FutureStream_ReportRawCommittedVersionRequest
}
