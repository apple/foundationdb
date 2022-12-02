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

extension RequestStream_UpdateRecoveryDataRequest: _FlowStreamOps {
    public typealias Element = UpdateRecoveryDataRequest
    public typealias AsyncIterator = FutureStream_UpdateRecoveryDataRequest.AsyncIterator
    typealias SingleCB = FlowSingleCallbackForSwiftContinuation_UpdateRecoveryDataRequest

    public var waitNext: Element? {
        mutating get async throws {
            var fs = self.getFuture()
            return try await fs.waitNext
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return self.getFuture().makeAsyncIterator()
    }

}

extension FutureStream_UpdateRecoveryDataRequest: _FlowStreamOps {
    public typealias Stream = Self
    public typealias Element = UpdateRecoveryDataRequest
    public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_UpdateRecoveryDataRequest

    public var waitNext: Element? {
        mutating get async throws {
            guard !self.isReady() else {
                if self.isError() {
                    let error = self.getError()
                    if error.isEndOfStream {
                        return nil
                    } else {
                        throw GeneralFlowError(error)
                    }
                } else {
                    return self.pop()
                }
            }

            var s = SingleCB()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<Element>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = Stream.Element

        var stream: Stream
        init(_ stream: Stream) {
            self.stream = stream
        }

        public mutating func next() async throws -> Element? {
            try await stream.waitNext
        }
    }
}

extension FutureStream_GetRawCommittedVersionRequest: _FlowStreamOps {
    public typealias Stream = Self
    public typealias Element = FDBClient.GetRawCommittedVersionRequest
    public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_GetRawCommittedVersionRequest

    public var waitNext: Element? {
        mutating get async throws {
            guard !self.isReady() else {
                if self.isError() {
                    let error = self.getError()
                    if error.isEndOfStream {
                        return nil
                    } else {
                        throw GeneralFlowError(error)
                    }
                } else {
                    return self.pop()
                }
            }

            var s = SingleCB()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<Element>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = Stream.Element

        var stream: Stream
        init(_ stream: Stream) {
            self.stream = stream
        }

        public mutating func next() async throws -> Element? {
            try await stream.waitNext
        }
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: GetCommitVersionRequest

extension FutureStream_GetCommitVersionRequest: _FlowStreamOps {
    public typealias Stream = Self
    public typealias Element = GetCommitVersionRequest
    public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_GetCommitVersionRequest

    public var waitNext: Element? {
        mutating get async throws {
            guard !self.isReady() else {
                if self.isError() {
                    let error = self.getError()
                    if error.isEndOfStream {
                        return nil
                    } else {
                        throw GeneralFlowError(error)
                    }
                } else {
                    return self.pop()
                }
            }

            var s = SingleCB()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<Element>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = Stream.Element

        var stream: Stream
        init(_ stream: Stream) {
            self.stream = stream
        }

        public mutating func next() async throws -> Element? {
            try await stream.waitNext
        }
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: ReportRawCommittedVersionRequest

extension FutureStream_ReportRawCommittedVersionRequest: _FlowStreamOps {
    public typealias Stream = Self
    public typealias Element = ReportRawCommittedVersionRequest
    public typealias SingleCB = FlowSingleCallbackForSwiftContinuation_ReportRawCommittedVersionRequest

    public var waitNext: Element? {
        mutating get async throws {
            guard !self.isReady() else {
                if self.isError() {
                    let error = self.getError()
                    if error.isEndOfStream {
                        return nil
                    } else {
                        throw GeneralFlowError(error)
                    }
                } else {
                    return self.pop()
                }
            }

            var s = SingleCB()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<Element>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        return .init(self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Element = Stream.Element

        var stream: Stream
        init(_ stream: Stream) {
            self.stream = stream
        }

        public mutating func next() async throws -> Element? {
            try await stream.waitNext
        }
    }
}
