/*
 * stream_support.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

public protocol FlowStreamOpsAsyncIterator: AsyncIteratorProtocol where Element == AssociatedFutureStream.Element {
    associatedtype AssociatedFutureStream: FlowStreamOps

    init(_: AssociatedFutureStream)
}

/// Corresponds to `FlowSingleCallbackForSwiftContinuation: Flow.SingleCallback<T>`
public protocol FlowSingleCallbackForSwiftContinuationProtocol<AssociatedFutureStream> {
    associatedtype AssociatedFutureStream: FlowStreamOps
    typealias Element = AssociatedFutureStream.Element

    init()

    ///
    /// ```
    /// 	void set(
    ///          const void * _Nonnull pointerToContinuationInstance,
    ///	         FutureStream<T> fs,
    ///	         const void * _Nonnull thisPointer)
    /// ```
    mutating func set(_ continuationPointer: UnsafeRawPointer,
                      _ stream: Self.AssociatedFutureStream,
                      _ thisPointer: UnsafeRawPointer)
}

public protocol FlowStreamOps: AsyncSequence
        where AsyncIterator: FlowStreamOpsAsyncIterator,
              AsyncIterator.Element == Self.Element,
              AsyncIterator.AssociatedFutureStream == Self,
              SingleCB.AssociatedFutureStream == Self {

    /// Element type of the stream
    associatedtype Element

    // : C++ SingleCallback<T> FlowSingleCallbackForSwiftContinuationProtocol
    associatedtype SingleCB: FlowSingleCallbackForSwiftContinuationProtocol
        where SingleCB.AssociatedFutureStream == Self


    /// FIXME: can't typealias like that, we need to repeat it everywhere: rdar://103021742 ([Sema] Crash during self referential generic requirement)
    // typealias AsyncIterator = FlowStreamOpsAsyncIteratorAsyncIterator<Self>

    // === ---------------------------------------------------------------------
    // Exposed Swift capabilities

    /// Suspends and awaits for the next element.
    ///
    /// If the stream completes while we're waiting on it, this will return `nil`.
    /// Other errors thrown by the stream are re-thrown by this computed property.
    var waitNext: Element? {
        mutating get async throws
    }

    /// Implements protocol requirement of `AsyncSequence`, and enables async-for looping over this type.
    func makeAsyncIterator() -> AsyncIterator

    // === ---------------------------------------------------------------------
    // C++ API

    func isReady() -> Bool
    func isError() -> Bool

    mutating func pop() -> Element

    mutating func getError() -> Flow.Error
}

/// Default implementations that are good for all adopters of this protocol, generally no need to override.
extension FlowStreamOps {

    public var waitNext: Element? {
        mutating get async throws {
            if self.isReady() {
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
            } else {
                 var s = SingleCB()
                return try await withCheckedThrowingContinuation { (cc: CheckedContinuation<Element, Swift.Error>) in
                    withUnsafeMutablePointer(to: &s) { ptr in
                        let ecc = FlowCheckedContinuation<Element>(cc)
                        withUnsafePointer(to: ecc) { ccPtr in
                            ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                        }
                    }
                }
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        AsyncIterator(self)
    }
}

public struct FlowStreamOpsAsyncIteratorAsyncIterator<AssociatedFutureStream>: AsyncIteratorProtocol, FlowStreamOpsAsyncIterator
    where AssociatedFutureStream: FlowStreamOps {
    public typealias Element = AssociatedFutureStream.Element

    var stream: AssociatedFutureStream
    public init(_ stream: AssociatedFutureStream) {
        self.stream = stream
    }

    public mutating func next() async throws -> Element? {
        try await stream.waitNext
    }
}
