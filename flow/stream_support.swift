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

public protocol _FlowStreamOps: AsyncSequence {
    /// Element type of the stream
    associatedtype Element
    typealias _T = Element // just convenience to share snippets between Future impl and this

    /// Box type to be used to wrap the checked continuation
    typealias CCBox = _Box<CheckedContinuation<Element?, Swift.Error>>

    /// Suspends and awaits for the next element.
    ///
    /// If the stream completes while we're waiting on it, this will return `nil`.
    /// Other errors thrown by the stream are re-thrown by this computed property.
    var waitNext: Element? {
        mutating get async throws
    }
}

// ==== ---------------------------------------------------------------------------------------------------------------
// MARK: FutureStreams

extension FutureStreamCInt: _FlowStreamOps {
    public typealias Element = CInt
    public typealias CB = SwiftContinuationSingleCallbackCInt

    public var waitNext: Element? {
        mutating get async throws {
            guard !self.isReady() else {
                pprint("[stream] stream next future was ready, return immediately.")
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

            return try await withCheckedThrowingContinuation { cc in
                let ccBox = Self.CCBox(cc)
                let rawCCBox = UnsafeMutableRawPointer(Unmanaged.passRetained(ccBox).toOpaque())

                let cb = CB.make(
                    rawCCBox,
                    /*returning:*/ { (_cc: UnsafeMutableRawPointer, value: _T) in
                        pprint("[stream] returning !!!")
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc).takeRetainedValue().value
                        cc.resume(returning: value)
                    },
                    /*throwing:*/ { (_cc: UnsafeMutableRawPointer, error: Flow.Error) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc).takeRetainedValue().value
                        pprint("[stream] throwing !!!")
                        if error.isEndOfStream {
                            pprint("[stream] END OF STREAM")
                            cc.resume(returning: nil)
                        } else {
                            pprint("[stream] throwing error !!!")
                            cc.resume(throwing: GeneralFlowError()) // TODO: map errors
                        }
                    }
                )
                // self.addCallbackAndClear(cb) // TODO: perhaps can workaround this? annot convert value of type 'SwiftContinuationSingleCallbackCInt' to expected argument type 'UnsafeMutablePointer<__CxxTemplateInst14SingleCallbackIiE>?'
                cb.addCallbackAndClearTo(self)
            }
        }
    }

    public func makeAsyncIterator() -> AsyncIterator {
        pprint("[stream] make iterator!")
        return .init(self)
    }

    public struct AsyncIterator: AsyncIteratorProtocol {
        public typealias Stream = FutureStreamCInt
        public typealias Element = Stream.Element

        var stream: Stream
        init(_ stream: Stream) {
            self.stream = stream
        }

        // FIXME(swift): how to implement "end of stream -> nil" -- probably emits an error in flow?
        public mutating func next() async throws -> Element? {
            pprint("[stream] await next!")
            let value = try await stream.waitNext
            pprint("[stream] await next - DONE!")
            return value
        }
    }
}

public final class _Box<Value> {
    public let value: Value
    public init(_ value: Value) {
        self.value = value
    }
}
