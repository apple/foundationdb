/*
 * Future.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2025 Apple Inc. and the FoundationDB project authors
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
import CFoundationDB

// TODO: Explore ways to use Span and avoid copying bytes from CFuture into Swift.
class SwiftFuture<T: Fdb.FutureResult> {
    private let cFuture: CFuturePtr

    init(_ cFuture: CFuturePtr) {
        self.cFuture = cFuture
    }

    deinit {
        fdb_future_destroy(cFuture)
    }

    public func getAsync() async throws -> T? {
        try await withCheckedThrowingContinuation {
            (continuation: CheckedContinuation<T?, Error>) in
            let box = CallbackBox { [continuation] future in
                do {
                    let err = fdb_future_get_error(future)
                    if err != 0 {
                        throw FdbError(code: err)
                    }

                    let value = try T.extract(fromFuture: self.cFuture)
                    continuation.resume(returning: value)
                } catch {
                    continuation.resume(throwing: error)
                }
            }

            let userdata = Unmanaged.passRetained(box).toOpaque()  // TODO: If future is canceled, this will not cleanup?
            fdb_future_set_callback(cFuture, fdbFutureCallback, userdata)
        }
    }
}

private final class CallbackBox {
    let callback: (CFuturePtr) -> Void
    init(callback: @escaping (CFuturePtr) -> Void) {
        self.callback = callback
    }
}

private func fdbFutureCallback(future: CFuturePtr?, userdata: UnsafeMutableRawPointer?) {
    guard let userdata, let future = future else { return }
    let box = Unmanaged<CallbackBox>.fromOpaque(userdata).takeRetainedValue()
    box.callback(future)
}
