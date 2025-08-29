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

protocol FutureResult: Sendable {
    static func extract(fromFuture: CFuturePtr) throws -> Self?
}

class Future<T: FutureResult> {
    private let cFuture: CFuturePtr

    init(_ cFuture: CFuturePtr) {
        self.cFuture = cFuture
    }

    deinit {
        fdb_future_destroy(cFuture)
    }

    func getAsync() async throws -> T? {
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

            let userdata = Unmanaged.passRetained(box).toOpaque() // TODO: If future is canceled, this will not cleanup?
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

struct ResultVoid: FutureResult {
    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        let err = fdb_future_get_error(fromFuture)
        if err != 0 {
            throw FdbError(code: err)
        }

        return Self()
    }
}

struct ResultVersion: FutureResult {
    let value: Fdb.Version

    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var version: Int64 = 0
        let err = fdb_future_get_int64(fromFuture, &version)
        if err != 0 {
            throw FdbError(code: err)
        }
        return Self(value: version)
    }
}

struct ResultKey: FutureResult {
    let value: Fdb.Key?

    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var keyPtr: UnsafePointer<UInt8>?
        var keyLen: Int32 = 0

        let err = fdb_future_get_key(fromFuture, &keyPtr, &keyLen)
        if err != 0 {
            throw FdbError(code: err)
        }

        if let keyPtr {
            let key = Array(UnsafeBufferPointer(start: keyPtr, count: Int(keyLen)))
            return Self(value: key)
        }

        return Self(value: nil)
    }
}

struct ResultValue: FutureResult {
    let value: Fdb.Value?

    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var present: Int32 = 0
        var valPtr: UnsafePointer<UInt8>?
        var valLen: Int32 = 0

        let err = fdb_future_get_value(fromFuture, &present, &valPtr, &valLen)
        if err != 0 {
            throw FdbError(code: err)
        }

        if present != 0, let valPtr {
            let value = Array(UnsafeBufferPointer(start: valPtr, count: Int(valLen)))
            return Self(value: value)
        }

        return Self(value: nil)
    }
}

public struct ResultRange: FutureResult {
    let records: Fdb.KeyValueArray
    let more: Bool

    static func extract(fromFuture: CFuturePtr) throws -> Self? {
        var kvPtr: UnsafePointer<FDBKeyValue>?
        var count: Int32 = 0
        var more: Int32 = 0

        let err = fdb_future_get_keyvalue_array(fromFuture, &kvPtr, &count, &more)
        if err != 0 {
            throw FdbError(code: err)
        }

        guard let kvPtr = kvPtr, count > 0 else {
            return nil
        }

        var keyValueArray: Fdb.KeyValueArray = []
        for i in 0 ..< Int(count) {
            let kv = kvPtr[i]
            let key = Array(UnsafeBufferPointer(start: kv.key, count: Int(kv.key_length)))
            let value = Array(UnsafeBufferPointer(start: kv.value, count: Int(kv.value_length)))
            keyValueArray.append((key, value))
        }

        return Self(records: keyValueArray, more: more > 0)
    }
}
