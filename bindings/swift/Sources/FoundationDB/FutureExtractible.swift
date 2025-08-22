/*
 * FutureExtratible.swift
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

protocol FutureExtractible {
    static func extract(fromFuture: CFuturePtr) throws -> Self?
}

struct FdbVoid: FutureExtractible {
    static func extract(fromFuture: CFuturePtr) throws -> FdbVoid? {
        let err = fdb_future_get_error(fromFuture)
        if err != 0 {
            throw FdbError(code: err)
        }

        return FdbVoid()
    }
}

extension Int64: FutureExtractible {
    static func extract(fromFuture: CFuturePtr) throws -> Int64? {
        var version: Int64 = 0
        let err = fdb_future_get_int64(fromFuture, &version)
        if err != 0 {
            throw FdbError(code: err)
        }
        return version
    }
}

extension Fdb.Bytes: FutureExtractible {
    static func extract(fromFuture: CFuturePtr) throws -> Fdb.Bytes? {
        var present: Int32 = 0
        var valPtr: UnsafePointer<UInt8>?
        var valLen: Int32 = 0

        let err = fdb_future_get_value(fromFuture, &present, &valPtr, &valLen)
        if err != 0 {
            throw FdbError(code: err)
        }

        if present != 0, let valPtr {
            return Array(UnsafeBufferPointer(start: valPtr, count: Int(valLen)))
        }

        return nil
    }
}

extension Fdb.RangeResult: FutureExtractible {
    static func extract(fromFuture: CFuturePtr) throws -> Fdb.RangeResult? {
        var kvPtr: UnsafePointer<FDBKeyValue>?
        var count: Int32 = 0
        var more: Int32 = 0

        let err = fdb_future_get_keyvalue_array(fromFuture, &kvPtr, &count, &more)
        if err != 0 {
            throw FdbError(code: err)
        }

        guard let kvPtr = kvPtr, count > 0 else {
            return Fdb.RangeResult(kvs: [], more: false)
        }

        var keyValueArray: Fdb.KeyValueArray = []
        for i in 0..<Int(count) {
            let kv = kvPtr[i]
            let key = Array(UnsafeBufferPointer(start: kv.key, count: Int(kv.key_length)))
            let value = Array(UnsafeBufferPointer(start: kv.value, count: Int(kv.value_length)))
            keyValueArray.append((key, value))
        }

        return Fdb.RangeResult(kvs: keyValueArray, more: more > 0)
    }
}
