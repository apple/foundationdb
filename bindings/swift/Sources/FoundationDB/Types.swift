/*
 * Types.swift
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

typealias CFuturePtr = OpaquePointer
typealias CCallback = @convention(c) (UnsafeRawPointer?, UnsafeRawPointer?) -> Void

public protocol Selectable {
    func toKeySelector() -> Fdb.KeySelector
}

extension Fdb.Key: Selectable {
    public func toKeySelector() -> Fdb.KeySelector {
        return Fdb.KeySelector.firstGreaterOrEqual(self)
    }
}

extension String: Selectable {
    public func toKeySelector() -> Fdb.KeySelector {
        return Fdb.KeySelector.firstGreaterOrEqual([UInt8](self.utf8))
    }
}

public struct Fdb {
    public typealias Bytes = [UInt8]
    public typealias Key = Bytes
    public typealias Value = Bytes
    public typealias KeyValue = (Key, Value)
    public typealias KeyValueArray = [KeyValue]

    public struct KeySelector: Selectable {
        public let key: Key
        public let orEqual: Bool
        public let offset: Int32

        public init(key: Key, orEqual: Bool, offset: Int32) {
            self.key = key
            self.orEqual = orEqual
            self.offset = offset
        }

        public func toKeySelector() -> KeySelector {
            return self
        }

        public static func firstGreaterOrEqual(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: false, offset: 1)
        }

        public static func firstGreaterOrEqual(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: false, offset: 1)
        }

        public static func firstGreaterThan(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: true, offset: 1)
        }

        public static func firstGreaterThan(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: true, offset: 1)
        }

        public static func lastLessOrEqual(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: true, offset: 0)
        }

        public static func lastLessOrEqual(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: true, offset: 0)
        }

        public static func lastLessThan(_ key: Key) -> KeySelector {
            return KeySelector(key: key, orEqual: false, offset: 0)
        }

        public static func lastLessThan(_ key: String) -> KeySelector {
            return KeySelector(key: [UInt8](key.utf8), orEqual: false, offset: 0)
        }
    }

    
    protocol FutureResult: Sendable {
        static func extract(fromFuture: CFuturePtr) throws -> Self?
    }

    struct ResultVoid: FutureResult {
        static func extract(fromFuture: CFuturePtr) throws -> ResultVoid? {
            let err = fdb_future_get_error(fromFuture)
            if err != 0 {
                throw FdbError(code: err)
            }

            return ResultVoid()
        }
    }

    // Internal result types that implement FutureResult
    struct ResultValue: FutureResult {
        let value: Value?

        static func extract(fromFuture: CFuturePtr) throws -> ResultValue? {
            var present: Int32 = 0
            var valPtr: UnsafePointer<UInt8>?
            var valLen: Int32 = 0

            let err = fdb_future_get_value(fromFuture, &present, &valPtr, &valLen)
            if err != 0 {
                throw FdbError(code: err)
            }

            if present != 0, let valPtr {
                let value = Array(UnsafeBufferPointer(start: valPtr, count: Int(valLen)))
                return ResultValue(value: value)
            }

            return ResultValue(value: nil)
        }
    }

    struct ResultKey: FutureResult {
        let value: Key?

        static func extract(fromFuture: CFuturePtr) throws -> ResultKey? {
            var keyPtr: UnsafePointer<UInt8>?
            var keyLen: Int32 = 0

            let err = fdb_future_get_key(fromFuture, &keyPtr, &keyLen)
            if err != 0 {
                throw FdbError(code: err)
            }

            if let keyPtr {
                let key = Array(UnsafeBufferPointer(start: keyPtr, count: Int(keyLen)))
                return ResultKey(value: key)
            }

            return ResultKey(value: nil)
        }
    }

    public struct ResultRange: FutureResult {
        let records: KeyValueArray
        let more: Bool

        static func extract(fromFuture: CFuturePtr) throws -> ResultRange? {
            var kvPtr: UnsafePointer<FDBKeyValue>?
            var count: Int32 = 0
            var more: Int32 = 0

            let err = fdb_future_get_keyvalue_array(fromFuture, &kvPtr, &count, &more)
            if err != 0 {
                throw FdbError(code: err)
            }

            guard let kvPtr = kvPtr, count > 0 else {
                return ResultRange(records: [], more: more > 0)
            }

            var keyValueArray: KeyValueArray = []
            for i in 0..<Int(count) {
                let kv = kvPtr[i]
                let key = Array(UnsafeBufferPointer(start: kv.key, count: Int(kv.key_length)))
                let value = Array(UnsafeBufferPointer(start: kv.value, count: Int(kv.value_length)))
                keyValueArray.append((key, value))
            }

            return ResultRange(records: keyValueArray, more: more > 0)
        }
    }

    struct ResultVersion: FutureResult {
        let value: Int64

        static func extract(fromFuture: CFuturePtr) throws -> ResultVersion? {
            var version: Int64 = 0
            let err = fdb_future_get_int64(fromFuture, &version)
            if err != 0 {
                throw FdbError(code: err)
            }
            return ResultVersion(value: version)
        }
    }

    typealias Future<T: FutureResult> = SwiftFuture<T>
}
