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

public enum Fdb {
    public typealias Version = Int64
    public typealias Bytes = [UInt8]
    public typealias Key = Bytes
    public typealias Value = Bytes
    public typealias KeyValue = (Key, Value)
    public typealias KeyValueArray = [KeyValue]

    public protocol Selectable {
        func toKeySelector() -> Fdb.KeySelector
    }

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
}

extension Fdb.Key: Fdb.Selectable {
    public func toKeySelector() -> Fdb.KeySelector {
        return Fdb.KeySelector.firstGreaterOrEqual(self)
    }
}

extension String: Fdb.Selectable {
    public func toKeySelector() -> Fdb.KeySelector {
        return Fdb.KeySelector.firstGreaterOrEqual([UInt8](utf8))
    }
}

public enum FdbMutationType: UInt32 {
    case add = 2
    case bitAnd = 6
    case bitOr = 7
    case bitXor = 8
    case appendIfFits = 9
    case max = 12
    case min = 13
    case setVersionstampedKey = 14
    case setVersionstampedValue = 15
    case byteMin = 16
    case byteMax = 17
    case compareAndClear = 20
}
