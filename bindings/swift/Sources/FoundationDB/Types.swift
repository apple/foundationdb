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

typealias CFuturePtr = OpaquePointer
typealias CCallback = @convention(c) (UnsafeRawPointer?, UnsafeRawPointer?) -> Void

public struct Fdb {
    public typealias Bytes = [UInt8]
    public typealias Key = Bytes
    public typealias Value = Bytes
    public typealias KeyValue = (Key, Value)
    public typealias KeyValueArray = [KeyValue]

    public struct RangeResult: Sendable {
        public let kvs: KeyValueArray
        public let more: Bool

        public init(kvs: KeyValueArray, more: Bool) {
            self.kvs = kvs
            self.more = more
        }
    }

    typealias Future<T: FutureExtractible> = SwiftFuture<T>
}
