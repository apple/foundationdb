/*
 * Transaction.swift
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

public class FdbTransaction {
    private let transaction: OpaquePointer

    init(transaction: OpaquePointer) {
        self.transaction = transaction
    }

    deinit {
        fdb_transaction_destroy(transaction)
    }

    public func getValue(for key: String, snapshot: Bool = false) async throws -> Fdb.Value? {
        let keyBytes = [UInt8](key.utf8)
        return try await getValue(for: keyBytes, snapshot: snapshot)
    }

    public func getValue(for key: Fdb.Key, snapshot: Bool = false) async throws -> Fdb.Value? {
        try await key.withUnsafeBytes { keyBytes in
            Fdb.Future<Fdb.Value>(
                fdb_transaction_get(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(key.count),
                    snapshot ? 1 : 0
                )
            )
        }.getAsync()
    }

    public func setValue(_ value: Fdb.Value, for key: Fdb.Key) {
        key.withUnsafeBytes { keyBytes in
            value.withUnsafeBytes { valueBytes in
                fdb_transaction_set(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(key.count),
                    valueBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(value.count)
                )
            }
        }
    }

    public func setValue(_ value: String, for key: String) {
        let keyBytes = [UInt8](key.utf8)
        let valueBytes = [UInt8](value.utf8)
        setValue(valueBytes, for: keyBytes)
    }

    public func clear(key: Fdb.Key) {
        key.withUnsafeBytes { keyBytes in
            fdb_transaction_clear(
                transaction,
                keyBytes.bindMemory(to: UInt8.self).baseAddress,
                Int32(key.count)
            )
        }
    }

    public func clear(key: String) {
        let keyBytes = [UInt8](key.utf8)
        clear(key: keyBytes)
    }

    public func clearRange(beginKey: Fdb.Key, endKey: Fdb.Key) {
        beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                fdb_transaction_clear_range(
                    transaction,
                    beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(beginKey.count),
                    endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(endKey.count)
                )
            }
        }
    }

    public func clearRange(beginKey: String, endKey: String) {
        let beginKeyBytes = [UInt8](beginKey.utf8)
        let endKeyBytes = [UInt8](endKey.utf8)
        clearRange(beginKey: beginKeyBytes, endKey: endKeyBytes)
    }

    public func getKey(selector: Selectable, snapshot: Bool = false) async throws -> Fdb.Key? {
        let keySelector = selector.toKeySelector()
        return try await getKey(selector: keySelector, snapshot: snapshot)
    }

    public func getKey(selector: Fdb.KeySelector, snapshot: Bool = false) async throws -> Fdb.Key? {
        try await selector.key.withUnsafeBytes { keyBytes in
            Fdb.Future<Fdb.KeyResult>(
                fdb_transaction_get_key(
                    transaction,
                    keyBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(selector.key.count),
                    selector.orEqual ? 1 : 0,
                    selector.offset,
                    snapshot ? 1 : 0
                )
            )
        }.getAsync()?.key
    }

    public func commit() async throws -> Bool {
        try await Fdb.Future<FdbVoid>(
            fdb_transaction_commit(transaction)
        ).getAsync() != nil
    }

    public func cancel() {
        fdb_transaction_cancel(transaction)
    }

    public func getVersionstamp() async throws -> Fdb.Key? {
        try await Fdb.Future<Fdb.Key>(
            fdb_transaction_get_versionstamp(transaction)
        ).getAsync()
    }

    public func setReadVersion(_ version: Int64) {
        fdb_transaction_set_read_version(transaction, version)
    }

    public func getReadVersion() async throws -> Int64 {
        try await Fdb.Future<Int64>(
            fdb_transaction_get_read_version(transaction)
        ).getAsync() ?? 0
    }

    public func getRange(begin: Selectable, end: Selectable, limit: Int32 = 0, snapshot: Bool = false) async throws -> Fdb.RangeResult {
        let beginSelector = begin.toKeySelector()
        let endSelector = end.toKeySelector()
        return try await getRange(beginSelector: beginSelector, endSelector: endSelector, limit: limit, snapshot: snapshot)
    }

    public func getRange(beginSelector: Fdb.KeySelector, endSelector: Fdb.KeySelector, limit: Int32 = 0, snapshot: Bool = false) async throws -> Fdb.RangeResult {
        let future = beginSelector.key.withUnsafeBytes { beginKeyBytes in
            endSelector.key.withUnsafeBytes { endKeyBytes in
                Fdb.Future<Fdb.RangeResult>(
                    fdb_transaction_get_range(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginSelector.key.count),
                        beginSelector.orEqual ? 1 : 0,
                        beginSelector.offset,
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endSelector.key.count),
                        endSelector.orEqual ? 1 : 0,
                        endSelector.offset,
                        limit,
                        0, // target_bytes = 0 (no limit)
                        FDBStreamingMode(-1), // mode = FDB_STREAMING_MODE_ITERATOR
                        1, // iteration = 1
                        snapshot ? 1 : 0,
                        0  // reverse = false
                    )
                )
            }
        }

        return try await future.getAsync() ?? Fdb.RangeResult(kvs: [], more: false)
    }

    public func getRange(beginKey: String, endKey: String, limit: Int32 = 0, snapshot: Bool = false) async throws -> Fdb.RangeResult {
        let beginKeyBytes = [UInt8](beginKey.utf8)
        let endKeyBytes = [UInt8](endKey.utf8)
        return try await getRange(beginKey: beginKeyBytes, endKey: endKeyBytes, limit: limit, snapshot: snapshot)
    }

    public func getRange(
        beginKey: Fdb.Key, endKey: Fdb.Key, limit: Int32 = 0, snapshot: Bool = false
    ) async throws -> Fdb.RangeResult {
        let future = beginKey.withUnsafeBytes { beginKeyBytes in
            endKey.withUnsafeBytes { endKeyBytes in
                Fdb.Future<Fdb.RangeResult>(
                    fdb_transaction_get_range(
                        transaction,
                        beginKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(beginKey.count),
                        1, // begin_or_equal = true
                        0, // begin_offset = 0
                        endKeyBytes.bindMemory(to: UInt8.self).baseAddress,
                        Int32(endKey.count),
                        1, // end_or_equal = false (exclusive)
                        0, // end_offset = 0
                        limit,
                        0, // target_bytes = 0 (no limit)
                        FDBStreamingMode(-1), // mode = FDB_STREAMING_MODE_ITERATOR
                        1, // iteration = 1
                        snapshot ? 1 : 0,
                        0  // reverse = false
                    )
                )
            }
        }

        return try await future.getAsync() ?? Fdb.RangeResult(kvs: [], more: false)
    }
}
