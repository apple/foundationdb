/*
 * FoundationDBTests.swift
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

@testable import FoundationDB
import Testing

@Test("getValue test")
func testGetValue() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let res1 = try await newTransaction.getValue(for: "test_nonexistent_key")
    #expect(res1 == nil, "Non-existent key should return nil")

    newTransaction.setValue("world", for: "test_hello")
    let res2 = try await newTransaction.getValue(for: "test_hello")
    #expect(res2 == Array("world".utf8))
}

@Test("setValue with byte arrays")
func setValueBytes() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_byte_key".utf8)
    let value: Fdb.Value = [UInt8]("test_byte_value".utf8)

    newTransaction.setValue(value, for: key)

    let retrievedValue = try await newTransaction.getValue(for: key)
    #expect(retrievedValue == value, "Retrieved value should match set value")
}

@Test("setValue with strings")
func setValueStrings() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key = "test_string_key"
    let value = "test_string_value"
    newTransaction.setValue(value, for: key)

    let retrievedValue = try await newTransaction.getValue(for: key)
    let expectedValue = [UInt8](value.utf8)
    #expect(retrievedValue == expectedValue, "Retrieved value should match set value")
}

@Test("clear with byte arrays")
func clearBytes() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_clear_key".utf8)
    let value: Fdb.Value = [UInt8]("test_clear_value".utf8)

    newTransaction.setValue(value, for: key)
    let retrievedValueBefore = try await newTransaction.getValue(for: key)
    #expect(retrievedValueBefore == value, "Value should exist before clear")

    newTransaction.clear(key: key)
    let retrievedValueAfter = try await newTransaction.getValue(for: key)
    #expect(retrievedValueAfter == nil, "Value should be nil after clear")
}

@Test("clear with strings")
func clearStrings() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key = "test_clear_string_key"
    let value = "test_clear_string_value"

    newTransaction.setValue(value, for: key)
    let retrievedValueBefore = try await newTransaction.getValue(for: key)
    let expectedValue = [UInt8](value.utf8)
    #expect(retrievedValueBefore == expectedValue, "Value should exist before clear")

    newTransaction.clear(key: key)
    let retrievedValueAfter = try await newTransaction.getValue(for: key)
    #expect(retrievedValueAfter == nil, "Value should be nil after clear")
}

@Test("clearRange with byte arrays")
func clearRangeBytes() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key1: Fdb.Key = [UInt8]("test_range_key_a".utf8)
    let key2: Fdb.Key = [UInt8]("test_range_key_b".utf8)
    let key3: Fdb.Key = [UInt8]("test_range_key_c".utf8)
    let value: Fdb.Value = [UInt8]("test_value".utf8)

    let beginKey: Fdb.Key = [UInt8]("test_range_key_a".utf8)
    let endKey: Fdb.Key = [UInt8]("test_range_key_c".utf8)

    newTransaction.setValue(value, for: key1)
    newTransaction.setValue(value, for: key2)
    newTransaction.setValue(value, for: key3)

    let value1Before = try await newTransaction.getValue(for: key1)
    let value2Before = try await newTransaction.getValue(for: key2)
    let value3Before = try await newTransaction.getValue(for: key3)
    #expect(value1Before == value, "Value1 should exist before clearRange")
    #expect(value2Before == value, "Value2 should exist before clearRange")
    #expect(value3Before == value, "Value3 should exist before clearRange")

    newTransaction.clearRange(beginKey: beginKey, endKey: endKey)

    let value1After = try await newTransaction.getValue(for: key1)
    let value2After = try await newTransaction.getValue(for: key2)
    let value3After = try await newTransaction.getValue(for: key3)
    #expect(value1After == nil, "Value1 should be nil after clearRange")
    #expect(value2After == nil, "Value2 should be nil after clearRange")
    #expect(value3After == value, "Value3 should still exist (end key is exclusive)")
}

@Test("clearRange with strings")
func clearRangeStrings() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key1 = "test_range_string_key_a"
    let key2 = "test_range_string_key_b"
    let key3 = "test_range_string_key_c"
    let value = "test_string_value"

    let beginKey = "test_range_string_key_a"
    let endKey = "test_range_string_key_c"

    newTransaction.setValue(value, for: key1)
    newTransaction.setValue(value, for: key2)
    newTransaction.setValue(value, for: key3)

    let expectedValue = [UInt8](value.utf8)
    let value1Before = try await newTransaction.getValue(for: key1)
    let value2Before = try await newTransaction.getValue(for: key2)
    let value3Before = try await newTransaction.getValue(for: key3)
    #expect(value1Before == expectedValue, "Value1 should exist before clearRange")
    #expect(value2Before == expectedValue, "Value2 should exist before clearRange")
    #expect(value3Before == expectedValue, "Value3 should exist before clearRange")

    newTransaction.clearRange(beginKey: beginKey, endKey: endKey)

    let value1After = try await newTransaction.getValue(for: key1)
    let value2After = try await newTransaction.getValue(for: key2)
    let value3After = try await newTransaction.getValue(for: key3)
    #expect(value1After == nil, "Value1 should be nil after clearRange")
    #expect(value2After == nil, "Value2 should be nil after clearRange")
    #expect(value3After == expectedValue, "Value3 should still exist (end key is exclusive)")
}

@Test("getKey with KeySelector")
func getKeyWithKeySelector() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up some test data
    newTransaction.setValue("value1", for: "test_getkey_a")
    newTransaction.setValue("value2", for: "test_getkey_b")
    newTransaction.setValue("value3", for: "test_getkey_c")
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()
    // Test getting key with KeySelector - firstGreaterOrEqual
    let selector = Fdb.KeySelector.firstGreaterOrEqual("test_getkey_b")
    let resultKey = try await readTransaction.getKey(selector: selector)
    let expectedKey = [UInt8]("test_getkey_b".utf8)
    #expect(resultKey == expectedKey, "getKey with KeySelector should find exact key")
}

@Test("getKey with different KeySelector methods")
func getKeyWithDifferentSelectors() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    newTransaction.setValue("value1", for: "test_selector_a")
    newTransaction.setValue("value2", for: "test_selector_b")
    newTransaction.setValue("value3", for: "test_selector_c")
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()

    // Test firstGreaterOrEqual
    let selectorGTE = Fdb.KeySelector.firstGreaterOrEqual("test_selector_b")
    let resultGTE = try await readTransaction.getKey(selector: selectorGTE)
    #expect(resultGTE == [UInt8]("test_selector_b".utf8), "firstGreaterOrEqual should find exact key")

    // Test firstGreaterThan
    let selectorGT = Fdb.KeySelector.firstGreaterThan("test_selector_b")
    let resultGT = try await readTransaction.getKey(selector: selectorGT)
    #expect(resultGT == [UInt8]("test_selector_c".utf8), "firstGreaterThan should find next key")

    // Test lastLessOrEqual
    let selectorLTE = Fdb.KeySelector.lastLessOrEqual("test_selector_b")
    let resultLTE = try await readTransaction.getKey(selector: selectorLTE)
    #expect(resultLTE == [UInt8]("test_selector_b".utf8), "lastLessOrEqual should find exact key")

    // Test lastLessThan
    let selectorLT = Fdb.KeySelector.lastLessThan("test_selector_b")
    let resultLT = try await readTransaction.getKey(selector: selectorLT)
    #expect(resultLT == [UInt8]("test_selector_a".utf8), "lastLessThan should find previous key")
}

@Test("getKey with Selectable protocol")
func getKeyWithSelectable() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_selectable_key".utf8)
    let value: Fdb.Value = [UInt8]("test_selectable_value".utf8)
    newTransaction.setValue(value, for: key)
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()

    // Test with Fdb.Key (which implements Selectable)
    let resultWithKey = try await readTransaction.getKey(selector: key)
    #expect(resultWithKey == key, "getKey with Fdb.Key should work")

    // Test with String (which implements Selectable)
    let stringKey = "test_selectable_key"
    let resultWithString = try await readTransaction.getKey(selector: stringKey)
    #expect(resultWithString == key, "getKey with String should work")
}

@Test("commit transaction")
func testCommit() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    newTransaction.setValue("test_commit_value", for: "test_commit_key")
    let commitResult = try await newTransaction.commit()
    #expect(commitResult == true, "Commit should return true on success")

    // Verify the value was committed by reading in a new transaction
    let readTransaction = try database.createTransaction()
    let retrievedValue = try await readTransaction.getValue(for: "test_commit_key")
    let expectedValue = [UInt8]("test_commit_value".utf8)
    #expect(retrievedValue == expectedValue, "Committed value should be readable in new transaction")
}

// @Test("getVersionstamp")
// func testGetVersionstamp() async throws {
//     try await FdbClient.initialize()
//     let database = try FdbClient.openDatabase()
//     let transaction = try database.createTransaction()

//     // Clear test key range
//     transaction.clearRange(beginKey: "test_", endKey: "test`")
//     _ = try await transaction.commit()

//     let newTransaction = try database.createTransaction()
//     newTransaction.setValue("test_versionstamp_value", for: "test_versionstamp_key")
//     let versionstamp = try await newTransaction.getVersionstamp()
//     #expect(versionstamp != nil, "Versionstamp should not be nil")
//     #expect(versionstamp?.count == 10, "Versionstamp should be 10 bytes")
// }

@Test("cancel transaction")
func testCancel() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    newTransaction.setValue("test_cancel_value", for: "test_cancel_key")
    newTransaction.cancel()

    // After canceling, operations should fail
    do {
        _ = try await newTransaction.getValue(for: "test_cancel_key")
        #expect(Bool(false), "Operations should fail after cancel")
    } catch {
        // Expected to throw an error
        #expect(error is FdbError, "Should throw FdbError after cancel")
    }
}

@Test("setReadVersion and getReadVersion")
func readVersion() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let testVersion: Int64 = 12345
    newTransaction.setReadVersion(testVersion)
    let retrievedVersion = try await newTransaction.getReadVersion()
    #expect(retrievedVersion == testVersion, "Retrieved read version should match set version")
}

@Test("read version with snapshot read")
func readVersionSnapshot() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set a specific read version
    let testVersion: Int64 = 98765
    newTransaction.setReadVersion(testVersion)

    // Test snapshot read with the version
    newTransaction.setValue("test_snapshot_value", for: "test_snapshot_key")
    let value = try await newTransaction.getValue(for: "test_snapshot_key", snapshot: true)
    #expect(value != nil, "Snapshot read should work with set read version")
}

@Test("getRange with byte arrays")
func getRangeBytes() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with byte arrays
    let key1: Fdb.Key = [UInt8]("test_byte_range_001".utf8)
    let key2: Fdb.Key = [UInt8]("test_byte_range_002".utf8)
    let key3: Fdb.Key = [UInt8]("test_byte_range_003".utf8)
    let value1: Fdb.Value = [UInt8]("byte_value1".utf8)
    let value2: Fdb.Value = [UInt8]("byte_value2".utf8)
    let value3: Fdb.Value = [UInt8]("byte_value3".utf8)

    newTransaction.setValue(value1, for: key1)
    newTransaction.setValue(value2, for: key2)
    newTransaction.setValue(value3, for: key3)
    _ = try await newTransaction.commit()

    // Test range query with byte arrays
    let readTransaction = try database.createTransaction()
    let beginKey: Fdb.Key = [UInt8]("test_byte_range_001".utf8)
    let endKey: Fdb.Key = [UInt8]("test_byte_range_003".utf8)
    let result = try await readTransaction.getRange(beginKey: beginKey, endKey: endKey)

    #expect(!result.more)
    try #require(result.records.count == 2, "Should return 2 key-value pairs (end key is exclusive)")

    // Sort results by key for predictable testing
    let sortedResults = result.records.sorted { $0.0.lexicographicallyPrecedes($1.0) }
    #expect(sortedResults[0].0 == key1, "First key should match key1")
    #expect(sortedResults[0].1 == value1, "First value should match value1")
    #expect(sortedResults[1].0 == key2, "Second key should match key2")
    #expect(sortedResults[1].1 == value2, "Second value should match value2")
}

@Test("getRange with limit")
func getRangeWithLimit() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with more entries
    for i in 1 ... 10 {
        let key = String(format: "test_limit_key_%03d", i)
        let value = "limit_value\(i)"
        newTransaction.setValue(value, for: key)
    }
    _ = try await newTransaction.commit()

    // Test with limit
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getRange(beginKey: "test_limit_key_001", endKey: "test_limit_key_999", limit: 3)
    #expect(result.records.count == 3, "Should return exactly 3 key-value pairs due to limit")

    // Verify we got the first 3 keys
    let sortedResults = result.records.sorted { String(bytes: $0.0, encoding: .utf8)! < String(bytes: $1.0, encoding: .utf8)! }

    #expect(String(bytes: sortedResults[0].0, encoding: .utf8) == "test_limit_key_001", "First key should be test_limit_key_001")
    #expect(String(bytes: sortedResults[1].0, encoding: .utf8) == "test_limit_key_002", "Second key should be test_limit_key_002")
    #expect(String(bytes: sortedResults[2].0, encoding: .utf8) == "test_limit_key_003", "Third key should be test_limit_key_003")
}

@Test("getRange empty range")
func getRangeEmpty() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Test empty range
    let result = try await newTransaction.getRange(beginKey: "test_empty_start", endKey: "test_empty_end")

    #expect(result.records.count == 0, "Empty range should return no results")
    #expect(result.records.isEmpty, "Results should be empty")
    #expect(result.more == false, "Should indicate no more results")
}

@Test("getRange with KeySelectors - firstGreaterOrEqual")
func getRangeWithKeySelectors() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data
    let key1: Fdb.Key = [UInt8]("test_selector_001".utf8)
    let key2: Fdb.Key = [UInt8]("test_selector_002".utf8)
    let key3: Fdb.Key = [UInt8]("test_selector_003".utf8)
    let value1: Fdb.Value = [UInt8]("selector_value1".utf8)
    let value2: Fdb.Value = [UInt8]("selector_value2".utf8)
    let value3: Fdb.Value = [UInt8]("selector_value3".utf8)

    newTransaction.setValue(value1, for: key1)
    newTransaction.setValue(value2, for: key2)
    newTransaction.setValue(value3, for: key3)
    _ = try await newTransaction.commit()

    // Test with KeySelectors using firstGreaterOrEqual
    let readTransaction = try database.createTransaction()
    let beginSelector = Fdb.KeySelector.firstGreaterOrEqual(key1)
    let endSelector = Fdb.KeySelector.firstGreaterOrEqual(key3)
    let result = try await readTransaction.getRange(beginSelector: beginSelector, endSelector: endSelector)

    #expect(!result.more)
    try #require(result.records.count == 2, "Should return 2 key-value pairs (end selector is exclusive)")

    // Sort results by key for predictable testing
    let sortedResults = result.records.sorted { $0.0.lexicographicallyPrecedes($1.0) }
    #expect(sortedResults[0].0 == key1, "First key should match key1")
    #expect(sortedResults[0].1 == value1, "First value should match value1")
    #expect(sortedResults[1].0 == key2, "Second key should match key2")
    #expect(sortedResults[1].1 == value2, "Second value should match value2")
}

@Test("getRange with KeySelectors - String keys")
func getRangeWithStringSelectorKeys() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with string keys
    newTransaction.setValue("str_value1", for: "test_str_selector_001")
    newTransaction.setValue("str_value2", for: "test_str_selector_002")
    newTransaction.setValue("str_value3", for: "test_str_selector_003")
    _ = try await newTransaction.commit()

    // Test with String-based KeySelectors
    let readTransaction = try database.createTransaction()
    let beginSelector = Fdb.KeySelector.firstGreaterOrEqual("test_str_selector_001")
    let endSelector = Fdb.KeySelector.firstGreaterOrEqual("test_str_selector_003")
    let result = try await readTransaction.getRange(beginSelector: beginSelector, endSelector: endSelector)

    #expect(!result.more)
    try #require(result.records.count == 2, "Should return 2 key-value pairs")

    // Convert back to strings for easier testing
    let keys = result.records.map { String(bytes: $0.0, encoding: .utf8)! }.sorted()
    let values = result.records.map { String(bytes: $0.1, encoding: .utf8)! }

    #expect(keys.contains("test_str_selector_001"), "Should contain first key")
    #expect(keys.contains("test_str_selector_002"), "Should contain second key")
    #expect(!keys.contains("test_str_selector_003"), "Should not contain end key (exclusive)")
}

@Test("getRange with Selectable protocol - mixed types")
func getRangeWithSelectable() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data
    newTransaction.setValue("mixed_value1", for: "test_mixed_001")
    newTransaction.setValue("mixed_value2", for: "test_mixed_002")
    newTransaction.setValue("mixed_value3", for: "test_mixed_003")
    _ = try await newTransaction.commit()

    // Test using the general Selectable protocol with mixed key types
    let readTransaction = try database.createTransaction()
    let beginKey: Fdb.Key = [UInt8]("test_mixed_001".utf8)
    let endString = "test_mixed_003"
    let result = try await readTransaction.getRange(begin: beginKey, end: endString)

    #expect(!result.more)
    try #require(result.records.count == 2, "Should return 2 key-value pairs")

    let keys = result.records.map { String(bytes: $0.0, encoding: .utf8)! }.sorted()
    #expect(keys.contains("test_mixed_001"), "Should contain first key")
    #expect(keys.contains("test_mixed_002"), "Should contain second key")
}

@Test("KeySelector static methods with different offsets")
func keySelectorMethods() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data
    newTransaction.setValue("offset_value1", for: "test_offset_001")
    newTransaction.setValue("offset_value2", for: "test_offset_002")
    newTransaction.setValue("offset_value3", for: "test_offset_003")
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()

    // Test firstGreaterThan vs firstGreaterOrEqual
    let beginSelectorGTE = Fdb.KeySelector.firstGreaterOrEqual("test_offset_002")
    let beginSelectorGT = Fdb.KeySelector.firstGreaterThan("test_offset_002")
    let endSelector = Fdb.KeySelector.firstGreaterOrEqual("test_offset_999")

    let resultGTE = try await readTransaction.getRange(beginSelector: beginSelectorGTE, endSelector: endSelector)
    let resultGT = try await readTransaction.getRange(beginSelector: beginSelectorGT, endSelector: endSelector)

    // firstGreaterOrEqual should include test_offset_002
    let keysGTE = resultGTE.records.map { String(bytes: $0.0, encoding: .utf8)! }.sorted()
    #expect(keysGTE.contains("test_offset_002"), "firstGreaterOrEqual should include the key")

    // firstGreaterThan should exclude test_offset_002 and start from test_offset_003
    let keysGT = resultGT.records.map { String(bytes: $0.0, encoding: .utf8)! }.sorted()
    #expect(!keysGT.contains("test_offset_002"), "firstGreaterThan should exclude the key")
    #expect(keysGT.contains("test_offset_003"), "firstGreaterThan should include next key")
}

@Test("withTransaction success")
func withTransactionSuccess() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    // Test successful withTransaction
    let result = try await database.withTransaction { transaction in
        transaction.setValue("success_value", for: "test_with_transaction_key")
        return "operation_completed"
    }

    #expect(result == "operation_completed", "withTransaction should return the operation result")

    // Verify the value was committed
    let verifyTransaction = try database.createTransaction()
    let retrievedValue = try await verifyTransaction.getValue(for: "test_with_transaction_key")
    let expectedValue = [UInt8]("success_value".utf8)
    #expect(retrievedValue == expectedValue, "Value should be committed after withTransaction")
}

@Test("withTransaction with exception in operation")
func withTransactionException() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    struct TestError: Error {}

    do {
        _ = try await database.withTransaction { transaction in
            transaction.setValue("exception_value", for: "test_with_transaction_exception")
            throw TestError()
        }
        #expect(Bool(false), "withTransaction should propagate thrown exceptions")
    } catch is TestError {
        // Expected behavior
    } catch {
        #expect(Bool(false), "Should catch TestError, got \(error)")
    }

    // Verify the value was NOT committed due to exception
    let verifyTransaction = try database.createTransaction()
    let retrievedValue = try await verifyTransaction.getValue(for: "test_with_transaction_exception")
    #expect(retrievedValue == nil, "Value should not be committed when exception occurs")
}

@Test("withTransaction with non-retryable error")
func withTransactionNonRetryableError() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    do {
        _ = try await database.withTransaction { transaction in
            transaction.setValue("non_retryable_value", for: "test_with_transaction_non_retryable")
            // Throw a non-retryable FDB error (transaction_cancelled)
            throw FdbError(.transactionCancelled)
        }
        #expect(Bool(false), "withTransaction should propagate non-retryable errors")
    } catch let error as FdbError {
        #expect(error.code == FdbErrorCode.transactionCancelled.rawValue, "Should propagate the exact FdbError")
        #expect(!error.isRetryable, "Error should be non-retryable")
    } catch {
        #expect(Bool(false), "Should catch FdbError, got \(error)")
    }
}

@Test("withTransaction returns value from operation")
func withTransactionReturnValue() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    // Test that withTransaction returns the correct value
    let stringResult = try await database.withTransaction { transaction in
        transaction.setValue("return_test_value", for: "test_return_key")
        return "success"
    }
    #expect(stringResult == "success", "Should return string value from operation")

    let intResult = try await database.withTransaction { transaction in
        transaction.setValue("return_test_value2", for: "test_return_key2")
        return 42
    }
    #expect(intResult == 42, "Should return integer value from operation")

    let arrayResult = try await database.withTransaction { transaction in
        try await transaction.getValue(for: "test_return_key")
    }
    let expectedValue = [UInt8]("return_test_value".utf8)
    #expect(arrayResult == expectedValue, "Should return retrieved value from operation")
}

@Test("withTransaction Sendable compliance")
func withTransactionSendable() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    // Test with Sendable types
    struct SendableData: Sendable {
        let id: Int
        let name: String
    }

    let result = try await database.withTransaction { transaction in
        transaction.setValue("sendable_value", for: "test_sendable_key")
        return SendableData(id: 123, name: "test")
    }

    #expect(result.id == 123, "Should return sendable struct with correct id")
    #expect(result.name == "test", "Should return sendable struct with correct name")
}

@Test("FdbError isRetryable property")
func fdbErrorRetryable() {
    // Test retryable errors
    let notCommittedError = FdbError(.notCommitted)
    #expect(notCommittedError.isRetryable, "not_committed should be retryable")

    let transactionTooOldError = FdbError(.transactionTooOld)
    #expect(transactionTooOldError.isRetryable, "transaction_too_old should be retryable")

    let futureVersionError = FdbError(.futureVersion)
    #expect(futureVersionError.isRetryable, "future_version should be retryable")

    let transactionTimedOutError = FdbError(.transactionTimedOut)
    #expect(transactionTimedOutError.isRetryable, "transaction_timed_out should be retryable")

    let processBehindError = FdbError(.processBehind)
    #expect(processBehindError.isRetryable, "process_behind should be retryable")

    let tagThrottledError = FdbError(.tagThrottled)
    #expect(tagThrottledError.isRetryable, "tag_throttled should be retryable")

    // Test non-retryable errors
    let transactionCancelledError = FdbError(.transactionCancelled)
    #expect(!transactionCancelledError.isRetryable, "transaction_cancelled should not be retryable")

    let unknownError = FdbError(.unknownError)
    #expect(!unknownError.isRetryable, "unknown error should not be retryable")

    let internalError = FdbError(.internalError)
    #expect(!internalError.isRetryable, "internal_error should not be retryable")
}

@Test("atomic operation ADD")
func atomicOpAdd() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_add".utf8)

    // Initial value: little-endian 64-bit integer 10
    let initialValue: Fdb.Value = withUnsafeBytes(of: Int64(10).littleEndian) { Array($0) }
    newTransaction.setValue(initialValue, for: key)

    // Add 5 using atomic operation
    let addValue: Fdb.Value = withUnsafeBytes(of: Int64(5).littleEndian) { Array($0) }
    newTransaction.atomicOp(key: key, param: addValue, mutationType: .add)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultValue = result!.withUnsafeBytes { $0.load(as: Int64.self) }
    #expect(Int64(littleEndian: resultValue) == 15, "10 + 5 should equal 15")
}

@Test("atomic operation BIT_AND")
func atomicOpBitAnd() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_and".utf8)

    // Initial value: 0xFF (255)
    let initialValue: Fdb.Value = [0xFF]
    newTransaction.setValue(initialValue, for: key)

    // AND with 0x0F (15)
    let andValue: Fdb.Value = [0x0F]
    newTransaction.atomicOp(key: key, param: andValue, mutationType: .bitAnd)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    #expect(result! == [0x0F], "0xFF AND 0x0F should equal 0x0F")
}

@Test("atomic operation BIT_OR")
func atomicOpBitOr() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_or".utf8)

    // Initial value: 0x0F (15)
    let initialValue: Fdb.Value = [0x0F]
    newTransaction.setValue(initialValue, for: key)

    // OR with 0xF0 (240)
    let orValue: Fdb.Value = [0xF0]
    newTransaction.atomicOp(key: key, param: orValue, mutationType: .bitOr)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    #expect(result! == [0xFF], "0x0F OR 0xF0 should equal 0xFF")
}

@Test("atomic operation BIT_XOR")
func atomicOpBitXor() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_xor".utf8)

    // Initial value: 0xFF (255)
    let initialValue: Fdb.Value = [0xFF]
    newTransaction.setValue(initialValue, for: key)

    // XOR with 0x0F (15)
    let xorValue: Fdb.Value = [0x0F]
    newTransaction.atomicOp(key: key, param: xorValue, mutationType: .bitXor)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    #expect(result! == [0xF0], "0xFF XOR 0x0F should equal 0xF0")
}

@Test("atomic operation MAX")
func atomicOpMax() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_max".utf8)

    // Initial value: little-endian 64-bit integer 10
    let initialValue: Fdb.Value = withUnsafeBytes(of: Int64(10).littleEndian) { Array($0) }
    newTransaction.setValue(initialValue, for: key)

    // Max with 15
    let maxValue: Fdb.Value = withUnsafeBytes(of: Int64(15).littleEndian) { Array($0) }
    newTransaction.atomicOp(key: key, param: maxValue, mutationType: .max)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultValue = result!.withUnsafeBytes { $0.load(as: Int64.self) }
    #expect(Int64(littleEndian: resultValue) == 15, "max(10, 15) should equal 15")
}

@Test("atomic operation MIN")
func atomicOpMin() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_min".utf8)

    // Initial value: little-endian 64-bit integer 10
    let initialValue: Fdb.Value = withUnsafeBytes(of: Int64(10).littleEndian) { Array($0) }
    newTransaction.setValue(initialValue, for: key)

    // Min with 5
    let minValue: Fdb.Value = withUnsafeBytes(of: Int64(5).littleEndian) { Array($0) }
    newTransaction.atomicOp(key: key, param: minValue, mutationType: .min)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultValue = result!.withUnsafeBytes { $0.load(as: Int64.self) }
    #expect(Int64(littleEndian: resultValue) == 5, "min(10, 5) should equal 5")
}

@Test("atomic operation APPEND_IF_FITS")
func atomicOpAppendIfFits() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_append".utf8)

    // Initial value: "Hello"
    let initialValue: Fdb.Value = [UInt8]("Hello".utf8)
    newTransaction.setValue(initialValue, for: key)

    // Append " World"
    let appendValue: Fdb.Value = [UInt8](" World".utf8)
    newTransaction.atomicOp(key: key, param: appendValue, mutationType: .appendIfFits)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultString = String(bytes: result!, encoding: .utf8)
    #expect(resultString == "Hello World", "Should append ' World' to 'Hello'")
}

@Test("atomic operation BYTE_MIN")
func atomicOpByteMin() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_byte_min".utf8)

    // Initial value: "zebra"
    let initialValue: Fdb.Value = [UInt8]("zebra".utf8)
    newTransaction.setValue(initialValue, for: key)

    // Compare with "apple" (lexicographically smaller)
    let compareValue: Fdb.Value = [UInt8]("apple".utf8)
    newTransaction.atomicOp(key: key, param: compareValue, mutationType: .byteMin)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultString = String(bytes: result!, encoding: .utf8)
    #expect(resultString == "apple", "byte_min should choose lexicographically smaller value")
}

@Test("atomic operation BYTE_MAX")
func atomicOpByteMax() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: Fdb.Key = [UInt8]("test_atomic_byte_max".utf8)

    // Initial value: "apple"
    let initialValue: Fdb.Value = [UInt8]("apple".utf8)
    newTransaction.setValue(initialValue, for: key)

    // Compare with "zebra" (lexicographically larger)
    let compareValue: Fdb.Value = [UInt8]("zebra".utf8)
    newTransaction.atomicOp(key: key, param: compareValue, mutationType: .byteMax)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultString = String(bytes: result!, encoding: .utf8)
    #expect(resultString == "zebra", "byte_max should choose lexicographically larger value")
}
