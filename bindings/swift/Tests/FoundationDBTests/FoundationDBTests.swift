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

import Testing
@testable import FoundationDB

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
func testSetValueBytes() async throws {
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
func testSetValueStrings() async throws {
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
func testClearBytes() async throws {
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
func testClearStrings() async throws {
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
func testClearRangeBytes() async throws {
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
func testClearRangeStrings() async throws {
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
func testGetKeyWithKeySelector() async throws {
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
func testGetKeyWithDifferentSelectors() async throws {
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
func testGetKeyWithSelectable() async throws {
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
func testReadVersion() async throws {
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
func testReadVersionSnapshot() async throws {
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
func testGetRangeBytes() async throws {
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
func testGetRangeWithLimit() async throws {
    try await FdbClient.initialize()
    let database = try FdbClient.openDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with more entries
    for i in 1...10 {
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
func testGetRangeEmpty() async throws {
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
func testGetRangeWithKeySelectors() async throws {
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
func testGetRangeWithStringSelectorKeys() async throws {
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
func testGetRangeWithSelectable() async throws {
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
func testKeySelectorMethods() async throws {
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
