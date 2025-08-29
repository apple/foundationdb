/*
 * Database.swift
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

public class FdbDatabase: IDatabase {
    private let database: OpaquePointer

    init(database: OpaquePointer) {
        self.database = database
    }

    deinit {
        fdb_database_destroy(database)
    }

    public func createTransaction() throws -> any ITransaction {
        var transaction: OpaquePointer?
        let error = fdb_database_create_transaction(database, &transaction)
        if error != 0 {
            throw FdbError(code: error)
        }

        guard let tr = transaction else {
            throw FdbError(.internalError)
        }

        return FdbTransaction(transaction: tr)
    }
}
