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

public class FdbDatabase {
    private let database: OpaquePointer

    init(database: OpaquePointer) {
        self.database = database
    }

    deinit {
        fdb_database_destroy(database)
    }

    public func createTransaction() throws -> FdbTransaction {
        var transaction: OpaquePointer?
        let error = fdb_database_create_transaction(database, &transaction)
        if error != 0 {
            throw FdbError(code: error)
        }

        guard let tr = transaction else {
            throw FdbError(code: 2000)  // internal_error
        }

        return FdbTransaction(transaction: tr)
    }

    public func withTransaction<T: Sendable>(
        _ operation: (FdbTransaction) async throws -> T
    ) async throws -> T {
        let maxRetries = 100  // TODO: Remove this.

        for attempt in 0..<maxRetries {
            let transaction = try createTransaction()

            do {
                let result = try await operation(transaction)
                let committed = try await transaction.commit()

                if committed {
                    return result
                }
            } catch {
                // TODO: If user wants to cancel, don't retry.
                transaction.cancel()

                if let fdbError = error as? FdbError, fdbError.isRetryable {
                    if attempt < maxRetries - 1 {
                        continue
                    }
                }

                throw error
            }
        }

        throw FdbError(code: 1020)  // transaction_too_old
    }
}
