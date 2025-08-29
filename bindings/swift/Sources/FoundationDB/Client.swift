/*
 * Client.swift
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

// TODO: Remove hard-coded error codes.
public class FdbClient {
    public enum APIVersion {
        public static let current: Int32 = 740
    }

    @MainActor
    public static func initialize(version: Int32 = APIVersion.current) async throws {
        try FdbNetwork.shared.initialize(version: version)
    }

    public static func openDatabase(clusterFilePath: String? = nil) throws -> FdbDatabase {
        var database: OpaquePointer?
        let error = fdb_create_database(clusterFilePath, &database)
        if error != 0 {
            throw FdbError(code: error)
        }

        guard let db = database else {
            throw FdbError(.clientError)
        }

        return FdbDatabase(database: db)
    }
}
