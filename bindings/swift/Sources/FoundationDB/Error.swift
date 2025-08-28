/*
 * Error.swift
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

public struct FdbError: Error, CustomStringConvertible {
    public let code: Int32

    public init(code: Int32) {
        self.code = code
    }

    public var description: String {
        guard let errorCString = fdb_get_error(code) else {
            return "Unknown FDB error: \(code)"
        }
        return String(cString: errorCString)
    }

    public var isRetryable: Bool {
        switch code {
        case 1007:  // not_committed
            return true
        case 1020:  // transaction_too_old
            return true
        case 1021:  // future_version
            return true
        case 1025:  // transaction_cancelled
            return false
        case 1031:  // transaction_timed_out
            return true
        case 1037:  // process_behind
            return true
        case 1213:  // tag_throttled
            return true
        default:
            return false
        }
    }
}
