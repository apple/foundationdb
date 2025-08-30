/*
 * Network.swift
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
import Foundation

// TODO: stopNetwork at deinit.
@MainActor
class FdbNetwork {
    static let shared = FdbNetwork()

    private var networkSetup = false
    private var networkThread: Thread?

    func initialize(version: Int32) throws {
        if networkSetup {
            // throw FdbError(code: 2201)
            return
        }

        try selectAPIVersion(version)
        try setupNetwork()
        startNetwork()
    }

    func selectAPIVersion(_ version: Int32) throws {
        let error = fdb_select_api_version_impl(version, FDB_API_VERSION)
        if error != 0 {
            throw FdbError(code: error)
        }
    }

    func setupNetwork() throws {
        guard !networkSetup else {
            throw FdbError(.networkError)
        }

        let error = fdb_setup_network()
        if error != 0 {
            throw FdbError(code: error)
        }

        networkSetup = true
    }

    func startNetwork() {
        guard networkSetup else {
            fatalError("Network must be setup before starting network thread")
        }

        networkThread = Thread {
            let error = fdb_run_network()
            if error != 0 {
                print("Network thread error: \(FdbError(code: error).description)")
            }
        }
        networkThread?.start()
    }

    func stopNetwork() throws {
        let error = fdb_stop_network()
        if error != 0 {
            throw FdbError(code: error)
        }

        networkThread?.cancel() // TODO: Is there a join() method?
        networkThread = nil
        networkSetup = false
    }
}
