/*
 * swift_test_streams.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import Flow
import flow_swift

// Don't do this at home;
// We assume we run single-threadedly in Net2 in these tests,
// as such, we access this global variable without synchronization and it is safe.
var _mainThreadID = _tid()

let SimpleSwiftTestSuites: [any SimpleSwiftTestSuite.Type] = [
    TaskTests.self,
    StreamTests.self,
]


@_expose(Cxx)
public func swiftyTestRunner(p: PromiseVoid) {
    Task {
        do {
            try await SimpleSwiftTestRunner().run()
        } catch {
            print("[swift] Test suite failed: \(error)".red)
        }

        p.send(Flow.Void())
    }
}

// ==== ---------------------------------------------------------------------------------------------------------------

func assertOnNet2EventLoop() {
    precondition(_tid() == _mainThreadID) // we're on the main thread, which the Net2 runloop runs on
}
