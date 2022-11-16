/*
 * swift_test_streams.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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
import FlowFutureSupport
import flow_swift_future
import FDBClient
import fdbclient_swift

// Don't do this at home;
// We assume we run single-threadedly in Net2 in these tests,
// as such, we access this global variable without synchronization and it is safe.
var _mainThreadID = _tid()

@_expose(Cxx)
public func swiftyTestRunner(p: PromiseVoid) {
    print("[swift] \(#function): Execute all tests!".green)

    Task {
        do {
            try await swift_flow_voidFuture_await()
//            try await swift_flow_task_await()
//            try await swift_flow_task_priority()

            try await swift_flow_trivial_promisestreams()
            try await swift_flow_trivial_promisestreams_asyncSequence()
        } catch {
            print("[swift][\(#function)] TEST THREW: \(error)")
        }

        var void = Flow.Void()
        p.send(&void)
    }
}

// ==== ---------------------------------------------------------------------------------------------------------------

func assertOnNet2EventLoop() {
    precondition(_tid() == _mainThreadID) // we're on the main thread, which the Net2 runloop runs on
}
