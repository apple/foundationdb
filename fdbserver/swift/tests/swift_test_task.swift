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

struct TaskTests: SimpleSwiftTestSuite {

    var tests: [TestCase] {
        TestCase("await \(FutureVoid.self)") {
            let p = PromiseVoid()
            var voidF = p.__getFutureUnsafe()

            var void = Flow.Void()
            p.send(&void)
            _ = try await voidF.waitValue
        }

        // FIXME(swift): Somehow the waitValue is broken on FutureCInt: https://github.com/FoundationDB/foundationdb-swift/pull/31
//        TestCase("await \(FutureCInt.self)") {
//            let p = PromiseCInt()
//            var intF: FutureCInt = p.__getFutureUnsafe()
//
//            var value: CInt = 42
//            p.send(&value)
//            let got = try await intF.waitValue
//            precondition(got == value, "\(got) did not equal \(value)")
//        }

        // FIXME(swift): Somehow the waitValue is broken on FutureCInt: https://github.com/FoundationDB/foundationdb-swift/pull/31
//        TestCase("more Flow task await tests") {
//            let p: PromiseCInt = PromiseCInt()
//            var f: FutureCInt = p.__getFutureUnsafe() // FIXME(swift): getFuture: C++ method 'getFuture' that returns unsafe projection of type 'Future' not imported
//            // TODO(swift): we perhaps should add a note that __getFutureUnsafe is available?
//
//            pprint("got PromiseCInt") // FIXME(swift/c++): printing the promise crashes!
//            precondition(!f.isReady(), "Future should not be ready yet")
//
//            var num = 1111
//            pprint("send \(num)") // FIXME: printing the promise crashes!
//            p.send(&num) // FIXME: rdar://99583467 ([C++ interop][fdb] Support xvalues, so we can use Future.send(U&& value))
//            pprint("without wait, f.get(): \(f.__getUnsafe().pointee)")
//
//            pprint("wait...")
//            let value: CInt = try await f.waitValue
//            assertOnNet2EventLoop() // hopped back to the right executor, yay
//            precondition(f.isReady(), "Future should be ready by now")
//
//            pprint("await value = \(value)")
//            precondition((value ?? -1) == num, "Value obtained from await did not match \(num), was: \(String(describing: value))!")
//
//            pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2 --------------------")
//            let p2 = PromiseCInt()
//            var f2: FutureCInt = p2.__getFutureUnsafe() // FIXME: Make these not unsafe...
//            let num2 = 2222
//            Task { [num2] in
//                assertOnNet2EventLoop()
//
//                pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: send \(num2)")
//                var workaroundVar = num2 // FIXME workaround since we need inout xvalue for the C++ send()
//                p2.send(&workaroundVar)
//            }
//            pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: waiting...")
//            let got2: CInt? = try? await f2.waitValue
//            pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2, got: \(String(describing: got2))")
//            precondition(got2! == num2, "Value obtained from send after await did not match \(num2), was: \(String(describing: got2))!")
//
//            // assert that we hopped back and are again on the Net2 event loop thread:
//            assertOnNet2EventLoop()
//        }

        TestCase("Flow task priorities in Task.priority") {
            await Task { assertOnNet2EventLoop() }.value
            assertOnNet2EventLoop()

            // Note that we can assign Flow priorities to tasks explicitly:
            pprint("Parent task priority: \(Task.currentPriority)")
            pprint("Execute task with priority: \(_Concurrency.TaskPriority.Worker)")
            precondition(_Concurrency.TaskPriority.Worker.rawValue == 60, "WAS: \(_Concurrency.TaskPriority.Worker.rawValue) wanted: 60")

            await Task(priority: .Worker) {
                pprint("Task executed, with priority: \(Task.currentPriority)")
                precondition(Task.currentPriority == .Worker)
                assertOnNet2EventLoop()
            }.value
            assertOnNet2EventLoop()

            pprint("Properly resumed...")
        }
    }
}
