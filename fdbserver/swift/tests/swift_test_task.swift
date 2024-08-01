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

struct TaskTests: SimpleSwiftTestSuite {

    var tests: [TestCase] {
        TestCase("await \(FutureVoid.self)") {
            let p = PromiseVoid()
            let voidF = p.getFuture()

            p.send(Flow.Void())
            _ = try await voidF.value()
        }

        TestCase("await \(FutureCInt.self)") {
            let p = PromiseCInt()
            let intF: FutureCInt = p.getFuture()

            let value: CInt = 42
            p.send(value)
            let got = try await intF.value()
            precondition(got == value, "\(got) did not equal \(value)")
        }

        TestCase("more Flow task await tests") {
            let p: PromiseCInt = PromiseCInt()
            let f: FutureCInt = p.getFuture()

            pprint("got PromiseCInt")
            precondition(!f.isReady(), "Future should not be ready yet")

            let num: CInt = 1111
            pprint("send \(num)")
            p.send(num)
            pprint("without wait, f.get(): \(f.__getUnsafe().pointee)")

            pprint("wait...")
            let value: CInt = try await f.value()
            assertOnNet2EventLoop() // hopped back to the right executor, yay
            precondition(f.isReady(), "Future should be ready by now")

            pprint("await value = \(value)")
            precondition((value) == num, "Value obtained from await did not match \(num), was: \(String(describing: value))!")

            pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2 --------------------")
            let p2 = PromiseCInt()
            let f2: FutureCInt = p2.getFuture()
            let num2: CInt = 2222
            Task { [num2] in
                assertOnNet2EventLoop()

                pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: send \(num2)")
                p2.send(num2)
            }
            pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: waiting...")
            let got2: CInt? = try? await f2.value()
            pprint("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2, got: \(String(describing: got2))")
            precondition(got2! == num2, "Value obtained from send after await did not match \(num2), was: \(String(describing: got2))!")

            // assert that we hopped back and are again on the Net2 event loop thread:
            assertOnNet2EventLoop()
        }

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
