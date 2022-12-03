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

struct StreamTests: SimpleSwiftTestSuite {
    var tests: [TestCase] {
        /// Corresponds to FlowTests.actor.cpp "/flow/flow/trivial promisestreams"
        TestCase("PromiseStream: await stream.waitNext") {
            var ps = PromiseStreamCInt()
            var fs: FutureStreamCInt = ps.__getFutureUnsafe()

            var i: CInt = 1
            ps.send(&i)
            precondition(ps.__getFutureUnsafe().isReady())
            precondition(fs.pop() == 1)

            i += 1
            ps.send(&i)

            let element = try? await fs.waitNext
            precondition(element == 2)
        }

        TestCase("PromiseStream: as Swift AsyncSequence") {
            let ps = PromiseStreamCInt()
            let fs: FutureStreamCInt = ps.__getFutureUnsafe()

            Task { [ps] in
                var ps = ps

                var i: CInt = 1
                await sleepALittleBit()
                ps.send(&i)

                i = 2
                await sleepALittleBit()
                ps.send(&i)

                i = 3
                await sleepALittleBit()
                ps.send(&i)

                ps.sendError(end_of_stream())
            }

            pprint("[stream] waiting async for loop on FutureStream")
            var sum: CInt = 0
            let expected: CInt = 1 + 2 + 3

            for try await num in fs {
                pprint("[stream] got num = \(num)")
                sum += num
            }

            pprint("[stream] done")
            precondition(sum == expected, "Expected \(expected) but got \(sum)")
        }

        /// This test showcases a semantic 1:1 equivalent of a "loop choose {}"
        /// It should be more efficient since we're using child tasks inside a task group,
        /// which can benefit from being allocated inside the parent task.
        ///
        /// Equivalent to `tutorial.actor.cpp::someFuture(Future<int> ready)`
        TestCase("1:1 simulate a 'loop choose {}' in Swift, with TaskGroup") {
            //    ACTOR Future<Void> someFuture(Future<int> ready) {
            //        loop choose {
            //            when(wait(delay(0.5))) { std::cout << "Still waiting...\n"; }
            //            when(int r = wait(ready)) {
            //                std::cout << format("Ready %d\n", r);
            //                wait(delay(double(r)));
            //                std::cout << "Done\n";
            //                return Void();
            //            }
            //        }
            //    }
            let promise = PromiseVoid()

            Task {
                try? await FlowClock.sleep(for: .seconds(1))
                pprint("[stream] Complete promise...")
                var void = Void()
                promise.send(&void)
            }

            enum Action {
                case wait
                case ready(CInt)
                case _ignore

                var isWait: Bool {
                    switch self {
                    case .wait: return true
                    default: return false
                    }
                }
                var isReady: Bool {
                    switch self {
                    case .ready: return true
                    default: return false
                    }
                }
            }

            let out = await withTaskGroup(of: Action.self) { group in
                var lastCollected: Action? = nil
                while true {
                    if lastCollected?.isWait ?? true {
                        group.addTask {
                            // When the group/task gets cancelled, the sleep will throw
                            // we ignore the throw though
                            try? await FlowClock.sleep(for: .seconds(1))
                            return .wait
                        }
                    }

                    if lastCollected?.isReady ?? true {
                        group.addTask {
                            var future = promise.__getFutureUnsafe()
                            let _ = try! await future.waitValue
                            return .ready(12)
                        }
                    }

                    lastCollected = await group.next()
                    switch lastCollected {
                    case .wait:
                        pprint("[stream] Still waiting...")
                    case .ready(let r):
                        pprint("[stream] Ready: \(r)")
                        group.cancelAll()
                        return r
                    default:
                        fatalError("should not happen, was: \(String(describing: lastCollected))")
                    }
                }
            }

            // assert the value we got out of the group is what we sent into it
            precondition(out == 12)
        }

        TestCase("Tasks consuming multiple streams, send requests into actor") {
            let p = PromiseVoid()

            actor Cook {
                let p: PromiseVoid
                var expected: Int
                var completed: Int

                init(p: PromiseVoid, expectedTasks expected: Int) {
                    self.p = p
                    self.expected = expected
                    self.completed = 0
                }

                func cook(task: CInt, delay: Duration) async throws -> String {
                    pprint("Inside Cook actor to handle: \(task)")
                    defer { pprint("Finished Cook actor to handle: \(task)") }

                    try await FlowClock.sleep(for: delay)

                    completed += 1
                    if completed >= expected {
                        var void = Flow.Void()
                        p.send(&void)
                    }

                    return "done:\(task)"
                }
            }

            let expectedTasks = 4
            let cook = Cook(p: p, expectedTasks: expectedTasks)

            let ps1 = PromiseStreamCInt()
            let ps2 = PromiseStreamCInt()

            // Spawn 2 tasks, one for consuming each of the streams.
            // These tasks will keep looping on the future streams "forever" - until the tasks are cancelled.
            let t1 = Task { [ps1] in
                let fs: FutureStreamCInt = ps1.__getFutureUnsafe()
                for try await t in fs {
                    pprint("send Task to cook: \(t) (from ps1)")
                    Task {
                        pprint("Inside Task to cook: \(t) (from ps1)")
                        let res = try await cook.cook(task: t, delay: .milliseconds(500))
                        pprint("Inside Task to cook: \(t) (from ps1), returned: \(res)")
                        precondition(res == "done:\(t)")
                    }
                }
            }
            let t2 = Task { [ps2] in
                let fs: FutureStreamCInt = ps2.__getFutureUnsafe()
                for try await t in fs {
                    pprint("send Task to cook: \(t) (from ps2)")
                    Task {
                        pprint("Inside Task to cook: \(t) (from ps2)")
                        let res = try await cook.cook(task: t, delay: .milliseconds(500))
                        pprint("Inside Task to cook: \(t) (from ps1), returned: \(res)")
                        precondition(res == "done:\(t)")
                    }
                }
            }

            // When we exit this scope, cancel all the tasks
            // TODO: we need to implement swift cancellation causing interruption of consuming a stream
            defer {
                t1.cancel()
                t2.cancel()
            }

            // Start another task that will feed events into the streams
            await Task { [ps1, ps2] in
                var ps1 = ps1
                var ps2 = ps2

                try! await FlowClock.sleep(for: .milliseconds(120))
                ps1.sendCopy(1)
                try! await FlowClock.sleep(for: .milliseconds(120))
                ps2.sendCopy(10)
                try! await FlowClock.sleep(for: .milliseconds(120))
                ps1.sendCopy(2)
                ps2.sendCopy(20)
            }.value

            var f = p.__getFutureUnsafe()
            _ = try await f.waitValue
            pprint("All done")
        }
    }
}

fileprivate func sleepALittleBit() async {
    try? await Task.sleep(until: .now.advanced(by: .milliseconds(200)), clock: .flow)
}

