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

@_expose(Cxx)
public func swiftyTestRunner(p: PromiseVoid) {
    print("[swift] \(#function), go!".green)

    Task {
        do {
            await swift_flow_trivial_promisestreams()
            try await swift_flow_trivial_promisestreams_asyncSequence()
        } catch {
            print("[swift][\(#function)] TEST THREW: \(error)")
        }

        var void = Flow.Void()
        p.send(&void)
    }
}

/// Corresponds to FlowTests.actor.cpp "/flow/flow/trivial promisestreams"
func swift_flow_trivial_promisestreams() async {
    print("[swift] Test: \(#function) ------------------------------------------------------------".yellow)
    defer { print("[swift] Finished: \(#function) ------------------------------------------------------------".green) }

    var ps = PromiseStreamCInt()
    var fs: FutureStreamCInt = ps.__getFutureUnsafe()

    var i: CInt = 1
    ps.send(&i)
    precondition(ps.__getFutureUnsafe().isReady())
    precondition(fs.pop() == 1)

    i += 1
    ps.send(&i)
//    var pf: Void = ps.getFuture()
//    let got = try await pf.next()

    var element = try? await fs.waitNext
    precondition(element == 2)
}

func swift_flow_trivial_promisestreams_asyncSequence() async throws {
    print("[swift] Test: \(#function) ------------------------------------------------------------".yellow)
    defer { print("[swift] Finished: \(#function) ------------------------------------------------------------".green) }

    var ps = PromiseStreamCInt()
    var fs: FutureStreamCInt = ps.__getFutureUnsafe()

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

fileprivate func sleepALittleBit() async {
    try? await Task.sleep(until: .now.advanced(by: .milliseconds(200)), clock: .flow)
}

