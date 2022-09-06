import Flow
//import std

print("[swift] start")

installGlobalSwiftConcurrencyHooks() // hook swift concurrency up to the net runloop

// capture the main thread ID
let mainTID = _tid()
func assertOnNet2EventLoop() {
    precondition(_tid() == mainTID) // we're on the main thread, which the Net2 runloop runs on
}

func swiftAsyncFunc() async {
    assertOnNet2EventLoop()
    print("[swift][\(#fileID):\(#line)](\(#function)) executing inside \(#function)")
    await Task {
        assertOnNet2EventLoop()
    }.value
    assertOnNet2EventLoop()
    print("[swift][tid:\(_tid())] Properly resumed...")
}

func swiftFlowFutureAwait() async {
    assertOnNet2EventLoop()

    let p: FlowPromiseInt = makePromiseInt()
    let f: FlowFutureInt = getFutureOfPromise(p) // FIXME(swift/c++): can't have a getFuture on the p because templates...
    print("[swift][\(#fileID):\(#line)](\(#function)) got FlowPromiseInt") // FIXME(swift/c++): printing the promise crashes!
    precondition(!f.isReady(), "Future should not be ready yet")

    var num = 1111
    print("[swift][\(#fileID):\(#line)](\(#function)) send \(num)") // FIXME: printing the promise crashes!
    p.send(&num) // FIXME: rdar://99583467 ([C++ interop][fdb] Support xvalues, so we can use Future.send(U&& value))
    print("[swift][\(#fileID):\(#line)](\(#function)) without wait, f.get(): \(f.get().pointee)")

    print("[swift][\(#fileID):\(#line)](\(#function)) wait...")
    let value: CInt? = try? await f.waitValue
    assertOnNet2EventLoop() // hopped back to the right executor, yay
    precondition(f.isReady(), "Future should be ready by now")

    print("[swift][\(#fileID):\(#line)](\(#function)) await value = \(value ?? -1)")
    precondition((value ?? -1) == num, "Value obtained from await did not match \(num), was: \(value)!")

    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2 --------------------")
    let p2 = makePromiseInt()
    let f2 = getFutureOfPromise(p2)
    let num2 = 2222
    Task { [num2] in
        assertOnNet2EventLoop()

        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: send \(num2)")
        var workaroundVar = num2 // FIXME workaround since we need inout xvalue for the C++ send()
        p2.send(&workaroundVar)
    }
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: waiting...")
    let got2: CInt? = try? await f2.waitValue
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2, got: \(got2)")
    precondition(got2! == num2, "Value obtained from send after await did not match \(num2), was: \(got2)!")

    // assert that we hopped back and are again on the Net2 event loop thread:
    assertOnNet2EventLoop()
}

let task = Task { // task execution will be intercepted
    assertOnNet2EventLoop()

//    print("[swift] await test -------------------------------------------------")
//    print("[swift][tid:\(_tid())] Started a task...")
//    await swiftAsyncFunc()
//    print("[swift] ==== done ---------------------------------------------------")

    print("[swift] futures test -----------------------------------------------")
    print("[swift] returned from 'await swiftAsyncFunc()'")
    await swiftFlowFutureAwait()
    print("[swift] returned from 'await swiftFlowFutureAwait()'")
    print("[swift] ==== done ---------------------------------------------------")

    exit(0)
}

// ==== ---------------------------------------------------------------------------------------------------------------

print("[swift][tid:\(_tid())] run network @ thread:\(_tid())")
globalNetworkRun()
