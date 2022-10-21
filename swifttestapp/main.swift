import Flow
import Distributed

<<<<<<< Updated upstream
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

    // Note that we can assign Flow priorities to tasks explicitly:
    print("[swift][tid:\(_tid())] Parent task priority: \(Task.currentPriority)")
    print("[swift][tid:\(_tid())] Execute task with priority: \(_Concurrency.TaskPriority.Worker)")
    precondition(_Concurrency.TaskPriority.Worker.rawValue == 60, "WAS: \(_Concurrency.TaskPriority.Worker.rawValue) wanted: 60")
    await Task(priority: .Worker) {
        print("[swift][tid:\(_tid())] Task executed, with priority: \(Task.currentPriority)")
        precondition(Task.currentPriority == .Worker)
        assertOnNet2EventLoop()
    }.value
    assertOnNet2EventLoop()

    print("[swift][tid:\(_tid())] Properly resumed...")
}

func swiftFutureAwait() async {
    assertOnNet2EventLoop()

    let p: PromiseCInt = PromiseCInt()
    var f: FutureCInt = p.__getFutureUnsafe() // FIXME(swift): getFuture: C++ method 'getFuture' that returns unsafe projection of type 'Future' not imported
    // TODO(swift): we perhaps should add a note that __getFutureUnsafe is available?

    print("[swift][\(#fileID):\(#line)](\(#function)) got PromiseCInt") // FIXME(swift/c++): printing the promise crashes!
    precondition(!f.isReady(), "Future should not be ready yet")

    var num = 1111
    print("[swift][\(#fileID):\(#line)](\(#function)) send \(num)") // FIXME: printing the promise crashes!
    p.send(&num) // FIXME: rdar://99583467 ([C++ interop][fdb] Support xvalues, so we can use Future.send(U&& value))
    print("[swift][\(#fileID):\(#line)](\(#function)) without wait, f.get(): \(f.__getUnsafe().pointee)")

    print("[swift][\(#fileID):\(#line)](\(#function)) wait...")
    let value: CInt? = try? await f.waitValue
    assertOnNet2EventLoop() // hopped back to the right executor, yay
    precondition(f.isReady(), "Future should be ready by now")

    print("[swift][\(#fileID):\(#line)](\(#function)) await value = \(value ?? -1)")
    precondition((value ?? -1) == num, "Value obtained from await did not match \(num), was: \(String(describing: value))!")

    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2 --------------------")
    let p2 = PromiseCInt()
    var f2 = p2.__getFutureUnsafe() // FIXME: Make these not unsafe...
    let num2 = 2222
    Task { [num2] in
        assertOnNet2EventLoop()

        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: send \(num2)")
        var workaroundVar = num2 // FIXME workaround since we need inout xvalue for the C++ send()
        p2.send(&workaroundVar)
    }
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2: waiting...")
    let got2: CInt? = try? await f2.waitValue
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) future 2, got: \(String(describing: got2))")
    precondition(got2! == num2, "Value obtained from send after await did not match \(num2), was: \(String(describing: got2))!")

    // assert that we hopped back and are again on the Net2 event loop thread:
    assertOnNet2EventLoop()
}

actor Greeter {
    let phrase: String
    init(phrase: String) {
        self.phrase = phrase
=======
distributed actor EchoServer {
    init(actorSystem: FlowActorSystem) {
        actorSystem.makeWellKnown(WLTOKEN_ECHO_SERVER, TaskPriority::DefaultEndpoint)
>>>>>>> Stashed changes
    }

    distributed func echo(req: EchoRequest) -> String {
        return req.message
    }

    distributed func reverse(req: ReverseRequest) -> String {
        return req.reverse
    }

    distributed func stream(req: StreamRequest) -> TODO {
        // TODO: not designed yet, but we can support back-pressure aware streams like that
    }
}
<<<<<<< Updated upstream

func actorTest() async {
    let ga = Greeter(phrase: "Hello,")

    assertOnNet2EventLoop()
    let greeting = await ga.greet(name: "Caplin")
    print("[swift][\(#fileID):\(#line)](\(#function)) Greeting: \(greeting)")
    assertOnNet2EventLoop()
}

func flowActorTest() async {
    do {
        var f = Flow.flowSimpleIncrement() // FIXME: we can't call .waitValue inline, since it is mutating
        let returned: CInt = try await f.waitValue
        precondition(returned == CInt(42))
    } catch {
        print("[swift][\(#fileID):\(#line)](\(#function)) error: \(error)")
        fatalError()
    }
}


let task = Task { // task execution will be intercepted
    assertOnNet2EventLoop()

//    print("[swift] await test -------------------------------------------------")
//    print("[swift][tid:\(_tid())] Started a task...")
//    await swiftAsyncFunc()
//    print("[swift] ==== done ---------------------------------------------------")
//
//    print("[swift] futures test -----------------------------------------------")
//    print("[swift] returned from 'await swiftAsyncFunc()'")
//    await swiftFutureAwait()
//    print("[swift] returned from 'await swiftFutureAwait()'")
//    print("[swift] ==== done ---------------------------------------------------")
//
//    print("[swift] actors test -------------------------------------------------")
//    await actorTest()
//    print("[swift] ==== done ---------------------------------------------------")

    print("[swift] swift -> flow async call test -------------------------------")
    await flowActorTest()
    print("[swift] ==== done ---------------------------------------------------")

    exit(0)
}

// ==== ---------------------------------------------------------------------------------------------------------------

print("[swift][tid:\(_tid())] run network @ thread:\(_tid())")
globalNetworkRun()

func test(_ p: ResolutionBalancer) {
}

=======
>>>>>>> Stashed changes
