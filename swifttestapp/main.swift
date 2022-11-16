import Flow
import FDBServer

print("[swift] start")

newNet2ThenInstallSwiftConcurrencyHooks() // hook swift concurrency up to the net runloop

// capture the main thread ID
let mainTID = _tid()

func swiftFutureAwait() async {
}

actor Greeter {
    let phrase: String
    init(phrase: String) {
        self.phrase = phrase
    }

    func greet(name: String) -> String {
        assertOnNet2EventLoop()
        return "\(phrase) \(name)!"
    }
}

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
