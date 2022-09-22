import Flow
import FDBServer
import FDBClient

#if ENABLE_REVERSE_INTEROP

@_expose(Cxx)
public func testSwiftFDBServerMain() {
    print("[swift] fdbserver")
    installGlobalSwiftConcurrencyHooks()

    // capture the main thread ID
    let mainTID = _tid()
    func assertOnNet2EventLoop() {
        precondition(_tid() == mainTID) // we're on the main thread, which the Net2 runloop runs on
    }

    print("[swift][tid:\(_tid())] run network @ thread:\(_tid())")
    globalNetworkRun()
}

@_expose(Cxx)
public func swiftFunctionCalledFromCpp(_ x: CInt) -> CInt {
    return x
}

#endif

func testFDBServerImport(_ p: ResolutionBalancer) {
}
