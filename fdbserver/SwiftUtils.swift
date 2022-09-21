import Flow
import FDBServer
import FDBClient

@_cdecl("testSwiftInFDB")
public func testSwiftInFDB() {
    print("[swift] fdbserver")
    print("a")
    installGlobalSwiftConcurrencyHooks()

    // capture the main thread ID
    let mainTID = _tid()
    func assertOnNet2EventLoop() {
        precondition(_tid() == mainTID) // we're on the main thread, which the Net2 runloop runs on
    }

    print("[swift][tid:\(_tid())] run network @ thread:\(_tid())")
    globalNetworkRun()
}

func testFDBServerImport(_ p: ResolutionBalancer) {
}
