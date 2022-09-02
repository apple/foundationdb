import Flow
//import std

print("[swift] start")

installGlobalSwiftConcurrencyHooks() // hook swift concurrency up to the net runloop

func test() async {
    print("[swift] executing inside \(#function)")
}

let task = Task { // task execution will be intercepted
    print("[swift] Started a task...")
    await test()
    print("[swift] returned from 'await test()'")
    exit(0)
}

// ==== ---------------------------------------------------------------------------------------------------------------

print("[swift] run network")
globalNetworkRun()
