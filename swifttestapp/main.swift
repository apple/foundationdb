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
}

// ==== ---------------------------------------------------------------------------------------------------------------

print("[swift] run network")
globalNetworkRun()

// FIXME: await task.value awaiting on top level causes:
// swift-frontend: /home/build-user/swift/lib/SILGen/SILGen.cpp:2068: (anonymous namespace)::SourceFileScope::~SourceFileScope(): Assertion `exitFuncDecl && "Failed to find exit function declaration"' failed.
while true {}
