import Flow
import FDBServer
import FDBClient

@_expose(Cxx)
public func swiftFunctionCalledFromCpp(_ x: CInt) -> CInt {
    return x
}

func testFDBServerImport(_ p: ResolutionBalancer) {
}

public func swiftCallMe() async -> CInt {
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) YES, called async code")
    return CInt(42)
}

@_cdecl("swiftCallMeFuture")
public func swiftCallMeFuture(result opaqueResultPromisePtr: OpaquePointer) {
    let promisePtr = UnsafeMutablePointer<PromiseCInt>(opaqueResultPromisePtr)
    let promise =  promisePtr.pointee

    Task {
        var value = await swiftCallMe()
        promise.send(&value)
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) sleep...!")
        try? await Task.sleep(until: .now.advanced(by: .seconds(1)), clock: .flow)
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) value [\(value)] sent!")
    }
}