import Flow
import FDBServer
import FDBClient

#if ENABLE_REVERSE_INTEROP

@_expose(Cxx)
public func swiftFunctionCalledFromCpp(_ x: CInt) -> CInt {
    return x
}

#endif

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
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) value [\(value)] sent!")
    }
}
