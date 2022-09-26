import Flow
import FDBServer
import FDBClient

#if ENABLE_REVERSE_INTEROP

actor MasterDataActor {

    /// Reply still done via req.reply
    func getVersion(req: GetCommitVersionRequest) async {
        // NOTE: the `req` is inout since `req.reply.sendNever()` imports as `mutating`
        var req = req

    }
}

@_expose(Cxx)
public func MasterDataActor_getVersion(
//        myself: ReferenceMasterDataShared,
        req: GetCommitVersionRequest,
        result opaqueResultPromisePtr: OpaquePointer) {
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")

    let promisePtr = UnsafeMutablePointer<PromiseVoid>(opaqueResultPromisePtr)
    let promise = promisePtr.pointee

    let target = MasterDataActor()

    Task {
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")
        await target.getVersion(req: req)
        var result = Flow.Void()
        promise.send(&result)
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Done calling getVersion impl!")
    }
}

#else
#error("Swift: C++ 'reverse' interop is required to build. Please update your Swift toolchain to a latest nightly build.")
#endif
