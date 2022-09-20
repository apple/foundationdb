import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

extension FutureCInt: _FlowFutureOps {
    typealias _T = CInt

    typealias CCBox = Box<CheckedContinuation<_T, Swift.Error>>
    typealias CC = SwiftContinuationCallbackInt
    typealias CB = Flow.SwiftContinuationCallbackInt

    // NOTE: cannot be a `var` because must be mutating because `self.addCallbackAndClear` is mutating (from C++)
    var waitValue: _T {
        mutating get async throws {
            if self.isReady() {
                print("[swift][\(#fileID):\(#line)](\(#function)) future was ready, return immediately.")
                // FIXME(swift): we'd need that technically to be:
                //               return get().pointee
                return __getUnsafe().pointee
            }

            return try await withCheckedThrowingContinuation { cc in
                let ccBox = CCBox(cc)
                let rawCCBox = UnsafeMutableRawPointer(Unmanaged.passRetained(ccBox).toOpaque())

                let cb = CC.make(
                    rawCCBox,
                    /*returning:*/ { (_cc: UnsafeMutableRawPointer?, value: _T) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc!).takeRetainedValue().value
                        cc.resume(returning: value)
                    },
                    /*throwing:*/ { (_cc: UnsafeMutableRawPointer?, value: Flow.Error) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc!).takeRetainedValue().value
                        cc.resume(throwing: GeneralFlowError()) // TODO: map errors
                    }
                )
                // self.addCallbackAndClear(&cb)
                cb.addCallbackAndClearTo(self)
            }
        }
    }
}

// ==== ---------------------------------------------------------------------------------------------------------------

protocol _FlowFutureOps {
    associatedtype _T
}

struct GeneralFlowError: Swift.Error {}

final class Box<Value> {
    let value: Value
    init(_ value: Value) {
        self.value = value
    }
}
