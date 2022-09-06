import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

//extension FlowPromiseInt: _FlowFutureOps {
extension FlowFutureInt: _FlowFutureOps {
    typealias _T = CInt

    typealias CCBox = Box<CheckedContinuation<_T, Swift.Error>>
    typealias CC = SwiftContinuationCallbackInt
    typealias CB = Flow.SwiftContinuationCallbackInt

    var waitValue: _T {
        get async throws {
            if self.isReady() {
                print("[swift][\(#fileID):\(#line)](\(#function)) future was ready, return immediately.")
                return get().pointee
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
