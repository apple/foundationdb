import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

// NOTE: can't implement waitValue only in the protocol because we end up with:
// /root/src/foundationdb/flow/future_support.swift:12:10: note: protocol requires function '__getUnsafe()' with type '() -> UnsafePointer<__CxxTemplateInst6FutureIiE._T>' (aka '() -> UnsafePointer<Void>'); do you want to add a stub?
//     func __getUnsafe() -> UnsafePointer<_T>
//          ^
// /root/src/foundationdb/flow/future_support.swift:20:1: error: type '__CxxTemplateInst6FutureI4VoidE' does not conform to protocol '_FlowFutureOps'
// extension FutureVoid: _FlowFutureOps {
// ^
// Flow.__CxxTemplateInst6FutureI4VoidE:8:17: note: candidate has non-matching type '() -> UnsafePointer<Void>'
//     public func __getUnsafe() -> UnsafePointer<Void>
//                ^
// since we have to express the __getUnsafe() as a protocol requirement...

public protocol _FlowFutureOps {
    /// Element type of the future
    associatedtype _T
    /// Swift continuation wrapper type to use for the async/await bridging
    associatedtype CB
    typealias CCBox = _Box<CheckedContinuation<_T, Swift.Error>>
}

extension FutureCInt: _FlowFutureOps {
    public typealias _T = CInt
    public typealias CB = SwiftContinuationCallbackCInt

    // FIXME: can't figure out a possible way to implement this using generics, we run into problems with the _T and the concrete template etc...
    public var waitValue: _T {
        mutating get async throws {
            guard !self.isReady() else {
                // FIXME(flow): handle isError and cancellation
                if self.isError() {
                    // let error = self.__getErrorUnsafe() // TODO: rethrow the error?
                    throw GeneralFlowError()
                } else {
                    // print("[swift][\(#fileID):\(#line)](\(#function)) future was ready, return immediately.")
                    // FIXME(swift): we'd need that technically to be:
                    //               return get().pointee
                    precondition(self.canGet())
                    return self.__getUnsafe().pointee
                }
            }

            return try await withCheckedThrowingContinuation { cc in
                let ccBox = CCBox(cc)
                let rawCCBox = UnsafeMutableRawPointer(Unmanaged.passRetained(ccBox).toOpaque())

                 let cb = CB.make(
                    rawCCBox,
                    /*returning:*/ { (_cc: UnsafeMutableRawPointer, value: _T) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc).takeRetainedValue().value
                        cc.resume(returning: value)
                    },
                    /*throwing:*/ { (_cc: UnsafeMutableRawPointer, error: Flow.Error) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc).takeRetainedValue().value
                        cc.resume(throwing: GeneralFlowError()) // TODO: map errors
                    }
                )
                cb.addCallbackAndClearTo(self)
            }
        }
    }
}

extension FutureVoid: _FlowFutureOps {
    public typealias _T = Void
    public typealias CB = SwiftContinuationCallbackVoid

    // FIXME: can't figure out a possible way to implement this using generics, we run into problems with the _T and the concrete template etc...
    public var waitValue: _T {
        mutating get async throws {
            guard !self.isReady() else {
                // FIXME(flow): handle isError and cancellation
                if self.isError() {
                    // let error = self.__getErrorUnsafe() // TODO: rethrow the error?
                    throw GeneralFlowError()
                } else {
                    precondition(self.canGet())
                    return self.__getUnsafe().pointee
                }
            }

            return try await withCheckedThrowingContinuation { cc in
                let ccBox = CCBox(cc)
                let rawCCBox = UnsafeMutableRawPointer(Unmanaged.passRetained(ccBox).toOpaque())

                 let cb = CB.make(
                    rawCCBox,
                    /*returning:*/ { (_cc: UnsafeMutableRawPointer, value: _T) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc).takeRetainedValue().value
                        cc.resume(returning: value)
                    },
                    /*throwing:*/ { (_cc: UnsafeMutableRawPointer, error: Flow.Error) in
                        let cc = Unmanaged<CCBox>.fromOpaque(_cc).takeRetainedValue().value
                        cc.resume(throwing: GeneralFlowError()) // TODO: map errors
                    }
                )
                cb.addCallbackAndClearTo(self)
            }
        }
    }
}

// ==== ---------------------------------------------------------------------------------------------------------------

public struct GeneralFlowError: Swift.Error {
    let underlying: Flow.Error?

    public init() {
        self.underlying = nil
    }

    public init(_ underlying: Flow.Error) {
        self.underlying = underlying
    }
}

public final class _Box<Value> {
    let value: Value
    init(_ value: Value) {
        self.value = value
    }
}
