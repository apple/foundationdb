import Flow
import flow_swift
import FlowFutureSupport

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
}

extension FutureCInt: _FlowFutureOps {
    public typealias _T = CInt

    // FIXME: can't figure out a possible way to implement this using generics, we run into problems with the _T and the concrete template etc...
    public var waitValue: CInt {
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
            var s = FlowCallbackForSwiftContinuationCInt()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<CInt>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }
}

extension FutureVoid: _FlowFutureOps {
    public typealias _T = Void

    // FIXME: can't figure out a possible way to implement this using generics, we run into problems with the _T and the concrete template etc...
    public var waitValue: Void {
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

            var s = FlowCallbackForSwiftContinuationVoid()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<Void>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }
}
