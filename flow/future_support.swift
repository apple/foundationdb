import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

public protocol _FlowFutureOps {
    /// Element type of the future
    associatedtype _T

    var waitValue: _T { mutating get async throws }
}

extension FutureCInt: _FlowFutureOps {
    public typealias _T = CInt

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
