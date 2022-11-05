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
}

@_expose(Cxx)
public struct ExposeVoidConf<T> {
    let x: CInt
}

@_expose(Cxx)
public func _exposeVoidValueTypeConformanceToCpp(_ val: ExposeVoidConf<Void>)  {
}

@_expose(Cxx)
public struct ExposedCheckedContinuation<T> {
    typealias CC = CheckedContinuation<T, Swift.Error>
    var cc: CC?

    public init() {}

    init(_ cc: CC) {
        self.cc = cc
    }

    public mutating func set(_ other: ExposedCheckedContinuation<T>) {
        // precondition: other continuation must be set.
        assert(other.cc != nil)
        cc = other.cc
    }

    public func resume(returning value: T) {
        // precondition: continuation must be set.
        assert(cc != nil)
        cc!.resume(returning: value)
    }

    public func resumeThrowing(_ value: Flow.Error) {
        // precondition: continuation must be set.
        assert(cc != nil)
        // TODO: map errors.
        cc!.resume(throwing: GeneralFlowError())
    }
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
            var s = SwiftContinuationCallbackStructCInt()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = ExposedCheckedContinuation<CInt>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        setContinutation(ptr, UnsafeRawPointer(ccPtr), self)
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

            var s = SwiftContinuationCallbackStructVoid()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = ExposedCheckedContinuation<Void>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        setContinutation(ptr, UnsafeRawPointer(ccPtr), self)
                    }
                }
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
