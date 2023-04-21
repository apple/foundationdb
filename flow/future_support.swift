import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

public protocol FlowCallbackForSwiftContinuationT {
    associatedtype AssociatedFuture: FlowFutureOps
    typealias Element = AssociatedFuture.Element

    init()

    mutating func set(_ continuationPointer: UnsafeRawPointer,
                      _ future: AssociatedFuture,
                      _ thisPointer: UnsafeRawPointer)
}

public protocol FlowFutureOps {
    /// Element type of the future
    associatedtype Element
    associatedtype FlowCallbackForSwiftContinuation: FlowCallbackForSwiftContinuationT

    func isReady() -> Bool
    func isError() -> Bool
    func canGet() -> Bool

    func __getUnsafe() -> UnsafePointer<Element>
}

extension FlowFutureOps where Self == FlowCallbackForSwiftContinuation.AssociatedFuture {

    /// Swift async method to make a Flow future awaitable.
    public func value() async throws -> Element {
        return try await self.waitValue
    }

    @inlinable
    internal var waitValue: Element {
        get async throws {
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
            var s = FlowCallbackForSwiftContinuation()
            return try await withCheckedThrowingContinuation { cc in
                withUnsafeMutablePointer(to: &s) { ptr in
                    let ecc = FlowCheckedContinuation<Element>(cc)
                    withUnsafePointer(to: ecc) { ccPtr in
                        ptr.pointee.set(UnsafeRawPointer(ccPtr), self, UnsafeRawPointer(ptr))
                    }
                }
            }
        }
    }
}

extension FlowFutureOps where Self == FlowCallbackForSwiftContinuation.AssociatedFuture,
                              Element == Flow.Void {

    /// Swift async method to make a Flow future awaitable.
    /// Specialized for Flow.Void making the returned result (Flow.Void) discardable.
    ///
    /// The reason these are done as value() and not a computed property is to allow discardable value
    /// on Flow.Void returning futures.
    @discardableResult
    public func value() async throws -> Element {
        return try await self.waitValue
    }
}
