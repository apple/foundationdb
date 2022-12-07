import Flow

// ==== Future: Conformances ------------------------------------------------------------------------------------------

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: CInt

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowCallbackForSwiftContinuationCInt: FlowCallbackForSwiftContinuationT {
    public typealias AssociatedFuture = FutureCInt
}
extension FutureCInt: _FlowFutureOps {
    public typealias Element = CInt
    public typealias FlowCallbackForSwiftContinuation = FlowCallbackForSwiftContinuationCInt
}

// ==== ----------------------------------------------------------------------------------------------------------------
// MARK: Void

// This is a C++ type that we add the conformance to; so no easier way around it currently
extension FlowCallbackForSwiftContinuationVoid: FlowCallbackForSwiftContinuationT {
    public typealias AssociatedFuture = FutureVoid
}
extension FutureVoid: _FlowFutureOps {
    public typealias Element = Void
    public typealias FlowCallbackForSwiftContinuation = FlowCallbackForSwiftContinuationVoid
}

// ==== ---------------------------------------------------------------------------------- ==== //
// ==== Add further conformances here that should be shared for all Flow importing modules ==== //
// ==== ---------------------------------------------------------------------------------- ==== //
