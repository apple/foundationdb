import Flow

// Generic interface for the Flow.Optional type.
public protocol FlowOptionalProtocol {
    associatedtype Wrapped

    func present() -> Bool
    // FIXME: Avoid using __getUnsafe.
    func __getUnsafe() -> UnsafePointer<Wrapped>
}

extension Swift.Optional {
    /// Construct a Swift.Optional from a FlowOptional.
    @inline(__always)
    public init<OptT: FlowOptionalProtocol>(cxxOptional value: OptT) where OptT.Wrapped == Wrapped {
        guard value.present() else {
            self = nil
            return
        }
        self = value.__getUnsafe().pointee
    }
}
