import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

/// Wrapper around `Flow.TraceEvent` that allows similar style "fluent api" use as the C++ flow event.
///
/// We need this because the C++ type actually mutates the struct, which in Swift would need to be
/// expressed as mutating functions on a variable, and cannot be done in a "fluent" style.
/// Instead, this wrapper returns a new struct when a detail is added allowing for the same style of use.
public struct STraceEvent {
    public var event: Flow.TraceEvent
    public init(_ type: String, _ id: Flow.UID) {
        event = .init(type, id)
    }

    // Due to a limitation of passing a generic `T` to a C++ template, we cannot just use
    // one generic detail<T>() and have to write out the overloads so the type is concrete
    // for the template expansion.

    @discardableResult
    public func detail(_ type: std.string, _ value: OptionalStdString) -> Self {
        var copy = self
        copy.event.addDetail(type, value)
        return copy
    }

    @discardableResult
    public func detail(_ type: std.string, _ value: std.string) -> Self {
        var copy = self
        copy.event.addDetail(type, value)
        return copy
    }

    @discardableResult
    public func detail(_ type: std.string, _ value: Float) -> Self {
        var copy = self
        copy.event.addDetail(type, value)
        return copy
    }

    @discardableResult
    public func detail(_ type: String, _ value: Double) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }

    @discardableResult
    public func detail(_ type: String, _ value: Int) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }
    @discardableResult
    public func detail(_ type: String, _ value: OptionalInt64) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }

    // TODO(swift): we seem to have a problem when mapping Int8 -> char
//        @discardableResult
//        public func detail(_ type: String, _ value: Int8) -> Self {
//            var copy = self
//            copy.event.addDetail(std.string(type), value)
//            return copy
//        }

    @discardableResult
    public func detail(_ type: String, _ value: UInt32) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }
    @discardableResult
    public func detail(_ type: String, _ value: Int32) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }
    @discardableResult
    public func detail(_ type: String, _ value: Int64) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }
    @discardableResult
    public func detail(_ type: String, _ value: UInt64) -> Self {
        var copy = self
        copy.event.addDetail(std.string(type), value)
        return copy
    }
}