import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

/// Wrapper around `Flow.TraceEvent` that allows similar style "fluent api" use as the C++ flow event.
///
/// We need this because the C++ type actually mutates the struct, which in Swift would need to be
/// expressed as mutating functions on a variable, and cannot be done in a "fluent" style.
/// Instead, this wrapper returns a new struct when a detail is added allowing for the same style of use.
public final class STraceEvent {
    private var keepAlive: [std.string] = []

    public var event: Flow.TraceEvent
    public init(_ type: std.string, _ id: Flow.UID) {
        keepAlive.append(type)
        event = .init(type.__c_strUnsafe(), id)
    }

    /// This function allows ignoring the returned value, i.e. emitting just a plain event
    /// without any extra `detail()` attached to it.
    @discardableResult
    public static func make(_ type: std.string, _ id: Flow.UID) -> Self {
        .init(type, id)
    }

    // Due to a limitation of passing a generic `T` to a C++ template, we cannot just use
    // one generic detail<T>() and have to write out the overloads so the type is concrete
    // for the template expansion.

    @discardableResult
    public func detail(_ type: std.string, _ value: OptionalStdString) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }

    @discardableResult
    public func detail(_ type: std.string, _ value: std.string) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }

    @discardableResult
    public func detail(_ type: std.string, _ value: Float) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }

    @discardableResult
    public func detail(_ type: std.string, _ value: Double) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }

    @discardableResult
    public func detail(_ type: std.string, _ value: Int) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }
    @discardableResult
    public func detail(_ type: std.string, _ value: OptionalInt64) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }

    // TODO(swift): we seem to have a problem when mapping Int8 -> char
//        @discardableResult
//        public func detail(_ type: std.string, _ value: Int8) -> Self {
//            // var copy = self
//            /*copy*/event.addDetail(type, value)
//            return self
//        }

    @discardableResult
    public func detail(_ type: std.string, _ value: UInt32) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }
    @discardableResult
    public func detail(_ type: std.string, _ value: Int32) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }
    @discardableResult
    public func detail(_ type: std.string, _ value: Int64) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }
    @discardableResult
    public func detail(_ type: std.string, _ value: UInt64) -> Self {
        /*copy*/keepAlive.append(type)
        /*copy*/event.addDetail(type, value)
        return self
    }
}