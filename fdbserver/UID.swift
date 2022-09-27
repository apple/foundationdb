import Flow
import FDBServer

#if ENABLE_REVERSE_INTEROP

extension Flow.UID: Hashable {
    public func hash(into hasher: inout Swift.Hasher) {
        self.first().hash(into: &hasher)
        self.second().hash(into: &hasher)
    }

    public static func ==(lhs: UID, rhs: UID) -> Swift.Bool {
        lhs.first() == rhs.first() && lhs.second() == rhs.second()
    }
}

#else
#error("Swift: C++ 'reverse' interop is required to build. Please update your Swift toolchain to a latest nightly build.")
#endif
