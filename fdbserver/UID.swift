import Flow
import FDBServer

extension Flow.UID: Hashable {
    public func hash(into hasher: inout Swift.Hasher) {
        self.first().hash(into: &hasher)
        self.second().hash(into: &hasher)
    }

    public static func ==(lhs: UID, rhs: UID) -> Swift.Bool {
        lhs.first() == rhs.first() && lhs.second() == rhs.second()
    }
}

