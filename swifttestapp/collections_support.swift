import Flow
import FDBServer
import Cxx

// ==== ---------------------------------------------------------------------------------------------------------------
// MARK: std::Map

public func ==(lhs: MAP_UInt64_GetCommitVersionReply.const_iterator,
               rhs: MAP_UInt64_GetCommitVersionReply.const_iterator) -> Bool {
    true
}

extension MAP_UInt64_GetCommitVersionReply.const_iterator: UnsafeCxxInputIterator {
}

extension MAP_UInt64_GetCommitVersionReply: CxxSequence {
}
