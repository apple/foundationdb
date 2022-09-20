import Flow
import FDBServer
import Cxx

// ==== ---------------------------------------------------------------------------------------------------------------
// MARK: std::Map

// FIXME(swift): remove this as the fixes for Map come in the new swift toolchain
public func ==(lhs: MAP_UInt64_GetCommitVersionReply.const_iterator,
               rhs: MAP_UInt64_GetCommitVersionReply.const_iterator) -> Bool {
    true
}

extension MAP_UInt64_GetCommitVersionReply.const_iterator: UnsafeCxxInputIterator {
}

extension MAP_UInt64_GetCommitVersionReply: CxxSequence {
}
