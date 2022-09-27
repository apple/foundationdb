import Flow
import FDBServer
import Cxx

#if ENABLE_REVERSE_INTEROP

// FIXME(swift): remove this as the fixes for Map come in the new swift toolchain
extension Map_UID_CommitProxyVersionReplies: CxxSequence {}
extension Map_UID_CommitProxyVersionReplies.const_iterator: UnsafeCxxInputIterator {}
public func ==(lhs: Map_UID_CommitProxyVersionReplies.const_iterator,
               rhs: Map_UID_CommitProxyVersionReplies.const_iterator) -> Bool {
    true
}

extension MAP_UInt64_GetCommitVersionReply.const_iterator: UnsafeCxxInputIterator {}
extension MAP_UInt64_GetCommitVersionReply: CxxSequence {}
public func ==(lhs: MAP_UInt64_GetCommitVersionReply.const_iterator,
               rhs: MAP_UInt64_GetCommitVersionReply.const_iterator) -> Bool {
    true
}

#else
#error("Swift: C++ 'reverse' interop is required to build. Please update your Swift toolchain to a latest nightly build.")
#endif
