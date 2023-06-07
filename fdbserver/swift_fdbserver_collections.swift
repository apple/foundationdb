import Flow
import FDBServer
import Cxx

#if NOTNEEDED
// FIXME(swift): remove this as the fixes for Map come in the new swift toolchain
extension Map_UID_CommitProxyVersionReplies.const_iterator: UnsafeCxxInputIterator {
    public typealias Pointee = MAP_UInt64_GetCommitVersionReply_Iterator_Pointee
}

extension Map_UID_CommitProxyVersionReplies: CxxSequence {
    public typealias RawIterator = Map_UID_CommitProxyVersionReplies.const_iterator
}

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
#endif
