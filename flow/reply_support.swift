import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

public protocol _FlowReplyPromiseOps {
    /// Element type of the future
    associatedtype _Reply

    /// Complete this `ReplyPromise` with the result of this function
    @_unsafeInheritExecutor
    func with(_ makeReply: () async -> _Reply) async

    /// ReplyPromise API
    func send(_ value: inout _Reply)
}

extension _FlowReplyPromiseOps {
    func with(_ makeReply: () async -> _Reply) async {
        var rep = await makeReply()
        self.send(&rep)
    }
}

// TODO(swift): would love to extension ReplyPromise, but we can't until templates have better support.
