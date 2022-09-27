import Flow
import FDBServer
import FDBClient

///
/// ```
/// #define BUGGIFY_WITH_PROB(x)                                                                                           \
///	(getSBVar(__FILE__, __LINE__, BuggifyType::General) && deterministicRandom()->random01() < (x))
///
/// #define BUGGIFY BUGGIFY_WITH_PROB(P_BUGGIFIED_SECTION_FIRES[int(BuggifyType::General)])
/// ```
var BUGGIFY: Bool {
    false
}

@_expose(Cxx)
public func makeMasterDataActor() -> MasterDataActor {
    fatalError()
}

@_expose(Cxx)
public actor MasterDataActor {
    // We're re-rolling the model type one field at a time...
    let myself: MasterDataShared

    init(data: MasterDataShared) {
        self.myself = data
    }

    /// Reply still done via req.reply
    func getVersion(req: GetCommitVersionRequest) async {
        // NOTE: the `req` is inout since `req.reply.sendNever()` imports as `mutating`
        var req = req

        // TODO: Wrap with a tracing span
        // NOTE: [] is not really usable, it wants the argument be inout:
        // /root/src/foundationdb/fdbserver/masterserver.swift:31:89: error: passing value of type 'UID' to an inout parameter requires explicit '&'
        //        let proxyItr: CommitProxyVersionReplies? = myself.lastCommitProxyVersionReplies[requestingProxy]
        //                                                                                        ^
        //                                                                                        &
        // but we cannot really do this, since:
        //
        // /root/src/foundationdb/fdbserver/masterserver.swift:50:90: error: cannot pass an inout argument to a subscript; use 'withUnsafeMutablePointer' to explicitly convert argument to a pointer
        //         let proxyItr: CommitProxyVersionReplies? = myself.lastCommitProxyVersionReplies[&requestingProxy]

        // we also can't use find:
        //
        // let proxyItr: CommitProxyVersionReplies? = myself.lastCommitProxyVersionReplies.find(req.requestingProxy)
        // because:
        //
        // /root/src/foundationdb/fdbserver/masterserver.swift:51:90: error: cannot convert value of type 'Any' to specified type 'CommitProxyVersionReplies?'
        //         let proxyItr: CommitProxyVersionReplies? = myself.lastCommitProxyVersionReplies.find(req.requestingProxy)
        //                                                    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^~~~~~~~~~~~~~~~~~~~~~~~~
        //                                                                                                                   as! CommitProxyVersionReplies

        // we can use first{} though it'll be less efficient:
        // FIXME: still does not work:
        // /root/src/foundationdb/fdbserver/masterserver.swift:59:89: error: value of type 'MAP_UInt64_GetCommitVersionReply' (aka 'std.__CxxTemplateInstSt3mapIm21GetCommitVersionReplySt4lessImESaISt4pairIKmS0_EEE') has no member 'first'
        //        let proxyItr: CommitProxyVersionReplies? = myself.lastCommitProxyVersionReplies.first {
        //            $0 == req.requestingProxy
        //        }
        let requestingProxyUID: UID = req.requestingProxy

        // can't use the unsafe find either:
        // let proxyItr = myself.lastCommitProxyVersionReplies.__findUnsafe(requestingProxyUID)
        //
        // /root/src/foundationdb/fdbserver/masterserver.swift:65:74: error: cannot convert value of type 'UID' to expected argument type 'std.__CxxTemplateInstSt3mapIm21GetCommitVersionReplySt4lessImESaISt4pairIKmS0_EEE.key_type' (aka 'UInt')
        //        let proxyItr = myself.lastCommitProxyVersionReplies.__findUnsafe(requestingProxyUID)
        //                                                                         ^
        // why would it want an UInt rather than the UID?

//        let proxyItr: CommitProxyVersionReplies? = myself.lastCommitProxyVersionReplies
//                .first(where: { $0.first == requestingProxyUID })

        let proxyItr = myself.lastCommitProxyVersionReplies.__findUnsafe(requestingProxyUID)


        // NOTE: += operator is not imported, so we added an increment()
        // FIXME(swift): rdar://100012917 ([c++ interop] += operator does not seem to work/be imported)
        myself.getCommitVersionRequests = myself.getCommitVersionRequests.successor() // this is ++

        guard let lastVersionReplies: CommitProxyVersionReplies = proxyItr else {
            // Request from invalid proxy (e.g. from duplicate recruitment request)
            req.reply.sendNever()
            return
        }

        // CODE_PROBE(lastVersionReplies.latestRequestNum.get() < req.requestNum - 1, "Commit version request queued up")

        // wait(lastVersionReplies.latestRequestNum.whenAtLeast(req.requestNum - 1))
        // BEFORE:
        // FIXME: await lastVersionReplies.latestRequestNum.whenAtLeast(limit: VersionMetricHandle.ValueType(req.requestNum - UInt64(1)))

        let itr = lastVersionReplies.replies.first { $0.first == UInt(req.requestNum) }
        if let itr {
            // NOTE: CODE_PROBE is macro, won't be imported
            // CODE_PROBE(true, "Duplicate request for sequence")
            var lastVersionNum = itr.second
            req.reply.send(&lastVersionNum) // TODO(swift): we should not require those to be inout
            return
//        } else if (req.requestNum <= lastVersionReplies.latestRequestNum.get()) {
//            // NOTE: CODE_PROBE is macro, won't be imported
//            // /root/src/foundationdb/flow/include/flow/CodeProbe.h:291:9: note: macro 'CODE_PROBE' not imported: function like macros not supported
//            //#define CODE_PROBE(condition, comment, ...)                                                                            \
//            //        ^
//            // CODE_PROBE(true,
//            //         "Old request for previously acknowledged sequence - may be impossible with current FlowTransport")
//            assert(req.requestNum <
//                    proxyItr.latestRequestNum.get()) // The latest request can never be acknowledged
//            req.reply.sendNever()
//            return
        } else {
            var rep = GetCommitVersionReply()

            if (myself.version == invalidVersion) {
                myself.lastVersionTime = now()
                myself.version = myself.recoveryTransactionVersion
                rep.prevVersion = myself.lastEpochEnd

            } else {
            }
            //                var t1 = now()
            //                if BUGGIFY {
            //                    t1 = self.lastVersionTime;
            //                }
            //
            //                let toAdd: Version =
            //                        Version(
            //                                Double.maximum(
            //                                        1,
            //                                        Double.minimum(
            //                                                Double(SERVER_KNOBS.MAX_READ_TRANSACTION_LIFE_VERSIONS),
            //                                                Double(SERVER_KNOBS.VERSIONS_PER_SECOND) * (t1 - self.lastVersionTime)
            //                                        )
            //                                ))
            //
            //                rep.prevVersion = self.version
            //                if let referenceVersion = self.referenceVersion {
            //                    self.version = figureVersion(
            //                            self.version,
            //                            g_network.timer(),
            //                            referenceVersion,
            //                            toAdd,
            //                            SERVER_KNOBS.MAX_VERSION_RATE_MODIFIER,
            //                            SERVER_KNOBS.MAX_VERSION_RATE_OFFSET);
            //                    assert(self.version > rep.prevVersion)
            //                } else {
            //                    self.version = self.version + toAdd
            //                }
            //
            //                CODE_PROBE(self.version - rep.prevVersion == 1, "Minimum possible version gap");
            //
            //                let maxVersionGap: Bool = self.version - rep.prevVersion == Version(SERVER_KNOBS.MAX_READ_TRANSACTION_LIFE_VERSIONS)
            //                CODE_PROBE(maxVersionGap, "Maximum possible version gap")
            //                self.lastVersionTime = t1
            //
            //                self.resolutionBalancer.setChangesInReply(req.requestingProxy, rep)
            //            }
            //
            //            rep.version = self.version;
            //            rep.requestNum = req.requestNum;
            //
            //            // TODO:
            //            // lastVersionReplies.replies.erase(
            //            //        lastVersionReplies.replies.begin(),
            //            //        lastVersionReplies.replies.upper_bound(req.mostRecentProcessedRequestNum))
            //            lastVersionReplies.replies[req.requestNum] = rep
            //            assert(rep.prevVersion >= 0)
            //
            //            req.reply.send(rep)
            //
            //            assert(lastVersionReplies.latestRequestNum.get().value == req.requestNum - 1)
            //            lastVersionReplies.latestRequestNum.set(.init(req.requestNum))
        }
    }
}

// FIXME: remove in favor of MasterDataActor_getVersion.
 @_expose(Cxx)
 public func MasterDataActor_getVersion_workaround(
     reqPtr: OpaquePointer,
     result opaqueResultPromisePtr: OpaquePointer) {
     let actor = makeMasterDataActor()

     MasterDataActor_getVersion(
             myself: actor,
             req: UnsafePointer<GetCommitVersionRequest>(reqPtr).pointee,
             result: opaqueResultPromisePtr
     )
 }

@_expose(Cxx)
public func MasterDataActor_getVersion(
        myself target: MasterDataActor,
        req: GetCommitVersionRequest,
        result opaqueResultPromisePtr: OpaquePointer) {
    print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")

    let promisePtr = UnsafeMutablePointer<PromiseVoid>(opaqueResultPromisePtr)
    let promise = promisePtr.pointee

    Task {
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")
        await target.getVersion(req: req)
        var result = Flow.Void()
        promise.send(&result)
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Done calling getVersion impl!")
    }
}
