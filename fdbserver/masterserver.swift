import Flow
import flow_swift
import FDBServer
import FDBClient
import fdbclient_swift

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

func clamp(_ v: Version, lowerBound: Version, upperBound: Version) -> Version {
    return max(min(v, upperBound), lowerBound)
}

func figureVersion(current: Version,
                   now: Double,
                   reference: Version,
                   toAdd: Int64,
                   maxVersionRateModifier: Double,
                   maxVersionRateOffset: Int64) -> Version {
    // Versions should roughly follow wall-clock time, based on the
    // system clock of the current machine and an FDB-specific epoch.
    // Calculate the expected version and determine whether we need to
    // hand out versions faster or slower to stay in sync with the
    // clock.
    let expected = Version(now * Double(getServerKnobs().VERSIONS_PER_SECOND)) - reference

    // Attempt to jump directly to the expected version. But make
    // sure that versions are still being handed out at a rate
    // around VERSIONS_PER_SECOND. This rate is scaled depending on
    // how far off the calculated version is from the expected
    // version.
    let maxOffset = min(Int64(Double(toAdd) * maxVersionRateModifier), maxVersionRateOffset)
    return clamp(expected, lowerBound: current + toAdd - maxOffset,
                           upperBound: current + toAdd + maxOffset)
}


public actor MasterDataActor {
    let myself: MasterData

    init(data: MasterData) {
        self.myself = data
    }

    /// Reply still done via req.reply
    func getVersion(req: GetCommitVersionRequest) async {
        print("[swift][\(#fileID):\(#line)](\(#function))\(Self.self) handle request")
        // NOTE: the `req` is inout since `req.reply.sendNever()` imports as `mutating`
        var req = req

        // TODO: Wrap with a tracing span
        let requestingProxyUID: UID = req.requestingProxy
        myself.getGetCommitVersionRequests() += 1

        // FIXME: workaround for std::map usability, see: rdar://100487652 ([fdp] std::map usability, can't effectively work with map in Swift)
        guard let lastVersionReplies = lookup_Map_UID_CommitProxyVersionReplies(&myself.lastCommitProxyVersionReplies, requestingProxyUID) else {
            // Request from invalid proxy (e.g. from duplicate recruitment request)
            req.reply.sendNever()
            return
        }

        // CODE_PROBE(lastVersionReplies.latestRequestNum.get() < req.requestNum - 1, "Commit version request queued up")
        var latestRequestNum = try! await lastVersionReplies.getLatestRequestNumRef()
                .atLeast(VersionMetricHandle.ValueType(req.requestNum - UInt64(1)))

        // FIXME: workaround for std::map usability, see: rdar://100487652 ([fdp] std::map usability, can't effectively work with map in Swift)
        if lastVersionReplies.replies.count(UInt(req.requestNum)) != 0 {
            // NOTE: CODE_PROBE is macro, won't be imported
            // CODE_PROBE(true, "Duplicate request for sequence")
            let lastVersionNum = lastVersionReplies.replies.__atUnsafe(UInt(req.requestNum))
            req.reply.sendCopy(lastVersionNum.pointee) // TODO(swift): we should not require those to be inout
            return
         } else if (req.requestNum <= lastVersionReplies.getLatestRequestNumRef().get()) {
             // NOTE: CODE_PROBE is macro, won't be imported
             // CODE_PROBE(true, "Old request for previously acknowledged sequence - may be impossible with current FlowTransport");
             assert(req.requestNum < lastVersionReplies.getLatestRequestNumRef().get()) // The latest request can never be acknowledged
             req.reply.sendNever()
             return
        }
        
        var rep = GetCommitVersionReply()

        if (myself.version == invalidVersion) {
            myself.lastVersionTime = now()
            myself.version = myself.recoveryTransactionVersion
            rep.prevVersion = myself.lastEpochEnd
        } else {
            var t1 = now()
            if BUGGIFY {
                t1 = myself.lastVersionTime
            }

            let toAdd = max(Version(1), min(
                          getServerKnobs().MAX_READ_TRANSACTION_LIFE_VERSIONS,
                          Version(Double(getServerKnobs().VERSIONS_PER_SECOND) * (t1 - myself.lastVersionTime))))
            rep.prevVersion = myself.version
            if myself.referenceVersion.present() {
                // FIXME: myself.referenceVersion.get()
                // FIXME: getMutating() ambiguity
                let r = myself.referenceVersion
                // FIXME: Do not use r.__getUnsafe
                myself.version = figureVersion(current: myself.version,
                                               now: SwiftGNetwork.timer(),
                              reference: Version(r.__getUnsafe().pointee),
                              toAdd: toAdd,
                              maxVersionRateModifier: getServerKnobs().MAX_VERSION_RATE_MODIFIER,
                              maxVersionRateOffset: getServerKnobs().MAX_VERSION_RATE_OFFSET)
                assert(myself.version > rep.prevVersion)
            } else {
                myself.version += toAdd
            }

            // NOTE: CODE_PROBE is macro, won't be imported
            // CODE_PROBE(self.version - rep.prevVersion == 1, "Minimum possible version gap");
            let maxVersionGap = myself.version - rep.prevVersion == getServerKnobs().MAX_READ_TRANSACTION_LIFE_VERSIONS
            // CODE_PROBE(maxVersionGap, "Maximum possible version gap");

            myself.lastVersionTime = t1
            myself.getResolutionBalancer().setChangesInReply(req.requestingProxy, &rep)
        }

        rep.version = myself.version
        rep.requestNum = req.requestNum
        print("[swift][\(#fileID):\(#line)](\(#function))\(Self.self) reply with version: \(rep.version)")

        //  FIXME: figure out how to map:
        //            // lastVersionReplies.replies.erase(
        //            //        lastVersionReplies.replies.begin(),
        //            //        lastVersionReplies.replies.upper_bound(req.mostRecentProcessedRequestNum))
        eraseReplies(&lastVersionReplies.replies, req.mostRecentProcessedRequestNum)
        lastVersionReplies.replies[UInt(req.requestNum)] = rep
        assert(rep.prevVersion >= 0)

        req.reply.send(&rep)

        assert(lastVersionReplies.getLatestRequestNumRef().get() == req.requestNum - 1)
        // FIXME: link issue (rdar://101092732).
        // lastVersionReplies.getLatestRequestNumRef().set(Int(req.requestNum))
        swift_workaround_setLatestRequestNumber(lastVersionReplies.getLatestRequestNumRef(),
                                                Version(req.requestNum))
    }
}

/// Bridge type that wraps the target actor.
@_expose(Cxx, "MasterDataActor")
public struct MasterDataActorCxx {
    let myself: MasterDataActor

    /// Mirror actor initializer, and initialize `myself`.
    public init(data: MasterData) {
        myself = MasterDataActor(data: data)
    }

    /// Promise type must match result type of the target function.
    /// If missing, please declare new `using PromiseXXX = Promise<XXX>;` in `swift_<MODULE>_future_support.h` files.
    public func getVersion(req: GetCommitVersionRequest, result promise: PromiseVoid) {
        print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")
        // FIXME: remove after https://github.com/apple/swift/issues/61627 makes MasterData refcounted FRT.
        swift_workaround_retainMasterData(myself.myself)
        Task {
            print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl in task!")
            await myself.getVersion(req: req)
            var result = Flow.Void()
            promise.send(&result)
            // FIXME: remove after https://github.com/apple/swift/issues/61627 makes MasterData refcounted FRT.
            swift_workaround_releaseMasterData(myself.myself)
            print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Done calling getVersion impl!")
        }
    }
}
