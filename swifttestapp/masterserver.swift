import Flow
import FDBServer
import Cxx

actor MasterDataActor {

    // NOTE: Rather than passing around the MasterData struct to all Flow ACTOR funcs,
    // we have methods on the MasterDataActor.
    // i.e. we don't do this:
    // var myself: MasterData // struct
    let dbgid: UID

    let lastEpochEnd: Version  // The last version in the old epoch not (to be) rolled back in this recovery
    let recoveryTransactionVersion: Version // The first version in this epoch
//    let prevTLogVersion: NotifiedVersion // Order of transactions to tlogs
//    let liveCommittedVersion: NotifiedVersion // The largest live committed version reported by commit proxies.
    let databaseLocked: Bool
    let proxyMetadataVersion: Value?
    let minKnownCommittedVersion: Version

    let coordinators: ServerCoordinators

    var version: Version // The last version assigned to a proxy by getVersion()
    var lastVersionTime: Double
    let referenceVersion: Version?

    let lastCommitProxyVersionReplies: [UID: CommitProxyVersionReplies]

    // FIXME: this is more like the protocol of the actor, right?
    let myInterface: MasterInterface

    let resolutionBalancer: ResolutionBalancer

    let forceRecovery: Bool

    // Captures the latest commit version targeted for each storage server in the cluster.
    // @todo We need to ensure that the latest commit versions of storage servers stay
    // up-to-date in the presence of key range splits/merges.
    let ssVersionVector: VersionVector

    let locality: Int8 // sequencer locality
    let cc: CounterCollection
    var getCommitVersionRequests: Counter
    let getLiveCommittedVersionRequests: Counter
    let reportLiveCommittedVersionRequests: Counter
    // This counter gives an estimate of the number of non-empty peeks that storage servers
    // should do from tlogs (in the worst case, ignoring blocking peek timeouts).
    let versionVectorTagUpdates: LatencySample
    let waitForPrevCommitRequests: Counter
    let nonWaitForPrevCommitRequests: Counter
    let versionVectorSizeOnCVReply: LatencySample
    let waitForPrevLatencies: LatencySample

    let addActor: PromiseStreamFutureVoid

    let logger: FutureVoid
    let balancer: FutureVoid

    init(dbgid: UID,
         lastEpochEnd: Version,
         recoveryTransactionVersion: Version,
//         prevTLogVersion: NotifiedVersion,
//         liveCommittedVersion: NotifiedVersion,
         databaseLocked: Bool,
         proxyMetadataVersion: Value?,
         minKnownCommittedVersion: Version,
         coordinators: ServerCoordinators,
         version: Version,
         lastVersionTime: Double,
         referenceVersion: Version?,
         lastCommitProxyVersionReplies: [UID: CommitProxyVersionReplies],
         myInterface: MasterInterface,
         resolutionBalancer: ResolutionBalancer,
         forceRecovery: Bool,
         ssVersionVector: VersionVector,
         locality: Int8,
         cc: CounterCollection,
         getCommitVersionRequests: Counter,
         getLiveCommittedVersionRequests: Counter,
         reportLiveCommittedVersionRequests: Counter,
         versionVectorTagUpdates: LatencySample,
         waitForPrevCommitRequests: Counter,
         nonWaitForPrevCommitRequests: Counter,
         versionVectorSizeOnCVReply: LatencySample,
         waitForPrevLatencies: LatencySample,
         addActor: PromiseStreamFutureVoid,
         logger: FutureVoid,
         balancer: FutureVoid) {
        self.dbgid = dbgid
        self.lastEpochEnd = lastEpochEnd
        self.recoveryTransactionVersion = recoveryTransactionVersion
//        self.prevTLogVersion = prevTLogVersion
//        self.liveCommittedVersion = liveCommittedVersion
        self.databaseLocked = databaseLocked
        self.proxyMetadataVersion = proxyMetadataVersion
        self.minKnownCommittedVersion = minKnownCommittedVersion
        self.coordinators = coordinators
        self.version = version
        self.lastVersionTime = lastVersionTime
        self.referenceVersion = referenceVersion
        self.lastCommitProxyVersionReplies = lastCommitProxyVersionReplies
        self.myInterface = myInterface
        self.resolutionBalancer = resolutionBalancer
        self.forceRecovery = forceRecovery
        self.ssVersionVector = ssVersionVector
        self.locality = locality
        self.cc = cc
        self.getCommitVersionRequests = getCommitVersionRequests
        self.getLiveCommittedVersionRequests = getLiveCommittedVersionRequests
        self.reportLiveCommittedVersionRequests = reportLiveCommittedVersionRequests
        self.versionVectorTagUpdates = versionVectorTagUpdates
        self.waitForPrevCommitRequests = waitForPrevCommitRequests
        self.nonWaitForPrevCommitRequests = nonWaitForPrevCommitRequests
        self.versionVectorSizeOnCVReply = versionVectorSizeOnCVReply
        self.waitForPrevLatencies = waitForPrevLatencies
        self.addActor = addActor
        self.logger = logger
        self.balancer = balancer
    }

}


extension UID: Hashable {
    public func hash(into hasher: inout Swift.Hasher) {
        self.first().hash(into: &hasher)
        self.second().hash(into: &hasher)
    }

    public static func ==(lhs: UID, rhs: UID) -> Swift.Bool {
        lhs.first() == rhs.first() && lhs.second() == rhs.second()
    }
}

extension MasterDataActor {

    /// Reply still done via req.reply
    func getVersion(req: GetCommitVersionRequest) async {
        // NOTE: the `req` is inout since `req.reply.sendNever()` imports as `mutating`
        var req = req

        // TODO: Wrap with a tracing span
        // BEFORE:
        // state std::map<UID, CommitProxyVersionReplies>::iterator proxyItr =
        //        self.lastCommitProxyVersionReplies.find(req.requestingProxy) // lastCommitProxyVersionReplies never changes
        let proxyItr: CommitProxyVersionReplies? = self.lastCommitProxyVersionReplies[req.requestingProxy]

        // NOTE: += operator is not imported, so we added an increment()
        // FIXME(swift): rdar://100012917 ([c++ interop] += operator does not seem to work/be imported)
        // FIXME: self.getCommitVersionRequests = self.getCommitVersionRequests.successor() // this is ++

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

                        if (self.version == invalidVersion) {
                            self.lastVersionTime = now()
                            self.version = self.recoveryTransactionVersion
                            rep.prevVersion = self.lastEpochEnd

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
