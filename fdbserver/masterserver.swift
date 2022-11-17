import Flow
import flow_swift
import FlowFutureSupport
import flow_swift_future
@preconcurrency import FDBServer
import FDBClient
import fdbclient_swift

func clamp(_ v: Version, lowerBound: Version, upperBound: Version) -> Version {
    return max(min(v, upperBound), lowerBound)
}

// FIXME: This should be synthesized?
extension Swift.Optional where Wrapped == Version {
    init(cxxOptional value: OptionalVersion) {
        guard value.present() else {
            self = nil
            return
        }
        self = Version(value.__getUnsafe().pointee)
    }
}

@_expose(Cxx)
public func figureVersion(current: Version,
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

extension NotifiedVersionValue {
    mutating func atLeast(_ limit: VersionMetricHandle.ValueType) async throws {
        var f: FutureVoid = self.whenAtLeast(limit)
        let _ = try await f.waitValue
    }
}

public class CommitProxyVersionReplies {
    var replies: [UInt64: GetCommitVersionReply] = [:]
    var latestRequestNum: NotifiedVersionValue
    
    public init() {
        latestRequestNum = NotifiedVersionValue(0)
    }
}

@_expose(Cxx)
public actor MasterDataActor {
    var lastCommitProxyVersionReplies: [Flow.UID: CommitProxyVersionReplies] = [:]

    public init() {
    }

    func registerLastCommitProxyVersionReplies(uids: [Flow.UID]) async {
        lastCommitProxyVersionReplies = [:]
        for uid in uids {
            lastCommitProxyVersionReplies[uid] = CommitProxyVersionReplies()
        }
    }

    nonisolated public func registerLastCommitProxyVersionReplies(uids: [Flow.UID], result promise: PromiseVoid) {
        Task {
            await registerLastCommitProxyVersionReplies(uids: uids)
            var result = Flow.Void()
            promise.send(&result)
        }
    }

    func getVersion(cxxState myself: MasterData, req: GetCommitVersionRequest) async -> GetCommitVersionReply? {

        print("[swift] getVersion impl, requestNum: \(req.requestNum) -> ")

        myself.getCommitVersionRequests += 1

        guard let lastVersionReplies = lastCommitProxyVersionReplies[req.requestingProxy] else {
            // Request from invalid proxy (e.g. from duplicate recruitment request)
            return nil
        }

        // CODE_PROBE(lastVersionReplies.latestRequestNum.get() < req.requestNum - 1, "Commit version request queued up")
        try! await lastVersionReplies.latestRequestNum
                .atLeast(VersionMetricHandle.ValueType(req.requestNum - UInt64(1)))

        if let lastReply = lastVersionReplies.replies[req.requestNum] {
            // NOTE: CODE_PROBE is macro, won't be imported
            // CODE_PROBE(true, "Duplicate request for sequence")
            return lastReply
        } else if (req.requestNum <= lastVersionReplies.latestRequestNum.get()) {
            // NOTE: CODE_PROBE is macro, won't be imported
            // CODE_PROBE(true, "Old request for previously acknowledged sequence - may be impossible with current FlowTransport");
            assert(req.requestNum < lastVersionReplies.latestRequestNum.get())
            // The latest request can never be acknowledged
            return nil
        }

        var rep = GetCommitVersionReply()

        if (myself.version == invalidVersion) {
            myself.lastVersionTime = now()
            myself.version = myself.recoveryTransactionVersion
            rep.prevVersion = myself.lastEpochEnd
        } else {
            var t1 = now()
            if BUGGIFY() {
                t1 = myself.lastVersionTime
            }

            let toAdd = max(Version(1), min(
                    getServerKnobs().MAX_READ_TRANSACTION_LIFE_VERSIONS,
                    Version(Double(getServerKnobs().VERSIONS_PER_SECOND) * (t1 - myself.lastVersionTime))))
            rep.prevVersion = myself.version
            if let referenceVersion = Swift.Optional(cxxOptional: myself.referenceVersion) {
                myself.version = figureVersion(current: myself.version,
                        now: SwiftGNetwork.timer(),
                        reference: referenceVersion,
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
        // print("[swift][\(#fileID):\(#line)](\(#function))\(Self.self) reply with version: \(rep.version)")

        lastVersionReplies.replies = lastVersionReplies.replies.filter({ $0.0 > req.mostRecentProcessedRequestNum })
        lastVersionReplies.replies[req.requestNum] = rep
        assert(rep.prevVersion >= 0)

        assert(lastVersionReplies.latestRequestNum.get() == req.requestNum - 1)
        lastVersionReplies.latestRequestNum.set(Int(req.requestNum))
        print("[swift] getVersion impl, requestNum: \(req.requestNum) -> version: \(rep.version)")
        return rep
    }

    /// Promise type must match result type of the target function.
    /// If missing, please declare new `using PromiseXXX = Promise<XXX>;` in `swift_<MODULE>_future_support.h` files.
    nonisolated public func getVersion(cxxState: MasterData, req: GetCommitVersionRequest, result promise: PromiseVoid) {
        // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")
        Task {
            // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl in task!")
            if let rep = await getVersion(cxxState: cxxState, req: req) {
                var repMut = rep
                req.reply.send(&repMut)
            } else {
                req.reply.sendNever()
            }
            var result = Flow.Void()
            promise.send(&result)
            // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Done calling getVersion impl!")
        }
    }

    // ACTOR Future<Void> waitForPrev(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
    func waitForPrev(cxxState myself: MasterData, req: ReportRawCommittedVersionRequest) async -> Void? {
        print("[swift] waitForPrev impl, version: \(req.version) -> ")

        // state double startTime = now();
        let startTime = now()

        // wait(self->liveCommittedVersion.whenAtLeast(req.prevVersion.get()));
        // NOTE.  To fix: error: value of type 'MasterData' has no member 'liveCommittedVersion'
        // change type of liveCommittedVersion from NotifiedVersion to NotifiedVersionValue
        // To fix: note: C++ method 'get' that returns unsafe projection of type 'reference' not imported
        // replace call to get() with __getUnsafe()
        // TODO: Why doesn't the C++ code check to see if prevVersion is present?
        //try! await myself.liveCommittedVersion.atLeast(req.prevVersion.__getUnsafe().pointee/* TODO: Sane way to reference an Optional<Int>? */)
        if let prevVersion = Swift.Optional(cxxOptional: req.prevVersion) {
            // TODO: Something is funky. pointee could be implicitly cast to an Int.  prevVersion is an Int64.
            try! await myself.liveCommittedVersion.atLeast(Int(prevVersion))
        }


        // double latency = now() - startTime;
        let latency = now() - startTime

        // self->waitForPrevLatencies.addMeasurement(latency);
        myself.waitForPrevLatencies.addMeasurement(latency)

        // ++self->waitForPrevCommitRequests;
        // Note.  To fix: error: value of type 'MasterData' has no member 'waitForPrevCommitRequests'
        // change the field type in MasterData from Counter to CounterValue
        myself.waitForPrevCommitRequests += 1;

        // updateLiveCommittedVersion(self, req);
        // NOTE.  To fix: error: cannot find 'updateLiveCommittedVersion' in scope
        // Add the C++ declaration to the .h...
        // Note:  To fix. cannot convert value of type 'MasterData' to expected argument type '__CxxTemplateInst9ReferenceI10MasterDataE'
        // Change the C++ method to take a raw C++ reference instead of an FDB Reference<Foo>
        updateLiveCommittedVersion(myself, req)

        // req.reply.send(Void());
        return Void()
    }

    nonisolated public func waitForPrev(cxxState: MasterData, req: ReportRawCommittedVersionRequest, result promise: PromiseVoid) {
        Task {
            if var rep = await waitForPrev(cxxState: cxxState, req: req) {
                req.reply.send(&rep)
            } else {
                req.reply.sendNever()
            }
            var result = Flow.Void()
            promise.send(&result)
        }
    }

    public func provideVersions(cxxState: MasterData) async {
    }

    nonisolated public func provideVersions(cxxState: MasterData, result promise: PromiseVoid) {
        Task {
            await provideVersions(cxxState: cxxState)

            var result = Flow.Void()
            promise.send(&result)
        }
    }

    func updateRecoveryData(cxxState myself: MasterData, req: UpdateRecoveryDataRequest) async -> Flow.Void {
//        TraceEvent("UpdateRecoveryData", self->dbgid)
//                .detail("ReceivedRecoveryTxnVersion", req.recoveryTransactionVersion)
//                .detail("ReceivedLastEpochEnd", req.lastEpochEnd)
//                .detail("CurrentRecoveryTxnVersion", self->recoveryTransactionVersion)
//                .detail("CurrentLastEpochEnd", self->lastEpochEnd)
//                .detail("NumCommitProxies", req.commitProxies.size())
//                .detail("VersionEpoch", req.versionEpoch)
//                .detail("PrimaryLocality", req.primaryLocality);
//
//        self->recoveryTransactionVersion = req.recoveryTransactionVersion;
//        self->lastEpochEnd = req.lastEpochEnd;
//
//        if (req.commitProxies.size() > 0) {
//            auto registeredUIDs = Swift::Array<UID>::init();
//            for (size_t j = 0; j < req.commitProxies.size(); ++j)
//            registeredUIDs.append(req.commitProxies[j].id());
//            auto promise = Promise<Void>();
//            self->swiftImpl->registerLastCommitProxyVersionReplies(registeredUIDs, promise);
//            wait(promise.getFuture());
//        }
//        if (req.versionEpoch.present()) {
//            self->referenceVersion = req.versionEpoch.get();
//        } else if (BUGGIFY) {
//            // Cannot use a positive version epoch in simulation because of the
//            // clock starting at 0. A positive version epoch would mean the initial
//            // cluster version was negative.
//            // TODO: Increase the size of this interval after fixing the issue
//            // with restoring ranges with large version gaps.
//            self->referenceVersion = deterministicRandom()->randomInt64(-1e6, 0);
//        }
//
//        self->resolutionBalancer.setCommitProxies(req.commitProxies);
//        self->resolutionBalancer.setResolvers(req.resolvers);
//
//        self->locality = req.primaryLocality;

        return Flow.Void()
    }

    nonisolated public func updateRecoveryDataStream(result promise: PromiseVoid) {}
    nonisolated public func waitForPrev_2222(cxxState: MasterData, req: ReportRawCommittedVersionRequest, result promise: PromiseVoid) {}


    /// Equivalent to:
    /// ```
    /// ACTOR Future<Void> updateRecoveryData(Reference<MasterData> self) {
    ///     loop {
    ///		    state UpdateRecoveryDataRequest req = waitNext(self->myInterface.updateRecoveryData.getFuture());
    ///     }
    /// }
    /// ```
    // nonisolated public func registerLastCommitProxyVersionReplies(uids: [Flow.UID], result promise: PromiseVoid) {
    nonisolated public func updateRecoveryDataStream(cxxState myself: MasterData, result promise: PromiseVoid) {
        Task {
            for try await req in myself.myInterface.updateRecoveryData.getFuture() {
                // Dispatch as another task, so we can await the result for this call,
                // but also immediately await for another incoming request. We do want
                // to allow interleaving/concurrent requests, which we get by scheduling
                // more onto the actor; It guarantees thread safety, but task executions may interleave.
                Task {
                    var result = await self.updateRecoveryData(cxxState: myself, req: req)
                    req.reply.send(&result)
                }
            }
//            // TODO: support this directly:
//            for await req in myself.myInterface.updateRecoveryData {
//
//            }

            var result = Flow.Void()
            promise.send(&result)
        }
    }
}