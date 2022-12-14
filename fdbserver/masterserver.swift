import Flow
import flow_swift
@preconcurrency import FDBServer
import FDBClient
import fdbclient_swift

func clamp(_ v: Version, lowerBound: Version, upperBound: Version) -> Version {
    return max(min(v, upperBound), lowerBound)
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

@_expose(Cxx)
public func updateLiveCommittedVersion(myself: MasterData, req: ReportRawCommittedVersionRequest) {
    myself.minKnownCommittedVersion = max(myself.minKnownCommittedVersion, req.minKnownCommittedVersion)

    if req.version > myself.liveCommittedVersion.get() {
        if getServerKnobs().ENABLE_VERSION_VECTOR,
           let writtenTags = Swift.Optional(cxxOptional: req.writtenTags) {
            let primaryLocality = getServerKnobs().ENABLE_VERSION_VECTOR_HA_OPTIMIZATION ?
                    myself.locality : Int8(tagLocalityInvalid)
             myself.ssVersionVector.setVersion(writtenTags, req.version, primaryLocality)
             myself.versionVectorTagUpdates.addMeasurement(Double(writtenTags.size()))
        }

        let curTime = now()
        // add debug here to change liveCommittedVersion to time bound of now()
        debug_advanceVersionTimestamp(Int64(myself.liveCommittedVersion.get()), curTime + getClientKnobs().MAX_VERSION_CACHE_LAG)
        // also add req.version but with no time bound
        debug_advanceVersionTimestamp(Int64(req.version), Double.greatestFiniteMagnitude)
        myself.databaseLocked = req.locked;
        myself.proxyMetadataVersion = req.metadataVersion
        // Note the set call switches context to any waiters on liveCommittedVersion before continuing.
        myself.liveCommittedVersion.set(Int(req.version))
    }
	myself.reportLiveCommittedVersionRequests += 1
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

    func getVersion(myself: MasterData, req: GetCommitVersionRequest) async -> GetCommitVersionReply? {
        myself.getCommitVersionRequests += 1

        guard let lastVersionReplies = self.lastCommitProxyVersionReplies[req.requestingProxy] else {
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
                        reference: Version(referenceVersion),
                        toAdd: toAdd,
                        maxVersionRateModifier: getServerKnobs().MAX_VERSION_RATE_MODIFIER,
                        maxVersionRateOffset: getServerKnobs().MAX_VERSION_RATE_OFFSET)
                assert(myself.version > rep.prevVersion)
            } else {
                myself.version += toAdd
            }

            // NOTE: CODE_PROBE is macro, won't be imported
            // CODE_PROBE(self.version - rep.prevVersion == 1, "Minimum possible version gap");
            let _ /*maxVersionGap*/ = myself.version - rep.prevVersion == getServerKnobs().MAX_READ_TRANSACTION_LIFE_VERSIONS
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
    nonisolated public func getVersion(myself: MasterData, req: GetCommitVersionRequest,
                                       result promise: PromiseVoid) {
        Task {
            if var rep = await getVersion(myself: myself, req: req) {
                req.reply.send(&rep)
            } else {
                req.reply.sendNever()
            }
            var result = Flow.Void()
            promise.send(&result)
        }
    }

    // ACTOR Future<Void> waitForPrev(Reference<MasterData> self, ReportRawCommittedVersionRequest req) {
    func waitForPrev(myself: MasterData, req: ReportRawCommittedVersionRequest) async -> Void {
        print("[swift] waitForPrev impl, version: \(req.version) -> ")

        let startTime = now()

        // wait(myself.liveCommittedVersion.whenAtLeast(req.prevVersion.get()));
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


        let latency = now() - startTime

        myself.waitForPrevLatencies.addMeasurement(latency)

        // Note.  To fix: error: value of type 'MasterData' has no member 'waitForPrevCommitRequests'
        // change the field type in MasterData from Counter to CounterValue
        myself.waitForPrevCommitRequests += 1

        // NOTE.  To fix: error: cannot find 'updateLiveCommittedVersion' in scope
        // Add the C++ declaration to the .h...
        // Note:  To fix. cannot convert value of type 'MasterData' to expected argument type '__CxxTemplateInst9ReferenceI10MasterDataE'
        // Change the C++ method to take a raw C++ reference instead of an FDB Reference<Foo>
        updateLiveCommittedVersion(myself: myself, req: req)

        return Void()
    }

    nonisolated public func waitForPrev(myself: MasterData, req: ReportRawCommittedVersionRequest,
                                        result promise: PromiseVoid) {
        Task {
            var rep = await waitForPrev(myself: myself, req: req)
            req.reply.send(&rep)
            promise.send(&rep)
        }
    }

    public func provideVersions(myself: MasterData) async throws -> Flow.Void {
        //  state ActorCollection versionActors(false);
        //
        //	loop choose {
        //		when(GetCommitVersionRequest req = waitNext(myself.myInterface.getCommitVersion.getFuture())) {
        //			versionActors.add(getVersion(self, req));
        //		}
        //		when(wait(versionActors.getResult())) {}
        //	}
        try await withThrowingTaskGroup(of: Swift.Void.self) { group in
            // TODO(swift): the only throw here is from the Sequence, but can it actually ever happen...?
            for try await req in myself.myInterface.getCommitVersion.getFuture() {
                _ = group.addTaskUnlessCancelled {
                    if var rep = await self.getVersion(myself: myself, req: req) {
                        req.reply.send(&rep)
                    } else {
                        req.reply.sendNever()
                    }
                }

                if Task.isCancelled {
                    return Flow.Void()
                }
            }
            return Flow.Void()
        }

    }

    nonisolated public func provideVersions(myself: MasterData, result promise: PromiseVoid) {
        Task {
            // TODO(swift): handle the error
            var void = try await self.provideVersions(myself: myself)
            promise.send(&void)
        }
    }

    public func getLiveCommittedVersion(myself: MasterData, _ req: GetRawCommittedVersionRequest) -> GetRawCommittedVersionReply {
        if req.debugID.present() {
            // TODO: trace event here
            //     g_traceBatch.addEvent("TransactionDebug",
            //		    req.debugID.get().first(),
            //			"MasterServer.serveLiveCommittedVersion.GetRawCommittedVersion");
        }

        if myself.liveCommittedVersion.get() == invalidVersion {
            myself.liveCommittedVersion.set(Int(myself.recoveryTransactionVersion));
        }

        myself.getLiveCommittedVersionRequests += 1

        var reply = GetRawCommittedVersionReply()
        let liveCommittedVersion: Int = myself.liveCommittedVersion.get()
        reply.version = Version(liveCommittedVersion)
        reply.locked = myself.databaseLocked
        reply.metadataVersion = myself.proxyMetadataVersion
        reply.minKnownCommittedVersion = myself.minKnownCommittedVersion

        if (getServerKnobs().ENABLE_VERSION_VECTOR) {
             // myself.ssVersionVector.getDelta(req.maxVersion, &reply.ssVersionVectorDelta) // FIXME: help!
            // FIXME:
            //     /root/build/fdbserver/CMakeFiles/fdbserver_swift.dir/./masterserver.swift.o: In function `VersionVector::getDelta(long, VersionVector&) const':
            //     masterserver.swift.o:(.text._ZNK13VersionVector8getDeltaElRS_[_ZNK13VersionVector8getDeltaElRS_]+0xc7): undefined reference to `std::tuple_element<0ul, std::pair<Tag, long> >::type const& std::get<0ul, Tag, long>(std::pair<Tag, long> const&)'
            //     masterserver.swift.o:(.text._ZNK13VersionVector8getDeltaElRS_[_ZNK13VersionVector8getDeltaElRS_]+0xd2): undefined reference to `std::tuple_element<1ul, std::pair<Tag, long> >::type const& std::get<1ul, Tag, long>(std::pair<Tag, long> const&)'
            myself.versionVectorSizeOnCVReply.addMeasurement(Double(reply.ssVersionVectorDelta.size()))
        }

        return reply
    }

    func reportLiveCommittedVersion(myself: MasterData, req: ReportRawCommittedVersionRequest) async -> Void {
        if let prevVersion = Swift.Optional(cxxOptional: req.prevVersion),
           getServerKnobs().ENABLE_VERSION_VECTOR &&
          (myself.liveCommittedVersion.get() != invalidVersion) &&
          (myself.liveCommittedVersion.get() < prevVersion) {
            return await waitForPrev(myself: myself, req: req)
        } else {
            updateLiveCommittedVersion(myself: myself, req: req)
            myself.nonWaitForPrevCommitRequests += 1
            return Void()
        }
    }

    public func serveLiveCommittedVersion(myself: MasterData) async -> Flow.Void {
        // TODO: use TaskPool
        await withThrowingTaskGroup(of: Swift.Void.self) { group in
            // getLiveCommittedVersion
            group.addTask {
                for try await req in myself.myInterface.getLiveCommittedVersion.getFuture() {
                    var rep = await self.getLiveCommittedVersion(myself: myself, req)
                    req.reply.send(&rep)
                }
            }

            // reportLiveCommittedVersion
            group.addTask {
                for try await req in myself.myInterface.reportLiveCommittedVersion.getFuture() {
                    var rep = await self.reportLiveCommittedVersion(myself: myself, req: req)
                    req.reply.send(&rep)
                }
            }
        }

        return Flow.Void()
    }

    nonisolated public func serveLiveCommittedVersion(myself: MasterData, result promise: PromiseVoid) {
        Task {
            var void = await self.serveLiveCommittedVersion(myself: myself)
            promise.send(&void)
        }
    }

    func updateRecoveryData(myself: MasterData, req: UpdateRecoveryDataRequest) async -> Flow.Void {
        // TODO: trace event here
        //        TraceEvent("UpdateRecoveryData", myself.dbgid)
        //                .detail("ReceivedRecoveryTxnVersion", req.recoveryTransactionVersion)
        //                .detail("ReceivedLastEpochEnd", req.lastEpochEnd)
        //                .detail("CurrentRecoveryTxnVersion", myself.recoveryTransactionVersion)
        //                .detail("CurrentLastEpochEnd", myself.lastEpochEnd)
        //                .detail("NumCommitProxies", req.commitProxies.size())
        //                .detail("VersionEpoch", req.versionEpoch)
        //                .detail("PrimaryLocality", req.primaryLocality)

        myself.recoveryTransactionVersion = req.recoveryTransactionVersion
        myself.lastEpochEnd = req.lastEpochEnd

        if (req.commitProxies.size() > 0) {
            var registeredUIDs = [UID]()
            for j in 0..<req.commitProxies.size() { // TODO(swift): can we make this be a Sequence?
                registeredUIDs.append(req.commitProxies[j].id())
            }
            await self.registerLastCommitProxyVersionReplies(uids: registeredUIDs)
        }

        if let versionEpoch = Swift.Optional(cxxOptional: req.versionEpoch) {
            myself.referenceVersion = OptionalVersion(versionEpoch)
        } else if BUGGIFY() {
            // Cannot use a positive version epoch in simulation because of the
            // clock starting at 0. A positive version epoch would mean the initial
            // cluster version was negative.
            // TODO: Increase the size of this interval after fixing the issue
            // with restoring ranges with large version gaps.
            let v = swift_get_randomInt64(deterministicRandom(), Int64(-1e6), 0)
            myself.referenceVersion = OptionalVersion(v)
        }

        myself.getResolutionBalancer().setCommitProxies(req.commitProxies)
        myself.getResolutionBalancer().setResolvers(req.resolvers)

        myself.locality = req.primaryLocality

        return Void()
    }

    public func serveUpdateRecoveryData(myself: MasterData) async throws -> Flow.Void {
        try await withThrowingTaskGroup(of: Swift.Void.self) { group in
            // Note: this is an example of one-by-one handling requests, notice the group.next() below.
            for try await req in myself.myInterface.updateRecoveryData.getFuture() {
                group.addTask {
                    var rep = await self.updateRecoveryData(myself: myself, req: req)
                    req.reply.send(&rep)
                }

                try await group.next()
            }
        }

        return Flow.Void()
    }

    nonisolated public func serveUpdateRecoveryData(myself: MasterData, promise: PromiseVoid) {
        Task {
            // TODO(swift): handle the error
            var void = try await self.serveUpdateRecoveryData(myself: myself)
            promise.send(&void)
        }
    }

}

extension MasterData {
    var swiftActorImpl: MasterDataActor {
        UnsafeRawPointer(self.getSwiftImpl()).load(as: MasterDataActor.self)
    }
}

@_expose(Cxx)
public func masterServerSwift(
        mi: MasterInterface,
        db: AsyncVar_ServerDBInfo,
        ccInterface: AsyncVar_Optional_ClusterControllerFullInterface,
        coordinators: ServerCoordinators,
        lifetime: LifetimeToken,
        forceRecovery: Bool,
        masterData: MasterData,
        promise: PromiseVoid) {
    Task {
        let myself = masterData.swiftActorImpl

        do {
            try await withThrowingTaskGroup(of: Swift.Void.self) { group in
                group.addTask {
                    var traceRoleFuture = traceRole(Role.MASTER, mi.id())
                    try await traceRoleFuture.waitValue // TODO: make these not mutating
                }
                group.addTask {
                    try await myself.provideVersions(myself: masterData)
                }
                group.addTask {
                    await myself.serveLiveCommittedVersion(myself: masterData)
                }
                group.addTask {
                    try await myself.serveUpdateRecoveryData(myself: masterData)
                }

//                CODE_PROBE(!lifetime.isStillValid(db->get().masterLifetime, mi.id() == db->get().master.id()),
//                        "Master born doomed");
//                TraceEvent("MasterLifetime", self->dbgid).detail("LifetimeToken", lifetime.toString());

                while true {
                    var change = db.onChange()
                    guard (try? await change.waitValue) != nil else {
                        return
                    }

                    guard lifetime.isStillValid(db.get().pointee.masterLifetime, mi.id() == db.get().pointee.master.id()) else {
                        // CODE_PROBE(true, "Master replaced, dying")
                        if BUGGIFY() {
                            try? await FlowClock.sleep(for: .seconds(5))
                        }

                        // throwing out of here, cancels all the other tasks in the group as well.
                        throw WorkerRemovedError()
                    }
                }
            }
        } catch {
            // TODO: WIP
        }

        var void = Void()
        promise.send(&void)
    }
}

struct WorkerRemovedError: Swift.Error {}