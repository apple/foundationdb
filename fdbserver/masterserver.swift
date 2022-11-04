import Flow
import flow_swift
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

extension NotifiedVersionValue {
    mutating func atLeast(_ limit: VersionMetricHandle.ValueType) async throws {
        var f = self.whenAtLeast(limit)
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

public actor MasterDataActor {
    var lastCommitProxyVersionReplies: [Flow.UID: CommitProxyVersionReplies] = [:]

    init() {
    }

    func registerLastCommitProxyVersionReplies(uids: StdVectorOfUIDs) async {
        lastCommitProxyVersionReplies = [:]
        // FIXME: Make this a for-in loop once we have automatic Sequence conformance.
        for i in 0..<uids.size() {
            lastCommitProxyVersionReplies[uids[i]] = CommitProxyVersionReplies()
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
}

/// Bridge type that wraps the target actor.
@_expose(Cxx, "MasterDataActor")
public struct MasterDataActorCxx {
    let myself: MasterDataActor

    /// Mirror actor initializer, and initialize `myself`.
    public init() {
        myself = MasterDataActor()
    }

    public func registerLastCommitProxyVersionReplies(uids: StdVectorOfUIDs, result promise: PromiseVoid) {
        Task {
            await myself.registerLastCommitProxyVersionReplies(uids: uids)
            var result = Flow.Void()
            promise.send(&result)
        }
    }

    /// Promise type must match result type of the target function.
    /// If missing, please declare new `using PromiseXXX = Promise<XXX>;` in `swift_<MODULE>_future_support.h` files.
    public func getVersion(cxxState: MasterData, req: GetCommitVersionRequest, result promise: PromiseVoid) {
        // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl!")
        // FIXME: remove after https://github.com/apple/swift/issues/61627 makes MasterData refcounted FRT.
        swift_workaround_retainMasterData(cxxState)
        Task {
            // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl in task!")
            if let rep = await myself.getVersion(cxxState: cxxState, req: req) {
                var repMut = rep
                req.reply.send(&repMut)
            } else {
                req.reply.sendNever()
            }
            var result = Flow.Void()
            promise.send(&result)
            // FIXME: remove after https://github.com/apple/swift/issues/61627 makes MasterData refcounted FRT.
            swift_workaround_releaseMasterData(cxxState)
            // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Done calling getVersion impl!")
        }
    }

    public func waitForPrev(cxxState: MasterData, req: ReportRawCommittedVersionRequest, result promise: PromiseVoid) {
        // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift waitForPrev impl!")
        // FIXME: remove after https://github.com/apple/swift/issues/61627 makes MasterData refcounted FRT.
        swift_workaround_retainMasterData(cxxState)
        Task {
            // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Calling swift getVersion impl in task!")
            if let rep = await myself.waitForPrev(cxxState: cxxState, req: req) {
                var repMut = rep
                req.reply.send(&repMut)
            } else {
                req.reply.sendNever()
            }
            var result = Flow.Void()
            promise.send(&result)
            // FIXME: remove after https://github.com/apple/swift/issues/61627 makes MasterData refcounted FRT.
            swift_workaround_releaseMasterData(cxxState)
            // print("[swift][tid:\(_tid())][\(#fileID):\(#line)](\(#function)) Done calling getVersion impl!")
        }
    }
}
