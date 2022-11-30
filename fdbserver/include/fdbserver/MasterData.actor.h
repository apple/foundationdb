/*
 * MasterData.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <map>

#include "fdbrpc/sim_validation.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/Knobs.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/ResolutionBalancer.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "flow/ActorCollection.h"
#include "flow/Trace.h"
#include "flow/swift_support.h"
#include "fdbclient/VersionVector.h"

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_MASTERDATA_ACTOR_G_H)
#define FDBSERVER_MASTERDATA_ACTOR_G_H
#include "fdbserver/MasterData.actor.g.h"
#elif !defined(FDBSERVER_MASTERDATA_ACTOR_H)
#define FDBSERVER_MASTERDATA_ACTOR_H
#include "flow/actorcompiler.h" // This must be the last #include

// FIXME: Remove once https://github.com/apple/swift/issues/61620 is fixed.
#define SWIFT_CXX_REF_MASTERDATA   \
    __attribute__((swift_attr("import_as_ref")))   \
    __attribute__((swift_attr("retain:addrefMasterData")))   \
    __attribute__((swift_attr("release:delrefMasterData")))

// A type with Swift value semantics for working with `Counter` types.
class CounterValue {
// FIXME: Delete immortal annotation from `Counter`.
public:
    using Value = Counter::Value;

    CounterValue(std::string const& name, CounterCollection& collection);

    void operator+=(Value delta);
    void operator++();
    void clear();
private:
    std::shared_ptr<Counter> value;
};

// A concrete Optional<Version> type that can be referenced in Swift.
using OptionalVersion = Optional<Version>;

// Forward declare the Swift actor.
namespace fdbserver_swift {
class MasterDataActor;
}

// FIXME (after the one below): Use SWIFT_CXX_REF once https://github.com/apple/swift/issues/61620 is fixed.
struct SWIFT_CXX_REF_MASTERDATA MasterData : NonCopyable, ReferenceCounted<MasterData> {
    UID dbgid;

    Version lastEpochEnd, // The last version in the old epoch not (to be) rolled back in this recovery
        recoveryTransactionVersion; // The first version in this epoch

    NotifiedVersion prevTLogVersion; // Order of transactions to tlogs

    NotifiedVersionValue liveCommittedVersion; // The largest live committed version reported by commit proxies.
    bool databaseLocked;
    Optional<Value> proxyMetadataVersion;
    Version minKnownCommittedVersion;

    ServerCoordinators coordinators;

    Version version; // The last version assigned to a proxy by getVersion()
    double lastVersionTime;
    Optional<Version> referenceVersion;
    MasterInterface myInterface;

    ResolutionBalancer resolutionBalancer;

    bool forceRecovery;

    // Captures the latest commit version targeted for each storage server in the cluster.
    // @todo We need to ensure that the latest commit versions of storage servers stay
    // up-to-date in the presence of key range splits/merges.
    VersionVector ssVersionVector;

    int8_t locality; // sequencer locality

    CounterCollection cc;
    CounterValue getCommitVersionRequests;
    CounterValue getLiveCommittedVersionRequests;
    CounterValue reportLiveCommittedVersionRequests;
    // This counter gives an estimate of the number of non-empty peeks that storage servers
    // should do from tlogs (in the worst case, ignoring blocking peek timeouts).
    LatencySample versionVectorTagUpdates;
    CounterValue waitForPrevCommitRequests;
    CounterValue nonWaitForPrevCommitRequests;
    LatencySample versionVectorSizeOnCVReply;
    LatencySample waitForPrevLatencies;

    PromiseStream<Future<Void>> addActor;

    Future<Void> logger;
    Future<Void> balancer;
    
    std::unique_ptr<fdbserver_swift::MasterDataActor> swiftImpl;

    MasterData(Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
               MasterInterface const& myInterface,
               ServerCoordinators const& coordinators,
               ClusterControllerFullInterface const& clusterController,
               Standalone<StringRef> const& dbId,
               PromiseStream<Future<Void>> addActor,
               bool forceRecovery);

    ~MasterData();

    inline ResolutionBalancer &getResolutionBalancer() {
        return resolutionBalancer;
    }
};

void updateLiveCommittedVersion(MasterData & self, ReportRawCommittedVersionRequest req);

// FIXME: Remove once https://github.com/apple/swift/issues/61620 is fixed.
inline void addrefMasterData(MasterData* ptr) {
    addref(ptr);
}

// FIXME: Remove once https://github.com/apple/swift/issues/61620 is fixed.
inline void delrefMasterData(MasterData* ptr) {
    delref(ptr);
}

using ReferenceMasterData = Reference<MasterData>;

using StdVectorOfUIDs = std::vector<UID>;

// FIXME: Workaround for linker issue (rdar://101092732).
void swift_workaround_setLatestRequestNumber(NotifiedVersion &latestRequestNum,
                                             Version v);

#include "flow/unactorcompiler.h"
#endif
