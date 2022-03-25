/*
 * ClusterRecovery.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#include <utility>

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_CLUSTERRECOVERY_ACTOR_G_H)
#define FDBSERVER_CLUSTERRECOVERY_ACTOR_G_H
#include "fdbserver/ClusterRecovery.actor.g.h"
#elif !defined(FDBSERVER_CLUSTERRECOVERY_ACTOR_H)
#define FDBSERVER_CLUSTERRECOVERY_ACTOR_H

#include "fdbclient/DatabaseContext.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/DBCoreState.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"
#include "flow/SystemMonitor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

typedef enum {
	CLUSTER_RECOVERY_STATE_EVENT_NAME,
	CLUSTER_RECOVERY_COMMIT_TLOG_EVENT_NAME,
	CLUSTER_RECOVERY_DURATION_EVENT_NAME,
	CLUSTER_RECOVERY_GENERATION_EVENT_NAME,
	CLUSTER_RECOVERY_SS_RECRUITMENT_EVENT_NAME,
	CLUSTER_RECOVERY_INVALID_CONFIG_EVENT_NAME,
	CLUSTER_RECOVERY_RECOVERING_EVENT_NAME,
	CLUSTER_RECOVERY_RECOVERED_EVENT_NAME,
	CLUSTER_RECOVERY_SNAPSHOT_CHECK_EVENT_NAME,
	CLUSTER_RECOVERY_PAUSE_AGENT_BACKUP_EVENT_NAME,
	CLUSTER_RECOVERY_COMMIT_EVENT_NAME,
	CLUSTER_RECOVERY_AVAILABLE_EVENT_NAME,
	CLUSTER_RECOVERY_METRICS_EVENT_NAME,
	CLUSTER_RECOVERY_LAST // Always the last entry
} ClusterRecoveryEventType;

ACTOR Future<Void> recoveryTerminateOnConflict(UID dbgid,
                                               Promise<Void> fullyRecovered,
                                               Future<Void> onConflict,
                                               Future<Void> switchedState);
std::string& getRecoveryEventName(ClusterRecoveryEventType type);

class ReusableCoordinatedState : NonCopyable {
public:
	Promise<Void> fullyRecovered;
	DBCoreState prevDBState;
	DBCoreState myDBState;
	bool finalWriteStarted;
	Future<Void> previousWrite;

	ReusableCoordinatedState(ServerCoordinators const& coordinators,
	                         PromiseStream<Future<Void>> const& addActor,
	                         UID const& dbgid)
	  : finalWriteStarted(false), previousWrite(Void()), cstate(coordinators), coordinators(coordinators),
	    addActor(addActor), dbgid(dbgid) {}

	Future<Void> read() { return _read(this); }

	Future<Void> write(DBCoreState newState, bool finalWrite = false) {
		previousWrite = _write(this, newState, finalWrite);
		return previousWrite;
	}

	Future<Void> move(ClusterConnectionString const& nc) { return cstate.move(nc); }

private:
	MovableCoordinatedState cstate;
	ServerCoordinators coordinators;
	PromiseStream<Future<Void>> addActor;
	Promise<Void> switchedState;
	UID dbgid;

	ACTOR Future<Void> _read(ReusableCoordinatedState* self) {
		Value prevDBStateRaw = wait(self->cstate.read());
		Future<Void> onConflict = recoveryTerminateOnConflict(
		    self->dbgid, self->fullyRecovered, self->cstate.onConflict(), self->switchedState.getFuture());
		if (onConflict.isReady() && onConflict.isError()) {
			throw onConflict.getError();
		}
		self->addActor.send(onConflict);

		if (prevDBStateRaw.size()) {
			self->prevDBState = BinaryReader::fromStringRef<DBCoreState>(prevDBStateRaw, IncludeVersion());
			self->myDBState = self->prevDBState;
		}

		return Void();
	}

	ACTOR Future<Void> _write(ReusableCoordinatedState* self, DBCoreState newState, bool finalWrite) {
		if (self->finalWriteStarted) {
			wait(Future<Void>(Never()));
		}

		if (finalWrite) {
			self->finalWriteStarted = true;
		}

		try {
			wait(self->cstate.setExclusive(
			    BinaryWriter::toValue(newState, IncludeVersion(ProtocolVersion::withDBCoreState()))));
		} catch (Error& e) {
			TEST(true); // Master displaced during writeMasterState
			throw;
		}

		self->myDBState = newState;

		if (!finalWrite) {
			self->switchedState.send(Void());
			self->cstate = MovableCoordinatedState(self->coordinators);
			Value rereadDBStateRaw = wait(self->cstate.read());
			DBCoreState readState;
			if (rereadDBStateRaw.size())
				readState = BinaryReader::fromStringRef<DBCoreState>(rereadDBStateRaw, IncludeVersion());

			if (readState != newState) {
				TraceEvent("RecoveryTerminated", self->dbgid).detail("Reason", "CStateChanged");
				TEST(true); // Coordinated state changed between writing and reading, recovery restarting
				throw worker_removed();
			}
			self->switchedState = Promise<Void>();
			self->addActor.send(recoveryTerminateOnConflict(
			    self->dbgid, self->fullyRecovered, self->cstate.onConflict(), self->switchedState.getFuture()));
		} else {
			self->fullyRecovered.send(Void());
		}

		return Void();
	}
};

struct ClusterRecoveryData : NonCopyable, ReferenceCounted<ClusterRecoveryData> {
	ClusterControllerData* controllerData;

	UID dbgid;

	AsyncTrigger registrationTrigger;
	Version lastEpochEnd, // The last version in the old epoch not (to be) rolled back in this recovery
	    recoveryTransactionVersion; // The first version in this epoch
	double lastCommitTime;

	Version liveCommittedVersion; // The largest live committed version reported by commit proxies.
	bool databaseLocked;
	Optional<Value> proxyMetadataVersion;
	Version minKnownCommittedVersion;

	DatabaseConfiguration originalConfiguration;
	DatabaseConfiguration configuration;
	std::vector<Optional<Key>> primaryDcId;
	std::vector<Optional<Key>> remoteDcIds;
	bool hasConfiguration;

	ServerCoordinators coordinators;

	Reference<ILogSystem> logSystem;
	double lastVersionTime;
	LogSystemDiskQueueAdapter* txnStateLogAdapter;
	IKeyValueStore* txnStateStore;
	int64_t memoryLimit;
	std::map<Optional<Value>, int8_t> dcId_locality;
	std::vector<Tag> allTags;

	int8_t getNextLocality() {
		int8_t maxLocality = -1;
		for (auto it : dcId_locality) {
			maxLocality = std::max(maxLocality, it.second);
		}
		return maxLocality + 1;
	}

	std::vector<CommitProxyInterface> commitProxies;
	std::vector<CommitProxyInterface> provisionalCommitProxies;
	std::vector<GrvProxyInterface> grvProxies;
	std::vector<GrvProxyInterface> provisionalGrvProxies;
	std::vector<ResolverInterface> resolvers;

	std::map<UID, CommitProxyVersionReplies> lastCommitProxyVersionReplies;

	UID clusterId;
	Standalone<StringRef> dbId;

	MasterInterface masterInterface;
	LifetimeToken masterLifetime;
	const ClusterControllerFullInterface
	    clusterController; // If the cluster controller changes, this master will die, so this is immutable.

	ReusableCoordinatedState cstate;
	Promise<Void> recoveryReadyForCommits;
	Promise<Void> cstateUpdated;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	int64_t registrationCount; // Number of different MasterRegistrationRequests sent to clusterController

	RecoveryState recoveryState;

	PromiseStream<Future<Void>> addActor;
	Reference<AsyncVar<bool>> recruitmentStalled;
	bool forceRecovery;
	bool neverCreated;
	int8_t safeLocality;
	int8_t primaryLocality;

	std::vector<WorkerInterface> backupWorkers; // Recruited backup workers from cluster controller.

	CounterCollection cc;
	Counter changeCoordinatorsRequests;
	Counter getCommitVersionRequests;
	Counter backupWorkerDoneRequests;
	Counter getLiveCommittedVersionRequests;
	Counter reportLiveCommittedVersionRequests;

	Future<Void> logger;

	Reference<EventCacheHolder> recoveredConfigEventHolder;
	Reference<EventCacheHolder> clusterRecoveryStateEventHolder;
	Reference<EventCacheHolder> clusterRecoveryGenerationsEventHolder;
	Reference<EventCacheHolder> clusterRecoveryDurationEventHolder;
	Reference<EventCacheHolder> clusterRecoveryAvailableEventHolder;

	ClusterRecoveryData(ClusterControllerData* controllerData,
	                    Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
	                    MasterInterface const& masterInterface,
	                    LifetimeToken const& masterLifetimeToken,
	                    ServerCoordinators const& coordinators,
	                    ClusterControllerFullInterface const& clusterController,
	                    Standalone<StringRef> const& dbId,
	                    PromiseStream<Future<Void>> const& addActor,
	                    bool forceRecovery)

	  : controllerData(controllerData), dbgid(masterInterface.id()), lastEpochEnd(invalidVersion),
	    recoveryTransactionVersion(invalidVersion), lastCommitTime(0), liveCommittedVersion(invalidVersion),
	    databaseLocked(false), minKnownCommittedVersion(invalidVersion), hasConfiguration(false),
	    coordinators(coordinators), lastVersionTime(0), txnStateStore(nullptr), memoryLimit(2e9), dbId(dbId),
	    masterInterface(masterInterface), masterLifetime(masterLifetimeToken), clusterController(clusterController),
	    cstate(coordinators, addActor, dbgid), dbInfo(dbInfo), registrationCount(0), addActor(addActor),
	    recruitmentStalled(makeReference<AsyncVar<bool>>(false)), forceRecovery(forceRecovery), neverCreated(false),
	    safeLocality(tagLocalityInvalid), primaryLocality(tagLocalityInvalid), cc("Master", dbgid.toString()),
	    changeCoordinatorsRequests("ChangeCoordinatorsRequests", cc),
	    getCommitVersionRequests("GetCommitVersionRequests", cc),
	    backupWorkerDoneRequests("BackupWorkerDoneRequests", cc),
	    getLiveCommittedVersionRequests("GetLiveCommittedVersionRequests", cc),
	    reportLiveCommittedVersionRequests("ReportLiveCommittedVersionRequests", cc),
	    recoveredConfigEventHolder(makeReference<EventCacheHolder>("RecoveredConfig")) {
		clusterRecoveryStateEventHolder = makeReference<EventCacheHolder>(
		    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME));
		clusterRecoveryGenerationsEventHolder = makeReference<EventCacheHolder>(
		    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_GENERATION_EVENT_NAME));
		clusterRecoveryDurationEventHolder = makeReference<EventCacheHolder>(
		    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_DURATION_EVENT_NAME));
		clusterRecoveryAvailableEventHolder = makeReference<EventCacheHolder>(
		    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_AVAILABLE_EVENT_NAME));
		logger = traceCounters(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_METRICS_EVENT_NAME),
		                       dbgid,
		                       SERVER_KNOBS->WORKER_LOGGING_INTERVAL,
		                       &cc,
		                       getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_METRICS_EVENT_NAME));
		if (forceRecovery && !controllerData->clusterControllerDcId.present()) {
			TraceEvent(SevError, "ForcedRecoveryRequiresDcID").log();
			forceRecovery = false;
		}
	}
	~ClusterRecoveryData() {
		if (txnStateStore)
			txnStateStore->close();
	}
};

ACTOR Future<Void> recruitNewMaster(ClusterControllerData* cluster,
                                    ClusterControllerData::DBInfo* db,
                                    MasterInterface* newMaster);
ACTOR Future<Void> cleanupRecoveryActorCollection(Reference<ClusterRecoveryData> self, bool exThrown);
ACTOR Future<Void> clusterRecoveryCore(Reference<ClusterRecoveryData> self);
bool isNormalClusterRecoveryError(const Error&);

#include "flow/unactorcompiler.h"

#endif
