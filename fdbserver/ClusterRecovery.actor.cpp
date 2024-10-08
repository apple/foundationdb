/*
 * ClusterRecovery.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbrpc/sim_validation.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/BackupProgress.actor.h"
#include "fdbserver/ClusterRecovery.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ProtocolVersion.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
EncryptionAtRestMode getEncryptionAtRest(DatabaseConfiguration config) {
	TraceEvent(SevDebug, "CREncryptionAtRestMode").detail("Mode", config.encryptionAtRestMode.toString());
	return config.encryptionAtRestMode;
}
} // namespace

static std::set<int> const& normalClusterRecoveryErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_operation_failed);
		s.insert(error_code_tlog_stopped);
		s.insert(error_code_tlog_failed);
		s.insert(error_code_commit_proxy_failed);
		s.insert(error_code_grv_proxy_failed);
		s.insert(error_code_resolver_failed);
		s.insert(error_code_backup_worker_failed);
		s.insert(error_code_recruitment_failed);
		s.insert(error_code_no_more_servers);
		s.insert(error_code_cluster_recovery_failed);
		s.insert(error_code_coordinated_state_conflict);
		s.insert(error_code_master_max_versions_in_flight);
		s.insert(error_code_worker_removed);
		s.insert(error_code_new_coordinators_timed_out);
		s.insert(error_code_broken_promise);
	}
	return s;
}

ACTOR Future<Void> recoveryTerminateOnConflict(UID dbgid,
                                               Promise<Void> fullyRecovered,
                                               Future<Void> onConflict,
                                               Future<Void> switchedState) {
	choose {
		when(wait(onConflict)) {
			if (!fullyRecovered.isSet()) {
				TraceEvent("RecoveryTerminated", dbgid).detail("Reason", "Conflict");
				CODE_PROBE(true, "Coordinated state conflict, recovery terminating");
				throw worker_removed();
			}
			return Void();
		}
		when(wait(switchedState)) {
			return Void();
		}
	}
}

ACTOR Future<Void> recruitNewMaster(ClusterControllerData* cluster,
                                    ClusterControllerData::DBInfo* db,
                                    MasterInterface* newMaster) {
	state Future<ErrorOr<MasterInterface>> fNewMaster;
	state WorkerFitnessInfo masterWorker;

	loop {
		// We must recruit the master in the same data center as the cluster controller.
		// This should always be possible, because we can recruit the master on the same process as the cluster
		// controller.
		std::map<Optional<Standalone<StringRef>>, int> id_used;
		id_used[cluster->clusterControllerProcessId]++;
		masterWorker = cluster->getWorkerForRoleInDatacenter(
		    cluster->clusterControllerDcId, ProcessClass::Master, ProcessClass::NeverAssign, db->config, id_used);
		if ((masterWorker.worker.processClass.machineClassFitness(ProcessClass::Master) >
		         SERVER_KNOBS->EXPECTED_MASTER_FITNESS ||
		     masterWorker.worker.interf.locality.processId() == cluster->clusterControllerProcessId) &&
		    !cluster->goodRecruitmentTime.isReady()) {
			TraceEvent("RecruitNewMaster", cluster->id)
			    .detail("Fitness", masterWorker.worker.processClass.machineClassFitness(ProcessClass::Master));
			wait(delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
			continue;
		}
		RecruitMasterRequest rmq;
		rmq.lifetime = db->serverInfo->get().masterLifetime;
		rmq.forceRecovery = db->forceRecovery;

		cluster->masterProcessId = masterWorker.worker.interf.locality.processId();
		cluster->db.unfinishedRecoveries++;
		fNewMaster = masterWorker.worker.interf.master.tryGetReply(rmq);
		wait(ready(fNewMaster) || db->forceMasterFailure.onTrigger());
		if (fNewMaster.isReady() && fNewMaster.get().present()) {
			TraceEvent("RecruitNewMaster", cluster->id).detail("Recruited", fNewMaster.get().get().id());

			// for status tool
			TraceEvent("RecruitedMasterWorker", cluster->id)
			    .detail("Address", fNewMaster.get().get().address())
			    .trackLatest(cluster->recruitedMasterWorkerEventHolder->trackingKey);

			*newMaster = fNewMaster.get().get();

			return Void();
		} else {
			CODE_PROBE(true, "clusterWatchDatabase() !newMaster.present()");
			wait(delay(SERVER_KNOBS->MASTER_SPIN_DELAY));
		}
	}
}

ACTOR Future<Void> clusterRecruitFromConfiguration(ClusterControllerData* self, Reference<RecruitWorkersInfo> req) {
	// At the moment this doesn't really need to be an actor (it always completes immediately)
	CODE_PROBE(true, "ClusterController RecruitTLogsRequest");
	loop {
		try {
			req->rep = self->findWorkersForConfiguration(req->req);
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && self->goodRecruitmentTime.isReady()) {
				self->outstandingRecruitmentRequests.push_back(req);
				TraceEvent(SevWarn, "RecruitFromConfigurationNotAvailable", self->id).error(e);
				wait(req->waitForCompletion.onTrigger());
				return Void();
			} else if (e.code() == error_code_operation_failed || e.code() == error_code_no_more_servers) {
				// recruitment not good enough, try again
				TraceEvent("RecruitFromConfigurationRetry", self->id)
				    .error(e)
				    .detail("GoodRecruitmentTimeReady", self->goodRecruitmentTime.isReady());
				while (!self->goodRecruitmentTime.isReady()) {
					wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
				}
			} else {
				TraceEvent(SevError, "RecruitFromConfigurationError", self->id).error(e);
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<RecruitRemoteFromConfigurationReply> clusterRecruitRemoteFromConfiguration(
    ClusterControllerData* self,
    Reference<RecruitRemoteWorkersInfo> req) {
	// At the moment this doesn't really need to be an actor (it always completes immediately)
	CODE_PROBE(true, "ClusterController RecruitTLogsRequest Remote");
	loop {
		try {
			auto rep = self->findRemoteWorkersForConfiguration(req->req);
			return rep;
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && self->goodRemoteRecruitmentTime.isReady()) {
				self->outstandingRemoteRecruitmentRequests.push_back(req);
				TraceEvent(SevWarn, "RecruitRemoteFromConfigurationNotAvailable", self->id).error(e);
				wait(req->waitForCompletion.onTrigger());
				return req->rep;
			} else if (e.code() == error_code_operation_failed || e.code() == error_code_no_more_servers) {
				// recruitment not good enough, try again
				TraceEvent("RecruitRemoteFromConfigurationRetry", self->id)
				    .error(e)
				    .detail("GoodRecruitmentTimeReady", self->goodRemoteRecruitmentTime.isReady());
				while (!self->goodRemoteRecruitmentTime.isReady()) {
					wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
				}
			} else {
				TraceEvent(SevError, "RecruitRemoteFromConfigurationError", self->id).error(e);
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> newCommitProxies(Reference<ClusterRecoveryData> self, RecruitFromConfigurationReply recr) {
	std::vector<Future<CommitProxyInterface>> initializationReplies;
	for (int i = 0; i < recr.commitProxies.size(); i++) {
		InitializeCommitProxyRequest req;
		req.master = self->masterInterface;
		req.masterLifetime = self->masterLifetime;
		req.recoveryCount = self->cstate.myDBState.recoveryCount + 1;
		req.recoveryTransactionVersion = self->recoveryTransactionVersion;
		req.firstProxy = i == 0;
		req.encryptMode = getEncryptionAtRest(self->configuration);
		req.commitProxyIndex = i;
		TraceEvent("CommitProxyReplies", self->dbgid)
		    .detail("WorkerID", recr.commitProxies[i].id())
		    .detail("RecoveryTxnVersion", self->recoveryTransactionVersion)
		    .detail("EncryptMode", req.encryptMode.toString())
		    .detail("FirstProxy", req.firstProxy ? "True" : "False")
		    .detail("CommitProxyIndex", req.commitProxyIndex);
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(recr.commitProxies[i].commitProxy.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    commit_proxy_failed()));
	}

	std::vector<CommitProxyInterface> newRecruits = wait(getAll(initializationReplies));
	TraceEvent("CommitProxyInitializationComplete", self->dbgid).log();
	// It is required for the correctness of COMMIT_ON_FIRST_PROXY that self->commitProxies[0] is the firstCommitProxy.
	self->commitProxies = newRecruits;

	return Void();
}

ACTOR Future<Void> newGrvProxies(Reference<ClusterRecoveryData> self, RecruitFromConfigurationReply recr) {
	std::vector<Future<GrvProxyInterface>> initializationReplies;
	for (int i = 0; i < recr.grvProxies.size(); i++) {
		InitializeGrvProxyRequest req;
		req.master = self->masterInterface;
		req.masterLifetime = self->masterLifetime;
		req.recoveryCount = self->cstate.myDBState.recoveryCount + 1;
		TraceEvent("GrvProxyReplies", self->dbgid).detail("WorkerID", recr.grvProxies[i].id());
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(recr.grvProxies[i].grvProxy.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    grv_proxy_failed()));
	}

	std::vector<GrvProxyInterface> newRecruits = wait(getAll(initializationReplies));
	TraceEvent("GrvProxyInitializationComplete", self->dbgid).log();
	self->grvProxies = newRecruits;
	return Void();
}

ACTOR Future<Void> newResolvers(Reference<ClusterRecoveryData> self, RecruitFromConfigurationReply recr) {
	std::vector<Future<ResolverInterface>> initializationReplies;
	for (int i = 0; i < recr.resolvers.size(); i++) {
		InitializeResolverRequest req;
		req.masterLifetime = self->masterLifetime;
		req.recoveryCount = self->cstate.myDBState.recoveryCount + 1;
		req.commitProxyCount = recr.commitProxies.size();
		req.resolverCount = recr.resolvers.size();
		req.encryptMode = getEncryptionAtRest(self->configuration);
		TraceEvent("ResolverReplies", self->dbgid).detail("WorkerID", recr.resolvers[i].id());
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(recr.resolvers[i].resolver.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    resolver_failed()));
	}

	std::vector<ResolverInterface> newRecruits = wait(getAll(initializationReplies));
	TraceEvent("ResolverInitializationComplete", self->dbgid).log();
	self->resolvers = newRecruits;

	return Void();
}

ACTOR Future<Void> newTLogServers(Reference<ClusterRecoveryData> self,
                                  RecruitFromConfigurationReply recr,
                                  Reference<ILogSystem> oldLogSystem,
                                  std::vector<Standalone<CommitTransactionRef>>* initialConfChanges) {
	if (self->configuration.usableRegions > 1) {
		state Optional<Key> remoteDcId = self->remoteDcIds.size() ? self->remoteDcIds[0] : Optional<Key>();
		if (!self->dcId_locality.count(recr.dcId)) {
			int8_t loc = self->getNextLocality();
			Standalone<CommitTransactionRef> tr;
			tr.set(tr.arena(), tagLocalityListKeyFor(recr.dcId), tagLocalityListValue(loc));
			initialConfChanges->push_back(tr);
			self->dcId_locality[recr.dcId] = loc;
			TraceEvent(SevWarn, "UnknownPrimaryDCID", self->dbgid).detail("PrimaryId", recr.dcId).detail("Loc", loc);
		}

		if (!self->dcId_locality.count(remoteDcId)) {
			int8_t loc = self->getNextLocality();
			Standalone<CommitTransactionRef> tr;
			tr.set(tr.arena(), tagLocalityListKeyFor(remoteDcId), tagLocalityListValue(loc));
			initialConfChanges->push_back(tr);
			self->dcId_locality[remoteDcId] = loc;
			TraceEvent(SevWarn, "UnknownRemoteDCID", self->dbgid).detail("RemoteId", remoteDcId).detail("Loc", loc);
		}

		std::vector<UID> exclusionWorkerIds;
		std::transform(recr.tLogs.begin(),
		               recr.tLogs.end(),
		               std::back_inserter(exclusionWorkerIds),
		               [](const WorkerInterface& in) { return in.id(); });
		std::transform(recr.satelliteTLogs.begin(),
		               recr.satelliteTLogs.end(),
		               std::back_inserter(exclusionWorkerIds),
		               [](const WorkerInterface& in) { return in.id(); });

		RecruitRemoteFromConfigurationRequest remoteRecruitReq(
		    self->configuration,
		    remoteDcId,
		    recr.tLogs.size() *
		        std::max<int>(1, self->configuration.desiredLogRouterCount / std::max<int>(1, recr.tLogs.size())),
		    exclusionWorkerIds);
		remoteRecruitReq.dbgId = self->dbgid;
		state Reference<RecruitRemoteWorkersInfo> recruitWorkersInfo =
		    makeReference<RecruitRemoteWorkersInfo>(remoteRecruitReq);
		recruitWorkersInfo->dbgId = self->dbgid;
		Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers =
		    clusterRecruitRemoteFromConfiguration(self->controllerData, recruitWorkersInfo);

		self->primaryLocality = self->dcId_locality[recr.dcId];
		self->logSystem = Reference<ILogSystem>(); // Cancels the actors in the previous log system.
		Reference<ILogSystem> newLogSystem = wait(oldLogSystem->newEpoch(recr,
		                                                                 fRemoteWorkers,
		                                                                 self->configuration,
		                                                                 self->cstate.myDBState.recoveryCount + 1,
		                                                                 self->recoveryTransactionVersion,
		                                                                 self->primaryLocality,
		                                                                 self->dcId_locality[remoteDcId],
		                                                                 self->allTags,
		                                                                 self->recruitmentStalled));
		self->logSystem = newLogSystem;
	} else {
		self->primaryLocality = tagLocalitySpecial;
		self->logSystem = Reference<ILogSystem>(); // Cancels the actors in the previous log system.
		Reference<ILogSystem> newLogSystem = wait(oldLogSystem->newEpoch(recr,
		                                                                 Never(),
		                                                                 self->configuration,
		                                                                 self->cstate.myDBState.recoveryCount + 1,
		                                                                 self->recoveryTransactionVersion,
		                                                                 self->primaryLocality,
		                                                                 tagLocalitySpecial,
		                                                                 self->allTags,
		                                                                 self->recruitmentStalled));
		self->logSystem = newLogSystem;
	}
	return Void();
}

ACTOR Future<Void> newSeedServers(Reference<ClusterRecoveryData> self,
                                  RecruitFromConfigurationReply recruits,
                                  std::vector<StorageServerInterface>* servers) {
	// This is only necessary if the database is at version 0
	servers->clear();
	if (self->lastEpochEnd)
		return Void();

	state int idx = 0;
	state std::map<Optional<Value>, Tag> dcId_tags;
	state int8_t nextLocality = 0;
	while (idx < recruits.storageServers.size()) {
		TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_SS_RECRUITMENT_EVENT_NAME).c_str(),
		           self->dbgid)
		    .detail("CandidateWorker", recruits.storageServers[idx].locality.toString());

		InitializeStorageRequest isr;
		isr.seedTag = dcId_tags.count(recruits.storageServers[idx].locality.dcId())
		                  ? dcId_tags[recruits.storageServers[idx].locality.dcId()]
		                  : Tag(nextLocality, 0);
		isr.storeType = self->configuration.storageServerStoreType;
		isr.reqId = deterministicRandom()->randomUniqueID();
		isr.interfaceId = deterministicRandom()->randomUniqueID();
		isr.initialClusterVersion = self->recoveryTransactionVersion;
		isr.encryptMode = self->configuration.encryptionAtRestMode;

		ErrorOr<InitializeStorageReply> newServer = wait(recruits.storageServers[idx].storage.tryGetReply(isr));

		if (newServer.isError()) {
			if (!newServer.isError(error_code_recruitment_failed) &&
			    !newServer.isError(error_code_request_maybe_delivered))
				throw newServer.getError();

			CODE_PROBE(true, "initial storage recuitment loop failed to get new server");
			wait(delay(SERVER_KNOBS->STORAGE_RECRUITMENT_DELAY));
		} else {
			if (!dcId_tags.count(recruits.storageServers[idx].locality.dcId())) {
				dcId_tags[recruits.storageServers[idx].locality.dcId()] = Tag(nextLocality, 0);
				nextLocality++;
			}

			Tag& tag = dcId_tags[recruits.storageServers[idx].locality.dcId()];
			tag.id++;
			idx++;

			servers->push_back(newServer.get().interf);
		}
	}

	self->dcId_locality.clear();
	for (auto& it : dcId_tags) {
		self->dcId_locality[it.first] = it.second.locality;
	}

	TraceEvent("ClusterRecoveryRecruitedInitialStorageServers", self->dbgid)
	    .detail("TargetCount", self->configuration.storageTeamSize)
	    .detail("Servers", describe(*servers));

	return Void();
}

Future<Void> waitCommitProxyFailure(std::vector<CommitProxyInterface> const& commitProxies) {
	std::vector<Future<Void>> failed;
	failed.reserve(commitProxies.size());
	for (auto commitProxy : commitProxies) {
		failed.push_back(waitFailureClient(commitProxy.waitFailure,
		                                   SERVER_KNOBS->TLOG_TIMEOUT,
		                                   -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
		                                   /*trace=*/true));
	}
	ASSERT(failed.size() >= 1);
	return tagError<Void>(quorum(failed, 1), commit_proxy_failed());
}

Future<Void> waitGrvProxyFailure(std::vector<GrvProxyInterface> const& grvProxies) {
	std::vector<Future<Void>> failed;
	failed.reserve(grvProxies.size());
	for (int i = 0; i < grvProxies.size(); i++)
		failed.push_back(waitFailureClient(grvProxies[i].waitFailure,
		                                   SERVER_KNOBS->TLOG_TIMEOUT,
		                                   -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
		                                   /*trace=*/true));
	ASSERT(failed.size() >= 1);
	return tagError<Void>(quorum(failed, 1), grv_proxy_failed());
}

Future<Void> waitResolverFailure(std::vector<ResolverInterface> const& resolvers) {
	std::vector<Future<Void>> failed;
	failed.reserve(resolvers.size());
	for (auto resolver : resolvers) {
		failed.push_back(waitFailureClient(resolver.waitFailure,
		                                   SERVER_KNOBS->TLOG_TIMEOUT,
		                                   -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
		                                   /*trace=*/true));
	}
	ASSERT(failed.size() >= 1);
	return tagError<Void>(quorum(failed, 1), resolver_failed());
}

ACTOR Future<Void> rejoinRequestHandler(Reference<ClusterRecoveryData> self) {
	loop {
		TLogRejoinRequest req = waitNext(self->clusterController.tlogRejoin.getFuture());
		TraceEvent(SevDebug, "TLogRejoinRequestHandler")
		    .detail("MasterLifeTime", self->dbInfo->get().masterLifetime.toString());
		req.reply.send(true);
	}
}

// Keeps the coordinated state (cstate) updated as the set of recruited tlogs change through recovery.
ACTOR Future<Void> trackTlogRecovery(Reference<ClusterRecoveryData> self,
                                     Reference<AsyncVar<Reference<ILogSystem>>> oldLogSystems,
                                     Future<Void> minRecoveryDuration) {
	state Future<Void> rejoinRequests = Never();
	state DBRecoveryCount recoverCount = self->cstate.myDBState.recoveryCount + 1;
	state EncryptionAtRestMode encryptionAtRestMode = getEncryptionAtRest(self->configuration);
	state DatabaseConfiguration configuration =
	    self->configuration; // self-configuration can be changed by configurationMonitor so we need a copy
	loop {
		state DBCoreState newState;
		self->logSystem->purgeOldRecoveredGenerations();
		self->logSystem->toCoreState(newState);
		newState.recoveryCount = recoverCount;

		// Update Coordinators EncryptionAtRest status during the very first recovery of the cluster (empty database)
		newState.encryptionAtRestMode = encryptionAtRestMode;

		state Future<Void> changed = self->logSystem->onCoreStateChanged();

		ASSERT(newState.tLogs[0].tLogWriteAntiQuorum == configuration.tLogWriteAntiQuorum &&
		       newState.tLogs[0].tLogReplicationFactor == configuration.tLogReplicationFactor);

		state bool allLogs =
		    newState.tLogs.size() ==
		    configuration.expectedLogSets(self->primaryDcId.size() ? self->primaryDcId[0] : Optional<Key>());
		state bool finalUpdate = !newState.oldTLogData.size() && allLogs;
		TraceEvent("TrackTlogRecovery")
		    .detail("FinalUpdate", finalUpdate)
		    .detail("NewState.tlogs", newState.tLogs.size())
		    .detail("NewState.OldTLogs", newState.oldTLogData.size())
		    .detail("NewState.EncryptionAtRestMode", newState.encryptionAtRestMode.toString())
		    .detail("Expected.tlogs",
		            configuration.expectedLogSets(self->primaryDcId.size() ? self->primaryDcId[0] : Optional<Key>()))
		    .detail("RecoveryCount", newState.recoveryCount);
		wait(self->cstate.write(newState, finalUpdate));
		if (self->cstateUpdated.canBeSet()) {
			self->cstateUpdated.send(Void());
		}

		wait(minRecoveryDuration);
		self->logSystem->coreStateWritten(newState);

		if (self->recoveryReadyForCommits.canBeSet()) {
			self->recoveryReadyForCommits.send(Void());
		}

		if (finalUpdate) {
			self->recoveryState = RecoveryState::FULLY_RECOVERED;
			TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
			           self->dbgid)
			    .detail("StatusCode", RecoveryStatus::fully_recovered)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::fully_recovered])
			    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);

			TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_GENERATION_EVENT_NAME).c_str(),
			           self->dbgid)
			    .detail("ActiveGenerations", 1)
			    .trackLatest(self->clusterRecoveryGenerationsEventHolder->trackingKey);
		} else if (!newState.oldTLogData.size() && self->recoveryState < RecoveryState::STORAGE_RECOVERED) {
			self->recoveryState = RecoveryState::STORAGE_RECOVERED;
			TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
			           self->dbgid)
			    .detail("StatusCode", RecoveryStatus::storage_recovered)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::storage_recovered])
			    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);
		} else if (allLogs && self->recoveryState < RecoveryState::ALL_LOGS_RECRUITED) {
			self->recoveryState = RecoveryState::ALL_LOGS_RECRUITED;
			TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
			           self->dbgid)
			    .detail("StatusCode", RecoveryStatus::all_logs_recruited)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::all_logs_recruited])
			    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);
		}

		self->registrationTrigger.trigger();

		if (finalUpdate) {
			oldLogSystems->get()->stopRejoins();
			rejoinRequests = rejoinRequestHandler(self);
			return Void();
		}

		wait(changed);
	}
}

ACTOR Future<Void> changeCoordinators(Reference<ClusterRecoveryData> self) {
	loop {
		ChangeCoordinatorsRequest req = waitNext(self->clusterController.changeCoordinators.getFuture());
		TraceEvent("ChangeCoordinators", self->dbgid).log();
		++self->changeCoordinatorsRequests;
		state ChangeCoordinatorsRequest changeCoordinatorsRequest = req;
		if (self->masterInterface.id() != changeCoordinatorsRequest.masterId) {
			// Make sure the request is coming from a proxy from the same
			// generation. If not, throw coordinators_changed - this is OK
			// because the client will still receive commit_unknown_result, and
			// will retry the request. This check is necessary because
			// otherwise in rare circumstances where a recovery occurs between
			// the change coordinators request from the client and the cstate
			// actually being moved, the client may think the change
			// coordinators command failed when it is still in progress. So we
			// preempt the issue here and force failure if the generations
			// don't match.
			throw coordinators_changed();
		}

		// Kill cluster controller to facilitate coordinator registration update
		if (self->controllerData->shouldCommitSuicide) {
			throw restart_cluster_controller();
		}
		self->controllerData->shouldCommitSuicide = true;

		while (!self->cstate.previousWrite.isReady()) {
			wait(self->cstate.previousWrite);
			wait(delay(
			    0)); // if a new core state is ready to be written, have that take priority over our finalizing write;
		}

		if (!self->cstate.fullyRecovered.isSet()) {
			wait(self->cstate.write(self->cstate.myDBState, true));
		}

		try {
			ClusterConnectionString conn(changeCoordinatorsRequest.newConnectionString.toString());
			wait(self->cstate.move(conn));
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				changeCoordinatorsRequest.reply.sendError(e);

			throw;
		}

		throw internal_error();
	}
}

ACTOR Future<Void> configurationMonitor(Reference<ClusterRecoveryData> self, Database cx) {
	loop {
		state ReadYourWritesTransaction tr(cx);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				RangeResult results = wait(tr.getRange(configKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!results.more && results.size() < CLIENT_KNOBS->TOO_MANY);

				DatabaseConfiguration conf;
				conf.fromKeyValues((VectorRef<KeyValueRef>)results);
				TraceEvent("ConfigurationMonitor", self->dbgid)
				    .detail(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
				            self->recoveryState);
				if (conf != self->configuration) {
					if (self->recoveryState != RecoveryState::ALL_LOGS_RECRUITED &&
					    self->recoveryState != RecoveryState::FULLY_RECOVERED) {
						self->controllerData->shouldCommitSuicide = true;
						throw restart_cluster_controller();
					}

					self->configuration = conf;
					self->registrationTrigger.trigger();
				}

				state Future<Void> watchFuture =
				    tr.watch(moveKeysLockOwnerKey) || tr.watch(excludedServersVersionKey) ||
				    tr.watch(failedServersVersionKey) || tr.watch(excludedLocalityVersionKey) ||
				    tr.watch(failedLocalityVersionKey);
				wait(tr.commit());
				wait(watchFuture);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR static Future<Optional<Version>> getMinBackupVersion(Reference<ClusterRecoveryData> self, Database cx) {
	loop {
		state ReadYourWritesTransaction tr(cx);

		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> value = wait(tr.get(backupStartedKey));
			Optional<Version> minVersion;
			if (value.present()) {
				auto uidVersions = decodeBackupStartedValue(value.get());
				TraceEvent e("GotBackupStartKey", self->dbgid);
				int i = 1;
				for (auto [uid, version] : uidVersions) {
					e.detail(format("BackupID%d", i), uid).detail(format("Version%d", i), version);
					i++;
					minVersion = minVersion.present() ? std::min(version, minVersion.get()) : version;
				}
			} else {
				TraceEvent("EmptyBackupStartKey", self->dbgid).log();
			}
			return minVersion;

		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR static Future<Void> recruitBackupWorkers(Reference<ClusterRecoveryData> self, Database cx) {
	ASSERT(self->backupWorkers.size() > 0);

	// Avoid race between a backup worker's save progress and the reads below.
	wait(delay(SERVER_KNOBS->SECONDS_BEFORE_RECRUIT_BACKUP_WORKER));

	state LogEpoch epoch = self->cstate.myDBState.recoveryCount;
	state Reference<BackupProgress> backupProgress(
	    new BackupProgress(self->dbgid, self->logSystem->getOldEpochTagsVersionsInfo()));
	state Future<Void> gotProgress = getBackupProgress(cx, self->dbgid, backupProgress, /*logging=*/true);
	state std::vector<Future<InitializeBackupReply>> initializationReplies;

	state std::vector<std::pair<UID, Tag>> idsTags; // worker IDs and tags for current epoch
	state int logRouterTags = self->logSystem->getLogRouterTags();
	idsTags.reserve(logRouterTags);
	for (int i = 0; i < logRouterTags; i++) {
		idsTags.emplace_back(deterministicRandom()->randomUniqueID(), Tag(tagLocalityLogRouter, i));
	}

	const Version startVersion = self->logSystem->getBackupStartVersion();
	state int i = 0;
	for (; i < logRouterTags; i++) {
		const auto& worker = self->backupWorkers[i % self->backupWorkers.size()];
		InitializeBackupRequest req(idsTags[i].first);
		req.recruitedEpoch = epoch;
		req.backupEpoch = epoch;
		req.routerTag = idsTags[i].second;
		req.totalTags = logRouterTags;
		req.startVersion = startVersion;
		TraceEvent("BackupRecruitment", self->dbgid)
		    .detail("RequestID", req.reqId)
		    .detail("Tag", req.routerTag.toString())
		    .detail("Epoch", epoch)
		    .detail("BackupEpoch", epoch)
		    .detail("StartVersion", req.startVersion);
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(worker.backup.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->BACKUP_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    backup_worker_failed()));
	}

	state Future<Optional<Version>> fMinVersion = getMinBackupVersion(self, cx);
	wait(gotProgress && success(fMinVersion));
	TraceEvent("MinBackupVersion", self->dbgid).detail("Version", fMinVersion.get().present() ? fMinVersion.get() : -1);

	std::map<std::tuple<LogEpoch, Version, int>, std::map<Tag, Version>> toRecruit =
	    backupProgress->getUnfinishedBackup();
	for (const auto& [epochVersionTags, tagVersions] : toRecruit) {
		const Version oldEpochEnd = std::get<1>(epochVersionTags);
		if (!fMinVersion.get().present() || fMinVersion.get().get() + 1 >= oldEpochEnd) {
			TraceEvent("SkipBackupRecruitment", self->dbgid)
			    .detail("MinVersion", fMinVersion.get().present() ? fMinVersion.get() : -1)
			    .detail("Epoch", epoch)
			    .detail("OldEpoch", std::get<0>(epochVersionTags))
			    .detail("OldEpochEnd", oldEpochEnd);
			continue;
		}
		for (const auto& [tag, version] : tagVersions) {
			const auto& worker = self->backupWorkers[i % self->backupWorkers.size()];
			i++;
			InitializeBackupRequest req(deterministicRandom()->randomUniqueID());
			req.recruitedEpoch = epoch;
			req.backupEpoch = std::get<0>(epochVersionTags);
			req.routerTag = tag;
			req.totalTags = std::get<2>(epochVersionTags);
			req.startVersion = version; // savedVersion + 1
			req.endVersion = std::get<1>(epochVersionTags) - 1;
			TraceEvent("BackupRecruitment", self->dbgid)
			    .detail("RequestID", req.reqId)
			    .detail("Tag", req.routerTag.toString())
			    .detail("Epoch", epoch)
			    .detail("BackupEpoch", req.backupEpoch)
			    .detail("StartVersion", req.startVersion)
			    .detail("EndVersion", req.endVersion.get());
			initializationReplies.push_back(transformErrors(
			    throwErrorOr(worker.backup.getReplyUnlessFailedFor(
			        req, SERVER_KNOBS->BACKUP_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    backup_worker_failed()));
		}
	}

	std::vector<InitializeBackupReply> newRecruits = wait(getAll(initializationReplies));
	self->logSystem->setBackupWorkers(newRecruits);
	TraceEvent("BackupRecruitmentDone", self->dbgid).log();
	self->registrationTrigger.trigger();
	return Void();
}

ACTOR Future<Void> updateLogsValue(Reference<ClusterRecoveryData> self, Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			Optional<Standalone<StringRef>> value = wait(tr.get(logsKey));
			ASSERT(value.present());
			auto logs = decodeLogsValue(value.get());

			std::set<UID> logIds;
			for (auto& log : logs.first) {
				logIds.insert(log.first);
			}

			bool found = false;
			for (auto& logSet : self->logSystem->getLogSystemConfig().tLogs) {
				for (auto& log : logSet.tLogs) {
					if (logIds.count(log.id())) {
						found = true;
						break;
					}
				}
				if (found) {
					break;
				}
			}

			if (!found) {
				CODE_PROBE(true, "old master attempted to change logsKey", probe::decoration::rare);
				return Void();
			}

			tr.set(logsKey, self->logSystem->getLogsValue());
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// TODO(ahusain): ClusterController orchestrating recovery, self message can be avoided.
Future<Void> sendMasterRegistration(ClusterRecoveryData* self,
                                    LogSystemConfig const& logSystemConfig,
                                    std::vector<CommitProxyInterface> commitProxies,
                                    std::vector<GrvProxyInterface> grvProxies,
                                    std::vector<ResolverInterface> resolvers,
                                    DBRecoveryCount recoveryCount,
                                    std::vector<UID> priorCommittedLogServers) {
	RegisterMasterRequest masterReq;
	masterReq.id = self->masterInterface.id();
	masterReq.mi = self->masterInterface.locality;
	masterReq.logSystemConfig = logSystemConfig;
	masterReq.commitProxies = commitProxies;
	masterReq.grvProxies = grvProxies;
	masterReq.resolvers = resolvers;
	masterReq.recoveryCount = recoveryCount;
	if (self->hasConfiguration)
		masterReq.configuration = self->configuration;
	masterReq.registrationCount = ++self->registrationCount;
	masterReq.priorCommittedLogServers = priorCommittedLogServers;
	masterReq.recoveryState = self->recoveryState;
	masterReq.recoveryStalled = self->recruitmentStalled->get();
	return brokenPromiseToNever(self->clusterController.registerMaster.getReply(masterReq));
}

ACTOR Future<Void> updateRegistration(Reference<ClusterRecoveryData> self, Reference<ILogSystem> logSystem) {
	state Database cx = openDBOnServer(self->dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	state Future<Void> trigger = self->registrationTrigger.onTrigger();
	state Future<Void> updateLogsKey;

	loop {
		wait(trigger);
		wait(delay(.001)); // Coalesce multiple changes

		trigger = self->registrationTrigger.onTrigger();

		auto logSystemConfig = logSystem->getLogSystemConfig();
		TraceEvent("UpdateRegistration", self->dbgid)
		    .detail("RecoveryCount", self->cstate.myDBState.recoveryCount)
		    .detail("OldestBackupEpoch", logSystemConfig.oldestBackupEpoch)
		    .detail("Logs", describe(logSystemConfig.tLogs))
		    .detail("OldGenerations", logSystemConfig.oldTLogs.size())
		    .detail("CStateUpdated", self->cstateUpdated.isSet())
		    .detail("RecoveryTxnVersion", self->recoveryTransactionVersion)
		    .detail("LastEpochEnd", self->lastEpochEnd);

		if (!self->cstateUpdated.isSet()) {
			wait(sendMasterRegistration(self.getPtr(),
			                            logSystemConfig,
			                            self->provisionalCommitProxies,
			                            self->provisionalGrvProxies,
			                            self->resolvers,
			                            self->cstate.myDBState.recoveryCount,
			                            self->cstate.prevDBState.getPriorCommittedLogServers()));

		} else if (self->recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
			updateLogsKey = updateLogsValue(self, cx);
			wait(sendMasterRegistration(self.getPtr(),
			                            logSystemConfig,
			                            self->commitProxies,
			                            self->grvProxies,
			                            self->resolvers,
			                            self->cstate.myDBState.recoveryCount,
			                            std::vector<UID>()));
		} else {
			// The cluster should enter the accepting commits phase soon, and then we will register again
			CODE_PROBE(true, "cstate is updated but we aren't accepting commits yet", probe::decoration::rare);
		}
	}
}

ACTOR Future<Standalone<CommitTransactionRef>> provisionalMaster(Reference<ClusterRecoveryData> parent,
                                                                 Future<Void> activate) {
	wait(activate);

	// Register a fake commit proxy (to be provided right here) to make ourselves available to clients
	parent->provisionalCommitProxies = std::vector<CommitProxyInterface>(1);
	parent->provisionalCommitProxies[0].provisional = true;
	parent->provisionalCommitProxies[0].initEndpoints();
	parent->provisionalGrvProxies = std::vector<GrvProxyInterface>(1);
	parent->provisionalGrvProxies[0].provisional = true;
	parent->provisionalGrvProxies[0].initEndpoints();
	state Future<Void> waitCommitProxyFailure =
	    waitFailureServer(parent->provisionalCommitProxies[0].waitFailure.getFuture());
	state Future<Void> waitGrvProxyFailure =
	    waitFailureServer(parent->provisionalGrvProxies[0].waitFailure.getFuture());
	parent->registrationTrigger.trigger();

	auto lockedKey = parent->txnStateStore->readValue(databaseLockedKey).get();
	state bool locked = lockedKey.present() && lockedKey.get().size();

	state Optional<Value> metadataVersion = parent->txnStateStore->readValue(metadataVersionKey).get();

	// We respond to a minimal subset of the commit proxy protocol.  Our sole purpose is to receive a single write-only
	// transaction which might repair our configuration, and return it.
	loop choose {
		when(GetReadVersionRequest req =
		         waitNext(parent->provisionalGrvProxies[0].getConsistentReadVersion.getFuture())) {
			if ((req.flags & GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY) &&
			    (req.flags & GetReadVersionRequest::FLAG_USE_PROVISIONAL_PROXIES) && parent->lastEpochEnd) {
				GetReadVersionReply rep;
				rep.version = parent->lastEpochEnd;
				rep.locked = locked;
				rep.metadataVersion = metadataVersion;
				rep.proxyId = parent->provisionalGrvProxies[0].id();
				req.reply.send(rep);
			} else
				req.reply.send(Never()); // We can't perform causally consistent reads without recovering
		}
		when(CommitTransactionRequest req = waitNext(parent->provisionalCommitProxies[0].commit.getFuture())) {
			req.reply.send(Never()); // don't reply (clients always get commit_unknown_result)
			auto t = &req.transaction;
			if (t->read_snapshot == parent->lastEpochEnd && //< So no transactions can fall between the read snapshot
			                                                // and the recovery transaction this (might) be merged with
			                                                // vvv and also the changes we will make in the recovery
			                                                // transaction (most notably to lastEpochEndKey) BEFORE we
			                                                // merge initialConfChanges won't conflict
			    !std::any_of(t->read_conflict_ranges.begin(), t->read_conflict_ranges.end(), [](KeyRangeRef const& r) {
				    return r.contains(lastEpochEndKey);
			    })) {
				for (auto m = t->mutations.begin(); m != t->mutations.end(); ++m) {
					TraceEvent("PM_CTM", parent->dbgid)
					    .detail("MType", m->type)
					    .detail("Param1", m->param1)
					    .detail("Param2", m->param2);
					// emergency transaction only mean to do configuration change
					if (parent->configuration.involveMutation(*m)) {
						// We keep the mutations and write conflict ranges from this transaction, but not its read
						// conflict ranges
						Standalone<CommitTransactionRef> out;
						out.read_snapshot = invalidVersion;
						out.mutations.append_deep(out.arena(), t->mutations.begin(), t->mutations.size());
						out.write_conflict_ranges.append_deep(
						    out.arena(), t->write_conflict_ranges.begin(), t->write_conflict_ranges.size());
						return out;
					}
				}
			}
		}
		when(GetKeyServerLocationsRequest req =
		         waitNext(parent->provisionalCommitProxies[0].getKeyServersLocations.getFuture())) {
			req.reply.send(Never());
		}
		when(GetBlobGranuleLocationsRequest req =
		         waitNext(parent->provisionalCommitProxies[0].getBlobGranuleLocations.getFuture())) {
			req.reply.send(Never());
		}
		when(wait(waitCommitProxyFailure)) {
			throw worker_removed();
		}
		when(wait(waitGrvProxyFailure)) {
			throw worker_removed();
		}
	}
}

ACTOR Future<std::vector<Standalone<CommitTransactionRef>>> recruitEverything(
    Reference<ClusterRecoveryData> self,
    std::vector<StorageServerInterface>* seedServers,
    Reference<ILogSystem> oldLogSystem) {
	if (!self->configuration.isValid()) {
		RecoveryStatus::RecoveryStatus status;
		if (self->configuration.initialized) {
			TraceEvent(
			    SevWarn,
			    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_INVALID_CONFIG_EVENT_NAME).c_str(),
			    self->dbgid)
			    .setMaxEventLength(11000)
			    .setMaxFieldLength(10000)
			    .detail("Conf", self->configuration.toString());
			status = RecoveryStatus::configuration_invalid;
		} else if (!self->cstate.prevDBState.tLogs.size()) {
			status = RecoveryStatus::configuration_never_created;
			self->neverCreated = true;
		} else {
			status = RecoveryStatus::configuration_missing;
		}
		TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
		           self->dbgid)
		    .detail("StatusCode", status)
		    .detail("Status", RecoveryStatus::names[status])
		    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);
		return Never();
	} else {
		TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
		           self->dbgid)
		    .setMaxFieldLength(-1)
		    .detail("StatusCode", RecoveryStatus::recruiting_transaction_servers)
		    .detail("Status", RecoveryStatus::names[RecoveryStatus::recruiting_transaction_servers])
		    .detail("Conf", self->configuration.toString())
		    .detail("RequiredCommitProxies", 1)
		    .detail("RequiredGrvProxies", 1)
		    .detail("RequiredResolvers", 1)
		    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);
		// The cluster's EncryptionAtRest status is now readable.
		if (self->controllerData->encryptionAtRestMode.canBeSet()) {
			self->controllerData->encryptionAtRestMode.send(getEncryptionAtRest(self->configuration));
		}
	}

	// FIXME: we only need log routers for the same locality as the master
	int maxLogRouters = self->cstate.prevDBState.logRouterTags;
	for (auto& old : self->cstate.prevDBState.oldTLogData) {
		maxLogRouters = std::max(maxLogRouters, old.logRouterTags);
	}

	RecruitFromConfigurationRequest recruitReq(self->configuration, self->lastEpochEnd == 0, maxLogRouters);
	state Reference<RecruitWorkersInfo> recruitWorkersInfo = makeReference<RecruitWorkersInfo>(recruitReq);
	recruitWorkersInfo->dbgId = self->dbgid;
	wait(clusterRecruitFromConfiguration(self->controllerData, recruitWorkersInfo));
	state RecruitFromConfigurationReply recruits = recruitWorkersInfo->rep;

	std::string primaryDcIds, remoteDcIds;

	self->primaryDcId.clear();
	self->remoteDcIds.clear();
	if (recruits.dcId.present()) {
		self->primaryDcId.push_back(recruits.dcId);
		if (!primaryDcIds.empty()) {
			primaryDcIds += ',';
		}
		primaryDcIds += printable(recruits.dcId);
		if (self->configuration.regions.size() > 1) {
			Key remoteDcId = recruits.dcId.get() == self->configuration.regions[0].dcId
			                     ? self->configuration.regions[1].dcId
			                     : self->configuration.regions[0].dcId;
			self->remoteDcIds.push_back(remoteDcId);
			if (!remoteDcIds.empty()) {
				remoteDcIds += ',';
			}
			remoteDcIds += printable(remoteDcId);
		}
	}
	self->backupWorkers.swap(recruits.backupWorkers);

	// Store recruitment result, which may be used to check the current being recruited transaction system in gray
	// failure detection.
	self->primaryRecruitment = recruits;

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::initializing_transaction_servers)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::initializing_transaction_servers])
	    .detail("CommitProxies", recruits.commitProxies.size())
	    .detail("GrvProxies", recruits.grvProxies.size())
	    .detail("TLogs", recruits.tLogs.size())
	    .detail("Resolvers", recruits.resolvers.size())
	    .detail("SatelliteTLogs", recruits.satelliteTLogs.size())
	    .detail("OldLogRouters", recruits.oldLogRouters.size())
	    .detail("StorageServers", recruits.storageServers.size())
	    .detail("BackupWorkers", self->backupWorkers.size())
	    .detail("PrimaryDcIds", primaryDcIds)
	    .detail("RemoteDcIds", remoteDcIds)
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);

	// Actually, newSeedServers does both the recruiting and initialization of the seed servers; so if this is a brand
	// new database we are sort of lying that we are past the recruitment phase.  In a perfect world we would split that
	// up so that the recruitment part happens above (in parallel with recruiting the transaction servers?).
	wait(newSeedServers(self, recruits, seedServers));
	state std::vector<Standalone<CommitTransactionRef>> confChanges;
	wait(newCommitProxies(self, recruits) && newGrvProxies(self, recruits) && newResolvers(self, recruits) &&
	     newTLogServers(self, recruits, oldLogSystem, &confChanges));

	// Update recovery related information to the newly elected sequencer (master) process.
	wait(brokenPromiseToNever(
	    self->masterInterface.updateRecoveryData.getReply(UpdateRecoveryDataRequest(self->recoveryTransactionVersion,
	                                                                                self->lastEpochEnd,
	                                                                                self->commitProxies,
	                                                                                self->resolvers,
	                                                                                self->versionEpoch,
	                                                                                self->primaryLocality))));

	return confChanges;
}

ACTOR Future<Void> updateLocalityForDcId(Optional<Key> dcId,
                                         Reference<ILogSystem> oldLogSystem,
                                         Reference<AsyncVar<PeekTxsInfo>> locality) {
	loop {
		std::pair<int8_t, int8_t> loc = oldLogSystem->getLogSystemConfig().getLocalityForDcId(dcId);
		Version ver = locality->get().knownCommittedVersion;
		if (ver == invalidVersion) {
			ver = oldLogSystem->getKnownCommittedVersion();
		}

		locality->set(PeekTxsInfo(loc.first, loc.second, ver));
		TraceEvent("UpdatedLocalityForDcId")
		    .detail("DcId", dcId)
		    .detail("Locality0", loc.first)
		    .detail("Locality1", loc.second)
		    .detail("Version", ver);
		wait(oldLogSystem->onLogSystemConfigChange() || oldLogSystem->onKnownCommittedVersionChange());
	}
}

ACTOR Future<Void> readTransactionSystemState(Reference<ClusterRecoveryData> self,
                                              Reference<ILogSystem> oldLogSystem,
                                              Version txsPoppedVersion) {
	state Reference<AsyncVar<PeekTxsInfo>> myLocality = Reference<AsyncVar<PeekTxsInfo>>(
	    new AsyncVar<PeekTxsInfo>(PeekTxsInfo(tagLocalityInvalid, tagLocalityInvalid, invalidVersion)));
	state Future<Void> localityUpdater =
	    updateLocalityForDcId(self->masterInterface.locality.dcId(), oldLogSystem, myLocality);
	// Peek the txnStateTag in oldLogSystem and recover self->txnStateStore

	// For now, we also obtain the recovery metadata that the log system obtained during the end_epoch process for
	// comparison

	// Sets self->lastEpochEnd and self->recoveryTransactionVersion
	// Sets self->configuration to the configuration (FF/conf/ keys) at self->lastEpochEnd

	// Recover transaction state store
	// If it's the first recovery the encrypt mode is not yet available so create the txn state store with encryption
	// disabled. This is fine since we will not write any data to disk using this txn store.
	state bool enableEncryptionForTxnStateStore = false;
	if (self->controllerData->encryptionAtRestMode.getFuture().isReady()) {
		EncryptionAtRestMode encryptMode = wait(self->controllerData->encryptionAtRestMode.getFuture());
		enableEncryptionForTxnStateStore = encryptMode.isEncryptionEnabled();
	}
	CODE_PROBE(enableEncryptionForTxnStateStore, "Enable encryption for txnStateStore", probe::decoration::rare);
	if (self->txnStateStore)
		self->txnStateStore->close();
	self->txnStateLogAdapter = openDiskQueueAdapter(oldLogSystem, myLocality, txsPoppedVersion);
	self->txnStateStore = keyValueStoreLogSystem(self->txnStateLogAdapter,
	                                             self->dbInfo,
	                                             self->dbgid,
	                                             self->memoryLimit,
	                                             false,
	                                             false,
	                                             true,
	                                             enableEncryptionForTxnStateStore);

	// Version 0 occurs at the version epoch. The version epoch is the number
	// of microseconds since the Unix epoch. It can be set through fdbcli.
	self->versionEpoch.reset();
	Optional<Standalone<StringRef>> versionEpochValue = wait(self->txnStateStore->readValue(versionEpochKey));
	if (versionEpochValue.present()) {
		self->versionEpoch = BinaryReader::fromStringRef<int64_t>(versionEpochValue.get(), Unversioned());
	}

	// Versionstamped operations (particularly those applied from DR) define a minimum commit version
	// that we may recover to, as they embed the version in user-readable data and require that no
	// transactions will be committed at a lower version.
	Optional<Standalone<StringRef>> requiredCommitVersion =
	    wait(self->txnStateStore->readValue(minRequiredCommitVersionKey));
	Version minRequiredCommitVersion = -1;
	if (requiredCommitVersion.present()) {
		minRequiredCommitVersion = BinaryReader::fromStringRef<Version>(requiredCommitVersion.get(), Unversioned());
	}
	if (g_network->isSimulated() && self->versionEpoch.present()) {
		minRequiredCommitVersion = std::max(
		    minRequiredCommitVersion,
		    static_cast<Version>(g_network->timer() * SERVER_KNOBS->VERSIONS_PER_SECOND - self->versionEpoch.get()));
	}

	// Recover version info
	self->lastEpochEnd = oldLogSystem->getEnd() - 1;
	if (self->lastEpochEnd == 0) {
		self->recoveryTransactionVersion = 1;
	} else {
		if (self->forceRecovery) {
			self->recoveryTransactionVersion = self->lastEpochEnd + SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT_FORCED;
		} else {
			self->recoveryTransactionVersion = self->lastEpochEnd + SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT;
		}

		if (self->recoveryTransactionVersion < minRequiredCommitVersion)
			self->recoveryTransactionVersion = minRequiredCommitVersion;

		// Test randomly increasing the recovery version by a large number.
		// When the version epoch is enabled, versions stay in sync with time.
		// An offline cluster could see a large version jump when it comes back
		// online, so test this behavior in simulation.
		if (BUGGIFY) {
			self->recoveryTransactionVersion += deterministicRandom()->randomInt64(0, 10000000);
		}
	}

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_RECOVERING_EVENT_NAME).c_str(),
	           self->dbgid)
	    .detail("LastEpochEnd", self->lastEpochEnd)
	    .detail("RecoveryTransactionVersion", self->recoveryTransactionVersion);

	RangeResult rawConf = wait(self->txnStateStore->readRange(configKeys));
	self->configuration.fromKeyValues(rawConf.castTo<VectorRef<KeyValueRef>>());
	self->originalConfiguration = self->configuration;
	self->hasConfiguration = true;

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_RECOVERED_EVENT_NAME).c_str(),
	           self->dbgid)
	    .setMaxEventLength(11000)
	    .setMaxFieldLength(10000)
	    .detail("Conf", self->configuration.toString())
	    .trackLatest(self->recoveredConfigEventHolder->trackingKey);

	RangeResult rawLocalities = wait(self->txnStateStore->readRange(tagLocalityListKeys));
	self->dcId_locality.clear();
	for (auto& kv : rawLocalities) {
		self->dcId_locality[decodeTagLocalityListKey(kv.key)] = decodeTagLocalityListValue(kv.value);
	}

	RangeResult rawTags = wait(self->txnStateStore->readRange(serverTagKeys));
	self->allTags.clear();
	if (self->lastEpochEnd > 0) {
		self->allTags.push_back(cacheTag);
	}

	if (self->forceRecovery) {
		self->safeLocality = oldLogSystem->getLogSystemConfig().tLogs[0].locality;
		for (auto& kv : rawTags) {
			Tag tag = decodeServerTagValue(kv.value);
			if (tag.locality == self->safeLocality) {
				self->allTags.push_back(tag);
			}
		}
	} else {
		for (auto& kv : rawTags) {
			self->allTags.push_back(decodeServerTagValue(kv.value));
		}
	}

	RangeResult rawHistoryTags = wait(self->txnStateStore->readRange(serverTagHistoryKeys));
	for (auto& kv : rawHistoryTags) {
		self->allTags.push_back(decodeServerTagValue(kv.value));
	}

	Optional<Value> metaclusterRegistrationVal =
	    wait(self->txnStateStore->readValue(metacluster::metadata::metaclusterRegistration().key));
	Optional<UnversionedMetaclusterRegistrationEntry> metaclusterRegistration =
	    UnversionedMetaclusterRegistrationEntry::decode(metaclusterRegistrationVal);
	Optional<ClusterName> metaclusterName;
	Optional<UID> metaclusterId;
	Optional<ClusterName> clusterName;
	Optional<UID> clusterId;
	self->controllerData->db.metaclusterRegistration = metaclusterRegistration;
	if (metaclusterRegistration.present()) {
		self->controllerData->db.metaclusterName = metaclusterRegistration.get().metaclusterName;
		self->controllerData->db.clusterType = metaclusterRegistration.get().clusterType;
		metaclusterName = metaclusterRegistration.get().metaclusterName;
		metaclusterId = metaclusterRegistration.get().metaclusterId;
		if (metaclusterRegistration.get().clusterType == ClusterType::METACLUSTER_DATA) {
			clusterName = metaclusterRegistration.get().name;
			clusterId = metaclusterRegistration.get().id;
		}
	} else {
		self->controllerData->db.metaclusterName = Optional<ClusterName>();
		self->controllerData->db.clusterType = ClusterType::STANDALONE;
	}

	TraceEvent("MetaclusterMetadata")
	    .detail("ClusterType", clusterTypeToString(self->controllerData->db.clusterType))
	    .detail("MetaclusterName", metaclusterName)
	    .detail("MetaclusterId", metaclusterId)
	    .detail("DataClusterName", clusterName)
	    .detail("DataClusterId", clusterId)
	    .trackLatest(self->metaclusterEventHolder->trackingKey);

	uniquify(self->allTags);

	// auto kvs = self->txnStateStore->readRange( systemKeys );
	// for( auto & kv : kvs.get() )
	//	TraceEvent("ClusterRecoveredTXS", self->dbgid).detail("K", kv.key).detail("V", kv.value);

	self->txnStateLogAdapter->setNextVersion(
	    oldLogSystem->getEnd()); //< FIXME: (1) the log adapter should do this automatically after recovery; (2) if we
	                             // make KeyValueStoreMemory guarantee immediate reads, we should be able to get rid of
	                             // the discardCommit() below and not need a writable log adapter

	TraceEvent("RTSSComplete", self->dbgid).log();

	return Void();
}

ACTOR Future<Void> sendInitialCommitToResolvers(Reference<ClusterRecoveryData> self) {
	state KeyRange txnKeys = allKeys;
	state Sequence txnSequence = 0;
	ASSERT(self->recoveryTransactionVersion);

	state RangeResult data =
	    self->txnStateStore
	        ->readRange(txnKeys, BUGGIFY ? 3 : SERVER_KNOBS->DESIRED_TOTAL_BYTES, SERVER_KNOBS->DESIRED_TOTAL_BYTES)
	        .get();
	state std::vector<Future<Void>> txnReplies;
	state int64_t dataOutstanding = 0;

	state std::vector<Endpoint> endpoints;
	for (auto& it : self->commitProxies) {
		endpoints.push_back(it.txnState.getEndpoint());
	}
	if (SERVER_KNOBS->PROXY_USE_RESOLVER_PRIVATE_MUTATIONS) {
		// Broadcasts transaction state store to resolvers.
		for (auto& it : self->resolvers) {
			endpoints.push_back(it.txnState.getEndpoint());
		}
	}
	loop {
		if (!data.size())
			break;
		((KeyRangeRef&)txnKeys) = KeyRangeRef(keyAfter(data.back().key, txnKeys.arena()), txnKeys.end);
		RangeResult nextData =
		    self->txnStateStore
		        ->readRange(txnKeys, BUGGIFY ? 3 : SERVER_KNOBS->DESIRED_TOTAL_BYTES, SERVER_KNOBS->DESIRED_TOTAL_BYTES)
		        .get();

		TxnStateRequest req;
		req.arena = data.arena();
		req.data = data;
		req.sequence = txnSequence;
		req.last = !nextData.size();
		req.broadcastInfo = endpoints;
		txnReplies.push_back(broadcastTxnRequest(req, SERVER_KNOBS->TXN_STATE_SEND_AMOUNT, false));
		dataOutstanding += SERVER_KNOBS->TXN_STATE_SEND_AMOUNT * data.arena().getSize();
		data = nextData;
		txnSequence++;

		if (dataOutstanding > SERVER_KNOBS->MAX_TXS_SEND_MEMORY) {
			wait(waitForAll(txnReplies));
			txnReplies = std::vector<Future<Void>>();
			dataOutstanding = 0;
		}

		wait(yield());
	}
	wait(waitForAll(txnReplies));
	TraceEvent("RecoveryInternal", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::recovery_transaction)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::recovery_transaction])
	    .detail("RecoveryTxnVersion", self->recoveryTransactionVersion)
	    .detail("LastEpochEnd", self->lastEpochEnd)
	    .detail("Step", "SentTxnStateStoreToCommitProxies");

	std::vector<Future<ResolveTransactionBatchReply>> replies;
	for (auto& r : self->resolvers) {
		ResolveTransactionBatchRequest req;
		req.prevVersion = -1;
		req.version = self->lastEpochEnd;
		req.lastReceivedVersion = -1;

		replies.push_back(brokenPromiseToNever(r.resolve.getReply(req)));
	}

	wait(waitForAll(replies));
	TraceEvent("RecoveryInternal", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::recovery_transaction)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::recovery_transaction])
	    .detail("RecoveryTxnVersion", self->recoveryTransactionVersion)
	    .detail("LastEpochEnd", self->lastEpochEnd)
	    .detail("Step", "InitializedAllResolvers");
	return Void();
}

ACTOR Future<Void> triggerUpdates(Reference<ClusterRecoveryData> self, Reference<ILogSystem> oldLogSystem) {
	loop {
		wait(oldLogSystem->onLogSystemConfigChange() || self->cstate.fullyRecovered.getFuture() ||
		     self->recruitmentStalled->onChange());
		if (self->cstate.fullyRecovered.isSet())
			return Void();

		self->registrationTrigger.trigger();
	}
}

ACTOR Future<Void> discardCommit(IKeyValueStore* store, LogSystemDiskQueueAdapter* adapter) {
	state Future<LogSystemDiskQueueAdapter::CommitMessage> fcm = adapter->getCommitMessage();
	state Future<Void> committed = store->commit();
	LogSystemDiskQueueAdapter::CommitMessage cm = wait(fcm);
	ASSERT(!committed.isReady());
	cm.acknowledge.send(Void());
	ASSERT(committed.isReady());
	return Void();
}

void updateConfigForForcedRecovery(Reference<ClusterRecoveryData> self,
                                   std::vector<Standalone<CommitTransactionRef>>* initialConfChanges) {
	bool regionsChanged = false;
	for (auto& it : self->configuration.regions) {
		if (it.dcId == self->controllerData->clusterControllerDcId.get() && it.priority < 0) {
			it.priority = 1;
			regionsChanged = true;
		} else if (it.dcId != self->controllerData->clusterControllerDcId.get() && it.priority >= 0) {
			it.priority = -1;
			regionsChanged = true;
		}
	}
	Standalone<CommitTransactionRef> regionCommit;
	regionCommit.mutations.push_back_deep(
	    regionCommit.arena(),
	    MutationRef(MutationRef::SetValue, configKeysPrefix.toString() + "usable_regions", "1"_sr));
	self->configuration.applyMutation(regionCommit.mutations.back());
	if (regionsChanged) {
		std::sort(
		    self->configuration.regions.begin(), self->configuration.regions.end(), RegionInfo::sort_by_priority());
		StatusObject regionJSON;
		regionJSON["regions"] = self->configuration.getRegionJSON();
		regionCommit.mutations.push_back_deep(
		    regionCommit.arena(),
		    MutationRef(MutationRef::SetValue,
		                configKeysPrefix.toString() + "regions",
		                BinaryWriter::toValue(regionJSON, IncludeVersion(ProtocolVersion::withRegionConfiguration()))
		                    .toString()));
		self->configuration.applyMutation(
		    regionCommit.mutations.back()); // modifying the configuration directly does not change the configuration
		                                    // when it is re-serialized unless we call applyMutation
		TraceEvent("ForcedRecoveryConfigChange", self->dbgid)
		    .setMaxEventLength(11000)
		    .setMaxFieldLength(10000)
		    .detail("Conf", self->configuration.toString());
	}
	initialConfChanges->push_back(regionCommit);
}

ACTOR Future<Void> recoverFrom(Reference<ClusterRecoveryData> self,
                               Reference<ILogSystem> oldLogSystem,
                               std::vector<StorageServerInterface>* seedServers,
                               std::vector<Standalone<CommitTransactionRef>>* initialConfChanges,
                               Future<Version> poppedTxsVersion) {
	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::reading_transaction_system_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::reading_transaction_system_state])
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);
	self->hasConfiguration = false;

	if (BUGGIFY)
		wait(delay(10.0));

	Version txsPoppedVersion = wait(poppedTxsVersion);
	wait(readTransactionSystemState(self, oldLogSystem, txsPoppedVersion));
	for (auto& itr : *initialConfChanges) {
		for (auto& m : itr.mutations) {
			self->configuration.applyMutation(m);
		}
	}

	if (self->forceRecovery) {
		updateConfigForForcedRecovery(self, initialConfChanges);
	}

	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
		debug_checkMaxRestoredVersion(UID(), self->lastEpochEnd, "DBRecovery");
	}

	// Ordinarily we pass through this loop once and recover.  We go around the loop if recovery stalls for more than a
	// second, a provisional master is initialized, and an "emergency transaction" is submitted that might change the
	// configuration so that we can finish recovery.

	state std::map<Optional<Value>, int8_t> originalLocalityMap = self->dcId_locality;
	state Future<std::vector<Standalone<CommitTransactionRef>>> recruitments =
	    recruitEverything(self, seedServers, oldLogSystem);
	state double provisionalDelay = SERVER_KNOBS->PROVISIONAL_START_DELAY;
	loop {
		state Future<Standalone<CommitTransactionRef>> provisional = provisionalMaster(self, delay(provisionalDelay));
		provisionalDelay =
		    std::min(SERVER_KNOBS->PROVISIONAL_MAX_DELAY, provisionalDelay * SERVER_KNOBS->PROVISIONAL_DELAY_GROWTH);
		choose {
			when(std::vector<Standalone<CommitTransactionRef>> confChanges = wait(recruitments)) {
				initialConfChanges->insert(initialConfChanges->end(), confChanges.begin(), confChanges.end());
				provisional.cancel();
				break;
			}
			when(Standalone<CommitTransactionRef> _req = wait(provisional)) {
				state Standalone<CommitTransactionRef> req = _req; // mutable
				CODE_PROBE(true, "Emergency transaction processing during recovery");
				TraceEvent("EmergencyTransaction", self->dbgid).log();
				for (auto m = req.mutations.begin(); m != req.mutations.end(); ++m)
					TraceEvent("EmergencyTransactionMutation", self->dbgid)
					    .detail("MType", m->type)
					    .detail("P1", m->param1)
					    .detail("P2", m->param2);

				DatabaseConfiguration oldConf = self->configuration;
				self->configuration = self->originalConfiguration;
				for (auto& m : req.mutations)
					self->configuration.applyMutation(m);

				initialConfChanges->clear();
				if (self->originalConfiguration.isValid() &&
				    self->configuration.usableRegions != self->originalConfiguration.usableRegions) {
					TraceEvent(SevWarnAlways, "CannotChangeUsableRegions", self->dbgid).log();
					self->configuration = self->originalConfiguration;
				} else {
					initialConfChanges->push_back(req);
				}
				if (self->forceRecovery) {
					updateConfigForForcedRecovery(self, initialConfChanges);
				}

				if (self->configuration != oldConf) { // confChange does not trigger when including servers
					self->dcId_locality = originalLocalityMap;
					recruitments = recruitEverything(self, seedServers, oldLogSystem);
				}
			}
		}

		provisional.cancel();
	}

	return Void();
}

ACTOR Future<Void> clusterRecoveryCore(Reference<ClusterRecoveryData> self) {
	state TraceInterval recoveryInterval("ClusterRecovery");
	state double recoverStartTime = now();

	self->addActor.send(waitFailureServer(self->masterInterface.waitFailure.getFuture()));

	TraceEvent(recoveryInterval.begin(), self->dbgid).log();

	self->recoveryState = RecoveryState::READING_CSTATE;
	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::reading_coordinated_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::reading_coordinated_state])
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);

	wait(self->cstate.read());

	// Unless the cluster database is 'empty', the cluster's EncryptionAtRest status is readable once cstate is
	// recovered
	if (!self->cstate.myDBState.tLogs.empty() && self->controllerData->encryptionAtRestMode.canBeSet()) {
		self->controllerData->encryptionAtRestMode.send(self->cstate.myDBState.encryptionAtRestMode);
	}

	if (self->cstate.prevDBState.lowestCompatibleProtocolVersion > currentProtocolVersion()) {
		TraceEvent(SevWarnAlways, "IncompatibleProtocolVersion", self->dbgid).log();
		throw internal_error();
	}

	self->recoveryState = RecoveryState::LOCKING_CSTATE;
	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::locking_coordinated_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::locking_coordinated_state])
	    .detail("TLogs", self->cstate.prevDBState.tLogs.size())
	    .detail("ActiveGenerations", self->cstate.myDBState.oldTLogData.size() + 1)
	    .detail("MyRecoveryCount", self->cstate.prevDBState.recoveryCount + 2)
	    .detail("ForceRecovery", self->forceRecovery)
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);
	// for (const auto& old : self->cstate.prevDBState.oldTLogData) {
	//	TraceEvent("BWReadCoreState", self->dbgid).detail("Epoch", old.epoch).detail("Version", old.epochEnd);
	//}

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_GENERATION_EVENT_NAME).c_str(),
	           self->dbgid)
	    .detail("ActiveGenerations", self->cstate.myDBState.oldTLogData.size() + 1)
	    .trackLatest(self->clusterRecoveryGenerationsEventHolder->trackingKey);

	if (self->cstate.myDBState.oldTLogData.size() > CLIENT_KNOBS->MAX_GENERATIONS_OVERRIDE) {
		if (self->cstate.myDBState.oldTLogData.size() >= CLIENT_KNOBS->MAX_GENERATIONS) {
			TraceEvent(SevError, "RecoveryStoppedTooManyOldGenerations")
			    .detail("OldGenerations", self->cstate.myDBState.oldTLogData.size())
			    .detail("Reason",
			            "Recovery stopped because too many recoveries have happened since the last time the cluster "
			            "was fully_recovered. Set --knob-max-generations-override on your server processes to a value "
			            "larger than OldGenerations to resume recovery once the underlying problem has been fixed.");
			wait(Future<Void>(Never()));
		} else if (self->cstate.myDBState.oldTLogData.size() > CLIENT_KNOBS->RECOVERY_DELAY_START_GENERATION) {
			TraceEvent(SevError, "RecoveryDelayedTooManyOldGenerations")
			    .detail("OldGenerations", self->cstate.myDBState.oldTLogData.size())
			    .detail("Reason",
			            "Recovery is delayed because too many recoveries have happened since the last time the cluster "
			            "was fully_recovered. Set --knob-max-generations-override on your server processes to a value "
			            "larger than OldGenerations to resume recovery once the underlying problem has been fixed.");
			wait(delay(CLIENT_KNOBS->RECOVERY_DELAY_SECONDS_PER_GENERATION *
			           (self->cstate.myDBState.oldTLogData.size() - CLIENT_KNOBS->RECOVERY_DELAY_START_GENERATION)));
		}
		if (g_network->isSimulated() && self->cstate.myDBState.oldTLogData.size() > CLIENT_KNOBS->MAX_GENERATIONS_SIM) {
			disableConnectionFailures("TooManyGenerations");
		}
	}

	state Reference<AsyncVar<Reference<ILogSystem>>> oldLogSystems(new AsyncVar<Reference<ILogSystem>>);
	state Future<Void> recoverAndEndEpoch =
	    ILogSystem::recoverAndEndEpoch(oldLogSystems,
	                                   self->dbgid,
	                                   self->cstate.prevDBState,
	                                   self->clusterController.tlogRejoin.getFuture(),
	                                   self->controllerData->db.serverInfo->get().myLocality,
	                                   std::addressof(self->forceRecovery));

	DBCoreState newState = self->cstate.myDBState;
	newState.recoveryCount++;
	if (self->cstate.prevDBState.newestProtocolVersion.isInvalid() ||
	    self->cstate.prevDBState.newestProtocolVersion < currentProtocolVersion()) {
		ASSERT(self->cstate.myDBState.lowestCompatibleProtocolVersion.isInvalid() ||
		       !self->cstate.myDBState.newestProtocolVersion.isInvalid());
		newState.newestProtocolVersion = currentProtocolVersion();
		newState.lowestCompatibleProtocolVersion = minCompatibleProtocolVersion;
	}
	wait(self->cstate.write(newState) || recoverAndEndEpoch);

	TraceEvent("ProtocolVersionCompatibilityChecked", self->dbgid)
	    .detail("NewestProtocolVersion", self->cstate.myDBState.newestProtocolVersion)
	    .detail("LowestCompatibleProtocolVersion", self->cstate.myDBState.lowestCompatibleProtocolVersion)
	    .trackLatest(self->swVersionCheckedEventHolder->trackingKey);

	self->recoveryState = RecoveryState::RECRUITING;

	state std::vector<StorageServerInterface> seedServers;
	state std::vector<Standalone<CommitTransactionRef>> initialConfChanges;
	state Future<Void> logChanges;
	state Future<Void> minRecoveryDuration;
	state Future<Version> poppedTxsVersion;

	loop {
		Reference<ILogSystem> oldLogSystem = oldLogSystems->get();
		if (oldLogSystem) {
			logChanges = triggerUpdates(self, oldLogSystem);
			if (!minRecoveryDuration.isValid()) {
				minRecoveryDuration = delay(SERVER_KNOBS->ENFORCED_MIN_RECOVERY_DURATION);
				poppedTxsVersion = oldLogSystem->getTxsPoppedVersion();
			}
		}

		state Future<Void> reg = oldLogSystem ? updateRegistration(self, oldLogSystem) : Never();
		self->registrationTrigger.trigger();

		choose {
			when(wait(oldLogSystem
			              ? recoverFrom(self, oldLogSystem, &seedServers, &initialConfChanges, poppedTxsVersion)
			              : Never())) {
				reg.cancel();
				break;
			}
			when(wait(oldLogSystems->onChange())) {}
			when(wait(reg)) {
				throw internal_error();
			}
			when(wait(recoverAndEndEpoch)) {
				throw internal_error();
			}
		}
	}

	if (self->neverCreated) {
		recoverStartTime = now();
	}

	recoverAndEndEpoch.cancel();

	ASSERT_LE(self->commitProxies.size(), self->configuration.getDesiredCommitProxies());
	ASSERT_GE(self->commitProxies.size(), 1);
	ASSERT_LE(self->grvProxies.size(), self->configuration.getDesiredGrvProxies());
	ASSERT_GE(self->grvProxies.size(), 1);
	ASSERT_LE(self->resolvers.size(), self->configuration.getDesiredResolvers());
	ASSERT_GE(self->resolvers.size(), 1);

	self->recoveryState = RecoveryState::RECOVERY_TRANSACTION;
	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::recovery_transaction)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::recovery_transaction])
	    .detail("PrimaryLocality", self->primaryLocality)
	    .detail("DcId", self->masterInterface.locality.dcId())
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);

	// Recovery transaction
	state bool debugResult = debug_checkMinRestoredVersion(UID(), self->lastEpochEnd, "DBRecovery", SevWarn);

	CommitTransactionRequest recoveryCommitRequest;
	recoveryCommitRequest.flags = recoveryCommitRequest.flags | CommitTransactionRequest::FLAG_IS_LOCK_AWARE;
	CommitTransactionRef& tr = recoveryCommitRequest.transaction;
	int mmApplied = 0; // The number of mutations in tr.mutations that have been applied to the txnStateStore so far
	if (self->lastEpochEnd != 0) {
		Optional<Value> snapRecoveryFlag = self->txnStateStore->readValue(writeRecoveryKey).get();
		TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_SNAPSHOT_CHECK_EVENT_NAME).c_str())
		    .detail("SnapRecoveryFlag", snapRecoveryFlag.present() ? snapRecoveryFlag.get().toString() : "N/A")
		    .detail("LastEpochEnd", self->lastEpochEnd);
		if (snapRecoveryFlag.present()) {
			CODE_PROBE(true, "Recovering from snapshot, writing to snapShotEndVersionKey");
			BinaryWriter bw(Unversioned());
			tr.set(recoveryCommitRequest.arena, snapshotEndVersionKey, (bw << self->lastEpochEnd).toValue());
			// Pause the backups that got restored in this snapshot to avoid data corruption
			// Requires further operational work to abort the backup
			TraceEvent(
			    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_PAUSE_AGENT_BACKUP_EVENT_NAME).c_str())
			    .log();
			Key backupPauseKey = FileBackupAgent::getPauseKey();
			tr.set(recoveryCommitRequest.arena, backupPauseKey, StringRef());
			// Clear the key so multiple recoveries will not overwrite the first version recorded
			tr.clear(recoveryCommitRequest.arena, singleKeyRange(writeRecoveryKey));
		}
		if (self->forceRecovery) {
			BinaryWriter bw(Unversioned());
			tr.set(recoveryCommitRequest.arena, killStorageKey, (bw << self->safeLocality).toValue());
		}

		// This transaction sets \xff/lastEpochEnd, which the shard servers can use to roll back speculatively
		//   processed semi-committed transactions from the previous epoch.
		// It also guarantees the shard servers and tlog servers eventually get versions in the new epoch, which
		//   clients might rely on.
		// This transaction is by itself in a batch (has its own version number), which simplifies storage servers
		// slightly (they assume there are no modifications to serverKeys in the same batch) The proxy also expects the
		// lastEpochEndKey mutation to be first in the transaction
		BinaryWriter bw(Unversioned());
		tr.set(recoveryCommitRequest.arena, lastEpochEndKey, (bw << self->lastEpochEnd).toValue());

		if (self->forceRecovery) {
			tr.set(recoveryCommitRequest.arena, rebootWhenDurableKey, StringRef());
			tr.set(recoveryCommitRequest.arena,
			       moveKeysLockOwnerKey,
			       BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned()));
		}
	} else {
		// Recruit and seed initial shard servers
		// This transaction must be the very first one in the database (version 1)
		seedShardServers(recoveryCommitRequest.arena, tr, seedServers);
	}
	// initialConfChanges have not been conflict checked against any earlier writes in the recovery transaction, so do
	// this as early as possible in the recovery transaction but see above comments as to why it can't be absolutely
	// first.  Theoretically emergency transactions should conflict check against the lastEpochEndKey.
	for (auto& itr : initialConfChanges) {
		tr.mutations.append_deep(recoveryCommitRequest.arena, itr.mutations.begin(), itr.mutations.size());
		tr.write_conflict_ranges.append_deep(
		    recoveryCommitRequest.arena, itr.write_conflict_ranges.begin(), itr.write_conflict_ranges.size());
	}

	tr.set(
	    recoveryCommitRequest.arena, primaryLocalityKey, BinaryWriter::toValue(self->primaryLocality, Unversioned()));
	tr.set(recoveryCommitRequest.arena, backupVersionKey, backupVersionValue);
	Optional<Value> txnStateStoreCoords = self->txnStateStore->readValue(coordinatorsKey).get();
	if (txnStateStoreCoords.present() &&
	    txnStateStoreCoords.get() != self->coordinators.ccr->getConnectionString().toString()) {
		tr.set(recoveryCommitRequest.arena, previousCoordinatorsKey, txnStateStoreCoords.get());
	}
	tr.set(recoveryCommitRequest.arena, coordinatorsKey, self->coordinators.ccr->getConnectionString().toString());
	tr.set(recoveryCommitRequest.arena, logsKey, self->logSystem->getLogsValue());
	tr.set(recoveryCommitRequest.arena,
	       primaryDatacenterKey,
	       self->controllerData->clusterControllerDcId.present() ? self->controllerData->clusterControllerDcId.get()
	                                                             : StringRef());

	tr.clear(recoveryCommitRequest.arena, tLogDatacentersKeys);
	for (auto& dc : self->primaryDcId) {
		tr.set(recoveryCommitRequest.arena, tLogDatacentersKeyFor(dc), StringRef());
	}
	if (self->configuration.usableRegions > 1) {
		for (auto& dc : self->remoteDcIds) {
			tr.set(recoveryCommitRequest.arena, tLogDatacentersKeyFor(dc), StringRef());
		}
	}

	applyMetadataMutations(SpanContext(),
	                       self->dbgid,
	                       recoveryCommitRequest.arena,
	                       tr.mutations.slice(mmApplied, tr.mutations.size()),
	                       self->txnStateStore);
	mmApplied = tr.mutations.size();

	tr.read_snapshot = self->recoveryTransactionVersion; // lastEpochEnd would make more sense, but isn't in the initial
	                                                     // window of the resolver(s)

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_COMMIT_EVENT_NAME).c_str(), self->dbgid)
	    .log();
	state Future<ErrorOr<CommitID>> recoveryCommit = self->commitProxies[0].commit.tryGetReply(recoveryCommitRequest);
	self->addActor.send(self->logSystem->onError());
	self->addActor.send(waitResolverFailure(self->resolvers));
	self->addActor.send(waitCommitProxyFailure(self->commitProxies));
	self->addActor.send(waitGrvProxyFailure(self->grvProxies));
	self->addActor.send(reportErrors(updateRegistration(self, self->logSystem), "UpdateRegistration", self->dbgid));
	self->registrationTrigger.trigger();

	wait(discardCommit(self->txnStateStore, self->txnStateLogAdapter));

	// Wait for the recovery transaction to complete.
	// SOMEDAY: For faster recovery, do this and setDBState asynchronously and don't wait for them
	// unless we want to change TLogs
	wait((success(recoveryCommit) && sendInitialCommitToResolvers(self)));
	if (recoveryCommit.isReady() && recoveryCommit.get().isError()) {
		CODE_PROBE(true, "Cluster recovery failed because of the initial commit failed");
		throw cluster_recovery_failed();
	}

	ASSERT(self->recoveryTransactionVersion != 0);

	self->recoveryState = RecoveryState::WRITING_CSTATE;
	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::writing_coordinated_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::writing_coordinated_state])
	    .detail("TLogList", self->logSystem->describe())
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);

	// Multiple masters prevent conflicts between themselves via CoordinatedState (self->cstate)
	//  1. If SetMaster succeeds, then by CS's contract, these "new" Tlogs are the immediate
	//     successors of the "old" ones we are replacing
	//  2. logSystem->recoverAndEndEpoch ensured that a co-quorum of the "old" tLogs were stopped at
	//     versions <= self->lastEpochEnd, so no versions > self->lastEpochEnd could be (fully) committed to them.
	//  3. No other master will attempt to commit anything to our "new" Tlogs
	//     because it didn't recruit them
	//  4. Therefore, no full commit can come between self->lastEpochEnd and the first commit
	//     we made to the new Tlogs (self->recoveryTransactionVersion), and only our own semi-commits can come between
	//     our first commit and the next new TLogs

	self->addActor.send(trackTlogRecovery(self, oldLogSystems, minRecoveryDuration));
	debug_advanceMaxCommittedVersion(UID(), self->recoveryTransactionVersion);
	wait(self->cstateUpdated.getFuture());
	debug_advanceMinCommittedVersion(UID(), self->recoveryTransactionVersion);

	if (debugResult) {
		TraceEvent(self->forceRecovery ? SevWarn : SevError, "DBRecoveryDurabilityError").log();
	}

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_COMMIT_EVENT_NAME).c_str(), self->dbgid)
	    .detail("TLogs", self->logSystem->describe())
	    .detail("RecoveryCount", self->cstate.myDBState.recoveryCount)
	    .detail("RecoveryTransactionVersion", self->recoveryTransactionVersion);

	TraceEvent(recoveryInterval.end(), self->dbgid)
	    .detail("RecoveryTransactionVersion", self->recoveryTransactionVersion);

	self->recoveryState = RecoveryState::ACCEPTING_COMMITS;
	double recoveryDuration = now() - recoverStartTime;

	TraceEvent((recoveryDuration > 4 && !g_network->isSimulated()) ? SevWarnAlways : SevInfo,
	           getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_DURATION_EVENT_NAME).c_str(),
	           self->dbgid)
	    .detail("RecoveryDuration", recoveryDuration)
	    .trackLatest(self->clusterRecoveryDurationEventHolder->trackingKey);

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(), self->dbgid)
	    .detail("StatusCode", RecoveryStatus::accepting_commits)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::accepting_commits])
	    .detail("StoreType", self->configuration.storageServerStoreType)
	    .detail("RecoveryDuration", recoveryDuration)
	    .trackLatest(self->clusterRecoveryStateEventHolder->trackingKey);

	TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_AVAILABLE_EVENT_NAME).c_str(),
	           self->dbgid)
	    .detail("NumOfOldGensOfLogs", self->cstate.myDBState.oldTLogData.size())
	    .detail("AvailableAtVersion", self->recoveryTransactionVersion)
	    .trackLatest(self->clusterRecoveryAvailableEventHolder->trackingKey);

	self->addActor.send(changeCoordinators(self));
	Database cx = openDBOnServer(self->dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	self->addActor.send(configurationMonitor(self, cx));
	if (self->configuration.backupWorkerEnabled) {
		self->addActor.send(recruitBackupWorkers(self, cx));
	} else {
		self->logSystem->setOldestBackupEpoch(self->cstate.myDBState.recoveryCount);
	}

	wait(Future<Void>(Never()));
	throw internal_error();
}

ACTOR Future<Void> cleanupRecoveryActorCollection(Reference<ClusterRecoveryData> self, bool exThrown) {
	if (self.isValid()) {
		wait(delay(0.0));

		while (!self->addActor.isEmpty()) {
			self->addActor.getFuture().pop();
		}
	}

	return Void();
}

bool isNormalClusterRecoveryError(const Error& error) {
	return normalClusterRecoveryErrors().count(error.code());
}

std::string& getRecoveryEventName(ClusterRecoveryEventType type) {
	ASSERT(type >= ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME &&
	       type < ClusterRecoveryEventType::CLUSTER_RECOVERY_LAST);

	// Cluster recovery state machine used to be driven from master/sequencer process, recovery state
	// tracking is used by test environment and tooling scripts. To ease the process of migration, prefix
	// is controlled by a ServerKnob for now.
	//
	// TODO: ServerKnob should be removed ones all tooling scripts and usage is identified and updated accordingly

	static std::map<ClusterRecoveryEventType, std::string> recoveryEventNameMap;
	if (recoveryEventNameMap.empty()) {
		// initialize the map
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryState" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_COMMIT_TLOG_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryCommittedTLogs" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_DURATION_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryDuration" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_GENERATION_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryGenerations" });
		recoveryEventNameMap.insert(
		    { ClusterRecoveryEventType::CLUSTER_RECOVERY_SS_RECRUITMENT_EVENT_NAME,
		      SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryRecruitingInitialStorageServer" });
		recoveryEventNameMap.insert(
		    { ClusterRecoveryEventType::CLUSTER_RECOVERY_INVALID_CONFIG_EVENT_NAME,
		      SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryInvalidConfiguration" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_RECOVERING_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "Recovering" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_RECOVERED_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveredConfig" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_SNAPSHOT_CHECK_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoverySnapshotCheck" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_SS_RECRUITMENT_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoverySnapshotCheck" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_PAUSE_AGENT_BACKUP_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryPauseBackupAgents" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_COMMIT_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryCommit" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_AVAILABLE_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryAvailable" });
		recoveryEventNameMap.insert({ ClusterRecoveryEventType::CLUSTER_RECOVERY_METRICS_EVENT_NAME,
		                              SERVER_KNOBS->CLUSTER_RECOVERY_EVENT_NAME_PREFIX + "RecoveryMetrics" });
	}

	auto iter = recoveryEventNameMap.find(type);
	ASSERT(iter != recoveryEventNameMap.end());
	return iter->second;
}