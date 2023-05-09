/*
 * Recruiter.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/Recruiter.h"

#include "flow/actorcompiler.h" // must be last include

class RecruiterImpl {
public:
	ACTOR static Future<Void> newSeedServers(Recruiter const* recruiter,
	                                         Reference<ClusterRecoveryData> clusterRecoveryData,
	                                         RecruitFromConfigurationReply recruits,
	                                         std::vector<StorageServerInterface>* servers) {
		// This is only necessary if the database is at version 0
		servers->clear();
		if (clusterRecoveryData->lastEpochEnd)
			return Void();

		state int idx = 0;
		state std::map<Optional<Value>, Tag> dcId_tags;
		state int8_t nextLocality = 0;
		while (idx < recruits.storageServers.size()) {
			TraceEvent(
			    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_SS_RECRUITMENT_EVENT_NAME).c_str(),
			    clusterRecoveryData->dbgid)
			    .detail("CandidateWorker", recruits.storageServers[idx].locality.toString());

			InitializeStorageRequest isr;
			isr.seedTag = dcId_tags.count(recruits.storageServers[idx].locality.dcId())
			                  ? dcId_tags[recruits.storageServers[idx].locality.dcId()]
			                  : Tag(nextLocality, 0);
			isr.storeType = clusterRecoveryData->configuration.storageServerStoreType;
			isr.reqId = deterministicRandom()->randomUniqueID();
			isr.interfaceId = deterministicRandom()->randomUniqueID();
			isr.initialClusterVersion = clusterRecoveryData->recoveryTransactionVersion;
			isr.encryptMode = clusterRecoveryData->configuration.encryptionAtRestMode;

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

		clusterRecoveryData->dcId_locality.clear();
		for (auto& it : dcId_tags) {
			clusterRecoveryData->dcId_locality[it.first] = it.second.locality;
		}

		TraceEvent("ClusterRecoveryRecruitedInitialStorageServers", clusterRecoveryData->dbgid)
		    .detail("TargetCount", clusterRecoveryData->configuration.storageTeamSize)
		    .detail("Servers", describe(*servers));

		return Void();
	}

	ACTOR static Future<Void> newCommitProxies(Recruiter const* recruiter,
	                                           Reference<ClusterRecoveryData> clusterRecoveryData,
	                                           RecruitFromConfigurationReply recr) {
		std::vector<Future<CommitProxyInterface>> initializationReplies;
		for (int i = 0; i < recr.commitProxies.size(); i++) {
			InitializeCommitProxyRequest req;
			req.master = clusterRecoveryData->masterInterface;
			req.masterLifetime = clusterRecoveryData->masterLifetime;
			req.recoveryCount = clusterRecoveryData->cstate.myDBState.recoveryCount + 1;
			req.recoveryTransactionVersion = clusterRecoveryData->recoveryTransactionVersion;
			req.firstProxy = i == 0;
			req.encryptMode = recruiter->getEncryptionAtRest(clusterRecoveryData->configuration);
			TraceEvent("CommitProxyReplies", clusterRecoveryData->dbgid)
			    .detail("WorkerID", recr.commitProxies[i].id())
			    .detail("RecoveryTxnVersion", clusterRecoveryData->recoveryTransactionVersion)
			    .detail("EncryptMode", req.encryptMode.toString())
			    .detail("FirstProxy", req.firstProxy ? "True" : "False");
			initializationReplies.push_back(transformErrors(
			    throwErrorOr(recr.commitProxies[i].commitProxy.getReplyUnlessFailedFor(
			        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    commit_proxy_failed()));
		}

		std::vector<CommitProxyInterface> newRecruits = wait(getAll(initializationReplies));
		TraceEvent("CommitProxyInitializationComplete", clusterRecoveryData->dbgid).log();
		// It is required for the correctness of COMMIT_ON_FIRST_PROXY that clusterRecoveryData->commitProxies[0] is the
		// firstCommitProxy.
		clusterRecoveryData->commitProxies = newRecruits;

		return Void();
	}

	ACTOR static Future<Void> newGrvProxies(Recruiter const* recruiter,
	                                        Reference<ClusterRecoveryData> clusterRecoveryData,
	                                        RecruitFromConfigurationReply recr) {
		std::vector<Future<GrvProxyInterface>> initializationReplies;
		for (int i = 0; i < recr.grvProxies.size(); i++) {
			InitializeGrvProxyRequest req;
			req.master = clusterRecoveryData->masterInterface;
			req.masterLifetime = clusterRecoveryData->masterLifetime;
			req.recoveryCount = clusterRecoveryData->cstate.myDBState.recoveryCount + 1;
			TraceEvent("GrvProxyReplies", clusterRecoveryData->dbgid).detail("WorkerID", recr.grvProxies[i].id());
			initializationReplies.push_back(transformErrors(
			    throwErrorOr(recr.grvProxies[i].grvProxy.getReplyUnlessFailedFor(
			        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    grv_proxy_failed()));
		}

		std::vector<GrvProxyInterface> newRecruits = wait(getAll(initializationReplies));
		TraceEvent("GrvProxyInitializationComplete", clusterRecoveryData->dbgid).log();
		clusterRecoveryData->grvProxies = newRecruits;
		return Void();
	}

	ACTOR static Future<Void> newResolvers(Recruiter const* recruiter,
	                                       Reference<ClusterRecoveryData> clusterRecoveryData,
	                                       RecruitFromConfigurationReply recr) {
		std::vector<Future<ResolverInterface>> initializationReplies;
		for (int i = 0; i < recr.resolvers.size(); i++) {
			InitializeResolverRequest req;
			req.masterLifetime = clusterRecoveryData->masterLifetime;
			req.recoveryCount = clusterRecoveryData->cstate.myDBState.recoveryCount + 1;
			req.commitProxyCount = recr.commitProxies.size();
			req.resolverCount = recr.resolvers.size();
			req.encryptMode = recruiter->getEncryptionAtRest(clusterRecoveryData->configuration);
			TraceEvent("ResolverReplies", clusterRecoveryData->dbgid).detail("WorkerID", recr.resolvers[i].id());
			initializationReplies.push_back(transformErrors(
			    throwErrorOr(recr.resolvers[i].resolver.getReplyUnlessFailedFor(
			        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
			    resolver_failed()));
		}

		std::vector<ResolverInterface> newRecruits = wait(getAll(initializationReplies));
		TraceEvent("ResolverInitializationComplete", clusterRecoveryData->dbgid).log();
		clusterRecoveryData->resolvers = newRecruits;

		return Void();
	}

	ACTOR static Future<Void> newTLogServers(Recruiter const* recruiter,
	                                         Reference<ClusterRecoveryData> clusterRecoveryData,
	                                         RecruitFromConfigurationReply recr,
	                                         Reference<ILogSystem> oldLogSystem,
	                                         std::vector<Standalone<CommitTransactionRef>>* initialConfChanges) {
		if (clusterRecoveryData->configuration.usableRegions > 1) {
			state Optional<Key> remoteDcId =
			    clusterRecoveryData->remoteDcIds.size() ? clusterRecoveryData->remoteDcIds[0] : Optional<Key>();
			if (!clusterRecoveryData->dcId_locality.count(recr.dcId)) {
				int8_t loc = clusterRecoveryData->getNextLocality();
				Standalone<CommitTransactionRef> tr;
				tr.set(tr.arena(), tagLocalityListKeyFor(recr.dcId), tagLocalityListValue(loc));
				initialConfChanges->push_back(tr);
				clusterRecoveryData->dcId_locality[recr.dcId] = loc;
				TraceEvent(SevWarn, "UnknownPrimaryDCID", clusterRecoveryData->dbgid)
				    .detail("PrimaryId", recr.dcId)
				    .detail("Loc", loc);
			}

			if (!clusterRecoveryData->dcId_locality.count(remoteDcId)) {
				int8_t loc = clusterRecoveryData->getNextLocality();
				Standalone<CommitTransactionRef> tr;
				tr.set(tr.arena(), tagLocalityListKeyFor(remoteDcId), tagLocalityListValue(loc));
				initialConfChanges->push_back(tr);
				clusterRecoveryData->dcId_locality[remoteDcId] = loc;
				TraceEvent(SevWarn, "UnknownRemoteDCID", clusterRecoveryData->dbgid)
				    .detail("RemoteId", remoteDcId)
				    .detail("Loc", loc);
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
			    clusterRecoveryData->configuration,
			    remoteDcId,
			    recr.tLogs.size() * std::max<int>(1,
			                                      clusterRecoveryData->configuration.desiredLogRouterCount /
			                                          std::max<int>(1, recr.tLogs.size())),
			    exclusionWorkerIds);
			remoteRecruitReq.dbgId = clusterRecoveryData->dbgid;
			state Reference<RecruitRemoteWorkersInfo> recruitWorkersInfo =
			    makeReference<RecruitRemoteWorkersInfo>(remoteRecruitReq);
			recruitWorkersInfo->dbgId = clusterRecoveryData->dbgid;
			Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers = clusterRecruitRemoteFromConfiguration(
			    recruiter, clusterRecoveryData->controllerData, recruitWorkersInfo);

			clusterRecoveryData->primaryLocality = clusterRecoveryData->dcId_locality[recr.dcId];
			clusterRecoveryData->logSystem = Reference<ILogSystem>(); // Cancels the actors in the previous log system.
			Reference<ILogSystem> newLogSystem =
			    wait(oldLogSystem->newEpoch(recr,
			                                fRemoteWorkers,
			                                clusterRecoveryData->configuration,
			                                clusterRecoveryData->cstate.myDBState.recoveryCount + 1,
			                                clusterRecoveryData->recoveryTransactionVersion,
			                                clusterRecoveryData->primaryLocality,
			                                clusterRecoveryData->dcId_locality[remoteDcId],
			                                clusterRecoveryData->allTags,
			                                clusterRecoveryData->recruitmentStalled));
			clusterRecoveryData->logSystem = newLogSystem;
		} else {
			clusterRecoveryData->primaryLocality = tagLocalitySpecial;
			clusterRecoveryData->logSystem = Reference<ILogSystem>(); // Cancels the actors in the previous log system.
			Reference<ILogSystem> newLogSystem =
			    wait(oldLogSystem->newEpoch(recr,
			                                Never(),
			                                clusterRecoveryData->configuration,
			                                clusterRecoveryData->cstate.myDBState.recoveryCount + 1,
			                                clusterRecoveryData->recoveryTransactionVersion,
			                                clusterRecoveryData->primaryLocality,
			                                tagLocalitySpecial,
			                                clusterRecoveryData->allTags,
			                                clusterRecoveryData->recruitmentStalled));
			clusterRecoveryData->logSystem = newLogSystem;
		}
		return Void();
	}

	ACTOR static Future<RecruitRemoteFromConfigurationReply> clusterRecruitRemoteFromConfiguration(
	    Recruiter const* recruiter,
	    ClusterControllerData* clusterControllerData,
	    Reference<RecruitRemoteWorkersInfo> req) {
		// At the moment this doesn't really need to be an actor (it always completes immediately)
		CODE_PROBE(true, "ClusterController RecruitTLogsRequest Remote");
		loop {
			try {
				auto rep = clusterControllerData->findRemoteWorkersForConfiguration(req->req);
				return rep;
			} catch (Error& e) {
				if (e.code() == error_code_no_more_servers &&
				    clusterControllerData->goodRemoteRecruitmentTime.isReady()) {
					clusterControllerData->outstandingRemoteRecruitmentRequests.push_back(req);
					TraceEvent(SevWarn, "RecruitRemoteFromConfigurationNotAvailable", clusterControllerData->id)
					    .error(e);
					wait(req->waitForCompletion.onTrigger());
					return req->rep;
				} else if (e.code() == error_code_operation_failed || e.code() == error_code_no_more_servers) {
					// recruitment not good enough, try again
					TraceEvent("RecruitRemoteFromConfigurationRetry", clusterControllerData->id)
					    .error(e)
					    .detail("GoodRecruitmentTimeReady", clusterControllerData->goodRemoteRecruitmentTime.isReady());
					while (!clusterControllerData->goodRemoteRecruitmentTime.isReady()) {
						wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
					}
				} else {
					TraceEvent(SevError, "RecruitRemoteFromConfigurationError", clusterControllerData->id).error(e);
					throw;
				}
			}
			wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
		}
	}

	ACTOR static Future<Void> clusterRecruitFromConfiguration(Recruiter const* recruiter,
	                                                          ClusterControllerData* clusterControllerData,
	                                                          Reference<RecruitWorkersInfo> req) {
		// At the moment this doesn't really need to be an actor (it always completes immediately)
		CODE_PROBE(true, "ClusterController RecruitTLogsRequest");
		loop {
			try {
				req->rep = clusterControllerData->findWorkersForConfiguration(req->req);
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_no_more_servers && clusterControllerData->goodRecruitmentTime.isReady()) {
					clusterControllerData->outstandingRecruitmentRequests.push_back(req);
					TraceEvent(SevWarn, "RecruitFromConfigurationNotAvailable", clusterControllerData->id).error(e);
					wait(req->waitForCompletion.onTrigger());
					return Void();
				} else if (e.code() == error_code_operation_failed || e.code() == error_code_no_more_servers) {
					// recruitment not good enough, try again
					TraceEvent("RecruitFromConfigurationRetry", clusterControllerData->id)
					    .error(e)
					    .detail("GoodRecruitmentTimeReady", clusterControllerData->goodRecruitmentTime.isReady());
					while (!clusterControllerData->goodRecruitmentTime.isReady()) {
						wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
					}
				} else {
					TraceEvent(SevError, "RecruitFromConfigurationError", clusterControllerData->id).error(e);
					throw;
				}
			}
			wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
		}
	}

	ACTOR static Future<std::vector<Standalone<CommitTransactionRef>>> recruitEverything(
	    Recruiter const* recruiter,
	    Reference<ClusterRecoveryData> clusterRecoveryData,
	    std::vector<StorageServerInterface>* seedServers,
	    Reference<ILogSystem> oldLogSystem) {
		if (!clusterRecoveryData->configuration.isValid()) {
			RecoveryStatus::RecoveryStatus status;
			if (clusterRecoveryData->configuration.initialized) {
				TraceEvent(
				    SevWarn,
				    getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_INVALID_CONFIG_EVENT_NAME).c_str(),
				    clusterRecoveryData->dbgid)
				    .setMaxEventLength(11000)
				    .setMaxFieldLength(10000)
				    .detail("Conf", clusterRecoveryData->configuration.toString());
				status = RecoveryStatus::configuration_invalid;
			} else if (!clusterRecoveryData->cstate.prevDBState.tLogs.size()) {
				status = RecoveryStatus::configuration_never_created;
				clusterRecoveryData->neverCreated = true;
			} else {
				status = RecoveryStatus::configuration_missing;
			}
			TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
			           clusterRecoveryData->dbgid)
			    .detail("StatusCode", status)
			    .detail("Status", RecoveryStatus::names[status])
			    .trackLatest(clusterRecoveryData->clusterRecoveryStateEventHolder->trackingKey);
			return Never();
		} else {
			TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
			           clusterRecoveryData->dbgid)
			    .setMaxFieldLength(-1)
			    .detail("StatusCode", RecoveryStatus::recruiting_transaction_servers)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::recruiting_transaction_servers])
			    .detail("Conf", clusterRecoveryData->configuration.toString())
			    .detail("RequiredCommitProxies", 1)
			    .detail("RequiredGrvProxies", 1)
			    .detail("RequiredResolvers", 1)
			    .trackLatest(clusterRecoveryData->clusterRecoveryStateEventHolder->trackingKey);
			// The cluster's EncryptionAtRest status is now readable.
			if (clusterRecoveryData->controllerData->encryptionAtRestMode.canBeSet()) {
				clusterRecoveryData->controllerData->encryptionAtRestMode.send(
				    recruiter->getEncryptionAtRest(clusterRecoveryData->configuration));
			}
		}

		// FIXME: we only need log routers for the same locality as the master
		int maxLogRouters = clusterRecoveryData->cstate.prevDBState.logRouterTags;
		for (auto& old : clusterRecoveryData->cstate.prevDBState.oldTLogData) {
			maxLogRouters = std::max(maxLogRouters, old.logRouterTags);
		}

		// TODO: Refactor to remove this request
		RecruitFromConfigurationRequest recruitReq(
		    clusterRecoveryData->configuration, clusterRecoveryData->lastEpochEnd == 0, maxLogRouters);
		state Reference<RecruitWorkersInfo> recruitWorkersInfo = makeReference<RecruitWorkersInfo>(recruitReq);
		recruitWorkersInfo->dbgId = clusterRecoveryData->dbgid;
		wait(clusterRecruitFromConfiguration(recruiter, clusterRecoveryData->controllerData, recruitWorkersInfo));
		state RecruitFromConfigurationReply recruits = recruitWorkersInfo->rep;

		std::string primaryDcIds, remoteDcIds;

		clusterRecoveryData->primaryDcId.clear();
		clusterRecoveryData->remoteDcIds.clear();
		if (recruits.dcId.present()) {
			clusterRecoveryData->primaryDcId.push_back(recruits.dcId);
			if (!primaryDcIds.empty()) {
				primaryDcIds += ',';
			}
			primaryDcIds += printable(recruits.dcId);
			if (clusterRecoveryData->configuration.regions.size() > 1) {
				Key remoteDcId = recruits.dcId.get() == clusterRecoveryData->configuration.regions[0].dcId
				                     ? clusterRecoveryData->configuration.regions[1].dcId
				                     : clusterRecoveryData->configuration.regions[0].dcId;
				clusterRecoveryData->remoteDcIds.push_back(remoteDcId);
				if (!remoteDcIds.empty()) {
					remoteDcIds += ',';
				}
				remoteDcIds += printable(remoteDcId);
			}
		}
		clusterRecoveryData->backupWorkers.swap(recruits.backupWorkers);

		TraceEvent(getRecoveryEventName(ClusterRecoveryEventType::CLUSTER_RECOVERY_STATE_EVENT_NAME).c_str(),
		           clusterRecoveryData->dbgid)
		    .detail("StatusCode", RecoveryStatus::initializing_transaction_servers)
		    .detail("Status", RecoveryStatus::names[RecoveryStatus::initializing_transaction_servers])
		    .detail("CommitProxies", recruits.commitProxies.size())
		    .detail("GrvProxies", recruits.grvProxies.size())
		    .detail("TLogs", recruits.tLogs.size())
		    .detail("Resolvers", recruits.resolvers.size())
		    .detail("SatelliteTLogs", recruits.satelliteTLogs.size())
		    .detail("OldLogRouters", recruits.oldLogRouters.size())
		    .detail("StorageServers", recruits.storageServers.size())
		    .detail("BackupWorkers", clusterRecoveryData->backupWorkers.size())
		    .detail("PrimaryDcIds", primaryDcIds)
		    .detail("RemoteDcIds", remoteDcIds)
		    .trackLatest(clusterRecoveryData->clusterRecoveryStateEventHolder->trackingKey);

		// Actually, newSeedServers does both the recruiting and initialization of the seed servers; so if this is a
		// brand new database we are sort of lying that we are past the recruitment phase.  In a perfect world we would
		// split that up so that the recruitment part happens above (in parallel with recruiting the transaction
		// servers?).
		wait(newSeedServers(recruiter, clusterRecoveryData, recruits, seedServers));
		state std::vector<Standalone<CommitTransactionRef>> confChanges;
		wait(newCommitProxies(recruiter, clusterRecoveryData, recruits) &&
		     newGrvProxies(recruiter, clusterRecoveryData, recruits) &&
		     newResolvers(recruiter, clusterRecoveryData, recruits) &&
		     newTLogServers(recruiter, clusterRecoveryData, recruits, oldLogSystem, &confChanges));

		// Update recovery related information to the newly elected sequencer (master) process.
		wait(brokenPromiseToNever(clusterRecoveryData->masterInterface.updateRecoveryData.getReply(
		    UpdateRecoveryDataRequest(clusterRecoveryData->recoveryTransactionVersion,
		                              clusterRecoveryData->lastEpochEnd,
		                              clusterRecoveryData->commitProxies,
		                              clusterRecoveryData->resolvers,
		                              clusterRecoveryData->versionEpoch,
		                              clusterRecoveryData->primaryLocality))));

		return confChanges;
	}
};

Future<std::vector<Standalone<CommitTransactionRef>>> Recruiter::recruitEverything(
    Reference<ClusterRecoveryData> clusterRecoveryData,
    std::vector<StorageServerInterface>* seedServers,
    Reference<ILogSystem> oldLogSystem) const {
	return RecruiterImpl::recruitEverything(this, clusterRecoveryData, seedServers, oldLogSystem);
}

EncryptionAtRestMode Recruiter::getEncryptionAtRest(DatabaseConfiguration config) const {
	TraceEvent(SevDebug, "CREncryptionAtRestMode").detail("Mode", config.encryptionAtRestMode.toString());
	return config.encryptionAtRestMode;
}
