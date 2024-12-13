/*
 * ClusterController.actor.cpp
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

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <vector>

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/BlobRestoreCommon.h"
#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/BlobGranuleServerCommon.actor.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/BackupProgress.actor.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/ClusterRecovery.actor.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/DBCoreState.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/LeaderElection.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/BlobManagerInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/SingletonRoles.h"
#include "fdbserver/Status.actor.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbserver/RecoveryState.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbrpc/sim_validation.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/Util.h"

#include "metacluster/MetaclusterMetrics.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Optional<Value>> getPreviousCoordinators(ClusterControllerData* self) {
	state ReadYourWritesTransaction tr(self->db.db);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Optional<Value> previousCoordinators = wait(tr.get(previousCoordinatorsKey));
			return previousCoordinators;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

bool ClusterControllerData::processesInSameDC(const NetworkAddress& addr1, const NetworkAddress& addr2) const {
	return this->addr_locality.contains(addr1) && this->addr_locality.contains(addr2) &&
	       this->addr_locality.at(addr1).dcId().present() && this->addr_locality.at(addr2).dcId().present() &&
	       this->addr_locality.at(addr1).dcId().get() == this->addr_locality.at(addr2).dcId().get();
}

bool ClusterControllerData::transactionSystemContainsDegradedServers() {
	const ServerDBInfo& dbi = db.serverInfo->get();
	const Reference<ClusterRecoveryData> recoveryData = db.recoveryData;
	auto transactionWorkerInList = [&dbi, &recoveryData](const std::unordered_set<NetworkAddress>& serverList,
	                                                     bool skipSatellite,
	                                                     bool skipRemoteTLog,
	                                                     bool skipRemoteLogRouter) -> bool {
		for (const auto& server : serverList) {
			if (dbi.master.addresses().contains(server)) {
				return true;
			}

			// Getting the current log system config to check if any TLogs are degraded. recoveryData->logSystem has the
			// most up to date log system and we should use it whenever available.
			auto logSystemConfig = (recoveryData.isValid() && recoveryData->logSystem.isValid())
			                           ? recoveryData->logSystem->getLogSystemConfig()
			                           : dbi.logSystemConfig;
			for (const auto& logSet : logSystemConfig.tLogs) {
				if (skipSatellite && logSet.locality == tagLocalitySatellite) {
					continue;
				}

				if (!logSet.isLocal) {
					if (!skipRemoteTLog) {
						for (const auto& tlog : logSet.tLogs) {
							if (tlog.present() && tlog.interf().addresses().contains(server)) {
								return true;
							}
						}
					}
					if (!skipRemoteLogRouter) {
						for (const auto& logRouter : logSet.logRouters) {
							if (logRouter.present() && logRouter.interf().addresses().contains(server)) {
								return true;
							}
						}
					}
				} else {
					for (const auto& tlog : logSet.tLogs) {
						if (tlog.present() && tlog.interf().addresses().contains(server)) {
							return true;
						}
					}
				}
			}

			if (SERVER_KNOBS->GRAY_FAILURE_ENABLE_TLOG_RECOVERY_MONITORING && recoveryData.isValid() &&
			    recoveryData->recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				// During recovery, TLogs may not be able to pull data from previous generation TLogs due to gray
				// failures. In this case, we rely on the latest recruitment information and see if any newly recruited
				// TLogs are degraded.
				for (const auto& tlog : recoveryData->primaryRecruitment.tLogs) {
					if (tlog.addresses().contains(server)) {
						return true;
					}
				}

				if (!skipSatellite) {
					for (const auto& satelliteLog : recoveryData->primaryRecruitment.satelliteTLogs) {
						if (satelliteLog.addresses().contains(server)) {
							return true;
						}
					}
				}
			}

			for (const auto& proxy : dbi.client.grvProxies) {
				if (proxy.addresses().contains(server)) {
					return true;
				}
			}

			for (const auto& proxy : dbi.client.commitProxies) {
				if (proxy.addresses().contains(server)) {
					return true;
				}
			}

			for (const auto& resolver : dbi.resolvers) {
				if (resolver.addresses().contains(server)) {
					return true;
				}
			}
		}
		return false;
	};

	// Check if transaction system contains degraded/disconnected servers. For satellite, we only
	// check for disconnection since the latency between prmary and satellite is across WAN and may not be very
	// stable.
	// TODO: Consider adding satellite latency degradation check and rely on
	//       SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY for accurate health signal
	return transactionWorkerInList(degradationInfo.degradedServers,
	                               /*skipSatellite=*/true,
	                               /*skipRemoteTLog=*/
	                               !(SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY &&
	                                 SERVER_KNOBS->CC_ENABLE_REMOTE_TLOG_DEGRADATION_MONITORING),
	                               /*skipRemoteLogRouter*/
	                               !(SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY &&
	                                 SERVER_KNOBS->CC_ENABLE_REMOTE_LOG_ROUTER_DEGRADATION_MONITORING)) ||
	       transactionWorkerInList(degradationInfo.disconnectedServers,
	                               /*skipSatellite=*/false,
	                               /*skipRemoteTLog=*/!SERVER_KNOBS->CC_ENABLE_REMOTE_TLOG_DISCONNECT_MONITORING,
	                               /*skipRemoteLogRouter*/ !SERVER_KNOBS->CC_ENABLE_REMOTE_LOG_ROUTER_MONITORING);
}

bool ClusterControllerData::remoteTransactionSystemContainsDegradedServers() {
	if (db.config.usableRegions <= 1) {
		return false;
	}

	for (const auto& excludedServer : degradationInfo.degradedServers) {
		if (addressInDbAndRemoteDc(excludedServer, db.serverInfo)) {
			return true;
		}
	}

	for (const auto& excludedServer : degradationInfo.disconnectedServers) {
		if (addressInDbAndRemoteDc(excludedServer, db.serverInfo)) {
			return true;
		}
	}

	return false;
}

ACTOR Future<Void> clusterWatchDatabase(ClusterControllerData* cluster,
                                        ClusterControllerData::DBInfo* db,
                                        ServerCoordinators coordinators) {
	state MasterInterface iMaster;
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> recoveryCore;

	// SOMEDAY: If there is already a non-failed master referenced by zkMasterInfo, use that one until it fails
	// When this someday is implemented, make sure forced failures still cause the master to be recruited again

	loop {
		TraceEvent("CCWDB", cluster->id).log();
		try {
			state double recoveryStart = now();
			state MasterInterface newMaster;
			state Future<Void> collection;

			TraceEvent("CCWDB", cluster->id).detail("Recruiting", "Master");
			wait(recruitNewMaster(cluster, db, std::addressof(newMaster)));

			iMaster = newMaster;

			db->masterRegistrationCount = 0;
			db->recoveryStalled = false;

			auto dbInfo = ServerDBInfo();
			dbInfo.master = iMaster;
			dbInfo.id = deterministicRandom()->randomUniqueID();
			dbInfo.infoGeneration = ++db->dbInfoCount;
			dbInfo.masterLifetime = db->serverInfo->get().masterLifetime;
			++dbInfo.masterLifetime;
			dbInfo.clusterInterface = db->serverInfo->get().clusterInterface;
			dbInfo.distributor = db->serverInfo->get().distributor;
			dbInfo.ratekeeper = db->serverInfo->get().ratekeeper;
			dbInfo.blobManager = db->serverInfo->get().blobManager;
			dbInfo.blobMigrator = db->serverInfo->get().blobMigrator;
			dbInfo.consistencyScan = db->serverInfo->get().consistencyScan;
			dbInfo.latencyBandConfig = db->serverInfo->get().latencyBandConfig;
			dbInfo.myLocality = db->serverInfo->get().myLocality;
			dbInfo.client = ClientDBInfo();
			dbInfo.client.encryptKeyProxy = db->serverInfo->get().client.encryptKeyProxy;
			dbInfo.client.tenantMode = TenantAPI::tenantModeForClusterType(db->clusterType, db->config.tenantMode);
			dbInfo.client.clusterId = db->serverInfo->get().client.clusterId;
			dbInfo.client.clusterType = db->clusterType;
			dbInfo.client.metaclusterName = db->metaclusterName;

			TraceEvent("CCWDB", cluster->id)
			    .detail("NewMaster", dbInfo.master.id().toString())
			    .detail("Lifetime", dbInfo.masterLifetime.toString())
			    .detail("ChangeID", dbInfo.id);
			db->serverInfo->set(dbInfo);

			state Future<Void> spinDelay = delay(
			    SERVER_KNOBS
			        ->MASTER_SPIN_DELAY); // Don't retry cluster recovery more than once per second, but don't delay
			                              // the "first" recovery after more than a second of normal operation

			TraceEvent("CCWDB", cluster->id).detail("Watching", iMaster.id());
			db->recoveryData = makeReference<ClusterRecoveryData>(cluster,
			                                                      db->serverInfo,
			                                                      db->serverInfo->get().master,
			                                                      db->serverInfo->get().masterLifetime,
			                                                      coordinators,
			                                                      db->serverInfo->get().clusterInterface,
			                                                      ""_sr,
			                                                      addActor,
			                                                      db->forceRecovery);

			collection = actorCollection(db->recoveryData->addActor.getFuture());
			recoveryCore = clusterRecoveryCore(db->recoveryData);
			cluster->recentHealthTriggeredRecoveryTime.push(now());

			// Master failure detection is pretty sensitive, but if we are in the middle of a very long recovery we
			// really don't want to have to start over
			loop choose {
				when(wait(recoveryCore)) {}
				when(wait(waitFailureClient(
				              iMaster.waitFailure,
				              db->masterRegistrationCount
				                  ? SERVER_KNOBS->MASTER_FAILURE_REACTION_TIME
				                  : (now() - recoveryStart) * SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY,
				              db->masterRegistrationCount ? -SERVER_KNOBS->MASTER_FAILURE_REACTION_TIME /
				                                                SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY
				                                          : SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY) ||
				          db->forceMasterFailure.onTrigger())) {
					break;
				}
				when(wait(db->serverInfo->onChange())) {}
				when(BackupWorkerDoneRequest req =
				         waitNext(db->serverInfo->get().clusterInterface.notifyBackupWorkerDone.getFuture())) {
					if (db->recoveryData->logSystem.isValid() && db->recoveryData->logSystem->removeBackupWorker(req)) {
						db->recoveryData->registrationTrigger.trigger();
					}
					++db->recoveryData->backupWorkerDoneRequests;
					req.reply.send(Void());
					TraceEvent(SevDebug, "BackupWorkerDoneRequest", cluster->id).log();
				}
				when(wait(collection)) {
					throw internal_error();
				}
			}
			// failed master (better master exists) could happen while change-coordinators request processing is
			// in-progress
			if (cluster->shouldCommitSuicide) {
				throw restart_cluster_controller();
			}

			recoveryCore.cancel();
			wait(cleanupRecoveryActorCollection(db->recoveryData, /*exThrown=*/false));
			ASSERT(addActor.isEmpty());

			wait(spinDelay);

			CODE_PROBE(true, "clusterWatchDatabase() master failed");
			TraceEvent(SevWarn, "DetectedFailedRecovery", cluster->id).detail("OldMaster", iMaster.id());
		} catch (Error& e) {
			state Error err = e;
			TraceEvent("CCWDB", cluster->id).errorUnsuppressed(e).detail("Master", iMaster.id());
			if (e.code() != error_code_actor_cancelled)
				wait(delay(0.0));

			recoveryCore.cancel();
			wait(cleanupRecoveryActorCollection(db->recoveryData, /*exThrown=*/true));
			ASSERT(addActor.isEmpty());
			if (cluster->outstandingRemoteRequestChecker.isValid()) {
				cluster->outstandingRemoteRequestChecker.cancel();
			}

			if (cluster->outstandingRequestChecker.isValid()) {
				cluster->outstandingRequestChecker.cancel();
			}

			CODE_PROBE(err.code() == error_code_tlog_failed, "Terminated due to tLog failure");
			CODE_PROBE(err.code() == error_code_commit_proxy_failed, "Terminated due to commit proxy failure");
			CODE_PROBE(err.code() == error_code_grv_proxy_failed, "Terminated due to GRV proxy failure");
			CODE_PROBE(err.code() == error_code_resolver_failed, "Terminated due to resolver failure");
			CODE_PROBE(err.code() == error_code_backup_worker_failed, "Terminated due to backup worker failure");
			CODE_PROBE(err.code() == error_code_operation_failed,
			           "Terminated due to failed operation",
			           probe::decoration::rare);
			CODE_PROBE(err.code() == error_code_restart_cluster_controller,
			           "Terminated due to cluster-controller restart.");

			if (cluster->shouldCommitSuicide || err.code() == error_code_coordinators_changed) {
				TraceEvent("ClusterControllerTerminate", cluster->id).errorUnsuppressed(err);
				throw restart_cluster_controller();
			}

			if (isNormalClusterRecoveryError(err)) {
				TraceEvent(SevWarn, "ClusterRecoveryRetrying", cluster->id).error(err);
			} else {
				bool ok = err.code() == error_code_no_more_servers;
				TraceEvent(ok ? SevWarn : SevError, "ClusterWatchDatabaseRetrying", cluster->id).error(err);
				if (!ok)
					throw err;
			}
			wait(delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
		}
	}
}

ACTOR Future<Void> clusterGetServerInfo(ClusterControllerData::DBInfo* db,
                                        UID knownServerInfoID,
                                        ReplyPromise<ServerDBInfo> reply) {
	while (db->serverInfo->get().id == knownServerInfoID) {
		choose {
			when(wait(yieldedFuture(db->serverInfo->onChange()))) {}
			when(wait(delayJittered(300))) {
				break;
			} // The server might be long gone!
		}
	}
	reply.send(db->serverInfo->get());
	return Void();
}

ACTOR Future<Void> clusterOpenDatabase(ClusterControllerData::DBInfo* db, OpenDatabaseRequest req) {
	db->clientStatus[req.reply.getEndpoint().getPrimaryAddress()] = std::make_pair(now(), req);
	if (db->clientStatus.size() > 10000) {
		TraceEvent(SevWarnAlways, "TooManyClientStatusEntries").suppressFor(1.0);
	}

	while (db->clientInfo->get().id == req.knownClientInfoID) {
		choose {
			when(wait(db->clientInfo->onChange())) {}
			when(wait(delayJittered(SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL))) {
				break;
			} // The client might be long gone!
		}
	}

	req.reply.send(db->clientInfo->get());
	return Void();
}

void checkOutstandingRecruitmentRequests(ClusterControllerData* self) {
	for (int i = 0; i < self->outstandingRecruitmentRequests.size(); i++) {
		Reference<RecruitWorkersInfo> info = self->outstandingRecruitmentRequests[i];
		try {
			info->rep = self->findWorkersForConfiguration(info->req);
			if (info->dbgId.present()) {
				TraceEvent("CheckOutstandingRecruitment", info->dbgId.get())
				    .detail("Request", info->req.configuration.toString());
			}
			info->waitForCompletion.trigger();
			swapAndPop(&self->outstandingRecruitmentRequests, i--);
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers || e.code() == error_code_operation_failed) {
				TraceEvent(SevWarn, "RecruitTLogMatchingSetNotAvailable", self->id).error(e);
			} else {
				TraceEvent(SevError, "RecruitTLogsRequestError", self->id).error(e);
				throw;
			}
		}
	}
}

void checkOutstandingRemoteRecruitmentRequests(ClusterControllerData* self) {
	for (int i = 0; i < self->outstandingRemoteRecruitmentRequests.size(); i++) {
		Reference<RecruitRemoteWorkersInfo> info = self->outstandingRemoteRecruitmentRequests[i];
		try {
			info->rep = self->findRemoteWorkersForConfiguration(info->req);
			if (info->dbgId.present()) {
				TraceEvent("CheckOutstandingRemoteRecruitment", info->dbgId.get())
				    .detail("Request", info->req.configuration.toString());
			}
			info->waitForCompletion.trigger();
			swapAndPop(&self->outstandingRemoteRecruitmentRequests, i--);
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers || e.code() == error_code_operation_failed) {
				TraceEvent(SevWarn, "RecruitRemoteTLogMatchingSetNotAvailable", self->id).error(e);
			} else {
				TraceEvent(SevError, "RecruitRemoteTLogsRequestError", self->id).error(e);
				throw;
			}
		}
	}
}

void checkOutstandingStorageRequests(ClusterControllerData* self) {
	for (int i = 0; i < self->outstandingStorageRequests.size(); i++) {
		auto& req = self->outstandingStorageRequests[i];
		try {
			if (req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop(&self->outstandingStorageRequests, i--);
			} else {
				if (!self->gotProcessClasses && !req.first.criticalRecruitment)
					throw no_more_servers();

				auto worker = self->getStorageWorker(req.first);
				RecruitStorageReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send(rep);
				swapAndPop(&self->outstandingStorageRequests, i--);
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitStorageNotAvailable", self->id)
				    .errorUnsuppressed(e)
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i)
				    .detail("IsCriticalRecruitment", req.first.criticalRecruitment);
			} else {
				TraceEvent(SevError, "RecruitStorageError", self->id).error(e);
				throw;
			}
		}
	}
}

// When workers aren't available at the time of request, the request
// gets added to a list of outstanding reqs. Here, we try to resolve these
// outstanding requests.
void checkOutstandingBlobWorkerRequests(ClusterControllerData* self) {
	for (int i = 0; i < self->outstandingBlobWorkerRequests.size(); i++) {
		auto& req = self->outstandingBlobWorkerRequests[i];
		try {
			if (req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop(&self->outstandingBlobWorkerRequests, i--);
			} else {
				if (!self->gotProcessClasses)
					throw no_more_servers();

				auto worker = self->getBlobWorker(req.first);
				RecruitBlobWorkerReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send(rep);
				// can remove it once we know the worker was found
				swapAndPop(&self->outstandingBlobWorkerRequests, i--);
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitBlobWorkerNotAvailable", self->id)
				    .errorUnsuppressed(e)
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i);
			} else {
				TraceEvent(SevError, "RecruitBlobWorkerError", self->id).error(e);
				throw;
			}
		}
	}
}

// Finds and returns a new process for role
WorkerDetails findNewProcessForSingleton(ClusterControllerData* self,
                                         const ProcessClass::ClusterRole role,
                                         std::map<Optional<Standalone<StringRef>>, int>& id_used) {
	// find new process in cluster for role
	WorkerDetails newWorker =
	    self->getWorkerForRoleInDatacenter(
	            self->clusterControllerDcId, role, ProcessClass::NeverAssign, self->db.config, id_used, {}, true)
	        .worker;

	// check if master's process is actually better suited for role
	if (self->onMasterIsBetter(newWorker, role)) {
		newWorker = self->id_worker[self->masterProcessId.get()].details;
	}

	// acknowledge that the pid is now potentially used by this role as well
	id_used[newWorker.interf.locality.processId()]++;

	return newWorker;
}

// Return best possible fitness for singleton. Note that lower fitness is better.
ProcessClass::Fitness findBestFitnessForSingleton(const ClusterControllerData* self,
                                                  const WorkerDetails& worker,
                                                  const ProcessClass::ClusterRole& role) {
	auto bestFitness = worker.processClass.machineClassFitness(role);
	// If the process has been marked as excluded, we take the max with ExcludeFit to ensure its fit
	// is at least as bad as ExcludeFit. This assists with successfully offboarding such processes
	// and removing them from the cluster.
	if (self->db.config.isExcludedServer(worker.interf.addresses(), worker.interf.locality)) {
		bestFitness = std::max(bestFitness, ProcessClass::ExcludeFit);
	}
	return bestFitness;
}

// Returns true iff the singleton is healthy. "Healthy" here means that
// the singleton is stable (see below) and doesn't need to be rerecruited.
// Side effects: (possibly) initiates recruitment
template <class SingletonClass>
bool isHealthySingleton(ClusterControllerData* self,
                        const WorkerDetails& newWorker,
                        const SingletonClass& singleton,
                        const ProcessClass::Fitness& bestFitness,
                        const Optional<UID> recruitingID) {
	// A singleton is stable if it exists in cluster, has not been killed off of proc and is not being recruited
	bool isStableSingleton = singleton.isPresent() &&
	                         self->id_worker.contains(singleton.getInterface().locality.processId()) &&
	                         (!recruitingID.present() || (recruitingID.get() == singleton.getInterface().id()));

	if (!isStableSingleton) {
		return false; // not healthy because unstable
	}

	auto& currWorker = self->id_worker[singleton.getInterface().locality.processId()];
	auto currFitness = currWorker.details.processClass.machineClassFitness(singleton.getClusterRole());
	if (currWorker.priorityInfo.isExcluded) {
		currFitness = ProcessClass::ExcludeFit;
	}
	// If any of the following conditions are met, we will switch the singleton's process:
	// - if the current proc is used by some non-master, non-singleton role
	// - if the current fitness is less than optimal (lower fitness is better)
	// - if currently at peak fitness but on same process as master, and the new worker is on different process
	bool shouldRerecruit =
	    self->isUsedNotMaster(currWorker.details.interf.locality.processId()) || bestFitness < currFitness ||
	    (currFitness == bestFitness && currWorker.details.interf.locality.processId() == self->masterProcessId &&
	     newWorker.interf.locality.processId() != self->masterProcessId);
	if (shouldRerecruit) {
		std::string roleAbbr = singleton.getRole().abbreviation;
		TraceEvent(("CCHalt" + roleAbbr).c_str(), self->id)
		    .detail(roleAbbr + "ID", singleton.getInterface().id())
		    .detail("Excluded", currWorker.priorityInfo.isExcluded)
		    .detail("Fitness", currFitness)
		    .detail("BestFitness", bestFitness)
		    .detail("MasterProcessId", self->masterProcessId)
		    .detail("CurrentWorkerProcessId", currWorker.details.interf.locality.processId())
		    .detail("NewWorkerProcessId", newWorker.interf.locality.processId())
		    .detail("IsUsedNotMaster", self->isUsedNotMaster(currWorker.details.interf.locality.processId()));
		singleton.recruit(*self); // SIDE EFFECT: initiating recruitment
		return false; // not healthy since needed to be rerecruited
	} else {
		return true; // healthy because doesn't need to be rerecruited
	}
}

// Returns a mapping from pid->pidCount for pids
std::map<Optional<Standalone<StringRef>>, int> getColocCounts(
    const std::vector<Optional<Standalone<StringRef>>>& pids) {
	std::map<Optional<Standalone<StringRef>>, int> counts;
	for (const auto& pid : pids) {
		if (pid.present()) {
			++counts[pid];
		}
	}
	return counts;
}

// Checks if there exists a better process for each singleton (e.g. DD) compared
// to the process it is currently on.
// Note: there is a lot of extra logic here to only recruit the blob manager when gate is open.
// When adding new singletons, just follow the ratekeeper/data distributor examples.
void checkBetterSingletons(ClusterControllerData* self) {
	if (!self->masterProcessId.present() ||
	    self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		return;
	}

	// note: this map doesn't consider pids used by existing singletons
	std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();

	// We prefer spreading out other roles more than separating singletons on their own process
	// so we artificially amplify the pid count for the processes used by non-singleton roles.
	// In other words, we make the processes used for other roles less desirable to be used
	// by singletons as well.
	for (auto& it : id_used) {
		it.second *= PID_USED_AMP_FOR_NON_SINGLETON;
	}

	// Try to find a new process for each singleton.
	WorkerDetails newRKWorker = findNewProcessForSingleton(self, ProcessClass::Ratekeeper, id_used);
	WorkerDetails newDDWorker = findNewProcessForSingleton(self, ProcessClass::DataDistributor, id_used);
	WorkerDetails newCSWorker = findNewProcessForSingleton(self, ProcessClass::ConsistencyScan, id_used);

	WorkerDetails newBMWorker;
	WorkerDetails newMGWorker;
	if (self->db.blobGranulesEnabled.get()) {
		newBMWorker = findNewProcessForSingleton(self, ProcessClass::BlobManager, id_used);
		if (self->db.blobRestoreEnabled.get()) {
			newMGWorker = findNewProcessForSingleton(self, ProcessClass::BlobMigrator, id_used);
		}
	}

	WorkerDetails newEKPWorker;
	EncryptionAtRestMode encryptMode = self->db.config.encryptionAtRestMode;
	const bool enableKmsCommunication =
	    encryptMode.isEncryptionEnabled() || SERVER_KNOBS->ENABLE_REST_KMS_COMMUNICATION;
	if (enableKmsCommunication) {
		newEKPWorker = findNewProcessForSingleton(self, ProcessClass::EncryptKeyProxy, id_used);
	}

	// Find best possible fitnesses for each singleton.
	auto bestFitnessForRK = findBestFitnessForSingleton(self, newRKWorker, ProcessClass::Ratekeeper);
	auto bestFitnessForDD = findBestFitnessForSingleton(self, newDDWorker, ProcessClass::DataDistributor);
	auto bestFitnessForCS = findBestFitnessForSingleton(self, newCSWorker, ProcessClass::ConsistencyScan);

	ProcessClass::Fitness bestFitnessForBM;
	ProcessClass::Fitness bestFitnessForMG;
	if (self->db.blobGranulesEnabled.get()) {
		bestFitnessForBM = findBestFitnessForSingleton(self, newBMWorker, ProcessClass::BlobManager);
		if (self->db.blobRestoreEnabled.get()) {
			bestFitnessForMG = findBestFitnessForSingleton(self, newMGWorker, ProcessClass::BlobManager);
		}
	}

	ProcessClass::Fitness bestFitnessForEKP;
	if (enableKmsCommunication) {
		bestFitnessForEKP = findBestFitnessForSingleton(self, newEKPWorker, ProcessClass::EncryptKeyProxy);
	}

	auto& db = self->db.serverInfo->get();
	auto rkSingleton = RatekeeperSingleton(db.ratekeeper);
	auto ddSingleton = DataDistributorSingleton(db.distributor);
	ConsistencyScanSingleton csSingleton(db.consistencyScan);
	BlobManagerSingleton bmSingleton(db.blobManager);
	BlobMigratorSingleton mgSingleton(db.blobMigrator);
	EncryptKeyProxySingleton ekpSingleton(db.client.encryptKeyProxy);

	// Check if the singletons are healthy.
	// side effect: try to rerecruit the singletons to more optimal processes
	bool rkHealthy = isHealthySingleton<RatekeeperSingleton>(
	    self, newRKWorker, rkSingleton, bestFitnessForRK, self->recruitingRatekeeperID);

	bool ddHealthy = isHealthySingleton<DataDistributorSingleton>(
	    self, newDDWorker, ddSingleton, bestFitnessForDD, self->recruitingDistributorID);

	bool csHealthy = isHealthySingleton<ConsistencyScanSingleton>(
	    self, newCSWorker, csSingleton, bestFitnessForCS, self->recruitingConsistencyScanID);

	bool bmHealthy = true;
	bool mgHealthy = true;
	if (self->db.blobGranulesEnabled.get()) {
		bmHealthy = isHealthySingleton<BlobManagerSingleton>(
		    self, newBMWorker, bmSingleton, bestFitnessForBM, self->recruitingBlobManagerID);
		if (self->db.blobRestoreEnabled.get()) {
			mgHealthy = isHealthySingleton<BlobMigratorSingleton>(
			    self, newMGWorker, mgSingleton, bestFitnessForMG, self->recruitingBlobMigratorID);
		}
	}

	bool ekpHealthy = true;
	if (enableKmsCommunication) {
		ekpHealthy = isHealthySingleton<EncryptKeyProxySingleton>(
		    self, newEKPWorker, ekpSingleton, bestFitnessForEKP, self->recruitingEncryptKeyProxyID);
	}
	// if any of the singletons are unhealthy (rerecruited or not stable), then do not
	// consider any further re-recruitments
	if (!(rkHealthy && ddHealthy && bmHealthy && ekpHealthy && csHealthy && mgHealthy)) {
		return;
	}

	// if we reach here, we know that the singletons are healthy so let's
	// check if we can colocate the singletons in a more optimal way
	Optional<Standalone<StringRef>> currRKProcessId = rkSingleton.getInterface().locality.processId();
	Optional<Standalone<StringRef>> currDDProcessId = ddSingleton.getInterface().locality.processId();
	Optional<Standalone<StringRef>> currCSProcessId = csSingleton.getInterface().locality.processId();
	Optional<Standalone<StringRef>> newRKProcessId = newRKWorker.interf.locality.processId();
	Optional<Standalone<StringRef>> newDDProcessId = newDDWorker.interf.locality.processId();
	Optional<Standalone<StringRef>> newCSProcessId = newCSWorker.interf.locality.processId();

	Optional<Standalone<StringRef>> currBMProcessId, newBMProcessId;
	Optional<Standalone<StringRef>> currMGProcessId, newMGProcessId;
	if (self->db.blobGranulesEnabled.get()) {
		currBMProcessId = bmSingleton.getInterface().locality.processId();
		newBMProcessId = newBMWorker.interf.locality.processId();
		if (self->db.blobRestoreEnabled.get()) {
			currMGProcessId = mgSingleton.getInterface().locality.processId();
			newMGProcessId = newMGWorker.interf.locality.processId();
		}
	}

	Optional<Standalone<StringRef>> currEKPProcessId, newEKPProcessId;
	if (enableKmsCommunication) {
		currEKPProcessId = ekpSingleton.getInterface().locality.processId();
		newEKPProcessId = newEKPWorker.interf.locality.processId();
	}

	std::vector<Optional<Standalone<StringRef>>> currPids = { currRKProcessId, currDDProcessId, currCSProcessId };
	std::vector<Optional<Standalone<StringRef>>> newPids = { newRKProcessId, newDDProcessId, newCSProcessId };
	if (self->db.blobGranulesEnabled.get()) {
		currPids.emplace_back(currBMProcessId);
		newPids.emplace_back(newBMProcessId);
		if (self->db.blobRestoreEnabled.get()) {
			currPids.emplace_back(currMGProcessId);
			newPids.emplace_back(newMGProcessId);
		}
	}

	if (enableKmsCommunication) {
		currPids.emplace_back(currEKPProcessId);
		newPids.emplace_back(newEKPProcessId);
	}

	auto currColocMap = getColocCounts(currPids);
	auto newColocMap = getColocCounts(newPids);

	// if the knob is disabled, the BM coloc counts should have no affect on the coloc counts check below
	if (!self->db.blobGranulesEnabled.get()) {
		ASSERT(currColocMap[currBMProcessId] == 0);
		ASSERT(newColocMap[newBMProcessId] == 0);
		if (self->db.blobRestoreEnabled.get()) {
			ASSERT(currColocMap[currMGProcessId] == 0);
			ASSERT(newColocMap[newMGProcessId] == 0);
		}
	}

	// if the knob is disabled, the EKP coloc counts should have no affect on the coloc counts check below
	if (!enableKmsCommunication) {
		ASSERT(currColocMap[currEKPProcessId] == 0);
		ASSERT(newColocMap[newEKPProcessId] == 0);
	}

	// if the new coloc counts are collectively better (i.e. each singleton's coloc count has not increased)
	if (newColocMap[newRKProcessId] <= currColocMap[currRKProcessId] &&
	    newColocMap[newDDProcessId] <= currColocMap[currDDProcessId] &&
	    newColocMap[newBMProcessId] <= currColocMap[currBMProcessId] &&
	    newColocMap[newMGProcessId] <= currColocMap[currMGProcessId] &&
	    newColocMap[newEKPProcessId] <= currColocMap[currEKPProcessId] &&
	    newColocMap[newCSProcessId] <= currColocMap[currCSProcessId]) {
		// rerecruit the singleton for which we have found a better process, if any
		if (newColocMap[newRKProcessId] < currColocMap[currRKProcessId]) {
			rkSingleton.recruit(*self);
		} else if (newColocMap[newDDProcessId] < currColocMap[currDDProcessId]) {
			ddSingleton.recruit(*self);
		} else if (self->db.blobGranulesEnabled.get() && newColocMap[newBMProcessId] < currColocMap[currBMProcessId]) {
			bmSingleton.recruit(*self);
		} else if (self->db.blobGranulesEnabled.get() && self->db.blobRestoreEnabled.get() &&
		           newColocMap[newMGProcessId] < currColocMap[currMGProcessId]) {
			mgSingleton.recruit(*self);
		} else if (enableKmsCommunication && newColocMap[newEKPProcessId] < currColocMap[currEKPProcessId]) {
			ekpSingleton.recruit(*self);
		} else if (newColocMap[newCSProcessId] < currColocMap[currCSProcessId]) {
			csSingleton.recruit(*self);
		}
	}
}

ACTOR Future<Void> doCheckOutstandingRequests(ClusterControllerData* self) {
	try {
		wait(delay(SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL));
		while (now() - self->lastRecruitTime < SERVER_KNOBS->SINGLETON_RECRUIT_BME_DELAY ||
		       !self->goodRecruitmentTime.isReady()) {
			if (now() - self->lastRecruitTime < SERVER_KNOBS->SINGLETON_RECRUIT_BME_DELAY) {
				wait(delay(SERVER_KNOBS->SINGLETON_RECRUIT_BME_DELAY + 0.001 - (now() - self->lastRecruitTime)));
			}
			if (!self->goodRecruitmentTime.isReady()) {
				wait(self->goodRecruitmentTime);
			}
		}

		checkOutstandingRecruitmentRequests(self);
		checkOutstandingStorageRequests(self);

		if (self->db.blobGranulesEnabled.get()) {
			checkOutstandingBlobWorkerRequests(self);
		}
		checkBetterSingletons(self);

		self->checkRecoveryStalled();
		if (self->betterMasterExists()) {
			self->db.forceMasterFailure.trigger();
			TraceEvent("MasterRegistrationKill", self->id).detail("MasterId", self->db.serverInfo->get().master.id());
		}
	} catch (Error& e) {
		if (e.code() != error_code_no_more_servers) {
			TraceEvent(SevError, "CheckOutstandingError").error(e);
		}
	}
	return Void();
}

ACTOR Future<Void> doCheckOutstandingRemoteRequests(ClusterControllerData* self) {
	try {
		wait(delay(SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL));
		while (!self->goodRemoteRecruitmentTime.isReady()) {
			wait(self->goodRemoteRecruitmentTime);
		}

		checkOutstandingRemoteRecruitmentRequests(self);
	} catch (Error& e) {
		if (e.code() != error_code_no_more_servers) {
			TraceEvent(SevError, "CheckOutstandingError").error(e);
		}
	}
	return Void();
}

void checkOutstandingRequests(ClusterControllerData* self) {
	if (self->outstandingRemoteRequestChecker.isReady()) {
		self->outstandingRemoteRequestChecker = doCheckOutstandingRemoteRequests(self);
	}

	if (self->outstandingRequestChecker.isReady()) {
		self->outstandingRequestChecker = doCheckOutstandingRequests(self);
	}
}

ACTOR Future<Void> rebootAndCheck(ClusterControllerData* cluster, Optional<Standalone<StringRef>> processID) {
	{
		ASSERT(processID.present());
		auto watcher = cluster->id_worker.find(processID);
		ASSERT(watcher != cluster->id_worker.end());

		watcher->second.reboots++;
		wait(delay(g_network->isSimulated() ? SERVER_KNOBS->SIM_SHUTDOWN_TIMEOUT : SERVER_KNOBS->SHUTDOWN_TIMEOUT));
	}

	{
		auto watcher = cluster->id_worker.find(processID);
		if (watcher != cluster->id_worker.end()) {
			watcher->second.reboots--;
			if (watcher->second.reboots < 2)
				checkOutstandingRequests(cluster);
		}
	}

	return Void();
}

ACTOR Future<Void> workerAvailabilityWatch(WorkerInterface worker,
                                           ProcessClass startingClass,
                                           ClusterControllerData* cluster) {
	state Future<Void> failed = (worker.address() == g_network->getLocalAddress())
	                                ? Never()
	                                : waitFailureClient(worker.waitFailure, SERVER_KNOBS->WORKER_FAILURE_TIME);
	cluster->updateWorkerList.set(worker.locality.processId(),
	                              ProcessData(worker.locality, startingClass, worker.stableAddress()));
	// This switching avoids a race where the worker can be added to id_worker map after the workerAvailabilityWatch
	// fails for the worker.
	wait(delay(0));

	loop {
		choose {
			when(wait(IFailureMonitor::failureMonitor().onStateEqual(
			    worker.storage.getEndpoint(),
			    FailureStatus(
			        IFailureMonitor::failureMonitor().getState(worker.storage.getEndpoint()).isAvailable())))) {
				if (IFailureMonitor::failureMonitor().getState(worker.storage.getEndpoint()).isAvailable()) {
					cluster->ac.add(rebootAndCheck(cluster, worker.locality.processId()));
					checkOutstandingRequests(cluster);
				}
			}
			when(wait(failed)) { // remove workers that have failed
				WorkerInfo& failedWorkerInfo = cluster->id_worker[worker.locality.processId()];

				if (!failedWorkerInfo.reply.isSet()) {
					failedWorkerInfo.reply.send(
					    RegisterWorkerReply(failedWorkerInfo.details.processClass, failedWorkerInfo.priorityInfo));
				}
				if (worker.locality.processId() == cluster->masterProcessId) {
					cluster->masterProcessId = Optional<Key>();
				}
				TraceEvent("ClusterControllerWorkerFailed", cluster->id)
				    .detail("ProcessId", worker.locality.processId())
				    .detail("ProcessClass", failedWorkerInfo.details.processClass.toString())
				    .detail("Address", worker.address());
				cluster->removedDBInfoEndpoints.insert(worker.updateServerDBInfo.getEndpoint());
				cluster->id_worker.erase(worker.locality.processId());
				// Currently, only CC_ONLY_CONSIDER_INTRA_DC_LATENCY feature relies on addr_locality mapping. In the
				// future, if needed, we can populate the mapping unconditionally.
				if (SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY) {
					cluster->addr_locality.erase(worker.address());
					if (worker.secondaryAddress().present()) {
						cluster->addr_locality.erase(worker.secondaryAddress().get());
					}
				}
				cluster->updateWorkerList.set(worker.locality.processId(), Optional<ProcessData>());
				return Void();
			}
		}
	}
}

struct FailureStatusInfo {
	FailureStatus status;
	double lastRequestTime;
	double penultimateRequestTime;

	FailureStatusInfo() : lastRequestTime(0), penultimateRequestTime(0) {}

	void insertRequest(double now) {
		penultimateRequestTime = lastRequestTime;
		lastRequestTime = now;
	}

	double latency(double now) const {
		return std::max(now - lastRequestTime, lastRequestTime - penultimateRequestTime);
	}
};

ACTOR Future<std::vector<TLogInterface>> requireAll(std::vector<Future<Optional<std::vector<TLogInterface>>>> in) {
	state std::vector<TLogInterface> out;
	state int i;
	for (i = 0; i < in.size(); i++) {
		Optional<std::vector<TLogInterface>> x = wait(in[i]);
		if (!x.present())
			throw recruitment_failed();
		out.insert(out.end(), x.get().begin(), x.get().end());
	}
	return out;
}

void clusterRecruitStorage(ClusterControllerData* self, RecruitStorageRequest req) {
	try {
		if (!self->gotProcessClasses && !req.criticalRecruitment)
			throw no_more_servers();
		auto worker = self->getStorageWorker(req);
		RecruitStorageReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send(rep);
	} catch (Error& e) {
		if (e.code() == error_code_no_more_servers) {
			self->outstandingStorageRequests.emplace_back(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT);
			TraceEvent(SevWarn, "RecruitStorageNotAvailable", self->id)
			    .error(e)
			    .detail("IsCriticalRecruitment", req.criticalRecruitment);
		} else {
			TraceEvent(SevError, "RecruitStorageError", self->id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

// Tries to send a reply to req with a worker (process) that a blob worker can be recruited on
// Otherwise, add the req to a list of outstanding reqs that will eventually be dealt with
void clusterRecruitBlobWorker(ClusterControllerData* self, RecruitBlobWorkerRequest req) {
	try {
		if (!self->gotProcessClasses)
			throw no_more_servers();
		auto worker = self->getBlobWorker(req);
		RecruitBlobWorkerReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send(rep);
	} catch (Error& e) {
		if (e.code() == error_code_no_more_servers) {
			self->outstandingBlobWorkerRequests.emplace_back(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT);
			TraceEvent(SevWarn, "RecruitBlobWorkerNotAvailable", self->id).error(e);
		} else {
			TraceEvent(SevError, "RecruitBlobWorkerError", self->id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

void clusterRegisterMaster(ClusterControllerData* self, RegisterMasterRequest const& req) {
	req.reply.send(Void());

	TraceEvent("MasterRegistrationReceived", self->id)
	    .detail("MasterId", req.id)
	    .detail("Master", req.mi.toString())
	    .detail("Tlogs", describe(req.logSystemConfig.tLogs))
	    .detail("Resolvers", req.resolvers.size())
	    .detail("RecoveryState", (int)req.recoveryState)
	    .detail("RegistrationCount", req.registrationCount)
	    .detail("CommitProxies", req.commitProxies.size())
	    .detail("GrvProxies", req.grvProxies.size())
	    .detail("RecoveryCount", req.recoveryCount)
	    .detail("Stalled", req.recoveryStalled)
	    .detail("OldestBackupEpoch", req.logSystemConfig.oldestBackupEpoch);

	// make sure the request comes from an active database
	auto db = &self->db;
	if (db->serverInfo->get().master.id() != req.id || req.registrationCount <= db->masterRegistrationCount) {
		TraceEvent("MasterRegistrationNotFound", self->id)
		    .detail("MasterId", req.id)
		    .detail("ExistingId", db->serverInfo->get().master.id())
		    .detail("RegCount", req.registrationCount)
		    .detail("ExistingRegCount", db->masterRegistrationCount);
		return;
	}

	if (req.recoveryState == RecoveryState::FULLY_RECOVERED) {
		self->db.unfinishedRecoveries = 0;
		ASSERT(!req.logSystemConfig.oldTLogs.size());
	}

	db->masterRegistrationCount = req.registrationCount;
	db->recoveryStalled = req.recoveryStalled;
	if (req.configuration.present()) {
		db->config = req.configuration.get();

		if (req.recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
			self->gotFullyRecoveredConfig = true;
			db->fullyRecoveredConfig = req.configuration.get();
			for (auto& it : self->id_worker) {
				bool isExcludedFromConfig = db->fullyRecoveredConfig.isExcludedServer(
				    it.second.details.interf.addresses(), it.second.details.interf.locality);
				if (it.second.priorityInfo.isExcluded != isExcludedFromConfig) {
					it.second.priorityInfo.isExcluded = isExcludedFromConfig;
					if (!it.second.reply.isSet()) {
						it.second.reply.send(
						    RegisterWorkerReply(it.second.details.processClass, it.second.priorityInfo));
					}
				}
			}
		}
	}

	bool isChanged = false;
	auto dbInfo = self->db.serverInfo->get();

	if (dbInfo.recoveryState != req.recoveryState) {
		dbInfo.recoveryState = req.recoveryState;
		isChanged = true;
	}

	if (dbInfo.priorCommittedLogServers != req.priorCommittedLogServers) {
		dbInfo.priorCommittedLogServers = req.priorCommittedLogServers;
		isChanged = true;
	}

	// Construct the client information
	if (db->clientInfo->get().commitProxies != req.commitProxies ||
	    db->clientInfo->get().grvProxies != req.grvProxies ||
	    db->clientInfo->get().tenantMode != db->config.tenantMode ||
	    db->clientInfo->get().clusterId != db->serverInfo->get().client.clusterId ||
	    db->clientInfo->get().clusterType != db->clusterType ||
	    db->clientInfo->get().metaclusterName != db->metaclusterName) {
		TraceEvent("PublishNewClientInfo", self->id)
		    .detail("Master", dbInfo.master.id())
		    .detail("GrvProxies", db->clientInfo->get().grvProxies)
		    .detail("ReqGrvProxies", req.grvProxies)
		    .detail("CommitProxies", db->clientInfo->get().commitProxies)
		    .detail("GlobalConfigHistorySize", db->clientInfo->get().history.size())
		    .detail("ReqCPs", req.commitProxies)
		    .detail("TenantMode", db->clientInfo->get().tenantMode.toString())
		    .detail("ReqTenantMode", db->config.tenantMode.toString())
		    .detail("ClusterId", db->serverInfo->get().client.clusterId)
		    .detail("ClientClusterId", db->clientInfo->get().clusterId)
		    .detail("ClusterType", db->clientInfo->get().clusterType)
		    .detail("ReqClusterType", db->clusterType)
		    .detail("MetaclusterName", db->clientInfo->get().metaclusterName)
		    .detail("ReqMetaclusterName", db->metaclusterName);
		isChanged = true;
		// TODO why construct a new one and not just copy the old one and change proxies + id?
		ClientDBInfo clientInfo;
		clientInfo.encryptKeyProxy = db->serverInfo->get().client.encryptKeyProxy;
		clientInfo.id = deterministicRandom()->randomUniqueID();
		clientInfo.commitProxies = req.commitProxies;
		clientInfo.grvProxies = req.grvProxies;
		clientInfo.history = db->clientInfo->get().history;
		clientInfo.tenantMode = TenantAPI::tenantModeForClusterType(db->clusterType, db->config.tenantMode);
		clientInfo.clusterId = db->serverInfo->get().client.clusterId;
		clientInfo.clusterType = db->clusterType;
		clientInfo.metaclusterName = db->metaclusterName;
		db->clientInfo->set(clientInfo);
		dbInfo.client = db->clientInfo->get();
	}

	if (!dbInfo.logSystemConfig.isEqual(req.logSystemConfig)) {
		isChanged = true;
		dbInfo.logSystemConfig = req.logSystemConfig;
	}

	if (dbInfo.resolvers != req.resolvers) {
		isChanged = true;
		dbInfo.resolvers = req.resolvers;
	}

	if (dbInfo.recoveryCount != req.recoveryCount) {
		isChanged = true;
		dbInfo.recoveryCount = req.recoveryCount;
	}

	if (isChanged) {
		dbInfo.id = deterministicRandom()->randomUniqueID();
		dbInfo.infoGeneration = ++self->db.dbInfoCount;
		self->db.serverInfo->set(dbInfo);
	}

	checkOutstandingRequests(self);
}

// Halts the registering (i.e. requesting) singleton if one is already in the process of being recruited
// or, halts the existing singleton in favour of the requesting one
template <class SingletonClass>
void haltRegisteringOrCurrentSingleton(ClusterControllerData* self,
                                       const WorkerInterface& worker,
                                       const SingletonClass& currSingleton,
                                       const SingletonClass& registeringSingleton,
                                       const Optional<UID> recruitingID) {
	ASSERT(currSingleton.getRole() == registeringSingleton.getRole());
	const UID registeringID = registeringSingleton.getInterface().id();
	const std::string roleName = currSingleton.getRole().roleName;
	const std::string roleAbbr = currSingleton.getRole().abbreviation;

	// halt the requesting singleton if it isn't the one currently being recruited
	if ((recruitingID.present() && recruitingID.get() != registeringID) ||
	    self->clusterControllerDcId != worker.locality.dcId()) {
		TraceEvent(("CCHaltRegistering" + roleName).c_str(), self->id)
		    .detail(roleAbbr + "ID", registeringID)
		    .detail("DcID", printable(self->clusterControllerDcId))
		    .detail("ReqDcID", printable(worker.locality.dcId()))
		    .detail("Recruiting" + roleAbbr + "ID", recruitingID.present() ? recruitingID.get() : UID());
		registeringSingleton.halt(*self, worker.locality.processId());
	} else if (!recruitingID.present()) {
		// if not currently recruiting, then halt previous one in favour of requesting one
		TraceEvent(("CCRegister" + roleName).c_str(), self->id).detail(roleAbbr + "ID", registeringID);
		if (currSingleton.isPresent() && currSingleton.getInterface().id() != registeringID &&
		    self->id_worker.contains(currSingleton.getInterface().locality.processId())) {
			TraceEvent(("CCHaltPrevious" + roleName).c_str(), self->id)
			    .detail(roleAbbr + "ID", currSingleton.getInterface().id())
			    .detail("DcID", printable(self->clusterControllerDcId))
			    .detail("ReqDcID", printable(worker.locality.dcId()))
			    .detail("Recruiting" + roleAbbr + "ID", recruitingID.present() ? recruitingID.get() : UID());
			currSingleton.halt(*self, currSingleton.getInterface().locality.processId());
		}
		// set the curr singleton if it doesn't exist or its different from the requesting one
		if (!currSingleton.isPresent() || currSingleton.getInterface().id() != registeringID) {
			registeringSingleton.setInterfaceToDbInfo(*self);
		}
	}
}

ACTOR Future<Void> registerWorker(RegisterWorkerRequest req,
                                  ClusterControllerData* self,
                                  ClusterConnectionString cs,
                                  ConfigBroadcaster* configBroadcaster) {
	std::vector<NetworkAddress> coordinatorAddresses = wait(cs.tryResolveHostnames());

	const WorkerInterface& w = req.wi;
	if (req.clusterId.present() && self->clusterId->get().present() && req.clusterId != self->clusterId->get() &&
	    req.processClass != ProcessClass::TesterClass) {
		TraceEvent(g_network->isSimulated() ? SevWarnAlways : SevError, "WorkerBelongsToExistingCluster", self->id)
		    .detail("WorkerClusterId", req.clusterId)
		    .detail("ClusterControllerClusterId", self->clusterId->get())
		    .detail("WorkerId", w.id())
		    .detail("ProcessId", w.locality.processId());
		req.reply.sendError(invalid_cluster_id());
		return Void();
	}

	ProcessClass newProcessClass = req.processClass;
	auto info = self->id_worker.find(w.locality.processId());
	ClusterControllerPriorityInfo newPriorityInfo = req.priorityInfo;
	newPriorityInfo.processClassFitness = newProcessClass.machineClassFitness(ProcessClass::ClusterController);

	bool isCoordinator =
	    (std::find(coordinatorAddresses.begin(), coordinatorAddresses.end(), w.address()) !=
	     coordinatorAddresses.end()) ||
	    (w.secondaryAddress().present() &&
	     std::find(coordinatorAddresses.begin(), coordinatorAddresses.end(), w.secondaryAddress().get()) !=
	         coordinatorAddresses.end());

	for (auto it : req.incompatiblePeers) {
		self->db.incompatibleConnections[it] = now() + SERVER_KNOBS->INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
	}
	self->removedDBInfoEndpoints.erase(w.updateServerDBInfo.getEndpoint());

	if (info == self->id_worker.end()) {
		TraceEvent("ClusterControllerActualWorkers", self->id)
		    .detail("WorkerId", w.id())
		    .detail("ProcessId", w.locality.processId())
		    .detail("ZoneId", w.locality.zoneId())
		    .detail("DataHall", w.locality.dataHallId())
		    .detail("PClass", req.processClass.toString())
		    .detail("Workers", self->id_worker.size())
		    .detail("RecoveredDiskFiles", req.recoveredDiskFiles);
		self->goodRecruitmentTime = lowPriorityDelay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY);
		self->goodRemoteRecruitmentTime = lowPriorityDelay(SERVER_KNOBS->WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY);
	} else {
		TraceEvent("ClusterControllerWorkerAlreadyRegistered", self->id)
		    .suppressFor(1.0)
		    .detail("WorkerId", w.id())
		    .detail("ProcessId", w.locality.processId())
		    .detail("ZoneId", w.locality.zoneId())
		    .detail("DataHall", w.locality.dataHallId())
		    .detail("PClass", req.processClass.toString())
		    .detail("Workers", self->id_worker.size())
		    .detail("Degraded", req.degraded)
		    .detail("RecoveredDiskFiles", req.recoveredDiskFiles);
	}
	if (w.address() == g_network->getLocalAddress()) {
		if (self->changingDcIds.get().first) {
			if (self->changingDcIds.get().second.present()) {
				newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness(
				    w.locality.dcId(), self->changingDcIds.get().second.get());
			}
		} else if (self->changedDcIds.get().second.present()) {
			newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness(
			    w.locality.dcId(), self->changedDcIds.get().second.get());
		}
	} else {
		if (!self->changingDcIds.get().first) {
			if (self->changingDcIds.get().second.present()) {
				newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness(
				    w.locality.dcId(), self->changingDcIds.get().second.get());
			}
		} else if (self->changedDcIds.get().second.present()) {
			newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness(
			    w.locality.dcId(), self->changedDcIds.get().second.get());
		}
	}

	// Check process class and exclusive property
	if (info == self->id_worker.end() || info->second.details.interf.id() != w.id() ||
	    req.generation >= info->second.gen) {
		if (self->gotProcessClasses) {
			auto classIter = self->id_class.find(w.locality.processId());

			if (classIter != self->id_class.end() && (classIter->second.classSource() == ProcessClass::DBSource ||
			                                          req.initialClass.classType() == ProcessClass::UnsetClass)) {
				newProcessClass = classIter->second;
			} else {
				newProcessClass = req.initialClass;
			}
			newPriorityInfo.processClassFitness = newProcessClass.machineClassFitness(ProcessClass::ClusterController);
		}

		if (self->gotFullyRecoveredConfig) {
			newPriorityInfo.isExcluded = self->db.fullyRecoveredConfig.isExcludedServer(w.addresses(), w.locality);
		}
	}

	if (info == self->id_worker.end()) {
		self->id_worker[w.locality.processId()] = WorkerInfo(workerAvailabilityWatch(w, newProcessClass, self),
		                                                     req.reply,
		                                                     req.generation,
		                                                     w,
		                                                     req.initialClass,
		                                                     newProcessClass,
		                                                     newPriorityInfo,
		                                                     req.degraded,
		                                                     req.recoveredDiskFiles,
		                                                     req.issues);
		// Currently, only CC_ONLY_CONSIDER_INTRA_DC_LATENCY feature relies on addr_locality mapping. In the future, if
		// needed, we can populate the mapping unconditionally.
		if (SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY) {
			const bool addrDcChanged = self->addr_locality.contains(w.address()) &&
			                           self->addr_locality[w.address()].dcId() != w.locality.dcId();
			if (addrDcChanged) {
				TraceEvent(SevWarn, "AddrDcChanged")
				    .detail("Addr", w.address())
				    .detail("ExistingLocality", self->addr_locality[w.address()].toString())
				    .detail("NewLocality", w.locality.toString());
			}
			ASSERT_WE_THINK(!addrDcChanged);
			self->addr_locality[w.address()] = w.locality;
			if (w.secondaryAddress().present()) {
				self->addr_locality[w.secondaryAddress().get()] = w.locality;
			}
		}
		if (!self->masterProcessId.present() &&
		    w.locality.processId() == self->db.serverInfo->get().master.locality.processId()) {
			self->masterProcessId = w.locality.processId();
		}
		if (configBroadcaster != nullptr && req.lastSeenKnobVersion.present() && req.knobConfigClassSet.present()) {
			self->addActor.send(configBroadcaster->registerNode(req.configBroadcastInterface,
			                                                    req.lastSeenKnobVersion.get(),
			                                                    req.knobConfigClassSet.get(),
			                                                    self->id_worker[w.locality.processId()].watcher,
			                                                    isCoordinator));
		}
		self->updateDBInfoEndpoints.insert(w.updateServerDBInfo.getEndpoint());
		self->updateDBInfo.trigger();
		checkOutstandingRequests(self);
	} else if (info->second.details.interf.id() != w.id() || req.generation >= info->second.gen) {
		if (!info->second.reply.isSet()) {
			info->second.reply.send(Never());
		}
		info->second.reply = req.reply;
		info->second.details.processClass = newProcessClass;
		info->second.priorityInfo = newPriorityInfo;
		info->second.initialClass = req.initialClass;
		info->second.details.degraded = req.degraded;
		info->second.details.recoveredDiskFiles = req.recoveredDiskFiles;
		info->second.gen = req.generation;
		info->second.issues = req.issues;

		if (info->second.details.interf.id() != w.id()) {
			self->removedDBInfoEndpoints.insert(info->second.details.interf.updateServerDBInfo.getEndpoint());
			info->second.details.interf = w;
			// Cancel the existing watcher actor; possible race condition could be, the older registered watcher
			// detects failures and removes the worker from id_worker even before the new watcher starts monitoring the
			// new interface
			info->second.watcher.cancel();
			info->second.watcher = workerAvailabilityWatch(w, newProcessClass, self);
		}
		if (req.requestDbInfo) {
			self->updateDBInfoEndpoints.insert(w.updateServerDBInfo.getEndpoint());
			self->updateDBInfo.trigger();
		}
		if (configBroadcaster != nullptr && req.lastSeenKnobVersion.present() && req.knobConfigClassSet.present()) {
			self->addActor.send(configBroadcaster->registerNode(req.configBroadcastInterface,
			                                                    req.lastSeenKnobVersion.get(),
			                                                    req.knobConfigClassSet.get(),
			                                                    info->second.watcher,
			                                                    isCoordinator));
		}
		checkOutstandingRequests(self);
	} else {
		CODE_PROBE(true, "Received an old worker registration request.", probe::decoration::rare);
	}

	// For each singleton
	// - if the registering singleton conflicts with the singleton being recruited, kill the registering one
	// - if the singleton is not being recruited, kill the existing one in favour of the registering one
	if (req.distributorInterf.present()) {
		auto currSingleton = DataDistributorSingleton(self->db.serverInfo->get().distributor);
		auto registeringSingleton = DataDistributorSingleton(req.distributorInterf);
		haltRegisteringOrCurrentSingleton<DataDistributorSingleton>(
		    self, w, currSingleton, registeringSingleton, self->recruitingDistributorID);
	}

	if (req.ratekeeperInterf.present()) {
		auto currSingleton = RatekeeperSingleton(self->db.serverInfo->get().ratekeeper);
		auto registeringSingleton = RatekeeperSingleton(req.ratekeeperInterf);
		haltRegisteringOrCurrentSingleton<RatekeeperSingleton>(
		    self, w, currSingleton, registeringSingleton, self->recruitingRatekeeperID);
	}

	if (self->db.blobGranulesEnabled.get() && req.blobManagerInterf.present()) {
		auto currSingleton = BlobManagerSingleton(self->db.serverInfo->get().blobManager);
		auto registeringSingleton = BlobManagerSingleton(req.blobManagerInterf);
		haltRegisteringOrCurrentSingleton<BlobManagerSingleton>(
		    self, w, currSingleton, registeringSingleton, self->recruitingBlobManagerID);
	}
	if (req.blobMigratorInterf.present() && self->db.blobRestoreEnabled.get()) {
		auto currSingleton = BlobMigratorSingleton(self->db.serverInfo->get().blobMigrator);
		auto registeringSingleton = BlobMigratorSingleton(req.blobMigratorInterf);
		haltRegisteringOrCurrentSingleton<BlobMigratorSingleton>(
		    self, w, currSingleton, registeringSingleton, self->recruitingBlobMigratorID);
	}

	if (self->db.config.encryptionAtRestMode.isEncryptionEnabled() && req.encryptKeyProxyInterf.present()) {
		auto currSingleton = EncryptKeyProxySingleton(self->db.serverInfo->get().client.encryptKeyProxy);
		auto registeringSingleton = EncryptKeyProxySingleton(req.encryptKeyProxyInterf);
		haltRegisteringOrCurrentSingleton<EncryptKeyProxySingleton>(
		    self, w, currSingleton, registeringSingleton, self->recruitingEncryptKeyProxyID);
	}

	if (req.consistencyScanInterf.present()) {
		auto currSingleton = ConsistencyScanSingleton(self->db.serverInfo->get().consistencyScan);
		auto registeringSingleton = ConsistencyScanSingleton(req.consistencyScanInterf);
		haltRegisteringOrCurrentSingleton<ConsistencyScanSingleton>(
		    self, w, currSingleton, registeringSingleton, self->recruitingConsistencyScanID);
	}

	// Notify the worker to register again with new process class/exclusive property
	if (!req.reply.isSet() && newPriorityInfo != req.priorityInfo) {
		req.reply.send(RegisterWorkerReply(newProcessClass, newPriorityInfo));
	}

	return Void();
}

#define TIME_KEEPER_VERSION "1"_sr

ACTOR Future<Void> timeKeeperSetVersion(ClusterControllerData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->set(timeKeeperVersionKey, TIME_KEEPER_VERSION);
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid
// running out of space, it limits the max number of entries and clears old entries on each update. This mapping is
// used from backup and restore to get the version information for a timestamp.
ACTOR Future<Void> timeKeeper(ClusterControllerData* self) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);

	TraceEvent("TimeKeeperStarted").log();

	wait(timeKeeperSetVersion(self));

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
		loop {
			try {
				state UID debugID = deterministicRandom()->randomUniqueID();
				if (!g_network->isSimulated()) {
					// This is done to provide an arbitrary logged transaction every ~10s.
					// FIXME: replace or augment this with logging on the proxy which tracks
					// how long it is taking to hear responses from each other component.
					tr->debugTransaction(debugID);
				}
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				Optional<Value> disableValue = wait(tr->get(timeKeeperDisableKey));
				if (disableValue.present()) {
					break;
				}

				Version v = tr->getReadVersion().get();
				int64_t currentTime = (int64_t)now();
				versionMap.set(tr, currentTime, v);
				if (!g_network->isSimulated()) {
					TraceEvent("TimeKeeperCommit", debugID).detail("Version", v);
				}
				int64_t ttl = currentTime - SERVER_KNOBS->TIME_KEEPER_DELAY * SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES;
				if (ttl > 0) {
					versionMap.erase(tr, 0, ttl);
				}

				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}

		wait(delay(SERVER_KNOBS->TIME_KEEPER_DELAY));
	}
}

ACTOR Future<Void> statusServer(FutureStream<StatusRequest> requests,
                                ClusterControllerData* self,
                                ServerCoordinators coordinators,
                                ConfigBroadcaster const* configBroadcaster) {
	// Seconds since the END of the last GetStatus executed
	state double last_request_time = 0.0;

	// Place to accumulate a batch of requests to respond to
	state std::vector<StatusRequest> requests_batch;

	loop {
		try {
			// Wait til first request is ready
			StatusRequest req = waitNext(requests);
			++self->statusRequests;
			requests_batch.push_back(req);

			// Earliest time at which we may begin a new request
			double next_allowed_request_time = last_request_time + SERVER_KNOBS->STATUS_MIN_TIME_BETWEEN_REQUESTS;

			// Wait if needed to satisfy min_time knob, also allows more requests to queue up.
			double minwait = std::max(next_allowed_request_time - now(), 0.0);
			wait(delay(minwait));

			// Get all requests that are ready right *now*, before GetStatus() begins.
			// All of these requests will be responded to with the next GetStatus() result.
			// If requests are batched, do not respond to more than MAX_STATUS_REQUESTS_PER_SECOND
			// requests per second
			while (requests.isReady()) {
				auto req = requests.pop();
				if (SERVER_KNOBS->STATUS_MIN_TIME_BETWEEN_REQUESTS > 0.0 &&
				    requests_batch.size() + 1 >
				        SERVER_KNOBS->STATUS_MIN_TIME_BETWEEN_REQUESTS * SERVER_KNOBS->MAX_STATUS_REQUESTS_PER_SECOND) {
					TraceEvent(SevWarnAlways, "TooManyStatusRequests")
					    .suppressFor(1.0)
					    .detail("BatchSize", requests_batch.size());
					req.reply.sendError(server_overloaded());
				} else {
					requests_batch.push_back(req);
				}
			}

			// Get status but trap errors to send back to client.
			std::vector<WorkerDetails> workers;
			std::vector<ProcessIssues> workerIssues;

			for (auto& it : self->id_worker) {
				workers.push_back(it.second.details);
				if (it.second.issues.size()) {
					workerIssues.emplace_back(it.second.details.interf.address(), it.second.issues);
				}
			}

			std::vector<NetworkAddress> incompatibleConnections;
			for (auto it = self->db.incompatibleConnections.begin(); it != self->db.incompatibleConnections.end();) {
				if (it->second < now()) {
					it = self->db.incompatibleConnections.erase(it);
				} else {
					incompatibleConnections.push_back(it->first);
					it++;
				}
			}

			state ErrorOr<StatusReply> result = wait(errorOr(clusterGetStatus(self->db.serverInfo,
			                                                                  self->cx,
			                                                                  workers,
			                                                                  workerIssues,
			                                                                  self->storageStatusInfos,
			                                                                  &self->db.clientStatus,
			                                                                  coordinators,
			                                                                  incompatibleConnections,
			                                                                  self->datacenterVersionDifference,
			                                                                  self->dcLogServerVersionDifference,
			                                                                  self->dcStorageServerVersionDifference,
			                                                                  configBroadcaster,
			                                                                  self->db.metaclusterRegistration,
			                                                                  self->db.metaclusterMetrics,
			                                                                  self->excludedDegradedServers)));

			if (result.isError() && result.getError().code() == error_code_actor_cancelled)
				throw result.getError();

			// Update last_request_time now because GetStatus is finished and the delay is to be measured between
			// requests
			last_request_time = now();

			state Optional<StatusReply> faultToleranceRelatedStatus;
			while (!requests_batch.empty()) {
				if (result.isError())
					requests_batch.back().reply.sendError(result.getError());
				else if (requests_batch.back().statusField.empty())
					requests_batch.back().reply.send(result.get());
				else {
					ASSERT(requests_batch.back().statusField == "fault_tolerance");
					if (!faultToleranceRelatedStatus.present()) {
						faultToleranceRelatedStatus = clusterGetFaultToleranceStatus(result.get().statusStr);
					}
					requests_batch.back().reply.send(faultToleranceRelatedStatus.get());
				}
				requests_batch.pop_back();
				wait(yield());
			}
			faultToleranceRelatedStatus.reset();
		} catch (Error& e) {
			TraceEvent(SevError, "StatusServerError").error(e);
			throw e;
		}
	}
}

ACTOR Future<Void> monitorProcessClasses(ClusterControllerData* self) {

	state ReadYourWritesTransaction trVer(self->db.db);
	loop {
		try {
			trVer.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			trVer.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> val = wait(trVer.get(processClassVersionKey));

			if (val.present())
				break;

			RangeResult processClasses = wait(trVer.getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!processClasses.more && processClasses.size() < CLIENT_KNOBS->TOO_MANY);

			trVer.clear(processClassKeys);
			trVer.set(processClassVersionKey, processClassVersionValue);
			for (auto it : processClasses) {
				UID processUid = decodeProcessClassKeyOld(it.key);
				trVer.set(processClassKeyFor(processUid.toString()), it.value);
			}

			wait(trVer.commit());
			TraceEvent("ProcessClassUpgrade").log();
			break;
		} catch (Error& e) {
			wait(trVer.onError(e));
		}
	}

	loop {
		state ReadYourWritesTransaction tr(self->db.db);

		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				RangeResult processClasses = wait(tr.getRange(processClassKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!processClasses.more && processClasses.size() < CLIENT_KNOBS->TOO_MANY);

				if (processClasses != self->lastProcessClasses || !self->gotProcessClasses) {
					self->id_class.clear();
					for (int i = 0; i < processClasses.size(); i++) {
						auto c = decodeProcessClassValue(processClasses[i].value);
						ASSERT(c.classSource() != ProcessClass::CommandLineSource);
						self->id_class[decodeProcessClassKey(processClasses[i].key)] = c;
					}

					for (auto& w : self->id_worker) {
						auto classIter = self->id_class.find(w.first);
						ProcessClass newProcessClass;

						if (classIter != self->id_class.end() &&
						    (classIter->second.classSource() == ProcessClass::DBSource ||
						     w.second.initialClass.classType() == ProcessClass::UnsetClass)) {
							newProcessClass = classIter->second;
						} else {
							newProcessClass = w.second.initialClass;
						}

						if (newProcessClass != w.second.details.processClass) {
							w.second.details.processClass = newProcessClass;
							w.second.priorityInfo.processClassFitness =
							    newProcessClass.machineClassFitness(ProcessClass::ClusterController);
							if (!w.second.reply.isSet()) {
								w.second.reply.send(
								    RegisterWorkerReply(w.second.details.processClass, w.second.priorityInfo));
							}
						}
					}

					self->lastProcessClasses = processClasses;
					self->gotProcessClasses = true;
					checkOutstandingRequests(self);
				}

				state Future<Void> watchFuture = tr.watch(processClassChangeKey);
				wait(tr.commit());
				wait(watchFuture);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> monitorServerInfoConfig(ClusterControllerData::DBInfo* db) {
	loop {
		state ReadYourWritesTransaction tr(db->db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

				Optional<Value> configVal = wait(tr.get(latencyBandConfigKey));
				Optional<LatencyBandConfig> config;
				if (configVal.present()) {
					config = LatencyBandConfig::parse(configVal.get());
				}

				auto serverInfo = db->serverInfo->get();
				if (config != serverInfo.latencyBandConfig) {
					TraceEvent("LatencyBandConfigChanged").detail("Present", config.present());
					serverInfo.id = deterministicRandom()->randomUniqueID();
					serverInfo.infoGeneration = ++db->dbInfoCount;
					serverInfo.latencyBandConfig = config;
					db->serverInfo->set(serverInfo);
				}

				state Future<Void> configChangeFuture = tr.watch(latencyBandConfigKey);

				wait(tr.commit());
				wait(configChangeFuture);

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

// Monitors storage metadata changes and updates to storage servers.
ACTOR Future<Void> monitorStorageMetadata(ClusterControllerData* self) {
	state KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(serverMetadataKeys.begin,
	                                                                                           IncludeVersion());
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	state std::vector<StorageServerMetaInfo> servers;
	loop {
		try {
			servers.clear();
			tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			state RangeResult serverList = wait(tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

			servers.reserve(serverList.size());
			for (const auto ss : serverList) {
				servers.push_back(StorageServerMetaInfo(decodeServerListValue(ss.value)));
			}

			state RangeResult serverMetadata = wait(tr->getRange(serverMetadataKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverMetadata.more && serverMetadata.size() < CLIENT_KNOBS->TOO_MANY);
			std::map<UID, StorageMetadataType> idMetadata;
			for (const auto& sm : serverMetadata) {
				const UID id = decodeServerMetadataKey(sm.key);
				idMetadata[id] = decodeServerMetadataValue(sm.value);
			}
			for (auto& s : servers) {
				if (idMetadata.contains(s.id())) {
					s.metadata = idMetadata[s.id()];
				} else {
					TraceEvent(SevWarn, "StorageServerMetadataMissing", self->id).detail("ServerID", s.id());
				}
			}

			state Future<Void> watchFuture = tr->watch(serverMetadataChangeKey);
			wait(tr->commit());

			self->storageStatusInfos = std::move(servers);
			wait(watchFuture);
			tr->reset();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

// Monitors the global configuration version key for changes. When changes are
// made, the global configuration history is read and any updates are sent to
// all processes in the system by updating the ClientDBInfo object. The
// GlobalConfig actor class contains the functionality to read the latest
// history and update the processes local view.
ACTOR Future<Void> monitorGlobalConfig(ClusterControllerData::DBInfo* db) {
	loop {
		state ReadYourWritesTransaction tr(db->db);
		state ClientDBInfo clientInfo;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				state Optional<Value> globalConfigVersion = wait(tr.get(globalConfigVersionKey));

				if (globalConfigVersion.present()) {
					// Since the history keys end with versionstamps, they
					// should be sorted correctly (versionstamps are stored in
					// big-endian order).
					RangeResult globalConfigHistory =
					    wait(tr.getRange(globalConfigHistoryKeys, CLIENT_KNOBS->TOO_MANY));
					// If the global configuration version key has been set,
					// the history should contain at least one item.
					ASSERT(globalConfigHistory.size() > 0);
					clientInfo = db->serverInfo->get().client;
					clientInfo.history.clear();

					for (const auto& kv : globalConfigHistory) {
						ObjectReader reader(kv.value.begin(), IncludeVersion());
						if (reader.protocolVersion() != g_network->protocolVersion() || BUGGIFY_WITH_PROB(0.01)) {
							// If the protocol version has changed, the
							// GlobalConfig actor should refresh its view by
							// reading the entire global configuration key
							// range.  Setting the version to the max int64_t
							// will always cause the global configuration
							// updater to refresh its view of the configuration
							// keyspace.
							clientInfo.history.clear();
							clientInfo.history.emplace_back(std::numeric_limits<Version>::max());
							break;
						}

						VersionHistory vh;
						reader.deserialize(vh);

						// Read commit version out of versionstamp at end of key.
						BinaryReader versionReader =
						    BinaryReader(kv.key.removePrefix(globalConfigHistoryPrefix), Unversioned());
						Version historyCommitVersion;
						versionReader >> historyCommitVersion;
						historyCommitVersion = bigEndian64(historyCommitVersion);
						vh.version = historyCommitVersion;

						clientInfo.history.push_back(std::move(vh));
					}

					if (clientInfo.history.size() > 0) {
						// The first item in the historical list of mutations
						// is only used to:
						//   a) Recognize that some historical changes may have
						//      been missed, and the entire global
						//      configuration keyspace needs to be read, or..
						//   b) Check which historical updates have already
						//      been applied. If this is the case, the first
						//      history item must have a version greater than
						//      or equal to whatever version the global
						//      configuration was last updated at, and
						//      therefore won't need to be applied again.
						clientInfo.history[0].mutations = Standalone<VectorRef<MutationRef>>();
					}

					clientInfo.id = deterministicRandom()->randomUniqueID();
					// Update ServerDBInfo so fdbserver processes receive updated history.
					ServerDBInfo serverInfo = db->serverInfo->get();
					serverInfo.id = deterministicRandom()->randomUniqueID();
					serverInfo.infoGeneration = ++db->dbInfoCount;
					serverInfo.client = clientInfo;
					db->serverInfo->set(serverInfo);

					// Update ClientDBInfo so client processes receive updated history.
					db->clientInfo->set(clientInfo);
				}

				state Future<Void> globalConfigFuture = tr.watch(globalConfigVersionKey);
				wait(tr.commit());
				wait(globalConfigFuture);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> updatedChangingDatacenters(ClusterControllerData* self) {
	// do not change the cluster controller until all the processes have had a chance to register
	wait(delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
	loop {
		state Future<Void> onChange = self->desiredDcIds.onChange();
		if (!self->desiredDcIds.get().present()) {
			self->changingDcIds.set(std::make_pair(false, self->desiredDcIds.get()));
		} else {
			auto& worker = self->id_worker[self->clusterControllerProcessId];
			uint8_t newFitness = ClusterControllerPriorityInfo::calculateDCFitness(
			    worker.details.interf.locality.dcId(), self->desiredDcIds.get().get());
			self->changingDcIds.set(
			    std::make_pair(worker.priorityInfo.dcFitness > newFitness, self->desiredDcIds.get()));

			TraceEvent("UpdateChangingDatacenter", self->id)
			    .detail("OldFitness", worker.priorityInfo.dcFitness)
			    .detail("NewFitness", newFitness);
			if (worker.priorityInfo.dcFitness > newFitness) {
				worker.priorityInfo.dcFitness = newFitness;
				if (!worker.reply.isSet()) {
					worker.reply.send(RegisterWorkerReply(worker.details.processClass, worker.priorityInfo));
				}
			} else {
				state int currentFit = ProcessClass::BestFit;
				while (currentFit <= ProcessClass::NeverAssign) {
					bool updated = false;
					for (auto& it : self->id_worker) {
						if ((!it.second.priorityInfo.isExcluded &&
						     it.second.priorityInfo.processClassFitness == currentFit) ||
						    currentFit == ProcessClass::NeverAssign) {
							uint8_t fitness = ClusterControllerPriorityInfo::calculateDCFitness(
							    it.second.details.interf.locality.dcId(), self->changingDcIds.get().second.get());
							if (it.first != self->clusterControllerProcessId &&
							    it.second.priorityInfo.dcFitness != fitness) {
								updated = true;
								it.second.priorityInfo.dcFitness = fitness;
								if (!it.second.reply.isSet()) {
									it.second.reply.send(
									    RegisterWorkerReply(it.second.details.processClass, it.second.priorityInfo));
								}
							}
						}
					}
					if (updated && currentFit < ProcessClass::NeverAssign) {
						wait(delay(SERVER_KNOBS->CC_CLASS_DELAY));
					}
					currentFit++;
				}
			}
		}

		wait(onChange);
	}
}

ACTOR Future<Void> updatedChangedDatacenters(ClusterControllerData* self) {
	state Future<Void> changeDelay = delay(SERVER_KNOBS->CC_CHANGE_DELAY);
	state Future<Void> onChange = self->changingDcIds.onChange();
	loop {
		choose {
			when(wait(onChange)) {
				changeDelay = delay(SERVER_KNOBS->CC_CHANGE_DELAY);
				onChange = self->changingDcIds.onChange();
			}
			when(wait(changeDelay)) {
				changeDelay = Never();
				onChange = self->changingDcIds.onChange();

				self->changedDcIds.set(self->changingDcIds.get());
				if (self->changedDcIds.get().second.present()) {
					TraceEvent("UpdateChangedDatacenter", self->id).detail("CCFirst", self->changedDcIds.get().first);
					if (!self->changedDcIds.get().first) {
						auto& worker = self->id_worker[self->clusterControllerProcessId];
						uint8_t newFitness = ClusterControllerPriorityInfo::calculateDCFitness(
						    worker.details.interf.locality.dcId(), self->changedDcIds.get().second.get());
						if (worker.priorityInfo.dcFitness != newFitness) {
							worker.priorityInfo.dcFitness = newFitness;
							if (!worker.reply.isSet()) {
								worker.reply.send(
								    RegisterWorkerReply(worker.details.processClass, worker.priorityInfo));
							}
						}
					} else {
						state int currentFit = ProcessClass::BestFit;
						while (currentFit <= ProcessClass::NeverAssign) {
							bool updated = false;
							for (auto& it : self->id_worker) {
								if ((!it.second.priorityInfo.isExcluded &&
								     it.second.priorityInfo.processClassFitness == currentFit) ||
								    currentFit == ProcessClass::NeverAssign) {
									uint8_t fitness = ClusterControllerPriorityInfo::calculateDCFitness(
									    it.second.details.interf.locality.dcId(),
									    self->changedDcIds.get().second.get());
									if (it.first != self->clusterControllerProcessId &&
									    it.second.priorityInfo.dcFitness != fitness) {
										updated = true;
										it.second.priorityInfo.dcFitness = fitness;
										if (!it.second.reply.isSet()) {
											it.second.reply.send(RegisterWorkerReply(it.second.details.processClass,
											                                         it.second.priorityInfo));
										}
									}
								}
							}
							if (updated && currentFit < ProcessClass::NeverAssign) {
								wait(delay(SERVER_KNOBS->CC_CLASS_DELAY));
							}
							currentFit++;
						}
					}
				}
			}
		}
	}
}

ACTOR Future<Void> updateDatacenterVersionDifference(ClusterControllerData* self) {
	state double lastLogTime = 0;
	loop {
		self->versionDifferenceUpdated = false;
		if (self->db.serverInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS &&
		    self->db.config.usableRegions == 1) {
			bool oldDifferenceTooLarge = !self->versionDifferenceUpdated ||
			                             self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
			self->versionDifferenceUpdated = true;
			self->datacenterVersionDifference = 0;
			self->dcLogServerVersionDifference = 0;
			self->dcStorageServerVersionDifference = 0;

			if (oldDifferenceTooLarge) {
				checkOutstandingRequests(self);
			}

			wait(self->db.serverInfo->onChange());
			continue;
		}

		state Optional<TLogInterface> primaryLog;
		state Optional<TLogInterface> remoteLog;
		if (self->db.serverInfo->get().recoveryState >= RecoveryState::ALL_LOGS_RECRUITED) {
			for (auto& logSet : self->db.serverInfo->get().logSystemConfig.tLogs) {
				if (logSet.isLocal && logSet.locality != tagLocalitySatellite) {
					for (auto& tLog : logSet.tLogs) {
						if (tLog.present()) {
							primaryLog = tLog.interf();
							break;
						}
					}
				}
				if (!logSet.isLocal) {
					for (auto& tLog : logSet.tLogs) {
						if (tLog.present()) {
							remoteLog = tLog.interf();
							break;
						}
					}
				}
			}
		}

		if (!primaryLog.present() || !remoteLog.present() || !self->db.serverInfo->get().ratekeeper.present()) {
			wait(self->db.serverInfo->onChange());
			continue;
		}

		state Future<Void> onChange = self->db.serverInfo->onChange();
		loop {
			state Future<TLogQueuingMetricsReply> primaryMetrics =
			    brokenPromiseToNever(primaryLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));
			state Future<TLogQueuingMetricsReply> remoteMetrics =
			    brokenPromiseToNever(remoteLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));
			state Future<GetSSVersionLagReply> ssVersionLagReply = brokenPromiseToNever(
			    self->db.serverInfo->get().ratekeeper.get().getSSVersionLag.getReply(GetSSVersionLagRequest()));

			wait((success(primaryMetrics) && success(remoteMetrics) && success(ssVersionLagReply)) || onChange);
			if (onChange.isReady()) {
				break;
			}

			if (primaryMetrics.get().v > 0 && remoteMetrics.get().v > 0) {
				bool oldDifferenceTooLarge = !self->versionDifferenceUpdated ||
				                             self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
				self->versionDifferenceUpdated = true;
				self->dcLogServerVersionDifference = primaryMetrics.get().v - remoteMetrics.get().v;
				self->dcStorageServerVersionDifference =
				    (ssVersionLagReply.get().maxPrimarySSVersion > 0 && ssVersionLagReply.get().maxRemoteSSVersion > 0)
				        ? (ssVersionLagReply.get().maxPrimarySSVersion - ssVersionLagReply.get().maxRemoteSSVersion)
				        : 0;
				self->datacenterVersionDifference =
				    std::max(self->dcLogServerVersionDifference, self->dcStorageServerVersionDifference);

				TraceEvent("VersionDifferenceOldLarge").detail("OldDifference", oldDifferenceTooLarge);

				if (oldDifferenceTooLarge && self->datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
					checkOutstandingRequests(self);
				}

				if (now() - lastLogTime > SERVER_KNOBS->CLUSTER_CONTROLLER_LOGGING_DELAY) {
					lastLogTime = now();
					TraceEvent("DatacenterVersionDifference", self->id)
					    .detail("Difference", self->datacenterVersionDifference)
					    .detail("LogServerVersionDifference", self->dcLogServerVersionDifference)
					    .detail("StorageServerVersionDifference", self->dcStorageServerVersionDifference);
				}
			}

			wait(delay(SERVER_KNOBS->VERSION_LAG_METRIC_INTERVAL) || onChange);
			if (onChange.isReady()) {
				break;
			}
		}
	}
}

// A background actor that periodically checks remote DC health, and `checkOutstandingRequests` if remote DC
// recovers.
ACTOR Future<Void> updateRemoteDCHealth(ClusterControllerData* self) {
	// The purpose of the initial delay is to wait for the cluster to achieve a steady state before checking remote
	// DC health, since remote DC healthy may trigger a failover, and we don't want that to happen too frequently.
	wait(delay(SERVER_KNOBS->INITIAL_UPDATE_CROSS_DC_INFO_DELAY));

	self->remoteDCMonitorStarted = true;

	// When the remote DC health just start, we may just recover from a health degradation. Check if we can failback
	// if we are currently in the remote DC in the database configuration.
	if (!self->remoteTransactionSystemDegraded) {
		checkOutstandingRequests(self);
	}

	loop {
		bool oldRemoteTransactionSystemDegraded = self->remoteTransactionSystemDegraded;
		self->remoteTransactionSystemDegraded = self->remoteTransactionSystemContainsDegradedServers();

		if (oldRemoteTransactionSystemDegraded && !self->remoteTransactionSystemDegraded) {
			checkOutstandingRequests(self);
		}
		wait(delay(SERVER_KNOBS->CHECK_REMOTE_HEALTH_INTERVAL));
	}
}

ACTOR Future<Void> doEmptyCommit(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.makeSelfConflicting();
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> handleForcedRecoveries(ClusterControllerData* self, ClusterControllerFullInterface interf) {
	loop {
		state ForceRecoveryRequest req = waitNext(interf.clientInterface.forceRecovery.getFuture());
		TraceEvent("ForcedRecoveryStart", self->id)
		    .detail("ClusterControllerDcId", self->clusterControllerDcId)
		    .detail("DcId", req.dcId.printable());
		state Future<Void> fCommit = doEmptyCommit(self->cx);
		wait(fCommit || delay(SERVER_KNOBS->FORCE_RECOVERY_CHECK_DELAY));
		if (!fCommit.isReady() || fCommit.isError()) {
			if (self->clusterControllerDcId != req.dcId) {
				std::vector<Optional<Key>> dcPriority;
				dcPriority.push_back(req.dcId);
				dcPriority.push_back(self->clusterControllerDcId);
				self->desiredDcIds.set(dcPriority);
			} else {
				self->db.forceRecovery = true;
				self->db.forceMasterFailure.trigger();
			}
			wait(fCommit);
		}
		TraceEvent("ForcedRecoveryFinish", self->id).log();
		self->db.forceRecovery = false;
		req.reply.send(Void());
	}
}

ACTOR Future<Void> triggerAuditStorage(ClusterControllerData* self, TriggerAuditRequest req) {
	state UID auditId;
	ASSERT(!req.cancel);
	try {
		while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
		       !self->db.serverInfo->get().distributor.present()) {
			wait(self->db.serverInfo->onChange());
		}
		TraceEvent(SevVerbose, "CCTriggerAuditStorageBegin", self->id)
		    .detail("Range", req.range)
		    .detail("AuditType", req.getType())
		    .detail("KeyValueStoreType", req.engineType.toString())
		    .detail("DDId", self->db.serverInfo->get().distributor.get().id());
		TriggerAuditRequest fReq(req.getType(), req.range, req.engineType);
		UID auditId_ = wait(self->db.serverInfo->get().distributor.get().triggerAudit.getReply(fReq));
		auditId = auditId_;
		TraceEvent(SevVerbose, "CCTriggerAuditStorageEnd", self->id)
		    .detail("AuditID", auditId)
		    .detail("Range", req.range)
		    .detail("AuditType", req.getType())
		    .detail("KeyValueStoreType", req.engineType.toString());
		req.reply.send(auditId);
	} catch (Error& e) {
		TraceEvent(SevInfo, "CCTriggerAuditStorageFailed", self->id)
		    .errorUnsuppressed(e)
		    .detail("AuditID", auditId)
		    .detail("Range", req.range)
		    .detail("AuditType", req.getType())
		    .detail("KeyValueStoreType", req.engineType.toString());
		req.reply.sendError(audit_storage_failed());
	}

	return Void();
}

ACTOR Future<Void> cancelAuditStorage(ClusterControllerData* self, TriggerAuditRequest req) {
	ASSERT(req.cancel);
	try {
		while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
		       !self->db.serverInfo->get().distributor.present()) {
			wait(self->db.serverInfo->onChange());
		}
		TraceEvent(SevVerbose, "CCCancelAuditStorageBegin", self->id)
		    .detail("AuditID", req.id)
		    .detail("AuditType", req.getType())
		    .detail("DDId", self->db.serverInfo->get().distributor.get().id());
		TriggerAuditRequest fReq(req.getType(), req.id);
		UID auditId = wait(self->db.serverInfo->get().distributor.get().triggerAudit.getReply(fReq));
		TraceEvent(SevVerbose, "CCCancelAuditStorageEnd", self->id)
		    .detail("ReturnedAuditID", auditId)
		    .detail("AuditID", auditId)
		    .detail("AuditType", req.getType());
		ASSERT(auditId == req.id);
		req.reply.send(auditId);
	} catch (Error& e) {
		TraceEvent(SevInfo, "CCCancelAuditStorageFailed", self->id)
		    .errorUnsuppressed(e)
		    .detail("AuditID", req.id)
		    .detail("AuditType", req.getType());
		req.reply.sendError(cancel_audit_storage_failed());
	}

	return Void();
}

ACTOR Future<Void> handleTriggerAuditStorage(ClusterControllerData* self, ClusterControllerFullInterface interf) {
	loop {
		TriggerAuditRequest req = waitNext(interf.clientInterface.triggerAudit.getFuture());
		TraceEvent(SevVerbose, "CCTriggerAuditStorageReceived", self->id)
		    .detail("ClusterControllerDcId", self->clusterControllerDcId)
		    .detail("Range", req.range)
		    .detail("AuditType", req.getType())
		    .detail("KeyValueStoreType", req.engineType.toString());
		if (req.cancel) {
			ASSERT(req.id.isValid());
			self->addActor.send(cancelAuditStorage(self, req));
		} else {
			self->addActor.send(triggerAuditStorage(self, req));
		}
	}
}

ACTOR Future<Void> startDataDistributor(ClusterControllerData* self, double waitTime) {
	// If master fails at the same time, give it a chance to clear master PID.
	// Also wait to avoid too many consecutive recruits in a small time window.
	wait(delay(waitTime));

	TraceEvent("CCStartDataDistributor", self->id).log();
	loop {
		try {
			state bool noDistributor = !self->db.serverInfo->get().distributor.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (noDistributor && self->db.serverInfo->get().distributor.present()) {
				// Existing distributor registers while waiting, so skip.
				return Void();
			}

			std::map<Optional<Standalone<StringRef>>, int> idUsed = self->getUsedIds();
			WorkerFitnessInfo ddWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                ProcessClass::DataDistributor,
			                                                                ProcessClass::NeverAssign,
			                                                                self->db.config,
			                                                                idUsed);
			InitializeDataDistributorRequest req(deterministicRandom()->randomUniqueID());
			state WorkerDetails worker = ddWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::DataDistributor)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingDistributorID = req.reqId;
			TraceEvent("CCRecruitDataDistributor", self->id)
			    .detail("Addr", worker.interf.address())
			    .detail("DDID", req.reqId);

			ErrorOr<DataDistributorInterface> ddInterf = wait(worker.interf.dataDistributor.getReplyUnlessFailedFor(
			    req, SERVER_KNOBS->WAIT_FOR_DISTRIBUTOR_JOIN_DELAY, 0));

			if (ddInterf.present()) {
				self->recruitDistributor.set(false);
				self->recruitingDistributorID = ddInterf.get().id();
				const auto& distributor = self->db.serverInfo->get().distributor;
				TraceEvent("CCDataDistributorRecruited", self->id)
				    .detail("Addr", worker.interf.address())
				    .detail("DDID", ddInterf.get().id());
				if (distributor.present() && distributor.get().id() != ddInterf.get().id() &&
				    self->id_worker.contains(distributor.get().locality.processId())) {

					TraceEvent("CCHaltDataDistributorAfterRecruit", self->id)
					    .detail("DDID", distributor.get().id())
					    .detail("DcID", printable(self->clusterControllerDcId));

					DataDistributorSingleton(distributor).halt(*self, distributor.get().locality.processId());
				}
				if (!distributor.present() || distributor.get().id() != ddInterf.get().id()) {
					self->db.setDistributor(ddInterf.get());
				}
				checkOutstandingRequests(self);
				return Void();
			}
		} catch (Error& e) {
			TraceEvent("CCDataDistributorRecruitError", self->id).error(e);
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> monitorDataDistributor(ClusterControllerData* self) {
	state SingletonRecruitThrottler recruitThrottler;
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		bool ddExist = self->db.serverInfo->get().distributor.present();
		TraceEvent(SevInfo, "CCMonitorDataDistributor", self->id)
		    .detail("Recruiting", self->recruitDistributor.get())
		    .detail("Existing", ddExist)
		    .detail("ExistingDD", ddExist ? self->db.serverInfo->get().distributor.get().id().toString() : "");

		if (self->db.serverInfo->get().distributor.present() && !self->recruitDistributor.get()) {
			choose {
				when(wait(waitFailureClient(self->db.serverInfo->get().distributor.get().waitFailure,
				                            SERVER_KNOBS->DD_FAILURE_TIME))) {
					const auto& distributor = self->db.serverInfo->get().distributor;
					TraceEvent("CCDataDistributorDied", self->id).detail("DDID", distributor.get().id());
					DataDistributorSingleton(distributor).halt(*self, distributor.get().locality.processId());
					self->db.clearInterf(ProcessClass::DataDistributorClass);
				}
				when(wait(self->recruitDistributor.onChange())) {}
			}
		} else {
			wait(startDataDistributor(self, recruitThrottler.newRecruitment()));
		}
	}
}

ACTOR Future<Void> startRatekeeper(ClusterControllerData* self, double waitTime) {
	// If master fails at the same time, give it a chance to clear master PID.
	// Also wait to avoid too many consecutive recruits in a small time window.
	wait(delay(waitTime));

	TraceEvent("CCStartRatekeeper", self->id).log();
	loop {
		try {
			state bool no_ratekeeper = !self->db.serverInfo->get().ratekeeper.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (no_ratekeeper && self->db.serverInfo->get().ratekeeper.present()) {
				// Existing ratekeeper registers while waiting, so skip.
				return Void();
			}

			std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			WorkerFitnessInfo rkWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                ProcessClass::Ratekeeper,
			                                                                ProcessClass::NeverAssign,
			                                                                self->db.config,
			                                                                id_used);
			InitializeRatekeeperRequest req(deterministicRandom()->randomUniqueID());
			state WorkerDetails worker = rkWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::Ratekeeper)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingRatekeeperID = req.reqId;
			TraceEvent("CCRecruitRatekeeper", self->id)
			    .detail("Addr", worker.interf.address())
			    .detail("RKID", req.reqId);

			ErrorOr<RatekeeperInterface> interf = wait(
			    worker.interf.ratekeeper.getReplyUnlessFailedFor(req, SERVER_KNOBS->WAIT_FOR_RATEKEEPER_JOIN_DELAY, 0));
			if (interf.present()) {
				self->recruitRatekeeper.set(false);
				self->recruitingRatekeeperID = interf.get().id();
				const auto& ratekeeper = self->db.serverInfo->get().ratekeeper;
				TraceEvent("CCRatekeeperRecruited", self->id)
				    .detail("Addr", worker.interf.address())
				    .detail("RKID", interf.get().id());
				if (ratekeeper.present() && ratekeeper.get().id() != interf.get().id() &&
				    self->id_worker.contains(ratekeeper.get().locality.processId())) {
					TraceEvent("CCHaltRatekeeperAfterRecruit", self->id)
					    .detail("RKID", ratekeeper.get().id())
					    .detail("DcID", printable(self->clusterControllerDcId));
					RatekeeperSingleton(ratekeeper).halt(*self, ratekeeper.get().locality.processId());
				}
				if (!ratekeeper.present() || ratekeeper.get().id() != interf.get().id()) {
					self->db.setRatekeeper(interf.get());
				}
				checkOutstandingRequests(self);
				return Void();
			}
		} catch (Error& e) {
			TraceEvent("CCRatekeeperRecruitError", self->id).error(e);
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> monitorRatekeeper(ClusterControllerData* self) {
	state SingletonRecruitThrottler recruitThrottler;
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		if (self->db.serverInfo->get().ratekeeper.present() && !self->recruitRatekeeper.get()) {
			choose {
				when(wait(waitFailureClient(self->db.serverInfo->get().ratekeeper.get().waitFailure,
				                            SERVER_KNOBS->RATEKEEPER_FAILURE_TIME))) {
					const auto& ratekeeper = self->db.serverInfo->get().ratekeeper;
					TraceEvent("CCRatekeeperDied", self->id).detail("RKID", ratekeeper.get().id());
					RatekeeperSingleton(ratekeeper).halt(*self, ratekeeper.get().locality.processId());
					self->db.clearInterf(ProcessClass::RatekeeperClass);
				}
				when(wait(self->recruitRatekeeper.onChange())) {}
			}
		} else {
			wait(startRatekeeper(self, recruitThrottler.newRecruitment()));
		}
	}
}

ACTOR Future<Void> startConsistencyScan(ClusterControllerData* self) {
	wait(delay(0.0)); // If master fails at the same time, give it a chance to clear master PID.
	TraceEvent("CCStartConsistencyScan", self->id).log();
	loop {
		try {
			state bool no_consistencyScan = !self->db.serverInfo->get().consistencyScan.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (no_consistencyScan && self->db.serverInfo->get().consistencyScan.present()) {
				// Existing consistencyScan registers while waiting, so skip.
				return Void();
			}

			std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			WorkerFitnessInfo csWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                ProcessClass::ConsistencyScan,
			                                                                ProcessClass::NeverAssign,
			                                                                self->db.config,
			                                                                id_used);

			InitializeConsistencyScanRequest req(deterministicRandom()->randomUniqueID());
			state WorkerDetails worker = csWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::ConsistencyScan)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingConsistencyScanID = req.reqId;
			TraceEvent("CCRecruitConsistencyScan", self->id)
			    .detail("Addr", worker.interf.address())
			    .detail("CSID", req.reqId);

			ErrorOr<ConsistencyScanInterface> interf = wait(worker.interf.consistencyScan.getReplyUnlessFailedFor(
			    req, SERVER_KNOBS->WAIT_FOR_CONSISTENCYSCAN_JOIN_DELAY, 0));
			if (interf.present()) {
				self->recruitConsistencyScan.set(false);
				self->recruitingConsistencyScanID = interf.get().id();
				const auto& consistencyScan = self->db.serverInfo->get().consistencyScan;
				TraceEvent("CCConsistencyScanRecruited", self->id)
				    .detail("Addr", worker.interf.address())
				    .detail("CKID", interf.get().id());
				if (consistencyScan.present() && consistencyScan.get().id() != interf.get().id() &&
				    self->id_worker.contains(consistencyScan.get().locality.processId())) {
					TraceEvent("CCHaltConsistencyScanAfterRecruit", self->id)
					    .detail("CKID", consistencyScan.get().id())
					    .detail("DcID", printable(self->clusterControllerDcId));
					ConsistencyScanSingleton(consistencyScan).halt(*self, consistencyScan.get().locality.processId());
				}
				if (!consistencyScan.present() || consistencyScan.get().id() != interf.get().id()) {
					self->db.setConsistencyScan(interf.get());
				}
				checkOutstandingRequests(self);
				return Void();
			} else {
				TraceEvent("CCConsistencyScanRecruitEmpty", self->id).log();
			}
		} catch (Error& e) {
			TraceEvent("CCConsistencyScanRecruitError", self->id).error(e);
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> monitorConsistencyScan(ClusterControllerData* self) {
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		TraceEvent("CCMonitorConsistencyScanWaitingForRecovery", self->id).log();
		wait(self->db.serverInfo->onChange());
	}
	TraceEvent("CCMonitorConsistencyScan", self->id).log();
	loop {
		if (self->db.serverInfo->get().consistencyScan.present() && !self->recruitConsistencyScan.get()) {
			state Future<Void> wfClient =
			    waitFailureClient(self->db.serverInfo->get().consistencyScan.get().waitFailure,
			                      SERVER_KNOBS->CONSISTENCYSCAN_FAILURE_TIME);
			choose {
				when(wait(wfClient)) {
					TraceEvent("CCMonitorConsistencyScanDied", self->id)
					    .detail("CKID", self->db.serverInfo->get().consistencyScan.get().id());
					self->db.clearInterf(ProcessClass::ConsistencyScanClass);
				}
				when(wait(self->recruitConsistencyScan.onChange())) {}
			}
		} else {
			TraceEvent("CCMonitorConsistencyScanStarting", self->id).log();
			wait(startConsistencyScan(self));
		}
	}
}

ACTOR Future<Void> startEncryptKeyProxy(ClusterControllerData* self, EncryptionAtRestMode encryptMode) {
	// If master fails at the same time, give it a chance to clear master PID.
	wait(delay(0.0));

	TraceEvent("CCEKP_Start", self->id).log();
	loop {
		try {
			// EncryptKeyServer interface is critical in recovering tlog encrypted transactions,
			// hence, the process only waits for the master recruitment and not the full cluster recovery.
			state bool noEncryptKeyServer = !self->db.serverInfo->get().client.encryptKeyProxy.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::LOCKING_CSTATE) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (noEncryptKeyServer && self->db.serverInfo->get().client.encryptKeyProxy.present()) {
				// Existing encryptKeyServer registers while waiting, so skip.
				return Void();
			}

			// Recruit EncryptKeyProxy in the same datacenter as the ClusterController.
			// This should always be possible, given EncryptKeyProxy is stateless, we can recruit EncryptKeyProxy
			// on the same process as the CluserController.
			state std::map<Optional<Standalone<StringRef>>, int> id_used;
			self->updateKnownIds(&id_used);
			state WorkerFitnessInfo ekpWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                       ProcessClass::EncryptKeyProxy,
			                                                                       ProcessClass::NeverAssign,
			                                                                       self->db.config,
			                                                                       id_used);

			InitializeEncryptKeyProxyRequest req(deterministicRandom()->randomUniqueID());
			req.encryptMode = encryptMode;
			state WorkerDetails worker = ekpWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::EncryptKeyProxy)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingEncryptKeyProxyID = req.reqId;
			TraceEvent("CCEKP_Recruit", self->id).detail("Addr", worker.interf.address()).detail("Id", req.reqId);

			ErrorOr<EncryptKeyProxyInterface> interf = wait(worker.interf.encryptKeyProxy.getReplyUnlessFailedFor(
			    req, SERVER_KNOBS->WAIT_FOR_ENCRYPT_KEY_PROXY_JOIN_DELAY, 0));
			if (interf.present()) {
				self->recruitEncryptKeyProxy.set(false);
				self->recruitingEncryptKeyProxyID = interf.get().id();
				const auto& encryptKeyProxy = self->db.serverInfo->get().client.encryptKeyProxy;
				TraceEvent("CCEKP_Recruited", self->id)
				    .detail("Addr", worker.interf.address())
				    .detail("Id", interf.get().id())
				    .detail("ProcessId", interf.get().locality.processId());
				if (encryptKeyProxy.present() && encryptKeyProxy.get().id() != interf.get().id() &&
				    self->id_worker.contains(encryptKeyProxy.get().locality.processId())) {
					TraceEvent("CCEKP_HaltAfterRecruit", self->id)
					    .detail("Id", encryptKeyProxy.get().id())
					    .detail("DcId", printable(self->clusterControllerDcId));
					EncryptKeyProxySingleton(encryptKeyProxy).halt(*self, encryptKeyProxy.get().locality.processId());
				}
				if (!encryptKeyProxy.present() || encryptKeyProxy.get().id() != interf.get().id()) {
					self->db.setEncryptKeyProxy(interf.get());
					TraceEvent("CCEKP_UpdateInf", self->id)
					    .detail("Id", self->db.serverInfo->get().client.encryptKeyProxy.get().id());
				}
				checkOutstandingRequests(self);
				return Void();
			}
		} catch (Error& e) {
			TraceEvent("CCEKP_RecruitError", self->id).error(e);
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> monitorEncryptKeyProxy(ClusterControllerData* self) {
	state EncryptionAtRestMode encryptMode = wait(self->encryptionAtRestMode.getFuture());
	if (!encryptMode.isEncryptionEnabled() && !SERVER_KNOBS->ENABLE_REST_KMS_COMMUNICATION) {
		TraceEvent("EKPNotConfigured");
		return Void();
	}
	state SingletonRecruitThrottler recruitThrottler;
	loop {
		if (self->db.serverInfo->get().client.encryptKeyProxy.present() && !self->recruitEncryptKeyProxy.get()) {
			loop choose {
				when(wait(waitFailureClient(self->db.serverInfo->get().client.encryptKeyProxy.get().waitFailure,
				                            SERVER_KNOBS->ENCRYPT_KEY_PROXY_FAILURE_TIME))) {
					const auto& encryptKeyProxy = self->db.serverInfo->get().client.encryptKeyProxy;
					EncryptKeyProxySingleton(encryptKeyProxy).halt(*self, encryptKeyProxy.get().locality.processId());
					self->db.clearInterf(ProcessClass::EncryptKeyProxyClass);
					TraceEvent("CCEKP_Died", self->id);
					break;
				}
				when(wait(self->recruitEncryptKeyProxy.onChange())) {
					break;
				}
			}
		} else {
			wait(startEncryptKeyProxy(self, encryptMode));
		}
	}
}

// Acquires the BM lock by getting the next epoch no.
ACTOR Future<int64_t> getNextBMEpoch(ClusterControllerData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);

	loop {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		try {
			Optional<Value> oldEpoch = wait(tr->get(blobManagerEpochKey));
			state int64_t newEpoch = oldEpoch.present() ? decodeBlobManagerEpochValue(oldEpoch.get()) + 1 : 1;
			tr->set(blobManagerEpochKey, blobManagerEpochValueFor(newEpoch));

			wait(tr->commit());
			TraceEvent(SevDebug, "CCNextBlobManagerEpoch", self->id).detail("Epoch", newEpoch);
			return newEpoch;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> stopConsistencyScan(Database db) {
	state ConsistencyScanState cs = ConsistencyScanState();
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	loop {
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);
			state ConsistencyScanState::Config config = wait(ConsistencyScanState().config().getD(tr));
			config.enabled = false;
			cs.config().set(tr, config);
			wait(tr->commit());
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> watchBlobRestoreCommand(ClusterControllerData* self) {
	state Reference<BlobRestoreController> restoreController =
	    makeReference<BlobRestoreController>(self->cx, normalKeys);
	loop {
		try {
			state BlobRestorePhase phase = wait(BlobRestoreController::currentPhase(restoreController));
			TraceEvent("WatchBlobRestore", self->id).detail("Phase", phase);
			if (phase == BlobRestorePhase::INIT) {
				if (self->db.blobGranulesEnabled.get()) {
					wait(BlobRestoreController::setPhase(restoreController, STARTING_MIGRATOR, {}));
					const auto& blobManager = self->db.serverInfo->get().blobManager;
					if (blobManager.present()) {
						BlobManagerSingleton(blobManager)
						    .haltBlobGranules(*self, blobManager.get().locality.processId());
					}
					const auto& blobMigrator = self->db.serverInfo->get().blobMigrator;
					if (blobMigrator.present()) {
						BlobMigratorSingleton(blobMigrator).halt(*self, blobMigrator.get().locality.processId());
					}
				} else {
					TraceEvent("SkipBlobRestoreInitCommand", self->id).log();
				}
			}
			self->db.blobRestoreEnabled.set(phase > BlobRestorePhase::UNINIT && phase < BlobRestorePhase::DONE);
			if (self->db.blobRestoreEnabled.get()) {
				wait(stopConsistencyScan(self->cx));
			}
			wait(BlobRestoreController::onPhaseChange(restoreController, BlobRestorePhase::INIT));
		} catch (Error& e) {
			TraceEvent("WatchBlobRestoreCommand", self->id).error(e);
			throw e;
		}
	}
}

ACTOR Future<Void> startBlobMigrator(ClusterControllerData* self, double waitTime) {
	// If master fails at the same time, give it a chance to clear master PID.
	// Also wait to avoid too many consecutive recruits in a small time window.
	wait(delay(waitTime));

	TraceEvent("CCStartBlobMigrator", self->id).log();
	loop {
		try {
			state bool noBlobMigrator = !self->db.serverInfo->get().blobMigrator.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (noBlobMigrator && self->db.serverInfo->get().blobMigrator.present()) {
				// Existing instance registers while waiting, so skip.
				return Void();
			}

			std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			WorkerFitnessInfo blobMigratorWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                          ProcessClass::BlobMigrator,
			                                                                          ProcessClass::NeverAssign,
			                                                                          self->db.config,
			                                                                          id_used);
			InitializeBlobMigratorRequest req(BlobMigratorInterface::newId());
			state WorkerDetails worker = blobMigratorWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::BlobMigrator)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingBlobMigratorID = req.reqId;
			TraceEvent("CCRecruitBlobMigrator", self->id)
			    .detail("Addr", worker.interf.address())
			    .detail("MGID", req.reqId);

			ErrorOr<BlobMigratorInterface> interf = wait(worker.interf.blobMigrator.getReplyUnlessFailedFor(
			    req, SERVER_KNOBS->WAIT_FOR_BLOB_MANAGER_JOIN_DELAY, 0));

			if (interf.present()) {
				self->recruitBlobMigrator.set(false);
				self->recruitingBlobMigratorID = interf.get().id();
				const auto& blobMigrator = self->db.serverInfo->get().blobMigrator;
				TraceEvent("CCBlobMigratorRecruited", self->id)
				    .detail("Addr", worker.interf.address())
				    .detail("MGID", interf.get().id());
				if (blobMigrator.present() && blobMigrator.get().id() != interf.get().id() &&
				    self->id_worker.contains(blobMigrator.get().locality.processId())) {
					TraceEvent("CCHaltBlobMigratorAfterRecruit", self->id)
					    .detail("MGID", blobMigrator.get().id())
					    .detail("DcID", printable(self->clusterControllerDcId));
					BlobMigratorSingleton(blobMigrator).halt(*self, blobMigrator.get().locality.processId());
				}
				if (!blobMigrator.present() || blobMigrator.get().id() != interf.get().id()) {
					self->db.setBlobMigrator(interf.get());
				}
				checkOutstandingRequests(self);
				return Void();
			}
		} catch (Error& e) {
			TraceEvent("CCBlobMigratorRecruitError", self->id).error(e);
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> monitorBlobMigrator(ClusterControllerData* self) {
	state SingletonRecruitThrottler recruitThrottler;
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}
	loop {
		if (self->db.serverInfo->get().blobMigrator.present() && !self->recruitBlobMigrator.get()) {
			state Future<Void> wfClient = waitFailureClient(self->db.serverInfo->get().blobMigrator.get().waitFailure,
			                                                SERVER_KNOBS->BLOB_MIGRATOR_FAILURE_TIME);
			loop {
				choose {
					when(wait(wfClient)) {
						UID mgID = self->db.serverInfo->get().blobMigrator.get().id();
						TraceEvent("CCBlobMigratorDied", self->id).detail("MGID", mgID);
						self->db.clearInterf(ProcessClass::BlobMigratorClass);
						break;
					}
					when(wait(self->recruitBlobMigrator.onChange())) {}
				}
			}
		} else if (self->db.blobGranulesEnabled.get() && self->db.blobRestoreEnabled.get()) {
			// if there is no blob migrator present but blob granules are now enabled, recruit a BM
			wait(startBlobMigrator(self, recruitThrottler.newRecruitment()));
		} else {
			wait(self->db.blobGranulesEnabled.onChange() || self->db.blobRestoreEnabled.onChange());
		}
	}
}

ACTOR Future<Void> startBlobManager(ClusterControllerData* self, double waitTime) {
	// If master fails at the same time, give it a chance to clear master PID.
	// Also wait to avoid too many consecutive recruits in a small time window.
	wait(delay(waitTime));

	TraceEvent("CCStartBlobManager", self->id).log();
	loop {
		try {
			state bool noBlobManager = !self->db.serverInfo->get().blobManager.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (noBlobManager && self->db.serverInfo->get().blobManager.present()) {
				// Existing blob manager registers while waiting, so skip.
				return Void();
			}

			state std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			state WorkerFitnessInfo bmWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                      ProcessClass::BlobManager,
			                                                                      ProcessClass::NeverAssign,
			                                                                      self->db.config,
			                                                                      id_used);

			int64_t nextEpoch = wait(getNextBMEpoch(self));
			if (!self->masterProcessId.present() ||
			    self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			    self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				continue;
			}
			InitializeBlobManagerRequest req(deterministicRandom()->randomUniqueID(), nextEpoch);
			state WorkerDetails worker = bmWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::BlobManager)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingBlobManagerID = req.reqId;
			TraceEvent("CCRecruitBlobManager", self->id)
			    .detail("Addr", worker.interf.address())
			    .detail("BMID", req.reqId)
			    .detail("Epoch", nextEpoch);

			ErrorOr<BlobManagerInterface> interf = wait(worker.interf.blobManager.getReplyUnlessFailedFor(
			    req, SERVER_KNOBS->WAIT_FOR_BLOB_MANAGER_JOIN_DELAY, 0));
			if (interf.present()) {
				self->recruitBlobManager.set(false);
				self->recruitingBlobManagerID = interf.get().id();
				const auto& blobManager = self->db.serverInfo->get().blobManager;
				TraceEvent("CCBlobManagerRecruited", self->id)
				    .detail("Addr", worker.interf.address())
				    .detail("BMID", interf.get().id());
				if (blobManager.present() && blobManager.get().id() != interf.get().id() &&
				    self->id_worker.contains(blobManager.get().locality.processId())) {
					TraceEvent("CCHaltBlobManagerAfterRecruit", self->id)
					    .detail("BMID", blobManager.get().id())
					    .detail("DcID", printable(self->clusterControllerDcId));
					BlobManagerSingleton(blobManager).halt(*self, blobManager.get().locality.processId());
				}
				if (!blobManager.present() || blobManager.get().id() != interf.get().id()) {
					self->db.setBlobManager(interf.get());
				}
				checkOutstandingRequests(self);
				return Void();
			}
		} catch (Error& e) {
			TraceEvent("CCBlobManagerRecruitError", self->id).error(e);
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> watchBlobGranulesConfigKey(ClusterControllerData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	state Key blobGranuleConfigKey = configKeysPrefix.withSuffix("blob_granules_enabled"_sr);

	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> blobConfig = wait(tr->get(blobGranuleConfigKey));
			if (blobConfig.present()) {
				self->db.blobGranulesEnabled.set(blobConfig.get() == "1"_sr);
			}

			state Future<Void> watch = tr->watch(blobGranuleConfigKey);
			wait(tr->commit());
			wait(watch);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> monitorBlobManager(ClusterControllerData* self) {
	state SingletonRecruitThrottler recruitThrottler;
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		if (self->db.serverInfo->get().blobManager.present() && !self->recruitBlobManager.get()) {
			state Future<Void> wfClient = waitFailureClient(self->db.serverInfo->get().blobManager.get().waitFailure,
			                                                SERVER_KNOBS->BLOB_MANAGER_FAILURE_TIME);
			loop {
				choose {
					when(wait(wfClient)) {
						const auto& blobManager = self->db.serverInfo->get().blobManager;
						TraceEvent("CCBlobManagerDied", self->id).detail("BMID", blobManager.get().id());
						BlobManagerSingleton(blobManager).halt(*self, blobManager.get().locality.processId());
						self->db.clearInterf(ProcessClass::BlobManagerClass);
						break;
					}
					when(wait(self->recruitBlobManager.onChange())) {
						break;
					}
					when(wait(self->db.blobGranulesEnabled.onChange())) {
						// if there is a blob manager present but blob granules are now disabled, stop the BM
						if (!self->db.blobGranulesEnabled.get()) {
							const auto& blobManager = self->db.serverInfo->get().blobManager;
							BlobManagerSingleton(blobManager)
							    .haltBlobGranules(*self, blobManager.get().locality.processId());
							if (self->db.blobRestoreEnabled.get()) {
								const auto& blobMigrator = self->db.serverInfo->get().blobMigrator;
								BlobMigratorSingleton(blobMigrator)
								    .halt(*self, blobMigrator.get().locality.processId());
							}
							break;
						}
					}
				}
			}
		} else if (self->db.blobGranulesEnabled.get()) {
			// if there is no blob manager present but blob granules are now enabled, recruit a BM
			wait(startBlobManager(self, recruitThrottler.newRecruitment()));
		} else {
			// if there is no blob manager present and blob granules are disabled, wait for a config change
			wait(self->db.blobGranulesEnabled.onChange());
		}
	}
}

ACTOR Future<Void> dbInfoUpdater(ClusterControllerData* self) {
	state Future<Void> dbInfoChange = self->db.serverInfo->onChange();
	state Future<Void> updateDBInfo = self->updateDBInfo.onTrigger();
	loop {
		choose {
			when(wait(updateDBInfo)) {
				wait(delay(SERVER_KNOBS->DBINFO_BATCH_DELAY) || dbInfoChange);
			}
			when(wait(dbInfoChange)) {}
		}

		state UpdateServerDBInfoRequest req;
		if (dbInfoChange.isReady()) {
			for (auto& it : self->id_worker) {
				req.broadcastInfo.push_back(it.second.details.interf.updateServerDBInfo.getEndpoint());
			}
		} else {
			for (auto it : self->removedDBInfoEndpoints) {
				self->updateDBInfoEndpoints.erase(it);
			}
			req.broadcastInfo =
			    std::vector<Endpoint>(self->updateDBInfoEndpoints.begin(), self->updateDBInfoEndpoints.end());
		}

		self->updateDBInfoEndpoints.clear();
		self->removedDBInfoEndpoints.clear();

		dbInfoChange = self->db.serverInfo->onChange();
		updateDBInfo = self->updateDBInfo.onTrigger();

		req.serializedDbInfo =
		    BinaryWriter::toValue(self->db.serverInfo->get(), AssumeVersion(g_network->protocolVersion()));

		TraceEvent("DBInfoStartBroadcast", self->id)
		    .detail("MasterLifetime", self->db.serverInfo->get().masterLifetime.toString());
		choose {
			when(std::vector<Endpoint> notUpdated =
			         wait(broadcastDBInfoRequest(req, SERVER_KNOBS->DBINFO_SEND_AMOUNT, Optional<Endpoint>(), false))) {
				TraceEvent("DBInfoFinishBroadcast", self->id).detail("NotUpdated", notUpdated.size());
				if (notUpdated.size()) {
					self->updateDBInfoEndpoints.insert(notUpdated.begin(), notUpdated.end());
					self->updateDBInfo.trigger();
				}
			}
			when(wait(dbInfoChange)) {}
			when(wait(updateDBInfo)) {
				// The current round of broadcast hasn't finished yet. So we need to include all the current broadcast
				// endpoints in the new round as well.
				self->updateDBInfoEndpoints.insert(req.broadcastInfo.begin(), req.broadcastInfo.end());
			}
		}
	}
}

// If we are excluding processes and triggering recovery because of gray failure, also
// invalidate the past complaints from such processes because that signal is no longer
// reliable.
static void invalidateExcludedProcessComplaints(ClusterControllerData* self) {
	if (!SERVER_KNOBS->CC_INVALIDATE_EXCLUDED_PROCESSES) {
		return;
	}
	for (const auto& [addr, _] : self->excludedDegradedServers) {
		self->workerHealth.erase(addr);
	}
}

// The actor that periodically monitors the health of tracked workers.
ACTOR Future<Void> workerHealthMonitor(ClusterControllerData* self) {
	loop {
		try {
			while (!self->goodRecruitmentTime.isReady()) {
				wait(lowPriorityDelay(SERVER_KNOBS->CC_WORKER_HEALTH_CHECKING_INTERVAL));
			}
			self->degradationInfo = self->getDegradationInfo();

			// Compare `self->degradationInfo` with `self->excludedDegradedServers` and remove those that have
			// recovered.
			bool hasRecoveredServer = false;
			for (auto it = self->excludedDegradedServers.begin(); it != self->excludedDegradedServers.end();) {
				if (self->degradationInfo.degradedServers.find(it->first) ==
				        self->degradationInfo.degradedServers.end() &&
				    self->degradationInfo.disconnectedServers.find(it->first) ==
				        self->degradationInfo.disconnectedServers.end()) {
					self->excludedDegradedServers.erase(it++);
					hasRecoveredServer = true;
				} else {
					++it;
				}
			}

			if (!self->degradationInfo.degradedServers.empty() || !self->degradationInfo.disconnectedServers.empty() ||
			    self->degradationInfo.degradedSatellite) {
				std::string degradedServerString;
				for (const auto& server : self->degradationInfo.degradedServers) {
					degradedServerString += server.toString() + " ";
				}
				std::string disconnectedServerString;
				for (const auto& server : self->degradationInfo.disconnectedServers) {
					disconnectedServerString += server.toString() + " ";
				}
				TraceEvent("ClusterControllerHealthMonitor")
				    .detail("DegradedServers", degradedServerString)
				    .detail("DisconnectedServers", disconnectedServerString)
				    .detail("DegradedSatellite", self->degradationInfo.degradedSatellite);

				// Check if the cluster controller should trigger a recovery to exclude any degraded servers from
				// the transaction system.
				if (self->shouldTriggerRecoveryDueToDegradedServers()) {
					if (SERVER_KNOBS->CC_HEALTH_TRIGGER_RECOVERY) {
						if (self->recentRecoveryCountDueToHealth() < SERVER_KNOBS->CC_MAX_HEALTH_RECOVERY_COUNT) {
							self->excludedDegradedServers.clear();
							for (const auto& degradedServer : self->degradationInfo.degradedServers) {
								self->excludedDegradedServers[degradedServer] = now();
							}
							for (const auto& disconnectedServer : self->degradationInfo.disconnectedServers) {
								self->excludedDegradedServers[disconnectedServer] = now();
							}
							invalidateExcludedProcessComplaints(self);
							TraceEvent(SevWarnAlways, "DegradedServerDetectedAndTriggerRecovery")
							    .detail("RecentRecoveryCountDueToHealth", self->recentRecoveryCountDueToHealth());
							self->db.forceMasterFailure.trigger();
						}
					} else {
						self->excludedDegradedServers.clear();
						TraceEvent(SevWarn, "DegradedServerDetectedAndSuggestRecovery").log();
					}
				} else if (self->shouldTriggerFailoverDueToDegradedServers()) {
					double ccUpTime = now() - machineStartTime();
					if (SERVER_KNOBS->CC_HEALTH_TRIGGER_FAILOVER &&
					    ccUpTime > SERVER_KNOBS->INITIAL_UPDATE_CROSS_DC_INFO_DELAY) {
						TraceEvent(SevWarn, "DegradedServerDetectedAndTriggerFailover").log();
						std::vector<Optional<Key>> dcPriority;
						auto remoteDcId = self->db.config.regions[0].dcId == self->clusterControllerDcId.get()
						                      ? self->db.config.regions[1].dcId
						                      : self->db.config.regions[0].dcId;

						// Switch the current primary DC and remote DC in desiredDcIds, so that the remote DC
						// becomes the new primary, and the primary DC becomes the new remote.
						dcPriority.push_back(remoteDcId);
						dcPriority.push_back(self->clusterControllerDcId);
						self->desiredDcIds.set(dcPriority);
					} else {
						TraceEvent(SevWarn, "DegradedServerDetectedAndSuggestFailover").detail("CCUpTime", ccUpTime);
					}
				}
			}

			if (hasRecoveredServer) {
				// The best transaction system might have changed after a server is recovered. Check outstanding request
				// and check if a better transaction system exists.
				checkOutstandingRequests(self);
			}

			wait(delay(SERVER_KNOBS->CC_WORKER_HEALTH_CHECKING_INTERVAL));
		} catch (Error& e) {
			TraceEvent(SevWarnAlways, "ClusterControllerHealthMonitorError").error(e);
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
		}
	}
}

ACTOR Future<Void> metaclusterMetricsUpdater(ClusterControllerData* self) {
	state Future<Void> updaterDelay = Void();
	loop {
		choose {
			when(wait(self->db.serverInfo->onChange())) {
				updaterDelay = Void();
			}
			when(wait(self->db.clusterType == ClusterType::METACLUSTER_MANAGEMENT ? updaterDelay : Never())) {
				try {
					wait(store(self->db.metaclusterMetrics,
					           metacluster::MetaclusterMetrics::getMetaclusterMetrics(self->cx)));
				} catch (Error& e) {
					// Ignore errors about the cluster changing type
					if (e.code() != error_code_invalid_metacluster_operation) {
						throw;
					}
				}

				updaterDelay = delay(60.0);
			}
		}
	}
}

// Update the DBInfo state with this processes cluster ID. If this process does
// not have a cluster ID and one does not exist in the database, generate one.
ACTOR Future<Void> updateClusterId(ClusterControllerData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	loop {
		try {
			state Optional<UID> durableClusterId = self->clusterId->get();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			Optional<Value> clusterIdVal = wait(tr->get(clusterIdKey));

			if (clusterIdVal.present()) {
				UID clusterId = BinaryReader::fromStringRef<UID>(clusterIdVal.get(), IncludeVersion());
				if (durableClusterId.present()) {
					// If this process has an on disk file for the cluster ID,
					// verify it matches the value in the database.
					ASSERT(clusterId == durableClusterId.get());
				} else {
					// Otherwise, write the cluster ID in the database to the
					// DbInfo object so all clients will learn of the cluster
					// ID.
					durableClusterId = clusterId;
				}
			} else if (!durableClusterId.present()) {
				// No cluster ID exists in the database or on the machine. Generate and set one.
				ASSERT(!durableClusterId.present());
				durableClusterId = deterministicRandom()->randomUniqueID();
				tr->set(clusterIdKey, BinaryWriter::toValue(durableClusterId.get(), IncludeVersion()));
				wait(tr->commit());
			}
			auto serverInfo = self->db.serverInfo->get();
			if (!serverInfo.client.clusterId.isValid()) {
				ASSERT(durableClusterId.present());
				serverInfo.id = deterministicRandom()->randomUniqueID();
				serverInfo.client.clusterId = durableClusterId.get();
				self->db.serverInfo->set(serverInfo);

				ClientDBInfo clientInfo = self->db.clientInfo->get();
				clientInfo.id = deterministicRandom()->randomUniqueID();
				clientInfo.clusterId = durableClusterId.get();
				self->db.clientInfo->set(clientInfo);
			}
			return Void();
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> handleGetEncryptionAtRestMode(ClusterControllerData* self, ClusterControllerFullInterface ccInterf) {
	loop {
		state GetEncryptionAtRestModeRequest req = waitNext(ccInterf.getEncryptionAtRestMode.getFuture());
		TraceEvent("HandleGetEncryptionAtRestModeStart").detail("TlogId", req.tlogId);
		EncryptionAtRestMode mode = wait(self->encryptionAtRestMode.getFuture());
		GetEncryptionAtRestModeResponse resp;
		resp.mode = mode;
		req.reply.send(resp);
		TraceEvent("HandleGetEncryptionAtRestModeEnd").detail("TlogId", req.tlogId).detail("Mode", resp.mode);
	}
}

ACTOR Future<Void> clusterControllerCore(ClusterControllerFullInterface interf,
                                         Future<Void> leaderFail,
                                         ServerCoordinators coordinators,
                                         LocalityData locality,
                                         ConfigDBType configDBType,
                                         Reference<AsyncVar<Optional<UID>>> clusterId) {
	state ClusterControllerData self(interf, locality, coordinators, clusterId);
	state Future<Void> coordinationPingDelay = delay(SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY);
	state uint64_t step = 0;
	state Future<ErrorOr<Void>> error = errorOr(actorCollection(self.addActor.getFuture()));
	state ConfigBroadcaster configBroadcaster;
	if (configDBType != ConfigDBType::DISABLED) {
		configBroadcaster = ConfigBroadcaster(coordinators, configDBType, getPreviousCoordinators(&self));
	}

	// EncryptKeyProxy is necessary for TLog recovery, recruit it as the first process
	self.addActor.send(monitorEncryptKeyProxy(&self));
	self.addActor.send(clusterWatchDatabase(&self, &self.db, coordinators)); // Start the master database
	self.addActor.send(self.updateWorkerList.init(self.db.db));
	self.addActor.send(statusServer(interf.clientInterface.databaseStatus.getFuture(),
	                                &self,
	                                coordinators,
	                                (configDBType == ConfigDBType::DISABLED) ? nullptr : &configBroadcaster));
	self.addActor.send(timeKeeper(&self));
	self.addActor.send(monitorProcessClasses(&self));
	self.addActor.send(monitorServerInfoConfig(&self.db));
	self.addActor.send(monitorStorageMetadata(&self));
	self.addActor.send(monitorGlobalConfig(&self.db));
	self.addActor.send(updatedChangingDatacenters(&self));
	self.addActor.send(updatedChangedDatacenters(&self));
	self.addActor.send(updateDatacenterVersionDifference(&self));
	self.addActor.send(handleForcedRecoveries(&self, interf));
	self.addActor.send(handleTriggerAuditStorage(&self, interf));
	self.addActor.send(monitorDataDistributor(&self));
	self.addActor.send(monitorRatekeeper(&self));
	self.addActor.send(monitorBlobManager(&self));
	self.addActor.send(watchBlobGranulesConfigKey(&self));
	self.addActor.send(monitorBlobMigrator(&self));
	self.addActor.send(watchBlobRestoreCommand(&self));
	self.addActor.send(monitorConsistencyScan(&self));
	self.addActor.send(metaclusterMetricsUpdater(&self));
	self.addActor.send(dbInfoUpdater(&self));
	self.addActor.send(updateClusterId(&self));
	self.addActor.send(handleGetEncryptionAtRestMode(&self, interf));
	self.addActor.send(self.clusterControllerMetrics.traceCounters("ClusterControllerMetrics",
	                                                               self.id,
	                                                               SERVER_KNOBS->STORAGE_LOGGING_DELAY,
	                                                               self.id.toString() + "/ClusterControllerMetrics"));
	self.addActor.send(traceRole(Role::CLUSTER_CONTROLLER, interf.id()));
	// printf("%s: I am the cluster controller\n", g_network->getLocalAddress().toString().c_str());
	if (SERVER_KNOBS->CC_ENABLE_WORKER_HEALTH_MONITOR) {
		self.addActor.send(workerHealthMonitor(&self));
		self.addActor.send(updateRemoteDCHealth(&self));
	}

	loop choose {
		when(ErrorOr<Void> err = wait(error)) {
			if (err.isError() && err.getError().code() != error_code_restart_cluster_controller) {
				endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Stop Received Error", false, err.getError());
			} else {
				endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Stop Received Signal", true);
			}

			// We shut down normally even if there was a serious error (so this fdbserver may be re-elected
			// cluster controller)
			return Void();
		}
		when(OpenDatabaseRequest req = waitNext(interf.clientInterface.openDatabase.getFuture())) {
			++self.openDatabaseRequests;
			self.addActor.send(clusterOpenDatabase(&self.db, req));
		}
		when(RecruitStorageRequest req = waitNext(interf.recruitStorage.getFuture())) {
			clusterRecruitStorage(&self, req);
		}
		when(RecruitBlobWorkerRequest req = waitNext(interf.recruitBlobWorker.getFuture())) {
			clusterRecruitBlobWorker(&self, req);
		}
		when(RegisterWorkerRequest req = waitNext(interf.registerWorker.getFuture())) {
			++self.registerWorkerRequests;
			self.addActor.send(registerWorker(req,
			                                  &self,
			                                  coordinators.ccr->getConnectionString(),
			                                  (configDBType == ConfigDBType::DISABLED) ? nullptr : &configBroadcaster));
		}
		when(GetWorkersRequest req = waitNext(interf.getWorkers.getFuture())) {
			++self.getWorkersRequests;
			std::vector<WorkerDetails> workers;

			for (auto const& [id, worker] : self.id_worker) {
				if ((req.flags & GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY) &&
				    self.db.config.isExcludedServer(worker.details.interf.addresses(),
				                                    worker.details.interf.locality)) {
					continue;
				}

				if ((req.flags & GetWorkersRequest::TESTER_CLASS_ONLY) &&
				    worker.details.processClass.classType() != ProcessClass::TesterClass) {
					continue;
				}

				workers.push_back(worker.details);
			}

			req.reply.send(workers);
		}
		when(GetClientWorkersRequest req = waitNext(interf.clientInterface.getClientWorkers.getFuture())) {
			++self.getClientWorkersRequests;
			std::vector<ClientWorkerInterface> workers;
			for (auto& it : self.id_worker) {
				if (it.second.details.processClass.classType() != ProcessClass::TesterClass) {
					workers.push_back(it.second.details.interf.clientInterface);
				}
			}
			req.reply.send(workers);
		}
		when(wait(coordinationPingDelay)) {
			CoordinationPingMessage message(self.id, step++);
			for (auto& it : self.id_worker)
				it.second.details.interf.coordinationPing.send(message);
			coordinationPingDelay = delay(SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY);
			TraceEvent("CoordinationPingSent", self.id).detail("TimeStep", message.timeStep);
		}
		when(RegisterMasterRequest req = waitNext(interf.registerMaster.getFuture())) {
			++self.registerMasterRequests;
			clusterRegisterMaster(&self, req);
		}
		when(UpdateWorkerHealthRequest req = waitNext(interf.updateWorkerHealth.getFuture())) {
			if (SERVER_KNOBS->CC_ENABLE_WORKER_HEALTH_MONITOR) {
				self.updateWorkerHealth(req);
			}
		}
		when(GetServerDBInfoRequest req = waitNext(interf.getServerDBInfo.getFuture())) {
			self.addActor.send(clusterGetServerInfo(&self.db, req.knownServerInfoID, req.reply));
		}
		when(wait(leaderFail)) {
			// We are no longer the leader if this has changed.
			endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Leader Replaced", true);
			CODE_PROBE(true, "Leader replaced");
			return Void();
		}
		when(ReplyPromise<Void> ping = waitNext(interf.clientInterface.ping.getFuture())) {
			ping.send(Void());
		}
	}
}

ACTOR Future<Void> replaceInterface(ClusterControllerFullInterface interf) {
	loop {
		if (interf.hasMessage()) {
			wait(delay(SERVER_KNOBS->REPLACE_INTERFACE_DELAY));
			return Void();
		}
		wait(delay(SERVER_KNOBS->REPLACE_INTERFACE_CHECK_DELAY));
	}
}

ACTOR Future<Void> clusterController(ServerCoordinators coordinators,
                                     Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC,
                                     bool hasConnected,
                                     Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo,
                                     LocalityData locality,
                                     ConfigDBType configDBType,
                                     Reference<AsyncVar<Optional<UID>>> clusterId) {
	loop {
		state ClusterControllerFullInterface cci;
		state bool inRole = false;
		cci.initEndpoints();
		try {
			// Register as a possible leader; wait to be elected
			state Future<Void> leaderFail =
			    tryBecomeLeader(coordinators, cci, currentCC, hasConnected, asyncPriorityInfo);
			state Future<Void> shouldReplace = replaceInterface(cci);

			while (!currentCC->get().present() || currentCC->get().get() != cci) {
				choose {
					when(wait(currentCC->onChange())) {}
					when(wait(leaderFail)) {
						ASSERT(false);
						throw internal_error();
					}
					when(wait(shouldReplace)) {
						break;
					}
				}
			}
			if (!shouldReplace.isReady()) {
				shouldReplace = Future<Void>();
				hasConnected = true;
				startRole(Role::CLUSTER_CONTROLLER, cci.id(), UID());
				inRole = true;

				wait(clusterControllerCore(cci, leaderFail, coordinators, locality, configDBType, clusterId));
			}
		} catch (Error& e) {
			if (inRole)
				endRole(Role::CLUSTER_CONTROLLER,
				        cci.id(),
				        "Error",
				        e.code() == error_code_actor_cancelled || e.code() == error_code_coordinators_changed,
				        e);
			else
				TraceEvent(e.code() == error_code_coordinators_changed ? SevInfo : SevError,
				           "ClusterControllerCandidateError",
				           cci.id())
				    .error(e);
			throw;
		}
	}
}

ACTOR Future<Void> clusterController(Reference<IClusterConnectionRecord> connRecord,
                                     Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC,
                                     Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo,
                                     LocalityData locality,
                                     ConfigDBType configDBType,
                                     Reference<AsyncVar<Optional<UID>>> clusterId) {
	state bool hasConnected = false;
	loop {
		try {
			ServerCoordinators coordinators(connRecord, configDBType);
			wait(clusterController(
			    coordinators, currentCC, hasConnected, asyncPriorityInfo, locality, configDBType, clusterId));
			hasConnected = true;
		} catch (Error& e) {
			if (e.code() != error_code_coordinators_changed)
				throw; // Expected to terminate fdbserver
		}
	}
}

namespace {

void addProcessesToSameDC(ClusterControllerData& self, const std::vector<NetworkAddress>&& processes) {
	LocalityData locality;
	locality.set(LocalityData::keyDcId, Standalone<StringRef>(std::string{ "1" }));
	for (const auto& process : processes) {
		const bool added = self.addr_locality.insert({ process, locality }).second;
		ASSERT(added);
	}
}

// Tests `ClusterControllerData::updateWorkerHealth()` can update `ClusterControllerData::workerHealth`
// based on `UpdateWorkerHealth` request correctly.
TEST_CASE("/fdbserver/clustercontroller/updateWorkerHealth") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	state ClusterControllerData data(ClusterControllerFullInterface(),
	                                 LocalityData(),
	                                 ServerCoordinators(Reference<IClusterConnectionRecord>(
	                                     new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                                 makeReference<AsyncVar<Optional<UID>>>());
	state NetworkAddress workerAddress(IPAddress(0x01010101), 1);
	state NetworkAddress badPeer1(IPAddress(0x02020202), 1);
	state NetworkAddress badPeer2(IPAddress(0x03030303), 1);
	state NetworkAddress badPeer3(IPAddress(0x04040404), 1);

	// Create a `UpdateWorkerHealthRequest` with two bad peers, and they should appear in the
	// `workerAddress`'s degradedPeers.
	{
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		req.degradedPeers.push_back(badPeer1);
		req.degradedPeers.push_back(badPeer2);
		req.disconnectedPeers.push_back(badPeer1);
		req.disconnectedPeers.push_back(badPeer2);
		data.updateWorkerHealth(req);
		ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
		auto& health = data.workerHealth[workerAddress];
		ASSERT_EQ(health.degradedPeers.size(), 2);
		ASSERT(health.degradedPeers.find(badPeer1) != health.degradedPeers.end());
		ASSERT_EQ(health.degradedPeers[badPeer1].startTime, health.degradedPeers[badPeer1].lastRefreshTime);
		ASSERT(health.degradedPeers.find(badPeer2) != health.degradedPeers.end());
		ASSERT_EQ(health.degradedPeers[badPeer2].startTime, health.degradedPeers[badPeer2].lastRefreshTime);
		ASSERT_EQ(health.disconnectedPeers.size(), 2);
		ASSERT(health.disconnectedPeers.find(badPeer1) != health.disconnectedPeers.end());
		ASSERT_EQ(health.disconnectedPeers[badPeer1].startTime, health.disconnectedPeers[badPeer1].lastRefreshTime);
		ASSERT(health.disconnectedPeers.find(badPeer2) != health.disconnectedPeers.end());
		ASSERT_EQ(health.disconnectedPeers[badPeer2].startTime, health.disconnectedPeers[badPeer2].lastRefreshTime);
	}

	// Create a `UpdateWorkerHealthRequest` with two bad peers, one from the previous test and a new one.
	// The one from the previous test should have lastRefreshTime updated.
	// The other one from the previous test not included in this test should not be removed.
	state double previousStartTime;
	state double previousRefreshTime;
	{
		// Make the time to move so that now() guarantees to return a larger value than before.
		wait(delay(0.001));
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		req.degradedPeers.push_back(badPeer1);
		req.degradedPeers.push_back(badPeer3);
		req.disconnectedPeers.push_back(badPeer1);
		req.disconnectedPeers.push_back(badPeer3);
		data.updateWorkerHealth(req);
		ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
		auto& health = data.workerHealth[workerAddress];
		ASSERT_EQ(health.degradedPeers.size(), 3);
		ASSERT(health.degradedPeers.find(badPeer1) != health.degradedPeers.end());
		ASSERT_LT(health.degradedPeers[badPeer1].startTime, health.degradedPeers[badPeer1].lastRefreshTime);
		ASSERT(health.degradedPeers.find(badPeer2) != health.degradedPeers.end());
		ASSERT_EQ(health.degradedPeers[badPeer2].startTime, health.degradedPeers[badPeer2].lastRefreshTime);
		ASSERT_EQ(health.degradedPeers[badPeer2].startTime, health.degradedPeers[badPeer1].startTime);
		ASSERT(health.degradedPeers.find(badPeer3) != health.degradedPeers.end());
		ASSERT_EQ(health.degradedPeers[badPeer3].startTime, health.degradedPeers[badPeer3].lastRefreshTime);
		ASSERT_EQ(health.disconnectedPeers.size(), 3);
		ASSERT(health.disconnectedPeers.find(badPeer1) != health.disconnectedPeers.end());
		ASSERT_LT(health.disconnectedPeers[badPeer1].startTime, health.disconnectedPeers[badPeer1].lastRefreshTime);
		ASSERT(health.disconnectedPeers.find(badPeer2) != health.disconnectedPeers.end());
		ASSERT_EQ(health.disconnectedPeers[badPeer2].startTime, health.disconnectedPeers[badPeer2].lastRefreshTime);
		ASSERT_EQ(health.disconnectedPeers[badPeer2].startTime, health.disconnectedPeers[badPeer1].startTime);
		ASSERT(health.disconnectedPeers.find(badPeer3) != health.disconnectedPeers.end());
		ASSERT_EQ(health.disconnectedPeers[badPeer3].startTime, health.disconnectedPeers[badPeer3].lastRefreshTime);

		previousStartTime = health.degradedPeers[badPeer3].startTime;
		previousRefreshTime = health.degradedPeers[badPeer3].lastRefreshTime;
	}

	// Create a `UpdateWorkerHealthRequest` with empty `degradedPeers`, which should not remove the worker
	// from `workerHealth`.
	{
		wait(delay(0.001));
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		data.updateWorkerHealth(req);
		ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
		auto& health = data.workerHealth[workerAddress];
		ASSERT_EQ(health.degradedPeers.size(), 3);
		ASSERT(health.degradedPeers.find(badPeer3) != health.degradedPeers.end());
		ASSERT_EQ(health.degradedPeers[badPeer3].startTime, previousStartTime);
		ASSERT_EQ(health.degradedPeers[badPeer3].lastRefreshTime, previousRefreshTime);
		ASSERT_EQ(health.disconnectedPeers.size(), 3);
		ASSERT(health.disconnectedPeers.find(badPeer3) != health.disconnectedPeers.end());
		ASSERT_EQ(health.disconnectedPeers[badPeer3].startTime, previousStartTime);
		ASSERT_EQ(health.disconnectedPeers[badPeer3].lastRefreshTime, previousRefreshTime);
	}

	// Make badPeer1 a recovered peer, and CC should remove it from `workerAddress` bad peers.
	{
		wait(delay(0.001));
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		req.recoveredPeers.push_back(badPeer1);
		data.updateWorkerHealth(req);
		auto& health = data.workerHealth[workerAddress];
		ASSERT(health.degradedPeers.find(badPeer1) == health.degradedPeers.end());
		ASSERT(health.disconnectedPeers.find(badPeer1) == health.disconnectedPeers.end());
	}

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/updateRecoveredWorkers") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<IClusterConnectionRecord>(
	                               new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                           makeReference<AsyncVar<Optional<UID>>>());
	NetworkAddress worker1(IPAddress(0x01010101), 1);
	NetworkAddress worker2(IPAddress(0x11111111), 1);
	NetworkAddress badPeer1(IPAddress(0x02020202), 1);
	NetworkAddress badPeer2(IPAddress(0x03030303), 1);
	NetworkAddress disconnectedPeer3(IPAddress(0x04040404), 1);

	// Create following test scenario:
	// 	 worker1 -> badPeer1 active
	// 	 worker1 -> badPeer2 recovered
	//   worker1 -> disconnectedPeer3 active
	// 	 worker2 -> badPeer2 recovered
	//   worker2 -> disconnectedPeer3 recovered
	data.workerHealth[worker1].degradedPeers[badPeer1] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1, now()
	};
	data.workerHealth[worker1].degradedPeers[badPeer2] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
	};
	data.workerHealth[worker1].degradedPeers[disconnectedPeer3] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1, now()
	};
	data.workerHealth[worker2].degradedPeers[badPeer2] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
	};
	data.workerHealth[worker2].degradedPeers[disconnectedPeer3] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
	};
	data.updateRecoveredWorkers();

	ASSERT_EQ(data.workerHealth.size(), 1);
	ASSERT(data.workerHealth.find(worker1) != data.workerHealth.end());
	ASSERT(data.workerHealth[worker1].degradedPeers.find(badPeer1) != data.workerHealth[worker1].degradedPeers.end());
	ASSERT(data.workerHealth[worker1].degradedPeers.find(badPeer2) == data.workerHealth[worker1].degradedPeers.end());
	ASSERT(data.workerHealth[worker1].degradedPeers.find(disconnectedPeer3) !=
	       data.workerHealth[worker1].degradedPeers.end());
	ASSERT(data.workerHealth.find(worker2) == data.workerHealth.end());

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/getDegradationInfo") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<IClusterConnectionRecord>(
	                               new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                           makeReference<AsyncVar<Optional<UID>>>());
	NetworkAddress worker(IPAddress(0x01010101), 1);
	NetworkAddress badPeer1(IPAddress(0x02020202), 1);
	NetworkAddress badPeer2(IPAddress(0x03030303), 1);
	NetworkAddress badPeer3(IPAddress(0x04040404), 1);
	NetworkAddress badPeer4(IPAddress(0x05050505), 1);

	if (SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY) {
		addProcessesToSameDC(data, { worker, badPeer1, badPeer2, badPeer3, badPeer4 });
	}

	// Test that a reported degraded link should stay for sometime before being considered as a degraded
	// link by cluster controller.
	{
		data.workerHealth[worker].degradedPeers[badPeer1] = { now(), now() };
		data.workerHealth[worker].disconnectedPeers[badPeer2] = { now(), now() };
		ASSERT(data.getDegradationInfo().degradedServers.empty());
		data.workerHealth.clear();
	}

	// Test that when there is only one reported degraded link, getDegradationInfo can return correct
	// degraded server.
	{
		data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		auto degradationInfo = data.getDegradationInfo();
		ASSERT(degradationInfo.degradedServers.size() == 1);
		ASSERT(degradationInfo.degradedServers.find(badPeer1) != degradationInfo.degradedServers.end());
		ASSERT(degradationInfo.disconnectedServers.empty());
		data.workerHealth.clear();
	}

	// Test that when there is only one reported disconnected link, getDegradationInfo can return correct
	// degraded server.
	{
		data.workerHealth[worker].disconnectedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		auto degradationInfo = data.getDegradationInfo();
		ASSERT(degradationInfo.disconnectedServers.size() == 1);
		ASSERT(degradationInfo.disconnectedServers.find(badPeer1) != degradationInfo.disconnectedServers.end());
		ASSERT(degradationInfo.degradedServers.empty());
		data.workerHealth.clear();
	}

	// Test that if both A complains B and B compalins A, only one of the server will be chosen as degraded
	// server.
	{
		data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[worker].disconnectedPeers[badPeer2] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[badPeer2].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		auto degradationInfo = data.getDegradationInfo();
		ASSERT(degradationInfo.degradedServers.size() == 1);
		ASSERT(degradationInfo.degradedServers.find(worker) != degradationInfo.degradedServers.end() ||
		       degradationInfo.degradedServers.find(badPeer1) != degradationInfo.degradedServers.end());
		ASSERT(degradationInfo.disconnectedServers.size() == 1);
		ASSERT(degradationInfo.disconnectedServers.find(worker) != degradationInfo.disconnectedServers.end() ||
		       degradationInfo.disconnectedServers.find(badPeer2) != degradationInfo.disconnectedServers.end());
		data.workerHealth.clear();
	}

	// Test that if B complains A and C complains A, A is selected as degraded server instead of B or C.
	{
		ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);

		// test for both degraded peers and disconnected peers.
		data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[worker].degradedPeers[badPeer2] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer2].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[worker].disconnectedPeers[badPeer3] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[badPeer3].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[worker].disconnectedPeers[badPeer4] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[badPeer4].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		auto degradationInfo = data.getDegradationInfo();
		ASSERT(degradationInfo.degradedServers.size() == 1);
		ASSERT(degradationInfo.degradedServers.find(worker) != degradationInfo.degradedServers.end());
		ASSERT(degradationInfo.disconnectedServers.size() == 1);
		ASSERT(degradationInfo.disconnectedServers.find(worker) != degradationInfo.disconnectedServers.end());
		data.workerHealth.clear();
	}

	// Test that if the number of complainers exceeds the threshold, no degraded server is returned.
	{
		ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);
		data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer2].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer3].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer4].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		ASSERT(data.getDegradationInfo().degradedServers.empty());
		data.workerHealth.clear();
	}

	// Test that CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE doesn't affect disconnectedServers calculation.
	{
		ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);
		data.workerHealth[badPeer1].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[badPeer2].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[badPeer3].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		data.workerHealth[badPeer4].disconnectedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                      now() };
		ASSERT(data.getDegradationInfo().disconnectedServers.size() == 1);
		ASSERT(data.getDegradationInfo().disconnectedServers.find(worker) !=
		       data.getDegradationInfo().disconnectedServers.end());
		data.workerHealth.clear();
	}

	// Test that if the degradation is reported both ways between A and other 4 servers, no degraded server
	// is returned.
	{
		ASSERT(SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE < 4);
		data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[worker].degradedPeers[badPeer2] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer2].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[worker].degradedPeers[badPeer3] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer3].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[worker].degradedPeers[badPeer4] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer4].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		ASSERT(data.getDegradationInfo().degradedServers.empty());
		data.workerHealth.clear();
	}

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/recentRecoveryCountDueToHealth") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<IClusterConnectionRecord>(
	                               new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                           makeReference<AsyncVar<Optional<UID>>>());

	ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 0);

	data.recentHealthTriggeredRecoveryTime.push(now() - SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL - 1);
	ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 0);

	data.recentHealthTriggeredRecoveryTime.push(now() - SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL + 1);
	ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 1);

	data.recentHealthTriggeredRecoveryTime.push(now());
	ASSERT_EQ(data.recentRecoveryCountDueToHealth(), 2);

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/shouldTriggerRecoveryDueToDegradedServers") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<IClusterConnectionRecord>(
	                               new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                           makeReference<AsyncVar<Optional<UID>>>());
	NetworkAddress master(IPAddress(0x01010101), 1);
	NetworkAddress tlog(IPAddress(0x02020202), 1);
	NetworkAddress satelliteTlog(IPAddress(0x03030303), 1);
	NetworkAddress remoteTlog(IPAddress(0x04040404), 1);
	NetworkAddress logRouter(IPAddress(0x05050505), 1);
	NetworkAddress backup(IPAddress(0x06060606), 1);
	NetworkAddress proxy(IPAddress(0x07070707), 1);
	NetworkAddress resolver(IPAddress(0x08080808), 1);
	NetworkAddress clusterController(IPAddress(0x09090909), 1);
	UID testUID(1, 2);

	// Create a ServerDBInfo using above addresses.
	ServerDBInfo testDbInfo;
	testDbInfo.clusterInterface.changeCoordinators =
	    RequestStream<struct ChangeCoordinatorsRequest>(Endpoint({ clusterController }, UID(1, 2)));

	MasterInterface mInterface;
	mInterface.getCommitVersion = RequestStream<struct GetCommitVersionRequest>(Endpoint({ master }, UID(1, 2)));
	testDbInfo.master = mInterface;

	TLogInterface localTLogInterf;
	localTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ tlog }, testUID));
	BackupInterface backupInterf;
	backupInterf.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ backup }, testUID));
	TLogSet localTLogSet;
	localTLogSet.isLocal = true;
	localTLogSet.tLogs.push_back(OptionalInterface(localTLogInterf));
	localTLogSet.backupWorkers.push_back(OptionalInterface(backupInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(localTLogSet);

	TLogInterface sateTLogInterf;
	sateTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ satelliteTlog }, testUID));
	TLogSet sateTLogSet;
	sateTLogSet.isLocal = true;
	sateTLogSet.locality = tagLocalitySatellite;
	sateTLogSet.tLogs.push_back(OptionalInterface(sateTLogInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(sateTLogSet);

	TLogInterface remoteTLogInterf;
	remoteTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ remoteTlog }, testUID));
	TLogInterface remoteLogRouterInterf;
	remoteLogRouterInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ logRouter }, testUID));
	TLogSet remoteTLogSet;
	remoteTLogSet.isLocal = false;
	remoteTLogSet.tLogs.push_back(OptionalInterface(remoteTLogInterf));
	remoteTLogSet.logRouters.push_back(OptionalInterface(remoteLogRouterInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(remoteTLogSet);

	GrvProxyInterface proxyInterf;
	proxyInterf.getConsistentReadVersion =
	    PublicRequestStream<struct GetReadVersionRequest>(Endpoint({ proxy }, testUID));
	testDbInfo.client.grvProxies.push_back(proxyInterf);

	ResolverInterface resolverInterf;
	resolverInterf.resolve = RequestStream<struct ResolveTransactionBatchRequest>(Endpoint({ resolver }, testUID));
	testDbInfo.resolvers.push_back(resolverInterf);

	testDbInfo.recoveryState = RecoveryState::ACCEPTING_COMMITS;

	// No recovery when no degraded servers.
	data.db.serverInfo->set(testDbInfo);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());

	// Trigger recovery when master is degraded.
	data.degradationInfo.degradedServers.insert(master);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(master);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// Trigger recovery when primary TLog is degraded.
	data.degradationInfo.degradedServers.insert(tlog);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(tlog);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// No recovery when satellite Tlog is degraded.
	data.degradationInfo.degradedServers.insert(satelliteTlog);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();

	// Trigger recovery when satellite Tlog is disconnected.
	data.degradationInfo.disconnectedServers.insert(satelliteTlog);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// No recovery when remote tlog is degraded.
	if (!(SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY &&
	      SERVER_KNOBS->CC_ENABLE_REMOTE_TLOG_DEGRADATION_MONITORING)) {
		data.degradationInfo.degradedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
	}
	if (!SERVER_KNOBS->CC_ENABLE_REMOTE_TLOG_DISCONNECT_MONITORING) {
		data.degradationInfo.disconnectedServers.insert(remoteTlog);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();
	}

	// No recovery when remote log router is degraded.
	if (!(SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY &&
	      SERVER_KNOBS->CC_ENABLE_REMOTE_LOG_ROUTER_DEGRADATION_MONITORING)) {
		data.degradationInfo.degradedServers.insert(logRouter);
		ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.degradedServers.clear();
	}

	// Trigger recovery when remote log router is disconnected.
	if (SERVER_KNOBS->CC_ENABLE_REMOTE_LOG_ROUTER_MONITORING) {
		data.degradationInfo.disconnectedServers.insert(logRouter);
		ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
		data.degradationInfo.disconnectedServers.clear();
	}

	// No recovery when backup worker is degraded.
	data.degradationInfo.degradedServers.insert(backup);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(backup);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// Trigger recovery when proxy is degraded.
	data.degradationInfo.degradedServers.insert(proxy);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(proxy);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// Trigger recovery when resolver is degraded.
	data.degradationInfo.degradedServers.insert(resolver);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(resolver);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/shouldTriggerFailoverDueToDegradedServers") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<IClusterConnectionRecord>(
	                               new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                           makeReference<AsyncVar<Optional<UID>>>());
	NetworkAddress master(IPAddress(0x01010101), 1);
	NetworkAddress tlog(IPAddress(0x02020202), 1);
	NetworkAddress satelliteTlog(IPAddress(0x03030303), 1);
	NetworkAddress remoteTlog(IPAddress(0x04040404), 1);
	NetworkAddress logRouter(IPAddress(0x05050505), 1);
	NetworkAddress backup(IPAddress(0x06060606), 1);
	NetworkAddress proxy(IPAddress(0x07070707), 1);
	NetworkAddress proxy2(IPAddress(0x08080808), 1);
	NetworkAddress resolver(IPAddress(0x09090909), 1);
	NetworkAddress clusterController(IPAddress(0x10101010), 1);
	UID testUID(1, 2);

	data.db.config.usableRegions = 2;

	// Create a ServerDBInfo using above addresses.
	ServerDBInfo testDbInfo;
	testDbInfo.clusterInterface.changeCoordinators =
	    RequestStream<struct ChangeCoordinatorsRequest>(Endpoint({ clusterController }, UID(1, 2)));

	TLogInterface localTLogInterf;
	localTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ tlog }, testUID));
	TLogInterface localLogRouterInterf;
	localLogRouterInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ logRouter }, testUID));
	BackupInterface backupInterf;
	backupInterf.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ backup }, testUID));
	TLogSet localTLogSet;
	localTLogSet.isLocal = true;
	localTLogSet.tLogs.push_back(OptionalInterface(localTLogInterf));
	localTLogSet.logRouters.push_back(OptionalInterface(localLogRouterInterf));
	localTLogSet.backupWorkers.push_back(OptionalInterface(backupInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(localTLogSet);

	TLogInterface sateTLogInterf;
	sateTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ satelliteTlog }, testUID));
	TLogSet sateTLogSet;
	sateTLogSet.isLocal = true;
	sateTLogSet.locality = tagLocalitySatellite;
	sateTLogSet.tLogs.push_back(OptionalInterface(sateTLogInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(sateTLogSet);

	TLogInterface remoteTLogInterf;
	remoteTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ remoteTlog }, testUID));
	TLogSet remoteTLogSet;
	remoteTLogSet.isLocal = false;
	remoteTLogSet.tLogs.push_back(OptionalInterface(remoteTLogInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(remoteTLogSet);

	GrvProxyInterface grvProxyInterf;
	grvProxyInterf.getConsistentReadVersion =
	    PublicRequestStream<struct GetReadVersionRequest>(Endpoint({ proxy }, testUID));
	testDbInfo.client.grvProxies.push_back(grvProxyInterf);

	CommitProxyInterface commitProxyInterf;
	commitProxyInterf.commit = PublicRequestStream<struct CommitTransactionRequest>(Endpoint({ proxy2 }, testUID));
	testDbInfo.client.commitProxies.push_back(commitProxyInterf);

	ResolverInterface resolverInterf;
	resolverInterf.resolve = RequestStream<struct ResolveTransactionBatchRequest>(Endpoint({ resolver }, testUID));
	testDbInfo.resolvers.push_back(resolverInterf);

	testDbInfo.recoveryState = RecoveryState::ACCEPTING_COMMITS;

	// No failover when no degraded servers.
	data.db.serverInfo->set(testDbInfo);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());

	// No failover when small number of degraded servers
	data.degradationInfo.degradedServers.insert(master);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(master);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// Trigger failover when enough servers in the txn system are degraded.
	data.degradationInfo.degradedServers.insert(master);
	data.degradationInfo.degradedServers.insert(tlog);
	data.degradationInfo.degradedServers.insert(proxy);
	data.degradationInfo.degradedServers.insert(proxy2);
	data.degradationInfo.degradedServers.insert(resolver);
	ASSERT(data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(master);
	data.degradationInfo.disconnectedServers.insert(tlog);
	data.degradationInfo.disconnectedServers.insert(proxy);
	data.degradationInfo.disconnectedServers.insert(proxy2);
	data.degradationInfo.disconnectedServers.insert(resolver);
	ASSERT(data.shouldTriggerFailoverDueToDegradedServers());

	// No failover when usable region is 1.
	data.db.config.usableRegions = 1;
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.db.config.usableRegions = 2;

	// No failover when remote is also degraded.
	data.degradationInfo.degradedServers.insert(remoteTlog);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(remoteTlog);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	// No failover when some are not from transaction system
	data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 1));
	data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 2));
	data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 3));
	data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 4));
	data.degradationInfo.degradedServers.insert(NetworkAddress(IPAddress(0x13131313), 5));
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();

	// Trigger failover when satellite is degraded.
	data.degradationInfo.degradedSatellite = true;
	ASSERT(data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();

	// No failover when satellite is degraded, but remote is not healthy.
	data.degradationInfo.degradedSatellite = true;
	data.degradationInfo.degradedServers.insert(remoteTlog);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.degradedServers.clear();
	data.degradationInfo.disconnectedServers.insert(remoteTlog);
	ASSERT(!data.shouldTriggerFailoverDueToDegradedServers());
	data.degradationInfo.disconnectedServers.clear();

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/invalidateExcludedProcessComplaints") {
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<IClusterConnectionRecord>(
	                               new ClusterConnectionMemoryRecord(ClusterConnectionString()))),
	                           makeReference<AsyncVar<Optional<UID>>>());
	NetworkAddress worker1(IPAddress::parse("1.1.1.0").get(), 1);
	NetworkAddress worker2(IPAddress::parse("1.1.1.1").get(), 1);
	NetworkAddress worker3(IPAddress::parse("1.1.1.2").get(), 1);
	NetworkAddress badPeer(IPAddress::parse("1.1.1.3").get(), 1);

	if (SERVER_KNOBS->CC_ONLY_CONSIDER_INTRA_DC_LATENCY) {
		addProcessesToSameDC(data, { worker1, worker2, worker3, badPeer });
	}

	ASSERT(data.workerHealth.empty());

	// {worker1, worker2, worker3} complain about badPeer
	data.workerHealth[worker1].degradedPeers[badPeer] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
		                                                  now() };
	data.workerHealth[worker2].degradedPeers[badPeer] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
		                                                  now() };
	data.workerHealth[worker3].degradedPeers[badPeer] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
		                                                  now() };

	// badPeer complains about {worker1, worker2, worker3}
	data.workerHealth[badPeer].degradedPeers[worker1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
		                                                  now() };
	data.workerHealth[badPeer].degradedPeers[worker2] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
		                                                  now() };
	data.workerHealth[badPeer].degradedPeers[worker3] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
		                                                  now() };

	// At this point, we should have 4 complaints total
	ASSERT(data.workerHealth.contains(worker1));
	ASSERT(data.workerHealth.contains(worker2));
	ASSERT(data.workerHealth.contains(worker3));
	ASSERT(data.workerHealth.contains(badPeer));
	ASSERT(data.workerHealth.size() == 4);

	// Compute degraded processes
	data.degradationInfo = data.getDegradationInfo();
	for (const auto& addr : data.degradationInfo.degradedServers) {
		data.excludedDegradedServers[addr] = now();
	}

	// Ensure badPeer is successfully added to excluded list
	// At this point, recovery would also be triggered in a production setting
	// We would also invalidate complaints by the excluded process at this point
	ASSERT(data.degradationInfo.degradedServers.size() == 1);
	ASSERT(data.degradationInfo.degradedServers.contains(badPeer));
	invalidateExcludedProcessComplaints(&data);

	// Now it's possible because of various factors (e.g. timing, expiration) that
	// the complaints against badPeer disappear, but the initial complaints that badPeer
	// made against others are still there.
	data.workerHealth[worker1].degradedPeers.erase(badPeer);
	data.workerHealth[worker2].degradedPeers.erase(badPeer);
	data.workerHealth[worker3].degradedPeers.erase(badPeer);

	// Compute degraded processes again
	data.degradationInfo = data.getDegradationInfo();
	for (const auto& addr : data.degradationInfo.degradedServers) {
		data.excludedDegradedServers[addr] = now();
	}

	if (SERVER_KNOBS->CC_INVALIDATE_EXCLUDED_PROCESSES) {
		// With CC_INVALIDATE_EXCLUDE_PROCESSES, we should got 0 degraded processes
		// because the original complaints by the now excluded badPeer were invalidated
		ASSERT(data.degradationInfo.degradedServers.empty());
	} else {
		// However, without CC_INVALIDATE_EXCLUDE_PROCESSES, we would get 3 degraded processes
		// which are: worker1, worker2, worker3. This is because we did not invalidate workerHealth
		// to remove badPeer complaints when badPeer was excluded and recovery was triggered.
		ASSERT(data.degradationInfo.degradedServers.size() == 3);
		ASSERT(data.degradationInfo.degradedServers.contains(worker1));
		ASSERT(data.degradationInfo.degradedServers.contains(worker2));
		ASSERT(data.degradationInfo.degradedServers.contains(worker3));
	}

	return Void();
}

} // namespace
