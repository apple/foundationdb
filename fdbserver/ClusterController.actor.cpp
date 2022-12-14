/*
 * ClusterController.actor.cpp
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

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <tuple>
#include <vector>

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/EncryptKeyProxyInterface.h"
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
#include "fdbserver/ClusterController.h"
#include "fdbserver/ClusterRecovery.actor.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/DBCoreState.h"
#include "fdbclient/Metacluster.h"
#include "fdbclient/MetaclusterManagement.actor.h"
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
#include "fdbclient/KeyBackedTypes.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#define TIME_KEEPER_VERSION "1"_sr

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

class ClusterControllerImpl {
public:
	ACTOR static Future<Optional<Value>> getPreviousCoordinators(ClusterController* self) {
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

	ACTOR static Future<Void> clusterWatchDatabase(ClusterController* self,
	                                               ClusterControllerDBInfo* db,
	                                               ServerCoordinators coordinators,
	                                               Future<Void> recoveredDiskFiles) {
		state MasterInterface iMaster;
		state Reference<ClusterRecoveryData> recoveryData;
		state PromiseStream<Future<Void>> addActor;
		state Future<Void> recoveryCore;

		// SOMEDAY: If there is already a non-failed master referenced by zkMasterInfo, use that one until it fails
		// When this someday is implemented, make sure forced failures still cause the master to be recruited again

		loop {
			TraceEvent("CCWDB", self->id).log();
			try {
				state double recoveryStart = now();
				state MasterInterface newMaster;
				state Future<Void> collection;

				TraceEvent("CCWDB", self->id).detail("Recruiting", "Master");
				wait(self->recruitNewMaster(db, std::addressof(newMaster)));

				iMaster = newMaster;

				db->masterRegistrationCount = 0;
				db->recoveryStalled = false;

				auto dbInfo = ServerDBInfo();
				dbInfo.master = iMaster;
				dbInfo.id = deterministicRandom()->randomUniqueID();
				dbInfo.infoGeneration = db->incrementAndGetDbInfoCount();
				dbInfo.masterLifetime = db->serverInfo->get().masterLifetime;
				++dbInfo.masterLifetime;
				dbInfo.clusterInterface = db->serverInfo->get().clusterInterface;
				dbInfo.distributor = db->serverInfo->get().distributor;
				dbInfo.ratekeeper = db->serverInfo->get().ratekeeper;
				dbInfo.blobManager = db->serverInfo->get().blobManager;
				dbInfo.blobMigrator = db->serverInfo->get().blobMigrator;
				dbInfo.encryptKeyProxy = db->serverInfo->get().encryptKeyProxy;
				dbInfo.consistencyScan = db->serverInfo->get().consistencyScan;
				dbInfo.latencyBandConfig = db->serverInfo->get().latencyBandConfig;
				dbInfo.myLocality = db->serverInfo->get().myLocality;
				dbInfo.client = ClientDBInfo();
				dbInfo.client.encryptKeyProxy = db->serverInfo->get().encryptKeyProxy;
				dbInfo.client.isEncryptionEnabled = SERVER_KNOBS->ENABLE_ENCRYPTION;
				dbInfo.client.tenantMode = TenantAPI::tenantModeForClusterType(db->clusterType, db->config.tenantMode);
				dbInfo.client.clusterId = db->serverInfo->get().client.clusterId;
				dbInfo.client.clusterType = db->clusterType;
				dbInfo.client.metaclusterName = db->metaclusterName;

				TraceEvent("CCWDB", self->id)
				    .detail("NewMaster", dbInfo.master.id().toString())
				    .detail("Lifetime", dbInfo.masterLifetime.toString())
				    .detail("ChangeID", dbInfo.id);
				db->serverInfo->set(dbInfo);

				state Future<Void> spinDelay = delay(
				    SERVER_KNOBS
				        ->MASTER_SPIN_DELAY); // Don't retry cluster recovery more than once per second, but don't delay
				                              // the "first" recovery after more than a second of normal operation

				TraceEvent("CCWDB", self->id).detail("Watching", iMaster.id());
				recoveryData = makeReference<ClusterRecoveryData>(self,
				                                                  db->serverInfo,
				                                                  db->serverInfo->get().master,
				                                                  db->serverInfo->get().masterLifetime,
				                                                  coordinators,
				                                                  db->serverInfo->get().clusterInterface,
				                                                  ""_sr,
				                                                  addActor,
				                                                  db->forceRecovery);

				collection = actorCollection(recoveryData->addActor.getFuture());
				recoveryCore = clusterRecoveryCore(recoveryData);

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
					          db->onMasterFailureForced())) {
						break;
					}
					when(wait(db->serverInfo->onChange())) {}
					when(BackupWorkerDoneRequest req =
					         waitNext(db->serverInfo->get().clusterInterface.notifyBackupWorkerDone.getFuture())) {
						if (recoveryData->logSystem.isValid() && recoveryData->logSystem->removeBackupWorker(req)) {
							recoveryData->registrationTrigger.trigger();
						}
						++recoveryData->backupWorkerDoneRequests;
						req.reply.send(Void());
						TraceEvent(SevDebug, "BackupWorkerDoneRequest", self->id).log();
					}
					when(wait(collection)) {
						throw internal_error();
					}
				}
				// failed master (better master exists) could happen while change-coordinators request processing is
				// in-progress
				if (self->shouldCommitSuicide) {
					throw restart_cluster_controller();
				}

				recoveryCore.cancel();
				wait(cleanupRecoveryActorCollection(recoveryData, /*exThrown=*/false));
				ASSERT(addActor.isEmpty());

				wait(spinDelay);

				CODE_PROBE(true, "clusterWatchDatabase() master failed");
				TraceEvent(SevWarn, "DetectedFailedRecovery", self->id).detail("OldMaster", iMaster.id());
			} catch (Error& e) {
				state Error err = e;
				TraceEvent("CCWDB", self->id).errorUnsuppressed(e).detail("Master", iMaster.id());
				if (e.code() != error_code_actor_cancelled)
					wait(delay(0.0));

				recoveryCore.cancel();
				wait(cleanupRecoveryActorCollection(recoveryData, /*exThrown=*/true));
				ASSERT(addActor.isEmpty());

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

				if (self->shouldCommitSuicide || err.code() == error_code_coordinators_changed) {
					TraceEvent("ClusterControllerTerminate", self->id).errorUnsuppressed(err);
					throw restart_cluster_controller();
				}

				if (isNormalClusterRecoveryError(err)) {
					TraceEvent(SevWarn, "ClusterRecoveryRetrying", self->id).error(err);
				} else {
					bool ok = err.code() == error_code_no_more_servers;
					TraceEvent(ok ? SevWarn : SevError, "ClusterWatchDatabaseRetrying", self->id).error(err);
					if (!ok)
						throw err;
				}
				wait(delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
			}
		}
	}

	ACTOR static Future<Void> doCheckOutstandingRequests(ClusterController* self) {
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

			self->checkOutstandingRecruitmentRequests();
			self->checkOutstandingStorageRequests();

			if (self->db.blobGranulesEnabled.get()) {
				self->checkOutstandingBlobWorkerRequests();
			}
			self->checkBetterSingletons();

			self->checkRecoveryStalled();
			if (self->betterMasterExists()) {
				self->db.forceMasterFailure();
				TraceEvent("MasterRegistrationKill", self->id)
				    .detail("MasterId", self->db.serverInfo->get().master.id());
			}
		} catch (Error& e) {
			if (e.code() != error_code_no_more_servers) {
				TraceEvent(SevError, "CheckOutstandingError").error(e);
			}
		}
		return Void();
	}

	ACTOR static Future<Void> doCheckOutstandingRemoteRequests(ClusterController* self) {
		try {
			wait(delay(SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL));
			while (!self->goodRemoteRecruitmentTime.isReady()) {
				wait(self->goodRemoteRecruitmentTime);
			}

			self->checkOutstandingRemoteRecruitmentRequests();
		} catch (Error& e) {
			if (e.code() != error_code_no_more_servers) {
				TraceEvent(SevError, "CheckOutstandingError").error(e);
			}
		}
		return Void();
	}

	ACTOR static Future<Void> rebootAndCheck(ClusterController* self, Optional<Standalone<StringRef>> processID) {
		{
			ASSERT(processID.present());
			auto watcher = self->id_worker.find(processID);
			ASSERT(watcher != self->id_worker.end());

			watcher->second.reboots++;
			wait(delay(g_network->isSimulated() ? SERVER_KNOBS->SIM_SHUTDOWN_TIMEOUT : SERVER_KNOBS->SHUTDOWN_TIMEOUT));
		}

		{
			auto watcher = self->id_worker.find(processID);
			if (watcher != self->id_worker.end()) {
				watcher->second.reboots--;
				if (watcher->second.reboots < 2)
					self->checkOutstandingRequests();
			}
		}

		return Void();
	}

	ACTOR static Future<Void> workerAvailabilityWatch(ClusterController* self,
	                                                  WorkerInterface worker,
	                                                  ProcessClass startingClass) {
		state Future<Void> failed =
		    (worker.address() == g_network->getLocalAddress() || startingClass.classType() == ProcessClass::TesterClass)
		        ? Never()
		        : waitFailureClient(worker.waitFailure, SERVER_KNOBS->WORKER_FAILURE_TIME);
		self->updateWorkerList.set(worker.locality.processId(),
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
						self->ac.add(rebootAndCheck(self, worker.locality.processId()));
						self->checkOutstandingRequests();
					}
				}
				when(wait(failed)) { // remove workers that have failed
					WorkerInfo& failedWorkerInfo = self->id_worker[worker.locality.processId()];

					if (!failedWorkerInfo.reply.isSet()) {
						failedWorkerInfo.reply.send(
						    RegisterWorkerReply(failedWorkerInfo.details.processClass, failedWorkerInfo.priorityInfo));
					}
					if (worker.locality.processId() == self->masterProcessId) {
						self->masterProcessId = Optional<Key>();
					}
					TraceEvent("ClusterControllerWorkerFailed", self->id)
					    .detail("ProcessId", worker.locality.processId())
					    .detail("ProcessClass", failedWorkerInfo.details.processClass.toString())
					    .detail("Address", worker.address());
					self->removedDBInfoEndpoints.insert(worker.updateServerDBInfo.getEndpoint());
					self->id_worker.erase(worker.locality.processId());
					self->updateWorkerList.set(worker.locality.processId(), Optional<ProcessData>());
					return Void();
				}
			}
		}
	}

	ACTOR static Future<Void> registerWorker(ClusterController* self,
	                                         RegisterWorkerRequest req,
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

		for (auto const& address : req.incompatiblePeers) {
			self->db.markConnectionIncompatible(address);
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
				newPriorityInfo.processClassFitness =
				    newProcessClass.machineClassFitness(ProcessClass::ClusterController);
			}

			if (self->gotFullyRecoveredConfig) {
				newPriorityInfo.isExcluded = self->db.fullyRecoveredConfig.isExcludedServer(w.addresses());
			}
		}

		if (info == self->id_worker.end()) {
			self->id_worker[w.locality.processId()] = WorkerInfo(self->workerAvailabilityWatch(w, newProcessClass),
			                                                     req.reply,
			                                                     req.generation,
			                                                     w,
			                                                     req.initialClass,
			                                                     newProcessClass,
			                                                     newPriorityInfo,
			                                                     req.degraded,
			                                                     req.recoveredDiskFiles,
			                                                     req.issues);
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
			self->checkOutstandingRequests();
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
				// detects failures and removes the worker from id_worker even before the new watcher starts monitoring
				// the new interface
				info->second.watcher.cancel();
				info->second.watcher = self->workerAvailabilityWatch(w, newProcessClass);
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
			self->checkOutstandingRequests();
		} else {
			CODE_PROBE(true, "Received an old worker registration request.", probe::decoration::rare);
		}

		// For each singleton
		// - if the registering singleton conflicts with the singleton being recruited, kill the registering one
		// - if the singleton is not being recruited, kill the existing one in favour of the registering one
		if (req.distributorInterf.present()) {
			auto currSingleton = DataDistributorSingleton(self->db.serverInfo->get().distributor);
			auto registeringSingleton = DataDistributorSingleton(req.distributorInterf);
			self->haltRegisteringOrCurrentSingleton<DataDistributorSingleton>(
			    w, currSingleton, registeringSingleton, self->recruitingDistributorID);
		}

		if (req.ratekeeperInterf.present()) {
			auto currSingleton = RatekeeperSingleton(self->db.serverInfo->get().ratekeeper);
			auto registeringSingleton = RatekeeperSingleton(req.ratekeeperInterf);
			self->haltRegisteringOrCurrentSingleton<RatekeeperSingleton>(
			    w, currSingleton, registeringSingleton, self->recruitingRatekeeperID);
		}

		if (self->db.blobGranulesEnabled.get() && req.blobManagerInterf.present()) {
			auto currSingleton = BlobManagerSingleton(self->db.serverInfo->get().blobManager);
			auto registeringSingleton = BlobManagerSingleton(req.blobManagerInterf);
			self->haltRegisteringOrCurrentSingleton<BlobManagerSingleton>(
			    w, currSingleton, registeringSingleton, self->recruitingBlobManagerID);
		}
		if (req.blobMigratorInterf.present() && self->db.blobRestoreEnabled.get()) {
			auto currSingleton = BlobMigratorSingleton(self->db.serverInfo->get().blobMigrator);
			auto registeringSingleton = BlobMigratorSingleton(req.blobMigratorInterf);
			self->haltRegisteringOrCurrentSingleton<BlobMigratorSingleton>(
			    w, currSingleton, registeringSingleton, self->recruitingBlobMigratorID);
		}

		if (SERVER_KNOBS->ENABLE_ENCRYPTION && req.encryptKeyProxyInterf.present()) {
			auto currSingleton = EncryptKeyProxySingleton(self->db.serverInfo->get().encryptKeyProxy);
			auto registeringSingleton = EncryptKeyProxySingleton(req.encryptKeyProxyInterf);
			self->haltRegisteringOrCurrentSingleton<EncryptKeyProxySingleton>(
			    w, currSingleton, registeringSingleton, self->recruitingEncryptKeyProxyID);
		}

		if (req.consistencyScanInterf.present()) {
			auto currSingleton = ConsistencyScanSingleton(self->db.serverInfo->get().consistencyScan);
			auto registeringSingleton = ConsistencyScanSingleton(req.consistencyScanInterf);
			self->haltRegisteringOrCurrentSingleton<ConsistencyScanSingleton>(
			    w, currSingleton, registeringSingleton, self->recruitingConsistencyScanID);
		}

		// Notify the worker to register again with new process class/exclusive property
		if (!req.reply.isSet() && newPriorityInfo != req.priorityInfo) {
			req.reply.send(RegisterWorkerReply(newProcessClass, newPriorityInfo));
		}

		return Void();
	}

	ACTOR static Future<Void> timeKeeperSetVersion(ClusterController* self) {
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
	ACTOR static Future<Void> timeKeeper(ClusterController* self) {
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

	ACTOR static Future<Void> statusServer(ClusterController* self,
	                                       FutureStream<StatusRequest> requests,
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

				// Wait if needed to satisfy min_time knob, also allows more requets to queue up.
				double minwait = std::max(next_allowed_request_time - now(), 0.0);
				wait(delay(minwait));

				// Get all requests that are ready right *now*, before GetStatus() begins.
				// All of these requests will be responded to with the next GetStatus() result.
				// If requests are batched, do not respond to more than MAX_STATUS_REQUESTS_PER_SECOND
				// requests per second
				while (requests.isReady()) {
					auto req = requests.pop();
					if (SERVER_KNOBS->STATUS_MIN_TIME_BETWEEN_REQUESTS > 0.0 &&
					    requests_batch.size() + 1 > SERVER_KNOBS->STATUS_MIN_TIME_BETWEEN_REQUESTS *
					                                    SERVER_KNOBS->MAX_STATUS_REQUESTS_PER_SECOND) {
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

				std::vector<NetworkAddress> incompatibleConnections = self->db.getIncompatibleConnections();

				state ErrorOr<StatusReply> result = wait(errorOr(clusterGetStatus(self->db.serverInfo,
				                                                                  self->cx,
				                                                                  workers,
				                                                                  workerIssues,
				                                                                  &self->db.clientStatus,
				                                                                  coordinators,
				                                                                  incompatibleConnections,
				                                                                  self->datacenterVersionDifference,
				                                                                  configBroadcaster,
				                                                                  self->db.metaclusterRegistration,
				                                                                  self->db.metaclusterMetrics)));

				if (result.isError() && result.getError().code() == error_code_actor_cancelled)
					throw result.getError();

				// Update last_request_time now because GetStatus is finished and the delay is to be measured between
				// requests
				last_request_time = now();

				while (!requests_batch.empty()) {
					if (result.isError())
						requests_batch.back().reply.sendError(result.getError());
					else
						requests_batch.back().reply.send(result.get());
					requests_batch.pop_back();
					wait(yield());
				}
			} catch (Error& e) {
				TraceEvent(SevError, "StatusServerError").error(e);
				throw e;
			}
		}
	}

	ACTOR static Future<Void> monitorProcessClasses(ClusterController* self) {

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
						self->checkOutstandingRequests();
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

	ACTOR static Future<Void> updatedChangingDatacenters(ClusterController* self) {
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

			wait(onChange);
		}
	}

	ACTOR static Future<Void> updatedChangedDatacenters(ClusterController* self) {
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
						TraceEvent("UpdateChangedDatacenter", self->id)
						    .detail("CCFirst", self->changedDcIds.get().first);
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

	ACTOR static Future<Void> updateDatacenterVersionDifference(ClusterController* self) {
		state double lastLogTime = 0;
		loop {
			self->versionDifferenceUpdated = false;
			if (self->db.serverInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS &&
			    self->db.config.usableRegions == 1) {
				bool oldDifferenceTooLarge = !self->versionDifferenceUpdated ||
				                             self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
				self->versionDifferenceUpdated = true;
				self->datacenterVersionDifference = 0;

				if (oldDifferenceTooLarge) {
					self->checkOutstandingRequests();
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

			if (!primaryLog.present() || !remoteLog.present()) {
				wait(self->db.serverInfo->onChange());
				continue;
			}

			state Future<Void> onChange = self->db.serverInfo->onChange();
			loop {
				state Future<TLogQueuingMetricsReply> primaryMetrics =
				    brokenPromiseToNever(primaryLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));
				state Future<TLogQueuingMetricsReply> remoteMetrics =
				    brokenPromiseToNever(remoteLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));

				wait((success(primaryMetrics) && success(remoteMetrics)) || onChange);
				if (onChange.isReady()) {
					break;
				}

				if (primaryMetrics.get().v > 0 && remoteMetrics.get().v > 0) {
					bool oldDifferenceTooLarge =
					    !self->versionDifferenceUpdated ||
					    self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
					self->versionDifferenceUpdated = true;
					self->datacenterVersionDifference = primaryMetrics.get().v - remoteMetrics.get().v;

					if (oldDifferenceTooLarge &&
					    self->datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
						self->checkOutstandingRequests();
					}

					if (now() - lastLogTime > SERVER_KNOBS->CLUSTER_CONTROLLER_LOGGING_DELAY) {
						lastLogTime = now();
						TraceEvent("DatacenterVersionDifference", self->id)
						    .detail("Difference", self->datacenterVersionDifference);
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
	ACTOR static Future<Void> updateRemoteDCHealth(ClusterController* self) {
		// The purpose of the initial delay is to wait for the cluster to achieve a steady state before checking remote
		// DC health, since remote DC healthy may trigger a failover, and we don't want that to happen too frequently.
		wait(delay(SERVER_KNOBS->INITIAL_UPDATE_CROSS_DC_INFO_DELAY));

		self->remoteDCMonitorStarted = true;

		// When the remote DC health just start, we may just recover from a health degradation. Check if we can failback
		// if we are currently in the remote DC in the database configuration.
		if (!self->remoteTransactionSystemDegraded) {
			self->checkOutstandingRequests();
		}

		loop {
			bool oldRemoteTransactionSystemDegraded = self->remoteTransactionSystemDegraded;
			self->remoteTransactionSystemDegraded = self->remoteTransactionSystemContainsDegradedServers();

			if (oldRemoteTransactionSystemDegraded && !self->remoteTransactionSystemDegraded) {
				self->checkOutstandingRequests();
			}
			wait(delay(SERVER_KNOBS->CHECK_REMOTE_HEALTH_INTERVAL));
		}
	}

	ACTOR static Future<Void> handleForcedRecoveries(ClusterController* self, ClusterControllerFullInterface interf) {
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
					self->db.forceMasterFailure();
				}
				wait(fCommit);
			}
			TraceEvent("ForcedRecoveryFinish", self->id).log();
			self->db.forceRecovery = false;
			req.reply.send(Void());
		}
	}

	ACTOR static Future<Void> triggerAuditStorage(ClusterController* self, TriggerAuditRequest req) {
		TraceEvent(SevInfo, "CCTriggerAuditStorageBegin", self->id)
		    .detail("Range", req.range)
		    .detail("AuditType", req.type);
		state UID auditId;

		try {
			while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
			       !self->db.serverInfo->get().distributor.present()) {
				wait(self->db.serverInfo->onChange());
			}

			TriggerAuditRequest fReq(req.getType(), req.range);
			UID auditId_ = wait(self->db.serverInfo->get().distributor.get().triggerAudit.getReply(fReq));
			auditId = auditId_;
			TraceEvent(SevDebug, "CCTriggerAuditStorageEnd", self->id)
			    .detail("AuditID", auditId)
			    .detail("Range", req.range)
			    .detail("AuditType", req.type);
			if (!req.reply.isSet()) {
				req.reply.send(auditId);
			}
		} catch (Error& e) {
			TraceEvent(SevDebug, "CCTriggerAuditStorageError", self->id)
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId)
			    .detail("Range", req.range)
			    .detail("AuditType", req.type);
			if (!req.reply.isSet()) {
				req.reply.sendError(audit_storage_failed());
			}
		}

		return Void();
	}

	ACTOR static Future<Void> handleTriggerAuditStorage(ClusterController* self,
	                                                    ClusterControllerFullInterface interf) {
		loop {
			TriggerAuditRequest req = waitNext(interf.clientInterface.triggerAudit.getFuture());
			TraceEvent(SevDebug, "TriggerAuditStorageReceived", self->id)
			    .detail("ClusterControllerDcId", self->clusterControllerDcId)
			    .detail("Range", req.range)
			    .detail("AuditType", req.type);
			self->addActor.send(triggerAuditStorage(self, req));
		}
	}

	ACTOR static Future<Void> startDataDistributor(ClusterController* self, double waitTime) {
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
					    self->id_worker.count(distributor.get().locality.processId())) {

						TraceEvent("CCHaltDataDistributorAfterRecruit", self->id)
						    .detail("DDID", distributor.get().id())
						    .detail("DcID", printable(self->clusterControllerDcId));

						DataDistributorSingleton(distributor).halt(*self, distributor.get().locality.processId());
					}
					if (!distributor.present() || distributor.get().id() != ddInterf.get().id()) {
						self->db.setDistributor(ddInterf.get());
					}
					self->checkOutstandingRequests();
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

	ACTOR static Future<Void> monitorDataDistributor(ClusterController* self) {
		state SingletonRecruitThrottler recruitThrottler;
		while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			wait(self->db.serverInfo->onChange());
		}

		loop {
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

	ACTOR static Future<Void> startRatekeeper(ClusterController* self, double waitTime) {
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

				ErrorOr<RatekeeperInterface> interf = wait(worker.interf.ratekeeper.getReplyUnlessFailedFor(
				    req, SERVER_KNOBS->WAIT_FOR_RATEKEEPER_JOIN_DELAY, 0));
				if (interf.present()) {
					self->recruitRatekeeper.set(false);
					self->recruitingRatekeeperID = interf.get().id();
					const auto& ratekeeper = self->db.serverInfo->get().ratekeeper;
					TraceEvent("CCRatekeeperRecruited", self->id)
					    .detail("Addr", worker.interf.address())
					    .detail("RKID", interf.get().id());
					if (ratekeeper.present() && ratekeeper.get().id() != interf.get().id() &&
					    self->id_worker.count(ratekeeper.get().locality.processId())) {
						TraceEvent("CCHaltRatekeeperAfterRecruit", self->id)
						    .detail("RKID", ratekeeper.get().id())
						    .detail("DcID", printable(self->clusterControllerDcId));
						RatekeeperSingleton(ratekeeper).halt(*self, ratekeeper.get().locality.processId());
					}
					if (!ratekeeper.present() || ratekeeper.get().id() != interf.get().id()) {
						self->db.setRatekeeper(interf.get());
					}
					self->checkOutstandingRequests();
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

	ACTOR static Future<Void> monitorRatekeeper(ClusterController* self) {
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

	ACTOR static Future<Void> startConsistencyScan(ClusterController* self) {
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
					    self->id_worker.count(consistencyScan.get().locality.processId())) {
						TraceEvent("CCHaltConsistencyScanAfterRecruit", self->id)
						    .detail("CKID", consistencyScan.get().id())
						    .detail("DcID", printable(self->clusterControllerDcId));
						ConsistencyScanSingleton(consistencyScan)
						    .halt(*self, consistencyScan.get().locality.processId());
					}
					if (!consistencyScan.present() || consistencyScan.get().id() != interf.get().id()) {
						self->db.setConsistencyScan(interf.get());
					}
					self->checkOutstandingRequests();
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

	ACTOR static Future<Void> monitorConsistencyScan(ClusterController* self) {
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

	ACTOR static Future<Void> startEncryptKeyProxy(ClusterController* self, double waitTime) {
		// If master fails at the same time, give it a chance to clear master PID.
		// Also wait to avoid too many consecutive recruits in a small time window.
		wait(delay(waitTime));

		TraceEvent("CCEKP_Start", self->id).log();
		loop {
			try {
				// EncryptKeyServer interface is critical in recovering tlog encrypted transactions,
				// hence, the process only waits for the master recruitment and not the full cluster recovery.
				state bool noEncryptKeyServer = !self->db.serverInfo->get().encryptKeyProxy.present();
				while (!self->masterProcessId.present() ||
				       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
				       self->db.serverInfo->get().recoveryState < RecoveryState::LOCKING_CSTATE) {
					wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
				}
				if (noEncryptKeyServer && self->db.serverInfo->get().encryptKeyProxy.present()) {
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
					const auto& encryptKeyProxy = self->db.serverInfo->get().encryptKeyProxy;
					TraceEvent("CCEKP_Recruited", self->id)
					    .detail("Addr", worker.interf.address())
					    .detail("Id", interf.get().id())
					    .detail("ProcessId", interf.get().locality.processId());
					if (encryptKeyProxy.present() && encryptKeyProxy.get().id() != interf.get().id() &&
					    self->id_worker.count(encryptKeyProxy.get().locality.processId())) {
						TraceEvent("CCEKP_HaltAfterRecruit", self->id)
						    .detail("Id", encryptKeyProxy.get().id())
						    .detail("DcId", printable(self->clusterControllerDcId));
						EncryptKeyProxySingleton(encryptKeyProxy)
						    .halt(*self, encryptKeyProxy.get().locality.processId());
					}
					if (!encryptKeyProxy.present() || encryptKeyProxy.get().id() != interf.get().id()) {
						self->db.setEncryptKeyProxy(interf.get());
						TraceEvent("CCEKP_UpdateInf", self->id)
						    .detail("Id", self->db.serverInfo->get().encryptKeyProxy.get().id());
					}
					self->checkOutstandingRequests();
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

	ACTOR static Future<Void> monitorEncryptKeyProxy(ClusterController* self) {
		state SingletonRecruitThrottler recruitThrottler;
		loop {
			if (self->db.serverInfo->get().encryptKeyProxy.present() && !self->recruitEncryptKeyProxy.get()) {
				choose {
					when(wait(waitFailureClient(self->db.serverInfo->get().encryptKeyProxy.get().waitFailure,
					                            SERVER_KNOBS->ENCRYPT_KEY_PROXY_FAILURE_TIME))) {
						TraceEvent("CCEKP_Died", self->id);
						const auto& encryptKeyProxy = self->db.serverInfo->get().encryptKeyProxy;
						EncryptKeyProxySingleton(encryptKeyProxy)
						    .halt(*self, encryptKeyProxy.get().locality.processId());
						self->db.clearInterf(ProcessClass::EncryptKeyProxyClass);
					}
					when(wait(self->recruitEncryptKeyProxy.onChange())) {}
				}
			} else {
				wait(startEncryptKeyProxy(self, recruitThrottler.newRecruitment()));
			}
		}
	}

	// Acquires the BM lock by getting the next epoch no.
	ACTOR static Future<int64_t> getNextBMEpoch(ClusterController* self) {
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

	ACTOR static Future<Void> watchBlobRestoreCommand(ClusterController* self) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
		state Key blobRestoreCommandKey = blobRestoreCommandKeyFor(normalKeys);
		loop {
			try {
				tr->reset();
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Optional<Value> blobRestoreCommand = wait(tr->get(blobRestoreCommandKey));
				if (blobRestoreCommand.present()) {
					Standalone<BlobRestoreStatus> status = decodeBlobRestoreStatus(blobRestoreCommand.get());
					TraceEvent("WatchBlobRestoreCommand")
					    .detail("Progress", status.progress)
					    .detail("Phase", status.phase);
					if (status.phase == BlobRestorePhase::INIT) {
						self->db.blobRestoreEnabled.set(true);
						if (self->db.blobGranulesEnabled.get()) {
							const auto& blobManager = self->db.serverInfo->get().blobManager;
							if (blobManager.present()) {
								BlobManagerSingleton(blobManager)
								    .haltBlobGranules(*self, blobManager.get().locality.processId());
							}
							const auto& blobMigrator = self->db.serverInfo->get().blobMigrator;
							if (blobMigrator.present()) {
								BlobMigratorSingleton(blobMigrator)
								    .halt(*self, blobMigrator.get().locality.processId());
							}
						}
					}
				}

				state Future<Void> watch = tr->watch(blobRestoreCommandKey);
				wait(tr->commit());
				wait(watch);
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> startBlobMigrator(ClusterController* self, double waitTime) {
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
				InitializeBlobMigratorRequest req(deterministicRandom()->randomUniqueID());
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
					    self->id_worker.count(blobMigrator.get().locality.processId())) {
						TraceEvent("CCHaltBlobMigratorAfterRecruit", self->id)
						    .detail("MGID", blobMigrator.get().id())
						    .detail("DcID", printable(self->clusterControllerDcId));
						BlobMigratorSingleton(blobMigrator).halt(*self, blobMigrator.get().locality.processId());
					}
					if (!blobMigrator.present() || blobMigrator.get().id() != interf.get().id()) {
						self->db.setBlobMigrator(interf.get());
					}
					self->checkOutstandingRequests();
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

	ACTOR static Future<Void> monitorBlobMigrator(ClusterController* self) {
		state SingletonRecruitThrottler recruitThrottler;
		while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			wait(self->db.serverInfo->onChange());
		}
		loop {
			if (self->db.serverInfo->get().blobMigrator.present() && !self->recruitBlobMigrator.get()) {
				state Future<Void> wfClient =
				    waitFailureClient(self->db.serverInfo->get().blobMigrator.get().waitFailure,
				                      SERVER_KNOBS->BLOB_MIGRATOR_FAILURE_TIME);
				loop {
					choose {
						when(wait(wfClient)) {
							TraceEvent("CCBlobMigratorDied", self->id)
							    .detail("MGID", self->db.serverInfo->get().blobMigrator.get().id());
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

	ACTOR static Future<Void> startBlobManager(ClusterController* self, double waitTime) {
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
					    self->id_worker.count(blobManager.get().locality.processId())) {
						TraceEvent("CCHaltBlobManagerAfterRecruit", self->id)
						    .detail("BMID", blobManager.get().id())
						    .detail("DcID", printable(self->clusterControllerDcId));
						BlobManagerSingleton(blobManager).halt(*self, blobManager.get().locality.processId());
					}
					if (!blobManager.present() || blobManager.get().id() != interf.get().id()) {
						self->db.setBlobManager(interf.get());
					}
					self->checkOutstandingRequests();
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

	ACTOR static Future<Void> watchBlobGranulesConfigKey(ClusterController* self) {
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

	ACTOR static Future<Void> monitorBlobManager(ClusterController* self) {
		state SingletonRecruitThrottler recruitThrottler;
		while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			wait(self->db.serverInfo->onChange());
		}

		loop {
			if (self->db.serverInfo->get().blobManager.present() && !self->recruitBlobManager.get()) {
				state Future<Void> wfClient = waitFailureClient(
				    self->db.serverInfo->get().blobManager.get().waitFailure, SERVER_KNOBS->BLOB_MANAGER_FAILURE_TIME);
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

	ACTOR static Future<Void> dbInfoUpdater(ClusterController* self) {
		state Future<Void> dbInfoChange = self->db.serverInfo->onChange();
		state Future<Void> updateDBInfo = self->updateDBInfo.onTrigger();
		loop {
			choose {
				when(wait(updateDBInfo)) {
					wait(delay(SERVER_KNOBS->DBINFO_BATCH_DELAY) || dbInfoChange);
				}
				when(wait(dbInfoChange)) {}
			}

			UpdateServerDBInfoRequest req;
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

			TraceEvent("DBInfoStartBroadcast", self->id).log();
			choose {
				when(std::vector<Endpoint> notUpdated = wait(
				         broadcastDBInfoRequest(req, SERVER_KNOBS->DBINFO_SEND_AMOUNT, Optional<Endpoint>(), false))) {
					TraceEvent("DBInfoFinishBroadcast", self->id).detail("NotUpdated", notUpdated.size());
					if (notUpdated.size()) {
						self->updateDBInfoEndpoints.insert(notUpdated.begin(), notUpdated.end());
						self->updateDBInfo.trigger();
					}
				}
				when(wait(dbInfoChange)) {}
			}
		}
	}

	// The actor that periodically monitors the health of tracked workers.
	ACTOR static Future<Void> workerHealthMonitor(ClusterController* self) {
		loop {
			try {
				while (!self->goodRecruitmentTime.isReady()) {
					wait(lowPriorityDelay(SERVER_KNOBS->CC_WORKER_HEALTH_CHECKING_INTERVAL));
				}

				self->degradationInfo = self->getDegradationInfo();

				// Compare `self->degradationInfo` with `self->excludedDegradedServers` and remove those that have
				// recovered.
				for (auto it = self->excludedDegradedServers.begin(); it != self->excludedDegradedServers.end();) {
					if (self->degradationInfo.degradedServers.find(*it) ==
					        self->degradationInfo.degradedServers.end() &&
					    self->degradationInfo.disconnectedServers.find(*it) ==
					        self->degradationInfo.disconnectedServers.end()) {
						self->excludedDegradedServers.erase(it++);
					} else {
						++it;
					}
				}

				if (!self->degradationInfo.degradedServers.empty() ||
				    !self->degradationInfo.disconnectedServers.empty() || self->degradationInfo.degradedSatellite) {
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
								self->recentHealthTriggeredRecoveryTime.push(now());
								self->excludedDegradedServers = self->degradationInfo.degradedServers;
								self->excludedDegradedServers.insert(self->degradationInfo.disconnectedServers.begin(),
								                                     self->degradationInfo.disconnectedServers.end());
								TraceEvent("DegradedServerDetectedAndTriggerRecovery")
								    .detail("RecentRecoveryCountDueToHealth", self->recentRecoveryCountDueToHealth());
								self->db.forceMasterFailure();
							}
						} else {
							self->excludedDegradedServers.clear();
							TraceEvent("DegradedServerDetectedAndSuggestRecovery").log();
						}
					} else if (self->shouldTriggerFailoverDueToDegradedServers()) {
						double ccUpTime = now() - machineStartTime();
						if (SERVER_KNOBS->CC_HEALTH_TRIGGER_FAILOVER &&
						    ccUpTime > SERVER_KNOBS->INITIAL_UPDATE_CROSS_DC_INFO_DELAY) {
							TraceEvent("DegradedServerDetectedAndTriggerFailover").log();
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
							TraceEvent("DegradedServerDetectedAndSuggestFailover").detail("CCUpTime", ccUpTime);
						}
					}
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

	ACTOR static Future<Void> metaclusterMetricsUpdater(ClusterController* self) {
		loop {
			state Future<Void> updaterDelay =
			    self->db.clusterType == ClusterType::METACLUSTER_MANAGEMENT ? delay(60.0) : Never();
			choose {
				when(wait(self->db.serverInfo->onChange())) {}
				when(wait(updaterDelay)) {
					state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
					loop {
						try {
							tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
							state std::map<ClusterName, DataClusterMetadata> clusters;
							state int64_t tenantCount;
							wait(store(clusters,
							           MetaclusterAPI::listClustersTransaction(
							               tr, ""_sr, "\xff"_sr, CLIENT_KNOBS->MAX_DATA_CLUSTERS)) &&
							     store(tenantCount,
							           MetaclusterAPI::ManagementClusterMetadata::tenantMetadata().tenantCount.getD(
							               tr, Snapshot::False, 0)));
							state std::pair<ClusterUsage, ClusterUsage> capacityNumbers =
							    MetaclusterAPI::metaclusterCapacity(clusters);

							MetaclusterMetrics metrics;
							metrics.numTenants = tenantCount;
							metrics.numDataClusters = clusters.size();
							metrics.tenantGroupCapacity = capacityNumbers.first.numTenantGroups;
							metrics.tenantGroupsAllocated = capacityNumbers.second.numTenantGroups;
							self->db.metaclusterMetrics = metrics;
							TraceEvent("MetaclusterCapacity")
							    .detail("TotalTenants", self->db.metaclusterMetrics.numTenants)
							    .detail("DataClusters", self->db.metaclusterMetrics.numDataClusters)
							    .detail("TenantGroupCapacity", self->db.metaclusterMetrics.tenantGroupCapacity)
							    .detail("TenantGroupsAllocated", self->db.metaclusterMetrics.tenantGroupsAllocated);
							break;
						} catch (Error& e) {
							TraceEvent("MetaclusterUpdaterError").error(e);
							// Cluster can change types during/before a metacluster transaction
							// and throw an error due to timing issues.
							// In such cases, go back to choose loop instead of retrying
							if (e.code() == error_code_invalid_metacluster_operation) {
								break;
							}
							wait(tr->onError(e));
						}
					}
				}
			}
		}
	}

	// Update the DBInfo state with this processes cluster ID. If this process does
	// not have a cluster ID and one does not exist in the database, generate one.
	ACTOR static Future<Void> updateClusterId(ClusterController* self) {
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

	ACTOR static Future<Void> handleGetEncryptionAtRestMode(ClusterController* self,
	                                                        ClusterControllerFullInterface ccInterf) {
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

	ACTOR static Future<Void> run(ClusterControllerFullInterface interf,
	                              Future<Void> leaderFail,
	                              ServerCoordinators coordinators,
	                              LocalityData locality,
	                              ConfigDBType configDBType,
	                              Future<Void> recoveredDiskFiles,
	                              Reference<AsyncVar<Optional<UID>>> clusterId) {
		state ClusterController self(interf, locality, coordinators, clusterId);
		state Future<Void> coordinationPingDelay = delay(SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY);
		state uint64_t step = 0;
		state Future<ErrorOr<Void>> error = errorOr(actorCollection(self.addActor.getFuture()));
		state ConfigBroadcaster configBroadcaster;
		if (configDBType != ConfigDBType::DISABLED) {
			configBroadcaster = ConfigBroadcaster(coordinators, configDBType, self.getPreviousCoordinators());
		}

		// EncryptKeyProxy is necessary for TLog recovery, recruit it as the first process
		if (SERVER_KNOBS->ENABLE_ENCRYPTION) {
			self.addActor.send(self.monitorEncryptKeyProxy());
		}
		// TODO: Extract db within function?
		self.addActor.send(
		    self.clusterWatchDatabase(&self.db, coordinators, recoveredDiskFiles)); // Start the master database
		self.addActor.send(self.updateWorkerList.init(self.db.db));
		self.addActor.send(self.statusServer(interf.clientInterface.databaseStatus.getFuture(),
		                                     coordinators,
		                                     (configDBType == ConfigDBType::DISABLED) ? nullptr : &configBroadcaster));
		self.addActor.send(self.timeKeeper());
		self.addActor.send(self.monitorProcessClasses());
		self.addActor.send(self.db.monitorServerInfoConfig());
		self.addActor.send(self.db.monitorGlobalConfig());
		self.addActor.send(self.updatedChangingDatacenters());
		self.addActor.send(self.updatedChangedDatacenters());
		self.addActor.send(self.updateDatacenterVersionDifference());
		self.addActor.send(self.handleForcedRecoveries(interf));
		self.addActor.send(self.handleTriggerAuditStorage(interf));
		self.addActor.send(self.monitorDataDistributor());
		self.addActor.send(self.monitorRatekeeper());
		self.addActor.send(self.monitorBlobManager());
		self.addActor.send(self.watchBlobGranulesConfigKey());
		self.addActor.send(self.monitorBlobMigrator());
		self.addActor.send(self.watchBlobRestoreCommand());
		self.addActor.send(self.monitorConsistencyScan());
		self.addActor.send(self.metaclusterMetricsUpdater());
		self.addActor.send(self.dbInfoUpdater());
		self.addActor.send(self.updateClusterId());
		self.addActor.send(self.handleGetEncryptionAtRestMode(interf));
		self.addActor.send(
		    self.clusterControllerMetrics.traceCounters("ClusterControllerMetrics",
		                                                self.id,
		                                                SERVER_KNOBS->STORAGE_LOGGING_DELAY,
		                                                self.id.toString() + "/ClusterControllerMetrics"));
		self.addActor.send(traceRole(Role::CLUSTER_CONTROLLER, interf.id()));
		// printf("%s: I am the cluster controller\n", g_network->getLocalAddress().toString().c_str());

		if (SERVER_KNOBS->CC_ENABLE_WORKER_HEALTH_MONITOR) {
			self.addActor.send(self.workerHealthMonitor());
			self.addActor.send(self.updateRemoteDCHealth());
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
				self.addActor.send(self.db.clusterOpenDatabase(req));
			}
			when(RecruitStorageRequest req = waitNext(interf.recruitStorage.getFuture())) {
				self.clusterRecruitStorage(req);
			}
			when(RecruitBlobWorkerRequest req = waitNext(interf.recruitBlobWorker.getFuture())) {
				self.clusterRecruitBlobWorker(req);
			}
			when(RegisterWorkerRequest req = waitNext(interf.registerWorker.getFuture())) {
				++self.registerWorkerRequests;
				self.addActor.send(
				    self.registerWorker(req,
				                        coordinators.ccr->getConnectionString(),
				                        (configDBType == ConfigDBType::DISABLED) ? nullptr : &configBroadcaster));
			}
			when(GetWorkersRequest req = waitNext(interf.getWorkers.getFuture())) {
				++self.getWorkersRequests;
				std::vector<WorkerDetails> workers;

				for (auto const& [id, worker] : self.id_worker) {
					if ((req.flags & GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY) &&
					    self.db.config.isExcludedServer(worker.details.interf.addresses())) {
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
				self.clusterRegisterMaster(req);
			}
			when(UpdateWorkerHealthRequest req = waitNext(interf.updateWorkerHealth.getFuture())) {
				if (SERVER_KNOBS->CC_ENABLE_WORKER_HEALTH_MONITOR) {
					self.updateWorkerHealth(req);
				}
			}
			when(GetServerDBInfoRequest req = waitNext(interf.getServerDBInfo.getFuture())) {
				self.addActor.send(self.db.clusterGetServerInfo(req.knownServerInfoID, req.reply));
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

	ACTOR static Future<Void> recruitNewMaster(ClusterController* cluster,
	                                           ClusterControllerDBInfo* db,
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
			wait(ready(fNewMaster) || db->onMasterFailureForced());
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

	ACTOR static Future<Void> clusterRecruitFromConfiguration(ClusterController* self,
	                                                          Reference<RecruitWorkersInfo> req) {
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

	ACTOR static Future<RecruitRemoteFromConfigurationReply> clusterRecruitRemoteFromConfiguration(
	    ClusterController* self,
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

}; // class ClusterControllerImpl

Future<Optional<Value>> ClusterController::getPreviousCoordinators() {
	return ClusterControllerImpl::getPreviousCoordinators(this);
}

Future<Void> ClusterController::clusterWatchDatabase(ClusterControllerDBInfo* db,
                                                     ServerCoordinators coordinators,
                                                     Future<Void> recoveredDiskFiles) {
	return ClusterControllerImpl::clusterWatchDatabase(this, db, coordinators, recoveredDiskFiles);
}

void ClusterController::checkOutstandingRecruitmentRequests() {
	for (int i = 0; i < outstandingRecruitmentRequests.size(); i++) {
		Reference<RecruitWorkersInfo> info = outstandingRecruitmentRequests[i];
		try {
			info->rep = findWorkersForConfiguration(info->req);
			if (info->dbgId.present()) {
				TraceEvent("CheckOutstandingRecruitment", info->dbgId.get())
				    .detail("Request", info->req.configuration.toString());
			}
			info->waitForCompletion.trigger();
			swapAndPop(&outstandingRecruitmentRequests, i--);
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers || e.code() == error_code_operation_failed) {
				TraceEvent(SevWarn, "RecruitTLogMatchingSetNotAvailable", id).error(e);
			} else {
				TraceEvent(SevError, "RecruitTLogsRequestError", id).error(e);
				throw;
			}
		}
	}
}

void ClusterController::checkOutstandingRemoteRecruitmentRequests() {
	for (int i = 0; i < outstandingRemoteRecruitmentRequests.size(); i++) {
		Reference<RecruitRemoteWorkersInfo> info = outstandingRemoteRecruitmentRequests[i];
		try {
			info->rep = findRemoteWorkersForConfiguration(info->req);
			if (info->dbgId.present()) {
				TraceEvent("CheckOutstandingRemoteRecruitment", info->dbgId.get())
				    .detail("Request", info->req.configuration.toString());
			}
			info->waitForCompletion.trigger();
			swapAndPop(&outstandingRemoteRecruitmentRequests, i--);
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers || e.code() == error_code_operation_failed) {
				TraceEvent(SevWarn, "RecruitRemoteTLogMatchingSetNotAvailable", id).error(e);
			} else {
				TraceEvent(SevError, "RecruitRemoteTLogsRequestError", id).error(e);
				throw;
			}
		}
	}
}

void ClusterController::checkOutstandingStorageRequests() {
	for (int i = 0; i < outstandingStorageRequests.size(); i++) {
		auto& req = outstandingStorageRequests[i];
		try {
			if (req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop(&outstandingStorageRequests, i--);
			} else {
				if (!gotProcessClasses && !req.first.criticalRecruitment)
					throw no_more_servers();

				auto worker = getStorageWorker(req.first);
				RecruitStorageReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send(rep);
				swapAndPop(&outstandingStorageRequests, i--);
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitStorageNotAvailable", id)
				    .errorUnsuppressed(e)
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i)
				    .detail("IsCriticalRecruitment", req.first.criticalRecruitment);
			} else {
				TraceEvent(SevError, "RecruitStorageError", id).error(e);
				throw;
			}
		}
	}
}

// When workers aren't available at the time of request, the request
// gets added to a list of outstanding reqs. Here, we try to resolve these
// outstanding requests.
void ClusterController::checkOutstandingBlobWorkerRequests() {
	for (int i = 0; i < outstandingBlobWorkerRequests.size(); i++) {
		auto& req = outstandingBlobWorkerRequests[i];
		try {
			if (req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop(&outstandingBlobWorkerRequests, i--);
			} else {
				if (!gotProcessClasses)
					throw no_more_servers();

				auto worker = getBlobWorker(req.first);
				RecruitBlobWorkerReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send(rep);
				// can remove it once we know the worker was found
				swapAndPop(&outstandingBlobWorkerRequests, i--);
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitBlobWorkerNotAvailable", id)
				    .errorUnsuppressed(e)
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i);
			} else {
				TraceEvent(SevError, "RecruitBlobWorkerError", id).error(e);
				throw;
			}
		}
	}
}

// Finds and returns a new process for role
WorkerDetails ClusterController::findNewProcessForSingleton(ProcessClass::ClusterRole role,
                                                            std::map<Optional<Standalone<StringRef>>, int>& id_used) {
	// find new process in cluster for role
	WorkerDetails newWorker = getWorkerForRoleInDatacenter(
	                              clusterControllerDcId, role, ProcessClass::NeverAssign, db.config, id_used, {}, true)
	                              .worker;

	// check if master's process is actually better suited for role
	if (onMasterIsBetter(newWorker, role)) {
		newWorker = id_worker[masterProcessId.get()].details;
	}

	// acknowledge that the pid is now potentially used by this role as well
	id_used[newWorker.interf.locality.processId()]++;

	return newWorker;
}

// Return best possible fitness for singleton. Note that lower fitness is better.
ProcessClass::Fitness ClusterController::findBestFitnessForSingleton(const WorkerDetails& worker,
                                                                     const ProcessClass::ClusterRole& role) {
	auto bestFitness = worker.processClass.machineClassFitness(role);
	// If the process has been marked as excluded, we take the max with ExcludeFit to ensure its fit
	// is at least as bad as ExcludeFit. This assists with successfully offboarding such processes
	// and removing them from the cluster.
	if (db.config.isExcludedServer(worker.interf.addresses())) {
		bestFitness = std::max(bestFitness, ProcessClass::ExcludeFit);
	}
	return bestFitness;
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
void ClusterController::checkBetterSingletons() {
	if (!masterProcessId.present() || db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		return;
	}

	// note: this map doesn't consider pids used by existing singletons
	std::map<Optional<Standalone<StringRef>>, int> id_used = getUsedIds();

	// We prefer spreading out other roles more than separating singletons on their own process
	// so we artificially amplify the pid count for the processes used by non-singleton roles.
	// In other words, we make the processes used for other roles less desirable to be used
	// by singletons as well.
	for (auto& it : id_used) {
		it.second *= PID_USED_AMP_FOR_NON_SINGLETON;
	}

	// Try to find a new process for each singleton.
	WorkerDetails newRKWorker = findNewProcessForSingleton(ProcessClass::Ratekeeper, id_used);
	WorkerDetails newDDWorker = findNewProcessForSingleton(ProcessClass::DataDistributor, id_used);
	WorkerDetails newCSWorker = findNewProcessForSingleton(ProcessClass::ConsistencyScan, id_used);

	WorkerDetails newBMWorker;
	WorkerDetails newMGWorker;
	if (db.blobGranulesEnabled.get()) {
		newBMWorker = findNewProcessForSingleton(ProcessClass::BlobManager, id_used);
		if (db.blobRestoreEnabled.get()) {
			newMGWorker = findNewProcessForSingleton(ProcessClass::BlobMigrator, id_used);
		}
	}

	WorkerDetails newEKPWorker;
	if (SERVER_KNOBS->ENABLE_ENCRYPTION) {
		newEKPWorker = findNewProcessForSingleton(ProcessClass::EncryptKeyProxy, id_used);
	}

	// Find best possible fitnesses for each singleton.
	auto bestFitnessForRK = findBestFitnessForSingleton(newRKWorker, ProcessClass::Ratekeeper);
	auto bestFitnessForDD = findBestFitnessForSingleton(newDDWorker, ProcessClass::DataDistributor);
	auto bestFitnessForCS = findBestFitnessForSingleton(newCSWorker, ProcessClass::ConsistencyScan);

	ProcessClass::Fitness bestFitnessForBM;
	ProcessClass::Fitness bestFitnessForMG;
	if (db.blobGranulesEnabled.get()) {
		bestFitnessForBM = findBestFitnessForSingleton(newBMWorker, ProcessClass::BlobManager);
		if (db.blobRestoreEnabled.get()) {
			bestFitnessForMG = findBestFitnessForSingleton(newMGWorker, ProcessClass::BlobManager);
		}
	}

	ProcessClass::Fitness bestFitnessForEKP;
	if (SERVER_KNOBS->ENABLE_ENCRYPTION) {
		bestFitnessForEKP = findBestFitnessForSingleton(newEKPWorker, ProcessClass::EncryptKeyProxy);
	}

	auto& serverInfoDb = db.serverInfo->get();
	auto rkSingleton = RatekeeperSingleton(serverInfoDb.ratekeeper);
	auto ddSingleton = DataDistributorSingleton(serverInfoDb.distributor);
	ConsistencyScanSingleton csSingleton(serverInfoDb.consistencyScan);
	BlobManagerSingleton bmSingleton(serverInfoDb.blobManager);
	BlobMigratorSingleton mgSingleton(serverInfoDb.blobMigrator);
	EncryptKeyProxySingleton ekpSingleton(serverInfoDb.encryptKeyProxy);

	// Check if the singletons are healthy.
	// side effect: try to rerecruit the singletons to more optimal processes
	bool rkHealthy =
	    isHealthySingleton<RatekeeperSingleton>(newRKWorker, rkSingleton, bestFitnessForRK, recruitingRatekeeperID);

	bool ddHealthy = isHealthySingleton<DataDistributorSingleton>(
	    newDDWorker, ddSingleton, bestFitnessForDD, recruitingDistributorID);

	bool csHealthy = isHealthySingleton<ConsistencyScanSingleton>(
	    newCSWorker, csSingleton, bestFitnessForCS, recruitingConsistencyScanID);

	bool bmHealthy = true;
	bool mgHealthy = true;
	if (db.blobGranulesEnabled.get()) {
		bmHealthy = isHealthySingleton<BlobManagerSingleton>(
		    newBMWorker, bmSingleton, bestFitnessForBM, recruitingBlobManagerID);
		if (db.blobRestoreEnabled.get()) {
			mgHealthy = isHealthySingleton<BlobMigratorSingleton>(
			    newMGWorker, mgSingleton, bestFitnessForMG, recruitingBlobMigratorID);
		}
	}

	bool ekpHealthy = true;
	if (SERVER_KNOBS->ENABLE_ENCRYPTION) {
		ekpHealthy = isHealthySingleton<EncryptKeyProxySingleton>(
		    newEKPWorker, ekpSingleton, bestFitnessForEKP, recruitingEncryptKeyProxyID);
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
	if (db.blobGranulesEnabled.get()) {
		currBMProcessId = bmSingleton.getInterface().locality.processId();
		newBMProcessId = newBMWorker.interf.locality.processId();
		if (db.blobRestoreEnabled.get()) {
			currMGProcessId = mgSingleton.getInterface().locality.processId();
			newMGProcessId = newMGWorker.interf.locality.processId();
		}
	}

	Optional<Standalone<StringRef>> currEKPProcessId, newEKPProcessId;
	if (SERVER_KNOBS->ENABLE_ENCRYPTION) {
		currEKPProcessId = ekpSingleton.getInterface().locality.processId();
		newEKPProcessId = newEKPWorker.interf.locality.processId();
	}

	std::vector<Optional<Standalone<StringRef>>> currPids = { currRKProcessId, currDDProcessId, currCSProcessId };
	std::vector<Optional<Standalone<StringRef>>> newPids = { newRKProcessId, newDDProcessId, newCSProcessId };
	if (db.blobGranulesEnabled.get()) {
		currPids.emplace_back(currBMProcessId);
		newPids.emplace_back(newBMProcessId);
		if (db.blobRestoreEnabled.get()) {
			currPids.emplace_back(currMGProcessId);
			newPids.emplace_back(newMGProcessId);
		}
	}

	if (SERVER_KNOBS->ENABLE_ENCRYPTION) {
		currPids.emplace_back(currEKPProcessId);
		newPids.emplace_back(newEKPProcessId);
	}

	auto currColocMap = getColocCounts(currPids);
	auto newColocMap = getColocCounts(newPids);

	// if the knob is disabled, the BM coloc counts should have no affect on the coloc counts check below
	if (!db.blobGranulesEnabled.get()) {
		ASSERT(currColocMap[currBMProcessId] == 0);
		ASSERT(newColocMap[newBMProcessId] == 0);
		if (db.blobRestoreEnabled.get()) {
			ASSERT(currColocMap[currMGProcessId] == 0);
			ASSERT(newColocMap[newMGProcessId] == 0);
		}
	}

	// if the knob is disabled, the EKP coloc counts should have no affect on the coloc counts check below
	if (!SERVER_KNOBS->ENABLE_ENCRYPTION) {
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
			rkSingleton.recruit(*this);
		} else if (newColocMap[newDDProcessId] < currColocMap[currDDProcessId]) {
			ddSingleton.recruit(*this);
		} else if (db.blobGranulesEnabled.get() && newColocMap[newBMProcessId] < currColocMap[currBMProcessId]) {
			bmSingleton.recruit(*this);
		} else if (db.blobGranulesEnabled.get() && db.blobRestoreEnabled.get() &&
		           newColocMap[newMGProcessId] < currColocMap[currMGProcessId]) {
			mgSingleton.recruit(*this);
		} else if (SERVER_KNOBS->ENABLE_ENCRYPTION && newColocMap[newEKPProcessId] < currColocMap[currEKPProcessId]) {
			ekpSingleton.recruit(*this);
		} else if (newColocMap[newCSProcessId] < currColocMap[currCSProcessId]) {
			csSingleton.recruit(*this);
		}
	}
}

Future<Void> ClusterController::doCheckOutstandingRequests() {
	return ClusterControllerImpl::doCheckOutstandingRequests(this);
}

Future<Void> ClusterController::doCheckOutstandingRemoteRequests() {
	return ClusterControllerImpl::doCheckOutstandingRemoteRequests(this);
}

void ClusterController::checkOutstandingRequests() {
	if (outstandingRemoteRequestChecker.isReady()) {
		outstandingRemoteRequestChecker = doCheckOutstandingRemoteRequests();
	}

	if (outstandingRequestChecker.isReady()) {
		outstandingRequestChecker = doCheckOutstandingRequests();
	}
}

Future<Void> ClusterController::workerAvailabilityWatch(WorkerInterface worker, ProcessClass startingClass) {
	return ClusterControllerImpl::workerAvailabilityWatch(this, worker, startingClass);
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

void ClusterController::clusterRecruitStorage(RecruitStorageRequest req) {
	try {
		if (!gotProcessClasses && !req.criticalRecruitment)
			throw no_more_servers();
		auto worker = getStorageWorker(req);
		RecruitStorageReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send(rep);
	} catch (Error& e) {
		if (e.code() == error_code_no_more_servers) {
			outstandingStorageRequests.emplace_back(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT);
			TraceEvent(SevWarn, "RecruitStorageNotAvailable", id)
			    .error(e)
			    .detail("IsCriticalRecruitment", req.criticalRecruitment);
		} else {
			TraceEvent(SevError, "RecruitStorageError", id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

// Trys to send a reply to req with a worker (process) that a blob worker can be recruited on
// Otherwise, add the req to a list of outstanding reqs that will eventually be dealt with
void ClusterController::clusterRecruitBlobWorker(RecruitBlobWorkerRequest req) {
	try {
		if (!gotProcessClasses)
			throw no_more_servers();
		auto worker = getBlobWorker(req);
		RecruitBlobWorkerReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send(rep);
	} catch (Error& e) {
		if (e.code() == error_code_no_more_servers) {
			outstandingBlobWorkerRequests.emplace_back(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT);
			TraceEvent(SevWarn, "RecruitBlobWorkerNotAvailable", id).error(e);
		} else {
			TraceEvent(SevError, "RecruitBlobWorkerError", id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

void ClusterController::clusterRegisterMaster(RegisterMasterRequest const& req) {
	req.reply.send(Void());

	TraceEvent("MasterRegistrationReceived", id)
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
	if (db.serverInfo->get().master.id() != req.id || req.registrationCount <= db.masterRegistrationCount) {
		TraceEvent("MasterRegistrationNotFound", id)
		    .detail("MasterId", req.id)
		    .detail("ExistingId", db.serverInfo->get().master.id())
		    .detail("RegCount", req.registrationCount)
		    .detail("ExistingRegCount", db.masterRegistrationCount);
		return;
	}

	if (req.recoveryState == RecoveryState::FULLY_RECOVERED) {
		db.unfinishedRecoveries = 0;
		db.logGenerations = 0;
		ASSERT(!req.logSystemConfig.oldTLogs.size());
	} else {
		db.logGenerations = std::max<int>(db.logGenerations, req.logSystemConfig.oldTLogs.size());
	}

	db.masterRegistrationCount = req.registrationCount;
	db.recoveryStalled = req.recoveryStalled;
	if (req.configuration.present()) {
		db.config = req.configuration.get();

		if (req.recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
			gotFullyRecoveredConfig = true;
			db.fullyRecoveredConfig = req.configuration.get();
			for (auto& it : id_worker) {
				bool isExcludedFromConfig =
				    db.fullyRecoveredConfig.isExcludedServer(it.second.details.interf.addresses());
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
	auto dbInfo = db.serverInfo->get();

	if (dbInfo.recoveryState != req.recoveryState) {
		dbInfo.recoveryState = req.recoveryState;
		isChanged = true;
	}

	if (dbInfo.priorCommittedLogServers != req.priorCommittedLogServers) {
		dbInfo.priorCommittedLogServers = req.priorCommittedLogServers;
		isChanged = true;
	}

	// Construct the client information
	if (db.clientInfo->get().commitProxies != req.commitProxies || db.clientInfo->get().grvProxies != req.grvProxies ||
	    db.clientInfo->get().tenantMode != db.config.tenantMode ||
	    db.clientInfo->get().isEncryptionEnabled != SERVER_KNOBS->ENABLE_ENCRYPTION ||
	    db.clientInfo->get().clusterId != db.serverInfo->get().client.clusterId ||
	    db.clientInfo->get().clusterType != db.clusterType ||
	    db.clientInfo->get().metaclusterName != db.metaclusterName ||
	    db.clientInfo->get().encryptKeyProxy != db.serverInfo->get().encryptKeyProxy) {
		TraceEvent("PublishNewClientInfo", id)
		    .detail("Master", dbInfo.master.id())
		    .detail("GrvProxies", db.clientInfo->get().grvProxies)
		    .detail("ReqGrvProxies", req.grvProxies)
		    .detail("CommitProxies", db.clientInfo->get().commitProxies)
		    .detail("ReqCPs", req.commitProxies)
		    .detail("TenantMode", db.clientInfo->get().tenantMode.toString())
		    .detail("ReqTenantMode", db.config.tenantMode.toString())
		    .detail("EncryptionEnabled", SERVER_KNOBS->ENABLE_ENCRYPTION)
		    .detail("ClusterId", db.serverInfo->get().client.clusterId)
		    .detail("ClientClusterId", db.clientInfo->get().clusterId)
		    .detail("ClusterType", db.clientInfo->get().clusterType)
		    .detail("ReqClusterType", db.clusterType)
		    .detail("MetaclusterName", db.clientInfo->get().metaclusterName)
		    .detail("ReqMetaclusterName", db.metaclusterName);
		isChanged = true;
		// TODO why construct a new one and not just copy the old one and change proxies + id?
		ClientDBInfo clientInfo;
		clientInfo.encryptKeyProxy = db.serverInfo->get().encryptKeyProxy;
		clientInfo.id = deterministicRandom()->randomUniqueID();
		clientInfo.isEncryptionEnabled = SERVER_KNOBS->ENABLE_ENCRYPTION;
		clientInfo.commitProxies = req.commitProxies;
		clientInfo.grvProxies = req.grvProxies;
		clientInfo.tenantMode = TenantAPI::tenantModeForClusterType(db.clusterType, db.config.tenantMode);
		clientInfo.clusterId = db.serverInfo->get().client.clusterId;
		clientInfo.clusterType = db.clusterType;
		clientInfo.metaclusterName = db.metaclusterName;
		db.clientInfo->set(clientInfo);
		dbInfo.client = db.clientInfo->get();
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
		dbInfo.infoGeneration = db.incrementAndGetDbInfoCount();
		db.serverInfo->set(dbInfo);
	}

	checkOutstandingRequests();
}

Future<Void> ClusterController::registerWorker(RegisterWorkerRequest req,
                                               ClusterConnectionString cs,
                                               ConfigBroadcaster* configBroadcaster) {
	return ClusterControllerImpl::registerWorker(this, req, cs, configBroadcaster);
}

Future<Void> ClusterController::timeKeeperSetVersion() {
	return ClusterControllerImpl::timeKeeperSetVersion(this);
}

// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid
// running out of space, it limits the max number of entries and clears old entries on each update. This mapping is
// used from backup and restore to get the version information for a timestamp.
Future<Void> ClusterController::timeKeeper() {
	return ClusterControllerImpl::timeKeeper(this);
}

Future<Void> ClusterController::statusServer(FutureStream<StatusRequest> requests,
                                             ServerCoordinators coordinators,
                                             ConfigBroadcaster const* configBroadcaster) {
	return ClusterControllerImpl::statusServer(this, requests, coordinators, configBroadcaster);
}

Future<Void> ClusterController::monitorProcessClasses() {
	return ClusterControllerImpl::monitorProcessClasses(this);
}

Future<Void> ClusterController::updatedChangingDatacenters() {
	return ClusterControllerImpl::updatedChangingDatacenters(this);
}

Future<Void> ClusterController::updatedChangedDatacenters() {
	return ClusterControllerImpl::updatedChangedDatacenters(this);
}

Future<Void> ClusterController::updateDatacenterVersionDifference() {
	return ClusterControllerImpl::updateDatacenterVersionDifference(this);
}

Future<Void> ClusterController::updateRemoteDCHealth() {
	return ClusterControllerImpl::updateRemoteDCHealth(this);
}

Future<Void> ClusterController::handleForcedRecoveries(ClusterControllerFullInterface interf) {
	return ClusterControllerImpl::handleForcedRecoveries(this, interf);
}

Future<Void> ClusterController::triggerAuditStorage(TriggerAuditRequest req) {
	return ClusterControllerImpl::triggerAuditStorage(this, req);
}

Future<Void> ClusterController::handleTriggerAuditStorage(ClusterControllerFullInterface interf) {
	return ClusterControllerImpl::handleTriggerAuditStorage(this, interf);
}

Future<Void> ClusterController::startDataDistributor(double waitTime) {
	return ClusterControllerImpl::startDataDistributor(this, waitTime);
}

Future<Void> ClusterController::monitorDataDistributor() {
	return ClusterControllerImpl::monitorDataDistributor(this);
}

Future<Void> ClusterController::startRatekeeper(double waitTime) {
	return ClusterControllerImpl::startRatekeeper(this, waitTime);
}

Future<Void> ClusterController::monitorRatekeeper() {
	return ClusterControllerImpl::monitorRatekeeper(this);
}

Future<Void> ClusterController::startConsistencyScan() {
	return ClusterControllerImpl::startConsistencyScan(this);
}

Future<Void> ClusterController::monitorConsistencyScan() {
	return ClusterControllerImpl::monitorConsistencyScan(this);
}

Future<Void> ClusterController::startEncryptKeyProxy(double waitTime) {
	return ClusterControllerImpl::startEncryptKeyProxy(this, waitTime);
}

Future<Void> ClusterController::monitorEncryptKeyProxy() {
	return ClusterControllerImpl::monitorEncryptKeyProxy(this);
}

Future<int64_t> ClusterController::getNextBMEpoch() {
	return ClusterControllerImpl::getNextBMEpoch(this);
}

Future<Void> ClusterController::watchBlobRestoreCommand() {
	return ClusterControllerImpl::watchBlobRestoreCommand(this);
}

Future<Void> ClusterController::startBlobMigrator(double waitTime) {
	return ClusterControllerImpl::startBlobMigrator(this, waitTime);
}

Future<Void> ClusterController::monitorBlobMigrator() {
	return ClusterControllerImpl::monitorBlobMigrator(this);
}

Future<Void> ClusterController::startBlobManager(double waitTime) {
	return ClusterControllerImpl::startBlobManager(this, waitTime);
}

Future<Void> ClusterController::monitorBlobManager() {
	return ClusterControllerImpl::monitorBlobManager(this);
}

Future<Void> ClusterController::watchBlobGranulesConfigKey() {
	return ClusterControllerImpl::watchBlobGranulesConfigKey(this);
}

Future<Void> ClusterController::dbInfoUpdater() {
	return ClusterControllerImpl::dbInfoUpdater(this);
}

Future<Void> ClusterController::workerHealthMonitor() {
	return ClusterControllerImpl::workerHealthMonitor(this);
}

Future<Void> ClusterController::metaclusterMetricsUpdater() {
	return ClusterControllerImpl::metaclusterMetricsUpdater(this);
}

Future<Void> ClusterController::updateClusterId() {
	return ClusterControllerImpl::updateClusterId(this);
}

Future<Void> ClusterController::handleGetEncryptionAtRestMode(ClusterControllerFullInterface ccInterf) {
	return ClusterControllerImpl::handleGetEncryptionAtRestMode(this, ccInterf);
}

Future<Void> ClusterController::recruitNewMaster(ClusterControllerDBInfo* db, MasterInterface* newMaster) {
	return ClusterControllerImpl::recruitNewMaster(this, db, newMaster);
}

Future<Void> ClusterController::clusterRecruitFromConfiguration(Reference<RecruitWorkersInfo> req) {
	return ClusterControllerImpl::clusterRecruitFromConfiguration(this, req);
}

Future<RecruitRemoteFromConfigurationReply> ClusterController::clusterRecruitRemoteFromConfiguration(
    Reference<RecruitRemoteWorkersInfo> req) {
	return ClusterControllerImpl::clusterRecruitRemoteFromConfiguration(this, req);
}

Future<Void> ClusterController::run(ClusterControllerFullInterface interf,
                                    Future<Void> leaderFail,
                                    ServerCoordinators coordinators,
                                    LocalityData locality,
                                    ConfigDBType configDBType,
                                    Future<Void> recoveredDiskFiles,
                                    Reference<AsyncVar<Optional<UID>>> clusterId) {
	return ClusterControllerImpl::run(
	    interf, leaderFail, coordinators, locality, configDBType, recoveredDiskFiles, clusterId);
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
                                     Future<Void> recoveredDiskFiles,
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

				wait(ClusterController::run(
				    cci, leaderFail, coordinators, locality, configDBType, recoveredDiskFiles, clusterId));
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
                                     Future<Void> recoveredDiskFiles,
                                     LocalityData locality,
                                     ConfigDBType configDBType,
                                     Reference<AsyncVar<Optional<UID>>> clusterId) {

	// Defer this wait optimization of cluster configuration has 'Encryption data at-rest' enabled.
	// Encryption depends on available of EncryptKeyProxy (EKP) FDB role to enable fetch/refresh of
	// encryption keys created and managed by external KeyManagementService (KMS).
	//
	// TODO: Wait optimization is to ensure the worker server on the same process gets registered with the
	// new CC before recruitment. Unify the codepath for both Encryption enable vs disable scenarios.

	if (!SERVER_KNOBS->ENABLE_ENCRYPTION) {
		wait(recoveredDiskFiles);
		TraceEvent("RecoveredDiskFiles").log();
	} else {
		TraceEvent("RecoveredDiskFiles_Deferred").log();
	}

	state bool hasConnected = false;
	loop {
		try {
			ServerCoordinators coordinators(connRecord, configDBType);
			wait(clusterController(coordinators,
			                       currentCC,
			                       hasConnected,
			                       asyncPriorityInfo,
			                       locality,
			                       configDBType,
			                       recoveredDiskFiles,
			                       clusterId));
			hasConnected = true;
		} catch (Error& e) {
			if (e.code() != error_code_coordinators_changed)
				throw; // Expected to terminate fdbserver
		}
	}
}

bool ClusterController::workerAvailable(WorkerInfo const& worker, bool checkStable) const {
	return (now() - startTime < 2 * FLOW_KNOBS->SERVER_REQUEST_INTERVAL) ||
	       (IFailureMonitor::failureMonitor().getState(worker.details.interf.storage.getEndpoint()).isAvailable() &&
	        (!checkStable || worker.reboots < 2));
}

bool ClusterController::isLongLivedStateless(Optional<Key> const& processId) const {
	return (db.serverInfo->get().distributor.present() &&
	        db.serverInfo->get().distributor.get().locality.processId() == processId) ||
	       (db.serverInfo->get().ratekeeper.present() &&
	        db.serverInfo->get().ratekeeper.get().locality.processId() == processId) ||
	       (db.serverInfo->get().blobManager.present() &&
	        db.serverInfo->get().blobManager.get().locality.processId() == processId) ||
	       (db.serverInfo->get().blobMigrator.present() &&
	        db.serverInfo->get().blobMigrator.get().locality.processId() == processId) ||
	       (db.serverInfo->get().encryptKeyProxy.present() &&
	        db.serverInfo->get().encryptKeyProxy.get().locality.processId() == processId) ||
	       (db.serverInfo->get().consistencyScan.present() &&
	        db.serverInfo->get().consistencyScan.get().locality.processId() == processId);
}

WorkerDetails ClusterController::getStorageWorker(RecruitStorageRequest const& req) {
	std::set<Optional<Standalone<StringRef>>> excludedMachines(req.excludeMachines.begin(), req.excludeMachines.end());
	std::set<Optional<Standalone<StringRef>>> includeDCs(req.includeDCs.begin(), req.includeDCs.end());
	std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());

	for (auto& it : id_worker)
		if (workerAvailable(it.second, false) && it.second.details.recoveredDiskFiles &&
		    !excludedMachines.count(it.second.details.interf.locality.zoneId()) &&
		    (includeDCs.size() == 0 || includeDCs.count(it.second.details.interf.locality.dcId())) &&
		    !addressExcluded(excludedAddresses, it.second.details.interf.address()) &&
		    (!it.second.details.interf.secondaryAddress().present() ||
		     !addressExcluded(excludedAddresses, it.second.details.interf.secondaryAddress().get())) &&
		    it.second.details.processClass.machineClassFitness(ProcessClass::Storage) <= ProcessClass::UnsetFit) {
			return it.second.details;
		}

	if (req.criticalRecruitment) {
		ProcessClass::Fitness bestFit = ProcessClass::NeverAssign;
		Optional<WorkerDetails> bestInfo;
		for (auto& it : id_worker) {
			ProcessClass::Fitness fit = it.second.details.processClass.machineClassFitness(ProcessClass::Storage);
			if (workerAvailable(it.second, false) && it.second.details.recoveredDiskFiles &&
			    !excludedMachines.count(it.second.details.interf.locality.zoneId()) &&
			    (includeDCs.size() == 0 || includeDCs.count(it.second.details.interf.locality.dcId())) &&
			    !addressExcluded(excludedAddresses, it.second.details.interf.address()) && fit < bestFit) {
				bestFit = fit;
				bestInfo = it.second.details;
			}
		}

		if (bestInfo.present()) {
			return bestInfo.get();
		}
	}

	throw no_more_servers();
}

WorkerDetails ClusterController::getBlobWorker(RecruitBlobWorkerRequest const& req) {
	std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());
	for (auto& it : id_worker) {
		// the worker must be available, have the same dcID as CC,
		// not be one of the excluded addrs from req and have the approriate fitness
		if (workerAvailable(it.second, false) && clusterControllerDcId == it.second.details.interf.locality.dcId() &&
		    !addressExcluded(excludedAddresses, it.second.details.interf.address()) &&
		    (!it.second.details.interf.secondaryAddress().present() ||
		     !addressExcluded(excludedAddresses, it.second.details.interf.secondaryAddress().get())) &&
		    it.second.details.processClass.machineClassFitness(ProcessClass::BlobWorker) == ProcessClass::BestFit) {
			return it.second.details;
		}
	}

	throw no_more_servers();
}

std::vector<WorkerDetails> ClusterController::getWorkersForSeedServers(
    DatabaseConfiguration const& conf,
    Reference<IReplicationPolicy> const& policy,
    Optional<Optional<Standalone<StringRef>>> const& dcId) {
	std::map<ProcessClass::Fitness, std::vector<WorkerDetails>> fitness_workers;
	std::vector<WorkerDetails> results;
	Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
	LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*)logServerSet.getPtr();
	bool bCompleted = false;

	for (auto& it : id_worker) {
		auto fitness = it.second.details.processClass.machineClassFitness(ProcessClass::Storage);
		if (workerAvailable(it.second, false) && it.second.details.recoveredDiskFiles &&
		    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
		    !isExcludedDegradedServer(it.second.details.interf.addresses()) && fitness != ProcessClass::NeverAssign &&
		    (!dcId.present() || it.second.details.interf.locality.dcId() == dcId.get())) {
			fitness_workers[fitness].push_back(it.second.details);
		}
	}

	for (auto& it : fitness_workers) {
		for (auto& worker : it.second) {
			logServerMap->add(worker.interf.locality, &worker);
		}

		std::vector<LocalityEntry> bestSet;
		if (logServerSet->selectReplicas(policy, bestSet)) {
			results.reserve(bestSet.size());
			for (auto& entry : bestSet) {
				auto object = logServerMap->getObject(entry);
				results.push_back(*object);
			}
			bCompleted = true;
			break;
		}
	}

	logServerSet->clear();
	logServerSet.clear();

	if (!bCompleted) {
		throw no_more_servers();
	}

	return results;
}

void ClusterController::addWorkersByLowestField(StringRef field,
                                                int desired,
                                                const std::vector<WorkerDetails>& workers,
                                                std::set<WorkerDetails>& resultSet) {
	typedef Optional<Standalone<StringRef>> Field;
	typedef Optional<Standalone<StringRef>> Zone;
	typedef std::tuple<int, bool, Field> FieldCount;
	typedef std::pair<int, Zone> ZoneCount;

	std::priority_queue<FieldCount, std::vector<FieldCount>, std::greater<FieldCount>> fieldQueue;
	std::map<Field, std::priority_queue<ZoneCount, std::vector<ZoneCount>, std::greater<ZoneCount>>> field_zoneQueue;

	std::map<Field, std::pair<int, bool>> field_count;
	std::map<Zone, std::pair<int, Field>> zone_count;
	std::map<Zone, std::vector<WorkerDetails>> zone_workers;

	// Count the amount of fields and zones already in the result set
	for (auto& worker : resultSet) {
		auto thisField = worker.interf.locality.get(field);
		auto thisZone = worker.interf.locality.zoneId();
		auto thisDc = worker.interf.locality.dcId();

		auto& fitness = field_count[thisField];
		fitness.first++;
		fitness.second = thisDc == clusterControllerDcId;

		auto& zc = zone_count[thisZone];
		zc.first++;
		zc.second = thisField;
	}

	for (auto& worker : workers) {
		auto thisField = worker.interf.locality.get(field);
		auto thisZone = worker.interf.locality.zoneId();

		if (field_count.count(thisField)) {
			zone_workers[thisZone].push_back(worker);
			zone_count[thisZone].second = thisField;
		}
	}

	// try to avoid fields in the cluster controller datacenter if everything else is equal
	for (auto& it : field_count) {
		fieldQueue.emplace(it.second.first, it.second.second, it.first);
	}

	for (auto& it : zone_count) {
		field_zoneQueue[it.second.second].emplace(it.second.first, it.first);
	}

	// start with the least used field, and try to find a worker with that field
	while (fieldQueue.size()) {
		auto lowestField = fieldQueue.top();
		auto& lowestZoneQueue = field_zoneQueue[std::get<2>(lowestField)];
		bool added = false;
		// start with the least used zoneId, and try and find a worker with that zone
		while (lowestZoneQueue.size() && !added) {
			auto lowestZone = lowestZoneQueue.top();
			auto& zoneWorkers = zone_workers[lowestZone.second];

			while (zoneWorkers.size() && !added) {
				if (!resultSet.count(zoneWorkers.back())) {
					resultSet.insert(zoneWorkers.back());
					if (resultSet.size() == desired) {
						return;
					}
					added = true;
				}
				zoneWorkers.pop_back();
			}
			lowestZoneQueue.pop();
			if (added && zoneWorkers.size()) {
				++lowestZone.first;
				lowestZoneQueue.push(lowestZone);
			}
		}
		fieldQueue.pop();
		if (added) {
			++std::get<0>(lowestField);
			fieldQueue.push(lowestField);
		}
	}
}

void ClusterController::addWorkersByLowestZone(int desired,
                                               const std::vector<WorkerDetails>& workers,
                                               std::set<WorkerDetails>& resultSet) {
	typedef Optional<Standalone<StringRef>> Zone;
	typedef std::pair<int, Zone> ZoneCount;

	std::map<Zone, int> zone_count;
	std::map<Zone, std::vector<WorkerDetails>> zone_workers;
	std::priority_queue<ZoneCount, std::vector<ZoneCount>, std::greater<ZoneCount>> zoneQueue;

	for (const auto& worker : workers) {
		auto thisZone = worker.interf.locality.zoneId();
		zone_count[thisZone] = 0;
		zone_workers[thisZone].push_back(worker);
	}

	for (auto& worker : resultSet) {
		auto thisZone = worker.interf.locality.zoneId();
		zone_count[thisZone]++;
	}

	for (auto& it : zone_count) {
		zoneQueue.emplace(it.second, it.first);
	}

	while (zoneQueue.size()) {
		auto lowestZone = zoneQueue.top();
		auto& zoneWorkers = zone_workers[lowestZone.second];

		bool added = false;
		while (zoneWorkers.size() && !added) {
			if (!resultSet.count(zoneWorkers.back())) {
				resultSet.insert(zoneWorkers.back());
				if (resultSet.size() == desired) {
					return;
				}
				added = true;
			}
			zoneWorkers.pop_back();
		}
		zoneQueue.pop();
		if (added && zoneWorkers.size()) {
			++lowestZone.first;
			zoneQueue.push(lowestZone);
		}
	}
}

// Log the reason why the worker is considered as unavailable.
void ClusterController::logWorkerUnavailable(const Severity severity,
                                             const UID& id,
                                             const std::string& method,
                                             const std::string& reason,
                                             const WorkerDetails& details,
                                             const ProcessClass::Fitness& fitness,
                                             const std::set<Optional<Key>>& dcIds) {
	// Construct the list of DCs where the TLog recruitment is happening. This is mainly for logging purpose.
	std::string dcList;
	for (const auto& dc : dcIds) {
		if (!dcList.empty()) {
			dcList += ',';
		}
		dcList += printable(dc);
	}
	// Logging every possible options is a lot for every recruitment; logging all of the options with GoodFit or
	// BestFit may work because there should only be like 30 tlog class processes. Plus, the recruitment happens
	// only during initial database creation and recovery. So these trace events should be sparse.
	if (fitness == ProcessClass::GoodFit || fitness == ProcessClass::BestFit || fitness == ProcessClass::NeverAssign) {
		TraceEvent(severity, "GetTLogTeamWorkerUnavailable", id)
		    .detail("TLogRecruitMethod", method)
		    .detail("Reason", reason)
		    .detail("WorkerID", details.interf.id())
		    .detail("WorkerDC", details.interf.locality.dcId())
		    .detail("Address", details.interf.addresses().toString())
		    .detail("Fitness", fitness)
		    .detail("RecruitmentDcIds", dcList);
	}
}

// A TLog recruitment method specialized for three_data_hall and three_datacenter configurations
// It attempts to evenly recruit processes from across data_halls or datacenters
std::vector<WorkerDetails> ClusterController::getWorkersForTlogsComplex(
    DatabaseConfiguration const& conf,
    int32_t desired,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    StringRef field,
    int minFields,
    int minPerField,
    bool allowDegraded,
    bool checkStable,
    const std::set<Optional<Key>>& dcIds,
    const std::vector<UID>& exclusionWorkerIds) {
	std::map<std::tuple<ProcessClass::Fitness, int, bool>, std::vector<WorkerDetails>> fitness_workers;

	// Go through all the workers to list all the workers that can be recruited.
	for (const auto& [worker_process_id, worker_info] : id_worker) {
		const auto& worker_details = worker_info.details;
		auto fitness = worker_details.processClass.machineClassFitness(ProcessClass::TLog);

		if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), worker_details.interf.id()) !=
		    exclusionWorkerIds.end()) {
			logWorkerUnavailable(SevInfo, id, "complex", "Worker is excluded", worker_details, fitness, dcIds);
			continue;
		}
		if (!workerAvailable(worker_info, checkStable)) {
			logWorkerUnavailable(SevInfo, id, "complex", "Worker is not available", worker_details, fitness, dcIds);
			continue;
		}
		if (!worker_details.recoveredDiskFiles) {
			logWorkerUnavailable(
			    SevInfo, id, "complex", "Worker disk file recovery unfinished", worker_details, fitness, dcIds);
			continue;
		}
		if (conf.isExcludedServer(worker_details.interf.addresses())) {
			logWorkerUnavailable(
			    SevInfo, id, "complex", "Worker server is excluded from the cluster", worker_details, fitness, dcIds);
			continue;
		}
		if (isExcludedDegradedServer(worker_details.interf.addresses())) {
			logWorkerUnavailable(SevInfo,
			                     id,
			                     "complex",
			                     "Worker server is excluded from the cluster due to degradation",
			                     worker_details,
			                     fitness,
			                     dcIds);
			continue;
		}
		if (fitness == ProcessClass::NeverAssign) {
			logWorkerUnavailable(
			    SevDebug, id, "complex", "Worker's fitness is NeverAssign", worker_details, fitness, dcIds);
			continue;
		}
		if (!dcIds.empty() && dcIds.count(worker_details.interf.locality.dcId()) == 0) {
			logWorkerUnavailable(
			    SevDebug, id, "complex", "Worker is not in the target DC", worker_details, fitness, dcIds);
			continue;
		}
		if (!allowDegraded && worker_details.degraded) {
			logWorkerUnavailable(
			    SevInfo, id, "complex", "Worker is degraded and not allowed", worker_details, fitness, dcIds);
			continue;
		}

		fitness_workers[std::make_tuple(fitness, id_used[worker_process_id], isLongLivedStateless(worker_process_id))]
		    .push_back(worker_details);
	}

	auto requiredFitness = ProcessClass::NeverAssign;
	int requiredUsed = 1e6;

	typedef Optional<Standalone<StringRef>> Field;
	typedef Optional<Standalone<StringRef>> Zone;
	std::map<Field, std::pair<std::set<Zone>, std::vector<WorkerDetails>>> field_zones;
	std::set<Field> fieldsWithMin;
	std::map<Field, int> field_count;
	std::map<Field, std::tuple<ProcessClass::Fitness, int, bool>> field_fitness;

	// Determine the best required workers by finding the workers with enough unique zoneIds per field
	for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end(); ++workerIter) {
		deterministicRandom()->randomShuffle(workerIter->second);
		auto fitness = std::get<0>(workerIter->first);
		auto used = std::get<1>(workerIter->first);

		if (fitness > requiredFitness || (fitness == requiredFitness && used > requiredUsed)) {
			break;
		}

		for (auto& worker : workerIter->second) {
			auto thisField = worker.interf.locality.get(field);
			auto& zones = field_zones[thisField];
			if (zones.first.insert(worker.interf.locality.zoneId()).second) {
				zones.second.push_back(worker);
				if (zones.first.size() == minPerField) {
					fieldsWithMin.insert(thisField);
				}
			}
			field_count[thisField]++;
			field_fitness.insert(
			    { thisField, std::make_tuple(fitness, used, worker.interf.locality.dcId() == clusterControllerDcId) });
		}
		if (fieldsWithMin.size() >= minFields) {
			requiredFitness = fitness;
			requiredUsed = used;
		}
	}

	if (fieldsWithMin.size() < minFields) {
		throw no_more_servers();
	}

	std::set<Field> chosenFields;
	// If we cannot use all of the fields, use the fields which allow the best workers to be chosen
	if (fieldsWithMin.size() * minPerField > desired) {
		std::vector<std::tuple<ProcessClass::Fitness, int, bool, int, Field>> orderedFields;
		for (auto& it : fieldsWithMin) {
			auto& fitness = field_fitness[it];
			orderedFields.emplace_back(
			    std::get<0>(fitness), std::get<1>(fitness), std::get<2>(fitness), field_count[it], it);
		}
		std::sort(orderedFields.begin(), orderedFields.end());
		int totalFields = desired / minPerField;
		int maxCount = 0;
		for (int i = 0; i < orderedFields.size() && chosenFields.size() < totalFields; i++) {
			if (chosenFields.size() == totalFields - 1 && maxCount + std::get<3>(orderedFields[i]) < desired) {
				for (int j = i + 1; j < orderedFields.size(); j++) {
					if (maxCount + std::get<3>(orderedFields[j]) >= desired) {
						chosenFields.insert(std::get<4>(orderedFields[j]));
						break;
					}
				}
			}
			if (chosenFields.size() < totalFields) {
				maxCount += std::get<3>(orderedFields[i]);
				chosenFields.insert(std::get<4>(orderedFields[i]));
			}
		}
	} else {
		chosenFields = fieldsWithMin;
	}

	// Create a result set with fulfills the minField and minPerField requirements before adding more workers
	std::set<WorkerDetails> resultSet;
	for (auto& it : chosenFields) {
		auto& w = field_zones[it].second;
		for (int i = 0; i < minPerField; i++) {
			resultSet.insert(w[i]);
		}
	}

	// Continue adding workers to the result set until we reach the desired number of workers
	for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end() && resultSet.size() < desired;
	     ++workerIter) {
		auto fitness = std::get<0>(workerIter->first);
		auto used = std::get<1>(workerIter->first);

		if (fitness > requiredFitness || (fitness == requiredFitness && used > requiredUsed)) {
			break;
		}
		if (workerIter->second.size() + resultSet.size() <= desired) {
			for (auto& worker : workerIter->second) {
				if (chosenFields.count(worker.interf.locality.get(field))) {
					resultSet.insert(worker);
				}
			}
		} else {
			addWorkersByLowestField(field, desired, workerIter->second, resultSet);
		}
	}

	for (auto& result : resultSet) {
		id_used[result.interf.locality.processId()]++;
	}

	return std::vector<WorkerDetails>(resultSet.begin(), resultSet.end());
}

std::vector<WorkerDetails> ClusterController::getWorkersForTlogsComplex(
    DatabaseConfiguration const& conf,
    int32_t desired,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    StringRef field,
    int minFields,
    int minPerField,
    bool checkStable,
    const std::set<Optional<Key>>& dcIds,
    const std::vector<UID>& exclusionWorkerIds) {
	desired = std::max(desired, minFields * minPerField);
	std::map<Optional<Standalone<StringRef>>, int> withDegradedUsed = id_used;
	auto withDegraded = getWorkersForTlogsComplex(
	    conf, desired, withDegradedUsed, field, minFields, minPerField, true, checkStable, dcIds, exclusionWorkerIds);
	RoleFitness withDegradedFitness(withDegraded, ProcessClass::TLog, withDegradedUsed);
	ASSERT(withDegraded.size() <= desired);

	bool usedDegraded = false;
	for (auto& it : withDegraded) {
		if (it.degraded) {
			usedDegraded = true;
			break;
		}
	}

	if (!usedDegraded) {
		id_used = withDegradedUsed;
		return withDegraded;
	}

	try {
		std::map<Optional<Standalone<StringRef>>, int> withoutDegradedUsed = id_used;
		auto withoutDegraded = getWorkersForTlogsComplex(conf,
		                                                 desired,
		                                                 withoutDegradedUsed,
		                                                 field,
		                                                 minFields,
		                                                 minPerField,
		                                                 false,
		                                                 checkStable,
		                                                 dcIds,
		                                                 exclusionWorkerIds);
		RoleFitness withoutDegradedFitness(withoutDegraded, ProcessClass::TLog, withoutDegradedUsed);
		ASSERT(withoutDegraded.size() <= desired);

		if (withDegradedFitness < withoutDegradedFitness) {
			id_used = withDegradedUsed;
			return withDegraded;
		}
		id_used = withoutDegradedUsed;
		return withoutDegraded;
	} catch (Error& e) {
		if (e.code() != error_code_no_more_servers) {
			throw;
		}
		id_used = withDegradedUsed;
		return withDegraded;
	}
}

std::vector<WorkerDetails> ClusterController::getWorkersForTlogsSimple(
    DatabaseConfiguration const& conf,
    int32_t required,
    int32_t desired,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    bool checkStable,
    const std::set<Optional<Key>>& dcIds,
    const std::vector<UID>& exclusionWorkerIds) {
	std::map<std::tuple<ProcessClass::Fitness, int, bool, bool, bool>, std::vector<WorkerDetails>> fitness_workers;

	// Go through all the workers to list all the workers that can be recruited.
	for (const auto& [worker_process_id, worker_info] : id_worker) {
		const auto& worker_details = worker_info.details;
		auto fitness = worker_details.processClass.machineClassFitness(ProcessClass::TLog);

		if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), worker_details.interf.id()) !=
		    exclusionWorkerIds.end()) {
			logWorkerUnavailable(SevInfo, id, "simple", "Worker is excluded", worker_details, fitness, dcIds);
			continue;
		}
		if (!workerAvailable(worker_info, checkStable)) {
			logWorkerUnavailable(SevInfo, id, "simple", "Worker is not available", worker_details, fitness, dcIds);
			continue;
		}
		if (!worker_details.recoveredDiskFiles) {
			logWorkerUnavailable(
			    SevInfo, id, "simple", "Worker disk file recovery unfinished", worker_details, fitness, dcIds);
			continue;
		}
		if (conf.isExcludedServer(worker_details.interf.addresses())) {
			logWorkerUnavailable(
			    SevInfo, id, "simple", "Worker server is excluded from the cluster", worker_details, fitness, dcIds);
			continue;
		}
		if (isExcludedDegradedServer(worker_details.interf.addresses())) {
			logWorkerUnavailable(SevInfo,
			                     id,
			                     "simple",
			                     "Worker server is excluded from the cluster due to degradation",
			                     worker_details,
			                     fitness,
			                     dcIds);
			continue;
		}
		if (fitness == ProcessClass::NeverAssign) {
			logWorkerUnavailable(
			    SevDebug, id, "simple", "Worker's fitness is NeverAssign", worker_details, fitness, dcIds);
			continue;
		}
		if (!dcIds.empty() && dcIds.count(worker_details.interf.locality.dcId()) == 0) {
			logWorkerUnavailable(
			    SevDebug, id, "simple", "Worker is not in the target DC", worker_details, fitness, dcIds);
			continue;
		}

		// This worker is a candidate for TLog recruitment.
		bool inCCDC = worker_details.interf.locality.dcId() == clusterControllerDcId;
		// Prefer recruiting a TransactionClass non-degraded process over a LogClass degraded process
		if (worker_details.degraded) {
			fitness = std::max(fitness, ProcessClass::GoodFit);
		}

		fitness_workers[std::make_tuple(fitness,
		                                id_used[worker_process_id],
		                                worker_details.degraded,
		                                isLongLivedStateless(worker_process_id),
		                                inCCDC)]
		    .push_back(worker_details);
	}

	auto requiredFitness = ProcessClass::BestFit;
	int requiredUsed = 0;

	std::set<Optional<Standalone<StringRef>>> zones;
	std::set<WorkerDetails> resultSet;

	// Determine the best required workers by finding the workers with enough unique zoneIds
	for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end(); ++workerIter) {
		auto fitness = std::get<0>(workerIter->first);
		auto used = std::get<1>(workerIter->first);
		deterministicRandom()->randomShuffle(workerIter->second);
		for (auto& worker : workerIter->second) {
			if (!zones.count(worker.interf.locality.zoneId())) {
				zones.insert(worker.interf.locality.zoneId());
				resultSet.insert(worker);
				if (resultSet.size() == required) {
					break;
				}
			}
		}
		if (resultSet.size() == required) {
			requiredFitness = fitness;
			requiredUsed = used;
			break;
		}
	}

	if (resultSet.size() < required) {
		throw no_more_servers();
	}

	// Continue adding workers to the result set until we reach the desired number of workers
	for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end() && resultSet.size() < desired;
	     ++workerIter) {
		auto fitness = std::get<0>(workerIter->first);
		auto used = std::get<1>(workerIter->first);
		if (fitness > requiredFitness || (fitness == requiredFitness && used > requiredUsed)) {
			break;
		}
		if (workerIter->second.size() + resultSet.size() <= desired) {
			for (auto& worker : workerIter->second) {
				resultSet.insert(worker);
			}
		} else {
			addWorkersByLowestZone(desired, workerIter->second, resultSet);
		}
	}

	ASSERT(resultSet.size() >= required && resultSet.size() <= desired);

	for (auto& result : resultSet) {
		id_used[result.interf.locality.processId()]++;
	}

	return std::vector<WorkerDetails>(resultSet.begin(), resultSet.end());
}

std::vector<WorkerDetails> ClusterController::getWorkersForTlogsBackup(
    DatabaseConfiguration const& conf,
    int32_t required,
    int32_t desired,
    Reference<IReplicationPolicy> const& policy,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    bool checkStable,
    const std::set<Optional<Key>>& dcIds,
    const std::vector<UID>& exclusionWorkerIds) {
	std::map<std::tuple<ProcessClass::Fitness, int, bool, bool>, std::vector<WorkerDetails>> fitness_workers;
	std::vector<WorkerDetails> results;
	Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
	LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*)logServerSet.getPtr();
	bool bCompleted = false;
	desired = std::max(required, desired);

	// Go through all the workers to list all the workers that can be recruited.
	for (const auto& [worker_process_id, worker_info] : id_worker) {
		const auto& worker_details = worker_info.details;
		auto fitness = worker_details.processClass.machineClassFitness(ProcessClass::TLog);

		if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), worker_details.interf.id()) !=
		    exclusionWorkerIds.end()) {
			logWorkerUnavailable(SevInfo, id, "deprecated", "Worker is excluded", worker_details, fitness, dcIds);
			continue;
		}
		if (!workerAvailable(worker_info, checkStable)) {
			logWorkerUnavailable(SevInfo, id, "deprecated", "Worker is not available", worker_details, fitness, dcIds);
			continue;
		}
		if (!worker_details.recoveredDiskFiles) {
			logWorkerUnavailable(
			    SevInfo, id, "deprecated", "Worker disk file recovery unfinished", worker_details, fitness, dcIds);
			continue;
		}
		if (conf.isExcludedServer(worker_details.interf.addresses())) {
			logWorkerUnavailable(SevInfo,
			                     id,
			                     "deprecated",
			                     "Worker server is excluded from the cluster",
			                     worker_details,
			                     fitness,
			                     dcIds);
			continue;
		}
		if (isExcludedDegradedServer(worker_details.interf.addresses())) {
			logWorkerUnavailable(SevInfo,
			                     id,
			                     "deprecated",
			                     "Worker server is excluded from the cluster due to degradation",
			                     worker_details,
			                     fitness,
			                     dcIds);
			continue;
		}
		if (fitness == ProcessClass::NeverAssign) {
			logWorkerUnavailable(
			    SevDebug, id, "deprecated", "Worker's fitness is NeverAssign", worker_details, fitness, dcIds);
			continue;
		}
		if (!dcIds.empty() && dcIds.count(worker_details.interf.locality.dcId()) == 0) {
			logWorkerUnavailable(
			    SevDebug, id, "deprecated", "Worker is not in the target DC", worker_details, fitness, dcIds);
			continue;
		}

		// This worker is a candidate for TLog recruitment.
		bool inCCDC = worker_details.interf.locality.dcId() == clusterControllerDcId;
		// Prefer recruiting a TransactionClass non-degraded process over a LogClass degraded process
		if (worker_details.degraded) {
			fitness = std::max(fitness, ProcessClass::GoodFit);
		}

		fitness_workers[std::make_tuple(fitness, id_used[worker_process_id], worker_details.degraded, inCCDC)]
		    .push_back(worker_details);
	}

	auto requiredFitness = ProcessClass::BestFit;
	int requiredUsed = 0;
	bool requiredDegraded = false;
	bool requiredInCCDC = false;

	// Determine the minimum fitness and used necessary to fulfill the policy
	for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end(); ++workerIter) {
		auto fitness = std::get<0>(workerIter->first);
		auto used = std::get<1>(workerIter->first);
		if (fitness > requiredFitness || used > requiredUsed) {
			if (logServerSet->size() >= required && logServerSet->validate(policy)) {
				bCompleted = true;
				break;
			}
			requiredFitness = fitness;
			requiredUsed = used;
		}

		if (std::get<2>(workerIter->first)) {
			requiredDegraded = true;
		}
		if (std::get<3>(workerIter->first)) {
			requiredInCCDC = true;
		}
		for (auto& worker : workerIter->second) {
			logServerMap->add(worker.interf.locality, &worker);
		}
	}

	if (!bCompleted && !(logServerSet->size() >= required && logServerSet->validate(policy))) {
		std::vector<LocalityData> tLocalities;
		for (auto& object : logServerMap->getObjects()) {
			tLocalities.push_back(object->interf.locality);
		}

		logServerSet->clear();
		logServerSet.clear();
		throw no_more_servers();
	}

	// If we have less than the desired amount, return all of the processes we have
	if (logServerSet->size() <= desired) {
		for (auto& object : logServerMap->getObjects()) {
			results.push_back(*object);
		}
		for (auto& result : results) {
			id_used[result.interf.locality.processId()]++;
		}
		return results;
	}

	// If we have added any degraded processes, try and remove them to see if we can still
	// have the desired amount of processes
	if (requiredDegraded) {
		logServerMap->clear();
		for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end(); ++workerIter) {
			auto fitness = std::get<0>(workerIter->first);
			auto used = std::get<1>(workerIter->first);
			if (fitness > requiredFitness || (fitness == requiredFitness && used > requiredUsed)) {
				break;
			}
			auto addingDegraded = std::get<2>(workerIter->first);
			if (addingDegraded) {
				continue;
			}
			for (auto& worker : workerIter->second) {
				logServerMap->add(worker.interf.locality, &worker);
			}
		}
		if (logServerSet->size() >= desired && logServerSet->validate(policy)) {
			requiredDegraded = false;
		}
	}

	// If we have added any processes in the CC DC, try and remove them to see if we can still
	// have the desired amount of processes
	if (requiredInCCDC) {
		logServerMap->clear();
		for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end(); ++workerIter) {
			auto fitness = std::get<0>(workerIter->first);
			auto used = std::get<1>(workerIter->first);
			if (fitness > requiredFitness || (fitness == requiredFitness && used > requiredUsed)) {
				break;
			}
			auto addingDegraded = std::get<2>(workerIter->first);
			auto inCCDC = std::get<3>(workerIter->first);
			if (inCCDC || (!requiredDegraded && addingDegraded)) {
				continue;
			}
			for (auto& worker : workerIter->second) {
				logServerMap->add(worker.interf.locality, &worker);
			}
		}
		if (logServerSet->size() >= desired && logServerSet->validate(policy)) {
			requiredInCCDC = false;
		}
	}

	logServerMap->clear();
	for (auto workerIter = fitness_workers.begin(); workerIter != fitness_workers.end(); ++workerIter) {
		auto fitness = std::get<0>(workerIter->first);
		auto used = std::get<1>(workerIter->first);
		if (fitness > requiredFitness || (fitness == requiredFitness && used > requiredUsed)) {
			break;
		}
		auto addingDegraded = std::get<2>(workerIter->first);
		auto inCCDC = std::get<3>(workerIter->first);
		if ((!requiredInCCDC && inCCDC) || (!requiredDegraded && addingDegraded)) {
			continue;
		}
		for (auto& worker : workerIter->second) {
			logServerMap->add(worker.interf.locality, &worker);
		}
	}

	if (logServerSet->size() == desired) {
		for (auto& object : logServerMap->getObjects()) {
			results.push_back(*object);
		}
		for (auto& result : results) {
			id_used[result.interf.locality.processId()]++;
		}
		return results;
	}

	std::vector<LocalityEntry> bestSet;
	std::vector<LocalityData> tLocalities;

	// We have more than the desired number of processes, so use the policy engine to
	// pick a diverse subset of them
	bCompleted = findBestPolicySet(
	    bestSet, logServerSet, policy, desired, SERVER_KNOBS->POLICY_RATING_TESTS, SERVER_KNOBS->POLICY_GENERATIONS);
	ASSERT(bCompleted);
	results.reserve(results.size() + bestSet.size());
	for (auto& entry : bestSet) {
		auto object = logServerMap->getObject(entry);
		ASSERT(object);
		results.push_back(*object);
		tLocalities.push_back(object->interf.locality);
	}
	for (auto& result : results) {
		id_used[result.interf.locality.processId()]++;
	}
	TraceEvent("GetTLogTeamDone")
	    .detail("Policy", policy->info())
	    .detail("Results", results.size())
	    .detail("Processes", logServerSet->size())
	    .detail("Workers", id_worker.size())
	    .detail("Required", required)
	    .detail("Desired", desired)
	    .detail("Fitness", requiredFitness)
	    .detail("Used", requiredUsed)
	    .detail("AddingDegraded", requiredDegraded)
	    .detail("InCCDC", requiredInCCDC)
	    .detail("BestCount", bestSet.size())
	    .detail("BestZones", ::describeZones(tLocalities))
	    .detail("BestDataHalls", ::describeDataHalls(tLocalities));
	return results;
}

std::vector<WorkerDetails> ClusterController::getWorkersForTlogs(
    DatabaseConfiguration const& conf,
    int32_t required,
    int32_t desired,
    Reference<IReplicationPolicy> const& policy,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    bool checkStable,
    const std::set<Optional<Key>>& dcIds,
    const std::vector<UID>& exclusionWorkerIds) {
	desired = std::max(required, desired);
	bool useSimple = false;
	if (policy->name() == "Across") {
		PolicyAcross* pa1 = (PolicyAcross*)policy.getPtr();
		Reference<IReplicationPolicy> embedded = pa1->embeddedPolicy();
		if (embedded->name() == "Across") {
			PolicyAcross* pa2 = (PolicyAcross*)embedded.getPtr();
			if (pa2->attributeKey() == "zoneid" && pa2->embeddedPolicyName() == "One") {
				std::map<Optional<Standalone<StringRef>>, int> testUsed = id_used;

				auto workers = getWorkersForTlogsComplex(conf,
				                                         desired,
				                                         id_used,
				                                         pa1->attributeKey(),
				                                         pa1->getCount(),
				                                         pa2->getCount(),
				                                         checkStable,
				                                         dcIds,
				                                         exclusionWorkerIds);

				if (g_network->isSimulated()) {
					try {
						auto testWorkers = getWorkersForTlogsBackup(
						    conf, required, desired, policy, testUsed, checkStable, dcIds, exclusionWorkerIds);
						RoleFitness testFitness(testWorkers, ProcessClass::TLog, testUsed);
						RoleFitness fitness(workers, ProcessClass::TLog, id_used);

						std::map<Optional<Standalone<StringRef>>, int> field_count;
						std::set<Optional<Standalone<StringRef>>> zones;
						for (auto& worker : testWorkers) {
							if (!zones.count(worker.interf.locality.zoneId())) {
								field_count[worker.interf.locality.get(pa1->attributeKey())]++;
								zones.insert(worker.interf.locality.zoneId());
							}
						}
						// backup recruitment is not required to use degraded processes that have better fitness
						// so we cannot compare degraded between the two methods
						testFitness.degraded = fitness.degraded;

						int minField = 100;

						for (auto& f : field_count) {
							minField = std::min(minField, f.second);
						}

						if (fitness > testFitness && minField > 1) {
							for (auto& w : testWorkers) {
								TraceEvent("TestTLogs").detail("Interf", w.interf.address());
							}
							for (auto& w : workers) {
								TraceEvent("RealTLogs").detail("Interf", w.interf.address());
							}
							TraceEvent("FitnessCompare")
							    .detail("TestF", testFitness.toString())
							    .detail("RealF", fitness.toString());
							ASSERT(false);
						}
					} catch (Error& e) {
						ASSERT(false); // Simulation only validation should not throw errors
					}
				}

				return workers;
			}
		} else if (pa1->attributeKey() == "zoneid" && embedded->name() == "One") {
			ASSERT(pa1->getCount() == required);
			useSimple = true;
		}
	} else if (policy->name() == "One") {
		useSimple = true;
	}
	if (useSimple) {
		std::map<Optional<Standalone<StringRef>>, int> testUsed = id_used;

		auto workers =
		    getWorkersForTlogsSimple(conf, required, desired, id_used, checkStable, dcIds, exclusionWorkerIds);

		if (g_network->isSimulated()) {
			try {
				auto testWorkers = getWorkersForTlogsBackup(
				    conf, required, desired, policy, testUsed, checkStable, dcIds, exclusionWorkerIds);
				RoleFitness testFitness(testWorkers, ProcessClass::TLog, testUsed);
				RoleFitness fitness(workers, ProcessClass::TLog, id_used);
				// backup recruitment is not required to use degraded processes that have better fitness
				// so we cannot compare degraded between the two methods
				testFitness.degraded = fitness.degraded;

				if (fitness > testFitness) {
					for (auto& w : testWorkers) {
						TraceEvent("TestTLogs").detail("Interf", w.interf.address());
					}
					for (auto& w : workers) {
						TraceEvent("RealTLogs").detail("Interf", w.interf.address());
					}
					TraceEvent("FitnessCompare")
					    .detail("TestF", testFitness.toString())
					    .detail("RealF", fitness.toString());
					ASSERT(false);
				}
			} catch (Error& e) {
				ASSERT(false); // Simulation only validation should not throw errors
			}
		}
		return workers;
	}
	TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "PolicyEngineNotOptimized");
	return getWorkersForTlogsBackup(conf, required, desired, policy, id_used, checkStable, dcIds, exclusionWorkerIds);
}

std::vector<WorkerDetails> ClusterController::getWorkersForSatelliteLogs(
    const DatabaseConfiguration& conf,
    const RegionInfo& region,
    const RegionInfo& remoteRegion,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    bool& satelliteFallback,
    bool checkStable) {
	int startDC = 0;
	loop {
		if (startDC > 0 && startDC >= region.satellites.size() + 1 -
		                                  (satelliteFallback ? region.satelliteTLogUsableDcsFallback
		                                                     : region.satelliteTLogUsableDcs)) {
			if (satelliteFallback || region.satelliteTLogUsableDcsFallback == 0) {
				throw no_more_servers();
			} else {
				if (!goodRecruitmentTime.isReady()) {
					throw operation_failed();
				}
				satelliteFallback = true;
				startDC = 0;
			}
		}

		try {
			bool remoteDCUsedAsSatellite = false;
			std::set<Optional<Key>> satelliteDCs;
			int32_t desiredSatelliteTLogs = 0;
			for (int s = startDC; s < std::min<int>(startDC + (satelliteFallback ? region.satelliteTLogUsableDcsFallback
			                                                                     : region.satelliteTLogUsableDcs),
			                                        region.satellites.size());
			     s++) {
				satelliteDCs.insert(region.satellites[s].dcId);
				if (region.satellites[s].satelliteDesiredTLogCount == -1 || desiredSatelliteTLogs == -1) {
					desiredSatelliteTLogs = -1;
				} else {
					desiredSatelliteTLogs += region.satellites[s].satelliteDesiredTLogCount;
				}
				if (region.satellites[s].dcId == remoteRegion.dcId) {
					remoteDCUsedAsSatellite = true;
				}
			}
			std::vector<UID> exclusionWorkerIds;
			// FIXME: If remote DC is used as satellite then this logic only ensures that required number of remote
			// TLogs can be recruited. It does not balance the number of desired TLogs across the satellite and
			// remote sides.
			if (remoteDCUsedAsSatellite) {
				std::map<Optional<Standalone<StringRef>>, int> tmpIdUsed;
				auto remoteLogs = getWorkersForTlogs(conf,
				                                     conf.getRemoteTLogReplicationFactor(),
				                                     conf.getRemoteTLogReplicationFactor(),
				                                     conf.getRemoteTLogPolicy(),
				                                     tmpIdUsed,
				                                     false,
				                                     { remoteRegion.dcId },
				                                     {});
				std::transform(remoteLogs.begin(),
				               remoteLogs.end(),
				               std::back_inserter(exclusionWorkerIds),
				               [](const WorkerDetails& in) { return in.interf.id(); });
			}
			if (satelliteFallback) {
				return getWorkersForTlogs(conf,
				                          region.satelliteTLogReplicationFactorFallback,
				                          desiredSatelliteTLogs > 0 ? desiredSatelliteTLogs
				                                                    : conf.getDesiredSatelliteLogs(region.dcId) *
				                                                          region.satelliteTLogUsableDcsFallback /
				                                                          region.satelliteTLogUsableDcs,
				                          region.satelliteTLogPolicyFallback,
				                          id_used,
				                          checkStable,
				                          satelliteDCs,
				                          exclusionWorkerIds);
			} else {
				return getWorkersForTlogs(conf,
				                          region.satelliteTLogReplicationFactor,
				                          desiredSatelliteTLogs > 0 ? desiredSatelliteTLogs
				                                                    : conf.getDesiredSatelliteLogs(region.dcId),
				                          region.satelliteTLogPolicy,
				                          id_used,
				                          checkStable,
				                          satelliteDCs,
				                          exclusionWorkerIds);
			}
		} catch (Error& e) {
			if (e.code() != error_code_no_more_servers) {
				throw;
			}
		}

		startDC++;
	}
}

ProcessClass::Fitness ClusterController::getBestFitnessForRoleInDatacenter(ProcessClass::ClusterRole role) {
	ProcessClass::Fitness bestFitness = ProcessClass::NeverAssign;
	for (const auto& it : id_worker) {
		if (it.second.priorityInfo.isExcluded || it.second.details.interf.locality.dcId() != clusterControllerDcId) {
			continue;
		}
		bestFitness = std::min(bestFitness, it.second.details.processClass.machineClassFitness(role));
	}
	return bestFitness;
}

WorkerFitnessInfo ClusterController::getWorkerForRoleInDatacenter(
    Optional<Standalone<StringRef>> const& dcId,
    ProcessClass::ClusterRole role,
    ProcessClass::Fitness unacceptableFitness,
    DatabaseConfiguration const& conf,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    std::map<Optional<Standalone<StringRef>>, int> preferredSharing,
    bool checkStable) {
	std::map<std::tuple<ProcessClass::Fitness, int, bool, int>, std::vector<WorkerDetails>> fitness_workers;

	for (auto& it : id_worker) {
		auto fitness = it.second.details.processClass.machineClassFitness(role);
		if (conf.isExcludedServer(it.second.details.interf.addresses()) ||
		    isExcludedDegradedServer(it.second.details.interf.addresses())) {
			fitness = std::max(fitness, ProcessClass::ExcludeFit);
		}
		if (workerAvailable(it.second, checkStable) && fitness < unacceptableFitness &&
		    it.second.details.interf.locality.dcId() == dcId) {
			auto sharing = preferredSharing.find(it.first);
			fitness_workers[std::make_tuple(fitness,
			                                id_used[it.first],
			                                isLongLivedStateless(it.first),
			                                sharing != preferredSharing.end() ? sharing->second : 1e6)]
			    .push_back(it.second.details);
		}
	}

	if (fitness_workers.size()) {
		auto worker = deterministicRandom()->randomChoice(fitness_workers.begin()->second);
		id_used[worker.interf.locality.processId()]++;
		return WorkerFitnessInfo(worker,
		                         std::max(ProcessClass::GoodFit, std::get<0>(fitness_workers.begin()->first)),
		                         std::get<1>(fitness_workers.begin()->first));
	}

	throw no_more_servers();
}

std::vector<WorkerDetails> ClusterController::getWorkersForRoleInDatacenter(
    Optional<Standalone<StringRef>> const& dcId,
    ProcessClass::ClusterRole role,
    int amount,
    DatabaseConfiguration const& conf,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    std::map<Optional<Standalone<StringRef>>, int> preferredSharing,
    Optional<WorkerFitnessInfo> minWorker,
    bool checkStable) {
	std::map<std::tuple<ProcessClass::Fitness, int, bool, int>, std::vector<WorkerDetails>> fitness_workers;
	std::vector<WorkerDetails> results;
	if (minWorker.present()) {
		results.push_back(minWorker.get().worker);
	}
	if (amount <= results.size()) {
		return results;
	}

	for (auto& it : id_worker) {
		auto fitness = it.second.details.processClass.machineClassFitness(role);
		if (workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.details.interf.addresses()) &&
		    !isExcludedDegradedServer(it.second.details.interf.addresses()) &&
		    it.second.details.interf.locality.dcId() == dcId &&
		    (!minWorker.present() ||
		     (it.second.details.interf.id() != minWorker.get().worker.interf.id() &&
		      (fitness < minWorker.get().fitness ||
		       (fitness == minWorker.get().fitness && id_used[it.first] <= minWorker.get().used))))) {
			auto sharing = preferredSharing.find(it.first);
			fitness_workers[std::make_tuple(fitness,
			                                id_used[it.first],
			                                isLongLivedStateless(it.first),
			                                sharing != preferredSharing.end() ? sharing->second : 1e6)]
			    .push_back(it.second.details);
		}
	}

	for (auto& it : fitness_workers) {
		deterministicRandom()->randomShuffle(it.second);
		for (int i = 0; i < it.second.size(); i++) {
			results.push_back(it.second[i]);
			id_used[it.second[i].interf.locality.processId()]++;
			if (results.size() == amount)
				return results;
		}
	}

	return results;
}

std::set<Optional<Standalone<StringRef>>> ClusterController::getDatacenters(DatabaseConfiguration const& conf,
                                                                            bool checkStable) {
	std::set<Optional<Standalone<StringRef>>> result;
	for (auto& it : id_worker)
		if (workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.details.interf.addresses()) &&
		    !isExcludedDegradedServer(it.second.details.interf.addresses()))
			result.insert(it.second.details.interf.locality.dcId());
	return result;
}

void ClusterController::updateKnownIds(std::map<Optional<Standalone<StringRef>>, int>* id_used) {
	(*id_used)[masterProcessId]++;
	(*id_used)[clusterControllerProcessId]++;
}

RecruitRemoteFromConfigurationReply ClusterController::findRemoteWorkersForConfiguration(
    RecruitRemoteFromConfigurationRequest const& req) {
	RecruitRemoteFromConfigurationReply result;
	std::map<Optional<Standalone<StringRef>>, int> id_used;

	updateKnownIds(&id_used);

	if (req.dbgId.present()) {
		TraceEvent(SevDebug, "FindRemoteWorkersForConf", req.dbgId.get())
		    .detail("RemoteDcId", req.dcId)
		    .detail("Configuration", req.configuration.toString())
		    .detail("Policy", req.configuration.getRemoteTLogPolicy()->name());
	}

	std::set<Optional<Key>> remoteDC;
	remoteDC.insert(req.dcId);

	auto remoteLogs = getWorkersForTlogs(req.configuration,
	                                     req.configuration.getRemoteTLogReplicationFactor(),
	                                     req.configuration.getDesiredRemoteLogs(),
	                                     req.configuration.getRemoteTLogPolicy(),
	                                     id_used,
	                                     false,
	                                     remoteDC,
	                                     req.exclusionWorkerIds);
	for (int i = 0; i < remoteLogs.size(); i++) {
		result.remoteTLogs.push_back(remoteLogs[i].interf);
	}

	auto logRouters = getWorkersForRoleInDatacenter(
	    req.dcId, ProcessClass::LogRouter, req.logRouterCount, req.configuration, id_used);
	for (int i = 0; i < logRouters.size(); i++) {
		result.logRouters.push_back(logRouters[i].interf);
	}

	if (!goodRemoteRecruitmentTime.isReady() &&
	    ((RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredRemoteLogs(), ProcessClass::TLog)
	          .betterCount(RoleFitness(remoteLogs, ProcessClass::TLog, id_used))) ||
	     (RoleFitness(SERVER_KNOBS->EXPECTED_LOG_ROUTER_FITNESS, req.logRouterCount, ProcessClass::LogRouter)
	          .betterCount(RoleFitness(logRouters, ProcessClass::LogRouter, id_used))))) {
		throw operation_failed();
	}

	if (req.dbgId.present()) {
		TraceEvent(SevDebug, "FindRemoteWorkersForConf_ReturnResult", req.dbgId.get())
		    .detail("RemoteDcId", req.dcId)
		    .detail("ResultRemoteLogs", result.remoteTLogs.size());
		result.dbgId = req.dbgId;
	}

	return result;
}

// Given datacenter ID, returns the primary and remote regions.
std::pair<RegionInfo, RegionInfo> ClusterController::getPrimaryAndRemoteRegion(const std::vector<RegionInfo>& regions,
                                                                               Key dcId) {
	RegionInfo region;
	RegionInfo remoteRegion;
	for (const auto& r : regions) {
		if (r.dcId == dcId) {
			region = r;
		} else {
			remoteRegion = r;
		}
	}
	return std::make_pair(region, remoteRegion);
}

ErrorOr<RecruitFromConfigurationReply> ClusterController::findWorkersForConfigurationFromDC(
    RecruitFromConfigurationRequest const& req,
    Optional<Key> dcId,
    bool checkGoodRecruitment) {
	RecruitFromConfigurationReply result;
	std::map<Optional<Standalone<StringRef>>, int> id_used;
	updateKnownIds(&id_used);

	ASSERT(dcId.present());

	std::set<Optional<Key>> primaryDC;
	primaryDC.insert(dcId);
	result.dcId = dcId;

	auto [region, remoteRegion] = getPrimaryAndRemoteRegion(req.configuration.regions, dcId.get());

	if (req.recruitSeedServers) {
		auto primaryStorageServers = getWorkersForSeedServers(req.configuration, req.configuration.storagePolicy, dcId);
		for (int i = 0; i < primaryStorageServers.size(); i++) {
			result.storageServers.push_back(primaryStorageServers[i].interf);
		}
	}

	auto tlogs = getWorkersForTlogs(req.configuration,
	                                req.configuration.tLogReplicationFactor,
	                                req.configuration.getDesiredLogs(),
	                                req.configuration.tLogPolicy,
	                                id_used,
	                                false,
	                                primaryDC);
	for (int i = 0; i < tlogs.size(); i++) {
		result.tLogs.push_back(tlogs[i].interf);
	}

	std::vector<WorkerDetails> satelliteLogs;
	if (region.satelliteTLogReplicationFactor > 0 && req.configuration.usableRegions > 1) {
		satelliteLogs =
		    getWorkersForSatelliteLogs(req.configuration, region, remoteRegion, id_used, result.satelliteFallback);
		for (int i = 0; i < satelliteLogs.size(); i++) {
			result.satelliteTLogs.push_back(satelliteLogs[i].interf);
		}
	}

	std::map<Optional<Standalone<StringRef>>, int> preferredSharing;
	auto first_commit_proxy = getWorkerForRoleInDatacenter(
	    dcId, ProcessClass::CommitProxy, ProcessClass::ExcludeFit, req.configuration, id_used, preferredSharing);
	preferredSharing[first_commit_proxy.worker.interf.locality.processId()] = 0;
	auto first_grv_proxy = getWorkerForRoleInDatacenter(
	    dcId, ProcessClass::GrvProxy, ProcessClass::ExcludeFit, req.configuration, id_used, preferredSharing);
	preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
	auto first_resolver = getWorkerForRoleInDatacenter(
	    dcId, ProcessClass::Resolver, ProcessClass::ExcludeFit, req.configuration, id_used, preferredSharing);
	preferredSharing[first_resolver.worker.interf.locality.processId()] = 2;

	// If one of the first process recruitments is forced to share a process, allow all of next recruitments
	// to also share a process.
	auto maxUsed = std::max({ first_commit_proxy.used, first_grv_proxy.used, first_resolver.used });
	first_commit_proxy.used = maxUsed;
	first_grv_proxy.used = maxUsed;
	first_resolver.used = maxUsed;

	auto commit_proxies = getWorkersForRoleInDatacenter(dcId,
	                                                    ProcessClass::CommitProxy,
	                                                    req.configuration.getDesiredCommitProxies(),
	                                                    req.configuration,
	                                                    id_used,
	                                                    preferredSharing,
	                                                    first_commit_proxy);
	auto grv_proxies = getWorkersForRoleInDatacenter(dcId,
	                                                 ProcessClass::GrvProxy,
	                                                 req.configuration.getDesiredGrvProxies(),
	                                                 req.configuration,
	                                                 id_used,
	                                                 preferredSharing,
	                                                 first_grv_proxy);
	auto resolvers = getWorkersForRoleInDatacenter(dcId,
	                                               ProcessClass::Resolver,
	                                               req.configuration.getDesiredResolvers(),
	                                               req.configuration,
	                                               id_used,
	                                               preferredSharing,
	                                               first_resolver);
	for (int i = 0; i < commit_proxies.size(); i++)
		result.commitProxies.push_back(commit_proxies[i].interf);
	for (int i = 0; i < grv_proxies.size(); i++)
		result.grvProxies.push_back(grv_proxies[i].interf);
	for (int i = 0; i < resolvers.size(); i++)
		result.resolvers.push_back(resolvers[i].interf);

	if (req.maxOldLogRouters > 0) {
		if (tlogs.size() == 1) {
			result.oldLogRouters.push_back(tlogs[0].interf);
		} else {
			for (int i = 0; i < tlogs.size(); i++) {
				if (tlogs[i].interf.locality.processId() != clusterControllerProcessId) {
					result.oldLogRouters.push_back(tlogs[i].interf);
				}
			}
		}
	}

	if (req.configuration.backupWorkerEnabled) {
		const int nBackup = std::max<int>(
		    (req.configuration.desiredLogRouterCount > 0 ? req.configuration.desiredLogRouterCount : tlogs.size()),
		    req.maxOldLogRouters);
		auto backupWorkers =
		    getWorkersForRoleInDatacenter(dcId, ProcessClass::Backup, nBackup, req.configuration, id_used);
		std::transform(backupWorkers.begin(),
		               backupWorkers.end(),
		               std::back_inserter(result.backupWorkers),
		               [](const WorkerDetails& w) { return w.interf; });
	}

	if (!goodRecruitmentTime.isReady() && checkGoodRecruitment &&
	    (RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(), ProcessClass::TLog)
	         .betterCount(RoleFitness(tlogs, ProcessClass::TLog, id_used)) ||
	     (region.satelliteTLogReplicationFactor > 0 && req.configuration.usableRegions > 1 &&
	      RoleFitness(
	          SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredSatelliteLogs(dcId), ProcessClass::TLog)
	          .betterCount(RoleFitness(satelliteLogs, ProcessClass::TLog, id_used))) ||
	     RoleFitness(SERVER_KNOBS->EXPECTED_COMMIT_PROXY_FITNESS,
	                 req.configuration.getDesiredCommitProxies(),
	                 ProcessClass::CommitProxy)
	         .betterCount(RoleFitness(commit_proxies, ProcessClass::CommitProxy, id_used)) ||
	     RoleFitness(
	         SERVER_KNOBS->EXPECTED_GRV_PROXY_FITNESS, req.configuration.getDesiredGrvProxies(), ProcessClass::GrvProxy)
	         .betterCount(RoleFitness(grv_proxies, ProcessClass::GrvProxy, id_used)) ||
	     RoleFitness(
	         SERVER_KNOBS->EXPECTED_RESOLVER_FITNESS, req.configuration.getDesiredResolvers(), ProcessClass::Resolver)
	         .betterCount(RoleFitness(resolvers, ProcessClass::Resolver, id_used)))) {
		return operation_failed();
	}

	return result;
}

RecruitFromConfigurationReply ClusterController::findWorkersForConfigurationDispatch(
    RecruitFromConfigurationRequest const& req,
    bool checkGoodRecruitment) {
	if (req.configuration.regions.size() > 1) {
		std::vector<RegionInfo> regions = req.configuration.regions;
		if (regions[0].priority == regions[1].priority && regions[1].dcId == clusterControllerDcId.get()) {
			TraceEvent("CCSwitchPrimaryDc", id)
			    .detail("CCDcId", clusterControllerDcId.get())
			    .detail("OldPrimaryDcId", regions[0].dcId)
			    .detail("NewPrimaryDcId", regions[1].dcId);
			std::swap(regions[0], regions[1]);
		}

		if (regions[1].dcId == clusterControllerDcId.get() &&
		    (!versionDifferenceUpdated || datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE)) {
			if (regions[1].priority >= 0) {
				TraceEvent("CCSwitchPrimaryDcVersionDifference", id)
				    .detail("CCDcId", clusterControllerDcId.get())
				    .detail("OldPrimaryDcId", regions[0].dcId)
				    .detail("NewPrimaryDcId", regions[1].dcId);
				std::swap(regions[0], regions[1]);
			} else {
				TraceEvent(SevWarnAlways, "CCDcPriorityNegative")
				    .detail("DcId", regions[1].dcId)
				    .detail("Priority", regions[1].priority)
				    .detail("FindWorkersInDc", regions[0].dcId)
				    .detail("Warning", "Failover did not happen but CC is in remote DC");
			}
		}

		TraceEvent("CCFindWorkersForConfiguration", id)
		    .detail("CCDcId", clusterControllerDcId.get())
		    .detail("Region0DcId", regions[0].dcId)
		    .detail("Region1DcId", regions[1].dcId)
		    .detail("DatacenterVersionDifference", datacenterVersionDifference)
		    .detail("VersionDifferenceUpdated", versionDifferenceUpdated);

		bool setPrimaryDesired = false;
		try {
			auto reply = findWorkersForConfigurationFromDC(req, regions[0].dcId, checkGoodRecruitment);
			setPrimaryDesired = true;
			std::vector<Optional<Key>> dcPriority;
			dcPriority.push_back(regions[0].dcId);
			dcPriority.push_back(regions[1].dcId);
			desiredDcIds.set(dcPriority);
			if (reply.isError()) {
				throw reply.getError();
			} else if (regions[0].dcId == clusterControllerDcId.get()) {
				return reply.get();
			}
			TraceEvent(SevWarn, "CCRecruitmentFailed", id)
			    .detail("Reason", "Recruited Txn system and CC are in different DCs")
			    .detail("CCDcId", clusterControllerDcId.get())
			    .detail("RecruitedTxnSystemDcId", regions[0].dcId);
			throw no_more_servers();
		} catch (Error& e) {
			if (!goodRemoteRecruitmentTime.isReady() && regions[1].dcId != clusterControllerDcId.get() &&
			    checkGoodRecruitment) {
				throw operation_failed();
			}

			if (e.code() != error_code_no_more_servers || regions[1].priority < 0) {
				throw;
			}
			TraceEvent(SevWarn, "AttemptingRecruitmentInRemoteDc", id)
			    .error(e)
			    .detail("SetPrimaryDesired", setPrimaryDesired);
			auto reply = findWorkersForConfigurationFromDC(req, regions[1].dcId, checkGoodRecruitment);
			if (!setPrimaryDesired) {
				std::vector<Optional<Key>> dcPriority;
				dcPriority.push_back(regions[1].dcId);
				dcPriority.push_back(regions[0].dcId);
				desiredDcIds.set(dcPriority);
			}
			if (reply.isError()) {
				throw reply.getError();
			} else if (regions[1].dcId == clusterControllerDcId.get()) {
				return reply.get();
			}
			throw;
		}
	} else if (req.configuration.regions.size() == 1) {
		std::vector<Optional<Key>> dcPriority;
		dcPriority.push_back(req.configuration.regions[0].dcId);
		desiredDcIds.set(dcPriority);
		auto reply = findWorkersForConfigurationFromDC(req, req.configuration.regions[0].dcId, checkGoodRecruitment);
		if (reply.isError()) {
			throw reply.getError();
		} else if (req.configuration.regions[0].dcId == clusterControllerDcId.get()) {
			return reply.get();
		}
		throw no_more_servers();
	} else {
		RecruitFromConfigurationReply result;
		std::map<Optional<Standalone<StringRef>>, int> id_used;
		updateKnownIds(&id_used);
		auto tlogs = getWorkersForTlogs(req.configuration,
		                                req.configuration.tLogReplicationFactor,
		                                req.configuration.getDesiredLogs(),
		                                req.configuration.tLogPolicy,
		                                id_used);
		for (int i = 0; i < tlogs.size(); i++) {
			result.tLogs.push_back(tlogs[i].interf);
		}

		if (req.maxOldLogRouters > 0) {
			if (tlogs.size() == 1) {
				result.oldLogRouters.push_back(tlogs[0].interf);
			} else {
				for (int i = 0; i < tlogs.size(); i++) {
					if (tlogs[i].interf.locality.processId() != clusterControllerProcessId) {
						result.oldLogRouters.push_back(tlogs[i].interf);
					}
				}
			}
		}

		if (req.recruitSeedServers) {
			auto primaryStorageServers = getWorkersForSeedServers(req.configuration, req.configuration.storagePolicy);
			for (int i = 0; i < primaryStorageServers.size(); i++)
				result.storageServers.push_back(primaryStorageServers[i].interf);
		}

		auto datacenters = getDatacenters(req.configuration);

		std::tuple<RoleFitness, RoleFitness, RoleFitness> bestFitness;
		int numEquivalent = 1;
		Optional<Key> bestDC;

		for (auto dcId : datacenters) {
			try {
				// SOMEDAY: recruitment in other DCs besides the clusterControllerDcID will not account for the
				// processes used by the master and cluster controller properly.
				auto used = id_used;
				std::map<Optional<Standalone<StringRef>>, int> preferredSharing;
				auto first_commit_proxy = getWorkerForRoleInDatacenter(dcId,
				                                                       ProcessClass::CommitProxy,
				                                                       ProcessClass::ExcludeFit,
				                                                       req.configuration,
				                                                       used,
				                                                       preferredSharing);
				preferredSharing[first_commit_proxy.worker.interf.locality.processId()] = 0;
				auto first_grv_proxy = getWorkerForRoleInDatacenter(
				    dcId, ProcessClass::GrvProxy, ProcessClass::ExcludeFit, req.configuration, used, preferredSharing);
				preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
				auto first_resolver = getWorkerForRoleInDatacenter(
				    dcId, ProcessClass::Resolver, ProcessClass::ExcludeFit, req.configuration, used, preferredSharing);
				preferredSharing[first_resolver.worker.interf.locality.processId()] = 2;

				// If one of the first process recruitments is forced to share a process, allow all of next
				// recruitments to also share a process.
				auto maxUsed = std::max({ first_commit_proxy.used, first_grv_proxy.used, first_resolver.used });
				first_commit_proxy.used = maxUsed;
				first_grv_proxy.used = maxUsed;
				first_resolver.used = maxUsed;

				auto commit_proxies = getWorkersForRoleInDatacenter(dcId,
				                                                    ProcessClass::CommitProxy,
				                                                    req.configuration.getDesiredCommitProxies(),
				                                                    req.configuration,
				                                                    used,
				                                                    preferredSharing,
				                                                    first_commit_proxy);

				auto grv_proxies = getWorkersForRoleInDatacenter(dcId,
				                                                 ProcessClass::GrvProxy,
				                                                 req.configuration.getDesiredGrvProxies(),
				                                                 req.configuration,
				                                                 used,
				                                                 preferredSharing,
				                                                 first_grv_proxy);

				auto resolvers = getWorkersForRoleInDatacenter(dcId,
				                                               ProcessClass::Resolver,
				                                               req.configuration.getDesiredResolvers(),
				                                               req.configuration,
				                                               used,
				                                               preferredSharing,
				                                               first_resolver);

				auto fitness = std::make_tuple(RoleFitness(commit_proxies, ProcessClass::CommitProxy, used),
				                               RoleFitness(grv_proxies, ProcessClass::GrvProxy, used),
				                               RoleFitness(resolvers, ProcessClass::Resolver, used));

				if (dcId == clusterControllerDcId) {
					bestFitness = fitness;
					bestDC = dcId;
					for (int i = 0; i < resolvers.size(); i++) {
						result.resolvers.push_back(resolvers[i].interf);
					}
					for (int i = 0; i < commit_proxies.size(); i++) {
						result.commitProxies.push_back(commit_proxies[i].interf);
					}
					for (int i = 0; i < grv_proxies.size(); i++) {
						result.grvProxies.push_back(grv_proxies[i].interf);
					}

					if (req.configuration.backupWorkerEnabled) {
						const int nBackup = std::max<int>(tlogs.size(), req.maxOldLogRouters);
						auto backupWorkers =
						    getWorkersForRoleInDatacenter(dcId, ProcessClass::Backup, nBackup, req.configuration, used);
						std::transform(backupWorkers.begin(),
						               backupWorkers.end(),
						               std::back_inserter(result.backupWorkers),
						               [](const WorkerDetails& w) { return w.interf; });
					}

					break;
				} else {
					if (fitness < bestFitness) {
						bestFitness = fitness;
						numEquivalent = 1;
						bestDC = dcId;
					} else if (fitness == bestFitness && deterministicRandom()->random01() < 1.0 / ++numEquivalent) {
						bestDC = dcId;
					}
				}
			} catch (Error& e) {
				if (e.code() != error_code_no_more_servers) {
					throw;
				}
			}
		}

		if (bestDC != clusterControllerDcId) {
			TraceEvent("BestDCIsNotClusterDC").log();
			std::vector<Optional<Key>> dcPriority;
			dcPriority.push_back(bestDC);
			desiredDcIds.set(dcPriority);
			throw no_more_servers();
		}
		// If this cluster controller dies, do not prioritize recruiting the next one in the same DC
		desiredDcIds.set(std::vector<Optional<Key>>());
		TraceEvent("FindWorkersForConfig")
		    .detail("Replication", req.configuration.tLogReplicationFactor)
		    .detail("DesiredLogs", req.configuration.getDesiredLogs())
		    .detail("ActualLogs", result.tLogs.size())
		    .detail("DesiredCommitProxies", req.configuration.getDesiredCommitProxies())
		    .detail("ActualCommitProxies", result.commitProxies.size())
		    .detail("DesiredGrvProxies", req.configuration.getDesiredGrvProxies())
		    .detail("ActualGrvProxies", result.grvProxies.size())
		    .detail("DesiredResolvers", req.configuration.getDesiredResolvers())
		    .detail("ActualResolvers", result.resolvers.size());

		if (!goodRecruitmentTime.isReady() && checkGoodRecruitment &&
		    (RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(), ProcessClass::TLog)
		         .betterCount(RoleFitness(tlogs, ProcessClass::TLog, id_used)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_COMMIT_PROXY_FITNESS,
		                 req.configuration.getDesiredCommitProxies(),
		                 ProcessClass::CommitProxy)
		         .betterCount(std::get<0>(bestFitness)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_GRV_PROXY_FITNESS,
		                 req.configuration.getDesiredGrvProxies(),
		                 ProcessClass::GrvProxy)
		         .betterCount(std::get<1>(bestFitness)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_RESOLVER_FITNESS,
		                 req.configuration.getDesiredResolvers(),
		                 ProcessClass::Resolver)
		         .betterCount(std::get<2>(bestFitness)))) {
			throw operation_failed();
		}

		return result;
	}
}

void ClusterController::updateIdUsed(const std::vector<WorkerInterface>& workers,
                                     std::map<Optional<Standalone<StringRef>>, int>& id_used) {
	for (auto& it : workers) {
		id_used[it.locality.processId()]++;
	}
}

void ClusterController::compareWorkers(const DatabaseConfiguration& conf,
                                       const std::vector<WorkerInterface>& first,
                                       const std::map<Optional<Standalone<StringRef>>, int>& firstUsed,
                                       const std::vector<WorkerInterface>& second,
                                       const std::map<Optional<Standalone<StringRef>>, int>& secondUsed,
                                       ProcessClass::ClusterRole role,
                                       std::string description) {
	std::vector<WorkerDetails> firstDetails;
	for (auto& worker : first) {
		auto w = id_worker.find(worker.locality.processId());
		ASSERT(w != id_worker.end());
		auto const& [_, workerInfo] = *w;
		ASSERT(!conf.isExcludedServer(workerInfo.details.interf.addresses()));
		firstDetails.push_back(workerInfo.details);
		//TraceEvent("CompareAddressesFirst").detail(description.c_str(), w->second.details.interf.address());
	}
	RoleFitness firstFitness(firstDetails, role, firstUsed);

	std::vector<WorkerDetails> secondDetails;
	for (auto& worker : second) {
		auto w = id_worker.find(worker.locality.processId());
		ASSERT(w != id_worker.end());
		auto const& [_, workerInfo] = *w;
		ASSERT(!conf.isExcludedServer(workerInfo.details.interf.addresses()));
		secondDetails.push_back(workerInfo.details);
		//TraceEvent("CompareAddressesSecond").detail(description.c_str(), w->second.details.interf.address());
	}
	RoleFitness secondFitness(secondDetails, role, secondUsed);

	if (!(firstFitness == secondFitness)) {
		TraceEvent(SevError, "NonDeterministicRecruitment")
		    .detail("FirstFitness", firstFitness.toString())
		    .detail("SecondFitness", secondFitness.toString())
		    .detail("ClusterRole", role);
	}
}

RecruitFromConfigurationReply ClusterController::findWorkersForConfiguration(
    RecruitFromConfigurationRequest const& req) {
	RecruitFromConfigurationReply rep = findWorkersForConfigurationDispatch(req, true);
	if (g_network->isSimulated()) {
		try {
			// FIXME: The logic to pick a satellite in a remote region is not
			// deterministic and can therefore break this nondeterminism check.
			// Since satellites will generally be in the primary region,
			// disable the determinism check for remote region satellites.
			bool remoteDCUsedAsSatellite = false;
			if (req.configuration.regions.size() > 1) {
				auto [region, remoteRegion] =
				    getPrimaryAndRemoteRegion(req.configuration.regions, req.configuration.regions[0].dcId);
				for (const auto& satellite : region.satellites) {
					if (satellite.dcId == remoteRegion.dcId) {
						remoteDCUsedAsSatellite = true;
					}
				}
			}
			if (!remoteDCUsedAsSatellite) {
				RecruitFromConfigurationReply compare = findWorkersForConfigurationDispatch(req, false);

				std::map<Optional<Standalone<StringRef>>, int> firstUsed;
				std::map<Optional<Standalone<StringRef>>, int> secondUsed;
				updateKnownIds(&firstUsed);
				updateKnownIds(&secondUsed);

				// auto mworker = id_worker.find(masterProcessId);
				//TraceEvent("CompareAddressesMaster")
				//    .detail("Master",
				//            mworker != id_worker.end() ? mworker->second.details.interf.address() :
				//            NetworkAddress());

				updateIdUsed(rep.tLogs, firstUsed);
				updateIdUsed(compare.tLogs, secondUsed);
				compareWorkers(
				    req.configuration, rep.tLogs, firstUsed, compare.tLogs, secondUsed, ProcessClass::TLog, "TLog");
				updateIdUsed(rep.satelliteTLogs, firstUsed);
				updateIdUsed(compare.satelliteTLogs, secondUsed);
				compareWorkers(req.configuration,
				               rep.satelliteTLogs,
				               firstUsed,
				               compare.satelliteTLogs,
				               secondUsed,
				               ProcessClass::TLog,
				               "Satellite");
				updateIdUsed(rep.commitProxies, firstUsed);
				updateIdUsed(compare.commitProxies, secondUsed);
				updateIdUsed(rep.grvProxies, firstUsed);
				updateIdUsed(compare.grvProxies, secondUsed);
				updateIdUsed(rep.resolvers, firstUsed);
				updateIdUsed(compare.resolvers, secondUsed);
				compareWorkers(req.configuration,
				               rep.commitProxies,
				               firstUsed,
				               compare.commitProxies,
				               secondUsed,
				               ProcessClass::CommitProxy,
				               "CommitProxy");
				compareWorkers(req.configuration,
				               rep.grvProxies,
				               firstUsed,
				               compare.grvProxies,
				               secondUsed,
				               ProcessClass::GrvProxy,
				               "GrvProxy");
				compareWorkers(req.configuration,
				               rep.resolvers,
				               firstUsed,
				               compare.resolvers,
				               secondUsed,
				               ProcessClass::Resolver,
				               "Resolver");
				updateIdUsed(rep.backupWorkers, firstUsed);
				updateIdUsed(compare.backupWorkers, secondUsed);
				compareWorkers(req.configuration,
				               rep.backupWorkers,
				               firstUsed,
				               compare.backupWorkers,
				               secondUsed,
				               ProcessClass::Backup,
				               "Backup");
			}
		} catch (Error& e) {
			ASSERT(false); // Simulation only validation should not throw errors
		}
	}
	return rep;
}

// Check if txn system is recruited successfully in each region
void ClusterController::checkRegions(const std::vector<RegionInfo>& regions) {
	if (desiredDcIds.get().present() && desiredDcIds.get().get().size() == 2 &&
	    desiredDcIds.get().get()[0].get() == regions[0].dcId && desiredDcIds.get().get()[1].get() == regions[1].dcId) {
		return;
	}

	try {
		std::map<Optional<Standalone<StringRef>>, int> id_used;
		getWorkerForRoleInDatacenter(
		    regions[0].dcId, ProcessClass::ClusterController, ProcessClass::ExcludeFit, db.config, id_used, {}, true);
		getWorkerForRoleInDatacenter(
		    regions[0].dcId, ProcessClass::Master, ProcessClass::ExcludeFit, db.config, id_used, {}, true);

		std::set<Optional<Key>> primaryDC;
		primaryDC.insert(regions[0].dcId);
		getWorkersForTlogs(db.config,
		                   db.config.tLogReplicationFactor,
		                   db.config.getDesiredLogs(),
		                   db.config.tLogPolicy,
		                   id_used,
		                   true,
		                   primaryDC);
		if (regions[0].satelliteTLogReplicationFactor > 0 && db.config.usableRegions > 1) {
			bool satelliteFallback = false;
			getWorkersForSatelliteLogs(db.config, regions[0], regions[1], id_used, satelliteFallback, true);
		}

		getWorkerForRoleInDatacenter(
		    regions[0].dcId, ProcessClass::Resolver, ProcessClass::ExcludeFit, db.config, id_used, {}, true);
		getWorkerForRoleInDatacenter(
		    regions[0].dcId, ProcessClass::CommitProxy, ProcessClass::ExcludeFit, db.config, id_used, {}, true);
		getWorkerForRoleInDatacenter(
		    regions[0].dcId, ProcessClass::GrvProxy, ProcessClass::ExcludeFit, db.config, id_used, {}, true);

		std::vector<Optional<Key>> dcPriority;
		dcPriority.push_back(regions[0].dcId);
		dcPriority.push_back(regions[1].dcId);
		desiredDcIds.set(dcPriority);
	} catch (Error& e) {
		if (e.code() != error_code_no_more_servers) {
			throw;
		}
	}
}

void ClusterController::checkRecoveryStalled() {
	if ((db.serverInfo->get().recoveryState == RecoveryState::RECRUITING ||
	     db.serverInfo->get().recoveryState == RecoveryState::ACCEPTING_COMMITS ||
	     db.serverInfo->get().recoveryState == RecoveryState::ALL_LOGS_RECRUITED) &&
	    db.recoveryStalled) {
		if (db.config.regions.size() > 1) {
			auto regions = db.config.regions;
			if (clusterControllerDcId.get() == regions[0].dcId && regions[1].priority >= 0) {
				std::swap(regions[0], regions[1]);
			}
			ASSERT(regions[1].priority < 0 || clusterControllerDcId.get() == regions[1].dcId);
			checkRegions(regions);
		}
	}
}

void ClusterController::updateIdUsed(const std::vector<WorkerDetails>& workers,
                                     std::map<Optional<Standalone<StringRef>>, int>& id_used) {
	for (auto& it : workers) {
		id_used[it.interf.locality.processId()]++;
	}
}

// FIXME: determine when to fail the cluster controller when a primaryDC has not been set

bool ClusterController::betterMasterExists() {
	const ServerDBInfo dbi = db.serverInfo->get();

	if (dbi.recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		return false;
	}

	// Do not trigger better master exists if the cluster controller is excluded, since the master will change
	// anyways once the cluster controller is moved
	if (id_worker[clusterControllerProcessId].priorityInfo.isExcluded) {
		TraceEvent("NewRecruitmentIsWorse", id).detail("Reason", "ClusterControllerExcluded");
		return false;
	}

	if (db.config.regions.size() > 1 && db.config.regions[0].priority > db.config.regions[1].priority &&
	    db.config.regions[0].dcId != clusterControllerDcId.get() && versionDifferenceUpdated &&
	    datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE && remoteDCIsHealthy()) {
		checkRegions(db.config.regions);
	}

	// Get master process
	auto masterWorker = id_worker.find(dbi.master.locality.processId());
	if (masterWorker == id_worker.end()) {
		TraceEvent("NewRecruitmentIsWorse", id)
		    .detail("Reason", "CannotFindMaster")
		    .detail("ProcessID", dbi.master.locality.processId());
		return false;
	}

	// Get tlog processes
	std::vector<WorkerDetails> tlogs;
	std::vector<WorkerDetails> remote_tlogs;
	std::vector<WorkerDetails> satellite_tlogs;
	std::vector<WorkerDetails> log_routers;
	std::set<NetworkAddress> logRouterAddresses;
	std::vector<WorkerDetails> backup_workers;
	std::set<NetworkAddress> backup_addresses;

	for (auto& logSet : dbi.logSystemConfig.tLogs) {
		for (auto& it : logSet.tLogs) {
			auto tlogWorker = id_worker.find(it.interf().filteredLocality.processId());
			if (tlogWorker == id_worker.end()) {
				TraceEvent("NewRecruitmentIsWorse", id)
				    .detail("Reason", "CannotFindTLog")
				    .detail("ProcessID", it.interf().filteredLocality.processId());
				return false;
			}
			if (tlogWorker->second.priorityInfo.isExcluded) {
				TraceEvent("BetterMasterExists", id)
				    .detail("Reason", "TLogExcluded")
				    .detail("ProcessID", it.interf().filteredLocality.processId());
				return true;
			}

			if (logSet.isLocal && logSet.locality == tagLocalitySatellite) {
				satellite_tlogs.push_back(tlogWorker->second.details);
			} else if (logSet.isLocal) {
				tlogs.push_back(tlogWorker->second.details);
			} else {
				remote_tlogs.push_back(tlogWorker->second.details);
			}
		}

		for (auto& it : logSet.logRouters) {
			auto tlogWorker = id_worker.find(it.interf().filteredLocality.processId());
			if (tlogWorker == id_worker.end()) {
				TraceEvent("NewRecruitmentIsWorse", id)
				    .detail("Reason", "CannotFindLogRouter")
				    .detail("ProcessID", it.interf().filteredLocality.processId());
				return false;
			}
			if (tlogWorker->second.priorityInfo.isExcluded) {
				TraceEvent("BetterMasterExists", id)
				    .detail("Reason", "LogRouterExcluded")
				    .detail("ProcessID", it.interf().filteredLocality.processId());
				return true;
			}
			if (!logRouterAddresses.count(tlogWorker->second.details.interf.address())) {
				logRouterAddresses.insert(tlogWorker->second.details.interf.address());
				log_routers.push_back(tlogWorker->second.details);
			}
		}

		for (const auto& worker : logSet.backupWorkers) {
			auto workerIt = id_worker.find(worker.interf().locality.processId());
			if (workerIt == id_worker.end()) {
				TraceEvent("NewRecruitmentIsWorse", id)
				    .detail("Reason", "CannotFindBackupWorker")
				    .detail("ProcessID", worker.interf().locality.processId());
				return false;
			}
			if (workerIt->second.priorityInfo.isExcluded) {
				TraceEvent("BetterMasterExists", id)
				    .detail("Reason", "BackupWorkerExcluded")
				    .detail("ProcessID", worker.interf().locality.processId());
				return true;
			}
			if (backup_addresses.count(workerIt->second.details.interf.address()) == 0) {
				backup_addresses.insert(workerIt->second.details.interf.address());
				backup_workers.push_back(workerIt->second.details);
			}
		}
	}

	// Get commit proxy classes
	std::vector<WorkerDetails> commitProxyClasses;
	for (auto& it : dbi.client.commitProxies) {
		auto commitProxyWorker = id_worker.find(it.processId);
		if (commitProxyWorker == id_worker.end()) {
			TraceEvent("NewRecruitmentIsWorse", id)
			    .detail("Reason", "CannotFindCommitProxy")
			    .detail("ProcessID", it.processId);
			return false;
		}
		if (commitProxyWorker->second.priorityInfo.isExcluded) {
			TraceEvent("BetterMasterExists", id)
			    .detail("Reason", "CommitProxyExcluded")
			    .detail("ProcessID", it.processId);
			return true;
		}
		commitProxyClasses.push_back(commitProxyWorker->second.details);
	}

	// Get grv proxy classes
	std::vector<WorkerDetails> grvProxyClasses;
	for (auto& it : dbi.client.grvProxies) {
		auto grvProxyWorker = id_worker.find(it.processId);
		if (grvProxyWorker == id_worker.end()) {
			TraceEvent("NewRecruitmentIsWorse", id)
			    .detail("Reason", "CannotFindGrvProxy")
			    .detail("ProcessID", it.processId);
			return false;
		}
		if (grvProxyWorker->second.priorityInfo.isExcluded) {
			TraceEvent("BetterMasterExists", id).detail("Reason", "GrvProxyExcluded").detail("ProcessID", it.processId);
			return true;
		}
		grvProxyClasses.push_back(grvProxyWorker->second.details);
	}

	// Get resolver classes
	std::vector<WorkerDetails> resolverClasses;
	for (auto& it : dbi.resolvers) {
		auto resolverWorker = id_worker.find(it.locality.processId());
		if (resolverWorker == id_worker.end()) {
			TraceEvent("NewRecruitmentIsWorse", id)
			    .detail("Reason", "CannotFindResolver")
			    .detail("ProcessID", it.locality.processId());
			return false;
		}
		if (resolverWorker->second.priorityInfo.isExcluded) {
			TraceEvent("BetterMasterExists", id)
			    .detail("Reason", "ResolverExcluded")
			    .detail("ProcessID", it.locality.processId());
			return true;
		}
		resolverClasses.push_back(resolverWorker->second.details);
	}

	// Check master fitness. Don't return false if master is excluded in case all the processes are excluded, we
	// still need master for recovery.
	ProcessClass::Fitness oldMasterFit =
	    masterWorker->second.details.processClass.machineClassFitness(ProcessClass::Master);
	if (db.config.isExcludedServer(dbi.master.addresses())) {
		oldMasterFit = std::max(oldMasterFit, ProcessClass::ExcludeFit);
	}

	std::map<Optional<Standalone<StringRef>>, int> id_used;
	std::map<Optional<Standalone<StringRef>>, int> old_id_used;
	id_used[clusterControllerProcessId]++;
	old_id_used[clusterControllerProcessId]++;
	WorkerFitnessInfo mworker = getWorkerForRoleInDatacenter(
	    clusterControllerDcId, ProcessClass::Master, ProcessClass::NeverAssign, db.config, id_used, {}, true);
	auto newMasterFit = mworker.worker.processClass.machineClassFitness(ProcessClass::Master);
	if (db.config.isExcludedServer(mworker.worker.interf.addresses())) {
		newMasterFit = std::max(newMasterFit, ProcessClass::ExcludeFit);
	}

	old_id_used[masterWorker->first]++;
	if (oldMasterFit < newMasterFit) {
		TraceEvent("NewRecruitmentIsWorse", id)
		    .detail("OldMasterFit", oldMasterFit)
		    .detail("NewMasterFit", newMasterFit)
		    .detail("OldIsCC", dbi.master.locality.processId() == clusterControllerProcessId)
		    .detail("NewIsCC", mworker.worker.interf.locality.processId() == clusterControllerProcessId);
		;
		return false;
	}
	if (oldMasterFit > newMasterFit || (dbi.master.locality.processId() == clusterControllerProcessId &&
	                                    mworker.worker.interf.locality.processId() != clusterControllerProcessId)) {
		TraceEvent("BetterMasterExists", id)
		    .detail("OldMasterFit", oldMasterFit)
		    .detail("NewMasterFit", newMasterFit)
		    .detail("OldIsCC", dbi.master.locality.processId() == clusterControllerProcessId)
		    .detail("NewIsCC", mworker.worker.interf.locality.processId() == clusterControllerProcessId);
		return true;
	}

	std::set<Optional<Key>> primaryDC;
	std::set<Optional<Key>> remoteDC;

	RegionInfo region;
	RegionInfo remoteRegion;
	if (db.config.regions.size()) {
		primaryDC.insert(clusterControllerDcId);
		for (auto& r : db.config.regions) {
			if (r.dcId != clusterControllerDcId.get()) {
				ASSERT(remoteDC.empty());
				remoteDC.insert(r.dcId);
				remoteRegion = r;
			} else {
				ASSERT(region.dcId == StringRef());
				region = r;
			}
		}
	}

	// Check tLog fitness
	updateIdUsed(tlogs, old_id_used);
	RoleFitness oldTLogFit(tlogs, ProcessClass::TLog, old_id_used);
	auto newTLogs = getWorkersForTlogs(db.config,
	                                   db.config.tLogReplicationFactor,
	                                   db.config.getDesiredLogs(),
	                                   db.config.tLogPolicy,
	                                   id_used,
	                                   true,
	                                   primaryDC);
	RoleFitness newTLogFit(newTLogs, ProcessClass::TLog, id_used);

	bool oldSatelliteFallback = false;

	if (region.satelliteTLogPolicyFallback.isValid()) {
		for (auto& logSet : dbi.logSystemConfig.tLogs) {
			if (region.satelliteTLogPolicy.isValid() && logSet.isLocal && logSet.locality == tagLocalitySatellite) {
				oldSatelliteFallback = logSet.tLogPolicy->info() != region.satelliteTLogPolicy->info();
				ASSERT(!oldSatelliteFallback ||
				       (region.satelliteTLogPolicyFallback.isValid() &&
				        logSet.tLogPolicy->info() == region.satelliteTLogPolicyFallback->info()));
				break;
			}
		}
	}

	updateIdUsed(satellite_tlogs, old_id_used);
	RoleFitness oldSatelliteTLogFit(satellite_tlogs, ProcessClass::TLog, old_id_used);
	bool newSatelliteFallback = false;
	auto newSatelliteTLogs = satellite_tlogs;
	RoleFitness newSatelliteTLogFit = oldSatelliteTLogFit;
	if (region.satelliteTLogReplicationFactor > 0 && db.config.usableRegions > 1) {
		newSatelliteTLogs =
		    getWorkersForSatelliteLogs(db.config, region, remoteRegion, id_used, newSatelliteFallback, true);
		newSatelliteTLogFit = RoleFitness(newSatelliteTLogs, ProcessClass::TLog, id_used);
	}

	std::map<Optional<Key>, int32_t> satellite_priority;
	for (auto& r : region.satellites) {
		satellite_priority[r.dcId] = r.priority;
	}

	int32_t oldSatelliteRegionFit = std::numeric_limits<int32_t>::max();
	for (auto& it : satellite_tlogs) {
		if (satellite_priority.count(it.interf.locality.dcId())) {
			oldSatelliteRegionFit = std::min(oldSatelliteRegionFit, satellite_priority[it.interf.locality.dcId()]);
		} else {
			oldSatelliteRegionFit = -1;
		}
	}

	int32_t newSatelliteRegionFit = std::numeric_limits<int32_t>::max();
	for (auto& it : newSatelliteTLogs) {
		if (satellite_priority.count(it.interf.locality.dcId())) {
			newSatelliteRegionFit = std::min(newSatelliteRegionFit, satellite_priority[it.interf.locality.dcId()]);
		} else {
			newSatelliteRegionFit = -1;
		}
	}

	if (oldSatelliteFallback && !newSatelliteFallback) {
		TraceEvent("BetterMasterExists", id)
		    .detail("OldSatelliteFallback", oldSatelliteFallback)
		    .detail("NewSatelliteFallback", newSatelliteFallback);
		return true;
	}
	if (!oldSatelliteFallback && newSatelliteFallback) {
		TraceEvent("NewRecruitmentIsWorse", id)
		    .detail("OldSatelliteFallback", oldSatelliteFallback)
		    .detail("NewSatelliteFallback", newSatelliteFallback);
		return false;
	}

	if (oldSatelliteRegionFit < newSatelliteRegionFit) {
		TraceEvent("BetterMasterExists", id)
		    .detail("OldSatelliteRegionFit", oldSatelliteRegionFit)
		    .detail("NewSatelliteRegionFit", newSatelliteRegionFit);
		return true;
	}
	if (oldSatelliteRegionFit > newSatelliteRegionFit) {
		TraceEvent("NewRecruitmentIsWorse", id)
		    .detail("OldSatelliteRegionFit", oldSatelliteRegionFit)
		    .detail("NewSatelliteRegionFit", newSatelliteRegionFit);
		return false;
	}

	updateIdUsed(remote_tlogs, old_id_used);
	RoleFitness oldRemoteTLogFit(remote_tlogs, ProcessClass::TLog, old_id_used);
	std::vector<UID> exclusionWorkerIds;
	auto fn = [](const WorkerDetails& in) { return in.interf.id(); };
	std::transform(newTLogs.begin(), newTLogs.end(), std::back_inserter(exclusionWorkerIds), fn);
	std::transform(newSatelliteTLogs.begin(), newSatelliteTLogs.end(), std::back_inserter(exclusionWorkerIds), fn);
	RoleFitness newRemoteTLogFit = oldRemoteTLogFit;
	if (db.config.usableRegions > 1 && (dbi.recoveryState == RecoveryState::ALL_LOGS_RECRUITED ||
	                                    dbi.recoveryState == RecoveryState::FULLY_RECOVERED)) {
		newRemoteTLogFit = RoleFitness(getWorkersForTlogs(db.config,
		                                                  db.config.getRemoteTLogReplicationFactor(),
		                                                  db.config.getDesiredRemoteLogs(),
		                                                  db.config.getRemoteTLogPolicy(),
		                                                  id_used,
		                                                  true,
		                                                  remoteDC,
		                                                  exclusionWorkerIds),
		                               ProcessClass::TLog,
		                               id_used);
	}
	int oldRouterCount =
	    oldTLogFit.count * std::max<int>(1, db.config.desiredLogRouterCount / std::max(1, oldTLogFit.count));
	int newRouterCount =
	    newTLogFit.count * std::max<int>(1, db.config.desiredLogRouterCount / std::max(1, newTLogFit.count));
	updateIdUsed(log_routers, old_id_used);
	RoleFitness oldLogRoutersFit(log_routers, ProcessClass::LogRouter, old_id_used);
	RoleFitness newLogRoutersFit = oldLogRoutersFit;
	if (db.config.usableRegions > 1 && dbi.recoveryState == RecoveryState::FULLY_RECOVERED) {
		newLogRoutersFit = RoleFitness(getWorkersForRoleInDatacenter(*remoteDC.begin(),
		                                                             ProcessClass::LogRouter,
		                                                             newRouterCount,
		                                                             db.config,
		                                                             id_used,
		                                                             {},
		                                                             Optional<WorkerFitnessInfo>(),
		                                                             true),
		                               ProcessClass::LogRouter,
		                               id_used);
	}

	if (oldLogRoutersFit.count < oldRouterCount) {
		oldLogRoutersFit.worstFit = ProcessClass::NeverAssign;
	}
	if (newLogRoutersFit.count < newRouterCount) {
		newLogRoutersFit.worstFit = ProcessClass::NeverAssign;
	}

	// Check proxy/grvProxy/resolver fitness
	updateIdUsed(commitProxyClasses, old_id_used);
	updateIdUsed(grvProxyClasses, old_id_used);
	updateIdUsed(resolverClasses, old_id_used);
	RoleFitness oldCommitProxyFit(commitProxyClasses, ProcessClass::CommitProxy, old_id_used);
	RoleFitness oldGrvProxyFit(grvProxyClasses, ProcessClass::GrvProxy, old_id_used);
	RoleFitness oldResolverFit(resolverClasses, ProcessClass::Resolver, old_id_used);

	std::map<Optional<Standalone<StringRef>>, int> preferredSharing;
	auto first_commit_proxy = getWorkerForRoleInDatacenter(clusterControllerDcId,
	                                                       ProcessClass::CommitProxy,
	                                                       ProcessClass::ExcludeFit,
	                                                       db.config,
	                                                       id_used,
	                                                       preferredSharing,
	                                                       true);
	preferredSharing[first_commit_proxy.worker.interf.locality.processId()] = 0;
	auto first_grv_proxy = getWorkerForRoleInDatacenter(clusterControllerDcId,
	                                                    ProcessClass::GrvProxy,
	                                                    ProcessClass::ExcludeFit,
	                                                    db.config,
	                                                    id_used,
	                                                    preferredSharing,
	                                                    true);
	preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
	auto first_resolver = getWorkerForRoleInDatacenter(clusterControllerDcId,
	                                                   ProcessClass::Resolver,
	                                                   ProcessClass::ExcludeFit,
	                                                   db.config,
	                                                   id_used,
	                                                   preferredSharing,
	                                                   true);
	preferredSharing[first_resolver.worker.interf.locality.processId()] = 2;
	auto maxUsed = std::max({ first_commit_proxy.used, first_grv_proxy.used, first_resolver.used });
	first_commit_proxy.used = maxUsed;
	first_grv_proxy.used = maxUsed;
	first_resolver.used = maxUsed;
	auto commit_proxies = getWorkersForRoleInDatacenter(clusterControllerDcId,
	                                                    ProcessClass::CommitProxy,
	                                                    db.config.getDesiredCommitProxies(),
	                                                    db.config,
	                                                    id_used,
	                                                    preferredSharing,
	                                                    first_commit_proxy,
	                                                    true);
	auto grv_proxies = getWorkersForRoleInDatacenter(clusterControllerDcId,
	                                                 ProcessClass::GrvProxy,
	                                                 db.config.getDesiredGrvProxies(),
	                                                 db.config,
	                                                 id_used,
	                                                 preferredSharing,
	                                                 first_grv_proxy,
	                                                 true);
	auto resolvers = getWorkersForRoleInDatacenter(clusterControllerDcId,
	                                               ProcessClass::Resolver,
	                                               db.config.getDesiredResolvers(),
	                                               db.config,
	                                               id_used,
	                                               preferredSharing,
	                                               first_resolver,
	                                               true);

	RoleFitness newCommitProxyFit(commit_proxies, ProcessClass::CommitProxy, id_used);
	RoleFitness newGrvProxyFit(grv_proxies, ProcessClass::GrvProxy, id_used);
	RoleFitness newResolverFit(resolvers, ProcessClass::Resolver, id_used);

	// Check backup worker fitness
	updateIdUsed(backup_workers, old_id_used);
	RoleFitness oldBackupWorkersFit(backup_workers, ProcessClass::Backup, old_id_used);
	const int nBackup = backup_addresses.size();
	RoleFitness newBackupWorkersFit(getWorkersForRoleInDatacenter(clusterControllerDcId,
	                                                              ProcessClass::Backup,
	                                                              nBackup,
	                                                              db.config,
	                                                              id_used,
	                                                              {},
	                                                              Optional<WorkerFitnessInfo>(),
	                                                              true),
	                                ProcessClass::Backup,
	                                id_used);

	auto oldFit = std::make_tuple(oldTLogFit,
	                              oldSatelliteTLogFit,
	                              oldCommitProxyFit,
	                              oldGrvProxyFit,
	                              oldResolverFit,
	                              oldBackupWorkersFit,
	                              oldRemoteTLogFit,
	                              oldLogRoutersFit);
	auto newFit = std::make_tuple(newTLogFit,
	                              newSatelliteTLogFit,
	                              newCommitProxyFit,
	                              newGrvProxyFit,
	                              newResolverFit,
	                              newBackupWorkersFit,
	                              newRemoteTLogFit,
	                              newLogRoutersFit);

	if (oldFit > newFit) {
		TraceEvent("BetterMasterExists", id)
		    .detail("OldMasterFit", oldMasterFit)
		    .detail("NewMasterFit", newMasterFit)
		    .detail("OldTLogFit", oldTLogFit.toString())
		    .detail("NewTLogFit", newTLogFit.toString())
		    .detail("OldSatelliteFit", oldSatelliteTLogFit.toString())
		    .detail("NewSatelliteFit", newSatelliteTLogFit.toString())
		    .detail("OldCommitProxyFit", oldCommitProxyFit.toString())
		    .detail("NewCommitProxyFit", newCommitProxyFit.toString())
		    .detail("OldGrvProxyFit", oldGrvProxyFit.toString())
		    .detail("NewGrvProxyFit", newGrvProxyFit.toString())
		    .detail("OldResolverFit", oldResolverFit.toString())
		    .detail("NewResolverFit", newResolverFit.toString())
		    .detail("OldBackupWorkerFit", oldBackupWorkersFit.toString())
		    .detail("NewBackupWorkerFit", newBackupWorkersFit.toString())
		    .detail("OldRemoteFit", oldRemoteTLogFit.toString())
		    .detail("NewRemoteFit", newRemoteTLogFit.toString())
		    .detail("OldRouterFit", oldLogRoutersFit.toString())
		    .detail("NewRouterFit", newLogRoutersFit.toString())
		    .detail("OldSatelliteFallback", oldSatelliteFallback)
		    .detail("NewSatelliteFallback", newSatelliteFallback);
		return true;
	}

	if (oldFit < newFit) {
		TraceEvent("NewRecruitmentIsWorse", id)
		    .detail("OldMasterFit", oldMasterFit)
		    .detail("NewMasterFit", newMasterFit)
		    .detail("OldTLogFit", oldTLogFit.toString())
		    .detail("NewTLogFit", newTLogFit.toString())
		    .detail("OldSatelliteFit", oldSatelliteTLogFit.toString())
		    .detail("NewSatelliteFit", newSatelliteTLogFit.toString())
		    .detail("OldCommitProxyFit", oldCommitProxyFit.toString())
		    .detail("NewCommitProxyFit", newCommitProxyFit.toString())
		    .detail("OldGrvProxyFit", oldGrvProxyFit.toString())
		    .detail("NewGrvProxyFit", newGrvProxyFit.toString())
		    .detail("OldResolverFit", oldResolverFit.toString())
		    .detail("NewResolverFit", newResolverFit.toString())
		    .detail("OldBackupWorkerFit", oldBackupWorkersFit.toString())
		    .detail("NewBackupWorkerFit", newBackupWorkersFit.toString())
		    .detail("OldRemoteFit", oldRemoteTLogFit.toString())
		    .detail("NewRemoteFit", newRemoteTLogFit.toString())
		    .detail("OldRouterFit", oldLogRoutersFit.toString())
		    .detail("NewRouterFit", newLogRoutersFit.toString())
		    .detail("OldSatelliteFallback", oldSatelliteFallback)
		    .detail("NewSatelliteFallback", newSatelliteFallback);
	}
	return false;
}

// Returns true iff processId is currently being used
// for any non-singleton role other than master
bool ClusterController::isUsedNotMaster(Optional<Key> processId) const {
	ASSERT(masterProcessId.present());
	if (processId == masterProcessId)
		return false;

	auto& dbInfo = db.serverInfo->get();
	for (const auto& tlogset : dbInfo.logSystemConfig.tLogs) {
		for (const auto& tlog : tlogset.tLogs) {
			if (tlog.present() && tlog.interf().filteredLocality.processId() == processId)
				return true;
		}
	}
	for (const CommitProxyInterface& interf : dbInfo.client.commitProxies) {
		if (interf.processId == processId)
			return true;
	}
	for (const GrvProxyInterface& interf : dbInfo.client.grvProxies) {
		if (interf.processId == processId)
			return true;
	}
	for (const ResolverInterface& interf : dbInfo.resolvers) {
		if (interf.locality.processId() == processId)
			return true;
	}
	if (processId == clusterControllerProcessId)
		return true;

	return false;
}

// Returns true iff
// - role is master, or
// - role is a singleton AND worker's pid is being used for any non-singleton role
bool ClusterController::onMasterIsBetter(const WorkerDetails& worker, ProcessClass::ClusterRole role) const {
	ASSERT(masterProcessId.present());
	const auto& pid = worker.interf.locality.processId();
	if ((role != ProcessClass::DataDistributor && role != ProcessClass::Ratekeeper &&
	     role != ProcessClass::BlobManager && role != ProcessClass::EncryptKeyProxy &&
	     role != ProcessClass::ConsistencyScan) ||
	    pid == masterProcessId.get()) {
		return false;
	}
	return isUsedNotMaster(pid);
}

// Returns a map of <pid, numRolesUsingPid> for all non-singleton roles
std::map<Optional<Standalone<StringRef>>, int> ClusterController::getUsedIds() {
	std::map<Optional<Standalone<StringRef>>, int> idUsed;
	updateKnownIds(&idUsed);

	auto& dbInfo = db.serverInfo->get();
	for (const auto& tlogset : dbInfo.logSystemConfig.tLogs) {
		for (const auto& tlog : tlogset.tLogs) {
			if (tlog.present()) {
				idUsed[tlog.interf().filteredLocality.processId()]++;
			}
		}
	}

	for (const CommitProxyInterface& interf : dbInfo.client.commitProxies) {
		ASSERT(interf.processId.present());
		idUsed[interf.processId]++;
	}
	for (const GrvProxyInterface& interf : dbInfo.client.grvProxies) {
		ASSERT(interf.processId.present());
		idUsed[interf.processId]++;
	}
	for (const ResolverInterface& interf : dbInfo.resolvers) {
		ASSERT(interf.locality.processId().present());
		idUsed[interf.locality.processId()]++;
	}
	return idUsed;
}

// Updates work health signals in `workerHealth` based on `req`.
void ClusterController::updateWorkerHealth(const UpdateWorkerHealthRequest& req) {
	std::string degradedPeersString;
	for (int i = 0; i < req.degradedPeers.size(); ++i) {
		degradedPeersString += (i == 0 ? "" : " ") + req.degradedPeers[i].toString();
	}
	std::string disconnectedPeersString;
	for (int i = 0; i < req.disconnectedPeers.size(); ++i) {
		disconnectedPeersString += (i == 0 ? "" : " ") + req.disconnectedPeers[i].toString();
	}
	TraceEvent("ClusterControllerUpdateWorkerHealth")
	    .detail("WorkerAddress", req.address)
	    .detail("DegradedPeers", degradedPeersString)
	    .detail("DisconnectedPeers", disconnectedPeersString);

	double currentTime = now();

	// Current `workerHealth` doesn't have any information about the incoming worker. Add the worker into
	// `workerHealth`.
	if (workerHealth.find(req.address) == workerHealth.end()) {
		workerHealth[req.address] = {};
		for (const auto& degradedPeer : req.degradedPeers) {
			workerHealth[req.address].degradedPeers[degradedPeer] = { currentTime, currentTime };
		}

		for (const auto& degradedPeer : req.disconnectedPeers) {
			workerHealth[req.address].disconnectedPeers[degradedPeer] = { currentTime, currentTime };
		}

		return;
	}

	// The incoming worker already exists in `workerHealth`.

	auto& health = workerHealth[req.address];

	// Update the worker's degradedPeers.
	for (const auto& peer : req.degradedPeers) {
		auto it = health.degradedPeers.find(peer);
		if (it == health.degradedPeers.end()) {
			health.degradedPeers[peer] = { currentTime, currentTime };
			continue;
		}
		it->second.lastRefreshTime = currentTime;
	}

	// Update the worker's disconnectedPeers.
	for (const auto& peer : req.disconnectedPeers) {
		auto it = health.disconnectedPeers.find(peer);
		if (it == health.disconnectedPeers.end()) {
			health.disconnectedPeers[peer] = { currentTime, currentTime };
			continue;
		}
		it->second.lastRefreshTime = currentTime;
	}
}

// Checks that if any worker or their degraded peers have recovered. If so, remove them from `workerHealth`.
void ClusterController::updateRecoveredWorkers() {
	double currentTime = now();
	for (auto& [workerAddress, health] : workerHealth) {
		for (auto it = health.degradedPeers.begin(); it != health.degradedPeers.end();) {
			if (currentTime - it->second.lastRefreshTime > SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL) {
				TraceEvent("WorkerPeerHealthRecovered").detail("Worker", workerAddress).detail("Peer", it->first);
				health.degradedPeers.erase(it++);
			} else {
				++it;
			}
		}
		for (auto it = health.disconnectedPeers.begin(); it != health.disconnectedPeers.end();) {
			if (currentTime - it->second.lastRefreshTime > SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL) {
				TraceEvent("WorkerPeerHealthRecovered").detail("Worker", workerAddress).detail("Peer", it->first);
				health.disconnectedPeers.erase(it++);
			} else {
				++it;
			}
		}
	}

	for (auto it = workerHealth.begin(); it != workerHealth.end();) {
		if (it->second.degradedPeers.empty() && it->second.disconnectedPeers.empty()) {
			TraceEvent("WorkerAllPeerHealthRecovered").detail("Worker", it->first);
			workerHealth.erase(it++);
		} else {
			++it;
		}
	}
}

DegradationInfo ClusterController::getDegradationInfo() {
	updateRecoveredWorkers();

	// Build a map keyed by measured degraded peer. This map gives the info that who complains a particular server.
	std::unordered_map<NetworkAddress, std::unordered_set<NetworkAddress>> degradedLinkDst2Src;
	std::unordered_map<NetworkAddress, std::unordered_set<NetworkAddress>> disconnectedLinkDst2Src;
	double currentTime = now();
	for (const auto& [server, health] : workerHealth) {
		for (const auto& [degradedPeer, times] : health.degradedPeers) {
			if (currentTime - times.startTime < SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL) {
				// This degraded link is not long enough to be considered as degraded.
				continue;
			}
			degradedLinkDst2Src[degradedPeer].insert(server);
		}
		for (const auto& [disconnectedPeer, times] : health.disconnectedPeers) {
			if (currentTime - times.startTime < SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL) {
				// This degraded link is not long enough to be considered as degraded.
				continue;
			}
			disconnectedLinkDst2Src[disconnectedPeer].insert(server);
		}
	}

	auto deterministicDecendingOrder = [](const std::pair<int, NetworkAddress>& a,
	                                      const std::pair<int, NetworkAddress>& b) -> bool {
		return a.first > b.first || (a.first == b.first && a.second < b.second);
	};

	// Sort degraded peers based on the number of workers complaining about it.
	std::vector<std::pair<int, NetworkAddress>> count2DegradedPeer;
	for (const auto& [degradedPeer, complainers] : degradedLinkDst2Src) {
		count2DegradedPeer.push_back({ complainers.size(), degradedPeer });
	}
	std::sort(count2DegradedPeer.begin(), count2DegradedPeer.end(), deterministicDecendingOrder);

	std::vector<std::pair<int, NetworkAddress>> count2DisconnectedPeer;
	for (const auto& [disconnectedPeer, complainers] : disconnectedLinkDst2Src) {
		count2DisconnectedPeer.push_back({ complainers.size(), disconnectedPeer });
	}
	std::sort(count2DisconnectedPeer.begin(), count2DisconnectedPeer.end(), deterministicDecendingOrder);

	// Go through all reported degraded peers by decreasing order of the number of complainers. For a particular
	// degraded peer, if a complainer has already be considered as degraded, we skip the current examine degraded
	// peer since there has been one endpoint on the link between degradedPeer and complainer considered as
	// degraded. This is to address the issue that both endpoints on a bad link may be considered as degraded
	// server.
	//
	// For example, if server A is already considered as a degraded server, and A complains B, we won't add B as
	// degraded since A is already considered as degraded.
	//
	// In the meantime, we also count the number of satellite workers got complained. If enough number of satellite
	// workers are degraded, this may indicates that the whole network between primary and satellite is bad.
	std::unordered_set<NetworkAddress> currentDegradedServers;
	int satelliteBadServerCount = 0;
	for (const auto& [complainerCount, badServer] : count2DegradedPeer) {
		for (const auto& complainer : degradedLinkDst2Src[badServer]) {
			if (currentDegradedServers.find(complainer) == currentDegradedServers.end()) {
				currentDegradedServers.insert(badServer);
				break;
			}
		}

		if (SERVER_KNOBS->CC_ENABLE_ENTIRE_SATELLITE_MONITORING &&
		    addressInDbAndPrimarySatelliteDc(badServer, db.serverInfo) &&
		    complainerCount >= SERVER_KNOBS->CC_SATELLITE_DEGRADATION_MIN_COMPLAINER) {
			++satelliteBadServerCount;
		}
	}

	DegradationInfo currentDegradationInfo;
	for (const auto& [complainerCount, badServer] : count2DisconnectedPeer) {
		for (const auto& complainer : disconnectedLinkDst2Src[badServer]) {
			if (currentDegradationInfo.disconnectedServers.find(complainer) ==
			    currentDegradationInfo.disconnectedServers.end()) {
				currentDegradationInfo.disconnectedServers.insert(badServer);
				break;
			}
		}

		if (SERVER_KNOBS->CC_ENABLE_ENTIRE_SATELLITE_MONITORING &&
		    addressInDbAndPrimarySatelliteDc(badServer, db.serverInfo) &&
		    complainerCount >= SERVER_KNOBS->CC_SATELLITE_DEGRADATION_MIN_COMPLAINER) {
			++satelliteBadServerCount;
		}
	}

	// For degraded server that are complained by more than SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE, we
	// don't know if it is a hot server, or the network is bad. We remove from the returned degraded server list.
	for (const auto& badServer : currentDegradedServers) {
		if (degradedLinkDst2Src[badServer].size() <= SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE) {
			currentDegradationInfo.degradedServers.insert(badServer);
		}
	}

	// If enough number of satellite workers are bad, we mark the entire satellite is bad. Note that this needs to
	// be used with caution (controlled by CC_ENABLE_ENTIRE_SATELLITE_MONITORING knob), since the slow workers may
	// also be caused by workload.
	if (satelliteBadServerCount >= SERVER_KNOBS->CC_SATELLITE_DEGRADATION_MIN_BAD_SERVER) {
		currentDegradationInfo.degradedSatellite = true;
	}
	return currentDegradationInfo;
}

// Whether the transaction system (in primary DC if in HA setting) contains degraded servers.
bool ClusterController::transactionSystemContainsDegradedServers() {
	const ServerDBInfo& dbi = db.serverInfo->get();
	auto transactionWorkerInList = [&dbi](const std::unordered_set<NetworkAddress>& serverList) -> bool {
		for (const auto& server : serverList) {
			if (dbi.master.addresses().contains(server)) {
				return true;
			}

			for (const auto& logSet : dbi.logSystemConfig.tLogs) {
				if (!logSet.isLocal || logSet.locality == tagLocalitySatellite) {
					continue;
				}
				for (const auto& tlog : logSet.tLogs) {
					if (tlog.present() && tlog.interf().addresses().contains(server)) {
						return true;
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

	return transactionWorkerInList(degradationInfo.degradedServers) ||
	       transactionWorkerInList(degradationInfo.disconnectedServers);
}

// Whether transaction system in the remote DC, e.g. log router and tlogs in the remote DC, contains degraded
// servers.
bool ClusterController::remoteTransactionSystemContainsDegradedServers() {
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

// Returns true if remote DC is healthy and can failover to.
bool ClusterController::remoteDCIsHealthy() {
	// Ignore remote DC health if worker health monitor is disabled.
	if (!SERVER_KNOBS->CC_ENABLE_WORKER_HEALTH_MONITOR) {
		return true;
	}

	// When we just start, we ignore any remote DC health info since the current CC may be elected at wrong DC due
	// to that all the processes are still starting.
	if (machineStartTime() == 0) {
		return true;
	}

	if (now() - machineStartTime() < SERVER_KNOBS->INITIAL_UPDATE_CROSS_DC_INFO_DELAY) {
		return true;
	}

	// When remote DC health is not monitored, we may not know whether the remote is healthy or not. So return false
	// here to prevent failover.
	if (!remoteDCMonitorStarted) {
		return false;
	}

	return !remoteTransactionSystemContainsDegradedServers();
}

// Returns true when the cluster controller should trigger a recovery due to degraded servers used in the
// transaction system in the primary data center.
bool ClusterController::shouldTriggerRecoveryDueToDegradedServers() {
	if (degradationInfo.degradedServers.size() + degradationInfo.disconnectedServers.size() >
	    SERVER_KNOBS->CC_MAX_EXCLUSION_DUE_TO_HEALTH) {
		return false;
	}

	if (db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		return false;
	}

	// Do not trigger recovery if the cluster controller is excluded, since the master will change
	// anyways once the cluster controller is moved
	if (id_worker[clusterControllerProcessId].priorityInfo.isExcluded) {
		return false;
	}

	return transactionSystemContainsDegradedServers();
}

// Returns true when the cluster controller should trigger a failover due to degraded servers used in the
// transaction system in the primary data center, and no degradation in the remote data center.
bool ClusterController::shouldTriggerFailoverDueToDegradedServers() {
	if (db.config.usableRegions <= 1) {
		return false;
	}

	if (SERVER_KNOBS->CC_FAILOVER_DUE_TO_HEALTH_MIN_DEGRADATION >
	    SERVER_KNOBS->CC_FAILOVER_DUE_TO_HEALTH_MAX_DEGRADATION) {
		TraceEvent(SevWarn, "TriggerFailoverDueToDegradedServersInvalidConfig")
		    .suppressFor(1.0)
		    .detail("Min", SERVER_KNOBS->CC_FAILOVER_DUE_TO_HEALTH_MIN_DEGRADATION)
		    .detail("Max", SERVER_KNOBS->CC_FAILOVER_DUE_TO_HEALTH_MAX_DEGRADATION);
		return false;
	}

	bool remoteIsHealthy = !remoteTransactionSystemContainsDegradedServers();
	if (degradationInfo.degradedSatellite && remoteIsHealthy) {
		// If the satellite DC is bad, a failover is desired despite the number of degraded servers.
		return true;
	}

	int degradedServerSize = degradationInfo.degradedServers.size() + degradationInfo.disconnectedServers.size();
	if (degradedServerSize < SERVER_KNOBS->CC_FAILOVER_DUE_TO_HEALTH_MIN_DEGRADATION ||
	    degradedServerSize > SERVER_KNOBS->CC_FAILOVER_DUE_TO_HEALTH_MAX_DEGRADATION) {
		return false;
	}

	// Do not trigger recovery if the cluster controller is excluded, since the master will change
	// anyways once the cluster controller is moved
	if (id_worker[clusterControllerProcessId].priorityInfo.isExcluded) {
		return false;
	}

	return transactionSystemContainsDegradedServers() && remoteIsHealthy;
}

int ClusterController::recentRecoveryCountDueToHealth() {
	while (!recentHealthTriggeredRecoveryTime.empty() &&
	       now() - recentHealthTriggeredRecoveryTime.front() > SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL) {
		recentHealthTriggeredRecoveryTime.pop();
	}
	return recentHealthTriggeredRecoveryTime.size();
}

bool ClusterController::isExcludedDegradedServer(const NetworkAddressList& a) const {
	for (const auto& server : excludedDegradedServers) {
		if (a.contains(server))
			return true;
	}
	return false;
}

ClusterController::ClusterController(ClusterControllerFullInterface const& ccInterface,
                                     LocalityData const& locality,
                                     ServerCoordinators const& coordinators,
                                     Reference<AsyncVar<Optional<UID>>> clusterId)
  : gotProcessClasses(false), gotFullyRecoveredConfig(false), shouldCommitSuicide(false),
    clusterControllerProcessId(locality.processId()), clusterControllerDcId(locality.dcId()), id(ccInterface.id()),
    clusterId(clusterId), ac(false), outstandingRequestChecker(Void()), outstandingRemoteRequestChecker(Void()),
    startTime(now()), goodRecruitmentTime(Never()), goodRemoteRecruitmentTime(Never()), datacenterVersionDifference(0),
    versionDifferenceUpdated(false), remoteDCMonitorStarted(false), remoteTransactionSystemDegraded(false),
    recruitDistributor(false), recruitRatekeeper(false), recruitBlobManager(false), recruitBlobMigrator(false),
    recruitEncryptKeyProxy(false), recruitConsistencyScan(false),
    clusterControllerMetrics("ClusterController", id.toString()),
    openDatabaseRequests("OpenDatabaseRequests", clusterControllerMetrics),
    registerWorkerRequests("RegisterWorkerRequests", clusterControllerMetrics),
    getWorkersRequests("GetWorkersRequests", clusterControllerMetrics),
    getClientWorkersRequests("GetClientWorkersRequests", clusterControllerMetrics),
    registerMasterRequests("RegisterMasterRequests", clusterControllerMetrics),
    statusRequests("StatusRequests", clusterControllerMetrics),
    recruitedMasterWorkerEventHolder(makeReference<EventCacheHolder>("RecruitedMasterWorker")) {
	auto serverInfo = ServerDBInfo();
	serverInfo.id = deterministicRandom()->randomUniqueID();
	serverInfo.infoGeneration = db.incrementAndGetDbInfoCount();
	serverInfo.masterLifetime.ccID = id;
	serverInfo.clusterInterface = ccInterface;
	serverInfo.myLocality = locality;
	serverInfo.client.isEncryptionEnabled = SERVER_KNOBS->ENABLE_ENCRYPTION;
	db.serverInfo->set(serverInfo);
	cx = openDBOnServer(db.serverInfo, TaskPriority::DefaultEndpoint, LockAware::True);

	specialCounter(clusterControllerMetrics, "ClientCount", [this]() { return db.getClientCount(); });
}

ClusterController::~ClusterController() {
	ac.clear(false);
	id_worker.clear();
}
