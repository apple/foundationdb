/*
 * ClusterController.actor.h
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
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/BlobMigratorInterface.h"
#include <utility>

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_CLUSTERCONTROLLER_ACTOR_G_H)
#define FDBSERVER_CLUSTERCONTROLLER_ACTOR_G_H
#include "fdbserver/ClusterController.actor.g.h"
#elif !defined(FDBSERVER_CLUSTERCONTROLLER_ACTOR_H)
#define FDBSERVER_CLUSTERCONTROLLER_ACTOR_H

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/Recruiter.h"
#include "fdbserver/RoleFitness.h"
#include "fdbserver/WorkerInfo.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/SystemMonitor.h"

#include "metacluster/MetaclusterMetrics.h"

#include "flow/actorcompiler.h" // This must be the last #include.

class ClusterControllerData {
public:
	struct DBInfo {
		Reference<AsyncVar<ClientDBInfo>> clientInfo;
		Reference<AsyncVar<ServerDBInfo>> serverInfo;
		std::map<NetworkAddress, double> incompatibleConnections;
		AsyncTrigger forceMasterFailure;
		int64_t masterRegistrationCount;
		int64_t dbInfoCount;
		bool recoveryStalled;
		bool forceRecovery;
		DatabaseConfiguration config; // Asynchronously updated via master registration
		DatabaseConfiguration fullyRecoveredConfig;
		Database db;
		int unfinishedRecoveries;
		int logGenerations;
		bool cachePopulated;
		std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>> clientStatus;
		Future<Void> clientCounter;
		int clientCount;
		AsyncVar<bool> blobGranulesEnabled;
		AsyncVar<bool> blobRestoreEnabled;
		ClusterType clusterType = ClusterType::STANDALONE;
		Optional<ClusterName> metaclusterName;
		Optional<UnversionedMetaclusterRegistrationEntry> metaclusterRegistration;
		metacluster::MetaclusterMetrics metaclusterMetrics;

		DBInfo()
		  : clientInfo(new AsyncVar<ClientDBInfo>()), serverInfo(new AsyncVar<ServerDBInfo>()),
		    masterRegistrationCount(0), dbInfoCount(0), recoveryStalled(false), forceRecovery(false),
		    db(DatabaseContext::create(clientInfo,
		                               Future<Void>(),
		                               LocalityData(),
		                               EnableLocalityLoadBalance::True,
		                               TaskPriority::DefaultEndpoint,
		                               LockAware::True)), // SOMEDAY: Locality!
		    unfinishedRecoveries(0), logGenerations(0), cachePopulated(false), clientCount(0),
		    blobGranulesEnabled(config.blobGranulesEnabled), blobRestoreEnabled(false) {
			clientCounter = countClients(this);
		}

		void setDistributor(const DataDistributorInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.distributor = interf;
			serverInfo->set(newInfo);
		}

		void setRatekeeper(const RatekeeperInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.ratekeeper = interf;
			serverInfo->set(newInfo);
		}

		void setBlobManager(const BlobManagerInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.blobManager = interf;
			serverInfo->set(newInfo);
		}

		void setBlobMigrator(const BlobMigratorInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.blobMigrator = interf;
			serverInfo->set(newInfo);
		}

		void setEncryptKeyProxy(const EncryptKeyProxyInterface& interf) {
			auto newInfo = serverInfo->get();
			auto newClientInfo = clientInfo->get();
			newClientInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.client.encryptKeyProxy = interf;
			newClientInfo.encryptKeyProxy = interf;
			serverInfo->set(newInfo);
			clientInfo->set(newClientInfo);
		}

		void setConsistencyScan(const ConsistencyScanInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.consistencyScan = interf;
			serverInfo->set(newInfo);
		}

		void clearInterf(ProcessClass::ClassType t) {
			auto newInfo = serverInfo->get();
			auto newClientInfo = clientInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newClientInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			if (t == ProcessClass::DataDistributorClass) {
				newInfo.distributor = Optional<DataDistributorInterface>();
			} else if (t == ProcessClass::RatekeeperClass) {
				newInfo.ratekeeper = Optional<RatekeeperInterface>();
			} else if (t == ProcessClass::BlobManagerClass) {
				newInfo.blobManager = Optional<BlobManagerInterface>();
			} else if (t == ProcessClass::BlobMigratorClass) {
				newInfo.blobMigrator = Optional<BlobMigratorInterface>();
			} else if (t == ProcessClass::EncryptKeyProxyClass) {
				newInfo.client.encryptKeyProxy = Optional<EncryptKeyProxyInterface>();
				newClientInfo.encryptKeyProxy = Optional<EncryptKeyProxyInterface>();
			} else if (t == ProcessClass::ConsistencyScanClass) {
				newInfo.consistencyScan = Optional<ConsistencyScanInterface>();
			}
			serverInfo->set(newInfo);
			clientInfo->set(newClientInfo);
		}

		ACTOR static Future<Void> countClients(DBInfo* self) {
			loop {
				wait(delay(SERVER_KNOBS->CC_PRUNE_CLIENTS_INTERVAL));

				self->clientCount = 0;
				for (auto itr = self->clientStatus.begin(); itr != self->clientStatus.end();) {
					if (now() - itr->second.first < 2 * SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL) {
						self->clientCount += itr->second.second.clientCount;
						++itr;
					} else {
						itr = self->clientStatus.erase(itr);
					}
				}
			}
		}
	};

	struct UpdateWorkerList {
		Future<Void> init(Database const& db) { return update(this, db); }

		void set(Optional<Standalone<StringRef>> processID, Optional<ProcessData> data) {
			delta[processID] = data;
			anyDelta.set(true);
		}

	private:
		std::map<Optional<Standalone<StringRef>>, Optional<ProcessData>> delta;
		AsyncVar<bool> anyDelta;

		ACTOR static Future<Void> update(UpdateWorkerList* self, Database db) {
			// The Database we are using is based on worker registrations to this cluster controller, which come only
			// from master servers that we started, so it shouldn't be possible for multiple cluster controllers to
			// fight.
			state Transaction tr(db);
			loop {
				try {
					tr.clear(workerListKeys);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			loop {
				tr.reset();

				// Wait for some changes
				while (!self->anyDelta.get())
					wait(self->anyDelta.onChange());
				self->anyDelta.set(false);

				state std::map<Optional<Standalone<StringRef>>, Optional<ProcessData>> delta;
				delta.swap(self->delta);

				TraceEvent("UpdateWorkerList").detail("DeltaCount", delta.size());

				// Do a transaction to write the changes
				loop {
					try {
						for (auto w = delta.begin(); w != delta.end(); ++w) {
							if (w->second.present()) {
								tr.set(workerListKeyFor(w->first.get()), workerListValue(w->second.get()));
							} else
								tr.clear(workerListKeyFor(w->first.get()));
						}
						wait(tr.commit());
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			}
		}
	};

	void checkRecoveryStalled() {
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
				recruiter.checkRegions(this, regions);
			}
		}
	}

	void updateIdUsed(const std::vector<WorkerDetails>& workers,
	                  std::map<Optional<Standalone<StringRef>>, int>& id_used) {
		for (auto& it : workers) {
			id_used[it.interf.locality.processId()]++;
		}
	}

	// FIXME: determine when to fail the cluster controller when a primaryDC has not been set

	// This function returns true when the cluster controller determines it is worth forcing
	// a cluster recovery in order to change the recruited processes in the transaction subsystem.
	bool betterMasterExists() {
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
			recruiter.checkRegions(this, db.config.regions);
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
				TraceEvent("BetterMasterExists", id)
				    .detail("Reason", "GrvProxyExcluded")
				    .detail("ProcessID", it.processId);
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
		WorkerFitnessInfo mworker = recruiter.getWorkerForRoleInDatacenter(
		    this, clusterControllerDcId, ProcessClass::Master, ProcessClass::NeverAssign, db.config, id_used, {}, true);
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
		auto newTLogs = recruiter.getWorkersForTLogs(this,
		                                             db.config,
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
			newSatelliteTLogs = recruiter.getWorkersForSatelliteLogs(
			    this, db.config, region, remoteRegion, id_used, newSatelliteFallback, true);
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
			newRemoteTLogFit = RoleFitness(recruiter.getWorkersForTLogs(this,
			                                                            db.config,
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
			newLogRoutersFit = RoleFitness(recruiter.getWorkersForRoleInDatacenter(this,
			                                                                       *remoteDC.begin(),
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
		auto first_commit_proxy = recruiter.getWorkerForRoleInDatacenter(this,
		                                                                 clusterControllerDcId,
		                                                                 ProcessClass::CommitProxy,
		                                                                 ProcessClass::ExcludeFit,
		                                                                 db.config,
		                                                                 id_used,
		                                                                 preferredSharing,
		                                                                 true);
		preferredSharing[first_commit_proxy.worker.interf.locality.processId()] = 0;
		auto first_grv_proxy = recruiter.getWorkerForRoleInDatacenter(this,
		                                                              clusterControllerDcId,
		                                                              ProcessClass::GrvProxy,
		                                                              ProcessClass::ExcludeFit,
		                                                              db.config,
		                                                              id_used,
		                                                              preferredSharing,
		                                                              true);
		preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
		auto first_resolver = recruiter.getWorkerForRoleInDatacenter(this,
		                                                             clusterControllerDcId,
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
		auto commit_proxies = recruiter.getWorkersForRoleInDatacenter(this,
		                                                              clusterControllerDcId,
		                                                              ProcessClass::CommitProxy,
		                                                              db.config.getDesiredCommitProxies(),
		                                                              db.config,
		                                                              id_used,
		                                                              preferredSharing,
		                                                              first_commit_proxy,
		                                                              true);
		auto grv_proxies = recruiter.getWorkersForRoleInDatacenter(this,
		                                                           clusterControllerDcId,
		                                                           ProcessClass::GrvProxy,
		                                                           db.config.getDesiredGrvProxies(),
		                                                           db.config,
		                                                           id_used,
		                                                           preferredSharing,
		                                                           first_grv_proxy,
		                                                           true);
		auto resolvers = recruiter.getWorkersForRoleInDatacenter(this,
		                                                         clusterControllerDcId,
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
		RoleFitness newBackupWorkersFit(recruiter.getWorkersForRoleInDatacenter(this,
		                                                                        clusterControllerDcId,
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
	bool isUsedNotMaster(Optional<Key> processId) const {
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
	bool onMasterIsBetter(const WorkerDetails& worker, ProcessClass::ClusterRole role) const {
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
	std::map<Optional<Standalone<StringRef>>, int> getUsedIds() {
		std::map<Optional<Standalone<StringRef>>, int> idUsed;
		Recruiter::updateKnownIds(this, &idUsed);

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
	void updateWorkerHealth(const UpdateWorkerHealthRequest& req) {
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
	void updateRecoveredWorkers() {
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

	struct DegradationInfo {
		std::unordered_set<NetworkAddress>
		    degradedServers; // The servers that the cluster controller is considered as degraded. The servers in this
		                     // list are not excluded unless they are added to `excludedDegradedServers`.
		std::unordered_set<NetworkAddress>
		    disconnectedServers; // Similar to the above list, but the servers experiencing connection issue.

		bool degradedSatellite = false; // Indicates that the entire satellite DC is degraded.
	};
	// Returns a list of servers who are experiencing degraded links. These are candidates to perform exclusion. Note
	// that only one endpoint of a bad link will be included in this list.
	DegradationInfo getDegradationInfo() {
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
	bool transactionSystemContainsDegradedServers() const {
		const ServerDBInfo& dbi = db.serverInfo->get();
		auto transactionWorkerInList =
		    [&dbi](const std::unordered_set<NetworkAddress>& serverList, bool skipSatellite, bool skipRemote) -> bool {
			for (const auto& server : serverList) {
				if (dbi.master.addresses().contains(server)) {
					return true;
				}

				for (const auto& logSet : dbi.logSystemConfig.tLogs) {
					if (skipSatellite && logSet.locality == tagLocalitySatellite) {
						continue;
					}

					if (skipRemote && !logSet.isLocal) {
						continue;
					}

					if (!logSet.isLocal) {
						// Only check log routers in the remote region.
						for (const auto& logRouter : logSet.logRouters) {
							if (logRouter.present() && logRouter.interf().addresses().contains(server)) {
								return true;
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

		// Check if transaction system contains degraded/disconnected servers. For satellite and remote regions, we only
		// check for disconnection since the latency between prmary and satellite is across WAN and may not be very
		// stable.
		return transactionWorkerInList(degradationInfo.degradedServers, /*skipSatellite=*/true, /*skipRemote=*/true) ||
		       transactionWorkerInList(degradationInfo.disconnectedServers,
		                               /*skipSatellite=*/false,
		                               /*skipRemote=*/!SERVER_KNOBS->CC_ENABLE_REMOTE_LOG_ROUTER_MONITORING);
	}

	// Whether transaction system in the remote DC, e.g. log router and tlogs in the remote DC, contains degraded
	// servers.
	bool remoteTransactionSystemContainsDegradedServers() const {
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
	bool remoteDCIsHealthy() const {
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
	bool shouldTriggerRecoveryDueToDegradedServers() {
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
	bool shouldTriggerFailoverDueToDegradedServers() {
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

	int recentRecoveryCountDueToHealth() {
		while (!recentHealthTriggeredRecoveryTime.empty() &&
		       now() - recentHealthTriggeredRecoveryTime.front() > SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL) {
			recentHealthTriggeredRecoveryTime.pop();
		}
		return recentHealthTriggeredRecoveryTime.size();
	}

	std::map<Optional<Standalone<StringRef>>, WorkerInfo> id_worker;
	std::map<Optional<Standalone<StringRef>>, ProcessClass>
	    id_class; // contains the mapping from process id to process class from the database
	RangeResult lastProcessClasses;
	bool gotProcessClasses;
	bool gotFullyRecoveredConfig;
	bool shouldCommitSuicide;
	Optional<Standalone<StringRef>> masterProcessId;
	Optional<Standalone<StringRef>> clusterControllerProcessId;
	Optional<Standalone<StringRef>> clusterControllerDcId;
	AsyncVar<Optional<std::vector<Optional<Key>>>> desiredDcIds; // desired DC priorities
	AsyncVar<std::pair<bool, Optional<std::vector<Optional<Key>>>>>
	    changingDcIds; // current DC priorities to change first, and whether that is the cluster controller
	AsyncVar<std::pair<bool, Optional<std::vector<Optional<Key>>>>>
	    changedDcIds; // current DC priorities to change second, and whether the cluster controller has been changed
	const UID id;
	Reference<AsyncVar<Optional<UID>>> clusterId;
	ActorCollection ac;
	UpdateWorkerList updateWorkerList;
	Future<Void> outstandingRequestChecker;
	AsyncTrigger updateDBInfo;
	std::set<Endpoint> updateDBInfoEndpoints;
	std::set<Endpoint> removedDBInfoEndpoints;

	DBInfo db;
	Database cx;
	Future<Void> goodRecruitmentTime;
	Future<Void> goodRemoteRecruitmentTime;
	Version datacenterVersionDifference;
	PromiseStream<Future<Void>> addActor;
	bool versionDifferenceUpdated;

	bool remoteDCMonitorStarted;
	bool remoteTransactionSystemDegraded;

	Recruiter recruiter;

	// recruitX is used to signal when role X needs to be (re)recruited.
	// recruitingXID is used to track the ID of X's interface which is being recruited.
	// We use AsyncVars to kill (i.e. halt) singletons that have been replaced.
	double lastRecruitTime = 0;
	AsyncVar<bool> recruitDistributor;
	Optional<UID> recruitingDistributorID;
	AsyncVar<bool> recruitRatekeeper;
	Optional<UID> recruitingRatekeeperID;
	AsyncVar<bool> recruitBlobManager;
	Optional<UID> recruitingBlobManagerID;
	AsyncVar<bool> recruitBlobMigrator;
	Optional<UID> recruitingBlobMigratorID;
	AsyncVar<bool> recruitEncryptKeyProxy;
	Optional<UID> recruitingEncryptKeyProxyID;
	AsyncVar<bool> recruitConsistencyScan;
	Optional<UID> recruitingConsistencyScanID;

	// Stores the health information from a particular worker's perspective.
	struct WorkerHealth {
		struct DegradedTimes {
			double startTime = 0;
			double lastRefreshTime = 0;
		};
		std::unordered_map<NetworkAddress, DegradedTimes> degradedPeers;
		std::unordered_map<NetworkAddress, DegradedTimes> disconnectedPeers;

		// TODO(zhewu): Include disk and CPU signals.
	};
	std::unordered_map<NetworkAddress, WorkerHealth> workerHealth;
	DegradationInfo degradationInfo;
	std::unordered_set<NetworkAddress>
	    excludedDegradedServers; // The degraded servers to be excluded when assigning workers to roles.
	std::queue<double> recentHealthTriggeredRecoveryTime;

	// Capture cluster's Encryption data at-rest mode; the status is set 'only' at the time of cluster creation.
	// The promise gets set as part of cluster recovery process and is used by recovering encryption participant
	// stateful processes (such as TLog) to ensure the stateful process on-disk encryption status matches with cluster's
	// encryption status.
	Promise<EncryptionAtRestMode> encryptionAtRestMode;

	CounterCollection clusterControllerMetrics;

	Counter openDatabaseRequests;
	Counter registerWorkerRequests;
	Counter getWorkersRequests;
	Counter getClientWorkersRequests;
	Counter registerMasterRequests;
	Counter statusRequests;

	Reference<EventCacheHolder> recruitedMasterWorkerEventHolder;

	ClusterControllerData(ClusterControllerFullInterface const& ccInterface,
	                      LocalityData const& locality,
	                      ServerCoordinators const& coordinators,
	                      Reference<AsyncVar<Optional<UID>>> clusterId)
	  : gotProcessClasses(false), gotFullyRecoveredConfig(false), shouldCommitSuicide(false),
	    clusterControllerProcessId(locality.processId()), clusterControllerDcId(locality.dcId()), id(ccInterface.id()),
	    clusterId(clusterId), ac(false), outstandingRequestChecker(Void()), goodRecruitmentTime(Never()),
	    goodRemoteRecruitmentTime(Never()), datacenterVersionDifference(0), versionDifferenceUpdated(false),
	    remoteDCMonitorStarted(false), remoteTransactionSystemDegraded(false), recruiter(id), recruitDistributor(false),
	    recruitRatekeeper(false), recruitBlobManager(false), recruitBlobMigrator(false), recruitEncryptKeyProxy(false),
	    recruitConsistencyScan(false), clusterControllerMetrics("ClusterController", id.toString()),
	    openDatabaseRequests("OpenDatabaseRequests", clusterControllerMetrics),
	    registerWorkerRequests("RegisterWorkerRequests", clusterControllerMetrics),
	    getWorkersRequests("GetWorkersRequests", clusterControllerMetrics),
	    getClientWorkersRequests("GetClientWorkersRequests", clusterControllerMetrics),
	    registerMasterRequests("RegisterMasterRequests", clusterControllerMetrics),
	    statusRequests("StatusRequests", clusterControllerMetrics),
	    recruitedMasterWorkerEventHolder(makeReference<EventCacheHolder>("RecruitedMasterWorker")) {
		auto serverInfo = ServerDBInfo();
		serverInfo.id = deterministicRandom()->randomUniqueID();
		serverInfo.infoGeneration = ++db.dbInfoCount;
		serverInfo.masterLifetime.ccID = id;
		serverInfo.clusterInterface = ccInterface;
		serverInfo.myLocality = locality;
		db.serverInfo->set(serverInfo);
		cx = openDBOnServer(db.serverInfo, TaskPriority::DefaultEndpoint, LockAware::True);

		specialCounter(clusterControllerMetrics, "ClientCount", [this]() { return db.clientCount; });
	}

	~ClusterControllerData() {
		ac.clear(false);
		id_worker.clear();
	}
};

#include "flow/unactorcompiler.h"

#endif
