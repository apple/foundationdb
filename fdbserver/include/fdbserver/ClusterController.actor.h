/*
 * ClusterController.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_CLUSTERCONTROLLER_ACTOR_G_H)
#define FDBSERVER_CLUSTERCONTROLLER_ACTOR_G_H
#include "fdbserver/ClusterController.actor.g.h"
#elif !defined(FDBSERVER_CLUSTERCONTROLLER_ACTOR_H)
#define FDBSERVER_CLUSTERCONTROLLER_ACTOR_H
#pragma once

#include <utility>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/MetaclusterRegistration.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/BlobMigratorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/SystemMonitor.h"

#include "metacluster/MetaclusterMetrics.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct WorkerInfo : NonCopyable {
	Future<Void> watcher;
	ReplyPromise<RegisterWorkerReply> reply;
	Generation gen;
	int reboots;
	ProcessClass initialClass;
	ClusterControllerPriorityInfo priorityInfo;
	WorkerDetails details;
	Future<Void> haltRatekeeper;
	Future<Void> haltDistributor;
	Future<Void> haltBlobManager;
	Future<Void> haltBlobMigrator;
	Future<Void> haltEncryptKeyProxy;
	Future<Void> haltConsistencyScan;
	Standalone<VectorRef<StringRef>> issues;

	WorkerInfo()
	  : gen(-1), reboots(0),
	    priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown) {}
	WorkerInfo(Future<Void> watcher,
	           ReplyPromise<RegisterWorkerReply> reply,
	           Generation gen,
	           WorkerInterface interf,
	           ProcessClass initialClass,
	           ProcessClass processClass,
	           ClusterControllerPriorityInfo priorityInfo,
	           bool degraded,
	           bool recoveredDiskFiles,
	           Standalone<VectorRef<StringRef>> issues)
	  : watcher(watcher), reply(reply), gen(gen), reboots(0), initialClass(initialClass), priorityInfo(priorityInfo),
	    details(interf, processClass, degraded, recoveredDiskFiles), issues(issues) {}

	WorkerInfo(WorkerInfo&& r) noexcept
	  : watcher(std::move(r.watcher)), reply(std::move(r.reply)), gen(r.gen), reboots(r.reboots),
	    initialClass(r.initialClass), priorityInfo(r.priorityInfo), details(std::move(r.details)),
	    haltRatekeeper(r.haltRatekeeper), haltDistributor(r.haltDistributor), haltBlobManager(r.haltBlobManager),
	    haltEncryptKeyProxy(r.haltEncryptKeyProxy), haltConsistencyScan(r.haltConsistencyScan), issues(r.issues) {}
	void operator=(WorkerInfo&& r) noexcept {
		watcher = std::move(r.watcher);
		reply = std::move(r.reply);
		gen = r.gen;
		reboots = r.reboots;
		initialClass = r.initialClass;
		priorityInfo = r.priorityInfo;
		details = std::move(r.details);
		haltRatekeeper = r.haltRatekeeper;
		haltDistributor = r.haltDistributor;
		haltBlobManager = r.haltBlobManager;
		haltEncryptKeyProxy = r.haltEncryptKeyProxy;
		issues = r.issues;
	}
};

struct WorkerFitnessInfo {
	WorkerDetails worker;
	ProcessClass::Fitness fitness;
	int used;

	WorkerFitnessInfo() : fitness(ProcessClass::NeverAssign), used(0) {}
	WorkerFitnessInfo(WorkerDetails worker, ProcessClass::Fitness fitness, int used)
	  : worker(worker), fitness(fitness), used(used) {}
};

struct RecruitWorkersInfo : ReferenceCounted<RecruitWorkersInfo> {
	RecruitFromConfigurationRequest req;
	RecruitFromConfigurationReply rep;
	AsyncTrigger waitForCompletion;
	Optional<UID> dbgId;

	RecruitWorkersInfo(RecruitFromConfigurationRequest const& req) : req(req) {}
};

struct RecruitRemoteWorkersInfo : ReferenceCounted<RecruitRemoteWorkersInfo> {
	RecruitRemoteFromConfigurationRequest req;
	RecruitRemoteFromConfigurationReply rep;
	AsyncTrigger waitForCompletion;
	Optional<UID> dbgId;

	RecruitRemoteWorkersInfo(RecruitRemoteFromConfigurationRequest const& req) : req(req) {}
};

struct ClusterRecoveryData;

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
		Reference<ClusterRecoveryData> recoveryData;

		DBInfo()
		  : clientInfo(new AsyncVar<ClientDBInfo>()), serverInfo(new AsyncVar<ServerDBInfo>()),
		    masterRegistrationCount(0), dbInfoCount(0), recoveryStalled(false), forceRecovery(false),
		    db(DatabaseContext::create(clientInfo,
		                               Future<Void>(),
		                               LocalityData(),
		                               EnableLocalityLoadBalance::True,
		                               TaskPriority::DefaultEndpoint,
		                               LockAware::True)), // SOMEDAY: Locality!
		    unfinishedRecoveries(0), cachePopulated(false), clientCount(0),
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

	struct WorkerUsage {
		std::bitset<ProcessClass::NoRole> roles;
		unsigned weight = 0;
		unsigned multiplier = 1;

		static unsigned roleWeight(ProcessClass::ClusterRole role) {
			// TO DO: Introduce knobs for different weights of different
			// roles
			return 1;
		}

		void addRole(ProcessClass::ClusterRole role) {
			if (!roles.test(role)) {
				roles.set(role);
				weight += roleWeight(role);
			}
		}

		unsigned getWeight() const { return weight * multiplier; }

		unsigned getUniqueHash() const { return roles.to_ulong(); }

		std::string toString() const {
			std::string roleCodes;

			for (unsigned r = 0; r < ProcessClass::NoRole; r++)
				if (roles.test((ProcessClass::ClusterRole)r)) {
					if (!roleCodes.empty())
						roleCodes.append(",");
					roleCodes.append(Role::get((ProcessClass::ClusterRole)r).abbreviation);
				}
			return roleCodes;
		}
	};

	using WorkerUsages = std::map<Optional<Standalone<StringRef>>, WorkerUsage>;

	bool workerAvailable(WorkerInfo const& worker, bool checkStable) const {
		return (now() - startTime < 2 * FLOW_KNOBS->SERVER_REQUEST_INTERVAL) ||
		       (IFailureMonitor::failureMonitor().getState(worker.details.interf.storage.getEndpoint()).isAvailable() &&
		        (!checkStable || worker.reboots < 2));
	}

	bool isLongLivedStateless(Optional<Key> const& processId) const {
		return (db.serverInfo->get().distributor.present() &&
		        db.serverInfo->get().distributor.get().locality.processId() == processId) ||
		       (db.serverInfo->get().ratekeeper.present() &&
		        db.serverInfo->get().ratekeeper.get().locality.processId() == processId) ||
		       (db.serverInfo->get().blobManager.present() &&
		        db.serverInfo->get().blobManager.get().locality.processId() == processId) ||
		       (db.serverInfo->get().blobMigrator.present() &&
		        db.serverInfo->get().blobMigrator.get().locality.processId() == processId) ||
		       (db.serverInfo->get().client.encryptKeyProxy.present() &&
		        db.serverInfo->get().client.encryptKeyProxy.get().locality.processId() == processId) ||
		       (db.serverInfo->get().consistencyScan.present() &&
		        db.serverInfo->get().consistencyScan.get().locality.processId() == processId);
	}

	WorkerDetails getStorageWorker(RecruitStorageRequest const& req) {
		std::set<Optional<Standalone<StringRef>>> excludedMachines(req.excludeMachines.begin(),
		                                                           req.excludeMachines.end());
		std::set<Optional<Standalone<StringRef>>> includeDCs(req.includeDCs.begin(), req.includeDCs.end());
		std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());

		for (auto& it : id_worker) {
			TraceEvent(SevVerbose, "RecruitStorageTry")
			    .detail("Worker", it.second.details.interf.address())
			    .detail("WorkerAvailable", workerAvailable(it.second, false))
			    .detail("RecoverDiskFiles", it.second.details.recoveredDiskFiles)
			    .detail("NotExcludedMachine", !excludedMachines.count(it.second.details.interf.locality.zoneId()))
			    .detail("IncludeDC",
			            (includeDCs.size() == 0 || includeDCs.count(it.second.details.interf.locality.dcId())))
			    .detail("NotExcludedAddress", !addressExcluded(excludedAddresses, it.second.details.interf.address()))
			    .detail("NotExcludedAddress2",
			            (!it.second.details.interf.secondaryAddress().present() ||
			             !addressExcluded(excludedAddresses, it.second.details.interf.secondaryAddress().get())))
			    .detail("MachineFitnessMatch",
			            it.second.details.processClass.machineClassFitness(ProcessClass::Storage) <=
			                ProcessClass::UnsetFit)
			    .detail("MachineFitness", it.second.details.processClass.machineClassFitness(ProcessClass::Storage));
			if (workerAvailable(it.second, false) && it.second.details.recoveredDiskFiles &&
			    !excludedMachines.count(it.second.details.interf.locality.zoneId()) &&
			    (includeDCs.size() == 0 || includeDCs.count(it.second.details.interf.locality.dcId())) &&
			    !addressExcluded(excludedAddresses, it.second.details.interf.address()) &&
			    (!it.second.details.interf.secondaryAddress().present() ||
			     !addressExcluded(excludedAddresses, it.second.details.interf.secondaryAddress().get())) &&
			    it.second.details.processClass.machineClassFitness(ProcessClass::Storage) <= ProcessClass::UnsetFit) {
				return it.second.details;
			}
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

	// Returns a worker that can be used by a blob worker
	// Note: we restrict the set of possible workers to those in the same DC as the BM/CC
	WorkerDetails getBlobWorker(RecruitBlobWorkerRequest const& req) {
		std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());
		for (auto& it : id_worker) {
			// the worker must be available, have the same dcID as CC,
			// not be one of the excluded addrs from req and have the appropiate fitness
			if (workerAvailable(it.second, false) &&
			    clusterControllerDcId == it.second.details.interf.locality.dcId() &&
			    !addressExcluded(excludedAddresses, it.second.details.interf.address()) &&
			    (!it.second.details.interf.secondaryAddress().present() ||
			     !addressExcluded(excludedAddresses, it.second.details.interf.secondaryAddress().get())) &&
			    it.second.details.processClass.machineClassFitness(ProcessClass::BlobWorker) == ProcessClass::BestFit) {
				return it.second.details;
			}
		}

		throw no_more_servers();
	}

	std::vector<WorkerDetails> getWorkersForSeedServers(
	    DatabaseConfiguration const& conf,
	    Reference<IReplicationPolicy> const& policy,
	    Optional<Optional<Standalone<StringRef>>> const& dcId = Optional<Optional<Standalone<StringRef>>>()) {
		std::map<ProcessClass::Fitness, std::vector<WorkerDetails>> fitness_workers;
		std::vector<WorkerDetails> results;
		Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
		LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*)logServerSet.getPtr();
		bool bCompleted = false;

		for (auto& it : id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness(ProcessClass::Storage);
			if (workerAvailable(it.second, false) && it.second.details.recoveredDiskFiles &&
			    !conf.isExcludedServer(it.second.details.interf.addresses(), it.second.details.interf.locality) &&
			    !isExcludedDegradedServer(it.second.details.interf.addresses()) &&
			    fitness != ProcessClass::NeverAssign &&
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

	// Adds workers to the result such that each field is used in the result set as evenly as possible,
	// with a secondary criteria of minimizing the reuse of zoneIds
	// only add workers which have a field which is already in the result set
	void addWorkersByLowestField(StringRef field,
	                             int desired,
	                             const std::vector<WorkerDetails>& workers,
	                             std::set<WorkerDetails>& resultSet) {
		typedef Optional<Standalone<StringRef>> Field;
		typedef Optional<Standalone<StringRef>> Zone;
		typedef std::tuple<int, bool, Field> FieldCount;
		typedef std::pair<int, Zone> ZoneCount;

		std::priority_queue<FieldCount, std::vector<FieldCount>, std::greater<FieldCount>> fieldQueue;
		std::map<Field, std::priority_queue<ZoneCount, std::vector<ZoneCount>, std::greater<ZoneCount>>>
		    field_zoneQueue;

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

	// Adds workers to the result which minimize the reuse of zoneIds
	void addWorkersByLowestZone(int desired,
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
	void logWorkerUnavailable(const Severity severity,
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
		if (fitness == ProcessClass::GoodFit || fitness == ProcessClass::BestFit ||
		    fitness == ProcessClass::NeverAssign) {
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
	std::vector<WorkerDetails> getWorkersForTlogsComplex(DatabaseConfiguration const& conf,
	                                                     int32_t desired,
	                                                     WorkerUsages& id_used,
	                                                     StringRef field,
	                                                     int minFields,
	                                                     int minPerField,
	                                                     bool allowDegraded,
	                                                     bool checkStable,
	                                                     const std::set<Optional<Key>>& dcIds,
	                                                     const std::vector<UID>& exclusionWorkerIds) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, unsigned>, std::vector<WorkerDetails>> fitness_workers;

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
			if (conf.isExcludedServer(worker_details.interf.addresses(), worker_details.interf.locality)) {
				logWorkerUnavailable(SevInfo,
				                     id,
				                     "complex",
				                     "Worker server is excluded from the cluster",
				                     worker_details,
				                     fitness,
				                     dcIds);
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

			fitness_workers[std::make_tuple(fitness,
			                                id_used[worker_process_id].getWeight(),
			                                isLongLivedStateless(worker_process_id),
			                                id_used[worker_process_id].getUniqueHash())]
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
				    { thisField,
				      std::make_tuple(fitness, used, worker.interf.locality.dcId() == clusterControllerDcId) });
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
		for (auto workerIter = fitness_workers.begin();
		     workerIter != fitness_workers.end() && resultSet.size() < desired;
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
			id_used[result.interf.locality.processId()].addRole(ProcessClass::TLog);
		}

		return std::vector<WorkerDetails>(resultSet.begin(), resultSet.end());
	}

	// Attempt to recruit TLogs without degraded processes and see if it improves the configuration
	std::vector<WorkerDetails> getWorkersForTlogsComplex(DatabaseConfiguration const& conf,
	                                                     int32_t desired,
	                                                     WorkerUsages& id_used,
	                                                     StringRef field,
	                                                     int minFields,
	                                                     int minPerField,
	                                                     bool checkStable,
	                                                     const std::set<Optional<Key>>& dcIds,
	                                                     const std::vector<UID>& exclusionWorkerIds) {
		desired = std::max(desired, minFields * minPerField);
		auto withDegradedUsed = id_used;
		auto withDegraded = getWorkersForTlogsComplex(conf,
		                                              desired,
		                                              withDegradedUsed,
		                                              field,
		                                              minFields,
		                                              minPerField,
		                                              true,
		                                              checkStable,
		                                              dcIds,
		                                              exclusionWorkerIds);
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
			auto withoutDegradedUsed = id_used;
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

	// A TLog recruitment method specialized for single, double, and triple configurations
	// It recruits processes from with unique zoneIds until it reaches the desired amount
	std::vector<WorkerDetails> getWorkersForTlogsSimple(DatabaseConfiguration const& conf,
	                                                    int32_t required,
	                                                    int32_t desired,
	                                                    WorkerUsages& id_used,
	                                                    bool checkStable,
	                                                    const std::set<Optional<Key>>& dcIds,
	                                                    const std::vector<UID>& exclusionWorkerIds) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, bool, bool, unsigned>, std::vector<WorkerDetails>>
		    fitness_workers;

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
			if (conf.isExcludedServer(worker_details.interf.addresses(), worker_details.interf.locality)) {
				logWorkerUnavailable(SevInfo,
				                     id,
				                     "simple",
				                     "Worker server is excluded from the cluster",
				                     worker_details,
				                     fitness,
				                     dcIds);
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
			                                id_used[worker_process_id].getWeight(),
			                                worker_details.degraded,
			                                isLongLivedStateless(worker_process_id),
			                                inCCDC,
			                                id_used[worker_process_id].getUniqueHash())]
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
		for (auto workerIter = fitness_workers.begin();
		     workerIter != fitness_workers.end() && resultSet.size() < desired;
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

		for (auto& result : resultSet)
			id_used[result.interf.locality.processId()].addRole(ProcessClass::TLog);

		return std::vector<WorkerDetails>(resultSet.begin(), resultSet.end());
	}

	// A backup method for TLog recruitment that is used for custom policies, but does a worse job
	// selecting the best workers.
	//   conf:        the database configuration.
	//   required:    the required number of TLog workers to select.
	//   desired:     the desired number of TLog workers to select.
	//   policy:      the TLog replication policy the selection needs to satisfy.
	//   id_used:     keep track of process IDs of selected workers.
	//   checkStable: when true, only select from workers that are considered as stable worker (not rebooted more than
	//                twice recently).
	//   dcIds:       the target data centers the workers are in. The selected workers must all be from these
	//                data centers:
	//   exclusionWorkerIds: the workers to be excluded from the selection.
	std::vector<WorkerDetails> getWorkersForTlogsBackup(
	    DatabaseConfiguration const& conf,
	    int32_t required,
	    int32_t desired,
	    Reference<IReplicationPolicy> const& policy,
	    WorkerUsages& id_used,
	    bool checkStable = false,
	    const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	    const std::vector<UID>& exclusionWorkerIds = {}) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, bool, unsigned>, std::vector<WorkerDetails>>
		    fitness_workers;
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
				logWorkerUnavailable(
				    SevInfo, id, "deprecated", "Worker is not available", worker_details, fitness, dcIds);
				continue;
			}
			if (!worker_details.recoveredDiskFiles) {
				logWorkerUnavailable(
				    SevInfo, id, "deprecated", "Worker disk file recovery unfinished", worker_details, fitness, dcIds);
				continue;
			}
			if (conf.isExcludedServer(worker_details.interf.addresses(), worker_details.interf.locality)) {
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

			fitness_workers[std::make_tuple(fitness,
			                                id_used[worker_process_id].getWeight(),
			                                worker_details.degraded,
			                                inCCDC,
			                                id_used[worker_process_id].getUniqueHash())]
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
				id_used[result.interf.locality.processId()].addRole(ProcessClass::TLog);
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
				id_used[result.interf.locality.processId()].addRole(ProcessClass::TLog);
			}
			return results;
		}

		std::vector<LocalityEntry> bestSet;
		std::vector<LocalityData> tLocalities;

		// We have more than the desired number of processes, so use the policy engine to
		// pick a diverse subset of them
		bCompleted = findBestPolicySet(bestSet,
		                               logServerSet,
		                               policy,
		                               desired,
		                               SERVER_KNOBS->POLICY_RATING_TESTS,
		                               SERVER_KNOBS->POLICY_GENERATIONS);
		ASSERT(bCompleted);
		results.reserve(results.size() + bestSet.size());
		for (auto& entry : bestSet) {
			auto object = logServerMap->getObject(entry);
			ASSERT(object);
			results.push_back(*object);
			tLocalities.push_back(object->interf.locality);
		}
		for (auto& result : results) {
			id_used[result.interf.locality.processId()].addRole(ProcessClass::TLog);
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

	// Selects the best method for TLog recruitment based on the specified policy
	std::vector<WorkerDetails> getWorkersForTlogs(DatabaseConfiguration const& conf,
	                                              int32_t required,
	                                              int32_t desired,
	                                              Reference<IReplicationPolicy> const& policy,
	                                              WorkerUsages& id_used,
	                                              bool checkStable = false,
	                                              const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	                                              const std::vector<UID>& exclusionWorkerIds = {}) {
		desired = std::max(required, desired);
		bool useSimple = false;
		if (policy->name() == "Across") {
			PolicyAcross* pa1 = (PolicyAcross*)policy.getPtr();
			Reference<IReplicationPolicy> embedded = pa1->embeddedPolicy();
			if (embedded->name() == "Across") {
				PolicyAcross* pa2 = (PolicyAcross*)embedded.getPtr();
				if (pa2->attributeKey() == "zoneid" && pa2->embeddedPolicyName() == "One") {
					auto testUsed = id_used;

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
			auto testUsed = id_used;

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
		return getWorkersForTlogsBackup(
		    conf, required, desired, policy, id_used, checkStable, dcIds, exclusionWorkerIds);
	}

	// FIXME: This logic will fallback unnecessarily when usable dcs > 1 because it does not check all combinations of
	// potential satellite locations
	std::vector<WorkerDetails> getWorkersForSatelliteLogs(const DatabaseConfiguration& conf,
	                                                      const RegionInfo& region,
	                                                      const RegionInfo& remoteRegion,
	                                                      WorkerUsages& id_used,
	                                                      bool& satelliteFallback,
	                                                      bool checkStable = false) {
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
				for (int s = startDC;
				     s < std::min<int>(startDC + (satelliteFallback ? region.satelliteTLogUsableDcsFallback
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
					WorkerUsages tmpIdUsed;
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

	ProcessClass::Fitness getBestFitnessForRoleInDatacenter(ProcessClass::ClusterRole role) {
		ProcessClass::Fitness bestFitness = ProcessClass::NeverAssign;
		for (const auto& it : id_worker) {
			if (it.second.priorityInfo.isExcluded ||
			    it.second.details.interf.locality.dcId() != clusterControllerDcId) {
				continue;
			}
			bestFitness = std::min(bestFitness, it.second.details.processClass.machineClassFitness(role));
		}
		return bestFitness;
	}

	WorkerFitnessInfo getWorkerForRoleInDatacenter(Optional<Standalone<StringRef>> const& dcId,
	                                               ProcessClass::ClusterRole role,
	                                               ProcessClass::Fitness unacceptableFitness,
	                                               DatabaseConfiguration const& conf,
	                                               WorkerUsages& id_used,
	                                               std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	                                               bool checkStable = false) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, int, unsigned>, std::vector<WorkerDetails>>
		    fitness_workers;

		for (auto& it : id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness(role);
			if (conf.isExcludedServer(it.second.details.interf.addresses(), it.second.details.interf.locality) ||
			    isExcludedDegradedServer(it.second.details.interf.addresses())) {
				fitness = std::max(fitness, ProcessClass::ExcludeFit);
			}
			if (workerAvailable(it.second, checkStable) && fitness < unacceptableFitness &&
			    it.second.details.interf.locality.dcId() == dcId) {
				auto sharing = preferredSharing.find(it.first);
				fitness_workers[std::make_tuple(fitness,
				                                id_used[it.first].getWeight(),
				                                isLongLivedStateless(it.first),
				                                sharing != preferredSharing.end() ? sharing->second : 1e6,
				                                id_used[it.first].getUniqueHash())]
				    .push_back(it.second.details);
			}
		}

		if (fitness_workers.size()) {
			auto worker = deterministicRandom()->randomChoice(fitness_workers.begin()->second);
			id_used[worker.interf.locality.processId()].addRole(role);
			return WorkerFitnessInfo(worker,
			                         std::max(ProcessClass::GoodFit, std::get<0>(fitness_workers.begin()->first)),
			                         std::get<1>(fitness_workers.begin()->first));
		}

		throw no_more_servers();
	}

	std::vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    int amount,
	    DatabaseConfiguration const& conf,
	    WorkerUsages& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	    Optional<WorkerFitnessInfo> minWorker = Optional<WorkerFitnessInfo>(),
	    bool checkStable = false) {
		// for avoiding recruiting workers with worse fitness
		ProcessClass::Fitness used_fitness = ProcessClass::NeverAssign;
		std::map<std::tuple<ProcessClass::Fitness, int, bool, int, unsigned>, std::vector<WorkerDetails>>
		    fitness_workers;
		std::vector<WorkerDetails> results;
		if (minWorker.present()) {
			used_fitness = minWorker.get().fitness;
			results.push_back(minWorker.get().worker);
		}
		if (amount <= results.size()) {
			return results;
		}

		for (auto& it : id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness(role);
			if (workerAvailable(it.second, checkStable) &&
			    !conf.isExcludedServer(it.second.details.interf.addresses(), it.second.details.interf.locality) &&
			    !isExcludedDegradedServer(it.second.details.interf.addresses()) &&
			    it.second.details.interf.locality.dcId() == dcId &&
			    (!minWorker.present() || (it.second.details.interf.id() != minWorker.get().worker.interf.id()))) {
				auto sharing = preferredSharing.find(it.first);
				fitness_workers[std::make_tuple(fitness,
				                                id_used[it.first].getWeight(),
				                                isLongLivedStateless(it.first),
				                                sharing != preferredSharing.end() ? sharing->second : 1e6,
				                                id_used[it.first].getUniqueHash())]
				    .push_back(it.second.details);
			}
		}

		for (auto& it : fitness_workers) {
			ProcessClass::Fitness next_fitness = std::get<0>(it.first);

			if (next_fitness > used_fitness)
				break; // do not recruit with a greater fitness
			used_fitness = next_fitness;
			deterministicRandom()->randomShuffle(it.second);
			for (int i = 0; i < it.second.size(); i++) {
				results.push_back(it.second[i]);
				id_used[it.second[i].interf.locality.processId()].addRole(role);
				if (results.size() == amount)
					return results;
			}
		}

		return results;
	}

	// Allows the comparison of two different recruitments to determine which one is better
	// Tlog recruitment is different from all the other roles, in that it avoids degraded processes
	// And tried to avoid recruitment in the same DC as the cluster controller
	struct RoleFitness {
		ProcessClass::Fitness bestFit;
		ProcessClass::Fitness worstFit;
		ProcessClass::ClusterRole role;
		int count;
		unsigned worstUsed = 1;
		bool degraded = false;

		RoleFitness(int bestFit, int worstFit, int count, ProcessClass::ClusterRole role)
		  : bestFit((ProcessClass::Fitness)bestFit), worstFit((ProcessClass::Fitness)worstFit), role(role),
		    count(count) {}

		RoleFitness(int fitness, int count, ProcessClass::ClusterRole role)
		  : bestFit((ProcessClass::Fitness)fitness), worstFit((ProcessClass::Fitness)fitness), role(role),
		    count(count) {}

		RoleFitness()
		  : bestFit(ProcessClass::NeverAssign), worstFit(ProcessClass::NeverAssign), role(ProcessClass::NoRole),
		    count(0) {}

		RoleFitness(const std::vector<WorkerDetails>& workers,
		            ProcessClass::ClusterRole role,
		            const WorkerUsages& id_used)
		  : role(role) {
			// Every recruitment will attempt to recruit the preferred amount through GoodFit,
			// So a recruitment which only has BestFit is not better than one that has a GoodFit process
			worstFit = ProcessClass::GoodFit;
			degraded = false;
			bestFit = ProcessClass::NeverAssign;
			worstUsed = 1;
			for (auto& it : workers) {
				auto thisFit = it.processClass.machineClassFitness(role);
				auto thisUsed = id_used.find(it.interf.locality.processId());

				if (thisUsed == id_used.end()) {
					TraceEvent(SevError, "UsedNotFound").detail("ProcessId", it.interf.locality.processId().get());
					ASSERT(false);
				}
				if ((unsigned)thisUsed->second.getWeight() == 0) {
					TraceEvent(SevError, "UsedIsZero").detail("ProcessId", it.interf.locality.processId().get());
					ASSERT(false);
				}

				bestFit = std::min(bestFit, thisFit);

				if (thisFit > worstFit) {
					worstFit = thisFit;
					worstUsed = thisUsed->second.getWeight();
				} else if (thisFit == worstFit) {
					worstUsed = std::max(worstUsed, thisUsed->second.getWeight());
				}
				degraded = degraded || it.degraded;
			}

			count = workers.size();

			// degraded is only used for recruitment of tlogs
			if (role != ProcessClass::TLog) {
				degraded = false;
			}
		}

		bool operator<(RoleFitness const& r) const {
			if (worstFit != r.worstFit)
				return worstFit < r.worstFit;
			if (worstUsed != r.worstUsed)
				return worstUsed < r.worstUsed;
			if (count != r.count)
				return count > r.count;
			if (degraded != r.degraded)
				return r.degraded;
			// FIXME: TLog recruitment process does not guarantee the best fit is not worsened.
			if (role != ProcessClass::TLog && role != ProcessClass::LogRouter && bestFit != r.bestFit)
				return bestFit < r.bestFit;
			return false;
		}
		bool operator>(RoleFitness const& r) const { return r < *this; }
		bool operator<=(RoleFitness const& r) const { return !(*this > r); }
		bool operator>=(RoleFitness const& r) const { return !(*this < r); }

		bool betterCount(RoleFitness const& r) const {
			if (count > r.count)
				return true;
			if (worstFit != r.worstFit)
				return worstFit < r.worstFit;
			if (worstUsed != r.worstUsed)
				return worstUsed < r.worstUsed;
			if (degraded != r.degraded)
				return r.degraded;
			return false;
		}

		bool operator==(RoleFitness const& r) const {
			return worstFit == r.worstFit && worstUsed == r.worstUsed && bestFit == r.bestFit && count == r.count &&
			       degraded == r.degraded;
		}

		std::string toString() const { return format("%d %d %d %d %d", worstFit, worstUsed, count, degraded, bestFit); }
	};

	std::set<Optional<Standalone<StringRef>>> getDatacenters(DatabaseConfiguration const& conf,
	                                                         bool checkStable = false) {
		std::set<Optional<Standalone<StringRef>>> result;
		for (auto& it : id_worker)
			if (workerAvailable(it.second, checkStable) &&
			    !conf.isExcludedServer(it.second.details.interf.addresses(), it.second.details.interf.locality) &&
			    !isExcludedDegradedServer(it.second.details.interf.addresses()))
				result.insert(it.second.details.interf.locality.dcId());
		return result;
	}

	void updateKnownIds(WorkerUsages* id_used) {
		(*id_used)[masterProcessId].addRole(ProcessClass::Master);
		(*id_used)[clusterControllerProcessId].addRole(ProcessClass::ClusterController);
	}

	RecruitRemoteFromConfigurationReply findRemoteWorkersForConfiguration(
	    RecruitRemoteFromConfigurationRequest const& req) {
		RecruitRemoteFromConfigurationReply result;
		WorkerUsages id_used;

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
		    ((RoleFitness(
		          SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredRemoteLogs(), ProcessClass::TLog)
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
	std::pair<RegionInfo, RegionInfo> getPrimaryAndRemoteRegion(const std::vector<RegionInfo>& regions, Key dcId) {
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

	ErrorOr<RecruitFromConfigurationReply> findWorkersForConfigurationFromDC(RecruitFromConfigurationRequest const& req,
	                                                                         Optional<Key> dcId,
	                                                                         bool checkGoodRecruitment) {
		RecruitFromConfigurationReply result;
		WorkerUsages id_used;
		updateKnownIds(&id_used);

		ASSERT(dcId.present());

		std::set<Optional<Key>> primaryDC;
		primaryDC.insert(dcId);
		result.dcId = dcId;

		auto [region, remoteRegion] = getPrimaryAndRemoteRegion(req.configuration.regions, dcId.get());

		if (req.recruitSeedServers) {
			auto primaryStorageServers =
			    getWorkersForSeedServers(req.configuration, req.configuration.storagePolicy, dcId);
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
		      RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS,
		                  req.configuration.getDesiredSatelliteLogs(dcId),
		                  ProcessClass::TLog)
		          .betterCount(RoleFitness(satelliteLogs, ProcessClass::TLog, id_used))) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_COMMIT_PROXY_FITNESS,
		                 req.configuration.getDesiredCommitProxies(),
		                 ProcessClass::CommitProxy)
		         .betterCount(RoleFitness(commit_proxies, ProcessClass::CommitProxy, id_used)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_GRV_PROXY_FITNESS,
		                 req.configuration.getDesiredGrvProxies(),
		                 ProcessClass::GrvProxy)
		         .betterCount(RoleFitness(grv_proxies, ProcessClass::GrvProxy, id_used)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_RESOLVER_FITNESS,
		                 req.configuration.getDesiredResolvers(),
		                 ProcessClass::Resolver)
		         .betterCount(RoleFitness(resolvers, ProcessClass::Resolver, id_used)))) {
			return operation_failed();
		}

		return result;
	}

	RecruitFromConfigurationReply findWorkersForConfigurationDispatch(RecruitFromConfigurationRequest const& req,
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
			auto reply =
			    findWorkersForConfigurationFromDC(req, req.configuration.regions[0].dcId, checkGoodRecruitment);
			if (reply.isError()) {
				throw reply.getError();
			} else if (req.configuration.regions[0].dcId == clusterControllerDcId.get()) {
				return reply.get();
			}
			throw no_more_servers();
		} else {
			RecruitFromConfigurationReply result;
			WorkerUsages id_used;
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
				auto primaryStorageServers =
				    getWorkersForSeedServers(req.configuration, req.configuration.storagePolicy);
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
					auto first_grv_proxy = getWorkerForRoleInDatacenter(dcId,
					                                                    ProcessClass::GrvProxy,
					                                                    ProcessClass::ExcludeFit,
					                                                    req.configuration,
					                                                    used,
					                                                    preferredSharing);
					preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
					auto first_resolver = getWorkerForRoleInDatacenter(dcId,
					                                                   ProcessClass::Resolver,
					                                                   ProcessClass::ExcludeFit,
					                                                   req.configuration,
					                                                   used,
					                                                   preferredSharing);
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
							auto backupWorkers = getWorkersForRoleInDatacenter(
							    dcId, ProcessClass::Backup, nBackup, req.configuration, used);
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
						} else if (fitness == bestFitness &&
						           deterministicRandom()->random01() < 1.0 / ++numEquivalent) {
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
			    (RoleFitness(
			         SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(), ProcessClass::TLog)
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

	void updateIdUsed(const std::vector<WorkerInterface>& workers,
	                  ProcessClass::ClusterRole role,
	                  WorkerUsages& id_used) {
		for (auto& it : workers) {
			id_used[it.locality.processId()].addRole(role);
		}
	}

	void compareWorkers(const DatabaseConfiguration& conf,
	                    const std::vector<WorkerInterface>& first,
	                    WorkerUsages& firstUsed,
	                    const std::vector<WorkerInterface>& second,
	                    WorkerUsages& secondUsed,
	                    ProcessClass::ClusterRole role,
	                    std::string description) {
		std::vector<WorkerDetails> firstDetails;
		for (auto& worker : first) {
			auto w = id_worker.find(worker.locality.processId());
			ASSERT(w != id_worker.end());
			auto const& [_, workerInfo] = *w;
			ASSERT(!conf.isExcludedServer(workerInfo.details.interf.addresses(), workerInfo.details.interf.locality));
			firstDetails.push_back(workerInfo.details);
			//TraceEvent("CompareAddressesFirst").detail(description.c_str(), w->second.details.interf.address());
		}
		RoleFitness firstFitness(firstDetails, role, firstUsed);

		std::vector<WorkerDetails> secondDetails;
		for (auto& worker : second) {
			auto w = id_worker.find(worker.locality.processId());
			ASSERT(w != id_worker.end());
			auto const& [_, workerInfo] = *w;
			ASSERT(!conf.isExcludedServer(workerInfo.details.interf.addresses(), workerInfo.details.interf.locality));
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

	RecruitFromConfigurationReply findWorkersForConfiguration(RecruitFromConfigurationRequest const& req) {
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

					WorkerUsages firstUsed;
					WorkerUsages secondUsed;
					updateKnownIds(&firstUsed);
					updateKnownIds(&secondUsed);

					// auto mworker = id_worker.find(masterProcessId);
					//TraceEvent("CompareAddressesMaster")
					//    .detail("Master",
					//            mworker != id_worker.end() ? mworker->second.details.interf.address() :
					//            NetworkAddress());

					updateIdUsed(rep.tLogs, ProcessClass::TLog, firstUsed);
					updateIdUsed(compare.tLogs, ProcessClass::TLog, secondUsed);
					compareWorkers(
					    req.configuration, rep.tLogs, firstUsed, compare.tLogs, secondUsed, ProcessClass::TLog, "TLog");
					updateIdUsed(rep.satelliteTLogs, ProcessClass::TLog, firstUsed);
					updateIdUsed(compare.satelliteTLogs, ProcessClass::TLog, secondUsed);
					compareWorkers(req.configuration,
					               rep.satelliteTLogs,
					               firstUsed,
					               compare.satelliteTLogs,
					               secondUsed,
					               ProcessClass::TLog,
					               "Satellite");
					updateIdUsed(rep.commitProxies, ProcessClass::CommitProxy, firstUsed);
					updateIdUsed(compare.commitProxies, ProcessClass::CommitProxy, secondUsed);
					updateIdUsed(rep.grvProxies, ProcessClass::GrvProxy, firstUsed);
					updateIdUsed(compare.grvProxies, ProcessClass::GrvProxy, secondUsed);
					updateIdUsed(rep.resolvers, ProcessClass::Resolver, firstUsed);
					updateIdUsed(compare.resolvers, ProcessClass::Resolver, secondUsed);
					updateIdUsed(rep.backupWorkers, ProcessClass::Backup, firstUsed);
					updateIdUsed(compare.backupWorkers, ProcessClass::Backup, secondUsed);
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
	void checkRegions(const std::vector<RegionInfo>& regions) {
		if (desiredDcIds.get().present() && desiredDcIds.get().get().size() == 2 &&
		    desiredDcIds.get().get()[0].get() == regions[0].dcId &&
		    desiredDcIds.get().get()[1].get() == regions[1].dcId) {
			return;
		}

		try {
			WorkerUsages id_used;
			getWorkerForRoleInDatacenter(regions[0].dcId,
			                             ProcessClass::ClusterController,
			                             ProcessClass::ExcludeFit,
			                             db.config,
			                             id_used,
			                             {},
			                             true);
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
				checkRegions(regions);
			}
		}
	}

	void updateIdUsed(const std::vector<WorkerDetails>& workers,
	                  ProcessClass::ClusterRole role,
	                  WorkerUsages& id_used) {
		for (auto& it : workers) {
			id_used[it.interf.locality.processId()].addRole(role);
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
		if (db.config.isExcludedServer(dbi.master.addresses(), dbi.master.locality)) {
			oldMasterFit = std::max(oldMasterFit, ProcessClass::ExcludeFit);
		}

		WorkerUsages id_used;
		WorkerUsages old_id_used;
		id_used[clusterControllerProcessId].addRole(ProcessClass::ClusterController);
		old_id_used[clusterControllerProcessId].addRole(ProcessClass::ClusterController);
		WorkerFitnessInfo mworker = getWorkerForRoleInDatacenter(
		    clusterControllerDcId, ProcessClass::Master, ProcessClass::NeverAssign, db.config, id_used, {}, true);
		auto newMasterFit = mworker.worker.processClass.machineClassFitness(ProcessClass::Master);
		if (db.config.isExcludedServer(mworker.worker.interf.addresses(), mworker.worker.interf.locality)) {
			newMasterFit = std::max(newMasterFit, ProcessClass::ExcludeFit);
		}

		old_id_used[masterWorker->first].addRole(ProcessClass::Master);
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
		updateIdUsed(tlogs, ProcessClass::TLog, old_id_used);
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

		updateIdUsed(satellite_tlogs, ProcessClass::TLog, old_id_used);
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

		updateIdUsed(remote_tlogs, ProcessClass::TLog, old_id_used);
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
		updateIdUsed(log_routers, ProcessClass::LogRouter, old_id_used);
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
		updateIdUsed(commitProxyClasses, ProcessClass::CommitProxy, old_id_used);
		updateIdUsed(grvProxyClasses, ProcessClass::GrvProxy, old_id_used);
		updateIdUsed(resolverClasses, ProcessClass::Resolver, old_id_used);
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
		updateIdUsed(backup_workers, ProcessClass::Backup, old_id_used);
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
	WorkerUsages getUsedIds() {
		WorkerUsages idUsed;
		updateKnownIds(&idUsed);

		auto& dbInfo = db.serverInfo->get();
		for (const auto& tlogset : dbInfo.logSystemConfig.tLogs) {
			for (const auto& tlog : tlogset.tLogs) {
				if (tlog.present()) {
					idUsed[tlog.interf().filteredLocality.processId()].addRole(ProcessClass::TLog);
				}
			}
		}

		for (const CommitProxyInterface& interf : dbInfo.client.commitProxies) {
			ASSERT(interf.processId.present());
			idUsed[interf.processId].addRole(ProcessClass::CommitProxy);
		}
		for (const GrvProxyInterface& interf : dbInfo.client.grvProxies) {
			ASSERT(interf.processId.present());
			idUsed[interf.processId].addRole(ProcessClass::GrvProxy);
		}
		for (const ResolverInterface& interf : dbInfo.resolvers) {
			ASSERT(interf.locality.processId().present());
			idUsed[interf.locality.processId()].addRole(ProcessClass::Resolver);
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
		std::string recoveredPeersString;
		for (int i = 0; i < req.recoveredPeers.size(); ++i) {
			recoveredPeersString += (i == 0 ? "" : " ") + req.recoveredPeers[i].toString();
		}
		TraceEvent("ClusterControllerUpdateWorkerHealth")
		    .detail("WorkerAddress", req.address)
		    .detail("DegradedPeers", degradedPeersString)
		    .detail("DisconnectedPeers", disconnectedPeersString)
		    .detail("RecoveredPeers", recoveredPeersString);

		double currentTime = now();

		// Current `workerHealth` doesn't have any information about the incoming worker. Add the worker into
		// `workerHealth`.
		if (workerHealth.find(req.address) == workerHealth.end()) {
			if (req.degradedPeers.empty() && req.disconnectedPeers.empty()) {
				// This request doesn't report any new degradation. Although there may contain recovered peer, since
				// `workerHealth` doesn't record any information on this address, those recovered peers have already
				// been considered recovered.
				return;
			}

			workerHealth[req.address] = {};
			for (const auto& degradedPeer : req.degradedPeers) {
				workerHealth[req.address].degradedPeers[degradedPeer] = { currentTime, currentTime };
			}

			for (const auto& degradedPeer : req.disconnectedPeers) {
				workerHealth[req.address].disconnectedPeers[degradedPeer] = { currentTime, currentTime };
			}

			// We can return directly here since we just created the health info for this address and there shouldn't be
			// any recovered peers.
			return;
		}

		// The incoming worker already exists in `workerHealth`.

		auto& health = workerHealth[req.address];

		// Remove any recovered peers.
		for (const auto& peer : req.recoveredPeers) {
			TraceEvent("ClusterControllerReceivedPeerRecovering")
			    .suppressFor(10.0)
			    .detail("Worker", req.address)
			    .detail("Peer", peer)
			    .detail("PeerAddress", peer);
			health.degradedPeers.erase(peer);
			health.disconnectedPeers.erase(peer);
		}

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
					TraceEvent("WorkerPeerHealthRecovered")
					    .detail("Worker", workerAddress)
					    .detail("Peer", it->first)
					    .detail("PeerAddress", it->first);
					health.degradedPeers.erase(it++);
				} else {
					++it;
				}
			}
			for (auto it = health.disconnectedPeers.begin(); it != health.disconnectedPeers.end();) {
				if (currentTime - it->second.lastRefreshTime > SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL) {
					TraceEvent("WorkerPeerHealthRecovered")
					    .detail("Worker", workerAddress)
					    .detail("Peer", it->first)
					    .detail("PeerAddress", it->first);
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
	bool transactionSystemContainsDegradedServers();

	// Whether transaction system in the remote DC, e.g. log router and tlogs in the remote DC, contains degraded
	// servers.
	bool remoteTransactionSystemContainsDegradedServers();

	// Returns true if remote DC is healthy and can failover to.
	bool remoteDCIsHealthy() {
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

	bool isExcludedDegradedServer(const NetworkAddressList& a) const {
		for (const auto& server : excludedDegradedServers) {
			if (a.contains(server))
				return true;
		}
		return false;
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
	UID id;
	Reference<AsyncVar<Optional<UID>>> clusterId;
	std::vector<Reference<RecruitWorkersInfo>> outstandingRecruitmentRequests;
	std::vector<Reference<RecruitRemoteWorkersInfo>> outstandingRemoteRecruitmentRequests;
	std::vector<std::pair<RecruitStorageRequest, double>> outstandingStorageRequests;
	std::vector<std::pair<RecruitBlobWorkerRequest, double>> outstandingBlobWorkerRequests;
	ActorCollection ac;
	UpdateWorkerList updateWorkerList;
	Future<Void> outstandingRequestChecker;
	Future<Void> outstandingRemoteRequestChecker;
	AsyncTrigger updateDBInfo;
	std::set<Endpoint> updateDBInfoEndpoints;
	std::set<Endpoint> removedDBInfoEndpoints;
	std::vector<StorageServerMetaInfo> storageStatusInfos;

	DBInfo db;
	Database cx;
	double startTime;
	Future<Void> goodRecruitmentTime;
	Future<Void> goodRemoteRecruitmentTime;
	// Lag between primary and remote log servers.
	Version dcLogServerVersionDifference;
	// Lag between primary and remote storage servers.
	Version dcStorageServerVersionDifference;
	// Max of "dcLogServerVersionDifference" and "dcStorageServerVersionDifference".
	Version datacenterVersionDifference;
	PromiseStream<Future<Void>> addActor;
	bool versionDifferenceUpdated;

	bool remoteDCMonitorStarted;
	bool remoteTransactionSystemDegraded;

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
	    clusterId(clusterId), ac(false), outstandingRequestChecker(Void()), outstandingRemoteRequestChecker(Void()),
	    startTime(now()), goodRecruitmentTime(Never()), goodRemoteRecruitmentTime(Never()),
	    dcLogServerVersionDifference(0), dcStorageServerVersionDifference(0), datacenterVersionDifference(0),
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
