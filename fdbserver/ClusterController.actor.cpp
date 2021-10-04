/*
 * ClusterController.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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
#include <cstddef>
#include <iterator>
#include <map>
#include <set>
#include <vector>

#include "fdbclient/MultiVersionTransaction.h"
#include "fdbserver/ApplyMetadataMutation.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/ConfigBroadcaster.h"
#include "fdbserver/CoordinatedState.h"
#include "fdbserver/CoordinationInterface.h" // copy constructors for ServerCoordinators class
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/DBCoreState.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbserver/LeaderElection.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/Status.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbserver/BackupProgress.actor.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Notified.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/simulator.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/Util.h"
#include "flow/actorcompiler.h" // This must be the last #include.

void failAfter(Future<Void> trigger, Endpoint e);

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
	           Standalone<VectorRef<StringRef>> issues)
	  : watcher(watcher), reply(reply), gen(gen), reboots(0), initialClass(initialClass), priorityInfo(priorityInfo),
	    details(interf, processClass, degraded), issues(issues) {}

	WorkerInfo(WorkerInfo&& r) noexcept
	  : watcher(std::move(r.watcher)), reply(std::move(r.reply)), gen(r.gen), reboots(r.reboots),
	    initialClass(r.initialClass), priorityInfo(r.priorityInfo), details(std::move(r.details)),
	    haltRatekeeper(r.haltRatekeeper), haltDistributor(r.haltDistributor), issues(r.issues) {}
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

		DBInfo()
		  : clientInfo(new AsyncVar<ClientDBInfo>()), serverInfo(new AsyncVar<ServerDBInfo>()),
		    masterRegistrationCount(0), dbInfoCount(0), recoveryStalled(false), forceRecovery(false),
		    db(DatabaseContext::create(clientInfo,
		                               Future<Void>(),
		                               LocalityData(),
		                               EnableLocalityLoadBalance::True,
		                               TaskPriority::DefaultEndpoint,
		                               LockAware::True)), // SOMEDAY: Locality!
		    unfinishedRecoveries(0), logGenerations(0), cachePopulated(false) {}

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

		void clearInterf(ProcessClass::ClassType t) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			if (t == ProcessClass::DataDistributorClass) {
				newInfo.distributor = Optional<DataDistributorInterface>();
			} else if (t == ProcessClass::RatekeeperClass) {
				newInfo.ratekeeper = Optional<RatekeeperInterface>();
			}
			serverInfo->set(newInfo);
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

	bool workerAvailable(WorkerInfo const& worker, bool checkStable) {
		return (now() - startTime < 2 * FLOW_KNOBS->SERVER_REQUEST_INTERVAL) ||
		       (IFailureMonitor::failureMonitor().getState(worker.details.interf.storage.getEndpoint()).isAvailable() &&
		        (!checkStable || worker.reboots < 2));
	}

	bool isLongLivedStateless(Optional<Key> const& processId) {
		return (db.serverInfo->get().distributor.present() &&
		        db.serverInfo->get().distributor.get().locality.processId() == processId) ||
		       (db.serverInfo->get().ratekeeper.present() &&
		        db.serverInfo->get().ratekeeper.get().locality.processId() == processId);
	}

	WorkerDetails getStorageWorker(RecruitStorageRequest const& req) {
		std::set<Optional<Standalone<StringRef>>> excludedMachines(req.excludeMachines.begin(),
		                                                           req.excludeMachines.end());
		std::set<Optional<Standalone<StringRef>>> includeDCs(req.includeDCs.begin(), req.includeDCs.end());
		std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());

		for (auto& it : id_worker)
			if (workerAvailable(it.second, false) &&
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
				if (workerAvailable(it.second, false) &&
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

	std::vector<WorkerDetails> getWorkersForSeedServers(
	    DatabaseConfiguration const& conf,
	    Reference<IReplicationPolicy> const& policy,
	    Optional<Optional<Standalone<StringRef>>> const& dcId = Optional<Optional<Standalone<StringRef>>>()) {
		std::map<ProcessClass::Fitness, vector<WorkerDetails>> fitness_workers;
		std::vector<WorkerDetails> results;
		Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
		LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*)logServerSet.getPtr();
		bool bCompleted = false;

		for (auto& it : id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness(ProcessClass::Storage);
			if (workerAvailable(it.second, false) && !conf.isExcludedServer(it.second.details.interf.addresses()) &&
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
			fieldQueue.push(std::make_tuple(it.second.first, it.second.second, it.first));
		}

		for (auto& it : zone_count) {
			field_zoneQueue[it.second.second].push(std::make_pair(it.second.first, it.first));
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
			zoneQueue.push(std::make_pair(it.second, it.first));
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
	                                                     std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                     StringRef field,
	                                                     int minFields,
	                                                     int minPerField,
	                                                     bool allowDegraded,
	                                                     bool checkStable,
	                                                     const std::set<Optional<Key>>& dcIds,
	                                                     const std::vector<UID>& exclusionWorkerIds) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool>, vector<WorkerDetails>> fitness_workers;

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
			if (conf.isExcludedServer(worker_details.interf.addresses())) {
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

			fitness_workers[std::make_tuple(
			                    fitness, id_used[worker_process_id], isLongLivedStateless(worker_process_id))]
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
			id_used[result.interf.locality.processId()]++;
		}

		return std::vector<WorkerDetails>(resultSet.begin(), resultSet.end());
	}

	// Attempt to recruit TLogs without degraded processes and see if it improves the configuration
	std::vector<WorkerDetails> getWorkersForTlogsComplex(DatabaseConfiguration const& conf,
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

	// A TLog recruitment method specialized for single, double, and triple configurations
	// It recruits processes from with unique zoneIds until it reaches the desired amount
	std::vector<WorkerDetails> getWorkersForTlogsSimple(DatabaseConfiguration const& conf,
	                                                    int32_t required,
	                                                    int32_t desired,
	                                                    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                    bool checkStable,
	                                                    const std::set<Optional<Key>>& dcIds,
	                                                    const std::vector<UID>& exclusionWorkerIds) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, bool, bool>, vector<WorkerDetails>> fitness_workers;

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
			if (conf.isExcludedServer(worker_details.interf.addresses())) {
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
				    SevDebug, id, "complex", "Worker's fitness is NeverAssign", worker_details, fitness, dcIds);
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

		ASSERT(resultSet.size() <= desired);

		for (auto& result : resultSet) {
			id_used[result.interf.locality.processId()]++;
		}

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
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    bool checkStable = false,
	    const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	    const std::vector<UID>& exclusionWorkerIds = {}) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, bool>, vector<WorkerDetails>> fitness_workers;
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
				    SevDebug, id, "complex", "Worker's fitness is NeverAssign", worker_details, fitness, dcIds);
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

	// Selects the best method for TLog recruitment based on the specified policy
	std::vector<WorkerDetails> getWorkersForTlogs(DatabaseConfiguration const& conf,
	                                              int32_t required,
	                                              int32_t desired,
	                                              Reference<IReplicationPolicy> const& policy,
	                                              std::map<Optional<Standalone<StringRef>>, int>& id_used,
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
	                                                      std::map<Optional<Standalone<StringRef>>, int>& id_used,
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
	                                               std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                               std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	                                               bool checkStable = false) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, int>, vector<WorkerDetails>> fitness_workers;

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

	vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    int amount,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	    Optional<WorkerFitnessInfo> minWorker = Optional<WorkerFitnessInfo>(),
	    bool checkStable = false) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, int>, vector<WorkerDetails>> fitness_workers;
		vector<WorkerDetails> results;
		if (minWorker.present()) {
			results.push_back(minWorker.get().worker);
		}
		if (amount <= results.size()) {
			return results;
		}

		for (auto& it : id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness(role);
			if (workerAvailable(it.second, checkStable) &&
			    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
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

	// Allows the comparison of two different recruitments to determine which one is better
	// Tlog recruitment is different from all the other roles, in that it avoids degraded processes
	// And tried to avoid recruitment in the same DC as the cluster controller
	struct RoleFitness {
		ProcessClass::Fitness bestFit;
		ProcessClass::Fitness worstFit;
		ProcessClass::ClusterRole role;
		int count;
		int worstUsed = 1;
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

		RoleFitness(const vector<WorkerDetails>& workers,
		            ProcessClass::ClusterRole role,
		            const std::map<Optional<Standalone<StringRef>>, int>& id_used)
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
				if (thisUsed->second == 0) {
					TraceEvent(SevError, "UsedIsZero").detail("ProcessId", it.interf.locality.processId().get());
					ASSERT(false);
				}

				bestFit = std::min(bestFit, thisFit);

				if (thisFit > worstFit) {
					worstFit = thisFit;
					worstUsed = thisUsed->second;
				} else if (thisFit == worstFit) {
					worstUsed = std::max(worstUsed, thisUsed->second);
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
			    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
			    !isExcludedDegradedServer(it.second.details.interf.addresses()))
				result.insert(it.second.details.interf.locality.dcId());
		return result;
	}

	void updateKnownIds(std::map<Optional<Standalone<StringRef>>, int>* id_used) {
		(*id_used)[masterProcessId]++;
		(*id_used)[clusterControllerProcessId]++;
	}

	RecruitRemoteFromConfigurationReply findRemoteWorkersForConfiguration(
	    RecruitRemoteFromConfigurationRequest const& req) {
		RecruitRemoteFromConfigurationReply result;
		std::map<Optional<Standalone<StringRef>>, int> id_used;

		updateKnownIds(&id_used);

		if (req.dbgId.present()) {
			TraceEvent("FindRemoteWorkersForConf", req.dbgId.get())
			    .detail("RemoteDcId", req.dcId)
			    .detail("Configuration", req.configuration.toString());
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
			TraceEvent("FindRemoteWorkersForConf_ReturnResult", req.dbgId.get())
			    .detail("RemoteDcId", req.dcId)
			    .detail("Result_RemoteLogs", result.remoteTLogs.size());
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
	                                                                         Optional<Key> dcId) {
		RecruitFromConfigurationReply result;
		std::map<Optional<Standalone<StringRef>>, int> id_used;
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

		if (!goodRecruitmentTime.isReady() &&
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

	RecruitFromConfigurationReply findWorkersForConfigurationDispatch(RecruitFromConfigurationRequest const& req) {
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
				auto reply = findWorkersForConfigurationFromDC(req, regions[0].dcId);
				setPrimaryDesired = true;
				vector<Optional<Key>> dcPriority;
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
				if (!goodRemoteRecruitmentTime.isReady() && regions[1].dcId != clusterControllerDcId.get()) {
					throw operation_failed();
				}

				if (e.code() != error_code_no_more_servers || regions[1].priority < 0) {
					throw;
				}
				TraceEvent(SevWarn, "AttemptingRecruitmentInRemoteDc", id)
				    .detail("SetPrimaryDesired", setPrimaryDesired)
				    .error(e);
				auto reply = findWorkersForConfigurationFromDC(req, regions[1].dcId);
				if (!setPrimaryDesired) {
					vector<Optional<Key>> dcPriority;
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
			vector<Optional<Key>> dcPriority;
			dcPriority.push_back(req.configuration.regions[0].dcId);
			desiredDcIds.set(dcPriority);
			auto reply = findWorkersForConfigurationFromDC(req, req.configuration.regions[0].dcId);
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
				vector<Optional<Key>> dcPriority;
				dcPriority.push_back(bestDC);
				desiredDcIds.set(dcPriority);
				throw no_more_servers();
			}
			// If this cluster controller dies, do not prioritize recruiting the next one in the same DC
			desiredDcIds.set(vector<Optional<Key>>());
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

			if (!goodRecruitmentTime.isReady() &&
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
	                  std::map<Optional<Standalone<StringRef>>, int>& id_used) {
		for (auto& it : workers) {
			id_used[it.locality.processId()]++;
		}
	}

	void compareWorkers(const DatabaseConfiguration& conf,
	                    const std::vector<WorkerInterface>& first,
	                    std::map<Optional<Standalone<StringRef>>, int>& firstUsed,
	                    const std::vector<WorkerInterface>& second,
	                    std::map<Optional<Standalone<StringRef>>, int>& secondUsed,
	                    ProcessClass::ClusterRole role,
	                    std::string description) {
		std::vector<WorkerDetails> firstDetails;
		for (auto& it : first) {
			auto w = id_worker.find(it.locality.processId());
			ASSERT(w != id_worker.end());
			ASSERT(!conf.isExcludedServer(w->second.details.interf.addresses()));
			firstDetails.push_back(w->second.details);
			//TraceEvent("CompareAddressesFirst").detail(description.c_str(), w->second.details.interf.address());
		}
		RoleFitness firstFitness(firstDetails, role, firstUsed);

		std::vector<WorkerDetails> secondDetails;
		for (auto& it : second) {
			auto w = id_worker.find(it.locality.processId());
			ASSERT(w != id_worker.end());
			ASSERT(!conf.isExcludedServer(w->second.details.interf.addresses()));
			secondDetails.push_back(w->second.details);
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
		RecruitFromConfigurationReply rep = findWorkersForConfigurationDispatch(req);
		if (g_network->isSimulated()) {
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
				RecruitFromConfigurationReply compare = findWorkersForConfigurationDispatch(req);

				std::map<Optional<Standalone<StringRef>>, int> firstUsed;
				std::map<Optional<Standalone<StringRef>>, int> secondUsed;
				updateKnownIds(&firstUsed);
				updateKnownIds(&secondUsed);

				// auto mworker = id_worker.find(masterProcessId);
				//TraceEvent("CompareAddressesMaster")
				//    .detail("Master",
				//            mworker != id_worker.end() ? mworker->second.details.interf.address() : NetworkAddress());

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
			std::map<Optional<Standalone<StringRef>>, int> id_used;
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

			vector<Optional<Key>> dcPriority;
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
				if (clusterControllerDcId.get() == regions[0].dcId) {
					std::swap(regions[0], regions[1]);
				}
				ASSERT(clusterControllerDcId.get() == regions[1].dcId);
				checkRegions(regions);
			}
		}
	}

	void updateIdUsed(const vector<WorkerDetails>& workers, std::map<Optional<Standalone<StringRef>>, int>& id_used) {
		for (auto& it : workers) {
			id_used[it.interf.locality.processId()]++;
		}
	}

	// FIXME: determine when to fail the cluster controller when a primaryDC has not been set

	// This function returns true when the cluster controller determines it is worth forcing
	// a master recovery in order to change the recruited processes in the transaction subsystem.
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
		    datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
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

	bool isUsedNotMaster(Optional<Key> processId) {
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

	bool onMasterIsBetter(const WorkerDetails& worker, ProcessClass::ClusterRole role) {
		ASSERT(masterProcessId.present());
		const auto& pid = worker.interf.locality.processId();
		if ((role != ProcessClass::DataDistributor && role != ProcessClass::Ratekeeper) ||
		    pid == masterProcessId.get()) {
			return false;
		}
		return isUsedNotMaster(pid);
	}

	std::map<Optional<Standalone<StringRef>>, int> getUsedIds() {
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
	void updateWorkerHealth(const UpdateWorkerHealthRequest& req) {
		std::string degradedPeersString;
		for (int i = 0; i < req.degradedPeers.size(); ++i) {
			degradedPeersString += i == 0 ? "" : " " + req.degradedPeers[i].toString();
		}
		TraceEvent("ClusterControllerUpdateWorkerHealth")
		    .detail("WorkerAddress", req.address)
		    .detail("DegradedPeers", degradedPeersString);

		// `req.degradedPeers` contains the latest peer performance view from the worker. Clear the worker if the
		// requested worker doesn't see any degraded peers.
		if (req.degradedPeers.empty()) {
			workerHealth.erase(req.address);
			return;
		}

		double currentTime = now();

		// Current `workerHealth` doesn't have any information about the incoming worker. Add the worker into
		// `workerHealth`.
		if (workerHealth.find(req.address) == workerHealth.end()) {
			workerHealth[req.address] = {};
			for (const auto& degradedPeer : req.degradedPeers) {
				workerHealth[req.address].degradedPeers[degradedPeer] = { currentTime, currentTime };
			}

			return;
		}

		// The incoming worker already exists in `workerHealth`.

		auto& health = workerHealth[req.address];

		// First, remove any degraded peers recorded in the `workerHealth`, but aren't in the incoming request. These
		// machines network performance should have recovered.
		std::unordered_set<NetworkAddress> recoveredPeers;
		for (const auto& [peer, times] : health.degradedPeers) {
			recoveredPeers.insert(peer);
		}
		for (const auto& peer : req.degradedPeers) {
			if (recoveredPeers.find(peer) != recoveredPeers.end()) {
				recoveredPeers.erase(peer);
			}
		}
		for (const auto& peer : recoveredPeers) {
			health.degradedPeers.erase(peer);
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
		}

		for (auto it = workerHealth.begin(); it != workerHealth.end();) {
			if (it->second.degradedPeers.empty()) {
				TraceEvent("WorkerAllPeerHealthRecovered").detail("Worker", it->first);
				workerHealth.erase(it++);
			} else {
				++it;
			}
		}
	}

	// Returns a list of servers who are experiencing degraded links. These are candidates to perform exclusion. Note
	// that only one endpoint of a bad link will be included in this list.
	std::unordered_set<NetworkAddress> getServersWithDegradedLink() {
		updateRecoveredWorkers();

		// Build a map keyed by measured degraded peer. This map gives the info that who complains a particular server.
		std::unordered_map<NetworkAddress, std::unordered_set<NetworkAddress>> degradedLinkDst2Src;
		double currentTime = now();
		for (const auto& [server, health] : workerHealth) {
			for (const auto& [degradedPeer, times] : health.degradedPeers) {
				if (currentTime - times.startTime < SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL) {
					// This degraded link is not long enough to be considered as degraded.
					continue;
				}
				degradedLinkDst2Src[degradedPeer].insert(server);
			}
		}

		// Sort degraded peers based on the number of workers complaining about it.
		std::vector<std::pair<int, NetworkAddress>> count2DegradedPeer;
		for (const auto& [degradedPeer, complainers] : degradedLinkDst2Src) {
			count2DegradedPeer.push_back({ complainers.size(), degradedPeer });
		}
		std::sort(count2DegradedPeer.begin(), count2DegradedPeer.end(), std::greater<>());

		// Go through all reported degraded peers by decreasing order of the number of complainers. For a particular
		// degraded peer, if a complainer has already be considered as degraded, we skip the current examine degraded
		// peer since there has been one endpoint on the link between degradedPeer and complainer considered as
		// degraded. This is to address the issue that both endpoints on a bad link may be considered as degraded
		// server.
		//
		// For example, if server A is already considered as a degraded server, and A complains B, we won't add B as
		// degraded since A is already considered as degraded.
		std::unordered_set<NetworkAddress> currentDegradedServers;
		for (const auto& [complainerCount, badServer] : count2DegradedPeer) {
			for (const auto& complainer : degradedLinkDst2Src[badServer]) {
				if (currentDegradedServers.find(complainer) == currentDegradedServers.end()) {
					currentDegradedServers.insert(badServer);
					break;
				}
			}
		}

		// For degraded server that are complained by more than SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE, we
		// don't know if it is a hot server, or the network is bad. We remove from the returned degraded server list.
		std::unordered_set<NetworkAddress> currentDegradedServersWithinLimit;
		for (const auto& badServer : currentDegradedServers) {
			if (degradedLinkDst2Src[badServer].size() <= SERVER_KNOBS->CC_DEGRADED_PEER_DEGREE_TO_EXCLUDE) {
				currentDegradedServersWithinLimit.insert(badServer);
			}
		}
		return currentDegradedServersWithinLimit;
	}

	// Returns true when the cluster controller should trigger a recovery due to degraded servers are used in the
	// transaction system in the primary data center.
	bool shouldTriggerRecoveryDueToDegradedServers() {
		if (degradedServers.size() > SERVER_KNOBS->CC_MAX_EXCLUSION_DUE_TO_HEALTH) {
			return false;
		}

		const ServerDBInfo dbi = db.serverInfo->get();
		if (dbi.recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			return false;
		}

		// Do not trigger recovery if the cluster controller is excluded, since the master will change
		// anyways once the cluster controller is moved
		if (id_worker[clusterControllerProcessId].priorityInfo.isExcluded) {
			return false;
		}

		for (const auto& excludedServer : degradedServers) {
			if (dbi.master.addresses().contains(excludedServer)) {
				return true;
			}

			for (auto& logSet : dbi.logSystemConfig.tLogs) {
				if (!logSet.isLocal || logSet.locality == tagLocalitySatellite) {
					continue;
				}
				for (const auto& tlog : logSet.tLogs) {
					if (tlog.present() && tlog.interf().addresses().contains(excludedServer)) {
						return true;
					}
				}
			}

			for (auto& proxy : dbi.client.grvProxies) {
				if (proxy.addresses().contains(excludedServer)) {
					return true;
				}
			}

			for (auto& proxy : dbi.client.commitProxies) {
				if (proxy.addresses().contains(excludedServer)) {
					return true;
				}
			}

			for (auto& resolver : dbi.resolvers) {
				if (resolver.addresses().contains(excludedServer)) {
					return true;
				}
			}
		}

		return false;
	}

	int recentRecoveryCountDueToHealth() {
		while (!recentHealthTriggeredRecoveryTime.empty() &&
		       now() - recentHealthTriggeredRecoveryTime.front() > SERVER_KNOBS->CC_TRACKING_HEALTH_RECOVERY_INTERVAL) {
			recentHealthTriggeredRecoveryTime.pop();
		}
		return recentHealthTriggeredRecoveryTime.size();
	}

	bool isExcludedDegradedServer(const NetworkAddressList& a) {
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
	AsyncVar<Optional<vector<Optional<Key>>>> desiredDcIds; // desired DC priorities
	AsyncVar<std::pair<bool, Optional<vector<Optional<Key>>>>>
	    changingDcIds; // current DC priorities to change first, and whether that is the cluster controller
	AsyncVar<std::pair<bool, Optional<vector<Optional<Key>>>>>
	    changedDcIds; // current DC priorities to change second, and whether the cluster controller has been changed
	UID id;
	std::vector<RecruitFromConfigurationRequest> outstandingRecruitmentRequests;
	std::vector<RecruitRemoteFromConfigurationRequest> outstandingRemoteRecruitmentRequests;
	std::vector<std::pair<RecruitStorageRequest, double>> outstandingStorageRequests;
	ActorCollection ac;
	UpdateWorkerList updateWorkerList;
	Future<Void> outstandingRequestChecker;
	Future<Void> outstandingRemoteRequestChecker;
	AsyncTrigger updateDBInfo;
	AsyncTrigger cancelClusterRecovery;
	std::set<Endpoint> updateDBInfoEndpoints;
	std::set<Endpoint> removedDBInfoEndpoints;

	DBInfo db;
	Database cx;
	double startTime;
	Future<Void> goodRecruitmentTime;
	Future<Void> goodRemoteRecruitmentTime;
	Version datacenterVersionDifference;
	PromiseStream<Future<Void>> addActor;
	bool versionDifferenceUpdated;
	bool recruitingDistributor;
	Optional<UID> recruitingRatekeeperID;
	AsyncVar<bool> recruitRatekeeper;

	// Stores the health information from a particular worker's perspective.
	struct WorkerHealth {
		struct DegradedTimes {
			double startTime = 0;
			double lastRefreshTime = 0;
		};
		std::unordered_map<NetworkAddress, DegradedTimes> degradedPeers;

		// TODO(zhewu): Include disk and CPU signals.
	};
	std::unordered_map<NetworkAddress, WorkerHealth> workerHealth;
	std::unordered_set<NetworkAddress>
	    degradedServers; // The servers that the cluster controller is considered as degraded. The servers in this list
	                     // are not excluded unless they are added to `excludedDegradedServers`.
	std::unordered_set<NetworkAddress>
	    excludedDegradedServers; // The degraded servers to be excluded when assigning workers to roles.
	std::queue<double> recentHealthTriggeredRecoveryTime;

	CounterCollection clusterControllerMetrics;

	Counter openDatabaseRequests;
	Counter registerWorkerRequests;
	Counter getWorkersRequests;
	Counter getClientWorkersRequests;
	Counter registerMasterRequests;
	Counter statusRequests;

	ClusterControllerData(ClusterControllerFullInterface const& ccInterface,
	                      LocalityData const& locality,
	                      ServerCoordinators const& coordinators)
	  : gotProcessClasses(false), gotFullyRecoveredConfig(false), shouldCommitSuicide(false),
	    clusterControllerProcessId(locality.processId()), clusterControllerDcId(locality.dcId()), id(ccInterface.id()),
	    ac(false), outstandingRequestChecker(Void()), outstandingRemoteRequestChecker(Void()), startTime(now()),
	    goodRecruitmentTime(Never()), goodRemoteRecruitmentTime(Never()), datacenterVersionDifference(0),
	    versionDifferenceUpdated(false), recruitingDistributor(false), recruitRatekeeper(false),
	    clusterControllerMetrics("ClusterController", id.toString()),
	    openDatabaseRequests("OpenDatabaseRequests", clusterControllerMetrics),
	    registerWorkerRequests("RegisterWorkerRequests", clusterControllerMetrics),
	    getWorkersRequests("GetWorkersRequests", clusterControllerMetrics),
	    getClientWorkersRequests("GetClientWorkersRequests", clusterControllerMetrics),
	    registerMasterRequests("RegisterMasterRequests", clusterControllerMetrics),
	    statusRequests("StatusRequests", clusterControllerMetrics) {
		auto serverInfo = ServerDBInfo();
		serverInfo.id = deterministicRandom()->randomUniqueID();
		serverInfo.infoGeneration = ++db.dbInfoCount;
		serverInfo.masterLifetime.ccID = id;
		serverInfo.clusterInterface = ccInterface;
		serverInfo.myLocality = locality;
		db.serverInfo->set(serverInfo);
		cx = openDBOnServer(db.serverInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	}

	~ClusterControllerData() {
		ac.clear(false);
		id_worker.clear();
	}
}; // class ClusterControllerData

// Assist Cluster Recovery state machine
namespace ClusterControllerRecovery {

static std::set<int> const& normalClusterRecoveryErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_operation_failed);
		s.insert(error_code_tlog_stopped);
		s.insert(error_code_tlog_failed);
		s.insert(error_code_commit_proxy_failed);
		s.insert(error_code_grv_proxy_failed);
		s.insert(error_code_resolver_failed);
		s.insert(error_code_master_backup_worker_failed);
		s.insert(error_code_recruitment_failed);
		s.insert(error_code_no_more_servers);
		s.insert(error_code_master_recovery_failed);
		s.insert(error_code_coordinated_state_conflict);
		s.insert(error_code_master_max_versions_in_flight);
		s.insert(error_code_worker_removed);
		s.insert(error_code_new_coordinators_timed_out);
		s.insert(error_code_broken_promise);
	}
	return s;
}


struct CommitProxyVersionReplies {
	std::map<uint64_t, GetCommitVersionReply> replies;
	NotifiedVersion latestRequestNum;

	CommitProxyVersionReplies(CommitProxyVersionReplies&& r) noexcept
	  : replies(std::move(r.replies)), latestRequestNum(std::move(r.latestRequestNum)) {}
	void operator=(CommitProxyVersionReplies&& r) noexcept {
		replies = std::move(r.replies);
		latestRequestNum = std::move(r.latestRequestNum);
	}

	CommitProxyVersionReplies() : latestRequestNum(0) {}
};

ACTOR Future<Void> masterTerminateOnConflict(UID dbgid,
                                             Promise<Void> fullyRecovered,
                                             Future<Void> onConflict,
                                             Future<Void> switchedState) {
	choose {
		when(wait(onConflict)) {
			if (!fullyRecovered.isSet()) {
				TraceEvent("MasterTerminated", dbgid).detail("Reason", "Conflict");
				TEST(true); // Coordinated state conflict, master dying
				throw worker_removed();
			}
			return Void();
		}
		when(wait(switchedState)) { return Void(); }
	}
}

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
		// TraceEvent("Write db").detail("PrvTLogs", prevDBState.tLogs.size()).detail("NewTLog", newState.tLogs.size());
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
		Future<Void> onConflict = masterTerminateOnConflict(
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
				TraceEvent("MasterTerminated", self->dbgid).detail("Reason", "CStateChanged");
				TEST(true); // Coordinated state changed between writing and reading, master dying
				throw worker_removed();
			}
			self->switchedState = Promise<Void>();
			self->addActor.send(masterTerminateOnConflict(
			    self->dbgid, self->fullyRecovered, self->cstate.onConflict(), self->switchedState.getFuture()));
		} else {
			self->fullyRecovered.send(Void());
		}

		return Void();
	}
};

struct ClusterRecoveryData : NonCopyable, ReferenceCounted<ClusterRecoveryData> {
	ClusterControllerData *controllerData;

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
	Version version; // The last version assigned to a proxy by getVersion()
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

	Standalone<StringRef> dbId;

	MasterInterface masterInterface;
	ClusterControllerFullInterface
	    clusterController; // If the cluster controller changes, this master will die, so this is immutable.

	ReusableCoordinatedState cstate;
	Promise<Void> cstateUpdated;
	Reference<AsyncVar<ServerDBInfo> const> dbInfo;
	int64_t registrationCount; // Number of different MasterRegistrationRequests sent to clusterController

	RecoveryState recoveryState;

	AsyncVar<Standalone<VectorRef<ResolverMoveRef>>> resolverChanges;
	Version resolverChangesVersion;
	std::set<UID> resolverNeedingChanges;

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

	ClusterRecoveryData(ClusterControllerData* controllerData,
	                    Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
	                    MasterInterface const& masterInterface,
	                    ServerCoordinators const& coordinators,
	                    ClusterControllerFullInterface const& clusterController,
	                    Standalone<StringRef> const& dbId,
	                    PromiseStream<Future<Void>> const& addActor,
	                    bool forceRecovery)

	  : controllerData(controllerData), dbgid(masterInterface.id()), lastEpochEnd(invalidVersion),
	    recoveryTransactionVersion(invalidVersion), lastCommitTime(0), liveCommittedVersion(invalidVersion),
	    databaseLocked(false), minKnownCommittedVersion(invalidVersion), hasConfiguration(false),
	    coordinators(coordinators), version(invalidVersion), lastVersionTime(0), txnStateStore(nullptr),
	    memoryLimit(2e9), dbId(dbId), masterInterface(masterInterface), clusterController(clusterController),
	    cstate(coordinators, addActor, dbgid), dbInfo(dbInfo), registrationCount(0), addActor(addActor),
	    recruitmentStalled(makeReference<AsyncVar<bool>>(false)), forceRecovery(forceRecovery), neverCreated(false),
	    safeLocality(tagLocalityInvalid), primaryLocality(tagLocalityInvalid),
	    cc("ClusterController", dbgid.toString()), changeCoordinatorsRequests("ChangeCoordinatorsRequests", cc),
	    getCommitVersionRequests("GetCommitVersionRequests", cc),
	    backupWorkerDoneRequests("BackupWorkerDoneRequests", cc),
	    getLiveCommittedVersionRequests("GetLiveCommittedVersionRequests", cc),
	    reportLiveCommittedVersionRequests("ReportLiveCommittedVersionRequests", cc) {
		logger = traceCounters("RecoveryMetrics", dbgid, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc, "RecoveryMetrics");
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
			TraceEvent("ClusterRecovery", cluster->id)
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
			TraceEvent("ClusterRecovery", cluster->id).detail("Recruited", fNewMaster.get().get().id());

			// for status tool
			TraceEvent("RecruitedMasterWorker", cluster->id)
			    .detail("Address", fNewMaster.get().get().address())
			    .trackLatest("RecruitedMasterWorker");

			*newMaster = fNewMaster.get().get();

			return Void();

		} else {
			TEST(true); // clusterWatchDatabase() !newMaster.present()
			wait(delay(SERVER_KNOBS->MASTER_SPIN_DELAY));
		}
	}
}

ACTOR Future<Void> newCommitProxies(Reference<ClusterRecoveryData> self, RecruitFromConfigurationReply recr) {
	vector<Future<CommitProxyInterface>> initializationReplies;
	for (int i = 0; i < recr.commitProxies.size(); i++) {
		InitializeCommitProxyRequest req;
		req.master = self->masterInterface;
		req.recoveryCount = self->cstate.myDBState.recoveryCount + 1;
		req.recoveryTransactionVersion = self->recoveryTransactionVersion;
		req.firstProxy = i == 0;
		TraceEvent("CommitProxyReplies", self->dbgid)
		    .detail("WorkerID", recr.commitProxies[i].id())
		    .detail("FirstProxy", req.firstProxy ? "True" : "False");
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(recr.commitProxies[i].commitProxy.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    commit_proxy_failed()));
	}

	vector<CommitProxyInterface> newRecruits = wait(getAll(initializationReplies));
	// It is required for the correctness of COMMIT_ON_FIRST_PROXY that self->commitProxies[0] is the firstCommitProxy.
	self->commitProxies = newRecruits;

	return Void();
}

ACTOR Future<Void> newGrvProxies(Reference<ClusterRecoveryData> self, RecruitFromConfigurationReply recr) {
	vector<Future<GrvProxyInterface>> initializationReplies;
	for (int i = 0; i < recr.grvProxies.size(); i++) {
		InitializeGrvProxyRequest req;
		req.master = self->masterInterface;
		req.recoveryCount = self->cstate.myDBState.recoveryCount + 1;
		TraceEvent("GrvProxyReplies", self->dbgid).detail("WorkerID", recr.grvProxies[i].id());
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(recr.grvProxies[i].grvProxy.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    grv_proxy_failed()));
	}

	vector<GrvProxyInterface> newRecruits = wait(getAll(initializationReplies));
	self->grvProxies = newRecruits;
	return Void();
}

ACTOR Future<Void> newResolvers(Reference<ClusterRecoveryData> self, RecruitFromConfigurationReply recr) {
	vector<Future<ResolverInterface>> initializationReplies;
	for (int i = 0; i < recr.resolvers.size(); i++) {
		InitializeResolverRequest req;
		req.recoveryCount = self->cstate.myDBState.recoveryCount + 1;
		req.commitProxyCount = recr.commitProxies.size();
		req.resolverCount = recr.resolvers.size();
		TraceEvent("ResolverReplies", self->dbgid).detail("WorkerID", recr.resolvers[i].id());
		initializationReplies.push_back(
		    transformErrors(throwErrorOr(recr.resolvers[i].resolver.getReplyUnlessFailedFor(
		                        req, SERVER_KNOBS->TLOG_TIMEOUT, SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY)),
		                    resolver_failed()));
	}

	vector<ResolverInterface> newRecruits = wait(getAll(initializationReplies));
	self->resolvers = newRecruits;

	return Void();
}

ACTOR Future<Void> newTLogServers(Reference<ClusterRecoveryData> self,
                                  RecruitFromConfigurationReply recr,
                                  Reference<ILogSystem> oldLogSystem,
                                  vector<Standalone<CommitTransactionRef>>* initialConfChanges) {
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

		Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers =
		    self->controllerData->findRemoteWorkersForConfiguration(RecruitRemoteFromConfigurationRequest(
		        self->configuration,
		        remoteDcId,
		        recr.tLogs.size() *
		            std::max<int>(1, self->configuration.desiredLogRouterCount / std::max<int>(1, recr.tLogs.size())),
		        exclusionWorkerIds));

		self->primaryLocality = self->dcId_locality[recr.dcId];
		self->logSystem = Reference<ILogSystem>(); // Cancels the actors in the previous log system.
		Reference<ILogSystem> newLogSystem = wait(oldLogSystem->newEpoch(recr,
		                                                                 fRemoteWorkers,
		                                                                 self->configuration,
		                                                                 self->cstate.myDBState.recoveryCount + 1,
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
                                  vector<StorageServerInterface>* servers) {
	// This is only necessary if the database is at version 0
	servers->clear();
	if (self->lastEpochEnd)
		return Void();

	state int idx = 0;
	state std::map<Optional<Value>, Tag> dcId_tags;
	state int8_t nextLocality = 0;
	while (idx < recruits.storageServers.size()) {
		TraceEvent("ClusterRecoveryRecruitingInitialStorageServer", self->dbgid)
		    .detail("CandidateWorker", recruits.storageServers[idx].locality.toString());

		InitializeStorageRequest isr;
		isr.seedTag = dcId_tags.count(recruits.storageServers[idx].locality.dcId())
		                  ? dcId_tags[recruits.storageServers[idx].locality.dcId()]
		                  : Tag(nextLocality, 0);
		isr.storeType = self->configuration.storageServerStoreType;
		isr.reqId = deterministicRandom()->randomUniqueID();
		isr.interfaceId = deterministicRandom()->randomUniqueID();

		ErrorOr<InitializeStorageReply> newServer = wait(recruits.storageServers[idx].storage.tryGetReply(isr));

		if (newServer.isError()) {
			if (!newServer.isError(error_code_recruitment_failed) &&
			    !newServer.isError(error_code_request_maybe_delivered))
				throw newServer.getError();

			TEST(true); // masterserver initial storage recuitment loop failed to get new server
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

Future<Void> waitMasterFailure(MasterInterface const& master) {
	std::vector<Future<Void>> failed;
	failed.push_back(waitFailureClient(master.waitFailure,
	                                   SERVER_KNOBS->TLOG_TIMEOUT,
	                                   -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
	                                   /*trace=*/true));
	ASSERT(failed.size() >= 1);
	return tagError<Void>(quorum(failed, 1), master_recovery_failed());
}

Future<Void> waitCommitProxyFailure(vector<CommitProxyInterface> const& commitProxies) {
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

Future<Void> waitGrvProxyFailure(vector<GrvProxyInterface> const& grvProxies) {
	vector<Future<Void>> failed;
	failed.reserve(grvProxies.size());
	for (int i = 0; i < grvProxies.size(); i++)
		failed.push_back(waitFailureClient(grvProxies[i].waitFailure,
		                                   SERVER_KNOBS->TLOG_TIMEOUT,
		                                   -SERVER_KNOBS->TLOG_TIMEOUT / SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY,
		                                   /*trace=*/true));
	ASSERT(failed.size() >= 1);
	return tagError<Void>(quorum(failed, 1), grv_proxy_failed());
}

Future<Void> waitResolverFailure(vector<ResolverInterface> const& resolvers) {
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
		TraceEvent("TLogRejoinRequestHandler").detail("MasterLifeTime", self->dbInfo->get().masterLifetime.toString());
		req.reply.send(true);
	}
}

// Keeps the coordinated state (cstate) updated as the set of recruited tlogs change through recovery.
ACTOR Future<Void> trackTlogRecovery(Reference<ClusterRecoveryData> self,
                                     Reference<AsyncVar<Reference<ILogSystem>>> oldLogSystems,
                                     Future<Void> minRecoveryDuration) {
	state Future<Void> rejoinRequests = Never();
	state DBRecoveryCount recoverCount = self->cstate.myDBState.recoveryCount + 1;
	state DatabaseConfiguration configuration =
	    self->configuration; // self-configuration can be changed by configurationMonitor so we need a copy

	loop {
		state DBCoreState newState;
		self->logSystem->toCoreState(newState);
		newState.recoveryCount = recoverCount;
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
		    .detail("Expected.tlogs",
		            configuration.expectedLogSets(self->primaryDcId.size() ? self->primaryDcId[0] : Optional<Key>()));
		wait(self->cstate.write(newState, finalUpdate));
		wait(minRecoveryDuration);
		self->logSystem->coreStateWritten(newState);
		if (self->cstateUpdated.canBeSet()) {
			self->cstateUpdated.send(Void());
		}

		if (finalUpdate) {
			self->recoveryState = RecoveryState::FULLY_RECOVERED;
			TraceEvent("ClusterRecoveryState", self->dbgid)
			    .detail("StatusCode", RecoveryStatus::fully_recovered)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::fully_recovered])
			    .detail("FullyRecoveredAtVersion", self->version)
			    .trackLatest("ClusterRecoveryState");

			TraceEvent("ClusterRecoveryGenerations", self->dbgid)
			    .detail("ActiveGenerations", 1)
			    .trackLatest("ClusterRecoveryGenerations");
		} else if (!newState.oldTLogData.size() && self->recoveryState < RecoveryState::STORAGE_RECOVERED) {
			self->recoveryState = RecoveryState::STORAGE_RECOVERED;
			TraceEvent("ClusterRecoveryState", self->dbgid)
			    .detail("StatusCode", RecoveryStatus::storage_recovered)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::storage_recovered])
			    .trackLatest("ClusterRecoveryState");
		} else if (allLogs && self->recoveryState < RecoveryState::ALL_LOGS_RECRUITED) {
			self->recoveryState = RecoveryState::ALL_LOGS_RECRUITED;
			TraceEvent("ClusterRecoveryState", self->dbgid)
			    .detail("StatusCode", RecoveryStatus::all_logs_recruited)
			    .detail("Status", RecoveryStatus::names[RecoveryStatus::all_logs_recruited])
			    .trackLatest("ClusterRecoveryState");
		}

		if (newState.oldTLogData.size() && configuration.repopulateRegionAntiQuorum > 0 &&
		    self->logSystem->remoteStorageRecovered()) {
			TraceEvent(SevWarnAlways, "RecruitmentStalled_RemoteStorageRecovered", self->dbgid).log();
			self->recruitmentStalled->set(true);
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

std::pair<KeyRangeRef, bool> findRange(CoalescedKeyRangeMap<int>& key_resolver,
                                       Standalone<VectorRef<ResolverMoveRef>>& movedRanges,
                                       int src,
                                       int dest) {
	auto ranges = key_resolver.ranges();
	auto prev = ranges.begin();
	auto it = ranges.begin();
	++it;
	if (it == ranges.end()) {
		if (ranges.begin().value() != src ||
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(ranges.begin()->range(), dest)) !=
		        movedRanges.end())
			throw operation_failed();
		return std::make_pair(ranges.begin().range(), true);
	}

	std::set<int> borders;
	// If possible expand an existing boundary between the two resolvers
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && prev->value() == dest &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (it->value() == dest && prev->value() == src &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(prev->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(prev->range(), false);
		}
		if (it->value() == dest)
			borders.insert(prev->value());
		if (prev->value() == dest)
			borders.insert(it->value());
		++prev;
	}

	prev = ranges.begin();
	it = ranges.begin();
	++it;
	// If possible create a new boundry which doesn't exist yet
	for (; it != ranges.end(); ++it) {
		if (it->value() == src && !borders.count(prev->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
		if (prev->value() == src && !borders.count(it->value()) &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(prev->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(prev->range(), false);
		}
		++prev;
	}

	it = ranges.begin();
	for (; it != ranges.end(); ++it) {
		if (it->value() == src &&
		    std::find(movedRanges.begin(), movedRanges.end(), ResolverMoveRef(it->range(), dest)) ==
		        movedRanges.end()) {
			return std::make_pair(it->range(), true);
		}
	}
	throw operation_failed(); // we are already attempting to move all of the data one resolver is assigned, so do not
	                          // move anything
}

ACTOR Future<Void> resolutionBalancing(Reference<ClusterRecoveryData> self) {
	state CoalescedKeyRangeMap<int> key_resolver;
	key_resolver.insert(allKeys, 0);
	loop {
		wait(delay(SERVER_KNOBS->MIN_BALANCE_TIME, TaskPriority::ResolutionMetrics));
		while (self->resolverChanges.get().size())
			wait(self->resolverChanges.onChange());
		state std::vector<Future<ResolutionMetricsReply>> futures;
		for (auto& p : self->resolvers)
			futures.push_back(
			    brokenPromiseToNever(p.metrics.getReply(ResolutionMetricsRequest(), TaskPriority::ResolutionMetrics)));
		wait(waitForAll(futures));
		state IndexedSet<std::pair<int64_t, int>, NoMetric> metrics;

		int64_t total = 0;
		for (int i = 0; i < futures.size(); i++) {
			total += futures[i].get().value;
			metrics.insert(std::make_pair(futures[i].get().value, i), NoMetric());
			//TraceEvent("ResolverMetric").detail("I", i).detail("Metric", futures[i].get());
		}
		if (metrics.lastItem()->first - metrics.begin()->first > SERVER_KNOBS->MIN_BALANCE_DIFFERENCE) {
			try {
				state int src = metrics.lastItem()->second;
				state int dest = metrics.begin()->second;
				state int64_t amount = std::min(metrics.lastItem()->first - total / self->resolvers.size(),
				                                total / self->resolvers.size() - metrics.begin()->first) /
				                       2;
				state Standalone<VectorRef<ResolverMoveRef>> movedRanges;

				loop {
					state std::pair<KeyRangeRef, bool> range = findRange(key_resolver, movedRanges, src, dest);

					ResolutionSplitRequest req;
					req.front = range.second;
					req.offset = amount;
					req.range = range.first;

					ResolutionSplitReply split =
					    wait(brokenPromiseToNever(self->resolvers[metrics.lastItem()->second].split.getReply(
					        req, TaskPriority::ResolutionMetrics)));
					KeyRangeRef moveRange = range.second ? KeyRangeRef(range.first.begin, split.key)
					                                     : KeyRangeRef(split.key, range.first.end);
					movedRanges.push_back_deep(movedRanges.arena(), ResolverMoveRef(moveRange, dest));
					TraceEvent("MovingResolutionRange")
					    .detail("Src", src)
					    .detail("Dest", dest)
					    .detail("Amount", amount)
					    .detail("StartRange", range.first)
					    .detail("MoveRange", moveRange)
					    .detail("Used", split.used)
					    .detail("KeyResolverRanges", key_resolver.size());
					amount -= split.used;
					if (moveRange != range.first || amount <= 0)
						break;
				}
				for (auto& it : movedRanges)
					key_resolver.insert(it.range, it.dest);
				// for(auto& it : key_resolver.ranges())
				//	TraceEvent("KeyResolver").detail("Range", it.range()).detail("Value", it.value());

				self->resolverChangesVersion = self->version + 1;
				for (auto& p : self->commitProxies)
					self->resolverNeedingChanges.insert(p.id());
				self->resolverChanges.set(movedRanges);
			} catch (Error& e) {
				if (e.code() != error_code_operation_failed)
					throw;
			}
		}
	}
}

ACTOR Future<Void> changeCoordinators(Reference<ClusterRecoveryData> self) {
	loop {
		ChangeCoordinatorsRequest req = waitNext(self->clusterController.changeCoordinators.getFuture());
		TraceEvent("ChangeCoordiantor", self->dbgid).log();
		++self->changeCoordinatorsRequests;

		// Kill cluster controller to facilitate coordinator registration update
		ASSERT(!self->controllerData->shouldCommitSuicide);
		self->controllerData->shouldCommitSuicide = true;

		state ChangeCoordinatorsRequest changeCoordinatorsRequest = req;

		while (!self->cstate.previousWrite.isReady()) {
			wait(self->cstate.previousWrite);
			wait(delay(
			    0)); // if a new core state is ready to be written, have that take priority over our finalizing write;
		}

		if (!self->cstate.fullyRecovered.isSet()) {
			wait(self->cstate.write(self->cstate.myDBState, true));
		}

		try {
			wait(self->cstate.move(ClusterConnectionString(changeCoordinatorsRequest.newConnectionString.toString())));
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
				if (conf != self->configuration) {
					if (self->recoveryState != RecoveryState::ALL_LOGS_RECRUITED &&
					    self->recoveryState != RecoveryState::FULLY_RECOVERED) {
						self->controllerData->shouldCommitSuicide = true;
						throw master_recovery_failed();
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
		                    master_backup_worker_failed()));
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
			    master_backup_worker_failed()));
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
				TEST(true); // old master attempted to change logsKey
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

Future<Void> sendMasterRegistration(ClusterRecoveryData* self,
                                    LogSystemConfig const& logSystemConfig,
                                    vector<CommitProxyInterface> commitProxies,
                                    vector<GrvProxyInterface> grvProxies,
                                    vector<ResolverInterface> resolvers,
                                    DBRecoveryCount recoveryCount,
                                    vector<UID> priorCommittedLogServers) {
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
		} else {
			updateLogsKey = updateLogsValue(self, cx);
			wait(sendMasterRegistration(self.getPtr(),
			                            logSystemConfig,
			                            self->commitProxies,
			                            self->grvProxies,
			                            self->resolvers,
			                            self->cstate.myDBState.recoveryCount,
			                            vector<UID>()));
		}
	}
}

ACTOR Future<Standalone<CommitTransactionRef>> provisionalMaster(Reference<ClusterRecoveryData> parent,
                                                                 Future<Void> activate) {
	wait(activate);

	TraceEvent("ClusterRecovery provisional master").log();

	// Register a fake commit proxy (to be provided right here) to make ourselves available to clients
	parent->provisionalCommitProxies = vector<CommitProxyInterface>(1);
	parent->provisionalCommitProxies[0].provisional = true;
	parent->provisionalCommitProxies[0].initEndpoints();
	parent->provisionalGrvProxies = vector<GrvProxyInterface>(1);
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
			if (req.flags & GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY && parent->lastEpochEnd) {
				GetReadVersionReply rep;
				rep.version = parent->lastEpochEnd;
				rep.locked = locked;
				rep.metadataVersion = metadataVersion;
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
					if (isMetadataMutation(*m)) {
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
		when(wait(waitCommitProxyFailure)) { throw worker_removed(); }
		when(wait(waitGrvProxyFailure)) { throw worker_removed(); }
	}
}

ACTOR Future<vector<Standalone<CommitTransactionRef>>> recruitEverything(Reference<ClusterRecoveryData> self,
                                                                         vector<StorageServerInterface>* seedServers,
                                                                         Reference<ILogSystem> oldLogSystem) {
	if (!self->configuration.isValid()) {
		RecoveryStatus::RecoveryStatus status;
		if (self->configuration.initialized) {
			TraceEvent(SevWarn, "ClusterRecoveryInvalidConfiguration", self->dbgid)
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
		TraceEvent("ClusterRecoveryState", self->dbgid)
		    .detail("StatusCode", status)
		    .detail("Status", RecoveryStatus::names[status])
		    .trackLatest("ClusterRecoveryState");
		return Never();
	} else
		TraceEvent("ClusterRecoveryState", self->dbgid)
		    .detail("StatusCode", RecoveryStatus::recruiting_transaction_servers)
		    .detail("Status", RecoveryStatus::names[RecoveryStatus::recruiting_transaction_servers])
		    .detail("Conf", self->configuration.toString())
		    .detail("RequiredCommitProxies", 1)
		    .detail("RequiredGrvProxies", 1)
		    .detail("RequiredResolvers", 1)
		    .trackLatest("ClusterRecoveryState");

	// FIXME: we only need log routers for the same locality as the master
	int maxLogRouters = self->cstate.prevDBState.logRouterTags;
	for (auto& old : self->cstate.prevDBState.oldTLogData) {
		maxLogRouters = std::max(maxLogRouters, old.logRouterTags);
	}

	state RecruitFromConfigurationReply recruits = self->controllerData->findWorkersForConfiguration(
	        RecruitFromConfigurationRequest(self->configuration, self->lastEpochEnd == 0, maxLogRouters));

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

	TraceEvent("ClusterRecoveryState", self->dbgid)
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
	    .trackLatest("ClusterRecoveryState");

	// Actually, newSeedServers does both the recruiting and initialization of the seed servers; so if this is a brand
	// new database we are sort of lying that we are past the recruitment phase.  In a perfect world we would split that
	// up so that the recruitment part happens above (in parallel with recruiting the transaction servers?).
	wait(newSeedServers(self, recruits, seedServers));
	state vector<Standalone<CommitTransactionRef>> confChanges;
	wait(newCommitProxies(self, recruits) && newGrvProxies(self, recruits) && newResolvers(self, recruits) &&
	     newTLogServers(self, recruits, oldLogSystem, &confChanges));

	// Update recoveryTransactionVersion to the master interface
	wait(brokenPromiseToNever(self->masterInterface.updateRecoveryData.getReply(
	    UpdateRecoveryDataRequest(self->recoveryTransactionVersion, self->lastEpochEnd, self->commitProxies)))); 

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
	    updateLocalityForDcId(self->controllerData->clusterControllerDcId.get(), oldLogSystem, myLocality);
	// Peek the txnStateTag in oldLogSystem and recover self->txnStateStore

	// For now, we also obtain the recovery metadata that the log system obtained during the end_epoch process for
	// comparison

	// Sets self->lastEpochEnd and self->recoveryTransactionVersion
	// Sets self->configuration to the configuration (FF/conf/ keys) at self->lastEpochEnd

	// Recover transaction state store
	if (self->txnStateStore)
		self->txnStateStore->close();
	self->txnStateLogAdapter = openDiskQueueAdapter(oldLogSystem, myLocality, txsPoppedVersion);
	self->txnStateStore =
	    keyValueStoreLogSystem(self->txnStateLogAdapter, self->dbgid, self->memoryLimit, false, false, true);

	// Versionstamped operations (particularly those applied from DR) define a minimum commit version
	// that we may recover to, as they embed the version in user-readable data and require that no
	// transactions will be committed at a lower version.
	Optional<Standalone<StringRef>> requiredCommitVersion =
	    wait(self->txnStateStore->readValue(minRequiredCommitVersionKey));
	Version minRequiredCommitVersion = -1;
	if (requiredCommitVersion.present()) {
		minRequiredCommitVersion = BinaryReader::fromStringRef<Version>(requiredCommitVersion.get(), Unversioned());
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

		if (BUGGIFY) {
			self->recoveryTransactionVersion +=
			    deterministicRandom()->randomInt64(0, SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT);
		}
		if (self->recoveryTransactionVersion < minRequiredCommitVersion)
			self->recoveryTransactionVersion = minRequiredCommitVersion;
	}

	TraceEvent("ClusterRecovering", self->dbgid)
	    .detail("LastEpochEnd", self->lastEpochEnd)
	    .detail("RecoveryTransactionVersion", self->recoveryTransactionVersion);

	RangeResult rawConf = wait(self->txnStateStore->readRange(configKeys));
	self->configuration.fromKeyValues(rawConf.castTo<VectorRef<KeyValueRef>>());
	self->originalConfiguration = self->configuration;
	self->hasConfiguration = true;

	TraceEvent("ClusterRecoveredConfig", self->dbgid)
	    .setMaxEventLength(11000)
	    .setMaxFieldLength(10000)
	    .detail("Conf", self->configuration.toString())
	    .trackLatest("RecoveredConfig");

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
			txnReplies = vector<Future<Void>>();
			dataOutstanding = 0;
		}

		wait(yield());
	}
	wait(waitForAll(txnReplies));
	TraceEvent("RecoveryInternal", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::recovery_transaction)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::recovery_transaction])
	    .detail("Step", "SentTxnStateStoreToCommitProxies");

	vector<Future<ResolveTransactionBatchReply>> replies;
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
                                   vector<Standalone<CommitTransactionRef>>* initialConfChanges) {
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
	    MutationRef(MutationRef::SetValue, configKeysPrefix.toString() + "usable_regions", LiteralStringRef("1")));
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
                               vector<StorageServerInterface>* seedServers,
                               vector<Standalone<CommitTransactionRef>>* initialConfChanges,
                               Future<Version> poppedTxsVersion) {
	TraceEvent("ClusterRecoveryState", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::reading_transaction_system_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::reading_transaction_system_state])
	    .trackLatest("ClusterRecoveryState");
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


	debug_checkMaxRestoredVersion(UID(), self->lastEpochEnd, "DBRecovery");

	// Ordinarily we pass through this loop once and recover.  We go around the loop if recovery stalls for more than a
	// second, a provisional master is initialized, and an "emergency transaction" is submitted that might change the
	// configuration so that we can finish recovery.

	state std::map<Optional<Value>, int8_t> originalLocalityMap = self->dcId_locality;
	state Future<vector<Standalone<CommitTransactionRef>>> recruitments =
	    recruitEverything(self, seedServers, oldLogSystem);
	state double provisionalDelay = SERVER_KNOBS->PROVISIONAL_START_DELAY;
	loop {
		state Future<Standalone<CommitTransactionRef>> provisional = provisionalMaster(self, delay(provisionalDelay));
		provisionalDelay =
		    std::min(SERVER_KNOBS->PROVISIONAL_MAX_DELAY, provisionalDelay * SERVER_KNOBS->PROVISIONAL_DELAY_GROWTH);
		choose {
			when(vector<Standalone<CommitTransactionRef>> confChanges = wait(recruitments)) {
				initialConfChanges->insert(initialConfChanges->end(), confChanges.begin(), confChanges.end());
				provisional.cancel();
				break;
			}
			when(Standalone<CommitTransactionRef> _req = wait(provisional)) {
				state Standalone<CommitTransactionRef> req = _req; // mutable
				TEST(true); // Emergency transaction processing during recovery
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
	TraceEvent("ClusterRecoveryState", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::reading_coordinated_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::reading_coordinated_state])
	    .trackLatest("ClusterRecoveryState");

	wait(self->cstate.read());

	self->recoveryState = RecoveryState::LOCKING_CSTATE;
	TraceEvent("ClusterRecoveryState", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::locking_coordinated_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::locking_coordinated_state])
	    .detail("TLogs", self->cstate.prevDBState.tLogs.size())
	    .detail("ActiveGenerations", self->cstate.myDBState.oldTLogData.size() + 1)
	    .detail("MyRecoveryCount", self->cstate.prevDBState.recoveryCount + 2)
	    .detail("ForceRecovery", self->forceRecovery)
	    .trackLatest("ClusterRecoveryState");
	// for (const auto& old : self->cstate.prevDBState.oldTLogData) {
	//	TraceEvent("BWReadCoreState", self->dbgid).detail("Epoch", old.epoch).detail("Version", old.epochEnd);
	//}

	TraceEvent("ClusterRecoveryGenerations", self->dbgid)
	    .detail("ActiveGenerations", self->cstate.myDBState.oldTLogData.size() + 1)
	    .trackLatest("ClusterRecoveryGenerations");

	if (self->cstate.myDBState.oldTLogData.size() > CLIENT_KNOBS->MAX_GENERATIONS_OVERRIDE) {
		if (self->cstate.myDBState.oldTLogData.size() >= CLIENT_KNOBS->MAX_GENERATIONS) {
			TraceEvent(SevError, "RecoveryStoppedTooManyOldGenerations")
			    .detail("OldGenerations", self->cstate.myDBState.oldTLogData.size())
			    .detail("Reason",
			            "Recovery stopped because too many recoveries have happened since the last time the cluster "
			            "was fully_recovered. Set --knob_max_generations_override on your server processes to a value "
			            "larger than OldGenerations to resume recovery once the underlying problem has been fixed.");
			wait(Future<Void>(Never()));
		} else if (self->cstate.myDBState.oldTLogData.size() > CLIENT_KNOBS->RECOVERY_DELAY_START_GENERATION) {
			TraceEvent(SevError, "RecoveryDelayedTooManyOldGenerations")
			    .detail("OldGenerations", self->cstate.myDBState.oldTLogData.size())
			    .detail("Reason",
			            "Recovery is delayed because too many recoveries have happened since the last time the cluster "
			            "was fully_recovered. Set --knob_max_generations_override on your server processes to a value "
			            "larger than OldGenerations to resume recovery once the underlying problem has been fixed.");
			wait(delay(CLIENT_KNOBS->RECOVERY_DELAY_SECONDS_PER_GENERATION *
			           (self->cstate.myDBState.oldTLogData.size() - CLIENT_KNOBS->RECOVERY_DELAY_START_GENERATION)));
		}
		if (g_network->isSimulated() && self->cstate.myDBState.oldTLogData.size() > CLIENT_KNOBS->MAX_GENERATIONS_SIM) {
			g_simulator.connectionFailuresDisableDuration = 1e6;
			g_simulator.speedUpSimulation = true;
			TraceEvent(SevWarnAlways, "DisableConnectionFailures_TooManyGenerations").log();
		}
	}

	state Reference<AsyncVar<Reference<ILogSystem>>> oldLogSystems(new AsyncVar<Reference<ILogSystem>>);
	state Future<Void> recoverAndEndEpoch = ILogSystem::recoverAndEndEpoch(oldLogSystems,
	                                                                       self->dbgid,
	                                                                       self->cstate.prevDBState,
	                                                                       self->clusterController.tlogRejoin.getFuture(),
	                                                                       self->controllerData->db.serverInfo->get().myLocality,
	                                                                       &self->forceRecovery);

	DBCoreState newState = self->cstate.myDBState;
	newState.recoveryCount++;
	wait(self->cstate.write(newState) || recoverAndEndEpoch);

	self->recoveryState = RecoveryState::RECRUITING;

	state vector<StorageServerInterface> seedServers;
	state vector<Standalone<CommitTransactionRef>> initialConfChanges;
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
			when(wait(reg)) { throw internal_error(); }
			when(wait(recoverAndEndEpoch)) { throw internal_error(); }
		}
	}

	if (self->neverCreated) {
		recoverStartTime = now();
	}

	recoverAndEndEpoch.cancel();

	ASSERT(self->commitProxies.size() <= self->configuration.getDesiredCommitProxies());
	ASSERT(self->commitProxies.size() >= 1);
	ASSERT(self->grvProxies.size() <= self->configuration.getDesiredGrvProxies());
	ASSERT(self->grvProxies.size() >= 1);
	ASSERT(self->resolvers.size() <= self->configuration.getDesiredResolvers());
	ASSERT(self->resolvers.size() >= 1);

	self->recoveryState = RecoveryState::RECOVERY_TRANSACTION;
	TraceEvent("ClusterRecoveryState", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::recovery_transaction)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::recovery_transaction])
	    .detail("PrimaryLocality", self->primaryLocality)
	    .detail("DcId", self->controllerData->clusterControllerDcId.get())
	    .trackLatest("ClusterRecoveryState");

	// Recovery transaction
	state bool debugResult = debug_checkMinRestoredVersion(UID(), self->lastEpochEnd, "DBRecovery", SevWarn);

	state UID debugId = deterministicRandom()->randomUniqueID();
	CommitTransactionRequest recoveryCommitRequest;
	recoveryCommitRequest.flags = recoveryCommitRequest.flags | CommitTransactionRequest::FLAG_IS_LOCK_AWARE;
	CommitTransactionRef& tr = recoveryCommitRequest.transaction;
	recoveryCommitRequest.debugID = debugId;
	TraceEvent("ClusterRecoveryCommitReq", debugId)
	    .detail("RecoveryDbgId", self->dbgid)
	    .detail("MasterLifetime", self->masterInterface.id().toString());
	int mmApplied = 0; // The number of mutations in tr.mutations that have been applied to the txnStateStore so far
	if (self->lastEpochEnd != 0) {
		Optional<Value> snapRecoveryFlag = self->txnStateStore->readValue(writeRecoveryKey).get();
		TraceEvent("ClusterRecoverySnapshotCheck")
		    .detail("SnapRecoveryFlag", snapRecoveryFlag.present() ? snapRecoveryFlag.get().toString() : "N/A")
		    .detail("LastEpochEnd", self->lastEpochEnd);
		if (snapRecoveryFlag.present()) {
			TEST(true); // Recovering from snapshot, writing to snapShotEndVersionKey
			BinaryWriter bw(Unversioned());
			tr.set(recoveryCommitRequest.arena, snapshotEndVersionKey, (bw << self->lastEpochEnd).toValue());
			// Pause the backups that got restored in this snapshot to avoid data corruption
			// Requires further operational work to abort the backup
			TraceEvent("ClusterRecoveryPauseBackupAgents").log();
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
	tr.set(recoveryCommitRequest.arena, coordinatorsKey, self->coordinators.ccf->getConnectionString().toString());
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

	applyMetadataMutations(SpanID(),
	                       self->dbgid,
	                       recoveryCommitRequest.arena,
	                       tr.mutations.slice(mmApplied, tr.mutations.size()),
	                       self->txnStateStore);
	mmApplied = tr.mutations.size();

	tr.read_snapshot = self->recoveryTransactionVersion; // lastEpochEnd would make more sense, but isn't in the initial
	                                                     // window of the resolver(s)

	TraceEvent("ClusterRecoveryCommit", self->dbgid).log();
	state Future<ErrorOr<CommitID>> recoveryCommit = self->commitProxies[0].commit.tryGetReply(recoveryCommitRequest);
	self->addActor.send(self->logSystem->onError());
	self->addActor.send(waitMasterFailure(self->masterInterface));
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
		TEST(true); // Cluster recovery failed because of the initial commit failed
		throw master_recovery_failed();
	}

	ASSERT(self->recoveryTransactionVersion != 0);

	self->recoveryState = RecoveryState::WRITING_CSTATE;
	TraceEvent("ClusterRecoveryState", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::writing_coordinated_state)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::writing_coordinated_state])
	    .detail("TLogList", self->logSystem->describe())
		.detail("RecoveryTxnVersion", self->recoveryTransactionVersion)
	    .trackLatest("ClusterRecoveryState");

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
	TraceEvent("AdvanceTxnVersion", self->dbgid)
	    .detail("Min", self->recoveryTransactionVersion)
	    .detail("Max", self->recoveryTransactionVersion);
	debug_advanceMinCommittedVersion(UID(), self->recoveryTransactionVersion);

	if (debugResult) {
		TraceEvent(self->forceRecovery ? SevWarn : SevError, "DBRecoveryDurabilityError").log();
	}

	TraceEvent("ClusterRecoveryCommittedTLogs", self->dbgid)
	    .detail("TLogs", self->logSystem->describe())
	    .detail("RecoveryCount", self->cstate.myDBState.recoveryCount)
	    .detail("RecoveryTransactionVersion", self->recoveryTransactionVersion);

	TraceEvent(recoveryInterval.end(), self->dbgid)
	    .detail("RecoveryTransactionVersion", self->recoveryTransactionVersion);

	self->recoveryState = RecoveryState::ACCEPTING_COMMITS;
	double recoveryDuration = now() - recoverStartTime;

	TraceEvent((recoveryDuration > 4 && !g_network->isSimulated()) ? SevWarnAlways : SevInfo,
	           "ClusterRecoveryDuration",
	           self->dbgid)
	    .detail("RecoveryDuration", recoveryDuration)
	    .trackLatest("ClusterRecoveryDuration");

	TraceEvent("ClusterRecoveryState", self->dbgid)
	    .detail("StatusCode", RecoveryStatus::accepting_commits)
	    .detail("Status", RecoveryStatus::names[RecoveryStatus::accepting_commits])
	    .detail("StoreType", self->configuration.storageServerStoreType)
	    .detail("RecoveryDuration", recoveryDuration)
	    .trackLatest("ClusterRecoveryState");

	TraceEvent("ClusterRecoveryAvailable", self->dbgid)
	    .detail("AvailableAtVersion", self->version)
	    .trackLatest("ClusterRecoveryAvailable");

	if ( self->resolvers.size() > 1)
		self->addActor.send(resolutionBalancing(self));

	self->addActor.send(changeCoordinators(self));
	Database cx = openDBOnServer(self->dbInfo, TaskPriority::DefaultEndpoint, LockAware::True);
	self->addActor.send(configurationMonitor(self, cx));
	if (self->configuration.backupWorkerEnabled) {
		self->addActor.send(recruitBackupWorkers(self, cx));
	} else {
		self->logSystem->setOldestBackupEpoch(self->cstate.myDBState.recoveryCount);
	}

	TraceEvent("ClusterRecovery end", self->dbgid).log();

	wait(Future<Void>(Never()));
	throw internal_error();
}

ACTOR Future<Void> cleanupActorCollection(Reference<ClusterRecoveryData> self, bool exThrown) {
	if (self.isValid()) {
		wait(delay(0.0));

		// printf("freeing up actors %p exThrown %d\n", self.getPtr(), exThrown);
		while (!self->addActor.isEmpty()) {
			self->addActor.getFuture().pop();
		}
	}

	return Void();
}

} // namespace ClusterControllerRecovery

ACTOR Future<Void> handleLeaderReplacement(Reference<ClusterControllerRecovery::ClusterRecoveryData> self) {
	loop {
		choose {
			when(wait(self->controllerData->cancelClusterRecovery.onTrigger())) {
				throw coordinators_changed();
			}
		}
	}
}

ACTOR Future<Void> clusterWatchDatabase(ClusterControllerData* cluster,
                                        ClusterControllerData::DBInfo* db,
                                        ServerCoordinators coordinators,
                                         Future<Void> leaderFail) {
	state MasterInterface iMaster;
	state Reference<ClusterControllerRecovery::ClusterRecoveryData> recoveryData;
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> recoveryCore;

	// SOMEDAY: If there is already a non-failed master referenced by zkMasterInfo, use that one until it fails
	// When this someday is implemented, make sure forced failures still cause the master to be recruited again
	loop {
		TraceEvent("CCWDB", cluster->id).log();
		try {
			state double recoveryStart = now();
			state MasterInterface newMaster;

			if (1) { //!SERVER_KNOBS->CLUSTERRECOVERY_CONTROLLER_DRIVEN_RECOVERY) {
				TraceEvent("CCWDB", cluster->id).detail("Recruiting", "Master");
				wait(ClusterControllerRecovery::recruitNewMaster(cluster, db, &newMaster));
			} else {
				// advertise ClusterController as Master interface to assist in cluster recovery
				// master role recruitment is done like any other cluster process
				newMaster.locality = db->serverInfo->get().myLocality;
				newMaster.initEndpoints();

				DUMPTOKEN_PROCESS(newMaster, newMaster.waitFailure);
				DUMPTOKEN_PROCESS(newMaster, newMaster.getCommitVersion);
				DUMPTOKEN_PROCESS(newMaster, newMaster.getLiveCommittedVersion);
				DUMPTOKEN_PROCESS(newMaster, newMaster.reportLiveCommittedVersion);

				TraceEvent("ClusterRecovery init master interface", cluster->id)
				    .detail("Address", iMaster.address())
				    .trackLatest("ClusterRecovery");
			}

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
			dbInfo.latencyBandConfig = db->serverInfo->get().latencyBandConfig;
			dbInfo.myLocality = db->serverInfo->get().myLocality;
			dbInfo.client = ClientDBInfo();

			TraceEvent("CCWDB", cluster->id)
			    .detail("Lifetime", dbInfo.masterLifetime.toString())
			    .detail("ChangeID", dbInfo.id);
			db->serverInfo->set(dbInfo);

			state Future<Void> spinDelay = delay(
			    SERVER_KNOBS
			        ->MASTER_SPIN_DELAY); // Don't retry master recovery more than once per second, but don't delay
			                              // the "first" recovery after more than a second of normal operation

			TraceEvent("CCWDB", cluster->id).detail("Watching", iMaster.id());

			state Future<Void> collection;

			if (SERVER_KNOBS->CLUSTERRECOVERY_CONTROLLER_DRIVEN_RECOVERY) {
				recoveryData = makeReference<ClusterControllerRecovery::ClusterRecoveryData>(
				    cluster,
				    db->serverInfo,
				    db->serverInfo->get().master,
				    coordinators,
				    db->serverInfo->get().clusterInterface,
				    LiteralStringRef(""),
				    addActor,
				    db->forceRecovery);

				recoveryData->addActor.send(handleLeaderReplacement(recoveryData));
				collection = actorCollection(recoveryData->addActor.getFuture());
				recoveryCore = ClusterControllerRecovery::clusterRecoveryCore(recoveryData);
			} else {
				recoveryCore = Never();
			}

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
					TraceEvent(SevWarn, "DetectedFailedMaster", cluster->id).detail("OldMaster", iMaster.id());
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
					TraceEvent("BackupWorkerDoneRequest", cluster->id).log();
				}
				when(wait(collection)) { throw internal_error(); }
			}

			recoveryCore.cancel();
			wait(ClusterControllerRecovery::cleanupActorCollection(recoveryData, false /* exThrown */));
			ASSERT(addActor.isEmpty());

			wait(spinDelay);
			TEST(true); // clusterWatchDatabase() recovery failed
			TraceEvent(SevWarn, "DetectedFailedRecovery", cluster->id).detail("OldMaster", iMaster.id());
		} catch (Error& e) {
			state Error err = e;
			TraceEvent("ClusterRecovery failed", cluster->id).error(e, true).detail("Master", iMaster.id());
			if (e.code() != error_code_actor_cancelled) {
				wait(delay(0.0));
			}

			recoveryCore.cancel();
			wait(ClusterControllerRecovery::cleanupActorCollection(recoveryData, true /* exThrown */));
			ASSERT(addActor.isEmpty());

			TEST(err.code() == error_code_tlog_failed);                 // Terminated due to tLog failure
			TEST(err.code() == error_code_commit_proxy_failed);         // Terminated due to commit proxy failure
			TEST(err.code() == error_code_grv_proxy_failed);            // Terminated due to GRV proxy failure
			TEST(err.code() == error_code_resolver_failed);             // Terminated due to resolver failure
			TEST(err.code() == error_code_master_backup_worker_failed); // Terminated due to backup worker failure
			TEST(err.code() == error_code_operation_failed); 			// Terminated due to failed operation

			if (cluster->shouldCommitSuicide || err.code() == error_code_coordinators_changed) {
				TraceEvent("CCTerminateCoordinatorChanged", cluster->id)
				    .error(err, true)
				    .detail("ClusterController", recoveryData->clusterController.id());

				throw coordinators_changed();
			}

			if (ClusterControllerRecovery::normalClusterRecoveryErrors().count(err.code())) {
				TraceEvent(SevWarn, "ClusterRecoveryRetrying", cluster->id).error(err);
			} else {
				bool ok = err.code() == error_code_no_more_servers;
				TraceEvent(ok ? SevWarn : SevError, "ClusterWatchDatabaseRetrying", cluster->id).error(err);
				if (!ok) {
					throw err;
				}
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
			when(wait(delayJittered(300))) { break; } // The server might be long gone!
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
		RecruitFromConfigurationRequest& req = self->outstandingRecruitmentRequests[i];
		try {
			RecruitFromConfigurationReply rep = self->findWorkersForConfiguration(req);
			req.reply.send(rep);
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
		RecruitRemoteFromConfigurationRequest& req = self->outstandingRemoteRecruitmentRequests[i];
		try {
			RecruitRemoteFromConfigurationReply rep = self->findRemoteWorkersForConfiguration(req);
			req.reply.send(rep);
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
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i)
				    .detail("IsCriticalRecruitment", req.first.criticalRecruitment)
				    .error(e);
			} else {
				TraceEvent(SevError, "RecruitStorageError", self->id).error(e);
				throw;
			}
		}
	}
}

void checkBetterDDOrRK(ClusterControllerData* self) {
	if (!self->masterProcessId.present() ||
	    self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		return;
	}

	std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
	WorkerDetails newRKWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
	                                                               ProcessClass::Ratekeeper,
	                                                               ProcessClass::NeverAssign,
	                                                               self->db.config,
	                                                               id_used,
	                                                               {},
	                                                               true)
	                                .worker;
	if (self->onMasterIsBetter(newRKWorker, ProcessClass::Ratekeeper)) {
		newRKWorker = self->id_worker[self->masterProcessId.get()].details;
	}
	id_used = self->getUsedIds();
	for (auto& it : id_used) {
		it.second *= 2;
	}
	id_used[newRKWorker.interf.locality.processId()]++;
	WorkerDetails newDDWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
	                                                               ProcessClass::DataDistributor,
	                                                               ProcessClass::NeverAssign,
	                                                               self->db.config,
	                                                               id_used,
	                                                               {},
	                                                               true)
	                                .worker;
	if (self->onMasterIsBetter(newDDWorker, ProcessClass::DataDistributor)) {
		newDDWorker = self->id_worker[self->masterProcessId.get()].details;
	}
	auto bestFitnessForRK = newRKWorker.processClass.machineClassFitness(ProcessClass::Ratekeeper);
	if (self->db.config.isExcludedServer(newRKWorker.interf.addresses())) {
		bestFitnessForRK = std::max(bestFitnessForRK, ProcessClass::ExcludeFit);
	}
	auto bestFitnessForDD = newDDWorker.processClass.machineClassFitness(ProcessClass::DataDistributor);
	if (self->db.config.isExcludedServer(newDDWorker.interf.addresses())) {
		bestFitnessForDD = std::max(bestFitnessForDD, ProcessClass::ExcludeFit);
	}
	//TraceEvent("CheckBetterDDorRKNewRecruits", self->id).detail("MasterProcessId", self->masterProcessId)
	//.detail("NewRecruitRKProcessId", newRKWorker.interf.locality.processId()).detail("NewRecruiteDDProcessId",
	// newDDWorker.interf.locality.processId());

	Optional<Standalone<StringRef>> currentRKProcessId;
	Optional<Standalone<StringRef>> currentDDProcessId;

	auto& db = self->db.serverInfo->get();
	bool ratekeeperHealthy = false;
	if (db.ratekeeper.present() && self->id_worker.count(db.ratekeeper.get().locality.processId()) &&
	    (!self->recruitingRatekeeperID.present() || (self->recruitingRatekeeperID.get() == db.ratekeeper.get().id()))) {
		auto& rkWorker = self->id_worker[db.ratekeeper.get().locality.processId()];
		currentRKProcessId = rkWorker.details.interf.locality.processId();
		auto rkFitness = rkWorker.details.processClass.machineClassFitness(ProcessClass::Ratekeeper);
		if (rkWorker.priorityInfo.isExcluded) {
			rkFitness = ProcessClass::ExcludeFit;
		}
		if (self->isUsedNotMaster(rkWorker.details.interf.locality.processId()) || bestFitnessForRK < rkFitness ||
		    (rkFitness == bestFitnessForRK && rkWorker.details.interf.locality.processId() == self->masterProcessId &&
		     newRKWorker.interf.locality.processId() != self->masterProcessId)) {
			TraceEvent("CCHaltRK", self->id)
			    .detail("RKID", db.ratekeeper.get().id())
			    .detail("Excluded", rkWorker.priorityInfo.isExcluded)
			    .detail("Fitness", rkFitness)
			    .detail("BestFitness", bestFitnessForRK);
			self->recruitRatekeeper.set(true);
		} else {
			ratekeeperHealthy = true;
		}
	}

	if (!self->recruitingDistributor && db.distributor.present() &&
	    self->id_worker.count(db.distributor.get().locality.processId())) {
		auto& ddWorker = self->id_worker[db.distributor.get().locality.processId()];
		auto ddFitness = ddWorker.details.processClass.machineClassFitness(ProcessClass::DataDistributor);
		currentDDProcessId = ddWorker.details.interf.locality.processId();
		if (ddWorker.priorityInfo.isExcluded) {
			ddFitness = ProcessClass::ExcludeFit;
		}
		if (self->isUsedNotMaster(ddWorker.details.interf.locality.processId()) || bestFitnessForDD < ddFitness ||
		    (ddFitness == bestFitnessForDD && ddWorker.details.interf.locality.processId() == self->masterProcessId &&
		     newDDWorker.interf.locality.processId() != self->masterProcessId) ||
		    (ddFitness == bestFitnessForDD &&
		     newRKWorker.interf.locality.processId() != newDDWorker.interf.locality.processId() && ratekeeperHealthy &&
		     currentRKProcessId.present() && currentDDProcessId == currentRKProcessId &&
		     (newRKWorker.interf.locality.processId() != self->masterProcessId &&
		      newDDWorker.interf.locality.processId() != self->masterProcessId))) {
			TraceEvent("CCHaltDD", self->id)
			    .detail("DDID", db.distributor.get().id())
			    .detail("Excluded", ddWorker.priorityInfo.isExcluded)
			    .detail("Fitness", ddFitness)
			    .detail("BestFitness", bestFitnessForDD)
			    .detail("CurrentRateKeeperProcessId",
			            currentRKProcessId.present() ? currentRKProcessId.get() : LiteralStringRef("None"))
			    .detail("CurrentDDProcessId", currentDDProcessId)
			    .detail("MasterProcessID", self->masterProcessId)
			    .detail("NewRKWorkers", newRKWorker.interf.locality.processId())
			    .detail("NewDDWorker", newDDWorker.interf.locality.processId());
			ddWorker.haltDistributor = brokenPromiseToNever(
			    db.distributor.get().haltDataDistributor.getReply(HaltDataDistributorRequest(self->id)));
		}
	}
}

ACTOR Future<Void> doCheckOutstandingRequests(ClusterControllerData* self) {
	try {
		wait(delay(SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL));
		while (!self->goodRecruitmentTime.isReady()) {
			wait(self->goodRecruitmentTime);
		}

		checkOutstandingRecruitmentRequests(self);
		checkOutstandingStorageRequests(self);
		checkBetterDDOrRK(self);

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
	state Future<Void> failed =
	    (worker.address() == g_network->getLocalAddress() || startingClass.classType() == ProcessClass::TesterClass)
	        ? Never()
	        : waitFailureClient(worker.waitFailure, SERVER_KNOBS->WORKER_FAILURE_TIME);
	cluster->updateWorkerList.set(worker.locality.processId(),
	                              ProcessData(worker.locality, startingClass, worker.stableAddress()));
	cluster->updateDBInfoEndpoints.insert(worker.updateServerDBInfo.getEndpoint());
	cluster->updateDBInfo.trigger();
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

ACTOR Future<vector<TLogInterface>> requireAll(vector<Future<Optional<vector<TLogInterface>>>> in) {
	state vector<TLogInterface> out;
	state int i;
	for (i = 0; i < in.size(); i++) {
		Optional<vector<TLogInterface>> x = wait(in[i]);
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
			self->outstandingStorageRequests.push_back(std::make_pair(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT));
			TraceEvent(SevWarn, "RecruitStorageNotAvailable", self->id)
			    .detail("IsCriticalRecruitment", req.criticalRecruitment)
			    .error(e);
		} else {
			TraceEvent(SevError, "RecruitStorageError", self->id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

ACTOR Future<Void> clusterRecruitFromConfiguration(ClusterControllerData* self, RecruitFromConfigurationRequest req) {
	// At the moment this doesn't really need to be an actor (it always completes immediately)
	TEST(true); // ClusterController RecruitTLogsRequest
	loop {
		try {
			auto rep = self->findWorkersForConfiguration(req);
			req.reply.send(rep);
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && self->goodRecruitmentTime.isReady()) {
				self->outstandingRecruitmentRequests.push_back(req);
				TraceEvent(SevWarn, "RecruitFromConfigurationNotAvailable", self->id).error(e);
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
				throw; // goodbye, cluster controller
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
	}
}

ACTOR Future<Void> clusterRecruitRemoteFromConfiguration(ClusterControllerData* self,
                                                         RecruitRemoteFromConfigurationRequest req) {
	// At the moment this doesn't really need to be an actor (it always completes immediately)
	TEST(true); // ClusterController RecruitTLogsRequest Remote
	loop {
		try {
			RecruitRemoteFromConfigurationReply rep = self->findRemoteWorkersForConfiguration(req);
			req.reply.send(rep);
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && self->goodRemoteRecruitmentTime.isReady()) {
				self->outstandingRemoteRecruitmentRequests.push_back(req);
				TraceEvent(SevWarn, "RecruitRemoteFromConfigurationNotAvailable", self->id).error(e);
				return Void();
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
				throw; // goodbye, cluster controller
			}
		}
		wait(lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY));
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
		self->db.logGenerations = 0;
		ASSERT(!req.logSystemConfig.oldTLogs.size());
	} else {
		self->db.logGenerations = std::max<int>(self->db.logGenerations, req.logSystemConfig.oldTLogs.size());
	}

	db->masterRegistrationCount = req.registrationCount;
	db->recoveryStalled = req.recoveryStalled;
	if (req.configuration.present()) {
		db->config = req.configuration.get();

		if (req.recoveryState >= RecoveryState::ACCEPTING_COMMITS) {
			self->gotFullyRecoveredConfig = true;
			db->fullyRecoveredConfig = req.configuration.get();
			for (auto& it : self->id_worker) {
				bool isExcludedFromConfig =
				    db->fullyRecoveredConfig.isExcludedServer(it.second.details.interf.addresses());
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
	    db->clientInfo->get().grvProxies != req.grvProxies) {
		isChanged = true;
		// TODO why construct a new one and not just copy the old one and change proxies + id?
		ClientDBInfo clientInfo;
		clientInfo.id = deterministicRandom()->randomUniqueID();
		clientInfo.commitProxies = req.commitProxies;
		clientInfo.grvProxies = req.grvProxies;
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

void registerWorker(RegisterWorkerRequest req, ClusterControllerData* self, ConfigBroadcaster* configBroadcaster) {
	const WorkerInterface& w = req.wi;
	ProcessClass newProcessClass = req.processClass;
	auto info = self->id_worker.find(w.locality.processId());
	ClusterControllerPriorityInfo newPriorityInfo = req.priorityInfo;
	newPriorityInfo.processClassFitness = newProcessClass.machineClassFitness(ProcessClass::ClusterController);

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
		    .detail("Workers", self->id_worker.size());
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
		    .detail("Degraded", req.degraded);
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
			newPriorityInfo.isExcluded = self->db.fullyRecoveredConfig.isExcludedServer(w.addresses());
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
		                                                     req.issues);
		if (!self->masterProcessId.present() &&
		    w.locality.processId() == self->db.serverInfo->get().master.locality.processId()) {
			self->masterProcessId = w.locality.processId();
		}
		if (configBroadcaster != nullptr) {
			self->addActor.send(configBroadcaster->registerWorker(
			    req.lastSeenKnobVersion,
			    req.knobConfigClassSet,
			    self->id_worker[w.locality.processId()].watcher,
			    self->id_worker[w.locality.processId()].details.interf.configBroadcastInterface));
		}
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
		info->second.gen = req.generation;
		info->second.issues = req.issues;

		if (info->second.details.interf.id() != w.id()) {
			self->removedDBInfoEndpoints.insert(info->second.details.interf.updateServerDBInfo.getEndpoint());
			info->second.details.interf = w;
			info->second.watcher = workerAvailabilityWatch(w, newProcessClass, self);
		}
		if (configBroadcaster != nullptr) {
			self->addActor.send(
			    configBroadcaster->registerWorker(req.lastSeenKnobVersion,
			                                      req.knobConfigClassSet,
			                                      info->second.watcher,
			                                      info->second.details.interf.configBroadcastInterface));
		}
		checkOutstandingRequests(self);
	} else {
		TEST(true); // Received an old worker registration request.
	}

	if (req.distributorInterf.present() && !self->db.serverInfo->get().distributor.present() &&
	    self->clusterControllerDcId == req.distributorInterf.get().locality.dcId() && !self->recruitingDistributor) {
		const DataDistributorInterface& di = req.distributorInterf.get();
		TraceEvent("CCRegisterDataDistributor", self->id).detail("DDID", di.id());
		self->db.setDistributor(di);
	}
	if (req.ratekeeperInterf.present()) {
		if ((self->recruitingRatekeeperID.present() &&
		     self->recruitingRatekeeperID.get() != req.ratekeeperInterf.get().id()) ||
		    self->clusterControllerDcId != w.locality.dcId()) {
			TraceEvent("CCHaltRegisteringRatekeeper", self->id)
			    .detail("RKID", req.ratekeeperInterf.get().id())
			    .detail("DcID", printable(self->clusterControllerDcId))
			    .detail("ReqDcID", printable(w.locality.dcId()))
			    .detail("RecruitingRKID",
			            self->recruitingRatekeeperID.present() ? self->recruitingRatekeeperID.get() : UID());
			self->id_worker[w.locality.processId()].haltRatekeeper = brokenPromiseToNever(
			    req.ratekeeperInterf.get().haltRatekeeper.getReply(HaltRatekeeperRequest(self->id)));
		} else if (!self->recruitingRatekeeperID.present()) {
			const RatekeeperInterface& rki = req.ratekeeperInterf.get();
			const auto& ratekeeper = self->db.serverInfo->get().ratekeeper;
			TraceEvent("CCRegisterRatekeeper", self->id).detail("RKID", rki.id());
			if (ratekeeper.present() && ratekeeper.get().id() != rki.id() &&
			    self->id_worker.count(ratekeeper.get().locality.processId())) {
				TraceEvent("CCHaltPreviousRatekeeper", self->id)
				    .detail("RKID", ratekeeper.get().id())
				    .detail("DcID", printable(self->clusterControllerDcId))
				    .detail("ReqDcID", printable(w.locality.dcId()))
				    .detail("RecruitingRKID",
				            self->recruitingRatekeeperID.present() ? self->recruitingRatekeeperID.get() : UID());
				self->id_worker[ratekeeper.get().locality.processId()].haltRatekeeper =
				    brokenPromiseToNever(ratekeeper.get().haltRatekeeper.getReply(HaltRatekeeperRequest(self->id)));
			}
			if (!ratekeeper.present() || ratekeeper.get().id() != rki.id()) {
				self->db.setRatekeeper(rki);
			}
		}
	}

	// Notify the worker to register again with new process class/exclusive property
	if (!req.reply.isSet() && newPriorityInfo != req.priorityInfo) {
		req.reply.send(RegisterWorkerReply(newProcessClass, newPriorityInfo));
	}
}

#define TIME_KEEPER_VERSION LiteralStringRef("1")

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

// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid running
// out of space, it limits the max number of entries and clears old entries on each update. This mapping is used from
// backup and restore to get the version information for a timestamp.
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
			vector<WorkerDetails> workers;
			std::vector<ProcessIssues> workerIssues;

			for (auto& it : self->id_worker) {
				workers.push_back(it.second.details);
				if (it.second.issues.size()) {
					workerIssues.push_back(ProcessIssues(it.second.details.interf.address(), it.second.issues));
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
			                                                                  &self->db.clientStatus,
			                                                                  coordinators,
			                                                                  incompatibleConnections,
			                                                                  self->datacenterVersionDifference,
			                                                                  configBroadcaster)));

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

// Monitors the global configuration version key for changes. When changes are
// made, the global configuration history is read and any updates are sent to
// all processes in the system by updating the ClientDBInfo object. The
// GlobalConfig actor class contains the functionality to read the latest
// history and update the processes local view.
ACTOR Future<Void> monitorGlobalConfig(ClusterControllerData::DBInfo* db) {
	loop {
		state ReadYourWritesTransaction tr(db->db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				state Optional<Value> globalConfigVersion = wait(tr.get(globalConfigVersionKey));
				state ClientDBInfo clientInfo = db->serverInfo->get().client;

				if (globalConfigVersion.present()) {
					// Since the history keys end with versionstamps, they
					// should be sorted correctly (versionstamps are stored in
					// big-endian order).
					RangeResult globalConfigHistory =
					    wait(tr.getRange(globalConfigHistoryKeys, CLIENT_KNOBS->TOO_MANY));
					// If the global configuration version key has been set,
					// the history should contain at least one item.
					ASSERT(globalConfigHistory.size() > 0);
					clientInfo.history.clear();

					for (const auto& kv : globalConfigHistory) {
						ObjectReader reader(kv.value.begin(), IncludeVersion());
						if (reader.protocolVersion() != g_network->protocolVersion()) {
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
				bool oldDifferenceTooLarge = !self->versionDifferenceUpdated ||
				                             self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
				self->versionDifferenceUpdated = true;
				self->datacenterVersionDifference = primaryMetrics.get().v - remoteMetrics.get().v;

				if (oldDifferenceTooLarge && self->datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
					checkOutstandingRequests(self);
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
				vector<Optional<Key>> dcPriority;
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

ACTOR Future<DataDistributorInterface> startDataDistributor(ClusterControllerData* self) {
	wait(delay(0.0)); // If master fails at the same time, give it a chance to clear master PID.

	TraceEvent("CCStartDataDistributor", self->id).log();
	loop {
		try {
			state bool no_distributor = !self->db.serverInfo->get().distributor.present();
			while (!self->masterProcessId.present() ||
			       self->masterProcessId != self->db.serverInfo->get().master.locality.processId() ||
			       self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (no_distributor && self->db.serverInfo->get().distributor.present()) {
				return self->db.serverInfo->get().distributor.get();
			}

			std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			WorkerFitnessInfo data_distributor = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId,
			                                                                        ProcessClass::DataDistributor,
			                                                                        ProcessClass::NeverAssign,
			                                                                        self->db.config,
			                                                                        id_used);
			state WorkerDetails worker = data_distributor.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::DataDistributor)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			InitializeDataDistributorRequest req(deterministicRandom()->randomUniqueID());
			TraceEvent("CCDataDistributorRecruit", self->id).detail("Addr", worker.interf.address());

			ErrorOr<DataDistributorInterface> distributor = wait(worker.interf.dataDistributor.getReplyUnlessFailedFor(
			    req, SERVER_KNOBS->WAIT_FOR_DISTRIBUTOR_JOIN_DELAY, 0));
			if (distributor.present()) {
				TraceEvent("CCDataDistributorRecruited", self->id).detail("Addr", worker.interf.address());
				return distributor.get();
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
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		if (self->db.serverInfo->get().distributor.present()) {
			wait(waitFailureClient(self->db.serverInfo->get().distributor.get().waitFailure,
			                       SERVER_KNOBS->DD_FAILURE_TIME));
			TraceEvent("CCDataDistributorDied", self->id)
			    .detail("DistributorId", self->db.serverInfo->get().distributor.get().id());
			self->db.clearInterf(ProcessClass::DataDistributorClass);
		} else {
			self->recruitingDistributor = true;
			DataDistributorInterface distributorInterf = wait(startDataDistributor(self));
			self->recruitingDistributor = false;
			self->db.setDistributor(distributorInterf);
		}
	}
}

ACTOR Future<Void> startRatekeeper(ClusterControllerData* self) {
	wait(delay(0.0)); // If master fails at the same time, give it a chance to clear master PID.

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
				    self->id_worker.count(ratekeeper.get().locality.processId())) {
					TraceEvent("CCHaltRatekeeperAfterRecruit", self->id)
					    .detail("RKID", ratekeeper.get().id())
					    .detail("DcID", printable(self->clusterControllerDcId));
					self->id_worker[ratekeeper.get().locality.processId()].haltRatekeeper =
					    brokenPromiseToNever(ratekeeper.get().haltRatekeeper.getReply(HaltRatekeeperRequest(self->id)));
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
	while (self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		if (self->db.serverInfo->get().ratekeeper.present() && !self->recruitRatekeeper.get()) {
			choose {
				when(wait(waitFailureClient(self->db.serverInfo->get().ratekeeper.get().waitFailure,
				                            SERVER_KNOBS->RATEKEEPER_FAILURE_TIME))) {
					TraceEvent("CCRatekeeperDied", self->id)
					    .detail("RKID", self->db.serverInfo->get().ratekeeper.get().id());
					self->db.clearInterf(ProcessClass::RatekeeperClass);
				}
				when(wait(self->recruitRatekeeper.onChange())) {}
			}
		} else {
			wait(startRatekeeper(self));
		}
	}
}

ACTOR Future<Void> dbInfoUpdater(ClusterControllerData* self) {
	state Future<Void> dbInfoChange = self->db.serverInfo->onChange();
	state Future<Void> updateDBInfo = self->updateDBInfo.onTrigger();
	loop {
		choose {
			when(wait(updateDBInfo)) { wait(delay(SERVER_KNOBS->DBINFO_BATCH_DELAY) || dbInfoChange); }
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
			when(std::vector<Endpoint> notUpdated =
			         wait(broadcastDBInfoRequest(req, SERVER_KNOBS->DBINFO_SEND_AMOUNT, Optional<Endpoint>(), false))) {
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
ACTOR Future<Void> workerHealthMonitor(ClusterControllerData* self) {
	loop {
		try {
			while (!self->goodRecruitmentTime.isReady()) {
				wait(lowPriorityDelay(SERVER_KNOBS->CC_WORKER_HEALTH_CHECKING_INTERVAL));
			}

			self->degradedServers = self->getServersWithDegradedLink();

			// Compare `self->degradedServers` with `self->excludedDegradedServers` and remove those that have
			// recovered.
			for (auto it = self->excludedDegradedServers.begin(); it != self->excludedDegradedServers.end();) {
				if (self->degradedServers.find(*it) == self->degradedServers.end()) {
					self->excludedDegradedServers.erase(it++);
				} else {
					++it;
				}
			}

			if (!self->degradedServers.empty()) {
				std::string degradedServerString;
				for (const auto& server : self->degradedServers) {
					degradedServerString += server.toString() + " ";
				}
				TraceEvent("ClusterControllerHealthMonitor").detail("DegradedServers", degradedServerString);

				// Check if the cluster controller should trigger a recovery to exclude any degraded servers from the
				// transaction system.
				if (self->shouldTriggerRecoveryDueToDegradedServers()) {
					if (SERVER_KNOBS->CC_HEALTH_TRIGGER_RECOVERY) {
						if (self->recentRecoveryCountDueToHealth() < SERVER_KNOBS->CC_MAX_HEALTH_RECOVERY_COUNT) {
							self->recentHealthTriggeredRecoveryTime.push(now());
							self->excludedDegradedServers = self->degradedServers;
							TraceEvent("DegradedServerDetectedAndTriggerRecovery")
							    .detail("RecentRecoveryCountDueToHealth", self->recentRecoveryCountDueToHealth());
							self->db.forceMasterFailure.trigger();
						}
					} else {
						self->excludedDegradedServers.clear();
						TraceEvent("DegradedServerDetectedAndSuggestRecovery").log();
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

ACTOR Future<Void> clusterControllerCore(ClusterControllerFullInterface interf,
                                         Future<Void> leaderFail,
                                         ServerCoordinators coordinators,
                                         LocalityData locality,
                                         ConfigDBType configDBType) {
	state ClusterControllerData self(interf, locality, coordinators);
	state ConfigBroadcaster configBroadcaster(coordinators, configDBType);
	state Future<Void> coordinationPingDelay = delay(SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY);
	state uint64_t step = 0;
	state Future<ErrorOr<Void>> error = errorOr(actorCollection(self.addActor.getFuture()));

	self.addActor.send(clusterWatchDatabase(&self, &self.db, coordinators, leaderFail)); // Start the master database
	self.addActor.send(self.updateWorkerList.init(self.db.db));
	self.addActor.send(statusServer(interf.clientInterface.databaseStatus.getFuture(),
	                                &self,
	                                coordinators,
	                                (configDBType == ConfigDBType::DISABLED) ? nullptr : &configBroadcaster));
	self.addActor.send(timeKeeper(&self));
	self.addActor.send(monitorProcessClasses(&self));
	self.addActor.send(monitorServerInfoConfig(&self.db));
	self.addActor.send(monitorGlobalConfig(&self.db));
	self.addActor.send(updatedChangingDatacenters(&self));
	self.addActor.send(updatedChangedDatacenters(&self));
	self.addActor.send(updateDatacenterVersionDifference(&self));
	self.addActor.send(handleForcedRecoveries(&self, interf));
	self.addActor.send(monitorDataDistributor(&self));
	self.addActor.send(monitorRatekeeper(&self));
	// self.addActor.send(monitorTSSMapping(&self));
	self.addActor.send(dbInfoUpdater(&self));
	self.addActor.send(traceCounters("ClusterControllerMetrics",
	                                 self.id,
	                                 SERVER_KNOBS->STORAGE_LOGGING_DELAY,
	                                 &self.clusterControllerMetrics,
	                                 self.id.toString() + "/ClusterControllerMetrics"));
	self.addActor.send(traceRole(Role::CLUSTER_CONTROLLER, interf.id()));
	// printf("%s: I am the cluster controller\n", g_network->getLocalAddress().toString().c_str());

	if (SERVER_KNOBS->CC_ENABLE_WORKER_HEALTH_MONITOR) {
		self.addActor.send(workerHealthMonitor(&self));
	}

	loop choose {
		when(ErrorOr<Void> err = wait(error)) {
			if (err.isError() && err.getError().code() != error_code_coordinators_changed) {
				endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Stop Received Error", false, err.getError());
			} else {
				endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Stop Received Signal", true);
			}

			// We shut down normally even if there was a serious error (so this fdbserver may be re-elected cluster
			// controller)
			return Void();
		}
		when(OpenDatabaseRequest req = waitNext(interf.clientInterface.openDatabase.getFuture())) {
			++self.openDatabaseRequests;
			self.addActor.send(clusterOpenDatabase(&self.db, req));
		}
		when(RecruitFromConfigurationRequest req = waitNext(interf.recruitFromConfiguration.getFuture())) {
			self.addActor.send(clusterRecruitFromConfiguration(&self, req));
		}
		when(RecruitRemoteFromConfigurationRequest req = waitNext(interf.recruitRemoteFromConfiguration.getFuture())) {
			self.addActor.send(clusterRecruitRemoteFromConfiguration(&self, req));
		}
		when(RecruitStorageRequest req = waitNext(interf.recruitStorage.getFuture())) {
			clusterRecruitStorage(&self, req);
		}
		when(RegisterWorkerRequest req = waitNext(interf.registerWorker.getFuture())) {
			++self.registerWorkerRequests;
			registerWorker(req, &self, (configDBType == ConfigDBType::DISABLED) ? nullptr : &configBroadcaster);
		}
		when(GetWorkersRequest req = waitNext(interf.getWorkers.getFuture())) {
			++self.getWorkersRequests;
			vector<WorkerDetails> workers;

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
			vector<ClientWorkerInterface> workers;
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
			self.cancelClusterRecovery.trigger();
			// We are no longer the leader if this has changed.
			endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Leader Replaced", true);
			TEST(true); // Lost Cluster Controller Role
			return Void();
		}
		when(ReplyPromise<Void> ping = waitNext(interf.clientInterface.ping.getFuture())) { ping.send(Void()); }
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
                                     ConfigDBType configDBType) {
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
					when(wait(shouldReplace)) { break; }
				}
			}
			if (!shouldReplace.isReady()) {
				shouldReplace = Future<Void>();
				hasConnected = true;
				startRole(Role::CLUSTER_CONTROLLER, cci.id(), UID());
				inRole = true;

				wait(clusterControllerCore(cci, leaderFail, coordinators, locality, configDBType));
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

ACTOR Future<Void> clusterController(Reference<ClusterConnectionFile> connFile,
                                     Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC,
                                     Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo,
                                     Future<Void> recoveredDiskFiles,
                                     LocalityData locality,
                                     ConfigDBType configDBType) {
	wait(recoveredDiskFiles);
	state bool hasConnected = false;
	loop {
		try {
			ServerCoordinators coordinators(connFile);
			wait(clusterController(coordinators, currentCC, hasConnected, asyncPriorityInfo, locality, configDBType));
		} catch (Error& e) {
			if (e.code() != error_code_coordinators_changed)
				throw; // Expected to terminate fdbserver
		}

		hasConnected = true;
	}
}

namespace {

// Tests `ClusterControllerData::updateWorkerHealth()` can update `ClusterControllerData::workerHealth` based on
// `UpdateWorkerHealth` request correctly.
TEST_CASE("/fdbserver/clustercontroller/updateWorkerHealth") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	state ClusterControllerData data(ClusterControllerFullInterface(),
	                                 LocalityData(),
	                                 ServerCoordinators(Reference<ClusterConnectionFile>(new ClusterConnectionFile())));
	state NetworkAddress workerAddress(IPAddress(0x01010101), 1);
	state NetworkAddress badPeer1(IPAddress(0x02020202), 1);
	state NetworkAddress badPeer2(IPAddress(0x03030303), 1);
	state NetworkAddress badPeer3(IPAddress(0x04040404), 1);

	// Create a `UpdateWorkerHealthRequest` with two bad peers, and they should appear in the `workerAddress`'s
	// degradedPeers.
	{
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		req.degradedPeers.push_back(badPeer1);
		req.degradedPeers.push_back(badPeer2);
		data.updateWorkerHealth(req);
		ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
		auto& health = data.workerHealth[workerAddress];
		ASSERT_EQ(health.degradedPeers.size(), 2);
		ASSERT(health.degradedPeers.find(badPeer1) != health.degradedPeers.end());
		ASSERT_EQ(health.degradedPeers[badPeer1].startTime, health.degradedPeers[badPeer1].lastRefreshTime);
		ASSERT(health.degradedPeers.find(badPeer2) != health.degradedPeers.end());
	}

	// Create a `UpdateWorkerHealthRequest` with two bad peers, one from the previous test and a new one.
	// The one from the previous test should have lastRefreshTime updated.
	// The other one from the previous test not included in this test should be removed.
	{
		// Make the time to move so that now() guarantees to return a larger value than before.
		wait(delay(0.001));
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		req.degradedPeers.push_back(badPeer1);
		req.degradedPeers.push_back(badPeer3);
		data.updateWorkerHealth(req);
		ASSERT(data.workerHealth.find(workerAddress) != data.workerHealth.end());
		auto& health = data.workerHealth[workerAddress];
		ASSERT_EQ(health.degradedPeers.size(), 2);
		ASSERT(health.degradedPeers.find(badPeer1) != health.degradedPeers.end());
		ASSERT_LT(health.degradedPeers[badPeer1].startTime, health.degradedPeers[badPeer1].lastRefreshTime);
		ASSERT(health.degradedPeers.find(badPeer2) == health.degradedPeers.end());
		ASSERT(health.degradedPeers.find(badPeer3) != health.degradedPeers.end());
	}

	// Create a `UpdateWorkerHealthRequest` with empty `degradedPeers`, which should remove the worker from
	// `workerHealth`.
	{
		UpdateWorkerHealthRequest req;
		req.address = workerAddress;
		data.updateWorkerHealth(req);
		ASSERT(data.workerHealth.find(workerAddress) == data.workerHealth.end());
	}

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/updateRecoveredWorkers") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<ClusterConnectionFile>(new ClusterConnectionFile())));
	NetworkAddress worker1(IPAddress(0x01010101), 1);
	NetworkAddress worker2(IPAddress(0x11111111), 1);
	NetworkAddress badPeer1(IPAddress(0x02020202), 1);
	NetworkAddress badPeer2(IPAddress(0x03030303), 1);

	// Create following test scenario:
	// 	 worker1 -> badPeer1 active
	// 	 worker1 -> badPeer2 recovered
	// 	 worker2 -> badPeer2 recovered
	data.workerHealth[worker1].degradedPeers[badPeer1] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1, now()
	};
	data.workerHealth[worker1].degradedPeers[badPeer2] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
	};
	data.workerHealth[worker2].degradedPeers[badPeer2] = {
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1,
		now() - SERVER_KNOBS->CC_DEGRADED_LINK_EXPIRATION_INTERVAL - 1
	};
	data.updateRecoveredWorkers();

	ASSERT_EQ(data.workerHealth.size(), 1);
	ASSERT(data.workerHealth.find(worker1) != data.workerHealth.end());
	ASSERT(data.workerHealth[worker1].degradedPeers.find(badPeer1) != data.workerHealth[worker1].degradedPeers.end());
	ASSERT(data.workerHealth[worker1].degradedPeers.find(badPeer2) == data.workerHealth[worker1].degradedPeers.end());
	ASSERT(data.workerHealth.find(worker2) == data.workerHealth.end());

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/getServersWithDegradedLink") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<ClusterConnectionFile>(new ClusterConnectionFile())));
	NetworkAddress worker(IPAddress(0x01010101), 1);
	NetworkAddress badPeer1(IPAddress(0x02020202), 1);
	NetworkAddress badPeer2(IPAddress(0x03030303), 1);
	NetworkAddress badPeer3(IPAddress(0x04040404), 1);
	NetworkAddress badPeer4(IPAddress(0x05050505), 1);

	// Test that a reported degraded link should stay for sometime before being considered as a degraded link by cluster
	// controller.
	{
		data.workerHealth[worker].degradedPeers[badPeer1] = { now(), now() };
		ASSERT(data.getServersWithDegradedLink().empty());
		data.workerHealth.clear();
	}

	// Test that when there is only one reported degraded link, getServersWithDegradedLink can return correct degraded
	// server.
	{
		data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		auto degradedServers = data.getServersWithDegradedLink();
		ASSERT(degradedServers.size() == 1);
		ASSERT(degradedServers.find(badPeer1) != degradedServers.end());
		data.workerHealth.clear();
	}

	// Test that if both A complains B and B compalins A, only one of the server will be chosen as degraded server.
	{
		data.workerHealth[worker].degradedPeers[badPeer1] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		data.workerHealth[badPeer1].degradedPeers[worker] = { now() - SERVER_KNOBS->CC_MIN_DEGRADATION_INTERVAL - 1,
			                                                  now() };
		auto degradedServers = data.getServersWithDegradedLink();
		ASSERT(degradedServers.size() == 1);
		ASSERT(degradedServers.find(worker) != degradedServers.end() ||
		       degradedServers.find(badPeer1) != degradedServers.end());
		data.workerHealth.clear();
	}

	// Test that if B complains A and C complains A, A is selected as degraded server instead of B or C.
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
		auto degradedServers = data.getServersWithDegradedLink();
		ASSERT(degradedServers.size() == 1);
		ASSERT(degradedServers.find(worker) != degradedServers.end());
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
		ASSERT(data.getServersWithDegradedLink().empty());
		data.workerHealth.clear();
	}

	// Test that if the degradation is reported both ways between A and other 4 servers, no degraded server is returned.
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
		ASSERT(data.getServersWithDegradedLink().empty());
		data.workerHealth.clear();
	}

	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/recentRecoveryCountDueToHealth") {
	// Create a testing ClusterControllerData. Most of the internal states do not matter in this test.
	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(),
	                           ServerCoordinators(Reference<ClusterConnectionFile>(new ClusterConnectionFile())));

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
	                           ServerCoordinators(Reference<ClusterConnectionFile>(new ClusterConnectionFile())));
	NetworkAddress master(IPAddress(0x01010101), 1);
	NetworkAddress tlog(IPAddress(0x02020202), 1);
	NetworkAddress satelliteTlog(IPAddress(0x03030303), 1);
	NetworkAddress remoteTlog(IPAddress(0x04040404), 1);
	NetworkAddress logRouter(IPAddress(0x05050505), 1);
	NetworkAddress backup(IPAddress(0x06060606), 1);
	NetworkAddress proxy(IPAddress(0x07070707), 1);
	NetworkAddress resolver(IPAddress(0x08080808), 1);
	NetworkAddress clusterController(IPAddress(0x09090909), 1);

	// Create a ServerDBInfo using above addresses.
	ServerDBInfo testDbInfo;
	testDbInfo.clusterInterface.changeCoordinators =
	    RequestStream<struct ChangeCoordinatorsRequest>(Endpoint({ clusterController }, UID(1, 2)));

	MasterInterface mInterface;
	mInterface.getCommitVersion = RequestStream<struct GetCommitVersionRequest>(Endpoint({ master }, UID(1, 2)));
	testDbInfo.master = mInterface;

	TLogInterface localTLogInterf;
	localTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ tlog }, UID(1, 2)));
	TLogInterface localLogRouterInterf;
	localLogRouterInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ logRouter }, UID(1, 2)));
	BackupInterface backupInterf;
	backupInterf.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ backup }, UID(1, 2)));
	TLogSet localTLogSet;
	localTLogSet.isLocal = true;
	localTLogSet.tLogs.push_back(OptionalInterface(localTLogInterf));
	localTLogSet.logRouters.push_back(OptionalInterface(localLogRouterInterf));
	localTLogSet.backupWorkers.push_back(OptionalInterface(backupInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(localTLogSet);

	TLogInterface sateTLogInterf;
	sateTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ satelliteTlog }, UID(1, 2)));
	TLogSet sateTLogSet;
	sateTLogSet.isLocal = true;
	sateTLogSet.locality = tagLocalitySatellite;
	sateTLogSet.tLogs.push_back(OptionalInterface(sateTLogInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(sateTLogSet);

	TLogInterface remoteTLogInterf;
	remoteTLogInterf.peekMessages = RequestStream<struct TLogPeekRequest>(Endpoint({ remoteTlog }, UID(1, 2)));
	TLogSet remoteTLogSet;
	remoteTLogSet.isLocal = false;
	remoteTLogSet.tLogs.push_back(OptionalInterface(remoteTLogInterf));
	testDbInfo.logSystemConfig.tLogs.push_back(remoteTLogSet);

	GrvProxyInterface proxyInterf;
	proxyInterf.getConsistentReadVersion = RequestStream<struct GetReadVersionRequest>(Endpoint({ proxy }, UID(1, 2)));
	testDbInfo.client.grvProxies.push_back(proxyInterf);

	ResolverInterface resolverInterf;
	resolverInterf.resolve = RequestStream<struct ResolveTransactionBatchRequest>(Endpoint({ resolver }, UID(1, 2)));
	testDbInfo.resolvers.push_back(resolverInterf);

	testDbInfo.recoveryState = RecoveryState::ACCEPTING_COMMITS;

	// No recovery when no degraded servers.
	data.db.serverInfo->set(testDbInfo);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());

	// Trigger recovery when master is degraded.
	data.degradedServers.insert(master);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// Trigger recovery when primary TLog is degraded.
	data.degradedServers.insert(tlog);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// No recovery when satellite Tlog is degraded.
	data.degradedServers.insert(satelliteTlog);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// No recovery when remote tlog is degraded.
	data.degradedServers.insert(remoteTlog);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// No recovery when log router is degraded.
	data.degradedServers.insert(logRouter);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// No recovery when backup worker is degraded.
	data.degradedServers.insert(backup);
	ASSERT(!data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// Trigger recovery when proxy is degraded.
	data.degradedServers.insert(proxy);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());
	data.degradedServers.clear();

	// Trigger recovery when resolver is degraded.
	data.degradedServers.insert(resolver);
	ASSERT(data.shouldTriggerRecoveryDueToDegradedServers());

	return Void();
}

} // namespace
