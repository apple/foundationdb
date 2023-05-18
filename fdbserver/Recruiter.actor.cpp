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

#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/ClusterRecovery.actor.h"
#include "fdbserver/RoleFitness.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

bool isExcludedDegradedServer(ClusterControllerData const* clusterControllerData, const NetworkAddressList& a) {
	for (const auto& server : clusterControllerData->excludedDegradedServers) {
		if (a.contains(server))
			return true;
	}
	return false;
}

void updateIdUsed(const std::vector<WorkerInterface>& workers,
                  std::map<Optional<Standalone<StringRef>>, int>& id_used) {
	for (auto& it : workers) {
		id_used[it.locality.processId()]++;
	}
}

void compareWorkers(std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker,
                    const DatabaseConfiguration& conf,
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

bool isLongLivedStateless(ClusterControllerData const* clusterControllerData, Optional<Key> const& processId) {
	return (clusterControllerData->db.serverInfo->get().distributor.present() &&
	        clusterControllerData->db.serverInfo->get().distributor.get().locality.processId() == processId) ||
	       (clusterControllerData->db.serverInfo->get().ratekeeper.present() &&
	        clusterControllerData->db.serverInfo->get().ratekeeper.get().locality.processId() == processId) ||
	       (clusterControllerData->db.serverInfo->get().blobManager.present() &&
	        clusterControllerData->db.serverInfo->get().blobManager.get().locality.processId() == processId) ||
	       (clusterControllerData->db.serverInfo->get().blobMigrator.present() &&
	        clusterControllerData->db.serverInfo->get().blobMigrator.get().locality.processId() == processId) ||
	       (clusterControllerData->db.serverInfo->get().client.encryptKeyProxy.present() &&
	        clusterControllerData->db.serverInfo->get().client.encryptKeyProxy.get().locality.processId() ==
	            processId) ||
	       (clusterControllerData->db.serverInfo->get().consistencyScan.present() &&
	        clusterControllerData->db.serverInfo->get().consistencyScan.get().locality.processId() == processId);
}

// Log the reason why the worker is considered as unavailable.
void logWorkerUnavailable(Severity severity,
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

// Adds workers to the result such that each field is used in the result set as evenly as possible,
// with a secondary criteria of minimizing the reuse of zoneIds
// only add workers which have a field which is already in the result set
void addWorkersByLowestField(ClusterControllerData const* clusterControllerData,
                             StringRef field,
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
		fitness.second = thisDc == clusterControllerData->clusterControllerDcId;

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

} // namespace

class RecruiterImpl {
public:
	static bool workerAvailable(Recruiter* self, WorkerInfo const& worker, bool checkStable) {
		return (now() - self->startTime < 2 * FLOW_KNOBS->SERVER_REQUEST_INTERVAL) ||
		       (IFailureMonitor::failureMonitor().getState(worker.details.interf.storage.getEndpoint()).isAvailable() &&
		        (!checkStable || worker.reboots < 2));
	}

	static std::set<Optional<Standalone<StringRef>>> getDatacenters(Recruiter* self,
	                                                                ClusterControllerData const* clusterControllerData,
	                                                                DatabaseConfiguration const& conf,
	                                                                bool checkStable = false) {
		std::set<Optional<Standalone<StringRef>>> result;
		for (auto& it : clusterControllerData->id_worker)
			if (workerAvailable(self, it.second, checkStable) &&
			    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
			    !isExcludedDegradedServer(clusterControllerData, it.second.details.interf.addresses()))
				result.insert(it.second.details.interf.locality.dcId());
		return result;
	}

	static std::vector<WorkerDetails> getWorkersForSeedServers(
	    Recruiter* self,
	    ClusterControllerData const* clusterControllerData,
	    DatabaseConfiguration const& conf,
	    Reference<IReplicationPolicy> const& policy,
	    Optional<Optional<Standalone<StringRef>>> const& dcId = Optional<Optional<Standalone<StringRef>>>()) {
		std::map<ProcessClass::Fitness, std::vector<WorkerDetails>> fitness_workers;
		std::vector<WorkerDetails> results;
		Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
		LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*)logServerSet.getPtr();
		bool bCompleted = false;

		for (auto& it : clusterControllerData->id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness(ProcessClass::Storage);
			if (workerAvailable(self, it.second, false) && it.second.details.recoveredDiskFiles &&
			    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
			    !isExcludedDegradedServer(clusterControllerData, it.second.details.interf.addresses()) &&
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

	// A TLog recruitment method specialized for single, double, and triple configurations
	// It recruits processes from with unique zoneIds until it reaches the desired amount
	static std::vector<WorkerDetails> getWorkersForTLogsSimple(Recruiter* self,
	                                                           ClusterControllerData const* clusterControllerData,
	                                                           DatabaseConfiguration const& conf,
	                                                           int32_t required,
	                                                           int32_t desired,
	                                                           std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                           bool checkStable,
	                                                           const std::set<Optional<Key>>& dcIds,
	                                                           const std::vector<UID>& exclusionWorkerIds) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, bool, bool>, std::vector<WorkerDetails>> fitness_workers;

		// Go through all the workers to list all the workers that can be recruited.
		for (const auto& [worker_process_id, worker_info] : clusterControllerData->id_worker) {
			const auto& worker_details = worker_info.details;
			auto fitness = worker_details.processClass.machineClassFitness(ProcessClass::TLog);

			if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), worker_details.interf.id()) !=
			    exclusionWorkerIds.end()) {
				logWorkerUnavailable(
				    SevInfo, clusterControllerData->id, "simple", "Worker is excluded", worker_details, fitness, dcIds);
				continue;
			}
			if (!workerAvailable(self, worker_info, checkStable)) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "simple",
				                     "Worker is not available",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!worker_details.recoveredDiskFiles) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "simple",
				                     "Worker disk file recovery unfinished",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (conf.isExcludedServer(worker_details.interf.addresses())) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "simple",
				                     "Worker server is excluded from the cluster",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (isExcludedDegradedServer(clusterControllerData, worker_details.interf.addresses())) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "simple",
				                     "Worker server is excluded from the cluster due to degradation",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (fitness == ProcessClass::NeverAssign) {
				logWorkerUnavailable(SevDebug,
				                     clusterControllerData->id,
				                     "simple",
				                     "Worker's fitness is NeverAssign",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!dcIds.empty() && dcIds.count(worker_details.interf.locality.dcId()) == 0) {
				logWorkerUnavailable(SevDebug,
				                     clusterControllerData->id,
				                     "simple",
				                     "Worker is not in the target DC",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}

			// This worker is a candidate for TLog recruitment.
			bool inCCDC = worker_details.interf.locality.dcId() == clusterControllerData->clusterControllerDcId;
			// Prefer recruiting a TransactionClass non-degraded process over a LogClass degraded process
			if (worker_details.degraded) {
				fitness = std::max(fitness, ProcessClass::GoodFit);
			}

			fitness_workers[std::make_tuple(fitness,
			                                id_used[worker_process_id],
			                                worker_details.degraded,
			                                isLongLivedStateless(clusterControllerData, worker_process_id),
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
	static std::vector<WorkerDetails> getWorkersForTLogsBackup(
	    Recruiter* self,
	    ClusterControllerData const* clusterControllerData,
	    DatabaseConfiguration const& conf,
	    int32_t required,
	    int32_t desired,
	    Reference<IReplicationPolicy> const& policy,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    bool checkStable = false,
	    const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	    const std::vector<UID>& exclusionWorkerIds = {}) {
		std::map<std::tuple<ProcessClass::Fitness, int, bool, bool>, std::vector<WorkerDetails>> fitness_workers;
		std::vector<WorkerDetails> results;
		Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
		LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*)logServerSet.getPtr();
		bool bCompleted = false;
		desired = std::max(required, desired);

		// Go through all the workers to list all the workers that can be recruited.
		for (const auto& [worker_process_id, worker_info] : clusterControllerData->id_worker) {
			const auto& worker_details = worker_info.details;
			auto fitness = worker_details.processClass.machineClassFitness(ProcessClass::TLog);

			if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), worker_details.interf.id()) !=
			    exclusionWorkerIds.end()) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker is excluded",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!workerAvailable(self, worker_info, checkStable)) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker is not available",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!worker_details.recoveredDiskFiles) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker disk file recovery unfinished",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (conf.isExcludedServer(worker_details.interf.addresses())) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker server is excluded from the cluster",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (isExcludedDegradedServer(clusterControllerData, worker_details.interf.addresses())) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker server is excluded from the cluster due to degradation",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (fitness == ProcessClass::NeverAssign) {
				logWorkerUnavailable(SevDebug,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker's fitness is NeverAssign",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!dcIds.empty() && dcIds.count(worker_details.interf.locality.dcId()) == 0) {
				logWorkerUnavailable(SevDebug,
				                     clusterControllerData->id,
				                     "deprecated",
				                     "Worker is not in the target DC",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}

			// This worker is a candidate for TLog recruitment.
			bool inCCDC = worker_details.interf.locality.dcId() == clusterControllerData->clusterControllerDcId;
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
		    .detail("Workers", clusterControllerData->id_worker.size())
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

	// A TLog recruitment method specialized for three_data_hall and three_datacenter configurations
	// It attempts to evenly recruit processes from across data_halls or datacenters
	static std::vector<WorkerDetails> getWorkersForTLogsComplex(Recruiter* self,
	                                                            ClusterControllerData const* clusterControllerData,
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
		for (const auto& [worker_process_id, worker_info] : clusterControllerData->id_worker) {
			const auto& worker_details = worker_info.details;
			auto fitness = worker_details.processClass.machineClassFitness(ProcessClass::TLog);

			if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), worker_details.interf.id()) !=
			    exclusionWorkerIds.end()) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker is excluded",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!workerAvailable(self, worker_info, checkStable)) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker is not available",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!worker_details.recoveredDiskFiles) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker disk file recovery unfinished",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (conf.isExcludedServer(worker_details.interf.addresses())) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker server is excluded from the cluster",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (isExcludedDegradedServer(clusterControllerData, worker_details.interf.addresses())) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker server is excluded from the cluster due to degradation",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (fitness == ProcessClass::NeverAssign) {
				logWorkerUnavailable(SevDebug,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker's fitness is NeverAssign",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!dcIds.empty() && dcIds.count(worker_details.interf.locality.dcId()) == 0) {
				logWorkerUnavailable(SevDebug,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker is not in the target DC",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}
			if (!allowDegraded && worker_details.degraded) {
				logWorkerUnavailable(SevInfo,
				                     clusterControllerData->id,
				                     "complex",
				                     "Worker is degraded and not allowed",
				                     worker_details,
				                     fitness,
				                     dcIds);
				continue;
			}

			fitness_workers[std::make_tuple(fitness,
			                                id_used[worker_process_id],
			                                isLongLivedStateless(clusterControllerData, worker_process_id))]
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
				      std::make_tuple(fitness,
				                      used,
				                      worker.interf.locality.dcId() == clusterControllerData->clusterControllerDcId) });
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
				addWorkersByLowestField(clusterControllerData, field, desired, workerIter->second, resultSet);
			}
		}

		for (auto& result : resultSet) {
			id_used[result.interf.locality.processId()]++;
		}

		return std::vector<WorkerDetails>(resultSet.begin(), resultSet.end());
	}

	// Attempt to recruit TLogs without degraded processes and see if it improves the configuration
	static std::vector<WorkerDetails> getWorkersForTLogsComplex(Recruiter* self,
	                                                            ClusterControllerData const* clusterControllerData,
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
		auto withDegraded = getWorkersForTLogsComplex(self,
		                                              clusterControllerData,
		                                              conf,
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
			auto withoutDegraded = getWorkersForTLogsComplex(self,
			                                                 clusterControllerData,
			                                                 conf,
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

	static WorkerDetails getStorageWorker(Recruiter* self,
	                                      RecruitStorageRequest const& req,
	                                      std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker) {
		std::set<Optional<Standalone<StringRef>>> excludedMachines(req.excludeMachines.begin(),
		                                                           req.excludeMachines.end());
		std::set<Optional<Standalone<StringRef>>> includeDCs(req.includeDCs.begin(), req.includeDCs.end());
		std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());

		for (auto& it : id_worker)
			if (workerAvailable(self, it.second, false) && it.second.details.recoveredDiskFiles &&
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
				if (workerAvailable(self, it.second, false) && it.second.details.recoveredDiskFiles &&
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
	static WorkerDetails getBlobWorker(Recruiter* self,
	                                   RecruitBlobWorkerRequest const& req,
	                                   std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker,
	                                   Optional<Standalone<StringRef>> clusterControllerDcId) {
		std::set<AddressExclusion> excludedAddresses(req.excludeAddresses.begin(), req.excludeAddresses.end());
		for (auto& it : id_worker) {
			// the worker must be available, have the same dcID as CC,
			// not be one of the excluded addrs from req and have the approriate fitness
			if (workerAvailable(self, it.second, false) &&
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

	static RecruitFromConfigurationReply findWorkersForConfiguration(Recruiter* self,
	                                                                 ClusterControllerData* clusterControllerData,
	                                                                 RecruitFromConfigurationRequest const& req) {
		RecruitFromConfigurationReply rep = findWorkersForConfigurationDispatch(self, clusterControllerData, req, true);
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
					RecruitFromConfigurationReply compare =
					    findWorkersForConfigurationDispatch(self, clusterControllerData, req, false);

					std::map<Optional<Standalone<StringRef>>, int> firstUsed;
					std::map<Optional<Standalone<StringRef>>, int> secondUsed;
					Recruiter::updateKnownIds(clusterControllerData, &firstUsed);
					Recruiter::updateKnownIds(clusterControllerData, &secondUsed);

					// auto mworker = id_worker.find(masterProcessId);
					//TraceEvent("CompareAddressesMaster")
					//    .detail("Master",
					//            mworker != id_worker.end() ? mworker->second.details.interf.address() :
					//            NetworkAddress());

					updateIdUsed(rep.tLogs, firstUsed);
					updateIdUsed(compare.tLogs, secondUsed);
					compareWorkers(clusterControllerData->id_worker,
					               req.configuration,
					               rep.tLogs,
					               firstUsed,
					               compare.tLogs,
					               secondUsed,
					               ProcessClass::TLog,
					               "TLog");
					updateIdUsed(rep.satelliteTLogs, firstUsed);
					updateIdUsed(compare.satelliteTLogs, secondUsed);
					compareWorkers(clusterControllerData->id_worker,
					               req.configuration,
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
					compareWorkers(clusterControllerData->id_worker,
					               req.configuration,
					               rep.commitProxies,
					               firstUsed,
					               compare.commitProxies,
					               secondUsed,
					               ProcessClass::CommitProxy,
					               "CommitProxy");
					compareWorkers(clusterControllerData->id_worker,
					               req.configuration,
					               rep.grvProxies,
					               firstUsed,
					               compare.grvProxies,
					               secondUsed,
					               ProcessClass::GrvProxy,
					               "GrvProxy");
					compareWorkers(clusterControllerData->id_worker,
					               req.configuration,
					               rep.resolvers,
					               firstUsed,
					               compare.resolvers,
					               secondUsed,
					               ProcessClass::Resolver,
					               "Resolver");
					updateIdUsed(rep.backupWorkers, firstUsed);
					updateIdUsed(compare.backupWorkers, secondUsed);
					compareWorkers(clusterControllerData->id_worker,
					               req.configuration,
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

	static ErrorOr<RecruitFromConfigurationReply> findWorkersForConfigurationFromDC(
	    Recruiter* self,
	    ClusterControllerData const* clusterControllerData,
	    RecruitFromConfigurationRequest const& req,
	    Optional<Key> dcId,
	    bool checkGoodRecruitment) {
		RecruitFromConfigurationReply result;
		std::map<Optional<Standalone<StringRef>>, int> id_used;
		Recruiter::updateKnownIds(clusterControllerData, &id_used);

		ASSERT(dcId.present());

		std::set<Optional<Key>> primaryDC;
		primaryDC.insert(dcId);
		result.dcId = dcId;

		auto [region, remoteRegion] = getPrimaryAndRemoteRegion(req.configuration.regions, dcId.get());

		if (req.recruitSeedServers) {
			auto primaryStorageServers = getWorkersForSeedServers(
			    self, clusterControllerData, req.configuration, req.configuration.storagePolicy, dcId);
			for (int i = 0; i < primaryStorageServers.size(); i++) {
				result.storageServers.push_back(primaryStorageServers[i].interf);
			}
		}

		auto tLogs = self->getWorkersForTLogs(clusterControllerData,
		                                      req.configuration,
		                                      req.configuration.tLogReplicationFactor,
		                                      req.configuration.getDesiredLogs(),
		                                      req.configuration.tLogPolicy,
		                                      id_used,
		                                      false,
		                                      primaryDC);
		for (int i = 0; i < tLogs.size(); i++) {
			result.tLogs.push_back(tLogs[i].interf);
		}

		std::vector<WorkerDetails> satelliteLogs;
		if (region.satelliteTLogReplicationFactor > 0 && req.configuration.usableRegions > 1) {
			satelliteLogs = self->getWorkersForSatelliteLogs(
			    clusterControllerData, req.configuration, region, remoteRegion, id_used, result.satelliteFallback);
			for (int i = 0; i < satelliteLogs.size(); i++) {
				result.satelliteTLogs.push_back(satelliteLogs[i].interf);
			}
		}

		std::map<Optional<Standalone<StringRef>>, int> preferredSharing;
		auto first_commit_proxy = self->getWorkerForRoleInDatacenter(clusterControllerData,
		                                                             dcId,
		                                                             ProcessClass::CommitProxy,
		                                                             ProcessClass::ExcludeFit,
		                                                             req.configuration,
		                                                             id_used,
		                                                             preferredSharing);
		preferredSharing[first_commit_proxy.worker.interf.locality.processId()] = 0;
		auto first_grv_proxy = self->getWorkerForRoleInDatacenter(clusterControllerData,
		                                                          dcId,
		                                                          ProcessClass::GrvProxy,
		                                                          ProcessClass::ExcludeFit,
		                                                          req.configuration,
		                                                          id_used,
		                                                          preferredSharing);
		preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
		auto first_resolver = self->getWorkerForRoleInDatacenter(clusterControllerData,
		                                                         dcId,
		                                                         ProcessClass::Resolver,
		                                                         ProcessClass::ExcludeFit,
		                                                         req.configuration,
		                                                         id_used,
		                                                         preferredSharing);
		preferredSharing[first_resolver.worker.interf.locality.processId()] = 2;

		// If one of the first process recruitments is forced to share a process, allow all of next recruitments
		// to also share a process.
		auto maxUsed = std::max({ first_commit_proxy.used, first_grv_proxy.used, first_resolver.used });
		first_commit_proxy.used = maxUsed;
		first_grv_proxy.used = maxUsed;
		first_resolver.used = maxUsed;

		auto commit_proxies = self->getWorkersForRoleInDatacenter(clusterControllerData,
		                                                          dcId,
		                                                          ProcessClass::CommitProxy,
		                                                          req.configuration.getDesiredCommitProxies(),
		                                                          req.configuration,
		                                                          id_used,
		                                                          preferredSharing,
		                                                          first_commit_proxy);
		auto grv_proxies = self->getWorkersForRoleInDatacenter(clusterControllerData,
		                                                       dcId,
		                                                       ProcessClass::GrvProxy,
		                                                       req.configuration.getDesiredGrvProxies(),
		                                                       req.configuration,
		                                                       id_used,
		                                                       preferredSharing,
		                                                       first_grv_proxy);
		auto resolvers = self->getWorkersForRoleInDatacenter(clusterControllerData,
		                                                     dcId,
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
			if (tLogs.size() == 1) {
				result.oldLogRouters.push_back(tLogs[0].interf);
			} else {
				for (int i = 0; i < tLogs.size(); i++) {
					if (tLogs[i].interf.locality.processId() != clusterControllerData->clusterControllerProcessId) {
						result.oldLogRouters.push_back(tLogs[i].interf);
					}
				}
			}
		}

		if (req.configuration.backupWorkerEnabled) {
			const int nBackup = std::max<int>(
			    (req.configuration.desiredLogRouterCount > 0 ? req.configuration.desiredLogRouterCount : tLogs.size()),
			    req.maxOldLogRouters);
			auto backupWorkers = self->getWorkersForRoleInDatacenter(
			    clusterControllerData, dcId, ProcessClass::Backup, nBackup, req.configuration, id_used);
			std::transform(backupWorkers.begin(),
			               backupWorkers.end(),
			               std::back_inserter(result.backupWorkers),
			               [](const WorkerDetails& w) { return w.interf; });
		}

		if (!clusterControllerData->goodRecruitmentTime.isReady() && checkGoodRecruitment &&
		    (RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(), ProcessClass::TLog)
		         .betterCount(RoleFitness(tLogs, ProcessClass::TLog, id_used)) ||
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

	static RecruitFromConfigurationReply findWorkersForConfigurationDispatch(
	    Recruiter* self,
	    ClusterControllerData* clusterControllerData,
	    RecruitFromConfigurationRequest const& req,
	    bool checkGoodRecruitment) {
		if (req.configuration.regions.size() > 1) {
			std::vector<RegionInfo> regions = req.configuration.regions;
			if (regions[0].priority == regions[1].priority &&
			    regions[1].dcId == clusterControllerData->clusterControllerDcId.get()) {
				TraceEvent("CCSwitchPrimaryDc", clusterControllerData->id)
				    .detail("CCDcId", clusterControllerData->clusterControllerDcId.get())
				    .detail("OldPrimaryDcId", regions[0].dcId)
				    .detail("NewPrimaryDcId", regions[1].dcId);
				std::swap(regions[0], regions[1]);
			}

			if (regions[1].dcId == clusterControllerData->clusterControllerDcId.get() &&
			    (!clusterControllerData->versionDifferenceUpdated ||
			     clusterControllerData->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE)) {
				if (regions[1].priority >= 0) {
					TraceEvent("CCSwitchPrimaryDcVersionDifference", clusterControllerData->id)
					    .detail("CCDcId", clusterControllerData->clusterControllerDcId.get())
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

			TraceEvent("CCFindWorkersForConfiguration", clusterControllerData->id)
			    .detail("CCDcId", clusterControllerData->clusterControllerDcId.get())
			    .detail("Region0DcId", regions[0].dcId)
			    .detail("Region1DcId", regions[1].dcId)
			    .detail("DatacenterVersionDifference", clusterControllerData->datacenterVersionDifference)
			    .detail("VersionDifferenceUpdated", clusterControllerData->versionDifferenceUpdated);

			bool setPrimaryDesired = false;
			try {
				auto reply = findWorkersForConfigurationFromDC(
				    self, clusterControllerData, req, regions[0].dcId, checkGoodRecruitment);
				setPrimaryDesired = true;
				std::vector<Optional<Key>> dcPriority;
				dcPriority.push_back(regions[0].dcId);
				dcPriority.push_back(regions[1].dcId);
				clusterControllerData->desiredDcIds.set(dcPriority);
				if (reply.isError()) {
					throw reply.getError();
				} else if (regions[0].dcId == clusterControllerData->clusterControllerDcId.get()) {
					return reply.get();
				}
				TraceEvent(SevWarn, "CCRecruitmentFailed", clusterControllerData->id)
				    .detail("Reason", "Recruited Txn system and CC are in different DCs")
				    .detail("CCDcId", clusterControllerData->clusterControllerDcId.get())
				    .detail("RecruitedTxnSystemDcId", regions[0].dcId);
				throw no_more_servers();
			} catch (Error& e) {
				if (!clusterControllerData->goodRemoteRecruitmentTime.isReady() &&
				    regions[1].dcId != clusterControllerData->clusterControllerDcId.get() && checkGoodRecruitment) {
					throw operation_failed();
				}

				if (e.code() != error_code_no_more_servers || regions[1].priority < 0) {
					throw;
				}
				TraceEvent(SevWarn, "AttemptingRecruitmentInRemoteDc", clusterControllerData->id)
				    .error(e)
				    .detail("SetPrimaryDesired", setPrimaryDesired);
				auto reply = findWorkersForConfigurationFromDC(
				    self, clusterControllerData, req, regions[1].dcId, checkGoodRecruitment);
				if (!setPrimaryDesired) {
					std::vector<Optional<Key>> dcPriority;
					dcPriority.push_back(regions[1].dcId);
					dcPriority.push_back(regions[0].dcId);
					clusterControllerData->desiredDcIds.set(dcPriority);
				}
				if (reply.isError()) {
					throw reply.getError();
				} else if (regions[1].dcId == clusterControllerData->clusterControllerDcId.get()) {
					return reply.get();
				}
				throw;
			}
		} else if (req.configuration.regions.size() == 1) {
			std::vector<Optional<Key>> dcPriority;
			dcPriority.push_back(req.configuration.regions[0].dcId);
			clusterControllerData->desiredDcIds.set(dcPriority);
			auto reply = findWorkersForConfigurationFromDC(
			    self, clusterControllerData, req, req.configuration.regions[0].dcId, checkGoodRecruitment);
			if (reply.isError()) {
				throw reply.getError();
			} else if (req.configuration.regions[0].dcId == clusterControllerData->clusterControllerDcId.get()) {
				return reply.get();
			}
			throw no_more_servers();
		} else {
			RecruitFromConfigurationReply result;
			std::map<Optional<Standalone<StringRef>>, int> id_used;
			Recruiter::updateKnownIds(clusterControllerData, &id_used);
			auto tLogs = self->getWorkersForTLogs(clusterControllerData,
			                                      req.configuration,
			                                      req.configuration.tLogReplicationFactor,
			                                      req.configuration.getDesiredLogs(),
			                                      req.configuration.tLogPolicy,
			                                      id_used);
			for (int i = 0; i < tLogs.size(); i++) {
				result.tLogs.push_back(tLogs[i].interf);
			}

			if (req.maxOldLogRouters > 0) {
				if (tLogs.size() == 1) {
					result.oldLogRouters.push_back(tLogs[0].interf);
				} else {
					for (int i = 0; i < tLogs.size(); i++) {
						if (tLogs[i].interf.locality.processId() != clusterControllerData->clusterControllerProcessId) {
							result.oldLogRouters.push_back(tLogs[i].interf);
						}
					}
				}
			}

			if (req.recruitSeedServers) {
				auto primaryStorageServers = getWorkersForSeedServers(
				    self, clusterControllerData, req.configuration, req.configuration.storagePolicy);
				for (int i = 0; i < primaryStorageServers.size(); i++)
					result.storageServers.push_back(primaryStorageServers[i].interf);
			}

			auto datacenters = getDatacenters(self, clusterControllerData, req.configuration);

			std::tuple<RoleFitness, RoleFitness, RoleFitness> bestFitness;
			int numEquivalent = 1;
			Optional<Key> bestDC;

			for (auto dcId : datacenters) {
				try {
					// SOMEDAY: recruitment in other DCs besides the clusterControllerDcID will not account for the
					// processes used by the master and cluster controller properly.
					auto used = id_used;
					std::map<Optional<Standalone<StringRef>>, int> preferredSharing;
					auto first_commit_proxy = self->getWorkerForRoleInDatacenter(clusterControllerData,
					                                                             dcId,
					                                                             ProcessClass::CommitProxy,
					                                                             ProcessClass::ExcludeFit,
					                                                             req.configuration,
					                                                             used,
					                                                             preferredSharing);
					preferredSharing[first_commit_proxy.worker.interf.locality.processId()] = 0;
					auto first_grv_proxy = self->getWorkerForRoleInDatacenter(clusterControllerData,
					                                                          dcId,
					                                                          ProcessClass::GrvProxy,
					                                                          ProcessClass::ExcludeFit,
					                                                          req.configuration,
					                                                          used,
					                                                          preferredSharing);
					preferredSharing[first_grv_proxy.worker.interf.locality.processId()] = 1;
					auto first_resolver = self->getWorkerForRoleInDatacenter(clusterControllerData,
					                                                         dcId,
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

					auto commit_proxies =
					    self->getWorkersForRoleInDatacenter(clusterControllerData,
					                                        dcId,
					                                        ProcessClass::CommitProxy,
					                                        req.configuration.getDesiredCommitProxies(),
					                                        req.configuration,
					                                        used,
					                                        preferredSharing,
					                                        first_commit_proxy);

					auto grv_proxies = self->getWorkersForRoleInDatacenter(clusterControllerData,
					                                                       dcId,
					                                                       ProcessClass::GrvProxy,
					                                                       req.configuration.getDesiredGrvProxies(),
					                                                       req.configuration,
					                                                       used,
					                                                       preferredSharing,
					                                                       first_grv_proxy);

					auto resolvers = self->getWorkersForRoleInDatacenter(clusterControllerData,
					                                                     dcId,
					                                                     ProcessClass::Resolver,
					                                                     req.configuration.getDesiredResolvers(),
					                                                     req.configuration,
					                                                     used,
					                                                     preferredSharing,
					                                                     first_resolver);

					auto fitness = std::make_tuple(RoleFitness(commit_proxies, ProcessClass::CommitProxy, used),
					                               RoleFitness(grv_proxies, ProcessClass::GrvProxy, used),
					                               RoleFitness(resolvers, ProcessClass::Resolver, used));

					if (dcId == clusterControllerData->clusterControllerDcId) {
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
							const int nBackup = std::max<int>(tLogs.size(), req.maxOldLogRouters);
							auto backupWorkers = self->getWorkersForRoleInDatacenter(
							    clusterControllerData, dcId, ProcessClass::Backup, nBackup, req.configuration, used);
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

			if (bestDC != clusterControllerData->clusterControllerDcId) {
				TraceEvent("BestDCIsNotClusterDC").log();
				std::vector<Optional<Key>> dcPriority;
				dcPriority.push_back(bestDC);
				clusterControllerData->desiredDcIds.set(dcPriority);
				throw no_more_servers();
			}
			// If this cluster controller dies, do not prioritize recruiting the next one in the same DC
			clusterControllerData->desiredDcIds.set(std::vector<Optional<Key>>());
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

			if (!clusterControllerData->goodRecruitmentTime.isReady() && checkGoodRecruitment &&
			    (RoleFitness(
			         SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(), ProcessClass::TLog)
			         .betterCount(RoleFitness(tLogs, ProcessClass::TLog, id_used)) ||
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

	static RecruitRemoteFromConfigurationReply findRemoteWorkersForConfiguration(
	    Recruiter* self,
	    ClusterControllerData const* clusterControllerData,
	    RecruitRemoteFromConfigurationRequest const& req) {
		RecruitRemoteFromConfigurationReply result;
		std::map<Optional<Standalone<StringRef>>, int> id_used;

		Recruiter::updateKnownIds(clusterControllerData, &id_used);

		if (req.dbgId.present()) {
			TraceEvent(SevDebug, "FindRemoteWorkersForConf", req.dbgId.get())
			    .detail("RemoteDcId", req.dcId)
			    .detail("Configuration", req.configuration.toString())
			    .detail("Policy", req.configuration.getRemoteTLogPolicy()->name());
		}

		std::set<Optional<Key>> remoteDC;
		remoteDC.insert(req.dcId);

		auto remoteLogs = self->getWorkersForTLogs(clusterControllerData,
		                                           req.configuration,
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

		auto logRouters = self->getWorkersForRoleInDatacenter(
		    clusterControllerData, req.dcId, ProcessClass::LogRouter, req.logRouterCount, req.configuration, id_used);
		for (int i = 0; i < logRouters.size(); i++) {
			result.logRouters.push_back(logRouters[i].interf);
		}

		if (!clusterControllerData->goodRemoteRecruitmentTime.isReady() &&
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

	ACTOR static Future<Void> newSeedServers(Reference<ClusterRecoveryData> clusterRecoveryData,
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

	ACTOR static Future<Void> newCommitProxies(Reference<ClusterRecoveryData> clusterRecoveryData,
	                                           RecruitFromConfigurationReply recr) {
		std::vector<Future<CommitProxyInterface>> initializationReplies;
		for (int i = 0; i < recr.commitProxies.size(); i++) {
			InitializeCommitProxyRequest req;
			req.master = clusterRecoveryData->masterInterface;
			req.masterLifetime = clusterRecoveryData->masterLifetime;
			req.recoveryCount = clusterRecoveryData->cstate.myDBState.recoveryCount + 1;
			req.recoveryTransactionVersion = clusterRecoveryData->recoveryTransactionVersion;
			req.firstProxy = i == 0;
			req.encryptMode = Recruiter::getEncryptionAtRest(clusterRecoveryData->configuration);
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

	ACTOR static Future<Void> newGrvProxies(Reference<ClusterRecoveryData> clusterRecoveryData,
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

	ACTOR static Future<Void> newResolvers(Reference<ClusterRecoveryData> clusterRecoveryData,
	                                       RecruitFromConfigurationReply recr) {
		std::vector<Future<ResolverInterface>> initializationReplies;
		for (int i = 0; i < recr.resolvers.size(); i++) {
			InitializeResolverRequest req;
			req.masterLifetime = clusterRecoveryData->masterLifetime;
			req.recoveryCount = clusterRecoveryData->cstate.myDBState.recoveryCount + 1;
			req.commitProxyCount = recr.commitProxies.size();
			req.resolverCount = recr.resolvers.size();
			req.encryptMode = Recruiter::getEncryptionAtRest(clusterRecoveryData->configuration);
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

	ACTOR static Future<Void> newTLogServers(Recruiter* self,
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
			Future<RecruitRemoteFromConfigurationReply> fRemoteWorkers =
			    clusterRecruitRemoteFromConfiguration(self, clusterRecoveryData->controllerData, recruitWorkersInfo);

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
	    Recruiter* self,
	    ClusterControllerData* clusterControllerData,
	    Reference<RecruitRemoteWorkersInfo> req) {
		// At the moment this doesn't really need to be an actor (it always completes immediately)
		CODE_PROBE(true, "ClusterController RecruitTLogsRequest Remote");
		loop {
			try {
				auto rep = findRemoteWorkersForConfiguration(self, clusterControllerData, req->req);
				return rep;
			} catch (Error& e) {
				if (e.code() == error_code_no_more_servers &&
				    clusterControllerData->goodRemoteRecruitmentTime.isReady()) {
					self->outstandingRemoteRecruitmentRequests.push_back(req);
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

	ACTOR static Future<Void> clusterRecruitFromConfiguration(Recruiter* self,
	                                                          ClusterControllerData* clusterControllerData,
	                                                          Reference<RecruitWorkersInfo> req) {
		// At the moment this doesn't really need to be an actor (it always completes immediately)
		CODE_PROBE(true, "ClusterController RecruitTLogsRequest");
		loop {
			try {
				req->rep = findWorkersForConfiguration(self, clusterControllerData, req->req);
				return Void();
			} catch (Error& e) {
				if (e.code() == error_code_no_more_servers && clusterControllerData->goodRecruitmentTime.isReady()) {
					self->outstandingRecruitmentRequests.push_back(req);
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
	    Recruiter* self,
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
				    Recruiter::getEncryptionAtRest(clusterRecoveryData->configuration));
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
		wait(clusterRecruitFromConfiguration(self, clusterRecoveryData->controllerData, recruitWorkersInfo));
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
		wait(newSeedServers(clusterRecoveryData, recruits, seedServers));
		state std::vector<Standalone<CommitTransactionRef>> confChanges;
		wait(newCommitProxies(clusterRecoveryData, recruits) && newGrvProxies(clusterRecoveryData, recruits) &&
		     newResolvers(clusterRecoveryData, recruits) &&
		     newTLogServers(self, clusterRecoveryData, recruits, oldLogSystem, &confChanges));

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

Recruiter::Recruiter(UID const& id) : id(id), startTime(now()) {}

Future<std::vector<Standalone<CommitTransactionRef>>> Recruiter::recruitEverything(
    Reference<ClusterRecoveryData> clusterRecoveryData,
    std::vector<StorageServerInterface>& seedServers,
    Reference<ILogSystem> oldLogSystem) {
	return RecruiterImpl::recruitEverything(this, clusterRecoveryData, &seedServers, oldLogSystem);
}

void Recruiter::clusterRecruitStorage(RecruitStorageRequest req,
                                      bool gotProcessClasses,
                                      std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker) {
	try {
		if (!gotProcessClasses && !req.criticalRecruitment)
			throw no_more_servers();
		auto worker = RecruiterImpl::getStorageWorker(this, req, id_worker);
		RecruitStorageReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send(rep);
	} catch (Error& e) {
		if (e.code() == error_code_no_more_servers) {
			this->outstandingStorageRequests.emplace_back(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT);
			TraceEvent(SevWarn, "RecruitStorageNotAvailable", this->id)
			    .error(e)
			    .detail("IsCriticalRecruitment", req.criticalRecruitment);
		} else {
			TraceEvent(SevError, "RecruitStorageError", this->id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

void Recruiter::clusterRecruitBlobWorker(RecruitBlobWorkerRequest req,
                                         bool gotProcessClasses,
                                         std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker,
                                         Optional<Standalone<StringRef>> clusterControllerDcId) {
	try {
		if (!gotProcessClasses)
			throw no_more_servers();
		auto worker = RecruiterImpl::getBlobWorker(this, req, id_worker, clusterControllerDcId);
		RecruitBlobWorkerReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send(rep);
	} catch (Error& e) {
		if (e.code() == error_code_no_more_servers) {
			this->outstandingBlobWorkerRequests.emplace_back(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT);
			TraceEvent(SevWarn, "RecruitBlobWorkerNotAvailable", this->id).error(e);
		} else {
			TraceEvent(SevError, "RecruitBlobWorkerError", this->id).error(e);
			throw; // Any other error will bring down the cluster controller
		}
	}
}

void Recruiter::checkRegions(ClusterControllerData* clusterControllerData, const std::vector<RegionInfo>& regions) {
	if (clusterControllerData->desiredDcIds.get().present() &&
	    clusterControllerData->desiredDcIds.get().get().size() == 2 &&
	    clusterControllerData->desiredDcIds.get().get()[0].get() == regions[0].dcId &&
	    clusterControllerData->desiredDcIds.get().get()[1].get() == regions[1].dcId) {
		return;
	}

	try {
		std::map<Optional<Standalone<StringRef>>, int> id_used;
		Recruiter::getWorkerForRoleInDatacenter(clusterControllerData,
		                                        regions[0].dcId,
		                                        ProcessClass::ClusterController,
		                                        ProcessClass::ExcludeFit,
		                                        clusterControllerData->db.config,
		                                        id_used,
		                                        {},
		                                        true);
		Recruiter::getWorkerForRoleInDatacenter(clusterControllerData,
		                                        regions[0].dcId,
		                                        ProcessClass::Master,
		                                        ProcessClass::ExcludeFit,
		                                        clusterControllerData->db.config,
		                                        id_used,
		                                        {},
		                                        true);

		std::set<Optional<Key>> primaryDC;
		primaryDC.insert(regions[0].dcId);
		Recruiter::getWorkersForTLogs(clusterControllerData,
		                              clusterControllerData->db.config,
		                              clusterControllerData->db.config.tLogReplicationFactor,
		                              clusterControllerData->db.config.getDesiredLogs(),
		                              clusterControllerData->db.config.tLogPolicy,
		                              id_used,
		                              true,
		                              primaryDC);
		if (regions[0].satelliteTLogReplicationFactor > 0 && clusterControllerData->db.config.usableRegions > 1) {
			bool satelliteFallback = false;
			Recruiter::getWorkersForSatelliteLogs(clusterControllerData,
			                                      clusterControllerData->db.config,
			                                      regions[0],
			                                      regions[1],
			                                      id_used,
			                                      satelliteFallback,
			                                      true);
		}

		Recruiter::getWorkerForRoleInDatacenter(clusterControllerData,
		                                        regions[0].dcId,
		                                        ProcessClass::Resolver,
		                                        ProcessClass::ExcludeFit,
		                                        clusterControllerData->db.config,
		                                        id_used,
		                                        {},
		                                        true);
		Recruiter::getWorkerForRoleInDatacenter(clusterControllerData,
		                                        regions[0].dcId,
		                                        ProcessClass::CommitProxy,
		                                        ProcessClass::ExcludeFit,
		                                        clusterControllerData->db.config,
		                                        id_used,
		                                        {},
		                                        true);
		Recruiter::getWorkerForRoleInDatacenter(clusterControllerData,
		                                        regions[0].dcId,
		                                        ProcessClass::GrvProxy,
		                                        ProcessClass::ExcludeFit,
		                                        clusterControllerData->db.config,
		                                        id_used,
		                                        {},
		                                        true);

		std::vector<Optional<Key>> dcPriority;
		dcPriority.push_back(regions[0].dcId);
		dcPriority.push_back(regions[1].dcId);
		clusterControllerData->desiredDcIds.set(dcPriority);
	} catch (Error& e) {
		if (e.code() != error_code_no_more_servers) {
			throw;
		}
	}
}

void Recruiter::checkOutstandingRecruitmentRequests(ClusterControllerData* clusterControllerData) {
	for (int i = 0; i < this->outstandingRecruitmentRequests.size(); i++) {
		Reference<RecruitWorkersInfo> info = this->outstandingRecruitmentRequests[i];
		try {
			info->rep = RecruiterImpl::findWorkersForConfiguration(this, clusterControllerData, info->req);
			if (info->dbgId.present()) {
				TraceEvent("CheckOutstandingRecruitment", info->dbgId.get())
				    .detail("Request", info->req.configuration.toString());
			}
			info->waitForCompletion.trigger();
			swapAndPop(&this->outstandingRecruitmentRequests, i--);
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers || e.code() == error_code_operation_failed) {
				TraceEvent(SevWarn, "RecruitTLogMatchingSetNotAvailable", this->id).error(e);
			} else {
				TraceEvent(SevError, "RecruitTLogsRequestError", this->id).error(e);
				throw;
			}
		}
	}
}

void Recruiter::checkOutstandingRemoteRecruitmentRequests(ClusterControllerData const* clusterControllerData) {
	for (int i = 0; i < this->outstandingRemoteRecruitmentRequests.size(); i++) {
		Reference<RecruitRemoteWorkersInfo> info = this->outstandingRemoteRecruitmentRequests[i];
		try {
			info->rep = RecruiterImpl::findRemoteWorkersForConfiguration(this, clusterControllerData, info->req);
			if (info->dbgId.present()) {
				TraceEvent("CheckOutstandingRemoteRecruitment", info->dbgId.get())
				    .detail("Request", info->req.configuration.toString());
			}
			info->waitForCompletion.trigger();
			swapAndPop(&this->outstandingRemoteRecruitmentRequests, i--);
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers || e.code() == error_code_operation_failed) {
				TraceEvent(SevWarn, "RecruitRemoteTLogMatchingSetNotAvailable", this->id).error(e);
			} else {
				TraceEvent(SevError, "RecruitRemoteTLogsRequestError", this->id).error(e);
				throw;
			}
		}
	}
}

void Recruiter::checkOutstandingStorageRequests(
    bool gotProcessClasses,
    std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker) {
	for (int i = 0; i < this->outstandingStorageRequests.size(); i++) {
		auto& req = this->outstandingStorageRequests[i];
		try {
			if (req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop(&this->outstandingStorageRequests, i--);
			} else {
				if (!gotProcessClasses && !req.first.criticalRecruitment)
					throw no_more_servers();

				auto worker = RecruiterImpl::getStorageWorker(this, req.first, id_worker);
				RecruitStorageReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send(rep);
				swapAndPop(&this->outstandingStorageRequests, i--);
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitStorageNotAvailable", this->id)
				    .errorUnsuppressed(e)
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i)
				    .detail("IsCriticalRecruitment", req.first.criticalRecruitment);
			} else {
				TraceEvent(SevError, "RecruitStorageError", this->id).error(e);
				throw;
			}
		}
	}
}

// When workers aren't available at the time of request, the request
// gets added to a list of outstanding reqs. Here, we try to resolve these
// outstanding requests.
void Recruiter::checkOutstandingBlobWorkerRequests(
    bool gotProcessClasses,
    std::map<Optional<Standalone<StringRef>>, WorkerInfo> const& id_worker,
    Optional<Standalone<StringRef>> clusterControllerDcId) {
	for (int i = 0; i < this->outstandingBlobWorkerRequests.size(); i++) {
		auto& req = this->outstandingBlobWorkerRequests[i];
		try {
			if (req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop(&this->outstandingBlobWorkerRequests, i--);
			} else {
				if (!gotProcessClasses)
					throw no_more_servers();

				auto worker = RecruiterImpl::getBlobWorker(this, req.first, id_worker, clusterControllerDcId);
				RecruitBlobWorkerReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send(rep);
				// can remove it once we know the worker was found
				swapAndPop(&this->outstandingBlobWorkerRequests, i--);
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitBlobWorkerNotAvailable", this->id)
				    .errorUnsuppressed(e)
				    .suppressFor(1.0)
				    .detail("OutstandingReq", i);
			} else {
				TraceEvent(SevError, "RecruitBlobWorkerError", this->id).error(e);
				throw;
			}
		}
	}
}

void Recruiter::updateKnownIds(ClusterControllerData const* clusterControllerData,
                               std::map<Optional<Standalone<StringRef>>, int>* id_used) {
	(*id_used)[clusterControllerData->masterProcessId]++;
	(*id_used)[clusterControllerData->clusterControllerProcessId]++;
}

WorkerFitnessInfo Recruiter::getWorkerForRoleInDatacenter(
    ClusterControllerData const* clusterControllerData,
    Optional<Standalone<StringRef>> const& dcId,
    ProcessClass::ClusterRole role,
    ProcessClass::Fitness unacceptableFitness,
    DatabaseConfiguration const& conf,
    std::map<Optional<Standalone<StringRef>>, int>& id_used,
    std::map<Optional<Standalone<StringRef>>, int> preferredSharing,
    bool checkStable) {
	std::map<std::tuple<ProcessClass::Fitness, int, bool, int>, std::vector<WorkerDetails>> fitness_workers;

	for (auto& it : clusterControllerData->id_worker) {
		auto fitness = it.second.details.processClass.machineClassFitness(role);
		if (conf.isExcludedServer(it.second.details.interf.addresses()) ||
		    isExcludedDegradedServer(clusterControllerData, it.second.details.interf.addresses())) {
			fitness = std::max(fitness, ProcessClass::ExcludeFit);
		}
		if (RecruiterImpl::workerAvailable(this, it.second, checkStable) && fitness < unacceptableFitness &&
		    it.second.details.interf.locality.dcId() == dcId) {
			auto sharing = preferredSharing.find(it.first);
			fitness_workers[std::make_tuple(fitness,
			                                id_used[it.first],
			                                isLongLivedStateless(clusterControllerData, it.first),
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

std::vector<WorkerDetails> Recruiter::getWorkersForRoleInDatacenter(
    ClusterControllerData const* clusterControllerData,
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

	for (auto& it : clusterControllerData->id_worker) {
		auto fitness = it.second.details.processClass.machineClassFitness(role);
		if (RecruiterImpl::workerAvailable(this, it.second, checkStable) &&
		    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
		    !isExcludedDegradedServer(clusterControllerData, it.second.details.interf.addresses()) &&
		    it.second.details.interf.locality.dcId() == dcId &&
		    (!minWorker.present() ||
		     (it.second.details.interf.id() != minWorker.get().worker.interf.id() &&
		      (fitness < minWorker.get().fitness ||
		       (fitness == minWorker.get().fitness && id_used[it.first] <= minWorker.get().used))))) {
			auto sharing = preferredSharing.find(it.first);
			fitness_workers[std::make_tuple(fitness,
			                                id_used[it.first],
			                                isLongLivedStateless(clusterControllerData, it.first),
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

std::vector<WorkerDetails> Recruiter::getWorkersForTLogs(ClusterControllerData const* clusterControllerData,
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

				auto workers = RecruiterImpl::getWorkersForTLogsComplex(this,
				                                                        clusterControllerData,
				                                                        conf,
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
						auto testWorkers = RecruiterImpl::getWorkersForTLogsBackup(this,
						                                                           clusterControllerData,
						                                                           conf,
						                                                           required,
						                                                           desired,
						                                                           policy,
						                                                           testUsed,
						                                                           checkStable,
						                                                           dcIds,
						                                                           exclusionWorkerIds);
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

		auto workers = RecruiterImpl::getWorkersForTLogsSimple(
		    this, clusterControllerData, conf, required, desired, id_used, checkStable, dcIds, exclusionWorkerIds);

		if (g_network->isSimulated()) {
			try {
				auto testWorkers = RecruiterImpl::getWorkersForTLogsBackup(this,
				                                                           clusterControllerData,
				                                                           conf,
				                                                           required,
				                                                           desired,
				                                                           policy,
				                                                           testUsed,
				                                                           checkStable,
				                                                           dcIds,
				                                                           exclusionWorkerIds);
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
	return RecruiterImpl::getWorkersForTLogsBackup(
	    this, clusterControllerData, conf, required, desired, policy, id_used, checkStable, dcIds, exclusionWorkerIds);
}

// FIXME: This logic will fallback unnecessarily when usable dcs > 1 because it does not check all combinations of
// potential satellite locations
std::vector<WorkerDetails> Recruiter::getWorkersForSatelliteLogs(
    ClusterControllerData const* clusterControllerData,
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
				if (!clusterControllerData->goodRecruitmentTime.isReady()) {
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
				auto remoteLogs = getWorkersForTLogs(clusterControllerData,
				                                     conf,
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
				return getWorkersForTLogs(clusterControllerData,
				                          conf,
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
				return getWorkersForTLogs(clusterControllerData,
				                          conf,
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

EncryptionAtRestMode Recruiter::getEncryptionAtRest(DatabaseConfiguration config) {
	TraceEvent(SevDebug, "CREncryptionAtRestMode").detail("Mode", config.encryptionAtRestMode.toString());
	return config.encryptionAtRestMode;
}
