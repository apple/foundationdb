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
#include <iterator>
#include <map>
#include <set>
#include <vector>

#include "fdbrpc/FailureMonitor.h"
#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MoveKeys.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/LeaderElection.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/Status.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/RecoveryState.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbclient/KeyBackedTypes.h"
#include "flow/Util.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

void failAfter( Future<Void> trigger, Endpoint e );

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

	WorkerInfo() : gen(-1), reboots(0), priorityInfo(ProcessClass::UnsetFit, false, ClusterControllerPriorityInfo::FitnessUnknown) {}
	WorkerInfo( Future<Void> watcher, ReplyPromise<RegisterWorkerReply> reply, Generation gen, WorkerInterface interf, ProcessClass initialClass, ProcessClass processClass, ClusterControllerPriorityInfo priorityInfo, bool degraded, Standalone<VectorRef<StringRef>> issues ) :
		watcher(watcher), reply(reply), gen(gen), reboots(0), initialClass(initialClass), priorityInfo(priorityInfo), details(interf, processClass, degraded), issues(issues) {}

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
	WorkerFitnessInfo(WorkerDetails worker, ProcessClass::Fitness fitness, int used) : worker(worker), fitness(fitness), used(used) {}
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
		DatabaseConfiguration config;   // Asynchronously updated via master registration
		DatabaseConfiguration fullyRecoveredConfig;
		Database db;
		int unfinishedRecoveries;
		int logGenerations;
		bool cachePopulated;
		std::map<NetworkAddress, std::pair<double, OpenDatabaseRequest>> clientStatus;

		DBInfo()
		  : masterRegistrationCount(0), recoveryStalled(false), forceRecovery(false), unfinishedRecoveries(0),
		    logGenerations(0), cachePopulated(false), clientInfo(new AsyncVar<ClientDBInfo>()), dbInfoCount(0),
		    serverInfo(new AsyncVar<ServerDBInfo>()),
		    db(DatabaseContext::create(clientInfo, Future<Void>(), LocalityData(), true, TaskPriority::DefaultEndpoint,
		                               true)) // SOMEDAY: Locality!
		{}

		void setDistributor(const DataDistributorInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.distributor = interf;
			serverInfo->set( newInfo );
		}

		void setRatekeeper(const RatekeeperInterface& interf) {
			auto newInfo = serverInfo->get();
			newInfo.id = deterministicRandom()->randomUniqueID();
			newInfo.infoGeneration = ++dbInfoCount;
			newInfo.ratekeeper = interf;
			serverInfo->set( newInfo );
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
			serverInfo->set( newInfo );
		}

	};

	struct UpdateWorkerList {
		Future<Void> init( Database const& db ) {
			return update(this, db);
		}

		void set(Optional<Standalone<StringRef>> processID, Optional<ProcessData> data ) {
			delta[processID] = data;
			anyDelta.set(true);
		}

	private:
		std::map<Optional<Standalone<StringRef>>, Optional<ProcessData>> delta;
		AsyncVar<bool> anyDelta;

		ACTOR static Future<Void> update( UpdateWorkerList* self, Database db ) {
			// The Database we are using is based on worker registrations to this cluster controller, which come only
			// from master servers that we started, so it shouldn't be possible for multiple cluster controllers to fight.
			state Transaction tr(db);
			loop {
				try {
					tr.clear( workerListKeys );
					wait( tr.commit() );
					break;
				} catch (Error& e) {
					wait( tr.onError(e) );
				}
			}

			loop {
				// Wait for some changes
				while (!self->anyDelta.get())
					wait( self->anyDelta.onChange() );
				self->anyDelta.set(false);

				state std::map<Optional<Standalone<StringRef>>, Optional<ProcessData>> delta;
				delta.swap( self->delta );

				TraceEvent("UpdateWorkerList").detail("DeltaCount", delta.size());

				// Do a transaction to write the changes
				loop {
					try {
						for(auto w = delta.begin(); w != delta.end(); ++w) {
							if (w->second.present()) {
								tr.set( workerListKeyFor( w->first.get() ), workerListValue( w->second.get()) );
							} else
								tr.clear( workerListKeyFor( w->first.get() ) );
						}
						wait( tr.commit() );
						break;
					} catch (Error& e) {
						wait( tr.onError(e) );
					}
				}
			}
		}
	};

	bool workerAvailable( WorkerInfo const& worker, bool checkStable ) {
		return ( now() - startTime < 2 * FLOW_KNOBS->SERVER_REQUEST_INTERVAL ) || ( IFailureMonitor::failureMonitor().getState(worker.details.interf.storage.getEndpoint()).isAvailable() && ( !checkStable || worker.reboots < 2 ) );
	}

	bool isLongLivedStateless( Optional<Key> const& processId ) {
		return (db.serverInfo->get().distributor.present() && db.serverInfo->get().distributor.get().locality.processId() == processId) ||
				   (db.serverInfo->get().ratekeeper.present() && db.serverInfo->get().ratekeeper.get().locality.processId() == processId);
	}

	WorkerDetails getStorageWorker( RecruitStorageRequest const& req ) {
		std::set<Optional<Standalone<StringRef>>> excludedMachines( req.excludeMachines.begin(), req.excludeMachines.end() );
		std::set<Optional<Standalone<StringRef>>> includeDCs( req.includeDCs.begin(), req.includeDCs.end() );
		std::set<AddressExclusion> excludedAddresses( req.excludeAddresses.begin(), req.excludeAddresses.end() );

		for( auto& it : id_worker )
			if( workerAvailable( it.second, false ) &&
					!excludedMachines.count(it.second.details.interf.locality.zoneId()) &&
					( includeDCs.size() == 0 || includeDCs.count(it.second.details.interf.locality.dcId()) ) &&
					!addressExcluded(excludedAddresses, it.second.details.interf.address()) &&
					( !it.second.details.interf.secondaryAddress().present() || !addressExcluded(excludedAddresses, it.second.details.interf.secondaryAddress().get()) ) &&
					it.second.details.processClass.machineClassFitness( ProcessClass::Storage ) <= ProcessClass::UnsetFit ) {
				return it.second.details;
			}

		if( req.criticalRecruitment ) {
			ProcessClass::Fitness bestFit = ProcessClass::NeverAssign;
			Optional<WorkerDetails> bestInfo;
			for( auto& it : id_worker ) {
				ProcessClass::Fitness fit = it.second.details.processClass.machineClassFitness( ProcessClass::Storage );
				if( workerAvailable( it.second, false ) &&
						!excludedMachines.count(it.second.details.interf.locality.zoneId()) &&
						( includeDCs.size() == 0 || includeDCs.count(it.second.details.interf.locality.dcId()) ) &&
						!addressExcluded(excludedAddresses, it.second.details.interf.address()) &&
						fit < bestFit ) {
					bestFit = fit;
					bestInfo = it.second.details;
				}
			}

			if( bestInfo.present() ) {
				return bestInfo.get();
			}
		}

		throw no_more_servers();
	}

	std::vector<WorkerDetails> getWorkersForSeedServers( DatabaseConfiguration const& conf, Reference<IReplicationPolicy> const& policy, Optional<Optional<Standalone<StringRef>>> const& dcId = Optional<Optional<Standalone<StringRef>>>() ) {
		std::map<ProcessClass::Fitness, vector<WorkerDetails>> fitness_workers;
		std::vector<WorkerDetails> results;
		Reference<LocalitySet> logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
		LocalityMap<WorkerDetails>* logServerMap = (LocalityMap<WorkerDetails>*) logServerSet.getPtr();
		bool bCompleted = false;

		for( auto& it : id_worker ) {
			auto fitness = it.second.details.processClass.machineClassFitness( ProcessClass::Storage );
			if( workerAvailable(it.second, false) && !conf.isExcludedServer(it.second.details.interf.addresses()) && fitness != ProcessClass::NeverAssign && ( !dcId.present() || it.second.details.interf.locality.dcId()==dcId.get() ) ) {
				fitness_workers[ fitness ].push_back(it.second.details);
			}
		}

		for( auto& it : fitness_workers ) {
			for (auto& worker : it.second ) {
				logServerMap->add(worker.interf.locality, &worker);
			}

			std::vector<LocalityEntry> bestSet;
			if( logServerSet->selectReplicas(policy, bestSet) ) {
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

	std::vector<WorkerDetails> getWorkersForTlogs(DatabaseConfiguration const& conf, int32_t required, int32_t desired,
	                                              Reference<IReplicationPolicy> const& policy,
	                                              std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                              bool checkStable = false,
	                                              std::set<Optional<Key>> dcIds = std::set<Optional<Key>>(),
	                                              std::vector<UID> exclusionWorkerIds = {}) {
		std::map<std::pair<ProcessClass::Fitness,bool>, vector<WorkerDetails>> fitness_workers;
		std::vector<WorkerDetails> results;
		std::vector<LocalityData> unavailableLocals;
		Reference<LocalitySet> logServerSet;
		LocalityMap<WorkerDetails>* logServerMap;
		bool bCompleted = false;

		logServerSet = Reference<LocalitySet>(new LocalityMap<WorkerDetails>());
		logServerMap = (LocalityMap<WorkerDetails>*) logServerSet.getPtr();
		for( auto& it : id_worker ) {
			if (std::find(exclusionWorkerIds.begin(), exclusionWorkerIds.end(), it.second.details.interf.id()) == exclusionWorkerIds.end()) {
				auto fitness = it.second.details.processClass.machineClassFitness(ProcessClass::TLog);
				if (workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.details.interf.addresses()) && fitness != ProcessClass::NeverAssign && (!dcIds.size() || dcIds.count(it.second.details.interf.locality.dcId()))) {
					fitness_workers[std::make_pair(fitness, it.second.details.degraded)].push_back(it.second.details);
				}
				else {
					unavailableLocals.push_back(it.second.details.interf.locality);
				}
			}
		}

		results.reserve(results.size() + id_worker.size());
		for (int fitness = ProcessClass::BestFit; fitness != ProcessClass::NeverAssign && !bCompleted; fitness++)
		{
			auto fitnessEnum = (ProcessClass::Fitness) fitness;
			for(int addingDegraded = 0; addingDegraded < 2; addingDegraded++) {
				auto workerItr = fitness_workers.find(std::make_pair(fitnessEnum,(bool)addingDegraded));
				if (workerItr != fitness_workers.end()) {
					for (auto& worker : workerItr->second ) {
						logServerMap->add(worker.interf.locality, &worker);
					}
				}

				if (logServerSet->size() < (addingDegraded == 0 ? desired : required)) {
				}
				else if (logServerSet->size() == required || logServerSet->size() <= desired) {
					if (logServerSet->validate(policy)) {
						for (auto& object : logServerMap->getObjects()) {
							results.push_back(*object);
						}
						bCompleted = true;
						break;
					}
					TraceEvent(SevWarn, "GWFTADNotAcceptable", id)
					    .detail("Fitness", fitness)
					    .detail("Processes", logServerSet->size())
					    .detail("Required", required)
					    .detail("TLogPolicy", policy->info())
					    .detail("DesiredLogs", desired)
					    .detail("AddingDegraded", addingDegraded);
				}
				// Try to select the desired size, if larger
				else {
					std::vector<LocalityEntry> bestSet;
					std::vector<LocalityData> tLocalities;

					// Try to find the best team of servers to fulfill the policy
					if (findBestPolicySet(bestSet, logServerSet, policy, desired, SERVER_KNOBS->POLICY_RATING_TESTS,
					                      SERVER_KNOBS->POLICY_GENERATIONS)) {
						results.reserve(results.size() + bestSet.size());
						for (auto& entry : bestSet) {
							auto object = logServerMap->getObject(entry);
							ASSERT(object);
							results.push_back(*object);
							tLocalities.push_back(object->interf.locality);
						}
						TraceEvent("GWFTADBestResults", id).detail("Fitness", fitness).detail("Processes", logServerSet->size()).detail("BestCount", bestSet.size()).detail("BestZones", ::describeZones(tLocalities))
							.detail("BestDataHalls", ::describeDataHalls(tLocalities)).detail("TLogPolicy", policy->info()).detail("TotalResults", results.size()).detail("DesiredLogs", desired).detail("AddingDegraded", addingDegraded);
						bCompleted = true;
						break;
					}
					TraceEvent(SevWarn,"GWFTADNoBest", id).detail("Fitness", fitness).detail("Processes", logServerSet->size()).detail("Required", required).detail("TLogPolicy", policy->info()).detail("DesiredLogs", desired).detail("AddingDegraded", addingDegraded);
				}
			}
		}

		// If policy cannot be satisfied
		if (!bCompleted) {
			std::vector<LocalityData> tLocalities;
			for (auto& object : logServerMap->getObjects()) {
				tLocalities.push_back(object->interf.locality);
			}

			TraceEvent(SevWarn, "GetTLogTeamFailed")
			    .detail("Policy", policy->info())
			    .detail("Processes", logServerSet->size())
			    .detail("Workers", id_worker.size())
			    .detail("FitnessGroups", fitness_workers.size())
			    .detail("TLogZones", ::describeZones(tLocalities))
			    .detail("TLogDataHalls", ::describeDataHalls(tLocalities))
			    .detail("MissingZones", ::describeZones(unavailableLocals))
			    .detail("MissingDataHalls", ::describeDataHalls(unavailableLocals))
			    .detail("Required", required)
			    .detail("DesiredLogs", desired)
			    .detail("RatingTests", SERVER_KNOBS->POLICY_RATING_TESTS)
			    .detail("CheckStable", checkStable)
			    .detail("NumExclusionWorkers", exclusionWorkerIds.size())
			    .detail("PolicyGenerations", SERVER_KNOBS->POLICY_GENERATIONS)
			    .backtrace();

			logServerSet->clear();
			logServerSet.clear();
			throw no_more_servers();
		}

		for (auto& result : results) {
			id_used[result.interf.locality.processId()]++;
		}

		TraceEvent("GetTLogTeamDone").detail("Completed", bCompleted).detail("Policy", policy->info()).detail("Results", results.size()).detail("Processes", logServerSet->size()).detail("Workers", id_worker.size())
			.detail("Required", required).detail("Desired", desired).detail("RatingTests",SERVER_KNOBS->POLICY_RATING_TESTS).detail("PolicyGenerations",SERVER_KNOBS->POLICY_GENERATIONS);

		return results;
	}

	//FIXME: This logic will fallback unnecessarily when usable dcs > 1 because it does not check all combinations of potential satellite locations
	std::vector<WorkerDetails> getWorkersForSatelliteLogs( const DatabaseConfiguration& conf, const RegionInfo& region, const RegionInfo& remoteRegion, std::map< Optional<Standalone<StringRef>>, int>& id_used, bool& satelliteFallback, bool checkStable = false ) {
		int startDC = 0;
		loop {
			if(startDC > 0 && startDC >= region.satellites.size() + 1 - (satelliteFallback ? region.satelliteTLogUsableDcsFallback : region.satelliteTLogUsableDcs)) {
				if(satelliteFallback || region.satelliteTLogUsableDcsFallback == 0) {
					throw no_more_servers();
				} else {
					if(!goodRecruitmentTime.isReady()) {
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
				for(int s = startDC; s < std::min<int>(startDC + (satelliteFallback ? region.satelliteTLogUsableDcsFallback : region.satelliteTLogUsableDcs), region.satellites.size()); s++) {
					satelliteDCs.insert(region.satellites[s].dcId);
					if(region.satellites[s].satelliteDesiredTLogCount == -1 || desiredSatelliteTLogs == -1) {
						desiredSatelliteTLogs = -1;
					} else {
						desiredSatelliteTLogs += region.satellites[s].satelliteDesiredTLogCount;
					}
					if (region.satellites[s].dcId == remoteRegion.dcId) {
						remoteDCUsedAsSatellite = true;
					}
				}
				std::vector<UID> exclusionWorkerIds;
				// FIXME: If remote DC is used as satellite then this logic only ensures that required number of remote TLogs can be recruited. It does not balance the number of desired TLogs
				// across the satellite and remote sides.
				if (remoteDCUsedAsSatellite) {
					std::map< Optional<Standalone<StringRef>>, int> tmpIdUsed;
					auto remoteLogs = getWorkersForTlogs(conf, conf.getRemoteTLogReplicationFactor(), conf.getRemoteTLogReplicationFactor(), conf.getRemoteTLogPolicy(), tmpIdUsed, false, { remoteRegion.dcId }, {});
					std::transform(remoteLogs.begin(), remoteLogs.end(), std::back_inserter(exclusionWorkerIds), [](const WorkerDetails &in) { return in.interf.id(); });
				}
				if(satelliteFallback) {
					return getWorkersForTlogs( conf, region.satelliteTLogReplicationFactorFallback, desiredSatelliteTLogs>0 ? desiredSatelliteTLogs : conf.getDesiredSatelliteLogs(region.dcId)*region.satelliteTLogUsableDcsFallback/region.satelliteTLogUsableDcs, region.satelliteTLogPolicyFallback, id_used, checkStable, satelliteDCs, exclusionWorkerIds);
				} else {
					return getWorkersForTlogs( conf, region.satelliteTLogReplicationFactor, desiredSatelliteTLogs>0 ? desiredSatelliteTLogs : conf.getDesiredSatelliteLogs(region.dcId), region.satelliteTLogPolicy, id_used, checkStable, satelliteDCs, exclusionWorkerIds);
				}
			} catch (Error &e) {
				if(e.code() != error_code_no_more_servers) {
					throw;
				}
			}

			startDC++;
		}
	}

	ProcessClass::Fitness getBestFitnessForRoleInDatacenter(ProcessClass::ClusterRole role) {
		ProcessClass::Fitness bestFitness = ProcessClass::NeverAssign;
		for (const auto& it : id_worker) {
			if (it.second.priorityInfo.isExcluded || it.second.details.interf.locality.dcId() != clusterControllerDcId) {
				continue;
			}
			bestFitness = std::min(bestFitness, it.second.details.processClass.machineClassFitness(role));
		}
		return bestFitness;
	}

	WorkerFitnessInfo getWorkerForRoleInDatacenter(Optional<Standalone<StringRef>> const& dcId, ProcessClass::ClusterRole role, ProcessClass::Fitness unacceptableFitness, DatabaseConfiguration const& conf, std::map< Optional<Standalone<StringRef>>, int>& id_used, bool checkStable = false ) {
		std::map<std::pair<ProcessClass::Fitness,int>, std::pair<vector<WorkerDetails>,vector<WorkerDetails>>> fitness_workers;

		for( auto& it : id_worker ) {
			auto fitness = it.second.details.processClass.machineClassFitness( role );
			if(conf.isExcludedServer(it.second.details.interf.addresses())) {
				fitness = std::max(fitness, ProcessClass::ExcludeFit);
			}
			if( workerAvailable(it.second, checkStable) && fitness < unacceptableFitness && it.second.details.interf.locality.dcId()==dcId ) {
				if (isLongLivedStateless(it.first)) {
					fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].second.push_back(it.second.details);
				} else {
					fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].first.push_back(it.second.details);
				}
			}
		}

		for( auto& it : fitness_workers ) {
			for( int j=0; j < 2; j++ ) {
				auto& w = j==0 ? it.second.first : it.second.second;
				deterministicRandom()->randomShuffle(w);
				for( int i=0; i < w.size(); i++ ) {
					id_used[w[i].interf.locality.processId()]++;
					return WorkerFitnessInfo(w[i], std::max(ProcessClass::GoodFit, it.first.first), it.first.second);
				}
			}
		}

		throw no_more_servers();
	}

	vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    Optional<Standalone<StringRef>> const& dcId, ProcessClass::ClusterRole role, int amount,
	    DatabaseConfiguration const& conf, std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    Optional<WorkerFitnessInfo> minWorker = Optional<WorkerFitnessInfo>(), bool checkStable = false) {
		std::map<std::pair<ProcessClass::Fitness, int>, std::pair<vector<WorkerDetails>, vector<WorkerDetails>>>
		    fitness_workers;
		vector<WorkerDetails> results;
		if (minWorker.present()) {
			results.push_back(minWorker.get().worker);
		}
		if (amount <= results.size()) {
			return results;
		}

		for (auto& it : id_worker) {
			auto fitness = it.second.details.processClass.machineClassFitness( role );
			if (workerAvailable(it.second, checkStable) &&
			    !conf.isExcludedServer(it.second.details.interf.addresses()) &&
			    it.second.details.interf.locality.dcId() == dcId &&
			    (!minWorker.present() ||
			     (it.second.details.interf.id() != minWorker.get().worker.interf.id() &&
			      (fitness < minWorker.get().fitness ||
			       (fitness == minWorker.get().fitness && id_used[it.first] <= minWorker.get().used))))) {
				if (isLongLivedStateless(it.first)) {
					fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].second.push_back(it.second.details);
				} else {
					fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].first.push_back(it.second.details);
				}
			}
		}

		for( auto& it : fitness_workers ) {
			for( int j=0; j < 2; j++ ) {
				auto& w = j==0 ? it.second.first : it.second.second;
				deterministicRandom()->randomShuffle(w);
				for( int i=0; i < w.size(); i++ ) {
					results.push_back(w[i]);
					id_used[w[i].interf.locality.processId()]++;
					if (results.size() == amount) return results;
				}
			}
		}

		return results;
	}

	struct RoleFitness {
		ProcessClass::Fitness bestFit;
		ProcessClass::Fitness worstFit;
		ProcessClass::ClusterRole role;
		int count;
		bool worstIsDegraded;

		RoleFitness(int bestFit, int worstFit, int count, ProcessClass::ClusterRole role) : bestFit((ProcessClass::Fitness)bestFit), worstFit((ProcessClass::Fitness)worstFit), count(count), role(role), worstIsDegraded(false) {}

		RoleFitness(int fitness, int count, ProcessClass::ClusterRole role) : bestFit((ProcessClass::Fitness)fitness), worstFit((ProcessClass::Fitness)fitness), count(count), role(role), worstIsDegraded(false) {}

		RoleFitness() : bestFit(ProcessClass::NeverAssign), worstFit(ProcessClass::NeverAssign), role(ProcessClass::NoRole), count(0), worstIsDegraded(false) {}

		RoleFitness( vector<WorkerDetails> workers, ProcessClass::ClusterRole role ) : role(role) {
			worstFit = ProcessClass::GoodFit;
			worstIsDegraded = false;
			bestFit = ProcessClass::NeverAssign;
			for(auto& it : workers) {
				auto thisFit = it.processClass.machineClassFitness( role );
				if(thisFit > worstFit) {
					worstFit = thisFit;
					worstIsDegraded = it.degraded;
				} else if(thisFit == worstFit) {
					worstIsDegraded = worstIsDegraded || it.degraded;
				}
				bestFit = std::min(bestFit, thisFit);
			}
			count = workers.size();
			//degraded is only used for recruitment of tlogs
			if(role != ProcessClass::TLog) {
				worstIsDegraded = false;
			}
		}

		bool operator < (RoleFitness const& r) const {
			if (worstFit != r.worstFit) return worstFit < r.worstFit;
			if (worstIsDegraded != r.worstIsDegraded) return r.worstIsDegraded;
			// FIXME: TLog recruitment process does not guarantee the best fit is not worsened.
			if (role != ProcessClass::TLog && role != ProcessClass::LogRouter && bestFit != r.bestFit) return bestFit < r.bestFit;
			return count > r.count;
		}
		bool operator>(RoleFitness const& r) const { return r < *this; }
		bool operator<=(RoleFitness const& r) const { return !(*this > r); }
		bool operator>=(RoleFitness const& r) const { return !(*this < r); }

		bool betterFitness (RoleFitness const& r) const {
			if (worstFit != r.worstFit) return worstFit < r.worstFit;
			if (worstIsDegraded != r.worstIsDegraded) return r.worstFit;
			if (bestFit != r.bestFit) return bestFit < r.bestFit;
			return false;
		}

		bool betterCount (RoleFitness const& r) const {
			if(count > r.count) return true;
			if(worstFit != r.worstFit) return worstFit < r.worstFit;
			if (worstIsDegraded != r.worstIsDegraded) return r.worstFit;
			return false;
		}

		bool operator == (RoleFitness const& r) const { return worstFit == r.worstFit && bestFit == r.bestFit && count == r.count && worstIsDegraded == r.worstIsDegraded; }

		std::string toString() const { return format("%d %d %d %d", bestFit, worstFit, count, worstIsDegraded); }
	};

	struct RoleFitnessPair {
		RoleFitness proxy;
		RoleFitness grvProxy;
		RoleFitness resolver;

		RoleFitnessPair() {}
		RoleFitnessPair(RoleFitness const& proxy, RoleFitness const& grvProxy, RoleFitness const& resolver)
		  : proxy(proxy), grvProxy(grvProxy), resolver(resolver) {}

		bool operator<(RoleFitnessPair const& r) const {
			if (proxy.betterFitness(r.proxy)) {
				return true;
			}
			if(r.proxy.betterFitness(proxy)) {
				return false;
			}
			if (grvProxy.betterFitness(r.grvProxy)) {
				return true;
			}
			if (r.grvProxy.betterFitness(grvProxy)) {
				return false;
			}
			if (resolver.betterFitness(r.resolver)) {
				return true;
			}
			if(r.resolver.betterFitness(resolver)) {
				return false;
			}
			if(proxy.count != r.proxy.count) {
				return proxy.count > r.proxy.count;
			}
			if (grvProxy.count != r.grvProxy.count) {
				return grvProxy.count > r.grvProxy.count;
			}
			return resolver.count > r.resolver.count;
		}
		bool operator>(RoleFitnessPair const& r) const { return r < *this; }
		bool operator<=(RoleFitnessPair const& r) const { return !(*this > r); }
		bool operator>=(RoleFitnessPair const& r) const { return !(*this < r); }

		bool operator==(RoleFitnessPair const& r) const {
			return proxy == r.proxy && grvProxy == r.grvProxy && resolver == r.resolver;
		}
	};

	std::set<Optional<Standalone<StringRef>>> getDatacenters( DatabaseConfiguration const& conf, bool checkStable = false ) {
		std::set<Optional<Standalone<StringRef>>> result;
		for( auto& it : id_worker )
			if( workerAvailable( it.second, checkStable ) && !conf.isExcludedServer( it.second.details.interf.addresses() ) )
				result.insert(it.second.details.interf.locality.dcId());
		return result;
	}

	void updateKnownIds(std::map< Optional<Standalone<StringRef>>, int>* id_used) {
		(*id_used)[masterProcessId]++;
		(*id_used)[clusterControllerProcessId]++;
	}

	RecruitRemoteFromConfigurationReply findRemoteWorkersForConfiguration( RecruitRemoteFromConfigurationRequest const& req ) {
		RecruitRemoteFromConfigurationReply result;
		std::map< Optional<Standalone<StringRef>>, int> id_used;

		updateKnownIds(&id_used);

		std::set<Optional<Key>> remoteDC;
		remoteDC.insert(req.dcId);

		auto remoteLogs = getWorkersForTlogs( req.configuration, req.configuration.getRemoteTLogReplicationFactor(), req.configuration.getDesiredRemoteLogs(), req.configuration.getRemoteTLogPolicy(), id_used, false, remoteDC, req.exclusionWorkerIds );
		for(int i = 0; i < remoteLogs.size(); i++) {
			result.remoteTLogs.push_back(remoteLogs[i].interf);
		}

		auto logRouters = getWorkersForRoleInDatacenter( req.dcId, ProcessClass::LogRouter, req.logRouterCount, req.configuration, id_used );
		for(int i = 0; i < logRouters.size(); i++) {
			result.logRouters.push_back(logRouters[i].interf);
		}


		if( !goodRemoteRecruitmentTime.isReady() &&
			( ( RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredRemoteLogs(), ProcessClass::TLog).betterCount(RoleFitness(remoteLogs, ProcessClass::TLog)) ) ||
			  ( RoleFitness(SERVER_KNOBS->EXPECTED_LOG_ROUTER_FITNESS, req.logRouterCount, ProcessClass::LogRouter).betterCount(RoleFitness(logRouters, ProcessClass::LogRouter)) ) ) ) {
			throw operation_failed();
		}

		return result;
	}

	ErrorOr<RecruitFromConfigurationReply> findWorkersForConfiguration( RecruitFromConfigurationRequest const& req, Optional<Key> dcId ) {
		RecruitFromConfigurationReply result;
		std::map< Optional<Standalone<StringRef>>, int> id_used;
		updateKnownIds(&id_used);

		ASSERT(dcId.present());

		std::set<Optional<Key>> primaryDC;
		primaryDC.insert(dcId);
		result.dcId = dcId;

		RegionInfo region;
		RegionInfo remoteRegion;
		for(auto& r : req.configuration.regions) {
			if(r.dcId == dcId.get()) {
				region = r;
			}
			else {
				remoteRegion = r;
			}
		}

		if(req.recruitSeedServers) {
			auto primaryStorageServers = getWorkersForSeedServers( req.configuration, req.configuration.storagePolicy, dcId );
			for(int i = 0; i < primaryStorageServers.size(); i++) {
				result.storageServers.push_back(primaryStorageServers[i].interf);
			}
		}

		auto tlogs = getWorkersForTlogs( req.configuration, req.configuration.tLogReplicationFactor, req.configuration.getDesiredLogs(), req.configuration.tLogPolicy, id_used, false, primaryDC );
		for(int i = 0; i < tlogs.size(); i++) {
			result.tLogs.push_back(tlogs[i].interf);
		}

		std::vector<WorkerDetails> satelliteLogs;
		if (region.satelliteTLogReplicationFactor > 0 && req.configuration.usableRegions > 1) {
			satelliteLogs =
			    getWorkersForSatelliteLogs(req.configuration, region, remoteRegion, id_used, result.satelliteFallback);
			for(int i = 0; i < satelliteLogs.size(); i++) {
				result.satelliteTLogs.push_back(satelliteLogs[i].interf);
			}
		}

		auto first_commit_proxy = getWorkerForRoleInDatacenter(dcId, ProcessClass::CommitProxy,
		                                                       ProcessClass::ExcludeFit, req.configuration, id_used);
		auto first_grv_proxy = getWorkerForRoleInDatacenter(dcId, ProcessClass::GrvProxy, ProcessClass::ExcludeFit,
		                                                    req.configuration, id_used);
		auto first_resolver = getWorkerForRoleInDatacenter(dcId, ProcessClass::Resolver, ProcessClass::ExcludeFit,
		                                                   req.configuration, id_used);

		auto commit_proxies =
		    getWorkersForRoleInDatacenter(dcId, ProcessClass::CommitProxy, req.configuration.getDesiredCommitProxies(),
		                                  req.configuration, id_used, first_commit_proxy);
		auto grv_proxies =
		    getWorkersForRoleInDatacenter(dcId, ProcessClass::GrvProxy, req.configuration.getDesiredGrvProxies(),
		                                  req.configuration, id_used, first_grv_proxy);
		auto resolvers = getWorkersForRoleInDatacenter( dcId, ProcessClass::Resolver, req.configuration.getDesiredResolvers(), req.configuration, id_used, first_resolver );
		for (int i = 0; i < commit_proxies.size(); i++) result.commitProxies.push_back(commit_proxies[i].interf);
		for (int i = 0; i < grv_proxies.size(); i++) result.grvProxies.push_back(grv_proxies[i].interf);
		for(int i = 0; i < resolvers.size(); i++)
			result.resolvers.push_back(resolvers[i].interf);

		if(req.maxOldLogRouters > 0) {
			if(tlogs.size() == 1) {
				result.oldLogRouters.push_back(tlogs[0].interf);
			} else {
				for(int i = 0; i < tlogs.size(); i++) {
					if(tlogs[i].interf.locality.processId() != clusterControllerProcessId) {
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
			std::transform(backupWorkers.begin(), backupWorkers.end(), std::back_inserter(result.backupWorkers),
			               [](const WorkerDetails& w) { return w.interf; });
		}

		if (!goodRecruitmentTime.isReady() &&
		    (RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(), ProcessClass::TLog)
		         .betterCount(RoleFitness(tlogs, ProcessClass::TLog)) ||
		     (region.satelliteTLogReplicationFactor > 0 && req.configuration.usableRegions > 1 &&
		      RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredSatelliteLogs(dcId),
		                  ProcessClass::TLog)
		          .betterCount(RoleFitness(satelliteLogs, ProcessClass::TLog))) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_COMMIT_PROXY_FITNESS, req.configuration.getDesiredCommitProxies(),
		                 ProcessClass::CommitProxy)
		         .betterCount(RoleFitness(commit_proxies, ProcessClass::CommitProxy)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_GRV_PROXY_FITNESS, req.configuration.getDesiredGrvProxies(),
		                 ProcessClass::GrvProxy)
		         .betterCount(RoleFitness(grv_proxies, ProcessClass::GrvProxy)) ||
		     RoleFitness(SERVER_KNOBS->EXPECTED_RESOLVER_FITNESS, req.configuration.getDesiredResolvers(),
		                 ProcessClass::Resolver)
		         .betterCount(RoleFitness(resolvers, ProcessClass::Resolver)))) {
			return operation_failed();
		}

		return result;
	}

	RecruitFromConfigurationReply findWorkersForConfiguration( RecruitFromConfigurationRequest const& req ) {
		if(req.configuration.regions.size() > 1) {
			std::vector<RegionInfo> regions = req.configuration.regions;
			if(regions[0].priority == regions[1].priority && regions[1].dcId == clusterControllerDcId.get()) {
				std::swap(regions[0], regions[1]);
			}

			if(regions[1].dcId == clusterControllerDcId.get() && regions[1].priority >= 0 && (!versionDifferenceUpdated || datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE)) {
				std::swap(regions[0], regions[1]);
			}

			bool setPrimaryDesired = false;
			try {
				auto reply = findWorkersForConfiguration(req, regions[0].dcId);
				setPrimaryDesired = true;
				vector<Optional<Key>> dcPriority;
				dcPriority.push_back(regions[0].dcId);
				dcPriority.push_back(regions[1].dcId);
				desiredDcIds.set(dcPriority);
				if(reply.isError()) {
					throw reply.getError();
				} else if(regions[0].dcId == clusterControllerDcId.get()) {
					return reply.get();
				}
				throw no_more_servers();
			} catch( Error& e ) {
				if (!goodRemoteRecruitmentTime.isReady() && regions[1].dcId != clusterControllerDcId.get()) {
					throw operation_failed();
				}

				if (e.code() != error_code_no_more_servers || regions[1].priority < 0) {
					throw;
				}
				TraceEvent(SevWarn, "AttemptingRecruitmentInRemoteDC", id).error(e);
				auto reply = findWorkersForConfiguration(req, regions[1].dcId);
				if(!setPrimaryDesired) {
					vector<Optional<Key>> dcPriority;
					dcPriority.push_back(regions[1].dcId);
					dcPriority.push_back(regions[0].dcId);
					desiredDcIds.set(dcPriority);
				}
				if(reply.isError()) {
					throw reply.getError();
				} else if (regions[1].dcId == clusterControllerDcId.get()) {
					return reply.get();
				}
				throw;
			}
		} else if(req.configuration.regions.size() == 1) {
			vector<Optional<Key>> dcPriority;
			dcPriority.push_back(req.configuration.regions[0].dcId);
			desiredDcIds.set(dcPriority);
			auto reply = findWorkersForConfiguration(req, req.configuration.regions[0].dcId);
			if(reply.isError()) {
				throw reply.getError();
			} else if (req.configuration.regions[0].dcId == clusterControllerDcId.get()) {
				return reply.get();
			}
			throw no_more_servers();
		} else {
			RecruitFromConfigurationReply result;
			std::map< Optional<Standalone<StringRef>>, int> id_used;
			updateKnownIds(&id_used);
			auto tlogs = getWorkersForTlogs( req.configuration, req.configuration.tLogReplicationFactor, req.configuration.getDesiredLogs(), req.configuration.tLogPolicy, id_used );
			for(int i = 0; i < tlogs.size(); i++) {
				result.tLogs.push_back(tlogs[i].interf);
			}

			if(req.maxOldLogRouters > 0) {
				if(tlogs.size() == 1) {
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
				auto primaryStorageServers = getWorkersForSeedServers( req.configuration, req.configuration.storagePolicy );
				for(int i = 0; i < primaryStorageServers.size(); i++)
					result.storageServers.push_back(primaryStorageServers[i].interf);
			}

			auto datacenters = getDatacenters( req.configuration );

			RoleFitnessPair bestFitness;
			int numEquivalent = 1;
			Optional<Key> bestDC;

			for(auto dcId : datacenters ) {
				try {
					//SOMEDAY: recruitment in other DCs besides the clusterControllerDcID will not account for the processes used by the master and cluster controller properly.
					auto used = id_used;
					auto first_commit_proxy = getWorkerForRoleInDatacenter(
					    dcId, ProcessClass::CommitProxy, ProcessClass::ExcludeFit, req.configuration, used);
					auto first_grv_proxy = getWorkerForRoleInDatacenter(
					    dcId, ProcessClass::GrvProxy, ProcessClass::ExcludeFit, req.configuration, used);
					auto first_resolver = getWorkerForRoleInDatacenter(
					    dcId, ProcessClass::Resolver, ProcessClass::ExcludeFit, req.configuration, used);

					auto commit_proxies = getWorkersForRoleInDatacenter(dcId, ProcessClass::CommitProxy,
					                                                    req.configuration.getDesiredCommitProxies(),
					                                                    req.configuration, used, first_commit_proxy);
					auto grv_proxies = getWorkersForRoleInDatacenter(dcId, ProcessClass::GrvProxy,
					                                                 req.configuration.getDesiredGrvProxies(),
					                                                 req.configuration, used, first_grv_proxy);
					auto resolvers = getWorkersForRoleInDatacenter( dcId, ProcessClass::Resolver, req.configuration.getDesiredResolvers(), req.configuration, used, first_resolver );

					RoleFitnessPair fitness(RoleFitness(commit_proxies, ProcessClass::CommitProxy),
					                        RoleFitness(grv_proxies, ProcessClass::GrvProxy),
					                        RoleFitness(resolvers, ProcessClass::Resolver));

					if(dcId == clusterControllerDcId) {
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
							auto backupWorkers = getWorkersForRoleInDatacenter(dcId, ProcessClass::Backup, nBackup,
							                                                   req.configuration, id_used);
							std::transform(backupWorkers.begin(), backupWorkers.end(),
							               std::back_inserter(result.backupWorkers),
							               [](const WorkerDetails& w) { return w.interf; });
						}

						break;
					} else {
						if(fitness < bestFitness) {
							bestFitness = fitness;
							numEquivalent = 1;
							bestDC = dcId;
						} else if (fitness == bestFitness &&
						           deterministicRandom()->random01() < 1.0 / ++numEquivalent) {
							bestDC = dcId;
						}
					}
				} catch( Error &e ) {
					if(e.code() != error_code_no_more_servers) {
						throw;
					}
				}
			}

			if(bestDC != clusterControllerDcId) {
				vector<Optional<Key>> dcPriority;
				dcPriority.push_back(bestDC);
				desiredDcIds.set(dcPriority);
				throw no_more_servers();
			}
			//If this cluster controller dies, do not prioritize recruiting the next one in the same DC
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
			    (RoleFitness(SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs(),
			                 ProcessClass::TLog)
			         .betterCount(RoleFitness(tlogs, ProcessClass::TLog)) ||
			     RoleFitness(SERVER_KNOBS->EXPECTED_COMMIT_PROXY_FITNESS, req.configuration.getDesiredCommitProxies(),
			                 ProcessClass::CommitProxy)
			         .betterCount(bestFitness.proxy) ||
			     RoleFitness(SERVER_KNOBS->EXPECTED_GRV_PROXY_FITNESS, req.configuration.getDesiredGrvProxies(),
			                 ProcessClass::GrvProxy)
			         .betterCount(bestFitness.grvProxy) ||
			     RoleFitness(SERVER_KNOBS->EXPECTED_RESOLVER_FITNESS, req.configuration.getDesiredResolvers(),
			                 ProcessClass::Resolver)
			         .betterCount(bestFitness.resolver))) {
				throw operation_failed();
			}

			return result;
		}
	}

	void checkRegions(const std::vector<RegionInfo>& regions) {
		if(desiredDcIds.get().present() && desiredDcIds.get().get().size() == 2 && desiredDcIds.get().get()[0].get() == regions[0].dcId && desiredDcIds.get().get()[1].get() == regions[1].dcId) {
			return;
		}

		try {
			std::map< Optional<Standalone<StringRef>>, int> id_used;
			getWorkerForRoleInDatacenter(regions[0].dcId, ProcessClass::ClusterController, ProcessClass::ExcludeFit, db.config, id_used, true);
			getWorkerForRoleInDatacenter(regions[0].dcId, ProcessClass::Master, ProcessClass::ExcludeFit, db.config, id_used, true);

			std::set<Optional<Key>> primaryDC;
			primaryDC.insert(regions[0].dcId);
			getWorkersForTlogs(db.config, db.config.tLogReplicationFactor, db.config.getDesiredLogs(), db.config.tLogPolicy, id_used, true, primaryDC);
			if(regions[0].satelliteTLogReplicationFactor > 0 && db.config.usableRegions > 1) {
				bool satelliteFallback = false;
				getWorkersForSatelliteLogs(db.config, regions[0], regions[1], id_used, satelliteFallback, true);
			}

			getWorkerForRoleInDatacenter( regions[0].dcId, ProcessClass::Resolver, ProcessClass::ExcludeFit, db.config, id_used, true );
			getWorkerForRoleInDatacenter(regions[0].dcId, ProcessClass::CommitProxy, ProcessClass::ExcludeFit,
			                             db.config, id_used, true);
			getWorkerForRoleInDatacenter(regions[0].dcId, ProcessClass::GrvProxy, ProcessClass::ExcludeFit, db.config,
			                             id_used, true);

			vector<Optional<Key>> dcPriority;
			dcPriority.push_back(regions[0].dcId);
			dcPriority.push_back(regions[1].dcId);
			desiredDcIds.set(dcPriority);
		} catch( Error &e ) {
			if(e.code() != error_code_no_more_servers) {
				throw;
			}
		}
	}

	void checkRecoveryStalled() {
		if( (db.serverInfo->get().recoveryState == RecoveryState::RECRUITING || db.serverInfo->get().recoveryState == RecoveryState::ACCEPTING_COMMITS || db.serverInfo->get().recoveryState == RecoveryState::ALL_LOGS_RECRUITED) && db.recoveryStalled ) {
			if (db.config.regions.size() > 1) {
				auto regions = db.config.regions;
				if(clusterControllerDcId.get() == regions[0].dcId) {
					std::swap(regions[0], regions[1]);
				}
				ASSERT(clusterControllerDcId.get() == regions[1].dcId);
				checkRegions(regions);
			}
		}
	}

	//FIXME: determine when to fail the cluster controller when a primaryDC has not been set
	bool betterMasterExists() {
		const ServerDBInfo dbi = db.serverInfo->get();

		if(dbi.recoveryState < RecoveryState::ACCEPTING_COMMITS) {
			return false;
		}

		// Do not trigger better master exists if the cluster controller is excluded, since the master will change anyways once the cluster controller is moved
		if(id_worker[clusterControllerProcessId].priorityInfo.isExcluded) {
			return false;
		}

		if (db.config.regions.size() > 1 && db.config.regions[0].priority > db.config.regions[1].priority &&
			db.config.regions[0].dcId != clusterControllerDcId.get() && versionDifferenceUpdated && datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
			checkRegions(db.config.regions);
		}

		// Get master process
		auto masterWorker = id_worker.find(dbi.master.locality.processId());
		if(masterWorker == id_worker.end()) {
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

		for( auto& logSet : dbi.logSystemConfig.tLogs ) {
			for( auto& it : logSet.tLogs ) {
				auto tlogWorker = id_worker.find(it.interf().filteredLocality.processId());
				if ( tlogWorker == id_worker.end() )
					return false;
				if ( tlogWorker->second.priorityInfo.isExcluded )
					return true;

				if(logSet.isLocal && logSet.locality == tagLocalitySatellite) {
					satellite_tlogs.push_back(tlogWorker->second.details);
				} else if (logSet.isLocal) {
					tlogs.push_back(tlogWorker->second.details);
				} else {
					remote_tlogs.push_back(tlogWorker->second.details);
				}
			}

			for (auto& it : logSet.logRouters) {
				auto tlogWorker = id_worker.find(it.interf().filteredLocality.processId());
				if ( tlogWorker == id_worker.end() )
					return false;
				if ( tlogWorker->second.priorityInfo.isExcluded )
					return true;
				if( !logRouterAddresses.count( tlogWorker->second.details.interf.address() ) ) {
					logRouterAddresses.insert( tlogWorker->second.details.interf.address() );
					log_routers.push_back(tlogWorker->second.details);
				}
			}

			for (const auto& worker : logSet.backupWorkers) {
				auto workerIt = id_worker.find(worker.interf().locality.processId());
				if (workerIt == id_worker.end()) return false;
				if (workerIt->second.priorityInfo.isExcluded) return true;
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
			if (commitProxyWorker == id_worker.end()) return false;
			if (commitProxyWorker->second.priorityInfo.isExcluded) return true;
			commitProxyClasses.push_back(commitProxyWorker->second.details);
		}

		// Get grv proxy classes
		std::vector<WorkerDetails> grvProxyClasses;
		for(auto& it : dbi.client.grvProxies ) {
			auto grvProxyWorker = id_worker.find(it.processId);
			if ( grvProxyWorker == id_worker.end() )
				return false;
			if ( grvProxyWorker->second.priorityInfo.isExcluded )
				return true;
			grvProxyClasses.push_back(grvProxyWorker->second.details);
		}

		// Get resolver classes
		std::vector<WorkerDetails> resolverClasses;
		for(auto& it : dbi.resolvers ) {
			auto resolverWorker = id_worker.find(it.locality.processId());
			if ( resolverWorker == id_worker.end() )
				return false;
			if ( resolverWorker->second.priorityInfo.isExcluded )
				return true;
			resolverClasses.push_back(resolverWorker->second.details);
		}

		// Check master fitness. Don't return false if master is excluded in case all the processes are excluded, we still need master for recovery.
		ProcessClass::Fitness oldMasterFit = masterWorker->second.details.processClass.machineClassFitness( ProcessClass::Master );
		if(db.config.isExcludedServer(dbi.master.addresses())) {
			oldMasterFit = std::max(oldMasterFit, ProcessClass::ExcludeFit);
		}

		std::map< Optional<Standalone<StringRef>>, int> id_used;
		id_used[clusterControllerProcessId]++;
		WorkerFitnessInfo mworker = getWorkerForRoleInDatacenter(clusterControllerDcId, ProcessClass::Master, ProcessClass::NeverAssign, db.config, id_used, true);
		auto newMasterFit = mworker.worker.processClass.machineClassFitness( ProcessClass::Master );
		if(db.config.isExcludedServer(mworker.worker.interf.addresses())) {
			newMasterFit = std::max(newMasterFit, ProcessClass::ExcludeFit);
		}

		if ( oldMasterFit <  newMasterFit )
			return false;
		if ( oldMasterFit > newMasterFit || ( dbi.master.locality.processId() == clusterControllerProcessId && mworker.worker.interf.locality.processId() != clusterControllerProcessId ) )
			return true;

		std::set<Optional<Key>> primaryDC;
		std::set<Optional<Key>> remoteDC;

		RegionInfo region;
		RegionInfo remoteRegion;
		if (db.config.regions.size()) {
			primaryDC.insert(clusterControllerDcId);
			for(auto& r : db.config.regions) {
				if(r.dcId != clusterControllerDcId.get()) {
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
		RoleFitness oldTLogFit(tlogs, ProcessClass::TLog);
		auto newTLogs = getWorkersForTlogs(db.config, db.config.tLogReplicationFactor, db.config.getDesiredLogs(), db.config.tLogPolicy, id_used, true, primaryDC);
		RoleFitness newTLogFit(newTLogs, ProcessClass::TLog);

		if(oldTLogFit < newTLogFit) return false;

		bool oldSatelliteFallback = false;

		for(auto& logSet : dbi.logSystemConfig.tLogs) {
			if(region.satelliteTLogPolicy.isValid() && logSet.isLocal && logSet.locality == tagLocalitySatellite) {
				oldSatelliteFallback = logSet.tLogPolicy->info() != region.satelliteTLogPolicy->info();
				ASSERT(!oldSatelliteFallback ||
				       (region.satelliteTLogPolicyFallback.isValid() &&
				        logSet.tLogPolicy->info() == region.satelliteTLogPolicyFallback->info()));
				break;
			}
		}

		RoleFitness oldSatelliteTLogFit(satellite_tlogs, ProcessClass::TLog);
		bool newSatelliteFallback = false;
		auto newSatelliteTLogs = (region.satelliteTLogReplicationFactor > 0 && db.config.usableRegions > 1) ? getWorkersForSatelliteLogs(db.config, region, remoteRegion, id_used, newSatelliteFallback, true) : satellite_tlogs;
		RoleFitness newSatelliteTLogFit(newSatelliteTLogs, ProcessClass::TLog);

		std::map<Optional<Key>,int32_t> satellite_priority;
		for(auto& r : region.satellites) {
			satellite_priority[r.dcId] = r.priority;
		}

		int32_t oldSatelliteRegionFit = std::numeric_limits<int32_t>::max();
		for(auto& it : satellite_tlogs) {
			if (satellite_priority.count(it.interf.locality.dcId())) {
				oldSatelliteRegionFit = std::min(oldSatelliteRegionFit, satellite_priority[it.interf.locality.dcId()]);
			} else {
				oldSatelliteRegionFit = -1;
			}
		}

		int32_t newSatelliteRegionFit = std::numeric_limits<int32_t>::max();
		for(auto& it : newSatelliteTLogs) {
			if(satellite_priority.count(it.interf.locality.dcId())) {
				newSatelliteRegionFit = std::min(newSatelliteRegionFit, satellite_priority[it.interf.locality.dcId()]);
			} else {
				newSatelliteRegionFit = -1;
			}
		}

		if (oldSatelliteFallback && !newSatelliteFallback) return true;
		if(!oldSatelliteFallback && newSatelliteFallback)
			return false;

		if (oldSatelliteRegionFit < newSatelliteRegionFit) return true;
		if(oldSatelliteRegionFit > newSatelliteRegionFit)
			return false;

		if(oldSatelliteTLogFit < newSatelliteTLogFit)
			return false;

		RoleFitness oldRemoteTLogFit(remote_tlogs, ProcessClass::TLog);
		std::vector<UID> exclusionWorkerIds;
		auto fn = [](const WorkerDetails &in) { return in.interf.id(); };
		std::transform(newTLogs.begin(), newTLogs.end(), std::back_inserter(exclusionWorkerIds), fn);
		std::transform(newSatelliteTLogs.begin(), newSatelliteTLogs.end(), std::back_inserter(exclusionWorkerIds), fn);
		RoleFitness newRemoteTLogFit(
			(db.config.usableRegions > 1 && (dbi.recoveryState == RecoveryState::ALL_LOGS_RECRUITED || dbi.recoveryState == RecoveryState::FULLY_RECOVERED)) ?
			getWorkersForTlogs(db.config, db.config.getRemoteTLogReplicationFactor(), db.config.getDesiredRemoteLogs(), db.config.getRemoteTLogPolicy(), id_used, true, remoteDC, exclusionWorkerIds)
			: remote_tlogs, ProcessClass::TLog);
		if(oldRemoteTLogFit < newRemoteTLogFit) return false;
		int oldRouterCount = oldTLogFit.count * std::max<int>(1, db.config.desiredLogRouterCount / std::max(1,oldTLogFit.count));
		int newRouterCount = newTLogFit.count * std::max<int>(1, db.config.desiredLogRouterCount / std::max(1,newTLogFit.count));
		RoleFitness oldLogRoutersFit(log_routers, ProcessClass::LogRouter);
		RoleFitness newLogRoutersFit(
		    (db.config.usableRegions > 1 && dbi.recoveryState == RecoveryState::FULLY_RECOVERED)
		        ? getWorkersForRoleInDatacenter(*remoteDC.begin(), ProcessClass::LogRouter, newRouterCount, db.config,
		                                        id_used, Optional<WorkerFitnessInfo>(), true)
		        : log_routers,
		    ProcessClass::LogRouter);

		if(oldLogRoutersFit.count < oldRouterCount) {
			oldLogRoutersFit.worstFit = ProcessClass::NeverAssign;
		}
		if(newLogRoutersFit.count < newRouterCount) {
			newLogRoutersFit.worstFit = ProcessClass::NeverAssign;
		}
		if(oldLogRoutersFit < newLogRoutersFit) return false;

		// Check proxy/grvProxy/resolver fitness
		RoleFitnessPair oldInFit(RoleFitness(commitProxyClasses, ProcessClass::CommitProxy),
		                         RoleFitness(grvProxyClasses, ProcessClass::GrvProxy),
		                         RoleFitness(resolverClasses, ProcessClass::Resolver));

		auto first_commit_proxy = getWorkerForRoleInDatacenter(clusterControllerDcId, ProcessClass::CommitProxy,
		                                                       ProcessClass::ExcludeFit, db.config, id_used, true);
		auto first_grv_proxy = getWorkerForRoleInDatacenter(clusterControllerDcId, ProcessClass::GrvProxy,
		                                                    ProcessClass::ExcludeFit, db.config, id_used, true);
		auto first_resolver = getWorkerForRoleInDatacenter(clusterControllerDcId, ProcessClass::Resolver,
		                                                   ProcessClass::ExcludeFit, db.config, id_used, true);
		auto commit_proxies = getWorkersForRoleInDatacenter(clusterControllerDcId, ProcessClass::CommitProxy,
		                                                    db.config.getDesiredCommitProxies(), db.config, id_used,
		                                                    first_commit_proxy, true);
		auto grv_proxies =
		    getWorkersForRoleInDatacenter(clusterControllerDcId, ProcessClass::GrvProxy,
		                                  db.config.getDesiredGrvProxies(), db.config, id_used, first_grv_proxy, true);
		auto resolvers = getWorkersForRoleInDatacenter( clusterControllerDcId, ProcessClass::Resolver, db.config.getDesiredResolvers(), db.config, id_used, first_resolver, true );

		RoleFitnessPair newInFit(RoleFitness(commit_proxies, ProcessClass::CommitProxy),
		                         RoleFitness(grv_proxies, ProcessClass::GrvProxy),
		                         RoleFitness(resolvers, ProcessClass::Resolver));
		if (oldInFit.proxy.betterFitness(newInFit.proxy) || oldInFit.grvProxy.betterFitness(newInFit.grvProxy) ||
		    oldInFit.resolver.betterFitness(newInFit.resolver)) {
			return false;
		}

		// Check backup worker fitness
		RoleFitness oldBackupWorkersFit(backup_workers, ProcessClass::Backup);
		const int nBackup = backup_addresses.size();
		RoleFitness newBackupWorkersFit(
		    getWorkersForRoleInDatacenter(clusterControllerDcId, ProcessClass::Backup, nBackup, db.config, id_used),
		    ProcessClass::Backup);

		if (oldTLogFit > newTLogFit || oldInFit > newInFit || oldSatelliteTLogFit > newSatelliteTLogFit ||
		    oldRemoteTLogFit > newRemoteTLogFit || oldLogRoutersFit > newLogRoutersFit ||
		    oldBackupWorkersFit > newBackupWorkersFit) {
			TraceEvent("BetterMasterExists", id)
			    .detail("OldMasterFit", oldMasterFit)
			    .detail("NewMasterFit", newMasterFit)
			    .detail("OldTLogFit", oldTLogFit.toString())
			    .detail("NewTLogFit", newTLogFit.toString())
			    .detail("OldProxyFit", oldInFit.proxy.toString())
			    .detail("NewProxyFit", newInFit.proxy.toString())
			    .detail("OldGrvProxyFit", oldInFit.grvProxy.toString())
			    .detail("NewGrvProxyFit", newInFit.grvProxy.toString())
			    .detail("OldResolverFit", oldInFit.resolver.toString())
			    .detail("NewResolverFit", newInFit.resolver.toString())
			    .detail("OldSatelliteFit", oldSatelliteTLogFit.toString())
			    .detail("NewSatelliteFit", newSatelliteTLogFit.toString())
			    .detail("OldRemoteFit", oldRemoteTLogFit.toString())
			    .detail("NewRemoteFit", newRemoteTLogFit.toString())
			    .detail("OldRouterFit", oldLogRoutersFit.toString())
			    .detail("NewRouterFit", newLogRoutersFit.toString())
			    .detail("OldBackupWorkerFit", oldBackupWorkersFit.toString())
			    .detail("NewBackupWorkerFit", newBackupWorkersFit.toString())
			    .detail("OldSatelliteFallback", oldSatelliteFallback)
			    .detail("NewSatelliteFallback", newSatelliteFallback);
			return true;
		}

		return false;
	}

	bool isUsedNotMaster(Optional<Key> processId) {
		ASSERT(masterProcessId.present());
		if (processId == masterProcessId) return false;

		auto& dbInfo = db.serverInfo->get();
		for (const auto& tlogset : dbInfo.logSystemConfig.tLogs) {
			for (const auto& tlog: tlogset.tLogs) {
				if (tlog.present() && tlog.interf().filteredLocality.processId() == processId) return true;
			}
		}
		for (const CommitProxyInterface& interf : dbInfo.client.commitProxies) {
			if (interf.processId == processId) return true;
		}
		for (const GrvProxyInterface& interf : dbInfo.client.grvProxies) {
			if (interf.processId == processId) return true;
		}
		for (const ResolverInterface& interf: dbInfo.resolvers) {
			if (interf.locality.processId() == processId) return true;
		}
		if (processId == clusterControllerProcessId) return true;

		return false;
	}

	bool onMasterIsBetter(const WorkerDetails& worker, ProcessClass::ClusterRole role) {
		ASSERT(masterProcessId.present());
		const auto& pid = worker.interf.locality.processId();
		if ((role != ProcessClass::DataDistributor && role != ProcessClass::Ratekeeper) || pid == masterProcessId.get()) {
			return false;
		}
		return isUsedNotMaster(pid);
	}

	std::map< Optional<Standalone<StringRef>>, int> getUsedIds() {
		std::map<Optional<Standalone<StringRef>>, int> idUsed;
		updateKnownIds(&idUsed);

		auto& dbInfo = db.serverInfo->get();
		for (const auto& tlogset : dbInfo.logSystemConfig.tLogs) {
			for (const auto& tlog: tlogset.tLogs) {
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
		for (const ResolverInterface& interf: dbInfo.resolvers) {
			ASSERT(interf.locality.processId().present());
			idUsed[interf.locality.processId()]++;
		}
		return idUsed;
	}

	std::map< Optional<Standalone<StringRef>>, WorkerInfo > id_worker;
	std::map< Optional<Standalone<StringRef>>, ProcessClass > id_class; //contains the mapping from process id to process class from the database
	Standalone<RangeResultRef> lastProcessClasses;
	bool gotProcessClasses;
	bool gotFullyRecoveredConfig;
	Optional<Standalone<StringRef>> masterProcessId;
	Optional<Standalone<StringRef>> clusterControllerProcessId;
	Optional<Standalone<StringRef>> clusterControllerDcId;
	AsyncVar<Optional<vector<Optional<Key>>>> desiredDcIds; //desired DC priorities
	AsyncVar<std::pair<bool,Optional<vector<Optional<Key>>>>> changingDcIds; //current DC priorities to change first, and whether that is the cluster controller
	AsyncVar<std::pair<bool,Optional<vector<Optional<Key>>>>> changedDcIds; //current DC priorities to change second, and whether the cluster controller has been changed
	UID id;
	std::vector<RecruitFromConfigurationRequest> outstandingRecruitmentRequests;
	std::vector<RecruitRemoteFromConfigurationRequest> outstandingRemoteRecruitmentRequests;
	std::vector<std::pair<RecruitStorageRequest, double>> outstandingStorageRequests;
	ActorCollection ac;
	UpdateWorkerList updateWorkerList;
	Future<Void> outstandingRequestChecker;
	Future<Void> outstandingRemoteRequestChecker;
	AsyncTrigger updateDBInfo;
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

	CounterCollection clusterControllerMetrics;

	Counter openDatabaseRequests;
	Counter registerWorkerRequests;
	Counter getWorkersRequests;
	Counter getClientWorkersRequests;
	Counter registerMasterRequests;
	Counter statusRequests;

	ClusterControllerData( ClusterControllerFullInterface const& ccInterface, LocalityData const& locality )
		: clusterControllerProcessId(locality.processId()), clusterControllerDcId(locality.dcId()),
			id(ccInterface.id()), ac(false), outstandingRequestChecker(Void()), outstandingRemoteRequestChecker(Void()), gotProcessClasses(false),
			gotFullyRecoveredConfig(false), startTime(now()), goodRecruitmentTime(Never()),
			goodRemoteRecruitmentTime(Never()), datacenterVersionDifference(0),
			versionDifferenceUpdated(false), recruitingDistributor(false), recruitRatekeeper(false),
			clusterControllerMetrics("ClusterController", id.toString()),
			openDatabaseRequests("OpenDatabaseRequests", clusterControllerMetrics),
			registerWorkerRequests("RegisterWorkerRequests", clusterControllerMetrics),
			getWorkersRequests("GetWorkersRequests", clusterControllerMetrics),
			getClientWorkersRequests("GetClientWorkersRequests", clusterControllerMetrics),
			registerMasterRequests("RegisterMasterRequests", clusterControllerMetrics),
			statusRequests("StatusRequests", clusterControllerMetrics)
	{
		auto serverInfo = ServerDBInfo();
		serverInfo.id = deterministicRandom()->randomUniqueID();
		serverInfo.infoGeneration = ++db.dbInfoCount;
		serverInfo.masterLifetime.ccID = id;
		serverInfo.clusterInterface = ccInterface;
		serverInfo.myLocality = locality;
		db.serverInfo->set( serverInfo );
		cx = openDBOnServer(db.serverInfo, TaskPriority::DefaultEndpoint, true, true);
	}

	~ClusterControllerData() {
		ac.clear(false);
		id_worker.clear();
	}
};

ACTOR Future<Void> clusterWatchDatabase( ClusterControllerData* cluster, ClusterControllerData::DBInfo* db )
{
	state MasterInterface iMaster;

	// SOMEDAY: If there is already a non-failed master referenced by zkMasterInfo, use that one until it fails
	// When this someday is implemented, make sure forced failures still cause the master to be recruited again

	loop {
		TraceEvent("CCWDB", cluster->id);
		try {
			state double recoveryStart = now();
			TraceEvent("CCWDB", cluster->id).detail("Recruiting", "Master");

			//We must recruit the master in the same data center as the cluster controller.
			//This should always be possible, because we can recruit the master on the same process as the cluster controller.
			std::map< Optional<Standalone<StringRef>>, int> id_used;
			id_used[cluster->clusterControllerProcessId]++;
			state WorkerFitnessInfo masterWorker = cluster->getWorkerForRoleInDatacenter(cluster->clusterControllerDcId, ProcessClass::Master, ProcessClass::NeverAssign, db->config, id_used);
			if( ( masterWorker.worker.processClass.machineClassFitness( ProcessClass::Master ) > SERVER_KNOBS->EXPECTED_MASTER_FITNESS || masterWorker.worker.interf.locality.processId() == cluster->clusterControllerProcessId )
				&& !cluster->goodRecruitmentTime.isReady() ) {
				TraceEvent("CCWDB", cluster->id).detail("Fitness", masterWorker.worker.processClass.machineClassFitness( ProcessClass::Master ));
				wait( delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
				continue;
			}
			RecruitMasterRequest rmq;
			rmq.lifetime = db->serverInfo->get().masterLifetime;
			rmq.forceRecovery = db->forceRecovery;

			cluster->masterProcessId = masterWorker.worker.interf.locality.processId();
			cluster->db.unfinishedRecoveries++;
			state Future<ErrorOr<MasterInterface>> fNewMaster = masterWorker.worker.interf.master.tryGetReply( rmq );
			wait( ready(fNewMaster) || db->forceMasterFailure.onTrigger() );
			if (fNewMaster.isReady() && fNewMaster.get().present()) {
				TraceEvent("CCWDB", cluster->id).detail("Recruited", fNewMaster.get().get().id());

				// for status tool
				TraceEvent("RecruitedMasterWorker", cluster->id)
					.detail("Address", fNewMaster.get().get().address())
					.trackLatest("RecruitedMasterWorker");

				iMaster = fNewMaster.get().get();

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

				TraceEvent("CCWDB", cluster->id).detail("Lifetime", dbInfo.masterLifetime.toString()).detail("ChangeID", dbInfo.id);
				db->serverInfo->set( dbInfo );

				state Future<Void> spinDelay = delay(SERVER_KNOBS->MASTER_SPIN_DELAY);  // Don't retry master recovery more than once per second, but don't delay the "first" recovery after more than a second of normal operation

				TraceEvent("CCWDB", cluster->id).detail("Watching", iMaster.id());

				// Master failure detection is pretty sensitive, but if we are in the middle of a very long recovery we really don't want to have to start over
				loop choose {
					when (wait( waitFailureClient( iMaster.waitFailure, db->masterRegistrationCount ?
						SERVER_KNOBS->MASTER_FAILURE_REACTION_TIME : (now() - recoveryStart) * SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY,
						db->masterRegistrationCount ? -SERVER_KNOBS->MASTER_FAILURE_REACTION_TIME/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY : SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) || db->forceMasterFailure.onTrigger() )) { break; }
					when (wait( db->serverInfo->onChange() )) {}
				}
				wait(spinDelay);

				TEST(true); // clusterWatchDatabase() master failed
				TraceEvent(SevWarn,"DetectedFailedMaster", cluster->id).detail("OldMaster", iMaster.id());
			} else {
				TEST(true); //clusterWatchDatabas() !newMaster.present()
				wait( delay(SERVER_KNOBS->MASTER_SPIN_DELAY) );
			}
		} catch (Error& e) {
			TraceEvent("CCWDB", cluster->id).error(e, true).detail("Master", iMaster.id());
			if (e.code() == error_code_actor_cancelled) throw;

			bool ok = e.code() == error_code_no_more_servers;
			TraceEvent(ok ? SevWarn : SevError,"ClusterWatchDatabaseRetrying", cluster->id).error(e);
			if (!ok)
				throw e;
			wait( delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
		}
	}
}

ACTOR Future<Void> clusterGetServerInfo(ClusterControllerData::DBInfo* db, UID knownServerInfoID,
                                        ReplyPromise<ServerDBInfo> reply) {
	while(db->serverInfo->get().id == knownServerInfoID) {
		choose {
			when (wait( yieldedFuture(db->serverInfo->onChange()) )) {}
			when (wait( delayJittered( 300 ) )) { break; }  // The server might be long gone!
		}
	}
	reply.send( db->serverInfo->get() );
	return Void();
}

ACTOR Future<Void> clusterOpenDatabase(ClusterControllerData::DBInfo* db, OpenDatabaseRequest req) {
	db->clientStatus[req.reply.getEndpoint().getPrimaryAddress()] = std::make_pair(now(), req);
	if(db->clientStatus.size() > 10000) {
		TraceEvent(SevWarnAlways, "TooManyClientStatusEntries").suppressFor(1.0);
	}

	while (db->clientInfo->get().id == req.knownClientInfoID) {
		choose {
			when (wait( db->clientInfo->onChange() )) {}
			when (wait( delayJittered( SERVER_KNOBS->COORDINATOR_REGISTER_INTERVAL ) )) { break; }  // The client might be long gone!
		}
	}

	req.reply.send( db->clientInfo->get() );
	return Void();
}

void checkOutstandingRecruitmentRequests( ClusterControllerData* self ) {
	for( int i = 0; i < self->outstandingRecruitmentRequests.size(); i++ ) {
		RecruitFromConfigurationRequest& req = self->outstandingRecruitmentRequests[i];
		try {
			RecruitFromConfigurationReply rep = self->findWorkersForConfiguration( req );
			req.reply.send( rep );
			swapAndPop( &self->outstandingRecruitmentRequests, i-- );
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

void checkOutstandingRemoteRecruitmentRequests( ClusterControllerData* self ) {
	for( int i = 0; i < self->outstandingRemoteRecruitmentRequests.size(); i++ ) {
		RecruitRemoteFromConfigurationRequest& req = self->outstandingRemoteRecruitmentRequests[i];
		try {
			RecruitRemoteFromConfigurationReply rep = self->findRemoteWorkersForConfiguration( req );
			req.reply.send( rep );
			swapAndPop( &self->outstandingRemoteRecruitmentRequests, i-- );
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

void checkOutstandingStorageRequests( ClusterControllerData* self ) {
	for( int i = 0; i < self->outstandingStorageRequests.size(); i++ ) {
		auto& req = self->outstandingStorageRequests[i];
		try {
			if(req.second < now()) {
				req.first.reply.sendError(timed_out());
				swapAndPop( &self->outstandingStorageRequests, i-- );
			} else {
				if(!self->gotProcessClasses && !req.first.criticalRecruitment)
					throw no_more_servers();

				auto worker = self->getStorageWorker(req.first);
				RecruitStorageReply rep;
				rep.worker = worker.interf;
				rep.processClass = worker.processClass;
				req.first.reply.send( rep );
				swapAndPop( &self->outstandingStorageRequests, i-- );
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
	if (!self->masterProcessId.present() || self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		return;
	}

	std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
	WorkerDetails newRKWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId, ProcessClass::Ratekeeper, ProcessClass::NeverAssign, self->db.config, id_used, true).worker;
	if (self->onMasterIsBetter(newRKWorker, ProcessClass::Ratekeeper)) {
		newRKWorker = self->id_worker[self->masterProcessId.get()].details;
	}
	id_used = self->getUsedIds();
	for(auto& it : id_used) {
		it.second *= 2;
	}
	id_used[newRKWorker.interf.locality.processId()]++;
	WorkerDetails newDDWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId, ProcessClass::DataDistributor, ProcessClass::NeverAssign, self->db.config, id_used, true).worker;
	if (self->onMasterIsBetter(newDDWorker, ProcessClass::DataDistributor)) {
		newDDWorker = self->id_worker[self->masterProcessId.get()].details;
	}
	auto bestFitnessForRK = newRKWorker.processClass.machineClassFitness(ProcessClass::Ratekeeper);
	if(self->db.config.isExcludedServer(newRKWorker.interf.addresses())) {
		bestFitnessForRK = std::max(bestFitnessForRK, ProcessClass::ExcludeFit);
	}
	auto bestFitnessForDD = newDDWorker.processClass.machineClassFitness(ProcessClass::DataDistributor);
	if(self->db.config.isExcludedServer(newDDWorker.interf.addresses())) {
		bestFitnessForDD = std::max(bestFitnessForDD, ProcessClass::ExcludeFit);
	}
	//TraceEvent("CheckBetterDDorRKNewRecruits", self->id).detail("MasterProcessId", self->masterProcessId)
	//.detail("NewRecruitRKProcessId", newRKWorker.interf.locality.processId()).detail("NewRecruiteDDProcessId", newDDWorker.interf.locality.processId());

	Optional<Standalone<StringRef>> currentRKProcessId;
	Optional<Standalone<StringRef>> currentDDProcessId;
	
	auto& db = self->db.serverInfo->get();
	bool ratekeeperHealthy = false;
	if (db.ratekeeper.present() && self->id_worker.count(db.ratekeeper.get().locality.processId()) &&
	   (!self->recruitingRatekeeperID.present() || (self->recruitingRatekeeperID.get() == db.ratekeeper.get().id()))) {
		auto& rkWorker = self->id_worker[db.ratekeeper.get().locality.processId()];
		currentRKProcessId = rkWorker.details.interf.locality.processId();
		auto rkFitness = rkWorker.details.processClass.machineClassFitness(ProcessClass::Ratekeeper);
		if(rkWorker.priorityInfo.isExcluded) {
			rkFitness = ProcessClass::ExcludeFit;
		}
		if (self->isUsedNotMaster(rkWorker.details.interf.locality.processId()) || bestFitnessForRK < rkFitness
				|| (rkFitness == bestFitnessForRK && rkWorker.details.interf.locality.processId() == self->masterProcessId && newRKWorker.interf.locality.processId() != self->masterProcessId)) {
			TraceEvent("CCHaltRK", self->id).detail("RKID", db.ratekeeper.get().id())
			.detail("Excluded", rkWorker.priorityInfo.isExcluded)
			.detail("Fitness", rkFitness).detail("BestFitness", bestFitnessForRK);
			self->recruitRatekeeper.set(true);
		} else {
			ratekeeperHealthy = true;
		}
	}

	if (!self->recruitingDistributor && db.distributor.present() && self->id_worker.count(db.distributor.get().locality.processId())) {
		auto& ddWorker = self->id_worker[db.distributor.get().locality.processId()];
		auto ddFitness = ddWorker.details.processClass.machineClassFitness(ProcessClass::DataDistributor);
		currentDDProcessId = ddWorker.details.interf.locality.processId();
		if(ddWorker.priorityInfo.isExcluded) {
			ddFitness = ProcessClass::ExcludeFit;
		}
		if (self->isUsedNotMaster(ddWorker.details.interf.locality.processId()) || bestFitnessForDD < ddFitness
				|| (ddFitness == bestFitnessForDD && ddWorker.details.interf.locality.processId() == self->masterProcessId && newDDWorker.interf.locality.processId() != self->masterProcessId)
				|| (ddFitness == bestFitnessForDD && newRKWorker.interf.locality.processId() != newDDWorker.interf.locality.processId() && ratekeeperHealthy && currentRKProcessId.present() && currentDDProcessId == currentRKProcessId
							&& (newRKWorker.interf.locality.processId() != self->masterProcessId  && newDDWorker.interf.locality.processId() != self->masterProcessId) )) {
			TraceEvent("CCHaltDD", self->id).detail("DDID", db.distributor.get().id())
			.detail("Excluded", ddWorker.priorityInfo.isExcluded)
			.detail("Fitness", ddFitness).detail("BestFitness", bestFitnessForDD)
			.detail("CurrentRateKeeperProcessId", currentRKProcessId.present() ? currentRKProcessId.get() : LiteralStringRef("None"))
			.detail("CurrentDDProcessId", currentDDProcessId)
			.detail("MasterProcessID", self->masterProcessId)
			.detail("NewRKWorkers", newRKWorker.interf.locality.processId())
			.detail("NewDDWorker", newDDWorker.interf.locality.processId());
			ddWorker.haltDistributor = brokenPromiseToNever(db.distributor.get().haltDataDistributor.getReply(HaltDataDistributorRequest(self->id)));
		}
	}
}

ACTOR Future<Void> doCheckOutstandingRequests( ClusterControllerData* self ) {
	try {
		wait( delay(SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL) );
		while( !self->goodRecruitmentTime.isReady() ) {
			wait(self->goodRecruitmentTime);
		}

		checkOutstandingRecruitmentRequests( self );
		checkOutstandingStorageRequests( self );
		checkBetterDDOrRK(self);

		self->checkRecoveryStalled();
		if (self->betterMasterExists()) {
			self->db.forceMasterFailure.trigger();
			TraceEvent("MasterRegistrationKill", self->id).detail("MasterId", self->db.serverInfo->get().master.id());
		}
	} catch( Error &e ) {
		if(e.code() != error_code_no_more_servers) {
			TraceEvent(SevError, "CheckOutstandingError").error(e);
		}
	}
	return Void();
}

ACTOR Future<Void> doCheckOutstandingRemoteRequests( ClusterControllerData* self ) {
	try {
		wait( delay(SERVER_KNOBS->CHECK_OUTSTANDING_INTERVAL) );
		while( !self->goodRemoteRecruitmentTime.isReady() ) {
			wait(self->goodRemoteRecruitmentTime);
		}

		checkOutstandingRemoteRecruitmentRequests( self );
	} catch( Error &e ) {
		if(e.code() != error_code_no_more_servers) {
			TraceEvent(SevError, "CheckOutstandingError").error(e);
		}
	}
	return Void();
}

void checkOutstandingRequests( ClusterControllerData* self ) {
	if( self->outstandingRemoteRequestChecker.isReady() ) {
		self->outstandingRemoteRequestChecker = doCheckOutstandingRemoteRequests(self);
	}

	if( self->outstandingRequestChecker.isReady() ) {
		self->outstandingRequestChecker = doCheckOutstandingRequests(self);
	}
}

ACTOR Future<Void> rebootAndCheck( ClusterControllerData* cluster, Optional<Standalone<StringRef>> processID ) {
	{
		auto watcher = cluster->id_worker.find(processID);
		ASSERT(watcher != cluster->id_worker.end());

		watcher->second.reboots++;
		wait( delay( g_network->isSimulated() ? SERVER_KNOBS->SIM_SHUTDOWN_TIMEOUT : SERVER_KNOBS->SHUTDOWN_TIMEOUT ) );
	}

	{
		auto watcher = cluster->id_worker.find(processID);
		if(watcher != cluster->id_worker.end()) {
			watcher->second.reboots--;
			if( watcher->second.reboots < 2 )
				checkOutstandingRequests( cluster );
		}
	}

	return Void();
}

ACTOR Future<Void> workerAvailabilityWatch( WorkerInterface worker, ProcessClass startingClass, ClusterControllerData* cluster) {
	state Future<Void> failed =
	    (worker.address() == g_network->getLocalAddress() || startingClass.classType() == ProcessClass::TesterClass)
	        ? Never()
	        : waitFailureClient(worker.waitFailure, SERVER_KNOBS->WORKER_FAILURE_TIME);
	cluster->updateWorkerList.set( worker.locality.processId(), ProcessData(worker.locality, startingClass, worker.stableAddress()) );
	cluster->updateDBInfoEndpoints.insert(worker.updateServerDBInfo.getEndpoint());
	cluster->updateDBInfo.trigger();
	// This switching avoids a race where the worker can be added to id_worker map after the workerAvailabilityWatch fails for the worker.
	wait(delay(0));

	loop {
		choose {
			when( wait( IFailureMonitor::failureMonitor().onStateEqual( worker.storage.getEndpoint(), FailureStatus(IFailureMonitor::failureMonitor().getState( worker.storage.getEndpoint() ).isAvailable()) ) ) ) {
				if( IFailureMonitor::failureMonitor().getState( worker.storage.getEndpoint() ).isAvailable() ) {
					cluster->ac.add( rebootAndCheck( cluster, worker.locality.processId() ) );
					checkOutstandingRequests( cluster );
				}
			}
			when( wait( failed ) ) {  // remove workers that have failed
				WorkerInfo& failedWorkerInfo = cluster->id_worker[ worker.locality.processId() ];

				if (!failedWorkerInfo.reply.isSet()) {
					failedWorkerInfo.reply.send( RegisterWorkerReply(failedWorkerInfo.details.processClass, failedWorkerInfo.priorityInfo) );
				}
				if (worker.locality.processId() == cluster->masterProcessId) {
					cluster->masterProcessId = Optional<Key>();
				}
				cluster->removedDBInfoEndpoints.insert(worker.updateServerDBInfo.getEndpoint());
				cluster->id_worker.erase( worker.locality.processId() );
				cluster->updateWorkerList.set( worker.locality.processId(), Optional<ProcessData>() );
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
		return std::max( now - lastRequestTime, lastRequestTime - penultimateRequestTime );
	}
};

ACTOR Future<vector<TLogInterface>> requireAll( vector<Future<Optional<vector<TLogInterface>>>> in ) {
	state vector<TLogInterface> out;
	state int i;
	for(i=0; i<in.size(); i++) {
		Optional<vector<TLogInterface>> x = wait(in[i]);
		if (!x.present()) throw recruitment_failed();
		out.insert(out.end(), x.get().begin(), x.get().end());
	}
	return out;
}

void clusterRecruitStorage( ClusterControllerData* self, RecruitStorageRequest req ) {
	try {
		if(!self->gotProcessClasses && !req.criticalRecruitment)
			throw no_more_servers();
		auto worker = self->getStorageWorker(req);
		RecruitStorageReply rep;
		rep.worker = worker.interf;
		rep.processClass = worker.processClass;
		req.reply.send( rep );
	} catch ( Error& e ) {
		if (e.code() == error_code_no_more_servers) {
			self->outstandingStorageRequests.push_back( std::make_pair(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT) );
			TraceEvent(SevWarn, "RecruitStorageNotAvailable", self->id)
			    .detail("IsCriticalRecruitment", req.criticalRecruitment)
			    .error(e);
		} else {
			TraceEvent(SevError, "RecruitStorageError", self->id).error(e);
			throw;  // Any other error will bring down the cluster controller
		}
	}
}

ACTOR Future<Void> clusterRecruitFromConfiguration( ClusterControllerData* self, RecruitFromConfigurationRequest req ) {
	// At the moment this doesn't really need to be an actor (it always completes immediately)
	TEST(true); //ClusterController RecruitTLogsRequest
	loop {
		try {
			auto rep = self->findWorkersForConfiguration( req );
			req.reply.send( rep );
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && self->goodRecruitmentTime.isReady()) {
				self->outstandingRecruitmentRequests.push_back( req );
				TraceEvent(SevWarn, "RecruitFromConfigurationNotAvailable", self->id).error(e);
				return Void();
			} else if(e.code() == error_code_operation_failed || e.code() == error_code_no_more_servers) {
				//recruitment not good enough, try again
			}
			else {
				TraceEvent(SevError, "RecruitFromConfigurationError", self->id).error(e);
				throw;  // goodbye, cluster controller
			}
		}
		wait( lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
	}
}

ACTOR Future<Void> clusterRecruitRemoteFromConfiguration( ClusterControllerData* self, RecruitRemoteFromConfigurationRequest req ) {
	// At the moment this doesn't really need to be an actor (it always completes immediately)
	TEST(true); //ClusterController RecruitTLogsRequest Remote
	loop {
		try {
			RecruitRemoteFromConfigurationReply rep = self->findRemoteWorkersForConfiguration( req );
			req.reply.send( rep );
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && self->goodRemoteRecruitmentTime.isReady()) {
				self->outstandingRemoteRecruitmentRequests.push_back( req );
				TraceEvent(SevWarn, "RecruitRemoteFromConfigurationNotAvailable", self->id).error(e);
				return Void();
			} else if(e.code() == error_code_operation_failed || e.code() == error_code_no_more_servers) {
				//recruitment not good enough, try again
			}
			else {
				TraceEvent(SevError, "RecruitRemoteFromConfigurationError", self->id).error(e);
				throw;  // goodbye, cluster controller
			}
		}
		wait( lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
	}
}

void clusterRegisterMaster( ClusterControllerData* self, RegisterMasterRequest const& req ) {
	req.reply.send( Void() );

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

	//make sure the request comes from an active database
	auto db = &self->db;
	if ( db->serverInfo->get().master.id() != req.id || req.registrationCount <= db->masterRegistrationCount ) {
		TraceEvent("MasterRegistrationNotFound", self->id).detail("MasterId", req.id).detail("ExistingId", db->serverInfo->get().master.id()).detail("RegCount", req.registrationCount).detail("ExistingRegCount", db->masterRegistrationCount);
		return;
	}

	if ( req.recoveryState == RecoveryState::FULLY_RECOVERED ) {
		self->db.unfinishedRecoveries = 0;
		self->db.logGenerations = 0;
		ASSERT( !req.logSystemConfig.oldTLogs.size() );
	} else {
		self->db.logGenerations = std::max<int>(self->db.logGenerations, req.logSystemConfig.oldTLogs.size());
	}

	db->masterRegistrationCount = req.registrationCount;
	db->recoveryStalled = req.recoveryStalled;
	if ( req.configuration.present() ) {
		db->config = req.configuration.get();

		if ( req.recoveryState >= RecoveryState::ACCEPTING_COMMITS ) {
			self->gotFullyRecoveredConfig = true;
			db->fullyRecoveredConfig = req.configuration.get();
			for ( auto& it : self->id_worker ) {
				bool isExcludedFromConfig = db->fullyRecoveredConfig.isExcludedServer(it.second.details.interf.addresses());
				if ( it.second.priorityInfo.isExcluded != isExcludedFromConfig ) {
					it.second.priorityInfo.isExcluded = isExcludedFromConfig;
					if( !it.second.reply.isSet() ) {
						it.second.reply.send( RegisterWorkerReply( it.second.details.processClass, it.second.priorityInfo ) );
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
		ClientDBInfo clientInfo;
		clientInfo.id = deterministicRandom()->randomUniqueID();
		clientInfo.commitProxies = req.commitProxies;
		clientInfo.grvProxies = req.grvProxies;
		clientInfo.clientTxnInfoSampleRate = db->clientInfo->get().clientTxnInfoSampleRate;
		clientInfo.clientTxnInfoSizeLimit = db->clientInfo->get().clientTxnInfoSizeLimit;
		db->clientInfo->set( clientInfo );
		dbInfo.client = db->clientInfo->get();
	}

	if( !dbInfo.logSystemConfig.isEqual(req.logSystemConfig) ) {
		isChanged = true;
		dbInfo.logSystemConfig = req.logSystemConfig;
	}

	if( dbInfo.resolvers != req.resolvers ) {
		isChanged = true;
		dbInfo.resolvers = req.resolvers;
	}

	if( dbInfo.recoveryCount != req.recoveryCount ) {
		isChanged = true;
		dbInfo.recoveryCount = req.recoveryCount;
	}

	if( isChanged ) {
		dbInfo.id = deterministicRandom()->randomUniqueID();
		dbInfo.infoGeneration = ++self->db.dbInfoCount;
		self->db.serverInfo->set( dbInfo );
	}

	checkOutstandingRequests(self);
}

void registerWorker( RegisterWorkerRequest req, ClusterControllerData *self ) {
	const WorkerInterface& w = req.wi;
	ProcessClass newProcessClass = req.processClass;
	auto info = self->id_worker.find( w.locality.processId() );
	ClusterControllerPriorityInfo newPriorityInfo = req.priorityInfo;
	newPriorityInfo.processClassFitness = newProcessClass.machineClassFitness(ProcessClass::ClusterController);

	for(auto it : req.incompatiblePeers) {
		self->db.incompatibleConnections[it] = now() + SERVER_KNOBS->INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
	}
	self->removedDBInfoEndpoints.erase(w.updateServerDBInfo.getEndpoint());

	if(info == self->id_worker.end()) {
		TraceEvent("ClusterControllerActualWorkers", self->id).detail("WorkerId",w.id()).detail("ProcessId", w.locality.processId()).detail("ZoneId", w.locality.zoneId()).detail("DataHall", w.locality.dataHallId()).detail("PClass", req.processClass.toString()).detail("Workers", self->id_worker.size());
		self->goodRecruitmentTime = lowPriorityDelay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY);
		self->goodRemoteRecruitmentTime = lowPriorityDelay(SERVER_KNOBS->WAIT_FOR_GOOD_REMOTE_RECRUITMENT_DELAY);
	} else {
		TraceEvent("ClusterControllerWorkerAlreadyRegistered", self->id).suppressFor(1.0).detail("WorkerId",w.id()).detail("ProcessId", w.locality.processId()).detail("ZoneId", w.locality.zoneId()).detail("DataHall", w.locality.dataHallId()).detail("PClass", req.processClass.toString()).detail("Workers", self->id_worker.size());
	}
	if ( w.address() == g_network->getLocalAddress() ) {
		if(self->changingDcIds.get().first) {
			if(self->changingDcIds.get().second.present()) {
				newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness( w.locality.dcId(), self->changingDcIds.get().second.get() );
			}
		} else if(self->changedDcIds.get().second.present()) {
			newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness( w.locality.dcId(), self->changedDcIds.get().second.get() );
		}
	} else {
		if(!self->changingDcIds.get().first) {
			if(self->changingDcIds.get().second.present()) {
				newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness( w.locality.dcId(), self->changingDcIds.get().second.get() );
			}
		} else if(self->changedDcIds.get().second.present()) {
			newPriorityInfo.dcFitness = ClusterControllerPriorityInfo::calculateDCFitness( w.locality.dcId(), self->changedDcIds.get().second.get() );
		}
	}

	// Check process class and exclusive property
	if ( info == self->id_worker.end() || info->second.details.interf.id() != w.id() || req.generation >= info->second.gen ) {
		if ( self->gotProcessClasses ) {
			auto classIter = self->id_class.find(w.locality.processId());

			if( classIter != self->id_class.end() && (classIter->second.classSource() == ProcessClass::DBSource || req.initialClass.classType() == ProcessClass::UnsetClass)) {
				newProcessClass = classIter->second;
			} else {
				newProcessClass = req.initialClass;
			}
			newPriorityInfo.processClassFitness = newProcessClass.machineClassFitness(ProcessClass::ClusterController);
		}

		if ( self->gotFullyRecoveredConfig ) {
			newPriorityInfo.isExcluded = self->db.fullyRecoveredConfig.isExcludedServer(w.addresses());
		}
	}

	if( info == self->id_worker.end() ) {
		self->id_worker[w.locality.processId()] = WorkerInfo( workerAvailabilityWatch( w, newProcessClass, self ), req.reply, req.generation, w, req.initialClass, newProcessClass, newPriorityInfo, req.degraded, req.issues );
		if (!self->masterProcessId.present() && w.locality.processId() == self->db.serverInfo->get().master.locality.processId()) {
			self->masterProcessId = w.locality.processId();
		}
		checkOutstandingRequests( self );
	} else if( info->second.details.interf.id() != w.id() || req.generation >= info->second.gen ) {
		if (!info->second.reply.isSet()) {
			info->second.reply.send( Never() );
		}
		info->second.reply = req.reply;
		info->second.details.processClass = newProcessClass;
		info->second.priorityInfo = newPriorityInfo;
		info->second.initialClass = req.initialClass;
		info->second.details.degraded = req.degraded;
		info->second.gen = req.generation;
		info->second.issues = req.issues;

		if(info->second.details.interf.id() != w.id()) {
			self->removedDBInfoEndpoints.insert(info->second.details.interf.updateServerDBInfo.getEndpoint());
			info->second.details.interf = w;
			info->second.watcher = workerAvailabilityWatch( w, newProcessClass, self );
		}
		checkOutstandingRequests( self );
	} else {
		TEST(true); // Received an old worker registration request.
	}

	if (req.distributorInterf.present() && !self->db.serverInfo->get().distributor.present() &&
			self->clusterControllerDcId == req.distributorInterf.get().locality.dcId() &&
			!self->recruitingDistributor) {
		const DataDistributorInterface& di = req.distributorInterf.get();
		TraceEvent("CCRegisterDataDistributor", self->id).detail("DDID", di.id());
		self->db.setDistributor(di);
	}
	if (req.ratekeeperInterf.present()) {
		if((self->recruitingRatekeeperID.present() && self->recruitingRatekeeperID.get() != req.ratekeeperInterf.get().id()) ||
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
			if (ratekeeper.present() && ratekeeper.get().id() != rki.id() && self->id_worker.count(ratekeeper.get().locality.processId())) {
				TraceEvent("CCHaltPreviousRatekeeper", self->id).detail("RKID", ratekeeper.get().id())
				.detail("DcID", printable(self->clusterControllerDcId))
				.detail("ReqDcID", printable(w.locality.dcId()))
				.detail("RecruitingRKID", self->recruitingRatekeeperID.present() ? self->recruitingRatekeeperID.get() : UID());
				self->id_worker[ratekeeper.get().locality.processId()].haltRatekeeper = brokenPromiseToNever(ratekeeper.get().haltRatekeeper.getReply(HaltRatekeeperRequest(self->id)));
			}
			if(!ratekeeper.present() || ratekeeper.get().id() != rki.id()) {
				self->db.setRatekeeper(rki);
			}
		}
	}

	// Notify the worker to register again with new process class/exclusive property
	if ( !req.reply.isSet() && newPriorityInfo != req.priorityInfo ) {
		req.reply.send( RegisterWorkerReply(newProcessClass, newPriorityInfo) );
	}
}

#define TIME_KEEPER_VERSION LiteralStringRef("1")

ACTOR Future<Void> timeKeeperSetVersion(ClusterControllerData *self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->set(timeKeeperVersionKey, TIME_KEEPER_VERSION);
			wait(tr->commit());
			break;
		} catch (Error &e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid running
// out of space, it limits the max number of entries and clears old entries on each update. This mapping is used from
// backup and restore to get the version information for a timestamp.
ACTOR Future<Void> timeKeeper(ClusterControllerData *self) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);

	TraceEvent("TimeKeeperStarted");

	wait(timeKeeperSetVersion(self));

	loop {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->cx);
		loop {
			try {
				if(!g_network->isSimulated()) {
					// This is done to provide an arbitrary logged transaction every ~10s.
					// FIXME: replace or augment this with logging on the proxy which tracks
					//       how long it is taking to hear responses from each other component.

					UID debugID = deterministicRandom()->randomUniqueID();
					TraceEvent("TimeKeeperCommit", debugID);
					tr->debugTransaction(debugID);
				}
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				Optional<Value> disableValue = wait( tr->get(timeKeeperDisableKey) );
				if(disableValue.present()) {
					break;
				}

				Version v = tr->getReadVersion().get();
				int64_t currentTime = (int64_t)now();
				versionMap.set(tr, currentTime, v);

				int64_t ttl = currentTime - SERVER_KNOBS->TIME_KEEPER_DELAY * SERVER_KNOBS->TIME_KEEPER_MAX_ENTRIES;
				if (ttl > 0) {
					versionMap.erase(tr, 0, ttl);
				}

				wait(tr->commit());
				break;
			} catch (Error &e) {
				wait(tr->onError(e));
			}
		}

		wait(delay(SERVER_KNOBS->TIME_KEEPER_DELAY));
	}
}

ACTOR Future<Void> statusServer(FutureStream< StatusRequest> requests,
								ClusterControllerData *self,
								ServerCoordinators coordinators)
{
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
					TraceEvent(SevWarnAlways, "TooManyStatusRequests").suppressFor(1.0).detail("BatchSize", requests_batch.size());
					req.reply.sendError(server_overloaded());
				} else {
					requests_batch.push_back(req);
				}
			}

			// Get status but trap errors to send back to client.
			vector<WorkerDetails> workers;
			std::vector<ProcessIssues> workerIssues;

			for(auto& it : self->id_worker) {
				workers.push_back(it.second.details);
				if(it.second.issues.size()) {
					workerIssues.push_back(ProcessIssues(it.second.details.interf.address(), it.second.issues));
				}
			}

			std::vector<NetworkAddress> incompatibleConnections;
			for(auto it = self->db.incompatibleConnections.begin(); it != self->db.incompatibleConnections.end();) {
				if(it->second < now()) {
					it = self->db.incompatibleConnections.erase(it);
				} else {
					incompatibleConnections.push_back(it->first);
					it++;
				}
			}

			state ErrorOr<StatusReply> result = wait(errorOr(clusterGetStatus(self->db.serverInfo, self->cx, workers, workerIssues, &self->db.clientStatus, coordinators, incompatibleConnections, self->datacenterVersionDifference)));

			if (result.isError() && result.getError().code() == error_code_actor_cancelled)
				throw result.getError();

			// Update last_request_time now because GetStatus is finished and the delay is to be measured between requests
			last_request_time = now();

			while (!requests_batch.empty())
			{
				if (result.isError())
					requests_batch.back().reply.sendError(result.getError());
				else
					requests_batch.back().reply.send(result.get());
				requests_batch.pop_back();
				wait( yield() );
			}
		}
		catch (Error &e) {
			TraceEvent(SevError, "StatusServerError").error(e);
			throw e;
		}
	}
}

ACTOR Future<Void> monitorProcessClasses(ClusterControllerData *self) {

	state ReadYourWritesTransaction trVer( self->db.db );
	loop {
		try {
			trVer.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
			trVer.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );

			Optional<Value> val = wait(trVer.get(processClassVersionKey));

			if (val.present())
				break;

			Standalone<RangeResultRef> processClasses = wait( trVer.getRange( processClassKeys, CLIENT_KNOBS->TOO_MANY ) );
			ASSERT( !processClasses.more && processClasses.size() < CLIENT_KNOBS->TOO_MANY );

			trVer.clear(processClassKeys);
			trVer.set(processClassVersionKey, processClassVersionValue);
			for (auto it : processClasses) {
				UID processUid = decodeProcessClassKeyOld(it.key);
				trVer.set(processClassKeyFor(processUid.toString()), it.value);
			}

			wait(trVer.commit());
			TraceEvent("ProcessClassUpgrade");
			break;
		}
		catch(Error &e) {
			wait( trVer.onError(e) );
		}
	}

	loop {
		state ReadYourWritesTransaction tr( self->db.db );

		loop {
			try {
				tr.setOption( FDBTransactionOptions::ACCESS_SYSTEM_KEYS );
				tr.setOption( FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE );
				Standalone<RangeResultRef> processClasses = wait( tr.getRange( processClassKeys, CLIENT_KNOBS->TOO_MANY ) );
				ASSERT( !processClasses.more && processClasses.size() < CLIENT_KNOBS->TOO_MANY );

				if(processClasses != self->lastProcessClasses || !self->gotProcessClasses) {
					self->id_class.clear();
					for( int i = 0; i < processClasses.size(); i++ ) {
						auto c = decodeProcessClassValue( processClasses[i].value );
						ASSERT( c.classSource() != ProcessClass::CommandLineSource );
						self->id_class[decodeProcessClassKey( processClasses[i].key )] = c;
					}

					for( auto& w : self->id_worker ) {
						auto classIter = self->id_class.find(w.first);
						ProcessClass newProcessClass;

						if( classIter != self->id_class.end() && (classIter->second.classSource() == ProcessClass::DBSource || w.second.initialClass.classType() == ProcessClass::UnsetClass) ) {
							newProcessClass = classIter->second;
						} else {
							newProcessClass = w.second.initialClass;
						}


						if (newProcessClass != w.second.details.processClass) {
							w.second.details.processClass = newProcessClass;
							w.second.priorityInfo.processClassFitness = newProcessClass.machineClassFitness(ProcessClass::ClusterController);
							if (!w.second.reply.isSet()) {
								w.second.reply.send( RegisterWorkerReply(w.second.details.processClass, w.second.priorityInfo) );
							}
						}
					}

					self->lastProcessClasses = processClasses;
					self->gotProcessClasses = true;
					checkOutstandingRequests( self );
				}

				state Future<Void> watchFuture = tr.watch(processClassChangeKey);
				wait(tr.commit());
				wait(watchFuture);
				break;
			}
			catch(Error &e) {
				wait( tr.onError(e) );
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
				if(configVal.present()) {
					config = LatencyBandConfig::parse(configVal.get());
				}

				auto serverInfo = db->serverInfo->get();
				if(config != serverInfo.latencyBandConfig) {
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
			}
			catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> monitorClientTxnInfoConfigs(ClusterControllerData::DBInfo* db) {
	loop {
		state ReadYourWritesTransaction tr(db->db);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				state Optional<Value> rateVal = wait(tr.get(fdbClientInfoTxnSampleRate));
				state Optional<Value> limitVal = wait(tr.get(fdbClientInfoTxnSizeLimit));
				ClientDBInfo clientInfo = db->clientInfo->get();
				double sampleRate = rateVal.present() ? BinaryReader::fromStringRef<double>(rateVal.get(), Unversioned()) : std::numeric_limits<double>::infinity();
				int64_t sizeLimit = limitVal.present() ? BinaryReader::fromStringRef<int64_t>(limitVal.get(), Unversioned()) : -1;
				if (sampleRate != clientInfo.clientTxnInfoSampleRate || sizeLimit != clientInfo.clientTxnInfoSampleRate) {
					clientInfo.id = deterministicRandom()->randomUniqueID();
					clientInfo.clientTxnInfoSampleRate = sampleRate;
					clientInfo.clientTxnInfoSizeLimit = sizeLimit;
					db->clientInfo->set(clientInfo);
				}

				state Future<Void> watchRateFuture = tr.watch(fdbClientInfoTxnSampleRate);
				state Future<Void> watchLimitFuture = tr.watch(fdbClientInfoTxnSizeLimit);
				wait(tr.commit());
				choose {
					when(wait(watchRateFuture)) { break; }
					when (wait(watchLimitFuture)) { break; }
				}
			}
			catch (Error &e) {
				wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> updatedChangingDatacenters(ClusterControllerData *self) {
	//do not change the cluster controller until all the processes have had a chance to register
	wait( delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY) );
	loop {
		state Future<Void> onChange = self->desiredDcIds.onChange();
		if(!self->desiredDcIds.get().present()) {
			self->changingDcIds.set(std::make_pair(false,self->desiredDcIds.get()));
		} else {
			auto& worker = self->id_worker[self->clusterControllerProcessId];
			uint8_t newFitness = ClusterControllerPriorityInfo::calculateDCFitness( worker.details.interf.locality.dcId(), self->desiredDcIds.get().get() );
			self->changingDcIds.set(std::make_pair(worker.priorityInfo.dcFitness > newFitness,self->desiredDcIds.get()));

			TraceEvent("UpdateChangingDatacenter", self->id).detail("OldFitness", worker.priorityInfo.dcFitness).detail("NewFitness", newFitness);
			if ( worker.priorityInfo.dcFitness > newFitness ) {
				worker.priorityInfo.dcFitness = newFitness;
				if(!worker.reply.isSet()) {
					worker.reply.send( RegisterWorkerReply( worker.details.processClass, worker.priorityInfo ) );
				}
			} else {
				state int currentFit = ProcessClass::BestFit;
				while(currentFit <= ProcessClass::NeverAssign) {
					bool updated = false;
					for ( auto& it : self->id_worker ) {
						if( ( !it.second.priorityInfo.isExcluded && it.second.priorityInfo.processClassFitness == currentFit ) || currentFit == ProcessClass::NeverAssign ) {
							uint8_t fitness = ClusterControllerPriorityInfo::calculateDCFitness( it.second.details.interf.locality.dcId(), self->changingDcIds.get().second.get() );
							if ( it.first != self->clusterControllerProcessId && it.second.priorityInfo.dcFitness != fitness ) {
								updated = true;
								it.second.priorityInfo.dcFitness = fitness;
								if(!it.second.reply.isSet()) {
									it.second.reply.send( RegisterWorkerReply( it.second.details.processClass, it.second.priorityInfo ) );
								}
							}
						}
					}
					if(updated && currentFit < ProcessClass::NeverAssign) {
						wait( delay(SERVER_KNOBS->CC_CLASS_DELAY) );
					}
					currentFit++;
				}
			}
		}

		wait(onChange);
	}
}

ACTOR Future<Void> updatedChangedDatacenters(ClusterControllerData *self) {
	state Future<Void> changeDelay = delay(SERVER_KNOBS->CC_CHANGE_DELAY);
	state Future<Void> onChange = self->changingDcIds.onChange();
	loop {
		choose {
			when( wait(onChange) ) {
				changeDelay = delay(SERVER_KNOBS->CC_CHANGE_DELAY);
				onChange = self->changingDcIds.onChange();
			}
			when( wait(changeDelay) ) {
				changeDelay = Never();
				onChange = self->changingDcIds.onChange();

				self->changedDcIds.set(self->changingDcIds.get());
				if(self->changedDcIds.get().second.present()) {
					TraceEvent("UpdateChangedDatacenter", self->id).detail("CCFirst", self->changedDcIds.get().first);
					if( !self->changedDcIds.get().first ) {
						auto& worker = self->id_worker[self->clusterControllerProcessId];
						uint8_t newFitness = ClusterControllerPriorityInfo::calculateDCFitness( worker.details.interf.locality.dcId(), self->changedDcIds.get().second.get() );
						if( worker.priorityInfo.dcFitness != newFitness ) {
							worker.priorityInfo.dcFitness = newFitness;
							if(!worker.reply.isSet()) {
								worker.reply.send( RegisterWorkerReply( worker.details.processClass, worker.priorityInfo ) );
							}
						}
					} else {
						state int currentFit = ProcessClass::BestFit;
						while(currentFit <= ProcessClass::NeverAssign) {
							bool updated = false;
							for ( auto& it : self->id_worker ) {
								if( ( !it.second.priorityInfo.isExcluded && it.second.priorityInfo.processClassFitness == currentFit ) || currentFit == ProcessClass::NeverAssign ) {
									uint8_t fitness = ClusterControllerPriorityInfo::calculateDCFitness( it.second.details.interf.locality.dcId(), self->changedDcIds.get().second.get() );
									if ( it.first != self->clusterControllerProcessId && it.second.priorityInfo.dcFitness != fitness ) {
										updated = true;
										it.second.priorityInfo.dcFitness = fitness;
										if(!it.second.reply.isSet()) {
											it.second.reply.send( RegisterWorkerReply( it.second.details.processClass, it.second.priorityInfo ) );
										}
									}
								}
							}
							if(updated && currentFit < ProcessClass::NeverAssign) {
								wait( delay(SERVER_KNOBS->CC_CLASS_DELAY) );
							}
							currentFit++;
						}
					}
				}
			}
		}
	}
}

ACTOR Future<Void> updateDatacenterVersionDifference( ClusterControllerData *self ) {
	state double lastLogTime = 0;
	loop {
		self->versionDifferenceUpdated = false;
		if(self->db.serverInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS && self->db.config.usableRegions == 1) {
			bool oldDifferenceTooLarge = !self->versionDifferenceUpdated || self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
			self->versionDifferenceUpdated = true;
			self->datacenterVersionDifference = 0;

			if(oldDifferenceTooLarge) {
				checkOutstandingRequests(self);
			}

			wait(self->db.serverInfo->onChange());
			continue;
		}

		state Optional<TLogInterface> primaryLog;
		state Optional<TLogInterface> remoteLog;
		if(self->db.serverInfo->get().recoveryState >= RecoveryState::ALL_LOGS_RECRUITED) {
			for(auto& logSet : self->db.serverInfo->get().logSystemConfig.tLogs) {
				if(logSet.isLocal && logSet.locality != tagLocalitySatellite) {
					for(auto& tLog : logSet.tLogs) {
						if(tLog.present()) {
							primaryLog = tLog.interf();
							break;
						}
					}
				}
				if(!logSet.isLocal) {
					for(auto& tLog : logSet.tLogs) {
						if(tLog.present()) {
							remoteLog = tLog.interf();
							break;
						}
					}
				}
			}
		}

		if(!primaryLog.present() || !remoteLog.present()) {
			wait(self->db.serverInfo->onChange());
			continue;
		}

		state Future<Void> onChange = self->db.serverInfo->onChange();
		loop {
			state Future<TLogQueuingMetricsReply> primaryMetrics = brokenPromiseToNever( primaryLog.get().getQueuingMetrics.getReply( TLogQueuingMetricsRequest() ) );
			state Future<TLogQueuingMetricsReply> remoteMetrics = brokenPromiseToNever( remoteLog.get().getQueuingMetrics.getReply( TLogQueuingMetricsRequest() ) );

			wait( ( success(primaryMetrics) && success(remoteMetrics) ) || onChange );
			if(onChange.isReady()) {
				break;
			}
			
			if(primaryMetrics.get().v > 0 && remoteMetrics.get().v > 0) {
				bool oldDifferenceTooLarge = !self->versionDifferenceUpdated || self->datacenterVersionDifference >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE;
				self->versionDifferenceUpdated = true;
				self->datacenterVersionDifference = primaryMetrics.get().v - remoteMetrics.get().v;

				if(oldDifferenceTooLarge && self->datacenterVersionDifference < SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
					checkOutstandingRequests(self);
				}

				if(now() - lastLogTime > SERVER_KNOBS->CLUSTER_CONTROLLER_LOGGING_DELAY) {
					lastLogTime = now();
					TraceEvent("DatacenterVersionDifference", self->id).detail("Difference", self->datacenterVersionDifference);
				}
			}

			wait( delay(SERVER_KNOBS->VERSION_LAG_METRIC_INTERVAL) || onChange );
			if(onChange.isReady()) {
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
		} catch( Error &e ) {
			wait( tr.onError(e) );
		}
	}
}

ACTOR Future<Void> handleForcedRecoveries( ClusterControllerData *self, ClusterControllerFullInterface interf ) {
	loop {
		state ForceRecoveryRequest req = waitNext( interf.clientInterface.forceRecovery.getFuture() );
		TraceEvent("ForcedRecoveryStart", self->id).detail("ClusterControllerDcId", self->clusterControllerDcId).detail("DcId", req.dcId.printable());
		state Future<Void> fCommit = doEmptyCommit(self->cx);
		wait(fCommit || delay(SERVER_KNOBS->FORCE_RECOVERY_CHECK_DELAY));
		if(!fCommit.isReady() || fCommit.isError()) {
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
		TraceEvent("ForcedRecoveryFinish", self->id);
		self->db.forceRecovery = false;
		req.reply.send(Void());
	}
}

ACTOR Future<DataDistributorInterface> startDataDistributor( ClusterControllerData *self ) {
	wait(delay(0.0));  // If master fails at the same time, give it a chance to clear master PID.

	TraceEvent("CCStartDataDistributor", self->id);
	loop {
		try {
			state bool no_distributor = !self->db.serverInfo->get().distributor.present();
			while (!self->masterProcessId.present() || self->masterProcessId != self->db.serverInfo->get().master.locality.processId() || self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (no_distributor && self->db.serverInfo->get().distributor.present()) {
				return self->db.serverInfo->get().distributor.get();
			}

			std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			WorkerFitnessInfo data_distributor = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId, ProcessClass::DataDistributor, ProcessClass::NeverAssign, self->db.config, id_used);
			state WorkerDetails worker = data_distributor.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::DataDistributor)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			InitializeDataDistributorRequest req(deterministicRandom()->randomUniqueID());
			TraceEvent("CCDataDistributorRecruit", self->id).detail("Addr", worker.interf.address());

			ErrorOr<DataDistributorInterface> distributor = wait( worker.interf.dataDistributor.getReplyUnlessFailedFor(req, SERVER_KNOBS->WAIT_FOR_DISTRIBUTOR_JOIN_DELAY, 0) );
			if (distributor.present()) {
				TraceEvent("CCDataDistributorRecruited", self->id).detail("Addr", worker.interf.address());
				return distributor.get();
			}
		}
		catch (Error& e) {
			TraceEvent("CCDataDistributorRecruitError", self->id).error(e);
			if ( e.code() != error_code_no_more_servers ) {
				throw;
			}
		}
		wait( lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
	}
}

ACTOR Future<Void> monitorDataDistributor(ClusterControllerData *self) {
	while(self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		if ( self->db.serverInfo->get().distributor.present() ) {
			wait( waitFailureClient( self->db.serverInfo->get().distributor.get().waitFailure, SERVER_KNOBS->DD_FAILURE_TIME ) );
			TraceEvent("CCDataDistributorDied", self->id)
			.detail("DistributorId", self->db.serverInfo->get().distributor.get().id());
			self->db.clearInterf(ProcessClass::DataDistributorClass);
		} else {
			self->recruitingDistributor = true;
			DataDistributorInterface distributorInterf = wait( startDataDistributor(self) );
			self->recruitingDistributor = false;
			self->db.setDistributor(distributorInterf);
		}
	}
}

ACTOR Future<Void> startRatekeeper(ClusterControllerData *self) {
	wait(delay(0.0));  // If master fails at the same time, give it a chance to clear master PID.

	TraceEvent("CCStartRatekeeper", self->id);
	loop {
		try {
			state bool no_ratekeeper = !self->db.serverInfo->get().ratekeeper.present();
			while (!self->masterProcessId.present() || self->masterProcessId != self->db.serverInfo->get().master.locality.processId() || self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
				wait(self->db.serverInfo->onChange() || delay(SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY));
			}
			if (no_ratekeeper && self->db.serverInfo->get().ratekeeper.present()) {
				// Existing ratekeeper registers while waiting, so skip.
				return Void();
			}

			std::map<Optional<Standalone<StringRef>>, int> id_used = self->getUsedIds();
			WorkerFitnessInfo rkWorker = self->getWorkerForRoleInDatacenter(self->clusterControllerDcId, ProcessClass::Ratekeeper, ProcessClass::NeverAssign, self->db.config, id_used);
			InitializeRatekeeperRequest req(deterministicRandom()->randomUniqueID());
			state WorkerDetails worker = rkWorker.worker;
			if (self->onMasterIsBetter(worker, ProcessClass::Ratekeeper)) {
				worker = self->id_worker[self->masterProcessId.get()].details;
			}

			self->recruitingRatekeeperID = req.reqId;
			TraceEvent("CCRecruitRatekeeper", self->id).detail("Addr", worker.interf.address()).detail("RKID", req.reqId);

			ErrorOr<RatekeeperInterface> interf = wait( worker.interf.ratekeeper.getReplyUnlessFailedFor(req, SERVER_KNOBS->WAIT_FOR_RATEKEEPER_JOIN_DELAY, 0) );
			if (interf.present()) {
				self->recruitRatekeeper.set(false);
				self->recruitingRatekeeperID = interf.get().id();
				const auto& ratekeeper = self->db.serverInfo->get().ratekeeper;
				TraceEvent("CCRatekeeperRecruited", self->id).detail("Addr", worker.interf.address()).detail("RKID", interf.get().id());
				if (ratekeeper.present() && ratekeeper.get().id() != interf.get().id() && self->id_worker.count(ratekeeper.get().locality.processId())) {
					TraceEvent("CCHaltRatekeeperAfterRecruit", self->id).detail("RKID", ratekeeper.get().id())
					.detail("DcID", printable(self->clusterControllerDcId));
					self->id_worker[ratekeeper.get().locality.processId()].haltRatekeeper = brokenPromiseToNever(ratekeeper.get().haltRatekeeper.getReply(HaltRatekeeperRequest(self->id)));
				}
				if(!ratekeeper.present() || ratekeeper.get().id() != interf.get().id()) {
					self->db.setRatekeeper(interf.get());
				}
				checkOutstandingRequests(self);
				return Void();
			}
		}
		catch (Error& e) {
			TraceEvent("CCRatekeeperRecruitError", self->id).error(e);
			if ( e.code() != error_code_no_more_servers ) {
				throw;
			}
		}
		wait( lowPriorityDelay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
	}
}

ACTOR Future<Void> monitorRatekeeper(ClusterControllerData *self) {
	while(self->db.serverInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS) {
		wait(self->db.serverInfo->onChange());
	}

	loop {
		if ( self->db.serverInfo->get().ratekeeper.present() && !self->recruitRatekeeper.get() ) {
			choose {
				when(wait(waitFailureClient( self->db.serverInfo->get().ratekeeper.get().waitFailure, SERVER_KNOBS->RATEKEEPER_FAILURE_TIME )))  {
					TraceEvent("CCRatekeeperDied", self->id)
					.detail("RKID", self->db.serverInfo->get().ratekeeper.get().id());
					self->db.clearInterf(ProcessClass::RatekeeperClass);
				}
				when(wait(self->recruitRatekeeper.onChange())) {}
			}
		} else {
			wait( startRatekeeper(self) );
		}
	}
}

ACTOR Future<Void> dbInfoUpdater( ClusterControllerData* self ) {
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
		if(dbInfoChange.isReady()) {
			for(auto &it : self->id_worker) {
				req.broadcastInfo.push_back(it.second.details.interf.updateServerDBInfo.getEndpoint());
			}
		} else {
			for(auto it : self->removedDBInfoEndpoints) {
				self->updateDBInfoEndpoints.erase(it);
			}
			req.broadcastInfo = std::vector<Endpoint>(self->updateDBInfoEndpoints.begin(), self->updateDBInfoEndpoints.end());
		}

		self->updateDBInfoEndpoints.clear();
		self->removedDBInfoEndpoints.clear();
		
		dbInfoChange = self->db.serverInfo->onChange();
		updateDBInfo = self->updateDBInfo.onTrigger();

		req.serializedDbInfo = BinaryWriter::toValue(self->db.serverInfo->get(), AssumeVersion(g_network->protocolVersion()));

		TraceEvent("DBInfoStartBroadcast", self->id);
		choose {
			when(std::vector<Endpoint> notUpdated = wait( broadcastDBInfoRequest(req, SERVER_KNOBS->DBINFO_SEND_AMOUNT, Optional<Endpoint>(), false) )) {
				TraceEvent("DBInfoFinishBroadcast", self->id).detail("NotUpdated", notUpdated.size());
				if(notUpdated.size()) {
					self->updateDBInfoEndpoints.insert(notUpdated.begin(), notUpdated.end());
					self->updateDBInfo.trigger();
				}
			}
			when(wait(dbInfoChange)) {}
		}
	}
}

ACTOR Future<Void> clusterControllerCore( ClusterControllerFullInterface interf, Future<Void> leaderFail, ServerCoordinators coordinators, LocalityData locality ) {
	state ClusterControllerData self( interf, locality );
	state Future<Void> coordinationPingDelay = delay( SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY );
	state uint64_t step = 0;
	state Future<ErrorOr<Void>> error = errorOr( actorCollection( self.addActor.getFuture() ) );

	self.addActor.send( clusterWatchDatabase( &self, &self.db ) );  // Start the master database
	self.addActor.send( self.updateWorkerList.init( self.db.db ) );
	self.addActor.send( statusServer( interf.clientInterface.databaseStatus.getFuture(), &self, coordinators));
	self.addActor.send( timeKeeper(&self) );
	self.addActor.send( monitorProcessClasses(&self) );
	self.addActor.send( monitorServerInfoConfig(&self.db) );
	self.addActor.send( monitorClientTxnInfoConfigs(&self.db) );
	self.addActor.send( updatedChangingDatacenters(&self) );
	self.addActor.send( updatedChangedDatacenters(&self) );
	self.addActor.send( updateDatacenterVersionDifference(&self) );
	self.addActor.send( handleForcedRecoveries(&self, interf) );
	self.addActor.send( monitorDataDistributor(&self) );
	self.addActor.send( monitorRatekeeper(&self) );
	self.addActor.send( dbInfoUpdater(&self) );
	self.addActor.send( traceCounters("ClusterControllerMetrics", self.id, SERVER_KNOBS->STORAGE_LOGGING_DELAY, &self.clusterControllerMetrics, self.id.toString() + "/ClusterControllerMetrics") );
	self.addActor.send( traceRole(Role::CLUSTER_CONTROLLER, interf.id()) );
	//printf("%s: I am the cluster controller\n", g_network->getLocalAddress().toString().c_str());

	loop choose {
		when( ErrorOr<Void> err = wait( error ) ) {
			if (err.isError()) {
				endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Stop Received Error", false, err.getError());
			}
			else {
				endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Stop Received Signal", true);
			}

			// We shut down normally even if there was a serious error (so this fdbserver may be re-elected cluster controller)
			return Void();
		}
		when( OpenDatabaseRequest req = waitNext( interf.clientInterface.openDatabase.getFuture() ) ) {
			++self.openDatabaseRequests;
			self.addActor.send(clusterOpenDatabase(&self.db, req));
		}
		when( RecruitFromConfigurationRequest req = waitNext( interf.recruitFromConfiguration.getFuture() ) ) {
			self.addActor.send( clusterRecruitFromConfiguration( &self, req ) );
		}
		when( RecruitRemoteFromConfigurationRequest req = waitNext( interf.recruitRemoteFromConfiguration.getFuture() ) ) {
			self.addActor.send( clusterRecruitRemoteFromConfiguration( &self, req ) );
		}
		when( RecruitStorageRequest req = waitNext( interf.recruitStorage.getFuture() ) ) {
			clusterRecruitStorage( &self, req );
		}
		when( RegisterWorkerRequest req = waitNext( interf.registerWorker.getFuture() ) ) {
			++self.registerWorkerRequests;
			registerWorker( req, &self );
		}
		when( GetWorkersRequest req = waitNext( interf.getWorkers.getFuture() ) ) {
			++self.getWorkersRequests;
			vector<WorkerDetails> workers;

			for(auto& it : self.id_worker) {
				if ( (req.flags & GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY) && self.db.config.isExcludedServer(it.second.details.interf.addresses()) ) {
					continue;
				}

				if ( (req.flags & GetWorkersRequest::TESTER_CLASS_ONLY) && it.second.details.processClass.classType() != ProcessClass::TesterClass ) {
					continue;
				}

				workers.push_back(it.second.details);
			}

			req.reply.send( workers );
		}
		when( GetClientWorkersRequest req = waitNext( interf.clientInterface.getClientWorkers.getFuture() ) ) {
			++self.getClientWorkersRequests;
			vector<ClientWorkerInterface> workers;
			for(auto& it : self.id_worker) {
				if (it.second.details.processClass.classType() != ProcessClass::TesterClass) {
					workers.push_back(it.second.details.interf.clientInterface);
				}
			}
			req.reply.send(workers);
		}
		when( wait( coordinationPingDelay ) ) {
			CoordinationPingMessage message(self.id, step++);
			for(auto& it : self.id_worker)
				it.second.details.interf.coordinationPing.send(message);
			coordinationPingDelay = delay( SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY );
			TraceEvent("CoordinationPingSent", self.id).detail("TimeStep", message.timeStep);
		}
		when( RegisterMasterRequest req = waitNext( interf.registerMaster.getFuture() ) ) {
			++self.registerMasterRequests;
			clusterRegisterMaster( &self, req );
		}
		when( GetServerDBInfoRequest req = waitNext( interf.getServerDBInfo.getFuture() ) ) {
			self.addActor.send( clusterGetServerInfo(&self.db, req.knownServerInfoID, req.reply) );
		}
		when( wait( leaderFail ) ) {
			// We are no longer the leader if this has changed.
			endRole(Role::CLUSTER_CONTROLLER, interf.id(), "Leader Replaced", true);
			TEST(true); // Lost Cluster Controller Role
			return Void();
		}
		when( ReplyPromise<Void> ping = waitNext( interf.clientInterface.ping.getFuture() ) ) {
			ping.send( Void() );
		}
	}
}

ACTOR Future<Void> replaceInterface( ClusterControllerFullInterface interf ) {
	loop {
		if( interf.hasMessage() ) {
			wait(delay(SERVER_KNOBS->REPLACE_INTERFACE_DELAY));
			return Void();
		}
		wait(delay(SERVER_KNOBS->REPLACE_INTERFACE_CHECK_DELAY));
	}
}

ACTOR Future<Void> clusterController( ServerCoordinators coordinators, Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC, bool hasConnected, Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo, LocalityData locality ) {
	loop {
		state ClusterControllerFullInterface cci;
		state bool inRole = false;
		cci.initEndpoints();
		try {
			//Register as a possible leader; wait to be elected
			state Future<Void> leaderFail = tryBecomeLeader( coordinators, cci, currentCC, hasConnected, asyncPriorityInfo );
			state Future<Void> shouldReplace = replaceInterface( cci );

			while (!currentCC->get().present() || currentCC->get().get() != cci) {
				choose {
					when( wait(currentCC->onChange()) ) {}
					when( wait(leaderFail) ) { ASSERT(false); throw internal_error(); }
					when( wait(shouldReplace) ) { break; }
				}
			}
			if(!shouldReplace.isReady()) {
				shouldReplace = Future<Void>();
				hasConnected = true;
				startRole(Role::CLUSTER_CONTROLLER, cci.id(), UID());
				inRole = true;

				wait( clusterControllerCore( cci, leaderFail, coordinators, locality ) );
			}
		} catch(Error& e) {
			if (inRole)
				endRole(Role::CLUSTER_CONTROLLER, cci.id(), "Error", e.code() == error_code_actor_cancelled || e.code() == error_code_coordinators_changed, e);
			else
				TraceEvent( e.code() == error_code_coordinators_changed ? SevInfo : SevError, "ClusterControllerCandidateError", cci.id()).error(e);
			throw;
		}
	}
}

ACTOR Future<Void> clusterController( Reference<ClusterConnectionFile> connFile, Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC, Reference<AsyncVar<ClusterControllerPriorityInfo>> asyncPriorityInfo, Future<Void> recoveredDiskFiles, LocalityData locality ) {
	wait(recoveredDiskFiles);
	state bool hasConnected = false;
	loop {
		try {
			ServerCoordinators coordinators( connFile );
			wait( clusterController( coordinators, currentCC, hasConnected, asyncPriorityInfo, locality ) );
		} catch( Error &e ) {
			if( e.code() != error_code_coordinators_changed )
				throw; // Expected to terminate fdbserver
		}

		hasConnected = true;
	}
}
