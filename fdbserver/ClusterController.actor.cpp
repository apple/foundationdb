/*
 * ClusterController.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/actorcompiler.h"
#include "fdbrpc/FailureMonitor.h"
#include "flow/ActorCollection.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/CoordinationInterface.h"
#include "Knobs.h"
#include "MoveKeys.h"
#include "WorkerInterface.h"
#include "LeaderElection.h"
#include "WaitFailure.h"
#include "ClusterRecruitmentInterface.h"
#include "ServerDBInfo.h"
#include "Status.h"
#include <algorithm>
#include "fdbclient/DatabaseContext.h"
#include "RecoveryState.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbclient/KeyBackedTypes.h"

void failAfter( Future<Void> trigger, Endpoint e );

struct WorkerInfo : NonCopyable {
	Future<Void> watcher;
	ReplyPromise<RegisterWorkerReply> reply;
	Generation gen;
	int reboots;
	WorkerInterface interf;
	ProcessClass initialClass;
	ProcessClass processClass;
	bool isExcluded;

	WorkerInfo() : gen(-1), reboots(0) {}
	WorkerInfo( Future<Void> watcher, ReplyPromise<RegisterWorkerReply> reply, Generation gen, WorkerInterface interf, ProcessClass initialClass, ProcessClass processClass, bool isExcluded ) :
		watcher(watcher), reply(reply), gen(gen), reboots(0), interf(interf), initialClass(initialClass), processClass(processClass), isExcluded(isExcluded) {}

	WorkerInfo( WorkerInfo&& r ) noexcept(true) : watcher(std::move(r.watcher)), reply(std::move(r.reply)), gen(r.gen),
		reboots(r.reboots), interf(std::move(r.interf)), initialClass(r.initialClass), processClass(r.processClass), isExcluded(r.isExcluded) {}
	void operator=( WorkerInfo&& r ) noexcept(true) {
		watcher = std::move(r.watcher);
		reply = std::move(r.reply);
		gen = r.gen;
		reboots = r.reboots;
		interf = std::move(r.interf);
		initialClass = r.initialClass;
		processClass = r.processClass;
		isExcluded = r.isExcluded;
	}
};

class ClusterControllerData {
public:
	struct DBInfo {
		Reference<AsyncVar<ClientDBInfo>> clientInfo;
		Reference<AsyncVar<ServerDBInfo>> serverInfo;
		ProcessIssuesMap clientsWithIssues, workersWithIssues;
		std::map<NetworkAddress, double> incompatibleConnections;
		ClientVersionMap clientVersionMap;
		std::map<NetworkAddress, std::string> traceLogGroupMap;
		Promise<Void> forceMasterFailure;
		int64_t masterRegistrationCount;
		DatabaseConfiguration config;   // Asynchronously updated via master registration
		DatabaseConfiguration fullyRecoveredConfig;
		Database db;

		DBInfo() : masterRegistrationCount(0),
			clientInfo( new AsyncVar<ClientDBInfo>( ClientDBInfo() ) ),
			serverInfo( new AsyncVar<ServerDBInfo>( ServerDBInfo( LiteralStringRef("DB") ) ) ),
			db( DatabaseContext::create( clientInfo, Future<Void>(), LocalityData(), true, TaskDefaultEndpoint, true ) )  // SOMEDAY: Locality!
		{

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
					Void _ = wait( tr.commit() );
					break;
				} catch (Error& e) {
					Void _ = wait( tr.onError(e) );
				}
			}

			loop {
				// Wait for some changes
				while (!self->anyDelta.get())
					Void _ = wait( self->anyDelta.onChange() );
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
						Void _ = wait( tr.commit() );
						break;
					} catch (Error& e) {
						Void _ = wait( tr.onError(e) );
					}
				}
			}
		}
	};

	bool workerAvailable( WorkerInfo const& worker, bool checkStable ) {
		return ( now() - startTime < 2 * FLOW_KNOBS->SERVER_REQUEST_INTERVAL ) || ( IFailureMonitor::failureMonitor().getState(worker.interf.storage.getEndpoint()).isAvailable() && ( !checkStable || worker.reboots < 2 ) );
	}

	std::pair<WorkerInterface, ProcessClass> getStorageWorker( RecruitStorageRequest const& req ) {
		std::set<Optional<Standalone<StringRef>>> excludedMachines( req.excludeMachines.begin(), req.excludeMachines.end() );
		std::set<Optional<Standalone<StringRef>>> excludedDCs( req.excludeDCs.begin(), req.excludeDCs.end() );
		std::set<AddressExclusion> excludedAddresses( req.excludeAddresses.begin(), req.excludeAddresses.end() );

		for( auto& it : id_worker )
			if( workerAvailable( it.second, false ) &&
					!excludedMachines.count(it.second.interf.locality.zoneId()) &&
					!excludedDCs.count(it.second.interf.locality.dcId()) &&
					!addressExcluded(excludedAddresses, it.second.interf.address()) &&
					it.second.processClass.machineClassFitness( ProcessClass::Storage ) <= ProcessClass::UnsetFit ) {
				return std::make_pair(it.second.interf, it.second.processClass);
			}

		if( req.criticalRecruitment ) {
			ProcessClass::Fitness bestFit = ProcessClass::NeverAssign;
			Optional<std::pair<WorkerInterface, ProcessClass>> bestInfo;
			for( auto& it : id_worker ) {
				ProcessClass::Fitness fit = it.second.processClass.machineClassFitness( ProcessClass::Storage );
				if( workerAvailable( it.second, false ) &&
						!excludedMachines.count(it.second.interf.locality.zoneId()) &&
						!excludedDCs.count(it.second.interf.locality.dcId()) &&
						!addressExcluded(excludedAddresses, it.second.interf.address()) &&
						fit < bestFit ) {
					bestFit = fit;
					bestInfo = std::make_pair(it.second.interf, it.second.processClass);
				}
			}

			if( bestInfo.present() ) {
				return bestInfo.get();
			}
		}

		throw no_more_servers();
	}

	//FIXME: get master in the same datacenter as the proxies and resolvers for ratekeeper, however this is difficult because the master is recruited before we know the cluster's configuration
	std::pair<WorkerInterface, ProcessClass> getMasterWorker( DatabaseConfiguration const& conf, bool checkStable = false ) {
		ProcessClass::Fitness bestFit = ProcessClass::NeverAssign;
		Optional<std::pair<WorkerInterface, ProcessClass>> bestInfo;
		bool bestIsClusterController = false;
		int numEquivalent = 1;
		for( auto& it : id_worker ) {
			auto fit = it.second.processClass.machineClassFitness( ProcessClass::Master );
			if(conf.isExcludedServer(it.second.interf.address())) {
				fit = std::max(fit, ProcessClass::ExcludeFit);
			}
			if( workerAvailable(it.second, checkStable) && fit != ProcessClass::NeverAssign ) {
				if( fit < bestFit || (fit == bestFit && bestIsClusterController) ) {
					bestInfo = std::make_pair(it.second.interf, it.second.processClass);
					bestFit = fit;
					numEquivalent = 1;
					bestIsClusterController = clusterControllerProcessId == it.first;
				}
				else if( fit == bestFit && clusterControllerProcessId != it.first && g_random->random01() < 1.0/++numEquivalent )
					bestInfo = std::make_pair(it.second.interf, it.second.processClass);
			}
		}

		if( bestInfo.present() )
			return bestInfo.get();

		throw no_more_servers();
	}

	std::vector<std::pair<WorkerInterface, ProcessClass>> getWorkersForSeedServers( DatabaseConfiguration const& conf ) {
		std::map<ProcessClass::Fitness, vector<std::pair<WorkerInterface, ProcessClass>>> fitness_workers;
		std::vector<std::pair<WorkerInterface, ProcessClass>> results;
		LocalitySetRef logServerSet = Reference<LocalitySet>(new LocalityMap<std::pair<WorkerInterface, ProcessClass>>());
		LocalityMap<std::pair<WorkerInterface, ProcessClass>>* logServerMap = (LocalityMap<std::pair<WorkerInterface, ProcessClass>>*) logServerSet.getPtr();
		bool bCompleted = false;

		for( auto& it : id_worker ) {
			auto fitness = it.second.processClass.machineClassFitness( ProcessClass::Storage );
			if( workerAvailable(it.second, false) && !conf.isExcludedServer(it.second.interf.address()) && fitness != ProcessClass::NeverAssign ) {
				fitness_workers[ fitness ].push_back(std::make_pair(it.second.interf, it.second.processClass));
			}
		}

		for( auto& it : fitness_workers ) {
			for (auto& worker : it.second ) {
				logServerMap->add(worker.first.locality, &worker);
			}

			std::vector<LocalityEntry> bestSet;
			if( logServerSet->selectReplicas(conf.storagePolicy, bestSet) ) {
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

	std::vector<std::pair<WorkerInterface, ProcessClass>> getWorkersForTlogs( DatabaseConfiguration const& conf, std::map< Optional<Standalone<StringRef>>, int>& id_used, bool checkStable = false )
	{
		std::map<ProcessClass::Fitness, vector<std::pair<WorkerInterface, ProcessClass>>> fitness_workers;
		std::vector<std::pair<WorkerInterface, ProcessClass>> results;
		std::vector<LocalityData> unavailableLocals;
		LocalitySetRef logServerSet;
		LocalityMap<std::pair<WorkerInterface, ProcessClass>>* logServerMap;
		UID functionId = g_nondeterministic_random->randomUniqueID();
		bool bCompleted = false;

		logServerSet = Reference<LocalitySet>(new LocalityMap<std::pair<WorkerInterface, ProcessClass>>());
		logServerMap = (LocalityMap<std::pair<WorkerInterface, ProcessClass>>*) logServerSet.getPtr();

		for( auto& it : id_worker ) {
			auto fitness = it.second.processClass.machineClassFitness( ProcessClass::TLog );
			if( workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.interf.address()) && fitness != ProcessClass::NeverAssign ) {
				fitness_workers[ fitness ].push_back(std::make_pair(it.second.interf, it.second.processClass));
			}
			else {
				unavailableLocals.push_back(it.second.interf.locality);
			}
		}

		results.reserve(results.size() + id_worker.size());
		for (int fitness = ProcessClass::BestFit; fitness != ProcessClass::NeverAssign; fitness ++)
		{
			auto fitnessEnum = (ProcessClass::Fitness) fitness;
			if (fitness_workers.find(fitnessEnum) == fitness_workers.end())
				continue;
			for (auto& worker : fitness_workers[(ProcessClass::Fitness) fitness] ) {
				logServerMap->add(worker.first.locality, &worker);
			}
			if (logServerSet->size() < conf.tLogReplicationFactor) {
				TraceEvent(SevWarn,"GWFTADTooFew", functionId)
					.detail("Fitness", fitness)
					.detail("Processes", logServerSet->size())
					.detail("tLogReplicationFactor", conf.tLogReplicationFactor)
					.detail("tLogPolicy", conf.tLogPolicy ? conf.tLogPolicy->info() : "[unset]")
					.detail("DesiredLogs", conf.getDesiredLogs())
					.detail("InterfaceId", id);
			}
			else if (logServerSet->size() <= conf.getDesiredLogs()) {
				ASSERT(conf.tLogPolicy);
				if (logServerSet->validate(conf.tLogPolicy))	{
					for (auto& object : logServerMap->getObjects()) {
						results.push_back(*object);
					}
					bCompleted = true;
					break;
				}
				else {
					TraceEvent(SevWarn,"GWFTADNotAcceptable", functionId)
						.detail("Fitness", fitness)
						.detail("Processes", logServerSet->size())
						.detail("tLogReplicationFactor", conf.tLogReplicationFactor)
						.detail("tLogPolicy", conf.tLogPolicy ? conf.tLogPolicy->info() : "[unset]")
						.detail("DesiredLogs", conf.getDesiredLogs())
						.detail("InterfaceId", id);
				}
			}
			// Try to select the desired size, if larger
			else {
				std::vector<LocalityEntry>	bestSet;
				std::vector<LocalityData>	tLocalities;
				ASSERT(conf.tLogPolicy);

				// Try to find the best team of servers to fulfill the policy
				if (findBestPolicySet(bestSet, logServerSet, conf.tLogPolicy, conf.getDesiredLogs(),
						SERVER_KNOBS->POLICY_RATING_TESTS, SERVER_KNOBS->POLICY_GENERATIONS))
				{
					results.reserve(results.size() + bestSet.size());
					for (auto& entry : bestSet) {
						auto object = logServerMap->getObject(entry);
						ASSERT(object);
						results.push_back(*object);
						tLocalities.push_back(object->first.locality);
					}
					TraceEvent("GWFTADBestResults", functionId)
						.detail("Fitness", fitness)
						.detail("Processes", logServerSet->size())
						.detail("BestCount", bestSet.size())
						.detail("BestZones", ::describeZones(tLocalities))
						.detail("BestDataHalls", ::describeDataHalls(tLocalities))
						.detail("tLogPolicy", conf.tLogPolicy ? conf.tLogPolicy->info() : "[unset]")
						.detail("TotalResults", results.size())
						.detail("DesiredLogs", conf.getDesiredLogs())
						.detail("InterfaceId", id);
					bCompleted = true;
					break;
				}
				else {
					TraceEvent(SevWarn,"GWFTADNoBest", functionId)
						.detail("Fitness", fitness)
						.detail("Processes", logServerSet->size())
						.detail("tLogReplicationFactor", conf.tLogReplicationFactor)
						.detail("tLogPolicy", conf.tLogPolicy ? conf.tLogPolicy->info() : "[unset]")
						.detail("DesiredLogs", conf.getDesiredLogs())
						.detail("InterfaceId", id);
				}
			}
		}

		// If policy cannot be satisfied
		if (!bCompleted)
		{
				std::vector<LocalityData>	tLocalities;
				for (auto& object : logServerMap->getObjects()) {
					tLocalities.push_back(object->first.locality);
				}

				TraceEvent(SevWarn, "GetTLogTeamFailed", functionId)
					.detail("Policy", conf.tLogPolicy->info())
					.detail("Processes", logServerSet->size())
					.detail("Workers", id_worker.size())
					.detail("FitnessGroups", fitness_workers.size())
					.detail("TLogZones", ::describeZones(tLocalities))
					.detail("TLogDataHalls", ::describeDataHalls(tLocalities))
					.detail("MissingZones", ::describeZones(unavailableLocals))
					.detail("MissingDataHalls", ::describeDataHalls(unavailableLocals))
					.detail("Replication", conf.tLogReplicationFactor)
					.detail("DesiredLogs", conf.getDesiredLogs())
					.detail("RatingTests",SERVER_KNOBS->POLICY_RATING_TESTS)
					.detail("checkStable", checkStable)
					.detail("PolicyGenerations",SERVER_KNOBS->POLICY_GENERATIONS)
					.detail("InterfaceId", id).backtrace();

			// Free the set
			logServerSet->clear();
			logServerSet.clear();
			throw no_more_servers();
		}

		for (auto& result : results) {
			id_used[result.first.locality.processId()]++;
		}

		TraceEvent("GetTLogTeamDone", functionId)
			.detail("Completed", bCompleted).detail("Policy", conf.tLogPolicy->info())
			.detail("Results", results.size()).detail("Processes", logServerSet->size())
			.detail("Workers", id_worker.size())
			.detail("Replication", conf.tLogReplicationFactor)
			.detail("Desired", conf.getDesiredLogs())
			.detail("RatingTests",SERVER_KNOBS->POLICY_RATING_TESTS)
			.detail("PolicyGenerations",SERVER_KNOBS->POLICY_GENERATIONS)
			.detail("InterfaceId", id);

		for (auto& result : results) {
			TraceEvent("GetTLogTeamWorker", functionId)
				.detail("Class", result.second.toString())
				.detail("Address", result.first.address())
				.detailext("Zone", result.first.locality.zoneId())
				.detailext("DataHall", result.first.locality.dataHallId())
				.detail("isExcludedServer", conf.isExcludedServer(result.first.address()))
				.detail("isAvailable", IFailureMonitor::failureMonitor().getState(result.first.storage.getEndpoint()).isAvailable());
		}

		// Free the set
		logServerSet->clear();
		logServerSet.clear();

		return results;
	}

	struct WorkerFitnessInfo {
		std::pair<WorkerInterface, ProcessClass> worker;
		ProcessClass::Fitness fitness;
		int used;

		WorkerFitnessInfo(std::pair<WorkerInterface, ProcessClass> worker, ProcessClass::Fitness fitness, int used) : worker(worker), fitness(fitness), used(used) {}
	};

	WorkerFitnessInfo getWorkerForRoleInDatacenter(Optional<Standalone<StringRef>> const& dcId, ProcessClass::ClusterRole role, DatabaseConfiguration const& conf, std::map< Optional<Standalone<StringRef>>, int>& id_used, bool checkStable = false ) {
		std::map<std::pair<ProcessClass::Fitness,int>, vector<std::pair<WorkerInterface, ProcessClass>>> fitness_workers;

		for( auto& it : id_worker ) {
			auto fitness = it.second.processClass.machineClassFitness( role );
			if( workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.interf.address()) && fitness != ProcessClass::NeverAssign && it.second.interf.locality.dcId()==dcId ) {
				fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].push_back(std::make_pair(it.second.interf, it.second.processClass));
			}
		}

		for( auto& it : fitness_workers ) {
			auto& w = it.second;
			g_random->randomShuffle(w);
			for( int i=0; i < w.size(); i++ ) {
				id_used[w[i].first.locality.processId()]++;
				return WorkerFitnessInfo(w[i], it.first.first, it.first.second);
			}
		}

		//If we did not find enough workers in the primary data center, add workers from other data centers
		fitness_workers.clear();
		for( auto& it : id_worker ) {
			auto fitness = it.second.processClass.machineClassFitness( role );
			if( workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.interf.address()) && fitness != ProcessClass::NeverAssign && it.second.interf.locality.dcId()!=dcId ) {
				fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].push_back(std::make_pair(it.second.interf, it.second.processClass));
			}
		}

		for( auto& it : fitness_workers ) {
			auto& w = it.second;
			g_random->randomShuffle(w);
			for( int i=0; i < w.size(); i++ ) {
				id_used[w[i].first.locality.processId()]++;
				return WorkerFitnessInfo(w[i], it.first.first, it.first.second);
			}
		}

		throw no_more_servers();
	}

	vector<std::pair<WorkerInterface, ProcessClass>> getWorkersForRoleInDatacenter(Optional<Standalone<StringRef>> const& dcId, ProcessClass::ClusterRole role, int amount, DatabaseConfiguration const& conf, std::map< Optional<Standalone<StringRef>>, int>& id_used, WorkerFitnessInfo minWorker, bool checkStable = false ) {
		std::map<std::pair<ProcessClass::Fitness,int>, vector<std::pair<WorkerInterface, ProcessClass>>> fitness_workers;
		vector<std::pair<WorkerInterface, ProcessClass>> results;
		if (amount <= 0)
			return results;

		for( auto& it : id_worker ) {
			auto fitness = it.second.processClass.machineClassFitness( role );
			if( workerAvailable(it.second, checkStable) && !conf.isExcludedServer(it.second.interf.address()) && it.second.interf.id() != minWorker.worker.first.id() && (fitness < minWorker.fitness || (fitness == minWorker.fitness && id_used[it.first] <= minWorker.used)) && it.second.interf.locality.dcId()==dcId ) {
				fitness_workers[ std::make_pair(fitness, id_used[it.first]) ].push_back(std::make_pair(it.second.interf, it.second.processClass));
			}
		}

		for( auto& it : fitness_workers ) {
			auto& w = it.second;
			g_random->randomShuffle(w);
			for( int i=0; i < w.size(); i++ ) {
				results.push_back(w[i]);
				id_used[w[i].first.locality.processId()]++;
				if( results.size() == amount )
					return results;
			}
		}

		return results;
	}

	struct InDatacenterFitness {
		ProcessClass::Fitness proxyFit;
		ProcessClass::Fitness resolverFit;
		int proxyCount;
		int resolverCount;

		InDatacenterFitness( ProcessClass::Fitness proxyFit, ProcessClass::Fitness resolverFit, int proxyCount, int resolverCount)
			: proxyFit(proxyFit), resolverFit(resolverFit), proxyCount(proxyCount), resolverCount(resolverCount) {}

		InDatacenterFitness() : proxyFit( ProcessClass::NeverAssign ), resolverFit( ProcessClass::NeverAssign ) {}

		InDatacenterFitness( vector<std::pair<WorkerInterface, ProcessClass>> proxies, vector<std::pair<WorkerInterface, ProcessClass>> resolvers ) {
			proxyFit = ProcessClass::BestFit;
			resolverFit = ProcessClass::BestFit;
			for(auto it: proxies) {
				proxyFit = std::max(proxyFit, it.second.machineClassFitness( ProcessClass::Proxy ));
			}
			for(auto it: resolvers) {
				resolverFit = std::max(resolverFit, it.second.machineClassFitness( ProcessClass::Resolver ));
			}
			proxyCount = proxies.size();
			resolverCount = resolvers.size();
		}

		InDatacenterFitness( vector<MasterProxyInterface> proxies, vector<ResolverInterface> resolvers, vector<ProcessClass> proxyClasses, vector<ProcessClass> resolverClasses ) {
			std::set<Optional<Standalone<StringRef>>> dcs;
			proxyFit = ProcessClass::BestFit;
			resolverFit = ProcessClass::BestFit;
			for(int i = 0; i < proxies.size(); i++) {
				dcs.insert(proxies[i].locality.dcId());
				proxyFit = std::max(proxyFit, proxyClasses[i].machineClassFitness( ProcessClass::Proxy ));
			}
			for(int i = 0; i < resolvers.size(); i++) {
				dcs.insert(resolvers[i].locality.dcId());
				resolverFit = std::max(resolverFit, resolverClasses[i].machineClassFitness( ProcessClass::Resolver ));
			}

			proxyCount = proxies.size();
			resolverCount = resolvers.size();
		}

		bool operator < (InDatacenterFitness const& r) const {
			int lmax = std::max(resolverFit,proxyFit);
			int lmin = std::min(resolverFit,proxyFit);
			int rmax = std::max(r.resolverFit,r.proxyFit);
			int rmin = std::min(r.resolverFit,r.proxyFit);

			if( lmax != rmax ) return lmax < rmax;
			if( lmin != rmin ) return lmin < rmin;
			if(proxyCount != r.proxyCount) return proxyCount > r.proxyCount;
			return resolverCount > r.resolverCount;
		}

		bool betterInDatacenterFitness (InDatacenterFitness const& r) const {
			int lmax = std::max(resolverFit,proxyFit);
			int lmin = std::min(resolverFit,proxyFit);
			int rmax = std::max(r.resolverFit,r.proxyFit);
			int rmin = std::min(r.resolverFit,r.proxyFit);

			if( lmax != rmax ) return lmax < rmax;
			if( lmin != rmin ) return lmin < rmin;

			return false;
		}

		bool operator == (InDatacenterFitness const& r) const { return proxyFit == r.proxyFit && resolverFit == r.resolverFit && proxyCount == r.proxyCount && resolverCount == r.resolverCount; }
	};

	struct TLogFitness {
		ProcessClass::Fitness bestFit;
		ProcessClass::Fitness worstFit;
		int tlogCount;

		TLogFitness( ProcessClass::Fitness bestFit, ProcessClass::Fitness worstFit, int tlogCount) : bestFit(bestFit), worstFit(worstFit), tlogCount(tlogCount) {}

		TLogFitness() : bestFit( ProcessClass::NeverAssign ), worstFit( ProcessClass::NeverAssign ), tlogCount(0) {}

		TLogFitness( vector<std::pair<WorkerInterface, ProcessClass>> tlogs ) {
			worstFit = ProcessClass::BestFit;
			bestFit = ProcessClass::NeverAssign;
			for(auto it : tlogs) {
				auto thisFit = it.second.machineClassFitness( ProcessClass::TLog );
				worstFit = std::max(worstFit, thisFit);
				bestFit = std::min(bestFit, thisFit);
			}
			
			tlogCount = tlogs.size();
		}

		bool operator < (TLogFitness const& r) const {
			if (worstFit != r.worstFit) return worstFit < r.worstFit;
			if (bestFit != r.bestFit) return bestFit < r.bestFit;
			return tlogCount > r.tlogCount;
		}

		bool operator == (TLogFitness const& r) const { return worstFit == r.worstFit && bestFit == r.bestFit && tlogCount == r.tlogCount; }
	};

	std::set<Optional<Standalone<StringRef>>> getDatacenters( DatabaseConfiguration const& conf, bool checkStable = false ) {
		std::set<Optional<Standalone<StringRef>>> result;
		for( auto& it : id_worker )
			if( workerAvailable( it.second, checkStable ) && !conf.isExcludedServer( it.second.interf.address() ) )
				result.insert(it.second.interf.locality.dcId());
		return result;
	}

	RecruitFromConfigurationReply findWorkersForConfiguration( RecruitFromConfigurationRequest const& req ) {
		RecruitFromConfigurationReply result;
		std::map< Optional<Standalone<StringRef>>, int> id_used;

		if(req.recruitSeedServers) {
			auto storageServers = getWorkersForSeedServers(req.configuration);
			for(int i = 0; i < storageServers.size(); i++)
				result.storageServers.push_back(storageServers[i].first);
		}

		id_used[clusterControllerProcessId]++;
		id_used[masterProcessId]++;
		auto tlogs = getWorkersForTlogs( req.configuration, id_used );
		for(int i = 0; i < tlogs.size(); i++)
			result.tLogs.push_back(tlogs[i].first);

		auto datacenters = getDatacenters( req.configuration );

		InDatacenterFitness bestFitness;
		int numEquivalent = 1;

		for(auto dcId : datacenters ) {
			auto used = id_used;
			auto first_resolver = getWorkerForRoleInDatacenter( dcId, ProcessClass::Resolver, req.configuration, used );
			auto first_proxy = getWorkerForRoleInDatacenter( dcId, ProcessClass::Proxy, req.configuration, used );

			auto proxies = getWorkersForRoleInDatacenter( dcId, ProcessClass::Proxy, req.configuration.getDesiredProxies()-1, req.configuration, used, first_proxy );
			auto resolvers = getWorkersForRoleInDatacenter( dcId, ProcessClass::Resolver, req.configuration.getDesiredResolvers()-1, req.configuration, used, first_resolver );

			proxies.push_back(first_proxy.worker);
			resolvers.push_back(first_resolver.worker);

			auto fitness = InDatacenterFitness(proxies, resolvers);
			if(fitness < bestFitness) {
				bestFitness = fitness;
				numEquivalent = 1;
				result.resolvers = vector<WorkerInterface>();
				result.proxies = vector<WorkerInterface>();
				for(int i = 0; i < resolvers.size(); i++)
					result.resolvers.push_back(resolvers[i].first);
				for(int i = 0; i < proxies.size(); i++)
					result.proxies.push_back(proxies[i].first);
			} else if( fitness == bestFitness && g_random->random01() < 1.0/++numEquivalent ) {
				result.resolvers = vector<WorkerInterface>();
				result.proxies = vector<WorkerInterface>();
				for(int i = 0; i < resolvers.size(); i++)
					result.resolvers.push_back(resolvers[i].first);
				for(int i = 0; i < proxies.size(); i++)
					result.proxies.push_back(proxies[i].first);
			}
		}

		ASSERT(bestFitness != InDatacenterFitness());

		TraceEvent("findWorkersForConfig").detail("replication", req.configuration.tLogReplicationFactor)
			.detail("desiredLogs", req.configuration.getDesiredLogs()).detail("actualLogs", result.tLogs.size())
			.detail("desiredProxies", req.configuration.getDesiredProxies()).detail("actualProxies", result.proxies.size())
			.detail("desiredResolvers", req.configuration.getDesiredResolvers()).detail("actualResolvers", result.resolvers.size());

		if( now() - startTime < SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY &&
			( TLogFitness(tlogs) > TLogFitness((ProcessClass::Fitness)SERVER_KNOBS->EXPECTED_TLOG_FITNESS, (ProcessClass::Fitness)SERVER_KNOBS->EXPECTED_TLOG_FITNESS, req.configuration.getDesiredLogs()) ||
			bestFitness > InDatacenterFitness((ProcessClass::Fitness)SERVER_KNOBS->EXPECTED_PROXY_FITNESS, (ProcessClass::Fitness)SERVER_KNOBS->EXPECTED_RESOLVER_FITNESS, req.configuration.getDesiredProxies(), req.configuration.getDesiredResolvers()) ) ) {
			throw operation_failed();
		}

		return result;
	}

	bool betterMasterExists() {
		ServerDBInfo dbi = db.serverInfo->get();

		if(dbi.recoveryState < RecoveryState::FULLY_RECOVERED) {
			return false;
		}

		// Get master process
		auto masterWorker = id_worker.find(dbi.master.locality.processId());
		if(masterWorker == id_worker.end()) {
			return false;
		}

		// Get tlog processes
		std::vector<std::pair<WorkerInterface, ProcessClass>> tlogs;
		for( auto& it : dbi.logSystemConfig.tLogs ) {
			auto tlogWorker = id_worker.find(it.interf().locality.processId());
			if ( tlogWorker == id_worker.end() )
				return false;
			if ( tlogWorker->second.isExcluded )
				return true;
			tlogs.push_back(std::make_pair(tlogWorker->second.interf, tlogWorker->second.processClass));
		}

		// Get proxy classes
		std::vector<ProcessClass> proxyClasses;
		for(auto& it : dbi.client.proxies ) {
			auto proxyWorker = id_worker.find(it.locality.processId());
			if ( proxyWorker == id_worker.end() )
				return false;
			if ( proxyWorker->second.isExcluded )
				return true;
			proxyClasses.push_back(proxyWorker->second.processClass);
		}

		// Get resolver classes
		std::vector<ProcessClass> resolverClasses;
		for(auto& it : dbi.resolvers ) {
			auto resolverWorker = id_worker.find(it.locality.processId());
			if ( resolverWorker == id_worker.end() )
				return false;
			if ( resolverWorker->second.isExcluded )
				return true;
			resolverClasses.push_back(resolverWorker->second.processClass);
		}

		// Check master fitness. Don't return false if master is excluded in case all the processes are excluded, we still need master for recovery.
		ProcessClass::Fitness oldMasterFit = masterWorker->second.processClass.machineClassFitness( ProcessClass::Master );
		if(db.config.isExcludedServer(dbi.master.address())) {
			oldMasterFit = std::max(oldMasterFit, ProcessClass::ExcludeFit);
		}

		auto mworker = getMasterWorker(db.config, true);
		ProcessClass::Fitness newMasterFit = mworker.second.machineClassFitness( ProcessClass::Master );
		if(db.config.isExcludedServer(mworker.first.address())) {
			newMasterFit = std::max(newMasterFit, ProcessClass::ExcludeFit);
		}

		if ( oldMasterFit < newMasterFit )
			return false;
		if ( oldMasterFit > newMasterFit || ( dbi.master.locality.processId() == clusterControllerProcessId && mworker.first.locality.processId() != clusterControllerProcessId ) )
			return true;

		// Check tLog fitness
		std::map< Optional<Standalone<StringRef>>, int> id_used;
		id_used[clusterControllerProcessId]++;
		id_used[masterProcessId]++;

		TLogFitness oldTLogFit(tlogs);
		TLogFitness newTLotFit(getWorkersForTlogs(db.config, id_used, true));

		if(oldTLogFit < newTLotFit) return false;

		// Check proxy/resolver fitness
		InDatacenterFitness oldInFit(dbi.client.proxies, dbi.resolvers, proxyClasses, resolverClasses);

		auto datacenters = getDatacenters( db.config, true );
		InDatacenterFitness newInFit;
		for(auto dcId : datacenters) {
			auto used = id_used;
			auto first_resolver = getWorkerForRoleInDatacenter( dcId, ProcessClass::Resolver, db.config, used, true );
			auto first_proxy = getWorkerForRoleInDatacenter( dcId, ProcessClass::Proxy, db.config, used, true );

			auto proxies = getWorkersForRoleInDatacenter( dcId, ProcessClass::Proxy, db.config.getDesiredProxies()-1, db.config, used, first_proxy, true );
			auto resolvers = getWorkersForRoleInDatacenter( dcId, ProcessClass::Resolver, db.config.getDesiredResolvers()-1, db.config, used, first_resolver, true );
			proxies.push_back(first_proxy.worker);
			resolvers.push_back(first_resolver.worker);

			auto fitness = InDatacenterFitness(proxies, resolvers);
			if(fitness < newInFit)
				newInFit = fitness;
		}

		if(oldInFit.betterInDatacenterFitness(newInFit)) return false;

		if(oldTLogFit > newTLotFit || oldInFit > newInFit) {
			TraceEvent("BetterMasterExists", id).detail("oldMasterFit", oldMasterFit).detail("newMasterFit", newMasterFit)
				.detail("oldTLogFitC", oldTLogFit.tlogCount).detail("newTLotFitC", newTLotFit.tlogCount)
				.detail("oldTLogWorstFitT", oldTLogFit.worstFit).detail("newTLotWorstFitT", newTLotFit.worstFit)
				.detail("oldTLogBestFitT", oldTLogFit.bestFit).detail("newTLotBestFitT", newTLotFit.bestFit)
				.detail("oldInFitP", oldInFit.proxyFit).detail("newInFitP", newInFit.proxyFit)
				.detail("oldInFitR", oldInFit.resolverFit).detail("newInFitR", newInFit.resolverFit)
				.detail("oldInFitPC", oldInFit.proxyCount).detail("newInFitPC", newInFit.proxyCount)
				.detail("oldInFitRC", oldInFit.resolverCount).detail("newInFitRC", newInFit.resolverCount);
			return true;
		}

		return false;
	}

	std::map< Optional<Standalone<StringRef>>, WorkerInfo > id_worker;
	std::map<  Optional<Standalone<StringRef>>, ProcessClass > id_class; //contains the mapping from process id to process class from the database
	Standalone<RangeResultRef> lastProcessClasses;
	bool gotProcessClasses;
	bool gotFullyRecoveredConfig;
	Optional<Standalone<StringRef>> masterProcessId;
	Optional<Standalone<StringRef>> clusterControllerProcessId;
	UID id;
	std::vector<RecruitFromConfigurationRequest> outstandingRecruitmentRequests;
	std::vector<std::pair<RecruitStorageRequest, double>> outstandingStorageRequests;
	ActorCollection ac;
	UpdateWorkerList updateWorkerList;
	Future<Void> betterMasterExistsChecker;

	DBInfo db;
	Database cx;
	double startTime;

	explicit ClusterControllerData( ClusterControllerFullInterface ccInterface )
		: id(ccInterface.id()), ac(false), betterMasterExistsChecker(Void()), gotProcessClasses(false), gotFullyRecoveredConfig(false), startTime(now())
	{
		auto serverInfo = db.serverInfo->get();
		serverInfo.id = g_random->randomUniqueID();
		serverInfo.masterLifetime.ccID = id;
		serverInfo.clusterInterface = ccInterface;
		db.serverInfo->set( serverInfo );
		cx = openDBOnServer(db.serverInfo, TaskDefaultEndpoint, true, true);
	}

	~ClusterControllerData() {
		ac.clear(false);
		id_worker.clear();
	}
};

template <class K, class T>
vector<T> values( std::map<K,T> const& map ) {
	vector<T> t;
	for(auto i = map.begin(); i!=map.end(); ++i)
		t.push_back(i->second);
	return t;
}

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
			state std::pair<WorkerInterface, ProcessClass> masterWorker = cluster->getMasterWorker(db->config);
			if( ( masterWorker.second.machineClassFitness( ProcessClass::Master ) > SERVER_KNOBS->EXPECTED_MASTER_FITNESS || masterWorker.first.locality.processId() == cluster->clusterControllerProcessId )
				&& now() - cluster->startTime < SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY ) {
				TraceEvent("CCWDB", cluster->id).detail("Fitness", masterWorker.second.machineClassFitness( ProcessClass::Master ));
				Void _ = wait( delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
				continue;
			}
			RecruitMasterRequest rmq;
			rmq.lifetime = db->serverInfo->get().masterLifetime;

			cluster->masterProcessId = masterWorker.first.locality.processId();
			ErrorOr<MasterInterface> newMaster = wait( masterWorker.first.master.tryGetReply( rmq ) );
			if (newMaster.present()) {
				TraceEvent("CCWDB", cluster->id).detail("Recruited", newMaster.get().id());

				// for status tool
				TraceEvent("RecruitedMasterWorker", cluster->id)
					.detail("Address", newMaster.get().address())
					.trackLatest("DB/RecruitedMasterWorker");

				iMaster = newMaster.get();

				db->masterRegistrationCount = 0;
				db->forceMasterFailure = Promise<Void>();

				auto dbInfo = ServerDBInfo( LiteralStringRef("DB") );
				dbInfo.master = iMaster;
				dbInfo.id = g_random->randomUniqueID();
				dbInfo.masterLifetime = db->serverInfo->get().masterLifetime;
				++dbInfo.masterLifetime;
				dbInfo.clusterInterface = db->serverInfo->get().clusterInterface;

				TraceEvent("CCWDB", cluster->id).detail("Lifetime", dbInfo.masterLifetime.toString()).detail("ChangeID", dbInfo.id);
				db->serverInfo->set( dbInfo );

				Void _ = wait( delay(SERVER_KNOBS->MASTER_SPIN_DELAY) );  // Don't retry master recovery more than once per second, but don't delay the "first" recovery after more than a second of normal operation

				TraceEvent("CCWDB", cluster->id).detail("Watching", iMaster.id());

				// Master failure detection is pretty sensitive, but if we are in the middle of a very long recovery we really don't want to have to start over
				loop choose {
					when (Void _ = wait( waitFailureClient( iMaster.waitFailure, db->masterRegistrationCount ?
						SERVER_KNOBS->MASTER_FAILURE_REACTION_TIME : (now() - recoveryStart) * SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY,
						db->masterRegistrationCount ? -SERVER_KNOBS->MASTER_FAILURE_REACTION_TIME/SERVER_KNOBS->SECONDS_BEFORE_NO_FAILURE_DELAY : SERVER_KNOBS->MASTER_FAILURE_SLOPE_DURING_RECOVERY ) || db->forceMasterFailure.getFuture() )) { break; }
					when (Void _ = wait( db->serverInfo->onChange() )) {}
				}

				TEST(true); // clusterWatchDatabase() master failed
				TraceEvent(SevWarn,"DetectedFailedMaster", cluster->id).detail("OldMaster", iMaster.id());
			} else {
				TEST(true); //clusterWatchDatabas() !newMaster.present()
				Void _ = wait( delay(SERVER_KNOBS->MASTER_SPIN_DELAY) );
			}
		} catch (Error& e) {
			TraceEvent("CCWDB", cluster->id).error(e, true).detail("Master", iMaster.id());
			if (e.code() == error_code_actor_cancelled) throw;

			bool ok = e.code() == error_code_no_more_servers;
			TraceEvent(ok ? SevWarn : SevError,"clusterWatchDatabaseRetrying", cluster->id).error(e);
			if (!ok)
				throw e;
			Void _ = wait( delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
		}
	}
}

void addIssue( ProcessIssuesMap& issueMap, NetworkAddress const& addr, std::string const& issue, UID& issueID ) {
	auto& e = issueMap[addr];
	e.first = issue;
	e.second = issueID = g_random->randomUniqueID();
	if (!issue.size()) issueMap.erase(addr);
}

void removeIssue( ProcessIssuesMap& issueMap, NetworkAddress const& addr, std::string const& issue, UID& issueID ) {
	if (!issue.size()) return;
	if ( issueMap.count(addr) && issueMap[addr].second == issueID )
		issueMap.erase( addr );
}

ACTOR Future<Void> clusterGetServerInfo(
	ClusterControllerData::DBInfo* db,
	UID knownServerInfoID,
	std::string issues,
	std::vector<NetworkAddress> incompatiblePeers,
	ReplyPromise<ServerDBInfo> reply)
{
	state UID issueID;
	addIssue( db->workersWithIssues, reply.getEndpoint().address, issues, issueID );
	for(auto it : incompatiblePeers) {
		db->incompatibleConnections[it] = now() + SERVER_KNOBS->INCOMPATIBLE_PEERS_LOGGING_INTERVAL;
	}

	while (db->serverInfo->get().id == knownServerInfoID) {
		choose {
			when (Void _ = wait( db->serverInfo->onChange() )) {}
			when (Void _ = wait( delayJittered( 300 ) )) { break; }  // The server might be long gone!
		}
	}

	removeIssue( db->workersWithIssues, reply.getEndpoint().address, issues, issueID );

	reply.send( db->serverInfo->get() );
	return Void();
}

ACTOR Future<Void> clusterOpenDatabase(
	ClusterControllerData::DBInfo* db,
	Standalone<StringRef> dbName,
	UID knownClientInfoID,
	std::string issues,
	Standalone<VectorRef<ClientVersionRef>> supportedVersions,
	Standalone<StringRef> traceLogGroup,
	ReplyPromise<ClientDBInfo> reply)
{
	// NOTE: The client no longer expects this function to return errors
	state UID issueID;
	addIssue( db->clientsWithIssues, reply.getEndpoint().address, issues, issueID );

	if(supportedVersions.size() > 0) {
		db->clientVersionMap[reply.getEndpoint().address] = supportedVersions;
	}

	db->traceLogGroupMap[reply.getEndpoint().address] = traceLogGroup.toString();

	while (db->clientInfo->get().id == knownClientInfoID) {
		choose {
			when (Void _ = wait( db->clientInfo->onChange() )) {}
			when (Void _ = wait( delayJittered( 300 ) )) { break; }  // The client might be long gone!
		}
	}

	removeIssue( db->clientsWithIssues, reply.getEndpoint().address, issues, issueID );
	db->clientVersionMap.erase(reply.getEndpoint().address);
	db->traceLogGroupMap.erase(reply.getEndpoint().address);

	reply.send( db->clientInfo->get() );
	return Void();
}

void checkOutstandingRecruitmentRequests( ClusterControllerData* self ) {
	for( int i = 0; i < self->outstandingRecruitmentRequests.size(); i++ ) {
		RecruitFromConfigurationRequest& req = self->outstandingRecruitmentRequests[i];
		try {
			req.reply.send( self->findWorkersForConfiguration( req ) );
			std::swap( self->outstandingRecruitmentRequests[i--], self->outstandingRecruitmentRequests.back() );
			self->outstandingRecruitmentRequests.pop_back();
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

void checkOutstandingStorageRequests( ClusterControllerData* self ) {
	for( int i = 0; i < self->outstandingStorageRequests.size(); i++ ) {
		auto& req = self->outstandingStorageRequests[i];
		try {
			if(req.second < now()) {
				req.first.reply.sendError(timed_out());
				std::swap( self->outstandingStorageRequests[i--], self->outstandingStorageRequests.back() );
				self->outstandingStorageRequests.pop_back();
			} else {
				if(!self->gotProcessClasses && !req.first.criticalRecruitment)
					throw no_more_servers();

				auto worker = self->getStorageWorker(req.first);
				RecruitStorageReply rep;
				rep.worker = worker.first;
				rep.processClass = worker.second;
				req.first.reply.send( rep );
				std::swap( self->outstandingStorageRequests[i--], self->outstandingStorageRequests.back() );
				self->outstandingStorageRequests.pop_back();
			}
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers) {
				TraceEvent(SevWarn, "RecruitStorageNotAvailable", self->id).error(e);
			} else {
				TraceEvent(SevError, "RecruitStorageError", self->id).error(e);
				throw;
			}
		}
	}
}

ACTOR Future<Void> doCheckOutstandingMasterRequests( ClusterControllerData* self ) {
	Void _ = wait( delay(SERVER_KNOBS->CHECK_BETTER_MASTER_INTERVAL) );
	if (self->betterMasterExists()) {
		if (!self->db.forceMasterFailure.isSet()) {
			self->db.forceMasterFailure.send( Void() );
			TraceEvent("MasterRegistrationKill", self->id).detail("MasterId", self->db.serverInfo->get().master.id());
		}
	}
	return Void();
}

void checkOutstandingMasterRequests( ClusterControllerData* self ) {
	if( !self->betterMasterExistsChecker.isReady() )
		return;

	self->betterMasterExistsChecker = doCheckOutstandingMasterRequests(self);
}

void checkOutstandingRequests( ClusterControllerData* self ) {
	checkOutstandingRecruitmentRequests( self );
	checkOutstandingStorageRequests( self );
	checkOutstandingMasterRequests( self );
}

ACTOR Future<Void> rebootAndCheck( ClusterControllerData* cluster, Optional<Standalone<StringRef>> processID ) {
	auto watcher = cluster->id_worker.find(processID);
	ASSERT(watcher != cluster->id_worker.end());

	watcher->second.reboots++;
	Void _ = wait( delay( g_network->isSimulated() ? SERVER_KNOBS->SIM_SHUTDOWN_TIMEOUT : SERVER_KNOBS->SHUTDOWN_TIMEOUT ) );

	auto watcher = cluster->id_worker.find(processID);
	if(watcher != cluster->id_worker.end()) {
		watcher->second.reboots--;
		if( watcher->second.reboots < 2 )
			checkOutstandingMasterRequests( cluster );
	}

	return Void();
}

ACTOR Future<Void> workerAvailabilityWatch( WorkerInterface worker, ProcessClass startingClass, ClusterControllerData* cluster ) {
	state Future<Void> failed = waitFailureClient( worker.waitFailure, SERVER_KNOBS->WORKER_FAILURE_TIME );
	cluster->updateWorkerList.set( worker.locality.processId(), ProcessData(worker.locality, startingClass, worker.address()) );
	loop {
		choose {
			when( Void _ = wait( IFailureMonitor::failureMonitor().onStateEqual( worker.storage.getEndpoint(), FailureStatus(IFailureMonitor::failureMonitor().getState( worker.storage.getEndpoint() ).isAvailable()) ) ) ) {
				if( IFailureMonitor::failureMonitor().getState( worker.storage.getEndpoint() ).isAvailable() ) {
					cluster->ac.add( rebootAndCheck( cluster, worker.locality.processId() ) );
					checkOutstandingRequests( cluster );
				}
			}
			when( Void _ = wait( failed ) ) {  // remove workers that have failed
				WorkerInfo& failedWorkerInfo = cluster->id_worker[ worker.locality.processId() ];
				if (!failedWorkerInfo.reply.isSet()) {
					failedWorkerInfo.reply.send( RegisterWorkerReply(failedWorkerInfo.processClass, failedWorkerInfo.isExcluded) );
				}
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

//The failure monitor client relies on the fact that the failure detection server will not declare itself failed
ACTOR Future<Void> failureDetectionServer( UID uniqueID, FutureStream< FailureMonitoringRequest > requests ) {
	state Version currentVersion = 0;
	state std::map<NetworkAddress, FailureStatusInfo> currentStatus;	// The status at currentVersion
	state std::deque<SystemFailureStatus> statusHistory;	// The last change in statusHistory is from currentVersion-1 to currentVersion
	state Future<Void> periodically = Void();
	state double lastT = 0;

	loop choose {
		when ( FailureMonitoringRequest req = waitNext( requests ) ) {
			if ( req.senderStatus.present() ) {
				// Update the status of requester, if necessary
				auto& address = req.reply.getEndpoint().address;
				auto& stat = currentStatus[ address ];
				auto& newStat = req.senderStatus.get();

				ASSERT( !newStat.failed || address != g_network->getLocalAddress() );

				stat.insertRequest(now());
				if (req.senderStatus != stat.status) {
					TraceEvent("FailureDetectionStatus", uniqueID).detail("System", address).detail("Status", newStat.failed ? "Failed" : "OK").detail("Why", "Request");
					statusHistory.push_back( SystemFailureStatus( address, newStat ) );
					++currentVersion;

					if (req.senderStatus == FailureStatus()){
						// failureMonitorClient reports explicitly that it is failed
						ASSERT(false); // This can't happen at the moment; if that changes, make this a TEST instead
						currentStatus.erase(address);
					} else {
						TEST(true);
						stat.status = newStat;
					}

					while (statusHistory.size() > currentStatus.size())
						statusHistory.pop_front();
				}
			}

			// Return delta-compressed status changes to requester
			Version reqVersion = req.failureInformationVersion;
			if (reqVersion > currentVersion){
				req.reply.sendError( future_version() );
				ASSERT(false);
			} else {
				TEST(true); // failureDetectionServer sending failure data to requester
				FailureMonitoringReply reply;
				reply.failureInformationVersion = currentVersion;
				if( req.senderStatus.present() ) {
					reply.clientRequestIntervalMS = FLOW_KNOBS->SERVER_REQUEST_INTERVAL * 1000;
					reply.considerServerFailedTimeoutMS = CLIENT_KNOBS->FAILURE_TIMEOUT_DELAY * 1000;
				} else {
					reply.clientRequestIntervalMS = FLOW_KNOBS->CLIENT_REQUEST_INTERVAL * 1000;
					reply.considerServerFailedTimeoutMS = CLIENT_KNOBS->CLIENT_FAILURE_TIMEOUT_DELAY * 1000;
				}

				ASSERT( currentVersion >= (int64_t)statusHistory.size());

				if (reqVersion < currentVersion - (int64_t)statusHistory.size() || reqVersion == 0) {
					// Send everything
					TEST(true); // failureDetectionServer sending all current data to requester
					reply.allOthersFailed = true;
					for(auto it = currentStatus.begin(); it != currentStatus.end(); ++it)
						reply.changes.push_back( reply.arena, SystemFailureStatus( it->first, it->second.status ) );
				} else {
					TEST(true); // failureDetectionServer sending delta-compressed data to requester
					// SOMEDAY: Send only the last change for a given address?
					reply.allOthersFailed = false;
					for(int v = reqVersion - currentVersion + statusHistory.size(); v < statusHistory.size(); v++) {
						reply.changes.push_back( reply.arena, statusHistory[v] );
					}
				}
				req.reply.send( reply );
			}
		}
		when ( Void _ = wait( periodically ) ) {
			periodically = delay( FLOW_KNOBS->SERVER_REQUEST_INTERVAL );
			double t = now();
			if (lastT != 0 && t - lastT > 1)
				TraceEvent("LongDelayOnClusterController").detail("Duration", t - lastT);
			lastT = t;

			// Adapt to global unresponsiveness
			vector<double> delays;
			for(auto it=currentStatus.begin(); it!=currentStatus.end(); it++)
				if (it->second.penultimateRequestTime) {
					delays.push_back(it->second.latency(t));
					TraceEvent("FDData", uniqueID).detail("S", it->first.toString()).detail("L", it->second.latency(t));
				}
			int pivot = std::max(0, (int)delays.size()-2);
			double pivotDelay = 0;
			if (delays.size()) {
				std::nth_element(delays.begin(), delays.begin()+pivot, delays.end());
				pivotDelay = *(delays.begin()+pivot);
			}
			pivotDelay = std::max(0.0, pivotDelay - FLOW_KNOBS->SERVER_REQUEST_INTERVAL);

			TraceEvent("FailureDetectionPoll", uniqueID).detail("PivotDelay", pivotDelay).detail("Clients", currentStatus.size());
			//TraceEvent("FailureDetectionAcceptableDelay").detail("ms", acceptableDelay*1000);

			for(auto it = currentStatus.begin(); it != currentStatus.end(); ) {
				double delay = t - it->second.lastRequestTime;

				if ( it->first != g_network->getLocalAddress() && ( delay > pivotDelay * 2 + FLOW_KNOBS->SERVER_REQUEST_INTERVAL + CLIENT_KNOBS->FAILURE_MIN_DELAY || delay > CLIENT_KNOBS->FAILURE_MAX_DELAY ) ) {
					//printf("Failure Detection Server: Status of '%s' is now '%s' after %f sec\n", it->first.toString().c_str(), "Failed", now() - it->second.lastRequestTime);
					TraceEvent("FailureDetectionStatus", uniqueID).detail("System", it->first).detail("Status","Failed").detail("Why", "Timeout").detail("LastRequestAge", delay)
						.detail("PivotDelay", pivotDelay);
					statusHistory.push_back( SystemFailureStatus( it->first, FailureStatus(true) ) );
					++currentVersion;
					it = currentStatus.erase(it);
					while (statusHistory.size() > currentStatus.size())
						statusHistory.pop_front();
				} else {
					++it;
				}
			}
		}
	}
}

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
		rep.worker = worker.first;
		rep.processClass = worker.second;
		req.reply.send( rep );
	} catch ( Error& e ) {
		if (e.code() == error_code_no_more_servers) {
			self->outstandingStorageRequests.push_back( std::make_pair(req, now() + SERVER_KNOBS->RECRUITMENT_TIMEOUT) );
			TraceEvent(SevWarn, "RecruitStorageNotAvailable", self->id).error(e);
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
			req.reply.send( self->findWorkersForConfiguration( req ) );
			return Void();
		} catch (Error& e) {
			if (e.code() == error_code_no_more_servers && now() - self->startTime >= SERVER_KNOBS->WAIT_FOR_GOOD_RECRUITMENT_DELAY) {
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
		Void _ = wait( delay(SERVER_KNOBS->ATTEMPT_RECRUITMENT_DELAY) );
	}
}

void clusterRegisterMaster( ClusterControllerData* self, RegisterMasterRequest const& req ) {
	req.reply.send( Void() );

	TraceEvent("MasterRegistrationReceived", self->id).detail("dbName", printable(req.dbName)).detail("MasterId", req.id).detail("Master", req.mi.toString()).detail("Tlogs", describe(req.logSystemConfig.tLogs)).detail("Resolvers", req.resolvers.size())
		.detail("RecoveryState", req.recoveryState).detail("RegistrationCount", req.registrationCount).detail("Proxies", req.proxies.size()).detail("RecoveryCount", req.recoveryCount);

	//make sure the request comes from an active database
	auto db = &self->db;
	if ( db->serverInfo->get().master.id() != req.id || req.registrationCount <= db->masterRegistrationCount ) {
		TraceEvent("MasterRegistrationNotFound", self->id).detail("dbName", printable(req.dbName)).detail("MasterId", req.id).detail("existingId", db->serverInfo->get().master.id()).detail("RegCount", req.registrationCount).detail("ExistingRegCount", db->masterRegistrationCount);
		return;
	}

	db->masterRegistrationCount = req.registrationCount;
	if ( req.configuration.present() ) {
		db->config = req.configuration.get();

		if ( req.recoveryState >= RecoveryState::FULLY_RECOVERED ) {
			self->gotFullyRecoveredConfig = true;
			db->fullyRecoveredConfig = req.configuration.get();
			for ( auto& it : self->id_worker ) {
				bool isExcludedFromConfig = db->fullyRecoveredConfig.isExcludedServer(it.second.interf.address());
				if ( it.second.isExcluded != isExcludedFromConfig && !it.second.reply.isSet() ) {
					it.second.reply.send( RegisterWorkerReply( it.second.processClass, isExcludedFromConfig) );
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
	if (db->clientInfo->get().proxies != req.proxies) {
		isChanged = true;
		ClientDBInfo clientInfo;
		clientInfo.id = g_random->randomUniqueID();
		clientInfo.proxies = req.proxies;
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
		dbInfo.id = g_random->randomUniqueID();
		self->db.serverInfo->set( dbInfo );
	}

	checkOutstandingMasterRequests(self);
}

void registerWorker( RegisterWorkerRequest req, ClusterControllerData *self ) {
	WorkerInterface w = req.wi;
	ProcessClass newProcessClass = req.processClass;
	bool newIsExcluded = req.isExcluded;
	auto info = self->id_worker.find( w.locality.processId() );

	TraceEvent("ClusterControllerActualWorkers", self->id).detail("WorkerID",w.id()).detailext("ProcessID", w.locality.processId()).detailext("ZoneId", w.locality.zoneId()).detailext("DataHall", w.locality.dataHallId()).detail("pClass", req.processClass.toString()).detail("Workers", self->id_worker.size()).detail("Registered", (info == self->id_worker.end() ? "False" : "True")).backtrace();

	if ( w.address() == g_network->getLocalAddress() ) {
		self->clusterControllerProcessId = w.locality.processId();
	}

	// Check process class and exclusive property
	if ( info == self->id_worker.end() || info->second.interf.id() != w.id() || req.generation >= info->second.gen ) {
		if ( self->gotProcessClasses ) {
			auto classIter = self->id_class.find(w.locality.processId());
			
			if( classIter != self->id_class.end() && (classIter->second.classSource() == ProcessClass::DBSource || req.initialClass.classType() == ProcessClass::UnsetClass)) {
				newProcessClass = classIter->second;
			} else {
				newProcessClass = req.initialClass;
			}
		}

		if ( self->gotFullyRecoveredConfig ) {
			newIsExcluded = self->db.fullyRecoveredConfig.isExcludedServer(w.address());
		}

		// Notify the worker to register again with new process class/exclusive property
		if ( !req.reply.isSet() && ( newProcessClass != req.processClass || newIsExcluded != req.isExcluded ) ) {
			req.reply.send( RegisterWorkerReply(newProcessClass, newIsExcluded) );
		}
	}

	if( info == self->id_worker.end() ) {
		self->id_worker[w.locality.processId()] = WorkerInfo( workerAvailabilityWatch( w, newProcessClass, self ), req.reply, req.generation, w, req.initialClass, newProcessClass, req.isExcluded );
		checkOutstandingRequests( self );
		return;
	}

	if( info->second.interf.id() != w.id() || req.generation >= info->second.gen ) {
		if (!info->second.reply.isSet()) {
			info->second.reply.send( Never() );
		}
		info->second.reply = req.reply;
		info->second.processClass = newProcessClass;
		info->second.isExcluded = req.isExcluded;
		info->second.initialClass = req.initialClass;
		info->second.gen = req.generation;

		if(info->second.interf.id() != w.id()) {
			info->second.interf = w;
			info->second.watcher = workerAvailabilityWatch( w, newProcessClass, self );
		}
		checkOutstandingRequests( self );
		return;
	}

	TEST(true); // Received an old worker registration request.
}

#define TIME_KEEPER_VERSION LiteralStringRef("1")

ACTOR Future<Void> timeKeeperSetVersion(ClusterControllerData *self) {
	loop {
		state Reference<ReadYourWritesTransaction> tr = Reference<ReadYourWritesTransaction>(
				new ReadYourWritesTransaction(self->cx));
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr->set(timeKeeperVersionKey, TIME_KEEPER_VERSION);
			Void _ = wait(tr->commit());
			break;
		} catch (Error &e) {
			Void _ = wait(tr->onError(e));
		}
	}

	return Void();
}

// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid running
// out of space, it limits the max number of entries and clears old entries on each update. This mapping is used from
// backup and restore to get the version information for a timestamp.
ACTOR Future<Void> timeKeeper(ClusterControllerData *self) {
	state KeyBackedMap<int64_t, Version> versionMap(timeKeeperPrefixRange.begin);

	TraceEvent(SevInfo, "TimeKeeperStarted");

	Void _ = wait(timeKeeperSetVersion(self));

	loop {
		state Reference<ReadYourWritesTransaction> tr = Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(self->cx));
		loop {
			try {
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

				Void _ = wait(tr->commit());
				break;
			} catch (Error &e) {
				Void _ = wait(tr->onError(e));
			}
		}

		Void _ = wait(delay(SERVER_KNOBS->TIME_KEEPER_DELAY));
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
			requests_batch.push_back(req);

			// Earliest time at which we may begin a new request
			double next_allowed_request_time = last_request_time + SERVER_KNOBS->STATUS_MIN_TIME_BETWEEN_REQUESTS;

			// Wait if needed to satisfy min_time knob, also allows more requets to queue up.
			double minwait = std::max(next_allowed_request_time - now(), 0.0);
			Void _ = wait(delay(minwait));

			// Get all requests that are ready right *now*, before GetStatus() begins.
			// All of these requests will be responded to with the next GetStatus() result.
			while (requests.isReady())
				requests_batch.push_back(requests.pop());

			// Get status but trap errors to send back to client.
			vector<std::pair<WorkerInterface, ProcessClass>> workers;
			for(auto& it : self->id_worker)
				workers.push_back(std::make_pair(it.second.interf, it.second.processClass));

			std::vector<NetworkAddress> incompatibleConnections;
			for(auto it = self->db.incompatibleConnections.begin(); it != self->db.incompatibleConnections.end();) {
				if(it->second < now()) {
					it = self->db.incompatibleConnections.erase(it);
				} else {
					incompatibleConnections.push_back(it->first);
					it++;
				}
			}

			ErrorOr<StatusReply> result = wait(errorOr(clusterGetStatus(self->db.serverInfo, self->cx, workers, self->db.workersWithIssues, self->db.clientsWithIssues, self->db.clientVersionMap, self->db.traceLogGroupMap, coordinators, incompatibleConnections)));
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

			Void _ = wait(trVer.commit());
			TraceEvent("ProcessClassUpgrade");
			break;
		}
		catch(Error &e) {
			Void _ = wait( trVer.onError(e) );
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

						if (newProcessClass != w.second.processClass) {
							w.second.processClass = newProcessClass;
							if (!w.second.reply.isSet()) {
								w.second.reply.send( RegisterWorkerReply(newProcessClass, w.second.isExcluded) );
							}
						}
					}

					self->lastProcessClasses = processClasses;
					self->gotProcessClasses = true;
					checkOutstandingRequests( self );
				}

				state Future<Void> watchFuture = tr.watch(processClassChangeKey);
				Void _ = wait(tr.commit());
				Void _ = wait(watchFuture);
				break;
			}
			catch(Error &e) {
				Void _ = wait( tr.onError(e) );
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
					clientInfo.id = g_random->randomUniqueID();
					clientInfo.clientTxnInfoSampleRate = sampleRate;
					clientInfo.clientTxnInfoSizeLimit = sizeLimit;
					db->clientInfo->set(clientInfo);
				}
				
				state Future<Void> watchRateFuture = tr.watch(fdbClientInfoTxnSampleRate);
				state Future<Void> watchLimitFuture = tr.watch(fdbClientInfoTxnSizeLimit);
				Void _ = wait(tr.commit());
				choose {
					when(Void _ = wait(watchRateFuture)) { break; }
					when (Void _ = wait(watchLimitFuture)) { break; }
				}
			}
			catch (Error &e) {
				Void _ = wait(tr.onError(e));
			}
		}
	}
}

ACTOR Future<Void> clusterControllerCore( ClusterControllerFullInterface interf, Future<Void> leaderFail, ServerCoordinators coordinators ) {
	state ClusterControllerData self( interf );
	state Future<Void> coordinationPingDelay = delay( SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY );
	state uint64_t step = 0;
	state PromiseStream<Future<Void>> addActor;
	state Future<ErrorOr<Void>> error = errorOr( actorCollection( addActor.getFuture() ) );

	auto pSelf = &self;
	addActor.send( failureDetectionServer( self.id, interf.clientInterface.failureMonitoring.getFuture() ) );
	addActor.send( clusterWatchDatabase( &self, &self.db ) );  // Start the master database
	addActor.send( self.updateWorkerList.init( self.db.db ) );
	addActor.send( statusServer( interf.clientInterface.databaseStatus.getFuture(), &self, coordinators));
	addActor.send( timeKeeper(&self) );
	addActor.send( monitorProcessClasses(&self) );
	addActor.send( monitorClientTxnInfoConfigs(&self.db) );
	//printf("%s: I am the cluster controller\n", g_network->getLocalAddress().toString().c_str());

	loop choose {
		when( ErrorOr<Void> err = wait( error ) ) {
			if (err.isError()) {
				endRole(interf.id(), "ClusterController", "Stop Received Error", false, err.getError());
			}
			else {
				endRole(interf.id(), "ClusterController", "Stop Received Signal", true);
			}

			// We shut down normally even if there was a serious error (so this fdbserver may be re-elected cluster controller)
			return Void();
		}
		when( OpenDatabaseRequest req = waitNext( interf.clientInterface.openDatabase.getFuture() ) ) {
			addActor.send( clusterOpenDatabase( &self.db, req.dbName, req.knownClientInfoID, req.issues.toString(), req.supportedVersions, req.traceLogGroup, req.reply ) );
		}
		when( RecruitFromConfigurationRequest req = waitNext( interf.recruitFromConfiguration.getFuture() ) ) {
			addActor.send( clusterRecruitFromConfiguration( &self, req ) );
		}
		when( RecruitStorageRequest req = waitNext( interf.recruitStorage.getFuture() ) ) {
			clusterRecruitStorage( &self, req );
		}
		when( RegisterWorkerRequest req = waitNext( interf.registerWorker.getFuture() ) ) {
			registerWorker( req, &self );
		}
		when( GetWorkersRequest req = waitNext( interf.getWorkers.getFuture() ) ) {
			vector<std::pair<WorkerInterface, ProcessClass>> workers;

			auto masterAddr = self.db.serverInfo->get().master.address();
			for(auto& it : self.id_worker) {
				if ( (req.flags & GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY) && self.db.config.isExcludedServer(it.second.interf.address()) ) {
					continue;
				}

				if ( (req.flags & GetWorkersRequest::TESTER_CLASS_ONLY) && it.second.processClass.classType() != ProcessClass::TesterClass ) {
					continue;
				}

				workers.push_back(std::make_pair(it.second.interf, it.second.processClass));
			}

			req.reply.send( workers );
		}
		when( GetClientWorkersRequest req = waitNext( interf.clientInterface.getClientWorkers.getFuture() ) ) {
			vector<ClientWorkerInterface> workers;
			for(auto& it : self.id_worker) {
				if (it.second.processClass.classType() != ProcessClass::TesterClass) {
					workers.push_back(it.second.interf.clientInterface);
				}
			}
			req.reply.send(workers);
		}
		when( Void _ = wait( coordinationPingDelay ) ) {
			CoordinationPingMessage message(self.id, step++);
			for(auto& it : self.id_worker)
				it.second.interf.coordinationPing.send(message);
			coordinationPingDelay = delay( SERVER_KNOBS->WORKER_COORDINATION_PING_DELAY );
			TraceEvent("CoordinationPingSent", self.id).detail("TimeStep", message.timeStep);
		}
		when( RegisterMasterRequest req = waitNext( interf.registerMaster.getFuture() ) ) {
			clusterRegisterMaster( &self, req );
		}
		when( GetServerDBInfoRequest req = waitNext( interf.getServerDBInfo.getFuture() ) ) {
			addActor.send( clusterGetServerInfo( &self.db, req.knownServerInfoID, req.issues.toString(), req.incompatiblePeers, req.reply ) );
		}
		when( Void _ = wait( leaderFail ) ) {
			// We are no longer the leader if this has changed.
			endRole(interf.id(), "ClusterController", "Leader Replaced", true);
			TEST(true); // Lost Cluster Controller Role
			return Void();
		}
		when( ReplyPromise<Void> ping = waitNext( interf.clientInterface.ping.getFuture() ) ) {
			ping.send( Void() );
		}
	}
}

ACTOR Future<Void> clusterController( ServerCoordinators coordinators, Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC, bool hasConnected, Reference<AsyncVar<ProcessClass>> asyncProcessClass, Reference<AsyncVar<bool>> asyncIsExcluded ) {
	loop {
		state ClusterControllerFullInterface cci;
		state bool inRole = false;
		cci.initEndpoints();
		try {
			//Register as a possible leader; wait to be elected
			state Future<Void> leaderFail = tryBecomeLeader( coordinators, cci, currentCC, hasConnected, asyncProcessClass, asyncIsExcluded );

			while (!currentCC->get().present() || currentCC->get().get() != cci) {
				choose {
					when( Void _ = wait(currentCC->onChange()) ) {}
					when( Void _ = wait(leaderFail) ) { ASSERT(false); throw internal_error(); }
				}
			}

			hasConnected = true;
			startRole(cci.id(), UID(), "ClusterController");
			inRole = true;

			Void _ = wait( clusterControllerCore( cci, leaderFail, coordinators ) );
		} catch(Error& e) {
			if (inRole)
				endRole(cci.id(), "ClusterController", "Error", e.code() == error_code_actor_cancelled || e.code() == error_code_coordinators_changed, e);
			else
				TraceEvent( e.code() == error_code_coordinators_changed ? SevInfo : SevError, "ClusterControllerCandidateError", cci.id()).error(e);
			throw;
		}
	}
}

ACTOR Future<Void> clusterController( Reference<ClusterConnectionFile> connFile, Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> currentCC, Reference<AsyncVar<ProcessClass>> asyncProcessClass, Reference<AsyncVar<bool>> asyncIsExcluded) {
	state bool hasConnected = false;
	loop {
		try {
			ServerCoordinators coordinators( connFile );
			Void _ = wait( clusterController( coordinators, currentCC, hasConnected, asyncProcessClass, asyncIsExcluded ) );
		} catch( Error &e ) {
			if( e.code() != error_code_coordinators_changed )
				throw; // Expected to terminate fdbserver
		}

		hasConnected = true;
	}
}
