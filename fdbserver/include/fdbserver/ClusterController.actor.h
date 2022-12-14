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
#include "fdbclient/Metacluster.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationUtils.h"
#include "fdbserver/ClusterControllerDBInfo.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WorkerInfo.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/SystemMonitor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

class ClusterController {
	friend class ClusterControllerImpl;

public:
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

	bool workerAvailable(WorkerInfo const& worker, bool checkStable) const;
	bool isLongLivedStateless(Optional<Key> const& processId) const;
	WorkerDetails getStorageWorker(RecruitStorageRequest const& req);

	// Returns a worker that can be used by a blob worker
	// Note: we restrict the set of possible workers to those in the same DC as the BM/CC
	WorkerDetails getBlobWorker(RecruitBlobWorkerRequest const& req);

	std::vector<WorkerDetails> getWorkersForSeedServers(
	    DatabaseConfiguration const& conf,
	    Reference<IReplicationPolicy> const& policy,
	    Optional<Optional<Standalone<StringRef>>> const& dcId = Optional<Optional<Standalone<StringRef>>>());

	// Adds workers to the result such that each field is used in the result set as evenly as possible,
	// with a secondary criteria of minimizing the reuse of zoneIds
	// only add workers which have a field which is already in the result set
	void addWorkersByLowestField(StringRef field,
	                             int desired,
	                             const std::vector<WorkerDetails>& workers,
	                             std::set<WorkerDetails>& resultSet);

	// Adds workers to the result which minimize the reuse of zoneIds
	void addWorkersByLowestZone(int desired,
	                            const std::vector<WorkerDetails>& workers,
	                            std::set<WorkerDetails>& resultSet);

	// Log the reason why the worker is considered as unavailable.
	void logWorkerUnavailable(const Severity severity,
	                          const UID& id,
	                          const std::string& method,
	                          const std::string& reason,
	                          const WorkerDetails& details,
	                          const ProcessClass::Fitness& fitness,
	                          const std::set<Optional<Key>>& dcIds);

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
	                                                     const std::vector<UID>& exclusionWorkerIds);

	// Attempt to recruit TLogs without degraded processes and see if it improves the configuration
	std::vector<WorkerDetails> getWorkersForTlogsComplex(DatabaseConfiguration const& conf,
	                                                     int32_t desired,
	                                                     std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                     StringRef field,
	                                                     int minFields,
	                                                     int minPerField,
	                                                     bool checkStable,
	                                                     const std::set<Optional<Key>>& dcIds,
	                                                     const std::vector<UID>& exclusionWorkerIds);

	// A TLog recruitment method specialized for single, double, and triple configurations
	// It recruits processes from with unique zoneIds until it reaches the desired amount
	std::vector<WorkerDetails> getWorkersForTlogsSimple(DatabaseConfiguration const& conf,
	                                                    int32_t required,
	                                                    int32_t desired,
	                                                    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                    bool checkStable,
	                                                    const std::set<Optional<Key>>& dcIds,
	                                                    const std::vector<UID>& exclusionWorkerIds);

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
	    const std::vector<UID>& exclusionWorkerIds = {});

	// Selects the best method for TLog recruitment based on the specified policy
	std::vector<WorkerDetails> getWorkersForTlogs(DatabaseConfiguration const& conf,
	                                              int32_t required,
	                                              int32_t desired,
	                                              Reference<IReplicationPolicy> const& policy,
	                                              std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                              bool checkStable = false,
	                                              const std::set<Optional<Key>>& dcIds = std::set<Optional<Key>>(),
	                                              const std::vector<UID>& exclusionWorkerIds = {});

	// FIXME: This logic will fallback unnecessarily when usable dcs > 1 because it does not check all combinations of
	// potential satellite locations
	std::vector<WorkerDetails> getWorkersForSatelliteLogs(const DatabaseConfiguration& conf,
	                                                      const RegionInfo& region,
	                                                      const RegionInfo& remoteRegion,
	                                                      std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                                      bool& satelliteFallback,
	                                                      bool checkStable = false);

	ProcessClass::Fitness getBestFitnessForRoleInDatacenter(ProcessClass::ClusterRole role);
	WorkerFitnessInfo getWorkerForRoleInDatacenter(Optional<Standalone<StringRef>> const& dcId,
	                                               ProcessClass::ClusterRole role,
	                                               ProcessClass::Fitness unacceptableFitness,
	                                               DatabaseConfiguration const& conf,
	                                               std::map<Optional<Standalone<StringRef>>, int>& id_used,
	                                               std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	                                               bool checkStable = false);
	std::vector<WorkerDetails> getWorkersForRoleInDatacenter(
	    Optional<Standalone<StringRef>> const& dcId,
	    ProcessClass::ClusterRole role,
	    int amount,
	    DatabaseConfiguration const& conf,
	    std::map<Optional<Standalone<StringRef>>, int>& id_used,
	    std::map<Optional<Standalone<StringRef>>, int> preferredSharing = {},
	    Optional<WorkerFitnessInfo> minWorker = Optional<WorkerFitnessInfo>(),
	    bool checkStable = false);

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

		RoleFitness(const std::vector<WorkerDetails>& workers,
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
	                  std::map<Optional<Standalone<StringRef>>, int>& id_used) {
		for (auto& it : workers) {
			id_used[it.locality.processId()]++;
		}
	}

	void compareWorkers(const DatabaseConfiguration& conf,
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
	bool transactionSystemContainsDegradedServers() {
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
	bool remoteTransactionSystemContainsDegradedServers() {
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

	bool isExcludedDegradedServer(const NetworkAddressList& a) const {
		for (const auto& server : excludedDegradedServers) {
			if (a.contains(server))
				return true;
		}
		return false;
	}

	Future<Optional<Value>> getPreviousCoordinators();
	Future<Void> clusterWatchDatabase(ClusterControllerDBInfo* db, ServerCoordinators coordinators, Future<Void> recoveredDiskFiles);
	void checkOutstandingRecruitmentRequests();
	void checkOutstandingRemoteRecruitmentRequests();
	void checkOutstandingStorageRequests();
	void checkOutstandingBlobWorkerRequests();

	// Finds and returns a new process for role
	WorkerDetails findNewProcessForSingleton(ProcessClass::ClusterRole,
	                                         std::map<Optional<Standalone<StringRef>>, int>& id_used);
	// Return best possible fitness for singleton. Note that lower fitness is better.
	ProcessClass::Fitness findBestFitnessForSingleton(const WorkerDetails&, const ProcessClass::ClusterRole&);

	void checkBetterSingletons();
	Future<Void> doCheckOutstandingRequests();
	Future<Void> doCheckOutstandingRemoteRequests();
	void checkOutstandingRequests();
	Future<Void> workerAvailabilityWatch(WorkerInterface worker, ProcessClass startingClass);
	void clusterRecruitStorage(RecruitStorageRequest);
	void clusterRecruitBlobWorker(RecruitBlobWorkerRequest);
	void clusterRegisterMaster(RegisterMasterRequest const&);
	Future<Void> registerWorker(RegisterWorkerRequest, ClusterConnectionString, class ConfigBroadcaster*);
	Future<Void> timeKeeperSetVersion();

	// This actor periodically gets read version and writes it to cluster with current timestamp as key. To avoid
	// running out of space, it limits the max number of entries and clears old entries on each update. This mapping is
	// used from backup and restore to get the version information for a timestamp.
	Future<Void> timeKeeper();

	Future<Void> statusServer(FutureStream<StatusRequest> requests,
	                          ServerCoordinators coordinators,
	                          ConfigBroadcaster const* configBroadcaster);
	Future<Void> monitorProcessClasses();
	Future<Void> updatedChangingDatacenters();
	Future<Void> updatedChangedDatacenters();
	Future<Void> updateDatacenterVersionDifference();

	// A background actor that periodically checks remote DC health, and `checkOutstandingRequests` if remote DC
	// recovers.
	Future<Void> updateRemoteDCHealth();

	Future<Void> handleForcedRecoveries(ClusterControllerFullInterface);
	Future<Void> triggerAuditStorage(TriggerAuditRequest req);
	Future<Void> handleTriggerAuditStorage(ClusterControllerFullInterface);
	Future<Void> startDataDistributor(double waitTime);
	Future<Void> monitorDataDistributor();
	Future<Void> startRatekeeper(double waitTime);
	Future<Void> monitorRatekeeper();
	Future<Void> startConsistencyScan();
	Future<Void> monitorConsistencyScan();
	Future<Void> startEncryptKeyProxy(double waitTime);
	Future<Void> monitorEncryptKeyProxy();
	Future<int64_t> getNextBMEpoch();
	Future<Void> watchBlobRestoreCommand();
	Future<Void> startBlobMigrator(double waitTime);
	Future<Void> monitorBlobMigrator();
	Future<Void> startBlobManager(double waitTime);
	Future<Void> monitorBlobManager();
	Future<Void> watchBlobGranulesConfigKey();
	Future<Void> dbInfoUpdater();
	Future<Void> workerHealthMonitor();
	Future<Void> metaclusterMetricsUpdater();
	Future<Void> updateClusterId();
	Future<Void> handleGetEncryptionAtRestMode(ClusterControllerFullInterface ccInterf);
	Future<Void> recruitNewMaster(ClusterControllerDBInfo* db, MasterInterface* newMaster);
	Future<Void> clusterRecruitFromConfiguration(Reference<RecruitWorkersInfo> req);
	Future<RecruitRemoteFromConfigurationReply> clusterRecruitRemoteFromConfiguration(
	    Reference<RecruitRemoteWorkersInfo> req);

	static Future<Void> run(ClusterControllerFullInterface interf,
	                        Future<Void> leaderFail,
	                        ServerCoordinators coordinators,
	                        LocalityData locality,
	                        ConfigDBType configDBType,
	                        Future<Void> recoveredDiskFiles,
	                        Reference<AsyncVar<Optional<UID>>> clusterId);

	// Halts the registering (i.e. requesting) singleton if one is already in the process of being recruited
	// or, halts the existing singleton in favour of the requesting one
	template <class SingletonClass>
	void haltRegisteringOrCurrentSingleton(const WorkerInterface& worker,
	                                       const SingletonClass& currSingleton,
	                                       const SingletonClass& registeringSingleton,
	                                       const Optional<UID> recruitingID) {
		ASSERT(currSingleton.getRole() == registeringSingleton.getRole());
		const UID registeringID = registeringSingleton.getInterface().id();
		const std::string roleName = currSingleton.getRole().roleName;
		const std::string roleAbbr = currSingleton.getRole().abbreviation;

		// halt the requesting singleton if it isn't the one currently being recruited
		if ((recruitingID.present() && recruitingID.get() != registeringID) ||
		    clusterControllerDcId != worker.locality.dcId()) {
			TraceEvent(("CCHaltRegistering" + roleName).c_str(), id)
			    .detail(roleAbbr + "ID", registeringID)
			    .detail("DcID", printable(clusterControllerDcId))
			    .detail("ReqDcID", printable(worker.locality.dcId()))
			    .detail("Recruiting" + roleAbbr + "ID", recruitingID.present() ? recruitingID.get() : UID());
			registeringSingleton.halt(*this, worker.locality.processId());
		} else if (!recruitingID.present()) {
			// if not currently recruiting, then halt previous one in favour of requesting one
			TraceEvent(("CCRegister" + roleName).c_str(), id).detail(roleAbbr + "ID", registeringID);
			if (currSingleton.isPresent() && currSingleton.getInterface().id() != registeringID &&
			    id_worker.count(currSingleton.getInterface().locality.processId())) {
				TraceEvent(("CCHaltPrevious" + roleName).c_str(), id)
				    .detail(roleAbbr + "ID", currSingleton.getInterface().id())
				    .detail("DcID", printable(clusterControllerDcId))
				    .detail("ReqDcID", printable(worker.locality.dcId()))
				    .detail("Recruiting" + roleAbbr + "ID", recruitingID.present() ? recruitingID.get() : UID());
				currSingleton.halt(*this, currSingleton.getInterface().locality.processId());
			}
			// set the curr singleton if it doesn't exist or its different from the requesting one
			if (!currSingleton.isPresent() || currSingleton.getInterface().id() != registeringID) {
				registeringSingleton.setInterfaceToDbInfo(*this);
			}
		}
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

	ClusterControllerDBInfo db;
	Database cx;
	double startTime;
	Future<Void> goodRecruitmentTime;
	Future<Void> goodRemoteRecruitmentTime;
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

	ClusterController(ClusterControllerFullInterface const& ccInterface,
	                  LocalityData const& locality,
	                  ServerCoordinators const& coordinators,
	                  Reference<AsyncVar<Optional<UID>>> clusterId)
	  : gotProcessClasses(false), gotFullyRecoveredConfig(false), shouldCommitSuicide(false),
	    clusterControllerProcessId(locality.processId()), clusterControllerDcId(locality.dcId()), id(ccInterface.id()),
	    clusterId(clusterId), ac(false), outstandingRequestChecker(Void()), outstandingRemoteRequestChecker(Void()),
	    startTime(now()), goodRecruitmentTime(Never()), goodRemoteRecruitmentTime(Never()),
	    datacenterVersionDifference(0), versionDifferenceUpdated(false), remoteDCMonitorStarted(false),
	    remoteTransactionSystemDegraded(false), recruitDistributor(false), recruitRatekeeper(false),
	    recruitBlobManager(false), recruitBlobMigrator(false), recruitEncryptKeyProxy(false),
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
		serverInfo.infoGeneration = db.incrementAndGetDbInfoCount();
		serverInfo.masterLifetime.ccID = id;
		serverInfo.clusterInterface = ccInterface;
		serverInfo.myLocality = locality;
		serverInfo.client.isEncryptionEnabled = SERVER_KNOBS->ENABLE_ENCRYPTION;
		db.serverInfo->set(serverInfo);
		cx = openDBOnServer(db.serverInfo, TaskPriority::DefaultEndpoint, LockAware::True);

		specialCounter(clusterControllerMetrics, "ClientCount", [this]() { return db.getClientCount(); });
	}

	~ClusterController() {
		ac.clear(false);
		id_worker.clear();
	}
};

#include "flow/unactorcompiler.h"

#endif
