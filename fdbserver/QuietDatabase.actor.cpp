/*
 * QuietDatabase.actor.cpp
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

#include <cinttypes>
#include <vector>
#include <map>

#include <boost/lexical_cast.hpp>
#include <fmt/ranges.h>

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "flow/flow.h"
#include "flow/ProcessEvents.h"
#include "flow/Trace.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<std::vector<WorkerDetails>> getWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo, int flags = 0) {
	loop {
		choose {
			when(std::vector<WorkerDetails> w = wait(brokenPromiseToNever(
			         dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest(flags))))) {
				return w;
			}
			when(wait(dbInfo->onChange())) {}
		}
	}
}

// Gets the WorkerInterface representing the Master server.
ACTOR Future<WorkerInterface> getMasterWorker(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("GetMasterWorker").detail("Stage", "GettingWorkers");

	loop {
		state std::vector<WorkerDetails> workers = wait(getWorkers(dbInfo));

		for (int i = 0; i < workers.size(); i++) {
			if (workers[i].interf.address() == dbInfo->get().master.address()) {
				TraceEvent("GetMasterWorker")
				    .detail("Stage", "GotWorkers")
				    .detail("MasterId", dbInfo->get().master.id())
				    .detail("WorkerId", workers[i].interf.id());
				return workers[i].interf;
			}
		}

		TraceEvent(SevWarn, "GetMasterWorkerError")
		    .detail("Error", "MasterWorkerNotFound")
		    .detail("Master", dbInfo->get().master.id())
		    .detail("MasterAddress", dbInfo->get().master.address())
		    .detail("WorkerCount", workers.size());

		wait(delay(1.0));
	}
}

// Gets the WorkerInterface representing the data distributor.
ACTOR Future<WorkerInterface> getDataDistributorWorker(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("GetDataDistributorWorker").detail("Stage", "GettingWorkers");

	loop {
		state std::vector<WorkerDetails> workers = wait(getWorkers(dbInfo));
		if (!dbInfo->get().distributor.present())
			continue;

		for (int i = 0; i < workers.size(); i++) {
			if (workers[i].interf.address() == dbInfo->get().distributor.get().address()) {
				TraceEvent("GetDataDistributorWorker")
				    .detail("Stage", "GotWorkers")
				    .detail("DataDistributorId", dbInfo->get().distributor.get().id())
				    .detail("WorkerId", workers[i].interf.id());
				return workers[i].interf;
			}
		}

		TraceEvent(SevWarn, "GetDataDistributorWorker")
		    .detail("Error", "DataDistributorWorkerNotFound")
		    .detail("DataDistributorId", dbInfo->get().distributor.get().id())
		    .detail("DataDistributorAddress", dbInfo->get().distributor.get().address())
		    .detail("WorkerCount", workers.size());
	}
}

// Gets the number of bytes in flight from the data distributor.
ACTOR Future<int64_t> getDataInFlight(Database cx, WorkerInterface distributorWorker) {
	try {
		TraceEvent("DataInFlight").detail("Stage", "ContactingDataDistributor");
		TraceEventFields md = wait(
		    timeoutError(distributorWorker.eventLogRequest.getReply(EventLogRequest("TotalDataInFlight"_sr)), 1.0));
		int64_t dataInFlight = boost::lexical_cast<int64_t>(md.getValue("TotalBytes"));
		return dataInFlight;
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .error(e)
		    .detail("Reason", "Failed to extract DataInFlight");
		throw;
	}
}

// Gets the number of bytes in flight from the data distributor.
ACTOR Future<int64_t> getDataInFlight(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	WorkerInterface distributorInterf = wait(getDataDistributorWorker(cx, dbInfo));
	int64_t dataInFlight = wait(getDataInFlight(cx, distributorInterf));
	return dataInFlight;
}

// Computes the queue size for storage servers and tlogs using the bytesInput and bytesDurable attributes
int64_t getQueueSize(const TraceEventFields& md) {
	double inputRate, durableRate;
	double inputRoughness, durableRoughness;
	int64_t inputBytes, durableBytes;

	sscanf(md.getValue("BytesInput").c_str(), "%lf %lf %" SCNd64, &inputRate, &inputRoughness, &inputBytes);
	sscanf(md.getValue("BytesDurable").c_str(), "%lf %lf %" SCNd64, &durableRate, &durableRoughness, &durableBytes);

	return inputBytes - durableBytes;
}

int64_t getDurableVersion(const TraceEventFields& md) {
	return boost::lexical_cast<int64_t>(md.getValue("DurableVersion"));
}

// Computes the popped version lag for tlogs
int64_t getPoppedVersionLag(const TraceEventFields& md) {
	int64_t persistentDataDurableVersion = boost::lexical_cast<int64_t>(md.getValue("PersistentDataDurableVersion"));
	int64_t queuePoppedVersion = boost::lexical_cast<int64_t>(md.getValue("QueuePoppedVersion"));

	return persistentDataDurableVersion - queuePoppedVersion;
}

ACTOR Future<std::vector<WorkerInterface>> getCoordWorkers(Database cx,
                                                           Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state std::vector<WorkerDetails> workers = wait(getWorkers(dbInfo));

	Optional<Value> coordinators =
	    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
		    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		    tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		    return tr->get(coordinatorsKey);
	    }));
	if (!coordinators.present()) {
		throw operation_failed();
	}
	ClusterConnectionString ccs(coordinators.get().toString());
	std::vector<NetworkAddress> coordinatorsAddr = wait(ccs.tryResolveHostnames());
	std::set<NetworkAddress> coordinatorsAddrSet;
	for (const auto& addr : coordinatorsAddr) {
		TraceEvent(SevDebug, "CoordinatorAddress").detail("Addr", addr);
		coordinatorsAddrSet.insert(addr);
	}

	std::vector<WorkerInterface> result;
	for (const auto& worker : workers) {
		NetworkAddress primary = worker.interf.address();
		Optional<NetworkAddress> secondary = worker.interf.tLog.getEndpoint().addresses.secondaryAddress;
		if (coordinatorsAddrSet.find(primary) != coordinatorsAddrSet.end() ||
		    (secondary.present() && (coordinatorsAddrSet.find(secondary.get()) != coordinatorsAddrSet.end()))) {
			result.push_back(worker.interf);
		}
	}
	return result;
}

// This is not robust in the face of a TLog failure
ACTOR Future<std::pair<int64_t, int64_t>> getTLogQueueInfo(Database cx,
                                                           Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("MaxTLogQueueSize").detail("Stage", "ContactingLogs");

	state std::vector<WorkerDetails> workers = wait(getWorkers(dbInfo));
	std::map<NetworkAddress, WorkerInterface> workersMap;
	for (auto worker : workers) {
		workersMap[worker.interf.address()] = worker.interf;
	}

	state std::vector<Future<TraceEventFields>> messages;
	state std::vector<TLogInterface> tlogs = dbInfo->get().logSystemConfig.allPresentLogs();
	for (int i = 0; i < tlogs.size(); i++) {
		auto itr = workersMap.find(tlogs[i].address());
		if (itr == workersMap.end()) {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Could not find worker for log server")
			    .detail("Tlog", tlogs[i].id());
			throw attribute_not_found();
		}
		messages.push_back(timeoutError(
		    itr->second.eventLogRequest.getReply(EventLogRequest(StringRef(tlogs[i].id().toString() + "/TLogMetrics"))),
		    1.0));
	}
	wait(waitForAll(messages));

	TraceEvent("MaxTLogQueueSize").detail("Stage", "ComputingMax").detail("MessageCount", messages.size());

	state int64_t maxQueueSize = 0;
	state int64_t maxPoppedVersionLag = 0;
	state int i = 0;
	for (; i < messages.size(); i++) {
		try {
			maxQueueSize = std::max(maxQueueSize, getQueueSize(messages[i].get()));
			maxPoppedVersionLag = std::max(maxPoppedVersionLag, getPoppedVersionLag(messages[i].get()));
		} catch (Error& e) {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Failed to extract MaxTLogQueue")
			    .detail("Tlog", tlogs[i].id());
			throw;
		}
	}

	return std::make_pair(maxQueueSize, maxPoppedVersionLag);
}

// Returns a vector of blob worker interfaces which have been persisted under the system key space
ACTOR Future<std::vector<BlobWorkerInterface>> getBlobWorkers(Database cx,
                                                              bool use_system_priority = false,
                                                              Version* grv = nullptr) {
	state Transaction tr(cx);
	loop {
		if (use_system_priority) {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			RangeResult blobWorkersList = wait(tr.getRange(blobWorkerListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!blobWorkersList.more && blobWorkersList.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<BlobWorkerInterface> blobWorkers;
			blobWorkers.reserve(blobWorkersList.size());
			for (int i = 0; i < blobWorkersList.size(); i++) {
				blobWorkers.push_back(decodeBlobWorkerListValue(blobWorkersList[i].value));
			}
			if (grv) {
				*grv = tr.getReadVersion().get();
			}
			return blobWorkers;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<std::pair<UID, UID>>> getBlobWorkerAffinity(Database cx,
                                                                     bool use_system_priority = false,
                                                                     Version* grv = nullptr) {
	state Transaction tr(cx);
	loop {
		if (use_system_priority) {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			RangeResult blobWorkerAffinity = wait(tr.getRange(blobWorkerAffinityKeys, CLIENT_KNOBS->TOO_MANY));

			std::vector<std::pair<UID, UID>> affinities;
			affinities.reserve(blobWorkerAffinity.size());
			for (int i = 0; i < blobWorkerAffinity.size(); i++) {
				affinities.push_back(std::make_pair(decodeBlobWorkerAffinityKey(blobWorkerAffinity[i].key),
				                                    decodeBlobWorkerAffinityValue(blobWorkerAffinity[i].value)));
			}
			if (grv) {
				*grv = tr.getReadVersion().get();
			}
			return affinities;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::vector<StorageServerInterface>> getStorageServers(Database cx, bool use_system_priority = false) {
	state Transaction tr(cx);
	loop {
		if (use_system_priority) {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<StorageServerInterface> servers;
			servers.reserve(serverList.size());
			for (int i = 0; i < serverList.size(); i++)
				servers.push_back(decodeServerListValue(serverList[i].value));
			return servers;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<std::pair<std::vector<WorkerInterface>, int>>
getStorageWorkers(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo, bool localOnly) {
	state std::vector<StorageServerInterface> servers = wait(getStorageServers(cx));
	state std::map<NetworkAddress, WorkerInterface> workersMap;
	std::vector<WorkerDetails> workers = wait(getWorkers(dbInfo));

	for (const auto& worker : workers) {
		workersMap[worker.interf.address()] = worker.interf;
	}
	Optional<Value> regionsValue =
	    wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
		    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		    tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		    return tr->get("usable_regions"_sr.withPrefix(configKeysPrefix));
	    }));
	int usableRegions = 1;
	if (regionsValue.present()) {
		usableRegions = atoi(regionsValue.get().toString().c_str());
	}
	auto masterDcId = dbInfo->get().master.locality.dcId();

	std::pair<std::vector<WorkerInterface>, int> result;
	auto& [workerInterfaces, failures] = result;
	failures = 0;
	for (const auto& server : servers) {
		TraceEvent(SevDebug, "DcIdInfo")
		    .detail("ServerLocalityID", server.locality.dcId())
		    .detail("MasterDcID", masterDcId);
		if (!localOnly || (usableRegions == 1 || server.locality.dcId() == masterDcId)) {
			auto itr = workersMap.find(server.address());
			if (itr == workersMap.end()) {
				TraceEvent(SevWarn, "GetStorageWorkers")
				    .detail("Reason", "Could not find worker for storage server")
				    .detail("SS", server.id());
				++failures;
			} else {
				workerInterfaces.push_back(itr->second);
			}
		}
	}
	return result;
}

// Helper function to extract he maximum SQ size of all provided messages. All futures in the
// messages vector have to be ready.
int64_t extractMaxQueueSize(const std::vector<Future<TraceEventFields>>& messages,
                            const std::vector<StorageServerInterface>& servers) {

	int64_t maxQueueSize = 0;
	UID maxQueueServer;

	for (int i = 0; i < messages.size(); i++) {
		try {
			auto queueSize = getQueueSize(messages[i].get());
			if (queueSize > maxQueueSize) {
				maxQueueSize = queueSize;
				maxQueueServer = servers[i].id();
			}
		} catch (Error& e) {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Failed to extract MaxStorageServerQueue")
			    .detail("SS", servers[i].id());
			for (auto& m : messages) {
				TraceEvent("Messages").detail("Info", m.get().toString());
			}
			throw;
		}
	}

	TraceEvent("QuietDatabaseGotMaxStorageServerQueueSize")
	    .detail("Stage", "MaxComputed")
	    .detail("Max", maxQueueSize)
	    .detail("MaxQueueServer", maxQueueServer);
	return maxQueueSize;
}

// Timeout wrapper when getting the storage metrics. This will do some additional tracing
ACTOR Future<TraceEventFields> getStorageMetricsTimeout(UID storage, WorkerInterface wi, Version version) {
	state int retries = 0;
	loop {
		++retries;
		state Future<TraceEventFields> eventLogReply =
		    wi.eventLogRequest.getReply(EventLogRequest(StringRef(storage.toString() + "/StorageMetrics")));
		state Future<Void> timeout = delay(30.0);
		state TraceEventFields storageMetrics;
		choose {
			when(wait(store(storageMetrics, eventLogReply))) {
				try {
					if (version == invalidVersion ||
					    getDurableVersion(storageMetrics) >= static_cast<int64_t>(version)) {
						return storageMetrics;
					}
				} catch (Error& e) {
					TraceEvent("QuietDatabaseFailure")
					    .error(e)
					    .detail("Reason", "Failed to extract DurableVersion from StorageMetrics")
					    .detail("SSID", storage)
					    .detail("StorageMetrics", storageMetrics.toString());
					throw;
				}
			}
			when(wait(timeout)) {
				TraceEvent("QuietDatabaseFailure")
				    .detail("Reason", "Could not fetch StorageMetrics")
				    .detail("Storage", storage);
				throw timed_out();
			}
		}
		if (retries > 30) {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Could not fetch StorageMetrics x30")
			    .detail("Storage", storage)
			    .detail("Version", version);
			throw timed_out();
		}
		wait(delay(1.0));
	}
}

// Gets the maximum size of all the storage server queues
ACTOR Future<int64_t> getMaxStorageServerQueueSize(Database cx,
                                                   Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                                   Version version) {

	TraceEvent("MaxStorageServerQueueSize").detail("Stage", "ContactingStorageServers");

	Future<std::vector<StorageServerInterface>> serversFuture = getStorageServers(cx);
	state Future<std::vector<WorkerDetails>> workersFuture = getWorkers(dbInfo);

	state std::vector<StorageServerInterface> servers = wait(serversFuture);
	state std::vector<WorkerDetails> workers = wait(workersFuture);

	std::map<NetworkAddress, WorkerInterface> workersMap;
	for (auto worker : workers) {
		workersMap[worker.interf.address()] = worker.interf;
	}

	state std::vector<Future<TraceEventFields>> messages;
	for (int i = 0; i < servers.size(); i++) {
		auto itr = workersMap.find(servers[i].address());
		if (itr == workersMap.end()) {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Could not find worker for storage server")
			    .detail("SS", servers[i].id());
			throw attribute_not_found();
		}
		messages.push_back(getStorageMetricsTimeout(servers[i].id(), itr->second, version));
	}

	wait(waitForAll(messages));

	TraceEvent("MaxStorageServerQueueSize").detail("Stage", "ComputingMax").detail("MessageCount", messages.size());

	return extractMaxQueueSize(messages, servers);
}

// Gets the size of the data distribution queue.  If reportInFlight is true, then data in flight is considered part of
// the queue
ACTOR Future<int64_t> getDataDistributionQueueSize(Database cx,
                                                   WorkerInterface distributorWorker,
                                                   bool reportInFlight) {
	try {
		TraceEvent("DataDistributionQueueSize").detail("Stage", "ContactingDataDistributor");

		TraceEventFields movingDataMessage =
		    wait(timeoutError(distributorWorker.eventLogRequest.getReply(EventLogRequest("MovingData"_sr)), 1.0));

		TraceEvent("DataDistributionQueueSize").detail("Stage", "GotString");

		int64_t inQueue = boost::lexical_cast<int64_t>(movingDataMessage.getValue("InQueue"));

		if (reportInFlight) {
			int64_t inFlight = boost::lexical_cast<int64_t>(movingDataMessage.getValue("InFlight"));
			inQueue += inFlight;
		}

		return inQueue;
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .detail("Reason", "Failed to extract DataDistributionQueueSize");
		throw;
	}
}

// Gets the size of the data distribution queue.  If reportInFlight is true, then data in flight is considered part of
// the queue Convenience method that first finds the master worker from a zookeeper interface
ACTOR Future<int64_t> getDataDistributionQueueSize(Database cx,
                                                   Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                                   bool reportInFlight) {
	WorkerInterface distributorInterf = wait(getDataDistributorWorker(cx, dbInfo));
	int64_t inQueue = wait(getDataDistributionQueueSize(cx, distributorInterf, reportInFlight));
	return inQueue;
}

// Gets if the number of process and machine teams does not exceed the maximum allowed number of teams
ACTOR Future<bool> getTeamCollectionValid(Database cx, WorkerInterface dataDistributorWorker) {
	state int attempts = 0;
	state bool ret = false;
	loop {
		try {
			if (!g_network->isSimulated()) {
				return true;
			}

			TraceEvent("GetTeamCollectionValid").detail("Stage", "ContactingMaster");

			TraceEventFields teamCollectionInfoMessage = wait(timeoutError(
			    dataDistributorWorker.eventLogRequest.getReply(EventLogRequest("TeamCollectionInfo"_sr)), 1.0));

			TraceEvent("GetTeamCollectionValid").detail("Stage", "GotString");

			state int64_t currentTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("CurrentServerTeams"));
			state int64_t desiredTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("DesiredTeams"));
			state int64_t maxTeams = boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxTeams"));
			state int64_t currentMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("CurrentMachineTeams"));
			state int64_t healthyMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("CurrentHealthyMachineTeams"));
			state int64_t desiredMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("DesiredMachineTeams"));
			state int64_t maxMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxMachineTeams"));

			state int64_t minServerTeamsOnServer =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MinTeamsOnServer"));
			state int64_t maxServerTeamsOnServer =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxTeamsOnServer"));
			state int64_t minMachineTeamsOnMachine =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MinMachineTeamsOnMachine"));
			state int64_t maxMachineTeamsOnMachine =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxMachineTeamsOnMachine"));

			// The if condition should be consistent with the condition in serverTeamRemover() and
			// machineTeamRemover() that decides if redundant teams exist.
			// Team number is always valid when we disable teamRemover, which avoids false positive in simulation test.
			// The minimum team number per server (and per machine) should be no less than 0 so that newly added machine
			// can host data on it.
			//
			// If the machineTeamRemover does not remove the machine team with the most machine teams,
			// we may oscillate between building more server teams by teamBuilder() and removing those teams by
			// teamRemover To avoid false positive in simulation, we skip the consistency check in this case.
			// This is a corner case. This is a work-around if case the team number requirements cannot be satisfied.
			//
			// The checking for too many teams is disabled because teamRemover may not remove a team if it leads to 0
			// team on a server
			//(!SERVER_KNOBS->TR_FLAG_DISABLE_MACHINE_TEAM_REMOVER &&
			//  healthyMachineTeams > desiredMachineTeams) ||
			// (!SERVER_KNOBS->TR_FLAG_DISABLE_SERVER_TEAM_REMOVER && currentTeams > desiredTeams) ||
			if ((minMachineTeamsOnMachine <= 0 || minServerTeamsOnServer <= 0) &&
			    SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS) {
				ret = false;

				if (attempts++ < 10) {
					wait(delay(60));
					continue; // We may not receive the most recent TeamCollectionInfo
				}

				// When DESIRED_TEAMS_PER_SERVER == 1, we see minMachineTeamOnMachine can be 0 in one out of 30k test
				// cases. Only check DESIRED_TEAMS_PER_SERVER == 3 for now since it is mostly used configuration.
				// TODO: Remove the constraint SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER == 3 to ensure that
				// the minimum team number per server (and per machine) is always > 0 for any number of replicas
				TraceEvent("GetTeamCollectionValid")
				    .detail("CurrentServerTeams", currentTeams)
				    .detail("DesiredTeams", desiredTeams)
				    .detail("MaxTeams", maxTeams)
				    .detail("CurrentHealthyMachineTeams", healthyMachineTeams)
				    .detail("DesiredMachineTeams", desiredMachineTeams)
				    .detail("CurrentMachineTeams", currentMachineTeams)
				    .detail("MaxMachineTeams", maxMachineTeams)
				    .detail("MinTeamsOnServer", minServerTeamsOnServer)
				    .detail("MaxTeamsOnServer", maxServerTeamsOnServer)
				    .detail("MinMachineTeamsOnMachine", minMachineTeamsOnMachine)
				    .detail("MaxMachineTeamsOnMachine", maxMachineTeamsOnMachine)
				    .detail("DesiredTeamsPerServer", SERVER_KNOBS->DESIRED_TEAMS_PER_SERVER)
				    .detail("MaxTeamsPerServer", SERVER_KNOBS->MAX_TEAMS_PER_SERVER)
				    .detail("RemoveMTWithMostTeams", SERVER_KNOBS->TR_FLAG_REMOVE_MT_WITH_MOST_TEAMS);
				return ret;
			} else {
				return true;
			}

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			TraceEvent("QuietDatabaseFailure", dataDistributorWorker.id())
			    .detail("Reason", "Failed to extract GetTeamCollectionValid information");
			attempts++;
			if (attempts > 10) {
				TraceEvent("QuietDatabaseNoTeamCollectionInfo", dataDistributorWorker.id())
				    .detail("Reason", "Had never called build team to build any team");
				return true;
			}
			// throw;
			wait(delay(10.0));
		}
	};
}

// Gets if the number of process and machine teams does not exceed the maximum allowed number of teams
// Convenience method that first finds the master worker from a zookeeper interface
ACTOR Future<bool> getTeamCollectionValid(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	WorkerInterface dataDistributorWorker = wait(getDataDistributorWorker(cx, dbInfo));
	bool valid = wait(getTeamCollectionValid(cx, dataDistributorWorker));
	return valid;
}

// Checks that data distribution is active
ACTOR Future<bool> getDataDistributionActive(Database cx, WorkerInterface distributorWorker) {
	try {
		TraceEvent("DataDistributionActive").detail("Stage", "ContactingDataDistributor");

		TraceEventFields activeMessage = wait(
		    timeoutError(distributorWorker.eventLogRequest.getReply(EventLogRequest("DDTrackerStarting"_sr)), 1.0));

		return activeMessage.getValue("State") == "Active";
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .detail("Reason", "Failed to extract DataDistributionActive");
		throw;
	}
}

// Checks to see if any storage servers are being recruited
ACTOR Future<bool> getStorageServersRecruiting(Database cx, WorkerInterface distributorWorker, UID distributorUID) {
	try {
		TraceEvent("StorageServersRecruiting").detail("Stage", "ContactingDataDistributor");
		TraceEventFields recruitingMessage =
		    wait(timeoutError(distributorWorker.eventLogRequest.getReply(
		                          EventLogRequest(StringRef("StorageServerRecruitment_" + distributorUID.toString()))),
		                      1.0));

		TraceEvent("StorageServersRecruiting").detail("Message", recruitingMessage.toString());

		if (recruitingMessage.getValue("State") == "Recruiting") {
			std::string tssValue;
			// if we're tss recruiting, that's fine because that can block indefinitely if only 1 free storage process
			if (!recruitingMessage.tryGetValue("IsTSS", tssValue) || tssValue == "False") {
				return true;
			}
		}
		return false;
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .detail("Reason", "Failed to extract StorageServersRecruiting")
		    .detail("DataDistributorID", distributorUID);
		throw;
	}
}

// Gets the difference between the expected version (based on the version
// epoch) and the actual version.
ACTOR Future<int64_t> getVersionOffset(Database cx,
                                       WorkerInterface distributorWorker,
                                       Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	loop {
		state Transaction tr(cx);
		try {
			TraceEvent("GetVersionOffset").detail("Stage", "ReadingVersionEpoch");

			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			state Version rv = wait(tr.getReadVersion());
			Optional<Standalone<StringRef>> versionEpochValue = wait(tr.get(versionEpochKey));
			if (!versionEpochValue.present()) {
				return 0;
			}
			int64_t versionEpoch = BinaryReader::fromStringRef<int64_t>(versionEpochValue.get(), Unversioned());
			int64_t versionOffset = abs(rv - (g_network->timer() * SERVER_KNOBS->VERSIONS_PER_SECOND - versionEpoch));
			return versionOffset;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Returns DC lag for simulation runs
ACTOR Future<Version> getDatacenterLag(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	loop {
		if (!g_network->isSimulated() || g_simulator->usableRegions == 1) {
			return 0;
		}

		state Optional<TLogInterface> primaryLog;
		state Optional<TLogInterface> remoteLog;
		if (dbInfo->get().recoveryState >= RecoveryState::ALL_LOGS_RECRUITED) {
			for (const auto& logset : dbInfo->get().logSystemConfig.tLogs) {
				if (logset.isLocal && logset.locality != tagLocalitySatellite) {
					for (const auto& tlog : logset.tLogs) {
						if (tlog.present()) {
							primaryLog = tlog.interf();
							break;
						}
					}
				}
				if (!logset.isLocal) {
					for (const auto& tlog : logset.tLogs) {
						if (tlog.present()) {
							remoteLog = tlog.interf();
							break;
						}
					}
				}
			}
		}

		if (!primaryLog.present() || !remoteLog.present()) {
			wait(dbInfo->onChange());
			continue;
		}

		ASSERT(primaryLog.present());
		ASSERT(remoteLog.present());

		state Future<Void> onChange = dbInfo->onChange();
		loop {
			state Future<TLogQueuingMetricsReply> primaryMetrics =
			    brokenPromiseToNever(primaryLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));
			state Future<TLogQueuingMetricsReply> remoteMetrics =
			    brokenPromiseToNever(remoteLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));

			wait((success(primaryMetrics) && success(remoteMetrics)) || onChange);
			if (onChange.isReady()) {
				break;
			}

			TraceEvent("DCLag").detail("Primary", primaryMetrics.get().v).detail("Remote", remoteMetrics.get().v);
			ASSERT(primaryMetrics.get().v >= 0 && remoteMetrics.get().v >= 0);
			return primaryMetrics.get().v - remoteMetrics.get().v;
		}
	}
}

ACTOR Future<Void> repairDeadDatacenter(Database cx,
                                        Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                        std::string context) {
	if (g_network->isSimulated() && g_simulator->usableRegions > 1 && !g_simulator->quiesced) {
		state bool primaryDead = g_simulator->datacenterDead(g_simulator->primaryDcId);
		state bool remoteDead = g_simulator->datacenterDead(g_simulator->remoteDcId);

		// FIXME: the primary and remote can both be considered dead because excludes are not handled properly by the
		// datacenterDead function
		if (primaryDead && remoteDead) {
			TraceEvent(SevWarnAlways, "CannotDisableFearlessConfiguration").log();
			return Void();
		}
		if (primaryDead || remoteDead) {
			if (remoteDead) {
				std::vector<AddressExclusion> servers =
				    g_simulator->getAllAddressesInDCToExclude(g_simulator->remoteDcId);
				TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration")
				    .detail("Location", context)
				    .detail("Stage", "ExcludeServers")
				    .detail("Servers", describe(servers));
				wait(excludeServers(cx, servers, false));
			}

			TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration")
			    .detail("Location", context)
			    .detail("Stage", "Repopulate")
			    .detail("RemoteDead", remoteDead)
			    .detail("PrimaryDead", primaryDead);
			g_simulator->usableRegions = 1;

			wait(success(ManagementAPI::changeConfig(
			    cx.getReference(),
			    (primaryDead ? g_simulator->disablePrimary : g_simulator->disableRemote) + " repopulate_anti_quorum=1",
			    true)));
			while (dbInfo->get().recoveryState < RecoveryState::STORAGE_RECOVERED) {
				wait(dbInfo->onChange());
			}
			TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration")
			    .detail("Location", context)
			    .detail("Stage", "Usable_Regions");
			wait(success(ManagementAPI::changeConfig(cx.getReference(), "usable_regions=1", true)));
		}
	}
	return Void();
}

ACTOR Future<Void> reconfigureAfter(Database cx,
                                    double time,
                                    Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                    std::string context) {
	wait(delay(time));
	wait(uncancellable(repairDeadDatacenter(cx, dbInfo, context)));
	return Void();
}

struct QuietDatabaseChecker {
	ProcessEvents::Callback timeoutCallback = [this](StringRef name, std::any const& msg, Error const& e) {
		logFailure(name, std::any_cast<StringRef>(msg), e);
	};
	double start = now();
	double maxDDRunTime;
	ProcessEvents::Event timeoutEvent;
	std::vector<std::string> lastFailReasons;

	QuietDatabaseChecker(double maxDDRunTime)
	  : maxDDRunTime(maxDDRunTime), timeoutEvent({ "Timeout"_sr, "TracedTooManyLines"_sr }, timeoutCallback) {}

	void logFailure(StringRef name, StringRef msg, Error const& e) {
		std::string reasons = fmt::format("{}", fmt::join(lastFailReasons, ", "));
		TraceEvent(SevError, "QuietDatabaseFailure")
		    .error(e)
		    .detail("EventName", name)
		    .detail("EventMessage", msg)
		    .detail("Reasons", lastFailReasons)
		    .log();
	};

	struct Impl {
		double start;
		std::string const& phase;
		double maxDDRunTime;
		std::vector<std::string>& failReasons;

		Impl(double start, const std::string& phase, const double maxDDRunTime, std::vector<std::string>& failReasons)
		  : start(start), phase(phase), maxDDRunTime(maxDDRunTime), failReasons(failReasons) {}

		template <class T, class Comparison = std::less_equal<>>
		Impl& add(BaseTraceEvent& evt,
		          const char* name,
		          T value,
		          T expected,
		          Comparison const& cmp = std::less_equal<>()) {
			std::string k = fmt::format("{}Gate", name);
			evt.detail(name, value).detail(k.c_str(), expected);
			if (!cmp(value, expected)) {
				failReasons.push_back(name);
			}
			return *this;
		}

		bool success() {
			bool timedOut = now() - start > maxDDRunTime;
			if (!failReasons.empty()) {
				std::string traceMessage = fmt::format("QuietDatabase{}Fail", phase);
				std::string reasons = fmt::format("{}", fmt::join(failReasons, ", "));
				TraceEvent(timedOut ? SevError : SevWarnAlways, traceMessage.c_str())
				    .detail("Reasons", reasons)
				    .detail("FailedAfter", now() - start)
				    .detail("Timeout", maxDDRunTime);
				if (timedOut) {
					// this bool is just created to make the assertion more readable
					bool ddGotStuck = true;
					// This assertion is here to make the test fail more quickly. If quietDatabase takes this
					// long without completing, we can assume that the test will eventually time out. However,
					// time outs are more annoying to debug. This will hopefully be easier to track down.
					ASSERT(!ddGotStuck || !g_network->isSimulated());
				}
				return false;
			}
			return true;
		}
	};

	Impl startIteration(std::string const& phase) {
		lastFailReasons.clear();
		Impl res(start, phase, maxDDRunTime, lastFailReasons);
		return res;
	}
};

ACTOR Future<Void> enableConsistencyScanInSim(Database db) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	state ConsistencyScanState cs;
	if (!g_network->isSimulated()) {
		return Void();
	}
	TraceEvent("ConsistencyScan_SimEnable").log();
	loop {
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);
			state ConsistencyScanState::Config config = wait(cs.config().getD(tr));

			// Enable if disabled, otherwise sometimes update the scan min version to restart it
			if (!config.enabled && g_simulator->consistencyScanState < ISimulator::SimConsistencyScanState::Enabled) {
				if (!g_simulator->doInjectConsistencyScanCorruption.present()) {
					g_simulator->doInjectConsistencyScanCorruption = BUGGIFY_WITH_PROB(0.1);
					TraceEvent("ConsistencyScan_DoInjectCorruption")
					    .detail("Val", g_simulator->doInjectConsistencyScanCorruption.get());
				}

				g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::DisabledStart,
				                                        ISimulator::SimConsistencyScanState::Enabling);
				config.enabled = true;
			} else {
				if (config.enabled && g_simulator->restarted &&
				    g_simulator->consistencyScanState == ISimulator::SimConsistencyScanState::DisabledStart) {
					TraceEvent("ConsistencyScan_SimEnableAlreadyDoneFromRestart").log();
					g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::DisabledStart,
					                                        ISimulator::SimConsistencyScanState::Enabling);
				}
				if (BUGGIFY_WITH_PROB(0.5)) {
					config.minStartVersion = tr->getReadVersion().get();
				}
			}
			// also change the rate
			config.maxReadByteRate = deterministicRandom()->randomInt(1, 50e6);
			config.targetRoundTimeSeconds = deterministicRandom()->randomSkewedUInt32(1, 100);
			config.minRoundTimeSeconds = deterministicRandom()->randomSkewedUInt32(1, 100);

			if (config.enabled) {
				cs.config().set(tr, config);
				wait(tr->commit());

				g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::Enabling,
				                                        ISimulator::SimConsistencyScanState::Enabled);
				TraceEvent("ConsistencyScan_SimEnabled")
				    .detail("MaxReadByteRate", config.maxReadByteRate)
				    .detail("TargetRoundTimeSeconds", config.targetRoundTimeSeconds)
				    .detail("MinRoundTimeSeconds", config.minRoundTimeSeconds);
				CODE_PROBE(true, "Consistency Scan enabled in simulation");
			}

			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> disableConsistencyScanInSim(Database db, bool waitForCompletion) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(db);
	state ConsistencyScanState cs;
	if (!g_network->isSimulated()) {
		return Void();
	}

	if (waitForCompletion) {
		TraceEvent("ConsistencyScan_SimDisableWaiting").log();
		printf("Waiting for consistency scan to complete...\n");
		loop {
			bool waitForCorruption = g_simulator->doInjectConsistencyScanCorruption.present() &&
			                         g_simulator->doInjectConsistencyScanCorruption.get();
			if (((waitForCorruption &&
			      g_simulator->consistencyScanState >= ISimulator::SimConsistencyScanState::Enabled_FoundCorruption) ||
			     (!waitForCorruption &&
			      g_simulator->consistencyScanState >= ISimulator::SimConsistencyScanState::Enabled))) {
				break;
			}
			wait(delay(1.0));
		}
	}
	TraceEvent("ConsistencyScan_SimDisable").log();
	loop {
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);
			state ConsistencyScanState::Config config = wait(cs.config().getD(tr));
			state bool skipDisable = false;
			// see if any rounds have completed
			if (waitForCompletion) {
				state ConsistencyScanState::RoundStats statsCurrentRound = wait(cs.currentRoundStats().getD(tr));
				state ConsistencyScanState::StatsHistoryMap::RangeResultType olderStats =
				    wait(cs.roundStatsHistory().getRange(tr, {}, {}, 1, Snapshot::False, Reverse::False));
				if (olderStats.results.empty() && !statsCurrentRound.complete) {
					TraceEvent("ConsistencyScan_SimDisable_NoRoundsCompleted").log();
					skipDisable = true;
				}
			}

			// Enable if disable, else set the scan min version to restart it
			if (config.enabled) {
				// state was either enabled or enabled_foundcorruption
				g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::Enabled,
				                                        ISimulator::SimConsistencyScanState::Complete);
				g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::Enabled_FoundCorruption,
				                                        ISimulator::SimConsistencyScanState::Complete);
				config.enabled = false;
			} else {
				TraceEvent("ConsistencyScan_SimDisableAlreadyDisabled").log();
				printf("Consistency scan already complete.\n");
				return Void();
			}

			if (skipDisable) {
				wait(delay(2.0));
				tr->reset();
			} else {
				cs.config().set(tr, config);
				wait(tr->commit());
				break;
			}
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::Complete,
	                                        ISimulator::SimConsistencyScanState::DisabledEnd);
	CODE_PROBE(true, "Consistency Scan disabled in simulation");
	TraceEvent("ConsistencyScan_SimDisabled").log();
	printf("Consistency scan complete.\n");
	return Void();
}

// Waits until a database quiets down (no data in flight, small tlog queue, low SQ, no active data distribution). This
// requires the database to be available and healthy in order to succeed.
ACTOR Future<Void> waitForQuietDatabase(Database cx,
                                        Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                        std::string phase,
                                        int64_t dataInFlightGate = 2e6,
                                        int64_t maxTLogQueueGate = 5e6,
                                        int64_t maxStorageServerQueueGate = 5e6,
                                        int64_t maxDataDistributionQueueSize = 0,
                                        int64_t maxPoppedVersionLag = 30e6,
                                        int64_t maxVersionOffset = 1e6) {
	state QuietDatabaseChecker checker(isGeneralBuggifyEnabled() ? 4000.0 : 1000.0);
	state Future<Void> reconfig =
	    reconfigureAfter(cx, 100 + (deterministicRandom()->random01() * 100), dbInfo, "QuietDatabase");
	state Future<int64_t> dataInFlight;
	state Future<std::pair<int64_t, int64_t>> tLogQueueInfo;
	state Future<int64_t> dataDistributionQueueSize;
	state Future<bool> teamCollectionValid;
	state Future<int64_t> storageQueueSize;
	state Future<bool> dataDistributionActive;
	state Future<bool> storageServersRecruiting;
	state Future<int64_t> versionOffset;
	state Future<Version> dcLag;
	state Version maxDcLag = 100e6;
	state std::string traceMessage = "QuietDatabase" + phase + "Begin";
	TraceEvent(traceMessage.c_str()).log();

	// In a simulated environment, wait 5 seconds so that workers can move to their optimal locations
	if (g_network->isSimulated())
		wait(delay(5.0));

	TraceEvent("QuietDatabaseWaitingOnFullRecovery").detail("Phase", phase).log();
	while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
		wait(dbInfo->onChange());
	}

	// The quiet database check (which runs at the end of every test) will always time out due to active data movement.
	// To get around this, quiet Database will disable the perpetual wiggle in the setup phase.

	printf("Set perpetual_storage_wiggle=0 ...\n");
	state Version version = wait(setPerpetualStorageWiggle(cx, false, LockAware::True));
	printf("Set perpetual_storage_wiggle=0 Done.\n");

	wait(disableConsistencyScanInSim(cx, false));

	// Require 3 consecutive successful quiet database checks spaced 2 second apart
	state int numSuccesses = 0;
	loop {
		try {
			TraceEvent("QuietDatabaseWaitingOnDataDistributor").log();
			WorkerInterface distributorWorker = wait(getDataDistributorWorker(cx, dbInfo));
			UID distributorUID = dbInfo->get().distributor.get().id();
			TraceEvent("QuietDatabaseGotDataDistributor", distributorUID)
			    .detail("Locality", distributorWorker.locality.toString());

			dataInFlight = getDataInFlight(cx, distributorWorker);
			tLogQueueInfo = getTLogQueueInfo(cx, dbInfo);
			dataDistributionQueueSize = getDataDistributionQueueSize(cx, distributorWorker, dataInFlightGate == 0);
			teamCollectionValid = getTeamCollectionValid(cx, distributorWorker);
			storageQueueSize = getMaxStorageServerQueueSize(cx, dbInfo, version);
			dataDistributionActive = getDataDistributionActive(cx, distributorWorker);
			storageServersRecruiting = getStorageServersRecruiting(cx, distributorWorker, distributorUID);
			versionOffset = getVersionOffset(cx, distributorWorker, dbInfo);
			dcLag = getDatacenterLag(cx, dbInfo);

			wait(success(dataInFlight) && success(tLogQueueInfo) && success(dataDistributionQueueSize) &&
			     success(teamCollectionValid) && success(storageQueueSize) && success(dataDistributionActive) &&
			     success(storageServersRecruiting) && success(versionOffset) && success(dcLag));

			maxVersionOffset += dbInfo->get().recoveryCount * SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT;

			auto check = checker.startIteration(phase);

			std::string evtType = "QuietDatabase" + phase;
			TraceEvent evt(evtType.c_str());
			check.add(evt, "DataInFlight", dataInFlight.get(), dataInFlightGate)
			    .add(evt, "MaxTLogQueueSize", tLogQueueInfo.get().first, maxTLogQueueGate)
			    .add(evt, "MaxTLogPoppedVersionLag", tLogQueueInfo.get().second, maxPoppedVersionLag)
			    .add(evt, "DataDistributionQueueSize", dataDistributionQueueSize.get(), maxDataDistributionQueueSize)
			    .add(evt, "TeamCollectionValid", teamCollectionValid.get(), true, std::equal_to<>())
			    .add(evt, "MaxStorageQueueSize", storageQueueSize.get(), maxStorageServerQueueGate)
			    .add(evt, "DataDistributionActive", dataDistributionActive.get(), true, std::equal_to<>())
			    .add(evt, "StorageServersRecruiting", storageServersRecruiting.get(), false, std::equal_to<>())
			    .add(evt, "VersionOffset", versionOffset.get(), maxVersionOffset)
			    .add(evt, "DatacenterLag", dcLag.get(), maxDcLag);

			evt.detail("RecoveryCount", dbInfo->get().recoveryCount).detail("NumSuccesses", numSuccesses);
			evt.log();

			if (check.success()) {
				if (++numSuccesses == 3) {
					auto msg = "QuietDatabase" + phase + "Done";
					TraceEvent(msg.c_str()).log();
					break;
				} else {
					wait(delay(g_network->isSimulated() ? 2.0 : 30.0));
				}
			} else {
				wait(delay(1.0));
				numSuccesses = 0;
			}
		} catch (Error& e) {
			TraceEvent(("QuietDatabase" + phase + "Error").c_str()).errorUnsuppressed(e);
			if (e.code() != error_code_actor_cancelled && e.code() != error_code_attribute_not_found &&
			    e.code() != error_code_timed_out)
				TraceEvent(("QuietDatabase" + phase + "Error").c_str()).error(e);

			// Client invalid operation occurs if we don't get back a message from one of the servers, often corrected
			// by retrying
			if (e.code() != error_code_attribute_not_found && e.code() != error_code_timed_out)
				throw;

			auto evtType = "QuietDatabase" + phase + "Retry";
			TraceEvent evt(evtType.c_str());
			evt.error(e);
			int notReadyCount = 0;
			if (dataInFlight.isReady() && dataInFlight.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "dataInFlight");
			}
			if (tLogQueueInfo.isReady() && tLogQueueInfo.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "tLogQueueInfo");
			}
			if (dataDistributionQueueSize.isReady() && dataDistributionQueueSize.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "dataDistributionQueueSize");
			}
			if (teamCollectionValid.isReady() && teamCollectionValid.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "teamCollectionValid");
			}
			if (storageQueueSize.isReady() && storageQueueSize.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "storageQueueSize");
			}
			if (dataDistributionActive.isReady() && dataDistributionActive.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "dataDistributionActive");
			}
			if (storageServersRecruiting.isReady() && storageServersRecruiting.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "storageServersRecruiting");
			}
			if (versionOffset.isReady() && versionOffset.isError()) {
				auto key = "NotReady" + std::to_string(notReadyCount++);
				evt.detail(key.c_str(), "versionOffset");
			}
			wait(delay(1.0));
			numSuccesses = 0;
		}
	}

	return Void();
}

Future<Void> quietDatabase(Database const& cx,
                           Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
                           std::string phase,
                           int64_t dataInFlightGate,
                           int64_t maxTLogQueueGate,
                           int64_t maxStorageServerQueueGate,
                           int64_t maxDataDistributionQueueSize,
                           int64_t maxPoppedVersionLag,
                           int64_t maxVersionOffset) {
	return waitForQuietDatabase(cx,
	                            dbInfo,
	                            phase,
	                            dataInFlightGate,
	                            maxTLogQueueGate,
	                            maxStorageServerQueueGate,
	                            maxDataDistributionQueueSize,
	                            maxPoppedVersionLag,
	                            maxVersionOffset);
}
