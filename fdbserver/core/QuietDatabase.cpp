/*
 * QuietDatabase.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/RunRYWTransaction.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbclient/ManagementAPI.h"
#include "flow/CoroUtils.h"

Future<std::vector<WorkerDetails>> getWorkers(Reference<AsyncVar<ServerDBInfo> const> dbInfo, int flags) {
	while (true) {
		auto res = co_await race(
		    brokenPromiseToNever(dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest(flags))),
		    dbInfo->onChange());
		if (res.index() == 0) {
			co_return std::get<0>(std::move(res));
		}
	}
}

// Gets the WorkerInterface representing the Master server.
Future<WorkerInterface> getMasterWorker(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("GetMasterWorker").detail("Stage", "GettingWorkers");

	while (true) {
		auto workers = co_await getWorkers(dbInfo);

		for (int i = 0; i < workers.size(); i++) {
			if (workers[i].interf.address() == dbInfo->get().master.address()) {
				TraceEvent("GetMasterWorker")
				    .detail("Stage", "GotWorkers")
				    .detail("MasterId", dbInfo->get().master.id())
				    .detail("WorkerId", workers[i].interf.id());
				co_return workers[i].interf;
			}
		}

		TraceEvent(SevWarn, "GetMasterWorkerError")
		    .detail("Error", "MasterWorkerNotFound")
		    .detail("Master", dbInfo->get().master.id())
		    .detail("MasterAddress", dbInfo->get().master.address())
		    .detail("WorkerCount", workers.size());

		co_await delay(1.0);
	}
}

// Gets the WorkerInterface representing the data distributor.
Future<WorkerInterface> getDataDistributorWorker(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("GetDataDistributorWorker").detail("Stage", "GettingWorkers");

	while (true) {
		auto workers = co_await getWorkers(dbInfo);
		if (!dbInfo->get().distributor.present())
			continue;

		for (int i = 0; i < workers.size(); i++) {
			if (workers[i].interf.address() == dbInfo->get().distributor.get().address()) {
				TraceEvent("GetDataDistributorWorker")
				    .detail("Stage", "GotWorkers")
				    .detail("DataDistributorId", dbInfo->get().distributor.get().id())
				    .detail("WorkerId", workers[i].interf.id());
				co_return workers[i].interf;
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
Future<int64_t> getDataInFlight(Database cx, WorkerInterface distributorWorker) {
	try {
		TraceEvent("DataInFlight").detail("Stage", "ContactingDataDistributor");
		TraceEventFields md = co_await timeoutError(
		    distributorWorker.eventLogRequest.getReply(EventLogRequest("TotalDataInFlight"_sr)), 1.0);
		int64_t dataInFlight = boost::lexical_cast<int64_t>(md.getValue("TotalBytes"));
		co_return dataInFlight;
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .error(e)
		    .detail("Reason", "Failed to extract DataInFlight");
		throw;
	}
}

// Gets the number of bytes in flight from the data distributor.
Future<int64_t> getDataInFlight(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	WorkerInterface distributorInterf = co_await getDataDistributorWorker(cx, dbInfo);
	int64_t dataInFlight = co_await getDataInFlight(cx, distributorInterf);
	co_return dataInFlight;
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

Future<std::vector<WorkerInterface>> getCoordWorkers(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	auto workers = co_await getWorkers(dbInfo);

	Optional<Value> coordinators =
	    co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
		    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		    tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		    return tr->get(coordinatorsKey);
	    });
	if (!coordinators.present()) {
		throw operation_failed();
	}
	ClusterConnectionString ccs(coordinators.get().toString());
	std::vector<NetworkAddress> coordinatorsAddr = co_await ccs.tryResolveHostnames();
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
	co_return result;
}

// This is not robust in the face of a TLog failure
Future<std::pair<int64_t, int64_t>> getTLogQueueInfo(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	TraceEvent("MaxTLogQueueSize").detail("Stage", "ContactingLogs");

	auto workers = co_await getWorkers(dbInfo);
	std::map<NetworkAddress, WorkerInterface> workersMap;
	for (auto worker : workers) {
		workersMap[worker.interf.address()] = worker.interf;
	}

	std::vector<Future<TraceEventFields>> messages;
	auto tlogs = dbInfo->get().logSystemConfig.allPresentLogs();
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
	co_await waitForAll(messages);

	TraceEvent("MaxTLogQueueSize").detail("Stage", "ComputingMax").detail("MessageCount", messages.size());

	int64_t maxQueueSize = 0;
	int64_t maxPoppedVersionLag = 0;
	for (int i = 0; i < messages.size(); i++) {
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

	co_return std::make_pair(maxQueueSize, maxPoppedVersionLag);
}

Future<std::vector<StorageServerInterface>> getStorageServers(Database cx, bool use_system_priority) {
	Transaction tr(cx);
	while (true) {
		Error err;
		if (use_system_priority) {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		}
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		try {
			RangeResult serverList = co_await tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

			std::vector<StorageServerInterface> servers;
			servers.reserve(serverList.size());
			for (int i = 0; i < serverList.size(); i++)
				servers.push_back(decodeServerListValue(serverList[i].value));
			co_return servers;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<std::pair<std::vector<WorkerInterface>, int>> getStorageWorkers(Database cx,
                                                                       Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                                                       bool localOnly) {
	auto servers = co_await getStorageServers(cx);
	std::map<NetworkAddress, WorkerInterface> workersMap;
	std::vector<WorkerDetails> workers = co_await getWorkers(dbInfo);

	for (const auto& worker : workers) {
		workersMap[worker.interf.address()] = worker.interf;
	}
	Optional<Value> regionsValue =
	    co_await runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) -> Future<Optional<Value>> {
		    tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		    tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		    return tr->get("usable_regions"_sr.withPrefix(configKeysPrefix));
	    });
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
	co_return result;
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
Future<TraceEventFields> getStorageMetricsTimeout(UID storage, WorkerInterface wi, Version version) {
	int retries = 0;
	while (true) {
		Future<TraceEventFields> eventLogReplyFuture =
		    wi.eventLogRequest.getReply(EventLogRequest(StringRef(storage.toString() + "/StorageMetrics")));
		auto eventLogReply = co_await timeout(eventLogReplyFuture, 30.0);
		if (eventLogReply.present()) {
			TraceEventFields const& storageMetrics = eventLogReply.get();
			try {
				if (version == invalidVersion || getDurableVersion(storageMetrics) >= static_cast<int64_t>(version)) {
					co_return storageMetrics;
				}
			} catch (Error& e) {
				TraceEvent("QuietDatabaseFailure")
				    .error(e)
				    .detail("Reason", "Failed to extract DurableVersion from StorageMetrics")
				    .detail("SSID", storage)
				    .detail("StorageMetrics", storageMetrics.toString());
				throw;
			}
		} else {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Could not fetch StorageMetrics")
			    .detail("Storage", storage);
			throw timed_out();
		}
		if (++retries > 30) {
			TraceEvent("QuietDatabaseFailure")
			    .detail("Reason", "Could not fetch StorageMetrics x30")
			    .detail("Storage", storage)
			    .detail("Version", version);
			throw timed_out();
		}
		co_await delay(1.0);
	}
}

// Gets the maximum size of all the storage server queues
Future<int64_t> getMaxStorageServerQueueSize(Database cx,
                                             Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                             Version version) {

	TraceEvent("MaxStorageServerQueueSize").detail("Stage", "ContactingStorageServers");

	Future<std::vector<StorageServerInterface>> serversFuture = getStorageServers(cx);
	Future<std::vector<WorkerDetails>> workersFuture = getWorkers(dbInfo);

	auto servers = co_await serversFuture;
	auto workers = co_await workersFuture;

	std::map<NetworkAddress, WorkerInterface> workersMap;
	for (auto worker : workers) {
		workersMap[worker.interf.address()] = worker.interf;
	}

	std::vector<Future<TraceEventFields>> messages;
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

	co_await waitForAll(messages);

	TraceEvent("MaxStorageServerQueueSize").detail("Stage", "ComputingMax").detail("MessageCount", messages.size());

	co_return extractMaxQueueSize(messages, servers);
}

// Gets the size of the data distribution queue.  If reportInFlight is true, then data in flight is considered part of
// the queue
Future<int64_t> getDataDistributionQueueSize(Database cx, WorkerInterface distributorWorker, bool reportInFlight) {
	try {
		TraceEvent("DataDistributionQueueSize").detail("Stage", "ContactingDataDistributor");

		TraceEventFields movingDataMessage =
		    co_await timeoutError(distributorWorker.eventLogRequest.getReply(EventLogRequest("MovingData"_sr)), 1.0);

		TraceEvent("DataDistributionQueueSize").detail("Stage", "GotString");

		int64_t inQueue = boost::lexical_cast<int64_t>(movingDataMessage.getValue("InQueue"));

		if (reportInFlight) {
			int64_t inFlight = boost::lexical_cast<int64_t>(movingDataMessage.getValue("InFlight"));
			inQueue += inFlight;
		}

		co_return inQueue;
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .detail("Reason", "Failed to extract DataDistributionQueueSize");
		throw;
	}
}

// Gets the size of the data distribution queue.  If reportInFlight is true, then data in flight is considered part of
// the queue Convenience method that first finds the master worker from a zookeeper interface
Future<int64_t> getDataDistributionQueueSize(Database cx,
                                             Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                             bool reportInFlight) {
	WorkerInterface distributorInterf = co_await getDataDistributorWorker(cx, dbInfo);
	int64_t inQueue = co_await getDataDistributionQueueSize(cx, distributorInterf, reportInFlight);
	co_return inQueue;
}

// Gets if the number of process and machine teams does not exceed the maximum allowed number of teams
Future<bool> getTeamCollectionValid(Database cx, WorkerInterface dataDistributorWorker) {
	int attempts = 0;
	bool ret = false;
	while (true) {
		Optional<double> retryDelay;
		try {
			if (!g_network->isSimulated()) {
				co_return true;
			}

			TraceEvent("GetTeamCollectionValid").detail("Stage", "ContactingMaster");

			TraceEventFields teamCollectionInfoMessage = co_await timeoutError(
			    dataDistributorWorker.eventLogRequest.getReply(EventLogRequest("TeamCollectionInfo"_sr)), 1.0);

			TraceEvent("GetTeamCollectionValid").detail("Stage", "GotString");

			int64_t currentTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("CurrentServerTeams"));
			int64_t desiredTeams = boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("DesiredTeams"));
			int64_t maxTeams = boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxTeams"));
			int64_t currentMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("CurrentMachineTeams"));
			int64_t healthyMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("CurrentHealthyMachineTeams"));
			int64_t desiredMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("DesiredMachineTeams"));
			int64_t maxMachineTeams =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxMachineTeams"));

			int64_t minServerTeamsOnServer =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MinTeamsOnServer"));
			int64_t maxServerTeamsOnServer =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MaxTeamsOnServer"));
			int64_t minMachineTeamsOnMachine =
			    boost::lexical_cast<int64_t>(teamCollectionInfoMessage.getValue("MinMachineTeamsOnMachine"));
			int64_t maxMachineTeamsOnMachine =
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
					retryDelay = 60.0; // We may not receive the most recent TeamCollectionInfo
				} else {
					// When DESIRED_TEAMS_PER_SERVER == 1, we see minMachineTeamOnMachine can be 0 in one out of 30k
					// test cases. Only check DESIRED_TEAMS_PER_SERVER == 3 for now since it is mostly used
					// configuration.
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
					co_return ret;
				}
			} else {
				co_return true;
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
				co_return true;
			}
			retryDelay = 10.0;
		}
		co_await delay(retryDelay.get());
	}
}

// Gets if the number of process and machine teams does not exceed the maximum allowed number of teams
// Convenience method that first finds the master worker from a zookeeper interface
Future<bool> getTeamCollectionValid(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	WorkerInterface dataDistributorWorker = co_await getDataDistributorWorker(cx, dbInfo);
	bool valid = co_await getTeamCollectionValid(cx, dataDistributorWorker);
	co_return valid;
}

// Checks that data distribution is active
Future<bool> getDataDistributionActive(Database cx, WorkerInterface distributorWorker) {
	try {
		TraceEvent("DataDistributionActive").detail("Stage", "ContactingDataDistributor");

		TraceEventFields activeMessage = co_await timeoutError(
		    distributorWorker.eventLogRequest.getReply(EventLogRequest("DDTrackerStarting"_sr)), 1.0);

		co_return activeMessage.getValue("State") == "Active";
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .detail("Reason", "Failed to extract DataDistributionActive");
		throw;
	}
}

// Checks to see if any storage servers are being recruited
Future<bool> getStorageServersRecruiting(Database cx, WorkerInterface distributorWorker, UID distributorUID) {
	try {
		TraceEvent("StorageServersRecruiting").detail("Stage", "ContactingDataDistributor");
		TraceEventFields recruitingMessage =
		    co_await timeoutError(distributorWorker.eventLogRequest.getReply(EventLogRequest(
		                              StringRef("StorageServerRecruitment_" + distributorUID.toString()))),
		                          1.0);

		TraceEvent("StorageServersRecruiting").detail("Message", recruitingMessage.toString());

		if (recruitingMessage.getValue("State") == "Recruiting") {
			std::string tssValue;
			// if we're tss recruiting, that's fine because that can block indefinitely if only 1 free storage process
			if (!recruitingMessage.tryGetValue("IsTSS", tssValue) || tssValue == "False") {
				co_return true;
			}
		}
		co_return false;
	} catch (Error& e) {
		TraceEvent("QuietDatabaseFailure", distributorWorker.id())
		    .detail("Reason", "Failed to extract StorageServersRecruiting")
		    .detail("DataDistributorID", distributorUID);
		throw;
	}
}

// Gets the difference between the expected version (based on the version
// epoch) and the actual version.
Future<int64_t> getVersionOffset(Database cx,
                                 WorkerInterface distributorWorker,
                                 Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			TraceEvent("GetVersionOffset").detail("Stage", "ReadingVersionEpoch");

			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Version rv = co_await tr.getReadVersion();
			Optional<Standalone<StringRef>> versionEpochValue = co_await tr.get(versionEpochKey);
			if (!versionEpochValue.present()) {
				co_return 0;
			}
			int64_t versionEpoch = BinaryReader::fromStringRef<int64_t>(versionEpochValue.get(), Unversioned());
			int64_t versionOffset = abs(rv - (g_network->timer() * SERVER_KNOBS->VERSIONS_PER_SECOND - versionEpoch));
			co_return versionOffset;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// Returns DC lag for simulation runs
Future<Version> getDatacenterLag(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	while (true) {
		if (!g_network->isSimulated() || g_simulator->usableRegions == 1) {
			co_return 0;
		}

		Optional<TLogInterface> primaryLog;
		Optional<TLogInterface> remoteLog;
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
			co_await dbInfo->onChange();
			continue;
		}

		ASSERT(primaryLog.present());
		ASSERT(remoteLog.present());

		Future<Void> onChange = dbInfo->onChange();
		while (true) {
			Future<TLogQueuingMetricsReply> primaryMetrics =
			    brokenPromiseToNever(primaryLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));
			Future<TLogQueuingMetricsReply> remoteMetrics =
			    brokenPromiseToNever(remoteLog.get().getQueuingMetrics.getReply(TLogQueuingMetricsRequest()));

			co_await ((success(primaryMetrics) && success(remoteMetrics)) || onChange);
			if (onChange.isReady()) {
				break;
			}

			TraceEvent("DCLag").detail("Primary", primaryMetrics.get().v).detail("Remote", remoteMetrics.get().v);
			ASSERT(primaryMetrics.get().v >= 0 && remoteMetrics.get().v >= 0);
			co_return primaryMetrics.get().v - remoteMetrics.get().v;
		}
	}
}

Future<Void> repairDeadDatacenter(Database cx, Reference<AsyncVar<ServerDBInfo> const> dbInfo, std::string context) {
	if (g_network->isSimulated() && g_simulator->usableRegions > 1 && !g_simulator->quiesced) {
		bool primaryDead = g_simulator->datacenterDead(g_simulator->primaryDcId);
		bool remoteDead = g_simulator->datacenterDead(g_simulator->remoteDcId);

		// FIXME: the primary and remote can both be considered dead because excludes are not handled properly by the
		// datacenterDead function
		if (primaryDead && remoteDead) {
			TraceEvent(SevWarnAlways, "CannotDisableFearlessConfiguration").log();
			co_return;
		}
		if (primaryDead || remoteDead) {
			if (remoteDead) {
				std::vector<AddressExclusion> servers =
				    g_simulator->getAllAddressesInDCToExclude(g_simulator->remoteDcId);
				TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration")
				    .detail("Location", context)
				    .detail("Stage", "ExcludeServers")
				    .detail("Servers", describe(servers));
				co_await excludeServers(cx, servers, false);
			}

			TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration")
			    .detail("Location", context)
			    .detail("Stage", "Repopulate")
			    .detail("RemoteDead", remoteDead)
			    .detail("PrimaryDead", primaryDead);
			g_simulator->usableRegions = 1;

			co_await ManagementAPI::changeConfig(
			    cx.getReference(),
			    (primaryDead ? g_simulator->disablePrimary : g_simulator->disableRemote) + " repopulate_anti_quorum=1",
			    true);
			while (dbInfo->get().recoveryState < RecoveryState::STORAGE_RECOVERED) {
				co_await dbInfo->onChange();
			}
			TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration")
			    .detail("Location", context)
			    .detail("Stage", "Usable_Regions");
			co_await ManagementAPI::changeConfig(cx.getReference(), "usable_regions=1", true);
		}
	}
}

Future<Void> reconfigureAfter(Database cx,
                              double time,
                              Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                              std::string context) {
	co_await delay(time);
	co_await uncancellable(repairDeadDatacenter(cx, dbInfo, context));
}

struct QuietDatabaseChecker {
	ProcessEvents::Callback timeoutCallback = [this](StringRef name, std::any const& msg, Error const& e) {
		logFailure(name, std::any_cast<StringRef>(msg), e);
	};
	double start = now();
	double maxDDRunTime;
	ProcessEvents::Event timeoutEvent;
	std::vector<std::string> lastFailReasons;

	explicit QuietDatabaseChecker(double maxDDRunTime)
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

Future<Void> enableConsistencyScanInSim(Database db) {
	auto tr = makeReference<ReadYourWritesTransaction>(db);
	ConsistencyScanState cs;
	if (!g_network->isSimulated()) {
		co_return;
	}
	TraceEvent("ConsistencyScan_SimEnable").log();
	while (true) {
		Error err;
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);
			ConsistencyScanState::Config config = co_await cs.config().getD(tr);

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
				co_await tr->commit();

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
			err = e;
		}
		co_await tr->onError(err);
	}
}

Future<Void> disableConsistencyScanInSim(Database db, bool waitForCompletion) {
	auto tr = makeReference<ReadYourWritesTransaction>(db);
	ConsistencyScanState cs;
	if (!g_network->isSimulated()) {
		co_return;
	}

	if (waitForCompletion) {
		TraceEvent("ConsistencyScan_SimDisableWaiting").log();
		printf("Waiting for consistency scan to complete...\n");
		while (true) {
			bool waitForCorruption = g_simulator->doInjectConsistencyScanCorruption.present() &&
			                         g_simulator->doInjectConsistencyScanCorruption.get();
			if (((waitForCorruption &&
			      g_simulator->consistencyScanState >= ISimulator::SimConsistencyScanState::Enabled_FoundCorruption) ||
			     (!waitForCorruption &&
			      g_simulator->consistencyScanState >= ISimulator::SimConsistencyScanState::Enabled))) {
				break;
			}
			co_await delay(1.0);
		}
	}
	TraceEvent("ConsistencyScan_SimDisable").log();
	while (true) {
		Error err;
		try {
			SystemDBWriteLockedNow(db.getReference())->setOptions(tr);
			ConsistencyScanState::Config config = co_await cs.config().getD(tr);
			bool skipDisable = false;
			// see if any rounds have completed
			if (waitForCompletion) {
				ConsistencyScanState::RoundStats statsCurrentRound = co_await cs.currentRoundStats().getD(tr);
				ConsistencyScanState::StatsHistoryMap::RangeResultType olderStats =
				    co_await cs.roundStatsHistory().getRange(tr, {}, {}, 1, Snapshot::False, Reverse::False);
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
				co_return;
			}

			if (skipDisable) {
				co_await delay(2.0);
				tr->reset();
				continue;
			} else {
				cs.config().set(tr, config);
				co_await tr->commit();
				break;
			}
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}

	g_simulator->updateConsistencyScanState(ISimulator::SimConsistencyScanState::Complete,
	                                        ISimulator::SimConsistencyScanState::DisabledEnd);
	CODE_PROBE(true, "Consistency Scan disabled in simulation");
	TraceEvent("ConsistencyScan_SimDisabled").log();
	printf("Consistency scan complete.\n");
}

// Waits until a database quiets down (no data in flight, small tlog queue, low SQ, no active data distribution). This
// requires the database to be available and healthy in order to succeed.
Future<Void> waitForQuietDatabase(Database cx,
                                  Reference<AsyncVar<ServerDBInfo> const> dbInfo,
                                  std::string phase,
                                  int64_t dataInFlightGate = 2e6,
                                  int64_t maxTLogQueueGate = 5e6,
                                  int64_t maxStorageServerQueueGate = 5e6,
                                  int64_t maxDataDistributionQueueSize = 0,
                                  int64_t maxPoppedVersionLag = 30e6,
                                  int64_t maxVersionOffset = 1e6,
                                  double maxDDRunTime = 0) {
	// Use provided maxDDRunTime, or fallback to default (1500s or 4000s with buggify)
	QuietDatabaseChecker checker(maxDDRunTime > 0 ? maxDDRunTime : (isGeneralBuggifyEnabled() ? 4000.0 : 1500.0));
	[[maybe_unused]] Future<Void> reconfig =
	    reconfigureAfter(cx, 100 + (deterministicRandom()->random01() * 100), dbInfo, "QuietDatabase");
	Future<int64_t> dataInFlight;
	Future<std::pair<int64_t, int64_t>> tLogQueueInfo;
	Future<int64_t> dataDistributionQueueSize;
	Future<bool> teamCollectionValid;
	Future<int64_t> storageQueueSize;
	Future<bool> dataDistributionActive;
	Future<bool> storageServersRecruiting;
	Future<int64_t> versionOffset;
	Future<Version> dcLag;
	Version maxDcLag = 100e6;
	std::string traceMessage = "QuietDatabase" + phase + "Begin";
	TraceEvent(traceMessage.c_str()).log();

	// In a simulated environment, wait 5 seconds so that workers can move to their optimal locations
	if (g_network->isSimulated())
		co_await delay(5.0);

	TraceEvent("QuietDatabaseWaitingOnFullRecovery").detail("Phase", phase).log();
	while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED) {
		co_await dbInfo->onChange();
	}

	// The quiet database check (which runs at the end of every test) will always time out due to active data movement.
	// To get around this, quiet Database will disable the perpetual wiggle in the setup phase.

	printf("Set perpetual_storage_wiggle=0 ...\n");
	Version version = co_await setPerpetualStorageWiggle(cx, false, LockAware::True);
	printf("Set perpetual_storage_wiggle=0 Done.\n");

	printf("Disabling backup worker ...\n");
	co_await disableBackupWorker(cx);
	printf("Disabled backup worker.\n");

	co_await disableConsistencyScanInSim(cx, false);

	// Require 3 consecutive successful quiet database checks spaced 2 second apart
	int numSuccesses = 0;
	while (true) {
		Optional<double> retryDelay;
		try {
			TraceEvent("QuietDatabaseWaitingOnDataDistributor").log();
			WorkerInterface distributorWorker = co_await getDataDistributorWorker(cx, dbInfo);
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

			co_await (success(dataInFlight) && success(tLogQueueInfo) && success(dataDistributionQueueSize) &&
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
					retryDelay = g_network->isSimulated() ? 2.0 : 30.0;
				}
			} else {
				retryDelay = 1.0;
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
			retryDelay = 1.0;
			numSuccesses = 0;
		}
		if (retryDelay.present()) {
			co_await delay(retryDelay.get());
		}
	}
}

Future<Void> quietDatabase(Database const& cx,
                           Reference<AsyncVar<ServerDBInfo> const> const& dbInfo,
                           std::string phase,
                           int64_t dataInFlightGate,
                           int64_t maxTLogQueueGate,
                           int64_t maxStorageServerQueueGate,
                           int64_t maxDataDistributionQueueSize,
                           int64_t maxPoppedVersionLag,
                           int64_t maxVersionOffset,
                           double maxDDRunTime) {
	return waitForQuietDatabase(cx,
	                            dbInfo,
	                            phase,
	                            dataInFlightGate,
	                            maxTLogQueueGate,
	                            maxStorageServerQueueGate,
	                            maxDataDistributionQueueSize,
	                            maxPoppedVersionLag,
	                            maxVersionOffset,
	                            maxDDRunTime);
}
