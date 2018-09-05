/*
 * QuietDatabase.actor.cpp
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

#include "flow/ActorCollection.h"
#include "fdbrpc/simulator.h"
#include "flow/Trace.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/DatabaseContext.h"
#include "TesterInterface.h"
#include "WorkerInterface.h"
#include "ServerDBInfo.h"
#include "Status.h"
#include "fdbclient/ManagementAPI.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

ACTOR Future<vector<std::pair<WorkerInterface, ProcessClass>>> getWorkers( Reference<AsyncVar<ServerDBInfo>> dbInfo, int flags = 0 ) {
	loop {
		choose {
			when( vector<std::pair<WorkerInterface, ProcessClass>> w = wait( brokenPromiseToNever( dbInfo->get().clusterInterface.getWorkers.getReply( GetWorkersRequest( flags ) ) ) ) ) {
				return w;
			}
			when( wait( dbInfo->onChange() ) ) {}
		}
	}
}

//Gets the WorkerInterface representing the Master server.
ACTOR Future<WorkerInterface> getMasterWorker( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo ) {
	TraceEvent("GetMasterWorker").detail("Stage", "GettingWorkers");

	loop {
		state vector<std::pair<WorkerInterface, ProcessClass>> workers = wait( getWorkers( dbInfo ) );

		for( int i = 0; i < workers.size(); i++ ) {
			if( workers[i].first.address() == dbInfo->get().master.address() ) {
				TraceEvent("GetMasterWorker").detail("Stage", "GotWorkers").detail("MasterId", dbInfo->get().master.id()).detail("WorkerId", workers[i].first.id());
				return workers[i].first;
			}
		}

		TraceEvent(SevWarn, "GetMasterWorkerError")
			.detail("Error", "MasterWorkerNotFound")
			.detail("Master", dbInfo->get().master.id()).detail("MasterAddress", dbInfo->get().master.address())
			.detail("WorkerCount", workers.size());

		wait(delay(1.0));
	}
}

//Gets the number of bytes in flight from the master
ACTOR Future<int64_t> getDataInFlight( Database cx, WorkerInterface masterWorker ) {
	try {
		TraceEvent("DataInFlight").detail("Stage", "ContactingMaster");
		TraceEventFields md = wait( timeoutError(masterWorker.eventLogRequest.getReply(
			EventLogRequest( LiteralStringRef("TotalDataInFlight") ) ), 1.0 ) );
		int64_t dataInFlight;
		sscanf(md.getValue("TotalBytes").c_str(), "%lld", &dataInFlight);
		return dataInFlight;
	} catch( Error &e ) {
		TraceEvent("QuietDatabaseFailure", masterWorker.id()).error(e).detail("Reason", "Failed to extract DataInFlight");
		throw;
	}

}

//Gets the number of bytes in flight from the master
//Convenience method that first finds the master worker from a zookeeper interface
ACTOR Future<int64_t> getDataInFlight( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo ) {
	WorkerInterface masterWorker = wait(getMasterWorker(cx, dbInfo));
	int64_t dataInFlight = wait(getDataInFlight(cx, masterWorker));
	return dataInFlight;
}

//Computes the queue size for storage servers and tlogs using the bytesInput and bytesDurable attributes
int64_t getQueueSize( TraceEventFields md ) {
	double inputRate, durableRate;
	double inputRoughness, durableRoughness;
	int64_t inputBytes, durableBytes;

	sscanf(md.getValue("BytesInput").c_str(), "%lf %lf %lld", &inputRate, &inputRoughness, &inputBytes);
	sscanf(md.getValue("BytesDurable").c_str(), "%lf %lf %lld", &durableRate, &durableRoughness, &durableBytes);

	return inputBytes - durableBytes;
}

// This is not robust in the face of a TLog failure
ACTOR Future<int64_t> getMaxTLogQueueSize( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo, WorkerInterface masterWorker ) {
	TraceEvent("MaxTLogQueueSize").detail("Stage", "ContactingLogs");

	state std::vector<std::pair<WorkerInterface, ProcessClass>> workers = wait(getWorkers(dbInfo));
	std::map<NetworkAddress, WorkerInterface> workersMap;
	for(auto worker : workers) {
		workersMap[worker.first.address()] = worker.first;
	}

	state std::vector<Future<TraceEventFields>> messages;
	state std::vector<TLogInterface> tlogs = dbInfo->get().logSystemConfig.allPresentLogs();
	for(int i = 0; i < tlogs.size(); i++) {
		auto itr = workersMap.find(tlogs[i].address());
		if(itr == workersMap.end()) {
			TraceEvent("QuietDatabaseFailure").detail("Reason", "Could not find worker for log server").detail("Tlog", tlogs[i].id());
			throw attribute_not_found();
		}
		messages.push_back( timeoutError(itr->second.eventLogRequest.getReply(
			EventLogRequest( StringRef(tlogs[i].id().toString() + "/TLogMetrics") ) ), 1.0 ) );
	}
	wait( waitForAll( messages ) );

	TraceEvent("MaxTLogQueueSize").detail("Stage", "ComputingMax").detail("MessageCount", messages.size());

	state int64_t maxQueueSize = 0;
	state int i = 0;
	for(; i < messages.size(); i++) {
		try {
			maxQueueSize = std::max( maxQueueSize, getQueueSize( messages[i].get() ) );
		} catch( Error &e ) {
			TraceEvent("QuietDatabaseFailure").detail("Reason", "Failed to extract MaxTLogQueue").detail("Tlog", tlogs[i].id());
			throw;
		}
	}

	return maxQueueSize;
}

ACTOR Future<int64_t> getMaxTLogQueueSize( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo ) {
	WorkerInterface masterWorker = wait(getMasterWorker(cx, dbInfo));
	int64_t maxQueueSize = wait(getMaxTLogQueueSize(cx, dbInfo, masterWorker));
	return maxQueueSize;
}

ACTOR Future<vector<StorageServerInterface>> getStorageServers( Database cx, bool use_system_priority = false) {
	state Transaction tr( cx );
	if (use_system_priority)
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	loop {
		try {
			Standalone<RangeResultRef> serverList = wait( tr.getRange( serverListKeys, CLIENT_KNOBS->TOO_MANY ) );
			ASSERT( !serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY );

			vector<StorageServerInterface> servers;
			for( int i = 0; i < serverList.size(); i++ )
				servers.push_back( decodeServerListValue( serverList[i].value ) );
			return servers;
		}
		catch(Error &e) {
			wait( tr.onError(e) );
		}
	}
}

//Gets the maximum size of all the storage server queues
ACTOR Future<int64_t> getMaxStorageServerQueueSize( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo, WorkerInterface masterWorker ) {
	TraceEvent("MaxStorageServerQueueSize").detail("Stage", "ContactingStorageServers");

	Future<std::vector<StorageServerInterface>> serversFuture = getStorageServers(cx);
	state Future<std::vector<std::pair<WorkerInterface, ProcessClass>>> workersFuture = getWorkers(dbInfo);

	state std::vector<StorageServerInterface> servers = wait(serversFuture);
	state std::vector<std::pair<WorkerInterface, ProcessClass>> workers = wait(workersFuture);

	std::map<NetworkAddress, WorkerInterface> workersMap;
	for(auto worker : workers) {
		workersMap[worker.first.address()] = worker.first;
	}

	state std::vector<Future<TraceEventFields>> messages;
	for(int i = 0; i < servers.size(); i++) {
		auto itr = workersMap.find(servers[i].address());
		if(itr == workersMap.end()) {
			TraceEvent("QuietDatabaseFailure").detail("Reason", "Could not find worker for storage server").detail("SS", servers[i].id());
			throw attribute_not_found();
		}
		messages.push_back( timeoutError(itr->second.eventLogRequest.getReply(
			EventLogRequest( StringRef(servers[i].id().toString() + "/StorageMetrics") ) ), 1.0 ) );
	}

	wait( waitForAll(messages) );

	TraceEvent("MaxStorageServerQueueSize").detail("Stage", "ComputingMax").detail("MessageCount", messages.size());

	state int64_t maxQueueSize = 0;
	state int i = 0;
	for(; i < messages.size(); i++) {
		try {
			maxQueueSize = std::max( maxQueueSize, getQueueSize( messages[i].get() ) );
		} catch( Error &e ) {
			TraceEvent("QuietDatabaseFailure", masterWorker.id()).detail("Reason", "Failed to extract MaxStorageServerQueue").detail("SS", servers[i].id());
			throw;
		}
	}

	return maxQueueSize;
}

//Gets the maximum size of all the storage server queues
//Convenience method that first gets the master worker and system map from a zookeeper interface
ACTOR Future<int64_t> getMaxStorageServerQueueSize( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo ) {
	WorkerInterface masterWorker = wait(getMasterWorker(cx, dbInfo));
	int64_t maxQueueSize = wait(getMaxStorageServerQueueSize(cx, dbInfo, masterWorker));
	return maxQueueSize;
}

//Gets the size of the data distribution queue.  If reportInFlight is true, then data in flight is considered part of the queue
ACTOR Future<int64_t> getDataDistributionQueueSize( Database cx, WorkerInterface masterWorker, bool reportInFlight) {
	try {
		TraceEvent("DataDistributionQueueSize").detail("Stage", "ContactingMaster");

		TraceEventFields movingDataMessage = wait( timeoutError(masterWorker.eventLogRequest.getReply(
			EventLogRequest( LiteralStringRef("MovingData") ) ), 1.0 ) );

		TraceEvent("DataDistributionQueueSize").detail("Stage", "GotString");

		int64_t inQueue;
		sscanf(movingDataMessage.getValue("InQueue").c_str(), "%lld", &inQueue);

		if(reportInFlight) {
			int64_t inFlight;
			sscanf(movingDataMessage.getValue("InFlight").c_str(), "%lld", &inFlight);
			inQueue += inFlight;
		}

		return inQueue;
	} catch( Error &e ) {
		TraceEvent("QuietDatabaseFailure", masterWorker.id()).detail("Reason", "Failed to extract DataDistributionQueueSize");
		throw;
	}
}

//Gets the size of the data distribution queue.  If reportInFlight is true, then data in flight is considered part of the queue
//Convenience method that first finds the master worker from a zookeeper interface
ACTOR Future<int64_t> getDataDistributionQueueSize( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo, bool reportInFlight ) {
	WorkerInterface masterWorker = wait(getMasterWorker(cx, dbInfo));
	int64_t inQueue = wait(getDataDistributionQueueSize( cx, masterWorker, reportInFlight));
	return inQueue;
}

//Checks that data distribution is active
ACTOR Future<bool> getDataDistributionActive( Database cx, WorkerInterface masterWorker ) {
	try {
		TraceEvent("DataDistributionActive").detail("Stage", "ContactingMaster");

		TraceEventFields activeMessage = wait( timeoutError(masterWorker.eventLogRequest.getReply(
			EventLogRequest( LiteralStringRef("DDTrackerStarting") ) ), 1.0 ) );

		return activeMessage.getValue("State") == "Active";
	} catch( Error &e ) {
		TraceEvent("QuietDatabaseFailure", masterWorker.id()).detail("Reason", "Failed to extract DataDistributionActive");
		throw;
	}
}

//Checks to see if any storage servers are being recruited
ACTOR Future<bool> getStorageServersRecruiting( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo, WorkerInterface masterWorker ) {
	try {
		TraceEvent("StorageServersRecruiting").detail("Stage", "ContactingMaster");

		TraceEventFields recruitingMessage = wait( timeoutError(masterWorker.eventLogRequest.getReply(
			EventLogRequest( StringRef( "StorageServerRecruitment_" + dbInfo->get().master.id().toString()) ) ), 1.0 ) );

		return recruitingMessage.getValue("State") == "Recruiting";
	} catch( Error &e ) {
		TraceEvent("QuietDatabaseFailure", masterWorker.id()).detail("Reason", "Failed to extract StorageServersRecruiting").detail("MasterID", dbInfo->get().master.id());
		throw;
	}
}

ACTOR Future<Void> repairDeadDatacenter(Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo, std::string context) {
	if(g_network->isSimulated()) {
		bool primaryDead = g_simulator.datacenterDead(g_simulator.primaryDcId);
		bool remoteDead = g_simulator.datacenterDead(g_simulator.remoteDcId);

		ASSERT(!primaryDead || !remoteDead);
		if(primaryDead || remoteDead) {
			TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration").detail("Location", context).detail("Stage", "Repopulate");
			g_simulator.usableRegions = 1;
			ConfigurationResult::Type _ = wait( changeConfig( cx, (primaryDead ? g_simulator.disablePrimary : g_simulator.disableRemote) + " repopulate_anti_quorum=1" ) );
			while( dbInfo->get().recoveryState < RecoveryState::STORAGE_RECOVERED ) {
				wait( dbInfo->onChange() );
			}
			TraceEvent(SevWarnAlways, "DisablingFearlessConfiguration").detail("Location", context).detail("Stage", "Usable_Regions");
			ConfigurationResult::Type _ = wait( changeConfig( cx, "usable_regions=1" ) );
		}
	}
	return Void();
}

ACTOR Future<Void> reconfigureAfter(Database cx, double time, Reference<AsyncVar<ServerDBInfo>> dbInfo, std::string context) {
	wait( delay(time) );
	wait( repairDeadDatacenter(cx, dbInfo, context) );
	return Void();
}

ACTOR Future<Void> waitForQuietDatabase( Database cx, Reference<AsyncVar<ServerDBInfo>> dbInfo, std::string phase, int64_t dataInFlightGate = 2e6,
	int64_t maxTLogQueueGate = 5e6, int64_t maxStorageServerQueueGate = 5e6, int64_t maxDataDistributionQueueSize = 0 ) {
	state Future<Void> reconfig = reconfigureAfter(cx, 100 + (g_random->random01()*100), dbInfo, "QuietDatabase");

	TraceEvent(("QuietDatabase" + phase + "Begin").c_str());

	//In a simulated environment, wait 5 seconds so that workers can move to their optimal locations
	if(g_network->isSimulated())
		wait(delay(5.0));

	//Require 3 consecutive successful quiet database checks spaced 2 second apart
	state int numSuccesses = 0;

	loop {
		try {
			TraceEvent("QuietDatabaseWaitingOnMaster");
			WorkerInterface masterWorker = wait(getMasterWorker( cx, dbInfo ));
			TraceEvent("QuietDatabaseGotMaster");

			state Future<int64_t> dataInFlight = getDataInFlight( cx, masterWorker);
			state Future<int64_t> tLogQueueSize = getMaxTLogQueueSize( cx, dbInfo, masterWorker );
			state Future<int64_t> dataDistributionQueueSize = getDataDistributionQueueSize( cx, masterWorker, dataInFlightGate == 0);
			state Future<int64_t> storageQueueSize = getMaxStorageServerQueueSize( cx, dbInfo, masterWorker );
			state Future<bool> dataDistributionActive = getDataDistributionActive( cx, masterWorker );
			state Future<bool> storageServersRecruiting = getStorageServersRecruiting ( cx, dbInfo, masterWorker );

			wait( success( dataInFlight ) && success( tLogQueueSize ) && success( dataDistributionQueueSize )
							&& success( storageQueueSize ) && success( dataDistributionActive ) && success( storageServersRecruiting ) );
			TraceEvent(("QuietDatabase" + phase).c_str())
				.detail("DataInFlight", dataInFlight.get()).detail("MaxTLogQueueSize", tLogQueueSize.get()).detail("DataDistributionQueueSize", dataDistributionQueueSize.get())
				.detail("MaxStorageQueueSize", storageQueueSize.get()).detail("DataDistributionActive", dataDistributionActive.get())
				.detail("StorageServersRecruiting", storageServersRecruiting.get());

			if( dataInFlight.get() > dataInFlightGate || tLogQueueSize.get() > maxTLogQueueGate
				|| dataDistributionQueueSize.get() > maxDataDistributionQueueSize || storageQueueSize.get() > maxStorageServerQueueGate
				|| dataDistributionActive.get() == false || storageServersRecruiting.get() == true) {

				wait( delay( 1.0 ) );
				numSuccesses = 0;
			} else {
				if(++numSuccesses == 3) {
					TraceEvent(("QuietDatabase" + phase + "Done").c_str());
					break;
				}
				else
					wait(delay( 2.0 ) );
			}
		} catch (Error& e) {
			if( e.code() != error_code_actor_cancelled && e.code() != error_code_attribute_not_found && e.code() != error_code_timed_out)
				TraceEvent(("QuietDatabase" + phase + "Error").c_str()).error(e);

			//Client invalid operation occurs if we don't get back a message from one of the servers, often corrected by retrying
			if(e.code() != error_code_attribute_not_found && e.code() != error_code_timed_out)
				throw;

			TraceEvent(("QuietDatabase" + phase + "Retry").c_str()).error(e);
			wait(delay(1.0));
			numSuccesses = 0;
		}
	}

	return Void();
}

Future<Void> quietDatabase( Database const& cx, Reference<AsyncVar<ServerDBInfo>> const& dbInfo, std::string phase, int64_t dataInFlightGate,
	int64_t maxTLogQueueGate, int64_t maxStorageServerQueueGate, int64_t maxDataDistributionQueueSize ) {
	return waitForQuietDatabase(cx, dbInfo, phase, dataInFlightGate, maxTLogQueueGate, maxStorageServerQueueGate, maxDataDistributionQueueSize);
}
