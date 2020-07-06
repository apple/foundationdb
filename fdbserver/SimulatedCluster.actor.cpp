/*
 * SimulatedCluster.actor.cpp
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

#include <fstream>
#include "fdbrpc/simulator.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbmonitor/SimpleIni.h"
#include "fdbrpc/AsyncFileNonDurable.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/versions.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

#undef max
#undef min

extern "C" int g_expect_full_pointermap;
extern const char* getSourceVersion();

const int MACHINE_REBOOT_TIME = 10;

bool destructed = false;

template <class T>
T simulate( const T& in ) {
	BinaryWriter writer(AssumeVersion(currentProtocolVersion));
	writer << in;
	BinaryReader reader( writer.getData(), writer.getLength(), AssumeVersion(currentProtocolVersion) );
	T out;
	reader >> out;
	return out;
}

ACTOR Future<Void> runBackup( Reference<ClusterConnectionFile> connFile ) {
	state std::vector<Future<Void>> agentFutures;

	while (g_simulator.backupAgents == ISimulator::WaitForType) {
		wait(delay(1.0));
	}

	if (g_simulator.backupAgents == ISimulator::BackupToFile) {
		Database cx = Database::createDatabase(connFile, -1);

		state FileBackupAgent fileAgent;
		state double backupPollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
		agentFutures.push_back(fileAgent.run(cx, &backupPollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));

		while (g_simulator.backupAgents == ISimulator::BackupToFile) {
			wait(delay(1.0));
		}

		for(auto it : agentFutures) {
			it.cancel();
		}
	}

	wait(Future<Void>(Never()));
	throw internal_error();
}

ACTOR Future<Void> runDr( Reference<ClusterConnectionFile> connFile ) {
	state std::vector<Future<Void>> agentFutures;

	while (g_simulator.drAgents == ISimulator::WaitForType) {
		wait(delay(1.0));
	}

	if (g_simulator.drAgents == ISimulator::BackupToDB) {
		Database cx = Database::createDatabase(connFile, -1);

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		state Database extraDB = Database::createDatabase(extraFile, -1);

		TraceEvent("StartingDrAgents").detail("ConnFile", connFile->getConnectionString().toString()).detail("ExtraString", extraFile->getConnectionString().toString());

		state DatabaseBackupAgent dbAgent = DatabaseBackupAgent(cx);
		state DatabaseBackupAgent extraAgent = DatabaseBackupAgent(extraDB);

		state double dr1PollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
		state double dr2PollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;

		agentFutures.push_back(extraAgent.run(cx, &dr1PollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));
		agentFutures.push_back(dbAgent.run(extraDB, &dr2PollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));

		while (g_simulator.drAgents == ISimulator::BackupToDB) {
			wait(delay(1.0));
		}

		TraceEvent("StoppingDrAgents");

		for(auto it : agentFutures) {
			it.cancel();
		}
	}

	wait(Future<Void>(Never()));
	throw internal_error();
}

enum AgentMode {
	AgentNone = 0,
	AgentOnly = 1,
	AgentAddition = 2
};

// SOMEDAY: when a process can be rebooted in isolation from the other on that machine,
//  a loop{} will be needed around the waiting on simulatedFDBD(). For now this simply
//  takes care of house-keeping such as context switching and file closing.
ACTOR Future<ISimulator::KillType> simulatedFDBDRebooter(Reference<ClusterConnectionFile> connFile, IPAddress ip,
                                                         bool sslEnabled,
                                                         uint16_t port, uint16_t listenPerProcess,
                                                         LocalityData localities, ProcessClass processClass,
                                                         std::string* dataFolder, std::string* coordFolder,
                                                         std::string baseFolder, ClusterConnectionString connStr,
                                                         bool useSeedFile, AgentMode runBackupAgents,
                                                         std::string whitelistBinPaths) {
	state ISimulator::ProcessInfo *simProcess = g_simulator.getCurrentProcess();
	state UID randomId = nondeterministicRandom()->randomUniqueID();
	state int cycles = 0;

	loop {
		auto waitTime = SERVER_KNOBS->MIN_REBOOT_TIME + (SERVER_KNOBS->MAX_REBOOT_TIME - SERVER_KNOBS->MIN_REBOOT_TIME) * deterministicRandom()->random01();
		cycles ++;
		TraceEvent("SimulatedFDBDPreWait").detail("Cycles", cycles).detail("RandomId", randomId)
			.detail("Address", NetworkAddress(ip, port, true, false))
			.detail("ZoneId", localities.zoneId())
			.detail("WaitTime", waitTime).detail("Port", port);

		wait( delay( waitTime ) );

		state ISimulator::ProcessInfo* process =
		    g_simulator.newProcess("Server", ip, port, sslEnabled, listenPerProcess, localities, processClass, dataFolder->c_str(),
		                           coordFolder->c_str());
		wait(g_simulator.onProcess(process,
		                           TaskPriority::DefaultYield)); // Now switch execution to the process on which we will run
		state Future<ISimulator::KillType> onShutdown = process->onShutdown();

		try {
			TraceEvent("SimulatedRebooterStarting").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("ZoneId", localities.zoneId())
				.detail("DataHall", localities.dataHallId())
				.detail("Address", process->address.toString())
				.detail("Excluded", process->excluded)
				.detail("UsingSSL", sslEnabled);
			TraceEvent("ProgramStart").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("SourceVersion", getSourceVersion())
				.detail("Version", FDB_VT_VERSION)
				.detail("PackageName", FDB_VT_PACKAGE_NAME)
				.detail("DataFolder", *dataFolder)
				.detail("ConnectionString", connFile ? connFile->getConnectionString().toString() : "")
				.detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
				.detail("CommandLine", "fdbserver -r simulation")
				.detail("BuggifyEnabled", isBuggifyEnabled(BuggifyType::General))
				.detail("Simulated", true)
				.trackLatest("ProgramStart");

			try {
				//SOMEDAY: test lower memory limits, without making them too small and causing the database to stop making progress
				FlowTransport::createInstance(processClass == ProcessClass::TesterClass || runBackupAgents == AgentOnly, 1);
				Sim2FileSystem::newFileSystem();

				vector<Future<Void>> futures;
				for (int listenPort = port; listenPort < port + listenPerProcess; ++listenPort) {
					NetworkAddress n(ip, listenPort, true, sslEnabled && listenPort == port);
					futures.push_back(FlowTransport::transport().bind( n, n ));
				}
				if(runBackupAgents != AgentOnly) {
					futures.push_back( fdbd( connFile, localities, processClass, *dataFolder, *coordFolder, 500e6, "", "", -1, whitelistBinPaths) );
				}
				if(runBackupAgents != AgentNone) {
					futures.push_back( runBackup(connFile) );
					futures.push_back( runDr(connFile) );
				}

				futures.push_back(success(onShutdown));
				wait( waitForAny(futures) );
			} catch (Error& e) {
				// If in simulation, if we make it here with an error other than io_timeout but enASIOTimedOut is set then somewhere an io_timeout was converted to a different error.
				if(g_network->isSimulated() && e.code() != error_code_io_timeout && (bool)g_network->global(INetwork::enASIOTimedOut))
					TraceEvent(SevError, "IOTimeoutErrorSuppressed").detail("ErrorCode", e.code()).detail("RandomId", randomId).backtrace();

				if (onShutdown.isReady() && onShutdown.isError()) throw onShutdown.getError();
				if(e.code() != error_code_actor_cancelled)
					printf("SimulatedFDBDTerminated: %s\n", e.what());
				ASSERT( destructed || g_simulator.getCurrentProcess() == process ); // simulatedFDBD catch called on different process
				TraceEvent(e.code() == error_code_actor_cancelled || e.code() == error_code_file_not_found || destructed ? SevInfo : SevError, "SimulatedFDBDTerminated").error(e, true).detail("ZoneId", localities.zoneId());
			}

			TraceEvent("SimulatedFDBDDone").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detail("Excluded", process->excluded)
				.detail("ZoneId", localities.zoneId())
				.detail("KillType", onShutdown.isReady() ? onShutdown.get() : ISimulator::None);

			if (!onShutdown.isReady())
				onShutdown = ISimulator::InjectFaults;
		} catch (Error& e) {
			TraceEvent(destructed ? SevInfo : SevError, "SimulatedFDBDRebooterError").error(e, true).detail("ZoneId", localities.zoneId()).detail("RandomId", randomId);
			onShutdown = e;
		}

		ASSERT( destructed || g_simulator.getCurrentProcess() == process );

		if( !process->shutdownSignal.isSet() && !destructed ) {
			process->rebooting = true;
			process->shutdownSignal.send(ISimulator::None);
		}
		TraceEvent("SimulatedFDBDWait").detail("Cycles", cycles).detail("RandomId", randomId)
			.detail("Address", process->address)
			.detail("Excluded", process->excluded)
			.detail("Rebooting", process->rebooting)
			.detail("ZoneId", localities.zoneId());
		wait( g_simulator.onProcess( simProcess ) );

		wait(delay(0.00001 + FLOW_KNOBS->MAX_BUGGIFIED_DELAY));  // One last chance for the process to clean up?

		g_simulator.destroyProcess( process );  // Leak memory here; the process may be used in other parts of the simulation

		auto shutdownResult = onShutdown.get();
		TraceEvent("SimulatedFDBDShutdown").detail("Cycles", cycles).detail("RandomId", randomId)
			.detail("Address", process->address)
			.detail("Excluded", process->excluded)
			.detail("ZoneId", localities.zoneId())
			.detail("KillType", shutdownResult);

		if( shutdownResult < ISimulator::RebootProcessAndDelete ) {
			TraceEvent("SimulatedFDBDLowerReboot").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detail("Excluded", process->excluded)
				.detail("ZoneId", localities.zoneId())
				.detail("KillType", shutdownResult);
			return onShutdown.get();
		}

		if( onShutdown.get() == ISimulator::RebootProcessAndDelete ) {
			TraceEvent("SimulatedFDBDRebootAndDelete").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detail("ZoneId", localities.zoneId())
				.detail("KillType", shutdownResult);
			*coordFolder = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
			*dataFolder = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
			platform::createDirectory( *dataFolder );

			if(!useSeedFile) {
				writeFile(joinPath(*dataFolder, "fdb.cluster"), connStr.toString());
				connFile = Reference<ClusterConnectionFile>( new ClusterConnectionFile( joinPath( *dataFolder, "fdb.cluster" )));
			}
			else {
				connFile = Reference<ClusterConnectionFile>( new ClusterConnectionFile( joinPath( *dataFolder, "fdb.cluster" ), connStr.toString() ) );
			}
		}
		else {
			TraceEvent("SimulatedFDBDJustRepeat").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detail("ZoneId", localities.zoneId())
				.detail("KillType", shutdownResult);
		}
	}
}

template<>
std::string describe(bool const& val) {
	return val ? "true" : "false";
}

template<>
std::string describe(int const& val) {
	return format("%d", val);
}

// Since a datacenter kill is considered to be the same as killing a machine, files cannot be swapped across datacenters
std::map< Optional<Standalone<StringRef>>, std::vector< std::vector< std::string > > > availableFolders;
// process count is no longer needed because it is now the length of the vector of ip's, because it was one ip per process
ACTOR Future<Void> simulatedMachine(ClusterConnectionString connStr, std::vector<IPAddress> ips, bool sslEnabled, LocalityData localities,
                                    ProcessClass processClass, std::string baseFolder, bool restarting,
                                    bool useSeedFile, AgentMode runBackupAgents, bool sslOnly, std::string whitelistBinPaths) {
	state int bootCount = 0;
	state std::vector<std::string> myFolders;
	state std::vector<std::string> coordFolders;
	state UID randomId = nondeterministicRandom()->randomUniqueID();
	state int listenPerProcess = (sslEnabled && !sslOnly) ? 2 : 1;

	try {
		CSimpleIni ini;
		ini.SetUnicode();
		ini.LoadFile(joinPath(baseFolder, "restartInfo.ini").c_str());

		for (int i = 0; i < ips.size(); i++) {
			if (restarting) {
				myFolders.push_back( ini.GetValue(printable(localities.machineId()).c_str(), format("%d", i*listenPerProcess).c_str(), joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()).c_str()) );

				if(i == 0) {
					std::string coordinationFolder = ini.GetValue(printable(localities.machineId()).c_str(), "coordinationFolder", "");
					if(!coordinationFolder.size())
						coordinationFolder = ini.GetValue(printable(localities.machineId()).c_str(), format("c%d", i*listenPerProcess).c_str(), joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()).c_str());
					coordFolders.push_back(coordinationFolder);
				} else {
					coordFolders.push_back( ini.GetValue(printable(localities.machineId()).c_str(), format("c%d", i*listenPerProcess).c_str(), joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()).c_str()) );
				}
			}
			else {
				coordFolders.push_back( joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString()) );
				std::string thisFolder = deterministicRandom()->randomUniqueID().toString();
				myFolders.push_back( joinPath(baseFolder, thisFolder ) );
				platform::createDirectory( myFolders[i] );

				if (!useSeedFile)
					writeFile(joinPath(myFolders[i], "fdb.cluster"), connStr.toString());
			}
		}

		loop {
			state std::vector< Future<ISimulator::KillType> > processes;
			for( int i = 0; i < ips.size(); i++ ) {
				std::string path = joinPath(myFolders[i], "fdb.cluster");
				Reference<ClusterConnectionFile> clusterFile(useSeedFile ? new ClusterConnectionFile(path, connStr.toString()) : new ClusterConnectionFile(path));
				const int listenPort = i*listenPerProcess + 1;
				AgentMode agentMode = runBackupAgents == AgentOnly ? ( i == ips.size()-1 ? AgentOnly : AgentNone ) : runBackupAgents;
				processes.push_back(simulatedFDBDRebooter(clusterFile, ips[i], sslEnabled, listenPort, listenPerProcess, localities, processClass, &myFolders[i], &coordFolders[i], baseFolder, connStr, useSeedFile, agentMode, whitelistBinPaths));
				TraceEvent("SimulatedMachineProcess", randomId).detail("Address", NetworkAddress(ips[i], listenPort, true, false)).detail("ZoneId", localities.zoneId()).detail("DataHall", localities.dataHallId()).detail("Folder", myFolders[i]);
			}

			TEST( bootCount >= 1 ); // Simulated machine rebooted
			TEST( bootCount >= 2 ); // Simulated machine rebooted twice
			TEST( bootCount >= 3 ); // Simulated machine rebooted three times
			++bootCount;

			TraceEvent("SimulatedMachineStart", randomId)
				.detail("Folder0", myFolders[0])
				.detail("CFolder0", coordFolders[0])
				.detail("MachineIPs", toIPVectorString(ips))
				.detail("SSL", sslEnabled)
				.detail("Processes", processes.size())
				.detail("BootCount", bootCount)
				.detail("ProcessClass", processClass.toString())
				.detail("Restarting", restarting)
				.detail("UseSeedFile", useSeedFile)
				.detail("ZoneId", localities.zoneId())
				.detail("DataHall", localities.dataHallId())
				.detail("Locality", localities.toString());

			wait( waitForAll( processes ) );

			TraceEvent("SimulatedMachineRebootStart", randomId)
				.detail("Folder0", myFolders[0])
				.detail("CFolder0", coordFolders[0])
				.detail("MachineIPs", toIPVectorString(ips))
				.detail("ZoneId", localities.zoneId())
				.detail("DataHall", localities.dataHallId());

			{
				//Kill all open files, which may cause them to write invalid data.
				auto& machineCache = g_simulator.getMachineById(localities.machineId())->openFiles;

				//Copy the file pointers to a vector because the map may be modified while we are killing files
				std::vector<AsyncFileNonDurable*> files;
				for(auto fileItr = machineCache.begin(); fileItr != machineCache.end(); ++fileItr) {
					ASSERT( fileItr->second.isReady() );
					files.push_back( (AsyncFileNonDurable*)fileItr->second.get().getPtr() );
				}

				std::vector<Future<Void>> killFutures;
				for(auto fileItr = files.begin(); fileItr != files.end(); ++fileItr)
					killFutures.push_back((*fileItr)->kill());

				wait( waitForAll( killFutures ) );
			}

			state std::set<std::string> filenames;
			state std::string closingStr;
			auto& machineCache = g_simulator.getMachineById(localities.machineId())->openFiles;
			for( auto it : machineCache ) {
				filenames.insert( it.first );
				closingStr += it.first + ", ";
				ASSERT( it.second.isReady() && !it.second.isError() );
			}

			for( auto it : g_simulator.getMachineById(localities.machineId())->deletingFiles ) {
				filenames.insert( it );
				closingStr += it + ", ";
			}

			TraceEvent("SimulatedMachineRebootAfterKills", randomId)
				.detail("Folder0", myFolders[0])
				.detail("CFolder0", coordFolders[0])
				.detail("MachineIPs", toIPVectorString(ips))
				.detail("Closing", closingStr)
				.detail("ZoneId", localities.zoneId())
				.detail("DataHall", localities.dataHallId());

			ISimulator::MachineInfo* machine = g_simulator.getMachineById(localities.machineId());
			machine->closingFiles = filenames;
			g_simulator.getMachineById(localities.machineId())->openFiles.clear();

			// During a reboot:
			//   The process is expected to close all files and be inactive in zero time, but not necessarily
			//   without delay(0)-equivalents, so delay(0) a few times waiting for it to achieve that goal.
			// After an injected fault:
			//   The process is expected to shut down eventually, but not necessarily instantly.  Wait up to 60 seconds.
			state int shutdownDelayCount = 0;
			state double backoff = 0;
			loop {
				auto& machineCache = g_simulator.getMachineById(localities.machineId())->closingFiles;

				if( !machineCache.empty() ) {
					std::string openFiles;
					int i = 0;
					for( auto it = machineCache.begin(); it != machineCache.end() && i < 5; ++it ) {
						openFiles += *it + ", ";
						i++;
					}
					TraceEvent("MachineFilesOpen", randomId).detail("PAddr", toIPVectorString(ips)).detail("OpenFiles", openFiles);
				} else
					break;

				if( shutdownDelayCount++ >= 50 ) {  // Worker doesn't shut down instantly on reboot
					TraceEvent(SevError, "SimulatedFDBDFilesCheck", randomId)
						.detail("PAddrs", toIPVectorString(ips))
						.detail("ZoneId", localities.zoneId())
						.detail("DataHall", localities.dataHallId());
					ASSERT( false );
				}

				wait( delay( backoff ) );
				backoff = std::min( backoff + 1.0, 6.0 );
			}

			TraceEvent("SimulatedFDBDFilesClosed", randomId)
				.detail("Address", toIPVectorString(ips))
				.detail("ZoneId", localities.zoneId())
				.detail("DataHall", localities.dataHallId());

			g_simulator.destroyMachine(localities.machineId());

			// SOMEDAY: when processes can be rebooted, this check will be needed
			//ASSERT( this machine is rebooting );

			// Since processes can end with different codes, take the highest (least severe) to detmine what to do
			state ISimulator::KillType killType = processes[0].get();
			for( int i = 1; i < ips.size(); i++ )
				killType = std::max( processes[i].get(), killType );

			TEST( true ); // Simulated machine has been rebooted

			state bool swap = killType == ISimulator::Reboot && BUGGIFY_WITH_PROB(0.75) && g_simulator.canSwapToMachine( localities.zoneId() );
			if( swap )
				availableFolders[localities.dcId()].push_back( myFolders );

			auto rebootTime = deterministicRandom()->random01() * MACHINE_REBOOT_TIME;

			TraceEvent("SimulatedMachineShutdown", randomId)
				.detail("Swap", swap)
				.detail("KillType", killType)
				.detail("RebootTime", rebootTime)
				.detail("ZoneId", localities.zoneId())
				.detail("DataHall", localities.dataHallId())
				.detail("MachineIPs", toIPVectorString(ips));

			wait( delay( rebootTime ) );

			if( swap ) {
				auto& avail = availableFolders[localities.dcId()];
				int i = deterministicRandom()->randomInt(0, avail.size());
				if( i != avail.size() - 1 )
					std::swap( avail[i], avail.back() );
				auto toRebootFrom = avail.back();
				avail.pop_back();

				if( myFolders != toRebootFrom ) {
					TEST( true ); // Simulated machine swapped data folders
					TraceEvent("SimulatedMachineFolderSwap", randomId)
						.detail("OldFolder0", myFolders[0]).detail("NewFolder0", toRebootFrom[0])
						.detail("MachineIPs", toIPVectorString(ips));
				}
				myFolders = toRebootFrom;
				if(!useSeedFile) {
					for(auto f : toRebootFrom) {
						if(!fileExists(joinPath(f, "fdb.cluster"))) {
							writeFile(joinPath(f, "fdb.cluster"), connStr.toString());
						}
					}
				}
			} else if( killType == ISimulator::RebootAndDelete ) {
				for( int i = 0; i < ips.size(); i++ ) {
					coordFolders[i] = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
					myFolders[i] = joinPath(baseFolder, deterministicRandom()->randomUniqueID().toString());
					platform::createDirectory( myFolders[i] );

					if(!useSeedFile) {
						writeFile(joinPath(myFolders[i], "fdb.cluster"), connStr.toString());
					}
				}
				// Do we ever test this?
				TEST( true ); // Simulated machine rebooted with data loss
			}

			//this machine is rebooting = false;
		}
	} catch( Error &e ) {
		g_simulator.getMachineById(localities.machineId())->openFiles.clear();
		throw;
	}
}

IPAddress makeIPAddressForSim(bool isIPv6, std::array<int, 4> parts) {
	if (isIPv6) {
		IPAddress::IPAddressStore addrStore{ 0xAB, 0xCD };
		uint16_t* ptr = (uint16_t*)addrStore.data();
		ptr[4] = (uint16_t)(parts[0] << 8);
		ptr[5] = (uint16_t)(parts[1] << 8);
		ptr[6] = (uint16_t)(parts[2] << 8);
		ptr[7] = (uint16_t)(parts[3] << 8);
		return IPAddress(addrStore);
	} else {
		return IPAddress(parts[0] << 24 | parts[1] << 16 | parts[2] << 8 | parts[3]);
	}
}

#include "fdbclient/MonitorLeader.h"

ACTOR Future<Void> restartSimulatedSystem(vector<Future<Void>>* systemActors, std::string baseFolder, int* pTesterCount,
                                          Optional<ClusterConnectionString>* pConnString,
                                          Standalone<StringRef>* pStartingConfiguration,
                                          int extraDB, std::string whitelistBinPaths) {
	CSimpleIni ini;
	ini.SetUnicode();
	ini.LoadFile(joinPath(baseFolder, "restartInfo.ini").c_str());

	// allows multiple ipAddr entries
	ini.SetMultiKey();

	try {
		int machineCount = atoi(ini.GetValue("META", "machineCount"));
		int processesPerMachine = atoi(ini.GetValue("META", "processesPerMachine"));
		int listenersPerProcess = 1;
		auto listenersPerProcessStr = ini.GetValue("META", "listenersPerProcess");
		if(listenersPerProcessStr != NULL) {
			listenersPerProcess = atoi(listenersPerProcessStr);
		}
		int desiredCoordinators = atoi(ini.GetValue("META", "desiredCoordinators"));
		int testerCount = atoi(ini.GetValue("META", "testerCount"));
		bool enableExtraDB = (extraDB == 3);
		ClusterConnectionString conn(ini.GetValue("META", "connectionString"));
		if (enableExtraDB) {
			g_simulator.extraDB = new ClusterConnectionString(ini.GetValue("META", "connectionString"));
		}
		*pConnString = conn;
		*pTesterCount = testerCount;
		bool usingSSL = conn.toString().find(":tls") != std::string::npos || listenersPerProcess > 1;
		int useSeedForMachine = deterministicRandom()->randomInt(0, machineCount);
		std::vector<std::string> dcIds;
		for( int i = 0; i < machineCount; i++) {
			Optional<Standalone<StringRef>> dcUID;
			Optional<Standalone<StringRef>> zoneId;
			std::string machineIdString = ini.GetValue("META", format("%d", i).c_str());
			Standalone<StringRef> machineId = StringRef(machineIdString);

			std::string	dcUIDini = ini.GetValue(machineIdString.c_str(), "dcUID");
			if (!dcUIDini.empty()) {
				dcUID = StringRef(dcUIDini);
			}

			auto zoneIDini = ini.GetValue(machineIdString.c_str(), "zoneId");
			if( zoneIDini == NULL ) {
				zoneId = machineId;
			} else {
				zoneId = StringRef(zoneIDini);
			}

			ProcessClass processClass = ProcessClass((ProcessClass::ClassType)atoi(ini.GetValue(machineIdString.c_str(), "mClass")), ProcessClass::CommandLineSource);
			if(processClass != ProcessClass::TesterClass) {
				dcIds.push_back(dcUIDini);
			}

			std::vector<IPAddress> ipAddrs;
			int processes = atoi(ini.GetValue(machineIdString.c_str(), "processes"));

			auto ip = ini.GetValue(machineIdString.c_str(), "ipAddr");

			// Helper to translate the IP address stored in INI file to out IPAddress representation.
			// After IPv6 work, we store the actual string representation of IP address, however earlier, it was
			// instead the 32 bit integer value.
			auto parseIp = [](const char* ipStr) -> IPAddress {
				Optional<IPAddress> parsedIp = IPAddress::parse(ipStr);
				if (parsedIp.present()) {
					return parsedIp.get();
				} else {
					return IPAddress(strtoul(ipStr, NULL, 10));
				}
			};

			if( ip == NULL ) {
				for (int i = 0; i < processes; i++) {
					const char* val =
					    ini.GetValue(machineIdString.c_str(), format("ipAddr%d", i * listenersPerProcess).c_str());
					ipAddrs.push_back(parseIp(val));
				}
			}
			else {
				// old way
				ipAddrs.push_back(parseIp(ip));

				for (int i = 1; i < processes; i++){
					if (ipAddrs.back().isV6()) {
						IPAddress::IPAddressStore store = ipAddrs.back().toV6();
						uint16_t* ptr = (uint16_t*)store.data();
						ptr[7] += 1;
						ipAddrs.push_back(IPAddress(store));
					} else {
						ipAddrs.push_back(IPAddress(ipAddrs.back().toV4() + 1));
					}
				}
			}

			LocalityData	localities(Optional<Standalone<StringRef>>(), zoneId, machineId, dcUID);
			localities.set(LiteralStringRef("data_hall"), dcUID);

			// SOMEDAY: parse backup agent from test file
			systemActors->push_back(reportErrors(
			    simulatedMachine(conn, ipAddrs, usingSSL, localities, processClass, baseFolder, true,
			                     i == useSeedForMachine, enableExtraDB ? AgentAddition : AgentNone,
			                     usingSSL && (listenersPerProcess == 1 || processClass == ProcessClass::TesterClass), whitelistBinPaths),
			    processClass == ProcessClass::TesterClass ? "SimulatedTesterMachine" : "SimulatedMachine"));
		}

		g_simulator.desiredCoordinators = desiredCoordinators;
		g_simulator.processesPerMachine = processesPerMachine;

		uniquify(dcIds);
		if(!BUGGIFY && dcIds.size() == 2 && dcIds[0] != "" && dcIds[1] != "") {
			StatusObject primaryObj;
			StatusObject primaryDcObj;
			primaryDcObj["id"] = dcIds[0];
			primaryDcObj["priority"] = 2;
			StatusArray primaryDcArr;
			primaryDcArr.push_back(primaryDcObj);

			StatusObject remoteObj;
			StatusObject remoteDcObj;
			remoteDcObj["id"] = dcIds[1];
			remoteDcObj["priority"] = 1;
			StatusArray remoteDcArr;
			remoteDcArr.push_back(remoteDcObj);

			primaryObj["datacenters"] = primaryDcArr;
			remoteObj["datacenters"] = remoteDcArr;

			StatusArray regionArr;
			regionArr.push_back(primaryObj);
			regionArr.push_back(remoteObj);

			*pStartingConfiguration = "single usable_regions=2 regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none);
		}

		TraceEvent("RestartSimulatorSettings")
		.detail("DesiredCoordinators", g_simulator.desiredCoordinators)
		.detail("ProcessesPerMachine", g_simulator.processesPerMachine)
		.detail("ListenersPerProcess", listenersPerProcess);
	}
	catch (Error& e) {
		TraceEvent(SevError, "RestartSimulationError").error(e);
	}

	wait(delay(1.0));

	return Void();
}

struct SimulationConfig {
	explicit SimulationConfig(int extraDB, int minimumReplication, int minimumRegions);
	int extraDB;

	DatabaseConfiguration db;

	void set_config(std::string config);

	// Simulation layout
	int datacenters;
	int machine_count;  // Total, not per DC.
	int processes_per_machine;
	int coordinators;
private:
	void generateNormalConfig(int minimumReplication, int minimumRegions);
};

SimulationConfig::SimulationConfig(int extraDB, int minimumReplication, int minimumRegions) : extraDB(extraDB) {
	generateNormalConfig(minimumReplication, minimumRegions);
}

void SimulationConfig::set_config(std::string config) {
	// The only mechanism we have for turning "single" into what single means
	// is buildConfiguration()... :/
	std::map<std::string, std::string> hack_map;
	ASSERT( buildConfiguration(config, hack_map) );
	for(auto kv : hack_map) db.set( kv.first, kv.second );
}

StringRef StringRefOf(const char* s) {
  return StringRef((uint8_t*)s, strlen(s));
}

void SimulationConfig::generateNormalConfig(int minimumReplication, int minimumRegions) {
	set_config("new");
	const bool simple = false;  // Set true to simplify simulation configs for easier debugging
	// generateMachineTeamTestConfig set up the number of servers per machine and the number of machines such that
	// if we do not remove the surplus server and machine teams, the simulation test will report error.
	// This is needed to make sure the number of server (and machine) teams is no larger than the desired number.
	bool generateMachineTeamTestConfig = BUGGIFY_WITH_PROB(0.1) ? true : false;
	bool generateFearless = simple ? false : (minimumRegions > 1 || deterministicRandom()->random01() < 0.5);
	datacenters = simple ? 1 : ( generateFearless ? ( minimumReplication > 0 || deterministicRandom()->random01() < 0.5 ? 4 : 6 ) : deterministicRandom()->randomInt( 1, 4 ) );
	if (deterministicRandom()->random01() < 0.25) db.desiredTLogCount = deterministicRandom()->randomInt(1,7);
	if (deterministicRandom()->random01() < 0.25) db.masterProxyCount = deterministicRandom()->randomInt(1,7);
	if (deterministicRandom()->random01() < 0.25) db.resolverCount = deterministicRandom()->randomInt(1,7);
	int storage_engine_type = deterministicRandom()->randomInt(0, 4);
	switch (storage_engine_type) {
	case 0: {
		TEST(true); // Simulated cluster using ssd storage engine
		set_config("ssd");
		break;
	}
	case 1: {
		TEST(true); // Simulated cluster using default memory storage engine
		set_config("memory");
		break;
	}
	case 2: {
		TEST(true); // Simulated cluster using radix-tree storage engine
		set_config("memory-radixtree-beta");
		break;
	}
	case 3: {
		TEST(true); // Simulated cluster using radix-tree storage engine
		set_config("ssd-redwood-experimental");
		break;
		}
	default:
		ASSERT(false); // Programmer forgot to adjust cases.
	}
	//	if (deterministicRandom()->random01() < 0.5) {
	//		set_config("ssd");
	//	} else {
	//		set_config("memory");
	//	}
	//	set_config("memory");
	//  set_config("memory-radixtree-beta");
	if(simple) {
		db.desiredTLogCount = 1;
		db.masterProxyCount = 1;
		db.resolverCount = 1;
	}
	int replication_type = simple ? 1 : ( std::max(minimumReplication, datacenters > 4 ? deterministicRandom()->randomInt(1,3) : std::min(deterministicRandom()->randomInt(0,6), 3)) );
	switch (replication_type) {
	case 0: {
		TEST( true );  // Simulated cluster using custom redundancy mode
		int storage_servers = deterministicRandom()->randomInt(1, generateFearless ? 4 : 5);
		//FIXME: log replicas must be more than storage replicas because otherwise better master exists will not recognize it needs to change dcs
		int replication_factor = deterministicRandom()->randomInt(storage_servers, generateFearless ? 4 : 5);
		int anti_quorum = deterministicRandom()->randomInt(0, (replication_factor/2) + 1); //The anti quorum cannot be more than half of the replication factor, or the log system will continue to accept commits when a recovery is impossible
		// Go through buildConfiguration, as it sets tLogPolicy/storagePolicy.
		set_config(format("storage_replicas:=%d log_replicas:=%d log_anti_quorum:=%d "
		                  "replica_datacenters:=1 min_replica_datacenters:=1",
		                  storage_servers, replication_factor, anti_quorum));
		break;
	}
	case 1: {
		TEST( true );  // Simulated cluster running in single redundancy mode
		set_config("single");
		break;
	}
	case 2: {
		TEST( true );  // Simulated cluster running in double redundancy mode
		set_config("double");
		break;
	}
	case 3: {
		if( datacenters <= 2 || generateFearless ) {
			TEST( true );  // Simulated cluster running in triple redundancy mode
			set_config("triple");
		}
		else if( datacenters == 3 ) {
			TEST( true );  // Simulated cluster running in 3 data-hall mode
			set_config("three_data_hall");
		}
		else {
			ASSERT( false );
		}
		break;
	}
	default:
		ASSERT(false);  // Programmer forgot to adjust cases.
	}

	if (deterministicRandom()->random01() < 0.5) {
		int logSpill = deterministicRandom()->randomInt( TLogSpillType::VALUE, TLogSpillType::END );
		set_config(format("log_spill:=%d", logSpill));
		int logVersion = deterministicRandom()->randomInt( TLogVersion::MIN_RECRUITABLE, TLogVersion::MAX_SUPPORTED+1 );
		set_config(format("log_version:=%d", logVersion));
	} else {
		if (deterministicRandom()->random01() < 0.7)
			set_config(format("log_version:=%d", TLogVersion::MAX_SUPPORTED));
		if (deterministicRandom()->random01() < 0.5)
			set_config(format("log_spill:=%d", TLogSpillType::DEFAULT));
	}

	if (deterministicRandom()->random01() < 0.5) {
		set_config("backup_worker_enabled:=1");
	}

	if(generateFearless || (datacenters == 2 && deterministicRandom()->random01() < 0.5)) {
		//The kill region workload relies on the fact that all "0", "2", and "4" are all of the possible primary dcids.
		StatusObject primaryObj;
		StatusObject primaryDcObj;
		primaryDcObj["id"] = "0";
		primaryDcObj["priority"] = 2;
		StatusArray primaryDcArr;
		primaryDcArr.push_back(primaryDcObj);

		StatusObject remoteObj;
		StatusObject remoteDcObj;
		remoteDcObj["id"] = "1";
		remoteDcObj["priority"] = 1;
		StatusArray remoteDcArr;
		remoteDcArr.push_back(remoteDcObj);

		bool needsRemote = generateFearless;
		if(generateFearless) {
			if(datacenters > 4) {
				//FIXME: we cannot use one satellite replication with more than one satellite per region because canKillProcesses does not respect usable_dcs
				int satellite_replication_type = deterministicRandom()->randomInt(0,3);
				switch (satellite_replication_type) {
				case 0: {
					TEST( true );  // Simulated cluster using no satellite redundancy mode
					break;
				}
				case 1: {
					TEST( true );  // Simulated cluster using two satellite fast redundancy mode
					primaryObj["satellite_redundancy_mode"] = "two_satellite_fast";
					remoteObj["satellite_redundancy_mode"] = "two_satellite_fast";
					break;
				}
				case 2: {
					TEST( true );  // Simulated cluster using two satellite safe redundancy mode
					primaryObj["satellite_redundancy_mode"] = "two_satellite_safe";
					remoteObj["satellite_redundancy_mode"] = "two_satellite_safe";
					break;
				}
				default:
					ASSERT(false);  // Programmer forgot to adjust cases.
				}
			} else {
				int satellite_replication_type = deterministicRandom()->randomInt(0,5);
				switch (satellite_replication_type) {
				case 0: {
					//FIXME: implement
					TEST( true );  // Simulated cluster using custom satellite redundancy mode
					break;
				}
				case 1: {
					TEST( true );  // Simulated cluster using no satellite redundancy mode
					break;
				}
				case 2: {
					TEST( true );  // Simulated cluster using single satellite redundancy mode
					primaryObj["satellite_redundancy_mode"] = "one_satellite_single";
					remoteObj["satellite_redundancy_mode"] = "one_satellite_single";
					break;
				}
				case 3: {
					TEST( true );  // Simulated cluster using double satellite redundancy mode
					primaryObj["satellite_redundancy_mode"] = "one_satellite_double";
					remoteObj["satellite_redundancy_mode"] = "one_satellite_double";
					break;
				}
				case 4: {
					TEST( true );  // Simulated cluster using triple satellite redundancy mode
					primaryObj["satellite_redundancy_mode"] = "one_satellite_triple";
					remoteObj["satellite_redundancy_mode"] = "one_satellite_triple";
					break;
				}
				default:
					ASSERT(false);  // Programmer forgot to adjust cases.
				}
			}

			if (deterministicRandom()->random01() < 0.25) primaryObj["satellite_logs"] =  deterministicRandom()->randomInt(1,7);
			if (deterministicRandom()->random01() < 0.25) remoteObj["satellite_logs"] =  deterministicRandom()->randomInt(1,7);

			// Q: MAX_READ_TRANSACTION_LIFE_VERSIONS can not be configured to be small value in HA setting? Then we
			// should sanity check this in ManagementAPI? Q: Why log routers cannot catch up?
			// We cannot run with a remote DC when MAX_READ_TRANSACTION_LIFE_VERSIONS is too small, because the log
			// routers will not be able to keep up.
			if (minimumRegions <= 1 && (deterministicRandom()->random01() < 0.25 || SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS < SERVER_KNOBS->VERSIONS_PER_SECOND)) {
				TEST( true );  // Simulated cluster using one region
				needsRemote = false;
			} else {
				TEST( true );  // Simulated cluster using two regions
				db.usableRegions = 2;
			}

			int remote_replication_type = deterministicRandom()->randomInt(0, datacenters > 4 ? 4 : 5);
			switch (remote_replication_type) {
			case 0: {
				//FIXME: implement
				TEST( true );  // Simulated cluster using custom remote redundancy mode
				break;
			}
			case 1: {
				TEST( true );  // Simulated cluster using default remote redundancy mode
				break;
			}
			case 2: {
				TEST( true );  // Simulated cluster using single remote redundancy mode
				set_config("remote_single");
				break;
			}
			case 3: {
				TEST( true );  // Simulated cluster using double remote redundancy mode
				set_config("remote_double");
				break;
			}
			case 4: {
				TEST( true );  // Simulated cluster using triple remote redundancy mode
				set_config("remote_triple");
				break;
			}
			default:
				ASSERT(false);  // Programmer forgot to adjust cases.
			}

			if (deterministicRandom()->random01() < 0.25) db.desiredLogRouterCount = deterministicRandom()->randomInt(1,7);
			if (deterministicRandom()->random01() < 0.25) db.remoteDesiredTLogCount = deterministicRandom()->randomInt(1,7);

			bool useNormalDCsAsSatellites = datacenters > 4 && minimumRegions < 2 && deterministicRandom()->random01() < 0.3;
			StatusObject primarySatelliteObj;
			primarySatelliteObj["id"] = useNormalDCsAsSatellites ? "1" : "2";
			primarySatelliteObj["priority"] = 1;
			primarySatelliteObj["satellite"] = 1;
			if (deterministicRandom()->random01() < 0.25) primarySatelliteObj["satellite_logs"] = deterministicRandom()->randomInt(1,7);
			primaryDcArr.push_back(primarySatelliteObj);

			StatusObject remoteSatelliteObj;
			remoteSatelliteObj["id"] = useNormalDCsAsSatellites ? "0" : "3";
			remoteSatelliteObj["priority"] = 1;
			remoteSatelliteObj["satellite"] = 1;
			if (deterministicRandom()->random01() < 0.25) remoteSatelliteObj["satellite_logs"] = deterministicRandom()->randomInt(1,7);
			remoteDcArr.push_back(remoteSatelliteObj);

			if (datacenters > 4) {
				StatusObject primarySatelliteObjB;
				primarySatelliteObjB["id"] = useNormalDCsAsSatellites ? "2" : "4";
				primarySatelliteObjB["priority"] = 1;
				primarySatelliteObjB["satellite"] = 1;
				if (deterministicRandom()->random01() < 0.25) primarySatelliteObjB["satellite_logs"] = deterministicRandom()->randomInt(1,7);
				primaryDcArr.push_back(primarySatelliteObjB);

				StatusObject remoteSatelliteObjB;
				remoteSatelliteObjB["id"] = useNormalDCsAsSatellites ? "2" : "5";
				remoteSatelliteObjB["priority"] = 1;
				remoteSatelliteObjB["satellite"] = 1;
				if (deterministicRandom()->random01() < 0.25) remoteSatelliteObjB["satellite_logs"] = deterministicRandom()->randomInt(1,7);
				remoteDcArr.push_back(remoteSatelliteObjB);
			}
			if (useNormalDCsAsSatellites) {
				datacenters = 3;
			}
		}

		primaryObj["datacenters"] = primaryDcArr;
		remoteObj["datacenters"] = remoteDcArr;

		StatusArray regionArr;
		regionArr.push_back(primaryObj);
		if(needsRemote || deterministicRandom()->random01() < 0.5) {
			regionArr.push_back(remoteObj);
		}

		set_config("regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none));

		if(needsRemote) {
			g_simulator.originalRegions = "regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none);

			StatusArray disablePrimary = regionArr;
			disablePrimary[0].get_obj()["datacenters"].get_array()[0].get_obj()["priority"] = -1;
			g_simulator.disablePrimary = "regions=" + json_spirit::write_string(json_spirit::mValue(disablePrimary), json_spirit::Output_options::none);

			StatusArray disableRemote = regionArr;
			disableRemote[1].get_obj()["datacenters"].get_array()[0].get_obj()["priority"] = -1;
			g_simulator.disableRemote = "regions=" + json_spirit::write_string(json_spirit::mValue(disableRemote), json_spirit::Output_options::none);
		}
	}

	if(generateFearless && minimumReplication > 1) {
		//low latency tests in fearless configurations need 4 machines per datacenter (3 for triple replication, 1 that is down during failures).
		machine_count = 16;
	} else if(generateFearless) {
		machine_count = 12;
	} else if(db.tLogPolicy && db.tLogPolicy->info() == "data_hall^2 x zoneid^2 x 1") {
		machine_count = 9;
	} else {
		//datacenters+2 so that the configure database workload can configure into three_data_hall
		machine_count = std::max(datacenters+2, ((db.minDatacentersRequired() > 0) ? datacenters : 1) * std::max(3, db.minZonesRequiredPerDatacenter()));
		machine_count = deterministicRandom()->randomInt( machine_count, std::max(machine_count+1, extraDB ? 6 : 10) );
		if (generateMachineTeamTestConfig) {
			// When DESIRED_TEAMS_PER_SERVER is set to 1, the desired machine team number is 5
			// while the max possible machine team number is 10.
			// If machine_count > 5, we can still test the effectivenss of machine teams
			// Note: machine_count may be much larger than 5 because we may have a big replication factor
			machine_count = std::max(machine_count, deterministicRandom()->randomInt(5, extraDB ? 6 : 10));
		}
	}

	//because we protect a majority of coordinators from being killed, it is better to run with low numbers of coordinators to prevent too many processes from being protected
	coordinators = ( minimumRegions <= 1 && BUGGIFY ) ? deterministicRandom()->randomInt(1, std::max(machine_count,2)) : 1;

	if(minimumReplication > 1 && datacenters == 3) {
		//low latency tests in 3 data hall mode need 2 other data centers with 2 machines each to avoid waiting for logs to recover.
		machine_count = std::max( machine_count, 6);
		coordinators = 3;
	}

	if(generateFearless) {
		processes_per_machine = 1;
	} else {
		processes_per_machine = deterministicRandom()->randomInt(1, (extraDB ? 14 : 28)/machine_count + 2 );
	}
}

void setupSimulatedSystem(vector<Future<Void>>* systemActors, std::string baseFolder, int* pTesterCount,
                          Optional<ClusterConnectionString>* pConnString, Standalone<StringRef>* pStartingConfiguration,
                          int extraDB, int minimumReplication, int minimumRegions, std::string whitelistBinPaths, bool configureLocked) {
	// SOMEDAY: this does not test multi-interface configurations
	SimulationConfig simconfig(extraDB, minimumReplication, minimumRegions);
	StatusObject startingConfigJSON = simconfig.db.toJSON(true);
	std::string startingConfigString = "new";
	if (configureLocked) {
		startingConfigString += " locked";
	}
	for( auto kv : startingConfigJSON) {
		startingConfigString += " ";
		if( kv.second.type() == json_spirit::int_type ) {
			startingConfigString += kv.first + ":=" + format("%d", kv.second.get_int());
		} else if( kv.second.type() == json_spirit::str_type ) {
			startingConfigString += kv.second.get_str();
		} else if( kv.second.type() == json_spirit::array_type ) {
			startingConfigString += kv.first + "=" + json_spirit::write_string(json_spirit::mValue(kv.second.get_array()), json_spirit::Output_options::none);
		} else {
			ASSERT(false);
		}
	}

	g_simulator.storagePolicy = simconfig.db.storagePolicy;
	g_simulator.tLogPolicy = simconfig.db.tLogPolicy;
	g_simulator.tLogWriteAntiQuorum = simconfig.db.tLogWriteAntiQuorum;
	g_simulator.remoteTLogPolicy = simconfig.db.getRemoteTLogPolicy();
	g_simulator.usableRegions = simconfig.db.usableRegions;

	if(simconfig.db.regions.size() > 0) {
		g_simulator.primaryDcId = simconfig.db.regions[0].dcId;
		g_simulator.hasSatelliteReplication = simconfig.db.regions[0].satelliteTLogReplicationFactor > 0;
		if(simconfig.db.regions[0].satelliteTLogUsableDcsFallback > 0) {
			g_simulator.satelliteTLogPolicyFallback = simconfig.db.regions[0].satelliteTLogPolicyFallback;
			g_simulator.satelliteTLogWriteAntiQuorumFallback = simconfig.db.regions[0].satelliteTLogWriteAntiQuorumFallback;
		} else {
			g_simulator.satelliteTLogPolicyFallback = simconfig.db.regions[0].satelliteTLogPolicy;
			g_simulator.satelliteTLogWriteAntiQuorumFallback = simconfig.db.regions[0].satelliteTLogWriteAntiQuorum;
		}
		g_simulator.satelliteTLogPolicy = simconfig.db.regions[0].satelliteTLogPolicy;
		g_simulator.satelliteTLogWriteAntiQuorum = simconfig.db.regions[0].satelliteTLogWriteAntiQuorum;

		for(auto s : simconfig.db.regions[0].satellites) {
			g_simulator.primarySatelliteDcIds.push_back(s.dcId);
		}
	} else {
		g_simulator.hasSatelliteReplication = false;
		g_simulator.satelliteTLogWriteAntiQuorum = 0;
	}

	if(simconfig.db.regions.size() == 2) {
		g_simulator.remoteDcId = simconfig.db.regions[1].dcId;
		ASSERT((!simconfig.db.regions[0].satelliteTLogPolicy && !simconfig.db.regions[1].satelliteTLogPolicy) || simconfig.db.regions[0].satelliteTLogPolicy->info() == simconfig.db.regions[1].satelliteTLogPolicy->info());

		for(auto s : simconfig.db.regions[1].satellites) {
			g_simulator.remoteSatelliteDcIds.push_back(s.dcId);
		}
	}

	if(g_simulator.usableRegions < 2 || !g_simulator.hasSatelliteReplication) {
		g_simulator.allowLogSetKills = false;
	}

	ASSERT(g_simulator.storagePolicy && g_simulator.tLogPolicy);
	ASSERT(!g_simulator.hasSatelliteReplication || g_simulator.satelliteTLogPolicy);
	TraceEvent("SimulatorConfig").detail("ConfigString", StringRef(startingConfigString));

	const int dataCenters = simconfig.datacenters;
	const int machineCount = simconfig.machine_count;
	const int coordinatorCount = simconfig.coordinators;
	const int processesPerMachine = simconfig.processes_per_machine;

	// half the time, when we have more than 4 machines that are not the first in their dataCenter, assign classes
	bool assignClasses = machineCount - dataCenters > 4 && deterministicRandom()->random01() < 0.5;

	// Use SSL 5% of the time
	bool sslEnabled = deterministicRandom()->random01() < 0.10;
	bool sslOnly = sslEnabled && deterministicRandom()->coinflip();
	g_simulator.listenersPerProcess = sslEnabled && !sslOnly ? 2 : 1;
	TEST( sslEnabled ); // SSL enabled
	TEST( !sslEnabled ); // SSL disabled

	// Use IPv6 25% of the time
	bool useIPv6 = deterministicRandom()->random01() < 0.25;
	TEST( useIPv6 );
	TEST( !useIPv6 );

	vector<NetworkAddress> coordinatorAddresses;
	if(minimumRegions > 1) {
		//do not put coordinators in the primary region so that we can kill that region safely
		int nonPrimaryDcs = dataCenters/2;
		for( int dc = 1; dc < dataCenters; dc+=2 ) {
			int dcCoordinators = coordinatorCount / nonPrimaryDcs + ((dc-1)/2 < coordinatorCount%nonPrimaryDcs);
			for(int m = 0; m < dcCoordinators; m++) {
				auto ip = makeIPAddressForSim(useIPv6, { 2, dc, 1, m });
				coordinatorAddresses.push_back(NetworkAddress(ip, sslEnabled && !sslOnly ? 2 : 1, true, sslEnabled && sslOnly));
				TraceEvent("SelectedCoordinator").detail("Address", coordinatorAddresses.back());
			}
		}
	} else {
		int assignedMachines = 0;
		int coordCount = coordinatorCount;
		if(coordinatorCount>4) {
			++coordCount;
		}
		for( int dc = 0; dc < dataCenters; dc++ ) {
			int dcCoordinators = coordCount / dataCenters + (dc < coordCount%dataCenters);
			int machines = machineCount / dataCenters + (dc < machineCount % dataCenters);
			for(int m = 0; m < dcCoordinators; m++) {
				if(coordinatorCount>4 && (assignedMachines==4 || (m+1==dcCoordinators && assignedMachines<4 && assignedMachines+machines-dcCoordinators>=4))) {
					auto ip = makeIPAddressForSim(useIPv6, { 2, dc, 1, m });
					TraceEvent("SkippedCoordinator")
					    .detail("Address", ip.toString())
					    .detail("M", m)
					    .detail("Machines", machines)
					    .detail("Assigned", assignedMachines)
					    .detail("DcCoord", dcCoordinators)
					    .detail("CoordinatorCount", coordinatorCount);
				} else {
					auto ip = makeIPAddressForSim(useIPv6, { 2, dc, 1, m });
					coordinatorAddresses.push_back(NetworkAddress(ip, sslEnabled && !sslOnly ? 2 : 1, true, sslEnabled && sslOnly));
					TraceEvent("SelectedCoordinator").detail("Address", coordinatorAddresses.back()).detail("M", m).detail("Machines", machines).detail("Assigned", assignedMachines).detail("DcCoord", dcCoordinators).detail("P1", (m+1==dcCoordinators)).detail("P2", (assignedMachines<4)).detail("P3", (assignedMachines+machines-dcCoordinators>=4)).detail("CoordinatorCount", coordinatorCount);
				}
				assignedMachines++;
			}
			assignedMachines += machines-dcCoordinators;
		}
	}

	deterministicRandom()->randomShuffle(coordinatorAddresses);
	for(int i = 0; i < (coordinatorAddresses.size()/2)+1; i++) {
		TraceEvent("ProtectCoordinator")
		    .detail("Address", coordinatorAddresses[i])
		    .detail("Coordinators", describe(coordinatorAddresses));
		g_simulator.protectedAddresses.insert(
		    NetworkAddress(coordinatorAddresses[i].ip, coordinatorAddresses[i].port, true, coordinatorAddresses[i].isTLS()));
		if(coordinatorAddresses[i].port==2) {
			g_simulator.protectedAddresses.insert(NetworkAddress(coordinatorAddresses[i].ip, 1, true, true));
		}
	}
	deterministicRandom()->randomShuffle(coordinatorAddresses);

	ASSERT( coordinatorAddresses.size() == coordinatorCount );
	ClusterConnectionString conn(coordinatorAddresses, LiteralStringRef("TestCluster:0"));

	// If extraDB==0, leave g_simulator.extraDB as null because the test does not use DR.
	if(extraDB==1) {
		// The DR database can be either a new database or itself
		g_simulator.extraDB = new ClusterConnectionString(coordinatorAddresses, BUGGIFY ? LiteralStringRef("TestCluster:0") : LiteralStringRef("ExtraCluster:0"));
	} else if(extraDB==2) {
		// The DR database is a new database
		g_simulator.extraDB = new ClusterConnectionString(coordinatorAddresses, LiteralStringRef("ExtraCluster:0"));
	} else if(extraDB==3) {
		// The DR database is the same database
		g_simulator.extraDB = new ClusterConnectionString(coordinatorAddresses, LiteralStringRef("TestCluster:0"));
	}

	*pConnString = conn;

	TraceEvent("SimulatedConnectionString").detail("String", conn.toString()).detail("ConfigString", startingConfigString);

	bool requiresExtraDBMachines = extraDB && g_simulator.extraDB->toString() != conn.toString();
	int assignedMachines = 0, nonVersatileMachines = 0;
	std::vector<ProcessClass::ClassType> processClassesSubSet = {ProcessClass::UnsetClass, ProcessClass::ResolutionClass, ProcessClass::MasterClass};
	for( int dc = 0; dc < dataCenters; dc++ ) {
		//FIXME: test unset dcID
		Optional<Standalone<StringRef>> dcUID = StringRef(format("%d", dc));
		std::vector<UID> machineIdentities;
		int machines = machineCount / dataCenters + (dc < machineCount % dataCenters); // add remainder of machines to first datacenter
		int dcCoordinators = coordinatorCount / dataCenters + (dc < coordinatorCount%dataCenters);
		printf("Datacenter %d: %d/%d machines, %d/%d coordinators\n", dc, machines, machineCount, dcCoordinators, coordinatorCount);
		ASSERT( dcCoordinators <= machines );

		//FIXME: temporarily code to test storage cache
		//TODO: caching disabled for this merge
		if(dc==0) {
			machines++;
		}

		int useSeedForMachine = deterministicRandom()->randomInt(0, machines);
		Standalone<StringRef> zoneId;
		Standalone<StringRef> newZoneId;
		for( int machine = 0; machine < machines; machine++ ) {
			Standalone<StringRef> machineId(deterministicRandom()->randomUniqueID().toString());
			if(machine == 0 || machineCount - dataCenters <= 4 || assignedMachines != 4 || simconfig.db.regions.size() || deterministicRandom()->random01() < 0.5) {
				zoneId = deterministicRandom()->randomUniqueID().toString();
				newZoneId = deterministicRandom()->randomUniqueID().toString();
			}

			//Choose a machine class
			ProcessClass processClass = ProcessClass(ProcessClass::UnsetClass, ProcessClass::CommandLineSource);
			if(assignClasses) {
				if(assignedMachines < 4)
					processClass = ProcessClass((ProcessClass::ClassType) deterministicRandom()->randomInt(0, 2), ProcessClass::CommandLineSource); //Unset or Storage
				else if(assignedMachines == 4 && !simconfig.db.regions.size())
					processClass = ProcessClass(processClassesSubSet[deterministicRandom()->randomInt(0, processClassesSubSet.size())], ProcessClass::CommandLineSource); //Unset or Resolution or Master
				else
					processClass = ProcessClass((ProcessClass::ClassType) deterministicRandom()->randomInt(0, 3), ProcessClass::CommandLineSource); //Unset, Storage, or Transaction
				if (processClass == ProcessClass::ResolutionClass)  // *can't* be assigned to other roles, even in an emergency
					nonVersatileMachines++;
			}

			//FIXME: temporarily code to test storage cache
			//TODO: caching disabled for this merge
			if(machine==machines-1 && dc==0) {
				processClass = ProcessClass(ProcessClass::StorageCacheClass, ProcessClass::CommandLineSource);
				nonVersatileMachines++;
			}

			std::vector<IPAddress> ips;
			for (int i = 0; i < processesPerMachine; i++) {
				ips.push_back(makeIPAddressForSim(useIPv6, { 2, dc, deterministicRandom()->randomInt(1, i + 2), machine }));
			}
			if(requiresExtraDBMachines) {
				ips.push_back(makeIPAddressForSim(useIPv6, { 2, dc, 1, machine }));
			}

			// check the sslEnablementMap using only one ip(
			LocalityData	localities(Optional<Standalone<StringRef>>(), zoneId, machineId, dcUID);
			localities.set(LiteralStringRef("data_hall"), dcUID);
			systemActors->push_back(reportErrors(simulatedMachine(conn, ips, sslEnabled,
				localities, processClass, baseFolder, false, machine == useSeedForMachine, requiresExtraDBMachines ? AgentOnly : AgentAddition, sslOnly, whitelistBinPaths ), "SimulatedMachine"));

			if (requiresExtraDBMachines) {
				std::vector<IPAddress> extraIps;
				for (int i = 0; i < processesPerMachine; i++){
					extraIps.push_back(makeIPAddressForSim(useIPv6, { 4, dc, deterministicRandom()->randomInt(1, i + 2), machine }));
				}

				Standalone<StringRef> newMachineId(deterministicRandom()->randomUniqueID().toString());

				LocalityData	localities(Optional<Standalone<StringRef>>(), newZoneId, newMachineId, dcUID);
				localities.set(LiteralStringRef("data_hall"), dcUID);
				systemActors->push_back(reportErrors(simulatedMachine(*g_simulator.extraDB, extraIps, sslEnabled,
					localities,
					processClass, baseFolder, false, machine == useSeedForMachine, AgentNone, sslOnly, whitelistBinPaths ), "SimulatedMachine"));
			}

			assignedMachines++;
		}
	}

	g_simulator.desiredCoordinators = coordinatorCount;
	g_simulator.physicalDatacenters = dataCenters;
	g_simulator.processesPerMachine = processesPerMachine;

	TraceEvent("SetupSimulatorSettings")
		.detail("DesiredCoordinators", g_simulator.desiredCoordinators)
		.detail("PhysicalDatacenters", g_simulator.physicalDatacenters)
		.detail("ProcessesPerMachine", g_simulator.processesPerMachine);

	// SOMEDAY: add locality for testers to simulate network topology
	// FIXME: Start workers with tester class instead, at least sometimes run tests with the testers-only flag
	int testerCount = *pTesterCount = deterministicRandom()->randomInt(4, 9);
	int useSeedForMachine = deterministicRandom()->randomInt(0, testerCount);
	for(int i=0; i<testerCount; i++) {
		std::vector<IPAddress> ips;
		ips.push_back(makeIPAddressForSim(useIPv6, { 3, 4, 3, i + 1 }));
		Standalone<StringRef> newZoneId = Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString());
		LocalityData	localities(Optional<Standalone<StringRef>>(), newZoneId, newZoneId, Optional<Standalone<StringRef>>());
		systemActors->push_back( reportErrors( simulatedMachine(
			conn, ips, sslEnabled && sslOnly,
			localities, ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource),
			baseFolder, false, i == useSeedForMachine, AgentNone, sslEnabled && sslOnly, whitelistBinPaths ),
			"SimulatedTesterMachine") );
	}
	*pStartingConfiguration = startingConfigString;

	// save some state that we only need when restarting the simulator.
	g_simulator.connectionString = conn.toString();
	g_simulator.testerCount = testerCount;

	TraceEvent("SimulatedClusterStarted")
		.detail("DataCenters", dataCenters)
		.detail("ServerMachineCount", machineCount)
		.detail("ProcessesPerServer", processesPerMachine)
		.detail("SSLEnabled", sslEnabled)
		.detail("SSLOnly", sslOnly)
		.detail("ClassesAssigned", assignClasses)
		.detail("StartingConfiguration", pStartingConfiguration->toString());
}

void checkTestConf(const char* testFile, int& extraDB, int& minimumReplication, int& minimumRegions,
                   int& configureLocked) {
	std::ifstream ifs;
	ifs.open(testFile, std::ifstream::in);
	if (!ifs.good())
		return;

	std::string cline;

	while (ifs.good()) {
		getline(ifs, cline);
		std::string line = removeWhitespace(std::string(cline));
		if (!line.size() || line.find(';') == 0)
			continue;

		size_t found = line.find('=');
		if (found == std::string::npos)
			// hmmm, not good
			continue;
		std::string attrib = removeWhitespace(line.substr(0, found));
		std::string value = removeWhitespace(line.substr(found + 1));

		if (attrib == "extraDB") {
			sscanf( value.c_str(), "%d", &extraDB );
		}

		if (attrib == "minimumReplication") {
			sscanf( value.c_str(), "%d", &minimumReplication );
		}

		if (attrib == "minimumRegions") {
			sscanf( value.c_str(), "%d", &minimumRegions );
		}

		if (attrib == "configureLocked") {
			sscanf(value.c_str(), "%d", &configureLocked);
		}
	}

	ifs.close();
}

ACTOR void setupAndRun(std::string dataFolder, const char *testFile, bool rebooting, bool restoring, std::string whitelistBinPaths) {
	state vector<Future<Void>> systemActors;
	state Optional<ClusterConnectionString> connFile;
	state Standalone<StringRef> startingConfiguration;
	state int testerCount = 1;
	state int extraDB = 0;
	state int minimumReplication = 0;
	state int minimumRegions = 0;
	state int configureLocked = 0;
	checkTestConf(testFile, extraDB, minimumReplication, minimumRegions, configureLocked);

	// TODO (IPv6) Use IPv6?
	wait(g_simulator.onProcess(
	    g_simulator.newProcess("TestSystem", IPAddress(0x01010101), 1, false, 1,
	                           LocalityData(Optional<Standalone<StringRef>>(),
	                                        Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                                        Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
	                                        Optional<Standalone<StringRef>>()),
	                           ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource), "", ""),
	    TaskPriority::DefaultYield));
	Sim2FileSystem::newFileSystem();
	FlowTransport::createInstance(true, 1);
	TEST(true);  // Simulation start

	try {
		//systemActors.push_back( startSystemMonitor(dataFolder) );
		if (rebooting) {
			wait( timeoutError( restartSimulatedSystem( &systemActors, dataFolder, &testerCount, &connFile, &startingConfiguration, extraDB, whitelistBinPaths), 100.0 ) );
			// FIXME: snapshot restore does not support multi-region restore, hence restore it as single region always
			if (restoring) {
				startingConfiguration = LiteralStringRef("usable_regions=1");
			}
		}
		else {
			g_expect_full_pointermap = 1;
			setupSimulatedSystem(&systemActors, dataFolder, &testerCount, &connFile, &startingConfiguration, extraDB,
			                     minimumReplication, minimumRegions, whitelistBinPaths, configureLocked);
			wait( delay(1.0) ); // FIXME: WHY!!!  //wait for machines to boot
		}
		std::string clusterFileDir = joinPath( dataFolder, deterministicRandom()->randomUniqueID().toString() );
		platform::createDirectory( clusterFileDir );
		writeFile(joinPath(clusterFileDir, "fdb.cluster"), connFile.get().toString());
		wait(timeoutError(runTests(Reference<ClusterConnectionFile>(
									   new ClusterConnectionFile(joinPath(clusterFileDir, "fdb.cluster"))),
								   TEST_TYPE_FROM_FILE, TEST_ON_TESTERS, testerCount, testFile, startingConfiguration),
						  isBuggifyEnabled(BuggifyType::General) ? 36000.0 : 5400.0));
	} catch (Error& e) {
		TraceEvent(SevError, "SetupAndRunError").error(e);
	}

	TraceEvent("SimulatedSystemDestruct");
	g_simulator.stop();
	destructed = true;
	wait(Never());
	ASSERT(false);
}
