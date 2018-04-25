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
#include "flow/actorcompiler.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/FailureMonitorClient.h"
#include "fdbclient/DatabaseContext.h"
#include "TesterInterface.h"
#include "WorkerInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "Knobs.h"
#include "ClusterRecruitmentInterface.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbmonitor/SimpleIni.h"
#include "fdbrpc/AsyncFileNonDurable.actor.h"
#include "fdbrpc/TLSConnection.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/BackupAgent.h"

#ifndef WIN32
#include "versions.h"
#endif

#undef max
#undef min

extern bool buggifyActivated;
extern "C" int g_expect_full_pointermap;
extern const char* getHGVersion();

const int PROCESS_START_TIME = 4;
const int MACHINE_REBOOT_TIME = 10;

bool destructed = false;

static const char* certBytes =
	"-----BEGIN CERTIFICATE-----\n"
	"MIIEGzCCAwOgAwIBAgIJANUQj1rRA2XMMA0GCSqGSIb3DQEBBQUAMIGjMQswCQYD\n"
	"VQQGEwJVUzELMAkGA1UECAwCVkExDzANBgNVBAcMBlZpZW5uYTEaMBgGA1UECgwR\n"
	"Rm91bmRhdGlvbkRCLCBMTEMxGTAXBgNVBAsMEFRlc3QgZW5naW5lZXJpbmcxFTAT\n"
	"BgNVBAMMDE1yLiBCaWcgVHVuYTEoMCYGCSqGSIb3DQEJARYZYmlnLnR1bmFAZm91\n"
	"bmRhdGlvbmRiLmNvbTAeFw0xNDEyMDUxNTEyMjFaFw0yNDEyMDIxNTEyMjFaMIGj\n"
	"MQswCQYDVQQGEwJVUzELMAkGA1UECAwCVkExDzANBgNVBAcMBlZpZW5uYTEaMBgG\n"
	"A1UECgwRRm91bmRhdGlvbkRCLCBMTEMxGTAXBgNVBAsMEFRlc3QgZW5naW5lZXJp\n"
	"bmcxFTATBgNVBAMMDE1yLiBCaWcgVHVuYTEoMCYGCSqGSIb3DQEJARYZYmlnLnR1\n"
	"bmFAZm91bmRhdGlvbmRiLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
	"ggEBAKZTL2edDkiet4HBTZnjysn6gOVZH2MP02KVBIv/H7e+3w7ZOIRvcPzhZe9M\n"
	"3cGH1t/pkr9DSXvzIb42EffMVlpLD2VQn2H8VC2QSdJCIQcf802u+Taf+XtW6K1h\n"
	"p/YPL1uhdopUs3c1oon8ykKwnOfrQYgv5pUa7jQdMkltI2MQJU3uFq3Z/LHTvIKe\n"
	"FN+bqK0iYhZthwMG7Rld4+RgKZoT4u1B6w/duEWk9KLjgs7fTf3Oe6JHCYNqwBJi\n"
	"78sJalwXz9Wf8wmMaYSG0XNA7vBOdpTFhVPSsh6e3rkydf5HydMade/II98MWpMe\n"
	"hFg7FFMaJP6ig8p5iL+9QP2VMCkCAwEAAaNQME4wHQYDVR0OBBYEFIXGmIcKptBP\n"
	"v3i9WS/mK78o5E/MMB8GA1UdIwQYMBaAFIXGmIcKptBPv3i9WS/mK78o5E/MMAwG\n"
	"A1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAJkVgNGOXT+ZHCNEYLjr/6OM\n"
	"UCHvwlMeaEyqxaOmK26J2kAADPhjBZ7lZOHWb2Wzb+BiQUIFGwNIMoRvsg8skpJa\n"
	"OCqpVciHVXY/U8BiYY70DKozRza93Ab9om3pySGDJ/akdCjqbMT1Cb7Kloyw+hNh\n"
	"XD4MML0lYiUE9KK35xyK6FgTx4A7IXl4b3lWBgglqTh4+P5J1+xy8AYJ0VfPoP7y\n"
	"OoZgwAmkpkMnalReNkN7LALHGqMzv/qH04ODlkU/HUGgExtnINMxK9VEDIe/yLGm\n"
	"DHy7gcQMj5Hyymack/d4ZF8CSrYpGZQeZGXoxOmTDwWcXgnYA+2o7lOYPb5Uu08=\n"
	"-----END CERTIFICATE-----\n"
	"-----BEGIN PRIVATE KEY-----\n"
	"MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCmUy9nnQ5InreB\n"
	"wU2Z48rJ+oDlWR9jD9NilQSL/x+3vt8O2TiEb3D84WXvTN3Bh9bf6ZK/Q0l78yG+\n"
	"NhH3zFZaSw9lUJ9h/FQtkEnSQiEHH/NNrvk2n/l7VuitYaf2Dy9boXaKVLN3NaKJ\n"
	"/MpCsJzn60GIL+aVGu40HTJJbSNjECVN7hat2fyx07yCnhTfm6itImIWbYcDBu0Z\n"
	"XePkYCmaE+LtQesP3bhFpPSi44LO3039znuiRwmDasASYu/LCWpcF8/Vn/MJjGmE\n"
	"htFzQO7wTnaUxYVT0rIent65MnX+R8nTGnXvyCPfDFqTHoRYOxRTGiT+ooPKeYi/\n"
	"vUD9lTApAgMBAAECggEBAIYCmDtfq9aPK0P8v82yX/4FPD2OZV+nrKXNc3BpCuE9\n"
	"hPOtyX/LWrol0b/Rqwr3rAWVaIt6Z4bbCuD7J9cEaL8voyP6pbCJYjmj/BbQ+VOI\n"
	"Rrzcsid1Fcpu5+JqwK3c5kdp/NzQChmOuXt8lmrNal7iilZ0YdDZdfu/WnkW2mBB\n"
	"oQHkujlnWr4PNYdwMOnBU6TwdOuz+inPVMLohOO0Vr585OxPsGzG2Ud3yQ/t34Cq\n"
	"F9nmOXQoszftGKsL1yuh/3fGj/O86g/CRsUy05qZhDDBEYQD6qZCvD5+yp8oOWIR\n"
	"SljM3GXDBnJqRPhP+Nyf6e6/GoQtfVZ9MPRzDDPzIBECgYEA2kX/zAs6taOiNqCb\n"
	"6nVGe7/3uQJz/CkmOSKIFKUu7lCEUjmMYpK3Xzp26RTUR9cT+g9y+cnJO1Vbaxtf\n"
	"Qidje6K+Oi1pQyUGQ6W+U8cPJHz43PVa7IB5Az5i/sS2tu0BGhvGo9G6iYQjxXeD\n"
	"1197DRACgnm5AORQMum616XvSPMCgYEAwxKbkAzJzfZF6A3Ys+/0kycNfDP8xZoC\n"
	"1zV3d1b2JncsdAPCHYSKtpniRrQN9ASa3RMdkh+wrMN/KlbtU9Ddoc4NHxSTFV7F\n"
	"wypFMzLZslqkQ6uHnVVewHV7prfoKsMci2c9iHO7W8TEv4aqW8XDd8OozP3/q2j4\n"
	"hvL7VIAVqXMCgYEAwAFnfOQ75uBkp00tGlfDgsRhc5vWz3CbMRNRRWfxGq41V+dL\n"
	"uMJ7EAfr5ijue6uU5RmF+HkqzUjOvC894oGnn3CPibm8qNX+5q7799JZXa2ZdTVX\n"
	"oEd7LAFLL/V3DP77Qy4/1Id/Ycydcu0pSuGw6tK0gnX06fXtHnxAYcaT8UUCgYAE\n"
	"MytcP5o8r/ezVlD7Fsh6PpYAvZHMo1M6VPFchWfJTjmLyeTtA8SEx+1iPlAql8rJ\n"
	"xbaWRc5k+dSMEdEMQ+vxpuELcUL1a9PwLsHMp2SefWsZ9eB2l7bxh9YAsebyvL6p\n"
	"lbBydqNrB2KBCSIz1Z8uveytdS6C/0CSjzqwCA3vVwKBgQDAXqjo3xrzMlHeXm5o\n"
	"qH/OjajjqbnPXHolHDitbLubyQ4E6KhMBMxfChBe/8VptB/Gs0efVbMVGuabxY7Q\n"
	"iastGId8HyONy3UPGPxCn4b95cIxKvdpt+hvWtYHIBCfHXluQK7zsDMgvtXjYNiz\n"
	"peZRikYlwmu1K2YRTf7oLE2Ogw==\n"
	"-----END PRIVATE KEY-----\n";

template <class T>
T simulate( const T& in ) {
	BinaryWriter writer(AssumeVersion(currentProtocolVersion));
	writer << in;
	BinaryReader reader( writer.getData(), writer.getLength(), AssumeVersion(currentProtocolVersion) );
	T out;
	reader >> out;
	return out;
}

static void simInitTLS() {
	Reference<TLSOptions> options( new TLSOptions );
	options->set_cert_data( certBytes );
	options->set_key_data( certBytes );
	options->register_network();
}

ACTOR Future<Void> runBackup( Reference<ClusterConnectionFile> connFile ) {
	state std::vector<Future<Void>> agentFutures;

	while (g_simulator.backupAgents == ISimulator::WaitForType) {
		Void _ = wait(delay(1.0));
	}

	if (g_simulator.backupAgents == ISimulator::BackupToFile) {
		Reference<Cluster> cluster = Cluster::createCluster(connFile, -1);
		Database cx = cluster->createDatabase(LiteralStringRef("DB")).get();

		state FileBackupAgent fileAgent;
		state double backupPollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
		agentFutures.push_back(fileAgent.run(cx, &backupPollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));

		while (g_simulator.backupAgents == ISimulator::BackupToFile) {
			Void _ = wait(delay(1.0));
		}

		for(auto it : agentFutures) {
			it.cancel();
		}
	}

	Void _= wait(Future<Void>(Never()));
	throw internal_error();
}

ACTOR Future<Void> runDr( Reference<ClusterConnectionFile> connFile ) {
	state std::vector<Future<Void>> agentFutures;

	while (g_simulator.drAgents == ISimulator::WaitForType) {
		Void _ = wait(delay(1.0));
	}

	if (g_simulator.drAgents == ISimulator::BackupToDB) {
		Reference<Cluster> cluster = Cluster::createCluster(connFile, -1);
		Database cx = cluster->createDatabase(LiteralStringRef("DB")).get();

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		Reference<Cluster> extraCluster = Cluster::createCluster(extraFile, -1);
		state Database extraDB = extraCluster->createDatabase(LiteralStringRef("DB")).get();

		TraceEvent("StartingDrAgents").detail("connFile", connFile->getConnectionString().toString()).detail("extraString", extraFile->getConnectionString().toString());

		state DatabaseBackupAgent dbAgent = DatabaseBackupAgent(cx);
		state DatabaseBackupAgent extraAgent = DatabaseBackupAgent(extraDB);

		state double dr1PollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;
		state double dr2PollDelay = 1.0 / CLIENT_KNOBS->BACKUP_AGGREGATE_POLL_RATE;

		agentFutures.push_back(extraAgent.run(cx, &dr1PollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));
		agentFutures.push_back(dbAgent.run(extraDB, &dr2PollDelay, CLIENT_KNOBS->SIM_BACKUP_TASKS_PER_AGENT));

		while (g_simulator.drAgents == ISimulator::BackupToDB) {
			Void _ = wait(delay(1.0));
		}

		TraceEvent("StoppingDrAgents");

		for(auto it : agentFutures) {
			it.cancel();
		}
	}

	Void _= wait(Future<Void>(Never()));
	throw internal_error();
}

// SOMEDAY: when a process can be rebooted in isolation from the other on that machine,
//  a loop{} will be needed around the waiting on simulatedFDBD(). For now this simply
//  takes care of house-keeping such as context switching and file closing.
ACTOR Future<ISimulator::KillType> simulatedFDBDRebooter(
		Reference<ClusterConnectionFile> connFile,
		uint32_t ip,
		bool useSSL,
		uint16_t port,
		LocalityData localities,
		ProcessClass processClass,
		std::string* dataFolder,
		std::string* coordFolder,
		std::string baseFolder,
		ClusterConnectionString connStr,
		bool useSeedFile,
		bool runBackupAgents)
{
	state ISimulator::ProcessInfo *simProcess = g_simulator.getCurrentProcess();
	state UID randomId = g_nondeterministic_random->randomUniqueID();
	state int cycles = 0;

	loop {
		auto waitTime = SERVER_KNOBS->MIN_REBOOT_TIME + (SERVER_KNOBS->MAX_REBOOT_TIME - SERVER_KNOBS->MIN_REBOOT_TIME) * g_random->random01();
		cycles ++;
		TraceEvent("SimulatedFDBDPreWait").detail("Cycles", cycles).detail("RandomId", randomId)
			.detail("Address", NetworkAddress(ip, port, true, false))
			.detailext("ZoneId", localities.zoneId())
			.detail("waitTime", waitTime).detail("Port", port);

		Void _ = wait( delay( waitTime ) );

		state ISimulator::ProcessInfo *process =  g_simulator.newProcess( "Server", ip, port, localities, processClass, dataFolder->c_str(), coordFolder->c_str() );
		Void _ = wait( g_simulator.onProcess(process, TaskDefaultYield) );	// Now switch execution to the process on which we will run
		state Future<ISimulator::KillType> onShutdown = process->onShutdown();

		try {
			TraceEvent("SimulatedRebooterStarting", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
				.detailext("ZoneId", localities.zoneId())
				.detailext("DataHall", localities.dataHallId())
				.detail("Address", process->address.toString())
				.detail("Excluded", process->excluded)
				.detail("UsingSSL", useSSL);
			TraceEvent("ProgramStart").detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("SourceVersion", getHGVersion())
				.detail("Version", FDB_VT_VERSION)
				.detail("PackageName", FDB_VT_PACKAGE_NAME)
				.detail("DataFolder", *dataFolder)
				.detail("ConnectionString", connFile ? connFile->getConnectionString().toString() : "")
				.detailf("ActualTime", "%lld", DEBUG_DETERMINISM ? 0 : time(NULL))
				.detail("CommandLine", "fdbserver -r simulation")
				.detail("BuggifyEnabled", buggifyActivated)
				.detail("Simulated", true)
				.trackLatest("ProgramStart");

			try {
				//SOMEDAY: test lower memory limits, without making them too small and causing the database to stop making progress
				FlowTransport::createInstance(1);
				Sim2FileSystem::newFileSystem();
				if (useSSL) {
					simInitTLS();
				}
				NetworkAddress n(ip, port, true, useSSL);
				Future<Void> listen = FlowTransport::transport().bind( n, n );
				Future<Void> fd = fdbd( connFile, localities, processClass, *dataFolder, *coordFolder, 500e6, "", "");
				Future<Void> backup = runBackupAgents ? runBackup(connFile) : Future<Void>(Never());
				Future<Void> dr = runBackupAgents ? runDr(connFile) : Future<Void>(Never());

				Void _ = wait(listen || fd || success(onShutdown) || backup || dr);
			} catch (Error& e) {
				// If in simulation, if we make it here with an error other than io_timeout but enASIOTimedOut is set then somewhere an io_timeout was converted to a different error.
				if(g_network->isSimulated() && e.code() != error_code_io_timeout && (bool)g_network->global(INetwork::enASIOTimedOut))
					TraceEvent(SevError, "IOTimeoutErrorSuppressed").detail("ErrorCode", e.code()).detail("RandomId", randomId).backtrace();

				if (onShutdown.isReady() && onShutdown.isError()) throw onShutdown.getError();
				if(e.code() != error_code_actor_cancelled)
					printf("SimulatedFDBDTerminated: %s\n", e.what());
				ASSERT( destructed || g_simulator.getCurrentProcess() == process ); // simulatedFDBD catch called on different process
				TraceEvent(e.code() == error_code_actor_cancelled || e.code() == error_code_file_not_found || destructed ? SevInfo : SevError, "SimulatedFDBDTerminated", localities.zoneId()).error(e, true);
			}

			TraceEvent("SimulatedFDBDDone", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detail("Excluded", process->excluded)
				.detailext("ZoneId", localities.zoneId())
				.detail("KillType", onShutdown.isReady() ? onShutdown.get() : ISimulator::None);

			if (!onShutdown.isReady())
				onShutdown = ISimulator::InjectFaults;
		} catch (Error& e) {
			TraceEvent(destructed ? SevInfo : SevError, "SimulatedFDBDRebooterError", localities.zoneId()).detail("RandomId", randomId).error(e, true);
			onShutdown = e;
		}

		ASSERT( destructed || g_simulator.getCurrentProcess() == process );

		if( !process->shutdownSignal.isSet() && !destructed ) {
			process->rebooting = true;
			process->shutdownSignal.send(ISimulator::None);
		}
		TraceEvent("SimulatedFDBDWait", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
			.detail("Address", process->address)
			.detail("Excluded", process->excluded)
			.detail("Rebooting", process->rebooting)
			.detailext("ZoneId", localities.zoneId());
		Void _ = wait( g_simulator.onProcess( simProcess ) );

		Void _ = wait(delay(0.00001 + FLOW_KNOBS->MAX_BUGGIFIED_DELAY));  // One last chance for the process to clean up?

		g_simulator.destroyProcess( process );  // Leak memory here; the process may be used in other parts of the simulation

		auto shutdownResult = onShutdown.get();
		TraceEvent("SimulatedFDBDShutdown", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
			.detail("Address", process->address)
			.detail("Excluded", process->excluded)
			.detailext("ZoneId", localities.zoneId())
			.detail("KillType", shutdownResult);

		if( shutdownResult < ISimulator::RebootProcessAndDelete ) {
			TraceEvent("SimulatedFDBDLowerReboot", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detail("Excluded", process->excluded)
				.detailext("ZoneId", localities.zoneId())
				.detail("KillType", shutdownResult);
			return onShutdown.get();
		}

		if( onShutdown.get() == ISimulator::RebootProcessAndDelete ) {
			TraceEvent("SimulatedFDBDRebootAndDelete", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detailext("ZoneId", localities.zoneId())
				.detail("KillType", shutdownResult);
			*coordFolder = joinPath(baseFolder, g_random->randomUniqueID().toString());
			*dataFolder = joinPath(baseFolder, g_random->randomUniqueID().toString());
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
			TraceEvent("SimulatedFDBDJustRepeat", localities.zoneId()).detail("Cycles", cycles).detail("RandomId", randomId)
				.detail("Address", process->address)
				.detailext("ZoneId", localities.zoneId())
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
ACTOR Future<Void> simulatedMachine(
		ClusterConnectionString connStr,
		std::vector<uint32_t> ips,
		bool sslEnabled,
		LocalityData localities,
		ProcessClass processClass,
		std::string baseFolder,
		bool restarting,
		bool useSeedFile,
		bool runBackupAgents)
{
	state int bootCount = 0;
	state std::vector<std::string> myFolders;
	state std::vector<std::string> coordFolders;
	state UID randomId = g_nondeterministic_random->randomUniqueID();

	try {
		CSimpleIni ini;
		ini.SetUnicode();
		ini.LoadFile(joinPath(baseFolder, "restartInfo.ini").c_str());

		for (int i = 0; i < ips.size(); i++) {
			if (restarting) {
				myFolders.push_back( ini.GetValue(printable(localities.zoneId()).c_str(), format("%d", i).c_str(), joinPath(baseFolder, g_random->randomUniqueID().toString()).c_str()) );

				if(i == 0) {
					std::string coordinationFolder = ini.GetValue(printable(localities.zoneId()).c_str(), "coordinationFolder", "");
					if(!coordinationFolder.size())
						coordinationFolder = ini.GetValue(printable(localities.zoneId()).c_str(), format("c%d", i).c_str(), joinPath(baseFolder, g_random->randomUniqueID().toString()).c_str());
					coordFolders.push_back(coordinationFolder);
				} else {
					coordFolders.push_back( ini.GetValue(printable(localities.zoneId()).c_str(), format("c%d", i).c_str(), joinPath(baseFolder, g_random->randomUniqueID().toString()).c_str()) );
				}
			}
			else {
				coordFolders.push_back( joinPath(baseFolder, g_random->randomUniqueID().toString()) );
				std::string thisFolder = g_random->randomUniqueID().toString();
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
				processes.push_back(simulatedFDBDRebooter(clusterFile, ips[i], sslEnabled, i + 1, localities, processClass, &myFolders[i], &coordFolders[i], baseFolder, connStr, useSeedFile, runBackupAgents));
				TraceEvent("SimulatedMachineProcess", randomId).detail("Address", NetworkAddress(ips[i], i+1, true, false)).detailext("ZoneId", localities.zoneId()).detailext("DataHall", localities.dataHallId()).detail("Folder", myFolders[i]);
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
				.detail("processes", processes.size())
				.detail("bootCount", bootCount)
				.detail("ProcessClass", processClass.toString())
				.detail("Restarting", restarting)
				.detail("UseSeedFile", useSeedFile)
				.detailext("ZoneId", localities.zoneId())
				.detailext("DataHall", localities.dataHallId())
				.detail("Locality", localities.toString());

			Void _ = wait( waitForAll( processes ) );

			TraceEvent("SimulatedMachineRebootStart", randomId)
				.detail("Folder0", myFolders[0])
				.detail("CFolder0", coordFolders[0])
				.detail("MachineIPs", toIPVectorString(ips))
				.detailext("ZoneId", localities.zoneId())
				.detailext("DataHall", localities.dataHallId());

			//Kill all open files, which may cause them to write invalid data.
			auto& machineCache = g_simulator.getMachineById(localities.zoneId())->openFiles;

			//Copy the file pointers to a vector because the map may be modified while we are killing files
			std::vector<AsyncFileNonDurable*> files;
			for(auto fileItr = machineCache.begin(); fileItr != machineCache.end(); ++fileItr) {
				ASSERT( fileItr->second.isReady() );
				files.push_back( (AsyncFileNonDurable*)fileItr->second.get().getPtr() );
			}

			std::vector<Future<Void>> killFutures;
			for(auto fileItr = files.begin(); fileItr != files.end(); ++fileItr)
				killFutures.push_back((*fileItr)->kill());

			Void _ = wait( waitForAll( killFutures ) );

			state std::set<std::string> filenames;
			state std::string closingStr;
			auto& machineCache = g_simulator.getMachineById(localities.zoneId())->openFiles;
			for( auto it : machineCache ) {
				filenames.insert( it.first );
				closingStr += it.first + ", ";
				ASSERT( it.second.isReady() && !it.second.isError() );
			}

			for( auto it : g_simulator.getMachineById(localities.zoneId())->deletingFiles ) {
				filenames.insert( it );
				closingStr += it + ", ";
			}

			TraceEvent("SimulatedMachineRebootAfterKills", randomId)
				.detail("Folder0", myFolders[0])
				.detail("CFolder0", coordFolders[0])
				.detail("MachineIPs", toIPVectorString(ips))
				.detail("Closing", closingStr)
				.detailext("ZoneId", localities.zoneId())
				.detailext("DataHall", localities.dataHallId());

			ISimulator::MachineInfo* machine = g_simulator.getMachineById(localities.zoneId());
			machine->closingFiles = filenames;
			g_simulator.getMachineById(localities.zoneId())->openFiles.clear();

			// During a reboot:
			//   The process is expected to close all files and be inactive in zero time, but not necessarily
			//   without delay(0)-equivalents, so delay(0) a few times waiting for it to achieve that goal.
			// After an injected fault:
			//   The process is expected to shut down eventually, but not necessarily instantly.  Wait up to 60 seconds.
			state int shutdownDelayCount = 0;
			state double backoff = 0;
			loop {
				auto& machineCache = g_simulator.getMachineById(localities.zoneId())->closingFiles;

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
						.detailext("ZoneId", localities.zoneId())
						.detailext("DataHall", localities.dataHallId());
					ASSERT( false );
				}

				Void _ = wait( delay( backoff ) );
				backoff = std::min( backoff + 1.0, 6.0 );
			}

			TraceEvent("SimulatedFDBDFilesClosed", randomId)
				.detail("Address", toIPVectorString(ips))
				.detailext("ZoneId", localities.zoneId())
				.detailext("DataHall", localities.dataHallId());

			g_simulator.destroyMachine(localities.zoneId());

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

			auto rebootTime = g_random->random01() * MACHINE_REBOOT_TIME;

			TraceEvent("SimulatedMachineShutdown", randomId)
				.detail("Swap", swap)
				.detail("KillType", killType)
				.detail("RebootTime", rebootTime)
				.detailext("ZoneId", localities.zoneId())
				.detailext("DataHall", localities.dataHallId())
				.detail("MachineIPs", toIPVectorString(ips));

			Void _ = wait( delay( rebootTime ) );

			if( swap ) {
				auto& avail = availableFolders[localities.dcId()];
				int i = g_random->randomInt(0, avail.size());
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
					coordFolders[i] = joinPath(baseFolder, g_random->randomUniqueID().toString());
					myFolders[i] = joinPath(baseFolder, g_random->randomUniqueID().toString());
					platform::createDirectory( myFolders[i] );

					if(!useSeedFile) {
						writeFile(joinPath(myFolders[i], "fdb.cluster"), connStr.toString());
					}
				}

				TEST( true ); // Simulated machine rebooted with data loss
			}

			//this machine is rebooting = false;
		}
	} catch( Error &e ) {
		g_simulator.getMachineById(localities.zoneId())->openFiles.clear();
		throw;
	}
}

#include "fdbclient/MonitorLeader.h"

ACTOR Future<Void> restartSimulatedSystem(vector<Future<Void>> *systemActors, std::string baseFolder,
										  int* pTesterCount, Optional<ClusterConnectionString> *pConnString) {
	CSimpleIni ini;
	ini.SetUnicode();
	ini.LoadFile(joinPath(baseFolder, "restartInfo.ini").c_str());

	// allows multiple ipAddr entries
	ini.SetMultiKey();

	try {
		int machineCount = atoi(ini.GetValue("META", "machineCount"));
		int processesPerMachine = atoi(ini.GetValue("META", "processesPerMachine"));
		int desiredCoordinators = atoi(ini.GetValue("META", "desiredCoordinators"));
		int testerCount = atoi(ini.GetValue("META", "testerCount"));
		ClusterConnectionString conn(ini.GetValue("META", "connectionString"));
		*pConnString = conn;
		*pTesterCount = testerCount;
		bool usingSSL = conn.toString().find(":tls") != std::string::npos;
		int useSeedForMachine = g_random->randomInt(0, machineCount);
		for( int i = 0; i < machineCount; i++) {
			Optional<Standalone<StringRef>> dcUID;
			std::string zoneIdString = ini.GetValue("META", format("%d", i).c_str());
			Standalone<StringRef> zoneId = StringRef(zoneIdString);
			std::string	dcUIDini = ini.GetValue(zoneIdString.c_str(), "dcUID");
			if (!dcUIDini.empty()) dcUID = StringRef(dcUIDini);
			ProcessClass processClass = ProcessClass((ProcessClass::ClassType)atoi(ini.GetValue(zoneIdString.c_str(), "mClass")), ProcessClass::CommandLineSource);

			std::vector<uint32_t> ipAddrs;
			int processes = atoi(ini.GetValue(zoneIdString.c_str(), "processes"));

			auto ip = ini.GetValue(zoneIdString.c_str(), "ipAddr");

			if( ip == NULL ) {
				for (int i = 0; i < processes; i++){
					ipAddrs.push_back(strtoul(ini.GetValue(zoneIdString.c_str(), format("ipAddr%d", i).c_str()), NULL, 10));
				}
			}
			else {
				// old way
				ipAddrs.push_back(strtoul(ip, NULL, 10));
				for (int i = 1; i < processes; i++){
					ipAddrs.push_back(ipAddrs.back() + 1);
				}
			}

			LocalityData	localities(Optional<Standalone<StringRef>>(), zoneId, zoneId, dcUID);
			localities.set(LiteralStringRef("data_hall"), dcUID);

			systemActors->push_back( reportErrors( simulatedMachine(
				conn, ipAddrs, usingSSL, localities, processClass, baseFolder, true, i == useSeedForMachine, false ),
				processClass == ProcessClass::TesterClass ? "SimulatedTesterMachine" : "SimulatedMachine") );
		}

		g_simulator.desiredCoordinators = desiredCoordinators;
		g_simulator.processesPerMachine = processesPerMachine;
	}
	catch (Error& e) {
		TraceEvent(SevError, "restartSimulationError").error(e);
	}

	TraceEvent("RestartSimulatorSettings")
		.detail("desiredCoordinators", g_simulator.desiredCoordinators)
		.detail("processesPerMachine", g_simulator.processesPerMachine);

	Void _ = wait(delay(1.0));

	return Void();
}

struct SimulationConfig {
	explicit SimulationConfig(int extraDB, int minimumReplication);
	int extraDB;

	DatabaseConfiguration db;

	void set_config(std::string config);

	// Simulation layout
	int datacenters;
	int machine_count;  // Total, not per DC.
	int processes_per_machine;
	int coordinators;
private:
	void generateNormalConfig(int minimumReplication);
};

SimulationConfig::SimulationConfig(int extraDB, int minimumReplication) : extraDB(extraDB) {
	generateNormalConfig(minimumReplication);
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

void SimulationConfig::generateNormalConfig(int minimumReplication) {
	set_config("new");
	bool generateFearless = false; //FIXME g_random->random01() < 0.5;
	datacenters = generateFearless ? 4 : g_random->randomInt( 1, 4 );
	if (g_random->random01() < 0.25) db.desiredTLogCount = g_random->randomInt(1,7);
	if (g_random->random01() < 0.25) db.masterProxyCount = g_random->randomInt(1,7);
	if (g_random->random01() < 0.25) db.resolverCount = g_random->randomInt(1,7);
	if (g_random->random01() < 0.5) {
		set_config("ssd");
	} else {
		set_config("memory");
	}

	int replication_type = std::max(minimumReplication, std::min(g_random->randomInt(0,6), 3));
	switch (replication_type) {
	case 0: {
		TEST( true );  // Simulated cluster using custom redundancy mode
		int storage_servers = g_random->randomInt(1, generateFearless ? 4 : 5);
		int replication_factor = g_random->randomInt(1, generateFearless ? 4 : 5);
		int anti_quorum = g_random->randomInt(0, replication_factor);
		// Go through buildConfiguration, as it sets tLogPolicy/storagePolicy.
		set_config(format("storage_replicas:=%d storage_quorum:=%d "
		                  "log_replicas:=%d log_anti_quorum:=%1 "
		                  "replica_datacenters:=1 min_replica_datacenters:=1",
		                  storage_servers, storage_servers,
		                  replication_factor, anti_quorum));
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

	if(generateFearless || (datacenters == 2 && g_random->random01() < 0.5)) {
		StatusObject primaryObj;
		primaryObj["id"] = "0";
		primaryObj["priority"] = 1;

		StatusObject remoteObj;
		remoteObj["id"] = "1";
		remoteObj["priority"] = 0;

		bool needsRemote = generateFearless;
		if(generateFearless) {
			StatusObject primarySatelliteObj;
			primarySatelliteObj["id"] = "2";
			primarySatelliteObj["priority"] = 1;
			StatusArray primarySatellitesArr;
			primarySatellitesArr.push_back(primarySatelliteObj);
			primaryObj["satellites"] = primarySatellitesArr;

			StatusObject remoteSatelliteObj;
			remoteSatelliteObj["id"] = "3";
			remoteSatelliteObj["priority"] = 1;
			StatusArray remoteSatellitesArr;
			remoteSatellitesArr.push_back(remoteSatelliteObj);
			remoteObj["satellites"] = remoteSatellitesArr;

			int satellite_replication_type = g_random->randomInt(0,5);
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

			if (g_random->random01() < 0.25) {
				int logs = g_random->randomInt(1,7);
				primaryObj["satellite_logs"] = logs;
				remoteObj["satellite_logs"] = logs;
			}
			
			int remote_replication_type = g_random->randomInt(0,5);
			switch (remote_replication_type) {
			case 0: {
				//FIXME: implement
				TEST( true );  // Simulated cluster using custom remote redundancy mode
				break;
			}
			case 1: {
				needsRemote = false;
				TEST( true );  // Simulated cluster using no remote redundancy mode
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

			if (g_random->random01() < 0.25) db.remoteDesiredTLogCount = g_random->randomInt(1,7);
			if (g_random->random01() < 0.25) db.desiredLogRouterCount = g_random->randomInt(1,7);
		}

		StatusArray regionArr;
		regionArr.push_back(primaryObj);
		if(needsRemote || g_random->random01() < 0.5) {
			regionArr.push_back(remoteObj);
		}

		set_config("regions=" + json_spirit::write_string(json_spirit::mValue(regionArr), json_spirit::Output_options::none));
	}
	
	if(generateFearless) {
		machine_count = 12;
	} else if(db.tLogPolicy && db.tLogPolicy->info() == "data_hall^2 x zoneid^2 x 1") {
		machine_count = 9;
	} else {
		//datacenters+2 so that the configure database workload can configure into three_data_hall
		machine_count = std::max(datacenters+2, ((db.minDatacentersRequired() > 0) ? datacenters : 1) * std::max(3, db.minMachinesRequiredPerDatacenter()));
		machine_count = g_random->randomInt( machine_count, std::max(machine_count+1, extraDB ? 6 : 10) );
	}

	//because we protect a majority of coordinators from being killed, it is better to run with low numbers of coordinators to prevent too many processes from being protected
	coordinators = BUGGIFY ? g_random->randomInt(1, machine_count+1) : 1;

	if(minimumReplication > 1 && datacenters == 3) {
		//low latency tests in 3 data hall mode need 2 other data centers with 2 machines each to avoid waiting for logs to recover.
		machine_count = std::max( machine_count, 6);
		coordinators = 3;
	}

	if(generateFearless) {
		processes_per_machine = 1;
	} else {
		processes_per_machine = g_random->randomInt(1, (extraDB ? 14 : 28)/machine_count + 2 );
	}
}

void setupSimulatedSystem( vector<Future<Void>> *systemActors, std::string baseFolder,
							int* pTesterCount, Optional<ClusterConnectionString> *pConnString,
							Standalone<StringRef> *pStartingConfiguration, int extraDB, int minimumReplication)
{
	// SOMEDAY: this does not test multi-interface configurations
	SimulationConfig simconfig(extraDB, minimumReplication);
	StatusObject startingConfigJSON = simconfig.db.toJSON(true);
	std::string startingConfigString = "new";
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
	g_simulator.hasRemoteReplication = simconfig.db.remoteTLogReplicationFactor > 0;
	g_simulator.remoteTLogPolicy = simconfig.db.remoteTLogPolicy;

	if(simconfig.db.regions.size() == 2) {
		g_simulator.primaryDcId = simconfig.db.regions[0].dcId;
		g_simulator.remoteDcId = simconfig.db.regions[1].dcId;
		g_simulator.hasSatelliteReplication = simconfig.db.regions[0].satelliteTLogReplicationFactor > 0 && simconfig.db.regions[0].satelliteTLogPolicy == simconfig.db.regions[1].satelliteTLogPolicy;
		g_simulator.satelliteTLogPolicy = simconfig.db.regions[0].satelliteTLogPolicy;
		g_simulator.satelliteTLogWriteAntiQuorum = simconfig.db.regions[0].satelliteTLogWriteAntiQuorum;

		for(auto s : simconfig.db.regions[0].satellites) {
			g_simulator.primarySatelliteDcIds.push_back(s.dcId);
		}
		for(auto s : simconfig.db.regions[1].satellites) {
			g_simulator.remoteSatelliteDcIds.push_back(s.dcId);
		}
	} else if(simconfig.db.regions.size() == 1) {
		g_simulator.primaryDcId = simconfig.db.regions[0].dcId;
		g_simulator.hasSatelliteReplication = simconfig.db.regions[0].satelliteTLogReplicationFactor > 0;
		g_simulator.satelliteTLogPolicy = simconfig.db.regions[0].satelliteTLogPolicy;
		g_simulator.satelliteTLogWriteAntiQuorum = simconfig.db.regions[0].satelliteTLogWriteAntiQuorum;

		for(auto s : simconfig.db.regions[0].satellites) {
			g_simulator.primarySatelliteDcIds.push_back(s.dcId);
		}
	} else {
		g_simulator.hasSatelliteReplication = false;
		g_simulator.satelliteTLogWriteAntiQuorum = 0;
	}
		
	ASSERT(g_simulator.storagePolicy && g_simulator.tLogPolicy);
	ASSERT(!g_simulator.hasRemoteReplication || g_simulator.remoteTLogPolicy);
	ASSERT(!g_simulator.hasSatelliteReplication || g_simulator.satelliteTLogPolicy);
	TraceEvent("simulatorConfig").detail("ConfigString", printable(StringRef(startingConfigString)));

	const int dataCenters = simconfig.datacenters;
	const int machineCount = simconfig.machine_count;
	const int coordinatorCount = simconfig.coordinators;
	const int processesPerMachine = simconfig.processes_per_machine;

	// half the time, when we have more than 4 machines that are not the first in their dataCenter, assign classes
	bool assignClasses = machineCount - dataCenters > 4 && g_random->random01() < 0.5;

	// Use SSL 5% of the time
	bool sslEnabled = g_random->random01() < 0.05;
	TEST( sslEnabled ); // SSL enabled
	TEST( !sslEnabled ); // SSL disabled

	vector<NetworkAddress> coordinatorAddresses;
	for( int dc = 0; dc < dataCenters; dc++ ) {
		int machines = machineCount / dataCenters + (dc < machineCount % dataCenters); // add remainder of machines to first datacenter
		int dcCoordinators = coordinatorCount / dataCenters + (dc < coordinatorCount%dataCenters);

		for(int m = 0; m < dcCoordinators; m++) {
			uint32_t ip = 2<<24 | dc<<16 | 1<<8 | m;
			coordinatorAddresses.push_back(NetworkAddress(ip, 1, true, sslEnabled));
			TraceEvent("SelectedCoordinator").detail("Address", coordinatorAddresses.back());
		}
	}

	g_random->randomShuffle(coordinatorAddresses);
	for(int i = 0; i < (coordinatorAddresses.size()/2)+1; i++) {
		TraceEvent("ProtectCoordinator").detail("Address", coordinatorAddresses[i]).detail("Coordinators", describe(coordinatorAddresses)).backtrace();
		g_simulator.protectedAddresses.insert(NetworkAddress(coordinatorAddresses[i].ip,coordinatorAddresses[i].port,true,false));
	}
	g_random->randomShuffle(coordinatorAddresses);

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

	TraceEvent("SimulatedConnectionString").detail("String", conn.toString()).detail("ConfigString", printable(StringRef(startingConfigString)));

	int assignedMachines = 0, nonVersatileMachines = 0;
	for( int dc = 0; dc < dataCenters; dc++ ) {
		//FIXME: test unset dcID
		Optional<Standalone<StringRef>> dcUID = StringRef(format("%d", dc));
		std::vector<UID> machineIdentities;
		int machines = machineCount / dataCenters + (dc < machineCount % dataCenters); // add remainder of machines to first datacenter
		int dcCoordinators = coordinatorCount / dataCenters + (dc < coordinatorCount%dataCenters);
		printf("Datacenter %d: %d/%d machines, %d/%d coordinators\n", dc, machines, machineCount, dcCoordinators, coordinatorCount);
		ASSERT( dcCoordinators <= machines );
		int useSeedForMachine = g_random->randomInt(0, machines);
		for( int machine = 0; machine < machines; machine++ ) {
			Standalone<StringRef> zoneId(g_random->randomUniqueID().toString());

			//Choose a machine class
			ProcessClass processClass = ProcessClass(ProcessClass::UnsetClass, ProcessClass::CommandLineSource);
			if(assignClasses) {
				if(assignedMachines < 4)
					processClass = ProcessClass((ProcessClass::ClassType) g_random->randomInt(0, 2), ProcessClass::CommandLineSource); //Unset or Storage
				else if(assignedMachines == 4 && !g_simulator.hasRemoteReplication && !g_simulator.hasSatelliteReplication)
					processClass = ProcessClass((ProcessClass::ClassType) (g_random->randomInt(0, 2) * ProcessClass::ResolutionClass), ProcessClass::CommandLineSource); //Unset or Resolution
				else
					processClass = ProcessClass((ProcessClass::ClassType) g_random->randomInt(0, 3), ProcessClass::CommandLineSource); //Unset, Storage, or Transaction
				if (processClass == ProcessClass::ResolutionClass)  // *can't* be assigned to other roles, even in an emergency
					nonVersatileMachines++;
			}

			std::vector<uint32_t> ips;
			for (int i = 0; i < processesPerMachine; i++){
				ips.push_back(2 << 24 | dc << 16 | g_random->randomInt(1, i+2) << 8 | machine);
			}
			// check the sslEnablementMap using only one ip(
			LocalityData	localities(Optional<Standalone<StringRef>>(), zoneId, zoneId, dcUID);
			localities.set(LiteralStringRef("data_hall"), dcUID);
			systemActors->push_back(reportErrors(simulatedMachine(conn, ips, sslEnabled,
				localities, processClass, baseFolder, false, machine == useSeedForMachine, true ), "SimulatedMachine"));

			if (extraDB && g_simulator.extraDB->toString() != conn.toString()) {
				std::vector<uint32_t> extraIps;
				for (int i = 0; i < processesPerMachine; i++){
					extraIps.push_back(4 << 24 | dc << 16 | g_random->randomInt(1, i + 2) << 8 | machine);
				}

				Standalone<StringRef> newZoneId = Standalone<StringRef>(g_random->randomUniqueID().toString());
				LocalityData	localities(Optional<Standalone<StringRef>>(), newZoneId, newZoneId, dcUID);
				localities.set(LiteralStringRef("data_hall"), dcUID);
				systemActors->push_back(reportErrors(simulatedMachine(*g_simulator.extraDB, extraIps, sslEnabled,
					localities,
					processClass, baseFolder, false, machine == useSeedForMachine, false ), "SimulatedMachine"));
			}

			assignedMachines++;
		}
	}

	g_simulator.desiredCoordinators = coordinatorCount;
	g_simulator.physicalDatacenters = dataCenters;
	g_simulator.processesPerMachine = processesPerMachine;

	TraceEvent("SetupSimulatorSettings")
		.detail("desiredCoordinators", g_simulator.desiredCoordinators)
		.detail("physicalDatacenters", g_simulator.physicalDatacenters)
		.detail("processesPerMachine", g_simulator.processesPerMachine);

	// SOMEDAY: add locality for testers to simulate network topology
	// FIXME: Start workers with tester class instead, at least sometimes run tests with the testers-only flag
	int testerCount = *pTesterCount = g_random->randomInt(4, 9);
	int useSeedForMachine = g_random->randomInt(0, testerCount);
	for(int i=0; i<testerCount; i++) {
		std::vector<uint32_t> ips;
		ips.push_back(0x03040301 + i);
		Standalone<StringRef> newZoneId = Standalone<StringRef>(g_random->randomUniqueID().toString());
		LocalityData	localities(Optional<Standalone<StringRef>>(), newZoneId, newZoneId, Optional<Standalone<StringRef>>());
		systemActors->push_back( reportErrors( simulatedMachine(
			conn, ips, sslEnabled,
			localities, ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource),
			baseFolder, false, i == useSeedForMachine, false ),
			"SimulatedTesterMachine") );
	}

	/*int testerCount = g_random->randomInt(4, 9);
	for(int i=0; i<testerCount; i++)
		g_simulator.asNewProcess("TestWorker", 0x03040301 + i, LocalityData(g_random->randomUniqueID().toString(), Optional<Standalone<StringRef>>()), [&] {
			vector<Future<Void>> v;

			Reference<AsyncVar<ClusterControllerFullInterface>> cc( new AsyncVar<ClusterControllerFullInterface> );
			Reference<AsyncVar<ClusterInterface>> ci( new AsyncVar<ClusterInterface> );
			v.push_back( monitorLeader( coordinators, cc ) );
			v.push_back( extractClusterInterface(cc,ci) );
			v.push_back( failureMonitorClient( ci ) );
			v.push_back( testerServer( cc ) );
			systemActors->push_back( waitForAll(v) );
		});*/


	*pStartingConfiguration = startingConfigString;

	// save some state that we only need when restarting the simulator.
	g_simulator.connectionString = conn.toString();
	g_simulator.testerCount = testerCount;

	TraceEvent("SimulatedClusterStarted")
		.detail("DataCenters", dataCenters)
		.detail("ServerMachineCount", machineCount)
		.detail("ProcessesPerServer", processesPerMachine)
		.detail("SSLEnabled", sslEnabled)
		.detail("ClassesAssigned", assignClasses)
		.detail("StartingConfiguration", pStartingConfiguration->toString());
}

void checkExtraDB(const char *testFile, int &extraDB, int &minimumReplication) {
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
	}

	ifs.close();
}

ACTOR void setupAndRun(std::string dataFolder, const char *testFile, bool rebooting, bool useSSL ) {
	state vector<Future<Void>> systemActors;
	state Optional<ClusterConnectionString> connFile;
	state Standalone<StringRef> startingConfiguration;
	state int testerCount = 1;
	state int extraDB = 0;
	state int minimumReplication = 0;
	checkExtraDB(testFile, extraDB, minimumReplication);

	Void _ = wait( g_simulator.onProcess( g_simulator.newProcess(
			"TestSystem", 0x01010101, 1, LocalityData(Optional<Standalone<StringRef>>(), Standalone<StringRef>(g_random->randomUniqueID().toString()), Optional<Standalone<StringRef>>(), Optional<Standalone<StringRef>>()), ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource), "", "" ), TaskDefaultYield ) );
	Sim2FileSystem::newFileSystem();
	FlowTransport::createInstance(1);
	if (useSSL) {
		simInitTLS();
	}

	TEST(true);  // Simulation start

	try {
		//systemActors.push_back( startSystemMonitor(dataFolder) );
		if (rebooting) {
			Void _ = wait( timeoutError( restartSimulatedSystem( &systemActors, dataFolder, &testerCount, &connFile), 100.0 ) );
		}
		else {
			g_expect_full_pointermap = 1;
			setupSimulatedSystem( &systemActors, dataFolder, &testerCount, &connFile, &startingConfiguration, extraDB, minimumReplication );
			Void _ = wait( delay(1.0) ); // FIXME: WHY!!!  //wait for machines to boot
		}
		std::string clusterFileDir = joinPath( dataFolder, g_random->randomUniqueID().toString() );
		platform::createDirectory( clusterFileDir );
		writeFile(joinPath(clusterFileDir, "fdb.cluster"), connFile.get().toString());
		Void _ = wait(timeoutError(runTests(Reference<ClusterConnectionFile>(new ClusterConnectionFile(joinPath(clusterFileDir, "fdb.cluster"))), TEST_TYPE_FROM_FILE, TEST_ON_TESTERS, testerCount, testFile, startingConfiguration), buggifyActivated ? 36000.0 : 5400.0));
	} catch (Error& e) {
		TraceEvent(SevError, "setupAndRunError").error(e);
	}

	TraceEvent("SimulatedSystemDestruct");
	destructed = true;
	systemActors.clear();

	g_simulator.stop();
}
