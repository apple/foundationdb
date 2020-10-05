/*
 * ProtocolVersion.actor.cpp
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

#include <cstdint>
#include <ostream>
#include <string>
#include <thread>
#include <vector>
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/MultiVersionTransaction.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbrpc/Locality.h"
#include "fdbserver/SimulatedCluster.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbmonitor/SimpleIni.h"
#include "flow/Arena.h"
#include "flow/ProtocolVersion.h"
#include "flow/flow.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // has to be last include

struct ProtocolVersionWorkload : TestWorkload {
    ProtocolVersionWorkload(WorkloadContext const& wcx)
	: TestWorkload(wcx) {
		// protocol = ProtocolVersion(getOption(options, LiteralStringRef("protocolVersion"), 0.0));
	}

	virtual std::string description() {
		return "ProtocolVersionWorkload";
	}

	virtual Future<Void> start(Database const& cx) {
       return _start(this, cx);
	}

    ACTOR Future<Void> _start(ProtocolVersionWorkload* self, Database cx) {
        // CSimpleIni ini;
		// // state Reference<ClusterConnectionFile> connFile(new ClusterConnectionFile(self->clusterFilePath));
        // state const char* whitelistBinPaths = "";
        // state std::string dataFolder = "simfdb";
        // state LocalityData localities =  LocalityData(Optional<Standalone<StringRef>>(),
        //                                             Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
        //                                             Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
        //                                             Optional<Standalone<StringRef>>());
        // state std::string coordFolder = ini.GetValue(printable(localities.machineId()).c_str(), "coordinationFolder", "");
        // state ProcessClass processClass = ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource);
        // state uint16_t listenPerProcess = 1;
        // state uint16_t port = 1;
        // state IPAddress ip = IPAddress(0x01011F11);
        // state bool sslEnabled = false;
 
		// state ISimulator::ProcessInfo* process = g_pSimulator->newProcess("ProtocolVersionProcess", ip, port, sslEnabled, listenPerProcess,
        //                             localities, processClass, dataFolder.c_str(), coordFolder.c_str(), currentProtocolVersion);
        //                             // localities, processClass, dataFolder.c_str(), coordFolder.c_str(), ProtocolVersion(0x0FDB00B070010000LL));
        // wait(g_pSimulator->onProcess(process, TaskPriority::DefaultYield));

        // FlowTransport::createInstance(true, 1);
		// Sim2FileSystem::newFileSystem();

        // NetworkAddress n(ip, port, true, sslEnabled);
        // FlowTransport::transport().bind( n, n );

        // state vector<Future<Void>> actors;
        // actors.push_back(fdbd( cx->getConnectionFile(), localities, processClass, dataFolder, coordFolder, 500e6, "", "", -1, whitelistBinPaths));

        // getting coord protocols from current protocol version
        state vector<Future<ProtocolInfoReply>> coordProtocols;
        vector<NetworkAddress> coordAddresses = cx->getConnectionFile()->getConnectionString().coordinators();
        for(int i = 0; i<coordAddresses.size(); i++) {
            RequestStream<ProtocolInfoRequest> requestStream{ Endpoint{ { coordAddresses[i] }, WLTOKEN_PROTOCOL_INFO } };
            coordProtocols.push_back(retryBrokenPromise(requestStream, ProtocolInfoRequest{}));
        }

        wait(waitForAll(coordProtocols));

        // state std::vector<ISimulator::ProcessInfo*> allProcesses = g_pSimulator->getAllProcesses();
        // state int i = 0;
        // for(; i<allProcesses.size(); i++) {
        //     auto f = g_pSimulator->onProcess(allProcesses[i], TaskPriority::DefaultYield);
        //     wait(f);
        //     std::cout << "PROCESS PRO VESRION: " << g_network->protocolVersion().version() << std::endl;
        // }
        std::cout << "CURR VERSION: " << g_network->protocolVersion().version() << std::endl;
        std::vector<bool> protocolMatches;
        protocolMatches.reserve(coordProtocols.size());
        for(int i = 0; i<coordProtocols.size(); i++){
            if(g_network->protocolVersion() != coordProtocols[i].get().version) std::cout << "MISMATCHED VERSIONS" << std::endl;
            protocolMatches.push_back(g_network->protocolVersion() == coordProtocols[i].get().version);
        }

        // ASSERT(count(protocolMatches.begin(), protocolMatches.end(), false) >= 1);

        // g_pSimulator->killProcess(process, ISimulator::KillType::KillInstantly);
        // stopAfter(waitForAll(actors));
		return Void();
	}

    virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}

    ProtocolVersion protocol;
    std::string clusterFilePath;
};

WorkloadFactory<ProtocolVersionWorkload> ProtocolVersionWorkloadFactory("ProtocolVersion");
 