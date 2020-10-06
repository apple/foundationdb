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
        
    }

	virtual std::string description() {
		return "ProtocolVersionWorkload";
	}

	virtual Future<Void> start(Database const& cx) {
       return _start(this, cx);
	}

    ACTOR Future<Void> _start(ProtocolVersionWorkload* self, Database cx) {
        state ISimulator::ProcessInfo* oldProcess = g_pSimulator->getCurrentProcess();

        state std::vector<ISimulator::ProcessInfo*> allProcesses = g_pSimulator->getAllProcesses();
        state std::vector<ISimulator::ProcessInfo*>::iterator diffVersionProcess = find_if(allProcesses.begin(), allProcesses.end(), [](const ISimulator::ProcessInfo* p){
            return p->protocolVersion != currentProtocolVersion;
        });
        
        ASSERT(diffVersionProcess != allProcesses.end());
        wait(g_pSimulator->onProcess(*diffVersionProcess, TaskPriority::DefaultYield));

        // getting coord protocols from current protocol version
        state vector<Future<ProtocolInfoReply>> coordProtocols;
        vector<NetworkAddress> coordAddresses = cx->getConnectionFile()->getConnectionString().coordinators();
        for(int i = 0; i<coordAddresses.size(); i++) {
            RequestStream<ProtocolInfoRequest> requestStream{ Endpoint{ { coordAddresses[i] }, WLTOKEN_PROTOCOL_INFO } };
            coordProtocols.push_back(retryBrokenPromise(requestStream, ProtocolInfoRequest{}));
        }

        wait(waitForAll(coordProtocols));

        std::vector<bool> protocolMatches;
        protocolMatches.reserve(coordProtocols.size());
        for(int i = 0; i<coordProtocols.size(); i++){
            protocolMatches.push_back(g_network->protocolVersion() == coordProtocols[i].get().version);
        }

        ASSERT(count(protocolMatches.begin(), protocolMatches.end(), false) >= 1);

        // go back to orig process for consistency check
        wait(g_pSimulator->onProcess(oldProcess, TaskPriority::DefaultYield));
		return Void();
	}

    virtual Future<bool> check(Database const& cx) {
		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {
	}
};

WorkloadFactory<ProtocolVersionWorkload> ProtocolVersionWorkloadFactory("ProtocolVersion");
 