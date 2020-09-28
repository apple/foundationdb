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
#include <string>
#include <thread>
#include <vector>
#include "fdbclient/CoordinationInterface.h"
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

ACTOR Future<Void> getProtocol(Endpoint endpoint) {
    std::vector<Future<ProtocolInfoReply>> coordProtocols;
    RequestStream<ProtocolInfoRequest> requestStream{ endpoint };
    auto f = retryBrokenPromise(requestStream, ProtocolInfoRequest{});
    ProtocolInfoReply res = wait(f);

    std::cout << "GOT VERSION: " << res.version.version() << std::endl;
    return Void();
}

struct _Struct {
    static constexpr FileIdentifier file_identifier = 2340487;
    int oldField = 0;
};

struct NewStruct : public _Struct {
    int newField = 0;

    bool isSet() const {
        return oldField == 1 && newField == 2;
    }
    void setFields() {
        oldField = 1;
        newField = 2;
    }

    template <class Archive>
    void serialize(Archive& ar) {
        serializer(ar, oldField, newField);
    }
};

ACTOR static Future<Void> writeNew(Database cx, int numObjects, Key key) {
    ProtocolVersion protocolVersion = ProtocolVersion(0x0FDB00B070010000LL);
    protocolVersion.addObjectSerializerFlag();
    ObjectWriter writer(IncludeVersion(protocolVersion));
    std::vector<NewStruct> data(numObjects);
    for (auto& newObject : data) {
        newObject.setFields();
    }
    writer.serialize(data);
    state Value value = writer.toStringRef();

    state Transaction tr(cx);
    loop {
        try {
            tr.set(key, value);
            wait(tr.commit());
            return Void();
        } catch (Error& e) {
            wait(tr.onError(e));
        }
    }
}

struct ProtocolVersionWorkload : TestWorkload {
    ProtocolVersionWorkload(WorkloadContext const& wcx)
	: TestWorkload(wcx) {
		protocol = ProtocolVersion(getOption(options, LiteralStringRef("protocolVersion"), 0.0));
        clusterFilePath = getOption(options, LiteralStringRef("clusterFilePath"), LiteralStringRef("")).toString();
	}

	virtual std::string description() {
		return "ProtocolVersionWorkload";
	}

	virtual Future<Void> start(Database const& cx) {
       return _start(this, cx);
	}

    ACTOR Future<Void> _start(ProtocolVersionWorkload* self, Database cx) {
        CSimpleIni ini;
		state Reference<ClusterConnectionFile> connFile(new ClusterConnectionFile(self->clusterFilePath));
        state const char* whitelistBinPaths = "";
        state std::string dataFolder = "simfdb";
        state LocalityData localities =  LocalityData(Optional<Standalone<StringRef>>(),
                                                    Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
                                                    Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
                                                    Optional<Standalone<StringRef>>());
        state std::string coordFolder = ini.GetValue(printable(localities.machineId()).c_str(), "coordinationFolder", "");
        state ProcessClass processClass = ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource);
        state uint16_t listenPerProcess = 1;
        state uint16_t port = 1;
        state IPAddress ip = IPAddress(0x01010101);
        state bool sslEnabled = false;
 
		state ISimulator::ProcessInfo* process = g_pSimulator->newProcess("ProtocolVersionProcess", ip, port, sslEnabled, listenPerProcess,
                                    // localities, processClass, dataFolder.c_str(), coordFolder.c_str(), currentProtocolVersion);
                                    localities, processClass, dataFolder.c_str(), coordFolder.c_str(), ProtocolVersion(0x0FDB00B070010000LL));
        wait(g_pSimulator->onProcess(process, TaskPriority::DefaultYield));

        FlowTransport::createInstance(true, 1);
		Sim2FileSystem::newFileSystem();

        NetworkAddress n(ip, port, true, sslEnabled);
        FlowTransport::transport().bind( n, n );

        state vector<Future<Void>> actors;
        actors.push_back(fdbd( connFile, localities, processClass, dataFolder, coordFolder, 500e6, "", "", -1, whitelistBinPaths));

        state std::vector<ISimulator::ProcessInfo*> allProcesses = g_pSimulator->getAllProcesses();
        state ISimulator::ProcessInfo* nextProcess = nullptr;
        for(ISimulator::ProcessInfo* p : allProcesses){
            if(p->address != process->address){
                nextProcess = p;
                break;
            }
        }

        ASSERT(nextProcess);
        wait(g_pSimulator->onProcess(nextProcess));

        wait(getProtocol(Endpoint{{process->addresses}, WLTOKEN_PROTOCOL_INFO}));
        stopAfter(waitForAll(actors));

        // wait(writeNew(cx, 1, LiteralStringRef("TEST")));
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
 