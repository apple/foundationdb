/*
 * SaveAndKill.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "boost/algorithm/string/predicate.hpp"

#undef state
#include "fdbclient/SimpleIni.h"
#define state
#undef max
#undef min
#include "flow/actorcompiler.h" // This must be the last #include.

struct SaveAndKillWorkload : TestWorkload {
	static constexpr auto NAME = "SaveAndKill";

	std::string restartInfo;
	double testDuration;
	int isRestoring;

	SaveAndKillWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		restartInfo = getOption(options, "restartInfoLocation"_sr, "simfdb/restartInfo.ini"_sr).toString();
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		isRestoring = getOption(options, "isRestoring"_sr, 0);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }
	Future<Void> setup(Database const& cx) override {
		g_simulator->disableSwapsToAll();
		return Void();
	}
	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	ACTOR Future<Void> _start(SaveAndKillWorkload* self, Database cx) {
		state int i;
		wait(delay(deterministicRandom()->random01() * self->testDuration));

		CSimpleIni ini;
		ini.SetUnicode();
		ini.LoadFile(self->restartInfo.c_str());

		ini.SetValue("RESTORE", "isRestoring", format("%d", self->isRestoring).c_str());
		ini.SetValue("META", "processesPerMachine", format("%d", g_simulator->processesPerMachine).c_str());
		ini.SetValue("META", "listenersPerProcess", format("%d", g_simulator->listenersPerProcess).c_str());
		ini.SetValue("META", "desiredCoordinators", format("%d", g_simulator->desiredCoordinators).c_str());
		ini.SetValue("META", "connectionString", g_simulator->connectionString.c_str());
		ini.SetValue("META", "testerCount", format("%d", g_simulator->testerCount).c_str());
		ini.SetValue("META", "tssMode", format("%d", g_simulator->tssMode).c_str());
		ini.SetValue("META", "mockDNS", INetworkConnections::net()->convertMockDNSToString().c_str());
		ini.SetValue("META", "tenantMode", cx->clientInfo->get().tenantMode.toValue().toString().c_str());
		if (cx->defaultTenant.present()) {
			ini.SetValue("META", "defaultTenant", cx->defaultTenant.get().toString().c_str());
		}

		ini.SetBoolValue("META", "enableEncryption", SERVER_KNOBS->ENABLE_ENCRYPTION);
		ini.SetBoolValue("META", "enableTLogEncryption", SERVER_KNOBS->ENABLE_TLOG_ENCRYPTION);
		ini.SetBoolValue("META", "enableStorageServerEncryption", SERVER_KNOBS->ENABLE_STORAGE_SERVER_ENCRYPTION);
		ini.SetBoolValue("META", "enableBlobGranuleEncryption", SERVER_KNOBS->ENABLE_BLOB_GRANULE_ENCRYPTION);

		std::vector<ISimulator::ProcessInfo*> processes = g_simulator->getAllProcesses();
		std::map<NetworkAddress, ISimulator::ProcessInfo*> rebootingProcesses =
		    g_simulator->currentlyRebootingProcesses;
		std::map<std::string, ISimulator::ProcessInfo*> allProcessesMap;
		for (const auto& [_, process] : rebootingProcesses) {
			if (allProcessesMap.find(process->dataFolder) == allProcessesMap.end() && !process->isSpawnedKVProcess()) {
				allProcessesMap[process->dataFolder] = process;
			}
		}
		for (const auto& process : processes) {
			if (allProcessesMap.find(process->dataFolder) == allProcessesMap.end() && !process->isSpawnedKVProcess()) {
				allProcessesMap[process->dataFolder] = process;
			}
		}
		ini.SetValue("META", "processCount", format("%d", allProcessesMap.size() - 1).c_str());
		std::map<std::string, int> machines;

		int j = 0;
		for (const auto& [_, process] : allProcessesMap) {
			std::string machineId = printable(process->locality.machineId());
			const char* machineIdString = machineId.c_str();
			if (!process->excludeFromRestarts) {
				if (machines.find(machineId) == machines.end()) {
					machines.insert(std::pair<std::string, int>(machineId, 1));
					ini.SetValue("META", format("%d", j).c_str(), machineIdString);
					ini.SetValue(
					    machineIdString,
					    "dcUID",
					    (process->locality.dcId().present()) ? process->locality.dcId().get().printable().c_str() : "");
					ini.SetValue(machineIdString,
					             "zoneId",
					             (process->locality.zoneId().present())
					                 ? process->locality.zoneId().get().printable().c_str()
					                 : "");
					ini.SetValue(machineIdString, "mClass", format("%d", process->startingClass.classType()).c_str());
					ini.SetValue(machineIdString,
					             format("ipAddr%d", process->address.port - 1).c_str(),
					             process->address.ip.toString().c_str());
					ini.SetValue(
					    machineIdString, format("%d", process->address.port - 1).c_str(), process->dataFolder.c_str());
					ini.SetValue(machineIdString,
					             format("c%d", process->address.port - 1).c_str(),
					             process->coordinationFolder.c_str());
					j++;
				} else {
					ini.SetValue(machineIdString,
					             format("ipAddr%d", process->address.port - 1).c_str(),
					             process->address.ip.toString().c_str());
					int oldValue = machines.find(machineId)->second;
					ini.SetValue(
					    machineIdString, format("%d", process->address.port - 1).c_str(), process->dataFolder.c_str());
					ini.SetValue(machineIdString,
					             format("c%d", process->address.port - 1).c_str(),
					             process->coordinationFolder.c_str());
					machines.erase(machines.find(machineId));
					machines.insert(std::pair<std::string, int>(machineId, oldValue + 1));
				}
			}
		}
		for (auto entry = machines.begin(); entry != machines.end(); entry++) {
			ini.SetValue((*entry).first.c_str(), "processes", format("%d", (*entry).second).c_str());
		}

		ini.SetValue("META", "machineCount", format("%d", machines.size()).c_str());
		ini.SaveFile(self->restartInfo.c_str());

		for (auto process = allProcessesMap.begin(); process != allProcessesMap.end(); process++) {
			g_simulator->killProcess(process->second, ISimulator::Reboot);
		}

		for (i = 0; i < 100; i++) {
			wait(delay(0.0));
		}

		g_simulator->stop();

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>&) override {}
};

WorkloadFactory<SaveAndKillWorkload> SaveAndKillWorkloadFactory;
