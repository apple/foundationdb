/*
 * MachineAttrition.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/FaultInjection.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static std::set<int> const& normalAttritionErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_please_reboot);
		s.insert(error_code_please_reboot_delete);
	}
	return s;
}

ACTOR Future<bool> ignoreSSFailuresForDuration(Database cx, double duration) {
	// duration doesn't matter since this won't timeout
	TraceEvent("IgnoreSSFailureStart").log();
	wait(success(setHealthyZone(cx, ignoreSSFailuresZoneString, 0)));
	TraceEvent("IgnoreSSFailureWait").log();
	wait(delay(duration));
	TraceEvent("IgnoreSSFailureClear").log();
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.clear(healthyZoneKey);
			wait(tr.commit());
			TraceEvent("IgnoreSSFailureComplete").log();
			return true;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

struct MachineAttritionWorkload : TestWorkload {
	bool enabled;
	int machinesToKill, machinesToLeave, workersToKill, workersToLeave;
	double testDuration, suspendDuration, liveDuration;
	bool reboot;
	bool killDc;
	bool killMachine;
	bool killDatahall;
	bool killProcess;
	bool killZone;
	bool killSelf;
	std::vector<std::string> targetIds;
	bool replacement;
	bool waitForVersion;
	bool allowFaultInjection;
	Future<bool> ignoreSSFailures;

	// This is set in setup from the list of workers when the cluster is started
	std::vector<LocalityData> machines;

	MachineAttritionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		// only do this on the "first" client, and only when in simulation and only when fault injection is enabled
		enabled = !clientId && g_network->isSimulated() && faultInjectionActivated;
		machinesToKill = getOption(options, LiteralStringRef("machinesToKill"), 2);
		machinesToLeave = getOption(options, LiteralStringRef("machinesToLeave"), 1);
		workersToKill = getOption(options, LiteralStringRef("workersToKill"), 2);
		workersToLeave = getOption(options, LiteralStringRef("workersToLeave"), 1);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		suspendDuration = getOption(options, LiteralStringRef("suspendDuration"), 1.0);
		liveDuration = getOption(options, LiteralStringRef("liveDuration"), 5.0);
		reboot = getOption(options, LiteralStringRef("reboot"), false);
		killDc = getOption(
		    options, LiteralStringRef("killDc"), g_network->isSimulated() && deterministicRandom()->random01() < 0.25);
		killMachine = getOption(options, LiteralStringRef("killMachine"), false);
		killDatahall = getOption(options, LiteralStringRef("killDatahall"), false);
		killProcess = getOption(options, LiteralStringRef("killProcess"), false);
		killZone = getOption(options, LiteralStringRef("killZone"), false);
		killSelf = getOption(options, LiteralStringRef("killSelf"), false);
		targetIds = getOption(options, LiteralStringRef("targetIds"), std::vector<std::string>());
		replacement =
		    getOption(options, LiteralStringRef("replacement"), reboot && deterministicRandom()->random01() < 0.5);
		waitForVersion = getOption(options, LiteralStringRef("waitForVersion"), false);
		allowFaultInjection = getOption(options, LiteralStringRef("allowFaultInjection"), true);
		ignoreSSFailures = true;
	}

	static std::vector<ISimulator::ProcessInfo*> getServers() {
		std::vector<ISimulator::ProcessInfo*> machines;
		std::vector<ISimulator::ProcessInfo*> all = g_simulator.getAllProcesses();
		for (int i = 0; i < all.size(); i++)
			if (!all[i]->failed && all[i]->name == std::string("Server") &&
			    all[i]->startingClass != ProcessClass::TesterClass)
				machines.push_back(all[i]);
		return machines;
	}

	std::string description() const override { return "MachineAttritionWorkload"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (enabled) {
			std::map<Optional<Standalone<StringRef>>, LocalityData> machineIDMap;
			auto processes = getServers();
			for (auto it = processes.begin(); it != processes.end(); ++it) {
				machineIDMap[(*it)->locality.zoneId()] = (*it)->locality;
			}
			machines.clear();
			for (auto it = machineIDMap.begin(); it != machineIDMap.end(); ++it) {
				machines.push_back(it->second);
			}
			deterministicRandom()->randomShuffle(machines);
			double meanDelay = testDuration / machinesToKill;
			TraceEvent("AttritionStarting")
			    .detail("KillDataCenters", killDc)
			    .detail("Reboot", reboot)
			    .detail("MachinesToLeave", machinesToLeave)
			    .detail("MachinesToKill", machinesToKill)
			    .detail("MeanDelay", meanDelay);

			return timeout(
			    reportErrorsExcept(
			        machineKillWorker(this, meanDelay, cx), "machineKillWorkerError", UID(), &normalAttritionErrors()),
			    testDuration,
			    Void());
		}
		if (!clientId && !g_network->isSimulated()) {
			return timeout(
			    reportErrorsExcept(
			        noSimMachineKillWorker(this, cx), "noSimMachineKillWorkerError", UID(), &normalAttritionErrors()),
			    testDuration,
			    Void());
		}
		if (killSelf)
			throw please_reboot();
		return Void();
	}
	Future<bool> check(Database const& cx) override { return ignoreSSFailures; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	static bool noSimIsViableKill(WorkerDetails worker) {
		return (worker.processClass != ProcessClass::ClassType::TesterClass);
	}

	template <typename Proc>
	static void sendRebootRequests(std::vector<WorkerDetails> workers,
	                               std::vector<std::string> targets,
	                               RebootRequest rbReq,
	                               Proc idAccess) {
		for (const auto& worker : workers) {
			// kill all matching workers
			if (idAccess(worker).present() &&
			    std::count(targets.begin(), targets.end(), idAccess(worker).get().toString())) {
				TraceEvent("SendingRebootRequest").detail("TargetWorker", worker.interf.locality.toString());
				worker.interf.clientInterface.reboot.send(rbReq);
			}
		}
	}

	ACTOR static Future<Void> noSimMachineKillWorker(MachineAttritionWorkload* self, Database cx) {
		ASSERT(!g_network->isSimulated());
		state int killedWorkers = 0;
		state std::vector<WorkerDetails> allWorkers =
		    wait(self->dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest()));
		// Can reuse reboot request to send to each interface since no reply promise needed
		state RebootRequest rbReq;
		if (self->reboot) {
			rbReq.waitForDuration = self->suspendDuration;
		} else {
			rbReq.waitForDuration = std::numeric_limits<uint32_t>::max();
		}
		state std::vector<WorkerDetails> workers;
		// Pre-processing step: remove all testers from list of workers
		for (const auto& worker : allWorkers) {
			if (noSimIsViableKill(worker)) {
				workers.push_back(worker);
			}
		}
		deterministicRandom()->randomShuffle(workers);
		wait(delay(self->liveDuration));
		// if a specific kill is requested, it must be accompanied by a set of target IDs otherwise no kills will occur
		if (self->killDc) {
			TraceEvent("Assassination").detail("TargetDataCenterIds", describe(self->targetIds));
			sendRebootRequests(workers,
			                   self->targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.dcId(); });
		} else if (self->killMachine) {
			TraceEvent("Assassination").detail("TargetMachineIds", describe(self->targetIds));
			sendRebootRequests(workers,
			                   self->targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.machineId(); });
		} else if (self->killDatahall) {
			TraceEvent("Assassination").detail("TargetDatahallIds", describe(self->targetIds));
			sendRebootRequests(workers,
			                   self->targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.dataHallId(); });
		} else if (self->killProcess) {
			TraceEvent("Assassination").detail("TargetProcessIds", describe(self->targetIds));
			sendRebootRequests(workers,
			                   self->targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.processId(); });
		} else if (self->killZone) {
			TraceEvent("Assassination").detail("TargetZoneIds", describe(self->targetIds));
			sendRebootRequests(workers,
			                   self->targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.zoneId(); });
		} else {
			while (killedWorkers < self->workersToKill && workers.size() > self->workersToLeave) {
				TraceEvent("WorkerKillBegin")
				    .detail("KilledWorkers", killedWorkers)
				    .detail("WorkersToKill", self->workersToKill)
				    .detail("WorkersToLeave", self->workersToLeave)
				    .detail("Workers", workers.size());
				if (self->waitForVersion) {
					state Transaction tr(cx);
					loop {
						try {
							tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
							tr.setOption(FDBTransactionOptions::LOCK_AWARE);
							wait(success(tr.getReadVersion()));
							break;
						} catch (Error& e) {
							wait(tr.onError(e));
						}
					}
				}
				// Pick a worker to kill
				state WorkerDetails targetWorker;
				targetWorker = workers.back();
				TraceEvent("Assassination")
				    .detail("TargetWorker", targetWorker.interf.locality.toString())
				    .detail("ZoneId", targetWorker.interf.locality.zoneId())
				    .detail("KilledWorkers", killedWorkers)
				    .detail("WorkersToKill", self->workersToKill)
				    .detail("WorkersToLeave", self->workersToLeave)
				    .detail("Workers", workers.size());
				targetWorker.interf.clientInterface.reboot.send(rbReq);
				killedWorkers++;
				workers.pop_back();
			}
		}
		return Void();
	}

	ACTOR static Future<Void> machineKillWorker(MachineAttritionWorkload* self, double meanDelay, Database cx) {
		state int killedMachines = 0;
		state double delayBeforeKill = deterministicRandom()->random01() * meanDelay;

		ASSERT(g_network->isSimulated());

		if (self->killDc) {
			wait(delay(delayBeforeKill));

			// decide on a machine to kill
			ASSERT(self->machines.size());
			Optional<Standalone<StringRef>> target = self->machines.back().dcId();

			ISimulator::KillType kt = ISimulator::Reboot;
			if (!self->reboot) {
				int killType = deterministicRandom()->randomInt(0, 3); // FIXME: enable disk stalls
				if (killType == 0)
					kt = ISimulator::KillInstantly;
				else if (killType == 1)
					kt = ISimulator::InjectFaults;
				else if (killType == 2)
					kt = ISimulator::RebootAndDelete;
				else
					kt = ISimulator::FailDisk;
			}
			TraceEvent("Assassination")
			    .detail("TargetDatacenter", target)
			    .detail("Reboot", self->reboot)
			    .detail("KillType", kt);

			g_simulator.killDataCenter(target, kt);
		} else {
			while (killedMachines < self->machinesToKill && self->machines.size() > self->machinesToLeave) {
				TraceEvent("WorkerKillBegin")
				    .detail("KilledMachines", killedMachines)
				    .detail("MachinesToKill", self->machinesToKill)
				    .detail("MachinesToLeave", self->machinesToLeave)
				    .detail("Machines", self->machines.size());
				TEST(true); // Killing a machine

				wait(delay(delayBeforeKill));
				TraceEvent("WorkerKillAfterDelay").log();

				if (self->waitForVersion) {
					state Transaction tr(cx);
					loop {
						try {
							tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
							tr.setOption(FDBTransactionOptions::LOCK_AWARE);
							wait(success(tr.getReadVersion()));
							break;
						} catch (Error& e) {
							wait(tr.onError(e));
						}
					}
				}

				// decide on a machine to kill
				state LocalityData targetMachine = self->machines.back();
				if (BUGGIFY_WITH_PROB(0.01)) {
					TEST(true); // Marked a zone for maintenance before killing it
					wait(success(
					    setHealthyZone(cx, targetMachine.zoneId().get(), deterministicRandom()->random01() * 20)));
				} else if (BUGGIFY_WITH_PROB(0.005)) {
					TEST(true); // Disable DD for all storage server failures
					self->ignoreSSFailures =
					    uncancellable(ignoreSSFailuresForDuration(cx, deterministicRandom()->random01() * 5));
				}

				TraceEvent("Assassination")
				    .detail("TargetMachine", targetMachine.toString())
				    .detail("ZoneId", targetMachine.zoneId())
				    .detail("Reboot", self->reboot)
				    .detail("KilledMachines", killedMachines)
				    .detail("MachinesToKill", self->machinesToKill)
				    .detail("MachinesToLeave", self->machinesToLeave)
				    .detail("Machines", self->machines.size())
				    .detail("Replace", self->replacement);

				if (self->reboot) {
					if (deterministicRandom()->random01() > 0.5) {
						g_simulator.rebootProcess(targetMachine.zoneId(), deterministicRandom()->random01() > 0.5);
					} else {
						g_simulator.killZone(targetMachine.zoneId(), ISimulator::Reboot);
					}
				} else {
					auto randomDouble = deterministicRandom()->random01();
					TraceEvent("WorkerKill")
					    .detail("MachineCount", self->machines.size())
					    .detail("RandomValue", randomDouble);
					if (randomDouble < 0.33) {
						TraceEvent("RebootAndDelete").detail("TargetMachine", targetMachine.toString());
						g_simulator.killZone(targetMachine.zoneId(), ISimulator::RebootAndDelete);
					} else {
						auto kt = ISimulator::KillInstantly;
						if (self->allowFaultInjection) {
							if (randomDouble < 0.50) {
								kt = ISimulator::InjectFaults;
							}
							// FIXME: enable disk stalls
							/*
							if( randomDouble < 0.56 ) {
							    kt = ISimulator::InjectFaults;
							} else if( randomDouble < 0.66 ) {
							    kt = ISimulator::FailDisk;
							}
							*/
						}
						g_simulator.killZone(targetMachine.zoneId(), kt);
					}
				}

				killedMachines++;
				if (!self->replacement)
					self->machines.pop_back();

				wait(delay(meanDelay - delayBeforeKill) && success(self->ignoreSSFailures));

				delayBeforeKill = deterministicRandom()->random01() * meanDelay;
				TraceEvent("WorkerKillAfterMeanDelay").detail("DelayBeforeKill", delayBeforeKill);
			}
		}

		if (self->killSelf)
			throw please_reboot();
		return Void();
	}
};

WorkloadFactory<MachineAttritionWorkload> MachineAttritionWorkloadFactory("Attrition");
