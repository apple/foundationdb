/*
 * MachineAttrition.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/FaultInjection.h"
#include "flow/DeterministicRandom.h"
#include "fdbrpc/SimulatorProcessInfo.h"
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

struct MachineAttritionWorkload : FailureInjectionWorkload {
	static constexpr auto NAME = "Attrition";
	bool enabled;
	int machinesToKill = 2, machinesToLeave = 1, workersToKill = 2, workersToLeave = 1;
	double testDuration = 10.0, suspendDuration = 1.0, liveDuration = 5.0;
	bool iterate = false;
	bool reboot = false;
	bool killDc = false;
	bool killMachine = false;
	bool killDatahall = false;
	bool killProcess = false;
	bool killZone = false;
	bool killSelf = false;
	bool killAll = false;
	std::vector<std::string> targetIds;
	bool replacement = false;
	bool waitForVersion = false;
	bool allowFaultInjection = true;
	Future<bool> ignoreSSFailures = true;
	double maxRunDuration = 60.0, backoff = 1.5;

	// This is set in setup from the list of workers when the cluster is started
	std::vector<LocalityData> machines;

	MachineAttritionWorkload(WorkloadContext const& wcx, NoOptions) : FailureInjectionWorkload(wcx) {
		enabled = !clientId && g_network->isSimulated() && faultInjectionActivated;
		suspendDuration = 10.0;
		iterate = true;
	}

	MachineAttritionWorkload(WorkloadContext const& wcx) : FailureInjectionWorkload(wcx) {
		// only do this on the "first" client, and only when in simulation and only when fault injection is enabled
		enabled = !clientId && g_network->isSimulated() && faultInjectionActivated;
		machinesToKill = getOption(options, "machinesToKill"_sr, machinesToKill);
		machinesToLeave = getOption(options, "machinesToLeave"_sr, machinesToLeave);
		workersToKill = getOption(options, "workersToKill"_sr, workersToKill);
		workersToLeave = getOption(options, "workersToLeave"_sr, workersToLeave);
		testDuration = getOption(options, "testDuration"_sr, testDuration);
		suspendDuration = getOption(options, "suspendDuration"_sr, suspendDuration);
		liveDuration = getOption(options, "liveDuration"_sr, liveDuration);
		reboot = getOption(options, "reboot"_sr, reboot);
		killDc = getOption(options, "killDc"_sr, g_network->isSimulated() && deterministicRandom()->random01() < 0.25);
		killMachine = getOption(options, "killMachine"_sr, killMachine);
		killDatahall = getOption(options, "killDatahall"_sr, killDatahall);
		killProcess = getOption(options, "killProcess"_sr, killProcess);
		killZone = getOption(options, "killZone"_sr, killZone);
		killSelf = getOption(options, "killSelf"_sr, killSelf);
		killAll =
		    getOption(options,
		              "killAll"_sr,
		              g_network->isSimulated() && !g_simulator->extraDatabases.empty() && BUGGIFY_WITH_PROB(0.01));
		targetIds = getOption(options, "targetIds"_sr, std::vector<std::string>());
		replacement = getOption(options, "replacement"_sr, reboot && deterministicRandom()->random01() < 0.5);
		waitForVersion = getOption(options, "waitForVersion"_sr, waitForVersion);
		allowFaultInjection = getOption(options, "allowFaultInjection"_sr, allowFaultInjection);
	}

	bool shouldInject(DeterministicRandom& random,
	                  const WorkloadRequest& work,
	                  const unsigned alreadyAdded) const override {
		if (g_network->isSimulated() && !g_simulator->extraDatabases.empty()) {
			// Remove this as soon as we track extra databases properly
			return false;
		}
		return work.useDatabase && random.random01() < 1.0 / (2.0 + alreadyAdded);
	}

	void initializeForInjection(DeterministicRandom& random) {
		reboot = random.random01() < 0.25;
		replacement = random.random01() < 0.25;
		allowFaultInjection = random.random01() < 0.5;
		suspendDuration = 10.0 * random.random01();
		if (g_network->isSimulated()) {
			std::set<Optional<StringRef>> dataCenters;
			std::set<Optional<StringRef>> dataHalls;
			std::set<Optional<StringRef>> zones;
			for (auto process : g_simulator->getAllProcesses()) {
				dataCenters.emplace(process->locality.dcId().castTo<StringRef>());
				dataHalls.emplace(process->locality.dataHallId().castTo<StringRef>());
				zones.emplace(process->locality.zoneId().castTo<StringRef>());
			}
			killDc = dataCenters.size() > 0 && random.random01() > (dataHalls.size() < 0 ? 0.1 : 0.25);
			killDatahall = dataHalls.size() > 0 && killDc && random.random01() < 0.5;
			killZone = zones.size() > 0 && random.random01() < 0.2;
		}
		TraceEvent("AddingFailureInjection")
		    .detail("Reboot", reboot)
		    .detail("Replacement", replacement)
		    .detail("AllowFaultInjection", allowFaultInjection)
		    .detail("KillDC", killDc)
		    .detail("KillDataHall", killDatahall)
		    .detail("KillZone", killZone);
	}

	static std::vector<ISimulator::ProcessInfo*> getServers() {
		std::vector<ISimulator::ProcessInfo*> machines;
		std::vector<ISimulator::ProcessInfo*> all = g_simulator->getAllProcesses();
		for (int i = 0; i < all.size(); i++)
			if (!all[i]->failed && all[i]->name == std::string("Server") &&
			    all[i]->startingClass != ProcessClass::TesterClass)
				machines.push_back(all[i]);
		return machines;
	}

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
		// if a specific kill is requested, it must be accompanied by a set of target IDs otherwise no kills will
		// occur
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
		ASSERT(g_network->isSimulated());
		state double delayBeforeKill;
		state double suspendDuration = self->suspendDuration;
		state double startTime = now();

		loop {
			if (self->killDc) {
				delayBeforeKill = deterministicRandom()->random01() * meanDelay;
				wait(delay(delayBeforeKill));

				// decide on a machine to kill
				ASSERT(self->machines.size());
				Optional<Standalone<StringRef>> target = self->machines.back().dcId();

				ISimulator::KillType kt = ISimulator::KillType::Reboot;
				if (!self->reboot) {
					int killType = deterministicRandom()->randomInt(0, 3); // FIXME: enable disk stalls
					if (killType == 0)
						kt = ISimulator::KillType::KillInstantly;
					else if (killType == 1)
						kt = ISimulator::KillType::InjectFaults;
					else if (killType == 2)
						kt = ISimulator::KillType::RebootAndDelete;
					else
						kt = ISimulator::KillType::FailDisk;
				}
				TraceEvent("Assassination")
				    .detail("TargetDatacenter", target)
				    .detail("Reboot", self->reboot)
				    .detail("KillType", kt);

				g_simulator->killDataCenter(target, kt);
			} else if (self->killDatahall) {
				delayBeforeKill = deterministicRandom()->random01() * meanDelay;
				wait(delay(delayBeforeKill));

				// It only makes sense to kill a single data hall.
				ASSERT(self->targetIds.size() == 1);
				auto target = self->targetIds.front();

				auto kt = ISimulator::KillType::KillInstantly;
				TraceEvent("Assassination").detail("TargetDataHall", target).detail("KillType", kt);

				g_simulator->killDataHall(target, kt);
			} else if (self->killAll) {
				state ISimulator::KillType kt = ISimulator::KillType::RebootProcessAndSwitch;
				TraceEvent("Assassination").detail("KillType", kt);
				g_simulator->killAll(kt, true);
				g_simulator->toggleGlobalSwitchCluster();
				wait(delay(self->testDuration / 2));
				g_simulator->killAll(kt, true);
				g_simulator->toggleGlobalSwitchCluster();
			} else {
				state int killedMachines = 0;
				while (killedMachines < self->machinesToKill && self->machines.size() > self->machinesToLeave) {
					TraceEvent("WorkerKillBegin")
					    .detail("KilledMachines", killedMachines)
					    .detail("MachinesToKill", self->machinesToKill)
					    .detail("MachinesToLeave", self->machinesToLeave)
					    .detail("Machines", self->machines.size());
					CODE_PROBE(true, "Killing a machine");

					delayBeforeKill = deterministicRandom()->random01() * meanDelay;
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
						CODE_PROBE(true, "Marked a zone for maintenance before killing it");
						wait(success(
						    setHealthyZone(cx, targetMachine.zoneId().get(), deterministicRandom()->random01() * 20)));
					} else if (!g_simulator->willRestart && BUGGIFY_WITH_PROB(0.005)) {
						// don't do this in restarting test, since test could exit before it is unset, and restarted
						// test would never unset it
						CODE_PROBE(true, "Disable DD for all storage server failures");
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
							g_simulator->rebootProcess(targetMachine.zoneId(), deterministicRandom()->random01() > 0.5);
						} else {
							g_simulator->killZone(targetMachine.zoneId(), ISimulator::KillType::Reboot);
						}
					} else {
						auto randomDouble = deterministicRandom()->random01();
						TraceEvent("WorkerKill")
						    .detail("MachineCount", self->machines.size())
						    .detail("RandomValue", randomDouble);
						if (randomDouble < 0.33) {
							TraceEvent("RebootAndDelete").detail("TargetMachine", targetMachine.toString());
							g_simulator->killZone(targetMachine.zoneId(), ISimulator::KillType::RebootAndDelete);
						} else {
							auto kt = ISimulator::KillType::KillInstantly;
							if (self->allowFaultInjection) {
								if (randomDouble < 0.50) {
									kt = ISimulator::KillType::InjectFaults;
								}
								// FIXME: enable disk stalls
								/*
								if( randomDouble < 0.56 ) {
								    kt = ISimulator::KillType::InjectFaults;
								} else if( randomDouble < 0.66 ) {
								    kt = ISimulator::KillType::FailDisk;
								}
								*/
							}
							g_simulator->killZone(targetMachine.zoneId(), kt);
						}
					}

					killedMachines++;
					if (self->replacement) {
						// Replace by reshuffling, since we always pick from the back.
						deterministicRandom()->randomShuffle(self->machines);
					} else {
						self->machines.pop_back();
					}

					wait(delay(meanDelay - delayBeforeKill) && success(self->ignoreSSFailures));

					delayBeforeKill = deterministicRandom()->random01() * meanDelay;
					TraceEvent("WorkerKillAfterMeanDelay").detail("DelayBeforeKill", delayBeforeKill);
				}
			}
			if (!self->iterate || now() - startTime > self->maxRunDuration) {
				break;
			} else {
				wait(delay(suspendDuration));
				suspendDuration *= self->backoff;
			}
		}

		if (self->killSelf)
			throw please_reboot();
		return Void();
	}
};

WorkloadFactory<MachineAttritionWorkload> MachineAttritionWorkloadFactory;
FailureInjectorFactory<MachineAttritionWorkload> MachineAttritionFailureWorkloadFactory;
