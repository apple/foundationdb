/*
 * MachineAttrition.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ManagementAPI.h"
#include "flow/FaultInjection.h"
#include "flow/DeterministicRandom.h"
#include "fdbrpc/SimulatorProcessInfo.h"

static std::set<int> const& normalAttritionErrors() {
	static std::set<int> s;
	if (s.empty()) {
		s.insert(error_code_please_reboot);
		s.insert(error_code_please_reboot_delete);
	}
	return s;
}

Future<bool> ignoreSSFailuresForDuration(Database cx, double duration) {
	// duration doesn't matter since this won't timeout
	TraceEvent("IgnoreSSFailureStart").log();
	co_await setHealthyZone(cx, ignoreSSFailuresZoneString, 0);
	TraceEvent("IgnoreSSFailureWait").log();
	co_await delay(duration);
	TraceEvent("IgnoreSSFailureClear").log();
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.clear(healthyZoneKey);
			co_await tr.commit();
			TraceEvent("IgnoreSSFailureComplete").log();
			co_return true;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
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
		killAll = getOption(options,
		                    "killAll"_sr,
		                    g_network->isSimulated() && !fdbSimulationPolicyState().extraDatabases.empty() &&
		                        BUGGIFY_WITH_PROB(0.01));
		targetIds = getOption(options, "targetIds"_sr, std::vector<std::string>());
		replacement = getOption(options, "replacement"_sr, reboot && deterministicRandom()->random01() < 0.5);
		waitForVersion = getOption(options, "waitForVersion"_sr, waitForVersion);
		allowFaultInjection = getOption(options, "allowFaultInjection"_sr, allowFaultInjection);
	}

	bool shouldInject(DeterministicRandom& random,
	                  const WorkloadRequest& work,
	                  const unsigned alreadyAdded) const override {
		if (g_network->isSimulated() && !fdbSimulationPolicyState().extraDatabases.empty()) {
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
			killDc = !dataCenters.empty() && random.random01() > (dataHalls.size() < 0 ? 0.1 : 0.25);
			killDatahall = !dataHalls.empty() && killDc && random.random01() < 0.5;
			killZone = !zones.empty() && random.random01() < 0.2;
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
			        machineKillWorker(meanDelay, cx), "machineKillWorkerError", UID(), &normalAttritionErrors()),
			    testDuration,
			    Void());
		}
		if (!clientId && !g_network->isSimulated()) {
			return timeout(
			    reportErrorsExcept(
			        noSimMachineKillWorker(cx), "noSimMachineKillWorkerError", UID(), &normalAttritionErrors()),
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
			    std::find(targets.begin(), targets.end(), idAccess(worker).get().toString()) != targets.end()) {
				TraceEvent("SendingRebootRequest").detail("TargetWorker", worker.interf.locality.toString());
				worker.interf.clientInterface.reboot.send(rbReq);
			}
		}
	}

	Future<Void> noSimMachineKillWorker(Database cx) {
		ASSERT(!g_network->isSimulated());
		int killedWorkers = 0;
		std::vector<WorkerDetails> allWorkers =
		    co_await dbInfo->get().clusterInterface.getWorkers.getReply(GetWorkersRequest());
		// Can reuse reboot request to send to each interface since no reply promise needed
		RebootRequest rbReq;
		if (reboot) {
			rbReq.waitForDuration = suspendDuration;
		} else {
			rbReq.waitForDuration = std::numeric_limits<uint32_t>::max();
		}
		std::vector<WorkerDetails> workers;
		// Pre-processing step: remove all testers from list of workers
		for (const auto& worker : allWorkers) {
			if (noSimIsViableKill(worker)) {
				workers.push_back(worker);
			}
		}
		deterministicRandom()->randomShuffle(workers);
		co_await delay(liveDuration);
		// if a specific kill is requested, it must be accompanied by a set of target IDs otherwise no kills will
		// occur
		if (killDc) {
			TraceEvent("Assassination").detail("TargetDataCenterIds", describe(targetIds));
			sendRebootRequests(workers,
			                   targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.dcId(); });
		} else if (killMachine) {
			TraceEvent("Assassination").detail("TargetMachineIds", describe(targetIds));
			sendRebootRequests(workers,
			                   targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.machineId(); });
		} else if (killDatahall) {
			TraceEvent("Assassination").detail("TargetDatahallIds", describe(targetIds));
			sendRebootRequests(workers,
			                   targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.dataHallId(); });
		} else if (killProcess) {
			TraceEvent("Assassination").detail("TargetProcessIds", describe(targetIds));
			sendRebootRequests(workers,
			                   targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.processId(); });
		} else if (killZone) {
			TraceEvent("Assassination").detail("TargetZoneIds", describe(targetIds));
			sendRebootRequests(workers,
			                   targetIds,
			                   rbReq,
			                   // idAccess lambda
			                   [](WorkerDetails worker) { return worker.interf.locality.zoneId(); });
		} else {
			while (killedWorkers < workersToKill && workers.size() > workersToLeave) {
				TraceEvent("WorkerKillBegin")
				    .detail("KilledWorkers", killedWorkers)
				    .detail("WorkersToKill", workersToKill)
				    .detail("WorkersToLeave", workersToLeave)
				    .detail("Workers", workers.size());
				if (waitForVersion) {
					Transaction tr(cx);
					while (true) {
						Error err;
						try {
							tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
							tr.setOption(FDBTransactionOptions::LOCK_AWARE);
							co_await tr.getReadVersion();
							break;
						} catch (Error& e) {
							err = e;
						}
						co_await tr.onError(err);
					}
				}
				// Pick a worker to kill
				WorkerDetails targetWorker;
				targetWorker = workers.back();
				TraceEvent("Assassination")
				    .detail("TargetWorker", targetWorker.interf.locality.toString())
				    .detail("ZoneId", targetWorker.interf.locality.zoneId())
				    .detail("KilledWorkers", killedWorkers)
				    .detail("WorkersToKill", workersToKill)
				    .detail("WorkersToLeave", workersToLeave)
				    .detail("Workers", workers.size());
				targetWorker.interf.clientInterface.reboot.send(rbReq);
				killedWorkers++;
				workers.pop_back();
			}
		}
	}

	Future<Void> machineKillWorker(double meanDelay, Database cx) {
		ASSERT(g_network->isSimulated());
		double delayBeforeKill{ 0 };
		double suspendDuration = this->suspendDuration;
		double startTime = now();

		while (true) {
			if (killDc) {
				delayBeforeKill = deterministicRandom()->random01() * meanDelay;
				co_await delay(delayBeforeKill);

				// decide on a machine to kill
				ASSERT(!machines.empty());
				Optional<Standalone<StringRef>> target = machines.back().dcId();

				ISimulator::KillType kt = ISimulator::KillType::Reboot;
				if (!reboot) {
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
				    .detail("Reboot", reboot)
				    .detail("KillType", kt);

				g_simulator->killDataCenter(target, kt);
			} else if (killDatahall) {
				delayBeforeKill = deterministicRandom()->random01() * meanDelay;
				co_await delay(delayBeforeKill);

				// It only makes sense to kill a single data hall.
				ASSERT(targetIds.size() == 1);
				auto target = targetIds.front();

				auto kt = ISimulator::KillType::KillInstantly;
				TraceEvent("Assassination").detail("TargetDataHall", target).detail("KillType", kt);

				g_simulator->killDataHall(target, kt);
			} else if (killAll) {
				ISimulator::KillType kt = ISimulator::KillType::RebootProcessAndSwitch;
				TraceEvent("Assassination").detail("KillType", kt);
				g_simulator->killAll(kt, true);
				g_simulator->toggleGlobalSwitchCluster();
				co_await delay(testDuration / 2);
				g_simulator->killAll(kt, true);
				g_simulator->toggleGlobalSwitchCluster();
			} else {
				int killedMachines = 0;
				while (killedMachines < machinesToKill && machines.size() > machinesToLeave) {
					TraceEvent("WorkerKillBegin")
					    .detail("KilledMachines", killedMachines)
					    .detail("MachinesToKill", machinesToKill)
					    .detail("MachinesToLeave", machinesToLeave)
					    .detail("Machines", machines.size());
					CODE_PROBE(true, "Killing a machine");

					delayBeforeKill = deterministicRandom()->random01() * meanDelay;
					co_await delay(delayBeforeKill);
					TraceEvent("WorkerKillAfterDelay").log();

					if (waitForVersion) {
						Transaction tr(cx);
						while (true) {
							Error err;
							try {
								tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
								tr.setOption(FDBTransactionOptions::LOCK_AWARE);
								co_await tr.getReadVersion();
								break;
							} catch (Error& e) {
								err = e;
							}
							co_await tr.onError(err);
						}
					}

					// decide on a machine to kill
					LocalityData targetMachine = machines.back();
					if (BUGGIFY_WITH_PROB(0.01)) {
						CODE_PROBE(true, "Marked a zone for maintenance before killing it");
						co_await setHealthyZone(
						    cx, targetMachine.zoneId().get(), deterministicRandom()->random01() * 20);
					} else if (!fdbSimulationPolicyState().willRestart && BUGGIFY_WITH_PROB(0.005)) {
						// don't do this in restarting test, since test could exit before it is unset, and restarted
						// test would never unset it
						CODE_PROBE(true, "Disable DD for all storage server failures");
						ignoreSSFailures =
						    uncancellable(ignoreSSFailuresForDuration(cx, deterministicRandom()->random01() * 5));
					}

					TraceEvent("Assassination")
					    .detail("TargetMachine", targetMachine.toString())
					    .detail("ZoneId", targetMachine.zoneId())
					    .detail("Reboot", reboot)
					    .detail("KilledMachines", killedMachines)
					    .detail("MachinesToKill", machinesToKill)
					    .detail("MachinesToLeave", machinesToLeave)
					    .detail("Machines", machines.size())
					    .detail("Replace", replacement);

					if (reboot) {
						if (deterministicRandom()->random01() > 0.5) {
							g_simulator->rebootProcess(targetMachine.zoneId(), deterministicRandom()->random01() > 0.5);
						} else {
							g_simulator->killZone(targetMachine.zoneId(), ISimulator::KillType::Reboot);
						}
					} else {
						auto randomDouble = deterministicRandom()->random01();
						TraceEvent("WorkerKill")
						    .detail("MachineCount", machines.size())
						    .detail("RandomValue", randomDouble);
						if (randomDouble < 0.33) {
							TraceEvent("RebootAndDelete").detail("TargetMachine", targetMachine.toString());
							g_simulator->killZone(targetMachine.zoneId(), ISimulator::KillType::RebootAndDelete);
						} else {
							auto kt = ISimulator::KillType::KillInstantly;
							if (allowFaultInjection) {
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
					if (replacement) {
						// Replace by reshuffling, since we always pick from the back.
						deterministicRandom()->randomShuffle(machines);
					} else {
						machines.pop_back();
					}

					co_await (delay(meanDelay - delayBeforeKill) && success(ignoreSSFailures));

					delayBeforeKill = deterministicRandom()->random01() * meanDelay;
					TraceEvent("WorkerKillAfterMeanDelay").detail("DelayBeforeKill", delayBeforeKill);
				}
			}
			if (!iterate || now() - startTime > maxRunDuration) {
				break;
			} else {
				co_await delay(suspendDuration);
				suspendDuration *= backoff;
			}
		}

		if (killSelf)
			throw please_reboot();
	}
};

WorkloadFactory<MachineAttritionWorkload> MachineAttritionWorkloadFactory;
FailureInjectorFactory<MachineAttritionWorkload> MachineAttritionFailureWorkloadFactory;
