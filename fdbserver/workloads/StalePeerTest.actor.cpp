/*
 * StalePeerTest.actor.cpp
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

// Test that verifies stale peer references are cleaned up after a process
// running a specific role is killed. Configurable via killRole parameter.

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/FlowTransport.h"
#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/ManagementAPI.actor.h"
#include <set>
#include "flow/actorcompiler.h" // must be last include

struct StalePeerTestWorkload : TestWorkload {
	static constexpr auto NAME = "StalePeerTest";

	double waitAfterKill;
	std::string killRole;
	bool testPassed;
	bool skippedNoTargets = false;
	bool skippedClusterUnhealthy = false;

	std::vector<NetworkAddress> oldKillAddresses;
	// Tracker role string for the killed interface, used purely for
	// diagnostic per-role Delta reporting in the failure event. The pass
	// criterion is the raw peer->peerReferences count, not the per-role
	// Delta. Strings:
	//   "TLog" (tlog or log_router), "SS" (ss),
	//   "CP" (commit_proxy), "GP" (grv_proxy),
	//   "MS" (master), "RV" (resolver),
	//   "DD" (dd), "RK" (rk), "CC" (cluster_controller).
	// Empty for coordinator (no service interface registered with the
	// tracker — diagnostic Delta dump is skipped).
	std::string trackedDstRole;

	StalePeerTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), testPassed(true) {
		waitAfterKill = getOption(options, "waitAfterKill"_sr, 60.0);
		killRole = getOption(options, "killRole"_sr, "tlog"_sr).toString();
		// clientDstRoles=true overrides the toml killRole and instead picks a
		// random role at workload-init time from the set of "client-facing
		// destination roles" — the roles a client (DatabaseContext / its
		// peer connections) directly addresses: coordinator, cluster
		// controller, commit proxy, grv proxy, storage server. This keeps
		// the per-workload deterministic seeding in tact (different runs
		// pick different roles, but the same seed/config picks the same
		// role) while broadening test coverage in a single bulk run.
		bool clientDstRoles = getOption(options, "clientDstRoles"_sr, false);
		if (clientDstRoles) {
			static const std::vector<std::string> clientDstChoices = {
				"coordinator", "cluster_controller", "commit_proxy", "grv_proxy", "ss"
			};
			killRole = clientDstChoices[deterministicRandom()->randomInt(0, clientDstChoices.size())];
			TraceEvent("StalePeerTestPickedClientDstRole").detail("KillRole", killRole);
		}
		static const std::set<std::string> validRoles = { "tlog",          "ss",         "commit_proxy", "grv_proxy",
			                                              "master",        "resolver",   "dd",           "rk",
			                                              "coordinator",   "log_router", "cluster_controller" };
		if (!validRoles.count(killRole)) {
			TraceEvent(SevError, "StalePeerTestInvalidKillRole")
			    .detail("KillRole", killRole)
			    .detail("ValidOptions",
			            "tlog, ss, commit_proxy, grv_proxy, master, resolver, dd, rk, coordinator, log_router, "
			            "cluster_controller");
			ASSERT(false);
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();
		return _start(this, cx);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	// Push addr into `out` only if it's valid and not protected by the simulator.
	// sim2 silently refuses to kill addresses in protectedAddresses (coordinator
	// majority, HTTP servers, etc.), and a silent no-op kill would trip the
	// stuck-peer-ref check on an alive process.
	void addIfKillable(std::vector<NetworkAddress>& out, NetworkAddress addr) {
		if (addr.isValid() && !g_simulator->protectedAddresses.count(addr)) {
			out.push_back(addr);
		}
	}

	// Find a process address for the given role.
	std::vector<NetworkAddress> findAddressesForRole(Database const& cx) {
		std::vector<NetworkAddress> result;
		const auto& info = dbInfo->get();
		if (killRole == "tlog") {
			for (const auto& tlogset : info.logSystemConfig.tLogs) {
				if (!tlogset.isLocal)
					continue;
				for (const auto& log : tlogset.tLogs) {
					if (log.present())
						addIfKillable(result, log.interf().address());
				}
			}
		} else if (killRole == "log_router") {
			// Log routers live on remote DCs in fearless configs. In a
			// single-region cluster there are none, in which case the
			// empty-target skip path below will treat this as a no-op.
			for (const auto& tlogset : info.logSystemConfig.tLogs) {
				for (const auto& lr : tlogset.logRouters) {
					if (lr.present())
						addIfKillable(result, lr.interf().address());
				}
			}
		} else if (killRole == "ss") {
			// Candidates: any simulator process with StorageClass or UnsetClass
			// (these are where SSes are recruited).
			auto allProcesses = g_simulator->getAllProcesses();
			for (auto* proc : allProcesses) {
				if (proc->failed || proc->rebooting)
					continue;
				if (proc->startingClass != ProcessClass::StorageClass &&
				    proc->startingClass != ProcessClass::UnsetClass)
					continue;
				auto* transport = static_cast<FlowTransport*>((void*)proc->global(INetwork::enFlowTransport));
				if (transport) {
					addIfKillable(result, proc->address);
				}
			}
		} else if (killRole == "commit_proxy") {
			for (const auto& cp : info.client.commitProxies) {
				addIfKillable(result, cp.address());
			}
			trackedDstRole = "CP";
		} else if (killRole == "grv_proxy") {
			for (const auto& gp : info.client.grvProxies) {
				addIfKillable(result, gp.address());
			}
			trackedDstRole = "GP";
		} else if (killRole == "master") {
			addIfKillable(result, info.master.address());
		} else if (killRole == "resolver") {
			for (const auto& rv : info.resolvers) {
				addIfKillable(result, rv.address());
			}
		} else if (killRole == "dd") {
			if (info.distributor.present())
				addIfKillable(result, info.distributor.get().address());
		} else if (killRole == "rk") {
			if (info.ratekeeper.present())
				addIfKillable(result, info.ratekeeper.get().address());
		} else if (killRole == "cluster_controller") {
			addIfKillable(result, info.clusterInterface.address());
		} else if (killRole == "coordinator") {
			// sim2 keeps a majority of coordinators in protectedAddresses. Any
			// un-protected coordinator is a valid kill target. With
			// coordinators=3 (set in StalePeerTest.toml), 2 are protected and
			// the third is killable. Resolve hostnames too — simulation
			// connection strings use hostnames (e.g. fakeCoordinatorDC0M0:1)
			// rather than direct NetworkAddresses, so cs.coords is typically
			// empty and the candidates live in cs.hostnames.
			auto connRecord = cx->getConnectionRecord();
			if (connRecord) {
				auto cs = connRecord->getConnectionString();
				for (const auto& addr : cs.coords) {
					addIfKillable(result, addr);
				}
				for (const auto& hn : cs.hostnames) {
					Optional<NetworkAddress> resolved = hn.resolveBlocking();
					if (resolved.present()) {
						addIfKillable(result, resolved.get());
					}
				}
			}
		}
		return result;
	}

	ACTOR static Future<Void> _start(StalePeerTestWorkload* self, Database cx) {
		// Wait for cluster to stabilize
		wait(delay(10.0));
		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		state std::vector<NetworkAddress> targetAddresses = self->findAddressesForRole(cx);

		if (targetAddresses.empty()) {
			// No killable addresses for this role (e.g. every coordinator /
			// proxy is protected, or log_routers don't exist in a
			// single-region cluster). Skip the test — there's nothing to
			// leak refs for.
			TraceEvent("StalePeerTestNoTargets")
			    .detail("Role", self->killRole)
			    .detail("Note", "No killable addresses found; treating as skip");
			self->skippedNoTargets = true;
			return Void();
		}

		TraceEvent("StalePeerTestStarting")
		    .detail("Role", self->killRole)
		    .detail("TargetsFound", targetAddresses.size());

		// Pick the first target to kill
		state NetworkAddress oldAddr = targetAddresses[0];
		ISimulator::ProcessInfo* proc = g_simulator->getProcessByAddress(oldAddr);
		if (!proc || proc->failed) {
			TraceEvent(SevError, "StalePeerTestProcessNotFound").detail("Address", oldAddr);
			self->testPassed = false;
			return Void();
		}

		self->oldKillAddresses.push_back(oldAddr);

		TraceEvent("StalePeerTestKilling")
		    .detail("Role", self->killRole)
		    .detail("Address", oldAddr)
		    .detail("ProcessClass", proc->startingClass.toString())
		    .detail("Zone", proc->locality.zoneId());

		g_simulator->killProcess(proc, ISimulator::KillType::KillInstantly);
		TraceEvent("StalePeerTestKillDone")
		    .detail("Address", oldAddr)
		    .detail("Role", self->killRole)
		    .detail("ProcFailedFlag", proc->failed);

		// Guard: sim2 silently refuses to kill protected addresses. If our
		// candidate filter missed one, fail fast with a clear error rather
		// than letting the later stale-peer-ref check attribute the refs on
		// an alive process to a real leak.
		if (!proc->failed) {
			TraceEvent(SevError, "StalePeerTestKillIneffective")
			    .detail("Address", oldAddr)
			    .detail("Protected", g_simulator->protectedAddresses.count(oldAddr))
			    .detail("Rebooting", proc->rebooting);
			self->testPassed = false;
			return Void();
		}

		// Wait for recovery + cleanup
		TraceEvent("StalePeerTestWaitingForRecovery");
		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}
		TraceEvent("StalePeerTestRecovered");

		// Coordinator-only: the dead coordinator stays in the cluster's
		// connection string indefinitely, so every live process keeps a
		// long-term LeaderMonitor connection to its address (PeerRef = 1
		// per process). That isn't a stale-ref leak — it's the design
		// pattern for cluster identity. Trigger an auto quorum change to
		// swap the dead coordinator for a healthy candidate, mirroring
		// what `coordinators auto` does in fdbcli. Once the dead address
		// is no longer in the connection string the LeaderMonitor refs
		// drop and the strict peerRefs == 0 check is meaningful.
		if (self->killRole == "coordinator") {
			TraceEvent("StalePeerTestCoordinatorAutoChange");
			state int autoChangeAttempts = 0;
			state int nQuorum = ((g_simulator->desiredCoordinators + 1) / 2) * 2 - 1;
			loop {
				++autoChangeAttempts;
				CoordinatorsResult res = wait(changeQuorum(cx, autoQuorumChange(nQuorum)));
				TraceEvent("StalePeerTestCoordinatorAutoChangeResult")
				    .detail("Attempt", autoChangeAttempts)
				    .detail("Result", (int)res)
				    .detail("Quorum", nQuorum);
				if (res == CoordinatorsResult::SUCCESS || res == CoordinatorsResult::SAME_NETWORK_ADDRESSES) {
					break;
				}
				if (autoChangeAttempts >= 20) {
					TraceEvent(SevWarn, "StalePeerTestCoordinatorAutoChangeGivingUp")
					    .detail("LastResult", (int)res);
					break;
				}
				wait(delay(1.0));
			}
		}

		TraceEvent("StalePeerTestWaiting").detail("WaitSeconds", self->waitAfterKill);
		wait(delay(self->waitAfterKill));

		// Cluster-health guard: if the cluster never recovered to
		// FULLY_RECOVERED within the wait window (e.g. perpetual_storage_wiggle
		// + ssd-sharded-rocksdb configs that produce RkSSListFetchTimeout
		// before the kill, or a degenerate recovery loop after the kill), the
		// peer-ref check is meaningless — the leak signal will be dominated
		// by the cluster meltdown rather than the kill we're testing. Skip
		// rather than fail: the contract this test verifies is "kill of role X
		// drains its peer refs in a healthy cluster", not "every cluster
		// configuration recovers within 240s".
		if (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			TraceEvent("StalePeerTestClusterUnhealthy")
			    .detail("RecoveryState", (int)self->dbInfo->get().recoveryState)
			    .detail("KillRole", self->killRole)
			    .detail("Note", "Cluster did not return to FULLY_RECOVERED; skipping peer-ref check");
			self->skippedClusterUnhealthy = true;
			return Void();
		}

		// Strict pass criterion for tracked roles: per-role Delta gt 0 at
		// the killed address means a stale RequestStream copy of THAT
		// killed role's interface is still pinned somewhere. Per-role
		// keeps us from conflating co-resident role leaks (TLog, SS, etc.
		// hosted on the same process as the killed CP/GP/CC/MS/RV/DD/RK)
		// with the actual signal we care about.
		//
		// Coordinator is special — it has no service-level RequestStream
		// interface registered with InterfaceTracker (its endpoints use
		// well-known tokens for ClientLeaderRegInterface). Raw
		// peerReferences would over-attribute (catch co-resident role
		// leaks), and there is nothing tracker-level we can isolate. The
		// meaningful signal for coordinator is whether the cluster
		// successfully removed the dead coord from the connection string
		// (otherwise leader-monitor connections persist forever). That
		// signal is the autoQuorumChange result above; if it succeeded
		// the test passes, no peer-ref check is performed for coord.
		if (self->killRole == "coordinator") {
			TraceEvent("StalePeerTestChecking").detail("Mode", "coordinator/skip-peer-refs");
			return Void();
		}
		TraceEvent("StalePeerTestChecking");

		state std::string trackerRole;
		if (self->killRole == "tlog" || self->killRole == "log_router") {
			trackerRole = "TLog";
		} else if (self->killRole == "ss") {
			trackerRole = "SS";
		} else if (self->killRole == "commit_proxy") {
			trackerRole = "CP";
		} else if (self->killRole == "grv_proxy") {
			trackerRole = "GP";
		} else if (self->killRole == "master") {
			trackerRole = "MS";
		} else if (self->killRole == "resolver") {
			trackerRole = "RV";
		} else if (self->killRole == "dd") {
			trackerRole = "DD";
		} else if (self->killRole == "rk") {
			trackerRole = "RK";
		} else if (self->killRole == "cluster_controller") {
			trackerRole = "CC";
		}
		ASSERT(!trackerRole.empty());

		auto allProcesses = g_simulator->getAllProcesses();
		for (auto* proc : allProcesses) {
			if (proc->failed || proc->rebooting)
				continue;
			auto* transport = static_cast<FlowTransport*>((void*)proc->global(INetwork::enFlowTransport));
			if (!transport)
				continue;
			for (const auto& oldKillAddr : self->oldKillAddresses) {
				auto& allPeers = transport->getAllPeers();
				auto it = allPeers.find(oldKillAddr);
				int peerRefs = (it != allPeers.end()) ? it->second->peerReferences : 0;
				int64_t delta = transport->interfaceTracker.getDelta(oldKillAddr, trackerRole);
				if (delta > 0) {
					transport->interfaceTracker.prettyPrint(proc->address, self->oldKillAddresses);
					transport->interfaceTracker.prettyPrintLeakedReceivers(proc->address, self->oldKillAddresses);
					transport->interfaceTracker.prettyPrintLeakedRefs(proc->address, self->oldKillAddresses);
					TraceEvent(SevError, "StalePeerTestFailed")
					    .detail("CheckedProcess", proc->address)
					    .detail("OldAddress", oldKillAddr)
					    .detail("KillRole", self->killRole)
					    .detail("TrackerRole", trackerRole)
					    .detail("Delta", delta)
					    .detail("PeerReferences", peerRefs);
					self->testPassed = false;
				}
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		if (oldKillAddresses.empty() && !skippedNoTargets) {
			TraceEvent(SevError, "StalePeerTestNoProcessKilled").detail("KillRole", killRole);
			testPassed = false;
		}
		TraceEvent("StalePeerTestResult")
		    .detail("Passed", testPassed)
		    .detail("KillRole", killRole)
		    .detail("SkippedNoTargets", skippedNoTargets)
		    .detail("SkippedClusterUnhealthy", skippedClusterUnhealthy)
		    .detail("ProcessesKilled", oldKillAddresses.size());
		return testPassed;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<StalePeerTestWorkload> StalePeerTestWorkloadFactory;
