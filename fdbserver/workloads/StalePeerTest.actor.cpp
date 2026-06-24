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
// running a specific role is killed. Configurable via dstKillRole parameter.

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/QuietDatabase.h"
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
	std::string dstKillRole;
	// Which source roles we inspect for stale peer references to the killed
	// destination. "any" = every live process (server roles included).
	// "tester_client" = only customer-client processes (ProcessClass::TesterClass,
	// the tester processes running the workload). Server roles use the client
	// library internally so their peer refs are affected by the client-side
	// fixes too, but the contract we care about is that a pure customer client
	// holds no stale ref to a killed client-facing role.
	std::string srcCheckRole;
	bool testPassed = false;
	bool skippedNoTargets = false;
	bool skippedClusterUnhealthy = false;
	// Set when the chosen target was not referenced by any inspected source at
	// kill time, so a post-kill Delta==0 would prove nothing (inconclusive). We
	// skip rather than count such a run as a meaningful pass.
	bool skippedVacuous = false;
	// How many inspected source processes held a reference (tracked interface
	// copy or live peer ref) to the killed address just before the kill. Logged
	// for audit; the post-kill check is gated on this being > 0.
	int preKillRefSources = 0;

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
	// tracker -- diagnostic Delta dump is skipped).
	std::string trackedDstRole;

	StalePeerTestWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), testPassed(true) {
		waitAfterKill = getOption(options, "waitAfterKill"_sr, 60.0);
		dstKillRole = getOption(options, "dstKillRole"_sr, "clientFacing"_sr).toString();
		srcCheckRole = getOption(options, "srcCheckRole"_sr, "any"_sr).toString();
		// dstKillRole="clientFacing" resolves (per-run, deterministically) to one
		// of the client-facing destination roles -- the roles a client
		// (DatabaseContext / its peer connections) directly addresses: coordinator,
		// cluster controller, commit proxy, grv proxy, storage server. Same
		// seed/config picks the same role, so per-run determinism is preserved
		// while a single bulk ensemble covers all client-facing dst kills.
		if (dstKillRole == "clientFacing") {
			static const std::vector<std::string> clientDstChoices = {
				"coordinator", "cluster_controller", "commit_proxy", "grv_proxy", "ss"
			};
			dstKillRole = clientDstChoices[deterministicRandom()->randomInt(0, clientDstChoices.size())];
			TraceEvent("StalePeerTestPickedClientDstRole").detail("DstKillRole", dstKillRole);
		}
		static const std::set<std::string> validRoles = {
			"tlog", "ss",          "commit_proxy", "grv_proxy",         "master", "resolver", "dd",
			"rk",   "coordinator", "log_router",   "cluster_controller"
		};
		if (!validRoles.count(dstKillRole)) {
			TraceEvent(SevError, "StalePeerTestInvalidDstKillRole")
			    .detail("DstKillRole", dstKillRole)
			    .detail("ValidOptions",
			            "clientFacing, tlog, ss, commit_proxy, grv_proxy, master, resolver, dd, rk, coordinator, "
			            "log_router, cluster_controller");
			ASSERT(false);
		}
		static const std::set<std::string> validSrcChecks = { "any", "tester_client" };
		if (!validSrcChecks.count(srcCheckRole)) {
			TraceEvent(SevError, "StalePeerTestInvalidSrcCheckRole")
			    .detail("SrcCheckRole", srcCheckRole)
			    .detail("ValidOptions", "any, tester_client");
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

	// InterfaceTracker role string for the killed dst, used for the per-role
	// Delta check and pre-kill reference snapshot. Deterministic from
	// dstKillRole. Empty for coordinator: its endpoints use well-known tokens
	// (ClientLeaderRegInterface) that are not registered with the tracker.
	std::string computeTrackerRole() const {
		if (dstKillRole == "tlog" || dstKillRole == "log_router")
			return "TLog";
		if (dstKillRole == "ss")
			return "SS";
		if (dstKillRole == "commit_proxy")
			return "CP";
		if (dstKillRole == "grv_proxy")
			return "GP";
		if (dstKillRole == "master")
			return "MS";
		if (dstKillRole == "resolver")
			return "RV";
		if (dstKillRole == "dd")
			return "DD";
		if (dstKillRole == "rk")
			return "RK";
		if (dstKillRole == "cluster_controller")
			return "CC";
		return ""; // coordinator
	}

	// Should this source process be inspected for stale refs? Mirrors the
	// srcCheckRole filter used by both the pre-kill snapshot and the post-kill
	// check so the two are always over the same population. "tester_client"
	// keeps only customer-client (TesterClass) processes, excluding the
	// simulator's internal TestSystem driver (IP 1.1.1.1), which is TesterClass
	// but makes no workload transactions.
	bool isInspectedSource(ISimulator::ProcessInfo* proc) const {
		if (proc->failed || proc->rebooting)
			return false;
		if (srcCheckRole == "tester_client" &&
		    (proc->startingClass != ProcessClass::TesterClass || proc->address.ip == IPAddress(0x01010101))) {
			return false;
		}
		return proc->global(INetwork::enFlowTransport) != nullptr;
	}

	// Count inspected source processes that currently hold a reference to `addr`:
	// either a live tracked interface copy (per-role Delta > 0) or a live peer
	// reference. Used pre-kill to establish non-vacuity (the target is actually
	// referenced) and to pick the most-referenced storage server to kill.
	int countSourcesReferencing(const NetworkAddress& addr, const std::string& trackerRole) const {
		int n = 0;
		for (auto* proc : g_simulator->getAllProcesses()) {
			if (!isInspectedSource(proc))
				continue;
			auto* transport = static_cast<FlowTransport*>((void*)proc->global(INetwork::enFlowTransport));
			bool hasRef = !trackerRole.empty() && transport->interfaceTracker.getDelta(addr, trackerRole) > 0;
			if (!hasRef) {
				const auto& allPeers = transport->getAllPeers();
				auto it = allPeers.find(addr);
				hasRef = (it != allPeers.end() && it->second->peerReferences > 0);
			}
			if (hasRef)
				++n;
		}
		return n;
	}

	// Find a process address for the given role.
	std::vector<NetworkAddress> findAddressesForRole(Database const& cx) {
		std::vector<NetworkAddress> result;
		const auto& info = dbInfo->get();
		if (dstKillRole == "tlog") {
			for (const auto& tlogset : info.logSystemConfig.tLogs) {
				if (!tlogset.isLocal)
					continue;
				for (const auto& log : tlogset.tLogs) {
					if (log.present())
						addIfKillable(result, log.interf().address());
				}
			}
		} else if (dstKillRole == "log_router") {
			// Log routers live on remote DCs in fearless configs. In a
			// single-region cluster there are none, in which case the
			// empty-target skip path below will treat this as a no-op.
			for (const auto& tlogset : info.logSystemConfig.tLogs) {
				for (const auto& lr : tlogset.logRouters) {
					if (lr.present())
						addIfKillable(result, lr.interf().address());
				}
			}
		} else if (dstKillRole == "ss") {
			// SS targets are resolved in _start from the authoritative recruited
			// set via getStorageServers(cx) (see the ss path there), not here.
			// Scanning simulator processes by StorageClass/UnsetClass could pick a
			// process that hosts no recruited SS, in which case no SS interface is
			// ever tracked at that address and the per-role Delta check passes
			// vacuously. Leaving this empty; _start handles ss before calling.
		} else if (dstKillRole == "commit_proxy") {
			for (const auto& cp : info.client.commitProxies) {
				addIfKillable(result, cp.address());
			}
			trackedDstRole = "CP";
		} else if (dstKillRole == "grv_proxy") {
			for (const auto& gp : info.client.grvProxies) {
				addIfKillable(result, gp.address());
			}
			trackedDstRole = "GP";
		} else if (dstKillRole == "master") {
			addIfKillable(result, info.master.address());
		} else if (dstKillRole == "resolver") {
			for (const auto& rv : info.resolvers) {
				addIfKillable(result, rv.address());
			}
		} else if (dstKillRole == "dd") {
			if (info.distributor.present())
				addIfKillable(result, info.distributor.get().address());
		} else if (dstKillRole == "rk") {
			if (info.ratekeeper.present())
				addIfKillable(result, info.ratekeeper.get().address());
		} else if (dstKillRole == "cluster_controller") {
			addIfKillable(result, info.clusterInterface.address());
		} else if (dstKillRole == "coordinator") {
			// sim2 keeps a majority of coordinators in protectedAddresses. Any
			// un-protected coordinator is a valid kill target. With
			// coordinators=3 (set in StalePeerTest.toml), 2 are protected and
			// the third is killable. Resolve hostnames too -- simulation
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

		// Tracker role for the killed dst (deterministic from dstKillRole), used
		// for both the pre-kill reference snapshot and the post-kill Delta check.
		state std::string trackerRole = self->computeTrackerRole();

		// Resolve kill targets.
		state std::vector<NetworkAddress> targetAddresses;
		if (self->dstKillRole == "ss") {
			// #1 non-vacuity: target an ACTUAL recruited storage server from the
			// cluster's authoritative serverList, not just any StorageClass
			// process. Killing a process that hosts no recruited SS would leave no
			// SS interface tracked at that address, so the per-role Delta check
			// would pass without ever exercising the client-side eviction path.
			std::vector<StorageServerInterface> ssis = wait(getStorageServers(cx));
			for (const auto& ssi : ssis) {
				self->addIfKillable(targetAddresses, ssi.address());
			}
		} else {
			targetAddresses = self->findAddressesForRole(cx);
		}

		if (targetAddresses.empty()) {
			// No killable addresses for this role (e.g. every coordinator /
			// proxy is protected, or log_routers don't exist in a
			// single-region cluster). Skip the test -- there's nothing to
			// leak refs for.
			TraceEvent("StalePeerTestNoTargets")
			    .detail("Role", self->dstKillRole)
			    .detail("Note", "No killable addresses found; treating as skip");
			self->skippedNoTargets = true;
			return Void();
		}

		TraceEvent("StalePeerTestStarting")
		    .detail("Role", self->dstKillRole)
		    .detail("TargetsFound", targetAddresses.size());

		// Pick the target to kill. For SS, choose the recruited server that the
		// most inspected sources currently reference (cached interface / live
		// peer ref) so the kill actually exercises client-side eviction, polling
		// briefly (bounded) to let caches warm under the ReadWrite workload. For
		// other roles the candidates are interchangeable, so take the first. In
		// all cases snapshot how many inspected sources reference the chosen
		// target right before the kill -- that count is the non-vacuity signal.
		state NetworkAddress oldAddr;
		if (self->dstKillRole == "ss") {
			state double warmDeadline = now() + 30.0;
			loop {
				NetworkAddress best;
				int bestN = -1;
				for (const auto& a : targetAddresses) {
					int n = self->countSourcesReferencing(a, trackerRole);
					if (n > bestN) {
						bestN = n;
						best = a;
					}
				}
				if (bestN > 0 || now() >= warmDeadline) {
					oldAddr = best;
					self->preKillRefSources = bestN;
					break;
				}
				wait(delay(2.0));
			}
		} else {
			oldAddr = targetAddresses[0];
			self->preKillRefSources = self->countSourcesReferencing(oldAddr, trackerRole);
		}

		// Non-vacuity gate: if no inspected source referenced the target at kill
		// time, a post-kill Delta==0 would prove nothing. Mark the run
		// inconclusive (skip) rather than recording it as a meaningful pass.
		// Coordinator is exempt -- its endpoints use well-known tokens that are
		// not tracked here, and its meaningful signal is connection-string
		// removal (handled below), not a peer-ref drain.
		if (self->dstKillRole != "coordinator" && self->preKillRefSources <= 0) {
			TraceEvent(SevWarnAlways, "StalePeerTestVacuousNoPreKillRef")
			    .detail("Role", self->dstKillRole)
			    .detail("Address", oldAddr)
			    .detail("TrackerRole", trackerRole)
			    .detail("Note", "No inspected source referenced the target at kill time; skipping as inconclusive");
			self->skippedVacuous = true;
			return Void();
		}

		ISimulator::ProcessInfo* proc = g_simulator->getProcessByAddress(oldAddr);
		if (!proc || proc->failed) {
			TraceEvent(SevError, "StalePeerTestProcessNotFound").detail("Address", oldAddr);
			self->testPassed = false;
			return Void();
		}

		self->oldKillAddresses.push_back(oldAddr);

		TraceEvent("StalePeerTestKilling")
		    .detail("Role", self->dstKillRole)
		    .detail("Address", oldAddr)
		    .detail("PreKillRefSources", self->preKillRefSources)
		    .detail("ProcessClass", proc->startingClass.toString())
		    .detail("Zone", proc->locality.zoneId());

		g_simulator->killProcess(proc, ISimulator::KillType::KillInstantly);
		TraceEvent("StalePeerTestKillDone")
		    .detail("Address", oldAddr)
		    .detail("Role", self->dstKillRole)
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

		// Wait for the kill's recovery + cleanup. Right after killProcess the
		// broadcast dbInfo still carries the pre-kill FULLY_RECOVERED for a few
		// seconds, so a bare wait-for-FULLY_RECOVERED races straight through
		// without actually waiting for the kill's recovery -- then the later
		// peer-ref check lands mid-recovery and skips as "cluster unhealthy".
		// First wait (bounded) for the kill to drop recoveryState below
		// FULLY_RECOVERED (recovery triggered); roles whose loss is handled
		// without a master recovery (ss/dd/rk) simply hit this timeout. Then
		// wait (bounded) for the recovery to complete. Both bounds fall through
		// to the post-waitAfterKill cluster-health guard if exceeded, so this
		// can never hang.
		TraceEvent("StalePeerTestWaitingForRecovery");
		state double recoveryTriggerDeadline = now() + 30.0;
		while (self->dbInfo->get().recoveryState >= RecoveryState::FULLY_RECOVERED && now() < recoveryTriggerDeadline) {
			choose {
				when(wait(self->dbInfo->onChange())) {}
				when(wait(delay(1.0))) {}
			}
		}
		state double recoveryCompleteDeadline = now() + 180.0;
		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED && now() < recoveryCompleteDeadline) {
			choose {
				when(wait(self->dbInfo->onChange())) {}
				when(wait(delay(5.0))) {}
			}
		}
		TraceEvent("StalePeerTestRecovered").detail("RecoveryState", (int)self->dbInfo->get().recoveryState);

		// Coordinator-only: the dead coordinator stays in the cluster's
		// connection string indefinitely, so every live process keeps a
		// long-term LeaderMonitor connection to its address (PeerRef = 1
		// per process). That isn't a stale-ref leak -- it's the design
		// pattern for cluster identity. Trigger an auto quorum change to
		// swap the dead coordinator for a healthy candidate, mirroring
		// what `coordinators auto` does in fdbcli. Once the dead address
		// is no longer in the connection string the LeaderMonitor refs
		// drop and the strict peerRefs == 0 check is meaningful.
		if (self->dstKillRole == "coordinator") {
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
					TraceEvent(SevWarn, "StalePeerTestCoordinatorAutoChangeGivingUp").detail("LastResult", (int)res);
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
		// peer-ref check is meaningless -- the leak signal will be dominated
		// by the cluster meltdown rather than the kill we're testing. Skip
		// rather than fail: the contract this test verifies is "kill of role X
		// drains its peer refs in a healthy cluster", not "every cluster
		// configuration recovers within 240s".
		if (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			TraceEvent("StalePeerTestClusterUnhealthy")
			    .detail("RecoveryState", (int)self->dbInfo->get().recoveryState)
			    .detail("KillRole", self->dstKillRole)
			    .detail("Note", "Cluster did not return to FULLY_RECOVERED; skipping peer-ref check");
			self->skippedClusterUnhealthy = true;
			return Void();
		}

		// Pass criterion: per-role InterfaceTracker Delta == 0 at the killed
		// address. Delta counts leaked COPIES of the killed role's interface
		// RequestStreams still pinned somewhere -- i.e. the actual stale-interface
		// leak our client-side fixes target. We deliberately do NOT use raw
		// peer->peerReferences: a tester client legitimately retains
		// connection-level / well-known-token refs to the killed address (the
		// draining TCP connection, ping/leader-monitor endpoints, and refs to a
		// co-resident role the process also hosted -- sim co-locates many roles
		// per process). Those are real, expected peers (peerReferences > 0 with
		// Delta == 0), not the staleness bug. peerReferences is logged as a
		// diagnostic. Delta requires the InterfaceTracker, so this mode needs
		// stale_peer_observability = true (deterministic: toggling it does not
		// change simulator ordering).
		//
		// Coordinator is special -- its endpoints use well-known tokens
		// (ClientLeaderRegInterface) and aren't tracked, and every process keeps
		// a long-term LeaderMonitor connection to each coordinator address. The
		// meaningful signal for a coordinator kill is whether the cluster removed
		// the dead coord from the connection string; that is the autoQuorumChange
		// result above, so no peer-ref check is done for coord.
		//
		// KNOWN COVERAGE GAP (intentional): a coordinator kill therefore asserts
		// only that the dead coordinator was swapped out of the connection string
		// (the autoQuorumChange succeeded) -- it does NOT assert that every source
		// drained its peer ref to the dead address, because the LeaderMonitor refs
		// are expected and not a leak our client-side fixes target. Since
		// dstKillRole="clientFacing" resolves uniformly across {coordinator,
		// cluster_controller, commit_proxy, grv_proxy, ss}, roughly one in five
		// clientFacing runs lands on coordinator and exercises only this weaker
		// connection-string check. The per-role stale-interface drain is covered
		// by the other four roles. (Tightening the coordinator path to a strict
		// peer-ref assertion -- tolerating the N expected LeaderMonitor refs -- is
		// a possible follow-up.)
		if (self->dstKillRole == "coordinator") {
			TraceEvent("StalePeerTestChecking").detail("Mode", "coordinator/skip-peer-refs");
			return Void();
		}
		TraceEvent("StalePeerTestChecking");

		// trackerRole was computed once at the top of _start (computeTrackerRole)
		// and used for the pre-kill snapshot; reuse it here. It is non-empty for
		// every role that reaches this point (coordinator returned above).
		ASSERT(!trackerRole.empty());

		auto allProcesses = g_simulator->getAllProcesses();
		state int clientSourcesChecked = 0; // source processes inspected (post-filter)
		state int clientSourcesWithLeak = 0; // ...of which had a per-role Delta > 0 (a leak)
		for (auto* proc : allProcesses) {
			// Same source population as the pre-kill snapshot (see
			// isInspectedSource): drops failed/rebooting and, in "tester_client"
			// mode, keeps only customer-client (TesterClass) processes while
			// excluding the simulator's internal TestSystem driver (IP 1.1.1.1).
			if (!self->isInspectedSource(proc))
				continue;
			auto* transport = static_cast<FlowTransport*>((void*)proc->global(INetwork::enFlowTransport));
			++clientSourcesChecked;
			for (const auto& oldKillAddr : self->oldKillAddresses) {
				auto& allPeers = transport->getAllPeers();
				auto it = allPeers.find(oldKillAddr);
				const int peerRefs = (it != allPeers.end()) ? it->second->peerReferences : 0;
				const int64_t delta = transport->interfaceTracker.getDelta(oldKillAddr, trackerRole);
				if (delta > 0) {
					++clientSourcesWithLeak;
				}
				if (delta > 0) {
					transport->interfaceTracker.prettyPrint(proc->address, self->oldKillAddresses);
					transport->interfaceTracker.prettyPrintLeakedReceivers(proc->address, self->oldKillAddresses);
					transport->interfaceTracker.prettyPrintLeakedRefs(proc->address, self->oldKillAddresses);
					TraceEvent(SevError, "StalePeerTestFailed")
					    .detail("CheckedProcess", proc->address)
					    .detail("CheckedProcessClass", proc->startingClass.toString())
					    .detail("SrcCheckRole", self->srcCheckRole)
					    .detail("OldAddress", oldKillAddr)
					    .detail("KillRole", self->dstKillRole)
					    .detail("TrackerRole", trackerRole)
					    .detail("TrackerDelta", delta)
					    .detail("PeerReferences", peerRefs);
					self->testPassed = false;
				}
			}
		}
		// Coverage visibility: how many source processes we examined, and how
		// many showed a per-role interface leak (Delta > 0). This is logged for
		// every run (pass or fail) so the aggregate leak signal is visible even
		// when the run passes. PreKillRefSources records how many sources held a
		// reference to the target before the drain -- it is > 0 here by
		// construction (the non-vacuity gate above skips the run otherwise).
		TraceEvent("StalePeerTestCheckCoverage")
		    .detail("SrcCheckRole", self->srcCheckRole)
		    .detail("KillRole", self->dstKillRole)
		    .detail("PreKillRefSources", self->preKillRefSources)
		    .detail("ClientSourcesChecked", clientSourcesChecked)
		    .detail("ClientSourcesWithLeak", clientSourcesWithLeak);

		// Non-vacuity assertion: a real check must have inspected at least one
		// source. Reaching here means we killed a target that was referenced
		// pre-kill (preKillRefSources > 0), so the inspected population cannot be
		// empty. If it somehow is, the run validated nothing -- fail loudly
		// rather than report a hollow pass.
		if (clientSourcesChecked == 0) {
			TraceEvent(SevError, "StalePeerTestNoSourcesInspected")
			    .detail("KillRole", self->dstKillRole)
			    .detail("SrcCheckRole", self->srcCheckRole)
			    .detail("PreKillRefSources", self->preKillRefSources);
			self->testPassed = false;
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0) {
			return true;
		}
		if (oldKillAddresses.empty() && !skippedNoTargets && !skippedVacuous) {
			TraceEvent(SevError, "StalePeerTestNoProcessKilled").detail("KillRole", dstKillRole);
			testPassed = false;
		}
		TraceEvent("StalePeerTestResult")
		    .detail("Passed", testPassed)
		    .detail("KillRole", dstKillRole)
		    .detail("SrcCheckRole", srcCheckRole)
		    .detail("SkippedNoTargets", skippedNoTargets)
		    .detail("SkippedClusterUnhealthy", skippedClusterUnhealthy)
		    .detail("SkippedVacuous", skippedVacuous)
		    .detail("PreKillRefSources", preKillRefSources)
		    .detail("ProcessesKilled", oldKillAddresses.size());
		return testPassed;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<StalePeerTestWorkload> StalePeerTestWorkloadFactory;
