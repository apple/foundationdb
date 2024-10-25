/*
 * FailoverWithSSLag.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/StatusClient.h"
#include "flow/actorcompiler.h" // This must be the last include.

// This actor tests failover with remote tlogs being in sync with primary but with remote storage servers lagging
// behind the primary. Failover shouldn't complete until the remote storage servers also get in sync with the primary.
struct FailoverWithSSLagWorkload : TestWorkload {
	static constexpr auto NAME = "FailoverWithSSLagWorkload";
	bool enabled;
	double testDuration;
	bool testSuccess;
	std::vector<IPAddress> tlogs; // remote tlogs
	std::vector<IPAddress> storages; // remote storages

	FailoverWithSSLagWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled =
		    !clientId && g_network->isSimulated(); // only do this on the "first" client, and only when in simulation
		testDuration = getOption(options, "testDuration"_sr, 400.0);
		testSuccess = true;
		g_simulator->usableRegions = 2;
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return timeout(reportErrors(clogClient(this, cx), "FailoverWithSSLagError"), testDuration, Void());
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return testSuccess; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Clog or unclog (based on argument "clog") connections between remote tlogs ("tlogs") and
	// remote storages ("storages").
	void clogUnclogRemoteStorages(bool clog, double seconds = 0) {
		for (const auto& tlog : tlogs) {
			for (const auto& storage : storages) {
				if (clog) {
					g_simulator->clogPair(tlog, storage, seconds);
					g_simulator->clogPair(storage, tlog, seconds);
				} else {
					g_simulator->unclogPair(tlog, storage);
					g_simulator->unclogPair(storage, tlog);
				}
			}
		}
	}

	// Find remote tlogs and remote storage servers and clog connections between them.
	bool findAndClogRemoteStorages(double seconds) {
		ASSERT(dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);

		// Find all remote tlogs (including remote satellite tlogs).
		for (const auto& tlogset : dbInfo->get().logSystemConfig.tLogs) {
			if (tlogset.isLocal) {
				continue;
			}
			for (const auto& tlog : tlogset.tLogs) {
				tlogs.push_back(tlog.interf().address().ip);
			}
		}

		if (tlogs.empty()) {
			return false;
		}

		// Find all remote storage servers.
		for (const auto& process : g_simulator->getAllProcesses()) {
			if (process->locality.dcId().present() && process->locality.dcId() == g_simulator->remoteDcId &&
			    g_simulator->hasRole(process->address, "StorageServer")) {
				storages.push_back(process->address.ip);
			}
		}

		if (storages.empty()) {
			return false;
		}

		// Clog connections between remote tlogs and storage servers.
		clogUnclogRemoteStorages(true /* clog */, seconds);

		return true;
	}

	// Fetches details (versions and seconds) of the specified type of lag (tlog/storage server/data center lag) from
	// the given status json document.
	bool fetchLagFromStatusObject(std::string path, StatusObjectReader& statusObj, Version& versions, double& seconds) {
		StatusObjectReader lagObject;
		if (!statusObj.get(path, lagObject)) {
			return false;
		}

		if (!lagObject.get("versions", versions)) {
			return false;
		}

		if (!lagObject.get("seconds", seconds)) {
			return false;
		}

		return true;
	}

	ACTOR static Future<Optional<Version>> fetchStorageServerLag(FailoverWithSSLagWorkload* self, Database cx) {
		state double startTime = now();
		StatusObject result = wait(StatusClient::statusFetcher(cx));
		double duration = now() - startTime;

		StatusObjectReader statusObj(result);
		StatusObjectReader statusObjCluster;
		if (!statusObj.get("cluster", statusObjCluster)) {
			TraceEvent("SSLagNoCluster");
			return Optional<Version>();
		}

		// Fetch the lag between primary and remote tlogs.
		Version tlogLagInVersions = 0;
		double tlogLagInSeconds = 0;
		if (!self->fetchLagFromStatusObject("logserver_lag", statusObjCluster, tlogLagInVersions, tlogLagInSeconds)) {
			TraceEvent("NoLogServerLagData");
			return Optional<Version>();
		}

		// Fetch the lag between primary and remote storage servers.
		Version ssLagInVersions = 0;
		double ssLagInSeconds = 0;
		if (!self->fetchLagFromStatusObject("storageserver_lag", statusObjCluster, ssLagInVersions, ssLagInSeconds)) {
			TraceEvent("NoStorageServerLagData");
			return Optional<Version>();
		}

		// Fetch the lag between primary and remote data centers.
		Version dcLagInVersions = 0;
		double dcLagInSeconds = 0;
		if (!self->fetchLagFromStatusObject("datacenter_lag", statusObjCluster, dcLagInVersions, dcLagInSeconds)) {
			TraceEvent("NoDataCenterLagData");
			return Optional<Version>();
		}

		TraceEvent("LagInfo")
		    .detail("LogServerLagInVersions", tlogLagInVersions)
		    .detail("LogServerLagInSeconds", tlogLagInSeconds)
		    .detail("StorageServerLagInVersions", ssLagInVersions)
		    .detail("StorageServerLagInSeconds", ssLagInSeconds)
		    .detail("DataCenterLagInVersions", dcLagInVersions)
		    .detail("DataCenterLagInSeconds", dcLagInSeconds)
		    .detail("TimeToFetchStatus", duration);

		return ssLagInVersions;
	}

	ACTOR static Future<Void> waitForRemoteDataCenterToLag(FailoverWithSSLagWorkload* self, Database cx) {
		state Future<Optional<Version>> ssLag = Never();
		loop choose {
			when(wait(delay(5.0))) {
				// Fetch SS lag every 5s.
				ssLag = self->fetchStorageServerLag(self, cx);
			}
			when(Optional<Version> versionLag = wait(ssLag)) {
				if (versionLag.present() && versionLag.get() >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
					TraceEvent("SSLag").detail("Versions", versionLag.get());
					return Void();
				}
				ssLag = Never();
			}
		}
	}

	ACTOR static Future<Void> failover(FailoverWithSSLagWorkload* self, Database cx) {
		TraceEvent("FailoverBegin").log();

		wait(success(ManagementAPI::changeConfig(cx.getReference(), g_simulator->disablePrimary, true)));
		TraceEvent("Failover_WaitFor_PrimaryDatacenterKey").log();

		// when failover, primaryDC should change to 1
		wait(waitForPrimaryDC(cx, "1"_sr));
		TraceEvent("FailoverComplete").log();
		return Void();
	}

	ACTOR static Future<Void> doFailover(FailoverWithSSLagWorkload* self, Database cx) {
		state bool connectionsClogged = true;
		state bool failoverCompleted = false;
		loop choose {
			// NOTE: We don't have a way of verifying that failover is blocked because of the
			// data center/storage server lag. So verify that failover is blocked for 100 seconds
			// (which is way longer than the time needed to complete failover) and then unclog
			// connections and let failover make progress.
			when(wait(delay(100.0))) {
				if (connectionsClogged) {
					if (failoverCompleted) {
						// Failover completed even while the remote storages are clogged, which
						// shouldn't happen. Mark the test as failed.
						self->testSuccess = false;
						return Void();
					}
					self->clogUnclogRemoteStorages(false /* clog */);
					connectionsClogged = false;
				}
			}
			when(wait(self->failover(self, cx))) {
				if (connectionsClogged) {
					// Failover completed even while the remote storages are clogged, which
					// shouldn't happen. Mark the test as failed.
					self->testSuccess = false;
					return Void();
				}
				failoverCompleted = true;

				// Verify that the storage server lag has gone below the threshold.
				state Future<Optional<Version>> ssLag = self->fetchStorageServerLag(self, cx);
				Optional<Version> versionLag = wait(ssLag);
				if (versionLag.present() && versionLag.get() >= SERVER_KNOBS->MAX_VERSION_DIFFERENCE) {
					TraceEvent("SSLag").detail("Versions", versionLag.get());
					self->testSuccess = false;
				}
				return Void();
			}
		}
	}

	ACTOR Future<Void> clogClient(FailoverWithSSLagWorkload* self, Database cx) {
		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		// Clog connections between remote tlogs and storage servers.
		if (!self->findAndClogRemoteStorages(self->testDuration)) {
			// Couldn't find remote tlogs/storage servers. Probably configuration will
			// need to be adjusted.
			self->testSuccess = false;
			return Void();
		}

		// Wait until the data center/storage server lag goes above the threshold.
		wait(self->waitForRemoteDataCenterToLag(self, cx));

		// Initiate failover and verify that it doesn't complete until the data center/
		// storage server lag gets below the threshold.
		wait(self->doFailover(self, cx));

		return Void();
	}
};

WorkloadFactory<FailoverWithSSLagWorkload> FailoverWithSSLagWorkloadFactory;
