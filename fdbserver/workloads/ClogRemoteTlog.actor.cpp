#include <cstdint>
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IPAddress.h"
#include "flow/IRandom.h"
#include "flow/Optional.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct ClogRemoteTLog : TestWorkload {
	static constexpr auto NAME = "ClogRemoteTLog";

	bool enabled{ false };
	double testDuration{ 0.0 };
	double lagMeasurementFrequencySec{ 0 };
	double clogInitDelaySec{ 0 };
	double clogDurationSec{ 0 };
	double lagThresholdSec{ 0 };

	ClogRemoteTLog(const WorkloadContext& wctx) : TestWorkload(wctx) {
		enabled =
		    (clientId == 0); // only run this workload for a single client, and that too the first client (by its id)
		testDuration = getOption(options, "testDuration"_sr, 1000);
		lagMeasurementFrequencySec = getOption(options, "lagMeasurementFrequencySec"_sr, 5);
		clogInitDelaySec = getOption(options, "clogInitDelaySec"_sr, 5);
		clogDurationSec = getOption(options, "clogDurationSec"_sr, 5);
		lagThresholdSec = getOption(options, "lagThresholdSec"_sr, 5);
	}

	Future<Void> setup(const Database& db) override { return Void(); }

	Future<Void> start(const Database& db) override {
		if (g_network->isSimulated() && enabled) {
			return timeout(reportErrors(workload(this, db), "ClogRemoteTLogError"), testDuration, Void());
		}
		return Void();
	}

	Future<bool> check(const Database& db) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Void> measureMaxSSLagSec(ClogRemoteTLog* self, Database db) {
		StatusObject status = wait(StatusClient::statusFetcher(db));
		StatusObjectReader reader(status);
		StatusObjectReader cluster;
		StatusObjectReader processMap;
		if (!reader.get("cluster", cluster)) {
			TraceEvent("NoCluster");
			return Void();
		}
		if (!cluster.get("processes", processMap)) {
			TraceEvent("NoProcesses");
			return Void();
		}
		double maxSSLagSec{ -1 };
		for (auto p : processMap.obj()) {
			StatusObjectReader process(p.second);
			if (process.has("roles")) {
				StatusArray roles = p.second.get_obj()["roles"].get_array();
				for (StatusObjectReader role : roles) {
					ASSERT(role.has("role"));
					if (role.has("data_lag")) {
						ASSERT(role["role"].get_str() == "storage");
						auto dataLag = role["data_lag"].get_obj();
						ASSERT(dataLag.contains("seconds"));
						ASSERT(dataLag.contains("versions"));
						TraceEvent("SSDataLag")
						    .detail("Process", p.first)
						    .detail("Role", role["role"].get_str())
						    .detail("SecondLag", dataLag["seconds"].get_value<double>())
						    .detail("VersionLag", dataLag["versions"].get_int64());
						maxSSLagSec = std::max(maxSSLagSec, dataLag["seconds"].get_value<double>());
					}
				}
			}
		}
		TraceEvent("MaxSSDataLag").detail("SecondLag", maxSSLagSec).detail("SecondThreshold", self->lagThresholdSec);
		// TODO (praza): Uncomment this assert when gray failure detection is improved to automatically fix this issue
		// ASSERT(maxSSLagSec < self->lagThresholdSec);
		return Void();
	}

	ACTOR static Future<std::vector<IPAddress>> getRemoteSSIPs(Database db) {
		state std::vector<IPAddress> ret;
		Transaction tr(db);
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
		    wait(NativeAPI::getServerListAndProcessClasses(&tr));
		for (auto& [ssi, p] : results) {
			if (ssi.locality.dcId().present() && ssi.locality.dcId().get() == g_simulator->remoteDcId) {
				ret.push_back(ssi.address().ip);
			}
		}
		return ret;
	}

	static std::vector<IPAddress> getRemoteTLogIPs(ClogRemoteTLog* self) {
		std::vector<IPAddress> remoteTLogIPs;
		for (const auto& tLogSet : self->dbInfo->get().logSystemConfig.tLogs) {
			if (tLogSet.isLocal) {
				continue;
			}
			for (const auto& tLog : tLogSet.tLogs) {
				remoteTLogIPs.push_back(tLog.interf().address().ip);
			}
		}
		return remoteTLogIPs;
	}

	ACTOR static Future<Void> clogRemoteTLog(ClogRemoteTLog* self, Database db) {
		wait(delay(self->clogInitDelaySec));

		// First, get all remote TLog IPs
		state std::vector<IPAddress> remoteTLogIPs = getRemoteTLogIPs(self);
		ASSERT(!remoteTLogIPs.empty());

		// Then, get all remote SS IPs
		std::vector<IPAddress> remoteSSIPs = wait(getRemoteSSIPs(db));
		ASSERT(!remoteSSIPs.empty());

		// Then, clog a random remote TLog -> SS network link
		state IPAddress remoteTLogIP = remoteTLogIPs[deterministicRandom()->randomInt(0, remoteTLogIPs.size())];
		state IPAddress remoteSSIP = remoteSSIPs[deterministicRandom()->randomInt(0, remoteSSIPs.size())];
		TraceEvent("ClogRemoteTLog").detail("SrcIP", remoteTLogIP).detail("DstIP", remoteSSIP);
		g_simulator->clogPair(remoteTLogIP, remoteSSIP, self->testDuration);

		// After some time, unclog
		wait(delay(self->clogDurationSec));
		g_simulator->unclogPair(remoteTLogIP, remoteSSIP);
		TraceEvent("UnclogRemoteTLog").detail("SrcIP", remoteTLogIP).detail("DstIP", remoteSSIP);

		// We only clog once, so this actor never finishes
		wait(Never());
		return Void();
	}

	ACTOR Future<Void> workload(ClogRemoteTLog* self, Database db) {
		state Future<Void> clog = self->clogRemoteTLog(self, db);
		loop choose {
			when(wait(delay(self->lagMeasurementFrequencySec))) {
				wait(measureMaxSSLagSec(self, db));
			}
			when(wait(clog)) {}
		}
	}
};

WorkloadFactory<ClogRemoteTLog> ClogRemoteTlogWorkloadFactory;