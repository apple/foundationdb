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
#include "flow/Error.h"
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
	double lagMeasurementFrequency{ 0 };
	double clogInitDelay{ 0 };
	double clogDuration{ 0 };
	double lagThreshold{ 0 };
	bool doStatePathCheck{ true };

	enum TestState { TEST_INIT, SS_LAG_NORMAL, SS_LAG_HIGH };
	// Currently, the only valid state path is: TEST_INIT, SS_LAG_NORMAL -> SS_LAG_HIGH -> SS_LAG_NORMAL
	const std::vector<std::vector<TestState>> expectedStatePaths{
		{ TEST_INIT, SS_LAG_NORMAL, SS_LAG_HIGH, SS_LAG_NORMAL }
	};
	std::vector<TestState>
	    actualStatePath; // to be populated when the test runs, and finally checked at the end in check()

	ClogRemoteTLog(const WorkloadContext& wctx) : TestWorkload(wctx) {
		enabled =
		    (clientId == 0); // only run this workload for a single client, and that too the first client (by its id)
		testDuration = getOption(options, "testDuration"_sr, 120);
		lagMeasurementFrequency = getOption(options, "lagMeasurementFrequency"_sr, 5);
		clogInitDelay = getOption(options, "clogInitDelay"_sr, 15);
		clogDuration = getOption(options, "clogDuration"_sr, 60);
		lagThreshold = getOption(options, "lagThreshold"_sr, 5);
	}

	Future<Void> setup(const Database& db) override { return Void(); }

	Future<Void> start(const Database& db) override {
		if (!g_network->isSimulated() || !enabled) {
			return Void();
		}
		return timeout(reportErrors(workload(this, db), "ClogRemoteTLogError"), testDuration, Void());
	}

	Future<bool> check(const Database& db) override {
		if (!g_network->isSimulated() || !enabled) {
			return true;
		}
		// First, emit trace event for potential debugging if test fails
		auto stateToStr = [](const TestState testState) {
			switch (testState) {
			case (TEST_INIT): {
				return "TEST_INIT";
			}
			case (SS_LAG_NORMAL): {
				return "SS_LAG_NORMAL";
			}
			case (SS_LAG_HIGH): {
				return "SS_LAG_HIGH";
			}
			default: {
				ASSERT(false);
			}
			};
		};
		auto print = [&stateToStr](const std::vector<TestState>& path) {
			std::string ret;
			for (size_t i = 0; i < path.size(); ++i) {
				const auto pathState = path[i];
				ret += stateToStr(pathState) + (i < path.size() - 1 ? std::string{ " -> " } : std::string{ "" });
			}
			return ret;
		};
		TraceEvent("ClogRemoteTLogCheck")
		    .detail("ActualStatePath", print(actualStatePath))
		    .detail("DoStatePathCheck", doStatePathCheck ? "True" : "False");

		// Then, do the actual check
		if (!doStatePathCheck) {
			return true;
		}
		auto match = [](const std::vector<TestState>& path1, const std::vector<TestState>& path2) -> bool {
			if (path1.size() != path2.size()) {
				return false;
			}
			for (size_t i = 0; i < path1.size(); ++i) {
				if (path1[i] != path2[i]) {
					return false;
				}
			}
			return true;
		};
		for (const auto& expectedPath : expectedStatePaths) {
			if (match(actualStatePath, expectedPath)) {
				return true;
			}
		}
		return false;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<Optional<double>> measureMaxSSLag(ClogRemoteTLog* self, Database db) {
		StatusObject status = wait(StatusClient::statusFetcher(db));
		StatusObjectReader reader(status);
		StatusObjectReader cluster;
		StatusObjectReader processMap;
		if (!reader.get("cluster", cluster)) {
			TraceEvent("NoCluster");
			return Optional<double>();
		}
		if (!cluster.get("processes", processMap)) {
			TraceEvent("NoProcesses");
			return Optional<double>();
		}
		double maxSSLag{ -1 };
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
						maxSSLag = std::max(maxSSLag, dataLag["seconds"].get_value<double>());
					}
				}
			}
		}
		TraceEvent("MaxSSDataLag")
		    .detail("SecondLag", maxSSLag == -1 ? "none" : std::to_string(maxSSLag))
		    .detail("SecondThreshold", self->lagThreshold);
		if (maxSSLag == -1) {
			return Optional<double>();
		} else {
			return maxSSLag;
		}
	}

	ACTOR static Future<std::vector<IPAddress>> getRemoteSSIPs(Database db) {
		state std::vector<IPAddress> ret;
		state Transaction tr(db);
		loop {
			try {
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
			} catch (Error& e) {
				TraceEvent("GetRemoteSSIPsError").error(e);
				wait(tr.onError(e));
				tr = Transaction(db);
			}
		}
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
		wait(delay(self->clogInitDelay));

		// Ensure db is ready
		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		// Then, get all remote TLog IPs
		state std::vector<IPAddress> remoteTLogIPs = getRemoteTLogIPs(self);
		ASSERT(!remoteTLogIPs.empty());

		// Then, get all remote SS IPs
		std::vector<IPAddress> remoteSSIPs = wait(getRemoteSSIPs(db));
		ASSERT(!remoteSSIPs.empty());

		// Then, attempt to find a remote tlog that is not on the same machine as a remote SS
		Optional<IPAddress> remoteTLogIP_temp;
		for (const auto& ip : remoteTLogIPs) {
			if (std::find(remoteSSIPs.begin(), remoteSSIPs.end(), ip) == remoteSSIPs.end()) {
				remoteTLogIP_temp = ip;
			}
		}

		state IPAddress remoteTLogIP;
		if (remoteTLogIP_temp.present()) {
			remoteTLogIP = remoteTLogIP_temp.get();
		} else {
			remoteTLogIP = remoteTLogIPs[deterministicRandom()->randomInt(0, remoteTLogIPs.size())];
			self->doStatePathCheck = false;
		}

		// Then, find all processes that the remote tlog will have degraded connection with
		IPAddress cc = self->dbInfo->get().clusterInterface.address().ip;
		state std::vector<IPAddress> processes;
		for (const auto& process : g_simulator->getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (process->startingClass != ProcessClass::TesterClass && ip != cc) {
				processes.push_back(ip);
			}
		}
		ASSERT(!processes.empty());

		// Finally, start the clogging between the remote tlog and the processes calculated above
		for (const auto& ip : processes) {
			if (remoteTLogIP == ip) {
				continue;
			}
			TraceEvent("ClogRemoteTLog").detail("SrcIP", remoteTLogIP).detail("DstIP", ip);
			g_simulator->clogPair(remoteTLogIP, ip, self->testDuration);
			g_simulator->clogPair(ip, remoteTLogIP, self->testDuration);
		}

		wait(Never());
		return Void();
	}

	ACTOR Future<Void> workload(ClogRemoteTLog* self, Database db) {
		state Future<Void> clog = self->clogRemoteTLog(self, db);
		state TestState testState = TestState::TEST_INIT;
		self->actualStatePath.push_back(testState);
		loop choose {
			when(wait(delay(self->lagMeasurementFrequency))) {
				Optional<double> ssLag = wait(measureMaxSSLag(self, db));
				if (!ssLag.present()) {
					continue;
				}
				TestState localState =
				    ssLag.get() < self->lagThreshold ? TestState::SS_LAG_NORMAL : TestState::SS_LAG_HIGH;
				// Anytime a state transition happens, append to the state path
				if (localState != testState) {
					self->actualStatePath.push_back(localState);
					testState = localState;
				}
			}
			when(wait(clog)) {}
		}
	}
};

WorkloadFactory<ClogRemoteTLog> ClogRemoteTLogWorkloadFactory;