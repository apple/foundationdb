#include <algorithm>
#include <cstdint>
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/IPAddress.h"
#include "flow/IRandom.h"
#include "flow/NetworkAddress.h"
#include "flow/Optional.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

struct ClogRemoteTLog : TestWorkload {
	static constexpr auto NAME = "ClogRemoteTLog";

	bool enabled{ false };
	bool doCheck{ false };
	double testDuration{ 0.0 };
	double lagMeasurementFrequency{ 0 };
	double clogInitDelay{ 0 };
	double lagThreshold{ 0 };

	enum TestState { TEST_INIT, SS_LAG_NORMAL, SS_LAG_HIGH, CLOGGED_REMOTE_TLOG_EXCLUDED };
	struct StatePath {
		std::vector<TestState> path;
		bool prefixMatch{ true };
	};
	const std::vector<StatePath> expectedStatePaths{
		{ .path = { TEST_INIT, SS_LAG_NORMAL, SS_LAG_HIGH, SS_LAG_NORMAL } },
		// For some topology and process placements, it's possible that the lag does not recover. However, we still
		// allow the test to pass as long as the bad/clogged remote tlog was excluded by gray failure.
		{ .path = { TEST_INIT, SS_LAG_NORMAL, SS_LAG_HIGH, CLOGGED_REMOTE_TLOG_EXCLUDED } },
		{ .path = { TEST_INIT, SS_LAG_NORMAL, CLOGGED_REMOTE_TLOG_EXCLUDED } },
		{ .path = { TEST_INIT, SS_LAG_HIGH, CLOGGED_REMOTE_TLOG_EXCLUDED } },
		{ .path = { TEST_INIT, SS_LAG_HIGH, SS_LAG_NORMAL } }
	};
	std::vector<TestState>
	    actualStatePath; // to be populated when the test runs, and finally checked at the end in check()

	Optional<NetworkAddress>
	    cloggedRemoteTLog; // set after clogging is done, we use this state to ensure that it's
	                       // eventually not present in dbInfo (which implies it was excluded by gray failure)

	ClogRemoteTLog(const WorkloadContext& wctx) : TestWorkload(wctx) {
		enabled =
		    (clientId == 0); // only run this workload for a single client, and that too the first client (by its id)
		testDuration = getOption(options, "testDuration"_sr, 120);
		lagMeasurementFrequency = getOption(options, "lagMeasurementFrequency"_sr, 5);
		clogInitDelay = getOption(options, "clogInitDelay"_sr, 10);
		lagThreshold = getOption(options, "lagThreshold"_sr, 20);
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
		auto stateToStr = [](const TestState testState) -> std::string {
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
			case (CLOGGED_REMOTE_TLOG_EXCLUDED): {
				return "CLOGGED_REMOTE_TLOG_EXCLUDED";
			}
			default: {
				ASSERT(false);
				return "";
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
		TraceEvent("ClogRemoteTLogCheck").detail("ActualStatePath", print(actualStatePath)).detail("DoCheck", doCheck);
		if (!doCheck || isGeneralBuggifyEnabled()) {
			return true;
		}

		// Then, do the actual check
		auto match =
		    [](const std::vector<TestState>& actualPath,
		       const std::vector<TestState>& expectedPath,
		       const bool
		           allowPrefix /* when true, relaxes match as long as a prefix of actualPath matches expectedPath */)
		    -> bool {
			if (!allowPrefix && actualPath.size() != expectedPath.size()) {
				return false;
			} else if (allowPrefix && actualPath.size() < expectedPath.size()) {
				return false;
			}
			for (size_t i = 0; i < std::min(actualPath.size(), expectedPath.size()); ++i) {
				if (actualPath[i] != expectedPath[i]) {
					return false;
				}
			}
			return true;
		};
		for (const auto& expectedPath : expectedStatePaths) {
			if (match(actualStatePath, expectedPath.path, expectedPath.prefixMatch)) {
				return true;
			}
		}
		TraceEvent(SevError, "ClogRemoteTLogCheckFailed");
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

	// Returns true if and only if there's a general error in fetching status json
	// An example failure is network issue from client to CC (server)
	static bool statusError(StatusObjectReader reader) {
		static const auto errors{ []() {
			std::unordered_set<std::string> errors;
			std::for_each(messageTypeToName.begin(), messageTypeToName.end(), [&errors](const auto& kvPair) {
				errors.insert(kvPair.second);
			});
			return errors;
		}() };

		StatusObjectReader client;
		if (!reader.get("client", client)) {
			TraceEvent("NoClient");
			return true;
		}

		StatusObjectReader cluster;
		if (!reader.get("cluster", cluster)) {
			TraceEvent("NoCluster");
			return true;
		}

		ASSERT(client.has("messages"));
		StatusArray messages = client["messages"].get_array();
		for (StatusObjectReader message : messages) {
			if (message.has("name") && errors.contains(message["name"].get_str())) {
				TraceEvent("StatusError").detail("Name", message["name"].get_str());
				return true;
			}
		}

		return false;
	}

	ACTOR static Future<bool> grayFailureStatusCheck(Database db, NetworkAddress cloggedRemoteTLog) {
		StatusObject status = wait(StatusClient::statusFetcher(db));
		StatusObjectReader reader(status);

		if (statusError(reader)) {
			// If there is some error to get the status (e.g. network issue), we let gray failure status check pass
			// since that's not what we are testing for here.
			return true;
		}

		StatusObjectReader cluster;
		ASSERT(reader.get("cluster", cluster));
		StatusObjectReader grayFailure;
		if (!cluster.get("gray_failure", grayFailure)) {
			TraceEvent("NoGrayFailure");
			return false;
		}
		ASSERT(grayFailure.has("excluded_servers"));
		StatusArray excludedProcesses = grayFailure["excluded_servers"].get_array();
		for (StatusObjectReader process : excludedProcesses) {
			ASSERT(process.has("address"));
			ASSERT(process.has("time"));
			TraceEvent("GrayFailureStatus")
			    .detail("Address", process["address"].get_str())
			    .detail("Ts", process["time"].get_real());
		}
		return true;
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
					if (ssi.locality.dcId().present() && g_simulator->remoteDcId.present() &&
					    ssi.locality.dcId().get() == g_simulator->remoteDcId.get()) {
						ret.push_back(ssi.address().ip);
					}
				}
				return ret;
			} catch (Error& e) {
				if (e.code() != error_code_actor_cancelled) {
					TraceEvent("GetRemoteSSIPsError").error(e);
				}
				wait(tr.onError(e));
			}
		}
	}

	static std::vector<NetworkAddress> getRemoteTLogs(ClogRemoteTLog* self) {
		std::vector<NetworkAddress> remoteTLogIPs;
		for (const auto& tLogSet : self->dbInfo->get().logSystemConfig.tLogs) {
			if (tLogSet.isLocal) {
				continue;
			}
			for (const auto& tLog : tLogSet.tLogs) {
				remoteTLogIPs.push_back(tLog.interf().address());
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
		state std::vector<NetworkAddress> remoteTLogs = getRemoteTLogs(self);
		ASSERT(!remoteTLogs.empty());

		// Then, get all remote SS IPs
		std::vector<IPAddress> remoteSSIPs = wait(getRemoteSSIPs(db));
		ASSERT(!remoteSSIPs.empty());

		// Then, attempt to find a remote tlog that is not on the same machine as a remote SS
		Optional<NetworkAddress> isolatedRemoteTLog;
		for (const auto& addr : remoteTLogs) {
			if (std::find(remoteSSIPs.begin(), remoteSSIPs.end(), addr.ip) == remoteSSIPs.end()) {
				isolatedRemoteTLog = addr;
			}
		}

		// If we can find such a machine that is just running a remote tlog, then we will do extra checking at the end
		// (in check() method). If we can't find such a machine, we pick a random machhine and still run the test to
		// ensure no crashes or correctness issues are observed.
		self->cloggedRemoteTLog = isolatedRemoteTLog.present()
		                              ? isolatedRemoteTLog.get()
		                              : self->cloggedRemoteTLog =
		                                    remoteTLogs[deterministicRandom()->randomInt(0, remoteTLogs.size())];
		ASSERT(self->cloggedRemoteTLog.present());

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
		int numClogged{ 0 };
		for (const auto& ip : processes) {
			if (self->cloggedRemoteTLog.get().ip == ip) {
				continue;
			}
			TraceEvent("ClogRemoteTLog").detail("SrcIP", self->cloggedRemoteTLog->ip).detail("DstIP", ip);
			g_simulator->clogPair(ip, self->cloggedRemoteTLog.get().ip, self->testDuration);
			numClogged++;
		}

		if (isolatedRemoteTLog.present() && numClogged > 1) {
			self->doCheck = true;
		}

		wait(Never());
		return Void();
	}

	// Returns true if and only if the provided remote tlog `addr` is not in dbInfo
	static bool remoteTLogNotInDbInfo(const NetworkAddress& addr, const ServerDBInfo& dbInfo) {
		for (const auto& tLogSet : dbInfo.logSystemConfig.tLogs) {
			if (tLogSet.isLocal) {
				continue;
			}
			for (const auto& tLog : tLogSet.tLogs) {
				if (tLog.present() && tLog.interf().addresses().contains(addr)) {
					return false;
				}
			}
		}
		return true;
	}

	ACTOR Future<Void> workload(ClogRemoteTLog* self, Database db) {
		state Future<Void> clog = self->clogRemoteTLog(self, db);
		state TestState testState = TestState::TEST_INIT;
		self->actualStatePath.push_back(testState);
		state bool statusCheckPassed = false;
		loop {
			wait(delay(self->lagMeasurementFrequency));
			Optional<double> ssLag = wait(measureMaxSSLag(self, db));
			if (!ssLag.present()) {
				continue;
			}
			// See if ss lag state changed
			state TestState localState =
			    ssLag.get() < self->lagThreshold ? TestState::SS_LAG_NORMAL : TestState::SS_LAG_HIGH;
			state bool stateTransition = localState != testState;
			// If ss lag state did not change, see if clogged remote tlog got excluded
			if (!stateTransition) {
				const bool acceptingCommits = self->dbInfo->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS;
				TraceEvent("ClogRemoteTLogMoreInfo")
				    .detail("CloggedRemoteTLogPresent", self->cloggedRemoteTLog.present())
				    .detail("Addr",
				            self->cloggedRemoteTLog.present() ? self->cloggedRemoteTLog.get().toString() : "DidNotFind")
				    .detail("NotInDbInfo",
				            self->cloggedRemoteTLog.present()
				                ? remoteTLogNotInDbInfo(self->cloggedRemoteTLog.get(), self->dbInfo->get())
				                : false)
				    .detail("AcceptingCommits", acceptingCommits)
				    .detail("RecoveryState", self->dbInfo->get().recoveryState);
				if (acceptingCommits && self->cloggedRemoteTLog.present() &&
				    remoteTLogNotInDbInfo(self->cloggedRemoteTLog.get(), self->dbInfo->get())) {
					localState = TestState::CLOGGED_REMOTE_TLOG_EXCLUDED;
					if (!statusCheckPassed) {
						wait(store(statusCheckPassed, grayFailureStatusCheck(db, self->cloggedRemoteTLog.get())));
						ASSERT(statusCheckPassed);
					}
					stateTransition = localState != testState;
				}
			}
			// If there was a state transition, append new state to state path
			if (stateTransition) {
				self->actualStatePath.push_back(localState);
				testState = localState;
			}
		}
	}
};

WorkloadFactory<ClogRemoteTLog> ClogRemoteTLogWorkloadFactory;
