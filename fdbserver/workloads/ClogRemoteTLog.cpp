#include <algorithm>
#include <cstdint>
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Status.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/IPAddress.h"
#include "flow/IRandom.h"
#include "flow/NetworkAddress.h"
#include "flow/Optional.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

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

	explicit ClogRemoteTLog(const WorkloadContext& wctx) : TestWorkload(wctx) {
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

	Future<Optional<double>> measureMaxSSLag(Database db) {
		StatusObject status = co_await StatusClient::statusFetcher(db);
		StatusObjectReader reader(status);
		StatusObjectReader cluster;
		StatusObjectReader processMap;
		if (!reader.get("cluster", cluster)) {
			TraceEvent("NoCluster");
			co_return Optional<double>();
		}
		if (!cluster.get("processes", processMap)) {
			TraceEvent("NoProcesses");
			co_return Optional<double>();
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
		    .detail("SecondThreshold", lagThreshold);
		if (maxSSLag == -1) {
			co_return Optional<double>();
		} else {
			co_return maxSSLag;
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

	// Returns true if the status response is incomplete, i.e. the cluster controller could not collect a full
	// status. This happens when status collection races past its deadline or throws, which is common during the
	// recoveries this workload induces (the exclusion of the clogged tlog is itself performed by a recovery). The
	// gray_failure section is built last in clusterGetStatusImpl, so a partial status omits it. The incompleteness
	// is reported as a cluster-level "status_incomplete" message; note statusError() above only inspects
	// client-level messages and therefore does not catch this case.
	static bool statusIncomplete(StatusObjectReader reader) {
		StatusObjectReader cluster;
		if (!reader.get("cluster", cluster) || !cluster.has("messages")) {
			return false;
		}
		StatusArray messages = cluster["messages"].get_array();
		for (StatusObjectReader message : messages) {
			if (message.has("name") && message["name"].get_str() == "status_incomplete") {
				TraceEvent("GrayFailureStatusIncomplete");
				return true;
			}
		}
		return false;
	}

	enum class GrayFailureCheckResult {
		Confirmed, // a fully collected status contained the gray_failure section
		Incomplete, // status was unavailable or incomplete; inconclusive, the caller should retry
		Missing // a fully collected status was returned but lacked gray_failure (a real regression)
	};

	static Future<GrayFailureCheckResult> grayFailureStatusCheck(Database db, NetworkAddress cloggedRemoteTLog) {
		StatusObject status = co_await StatusClient::statusFetcher(db);
		StatusObjectReader reader(status);

		// Check for the gray_failure section FIRST. Its presence is authoritative: gray_failure is only ever
		// produced by the cluster controller, so if it's in the response we have what we're testing for -- even
		// if the status is otherwise marked incomplete. This ordering matters under buggify: status collection
		// frequently records a "status_incomplete" message because some unrelated subsection failed
		// (status_incomplete_reasons is fed by errorOr'd subsections that don't abort the build), even when
		// gray_failure was collected just fine. Treating that as a reason to discard a valid gray_failure section
		// would make this check vacuous -- it would never confirm and the test would pass without verifying
		// anything.
		StatusObjectReader cluster;
		StatusObjectReader grayFailure;
		if (reader.get("cluster", cluster) && cluster.get("gray_failure", grayFailure)) {
			ASSERT(grayFailure.has("excluded_servers"));
			StatusArray excludedProcesses = grayFailure["excluded_servers"].get_array();
			for (StatusObjectReader process : excludedProcesses) {
				ASSERT(process.has("address"));
				ASSERT(process.has("time"));
				TraceEvent("GrayFailureStatus")
				    .detail("Address", process["address"].get_str())
				    .detail("Ts", process["time"].get_real());
			}
			co_return GrayFailureCheckResult::Confirmed;
		}

		// The gray_failure section is absent. If the response was incomplete (a client/CC error, or the CC's
		// status collection raced past its deadline / threw before building the section, which is built last),
		// the result is inconclusive -- retry on a later iteration. Otherwise a fully collected status genuinely
		// lacks gray_failure, which in simulation (CC_GRAY_FAILURE_STATUS_JSON always on) is a real regression.
		if (statusError(reader) || statusIncomplete(reader)) {
			co_return GrayFailureCheckResult::Incomplete;
		}
		TraceEvent("NoGrayFailure");
		co_return GrayFailureCheckResult::Missing;
	}

	static Future<std::vector<IPAddress>> getRemoteSSIPs(Database db) {
		std::vector<IPAddress> ret;
		Transaction tr(db);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
				    co_await NativeAPI::getServerListAndProcessClasses(&tr);
				for (auto& [ssi, p] : results) {
					if (ssi.locality.dcId().present() && fdbSimulationPolicyState().remoteDcId.present() &&
					    ssi.locality.dcId().get() == fdbSimulationPolicyState().remoteDcId.get()) {
						ret.push_back(ssi.address().ip);
					}
				}
				co_return ret;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() != error_code_actor_cancelled) {
				TraceEvent("GetRemoteSSIPsError").error(err);
			}
			co_await tr.onError(err);
		}
	}

	std::vector<NetworkAddress> getRemoteTLogs() {
		std::vector<NetworkAddress> remoteTLogIPs;
		for (const auto& tLogSet : dbInfo->get().logSystemConfig.tLogs) {
			if (tLogSet.isLocal) {
				continue;
			}
			for (const auto& tLog : tLogSet.tLogs) {
				remoteTLogIPs.push_back(tLog.interf().address());
			}
		}
		return remoteTLogIPs;
	}

	Future<Void> clogRemoteTLog(Database db) {
		co_await delay(clogInitDelay);

		// Ensure db is ready
		while (dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			co_await dbInfo->onChange();
		}

		// Then, get all remote TLog IPs
		std::vector<NetworkAddress> remoteTLogs = getRemoteTLogs();
		ASSERT(!remoteTLogs.empty());

		// Then, get all remote SS IPs
		std::vector<IPAddress> remoteSSIPs = co_await getRemoteSSIPs(db);
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
		cloggedRemoteTLog = isolatedRemoteTLog.present()
		                        ? isolatedRemoteTLog.get()
		                        : cloggedRemoteTLog =
		                              remoteTLogs[deterministicRandom()->randomInt(0, remoteTLogs.size())];
		ASSERT(cloggedRemoteTLog.present());

		// Then, find all processes that the remote tlog will have degraded connection with
		IPAddress cc = dbInfo->get().clusterInterface.address().ip;
		std::vector<IPAddress> processes;
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
			if (cloggedRemoteTLog.get().ip == ip) {
				continue;
			}
			double clogDuration = testDuration * (0.5 + 0.4 * deterministicRandom()->random01());
			// clogDuration must be less than testDuration to ensure that the clogging ends before the test ends
			g_simulator->clogPair(ip, cloggedRemoteTLog.get().ip, clogDuration);
			TraceEvent("ClogRemoteTLog")
			    .detail("SrcIP", cloggedRemoteTLog->ip)
			    .detail("DstIP", ip)
			    .detail("Duration", clogDuration);
			numClogged++;
		}

		if (isolatedRemoteTLog.present() && numClogged > 1) {
			doCheck = true;
		}

		co_await Future<Void>(Never());
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

	Future<Void> workload(ClogRemoteTLog* self, Database db) {
		Future<Void> clog = clogRemoteTLog(db);
		TestState testState = TestState::TEST_INIT;
		self->actualStatePath.push_back(testState);
		bool statusCheckPassed = false;
		while (true) {
			co_await delay(self->lagMeasurementFrequency);
			Optional<double> ssLag = co_await measureMaxSSLag(db);
			if (!ssLag.present()) {
				continue;
			}
			// See if ss lag state changed
			TestState localState = ssLag.get() < self->lagThreshold ? TestState::SS_LAG_NORMAL : TestState::SS_LAG_HIGH;
			bool stateTransition = localState != testState;
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
						const GrayFailureCheckResult result =
						    co_await grayFailureStatusCheck(db, self->cloggedRemoteTLog.get());
						// A fully collected status that omits gray_failure is a real regression: in simulation
						// CC_GRAY_FAILURE_STATUS_JSON is always enabled, so a complete status must include it.
						ASSERT(result != GrayFailureCheckResult::Missing);
						// Only stop probing once gray_failure was actually confirmed. An Incomplete status
						// (common right after the recovery that performs the exclusion) is inconclusive, so keep
						// checking on later iterations.
						statusCheckPassed = (result == GrayFailureCheckResult::Confirmed);
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
