/*
 * test.cpp
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

#include <cstdio>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <fmt/ranges.h>

#include "flow/CoroUtils.h"
#include "flow/DeterministicRandom.h"
#include "flow/Platform.h"
#include "flow/ProcessEvents.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "fdbrpc/sim_validation.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/DataDistributionConfig.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/tester/KnobProtectiveGroups.h"
#include "ConsistencyChecker.h"
#include "DatabaseMaintenance.h"
#include "TestSpecParser.h"
#include "TesterServer.h"
#include "fdbserver/tester/WorkloadUtils.h"
#include "fdbserver/tester/tester.h"
#include "fdbserver/tester/workloads.h"

int passCount = 0;
int failCount = 0;

template <class T>
void throwIfError(const std::vector<Future<ErrorOr<T>>>& futures, std::string errorMsg) {
	for (auto& future : futures) {
		if (future.get().isError()) {
			TraceEvent(SevError, errorMsg.c_str()).error(future.get().getError());
			throw future.get().getError();
		}
	}
}

Future<DistributedTestResults> runWorkload(Database const& cx,
                                           std::vector<TesterInterface> const& testers,
                                           TestSpec const& spec) {
	// C++20 coroutine safety: copy const& params to survive across suspend points
	Database cxCopy = cx;
	std::vector<TesterInterface> testersCopy = testers;
	TestSpec specCopy = spec;
	std::string name = printable(specCopy.title);

	TraceEvent("TestRunning")
	    .detail("WorkloadTitle", specCopy.title)
	    .detail("TesterCount", testersCopy.size())
	    .detail("Phases", specCopy.phases)
	    .detail("TestTimeout", specCopy.timeout);

	std::vector<Future<WorkloadInterface>> workRequests;
	std::vector<std::vector<PerfMetric>> metricsResults;

	int i = 0;
	int success = 0;
	int failure = 0;
	int64_t sharedRandom = deterministicRandom()->randomInt64(0, 10000000);
	for (; i < testersCopy.size(); i++) {
		WorkloadRequest req;
		req.title = specCopy.title;
		req.useDatabase = specCopy.useDB;
		req.runFailureWorkloads = specCopy.runFailureWorkloads;
		req.timeout = specCopy.timeout;
		req.databasePingDelay = specCopy.useDB ? specCopy.databasePingDelay : 0.0;
		req.options = specCopy.options;
		req.clientId = i;
		req.clientCount = testersCopy.size();
		req.sharedRandomNumber = sharedRandom;
		req.disabledFailureInjectionWorkloads = specCopy.disabledFailureInjectionWorkloads;
		workRequests.push_back(testersCopy[i].recruitments.getReply(req));
	}

	std::vector<WorkloadInterface> workloads = co_await getAll(workRequests);
	double waitForFailureTime = g_network->isSimulated() ? 24 * 60 * 60 : 60;
	if (g_network->isSimulated() && specCopy.simCheckRelocationDuration)
		debug_setCheckRelocationDuration(true);

	if (specCopy.phases & TestWorkload::SETUP) {
		std::vector<Future<ErrorOr<Void>>> setups;
		printf("setting up test (%s)...\n", printable(specCopy.title).c_str());
		TraceEvent("TestSetupStart").detail("WorkloadTitle", specCopy.title);
		setups.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			setups.push_back(workloads[i].setup.template getReplyUnlessFailedFor<Void>(waitForFailureTime, 0));
		co_await waitForAll(setups);
		throwIfError(setups, "SetupFailedForWorkload" + printable(specCopy.title));
		TraceEvent("TestSetupComplete").detail("WorkloadTitle", specCopy.title);
		TraceEvent("TestProgress").log("runWorkload: workload [%s] setup finished", name.c_str());
	}

	if (specCopy.phases & TestWorkload::EXECUTION) {
		TraceEvent("TestStarting").detail("WorkloadTitle", specCopy.title);
		printf("running test (%s)...\n", printable(specCopy.title).c_str());
		std::vector<Future<ErrorOr<Void>>> starts;
		starts.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			starts.push_back(workloads[i].start.template getReplyUnlessFailedFor<Void>(waitForFailureTime, 0));
		co_await waitForAll(starts);
		throwIfError(starts, "StartFailedForWorkload" + printable(specCopy.title));
		printf("%s complete\n", printable(specCopy.title).c_str());
		TraceEvent("TestComplete").detail("WorkloadTitle", specCopy.title);
		TraceEvent("TestProgress").log("runWorkload: workload [%s] start finished", name.c_str());
	}

	if (specCopy.phases & TestWorkload::CHECK) {
		if (specCopy.useDB && (specCopy.phases & TestWorkload::EXECUTION)) {
			co_await delay(3.0);
		}

		std::vector<Future<ErrorOr<CheckReply>>> checks;
		TraceEvent("TestCheckingResults").detail("WorkloadTitle", specCopy.title);

		printf("checking test (%s)...\n", name.c_str());
		TraceEvent("TestProgress").log("runWorkload: calling check interface for test [%s]", name.c_str());

		checks.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			checks.push_back(workloads[i].check.template getReplyUnlessFailedFor<CheckReply>(waitForFailureTime, 0));
		co_await waitForAll(checks);

		throwIfError(checks, "CheckFailedForWorkload" + printable(specCopy.title));

		for (int i = 0; i < checks.size(); i++) {
			if (checks[i].get().get().value)
				success++;
			else
				failure++;
		}
		TraceEvent("TestCheckComplete").detail("WorkloadTitle", specCopy.title);
		TraceEvent("TestProgress")
		    .log("finished check() for test [%s]; success [%d], failure [%d]", name.c_str(), success, failure);
	}

	if (specCopy.phases & TestWorkload::METRICS) {
		std::vector<Future<ErrorOr<std::vector<PerfMetric>>>> metricTasks;
		printf("fetching metrics (%s)...\n", printable(specCopy.title).c_str());
		TraceEvent("TestFetchingMetrics").detail("WorkloadTitle", specCopy.title);
		TraceEvent("TestProgress").log("runWorkload: calling metrics interface for test [%s]", name.c_str());
		metricTasks.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			metricTasks.push_back(
			    workloads[i].metrics.template getReplyUnlessFailedFor<std::vector<PerfMetric>>(waitForFailureTime, 0));
		co_await waitForAll(metricTasks);
		throwIfError(metricTasks, "MetricFailedForWorkload" + printable(specCopy.title));
		for (int i = 0; i < metricTasks.size(); i++) {
			metricsResults.push_back(metricTasks[i].get().get());
		}
	}

	// Stopping the workloads is unreliable, but they have a timeout
	// FIXME: stop if one of the above phases throws an exception
	for (int i = 0; i < workloads.size(); i++)
		workloads[i].stop.send(ReplyPromise<Void>());

	co_return DistributedTestResults(aggregateMetrics(metricsResults), success, failure);
}

// Sets the database configuration by running the ChangeConfig workload
Future<Void> changeConfiguration(Database cx, std::vector<TesterInterface> testers, StringRef configMode) {
	TestSpec spec;
	Standalone<VectorRef<KeyValueRef>> options;
	spec.title = "ChangeConfig"_sr;
	spec.runFailureWorkloads = false;
	options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ChangeConfig"_sr));
	options.push_back_deep(options.arena(), KeyValueRef("configMode"_sr, configMode));
	spec.options.push_back_deep(spec.options.arena(), options);

	DistributedTestResults testResults = co_await runWorkload(cx, testers, spec);

	co_return;
}

Future<bool> runTest(Database cx,
                     std::vector<TesterInterface> testers,
                     TestSpec spec,
                     Reference<AsyncVar<ServerDBInfo>> dbInfo,
                     TesterConsistencyScanState* consistencyScanState) {
	DistributedTestResults testResults;
	double savedDisableDuration = 0;
	bool hadException = false;
	Error caughtError;
	std::string name = spec.title.toString();

	try {
		Future<DistributedTestResults> fTestResults = runWorkload(cx, testers, spec);
		if (g_network->isSimulated() && spec.simConnectionFailuresDisableDuration > 0) {
			savedDisableDuration = g_simulator->connectionFailuresDisableDuration;
			g_simulator->connectionFailuresDisableDuration = spec.simConnectionFailuresDisableDuration;
		}
		if (spec.timeout > 0) {
			fTestResults = timeoutError(fTestResults, spec.timeout);
		}
		DistributedTestResults _testResults = co_await fTestResults;
		printf("Test complete\n");
		TraceEvent("TestProgress").log("runTest: test [%s] complete", name.c_str());
		testResults = _testResults;
		logMetrics(testResults.metrics);
		if (g_network->isSimulated() && savedDisableDuration > 0) {
			g_simulator->connectionFailuresDisableDuration = savedDisableDuration;
		}
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			auto msg = fmt::format("Process timed out after {} seconds", spec.timeout);
			ProcessEvents::trigger("Timeout"_sr, StringRef(msg), e);
			TraceEvent(SevError, "TestFailure")
			    .error(e)
			    .detail("Reason", "Test timed out")
			    .detail("Timeout", spec.timeout);
			fprintf(stderr, "ERROR: Test [%s] timed out after %d seconds.\n", name.c_str(), spec.timeout);
			testResults.failures = testers.size();
			testResults.successes = 0;
		} else {
			caughtError = e;
			hadException = true;
		}
	}

	if (hadException) {
		throw caughtError;
	}

	bool ok = testResults.ok();

	if (spec.useDB) {
		printf("%d test clients passed; %d test clients failed\n", testResults.successes, testResults.failures);
		TraceEvent("TestProgress")
		    .log("runTest: [%d] test clients passed; [%d] test clients failed",
		         testResults.successes,
		         testResults.failures);
		if (spec.dumpAfterTest) {
			Error dumpErr;
			try {
				co_await timeoutError(dumpDatabase(cx, "dump after " + printable(spec.title) + ".html", allKeys), 30.0);
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to dump database");
				ok = false;
			}

			co_await delay(1.0);
		}

		TraceEvent("TestProgress").log("runTest: invoking checkConsistencyScanAfterTest()");
		// Disable consistency scan before checkConsistency because otherwise it will prevent quiet database from
		// quiescing
		co_await checkConsistencyScanAfterTest(cx, consistencyScanState);
		printf("Consistency scan done\n");
		TraceEvent("TestProgress").log("runTest: checkConsistencyScanAfterTest returned");

		// Run the consistency check workload
		if (spec.runConsistencyCheck) {
			bool quiescent = g_network->isSimulated() ? !BUGGIFY : spec.waitForQuiescenceEnd;
			try {
				printf("Running urgent consistency check...\n");
				TraceEvent("TestProgress").log("Running urgent consistency check");
				co_await timeoutError(checkConsistencyUrgentSim(cx, testers), 20000.0);
				printf("Urgent consistency check done\nRunning consistency check...\n");
				TraceEvent("TestProgress").log("Urgent consistency check done; now invoking checkConsistency()");
				co_await timeoutError(checkConsistency(cx,
				                                       testers,
				                                       quiescent,
				                                       spec.runConsistencyCheckOnTSS,
				                                       spec.maxDDRunTime > 0 ? spec.maxDDRunTime : 10000.0,
				                                       5000,
				                                       spec.databasePingDelay,
				                                       dbInfo),
				                      20000.0);
				printf("Consistency check done\n");
				TraceEvent("TestProgress").log("checkConsistency() returned");
			} catch (Error& e) {
				TraceEvent("TestProgress").log("Exception in checkConsistencyUrgentSim or checkConsistency");
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to perform consistency check");
				ok = false;
			}

			// Run auditStorage at the end of simulation
			if (ok && quiescent && g_network->isSimulated()) {
				try {
					TraceEvent("AuditStorageStart");
					TraceEvent("TestProgress")
					    .log("runTest: calling auditStorageCorrectness(dbinfo, ValidateHA, 1500.0)");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateHA), 1500.0);
					TraceEvent("AuditStorageCorrectnessHADone");
					TraceEvent("TestProgress")
					    .log("runTest: calling auditStorageCorrectness(dbinfo, ValidateReplica, 1500.0)");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateReplica), 1500.0);
					TraceEvent("AuditStorageCorrectnessReplicaDone");
					TraceEvent("TestProgress")
					    .log("runTest: calling auditStorageCorrectness(dbinfo, ValidateLocationMetadata, 1500.0)");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateLocationMetadata), 1500.0);

					TraceEvent("AuditStorageCorrectnessLocationMetadataDone");
					TraceEvent("TestProgress")
					    .log("runTest: calling auditStorageCorrectness(dbinfo, ValidateSTorageServerShard, 1500.0)");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateStorageServerShard),
					                      1500.0);
					TraceEvent("AuditStorageCorrectnessStorageServerShardDone");
					TraceEvent("TestProgress").log("runTest: storage audits completed.");
				} catch (Error& e) {
					ok = false;
					TraceEvent(SevError, "TestFailure")
					    .error(e)
					    .detail("Reason", "Unable to perform auditStorage check.");
				}
			}
		}
	}

	TraceEvent(ok ? SevInfo : SevWarnAlways, "TestResults").detail("Workload", spec.title).detail("Passed", (int)ok);
	TraceEvent("TestProgress")
	    .log("runTest: Workload [%s] passed: [%s]", spec.title.toString().c_str(), (ok ? "TRUE" : "FALSE"));

	if (ok) {
		passCount++;
	} else {
		failCount++;
	}

	printf("%d test clients passed; %d test clients failed\n", testResults.successes, testResults.failures);
	TraceEvent("TestProgress")
	    .log(
	        "runTest: [%d] test clients passed; [%d] test clients failed", testResults.successes, testResults.failures);

	if (spec.useDB && spec.clearAfterTest) {
		Error clearErr;
		try {
			TraceEvent("TesterClearingDatabase").log();
			TraceEvent("TestProgress").log("runTest: cleaning database via clearData(cx, 1000.0)");
			co_await timeoutError(clearData(cx), 1000.0);
		} catch (Error& e) {
			TraceEvent(SevError, "ErrorClearingDatabaseAfterTest").error(e);
			throw; // If we didn't do this, we don't want any later tests to run on this DB
		}

		co_await delay(1.0);
	}

	co_return ok;
}

Future<Void> monitorServerDBInfo(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface,
                                 LocalityData locality,
                                 Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	// Initially most of the serverDBInfo is not known, but we know our locality right away
	ServerDBInfo localInfo;
	localInfo.myLocality = locality;
	dbInfo->set(localInfo);

	while (true) {
		GetServerDBInfoRequest req;
		req.knownServerInfoID = dbInfo->get().id;

		Future<ServerDBInfo> getInfoReq =
		    ccInterface->get().present() ? brokenPromiseToNever(ccInterface->get().get().getServerDBInfo.getReply(req))
		                                 : Future<ServerDBInfo>(Never());
		Future<Void> ccChange = ccInterface->onChange();

		int action = 0;
		ServerDBInfo gotInfo;

		co_await Choose()
		    .When(getInfoReq,
		          [&](ServerDBInfo const& info) {
			          gotInfo = info;
			          action = 1;
		          })
		    .When(ccChange, [&](Void const&) { action = 2; })
		    .run();

		if (action == 1) {
			TraceEvent("GotServerDBInfoChange")
			    .detail("ChangeID", gotInfo.id)
			    .detail("MasterID", gotInfo.master.id())
			    .detail("RatekeeperID", gotInfo.ratekeeper.present() ? gotInfo.ratekeeper.get().id() : UID())
			    .detail("DataDistributorID", gotInfo.distributor.present() ? gotInfo.distributor.get().id() : UID());

			gotInfo.myLocality = locality;
			dbInfo->set(gotInfo);
		} else if (action == 2) {
			if (ccInterface->get().present())
				TraceEvent("GotCCInterfaceChange")
				    .detail("CCID", ccInterface->get().get().id())
				    .detail("CCMachine", ccInterface->get().get().getWorkers.getEndpoint().getPrimaryAddress());
		}
	}
}

Future<Void> initializeSimConfig(Database db) {
	Transaction tr(db);
	ASSERT(g_network->isSimulated());
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			DatabaseConfiguration dbConfig = co_await getDatabaseConfiguration(&tr);
			g_simulator->storagePolicy = dbConfig.storagePolicy;
			g_simulator->tLogPolicy = dbConfig.tLogPolicy;
			g_simulator->tLogWriteAntiQuorum = dbConfig.tLogWriteAntiQuorum;
			g_simulator->usableRegions = dbConfig.usableRegions;

			// If the same region is being shared between the remote and a satellite, then our simulated policy checking
			// may fail to account for the total number of needed machines when deciding what can be killed. To work
			// around this, we increase the required transaction logs in the remote policy to include the number of
			// satellite logs that may get recruited there
			bool foundSharedDcId = false;
			std::set<Key> dcIds;
			int maxSatelliteReplication = 0;
			for (auto const& r : dbConfig.regions) {
				if (!dcIds.insert(r.dcId).second) {
					foundSharedDcId = true;
				}
				if (!r.satellites.empty() && r.satelliteTLogReplicationFactor > 0 && r.satelliteTLogUsableDcs > 0) {
					for (auto const& s : r.satellites) {
						if (!dcIds.insert(s.dcId).second) {
							foundSharedDcId = true;
						}
					}

					maxSatelliteReplication =
					    std::max(maxSatelliteReplication, r.satelliteTLogReplicationFactor / r.satelliteTLogUsableDcs);
				}
			}

			if (foundSharedDcId) {
				int totalRequired = std::max(dbConfig.tLogReplicationFactor, dbConfig.remoteTLogReplicationFactor) +
				                    maxSatelliteReplication;
				g_simulator->remoteTLogPolicy = Reference<IReplicationPolicy>(
				    new PolicyAcross(totalRequired, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
				TraceEvent("ChangingSimTLogPolicyForSharedRemote")
				    .detail("TotalRequired", totalRequired)
				    .detail("MaxSatelliteReplication", maxSatelliteReplication)
				    .detail("ActualPolicy", dbConfig.getRemoteTLogPolicy()->info())
				    .detail("SimulatorPolicy", g_simulator->remoteTLogPolicy->info());
			} else {
				g_simulator->remoteTLogPolicy = dbConfig.getRemoteTLogPolicy();
			}

			co_return;
		} catch (Error& e) {
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await tr.onError(caughtError);
		}
	}
}

// Disables connection failures after the given time seconds
Future<Void> disableConnectionFailuresAfter(double seconds, std::string context) {
	if (g_network->isSimulated()) {
		TraceEvent(SevWarnAlways, ("ScheduleDisableConnectionFailures_" + context).c_str())
		    .detail("At", now() + seconds);
		co_await delay(seconds);
		while (true) {
			double delaySeconds = disableConnectionFailures(context, ForceDisable::False);
			if (delaySeconds > DISABLE_CONNECTION_FAILURE_MIN_INTERVAL) {
				co_await delay(delaySeconds);
			} else {
				// disableConnectionFailures will always take effect if less than
				// DISABLE_CONNECTION_FAILURE_MIN_INTERVAL is returned.
				break;
			}
		}
	}
	co_return;
}

/**
 * Test orchestrator: sends test specification to testers in the right order and collects the results.
 *
 * There are multiple actors in this file with similar names (runTest, runTests) and slightly different signatures.
 *
 * This is the actual orchestrator. It reads the test specifications (from tests), prepares the cluster (by running the
 * configure command given in startingConfiguration) and then runs the workload.
 *
 * NOTE: this assumes that referenced objects will outlive all suspension points encounted by this function.
 *
 * Rturns a future which will be set after all tests finished.
 */
Future<Void> runTests7(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> cc,
                       Reference<AsyncVar<Optional<struct ClusterInterface>>> ci,
                       std::vector<TesterInterface> testers,
                       std::vector<TestSpec> tests,
                       StringRef startingConfiguration,
                       LocalityData locality,
                       bool restartingTest) {
	Database cx;
	Reference<AsyncVar<ServerDBInfo>> dbInfo(new AsyncVar<ServerDBInfo>);
	Future<Void> ccMonitor = monitorServerDBInfo(cc, LocalityData(), dbInfo); // FIXME: locality

	bool useDB = false;
	bool waitForQuiescenceBegin = false;
	bool waitForQuiescenceEnd = false;
	bool restorePerpetualWiggleSetting = false;
	bool perpetualWiggleEnabled = false;
	bool backupWorkerEnabled = false;
	double startDelay = 0.0;
	double databasePingDelay = 1e9;
	ISimulator::BackupAgentType simBackupAgents = ISimulator::BackupAgentType::NoBackupAgents;
	ISimulator::BackupAgentType simDrAgents = ISimulator::BackupAgentType::NoBackupAgents;
	bool enableDD = false;
	TesterConsistencyScanState consistencyScanState;

	// Gives chance for g_network->run() to run inside event loop and hence let
	// the tests see correct value for `isOnMainThread()`.
	// FIXME: this type of knowledge of random implementation details elsewhere is obviously
	// undesirable and getting rid of hacks like this to simplify things, reduce cognitive load,
	// etc would be nice.
	co_await yield();

	if (tests.empty()) {
		useDB = true;
	}
	for (auto iter = tests.begin(); iter != tests.end(); ++iter) {
		if (iter->useDB) {
			useDB = true;
		}
		if (iter->waitForQuiescenceBegin) {
			waitForQuiescenceBegin = true;
		}
		if (iter->waitForQuiescenceEnd) {
			waitForQuiescenceEnd = true;
		}
		if (iter->restorePerpetualWiggleSetting) {
			restorePerpetualWiggleSetting = true;
		}
		startDelay = std::max(startDelay, iter->startDelay);
		databasePingDelay = std::min(databasePingDelay, iter->databasePingDelay);
		if (iter->simBackupAgents != ISimulator::BackupAgentType::NoBackupAgents) {
			simBackupAgents = iter->simBackupAgents;
		}

		if (iter->simDrAgents != ISimulator::BackupAgentType::NoBackupAgents) {
			simDrAgents = iter->simDrAgents;
		}
		enableDD = enableDD || getOption(iter->options[0], "enableDD"_sr, false);
	}

	if (g_network->isSimulated()) {
		g_simulator->backupAgents = simBackupAgents;
		g_simulator->drAgents = simDrAgents;
	}

	// turn off the database ping functionality if the suite of tests are not going to be using the database
	if (!useDB) {
		databasePingDelay = 0.0;
	}

	if (useDB) {
		cx = openDBOnServer(dbInfo);
	}

	consistencyScanState.enabled = g_network->isSimulated() && deterministicRandom()->coinflip();
	consistencyScanState.waitForComplete =
	    consistencyScanState.enabled && waitForQuiescenceEnd && deterministicRandom()->coinflip();
	consistencyScanState.enableAfter = consistencyScanState.waitForComplete && deterministicRandom()->random01() < 0.1;

	disableConnectionFailures("Tester");

	// Change the configuration (and/or create the database) if necessary
	printf("startingConfiguration:%s start\n", startingConfiguration.toString().c_str());
	fmt::print("useDB: {}\n", useDB);

	TraceEvent("TestProgress")
	    .log("runTests7: startingConfiguration: [%s]; useDB: [%s]",
	         startingConfiguration.toString().c_str(),
	         (useDB ? "TRUE" : "FALSE"));

	// If this is important it should be in the trace file also.  Who wants to read two places
	// when we could be reading just one?
	printSimulatedTopology();
	if (useDB && !startingConfiguration.empty()) {
		Error configErr;
		try {
			co_await timeoutError(changeConfiguration(cx, testers, startingConfiguration), 2000.0);
			if (g_network->isSimulated() && enableDD) {
				co_await setDDMode(cx, 1);
			}
		} catch (Error& e) {
			TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to set starting configuration");
		}
		std::string_view confView(reinterpret_cast<const char*>(startingConfiguration.begin()),
		                          startingConfiguration.size());
		if (restorePerpetualWiggleSetting) {
			const std::string setting = "perpetual_storage_wiggle:=";
			auto pos = confView.find(setting);
			if (pos != confView.npos && confView.at(pos + setting.size()) == '1') {
				perpetualWiggleEnabled = true;
			}
		}
		const std::string bwSetting = "backup_worker_enabled:=";
		auto pos = confView.find(bwSetting);
		if (pos != confView.npos && confView.at(pos + bwSetting.size()) == '1') {
			backupWorkerEnabled = true;
		}
	}

	// Read cluster configuration
	if (useDB && g_network->isSimulated()) {
		DatabaseConfiguration configuration = co_await getDatabaseConfiguration(cx);

		g_simulator->storagePolicy = configuration.storagePolicy;
		g_simulator->tLogPolicy = configuration.tLogPolicy;
		g_simulator->tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
		g_simulator->remoteTLogPolicy = configuration.remoteTLogPolicy;
		g_simulator->usableRegions = configuration.usableRegions;
		if (!configuration.regions.empty()) {
			g_simulator->primaryDcId = configuration.regions[0].dcId;
			g_simulator->hasSatelliteReplication = configuration.regions[0].satelliteTLogReplicationFactor > 0;
			if (configuration.regions[0].satelliteTLogUsableDcsFallback > 0) {
				g_simulator->satelliteTLogPolicyFallback = configuration.regions[0].satelliteTLogPolicyFallback;
				g_simulator->satelliteTLogWriteAntiQuorumFallback =
				    configuration.regions[0].satelliteTLogWriteAntiQuorumFallback;
			} else {
				g_simulator->satelliteTLogPolicyFallback = configuration.regions[0].satelliteTLogPolicy;
				g_simulator->satelliteTLogWriteAntiQuorumFallback =
				    configuration.regions[0].satelliteTLogWriteAntiQuorum;
			}
			g_simulator->satelliteTLogPolicy = configuration.regions[0].satelliteTLogPolicy;
			g_simulator->satelliteTLogWriteAntiQuorum = configuration.regions[0].satelliteTLogWriteAntiQuorum;

			for (const auto& s : configuration.regions[0].satellites) {
				g_simulator->primarySatelliteDcIds.push_back(s.dcId);
			}
		} else {
			g_simulator->hasSatelliteReplication = false;
			g_simulator->satelliteTLogWriteAntiQuorum = 0;
		}

		if (configuration.regions.size() == 2) {
			g_simulator->remoteDcId = configuration.regions[1].dcId;
			ASSERT((!configuration.regions[0].satelliteTLogPolicy && !configuration.regions[1].satelliteTLogPolicy) ||
			       configuration.regions[0].satelliteTLogPolicy->info() ==
			           configuration.regions[1].satelliteTLogPolicy->info());

			for (const auto& s : configuration.regions[1].satellites) {
				g_simulator->remoteSatelliteDcIds.push_back(s.dcId);
			}
		}

		if (restartingTest || g_simulator->usableRegions < 2 || !g_simulator->hasSatelliteReplication) {
			g_simulator->allowLogSetKills = false;
		}

		ASSERT(g_simulator->storagePolicy && g_simulator->tLogPolicy);
		ASSERT(!g_simulator->hasSatelliteReplication || g_simulator->satelliteTLogPolicy);

		// Randomly inject custom shard configuration
		// TODO:  Move this to a workload representing non-failure behaviors which can be randomly added to any test
		// run.
		if (deterministicRandom()->random01() < 0.25) {
			co_await customShardConfigWorkload(cx);
		}
	}

	if (useDB) {
		if (g_network->isSimulated()) {
			co_await initializeSimConfig(cx);
		}
	}

	bool runPretestChecks = (useDB && waitForQuiescenceBegin);
	TraceEvent("TestProgress").log("runTests7: doing PreTestChecks? [%s]", (runPretestChecks ? "TRUE" : "FALSE"));

	if (runPretestChecks) {
		TraceEvent("TesterStartingPreTestChecks")
		    .detail("DatabasePingDelay", databasePingDelay)
		    .detail("StartDelay", startDelay);

		Error quietDbErr;
		try {
			co_await (quietDatabase(cx, dbInfo, "Start") ||
			          (databasePingDelay == 0.0
			               ? Never()
			               : testDatabaseLiveness(cx, databasePingDelay, "QuietDatabaseStart", startDelay)));
		} catch (Error& e) {
			TraceEvent("QuietDatabaseStartExternalError").error(e);
			throw;
		}

		if (perpetualWiggleEnabled) { // restore the enabled perpetual storage wiggle setting
			printf("Set perpetual_storage_wiggle=1 ...\n");
			Version cVer = co_await setPerpetualStorageWiggle(cx, true, LockAware::True);
			(void)cVer;
			printf("Set perpetual_storage_wiggle=1 Done.\n");
		}

		if (backupWorkerEnabled) {
			printf("Enabling backup worker ...\n");
			co_await enableBackupWorker(cx);
			printf("Enabled backup worker.\n");
		}

		// TODO: Move this to a BehaviorInjection workload once that concept exists.
		if (consistencyScanState.enabled && !consistencyScanState.enableAfter) {
			printf("Enabling consistency scan ...\n");
			co_await enableConsistencyScanInSim(cx);
			printf("Enabled consistency scan.\n");
		}
	}

	// Use the first test's connectionFailuresDisableDuration if set
	double connectionFailuresDisableDuration = 0.0;
	if (!tests.empty() && tests[0].simConnectionFailuresDisableDuration > 0) {
		connectionFailuresDisableDuration = tests[0].simConnectionFailuresDisableDuration;
	}

	// Must be at function scope so the background actor isn't cancelled when the else block exits.
	// In the original ACTOR code, `state` hoists variables to actor scope regardless of block scope.
	Future<Void> disabler;
	if (connectionFailuresDisableDuration > 0) {
		// Disable connection failures with specified duration
		disableConnectionFailures("Tester", ForceDisable::True, connectionFailuresDisableDuration);
	} else {
		enableConnectionFailures("Tester", FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS);
		disabler = disableConnectionFailuresAfter(FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, "Tester");
	}
	Future<Void> repairDataCenter;
	if (useDB) {
		// Keep datacenter repair at the default duration regardless of connection failures setting
		Future<Void> reconfigure = reconfigureAfter(cx, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, dbInfo, "Tester");
		repairDataCenter = reconfigure;
	}

	TraceEvent("TestProgress").log("runTests7: going to run [%d] tests", tests.size());
	// Also emit legacy log:
	TraceEvent("TestsExpectedToPass").detail("Count", tests.size());

	int idx = 0;
	std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup;
	for (; idx < tests.size(); idx++) {
		printf("Run test:%s start\n", tests[idx].title.toString().c_str());
		TraceEvent("TestProgress").log("runTests7: starting test [%s]", tests[idx].title.toString().c_str());
		knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(tests[idx].overrideKnobs);
		co_await runTest(cx, testers, tests[idx], dbInfo, &consistencyScanState);
		knobProtectiveGroup.reset(nullptr);
		printf("Run test:%s Done.\n", tests[idx].title.toString().c_str());
		TraceEvent("TestProgress").log("runTests7: done running test [%s]", tests[idx].title.toString().c_str());
	}

	printf("\n%d tests passed; %d tests failed.\n", passCount, failCount);
	TraceEvent("TestProgress").log("runTests7: [%d] tests passed; [%d] tests failed.", passCount, failCount);

	bool ranConsistencyScan = false;
	if (useDB) {
		if (waitForQuiescenceEnd) {
			TraceEvent("TestProgress")
			    .log("runTests7: useDB && waitForQuiescenceEnd ==> invoking checkConsistencyScanAfterTest()");
			printf("Waiting for DD to end...\n");
			TraceEvent("QuietDatabaseEndStart");
			try {
				TraceEvent("QuietDatabaseEndWait");
				ranConsistencyScan = true;
				Future<Void> waitConsistencyScanEnd = checkConsistencyScanAfterTest(cx, &consistencyScanState);
				Future<Void> waitQuietDatabaseEnd =
				    quietDatabase(cx, dbInfo, "End", 0, 2e6, 2e6) ||
				    (databasePingDelay == 0.0 ? Never()
				                              : testDatabaseLiveness(cx, databasePingDelay, "QuietDatabaseEnd"));

				co_await (waitConsistencyScanEnd && waitQuietDatabaseEnd);
			} catch (Error& e) {
				TraceEvent("QuietDatabaseEndExternalError").error(e);
				throw;
			}
		}
	}
	if (ranConsistencyScan) {
		TraceEvent("TestProgress").log("runTests7: finished running consistency scan");
	} else {
		TraceEvent("TestProgress").log("runTests7: didn't run consistency scan");
	}

	printf("\n");

	co_return;
}

/**
 * \brief Proxy function that waits until enough testers are available and then calls into the orchestrator.
 *
 * There are multiple actors in this file with similar names (runTest, runTests) and slightly different signatures.
 *
 * This actor wraps the actual orchestrator (also called runTests). But before calling that actor, it waits for enough
 * testers to come up.
 *
 * \param cc The cluster controller interface
 * \param ci Same as cc.clientInterface
 * \param tests The test specifications to run
 * \param minTestersExpected The number of testers to expect. This actor will block until it can find this many testers.
 * \param startingConfiguration If non-empty, the orchestrator will attempt to set this configuration before starting
 * the tests.
 * \param locality client locality (it seems this is unused?)
 *
 * \returns A future which will be set after all tests finished.
 */
Future<Void> runTests8(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> cc,
                       Reference<AsyncVar<Optional<struct ClusterInterface>>> ci,
                       std::vector<TestSpec> tests,
                       test_location_t at,
                       int minTestersExpected,
                       StringRef startingConfiguration,
                       LocalityData locality,
                       bool restartingTest) {
	int flags = (at == TEST_ON_SERVERS ? 0 : GetWorkersRequest::TESTER_CLASS_ONLY) |
	            GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY;
	Future<Void> testerTimeout = delay(600.0); // wait 600 sec for testers to show up
	std::vector<WorkerDetails> workers;

	while (true) {
		Future<std::vector<WorkerDetails>> getWorkersReq =
		    cc->get().present() ? brokenPromiseToNever(cc->get().get().getWorkers.getReply(GetWorkersRequest(flags)))
		                        : Future<std::vector<WorkerDetails>>(Never());
		Future<Void> ccChange = cc->onChange();

		int action = 0;
		std::vector<WorkerDetails> gotWorkers;

		co_await Choose()
		    .When(getWorkersReq,
		          [&](std::vector<WorkerDetails> const& w) {
			          gotWorkers = w;
			          action = 1;
		          })
		    .When(ccChange, [&](Void const&) { action = 2; })
		    .When(testerTimeout, [&](Void const&) { action = 3; })
		    .run();

		if (action == 1) {
			if (static_cast<int>(gotWorkers.size()) >= minTestersExpected) {
				workers = gotWorkers;
				break;
			}
			co_await delay(SERVER_KNOBS->WORKER_POLL_DELAY);
		} else if (action == 3) {
			TraceEvent(SevError, "TesterRecruitmentTimeout").log();
			throw timed_out();
		}
		// action == 2: cc changed, loop again
	}

	std::vector<TesterInterface> ts;
	ts.reserve(workers.size());
	for (int i = 0; i < workers.size(); i++)
		ts.push_back(workers[i].interf.testerInterface);

	co_await runTests7(cc, ci, ts, tests, startingConfiguration, locality, restartingTest);
	co_return;
}

/**
 * \brief Set up testing environment and run the given tests on a cluster.
 *
 * There are multiple actors in this file with similar names (runTest, runTests) and slightly different signatures.
 *
 * This actor is usually the first entry point into the test environment. It itself doesn't implement too much
 * functionality. Its main purpose is to generate the test specification from passed arguments and then call into the
 * correct actor which will orchestrate the actual test.
 *
 * \param connRecord A cluster connection record. Not all tests require a functional cluster but all tests require
 * a cluster record.
 * \param whatToRun TEST_TYPE_FROM_FILE to read the test description from a passed toml file or
 * TEST_TYPE_CONSISTENCY_CHECK to generate a test spec for consistency checking
 * \param at TEST_HERE: this process will act as a test client and execute the given workload. TEST_ON_SERVERS: Run a
 * test client on every worker in the cluster. TEST_ON_TESTERS: Run a test client on all servers with class Test
 * \param minTestersExpected In at is not TEST_HERE, this will instruct the orchestrator until it can find at least
 * minTestersExpected test-clients. This is usually passed through from a command line argument. In simulation, the
 * simulator will pass the number of testers that it started.
 * \param fileName The path to the toml-file containing the test description. Is ignored if whatToRun !=
 * TEST_TYPE_FROM_FILE
 * \param startingConfiguration Can be used to configure a cluster before running the test. If this is an empty string,
 * it will be ignored, otherwise it will be passed to changeConfiguration.
 * \param locality The client locality to be used. This is only used if at == TEST_HERE
 *
 * \returns A future which will be set after all tests finished.
 */
Future<Void> runTests(Reference<IClusterConnectionRecord> const& connRecordUnsafe,
                      test_type_t const& whatToRunUnsafe,
                      test_location_t const& atUnsafe,
                      int const& minTestersExpectedUnsafe,
                      std::string const& fileNameUnsafe,
                      StringRef const& startingConfigurationUnsafe,
                      LocalityData const& localityUnsafe,
                      UnitTestParameters const& testOptionsUnsafe,
                      bool const& restartingTestUnsafe) {
	// C++20 coroutine safety: copy parameters that might bind to temporaries (default args).
	// const& parameters only store the reference in the coroutine frame; temporaries are
	// destroyed after the first suspend point, leaving dangling references.
	// Just do this for all parameters, including ones that could be passed by value.
	Reference<IClusterConnectionRecord> connRecord = connRecordUnsafe;
	test_type_t whatToRun = whatToRunUnsafe;
	test_location_t at = atUnsafe;
	int minTestersExpected = minTestersExpectedUnsafe;
	std::string fileName = fileNameUnsafe;
	StringRef startingConfiguration = startingConfigurationUnsafe;
	LocalityData locality = localityUnsafe;
	UnitTestParameters testOptions = testOptionsUnsafe;
	bool restartingTest = restartingTestUnsafe;

	TestSet testSet;
	std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup(nullptr);
	auto cc = makeReference<AsyncVar<Optional<ClusterControllerFullInterface>>>();
	auto ci = makeReference<AsyncVar<Optional<ClusterInterface>>>();
	std::vector<Future<Void>> actors;
	if (connRecord) {
		actors.push_back(reportErrors(monitorLeader(connRecord, cc), "MonitorLeader"));
		actors.push_back(reportErrors(extractClusterInterface(cc, ci), "ExtractClusterInterface"));
	}

	TraceEvent("TestProgress").log("runTests: invoked with whatToRun = [%d]", whatToRun);

	if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK_URGENT) {
		// Need not to set spec here. Will set spec when triggering workload
	} else if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = "ConsistencyCheck"_sr;
		spec.runFailureWorkloads = false;
		spec.databasePingDelay = 0;
		spec.timeout = 0;
		spec.waitForQuiescenceBegin = false;
		spec.waitForQuiescenceEnd = false;
		std::string rateLimitMax = format("%d", CLIENT_KNOBS->CONSISTENCY_CHECK_RATE_LIMIT_MAX);
		options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ConsistencyCheck"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("performQuiescentChecks"_sr, "false"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("distributed"_sr, "false"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("failureIsError"_sr, "true"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("indefinite"_sr, "true"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("rateLimitMax"_sr, StringRef(rateLimitMax)));
		options.push_back_deep(options.arena(), KeyValueRef("shuffleShards"_sr, "true"_sr));
		spec.options.push_back_deep(spec.options.arena(), options);
		testSet.testSpecs.push_back(spec);
	} else if (whatToRun == TEST_TYPE_UNIT_TESTS) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = "UnitTests"_sr;
		spec.startDelay = 0;
		spec.useDB = false;
		spec.timeout = 0;
		options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "UnitTests"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("testsMatching"_sr, fileName));
		// Add unit test options as test spec options
		for (auto& kv : testOptions.params) {
			options.push_back_deep(options.arena(), KeyValueRef(kv.first, kv.second));
		}
		spec.options.push_back_deep(spec.options.arena(), options);
		testSet.testSpecs.push_back(spec);
	} else {
		ASSERT(whatToRun == TEST_TYPE_FROM_FILE);
		std::ifstream ifs;
		ifs.open(fileName.c_str(), std::ifstream::in);
		if (!ifs.good()) {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "file open failed")
			    .detail("File", fileName.c_str());
			fprintf(stderr, "ERROR: Could not open file `%s'\n", fileName.c_str());
			co_return;
		}
		enableClientInfoLogging(); // Enable Client Info logging by default for tester
		if (fileName.ends_with(".txt")) {
			testSet.testSpecs = readTests(ifs);
		} else if (fileName.ends_with(".toml")) {
			// TOML is weird about opening the file as binary on windows, so we
			// just let TOML re-open the file instead of using ifs.
			testSet = readTOMLTests(fileName);
		} else {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "unknown tests specification extension")
			    .detail("File", fileName.c_str());
			co_return;
		}
		ifs.close();
	}

	knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(testSet.overrideKnobs);
	Future<Void> tests;
	// These must be at function scope so they aren't destroyed when the if-block exits.
	// In the original ACTOR code, `state` hoists variables to actor scope regardless of block scope.
	Database urgentCx;
	Reference<AsyncVar<ServerDBInfo>> urgentDbInfo;
	Future<Void> urgentCcMonitor;
	if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK_URGENT) {
		urgentDbInfo = makeReference<AsyncVar<ServerDBInfo>>();
		urgentCcMonitor = monitorServerDBInfo(cc, LocalityData(), urgentDbInfo); // FIXME: locality
		urgentCx = openDBOnServer(urgentDbInfo);
		tests = reportErrors(
		    runConsistencyCheckerUrgentHolder(
		        cc, urgentCx, Optional<std::vector<TesterInterface>>(), minTestersExpected, /*repeatRun=*/true),
		    "runConsistencyCheckerUrgentHolder");
	} else if (at == TEST_HERE) {
		auto db = makeReference<AsyncVar<ServerDBInfo>>();
		std::vector<TesterInterface> iTesters(1);
		actors.push_back(
		    reportErrors(monitorServerDBInfo(cc, LocalityData(), db), "MonitorServerDBInfo")); // FIXME: Locality
		actors.push_back(reportErrors(testerServerCore(iTesters[0], connRecord, db, locality), "TesterServerCore"));
		tests = runTests7(cc, ci, iTesters, testSet.testSpecs, startingConfiguration, locality, restartingTest);
	} else {
		tests = reportErrors(
		    runTests8(
		        cc, ci, testSet.testSpecs, at, minTestersExpected, startingConfiguration, locality, restartingTest),
		    "RunTests");
	}

	TraceEvent("TestProgress").log("runTests: waiting for actors to finish.");

	Future<Void> actorsFuture = actors.empty() ? Never() : waitForAll(actors);
	co_await Choose()
	    .When(tests, [](Void const&) {})
	    .When(actorsFuture,
	          [](Void const&) {
		          ASSERT(false);
		          throw internal_error();
	          })
	    .run();
}

namespace {
Future<Void> testExpectedErrorImpl(Future<Void> test,
                                   const char* testDescr,
                                   Optional<Error> expectedError,
                                   Optional<bool*> successFlag,
                                   std::map<std::string, std::string> details,
                                   Optional<Error> throwOnError,
                                   UID id) {
	Error actualError;
	bool hadError = false;

	try {
		co_await test;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		actualError = e;
		hadError = true;
		// The test failed as expected
		if (!expectedError.present() || actualError.code() == expectedError.get().code()) {
			co_return;
		}
	}

	// The test has failed
	if (successFlag.present()) {
		*(successFlag.get()) = false;
	}
	TraceEvent evt(SevError, "TestErrorFailed", id);
	evt.detail("TestDescription", testDescr);
	if (expectedError.present()) {
		evt.detail("ExpectedError", expectedError.get().name());
		evt.detail("ExpectedErrorCode", expectedError.get().code());
	}
	if (hadError && actualError.isValid()) {
		evt.detail("ActualError", actualError.name());
		evt.detail("ActualErrorCode", actualError.code());
	} else {
		evt.detail("Reason", "Unexpected success");
	}

	// Make sure that no duplicate details were provided
	ASSERT(!details.contains("TestDescription"));
	ASSERT(!details.contains("ExpectedError"));
	ASSERT(!details.contains("ExpectedErrorCode"));
	ASSERT(!details.contains("ActualError"));
	ASSERT(!details.contains("ActualErrorCode"));
	ASSERT(!details.contains("Reason"));

	for (auto& p : details) {
		evt.detail(p.first.c_str(), p.second);
	}
	if (throwOnError.present()) {
		throw throwOnError.get();
	}
	co_return;
}
} // namespace

Future<Void> testExpectedError(Future<Void> test,
                               const char* testDescr,
                               Optional<Error> expectedError,
                               Optional<bool*> successFlag,
                               std::map<std::string, std::string> details,
                               Optional<Error> throwOnError,
                               UID id) {
	return testExpectedErrorImpl(test, testDescr, expectedError, successFlag, details, throwOnError, id);
}
