/*
 * IdleTLogQueueCleanup.cpp
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

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/ProcessData.h"
#include "fdbserver/core/FDBSimulationPolicy.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/simulator.h"
#include "flow/CoroUtils.h"
#include "flow/Platform.h"
#include "flow/ScopeExit.h"

struct IdleTLogQueueCleanupWorkload : TestWorkload {
	static constexpr auto NAME = "IdleTLogQueueCleanup";
	bool passed = false;
	double operationTimeout;
	std::string crashAt;
	TLogInterface target;
	int keyCount = 128;
	int valueBytes = 32768;

	explicit IdleTLogQueueCleanupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		operationTimeout = getOption(options, "operationTimeout"_sr, 180.0);
		crashAt = getOption(options, "crashAt"_sr, "none"_sr).toString();
		if (crashAt == "random") {
			const std::vector<std::string> checkpoints = { "Restored",
				                                           "BeforeRetireCommit",
				                                           "RetireCommitInFlight",
				                                           "AfterRetireCommit",
				                                           "AfterQueuePop",
				                                           "EmptyMarkerWritten",
				                                           "BeforeCompact",
				                                           "AfterCompactFileCreate",
				                                           "AfterCompactMarkerWrite",
				                                           "AfterCompactMarker",
				                                           "AfterCompactOldFileReplaced",
				                                           "AfterCompact" };
			crashAt = deterministicRandom()->randomChoice(checkpoints);
		}
	}

	~IdleTLogQueueCleanupWorkload() override {
		if (clientId == 0 && g_network->isSimulated()) {
			fdbSimulationPolicyState().tLogQueueCleanup = {};
		}
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		return clientId == 0 ? timeoutError(run(cx), operationTimeout) : Future<Void>(Void());
	}
	Future<bool> check(Database const&) override { return clientId != 0 || passed; }
	void getMetrics(std::vector<PerfMetric>&) override {}

	Future<Void> waitRecovered(Optional<LogEpoch> afterEpoch = {}) {
		while (dbInfo->get().recoveryState != RecoveryState::FULLY_RECOVERED ||
		       (afterEpoch.present() && dbInfo->get().logSystemConfig.epoch <= afterEpoch.get())) {
			co_await dbInfo->onChange();
		}
	}

	bool isCurrentTarget() const {
		for (const auto& set : dbInfo->get().logSystemConfig.tLogs) {
			for (const auto& log : set.tLogs) {
				if (log.present() && log.interf().getSharedTLogID() == target.getSharedTLogID()) {
					return true;
				}
			}
		}
		return false;
	}

	void rebootTransactionSystem() {
		NetworkAddress victim = dbInfo->get().master.address();
		if (victim == target.address()) {
			for (const auto& proxy : dbInfo->get().client.commitProxies) {
				if (proxy.address() != target.address()) {
					victim = proxy.address();
					break;
				}
			}
		}
		ASSERT(victim != target.address());
		g_simulator->rebootProcess(g_simulator->getProcessByAddress(victim), ISimulator::KillType::Reboot);
	}

	Future<int64_t> queueFileBytes() {
		auto* tester = g_simulator->getCurrentProcess();
		auto* process = g_simulator->getProcessByAddress(target.address());
		co_await g_simulator->onProcess(process);
		std::vector<Future<int64_t>> sizes;
		for (auto& [name, file] : process->machine->openFiles) {
			if (name.find("logqueue-") != std::string::npos &&
			    name.find(target.getSharedTLogID().toString()) != std::string::npos && name.ends_with(".fdq") &&
			    file.get().isReady()) {
				fdbSimulationPolicyState().tLogQueueCleanup.queueBasename = abspath(name.substr(0, name.size() - 5));
				sizes.push_back(file.get().get()->size());
			}
		}
		ASSERT(sizes.size() == 2);
		std::vector<int64_t> bytes = co_await getAll(sizes);
		co_await g_simulator->onProcess(tester);
		co_return bytes[0] + bytes[1];
	}

	Future<Void> writeData(Database cx, int round) {
		for (int i = 0; i < keyCount; ++i) {
			Transaction tr(cx);
			while (true) {
				Optional<Error> error;
				try {
					tr.set(StringRef(format("IdleTLog/%04d", i)), StringRef(std::string(valueBytes, 'a' + round)));
					co_await tr.commit();
					break;
				} catch (Error& e) {
					error = e;
				}
				co_await tr.onError(error.get());
			}
		}
	}

	Future<Void> verifyData(Database cx, int round = 1) {
		Transaction tr(cx);
		while (true) {
			Optional<Error> error;
			try {
				for (int i = 0; i < keyCount; ++i) {
					Optional<Value> value = co_await tr.get(StringRef(format("IdleTLog/%04d", i)));
					ASSERT(value.present() && value.get() == StringRef(std::string(valueBytes, 'a' + round)));
				}
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error.get());
		}
	}

	Future<Void> run(Database cx) {
		ASSERT(g_network->isSimulated());
		co_await waitRecovered();
		TraceEvent("IdleTLogQueueCleanupPhase").detail("Phase", "InitialRecovery");
		for (const auto& set : dbInfo->get().logSystemConfig.tLogs) {
			for (const auto& log : set.tLogs) {
				if (set.isLocal && log.present() && log.interf().address() != dbInfo->get().master.address()) {
					target = log.interf();
					break;
				}
			}
		}
		ASSERT(target.getSharedTLogID() != UID());
		auto& test = fdbSimulationPolicyState().tLogQueueCleanup;
		test = {};
		test.sharedTLogId = target.getSharedTLogID();
		test.pauseAt = "RetirementBegin";
		test.retainOldLogRoles = true;
		TraceEvent("IdleTLogQueueCleanupTarget", test.sharedTLogId)
		    .detail("Address", target.address())
		    .detail("CrashAt", crashAt);

		// Keep this shared queue in the next generation, then move the current log elsewhere.
		const AddressExclusion exclusion(target.address().ip, target.address().port);
		std::vector<ProcessData> workers = co_await getWorkers(cx);
		std::map<NetworkAddress, ProcessClass> originalClasses;
		for (const auto& worker : workers) {
			if (worker.address == target.address() || worker.processClass.classType() == ProcessClass::LogClass) {
				originalClasses.emplace(worker.address, worker.processClass);
			}
			if (worker.address != target.address() && worker.processClass.classType() == ProcessClass::LogClass) {
				co_await setClass(cx,
				                  AddressExclusion(worker.address.ip, worker.address.port),
				                  ProcessClass(ProcessClass::TransactionClass, ProcessClass::DBSource));
			}
		}
		co_await setClass(cx, exclusion, ProcessClass(ProcessClass::LogClass, ProcessClass::DBSource));
		UID snapshot = deterministicRandom()->randomUniqueID();
		co_await target.disablePopRequest.getReply(TLogDisablePopRequest(snapshot));
		co_await writeData(cx, 0);
		TraceEvent("IdleTLogQueueCleanupPhase").detail("Phase", "FirstGenerationWritten");
		LogEpoch firstEpoch = dbInfo->get().logSystemConfig.epoch;
		rebootTransactionSystem();
		// Keep recovery unfinished so both generations remain durable in coordinator state.
		while (dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
		       dbInfo->get().logSystemConfig.epoch <= firstEpoch) {
			co_await dbInfo->onChange();
		}
		TraceEvent("IdleTLogQueueCleanupPhase").detail("Phase", "SecondGenerationAcceptingCommits");
		ASSERT(isCurrentTarget());
		for (const auto& set : dbInfo->get().logSystemConfig.tLogs) {
			for (const auto& log : set.tLogs) {
				if (log.present() && log.interf().getSharedTLogID() == target.getSharedTLogID()) {
					target = log.interf();
				}
			}
		}
		co_await writeData(cx, 1);
		TraceEvent("IdleTLogQueueCleanupPhase").detail("Phase", "SecondGenerationWritten");
		co_await excludeServers(cx, { exclusion });
		rebootTransactionSystem();
		while (isCurrentTarget()) {
			co_await dbInfo->onChange();
		}
		co_await waitRecovered();
		TraceEvent("IdleTLogQueueCleanupPhase").detail("Phase", "TargetExcluded");
		co_await delay(0.1);
		TraceEvent("IdleTLogQueueCleanupInitialState", test.sharedTLogId)
		    .detail("LogCount", test.logCount)
		    .detail("QueueBytes", test.queueBytes)
		    .detail("HardLimitBytes", SERVER_KNOBS->TLOG_HARD_LIMIT_BYTES);
		ASSERT(test.logCount >= 2 && test.queueBytes > 2 * SERVER_KNOBS->TLOG_HARD_LIMIT_BYTES);
		const int64_t initialQueueBytes = test.queueBytes;
		ASSERT(co_await queueFileBytes() == initialQueueBytes);
		const int64_t maxQueueBytes = 2 * SERVER_KNOBS->DISK_QUEUE_FILE_EXTENSION_BYTES + 4096;

		test.crashAt = crashAt == "none" ? "" : crashAt;
		test.restores = 0;
		g_simulator->rebootProcess(g_simulator->getProcessByAddress(target.address()), ISimulator::KillType::Reboot);
		while (test.restores < (crashAt == "Restored" ? 2 : 1)) {
			co_await delay(0.1);
		}
		test.pauseAt.clear();
		const double cleanupDeadline = now() + 60.0;
		while (true) {
			if (test.retiredCount == test.logCount && test.activeCount == 0 && test.queueBytes <= maxQueueBytes &&
			    test.crashes == (crashAt == "none" ? 0 : 1)) {
				break;
			}
			if (now() > cleanupDeadline) {
				TraceEvent(SevError, "IdleTLogQueueCleanupFailed", test.sharedTLogId)
				    .detail("InitialQueueBytes", initialQueueBytes)
				    .detail("QueueBytes", test.queueBytes)
				    .detail("LogCount", test.logCount)
				    .detail("RetiredCount", test.retiredCount)
				    .detail("ActiveCount", test.activeCount)
				    .detail("Restores", test.restores)
				    .detail("Crashes", test.crashes);
				co_return;
			}
			co_await delay(0.2);
		}
		const int64_t cleanedQueueBytes = co_await queueFileBytes();
		ASSERT(cleanedQueueBytes <= maxQueueBytes);
		ASSERT(test.checkpoints["ReferenceRead"] > 0);
		if (crashAt == "BeforeRetireCommit" || crashAt == "RetireCommitInFlight" || crashAt == "AfterRetireCommit") {
			ASSERT(test.checkpoints["ReferenceReadAfterCrash"] > 0);
		}
		co_await verifyData(cx);

		const int previousRestores = test.restores;
		g_simulator->rebootProcess(g_simulator->getProcessByAddress(target.address()), ISimulator::KillType::Reboot);
		while (test.restores == previousRestores) {
			co_await delay(0.1);
		}
		co_await waitRecovered();
		co_await verifyData(cx);
		ASSERT(co_await queueFileBytes() <= maxQueueBytes);
		co_await includeServers(cx, { exclusion });
		for (const auto& [address, processClass] : originalClasses) {
			ProcessClass restore = processClass.classSource() == ProcessClass::CommandLineSource
			                           ? ProcessClass(ProcessClass::InvalidClass, ProcessClass::DBSource)
			                           : processClass;
			co_await setClass(cx, AddressExclusion(address.ip, address.port), restore);
		}
		while (true) {
			auto availableWorkers = co_await getWorkers(dbInfo, GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY);
			std::map<NetworkAddress, ProcessClass> observedClasses;
			for (const auto& worker : availableWorkers) {
				observedClasses.emplace(worker.interf.address(), worker.processClass);
			}
			bool restored = true;
			for (const auto& [address, processClass] : originalClasses) {
				auto observed = observedClasses.find(address);
				if (observed == observedClasses.end() || observed->second != processClass) {
					restored = false;
				}
			}
			if (restored) {
				break;
			}
			co_await delay(0.1);
		}
		co_await waitRecovered();
		LogEpoch previousEpoch = dbInfo->get().logSystemConfig.epoch;
		UID previousMaster = dbInfo->get().master.id();
		// Change retention only after the old master's final registrations become obsolete.
		fdbSimulationPolicyState().disableTLogRecoveryFinish = true;
		ScopeExit unpauseRecovery([]() { fdbSimulationPolicyState().disableTLogRecoveryFinish = false; });
		rebootTransactionSystem();
		while (dbInfo->get().master.id() == previousMaster ||
		       dbInfo->get().recoveryState == RecoveryState::FULLY_RECOVERED) {
			co_await dbInfo->onChange();
		}
		test.retainOldLogRoles = false;
		fdbSimulationPolicyState().disableTLogRecoveryFinish = false;
		co_await waitRecovered(Optional<LogEpoch>(previousEpoch));
		co_await verifyData(cx);
		// Placement may choose other logs after the original process classes are restored.
		const bool reusedSharedQueue = isCurrentTarget();
		bool recruitedProcess = false;
		for (const auto& set : dbInfo->get().logSystemConfig.tLogs) {
			for (const auto& log : set.tLogs) {
				if (log.present() && log.interf().address() == target.address()) {
					recruitedProcess = true;
				}
			}
		}
		co_await writeData(cx, 2);
		co_await verifyData(cx, 2);
		TraceEvent("IdleTLogQueueCleanupComplete", test.sharedTLogId)
		    .detail("InitialQueueBytes", initialQueueBytes)
		    .detail("PhysicalQueueBytesAfterCleanup", cleanedQueueBytes)
		    .detail("ReusedSharedQueue", reusedSharedQueue)
		    .detail("RecruitedProcess", recruitedProcess)
		    .detail("CrashAt", crashAt)
		    .detail("ReferenceReads", test.checkpoints["ReferenceRead"])
		    .detail("ReferenceReadsAfterCrash", test.checkpoints["ReferenceReadAfterCrash"])
		    .detail("Crashes", test.crashes);
		passed = true;
	}
};

WorkloadFactory<IdleTLogQueueCleanupWorkload> IdleTLogQueueCleanupWorkloadFactory;
