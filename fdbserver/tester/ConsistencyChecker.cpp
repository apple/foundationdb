/*
 * ConsistencyChecker.cpp
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
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "flow/DeterministicRandom.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/MoveKeys.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "ConsistencyChecker.h"
#include "fdbserver/tester/workloads.h"

Future<Void> auditStorageCorrectness(Reference<AsyncVar<ServerDBInfo>> dbInfo, AuditType auditType) {
	if (SERVER_KNOBS->DISABLE_AUDIT_STORAGE_FINAL_REPLICA_CHECK_IN_SIM &&
	    (auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica)) {
		co_return;
	}
	TraceEvent(SevDebug, "AuditStorageCorrectnessBegin").detail("AuditType", auditType);
	Database cx;
	UID auditId;
	AuditStorageState auditState;

	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			while (dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
			       !dbInfo->get().distributor.present()) {
				co_await dbInfo->onChange();
			}
			TriggerAuditRequest req(auditType, allKeys, KeyValueStoreType::END); // do not specify engine type to check
			UID auditId_ = co_await timeoutError(dbInfo->get().distributor.get().triggerAudit.getReply(req), 300);
			auditId = auditId_;
			TraceEvent(SevDebug, "AuditStorageCorrectnessTriggered")
			    .detail("AuditID", auditId)
			    .detail("AuditType", auditType);
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "AuditStorageCorrectnessTriggerError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId)
			    .detail("AuditType", auditType);
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await delay(1);
		}
	}

	int retryCount = 0;
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			cx = openDBOnServer(dbInfo);
			AuditStorageState auditState_ = co_await getAuditState(cx, auditType, auditId);
			auditState = auditState_;
			if (auditState.getPhase() == AuditPhase::Complete) {
				break;
			} else if (auditState.getPhase() == AuditPhase::Running) {
				TraceEvent("AuditStorageCorrectnessWait")
				    .detail("AuditID", auditId)
				    .detail("AuditType", auditType)
				    .detail("RetryCount", retryCount);
				co_await delay(25);
				if (retryCount > 20) {
					TraceEvent("AuditStorageCorrectnessWaitFailed")
					    .detail("AuditID", auditId)
					    .detail("AuditType", auditType);
					break;
				}
				retryCount++;
				continue;
			} else if (auditState.getPhase() == AuditPhase::Error) {
				break;
			} else if (auditState.getPhase() == AuditPhase::Failed) {
				break;
			} else {
				UNREACHABLE();
			}
		} catch (Error& e) {
			TraceEvent("AuditStorageCorrectnessWaitError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId)
			    .detail("AuditType", auditType)
			    .detail("AuditState", auditState.toString());
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await delay(1);
		}
	}
	TraceEvent("AuditStorageCorrectnessWaitEnd")
	    .detail("AuditID", auditId)
	    .detail("AuditType", auditType)
	    .detail("AuditState", auditState.toString());

	co_return;
}

// Runs the consistency check workload, which verifies that the database is in a consistent state
Future<Void> checkConsistency(Database cx,
                              std::vector<TesterInterface> testers,
                              bool doQuiescentCheck,
                              bool doTSSCheck,
                              double maxDDRunTime,
                              double softTimeLimit,
                              double databasePingDelay,
                              Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	TestSpec spec;

	double connectionFailures;
	if (g_network->isSimulated()) {
		// NOTE: the value will be reset after consistency check
		connectionFailures = g_simulator->connectionFailuresDisableDuration;
		disableConnectionFailures("ConsistencyCheck");
		fdbSimulationPolicyState().isConsistencyChecked = true;
	}

	Standalone<VectorRef<KeyValueRef>> options;
	StringRef performQuiescent = "false"_sr;
	StringRef performTSSCheck = "false"_sr;
	if (doQuiescentCheck) {
		performQuiescent = "true"_sr;
		spec.restorePerpetualWiggleSetting = false;
	}
	if (doTSSCheck) {
		performTSSCheck = "true"_sr;
	}
	spec.title = "ConsistencyCheck"_sr;
	spec.databasePingDelay = databasePingDelay;
	spec.runFailureWorkloads = false;
	spec.timeout = 32000;
	options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ConsistencyCheck"_sr));
	options.push_back_deep(options.arena(), KeyValueRef("performQuiescentChecks"_sr, performQuiescent));
	options.push_back_deep(options.arena(), KeyValueRef("performTSSCheck"_sr, performTSSCheck));
	options.push_back_deep(options.arena(),
	                       KeyValueRef("maxDDRunTime"_sr, ValueRef(options.arena(), format("%f", maxDDRunTime))));
	options.push_back_deep(options.arena(), KeyValueRef("distributed"_sr, "false"_sr));
	spec.options.push_back_deep(spec.options.arena(), options);

	double start = now();
	bool lastRun = false;
	while (true) {
		DistributedTestResults testResults = co_await runWorkload(cx, testers, spec);
		if (testResults.ok() || lastRun) {
			if (g_network->isSimulated()) {
				g_simulator->connectionFailuresDisableDuration = connectionFailures;
				fdbSimulationPolicyState().isConsistencyChecked = false;
			}
			co_return;
		}
		if (now() - start > softTimeLimit) {
			spec.options[0].push_back_deep(spec.options.arena(), KeyValueRef("failureIsError"_sr, "true"_sr));
			lastRun = true;
		}

		co_await repairDeadDatacenter(cx, dbInfo, "ConsistencyCheck");
	}
}

namespace {

Future<std::unordered_set<int>> runUrgentConsistencyCheckWorkload(
    Database cx,
    std::vector<TesterInterface> testers,
    int64_t consistencyCheckerId,
    std::unordered_map<int, std::vector<KeyRange>> assignment) {
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_DispatchWorkloads")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);

	// Step 1: Get interfaces for running workloads
	std::vector<Future<ErrorOr<WorkloadInterface>>> workRequests;
	Standalone<VectorRef<KeyValueRef>> option;
	option.push_back_deep(option.arena(), KeyValueRef("testName"_sr, "ConsistencyCheckUrgent"_sr));
	Standalone<VectorRef<VectorRef<KeyValueRef>>> options;
	options.push_back_deep(options.arena(), option);
	for (int i = 0; i < testers.size(); i++) {
		WorkloadRequest req;
		req.title = "ConsistencyCheckUrgent"_sr;
		req.useDatabase = true;
		req.timeout = 0.0; // disable timeout workload
		req.databasePingDelay = 0.0; // disable databased ping check
		req.options = options;
		req.clientId = i;
		req.clientCount = testers.size();
		req.sharedRandomNumber = consistencyCheckerId;
		req.rangesToCheck = assignment[i];
		workRequests.push_back(testers[i].recruitments.getReplyUnlessFailedFor(req, 10, 0));
		// workRequests follows the order of clientId of assignment
	}
	co_await waitForAll(workRequests);

	// Step 2: Run workloads via the interfaces
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_TriggerWorkloads")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);
	std::unordered_set<int> completeClientIds;
	std::vector<int> clientIds; // record the clientId for jobs
	std::vector<Future<ErrorOr<Void>>> jobs;
	for (int i = 0; i < workRequests.size(); i++) {
		ASSERT(workRequests[i].isReady());
		if (workRequests[i].get().isError()) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_FailedToContactTester")
			    .error(workRequests[i].get().getError())
			    .detail("TesterCount", testers.size())
			    .detail("TesterId", i)
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
		} else {
			jobs.push_back(workRequests[i].get().get().start.template getReplyUnlessFailedFor<Void>(10, 0));
			clientIds.push_back(i);
		}
	}
	co_await waitForAll(jobs);
	for (int i = 0; i < jobs.size(); i++) {
		if (jobs[i].isError()) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RunWorkloadError1")
			    .errorUnsuppressed(jobs[i].getError())
			    .detail("ClientId", clientIds[i])
			    .detail("ClientCount", testers.size())
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
		} else if (jobs[i].get().isError()) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RunWorkloadError2")
			    .errorUnsuppressed(jobs[i].get().getError())
			    .detail("ClientId", clientIds[i])
			    .detail("ClientCount", testers.size())
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
		} else {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RunWorkloadComplete")
			    .detail("ClientId", clientIds[i])
			    .detail("ClientCount", testers.size())
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
			completeClientIds.insert(clientIds[i]); // Add complete clients
		}
	}

	TraceEvent(SevInfo, "ConsistencyCheckUrgent_DispatchWorkloadEnd")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);

	co_return completeClientIds;
}

Future<std::vector<KeyRange>> getConsistencyCheckShards(Database cx, std::vector<KeyRange> ranges) {
	// Get the scope of the input list of ranges
	Key beginKeyToReadKeyServer;
	Key endKeyToReadKeyServer;
	for (int i = 0; i < ranges.size(); i++) {
		if (i == 0 || ranges[i].begin < beginKeyToReadKeyServer) {
			beginKeyToReadKeyServer = ranges[i].begin;
		}
		if (i == 0 || ranges[i].end > endKeyToReadKeyServer) {
			endKeyToReadKeyServer = ranges[i].end;
		}
	}
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_GetConsistencyCheckShards")
	    .detail("RangeBegin", beginKeyToReadKeyServer)
	    .detail("RangeEnd", endKeyToReadKeyServer);
	// Read KeyServer space within the scope and add shards intersecting with the input ranges
	std::vector<KeyRange> res;
	Transaction tr(cx);
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			KeyRange rangeToRead = Standalone(KeyRangeRef(beginKeyToReadKeyServer, endKeyToReadKeyServer));
			RangeResult readResult = co_await krmGetRanges(&tr,
			                                               keyServersPrefix,
			                                               rangeToRead,
			                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
			                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES);
			for (int i = 0; i < readResult.size() - 1; ++i) {
				KeyRange rangeToCheck = Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key));
				Value valueToCheck = Standalone(readResult[i].value);
				bool toAdd = false;
				for (const auto& range : ranges) {
					if (rangeToCheck.intersects(range) == true) {
						toAdd = true;
						break;
					}
				}
				if (toAdd == true) {
					res.push_back(rangeToCheck);
				}
				beginKeyToReadKeyServer = readResult[i + 1].key;
			}
			if (beginKeyToReadKeyServer >= endKeyToReadKeyServer) {
				break;
			}
		} catch (Error& e) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_GetConsistencyCheckShardsRetry").error(e);
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await tr.onError(caughtError);
		}
	}
	co_return res;
}

Future<std::vector<TesterInterface>> getTesters(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                                int minTestersExpected) {
	// Recruit workers
	int flags = GetWorkersRequest::TESTER_CLASS_ONLY | GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY;
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
			TraceEvent(SevWarnAlways, "TesterRecruitmentTimeout");
			throw timed_out();
		}
		// action == 2: cc changed, loop again
	}
	std::vector<TesterInterface> ts;
	ts.reserve(workers.size());
	for (int i = 0; i < workers.size(); i++)
		ts.push_back(workers[i].interf.testerInterface);
	deterministicRandom()->randomShuffle(ts);
	co_return ts;
}

const std::unordered_map<char, uint8_t> parseCharMap{
	{ '0', 0 },  { '1', 1 },  { '2', 2 },  { '3', 3 },  { '4', 4 },  { '5', 5 },  { '6', 6 },  { '7', 7 },
	{ '8', 8 },  { '9', 9 },  { 'a', 10 }, { 'b', 11 }, { 'c', 12 }, { 'd', 13 }, { 'e', 14 }, { 'f', 15 },
	{ 'A', 10 }, { 'B', 11 }, { 'C', 12 }, { 'D', 13 }, { 'E', 14 }, { 'F', 15 },
};

Optional<Key> getKeyFromString(const std::string& str) {
	Key emptyKey;
	if (str.empty()) {
		return emptyKey;
	}
	if (str.size() % 4 != 0) {
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "ConsistencyCheckUrgent_GetKeyFromStringError")
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("Reason", "WrongLength")
		    .detail("InputStr", str);
		return Optional<Key>();
	}
	std::vector<uint8_t> byteList;
	for (int i = 0; i < str.size(); i += 4) {
		if (str.at(i + 0) != '\\' || str.at(i + 1) != 'x') {
			TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
			           "ConsistencyCheckUrgent_GetKeyFromStringError")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Reason", "WrongBytePrefix")
			    .detail("InputStr", str);
			return Optional<Key>();
		}
		const char first = str.at(i + 2);
		const char second = str.at(i + 3);
		if (!parseCharMap.contains(first) || !parseCharMap.contains(second)) {
			TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
			           "ConsistencyCheckUrgent_GetKeyFromStringError")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Reason", "WrongByteContent")
			    .detail("InputStr", str);
			return Optional<Key>();
		}
		uint8_t parsedValue = parseCharMap.at(first) * 16 + parseCharMap.at(second);
		byteList.push_back(parsedValue);
	}
	return Standalone(StringRef(byteList.data(), byteList.size()));
}

Optional<std::vector<KeyRange>> loadRangesToCheckFromKnob() {
	// Load string from knob
	std::vector<std::string> beginKeyStrs = {
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_0,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_1,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_2,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_3,
	};
	std::vector<std::string> endKeyStrs = {
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_0,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_1,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_2,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_3,
	};

	// Get keys from strings
	std::vector<Key> beginKeys;
	for (const auto& beginKeyStr : beginKeyStrs) {
		Optional<Key> key = getKeyFromString(beginKeyStr);
		if (key.present()) {
			beginKeys.push_back(key.get());
		} else {
			return Optional<std::vector<KeyRange>>();
		}
	}
	std::vector<Key> endKeys;
	for (const auto& endKeyStr : endKeyStrs) {
		Optional<Key> key = getKeyFromString(endKeyStr);
		if (key.present()) {
			endKeys.push_back(key.get());
		} else {
			return Optional<std::vector<KeyRange>>();
		}
	}
	if (beginKeys.size() != endKeys.size()) {
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "ConsistencyCheckUrgent_GetKeyFromStringError")
		    .detail("Reason", "MismatchBeginKeysAndEndKeys");
		return Optional<std::vector<KeyRange>>();
	}

	// Get ranges
	KeyRangeMap<bool> rangeToCheckMap;
	for (int i = 0; i < beginKeys.size(); i++) {
		Key rangeBegin = beginKeys[i];
		Key rangeEnd = endKeys[i];
		if (rangeBegin.empty() && rangeEnd.empty()) {
			continue;
		}
		if (rangeBegin > allKeys.end) {
			rangeBegin = allKeys.end;
		}
		if (rangeEnd > allKeys.end) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_ReverseInputRange")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Index", i)
			    .detail("RangeBegin", rangeBegin)
			    .detail("RangeEnd", rangeEnd);
			rangeEnd = allKeys.end;
		}

		KeyRange rangeToCheck;
		if (rangeBegin < rangeEnd) {
			rangeToCheck = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		} else if (rangeBegin > rangeEnd) {
			rangeToCheck = Standalone(KeyRangeRef(rangeEnd, rangeBegin));
		} else {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_EmptyInputRange")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Index", i)
			    .detail("RangeBegin", rangeBegin)
			    .detail("RangeEnd", rangeEnd);
			continue;
		}
		rangeToCheckMap.insert(rangeToCheck, true);
	}

	rangeToCheckMap.coalesce(allKeys);

	std::vector<KeyRange> res;
	for (auto rangeToCheck : rangeToCheckMap.ranges()) {
		if (rangeToCheck.value() == true) {
			res.push_back(rangeToCheck.range());
		}
	}
	TraceEvent e(SevInfo, "ConsistencyCheckUrgent_LoadedInputRange");
	e.setMaxEventLength(-1);
	e.setMaxFieldLength(-1);
	for (int i = 0; i < res.size(); i++) {
		e.detail("RangeBegin" + std::to_string(i), res[i].begin);
		e.detail("RangeEnd" + std::to_string(i), res[i].end);
	}
	return res;
}

std::unordered_map<int, std::vector<KeyRange>> makeTaskAssignment(Database cx,
                                                                  int64_t consistencyCheckerId,
                                                                  std::vector<KeyRange> shardsToCheck,
                                                                  int testersCount,
                                                                  int round) {
	ASSERT(testersCount >= 1);
	std::unordered_map<int, std::vector<KeyRange>> assignment;

	std::vector<size_t> shuffledIndices(testersCount);
	std::iota(shuffledIndices.begin(), shuffledIndices.end(), 0); // creates [0, 1, ..., testersCount - 1]
	deterministicRandom()->randomShuffle(shuffledIndices);

	int batchSize = CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_BATCH_SHARD_COUNT;
	int startingPoint = 0;
	if (shardsToCheck.size() > batchSize * testersCount) {
		startingPoint = deterministicRandom()->randomInt(0, shardsToCheck.size() - batchSize * testersCount);
		// We randomly pick a set of successive shards:
		// (1) We want to retry for different shards to avoid repeated failure on the same shards
		// (2) We want to check successive shards to avoid inefficiency incurred by fragments
	}
	assignment.clear();
	for (int i = startingPoint; i < shardsToCheck.size(); i++) {
		int testerIdx = (i - startingPoint) / batchSize;
		if (testerIdx > testersCount - 1) {
			break; // Have filled up all testers
		}
		// When assigning a shards/batch to a tester idx, there are certain edge cases which can result in urgent
		// consistency checker being infinetely stuck in a loop. Examples:
		//      1. if there is 1 remaining shard, and tester 0 consistently fails, we will still always pick tester 0
		//      2. if there are 10 remaining shards, and batch size is 10, and tester 0 consistently fails, we will
		//      still always pick tester 0
		//      3. if there are 20 remaining shards, and batch size is 10, and testers {0, 1} consistently fail, we will
		//      keep picking testers {0, 1}
		// To avoid repeatedly picking the same testers even though they could be failing, shuffledIndices provides an
		// indirection to a random tester idx. That way, each invocation of makeTaskAssignment won't
		// result in the same task assignment for the class of edge cases mentioned above.
		assignment[shuffledIndices[testerIdx]].push_back(shardsToCheck[i]);
	}
	std::unordered_map<int, std::vector<KeyRange>>::iterator assignIt;
	for (assignIt = assignment.begin(); assignIt != assignment.end(); assignIt++) {
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_AssignTaskToTesters")
		    .detail("ConsistencyCheckerId", consistencyCheckerId)
		    .detail("Round", round)
		    .detail("ClientId", assignIt->first)
		    .detail("ShardsCount", assignIt->second.size());
	}
	return assignment;
}

Future<Void> runConsistencyCheckerUrgentCore(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                             Database cx,
                                             Optional<std::vector<TesterInterface>> testers,
                                             int minTestersExpected) {
	KeyRangeMap<bool> globalProgressMap; // used to keep track of progress
	std::unordered_map<int, std::vector<KeyRange>> assignment; // used to keep track of assignment of tasks
	std::vector<TesterInterface> ts; // used to store testers interface
	std::vector<KeyRange> rangesToCheck; // get from globalProgressMap
	std::vector<KeyRange> shardsToCheck; // get from keyServer metadata
	Optional<double> whenFailedToGetTesterStart;

	// Initialize globalProgressMap
	Optional<std::vector<KeyRange>> rangesToCheck_ = loadRangesToCheckFromKnob();
	if (rangesToCheck_.present()) {
		globalProgressMap.insert(allKeys, true);
		for (const auto& rangeToCheck : rangesToCheck_.get()) {
			// Mark rangesToCheck as incomplete
			// Those ranges will be checked
			globalProgressMap.insert(rangeToCheck, false);
		}
		globalProgressMap.coalesce(allKeys);
	} else {
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_FailedToLoadRangeFromKnob");
		globalProgressMap.insert(allKeys, false);
	}

	int64_t consistencyCheckerId = deterministicRandom()->randomInt64(1, 10000000);
	int retryTimes = 0;
	int round = 0;

	// Main loop
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			// Step 1: Load ranges to check, if nothing to run, exit
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RoundBegin")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("RetryTimes", retryTimes)
			    .detail("TesterCount", ts.size())
			    .detail("Round", round);
			rangesToCheck.clear();
			for (auto& range : globalProgressMap.ranges()) {
				if (!range.value()) { // range that is not finished
					rangesToCheck.push_back(range.range());
				}
			}
			if (rangesToCheck.empty()) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_Complete")
				    .detail("ConsistencyCheckerId", consistencyCheckerId)
				    .detail("RetryTimes", retryTimes)
				    .detail("Round", round);
				co_return;
			}

			// Step 2: Get testers
			ts.clear();
			if (!testers.present()) { // In real clusters
				bool getTesterError = false;
				Error getTesterErr;

				try {
					ts = co_await getTesters(cc, minTestersExpected);
					whenFailedToGetTesterStart.reset();
				} catch (Error& e) {
					if (e.code() == error_code_timed_out) {
						if (!whenFailedToGetTesterStart.present()) {
							whenFailedToGetTesterStart = now();
						} else if (now() - whenFailedToGetTesterStart.get() > 3600 * 24) { // 1 day
							TraceEvent(SevError, "TesterRecruitmentTimeout");
						}
					}
					getTesterErr = e;
					getTesterError = true;
				}

				if (getTesterError) {
					throw getTesterErr;
				}

				if (g_network->isSimulated() && deterministicRandom()->random01() < 0.05) {
					throw operation_failed(); // Introduce random failure
				}
			} else { // In simulation
				ts = testers.get();
			}
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_GotTesters")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("Round", round)
			    .detail("RetryTimes", retryTimes)
			    .detail("TesterCount", ts.size());

			// Step 3: Load shards to check from keyserver space
			// Shard is the unit for the task assignment
			shardsToCheck.clear();
			shardsToCheck = co_await getConsistencyCheckShards(cx, rangesToCheck);
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_GotShardsToCheck")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("Round", round)
			    .detail("RetryTimes", retryTimes)
			    .detail("TesterCount", ts.size())
			    .detail("ShardCount", shardsToCheck.size());

			// Step 4: Assign tasks to clients
			assignment.clear();
			assignment = makeTaskAssignment(cx, consistencyCheckerId, shardsToCheck, ts.size(), round);

			// Step 5: Run checking on testers
			std::unordered_set<int> completeClients =
			    co_await runUrgentConsistencyCheckWorkload(cx, ts, consistencyCheckerId, assignment);
			if (g_network->isSimulated() && deterministicRandom()->random01() < 0.05) {
				throw operation_failed(); // Introduce random failure
			}
			// We use the complete client to decide which ranges are completed
			for (const auto& clientId : completeClients) {
				for (const auto& range : assignment[clientId]) {
					globalProgressMap.insert(range, true); // Mark the ranges as complete
				}
			}
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RoundEnd")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("RetryTimes", retryTimes)
			    .detail("SucceedTesterCount", completeClients.size())
			    .detail("SucceedTesters", describe(completeClients))
			    .detail("TesterCount", ts.size())
			    .detail("Round", round);
			round++;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			} else {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_CoreWithRetriableFailure")
				    .errorUnsuppressed(e)
				    .detail("ConsistencyCheckerId", consistencyCheckerId)
				    .detail("RetryTimes", retryTimes)
				    .detail("Round", round);
				caughtError = e;
				needsErrorHandling = true;
			}
		}

		if (needsErrorHandling) {
			co_await delay(10.0);
			retryTimes++;
		} else {
			co_await delay(10.0); // Backoff 10 seconds for the next round
		}

		// Decide and enforce the consistencyCheckerId for the next round
		consistencyCheckerId = deterministicRandom()->randomInt64(1, 10000000);
	}
}

} // namespace

Future<Void> runConsistencyCheckerUrgentHolder(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                               Database cx,
                                               Optional<std::vector<TesterInterface>> testers,
                                               int minTestersExpected,
                                               bool repeatRun) {
	while (true) {
		co_await runConsistencyCheckerUrgentCore(cc, cx, testers, minTestersExpected);
		if (!repeatRun) {
			break;
		}
		co_await delay(CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_NEXT_WAIT_TIME);
	}
	co_return;
}

Future<Void> checkConsistencyUrgentSim(Database cx, std::vector<TesterInterface> testers) {
	if (SERVER_KNOBS->DISABLE_AUDIT_STORAGE_FINAL_REPLICA_CHECK_IN_SIM) {
		return Void();
	}
	return runConsistencyCheckerUrgentHolder(
	    Reference<AsyncVar<Optional<ClusterControllerFullInterface>>>(), cx, testers, 1, /*repeatRun=*/false);
}
