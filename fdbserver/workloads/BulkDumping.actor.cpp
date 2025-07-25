/*
 * BulkDumping.actor.cpp
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

#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/MockS3Server.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const std::string simulationBulkDumpFolder = joinPath("simfdb", "bulkdump");

struct BulkDumping : TestWorkload {
	static constexpr auto NAME = "BulkDumpingWorkload";
	const bool enabled = true;
	bool pass = true;
	int cancelTimes = 0;
	int maxCancelTimes = 0;
	int bulkLoadTransportMethod = 1; // Default to CP method
	std::string jobRoot = "";

	// This workload is not compatible with following workload because they will race in changing the DD mode
	// This workload is not compatible with RandomRangeLock for the conflict in range lock
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		out.insert({ "RandomMoveKeys",
		             "DataLossRecovery",
		             "IDDTxnProcessorApiCorrectness",
		             "PerpetualWiggleStatsWorkload",
		             "PhysicalShardMove",
		             "StorageCorruption",
		             "StorageServerCheckpointRestoreTest",
		             "ValidateStorage",
		             "RandomRangeLock",
		             "BulkLoading" });
	}

	BulkDumping(WorkloadContext const& wcx)
	  : TestWorkload(wcx), enabled(true), pass(true), cancelTimes(0),
	    maxCancelTimes(getOption(options, "maxCancelTimes"_sr, deterministicRandom()->randomInt(0, 2))),
	    bulkLoadTransportMethod(getOption(options, "bulkLoadTransportMethod"_sr, 1)),
	    jobRoot(getOption(options, "jobRoot"_sr, ""_sr).toString()) {}

	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Standalone<StringRef> getRandomStringRef() const {
		int stringLength = deterministicRandom()->randomInt(1, 10);
		Standalone<StringRef> stringBuffer = makeString(stringLength);
		deterministicRandom()->randomBytes(mutateString(stringBuffer), stringLength);
		return stringBuffer;
	}

	KeyRange getRandomRange(BulkDumping* self, KeyRange scope) const {
		loop {
			Standalone<StringRef> keyA = self->getRandomStringRef();
			Standalone<StringRef> keyB = self->getRandomStringRef();
			if (!scope.contains(keyA) || !scope.contains(keyB)) {
				continue;
			}
			KeyRange range = keyA < keyB ? KeyRangeRef(keyA, keyB) : KeyRangeRef(keyB, keyA);
			if (range.empty() || range.singleKeyRange()) {
				continue;
			}
			return range;
		}
	}

	std::map<Key, Value> generateOrderedKVS(BulkDumping* self, KeyRange range, size_t count) {
		std::map<Key, Value> kvs; // ordered
		while (kvs.size() < count) {
			Standalone<StringRef> str = self->getRandomStringRef();
			Key key = range.begin.withSuffix(str);
			Value val = self->getRandomStringRef();
			if (!range.contains(key)) {
				continue;
			}
			auto res = kvs.insert({ key, val });
			if (!res.second) {
				continue;
			}
		}
		return kvs; // ordered
	}

	ACTOR Future<Void> setKeys(Database cx, std::map<Key, Value> kvs) {
		state Transaction tr(cx);
		loop {
			try {
				for (const auto& [key, value] : kvs) {
					tr.set(key, value);
				}
				wait(tr.commit());
				TraceEvent("BulkDumpingWorkLoadSetKey")
				    .detail("KeyCount", kvs.size())
				    .detail("Version", tr.getCommittedVersion());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> waitUntilDumpJobComplete(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<BulkDumpState> aliveJob = wait(getSubmittedBulkDumpJob(&tr));
				if (!aliveJob.present()) {
					break;
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				wait(tr.onError(e));
			}
			wait(delay(30.0));
		}
		return Void();
	}

	ACTOR Future<Void> clearDatabase(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(normalKeys);
				tr.clear(bulkDumpKeys);
				tr.clear(bulkLoadJobKeys);
				tr.clear(bulkLoadTaskKeys);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	// Return error tasks
	ACTOR Future<std::vector<BulkLoadTaskState>> validateAllBulkLoadTaskAcknowledgedOrError(Database cx,
	                                                                                        UID jobId,
	                                                                                        KeyRange jobRange) {
		state RangeResult rangeResult;
		state Key beginKey = jobRange.begin;
		state Key endKey = jobRange.end;
		state Transaction tr(cx);
		state std::vector<BulkLoadTaskState> errorTasks;
		TraceEvent("BulkDumpingWorkLoad")
		    .detail("Phase", "ValidateAllBulkLoadTaskAcknowledgedOrError")
		    .detail("Job", jobId.toString())
		    .detail("Range", jobRange);
		while (beginKey < endKey) {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				rangeResult.clear();
				wait(store(rangeResult, krmGetRanges(&tr, bulkLoadTaskPrefix, KeyRangeRef(beginKey, endKey))));
				for (int i = 0; i < rangeResult.size() - 1; ++i) {
					ASSERT(!rangeResult[i].value.empty());
					BulkLoadTaskState bulkLoadTaskState = decodeBulkLoadTaskState(rangeResult[i].value);
					if (!bulkLoadTaskState.isValid()) {
						continue; // Has been cleared by engine
					}
					if (bulkLoadTaskState.getJobId() != jobId) {
						throw bulkload_task_outdated();
					}
					if (bulkLoadTaskState.phase == BulkLoadPhase::Error) {
						TraceEvent(SevWarnAlways, "BulkDumpingWorkLoadBulkLoadTaskHasError")
						    .setMaxEventLength(-1)
						    .setMaxFieldLength(-1)
						    .detail("Task", bulkLoadTaskState.toString());
						errorTasks.push_back(bulkLoadTaskState);
					}
					if (bulkLoadTaskState.phase != BulkLoadPhase::Acknowledged &&
					    bulkLoadTaskState.phase != BulkLoadPhase::Error) {
						TraceEvent(SevError, "BulkDumpingWorkLoadBulkLoadTaskWrongPhase")
						    .setMaxEventLength(-1)
						    .setMaxFieldLength(-1)
						    .detail("Task", bulkLoadTaskState.toString());
						ASSERT(false);
					}
				}
				beginKey = rangeResult.back().key;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return errorTasks;
	}

	ACTOR Future<std::vector<BulkLoadTaskState>> waitUntilLoadJobCompleteOrError(BulkDumping* self,
	                                                                             Database cx,
	                                                                             UID jobId,
	                                                                             KeyRange jobRange) {
		loop {
			Optional<BulkLoadJobState> runningJob = wait(getRunningBulkLoadJob(cx));
			if (runningJob.present()) {
				ASSERT(runningJob.get().getJobId() == jobId);
				// During the wait for the job completion, we may inject the job cancellation.
				// We varies the timing of the job cancellation, we trigger the job cancellation with 10% probability at
				// each time. Throughout the entire test, we inject the job cancellation at most maxCancelTimes times to
				// ensure the job can complete fast.
				if (SERVER_KNOBS->BULKLOAD_SIM_FAILURE_INJECTION && self->cancelTimes < self->maxCancelTimes &&
				    deterministicRandom()->random01() < 0.1) {
					wait(cancelBulkLoadJob(cx, jobId));
					self->cancelTimes++; // Inject cancellation. Then the bulkload job should run again.
					TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Job Cancelled").detail("Job", jobId.toString());
					return std::vector<BulkLoadTaskState>();
				}
				wait(delay(10.0));
				continue;
			}
			std::vector<BulkLoadTaskState> errorTasks =
			    wait(self->validateAllBulkLoadTaskAcknowledgedOrError(cx, jobId, jobRange));
			return errorTasks;
		}
	}

	ACTOR Future<Void> validateBulkLoadJobHistory(Database cx, UID jobId, bool hasError) {
		std::vector<BulkLoadJobState> jobHistory = wait(getBulkLoadJobFromHistory(cx));
		Optional<BulkLoadJobState> jobInHistory;
		for (const auto& job : jobHistory) {
			ASSERT(job.isValid());
			ASSERT(job.getJobId() == jobId);
			ASSERT(!jobInHistory.present());
			jobInHistory = job;
		}
		ASSERT(jobInHistory.present());
		if (hasError) {
			ASSERT(jobInHistory.get().getPhase() == BulkLoadJobPhase::Error);
		} else {
			ASSERT(jobInHistory.get().getPhase() == BulkLoadJobPhase::Complete);
		}
		return Void();
	}

	ACTOR Future<std::map<Key, Value>> getAllKVSFromDB(Database cx) {
		state Transaction tr(cx);
		state std::map<Key, Value> kvs;
		loop {
			try {
				RangeResult kvsRes = wait(tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!kvsRes.more);
				kvs.clear();
				for (auto& kv : kvsRes) {
					auto res = kvs.insert({ kv.key, kv.value });
					ASSERT(res.second);
				}
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return kvs;
	}

	bool keyContainedInRanges(const Key& key, const std::vector<KeyRange>& ranges) {
		for (const auto& range : ranges) {
			if (range.contains(key)) {
				return true;
			}
		}
		return false;
	}

	bool checkSame(BulkDumping* self,
	               std::map<Key, Value> kvs,
	               std::map<Key, Value> newKvs,
	               std::vector<KeyRange> ignoreRanges) {
		std::vector<KeyValue> kvsToCheck;
		std::vector<KeyValue> newKvsToCheck;
		for (const auto& [key, value] : kvs) {
			if (self->keyContainedInRanges(key, ignoreRanges)) {
				continue;
			}
			kvsToCheck.push_back(KeyValueRef(key, value));
		}
		for (const auto& [key, value] : newKvs) {
			if (self->keyContainedInRanges(key, ignoreRanges)) {
				continue;
			}
			newKvsToCheck.push_back(KeyValueRef(key, value));
		}
		return kvsToCheck == newKvsToCheck;
	}

	ACTOR Future<Void> _start(BulkDumping* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}
		if (g_network->isSimulated()) {
			// Network partition between CC and DD can cause DD no longer existing,
			// which results in the bulk loading task cannot complete
			// So, this workload disable the network partition
			disableConnectionFailures("BulkDumping");
		}
		state std::map<Key, Value> kvs = self->generateOrderedKVS(self, normalKeys, 1000);
		wait(self->setKeys(cx, kvs));

		// BulkLoad uses range lock
		wait(registerRangeLockOwner(cx, rangeLockNameForBulkLoad, rangeLockNameForBulkLoad));

		std::vector<RangeLockOwner> lockOwners = wait(getAllRangeLockOwners(cx));
		ASSERT(lockOwners.size() == 1 && lockOwners[0].getOwnerUniqueId() == rangeLockNameForBulkLoad);

		// Submit a bulk dump job
		state int oldBulkDumpMode = 0;
		wait(store(oldBulkDumpMode, setBulkDumpMode(cx, 1))); // Enable bulkDump
		state std::string dumpFolder = self->jobRoot.empty() ? simulationBulkDumpFolder : self->jobRoot;
		state BulkDumpState newJob =
		    createBulkDumpJob(normalKeys,
		                      dumpFolder,
		                      BulkLoadType::SST,
		                      static_cast<BulkLoadTransportMethod>(self->bulkLoadTransportMethod));
		wait(submitBulkDumpJob(cx, newJob));
		TraceEvent("BulkDumpingWorkLoad")
		    .detail("Phase", "Dump Job Submitted")
		    .detail("TransportMethod", self->bulkLoadTransportMethod)
		    .detail("JobRoot", dumpFolder)
		    .detail("Job", newJob.toString());

		// Wait until the dump job completes
		wait(self->waitUntilDumpJobComplete(cx));
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Dump Job Complete").detail("Job", newJob.toString());

		// Clear database
		wait(self->clearDatabase(cx));
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Clear DB").detail("Job", newJob.toString());

		// Submit a bulk load job
		state int oldBulkLoadMode = 0;
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1))); // Enable bulkLoad
		loop {
			// We randomly injects the job cancellation when waiting for the job completion to test the job
			// cancellation. If the job is cancelled, we should re-submit the job.
			state bool hasError = false;
			state int oldCancelTimes = self->cancelTimes;
			state BulkLoadJobState bulkLoadJob =
			    createBulkLoadJob(newJob.getJobId(),
			                      newJob.getJobRange(),
			                      newJob.getJobRoot(),
			                      static_cast<BulkLoadTransportMethod>(self->bulkLoadTransportMethod));
			TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Load Job Submitted").detail("Job", newJob.toString());
			wait(submitBulkLoadJob(cx, bulkLoadJob));

			// Wait until the load job complete
			state std::vector<KeyRange> errorRanges;
			state std::vector<BulkLoadTaskState> errorTasks =
			    wait(self->waitUntilLoadJobCompleteOrError(self, cx, newJob.getJobId(), newJob.getJobRange()));
			// waitUntilLoadJobCompleteOrError can cancel the job and set self->cancelled to true.
			// If this happens, the current job is intentionally cancelled and we should retry the job.
			ASSERT(self->cancelTimes >= oldCancelTimes);
			if (self->cancelTimes > oldCancelTimes) {
				// self->cancelTimes increments when waitUntilLoadJobCompleteOrError injects job cancellation
				wait(delay(deterministicRandom()->random01() * 10.0));
				continue;
			}
			for (const auto& errorTask : errorTasks) {
				errorRanges.push_back(errorTask.getRange());
				hasError = true;
			}
			TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Load Job Complete").detail("Job", newJob.toString());

			// Check the loaded data in DB is same as the data in DB before dumping
			std::map<Key, Value> newKvs = wait(self->getAllKVSFromDB(cx));
			if (!self->checkSame(self, kvs, newKvs, errorRanges)) {
				TraceEvent(SevError, "BulkDumpingWorkLoadError")
				    .detail("KVS", kvs.size())
				    .detail("NewKVS", newKvs.size());
				ASSERT(false);
			}

			// Acknowledge any error task of the job
			wait(acknowledgeAllErrorBulkLoadTasks(cx, bulkLoadJob.getJobId(), bulkLoadJob.getJobRange()));
			wait(self->validateBulkLoadJobHistory(cx, bulkLoadJob.getJobId(), hasError));
			break;
		}

		// Make sure all ranges locked by the workload are unlocked
		std::vector<std::pair<KeyRange, RangeLockState>> res =
		    wait(findExclusiveReadLockOnRange(cx, normalKeys, rangeLockNameForBulkLoad));
		ASSERT(res.empty());

		wait(removeRangeLockOwner(cx, rangeLockNameForBulkLoad));

		std::vector<RangeLockOwner> lockOwnersAfterRemove = wait(getAllRangeLockOwners(cx));
		ASSERT(lockOwnersAfterRemove.empty());

		return Void();
	}

	ACTOR Future<Void> _setup(BulkDumping* self, Database cx) {
		// Only client 0 registers the MockS3Server to avoid duplicates
		if (self->clientId == 0 && self->bulkLoadTransportMethod == 2) { // BLOBSTORE method
			// Check if we're using a local mock server URL pattern
			bool useMockS3 = self->jobRoot.find("127.0.0.1") != std::string::npos ||
			                 self->jobRoot.find("localhost") != std::string::npos ||
			                 self->jobRoot.find("mock-s3-server") != std::string::npos;

			if (useMockS3 && g_network->isSimulated()) {
				TraceEvent("BulkDumpingWorkload")
				    .detail("Phase", "Registering MockS3Server")
				    .detail("JobRoot", self->jobRoot);

				// Register MockS3Server with IP address - simulation environment doesn't support hostname resolution
				// See in HTTPServer.actor.cpp how the MockS3RequestHandler is implemented. Client connects to
				// connect("127.0.0.1", "8080") and then simulation network routes it to MockS3Server.
				wait(g_simulator->registerSimHTTPServer("127.0.0.1", "8080", makeReference<MockS3RequestHandler>()));

				TraceEvent("BulkDumpingWorkload")
				    .detail("Phase", "MockS3Server Registered")
				    .detail("Address", "127.0.0.1:8080");
			}
		}
		return Void();
	}
};

WorkloadFactory<BulkDumping> BulkDumpingFactory;