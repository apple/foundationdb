/*
 * BulkDumping.cpp
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

#include "fdbclient/BulkDumping.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/mocks3/MockS3Server.h"
#include "fdbserver/mocks3/MockS3ServerChaos.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "fdbrpc/simulator.h"

const std::string simulationBulkDumpFolder = joinPath("simfdb", "bulkdump");

struct BulkDumping : TestWorkload {
	static constexpr auto NAME = "BulkDumpingWorkload";
	const bool enabled = true;
	bool pass = true;
	int cancelTimes = 0;
	int maxCancelTimes = 0;
	BulkLoadTransportMethod bulkLoadTransportMethod = BulkLoadTransportMethod::CP; // Default to CP method
	std::string jobRoot = "";

	// Chaos injection options
	bool enableChaos = false;
	double errorRate = 0.1;
	double throttleRate = 0.05;
	double delayRate = 0.1;
	double corruptionRate = 0.01;
	double maxDelay = 2.0;

	// Timeout configuration
	double jobCompletionTimeout = 1800.0; // Timeout for waiting on bulk dump/load job completion
	double modeSetTimeout = 60.0; // Timeout for setBulkLoadMode/setBulkDumpMode operations
	double jobSubmitTimeout = 60.0; // Timeout for submitBulkDumpJob/submitBulkLoadJob operations

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
	    bulkLoadTransportMethod(
	        static_cast<BulkLoadTransportMethod>(getOption(options, "bulkLoadTransportMethod"_sr, 1))),
	    jobRoot(getOption(options, "jobRoot"_sr, ""_sr).toString()) {
		maxCancelTimes = 0; // TODO(BulkLoad): allow to cancel job when job ID randomly generated.

		// Initialize chaos options
		enableChaos = getOption(options, "enableChaos"_sr, false);
		errorRate = getOption(options, "errorRate"_sr, 0.1);
		throttleRate = getOption(options, "throttleRate"_sr, 0.05);
		delayRate = getOption(options, "delayRate"_sr, 0.1);
		corruptionRate = getOption(options, "corruptionRate"_sr, 0.01);
		maxDelay = getOption(options, "maxDelay"_sr, 2.0);

		// Initialize timeout options
		jobCompletionTimeout = getOption(options, "jobCompletionTimeout"_sr, 1800.0);
		modeSetTimeout = getOption(options, "modeSetTimeout"_sr, 60.0);
		jobSubmitTimeout = getOption(options, "jobSubmitTimeout"_sr, 60.0);
	}

	Future<Void> setup(Database const& cx) override { return _setup(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Standalone<StringRef> getRandomStringRef() const {
		int stringLength = deterministicRandom()->randomInt(1, 10);
		Standalone<StringRef> stringBuffer = makeString(stringLength);
		deterministicRandom()->randomBytes(mutateString(stringBuffer), stringLength);
		return stringBuffer;
	}

	KeyRange getRandomRange(BulkDumping* self, KeyRange maxRange) const {
		while (true) {
			Standalone<StringRef> keyA = self->getRandomStringRef();
			Standalone<StringRef> keyB = self->getRandomStringRef();
			if (!maxRange.contains(keyA) || !maxRange.contains(keyB)) {
				continue;
			}
			KeyRange range = keyA < keyB ? KeyRangeRef(keyA, keyB) : KeyRangeRef(keyB, keyA);
			if (range.empty() || range.singleKeyRange()) {
				continue;
			}
			return range;
		}
	}

	std::map<Key, Value> generateOrderedKVS(BulkDumping* self, KeyRange maxRange, size_t count) {
		std::map<Key, Value> kvs; // ordered
		while (kvs.size() < count) {
			Key key = self->getRandomStringRef();
			if (!maxRange.contains(key)) {
				continue;
			}
			Value val = self->getRandomStringRef();
			auto res = kvs.insert({ key, val });
			if (!res.second) {
				continue;
			}
		}
		return kvs; // ordered
	}

	Future<Void> setKeys(Database cx, std::map<Key, Value> kvs) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				for (const auto& [key, value] : kvs) {
					tr.set(key, value);
				}
				co_await tr.commit();
				TraceEvent("BulkDumpingWorkLoadSetKey")
				    .detail("KeyCount", kvs.size())
				    .detail("Version", tr.getCommittedVersion());
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> waitUntilDumpJobComplete(BulkDumping* self, Database cx) {
		double startTime = now();
		while (true) {
			// Check for timeout to prevent infinite waiting
			if (now() - startTime > self->jobCompletionTimeout) {
				TraceEvent(SevWarnAlways, "BulkDumpingWorkLoadDumpJobTimeout").detail("WaitTime", now() - startTime);
				// Timeout: assume job has completed and proceed
				break;
			}
			// Create a fresh transaction each iteration to avoid stale reads
			Transaction tr(cx);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<BulkDumpState> aliveJob = co_await getSubmittedBulkDumpJob(&tr);
				if (!aliveJob.present()) {
					break;
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid() && err.code() == error_code_actor_cancelled) {
				throw err;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
			co_await delay(30.0);
		}
	}

	Future<Void> clearDatabase(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(normalKeys);
				tr.clear(bulkDumpKeys);
				tr.clear(bulkLoadJobKeys);
				tr.clear(bulkLoadTaskKeys);
				tr.clear(bulkLoadJobHistoryKeys);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> clearRangeData(Database cx, KeyRange range) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.clear(range);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// Return error tasks
	// If allowIntermediateStates is true, tasks in Running/Complete phases are allowed (for timeout scenarios)
	Future<std::vector<BulkLoadTaskState>> validateAllBulkLoadTaskAcknowledgedOrError(
	    Database cx,
	    UID jobId,
	    KeyRange jobRange,
	    bool allowIntermediateStates = false) {
		RangeResult rangeResult;
		Key beginKey = jobRange.begin;
		Key endKey = jobRange.end;
		Transaction tr(cx);
		std::vector<BulkLoadTaskState> errorTasks;
		TraceEvent("BulkDumpingWorkLoad")
		    .detail("Phase", "ValidateAllBulkLoadTaskAcknowledgedOrError")
		    .detail("Job", jobId.toString())
		    .detail("Range", jobRange)
		    .detail("AllowIntermediateStates", allowIntermediateStates);
		while (beginKey < endKey) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				rangeResult.clear();
				rangeResult = co_await krmGetRanges(&tr, bulkLoadTaskPrefix, KeyRangeRef(beginKey, endKey));
				if (rangeResult.empty()) {
					break;
				}
				for (int i = 0; i < static_cast<int>(rangeResult.size()) - 1; ++i) {
					if (rangeResult[i].value.empty()) {
						continue;
					}
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
					// Only assert on wrong phases if we're in normal completion mode (not timeout)
					if (!allowIntermediateStates && bulkLoadTaskState.phase != BulkLoadPhase::Acknowledged &&
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
				err = e;
			}
			if (err.isValid()) {
				co_await tr.onError(err);
			}
		}
		co_return errorTasks;
	}

	Future<std::vector<BulkLoadTaskState>> waitUntilLoadJobCompleteOrError(BulkDumping* self,
	                                                                       Database cx,
	                                                                       UID jobId,
	                                                                       KeyRange jobRange) {
		double startTime = now();
		while (true) {
			// Check for timeout to prevent infinite waiting
			if (now() - startTime > self->jobCompletionTimeout) {
				TraceEvent(SevWarnAlways, "BulkDumpingWorkLoadJobTimeout")
				    .detail("Job", jobId.toString())
				    .detail("Range", jobRange)
				    .detail("WaitTime", now() - startTime);
				// Timeout: validate what we have and return (allow intermediate states since job may still be running)
				std::vector<BulkLoadTaskState> errorTasks =
				    co_await self->validateAllBulkLoadTaskAcknowledgedOrError(cx, jobId, jobRange, true);
				co_return errorTasks;
			}

			Optional<BulkLoadJobState> runningJob = co_await getRunningBulkLoadJob(cx);
			if (runningJob.present()) {
				ASSERT(runningJob.get().getJobId() == jobId);
				// During the wait for the job completion, we may inject the job cancellation.
				// We varies the timing of the job cancellation, we trigger the job cancellation with 10% probability at
				// each time. Throughout the entire test, we inject the job cancellation at most maxCancelTimes times to
				// ensure the job can complete fast.
				if (SERVER_KNOBS->BULKLOAD_SIM_FAILURE_INJECTION && self->cancelTimes < self->maxCancelTimes &&
				    deterministicRandom()->random01() < 0.1) {
					co_await cancelBulkLoadJob(cx, jobId);
					self->cancelTimes++; // Inject cancellation. Then the bulkload job should run again.
					TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Job Cancelled").detail("Job", jobId.toString());
					co_await self->clearRangeData(cx, jobRange);
					TraceEvent("BulkDumpingWorkLoad")
					    .detail("Phase", "Data Cleared")
					    .detail("Job", jobId.toString())
					    .detail("JobRange", jobRange);
					co_return std::vector<BulkLoadTaskState>();
				}
				co_await delay(10.0);
				continue;
			}
			std::vector<BulkLoadTaskState> errorTasks =
			    co_await self->validateAllBulkLoadTaskAcknowledgedOrError(cx, jobId, jobRange);
			co_return errorTasks;
		}
	}

	Future<Void> validateBulkLoadJobHistory(Database cx,
	                                        UID jobId,
	                                        bool hasError,
	                                        bool bulkDumpRangeContainBulkLoadRange) {
		std::vector<BulkLoadJobState> jobHistory = co_await getBulkLoadJobFromHistory(cx);
		Optional<BulkLoadJobState> jobInHistory;
		for (const auto& job : jobHistory) {
			ASSERT(job.isValid());
			if (job.getJobId() == jobId) {
				ASSERT(!jobInHistory.present());
				jobInHistory = job;
			}
		}
		ASSERT(jobInHistory.present());
		if (hasError || !bulkDumpRangeContainBulkLoadRange) {
			ASSERT(jobInHistory.get().getPhase() == BulkLoadJobPhase::Error);
			ASSERT(jobInHistory.get().getErrorMessage().present());
			if (jobInHistory.get().getErrorMessage().get().find(
			        std::to_string(bulkload_dataset_not_cover_required_range().code())) != std::string::npos) {
				ASSERT(!bulkDumpRangeContainBulkLoadRange);
			}
		} else {
			ASSERT(jobInHistory.get().getPhase() == BulkLoadJobPhase::Complete);
		}
	}

	Future<std::map<Key, Value>> getAllKVSFromDB(Database cx) {
		Transaction tr(cx);
		std::map<Key, Value> kvs;
		while (true) {
			Error err;
			try {
				RangeResult kvsRes = co_await tr.getRange(normalKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!kvsRes.more);
				kvs.clear();
				for (auto& kv : kvsRes) {
					auto res = kvs.insert({ kv.key, kv.value });
					ASSERT(res.second);
				}
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		co_return kvs;
	}

	bool keyContainedInRanges(const Key& key, const std::vector<KeyRange>& ranges) {
		for (const auto& range : ranges) {
			if (range.contains(key)) {
				return true;
			}
		}
		return false;
	}

	// kvs is the key value pairs generated initially in the database.
	// newKvs is the key value pairs loaded by the bulk loading job.
	// The workload first does bulkdump which only dumps the data within the bulkDumpJobRange.
	// The workload then does bulkload which loads the data within the bulkLoadJobRange.
	// The bulkLoadJob may be failed with unretryable error. So, some ranges (i.e. ignoreRanges) is not loaded.
	// This function compares the consistency between kvs and newKvs within bulkLoadJobRange and bulkDumpJobRange and
	// outside ignoreRanges. If a key in kvs is outside the bulkDumpJobRange, the newKvs should not contain the key.
	void processCheck(BulkDumping* self,
	                  std::map<Key, Value> kvs,
	                  std::map<Key, Value> newKvs,
	                  KeyRange bulkLoadJobRange,
	                  KeyRange bulkDumpJobRange,
	                  std::vector<KeyRange> ignoreRanges) {
		std::vector<KeyValue> kvsToCheck;
		std::vector<KeyValue> newKvsToCheck;
		std::unordered_set<Key> keyOutsideDumpData;
		for (const auto& [key, value] : kvs) {
			if (!bulkDumpJobRange.contains(key)) {
				keyOutsideDumpData.insert(key);
				continue; // kvs may contain keys outside the bulkDumpJobRange
			}
			if (self->keyContainedInRanges(key, ignoreRanges)) {
				continue;
			}
			if (!bulkLoadJobRange.contains(key)) {
				continue; // kvs may contain keys outside the bulkLoadJobRange
			}
			kvsToCheck.push_back(KeyValueRef(key, value));
		}
		for (const auto& [key, value] : newKvs) {
			// newKvs should not contain keys outside the bulkDumpJobRange
			ASSERT(keyOutsideDumpData.find(key) == keyOutsideDumpData.end() && bulkDumpJobRange.contains(key));
			if (self->keyContainedInRanges(key, ignoreRanges)) {
				continue;
			}
			// newKvs should not contain keys outside the bulkLoadJobRange nor bulkDumpJobRange
			ASSERT(bulkLoadJobRange.contains(key));
			ASSERT(bulkDumpJobRange.contains(key));
			newKvsToCheck.push_back(KeyValueRef(key, value));
		}
		if (kvsToCheck != newKvsToCheck) {
			TraceEvent(SevError, "BulkDumpingWorkLoadError")
			    .detail("KVS", kvsToCheck.size())
			    .detail("NewKVS", newKvsToCheck.size());
			ASSERT(false);
		}
	}

	// This workload does following:
	// (1) Generate 1000 key value pairs in normalKey space;
	// (2) Randomly select a key range from normalKey space;
	// (3) Submit a bulk dump job with the selected key range;
	// (4) Wait until the bulk dump job completes;
	// (5) Clear the database;
	// (6) Randomly select a key range from normalKey space;
	// (7) Submit a bulk load job with the selected key range;
	// (8) Wait until the bulk load job completes;
	// (9) Validate the loaded data in DB is same as the data in DB before dumping within the bulkdump job range and
	// bulkload job range. Note that the bulkload job can be unretriable error. In this case, we ignore the error range;
	// (10) Validate the bulk load job history.
	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}

		// Cleanup any leftover state from previous test iterations BEFORE starting work
		// This ensures we start clean even if a previous iteration timed out or crashed
		if (bulkLoadTransportMethod == BulkLoadTransportMethod::BLOBSTORE && g_network->isSimulated()) {
			// Clear MockS3Server storage to prevent memory accumulation over test iterations
			clearMockS3Storage();
		}

		if (g_network->isSimulated()) {
			// Network partition between CC and DD can cause DD no longer existing,
			// which results in the bulk loading task cannot complete
			// So, this workload disable the network partition
			disableConnectionFailures("BulkDumping");
		}

		KeyRange bulkDumpJobRange = deterministicRandom()->coinflip() ? normalKeys : getRandomRange(this, normalKeys);

		bool bulkDumpRangeContainBulkLoadRange = true; // Will set to false if the bulk load job range is not
		// contained in the bulk dump job range. In this case, the bulk load job will be failed fast with error.
		// So, when set to false, skip the processCheck() in the end of the workload.
		// Also, check bulkload job history to ensure the job is failed with the expected error.

		std::map<Key, Value> kvs = generateOrderedKVS(this, normalKeys, 1000);
		co_await setKeys(cx, kvs);

		// BulkLoad uses range lock
		co_await registerRangeLockOwner(cx, rangeLockNameForBulkLoad, rangeLockNameForBulkLoad);

		std::vector<RangeLockOwner> lockOwners = co_await getAllRangeLockOwners(cx);
		ASSERT(lockOwners.size() == 1 && lockOwners[0].getOwnerUniqueId() == rangeLockNameForBulkLoad);

		// Submit a bulk dump job
		int oldBulkDumpMode = 0;
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Setting BulkDump Mode");
		try {
			oldBulkDumpMode = co_await timeoutError(setBulkDumpMode(cx, 1), modeSetTimeout); // Enable bulkDump
			TraceEvent("BulkDumpingWorkLoad").detail("Phase", "BulkDump Mode Set").detail("OldMode", oldBulkDumpMode);
		} catch (Error& e) {
			if (e.code() == error_code_timed_out) {
				TraceEvent(SevWarnAlways, "BulkDumpingWorkLoadSetDumpModeTimeout")
				    .detail("TimeoutSeconds", modeSetTimeout);
				throw;
			}
			throw;
		}
		std::string dumpFolder = jobRoot.empty() ? simulationBulkDumpFolder : jobRoot;
		BulkDumpState bulkDumpJob =
		    createBulkDumpJob(bulkDumpJobRange, dumpFolder, BulkLoadType::SST, bulkLoadTransportMethod);
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Submitting Dump Job").detail("Job", bulkDumpJob.getJobId());
		co_await timeoutError(submitBulkDumpJob(cx, bulkDumpJob), jobSubmitTimeout);
		TraceEvent("BulkDumpingWorkLoad")
		    .detail("Phase", "Dump Job Submitted")
		    .detail("TransportMethod", convertBulkLoadTransportMethodToString(bulkLoadTransportMethod))
		    .detail("JobRoot", dumpFolder)
		    .detail("Job", bulkDumpJob.toString());

		// Wait until the dump job completes
		co_await waitUntilDumpJobComplete(this, cx);
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Dump Job Complete").detail("Job", bulkDumpJob.toString());

		// Clear database
		co_await clearDatabase(cx);
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Clear DB").detail("Job", bulkDumpJob.toString());

		// Submit a bulk load job
		int oldBulkLoadMode = 0;
		TraceEvent("BulkDumpingWorkLoad")
		    .detail("Phase", "Setting BulkLoad Mode")
		    .detail("Job", bulkDumpJob.toString());
		try {
			oldBulkLoadMode = co_await timeoutError(setBulkLoadMode(cx, 1), modeSetTimeout); // Enable bulkLoad
			TraceEvent("BulkDumpingWorkLoad").detail("Phase", "BulkLoad Mode Set").detail("OldMode", oldBulkLoadMode);
		} catch (Error& e) {
			if (e.code() == error_code_timed_out) {
				TraceEvent(SevWarnAlways, "BulkDumpingWorkLoadSetModeTimeout")
				    .detail("Job", bulkDumpJob.toString())
				    .detail("TimeoutSeconds", modeSetTimeout);
				throw;
			}
			throw;
		}
		while (true) {
			// We randomly injects the job cancellation when waiting for the job completion to test the job
			// cancellation. If the job is cancelled, we should re-submit the job.
			bool hasError = false;
			int oldCancelTimes = cancelTimes;
			KeyRange bulkLoadJobRange =
			    deterministicRandom()->coinflip()
			        ? bulkDumpJob.getJobRange()
			        : getRandomRange(this, deterministicRandom()->coinflip() ? normalKeys : bulkDumpJobRange);
			UID dataSourceId = bulkDumpJob.getJobId();
			std::string dataSourceRoot = bulkDumpJob.getJobRoot();
			BulkLoadJobState bulkLoadJob =
			    createBulkLoadJob(dataSourceId, bulkLoadJobRange, dataSourceRoot, bulkLoadTransportMethod);
			TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Submitting Load Job").detail("JobId", dataSourceId);
			co_await timeoutError(submitBulkLoadJob(cx, bulkLoadJob), jobSubmitTimeout);
			TraceEvent("BulkDumpingWorkLoad")
			    .detail("Phase", "Load Job Submitted")
			    .detail("JobId", dataSourceId)
			    .detail("JobRange", bulkLoadJobRange)
			    .detail("JobRoot", dataSourceRoot)
			    .detail("TransportMethod", bulkLoadJob.getTransportMethod());

			// Wait until the load job complete
			std::vector<KeyRange> errorRanges;
			std::vector<BulkLoadTaskState> errorTasks =
			    co_await waitUntilLoadJobCompleteOrError(this, cx, bulkLoadJob.getJobId(), bulkLoadJob.getJobRange());
			// waitUntilLoadJobCompleteOrError can cancel the job and set cancelled to true.
			// If this happens, the current job is intentionally cancelled and we should retry the job.
			ASSERT(cancelTimes >= oldCancelTimes);
			if (cancelTimes > oldCancelTimes) {
				// cancelTimes increments when waitUntilLoadJobCompleteOrError injects job cancellation
				co_await delay(deterministicRandom()->random01() * 10.0);
				continue;
			}
			for (const auto& errorTask : errorTasks) {
				errorRanges.push_back(errorTask.getRange());
				hasError = true;
			}
			TraceEvent("BulkDumpingWorkLoad")
			    .detail("Phase", "Load Job Complete")
			    .detail("BulkLoadJobId", dataSourceId)
			    .detail("BulkLoadJobRange", bulkLoadJobRange)
			    .detail("BulkLoadJobRoot", dataSourceRoot)
			    .detail("BulkLoadTransportMethod", bulkLoadJob.getTransportMethod());

			// Check the loaded data in DB is same as the data in DB before dumping
			std::map<Key, Value> newKvs = co_await getAllKVSFromDB(cx);
			if (bulkDumpJobRange.contains(bulkLoadJobRange)) {
				processCheck(this, kvs, newKvs, bulkLoadJobRange, bulkDumpJobRange, errorRanges);
				bulkDumpRangeContainBulkLoadRange = true;
			} else {
				TraceEvent(SevWarnAlways, "BulkDumpingWorkLoad")
				    .detail("Phase", "SkippedCheck")
				    .detail("BulkDumpJobRange", bulkDumpJobRange)
				    .detail("BulkLoadJobRange", bulkLoadJobRange);
				bulkDumpRangeContainBulkLoadRange = false;
			}

			// Acknowledge any error task of the job
			co_await acknowledgeAllErrorBulkLoadTasks(cx, bulkLoadJob.getJobId(), bulkLoadJob.getJobRange());
			co_await validateBulkLoadJobHistory(
			    cx, bulkLoadJob.getJobId(), hasError, bulkDumpRangeContainBulkLoadRange);
			break;
		}

		// Make sure all ranges locked by the workload are unlocked
		std::vector<std::pair<KeyRange, RangeLockState>> res =
		    co_await findExclusiveReadLockOnRange(cx, normalKeys, rangeLockNameForBulkLoad);
		ASSERT(res.empty());

		co_await removeRangeLockOwner(cx, rangeLockNameForBulkLoad);

		std::vector<RangeLockOwner> lockOwnersAfterRemove = co_await getAllRangeLockOwners(cx);
		ASSERT(lockOwnersAfterRemove.empty());
	}

	Future<Void> _setup(BulkDumping* self, Database cx) {
		// Only client 0 registers the MockS3Server to avoid duplicates
		if (self->clientId == 0 && self->bulkLoadTransportMethod == BulkLoadTransportMethod::BLOBSTORE) {
			// Check if we're using a local mock server URL pattern
			bool useMockS3 = self->jobRoot.find("127.0.0.1") != std::string::npos ||
			                 self->jobRoot.find("localhost") != std::string::npos ||
			                 self->jobRoot.find("mock-s3-server") != std::string::npos;

			if (useMockS3 && g_network->isSimulated()) {
				// Check if 127.0.0.1:8080 is already registered in simulator's httpHandlers
				std::string serverKey = "127.0.0.1:8080";
				bool alreadyRegistered = g_simulator->httpHandlers.contains(serverKey);

				if (alreadyRegistered) {
					TraceEvent("BulkDumpingWorkload")
					    .detail("Phase", "MockS3Server Already Registered")
					    .detail("Address", serverKey)
					    .detail("ChaosRequested", self->enableChaos)
					    .detail("Reason", "Reusing existing HTTP handler from previous test");
				} else if (self->enableChaos) {
					TraceEvent("BulkDumpingWorkload")
					    .detail("Phase", "Starting MockS3ServerChaos")
					    .detail("JobRoot", self->jobRoot);

					// Start MockS3ServerChaos - has internal duplicate detection
					NetworkAddress listenAddress(IPAddress(0x7f000001), 8080);
					co_await startMockS3ServerChaos(listenAddress);

					TraceEvent("BulkDumpingWorkload")
					    .detail("Phase", "MockS3ServerChaos Started")
					    .detail("Address", "127.0.0.1:8080");
				} else {
					TraceEvent("BulkDumpingWorkload")
					    .detail("Phase", "Registering MockS3Server")
					    .detail("JobRoot", self->jobRoot);

					// Register MockS3Server with persistence enabled
					co_await registerMockS3Server("127.0.0.1", "8080");

					TraceEvent("BulkDumpingWorkload")
					    .detail("Phase", "MockS3Server Registered")
					    .detail("Address", "127.0.0.1:8080");
				}
			}
		}

		// Configure chaos rates for all clients if chaos is enabled
		// This allows each test to have different chaos rates
		if (self->enableChaos && g_network->isSimulated()) {
			auto injector = S3FaultInjector::injector();
			injector->setErrorRate(self->errorRate);
			injector->setThrottleRate(self->throttleRate);
			injector->setDelayRate(self->delayRate);
			injector->setCorruptionRate(self->corruptionRate);
			injector->setMaxDelay(self->maxDelay);

			TraceEvent("BulkDumpingWorkload")
			    .detail("Phase", "Chaos Configured")
			    .detail("ClientID", self->clientId)
			    .detail("ErrorRate", self->errorRate)
			    .detail("ThrottleRate", self->throttleRate)
			    .detail("DelayRate", self->delayRate)
			    .detail("CorruptionRate", self->corruptionRate);
		}
	}
};

WorkloadFactory<BulkDumping> BulkDumpingFactory;
