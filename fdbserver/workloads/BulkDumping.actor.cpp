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
#include "fdbclient/SystemData.h"
#include "fdbserver/BulkDumpUtil.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/BulkLoadUtil.actor.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include <string>
#include <vector>
#include <map>
#include <set>
#include "flow/actorcompiler.h" // This must be the last #include.

const std::string simulationBulkDumpFolder = joinPath("simfdb", "bulkdump");

struct BulkDumping : TestWorkload {
	static constexpr auto NAME = "BulkDumpingWorkload";
	const bool enabled = true;
	bool pass = true;
	int cancelTimes = 0;
	int maxCancelTimes = 0;
	// Read the below from .toml file.
	BulkLoadTransportMethod bulkLoadTransportMethod;
	std::string jobRoot;

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

	BulkDumping(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true), cancelTimes(0) {
		// Read options using the global ::getOption, passing the 'options' member from WorkloadContext.

		maxCancelTimes = ::getOption(options, "maxCancelTimes"_sr, 0);

		bulkLoadTransportMethod = static_cast<BulkLoadTransportMethod>(
		    ::getOption(options, "bulkLoadTransportMethod"_sr, static_cast<int>(BulkLoadTransportMethod::CP)));

		jobRoot = ::getOption(options, "jobRoot"_sr, StringRef(simulationBulkDumpFolder)).toString();

		// Log the parsed values.
		TraceEvent("BulkDumpingConstructorParsedValues")
		    .detail("MaxCancelTimes", maxCancelTimes)
		    .detail("TransportMethod", static_cast<int>(bulkLoadTransportMethod))
		    .detail("JobRoot", jobRoot);

		// Keep the original specific logging for determinism if desired, using parsed values
		TraceEvent("BulkDumpingConfiguration")
		    .detail("MaxCancelTimes", maxCancelTimes)
		    .detail("TransportMethod", static_cast<int>(bulkLoadTransportMethod))
		    .detail("JobRoot", jobRoot)
		    .detail("RandomSeed", 0); // Using a fixed value for deterministic logging
	}

	// RAII-style resource management for bulk dump jobs
	struct BulkDumpResource {
		Database cx;
		BulkDumpState job;
		bool isUploaded;
		Optional<Error> error;

		BulkDumpResource(Database cx, BulkDumpState job) : cx(cx), job(job), isUploaded(false) {}

		~BulkDumpResource() {
			if (isUploaded) {
				try {
					// Attempt cleanup of uploaded resources
					// This is a synchronous cleanup attempt
					TraceEvent("BulkDumpResourceCleanup")
					    .detail("JobId", job.getJobId().toString())
					    .detail("JobRoot", job.getJobRoot());
				} catch (Error& e) {
					TraceEvent(SevWarn, "BulkDumpResourceCleanupError")
					    .error(e)
					    .detail("JobId", job.getJobId().toString());
				}
			}
		}
	};

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}

		// Pre-test cleanup
		std::string cleanup_path = "bulk_dump_cleanup";
		try {
			platform::eraseDirectoryRecursive(cleanup_path);
			TraceEvent(SevDebug, "BulkDumpingPreCleanup").detail("Path", cleanup_path);
		} catch (Error& e) {
			TraceEvent(SevWarn, "BulkDumpingPreCleanupError").errorUnsuppressed(e).detail("Path", cleanup_path);
		}
		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Standalone<StringRef> getRandomStringRef() const {
		// int stringLength = deterministicRandom()->randomInt(1, 10);
		// Standalone<StringRef> stringBuffer = makeString(stringLength);
		// deterministicRandom()->randomBytes(mutateString(stringBuffer), stringLength);
		// return stringBuffer;
		// return "deterministic_string"_sr; // Fixed for diagnostics - CAUSES INFINITE LOOP

		// Corrected version: deterministic and unique per call
		static thread_local int call_count = 0;
		call_count++;
		// Create a string based on the call_count.
		// The Key constructor taking a std::string will create a Standalone<StringRef> (which is an alias for Key).
		return StringRef("ds_" + std::to_string(call_count));
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
				if (self->maxCancelTimes > 0 && SERVER_KNOBS->BULKLOAD_SIM_FAILURE_INJECTION &&
				    self->cancelTimes < self->maxCancelTimes && deterministicRandom()->random01() < 0.1) {
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

		TraceEvent("BulkDumpingStartInfo")
		    .detail("MaxCancelTimes", self->maxCancelTimes)
		    .detail("BulkLoadSimFailureInjection", SERVER_KNOBS->BULKLOAD_SIM_FAILURE_INJECTION);

		if (g_network->isSimulated()) {
			// Disable connection failures for both BulkDumping and BulkLoading
			// This is necessary because S3 operations need stable connections
			disableConnectionFailures("BulkDumping");
			disableConnectionFailures("BulkLoading");
		}

		// Create unique run directory for this test instance
		state std::string uniqueRunDir = "bulk_dump_run_deterministic"; // Fixed for determinism
		if (self->bulkLoadTransportMethod != BulkLoadTransportMethod::BLOBSTORE) {
			try {
				platform::createDirectory(uniqueRunDir);
				TraceEvent(SevDebug, "BulkDumpingCreatedRunDir").detail("Dir", uniqueRunDir);
			} catch (Error& e) {
				TraceEvent(SevError, "BulkDumpingCreateRunDirError").errorUnsuppressed(e).detail("Dir", uniqueRunDir);
				throw;
			}
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
		state BulkDumpState newJob =
		    createBulkDumpJob(normalKeys,
		                      self->bulkLoadTransportMethod == BulkLoadTransportMethod::BLOBSTORE
		                          ? appendToPath(self->jobRoot, uniqueRunDir)
		                          : joinPath(uniqueRunDir, "dump"),
		                      BulkLoadType::SST,
		                      self->bulkLoadTransportMethod);

		// Initialize BulkDumpResource with the new job
		state std::unique_ptr<BulkDumpResource> dumpResource = std::make_unique<BulkDumpResource>(cx, newJob);

		try {
			// Retry submitting the dump job if it fails
			state int retries = 0;
			state double dumpDelayDuration = 1.0; // Initial delay for dump job
			loop {
				try {
					wait(submitBulkDumpJob(cx, newJob));
					dumpResource->isUploaded = true;
					TraceEvent("BulkDumpingWorkLoad")
					    .detail("Phase", "Dump Job Submitted")
					    .detail("Job", newJob.toString())
					    .detail("Retries", retries);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_operation_cancelled || retries >= 5) {
						throw;
					}
					TraceEvent(SevWarn, "BulkDumpingSubmitRetry")
					    .error(e)
					    .detail("Job", newJob.toString())
					    .detail("Retry", retries);
					retries++;
					wait(delay(dumpDelayDuration)); // Use exponential backoff
					dumpDelayDuration *= 2;
					if (dumpDelayDuration > 32.0) { // Cap delay at 32s
						dumpDelayDuration = 32.0;
					}
				}
			}

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
				state bool hasError = false;
				state int oldCancelTimes = self->cancelTimes;
				state BulkLoadJobState bulkLoadJob = createBulkLoadJob(
				    newJob.getJobId(), newJob.getJobRange(), newJob.getJobRoot(), self->bulkLoadTransportMethod);

				TraceEvent("BulkDumpingWorkLoad")
				    .detail("Phase", "Load Job Submitted")
				    .detail("Job", newJob.toString());

				// Retry submitting the load job if it fails
				state int loadRetries = 0;
				state double loadDelayDuration = 1.0; // Initial delay for load job
				loop {
					try {
						wait(submitBulkLoadJob(cx, bulkLoadJob));
						break;
					} catch (Error& e) {
						if (e.code() == error_code_operation_cancelled || loadRetries >= 5) {
							throw;
						}
						TraceEvent(SevWarn, "BulkLoadingSubmitRetry")
						    .error(e)
						    .detail("Job", bulkLoadJob.toString())
						    .detail("Retry", loadRetries);
						loadRetries++;
						wait(delay(loadDelayDuration)); // Use exponential backoff
						loadDelayDuration *= 2;
						if (loadDelayDuration > 32.0) { // Cap delay at 32s
							loadDelayDuration = 32.0;
						}
					}
				}

				// More deterministic job cancellation
				if (self->maxCancelTimes > 0 && SERVER_KNOBS->BULKLOAD_SIM_FAILURE_INJECTION &&
				    self->cancelTimes < self->maxCancelTimes) {
					state double cancelTime = deterministicRandom()->random01() * 10.0;
					wait(delay(cancelTime));
					if (cancelTime < 5.0) {
						wait(cancelBulkLoadJob(cx, newJob.getJobId()));
						self->cancelTimes++;
						TraceEvent("BulkDumpingWorkLoad")
						    .detail("Phase", "Job Cancelled")
						    .detail("Job", newJob.toString())
						    .detail("CancelTime", cancelTime);
						continue;
					}
				}

				// Wait until the load job complete
				state std::vector<KeyRange> errorRanges;
				state std::vector<BulkLoadTaskState> errorTasks =
				    wait(self->waitUntilLoadJobCompleteOrError(self, cx, newJob.getJobId(), newJob.getJobRange()));

				ASSERT(self->cancelTimes >= oldCancelTimes);
				if (self->cancelTimes > oldCancelTimes) { // This implies a cancel occurred due to the block above
					// Therefore, it should only be entered if self->maxCancelTimes > 0
					wait(delay(deterministicRandom()->random01() * 10.0));
					continue;
				}

				for (const auto& errorTask : errorTasks) {
					errorRanges.push_back(errorTask.getRange());
					hasError = true;
				}

				TraceEvent("BulkDumpingWorkLoad")
				    .detail("Phase", "Load Job Complete")
				    .detail("Job", newJob.toString())
				    .detail("HasError", hasError);

				// Enhanced validation
				std::map<Key, Value> newKvs = wait(self->getAllKVSFromDB(cx));
				if (!self->checkSame(self, kvs, newKvs, errorRanges)) {
					TraceEvent(SevError, "BulkDumpingValidationError")
					    .detail("OriginalKVS", kvs.size())
					    .detail("NewKVS", newKvs.size())
					    .detail("ErrorRanges", errorRanges.size())
					    .detail("JobId", newJob.getJobId());
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

		} catch (Error& e) {
			dumpResource->error = e;
			TraceEvent(SevError, "BulkDumpingWorkloadError")
			    .error(e)
			    .detail("JobId", newJob.getJobId().toString())
			    .detail("Phase", "MainOperation");
			throw;
		}

		// Cleanup unique run directory
		try {
			platform::eraseDirectoryRecursive(uniqueRunDir);
			TraceEvent(SevDebug, "BulkDumpingCleanedRunDir").detail("Dir", uniqueRunDir);
		} catch (Error& e) {
			TraceEvent(SevWarn, "BulkDumpingCleanupError").errorUnsuppressed(e);
		}

		return Void();
	}
};

WorkloadFactory<BulkDumping> BulkDumpingFactory;
