/*
 * RangeLockLifecycle.cpp
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

#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

class RangeLockLifecycle : public TestWorkload {
public:
	static constexpr auto NAME = "RangeLockLifecycle";

	explicit RangeLockLifecycle(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}
		co_await testOwnerLifecycle(cx);
		co_await testConcurrentRetirement(cx);
		co_await testBulkLoadAttempts(cx);
		TraceEvent("RangeLockLifecyclePassed");
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}

private:
	const std::string ownerName = "RangeLockLifecycle";
	const Key ownerKey = "rangeLockLifecycle/owner"_sr;
	const Key raceKey = "rangeLockLifecycle/race"_sr;
	const Key bulkLoadKey = "rangeLockLifecycle/bulkLoad"_sr;

	static RangeLockState newLock(const std::string& owner, const KeyRange& range) {
		return RangeLockState(
		    RangeLockType::ExclusiveReadLock, owner, range, deterministicRandom()->randomUniqueID().toString());
	}

	template <class T>
	static Future<Void> expectError(Future<T> operation, int expectedCode) {
		ErrorOr<T> result = co_await errorOr(operation);
		if (result.isError() && result.getError().code() == error_code_actor_cancelled) {
			throw result.getError();
		}
		if (!result.isError() || result.getError().code() != expectedCode) {
			TraceEvent(SevError, "RangeLockLifecycleUnexpectedResult")
			    .detail("ExpectedError", expectedCode)
			    .detail("ActualError", result.isError() ? result.getError().code() : 0);
			ASSERT(false);
		}
	}

	static Future<RangeLockOwner> requireOwner(Database cx, std::string name) {
		Optional<RangeLockOwner> owner = co_await getRangeLockOwner(cx, name);
		ASSERT(owner.present() && owner.get().getGeneration().isValid());
		co_return owner.get();
	}

	static Future<bool> isHeld(Database cx, RangeLockState lock) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				co_return co_await isExclusiveReadLockHeld(&tr, lock);
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> write(Database cx, Key key) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.set(key, "value"_sr);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_transaction_rejected_range_locked) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<std::vector<std::pair<Key, Value>>> readSystemRange(Database cx, KeyRange range) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				std::vector<std::pair<Key, Value>> result;
				Key begin = range.begin;
				while (begin < range.end) {
					RangeResult page = co_await tr.getRange(KeyRangeRef(begin, range.end), 1000);
					for (const auto& kv : page) {
						result.emplace_back(Key(kv.key), Value(kv.value));
					}
					if (!page.more) {
						break;
					}
					ASSERT(!page.empty());
					begin = keyAfter(page.back().key);
				}
				co_return result;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> testOwnerLifecycle(Database cx) {
		const KeyRange range = singleKeyRange(ownerKey);
		co_await registerRangeLockOwner(cx, ownerName, "Range-lock lifecycle test");
		const RangeLockOwner firstOwner = co_await requireOwner(cx, ownerName);
		co_await registerRangeLockOwner(cx, ownerName, "Updated lifecycle-test description");
		ASSERT((co_await requireOwner(cx, ownerName)).getGeneration() == firstOwner.getGeneration());

		const RangeLockState first = newLock(ownerName, range);
		const RangeLockState second = newLock(ownerName, range);
		co_await takeExclusiveReadLockOnRange(cx, first, firstOwner.getGeneration());
		co_await takeExclusiveReadLockOnRange(cx, first, firstOwner.getGeneration());
		// The administrative idempotent API must preserve an acquisition's token.
		co_await takeExclusiveReadLockOnRange(cx, range, ownerName);
		ASSERT(co_await isHeld(cx, first));
		co_await expectError(removeRangeLockOwner(cx, ownerName), error_code_range_lock_reject);
		co_await expectError(removeRangeLockOwner(cx, firstOwner), error_code_range_lock_reject);
		co_await expectError(takeExclusiveReadLockOnRange(cx, second, firstOwner.getGeneration()),
		                     error_code_range_lock_reject);
		ASSERT((co_await requireOwner(cx, ownerName)).getGeneration() == firstOwner.getGeneration());
		ASSERT(co_await isHeld(cx, first));
		ASSERT(!(co_await isHeld(cx, second)));
		co_await expectError(write(cx, ownerKey), error_code_transaction_rejected_range_locked);

		co_await releaseExclusiveReadLockOnRange(cx, first);
		co_await releaseExclusiveReadLockOnRange(cx, first);
		co_await takeExclusiveReadLockOnRange(cx, second, firstOwner.getGeneration());
		co_await expectError(releaseExclusiveReadLockOnRange(cx, first), error_code_range_unlock_reject);
		ASSERT(co_await isHeld(cx, second));
		ASSERT(!(co_await isHeld(cx, first)));
		co_await expectError(write(cx, ownerKey), error_code_transaction_rejected_range_locked);
		co_await releaseExclusiveReadLockOnRange(cx, second);
		co_await removeRangeLockOwner(cx, firstOwner);
		ASSERT(!(co_await getRangeLockOwner(cx, ownerName)).present());

		co_await registerRangeLockOwner(cx, ownerName, "Reused lifecycle-test owner name");
		const RangeLockOwner replacementOwner = co_await requireOwner(cx, ownerName);
		ASSERT(replacementOwner.getGeneration() != firstOwner.getGeneration());
		co_await expectError(removeRangeLockOwner(cx, firstOwner), error_code_range_lock_failed);
		co_await expectError(takeExclusiveReadLockOnRange(cx, first, firstOwner.getGeneration()),
		                     error_code_range_lock_failed);
		const RangeLockState replacement = newLock(ownerName, range);
		co_await takeExclusiveReadLockOnRange(cx, replacement, replacementOwner.getGeneration());
		co_await expectError(releaseExclusiveReadLockOnRange(cx, second), error_code_range_unlock_reject);
		ASSERT(co_await isHeld(cx, replacement));
		co_await releaseExclusiveReadLockOnRange(cx, replacement);
		co_await removeRangeLockOwner(cx, replacementOwner);
		ASSERT(!(co_await getRangeLockOwner(cx, ownerName)).present());
		co_await write(cx, ownerKey);
	}

	Future<Void> testConcurrentRetirement(Database cx) {
		const std::string raceOwnerName = ownerName + "/race";
		const KeyRange range = singleKeyRange(raceKey);
		for (int i = 0; i < 8; ++i) {
			co_await registerRangeLockOwner(cx, raceOwnerName, "Concurrent range-lock retirement test");
			const RangeLockOwner owner = co_await requireOwner(cx, raceOwnerName);
			const RangeLockState lock = newLock(raceOwnerName, range);
			Future<Void> take = takeExclusiveReadLockOnRange(cx, lock, owner.getGeneration());
			Future<Void> retire = removeRangeLockOwner(cx, owner);
			ErrorOr<Void> takeResult = co_await errorOr(take);
			ErrorOr<Void> retireResult = co_await errorOr(retire);
			if (takeResult.isError() && takeResult.getError().code() == error_code_actor_cancelled) {
				throw takeResult.getError();
			}
			if (retireResult.isError() && retireResult.getError().code() == error_code_actor_cancelled) {
				throw retireResult.getError();
			}
			// Exactly one wins: a successful take must leave its registration intact.
			ASSERT(takeResult.isError() != retireResult.isError());
			if (takeResult.isError()) {
				ASSERT(takeResult.getError().code() == error_code_range_lock_failed);
				ASSERT(!(co_await getRangeLockOwner(cx, raceOwnerName)).present());
				ASSERT(!(co_await isHeld(cx, lock)));
			} else {
				ASSERT(retireResult.getError().code() == error_code_range_lock_reject);
				ASSERT((co_await requireOwner(cx, raceOwnerName)).getGeneration() == owner.getGeneration());
				ASSERT(co_await isHeld(cx, lock));
				co_await releaseExclusiveReadLockOnRange(cx, lock);
				co_await removeRangeLockOwner(cx, owner);
			}
		}
	}

	static Future<Void> checkBulkLoadFence(Database cx, BulkLoadJobHandle expectedJob) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				co_await checkBulkLoadJobLock(&tr, expectedJob.getJobState().getJobId(), expectedJob.getRangeLock());
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> requireCurrentJob(Database cx, BulkLoadJobHandle expectedJob) {
		Optional<BulkLoadJobHandle> current = co_await getRunningBulkLoadJobHandle(cx);
		ASSERT(current.present() && current.get().hasSameSubmission(expectedJob));
		co_await checkBulkLoadFence(cx, expectedJob);
		ASSERT(co_await isHeld(cx, expectedJob.getRangeLock()));
		ASSERT(!(co_await getBulkLoadJobOutcome(cx, expectedJob)).present());
	}

	static Future<BulkLoadTaskState> installErrorTask(Database cx, BulkLoadJobHandle expectedJob) {
		const BulkLoadJobState& job = expectedJob.getJobState();
		BulkLoadTaskState task = createBulkLoadTask(
		    job.getJobId(),
		    job.getJobRange(),
		    BulkLoadFileSet(job.getJobRoot(), "", generateEmptyManifestFileName(), "", "", BulkLoadChecksum()),
		    BulkLoadByteSampleSetting(0, "hashlittle2", 1, 0, 1.0),
		    invalidVersion,
		    0,
		    0,
		    BulkLoadType::SST,
		    BulkLoadTransportMethod::CP);
		task.phase = BulkLoadPhase::Error;
		ASSERT(task.isValid());
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				co_await checkBulkLoadJobLock(&tr, job.getJobId(), expectedJob.getRangeLock());
				co_await krmSetRange(&tr, bulkLoadTaskPrefix, task.getRange(), bulkLoadTaskStateValue(task));
				co_await tr.commit();
				co_return task;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> requireTaskPhase(Database cx, BulkLoadTaskState expectedTask, BulkLoadPhase phase) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				BulkLoadTaskState current =
				    co_await getBulkLoadTask(&tr, expectedTask.getRange(), expectedTask.getTaskId(), { phase });
				ASSERT(current.getJobId() == expectedTask.getJobId());
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> testBulkLoadAttempts(Database cx) {
		// No source files are needed: DD must not execute this metadata-only job.
		const int previousMode = co_await setBulkLoadMode(cx, 0);
		ASSERT(!(co_await getRunningBulkLoadJobHandle(cx)).present());
		const KeyRange range = singleKeyRange(bulkLoadKey);
		const BulkLoadJobState job(deterministicRandom()->randomUniqueID(),
		                           "range-lock-lifecycle-unused-source",
		                           range,
		                           BulkLoadTransportMethod::CP);

		const std::string blockerName = ownerName + "/bulkLoadBlocker";
		co_await registerRangeLockOwner(cx, blockerName, "Bulk-load range-lock contention test");
		const RangeLockOwner blockerOwner = co_await requireOwner(cx, blockerName);
		const RangeLockState blocker = newLock(blockerName, range);
		co_await takeExclusiveReadLockOnRange(cx, blocker, blockerOwner.getGeneration());
		const auto jobsBefore = co_await readSystemRange(cx, bulkLoadJobKeys);
		const auto tasksBefore = co_await readSystemRange(cx, bulkLoadTaskKeys);
		const auto fencesBefore = co_await readSystemRange(cx, bulkLoadJobRangeLockKeys);
		co_await expectError(submitBulkLoadJobWithLock(cx, job), error_code_range_lock_reject);
		ASSERT(!(co_await getRunningBulkLoadJobHandle(cx)).present());
		ASSERT(co_await readSystemRange(cx, bulkLoadJobKeys) == jobsBefore);
		ASSERT(co_await readSystemRange(cx, bulkLoadTaskKeys) == tasksBefore);
		ASSERT(co_await readSystemRange(cx, bulkLoadJobRangeLockKeys) == fencesBefore);
		ASSERT(co_await isHeld(cx, blocker));
		co_await releaseExclusiveReadLockOnRange(cx, blocker);
		co_await removeRangeLockOwner(cx, blockerOwner);

		const BulkLoadJobHandle first = co_await submitBulkLoadJobWithLock(cx, job);
		ASSERT((co_await submitBulkLoadJobWithLock(cx, job)).hasSameSubmission(first));
		co_await requireCurrentJob(cx, first);
		co_await expectError(releaseExclusiveReadLockOnRange(cx, range, rangeLockNameForBulkLoad),
		                     error_code_range_unlock_reject);
		co_await expectError(releaseExclusiveReadLockByUser(cx, rangeLockNameForBulkLoad),
		                     error_code_range_unlock_reject);
		co_await requireCurrentJob(cx, first);
		co_await cancelBulkLoadJob(cx, first);
		ASSERT(!(co_await getRunningBulkLoadJobHandle(cx)).present());
		ASSERT(!(co_await isHeld(cx, first.getRangeLock())));
		Optional<BulkLoadJobState> firstOutcome = co_await getBulkLoadJobOutcome(cx, first);
		ASSERT(firstOutcome.present() && firstOutcome.get().getPhase() == BulkLoadJobPhase::Cancelled);

		// A dump can be loaded again, but its job ID is not an acquisition token.
		const BulkLoadJobHandle second = co_await submitBulkLoadJobWithLock(cx, job);
		ASSERT(second.getJobState().getJobId() == first.getJobState().getJobId());
		ASSERT(!second.hasSameSubmission(first));
		ASSERT(second.getRangeLock().getLockId() != first.getRangeLock().getLockId());
		co_await requireCurrentJob(cx, second);
		// The old dump ID also must not authorize acknowledgment of a replacement attempt's tasks.
		const BulkLoadTaskState secondTask = co_await installErrorTask(cx, second);
		const auto replacementTasks = co_await readSystemRange(cx, bulkLoadTaskKeys);
		co_await expectError(acknowledgeAllErrorBulkLoadTasks(cx, first), error_code_bulkload_task_outdated);
		ASSERT(co_await readSystemRange(cx, bulkLoadTaskKeys) == replacementTasks);
		co_await requireTaskPhase(cx, secondTask, BulkLoadPhase::Error);
		co_await expectError(cancelBulkLoadJob(cx, first), error_code_bulkload_task_outdated);
		co_await expectError(failBulkLoadJob(cx, first, "stale attempt"), error_code_bulkload_task_outdated);
		co_await expectError(checkBulkLoadFence(cx, first), error_code_bulkload_task_outdated);
		co_await expectError(releaseExclusiveReadLockOnRange(cx, first.getRangeLock()), error_code_range_unlock_reject);
		co_await requireCurrentJob(cx, second);
		co_await expectError(write(cx, bulkLoadKey), error_code_transaction_rejected_range_locked);
		firstOutcome = co_await getBulkLoadJobOutcome(cx, first);
		ASSERT(firstOutcome.present() && firstOutcome.get().getPhase() == BulkLoadJobPhase::Cancelled);
		co_await acknowledgeAllErrorBulkLoadTasks(cx, second);
		co_await requireTaskPhase(cx, secondTask, BulkLoadPhase::Acknowledged);

		const std::string terminalError = "RangeLockLifecycle expected terminal error";
		co_await failBulkLoadJob(cx, second, terminalError);
		ASSERT(!(co_await getRunningBulkLoadJobHandle(cx)).present());
		ASSERT(!(co_await isHeld(cx, second.getRangeLock())));
		const Optional<BulkLoadJobState> secondOutcome = co_await getBulkLoadJobOutcome(cx, second);
		ASSERT(secondOutcome.present() && secondOutcome.get().getPhase() == BulkLoadJobPhase::Error);
		ASSERT(secondOutcome.get().getErrorMessage().present() &&
		       secondOutcome.get().getErrorMessage().get() == terminalError);
		// History is bounded by dump job ID. A replacement entry must not be reported as the old attempt's outcome.
		co_await expectError(getBulkLoadJobOutcome(cx, first), error_code_bulkload_task_outdated);
		co_await clearBulkLoadJobHistory(cx, job.getJobId());
		co_await expectError(getBulkLoadJobOutcome(cx, second), error_code_bulkload_task_outdated);
		co_await write(cx, bulkLoadKey);
		co_await setBulkLoadMode(cx, previousMode);
	}
};

WorkloadFactory<RangeLockLifecycle> RangeLockLifecycleFactory;
