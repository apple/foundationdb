/*
 * RangeLockReconciliation.cpp
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
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/RangeLockConfiguration.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

class RangeLockReconciliation : public TestWorkload {
public:
	static constexpr auto NAME = "RangeLockReconciliation";

	explicit RangeLockReconciliation(WorkloadContext const& wcx)
	  : TestWorkload(wcx), phase(getOption(options, "phase"_sr, "full"_sr).toString()) {
		ASSERT(phase == "full" || phase == "prepareRestart" || phase == "finishRestart");
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }
	Future<Void> setup(Database const& cx) override {
		if (clientId == 0 && phase == "prepareRestart") {
			co_await prepareRestart(cx);
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}
		if (phase == "prepareRestart") {
			co_return;
		}
		if (phase == "finishRestart") {
			co_await finishRestart(cx);
			co_return;
		}
		ASSERT((co_await getRangeLockConfiguration(cx)).isReady());
		ASSERT((co_await getRangeLockAdmissionStatus(cx)).allProxiesEnableAcquisition);
		co_await reconcileEmptyMap(cx);
		co_await registerRangeLockOwner(cx, ownerName, "Range-lock reconciliation regression");
		co_await takeExclusiveReadLockOnRange(cx, middleRange, ownerName);
		co_await takeExclusiveReadLockOnRange(cx, suffixRange, ownerName);
		const auto before = co_await findExclusiveReadLockOnRange(cx, normalKeys, ownerName);
		ASSERT_EQ(before.size(), 2);

		const UID migrationId = deterministicRandom()->randomUniqueID();
		co_await lockDatabase(cx, migrationId);
		RangeLockConfiguration configuration = co_await begin(cx, migrationId);
		ASSERT(configuration.isMigrating() && configuration.migrationId() == migrationId);
		ASSERT(configuration.nextKey() == normalKeys.begin);
		ASSERT(co_await databaseLock(cx) == Optional<UID>(migrationId));

		co_await expectError(finish(cx, migrationId), error_code_range_lock_not_ready);
		co_await expectError(batch(cx, deterministicRandom()->randomUniqueID()), error_code_database_locked);
		co_await expectError(takeExclusiveReadLockOnRange(cx, unusedRange, ownerName), error_code_range_lock_not_ready);
		co_await expectError(releaseExclusiveReadLockOnRange(cx, middleRange, ownerName),
		                     error_code_range_lock_not_ready);
		co_await expectError(write(cx, "rangeLockReconciliation/unlocked"_sr), error_code_database_locked);
		// A raw metadata caller must not publish Ready or skip the replay cursor.
		co_await expectError(skipReplay(cx, migrationId), error_code_range_lock_not_ready);

		configuration = co_await batch(cx, migrationId);
		ASSERT(configuration.isMigrating() && configuration.nextKey() > normalKeys.begin &&
		       configuration.nextKey() < normalKeys.end);
		ASSERT(co_await findExclusiveReadLockOnRange(cx, normalKeys, ownerName) == before);

		// Use a new high-level invocation after the committed page, as an operator
		// would after the original reconciler exited or lost its connection.
		co_await reconcileRangeLocks(cx, migrationId);
		configuration = co_await getRangeLockConfiguration(cx);
		ASSERT(configuration.completedBy(migrationId));
		ASSERT(configuration.nextKey() == normalKeys.end);
		ASSERT(!(co_await databaseLock(cx)).present());
		ASSERT(co_await findExclusiveReadLockOnRange(cx, normalKeys, ownerName) == before);
		// Repeating a completed migration must not leave the database locked.
		co_await reconcileRangeLocks(cx, migrationId);
		ASSERT(!(co_await databaseLock(cx)).present());

		co_await expectError(write(cx, middleKey), error_code_transaction_rejected_range_locked);
		co_await expectError(write(cx, suffixKey), error_code_transaction_rejected_range_locked);
		co_await write(cx, "rangeLockReconciliation/unlocked"_sr);
		co_await releaseExclusiveReadLockOnRange(cx, middleRange, ownerName);
		co_await releaseExclusiveReadLockOnRange(cx, suffixRange, ownerName);
		co_await removeRangeLockOwner(cx, ownerName);
		co_await write(cx, middleKey);
		TraceEvent("RangeLockReconciliationPassed").detail("MigrationID", migrationId);
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}

private:
	const std::string phase;
	const std::string ownerName = "RangeLockReconciliation";
	const Key middleKey = "rangeLockReconciliation/b/key"_sr;
	const Key suffixKey = "rangeLockReconciliation/z/key"_sr;
	const KeyRange middleRange = KeyRangeRef("rangeLockReconciliation/b/"_sr, "rangeLockReconciliation/b0"_sr);
	const KeyRange suffixRange = KeyRangeRef("rangeLockReconciliation/z/"_sr, normalKeys.end);
	const KeyRange unusedRange = KeyRangeRef("rangeLockReconciliation/c/"_sr, "rangeLockReconciliation/c0"_sr);

	static Future<Void> reconcileEmptyMap(Database cx) {
		ASSERT((co_await findExclusiveReadLockOnRange(cx, normalKeys)).empty());
		const Key predecessor = "\xff/rangeLock."_sr;
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				const Optional<Value> previous = co_await tr.get(predecessor);
				ASSERT(!previous.present() || previous.get() == "not a range-lock value"_sr);
				tr.clear(rangeLockKeys);
				// KRM reads may synthesize an end row from this unrelated
				// predecessor when the map has no stored boundaries.
				tr.set(predecessor, "not a range-lock value"_sr);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
		const UID migrationId = deterministicRandom()->randomUniqueID();
		co_await reconcileRangeLocks(cx, migrationId);
		ASSERT((co_await getRangeLockConfiguration(cx)).completedBy(migrationId));
		ASSERT(!(co_await databaseLock(cx)).present());
		ASSERT((co_await findExclusiveReadLockOnRange(cx, normalKeys)).empty());
		tr.reset();
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.clear(predecessor);
				co_await tr.commit();
				break;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
		TraceEvent("RangeLockEmptyMapReconciliationPassed").detail("MigrationID", migrationId);
	}

	Future<Void> prepareRestart(Database cx) {
		ASSERT((co_await getRangeLockConfiguration(cx)).isReady());
		co_await registerRangeLockOwner(cx, ownerName, "Interrupted range-lock reconciliation regression");
		co_await takeExclusiveReadLockOnRange(cx, middleRange, ownerName);
		co_await takeExclusiveReadLockOnRange(cx, suffixRange, ownerName);
		const UID migrationId = deterministicRandom()->randomUniqueID();
		co_await lockDatabase(cx, migrationId);
		co_await begin(cx, migrationId);
		const RangeLockConfiguration configuration = co_await batch(cx, migrationId);
		ASSERT(configuration.isMigrating() && configuration.nextKey() > normalKeys.begin &&
		       configuration.nextKey() < normalKeys.end);
		TraceEvent("RangeLockReconciliationRestartPrepared")
		    .detail("MigrationID", migrationId)
		    .detail("NextKey", configuration.nextKey());
	}

	Future<Void> finishRestart(Database cx) {
		RangeLockConfiguration configuration = co_await getRangeLockConfiguration(cx);
		ASSERT(configuration.isMigrating() && configuration.nextKey() > normalKeys.begin &&
		       configuration.nextKey() < normalKeys.end);
		const UID migrationId = configuration.migrationId();
		ASSERT(co_await databaseLock(cx) == Optional<UID>(migrationId));
		const auto before = co_await findExclusiveReadLockOnRange(cx, normalKeys, ownerName);
		ASSERT_EQ(before.size(), 2);
		co_await expectError(takeExclusiveReadLockOnRange(cx, unusedRange, ownerName), error_code_range_lock_not_ready);
		co_await expectError(releaseExclusiveReadLockOnRange(cx, middleRange, ownerName),
		                     error_code_range_lock_not_ready);
		co_await reconcileRangeLocks(cx, migrationId);
		configuration = co_await getRangeLockConfiguration(cx);
		ASSERT(configuration.completedBy(migrationId));
		ASSERT(!(co_await databaseLock(cx)).present());
		ASSERT(co_await findExclusiveReadLockOnRange(cx, normalKeys, ownerName) == before);
		co_await expectError(write(cx, middleKey), error_code_transaction_rejected_range_locked);
		co_await expectError(write(cx, suffixKey), error_code_transaction_rejected_range_locked);
		co_await releaseExclusiveReadLockOnRange(cx, middleRange, ownerName);
		co_await releaseExclusiveReadLockOnRange(cx, suffixRange, ownerName);
		co_await removeRangeLockOwner(cx, ownerName);
		co_await write(cx, middleKey);
		TraceEvent("RangeLockReconciliationRestartPassed").detail("MigrationID", migrationId);
	}

	template <class T>
	static Future<Void> expectError(Future<T> operation, int expectedCode) {
		ErrorOr<T> result = co_await errorOr(operation);
		if (result.isError() && result.getError().code() == error_code_actor_cancelled) {
			throw result.getError();
		}
		if (!result.isError() || result.getError().code() != expectedCode) {
			TraceEvent(SevError, "RangeLockReconciliationUnexpectedResult")
			    .detail("ExpectedError", expectedCode)
			    .detail("ActualError", result.isError() ? result.getError().code() : 0);
			ASSERT(false);
		}
	}

	static Future<RangeLockConfiguration> begin(Database cx, UID migrationId) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				RangeLockConfiguration result = co_await beginRangeLockReconciliation(&tr, migrationId);
				co_await tr.commit();
				co_return result;
			} catch (Error& e) {
				if (e.code() == error_code_database_locked || e.code() == error_code_range_lock_not_ready) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<RangeLockConfiguration> batch(Database cx, UID migrationId) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				RangeLockConfiguration result = co_await reconcileRangeLockBatch(&tr, migrationId, 2);
				co_await tr.commit();
				co_return result;
			} catch (Error& e) {
				if (e.code() == error_code_database_locked || e.code() == error_code_range_lock_not_ready) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> finish(Database cx, UID migrationId) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				co_await finishRangeLockReconciliation(&tr, migrationId);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_database_locked || e.code() == error_code_range_lock_not_ready) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Void> skipReplay(Database cx, UID migrationId) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.set(rangeLockConfigurationKey,
				       rangeLockConfigurationValue(RangeLockConfiguration::migrating(migrationId, normalKeys.end)));
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_database_locked || e.code() == error_code_range_lock_not_ready) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<Optional<UID>> databaseLock(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				co_return decodeRangeLockDatabaseLock(co_await tr.get(databaseLockedKey));
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
				if (e.code() == error_code_database_locked ||
				    e.code() == error_code_transaction_rejected_range_locked) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}
};

WorkloadFactory<RangeLockReconciliation> RangeLockReconciliationFactory;
