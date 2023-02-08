/*
 * TenantLock.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/TenantManagement.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace {

struct TenantLock : TestWorkload {
	static constexpr auto NAME = "TenantLock";
	Value tenant1;
	Value tenant2;

	enum class LockAwareTx { NO, READ, READ_WRITE };

	TenantLock(WorkloadContext const& wcx) : TestWorkload(wcx) {
		tenant1 = getOption(options, "tenant1"_sr, ""_sr);
		tenant2 = getOption(options, "tenant2"_sr, ""_sr);
		// Both tenants should be created using the CreateTenant workload and then passed to this workload in the
		// toml file
		ASSERT(!tenant1.empty() && !tenant2.empty());
	}

	ACTOR static Future<Optional<UID>> probeTx(Database db,
	                                           TenantName name,
	                                           Optional<UID> expectedVal,
	                                           bool readOnly = false,
	                                           LockAwareTx lA = LockAwareTx::NO) {
		state ReadYourWritesTransaction tr(db, makeReference<Tenant>(db, name));
		state UID result = deterministicRandom()->randomUniqueID();
		loop {
			try {
				switch (lA) {
				case LockAwareTx::NO:
					break;
				case LockAwareTx::READ:
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					break;
				case LockAwareTx::READ_WRITE:
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				}
				Optional<Value> val = wait(tr.get("foo"_sr));
				ASSERT(expectedVal.present() == val.present());
				ASSERT(!val.present() ||
				       BinaryReader::fromStringRef<UID>(val.get(), IncludeVersion()) == expectedVal.get());
				if (readOnly) {
					return BinaryReader::fromStringRef<UID>(val.get(), IncludeVersion());
				}
				tr.set("foo"_sr, BinaryWriter::toValue(result, IncludeVersion()));
				wait(tr.commit());
				return result;
			} catch (Error& e) {
				TraceEvent("ProbeTxError").error(e);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> changeLockState(Database db, TenantName name, TenantLockState desiredState, UID lockID) {
		state Reference<Tenant> tenant = makeReference<Tenant>(db, name);
		state ReadYourWritesTransaction tr(db);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		loop {
			try {
				wait(tenant->ready());
				wait(TenantAPI::changeLockState(&tr, tenant->id(), desiredState, lockID));
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(TenantLock* self, Database db) {
		state Optional<UID> currentValue1;
		state Optional<UID> currentValue2;
		state UID lockID1 = deterministicRandom()->randomUniqueID();
		state UID lockID2 = deterministicRandom()->randomUniqueID();
		TraceEvent("TenantLockProgress").detail("Phase", "First Tx against unlocked tenant").log();
		wait(store(currentValue1, probeTx(db, self->tenant1, currentValue1)));
		wait(store(currentValue2, probeTx(db, self->tenant2, currentValue2)));
		TraceEvent("TenantLockProgress")
		    .detail("Phase", "Lock Both tenants")
		    .detail("LockID1", lockID1)
		    .detail("LockID2", lockID2)
		    .log();
		wait(changeLockState(db, self->tenant1, TenantLockState::LOCKED, lockID1));
		wait(changeLockState(db, self->tenant2, TenantLockState::LOCKED, lockID2));
		TraceEvent("TenantLockProgress").detail("Phase", "Tx against locked tenant").log();
		wait(testExpectedError(success(probeTx(db, self->tenant1, currentValue1)),
		                       "AccessLockedTenant",
		                       tenant_locked(),
		                       Optional<bool*>(),
		                       {},
		                       Error::fromCode(error_code_internal_error)));
		TraceEvent("TenantLockProgress").detail("Phase", "Read-only Tx against locked tenant").log();
		wait(testExpectedError(success(probeTx(db, self->tenant2, currentValue2, true)),
		                       "AccessLockedTenant",
		                       tenant_locked(),
		                       Optional<bool*>(),
		                       {},
		                       Error::fromCode(error_code_internal_error)));
		// make sure we can read with read-lock-aware
		TraceEvent("TenantLockProgress")
		    .detail("Phase", "Read-only Tx against locked tenant with read-lock-aware")
		    .log();
		wait(success(probeTx(db, self->tenant1, currentValue1, true, LockAwareTx::READ)));
		// make sure we can write with a lock-aware transaction
		TraceEvent("TenantLockProgress").detail("Phase", "Read-write Tx against locked tenant with lock-aware").log();
		wait(store(currentValue2, probeTx(db, self->tenant2, currentValue2, false, LockAwareTx::READ_WRITE)));
		TraceEvent("TenantLockProgress").detail("Phase", "Unlock tenant1").log();
		wait(changeLockState(db, self->tenant1, TenantLockState::UNLOCKED, lockID1));
		TraceEvent("TenantLockProgress").detail("Phase", "Tx against unlocked tenant1").log();
		wait(store(currentValue1, probeTx(db, self->tenant1, currentValue1)));
		TraceEvent("TenantLockProgress").detail("Phase", "Tx against locked tenant2").log();
		wait(testExpectedError(success(probeTx(db, self->tenant2, currentValue2, true)),
		                       "AccessLockedTenant",
		                       tenant_locked(),
		                       Optional<bool*>(),
		                       {},
		                       Error::fromCode(error_code_internal_error)));
		TraceEvent("TenantLockProgress").detail("Phase", "Unlock tenant2 with wrong lockID").log();
		wait(testExpectedError(changeLockState(db, self->tenant2, TenantLockState::UNLOCKED, lockID1),
		                       "UnlockWithWrongID",
		                       tenant_locked(),
		                       Optional<bool*>(),
		                       {},
		                       Error::fromCode(error_code_internal_error)));
		TraceEvent("TenantLockProgress").detail("Phase", "Unlock tenant2").log();
		wait(changeLockState(db, self->tenant2, TenantLockState::UNLOCKED, lockID2));
		TraceEvent("TenantLockProgress").detail("Phase", "Second Tx against both unlocked tenants").log();
		wait(store(currentValue1, probeTx(db, self->tenant1, currentValue1)));
		wait(store(currentValue2, probeTx(db, self->tenant2, currentValue2)));
		return Void();
	}

	Future<Void> start(const Database& cx) override {
		if (clientId == 0) {
			return _start(this, cx);
		}
		return Void();
	}
	Future<bool> check(const Database& cx) override { return true; }

private:
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

} // namespace

WorkloadFactory<TenantLock> TenantLockFactory;