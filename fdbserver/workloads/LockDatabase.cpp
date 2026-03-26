/*
 * LockDatabase.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbclient/ManagementAPI.h"

struct LockDatabaseWorkload : TestWorkload {
	static constexpr auto NAME = "LockDatabase";

	double lockAfter, unlockAfter;
	bool ok;
	bool onlyCheckLocked;

	LockDatabaseWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), ok(true) {
		lockAfter = getOption(options, "lockAfter"_sr, 0.0);
		unlockAfter = getOption(options, "unlockAfter"_sr, 10.0);
		onlyCheckLocked = getOption(options, "onlyCheckLocked"_sr, false);
		ASSERT(unlockAfter > lockAfter);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return onlyCheckLocked ? timeout(checkLocked(cx), 60, Void()) : lockWorker(cx);
		return Void();
	}

	Future<bool> check(Database const& cx) override { return ok; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<RangeResult> lockAndSave(Database cx, UID lockID) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				co_await lockDatabase(&tr, lockID);
				RangeResult data = co_await tr.getRange(normalKeys, 50000);
				ASSERT(!data.more);
				co_await tr.commit();
				co_return data;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> unlockAndCheck(Database cx, UID lockID, RangeResult data) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val = co_await tr.get(databaseLockedKey);
				if (!val.present())
					co_return;

				co_await unlockDatabase(&tr, lockID);
				RangeResult data2 = co_await tr.getRange(normalKeys, 50000);
				if (data.size() != data2.size()) {
					TraceEvent(SevError, "DataChangedWhileLocked")
					    .detail("BeforeSize", data.size())
					    .detail("AfterSize", data2.size());
					ok = false;
				} else if (data != data2) {
					TraceEvent(SevError, "DataChangedWhileLocked").detail("Size", data.size());
					for (int i = 0; i < data.size(); i++) {
						if (data[i] != data2[i]) {
							TraceEvent(SevError, "DataChangedWhileLocked")
							    .detail("I", i)
							    .detail("Before", printable(data[i]))
							    .detail("After", printable(data2[i]));
						}
					}
					ok = false;
				}
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> checkLocked(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Version v = co_await tr.getReadVersion();
				TraceEvent(SevError, "GotVersionWhileLocked").detail("Version", v);
				ok = false;
				co_return;
			} catch (Error& e) {
				err = e;
			}
			CODE_PROBE(err.code() == error_code_database_locked, "Database confirmed locked");
			co_await tr.onError(err);
		}
	}

	Future<Void> lockWorker(Database cx) {
		UID lockID = deterministicRandom()->randomUniqueID();
		co_await delay(lockAfter);
		RangeResult data = co_await lockAndSave(cx, lockID);
		Future<Void> checker = checkLocked(cx);
		co_await delay(unlockAfter - lockAfter);
		checker.cancel();
		co_await unlockAndCheck(cx, lockID, data);
	}
};

WorkloadFactory<LockDatabaseWorkload> LockDatabaseWorkloadFactory;
