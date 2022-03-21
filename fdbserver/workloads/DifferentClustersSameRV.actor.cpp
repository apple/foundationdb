/*
 * DifferentClustersSameRV.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload attempts to read from two different clusters with the same read version.
struct DifferentClustersSameRVWorkload : TestWorkload {
	Database originalDB;
	Database extraDB;
	double testDuration;
	double switchAfter;
	Value keyToRead;
	Value keyToWatch;
	bool switchComplete = false;

	DifferentClustersSameRVWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		ASSERT(g_simulator.extraDB != nullptr);
		auto extraFile = makeReference<ClusterConnectionMemoryRecord>(*g_simulator.extraDB);
		extraDB = Database::createDatabase(extraFile, -1);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 100.0);
		switchAfter = getOption(options, LiteralStringRef("switchAfter"), 50.0);
		keyToRead = getOption(options, LiteralStringRef("keyToRead"), LiteralStringRef("someKey"));
		keyToWatch = getOption(options, LiteralStringRef("keyToWatch"), LiteralStringRef("anotherKey"));
	}

	std::string description() const override { return "DifferentClustersSameRV"; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		auto switchConnFileDb = Database::createDatabase(cx->getConnectionRecord(), -1);
		originalDB = cx;
		std::vector<Future<Void>> clients = { readerClientSeparateDBs(cx, this),
			                                  doSwitch(switchConnFileDb, this),
			                                  writerClient(cx, this),
			                                  writerClient(extraDB, this) };
		return success(timeout(waitForAll(clients), testDuration));
	}

	Future<bool> check(Database const& cx) override {
		if (clientId == 0 && !switchComplete) {
			TraceEvent(SevError, "DifferentClustersSwitchNotComplete").log();
			return false;
		}
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	ACTOR static Future<std::pair<Version, Optional<Value>>> doRead(Database cx,
	                                                                DifferentClustersSameRVWorkload* self) {
		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			try {
				state Version rv = wait(tr.getReadVersion());
				Optional<Value> val1 = wait(tr.get(self->keyToRead));
				return std::make_pair(rv, val1);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> doWrite(Database cx, Value key, Optional<Value> val) {
		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				if (val.present()) {
					tr.set(key, val.get());
				} else {
					tr.clear(key);
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> advanceVersion(Database cx, Version v) {
		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				Version extraDBVersion = wait(tr.getReadVersion());
				if (extraDBVersion <= v) {
					tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(v + 1, Unversioned()));
					wait(tr.commit());
				} else {
					return Void();
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> doSwitch(Database cx, DifferentClustersSameRVWorkload* self) {
		state UID lockUid = deterministicRandom()->randomUniqueID();
		wait(delay(self->switchAfter));
		state Future<Void> watchFuture;
		wait(runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) mutable -> Future<Void> {
			watchFuture = tr->watch(self->keyToWatch);
			return Void();
		}));
		wait(lockDatabase(self->originalDB, lockUid) && lockDatabase(self->extraDB, lockUid));
		TraceEvent("DifferentClusters_LockedDatabases").log();
		std::pair<Version, Optional<Value>> read1 = wait(doRead(self->originalDB, self));
		state Version rv = read1.first;
		state Optional<Value> val1 = read1.second;
		wait(doWrite(self->extraDB, self->keyToRead, val1));
		TraceEvent("DifferentClusters_CopiedDatabase").log();
		wait(advanceVersion(self->extraDB, rv));
		TraceEvent("DifferentClusters_AdvancedVersion").log();
		wait(cx->switchConnectionRecord(
		    makeReference<ClusterConnectionMemoryRecord>(self->extraDB->getConnectionRecord()->getConnectionString())));
		TraceEvent("DifferentClusters_SwitchedConnectionFile").log();
		state Transaction tr(cx);
		tr.setVersion(rv);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		try {
			Optional<Value> val2 = wait(tr.get(self->keyToRead));
			// We read the same key at the same read version with the same db, we must get the same value (or fail to
			// read)
			ASSERT(val1 == val2);
		} catch (Error& e) {
			TraceEvent("DifferentClusters_ReadError").error(e);
			wait(tr.onError(e));
		}
		// In an actual switch we would call switchConnectionRecord after unlocking the database. But it's possible
		// that a storage server serves a read at |rv| even after the recovery caused by unlocking the database, and we
		// want to make that more likely for this test. So read at |rv| then unlock.
		wait(unlockDatabase(self->extraDB, lockUid));
		TraceEvent("DifferentClusters_UnlockedExtraDB").log();
		ASSERT(!watchFuture.isReady() || watchFuture.isError());
		wait(doWrite(self->extraDB, self->keyToWatch, Optional<Value>{ LiteralStringRef("") }));
		TraceEvent("DifferentClusters_WaitingForWatch").log();
		try {
			wait(timeoutError(watchFuture, (self->testDuration - self->switchAfter) / 2));
		} catch (Error& e) {
			TraceEvent("DifferentClusters_WatchError").error(e);
			wait(tr.onError(e));
		}
		TraceEvent("DifferentClusters_Done").log();
		self->switchComplete = true;
		wait(unlockDatabase(self->originalDB, lockUid)); // So quietDatabase can finish
		return Void();
	}

	ACTOR static Future<Void> writerClient(Database cx, DifferentClustersSameRVWorkload* self) {
		state Transaction tr(cx);
		loop {
			try {
				Optional<Value> value = wait(tr.get(self->keyToRead));
				int x = 0;
				if (value.present()) {
					BinaryReader r(value.get(), Unversioned());
					serializer(r, x);
				}
				x += 1;
				BinaryWriter w(Unversioned());
				serializer(w, x);
				tr.set(self->keyToRead, w.toValue());
				wait(tr.commit());
				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> readerClientSeparateDBs(Database cx, DifferentClustersSameRVWorkload* self) {
		loop {
			state Transaction tr1(cx);
			state Transaction tr2(self->extraDB);
			tr1.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr2.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			try {
				wait(success(tr1.getReadVersion()) && success(tr2.getReadVersion()));
				state Version rv = std::min(tr1.getReadVersion().get(), tr2.getReadVersion().get());
				tr1.reset();
				tr2.reset();
				tr1.setVersion(rv);
				tr2.setVersion(rv);
				state Future<Optional<Value>> val1 = tr1.get(self->keyToRead);
				state Future<Optional<Value>> val2 = tr2.get(self->keyToRead);
				wait(success(val1) && success(val2));
				// We're reading from different db's with the same read version. We can get a different value.
				TEST(val1.get() != val2.get()); // reading from different dbs with the same version
			} catch (Error& e) {
				wait(tr1.onError(e) && tr2.onError(e));
			}
		}
	}
};

WorkloadFactory<DifferentClustersSameRVWorkload> DifferentClustersSameRVWorkloadFactory("DifferentClustersSameRV");
