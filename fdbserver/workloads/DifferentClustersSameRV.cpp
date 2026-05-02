/*
 * DifferentClustersSameRV.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/RunRYWTransaction.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/tester/workloads.h"
#include "flow/ApiVersion.h"
#include "flow/genericactors.actor.h"

void traceError(const char* filename, int line, Error const& e) {
	TraceEvent("DifferentClustersSameRVWorkload_Error").error(e).detail("File", filename).detail("Line", line);
}

#define TRACE_ERROR(e) traceError(__FILE__, __LINE__, e)

// A workload attempts to read from two different clusters with the same read version.
struct DifferentClustersSameRVWorkload : TestWorkload {
	static constexpr auto NAME = "DifferentClustersSameRV";
	Database originalDB;
	Database extraDB;
	double testDuration;
	double switchAfter;
	Value keyToRead;
	Value keyToWatch;
	bool switchComplete = false;

	explicit DifferentClustersSameRVWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		ASSERT(fdbSimulationPolicyState().extraDatabases.size() == 1);
		extraDB = Database::createSimulatedExtraDatabase(fdbSimulationPolicyState().extraDatabases[0]);
		testDuration = getOption(options, "testDuration"_sr, 100.0);
		switchAfter = getOption(options, "switchAfter"_sr, 50.0);
		keyToRead = getOption(options, "keyToRead"_sr, "someKey"_sr);
		keyToWatch = getOption(options, "keyToWatch"_sr, "anotherKey"_sr);
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return _setup(cx, extraDB);
	}

	static Future<Void> _setup(Database cx, Database extraDB) {
		Version rv1{ 0 };
		Version rv2{ 0 };
		Version newClusterVersion{ 0 };
		Transaction tr1(cx);
		Transaction tr2(extraDB);
		TraceEvent("DifferentClustersSameRVWorkload");

		// we want to advance the read version of both clusters so that they are roughly the same. This makes the test
		// more effective (since it's more likely that we can read from both clusters with the same version).
		while (true) {
			Error err;
			try {
				co_await (store(rv1, tr1.getReadVersion()) && store(rv2, tr2.getReadVersion()));
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await (tr1.onError(err) && tr2.onError(err));
		}
		newClusterVersion = std::max(rv1, rv2) + 10e6;
		co_await (::advanceVersion(cx, newClusterVersion) && ::advanceVersion(extraDB, newClusterVersion));
		TraceEvent("DifferentClustersSameRVWorkload_AdvancedVersion").detail("Version", newClusterVersion);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		auto switchConnFileDb = Database::createDatabase(cx->getConnectionRecord(), -1);
		originalDB = cx;
		std::vector<Future<Void>> clients = { readerClientSeparateDBs(cx, extraDB, keyToRead),
			                                  doSwitch(switchConnFileDb),
			                                  writerClient(cx, keyToRead),
			                                  writerClient(extraDB, keyToRead) };
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

	static Future<std::pair<Version, Optional<Value>>> doRead(Database cx, Value const& keyToRead) {
		Transaction tr(cx);
		while (true) {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Error err;
			try {
				Version rv = co_await tr.getReadVersion();
				Optional<Value> val1 = co_await tr.get(keyToRead);
				co_return std::make_pair(rv, val1);
			} catch (Error& e) {
				err = e;
			}
			TRACE_ERROR(err);
			co_await tr.onError(err);
		}
	}

	static Future<Void> doWrite(Database cx, Value key, Optional<Value> val) {
		Transaction tr(cx);
		while (true) {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Error err;
			try {
				if (val.present()) {
					tr.set(key, val.get());
				} else {
					tr.clear(key);
				}
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			TRACE_ERROR(err);
			co_await tr.onError(err);
		}
	}

	static Future<Void> advanceVersion(Database cx, Version v) {
		Transaction tr(cx);
		while (true) {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Error err;
			try {
				Version extraDBVersion = co_await tr.getReadVersion();
				if (extraDBVersion <= v) {
					tr.set(minRequiredCommitVersionKey, BinaryWriter::toValue(v + 1, Unversioned()));
					co_await tr.commit();
					continue;
				} else {
					co_return;
				}
			} catch (Error& e) {
				err = e;
			}
			TRACE_ERROR(err);
			co_await tr.onError(err);
		}
	}

	Future<Void> doSwitch(Database cx) {
		UID lockUid = deterministicRandom()->randomUniqueID();
		co_await delay(switchAfter);
		Future<Void> watchFuture;
		co_await runRYWTransaction(
		    cx, [this, &watchFuture](Reference<ReadYourWritesTransaction> tr) mutable -> Future<Void> {
			    watchFuture = tr->watch(keyToWatch);
			    return Void();
		    });
		co_await (lockDatabase(originalDB, lockUid) && lockDatabase(extraDB, lockUid));
		TraceEvent("DifferentClusters_LockedDatabases").log();
		std::pair<Version, Optional<Value>> read1 = co_await doRead(originalDB, keyToRead);
		Version rv = read1.first;
		Optional<Value> val1 = read1.second;
		co_await doWrite(extraDB, keyToRead, val1);
		TraceEvent("DifferentClusters_CopiedDatabase").log();
		co_await advanceVersion(extraDB, rv);
		TraceEvent("DifferentClusters_AdvancedVersion").log();
		co_await cx->switchConnectionRecord(
		    makeReference<ClusterConnectionMemoryRecord>(extraDB->getConnectionRecord()->getConnectionString()));
		TraceEvent("DifferentClusters_SwitchedConnectionFile").log();
		Transaction tr(cx);
		tr.setVersion(rv);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		Error readErr;
		bool readFailed = false;
		try {
			Optional<Value> val2 = co_await tr.get(keyToRead);
			// We read the same key at the same read version with the same db, we must get the same value (or fail to
			// read)
			ASSERT(val1 == val2);
		} catch (Error& err) {
			readErr = err;
			readFailed = true;
		}
		if (readFailed) {
			TraceEvent("DifferentClusters_ReadError").error(readErr);
			TRACE_ERROR(readErr);
			co_await tr.onError(readErr);
		}
		// In an actual switch we would call switchConnectionRecord after unlocking the database. But it's possible
		// that a storage server serves a read at |rv| even after the recovery caused by unlocking the database, and we
		// want to make that more likely for this test. So read at |rv| then unlock.
		co_await unlockDatabase(extraDB, lockUid);
		TraceEvent("DifferentClusters_UnlockedExtraDB").log();
		ASSERT(!watchFuture.isReady() || watchFuture.isError());
		co_await doWrite(extraDB, keyToWatch, Optional<Value>{ ""_sr });
		TraceEvent("DifferentClusters_WaitingForWatch").log();
		Error watchErr;
		bool watchFailed = false;
		try {
			co_await timeoutError(watchFuture, (testDuration - switchAfter) / 2);
		} catch (Error& err) {
			watchErr = err;
			watchFailed = true;
		}
		if (watchFailed) {
			TraceEvent("DifferentClusters_WatchError").error(watchErr);
			co_await tr.onError(watchErr);
		}
		TraceEvent("DifferentClusters_Done").log();
		switchComplete = true;
		co_await unlockDatabase(originalDB, lockUid); // So quietDatabase can finish
	}

	static Future<Void> writerClient(Database cx, Value const& keyToRead) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Optional<Value> value = co_await tr.get(keyToRead);
				int x = 0;
				if (value.present()) {
					BinaryReader r(value.get(), Unversioned());
					serializer(r, x);
				}
				x += 1;
				BinaryWriter w(Unversioned());
				serializer(w, x);
				tr.set(keyToRead, w.toValue());
				co_await tr.commit();
				tr.reset();
				continue;
			} catch (Error& e) {
				err = e;
			}
			TRACE_ERROR(err);
			co_await tr.onError(err);
		}
	}

	static Future<Optional<Value>> readAtVersion(Value const& keyToRead,
	                                             const char* name,
	                                             Transaction* tr,
	                                             Version version) {
		Optional<Value> res;
		try {
			tr->reset();
			tr->setVersion(version);
			res = co_await tr->get(keyToRead);
			co_return res;
		} catch (Error& e) {
			TraceEvent(name).error(e);
			throw e;
		}
	}

	static Future<Void> readerClientSeparateDBs(Database cx, Database extraDB, Value const& keyToRead) {
		Transaction tr1(cx);
		Transaction tr2(extraDB);
		Version rv1{ 0 };
		Version rv2{ 0 };
		Optional<Value> val1;
		Optional<Value> val2;
		while (true) {
			tr1.reset();
			tr2.reset();
			tr1.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr2.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Error err;
			try {
				co_await (store(rv1, tr1.getReadVersion()) && store(rv2, tr2.getReadVersion()));
				TraceEvent("DifferentClustersSameRVWorkload_GotReadVersion").detail("RV1", rv1).detail("RV2", rv2);
				Version rv = std::min(rv1, rv2);
				co_await (
				    store(val1,
				          readAtVersion(keyToRead, "DifferentClustersSameRVWorkload_Transaction1_Error", &tr1, rv)) &&
				    store(val2,
				          readAtVersion(keyToRead, "DifferentClustersSameRVWorkload_Transaction2_Error", &tr2, rv)));
				// We're reading from different db's with the same read version. We can get a different value.
				CODE_PROBE(val1.present() != val2.present() || !val1.present() || val1.get() != val2.get(),
				           "reading from different dbs with the same version");
				continue;
			} catch (Error& e) {
				err = e;
			}
			TRACE_ERROR(err);
			co_await (tr1.onError(err) && tr2.onError(err));
		}
	}
};

WorkloadFactory<DifferentClustersSameRVWorkload> DifferentClustersSameRVWorkloadFactory;
