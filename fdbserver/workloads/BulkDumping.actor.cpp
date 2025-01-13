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
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/Platform.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const std::string simulationBulkDumpFolder = joinPath("simfdb", "bulkdump");

struct BulkDumping : TestWorkload {
	static constexpr auto NAME = "BulkDumpingWorkload";
	const bool enabled;
	bool pass;

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

	BulkDumping(WorkloadContext const& wcx) : TestWorkload(wcx), enabled(true), pass(true) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

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
			} else if (keyA < keyB) {
				return Standalone(KeyRangeRef(keyA, keyB));
			} else if (keyA > keyB) {
				return Standalone(KeyRangeRef(keyB, keyA));
			} else {
				continue;
			}
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
				Optional<UID> aliveJob = wait(getAliveBulkDumpJob(&tr));
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

	ACTOR Future<Void> waitUntilLoadJobComplete(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<BulkLoadJobState> aliveJob = wait(getAliveBulkLoadJob(&tr));
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

		// Submit a bulk dump job
		state int oldBulkDumpMode = 0;
		wait(store(oldBulkDumpMode, setBulkDumpMode(cx, 1))); // Enable bulkDump
		state BulkDumpState newJob =
		    createBulkDumpJob(normalKeys, simulationBulkDumpFolder, BulkLoadType::SST, BulkLoadTransportMethod::CP);
		wait(submitBulkDumpJob(cx, newJob));
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Dump Job Submitted").detail("Job", newJob.toString());

		// Wait until the dump job completes
		wait(self->waitUntilDumpJobComplete(cx));
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Dump Job Complete").detail("Job", newJob.toString());

		// Clear database
		wait(self->clearDatabase(cx));
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Clear DB").detail("Job", newJob.toString());

		// Submit a bulk load job
		state int oldBulkLoadMode = 0;
		wait(store(oldBulkLoadMode, setBulkLoadMode(cx, 1))); // Enable bulkLoad
		state BulkLoadJobState bulkLoadJob = createBulkLoadJob(
		    newJob.getJobId(), newJob.getJobRange(), newJob.getJobRoot(), BulkLoadTransportMethod::CP);
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Load Job Submitted").detail("Job", newJob.toString());
		wait(submitBulkLoadJob(cx, bulkLoadJob));

		// Wait until the load job complete
		wait(self->waitUntilLoadJobComplete(cx));
		TraceEvent("BulkDumpingWorkLoad").detail("Phase", "Load Job Complete").detail("Job", newJob.toString());

		// Check the loaded data in DB is same as the data in DB before dumping
		std::map<Key, Value> newKvs = wait(self->getAllKVSFromDB(cx));
		if (kvs != newKvs) {
			TraceEvent(SevError, "BulkDumpingWorkLoadError").detail("KVS", kvs.size()).detail("NewKVS", newKvs.size());
			ASSERT(false);
		}
		return Void();
	}
};

WorkloadFactory<BulkDumping> BulkDumpingFactory;