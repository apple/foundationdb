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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const std::string simulationBulkDumpFolder = "bulkDump";

struct BulkDumping : TestWorkload {
	static constexpr auto NAME = "BulkDumpingWorkload";
	const bool enabled;
	bool pass;

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

	std::vector<KeyValue> generateOrderedKVS(BulkDumping* self, KeyRange range, size_t count) {
		std::set<Key> keys; // ordered
		while (keys.size() < count) {
			Standalone<StringRef> str = self->getRandomStringRef();
			Key key = range.begin.withSuffix(str);
			if (keys.contains(key)) {
				continue;
			}
			if (!range.contains(key)) {
				continue;
			}
			keys.insert(key);
		}
		std::vector<KeyValue> res;
		for (const auto& key : keys) {
			Value val = self->getRandomStringRef();
			res.push_back(Standalone(KeyValueRef(key, val)));
		}
		return res; // ordered
	}

	ACTOR Future<Void> setKeys(Database cx, std::vector<KeyValue> kvs) {
		state Transaction tr(cx);
		loop {
			try {
				for (const auto& kv : kvs) {
					tr.set(kv.key, kv.value);
				}
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> _start(BulkDumping* self, Database cx) {
		if (self->clientId != 0) {
			return Void();
		}

		std::vector<KeyValue> kvs = self->generateOrderedKVS(self, normalKeys, 1000);
		wait(self->setKeys(cx, kvs));

		state int oldBulkDumpMode = 0;
		wait(store(oldBulkDumpMode, setBulkDumpMode(cx, 1)));
		TraceEvent("BulkDumpingSetMode").detail("OldMode", oldBulkDumpMode).detail("NewMode", 1);

		BulkDumpState newTask = newBulkDumpTaskLocalSST(normalKeys, simulationBulkDumpFolder);
		TraceEvent("BulkDumpingTaskNew").detail("Task", newTask.toString());
		wait(submitBulkDumpTask(cx, newTask));
		std::vector<BulkDumpState> res = wait(getValidBulkDumpTasksWithinRange(cx, normalKeys, 100));
		for (const auto& task : res) {
			TraceEvent("BulkDumpingTaskRes").detail("Task", task.toString());
		}
		wait(delay(100.0));
		wait(delay(100.0));
		wait(delay(100.0));
		wait(delay(100.0));
		wait(delay(100.0));
		wait(delay(100.0));

		return Void();
	}
};

WorkloadFactory<BulkDumping> BulkDumpingFactory;
