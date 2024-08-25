/*
 * FastTriggeredWatches.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct FastTriggeredWatchesWorkload : TestWorkload {
	static constexpr auto NAME = "FastTriggeredWatches";
	// Tests the time it takes for a watch to be fired after the value has changed in the storage server
	int nodes, keyBytes;
	double testDuration;
	std::vector<Future<Void>> clients;
	PerfIntCounter operations, retries;
	Value defaultValue;

	FastTriggeredWatchesWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), operations("Operations"), retries("Retries") {
		testDuration = getOption(options, "testDuration"_sr, 600.0);
		nodes = getOption(options, "nodes"_sr, 100);
		defaultValue = StringRef(format("%010d", deterministicRandom()->randomInt(0, 1000)));
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		// This test asserts that watches fire within a certain version range. Attrition will make this assertion fail
		// since it can cause recoveries which will bump the cluster version significantly
		out.emplace("Attrition");
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0)
			return _setup(cx, this);
		return Void();
	}

	ACTOR Future<Void> _setup(Database cx, FastTriggeredWatchesWorkload* self) {
		state Transaction tr(cx);

		loop {
			try {
				for (int i = 0; i < self->nodes; i += 2)
					tr.set(self->keyForIndex(i), self->defaultValue);

				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx, this);
		return Void();
	}

	ACTOR Future<Version> setter(Database cx, Key key, Optional<Value> value) {
		state ReadYourWritesTransaction tr(cx);
		// set the value of key and return the commit version
		wait(delay(deterministicRandom()->random01()));
		loop {
			try {
				if (value.present())
					tr.set(key, value.get());
				else
					tr.clear(key);
				//TraceEvent("FTWSetBegin").detail("Key", printable(key)).detail("Value", printable(value));
				wait(tr.commit());
				//TraceEvent("FTWSetEnd").detail("Key", printable(key)).detail("Value", printable(value)).detail("Ver", tr.getCommittedVersion());
				return tr.getCommittedVersion();
			} catch (Error& e) {
				//TraceEvent("FTWSetError").error(e).detail("Key", printable(key)).detail("Value", printable(value));
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(Database cx, FastTriggeredWatchesWorkload* self) {
		state double testStart = now();
		state Version lastReadVersion = 0;
		try {
			loop {
				state double getDuration = 0;
				state double watchEnd = 0;
				state bool watchCommitted = false;
				state Key setKey = self->keyForIndex(deterministicRandom()->randomInt(0, self->nodes));
				state Optional<Value> setValue;
				if (deterministicRandom()->random01() > 0.5)
					setValue = StringRef(format("%010d", deterministicRandom()->randomInt(0, 1000)));
				// Set the value at setKey to something random
				state Future<Version> setFuture = self->setter(cx, setKey, setValue);
				wait(delay(deterministicRandom()->random01()));
				state Version watchCommitVersion = 0;
				loop {
					state ReadYourWritesTransaction tr(cx);

					try {
						Optional<Value> val = wait(tr.get(setKey));
						if (watchCommitted) {
							getDuration = now() - watchEnd;
						}
						lastReadVersion = tr.getReadVersion().get();
						//TraceEvent("FTWGet").detail("Key", printable(setKey)).detail("Value", printable(val)).detail("Ver", tr.getReadVersion().get());
						// if the value is already setValue then there is no point setting a watch so break out of the
						// loop
						if (val == setValue)
							break;
						ASSERT(!watchCommitted);
						tr.addWriteConflictRange(singleKeyRange(""_sr));
						// set a watch and wait for it to be triggered (i.e for self->setter to set the value)
						state Future<Void> watchFuture = tr.watch(setKey);
						wait(tr.commit());
						watchCommitVersion = tr.getCommittedVersion();

						//TraceEvent("FTWStartWatch").detail("Key", printable(setKey));
						wait(watchFuture);
						watchEnd = now();
						watchCommitted = true;
					} catch (Error& e) {
						//TraceEvent("FTWWatchError").error(e).detail("Key", printable(setKey));
						wait(tr.onError(e));
					}
				}
				Version keySetVersion = wait(setFuture);
				int64_t versionDelta = lastReadVersion - std::max(keySetVersion, watchCommitVersion);
				//TraceEvent("FTWWatchDone").detail("Key", printable(setKey));
				// Assert that the time from setting the key to triggering the watch is no greater than 25s
				ASSERT(!watchCommitted || versionDelta >= SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT ||
				       versionDelta < SERVER_KNOBS->VERSIONS_PER_SECOND * (25 + getDuration));

				if (now() - testStart > self->testDuration)
					break;
			}
			return Void();
		} catch (Error& e) {
			TraceEvent(SevError, "FastWatchError").errorUnsuppressed(e);
			throw;
		}
	}

	Future<bool> check(Database const& cx) override {
		bool ok = true;
		for (int i = 0; i < clients.size(); i++)
			if (clients[i].isError())
				ok = false;
		clients.clear();
		return ok;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		double duration = testDuration;
		m.emplace_back("Operations/sec", operations.getValue() / duration, Averaged::False);
		m.push_back(operations.getMetric());
		m.push_back(retries.getMetric());
	}

	Key keyForIndex(uint64_t index) const {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodes;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}
};

WorkloadFactory<FastTriggeredWatchesWorkload> FastTriggeredWatchesWorkloadFactory;
