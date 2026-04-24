/*
 * FastTriggeredWatches.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"

struct FastTriggeredWatchesWorkload : TestWorkload {
	static constexpr auto NAME = "FastTriggeredWatches";
	// Tests the time it takes for a watch to be fired after the value has changed in the storage server
	int nodes, keyBytes;
	double testDuration;
	std::vector<Future<Void>> clients;
	PerfIntCounter operations, retries;
	Value defaultValue;

	explicit FastTriggeredWatchesWorkload(WorkloadContext const& wcx)
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

	Future<Void> _setup(Database cx, FastTriggeredWatchesWorkload* self) {
		Transaction tr(cx);

		while (true) {
			Error err;
			try {
				for (int i = 0; i < self->nodes; i += 2)
					tr.set(self->keyForIndex(i), self->defaultValue);

				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx);
		return Void();
	}

	Future<Version> setter(Database cx, Key key, Optional<Value> value) {
		ReadYourWritesTransaction tr(cx);
		// set the value of key and return the commit version
		co_await delay(deterministicRandom()->random01());
		while (true) {
			Error err;
			try {
				if (value.present())
					tr.set(key, value.get());
				else
					tr.clear(key);
				//TraceEvent("FTWSetBegin").detail("Key", printable(key)).detail("Value", printable(value));
				co_await tr.commit();
				//TraceEvent("FTWSetEnd").detail("Key", printable(key)).detail("Value", printable(value)).detail("Ver", tr.getCommittedVersion());
				co_return tr.getCommittedVersion();
			} catch (Error& e) {
				err = e;
			}
			//TraceEvent("FTWSetError").error(e).detail("Key", printable(key)).detail("Value", printable(value));
			co_await tr.onError(err);
		}
	}

	Future<Void> _start(Database cx) {
		double testStart = now();
		Version lastReadVersion = 0;
		try {
			while (true) {
				double getDuration = 0;
				double watchEnd = 0;
				bool watchCommitted = false;
				Key setKey = keyForIndex(deterministicRandom()->randomInt(0, nodes));
				Optional<Value> setValue;
				if (deterministicRandom()->random01() > 0.5)
					setValue = StringRef(format("%010d", deterministicRandom()->randomInt(0, 1000)));
				// Set the value at setKey to something random
				Future<Version> setFuture = setter(cx, setKey, setValue);
				co_await delay(deterministicRandom()->random01());
				Version watchCommitVersion = 0;
				while (true) {
					ReadYourWritesTransaction tr(cx);

					Error err;
					try {
						Optional<Value> val = co_await tr.get(setKey);
						if (watchCommitted) {
							getDuration = now() - watchEnd;
						}
						lastReadVersion = tr.getReadVersion().get();
						//TraceEvent("FTWGet").detail("Key", printable(setKey)).detail("Value", printable(val)).detail("Ver", tr.getReadVersion().get());
						// if the value is already setValue then there is no point setting a watch so break out of
						// the loop
						if (val == setValue)
							break;
						ASSERT(!watchCommitted);
						tr.addWriteConflictRange(singleKeyRange(""_sr));
						// set a watch and wait for it to be triggered (i.e for setter to set the value)
						Future<Void> watchFuture = tr.watch(setKey);
						co_await tr.commit();
						watchCommitVersion = tr.getCommittedVersion();

						//TraceEvent("FTWStartWatch").detail("Key", printable(setKey));
						co_await watchFuture;
						watchEnd = now();
						watchCommitted = true;
					} catch (Error& e) {
						err = e;
					}
					//TraceEvent("FTWWatchError").error(e).detail("Key", printable(setKey));
					if (err.isValid()) {
						co_await tr.onError(err);
					}
				}
				Version keySetVersion = co_await setFuture;
				int64_t versionDelta = lastReadVersion - std::max(keySetVersion, watchCommitVersion);
				//TraceEvent("FTWWatchDone").detail("Key", printable(setKey));
				// Assert that the time from setting the key to triggering the watch is no greater than 25s
				ASSERT(!watchCommitted || versionDelta >= SERVER_KNOBS->MAX_VERSIONS_IN_FLIGHT ||
				       versionDelta < SERVER_KNOBS->VERSIONS_PER_SECOND * (25 + getDuration));

				if (now() - testStart > testDuration)
					break;
			}
			co_return;
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
