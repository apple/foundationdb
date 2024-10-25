/*
 * Watches.actor.cpp
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

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "flow/CodeProbe.h"
#include "flow/Coroutines.h"
#include "flow/DeterministicRandom.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct WatchesWorkload : TestWorkload {
	static constexpr auto NAME = "Watches";

	int nodes, keyBytes, extraPerNode;
	double testDuration;
	std::vector<Future<Void>> clients;
	PerfIntCounter cycles;
	DDSketch<double> cycleLatencies;
	std::vector<int> nodeOrder;

	WatchesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), cycles("Cycles"), cycleLatencies() {
		testDuration = getOption(options, "testDuration"_sr, 600.0);
		nodes = getOption(options, "nodeCount"_sr, 100);
		extraPerNode = getOption(options, "extraPerNode"_sr, 1000);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);

		for (int i = 0; i < nodes + 1; i++)
			nodeOrder.push_back(i);
		DeterministicRandom tempRand(1);
		tempRand.randomShuffle(nodeOrder);
	}

	Future<Void> setup(Database const& cx) override {
		// return _setup(cx, this);
		std::vector<Future<Void>> setupActors;
		for (int i = 0; i < nodes; i++)
			if (i % clientCount == clientId)
				setupActors.push_back(
				    watcherInit(cx, keyForIndex(nodeOrder[i]), keyForIndex(nodeOrder[i + 1]), extraPerNode));

		co_await waitForAll(setupActors);

		for (int i = 0; i < nodes; i++)
			if (i % clientCount == clientId)
				clients.push_back(watcher(cx, keyForIndex(nodeOrder[i]), keyForIndex(nodeOrder[i + 1]), extraPerNode));

		co_return;
	}

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return watchesWorker(cx, this);
		return Void();
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
		if (clientId == 0) {
			m.push_back(cycles.getMetric());
			m.emplace_back("Mean Latency (ms)", 1000 * cycleLatencies.mean() / nodes, Averaged::True);
		}
	}

	Key keyForIndex(uint64_t index) {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(index) / nodes;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}

	Future<Void> watcherInit(Database cx, Key watchKey, Key setKey, int extraNodes) {
		int extraLoc = 0;
		while (extraLoc < extraNodes) {
			co_await cx.run([&](Transaction* tr) -> Future<Void> {
				for (int i = 0; i < 1000 && extraLoc + i < extraNodes; i++) {
					Key extraKey = KeyRef(watchKey.toString() + format("%d", extraLoc + i));
					Value extraValue = ValueRef(std::string(100, '.'));
					tr->set(extraKey, extraValue);
					// TraceEvent("WatcherInitialSetupExtra").detail("Key", extraKey).detail("Value", extraValue);
				}
				co_await tr->commit();
				extraLoc += 1000;
				CODE_PROBE(true, "Watches workload initial setup");
				// TraceEvent("WatcherInitialSetup").detail("Watch", watchKey).detail("Ver", tr->getCommittedVersion());
			});
		}
	}

	ACTOR static Future<Void> watcher(Database cx, Key watchKey, Key setKey, int extraNodes) {
		state Optional<Optional<Value>> lastValue;

		loop {
			loop {
				state std::unique_ptr<Transaction> tr = std::make_unique<Transaction>(cx);
				try {
					state Future<Optional<Value>> setValueFuture = tr->get(setKey);
					state Optional<Value> watchValue = wait(tr->get(watchKey));
					Optional<Value> setValue = wait(setValueFuture);

					if (lastValue.present() && lastValue.get() == watchValue) {
						TraceEvent(SevError, "WatcherTriggeredWithoutChanging")
						    .detail("WatchKey", printable(watchKey))
						    .detail("SetKey", printable(setKey))
						    .detail("WatchValue", printable(watchValue))
						    .detail("SetValue", printable(setValue))
						    .detail("ReadVersion", tr->getReadVersion().get());
					}

					lastValue = Optional<Optional<Value>>();

					if (watchValue != setValue) {
						if (watchValue.present())
							tr->set(setKey, watchValue.get());
						else
							tr->clear(setKey);
						//TraceEvent("WatcherSetStart").detail("Watch", printable(watchKey)).detail("Set", printable(setKey)).detail("Value", printable( watchValue ) );
						wait(tr->commit());
						//TraceEvent("WatcherSetFinish").detail("Watch", printable(watchKey)).detail("Set", printable(setKey)).detail("Value", printable( watchValue ) ).detail("Ver", tr->getCommittedVersion());
					} else {
						//TraceEvent("WatcherWatch").detail("Watch", printable(watchKey));
						state Future<Void> watchFuture = tr->watch(makeReference<Watch>(watchKey, watchValue));
						wait(tr->commit());
						if (BUGGIFY) {
							// Make watch future outlive transaction
							tr.reset();
						}
						wait(watchFuture);
						if (watchValue.present())
							lastValue = watchValue;
					}
					break;
				} catch (Error& e) {
					if (tr != nullptr) {
						wait(tr->onError(e));
					}
				}
			}
		}
	}

	Future<Void> watchesWorker(Database cx, WatchesWorkload* self) {
		Key startKey = self->keyForIndex(self->nodeOrder[0]);
		Key endKey = self->keyForIndex(self->nodeOrder[self->nodes]);
		Optional<Value> expectedValue;
		Optional<Value> startValue;
		double startTime = now();
		double chainStartTime;
		loop {
			bool isValue = deterministicRandom()->random01() > 0.5;
			Value assignedValue = Value(deterministicRandom()->randomUniqueID().toString());
			bool firstAttempt = true;
			co_await cx.run([&](Transaction* tr) -> Future<Void> {
				co_await tr->getReadVersion();
				Optional<Value> _startValue = co_await tr->get(startKey);
				if (firstAttempt) {
					startValue = _startValue;
					firstAttempt = false;
				}
				expectedValue = Optional<Value>();
				if (startValue.present()) {
					if (isValue)
						expectedValue = assignedValue;
				} else
					expectedValue = assignedValue;

				if (expectedValue.present())
					tr->set(startKey, expectedValue.get());
				else
					tr->clear(startKey);

				co_await tr->commit();
				CODE_PROBE(expectedValue.present(), "watches workload set a key");
				CODE_PROBE(!expectedValue.present(), "watches workload clear a key");
				co_return;
			});

			chainStartTime = now();
			firstAttempt = true;
			bool finished = false;
			while (!finished) {
				co_await cx.run([&](Transaction* tr2) -> Future<Void> {
					Optional<Value> endValue = co_await tr2->get(endKey);
					if (endValue == expectedValue) {
						finished = true;
						co_return;
					}
					if (!firstAttempt || endValue != startValue) {
						TraceEvent(SevError, "WatcherError")
						    .detail("FirstAttempt", firstAttempt)
						    .detail("StartValue", printable(startValue))
						    .detail("EndValue", printable(endValue))
						    .detail("ExpectedValue", printable(expectedValue))
						    .detail("EndVersion", tr2->getReadVersion().get());
					}
					Future<Void> watchFuture = tr2->watch(makeReference<Watch>(endKey, startValue));
					co_await tr2->commit();
					co_await watchFuture;
					CODE_PROBE(true, "watcher workload watch fired");
					firstAttempt = false;
				});
			}
			self->cycleLatencies.addSample(now() - chainStartTime);
			++self->cycles;

			if (g_network->isSimulated())
				co_await delay(deterministicRandom()->random01() < 0.5 ? 0 : deterministicRandom()->random01() * 60);

			if (now() - startTime > self->testDuration)
				break;
		}
	}
};

WorkloadFactory<WatchesWorkload> WatchesWorkloadFactory;
