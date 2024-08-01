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
#include "fdbserver/TesterInterface.actor.h"
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

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

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

	ACTOR Future<Void> _setup(Database cx, WatchesWorkload* self) {
		std::vector<Future<Void>> setupActors;
		for (int i = 0; i < self->nodes; i++)
			if (i % self->clientCount == self->clientId)
				setupActors.push_back(self->watcherInit(cx,
				                                        self->keyForIndex(self->nodeOrder[i]),
				                                        self->keyForIndex(self->nodeOrder[i + 1]),
				                                        self->extraPerNode));

		wait(waitForAll(setupActors));

		for (int i = 0; i < self->nodes; i++)
			if (i % self->clientCount == self->clientId)
				self->clients.push_back(self->watcher(cx,
				                                      self->keyForIndex(self->nodeOrder[i]),
				                                      self->keyForIndex(self->nodeOrder[i + 1]),
				                                      self->extraPerNode));

		return Void();
	}

	ACTOR static Future<Void> watcherInit(Database cx, Key watchKey, Key setKey, int extraNodes) {
		state Transaction tr(cx);
		state int extraLoc = 0;
		while (extraLoc < extraNodes) {
			try {
				for (int i = 0; i < 1000 && extraLoc + i < extraNodes; i++) {
					Key extraKey = KeyRef(watchKey.toString() + format("%d", extraLoc + i));
					Value extraValue = ValueRef(std::string(100, '.'));
					tr.set(extraKey, extraValue);
					//TraceEvent("WatcherInitialSetupExtra").detail("Key", printable(extraKey)).detail("Value", printable(extraValue));
				}
				wait(tr.commit());
				extraLoc += 1000;
				//TraceEvent("WatcherInitialSetup").detail("Watch", printable(watchKey)).detail("Ver", tr.getCommittedVersion());
			} catch (Error& e) {
				//TraceEvent("WatcherInitialSetupError").error(e).detail("ExtraLoc", extraLoc);
				wait(tr.onError(e));
			}
		}
		return Void();
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

	ACTOR static Future<Void> watchesWorker(Database cx, WatchesWorkload* self) {
		state Key startKey = self->keyForIndex(self->nodeOrder[0]);
		state Key endKey = self->keyForIndex(self->nodeOrder[self->nodes]);
		state Optional<Value> expectedValue;
		state Optional<Value> startValue;
		state double startTime = now();
		state double chainStartTime;
		loop {
			state Transaction tr(cx);
			state bool isValue = deterministicRandom()->random01() > 0.5;
			state Value assignedValue = Value(deterministicRandom()->randomUniqueID().toString());
			state bool firstAttempt = true;
			loop {
				try {
					wait(success(tr.getReadVersion()));
					Optional<Value> _startValue = wait(tr.get(startKey));
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
						tr.set(startKey, expectedValue.get());
					else
						tr.clear(startKey);

					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}

			chainStartTime = now();
			firstAttempt = true;
			loop {
				state Transaction tr2(cx);
				state bool finished = false;
				loop {
					try {
						state Optional<Value> endValue = wait(tr2.get(endKey));
						if (endValue == expectedValue) {
							finished = true;
							break;
						}
						if (!firstAttempt || endValue != startValue) {
							TraceEvent(SevError, "WatcherError")
							    .detail("FirstAttempt", firstAttempt)
							    .detail("StartValue", printable(startValue))
							    .detail("EndValue", printable(endValue))
							    .detail("ExpectedValue", printable(expectedValue))
							    .detail("EndVersion", tr2.getReadVersion().get());
						}
						state Future<Void> watchFuture = tr2.watch(makeReference<Watch>(endKey, startValue));
						wait(tr2.commit());
						wait(watchFuture);
						firstAttempt = false;
						break;
					} catch (Error& e) {
						wait(tr2.onError(e));
					}
				}
				if (finished)
					break;
			}
			self->cycleLatencies.addSample(now() - chainStartTime);
			++self->cycles;

			if (g_network->isSimulated())
				wait(delay(deterministicRandom()->random01() < 0.5 ? 0 : deterministicRandom()->random01() * 60));

			if (now() - startTime > self->testDuration)
				break;
		}
		return Void();
	}
};

WorkloadFactory<WatchesWorkload> WatchesWorkloadFactory;
