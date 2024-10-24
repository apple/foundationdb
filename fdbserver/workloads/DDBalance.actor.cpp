/*
 * DDBalance.actor.cpp
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
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DDBalanceWorkload : TestWorkload {
	static constexpr auto NAME = "DDBalance";
	int actorsPerClient, nodesPerActor, moversPerClient, currentbin, binCount, writesPerTransaction,
	    keySpaceDriftFactor;
	double testDuration, warmingDelay, transactionsPerSecond;
	bool discardEdgeMeasurements;

	std::vector<Future<Void>> clients;
	PerfIntCounter bin_shifts, operations, retries;
	DDSketch<double> latencies;

	DDBalanceWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), bin_shifts("Bin_Shifts"), operations("Operations"), retries("Retries"), latencies() {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		binCount = getOption(options, "binCount"_sr, 1000);
		writesPerTransaction = getOption(options, "writesPerTransaction"_sr, 1);
		keySpaceDriftFactor = getOption(options, "keySpaceDriftFactor"_sr, 1);
		moversPerClient = std::max(getOption(options, "moversPerClient"_sr, 10), 1);
		actorsPerClient = std::max(getOption(options, "actorsPerClient"_sr, 100), 1);
		int nodes = getOption(options, "nodes"_sr, 10000);
		discardEdgeMeasurements = getOption(options, "discardEdgeMeasurements"_sr, true);
		warmingDelay = getOption(options, "warmingDelay"_sr, 0.0);
		transactionsPerSecond =
		    getOption(options, "transactionsPerSecond"_sr, 5000.0) / (clientCount * moversPerClient);

		nodesPerActor = nodes / (actorsPerClient * clientCount);

		currentbin = deterministicRandom()->randomInt(0, binCount);
	}

	Future<Void> setup(Database const& cx) override { return ddbalanceSetup(cx, this); }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR Future<Void> _start(Database cx, DDBalanceWorkload* self) {
		for (int c = 0; c < self->moversPerClient; c++)
			self->clients.push_back(timeout(self->ddBalanceMover(cx, self, c), self->testDuration, Void()));
		wait(waitForAll(self->clients));
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
		double duration = testDuration * (discardEdgeMeasurements ? 0.75 : 1.0);
		m.emplace_back("Operations/sec", operations.getValue() / duration, Averaged::False);
		m.push_back(operations.getMetric());
		m.push_back(retries.getMetric());
		m.push_back(bin_shifts.getMetric());
		m.emplace_back("Mean Latency (ms)", 1000 * latencies.mean(), Averaged::True);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * latencies.median(), Averaged::True);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * latencies.percentile(0.90), Averaged::True);
		m.emplace_back("98% Latency (ms, averaged)", 1000 * latencies.percentile(0.98), Averaged::True);
	}

	Key key(int bin, int n, int actorid, int clientid) {
		return StringRef(format("%08x%08x%08x%08x", bin, n, actorid, clientid));
	}

	Value value(int n) { return doubleToTestKey(n); }

	ACTOR Future<Void> setKeyIfNotPresent(Transaction* tr, Key key, Value val) {
		Optional<Value> f = wait(tr->get(key));
		if (!f.present())
			tr->set(key, val);
		return Void();
	}

	ACTOR Future<Void> ddbalanceSetupRange(Database cx, DDBalanceWorkload* self, int begin, int end) {
		state Transaction tr(cx);
		loop {
			try {
				std::vector<Future<Void>> setActors;
				for (int n = begin; n < end; n++) {
					int objectnum = n / self->moversPerClient;
					int moverid = n % self->moversPerClient;
					setActors.push_back(self->setKeyIfNotPresent(
					    &tr, self->key(self->currentbin, objectnum, moverid, self->clientId), self->value(objectnum)));
				}
				wait(waitForAll(setActors));
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR Future<Void> ddbalanceSetup(Database cx, DDBalanceWorkload* self) {
		state int i;
		state std::vector<int> order;

		for (int o = 0; o <= self->nodesPerActor * self->actorsPerClient / 10; o++)
			order.push_back(o * 10);

		deterministicRandom()->randomShuffle(order);
		for (i = 0; i < order.size();) {
			std::vector<Future<Void>> fs;
			for (int j = 0; j < 100 && i < order.size(); j++) {
				fs.push_back(self->ddbalanceSetupRange(cx, self, order[i], order[i] + 10));
				i++;
			}
			wait(waitForAll(fs));
		}

		if (self->warmingDelay > 0) {
			wait(timeout(databaseWarmer(cx), self->warmingDelay, Void()));
		}

		return Void();
	}

	bool shouldRecord(double clientBegin) {
		double n = now();
		return !discardEdgeMeasurements ||
		       (n > (clientBegin + testDuration * 0.125) && n < (clientBegin + testDuration * 0.875));
	}

	ACTOR Future<Void> ddBalanceWorker(Database cx,
	                                   DDBalanceWorkload* self,
	                                   int moverId,
	                                   int sourceBin,
	                                   int destinationBin,
	                                   int begin,
	                                   int end,
	                                   double clientBegin,
	                                   double* lastTime,
	                                   double delay) {
		state int i;
		state int j;
		state int moves;
		state int maxMovedAmount = 0;
		for (i = begin; i < end;) {
			wait(poisson(lastTime, delay));
			state double tstart = now();
			state Transaction tr(cx);
			loop {
				state int startvalue = i;
				moves = 0;
				try {
					for (j = 0; i < end && j < self->writesPerTransaction; j++) {
						state Key myKey = self->key(sourceBin, i, moverId, self->clientId);
						state Key nextKey = self->key(destinationBin, i, moverId, self->clientId);
						moves++;
						i++;

						Optional<Value> f = wait(tr.get(myKey));
						if (f.present()) {
							maxMovedAmount++;
							tr.set(nextKey, f.get());
							tr.clear(myKey);
						} else {
							TraceEvent("KeyNotPresent")
							    .detail("ClientId", self->clientId)
							    .detail("MoverId", moverId)
							    .detail("CurrentBin", sourceBin)
							    .detail("NextBin", destinationBin);
						}
					}
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
					if (self->shouldRecord(clientBegin))
						++self->retries;
					i = startvalue;
				}
			}

			tr = Transaction();
			if (self->shouldRecord(clientBegin)) {
				self->operations += 3 * moves;
				double latency = now() - tstart;
				self->latencies.addSample(latency);
			}
		}

		if (maxMovedAmount < end - begin) {
			TraceEvent(SevError, "LostKeys")
			    .detail("MaxMoved", maxMovedAmount)
			    .detail("ShouldHaveMoved", end - begin)
			    .detail("ClientId", self->clientId)
			    .detail("MoverId", moverId)
			    .detail("CurrentBin", sourceBin)
			    .detail("NextBin", destinationBin);
			ASSERT(false);
		}

		return Void();
	}

	ACTOR Future<Void> ddBalanceMover(Database cx, DDBalanceWorkload* self, int moverId) {
		state int currentBin = self->currentbin;
		state int nextBin = 0;
		state int key_space_drift = 0;

		state double clientBegin = now();
		state double lastTime = now();

		loop {
			nextBin = deterministicRandom()->randomInt(key_space_drift, self->binCount + key_space_drift);
			while (nextBin == currentBin)
				nextBin = deterministicRandom()->randomInt(key_space_drift, self->binCount + key_space_drift);

			std::vector<Future<Void>> fs;
			fs.reserve(self->actorsPerClient / self->moversPerClient);
			for (int i = 0; i < self->actorsPerClient / self->moversPerClient; i++)
				fs.push_back(self->ddBalanceWorker(cx,
				                                   self,
				                                   moverId,
				                                   currentBin,
				                                   nextBin,
				                                   i * self->nodesPerActor,
				                                   (i + 1) * self->nodesPerActor,
				                                   clientBegin,
				                                   &lastTime,
				                                   1.0 / self->transactionsPerSecond));
			wait(waitForAll(fs));

			currentBin = nextBin;
			key_space_drift += self->keySpaceDriftFactor;
			++self->bin_shifts;
		}
	}
};

WorkloadFactory<DDBalanceWorkload> DDBalanceWorkloadFactory;
