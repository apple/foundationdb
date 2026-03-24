/*
 * Inventory.cpp
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
#include "fdbserver/tester/workloads.actor.h"

// SOMEDAY: Make this actually run on multiple clients

struct InventoryTestWorkload : TestWorkload {
	static constexpr auto NAME = "InventoryTest";
	std::map<Key, int> minExpectedResults,
	    maxExpectedResults; // Destroyed last, since it's used in actor cancellation of InventoryTestClient(Actor)

	int actorCount, productsPerWrite, nProducts;
	double testDuration, transactionsPerSecond, fractionWriteTransactions;
	std::vector<Future<Void>> clients;

	PerfIntCounter transactions, retries;
	PerfDoubleCounter totalLatency;

	InventoryTestWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), totalLatency("Latency") {
		actorCount = getOption(options, "actorCount"_sr, 500);
		nProducts = getOption(options, "nProducts"_sr, 100000);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 10000);
		fractionWriteTransactions = getOption(options, "fractionWriteTransactions"_sr, 0.01);
		productsPerWrite = getOption(options, "productsPerWrite"_sr, 2);
	}

	Future<Void> start(Database const& cx) override {
		if (clientId)
			return Void();
		for (int c = 0; c < actorCount; c++)
			clients.push_back(timeout(
			    inventoryTestClient(
			        cx->clone(), this, actorCount / transactionsPerSecond, fractionWriteTransactions, productsPerWrite),
			    testDuration,
			    Void()));
		return waitForAll(clients);
	}

	int failures() const {
		int failures = 0;
		for (int c = 0; c < clients.size(); c++)
			if (clients[c].isReady() && clients[c].isError()) {
				++failures;
			}
		return failures;
	}

	Future<bool> check(Database const& cx) override {
		if (clientId)
			return true;
		return inventoryTestCheck(cx->clone(), this);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Client Failures", failures(), Averaged::False);
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.emplace_back("Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), Averaged::True);
		m.emplace_back("Read rows/simsec (approx)",
		               transactions.getValue() *
		                   (2 * fractionWriteTransactions + 1 * (1.0 - fractionWriteTransactions)) / testDuration,
		               Averaged::True);
		m.emplace_back("Write rows/simsec (approx)",
		               transactions.getValue() * 2 * fractionWriteTransactions / testDuration,
		               Averaged::True);
	}

	Key chooseProduct() const {
		int p = deterministicRandom()->randomInt(0, nProducts);
		return doubleToTestKey((double)p / nProducts);
		// std::string s = std::string(1,'a' + (p%26)) + format("%d",p/26);
		/*int c = deterministicRandom()->randomInt(0,10);
		s += ',';
		for(int i=0; i<c; i++)
		    s += format("%d", c);
		s += '.';*/
		// return s;
	}

	Future<bool> inventoryTestCheck(Database cx, InventoryTestWorkload* self) {
		if (self->failures()) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were client failures.");
			co_return false;
		}
		/*if (self->transactions.getValue() < .9 * self->transactionsPerSecond * self->testDuration) {
		    TraceEvent(SevError, "TestFailure").detail("Reason", "Less than 90% desired transaction rate.");
		    return false;
		}*/
		self->clients.clear();

		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				RangeResult data = co_await tr.getRange(
				    firstGreaterOrEqual(doubleToTestKey(0)), firstGreaterOrEqual(doubleToTestKey(1)), self->nProducts);

				std::map<Key, int> actualResults;
				for (int i = 0; i < data.size(); i++)
					actualResults[data[i].key] = atoi(data[i].value.toString().c_str());
				for (auto i = self->minExpectedResults.begin(); i != self->minExpectedResults.end(); ++i)
					actualResults[i->first];
				bool error = false;
				for (auto i = actualResults.begin(); i != actualResults.end(); ++i)
					if (i->second < self->minExpectedResults[i->first] ||
					    i->second > self->maxExpectedResults[i->first]) {
						if (!error)
							TraceEvent(SevError, "TestFailure").detail("Reason", "Incorrect results.");
						error = true;
						std::string str;
						for (int d = 0; d < data.size(); d++)
							if (data[d].key == i->first)
								str = data[d].value.toString();
						TraceEvent(SevError, "IncorrectTestResult")
						    .detail("Key", printable(i->first))
						    .detail("ActualValue", i->second)
						    .detail("ActualValueString", str)
						    .detail("MinExpected", self->minExpectedResults[i->first])
						    .detail("MaxExpected", self->maxExpectedResults[i->first]);
					}
				if (error)
					co_return false;
				co_return true;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> inventoryTestWrite(Transaction* tr, Key key) {
		Optional<Value> val = co_await tr->get(key);
		int count = !val.present() ? 0 : atoi(val.get().toString().c_str());
		ASSERT(count >= 0 && count < 1000000);
		tr->set(key, format("%d", count + 1));
	}

	Future<Void> inventoryTestClient(Database cx,
	                                 InventoryTestWorkload* self,
	                                 double transactionDelay,
	                                 double fractionWriteTransactions,
	                                 int productsPerWrite) {
		double lastTime = now();
		while (true) {
			co_await poisson(&lastTime, transactionDelay);
			double st = now();
			Transaction tr(cx);
			if (deterministicRandom()->random01() < fractionWriteTransactions) {
				std::set<Key> products;
				for (int i = 0; i < productsPerWrite; i++)
					products.insert(self->chooseProduct());
				for (auto p = products.begin(); p != products.end(); ++p)
					self->maxExpectedResults[*p]++;
				while (1) {
					std::vector<Future<Void>> todo;
					for (auto p = products.begin(); p != products.end(); ++p)
						todo.push_back(self->inventoryTestWrite(&tr, *p));
					Error err;
					try {
						try {
							co_await waitForAll(todo);
						} catch (Error& e) {
							if (e.code() == error_code_actor_cancelled)
								for (auto p = products.begin(); p != products.end(); ++p)
									self->maxExpectedResults[*p]--;
							throw e;
						}
						co_await tr.commit();
						break;
					} catch (Error& e) {
						err = e;
					}
					co_await tr.onError(err);
					++self->retries;
					for (auto p = products.begin(); p != products.end(); ++p)
						self->maxExpectedResults[*p]++;
				}
				for (auto p = products.begin(); p != products.end(); ++p)
					self->minExpectedResults[*p]++;
			} else {
				while (true) {
					Error err;
					try {
						Optional<Value> val = co_await tr.get(self->chooseProduct());
						break;
					} catch (Error& e) {
						err = e;
					}
					co_await tr.onError(err);
				}
			}
			self->totalLatency += now() - st;
			++self->transactions;
		}
	}
};

WorkloadFactory<InventoryTestWorkload> InventoryTestWorkloadFactory;
