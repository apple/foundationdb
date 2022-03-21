/*
 * Storefront.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Storefront workload will maintain 2 tables: one for orders and one for items
// Items table will have an entry for each item and the current total of "unfilled" orders
// Orders table will have an entry for each order with a 16-character representation list of each item ordered

typedef uint64_t orderID;

struct StorefrontWorkload : TestWorkload {
	double testDuration, transactionsPerSecond, minExpectedTransactionsPerSecond;
	int actorCount, itemCount, maxOrderSize;
	// bool isFulfilling;

	std::vector<Future<Void>> clients;
	std::map<orderID, std::map<int, int>> orders;
	PerfIntCounter transactions, retries, spuriousCommitFailures;
	PerfDoubleCounter totalLatency;

	StorefrontWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"),
	    spuriousCommitFailures("Spurious Commit Failures"), totalLatency("Total Latency") {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 1000.0);
		actorCount =
		    getOption(options, LiteralStringRef("actorsPerClient"), std::max((int)(transactionsPerSecond / 100), 1));
		maxOrderSize = getOption(options, LiteralStringRef("maxOrderSize"), 20);
		itemCount =
		    getOption(options, LiteralStringRef("itemCount"), transactionsPerSecond * clientCount * maxOrderSize);
		minExpectedTransactionsPerSecond =
		    transactionsPerSecond * getOption(options, LiteralStringRef("expectedRate"), 0.9);
	}

	std::string description() const override { return "StorefrontWorkload"; }

	Future<Void> setup(Database const& cx) override { return bulkSetup(cx, this, itemCount, Promise<double>()); }

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorCount; c++)
			clients.push_back(orderingClient(cx->clone(), this, actorCount / transactionsPerSecond));
		/*if(isFulfilling)
		    for(int c=0; c<actorCount; c++)
		            clients.push_back(
		                fulfillmentClient( cx->clone(), this, actorCount / transactionsPerSecond, itemCount ) );*/
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		int errors = 0;
		for (int c = 0; c < clients.size(); c++)
			if (clients[c].isError()) {
				errors++;
				TraceEvent(SevError, "TestFailure").error(clients[c].getError()).detail("Reason", "ClientError");
			}
		clients.clear();
		return inventoryCheck(cx->clone(), this, !errors);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
		m.emplace_back("Avg Latency (ms)", 1000 * totalLatency.getValue() / transactions.getValue(), Averaged::True);
	}

	/*static inline orderID valueToOrderID( const StringRef& v ) {
	    orderID x = 0;
	    sscanf( v.toString().c_str(), "%llx", &x );
	    return x;
	}*/

	static inline int valueToInt(const StringRef& v) {
		int x = 0;
		sscanf(v.toString().c_str(), "%d", &x);
		return x;
	}

	Key keyForIndex(int n) { return itemKey(n); }
	Key itemKey(int item) { return StringRef(format("/items/%016d", item)); }
	Key orderKey(orderID order) { return StringRef(format("/orders/%016llx", order)); }
	Value itemValue(int count) { return StringRef(format("%d", count)); }

	Standalone<KeyValueRef> operator()(int n) { return KeyValueRef(itemKey(n), itemValue(0)); }

	ACTOR Future<Void> itemUpdater(Transaction* tr, StorefrontWorkload* self, int item, int quantity) {
		state Key iKey = self->itemKey(item);
		Optional<Value> val = wait(tr->get(iKey));
		if (!val.present()) {
			TraceEvent(SevError, "StorefrontItemMissing")
			    .detail("Key", printable(iKey))
			    .detail("Item", item)
			    .detail("Version", tr->getReadVersion().get())
			    .detailf("From", "%016llx", debug_lastLoadBalanceResultEndpointToken);
			ASSERT(val.present());
		}
		int currentCount = valueToInt(val.get());
		tr->set(iKey, self->itemValue(currentCount + quantity));
		return Void();
	}

	ACTOR Future<Void> orderingClient(Database cx, StorefrontWorkload* self, double delay) {
		state double lastTime = now();
		try {
			loop {
				wait(poisson(&lastTime, delay));

				state double tstart = now();
				state int itemsToOrder = deterministicRandom()->randomInt(1, self->maxOrderSize);
				state orderID id = deterministicRandom()->randomUniqueID().hash();
				state Key orderKey = self->orderKey(id);
				state Transaction tr(cx);
				loop {
					try {
						Optional<Value> order = wait(tr.get(orderKey));
						if (order.present()) {
							++self->spuriousCommitFailures;
							break; // the order was already committed
						}

						// pick items
						state std::map<int, int> items;
						for (int i = 0; i < itemsToOrder; i++)
							items[deterministicRandom()->randomInt(0, self->itemCount)]++;

						// create "value"
						state std::vector<int> itemList;
						std::map<int, int>::iterator it;
						state std::vector<Future<Void>> updaters;
						for (it = items.begin(); it != items.end(); it++) {
							for (int i = 0; i < it->second; i++)
								itemList.push_back(it->first);
							updaters.push_back(self->itemUpdater(&tr, self, it->first, it->second));
						}
						wait(waitForAll(updaters));
						updaters.clear();

						// set value for the order
						BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
						wr << itemList;
						tr.set(orderKey, wr.toValue());

						wait(tr.commit());
						self->orders[id] = items; // save this in a local list to test durability
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
					++self->retries;
				}
				++self->transactions;
				self->totalLatency += now() - tstart;
			}
		} catch (Error& e) {
			TraceEvent(SevError, "OrderingClient").error(e);
			throw;
		}
	}

	ACTOR Future<std::vector<int>> orderAccumulator(Database cx, StorefrontWorkload* self, KeyRangeRef keyRange) {
		state std::vector<int> result(self->itemCount);
		state Transaction tr(cx);
		state int fetched = 10000;
		state KeySelectorRef begin = firstGreaterThan(keyRange.begin);
		state KeySelectorRef end = lastLessThan(keyRange.end);
		while (fetched == 10000) {
			RangeResult values = wait(tr.getRange(begin, end, 10000));
			int orderIdx;
			for (orderIdx = 0; orderIdx < values.size(); orderIdx++) {
				std::vector<int> saved;
				BinaryReader br(values[orderIdx].value, AssumeVersion(g_network->protocolVersion()));
				br >> saved;
				for (int c = 0; c < saved.size(); c++)
					result[saved[c]]++;
			}
			fetched = values.size();
			begin = begin + fetched;
		}
		return result;
	}

	ACTOR Future<bool> tableBalancer(Database cx, StorefrontWorkload* self, KeyRangeRef keyRange) {
		state std::vector<Future<std::vector<int>>> accumulators;
		accumulators.push_back(
		    self->orderAccumulator(cx, self, KeyRangeRef(LiteralStringRef("/orders/f"), LiteralStringRef("/orders0"))));
		for (int c = 0; c < 15; c++)
			accumulators.push_back(self->orderAccumulator(
			    cx, self, KeyRangeRef(Key(format("/orders/%x", c)), Key(format("/orders/%x", c + 1)))));

		Transaction tr(cx);
		state Future<RangeResult> values =
		    tr.getRange(KeyRangeRef(self->itemKey(0), self->itemKey(self->itemCount)), self->itemCount + 1);

		wait(waitForAll(accumulators));
		state std::vector<int> totals(self->itemCount);
		for (int c = 0; c < accumulators.size(); c++) {
			std::vector<int> subTotals = accumulators[c].get();
			for (int i = 0; i < subTotals.size(); i++)
				totals[i] += subTotals[i];
		}

		RangeResult inventory = wait(values);
		for (int c = 0; c < inventory.size(); c++) {
			if (self->valueToInt(inventory[c].value) != totals[c]) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "OrderTotalMismatch")
				    .detail("Item", c)
				    .detail("OrderTotal", totals[c])
				    .detail("RecordedInventory", self->valueToInt(inventory[c].value));
				return false;
			}
		}
		return true;
	}

	ACTOR Future<bool> orderChecker(Database cx, StorefrontWorkload* self, std::vector<orderID> ids) {
		state Transaction tr(cx);
		state int idx = 0;
		loop {
			try {
				for (; idx < ids.size(); idx++) {
					state orderID id = ids[idx];
					Optional<Value> val = wait(tr.get(self->orderKey(id)));
					if (!val.present()) {
						TraceEvent(SevError, "TestFailure").detail("Reason", "OrderNotPresent").detail("OrderID", id);
						return false;
					}
					std::vector<int> itemList;
					std::map<int, int>::iterator it;
					for (it = self->orders[id].begin(); it != self->orders[id].end(); it++) {
						for (int i = 0; i < it->second; i++)
							itemList.push_back(it->first);
					}
					BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
					wr << itemList;
					if (wr.toValue() != val.get().toString()) {
						TraceEvent(SevError, "TestFailure")
						    .detail("Reason", "OrderContentsMismatch")
						    .detail("OrderID", id);
						return false;
					}
				}
				return true;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<bool> inventoryCheck(Database cx, StorefrontWorkload* self, bool ok) {
		state std::vector<Future<bool>> checkers;
		state std::map<orderID, std::map<int, int>>::iterator it(self->orders.begin());
		while (it != self->orders.end()) {
			for (int a = 0; a < self->actorCount && it != self->orders.end(); a++) {
				std::vector<orderID> orderIDs;
				for (int i = 0; i < 100 && it != self->orders.end(); i++) {
					orderIDs.push_back(it->first);
					it++;
				}
				checkers.push_back(self->orderChecker(cx->clone(), self, orderIDs));
			}
			wait(waitForAll(checkers));
			for (int c = 0; c < checkers.size(); c++)
				ok = ok && !checkers[c].isError() && checkers[c].isReady() && checkers[c].get();
			checkers.clear();
		}

		// FIXME: match order table with inventory table

		return ok;
	}
};

WorkloadFactory<StorefrontWorkload> StorefrontWorkloadFactory("Storefront");
