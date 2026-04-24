/*
 * SkewedReadWrite.cpp
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

#include <boost/lexical_cast.hpp>
#include <utility>
#include <vector>

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"
#include "ReadWriteWorkload.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/TDMetric.h"
#include "fdbclient/RunRYWTransaction.h"

struct SkewedReadWriteWorkload : ReadWriteCommon {
	static constexpr auto NAME = "SkewedReadWrite";
	// server based hot traffic setting
	int skewRound = 0; // skewDuration = ceil(testDuration / skewRound)
	double hotServerFraction = 0, hotServerShardFraction = 1.0; // set > 0 to issue hot key based on shard map
	double hotServerReadFrac, hotServerWriteFrac; // hot many traffic goes to hot servers
	double hotReadWriteServerOverlap; // the portion of intersection of write and hot server

	// hot server state
	using IndexRangeVec = std::vector<std::pair<int64_t, int64_t>>;
	// keyForIndex generate key from index. So for a shard range, recording the start and end is enough
	std::vector<std::pair<UID, IndexRangeVec>> serverShards; // storage server and the shards it owns
	std::map<UID, StorageServerInterface> serverInterfaces;
	int hotServerCount = 0, currentHotRound = -1;

	SkewedReadWriteWorkload(WorkloadContext const& wcx) : ReadWriteCommon(wcx) {
		descriptionString = getOption(options, "description"_sr, "SkewedReadWrite"_sr);
		hotServerFraction = getOption(options, "hotServerFraction"_sr, 0.2);
		hotServerShardFraction = getOption(options, "hotServerShardFraction"_sr, 1.0);
		hotReadWriteServerOverlap = getOption(options, "hotReadWriteServerOverlap"_sr, 0.0);
		skewRound = getOption(options, "skewRound"_sr, 1);
		hotServerReadFrac = getOption(options, "hotServerReadFrac"_sr, 0.8);
		hotServerWriteFrac = getOption(options, "hotServerWriteFrac"_sr, 0.0);
		ASSERT((hotServerReadFrac >= hotServerFraction || hotServerWriteFrac >= hotServerFraction) && skewRound > 0);
	}

	void debugPrintServerShards() const {
		std::cout << std::hex;
		for (const auto& it : this->serverShards) {
			std::cout << serverInterfaces.at(it.first).address().toString() << ": [";
			for (auto p : it.second) {
				std::cout << "[" << p.first << "," << p.second << "], ";
			}
			std::cout << "] \n";
		}
	}

	// for each boundary except the last one in boundaries, found the first existed key generated from keyForIndex as
	// beginIdx, found the last existed key generated from keyForIndex the endIdx.
	Future<IndexRangeVec> convertKeyBoundaryToIndexShard(Database cx, Standalone<VectorRef<KeyRef>> boundaries) {
		IndexRangeVec res;
		for (int i = 0; i < boundaries.size() - 1; ++i) {
			KeyRangeRef currentShard = KeyRangeRef(boundaries[i], boundaries[i + 1]);
			// std::cout << currentShard.toString() << "\n";
			std::vector<RangeResult> ranges = co_await runRYWTransaction(
			    cx, [currentShard](Reference<ReadYourWritesTransaction> tr) -> Future<std::vector<RangeResult>> {
				    std::vector<Future<RangeResult>> f;
				    f.push_back(tr->getRange(currentShard, 1, Snapshot::False, Reverse::False));
				    f.push_back(tr->getRange(currentShard, 1, Snapshot::False, Reverse::True));
				    return getAll(f);
			    });
			ASSERT(ranges[0].size() == 1 && ranges[1].size() == 1);
			res.emplace_back(indexForKey(ranges[0][0].key), indexForKey(ranges[1][0].key));
		}

		ASSERT(res.size() == boundaries.size() - 1);
		co_return res;
	}

	Future<Void> updateServerShards(Database cx) {
		RangeResult serverList;
		RangeResult range;
		Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));
		while (true) {
			// read in transaction to ensure two key ranges are transactionally consistent
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Future<RangeResult> serverListF = tr->getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
				Future<RangeResult> rangeF = tr->getRange(serverKeysRange, CLIENT_KNOBS->TOO_MANY);
				serverList = co_await serverListF;
				range = co_await rangeF;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
		// decode server interfaces
		serverInterfaces.clear();
		for (int i = 0; i < serverList.size(); i++) {
			auto ssi = decodeServerListValue(serverList[i].value);
			serverInterfaces.emplace(ssi.id(), ssi);
		}
		// clear self->serverShards
		serverShards.clear();

		// leftEdge < workloadBegin < workloadEnd
		Key workloadBegin = keyForIndex(0), workloadEnd = keyForIndex(nodeCount);
		Key leftEdge(allKeys.begin);
		std::vector<UID> leftServer; // left server owns the range [leftEdge, workloadBegin)
		KeyRangeRef workloadRange(workloadBegin, workloadEnd);
		std::map<Key, std::vector<UID>> beginServers; // begin index to server ID

		for (auto kv = range.begin(); kv != range.end(); kv++) {
			if (serverHasKey(kv->value)) {
				auto [id, key] = serverKeysDecodeServerBegin(kv->key);

				if (workloadRange.contains(key)) {
					beginServers[key].push_back(id);
				} else if (workloadBegin > key && key > leftEdge) { // update left boundary
					leftEdge = key;
					leftServer.clear();
				}

				if (key == leftEdge) {
					leftServer.push_back(id);
				}
			}
		}
		ASSERT(beginServers.empty() || beginServers.begin()->first >= workloadBegin);
		// handle the left boundary
		if (beginServers.empty() || beginServers.begin()->first > workloadBegin) {
			beginServers[workloadBegin] = leftServer;
		}
		Standalone<VectorRef<KeyRef>> keyBegins;
		for (auto p = beginServers.begin(); p != beginServers.end(); ++p) {
			keyBegins.push_back(keyBegins.arena(), p->first);
		}
		// deep count because wait below will destruct workloadEnd
		keyBegins.push_back_deep(keyBegins.arena(), workloadEnd);

		IndexRangeVec indexShards = co_await convertKeyBoundaryToIndexShard(cx, keyBegins);
		ASSERT(beginServers.size() == indexShards.size());
		// sort shard begin idx
		// build self->serverShards, starting from the left shard
		std::map<UID, IndexRangeVec> serverShards;
		int i = 0;
		for (auto p = beginServers.begin(); p != beginServers.end(); ++p) {
			for (int j = 0; j < p->second.size(); ++j) {
				serverShards[p->second[j]].emplace_back(indexShards[i]);
			}
			++i;
		}
		// self->serverShards is ordered by UID
		for (const auto& it : serverShards) {
			this->serverShards.emplace_back(it);
		}
		//		if (self->clientId == 0) {
		//			self->debugPrintServerShards();
		//		}
	}

	template <class Trans>
	Future<Void> readOp(Trans* tr, std::vector<int64_t> keys, SkewedReadWriteWorkload* self, bool shouldRecord) {
		if (keys.empty())
			co_return;

		std::vector<Future<Void>> readers;
		for (int op = 0; op < keys.size(); op++) {
			++self->totalReadsMetric;
			readers.push_back(self->logLatency(tr->get(self->keyForIndex(keys[op])), shouldRecord));
		}

		co_await waitForAll(readers);
	}

	void startReadWriteClients(Database cx, std::vector<Future<Void>>& clients) {
		clientBegin = now();
		for (int c = 0; c < actorCount; c++) {
			Future<Void> worker;
			if (useRYW)
				worker =
				    randomReadWriteClient<ReadYourWritesTransaction>(cx, this, actorCount / transactionsPerSecond, c);
			else
				worker = randomReadWriteClient<Transaction>(cx, this, actorCount / transactionsPerSecond, c);
			clients.push_back(worker);
		}
	}

	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> clients;
		if (enableReadLatencyLogging)
			clients.push_back(tracePeriodically());

		co_await updateServerShards(cx);
		for (currentHotRound = 0; currentHotRound < skewRound; ++currentHotRound) {
			setHotServers();
			startReadWriteClients(cx, clients);
			co_await timeout(waitForAll(clients), testDuration / skewRound, Void());
			clients.clear();
			co_await delay(5.0);
			co_await updateServerShards(cx);
		}
	}

	// calculate hot server count
	void setHotServers() {
		hotServerCount = ceil(hotServerFraction * serverShards.size());
		std::cout << "Choose " << hotServerCount << "/" << serverShards.size() << "/" << serverInterfaces.size()
		          << " hot servers: [";
		int begin = currentHotRound * hotServerCount;
		for (int i = 0; i < hotServerCount; ++i) {
			int idx = (begin + i) % serverShards.size();
			std::cout << serverInterfaces.at(serverShards[idx].first).address().toString() << ",";
		}
		std::cout << "]\n";
	}

	int64_t getRandomKeyFromHotServer(bool hotServerRead = true) {
		ASSERT(hotServerCount > 0);
		int begin = currentHotRound * hotServerCount;
		if (!hotServerRead) {
			begin += hotServerCount * (1.0 - hotReadWriteServerOverlap); // calculate non-overlap part offset
		}
		int idx = deterministicRandom()->randomInt(begin, begin + hotServerCount) % serverShards.size();
		int shardMax = std::min(serverShards[idx].second.size(),
		                        (size_t)ceil(serverShards[idx].second.size() * hotServerShardFraction));
		int shardIdx = deterministicRandom()->randomInt(0, shardMax);
		return deterministicRandom()->randomInt64(serverShards[idx].second[shardIdx].first,
		                                          serverShards[idx].second[shardIdx].second + 1);
	}

	int64_t getRandomKey(uint64_t nodeCount, bool hotServerRead = true) {
		auto random = deterministicRandom()->random01();
		if (hotServerFraction > 0) {
			if ((hotServerRead && random < hotServerReadFrac) || (!hotServerRead && random < hotServerWriteFrac)) {
				return getRandomKeyFromHotServer(hotServerRead);
			}
		}
		return deterministicRandom()->randomInt64(0, nodeCount);
	}

	template <class Trans>
	Future<Void> randomReadWriteClient(Database cx, SkewedReadWriteWorkload* self, double delay, int clientIndex) {
		double lastTime = now();
		double GRVStartTime{ 0 };
		UID debugID;

		while (true) {
			co_await poisson(&lastTime, delay);

			double tstart = now();
			bool aTransaction = deterministicRandom()->random01() > self->alpha;

			std::vector<int64_t> keys;
			std::vector<Value> values;
			std::vector<KeyRange> extra_ranges;
			int reads = aTransaction ? self->readsPerTransactionA : self->readsPerTransactionB;
			int writes = aTransaction ? self->writesPerTransactionA : self->writesPerTransactionB;
			for (int op = 0; op < reads; op++)
				keys.push_back(self->getRandomKey(self->nodeCount));

			values.reserve(writes);
			for (int op = 0; op < writes; op++)
				values.push_back(self->randomValue());

			Trans tr(cx);

			if (tstart - self->clientBegin > self->debugTime &&
			    tstart - self->clientBegin <= self->debugTime + self->debugInterval) {
				debugID = deterministicRandom()->randomUniqueID();
				tr.debugTransaction(debugID);
				g_traceBatch.addEvent("TransactionDebug", debugID.first(), "ReadWrite.randomReadWriteClient.Before");
			} else {
				debugID = UID();
			}

			self->transactionSuccessMetric->retries = 0;
			self->transactionSuccessMetric->commitLatency = -1;

			while (true) {
				Error err;
				try {
					GRVStartTime = now();
					self->transactionFailureMetric->startLatency = -1;

					double grvLatency = now() - GRVStartTime;
					self->transactionSuccessMetric->startLatency = grvLatency * 1e9;
					self->transactionFailureMetric->startLatency = grvLatency * 1e9;
					if (self->shouldRecord())
						self->GRVLatencies.addSample(grvLatency);

					double readStart = now();
					co_await self->readOp(&tr, keys, self, self->shouldRecord());

					double readLatency = now() - readStart;
					if (self->shouldRecord())
						self->fullReadLatencies.addSample(readLatency);

					if (!writes)
						break;

					for (int op = 0; op < writes; op++)
						tr.set(self->keyForIndex(self->getRandomKey(self->nodeCount, false), false), values[op]);

					double commitStart = now();
					co_await tr.commit();

					double commitLatency = now() - commitStart;
					self->transactionSuccessMetric->commitLatency = commitLatency * 1e9;
					if (self->shouldRecord())
						self->commitLatencies.addSample(commitLatency);

					break;
				} catch (Error& e) {
					err = e;
				}
				self->transactionFailureMetric->errorCode = err.code();
				self->transactionFailureMetric->log();

				co_await tr.onError(err);

				++self->transactionSuccessMetric->retries;
				++self->totalRetriesMetric;

				if (self->shouldRecord())
					++self->retries;
			}

			if (debugID != UID())
				g_traceBatch.addEvent("TransactionDebug", debugID.first(), "ReadWrite.randomReadWriteClient.After");

			tr = Trans();

			double transactionLatency = now() - tstart;
			self->transactionSuccessMetric->totalLatency = transactionLatency * 1e9;
			self->transactionSuccessMetric->log();

			if (self->shouldRecord()) {
				if (aTransaction)
					++self->aTransactions;
				else
					++self->bTransactions;

				self->latencies.addSample(transactionLatency);
			}
		}
	}
};

WorkloadFactory<SkewedReadWriteWorkload> SkewedReadWriteWorkloadFactory;

TEST_CASE("/KVWorkload/methods/ParseKeyForIndex") {
	WorkloadContext wcx;
	wcx.clientId = 1;
	wcx.clientCount = 1;
	wcx.sharedRandomNumber = 1;

	auto wk = TestWorkloadImpl<SkewedReadWriteWorkload>(wcx);
	for (int i = 0; i < 1000; ++i) {
		auto idx = deterministicRandom()->randomInt64(0, wk.nodeCount);
		Key k = wk.keyForIndex(idx);
		auto parse = wk.indexForKey(k);
		// std::cout << parse << " " << idx << "\n";
		ASSERT(parse == idx);
	}
	for (int i = 0; i < 1000; ++i) {
		auto idx = deterministicRandom()->randomInt64(0, wk.nodeCount);
		Key k = wk.keyForIndex(idx, true);
		auto parse = wk.indexForKey(k, true);
		ASSERT(parse == idx);
	}
	return Void();
}
