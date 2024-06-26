/*
 * WriteDuringRead.actor.cpp
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

#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/TenantManagement.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/ActorCollection.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/Atomic.h"
#include "flow/ApiVersion.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct WriteDuringReadWorkload : TestWorkload {
	static constexpr auto NAME = "WriteDuringRead";

	double testDuration, slowModeStart;
	int numOps;
	bool rarelyCommit, adjacentKeys;
	PerfIntCounter transactions, retries;
	std::map<Key, Value> memoryDatabase;
	std::map<Key, Value> lastCommittedDatabase;
	KeyRangeMap<int> changeCount;
	int minNode, nodes;
	double initialKeyDensity;
	AsyncTrigger finished;
	KeyRange conflictRange;
	std::pair<int, int> valueSizeRange;
	int maxClearSize;
	CoalescedKeyRangeMap<bool> addedConflicts;
	bool useSystemKeys;
	std::string keyPrefix;
	int64_t maximumTotalData;
	int64_t maximumDataWritten;

	int64_t dataWritten = 0;

	bool success;
	Database extraDB;
	bool useExtraDB;

	WriteDuringReadWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries"), success(true) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
		slowModeStart = getOption(options, "slowModeStart"_sr, 1000.0);
		numOps = getOption(options, "numOps"_sr, 21);
		rarelyCommit = getOption(options, "rarelyCommit"_sr, false);
		maximumTotalData = getOption(options, "maximumTotalData"_sr, 3e6);
		maximumDataWritten = getOption(options, "maximumDataWritten"_sr, std::numeric_limits<int64_t>::max());
		minNode = getOption(options, "minNode"_sr, 0);
		useSystemKeys = getOption(options, "useSystemKeys"_sr, deterministicRandom()->random01() < 0.5);
		adjacentKeys = deterministicRandom()->random01() < 0.5;
		initialKeyDensity = deterministicRandom()->random01(); // This fraction of keys are present before the first
		                                                       // transaction (and after an unknown result)
		valueSizeRange = std::make_pair(
		    0,
		    std::min<int>(deterministicRandom()->randomInt(0, 4 << deterministicRandom()->randomInt(0, 16)),
		                  CLIENT_KNOBS->VALUE_SIZE_LIMIT * 1.2));
		if (adjacentKeys) {
			nodes = std::min<int64_t>(deterministicRandom()->randomInt(1, 4 << deterministicRandom()->randomInt(0, 14)),
			                          CLIENT_KNOBS->KEY_SIZE_LIMIT * 1.2);
		} else {
			nodes = deterministicRandom()->randomInt(1, 4 << deterministicRandom()->randomInt(0, 20));
		}

		dataWritten = 0;
		int newNodes = std::min<int>(nodes, maximumTotalData / (getKeyForIndex(nodes).size() + valueSizeRange.second));
		minNode = std::max(minNode, nodes - newNodes);
		nodes = newNodes;

		CODE_PROBE(adjacentKeys && (nodes + minNode) > CLIENT_KNOBS->KEY_SIZE_LIMIT,
		           "WriteDuringReadWorkload testing large keys");

		useExtraDB = g_network->isSimulated() && !g_simulator->extraDatabases.empty();
		if (useExtraDB) {
			ASSERT(g_simulator->extraDatabases.size() == 1);
			extraDB = Database::createSimulatedExtraDatabase(g_simulator->extraDatabases[0], wcx.defaultTenant);
			useSystemKeys = false;
		}

		if (useSystemKeys && deterministicRandom()->random01() < 0.5) {
			keyPrefix = "\xff\x01";
		} else {
			keyPrefix = "\x02";
		}

		maxClearSize = 1 << deterministicRandom()->randomInt(0, 20);
		conflictRange = KeyRangeRef("\xfe"_sr, "\xfe\x00"_sr);
		if (clientId == 0)
			TraceEvent("RYWConfiguration")
			    .detail("Nodes", nodes)
			    .detail("InitialKeyDensity", initialKeyDensity)
			    .detail("AdjacentKeys", adjacentKeys)
			    .detail("ValueSizeMin", valueSizeRange.first)
			    .detail("ValueSizeMax", valueSizeRange.second)
			    .detail("MaxClearSize", maxClearSize);
	}

	ACTOR Future<Void> setupImpl(WriteDuringReadWorkload* self, Database cx) {
		// If we are operating in the default tenant but enable raw access, we should only write keys
		// in the tenant's key-space.
		if (self->useSystemKeys && cx->defaultTenant.present() && self->keyPrefix < systemKeys.begin) {
			TenantMapEntry entry = wait(TenantAPI::getTenant(cx.getReference(), cx->defaultTenant.get()));
			self->keyPrefix = entry.prefix.withSuffix(self->keyPrefix).toString();
		}
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return setupImpl(this, cx); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return loadAndRun(cx, this);
		return Void();
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
	}

	Key memoryGetKey(std::map<Key, Value>* db, KeySelector key) const {
		std::map<Key, Value>::iterator iter;
		if (key.orEqual)
			iter = db->upper_bound(key.getKey());
		else
			iter = db->lower_bound(key.getKey());

		int offset = key.offset - 1;
		while (offset > 0) {
			if (iter == db->end())
				return useSystemKeys ? allKeys.end : normalKeys.end;
			++iter;
			--offset;
		}
		while (offset < 0) {
			if (iter == db->begin())
				return allKeys.begin;
			--iter;
			++offset;
		}
		if (iter == db->end())
			return useSystemKeys ? allKeys.end : normalKeys.end;
		return iter->first;
	}

	ACTOR Future<Void> getKeyAndCompare(ReadYourWritesTransaction* tr,
	                                    KeySelector key,
	                                    Snapshot snapshot,
	                                    bool readYourWritesDisabled,
	                                    bool snapshotRYWDisabled,
	                                    WriteDuringReadWorkload* self,
	                                    bool* doingCommit,
	                                    int64_t* memLimit) {
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		//TraceEvent("WDRGetKey", randomID);
		try {
			state Key memRes = self->memoryGetKey(readYourWritesDisabled || (snapshot && snapshotRYWDisabled)
			                                          ? &self->lastCommittedDatabase
			                                          : &self->memoryDatabase,
			                                      key);
			*memLimit -= memRes.expectedSize();
			Key _res = wait(tr->getKey(key, snapshot));
			Key res = _res;
			*memLimit += memRes.expectedSize();
			if (self->useSystemKeys && res > self->getKeyForIndex(self->nodes))
				res = allKeys.end;
			if (res != memRes) {
				TraceEvent(SevError, "WDRGetKeyWrongResult", randomID)
				    .detail("Key", key.getKey())
				    .detail("Offset", key.offset)
				    .detail("OrEqual", key.orEqual)
				    .detail("Snapshot", snapshot)
				    .detail("MemoryResult", memRes)
				    .detail("DbResult", res);
				self->success = false;
			}
			return Void();
		} catch (Error& e) {
			//TraceEvent("WDRGetKeyError", randomID).error(e,true);
			if (e.code() == error_code_used_during_commit) {
				ASSERT(*doingCommit);
				return Void();
			} else if (e.code() == error_code_transaction_cancelled)
				return Void();
			throw;
		}
	}

	Standalone<VectorRef<KeyValueRef>> memoryGetRange(std::map<Key, Value>* db,
	                                                  KeySelector begin,
	                                                  KeySelector end,
	                                                  GetRangeLimits limit,
	                                                  Reverse reverse) {
		Key beginKey = memoryGetKey(db, begin);
		Key endKey = memoryGetKey(db, end);
		//TraceEvent("WDRGetRange").detail("Begin", beginKey).detail("End", endKey);
		if (beginKey >= endKey)
			return Standalone<VectorRef<KeyValueRef>>();

		auto beginIter = db->lower_bound(beginKey);
		auto endIter = db->lower_bound(endKey);

		Standalone<VectorRef<KeyValueRef>> results;
		if (reverse) {
			loop {
				if (beginIter == endIter || limit.reachedBy(results))
					break;

				--endIter;
				results.push_back_deep(results.arena(), KeyValueRef(endIter->first, endIter->second));
			}
		} else {
			for (; beginIter != endIter && !limit.reachedBy(results); ++beginIter)
				results.push_back_deep(results.arena(), KeyValueRef(beginIter->first, beginIter->second));
		}
		return results;
	}

	ACTOR Future<Void> getRangeAndCompare(ReadYourWritesTransaction* tr,
	                                      KeySelector begin,
	                                      KeySelector end,
	                                      GetRangeLimits limit,
	                                      Snapshot snapshot,
	                                      Reverse reverse,
	                                      bool readYourWritesDisabled,
	                                      bool snapshotRYWDisabled,
	                                      WriteDuringReadWorkload* self,
	                                      bool* doingCommit,
	                                      int64_t* memLimit) {
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		/*TraceEvent("WDRGetRange", randomID).detail("BeginKey", begin.getKey()).detail("BeginOffset", begin.offset).detail("BeginOrEqual", begin.orEqual)
		    .detail("EndKey", end.getKey()).detail("EndOffset", end.offset).detail("EndOrEqual", end.orEqual)
		    .detail("Limit", limit.rows).detail("Snapshot", snapshot).detail("Reverse",
		   reverse).detail("ReadYourWritesDisabled", readYourWritesDisabled);*/

		try {
			state Standalone<VectorRef<KeyValueRef>> memRes = self->memoryGetRange(
			    readYourWritesDisabled || (snapshot && snapshotRYWDisabled) ? &self->lastCommittedDatabase
			                                                                : &self->memoryDatabase,
			    begin,
			    end,
			    limit,
			    reverse);
			*memLimit -= memRes.expectedSize();
			RangeResult _res = wait(tr->getRange(begin, end, limit, snapshot, reverse));
			RangeResult res = _res;
			*memLimit += memRes.expectedSize();

			int systemKeyCount = 0;
			bool resized = false;
			if (self->useSystemKeys) {
				if (!reverse) {
					int newSize =
					    std::lower_bound(
					        res.begin(), res.end(), self->getKeyForIndex(self->nodes), KeyValueRef::OrderByKey()) -
					    res.begin();
					if (newSize != res.size()) {
						res.resize(res.arena(), newSize);
						resized = true;
					}
				} else {
					for (; systemKeyCount < res.size(); systemKeyCount++)
						if (res[systemKeyCount].key < self->getKeyForIndex(self->nodes))
							break;
					if (systemKeyCount > 0) {
						res = RangeResultRef(VectorRef<KeyValueRef>(&res[systemKeyCount], res.size() - systemKeyCount),
						                     true);
						resized = true;
					}
				}
			}

			if (!limit.hasByteLimit() && systemKeyCount == 0) {
				if (res.size() != memRes.size()) {
					TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
					    .detail("BeginKey", begin.getKey())
					    .detail("BeginOffset", begin.offset)
					    .detail("BeginOrEqual", begin.orEqual)
					    .detail("EndKey", end.getKey())
					    .detail("EndOffset", end.offset)
					    .detail("EndOrEqual", end.orEqual)
					    .detail("LimitRows", limit.rows)
					    .detail("LimitBytes", limit.bytes)
					    .detail("Snapshot", snapshot)
					    .detail("Reverse", reverse)
					    .detail("MemorySize", memRes.size())
					    .detail("DbSize", res.size())
					    .detail("ReadYourWritesDisabled", readYourWritesDisabled);

					self->success = false;
					return Void();
				}

				for (int i = 0; i < res.size(); i++) {
					if (res[i] != memRes[i]) {
						TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
						    .detail("BeginKey", begin.getKey())
						    .detail("BeginOffset", begin.offset)
						    .detail("BeginOrEqual", begin.orEqual)
						    .detail("EndKey", end.getKey())
						    .detail("EndOffset", end.offset)
						    .detail("EndOrEqual", end.orEqual)
						    .detail("LimitRows", limit.rows)
						    .detail("LimitBytes", limit.bytes)
						    .detail("Snapshot", snapshot)
						    .detail("Reverse", reverse)
						    .detail("Size", memRes.size())
						    .detail("WrongLocation", i)
						    .detail("MemoryResultKey", memRes[i].key)
						    .detail("DbResultKey", res[i].key)
						    .detail("MemoryResultValueSize", memRes[i].value.size())
						    .detail("DbResultValueSize", res[i].value.size())
						    .detail("ReadYourWritesDisabled", readYourWritesDisabled);
						self->success = false;
						return Void();
					}
				}
			} else {
				if (res.size() > memRes.size() || (res.size() < memRes.size() && !res.more) ||
				    (res.size() == 0 && res.more && !resized)) {
					TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
					    .detail("BeginKey", begin.getKey())
					    .detail("BeginOffset", begin.offset)
					    .detail("BeginOrEqual", begin.orEqual)
					    .detail("EndKey", end.getKey())
					    .detail("EndOffset", end.offset)
					    .detail("EndOrEqual", end.orEqual)
					    .detail("LimitRows", limit.rows)
					    .detail("LimitBytes", limit.bytes)
					    .detail("Snapshot", snapshot)
					    .detail("Reverse", reverse)
					    .detail("MemorySize", memRes.size())
					    .detail("DbSize", res.size())
					    .detail("ReadYourWritesDisabled", readYourWritesDisabled)
					    .detail("More", res.more)
					    .detail("SystemKeyCount", systemKeyCount);

					self->success = false;
					return Void();
				}

				for (int i = 0; i < res.size(); i++) {
					if (res[i] != memRes[i]) {
						TraceEvent(SevError, "WDRGetRangeWrongResult", randomID)
						    .detail("BeginKey", begin.getKey())
						    .detail("BeginOffset", begin.offset)
						    .detail("BeginOrEqual", begin.orEqual)
						    .detail("EndKey", end.getKey())
						    .detail("EndOffset", end.offset)
						    .detail("EndOrEqual", end.orEqual)
						    .detail("LimitRows", limit.rows)
						    .detail("LimitBytes", limit.bytes)
						    .detail("Snapshot", snapshot)
						    .detail("Reverse", reverse)
						    .detail("Size", memRes.size())
						    .detail("WrongLocation", i)
						    .detail("MemoryResultKey", memRes[i].key)
						    .detail("DbResultKey", res[i].key)
						    .detail("MemoryResultValueSize", memRes[i].value.size())
						    .detail("DbResultValueSize", res[i].value.size())
						    .detail("ReadYourWritesDisabled", readYourWritesDisabled)
						    .detail("More", res.more);
						self->success = false;
						return Void();
					}
				}
			}
			return Void();
		} catch (Error& e) {
			//TraceEvent("WDRGetRangeError", randomID).error(e,true);
			if (e.code() == error_code_used_during_commit) {
				ASSERT(*doingCommit);
				return Void();
			} else if (e.code() == error_code_transaction_cancelled)
				return Void();
			throw;
		}
	}

	Optional<Value> memoryGet(std::map<Key, Value>* db, Key key) {
		auto iter = db->find(key);
		if (iter == db->end())
			return Optional<Value>();
		else
			return iter->second;
	}

	ACTOR Future<Void> getAndCompare(ReadYourWritesTransaction* tr,
	                                 Key key,
	                                 Snapshot snapshot,
	                                 bool readYourWritesDisabled,
	                                 bool snapshotRYWDisabled,
	                                 WriteDuringReadWorkload* self,
	                                 bool* doingCommit,
	                                 int64_t* memLimit) {
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		//TraceEvent("WDRGet", randomID);
		try {
			state Optional<Value> memRes = self->memoryGet(readYourWritesDisabled || (snapshot && snapshotRYWDisabled)
			                                                   ? &self->lastCommittedDatabase
			                                                   : &self->memoryDatabase,
			                                               key);
			*memLimit -= memRes.expectedSize();
			Optional<Value> res = wait(tr->get(key, snapshot));
			*memLimit += memRes.expectedSize();
			if (res != memRes) {
				TraceEvent(SevError, "WDRGetWrongResult", randomID)
				    .detail("Key", key)
				    .detail("Snapshot", snapshot)
				    .detail("MemoryResult", memRes.present() ? memRes.get().size() : -1)
				    .detail("DbResult", res.present() ? res.get().size() : -1)
				    .detail("RywDisable", readYourWritesDisabled);
				self->success = false;
			}
			return Void();
		} catch (Error& e) {
			//TraceEvent("WDRGetError", randomID).error(e,true);
			if (e.code() == error_code_used_during_commit) {
				ASSERT(*doingCommit);
				return Void();
			} else if (e.code() == error_code_transaction_cancelled)
				return Void();
			throw;
		}
	}

	ACTOR Future<Void> watchAndCompare(ReadYourWritesTransaction* tr,
	                                   Key key,
	                                   bool readYourWritesDisabled,
	                                   WriteDuringReadWorkload* self,
	                                   bool* doingCommit,
	                                   int64_t* memLimit) {
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		// SOMEDAY: test setting a low outstanding watch limit
		if (readYourWritesDisabled) // Only tests RYW activated watches
			return Void();

		//TraceEvent("WDRWatch", randomID).detail("Key", key);
		try {
			state int changeNum = self->changeCount[key];
			state Optional<Value> memRes = self->memoryGet(&self->memoryDatabase, key);
			*memLimit -= memRes.expectedSize();

			// This if block prevents formatting issues with clang-format
			if (1) {
				choose {
					when(wait(tr->watch(key))) {
						if (changeNum == self->changeCount[key]) {
							TraceEvent(SevError, "WDRWatchWrongResult", randomID)
							    .detail("Reason", "Triggered without changing")
							    .detail("Key", key)
							    .detail("Value", changeNum)
							    .detail("DuringCommit", *doingCommit);
						}
					}
					when(wait(self->finished.onTrigger())) {
						Optional<Value> memRes2 = self->memoryGet(&self->memoryDatabase, key);
						if (memRes != memRes2) {
							TraceEvent(SevError, "WDRWatchWrongResult", randomID)
							    .detail("Reason", "Changed without triggering")
							    .detail("Key", key)
							    .detail("Value1", memRes)
							    .detail("Value2", memRes2);
						}
					}
				}
			}
			*memLimit += memRes.expectedSize();

			return Void();
		} catch (Error& e) {
			// check for transaction cancelled if the watch was not committed
			//TraceEvent("WDRWatchError", randomID).error(e,true);
			if (e.code() == error_code_used_during_commit) {
				ASSERT(*doingCommit);
				return Void();
			} else if (e.code() == error_code_transaction_cancelled)
				return Void();
			throw;
		}
	}

	ACTOR Future<Void> commitAndUpdateMemory(ReadYourWritesTransaction* tr,
	                                         WriteDuringReadWorkload* self,
	                                         bool* cancelled,
	                                         bool readYourWritesDisabled,
	                                         bool snapshotRYWDisabled,
	                                         bool readAheadDisabled,
	                                         bool useBatchPriority,
	                                         bool* doingCommit,
	                                         double* startTime,
	                                         Key timebombStr) {
		// state UID randomID = nondeterministicRandom()->randomUniqueID();
		//TraceEvent("WDRCommit", randomID);
		try {
			if (!readYourWritesDisabled && !*cancelled) {
				KeyRangeMap<bool> transactionConflicts;
				tr->getWriteConflicts(&transactionConflicts);

				auto transactionRanges = transactionConflicts.ranges();
				auto addedRanges = self->addedConflicts.ranges();
				auto transactionIter = transactionRanges.begin();
				auto addedIter = addedRanges.begin();

				bool failed = false;
				while (transactionIter != transactionRanges.end() && addedIter != addedRanges.end()) {
					if (transactionIter->begin() != addedIter->begin() ||
					    transactionIter->value() != addedIter->value()) {
						TraceEvent(SevError, "WriteConflictError")
						    .detail("TransactionKey", transactionIter->begin())
						    .detail("AddedKey", addedIter->begin())
						    .detail("TransactionVal", transactionIter->value())
						    .detail("AddedVal", addedIter->value());
						failed = true;
					}
					++transactionIter;
					++addedIter;
				}

				if (transactionIter != transactionRanges.end() || addedIter != addedRanges.end()) {
					failed = true;
				}

				if (failed) {
					TraceEvent(SevError, "WriteConflictRangeError").log();
					for (transactionIter = transactionRanges.begin(); transactionIter != transactionRanges.end();
					     ++transactionIter) {
						TraceEvent("WCRTransaction")
						    .detail("Range", transactionIter.range())
						    .detail("Value", transactionIter.value());
					}
					for (addedIter = addedRanges.begin(); addedIter != addedRanges.end(); ++addedIter) {
						TraceEvent("WCRAdded").detail("Range", addedIter.range()).detail("Value", addedIter.value());
					}
				}
			}

			state int64_t txnSize = tr->getApproximateSize();
			state std::map<Key, Value> committedDB = self->memoryDatabase;
			*doingCommit = true;
			wait(tr->commit());
			*doingCommit = false;
			self->finished.trigger();
			self->dataWritten += txnSize;

			// It's not legal to set readYourWritesDisabled after performing
			// reads on a transaction, even if all those reads have completed
			// and there are none in-flight.
			// if (readYourWritesDisabled)
			// 	tr->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);

			if (snapshotRYWDisabled)
				tr->setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);
			if (readAheadDisabled)
				tr->setOption(FDBTransactionOptions::READ_AHEAD_DISABLE);
			if (useBatchPriority)
				tr->setOption(FDBTransactionOptions::PRIORITY_BATCH);
			if (self->useSystemKeys)
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->addWriteConflictRange(self->conflictRange);
			self->addedConflicts.insert(allKeys, false);
			self->addedConflicts.insert(self->conflictRange, true);
			*startTime = now();
			tr->setOption(FDBTransactionOptions::TIMEOUT, timebombStr);

			//TraceEvent("WDRCommitSuccess", randomID).detail("CommittedVersion", tr->getCommittedVersion());
			self->lastCommittedDatabase = committedDB;

			return Void();
		} catch (Error& e) {
			//TraceEvent("WDRCommitCancelled", randomID).error(e,true);
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_transaction_cancelled ||
			    e.code() == error_code_used_during_commit)
				*cancelled = true;
			if (e.code() == error_code_actor_cancelled || e.code() == error_code_transaction_cancelled)
				throw commit_unknown_result();
			if (e.code() == error_code_transaction_too_old)
				throw not_committed();
			throw;
		}
	}

	Value getRandomValue() {
		return Value(
		    std::string(deterministicRandom()->randomInt(valueSizeRange.first, valueSizeRange.second + 1), 'x'));
	}

	// Prevent a write only transaction whose commit was previously cancelled from being reordered after this
	// transaction
	ACTOR Future<Void> writeBarrier(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

				// Write-only transactions have a self-conflict in the system keys
				tr.addWriteConflictRange(allKeys);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> loadAndRun(Database cx, WriteDuringReadWorkload* self) {
		state double startTime = now();
		loop {
			wait(self->writeBarrier(cx));

			state int i = 0;
			state int keysPerBatch =
			    std::min<int64_t>(1000,
			                      1 + CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT / 6 /
			                              (self->getKeyForIndex(self->nodes).size() + self->valueSizeRange.second));
			self->memoryDatabase = std::map<Key, Value>();
			for (; i < self->nodes; i += keysPerBatch) {
				state Transaction tr(cx);
				loop {
					try {
						if (now() - startTime > self->testDuration || self->dataWritten >= self->maximumDataWritten)
							return Void();
						if (self->useSystemKeys)
							tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

						state int64_t txnSize = 0;
						if (i == 0) {
							tr.clear(normalKeys);
						}

						int end = std::min(self->nodes, i + keysPerBatch);
						tr.clear(KeyRangeRef(self->getKeyForIndex(i), self->getKeyForIndex(end)));
						self->memoryDatabase.erase(self->memoryDatabase.lower_bound(self->getKeyForIndex(i)),
						                           self->memoryDatabase.lower_bound(self->getKeyForIndex(end)));

						for (int j = i; j < end; j++) {
							if (deterministicRandom()->random01() < self->initialKeyDensity) {
								Key key = self->getKeyForIndex(j);
								if (key.size() <= getMaxWriteKeySize(key, false)) {
									Value value = self->getRandomValue();
									value =
									    value.substr(0, std::min<int>(value.size(), CLIENT_KNOBS->VALUE_SIZE_LIMIT));
									self->memoryDatabase[key] = value;
									tr.set(key, value);
									int64_t rowSize = key.expectedSize() + value.expectedSize();
									txnSize += rowSize;
								}
							}
						}
						wait(tr.commit());
						self->dataWritten += txnSize;
						//TraceEvent("WDRInitBatch").detail("I", i).detail("CommittedVersion", tr.getCommittedVersion());
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}
			}
			self->lastCommittedDatabase = self->memoryDatabase;
			self->addedConflicts.insert(allKeys, false);
			//TraceEvent("WDRInit");

			loop {
				wait(delay(now() - startTime > self->slowModeStart ||
				                   (g_network->isSimulated() && g_simulator->speedUpSimulation)
				               ? 1.0
				               : 0.1));
				try {
					wait(self->randomTransaction(
					    (self->useExtraDB && deterministicRandom()->random01() < 0.5) ? self->extraDB : cx,
					    self,
					    startTime));
				} catch (Error& e) {
					if (e.code() != error_code_not_committed)
						throw;
					break;
				}
				if (now() - startTime > self->testDuration || self->dataWritten >= self->maximumDataWritten)
					return Void();
			}
		}
	}

	Key getRandomKey() { return getKeyForIndex(deterministicRandom()->randomInt(0, nodes)); }

	Key getKeyForIndex(int idx) {
		idx += minNode;
		if (adjacentKeys) {
			return Key(keyPrefix + (idx ? std::string(idx, '\x00') : ""));
		} else {
			return Key(keyPrefix + format("%010d", idx));
		}
	}

	Key versionStampKeyForIndex(int idx) {
		Key result = KeyRef(getKeyForIndex(idx).toString() + std::string(14, '\x00'));
		int32_t pos = deterministicRandom()->randomInt(0, result.size() - 13);
		pos = littleEndian32(pos);
		uint8_t* data = mutateString(result);
		memcpy(data + result.size() - sizeof(int32_t), &pos, sizeof(int32_t));
		return result;
	}

	Key getRandomVersionStampKey() { return versionStampKeyForIndex(deterministicRandom()->randomInt(0, nodes)); }

	KeySelector getRandomKeySelector() {
		int scale = 1 << deterministicRandom()->randomInt(0, 14);
		return KeySelectorRef(
		    getRandomKey(), deterministicRandom()->random01() < 0.5, deterministicRandom()->randomInt(-scale, scale));
	}

	GetRangeLimits getRandomLimits() {
		int kind = deterministicRandom()->randomInt(0, 3);
		return GetRangeLimits(
		    (kind & 1) ? GetRangeLimits::ROW_LIMIT_UNLIMITED
		               : deterministicRandom()->randomInt(0, 1 << deterministicRandom()->randomInt(1, 10)),
		    (kind & 2) ? GetRangeLimits::BYTE_LIMIT_UNLIMITED
		               : deterministicRandom()->randomInt(0, 1 << deterministicRandom()->randomInt(1, 15)));
	}

	KeyRange getRandomRange(int sizeLimit) {
		int startLocation = deterministicRandom()->randomInt(0, nodes);
		int scale = deterministicRandom()->randomInt(
		    0, deterministicRandom()->randomInt(2, 5) * deterministicRandom()->randomInt(2, 5));
		int endLocation = startLocation + deterministicRandom()->randomInt(
		                                      0, 1 + std::min(sizeLimit, std::min(nodes - startLocation, 1 << scale)));

		return KeyRangeRef(getKeyForIndex(startLocation), getKeyForIndex(endLocation));
	}

	Value applyAtomicOp(Optional<StringRef> existingValue, Value value, MutationRef::Type type) {
		Arena arena;
		if (type == MutationRef::SetValue)
			return value;
		else if (type == MutationRef::AddValue)
			return doLittleEndianAdd(existingValue, value, arena);
		else if (type == MutationRef::AppendIfFits)
			return doAppendIfFits(existingValue, value, arena);
		else if (type == MutationRef::And)
			return doAndV2(existingValue, value, arena);
		else if (type == MutationRef::Or)
			return doOr(existingValue, value, arena);
		else if (type == MutationRef::Xor)
			return doXor(existingValue, value, arena);
		else if (type == MutationRef::Max)
			return doMax(existingValue, value, arena);
		else if (type == MutationRef::Min)
			return doMinV2(existingValue, value, arena);
		else if (type == MutationRef::ByteMin)
			return doByteMin(existingValue, value, arena);
		else if (type == MutationRef::ByteMax)
			return doByteMax(existingValue, value, arena);
		ASSERT(false);
		return Value();
	}

	ACTOR Future<Void> randomTransaction(Database cx, WriteDuringReadWorkload* self, double testStartTime) {
		state ReadYourWritesTransaction tr(cx);
		state bool readYourWritesDisabled = deterministicRandom()->random01() < 0.5;
		state bool readAheadDisabled = deterministicRandom()->random01() < 0.5;
		state bool snapshotRYWDisabled = deterministicRandom()->random01() < 0.5;
		state bool useBatchPriority = deterministicRandom()->random01() < 0.5;
		state int64_t timebomb =
		    (FLOW_KNOBS->MAX_BUGGIFIED_DELAY == 0.0 && deterministicRandom()->random01() < 0.01)
		        ? deterministicRandom()->randomInt64(1, 6000)
		        : 0; // timebomb check can fail incorrectly if simulation injects delay longer than the timebomb
		state std::vector<Future<Void>> operations;
		state ActorCollection commits(false);
		state std::vector<Future<Void>> watches;
		state int changeNum = 1;
		state bool doingCommit = false;
		state int waitLocation = 0;
		state double startTime = now();

		state bool disableGetKey = BUGGIFY;
		state bool disableGetRange = BUGGIFY;
		state bool disableGet = BUGGIFY;
		state bool disableCommit = BUGGIFY;
		state bool disableClearRange = BUGGIFY;
		state bool disableClear = BUGGIFY;
		state bool disableWatch = BUGGIFY;
		state bool disableWriteConflictRange = BUGGIFY;
		state bool disableDelay = BUGGIFY;
		state bool disableReset = BUGGIFY;
		state bool disableReadConflictRange = BUGGIFY;
		state bool disableSet = BUGGIFY;
		state bool disableAtomicOp = BUGGIFY;

		state Key timebombStr = makeString(8);
		uint8_t* data = mutateString(timebombStr);
		memcpy(data, &timebomb, 8);

		loop {
			if (now() - testStartTime > self->testDuration) {
				return Void();
			}

			state int64_t memLimit = 1e8;
			state bool cancelled = false;
			if (readYourWritesDisabled)
				tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
			if (snapshotRYWDisabled)
				tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);
			if (readAheadDisabled)
				tr.setOption(FDBTransactionOptions::READ_AHEAD_DISABLE);
			if (useBatchPriority)
				tr.setOption(FDBTransactionOptions::PRIORITY_BATCH);
			if (self->useSystemKeys)
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::TIMEOUT, timebombStr);
			tr.addWriteConflictRange(self->conflictRange);
			self->addedConflicts.insert(self->conflictRange, true);
			try {
				state int numWaits = deterministicRandom()->randomInt(1, 5);
				state int i = 0;
				for (; i < numWaits && memLimit > 0; i++) {
					//TraceEvent("WDROps").detail("Count", i).detail("Max", numWaits).detail("ReadYourWritesDisabled",readYourWritesDisabled);
					state int numOps = deterministicRandom()->randomInt(1, self->numOps);
					state int j = 0;
					for (; j < numOps && memLimit > 0; j++) {
						if (commits.getResult().isError())
							throw commits.getResult().getError();
						try {
							state int operationType = deterministicRandom()->randomInt(0, 21);
							if (operationType == 0 && !disableGetKey) {
								operations.push_back(
								    self->getKeyAndCompare(&tr,
								                           self->getRandomKeySelector(),
								                           Snapshot{ deterministicRandom()->coinflip() },
								                           readYourWritesDisabled,
								                           snapshotRYWDisabled,
								                           self,
								                           &doingCommit,
								                           &memLimit));
							} else if (operationType == 1 && !disableGetRange) {
								operations.push_back(
								    self->getRangeAndCompare(&tr,
								                             self->getRandomKeySelector(),
								                             self->getRandomKeySelector(),
								                             self->getRandomLimits(),
								                             Snapshot{ deterministicRandom()->coinflip() },
								                             Reverse{ deterministicRandom()->coinflip() },
								                             readYourWritesDisabled,
								                             snapshotRYWDisabled,
								                             self,
								                             &doingCommit,
								                             &memLimit));
							} else if (operationType == 2 && !disableGet) {
								operations.push_back(self->getAndCompare(&tr,
								                                         self->getRandomKey(),
								                                         Snapshot{ deterministicRandom()->coinflip() },
								                                         readYourWritesDisabled,
								                                         snapshotRYWDisabled,
								                                         self,
								                                         &doingCommit,
								                                         &memLimit));
							} else if (operationType == 3 && !disableCommit) {
								if (!self->rarelyCommit || deterministicRandom()->random01() < 1.0 / self->numOps) {
									Future<Void> commit = self->commitAndUpdateMemory(&tr,
									                                                  self,
									                                                  &cancelled,
									                                                  readYourWritesDisabled,
									                                                  snapshotRYWDisabled,
									                                                  readAheadDisabled,
									                                                  useBatchPriority,
									                                                  &doingCommit,
									                                                  &startTime,
									                                                  timebombStr);
									operations.push_back(commit);
									commits.add(commit);
								}
							} else if (operationType == 4 && !disableClearRange) {
								KeyRange range = self->getRandomRange(self->maxClearSize);
								self->changeCount.insert(range, changeNum++);
								bool noConflict = deterministicRandom()->random01() < 0.5;
								//TraceEvent("WDRClearRange").detail("Begin", range).detail("NoConflict", noConflict);
								if (noConflict)
									tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
								tr.clear(range);
								if (!noConflict) {
									KeyRangeRef conflict(
									    range.begin.substr(
									        0, std::min<int>(range.begin.size(), getMaxClearKeySize(range.begin) + 1)),
									    range.end.substr(
									        0, std::min<int>(range.end.size(), getMaxClearKeySize(range.end) + 1)));
									self->addedConflicts.insert(conflict, true);
								}
								self->memoryDatabase.erase(self->memoryDatabase.lower_bound(range.begin),
								                           self->memoryDatabase.lower_bound(range.end));
							} else if (operationType == 5 && !disableClear) {
								Key key = self->getRandomKey();
								self->changeCount.insert(key, changeNum++);
								bool noConflict = deterministicRandom()->random01() < 0.5;
								//TraceEvent("WDRClear").detail("Key", key).detail("NoConflict", noConflict);
								if (noConflict)
									tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
								tr.clear(key);
								if (!noConflict && key.size() <= getMaxClearKeySize(key)) {
									self->addedConflicts.insert(key, true);
								}
								self->memoryDatabase.erase(key);
							} else if (operationType == 6 && !disableWatch) {
								watches.push_back(self->watchAndCompare(
								    &tr, self->getRandomKey(), readYourWritesDisabled, self, &doingCommit, &memLimit));
							} else if (operationType == 7 && !disableWriteConflictRange) {
								KeyRange range = self->getRandomRange(self->nodes);
								//TraceEvent("WDRAddWriteConflict").detail("Range", range);
								tr.addWriteConflictRange(range);
								KeyRangeRef conflict(
								    range.begin.substr(
								        0, std::min<int>(range.begin.size(), getMaxKeySize(range.begin) + 1)),
								    range.end.substr(0, std::min<int>(range.end.size(), getMaxKeySize(range.end) + 1)));
								self->addedConflicts.insert(conflict, true);
							} else if (operationType == 8 && !disableDelay) {
								double maxTime = 6.0;
								if (timebomb > 0)
									maxTime = startTime + timebomb / 1000.0 - now();
								operations.push_back(
								    delay(deterministicRandom()->random01() * deterministicRandom()->random01() *
								          deterministicRandom()->random01() * maxTime));
							} else if (operationType == 9 && !disableReset) {
								if (deterministicRandom()->random01() < 0.001) {
									//TraceEvent("WDRReset");
									tr.reset();
									self->memoryDatabase = self->lastCommittedDatabase;
									self->addedConflicts.insert(allKeys, false);
									if (readYourWritesDisabled)
										tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
									if (snapshotRYWDisabled)
										tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);
									if (readAheadDisabled)
										tr.setOption(FDBTransactionOptions::READ_AHEAD_DISABLE);
									if (self->useSystemKeys)
										tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
									tr.addWriteConflictRange(self->conflictRange);
									self->addedConflicts.insert(self->conflictRange, true);
									startTime = now();
									tr.setOption(FDBTransactionOptions::TIMEOUT, timebombStr);
								}
							} else if (operationType == 10 && !disableReadConflictRange) {
								KeyRange range = self->getRandomRange(self->maxClearSize);
								tr.addReadConflictRange(range);
							} else if (operationType == 11 && !disableAtomicOp) {
								if (!self->useSystemKeys && deterministicRandom()->random01() < 0.01) {
									Key versionStampKey = self->getRandomVersionStampKey();
									Value value = self->getRandomValue();
									KeyRangeRef range = getVersionstampKeyRange(versionStampKey.arena(),
									                                            versionStampKey,
									                                            tr.getCachedReadVersion(),
									                                            normalKeys.end);
									self->changeCount.insert(range, changeNum++);
									//TraceEvent("WDRVersionStamp").detail("VersionStampKey", versionStampKey).detail("Range", range);
									tr.atomicOp(versionStampKey, value, MutationRef::SetVersionstampedKey);
									tr.clear(range);
									KeyRangeRef conflict(
									    range.begin.substr(
									        0, std::min<int>(range.begin.size(), getMaxClearKeySize(range.begin) + 1)),
									    range.end.substr(
									        0, std::min<int>(range.end.size(), getMaxClearKeySize(range.end) + 1)));
									self->addedConflicts.insert(conflict, true);
									self->memoryDatabase.erase(self->memoryDatabase.lower_bound(range.begin),
									                           self->memoryDatabase.lower_bound(range.end));
								} else {
									Key key = self->getRandomKey();
									Value value = self->getRandomValue();
									MutationRef::Type opType;
									switch (deterministicRandom()->randomInt(0, 8)) {
									case 0:
										opType = MutationRef::AddValue;
										break;
									case 1:
										opType = MutationRef::And;
										break;
									case 2:
										opType = MutationRef::Or;
										break;
									case 3:
										opType = MutationRef::Xor;
										break;
									case 4:
										opType = MutationRef::Max;
										break;
									case 5:
										opType = MutationRef::Min;
										break;
									case 6:
										opType = MutationRef::ByteMin;
										break;
									case 7:
										opType = MutationRef::ByteMax;
										break;
									}
									self->changeCount.insert(key, changeNum++);
									bool noConflict = deterministicRandom()->random01() < 0.5;
									//TraceEvent("WDRAtomicOp").detail("Key", key).detail("Value", value.size()).detail("NoConflict", noConflict);
									if (noConflict)
										tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
									tr.atomicOp(key, value, opType);
									//TraceEvent("WDRAtomicOpSuccess").detail("Key", key).detail("Value", value.size());
									if (!noConflict && key.size() <= getMaxWriteKeySize(key, self->useSystemKeys)) {
										self->addedConflicts.insert(key, true);
									}
									Optional<Value> existing = self->memoryGet(&self->memoryDatabase, key);
									self->memoryDatabase[key] =
									    self->applyAtomicOp(existing.present() ? Optional<StringRef>(existing.get())
									                                           : Optional<StringRef>(),
									                        value,
									                        opType);
								}
							} else if (operationType > 11 && !disableSet) {
								Key key = self->getRandomKey();
								Value value = self->getRandomValue();
								self->changeCount.insert(key, changeNum++);
								bool noConflict = deterministicRandom()->random01() < 0.5;
								//TraceEvent("WDRSet").detail("Key", key).detail("Value", value.size()).detail("NoConflict", noConflict);
								if (noConflict)
									tr.setOption(FDBTransactionOptions::NEXT_WRITE_NO_WRITE_CONFLICT_RANGE);
								tr.set(key, value);
								if (!noConflict && key.size() <= getMaxWriteKeySize(key, self->useSystemKeys)) {
									self->addedConflicts.insert(key, true);
								}
								//TraceEvent("WDRSetSuccess").detail("Key", key).detail("Value", value.size());
								self->memoryDatabase[key] = value;
							}
						} catch (Error& e) {
							if (e.code() == error_code_used_during_commit) {
								ASSERT(doingCommit);
							} else if (e.code() != error_code_transaction_cancelled) {
								throw;
							}
						}
					}

					if (waitLocation < operations.size()) {
						int waitOp = deterministicRandom()->randomInt(waitLocation, operations.size());
						//TraceEvent("WDRWait").detail("Op", waitOp).detail("Operations", operations.size()).detail("WaitLocation", waitLocation);
						wait(operations[waitOp]);
						wait(delay(0.000001)); // to ensure errors have propagated from reads to commits
						waitLocation = operations.size();
					}
				}
				wait(waitForAll(operations));
				ASSERT(timebomb == 0 || 1000 * (now() - startTime) <= timebomb + 1);
				wait(tr.debug_onIdle());
				wait(delay(0.000001)); // to ensure triggered watches have a change to register
				self->finished.trigger();
				wait(waitForAll(watches)); // only for errors, should have all returned
				self->changeCount.insert(allKeys, 0);
				break;
			} catch (Error& e) {
				operations.clear();
				commits.clear(false);
				waitLocation = 0;
				watches.clear();
				self->changeCount.insert(allKeys, 0);
				doingCommit = false;
				//TraceEvent("WDRError").errorUnsuppressed(e);
				if (e.code() == error_code_database_locked) {
					self->memoryDatabase = self->lastCommittedDatabase;
					self->addedConflicts.insert(allKeys, false);
					return Void();
				}
				if (e.code() == error_code_not_committed || e.code() == error_code_commit_unknown_result ||
				    e.code() == error_code_transaction_too_large || e.code() == error_code_key_too_large ||
				    e.code() == error_code_value_too_large || e.code() == error_code_too_many_watches || cancelled)
					throw not_committed();
				try {
					wait(tr.onError(e));
				} catch (Error& e) {
					if (e.code() == error_code_transaction_timed_out) {
						ASSERT(timebomb != 0 && 1000 * (now() - startTime) >= timebomb - 1);
						throw not_committed();
					}
					throw e;
				}
				self->memoryDatabase = self->lastCommittedDatabase;
				self->addedConflicts.insert(allKeys, false);
			}
		}
		self->memoryDatabase = self->lastCommittedDatabase;
		self->addedConflicts.insert(allKeys, false);
		return Void();
	}
};

WorkloadFactory<WriteDuringReadWorkload> WriteDuringReadWorkloadFactory;
