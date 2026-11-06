/*
 * RangeKeys.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/QuietDatabase.h"
#include "fdbserver/tester/workloads.h"

struct RangeKeysWorkload : TestWorkload {
	static constexpr auto NAME = "RangeKeys";
	static constexpr int KEY_COUNT = 64;
	int apiVersion;
	bool checkReplicas;
	Version populatedVersion = invalidVersion;
	bool finished = false;
	std::map<Key, Value> contents;
	Key prefix;
	KeyRange range;

	explicit RangeKeysWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		apiVersion = getOption(options, "apiVersion"_sr, ApiVersion::withRangeKeys().version());
		checkReplicas = getOption(options, "checkReplicas"_sr, false);
		prefix = Key(format("rangeKeys/%d/", clientId));
		range = KeyRangeRef(prefix, strinc(prefix));
		for (int i = 0; i < KEY_COUNT; ++i) {
			int size = i % 4 == 0 ? 0 : (i % 4 == 1 ? 1 : (i % 4 == 2 ? 4096 : 65536));
			contents.emplace(key(i * 2), Value(std::string(size, 'a' + i % 26)));
		}
	}

	Key key(int index) const { return Key(format("%s%04d", prefix.toString().c_str(), index)); }

	Future<Void> setup(Database const& cx) override { return populate(cx); }

	Future<Void> populate(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.clear(range);
				for (const auto& [key, value] : contents)
					tr.set(key, value);
				co_await tr.commit();
				populatedVersion = tr.getCommittedVersion();
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> start(Database const& cx) override { return run(cx->clone()); }
	Future<bool> check(Database const&) override { return finished; }
	void getMetrics(std::vector<PerfMetric>&) override {}
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		// Range locks can independently conflict with the snapshot reader's commit
		out.insert("RandomRangeLock");
	}

	Future<Void> waitForDurableData(Database cx) {
		while (true) {
			try {
				co_await getMaxStorageServerQueueSize(cx, dbInfo, populatedVersion);
				co_return;
			} catch (Error& e) {
				if (e.code() != error_code_timed_out && e.code() != error_code_attribute_not_found)
					throw;
			}
			co_await delay(1.0);
		}
	}

	template <class Tr>
	Future<std::vector<Key>> readKeys(Tr* tr,
	                                  KeySelector begin,
	                                  KeySelector end,
	                                  GetRangeLimits limits,
	                                  Snapshot snapshot,
	                                  Reverse reverse) {
		std::vector<Key> keys;
		for (int pageNumber = 0; pageNumber <= KEY_COUNT * 2; ++pageNumber) {
			RangeKeysResult page = co_await tr->getRangeKeys(begin, end, limits, snapshot, reverse);
			if (limits.hasRowLimit())
				ASSERT(page.size() <= limits.rows);
			// Reverse RYW reads may return cached rows before satisfying minRows
			if constexpr (std::is_same_v<Tr, Transaction>) {
				if (page.more)
					ASSERT(page.size() >= limits.minRows);
			}
			for (const auto& row : page) {
				if (!keys.empty())
					ASSERT(reverse ? keys.back() > row.key : keys.back() < row.key);
				keys.emplace_back(row.key);
			}
			if (!page.more)
				co_return keys;
			ASSERT(!page.empty() || page.readThrough.present());
			if (reverse)
				end = page.nextEndKeySelector();
			else
				begin = page.nextBeginKeySelector();
		}
		ASSERT(false);
		co_return keys;
	}

	template <class Tr>
	Future<Void> compareRead(Tr* tr,
	                         KeySelector begin,
	                         KeySelector end,
	                         GetRangeLimits limits,
	                         Snapshot snapshot,
	                         Reverse reverse,
	                         const std::map<Key, Value>* expected,
	                         int expectedCount = -1) {
		co_await tr->getReadVersion();
		std::vector<Key> keys = co_await readKeys(tr, begin, end, limits, snapshot, reverse);
		if (expectedCount >= 0)
			ASSERT(keys.size() == static_cast<size_t>(expectedCount));
		size_t index = 0;
		while (true) {
			RangeResult values = co_await tr->getRange(begin, end, GetRangeLimits(11), snapshot, reverse);
			for (const auto& row : values) {
				ASSERT(index < keys.size());
				ASSERT(keys[index++] == row.key);
				if (expected) {
					auto entry = expected->find(row.key);
					ASSERT(entry != expected->end());
					ASSERT(row.value == entry->second);
				}
			}
			if (!values.more)
				break;
			ASSERT(!values.empty() || values.readThrough.present());
			if (reverse)
				end = values.nextEndKeySelector();
			else
				begin = values.nextBeginKeySelector();
		}
		ASSERT(index == keys.size());
	}

	template <class Tr>
	Future<Void> testScans(Database cx) {
		for (int scenario = 0; scenario < 9; ++scenario) {
			TraceEvent("RangeKeysScanScenario")
			    .detail("ApiVersion", apiVersion)
			    .detail("ClientId", clientId)
			    .detail("Native", (std::is_same_v<Tr, Transaction>))
			    .detail("Scenario", scenario);
			Tr tr(cx);
			while (true) {
				Error error;
				try {
					KeySelector begin = firstGreaterOrEqual(range.begin);
					KeySelector end = firstGreaterOrEqual(range.end);
					GetRangeLimits limits(7);
					Reverse reverse(scenario % 2 != 0);
					std::map<Key, Value> expected = contents;
					int expectedCount = KEY_COUNT;
					if (scenario == 2 || scenario == 3) {
						begin = KeySelectorRef(key(32), false, -4);
						end = KeySelectorRef(key(88), true, 5);
						limits = GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, 128);
						expected.erase(expected.begin(), expected.lower_bound(key(22)));
						expected.erase(expected.lower_bound(key(98)), expected.end());
						expectedCount = 38;
					} else if (scenario == 4 || scenario == 5) {
						begin = firstGreaterOrEqual(key(scenario == 4 ? 13 : 129));
						end = firstGreaterOrEqual(key(scenario == 4 ? 13 : 130));
						expectedCount = 0;
					} else if (scenario == 6 || scenario == 7) {
						limits = GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, 1);
						limits.minRows = 3;
					} else if (scenario == 8) {
						limits = GetRangeLimits(KEY_COUNT, 4096);
					}
					co_await compareRead(&tr, begin, end, limits, Snapshot::False, reverse, &expected, expectedCount);
					RangeKeysResult empty =
					    co_await tr.getRangeKeys(firstGreaterOrEqual(range.begin), firstGreaterOrEqual(range.end), 0);
					ASSERT(empty.empty() && !empty.more);
					break;
				} catch (Error& e) {
					error = e;
				}
				co_await tr.onError(error);
			}
		}
	}

	Future<Void> testCommittedMutations(Database cx) {
		Transaction retry(cx);
		while (true) {
			co_await populate(cx);
			co_await waitForDurableData(cx);
			Error error;
			try {
				Transaction before(cx);
				co_await before.getReadVersion();
				Transaction writer(cx);
				std::map<Key, Value> expected = contents;
				// Disk slices separated by MVCC mutations must share one scan-work budget
				for (int i = 0; i < KEY_COUNT; i += 8) {
					writer.clear(KeyRangeRef(key(i * 2), key(i * 2 + 4)));
					expected.erase(key(i * 2));
					expected.erase(key(i * 2 + 2));
					writer.set(key(i * 2 + 1), contents.at(key(6)));
					expected[key(i * 2 + 1)] = contents.at(key(6));
					writer.set(key(i * 2 + 6), "replaced"_sr);
					expected[key(i * 2 + 6)] = "replaced"_sr;
				}
				co_await writer.commit();
				Transaction after(cx);
				after.setVersion(writer.getCommittedVersion());
				for (bool reverse : { false, true }) {
					co_await compareRead(&after,
					                     firstGreaterOrEqual(range.begin),
					                     firstGreaterOrEqual(range.end),
					                     GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, 4096),
					                     Snapshot::True,
					                     Reverse(reverse),
					                     &expected,
					                     expected.size());
					co_await compareRead(&before,
					                     firstGreaterOrEqual(range.begin),
					                     firstGreaterOrEqual(range.end),
					                     GetRangeLimits(9),
					                     Snapshot::True,
					                     Reverse(reverse),
					                     &contents,
					                     KEY_COUNT);
				}
				break;
			} catch (Error& e) {
				error = e;
			}
			co_await retry.onError(error);
		}
		co_await populate(cx);
	}

	Future<Void> testLocalWrites(Database cx) {
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error error;
			try {
				Future<RangeKeysResult> pending = tr.getRangeKeys(range, KEY_COUNT);
				std::map<Key, Value> expected = contents;
				tr.clear(key(2));
				expected.erase(key(2));
				tr.clear(KeyRangeRef(key(8), key(12)));
				expected.erase(key(8));
				expected.erase(key(10));
				tr.set(key(7), "inserted"_sr);
				expected[key(7)] = "inserted"_sr;
				tr.clear(key(20));
				tr.set(key(20), "replaced"_sr);
				expected[key(20)] = "replaced"_sr;
				tr.atomicOp(key(4), contents.at(key(4)), MutationRef::CompareAndClear);
				expected.erase(key(4));
				tr.atomicOp(key(6), "not the current value"_sr, MutationRef::CompareAndClear);
				RangeKeysResult retained = co_await pending;
				co_await compareRead(&tr,
				                     firstGreaterOrEqual(range.begin),
				                     firstGreaterOrEqual(range.end),
				                     GetRangeLimits(5),
				                     Snapshot::False,
				                     Reverse::False,
				                     &expected,
				                     expected.size());
				co_await compareRead(&tr,
				                     firstGreaterOrEqual(range.begin),
				                     firstGreaterOrEqual(range.end),
				                     GetRangeLimits(5),
				                     Snapshot::True,
				                     Reverse::True,
				                     &expected,
				                     expected.size());
				tr.setOption(FDBTransactionOptions::SNAPSHOT_RYW_DISABLE);
				co_await compareRead(&tr,
				                     firstGreaterOrEqual(range.begin),
				                     firstGreaterOrEqual(range.end),
				                     GetRangeLimits(5),
				                     Snapshot::True,
				                     Reverse::False,
				                     &contents,
				                     KEY_COUNT);
				tr.reset();
				ASSERT(retained.size() == KEY_COUNT);
				for (int i = 0; i < retained.size(); ++i)
					ASSERT(retained[i].key == key(i * 2));
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> testReplicaConsistency(Database cx) {
		if (!checkReplicas)
			co_return;
		for (int scenario = 0; scenario < 3; ++scenario) {
			Transaction tr(cx);
			while (true) {
				Error error;
				try {
					auto addresses = co_await tr.getAddressesForKey(key(0));
					ASSERT(addresses.size() >= 2);
					tr.setOption(FDBTransactionOptions::ENABLE_REPLICA_CONSISTENCY_CHECK);
					int64_t requiredReplicas = 1;
					tr.setOption(
					    FDBTransactionOptions::CONSISTENCY_CHECK_REQUIRED_REPLICAS,
					    StringRef(reinterpret_cast<const uint8_t*>(&requiredReplicas), sizeof(requiredReplicas)));
					TraceEvent("RangeKeysReplicaConsistencyScenario")
					    .detail("ApiVersion", apiVersion)
					    .detail("ClientId", clientId)
					    .detail("Scenario", scenario)
					    .detail("AvailableReplicas", addresses.size())
					    .detail("RequiredAdditionalReplicas", requiredReplicas);
					KeySelector begin = firstGreaterOrEqual(range.begin);
					KeySelector end = firstGreaterOrEqual(range.end);
					GetRangeLimits limits(KEY_COUNT, 4096);
					int expectedCount = KEY_COUNT;
					if (scenario == 2) {
						begin = KeySelectorRef(key(32), false, -4);
						end = KeySelectorRef(key(88), true, 5);
						limits = GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, 128);
						expectedCount = 38;
					}
					co_await compareRead(
					    &tr, begin, end, limits, Snapshot::False, Reverse(scenario == 1), &contents, expectedCount);
					break;
				} catch (Error& e) {
					error = e;
				}
				co_await tr.onError(error);
			}
		}
	}

	Future<Void> testReadYourWritesDisabled(Database cx) {
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
				co_await compareRead(&tr,
				                     firstGreaterOrEqual(range.begin),
				                     firstGreaterOrEqual(range.end),
				                     GetRangeLimits(9),
				                     Snapshot::False,
				                     Reverse::True,
				                     &contents,
				                     KEY_COUNT);
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> testSpecialKeys(Database cx) {
		for (bool reverse : { false, true }) {
			ReadYourWritesTransaction tr(cx);
			tr.addReadConflictRange(KeyRangeRef(key(2), key(4)));
			tr.addReadConflictRange(KeyRangeRef(key(8), key(10)));
			KeySelector begin = firstGreaterOrEqual(readConflictRangeKeysRange.begin);
			KeySelector end = firstGreaterOrEqual(readConflictRangeKeysRange.end);
			std::vector<Key> expected;
			for (int index : { 2, 4, 8, 10 })
				expected.push_back(key(index).withPrefix(readConflictRangeKeysRange.begin));
			if (reverse)
				std::reverse(expected.begin(), expected.end());
			size_t index = 0;
			while (true) {
				RangeKeysResult keys = co_await tr.getRangeKeys(begin, end, 1, Snapshot::False, Reverse(reverse));
				RangeResult values = co_await tr.getRange(begin, end, 1, Snapshot::False, Reverse(reverse));
				ASSERT(keys.size() == values.size());
				ASSERT(keys.more == values.more);
				ASSERT(keys.readThrough == values.readThrough);
				ASSERT(keys.readToBegin == values.readToBegin);
				ASSERT(keys.readThroughEnd == values.readThroughEnd);
				for (int row = 0; row < keys.size(); ++row) {
					ASSERT(index < expected.size());
					ASSERT(keys[row].key == expected[index++]);
					ASSERT(keys[row].key == values[row].key);
				}
				if (!keys.more)
					break;
				ASSERT(!keys.empty());
				if (reverse)
					end = keys.nextEndKeySelector();
				else
					begin = keys.nextBeginKeySelector();
			}
			ASSERT(index == expected.size());
		}
	}

	Future<Void> testSelectorClipping(Database cx) {
		for (int scenario = 0; scenario < 4; ++scenario) {
			Reverse reverse(scenario % 2 != 0);
			ReadYourWritesTransaction tr(cx);
			while (true) {
				Error error;
				try {
					if (scenario >= 2)
						tr.setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					KeySelector begin = KeySelectorRef(normalKeys.end, false, -4);
					KeySelector end = KeySelectorRef(normalKeys.end, false, 2);
					co_await compareRead(&tr, begin, end, GetRangeLimits(2), Snapshot::False, reverse, nullptr);
					RangeKeysResult keys = co_await tr.getRangeKeys(begin, end, 10, Snapshot::False, reverse);
					RangeResult values = co_await tr.getRange(begin, end, 10, Snapshot::False, reverse);
					ASSERT(!keys.more && !values.more);
					ASSERT(keys.readThroughEnd && values.readThroughEnd);
					ASSERT(!keys.readThrough.present() && !values.readThrough.present());
					for (const auto& row : keys)
						ASSERT(row.key < normalKeys.end);
					break;
				} catch (Error& e) {
					error = e;
				}
				co_await tr.onError(error);
			}
		}
	}

	template <class Tr>
	Future<Void> testValueConflict(Database cx, Snapshot snapshot) {
		Tr reader(cx);
		while (true) {
			Error error;
			try {
				Future<RangeKeysResult> pending = reader.getRangeKeys(
				    firstGreaterOrEqual(key(16)), firstGreaterOrEqual(key(32)), GetRangeLimits(3), snapshot);
				// A later local write must not remove a conflict established by this pending read
				reader.set(key(18), "local write after read invocation"_sr);
				RangeKeysResult keys = co_await pending;
				ASSERT(keys.size() == 3);
				Transaction writer(cx);
				writer.set(key(18), "value changed without changing the key"_sr);
				co_await writer.commit();
				reader.set(key(KEY_COUNT * 2 + 1), "force conflict checking"_sr);
				bool conflicted = false;
				try {
					co_await reader.commit();
				} catch (Error& e) {
					if (e.code() != error_code_not_committed || e.isInjectedFault())
						throw;
					conflicted = true;
				}
				ASSERT(conflicted == !snapshot);
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await reader.onError(error);
		}
	}

	Future<Void> testCancelledTransaction(Database cx) {
		ReadYourWritesTransaction tr(cx);
		tr.cancel();
		try {
			co_await tr.getRangeKeys(range, 1);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_transaction_cancelled);
		}
		for (bool reset : { false, true }) {
			tr.reset();
			while (true) {
				Error error;
				try {
					co_await tr.getReadVersion();
					Future<RangeKeysResult> pending = tr.getRangeKeys(range, 1);
					if (pending.isError())
						throw pending.getError();
					ASSERT(!pending.isReady());
					if (reset)
						tr.reset();
					else
						tr.cancel();
					try {
						co_await pending;
						ASSERT(false);
					} catch (Error& e) {
						ASSERT(e.code() == error_code_transaction_cancelled);
					}
					tr.reset();
					RangeKeysResult keys = co_await tr.getRangeKeys(range, 1);
					ASSERT(keys.size() == 1 && keys[0].key == key(0));
					break;
				} catch (Error& e) {
					error = e;
				}
				co_await tr.onError(error);
			}
		}
	}

	template <class Tr>
	Future<Void> testUnderflowConflict(Database cx) {
		if (clientId != 0)
			co_return;
		Key testPrefix = "\x01rangeKeysUnderflow/"_sr;
		Tr reader(cx);
		while (true) {
			Error error;
			try {
				Transaction setup(cx);
				setup.clear(KeyRangeRef(testPrefix, strinc(testPrefix)));
				setup.set(testPrefix.withSuffix("m"_sr), "value"_sr);
				co_await setup.commit();
				RangeKeysResult keys =
				    co_await reader.getRangeKeys(KeySelectorRef(testPrefix.withSuffix("b"_sr), false, -2),
				                                 KeySelectorRef(testPrefix.withSuffix("z"_sr), false, -2),
				                                 GetRangeLimits(10));
				ASSERT(keys.empty() && !keys.more);
				Transaction writer(cx);
				writer.set(testPrefix.withSuffix("a0"_sr), "value"_sr);
				writer.set(testPrefix.withSuffix("a1"_sr), "value"_sr);
				writer.set(testPrefix.withSuffix("a2"_sr), "value"_sr);
				co_await writer.commit();
				reader.set(key(KEY_COUNT * 2 + 1), "force conflict checking"_sr);
				bool conflicted = false;
				try {
					co_await reader.commit();
				} catch (Error& e) {
					if (e.code() != error_code_not_committed || e.isInjectedFault())
						throw;
					conflicted = true;
				}
				ASSERT(conflicted);
				co_return;
			} catch (Error& e) {
				error = e;
			}
			co_await reader.onError(error);
		}
	}

	Future<Void> run(Database cx) {
		cx->apiVersion = ApiVersion(apiVersion);
		TraceEvent("RangeKeysTestStart").detail("ApiVersion", apiVersion).detail("ClientId", clientId);
		co_await testScans<Transaction>(cx);
		co_await waitForDurableData(cx);
		co_await testScans<Transaction>(cx);
		co_await testReplicaConsistency(cx);
		co_await testCommittedMutations(cx);
		co_await testScans<ReadYourWritesTransaction>(cx);
		co_await testLocalWrites(cx);
		co_await testReadYourWritesDisabled(cx);
		co_await testSpecialKeys(cx);
		co_await testSelectorClipping(cx);
		co_await testCancelledTransaction(cx);
		co_await testValueConflict<Transaction>(cx, Snapshot::False);
		co_await testValueConflict<Transaction>(cx, Snapshot::True);
		co_await testValueConflict<ReadYourWritesTransaction>(cx, Snapshot::False);
		co_await testValueConflict<ReadYourWritesTransaction>(cx, Snapshot::True);
		co_await testUnderflowConflict<Transaction>(cx);
		co_await testUnderflowConflict<ReadYourWritesTransaction>(cx);
		finished = true;
		TraceEvent("RangeKeysTestComplete").detail("ApiVersion", apiVersion).detail("ClientId", clientId);
	}
};

WorkloadFactory<RangeKeysWorkload> RangeKeysWorkloadFactory;
