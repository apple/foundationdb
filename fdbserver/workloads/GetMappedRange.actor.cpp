/*
 * GetMappedRange.actor.cpp
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

#include <cstdint>
#include <limits>
#include <algorithm>
#include "fdbrpc/simulator.h"
#include "fdbclient/MutationLogReader.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/workloads/ApiWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const Value EMPTY = Tuple().pack();
ValueRef SOMETHING = "SOMETHING"_sr;
const KeyRef prefix = "prefix"_sr;
const KeyRef RECORD = "RECORD"_sr;
const KeyRef INDEX = "INDEX"_sr;

struct GetMappedRangeWorkload : ApiWorkload {
	bool enabled;
	Snapshot snapshot = Snapshot::False;

	//	const bool BAD_MAPPER = deterministicRandom()->random01() < 0.1;
	const bool BAD_MAPPER = false;
	//	const bool SPLIT_RECORDS = deterministicRandom()->random01() < 0.5;
	const bool SPLIT_RECORDS = true;
	const static int SPLIT_SIZE = 3;

	GetMappedRangeWorkload(WorkloadContext const& wcx) : ApiWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
	}

	std::string description() const override { return "GetMappedRange"; }

	Future<Void> start(Database const& cx) override {
		// This workload is generated different from typical ApiWorkload. So don't use ApiWorkload::_start.
		if (enabled) {
			return GetMappedRangeWorkload::_start(cx, this);
		}
		return Void();
	}

	ACTOR Future<Void> performSetup(Database cx, GetMappedRangeWorkload* self) {
		std::vector<TransactionType> types;
		types.push_back(NATIVE);
		types.push_back(READ_YOUR_WRITES);

		wait(self->chooseTransactionFactory(cx, types));
		return Void();
	}

	Future<Void> performSetup(Database const& cx) override { return performSetup(cx, this); }

	Future<Void> performTest(Database const& cx, Standalone<VectorRef<KeyValueRef>> const& data) override {
		// Ignore this because we are not using ApiWorkload's default ::start.
		return Future<Void>();
	}

	static Key primaryKey(int i) { return Key(format("primary-key-of-record-%08d", i)); }
	static Key indexKey(int i) { return Key(format("index-key-of-record-%08d", i)); }
	static Value dataOfRecord(int i) { return Key(format("data-of-record-%08d", i)); }
	static Value dataOfRecord(int i, int split) { return Key(format("data-of-record-%08d-split-%08d", i, split)); }

	static Key indexEntryKey(int i) {
		return Tuple().append(prefix).append(INDEX).append(indexKey(i)).append(primaryKey(i)).pack();
	}
	static Key recordKey(int i) { return Tuple().append(prefix).append(RECORD).append(primaryKey(i)).pack(); }
	static Key recordKey(int i, int split) {
		return Tuple().append(prefix).append(RECORD).append(primaryKey(i)).append(split).pack();
	}
	static Value recordValue(int i) { return Tuple().append(dataOfRecord(i)).pack(); }
	static Value recordValue(int i, int split) { return Tuple().append(dataOfRecord(i, split)).pack(); }

	ACTOR Future<Void> fillInRecords(Database cx, int n, GetMappedRangeWorkload* self) {
		state Transaction tr(cx);
		loop {
			std::cout << "start fillInRecords n=" << n << std::endl;
			// TODO: When n is large, split into multiple transactions.
			try {
				for (int i = 0; i < n; i++) {
					if (self->SPLIT_RECORDS) {
						for (int split = 0; split < SPLIT_SIZE; split++) {
							tr.set(recordKey(i, split), recordValue(i, split));
						}
					} else {
						tr.set(recordKey(i), recordValue(i));
					}
					tr.set(indexEntryKey(i), EMPTY);
				}
				wait(tr.commit());
				std::cout << "finished fillInRecords with version " << tr.getCommittedVersion() << std::endl;
				break;
			} catch (Error& e) {
				std::cout << "failed fillInRecords, retry" << std::endl;
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	static void showResult(const RangeResult& result) {
		std::cout << "result size: " << result.size() << std::endl;
		for (const KeyValueRef* it = result.begin(); it != result.end(); it++) {
			std::cout << "key=" << it->key.printable() << ", value=" << it->value.printable() << std::endl;
		}
	}

	ACTOR Future<Void> scanRange(Database cx, KeyRangeRef range) {
		std::cout << "start scanRange " << range.toString() << std::endl;
		// TODO: When n is large, split into multiple transactions.
		state Transaction tr(cx);
		loop {
			try {
				RangeResult result = wait(tr.getRange(range, CLIENT_KNOBS->TOO_MANY));
				//			showResult(result);
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		std::cout << "finished scanRange" << std::endl;
		return Void();
	}

	static void validateRecord(int expectedId, const MappedKeyValueRef* it, GetMappedRangeWorkload* self) {
		//		std::cout << "validateRecord expectedId " << expectedId << " it->key " << printable(it->key) << "
		// indexEntryKey(expectedId) " << printable(indexEntryKey(expectedId)) << std::endl;
		ASSERT(it->key == indexEntryKey(expectedId));
		ASSERT(it->value == EMPTY);

		if (self->SPLIT_RECORDS) {
			ASSERT(std::holds_alternative<GetRangeReqAndResultRef>(it->reqAndResult));
			auto& getRange = std::get<GetRangeReqAndResultRef>(it->reqAndResult);
			auto& rangeResult = getRange.result;
			//					std::cout << "rangeResult.size()=" << rangeResult.size() << std::endl;
			ASSERT(rangeResult.more == false);
			ASSERT(rangeResult.size() == SPLIT_SIZE);
			for (int split = 0; split < SPLIT_SIZE; split++) {
				auto& kv = rangeResult[split];
				//						std::cout << "kv.key=" << printable(kv.key)
				//						          << ", recordKey(id, split)=" << printable(recordKey(id, split)) <<
				// std::endl; std::cout << "kv.value=" << printable(kv.value)
				//						          << ", recordValue(id, split)=" << printable(recordValue(id, split)) <<
				// std::endl;
				ASSERT(kv.key == recordKey(expectedId, split));
				ASSERT(kv.value == recordValue(expectedId, split));
			}
		} else {
			ASSERT(std::holds_alternative<GetValueReqAndResultRef>(it->reqAndResult));
			auto& getValue = std::get<GetValueReqAndResultRef>(it->reqAndResult);
			ASSERT(getValue.key == recordKey(expectedId));
			ASSERT(getValue.result.present());
			ASSERT(getValue.result.get() == recordValue(expectedId));
		}
	}

	ACTOR Future<MappedRangeResult> scanMappedRangeWithLimits(Database cx,
	                                                          KeySelector beginSelector,
	                                                          KeySelector endSelector,
	                                                          Key mapper,
	                                                          int limit,
	                                                          int expectedBeginId,
	                                                          GetMappedRangeWorkload* self) {

		std::cout << "start scanMappedRangeWithLimits beginSelector:" << beginSelector.toString()
		          << " endSelector:" << endSelector.toString() << " expectedBeginId:" << expectedBeginId
		          << " limit:" << limit << std::endl;
		loop {
			state Reference<TransactionWrapper> tr = self->createTransaction();
			try {
				MappedRangeResult result = wait(tr->getMappedRange(
				    beginSelector, endSelector, mapper, GetRangeLimits(limit), self->snapshot, Reverse::False));
				//			showResult(result);
				if (self->BAD_MAPPER) {
					TraceEvent("GetMappedRangeWorkloadShouldNotReachable").detail("ResultSize", result.size());
				}
				std::cout << "result.size()=" << result.size() << std::endl;
				std::cout << "result.more=" << result.more << std::endl;
				ASSERT(result.size() <= limit);
				int expectedId = expectedBeginId;
				for (const MappedKeyValueRef* it = result.begin(); it != result.end(); it++) {
					validateRecord(expectedId, it, self);
					expectedId++;
				}
				std::cout << "finished scanMappedRangeWithLimits" << std::endl;
				return result;
			} catch (Error& e) {
				if ((self->BAD_MAPPER && e.code() == error_code_mapper_bad_index) ||
				    (!SERVER_KNOBS->QUICK_GET_VALUE_FALLBACK && e.code() == error_code_quick_get_value_miss) ||
				    (!SERVER_KNOBS->QUICK_GET_KEY_VALUES_FALLBACK &&
				     e.code() == error_code_quick_get_key_values_miss)) {
					TraceEvent("GetMappedRangeWorkloadExpectedErrorDetected").error(e);
					return MappedRangeResult();
				} else {
					std::cout << "error " << e.what() << std::endl;
					wait(tr->onError(e));
				}
				std::cout << "failed scanMappedRangeWithLimits" << std::endl;
			}
		}
	}

	ACTOR Future<Void> scanMappedRange(Database cx, int beginId, int endId, Key mapper, GetMappedRangeWorkload* self) {
		Key beginTuple = Tuple().append(prefix).append(INDEX).append(indexKey(beginId)).getDataAsStandalone();
		state KeySelector beginSelector = KeySelector(firstGreaterOrEqual(beginTuple));
		Key endTuple = Tuple().append(prefix).append(INDEX).append(indexKey(endId)).getDataAsStandalone();
		state KeySelector endSelector = KeySelector(firstGreaterOrEqual(endTuple));
		state int limit = 100;
		state int expectedBeginId = beginId;
		while (true) {
			MappedRangeResult result = wait(
			    self->scanMappedRangeWithLimits(cx, beginSelector, endSelector, mapper, limit, expectedBeginId, self));
			expectedBeginId += result.size();
			if (result.more) {
				if (result.empty()) {
					// This is usually not expected.
					std::cout << "not result but have more, try again" << std::endl;
				} else {
					beginSelector = KeySelector(firstGreaterThan(result.back().key));
				}
			} else {
				// No more, finished.
				break;
			}
		}
		ASSERT(expectedBeginId == endId);
		return Void();
	}

	static void conflictWriteOnRecord(int conflictRecordId,
	                                  Reference<TransactionWrapper>& tr,
	                                  GetMappedRangeWorkload* self) {
		Key writeKey;
		if (deterministicRandom()->random01() < 0.5) {
			// Concurrent write to the primary scanned range
			writeKey = indexEntryKey(conflictRecordId);
		} else {
			// Concurrent write to the underlying scanned ranges/keys
			if (self->SPLIT_RECORDS) {
				// Update one of the splits is sufficient.
				writeKey = recordKey(conflictRecordId, 0);
			} else {
				writeKey = recordKey(conflictRecordId);
			}
		}
		tr->set(writeKey, SOMETHING);
		std::cout << "conflict write to " << printable(writeKey) << std::endl;
	}

	static Future<MappedRangeResult> runGetMappedRange(int beginId,
	                                                   int endId,
	                                                   Reference<TransactionWrapper>& tr,
	                                                   GetMappedRangeWorkload* self) {
		Key mapper = getMapper(self);
		Key beginTuple = Tuple().append(prefix).append(INDEX).append(indexKey(beginId)).getDataAsStandalone();
		KeySelector beginSelector = KeySelector(firstGreaterOrEqual(beginTuple));
		Key endTuple = Tuple().append(prefix).append(INDEX).append(indexKey(endId)).getDataAsStandalone();
		KeySelector endSelector = KeySelector(firstGreaterOrEqual(endTuple));
		return tr->getMappedRange(beginSelector,
		                          endSelector,
		                          mapper,
		                          GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED),
		                          self->snapshot,
		                          Reverse::False);
	}

	// If another transaction writes to our read set (the scanned ranges) before we commit, the transaction should
	// fail.
	ACTOR Future<Void> testSerializableConflicts(GetMappedRangeWorkload* self) {
		std::cout << "testSerializableConflicts" << std::endl;

		loop {
			state Reference<TransactionWrapper> tr1 = self->createTransaction();
			try {
				MappedRangeResult result = wait(runGetMappedRange(5, 10, tr1, self));

				// Commit another transaction that has conflict writes.
				loop {
					state Reference<TransactionWrapper> tr2 = self->createTransaction();
					try {
						conflictWriteOnRecord(7, tr2, self);
						wait(tr2->commit());
						break;
					} catch (Error& e) {
						std::cout << "tr2 error " << e.what() << std::endl;
						wait(tr2->onError(e));
					}
				}

				// Do some writes so that tr1 is not read-only.
				tr1->set(SOMETHING, SOMETHING);
				wait(tr1->commit());
				UNREACHABLE();
			} catch (Error& e) {
				if (e.code() == error_code_not_committed) {
					std::cout << "tr1 failed because of conflicts (as expected)" << std::endl;
					TraceEvent("GetMappedRangeWorkloadExpectedErrorDetected").error(e);
					return Void();
				} else {
					std::cout << "tr1 error " << e.what() << std::endl;
					wait(tr1->onError(e));
				}
			}
		}
	}

	// If the same transaction writes to the read set (the scanned ranges) before reading, it should throw read your
	// write exception.
	ACTOR Future<Void> testRYW(GetMappedRangeWorkload* self) {
		std::cout << "testRYW" << std::endl;
		loop {
			state Reference<TransactionWrapper> tr1 = self->createTransaction();
			try {
				// Write something that will be read in getMappedRange.
				conflictWriteOnRecord(7, tr1, self);
				MappedRangeResult result = wait(runGetMappedRange(5, 10, tr1, self));
				UNREACHABLE();
			} catch (Error& e) {
				if (e.code() == error_code_get_mapped_range_reads_your_writes) {
					std::cout << "tr1 failed because of read your writes (as expected)" << std::endl;
					TraceEvent("GetMappedRangeWorkloadExpectedErrorDetected").error(e);
					return Void();
				} else {
					std::cout << "tr1 error " << e.what() << std::endl;
					wait(tr1->onError(e));
				}
			}
		}
	}

	ACTOR Future<Void> _start(Database cx, GetMappedRangeWorkload* self) {
		TraceEvent("GetMappedRangeWorkloadConfig").detail("BadMapper", self->BAD_MAPPER);

		// TODO: Use toml to config
		wait(self->fillInRecords(cx, 500, self));

		if (self->transactionType == NATIVE) {
			self->snapshot = Snapshot::True;
		} else if (self->transactionType == READ_YOUR_WRITES) {
			self->snapshot = Snapshot::False;
			const double rand = deterministicRandom()->random01();
			if (rand < 0.1) {
				wait(self->testSerializableConflicts(self));
				return Void();
			} else if (rand < 0.2) {
				wait(self->testRYW(self));
				return Void();
			} else {
				// Test the happy path where there is no conflicts or RYW
			}
		} else {
			UNREACHABLE();
		}

		std::cout << "Test configuration: transactionType:" << self->transactionType << " snapshot:" << self->snapshot
		          << "bad_mapper:" << self->BAD_MAPPER << std::endl;

		Key mapper = getMapper(self);
		// The scanned range cannot be too large to hit get_mapped_key_values_has_more. We have a unit validating the
		// error is thrown when the range is large.
		wait(self->scanMappedRange(cx, 10, 490, mapper, self));
		return Void();
	}

	static Key getMapper(GetMappedRangeWorkload* self) {
		Tuple mapperTuple;
		if (self->BAD_MAPPER) {
			mapperTuple << prefix << RECORD << "{K[xxx]}"_sr;
		} else {
			mapperTuple << prefix << RECORD << "{K[3]}"_sr;
			if (self->SPLIT_RECORDS) {
				mapperTuple << "{...}"_sr;
			}
		}
		Key mapper = mapperTuple.getDataAsStandalone();
		return mapper;
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GetMappedRangeWorkload> GetMappedRangeWorkloadFactory("GetMappedRange");
