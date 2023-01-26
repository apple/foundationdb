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

#include <algorithm>
#include "fdbclient/MutationLogReader.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/workloads/ApiWorkload.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Knobs.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using Result = std::variant<MappedRangeResult, MappedRangeResultV2>;

const Value EMPTY = Tuple().pack();
ValueRef SOMETHING = "SOMETHING"_sr;
const KeyRef prefix = "prefix"_sr;
const KeyRef RECORD = "RECORD"_sr;
const KeyRef INDEX = "INDEX"_sr;
const int MATCH_INDEX_TEST_710_API = -1;
const int code_int = 1;
const int code_bool = 2;
const int code_invalid = 0;
const int VERSION = 2;
const int INVALID_VERSION = 1;
static std::unordered_map<int, int> versionToPosMatchIndex = { std::make_pair(2, 1) };
static std::unordered_map<int, int> versionToPosFetchLocalOnly = { std::make_pair(2, 6) };
static std::unordered_map<int, int> versionToPosLocal = { std::make_pair(2, 1) };
static std::unordered_map<int, int> versionToLength = { std::make_pair(2, 8) };

int recordSize;
int indexSize;
struct GetMappedRangeWorkload : ApiWorkload {
	static constexpr auto NAME = "GetMappedRange";
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

	// TODO: Currently this workload doesn't play well with MachineAttrition, but it probably should
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("Attrition"); }

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

	static Key indexEntryKey(int i) { return Tuple::makeTuple(prefix, INDEX, indexKey(i), primaryKey(i)).pack(); }
	static Key recordKey(int i) { return Tuple::makeTuple(prefix, RECORD, primaryKey(i)).pack(); }
	static Key recordKey(int i, int split) { return Tuple::makeTuple(prefix, RECORD, primaryKey(i), split).pack(); }
	static Value recordValue(int i) { return Tuple::makeTuple(dataOfRecord(i)).pack(); }
	static Value recordValue(int i, int split) { return Tuple::makeTuple(dataOfRecord(i, split)).pack(); }

	ACTOR Future<Void> fillInRecords(Database cx, int n, GetMappedRangeWorkload* self) {
		state Transaction tr(cx);
		loop {
			std::cout << "start fillInRecords n=" << n << std::endl;
			// TODO: When n is large, split into multiple transactions.
			recordSize = 0;
			indexSize = 0;
			try {
				for (int i = 0; i < n; i++) {
					if (self->SPLIT_RECORDS) {
						for (int split = 0; split < SPLIT_SIZE; split++) {
							tr.set(recordKey(i, split), recordValue(i, split));
							if (i == 0) {
								recordSize +=
								    recordKey(i, split).size() + recordValue(i, split).size() + sizeof(KeyValueRef);
							}
						}
					} else {
						tr.set(recordKey(i), recordValue(i));
						if (i == 0) {
							recordSize += recordKey(i).size() + recordValue(i).size() + sizeof(KeyValueRef);
						}
					}
					tr.set(indexEntryKey(i), EMPTY);
					if (i == 0) {
						indexSize += indexEntryKey(i).size() + sizeof(KeyValueRef);
					}
				}
				wait(tr.commit());
				std::cout << "finished fillInRecords with version " << tr.getCommittedVersion() << " recordSize "
				          << recordSize << " indexSize " << indexSize << std::endl;
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

	template <class KV>
	static bool localMiss(const KV* it) {
		if constexpr (std::is_same<KV, MappedKeyValueRefV2>::value) {
			return it->responseBytes[versionToPosLocal[VERSION] + 1] == 0;
		}
		return false;
	}

	// Return true if need to retry.
	template <class KV>
	static bool validateRecord(int expectedId,
	                           const KV* it,
	                           GetMappedRangeWorkload* self,
	                           int matchIndex,
	                           bool isBoundary,
	                           bool allMissing,
	                           bool fetchLocalOnly) {
		// std::cout << "validateRecord expectedId " << expectedId << " it->key " << printable(it->key)
		//           << " indexEntryKey(expectedId) " << printable(indexEntryKey(expectedId))
		//           << " matchIndex: " << matchIndex << " fetchLocalOnly:" << fetchLocalOnly << std::endl;
		if (matchIndex == MATCH_INDEX_ALL || matchIndex == MATCH_INDEX_TEST_710_API || isBoundary ||
		    (fetchLocalOnly && localMiss(it))) {
			ASSERT(it->key == indexEntryKey(expectedId));
		} else if (matchIndex == MATCH_INDEX_MATCHED_ONLY) {
			ASSERT(it->key == (allMissing ? EMPTY : indexEntryKey(expectedId)));
		} else if (matchIndex == MATCH_INDEX_UNMATCHED_ONLY) {
			ASSERT(it->key == (allMissing ? indexEntryKey(expectedId) : EMPTY));
		} else {
			ASSERT(it->key == EMPTY);
		}
		ASSERT(it->value == EMPTY);
		if constexpr (std::is_same<KV, MappedKeyValueRefV2>::value) {
			ASSERT(it->responseBytes[0] == VERSION); // version
			ASSERT(it->responseBytes[versionToPosLocal[VERSION]] == code_bool); // type
			ASSERT((it->responseBytes[versionToPosLocal[VERSION] + 1] == 0 ||
			        it->responseBytes[versionToPosLocal[VERSION] + 1] == 1));
		}
		if (self->SPLIT_RECORDS) {
			ASSERT(std::holds_alternative<GetRangeReqAndResultRef>(it->reqAndResult));
			auto& getRange = std::get<GetRangeReqAndResultRef>(it->reqAndResult);
			auto& rangeResult = getRange.result;
			//					std::cout << "rangeResult.size()=" << rangeResult.size() << std::endl;
			// In the future, we may be able to do the continuation more efficiently by combining partial results
			// together and then validate.
			if (rangeResult.more) {
				// Retry if the underlying request is not fully completed.
				return true;
			}
			if (!(allMissing || (fetchLocalOnly && localMiss(it)))) {
				ASSERT(rangeResult.size() == SPLIT_SIZE);
				for (int split = 0; split < SPLIT_SIZE; split++) {
					auto& kv = rangeResult[split];
					//				std::cout << "kv.key=" << printable(kv.key)
					//						   << ", recordKey(id, split)=" << printable(recordKey(id, split)) <<
					// std::endl; std::cout << "kv.value=" << printable(kv.value)
					//						   << ", recordValue(id, split)=" << printable(recordValue(id,split)) <<
					// std::endl;
					ASSERT(kv.key == recordKey(expectedId, split));
					ASSERT(kv.value == recordValue(expectedId, split));
				}
			}
		} else {
			ASSERT(std::holds_alternative<GetValueReqAndResultRef>(it->reqAndResult));
			auto& getValue = std::get<GetValueReqAndResultRef>(it->reqAndResult);
			ASSERT(getValue.key == recordKey(expectedId));
			ASSERT(getValue.result.present());
			ASSERT(getValue.result.get() == recordValue(expectedId));
		}
		return false;
	}

	template <class Result, class KV>
	static bool validate(Result result,
	                     int limit,
	                     int expectedBeginId,
	                     GetMappedRangeWorkload* self,
	                     int matchIndex,
	                     int allMissing,
	                     bool fetchLocalOnly) {
		if (self->BAD_MAPPER) {
			TraceEvent("GetMappedRangeWorkloadShouldNotReachable").detail("ResultSize", result.size());
		}
		std::cout << "result.size()=" << result.size() << ". result.more=" << result.more
		          << " matchIndex=" << matchIndex << std::endl;
		ASSERT(result.size() <= limit);
		int expectedId = expectedBeginId;
		bool needRetry = false;
		int cnt = 0;
		const KV* it = result.begin();
		for (; cnt < result.size(); cnt++, it++) {
			if (validateRecord(expectedId,
			                   it,
			                   self,
			                   matchIndex,
			                   cnt == 0 || cnt == result.size() - 1,
			                   allMissing,
			                   fetchLocalOnly)) {
				needRetry = true;
				break;
			}
			expectedId++;
		}
		if (needRetry) {
			return true;
		}
		std::cout << "finished scanMappedRangeWithLimits" << std::endl;
		return false;
	}

	ACTOR Future<std::pair<int, int>> scanMappedRangeWithLimits(Database cx,
	                                                            KeySelector beginSelector,
	                                                            KeySelector endSelector,
	                                                            Key mapper,
	                                                            int limit,
	                                                            int byteLimit,
	                                                            int expectedBeginId,
	                                                            GetMappedRangeWorkload* self,
	                                                            KeyRef* next,
	                                                            int matchIndex,
	                                                            bool allMissing) {

		state bool fetchLocalOnly = deterministicRandom()->random01() < 0.5;
		state int currentVersion = deterministicRandom()->random01() < 0.9 ? VERSION : INVALID_VERSION;
		state int codeMatchIndex = deterministicRandom()->random01() < 0.95 ? code_int : code_invalid;
		state int codeFetchLocalOnly = deterministicRandom()->random01() < 0.95 ? code_bool : code_invalid;
		std::cout << "start scanMappedRangeWithLimits beginSelector:" << beginSelector.toString()
		          << " endSelector:" << endSelector.toString() << " expectedBeginId:" << expectedBeginId
		          << " limit:" << limit << " byteLimit: " << byteLimit << "  recordSize: " << recordSize
		          << " STRICTLY_ENFORCE_BYTE_LIMIT: " << SERVER_KNOBS->STRICTLY_ENFORCE_BYTE_LIMIT << " allMissing "
		          << allMissing << " matchIndex: " << matchIndex << " currentversion=" << currentVersion
		          << " codeMatchIndex=" << codeMatchIndex << " codeFetchLocalOnly=" << codeFetchLocalOnly << std::endl;
		loop {
			state Reference<TransactionWrapper> tr = self->createTransaction();
			try {
				if (matchIndex == MATCH_INDEX_TEST_710_API) {
					MappedRangeResult result = wait(tr->getMappedRange(beginSelector,
					                                                   endSelector,
					                                                   mapper,
					                                                   GetRangeLimits(limit, byteLimit),
					                                                   self->snapshot,
					                                                   Reverse::False));
					if (validate<MappedRangeResult, MappedKeyValueRef>(
					        result, limit, expectedBeginId, self, matchIndex, allMissing, fetchLocalOnly)) {
						continue;
					}
					if (result.size() != 0) {
						*next = result.back().key;
					}
					return std::make_pair(result.size(), result.more);
				} else {
					int paramsBytesLength = versionToLength[VERSION];
					uint8_t tmp[paramsBytesLength];
					memset(tmp, 0, paramsBytesLength);
					tmp[0] = currentVersion;
					int posMatchIndex = versionToPosMatchIndex[VERSION];
					int posFetchLocalOnly = versionToPosFetchLocalOnly[VERSION];
					tmp[posMatchIndex] = codeMatchIndex;
					tmp[posMatchIndex + 1] = matchIndex; // little endian
					tmp[posFetchLocalOnly] = codeFetchLocalOnly;
					tmp[posFetchLocalOnly + 1] = fetchLocalOnly;
					StringRef str(tmp, paramsBytesLength);
					Key paramsBytes(str);
					MappedRangeResultV2 result = wait(tr->getMappedRangeV2(beginSelector,
					                                                       endSelector,
					                                                       mapper,
					                                                       paramsBytes,
					                                                       GetRangeLimits(limit, byteLimit),
					                                                       self->snapshot,
					                                                       Reverse::False));

					if (validate<MappedRangeResultV2, MappedKeyValueRefV2>(
					        result, limit, expectedBeginId, self, matchIndex, allMissing, fetchLocalOnly)) {
						continue;
					}
					if (result.size() != 0) {
						*next = result.back().key;
					}
					return std::make_pair(result.size(), result.more);
				}
			} catch (Error& e) {
				if ((self->BAD_MAPPER && e.code() == error_code_mapper_bad_index) ||
				    (currentVersion == INVALID_VERSION &&
				     e.code() == error_code_get_mapped_range_version_not_support) ||
				    (codeMatchIndex == code_invalid && e.code() == error_code_get_mapped_range_parsing_error) ||
				    (codeFetchLocalOnly == code_invalid && e.code() == error_code_get_mapped_range_parsing_error)) {
					TraceEvent("GetMappedRangeWorkloadExpectedErrorDetected").error(e);
					return std::make_pair(-1, 0);
				} else {
					std::cout << "error " << e.what() << std::endl;
					wait(tr->onError(e));
				}
				std::cout << "failed scanMappedRangeWithLimits" << std::endl;
			}
		}
	}

	ACTOR Future<Void> scanMappedRange(Database cx,
	                                   int beginId,
	                                   int endId,
	                                   Key mapper,
	                                   GetMappedRangeWorkload* self,
	                                   int matchIndex,
	                                   bool allMissing = false) {
		Key beginTuple = Tuple::makeTuple(prefix, INDEX, indexKey(beginId)).getDataAsStandalone();
		state KeySelector beginSelector = KeySelector(firstGreaterOrEqual(beginTuple));
		Key endTuple = Tuple::makeTuple(prefix, INDEX, indexKey(endId)).getDataAsStandalone();
		state KeySelector endSelector = KeySelector(firstGreaterOrEqual(endTuple));
		state int limit = 100;
		state int byteLimit = deterministicRandom()->randomInt(1, 9) * 10000;
		state int expectedBeginId = beginId;
		state KeyRef next;
		std::cout << "ByteLimit: " << byteLimit << " limit: " << limit
		          << " FRACTION_INDEX_BYTELIMIT_PREFETCH: " << SERVER_KNOBS->FRACTION_INDEX_BYTELIMIT_PREFETCH
		          << " MAX_PARALLEL_QUICK_GET_VALUE: " << SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE << std::endl;
		while (true) {
			std::pair<int, int> p = wait(self->scanMappedRangeWithLimits(cx,
			                                                             beginSelector,
			                                                             endSelector,
			                                                             mapper,
			                                                             limit,
			                                                             byteLimit,
			                                                             expectedBeginId,
			                                                             self,
			                                                             &next,
			                                                             matchIndex,
			                                                             allMissing));
			expectedBeginId += p.first;
			if (p.first == -1) {
				// expected error case
				return Void();
			}
			if (p.second) {
				if (p.first == 0) {
					// This is usually not expected.
					std::cout << "not result but have more, try again" << std::endl;
				} else {
					int size = allMissing ? indexSize : (indexSize + recordSize);
					int expectedCnt = limit;
					int indexByteLimit = byteLimit * SERVER_KNOBS->FRACTION_INDEX_BYTELIMIT_PREFETCH;
					int indexCountByteLimit = indexByteLimit / indexSize + (indexByteLimit % indexSize != 0);
					int indexCount = std::min(limit, indexCountByteLimit);
					// result set cannot be larger than the number of index fetched
					ASSERT(p.first <= indexCount);

					expectedCnt = std::min(expectedCnt, indexCount);
					int boundByRecord;
					if (SERVER_KNOBS->STRICTLY_ENFORCE_BYTE_LIMIT) {
						// might have 1 additional entry over the limit
						boundByRecord = byteLimit / size + (byteLimit % size != 0);
					} else {
						// might have 1 additional batch over the limit
						int roundSize = size * SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE;
						int round = byteLimit / roundSize + (byteLimit % roundSize != 0);
						boundByRecord = round * SERVER_KNOBS->MAX_PARALLEL_QUICK_GET_VALUE;
					}
					expectedCnt = std::min(expectedCnt, boundByRecord);
					// result could be smaller, in the case that begin and end key is not on the same SS
					// in this case, the range is severed in NativeAPI and a smaller end is set to the request
					ASSERT(p.first <= expectedCnt);
					beginSelector = KeySelector(firstGreaterThan(next));
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
		Key mapper = getMapper(self, false);
		Key beginTuple = Tuple::makeTuple(prefix, INDEX, indexKey(beginId)).getDataAsStandalone();
		KeySelector beginSelector = KeySelector(firstGreaterOrEqual(beginTuple));
		Key endTuple = Tuple::makeTuple(prefix, INDEX, indexKey(endId)).getDataAsStandalone();
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

		Key mapper = getMapper(self, false);
		// The scanned range cannot be too large to hit get_mapped_key_values_has_more. We have a unit validating the
		// error is thrown when the range is large.
		const double r = deterministicRandom()->random01();
		int matchIndex = MATCH_INDEX_ALL;
		if (r < 0.2) {
			matchIndex = MATCH_INDEX_NONE;
		} else if (r < 0.4) {
			matchIndex = MATCH_INDEX_MATCHED_ONLY;
		} else if (r < 0.6) {
			matchIndex = MATCH_INDEX_UNMATCHED_ONLY;
		} else if (r < 0.8) {
			matchIndex = MATCH_INDEX_TEST_710_API; // use 7.1 version API
		}
		state bool originalStrictlyEnforeByteLimit = SERVER_KNOBS->STRICTLY_ENFORCE_BYTE_LIMIT;
		(const_cast<ServerKnobs*> SERVER_KNOBS)->STRICTLY_ENFORCE_BYTE_LIMIT = deterministicRandom()->coinflip();
		wait(self->scanMappedRange(cx, 10, 490, mapper, self, matchIndex));

		{
			Key mapper = getMapper(self, true);
			wait(self->scanMappedRange(cx, 10, 490, mapper, self, MATCH_INDEX_UNMATCHED_ONLY, true));
		}

		// reset it to default
		(const_cast<ServerKnobs*> SERVER_KNOBS)->STRICTLY_ENFORCE_BYTE_LIMIT = originalStrictlyEnforeByteLimit;
		return Void();
	}

	static Key getMapper(GetMappedRangeWorkload* self, bool mapperForAllMissing) {
		Tuple mapperTuple;
		if (self->BAD_MAPPER) {
			mapperTuple << prefix << RECORD << "{K[xxx]}"_sr;
		} else {
			mapperTuple << prefix << RECORD << (mapperForAllMissing ? "{K[2]}"_sr : "{K[3]}"_sr);
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

WorkloadFactory<GetMappedRangeWorkload> GetMappedRangeWorkloadFactory;
