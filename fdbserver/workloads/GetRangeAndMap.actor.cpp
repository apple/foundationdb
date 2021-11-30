/*
 * GetRangeAndMap.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/simulator.h"
#include "fdbclient/MutationLogReader.actor.h"
#include "fdbclient/Tuple.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const Value EMPTY = Tuple().pack();
const KeyRef prefix = "prefix"_sr;
const KeyRef RECORD = "RECORD"_sr;
const KeyRef INDEX = "INDEX"_sr;

struct GetRangeAndMapWorkload : TestWorkload {
	bool enabled;
	const bool BAD_MAPPER = deterministicRandom()->random01() < 0.1;

	GetRangeAndMapWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
	}

	std::string description() const override { return "GetRangeAndMap"; }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return _start(cx, this);
		}
		return Void();
	}

	static Key primaryKey(int i) { return Key(format("primary-key-of-record-%08d", i)); }
	static Key indexKey(int i) { return Key(format("index-key-of-record-%08d", i)); }
	static Value dataOfRecord(int i) { return Key(format("data-of-record-%08d", i)); }

	static Key indexEntryKey(int i) {
		return Tuple().append(prefix).append(INDEX).append(indexKey(i)).append(primaryKey(i)).pack();
	}
	static Key recordKey(int i) { return Tuple().append(prefix).append(RECORD).append(primaryKey(i)).pack(); }
	static Value recordValue(int i) { return Tuple().append(dataOfRecord(i)).pack(); }

	ACTOR Future<Void> fillInRecords(Database cx, int n) {
		loop {
			std::cout << "start fillInRecords n=" << n << std::endl;
			// TODO: When n is large, split into multiple transactions.
			state Transaction tr(cx);
			try {
				tr.reset();
				for (int i = 0; i < n; i++) {
					tr.set(recordKey(i), recordValue(i));
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
		const KeyValueRef* it = result.begin();
		for (; it != result.end(); it++) {
			std::cout << "key=" << it->key.printable() << ", value=" << it->value.printable() << std::endl;
		}
	}

	ACTOR Future<Void> scanRange(Database cx, KeyRangeRef range) {
		std::cout << "start scanRange " << range.toString() << std::endl;
		// TODO: When n is large, split into multiple transactions.
		state Transaction tr(cx);
		try {
			tr.reset();
			RangeResult result = wait(tr.getRange(range, CLIENT_KNOBS->TOO_MANY));
			//			showResult(result);
		} catch (Error& e) {
			wait(tr.onError(e));
		}
		std::cout << "finished scanRange" << std::endl;
		return Void();
	}

	ACTOR Future<Void> scanRangeAndFlatMap(Database cx,
	                                       int beginId,
	                                       int endId,
	                                       Key mapper,
	                                       GetRangeAndMapWorkload* self) {
		Key someIndexesBegin = Tuple().append(prefix).append(INDEX).append(indexKey(beginId)).getDataAsStandalone();
		Key someIndexesEnd = Tuple().append(prefix).append(INDEX).append(indexKey(endId)).getDataAsStandalone();
		state KeyRange range = KeyRangeRef(someIndexesBegin, someIndexesEnd);

		std::cout << "start scanRangeAndFlatMap " << range.toString() << std::endl;
		// TODO: When n is large, split into multiple transactions.
		state Transaction tr(cx);
		try {
			tr.reset();
			RangeResult result =
			    wait(tr.getRangeAndFlatMap(KeySelector(firstGreaterOrEqual(range.begin), range.arena()),
			                               KeySelector(firstGreaterOrEqual(range.end), range.arena()),
			                               mapper,
			                               GetRangeLimits(CLIENT_KNOBS->TOO_MANY),
			                               Snapshot::True));
			//			showResult(result);
			if (self->BAD_MAPPER) {
				TraceEvent("GetRangeAndMapWorkloadShouldNotReachable").detail("ResultSize", result.size());
			}
			// Examples:
			// key=\x01prefix\x00\x01RECORD\x00\x01primary-key-of-record-2\x00, value=\x01data-of-record-2\x00
			// key=\x01prefix\x00\x01RECORD\x00\x01primary-key-of-record-3\x00, value=\x01data-of-record-3\x00
			std::cout << "result.size()=" << result.size() << std::endl;
			std::cout << "result.more=" << result.more << std::endl;
			ASSERT(result.size() == endId - beginId);
			const KeyValueRef* it = result.begin();
			int id = beginId;
			for (; it != result.end(); it++) {
				ASSERT(it->key == recordKey(id));
				ASSERT(it->value == recordValue(id));
				id++;
			}
		} catch (Error& e) {
			if (self->BAD_MAPPER && e.code() == error_code_mapper_bad_index) {
				TraceEvent("GetRangeAndMapWorkloadBadMapperDetected").error(e);
			} else {
				wait(tr.onError(e));
			}
		}
		std::cout << "finished scanRangeAndFlatMap" << std::endl;
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, GetRangeAndMapWorkload* self) {
		TraceEvent("GetRangeAndMapWorkloadConfig").detail("BadMapper", self->BAD_MAPPER);

		// TODO: Use toml to config
		wait(self->fillInRecords(cx, 200));

		wait(self->scanRange(cx, normalKeys));

		//		wait(self->scanRange(cx, someIndexes));

		Tuple mapperTuple;
		if (self->BAD_MAPPER) {
			mapperTuple << prefix << RECORD << "{K[xxx]}"_sr;
		} else {
			mapperTuple << prefix << RECORD << "{K[3]}"_sr;
		}
		Key mapper = mapperTuple.getDataAsStandalone();
		// The scanned range cannot be too large to hit get_key_values_and_map_has_more. We have a unit validating the
		// error is thrown when the range is large.
		wait(self->scanRangeAndFlatMap(cx, 10, 190, mapper, self));
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<GetRangeAndMapWorkload> GetRangeAndMapWorkloadFactory("GetRangeAndMap");
