/*
 * IndexPrefetchDemo.actor.cpp
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
#include "fdbclient/Tuple.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "flow/actorcompiler.h" // This must be the last #include.

const KeyRef prefix = "prefix"_sr;
const KeyRef RECORD = "RECORD"_sr;
const KeyRef INDEX = "INDEX"_sr;

struct IndexPrefetchDemoWorkload : TestWorkload {
	bool enabled;
	const bool BAD_MAPPER = deterministicRandom()->random01() < 0.1;

	IndexPrefetchDemoWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
	}

	std::string description() const override { return "IndexPrefetchDemo"; }

	Future<Void> start(Database const& cx) override {
		if (enabled) {
			return _start(cx, this);
		}
		return Void();
	}

	static Key primaryKey(int i) { return KeyRef("primary-key-of-record-" + std::to_string(i)); }
	static Key indexKey(int i) { return KeyRef("index-key-of-record-" + std::to_string(i)); }
	static Key data(int i) { return KeyRef("data-of-record-" + std::to_string(i)); }

	ACTOR Future<Void> fillInRecords(Database cx, int n) {
		std::cout << "start fillInRecords n=" << n << std::endl;
		// TODO: When n is large, split into multiple transactions.
		state Transaction tr(cx);
		try {
			tr.reset();
			for (int i = 0; i < n; i++) {
				tr.set(Tuple().append(prefix).append(RECORD).append(primaryKey(i)).pack(),
				       Tuple().append(data(i)).pack());
				tr.set(Tuple().append(prefix).append(INDEX).append(indexKey(i)).append(primaryKey(i)).pack(),
				       Tuple().pack());
			}
			wait(tr.commit());
			std::cout << "finished fillInRecords" << std::endl;
		} catch (Error& e) {
			std::cout << "failed fillInRecords" << std::endl;
			wait(tr.onError(e));
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
			showResult(result);
		} catch (Error& e) {
			wait(tr.onError(e));
		}
		std::cout << "finished scanRange" << std::endl;
		return Void();
	}

	ACTOR Future<Void> scanRangeAndFlatMap(Database cx, KeyRange range, Key mapper, IndexPrefetchDemoWorkload* self) {
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
			                               true));
			showResult(result);
			if (self->BAD_MAPPER) {
				TraceEvent("IndexPrefetchDemoWorkloadShouldNotReachable").detail("ResultSize", result.size());
			}
			// result size: 2
			// key=\x01prefix\x00\x01RECORD\x00\x01primary-key-of-record-2\x00, value=\x01data-of-record-2\x00
			// key=\x01prefix\x00\x01RECORD\x00\x01primary-key-of-record-3\x00, value=\x01data-of-record-3\x00
		} catch (Error& e) {
			if (self->BAD_MAPPER && e.code() == error_code_mapper_bad_index) {
				TraceEvent("IndexPrefetchDemoWorkloadBadMapperDetected").error(e);
			} else {
				wait(tr.onError(e));
			}
		}
		std::cout << "finished scanRangeAndFlatMap" << std::endl;
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, IndexPrefetchDemoWorkload* self) {
		TraceEvent("IndexPrefetchDemoWorkloadConfig").detail("BadMapper", self->BAD_MAPPER);

		// TODO: Use toml to config
		wait(self->fillInRecords(cx, 5));

		wait(self->scanRange(cx, normalKeys));

		Key someIndexesBegin = Tuple().append(prefix).append(INDEX).append(indexKey(2)).getDataAsStandalone();
		Key someIndexesEnd = Tuple().append(prefix).append(INDEX).append(indexKey(4)).getDataAsStandalone();
		state KeyRange someIndexes = KeyRangeRef(someIndexesBegin, someIndexesEnd);
		wait(self->scanRange(cx, someIndexes));

		Tuple mapperTuple;
		if (self->BAD_MAPPER) {
			mapperTuple << prefix << RECORD << "{K[xxx]}"_sr;
		} else {
			mapperTuple << prefix << RECORD << "{K[3]}"_sr;
		}
		Key mapper = mapperTuple.getDataAsStandalone();
		wait(self->scanRangeAndFlatMap(cx, someIndexes, mapper, self));
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<IndexPrefetchDemoWorkload> IndexPrefetchDemoWorkloadFactory("IndexPrefetchDemo");
