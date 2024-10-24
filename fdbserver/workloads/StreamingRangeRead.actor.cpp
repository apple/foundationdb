/*
 * StreamingRangeRead.actor.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include <cstring>

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR Future<Void> streamUsingGetRange(PromiseStream<RangeResult> results, Transaction* tr, KeyRange keys) {
	state KeySelectorRef begin = firstGreaterOrEqual(keys.begin);
	state KeySelectorRef end = firstGreaterOrEqual(keys.end);

	try {
		loop {
			GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED, 1e6);
			limits.minRows = 0;
			state RangeResult rep = wait(tr->getRange(begin, end, limits, Snapshot::True));

			results.send(rep);

			if (!rep.more) {
				results.sendError(end_of_stream());
				return Void();
			}

			begin = rep.nextBeginKeySelector();
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		results.sendError(e);
		throw;
	}
}

ACTOR Future<Void> convertStream(PromiseStream<RangeResult> input, PromiseStream<KeyValue> output) {
	try {
		loop {
			RangeResult res = waitNext(input.getFuture());
			for (auto& kv : res) {
				output.send(kv);
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		output.sendError(e);
	}
	return Void();
}

struct StreamingRangeReadWorkload : KVWorkload {
	static constexpr auto NAME = "StreamingRangeRead";
	double testDuration;
	std::string valueString;
	Future<Void> client;

	StreamingRangeReadWorkload(WorkloadContext const& wcx) : KVWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
		valueString = std::string(maxValueBytes, '.');
	}

	Value randomValue() {
		return StringRef((uint8_t*)valueString.c_str(),
		                 deterministicRandom()->randomInt(minValueBytes, maxValueBytes + 1));
	}

	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n, false), randomValue()); }

	Future<Void> setup(Database const& cx) override { return bulkSetup(cx, this, nodeCount, Promise<double>()); }
	Future<Void> start(Database const& cx) override {
		client = timeout(streamingClient(cx->clone(), this), testDuration, Void());
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Void();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Reads the database using both the normal get range API and the streaming API and compares the results
	ACTOR Future<Void> streamingClient(Database cx, StreamingRangeReadWorkload* self) {
		state Transaction tr(cx);
		state Key next;
		state Future<Void> rateLimit = delay(0.01);
		loop {
			state PromiseStream<RangeResult> streamRaw;
			state PromiseStream<RangeResult> compareRaw;
			state PromiseStream<KeyValue> streamResults;
			state PromiseStream<KeyValue> compareResults;

			try {
				state Future<Void> compareConvert = convertStream(compareRaw, compareResults);
				state Future<Void> streamConvert = convertStream(streamRaw, streamResults);
				state Future<Void> compare = streamUsingGetRange(compareRaw, &tr, KeyRangeRef(next, normalKeys.end));
				state Future<Void> stream = tr.getRangeStream(streamRaw,
				                                              KeySelector(firstGreaterOrEqual(next), next.arena()),
				                                              KeySelector(firstGreaterOrEqual(normalKeys.end)),
				                                              GetRangeLimits());
				loop {
					state Optional<KeyValue> cmp;
					state Optional<KeyValue> res;
					try {
						KeyValue _cmp = waitNext(streamResults.getFuture());
						cmp = _cmp;
					} catch (Error& e) {
						if (e.code() != error_code_end_of_stream) {
							throw;
						}
						cmp = Optional<KeyValue>();
					}
					try {
						KeyValue _res = waitNext(compareResults.getFuture());
						res = _res;
					} catch (Error& e) {
						if (e.code() != error_code_end_of_stream) {
							throw;
						}
						res = Optional<KeyValue>();
					}
					if (cmp != res) {
						TraceEvent(SevError, "RangeStreamMismatch");
						ASSERT(false);
					}
					if (cmp.present()) {
						next = keyAfter(cmp.get().key);
					} else {
						next = Key();
						break;
					}
				}
			} catch (Error& e) {
				wait(tr.onError(e));
			}
			wait(rateLimit);
			rateLimit = delay(0.01);
		}
	}
};

WorkloadFactory<StreamingRangeReadWorkload> StreamingRangeReadWorkloadFactory;
