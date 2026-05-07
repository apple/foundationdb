/*
 * StreamingRangeRead.cpp
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

#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "BulkSetup.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include <cstring>

Future<Void> streamUsingGetRange(PromiseStream<RangeResult> results, Transaction* tr, KeyRange keys) {
	KeySelectorRef begin = firstGreaterOrEqual(keys.begin);
	KeySelectorRef end = firstGreaterOrEqual(keys.end);

	try {
		while (true) {
			GetRangeLimits limits(GetRangeLimits::ROW_LIMIT_UNLIMITED, 1e6);
			limits.minRows = 0;
			RangeResult rep = co_await tr->getRange(begin, end, limits, Snapshot::True);

			results.send(rep);

			if (!rep.more) {
				results.sendError(end_of_stream());
				co_return;
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

Future<Void> convertStream(PromiseStream<RangeResult> input, PromiseStream<KeyValue> output) {
	try {
		while (true) {
			RangeResult res = co_await input.getFuture();
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
}

struct StreamingRangeReadWorkload : KVWorkload {
	static constexpr auto NAME = "StreamingRangeRead";
	double testDuration;
	Future<Void> client;

	explicit StreamingRangeReadWorkload(WorkloadContext const& wcx) : KVWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 60.0);
	}

	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n, false), randomValue()); }

	Future<Void> setup(Database const& cx) override { return bulkSetup(cx, this, nodeCount, Promise<double>()); }
	Future<Void> start(Database const& cx) override {
		client = timeout(streamingClient(cx->clone()), testDuration, Void());
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		client = Void();
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Reads the database using both the normal get range API and the streaming API and compares the results
	Future<Void> streamingClient(Database cx) {
		Transaction tr(cx);
		Key next;
		Future<Void> rateLimit = delay(0.01);
		while (true) {
			PromiseStream<RangeResult> streamRaw;
			PromiseStream<RangeResult> compareRaw;
			PromiseStream<KeyValue> streamResults;
			PromiseStream<KeyValue> compareResults;
			Future<Void> compareConvert;
			Future<Void> streamConvert;
			Future<Void> compare;
			Future<Void> stream;

			Error err;
			bool failed = false;
			try {
				compareConvert = convertStream(compareRaw, compareResults);
				streamConvert = convertStream(streamRaw, streamResults);
				compare = streamUsingGetRange(compareRaw, &tr, KeyRangeRef(next, normalKeys.end));
				stream = tr.getRangeStream(streamRaw,
				                           KeySelector(firstGreaterOrEqual(next), next.arena()),
				                           KeySelector(firstGreaterOrEqual(normalKeys.end)),
				                           GetRangeLimits());
				while (true) {
					Optional<KeyValue> cmp;
					Optional<KeyValue> res;
					try {
						KeyValue _cmp = co_await streamResults.getFuture();
						cmp = _cmp;
					} catch (Error& e) {
						if (e.code() != error_code_end_of_stream) {
							throw;
						}
						cmp = Optional<KeyValue>();
					}
					try {
						KeyValue _res = co_await compareResults.getFuture();
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
				err = e;
				failed = true;
			}
			if (failed) {
				co_await tr.onError(err);
			}
			co_await rateLimit;
			rateLimit = delay(0.01);
		}
	}
};

WorkloadFactory<StreamingRangeReadWorkload> StreamingRangeReadWorkloadFactory;
