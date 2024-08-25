/*
 * GetRangeStream.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct GetRangeStream : TestWorkload {
	static constexpr auto NAME = "GetRangeStream";
	PerfIntCounter bytesRead;
	bool useGetRange;
	Key begin;
	Key end;
	bool printKVPairs;

	GetRangeStream(WorkloadContext const& wcx) : TestWorkload(wcx), bytesRead("BytesRead") {
		useGetRange = getOption(options, "useGetRange"_sr, false);
		begin = getOption(options, "begin"_sr, normalKeys.begin);
		end = getOption(options, "end"_sr, normalKeys.end);
		printKVPairs = getOption(options, "printKVPairs"_sr, false);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		return clientId != 0 ? Void() : useGetRange ? fdbClientGetRange(cx, this) : fdbClientStream(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override { m.push_back(bytesRead.getMetric()); }

	ACTOR static Future<Void> logThroughput(GetRangeStream* self, Key* next) {
		loop {
			state int64_t last = self->bytesRead.getValue();
			state double before = g_network->now();
			wait(delay(1));
			state double after = g_network->now();
			if (after > before) {
				printf("throughput: %g bytes/s, next: %s\n",
				       (self->bytesRead.getValue() - last) / (after - before),
				       printable(*next).c_str());
			}
		}
	}

	ACTOR static Future<Void> fdbClientGetRange(Database db, GetRangeStream* self) {
		state Transaction tx(db);
		state Key next = self->begin;
		state Future<Void> logFuture = logThroughput(self, &next);
		loop {
			try {
				Standalone<RangeResultRef> range = wait(
				    tx.getRange(KeySelector(firstGreaterOrEqual(next), next.arena()),
				                KeySelector(firstGreaterOrEqual(self->end)),
				                GetRangeLimits(GetRangeLimits::ROW_LIMIT_UNLIMITED, CLIENT_KNOBS->REPLY_BYTE_LIMIT)));
				for (const auto& [k, v] : range) {
					if (self->printKVPairs) {
						printf("%s -> %s\n", printable(k).c_str(), printable(v).c_str());
					}
					self->bytesRead += k.size() + v.size();
				}
				if (!range.more) {
					break;
				}
				next = keyAfter(range.back().key);
			} catch (Error& e) {
				wait(tx.onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> fdbClientStream(Database db, GetRangeStream* self) {
		state Transaction tx(db);
		state Key next = self->begin;
		state Future<Void> logFuture = logThroughput(self, &next);
		loop {
			state PromiseStream<Standalone<RangeResultRef>> results;
			try {
				state Future<Void> stream = tx.getRangeStream(results,
				                                              KeySelector(firstGreaterOrEqual(next), next.arena()),
				                                              KeySelector(firstGreaterOrEqual(self->end)),
				                                              GetRangeLimits());
				loop {
					Standalone<RangeResultRef> range = waitNext(results.getFuture());
					for (const auto& [k, v] : range) {
						if (self->printKVPairs) {
							printf("%s -> %s\n", printable(k).c_str(), printable(v).c_str());
						}
						self->bytesRead += k.size() + v.size();
					}
					if (range.size()) {
						next = keyAfter(range.back().key);
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_end_of_stream) {
					break;
				}
				wait(tx.onError(e));
			}
		}
		return Void();
	}
};

WorkloadFactory<GetRangeStream> GetRangeStreamWorkloadFactory;
