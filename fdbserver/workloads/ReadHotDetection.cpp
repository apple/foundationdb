/*
 * ReadHotDetection.cpp
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

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "BulkSetup.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/workloads.h"

struct ReadHotDetectionWorkload : TestWorkload {
	static constexpr auto NAME = "ReadHotDetection";

	int actorCount, keyCount;

	double testDuration, transactionsPerSecond;
	std::vector<Future<Void>> clients;
	Future<Void> readHotCheck;
	Key readKey;
	KeyRange wholeRange;
	bool passed;

	explicit ReadHotDetectionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 1000.0) / clientCount;
		actorCount = getOption(options, "actorsPerClient"_sr, transactionsPerSecond / 5);
		keyCount = getOption(options, "keyCount"_sr, 100);
		readKey = StringRef(format("testkey%08x", deterministicRandom()->randomInt(0, keyCount)));
	}

	Future<Void> setup(Database const& cx) override {
		Standalone<StringRef> largeValue;
		Standalone<StringRef> smallValue;
		largeValue = randomString(largeValue.arena(), 100000);
		smallValue = randomString(smallValue.arena(), 100);
		ReadYourWritesTransaction tr(cx);
		while (true) {
			Error err;
			try {
				for (int i = 0; i < keyCount; i++) {
					Standalone<StringRef> key = StringRef(format("testkey%08x", i));
					if (key == readKey) {
						tr.set(key, largeValue);
					} else {
						tr.set(key, deterministicRandom()->random01() > 0.8 ? largeValue : smallValue);
					}
				}
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		wholeRange = KeyRangeRef(""_sr, "\xff"_sr);
		// TraceEvent("RHDLog").detail("Phase", "DoneSetup");
	}

	Future<Void> start(Database const& cx) override {
		for (int c = 0; c < actorCount; c++) {
			clients.push_back(timeout(
			    keyReader(
			        cx->clone(), this, actorCount / transactionsPerSecond, deterministicRandom()->random01() > 0.4),
			    testDuration,
			    Void()));
		}
		readHotCheck = clientId == 0 ? _check(cx->clone(), this) : Void();
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		return passed;
	}

	Future<Void> _check(Database cx, ReadHotDetectionWorkload* self) {
		while (true) {
			Transaction tr(cx);
			Error err;
			try {
				StorageMetrics sm = co_await cx->getStorageMetrics(self->wholeRange, 100);
				(void)sm; // suppress unused variable warning
				// TraceEvent("RHDCheckPhaseLog")
				//     .detail("KeyRangeSize", sm.bytes)
				//     .detail("KeyRangeReadBandwidth", sm.bytesReadPerKSecond);
				Standalone<VectorRef<ReadHotRangeWithMetrics>> keyRanges =
				    co_await cx->getReadHotRanges(self->wholeRange);
				// TraceEvent("RHDCheckPhaseLog")
				//     .detail("KeyRangesSize", keyRanges.size())
				//     .detail("ReadKey", self->readKey.printable().c_str())
				//     .detail("KeyRangesBackBeginKey", keyRanges.back().begin)
				//     .detail("KeyRangesBackEndKey", keyRanges.back().end);
				// Loose check.
				for (const auto& kr : keyRanges) {
					if (kr.keys.contains(self->readKey)) {
						self->passed = true;
						co_return;
					}
				}
				// The key ranges deemed read hot does not contain the readKey, which is impossible here.
				// TraceEvent("RHDCheckPhaseFailed")
				// 	.detail("KeyRangesSize", keyRanges.size())
				// 	.detail("ReadKey", self->readKey.printable().c_str())
				// 	.detail("KeyRangesBackBeginKey", keyRanges.back().begin)
				// 	.detail("KeyRangesBackEndKey", keyRanges.back().end);
				// for(auto kr : keyRanges) {
				// 	TraceEvent("RHDCheckPhaseFailed").detail("KeyRagneBegin", kr.begin).detail("KeyRagneEnd",
				// kr.end);
				// }
				self->passed = false;
			} catch (Error& e) {
				err = e;
			}
			// TraceEvent("RHDCheckPhaseReadGotError").error(e);
			co_await tr.onError(err);
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> keyReader(Database cx, ReadHotDetectionWorkload* self, double delay, bool useReadKey) {
		double lastTime = now();
		while (true) {
			co_await poisson(&lastTime, delay);
			ReadYourWritesTransaction tr(cx);
			while (true) {
				Error err;
				try {
					Optional<Value> v = co_await tr.get(
					    useReadKey
					        ? self->readKey
					        : StringRef(format("testkey%08x", deterministicRandom()->randomInt(0, self->keyCount))));
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}
	}
	StringRef randomString(Arena& arena, int len, char firstChar = 'a', char lastChar = 'z') {
		++lastChar;
		StringRef s = makeString(len, arena);
		for (int i = 0; i < len; ++i) {
			*(uint8_t*)(s.begin() + i) = (uint8_t)deterministicRandom()->randomInt(firstChar, lastChar);
		}
		return s;
	}
};

WorkloadFactory<ReadHotDetectionWorkload> ReadHotDetectionWorkloadFactory;
