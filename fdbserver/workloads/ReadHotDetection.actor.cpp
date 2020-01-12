/*
 * ReadHotDetection.actor.cpp
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

#include "fdbrpc/ContinuousSample.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ReadHotDetectionWorkload : TestWorkload {
	int actorCount, rowCount;

	double testDuration, transactionsPerSecond;
	vector<Future<Void>> clients;
	Key readKey;
	KeyRange wholeRange;

	ReadHotDetectionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 120.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 1000.0) / clientCount;
		actorCount = getOption(options, LiteralStringRef("actorsPerClient"), transactionsPerSecond / 5);
		rowCount = getOption(options, LiteralStringRef("rowCount"), 1000);
	}

	virtual std::string description() { return "ReadHotDetection"; }

	virtual Future<Void> setup(Database const& cx) { return _setup(cx, this); }

	virtual Future<Void> start(Database const& cx) {
		int group = deterministicRandom()->randomInt(0, 100);
		int nodeIndex = deterministicRandom()->randomInt(0, this->rowCount / 100);
		this->readKey = Key(format("key%08x%08x", group, nodeIndex));
		for (int c = 0; c < actorCount; c++) {
			clients.push_back(timeout(keyReader(cx->clone(), this, actorCount / transactionsPerSecond, this->readKey),
			                          testDuration, Void()));
		}
		return delay(testDuration);
	}

	virtual Future<bool> check(Database const& cx) {
		if (clientId != 0) return true;
		return _check(cx, this);
	}

	ACTOR Future<Void> _setup(Database cx, ReadHotDetectionWorkload* self) {
		state int g = 0;
		state Standalone<StringRef> largeValue;
		largeValue = self->randomString(largeValue.arena(), 20);
		for (; g < 100; g++) {
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					for (int i = 0; i < self->rowCount / 100; i++) {
						tr.set(StringRef(format("key%08x%08x", g, i)), largeValue);
					}
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
		self->wholeRange = KeyRangeRef(KeyRef(format("key%08x%08x", 0, 0)),
		                               KeyRef(format("key%08x%08x", 99, self->rowCount / 100 - 1)));
		// TraceEvent("RHDLog").detail("Phase", "DoneSetup");
		return Void();
	}

	ACTOR Future<bool> _check(Database cx, ReadHotDetectionWorkload* self) {
		state Transaction tr(cx);
		try {
			StorageMetrics sm = wait(tr.getStorageMetrics(self->wholeRange, 100));
			// TraceEvent("RHDCheckPhaseLog")
			//     .detail("KeyRangeSize", sm.bytes)
			//     .detail("KeyRangeReadBandwith", sm.bytesReadPerKSecond);
			Standalone<VectorRef<KeyRangeRef>> keyRanges = wait(tr.getReadHotRanges(self->wholeRange));
			// TraceEvent("RHDCheckPhaseLog")
			//     .detail("KeyRangesSize", keyRanges.size())
			//     .detail("ReadKey", self->readKey.printable().c_str())
			//     .detail("KeyRangesBackBeginKey", keyRanges.back().begin)
			//     .detail("KeyRangesBackEndKey", keyRanges.back().end);
			// Loose check.
			if (keyRanges.size() != 1 || !keyRanges.back().contains(self->readKey)) {
				// TraceEvent("RHDCheckPhaseFailed")
				//     .detail("KeyRangesSize", keyRanges.size())
				//     .detail("ReadKey", self->readKey.printable().c_str())
				//     .detail("KeyRangesBackBeginKey", keyRanges.back().begin)
				//     .detail("KeyRangesBackEndKey", keyRanges.back().end);
				return false;
			}
		} catch (Error& e) {
			// TraceEvent("RHDCheckPhaseReadGotError").error(e);
			wait(tr.onError(e));
		}

		return true;
	}

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR Future<Void> keyReader(Database cx, ReadHotDetectionWorkload* self, double delay, KeyRef readKey) {
		state double lastTime = now();
		loop {
			wait(poisson(&lastTime, delay));
			state ReadYourWritesTransaction tr(cx);
			loop {
				try {
					Optional<Value> v = wait(tr.get(readKey));
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
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

WorkloadFactory<ReadHotDetectionWorkload> ReadHotDetectionWorkloadFactory("ReadHotDetection");
