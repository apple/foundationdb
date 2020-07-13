/*
 * ReadAfterWrite.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include <vector>

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static constexpr int SAMPLE_SIZE = 10000;

// If the log->storage propagation delay is longer than 1 second, then it's likely that our read
// will see a `future_version` error from the storage server.  We need to retry the read until
// a value is returned, or a different error is thrown.
ACTOR Future<double> latencyOfRead(Transaction* tr, Key k) {
	state double start = timer();
	loop {
		try {
			wait(success(tr->get(k)));
			break;
		} catch (Error &e) {
			if (e.code() == error_code_future_version) {
				continue;
			}
			throw;
		}
	}
	return timer() - start;
}

// Measure the latency of a storage server making a committed value available for reading.
struct ReadAfterWriteWorkload : KVWorkload {

	double testDuration;
	ContinuousSample<double> propagationLatency;

	ReadAfterWriteWorkload(WorkloadContext const& wcx) : KVWorkload(wcx), propagationLatency(SAMPLE_SIZE) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
	}

	virtual std::string description() { return "ReadAfterWriteWorkload"; }

	virtual Future<Void> setup(Database const& cx) { return Void(); }

	ACTOR static Future<Void> benchmark(Database cx, ReadAfterWriteWorkload* self) {
		loop {
			state Key key = self->getRandomKey();
			state Transaction writeTr(cx);
			state Transaction baselineReadTr(cx);
			state Transaction afterWriteTr(cx);

			try {
				state Version readVersion = wait(writeTr.getReadVersion());

				// We do a read in this writeTransaction only to enforce that `readVersion` is already on a storage
				// server after we commit.  Its existence or non-existence is irrelevant.  We write back the exact same
				// value (or clear the key, if empty) so that the database state is not mutated.  This means this
				// workload can be paired with any other workload, and it won't affect any results.
				Optional<Value> value = wait( writeTr.get(key) );
				if (value.present()) {
					writeTr.set(key, value.get());
				} else {
					writeTr.clear(key);
				}

				wait(writeTr.commit());

				Version commitVersion = writeTr.getCommittedVersion();

				baselineReadTr.setVersion(readVersion);
				afterWriteTr.setVersion(commitVersion);

				state double baselineLatency = 0;
				state double afterWriteLatency = 0;

				wait(store(baselineLatency, latencyOfRead(&baselineReadTr, key)) &&
				     store(afterWriteLatency, latencyOfRead(&afterWriteTr, key)));

				// By reading the same key at two different versions, we should be able to measure the latency of the
				// network, the storage server overhead, and the propagation delay, and then with our baseline read,
				// subtract out the network and the storage server overhead, leaving only the propagation delay.
				self->propagationLatency.addSample(std::max<double>(afterWriteLatency - baselineLatency, 0));
			} catch (Error& e) {
				wait(writeTr.onError(e));
			}
		}
	}

	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	ACTOR Future<Void> _start(Database cx, ReadAfterWriteWorkload* self) {
		state Future<Void> lifetime = benchmark(cx, self);
		wait(delay(self->testDuration));
		return Void();
	}

	virtual Future<bool> check(Database const& cx) override { return true; }

	virtual void getMetrics(std::vector<PerfMetric>& m) {
		m.emplace_back("Mean Latency (ms)", 1000 * propagationLatency.mean(), true);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * propagationLatency.median(), true);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * propagationLatency.percentile(0.90), true);
		m.emplace_back("99% Latency (ms, averaged)", 1000 * propagationLatency.percentile(0.99), true);
		m.emplace_back("Max Latency (ms, averaged)", 1000 * propagationLatency.max(), true);
	}
};

WorkloadFactory<ReadAfterWriteWorkload> ReadAfterWriteWorkloadFactory("ReadAfterWrite");
