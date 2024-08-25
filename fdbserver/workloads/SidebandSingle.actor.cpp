/*
 * SidebandSingle.actor.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * This workload is modelled off the Sideband workload, except it uses a single
 * mutator and checker rather than several. In addition to ordinary consistency checks,
 * it also checks the consistency of the cached read versions when using the
 * USE_GRV_CACHE transaction option, specifically when commit_unknown_result
 * produces a maybe/maybe-not written scenario. It makes sure that a cached read of an
 * unknown result matches the regular read of that same key and is not too stale.
 */

struct SidebandSingleWorkload : TestWorkload {
	static constexpr auto NAME = "SidebandSingle";

	double testDuration, operationsPerSecond;
	// Pair represents <Key, commitVersion>
	PromiseStream<std::pair<uint64_t, Version>> interf;

	std::vector<Future<Void>> clients;
	PerfIntCounter messages, consistencyErrors, keysUnexpectedlyPresent;

	SidebandSingleWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), messages("Messages"), consistencyErrors("Causal Consistency Errors"),
	    keysUnexpectedlyPresent("KeysUnexpectedlyPresent") {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		operationsPerSecond = getOption(options, "operationsPerSecond"_sr, 50.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (clientId != 0)
			return Void();

		clients.push_back(mutator(this, cx));
		clients.push_back(checker(this, cx));
		return delay(testDuration);
	}
	Future<bool> check(Database const& cx) override {
		if (clientId != 0)
			return true;
		int errors = 0;
		for (int c = 0; c < clients.size(); c++) {
			errors += clients[c].isError();
		}
		if (errors)
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were client errors.");
		clients.clear();
		if (consistencyErrors.getValue())
			TraceEvent(SevError, "TestFailure").detail("Reason", "There were causal consistency errors.");
		return !errors && !consistencyErrors.getValue();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(messages.getMetric());
		m.push_back(consistencyErrors.getMetric());
		m.push_back(keysUnexpectedlyPresent.getMetric());
	}

	ACTOR Future<Void> mutator(SidebandSingleWorkload* self, Database cx) {
		state double lastTime = now();
		state Version commitVersion;
		state bool unknown = false;

		loop {
			wait(poisson(&lastTime, 1.0 / self->operationsPerSecond));
			state Transaction tr0(cx);
			state Transaction tr(cx);
			state uint64_t key = deterministicRandom()->randomUniqueID().hash();

			state Standalone<StringRef> messageKey(format("Sideband/Message/%llx", key));
			// first set, this is the "old" value, always retry
			loop {
				try {
					tr0.set(messageKey, "oldbeef"_sr);
					wait(tr0.commit());
					break;
				} catch (Error& e) {
					wait(tr0.onError(e));
				}
			}
			// second set, the checker should see this, no retries on unknown result
			loop {
				try {
					tr.set(messageKey, "deadbeef"_sr);
					wait(tr.commit());
					commitVersion = tr.getCommittedVersion();
					break;
				} catch (Error& e) {
					if (e.code() == error_code_commit_unknown_result) {
						unknown = true;
						++self->messages;
						self->interf.send(std::pair(key, invalidVersion));
						break;
					}
					wait(tr.onError(e));
				}
			}
			if (unknown) {
				unknown = false;
				continue;
			}
			++self->messages;
			self->interf.send(std::pair(key, commitVersion));
		}
	}

	ACTOR Future<Void> checker(SidebandSingleWorkload* self, Database cx) {
		// Required for GRV Cache to work in simulation.
		// Normally, MVC would set the shared state and it is verified upon setting the
		// transaction option for GRV Cache.
		// In simulation, explicitly initialize it when used for tests.
		cx->initSharedState();
		loop {
			// Pair represents <Key, commitVersion>
			state std::pair<uint64_t, Version> message = waitNext(self->interf.getFuture());
			state Standalone<StringRef> messageKey(format("Sideband/Message/%llx", message.first));
			state Transaction tr(cx);
			loop {
				try {
					tr.setOption(FDBTransactionOptions::USE_GRV_CACHE);
					state Optional<Value> val = wait(tr.get(messageKey));
					if (!val.present()) {
						TraceEvent(SevError, "CausalConsistencyError1")
						    .detail("MessageKey", messageKey.toString().c_str())
						    .detail("RemoteCommitVersion", message.second)
						    .detail("LocalReadVersion",
						            tr.getReadVersion().get()); // will assert that ReadVersion is set
						++self->consistencyErrors;
					} else if (val.get() != "deadbeef"_sr) {
						// If we read something NOT "deadbeef" and there was no commit_unknown_result,
						// the cache somehow read a stale version of our key
						if (message.second != invalidVersion) {
							TraceEvent(SevError, "CausalConsistencyError2")
							    .detail("MessageKey", messageKey.toString().c_str());
							++self->consistencyErrors;
							break;
						}
						// check again without cache, and if it's the same, that's expected
						state Transaction tr2(cx);
						state Optional<Value> val2;
						loop {
							try {
								wait(store(val2, tr2.get(messageKey)));
								break;
							} catch (Error& e) {
								TraceEvent("DebugSidebandNoCacheError").errorUnsuppressed(e);
								wait(tr2.onError(e));
							}
						}
						if (val != val2) {
							TraceEvent(SevError, "CausalConsistencyError3")
							    .detail("MessageKey", messageKey.toString().c_str())
							    .detail("Val1", val)
							    .detail("Val2", val2)
							    .detail("RemoteCommitVersion", message.second)
							    .detail("LocalReadVersion",
							            tr.getReadVersion().get()); // will assert that ReadVersion is set
							++self->consistencyErrors;
						}
					}
					break;
				} catch (Error& e) {
					TraceEvent("DebugSidebandCheckError").errorUnsuppressed(e);
					wait(tr.onError(e));
				}
			}
		}
	}
};

WorkloadFactory<SidebandSingleWorkload> SidebandSingleWorkloadFactory;
