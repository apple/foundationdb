/*
 * SidebandSingle.cpp
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

#include "fdbclient/Knobs.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
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

	explicit SidebandSingleWorkload(WorkloadContext const& wcx)
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

	Future<Void> mutator(SidebandSingleWorkload* self, Database cx) {
		double lastTime = now();
		Version commitVersion{ 0 };
		bool unknown = false;

		while (true) {
			co_await poisson(&lastTime, 1.0 / self->operationsPerSecond);
			Transaction tr0(cx);
			Transaction tr(cx);
			uint64_t key = deterministicRandom()->randomUniqueID().hash();

			Standalone<StringRef> messageKey(format("Sideband/Message/%llx", key));
			// first set, this is the "old" value, always retry
			while (true) {
				Error err;
				try {
					tr0.set(messageKey, "oldbeef"_sr);
					co_await tr0.commit();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr0.onError(err);
			}
			// second set, the checker should see this, no retries on unknown result
			while (true) {
				Error err;
				try {
					tr.set(messageKey, "deadbeef"_sr);
					co_await tr.commit();
					commitVersion = tr.getCommittedVersion();
					break;
				} catch (Error& e) {
					err = e;
				}
				if (err.code() == error_code_commit_unknown_result) {
					unknown = true;
					++self->messages;
					self->interf.send(std::pair(key, invalidVersion));
					break;
				}
				co_await tr.onError(err);
			}
			if (unknown) {
				unknown = false;
				continue;
			}
			++self->messages;
			self->interf.send(std::pair(key, commitVersion));
		}
	}

	Future<Void> checker(SidebandSingleWorkload* self, Database cx) {
		// Required for GRV Cache to work in simulation.
		// Normally, MVC would set the shared state and it is verified upon setting the
		// transaction option for GRV Cache.
		// In simulation, explicitly initialize it when used for tests.
		cx->initSharedState();
		while (true) {
			// Pair represents <Key, commitVersion>
			std::pair<uint64_t, Version> message = co_await self->interf.getFuture();
			Standalone<StringRef> messageKey(format("Sideband/Message/%llx", message.first));
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					tr.setOption(FDBTransactionOptions::USE_GRV_CACHE);
					Optional<Value> val = co_await tr.get(messageKey);
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
						Transaction tr2(cx);
						Optional<Value> val2;
						while (true) {
							Error err;
							try {
								val2 = co_await tr2.get(messageKey);
								break;
							} catch (Error& e) {
								err = e;
							}
							TraceEvent("DebugSidebandNoCacheError").errorUnsuppressed(err);
							co_await tr2.onError(err);
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
					err = e;
				}
				TraceEvent("DebugSidebandCheckError").errorUnsuppressed(err);
				co_await tr.onError(err);
			}
		}
	}
};

WorkloadFactory<SidebandSingleWorkload> SidebandSingleWorkloadFactory;
