/*
 * PrivateEndpoints.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/workloads.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace {

struct PrivateEndpoints : TestWorkload {
	static constexpr const char* WorkloadName = "PrivateEndpoints";
	bool success = true;
	int numSuccesses = 0;
	double startAfter;
	double runFor;

	std::vector<std::function<Future<Void>(Reference<AsyncVar<ClientDBInfo>> const&)>> testFunctions;

	template <class T>
	static Optional<T> getRandom(std::vector<T> const& v) {
		if (v.empty()) {
			return Optional<T>();
		} else {
			return deterministicRandom()->randomChoice(v);
		}
	}

	template <class T>
	static Optional<T> getInterface(Reference<AsyncVar<ClientDBInfo>> const& clientDBInfo) {
		if constexpr (std::is_same_v<T, GrvProxyInterface>) {
			return getRandom(clientDBInfo->get().grvProxies);
		} else if constexpr (std::is_same_v<T, CommitProxyInterface>) {
			return getRandom(clientDBInfo->get().commitProxies);
		} else {
			ASSERT(false); // don't know how to handle this type
		}
	}

	ACTOR template <class T>
	static Future<Void> assumeFailure(Future<T> f) {
		try {
			T t = wait(f);
			(void)t;
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			} else if (e.code() == error_code_unauthorized_attempt) {
				TraceEvent("SuccessPrivateEndpoint").log();
			} else if (e.code() == error_code_request_maybe_delivered) {
				// this is also fine, because even when calling private endpoints
				// we might see connection failures
				TraceEvent("SuccessRequestMaybeDelivered").log();
			} else {
				TraceEvent(SevError, "WrongErrorCode").error(e);
			}
		}
		return Void();
	}

	template <class I, class RT>
	void addTestFor(RequestStream<RT, false> I::*channel) {
		testFunctions.push_back([channel](Reference<AsyncVar<ClientDBInfo>> const& clientDBInfo) {
			auto optintf = getInterface<I>(clientDBInfo);
			if (!optintf.present()) {
				return clientDBInfo->onChange();
			}
			RequestStream<RT> s = optintf.get().*channel;
			RT req;
			return assumeFailure(deterministicRandom()->coinflip() ? throwErrorOr(s.tryGetReply(req))
			                                                       : s.getReply(req));
		});
	}

	explicit PrivateEndpoints(WorkloadContext const& wcx) : TestWorkload(wcx) {
		// The commented out request streams below can't be default initialized properly
		// as they won't initialize all of their memory which causes valgrind to complain.
		startAfter = getOption(options, "startAfter"_sr, 10.0);
		runFor = getOption(options, "runFor"_sr, 10.0);
		addTestFor(&GrvProxyInterface::waitFailure);
		addTestFor(&GrvProxyInterface::getHealthMetrics);
		// addTestFor(&CommitProxyInterface::getStorageServerRejoinInfo);
		addTestFor(&CommitProxyInterface::waitFailure);
		// addTestFor(&CommitProxyInterface::txnState);
		// addTestFor(&CommitProxyInterface::getHealthMetrics);
		// addTestFor(&CommitProxyInterface::proxySnapReq);
		addTestFor(&CommitProxyInterface::exclusionSafetyCheckReq);
		// addTestFor(&CommitProxyInterface::getDDMetrics);
	}
	std::string description() const override { return WorkloadName; }
	Future<Void> start(Database const& cx) override { return _start(this, cx); }
	Future<bool> check(Database const& cx) override { return success; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Successes", double(numSuccesses), Averaged::True);
	}

	ACTOR static Future<Void> _start(PrivateEndpoints* self, Database cx) {
		state Reference<AsyncVar<ClientDBInfo>> clientInfo = cx->clientInfo;
		state Future<Void> end;
		TraceEvent("PrivateEndpointTestStartWait").detail("WaitTime", self->startAfter).log();
		wait(delay(self->startAfter));
		TraceEvent("PrivateEndpointTestStart").detail("RunFor", self->runFor).log();
		end = delay(self->runFor);
		try {
			loop {
				auto testFuture = deterministicRandom()->randomChoice(self->testFunctions)(cx->clientInfo);
				choose {
					when(wait(end)) {
						TraceEvent("PrivateEndpointTestDone").log();
						return Void();
					}
					when(wait(testFuture)) {
						++self->numSuccesses;
					}
				}
				wait(delay(0.2));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "PrivateEndpointTestError").errorUnsuppressed(e);
			ASSERT(false);
		}
		UNREACHABLE();
		return Void();
	}
};

} // namespace

WorkloadFactory<PrivateEndpoints> PrivateEndpointsFactory(PrivateEndpoints::WorkloadName, true);
