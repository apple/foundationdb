/*
 * PrivateEndpoints.cpp
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

#include "fdbserver/tester/workloads.actor.h"

#include "flow/CoroUtils.h"

namespace {

struct PrivateEndpoints : TestWorkload {
	static constexpr auto NAME = "PrivateEndpoints";

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

	template <class T>
	static Future<Void> assumeFailure(Future<T> f) {
		try {
			if constexpr (std::is_same_v<T, Void>) {
				co_await f;
			} else {
				T t = co_await f;
				(void)t;
			}
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
	}

	template <class I, class RT>
	void addTestFor(RequestStream<RT, false> I::* channel) {
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
	Future<Void> start(Database const& cx) override { return _start(cx); }
	Future<bool> check(Database const& cx) override { return success; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("Successes", double(numSuccesses), Averaged::True);
	}

	Future<Void> _start(Database cx) {
		Reference<AsyncVar<ClientDBInfo>> clientInfo = cx->clientInfo;
		TraceEvent("PrivateEndpointTestStartWait").detail("WaitTime", startAfter).log();
		co_await delay(startAfter);
		TraceEvent("PrivateEndpointTestStart").detail("RunFor", runFor).log();
		Future<Void> end = delay(runFor);
		try {
			while (true) {
				auto testFuture = deterministicRandom()->randomChoice(testFunctions)(clientInfo);
				{
					auto choice = co_await race(end, testFuture);
					if (choice.index() == 0) {
						TraceEvent("PrivateEndpointTestDone").log();
						co_return;
					} else if (choice.index() == 1) {
						++numSuccesses;
					} else {
						UNREACHABLE();
					}
				}
				co_await delay(0.2);
			}
		} catch (Error& e) {
			TraceEvent(SevError, "PrivateEndpointTestError").errorUnsuppressed(e);
			ASSERT(false);
		}
		UNREACHABLE();
	}
};

} // namespace

WorkloadFactory<PrivateEndpoints> PrivateEndpointsFactory(UntrustedMode::True);
