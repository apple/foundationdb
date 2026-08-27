/*
 * LoadBalance.cpp
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

#include "fdbrpc/LoadBalance.actor.h"
#include "flow/CoroUtils.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"

// Throwing all_alternatives_failed will cause the client to issue a GetKeyLocationRequest to the proxy, so this actor
// attempts to limit the number of these errors thrown by a single client to prevent it from saturating the proxies with
// these requests
Future<Void> allAlternativesFailedDelay(Future<Void> okFuture) {
	if (now() - g_network->networkInfo.newestAlternativesFailure > FLOW_KNOBS->ALTERNATIVES_FAILURE_RESET_TIME) {
		g_network->networkInfo.oldestAlternativesFailure = now();
	}

	double delay = FLOW_KNOBS->ALTERNATIVES_FAILURE_MIN_DELAY;
	if (now() - g_network->networkInfo.lastAlternativesFailureSkipDelay > FLOW_KNOBS->ALTERNATIVES_FAILURE_SKIP_DELAY) {
		g_network->networkInfo.lastAlternativesFailureSkipDelay = now();
	} else {
		double elapsed = now() - g_network->networkInfo.oldestAlternativesFailure;
		delay = std::max(delay,
		                 std::min(elapsed * FLOW_KNOBS->ALTERNATIVES_FAILURE_DELAY_RATIO,
		                          FLOW_KNOBS->ALTERNATIVES_FAILURE_MAX_DELAY));
		delay = std::max(delay,
		                 std::min(elapsed * FLOW_KNOBS->ALTERNATIVES_FAILURE_SLOW_DELAY_RATIO,
		                          FLOW_KNOBS->ALTERNATIVES_FAILURE_SLOW_MAX_DELAY));
	}

	g_network->networkInfo.newestAlternativesFailure = now();

	auto res = co_await race(okFuture, ::delayJittered(delay));
	if (res.index() == 1) {
		throw all_alternatives_failed();
	}
}

namespace {

using LoadBalanceTestRequest = ReplyPromise<UID>;

struct LoadBalanceTestInterface {
	RequestStream<LoadBalanceTestRequest> request;

	UID id() const { return request.getEndpoint().token; }
	std::string toString() const { return id().toString(); }
};

using LoadBalanceTestMulti = ReferencedInterface<LoadBalanceTestInterface>;
using LoadBalanceTestAlternatives = Reference<MultiInterface<LoadBalanceTestMulti>>;
using LoadBalanceTestChannel = RequestStream<LoadBalanceTestRequest> LoadBalanceTestInterface::*;

std::string describe(const std::vector<Reference<LoadBalanceTestMulti>>& alternatives) {
	std::string result;
	for (const auto& alternative : alternatives) {
		if (!result.empty()) {
			result += ", ";
		}
		result += alternative->toString();
	}
	return result;
}

class LoadBalanceTestEndpointHealth : NonCopyable {
public:
	explicit LoadBalanceTestEndpointHealth(const Endpoint& endpoint)
	  : monitor(IFailureMonitor::failureMonitor()), address(endpoint.getPrimaryAddress()),
	    previousStatus(monitor.getState(address)) {
		monitor.setStatus(address, FailureStatus(false));
	}

	~LoadBalanceTestEndpointHealth() { monitor.setStatus(address, previousStatus); }

private:
	IFailureMonitor& monitor;
	NetworkAddress address;
	FailureStatus previousStatus;
};

LoadBalanceTestAlternatives makeLoadBalanceTestAlternatives(const LoadBalanceTestInterface& interf) {
	ASSERT(!IFailureMonitor::failureMonitor().getState(interf.request.getEndpoint()).failed);
	return makeReference<MultiInterface<LoadBalanceTestMulti>>(
	    std::vector<Reference<LoadBalanceTestMulti>>{ makeReference<LoadBalanceTestMulti>(interf) });
}

class LoadBalanceTestModel final : public QueueModel, NonCopyable {
public:
	void recordDuplicate() { ++duplicateRequests; }
	int numDuplicateRequests() const { return duplicateRequests; }
	int numComparisons() const { return comparisons; }
	Future<Void> onComparison() const { return comparisonStarted.getFuture(); }

	Future<Void> startComparison() {
		ASSERT_EQ(comparisons, 0);
		++comparisons;
		Future<Void> result = comparisonResult.getFuture();
		comparisonStarted.send(Void());
		return result;
	}

	void failComparison(Error error) {
		ASSERT_EQ(comparisons, 1);
		comparisonResult.sendError(error);
	}

private:
	int duplicateRequests = 0;
	int comparisons = 0;
	Promise<Void> comparisonStarted;
	Promise<Void> comparisonResult;
};

} // namespace

template <>
struct LoadBalanceHooksRequired<LoadBalanceTestModel> : std::true_type {};

template <>
struct LoadBalanceRequestHooks<LoadBalanceTestRequest,
                               LoadBalanceTestInterface,
                               LoadBalanceTestMulti,
                               LoadBalanceTestModel,
                               false> {
	static void maybeDuplicate(RequestStream<LoadBalanceTestRequest> const*,
	                           LoadBalanceTestRequest&,
	                           LoadBalanceTestModel* model,
	                           Future<ErrorOr<UID>>,
	                           LoadBalanceTestAlternatives,
	                           LoadBalanceTestChannel) {
		ASSERT(model != nullptr);
		model->recordDuplicate();
	}

	static Future<Void> maybeCompare(LoadBalanceTestRequest&,
	                                 LoadBalanceTestModel* model,
	                                 RequestStream<LoadBalanceTestRequest> const*,
	                                 Future<ErrorOr<UID>> response,
	                                 LoadBalanceTestAlternatives,
	                                 LoadBalanceTestChannel,
	                                 bool compareReplicas,
	                                 int requiredReplicas) {
		ASSERT(model != nullptr);
		ASSERT(response.isReady() && response.get().present());
		ASSERT(compareReplicas);
		ASSERT_EQ(requiredReplicas, 2);
		return model->startComparison();
	}
};

TEST_CASE("/fdbrpc/loadBalance/hooks/requiredModel") {
	LoadBalanceTestInterface interf;
	LoadBalanceTestEndpointHealth endpointHealth(interf.request.getEndpoint());
	LoadBalanceTestAlternatives alternatives = makeLoadBalanceTestAlternatives(interf);
	FutureStream<LoadBalanceTestRequest> requests = interf.request.getFuture();
	LoadBalanceTestModel model;
	Future<UID> result = loadBalance(alternatives,
	                                 &LoadBalanceTestInterface::request,
	                                 LoadBalanceTestRequest(),
	                                 TaskPriority::DefaultPromiseEndpoint,
	                                 AtMostOnce::False,
	                                 &model,
	                                 true,
	                                 2);
	LoadBalanceTestRequest reply = co_await requests;
	ASSERT_EQ(model.numDuplicateRequests(), 1);
	ASSERT_EQ(model.numComparisons(), 0);
	ASSERT(!result.isReady());
	reply.send(UID(42, 0));

	// A missing comparison must fail the test instead of leaving it waiting on a hook that never ran.
	auto completed = co_await race(model.onComparison(), result);
	ASSERT_EQ(completed.index(), 0);
	ASSERT_EQ(model.numComparisons(), 1);
	ASSERT(!result.isReady());

	model.failComparison(operation_failed());
	ErrorOr<UID> outcome = co_await coro::errorOr(result);
	ASSERT(outcome.isError());
	ASSERT_EQ(outcome.getError().code(), error_code_operation_failed);
}

TEST_CASE("/fdbrpc/loadBalance/hooks/genericModel") {
	LoadBalanceTestInterface interf;
	LoadBalanceTestEndpointHealth endpointHealth(interf.request.getEndpoint());
	LoadBalanceTestAlternatives alternatives = makeLoadBalanceTestAlternatives(interf);
	FutureStream<LoadBalanceTestRequest> requests = interf.request.getFuture();
	Future<UID> withoutModel = loadBalance(alternatives, &LoadBalanceTestInterface::request);
	LoadBalanceTestRequest firstReply = co_await requests;
	firstReply.send(UID(7, 0));
	UID value = co_await withoutModel;
	ASSERT_EQ(value, UID(7, 0));

	QueueModel model;
	Future<UID> withModel = loadBalance(alternatives,
	                                    &LoadBalanceTestInterface::request,
	                                    LoadBalanceTestRequest(),
	                                    TaskPriority::DefaultPromiseEndpoint,
	                                    AtMostOnce::False,
	                                    &model);
	LoadBalanceTestRequest secondReply = co_await requests;
	secondReply.send(UID(11, 0));
	value = co_await withModel;
	ASSERT_EQ(value, UID(11, 0));
}
