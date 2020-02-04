/*
 * Cycle.actor.cpp
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

#include "Arena.h"
#include "NativeAPI.actor.h"
#include "PerfMetric.h"
#include "UnitTest.h"
#include "fdbserver/workloads/workloads.actor.h"
#include <utility>
#include "flow.h"
#include "flow/actorcompiler.h" // This must be the last #include

namespace {

// Functor implementation for Future

ACTOR template <class Fun, class A>
Future<decltype(std::declval<Fun>()(std::declval<A>()))> fmap(Future<A> a, Fun fun) {
	A val = wait(a);
	return fun(val);
}

// Monad implementation for Future

ACTOR template <class A, class B, class Fun>
Future<B> monadOperator(Future<A> a, Fun fun) {
	A val = wait(a);
	B res = wait(fun(val));
	return res;
}

template <class A, class Fun>
Future<typename std::remove_const<decltype(std::declval<Fun>()(std::declval<A>()).getValue())>::type> operator>>=(
    Future<A> a, Fun fun) {
	return monadOperator<
	    A, typename std::remove_const<decltype(std::declval<Fun>()(std::declval<A>()).getValue())>::type, Fun>(a, fun);
}

template <class A, class Fun>
Future<typename std::remove_const<decltype(std::declval<Fun>()().getValue())>::type> operator>>(Future<A> a, Fun fun) {
	return a >>= [fun](const A&) { return fun(); };
}

ACTOR Future<int> delayedAdd(int x) {
	wait(delay(0.1));
	return x + 5;
}

TEST_CASE("/flow/flow/Monad") {
	Future<int> f(2);
	int b = wait(fmap(f, [](int x) { return x + 1; }));
	ASSERT(b == 3);
	Future<int> f2(4);
	int c = wait(f2 >>= delayedAdd);
	ASSERT(c == 9);
	return Void();
}

struct ByzentifyWorkload : TestWorkload {
	constexpr static const char* NAME = "Byzentify";
	double testDuration, startDelay;

	explicit ByzentifyWorkload(const WorkloadContext& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, LiteralStringRef("testDuration"), 30.0);
		startDelay = getOption(options, LiteralStringRef("startDelay"), 0.0);
	}
	std::string description() override { return NAME; }
	Future<Void> start(const Database& cx) override {
		auto myDelay = [](double d) { return delay(d); };
		if (clientId == 0) {
			return (delay(startDelay) >>
			            [this]() {
				            byzantineEnabled() = true;
				            return Future<double>(testDuration);
			            } >>= myDelay) >>
			       []() {
				       byzantineEnabled() = false;
				       return Future<Void>();
			       };
		}
		return Void();
	}
	Future<bool> check(const Database& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

} // namespace

WorkloadFactory<ByzentifyWorkload> ByzentifyWorkloadFactory(ByzentifyWorkload::NAME);
