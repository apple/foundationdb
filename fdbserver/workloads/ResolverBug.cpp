/*
 * ResolverBug.cpp
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
#include "flow/ProcessEvents.h"
#include "fdbserver/tester/workloads.actor.h"
#include "fdbserver/resolver/ResolverBug.h"
#include "fdbserver/core/ServerDBInfo.actor.h"

namespace {

struct ResolverBugWorkload : TestWorkload {
	constexpr static auto NAME = "ResolverBug";
	bool disableFailureInjections;
	ResolverBug resolverBug;
	Standalone<VectorRef<KeyValueRef>> cycleOptions;
	KeyRef controlKey = "workload_control"_sr;
	Promise<Void> bugFound;

	ResolverBugWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		disableFailureInjections = getOption(options, "disableFailureInjections"_sr, true);
		resolverBug.ignoreTooOldProbability = getOption(options, "ignoreTooOldProbability"_sr, 0.0);
		resolverBug.ignoreWriteSetProbability = getOption(options, "ignoreWriteSetProbability"_sr, 0.0);
		resolverBug.ignoreReadSetProbability = getOption(options, "ignoreReadSetProbability"_sr, 0.0);

		for (auto& o : options) {
			if (o.key.startsWith("cycle_"_sr)) {
				KeyValueRef option;
				option.key = o.key.removePrefix("cycle_"_sr);
				option.value = o.value;
				cycleOptions.push_back_deep(cycleOptions.arena(), option);
				o.value = ""_sr;
			}
		}

		if (clientId == 0) {
			SimBugInjector().enable();
			auto bug = SimBugInjector().enable<ResolverBug>(ResolverBugID());
			*bug = resolverBug;
			bug->cycleState.resize(clientCount, 0);
			SimBugInjector().disable();
		}
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		if (disableFailureInjections) {
			out.insert("all");
		}
	}

	Reference<TestWorkload> createCycle() {
		WorkloadContext wcx;
		wcx.clientId = clientId;
		wcx.clientCount = clientCount;
		wcx.ccr = ccr;
		wcx.dbInfo = dbInfo;
		wcx.options = cycleOptions;
		wcx.sharedRandomNumber = sharedRandomNumber;
		return IWorkloadFactory::create("Cycle", wcx);
	}

	static Future<Void> waitForPhase(std::shared_ptr<ResolverBug> bug, int phase) {
		while (bug->currentPhase != phase) {
			co_await delay(0.5);
		}
	}

	static Future<Void> waitForPhaseDone(std::shared_ptr<ResolverBug> bug, int phase, int clientCount) {
		while (std::count(bug->cycleState.begin(), bug->cycleState.end(), phase) != clientCount) {
			co_await delay(0.5);
		}
	}

	struct ReportTraces {
		ReportTraces() { g_traceProcessEvents = true; }
		~ReportTraces() { g_traceProcessEvents = false; }
	};

	struct OnTestFailure {
		std::shared_ptr<ResolverBug> bug;
		OnTestFailure(std::shared_ptr<ResolverBug> bug) : bug(bug) {}

		void operator()(StringRef, auto const& data, Error const&) {
			auto* trace = std::any_cast<BaseTraceEvent*>(data);
			if (trace->getSeverity() == SevError) {
				bug->bugFound = true;
			}
		}
	};

	static Future<Void> driveWorkload(std::shared_ptr<ResolverBug> bug, int clientCount) {
		ReportTraces _;
		OnTestFailure onTestFailure(bug);
		ProcessEvents::Event ev("TraceEvent::TestFailure"_sr, onTestFailure);
		while (true) {
			bug->currentPhase = 1;
			co_await waitForPhaseDone(bug, 1, clientCount);
			SimBugInjector().enable();
			bug->currentPhase = 2;
			co_await waitForPhaseDone(bug, 2, clientCount);
			SimBugInjector().disable();
			bug->currentPhase = 3;
			co_await waitForPhaseDone(bug, 3, clientCount);
		}
	}

	Future<Void> _start(Database cx) {
		Reference<TestWorkload> cycle;
		std::shared_ptr<ResolverBug> bug = SimBugInjector().get<ResolverBug>(ResolverBugID(), true);
		while (true) {
			co_await waitForPhase(bug, 1);
			cycle = createCycle();
			co_await cycle->setup(cx);
			bug->cycleState[clientId] = 1;
			co_await waitForPhase(bug, 2);
			co_await cycle->start(cx);
			bug->cycleState[clientId] = 2;
			co_await waitForPhase(bug, 3);
			co_await cycle->check(cx);
			bug->cycleState[clientId] = 3;
		}
	}

	static Future<Void> onBug(std::shared_ptr<ResolverBug> bug) {
		while (true) {
			if (bug->bugFound) {
				TraceEvent("NegativeTestSuccess").log();
				co_return;
			}
			co_await delay(0.5);
		}
	}

	Future<Void> start(const Database& cx) override {
		std::vector<Future<Void>> futures;
		auto bug = SimBugInjector().get<ResolverBug>(ResolverBugID(), true);
		if (clientId == 0) {
			futures.push_back(driveWorkload(bug, clientCount));
		}
		futures.push_back(_start(cx->clone()));
		return onBug(bug) || waitForAll(futures);
	}
	Future<bool> check(Database const& cx) override { return true; };

private:
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ResolverBugWorkload> workloadFactory;

} // namespace
