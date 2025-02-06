/*
 * DisableFailureInjection.actor.cpp
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

#include <fdbserver/workloads/workloads.actor.h>

#include <string_view>

namespace {

using namespace std::string_view_literals;

struct DisableFailureInjectionWorkload : TestWorkload {
	constexpr static const char* NAME = "DisableFailureInjection";
	std::set<std::string> disabledWorkloads;

	DisableFailureInjectionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		auto disabled = getOption(options, "disableWorkloads"_sr, std::vector<std::string>());
		for (auto const& w : disabled) {
			TraceEvent("DisableInjectedFailureWorkload").detail("Workload", w);
			disabledWorkloads.insert(w);
		}
	}

	std::string description() const override { return { NAME }; }
	Future<Void> start(const Database& cx) override { return Void(); }
	Future<bool> check(const Database& cx) override { return true; }
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override {
		for (const auto& workload : disabledWorkloads) {
			out.insert(workload);
		}
	}

private:
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DisableFailureInjectionWorkload> factory(DisableFailureInjectionWorkload::NAME);

} // namespace
