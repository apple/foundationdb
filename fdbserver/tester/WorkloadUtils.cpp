/*
 * WorkloadUtils.cpp
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

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <limits>
#include <memory>

#include <fmt/ranges.h>

#include "flow/CoroUtils.h"
#include "flow/DeterministicRandom.h"
#include "flow/ParseNumber.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/genericactors.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"

namespace {

template <class T>
Optional<T> parseNumericOption(StringRef value) {
	// Legacy options accept numeric prefixes, including "100000.0" for integers.
	return parseNumberPrefix<T>(value);
}

constexpr char HEX_CHAR_LOOKUP[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

Future<std::vector<PerfMetric>> getMetricsCompoundWorkload(CompoundWorkload* self) {
	std::vector<Future<std::vector<PerfMetric>>> results;
	for (int w = 0; w < self->workloads.size(); w++) {
		std::vector<PerfMetric> p;
		results.push_back(self->workloads[w]->getMetrics());
	}
	co_await waitForAll(results);
	std::vector<PerfMetric> res;
	for (int i = 0; i < results.size(); ++i) {
		auto const& p = results[i].get();
		for (auto const& m : p) {
			res.push_back(m.withPrefix(self->workloads[i]->description() + "."));
		}
	}
	co_return res;
}

} // namespace

WorkloadContext::WorkloadContext() = default;

WorkloadContext::WorkloadContext(const WorkloadContext& r) = default;

WorkloadContext::~WorkloadContext() = default;

void emplaceIndex(uint8_t* data, int offset, int64_t index) {
	for (int i = 0; i < 16; i++) {
		data[(15 - i) + offset] = HEX_CHAR_LOOKUP[index & 0xf];
		index = index >> 4;
	}
}

Key KVWorkload::getRandomKey() const {
	return getRandomKey(absentFrac);
}

Key KVWorkload::getRandomKey(double absentFrac) const {
	if (absentFrac > 0.0000001) {
		return getRandomKey(deterministicRandom()->random01() < absentFrac);
	} else {
		return getRandomKey(false);
	}
}

Key KVWorkload::getRandomKey(bool absent) const {
	return keyForIndex(deterministicRandom()->randomInt(0, nodeCount), absent);
}

Key KVWorkload::keyForIndex(uint64_t index) const {
	if (absentFrac > 0.0000001) {
		return keyForIndex(index, deterministicRandom()->random01() < absentFrac);
	} else {
		return keyForIndex(index, false);
	}
}

int64_t KVWorkload::indexForKey(const KeyRef& key, bool absent) const {
	int idx = 0;
	if (nodePrefix > 0) {
		ASSERT(keyBytes >= 32);
		idx += 16;
	}
	ASSERT(keyBytes >= 16);
	auto end = key.size() - idx - (absent ? 1 : 0);
	std::string str((char*)key.begin() + idx, end);
	int64_t res = std::stoll(str, nullptr, 16);
	return res;
}

Key KVWorkload::keyForIndex(uint64_t index, bool absent) const {
	int adjustedKeyBytes = absent ? (keyBytes + 1) : keyBytes;
	Key result = makeString(adjustedKeyBytes);
	uint8_t* data = mutateString(result);
	memset(data, '.', adjustedKeyBytes);

	int idx = 0;
	if (nodePrefix > 0) {
		ASSERT(keyBytes >= 32);
		emplaceIndex(data, 0, nodePrefix);
		idx += 16;
	}
	ASSERT(keyBytes >= 16);
	emplaceIndex(data, idx, (int64_t)index);

	return result;
}

Future<Void> poisson(double* last, double meanInterval) {
	*last += meanInterval * -log(deterministicRandom()->random01());
	co_await delayUntil(*last);
	co_return;
}

Future<Void> uniform(double* last, double meanInterval) {
	*last += meanInterval;
	co_await delayUntil(*last);
	co_return;
}

Value getOption(VectorRef<KeyValueRef> options, Key key, Value defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			Value value = options[i].value;
			options[i].value = ""_sr;
			return value;
		}
	}

	return defaultValue;
}

int getOption(VectorRef<KeyValueRef> options, Key key, int defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			auto r = parseNumericOption<int>(options[i].value);
			if (r.present()) {
				options[i].value = ""_sr;
				return r.get();
			} else {
				TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
				throw test_specification_invalid();
			}
		}
	}

	return defaultValue;
}

uint64_t getOption(VectorRef<KeyValueRef> options, Key key, uint64_t defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			auto r = parseNumericOption<uint64_t>(options[i].value);
			if (r.present()) {
				options[i].value = ""_sr;
				return r.get();
			} else {
				TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
				throw test_specification_invalid();
			}
		}
	}

	return defaultValue;
}

int64_t getOption(VectorRef<KeyValueRef> options, Key key, int64_t defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			auto r = parseNumericOption<int64_t>(options[i].value);
			if (r.present()) {
				options[i].value = ""_sr;
				return r.get();
			} else {
				TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
				throw test_specification_invalid();
			}
		}
	}

	return defaultValue;
}

double getOption(VectorRef<KeyValueRef> options, Key key, double defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			// Preserve the float rounding used by existing simulation configurations.
			auto r = parseNumericOption<float>(options[i].value);
			if (r.present()) {
				options[i].value = ""_sr;
				return r.get();
			}
		}
	}

	return defaultValue;
}

bool getOption(VectorRef<KeyValueRef> options, Key key, bool defaultValue) {
	Value p = getOption(options, key, defaultValue ? "true"_sr : "false"_sr);
	if (p == "true"_sr)
		return true;
	if (p == "false"_sr)
		return false;
	ASSERT(false);
	return false;
}

std::vector<std::string> getOption(VectorRef<KeyValueRef> options, Key key, std::vector<std::string> defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			std::vector<std::string> v;
			int begin = 0;
			for (int c = 0; c < options[i].value.size(); c++) {
				if (options[i].value[c] == ',') {
					v.push_back(options[i].value.substr(begin, c - begin).toString());
					begin = c + 1;
				}
			}
			v.push_back(options[i].value.substr(begin).toString());
			options[i].value = ""_sr;
			return v;
		}
	}
	return defaultValue;
}

std::vector<int> getOption(VectorRef<KeyValueRef> options, Key key, std::vector<int> defaultValue) {
	for (int i = 0; i < options.size(); i++) {
		if (options[i].key == key) {
			std::vector<int> v;
			auto appendValue = [&](StringRef value) {
				auto parsed = parseNumericOption<int>(value);
				if (!parsed.present()) {
					TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
					throw test_specification_invalid();
				}
				v.push_back(parsed.get());
			};
			int begin = 0;
			for (int c = 0; c < options[i].value.size(); c++) {
				if (options[i].value[c] == ',') {
					appendValue(options[i].value.substr(begin, c - begin));
					begin = c + 1;
				}
			}
			appendValue(options[i].value.substr(begin));
			options[i].value = ""_sr;
			return v;
		}
	}
	return defaultValue;
}

TEST_CASE("/fdbserver/WorkloadUtils/numericOptions") {
	ASSERT_EQ(parseNumericOption<int>(" \t+0012 \n"_sr).get(), 12);
	ASSERT_EQ(parseNumericOption<int>("100000.0"_sr).get(), 100000);
	ASSERT_EQ(parseNumericOption<int>("12suffix"_sr).get(), 12);
	ASSERT_EQ(parseNumericOption<int>("12\0suffix"_sr).get(), 12);
	ASSERT_EQ(parseNumericOption<int64_t>("-9223372036854775808"_sr).get(), std::numeric_limits<int64_t>::min());
	ASSERT_EQ(parseNumericOption<int64_t>("9223372036854775807"_sr).get(), std::numeric_limits<int64_t>::max());
	ASSERT_EQ(parseNumericOption<uint64_t>("18446744073709551615"_sr).get(), std::numeric_limits<uint64_t>::max());
	ASSERT_EQ(parseNumericOption<uint64_t>("-1"_sr).get(), std::numeric_limits<uint64_t>::max());
	ASSERT_EQ(parseNumericOption<float>(" \t+1.25e2 \n"_sr).get(), 125.0f);
	ASSERT_EQ(parseNumericOption<float>("1.25suffix"_sr).get(), 1.25f);
	ASSERT(!parseNumericOption<int64_t>("9223372036854775808"_sr).present());
	ASSERT(!parseNumericOption<int64_t>("-9223372036854775809"_sr).present());
	ASSERT(!parseNumericOption<uint64_t>("18446744073709551616"_sr).present());
	ASSERT(
	    !parseNumericOption<int>(std::to_string(static_cast<int64_t>(std::numeric_limits<int>::max()) + 1)).present());
	ASSERT(!parseNumericOption<float>("1e9999"_sr).present());
	ASSERT(!parseNumericOption<float>("1e-9999"_sr).present());
	for (StringRef text : { ""_sr, " \t"_sr, "+"_sr, "suffix12"_sr }) {
		ASSERT(!parseNumericOption<int>(text).present());
		ASSERT(!parseNumericOption<float>(text).present());
	}

	const uint8_t backing[] = { '1', ',', '2', '3', 0 };
	Standalone<VectorRef<KeyValueRef>> options;
	options.push_back(options.arena(), KeyValueRef("integers"_sr, StringRef(backing, 3)));
	const std::vector<int> parsed = getOption(options, "integers"_sr, std::vector<int>{});
	ASSERT(parsed == std::vector<int>({ 1, 2 }));
	ASSERT(options[0].value.empty());

	options.push_back(options.arena(), KeyValueRef("double"_sr, "0.1"_sr));
	ASSERT_EQ(getOption(options, "double"_sr, 0.0), static_cast<double>(0.1f));
	options.push_back(options.arena(), KeyValueRef("invalidDouble"_sr, "invalid"_sr));
	ASSERT_EQ(getOption(options, "invalidDouble"_sr, 3.5), 3.5);
	ASSERT(options[2].value == "invalid"_sr);

	return Void();
}

bool hasOption(VectorRef<KeyValueRef> options, Key key) {
	for (const auto& option : options) {
		if (option.key == key) {
			return true;
		}
	}
	return false;
}

CompoundWorkload::CompoundWorkload(WorkloadContext& wcx) : TestWorkload(wcx) {}

CompoundWorkload* CompoundWorkload::add(Reference<TestWorkload>&& w) {
	workloads.push_back(std::move(w));
	return this;
}

std::string CompoundWorkload::description() const {
	std::vector<std::string> names;
	names.reserve(workloads.size());
	for (auto const& w : workloads) {
		names.push_back(w->description());
	}
	return fmt::format("{}", fmt::join(std::move(names), ";"));
}

Future<Void> CompoundWorkload::setup(Database const& cx) {
	std::vector<Future<Void>> all;
	all.reserve(workloads.size());
	for (int w = 0; w < workloads.size(); w++)
		all.push_back(workloads[w]->setup(cx));
	auto done = waitForAll(all);
	if (failureInjection.empty()) {
		return done;
	}
	std::vector<Future<Void>> res;
	res.reserve(failureInjection.size());
	for (auto& f : failureInjection) {
		res.push_back(f->setupInjectionWorkload(cx, done));
	}
	return waitForAll(res);
}

Future<Void> CompoundWorkload::start(Database const& cx) {
	std::vector<Future<Void>> all;
	all.reserve(workloads.size() + failureInjection.size());
	auto wCount = std::make_shared<unsigned>(0);
	auto startWorkload = [&](TestWorkload& workload) -> Future<Void> {
		auto workloadName = workload.description();
		++(*wCount);
		TraceEvent("WorkloadRunStatus").detail("Name", workloadName).detail("Count", *wCount).detail("Phase", "Start");
		return fmap(
		    [workloadName, wCount](Void value) {
			    --(*wCount);
			    TraceEvent("WorkloadRunStatus")
			        .detail("Name", workloadName)
			        .detail("Remaining", *wCount)
			        .detail("Phase", "End");
			    return Void();
		    },
		    workload.start(cx));
	};
	for (auto& workload : workloads) {
		all.push_back(startWorkload(*workload));
	}
	for (auto& workload : failureInjection) {
		all.push_back(startWorkload(*workload));
	}
	return waitForAll(all);
}

Future<bool> CompoundWorkload::check(Database const& cx) {
	std::vector<Future<bool>> all;
	all.reserve(workloads.size() + failureInjection.size());
	auto wCount = std::make_shared<unsigned>(0);
	auto starter = [&](TestWorkload& workload) -> Future<bool> {
		++(*wCount);
		std::string workloadName = workload.description();
		TraceEvent("WorkloadCheckStatus")
		    .detail("Name", workloadName)
		    .detail("Count", *wCount)
		    .detail("Phase", "Start");
		return fmap(
		    [workloadName, wCount](bool ret) {
			    --(*wCount);
			    TraceEvent("WorkloadCheckStatus")
			        .detail("Name", workloadName)
			        .detail("Remaining", *wCount)
			        .detail("Phase", "End");
			    return ret;
		    },
		    workload.check(cx));
	};
	for (auto& workload : workloads) {
		all.push_back(starter(*workload));
	}
	for (auto& workload : failureInjection) {
		all.push_back(starter(*workload));
	}
	return allTrue(all);
}

void CompoundWorkload::addFailureInjection(WorkloadRequest& work) {
	if (!work.runFailureWorkloads) {
		return;
	}
	std::set<std::string> disabledWorkloads;
	for (auto const& w : workloads) {
		w->disableFailureInjectionWorkloads(disabledWorkloads);
	}
	if (disabledWorkloads.contains("all")) {
		return;
	}
	auto& factories = IFailureInjectorFactory::factories();
	DeterministicRandom random(sharedRandomNumber);
	for (auto& factory : factories) {
		auto workload = factory->create(*this);
		if (disabledWorkloads.contains(workload->description())) {
			continue;
		}
		if (std::find(work.disabledFailureInjectionWorkloads.begin(),
		              work.disabledFailureInjectionWorkloads.end(),
		              workload->description()) != work.disabledFailureInjectionWorkloads.end()) {
			continue;
		}
		while (shouldInjectFailure(random, work, workload)) {
			workload->initFailureInjectionMode(random);
			TraceEvent("AddFailureInjectionWorkload")
			    .detail("Name", workload->description())
			    .detail("ClientID", work.clientId)
			    .detail("ClientCount", clientCount)
			    .detail("Title", work.title);
			failureInjection.push_back(workload);
			workload = factory->create(*this);
		}
	}
}

bool CompoundWorkload::shouldInjectFailure(DeterministicRandom& random,
                                           const WorkloadRequest& work,
                                           Reference<FailureInjectionWorkload> failure) const {
	auto desc = failure->description();
	unsigned alreadyAdded =
	    std::count_if(workloads.begin(), workloads.end(), [&desc](auto const& w) { return w->description() == desc; });
	alreadyAdded += std::count_if(
	    failureInjection.begin(), failureInjection.end(), [&desc](auto const& w) { return w->description() == desc; });
	return failure->shouldInject(random, work, alreadyAdded);
}

Future<std::vector<PerfMetric>> CompoundWorkload::getMetrics() {
	return getMetricsCompoundWorkload(this);
}

double CompoundWorkload::getCheckTimeout() const {
	double m = 0;
	for (int w = 0; w < workloads.size(); w++)
		m = std::max(workloads[w]->getCheckTimeout(), m);
	return m;
}

void CompoundWorkload::getMetrics(std::vector<PerfMetric>&) {
	ASSERT(false);
}

void TestWorkload::disableFailureInjectionWorkloads(std::set<std::string>& out) const {}

FailureInjectionWorkload::FailureInjectionWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

void FailureInjectionWorkload::initFailureInjectionMode(DeterministicRandom& random) {}

bool FailureInjectionWorkload::shouldInject(DeterministicRandom& random,
                                            const WorkloadRequest& work,
                                            const unsigned alreadyAdded) const {
	return alreadyAdded < 3 && work.useDatabase && 0.1 / (1 + alreadyAdded) > random.random01();
}

Future<Void> FailureInjectionWorkload::setupInjectionWorkload(const Database& cx, Future<Void> done) {
	return holdWhile(this->setup(cx), done);
}

Future<Void> FailureInjectionWorkload::startInjectionWorkload(const Database& cx, Future<Void> done) {
	return holdWhile(this->start(cx), done);
}

Future<bool> FailureInjectionWorkload::checkInjectionWorkload(const Database& cx, Future<bool> done) {
	return holdWhile(this->check(cx), done);
}
