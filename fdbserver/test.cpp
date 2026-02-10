/*
 * tester.actor.cpp
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

#include <cinttypes>
#include <filesystem>
#include <fstream>
#include <functional>
#include <istream>
#include <iterator>
#include <map>
#include <streambuf>
#include <numeric>

#include <fmt/ranges.h>
#include <toml.hpp>

#include "flow/ActorCollection.h"
#include "flow/DeterministicRandom.h"
#include "flow/Histogram.h"
#include "flow/ProcessEvents.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.actor.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/DataDistributionConfig.actor.h"
#include "fdbserver/KnobProtectiveGroups.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/MoveKeys.actor.h"
#include "flow/Platform.h"

#include "flow/Trace.h"
#include "flow/CoroUtils.h"

WorkloadContext::WorkloadContext() {}

WorkloadContext::WorkloadContext(const WorkloadContext& r)
  : options(r.options), clientId(r.clientId), clientCount(r.clientCount), sharedRandomNumber(r.sharedRandomNumber),
    dbInfo(r.dbInfo), ccr(r.ccr), rangesToCheck(r.rangesToCheck) {}

WorkloadContext::~WorkloadContext() {}

const char HEX_CHAR_LOOKUP[16] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

void emplaceIndex(uint8_t* data, int offset, int64_t index) {
	for (int i = 0; i < 16; i++) {
		data[(15 - i) + offset] = HEX_CHAR_LOOKUP[index & 0xf];
		index = index >> 4;
	}
}

Key doubleToTestKey(double p) {
	return StringRef(format("%016llx", *(uint64_t*)&p));
}

double testKeyToDouble(const KeyRef& p) {
	uint64_t x = 0;
	sscanf(p.toString().c_str(), "%" SCNx64, &x);
	return *(double*)&x;
}

Key doubleToTestKey(double p, const KeyRef& prefix) {
	return doubleToTestKey(p).withPrefix(prefix);
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
	// extract int64_t index, the reverse process of emplaceIndex()
	auto end = key.size() - idx - (absent ? 1 : 0);
	std::string str((char*)key.begin() + idx, end);
	int64_t res = std::stoll(str, nullptr, 16);
	return res;
}

Key KVWorkload::keyForIndex(uint64_t index, bool absent) const {
	int adjustedKeyBytes = (absent) ? (keyBytes + 1) : keyBytes;
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
	// ASSERT(indexForKey(result) == (int64_t)index); // debug assert

	return result;
}

double testKeyToDouble(const KeyRef& p, const KeyRef& prefix) {
	return testKeyToDouble(p.removePrefix(prefix));
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
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			Value value = options[i].value;
			options[i].value = ""_sr;
			return value;
		}

	return defaultValue;
}

int getOption(VectorRef<KeyValueRef> options, Key key, int defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			int r;
			if (sscanf(options[i].value.toString().c_str(), "%d", &r)) {
				options[i].value = ""_sr;
				return r;
			} else {
				TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
				throw test_specification_invalid();
			}
		}

	return defaultValue;
}

uint64_t getOption(VectorRef<KeyValueRef> options, Key key, uint64_t defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			uint64_t r;
			if (sscanf(options[i].value.toString().c_str(), "%" SCNd64, &r)) {
				options[i].value = ""_sr;
				return r;
			} else {
				TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
				throw test_specification_invalid();
			}
		}

	return defaultValue;
}

int64_t getOption(VectorRef<KeyValueRef> options, Key key, int64_t defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			int64_t r;
			if (sscanf(options[i].value.toString().c_str(), "%" SCNd64, &r)) {
				options[i].value = ""_sr;
				return r;
			} else {
				TraceEvent(SevError, "InvalidTestOption").detail("OptionName", key);
				throw test_specification_invalid();
			}
		}

	return defaultValue;
}

double getOption(VectorRef<KeyValueRef> options, Key key, double defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			float r;
			if (sscanf(options[i].value.toString().c_str(), "%f", &r)) {
				options[i].value = ""_sr;
				return r;
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
	return false; // Assure that compiler is fine with the function
}

std::vector<std::string> getOption(VectorRef<KeyValueRef> options, Key key, std::vector<std::string> defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			std::vector<std::string> v;
			int begin = 0;
			for (int c = 0; c < options[i].value.size(); c++)
				if (options[i].value[c] == ',') {
					v.push_back(options[i].value.substr(begin, c - begin).toString());
					begin = c + 1;
				}
			v.push_back(options[i].value.substr(begin).toString());
			options[i].value = ""_sr;
			return v;
		}
	return defaultValue;
}

std::vector<int> getOption(VectorRef<KeyValueRef> options, Key key, std::vector<int> defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			std::vector<int> v;
			int begin = 0;
			for (int c = 0; c < options[i].value.size(); c++)
				if (options[i].value[c] == ',') {
					v.push_back(atoi((char*)options[i].value.begin() + begin));
					begin = c + 1;
				}
			v.push_back(atoi((char*)options[i].value.begin() + begin));
			options[i].value = ""_sr;
			return v;
		}
	return defaultValue;
}

bool hasOption(VectorRef<KeyValueRef> options, Key key) {
	for (const auto& option : options) {
		if (option.key == key) {
			return true;
		}
	}
	return false;
}

// returns unconsumed options
Standalone<VectorRef<KeyValueRef>> checkAllOptionsConsumed(VectorRef<KeyValueRef> options) {
	static StringRef nothing = ""_sr;
	Standalone<VectorRef<KeyValueRef>> unconsumed;
	for (int i = 0; i < options.size(); i++)
		if (!(options[i].value == nothing)) {
			TraceEvent(SevError, "OptionNotConsumed")
			    .detail("Key", options[i].key.toString().c_str())
			    .detail("Value", options[i].value.toString().c_str());
			unconsumed.push_back_deep(unconsumed.arena(), options[i]);
		}
	return unconsumed;
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

void CompoundWorkload::addFailureInjection(WorkloadRequest& work) {
	if (!work.runFailureWorkloads) {
		return;
	}
	// Some workloads won't work with some failure injection workloads
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

Future<Reference<TestWorkload>> getWorkloadIface(WorkloadRequest work,
                                                 Reference<IClusterConnectionRecord> ccr,
                                                 VectorRef<KeyValueRef> options,
                                                 Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	Reference<TestWorkload> workload;
	Value testName = getOption(options, "testName"_sr, "no-test-specified"_sr);
	WorkloadContext wcx;
	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.ccr = ccr;
	wcx.dbInfo = dbInfo;
	wcx.options = options;
	wcx.sharedRandomNumber = work.sharedRandomNumber;
	wcx.rangesToCheck = work.rangesToCheck;

	workload = IWorkloadFactory::create(testName.toString(), wcx);
	if (workload) {
		co_await workload->initialized();
	}

	auto unconsumedOptions = checkAllOptionsConsumed(workload ? workload->options : VectorRef<KeyValueRef>());
	if (!workload || unconsumedOptions.size()) {
		TraceEvent evt(SevError, "TestCreationError");
		evt.detail("TestName", testName);
		if (!workload) {
			evt.detail("Reason", "Null workload");
			fprintf(stderr,
			        "ERROR: Workload could not be created, perhaps testName (%s) is not a valid workload\n",
			        printable(testName).c_str());
		} else {
			evt.detail("Reason", "Not all options consumed");
			fprintf(stderr, "ERROR: Workload had invalid options. The following were unrecognized:\n");
			for (int i = 0; i < unconsumedOptions.size(); i++)
				fprintf(stderr,
				        " '%s' = '%s'\n",
				        unconsumedOptions[i].key.toString().c_str(),
				        unconsumedOptions[i].value.toString().c_str());
		}
		throw test_specification_invalid();
	}
	co_return workload;
}

Future<Reference<TestWorkload>> getWorkloadIface(WorkloadRequest work,
                                                 Reference<IClusterConnectionRecord> ccr,
                                                 Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	WorkloadContext wcx;
	std::vector<Future<Reference<TestWorkload>>> ifaces;
	if (work.options.size() < 1) {
		TraceEvent(SevError, "TestCreationError").detail("Reason", "No options provided");
		fprintf(stderr, "ERROR: No options were provided for workload.\n");
		throw test_specification_invalid();
	}

	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.sharedRandomNumber = work.sharedRandomNumber;
	wcx.ccr = ccr;
	wcx.dbInfo = dbInfo;
	wcx.rangesToCheck = work.rangesToCheck;
	// FIXME: Other stuff not filled in; why isn't this constructed here and passed down to the other
	// getWorkloadIface()?
	for (int i = 0; i < work.options.size(); i++) {
		ifaces.push_back(getWorkloadIface(work, ccr, work.options[i], dbInfo));
	}
	co_await waitForAll(ifaces);
	auto compound = makeReference<CompoundWorkload>(wcx);
	for (int i = 0; i < work.options.size(); i++) {
		compound->add(ifaces[i].getValue());
	}
	compound->addFailureInjection(work);
	co_return compound;
}

/**
 * Only works in simulation. This method prints all simulated processes in a human readable form to stdout. It groups
 * processes by data center, data hall, zone, and machine (in this order).
 */
void printSimulatedTopology() {
	if (!g_network->isSimulated()) {
		return;
	}
	auto processes = g_simulator->getAllProcesses();
	std::sort(processes.begin(), processes.end(), [](ISimulator::ProcessInfo* lhs, ISimulator::ProcessInfo* rhs) {
		auto l = lhs->locality;
		auto r = rhs->locality;
		if (l.dcId() != r.dcId()) {
			return l.dcId() < r.dcId();
		}
		if (l.dataHallId() != r.dataHallId()) {
			return l.dataHallId() < r.dataHallId();
		}
		if (l.zoneId() != r.zoneId()) {
			return l.zoneId() < r.zoneId();
		}
		if (l.machineId() != r.zoneId()) {
			return l.machineId() < r.machineId();
		}
		return lhs->address < rhs->address;
	});
	printf("Simulated Cluster Topology:\n");
	printf("===========================\n");
	Optional<Standalone<StringRef>> dcId, dataHallId, zoneId, machineId;
	for (auto p : processes) {
		std::string indent = "";
		if (dcId != p->locality.dcId()) {
			dcId = p->locality.dcId();
			printf("%sdcId: %s\n", indent.c_str(), p->locality.describeDcId().c_str());
		}
		indent += "  ";
		if (dataHallId != p->locality.dataHallId()) {
			dataHallId = p->locality.dataHallId();
			printf("%sdataHallId: %s\n", indent.c_str(), p->locality.describeDataHall().c_str());
		}
		indent += "  ";
		if (zoneId != p->locality.zoneId()) {
			zoneId = p->locality.zoneId();
			printf("%szoneId: %s\n", indent.c_str(), p->locality.describeZone().c_str());
		}
		indent += "  ";
		if (machineId != p->locality.machineId()) {
			machineId = p->locality.machineId();
			printf("%smachineId: %s\n", indent.c_str(), p->locality.describeMachineId().c_str());
		}
		indent += "  ";
		printf("%sAddress: %s\n", indent.c_str(), p->address.toString().c_str());
		indent += "  ";
		printf("%sClass: %s\n", indent.c_str(), p->startingClass.toString().c_str());
		printf("%sName: %s\n", indent.c_str(), p->name.c_str());
	}
}

Future<Void> databaseWarmer(Database const& cx) {
	// C++20 coroutine safety: copy const& params to survive across suspend points
	Database cxCopy = cx;
	while (true) {
		Transaction tr(cxCopy);
		co_await success(tr.getReadVersion());
		co_await delay(0.25);
	}
	co_return; // Unreachable but required for coroutine
}

// Tries indefinitely to commit a simple, self conflicting transaction
Future<Void> pingDatabase(Database cx) {
	Transaction tr(cx);
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> v =
			    co_await tr.get(StringRef("/Liveness/" + deterministicRandom()->randomUniqueID().toString()));
			tr.makeSelfConflicting();
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			TraceEvent("PingingDatabaseTransactionError").error(e);
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await tr.onError(caughtError);
		}
	}
}

Future<Void> testDatabaseLiveness(Database cx, double databasePingDelay, std::string context, double startDelay = 0.0) {
	co_await delay(startDelay);
	while (true) {
		Error caughtError;

		try {
			double start = now();
			auto traceMsg = "PingingDatabaseLiveness_" + context;
			TraceEvent(traceMsg.c_str()).log();
			co_await timeoutError(pingDatabase(cx), databasePingDelay);
			double pingTime = now() - start;
			ASSERT(pingTime > 0);
			TraceEvent(("PingingDatabaseLivenessDone_" + context).c_str()).detail("TimeTaken", pingTime);
			co_await delay(databasePingDelay - pingTime);
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				TraceEvent(SevError, ("PingingDatabaseLivenessError_" + context).c_str())
				    .error(e)
				    .detail("PingDelay", databasePingDelay);
			throw;
		}
	}
}

template <class T>
void sendResult(ReplyPromise<T>& reply, Optional<ErrorOr<T>> const& result) {
	auto& res = result.get();
	if (res.isError())
		reply.sendError(res.getError());
	else
		reply.send(res.get());
}

Future<Reference<TestWorkload>> getConsistencyCheckUrgentWorkloadIface(WorkloadRequest work,
                                                                       Reference<IClusterConnectionRecord> ccr,
                                                                       Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	WorkloadContext wcx;
	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.sharedRandomNumber = work.sharedRandomNumber;
	wcx.ccr = ccr;
	wcx.dbInfo = dbInfo;
	wcx.rangesToCheck = work.rangesToCheck;
	Reference<TestWorkload> iface = co_await getWorkloadIface(work, ccr, work.options[0], dbInfo);
	co_return iface;
}

Future<Void> runConsistencyCheckUrgentWorkloadAsync(Database cx,
                                                    WorkloadInterface workIface,
                                                    Reference<TestWorkload> workload) {
	ReplyPromise<Void> jobReq = co_await workIface.start.getFuture();

	try {
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadReceived", workIface.id())
		    .detail("WorkloadName", workload->description())
		    .detail("ClientCount", workload->clientCount)
		    .detail("ClientId", workload->clientId);
		co_await workload->start(cx);
		jobReq.send(Void());
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadError", workIface.id())
		    .errorUnsuppressed(e)
		    .detail("WorkloadName", workload->description())
		    .detail("ClientCount", workload->clientCount)
		    .detail("ClientId", workload->clientId);
		jobReq.sendError(consistency_check_urgent_task_failed());
	}
	co_return;
}

Future<Void> testerServerConsistencyCheckerUrgentWorkload(WorkloadRequest work,
                                                          Reference<IClusterConnectionRecord> ccr,
                                                          Reference<AsyncVar<struct ServerDBInfo> const> dbInfo) {
	WorkloadInterface workIface;
	bool replied = false;

	try {
		Database cx = openDBOnServer(dbInfo);
		co_await delay(1.0);
		Reference<TestWorkload> workload = co_await getConsistencyCheckUrgentWorkloadIface(work, ccr, dbInfo);
		Future<Void> test = runConsistencyCheckUrgentWorkloadAsync(cx, workIface, workload);
		work.reply.send(workIface);
		replied = true;
		co_await test;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterRunWorkloadFailed").errorUnsuppressed(e);
		if (!replied) {
			work.reply.sendError(e);
		}
	}
	co_return;
}

Future<Void> runWorkloadAsync(Database cx,
                              WorkloadInterface workIface,
                              Reference<TestWorkload> workload,
                              double databasePingDelay) {
	Optional<ErrorOr<Void>> setupResult;
	Optional<ErrorOr<Void>> startResult;
	Optional<ErrorOr<CheckReply>> checkResult;
	ReplyPromise<Void> setupReq;
	ReplyPromise<Void> startReq;
	ReplyPromise<CheckReply> checkReq;
	ReplyPromise<std::vector<PerfMetric>> metricsReq;

	TraceEvent("TestBeginAsync", workIface.id())
	    .detail("Workload", workload->description())
	    .detail("DatabasePingDelay", databasePingDelay);

	Future<Void> databaseError =
	    databasePingDelay == 0.0 ? Never() : testDatabaseLiveness(cx, databasePingDelay, "RunWorkloadAsync");

	ReplyPromise<Void> stopReq;
	while (true) {
		int action = 0;
		co_await Choose()
		    .When(workIface.setup.getFuture(), [&](ReplyPromise<Void> const& req) { setupReq = req; action = 1; })
		    .When(workIface.start.getFuture(), [&](ReplyPromise<Void> const& req) { startReq = req; action = 2; })
		    .When(workIface.check.getFuture(),
		          [&](ReplyPromise<CheckReply> const& req) { checkReq = req; action = 3; })
		    .When(workIface.metrics.getFuture(),
		          [&](ReplyPromise<std::vector<PerfMetric>> const& req) { metricsReq = req; action = 4; })
		    .When(workIface.stop.getFuture(), [&](ReplyPromise<Void> const& r) { stopReq = r; action = 5; })
		    .run();

		if (action == 5) {
			stopReq.send(Void());
			break;
		}

		if (action == 1) {
			// setup
			printf("Test received trigger for setup...\n");
			TraceEvent("TestSetupBeginning", workIface.id()).detail("Workload", workload->description());
			if (!setupResult.present()) {
				try {
					co_await (workload->setup(cx) || databaseError);
					TraceEvent("TestSetupComplete", workIface.id()).detail("Workload", workload->description());
					setupResult = Void();
				} catch (Error& e) {
					setupResult = operation_failed();
					TraceEvent(SevError, "TestSetupError", workIface.id())
					    .error(e)
					    .detail("Workload", workload->description());
					if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
						throw;
				}
			}
			sendResult(setupReq, setupResult);
		} else if (action == 2) {
			// start
			if (!startResult.present()) {
				try {
					TraceEvent("TestStarting", workIface.id()).detail("Workload", workload->description());
					co_await (workload->start(cx) || databaseError);
					startResult = Void();
				} catch (Error& e) {
					startResult = operation_failed();
					if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
						throw;
					TraceEvent(SevError, "TestFailure", workIface.id())
					    .errorUnsuppressed(e)
					    .detail("Reason", "Error starting workload")
					    .detail("Workload", workload->description());
				}
				TraceEvent("TestComplete", workIface.id())
				    .detail("Workload", workload->description())
				    .detail("OK", !startResult.get().isError());
				printf("%s complete\n", workload->description().c_str());
			}
			sendResult(startReq, startResult);
		} else if (action == 3) {
			// check
			if (!checkResult.present()) {
				try {
					TraceEvent("TestChecking", workIface.id()).detail("Workload", workload->description());
					bool check = co_await timeoutError(workload->check(cx), workload->getCheckTimeout());
					checkResult = CheckReply{ (!startResult.present() || !startResult.get().isError()) && check };
				} catch (Error& e) {
					checkResult = operation_failed();
					if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
						throw;
					TraceEvent(SevError, "TestFailure", workIface.id())
					    .error(e)
					    .detail("Reason", "Error checking workload")
					    .detail("Workload", workload->description());
				}
				TraceEvent("TestCheckComplete", workIface.id()).detail("Workload", workload->description());
			}
			sendResult(checkReq, checkResult);
		} else if (action == 4) {
			// metrics
			try {
				std::vector<PerfMetric> m = co_await workload->getMetrics();
				TraceEvent("WorkloadSendMetrics", workIface.id()).detail("Count", m.size());
				metricsReq.send(m);
			} catch (Error& e) {
				if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
					throw;
				TraceEvent(SevError, "WorkloadSendMetrics", workIface.id()).error(e);
				metricsReq.sendError(operation_failed());
			}
		}
	}
	co_return;
}

Future<Void> testerServerWorkload(WorkloadRequest work,
                                  Reference<IClusterConnectionRecord> ccr,
                                  Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                  LocalityData locality) {
	WorkloadInterface workIface;
	bool replied = false;
	Database cx;

	try {
		std::map<std::string, std::string> details;
		details["WorkloadTitle"] = printable(work.title);
		details["ClientId"] = format("%d", work.clientId);
		details["ClientCount"] = format("%d", work.clientCount);
		details["WorkloadTimeout"] = format("%d", work.timeout);
		startRole(Role::TESTER, workIface.id(), UID(), details);

		if (work.useDatabase) {
			cx = Database::createDatabase(ccr, ApiVersion::LATEST_VERSION, IsInternal::True, locality);
			co_await delay(1.0);
		}

		// add test for "done" ?
		TraceEvent("WorkloadReceived", workIface.id()).detail("Title", work.title);
		Reference<TestWorkload> workload = co_await getWorkloadIface(work, ccr, dbInfo);
		if (!workload) {
			TraceEvent("TestCreationError").detail("Reason", "Workload could not be created");
			fprintf(stderr, "ERROR: The workload could not be created.\n");
			throw test_specification_invalid();
		}
		Future<Void> test = runWorkloadAsync(cx, workIface, workload, work.databasePingDelay) ||
		                    traceRole(Role::TESTER, workIface.id());
		work.reply.send(workIface);
		replied = true;

		if (work.timeout > 0) {
			test = timeoutError(test, work.timeout);
		}

		co_await test;

		endRole(Role::TESTER, workIface.id(), "Complete");
	} catch (Error& e) {
		TraceEvent(SevDebug, "TesterWorkloadFailed").errorUnsuppressed(e);
		if (!replied) {
			if (e.code() == error_code_test_specification_invalid)
				work.reply.sendError(e);
			else
				work.reply.sendError(operation_failed());
		}

		bool ok = e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete ||
		          e.code() == error_code_actor_cancelled;
		endRole(Role::TESTER, workIface.id(), "Error", ok, e);

		if (e.code() != error_code_test_specification_invalid && e.code() != error_code_timed_out) {
			throw; // fatal errors will kill the testerServer as well
		}
	}
	co_return;
}

Future<Void> testerServerCore(TesterInterface const& interf,
                              Reference<IClusterConnectionRecord> const& ccr,
                              Reference<AsyncVar<struct ServerDBInfo> const> const& dbInfo,
                              LocalityData const& locality,
                              Optional<std::string> const& expectedWorkLoad) {
	// C++20 coroutine safety: const& parameters only store the reference in the coroutine frame,
	// not the object. The referred-to object may be destroyed after the coroutine suspends
	// (e.g. local variables in a caller's if-block, or temporaries from default arguments).
	// Copy all const& parameters to ensure they survive across suspend points.
	TesterInterface interfCopy = interf;
	Reference<IClusterConnectionRecord> ccrCopy = ccr;
	Reference<AsyncVar<struct ServerDBInfo> const> dbInfoCopy = dbInfo;
	LocalityData localityCopy = locality;
	Optional<std::string> expectedWorkLoadCopy = expectedWorkLoad;

	PromiseStream<Future<Void>> addWorkload;
	Future<Void> workerFatalError = actorCollection(addWorkload.getFuture());

	// Dedicated to consistencyCheckerUrgent
	// At any time, we only allow at most 1 consistency checker workload on a server
	std::pair<int64_t, Future<Void>> consistencyCheckerUrgentTester = std::make_pair(0, Future<Void>());

	TraceEvent("StartingTesterServerCore", interfCopy.id())
	    .detail("ExpectedWorkload", expectedWorkLoadCopy.present() ? expectedWorkLoadCopy.get() : "[Unset]");

	while (true) {
		int action = 0;
		WorkloadRequest workReq;
		Future<Void> checkerDone = consistencyCheckerUrgentTester.second.isValid()
		                               ? consistencyCheckerUrgentTester.second
		                               : Never();

		co_await Choose()
		    .When(workerFatalError, [&](Void const&) { action = 1; })
		    .When(checkerDone, [&](Void const&) { action = 2; })
		    .When(interfCopy.recruitments.getFuture(),
		          [&](WorkloadRequest const& work) {
			          workReq = work;
			          action = 3;
		          })
		    .run();

		if (action == 1) {
			break;
		}

		if (action == 2) {
			ASSERT(consistencyCheckerUrgentTester.first != 0);
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadEnd", interfCopy.id())
			    .detail("ConsistencyCheckerId", consistencyCheckerUrgentTester.first);
			consistencyCheckerUrgentTester = std::make_pair(0, Future<Void>()); // reset
			continue;
		}

		if (action == 3) {
			if (expectedWorkLoadCopy.present() && expectedWorkLoadCopy.get() != workReq.title) {
				TraceEvent(SevError, "StartingTesterServerCoreUnexpectedWorkload", interfCopy.id())
				    .detail("ClientId", workReq.clientId)
				    .detail("ClientCount", workReq.clientCount)
				    .detail("ExpectedWorkLoad", expectedWorkLoadCopy.get())
				    .detail("WorkLoad", workReq.title);
				// Drop the workload
			} else if (workReq.title == "ConsistencyCheckUrgent") {
				// The workload is a consistency checker urgent workload
				if (workReq.sharedRandomNumber == consistencyCheckerUrgentTester.first) {
					// A single req can be sent for multiple times. In this case, the sharedRandomNumber is same as
					// the existing one. For this scenario, we reply an error. This case should be rare.
					TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterDuplicatedRequest", interfCopy.id())
					    .detail("ConsistencyCheckerId", workReq.sharedRandomNumber)
					    .detail("ClientId", workReq.clientId)
					    .detail("ClientCount", workReq.clientCount);
					workReq.reply.sendError(consistency_check_urgent_duplicate_request());
				} else {
					// When the req.sharedRandomNumber is different from the existing one, the cluster has muiltiple
					// consistencycheckurgent roles at the same time. Evenutally, the cluster will have only one
					// consistencycheckurgent role in a stable state. So, in this case, we simply let the new request to
					// overwrite the old request. After the work is destroyed, the broken_promise will be replied.
					if (consistencyCheckerUrgentTester.second.isValid() &&
					    !consistencyCheckerUrgentTester.second.isReady()) {
						TraceEvent(SevWarnAlways, "ConsistencyCheckUrgent_TesterWorkloadConflict", interfCopy.id())
						    .detail("ExistingConsistencyCheckerId", consistencyCheckerUrgentTester.first)
						    .detail("ArrivingConsistencyCheckerId", workReq.sharedRandomNumber)
						    .detail("ClientId", workReq.clientId)
						    .detail("ClientCount", workReq.clientCount);
					}
					consistencyCheckerUrgentTester = std::make_pair(
					    workReq.sharedRandomNumber,
					    testerServerConsistencyCheckerUrgentWorkload(workReq, ccrCopy, dbInfoCopy));
					TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadInitialized", interfCopy.id())
					    .detail("ConsistencyCheckerId", consistencyCheckerUrgentTester.first)
					    .detail("ClientId", workReq.clientId)
					    .detail("ClientCount", workReq.clientCount);
				}
			} else {
				addWorkload.send(testerServerWorkload(workReq, ccrCopy, dbInfoCopy, localityCopy));
			}
		}
	}
	co_return;
}

Future<Void> clearData(Database cx) {
	TraceEvent("ClearData_Start").detail("Phase", "Enter");

	Transaction tr(cx);
	TraceEvent("ClearData_TrCreated").detail("Phase", "Loop1_Start");

	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		TraceEvent("ClearData_Loop1_Iteration").detail("Phase", "TryBlock");

		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			TraceEvent("ClearData_DebugSet").detail("Phase", "Loop1_Debug");

			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterClearingDatabaseStart", tr.trState->readOptions.get().debugID.get()).log();

			// This transaction needs to be self-conflicting, but not conflict consistently with
			// any other transactions
			tr.clear(normalKeys);
			tr.makeSelfConflicting();
			TraceEvent("ClearData_PreGetReadVersion").detail("Phase", "Loop1_AboutToAwait");

			Version rv = co_await tr.getReadVersion(); // required since we use addReadConflictRange but not get
			TraceEvent("TesterClearingDatabaseRV", tr.trState->readOptions.get().debugID.get()).detail("RV", rv);
			TraceEvent("ClearData_PreCommit").detail("Phase", "Loop1_AboutToCommit");

			co_await tr.commit();
			TraceEvent("TesterClearingDatabase", tr.trState->readOptions.get().debugID.get())
			    .detail("AtVersion", tr.getCommittedVersion());
			TraceEvent("ClearData_Loop1_Success").detail("Phase", "Loop1_Complete");
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterClearingDatabaseError", tr.trState->readOptions.get().debugID.get()).error(e);
			TraceEvent("ClearData_Loop1_Catch").detail("Phase", "Loop1_Error").detail("ErrorCode", e.code());
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			TraceEvent("ClearData_Loop1_ErrorHandling").detail("Phase", "Loop1_OnError");
			co_await tr.onError(caughtError);
		}
	}

	TraceEvent("ClearData_Loop1_Done").detail("Phase", "Loop2_Start");
	tr = Transaction(cx);
	TraceEvent("ClearData_NewTr").detail("Phase", "Loop2_TrCreated");

	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		TraceEvent("ClearData_Loop2_Iteration").detail("Phase", "Loop2_TryBlock");

		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			tr.setOption(FDBTransactionOptions::RAW_ACCESS);
			TraceEvent("ClearData_Loop2_PreGetRange").detail("Phase", "Loop2_AboutToAwait");

			RangeResult rangeResult = co_await tr.getRange(normalKeys, 1);
			TraceEvent("ClearData_Loop2_PostGetRange")
			    .detail("Phase", "Loop2_RangeGot")
			    .detail("ResultSize", rangeResult.size());

			// If the result is non-empty, it is possible that there is some bug.
			if (!rangeResult.empty()) {
				TraceEvent(SevError, "TesterClearFailure").detail("FirstKey", rangeResult[0].key);
				ASSERT(false);
			}
			// TODO(gglass): the assert below is leftover from some tenant logic.  If it fails, take it out.
			// If not, leave it in and take out this comment.
			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterCheckDatabaseClearedDone", tr.trState->readOptions.get().debugID.get());
			TraceEvent("ClearData_Loop2_Success").detail("Phase", "Loop2_Complete");
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterCheckDatabaseClearedError", tr.trState->readOptions.get().debugID.get())
			    .error(e);
			TraceEvent("ClearData_Loop2_Catch").detail("Phase", "Loop2_Error").detail("ErrorCode", e.code());
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			TraceEvent("ClearData_Loop2_ErrorHandling").detail("Phase", "Loop2_OnError");
			co_await tr.onError(caughtError);
		}
	}

	TraceEvent("ClearData_End").detail("Phase", "Success");
	co_return;
}

Future<Void> dumpDatabase(Database const& cx, std::string const& outputFilename, KeyRange const& range);

int passCount = 0;
int failCount = 0;

std::vector<PerfMetric> aggregateMetrics(std::vector<std::vector<PerfMetric>> metrics) {
	std::map<std::string, std::vector<PerfMetric>> metricMap;
	for (int i = 0; i < metrics.size(); i++) {
		std::vector<PerfMetric> workloadMetrics = metrics[i];
		TraceEvent("MetricsReturned").detail("Count", workloadMetrics.size());
		for (int m = 0; m < workloadMetrics.size(); m++) {
			printf("Metric (%d, %d): %s, %f, %s\n",
			       i,
			       m,
			       workloadMetrics[m].name().c_str(),
			       workloadMetrics[m].value(),
			       workloadMetrics[m].formatted().c_str());
			metricMap[workloadMetrics[m].name()].push_back(workloadMetrics[m]);
		}
	}
	TraceEvent("Metric")
	    .detail("Name", "Reporting Clients")
	    .detail("Value", (double)metrics.size())
	    .detail("Formatted", format("%d", metrics.size()).c_str());

	std::vector<PerfMetric> result;
	std::map<std::string, std::vector<PerfMetric>>::iterator it;
	for (it = metricMap.begin(); it != metricMap.end(); it++) {
		auto& vec = it->second;
		if (!vec.size())
			continue;
		double sum = 0;
		for (int i = 0; i < vec.size(); i++)
			sum += vec[i].value();
		if (vec[0].averaged() && vec.size())
			sum /= vec.size();
		result.emplace_back(vec[0].name(), sum, Averaged::False, vec[0].format_code());
	}
	return result;
}

void logMetrics(std::vector<PerfMetric> metrics) {
	for (int idx = 0; idx < metrics.size(); idx++)
		TraceEvent("Metric")
		    .detail("Name", metrics[idx].name())
		    .detail("Value", metrics[idx].value())
		    .detail("Formatted", format(metrics[idx].format_code().c_str(), metrics[idx].value()));
}

template <class T>
void throwIfError(const std::vector<Future<ErrorOr<T>>>& futures, std::string errorMsg) {
	for (auto& future : futures) {
		if (future.get().isError()) {
			TraceEvent(SevError, errorMsg.c_str()).error(future.get().getError());
			throw future.get().getError();
		}
	}
}

struct TesterConsistencyScanState {
	bool enabled = false;
	bool enableAfter = false;
	bool waitForComplete = false;
};

Future<Void> checkConsistencyScanAfterTest(Database cx, TesterConsistencyScanState* csState) {
	if (!csState->enabled) {
		co_return;
	}

	// mark it as done so later so this does not repeat
	csState->enabled = false;

	if (csState->enableAfter || csState->waitForComplete) {
		printf("Enabling consistency scan after test ...\n");
		co_await enableConsistencyScanInSim(cx);
		printf("Enabled consistency scan after test.\n");
	}

	co_await disableConsistencyScanInSim(cx, csState->waitForComplete);

	co_return;
}

Future<DistributedTestResults> runWorkload(Database const& cx,
                                           std::vector<TesterInterface> const& testers,
                                           TestSpec const& spec) {
	// C++20 coroutine safety: copy const& params to survive across suspend points
	Database cxCopy = cx;
	std::vector<TesterInterface> testersCopy = testers;
	TestSpec specCopy = spec;

	TraceEvent("TestRunning")
	    .detail("WorkloadTitle", specCopy.title)
	    .detail("TesterCount", testersCopy.size())
	    .detail("Phases", specCopy.phases)
	    .detail("TestTimeout", specCopy.timeout);

	std::vector<Future<WorkloadInterface>> workRequests;
	std::vector<std::vector<PerfMetric>> metricsResults;

	int i = 0;
	int success = 0;
	int failure = 0;
	int64_t sharedRandom = deterministicRandom()->randomInt64(0, 10000000);
	for (; i < testersCopy.size(); i++) {
		WorkloadRequest req;
		req.title = specCopy.title;
		req.useDatabase = specCopy.useDB;
		req.runFailureWorkloads = specCopy.runFailureWorkloads;
		req.timeout = specCopy.timeout;
		req.databasePingDelay = specCopy.useDB ? specCopy.databasePingDelay : 0.0;
		req.options = specCopy.options;
		req.clientId = i;
		req.clientCount = testersCopy.size();
		req.sharedRandomNumber = sharedRandom;
		req.disabledFailureInjectionWorkloads = specCopy.disabledFailureInjectionWorkloads;
		workRequests.push_back(testersCopy[i].recruitments.getReply(req));
	}

	std::vector<WorkloadInterface> workloads = co_await getAll(workRequests);
	double waitForFailureTime = g_network->isSimulated() ? 24 * 60 * 60 : 60;
	if (g_network->isSimulated() && specCopy.simCheckRelocationDuration)
		debug_setCheckRelocationDuration(true);

	if (specCopy.phases & TestWorkload::SETUP) {
		std::vector<Future<ErrorOr<Void>>> setups;
		printf("setting up test (%s)...\n", printable(specCopy.title).c_str());
		TraceEvent("TestSetupStart").detail("WorkloadTitle", specCopy.title);
		setups.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			setups.push_back(workloads[i].setup.template getReplyUnlessFailedFor<Void>(waitForFailureTime, 0));
		co_await waitForAll(setups);
		throwIfError(setups, "SetupFailedForWorkload" + printable(specCopy.title));
		TraceEvent("TestSetupComplete").detail("WorkloadTitle", specCopy.title);
	}

	if (specCopy.phases & TestWorkload::EXECUTION) {
		TraceEvent("TestStarting").detail("WorkloadTitle", specCopy.title);
		printf("running test (%s)...\n", printable(specCopy.title).c_str());
		std::vector<Future<ErrorOr<Void>>> starts;
		starts.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			starts.push_back(workloads[i].start.template getReplyUnlessFailedFor<Void>(waitForFailureTime, 0));
		co_await waitForAll(starts);
		throwIfError(starts, "StartFailedForWorkload" + printable(specCopy.title));
		printf("%s complete\n", printable(specCopy.title).c_str());
		TraceEvent("TestComplete").detail("WorkloadTitle", specCopy.title);
	}

	if (specCopy.phases & TestWorkload::CHECK) {
		if (specCopy.useDB && (specCopy.phases & TestWorkload::EXECUTION)) {
			co_await delay(3.0);
		}

		std::vector<Future<ErrorOr<CheckReply>>> checks;
		TraceEvent("TestCheckingResults").detail("WorkloadTitle", specCopy.title);

		printf("checking test (%s)...\n", printable(specCopy.title).c_str());

		checks.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			checks.push_back(workloads[i].check.template getReplyUnlessFailedFor<CheckReply>(waitForFailureTime, 0));
		co_await waitForAll(checks);

		throwIfError(checks, "CheckFailedForWorkload" + printable(specCopy.title));

		for (int i = 0; i < checks.size(); i++) {
			if (checks[i].get().get().value)
				success++;
			else
				failure++;
		}
		TraceEvent("TestCheckComplete").detail("WorkloadTitle", specCopy.title);
	}

	if (specCopy.phases & TestWorkload::METRICS) {
		std::vector<Future<ErrorOr<std::vector<PerfMetric>>>> metricTasks;
		printf("fetching metrics (%s)...\n", printable(specCopy.title).c_str());
		TraceEvent("TestFetchingMetrics").detail("WorkloadTitle", specCopy.title);
		metricTasks.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			metricTasks.push_back(
			    workloads[i].metrics.template getReplyUnlessFailedFor<std::vector<PerfMetric>>(waitForFailureTime, 0));
		co_await waitForAll(metricTasks);
		throwIfError(metricTasks, "MetricFailedForWorkload" + printable(specCopy.title));
		for (int i = 0; i < metricTasks.size(); i++) {
			metricsResults.push_back(metricTasks[i].get().get());
		}
	}

	// Stopping the workloads is unreliable, but they have a timeout
	// FIXME: stop if one of the above phases throws an exception
	for (int i = 0; i < workloads.size(); i++)
		workloads[i].stop.send(ReplyPromise<Void>());

	co_return DistributedTestResults(aggregateMetrics(metricsResults), success, failure);
}

// Sets the database configuration by running the ChangeConfig workload
Future<Void> changeConfiguration(Database cx, std::vector<TesterInterface> testers, StringRef configMode) {
	TestSpec spec;
	Standalone<VectorRef<KeyValueRef>> options;
	spec.title = "ChangeConfig"_sr;
	spec.runFailureWorkloads = false;
	options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ChangeConfig"_sr));
	options.push_back_deep(options.arena(), KeyValueRef("configMode"_sr, configMode));
	spec.options.push_back_deep(spec.options.arena(), options);

	DistributedTestResults testResults = co_await runWorkload(cx, testers, spec);

	co_return;
}

Future<Void> auditStorageCorrectness(Reference<AsyncVar<ServerDBInfo>> dbInfo, AuditType auditType) {
	if (SERVER_KNOBS->DISABLE_AUDIT_STORAGE_FINAL_REPLICA_CHECK_IN_SIM &&
	    (auditType == AuditType::ValidateHA || auditType == AuditType::ValidateReplica)) {
		co_return;
	}
	TraceEvent(SevDebug, "AuditStorageCorrectnessBegin").detail("AuditType", auditType);
	Database cx;
	UID auditId;
	AuditStorageState auditState;

	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			while (dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
			       !dbInfo->get().distributor.present()) {
				co_await dbInfo->onChange();
			}
			TriggerAuditRequest req(auditType, allKeys, KeyValueStoreType::END); // do not specify engine type to check
			UID auditId_ = co_await timeoutError(dbInfo->get().distributor.get().triggerAudit.getReply(req), 300);
			auditId = auditId_;
			TraceEvent(SevDebug, "AuditStorageCorrectnessTriggered")
			    .detail("AuditID", auditId)
			    .detail("AuditType", auditType);
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "AuditStorageCorrectnessTriggerError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId)
			    .detail("AuditType", auditType);
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await delay(1);
		}
	}

	int retryCount = 0;
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			cx = openDBOnServer(dbInfo);
			AuditStorageState auditState_ = co_await getAuditState(cx, auditType, auditId);
			auditState = auditState_;
			if (auditState.getPhase() == AuditPhase::Complete) {
				break;
			} else if (auditState.getPhase() == AuditPhase::Running) {
				TraceEvent("AuditStorageCorrectnessWait")
				    .detail("AuditID", auditId)
				    .detail("AuditType", auditType)
				    .detail("RetryCount", retryCount);
				co_await delay(25);
				if (retryCount > 20) {
					TraceEvent("AuditStorageCorrectnessWaitFailed")
					    .detail("AuditID", auditId)
					    .detail("AuditType", auditType);
					break;
				}
				retryCount++;
				continue;
			} else if (auditState.getPhase() == AuditPhase::Error) {
				break;
			} else if (auditState.getPhase() == AuditPhase::Failed) {
				break;
			} else {
				UNREACHABLE();
			}
		} catch (Error& e) {
			TraceEvent("AuditStorageCorrectnessWaitError")
			    .errorUnsuppressed(e)
			    .detail("AuditID", auditId)
			    .detail("AuditType", auditType)
			    .detail("AuditState", auditState.toString());
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await delay(1);
		}
	}
	TraceEvent("AuditStorageCorrectnessWaitEnd")
	    .detail("AuditID", auditId)
	    .detail("AuditType", auditType)
	    .detail("AuditState", auditState.toString());

	co_return;
}

// Runs the consistency check workload, which verifies that the database is in a consistent state
Future<Void> checkConsistency(Database cx,
                              std::vector<TesterInterface> testers,
                              bool doQuiescentCheck,
                              bool doTSSCheck,
                              double maxDDRunTime,
                              double softTimeLimit,
                              double databasePingDelay,
                              Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	TestSpec spec;

	double connectionFailures;
	if (g_network->isSimulated()) {
		// NOTE: the value will be reset after consistency check
		connectionFailures = g_simulator->connectionFailuresDisableDuration;
		disableConnectionFailures("ConsistencyCheck");
		g_simulator->isConsistencyChecked = true;
	}

	Standalone<VectorRef<KeyValueRef>> options;
	StringRef performQuiescent = "false"_sr;
	StringRef performTSSCheck = "false"_sr;
	if (doQuiescentCheck) {
		performQuiescent = "true"_sr;
		spec.restorePerpetualWiggleSetting = false;
	}
	if (doTSSCheck) {
		performTSSCheck = "true"_sr;
	}
	spec.title = "ConsistencyCheck"_sr;
	spec.databasePingDelay = databasePingDelay;
	spec.runFailureWorkloads = false;
	spec.timeout = 32000;
	options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ConsistencyCheck"_sr));
	options.push_back_deep(options.arena(), KeyValueRef("performQuiescentChecks"_sr, performQuiescent));
	options.push_back_deep(options.arena(), KeyValueRef("performTSSCheck"_sr, performTSSCheck));
	options.push_back_deep(options.arena(),
	                       KeyValueRef("maxDDRunTime"_sr, ValueRef(options.arena(), format("%f", maxDDRunTime))));
	options.push_back_deep(options.arena(), KeyValueRef("distributed"_sr, "false"_sr));
	spec.options.push_back_deep(spec.options.arena(), options);

	double start = now();
	bool lastRun = false;
	while (true) {
		DistributedTestResults testResults = co_await runWorkload(cx, testers, spec);
		if (testResults.ok() || lastRun) {
			if (g_network->isSimulated()) {
				g_simulator->connectionFailuresDisableDuration = connectionFailures;
				g_simulator->isConsistencyChecked = false;
			}
			co_return;
		}
		if (now() - start > softTimeLimit) {
			spec.options[0].push_back_deep(spec.options.arena(), KeyValueRef("failureIsError"_sr, "true"_sr));
			lastRun = true;
		}

		co_await repairDeadDatacenter(cx, dbInfo, "ConsistencyCheck");
	}
}

Future<std::unordered_set<int>> runUrgentConsistencyCheckWorkload(
    Database cx,
    std::vector<TesterInterface> testers,
    int64_t consistencyCheckerId,
    std::unordered_map<int, std::vector<KeyRange>> assignment) {
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_DispatchWorkloads")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);

	// Step 1: Get interfaces for running workloads
	std::vector<Future<ErrorOr<WorkloadInterface>>> workRequests;
	Standalone<VectorRef<KeyValueRef>> option;
	option.push_back_deep(option.arena(), KeyValueRef("testName"_sr, "ConsistencyCheckUrgent"_sr));
	Standalone<VectorRef<VectorRef<KeyValueRef>>> options;
	options.push_back_deep(options.arena(), option);
	for (int i = 0; i < testers.size(); i++) {
		WorkloadRequest req;
		req.title = "ConsistencyCheckUrgent"_sr;
		req.useDatabase = true;
		req.timeout = 0.0; // disable timeout workload
		req.databasePingDelay = 0.0; // disable databased ping check
		req.options = options;
		req.clientId = i;
		req.clientCount = testers.size();
		req.sharedRandomNumber = consistencyCheckerId;
		req.rangesToCheck = assignment[i];
		workRequests.push_back(testers[i].recruitments.getReplyUnlessFailedFor(req, 10, 0));
		// workRequests follows the order of clientId of assignment
	}
	co_await waitForAll(workRequests);

	// Step 2: Run workloads via the interfaces
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_TriggerWorkloads")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);
	std::unordered_set<int> completeClientIds;
	std::vector<int> clientIds; // record the clientId for jobs
	std::vector<Future<ErrorOr<Void>>> jobs;
	for (int i = 0; i < workRequests.size(); i++) {
		ASSERT(workRequests[i].isReady());
		if (workRequests[i].get().isError()) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_FailedToContactTester")
			    .error(workRequests[i].get().getError())
			    .detail("TesterCount", testers.size())
			    .detail("TesterId", i)
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
		} else {
			jobs.push_back(workRequests[i].get().get().start.template getReplyUnlessFailedFor<Void>(10, 0));
			clientIds.push_back(i);
		}
	}
	co_await waitForAll(jobs);
	for (int i = 0; i < jobs.size(); i++) {
		if (jobs[i].isError()) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RunWorkloadError1")
			    .errorUnsuppressed(jobs[i].getError())
			    .detail("ClientId", clientIds[i])
			    .detail("ClientCount", testers.size())
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
		} else if (jobs[i].get().isError()) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RunWorkloadError2")
			    .errorUnsuppressed(jobs[i].get().getError())
			    .detail("ClientId", clientIds[i])
			    .detail("ClientCount", testers.size())
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
		} else {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RunWorkloadComplete")
			    .detail("ClientId", clientIds[i])
			    .detail("ClientCount", testers.size())
			    .detail("ConsistencyCheckerId", consistencyCheckerId);
			completeClientIds.insert(clientIds[i]); // Add complete clients
		}
	}

	TraceEvent(SevInfo, "ConsistencyCheckUrgent_DispatchWorkloadEnd")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);

	co_return completeClientIds;
}

Future<std::vector<KeyRange>> getConsistencyCheckShards(Database cx, std::vector<KeyRange> ranges) {
	// Get the scope of the input list of ranges
	Key beginKeyToReadKeyServer;
	Key endKeyToReadKeyServer;
	for (int i = 0; i < ranges.size(); i++) {
		if (i == 0 || ranges[i].begin < beginKeyToReadKeyServer) {
			beginKeyToReadKeyServer = ranges[i].begin;
		}
		if (i == 0 || ranges[i].end > endKeyToReadKeyServer) {
			endKeyToReadKeyServer = ranges[i].end;
		}
	}
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_GetConsistencyCheckShards")
	    .detail("RangeBegin", beginKeyToReadKeyServer)
	    .detail("RangeEnd", endKeyToReadKeyServer);
	// Read KeyServer space within the scope and add shards intersecting with the input ranges
	std::vector<KeyRange> res;
	Transaction tr(cx);
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			KeyRange rangeToRead = Standalone(KeyRangeRef(beginKeyToReadKeyServer, endKeyToReadKeyServer));
			RangeResult readResult = co_await krmGetRanges(&tr,
			                                               keyServersPrefix,
			                                               rangeToRead,
			                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
			                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES);
			for (int i = 0; i < readResult.size() - 1; ++i) {
				KeyRange rangeToCheck = Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key));
				Value valueToCheck = Standalone(readResult[i].value);
				bool toAdd = false;
				for (const auto& range : ranges) {
					if (rangeToCheck.intersects(range) == true) {
						toAdd = true;
						break;
					}
				}
				if (toAdd == true) {
					res.push_back(rangeToCheck);
				}
				beginKeyToReadKeyServer = readResult[i + 1].key;
			}
			if (beginKeyToReadKeyServer >= endKeyToReadKeyServer) {
				break;
			}
		} catch (Error& e) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_GetConsistencyCheckShardsRetry").error(e);
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await tr.onError(caughtError);
		}
	}
	co_return res;
}

Future<std::vector<TesterInterface>> getTesters(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                                int minTestersExpected) {
	// Recruit workers
	int flags = GetWorkersRequest::TESTER_CLASS_ONLY | GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY;
	Future<Void> testerTimeout = delay(600.0); // wait 600 sec for testers to show up
	std::vector<WorkerDetails> workers;

	while (true) {
		Future<std::vector<WorkerDetails>> getWorkersReq =
		    cc->get().present()
		        ? brokenPromiseToNever(cc->get().get().getWorkers.getReply(GetWorkersRequest(flags)))
		        : Future<std::vector<WorkerDetails>>(Never());
		Future<Void> ccChange = cc->onChange();

		int action = 0;
		std::vector<WorkerDetails> gotWorkers;

		co_await Choose()
		    .When(getWorkersReq,
		          [&](std::vector<WorkerDetails> const& w) {
			          gotWorkers = w;
			          action = 1;
		          })
		    .When(ccChange, [&](Void const&) { action = 2; })
		    .When(testerTimeout, [&](Void const&) { action = 3; })
		    .run();

		if (action == 1) {
			if (static_cast<int>(gotWorkers.size()) >= minTestersExpected) {
				workers = gotWorkers;
				break;
			}
			co_await delay(SERVER_KNOBS->WORKER_POLL_DELAY);
		} else if (action == 3) {
			TraceEvent(SevWarnAlways, "TesterRecruitmentTimeout");
			throw timed_out();
		}
		// action == 2: cc changed, loop again
	}
	std::vector<TesterInterface> ts;
	ts.reserve(workers.size());
	for (int i = 0; i < workers.size(); i++)
		ts.push_back(workers[i].interf.testerInterface);
	deterministicRandom()->randomShuffle(ts);
	co_return ts;
}

const std::unordered_map<char, uint8_t> parseCharMap{
	{ '0', 0 },  { '1', 1 },  { '2', 2 },  { '3', 3 },  { '4', 4 },  { '5', 5 },  { '6', 6 },  { '7', 7 },
	{ '8', 8 },  { '9', 9 },  { 'a', 10 }, { 'b', 11 }, { 'c', 12 }, { 'd', 13 }, { 'e', 14 }, { 'f', 15 },
	{ 'A', 10 }, { 'B', 11 }, { 'C', 12 }, { 'D', 13 }, { 'E', 14 }, { 'F', 15 },
};

Optional<Key> getKeyFromString(const std::string& str) {
	Key emptyKey;
	if (str.size() == 0) {
		return emptyKey;
	}
	if (str.size() % 4 != 0) {
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "ConsistencyCheckUrgent_GetKeyFromStringError")
		    .setMaxEventLength(-1)
		    .setMaxFieldLength(-1)
		    .detail("Reason", "WrongLength")
		    .detail("InputStr", str);
		return Optional<Key>();
	}
	std::vector<uint8_t> byteList;
	for (int i = 0; i < str.size(); i += 4) {
		if (str.at(i + 0) != '\\' || str.at(i + 1) != 'x') {
			TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
			           "ConsistencyCheckUrgent_GetKeyFromStringError")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Reason", "WrongBytePrefix")
			    .detail("InputStr", str);
			return Optional<Key>();
		}
		const char first = str.at(i + 2);
		const char second = str.at(i + 3);
		if (!parseCharMap.contains(first) || !parseCharMap.contains(second)) {
			TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways,
			           "ConsistencyCheckUrgent_GetKeyFromStringError")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Reason", "WrongByteContent")
			    .detail("InputStr", str);
			return Optional<Key>();
		}
		uint8_t parsedValue = parseCharMap.at(first) * 16 + parseCharMap.at(second);
		byteList.push_back(parsedValue);
	}
	return Standalone(StringRef(byteList.data(), byteList.size()));
}

Optional<std::vector<KeyRange>> loadRangesToCheckFromKnob() {
	// Load string from knob
	std::vector<std::string> beginKeyStrs = {
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_0,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_1,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_2,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_BEGIN_3,
	};
	std::vector<std::string> endKeyStrs = {
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_0,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_1,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_2,
		CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RANGE_END_3,
	};

	// Get keys from strings
	std::vector<Key> beginKeys;
	for (const auto& beginKeyStr : beginKeyStrs) {
		Optional<Key> key = getKeyFromString(beginKeyStr);
		if (key.present()) {
			beginKeys.push_back(key.get());
		} else {
			return Optional<std::vector<KeyRange>>();
		}
	}
	std::vector<Key> endKeys;
	for (const auto& endKeyStr : endKeyStrs) {
		Optional<Key> key = getKeyFromString(endKeyStr);
		if (key.present()) {
			endKeys.push_back(key.get());
		} else {
			return Optional<std::vector<KeyRange>>();
		}
	}
	if (beginKeys.size() != endKeys.size()) {
		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "ConsistencyCheckUrgent_GetKeyFromStringError")
		    .detail("Reason", "MismatchBeginKeysAndEndKeys");
		return Optional<std::vector<KeyRange>>();
	}

	// Get ranges
	KeyRangeMap<bool> rangeToCheckMap;
	for (int i = 0; i < beginKeys.size(); i++) {
		Key rangeBegin = beginKeys[i];
		Key rangeEnd = endKeys[i];
		if (rangeBegin.empty() && rangeEnd.empty()) {
			continue;
		}
		if (rangeBegin > allKeys.end) {
			rangeBegin = allKeys.end;
		}
		if (rangeEnd > allKeys.end) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_ReverseInputRange")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Index", i)
			    .detail("RangeBegin", rangeBegin)
			    .detail("RangeEnd", rangeEnd);
			rangeEnd = allKeys.end;
		}

		KeyRange rangeToCheck;
		if (rangeBegin < rangeEnd) {
			rangeToCheck = Standalone(KeyRangeRef(rangeBegin, rangeEnd));
		} else if (rangeBegin > rangeEnd) {
			rangeToCheck = Standalone(KeyRangeRef(rangeEnd, rangeBegin));
		} else {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_EmptyInputRange")
			    .setMaxEventLength(-1)
			    .setMaxFieldLength(-1)
			    .detail("Index", i)
			    .detail("RangeBegin", rangeBegin)
			    .detail("RangeEnd", rangeEnd);
			continue;
		}
		rangeToCheckMap.insert(rangeToCheck, true);
	}

	rangeToCheckMap.coalesce(allKeys);

	std::vector<KeyRange> res;
	for (auto rangeToCheck : rangeToCheckMap.ranges()) {
		if (rangeToCheck.value() == true) {
			res.push_back(rangeToCheck.range());
		}
	}
	TraceEvent e(SevInfo, "ConsistencyCheckUrgent_LoadedInputRange");
	e.setMaxEventLength(-1);
	e.setMaxFieldLength(-1);
	for (int i = 0; i < res.size(); i++) {
		e.detail("RangeBegin" + std::to_string(i), res[i].begin);
		e.detail("RangeEnd" + std::to_string(i), res[i].end);
	}
	return res;
}

std::unordered_map<int, std::vector<KeyRange>> makeTaskAssignment(Database cx,
                                                                  int64_t consistencyCheckerId,
                                                                  std::vector<KeyRange> shardsToCheck,
                                                                  int testersCount,
                                                                  int round) {
	ASSERT(testersCount >= 1);
	std::unordered_map<int, std::vector<KeyRange>> assignment;

	std::vector<size_t> shuffledIndices(testersCount);
	std::iota(shuffledIndices.begin(), shuffledIndices.end(), 0); // creates [0, 1, ..., testersCount - 1]
	deterministicRandom()->randomShuffle(shuffledIndices);

	int batchSize = CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_BATCH_SHARD_COUNT;
	int startingPoint = 0;
	if (shardsToCheck.size() > batchSize * testersCount) {
		startingPoint = deterministicRandom()->randomInt(0, shardsToCheck.size() - batchSize * testersCount);
		// We randomly pick a set of successive shards:
		// (1) We want to retry for different shards to avoid repeated failure on the same shards
		// (2) We want to check successive shards to avoid inefficiency incurred by fragments
	}
	assignment.clear();
	for (int i = startingPoint; i < shardsToCheck.size(); i++) {
		int testerIdx = (i - startingPoint) / batchSize;
		if (testerIdx > testersCount - 1) {
			break; // Have filled up all testers
		}
		// When assigning a shards/batch to a tester idx, there are certain edge cases which can result in urgent
		// consistency checker being infinetely stuck in a loop. Examples:
		//      1. if there is 1 remaining shard, and tester 0 consistently fails, we will still always pick tester 0
		//      2. if there are 10 remaining shards, and batch size is 10, and tester 0 consistently fails, we will
		//      still always pick tester 0
		//      3. if there are 20 remaining shards, and batch size is 10, and testers {0, 1} consistently fail, we will
		//      keep picking testers {0, 1}
		// To avoid repeatedly picking the same testers even though they could be failing, shuffledIndices provides an
		// indirection to a random tester idx. That way, each invocation of makeTaskAssignment won't
		// result in the same task assignment for the class of edge cases mentioned above.
		assignment[shuffledIndices[testerIdx]].push_back(shardsToCheck[i]);
	}
	std::unordered_map<int, std::vector<KeyRange>>::iterator assignIt;
	for (assignIt = assignment.begin(); assignIt != assignment.end(); assignIt++) {
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_AssignTaskToTesters")
		    .detail("ConsistencyCheckerId", consistencyCheckerId)
		    .detail("Round", round)
		    .detail("ClientId", assignIt->first)
		    .detail("ShardsCount", assignIt->second.size());
	}
	return assignment;
}

Future<Void> runConsistencyCheckerUrgentCore(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                             Database cx,
                                             Optional<std::vector<TesterInterface>> testers,
                                             int minTestersExpected) {
	KeyRangeMap<bool> globalProgressMap; // used to keep track of progress
	std::unordered_map<int, std::vector<KeyRange>> assignment; // used to keep track of assignment of tasks
	std::vector<TesterInterface> ts; // used to store testers interface
	std::vector<KeyRange> rangesToCheck; // get from globalProgressMap
	std::vector<KeyRange> shardsToCheck; // get from keyServer metadata
	Optional<double> whenFailedToGetTesterStart;

	// Initialize globalProgressMap
	Optional<std::vector<KeyRange>> rangesToCheck_ = loadRangesToCheckFromKnob();
	if (rangesToCheck_.present()) {
		globalProgressMap.insert(allKeys, true);
		for (const auto& rangeToCheck : rangesToCheck_.get()) {
			// Mark rangesToCheck as incomplete
			// Those ranges will be checked
			globalProgressMap.insert(rangeToCheck, false);
		}
		globalProgressMap.coalesce(allKeys);
	} else {
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_FailedToLoadRangeFromKnob");
		globalProgressMap.insert(allKeys, false);
	}

	int64_t consistencyCheckerId = deterministicRandom()->randomInt64(1, 10000000);
	int retryTimes = 0;
	int round = 0;

	// Main loop
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			// Step 1: Load ranges to check, if nothing to run, exit
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RoundBegin")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("RetryTimes", retryTimes)
			    .detail("TesterCount", ts.size())
			    .detail("Round", round);
			rangesToCheck.clear();
			for (auto& range : globalProgressMap.ranges()) {
				if (!range.value()) { // range that is not finished
					rangesToCheck.push_back(range.range());
				}
			}
			if (rangesToCheck.size() == 0) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_Complete")
				    .detail("ConsistencyCheckerId", consistencyCheckerId)
				    .detail("RetryTimes", retryTimes)
				    .detail("Round", round);
				co_return;
			}

			// Step 2: Get testers
			ts.clear();
			if (!testers.present()) { // In real clusters
				bool getTesterError = false;
				Error getTesterErr;

				try {
					ts = co_await getTesters(cc, minTestersExpected);
					whenFailedToGetTesterStart.reset();
				} catch (Error& e) {
					if (e.code() == error_code_timed_out) {
						if (!whenFailedToGetTesterStart.present()) {
							whenFailedToGetTesterStart = now();
						} else if (now() - whenFailedToGetTesterStart.get() > 3600 * 24) { // 1 day
							TraceEvent(SevError, "TesterRecruitmentTimeout");
						}
					}
					getTesterErr = e;
					getTesterError = true;
				}

				if (getTesterError) {
					throw getTesterErr;
				}

				if (g_network->isSimulated() && deterministicRandom()->random01() < 0.05) {
					throw operation_failed(); // Introduce random failure
				}
			} else { // In simulation
				ts = testers.get();
			}
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_GotTesters")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("Round", round)
			    .detail("RetryTimes", retryTimes)
			    .detail("TesterCount", ts.size());

			// Step 3: Load shards to check from keyserver space
			// Shard is the unit for the task assignment
			shardsToCheck.clear();
			shardsToCheck = co_await getConsistencyCheckShards(cx, rangesToCheck);
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_GotShardsToCheck")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("Round", round)
			    .detail("RetryTimes", retryTimes)
			    .detail("TesterCount", ts.size())
			    .detail("ShardCount", shardsToCheck.size());

			// Step 4: Assign tasks to clients
			assignment.clear();
			assignment = makeTaskAssignment(cx, consistencyCheckerId, shardsToCheck, ts.size(), round);

			// Step 5: Run checking on testers
			std::unordered_set<int> completeClients =
			    co_await runUrgentConsistencyCheckWorkload(cx, ts, consistencyCheckerId, assignment);
			if (g_network->isSimulated() && deterministicRandom()->random01() < 0.05) {
				throw operation_failed(); // Introduce random failure
			}
			// We use the complete client to decide which ranges are completed
			for (const auto& clientId : completeClients) {
				for (const auto& range : assignment[clientId]) {
					globalProgressMap.insert(range, true); // Mark the ranges as complete
				}
			}
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_RoundEnd")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("RetryTimes", retryTimes)
			    .detail("SucceedTesterCount", completeClients.size())
			    .detail("SucceedTesters", describe(completeClients))
			    .detail("TesterCount", ts.size())
			    .detail("Round", round);
			round++;

		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			} else {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_CoreWithRetriableFailure")
				    .errorUnsuppressed(e)
				    .detail("ConsistencyCheckerId", consistencyCheckerId)
				    .detail("RetryTimes", retryTimes)
				    .detail("Round", round);
				caughtError = e;
				needsErrorHandling = true;
			}
		}

		if (needsErrorHandling) {
			co_await delay(10.0);
			retryTimes++;
		} else {
			co_await delay(10.0); // Backoff 10 seconds for the next round
		}

		// Decide and enforce the consistencyCheckerId for the next round
		consistencyCheckerId = deterministicRandom()->randomInt64(1, 10000000);
	}
}

Future<Void> runConsistencyCheckerUrgentHolder(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                               Database cx,
                                               Optional<std::vector<TesterInterface>> testers,
                                               int minTestersExpected,
                                               bool repeatRun) {
	while (true) {
		co_await runConsistencyCheckerUrgentCore(cc, cx, testers, minTestersExpected);
		if (!repeatRun) {
			break;
		}
		co_await delay(CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_NEXT_WAIT_TIME);
	}
	co_return;
}

Future<Void> checkConsistencyUrgentSim(Database cx, std::vector<TesterInterface> testers) {
	if (SERVER_KNOBS->DISABLE_AUDIT_STORAGE_FINAL_REPLICA_CHECK_IN_SIM) {
		return Void();
	}
	return runConsistencyCheckerUrgentHolder(
	    Reference<AsyncVar<Optional<ClusterControllerFullInterface>>>(), cx, testers, 1, /*repeatRun=*/false);
}

Future<bool> runTest(Database cx,
                     std::vector<TesterInterface> testers,
                     TestSpec spec,
                     Reference<AsyncVar<ServerDBInfo>> dbInfo,
                     TesterConsistencyScanState* consistencyScanState) {
	DistributedTestResults testResults;
	double savedDisableDuration = 0;
	bool hadException = false;
	Error caughtError;

	try {
		Future<DistributedTestResults> fTestResults = runWorkload(cx, testers, spec);
		if (g_network->isSimulated() && spec.simConnectionFailuresDisableDuration > 0) {
			savedDisableDuration = g_simulator->connectionFailuresDisableDuration;
			g_simulator->connectionFailuresDisableDuration = spec.simConnectionFailuresDisableDuration;
		}
		if (spec.timeout > 0) {
			fTestResults = timeoutError(fTestResults, spec.timeout);
		}
		DistributedTestResults _testResults = co_await fTestResults;
		printf("Test complete\n");
		testResults = _testResults;
		logMetrics(testResults.metrics);
		if (g_network->isSimulated() && savedDisableDuration > 0) {
			g_simulator->connectionFailuresDisableDuration = savedDisableDuration;
		}
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
			auto msg = fmt::format("Process timed out after {} seconds", spec.timeout);
			ProcessEvents::trigger("Timeout"_sr, StringRef(msg), e);
			TraceEvent(SevError, "TestFailure")
			    .error(e)
			    .detail("Reason", "Test timed out")
			    .detail("Timeout", spec.timeout);
			fprintf(stderr, "ERROR: Test timed out after %d seconds.\n", spec.timeout);
			testResults.failures = testers.size();
			testResults.successes = 0;
		} else {
			caughtError = e;
			hadException = true;
		}
	}

	if (hadException) {
		throw caughtError;
	}

	bool ok = testResults.ok();

	if (spec.useDB) {
		printf("%d test clients passed; %d test clients failed\n", testResults.successes, testResults.failures);
		if (spec.dumpAfterTest) {
			Error dumpErr;
			try {
				co_await timeoutError(dumpDatabase(cx, "dump after " + printable(spec.title) + ".html", allKeys), 30.0);
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to dump database");
				ok = false;
			}

			co_await delay(1.0);
		}

		// Disable consistency scan before checkConsistency because otherwise it will prevent quiet database from
		// quiescing
		co_await checkConsistencyScanAfterTest(cx, consistencyScanState);
		printf("Consistency scan done\n");

		// Run the consistency check workload
		if (spec.runConsistencyCheck) {
			bool quiescent = g_network->isSimulated() ? !BUGGIFY : spec.waitForQuiescenceEnd;
			try {
				printf("Running urgent consistency check...\n");
				// For testing urgent consistency check
				co_await timeoutError(checkConsistencyUrgentSim(cx, testers), 20000.0);
				printf("Urgent consistency check done\nRunning consistency check...\n");
				co_await timeoutError(checkConsistency(cx,
				                                       testers,
				                                       quiescent,
				                                       spec.runConsistencyCheckOnTSS,
				                                       spec.maxDDRunTime > 0 ? spec.maxDDRunTime : 10000.0,
				                                       5000,
				                                       spec.databasePingDelay,
				                                       dbInfo),
				                      20000.0);
				printf("Consistency check done\n");
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to perform consistency check");
				ok = false;
			}

			// Run auditStorage at the end of simulation
			if (quiescent && g_network->isSimulated()) {
				try {
					TraceEvent("AuditStorageStart");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateHA), 1500.0);
					TraceEvent("AuditStorageCorrectnessHADone");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateReplica), 1500.0);
					TraceEvent("AuditStorageCorrectnessReplicaDone");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateLocationMetadata), 1500.0);
					TraceEvent("AuditStorageCorrectnessLocationMetadataDone");
					co_await timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateStorageServerShard),
					                      1500.0);
					TraceEvent("AuditStorageCorrectnessStorageServerShardDone");
				} catch (Error& e) {
					ok = false;
					TraceEvent(SevError, "TestFailure")
					    .error(e)
					    .detail("Reason", "Unable to perform auditStorage check.");
				}
			}
		}
	}

	TraceEvent(ok ? SevInfo : SevWarnAlways, "TestResults").detail("Workload", spec.title).detail("Passed", (int)ok);
	//.detail("Metrics", metricSummary);

	if (ok) {
		passCount++;
	} else {
		failCount++;
	}

	printf("%d test clients passed; %d test clients failed\n", testResults.successes, testResults.failures);

	if (spec.useDB && spec.clearAfterTest) {
		Error clearErr;
		try {
			TraceEvent("TesterClearingDatabase").log();
			co_await timeoutError(clearData(cx), 1000.0);
		} catch (Error& e) {
			TraceEvent(SevError, "ErrorClearingDatabaseAfterTest").error(e);
			throw; // If we didn't do this, we don't want any later tests to run on this DB
		}

		co_await delay(1.0);
	}

	co_return ok;
}

std::map<std::string, std::function<void(const std::string&)>> testSpecGlobalKeys = {
	// These are read by SimulatedCluster and used before testers exist.  Thus, they must
	// be recognized and accepted, but there's no point in placing them into a testSpec.
	// testClass and testPriority are only used for TestHarness, we'll ignore those here
	{ "testClass", [](std::string const&) {} },
	{ "testPriority", [](std::string const&) {} },
	{ "extraDatabaseMode",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedExtraDatabaseMode", ""); } },
	{ "extraDatabaseCount",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedExtraDatabaseCount", ""); } },
	{ "configureLocked",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedConfigureLocked", ""); } },
	{ "minimumReplication",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedMinimumReplication", ""); } },
	{ "minimumRegions",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedMinimumRegions", ""); } },
	{ "logAntiQuorum",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedLogAntiQuorum", ""); } },
	{ "buggify", [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedBuggify", value); } },
	// The test harness handles NewSeverity events specially.
	{ "StderrSeverity", [](const std::string& value) { TraceEvent("StderrSeverity").detail("NewSeverity", value); } },
	{ "ClientInfoLogging",
	  [](const std::string& value) {
	      if (value == "false") {
		      setNetworkOption(FDBNetworkOptions::DISABLE_CLIENT_STATISTICS_LOGGING);
	      }
	      // else { } It is enable by default for tester
	      TraceEvent("TestParserTest").detail("ClientInfoLogging", value);
	  } },
	{ "startIncompatibleProcess",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedStartIncompatibleProcess", value); } },
	{ "storageEngineExcludeTypes",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedStorageEngineExcludeTypes", ""); } },
	{ "maxTLogVersion",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedMaxTLogVersion", ""); } },
	{ "disableTss", [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedDisableTSS", ""); } },
	{ "disableHostname",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedDisableHostname", ""); } },
	{ "disableRemoteKVS",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedRemoteKVS", ""); } },
	// TODO(gglass): remove this when it's no longer used.  For now it's a useful signal in a toml file
	// for tests against functionality that might not actually be tenant related and hence need to keep.
	{ "allowDefaultTenant",
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedDefaultTenant", ""); } }
};

std::map<std::string, std::function<void(const std::string& value, TestSpec* spec)>> testSpecTestKeys = {
	{ "testTitle",
	  [](const std::string& value, TestSpec* spec) {
	      spec->title = value;
	      TraceEvent("TestParserTest").detail("ParsedTest", spec->title);
	  } },
	{ "timeout",
	  [](const std::string& value, TestSpec* spec) {
	      sscanf(value.c_str(), "%d", &(spec->timeout));
	      ASSERT(spec->timeout > 0);
	      TraceEvent("TestParserTest").detail("ParsedTimeout", spec->timeout);
	  } },
	{ "databasePingDelay",
	  [](const std::string& value, TestSpec* spec) {
	      double databasePingDelay;
	      sscanf(value.c_str(), "%lf", &databasePingDelay);
	      ASSERT(databasePingDelay >= 0);
	      if (!spec->useDB && databasePingDelay > 0) {
		      TraceEvent(SevError, "TestParserError")
		          .detail("Reason", "Cannot have non-zero ping delay on test that does not use database")
		          .detail("PingDelay", databasePingDelay)
		          .detail("UseDB", spec->useDB);
		      ASSERT(false);
	      }
	      spec->databasePingDelay = databasePingDelay;
	      TraceEvent("TestParserTest").detail("ParsedPingDelay", spec->databasePingDelay);
	  } },
	{ "runSetup",
	  [](const std::string& value, TestSpec* spec) {
	      spec->phases = TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
	      if (value == "true")
		      spec->phases |= TestWorkload::SETUP;
	      TraceEvent("TestParserTest").detail("ParsedSetupFlag", (spec->phases & TestWorkload::SETUP) != 0);
	  } },
	{ "dumpAfterTest",
	  [](const std::string& value, TestSpec* spec) {
	      spec->dumpAfterTest = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedDumpAfter", spec->dumpAfterTest);
	  } },
	{ "clearAfterTest",
	  [](const std::string& value, TestSpec* spec) {
	      spec->clearAfterTest = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedClearAfter", spec->clearAfterTest);
	  } },
	{ "useDB",
	  [](const std::string& value, TestSpec* spec) {
	      spec->useDB = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedUseDB", spec->useDB);
	      if (!spec->useDB)
		      spec->databasePingDelay = 0.0;
	  } },
	{ "startDelay",
	  [](const std::string& value, TestSpec* spec) {
	      sscanf(value.c_str(), "%lf", &spec->startDelay);
	      TraceEvent("TestParserTest").detail("ParsedStartDelay", spec->startDelay);
	  } },
	{ "runConsistencyCheck",
	  [](const std::string& value, TestSpec* spec) {
	      spec->runConsistencyCheck = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedRunConsistencyCheck", spec->runConsistencyCheck);
	  } },
	{ "runConsistencyCheckOnTSS",
	  [](const std::string& value, TestSpec* spec) {
	      spec->runConsistencyCheckOnTSS = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedRunConsistencyCheckOnTSS", spec->runConsistencyCheckOnTSS);
	  } },
	{ "maxDDRunTime",
	  [](const std::string& value, TestSpec* spec) {
	      sscanf(value.c_str(), "%lf", &(spec->maxDDRunTime));
	      ASSERT(spec->maxDDRunTime >= 0);
	      TraceEvent("TestParserTest").detail("ParsedMaxDDRunTime", spec->maxDDRunTime);
	  } },
	{ "waitForQuiescence",
	  [](const std::string& value, TestSpec* spec) {
	      bool toWait = value == "true";
	      spec->waitForQuiescenceBegin = toWait;
	      spec->waitForQuiescenceEnd = toWait;
	      TraceEvent("TestParserTest").detail("ParsedWaitForQuiescence", toWait);
	  } },
	{ "waitForQuiescenceBegin",
	  [](const std::string& value, TestSpec* spec) {
	      bool toWait = value == "true";
	      spec->waitForQuiescenceBegin = toWait;
	      TraceEvent("TestParserTest").detail("ParsedWaitForQuiescenceBegin", toWait);
	  } },
	{ "waitForQuiescenceEnd",
	  [](const std::string& value, TestSpec* spec) {
	      bool toWait = value == "true";
	      spec->waitForQuiescenceEnd = toWait;
	      TraceEvent("TestParserTest").detail("ParsedWaitForQuiescenceEnd", toWait);
	  } },
	{ "simCheckRelocationDuration",
	  [](const std::string& value, TestSpec* spec) {
	      spec->simCheckRelocationDuration = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedSimCheckRelocationDuration", spec->simCheckRelocationDuration);
	  } },
	{ "connectionFailuresDisableDuration",
	  [](const std::string& value, TestSpec* spec) {
	      double connectionFailuresDisableDuration;
	      sscanf(value.c_str(), "%lf", &connectionFailuresDisableDuration);
	      ASSERT(connectionFailuresDisableDuration >= 0);
	      spec->simConnectionFailuresDisableDuration = connectionFailuresDisableDuration;
	      TraceEvent("TestParserTest")
	          .detail("ParsedSimConnectionFailuresDisableDuration", spec->simConnectionFailuresDisableDuration);
	  } },
	{ "simBackupAgents",
	  [](const std::string& value, TestSpec* spec) {
	      if (value == "BackupToFile" || value == "BackupToFileAndDB")
		      spec->simBackupAgents = ISimulator::BackupAgentType::BackupToFile;
	      else
		      spec->simBackupAgents = ISimulator::BackupAgentType::NoBackupAgents;
	      TraceEvent("TestParserTest").detail("ParsedSimBackupAgents", spec->simBackupAgents);

	      if (value == "BackupToDB" || value == "BackupToFileAndDB")
		      spec->simDrAgents = ISimulator::BackupAgentType::BackupToDB;
	      else
		      spec->simDrAgents = ISimulator::BackupAgentType::NoBackupAgents;
	      TraceEvent("TestParserTest").detail("ParsedSimDrAgents", spec->simDrAgents);
	  } },
	{ "checkOnly",
	  [](const std::string& value, TestSpec* spec) {
	      if (value == "true")
		      spec->phases = TestWorkload::CHECK;
	  } },
	{ "restorePerpetualWiggleSetting",
	  [](const std::string& value, TestSpec* spec) {
	      if (value == "false")
		      spec->restorePerpetualWiggleSetting = false;
	  } },
	{ "runFailureWorkloads",
	  [](const std::string& value, TestSpec* spec) { spec->runFailureWorkloads = (value == "true"); } },
	{ "disabledFailureInjectionWorkloads",
	  [](const std::string& value, TestSpec* spec) {
	      // Expects a comma separated list of workload names in "value".
	      // This custom encoding is needed because both text and toml files need to be supported
	      // and "value" is passed in as a string.
	      std::stringstream ss(value);
	      while (ss.good()) {
		      std::string substr;
		      getline(ss, substr, ',');
		      substr = removeWhitespace(substr);
		      if (!substr.empty()) {
			      spec->disabledFailureInjectionWorkloads.push_back(substr);
		      }
	      }
	  } },
};

std::vector<TestSpec> readTests(std::ifstream& ifs) {
	TestSpec spec;
	std::vector<TestSpec> result;
	Standalone<VectorRef<KeyValueRef>> workloadOptions;
	std::string cline;
	bool beforeFirstTest = true;
	bool parsingWorkloads = false;

	while (ifs.good()) {
		getline(ifs, cline);
		std::string line = removeWhitespace(cline);
		if (!line.size() || line.find(';') == 0)
			continue;

		size_t found = line.find('=');
		if (found == std::string::npos)
			// hmmm, not good
			continue;
		std::string attrib = removeWhitespace(line.substr(0, found));
		std::string value = removeWhitespace(line.substr(found + 1));

		if (attrib == "testTitle") {
			beforeFirstTest = false;
			parsingWorkloads = false;
			if (workloadOptions.size()) {
				spec.options.push_back_deep(spec.options.arena(), workloadOptions);
				workloadOptions = Standalone<VectorRef<KeyValueRef>>();
			}
			if (spec.options.size() && spec.title.size()) {
				result.push_back(spec);
				spec = TestSpec();
			}

			testSpecTestKeys[attrib](value, &spec);
		} else if (testSpecTestKeys.find(attrib) != testSpecTestKeys.end()) {
			if (parsingWorkloads)
				TraceEvent(SevError, "TestSpecTestParamInWorkload").detail("Attrib", attrib).detail("Value", value);
			testSpecTestKeys[attrib](value, &spec);
		} else if (testSpecGlobalKeys.find(attrib) != testSpecGlobalKeys.end()) {
			if (!beforeFirstTest)
				TraceEvent(SevError, "TestSpecGlobalParamInTest").detail("Attrib", attrib).detail("Value", value);
			testSpecGlobalKeys[attrib](value);
		} else {
			if (attrib == "testName") {
				parsingWorkloads = true;
				if (workloadOptions.size()) {
					TraceEvent("TestParserFlush").detail("Reason", "new (compound) test");
					spec.options.push_back_deep(spec.options.arena(), workloadOptions);
					workloadOptions = Standalone<VectorRef<KeyValueRef>>();
				}
			}

			workloadOptions.push_back_deep(workloadOptions.arena(), KeyValueRef(StringRef(attrib), StringRef(value)));
			TraceEvent("TestParserOption").detail("ParsedKey", attrib).detail("ParsedValue", value);
		}
	}
	if (workloadOptions.size())
		spec.options.push_back_deep(spec.options.arena(), workloadOptions);
	if (spec.options.size() && spec.title.size()) {
		result.push_back(spec);
	}

	return result;
}

template <typename T>
std::string toml_to_string(const T& value) {
	// TOML formatting converts numbers to strings exactly how they're in the file
	// and thus, is equivalent to testspec.  However, strings are quoted, so we
	// must remove the quotes.
	if (value.type() == toml::value_t::string) {
		const std::string& formatted = toml::format(value);
		return formatted.substr(1, formatted.size() - 2);
	} else {
		return toml::format(value);
	}
}

struct TestSet {
	KnobKeyValuePairs overrideKnobs;
	std::vector<TestSpec> testSpecs;
};

namespace {

// In the current TOML scope, look for "knobs" field. If exists, translate all
// key value pairs into KnobKeyValuePairs
KnobKeyValuePairs getOverriddenKnobKeyValues(const toml::value& context) {
	KnobKeyValuePairs result;

	try {
		const toml::array& overrideKnobs = toml::find(context, "knobs").as_array();
		for (const toml::value& knob : overrideKnobs) {
			for (const auto& [key, value_] : knob.as_table()) {
				const std::string& value = toml_to_string(value_);
				ParsedKnobValue parsedValue = CLIENT_KNOBS->parseKnobValue(key, value);
				if (std::get_if<NoKnobFound>(&parsedValue)) {
					parsedValue = SERVER_KNOBS->parseKnobValue(key, value);
				}
				if (std::get_if<NoKnobFound>(&parsedValue)) {
					parsedValue = FLOW_KNOBS->parseKnobValue(key, value);
				}
				if (std::get_if<NoKnobFound>(&parsedValue)) {
					TraceEvent(SevError, "TestSpecUnrecognizedKnob")
					    .detail("KnobName", key)
					    .detail("OverrideValue", value);
					continue;
				}
				result.set(key, parsedValue);
			}
		}
	} catch (const std::out_of_range&) {
		// No knobs field in this scope, this is not an error
	}

	return result;
}

} // namespace

TestSet readTOMLTests_(std::string fileName) {
	Standalone<VectorRef<KeyValueRef>> workloadOptions;
	TestSet result;

	const toml::value& conf = toml::parse(fileName);

	// Parse the global knob changes
	result.overrideKnobs = getOverriddenKnobKeyValues(conf);

	// Then parse each test
	const toml::array& tests = toml::find(conf, "test").as_array();
	for (const toml::value& test : tests) {
		TestSpec spec;

		// First handle all test-level settings
		for (const auto& [k, v] : test.as_table()) {
			if (k == "workload" || k == "knobs") {
				continue;
			}
			if (testSpecTestKeys.find(k) != testSpecTestKeys.end()) {
				testSpecTestKeys[k](toml_to_string(v), &spec);
			} else {
				TraceEvent(SevError, "TestSpecUnrecognizedTestParam")
				    .detail("Attrib", k)
				    .detail("Value", toml_to_string(v));
			}
		}

		// And then copy the workload attributes to spec.options
		const toml::array& workloads = toml::find(test, "workload").as_array();
		for (const toml::value& workload : workloads) {
			workloadOptions = Standalone<VectorRef<KeyValueRef>>();
			TraceEvent("TestParserFlush").detail("Reason", "new (compound) test");
			for (const auto& [attrib, v] : workload.as_table()) {
				const std::string& value = toml_to_string(v);
				workloadOptions.push_back_deep(workloadOptions.arena(),
				                               KeyValueRef(StringRef(attrib), StringRef(value)));
				TraceEvent("TestParserOption").detail("ParsedKey", attrib).detail("ParsedValue", value);
			}
			spec.options.push_back_deep(spec.options.arena(), workloadOptions);
		}

		// And then copy the knob attributes to spec.overrideKnobs
		spec.overrideKnobs = getOverriddenKnobKeyValues(test);

		result.testSpecs.push_back(spec);
	}

	return result;
}

// A hack to catch and log std::exception, because TOML11 has very useful
// error messages, but the actor framework can't handle std::exception.
TestSet readTOMLTests(std::string fileName) {
	try {
		return readTOMLTests_(fileName);
	} catch (std::exception& e) {
		std::cerr << e.what() << std::endl;
		TraceEvent("TOMLParseError").detail("Error", printable(e.what()));
		// TODO: replace with toml_parse_error();
		throw unknown_error();
	}
}

Future<Void> monitorServerDBInfo(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface,
                                 LocalityData locality,
                                 Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	// Initially most of the serverDBInfo is not known, but we know our locality right away
	ServerDBInfo localInfo;
	localInfo.myLocality = locality;
	dbInfo->set(localInfo);

	while (true) {
		GetServerDBInfoRequest req;
		req.knownServerInfoID = dbInfo->get().id;

		Future<ServerDBInfo> getInfoReq =
		    ccInterface->get().present()
		        ? brokenPromiseToNever(ccInterface->get().get().getServerDBInfo.getReply(req))
		        : Future<ServerDBInfo>(Never());
		Future<Void> ccChange = ccInterface->onChange();

		int action = 0;
		ServerDBInfo gotInfo;

		co_await Choose()
		    .When(getInfoReq,
		          [&](ServerDBInfo const& info) {
			          gotInfo = info;
			          action = 1;
		          })
		    .When(ccChange, [&](Void const&) { action = 2; })
		    .run();

		if (action == 1) {
			TraceEvent("GotServerDBInfoChange")
			    .detail("ChangeID", gotInfo.id)
			    .detail("MasterID", gotInfo.master.id())
			    .detail("RatekeeperID", gotInfo.ratekeeper.present() ? gotInfo.ratekeeper.get().id() : UID())
			    .detail("DataDistributorID",
			            gotInfo.distributor.present() ? gotInfo.distributor.get().id() : UID());

			gotInfo.myLocality = locality;
			dbInfo->set(gotInfo);
		} else if (action == 2) {
			if (ccInterface->get().present())
				TraceEvent("GotCCInterfaceChange")
				    .detail("CCID", ccInterface->get().get().id())
				    .detail("CCMachine", ccInterface->get().get().getWorkers.getEndpoint().getPrimaryAddress());
		}
	}
}

Future<Void> initializeSimConfig(Database db) {
	Transaction tr(db);
	ASSERT(g_network->isSimulated());
	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		try {
			DatabaseConfiguration dbConfig = co_await getDatabaseConfiguration(&tr);
			g_simulator->storagePolicy = dbConfig.storagePolicy;
			g_simulator->tLogPolicy = dbConfig.tLogPolicy;
			g_simulator->tLogWriteAntiQuorum = dbConfig.tLogWriteAntiQuorum;
			g_simulator->usableRegions = dbConfig.usableRegions;

			// If the same region is being shared between the remote and a satellite, then our simulated policy checking
			// may fail to account for the total number of needed machines when deciding what can be killed. To work
			// around this, we increase the required transaction logs in the remote policy to include the number of
			// satellite logs that may get recruited there
			bool foundSharedDcId = false;
			std::set<Key> dcIds;
			int maxSatelliteReplication = 0;
			for (auto const& r : dbConfig.regions) {
				if (!dcIds.insert(r.dcId).second) {
					foundSharedDcId = true;
				}
				if (!r.satellites.empty() && r.satelliteTLogReplicationFactor > 0 && r.satelliteTLogUsableDcs > 0) {
					for (auto const& s : r.satellites) {
						if (!dcIds.insert(s.dcId).second) {
							foundSharedDcId = true;
						}
					}

					maxSatelliteReplication =
					    std::max(maxSatelliteReplication, r.satelliteTLogReplicationFactor / r.satelliteTLogUsableDcs);
				}
			}

			if (foundSharedDcId) {
				int totalRequired = std::max(dbConfig.tLogReplicationFactor, dbConfig.remoteTLogReplicationFactor) +
				                    maxSatelliteReplication;
				g_simulator->remoteTLogPolicy = Reference<IReplicationPolicy>(
				    new PolicyAcross(totalRequired, "zoneid", Reference<IReplicationPolicy>(new PolicyOne())));
				TraceEvent("ChangingSimTLogPolicyForSharedRemote")
				    .detail("TotalRequired", totalRequired)
				    .detail("MaxSatelliteReplication", maxSatelliteReplication)
				    .detail("ActualPolicy", dbConfig.getRemoteTLogPolicy()->info())
				    .detail("SimulatorPolicy", g_simulator->remoteTLogPolicy->info());
			} else {
				g_simulator->remoteTLogPolicy = dbConfig.getRemoteTLogPolicy();
			}

			co_return;
		} catch (Error& e) {
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			co_await tr.onError(caughtError);
		}
	}
}

// Disables connection failures after the given time seconds
Future<Void> disableConnectionFailuresAfter(double seconds, std::string context) {
	if (g_network->isSimulated()) {
		TraceEvent(SevWarnAlways, ("ScheduleDisableConnectionFailures_" + context).c_str())
		    .detail("At", now() + seconds);
		co_await delay(seconds);
		while (true) {
			double delaySeconds = disableConnectionFailures(context, ForceDisable::False);
			if (delaySeconds > DISABLE_CONNECTION_FAILURE_MIN_INTERVAL) {
				co_await delay(delaySeconds);
			} else {
				// disableConnectionFailures will always take effect if less than
				// DISABLE_CONNECTION_FAILURE_MIN_INTERVAL is returned.
				break;
			}
		}
	}
	co_return;
}

/**
 * \brief Test orchestrator: sends test specification to testers in the right order and collects the results.
 *
 * There are multiple actors in this file with similar names (runTest, runTests) and slightly different signatures.
 *
 * This is the actual orchestrator. It reads the test specifications (from tests), prepares the cluster (by running the
 * configure command given in startingConfiguration) and then runs the workload.
 *
 * \param cc The cluster controller interface
 * \param ci Same as cc.clientInterface
 * \param testers The interfaces of the testers that should run the actual workloads
 * \param tests The test specifications to run
 * \param startingConfiguration If non-empty, the orchestrator will attempt to set this configuration before starting
 * the tests.
 * \param locality client locality (it seems this is unused?)
 *
 * \returns A future which will be set after all tests finished.
 */
Future<Void> runTests(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> cc,
                      Reference<AsyncVar<Optional<struct ClusterInterface>>> ci,
                      std::vector<TesterInterface> testers,
                      std::vector<TestSpec> tests,
                      StringRef startingConfiguration,
                      LocalityData locality,
                      bool restartingTest) {
	Database cx;
	Reference<AsyncVar<ServerDBInfo>> dbInfo(new AsyncVar<ServerDBInfo>);
	Future<Void> ccMonitor = monitorServerDBInfo(cc, LocalityData(), dbInfo); // FIXME: locality

	bool useDB = false;
	bool waitForQuiescenceBegin = false;
	bool waitForQuiescenceEnd = false;
	bool restorePerpetualWiggleSetting = false;
	bool perpetualWiggleEnabled = false;
	bool backupWorkerEnabled = false;
	double startDelay = 0.0;
	double databasePingDelay = 1e9;
	ISimulator::BackupAgentType simBackupAgents = ISimulator::BackupAgentType::NoBackupAgents;
	ISimulator::BackupAgentType simDrAgents = ISimulator::BackupAgentType::NoBackupAgents;
	bool enableDD = false;
	TesterConsistencyScanState consistencyScanState;

	// Gives chance for g_network->run() to run inside event loop and hence let
	// the tests see correct value for `isOnMainThread()`.
	co_await yield();

	if (tests.empty())
		useDB = true;
	for (auto iter = tests.begin(); iter != tests.end(); ++iter) {
		if (iter->useDB)
			useDB = true;
		if (iter->waitForQuiescenceBegin)
			waitForQuiescenceBegin = true;
		if (iter->waitForQuiescenceEnd)
			waitForQuiescenceEnd = true;
		if (iter->restorePerpetualWiggleSetting)
			restorePerpetualWiggleSetting = true;
		startDelay = std::max(startDelay, iter->startDelay);
		databasePingDelay = std::min(databasePingDelay, iter->databasePingDelay);
		if (iter->simBackupAgents != ISimulator::BackupAgentType::NoBackupAgents)
			simBackupAgents = iter->simBackupAgents;

		if (iter->simDrAgents != ISimulator::BackupAgentType::NoBackupAgents) {
			simDrAgents = iter->simDrAgents;
		}
		enableDD = enableDD || getOption(iter->options[0], "enableDD"_sr, false);
	}

	if (g_network->isSimulated()) {
		g_simulator->backupAgents = simBackupAgents;
		g_simulator->drAgents = simDrAgents;
	}

	// turn off the database ping functionality if the suite of tests are not going to be using the database
	if (!useDB)
		databasePingDelay = 0.0;

	if (useDB) {
		cx = openDBOnServer(dbInfo);
	}

	consistencyScanState.enabled = g_network->isSimulated() && deterministicRandom()->coinflip();
	consistencyScanState.waitForComplete =
	    consistencyScanState.enabled && waitForQuiescenceEnd && deterministicRandom()->coinflip();
	consistencyScanState.enableAfter = consistencyScanState.waitForComplete && deterministicRandom()->random01() < 0.1;

	disableConnectionFailures("Tester");

	// Change the configuration (and/or create the database) if necessary
	printf("startingConfiguration:%s start\n", startingConfiguration.toString().c_str());
	fmt::print("useDB: {}\n", useDB);
	printSimulatedTopology();
	if (useDB && startingConfiguration != StringRef()) {
		Error configErr;
		try {
			co_await timeoutError(changeConfiguration(cx, testers, startingConfiguration), 2000.0);
			if (g_network->isSimulated() && enableDD) {
				co_await success(setDDMode(cx, 1));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to set starting configuration");
		}
		std::string_view confView(reinterpret_cast<const char*>(startingConfiguration.begin()),
		                          startingConfiguration.size());
		if (restorePerpetualWiggleSetting) {
			const std::string setting = "perpetual_storage_wiggle:=";
			auto pos = confView.find(setting);
			if (pos != confView.npos && confView.at(pos + setting.size()) == '1') {
				perpetualWiggleEnabled = true;
			}
		}
		const std::string bwSetting = "backup_worker_enabled:=";
		auto pos = confView.find(bwSetting);
		if (pos != confView.npos && confView.at(pos + bwSetting.size()) == '1') {
			backupWorkerEnabled = true;
		}
	}

	// Read cluster configuration
	if (useDB && g_network->isSimulated()) {
		DatabaseConfiguration configuration = co_await getDatabaseConfiguration(cx);

		g_simulator->storagePolicy = configuration.storagePolicy;
		g_simulator->tLogPolicy = configuration.tLogPolicy;
		g_simulator->tLogWriteAntiQuorum = configuration.tLogWriteAntiQuorum;
		g_simulator->remoteTLogPolicy = configuration.remoteTLogPolicy;
		g_simulator->usableRegions = configuration.usableRegions;
		if (configuration.regions.size() > 0) {
			g_simulator->primaryDcId = configuration.regions[0].dcId;
			g_simulator->hasSatelliteReplication = configuration.regions[0].satelliteTLogReplicationFactor > 0;
			if (configuration.regions[0].satelliteTLogUsableDcsFallback > 0) {
				g_simulator->satelliteTLogPolicyFallback = configuration.regions[0].satelliteTLogPolicyFallback;
				g_simulator->satelliteTLogWriteAntiQuorumFallback =
				    configuration.regions[0].satelliteTLogWriteAntiQuorumFallback;
			} else {
				g_simulator->satelliteTLogPolicyFallback = configuration.regions[0].satelliteTLogPolicy;
				g_simulator->satelliteTLogWriteAntiQuorumFallback =
				    configuration.regions[0].satelliteTLogWriteAntiQuorum;
			}
			g_simulator->satelliteTLogPolicy = configuration.regions[0].satelliteTLogPolicy;
			g_simulator->satelliteTLogWriteAntiQuorum = configuration.regions[0].satelliteTLogWriteAntiQuorum;

			for (auto s : configuration.regions[0].satellites) {
				g_simulator->primarySatelliteDcIds.push_back(s.dcId);
			}
		} else {
			g_simulator->hasSatelliteReplication = false;
			g_simulator->satelliteTLogWriteAntiQuorum = 0;
		}

		if (configuration.regions.size() == 2) {
			g_simulator->remoteDcId = configuration.regions[1].dcId;
			ASSERT((!configuration.regions[0].satelliteTLogPolicy && !configuration.regions[1].satelliteTLogPolicy) ||
			       configuration.regions[0].satelliteTLogPolicy->info() ==
			           configuration.regions[1].satelliteTLogPolicy->info());

			for (auto s : configuration.regions[1].satellites) {
				g_simulator->remoteSatelliteDcIds.push_back(s.dcId);
			}
		}

		if (restartingTest || g_simulator->usableRegions < 2 || !g_simulator->hasSatelliteReplication) {
			g_simulator->allowLogSetKills = false;
		}

		ASSERT(g_simulator->storagePolicy && g_simulator->tLogPolicy);
		ASSERT(!g_simulator->hasSatelliteReplication || g_simulator->satelliteTLogPolicy);

		// Randomly inject custom shard configuration
		// TODO:  Move this to a workload representing non-failure behaviors which can be randomly added to any test
		// run.
		if (deterministicRandom()->random01() < 0.25) {
			co_await customShardConfigWorkload(cx);
		}
	}

	if (useDB) {
		if (g_network->isSimulated()) {
			co_await initializeSimConfig(cx);
		}
	}

	if (useDB && waitForQuiescenceBegin) {
		TraceEvent("TesterStartingPreTestChecks")
		    .detail("DatabasePingDelay", databasePingDelay)
		    .detail("StartDelay", startDelay);

		Error quietDbErr;
		try {
			co_await (quietDatabase(cx, dbInfo, "Start") ||
			          (databasePingDelay == 0.0
			               ? Never()
			               : testDatabaseLiveness(cx, databasePingDelay, "QuietDatabaseStart", startDelay)));
		} catch (Error& e) {
			TraceEvent("QuietDatabaseStartExternalError").error(e);
			throw;
		}

		if (perpetualWiggleEnabled) { // restore the enabled perpetual storage wiggle setting
			printf("Set perpetual_storage_wiggle=1 ...\n");
			Version cVer = co_await setPerpetualStorageWiggle(cx, true, LockAware::True);
			(void)cVer;
			printf("Set perpetual_storage_wiggle=1 Done.\n");
		}

		if (backupWorkerEnabled) {
			printf("Enabling backup worker ...\n");
			co_await enableBackupWorker(cx);
			printf("Enabled backup worker.\n");
		}

		// TODO: Move this to a BehaviorInjection workload once that concept exists.
		if (consistencyScanState.enabled && !consistencyScanState.enableAfter) {
			printf("Enabling consistency scan ...\n");
			co_await enableConsistencyScanInSim(cx);
			printf("Enabled consistency scan.\n");
		}
	}

	// Use the first test's connectionFailuresDisableDuration if set
	double connectionFailuresDisableDuration = 0.0;
	if (!tests.empty() && tests[0].simConnectionFailuresDisableDuration > 0) {
		connectionFailuresDisableDuration = tests[0].simConnectionFailuresDisableDuration;
	}

	// Must be at function scope so the background actor isn't cancelled when the else block exits.
	// In the original ACTOR code, `state` hoists variables to actor scope regardless of block scope.
	Future<Void> disabler;
	if (connectionFailuresDisableDuration > 0) {
		// Disable connection failures with specified duration
		disableConnectionFailures("Tester", ForceDisable::True, connectionFailuresDisableDuration);
	} else {
		enableConnectionFailures("Tester", FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS);
		disabler = disableConnectionFailuresAfter(FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, "Tester");
	}
	Future<Void> repairDataCenter;
	if (useDB) {
		// Keep datacenter repair at the default duration regardless of connection failures setting
		Future<Void> reconfigure = reconfigureAfter(cx, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, dbInfo, "Tester");
		repairDataCenter = reconfigure;
	}

	TraceEvent("TestsExpectedToPass").detail("Count", tests.size());
	int idx = 0;
	std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup;
	for (; idx < tests.size(); idx++) {
		printf("Run test:%s start\n", tests[idx].title.toString().c_str());
		knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(tests[idx].overrideKnobs);
		co_await success(runTest(cx, testers, tests[idx], dbInfo, &consistencyScanState));
		knobProtectiveGroup.reset(nullptr);
		printf("Run test:%s Done.\n", tests[idx].title.toString().c_str());
		// do we handle a failure here?
	}

	printf("\n%d tests passed; %d tests failed.\n", passCount, failCount);

	// If the database was deleted during the workload we need to recreate the database
	if (tests.empty() || useDB) {
		if (waitForQuiescenceEnd) {
			printf("Waiting for DD to end...\n");
			TraceEvent("QuietDatabaseEndStart");
			try {
				TraceEvent("QuietDatabaseEndWait");
				Future<Void> waitConsistencyScanEnd = checkConsistencyScanAfterTest(cx, &consistencyScanState);
				Future<Void> waitQuietDatabaseEnd =
				    quietDatabase(cx, dbInfo, "End", 0, 2e6, 2e6) ||
				    (databasePingDelay == 0.0 ? Never()
				                              : testDatabaseLiveness(cx, databasePingDelay, "QuietDatabaseEnd"));

				co_await (waitConsistencyScanEnd && waitQuietDatabaseEnd);
			} catch (Error& e) {
				TraceEvent("QuietDatabaseEndExternalError").error(e);
				throw;
			}
		}
	}
	printf("\n");

	co_return;
}

/**
 * \brief Proxy function that waits until enough testers are available and then calls into the orchestrator.
 *
 * There are multiple actors in this file with similar names (runTest, runTests) and slightly different signatures.
 *
 * This actor wraps the actual orchestrator (also called runTests). But before calling that actor, it waits for enough
 * testers to come up.
 *
 * \param cc The cluster controller interface
 * \param ci Same as cc.clientInterface
 * \param tests The test specifications to run
 * \param minTestersExpected The number of testers to expect. This actor will block until it can find this many testers.
 * \param startingConfiguration If non-empty, the orchestrator will attempt to set this configuration before starting
 * the tests.
 * \param locality client locality (it seems this is unused?)
 *
 * \returns A future which will be set after all tests finished.
 */
Future<Void> runTests(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> cc,
                      Reference<AsyncVar<Optional<struct ClusterInterface>>> ci,
                      std::vector<TestSpec> tests,
                      test_location_t at,
                      int minTestersExpected,
                      StringRef startingConfiguration,
                      LocalityData locality,
                      bool restartingTest) {
	int flags = (at == TEST_ON_SERVERS ? 0 : GetWorkersRequest::TESTER_CLASS_ONLY) |
	            GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY;
	Future<Void> testerTimeout = delay(600.0); // wait 600 sec for testers to show up
	std::vector<WorkerDetails> workers;

	while (true) {
		Future<std::vector<WorkerDetails>> getWorkersReq =
		    cc->get().present()
		        ? brokenPromiseToNever(cc->get().get().getWorkers.getReply(GetWorkersRequest(flags)))
		        : Future<std::vector<WorkerDetails>>(Never());
		Future<Void> ccChange = cc->onChange();

		int action = 0;
		std::vector<WorkerDetails> gotWorkers;

		co_await Choose()
		    .When(getWorkersReq,
		          [&](std::vector<WorkerDetails> const& w) {
			          gotWorkers = w;
			          action = 1;
		          })
		    .When(ccChange, [&](Void const&) { action = 2; })
		    .When(testerTimeout, [&](Void const&) { action = 3; })
		    .run();

		if (action == 1) {
			if (static_cast<int>(gotWorkers.size()) >= minTestersExpected) {
				workers = gotWorkers;
				break;
			}
			co_await delay(SERVER_KNOBS->WORKER_POLL_DELAY);
		} else if (action == 3) {
			TraceEvent(SevError, "TesterRecruitmentTimeout").log();
			throw timed_out();
		}
		// action == 2: cc changed, loop again
	}

	std::vector<TesterInterface> ts;
	ts.reserve(workers.size());
	for (int i = 0; i < workers.size(); i++)
		ts.push_back(workers[i].interf.testerInterface);

	co_await runTests(cc, ci, ts, tests, startingConfiguration, locality, restartingTest);
	co_return;
}

/**
 * \brief Set up testing environment and run the given tests on a cluster.
 *
 * There are multiple actors in this file with similar names (runTest, runTests) and slightly different signatures.
 *
 * This actor is usually the first entry point into the test environment. It itself doesn't implement too much
 * functionality. Its main purpose is to generate the test specification from passed arguments and then call into the
 * correct actor which will orchestrate the actual test.
 *
 * \param connRecord A cluster connection record. Not all tests require a functional cluster but all tests require
 * a cluster record.
 * \param whatToRun TEST_TYPE_FROM_FILE to read the test description from a passed toml file or
 * TEST_TYPE_CONSISTENCY_CHECK to generate a test spec for consistency checking
 * \param at TEST_HERE: this process will act as a test client and execute the given workload. TEST_ON_SERVERS: Run a
 * test client on every worker in the cluster. TEST_ON_TESTERS: Run a test client on all servers with class Test
 * \param minTestersExpected In at is not TEST_HERE, this will instruct the orchestrator until it can find at least
 * minTestersExpected test-clients. This is usually passed through from a command line argument. In simulation, the
 * simulator will pass the number of testers that it started.
 * \param fileName The path to the toml-file containing the test description. Is ignored if whatToRun !=
 * TEST_TYPE_FROM_FILE
 * \param startingConfiguration Can be used to configure a cluster before running the test. If this is an empty string,
 * it will be ignored, otherwise it will be passed to changeConfiguration.
 * \param locality The client locality to be used. This is only used if at == TEST_HERE
 *
 * \returns A future which will be set after all tests finished.
 */
Future<Void> runTests(Reference<IClusterConnectionRecord> const& connRecord,
                      test_type_t const& whatToRun,
                      test_location_t const& at,
                      int const& minTestersExpected,
                      std::string const& fileName,
                      StringRef const& startingConfiguration,
                      LocalityData const& locality,
                      UnitTestParameters const& testOptions,
                      bool const& restartingTest) {
	// C++20 coroutine safety: copy parameters that might bind to temporaries (default args).
	// const& parameters only store the reference in the coroutine frame; temporaries are
	// destroyed after the first suspend point, leaving dangling references.
	Reference<IClusterConnectionRecord> connRecordCopy = connRecord;
	test_type_t whatToRunCopy = whatToRun;
	test_location_t atCopy = at;
	int minTestersExpectedCopy = minTestersExpected;
	std::string fileNameCopy = fileName;
	StringRef startingConfigurationCopy = startingConfiguration;
	LocalityData localityCopy = locality;
	UnitTestParameters testOptionsCopy = testOptions;
	bool restartingTestCopy = restartingTest;

	TestSet testSet;
	std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup(nullptr);
	auto cc = makeReference<AsyncVar<Optional<ClusterControllerFullInterface>>>();
	auto ci = makeReference<AsyncVar<Optional<ClusterInterface>>>();
	std::vector<Future<Void>> actors;
	if (connRecordCopy) {
		actors.push_back(reportErrors(monitorLeader(connRecordCopy, cc), "MonitorLeader"));
		actors.push_back(reportErrors(extractClusterInterface(cc, ci), "ExtractClusterInterface"));
	}

	if (whatToRunCopy == TEST_TYPE_CONSISTENCY_CHECK_URGENT) {
		// Need not to set spec here. Will set spec when triggering workload
	} else if (whatToRunCopy == TEST_TYPE_CONSISTENCY_CHECK) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = "ConsistencyCheck"_sr;
		spec.runFailureWorkloads = false;
		spec.databasePingDelay = 0;
		spec.timeout = 0;
		spec.waitForQuiescenceBegin = false;
		spec.waitForQuiescenceEnd = false;
		std::string rateLimitMax = format("%d", CLIENT_KNOBS->CONSISTENCY_CHECK_RATE_LIMIT_MAX);
		options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ConsistencyCheck"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("performQuiescentChecks"_sr, "false"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("distributed"_sr, "false"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("failureIsError"_sr, "true"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("indefinite"_sr, "true"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("rateLimitMax"_sr, StringRef(rateLimitMax)));
		options.push_back_deep(options.arena(), KeyValueRef("shuffleShards"_sr, "true"_sr));
		spec.options.push_back_deep(spec.options.arena(), options);
		testSet.testSpecs.push_back(spec);
	} else if (whatToRunCopy == TEST_TYPE_UNIT_TESTS) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = "UnitTests"_sr;
		spec.startDelay = 0;
		spec.useDB = false;
		spec.timeout = 0;
		options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "UnitTests"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("testsMatching"_sr, fileNameCopy));
		// Add unit test options as test spec options
		for (auto& kv : testOptionsCopy.params) {
			options.push_back_deep(options.arena(), KeyValueRef(kv.first, kv.second));
		}
		spec.options.push_back_deep(spec.options.arena(), options);
		testSet.testSpecs.push_back(spec);
	} else {
		std::ifstream ifs;
		ifs.open(fileNameCopy.c_str(), std::ifstream::in);
		if (!ifs.good()) {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "file open failed")
			    .detail("File", fileNameCopy.c_str());
			fprintf(stderr, "ERROR: Could not open file `%s'\n", fileNameCopy.c_str());
			co_return;
		}
		enableClientInfoLogging(); // Enable Client Info logging by default for tester
		if (fileNameCopy.ends_with(".txt")) {
			testSet.testSpecs = readTests(ifs);
		} else if (fileNameCopy.ends_with(".toml")) {
			// TOML is weird about opening the file as binary on windows, so we
			// just let TOML re-open the file instead of using ifs.
			testSet = readTOMLTests(fileNameCopy);
		} else {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "unknown tests specification extension")
			    .detail("File", fileNameCopy.c_str());
			co_return;
		}
		ifs.close();
	}

	knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(testSet.overrideKnobs);
	Future<Void> tests;
	// These must be at function scope so they aren't destroyed when the if-block exits.
	// In the original ACTOR code, `state` hoists variables to actor scope regardless of block scope.
	Database urgentCx;
	Reference<AsyncVar<ServerDBInfo>> urgentDbInfo;
	Future<Void> urgentCcMonitor;
	if (whatToRunCopy == TEST_TYPE_CONSISTENCY_CHECK_URGENT) {
		urgentDbInfo = makeReference<AsyncVar<ServerDBInfo>>();
		urgentCcMonitor = monitorServerDBInfo(cc, LocalityData(), urgentDbInfo); // FIXME: locality
		urgentCx = openDBOnServer(urgentDbInfo);
		tests =
		    reportErrors(runConsistencyCheckerUrgentHolder(
		                     cc, urgentCx, Optional<std::vector<TesterInterface>>(), minTestersExpectedCopy, /*repeatRun=*/true),
		                 "runConsistencyCheckerUrgentHolder");
	} else if (atCopy == TEST_HERE) {
		auto db = makeReference<AsyncVar<ServerDBInfo>>();
		std::vector<TesterInterface> iTesters(1);
		actors.push_back(
		    reportErrors(monitorServerDBInfo(cc, LocalityData(), db), "MonitorServerDBInfo")); // FIXME: Locality
		actors.push_back(
		    reportErrors(testerServerCore(iTesters[0], connRecordCopy, db, localityCopy), "TesterServerCore"));
		tests = runTests(
		    cc, ci, iTesters, testSet.testSpecs, startingConfigurationCopy, localityCopy, restartingTestCopy);
	} else {
		tests = reportErrors(
		    runTests(cc,
		             ci,
		             testSet.testSpecs,
		             atCopy,
		             minTestersExpectedCopy,
		             startingConfigurationCopy,
		             localityCopy,
		             restartingTestCopy),
		    "RunTests");
	}

	Future<Void> actorsFuture = actors.empty() ? Never() : waitForAll(actors);
	co_await Choose()
	    .When(tests, [](Void const&) {})
	    .When(actorsFuture,
	          [](Void const&) {
		          ASSERT(false);
		          throw internal_error();
	          })
	    .run();
}

namespace {
Future<Void> testExpectedErrorImpl(Future<Void> test,
                                   const char* testDescr,
                                   Optional<Error> expectedError,
                                   Optional<bool*> successFlag,
                                   std::map<std::string, std::string> details,
                                   Optional<Error> throwOnError,
                                   UID id) {
	Error actualError;
	bool hadError = false;

	try {
		co_await test;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		actualError = e;
		hadError = true;
		// The test failed as expected
		if (!expectedError.present() || actualError.code() == expectedError.get().code()) {
			co_return;
		}
	}

	// The test has failed
	if (successFlag.present()) {
		*(successFlag.get()) = false;
	}
	TraceEvent evt(SevError, "TestErrorFailed", id);
	evt.detail("TestDescription", testDescr);
	if (expectedError.present()) {
		evt.detail("ExpectedError", expectedError.get().name());
		evt.detail("ExpectedErrorCode", expectedError.get().code());
	}
	if (hadError && actualError.isValid()) {
		evt.detail("ActualError", actualError.name());
		evt.detail("ActualErrorCode", actualError.code());
	} else {
		evt.detail("Reason", "Unexpected success");
	}

	// Make sure that no duplicate details were provided
	ASSERT(!details.contains("TestDescription"));
	ASSERT(!details.contains("ExpectedError"));
	ASSERT(!details.contains("ExpectedErrorCode"));
	ASSERT(!details.contains("ActualError"));
	ASSERT(!details.contains("ActualErrorCode"));
	ASSERT(!details.contains("Reason"));

	for (auto& p : details) {
		evt.detail(p.first.c_str(), p.second);
	}
	if (throwOnError.present()) {
		throw throwOnError.get();
	}
	co_return;
}
} // namespace

Future<Void> testExpectedError(Future<Void> test,
                               const char* testDescr,
                               Optional<Error> expectedError,
                               Optional<bool*> successFlag,
                               std::map<std::string, std::string> details,
                               Optional<Error> throwOnError,
                               UID id) {
	return testExpectedErrorImpl(test, testDescr, expectedError, successFlag, details, throwOnError, id);
}
