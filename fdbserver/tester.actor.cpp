/*
 * tester.actor.cpp
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
#include "fdbclient/TenantManagement.actor.h"
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

#include "flow/actorcompiler.h" // This must be the last #include.

WorkloadContext::WorkloadContext() {}

WorkloadContext::WorkloadContext(const WorkloadContext& r)
  : options(r.options), clientId(r.clientId), clientCount(r.clientCount), sharedRandomNumber(r.sharedRandomNumber),
    dbInfo(r.dbInfo), ccr(r.ccr), defaultTenant(r.defaultTenant), rangesToCheck(r.rangesToCheck) {}

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

ACTOR Future<Void> poisson(double* last, double meanInterval) {
	*last += meanInterval * -log(deterministicRandom()->random01());
	wait(delayUntil(*last));
	return Void();
}

ACTOR Future<Void> uniform(double* last, double meanInterval) {
	*last += meanInterval;
	wait(delayUntil(*last));
	return Void();
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

ACTOR Future<std::vector<PerfMetric>> getMetricsCompoundWorkload(CompoundWorkload* self) {
	state std::vector<Future<std::vector<PerfMetric>>> results;
	for (int w = 0; w < self->workloads.size(); w++) {
		std::vector<PerfMetric> p;
		results.push_back(self->workloads[w]->getMetrics());
	}
	wait(waitForAll(results));
	std::vector<PerfMetric> res;
	for (int i = 0; i < results.size(); ++i) {
		auto const& p = results[i].get();
		for (auto const& m : p) {
			res.push_back(m.withPrefix(self->workloads[i]->description() + "."));
		}
	}
	return res;
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

ACTOR Future<Reference<TestWorkload>> getWorkloadIface(WorkloadRequest work,
                                                       Reference<IClusterConnectionRecord> ccr,
                                                       VectorRef<KeyValueRef> options,
                                                       Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state Reference<TestWorkload> workload;
	state Value testName = getOption(options, "testName"_sr, "no-test-specified"_sr);
	WorkloadContext wcx;
	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.ccr = ccr;
	wcx.dbInfo = dbInfo;
	wcx.options = options;
	wcx.sharedRandomNumber = work.sharedRandomNumber;
	wcx.defaultTenant = work.defaultTenant.castTo<TenantName>();
	wcx.rangesToCheck = work.rangesToCheck;

	workload = IWorkloadFactory::create(testName.toString(), wcx);
	if (workload) {
		wait(workload->initialized());
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
	return workload;
}

ACTOR Future<Reference<TestWorkload>> getWorkloadIface(WorkloadRequest work,
                                                       Reference<IClusterConnectionRecord> ccr,
                                                       Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state WorkloadContext wcx;
	state std::vector<Future<Reference<TestWorkload>>> ifaces;
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
	wcx.defaultTenant = work.defaultTenant.castTo<TenantName>();
	wcx.rangesToCheck = work.rangesToCheck;
	// FIXME: Other stuff not filled in; why isn't this constructed here and passed down to the other
	// getWorkloadIface()?
	for (int i = 0; i < work.options.size(); i++) {
		ifaces.push_back(getWorkloadIface(work, ccr, work.options[i], dbInfo));
	}
	wait(waitForAll(ifaces));
	auto compound = makeReference<CompoundWorkload>(wcx);
	for (int i = 0; i < work.options.size(); i++) {
		compound->add(ifaces[i].getValue());
	}
	compound->addFailureInjection(work);
	return compound;
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

ACTOR Future<Void> databaseWarmer(Database cx) {
	loop {
		state Transaction tr(cx);
		wait(success(tr.getReadVersion()));
		wait(delay(0.25));
	}
}

// Tries indefinitely to commit a simple, self conflicting transaction
ACTOR Future<Void> pingDatabase(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> v =
			    wait(tr.get(StringRef("/Liveness/" + deterministicRandom()->randomUniqueID().toString())));
			tr.makeSelfConflicting();
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			TraceEvent("PingingDatabaseTransactionError").error(e);
			wait(tr.onError(e));
		}
	}
}

ACTOR Future<Void> testDatabaseLiveness(Database cx,
                                        double databasePingDelay,
                                        std::string context,
                                        double startDelay = 0.0) {
	wait(delay(startDelay));
	loop {
		try {
			state double start = now();
			auto traceMsg = "PingingDatabaseLiveness_" + context;
			TraceEvent(traceMsg.c_str()).log();
			wait(timeoutError(pingDatabase(cx), databasePingDelay));
			double pingTime = now() - start;
			ASSERT(pingTime > 0);
			TraceEvent(("PingingDatabaseLivenessDone_" + context).c_str()).detail("TimeTaken", pingTime);
			wait(delay(databasePingDelay - pingTime));
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

ACTOR Future<Reference<TestWorkload>> getConsistencyCheckUrgentWorkloadIface(
    WorkloadRequest work,
    Reference<IClusterConnectionRecord> ccr,
    Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state WorkloadContext wcx;
	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.sharedRandomNumber = work.sharedRandomNumber;
	wcx.ccr = ccr;
	wcx.dbInfo = dbInfo;
	wcx.defaultTenant = work.defaultTenant.castTo<TenantName>();
	wcx.rangesToCheck = work.rangesToCheck;
	Reference<TestWorkload> iface = wait(getWorkloadIface(work, ccr, work.options[0], dbInfo));
	return iface;
}

ACTOR Future<Void> runConsistencyCheckUrgentWorkloadAsync(Database cx,
                                                          WorkloadInterface workIface,
                                                          Reference<TestWorkload> workload) {
	state ReplyPromise<Void> jobReq;
	loop choose {
		when(ReplyPromise<Void> req = waitNext(workIface.start.getFuture())) {
			jobReq = req;
			try {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadReceived", workIface.id())
				    .detail("WorkloadName", workload->description())
				    .detail("ClientCount", workload->clientCount)
				    .detail("ClientId", workload->clientId);
				wait(workload->start(cx));
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
			break;
		}
	}
	return Void();
}

ACTOR Future<Void> testerServerConsistencyCheckerUrgentWorkload(WorkloadRequest work,
                                                                Reference<IClusterConnectionRecord> ccr,
                                                                Reference<AsyncVar<struct ServerDBInfo> const> dbInfo) {
	state WorkloadInterface workIface;
	state bool replied = false;
	try {
		state Database cx = openDBOnServer(dbInfo);
		wait(delay(1.0));
		Reference<TestWorkload> workload = wait(getConsistencyCheckUrgentWorkloadIface(work, ccr, dbInfo));
		Future<Void> test = runConsistencyCheckUrgentWorkloadAsync(cx, workIface, workload);
		work.reply.send(workIface);
		replied = true;
		wait(test);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterRunWorkloadFailed").errorUnsuppressed(e);
		if (!replied) {
			work.reply.sendError(e);
		}
	}
	return Void();
}

ACTOR Future<Void> runWorkloadAsync(Database cx,
                                    WorkloadInterface workIface,
                                    Reference<TestWorkload> workload,
                                    double databasePingDelay) {
	state Optional<ErrorOr<Void>> setupResult;
	state Optional<ErrorOr<Void>> startResult;
	state Optional<ErrorOr<CheckReply>> checkResult;
	state ReplyPromise<Void> setupReq;
	state ReplyPromise<Void> startReq;
	state ReplyPromise<CheckReply> checkReq;

	TraceEvent("TestBeginAsync", workIface.id())
	    .detail("Workload", workload->description())
	    .detail("DatabasePingDelay", databasePingDelay);

	state Future<Void> databaseError =
	    databasePingDelay == 0.0 ? Never() : testDatabaseLiveness(cx, databasePingDelay, "RunWorkloadAsync");

	loop choose {
		when(ReplyPromise<Void> req = waitNext(workIface.setup.getFuture())) {
			printf("Test received trigger for setup...\n");
			TraceEvent("TestSetupBeginning", workIface.id()).detail("Workload", workload->description());
			setupReq = req;
			if (!setupResult.present()) {
				try {
					wait(workload->setup(cx) || databaseError);
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
		}
		when(ReplyPromise<Void> req = waitNext(workIface.start.getFuture())) {
			startReq = req;
			if (!startResult.present()) {
				try {
					TraceEvent("TestStarting", workIface.id()).detail("Workload", workload->description());
					wait(workload->start(cx) || databaseError);
					startResult = Void();
				} catch (Error& e) {
					startResult = operation_failed();
					if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
						throw;
					TraceEvent(SevError, "TestFailure", workIface.id())
					    .errorUnsuppressed(e)
					    .detail("Reason", "Error starting workload")
					    .detail("Workload", workload->description());
					// ok = false;
				}
				TraceEvent("TestComplete", workIface.id())
				    .detail("Workload", workload->description())
				    .detail("OK", !startResult.get().isError());
				printf("%s complete\n", workload->description().c_str());
			}
			sendResult(startReq, startResult);
		}
		when(ReplyPromise<CheckReply> req = waitNext(workIface.check.getFuture())) {
			checkReq = req;
			if (!checkResult.present()) {
				try {
					TraceEvent("TestChecking", workIface.id()).detail("Workload", workload->description());
					bool check = wait(timeoutError(workload->check(cx), workload->getCheckTimeout()));
					checkResult = CheckReply{ (!startResult.present() || !startResult.get().isError()) && check };
				} catch (Error& e) {
					checkResult = operation_failed(); // was: checkResult = false;
					if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
						throw;
					TraceEvent(SevError, "TestFailure", workIface.id())
					    .error(e)
					    .detail("Reason", "Error checking workload")
					    .detail("Workload", workload->description());
					// ok = false;
				}
				TraceEvent("TestCheckComplete", workIface.id()).detail("Workload", workload->description());
			}

			sendResult(checkReq, checkResult);
		}
		when(ReplyPromise<std::vector<PerfMetric>> req = waitNext(workIface.metrics.getFuture())) {
			state ReplyPromise<std::vector<PerfMetric>> s_req = req;
			try {
				std::vector<PerfMetric> m = wait(workload->getMetrics());
				TraceEvent("WorkloadSendMetrics", workIface.id()).detail("Count", m.size());
				s_req.send(m);
			} catch (Error& e) {
				if (e.code() == error_code_please_reboot || e.code() == error_code_please_reboot_delete)
					throw;
				TraceEvent(SevError, "WorkloadSendMetrics", workIface.id()).error(e);
				s_req.sendError(operation_failed());
			}
		}
		when(ReplyPromise<Void> r = waitNext(workIface.stop.getFuture())) {
			r.send(Void());
			break;
		}
	}
	return Void();
}

ACTOR Future<Void> testerServerWorkload(WorkloadRequest work,
                                        Reference<IClusterConnectionRecord> ccr,
                                        Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                        LocalityData locality) {
	state WorkloadInterface workIface;
	state bool replied = false;
	state Database cx;
	try {
		std::map<std::string, std::string> details;
		details["WorkloadTitle"] = printable(work.title);
		details["ClientId"] = format("%d", work.clientId);
		details["ClientCount"] = format("%d", work.clientCount);
		details["WorkloadTimeout"] = format("%d", work.timeout);
		startRole(Role::TESTER, workIface.id(), UID(), details);

		if (work.useDatabase) {
			cx = Database::createDatabase(ccr, ApiVersion::LATEST_VERSION, IsInternal::True, locality);
			cx->defaultTenant = work.defaultTenant.castTo<TenantName>();
			wait(delay(1.0));
		}

		// add test for "done" ?
		TraceEvent("WorkloadReceived", workIface.id()).detail("Title", work.title);
		Reference<TestWorkload> workload = wait(getWorkloadIface(work, ccr, dbInfo));
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

		wait(test);

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
	return Void();
}

ACTOR Future<Void> testerServerCore(TesterInterface interf,
                                    Reference<IClusterConnectionRecord> ccr,
                                    Reference<AsyncVar<struct ServerDBInfo> const> dbInfo,
                                    LocalityData locality,
                                    Optional<std::string> expectedWorkLoad) {
	state PromiseStream<Future<Void>> addWorkload;
	state Future<Void> workerFatalError = actorCollection(addWorkload.getFuture());

	// Dedicated to consistencyCheckerUrgent
	// At any time, we only allow at most 1 consistency checker workload on a server
	state std::pair<int64_t, Future<Void>> consistencyCheckerUrgentTester = std::make_pair(0, Future<Void>());

	TraceEvent("StartingTesterServerCore", interf.id())
	    .detail("ExpectedWorkload", expectedWorkLoad.present() ? expectedWorkLoad.get() : "[Unset]");
	loop choose {
		when(wait(workerFatalError)) {}
		when(wait(consistencyCheckerUrgentTester.second.isValid() ? consistencyCheckerUrgentTester.second : Never())) {
			ASSERT(consistencyCheckerUrgentTester.first != 0);
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadEnd", interf.id())
			    .detail("ConsistencyCheckerId", consistencyCheckerUrgentTester.first);
			consistencyCheckerUrgentTester = std::make_pair(0, Future<Void>()); // reset
		}
		when(WorkloadRequest work = waitNext(interf.recruitments.getFuture())) {
			if (expectedWorkLoad.present() && expectedWorkLoad.get() != work.title) {
				TraceEvent(SevError, "StartingTesterServerCoreUnexpectedWorkload", interf.id())
				    .detail("ClientId", work.clientId)
				    .detail("ClientCount", work.clientCount)
				    .detail("ExpectedWorkLoad", expectedWorkLoad.get())
				    .detail("WorkLoad", work.title);
				// Drop the workload
			} else if (work.title == "ConsistencyCheckUrgent") {
				// The workload is a consistency checker urgent workload
				if (work.sharedRandomNumber == consistencyCheckerUrgentTester.first) {
					TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterDuplicatedRequest", interf.id())
					    .detail("ConsistencyCheckerId", work.sharedRandomNumber)
					    .detail("ClientId", work.clientId)
					    .detail("ClientCount", work.clientCount);
					work.reply.sendError(consistency_check_urgent_duplicate_request());
				} else if (consistencyCheckerUrgentTester.second.isValid() &&
				           !consistencyCheckerUrgentTester.second.isReady()) {
					TraceEvent(SevWarnAlways, "ConsistencyCheckUrgent_TesterWorkloadConflict", interf.id())
					    .detail("ExistingConsistencyCheckerId", consistencyCheckerUrgentTester.first)
					    .detail("ArrivingConsistencyCheckerId", work.sharedRandomNumber)
					    .detail("ClientId", work.clientId)
					    .detail("ClientCount", work.clientCount);
					work.reply.sendError(consistency_check_urgent_conflicting_request());
				} else {
					consistencyCheckerUrgentTester = std::make_pair(
					    work.sharedRandomNumber, testerServerConsistencyCheckerUrgentWorkload(work, ccr, dbInfo));
					TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterWorkloadInitialized", interf.id())
					    .detail("ConsistencyCheckerId", consistencyCheckerUrgentTester.first)
					    .detail("ClientId", work.clientId)
					    .detail("ClientCount", work.clientCount);
				}
			} else {
				addWorkload.send(testerServerWorkload(work, ccr, dbInfo, locality));
			}
		}
	}
}

ACTOR Future<Void> clearData(Database cx, Optional<TenantName> defaultTenant) {
	state Transaction tr(cx);

	loop {
		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterClearingDatabaseStart", tr.trState->readOptions.get().debugID.get()).log();
			// This transaction needs to be self-conflicting, but not conflict consistently with
			// any other transactions
			tr.clear(normalKeys);
			tr.makeSelfConflicting();
			Version rv = wait(tr.getReadVersion()); // required since we use addReadConflictRange but not get
			TraceEvent("TesterClearingDatabaseRV", tr.trState->readOptions.get().debugID.get()).detail("RV", rv);
			wait(tr.commit());
			TraceEvent("TesterClearingDatabase", tr.trState->readOptions.get().debugID.get())
			    .detail("AtVersion", tr.getCommittedVersion());
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterClearingDatabaseError", tr.trState->readOptions.get().debugID.get()).error(e);
			wait(tr.onError(e));
		}
	}

	tr = Transaction(cx);
	loop {
		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterClearingTenantsStart", tr.trState->readOptions.get().debugID.get());
			state KeyBackedRangeResult<std::pair<int64_t, TenantMapEntry>> tenants =
			    wait(TenantMetadata::tenantMap().getRange(&tr, {}, {}, 1000));

			TraceEvent("TesterClearingTenantsDeletingBatch", tr.trState->readOptions.get().debugID.get())
			    .detail("FirstTenant", tenants.results.empty() ? "<none>"_sr : tenants.results[0].second.tenantName)
			    .detail("BatchSize", tenants.results.size());

			std::vector<Future<Void>> deleteFutures;
			for (auto const& [id, entry] : tenants.results) {
				if (!defaultTenant.present() || entry.tenantName != defaultTenant.get()) {
					deleteFutures.push_back(TenantAPI::deleteTenantTransaction(&tr, id));
				}
			}

			CODE_PROBE(deleteFutures.size() > 0, "Clearing tenants after test");

			wait(waitForAll(deleteFutures));
			wait(tr.commit());

			TraceEvent("TesterClearingTenantsDeletedBatch", tr.trState->readOptions.get().debugID.get())
			    .detail("FirstTenant", tenants.results.empty() ? "<none>"_sr : tenants.results[0].second.tenantName)
			    .detail("BatchSize", tenants.results.size());

			if (!tenants.more) {
				TraceEvent("TesterClearingTenantsComplete", tr.trState->readOptions.get().debugID.get())
				    .detail("AtVersion", tr.getCommittedVersion());
				break;
			}
			tr.reset();
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterClearingTenantsError", tr.trState->readOptions.get().debugID.get()).error(e);
			wait(tr.onError(e));
		}
	}

	tr = Transaction(cx);
	loop {
		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			tr.setOption(FDBTransactionOptions::RAW_ACCESS);
			state RangeResult rangeResult = wait(tr.getRange(normalKeys, 1));
			state Optional<Key> tenantPrefix;

			// If the result is non-empty, it is possible that there is some bad interaction between the test
			// and the optional simulated default tenant:
			//
			// 1. If the test is creating/deleting tenants itself, then it should disable the default tenant.
			// 2. If the test is opening Database objects itself, then it needs to propagate the default tenant
			//    value from the existing Database.
			// 3. If the test is using raw access or system key access and writing to the normal key-space, then
			//    it should disable the default tenant.
			if (!rangeResult.empty()) {
				if (cx->defaultTenant.present()) {
					TenantMapEntry entry = wait(TenantAPI::getTenant(cx.getReference(), cx->defaultTenant.get()));
					tenantPrefix = entry.prefix;
				}

				TraceEvent(SevError, "TesterClearFailure")
				    .detail("DefaultTenant", cx->defaultTenant)
				    .detail("TenantPrefix", tenantPrefix)
				    .detail("FirstKey", rangeResult[0].key);

				ASSERT(false);
			}
			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterCheckDatabaseClearedDone", tr.trState->readOptions.get().debugID.get());
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterCheckDatabaseClearedError", tr.trState->readOptions.get().debugID.get())
			    .error(e);
			wait(tr.onError(e));
		}
	}
	return Void();
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

ACTOR Future<Void> checkConsistencyScanAfterTest(Database cx, TesterConsistencyScanState* csState) {
	if (!csState->enabled) {
		return Void();
	}

	// mark it as done so later so this does not repeat
	csState->enabled = false;

	if (csState->enableAfter || csState->waitForComplete) {
		printf("Enabling consistency scan after test ...\n");
		wait(enableConsistencyScanInSim(cx));
		printf("Enabled consistency scan after test.\n");
	}

	wait(disableConsistencyScanInSim(cx, csState->waitForComplete));

	return Void();
}

ACTOR Future<DistributedTestResults> runWorkload(Database cx,
                                                 std::vector<TesterInterface> testers,
                                                 TestSpec spec,
                                                 Optional<TenantName> defaultTenant) {
	TraceEvent("TestRunning")
	    .detail("WorkloadTitle", spec.title)
	    .detail("TesterCount", testers.size())
	    .detail("Phases", spec.phases)
	    .detail("TestTimeout", spec.timeout);

	state std::vector<Future<WorkloadInterface>> workRequests;
	state std::vector<std::vector<PerfMetric>> metricsResults;

	state int i = 0;
	state int success = 0;
	state int failure = 0;
	int64_t sharedRandom = deterministicRandom()->randomInt64(0, 10000000);
	for (; i < testers.size(); i++) {
		WorkloadRequest req;
		req.title = spec.title;
		req.useDatabase = spec.useDB;
		req.runFailureWorkloads = spec.runFailureWorkloads;
		req.timeout = spec.timeout;
		req.databasePingDelay = spec.useDB ? spec.databasePingDelay : 0.0;
		req.options = spec.options;
		req.clientId = i;
		req.clientCount = testers.size();
		req.sharedRandomNumber = sharedRandom;
		req.defaultTenant = defaultTenant.castTo<TenantNameRef>();
		req.disabledFailureInjectionWorkloads = spec.disabledFailureInjectionWorkloads;
		workRequests.push_back(testers[i].recruitments.getReply(req));
	}

	state std::vector<WorkloadInterface> workloads = wait(getAll(workRequests));
	state double waitForFailureTime = g_network->isSimulated() ? 24 * 60 * 60 : 60;
	if (g_network->isSimulated() && spec.simCheckRelocationDuration)
		debug_setCheckRelocationDuration(true);

	if (spec.phases & TestWorkload::SETUP) {
		state std::vector<Future<ErrorOr<Void>>> setups;
		printf("setting up test (%s)...\n", printable(spec.title).c_str());
		TraceEvent("TestSetupStart").detail("WorkloadTitle", spec.title);
		setups.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			setups.push_back(workloads[i].setup.template getReplyUnlessFailedFor<Void>(waitForFailureTime, 0));
		wait(waitForAll(setups));
		throwIfError(setups, "SetupFailedForWorkload" + printable(spec.title));
		TraceEvent("TestSetupComplete").detail("WorkloadTitle", spec.title);
	}

	if (spec.phases & TestWorkload::EXECUTION) {
		TraceEvent("TestStarting").detail("WorkloadTitle", spec.title);
		printf("running test (%s)...\n", printable(spec.title).c_str());
		state std::vector<Future<ErrorOr<Void>>> starts;
		starts.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			starts.push_back(workloads[i].start.template getReplyUnlessFailedFor<Void>(waitForFailureTime, 0));
		wait(waitForAll(starts));
		throwIfError(starts, "StartFailedForWorkload" + printable(spec.title));
		printf("%s complete\n", printable(spec.title).c_str());
		TraceEvent("TestComplete").detail("WorkloadTitle", spec.title);
	}

	if (spec.phases & TestWorkload::CHECK) {
		if (spec.useDB && (spec.phases & TestWorkload::EXECUTION)) {
			wait(delay(3.0));
		}

		state std::vector<Future<ErrorOr<CheckReply>>> checks;
		TraceEvent("TestCheckingResults").detail("WorkloadTitle", spec.title);

		printf("checking test (%s)...\n", printable(spec.title).c_str());

		checks.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			checks.push_back(workloads[i].check.template getReplyUnlessFailedFor<CheckReply>(waitForFailureTime, 0));
		wait(waitForAll(checks));

		throwIfError(checks, "CheckFailedForWorkload" + printable(spec.title));

		for (int i = 0; i < checks.size(); i++) {
			if (checks[i].get().get().value)
				success++;
			else
				failure++;
		}
		TraceEvent("TestCheckComplete").detail("WorkloadTitle", spec.title);
	}

	if (spec.phases & TestWorkload::METRICS) {
		state std::vector<Future<ErrorOr<std::vector<PerfMetric>>>> metricTasks;
		printf("fetching metrics (%s)...\n", printable(spec.title).c_str());
		TraceEvent("TestFetchingMetrics").detail("WorkloadTitle", spec.title);
		metricTasks.reserve(workloads.size());
		for (int i = 0; i < workloads.size(); i++)
			metricTasks.push_back(
			    workloads[i].metrics.template getReplyUnlessFailedFor<std::vector<PerfMetric>>(waitForFailureTime, 0));
		wait(waitForAll(metricTasks));
		throwIfError(metricTasks, "MetricFailedForWorkload" + printable(spec.title));
		for (int i = 0; i < metricTasks.size(); i++) {
			metricsResults.push_back(metricTasks[i].get().get());
		}
	}

	// Stopping the workloads is unreliable, but they have a timeout
	// FIXME: stop if one of the above phases throws an exception
	for (int i = 0; i < workloads.size(); i++)
		workloads[i].stop.send(ReplyPromise<Void>());

	return DistributedTestResults(aggregateMetrics(metricsResults), success, failure);
}

// Sets the database configuration by running the ChangeConfig workload
ACTOR Future<Void> changeConfiguration(Database cx, std::vector<TesterInterface> testers, StringRef configMode) {
	state TestSpec spec;
	Standalone<VectorRef<KeyValueRef>> options;
	spec.title = "ChangeConfig"_sr;
	spec.runFailureWorkloads = false;
	options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "ChangeConfig"_sr));
	options.push_back_deep(options.arena(), KeyValueRef("configMode"_sr, configMode));
	spec.options.push_back_deep(spec.options.arena(), options);

	DistributedTestResults testResults = wait(runWorkload(cx, testers, spec, Optional<TenantName>()));

	return Void();
}

ACTOR Future<Void> auditStorageCorrectness(Reference<AsyncVar<ServerDBInfo>> dbInfo, AuditType auditType) {
	TraceEvent(SevDebug, "AuditStorageCorrectnessBegin").detail("AuditType", auditType);
	state Database cx;
	state UID auditId;
	state AuditStorageState auditState;
	loop {
		try {
			while (dbInfo->get().recoveryState < RecoveryState::ACCEPTING_COMMITS ||
			       !dbInfo->get().distributor.present()) {
				wait(dbInfo->onChange());
			}
			TriggerAuditRequest req(auditType, allKeys, KeyValueStoreType::END); // do not specify engine type to check
			UID auditId_ = wait(timeoutError(dbInfo->get().distributor.get().triggerAudit.getReply(req), 300));
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
			wait(delay(1));
		}
	}
	state int retryCount = 0;
	loop {
		try {
			cx = openDBOnServer(dbInfo);
			AuditStorageState auditState_ = wait(getAuditState(cx, auditType, auditId));
			auditState = auditState_;
			if (auditState.getPhase() == AuditPhase::Complete) {
				break;
			} else if (auditState.getPhase() == AuditPhase::Running) {
				TraceEvent("AuditStorageCorrectnessWait")
				    .detail("AuditID", auditId)
				    .detail("AuditType", auditType)
				    .detail("RetryCount", retryCount);
				wait(delay(25));
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
			wait(delay(1));
		}
	}
	TraceEvent("AuditStorageCorrectnessWaitEnd")
	    .detail("AuditID", auditId)
	    .detail("AuditType", auditType)
	    .detail("AuditState", auditState.toString());

	return Void();
}

// Runs the consistency check workload, which verifies that the database is in a consistent state
ACTOR Future<Void> checkConsistency(Database cx,
                                    std::vector<TesterInterface> testers,
                                    bool doQuiescentCheck,
                                    bool doCacheCheck,
                                    bool doTSSCheck,
                                    double quiescentWaitTimeout,
                                    double softTimeLimit,
                                    double databasePingDelay,
                                    Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	state TestSpec spec;

	state double connectionFailures;
	if (g_network->isSimulated()) {
		// NOTE: the value will be reset after consistency check
		connectionFailures = g_simulator->connectionFailuresDisableDuration;
		disableConnectionFailures("ConsistencyCheck");
		g_simulator->isConsistencyChecked = true;
	}

	Standalone<VectorRef<KeyValueRef>> options;
	StringRef performQuiescent = "false"_sr;
	StringRef performCacheCheck = "false"_sr;
	StringRef performTSSCheck = "false"_sr;
	if (doQuiescentCheck) {
		performQuiescent = "true"_sr;
		spec.restorePerpetualWiggleSetting = false;
	}
	if (doCacheCheck) {
		performCacheCheck = "true"_sr;
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
	options.push_back_deep(options.arena(), KeyValueRef("performCacheCheck"_sr, performCacheCheck));
	options.push_back_deep(options.arena(), KeyValueRef("performTSSCheck"_sr, performTSSCheck));
	options.push_back_deep(
	    options.arena(),
	    KeyValueRef("quiescentWaitTimeout"_sr, ValueRef(options.arena(), format("%f", quiescentWaitTimeout))));
	options.push_back_deep(options.arena(), KeyValueRef("distributed"_sr, "false"_sr));
	spec.options.push_back_deep(spec.options.arena(), options);

	state double start = now();
	state bool lastRun = false;
	loop {
		DistributedTestResults testResults = wait(runWorkload(cx, testers, spec, Optional<TenantName>()));
		if (testResults.ok() || lastRun) {
			if (g_network->isSimulated()) {
				g_simulator->connectionFailuresDisableDuration = connectionFailures;
				g_simulator->isConsistencyChecked = false;
			}
			return Void();
		}
		if (now() - start > softTimeLimit) {
			spec.options[0].push_back_deep(spec.options.arena(), KeyValueRef("failureIsError"_sr, "true"_sr));
			lastRun = true;
		}

		wait(repairDeadDatacenter(cx, dbInfo, "ConsistencyCheck"));
	}
}

ACTOR Future<std::unordered_set<int>> runUrgentConsistencyCheckWorkload(
    Database cx,
    std::vector<TesterInterface> testers,
    int64_t consistencyCheckerId,
    std::unordered_map<int, std::vector<KeyRange>> assignment) {
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_DispatchWorkloads")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);

	// Step 1: Get interfaces for running workloads
	state std::vector<Future<ErrorOr<WorkloadInterface>>> workRequests;
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
	wait(waitForAll(workRequests));

	// Step 2: Run workloads via the interfaces
	TraceEvent(SevInfo, "ConsistencyCheckUrgent_TriggerWorkloads")
	    .detail("TesterCount", testers.size())
	    .detail("ConsistencyCheckerId", consistencyCheckerId);
	state std::unordered_set<int> completeClientIds;
	state std::vector<int> clientIds; // record the clientId for jobs
	state std::vector<Future<ErrorOr<Void>>> jobs;
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
	wait(waitForAll(jobs));
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

	return completeClientIds;
}

ACTOR Future<std::vector<KeyRange>> getConsistencyCheckShards(Database cx, std::vector<KeyRange> ranges) {
	// Get the scope of the input list of ranges
	state Key beginKeyToReadKeyServer;
	state Key endKeyToReadKeyServer;
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
	state std::vector<KeyRange> res;
	state Transaction tr(cx);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			KeyRange rangeToRead = Standalone(KeyRangeRef(beginKeyToReadKeyServer, endKeyToReadKeyServer));
			RangeResult readResult = wait(krmGetRanges(&tr,
			                                           keyServersPrefix,
			                                           rangeToRead,
			                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
			                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
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
			wait(tr.onError(e));
		}
	}
	return res;
}

ACTOR Future<std::vector<TesterInterface>> getTesters(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                                      int minTestersExpected) {
	// Recruit workers
	state int flags = GetWorkersRequest::TESTER_CLASS_ONLY | GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY;
	state Future<Void> testerTimeout = delay(600.0); // wait 600 sec for testers to show up
	state std::vector<WorkerDetails> workers;
	loop {
		choose {
			when(std::vector<WorkerDetails> w =
			         wait(cc->get().present()
			                  ? brokenPromiseToNever(cc->get().get().getWorkers.getReply(GetWorkersRequest(flags)))
			                  : Never())) {
				if (w.size() >= minTestersExpected) {
					workers = w;
					break;
				}
				wait(delay(SERVER_KNOBS->WORKER_POLL_DELAY));
			}
			when(wait(cc->onChange())) {}
			when(wait(testerTimeout)) {
				TraceEvent(SevWarnAlways, "TesterRecruitmentTimeout");
				throw timed_out();
			}
		}
	}
	state std::vector<TesterInterface> ts;
	ts.reserve(workers.size());
	for (int i = 0; i < workers.size(); i++)
		ts.push_back(workers[i].interf.testerInterface);
	deterministicRandom()->randomShuffle(ts);
	return ts;
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

ACTOR Future<Void> runConsistencyCheckerUrgentCore(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                                   Database cx,
                                                   Optional<std::vector<TesterInterface>> testers,
                                                   int minTestersExpected) {
	state KeyRangeMap<bool> globalProgressMap; // used to keep track of progress
	state std::unordered_map<int, std::vector<KeyRange>> assignment; // used to keep track of assignment of tasks
	state std::vector<TesterInterface> ts; // used to store testers interface
	state std::vector<KeyRange> rangesToCheck; // get from globalProgressMap
	state std::vector<KeyRange> shardsToCheck; // get from keyServer metadata
	state Optional<double> whenFailedToGetTesterStart;

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

	state int64_t consistencyCheckerId = deterministicRandom()->randomInt64(1, 10000000);
	state int retryTimes = 0;
	state int round = 0;

	// Main loop
	loop {
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
				return Void();
			}

			// Step 2: Get testers
			ts.clear();
			if (!testers.present()) { // In real clusters
				try {
					wait(store(ts, getTesters(cc, minTestersExpected)));
					whenFailedToGetTesterStart.reset();
				} catch (Error& e) {
					if (e.code() == error_code_timed_out) {
						if (!whenFailedToGetTesterStart.present()) {
							whenFailedToGetTesterStart = now();
						} else if (now() - whenFailedToGetTesterStart.get() > 3600 * 24) { // 1 day
							TraceEvent(SevError, "TesterRecruitmentTimeout");
						}
					}
					throw e;
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
			wait(store(shardsToCheck, getConsistencyCheckShards(cx, rangesToCheck)));
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
			    wait(runUrgentConsistencyCheckWorkload(cx, ts, consistencyCheckerId, assignment));
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
				wait(delay(10.0));
				retryTimes++;
			}
		}

		wait(delay(10.0)); // Backoff 10 seconds for the next round

		// Decide and enforce the consistencyCheckerId for the next round
		consistencyCheckerId = deterministicRandom()->randomInt64(1, 10000000);
	}
}

ACTOR Future<Void> runConsistencyCheckerUrgentHolder(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> cc,
                                                     Database cx,
                                                     Optional<std::vector<TesterInterface>> testers,
                                                     int minTestersExpected,
                                                     bool repeatRun) {
	loop {
		wait(runConsistencyCheckerUrgentCore(cc, cx, testers, minTestersExpected));
		if (!repeatRun) {
			break;
		}
		wait(delay(CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_NEXT_WAIT_TIME));
	}
	return Void();
}

Future<Void> checkConsistencyUrgentSim(Database cx, std::vector<TesterInterface> testers) {
	return runConsistencyCheckerUrgentHolder(
	    Reference<AsyncVar<Optional<ClusterControllerFullInterface>>>(), cx, testers, 1, /*repeatRun=*/false);
}

ACTOR Future<bool> runTest(Database cx,
                           std::vector<TesterInterface> testers,
                           TestSpec spec,
                           Reference<AsyncVar<ServerDBInfo>> dbInfo,
                           Optional<TenantName> defaultTenant,
                           TesterConsistencyScanState* consistencyScanState) {
	state DistributedTestResults testResults;
	state double savedDisableDuration = 0;

	try {
		Future<DistributedTestResults> fTestResults = runWorkload(cx, testers, spec, defaultTenant);
		if (g_network->isSimulated() && spec.simConnectionFailuresDisableDuration > 0) {
			savedDisableDuration = g_simulator->connectionFailuresDisableDuration;
			g_simulator->connectionFailuresDisableDuration = spec.simConnectionFailuresDisableDuration;
		}
		if (spec.timeout > 0) {
			fTestResults = timeoutError(fTestResults, spec.timeout);
		}
		DistributedTestResults _testResults = wait(fTestResults);
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
		} else
			throw;
	}

	state bool ok = testResults.ok();

	if (spec.useDB) {
		printf("%d test clients passed; %d test clients failed\n", testResults.successes, testResults.failures);
		if (spec.dumpAfterTest) {
			try {
				wait(timeoutError(dumpDatabase(cx, "dump after " + printable(spec.title) + ".html", allKeys), 30.0));
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to dump database");
				ok = false;
			}

			wait(delay(1.0));
		}

		// Disable consistency scan before checkConsistency because otherwise it will prevent quiet database from
		// quiescing
		wait(checkConsistencyScanAfterTest(cx, consistencyScanState));
		printf("Consistency scan done\n");

		// Run the consistency check workload
		if (spec.runConsistencyCheck) {
			state bool quiescent = g_network->isSimulated() ? !BUGGIFY : spec.waitForQuiescenceEnd;
			try {
				printf("Running urgent consistency check...\n");
				// For testing urgent consistency check
				wait(timeoutError(checkConsistencyUrgentSim(cx, testers), 20000.0));
				printf("Urgent consistency check done\nRunning consistency check...\n");
				wait(timeoutError(checkConsistency(cx,
				                                   testers,
				                                   quiescent,
				                                   spec.runConsistencyCheckOnCache,
				                                   spec.runConsistencyCheckOnTSS,
				                                   10000.0,
				                                   5000,
				                                   spec.databasePingDelay,
				                                   dbInfo),
				                  20000.0));
				printf("Consistency check done\n");
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to perform consistency check");
				ok = false;
			}

			// Run auditStorage at the end of simulation
			if (quiescent && g_network->isSimulated()) {
				try {
					TraceEvent("AuditStorageStart");
					wait(timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateHA), 1500.0));
					TraceEvent("AuditStorageCorrectnessHADone");
					wait(timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateReplica), 1500.0));
					TraceEvent("AuditStorageCorrectnessReplicaDone");
					wait(timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateLocationMetadata), 1500.0));
					TraceEvent("AuditStorageCorrectnessLocationMetadataDone");
					wait(timeoutError(auditStorageCorrectness(dbInfo, AuditType::ValidateStorageServerShard), 1500.0));
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
		try {
			TraceEvent("TesterClearingDatabase").log();
			wait(timeoutError(clearData(cx, defaultTenant), 1000.0));
		} catch (Error& e) {
			TraceEvent(SevError, "ErrorClearingDatabaseAfterTest").error(e);
			throw; // If we didn't do this, we don't want any later tests to run on this DB
		}

		wait(delay(1.0));
	}

	return ok;
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
	{ "buggify", [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedBuggify", ""); } },
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
	{ "runConsistencyCheckOnCache",
	  [](const std::string& value, TestSpec* spec) {
	      spec->runConsistencyCheckOnCache = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedRunConsistencyCheckOnCache", spec->runConsistencyCheckOnCache);
	  } },
	{ "runConsistencyCheckOnTSS",
	  [](const std::string& value, TestSpec* spec) {
	      spec->runConsistencyCheckOnTSS = (value == "true");
	      TraceEvent("TestParserTest").detail("ParsedRunConsistencyCheckOnTSS", spec->runConsistencyCheckOnTSS);
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

ACTOR Future<Void> monitorServerDBInfo(Reference<AsyncVar<Optional<ClusterControllerFullInterface>>> ccInterface,
                                       LocalityData locality,
                                       Reference<AsyncVar<ServerDBInfo>> dbInfo) {
	// Initially most of the serverDBInfo is not known, but we know our locality right away
	ServerDBInfo localInfo;
	localInfo.myLocality = locality;
	dbInfo->set(localInfo);

	loop {
		GetServerDBInfoRequest req;
		req.knownServerInfoID = dbInfo->get().id;

		choose {
			when(ServerDBInfo _localInfo =
			         wait(ccInterface->get().present()
			                  ? brokenPromiseToNever(ccInterface->get().get().getServerDBInfo.getReply(req))
			                  : Never())) {
				ServerDBInfo localInfo = _localInfo;
				TraceEvent("GotServerDBInfoChange")
				    .detail("ChangeID", localInfo.id)
				    .detail("MasterID", localInfo.master.id())
				    .detail("RatekeeperID", localInfo.ratekeeper.present() ? localInfo.ratekeeper.get().id() : UID())
				    .detail("DataDistributorID",
				            localInfo.distributor.present() ? localInfo.distributor.get().id() : UID());

				localInfo.myLocality = locality;
				dbInfo->set(localInfo);
			}
			when(wait(ccInterface->onChange())) {
				if (ccInterface->get().present())
					TraceEvent("GotCCInterfaceChange")
					    .detail("CCID", ccInterface->get().get().id())
					    .detail("CCMachine", ccInterface->get().get().getWorkers.getEndpoint().getPrimaryAddress());
			}
		}
	}
}

ACTOR Future<Void> initializeSimConfig(Database db) {
	state Transaction tr(db);
	ASSERT(g_network->isSimulated());
	loop {
		try {
			DatabaseConfiguration dbConfig = wait(getDatabaseConfiguration(&tr));
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

			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

void encryptionAtRestPlaintextMarkerCheck() {
	if (!g_network->isSimulated() || !g_simulator->dataAtRestPlaintextMarker.present()) {
		// Encryption at-rest was not enabled, do nothing
		return;
	}

	namespace fs = std::filesystem;

	printf("EncryptionAtRestPlaintextMarkerCheckStart\n");
	TraceEvent("EncryptionAtRestPlaintextMarkerCheckStart");
	fs::path p("simfdb/");
	fs::recursive_directory_iterator end;
	int scanned = 0;
	bool success = true;
	// Enumerate all files in the "simfdb/" folder and look for "marker" string
	for (fs::recursive_directory_iterator itr(p); itr != end; ++itr) {
		if (fs::is_regular_file(itr->path())) {
			std::ifstream f(itr->path());
			if (f) {
				std::string buf;
				int count = 0;
				while (std::getline(f, buf)) {
					// SOMEDAY: using 'std::boyer_moore_horspool_searcher' would significantly improve search
					// time
					if (!g_network->isSimulated() || !ENABLE_MUTATION_TRACKING_WITH_BLOB_CIPHER) {
						if (buf.find(g_simulator->dataAtRestPlaintextMarker.get()) != std::string::npos) {
							TraceEvent(SevError, "EncryptionAtRestPlaintextMarkerCheckPanic")
							    .detail("Filename", itr->path().string())
							    .detail("LineBuf", buf)
							    .detail("Marker", g_simulator->dataAtRestPlaintextMarker.get());
							success = false;
						}
					}
					count++;
				}
				TraceEvent("EncryptionAtRestPlaintextMarkerCheckScanned")
				    .detail("Filename", itr->path().string())
				    .detail("NumLines", count);
				scanned++;
				if (itr->path().string().find("storage") != std::string::npos) {
					CODE_PROBE(true,
					           "EncryptionAtRestPlaintextMarkerCheckScanned storage file scanned",
					           probe::decoration::rare);
				} else if (itr->path().string().find("fdbblob") != std::string::npos) {
					CODE_PROBE(true,
					           "EncryptionAtRestPlaintextMarkerCheckScanned BlobGranule file scanned",
					           probe::decoration::rare);
				} else if (itr->path().string().find("logqueue") != std::string::npos) {
					CODE_PROBE(
					    true, "EncryptionAtRestPlaintextMarkerCheckScanned TLog file scanned", probe::decoration::rare);
				} else if (itr->path().string().find("backup") != std::string::npos) {
					CODE_PROBE(true,
					           "EncryptionAtRestPlaintextMarkerCheckScanned KVBackup file scanned",
					           probe::decoration::rare);
				}
			} else {
				TraceEvent(SevError, "FileOpenError").detail("Filename", itr->path().string());
			}
		}
		ASSERT(success);
	}
	printf("EncryptionAtRestPlaintextMarkerCheckEnd NumFiles: %d\n", scanned);
	TraceEvent("EncryptionAtRestPlaintextMarkerCheckEnd").detail("NumFiles", scanned);
}

// Disables connection failures after the given time seconds
ACTOR Future<Void> disableConnectionFailuresAfter(double seconds, std::string context) {
	if (g_network->isSimulated()) {
		TraceEvent(SevWarnAlways, ("ScheduleDisableConnectionFailures_" + context).c_str())
		    .detail("At", now() + seconds);
		wait(delay(seconds));
		disableConnectionFailures(context);
	}
	return Void();
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
ACTOR Future<Void> runTests(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> cc,
                            Reference<AsyncVar<Optional<struct ClusterInterface>>> ci,
                            std::vector<TesterInterface> testers,
                            std::vector<TestSpec> tests,
                            StringRef startingConfiguration,
                            LocalityData locality,
                            Optional<TenantName> defaultTenant,
                            Standalone<VectorRef<TenantNameRef>> tenantsToCreate,
                            bool restartingTest) {
	state Database cx;
	state Reference<AsyncVar<ServerDBInfo>> dbInfo(new AsyncVar<ServerDBInfo>);
	state Future<Void> ccMonitor = monitorServerDBInfo(cc, LocalityData(), dbInfo); // FIXME: locality

	state bool useDB = false;
	state bool waitForQuiescenceBegin = false;
	state bool waitForQuiescenceEnd = false;
	state bool restorePerpetualWiggleSetting = false;
	state bool perpetualWiggleEnabled = false;
	state double startDelay = 0.0;
	state double databasePingDelay = 1e9;
	state ISimulator::BackupAgentType simBackupAgents = ISimulator::BackupAgentType::NoBackupAgents;
	state ISimulator::BackupAgentType simDrAgents = ISimulator::BackupAgentType::NoBackupAgents;
	state bool enableDD = false;
	state TesterConsistencyScanState consistencyScanState;

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
		cx->defaultTenant = defaultTenant;
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
		try {
			wait(timeoutError(changeConfiguration(cx, testers, startingConfiguration), 2000.0));
			if (g_network->isSimulated() && enableDD) {
				wait(success(setDDMode(cx, 1)));
			}
		} catch (Error& e) {
			TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to set starting configuration");
		}
		if (restorePerpetualWiggleSetting) {
			std::string_view confView(reinterpret_cast<const char*>(startingConfiguration.begin()),
			                          startingConfiguration.size());
			const std::string setting = "perpetual_storage_wiggle:=";
			auto pos = confView.find(setting);
			if (pos != confView.npos && confView.at(pos + setting.size()) == '1') {
				perpetualWiggleEnabled = true;
			}
		}
	}

	// Read cluster configuration
	if (useDB && g_network->isSimulated()) {
		DatabaseConfiguration configuration = wait(getDatabaseConfiguration(cx));

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
			wait(customShardConfigWorkload(cx));
		}
	}

	if (useDB) {
		std::vector<Future<Void>> tenantFutures;
		for (auto tenant : tenantsToCreate) {
			TenantMapEntry entry;
			if (deterministicRandom()->coinflip()) {
				entry.tenantGroup = "TestTenantGroup"_sr;
			}
			TraceEvent("CreatingTenant").detail("Tenant", tenant).detail("TenantGroup", entry.tenantGroup);
			tenantFutures.push_back(success(TenantAPI::createTenant(cx.getReference(), tenant, entry)));
		}

		wait(waitForAll(tenantFutures));
		if (g_network->isSimulated()) {
			wait(initializeSimConfig(cx));
		}
	}

	if (useDB && waitForQuiescenceBegin) {
		TraceEvent("TesterStartingPreTestChecks")
		    .detail("DatabasePingDelay", databasePingDelay)
		    .detail("StartDelay", startDelay);

		try {
			wait(quietDatabase(cx, dbInfo, "Start") ||
			     (databasePingDelay == 0.0
			          ? Never()
			          : testDatabaseLiveness(cx, databasePingDelay, "QuietDatabaseStart", startDelay)));
		} catch (Error& e) {
			TraceEvent("QuietDatabaseStartExternalError").error(e);
			throw;
		}

		if (perpetualWiggleEnabled) { // restore the enabled perpetual storage wiggle setting
			printf("Set perpetual_storage_wiggle=1 ...\n");
			Version cVer = wait(setPerpetualStorageWiggle(cx, true, LockAware::True));
			(void)cVer;
			printf("Set perpetual_storage_wiggle=1 Done.\n");
		}

		// TODO: Move this to a BehaviorInjection workload once that concept exists.
		if (consistencyScanState.enabled && !consistencyScanState.enableAfter) {
			printf("Enabling consistency scan ...\n");
			wait(enableConsistencyScanInSim(cx));
			printf("Enabled consistency scan.\n");
		}
	}

	enableConnectionFailures("Tester");
	state Future<Void> disabler = disableConnectionFailuresAfter(FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, "Tester");
	state Future<Void> repairDataCenter;
	if (useDB) {
		Future<Void> reconfigure = reconfigureAfter(cx, FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, dbInfo, "Tester");
		repairDataCenter = reconfigure;
	}

	TraceEvent("TestsExpectedToPass").detail("Count", tests.size());
	state int idx = 0;
	state std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup;
	for (; idx < tests.size(); idx++) {
		printf("Run test:%s start\n", tests[idx].title.toString().c_str());
		knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(tests[idx].overrideKnobs);
		wait(success(runTest(cx, testers, tests[idx], dbInfo, defaultTenant, &consistencyScanState)));
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

				wait(waitConsistencyScanEnd && waitQuietDatabaseEnd);
			} catch (Error& e) {
				TraceEvent("QuietDatabaseEndExternalError").error(e);
				throw;
			}
		}
	}
	printf("\n");

	encryptionAtRestPlaintextMarkerCheck();

	return Void();
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
ACTOR Future<Void> runTests(Reference<AsyncVar<Optional<struct ClusterControllerFullInterface>>> cc,
                            Reference<AsyncVar<Optional<struct ClusterInterface>>> ci,
                            std::vector<TestSpec> tests,
                            test_location_t at,
                            int minTestersExpected,
                            StringRef startingConfiguration,
                            LocalityData locality,
                            Optional<TenantName> defaultTenant,
                            Standalone<VectorRef<TenantNameRef>> tenantsToCreate,
                            bool restartingTest) {
	state int flags = (at == TEST_ON_SERVERS ? 0 : GetWorkersRequest::TESTER_CLASS_ONLY) |
	                  GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY;
	state Future<Void> testerTimeout = delay(600.0); // wait 600 sec for testers to show up
	state std::vector<WorkerDetails> workers;

	loop {
		choose {
			when(std::vector<WorkerDetails> w =
			         wait(cc->get().present()
			                  ? brokenPromiseToNever(cc->get().get().getWorkers.getReply(GetWorkersRequest(flags)))
			                  : Never())) {
				if (w.size() >= minTestersExpected) {
					workers = w;
					break;
				}
				wait(delay(SERVER_KNOBS->WORKER_POLL_DELAY));
			}
			when(wait(cc->onChange())) {}
			when(wait(testerTimeout)) {
				TraceEvent(SevError, "TesterRecruitmentTimeout").log();
				throw timed_out();
			}
		}
	}

	std::vector<TesterInterface> ts;
	ts.reserve(workers.size());
	for (int i = 0; i < workers.size(); i++)
		ts.push_back(workers[i].interf.testerInterface);

	wait(runTests(cc, ci, ts, tests, startingConfiguration, locality, defaultTenant, tenantsToCreate, restartingTest));
	return Void();
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
ACTOR Future<Void> runTests(Reference<IClusterConnectionRecord> connRecord,
                            test_type_t whatToRun,
                            test_location_t at,
                            int minTestersExpected,
                            std::string fileName,
                            StringRef startingConfiguration,
                            LocalityData locality,
                            UnitTestParameters testOptions,
                            Optional<TenantName> defaultTenant,
                            Standalone<VectorRef<TenantNameRef>> tenantsToCreate,
                            bool restartingTest) {
	state TestSet testSet;
	state std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup(nullptr);
	auto cc = makeReference<AsyncVar<Optional<ClusterControllerFullInterface>>>();
	auto ci = makeReference<AsyncVar<Optional<ClusterInterface>>>();
	std::vector<Future<Void>> actors;
	if (connRecord) {
		actors.push_back(reportErrors(monitorLeader(connRecord, cc), "MonitorLeader"));
		actors.push_back(reportErrors(extractClusterInterface(cc, ci), "ExtractClusterInterface"));
	}

	if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK_URGENT) {
		// Need not to set spec here. Will set spec when triggering workload
	} else if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK) {
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
	} else if (whatToRun == TEST_TYPE_UNIT_TESTS) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = "UnitTests"_sr;
		spec.startDelay = 0;
		spec.useDB = false;
		spec.timeout = 0;
		options.push_back_deep(options.arena(), KeyValueRef("testName"_sr, "UnitTests"_sr));
		options.push_back_deep(options.arena(), KeyValueRef("testsMatching"_sr, fileName));
		// Add unit test options as test spec options
		for (auto& kv : testOptions.params) {
			options.push_back_deep(options.arena(), KeyValueRef(kv.first, kv.second));
		}
		spec.options.push_back_deep(spec.options.arena(), options);
		testSet.testSpecs.push_back(spec);
	} else {
		std::ifstream ifs;
		ifs.open(fileName.c_str(), std::ifstream::in);
		if (!ifs.good()) {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "file open failed")
			    .detail("File", fileName.c_str());
			fprintf(stderr, "ERROR: Could not open file `%s'\n", fileName.c_str());
			return Void();
		}
		enableClientInfoLogging(); // Enable Client Info logging by default for tester
		if (fileName.ends_with(".txt")) {
			testSet.testSpecs = readTests(ifs);
		} else if (fileName.ends_with(".toml")) {
			// TOML is weird about opening the file as binary on windows, so we
			// just let TOML re-open the file instead of using ifs.
			testSet = readTOMLTests(fileName);
		} else {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "unknown tests specification extension")
			    .detail("File", fileName.c_str());
			return Void();
		}
		ifs.close();
	}

	knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(testSet.overrideKnobs);
	Future<Void> tests;
	if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK_URGENT) {
		state Database cx;
		state Reference<AsyncVar<ServerDBInfo>> dbInfo(new AsyncVar<ServerDBInfo>);
		state Future<Void> ccMonitor = monitorServerDBInfo(cc, LocalityData(), dbInfo); // FIXME: locality
		cx = openDBOnServer(dbInfo);
		tests =
		    reportErrors(runConsistencyCheckerUrgentHolder(
		                     cc, cx, Optional<std::vector<TesterInterface>>(), minTestersExpected, /*repeatRun=*/true),
		                 "runConsistencyCheckerUrgentHolder");
	} else if (at == TEST_HERE) {
		auto db = makeReference<AsyncVar<ServerDBInfo>>();
		std::vector<TesterInterface> iTesters(1);
		actors.push_back(
		    reportErrors(monitorServerDBInfo(cc, LocalityData(), db), "MonitorServerDBInfo")); // FIXME: Locality
		actors.push_back(reportErrors(testerServerCore(iTesters[0], connRecord, db, locality), "TesterServerCore"));
		tests = runTests(cc,
		                 ci,
		                 iTesters,
		                 testSet.testSpecs,
		                 startingConfiguration,
		                 locality,
		                 defaultTenant,
		                 tenantsToCreate,
		                 restartingTest);
	} else {
		tests = reportErrors(runTests(cc,
		                              ci,
		                              testSet.testSpecs,
		                              at,
		                              minTestersExpected,
		                              startingConfiguration,
		                              locality,
		                              defaultTenant,
		                              tenantsToCreate,
		                              restartingTest),
		                     "RunTests");
	}

	choose {
		when(wait(tests)) {
			return Void();
		}
		when(wait(quorum(actors, 1))) {
			ASSERT(false);
			throw internal_error();
		}
	}
}

namespace {
ACTOR Future<Void> testExpectedErrorImpl(Future<Void> test,
                                         const char* testDescr,
                                         Optional<Error> expectedError,
                                         Optional<bool*> successFlag,
                                         std::map<std::string, std::string> details,
                                         Optional<Error> throwOnError,
                                         UID id) {
	state Error actualError;
	try {
		wait(test);
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw e;
		}
		actualError = e;
		// The test failed as expected
		if (!expectedError.present() || actualError.code() == expectedError.get().code()) {
			return Void();
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
	if (actualError.isValid()) {
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
	return Void();
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
