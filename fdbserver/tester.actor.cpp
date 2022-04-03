/*
 * tester.actor.cpp
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

#include <boost/algorithm/string/predicate.hpp>
#include <cinttypes>
#include <fstream>
#include <functional>
#include <map>
#include <toml.hpp>

#include "flow/ActorCollection.h"
#include "fdbrpc/sim_validation.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/KnobProtectiveGroups.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/Status.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbclient/MonitorLeader.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

WorkloadContext::WorkloadContext() {}

WorkloadContext::WorkloadContext(const WorkloadContext& r)
  : options(r.options), clientId(r.clientId), clientCount(r.clientCount), sharedRandomNumber(r.sharedRandomNumber),
    dbInfo(r.dbInfo) {}

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
	double d = double(index) / nodeCount;
	emplaceIndex(data, idx, *(int64_t*)&d);

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
			options[i].value = LiteralStringRef("");
			return value;
		}

	return defaultValue;
}

int getOption(VectorRef<KeyValueRef> options, Key key, int defaultValue) {
	for (int i = 0; i < options.size(); i++)
		if (options[i].key == key) {
			int r;
			if (sscanf(options[i].value.toString().c_str(), "%d", &r)) {
				options[i].value = LiteralStringRef("");
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
				options[i].value = LiteralStringRef("");
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
				options[i].value = LiteralStringRef("");
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
				options[i].value = LiteralStringRef("");
				return r;
			}
		}

	return defaultValue;
}

bool getOption(VectorRef<KeyValueRef> options, Key key, bool defaultValue) {
	Value p = getOption(options, key, defaultValue ? LiteralStringRef("true") : LiteralStringRef("false"));
	if (p == LiteralStringRef("true"))
		return true;
	if (p == LiteralStringRef("false"))
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
			options[i].value = LiteralStringRef("");
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
	static StringRef nothing = LiteralStringRef("");
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

struct CompoundWorkload : TestWorkload {
	std::vector<Reference<TestWorkload>> workloads;

	CompoundWorkload(WorkloadContext& wcx) : TestWorkload(wcx) {}
	CompoundWorkload* add(Reference<TestWorkload>&& w) {
		workloads.push_back(std::move(w));
		return this;
	}

	std::string description() const override {
		std::string d;
		for (int w = 0; w < workloads.size(); w++)
			d += workloads[w]->description() + (w == workloads.size() - 1 ? "" : ";");
		return d;
	}
	Future<Void> setup(Database const& cx) override {
		std::vector<Future<Void>> all;
		all.reserve(workloads.size());
		for (int w = 0; w < workloads.size(); w++)
			all.push_back(workloads[w]->setup(cx));
		return waitForAll(all);
	}
	Future<Void> start(Database const& cx) override {
		std::vector<Future<Void>> all;
		all.reserve(workloads.size());
		auto wCount = std::make_shared<unsigned>(0);
		for (int i = 0; i < workloads.size(); i++) {
			std::string workloadName = workloads[i]->description();
			++(*wCount);
			TraceEvent("WorkloadRunStatus")
			    .detail("Name", workloadName)
			    .detail("Count", *wCount)
			    .detail("Phase", "Start");
			all.push_back(fmap(
			    [workloadName, wCount](Void value) {
				    --(*wCount);
				    TraceEvent("WorkloadRunStatus")
				        .detail("Name", workloadName)
				        .detail("Remaining", *wCount)
				        .detail("Phase", "End");
				    return Void();
			    },
			    workloads[i]->start(cx)));
		}
		return waitForAll(all);
	}
	Future<bool> check(Database const& cx) override {
		std::vector<Future<bool>> all;
		all.reserve(workloads.size());
		auto wCount = std::make_shared<unsigned>(0);
		for (int i = 0; i < workloads.size(); i++) {
			++(*wCount);
			std::string workloadName = workloads[i]->description();
			TraceEvent("WorkloadCheckStatus")
			    .detail("Name", workloadName)
			    .detail("Count", *wCount)
			    .detail("Phase", "Start");
			all.push_back(fmap(
			    [workloadName, wCount](bool ret) {
				    --(*wCount);
				    TraceEvent("WorkloadCheckStatus")
				        .detail("Name", workloadName)
				        .detail("Remaining", *wCount)
				        .detail("Phase", "End");
				    return true;
			    },
			    workloads[i]->check(cx)));
		}
		return allTrue(all);
	}
	void getMetrics(std::vector<PerfMetric>& m) override {
		for (int w = 0; w < workloads.size(); w++) {
			std::vector<PerfMetric> p;
			workloads[w]->getMetrics(p);
			for (int i = 0; i < p.size(); i++)
				m.push_back(p[i].withPrefix(workloads[w]->description() + "."));
		}
	}
	double getCheckTimeout() const override {
		double m = 0;
		for (int w = 0; w < workloads.size(); w++)
			m = std::max(workloads[w]->getCheckTimeout(), m);
		return m;
	}
};

Reference<TestWorkload> getWorkloadIface(WorkloadRequest work,
                                         VectorRef<KeyValueRef> options,
                                         Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	Value testName = getOption(options, LiteralStringRef("testName"), LiteralStringRef("no-test-specified"));
	WorkloadContext wcx;
	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.dbInfo = dbInfo;
	wcx.options = options;
	wcx.sharedRandomNumber = work.sharedRandomNumber;

	auto workload = IWorkloadFactory::create(testName.toString(), wcx);

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

Reference<TestWorkload> getWorkloadIface(WorkloadRequest work, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	if (work.options.size() < 1) {
		TraceEvent(SevError, "TestCreationError").detail("Reason", "No options provided");
		fprintf(stderr, "ERROR: No options were provided for workload.\n");
		throw test_specification_invalid();
	}
	if (work.options.size() == 1)
		return getWorkloadIface(work, work.options[0], dbInfo);

	WorkloadContext wcx;
	wcx.clientId = work.clientId;
	wcx.clientCount = work.clientCount;
	wcx.sharedRandomNumber = work.sharedRandomNumber;
	// FIXME: Other stuff not filled in; why isn't this constructed here and passed down to the other
	// getWorkloadIface()?
	auto compound = makeReference<CompoundWorkload>(wcx);
	for (int i = 0; i < work.options.size(); i++) {
		compound->add(getWorkloadIface(work, work.options[i], dbInfo));
	}
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
	auto processes = g_simulator.getAllProcesses();
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
		printf("%sName: %s\n", indent.c_str(), p->name);
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
			TraceEvent(traceMsg.c_str());
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
				std::vector<PerfMetric> m;
				workload->getMetrics(m);
				TraceEvent("WorkloadSendMetrics", workIface.id()).detail("Count", m.size());
				req.send(m);
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
			cx = Database::createDatabase(ccr, -1, IsInternal::True, locality);
			cx->defaultTenant = work.defaultTenant.castTo<TenantName>();
			wait(delay(1.0));
		}

		// add test for "done" ?
		TraceEvent("WorkloadReceived", workIface.id()).detail("Title", work.title);
		auto workload = getWorkloadIface(work, dbInfo);
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
                                    LocalityData locality) {
	state PromiseStream<Future<Void>> addWorkload;
	state Future<Void> workerFatalError = actorCollection(addWorkload.getFuture());

	TraceEvent("StartingTesterServerCore", interf.id());
	loop choose {
		when(wait(workerFatalError)) {}
		when(WorkloadRequest work = waitNext(interf.recruitments.getFuture())) {
			addWorkload.send(testerServerWorkload(work, ccr, dbInfo, locality));
		}
	}
}

ACTOR Future<Void> clearData(Database cx) {
	state Transaction tr(cx);
	loop {
		try {
			// This transaction needs to be self-conflicting, but not conflict consistently with
			// any other transactions
			tr.clear(normalKeys);
			tr.makeSelfConflicting();
			wait(success(tr.getReadVersion())); // required since we use addReadConflictRange but not get
			wait(tr.commit());
			TraceEvent("TesterClearingDatabase").detail("AtVersion", tr.getCommittedVersion());
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterClearingDatabaseError").error(e);
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
		req.timeout = spec.timeout;
		req.databasePingDelay = spec.useDB ? spec.databasePingDelay : 0.0;
		req.options = spec.options;
		req.clientId = i;
		req.clientCount = testers.size();
		req.sharedRandomNumber = sharedRandom;
		req.defaultTenant = defaultTenant.castTo<TenantNameRef>();
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
		TraceEvent("CheckingResults").log();

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
	spec.title = LiteralStringRef("ChangeConfig");
	options.push_back_deep(options.arena(),
	                       KeyValueRef(LiteralStringRef("testName"), LiteralStringRef("ChangeConfig")));
	options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("configMode"), configMode));
	spec.options.push_back_deep(spec.options.arena(), options);

	DistributedTestResults testResults = wait(runWorkload(cx, testers, spec, Optional<TenantName>()));

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
		connectionFailures = g_simulator.connectionFailuresDisableDuration;
		g_simulator.connectionFailuresDisableDuration = 1e6;
		g_simulator.speedUpSimulation = true;
	}

	Standalone<VectorRef<KeyValueRef>> options;
	StringRef performQuiescent = LiteralStringRef("false");
	StringRef performCacheCheck = LiteralStringRef("false");
	StringRef performTSSCheck = LiteralStringRef("false");
	if (doQuiescentCheck) {
		performQuiescent = LiteralStringRef("true");
		spec.restorePerpetualWiggleSetting = false;
	}
	if (doCacheCheck) {
		performCacheCheck = LiteralStringRef("true");
	}
	if (doTSSCheck) {
		performTSSCheck = LiteralStringRef("true");
	}
	spec.title = LiteralStringRef("ConsistencyCheck");
	spec.databasePingDelay = databasePingDelay;
	spec.timeout = 32000;
	options.push_back_deep(options.arena(),
	                       KeyValueRef(LiteralStringRef("testName"), LiteralStringRef("ConsistencyCheck")));
	options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("performQuiescentChecks"), performQuiescent));
	options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("performCacheCheck"), performCacheCheck));
	options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("performTSSCheck"), performTSSCheck));
	options.push_back_deep(options.arena(),
	                       KeyValueRef(LiteralStringRef("quiescentWaitTimeout"),
	                                   ValueRef(options.arena(), format("%f", quiescentWaitTimeout))));
	options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("distributed"), LiteralStringRef("false")));
	spec.options.push_back_deep(spec.options.arena(), options);

	state double start = now();
	state bool lastRun = false;
	loop {
		DistributedTestResults testResults = wait(runWorkload(cx, testers, spec, Optional<TenantName>()));
		if (testResults.ok() || lastRun) {
			if (g_network->isSimulated()) {
				g_simulator.connectionFailuresDisableDuration = connectionFailures;
			}
			return Void();
		}
		if (now() - start > softTimeLimit) {
			spec.options[0].push_back_deep(spec.options.arena(),
			                               KeyValueRef(LiteralStringRef("failureIsError"), LiteralStringRef("true")));
			lastRun = true;
		}

		wait(repairDeadDatacenter(cx, dbInfo, "ConsistencyCheck"));
	}
}

ACTOR Future<bool> runTest(Database cx,
                           std::vector<TesterInterface> testers,
                           TestSpec spec,
                           Reference<AsyncVar<ServerDBInfo>> dbInfo,
                           Optional<TenantName> defaultTenant) {
	state DistributedTestResults testResults;

	try {
		Future<DistributedTestResults> fTestResults = runWorkload(cx, testers, spec, defaultTenant);
		if (spec.timeout > 0) {
			fTestResults = timeoutError(fTestResults, spec.timeout);
		}
		DistributedTestResults _testResults = wait(fTestResults);
		testResults = _testResults;
		logMetrics(testResults.metrics);
	} catch (Error& e) {
		if (e.code() == error_code_timed_out) {
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
		if (spec.dumpAfterTest) {
			try {
				wait(timeoutError(dumpDatabase(cx, "dump after " + printable(spec.title) + ".html", allKeys), 30.0));
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to dump database");
				ok = false;
			}

			wait(delay(1.0));
		}

		// Run the consistency check workload
		if (spec.runConsistencyCheck) {
			try {
				bool quiescent = g_network->isSimulated() ? !BUGGIFY : spec.waitForQuiescenceEnd;
				wait(timeoutError(checkConsistency(cx,
				                                   testers,
				                                   quiescent,
				                                   spec.runConsistencyCheckOnCache,
				                                   spec.runConsistencyCheckOnTSS,
				                                   10000.0,
				                                   18000,
				                                   spec.databasePingDelay,
				                                   dbInfo),
				                  20000.0));
			} catch (Error& e) {
				TraceEvent(SevError, "TestFailure").error(e).detail("Reason", "Unable to perform consistency check");
				ok = false;
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
			wait(timeoutError(clearData(cx), 1000.0));
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
	{ "extraDB", [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedExtraDB", ""); } },
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
	  [](const std::string& value) { TraceEvent("TestParserTest").detail("ParsedDisableHostname", ""); } }
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
	      if (g_network->isSimulated())
		      g_simulator.connectionFailuresDisableDuration = spec->simConnectionFailuresDisableDuration;
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

std::vector<TestSpec> readTOMLTests_(std::string fileName) {
	TestSpec spec;
	Standalone<VectorRef<KeyValueRef>> workloadOptions;
	std::vector<TestSpec> result;

	const toml::value& conf = toml::parse(fileName);

	// Then parse each test
	const toml::array& tests = toml::find(conf, "test").as_array();
	for (const toml::value& test : tests) {
		spec = TestSpec();

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
		try {
			const toml::array& overrideKnobs = toml::find(test, "knobs").as_array();
			for (const toml::value& knob : overrideKnobs) {
				for (const auto& [key, value_] : knob.as_table()) {
					const std::string& value = toml_to_string(value_);
					ParsedKnobValue parsedValue = CLIENT_KNOBS->parseKnobValue(key, value);
					if (std::get_if<NoKnobFound>(&parsedValue)) {
						parsedValue = SERVER_KNOBS->parseKnobValue(key, value);
					}
					if (std::get_if<NoKnobFound>(&parsedValue)) {
						TraceEvent(SevError, "TestSpecUnrecognizedKnob")
						    .detail("KnobName", key)
						    .detail("OverrideValue", value);
						continue;
					}
					spec.overrideKnobs.set(key, parsedValue);
				}
			}
		} catch (const std::out_of_range&) {
			// no knob overridden
		}

		result.push_back(spec);
	}

	return result;
}

// A hack to catch and log std::exception, because TOML11 has very useful
// error messages, but the actor framework can't handle std::exception.
std::vector<TestSpec> readTOMLTests(std::string fileName) {
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
                            Optional<TenantName> defaultTenant) {
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
		enableDD = enableDD || getOption(iter->options[0], LiteralStringRef("enableDD"), false);
	}

	if (g_network->isSimulated()) {
		g_simulator.backupAgents = simBackupAgents;
		g_simulator.drAgents = simDrAgents;
	}

	// turn off the database ping functionality if the suite of tests are not going to be using the database
	if (!useDB)
		databasePingDelay = 0.0;

	if (useDB) {
		cx = openDBOnServer(dbInfo);
		cx->defaultTenant = defaultTenant;
	}

	state Future<Void> disabler = disableConnectionFailuresAfter(FLOW_KNOBS->SIM_SPEEDUP_AFTER_SECONDS, "Tester");

	// Change the configuration (and/or create the database) if necessary
	printf("startingConfiguration:%s start\n", startingConfiguration.toString().c_str());
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

	if (useDB && defaultTenant.present()) {
		TraceEvent("CreatingDefaultTenant").detail("Tenant", defaultTenant.get());
		wait(ManagementAPI::createTenant(cx.getReference(), defaultTenant.get()));
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
			wait(setPerpetualStorageWiggle(cx, true, LockAware::True));
			printf("Set perpetual_storage_wiggle=1 Done.\n");
		}
	}

	TraceEvent("TestsExpectedToPass").detail("Count", tests.size());
	state int idx = 0;
	state std::unique_ptr<KnobProtectiveGroup> knobProtectiveGroup;
	for (; idx < tests.size(); idx++) {
		printf("Run test:%s start\n", tests[idx].title.toString().c_str());
		knobProtectiveGroup = std::make_unique<KnobProtectiveGroup>(tests[idx].overrideKnobs);
		wait(success(runTest(cx, testers, tests[idx], dbInfo, defaultTenant)));
		knobProtectiveGroup.reset(nullptr);
		printf("Run test:%s Done.\n", tests[idx].title.toString().c_str());
		// do we handle a failure here?
	}

	printf("\n%d tests passed; %d tests failed.\n", passCount, failCount);

	// If the database was deleted during the workload we need to recreate the database
	if (tests.empty() || useDB) {
		if (waitForQuiescenceEnd) {
			printf("Waiting for DD to end...\n");
			try {
				wait(quietDatabase(cx, dbInfo, "End", 0, 2e6, 2e6) ||
				     (databasePingDelay == 0.0 ? Never()
				                               : testDatabaseLiveness(cx, databasePingDelay, "QuietDatabaseEnd")));
			} catch (Error& e) {
				TraceEvent("QuietDatabaseEndExternalError").error(e);
				throw;
			}
		}
	}
	printf("\n");

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
                            Optional<TenantName> defaultTenant) {
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

	wait(runTests(cc, ci, ts, tests, startingConfiguration, locality, defaultTenant));
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
                            Optional<TenantName> defaultTenant) {
	state std::vector<TestSpec> testSpecs;
	auto cc = makeReference<AsyncVar<Optional<ClusterControllerFullInterface>>>();
	auto ci = makeReference<AsyncVar<Optional<ClusterInterface>>>();
	std::vector<Future<Void>> actors;
	if (connRecord) {
		actors.push_back(reportErrors(monitorLeader(connRecord, cc), "MonitorLeader"));
		actors.push_back(reportErrors(extractClusterInterface(cc, ci), "ExtractClusterInterface"));
	}

	if (whatToRun == TEST_TYPE_CONSISTENCY_CHECK) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = LiteralStringRef("ConsistencyCheck");
		spec.databasePingDelay = 0;
		spec.timeout = 0;
		spec.waitForQuiescenceBegin = false;
		spec.waitForQuiescenceEnd = false;
		std::string rateLimitMax = format("%d", CLIENT_KNOBS->CONSISTENCY_CHECK_RATE_LIMIT_MAX);
		options.push_back_deep(options.arena(),
		                       KeyValueRef(LiteralStringRef("testName"), LiteralStringRef("ConsistencyCheck")));
		options.push_back_deep(options.arena(),
		                       KeyValueRef(LiteralStringRef("performQuiescentChecks"), LiteralStringRef("false")));
		options.push_back_deep(options.arena(),
		                       KeyValueRef(LiteralStringRef("distributed"), LiteralStringRef("false")));
		options.push_back_deep(options.arena(),
		                       KeyValueRef(LiteralStringRef("failureIsError"), LiteralStringRef("true")));
		options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("indefinite"), LiteralStringRef("true")));
		options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("rateLimitMax"), StringRef(rateLimitMax)));
		options.push_back_deep(options.arena(),
		                       KeyValueRef(LiteralStringRef("shuffleShards"), LiteralStringRef("true")));
		spec.options.push_back_deep(spec.options.arena(), options);
		testSpecs.push_back(spec);
	} else if (whatToRun == TEST_TYPE_UNIT_TESTS) {
		TestSpec spec;
		Standalone<VectorRef<KeyValueRef>> options;
		spec.title = LiteralStringRef("UnitTests");
		spec.startDelay = 0;
		spec.useDB = false;
		spec.timeout = 0;
		options.push_back_deep(options.arena(),
		                       KeyValueRef(LiteralStringRef("testName"), LiteralStringRef("UnitTests")));
		options.push_back_deep(options.arena(), KeyValueRef(LiteralStringRef("testsMatching"), fileName));
		// Add unit test options as test spec options
		for (auto& kv : testOptions.params) {
			options.push_back_deep(options.arena(), KeyValueRef(kv.first, kv.second));
		}
		spec.options.push_back_deep(spec.options.arena(), options);
		testSpecs.push_back(spec);
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
		if (boost::algorithm::ends_with(fileName, ".txt")) {
			testSpecs = readTests(ifs);
		} else if (boost::algorithm::ends_with(fileName, ".toml")) {
			// TOML is weird about opening the file as binary on windows, so we
			// just let TOML re-open the file instead of using ifs.
			testSpecs = readTOMLTests(fileName);
		} else {
			TraceEvent(SevError, "TestHarnessFail")
			    .detail("Reason", "unknown tests specification extension")
			    .detail("File", fileName.c_str());
			return Void();
		}
		ifs.close();
	}

	Future<Void> tests;
	if (at == TEST_HERE) {
		auto db = makeReference<AsyncVar<ServerDBInfo>>();
		std::vector<TesterInterface> iTesters(1);
		actors.push_back(
		    reportErrors(monitorServerDBInfo(cc, LocalityData(), db), "MonitorServerDBInfo")); // FIXME: Locality
		actors.push_back(reportErrors(testerServerCore(iTesters[0], connRecord, db, locality), "TesterServerCore"));
		tests = runTests(cc, ci, iTesters, testSpecs, startingConfiguration, locality, defaultTenant);
	} else {
		tests = reportErrors(
		    runTests(cc, ci, testSpecs, at, minTestersExpected, startingConfiguration, locality, defaultTenant),
		    "RunTests");
	}

	choose {
		when(wait(tests)) { return Void(); }
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
	ASSERT(details.count("TestDescription") == 0);
	ASSERT(details.count("ExpectedError") == 0);
	ASSERT(details.count("ExpectedErrorCode") == 0);
	ASSERT(details.count("ActualError") == 0);
	ASSERT(details.count("ActualErrorCode") == 0);
	ASSERT(details.count("Reason") == 0);

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
