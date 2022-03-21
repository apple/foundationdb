/*
 * workloads.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_WORKLOADS_ACTOR_G_H)
#define FDBSERVER_WORKLOADS_ACTOR_G_H
#include "fdbserver/workloads/workloads.actor.g.h"
#elif !defined(FDBSERVER_WORKLOADS_ACTOR_H)
#define FDBSERVER_WORKLOADS_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/DatabaseContext.h" // for clone()
#include "fdbserver/KnobProtectiveGroups.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h"

/*
 * Gets an Value from a list of key/value pairs, using a default value if the key is not present.
 */
Value getOption(VectorRef<KeyValueRef> options, Key key, Value defaultValue);
int getOption(VectorRef<KeyValueRef> options, Key key, int defaultValue);
uint64_t getOption(VectorRef<KeyValueRef> options, Key key, uint64_t defaultValue);
int64_t getOption(VectorRef<KeyValueRef> options, Key key, int64_t defaultValue);
double getOption(VectorRef<KeyValueRef> options, Key key, double defaultValue);
bool getOption(VectorRef<KeyValueRef> options, Key key, bool defaultValue);
std::vector<std::string> getOption(VectorRef<KeyValueRef> options,
                                   Key key,
                                   std::vector<std::string> defaultValue); // comma-separated strings
bool hasOption(VectorRef<KeyValueRef> options, Key key);

struct WorkloadContext {
	Standalone<VectorRef<KeyValueRef>> options;
	int clientId, clientCount;
	int64_t sharedRandomNumber;
	Reference<AsyncVar<struct ServerDBInfo> const> dbInfo;

	WorkloadContext();
	WorkloadContext(const WorkloadContext&);
	~WorkloadContext();
	WorkloadContext& operator=(const WorkloadContext&) = delete;
};

struct TestWorkload : NonCopyable, WorkloadContext, ReferenceCounted<TestWorkload> {
	int phases;

	// Subclasses are expected to also have a constructor with this signature (to work with WorkloadFactory<>):
	explicit TestWorkload(WorkloadContext const& wcx) : WorkloadContext(wcx) {
		bool runSetup = getOption(options, LiteralStringRef("runSetup"), true);
		phases = TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
		if (runSetup)
			phases |= TestWorkload::SETUP;
	}
	virtual ~TestWorkload(){};
	virtual std::string description() const = 0;
	virtual Future<Void> setup(Database const& cx) { return Void(); }
	virtual Future<Void> start(Database const& cx) = 0;
	virtual Future<bool> check(Database const& cx) = 0;
	virtual void getMetrics(std::vector<PerfMetric>& m) = 0;

	virtual double getCheckTimeout() const { return 3000; }

	enum WorkloadPhase { SETUP = 1, EXECUTION = 2, CHECK = 4, METRICS = 8 };
};

struct KVWorkload : TestWorkload {
	uint64_t nodeCount;
	int64_t nodePrefix;
	int actorCount, keyBytes, maxValueBytes, minValueBytes;
	double absentFrac;

	explicit KVWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nodeCount = getOption(options, LiteralStringRef("nodeCount"), (uint64_t)100000);
		nodePrefix = getOption(options, LiteralStringRef("nodePrefix"), (int64_t)-1);
		actorCount = getOption(options, LiteralStringRef("actorCount"), 50);
		keyBytes = std::max(getOption(options, LiteralStringRef("keyBytes"), 16), 4);
		maxValueBytes = getOption(options, LiteralStringRef("valueBytes"), 96);
		minValueBytes = getOption(options, LiteralStringRef("minValueBytes"), maxValueBytes);
		ASSERT(minValueBytes <= maxValueBytes);

		absentFrac = getOption(options, LiteralStringRef("absentFrac"), 0.0);
	}
	Key getRandomKey() const;
	Key getRandomKey(double absentFrac) const;
	Key getRandomKey(bool absent) const;
	Key keyForIndex(uint64_t index) const;
	Key keyForIndex(uint64_t index, bool absent) const;
};

struct IWorkloadFactory : ReferenceCounted<IWorkloadFactory> {
	static Reference<TestWorkload> create(std::string const& name, WorkloadContext const& wcx) {
		auto it = factories().find(name);
		if (it == factories().end())
			return {}; // or throw?
		return it->second->create(wcx);
	}
	static std::map<std::string, Reference<IWorkloadFactory>>& factories() {
		static std::map<std::string, Reference<IWorkloadFactory>> theFactories;
		return theFactories;
	}

	virtual ~IWorkloadFactory() = default;
	virtual Reference<TestWorkload> create(WorkloadContext const& wcx) = 0;
};

template <class WorkloadType>
struct WorkloadFactory : IWorkloadFactory {
	WorkloadFactory(const char* name) { factories()[name] = Reference<IWorkloadFactory>::addRef(this); }
	Reference<TestWorkload> create(WorkloadContext const& wcx) override { return makeReference<WorkloadType>(wcx); }
};

#define REGISTER_WORKLOAD(classname) WorkloadFactory<classname> classname##WorkloadFactory(#classname)

struct DistributedTestResults {
	std::vector<PerfMetric> metrics;
	int successes, failures;

	DistributedTestResults() {}

	DistributedTestResults(std::vector<PerfMetric> const& metrics, int successes, int failures)
	  : metrics(metrics), successes(successes), failures(failures) {}

	bool ok() const { return successes && !failures; }
};

class TestSpec {
public:
	TestSpec() {
		title = StringRef();
		dumpAfterTest = false;
		clearAfterTest = g_network->isSimulated();
		useDB = true;
		startDelay = 30.0;
		phases = TestWorkload::SETUP | TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
		timeout = g_network->isSimulated() ? 15000 : 1500;
		databasePingDelay = g_network->isSimulated() ? 0.0 : 15.0;
		runConsistencyCheck = g_network->isSimulated();
		runConsistencyCheckOnCache = false;
		runConsistencyCheckOnTSS = true;
		waitForQuiescenceBegin = true;
		waitForQuiescenceEnd = true;
		simCheckRelocationDuration = false;
		simConnectionFailuresDisableDuration = 0;
		simBackupAgents = ISimulator::BackupAgentType::NoBackupAgents;
		simDrAgents = ISimulator::BackupAgentType::NoBackupAgents;
		restorePerpetualWiggleSetting = true;
	}
	TestSpec(StringRef title,
	         bool dump,
	         bool clear,
	         double startDelay = 30.0,
	         bool useDB = true,
	         double databasePingDelay = -1.0)
	  : title(title), dumpAfterTest(dump), clearAfterTest(clear), useDB(useDB), startDelay(startDelay), timeout(600),
	    databasePingDelay(databasePingDelay), runConsistencyCheck(g_network->isSimulated()),
	    runConsistencyCheckOnCache(false), runConsistencyCheckOnTSS(false), waitForQuiescenceBegin(true),
	    waitForQuiescenceEnd(true), restorePerpetualWiggleSetting(true), simCheckRelocationDuration(false),
	    simConnectionFailuresDisableDuration(0), simBackupAgents(ISimulator::BackupAgentType::NoBackupAgents),
	    simDrAgents(ISimulator::BackupAgentType::NoBackupAgents) {
		phases = TestWorkload::SETUP | TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
		if (databasePingDelay < 0)
			databasePingDelay = g_network->isSimulated() ? 0.0 : 15.0;
	}

	Standalone<StringRef> title;
	bool dumpAfterTest;
	bool clearAfterTest;
	bool useDB;
	double startDelay;
	int phases;
	Standalone<VectorRef<VectorRef<KeyValueRef>>> options;
	int timeout;
	double databasePingDelay;
	bool runConsistencyCheck;
	bool runConsistencyCheckOnCache;
	bool runConsistencyCheckOnTSS;
	bool waitForQuiescenceBegin;
	bool waitForQuiescenceEnd;
	bool restorePerpetualWiggleSetting; // whether set perpetual_storage_wiggle as the value after run
	                                    // QuietDatabase. QuietDatabase always disables perpetual storage wiggle on
	                                    // purpose. If waitForQuiescenceBegin == true and we want to keep perpetual
	                                    // storage wiggle the same setting as before during testing, this value should
	                                    // be set true.

	bool simCheckRelocationDuration; // If set to true, then long duration relocations generate SevWarnAlways messages.
	                                 // Once any workload sets this to true, it will be true for the duration of the
	                                 // program.  Can only be used in simulation.
	double simConnectionFailuresDisableDuration;
	ISimulator::BackupAgentType simBackupAgents; // If set to true, then the simulation runs backup agents on the
	                                             // workers. Can only be used in simulation.
	ISimulator::BackupAgentType simDrAgents;

	KnobKeyValuePairs overrideKnobs;
};

ACTOR Future<DistributedTestResults> runWorkload(Database cx,
                                                 std::vector<TesterInterface> testers,
                                                 TestSpec spec,
                                                 Optional<TenantName> defaultTenant);

void logMetrics(std::vector<PerfMetric> metrics);

ACTOR Future<Void> poisson(double* last, double meanInterval);
ACTOR Future<Void> uniform(double* last, double meanInterval);

void emplaceIndex(uint8_t* data, int offset, int64_t index);
Key doubleToTestKey(double p);
double testKeyToDouble(const KeyRef& p);
Key doubleToTestKey(double p, const KeyRef& prefix);
double testKeyToDouble(const KeyRef& p, const KeyRef& prefix);

ACTOR Future<Void> databaseWarmer(Database cx);

Future<Void> quietDatabase(Database const& cx,
                           Reference<AsyncVar<struct ServerDBInfo> const> const&,
                           std::string phase,
                           int64_t dataInFlightGate = 2e6,
                           int64_t maxTLogQueueGate = 5e6,
                           int64_t maxStorageServerQueueGate = 5e6,
                           int64_t maxDataDistributionQueueSize = 0,
                           int64_t maxPoppedVersionLag = 30e6);

/**
 * A utility function for testing error situations. It succeeds if the given test
 * throws an error. If expectedError is provided, it additionally checks if the
 * error code is as expected.
 *
 * In case of a failure, logs an corresponding error in the trace with the given
 * description and details, sets the given success flag to false (optional)
 * and throws the given exception (optional). Note that in case of a successful
 * test execution, the success flag is kept unchanged.
 */
Future<Void> testExpectedError(Future<Void> test,
                               const char* testDescr,
                               Optional<Error> expectedError = Optional<Error>(),
                               Optional<bool*> successFlag = Optional<bool*>(),
                               std::map<std::string, std::string> details = {},
                               Optional<Error> throwOnError = Optional<Error>(),
                               UID id = UID());

#include "flow/unactorcompiler.h"

#endif
