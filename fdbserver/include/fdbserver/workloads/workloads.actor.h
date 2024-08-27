/*
 * workloads.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_WORKLOADS_ACTOR_G_H)
#define FDBSERVER_WORKLOADS_ACTOR_G_H
#include "fdbserver/workloads/workloads.actor.g.h"
#elif !defined(FDBSERVER_WORKLOADS_ACTOR_H)
#define FDBSERVER_WORKLOADS_ACTOR_H

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/DatabaseContext.h" // for clone()
#include "fdbserver/KnobProtectiveGroups.h"
#include "fdbserver/TesterInterface.h"
#include "fdbrpc/simulator.h"

#include <functional>

#include "flow/actorcompiler.h" // has to be last import

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
std::vector<int> getOption(VectorRef<KeyValueRef> options,
                           Key key,
                           std::vector<int> defaultValue = {}); // comma-separated integers
bool hasOption(VectorRef<KeyValueRef> options, Key key);

struct WorkloadContext {
	Standalone<VectorRef<KeyValueRef>> options;
	int clientId, clientCount;
	int64_t sharedRandomNumber;
	Reference<AsyncVar<struct ServerDBInfo> const> dbInfo;
	Reference<IClusterConnectionRecord> ccr;
	Optional<TenantName> defaultTenant;
	std::vector<KeyRange> rangesToCheck; // for urgent consistency checker

	WorkloadContext();
	WorkloadContext(const WorkloadContext&);
	~WorkloadContext();
	WorkloadContext& operator=(const WorkloadContext&) = delete;
};

struct TestWorkload : NonCopyable, WorkloadContext, ReferenceCounted<TestWorkload> {
	// Implementations of TestWorkload need to provide their name by defining a static member variable called name:
	// static constexpr const char* name = "WorkloadName";
	int phases;

	// Subclasses are expected to also have a constructor with this signature (to work with WorkloadFactory<>):
	explicit TestWorkload(WorkloadContext const& wcx) : WorkloadContext(wcx) {
		bool runSetup = getOption(options, "runSetup"_sr, true);
		phases = TestWorkload::EXECUTION | TestWorkload::CHECK | TestWorkload::METRICS;
		if (runSetup)
			phases |= TestWorkload::SETUP;
	}
	virtual ~TestWorkload(){};
	virtual Future<Void> initialized() { return Void(); }
	// WARNING: this method must not be implemented by a workload directly. Instead, this will be implemented by
	// the workload factory. Instead, provide a static member variable called name.
	virtual std::string description() const = 0;
	virtual void disableFailureInjectionWorkloads(std::set<std::string>& out) const;
	virtual Future<Void> setup(Database const& cx) { return Void(); }
	virtual Future<Void> start(Database const& cx) = 0;
	virtual Future<bool> check(Database const& cx) = 0;
	virtual Future<std::vector<PerfMetric>> getMetrics() {
		std::vector<PerfMetric> result;
		getMetrics(result);
		return result;
	}

	virtual double getCheckTimeout() const { return 3000; }

	enum WorkloadPhase { SETUP = 1, EXECUTION = 2, CHECK = 4, METRICS = 8 };

private:
	virtual void getMetrics(std::vector<PerfMetric>& m) = 0;
};

struct NoOptions {};

template <class Workload, bool isFailureInjectionWorkload = false>
struct TestWorkloadImpl : Workload {
	static_assert(std::is_convertible_v<Workload&, TestWorkload&>);
	static_assert(std::is_convertible_v<decltype(Workload::NAME), std::string>,
	              "Workload must have a static member `name` which is convertible to string");
	static_assert(std::is_same_v<decltype(&TestWorkload::description), decltype(&Workload::description)>,
	              "Workload must not override TestWorkload::description");

	TestWorkloadImpl(WorkloadContext const& wcx) : Workload(wcx) {}
	template <bool E = isFailureInjectionWorkload>
	TestWorkloadImpl(WorkloadContext const& wcx, std::enable_if_t<E, NoOptions> o) : Workload(wcx, o) {}

	std::string description() const override { return Workload::NAME; }
};

struct CompoundWorkload;
class DeterministicRandom;

struct FailureInjectionWorkload : TestWorkload {
	FailureInjectionWorkload(WorkloadContext const&);
	virtual ~FailureInjectionWorkload() {}
	virtual void initFailureInjectionMode(DeterministicRandom& random);
	virtual bool shouldInject(DeterministicRandom& random, const WorkloadRequest& work, const unsigned count) const;

	Future<Void> setupInjectionWorkload(Database const& cx, Future<Void> done);
	Future<Void> startInjectionWorkload(Database const& cx, Future<Void> done);
	Future<bool> checkInjectionWorkload(Database const& cx, Future<bool> done);
};

struct IFailureInjectorFactory : ReferenceCounted<IFailureInjectorFactory> {
	virtual ~IFailureInjectorFactory() = default;
	static std::vector<Reference<IFailureInjectorFactory>>& factories() {
		static std::vector<Reference<IFailureInjectorFactory>> _factories;
		return _factories;
	}
	virtual Reference<FailureInjectionWorkload> create(WorkloadContext const& wcx) = 0;
};

template <class W>
struct FailureInjectorFactory : IFailureInjectorFactory {
	static_assert(std::is_base_of<FailureInjectionWorkload, W>::value);
	FailureInjectorFactory() {
		IFailureInjectorFactory::factories().push_back(Reference<IFailureInjectorFactory>::addRef(this));
	}
	Reference<FailureInjectionWorkload> create(WorkloadContext const& wcx) override {
		return makeReference<TestWorkloadImpl<W, true>>(wcx, NoOptions());
	}
};

struct CompoundWorkload : TestWorkload {
	std::vector<Reference<TestWorkload>> workloads;
	std::vector<Reference<FailureInjectionWorkload>> failureInjection;

	CompoundWorkload(WorkloadContext& wcx);
	CompoundWorkload* add(Reference<TestWorkload>&& w);
	void addFailureInjection(WorkloadRequest& work);
	bool shouldInjectFailure(DeterministicRandom& random,
	                         const WorkloadRequest& work,
	                         Reference<FailureInjectionWorkload> failureInjection) const;

	std::string description() const override;

	Future<Void> setup(Database const& cx) override;
	Future<Void> start(Database const& cx) override;
	Future<bool> check(Database const& cx) override;

	Future<std::vector<PerfMetric>> getMetrics() override;
	double getCheckTimeout() const override;

	void getMetrics(std::vector<PerfMetric>&) override;
};

struct WorkloadProcess;
struct ClientWorkload : TestWorkload {
	WorkloadProcess* impl;
	using CreateWorkload = std::function<Reference<TestWorkload>(WorkloadContext const&)>;
	ClientWorkload(CreateWorkload const& childCreator, WorkloadContext const& wcx);
	~ClientWorkload();
	Future<Void> initialized() override;
	std::string description() const override;
	Future<Void> setup(Database const& cx) override;
	Future<Void> start(Database const& cx) override;
	Future<bool> check(Database const& cx) override;
	void getMetrics(std::vector<PerfMetric>& m) override;
	Future<std::vector<PerfMetric>> getMetrics() override;

	double getCheckTimeout() const override;
};

struct KVWorkload : TestWorkload {
	uint64_t nodeCount;
	int64_t nodePrefix;
	int actorCount, keyBytes, maxValueBytes, minValueBytes;
	double absentFrac;

	explicit KVWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nodeCount = getOption(options, "nodeCount"_sr, (uint64_t)100000);
		nodePrefix = getOption(options, "nodePrefix"_sr, (int64_t)-1);
		actorCount = getOption(options, "actorCount"_sr, 50);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 4);
		maxValueBytes = getOption(options, "valueBytes"_sr, 96);
		minValueBytes = getOption(options, "minValueBytes"_sr, maxValueBytes);
		ASSERT(minValueBytes <= maxValueBytes);

		absentFrac = getOption(options, "absentFrac"_sr, 0.0);
	}
	Key getRandomKey() const;
	Key getRandomKey(double absentFrac) const;
	Key getRandomKey(bool absent) const;
	Key keyForIndex(uint64_t index) const;
	Key keyForIndex(uint64_t index, bool absent) const;
	// the reverse process of keyForIndex() without division. Set absent=true to ignore the last byte in Key
	int64_t indexForKey(const KeyRef& key, bool absent = false) const;
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

FDB_BOOLEAN_PARAM(UntrustedMode);

template <class Workload>
struct WorkloadFactory : IWorkloadFactory {
	static_assert(std::is_convertible_v<decltype(Workload::NAME), std::string>,
	              "Each workload must have a Workload::NAME member");
	using WorkloadType = TestWorkloadImpl<Workload>;
	bool runInUntrustedClient;
	WorkloadFactory(UntrustedMode runInUntrustedClient = UntrustedMode::False)
	  : runInUntrustedClient(runInUntrustedClient) {
		factories()[WorkloadType::NAME] = Reference<IWorkloadFactory>::addRef(this);
	}
	Reference<TestWorkload> create(WorkloadContext const& wcx) override {
		if (g_network->isSimulated() && runInUntrustedClient) {
			return makeReference<ClientWorkload>(
			    [](WorkloadContext const& wcx) { return makeReference<WorkloadType>(wcx); }, wcx);
		}
		return makeReference<WorkloadType>(wcx);
	}
};

#define REGISTER_WORKLOAD(classname) WorkloadFactory<classname> classname##WorkloadFactory

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
	bool runFailureWorkloads = true;
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
	std::vector<std::string> disabledFailureInjectionWorkloads;
};

Future<DistributedTestResults> runWorkload(Database cx,
                                           std::vector<TesterInterface> testers,
                                           TestSpec spec,
                                           Optional<TenantName> defaultTenant);

void logMetrics(std::vector<PerfMetric> metrics);

Future<Void> poisson(double* last, double meanInterval);
Future<Void> uniform(double* last, double meanInterval);

void emplaceIndex(uint8_t* data, int offset, int64_t index);
Key doubleToTestKey(double p);
double testKeyToDouble(const KeyRef& p);
Key doubleToTestKey(double p, const KeyRef& prefix);
double testKeyToDouble(const KeyRef& p, const KeyRef& prefix);

Future<Void> databaseWarmer(Database cx);

Future<Void> quietDatabase(Database const& cx,
                           Reference<AsyncVar<struct ServerDBInfo> const> const&,
                           std::string phase,
                           int64_t dataInFlightGate = 2e6,
                           int64_t maxTLogQueueGate = 5e6,
                           int64_t maxStorageServerQueueGate = 5e6,
                           int64_t maxDataDistributionQueueSize = 0,
                           int64_t maxPoppedVersionLag = 30e6,
                           int64_t maxVersionOffset = 1e6);

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

std::string getTestEncryptionFileName();

// This should become a BehaviorInjectionWorkload or perhaps ConfigInjectionWorkload which should be a new class that
// should represent non-failure behaviors that can be randomly injected into any test run.
ACTOR Future<Void> customShardConfigWorkload(Database cx);

#include "flow/unactorcompiler.h"

#endif
