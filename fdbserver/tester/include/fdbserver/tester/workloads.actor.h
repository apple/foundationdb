/*
 * workloads.actor.h
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

#ifndef FDBSERVER_TESTER_WORKLOADS_ACTOR_H
#define FDBSERVER_TESTER_WORKLOADS_ACTOR_H
#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/DatabaseContext.h" // for clone()
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/core/WorkloadKeys.h"

#include <algorithm>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

struct WorkloadContext {
	Standalone<VectorRef<KeyValueRef>> options;
	int clientId, clientCount;
	int64_t sharedRandomNumber;
	Reference<AsyncVar<struct ServerDBInfo> const> dbInfo;
	Reference<IClusterConnectionRecord> ccr;
	std::vector<KeyRange> rangesToCheck; // for urgent consistency checker

	WorkloadContext();
	explicit(false) WorkloadContext(const WorkloadContext&);
	~WorkloadContext();
	WorkloadContext& operator=(const WorkloadContext&) = delete;
};

Value getOption(VectorRef<KeyValueRef> options, Key key, Value defaultValue);
int getOption(VectorRef<KeyValueRef> options, Key key, int defaultValue);
uint64_t getOption(VectorRef<KeyValueRef> options, Key key, uint64_t defaultValue);
int64_t getOption(VectorRef<KeyValueRef> options, Key key, int64_t defaultValue);
double getOption(VectorRef<KeyValueRef> options, Key key, double defaultValue);
bool getOption(VectorRef<KeyValueRef> options, Key key, bool defaultValue);
std::vector<std::string> getOption(VectorRef<KeyValueRef> options, Key key, std::vector<std::string> defaultValue);
std::vector<int> getOption(VectorRef<KeyValueRef> options, Key key, std::vector<int> defaultValue = {});
bool hasOption(VectorRef<KeyValueRef> options, Key key);
Future<Void> poisson(double* last, double meanInterval);
Future<Void> uniform(double* last, double meanInterval);
void emplaceIndex(uint8_t* data, int offset, int64_t index);

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
	virtual ~TestWorkload() {};
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

	explicit(false) TestWorkloadImpl(WorkloadContext const& wcx) : Workload(wcx) {}
	template <bool E = isFailureInjectionWorkload>
	TestWorkloadImpl(WorkloadContext const& wcx, std::enable_if_t<E, NoOptions> o) : Workload(wcx, o) {}

	std::string description() const override { return Workload::NAME; }
};

struct CompoundWorkload;
class DeterministicRandom;

struct FailureInjectionWorkload : TestWorkload {
	explicit(false) FailureInjectionWorkload(WorkloadContext const&);
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

	explicit(false) CompoundWorkload(WorkloadContext& wcx);
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
	double absentFrac, zeroPaddingRatio;

	explicit KVWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		nodeCount = getOption(options, "nodeCount"_sr, (uint64_t)100000);
		nodePrefix = getOption(options, "nodePrefix"_sr, (int64_t)-1);
		actorCount = getOption(options, "actorCount"_sr, 50);
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 4);
		maxValueBytes = getOption(options, "valueBytes"_sr, 96);
		minValueBytes = getOption(options, "minValueBytes"_sr, maxValueBytes);
		zeroPaddingRatio = getOption(options, "zeroPaddingRatio"_sr, 0.15);
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

	virtual Value randomValue() const {
		int length = deterministicRandom()->randomInt(minValueBytes, maxValueBytes + 1);
		int zeroPadding = static_cast<int>(zeroPaddingRatio * length);
		if (zeroPadding > length) {
			zeroPadding = length;
		}
		std::string valueString = deterministicRandom()->randomAlphaNumeric(length);
		for (int i = 0; i < zeroPadding; ++i) {
			valueString[i] = '\0';
		}
		return StringRef((uint8_t*)valueString.c_str(), length);
	}
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
	explicit(false) WorkloadFactory(UntrustedMode runInUntrustedClient = UntrustedMode::False)
	  : runInUntrustedClient(runInUntrustedClient) {
		auto& f = factories();
		std::string name = WorkloadType::NAME;
		ASSERT(!f.contains(name));
		f[name] = Reference<IWorkloadFactory>::addRef(this);
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

Future<Void> quietDatabase(Database const& cx,
                           Reference<AsyncVar<struct ServerDBInfo> const> const&,
                           std::string phase,
                           int64_t dataInFlightGate = 2e6,
                           int64_t maxTLogQueueGate = 5e6,
                           int64_t maxStorageServerQueueGate = 5e6,
                           int64_t maxDataDistributionQueueSize = 0,
                           int64_t maxPoppedVersionLag = 30e6,
                           int64_t maxVersionOffset = 1e6,
                           double maxDDRunTime = 0);

#endif
