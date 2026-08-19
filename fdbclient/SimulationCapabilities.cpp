/*
 * SimulationCapabilities.cpp
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

#include "fdbclient/SimulationCapabilities.h"
#include "flow/UnitTest.h"

#include <array>
#include <utility>

IFDBSimulationPolicy::~IFDBSimulationPolicy() = default;

namespace {

constexpr std::array capabilities = { FDBSimulationCapability::WarnOnStorageMismatch,
	                                  FDBSimulationCapability::StorageReplicaFaultInjection,
	                                  FDBSimulationCapability::StorageReplicaDelay,
	                                  FDBSimulationCapability::StorageReplicaMutationDrop,
	                                  FDBSimulationCapability::LimitStorageServerReadBytes };

void assertAllCapabilities(bool expected) {
	for (auto capability : capabilities) {
		ASSERT_EQ(fdbSimulationHasCapability(capability), expected);
	}
}

class SimulationPolicyRestore final : NonCopyable {
public:
	SimulationPolicyRestore()
	  : network(g_network), simulator(g_simulator),
	    policy(simulator ? simulator->getSimulationPolicy() : Reference<ISimulationPolicy>()) {}

	~SimulationPolicyRestore() {
		g_network = network;
		g_simulator = simulator;
		if (simulator) {
			simulator->setSimulationPolicy(policy);
		}
	}

private:
	INetwork* network;
	ISimulator* simulator;
	Reference<ISimulationPolicy> policy;
};

// Test policy that returns the same fixed answer for every capability.
class TestCapabilityPolicy final : public IFDBSimulationPolicy {
public:
	explicit TestCapabilityPolicy(bool enabled) : enabled(enabled) {}
	bool hasCapability(FDBSimulationCapability) const override { return enabled; }

private:
	const bool enabled;
};

struct ReentrantPolicyLifetime {
	int destructions = 0;
	bool queryInProgress = false;
	bool destroyedDuringQuery = false;
};

// Replaces itself during a capability query to verify that the queried policy stays alive
// until the call returns.
class ReplacingCapabilityPolicy final : public IFDBSimulationPolicy {
public:
	ReplacingCapabilityPolicy(ISimulator* simulator,
	                          Reference<ISimulationPolicy> replacement,
	                          ReentrantPolicyLifetime* lifetime)
	  : simulator(simulator), replacement(std::move(replacement)), lifetime(lifetime) {}

	~ReplacingCapabilityPolicy() override {
		++lifetime->destructions;
		lifetime->destroyedDuringQuery = lifetime->queryInProgress;
	}

	bool hasCapability(FDBSimulationCapability) const override {
		// Keep the test safe even if replacing the policy destroys this object during the query.
		auto* const queryLifetime = lifetime;
		auto* const activeSimulator = simulator;
		const auto nextPolicy = replacement;
		queryLifetime->queryInProgress = true;
		activeSimulator->setSimulationPolicy(nextPolicy);
		const bool stillAlive = queryLifetime->destructions == 0;
		queryLifetime->queryInProgress = false;
		return stillAlive;
	}

private:
	ISimulator* simulator;
	Reference<ISimulationPolicy> replacement;
	ReentrantPolicyLifetime* lifetime;
};

} // namespace

TEST_CASE("/fdbclient/SimulationCapabilities/Defaults") {
	SimulationPolicyRestore restore;
	auto* const network = g_network;
	auto* const simulator = g_simulator;
	ASSERT(network);

	if (!network->isSimulated()) {
		assertAllCapabilities(false);
		g_network = nullptr;
		assertAllCapabilities(false);
		g_network = network;
		return Void();
	}

	ASSERT(simulator);
	simulator->setSimulationPolicy({});
	assertAllCapabilities(false);
	simulator->setSimulationPolicy(makeReference<ISimulationPolicy>());
	assertAllCapabilities(false);
	simulator->setSimulationPolicy(makeReference<TestCapabilityPolicy>(true));
	assertAllCapabilities(true);

	g_network = nullptr;
	assertAllCapabilities(false);
	g_network = network;
	g_simulator = nullptr;
	assertAllCapabilities(false);
	g_simulator = simulator;
	assertAllCapabilities(true);
	return Void();
}

TEST_CASE("/fdbclient/SimulationCapabilities/ReentrantReplacement") {
	ASSERT(g_network);
	if (!g_network->isSimulated()) {
		assertAllCapabilities(false);
		return Void();
	}

	ReentrantPolicyLifetime lifetime;
	SimulationPolicyRestore restore;
	auto* const simulator = g_simulator;
	ASSERT(simulator);
	auto replacement = makeReference<TestCapabilityPolicy>(false);
	simulator->setSimulationPolicy(makeReference<ReplacingCapabilityPolicy>(simulator, replacement, &lifetime));

	ASSERT(fdbSimulationHasCapability(FDBSimulationCapability::WarnOnStorageMismatch));
	ASSERT_EQ(lifetime.destructions, 1);
	ASSERT(!lifetime.destroyedDuringQuery);
	ASSERT(simulator->getSimulationPolicy().getPtr() == replacement.getPtr());
	assertAllCapabilities(false);
	return Void();
}
