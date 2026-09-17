/*
 * SimulationCapabilities.h
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

#ifndef FDBCLIENT_SIMULATIONCAPABILITIES_H
#define FDBCLIENT_SIMULATIONCAPABILITIES_H
#pragma once

#include "fdbrpc/simulator.h"

enum class FDBSimulationCapability {
	WarnOnStorageMismatch,
	StorageReplicaFaultInjection,
	StorageReplicaDelay,
	StorageReplicaMutationDrop,
	LimitStorageServerReadBytes
};

class IFDBSimulationPolicy : public ISimulationPolicy {
public:
	~IFDBSimulationPolicy() override;
	virtual bool hasCapability(FDBSimulationCapability capability) const = 0;
};

// Capabilities are disabled outside simulation and when the active policy does not implement the FDB extension.
inline bool fdbSimulationHasCapability(FDBSimulationCapability capability) {
	if (!g_network || !g_network->isSimulated() || !g_simulator) {
		return false;
	}
	const auto policy = g_simulator->getSimulationPolicy();
	const auto* fdbPolicy = dynamic_cast<const IFDBSimulationPolicy*>(policy.getPtr());
	return fdbPolicy && fdbPolicy->hasCapability(capability);
}

#endif
