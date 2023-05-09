/*
 * Recruiter.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include <vector>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/ClusterRecovery.actor.h"
#include "fdbserver/LogSystem.h"

//
// Handles recruitment for stateless roles plus the transaction logs.
// Recruitment consists of two main phases:
//
//   1. Find an optimal assignment of roles to workers
//   2. Send recruitment messages to the assigned workers
//
class Recruiter {
	friend class RecruiterImpl;

public:
	// Create assignments for each worker and recruit commit proxies, GRV
	// proxies, resolvers, and transaction logs.
	Future<std::vector<Standalone<CommitTransactionRef>>> recruitEverything(
	    Reference<ClusterRecoveryData> clusterRecoveryData,
	    std::vector<StorageServerInterface>* seedServers,
	    Reference<ILogSystem> oldLogSystem) const;

	// Get the encryption at rest mode from the database configuration.
	EncryptionAtRestMode getEncryptionAtRest(DatabaseConfiguration config) const;
};
