/*
 * ServerDBInfo.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_SERVERDBINFO_H
#define FDBSERVER_SERVERDBINFO_H
#pragma once

#include "fdbserver/ClusterRecruitmentInterface.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/RecoveryState.h"

struct ServerDBInfo {
	// This structure contains transient information which is broadcast to all workers for a database,
	// permitting them to communicate with each other.  It is not available to the client.  This mechanism
	// (see GetServerDBInfoRequest) is closely parallel to OpenDatabaseRequest for the client.

	UID id;  // Changes each time any other member changes
	ClusterControllerFullInterface clusterInterface;
	ClientDBInfo client;           // After a successful recovery, eventually proxies that communicate with it
	MasterInterface master;        // The best guess as to the most recent master, which might still be recovering
	vector<ResolverInterface> resolvers;
	DBRecoveryCount recoveryCount; // A recovery count from DBCoreState.  A successful master recovery increments it twice; unsuccessful recoveries may increment it once. Depending on where the current master is in its recovery process, this might not have been written by the current master.
	RecoveryState recoveryState;
	LifetimeToken masterLifetime;  // Used by masterserver to detect not being the currently chosen master
	LocalityData myLocality;       // (Not serialized) Locality information, if available, for the *local* process
	LogSystemConfig logSystemConfig;
	std::vector<UID> priorCommittedLogServers;   // If !fullyRecovered and logSystemConfig refers to a new log system which may not have been committed to the coordinated state yet, then priorCommittedLogServers are the previous, fully committed generation which need to stay alive in case this recovery fails

	explicit ServerDBInfo() : recoveryCount(0), recoveryState(RecoveryState::UNINITIALIZED) {}

	bool operator == (ServerDBInfo const& r) const { return id == r.id; }
	bool operator != (ServerDBInfo const& r) const { return id != r.id; }

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, id, clusterInterface, client, master, resolvers, recoveryCount, masterLifetime, logSystemConfig, priorCommittedLogServers, recoveryState);
	}
};

#endif
