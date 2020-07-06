/*
 * ServerDBInfo.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_SERVERDBINFO_ACTOR_G_H)
#define FDBSERVER_SERVERDBINFO_ACTOR_G_H
#include "fdbserver/ServerDBInfo.actor.g.h"
#elif !defined(FDBSERVER_SERVERDBINFO_ACTOR_H)
#define FDBSERVER_SERVERDBINFO_ACTOR_H
#define FDBSERVER_SERVERDBINFO_H
#pragma once

#include "fdbserver/DataDistributorInterface.h"
#include "fdbserver/MasterInterface.h"
#include "fdbserver/LogSystemConfig.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/LatencyBandConfig.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// TODO: May add TXN lifetime here
struct ServerDBInfo {
	constexpr static FileIdentifier file_identifier = 13838807;
	// This structure contains transient information which is broadcast to all workers for a database,
	// permitting them to communicate with each other.  It is not available to the client.  This mechanism
	// (see GetServerDBInfoRequest) is closely parallel to OpenDatabaseRequest for the client.

	UID id;  // Changes each time any other member changes
	ClusterControllerFullInterface clusterInterface;
	ClientDBInfo client;           // After a successful recovery, eventually proxies that communicate with it
	Optional<DataDistributorInterface> distributor;  // The best guess of current data distributor.
	MasterInterface master;        // The best guess as to the most recent master, which might still be recovering
	Optional<RatekeeperInterface> ratekeeper;
	std::vector<ResolverInterface> resolvers;
	DBRecoveryCount recoveryCount; // A recovery count from DBCoreState.  A successful master recovery increments it twice; unsuccessful recoveries may increment it once. Depending on where the current master is in its recovery process, this might not have been written by the current master.
	RecoveryState recoveryState;
	LifetimeToken masterLifetime;  // Used by masterserver to detect not being the currently chosen master
	LocalityData myLocality;       // (Not serialized) Locality information, if available, for the *local* process
	LogSystemConfig logSystemConfig;
	std::vector<UID> priorCommittedLogServers;   // If !fullyRecovered and logSystemConfig refers to a new log system which may not have been committed to the coordinated state yet, then priorCommittedLogServers are the previous, fully committed generation which need to stay alive in case this recovery fails
	Optional<LatencyBandConfig> latencyBandConfig;
	int64_t infoGeneration;
	Version readTxnLifetime;

	ServerDBInfo() : recoveryCount(0), recoveryState(RecoveryState::UNINITIALIZED), logSystemConfig(0), infoGeneration(0) {}

	bool operator == (ServerDBInfo const& r) const { return id == r.id; }
	bool operator != (ServerDBInfo const& r) const { return id != r.id; }

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, id, clusterInterface, client, distributor, master, ratekeeper, resolvers, recoveryCount, recoveryState, masterLifetime, logSystemConfig, priorCommittedLogServers, latencyBandConfig, infoGeneration, readTxnLifetime);
	}
};

struct UpdateServerDBInfoRequest {
	constexpr static FileIdentifier file_identifier = 9467438;
	Standalone<StringRef> serializedDbInfo;
	std::vector<Endpoint> broadcastInfo;
	ReplyPromise<std::vector<Endpoint>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, serializedDbInfo, broadcastInfo, reply);
	}
};

struct GetServerDBInfoRequest {
	constexpr static FileIdentifier file_identifier = 9467439;
	UID knownServerInfoID;
	ReplyPromise<struct ServerDBInfo> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, knownServerInfoID, reply);
	}
};


ACTOR Future<Void> broadcastTxnRequest(TxnStateRequest req, int sendAmount, bool sendReply);

ACTOR Future<std::vector<Endpoint>> broadcastDBInfoRequest(UpdateServerDBInfoRequest req, int sendAmount, Optional<Endpoint> sender, bool sendReply);

#include "flow/unactorcompiler.h"
#endif
