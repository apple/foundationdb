/*
 * CoordinationInterface.h
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

#ifndef FDBCLIENT_COORDINATIONINTERFACE_H
#define FDBCLIENT_COORDINATIONINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/ClusterConnectionFile.h"

const int MAX_CLUSTER_FILE_BYTES = 60000;

struct ClientLeaderRegInterface {
	RequestStream< struct GetLeaderRequest > getLeader;
	RequestStream< struct OpenDatabaseCoordRequest > openDatabase;

	ClientLeaderRegInterface() {}
	ClientLeaderRegInterface( NetworkAddress remote );
	ClientLeaderRegInterface( INetwork* local );
};

struct LeaderInfo {
	constexpr static FileIdentifier file_identifier = 8338794;
	UID changeID;
	static const uint64_t mask = ~(127ll << 57);
	Value serializedInfo;
	bool forward;  // If true, serializedInfo is a connection string instead!

	LeaderInfo() : forward(false) {}
	LeaderInfo(UID changeID) : changeID(changeID), forward(false) {}

	bool operator < (LeaderInfo const& r) const { return changeID < r.changeID; }
	bool operator == (LeaderInfo const& r) const { return changeID == r.changeID; }

	// The first 7 bits of ChangeID represent cluster controller process class fitness, the lower the better
	void updateChangeID(ClusterControllerPriorityInfo info) {
		changeID = UID( ((uint64_t)info.processClassFitness << 57) | ((uint64_t)info.isExcluded << 60) | ((uint64_t)info.dcFitness << 61) | (changeID.first() & mask), changeID.second() );
	}

	// All but the first 7 bits are used to represent process id
	bool equalInternalId(LeaderInfo const& leaderInfo) const {
		return ((changeID.first() & mask) == (leaderInfo.changeID.first() & mask)) && changeID.second() == leaderInfo.changeID.second();
	}

	// Change leader only if
	// 1. the candidate has better process class fitness and the candidate is not the leader
	// 2. the leader process class fitness becomes worse
	bool leaderChangeRequired(LeaderInfo const& candidate) const {
		return ((changeID.first() & ~mask) > (candidate.changeID.first() & ~mask) && !equalInternalId(candidate)) || ((changeID.first() & ~mask) < (candidate.changeID.first() & ~mask) && equalInternalId(candidate));
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, changeID, serializedInfo, forward);
	}
};

struct GetLeaderRequest {
	constexpr static FileIdentifier file_identifier = 214727;
	Key key;
	UID knownLeader;
	ReplyPromise< Optional<LeaderInfo> > reply;

	GetLeaderRequest() {}
	explicit GetLeaderRequest(Key key, UID kl) : key(key), knownLeader(kl) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, knownLeader, reply);
	}
};

struct OpenDatabaseCoordRequest {
	constexpr static FileIdentifier file_identifier = 214728;
	// Sent by the native API to the coordinator to open a database and track client
	//   info changes.  Returns immediately if the current client info id is different from
	//   knownClientInfoID; otherwise returns when it next changes (or perhaps after a long interval)
	Key traceLogGroup;
	Standalone<VectorRef<StringRef>> issues;
	Standalone<VectorRef<ClientVersionRef>> supportedVersions;
	UID knownClientInfoID;
	Key clusterKey;
	vector<NetworkAddress> coordinators;
	ReplyPromise< CachedSerialization<struct ClientDBInfo> > reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, issues, supportedVersions, traceLogGroup, knownClientInfoID, clusterKey, coordinators, reply);
	}
};

class ClientCoordinators {
public:
	vector< ClientLeaderRegInterface > clientLeaderServers;
	Key clusterKey;
	Reference<ClusterConnectionFile> ccf; 

	explicit ClientCoordinators( Reference<ClusterConnectionFile> ccf );
	ClientCoordinators() {}
};

#endif
