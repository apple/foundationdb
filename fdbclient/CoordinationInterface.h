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
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ClusterInterface.h"
#include "fdbclient/WellKnownEndpoints.h"

const int MAX_CLUSTER_FILE_BYTES = 60000;

struct ClientLeaderRegInterface {
	RequestStream<struct GetLeaderRequest> getLeader;
	RequestStream<struct OpenDatabaseCoordRequest> openDatabase;
	RequestStream<struct CheckDescriptorMutableRequest> checkDescriptorMutable;

	ClientLeaderRegInterface() {}
	ClientLeaderRegInterface(NetworkAddress remote);
	ClientLeaderRegInterface(INetwork* local);

	bool operator==(const ClientLeaderRegInterface& rhs) const {
		return getLeader == rhs.getLeader && openDatabase == rhs.openDatabase;
	}
};

class ClusterConnectionString {
public:
	ClusterConnectionString() {}
	ClusterConnectionString(std::string const& connectionString);
	ClusterConnectionString(std::vector<NetworkAddress>, Key);
	std::vector<NetworkAddress> const& coordinators() const { return coord; }
	Key clusterKey() const { return key; }
	Key clusterKeyName() const {
		return keyDesc;
	} // Returns the "name" or "description" part of the clusterKey (the part before the ':')
	std::string toString() const;
	static std::string getErrorString(std::string const& source, Error const& e);

private:
	void parseKey(std::string const& key);

	std::vector<NetworkAddress> coord;
	Key key, keyDesc;
};

class IClusterConnectionRecord {
public:
	IClusterConnectionRecord(bool connectionStringNeedsPersisted)
	  : connectionStringNeedsPersisted(connectionStringNeedsPersisted) {}
	virtual ~IClusterConnectionRecord() {}

	virtual ClusterConnectionString const& getConnectionString() const = 0;
	virtual Future<Void> setConnectionString(ClusterConnectionString const&) = 0;
	virtual Future<ClusterConnectionString> getStoredConnectionString() = 0;

	Future<bool> upToDate();
	virtual Future<bool> upToDate(ClusterConnectionString& connectionString) = 0;

	virtual Standalone<StringRef> getLocation() const = 0;
	virtual Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const = 0;

	virtual bool isValid() const = 0;
	virtual std::string toString() const = 0;

	void notifyConnected();

	virtual void addref() = 0;
	virtual void delref() = 0;

protected:
	virtual Future<bool> persist() = 0;

	bool needsToBePersisted() const;
	void setPersisted();

private:
	bool connectionStringNeedsPersisted;
};

struct LeaderInfo {
	constexpr static FileIdentifier file_identifier = 8338794;
	// The first 7 bits of changeID represent cluster controller process class fitness, the lower the better
	UID changeID;
	static const uint64_t changeIDMask = ~(uint64_t(0b1111111) << 57);
	Value serializedInfo;
	bool forward; // If true, serializedInfo is a connection string instead!

	LeaderInfo() : forward(false) {}
	LeaderInfo(UID changeID) : changeID(changeID), forward(false) {}

	bool operator<(LeaderInfo const& r) const { return changeID < r.changeID; }
	bool operator>(LeaderInfo const& r) const { return r < *this; }
	bool operator<=(LeaderInfo const& r) const { return !(*this > r); }
	bool operator>=(LeaderInfo const& r) const { return !(*this < r); }
	bool operator==(LeaderInfo const& r) const { return changeID == r.changeID; }
	bool operator!=(LeaderInfo const& r) const { return !(*this == r); }

	// The first 7 bits of ChangeID represent cluster controller process class fitness, the lower the better
	void updateChangeID(ClusterControllerPriorityInfo info) {
		changeID = UID(((uint64_t)info.processClassFitness << 57) | ((uint64_t)info.isExcluded << 60) |
		                   ((uint64_t)info.dcFitness << 61) | (changeID.first() & changeIDMask),
		               changeID.second());
	}

	// All but the first 7 bits are used to represent process id
	bool equalInternalId(LeaderInfo const& leaderInfo) const {
		return ((changeID.first() & changeIDMask) == (leaderInfo.changeID.first() & changeIDMask)) &&
		       changeID.second() == leaderInfo.changeID.second();
	}

	// Change leader only if
	// 1. the candidate has better process class fitness and the candidate is not the leader
	// 2. the leader process class fitness becomes worse
	bool leaderChangeRequired(LeaderInfo const& candidate) const {
		return ((changeID.first() & ~changeIDMask) > (candidate.changeID.first() & ~changeIDMask) &&
		        !equalInternalId(candidate)) ||
		       ((changeID.first() & ~changeIDMask) < (candidate.changeID.first() & ~changeIDMask) &&
		        equalInternalId(candidate));
	}

	ClusterControllerPriorityInfo getPriorityInfo() const {
		ClusterControllerPriorityInfo info;
		info.processClassFitness = (changeID.first() >> 57) & 7;
		info.isExcluded = (changeID.first() >> 60) & 1;
		info.dcFitness = (changeID.first() >> 61) & 7;
		return info;
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
	ReplyPromise<Optional<LeaderInfo>> reply;

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
	std::vector<NetworkAddress> coordinators;
	ReplyPromise<CachedSerialization<struct ClientDBInfo>> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, issues, supportedVersions, traceLogGroup, knownClientInfoID, clusterKey, coordinators, reply);
	}
};

class ClientCoordinators {
public:
	std::vector<ClientLeaderRegInterface> clientLeaderServers;
	Key clusterKey;
	Reference<IClusterConnectionRecord> ccr;

	explicit ClientCoordinators(Reference<IClusterConnectionRecord> ccr);
	explicit ClientCoordinators(Key clusterKey, std::vector<NetworkAddress> coordinators);
	ClientCoordinators() {}
};

struct ProtocolInfoReply {
	constexpr static FileIdentifier file_identifier = 7784298;
	ProtocolVersion version;
	template <class Ar>
	void serialize(Ar& ar) {
		uint64_t version_ = 0;
		if (Ar::isSerializing) {
			version_ = version.versionWithFlags();
		}
		serializer(ar, version_);
		if (Ar::isDeserializing) {
			version = ProtocolVersion(version_);
		}
	}
};

struct ProtocolInfoRequest {
	constexpr static FileIdentifier file_identifier = 13261233;
	ReplyPromise<ProtocolInfoReply> reply{ PeerCompatibilityPolicy{ RequirePeer::AtLeast,
		                                                            ProtocolVersion::withStableInterfaces() } };
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// Returns true if the cluster descriptor may be modified.
struct CheckDescriptorMutableReply {
	constexpr static FileIdentifier file_identifier = 7784299;
	CheckDescriptorMutableReply() = default;
	explicit CheckDescriptorMutableReply(bool isMutable) : isMutable(isMutable) {}
	bool isMutable;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, isMutable);
	}
};

// Allows client to check if allowed to change the cluster descriptor.
struct CheckDescriptorMutableRequest {
	constexpr static FileIdentifier file_identifier = 214729;
	ReplyPromise<CheckDescriptorMutableReply> reply;
	CheckDescriptorMutableRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

#endif
