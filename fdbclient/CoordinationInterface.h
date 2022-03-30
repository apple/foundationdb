/*
 * CoordinationInterface.h
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

// A string containing the information necessary to connect to a cluster.
//
// The format of the connection string is: description:id@[addrs]+
// The description and id together are called the "key"
//
// The following is enforced about the format of the file:
//  - The key must contain one (and only one) ':' character
//  - The description contains only allowed characters (a-z, A-Z, 0-9, _)
//  - The ID contains only allowed characters (a-z, A-Z, 0-9)
//  - At least one address is specified
//  - There is no address present more than once
class ClusterConnectionString {
public:
	enum ConnectionStringStatus { RESOLVED, RESOLVING, UNRESOLVED };

	ClusterConnectionString() {}
	ClusterConnectionString(const std::string& connStr);
	ClusterConnectionString(const std::vector<NetworkAddress>& coordinators, Key key);
	ClusterConnectionString(const std::vector<Hostname>& hosts, Key key);

	ClusterConnectionString(const ClusterConnectionString& rhs) { operator=(rhs); }
	ClusterConnectionString& operator=(const ClusterConnectionString& rhs) {
		// Copy everything except AsyncTrigger resolveFinish.
		status = rhs.status;
		coords = rhs.coords;
		hostnames = rhs.hostnames;
		networkAddressToHostname = rhs.networkAddressToHostname;
		key = rhs.key;
		keyDesc = rhs.keyDesc;
		connectionString = rhs.connectionString;
		return *this;
	}

	std::vector<NetworkAddress> const& coordinators() const { return coords; }
	void addResolved(const Hostname& hostname, const NetworkAddress& address) {
		coords.push_back(address);
		networkAddressToHostname.emplace(address, hostname);
	}
	Key clusterKey() const { return key; }
	Key clusterKeyName() const {
		return keyDesc;
	} // Returns the "name" or "description" part of the clusterKey (the part before the ':')
	std::string toString() const;
	static std::string getErrorString(std::string const& source, Error const& e);
	Future<Void> resolveHostnames();
	// This one should only be used when resolving asynchronously is impossible. For all other cases, resolveHostnames()
	// should be preferred.
	void resolveHostnamesBlocking();
	// This function derives the member connectionString from the current key, coordinators and hostnames.
	void resetConnectionString();

	void resetToUnresolved();
	void parseKey(const std::string& key);

	ConnectionStringStatus status = RESOLVED;
	AsyncTrigger resolveFinish;
	std::vector<NetworkAddress> coords;
	std::vector<Hostname> hostnames;
	std::unordered_map<NetworkAddress, Hostname> networkAddressToHostname;

private:
	void parseConnString();
	Key key, keyDesc;
	std::string connectionString;
};

FDB_DECLARE_BOOLEAN_PARAM(ConnectionStringNeedsPersisted);

// A record that stores the connection string used to connect to a cluster. This record can be updated when a cluster
// notifies a connected party that the connection string has changed.
//
// The typically used cluster connection record is a cluster file (implemented in ClusterConnectionFile). This interface
// provides an abstraction over the cluster file so that we can persist the connection string in other locations or have
// one that is only stored in memory.
class IClusterConnectionRecord {
public:
	IClusterConnectionRecord(ConnectionStringNeedsPersisted connectionStringNeedsPersisted)
	  : connectionStringNeedsPersisted(connectionStringNeedsPersisted) {}
	virtual ~IClusterConnectionRecord() {}

	// Returns the connection string currently held in this object. This may not match the stored record if it hasn't
	// been persisted or if the persistent storage for the record has been modified externally.
	ClusterConnectionString& getConnectionString();

	// Sets the connections string held by this object and persists it.
	virtual Future<Void> setAndPersistConnectionString(ClusterConnectionString const&) = 0;

	// If this record is backed by persistent storage, get the connection string from that storage. Otherwise, return
	// the connection string stored in memory.
	virtual Future<ClusterConnectionString> getStoredConnectionString() = 0;

	// Checks whether the connection string in persisten storage matches the connection string stored in memory.
	Future<bool> upToDate();

	// Checks whether the connection string in persisten storage matches the connection string stored in memory. The
	// cluster string stored in persistent storage is returned via the reference parameter connectionString.
	virtual Future<bool> upToDate(ClusterConnectionString& connectionString) = 0;

	// Returns a string representing the location of the cluster record. For example, this could be the filename or key
	// that stores the connection string.
	virtual std::string getLocation() const = 0;

	// Creates a copy of this object with a modified connection string but that isn't persisted.
	virtual Reference<IClusterConnectionRecord> makeIntermediateRecord(
	    ClusterConnectionString const& connectionString) const = 0;

	// Returns a string representation of this cluster connection record. This will include the type and location of the
	// record.
	virtual std::string toString() const = 0;

	// Signals to the connection record that it was successfully used to connect to a cluster.
	void notifyConnected();

	ClusterConnectionString::ConnectionStringStatus connectionStringStatus() const;
	Future<Void> resolveHostnames();
	// This one should only be used when resolving asynchronously is impossible. For all other cases, resolveHostnames()
	// should be preferred.
	void resolveHostnamesBlocking();

	virtual void addref() = 0;
	virtual void delref() = 0;

protected:
	// Writes the connection string to the backing persistent storage, if applicable.
	virtual Future<bool> persist() = 0;

	// Returns whether the connection record contains a connection string that needs to be persisted upon connection.
	bool needsToBePersisted() const;

	// Clears the flag needs persisted flag.
	void setPersisted();

	ClusterConnectionString cs;

private:
	// A flag that indicates whether this connection record needs to be persisted when it succesfully establishes a
	// connection.
	bool connectionStringNeedsPersisted;
};

struct LeaderInfo {
	constexpr static FileIdentifier file_identifier = 8338794;
	// The first 7 bits of changeID represent cluster controller process class fitness, the lower the better
	UID changeID;
	static const uint64_t changeIDMask = ~(uint64_t(0b1111111) << 57);
	Value serializedInfo;
	// If true, serializedInfo is a connection string instead!
	// If true, it also means the receipient need to update their local cluster file
	// with the latest list of coordinators
	bool forward;

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
