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

#include "FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

const int MAX_CLUSTER_FILE_BYTES = 60000;

struct ClientLeaderRegInterface {
	RequestStream< struct GetLeaderRequest > getLeader;

	ClientLeaderRegInterface() {}
	ClientLeaderRegInterface( NetworkAddress remote );
	ClientLeaderRegInterface( INetwork* local );
};

class ClusterConnectionString {
public:
	ClusterConnectionString() {}
	ClusterConnectionString( std::string const& connectionString );
	ClusterConnectionString( vector<NetworkAddress>, Key );
	vector<NetworkAddress> const& coordinators() const { return coord; }
	Key clusterKey() const { return key; }
	Key clusterKeyName() const { return keyDesc; }  // Returns the "name" or "description" part of the clusterKey (the part before the ':')
	std::string toString() const;
	static std::string getErrorString(std::string const& source, Error const& e);
private:
	void parseKey( std::string const& key );

	vector<NetworkAddress> coord;
	Key key, keyDesc;
};

class ClusterConnectionFile : NonCopyable, public ReferenceCounted<ClusterConnectionFile> {
public:
	ClusterConnectionFile() {}
	// Loads and parses the file at 'path', throwing errors if the file cannot be read or the format is invalid.
	//
	// The format of the file is: description:id@[addrs]+
	//  The description and id together are called the "key"
	//
	// The following is enforced about the format of the file:
	//  - The key must contain one (and only one) ':' character
	//  - The description contains only allowed characters (a-z, A-Z, 0-9, _)
	//  - The ID contains only allowed characters (a-z, A-Z, 0-9)
	//  - At least one address is specified
	//  - All addresses either have TLS enabled or disabled (no mixing)
	//  - There is no address present more than once
	explicit ClusterConnectionFile( std::string const& path );
	explicit ClusterConnectionFile(ClusterConnectionString const& cs) : cs(cs), setConn(false) {}
	explicit ClusterConnectionFile(std::string const& filename, ClusterConnectionString const& contents);

	// returns <resolved name, was default file>
	static std::pair<std::string, bool> lookupClusterFileName( std::string const& filename );
	// get a human readable error message describing the error returned from the constructor
	static std::string getErrorString( std::pair<std::string, bool> const& resolvedFile, Error const& e );

	ClusterConnectionString const& getConnectionString();
	bool writeFile();
	void setConnectionString( ClusterConnectionString const& );
	std::string const& getFilename() const { ASSERT( filename.size() ); return filename; }
	bool canGetFilename() { return filename.size() != 0; }
	bool fileContentsUpToDate() const;
	bool fileContentsUpToDate(ClusterConnectionString &fileConnectionString) const;
	void notifyConnected();
private:
	ClusterConnectionString cs;
	std::string filename;
	bool setConn;
};

struct LeaderInfo {
	UID changeID;
	uint64_t mask = ~(15ll << 60);
	Value serializedInfo;
	bool forward;  // If true, serializedInfo is a connection string instead!

	LeaderInfo() : forward(false) {}
	LeaderInfo(UID changeID) : changeID(changeID), forward(false) {}

	bool operator < (LeaderInfo const& r) const { return changeID < r.changeID; }
	bool operator == (LeaderInfo const& r) const { return changeID == r.changeID; }

	// The first 4 bits of ChangeID represent cluster controller process class fitness, the lower the better
	void updateChangeID(uint64_t processClassFitness, bool isExcluded) {
		changeID = UID( ( (uint64_t)isExcluded << 63) | (processClassFitness << 60) | (changeID.first() & mask ), changeID.second() );
	}

	// All but the first 4 bits are used to represent process id
	bool equalInternalId(LeaderInfo const& leaderInfo) const {
		if ( (changeID.first() & mask) == (leaderInfo.changeID.first() & mask) && changeID.second() == leaderInfo.changeID.second() ) {
			return true;
		} else {
			return false;
		}
	}

	// Change leader only if 
	// 1. the candidate has better process class fitness and the candidate is not the leader
	// 2. the leader process class fitness become worse
	bool leaderChangeRequired(LeaderInfo const& candidate) const {
		if ( ((changeID.first() & ~mask) > (candidate.changeID.first() & ~mask) && !equalInternalId(candidate)) || ((changeID.first() & ~mask) < (candidate.changeID.first() & ~mask) && equalInternalId(candidate)) ) {
			return true;
		} else {
			return false;
		}
	}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & changeID & serializedInfo & forward;
	}
};

struct GetLeaderRequest {
	Key key;
	UID knownLeader;
	ReplyPromise< Optional<LeaderInfo> > reply;

	GetLeaderRequest() {}
	explicit GetLeaderRequest(Key key, UID kl) : key(key), knownLeader(kl) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & key & knownLeader & reply;
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
