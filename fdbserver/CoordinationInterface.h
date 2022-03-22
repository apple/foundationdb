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

#ifndef FDBSERVER_COORDINATIONINTERFACE_H
#define FDBSERVER_COORDINATIONINTERFACE_H
#pragma once

#include "fdbclient/CoordinationInterface.h"
#include "fdbclient/WellKnownEndpoints.h"
#include "fdbserver/ConfigFollowerInterface.h"
#include "fdbserver/ConfigBroadcastInterface.h"

struct GenerationRegInterface {
	constexpr static FileIdentifier file_identifier = 16726744;
	RequestStream<struct GenerationRegReadRequest> read;
	RequestStream<struct GenerationRegWriteRequest> write;

	// read(key,gen2) returns (value,gen,rgen).
	//   If there was no prior write(_,_,0) or a data loss fault,
	//     returns (Optional(),0,0)   //< FIXME: The returned rgen is not zero, but >= gen2!  (Specification bug)
	//   Else
	//     There was some earlier or concurrent write(key,value,gen).
	//     There was some earlier or concurrent read(key,rgen).
	//     If there is a write(key,_,gen1)=>gen1 s.t. gen1 < gen2 OR the write completed before this read started, then
	//     gen >= gen1. If there is a read(key,gen1) that completed before this read started, then rgen >= gen1
	// write(key,value,gen) returns gen1.
	//   If gen>0 and there was no prior write(_,_,0) or a data loss fault, throws not_created()?
	//   (gen1==gen is considered a "successful" write)
	//   There is some earlier or concurrent read(key,gen1) or write(key,_,gen1).  (In the successful case, the
	//   concurrent write is this one)

	// All instances of the pattern
	//    read(key, g)=>v1 and write(key, v2, g)=>true
	// thus form a totally ordered sequence of modifications, in which
	// the v2 of the previous generation is the v1 of the next.

	GenerationRegInterface() {}
	GenerationRegInterface(NetworkAddress remote);
	GenerationRegInterface(INetwork* local);
};

struct UniqueGeneration {
	constexpr static FileIdentifier file_identifier = 16684234;
	uint64_t generation;
	UID uid;
	UniqueGeneration() : generation(0) {}
	UniqueGeneration(uint64_t generation, UID uid) : generation(generation), uid(uid) {}
	bool operator<(UniqueGeneration const& r) const {
		if (generation < r.generation)
			return true;
		if (r.generation < generation)
			return false;
		return uid < r.uid;
	}
	bool operator>(UniqueGeneration const& r) const { return r < *this; }
	bool operator<=(UniqueGeneration const& r) const { return !(*this > r); }
	bool operator>=(UniqueGeneration const& r) const { return !(*this < r); }
	bool operator==(UniqueGeneration const& r) const { return generation == r.generation && uid == r.uid; }
	bool operator!=(UniqueGeneration const& r) const { return !(*this == r); }
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, generation, uid);
	}
};

struct GenerationRegReadReply {
	constexpr static FileIdentifier file_identifier = 12623609;
	Optional<Value> value;
	UniqueGeneration gen, rgen;
	GenerationRegReadReply() {}
	GenerationRegReadReply(Optional<Value> value, UniqueGeneration gen, UniqueGeneration rgen)
	  : value(value), gen(gen), rgen(rgen) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value, gen, rgen);
	}
};

struct GenerationRegReadRequest {
	constexpr static FileIdentifier file_identifier = 8975311;
	Key key;
	UniqueGeneration gen;
	ReplyPromise<struct GenerationRegReadReply> reply;
	GenerationRegReadRequest() {}
	GenerationRegReadRequest(Key key, UniqueGeneration gen) : key(key), gen(gen) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, gen, reply);
	}
};

struct GenerationRegWriteRequest {
	constexpr static FileIdentifier file_identifier = 3521510;
	KeyValue kv;
	UniqueGeneration gen;
	ReplyPromise<UniqueGeneration> reply;
	GenerationRegWriteRequest() {}
	GenerationRegWriteRequest(KeyValue kv, UniqueGeneration gen) : kv(kv), gen(gen) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, kv, gen, reply);
	}
};

struct LeaderElectionRegInterface : ClientLeaderRegInterface {
	RequestStream<struct CandidacyRequest> candidacy;
	RequestStream<struct ElectionResultRequest> electionResult;
	RequestStream<struct LeaderHeartbeatRequest> leaderHeartbeat;
	RequestStream<struct ForwardRequest> forward;

	LeaderElectionRegInterface() {}
	LeaderElectionRegInterface(NetworkAddress remote);
	LeaderElectionRegInterface(INetwork* local);
};

struct CandidacyRequest {
	constexpr static FileIdentifier file_identifier = 14473958;
	Key key;
	LeaderInfo myInfo;
	UID knownLeader, prevChangeID;
	ReplyPromise<Optional<LeaderInfo>> reply;

	CandidacyRequest() {}
	CandidacyRequest(Key key, LeaderInfo const& myInfo, UID const& knownLeader, UID const& prevChangeID)
	  : key(key), myInfo(myInfo), knownLeader(knownLeader), prevChangeID(prevChangeID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, myInfo, knownLeader, prevChangeID, reply);
	}
};

struct ElectionResultRequest {
	constexpr static FileIdentifier file_identifier = 11815465;
	Key key;
	std::vector<NetworkAddress> coordinators;
	UID knownLeader;
	ReplyPromise<Optional<LeaderInfo>> reply;

	ElectionResultRequest() = default;
	ElectionResultRequest(Key key, std::vector<NetworkAddress> coordinators, UID knownLeader)
	  : key(key), coordinators(std::move(coordinators)), knownLeader(knownLeader) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, coordinators, knownLeader, reply);
	}
};

struct LeaderHeartbeatReply {
	constexpr static FileIdentifier file_identifier = 11;

	bool value = false;
	LeaderHeartbeatReply() = default;
	explicit LeaderHeartbeatReply(bool value) : value(value) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, value);
	}
};

inline bool operator==(const LeaderHeartbeatReply& lhs, const LeaderHeartbeatReply& rhs) {
	return lhs.value == rhs.value;
}

struct LeaderHeartbeatRequest {
	constexpr static FileIdentifier file_identifier = 9495992;
	Key key;
	LeaderInfo myInfo;
	UID prevChangeID;
	ReplyPromise<LeaderHeartbeatReply> reply;

	LeaderHeartbeatRequest() {}
	explicit LeaderHeartbeatRequest(Key key, LeaderInfo const& myInfo, UID prevChangeID)
	  : key(key), myInfo(myInfo), prevChangeID(prevChangeID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, myInfo, prevChangeID, reply);
	}
};

struct ForwardRequest {
	constexpr static FileIdentifier file_identifier = 13570359;
	Key key;
	Value conn; // a cluster connection string
	ReplyPromise<Void> reply;

	ForwardRequest() {}
	ForwardRequest(Key key, Value conn) : key(key), conn(conn) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, conn, reply);
	}
};

class ConfigNode;

class ServerCoordinators : public ClientCoordinators {
public:
	explicit ServerCoordinators(Reference<IClusterConnectionRecord>);

	std::vector<LeaderElectionRegInterface> leaderElectionServers;
	std::vector<GenerationRegInterface> stateServers;
	std::vector<ConfigFollowerInterface> configServers;
};

Future<Void> coordinationServer(std::string const& dataFolder,
                                Reference<IClusterConnectionRecord> const& ccf,
                                Reference<ConfigNode> const&,
                                ConfigBroadcastInterface const&);

#endif
