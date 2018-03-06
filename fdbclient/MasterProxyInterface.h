/*
 * MasterProxyInterface.h
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

#ifndef FDBCLIENT_MASTERPROXYINTERFACE_H
#define FDBCLIENT_MASTERPROXYINTERFACE_H
#pragma once

#include "FDBTypes.h"
#include "StorageServerInterface.h"
#include "CommitTransaction.h"

struct MasterProxyInterface {
	enum { LocationAwareLoadBalance = 1 };

	LocalityData locality;
	RequestStream< struct CommitTransactionRequest > commit;
	RequestStream< struct GetReadVersionRequest > getConsistentReadVersion;  // Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit response) when this request was sent
															     //   (at some point between when this request is sent and when its response is received, the latest version reported committed)
	RequestStream< struct GetKeyServerLocationsRequest > getKeyServersLocations;
	RequestStream< struct GetStorageServerRejoinInfoRequest > getStorageServerRejoinInfo;

	RequestStream<ReplyPromise<Void>> waitFailure;

	RequestStream< struct GetRawCommittedVersionRequest > getRawCommittedVersion;
	RequestStream< struct TxnStateRequest >  txnState;

	UID id() const { return commit.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator == (MasterProxyInterface const& r) const { return id() == r.id(); }
	bool operator != (MasterProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return commit.getEndpoint().address; }

	template <class Archive>
	void serialize(Archive& ar) {
		ar & locality & commit & getConsistentReadVersion & getKeyServersLocations & waitFailure & getStorageServerRejoinInfo & getRawCommittedVersion & txnState;
	}

	void initEndpoints() {
		getConsistentReadVersion.getEndpoint(TaskProxyGetConsistentReadVersion);
		getRawCommittedVersion.getEndpoint(TaskProxyGetRawCommittedVersion);
		commit.getEndpoint(TaskProxyCommit);
		getKeyServersLocations.getEndpoint(TaskProxyGetKeyServersLocations);
	}
};

struct CommitID {
	Version version; 			// returns invalidVersion if transaction conflicts
	uint16_t txnBatchId;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & version & txnBatchId;
	}

	CommitID() : version(invalidVersion), txnBatchId(0) {}
	CommitID( Version version, uint16_t txnBatchId ) : version(version), txnBatchId(txnBatchId) {}
};

struct CommitTransactionRequest {
	Arena arena;
	CommitTransactionRef transaction;
	ReplyPromise<CommitID> reply;		
	Optional<UID> debugID;
	bool isLockAware;

	template <class Ar> 
	void serialize(Ar& ar) { 
		ar & transaction & reply & arena & debugID & isLockAware;
	}
};

static inline int getBytes( CommitTransactionRequest const& r ) { 
	// SOMEDAY: Optimize
	//return r.arena.getSize(); // NOT correct because arena can be shared!
	int total = sizeof(r);
	for(auto m = r.transaction.mutations.begin(); m != r.transaction.mutations.end(); ++m)
		total += m->expectedSize();
	for(auto i = r.transaction.read_conflict_ranges.begin(); i != r.transaction.read_conflict_ranges.end(); ++i)
		total += i->expectedSize();
	for(auto i = r.transaction.write_conflict_ranges.begin(); i != r.transaction.write_conflict_ranges.end(); ++i)
		total += i->expectedSize();
	return total;
}

struct GetReadVersionReply {
	Version version;
	bool locked;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & version & locked;
	}
};

struct GetReadVersionRequest {
	enum { 
		PRIORITY_SYSTEM_IMMEDIATE = 15 << 24,  // Highest possible priority, always executed even if writes are otherwise blocked
		PRIORITY_DEFAULT = 8 << 24,
		PRIORITY_BATCH = 1 << 24
	};
	enum { 
		FLAG_CAUSAL_READ_RISKY = 1,
		FLAG_PRIORITY_MASK = PRIORITY_SYSTEM_IMMEDIATE,
	};

	uint32_t transactionCount;
	uint32_t flags;
	Optional<UID> debugID;
	ReplyPromise<GetReadVersionReply> reply;

	GetReadVersionRequest() : transactionCount( 1 ), flags( PRIORITY_DEFAULT ) {}
	GetReadVersionRequest( uint32_t transactionCount, uint32_t flags, Optional<UID> debugID = Optional<UID>() ) : transactionCount( transactionCount ), flags( flags ), debugID( debugID ) {}
	
	int priority() const { return flags & FLAG_PRIORITY_MASK; }
	bool operator < (GetReadVersionRequest const& rhs) const { return priority() < rhs.priority(); }

	template <class Ar> 
	void serialize(Ar& ar) { 
		ar & transactionCount & flags & debugID & reply;
	}
};

struct GetKeyServerLocationsReply {
	Arena arena;
	vector<pair<KeyRangeRef, vector<StorageServerInterface>>> results;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & results & arena;
	}
};

struct GetKeyServerLocationsRequest {
	Arena arena;
	KeyRef begin;
	Optional<KeyRef> end;
	int limit;
	bool reverse;
	ReplyPromise<GetKeyServerLocationsReply> reply;

	GetKeyServerLocationsRequest() : limit(0), reverse(false) {}
	GetKeyServerLocationsRequest( KeyRef const& begin, Optional<KeyRef> const& end, int limit, bool reverse, Arena const& arena ) : begin( begin ), end( end ), limit( limit ), reverse( reverse ), arena( arena ) {}
	
	template <class Ar> 
	void serialize(Ar& ar) { 
		ar & begin & end & limit & reverse & reply & arena;
	}
};

struct GetRawCommittedVersionRequest {
	Optional<UID> debugID;
	ReplyPromise<GetReadVersionReply> reply;

	explicit GetRawCommittedVersionRequest(Optional<UID> const& debugID = Optional<UID>()) : debugID(debugID) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & debugID & reply;
	}
};

struct GetStorageServerRejoinInfoReply {
	Version version;
	Tag tag;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & version & tag;
	}
};

struct GetStorageServerRejoinInfoRequest {
	UID id;
	ReplyPromise< GetStorageServerRejoinInfoReply > reply;

	GetStorageServerRejoinInfoRequest() {}
	explicit GetStorageServerRejoinInfoRequest(UID const& id ) : id(id) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & id & reply;
	}
};

struct TxnStateRequest {
	Arena arena;
	VectorRef<KeyValueRef> data;
	Sequence sequence;
	bool last;
	ReplyPromise<Void> reply;

	template <class Ar> 
	void serialize(Ar& ar) { 
		ar & data & sequence & last & reply & arena;
	}
};

#endif
