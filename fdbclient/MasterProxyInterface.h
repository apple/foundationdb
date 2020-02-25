
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

#include <utility>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/CommitTransaction.h"

#include "flow/Stats.h"
#include "fdbrpc/TimedRequest.h"

struct MasterProxyInterface {
	constexpr static FileIdentifier file_identifier = 8954922;
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	LocalityData locality;
	bool provisional;
	RequestStream< struct CommitTransactionRequest > commit;
	RequestStream< struct GetReadVersionRequest > getConsistentReadVersion;  // Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit response) when this request was sent
															     //   (at some point between when this request is sent and when its response is received, the latest version reported committed)
	RequestStream< struct GetKeyServerLocationsRequest > getKeyServersLocations;
	RequestStream< struct GetStorageServerRejoinInfoRequest > getStorageServerRejoinInfo;

	RequestStream<ReplyPromise<Void>> waitFailure;

	RequestStream< struct GetRawCommittedVersionRequest > getRawCommittedVersion;
	RequestStream< struct TxnStateRequest >  txnState;
	RequestStream< struct GetHealthMetricsRequest > getHealthMetrics;
	RequestStream< struct ProxySnapRequest > proxySnapReq;

	UID id() const { return commit.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator == (MasterProxyInterface const& r) const { return id() == r.id(); }
	bool operator != (MasterProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return commit.getEndpoint().getPrimaryAddress(); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, locality, provisional, commit, getConsistentReadVersion, getKeyServersLocations,
				   waitFailure, getStorageServerRejoinInfo, getRawCommittedVersion,
				   txnState, getHealthMetrics, proxySnapReq);
	}

	void initEndpoints() {
		getConsistentReadVersion.getEndpoint(TaskPriority::ProxyGetConsistentReadVersion);
		getRawCommittedVersion.getEndpoint(TaskPriority::ProxyGetRawCommittedVersion);
		commit.getEndpoint(TaskPriority::ProxyCommitDispatcher);
		getStorageServerRejoinInfo.getEndpoint(TaskPriority::ProxyStorageRejoin);
		//getKeyServersLocations.getEndpoint(TaskProxyGetKeyServersLocations); //do not increase the priority of these requests, because clients cans bring down the cluster with too many of these messages.
	}
};

// ClientDBInfo is all the information needed by a database client to access the database
// It is returned (and kept up to date) by the OpenDatabaseRequest interface of ClusterInterface
struct ClientDBInfo {
	constexpr static FileIdentifier file_identifier = 5355080;
	UID id;  // Changes each time anything else changes
	vector< MasterProxyInterface > proxies;
	Optional<MasterProxyInterface> firstProxy; //not serialized, used for commitOnFirstProxy when the proxies vector has been shrunk
	double clientTxnInfoSampleRate;
	int64_t clientTxnInfoSizeLimit;
	Optional<Value> forward;
	ClientDBInfo() : clientTxnInfoSampleRate(std::numeric_limits<double>::infinity()), clientTxnInfoSizeLimit(-1) {}

	bool operator == (ClientDBInfo const& r) const { return id == r.id; }
	bool operator != (ClientDBInfo const& r) const { return id != r.id; }

	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, proxies, id, clientTxnInfoSampleRate, clientTxnInfoSizeLimit, forward);
	}
};

struct CommitID {
	constexpr static FileIdentifier file_identifier = 14254927;
	Version version; 			// returns invalidVersion if transaction conflicts
	uint16_t txnBatchId;
	Optional<Value> metadataVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, txnBatchId, metadataVersion);
	}

	CommitID() : version(invalidVersion), txnBatchId(0) {}
	CommitID( Version version, uint16_t txnBatchId, const Optional<Value>& metadataVersion ) : version(version), txnBatchId(txnBatchId), metadataVersion(metadataVersion) {}
};

struct CommitTransactionRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 93948;
	enum { 
		FLAG_IS_LOCK_AWARE = 0x1,
		FLAG_FIRST_IN_BATCH = 0x2
	};

	bool isLockAware() const { return (flags & FLAG_IS_LOCK_AWARE) != 0; }
	bool firstInBatch() const { return (flags & FLAG_FIRST_IN_BATCH) != 0; }
	
	Arena arena;
	CommitTransactionRef transaction;
	ReplyPromise<CommitID> reply;
	uint32_t flags;
	Optional<UID> debugID;

	CommitTransactionRequest() : flags(0) {}

	template <class Ar> 
	void serialize(Ar& ar) { 
		serializer(ar, transaction, reply, arena, flags, debugID);
	}
};

static inline int getBytes( CommitTransactionRequest const& r ) {
	// SOMEDAY: Optimize
	//return r.arena.getSize(); // NOT correct because arena can be shared!
	int total = sizeof(r);
	for(auto m = r.transaction.mutations.begin(); m != r.transaction.mutations.end(); ++m)
		total += m->expectedSize() + CLIENT_KNOBS->PROXY_COMMIT_OVERHEAD_BYTES;
	for(auto i = r.transaction.read_conflict_ranges.begin(); i != r.transaction.read_conflict_ranges.end(); ++i)
		total += i->expectedSize();
	for(auto i = r.transaction.write_conflict_ranges.begin(); i != r.transaction.write_conflict_ranges.end(); ++i)
		total += i->expectedSize();
	return total;
}

struct GetReadVersionReply {
	constexpr static FileIdentifier file_identifier = 15709388;
	Version version;
	bool locked;
	Optional<Value> metadataVersion;

	GetReadVersionReply() : version(invalidVersion), locked(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, locked, metadataVersion);
	}
};

struct GetReadVersionRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 838566;
	enum { 
		PRIORITY_SYSTEM_IMMEDIATE = 15 << 24,  // Highest possible priority, always executed even if writes are otherwise blocked
		PRIORITY_DEFAULT = 8 << 24,
		PRIORITY_BATCH = 1 << 24
	};
	enum {
		FLAG_USE_PROVISIONAL_PROXIES = 2,
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
		serializer(ar, transactionCount, flags, debugID, reply);
	}
};

struct GetKeyServerLocationsReply {
	constexpr static FileIdentifier file_identifier = 10636023;
	Arena arena;
	std::vector<std::pair<KeyRangeRef, vector<StorageServerInterface>>> results;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, results, arena);
	}
};

struct GetKeyServerLocationsRequest {
	constexpr static FileIdentifier file_identifier = 9144680;
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
		serializer(ar, begin, end, limit, reverse, reply, arena);
	}
};

struct GetRawCommittedVersionRequest {
	constexpr static FileIdentifier file_identifier = 12954034;
	Optional<UID> debugID;
	ReplyPromise<GetReadVersionReply> reply;

	explicit GetRawCommittedVersionRequest(Optional<UID> const& debugID = Optional<UID>()) : debugID(debugID) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, debugID, reply);
	}
};

struct GetStorageServerRejoinInfoReply {
	constexpr static FileIdentifier file_identifier = 9469225;
	Version version;
	Tag tag;
	Optional<Tag> newTag;
	bool newLocality;
	std::vector<std::pair<Version, Tag>> history;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, tag, newTag, newLocality, history);
	}
};

struct GetStorageServerRejoinInfoRequest {
	constexpr static FileIdentifier file_identifier = 994279;
	UID id;
	Optional<Value> dcId;
	ReplyPromise< GetStorageServerRejoinInfoReply > reply;

	GetStorageServerRejoinInfoRequest() {}
	explicit GetStorageServerRejoinInfoRequest( UID const& id, Optional<Value> const& dcId ) : id(id), dcId(dcId) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, id, dcId, reply);
	}
};

struct TxnStateRequest {
	constexpr static FileIdentifier file_identifier = 15250781;
	Arena arena;
	VectorRef<KeyValueRef> data;
	Sequence sequence;
	bool last;
	ReplyPromise<Void> reply;

	template <class Ar> 
	void serialize(Ar& ar) { 
		serializer(ar, data, sequence, last, reply, arena);
	}
};

struct GetHealthMetricsReply
{
	constexpr static FileIdentifier file_identifier = 11544290;
	Standalone<StringRef> serialized;
	HealthMetrics healthMetrics;

	explicit GetHealthMetricsReply(const HealthMetrics& healthMetrics = HealthMetrics()) :
		healthMetrics(healthMetrics)
	{
		update(healthMetrics, true, true);
	}

	void update(const HealthMetrics& healthMetrics, bool detailedInput, bool detailedOutput)
	{
		this->healthMetrics.update(healthMetrics, detailedInput, detailedOutput);
		BinaryWriter bw(IncludeVersion());
		bw << this->healthMetrics;
		serialized = bw.toValue();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, serialized);
		if (ar.isDeserializing) {
			BinaryReader br(serialized, IncludeVersion());
			br >> healthMetrics;
		}
	}
};

struct GetHealthMetricsRequest
{
	constexpr static FileIdentifier file_identifier = 11403900;
	ReplyPromise<struct GetHealthMetricsReply> reply;
	bool detailed;

	explicit GetHealthMetricsRequest(bool detailed = false) : detailed(detailed) {}

	template <class Ar>
	void serialize(Ar& ar)
	{
		serializer(ar, reply, detailed);
	}
};

struct ProxySnapRequest
{
	constexpr static FileIdentifier file_identifier = 22204900;
	Arena arena;
	StringRef snapPayload;
	UID snapUID;
	ReplyPromise<Void> reply;
	Optional<UID> debugID;

	explicit ProxySnapRequest(Optional<UID> const& debugID = Optional<UID>()) : debugID(debugID) {}
	explicit ProxySnapRequest(StringRef snap, UID snapUID, Optional<UID> debugID = Optional<UID>()) : snapPayload(snap), snapUID(snapUID), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapPayload, snapUID, reply, arena, debugID);
	}
};

#endif
