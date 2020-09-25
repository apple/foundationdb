
/*
 * CommitProxyInterface.h
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

#ifndef FDBCLIENT_COMMITPROXYINTERFACE_H
#define FDBCLIENT_COMMITPROXYINTERFACE_H
#pragma once

#include <utility>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbclient/TagThrottle.h"

#include "fdbrpc/Stats.h"
#include "fdbrpc/TimedRequest.h"
#include "GrvProxyInterface.h"

struct CommitProxyInterface {
	constexpr static FileIdentifier file_identifier = 8954922;
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	Optional<Key> processId;
	bool provisional;
	RequestStream< struct CommitTransactionRequest > commit;
	RequestStream< struct GetReadVersionRequest > getConsistentReadVersion;  // Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit response) when this request was sent
															     //   (at some point between when this request is sent and when its response is received, the latest version reported committed)
	RequestStream< struct GetKeyServerLocationsRequest > getKeyServersLocations;
	RequestStream< struct GetStorageServerRejoinInfoRequest > getStorageServerRejoinInfo;

	RequestStream<ReplyPromise<Void>> waitFailure;

	RequestStream< struct TxnStateRequest >  txnState;
	RequestStream< struct GetHealthMetricsRequest > getHealthMetrics;
	RequestStream< struct ProxySnapRequest > proxySnapReq;
	RequestStream< struct ExclusionSafetyCheckRequest > exclusionSafetyCheckReq;
	RequestStream< struct GetDDMetricsRequest > getDDMetrics;

	UID id() const { return commit.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator==(CommitProxyInterface const& r) const { return id() == r.id(); }
	bool operator!=(CommitProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return commit.getEndpoint().getPrimaryAddress(); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, processId, provisional, commit);
		if( Archive::isDeserializing ) {
			getConsistentReadVersion = RequestStream< struct GetReadVersionRequest >( commit.getEndpoint().getAdjustedEndpoint(1) );
			getKeyServersLocations = RequestStream< struct GetKeyServerLocationsRequest >( commit.getEndpoint().getAdjustedEndpoint(2) );
			getStorageServerRejoinInfo = RequestStream< struct GetStorageServerRejoinInfoRequest >( commit.getEndpoint().getAdjustedEndpoint(3) );
			waitFailure = RequestStream<ReplyPromise<Void>>( commit.getEndpoint().getAdjustedEndpoint(4) );
			txnState = RequestStream< struct TxnStateRequest >( commit.getEndpoint().getAdjustedEndpoint(5) );
			getHealthMetrics = RequestStream< struct GetHealthMetricsRequest >( commit.getEndpoint().getAdjustedEndpoint(6) );
			proxySnapReq = RequestStream< struct ProxySnapRequest >( commit.getEndpoint().getAdjustedEndpoint(7) );
			exclusionSafetyCheckReq = RequestStream< struct ExclusionSafetyCheckRequest >( commit.getEndpoint().getAdjustedEndpoint(8) );
			getDDMetrics = RequestStream< struct GetDDMetricsRequest >( commit.getEndpoint().getAdjustedEndpoint(9) );
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(commit.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(getConsistentReadVersion.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(getKeyServersLocations.getReceiver(TaskPriority::ReadSocket)); //priority lowered to TaskPriority::DefaultEndpoint on the proxy
		streams.push_back(getStorageServerRejoinInfo.getReceiver(TaskPriority::ProxyStorageRejoin));
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(txnState.getReceiver());
		streams.push_back(getHealthMetrics.getReceiver());
		streams.push_back(proxySnapReq.getReceiver());
		streams.push_back(exclusionSafetyCheckReq.getReceiver());
		streams.push_back(getDDMetrics.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

// ClientDBInfo is all the information needed by a database client to access the database
// It is returned (and kept up to date) by the OpenDatabaseRequest interface of ClusterInterface
struct ClientDBInfo {
	constexpr static FileIdentifier file_identifier = 5355080;
	UID id;  // Changes each time anything else changes
	vector<GrvProxyInterface> grvProxies;
	vector<CommitProxyInterface> commitProxies;
	Optional<CommitProxyInterface>
	    firstCommitProxy; // not serialized, used for commitOnFirstProxy when the commit proxies vector has been shrunk
	double clientTxnInfoSampleRate;
	int64_t clientTxnInfoSizeLimit;
	Optional<Value> forward;
	double transactionTagSampleRate;
	double transactionTagSampleCost;

	ClientDBInfo()
	  : clientTxnInfoSampleRate(std::numeric_limits<double>::infinity()), clientTxnInfoSizeLimit(-1),
	    transactionTagSampleRate(CLIENT_KNOBS->READ_TAG_SAMPLE_RATE),
	    transactionTagSampleCost(CLIENT_KNOBS->COMMIT_SAMPLE_COST) {}

	bool operator == (ClientDBInfo const& r) const { return id == r.id; }
	bool operator != (ClientDBInfo const& r) const { return id != r.id; }

	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, grvProxies, commitProxies, id, clientTxnInfoSampleRate, clientTxnInfoSizeLimit, forward,
		           transactionTagSampleRate, transactionTagSampleCost);
	}
};

struct CommitID {
	constexpr static FileIdentifier file_identifier = 14254927;
	Version version; 			// returns invalidVersion if transaction conflicts
	uint16_t txnBatchId;
	Optional<Value> metadataVersion;
	Optional<Standalone<VectorRef<int>>> conflictingKRIndices;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, txnBatchId, metadataVersion, conflictingKRIndices);
	}

	CommitID() : version(invalidVersion), txnBatchId(0) {}
	CommitID(Version version, uint16_t txnBatchId, const Optional<Value>& metadataVersion,
	         const Optional<Standalone<VectorRef<int>>>& conflictingKRIndices = Optional<Standalone<VectorRef<int>>>())
	  : version(version), txnBatchId(txnBatchId), metadataVersion(metadataVersion),
	    conflictingKRIndices(conflictingKRIndices) {}
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
	SpanID spanContext;
	CommitTransactionRef transaction;
	ReplyPromise<CommitID> reply;
	uint32_t flags;
	Optional<UID> debugID;
	Optional<ClientTrCommitCostEstimation> commitCostEstimation;
	Optional<TagSet> tagSet;

	CommitTransactionRequest() : flags(0) {}

	template <class Ar> 
	void serialize(Ar& ar) { 
		serializer(ar, transaction, reply, arena, flags, debugID, commitCostEstimation, tagSet, spanContext);
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

struct GetReadVersionReply : public BasicLoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 15709388;
	Version version;
	bool locked;
	Optional<Value> metadataVersion;
	int64_t midShardSize = 0;

	TransactionTagMap<ClientTagThrottleLimits> tagThrottleInfo;

	GetReadVersionReply() : version(invalidVersion), locked(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, BasicLoadBalancedReply::processBusyTime, version, locked, metadataVersion, tagThrottleInfo, midShardSize);
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
		FLAG_USE_MIN_KNOWN_COMMITTED_VERSION = 4,
		FLAG_USE_PROVISIONAL_PROXIES = 2,
		FLAG_CAUSAL_READ_RISKY = 1,
		FLAG_PRIORITY_MASK = PRIORITY_SYSTEM_IMMEDIATE,
	};

	SpanID spanContext;
	uint32_t transactionCount;
	uint32_t flags;
	TransactionPriority priority;

	TransactionTagMap<uint32_t> tags;

	Optional<UID> debugID;
	ReplyPromise<GetReadVersionReply> reply;

	GetReadVersionRequest() : transactionCount(1), flags(0) {}
	GetReadVersionRequest(SpanID spanContext, uint32_t transactionCount, TransactionPriority priority,
	                      uint32_t flags = 0, TransactionTagMap<uint32_t> tags = TransactionTagMap<uint32_t>(),
	                      Optional<UID> debugID = Optional<UID>())
	  : spanContext(spanContext), transactionCount(transactionCount), priority(priority), flags(flags), tags(tags),
	    debugID(debugID) {
		flags = flags & ~FLAG_PRIORITY_MASK;
		switch(priority) {
			case TransactionPriority::BATCH:
				flags |= PRIORITY_BATCH;
				break;
			case TransactionPriority::DEFAULT:
				flags |= PRIORITY_DEFAULT;
				break;
			case TransactionPriority::IMMEDIATE:
				flags |= PRIORITY_SYSTEM_IMMEDIATE;
				break;
			default:
				ASSERT(false);
		}
	}

	bool operator < (GetReadVersionRequest const& rhs) const { return priority < rhs.priority; }

	template <class Ar> 
	void serialize(Ar& ar) { 
		serializer(ar, transactionCount, flags, tags, debugID, reply, spanContext);

		if(ar.isDeserializing) {
			if((flags & PRIORITY_SYSTEM_IMMEDIATE) == PRIORITY_SYSTEM_IMMEDIATE) {
				priority = TransactionPriority::IMMEDIATE;
			}
			else if((flags & PRIORITY_DEFAULT) == PRIORITY_DEFAULT) {
				priority = TransactionPriority::DEFAULT;
			}
			else if((flags & PRIORITY_BATCH) == PRIORITY_BATCH) {
				priority = TransactionPriority::BATCH;
			}
			else {
				priority = TransactionPriority::DEFAULT;
			}
		}
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
	SpanID spanContext;
	KeyRef begin;
	Optional<KeyRef> end;
	int limit;
	bool reverse;
	ReplyPromise<GetKeyServerLocationsReply> reply;

	GetKeyServerLocationsRequest() : limit(0), reverse(false) {}
	GetKeyServerLocationsRequest(SpanID spanContext, KeyRef const& begin, Optional<KeyRef> const& end, int limit,
	                             bool reverse, Arena const& arena)
	  : spanContext(spanContext), begin(begin), end(end), limit(limit), reverse(reverse), arena(arena) {}

	template <class Ar>
	void serialize(Ar& ar) { 
		serializer(ar, begin, end, limit, reverse, reply, spanContext, arena);
	}
};

struct GetRawCommittedVersionReply {
	constexpr static FileIdentifier file_identifier = 1314732;
	Optional<UID> debugID;
	Version version;
	bool locked;
	Optional<Value> metadataVersion;
	Version minKnownCommittedVersion;

	GetRawCommittedVersionReply(): debugID(Optional<UID>()), version(invalidVersion), locked(false), metadataVersion(Optional<Value>()), minKnownCommittedVersion(invalidVersion) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, debugID, version, locked, metadataVersion, minKnownCommittedVersion);
	}
};

struct GetRawCommittedVersionRequest {
	constexpr static FileIdentifier file_identifier = 12954034;
	SpanID spanContext;
	Optional<UID> debugID;
	ReplyPromise<GetRawCommittedVersionReply> reply;

	explicit GetRawCommittedVersionRequest(SpanID spanContext, Optional<UID> const& debugID = Optional<UID>()) : spanContext(spanContext), debugID(debugID) {}
	explicit GetRawCommittedVersionRequest() : spanContext(), debugID() {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, debugID, reply, spanContext);
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
	std::vector<Endpoint> broadcastInfo;
	ReplyPromise<Void> reply;

	template <class Ar> 
	void serialize(Ar& ar) { 
		serializer(ar, data, sequence, last, broadcastInfo, reply, arena);
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

struct GetDDMetricsReply
{
	constexpr static FileIdentifier file_identifier = 7277713;
	Standalone<VectorRef<DDMetricsRef>> storageMetricsList;

	GetDDMetricsReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, storageMetricsList);
	}
};

struct GetDDMetricsRequest {
	constexpr static FileIdentifier file_identifier = 14536812;
	KeyRange keys;
	int shardLimit;
	ReplyPromise<struct GetDDMetricsReply> reply;

	GetDDMetricsRequest() {}
	explicit GetDDMetricsRequest(KeyRange const& keys, const int shardLimit) : keys(keys), shardLimit(shardLimit) {}

	template<class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, shardLimit, reply);
  }
};

struct ProxySnapRequest
{
	constexpr static FileIdentifier file_identifier = 5427684;
	Arena arena;
	StringRef snapPayload; // command used to snapshot the data folder
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

struct ExclusionSafetyCheckReply
{
	constexpr static FileIdentifier file_identifier = 11;
	bool safe;

	ExclusionSafetyCheckReply() : safe(false) {}
	explicit ExclusionSafetyCheckReply(bool safe) : safe(safe) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, safe);
	}
};

struct ExclusionSafetyCheckRequest
{
	constexpr static FileIdentifier file_identifier = 13852702;
	vector<AddressExclusion> exclusions;
	ReplyPromise<ExclusionSafetyCheckReply> reply;

	ExclusionSafetyCheckRequest() {}
	explicit ExclusionSafetyCheckRequest(vector<AddressExclusion> exclusions) : exclusions(exclusions) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, exclusions, reply);
	}
};

#endif
