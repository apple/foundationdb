/*
 * StorageServerInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_STORAGESERVERINTERFACE_H
#define FDBCLIENT_STORAGESERVERINTERFACE_H
#pragma once

#include "fdbclient/Audit.h"
#include "fdbclient/BulkDumping.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageCheckpoint.h"
#include "fdbclient/StorageServerShard.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "fdbrpc/Stats.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbrpc/TenantInfo.h"
#include "fdbrpc/TSSComparison.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbclient/Tenant.h"
#include "fdbclient/Tracing.h"
#include "flow/UnitTest.h"
#include "fdbclient/VersionVector.h"

// Dead code, removed in the next protocol version
struct VersionReply {
	constexpr static FileIdentifier file_identifier = 3;

	Version version;
	VersionReply() = default;
	explicit VersionReply(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

// This struct is used by RK to forward the commit cost to SS, see discussion in #7258
struct UpdateCommitCostRequest {
	constexpr static FileIdentifier file_identifier = 4159439;

	// Ratekeeper ID, it is only reasonable to compare postTime from the same Ratekeeper
	UID ratekeeperID;

	// The time the request being posted
	double postTime;

	double elapsed;
	TransactionTag busiestTag;

	// Properties that are defined in TransactionCommitCostEstimation
	int opsSum;
	uint64_t costSum;

	uint64_t totalWriteCosts;
	bool reported;

	ReplyPromise<Void> reply;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, ratekeeperID, postTime, elapsed, busiestTag, opsSum, costSum, totalWriteCosts, reported, reply);
	}
};

struct StorageServerInterface {
	constexpr static FileIdentifier file_identifier = 15302073;
	enum { BUSY_ALLOWED = 0, BUSY_FORCE = 1, BUSY_LOCAL = 2 };

	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 0 };

	LocalityData locality;
	UID uniqueID;
	Optional<UID> tssPairID;

	PublicRequestStream<struct GetValueRequest> getValue;
	PublicRequestStream<struct GetKeyRequest> getKey;

	// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
	// selector offset prevents all data from being read in one range read
	PublicRequestStream<struct GetKeyValuesRequest> getKeyValues;
	PublicRequestStream<struct GetMappedKeyValuesRequest> getMappedKeyValues;

	RequestStream<struct GetShardStateRequest> getShardState;
	PublicRequestStream<struct WaitMetricsRequest> waitMetrics;
	RequestStream<struct SplitMetricsRequest> splitMetrics;
	RequestStream<struct GetStorageMetricsRequest> getStorageMetrics;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct StorageQueuingMetricsRequest> getQueuingMetrics;

	RequestStream<ReplyPromise<KeyValueStoreType>> getKeyValueStoreType;
	PublicRequestStream<struct WatchValueRequest> watchValue;
	RequestStream<struct ReadHotSubRangeRequest> getReadHotRanges;
	RequestStream<struct SplitRangeRequest> getRangeSplitPoints;
	PublicRequestStream<struct GetKeyValuesStreamRequest> getKeyValuesStream;
	RequestStream<struct ChangeFeedStreamRequest> changeFeedStream;
	RequestStream<struct OverlappingChangeFeedsRequest> overlappingChangeFeeds;
	RequestStream<struct ChangeFeedPopRequest> changeFeedPop;
	RequestStream<struct ChangeFeedVersionUpdateRequest> changeFeedVersionUpdate;
	RequestStream<struct GetCheckpointRequest> checkpoint;
	RequestStream<struct FetchCheckpointRequest> fetchCheckpoint;
	RequestStream<struct FetchCheckpointKeyValuesRequest> fetchCheckpointKeyValues;
	RequestStream<struct UpdateCommitCostRequest> updateCommitCostRequest;
	RequestStream<struct AuditStorageRequest> auditStorage;
	RequestStream<struct GetHotShardsRequest> getHotShards;
	RequestStream<struct GetStorageCheckSumRequest> getCheckSum;
	RequestStream<struct BulkDumpRequest> bulkdump;

private:
	bool acceptingRequests;

public:
	explicit StorageServerInterface(UID uid) : uniqueID(uid) { acceptingRequests = false; }
	StorageServerInterface() : uniqueID(deterministicRandom()->randomUniqueID()) { acceptingRequests = false; }
	NetworkAddress address() const { return getValue.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return getValue.getEndpoint().getStableAddress(); }
	Optional<NetworkAddress> secondaryAddress() const { return getValue.getEndpoint().addresses.secondaryAddress; }
	UID id() const { return uniqueID; }
	bool isAcceptingRequests() const { return acceptingRequests; }
	void startAcceptingRequests() { acceptingRequests = true; }
	void stopAcceptingRequests() { acceptingRequests = false; }
	bool isTss() const { return tssPairID.present(); }
	std::string toString() const { return id().shortString(); }
	template <class Ar>
	void serialize(Ar& ar) {
		// StorageServerInterface is persisted in the database, so changes here have to be versioned carefully!
		// To change this serialization, ProtocolVersion::ServerListValue must be updated, and downgrades need to be
		// considered

		ASSERT_WE_THINK(ar.protocolVersion().hasSmallEndpoints()); // 6.3
		ASSERT_WE_THINK(ar.protocolVersion().hasTSS()); // 7.0
		ASSERT_WE_THINK(ar.protocolVersion().hasStorageInterfaceReadiness()); // 7.1

		serializer(ar, uniqueID, locality, getValue, tssPairID, acceptingRequests);

		if (Ar::isDeserializing) {
			getKey = PublicRequestStream<struct GetKeyRequest>(getValue.getEndpoint().getAdjustedEndpoint(1));
			getKeyValues =
			    PublicRequestStream<struct GetKeyValuesRequest>(getValue.getEndpoint().getAdjustedEndpoint(2));
			getShardState = RequestStream<struct GetShardStateRequest>(getValue.getEndpoint().getAdjustedEndpoint(3));
			waitMetrics = PublicRequestStream<struct WaitMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(4));
			splitMetrics = RequestStream<struct SplitMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(5));
			getStorageMetrics =
			    RequestStream<struct GetStorageMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(6));
			waitFailure = RequestStream<ReplyPromise<Void>>(getValue.getEndpoint().getAdjustedEndpoint(7));
			getQueuingMetrics =
			    RequestStream<struct StorageQueuingMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(8));
			getKeyValueStoreType =
			    RequestStream<ReplyPromise<KeyValueStoreType>>(getValue.getEndpoint().getAdjustedEndpoint(9));
			watchValue = PublicRequestStream<struct WatchValueRequest>(getValue.getEndpoint().getAdjustedEndpoint(10));
			getReadHotRanges =
			    RequestStream<struct ReadHotSubRangeRequest>(getValue.getEndpoint().getAdjustedEndpoint(11));
			getRangeSplitPoints =
			    RequestStream<struct SplitRangeRequest>(getValue.getEndpoint().getAdjustedEndpoint(12));
			getKeyValuesStream =
			    PublicRequestStream<struct GetKeyValuesStreamRequest>(getValue.getEndpoint().getAdjustedEndpoint(13));
			getMappedKeyValues =
			    PublicRequestStream<struct GetMappedKeyValuesRequest>(getValue.getEndpoint().getAdjustedEndpoint(14));
			changeFeedStream =
			    RequestStream<struct ChangeFeedStreamRequest>(getValue.getEndpoint().getAdjustedEndpoint(15));
			overlappingChangeFeeds =
			    RequestStream<struct OverlappingChangeFeedsRequest>(getValue.getEndpoint().getAdjustedEndpoint(16));
			changeFeedPop = RequestStream<struct ChangeFeedPopRequest>(getValue.getEndpoint().getAdjustedEndpoint(17));
			changeFeedVersionUpdate =
			    RequestStream<struct ChangeFeedVersionUpdateRequest>(getValue.getEndpoint().getAdjustedEndpoint(18));
			checkpoint = RequestStream<struct GetCheckpointRequest>(getValue.getEndpoint().getAdjustedEndpoint(19));
			fetchCheckpoint =
			    RequestStream<struct FetchCheckpointRequest>(getValue.getEndpoint().getAdjustedEndpoint(20));
			fetchCheckpointKeyValues =
			    RequestStream<struct FetchCheckpointKeyValuesRequest>(getValue.getEndpoint().getAdjustedEndpoint(21));
			updateCommitCostRequest =
			    RequestStream<struct UpdateCommitCostRequest>(getValue.getEndpoint().getAdjustedEndpoint(22));
			auditStorage = RequestStream<struct AuditStorageRequest>(getValue.getEndpoint().getAdjustedEndpoint(23));
			getHotShards = RequestStream<struct GetHotShardsRequest>(getValue.getEndpoint().getAdjustedEndpoint(24));
			getCheckSum =
			    RequestStream<struct GetStorageCheckSumRequest>(getValue.getEndpoint().getAdjustedEndpoint(25));
			bulkdump = RequestStream<struct BulkDumpRequest>(getValue.getEndpoint().getAdjustedEndpoint(26));
		}
	}
	bool operator==(StorageServerInterface const& s) const { return uniqueID == s.uniqueID; }
	bool operator<(StorageServerInterface const& s) const { return uniqueID < s.uniqueID; }
	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(getValue.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getKey.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getKeyValues.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getShardState.getReceiver());
		streams.push_back(waitMetrics.getReceiver());
		streams.push_back(splitMetrics.getReceiver());
		streams.push_back(getStorageMetrics.getReceiver());
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(getQueuingMetrics.getReceiver());
		streams.push_back(getKeyValueStoreType.getReceiver());
		streams.push_back(watchValue.getReceiver());
		streams.push_back(getReadHotRanges.getReceiver());
		streams.push_back(getRangeSplitPoints.getReceiver());
		streams.push_back(getKeyValuesStream.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(getMappedKeyValues.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(changeFeedStream.getReceiver());
		streams.push_back(overlappingChangeFeeds.getReceiver());
		streams.push_back(changeFeedPop.getReceiver());
		streams.push_back(changeFeedVersionUpdate.getReceiver());
		streams.push_back(checkpoint.getReceiver());
		streams.push_back(fetchCheckpoint.getReceiver());
		streams.push_back(fetchCheckpointKeyValues.getReceiver());
		streams.push_back(updateCommitCostRequest.getReceiver());
		streams.push_back(auditStorage.getReceiver());
		streams.push_back(getHotShards.getReceiver());
		streams.push_back(getCheckSum.getReceiver());
		streams.push_back(bulkdump.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct StorageInfo : NonCopyable, public ReferenceCounted<StorageInfo> {
	Tag tag;
	StorageServerInterface interf;
	StorageInfo() : tag(invalidTag) {}
};

struct StorageServerMetaInfo : public StorageServerInterface {
	Optional<StorageMetadataType> metadata;

	StorageServerMetaInfo(const StorageServerInterface& interface,
	                      Optional<StorageMetadataType> metadata = Optional<StorageMetadataType>())
	  : StorageServerInterface(interface), metadata(metadata) {}
};

struct ServerCacheInfo {
	std::vector<Tag> tags; // all tags in both primary and remote DC for the key-range
	std::vector<Reference<StorageInfo>> src_info;
	std::vector<Reference<StorageInfo>> dest_info;

	void populateTags() {
		if (tags.size())
			return;

		for (const auto& info : src_info) {
			tags.push_back(info->tag);
		}
		for (const auto& info : dest_info) {
			tags.push_back(info->tag);
		}
		uniquify(tags);
	}
};

struct GetValueReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1378929;
	Optional<Value> value;
	bool cached;

	GetValueReply() : cached(false) {}
	GetValueReply(Optional<Value> value, bool cached) : value(value), cached(cached) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, value, cached);
	}
};

struct GetValueRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 8454530;
	SpanContext spanContext;
	TenantInfo tenantInfo;
	Key key;
	Version version;
	Optional<TagSet> tags;
	ReplyPromise<GetValueReply> reply;
	Optional<ReadOptions> options;
	VersionVector ssLatestCommitVersions; // includes the latest commit versions, as known
	                                      // to this client, of all storage replicas that
	                                      // serve the given key
	GetValueRequest() {}

	bool verify() const { return tenantInfo.isAuthorized(); }

	GetValueRequest(SpanContext spanContext,
	                const TenantInfo& tenantInfo,
	                const Key& key,
	                Version ver,
	                Optional<TagSet> tags,
	                Optional<ReadOptions> options,
	                VersionVector latestCommitVersions)
	  : spanContext(spanContext), tenantInfo(tenantInfo), key(key), version(ver), tags(tags), options(options),
	    ssLatestCommitVersions(latestCommitVersions) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, version, tags, reply, spanContext, tenantInfo, options, ssLatestCommitVersions);
	}
};

struct WatchValueReply {
	constexpr static FileIdentifier file_identifier = 3;

	Version version;
	bool cached = false;
	WatchValueReply() = default;
	explicit WatchValueReply(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, cached);
	}
};

struct WatchValueRequest {
	constexpr static FileIdentifier file_identifier = 14747733;
	SpanContext spanContext;
	TenantInfo tenantInfo;
	Key key;
	Optional<Value> value;
	Version version;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<WatchValueReply> reply;

	WatchValueRequest() {}

	WatchValueRequest(SpanContext spanContext,
	                  TenantInfo tenantInfo,
	                  const Key& key,
	                  Optional<Value> value,
	                  Version ver,
	                  Optional<TagSet> tags,
	                  Optional<UID> debugID)
	  : spanContext(spanContext), tenantInfo(tenantInfo), key(key), value(value), version(ver), tags(tags),
	    debugID(debugID) {}

	bool verify() const { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value, version, tags, debugID, reply, spanContext, tenantInfo);
	}
};

struct GetKeyValuesReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	Version version; // useful when latestVersion was requested
	bool more;
	bool cached = false;

	GetKeyValuesReply() : version(invalidVersion), more(false), cached(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, data, version, more, cached, arena);
	}
};

struct GetKeyValuesRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanContext spanContext;
	Arena arena;
	TenantInfo tenantInfo;
	KeySelectorRef begin, end;
	// This is a dummy field there has never been used.
	// TODO: Get rid of this by constexpr or other template magic in getRange
	KeyRef mapper = KeyRef();
	Version version; // or latestVersion
	int limit, limitBytes;
	Optional<TagSet> tags;
	Optional<ReadOptions> options;
	ReplyPromise<GetKeyValuesReply> reply;
	VersionVector ssLatestCommitVersions; // includes the latest commit versions, as known
	                                      // to this client, of all storage replicas that
	                                      // serve the given key

	GetKeyValuesRequest() {}

	bool verify() const { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           begin,
		           end,
		           version,
		           limit,
		           limitBytes,
		           tags,
		           reply,
		           spanContext,
		           tenantInfo,
		           options,
		           ssLatestCommitVersions,
		           arena);
	}
};

struct GetMappedKeyValuesReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1783067;
	Arena arena;
	// MappedKeyValueRef is not string_serialized_traits, so we have to use FlatBuffers.
	VectorRef<MappedKeyValueRef, VecSerStrategy::FlatBuffers> data;

	Version version; // useful when latestVersion was requested
	bool more;
	bool cached = false;

	GetMappedKeyValuesReply() : version(invalidVersion), more(false), cached(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, data, version, more, cached, arena);
	}
};

struct GetMappedKeyValuesRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 6795747;
	SpanContext spanContext;
	Arena arena;
	TenantInfo tenantInfo;
	KeySelectorRef begin, end;
	KeyRef mapper;
	Version version; // or latestVersion
	int limit, limitBytes;
	Optional<TagSet> tags;
	Optional<ReadOptions> options;
	ReplyPromise<GetMappedKeyValuesReply> reply;
	VersionVector ssLatestCommitVersions; // includes the latest commit versions, as known
	                                      // to this client, of all storage replicas that
	                                      // serve the given key range

	GetMappedKeyValuesRequest() {}

	bool verify() const { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           begin,
		           end,
		           mapper,
		           version,
		           limit,
		           limitBytes,
		           tags,
		           reply,
		           spanContext,
		           tenantInfo,
		           options,
		           ssLatestCommitVersions,
		           arena);
	}
};

struct GetKeyValuesStreamReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	Version version; // useful when latestVersion was requested
	bool more;
	bool cached = false;

	GetKeyValuesStreamReply() : version(invalidVersion), more(false), cached(false) {}
	GetKeyValuesStreamReply(GetKeyValuesReply r)
	  : arena(r.arena), data(r.data), version(r.version), more(r.more), cached(r.cached) {}

	int expectedSize() const { return sizeof(GetKeyValuesStreamReply) + data.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           ReplyPromiseStreamReply::acknowledgeToken,
		           ReplyPromiseStreamReply::sequence,
		           data,
		           version,
		           more,
		           cached,
		           arena);
	}
};

struct GetKeyValuesStreamRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanContext spanContext;
	Arena arena;
	TenantInfo tenantInfo;
	KeySelectorRef begin, end;
	Version version; // or latestVersion
	int limit, limitBytes;
	Optional<TagSet> tags;
	Optional<ReadOptions> options;
	ReplyPromiseStream<GetKeyValuesStreamReply> reply;
	VersionVector ssLatestCommitVersions; // includes the latest commit versions, as known
	                                      // to this client, of all storage replicas that
	                                      // serve the given key range

	GetKeyValuesStreamRequest() {}

	bool verify() const { return tenantInfo.isAuthorized(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           begin,
		           end,
		           version,
		           limit,
		           limitBytes,
		           tags,
		           reply,
		           spanContext,
		           tenantInfo,
		           options,
		           ssLatestCommitVersions,
		           arena);
	}
};

struct GetKeyReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 11226513;
	KeySelector sel;
	bool cached;

	GetKeyReply() : cached(false) {}
	GetKeyReply(KeySelector sel, bool cached) : sel(sel), cached(cached) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, sel, cached);
	}
};

struct GetKeyRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 10457870;
	SpanContext spanContext;
	Arena arena;
	TenantInfo tenantInfo;
	KeySelectorRef sel;
	Version version; // or latestVersion
	Optional<TagSet> tags;
	ReplyPromise<GetKeyReply> reply;
	Optional<ReadOptions> options;
	VersionVector ssLatestCommitVersions; // includes the latest commit versions, as known
	                                      // to this client, of all storage replicas that
	                                      // serve the given key

	GetKeyRequest() {}

	bool verify() const { return tenantInfo.isAuthorized(); }

	GetKeyRequest(SpanContext spanContext,
	              TenantInfo tenantInfo,
	              KeySelectorRef const& sel,
	              Version version,
	              Optional<TagSet> tags,
	              Optional<ReadOptions> options,
	              VersionVector latestCommitVersions)
	  : spanContext(spanContext), tenantInfo(tenantInfo), sel(sel), version(version), tags(tags), options(options),
	    ssLatestCommitVersions(latestCommitVersions) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sel, version, tags, reply, spanContext, tenantInfo, options, ssLatestCommitVersions, arena);
	}
};

struct GetShardStateReply {
	constexpr static FileIdentifier file_identifier = 0;

	Version first;
	Version second;
	std::vector<StorageServerShard> shards;
	GetShardStateReply() = default;
	GetShardStateReply(Version first, Version second) : first(first), second(second) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, first, second, shards);
	}
};

struct GetShardStateRequest {
	constexpr static FileIdentifier file_identifier = 15860168;
	enum waitMode { NO_WAIT = 0, FETCHING = 1, READABLE = 2 };

	KeyRange keys;
	int32_t mode;
	bool includePhysicalShard;
	ReplyPromise<GetShardStateReply> reply;
	GetShardStateRequest() = default;
	GetShardStateRequest(KeyRange const& keys, waitMode mode, bool includePhysicalShard)
	  : keys(keys), mode(mode), includePhysicalShard(includePhysicalShard) {}
	GetShardStateRequest(KeyRange const& keys, waitMode mode) : keys(keys), mode(mode), includePhysicalShard(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, mode, reply, includePhysicalShard);
	}
};

struct StorageMetrics {
	constexpr static FileIdentifier file_identifier = 13622226;
	int64_t bytes = 0; // total storage
	int64_t bytesWrittenPerKSecond = 0; // bytes write to SQ

	// FIXME: currently, iosPerKSecond is not used in DataDistribution calculations.
	int64_t iosPerKSecond = 0;
	int64_t bytesReadPerKSecond = 0;
	int64_t opsReadPerKSecond = 0;

	static const int64_t infinity = 1LL << 60;

	// the read load model coming from both read ops and read bytes
	int64_t readLoadKSecond() const;

	bool allLessOrEqual(const StorageMetrics& rhs) const {
		return bytes <= rhs.bytes && bytesWrittenPerKSecond <= rhs.bytesWrittenPerKSecond &&
		       iosPerKSecond <= rhs.iosPerKSecond && bytesReadPerKSecond <= rhs.bytesReadPerKSecond &&
		       opsReadPerKSecond <= rhs.opsReadPerKSecond;
	}
	void operator+=(const StorageMetrics& rhs) {
		bytes += rhs.bytes;
		bytesWrittenPerKSecond += rhs.bytesWrittenPerKSecond;
		iosPerKSecond += rhs.iosPerKSecond;
		bytesReadPerKSecond += rhs.bytesReadPerKSecond;
		opsReadPerKSecond += rhs.opsReadPerKSecond;
	}
	void operator-=(const StorageMetrics& rhs) {
		bytes -= rhs.bytes;
		bytesWrittenPerKSecond -= rhs.bytesWrittenPerKSecond;
		iosPerKSecond -= rhs.iosPerKSecond;
		bytesReadPerKSecond -= rhs.bytesReadPerKSecond;
		opsReadPerKSecond -= rhs.opsReadPerKSecond;
	}
	template <class F>
	void operator*=(F f) {
		bytes *= f;
		bytesWrittenPerKSecond *= f;
		iosPerKSecond *= f;
		bytesReadPerKSecond *= f;
		opsReadPerKSecond *= f;
	}
	bool allZero() const {
		return !bytes && !bytesWrittenPerKSecond && !iosPerKSecond && !bytesReadPerKSecond && !opsReadPerKSecond;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, bytes, bytesWrittenPerKSecond, iosPerKSecond, bytesReadPerKSecond, opsReadPerKSecond);
	}

	void negate() { operator*=(-1.0); }
	StorageMetrics operator-() const {
		StorageMetrics x(*this);
		x.negate();
		return x;
	}
	StorageMetrics operator+(const StorageMetrics& r) const {
		StorageMetrics x(*this);
		x += r;
		return x;
	}
	StorageMetrics operator-(const StorageMetrics& r) const {
		StorageMetrics x(r);
		x.negate();
		x += *this;
		return x;
	}
	template <class F>
	StorageMetrics operator*(F f) const {
		StorageMetrics x(*this);
		x *= f;
		return x;
	}

	bool operator==(StorageMetrics const& rhs) const {
		return bytes == rhs.bytes && bytesWrittenPerKSecond == rhs.bytesWrittenPerKSecond &&
		       iosPerKSecond == rhs.iosPerKSecond && bytesReadPerKSecond == rhs.bytesReadPerKSecond &&
		       opsReadPerKSecond == rhs.opsReadPerKSecond;
	}

	std::string toString() const {
		return format("Bytes: %lld, BWritePerKSec: %lld, iosPerKSec: %lld, BReadPerKSec: %lld, OpReadPerKSec: %lld",
		              bytes,
		              bytesWrittenPerKSecond,
		              iosPerKSecond,
		              bytesReadPerKSecond,
		              opsReadPerKSecond);
	}
};

struct WaitMetricsRequest {
	// Waits for any of the given minimum or maximum metrics to be exceeded, and then returns the current values
	// Send a reversed range for min, max to receive an immediate report
	constexpr static FileIdentifier file_identifier = 1795961;
	Arena arena;
	// Setting the tenantInfo makes the request tenant-aware.
	TenantInfo tenantInfo;
	// Set `minVersion` to a version where the tenant info was read. Not needed for non-tenant-aware request.
	Version minVersion = 0;
	KeyRangeRef keys;
	StorageMetrics min, max;
	ReplyPromise<StorageMetrics> reply;

	bool verify() const { return tenantInfo.isAuthorized(); }

	WaitMetricsRequest() {}
	WaitMetricsRequest(TenantInfo tenantInfo,
	                   Version minVersion,
	                   KeyRangeRef const& keys,
	                   StorageMetrics const& min,
	                   StorageMetrics const& max)
	  : tenantInfo(tenantInfo), minVersion(minVersion), keys(arena, keys), min(min), max(max) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, min, max, reply, tenantInfo, minVersion, arena);
	}
};

struct SplitMetricsReply {
	constexpr static FileIdentifier file_identifier = 11530792;
	Standalone<VectorRef<KeyRef>> splits;
	StorageMetrics used;
	bool more = false;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, splits, used, more);
	}
};

struct SplitMetricsRequest {
	constexpr static FileIdentifier file_identifier = 10463876;
	Arena arena;
	KeyRangeRef keys;
	StorageMetrics limits;
	StorageMetrics used;
	StorageMetrics estimated;
	bool isLastShard;
	ReplyPromise<SplitMetricsReply> reply;
	Optional<int> minSplitBytes;

	SplitMetricsRequest() {}
	SplitMetricsRequest(KeyRangeRef const& keys,
	                    StorageMetrics const& limits,
	                    StorageMetrics const& used,
	                    StorageMetrics const& estimated,
	                    bool isLastShard,
	                    Optional<int> minSplitBytes)
	  : keys(arena, keys), limits(limits), used(used), estimated(estimated), isLastShard(isLastShard),
	    minSplitBytes(minSplitBytes) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, limits, used, estimated, isLastShard, reply, minSplitBytes, arena);
	}
};

// Should always be used inside a `Standalone`.
struct ReadHotRangeWithMetrics {
	KeyRangeRef keys;
	// density refers to the ratio of bytes sent(because of the read) and bytes on disk.
	// For example if key range [A, B) and [B, C) respectively has byte size 100 bytes on disk.
	// Key range [A,B) was read 30 times.
	// The density for key range [A,C) is 30 * 100 / 200 = 15
	double density;
	// How many bytes of data was sent in a period of time because of read requests.
	double readBandwidthSec;

	int64_t bytes; // storage bytes
	double readOpsSec; // an interpolated value over sampling interval

	ReadHotRangeWithMetrics() = default;
	ReadHotRangeWithMetrics(KeyRangeRef const& keys, double density, double readBandwidth)
	  : keys(keys), density(density), readBandwidthSec(readBandwidth) {}

	ReadHotRangeWithMetrics(KeyRangeRef const& keys, int64_t bytes, double readBandwidth, double readOpsKSec)
	  : keys(keys), density(readBandwidth / std::max((int64_t)1, bytes)), readBandwidthSec(readBandwidth), bytes(bytes),
	    readOpsSec(readOpsKSec) {}

	ReadHotRangeWithMetrics(Arena& arena, const ReadHotRangeWithMetrics& rhs)
	  : keys(arena, rhs.keys), density(rhs.density), readBandwidthSec(rhs.readBandwidthSec), bytes(rhs.bytes),
	    readOpsSec(rhs.readOpsSec) {}

	int expectedSize() const {
		return keys.expectedSize() + sizeof(density) + sizeof(readBandwidthSec) + sizeof(bytes) + sizeof(readOpsSec);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, density, readBandwidthSec, bytes, readOpsSec);
	}
};

struct ReadHotSubRangeReply {
	constexpr static FileIdentifier file_identifier = 10424537;
	Standalone<VectorRef<ReadHotRangeWithMetrics>> readHotRanges;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, readHotRanges);
	}
};
struct ReadHotSubRangeRequest {
	constexpr static FileIdentifier file_identifier = 10259266;
	enum SplitType : uint8_t { BYTES, READ_BYTES, READ_OPS };

	Arena arena;
	KeyRangeRef keys;
	ReplyPromise<ReadHotSubRangeReply> reply;

	uint8_t type = SplitType::BYTES;
	int chunkCount = 1;

	ReadHotSubRangeRequest() {}
	ReadHotSubRangeRequest(KeyRangeRef const& keys, SplitType type = SplitType::BYTES, int chunkCount = 1)
	  : keys(arena, keys), type(type), chunkCount(chunkCount) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, reply, type, chunkCount, arena);
	}
};

struct SplitRangeReply {
	constexpr static FileIdentifier file_identifier = 11813134;
	// If the given range can be divided, contains the split points.
	// If the given range cannot be divided(for example its total size is smaller than the chunk size), this would be
	// empty
	Standalone<VectorRef<KeyRef>> splitPoints;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, splitPoints);
	}
};

struct SplitRangeRequest {
	constexpr static FileIdentifier file_identifier = 10725174;
	Arena arena;
	TenantInfo tenantInfo;
	KeyRangeRef keys;
	int64_t chunkSize;
	ReplyPromise<SplitRangeReply> reply;

	SplitRangeRequest() {}
	SplitRangeRequest(TenantInfo tenantInfo, KeyRangeRef const& keys, int64_t chunkSize)
	  : tenantInfo(tenantInfo), keys(arena, keys), chunkSize(chunkSize) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, chunkSize, reply, tenantInfo, arena);
	}
};

struct ChangeFeedStreamReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<MutationsAndVersionRef> mutations;
	bool atLatestVersion = false;
	Version minStreamVersion = invalidVersion;
	Version popVersion = invalidVersion;

	ChangeFeedStreamReply() {}

	int expectedSize() const { return sizeof(ChangeFeedStreamReply) + mutations.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           ReplyPromiseStreamReply::acknowledgeToken,
		           ReplyPromiseStreamReply::sequence,
		           mutations,
		           atLatestVersion,
		           minStreamVersion,
		           popVersion,
		           arena);
	}
};

struct ChangeFeedStreamRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanContext spanContext;
	Arena arena;
	Key rangeID;
	Version begin = 0;
	Version end = 0;
	KeyRange range;
	int replyBufferSize = -1;
	bool canReadPopped = true;
	UID id; // This must be globally unique among ChangeFeedStreamRequest instances
	Optional<ReadOptions> options;
	bool encrypted = false;

	ReplyPromiseStream<ChangeFeedStreamReply> reply;

	ChangeFeedStreamRequest() {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           rangeID,
		           begin,
		           end,
		           range,
		           reply,
		           spanContext,
		           replyBufferSize,
		           canReadPopped,
		           id,
		           options,
		           encrypted,
		           arena);
	}
};

struct ChangeFeedPopRequest {
	constexpr static FileIdentifier file_identifier = 10726174;
	Key rangeID;
	Version version;
	KeyRange range;
	ReplyPromise<Void> reply;

	ChangeFeedPopRequest() {}
	ChangeFeedPopRequest(Key const& rangeID, Version version, KeyRange const& range)
	  : rangeID(rangeID), version(version), range(range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeID, version, range, reply);
	}
};

// Request to search for a checkpoint for a minimum keyrange: `range`, at the specific version,
// in the specific format.
// A CheckpointMetaData will be returned if the specific checkpoint is found.
struct GetCheckpointRequest {
	constexpr static FileIdentifier file_identifier = 13804343;
	Version version; // The FDB version at which the checkpoint is created.
	std::vector<KeyRange> ranges;
	int16_t format; // CheckpointFormat.
	Optional<UID> actionId;
	ReplyPromise<CheckpointMetaData> reply;

	GetCheckpointRequest() {}
	GetCheckpointRequest(std::vector<KeyRange> ranges,
	                     Version version,
	                     CheckpointFormat format,
	                     const Optional<UID>& actionId)
	  : version(version), ranges(ranges), format(format), actionId(actionId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, ranges, format, actionId, reply);
	}
};

// Reply to FetchCheckpointRequest, transfers checkpoint back to client.
struct FetchCheckpointReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 13804345;
	Standalone<StringRef> token; // Serialized data specific to a particular checkpoint format.
	Standalone<StringRef> data;

	FetchCheckpointReply() {}
	FetchCheckpointReply(StringRef token) : token(token) {}

	int expectedSize() const { return data.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ReplyPromiseStreamReply::acknowledgeToken, ReplyPromiseStreamReply::sequence, token, data);
	}
};

// Request to fetch checkpoint from a storage server.
struct FetchCheckpointRequest {
	constexpr static FileIdentifier file_identifier = 13804344;
	UID checkpointID;
	Standalone<StringRef> token; // Serialized data specific to a particular checkpoint format.
	ReplyPromiseStream<FetchCheckpointReply> reply;

	FetchCheckpointRequest() = default;
	FetchCheckpointRequest(UID checkpointID, StringRef token) : checkpointID(checkpointID), token(token) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checkpointID, token, reply);
	}
};

struct FetchCheckpointKeyValuesStreamReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 13804353;
	Arena arena;
	VectorRef<KeyValueRef> data;

	FetchCheckpointKeyValuesStreamReply() = default;

	int expectedSize() const { return data.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ReplyPromiseStreamReply::acknowledgeToken, ReplyPromiseStreamReply::sequence, data, arena);
	}
};

// Fetch checkpoint in the format of key-value pairs.
struct FetchCheckpointKeyValuesRequest {
	constexpr static FileIdentifier file_identifier = 13804354;
	UID checkpointID;
	KeyRange range;
	ReplyPromiseStream<FetchCheckpointKeyValuesStreamReply> reply;

	FetchCheckpointKeyValuesRequest() = default;
	FetchCheckpointKeyValuesRequest(UID checkpointID, KeyRange range) : checkpointID(checkpointID), range(range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checkpointID, range, reply);
	}
};

struct OverlappingChangeFeedEntry {
	KeyRef feedId;
	KeyRangeRef range;
	Version emptyVersion;
	Version stopVersion;
	Version feedMetadataVersion;

	bool operator==(const OverlappingChangeFeedEntry& r) const {
		return feedId == r.feedId && range == r.range && emptyVersion == r.emptyVersion &&
		       stopVersion == r.stopVersion && feedMetadataVersion == r.feedMetadataVersion;
	}

	OverlappingChangeFeedEntry() {}
	OverlappingChangeFeedEntry(KeyRef const& feedId,
	                           KeyRangeRef const& range,
	                           Version emptyVersion,
	                           Version stopVersion,
	                           Version feedMetadataVersion)
	  : feedId(feedId), range(range), emptyVersion(emptyVersion), stopVersion(stopVersion),
	    feedMetadataVersion(feedMetadataVersion) {}

	OverlappingChangeFeedEntry(Arena& arena, const OverlappingChangeFeedEntry& rhs)
	  : feedId(arena, rhs.feedId), range(arena, rhs.range), emptyVersion(rhs.emptyVersion),
	    stopVersion(rhs.stopVersion), feedMetadataVersion(rhs.feedMetadataVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, feedId, range, emptyVersion, stopVersion, feedMetadataVersion);
	}
};

struct OverlappingChangeFeedsReply {
	constexpr static FileIdentifier file_identifier = 11815134;
	VectorRef<OverlappingChangeFeedEntry> feeds;
	bool cached;
	Arena arena;
	Version feedMetadataVersion;

	OverlappingChangeFeedsReply() : cached(false), feedMetadataVersion(invalidVersion) {}
	explicit OverlappingChangeFeedsReply(VectorRef<OverlappingChangeFeedEntry> const& feeds,
	                                     Version feedMetadataVersion)
	  : feeds(feeds), cached(false), feedMetadataVersion(feedMetadataVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, feeds, feedMetadataVersion, arena);
	}
};

struct OverlappingChangeFeedsRequest {
	constexpr static FileIdentifier file_identifier = 7228462;
	KeyRange range;
	Version minVersion;
	ReplyPromise<OverlappingChangeFeedsReply> reply;

	OverlappingChangeFeedsRequest() {}
	explicit OverlappingChangeFeedsRequest(KeyRange const& range) : range(range) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, minVersion, reply);
	}
};

struct ChangeFeedVersionUpdateReply {
	constexpr static FileIdentifier file_identifier = 4246160;
	Version version = 0;

	ChangeFeedVersionUpdateReply() {}
	explicit ChangeFeedVersionUpdateReply(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct ChangeFeedVersionUpdateRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	Version minVersion;
	ReplyPromise<ChangeFeedVersionUpdateReply> reply;

	ChangeFeedVersionUpdateRequest() {}
	explicit ChangeFeedVersionUpdateRequest(Version minVersion) : minVersion(minVersion) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, minVersion, reply);
	}
};

struct GetStorageMetricsReply {
	constexpr static FileIdentifier file_identifier = 15491478;
	StorageMetrics load; // sum of key-value metrics (logical bytes)
	StorageMetrics available; // physical bytes
	StorageMetrics capacity; // physical bytes
	double bytesInputRate = 0;
	int64_t versionLag = 0;
	double lastUpdate = 0;
	int64_t bytesDurable = 0, bytesInput = 0;

	GetStorageMetricsReply() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, load, available, capacity, bytesInputRate, versionLag, lastUpdate, bytesDurable, bytesInput);
	}
};

struct GetStorageMetricsRequest {
	constexpr static FileIdentifier file_identifier = 13290999;
	ReplyPromise<GetStorageMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// Tracks the busyness of tags on individual storage servers.
struct BusyTagInfo {
	constexpr static FileIdentifier file_identifier = 4528694;
	TransactionTag tag;
	double rate{ 0.0 };
	double fractionalBusyness{ 0.0 };

	BusyTagInfo() = default;
	BusyTagInfo(TransactionTag const& tag, double rate, double fractionalBusyness)
	  : tag(tag), rate(rate), fractionalBusyness(fractionalBusyness) {}

	bool operator<(BusyTagInfo const& rhs) const { return rate < rhs.rate; }
	bool operator>(BusyTagInfo const& rhs) const { return rate > rhs.rate; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tag, rate, fractionalBusyness);
	}
};

struct StorageQueuingMetricsReply {
	constexpr static FileIdentifier file_identifier = 7633366;
	double localTime;
	int64_t instanceID; // changes if bytesDurable and bytesInput reset
	int64_t bytesDurable{ 0 }, bytesInput{ 0 };
	StorageBytes storageBytes;
	Version version; // current storage server version
	Version durableVersion; // latest version durable on storage server
	double cpuUsage{ 0.0 };
	double diskUsage{ 0.0 };
	double localRateLimit;
	std::vector<BusyTagInfo> busiestTags;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           localTime,
		           instanceID,
		           bytesDurable,
		           bytesInput,
		           version,
		           storageBytes,
		           durableVersion,
		           cpuUsage,
		           diskUsage,
		           localRateLimit,
		           busiestTags);
	}
};

struct StorageQueuingMetricsRequest {
	// SOMEDAY: Send threshold value to avoid polling faster than the information changes?
	constexpr static FileIdentifier file_identifier = 3978640;
	ReplyPromise<struct StorageQueuingMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct GetHotShardsReply {
	constexpr static FileIdentifier file_identifier = 3828140;
	std::vector<KeyRange> hotShards;

	GetHotShardsReply() {}
	explicit GetHotShardsReply(std::vector<KeyRange> hotShards) : hotShards(hotShards) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, hotShards);
	}
};

struct GetHotShardsRequest {
	constexpr static FileIdentifier file_identifier = 3828141;
	ReplyPromise<GetHotShardsReply> reply;

	GetHotShardsRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

enum class CheckSumMethod : uint8_t {
	Invalid = 0,
};

struct CheckSumMetaData {
	constexpr static FileIdentifier file_identifier = 3828142;
	KeyRange range;
	Version version;
	StringRef checkSumValue;

	CheckSumMetaData() {}
	CheckSumMetaData(KeyRange range, Version version, StringRef checkSumValue)
	  : range(range), version(version), checkSumValue(checkSumValue) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, version, checkSumValue);
	}
};

struct GetStorageCheckSumReply {
	constexpr static FileIdentifier file_identifier = 3828143;
	std::vector<CheckSumMetaData> checkSums;
	uint8_t checkSumMethod;

	GetStorageCheckSumReply() {}
	GetStorageCheckSumReply(const std::vector<CheckSumMetaData>& checkSums, CheckSumMethod checkSumMethod)
	  : checkSums(checkSums), checkSumMethod(static_cast<uint8_t>(checkSumMethod)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checkSums, checkSumMethod);
	}
};

struct GetStorageCheckSumRequest {
	constexpr static FileIdentifier file_identifier = 3828144;
	std::vector<std::pair<KeyRange, Optional<Version>>> ranges;
	Optional<UID> actionId;
	uint8_t checkSumMethod;
	ReplyPromise<GetStorageCheckSumReply> reply;

	GetStorageCheckSumRequest() {}
	GetStorageCheckSumRequest(const std::vector<std::pair<KeyRange, Optional<Version>>>& ranges,
	                          Optional<UID> actionId,
	                          CheckSumMethod checkSumMethod)
	  : ranges(ranges), actionId(actionId), checkSumMethod(static_cast<uint8_t>(checkSumMethod)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ranges, actionId, checkSumMethod, reply);
	}
};

struct BulkDumpRequest {
	constexpr static FileIdentifier file_identifier = 3828145;
	std::vector<UID> checksumServers;
	BulkDumpState bulkDumpState;
	ReplyPromise<BulkDumpState> reply;

	BulkDumpRequest() {}
	BulkDumpRequest(const std::vector<UID>& checksumServers, const BulkDumpState& bulkDumpState)
	  : checksumServers(checksumServers), bulkDumpState(bulkDumpState){};

	std::string toString() const {
		return "[BulkDumpState]: " + bulkDumpState.toString() + ", [ChecksumServers]: " + describe(checksumServers);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checksumServers, bulkDumpState, reply);
	}
};

// Memory size for storing mutation in the mutation log and the versioned map.
inline int mvccStorageBytes(int mutationBytes) {
	// Why * 2:
	// - 1 insertion into version map costs 2 nodes in avg;
	// - The mutation will be stored in both mutation log and versioned map;
	return VersionedMap<KeyRef, ValueOrClearToRef>::overheadPerItem * 2 +
	       (mutationBytes + MutationRef::OVERHEAD_BYTES) * 2;
}

#endif
