/*
 * StorageServerInterface.h
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

#ifndef FDBCLIENT_STORAGESERVERINTERFACE_H
#define FDBCLIENT_STORAGESERVERINTERFACE_H
#pragma once

#include <ostream>
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "fdbrpc/Stats.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbrpc/TSSComparison.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/TagThrottle.actor.h"
#include "flow/UnitTest.h"

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

struct StorageServerInterface {
	constexpr static FileIdentifier file_identifier = 15302073;
	enum { BUSY_ALLOWED = 0, BUSY_FORCE = 1, BUSY_LOCAL = 2 };

	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 0 };

	LocalityData locality;
	UID uniqueID;
	Optional<UID> tssPairID;

	RequestStream<struct GetValueRequest> getValue;
	RequestStream<struct GetKeyRequest> getKey;

	// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
	// selector offset prevents all data from being read in one range read
	RequestStream<struct GetKeyValuesRequest> getKeyValues;
	RequestStream<struct GetKeyValuesAndFlatMapRequest> getKeyValuesAndFlatMap;

	RequestStream<struct GetShardStateRequest> getShardState;
	RequestStream<struct WaitMetricsRequest> waitMetrics;
	RequestStream<struct SplitMetricsRequest> splitMetrics;
	RequestStream<struct GetStorageMetricsRequest> getStorageMetrics;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct StorageQueuingMetricsRequest> getQueuingMetrics;

	RequestStream<ReplyPromise<KeyValueStoreType>> getKeyValueStoreType;
	RequestStream<struct WatchValueRequest> watchValue;
	RequestStream<struct ReadHotSubRangeRequest> getReadHotRanges;
	RequestStream<struct SplitRangeRequest> getRangeSplitPoints;
	RequestStream<struct GetKeyValuesStreamRequest> getKeyValuesStream;
	RequestStream<struct ChangeFeedStreamRequest> changeFeedStream;
	RequestStream<struct OverlappingChangeFeedsRequest> overlappingChangeFeeds;
	RequestStream<struct ChangeFeedPopRequest> changeFeedPop;
	RequestStream<struct ChangeFeedVersionUpdateRequest> changeFeedVersionUpdate;
	RequestStream<struct GetCheckpointRequest> checkpoint;
	RequestStream<struct GetFileRequest> getFile;

	explicit StorageServerInterface(UID uid) : uniqueID(uid) {}
	StorageServerInterface() : uniqueID(deterministicRandom()->randomUniqueID()) {}
	NetworkAddress address() const { return getValue.getEndpoint().getPrimaryAddress(); }
	NetworkAddress stableAddress() const { return getValue.getEndpoint().getStableAddress(); }
	Optional<NetworkAddress> secondaryAddress() const { return getValue.getEndpoint().addresses.secondaryAddress; }
	UID id() const { return uniqueID; }
	bool isTss() const { return tssPairID.present(); }
	std::string toString() const { return id().shortString(); }
	template <class Ar>
	void serialize(Ar& ar) {
		// StorageServerInterface is persisted in the database, so changes here have to be versioned carefully!
		// To change this serialization, ProtocolVersion::ServerListValue must be updated, and downgrades need to be
		// considered

		if (ar.protocolVersion().hasSmallEndpoints()) {
			if (ar.protocolVersion().hasTSS()) {
				serializer(ar, uniqueID, locality, getValue, tssPairID);
			} else {
				serializer(ar, uniqueID, locality, getValue);
			}
			if (Ar::isDeserializing) {
				getKey = RequestStream<struct GetKeyRequest>(getValue.getEndpoint().getAdjustedEndpoint(1));
				getKeyValues = RequestStream<struct GetKeyValuesRequest>(getValue.getEndpoint().getAdjustedEndpoint(2));
				getShardState =
				    RequestStream<struct GetShardStateRequest>(getValue.getEndpoint().getAdjustedEndpoint(3));
				waitMetrics = RequestStream<struct WaitMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(4));
				splitMetrics = RequestStream<struct SplitMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(5));
				getStorageMetrics =
				    RequestStream<struct GetStorageMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(6));
				waitFailure = RequestStream<ReplyPromise<Void>>(getValue.getEndpoint().getAdjustedEndpoint(7));
				getQueuingMetrics =
				    RequestStream<struct StorageQueuingMetricsRequest>(getValue.getEndpoint().getAdjustedEndpoint(8));
				getKeyValueStoreType =
				    RequestStream<ReplyPromise<KeyValueStoreType>>(getValue.getEndpoint().getAdjustedEndpoint(9));
				watchValue = RequestStream<struct WatchValueRequest>(getValue.getEndpoint().getAdjustedEndpoint(10));
				getReadHotRanges =
				    RequestStream<struct ReadHotSubRangeRequest>(getValue.getEndpoint().getAdjustedEndpoint(11));
				getRangeSplitPoints =
				    RequestStream<struct SplitRangeRequest>(getValue.getEndpoint().getAdjustedEndpoint(12));
				getKeyValuesStream =
				    RequestStream<struct GetKeyValuesStreamRequest>(getValue.getEndpoint().getAdjustedEndpoint(13));
				getKeyValuesAndFlatMap =
				    RequestStream<struct GetKeyValuesAndFlatMapRequest>(getValue.getEndpoint().getAdjustedEndpoint(14));
				changeFeedStream =
				    RequestStream<struct ChangeFeedStreamRequest>(getValue.getEndpoint().getAdjustedEndpoint(15));
				overlappingChangeFeeds =
				    RequestStream<struct OverlappingChangeFeedsRequest>(getValue.getEndpoint().getAdjustedEndpoint(16));
				changeFeedPop =
				    RequestStream<struct ChangeFeedPopRequest>(getValue.getEndpoint().getAdjustedEndpoint(17));
				changeFeedVersionUpdate = RequestStream<struct ChangeFeedVersionUpdateRequest>(
				    getValue.getEndpoint().getAdjustedEndpoint(18));
				checkpoint = RequestStream<struct GetCheckpointRequest>(getValue.getEndpoint().getAdjustedEndpoint(19));
				getFile = RequestStream<struct GetFileRequest>(getValue.getEndpoint().getAdjustedEndpoint(20));
			}
		} else {
			ASSERT(Ar::isDeserializing);
			if constexpr (is_fb_function<Ar>) {
				ASSERT(false);
			}
			serializer(ar,
			           uniqueID,
			           locality,
			           getValue,
			           getKey,
			           getKeyValues,
			           getShardState,
			           waitMetrics,
			           splitMetrics,
			           getStorageMetrics,
			           waitFailure,
			           getQueuingMetrics,
			           getKeyValueStoreType);
			if (ar.protocolVersion().hasWatches()) {
				serializer(ar, watchValue);
			}
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
		streams.push_back(getKeyValuesAndFlatMap.getReceiver(TaskPriority::LoadBalancedEndpoint));
		streams.push_back(changeFeedStream.getReceiver());
		streams.push_back(overlappingChangeFeeds.getReceiver());
		streams.push_back(changeFeedPop.getReceiver());
		streams.push_back(changeFeedVersionUpdate.getReceiver());
		streams.push_back(checkpoint.getReceiver());
		streams.push_back(getFile.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct StorageInfo : NonCopyable, public ReferenceCounted<StorageInfo> {
	Tag tag;
	StorageServerInterface interf;
	StorageInfo() : tag(invalidTag) {}
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
	SpanID spanContext;
	Key key;
	Version version;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetValueReply> reply;

	GetValueRequest() {}
	GetValueRequest(SpanID spanContext, const Key& key, Version ver, Optional<TagSet> tags, Optional<UID> debugID)
	  : spanContext(spanContext), key(key), version(ver), tags(tags), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, version, tags, debugID, reply, spanContext);
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
	SpanID spanContext;
	Key key;
	Optional<Value> value;
	Version version;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<WatchValueReply> reply;

	WatchValueRequest() {}
	WatchValueRequest(SpanID spanContext,
	                  const Key& key,
	                  Optional<Value> value,
	                  Version ver,
	                  Optional<TagSet> tags,
	                  Optional<UID> debugID)
	  : spanContext(spanContext), key(key), value(value), version(ver), tags(tags), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value, version, tags, debugID, reply, spanContext);
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
	SpanID spanContext;
	Arena arena;
	KeySelectorRef begin, end;
	// This is a dummy field there has never been used.
	// TODO: Get rid of this by constexpr or other template magic in getRange
	KeyRef mapper = KeyRef();
	Version version; // or latestVersion
	int limit, limitBytes;
	bool isFetchKeys;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetKeyValuesReply> reply;

	GetKeyValuesRequest() : isFetchKeys(false) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end, version, limit, limitBytes, isFetchKeys, tags, debugID, reply, spanContext, arena);
	}
};

struct GetKeyValuesAndFlatMapReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1783067;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	Version version; // useful when latestVersion was requested
	bool more;
	bool cached = false;

	GetKeyValuesAndFlatMapReply() : version(invalidVersion), more(false), cached(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, data, version, more, cached, arena);
	}
};

struct GetKeyValuesAndFlatMapRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 6795747;
	SpanID spanContext;
	Arena arena;
	KeySelectorRef begin, end;
	KeyRef mapper;
	Version version; // or latestVersion
	int limit, limitBytes;
	bool isFetchKeys;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetKeyValuesAndFlatMapReply> reply;

	GetKeyValuesAndFlatMapRequest() : isFetchKeys(false) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, begin, end, mapper, version, limit, limitBytes, isFetchKeys, tags, debugID, reply, spanContext, arena);
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
	SpanID spanContext;
	Arena arena;
	KeySelectorRef begin, end;
	Version version; // or latestVersion
	int limit, limitBytes;
	bool isFetchKeys;
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromiseStream<GetKeyValuesStreamReply> reply;

	GetKeyValuesStreamRequest() : isFetchKeys(false) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end, version, limit, limitBytes, isFetchKeys, tags, debugID, reply, spanContext, arena);
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
	SpanID spanContext;
	Arena arena;
	KeySelectorRef sel;
	Version version; // or latestVersion
	Optional<TagSet> tags;
	Optional<UID> debugID;
	ReplyPromise<GetKeyReply> reply;

	GetKeyRequest() {}
	GetKeyRequest(SpanID spanContext,
	              KeySelectorRef const& sel,
	              Version version,
	              Optional<TagSet> tags,
	              Optional<UID> debugID)
	  : spanContext(spanContext), sel(sel), version(version), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sel, version, tags, debugID, reply, spanContext, arena);
	}
};

struct GetShardStateReply {
	constexpr static FileIdentifier file_identifier = 0;

	Version first;
	Version second;
	GetShardStateReply() = default;
	GetShardStateReply(Version first, Version second) : first(first), second(second) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, first, second);
	}
};

struct GetShardStateRequest {
	constexpr static FileIdentifier file_identifier = 15860168;
	enum waitMode { NO_WAIT = 0, FETCHING = 1, READABLE = 2 };

	KeyRange keys;
	int32_t mode;
	ReplyPromise<GetShardStateReply> reply;
	GetShardStateRequest() {}
	GetShardStateRequest(KeyRange const& keys, waitMode mode) : keys(keys), mode(mode) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, mode, reply);
	}
};

struct StorageMetrics {
	constexpr static FileIdentifier file_identifier = 13622226;
	int64_t bytes = 0; // total storage
	// FIXME: currently, neither of bytesPerKSecond or iosPerKSecond are actually used in DataDistribution calculations.
	// This may change in the future, but this comment is left here to avoid any confusion for the time being.
	int64_t bytesPerKSecond = 0; // network bandwidth (average over 10s)
	int64_t iosPerKSecond = 0;
	int64_t bytesReadPerKSecond = 0;

	static const int64_t infinity = 1LL << 60;

	bool allLessOrEqual(const StorageMetrics& rhs) const {
		return bytes <= rhs.bytes && bytesPerKSecond <= rhs.bytesPerKSecond && iosPerKSecond <= rhs.iosPerKSecond &&
		       bytesReadPerKSecond <= rhs.bytesReadPerKSecond;
	}
	void operator+=(const StorageMetrics& rhs) {
		bytes += rhs.bytes;
		bytesPerKSecond += rhs.bytesPerKSecond;
		iosPerKSecond += rhs.iosPerKSecond;
		bytesReadPerKSecond += rhs.bytesReadPerKSecond;
	}
	void operator-=(const StorageMetrics& rhs) {
		bytes -= rhs.bytes;
		bytesPerKSecond -= rhs.bytesPerKSecond;
		iosPerKSecond -= rhs.iosPerKSecond;
		bytesReadPerKSecond -= rhs.bytesReadPerKSecond;
	}
	template <class F>
	void operator*=(F f) {
		bytes *= f;
		bytesPerKSecond *= f;
		iosPerKSecond *= f;
		bytesReadPerKSecond *= f;
	}
	bool allZero() const { return !bytes && !bytesPerKSecond && !iosPerKSecond && !bytesReadPerKSecond; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, bytes, bytesPerKSecond, iosPerKSecond, bytesReadPerKSecond);
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
		return bytes == rhs.bytes && bytesPerKSecond == rhs.bytesPerKSecond && iosPerKSecond == rhs.iosPerKSecond &&
		       bytesReadPerKSecond == rhs.bytesReadPerKSecond;
	}

	std::string toString() const {
		return format("Bytes: %lld, BPerKSec: %lld, iosPerKSec: %lld, BReadPerKSec: %lld",
		              bytes,
		              bytesPerKSecond,
		              iosPerKSecond,
		              bytesReadPerKSecond);
	}
};

struct WaitMetricsRequest {
	// Waits for any of the given minimum or maximum metrics to be exceeded, and then returns the current values
	// Send a reversed range for min, max to receive an immediate report
	constexpr static FileIdentifier file_identifier = 1795961;
	Arena arena;
	KeyRangeRef keys;
	StorageMetrics min, max;
	ReplyPromise<StorageMetrics> reply;

	WaitMetricsRequest() {}
	WaitMetricsRequest(KeyRangeRef const& keys, StorageMetrics const& min, StorageMetrics const& max)
	  : keys(arena, keys), min(min), max(max) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, min, max, reply, arena);
	}
};

struct SplitMetricsReply {
	constexpr static FileIdentifier file_identifier = 11530792;
	Standalone<VectorRef<KeyRef>> splits;
	StorageMetrics used;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, splits, used);
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

	SplitMetricsRequest() {}
	SplitMetricsRequest(KeyRangeRef const& keys,
	                    StorageMetrics const& limits,
	                    StorageMetrics const& used,
	                    StorageMetrics const& estimated,
	                    bool isLastShard)
	  : keys(arena, keys), limits(limits), used(used), estimated(estimated), isLastShard(isLastShard) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, limits, used, estimated, isLastShard, reply, arena);
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
	double readBandwidth;

	ReadHotRangeWithMetrics() = default;
	ReadHotRangeWithMetrics(KeyRangeRef const& keys, double density, double readBandwidth)
	  : keys(keys), density(density), readBandwidth(readBandwidth) {}

	ReadHotRangeWithMetrics(Arena& arena, const ReadHotRangeWithMetrics& rhs)
	  : keys(arena, rhs.keys), density(rhs.density), readBandwidth(rhs.readBandwidth) {}

	int expectedSize() const { return keys.expectedSize() + sizeof(density) + sizeof(readBandwidth); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, density, readBandwidth);
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
	Arena arena;
	KeyRangeRef keys;
	ReplyPromise<ReadHotSubRangeReply> reply;

	ReadHotSubRangeRequest() {}
	ReadHotSubRangeRequest(KeyRangeRef const& keys) : keys(arena, keys) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, reply, arena);
	}
};

struct SplitRangeReply {
	constexpr static FileIdentifier file_identifier = 11813134;
	// If the given range can be divided, contains the split points.
	// If the given range cannot be divided(for exmaple its total size is smaller than the chunk size), this would be
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
	KeyRangeRef keys;
	int64_t chunkSize;
	ReplyPromise<SplitRangeReply> reply;

	SplitRangeRequest() {}
	SplitRangeRequest(KeyRangeRef const& keys, int64_t chunkSize) : keys(arena, keys), chunkSize(chunkSize) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, chunkSize, reply, arena);
	}
};

struct ChangeFeedStreamReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<MutationsAndVersionRef> mutations;
	bool atLatestVersion = false;
	Version minStreamVersion = invalidVersion;

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
		           arena);
	}
};

struct ChangeFeedStreamRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	SpanID spanContext;
	Arena arena;
	Key rangeID;
	Version begin = 0;
	Version end = 0;
	KeyRange range;
	ReplyPromiseStream<ChangeFeedStreamReply> reply;

	ChangeFeedStreamRequest() {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeID, begin, end, range, reply, spanContext, arena);
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

// FDB storage checkpoint format.
enum CheckpointFormat {
	InvalidFormat = 0,
	// Checkpoint generated via rocksdb::Checkpoint::ExportColumnFamily().
	RocksDBColumnFamily = 1,
	// Checkpoint generated via rocksdb::Checkpoint::CreateCheckpoint().
	RocksDBCheckpoint = 2,
};

namespace {

std::string getFdbCheckpointFormatName(const CheckpointFormat& format) {
	std::string name;
	switch (format) {
	case InvalidFormat:
		name = "Invalid";
		break;
	case RocksDBColumnFamily:
		name = "RocksDBColumnFamily";
		break;
	case RocksDBCheckpoint:
		name = "RocksDBCheckpoint";
	}

	return name;
}

} // namespace

// Copied from rocksdb/metadata.h, so that we can add serializer.
struct SstFileMetaData {
	constexpr static FileIdentifier file_identifier = 3804347;
	SstFileMetaData()
	  : size(0), file_number(0), smallest_seqno(0), largest_seqno(0), num_reads_sampled(0), being_compacted(false),
	    num_entries(0), num_deletions(0), temperature(0), oldest_blob_file_number(0), oldest_ancester_time(0),
	    file_creation_time(0) {}

	SstFileMetaData(const std::string& _file_name,
	                uint64_t _file_number,
	                const std::string& _path,
	                size_t _size,
	                uint64_t _smallest_seqno,
	                uint64_t _largest_seqno,
	                const std::string& _smallestkey,
	                const std::string& _largestkey,
	                uint64_t _num_reads_sampled,
	                bool _being_compacted,
	                int _temperature,
	                uint64_t _oldest_blob_file_number,
	                uint64_t _oldest_ancester_time,
	                uint64_t _file_creation_time,
	                std::string& _file_checksum,
	                std::string& _file_checksum_func_name)
	  : size(_size), name(_file_name), file_number(_file_number), db_path(_path), smallest_seqno(_smallest_seqno),
	    largest_seqno(_largest_seqno), smallestkey(_smallestkey), largestkey(_largestkey),
	    num_reads_sampled(_num_reads_sampled), being_compacted(_being_compacted), num_entries(0), num_deletions(0),
	    temperature(_temperature), oldest_blob_file_number(_oldest_blob_file_number),
	    oldest_ancester_time(_oldest_ancester_time), file_creation_time(_file_creation_time),
	    file_checksum(_file_checksum), file_checksum_func_name(_file_checksum_func_name) {}

	// File size in bytes.
	size_t size;
	// The name of the file.
	std::string name;
	// The id of the file.
	uint64_t file_number;
	// The full path where the file locates.
	std::string db_path;

	uint64_t smallest_seqno; // Smallest sequence number in file.
	uint64_t largest_seqno; // Largest sequence number in file.
	std::string smallestkey; // Smallest user defined key in the file.
	std::string largestkey; // Largest user defined key in the file.
	uint64_t num_reads_sampled; // How many times the file is read.
	bool being_compacted; // true if the file is currently being compacted.

	uint64_t num_entries;
	uint64_t num_deletions;

	// This feature is experimental and subject to change.
	int temperature;

	uint64_t oldest_blob_file_number; // The id of the oldest blob file
	                                  // referenced by the file.
	// An SST file may be generated by compactions whose input files may
	// in turn be generated by earlier compactions. The creation time of the
	// oldest SST file that is the compaction ancestor of this file.
	// The timestamp is provided SystemClock::GetCurrentTime().
	// 0 if the information is not available.
	//
	// Note: for TTL blob files, it contains the start of the expiration range.
	uint64_t oldest_ancester_time;
	// Timestamp when the SST file is created, provided by
	// SystemClock::GetCurrentTime(). 0 if the information is not available.
	uint64_t file_creation_time;

	// The checksum of a SST file, the value is decided by the file content and
	// the checksum algorithm used for this SST file. The checksum function is
	// identified by the file_checksum_func_name. If the checksum function is
	// not specified, file_checksum is "0" by default.
	std::string file_checksum;

	// The name of the checksum function used to generate the file checksum
	// value. If file checksum is not enabled (e.g., sst_file_checksum_func is
	// null), file_checksum_func_name is UnknownFileChecksumFuncName, which is
	// "Unknown".
	std::string file_checksum_func_name;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           size,
		           name,
		           file_number,
		           db_path,
		           smallest_seqno,
		           largest_seqno,
		           smallestkey,
		           largestkey,
		           num_reads_sampled,
		           being_compacted,
		           num_entries,
		           num_deletions,
		           temperature,
		           oldest_blob_file_number,
		           oldest_ancester_time,
		           file_creation_time,
		           file_checksum,
		           file_checksum_func_name);
	}
};

// Copied from rocksdb::LiveFileMetaData.
struct LiveFileMetaData : public SstFileMetaData {
	constexpr static FileIdentifier file_identifier = 3804346;
	std::string column_family_name; // Name of the column family
	int level; // Level at which this file resides.
	bool fetched;
	LiveFileMetaData() : column_family_name(), level(0), fetched(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           SstFileMetaData::size,
		           SstFileMetaData::name,
		           SstFileMetaData::file_number,
		           SstFileMetaData::db_path,
		           SstFileMetaData::smallest_seqno,
		           SstFileMetaData::largest_seqno,
		           SstFileMetaData::smallestkey,
		           SstFileMetaData::largestkey,
		           SstFileMetaData::num_reads_sampled,
		           SstFileMetaData::being_compacted,
		           SstFileMetaData::num_entries,
		           SstFileMetaData::num_deletions,
		           SstFileMetaData::temperature,
		           SstFileMetaData::oldest_blob_file_number,
		           SstFileMetaData::oldest_ancester_time,
		           SstFileMetaData::file_creation_time,
		           SstFileMetaData::file_checksum,
		           SstFileMetaData::file_checksum_func_name,
		           column_family_name,
		           level,
		           fetched);
	}
};

// Checkpoint metadata associated with RockDBColumnFamily format.
// Based on rocksdb::ExportImportFilesMetaData.
struct RocksDBColumnFamilyCheckpoint {
	constexpr static FileIdentifier file_identifier = 13804346;
	std::string dbComparatorName;

	std::vector<LiveFileMetaData> sstFiles;

	CheckpointFormat format() const { return RocksDBColumnFamily; }

	std::string toString() const {
		std::string res = "RocksDBColumnFamilyCheckpoint:\nSST Files:\n";
		for (const auto& file : sstFiles) {
			res += file.db_path + file.name + "\n";
		}
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, dbComparatorName, sstFiles);
	}
};

struct RocksDBCheckpoint {
	constexpr static FileIdentifier file_identifier = 13804346;
	std::string checkpointDir;

	CheckpointFormat format() const { return RocksDBCheckpoint; }

	std::string toString() const {
		return "RocksDBCheckpoint:\nCheckpoint dir:\n" + checkpointDir;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, checkpointDir);
	}
}

// Metadata of a FDB checkpoint.
struct CheckpointMetaData {
	enum CheckpointState {
		InvalidState = 0,
		Pending = 1,
		Complete = 2,
		Deleting = 3,
		Fail = 4,
	};

	constexpr static FileIdentifier file_identifier = 13804342;
	Version version;
	KeyRange range;
	int16_t format;
	int16_t state;
	UID checkpointID; // A unique id for this checkpoint.
	UID ssID; // Storage server ID on which this checkpoint is created.
	int referenceCount; // A reference count on the checkpoint, it can only be deleted when this is 0.

	int64_t gcTime; // Time to delete this checkpoint, a Unix timestamp in seconds.

	Optional<RocksDBColumnFamilyCheckpoint> rocksCF; // Present when format == RocksDBColumnFamily.

	Optional<RocksDBCheckpoint> rocksCheckpoint; // Present when format == RocksDBCheckpoint.

	CheckpointMetaData() : format(InvalidFormat), state(InvalidState), referenceCount(0) {}
	CheckpointMetaData(KeyRange const& range, CheckpointFormat format, UID const& ssID, UID const& checkpointID)
	  : version(invalidVersion), range(range), format(format), ssID(ssID), checkpointID(checkpointID), state(Pending),
	    referenceCount(0) {}
	CheckpointMetaData(Version version, KeyRange const& range, CheckpointFormat format, UID checkpointID)
	  : version(version), range(range), format(format), checkpointID(checkpointID), referenceCount(0) {}

	CheckpointState getState() const { return static_cast<CheckpointState>(state); }

	void setState(CheckpointState state) { this->state = static_cast<int16_t>(state); }

	std::string toString() const {
		std::string res = "Checkpoint MetaData:\nServer: " + ssID.toString() + "\nID: " + checkpointID.toString() +
		                  "\nVersion: " + std::to_string(version) +
		                  "\nFormat: " + getFdbCheckpointFormatName(static_cast<CheckpointFormat>(format)) +
		                  "\nState: " + std::to_string(static_cast<int>(state)) + "\n";
		if (rocksCF.present()) {
			res += rocksCF.get().toString();
		}
		return res;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, range, format, state, checkpointID, ssID, gcTime, rocksCF, rocksCheckpoint);
	}
};

// Request to search for a checkpoint for a minimum keyrange: `range`, at the specific version,
// in the specific format.
// A CheckpointMetaData will be returned if the specific checkpoint is found.
struct GetCheckpointRequest {
	constexpr static FileIdentifier file_identifier = 13804343;
	Version version;
	KeyRange range;
	int16_t format;
	Optional<UID> checkpointID; // When present, look for the checkpoint with the exact UID.
	ReplyPromise<CheckpointMetaData> reply;

	GetCheckpointRequest() {}
	GetCheckpointRequest(Version version, KeyRange const& range, CheckpointFormat format)
	  : version(version), range(range), format(format) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, range, format, checkpointID, reply);
	}
};

// Reply to GetFileRequest, used to transfer files.
struct GetFileReply : public ReplyPromiseStreamReply {
	constexpr static FileIdentifier file_identifier = 13804345;
	std::string name; // File name.
	int64_t offset;
	int64_t size; // Size of `data`.
	Standalone<StringRef> data;

	GetFileReply() {}
	GetFileReply(std::string name, int64_t offset, int64_t size) : name(name), offset(offset), size(size) {}
	GetFileReply(std::string name) : GetFileReply(name, 0, 0) {}

	int expectedSize() const { return data.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(
		    ar, ReplyPromiseStreamReply::acknowledgeToken, ReplyPromiseStreamReply::sequence, name, offset, size, data);
	}
};

// Request to get a file from a storage server.
struct GetFileRequest {
	constexpr static FileIdentifier file_identifier = 13804344;
	std::string path; // File path on the storage server.
	int64_t offset;
	ReplyPromiseStream<GetFileReply> reply;

	GetFileRequest() {}
	GetFileRequest(std::string path, int64_t offset) : path(path), offset(offset) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, path, offset, reply);
	}
};

struct OverlappingChangeFeedEntry {
	Key rangeId;
	KeyRange range;
	bool stopped = false;

	bool operator==(const OverlappingChangeFeedEntry& r) const {
		return rangeId == r.rangeId && range == r.range && stopped == r.stopped;
	}

	OverlappingChangeFeedEntry() {}
	OverlappingChangeFeedEntry(Key const& rangeId, KeyRange const& range, bool stopped)
	  : rangeId(rangeId), range(range), stopped(stopped) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeId, range, stopped);
	}
};

struct OverlappingChangeFeedsReply {
	constexpr static FileIdentifier file_identifier = 11815134;
	std::vector<OverlappingChangeFeedEntry> rangeIds;
	bool cached;
	Arena arena;

	OverlappingChangeFeedsReply() : cached(false) {}
	explicit OverlappingChangeFeedsReply(std::vector<OverlappingChangeFeedEntry> const& rangeIds)
	  : rangeIds(rangeIds), cached(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, rangeIds, arena);
	}
};

struct OverlappingChangeFeedsRequest {
	constexpr static FileIdentifier file_identifier = 10726174;
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
	constexpr static FileIdentifier file_identifier = 11815134;
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
	StorageMetrics load;
	StorageMetrics available;
	StorageMetrics capacity;
	double bytesInputRate;
	int64_t versionLag;
	double lastUpdate;

	GetStorageMetricsReply() : bytesInputRate(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, load, available, capacity, bytesInputRate, versionLag, lastUpdate);
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

struct StorageQueuingMetricsReply {
	constexpr static FileIdentifier file_identifier = 7633366;
	double localTime;
	int64_t instanceID; // changes if bytesDurable and bytesInput reset
	int64_t bytesDurable, bytesInput;
	StorageBytes storageBytes;
	Version version; // current storage server version
	Version durableVersion; // latest version durable on storage server
	double cpuUsage;
	double diskUsage;
	double localRateLimit;
	Optional<TransactionTag> busiestTag;
	double busiestTagFractionalBusyness;
	double busiestTagRate;

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
		           busiestTag,
		           busiestTagFractionalBusyness,
		           busiestTagRate);
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

#endif
