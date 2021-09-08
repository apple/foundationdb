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

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "fdbrpc/Stats.h"
#include "fdbrpc/TimedRequest.h"

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

	RequestStream<ReplyPromise<VersionReply>> getVersion;
	RequestStream<struct GetValueRequest> getValue;
	RequestStream<struct GetKeyRequest> getKey;

	// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large
	// selector offset prevents all data from being read in one range read
	RequestStream<struct GetKeyValuesRequest> getKeyValues;

	RequestStream<struct GetShardStateRequest> getShardState;
	RequestStream<struct WaitMetricsRequest> waitMetrics;
	RequestStream<struct SplitMetricsRequest> splitMetrics;
	RequestStream<struct GetStorageMetricsRequest> getStorageMetrics;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct StorageQueuingMetricsRequest> getQueuingMetrics;

	RequestStream<ReplyPromise<KeyValueStoreType>> getKeyValueStoreType;
	RequestStream<struct WatchValueRequest> watchValue;

	explicit StorageServerInterface(UID uid) : uniqueID(uid) {}
	StorageServerInterface() : uniqueID(deterministicRandom()->randomUniqueID()) {}
	NetworkAddress address() const { return getVersion.getEndpoint().getPrimaryAddress(); }
	UID id() const { return uniqueID; }
	std::string toString() const { return id().shortString(); }
	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.protocolVersion() == supportDowngradeProtocolVersion && ar.isDeserializing) {
			serializer(ar, uniqueID, locality, getValue);
			getKey = RequestStream<struct GetKeyRequest>(getValue.getEndpoint());
			getKeyValues = RequestStream<struct GetKeyValuesRequest>(getValue.getEndpoint());
			getShardState = RequestStream<struct GetShardStateRequest>(getValue.getEndpoint());
			waitMetrics = RequestStream<struct WaitMetricsRequest>(getValue.getEndpoint());
			splitMetrics = RequestStream<struct SplitMetricsRequest>(getValue.getEndpoint());
			getStorageMetrics = RequestStream<struct GetStorageMetricsRequest>(getValue.getEndpoint());
			waitFailure = RequestStream<ReplyPromise<Void>>(getValue.getEndpoint());
			getQueuingMetrics = RequestStream<struct StorageQueuingMetricsRequest>(getValue.getEndpoint());
			getKeyValueStoreType = RequestStream<ReplyPromise<KeyValueStoreType>>(getValue.getEndpoint());
			watchValue = RequestStream<struct WatchValueRequest>(getValue.getEndpoint());
		} else {
			// StorageServerInterface is persisted in the database and in the tLog's data structures, so changes here
			// have to be versioned carefully!
			if constexpr (!is_fb_function<Ar>) {
				serializer(ar,
				           uniqueID,
				           locality,
				           getVersion,
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
				if (ar.protocolVersion().hasWatches())
					serializer(ar, watchValue);
			} else {
				serializer(ar,
				           uniqueID,
				           locality,
				           getVersion,
				           getValue,
				           getKey,
				           getKeyValues,
				           getShardState,
				           waitMetrics,
				           splitMetrics,
				           getStorageMetrics,
				           waitFailure,
				           getQueuingMetrics,
				           getKeyValueStoreType,
				           watchValue);
			}
		}
	}
	bool operator==(StorageServerInterface const& s) const { return uniqueID == s.uniqueID; }
	bool operator<(StorageServerInterface const& s) const { return uniqueID < s.uniqueID; }
	void initEndpoints() {
		getValue.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		getKey.getEndpoint(TaskPriority::LoadBalancedEndpoint);
		getKeyValues.getEndpoint(TaskPriority::LoadBalancedEndpoint);
	}
};

struct StorageInfo : NonCopyable, public ReferenceCounted<StorageInfo> {
	Tag tag;
	StorageServerInterface interf;
	StorageInfo() : tag(invalidTag) {}
};

struct ServerCacheInfo {
	std::vector<Tag> tags;
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

	GetValueReply() {}
	GetValueReply(Optional<Value> value) : value(value) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, value);
	}
};

struct GetValueRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 8454530;
	Key key;
	Version version;
	Optional<UID> debugID;
	ReplyPromise<GetValueReply> reply;

	GetValueRequest() {}
	GetValueRequest(const Key& key, Version ver, Optional<UID> debugID) : key(key), version(ver), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, version, debugID, reply);
	}
};

struct WatchValueReply {
	constexpr static FileIdentifier file_identifier = 3;

	Version version;
	WatchValueReply() = default;
	explicit WatchValueReply(Version version) : version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct WatchValueRequest {
	constexpr static FileIdentifier file_identifier = 14747733;
	Key key;
	Optional<Value> value;
	Version version;
	Optional<UID> debugID;
	ReplyPromise<WatchValueReply> reply;

	WatchValueRequest() {}
	WatchValueRequest(const Key& key, Optional<Value> value, Version ver, Optional<UID> debugID)
	  : key(key), value(value), version(ver), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, value, version, debugID, reply);
	}
};

struct GetKeyValuesReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 1783066;
	Arena arena;
	VectorRef<KeyValueRef, VecSerStrategy::String> data;
	Version version; // useful when latestVersion was requested
	bool more;

	GetKeyValuesReply() : version(invalidVersion), more(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, data, version, more, arena);
	}
};

struct GetKeyValuesRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 6795746;
	Arena arena;
	KeySelectorRef begin, end;
	Version version; // or latestVersion
	int limit, limitBytes;
	bool isFetchKeys;
	Optional<UID> debugID;
	ReplyPromise<GetKeyValuesReply> reply;

	GetKeyValuesRequest() : isFetchKeys(false) {}
	//	GetKeyValuesRequest(const KeySelectorRef& begin, const KeySelectorRef& end, Version version, int limit, int
	// limitBytes, Optional<UID> debugID) : begin(begin), end(end), version(version), limit(limit),
	// limitBytes(limitBytes) {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, begin, end, version, limit, limitBytes, isFetchKeys, debugID, reply, arena);
	}
};

struct GetKeyReply : public LoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 11226513;
	KeySelector sel;

	GetKeyReply() {}
	GetKeyReply(KeySelector sel) : sel(sel) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, LoadBalancedReply::penalty, LoadBalancedReply::error, sel);
	}
};

struct GetKeyRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 10457870;
	Arena arena;
	KeySelectorRef sel;
	Version version; // or latestVersion
	ReplyPromise<GetKeyReply> reply;

	GetKeyRequest() {}
	GetKeyRequest(KeySelectorRef const& sel, Version version) : sel(sel), version(version) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, sel, version, reply, arena);
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
	int64_t bytesPerKSecond = 0; // network bandwidth (average over 10s)
	int64_t iosPerKSecond = 0;

	static const int64_t infinity = 1LL << 60;

	bool allLessOrEqual(const StorageMetrics& rhs) const {
		return bytes <= rhs.bytes && bytesPerKSecond <= rhs.bytesPerKSecond && iosPerKSecond <= rhs.iosPerKSecond;
	}
	void operator+=(const StorageMetrics& rhs) {
		bytes += rhs.bytes;
		bytesPerKSecond += rhs.bytesPerKSecond;
		iosPerKSecond += rhs.iosPerKSecond;
	}
	void operator-=(const StorageMetrics& rhs) {
		bytes -= rhs.bytes;
		bytesPerKSecond -= rhs.bytesPerKSecond;
		iosPerKSecond -= rhs.iosPerKSecond;
	}
	template <class F>
	void operator*=(F f) {
		bytes *= f;
		bytesPerKSecond *= f;
		iosPerKSecond *= f;
	}
	bool allZero() const { return !bytes && !bytesPerKSecond && !iosPerKSecond; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, bytes, bytesPerKSecond, iosPerKSecond);
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
		return bytes == rhs.bytes && bytesPerKSecond == rhs.bytesPerKSecond && iosPerKSecond == rhs.iosPerKSecond;
	}

	std::string toString() const {
		return format("Bytes: %lld, BPerKSec: %lld, iosPerKSec: %lld", bytes, bytesPerKSecond, iosPerKSecond);
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

struct GetStorageMetricsReply {
	constexpr static FileIdentifier file_identifier = 15491478;
	StorageMetrics load;
	StorageMetrics available;
	StorageMetrics capacity;
	double bytesInputRate;

	GetStorageMetricsReply() : bytesInputRate(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, load, available, capacity, bytesInputRate);
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
		           localRateLimit);
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
