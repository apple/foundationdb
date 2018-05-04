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

#include "FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/QueueModel.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.actor.h"

struct StorageServerInterface {
	enum { 
		BUSY_ALLOWED = 0,
		BUSY_FORCE = 1,
		BUSY_LOCAL = 2
	};

	enum { LocationAwareLoadBalance = 1 };

	LocalityData locality;
	UID uniqueID;

	RequestStream<ReplyPromise<Version>> getVersion;
	RequestStream<struct GetValueRequest> getValue;
	RequestStream<struct GetKeyRequest> getKey;

	// Throws a wrong_shard_server if the keys in the request or result depend on data outside this server OR if a large selector offset prevents
	// all data from being read in one range read
	RequestStream<struct GetKeyValuesRequest> getKeyValues;

	RequestStream<struct GetShardStateRequest> getShardState;
	RequestStream<struct WaitMetricsRequest> waitMetrics;
	RequestStream<struct SplitMetricsRequest> splitMetrics;
	RequestStream<struct GetPhysicalMetricsRequest> getPhysicalMetrics;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct StorageQueuingMetricsRequest> getQueuingMetrics;

	RequestStream<ReplyPromise<KeyValueStoreType>> getKeyValueStoreType;
	RequestStream<struct WatchValueRequest> watchValue;

	explicit StorageServerInterface(UID uid) : uniqueID( uid ) {}
	StorageServerInterface() : uniqueID( g_random->randomUniqueID() ) {}
	NetworkAddress address() const { return getVersion.getEndpoint().address; }
	UID id() const { return uniqueID; }
	std::string toString() const { return id().shortString(); }
	template <class Ar> 
	void serialize( Ar& ar ) {
		// StorageServerInterface is persisted in the database and in the tLog's data structures, so changes here have to be
		// versioned carefully!
		ar & uniqueID & locality & getVersion & getValue & getKey & getKeyValues & getShardState & waitMetrics 
			& splitMetrics & getPhysicalMetrics & waitFailure & getQueuingMetrics & getKeyValueStoreType;

		if( ar.protocolVersion() >= 0x0FDB00A200090001LL )
			ar & watchValue;
	}
	bool operator == (StorageServerInterface const& s) const { return uniqueID == s.uniqueID; }
	bool operator < (StorageServerInterface const& s) const { return uniqueID < s.uniqueID; }
	void initEndpoints() {
		getValue.getEndpoint( TaskLoadBalancedEndpoint );
		getKey.getEndpoint( TaskLoadBalancedEndpoint );
		getKeyValues.getEndpoint( TaskLoadBalancedEndpoint );
	}
};

struct StorageInfo : NonCopyable, public ReferenceCounted<StorageInfo> {
	Tag tag;
	StorageServerInterface interf;
	StorageInfo() : tag(invalidTag) {}
};

struct ServerCacheInfo {
	std::vector<Tag> tags;
	std::vector<Reference<StorageInfo>> info;
};

struct GetValueReply : public LoadBalancedReply {
	Optional<Value> value;

	GetValueReply() {}
	GetValueReply(Optional<Value> value) : value(value) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & *(LoadBalancedReply*)this & value;
	}
};

struct GetValueRequest {
	Key key;
	Version version;
	Optional<UID> debugID;
	ReplyPromise<GetValueReply> reply;

	GetValueRequest(){}
	GetValueRequest(const Key& key, Version ver, Optional<UID> debugID) : key(key), version(ver), debugID(debugID) {}
	
	template <class Ar> 
	void serialize( Ar& ar ) {
		ar & key & version & debugID & reply;
	}
};

struct WatchValueRequest {
	Key key;
	Optional<Value> value;
	Version version;
	Optional<UID> debugID;
	ReplyPromise< Version > reply;
	
	WatchValueRequest(){}
	WatchValueRequest(const Key& key, Optional<Value> value, Version ver, Optional<UID> debugID) : key(key), value(value), version(ver), debugID(debugID) {}
	
	template <class Ar> 
	void serialize( Ar& ar ) {
		ar & key & value & version & debugID & reply;
	}
};

struct GetKeyValuesReply : public LoadBalancedReply {
	Arena arena;
	VectorRef<KeyValueRef> data;
	Version version; // useful when latestVersion was requested
	bool more;

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & *(LoadBalancedReply*)this & data & version & more & arena;
	}
};

struct GetKeyValuesRequest {
	Arena arena;
	KeySelectorRef begin, end;
	Version version;		// or latestVersion
	int limit, limitBytes;
	Optional<UID> debugID;
	ReplyPromise<GetKeyValuesReply> reply;

	GetKeyValuesRequest() {}
//	GetKeyValuesRequest(const KeySelectorRef& begin, const KeySelectorRef& end, Version version, int limit, int limitBytes, Optional<UID> debugID) : begin(begin), end(end), version(version), limit(limit), limitBytes(limitBytes) {}
	template <class Ar>
	void serialize( Ar& ar ) {
		ar & begin & end & version & limit & limitBytes & debugID & reply & arena;
	}
};

struct GetKeyReply : public LoadBalancedReply {
	KeySelector sel;

	GetKeyReply() {}
	GetKeyReply(KeySelector sel) : sel(sel) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & *(LoadBalancedReply*)this & sel;
	}
};

struct GetKeyRequest {
	Arena arena;
	KeySelectorRef sel;
	Version version;		// or latestVersion
	ReplyPromise<GetKeyReply> reply;

	GetKeyRequest() {}
	GetKeyRequest(KeySelectorRef const& sel, Version version) : sel(sel), version(version) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & sel & version & reply & arena;
	}
};

struct GetShardStateRequest {
	enum waitMode {
		NO_WAIT = 0,
		FETCHING = 1,
		READABLE = 2
	};
	
	KeyRange keys;
	int32_t mode;
	ReplyPromise< Version > reply;
	GetShardStateRequest() {}
	GetShardStateRequest( KeyRange const& keys, waitMode mode ) : keys(keys), mode(mode) {}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & keys & mode & reply;
	}
};

struct StorageMetrics {
	int64_t bytes;				// total storage
	int64_t bytesPerKSecond;	// network bandwidth (average over 10s)
	int64_t iosPerKSecond;

	static const int64_t infinity = 1LL<<60;

	StorageMetrics() : bytes(0), bytesPerKSecond(0), iosPerKSecond(0) {}

	bool allLessOrEqual( const StorageMetrics& rhs ) const {
		return bytes <= rhs.bytes && bytesPerKSecond <= rhs.bytesPerKSecond && iosPerKSecond <= rhs.iosPerKSecond;
	}
	void operator += ( const StorageMetrics& rhs ) {
		bytes += rhs.bytes;
		bytesPerKSecond += rhs.bytesPerKSecond;
		iosPerKSecond += rhs.iosPerKSecond;
	}
	void operator -= ( const StorageMetrics& rhs ) {
		bytes -= rhs.bytes;
		bytesPerKSecond -= rhs.bytesPerKSecond;
		iosPerKSecond -= rhs.iosPerKSecond;
	}
	template <class F>
	void operator *= ( F f ) {
		bytes *= f;
		bytesPerKSecond *= f;
		iosPerKSecond *= f;
	}
	bool allZero() const { return !bytes && !bytesPerKSecond && !iosPerKSecond; }

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & bytes & bytesPerKSecond & iosPerKSecond;
	}

	void negate() { operator*=(-1.0); }
	StorageMetrics operator - () const { StorageMetrics x(*this); x.negate(); return x; }
	StorageMetrics operator + ( const StorageMetrics& r ) const { StorageMetrics x(*this); x+=r; return x; }
	StorageMetrics operator - ( const StorageMetrics& r ) const { StorageMetrics x(r); x.negate(); x+=*this; return x; }
	template <class F> StorageMetrics operator * ( F f ) const { StorageMetrics x(*this); x*=f; return x; }

	bool operator == ( StorageMetrics const& rhs ) const {
		return bytes == rhs.bytes && bytesPerKSecond == rhs.bytesPerKSecond && iosPerKSecond == rhs.iosPerKSecond;
	}

	std::string toString() const {
		return format("Bytes: %lld, BPerKSec: %lld, iosPerKSec: %lld", bytes, bytesPerKSecond, iosPerKSecond);
	}
};

struct WaitMetricsRequest {
	// Waits for any of the given minimum or maximum metrics to be exceeded, and then returns the current values
	// Send a reversed range for min, max to receive an immediate report
	Arena arena;
	KeyRangeRef keys;
	StorageMetrics min, max;
	ReplyPromise<StorageMetrics> reply;

	WaitMetricsRequest() {}
	WaitMetricsRequest( KeyRangeRef const& keys, StorageMetrics const& min, StorageMetrics const& max )
		: keys( arena, keys ), min( min ), max( max )
	{
	}

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & keys & min & max & reply & arena;
	}
};

struct SplitMetricsReply {
	Standalone<VectorRef<KeyRef>> splits;
	StorageMetrics used;

	template <class Ar>
	void serialize( Ar& ar ) {
		ar & splits & used;
	}
};

struct SplitMetricsRequest {
	Arena arena;
	KeyRangeRef keys;
	StorageMetrics limits;
	StorageMetrics used;
	StorageMetrics estimated;
	bool isLastShard;
	ReplyPromise<SplitMetricsReply> reply;

	SplitMetricsRequest() {}
	SplitMetricsRequest( KeyRangeRef const& keys, StorageMetrics const& limits, StorageMetrics const& used, StorageMetrics const& estimated, bool isLastShard ) : keys( arena, keys ), limits( limits ), used( used ), estimated( estimated ), isLastShard( isLastShard ) {}

	template <class Ar>
	void serialize(Ar& ar) {
		ar & keys & limits & used & estimated & isLastShard & reply & arena;
	}
};

struct GetPhysicalMetricsReply {
	StorageMetrics load;
	StorageMetrics free;
	StorageMetrics capacity;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & load & free & capacity;
	}
};

struct GetPhysicalMetricsRequest {
	ReplyPromise<GetPhysicalMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & reply;
	}
};

struct StorageQueuingMetricsRequest {
	// SOMEDAY: Send threshold value to avoid polling faster than the information changes?
	ReplyPromise<struct StorageQueuingMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		ar & reply;
	}
};

struct StorageQueuingMetricsReply {
	double localTime;
	int64_t instanceID;  // changes if bytesDurable and bytesInput reset
	int64_t bytesDurable, bytesInput;
	StorageBytes storageBytes;
	Version v; // current storage server version

	template <class Ar>
	void serialize(Ar& ar) {
		ar & localTime & instanceID & bytesDurable & bytesInput & v & storageBytes;
	}
};

#endif
