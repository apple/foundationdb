/*
 * GrvProxyInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_GRVPROXYINTERFACE_H
#define FDBCLIENT_GRVPROXYINTERFACE_H
#pragma once
#include "fdbclient/TagThrottle.h"
#include "fdbclient/VersionVector.h"
#include "flow/FileIdentifier.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/LoadBalance.actor.h"
#include "fdbrpc/Stats.h"
#include "fdbrpc/TimedRequest.h"
#include "fdbclient/FDBTypes.h"

struct GetReadVersionReply : public BasicLoadBalancedReply {
	constexpr static FileIdentifier file_identifier = 15709388;
	Version version;
	bool locked;
	Optional<Value> metadataVersion;
	int64_t midShardSize = 0;
	bool rkDefaultThrottled = false;
	bool rkBatchThrottled = false;

	TransactionTagMap<ClientTagThrottleLimits> tagThrottleInfo;
	double proxyTagThrottledDuration{ 0.0 };

	VersionVector ssVersionVectorDelta;
	UID proxyId; // GRV proxy ID to detect old GRV proxies at client side

	GetReadVersionReply() : version(invalidVersion), locked(false) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           BasicLoadBalancedReply::processBusyTime,
		           version,
		           locked,
		           metadataVersion,
		           tagThrottleInfo,
		           midShardSize,
		           rkDefaultThrottled,
		           rkBatchThrottled,
		           ssVersionVectorDelta,
		           proxyId,
		           proxyTagThrottledDuration);
	}
};

struct GetReadVersionRequest : TimedRequest {
	constexpr static FileIdentifier file_identifier = 838566;
	enum {
		PRIORITY_SYSTEM_IMMEDIATE =
		    15 << 24, // Highest possible priority, always executed even if writes are otherwise blocked
		PRIORITY_DEFAULT = 8 << 24,
		PRIORITY_BATCH = 1 << 24
	};
	enum {
		FLAG_USE_MIN_KNOWN_COMMITTED_VERSION = 4,
		FLAG_USE_PROVISIONAL_PROXIES = 2,
		FLAG_CAUSAL_READ_RISKY = 1,
		FLAG_PRIORITY_MASK = PRIORITY_SYSTEM_IMMEDIATE,
	};

	SpanContext spanContext;
	uint32_t transactionCount;
	uint32_t flags;
	TransactionPriority priority;

	TransactionTagMap<uint32_t> tags;
	// Not serialized, because this field does not need to be sent to master.
	// It is used for reporting to clients the amount of time spent delayed by
	// the TagQueue
	double proxyTagThrottledDuration{ 0.0 };

	Optional<UID> debugID;
	ReplyPromise<GetReadVersionReply> reply;

	Version maxVersion; // max version in the client's version vector cache

	GetReadVersionRequest() : transactionCount(1), flags(0), maxVersion(invalidVersion) {}
	GetReadVersionRequest(SpanContext spanContext,
	                      uint32_t transactionCount,
	                      TransactionPriority priority,
	                      Version maxVersion,
	                      uint32_t flags = 0,
	                      TransactionTagMap<uint32_t> tags = TransactionTagMap<uint32_t>(),
	                      Optional<UID> debugID = Optional<UID>())
	  : spanContext(spanContext), transactionCount(transactionCount), flags(flags), priority(priority), tags(tags),
	    debugID(debugID), maxVersion(maxVersion) {
		this->flags &= ~FLAG_PRIORITY_MASK;
		switch (priority) {
		case TransactionPriority::BATCH:
			this->flags |= PRIORITY_BATCH;
			break;
		case TransactionPriority::DEFAULT:
			this->flags |= PRIORITY_DEFAULT;
			break;
		case TransactionPriority::IMMEDIATE:
			this->flags |= PRIORITY_SYSTEM_IMMEDIATE;
			break;
		default:
			ASSERT(false);
		}
	}

	bool verify() const { return true; }

	bool operator<(GetReadVersionRequest const& rhs) const { return priority < rhs.priority; }

	bool isTagged() const { return !tags.empty(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, transactionCount, flags, tags, debugID, reply, spanContext, maxVersion);

		if (ar.isDeserializing) {
			if ((flags & PRIORITY_SYSTEM_IMMEDIATE) == PRIORITY_SYSTEM_IMMEDIATE) {
				priority = TransactionPriority::IMMEDIATE;
			} else if ((flags & PRIORITY_DEFAULT) == PRIORITY_DEFAULT) {
				priority = TransactionPriority::DEFAULT;
			} else if ((flags & PRIORITY_BATCH) == PRIORITY_BATCH) {
				priority = TransactionPriority::BATCH;
			} else {
				priority = TransactionPriority::DEFAULT;
			}
		}
	}
};

// GrvProxy is proxy primarily specializing on serving GetReadVersion. It also
// serves health metrics since it communicates with RateKeeper to gather health
// information of the cluster, and handles proxied GlobalConfig requests.
struct GrvProxyInterface {
	constexpr static FileIdentifier file_identifier = 8743216;
	enum { LocationAwareLoadBalance = 1 };
	enum { AlwaysFresh = 1 };

	Optional<Key> processId;
	bool provisional;

	PublicRequestStream<struct GetReadVersionRequest>
	    getConsistentReadVersion; // Returns a version which (1) is committed, and (2) is >= the latest version reported
	                              // committed (by a commit response) when this request was sent
	//   (at some point between when this request is sent and when its response is received, the latest version reported
	//   committed)
	RequestStream<ReplyPromise<Void>> waitFailure; // reports heartbeat to master.
	RequestStream<struct GetHealthMetricsRequest> getHealthMetrics;
	PublicRequestStream<struct GlobalConfigRefreshRequest> refreshGlobalConfig;

	UID id() const { return getConsistentReadVersion.getEndpoint().token; }
	std::string toString() const { return id().shortString(); }
	bool operator==(GrvProxyInterface const& r) const { return id() == r.id(); }
	bool operator!=(GrvProxyInterface const& r) const { return id() != r.id(); }
	NetworkAddress address() const { return getConsistentReadVersion.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return getConsistentReadVersion.getEndpoint().addresses; }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar, processId, provisional, getConsistentReadVersion);
		if (Archive::isDeserializing) {
			waitFailure =
			    RequestStream<ReplyPromise<Void>>(getConsistentReadVersion.getEndpoint().getAdjustedEndpoint(1));
			getHealthMetrics = RequestStream<struct GetHealthMetricsRequest>(
			    getConsistentReadVersion.getEndpoint().getAdjustedEndpoint(2));
			refreshGlobalConfig = PublicRequestStream<struct GlobalConfigRefreshRequest>(
			    getConsistentReadVersion.getEndpoint().getAdjustedEndpoint(3));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(getConsistentReadVersion.getReceiver(TaskPriority::ReadSocket));
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(getHealthMetrics.getReceiver());
		streams.push_back(refreshGlobalConfig.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

#endif // FDBCLIENT_GRVPROXYINTERFACE_H
