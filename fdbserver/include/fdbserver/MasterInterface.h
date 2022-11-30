/*
 * MasterInterface.h
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

#ifndef FDBSERVER_MASTERINTERFACE_H
#define FDBSERVER_MASTERINTERFACE_H
#pragma once

#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbclient/VersionVector.h"
#include "fdbserver/TLogInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Notified.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/TLogInterface.h"
#include "flow/swift_support.h"
#include "flow/swift_future_support.h"

using DBRecoveryCount = uint64_t;

// A concrete type that can be referenced (in the context of Optional<set<Tag>>) in Swift.
using SetTag = std::set<Tag>;
using OptionalSetTag = Optional<SetTag>;


struct MasterInterface {
	constexpr static FileIdentifier file_identifier = 5979145;
	LocalityData locality;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct GetCommitVersionRequest> getCommitVersion;
	// Get the centralized live committed version reported by commit proxies.
	RequestStream<struct GetRawCommittedVersionRequest> getLiveCommittedVersion;
	// Report a proxy's committed version.
	RequestStream<struct ReportRawCommittedVersionRequest> reportLiveCommittedVersion;
	RequestStream<struct UpdateRecoveryDataRequest> updateRecoveryData;

	NetworkAddress address() const { return getCommitVersion.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return getCommitVersion.getEndpoint().addresses; }

	UID id() const { return getCommitVersion.getEndpoint().token; }
	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, locality, waitFailure);
		if (Archive::isDeserializing) {
			getCommitVersion =
			    RequestStream<struct GetCommitVersionRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(1));
			getLiveCommittedVersion =
			    RequestStream<struct GetRawCommittedVersionRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(2));
			reportLiveCommittedVersion = RequestStream<struct ReportRawCommittedVersionRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(3));
			updateRecoveryData =
			    RequestStream<struct UpdateRecoveryDataRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(4));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(getCommitVersion.getReceiver(TaskPriority::GetConsistentReadVersion));
		streams.push_back(getLiveCommittedVersion.getReceiver(TaskPriority::GetLiveCommittedVersion));
		streams.push_back(reportLiveCommittedVersion.getReceiver(TaskPriority::ReportLiveCommittedVersion));
		streams.push_back(updateRecoveryData.getReceiver(TaskPriority::UpdateRecoveryTransactionVersion));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct ChangeCoordinatorsRequest {
	constexpr static FileIdentifier file_identifier = 13605416;
	Standalone<StringRef> newConnectionString;
	ReplyPromise<Void> reply; // normally throws even on success!
	UID masterId;

	ChangeCoordinatorsRequest() {}
	ChangeCoordinatorsRequest(Standalone<StringRef> newConnectionString, UID masterId)
	  : newConnectionString(newConnectionString), masterId(masterId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, newConnectionString, reply, masterId);
	}
};

struct ResolverMoveRef {
	constexpr static FileIdentifier file_identifier = 11945475;
	KeyRangeRef range;
	int dest;

	ResolverMoveRef() : dest(0) {}
	ResolverMoveRef(KeyRangeRef const& range, int dest) : range(range), dest(dest) {}
	ResolverMoveRef(Arena& a, const ResolverMoveRef& copyFrom) : range(a, copyFrom.range), dest(copyFrom.dest) {}

	bool operator==(ResolverMoveRef const& rhs) const { return range == rhs.range && dest == rhs.dest; }
	bool operator!=(ResolverMoveRef const& rhs) const { return range != rhs.range || dest != rhs.dest; }

	size_t expectedSize() const { return range.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, dest);
	}
};

struct GetCommitVersionReply {
	constexpr static FileIdentifier file_identifier = 3568822;
	Standalone<VectorRef<ResolverMoveRef>> resolverChanges;
	Version resolverChangesVersion;
	Version version;
	Version prevVersion;
	uint64_t requestNum;

	GetCommitVersionReply() : resolverChangesVersion(0), version(0), prevVersion(0), requestNum(0) {}
	explicit GetCommitVersionReply(Version version, Version prevVersion, uint64_t requestNum)
	  : resolverChangesVersion(0), version(version), prevVersion(prevVersion), requestNum(requestNum) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, resolverChanges, resolverChangesVersion, version, prevVersion, requestNum);
	}
};

struct SWIFT_CXX_IMPORT_OWNED GetCommitVersionRequest {
	constexpr static FileIdentifier file_identifier = 16683181;
	SpanContext spanContext;
	uint64_t requestNum;
	uint64_t mostRecentProcessedRequestNum;
	UID requestingProxy;
	ReplyPromise<GetCommitVersionReply> reply;

	GetCommitVersionRequest() {}
	GetCommitVersionRequest(SpanContext spanContext,
	                        uint64_t requestNum,
	                        uint64_t mostRecentProcessedRequestNum,
	                        UID requestingProxy)
	  : spanContext(spanContext), requestNum(requestNum), mostRecentProcessedRequestNum(mostRecentProcessedRequestNum),
	    requestingProxy(requestingProxy) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requestNum, mostRecentProcessedRequestNum, requestingProxy, reply, spanContext);
	}
};

struct GetTLogPrevCommitVersionReply {
	constexpr static FileIdentifier file_identifier = 16683183;
	GetTLogPrevCommitVersionReply() {}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct SWIFT_CXX_IMPORT_OWNED UpdateRecoveryDataRequest {
	constexpr static FileIdentifier file_identifier = 13605417;
	Version recoveryTransactionVersion;
	Version lastEpochEnd;
	std::vector<CommitProxyInterface> commitProxies;
	std::vector<ResolverInterface> resolvers;
	Optional<int64_t> versionEpoch;
	ReplyPromise<Void> reply;
	int8_t primaryLocality;

	UpdateRecoveryDataRequest() = default;
	UpdateRecoveryDataRequest(Version recoveryTransactionVersion,
	                          Version lastEpochEnd,
	                          const std::vector<CommitProxyInterface>& commitProxies,
	                          const std::vector<ResolverInterface>& resolvers,
	                          Optional<int64_t> versionEpoch,
	                          int8_t primaryLocality)
	  : recoveryTransactionVersion(recoveryTransactionVersion), lastEpochEnd(lastEpochEnd),
	    commitProxies(commitProxies), resolvers(resolvers), versionEpoch(versionEpoch),
	    primaryLocality(primaryLocality) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           recoveryTransactionVersion,
		           lastEpochEnd,
		           commitProxies,
		           resolvers,
		           versionEpoch,
		           reply,
		           primaryLocality);
	}
};

struct SWIFT_CXX_IMPORT_OWNED ReportRawCommittedVersionRequest {
	constexpr static FileIdentifier file_identifier = 1853148;
	Version version;
	bool locked;
	Optional<Value> metadataVersion;
	Version minKnownCommittedVersion;
	Optional<Version> prevVersion; // if present, wait for prevVersion to be committed before replying
	Optional<SetTag> writtenTags;
	ReplyPromise<Void> reply;

	ReportRawCommittedVersionRequest() : version(invalidVersion), locked(false), minKnownCommittedVersion(0) {}
	ReportRawCommittedVersionRequest(Version version,
	                                 bool locked,
	                                 Optional<Value> metadataVersion,
	                                 Version minKnownCommittedVersion,
	                                 Optional<Version> prevVersion,
	                                 Optional<SetTag> writtenTags = Optional<SetTag>())
	  : version(version), locked(locked), metadataVersion(metadataVersion),
	    minKnownCommittedVersion(minKnownCommittedVersion), prevVersion(prevVersion), writtenTags(writtenTags) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, locked, metadataVersion, minKnownCommittedVersion, prevVersion, writtenTags, reply);
	}
};

struct LifetimeToken {
	UID ccID;
	int64_t count;

	LifetimeToken() : count(0) {}

	bool isStillValid(LifetimeToken const& latestToken, bool isLatestID) const {
		return ccID == latestToken.ccID && (count >= latestToken.count || isLatestID);
	}
	bool isEqual(LifetimeToken const& toCompare) {
		return ccID.compare(toCompare.ccID) == 0 && count == toCompare.count;
	}
	std::string toString() const { return ccID.shortString() + format("#%lld", count); }
	void operator++() { ++count; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ccID, count);
	}
};

using MAP_UInt64_GetCommitVersionReply = std::map<uint64_t, GetCommitVersionReply>;

inline void eraseReplies(MAP_UInt64_GetCommitVersionReply &replies,
                         uint64_t untilUpperBound) {
    replies.erase(replies.begin(),
                  replies.upper_bound(untilUpperBound));
}

// Swift value type interface for Notified.
template <class T>
struct NotifiedValue {
    using ValueType = decltype(std::declval<T>().get());
    explicit NotifiedValue(ValueType v = 0) : value(std::make_shared<T>(v)) { }

    [[nodiscard]] __attribute__((swift_attr("import_unsafe"))) Future<Void> whenAtLeast(const ValueType& limit) {
        return value->whenAtLeast(limit);
    }

    [[nodiscard]] ValueType get() const { return value->get(); }

    void set(const ValueType& v) {
        value->set(v);
    }
private:
    std::shared_ptr<T> value;
};

using NotifiedVersionValue = NotifiedValue<NotifiedVersion>;

struct CommitProxyVersionReplies {
  MAP_UInt64_GetCommitVersionReply replies;
    NotifiedVersion latestRequestNum;

	CommitProxyVersionReplies(CommitProxyVersionReplies&& r) noexcept
	  : replies(std::move(r.replies)), latestRequestNum(std::move(r.latestRequestNum)) {}

	void operator=(CommitProxyVersionReplies&& r) noexcept {
		replies = std::move(r.replies);
		latestRequestNum = std::move(r.latestRequestNum);
	}

	CommitProxyVersionReplies() : latestRequestNum(0) {}
};

#endif
