/*
 * TLogInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PTXN_TLOGINTERFACE_H
#define FDBSERVER_PTXN_TLOGINTERFACE_H

#pragma once

#include <memory>
#include <vector>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/ptxn/Config.h"
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"

namespace ptxn {

struct TLogQueueEntryRef {
	UID id;
	std::vector<StorageTeamID> storageTeams;
	std::vector<StringRef> messages;
	Version version;
	Version knownCommittedVersion;

	TLogQueueEntryRef() : version(0), knownCommittedVersion(0) {}
	TLogQueueEntryRef(Arena& a, TLogQueueEntryRef const& from)
	  : id(from.id), storageTeams(from.storageTeams), version(from.version),
	    knownCommittedVersion(from.knownCommittedVersion) {
		messages.reserve(from.messages.size());
		for (const auto& message : from.messages) {
			messages.emplace_back(a, message);
		}
	}

	// To change this serialization, ProtocolVersion::TLogQueueEntryRef must be updated, and downgrades need to be
	// considered
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, messages, knownCommittedVersion, id, storageTeams, messages);
	}
	size_t expectedSize() const {
		size_t total = 0;
		for (const auto& message : messages) {
			total += message.expectedSize();
		}
		return total;
	}
};

typedef Standalone<TLogQueueEntryRef> TLogQueueEntry;

struct TLogCommitReply {
	constexpr static FileIdentifier file_identifier = 178491;

	Version version;

	TLogCommitReply() = default;
	explicit TLogCommitReply(Version version) : version(version) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
	}
};

struct TLogCommitRequest {
	constexpr static FileIdentifier file_identifier = 316371;

	// SpanID for tracing
	SpanID spanID;

	TLogGroupID tLogGroupID;

	// Arena
	Arena arena;

	// Serialized messages
	std::unordered_map<StorageTeamID, StringRef> messages;

	// Versions
	Version prevVersion;
	Version version;
	Version knownCommittedVersion;
	Version minKnownCommittedVersion;

	// Team changes within group.
	std::set<ptxn::StorageTeamID> addedTeams;
	std::set<ptxn::StorageTeamID> removedTeams;

	// Maps storage team ID to the list of tags within that team.x
	std::map<ptxn::StorageTeamID, std::vector<Tag>> teamToTags;

	// Debug ID
	Optional<UID> debugID;

	// Response
	ReplyPromise<ptxn::TLogCommitReply> reply;

	TLogCommitRequest() = default;
	TLogCommitRequest(const SpanID& spanID_,
	                  const TLogGroupID& tLogGroupID_,
	                  const Arena arena_,
	                  std::unordered_map<StorageTeamID, StringRef> messages_,
	                  const Version prevVersion_,
	                  const Version version_,
	                  const Version knownCommittedVersion_,
	                  const Version minKnownCommittedVersion_,
	                  const std::set<ptxn::StorageTeamID>& addedTeams_,
	                  const std::set<ptxn::StorageTeamID>& removedTeams_,
	                  std::map<ptxn::StorageTeamID, vector<Tag>> teamToTags_,
	                  const Optional<UID>& debugID_)
	  : spanID(spanID_), tLogGroupID(tLogGroupID_), arena(arena_), messages(std::move(messages_)),
	    prevVersion(prevVersion_), version(version_), knownCommittedVersion(knownCommittedVersion_),
	    minKnownCommittedVersion(minKnownCommittedVersion_), addedTeams(addedTeams_), removedTeams(removedTeams_),
	    teamToTags(teamToTags_), debugID(debugID_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           spanID,
		           tLogGroupID,
		           arena,
		           messages,
		           prevVersion,
		           version,
		           knownCommittedVersion,
		           minKnownCommittedVersion,
		           debugID,
		           addedTeams,
		           removedTeams,
				   teamToTags,
		           reply);
	}
};

struct TLogPeekReply {
	static constexpr FileIdentifier file_identifier = 292724;

	Optional<UID> debugID;

	// Arena containing the serialized mutation data
	Arena arena;
	// StringRef referring the serialized mutation data
	StringRef data;

	// The first version of this reply. May be non-present if reply is empty.
	Optional<Version> beginVersion;
	// Non-inclusive end version of this reply.
	Version endVersion;

	Optional<Version> popped;

	Version maxKnownVersion;
	Version minKnownCommittedVersion;

	bool onlySpilled = false;

	TLogPeekReply() = default;
	TLogPeekReply(const Optional<UID>& debugID_, Arena arena_, StringRef data_)
	  : debugID(debugID_), arena(arena_), data(data_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           debugID,
		           arena,
		           data,
		           beginVersion,
		           endVersion,
		           popped,
		           maxKnownVersion,
		           minKnownCommittedVersion,
		           onlySpilled);
	}
};

struct TLogPeekRequest {
	static constexpr FileIdentifier file_identifier = 356070;

	Optional<UID> debugID;

	Arena arena;
	// We are interested in versions between [beginVersion, endVersion)
	// Following the C++ custom, the endVersion is *EXCLUSIVE*.
	Version beginVersion;
	Optional<Version> endVersion;
	StorageTeamID storageTeamID;
	TLogGroupID tLogGroupID;

	Tag tag;
	bool returnIfBlocked;
	bool onlySpilled;
	Optional<std::pair<UID, int>> sequence;
	ReplyPromise<TLogPeekReply> reply;

	TLogPeekRequest() = default;
	TLogPeekRequest(const Optional<UID>& debugID_,
	                const Version& beginVersion_,
	                const Optional<Version>& endVersion_,
	                bool returnIfBlocked_,
	                bool onlySpilled_,
	                const StorageTeamID& storageTeamID_,
	                const TLogGroupID& tLogGroupID_)
	  : debugID(debugID_), beginVersion(beginVersion_), endVersion(endVersion_), storageTeamID(storageTeamID_),
	    tLogGroupID(tLogGroupID_), returnIfBlocked(returnIfBlocked_), onlySpilled(onlySpilled_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           debugID,
		           arena,
		           beginVersion,
		           endVersion,
		           storageTeamID,
		           tLogGroupID,
		           tag,
		           returnIfBlocked,
		           onlySpilled,
		           sequence,
		           reply);
	}
};

struct TLogPopRequest {
	static constexpr FileIdentifier file_identifier = 288041;

	Arena arena;
	Version version;
	Version durableKnownCommittedVersion;
	Tag tag;
	StorageTeamID storageTeamID;

	ReplyPromise<Void> reply;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, version, durableKnownCommittedVersion, tag, storageTeamID, reply);
	}
};

struct TLogGroupLockResult {
	constexpr static FileIdentifier file_identifier = 4635715;

	TLogGroupID id;
	Version end;
	Version knownCommittedVersion;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, id, end, knownCommittedVersion);
	}
};

struct TLogLockResult {
	constexpr static FileIdentifier file_identifier = 5232634;
	std::vector<TLogGroupLockResult> groupResults;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, groupResults);
	}
};

struct TLogRecoveryFinishedRequest {
	constexpr static FileIdentifier file_identifier = 6634364;
	ReplyPromise<Void> reply;

	TLogRecoveryFinishedRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct TLogConfirmRunningRequest {
	constexpr static FileIdentifier file_identifier = 8013123;
	Optional<UID> debugID;
	ReplyPromise<Void> reply;

	TLogConfirmRunningRequest() {}
	TLogConfirmRunningRequest(Optional<UID> debugID) : debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, debugID, reply);
	}
};

struct VerUpdateRef {
	Version version;
	VectorRef<MutationRef> mutations;
	bool isPrivateData;

	VerUpdateRef() : version(invalidVersion), isPrivateData(false) {}
	VerUpdateRef(Arena& to, const VerUpdateRef& from)
	  : version(from.version), mutations(to, from.mutations), isPrivateData(from.isPrivateData) {}
	int expectedSize() const { return mutations.expectedSize(); }

	MutationRef push_back_deep(Arena& arena, const MutationRef& m) {
		mutations.push_back_deep(arena, m);
		return mutations.back();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, mutations, isPrivateData);
	}
};

struct TagMessagesRef {
	Tag tag;
	VectorRef<int> messageOffsets;

	TagMessagesRef() {}
	TagMessagesRef(Arena& a, const TagMessagesRef& from) : tag(from.tag), messageOffsets(a, from.messageOffsets) {}

	size_t expectedSize() const { return messageOffsets.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tag, messageOffsets);
	}
};

struct TLogQueuingMetricsReply {
	constexpr static FileIdentifier file_identifier = 3123450;
	double localTime;
	int64_t instanceID; // changes if bytesDurable and bytesInput reset
	int64_t bytesDurable, bytesInput;
	StorageBytes storageBytes;
	Version v; // committed version

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, localTime, instanceID, bytesDurable, bytesInput, storageBytes, v);
	}
};

struct TLogQueuingMetricsRequest {
	constexpr static FileIdentifier file_identifier = 8756454;
	ReplyPromise<struct TLogQueuingMetricsReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct TLogDisablePopRequest {
	constexpr static FileIdentifier file_identifier = 4252331;
	Arena arena;
	UID snapUID;
	ReplyPromise<Void> reply;
	Optional<UID> debugID;

	TLogDisablePopRequest() = default;
	TLogDisablePopRequest(const UID uid) : snapUID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapUID, reply, arena, debugID);
	}
};

struct TLogEnablePopRequest {
	constexpr static FileIdentifier file_identifier = 9025233;
	Arena arena;
	UID snapUID;
	ReplyPromise<Void> reply;
	Optional<UID> debugID;

	TLogEnablePopRequest() = default;
	TLogEnablePopRequest(const UID uid) : snapUID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapUID, reply, arena, debugID);
	}
};

struct TLogSnapRequest {
	constexpr static FileIdentifier file_identifier = 6454632;
	ReplyPromise<Void> reply;
	Arena arena;
	StringRef snapPayload;
	UID snapUID;
	StringRef role;

	TLogSnapRequest(StringRef snapPayload, UID snapUID, StringRef role)
	  : snapPayload(snapPayload), snapUID(snapUID), role(role) {}
	TLogSnapRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply, snapPayload, snapUID, role, arena);
	}
};

struct TLogInterfaceBase {

	constexpr static FileIdentifier file_identifier = 4121433;

	RequestStream<TLogCommitRequest> commit;
	RequestStream<TLogPeekRequest> peek;
	RequestStream<TLogPopRequest> pop;
	RequestStream<ReplyPromise<TLogLockResult>> lock; // first stage of database recovery
	RequestStream<TLogQueuingMetricsRequest> getQueuingMetrics;
	RequestStream<TLogConfirmRunningRequest> confirmRunning; // used for getReadVersion requests from client
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<TLogRecoveryFinishedRequest> recoveryFinished;
	RequestStream<TLogSnapRequest> snapRequest;

	UID id() const { return uniqueID; }
	UID getSharedTLogID() const { return sharedTLogID; }
	std::string toString() const { return id().shortString(); }
	bool operator==(TLogInterfaceBase const& r) const { return id() == r.id(); }
	bool operator!=(const TLogInterfaceBase& r) const { return !this->operator==(r); }
	NetworkAddress address() const { return commit.getEndpoint().getPrimaryAddress(); }
	Optional<NetworkAddress> secondaryAddress() const { return commit.getEndpoint().addresses.secondaryAddress; }
	const LocalityData& getLocality() const { return filteredLocality; }
	LocalityData& getFilteredLocality() { return filteredLocality; }

	MessageTransferModel getMessageTransferModel() const;

	virtual void initEndpoints() = 0;

protected:
	UID uniqueID;
	UID sharedTLogID;
	MessageTransferModel messageTransferModel;
	LocalityData filteredLocality;

	explicit TLogInterfaceBase(const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : TLogInterfaceBase(LocalityData(), model_) {}

	TLogInterfaceBase(const LocalityData& locality_,
	                  const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : uniqueID(deterministicRandom()->randomUniqueID()), sharedTLogID(uniqueID), messageTransferModel(model_),
	    filteredLocality(locality_) {}

	TLogInterfaceBase(UID sharedTLogID_,
	                  const LocalityData& locality_,
	                  const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : TLogInterfaceBase(deterministicRandom()->randomUniqueID(), sharedTLogID_, locality_, model_) {}

	TLogInterfaceBase(UID id_,
	                  UID sharedTLogID_,
	                  const LocalityData& locality_,
	                  const MessageTransferModel& model_ = MessageTransferModel::StorageServerActivelyPull)
	  : uniqueID(id_), sharedTLogID(sharedTLogID_), messageTransferModel(model_), filteredLocality(locality_) {}

	void initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers);

	template <typename Ar>
	void serializeImpl(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.isDeserializing || uniqueID != UID());
		}
		serializer(ar, uniqueID, sharedTLogID, filteredLocality, messageTransferModel, commit);
		// We only serialize the first RequestStream, all the rest have tokens + offset
		// as described in the deserializing, i.e., getAdjustedEndpoint(offset). This is
		// to save space for network message size.
		if (Ar::isDeserializing) {
			peek = RequestStream<TLogPeekRequest>(commit.getEndpoint().getAdjustedEndpoint(1));
			pop = RequestStream<TLogPopRequest>(commit.getEndpoint().getAdjustedEndpoint(2));
			lock = RequestStream<ReplyPromise<TLogLockResult>>(commit.getEndpoint().getAdjustedEndpoint(3));
			getQueuingMetrics = RequestStream<TLogQueuingMetricsRequest>(commit.getEndpoint().getAdjustedEndpoint(4));
			confirmRunning = RequestStream<TLogConfirmRunningRequest>(commit.getEndpoint().getAdjustedEndpoint(5));
			waitFailure = RequestStream<ReplyPromise<Void>>(commit.getEndpoint().getAdjustedEndpoint(6));
			recoveryFinished = RequestStream<TLogRecoveryFinishedRequest>(commit.getEndpoint().getAdjustedEndpoint(7));
			snapRequest = RequestStream<TLogSnapRequest>(commit.getEndpoint().getAdjustedEndpoint(8));
		}
	}
};

struct TLogInterface_ActivelyPush : public TLogInterfaceBase {
	constexpr static FileIdentifier file_identifier = 386669;

	template <typename Ar>
	void serialize(Ar& ar) {
		TLogInterfaceBase::serializeImpl(ar);
	}

	TLogInterface_ActivelyPush() : TLogInterfaceBase(MessageTransferModel::TLogActivelyPush) {}

	void initEndpoints() override;
};

struct TLogInterface_PassivelyPull : public TLogInterfaceBase {
	constexpr static FileIdentifier file_identifier = 748550;

	RequestStream<TLogDisablePopRequest> disablePopRequest;
	RequestStream<TLogEnablePopRequest> enablePopRequest;

	template <typename Ar>
	void serialize(Ar& ar) {
		TLogInterfaceBase::serializeImpl(ar);
		if (Ar::isDeserializing) {
			disablePopRequest = RequestStream<TLogDisablePopRequest>(commit.getEndpoint().getAdjustedEndpoint(9));
			enablePopRequest = RequestStream<TLogEnablePopRequest>(commit.getEndpoint().getAdjustedEndpoint(10));
		}
	}

	TLogInterface_PassivelyPull() : TLogInterfaceBase(LocalityData()) {}

	explicit TLogInterface_PassivelyPull(const LocalityData& locality_)
	  : TLogInterfaceBase(deterministicRandom()->randomUniqueID(), locality_) {}

	TLogInterface_PassivelyPull(const UID sharedLogId_, const LocalityData& locality_)
	  : TLogInterfaceBase(deterministicRandom()->randomUniqueID(), sharedLogId_, locality_) {}

	TLogInterface_PassivelyPull(const UID id_, const UID sharedLogId_, LocalityData& locality_)
	  : TLogInterfaceBase(id_, sharedLogId_, locality_) {}

	void initEndpoints() override;
};

std::shared_ptr<TLogInterfaceBase> getNewTLogInterface(const MessageTransferModel model,
                                                       UID id_ = deterministicRandom()->randomUniqueID(),
                                                       UID sharedTLogID_ = deterministicRandom()->randomUniqueID(),
                                                       LocalityData locality = LocalityData());

} // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGINTERFACE_H
