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

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/ptxn/Config.h"
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"

namespace ptxn {

struct TLogCommitReply {
	constexpr static FileIdentifier file_identifier = 178491;

	Version version;

	TLogCommitReply() {}
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

	// Team ID
	TeamID teamID;

	// Arena
	Arena arena;

	// Messages (store the mutation data)
	StringRef messages;

	// Versions
	Version prevVersion;
	Version version;

	// Debug ID
	Optional<UID> debugID;

	// Response
	ReplyPromise<TLogCommitReply> reply;

	TLogCommitRequest() {}
	TLogCommitRequest(const SpanID& spanID_,
	                  const TeamID& teamID_,
	                  const Arena arena_,
	                  StringRef messages_,
	                  const Version prevVersion_,
	                  const Version version_,
	                  const Optional<UID>& debugID_)
	  : spanID(spanID_), teamID(teamID_), arena(arena_), messages(messages_), prevVersion(prevVersion_),
	    version(version_), debugID(debugID_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, spanID, arena, messages, prevVersion, version, debugID, reply);
	}
};

struct TLogPeekReply {
	static constexpr FileIdentifier file_identifier = 292724;

	Optional<UID> debugID;

	// Arena containing the serialized mutation data, see TLogStorageServerPeekSerializer
	Arena arena;
	// StringRef referring the serialized mutation data, see TLogStorageServerPeekSerializer
	StringRef data;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, debugID, arena, data);
	}
};

struct TLogPeekRequest {
	static constexpr FileIdentifier file_identifier = 356070;

	Optional<UID> debugID;

	// We are interested in versions between [beginVersion, endVersion)
	// Following the C++ custom, the endVersion is *EXCLUSIVE*.
	Version beginVersion;
	Optional<Version> endVersion;
	TeamID teamID;

	ReplyPromise<TLogPeekReply> reply;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, debugID, beginVersion, endVersion, teamID, reply);
	}
};

struct TLogPopRequest {
	static constexpr FileIdentifier file_identifier = 288041;

	Version version;
	TeamID teamID;

	ReplyPromise<Void> reply;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, teamID, reply);
	}
};

struct TLogInterfaceBase {
	RequestStream<TLogCommitRequest> commit;
	RequestStream<TLogPeekRequest> peek;
	RequestStream<TLogPopRequest> pop;

	const UID id;

	MessageTransferModel getMessageTransferModel() const;

	virtual void initEndpoints() = 0;

protected:
	MessageTransferModel messageTransferModel;

	explicit TLogInterfaceBase(const MessageTransferModel messageTransferModel_);

	void initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers);

	template <typename Ar>
	void serializeImpl(Ar& ar) {
		serializer(ar, messageTransferModel, commit);
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

struct TLogCursor {};

struct TLogPullReply {
	constexpr static FileIdentifier file_identifier = 395247;

	TLogCursor cursor;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct TLogPullRequest {
	constexpr static FileIdentifier file_identifier = 3310823;

	Version pullStartVersion;

	ReplyPromise<TLogPullReply> reply;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, pullStartVersion, reply);
	}
};

struct TLogInterface_PassivelyPull : public TLogInterfaceBase {
	constexpr static FileIdentifier file_identifier = 748550;

	RequestStream<TLogPullRequest> pullRequests;

	template <typename Ar>
	void serialize(Ar& ar) {
		TLogInterfaceBase::serializeImpl(ar);
	}

	TLogInterface_PassivelyPull() : TLogInterfaceBase(MessageTransferModel::StorageServerActivelyPull) {}

	void initEndpoints() override;
};

std::shared_ptr<TLogInterfaceBase> getNewTLogInterface(const MessageTransferModel model);

} // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGINTERFACE_H
