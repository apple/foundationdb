/*
 * StorageServerInterface.h
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

#ifndef FDBSERVER_PTXN_STORAGESERVERINTERFACE_H
#define FDBSERVER_PTXN_STORAGESERVERINTERFACE_H

#pragma once

#include <vector>

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbserver/ptxn/Config.h"
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"

namespace ptxn {

struct StorageServerPushReply {
	constexpr static FileIdentifier file_identifier = 661158;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar);
	}
};

struct StorageServerPushRequest {
	constexpr static FileIdentifier file_identifier = 4090417;

	// Span ID
	SpanID spanID;

	// Team ID
	StorageTeamID storageTeamID;

	// Version of the mutations
	Version version;

	// Arena of the mutations
	Arena arena;

	// Mutations to be sent
	VectorRef<MutationRef> mutations;

	// Response
	ReplyPromise<StorageServerPushReply> reply;

	StorageServerPushRequest() {}
	StorageServerPushRequest(const SpanID& spanID_,
	                         const StorageTeamID& storageTeamID_,
	                         const Version version_,
	                         Arena& arena_,
	                         const VectorRef<MutationRef>& mutations_)
	  : spanID(spanID_), storageTeamID(storageTeamID_), version(version_), arena(arena_), mutations(mutations_) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, spanID, storageTeamID, version, mutations, reply);
	}
};

struct StorageServerInterfaceBase {
	MessageTransferModel getMessageTransferModel() const { return messageTransferModel; }

	virtual void initEndpoints() = 0;

protected:
	MessageTransferModel messageTransferModel;

	explicit StorageServerInterfaceBase(const MessageTransferModel messageTransferModel_)
	  : messageTransferModel(messageTransferModel_) {}

	void initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers);

	template <typename Ar>
	void serializeImpl(Ar& ar) {
		serializer(ar, messageTransferModel);
	}
};

struct StorageServerInterface_ActivelyPull : public StorageServerInterfaceBase {
	constexpr static FileIdentifier file_identifier = 256954;

	StorageServerInterface_ActivelyPull() : StorageServerInterfaceBase(MessageTransferModel::TLogActivelyPush) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		StorageServerInterfaceBase::serializeImpl(ar);
	}

	void initEndpoints();
};

struct StorageServerInterface_PassivelyReceive : public StorageServerInterfaceBase {
	constexpr static FileIdentifier file_identifier = 275385;

	RequestStream<StorageServerPushRequest> pushRequests;

	StorageServerInterface_PassivelyReceive()
	  : StorageServerInterfaceBase(MessageTransferModel::StorageServerActivelyPull) {}

	template <typename Ar>
	void serialize(Ar& ar) {
		StorageServerInterfaceBase::serializeImpl(ar);
	}

	void initEndpoints();
};

std::shared_ptr<StorageServerInterfaceBase> getNewStorageServerInterface(const MessageTransferModel model);

} // namespace ptxn

#endif // FDBSERVER_PTXN_STORAGESERVERINTERFACE_H
