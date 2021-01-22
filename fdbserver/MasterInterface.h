/*
 * MasterInterface.h
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

#ifndef FDBSERVER_MASTERINTERFACE_H
#define FDBSERVER_MASTERINTERFACE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/DatabaseConfiguration.h"
#include "fdbserver/TLogInterface.h"

typedef uint64_t DBRecoveryCount;

struct MasterInterface {
	constexpr static FileIdentifier file_identifier = 5979145;
	LocalityData locality;
	RequestStream< ReplyPromise<Void> > waitFailure;
	RequestStream< struct TLogRejoinRequest > tlogRejoin; // sent by tlog (whether or not rebooted) to communicate with a new master
	RequestStream< struct ChangeCoordinatorsRequest > changeCoordinators;
	RequestStream< struct GetCommitVersionRequest > getCommitVersion;
	RequestStream<struct BackupWorkerDoneRequest> notifyBackupWorkerDone;

	NetworkAddress address() const { return changeCoordinators.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return changeCoordinators.getEndpoint().addresses; }

	UID id() const { return changeCoordinators.getEndpoint().token; }
	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, locality, waitFailure);
		if( Archive::isDeserializing ) {
			tlogRejoin = RequestStream< struct TLogRejoinRequest >( waitFailure.getEndpoint().getAdjustedEndpoint(1) );
			changeCoordinators = RequestStream< struct ChangeCoordinatorsRequest >( waitFailure.getEndpoint().getAdjustedEndpoint(2) );
			getCommitVersion = RequestStream< struct GetCommitVersionRequest >( waitFailure.getEndpoint().getAdjustedEndpoint(3) );
			notifyBackupWorkerDone = RequestStream<struct BackupWorkerDoneRequest>( waitFailure.getEndpoint().getAdjustedEndpoint(4) );
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(tlogRejoin.getReceiver(TaskPriority::MasterTLogRejoin));
		streams.push_back(changeCoordinators.getReceiver());
		streams.push_back(getCommitVersion.getReceiver(TaskPriority::GetConsistentReadVersion));
		streams.push_back(notifyBackupWorkerDone.getReceiver());
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct TLogRejoinReply {
	constexpr static FileIdentifier file_identifier = 11;

	// false means someone else registered, so we should re-register.  true means this master is recovered, so don't
	// send again to the same master.
	bool masterIsRecovered;
	TLogRejoinReply() = default;
	explicit TLogRejoinReply(bool masterIsRecovered) : masterIsRecovered(masterIsRecovered) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, masterIsRecovered);
	}
};

struct TLogRejoinRequest {
	constexpr static FileIdentifier file_identifier = 15692200;
	TLogInterface myInterface;
	ReplyPromise<TLogRejoinReply> reply;

	TLogRejoinRequest() { }
	explicit TLogRejoinRequest(const TLogInterface &interf) : myInterface(interf) { }
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, myInterface, reply);
	}
};

struct ChangeCoordinatorsRequest {
	constexpr static FileIdentifier file_identifier = 13605416;
	Standalone<StringRef> newConnectionString;
	ReplyPromise<Void> reply;  // normally throws even on success!

	ChangeCoordinatorsRequest() {}
	ChangeCoordinatorsRequest(Standalone<StringRef> newConnectionString) : newConnectionString(newConnectionString) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, newConnectionString, reply);
	}
};

struct ResolverMoveRef {
	constexpr static FileIdentifier file_identifier = 11945475;
	KeyRangeRef range;
	int dest;

	ResolverMoveRef() : dest(0) {}
	ResolverMoveRef(KeyRangeRef const& range, int dest) : range(range), dest(dest) {}
	ResolverMoveRef( Arena& a, const ResolverMoveRef& copyFrom ) : range(a, copyFrom.range), dest(copyFrom.dest) {}

	bool operator == ( ResolverMoveRef const& rhs ) const {
		return range == rhs.range && dest == rhs.dest;
	}
	bool operator != ( ResolverMoveRef const& rhs ) const {
		return range != rhs.range || dest != rhs.dest;
	}

	size_t expectedSize() const {
		return range.expectedSize();
	}

	template <class Ar>
	void serialize( Ar& ar ) {
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
	explicit GetCommitVersionReply( Version version, Version prevVersion, uint64_t requestNum ) : version(version), prevVersion(prevVersion), resolverChangesVersion(0), requestNum(requestNum) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, resolverChanges, resolverChangesVersion, version, prevVersion, requestNum);
	}
};

struct GetCommitVersionRequest {
	constexpr static FileIdentifier file_identifier = 16683181;
	uint64_t requestNum;
	uint64_t mostRecentProcessedRequestNum;
	UID requestingProxy;
	ReplyPromise<GetCommitVersionReply> reply;

	GetCommitVersionRequest() { }
	GetCommitVersionRequest(uint64_t requestNum, uint64_t mostRecentProcessedRequestNum, UID requestingProxy)
		: requestNum(requestNum), mostRecentProcessedRequestNum(mostRecentProcessedRequestNum), requestingProxy(requestingProxy) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requestNum, mostRecentProcessedRequestNum, requestingProxy, reply);
	}
};

struct BackupWorkerDoneRequest {
	constexpr static FileIdentifier file_identifier = 8736351;
	UID workerUID;
	LogEpoch backupEpoch;
	ReplyPromise<Void> reply;

	BackupWorkerDoneRequest() : workerUID(), backupEpoch(-1) {}
	BackupWorkerDoneRequest(UID id, LogEpoch epoch) : workerUID(id), backupEpoch(epoch) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, workerUID, backupEpoch, reply);
	}
};

struct LifetimeToken {
	UID ccID;
	int64_t count;

	LifetimeToken() : count(0) {}

	bool isStillValid( LifetimeToken const& latestToken, bool isLatestID ) const {
		return ccID == latestToken.ccID && (count >= latestToken.count || isLatestID);
	}
	std::string toString() const {
		return ccID.shortString() + format("#%lld", count);
	}
	void operator++() {
		++count;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ccID, count);
	}
};

#endif
