/*
 * ClusterInterface.h
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

#ifndef FDBCLIENT_ClusterInterface_H
#define FDBCLIENT_ClusterInterface_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbclient/Status.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/ClientWorkerInterface.h"
#include "fdbclient/ClientVersion.h"

struct ClusterInterface {
	constexpr static FileIdentifier file_identifier = 15888863;
	RequestStream<struct OpenDatabaseRequest> openDatabase;
	RequestStream<struct FailureMonitoringRequest> failureMonitoring;
	RequestStream<struct StatusRequest> databaseStatus;
	RequestStream<ReplyPromise<Void>> ping;
	RequestStream<struct GetClientWorkersRequest> getClientWorkers;
	RequestStream<struct ForceRecoveryRequest> forceRecovery;
	RequestStream<struct MoveShardRequest> moveShard;
	RequestStream<struct RepairSystemDataRequest> repairSystemData;
	RequestStream<struct SplitShardRequest> splitShard;

	bool operator==(ClusterInterface const& r) const { return id() == r.id(); }
	bool operator!=(ClusterInterface const& r) const { return id() != r.id(); }
	UID id() const { return openDatabase.getEndpoint().token; }
	NetworkAddress address() const { return openDatabase.getEndpoint().getPrimaryAddress(); }

	bool hasMessage() const {
		return openDatabase.getFuture().isReady() || failureMonitoring.getFuture().isReady() ||
		       databaseStatus.getFuture().isReady() || ping.getFuture().isReady() ||
		       getClientWorkers.getFuture().isReady() || forceRecovery.getFuture().isReady() ||
		       moveShard.getFuture().isReady() || repairSystemData.getFuture().isReady() ||
		       splitShard.getFuture().isReady();
	}

	void initEndpoints() {
		openDatabase.getEndpoint(TaskPriority::ClusterController);
		failureMonitoring.getEndpoint(TaskPriority::FailureMonitor);
		databaseStatus.getEndpoint(TaskPriority::ClusterController);
		ping.getEndpoint(TaskPriority::ClusterController);
		getClientWorkers.getEndpoint(TaskPriority::ClusterController);
		forceRecovery.getEndpoint(TaskPriority::ClusterController);
		moveShard.getEndpoint(TaskPriority::ClusterController);
		repairSystemData.getEndpoint(TaskPriority::ClusterController);
		splitShard.getEndpoint(TaskPriority::ClusterController);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           openDatabase,
		           failureMonitoring,
		           databaseStatus,
		           ping,
		           getClientWorkers,
		           forceRecovery,
		           moveShard,
		           repairSystemData,
		           splitShard);
	}
};

struct ClusterControllerClientInterface {
	constexpr static FileIdentifier file_identifier = 14997695;
	ClusterInterface clientInterface;

	bool operator==(ClusterControllerClientInterface const& r) const {
		return clientInterface.id() == r.clientInterface.id();
	}
	bool operator!=(ClusterControllerClientInterface const& r) const {
		return clientInterface.id() != r.clientInterface.id();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, clientInterface);
	}
};

template <class T>
struct ItemWithExamples {
	T item;
	int count;
	std::vector<std::pair<NetworkAddress, Key>> examples;

	ItemWithExamples() : item{}, count(0) {}
	ItemWithExamples(T const& item, int count, std::vector<std::pair<NetworkAddress, Key>> const& examples)
	  : item(item), count(count), examples(examples) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, item, count, examples);
	}
};

struct OpenDatabaseRequest {
	constexpr static FileIdentifier file_identifier = 2799502;
	// Sent by the native API to the cluster controller to open a database and track client
	//   info changes.  Returns immediately if the current client info id is different from
	//   knownClientInfoID; otherwise returns when it next changes (or perhaps after a long interval)

	int clientCount;
	std::vector<ItemWithExamples<Key>> issues;
	std::vector<ItemWithExamples<Standalone<ClientVersionRef>>> supportedVersions;
	std::vector<ItemWithExamples<Key>> maxProtocolSupported;

	UID knownClientInfoID;
	ReplyPromise<struct ClientDBInfo> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (!is_fb_function<Ar>) {
			ASSERT(ar.protocolVersion().hasOpenDatabase());
		}
		serializer(ar, clientCount, issues, supportedVersions, maxProtocolSupported, knownClientInfoID, reply);
	}
};

struct SystemFailureStatus {
	constexpr static FileIdentifier file_identifier = 3194108;
	NetworkAddressList addresses;
	FailureStatus status;

	SystemFailureStatus() {}
	SystemFailureStatus(NetworkAddressList const& a, FailureStatus const& s) : addresses(a), status(s) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, addresses, status);
	}
};

struct FailureMonitoringReply {
	constexpr static FileIdentifier file_identifier = 6820325;
	VectorRef<SystemFailureStatus> changes;
	Version failureInformationVersion;
	bool allOthersFailed; // If true, changes are relative to all servers being failed, otherwise to the version given
	                      // in the request
	int clientRequestIntervalMS, // after this many milliseconds, send another request
	    considerServerFailedTimeoutMS; // after this many additional milliseconds, consider the ClusterController itself
	                                   // to be failed
	Arena arena;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           changes,
		           failureInformationVersion,
		           allOthersFailed,
		           clientRequestIntervalMS,
		           considerServerFailedTimeoutMS,
		           arena);
	}
};

struct FailureMonitoringRequest {
	// Sent by all participants to the cluster controller reply.clientRequestIntervalMS
	//   ms after receiving the previous reply.
	// Provides the controller the self-diagnosed status of the sender, and also
	//   requests the status of other systems.  Failure to timely send one of these implies
	//   a failed status.
	// If !senderStatus.present(), the sender wants to receive the latest failure information
	//   but doesn't want to be monitored.
	// The failureInformationVersion returned in reply should be passed back to the
	//   next request to facilitate delta compression of the failure information.

	constexpr static FileIdentifier file_identifier = 5867851;
	Optional<FailureStatus> senderStatus;
	Version failureInformationVersion;
	NetworkAddressList addresses;
	ReplyPromise<struct FailureMonitoringReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, senderStatus, failureInformationVersion, addresses, reply);
	}
};

struct StatusReply {
	constexpr static FileIdentifier file_identifier = 9980504;
	StatusObject statusObj;
	std::string statusStr;

	StatusReply() {}
	explicit StatusReply(StatusObject obj)
	  : statusObj(obj), statusStr(json_spirit::write_string(json_spirit::mValue(obj))) {}
	explicit StatusReply(std::string&& text) : statusStr(text) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, statusStr);
		if (ar.isDeserializing) {
			json_spirit::mValue mv;
			if (g_network->isSimulated()) {
				mv = readJSONStrictly(statusStr);
			} else {
				// In non-simulation allow errors because some status data is better than no status data
				json_spirit::read_string(statusStr, mv);
			}
			statusObj = std::move(mv.get_obj());
		}
	}
};

struct StatusRequest {
	constexpr static FileIdentifier file_identifier = 14419140;
	ReplyPromise<struct StatusReply> reply;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct GetClientWorkersRequest {
	constexpr static FileIdentifier file_identifier = 10771791;
	ReplyPromise<std::vector<ClientWorkerInterface>> reply;

	GetClientWorkersRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct ForceRecoveryRequest {
	constexpr static FileIdentifier file_identifier = 14821350;
	Key dcId;
	ReplyPromise<Void> reply;

	ForceRecoveryRequest() {}
	explicit ForceRecoveryRequest(Key dcId) : dcId(dcId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, dcId, reply);
	}
};

// Request to move a keyrange (shard) to a new team represented as addresses.
struct MoveShardRequest {
	constexpr static FileIdentifier file_identifier = 2799592;

	KeyRange shard;
	std::vector<NetworkAddress> addresses;
	ReplyPromise<Void> reply;

	MoveShardRequest() {}
	MoveShardRequest(KeyRange shard, std::vector<NetworkAddress> addresses)
	  : shard{ std::move(shard) }, addresses{ std::move(addresses) } {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, shard, addresses, reply);
	}
};

// Request to trigger a master recovery, and during the following recovery, the system metadata will be
// reconstructed from TLogs, and written to a new SS team.
// This is used when metadata on SSes are lost or corrupted.
struct RepairSystemDataRequest {
	constexpr static FileIdentifier file_identifier = 2799593;

	ReplyPromise<Void> reply;

	RepairSystemDataRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

// Returns the actual shards generated by the SplitShardRequest.
struct SplitShardReply {
	constexpr static FileIdentifier file_identifier = 1384440;
	std::vector<KeyRange> shards;

	SplitShardReply() {}
	explicit SplitShardReply(std::vector<KeyRange> shards) : shards{ std::move(shards) } {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, shards);
	}
};

// Split keyrange [shard.begin, shard.end) into num shards.
// Split points are chosen as the arithmetically equal division points of the given range.
struct SplitShardRequest {
	constexpr static FileIdentifier file_identifier = 1384443;
	KeyRange shard;
	int num;
	ReplyPromise<SplitShardReply> reply;

	SplitShardRequest() : num(0) {}
	SplitShardRequest(KeyRange shard, int num) : shard{ std::move(shard) }, num(num) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, shard, num, reply);
	}
};
#endif
