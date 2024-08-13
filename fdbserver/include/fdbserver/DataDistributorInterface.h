/*
 * DataDistributorInterface.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_DATADISTRIBUTORINTERFACE_H
#define FDBSERVER_DATADISTRIBUTORINTERFACE_H

#include "fdbclient/ClusterInterface.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/fdbrpc.h"
#include <unordered_set>

FDB_BOOLEAN_PARAM(IsMocked);

struct DataDistributorInterface {
	constexpr static FileIdentifier file_identifier = 12383874;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltDataDistributorRequest> haltDataDistributor;
	struct LocalityData locality;
	UID myId;
	RequestStream<struct DistributorSnapRequest> distributorSnapReq;
	RequestStream<struct DistributorExclusionSafetyCheckRequest> distributorExclCheckReq;
	RequestStream<struct GetDataDistributorMetricsRequest> dataDistributorMetrics;
	RequestStream<struct DistributorSplitRangeRequest> distributorSplitRange;
	RequestStream<struct GetStorageWigglerStateRequest> storageWigglerState;
	RequestStream<struct TriggerAuditRequest> triggerAudit;
	RequestStream<struct TenantsOverStorageQuotaRequest> tenantsOverStorageQuota;
	RequestStream<struct PrepareBlobRestoreRequest> prepareBlobRestoreReq;

	DataDistributorInterface() = default;
	explicit DataDistributorInterface(const struct LocalityData& l, UID id) : locality(l), myId(id) {}

	void initEndpoints() {}
	UID id() const { return myId; }
	NetworkAddress address() const { return waitFailure.getEndpoint().getPrimaryAddress(); }
	bool operator==(const DataDistributorInterface& r) const { return id() == r.id(); }
	bool operator!=(const DataDistributorInterface& r) const { return !(*this == r); }

	template <class Archive>
	void serialize(Archive& ar) {
		serializer(ar,
		           waitFailure,
		           haltDataDistributor,
		           locality,
		           myId,
		           distributorSnapReq,
		           distributorExclCheckReq,
		           dataDistributorMetrics,
		           distributorSplitRange,
		           storageWigglerState,
		           triggerAudit,
		           tenantsOverStorageQuota,
		           prepareBlobRestoreReq);
	}
};

struct PrepareBlobRestoreReply {
	constexpr static FileIdentifier file_identifier = 1024888;

	enum Type : int8_t { SUCCESS = 0, CONFLICT_SNAPSHOT, CONFLICT_BLOB_RESTORE };
	int8_t res;

	PrepareBlobRestoreReply() = default;
	PrepareBlobRestoreReply(Type t) : res(int8_t(t)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, res);
	}

	std::string toString() const {
		switch (res) {
		case SUCCESS:
			return "success";
		case CONFLICT_SNAPSHOT:
			return "conflict_snapshot";
		case CONFLICT_BLOB_RESTORE:
			return "conflict_blob_restore";
		default:
			ASSERT(false);
		}
		return "";
	}
};

// when receive this request, DD will do the preparation work for blob migrator
struct PrepareBlobRestoreRequest {
	constexpr static FileIdentifier file_identifier = 1704246;
	UID requesterID;
	ReplyPromise<PrepareBlobRestoreReply> reply;
	StorageServerInterface ssi;
	KeyRange keys;

	PrepareBlobRestoreRequest() = default;
	PrepareBlobRestoreRequest(const UID& id, StorageServerInterface ssi, KeyRangeRef keyRange)
	  : requesterID(id), ssi(std::move(ssi)), keys(keyRange) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply, ssi, keys);
	}
};

struct HaltDataDistributorRequest {
	constexpr static FileIdentifier file_identifier = 1904127;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltDataDistributorRequest() = default;
	explicit HaltDataDistributorRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

struct GetDataDistributorMetricsReply {
	constexpr static FileIdentifier file_identifier = 1284337;
	Standalone<VectorRef<DDMetricsRef>> storageMetricsList;
	Optional<int64_t> midShardSize;

	GetDataDistributorMetricsReply() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, storageMetricsList, midShardSize);
	}
};

struct GetDataDistributorMetricsRequest {
	constexpr static FileIdentifier file_identifier = 1059267;
	KeyRange keys;
	int shardLimit;
	ReplyPromise<struct GetDataDistributorMetricsReply> reply;
	bool midOnly = false;

	GetDataDistributorMetricsRequest() = default;
	explicit GetDataDistributorMetricsRequest(KeyRange const& keys, const int shardLimit, bool midOnly = false)
	  : keys(keys), shardLimit(shardLimit), midOnly(midOnly) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keys, shardLimit, reply, midOnly);
	}
};

struct DistributorSnapRequest {
	constexpr static FileIdentifier file_identifier = 5427684;
	Arena arena;
	StringRef snapPayload;
	UID snapUID;
	ReplyPromise<Void> reply;
	Optional<UID> debugID;

	explicit DistributorSnapRequest(Optional<UID> const& debugID = Optional<UID>()) : debugID(debugID) {}
	explicit DistributorSnapRequest(StringRef snap, UID snapUID, Optional<UID> debugID = Optional<UID>())
	  : snapPayload(snap), snapUID(snapUID), debugID(debugID) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, snapPayload, snapUID, reply, debugID, arena);
	}
};

struct DistributorExclusionSafetyCheckReply {
	constexpr static FileIdentifier file_identifier = 13005960;
	bool safe;

	DistributorExclusionSafetyCheckReply() : safe(false) {}
	explicit DistributorExclusionSafetyCheckReply(bool safe) : safe(safe) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, safe);
	}
};

struct DistributorExclusionSafetyCheckRequest {
	constexpr static FileIdentifier file_identifier = 5830931;
	std::vector<AddressExclusion> exclusions;
	ReplyPromise<DistributorExclusionSafetyCheckReply> reply;

	DistributorExclusionSafetyCheckRequest() = default;
	explicit DistributorExclusionSafetyCheckRequest(std::vector<AddressExclusion> exclusions)
	  : exclusions(std::move(exclusions)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, exclusions, reply);
	}
};

// Insert split points, and distribute the resulted shards to different teams.
struct DistributorSplitRangeRequest {
	constexpr static FileIdentifier file_identifier = 1384441;
	std::vector<Key> splitPoints;
	ReplyPromise<SplitShardReply> reply;

	DistributorSplitRangeRequest() = default;
	explicit DistributorSplitRangeRequest(std::vector<Key> splitPoints) : splitPoints{ std::move(splitPoints) } {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, splitPoints, reply);
	}
};

struct GetStorageWigglerStateReply {
	constexpr static FileIdentifier file_identifier = 356721;
	uint8_t primary = 0, remote = 0; // StorageWiggler::State enum
	double lastStateChangePrimary = 0.0, lastStateChangeRemote = 0.0;

	GetStorageWigglerStateReply() = default;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, primary, remote);
	}
};

struct GetStorageWigglerStateRequest {
	constexpr static FileIdentifier file_identifier = 356722;
	ReplyPromise<GetStorageWigglerStateReply> reply;

	GetStorageWigglerStateRequest() = default;
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct TenantsOverStorageQuotaReply {
	constexpr static FileIdentifier file_identifier = 5952266;
	std::unordered_set<int64_t> tenants;

	TenantsOverStorageQuotaReply() = default;
	explicit TenantsOverStorageQuotaReply(std::unordered_set<int64_t> const& tenants) : tenants(tenants) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, tenants);
	}
};

struct TenantsOverStorageQuotaRequest {
	constexpr static FileIdentifier file_identifier = 84290;
	ReplyPromise<TenantsOverStorageQuotaReply> reply;

	TenantsOverStorageQuotaRequest() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

#endif // FDBSERVER_DATADISTRIBUTORINTERFACE_H
