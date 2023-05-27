/*
 * EncryptKeyProxyInterface.h
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

#ifndef FDBSERVER_ENCRYPTKEYPROXYINTERFACE_H
#define FDBSERVER_ENCRYPTKEYPROXYINTERFACE_H
#pragma once

#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/IRandom.h"
#include "flow/network.h"

#include <limits>

#define DEBUG_ENCRYPT_KEY_PROXY false

struct KMSHealthStatus {
	constexpr static FileIdentifier file_identifier = 2378149;
	bool canConnectToKms;
	bool canConnectToEKP;
	double lastUpdatedTS;

	KMSHealthStatus() : canConnectToEKP(false), canConnectToKms(false), lastUpdatedTS(-1) {}
	KMSHealthStatus(bool canConnectToKms, bool canConnectToEKP, double lastUpdatedTS)
	  : canConnectToKms(canConnectToKms), canConnectToEKP(canConnectToEKP), lastUpdatedTS(lastUpdatedTS) {}

	bool operator==(const KMSHealthStatus& other) {
		return canConnectToKms == other.canConnectToKms && canConnectToEKP == other.canConnectToEKP;
	}

	std::string toString() const {
		std::stringstream ss;
		ss << "CanConnectToKms(" << canConnectToKms << ")"
		   << ", CanConnectToEKP(" << canConnectToEKP << ")"
		   << ", LastUpdatedTS(" << lastUpdatedTS << ")";
		return ss.str();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, canConnectToKms, canConnectToEKP, lastUpdatedTS);
	}
};

struct EncryptKeyProxyInterface {
	constexpr static FileIdentifier file_identifier = 1303419;
	struct LocalityData locality;
	UID myId;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltEncryptKeyProxyRequest> haltEncryptKeyProxy;
	RequestStream<struct EKPGetBaseCipherKeysByIdsRequest> getBaseCipherKeysByIds;
	RequestStream<struct EKPGetLatestBaseCipherKeysRequest> getLatestBaseCipherKeys;
	RequestStream<struct EKPGetLatestBlobMetadataRequest> getLatestBlobMetadata;
	RequestStream<struct EncryptKeyProxyHealthStatusRequest> getHealthStatus;

	EncryptKeyProxyInterface() {}
	explicit EncryptKeyProxyInterface(const struct LocalityData& loc, UID id) : locality(loc), myId(id) {}

	NetworkAddress address() const { return haltEncryptKeyProxy.getEndpoint().getPrimaryAddress(); }
	NetworkAddressList addresses() const { return haltEncryptKeyProxy.getEndpoint().addresses; }

	UID id() const { return myId; }

	bool operator==(const EncryptKeyProxyInterface& toCompare) const { return myId == toCompare.myId; }
	bool operator!=(const EncryptKeyProxyInterface& toCompare) const { return !(*this == toCompare); }

	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, locality, myId, waitFailure);
		if (Archive::isDeserializing) {
			haltEncryptKeyProxy =
			    RequestStream<struct HaltEncryptKeyProxyRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(1));
			getBaseCipherKeysByIds = RequestStream<struct EKPGetBaseCipherKeysByIdsRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(2));
			getLatestBaseCipherKeys = RequestStream<struct EKPGetLatestBaseCipherKeysRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(3));
			getLatestBlobMetadata =
			    RequestStream<struct EKPGetLatestBlobMetadataRequest>(waitFailure.getEndpoint().getAdjustedEndpoint(4));
			getHealthStatus = RequestStream<struct EncryptKeyProxyHealthStatusRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(5));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(haltEncryptKeyProxy.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		streams.push_back(getBaseCipherKeysByIds.getReceiver(TaskPriority::Worker));
		streams.push_back(getLatestBaseCipherKeys.getReceiver(TaskPriority::Worker));
		streams.push_back(getLatestBlobMetadata.getReceiver(TaskPriority::Worker));
		streams.push_back(getHealthStatus.getReceiver(TaskPriority::Worker));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct HaltEncryptKeyProxyRequest {
	constexpr static FileIdentifier file_identifier = 2378138;
	UID requesterID;
	ReplyPromise<Void> reply;

	HaltEncryptKeyProxyRequest() : requesterID(deterministicRandom()->randomUniqueID()) {}
	explicit HaltEncryptKeyProxyRequest(UID uid) : requesterID(uid) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, reply);
	}
};

struct EncryptKeyProxyHealthStatusRequest {
	constexpr static FileIdentifier file_identifier = 2378139;
	ReplyPromise<KMSHealthStatus> reply;

	EncryptKeyProxyHealthStatusRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct EKPBaseCipherDetails {
	constexpr static FileIdentifier file_identifier = 2149615;
	int64_t encryptDomainId;
	uint64_t baseCipherId;
	Standalone<StringRef> baseCipherKey;
	EncryptCipherKeyCheckValue baseCipherKCV;
	int64_t refreshAt;
	int64_t expireAt;

	EKPBaseCipherDetails()
	  : encryptDomainId(0), baseCipherId(0), baseCipherKey(Standalone<StringRef>()), baseCipherKCV(0), refreshAt(0),
	    expireAt(-1) {}
	explicit EKPBaseCipherDetails(int64_t dId,
	                              uint64_t id,
	                              Standalone<StringRef> key,
	                              EncryptCipherKeyCheckValue cipherKCV)
	  : encryptDomainId(dId), baseCipherId(id), baseCipherKey(key), baseCipherKCV(cipherKCV),
	    refreshAt(std::numeric_limits<int64_t>::max()), expireAt(std::numeric_limits<int64_t>::max()) {}
	explicit EKPBaseCipherDetails(int64_t dId,
	                              uint64_t id,
	                              Standalone<StringRef> key,
	                              EncryptCipherKeyCheckValue cipherKCV,
	                              int64_t refAt,
	                              int64_t expAt)
	  : encryptDomainId(dId), baseCipherId(id), baseCipherKey(key), baseCipherKCV(cipherKCV), refreshAt(refAt),
	    expireAt(expAt) {}

	bool operator==(const EKPBaseCipherDetails& r) const {
		return encryptDomainId == r.encryptDomainId && baseCipherId == r.baseCipherId && refreshAt == r.refreshAt &&
		       expireAt == r.expireAt && baseCipherKey.toString() == r.baseCipherKey.toString();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, baseCipherId, baseCipherKey, baseCipherKCV, refreshAt, expireAt);
	}
};

struct EKPGetBaseCipherKeysByIdsReply {
	constexpr static FileIdentifier file_identifier = 9485259;
	std::vector<EKPBaseCipherDetails> baseCipherDetails;
	int numHits;
	Optional<Error> error;

	EKPGetBaseCipherKeysByIdsReply() : numHits(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, baseCipherDetails, numHits, error);
	}
};

struct EKPGetBaseCipherKeysRequestInfo {
	constexpr static FileIdentifier file_identifier = 2180516;
	// Encryption cipher domain identifier
	EncryptCipherDomainId domainId;
	// Encryption cipher KMS assigned identifier
	EncryptCipherBaseKeyId baseCipherId;

	EKPGetBaseCipherKeysRequestInfo()
	  : domainId(INVALID_ENCRYPT_DOMAIN_ID), baseCipherId(INVALID_ENCRYPT_CIPHER_KEY_ID) {}
	EKPGetBaseCipherKeysRequestInfo(const EncryptCipherDomainId dId, const EncryptCipherBaseKeyId bCId)
	  : domainId(dId), baseCipherId(bCId) {}

	bool operator==(const EKPGetBaseCipherKeysRequestInfo& info) const {
		return domainId == info.domainId && baseCipherId == info.baseCipherId;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, baseCipherId);
	}
};

struct EKPGetBaseCipherKeysByIdsRequest {
	constexpr static FileIdentifier file_identifier = 4930263;
	std::vector<EKPGetBaseCipherKeysRequestInfo> baseCipherInfos;
	Optional<UID> debugId;
	ReplyPromise<EKPGetBaseCipherKeysByIdsReply> reply;

	EKPGetBaseCipherKeysByIdsRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, baseCipherInfos, debugId, reply);
	}
};

struct EKPGetLatestBaseCipherKeysReply {
	constexpr static FileIdentifier file_identifier = 4831583;
	std::vector<EKPBaseCipherDetails> baseCipherDetails;
	int numHits;
	Optional<Error> error;

	EKPGetLatestBaseCipherKeysReply() : numHits(0) {}
	explicit EKPGetLatestBaseCipherKeysReply(const std::vector<EKPBaseCipherDetails>& cipherDetails)
	  : baseCipherDetails(cipherDetails), numHits(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, baseCipherDetails, numHits, error);
	}
};

struct EKPGetBaseCipherKeysRequestInfo_Hash {
	std::size_t operator()(const EKPGetBaseCipherKeysRequestInfo& info) const {
		boost::hash<std::pair<EncryptCipherDomainId, EncryptCipherBaseKeyId>> hasher;
		return hasher(std::make_pair(info.domainId, info.baseCipherId));
	}
};

struct EKPGetLatestBaseCipherKeysRequest {
	constexpr static FileIdentifier file_identifier = 1910123;
	std::vector<EncryptCipherDomainId> encryptDomainIds;
	Optional<UID> debugId;
	ReplyPromise<EKPGetLatestBaseCipherKeysReply> reply;

	EKPGetLatestBaseCipherKeysRequest() {}
	explicit EKPGetLatestBaseCipherKeysRequest(const std::vector<EncryptCipherDomainId>& ids) : encryptDomainIds(ids) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainIds, debugId, reply);
	}
};

// partition and credentials information for a given blob domain

struct EKPGetLatestBlobMetadataReply {
	constexpr static FileIdentifier file_identifier = 5761581;
	Standalone<VectorRef<BlobMetadataDetailsRef>> blobMetadataDetails;

	EKPGetLatestBlobMetadataReply() {}
	explicit EKPGetLatestBlobMetadataReply(const Standalone<VectorRef<BlobMetadataDetailsRef>>& blobMetadataDetails)
	  : blobMetadataDetails(blobMetadataDetails) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, blobMetadataDetails);
	}
};

struct EKPGetLatestBlobMetadataRequest {
	constexpr static FileIdentifier file_identifier = 3821549;
	std::vector<EncryptCipherDomainId> domainIds;
	Optional<UID> debugId;
	ReplyPromise<EKPGetLatestBlobMetadataReply> reply;

	EKPGetLatestBlobMetadataRequest() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainIds, debugId, reply);
	}
};

#endif