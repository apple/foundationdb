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
#include "flow/Arena.h"
#include "flow/FileIdentifier.h"
#include "flow/IRandom.h"
#include "flow/network.h"
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Locality.h"

struct EncryptKeyProxyInterface {
	constexpr static FileIdentifier file_identifier = 1303419;
	struct LocalityData locality;
	UID myId;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct HaltEncryptKeyProxyRequest> haltEncryptKeyProxy;
	RequestStream<struct EKPGetBaseCipherKeysByIdsRequest> getBaseCipherKeysByIds;
	RequestStream<struct EKPGetLatestBaseCipherKeysRequest> getLatestBaseCipherKeys;

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
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(haltEncryptKeyProxy.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		streams.push_back(getBaseCipherKeysByIds.getReceiver(TaskPriority::Worker));
		streams.push_back(getLatestBaseCipherKeys.getReceiver(TaskPriority::Worker));
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

struct EKPBaseCipherDetails {
	constexpr static FileIdentifier file_identifier = 2149615;
	int64_t encryptDomainId;
	uint64_t baseCipherId;
	StringRef baseCipherKey;

	EKPBaseCipherDetails() : encryptDomainId(0), baseCipherId(0), baseCipherKey(StringRef()) {}
	explicit EKPBaseCipherDetails(int64_t dId, uint64_t id, StringRef key, Arena& arena)
	  : encryptDomainId(dId), baseCipherId(id), baseCipherKey(StringRef(arena, key)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, baseCipherId, baseCipherKey);
	}
};

struct EKPGetBaseCipherKeysByIdsReply {
	constexpr static FileIdentifier file_identifier = 9485259;
	Arena arena;
	std::vector<EKPBaseCipherDetails> baseCipherDetails;
	int numHits;
	Optional<Error> error;

	EKPGetBaseCipherKeysByIdsReply() : numHits(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, baseCipherDetails, numHits, error);
	}
};

struct EKPGetBaseCipherKeysByIdsRequest {
	constexpr static FileIdentifier file_identifier = 4930263;
	UID requesterID;
	std::vector<std::pair<uint64_t, int64_t>> baseCipherIds;
	ReplyPromise<EKPGetBaseCipherKeysByIdsReply> reply;

	EKPGetBaseCipherKeysByIdsRequest() : requesterID(deterministicRandom()->randomUniqueID()) {}
	explicit EKPGetBaseCipherKeysByIdsRequest(UID uid, const std::vector<std::pair<uint64_t, int64_t>>& ids)
	  : requesterID(uid), baseCipherIds(ids) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, baseCipherIds, reply);
	}
};

struct EKPGetLatestBaseCipherKeysReply {
	constexpr static FileIdentifier file_identifier = 4831583;
	Arena arena;
	std::vector<EKPBaseCipherDetails> baseCipherDetails;
	int numHits;
	Optional<Error> error;

	EKPGetLatestBaseCipherKeysReply() : numHits(0) {}
	explicit EKPGetLatestBaseCipherKeysReply(const std::vector<EKPBaseCipherDetails>& cipherDetails)
	  : baseCipherDetails(cipherDetails), numHits(0) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, baseCipherDetails, numHits, error);
	}
};

struct EKPGetLatestBaseCipherKeysRequest {
	constexpr static FileIdentifier file_identifier = 1910123;
	UID requesterID;
	std::vector<uint64_t> encryptDomainIds;
	ReplyPromise<EKPGetLatestBaseCipherKeysReply> reply;

	EKPGetLatestBaseCipherKeysRequest() : requesterID(deterministicRandom()->randomUniqueID()) {}
	explicit EKPGetLatestBaseCipherKeysRequest(UID uid, const std::vector<uint64_t>& ids)
	  : requesterID(uid), encryptDomainIds(ids) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, requesterID, encryptDomainIds, reply);
	}
};

#endif