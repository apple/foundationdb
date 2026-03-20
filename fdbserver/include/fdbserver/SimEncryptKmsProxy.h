/*
 * SimEncryptKmsProxy.h
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

#pragma once

#include "flow/Arena.h"

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/fdbrpc.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"

using SimEncryptKey = std::string;

struct SimKmsProxyInterface {
	constexpr static FileIdentifier file_identifier = 2416711;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct SimGetEncryptKeysByKeyIdsRequest> encryptKeyLookupByKeyIds;
	RequestStream<struct SimGetEncryptKeysByDomainIdsRequest> encryptKeyLookupByDomainId;

	SimKmsProxyInterface() {}

	UID id() const { return encryptKeyLookupByKeyIds.getEndpoint().token; }
	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, waitFailure);
		if (Archive::isDeserializing) {
			encryptKeyLookupByKeyIds = RequestStream<struct SimGetEncryptKeysByKeyIdsRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(1));
			encryptKeyLookupByDomainId = RequestStream<struct SimGetEncryptKeysByDomainIdsRequest>(
			    waitFailure.getEndpoint().getAdjustedEndpoint(2));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(encryptKeyLookupByKeyIds.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		streams.push_back(encryptKeyLookupByDomainId.getReceiver(TaskPriority::DefaultPromiseEndpoint));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct SimEncryptKeyDetails {
	constexpr static FileIdentifier file_identifier = 1227025;
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId encryptKeyId;
	StringRef encryptKey;

	SimEncryptKeyDetails() {}
	explicit SimEncryptKeyDetails(EncryptCipherDomainId domainId,
	                              EncryptCipherBaseKeyId keyId,
	                              StringRef key,
	                              Arena& arena)
	  : encryptDomainId(domainId), encryptKeyId(keyId), encryptKey(StringRef(arena, key)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, encryptKeyId, encryptKey);
	}
};

struct SimGetEncryptKeysByKeyIdsReply {
	constexpr static FileIdentifier file_identifier = 2313778;
	Arena arena;
	std::vector<SimEncryptKeyDetails> encryptKeyDetails;

	SimGetEncryptKeysByKeyIdsReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptKeyDetails, arena);
	}
};

struct SimGetEncryptKeysByKeyIdsRequest {
	constexpr static FileIdentifier file_identifier = 6913396;
	std::vector<std::pair<EncryptCipherBaseKeyId, EncryptCipherDomainId>> encryptKeyIds;
	ReplyPromise<SimGetEncryptKeysByKeyIdsReply> reply;

	SimGetEncryptKeysByKeyIdsRequest() {}
	explicit SimGetEncryptKeysByKeyIdsRequest(
	    const std::vector<std::pair<EncryptCipherBaseKeyId, EncryptCipherDomainId>>& keyIds)
	  : encryptKeyIds(keyIds) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptKeyIds, reply);
	}
};

struct SimGetEncryptKeyByDomainIdReply {
	constexpr static FileIdentifier file_identifier = 3009025;
	Arena arena;
	std::vector<SimEncryptKeyDetails> encryptKeyDetails;

	SimGetEncryptKeyByDomainIdReply() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptKeyDetails, arena);
	}
};

struct SimGetEncryptKeysByDomainIdsRequest {
	constexpr static FileIdentifier file_identifier = 9918682;
	std::vector<EncryptCipherDomainId> encryptDomainIds;
	ReplyPromise<SimGetEncryptKeyByDomainIdReply> reply;

	SimGetEncryptKeysByDomainIdsRequest() {}
	explicit SimGetEncryptKeysByDomainIdsRequest(const std::vector<EncryptCipherDomainId>& ids)
	  : encryptDomainIds(ids) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainIds, reply);
	}
};

Future<Void> simEncryptKmsProxyCore(struct SimKmsProxyInterface interf);
