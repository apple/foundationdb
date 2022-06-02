/*
 * KmsConnectorInterface.h
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

#ifndef FDBSERVER_KMSCONNECTORINTERFACE_H
#define FDBSERVER_KMSCONNECTORINTERFACE_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"

struct KmsConnectorInterface {
	constexpr static FileIdentifier file_identifier = 2416711;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct KmsConnLookupEKsByKeyIdsReq> ekLookupByIds;
	RequestStream<struct KmsConnLookupEKsByDomainIdsReq> ekLookupByDomainIds;

	KmsConnectorInterface() {}

	UID id() const { return ekLookupByIds.getEndpoint().token; }
	template <class Archive>
	void serialize(Archive& ar) {
		if constexpr (!is_fb_function<Archive>) {
			ASSERT(ar.protocolVersion().isValid());
		}
		serializer(ar, waitFailure);
		if (Archive::isDeserializing) {
			ekLookupByIds =
			    RequestStream<struct KmsConnLookupEKsByKeyIdsReq>(waitFailure.getEndpoint().getAdjustedEndpoint(1));
			ekLookupByDomainIds =
			    RequestStream<struct KmsConnLookupEKsByDomainIdsReq>(waitFailure.getEndpoint().getAdjustedEndpoint(2));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(ekLookupByIds.getReceiver(TaskPriority::Worker));
		streams.push_back(ekLookupByDomainIds.getReceiver(TaskPriority::Worker));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct EncryptCipherKeyDetails {
	constexpr static FileIdentifier file_identifier = 1227025;
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId encryptKeyId;
	StringRef encryptKey;

	EncryptCipherKeyDetails() {}
	explicit EncryptCipherKeyDetails(EncryptCipherDomainId dId,
	                                 EncryptCipherBaseKeyId keyId,
	                                 StringRef key,
	                                 Arena& arena)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(StringRef(arena, key)) {}

	bool operator==(const EncryptCipherKeyDetails& toCompare) {
		return encryptDomainId == toCompare.encryptDomainId && encryptKeyId == toCompare.encryptKeyId &&
		       encryptKey.compare(toCompare.encryptKey) == 0;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, encryptKeyId, encryptKey);
	}
};

struct KmsConnLookupEKsByKeyIdsRep {
	constexpr static FileIdentifier file_identifier = 2313778;
	Arena arena;
	std::vector<EncryptCipherKeyDetails> cipherKeyDetails;

	KmsConnLookupEKsByKeyIdsRep() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, cipherKeyDetails);
	}
};

struct KmsConnLookupKeyIdsReqInfo {
	constexpr static FileIdentifier file_identifier = 3092256;
	EncryptCipherDomainId domainId;
	EncryptCipherBaseKeyId baseCipherId;
	EncryptCipherDomainName domainName;

	KmsConnLookupKeyIdsReqInfo() : domainId(ENCRYPT_INVALID_DOMAIN_ID), baseCipherId(ENCRYPT_INVALID_CIPHER_KEY_ID) {}
	explicit KmsConnLookupKeyIdsReqInfo(const EncryptCipherDomainId dId,
	                                    const EncryptCipherBaseKeyId bCId,
	                                    StringRef name,
	                                    Arena& arena)
	  : domainId(dId), baseCipherId(bCId), domainName(StringRef(arena, name)) {}

	bool operator==(const KmsConnLookupKeyIdsReqInfo& info) const {
		return domainId == info.domainId && baseCipherId == info.baseCipherId &&
		       (domainName.compare(info.domainName) == 0);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, baseCipherId, domainName);
	}
};

struct KmsConnLookupEKsByKeyIdsReq {
	constexpr static FileIdentifier file_identifier = 6913396;
	Arena arena;
	std::vector<KmsConnLookupKeyIdsReqInfo> encryptKeyInfos;
	Optional<UID> debugId;
	ReplyPromise<KmsConnLookupEKsByKeyIdsRep> reply;

	KmsConnLookupEKsByKeyIdsReq() {}
	explicit KmsConnLookupEKsByKeyIdsReq(const std::vector<KmsConnLookupKeyIdsReqInfo>& keyInfos, Optional<UID> dbgId)
	  : encryptKeyInfos(keyInfos), debugId(dbgId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, encryptKeyInfos, debugId, reply);
	}
};

struct KmsConnLookupEKsByDomainIdsRep {
	constexpr static FileIdentifier file_identifier = 3009025;
	Arena arena;
	std::vector<EncryptCipherKeyDetails> cipherKeyDetails;

	KmsConnLookupEKsByDomainIdsRep() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, cipherKeyDetails);
	}
};

struct KmsConnLookupDomainIdsReqInfo {
	constexpr static FileIdentifier file_identifier = 8980149;
	EncryptCipherDomainId domainId;
	EncryptCipherDomainName domainName;

	KmsConnLookupDomainIdsReqInfo() : domainId(ENCRYPT_INVALID_DOMAIN_ID) {}
	explicit KmsConnLookupDomainIdsReqInfo(const EncryptCipherDomainId dId, StringRef name, Arena& arena)
	  : domainId(dId), domainName(StringRef(arena, name)) {}

	bool operator==(const KmsConnLookupDomainIdsReqInfo& info) const {
		return domainId == info.domainId && (domainName.compare(info.domainName) == 0);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, domainName);
	}
};

struct KmsConnLookupEKsByDomainIdsReq {
	constexpr static FileIdentifier file_identifier = 9918682;
	Arena arena;
	std::vector<KmsConnLookupDomainIdsReqInfo> encryptDomainInfos;
	Optional<UID> debugId;
	ReplyPromise<KmsConnLookupEKsByDomainIdsRep> reply;

	KmsConnLookupEKsByDomainIdsReq() {}
	explicit KmsConnLookupEKsByDomainIdsReq(const std::vector<KmsConnLookupDomainIdsReqInfo>& infos,
	                                        Optional<UID> dbgId)
	  : encryptDomainInfos(infos), debugId(dbgId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, encryptDomainInfos, debugId, reply);
	}
};

#endif
