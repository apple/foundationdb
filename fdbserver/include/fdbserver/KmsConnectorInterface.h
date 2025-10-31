/*
 * KmsConnectorInterface.h
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

#ifndef FDBSERVER_KMSCONNECTORINTERFACE_H
#define FDBSERVER_KMSCONNECTORINTERFACE_H
#pragma once

#include "fdbrpc/fdbrpc.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "fdbclient/BlobMetadataUtils.h"

struct KmsConnectorInterface {
	constexpr static FileIdentifier file_identifier = 2416711;
	RequestStream<ReplyPromise<Void>> waitFailure;
	RequestStream<struct KmsConnLookupEKsByKeyIdsReq> ekLookupByIds;
	RequestStream<struct KmsConnLookupEKsByDomainIdsReq> ekLookupByDomainIds;
	RequestStream<struct KmsConnBlobMetadataReq> blobMetadataReq;
	RequestStream<struct KmsConnGetKMSStateReq> getKMSStateReq;

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
			blobMetadataReq =
			    RequestStream<struct KmsConnBlobMetadataReq>(waitFailure.getEndpoint().getAdjustedEndpoint(3));
			getKMSStateReq =
			    RequestStream<struct KmsConnGetKMSStateReq>(waitFailure.getEndpoint().getAdjustedEndpoint(4));
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(ekLookupByIds.getReceiver(TaskPriority::Worker));
		streams.push_back(ekLookupByDomainIds.getReceiver(TaskPriority::Worker));
		streams.push_back(blobMetadataReq.getReceiver(TaskPriority::Worker));
		streams.push_back(getKMSStateReq.getReceiver(TaskPriority::Worker));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct EncryptCipherKeyDetailsRef {
	constexpr static FileIdentifier file_identifier = 1227025;
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId encryptKeyId;
	StringRef encryptKey;
	EncryptCipherKeyCheckValue encryptKCV;
	Optional<int64_t> refreshAfterSec;
	Optional<int64_t> expireAfterSec;

	EncryptCipherKeyDetailsRef()
	  : encryptDomainId(INVALID_ENCRYPT_DOMAIN_ID), encryptKeyId(INVALID_ENCRYPT_CIPHER_KEY_ID),
	    encryptKey(StringRef()) {}
	explicit EncryptCipherKeyDetailsRef(Arena& arena,
	                                    EncryptCipherDomainId dId,
	                                    EncryptCipherBaseKeyId keyId,
	                                    StringRef key,
	                                    EncryptCipherKeyCheckValue keyKCV)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(StringRef(arena, key)), encryptKCV(keyKCV),
	    refreshAfterSec(Optional<int64_t>()), expireAfterSec(Optional<int64_t>()) {}
	explicit EncryptCipherKeyDetailsRef(Arena& arena,
	                                    EncryptCipherDomainId dId,
	                                    EncryptCipherBaseKeyId keyId,
	                                    StringRef key,
	                                    EncryptCipherKeyCheckValue keyKCV,
	                                    Optional<int64_t> refAfterSec,
	                                    Optional<int64_t> expAfterSec)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(StringRef(arena, key)), encryptKCV(keyKCV),
	    refreshAfterSec(refAfterSec), expireAfterSec(expAfterSec) {}

	bool operator==(const EncryptCipherKeyDetailsRef& toCompare) {
		return encryptDomainId == toCompare.encryptDomainId && encryptKeyId == toCompare.encryptKeyId &&
		       encryptKey.compare(toCompare.encryptKey) == 0 && encryptKCV == toCompare.encryptKCV;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, encryptKeyId, encryptKey, encryptKCV, refreshAfterSec, expireAfterSec);
	}
};

struct KmsConnLookupEKsByKeyIdsRep {
	constexpr static FileIdentifier file_identifier = 2313778;
	Arena arena;
	Standalone<VectorRef<EncryptCipherKeyDetailsRef>> cipherKeyDetails;

	KmsConnLookupEKsByKeyIdsRep() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, cipherKeyDetails, arena);
	}
};

struct KmsConnLookupKeyIdsReqInfo {
	constexpr static FileIdentifier file_identifier = 3092256;
	// Encryption at-rest relies on partitioning database to define encryption domains, one such possible domain is
	// "Tenants". It is possible that KMS assigned 'baseCipherId' embedded 'encryption domain information', hence, make
	// the field optional.
	Optional<EncryptCipherDomainId> domainId;
	EncryptCipherBaseKeyId baseCipherId;

	KmsConnLookupKeyIdsReqInfo() : domainId(INVALID_ENCRYPT_DOMAIN_ID), baseCipherId(INVALID_ENCRYPT_CIPHER_KEY_ID) {}
	explicit KmsConnLookupKeyIdsReqInfo(const Optional<EncryptCipherDomainId> dId, const EncryptCipherBaseKeyId bCId)
	  : domainId(dId), baseCipherId(bCId) {}

	bool operator==(const KmsConnLookupKeyIdsReqInfo& info) const {
		if (domainId.present()) {
			if (!info.domainId.present() || domainId.get() != info.domainId.get()) {
				return false;
			}
		}
		return baseCipherId == info.baseCipherId;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, baseCipherId);
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
		serializer(ar, encryptKeyInfos, debugId, reply, arena);
	}
};

struct KmsConnLookupEKsByDomainIdsRep {
	constexpr static FileIdentifier file_identifier = 3009025;
	Arena arena;
	Standalone<VectorRef<EncryptCipherKeyDetailsRef>> cipherKeyDetails;

	KmsConnLookupEKsByDomainIdsRep() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, cipherKeyDetails, arena);
	}
};

struct KmsConnLookupEKsByDomainIdsReq {
	constexpr static FileIdentifier file_identifier = 9918682;
	Arena arena;
	std::vector<EncryptCipherDomainId> encryptDomainIds;
	Optional<UID> debugId;
	ReplyPromise<KmsConnLookupEKsByDomainIdsRep> reply;

	KmsConnLookupEKsByDomainIdsReq() {}
	explicit KmsConnLookupEKsByDomainIdsReq(std::vector<EncryptCipherDomainId>& ids, Optional<UID> dbgId)
	  : encryptDomainIds(ids), debugId(dbgId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainIds, debugId, reply, arena);
	}
};

struct KmsConnBlobMetadataRep {
	constexpr static FileIdentifier file_identifier = 2919714;
	Standalone<VectorRef<BlobMetadataDetailsRef>> metadataDetails;

	KmsConnBlobMetadataRep() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, metadataDetails);
	}
};

struct KmsConnBlobMetadataReq {
	constexpr static FileIdentifier file_identifier = 3913147;
	Arena arena;
	std::vector<EncryptCipherDomainId> domainIds;
	Optional<UID> debugId;
	ReplyPromise<KmsConnBlobMetadataRep> reply;

	KmsConnBlobMetadataReq() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainIds, debugId, reply, arena);
	}
};

struct KmsConnGetKMSStateRep {
	constexpr static FileIdentifier file_identifier = 111862;
	Arena arena;
	VectorRef<StringRef> restKMSUrls;
	bool kmsStable;

	KmsConnGetKMSStateRep() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, restKMSUrls, kmsStable, arena);
	}
};

struct KmsConnGetKMSStateReq {
	constexpr static FileIdentifier file_identifier = 2349929;
	ReplyPromise<KmsConnGetKMSStateRep> reply;

	KmsConnGetKMSStateReq() = default;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

#endif
