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
		}
	}

	void initEndpoints() {
		std::vector<std::pair<FlowReceiver*, TaskPriority>> streams;
		streams.push_back(waitFailure.getReceiver());
		streams.push_back(ekLookupByIds.getReceiver(TaskPriority::Worker));
		streams.push_back(ekLookupByDomainIds.getReceiver(TaskPriority::Worker));
		streams.push_back(blobMetadataReq.getReceiver(TaskPriority::Worker));
		FlowTransport::transport().addEndpoints(streams);
	}
};

struct EncryptCipherKeyDetails {
	constexpr static FileIdentifier file_identifier = 1227025;
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId encryptKeyId;
	StringRef encryptKey;
	Optional<int64_t> refreshAfterSec;
	Optional<int64_t> expireAfterSec;

	EncryptCipherKeyDetails() {}
	explicit EncryptCipherKeyDetails(Arena& arena,
	                                 EncryptCipherDomainId dId,
	                                 EncryptCipherBaseKeyId keyId,
	                                 StringRef key)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(StringRef(arena, key)),
	    refreshAfterSec(Optional<int64_t>()), expireAfterSec(Optional<int64_t>()) {}
	explicit EncryptCipherKeyDetails(EncryptCipherDomainId dId, EncryptCipherBaseKeyId keyId, StringRef key)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(key), refreshAfterSec(Optional<int64_t>()),
	    expireAfterSec(Optional<int64_t>()) {}
	explicit EncryptCipherKeyDetails(Arena& arena,
	                                 EncryptCipherDomainId dId,
	                                 EncryptCipherBaseKeyId keyId,
	                                 StringRef key,
	                                 Optional<int64_t> refAfterSec,
	                                 Optional<int64_t> expAfterSec)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(StringRef(arena, key)), refreshAfterSec(refAfterSec),
	    expireAfterSec(expAfterSec) {}
	explicit EncryptCipherKeyDetails(EncryptCipherDomainId dId,
	                                 EncryptCipherBaseKeyId keyId,
	                                 StringRef key,
	                                 Optional<int64_t> refAfterSec,
	                                 Optional<int64_t> expAfterSec)
	  : encryptDomainId(dId), encryptKeyId(keyId), encryptKey(key), refreshAfterSec(refAfterSec),
	    expireAfterSec(expAfterSec) {}

	bool operator==(const EncryptCipherKeyDetails& toCompare) {
		return encryptDomainId == toCompare.encryptDomainId && encryptKeyId == toCompare.encryptKeyId &&
		       encryptKey.compare(toCompare.encryptKey) == 0;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, encryptKeyId, encryptKey, refreshAfterSec, expireAfterSec);
	}
};

struct KmsConnLookupEKsByKeyIdsRep {
	constexpr static FileIdentifier file_identifier = 2313778;
	Arena arena;
	VectorRef<EncryptCipherKeyDetails> cipherKeyDetails;

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
	explicit KmsConnLookupKeyIdsReqInfo(Arena& arena,
	                                    const EncryptCipherDomainId dId,
	                                    const EncryptCipherBaseKeyId bCId,
	                                    StringRef name)
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
	VectorRef<KmsConnLookupKeyIdsReqInfo> encryptKeyInfos;
	Optional<UID> debugId;
	ReplyPromise<KmsConnLookupEKsByKeyIdsRep> reply;

	KmsConnLookupEKsByKeyIdsReq() {}
	explicit KmsConnLookupEKsByKeyIdsReq(VectorRef<KmsConnLookupKeyIdsReqInfo> keyInfos, Optional<UID> dbgId)
	  : encryptKeyInfos(keyInfos), debugId(dbgId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, encryptKeyInfos, debugId, reply);
	}
};

struct KmsConnLookupEKsByDomainIdsRep {
	constexpr static FileIdentifier file_identifier = 3009025;
	Arena arena;
	VectorRef<EncryptCipherKeyDetails> cipherKeyDetails;

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
	explicit KmsConnLookupDomainIdsReqInfo(Arena& arena, const EncryptCipherDomainId dId, StringRef name)
	  : domainId(dId), domainName(StringRef(arena, name)) {}
	explicit KmsConnLookupDomainIdsReqInfo(const EncryptCipherDomainId dId, StringRef name)
	  : domainId(dId), domainName(name) {}

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
	VectorRef<KmsConnLookupDomainIdsReqInfo> encryptDomainInfos;
	Optional<UID> debugId;
	ReplyPromise<KmsConnLookupEKsByDomainIdsRep> reply;

	KmsConnLookupEKsByDomainIdsReq() {}
	explicit KmsConnLookupEKsByDomainIdsReq(VectorRef<KmsConnLookupDomainIdsReqInfo>& infos, Optional<UID> dbgId)
	  : encryptDomainInfos(infos), debugId(dbgId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, arena, encryptDomainInfos, debugId, reply);
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
	std::vector<BlobMetadataDomainId> domainIds;
	Optional<UID> debugId;
	ReplyPromise<KmsConnBlobMetadataRep> reply;

	KmsConnBlobMetadataReq() {}
	explicit KmsConnBlobMetadataReq(const std::vector<BlobMetadataDomainId>& ids, Optional<UID> dbgId)
	  : domainIds(ids), debugId(dbgId) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainIds, debugId, reply);
	}
};

#endif
