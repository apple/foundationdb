/*
 * BlobGranuleCommon.h
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

#ifndef FDBCLIENT_BLOBGRANULECOMMON_H
#define FDBCLIENT_BLOBGRANULECOMMON_H
#pragma once

#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"

#include "flow/BlobCipher.h"
#include "flow/EncryptUtils.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"

#include <sstream>

#define BG_ENCRYPT_COMPRESS_DEBUG false

// file format of actual blob files
// FIXME: use VecSerStrategy::String serialization for this
struct GranuleSnapshot : VectorRef<KeyValueRef> {

	constexpr static FileIdentifier file_identifier = 1300395;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<KeyValueRef>&)*this));
	}
};

struct GranuleDeltas : VectorRef<MutationsAndVersionRef> {
	constexpr static FileIdentifier file_identifier = 8563013;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<MutationsAndVersionRef>&)*this));
	}
};

struct BlobGranuleCipherKeysMeta {
	EncryptCipherDomainId textDomainId;
	EncryptCipherBaseKeyId textBaseCipherId;
	EncryptCipherRandomSalt textSalt;
	EncryptCipherDomainId headerDomainId;
	EncryptCipherBaseKeyId headerBaseCipherId;
	EncryptCipherRandomSalt headerSalt;
	std::string ivStr;

	BlobGranuleCipherKeysMeta() {}
	BlobGranuleCipherKeysMeta(const EncryptCipherDomainId tDomainId,
	                          const EncryptCipherBaseKeyId tBaseCipherId,
	                          const EncryptCipherRandomSalt tSalt,
	                          const EncryptCipherDomainId hDomainId,
	                          const EncryptCipherBaseKeyId hBaseCipherId,
	                          const EncryptCipherRandomSalt hSalt,
	                          const std::string& iv)
	  : textDomainId(tDomainId), textBaseCipherId(tBaseCipherId), textSalt(tSalt), headerDomainId(hDomainId),
	    headerBaseCipherId(hBaseCipherId), headerSalt(hSalt), ivStr(iv) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, textDomainId, textBaseCipherId, textSalt, headerDomainId, headerBaseCipherId, headerSalt, ivStr);
	}
};

struct BlobGranuleCipherKey {
	constexpr static FileIdentifier file_identifier = 7274734;
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId baseCipherId;
	EncryptCipherRandomSalt salt;
	StringRef baseCipher;

	static BlobGranuleCipherKey fromBlobCipherKey(Reference<BlobCipherKey> keyRef, Arena& arena) {
		BlobGranuleCipherKey cipherKey;
		cipherKey.encryptDomainId = keyRef->getDomainId();
		cipherKey.baseCipherId = keyRef->getBaseCipherId();
		cipherKey.salt = keyRef->getSalt();
		cipherKey.baseCipher = makeString(keyRef->getBaseCipherLen(), arena);
		memcpy(mutateString(cipherKey.baseCipher), keyRef->rawBaseCipher(), keyRef->getBaseCipherLen());

		return cipherKey;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, baseCipherId, salt, baseCipher);
	}
};

struct BlobGranuleCipherKeysCtx {
	constexpr static FileIdentifier file_identifier = 1278718;
	BlobGranuleCipherKey textCipherKey;
	BlobGranuleCipherKey headerCipherKey;
	StringRef ivRef;

	static BlobGranuleCipherKeysMeta toCipherKeysMeta(const BlobGranuleCipherKeysCtx& ctx) {
		return BlobGranuleCipherKeysMeta(ctx.textCipherKey.encryptDomainId,
		                                 ctx.textCipherKey.baseCipherId,
		                                 ctx.textCipherKey.salt,
		                                 ctx.headerCipherKey.encryptDomainId,
		                                 ctx.headerCipherKey.baseCipherId,
		                                 ctx.headerCipherKey.salt,
		                                 ctx.ivRef.toString());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, textCipherKey, headerCipherKey, ivRef);
	}
};

struct BlobGranuleFileEncryptionKeys {
	Reference<BlobCipherKey> textCipherKey;
	Reference<BlobCipherKey> headerCipherKey;
};

struct BlobGranuleCipherKeysMetaRef {
	EncryptCipherDomainId textDomainId;
	EncryptCipherBaseKeyId textBaseCipherId;
	EncryptCipherRandomSalt textSalt;
	EncryptCipherDomainId headerDomainId;
	EncryptCipherBaseKeyId headerBaseCipherId;
	EncryptCipherRandomSalt headerSalt;
	StringRef ivRef;

	BlobGranuleCipherKeysMetaRef() {}
	BlobGranuleCipherKeysMetaRef(Arena& to,
	                             const EncryptCipherDomainId tDomainId,
	                             const EncryptCipherBaseKeyId tBaseCipherId,
	                             const EncryptCipherRandomSalt tSalt,
	                             const EncryptCipherDomainId hDomainId,
	                             const EncryptCipherBaseKeyId hBaseCipherId,
	                             const EncryptCipherRandomSalt hSalt,
	                             const std::string& ivStr)
	  : textDomainId(tDomainId), textBaseCipherId(tBaseCipherId), textSalt(tSalt), headerDomainId(hDomainId),
	    headerBaseCipherId(hBaseCipherId), headerSalt(hSalt), ivRef(StringRef(to, ivStr)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, textDomainId, textBaseCipherId, textSalt, headerDomainId, headerBaseCipherId, headerSalt, ivRef);
	}
};

struct BlobFilePointerRef {
	constexpr static FileIdentifier file_identifier = 5253554;
	StringRef filename;
	int64_t offset;
	int64_t length;
	int64_t fullFileLength;
	Optional<BlobGranuleCipherKeysMetaRef> cipherKeysMetaRef;

	BlobFilePointerRef() {}
	BlobFilePointerRef(Arena& to, const std::string& filename, int64_t offset, int64_t length, int64_t fullFileLength)
	  : filename(to, filename), offset(offset), length(length), fullFileLength(fullFileLength) {}

	BlobFilePointerRef(Arena& to,
	                   const std::string& filename,
	                   int64_t offset,
	                   int64_t length,
	                   int64_t fullFileLength,
	                   Optional<BlobGranuleCipherKeysMeta> ciphKeysMeta)
	  : filename(to, filename), offset(offset), length(length), fullFileLength(fullFileLength) {
		if (ciphKeysMeta.present()) {
			cipherKeysMetaRef = BlobGranuleCipherKeysMetaRef(to,
			                                                 ciphKeysMeta.get().textDomainId,
			                                                 ciphKeysMeta.get().textBaseCipherId,
			                                                 ciphKeysMeta.get().textSalt,
			                                                 ciphKeysMeta.get().headerDomainId,
			                                                 ciphKeysMeta.get().headerBaseCipherId,
			                                                 ciphKeysMeta.get().headerSalt,
			                                                 ciphKeysMeta.get().ivStr);
		}
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, filename, offset, length, fullFileLength, cipherKeysMetaRef);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << filename.toString() << ":" << offset << ":" << length << ":" << fullFileLength;
		if (cipherKeysMetaRef.present()) {
			ss << ":CipherKeysMeta:TextCipher:" << cipherKeysMetaRef.get().textDomainId << ":"
			   << cipherKeysMetaRef.get().textBaseCipherId << ":" << cipherKeysMetaRef.get().textSalt
			   << ":HeaderCipher:" << cipherKeysMetaRef.get().headerDomainId << ":"
			   << cipherKeysMetaRef.get().headerBaseCipherId << ":" << cipherKeysMetaRef.get().headerSalt;
		}
		return std::move(ss).str();
	}
};

// the assumption of this response is that the client will deserialize the files
// and apply the mutations themselves
// TODO could filter out delta files that don't intersect the key range being
// requested?
// TODO since client request passes version, we don't need to include the
// version of each mutation in the response if we pruned it there
struct BlobGranuleChunkRef {
	constexpr static FileIdentifier file_identifier = 865198;
	KeyRangeRef keyRange;
	Version includedVersion;
	Version snapshotVersion;
	Optional<BlobFilePointerRef> snapshotFile; // not set if it's an incremental read
	VectorRef<BlobFilePointerRef> deltaFiles;
	GranuleDeltas newDeltas;
	Optional<KeyRef> tenantPrefix;
	Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           keyRange,
		           includedVersion,
		           snapshotVersion,
		           snapshotFile,
		           deltaFiles,
		           newDeltas,
		           tenantPrefix,
		           cipherKeysCtx);
	}
};

enum BlobGranuleSplitState { Unknown = 0, Initialized = 1, Assigned = 2, Done = 3 };

struct BlobGranuleHistoryValue {
	constexpr static FileIdentifier file_identifier = 991434;
	UID granuleID;
	// VectorRef<std::pair<KeyRangeRef, Version>> parentGranules;
	VectorRef<KeyRef> parentBoundaries;
	VectorRef<Version> parentVersions;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, granuleID, parentBoundaries, parentVersions);
	}
};

#endif
