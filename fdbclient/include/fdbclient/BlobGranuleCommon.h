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

#include "fdbclient/BlobCipher.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.actor.h"
#include "flow/EncryptUtils.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"

#include <sstream>

#define BG_ENCRYPT_COMPRESS_DEBUG false

// file format of actual blob files
struct GranuleSnapshot : VectorRef<KeyValueRef> {

	constexpr static FileIdentifier file_identifier = 1300395;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<KeyValueRef>&)*this));
	}
};

// Deltas in version order
struct GranuleDeltas : VectorRef<MutationsAndVersionRef> {
	constexpr static FileIdentifier file_identifier = 8563013;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ((VectorRef<MutationsAndVersionRef>&)*this));
	}
};

#pragma pack(push, 4)
struct GranuleMutationRef {
	MutationRef::Type type;
	Version version;
	StringRef param1;
	StringRef param2;

	GranuleMutationRef() {}
	GranuleMutationRef(MutationRef::Type t, Version v, StringRef param1, StringRef param2)
	  : type(t), version(v), param1(param1), param2(param2) {}
	GranuleMutationRef(Arena& to, MutationRef::Type t, Version v, StringRef param1, StringRef param2)
	  : type(t), version(v), param1(to, param1), param2(to, param2) {}
	GranuleMutationRef(Arena& to, const GranuleMutationRef& from)
	  : type(from.type), version(from.version), param1(to, from.param1), param2(to, from.param2) {}
};
#pragma pack(pop)

struct GranuleMaterializeStats {
	// file-level stats
	int64_t inputBytes;
	int64_t outputBytes;

	// merge stats
	int32_t snapshotRows;
	int32_t rowsCleared;
	int32_t rowsInserted;
	int32_t rowsUpdated;

	GranuleMaterializeStats()
	  : inputBytes(0), outputBytes(0), snapshotRows(0), rowsCleared(0), rowsInserted(0), rowsUpdated(0) {}
};

struct BlobGranuleCipherKeysMeta {
	EncryptCipherDomainId textDomainId;
	EncryptCipherBaseKeyId textBaseCipherId;
	EncryptCipherKeyCheckValue textBaseCipherKCV;
	EncryptCipherRandomSalt textSalt;
	EncryptCipherDomainId headerDomainId;
	EncryptCipherBaseKeyId headerBaseCipherId;
	EncryptCipherKeyCheckValue headerBaseCipherKCV;
	EncryptCipherRandomSalt headerSalt;
	std::string ivStr;

	BlobGranuleCipherKeysMeta() {}
	BlobGranuleCipherKeysMeta(const EncryptCipherDomainId tDomainId,
	                          const EncryptCipherBaseKeyId tBaseCipherId,
	                          const EncryptCipherKeyCheckValue tBaseCipherKCV,
	                          const EncryptCipherRandomSalt tSalt,
	                          const EncryptCipherDomainId hDomainId,
	                          const EncryptCipherBaseKeyId hBaseCipherId,
	                          const EncryptCipherKeyCheckValue hBaseCipherKCV,
	                          const EncryptCipherRandomSalt hSalt,
	                          const std::string& iv)
	  : textDomainId(tDomainId), textBaseCipherId(tBaseCipherId), textBaseCipherKCV(tBaseCipherKCV), textSalt(tSalt),
	    headerDomainId(hDomainId), headerBaseCipherId(hBaseCipherId), headerBaseCipherKCV(hBaseCipherKCV),
	    headerSalt(hSalt), ivStr(iv) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar,
		           textDomainId,
		           textBaseCipherId,
		           textBaseCipherKCV,
		           textSalt,
		           headerDomainId,
		           headerBaseCipherId,
		           headerBaseCipherKCV,
		           headerSalt,
		           ivStr);
	}
};

// When updating this struct with new fields, you must update and add new api versioning for corresponding struct in
// fdb_c.h!
struct BlobGranuleCipherKey {
	constexpr static FileIdentifier file_identifier = 7274734;
	EncryptCipherDomainId encryptDomainId;
	EncryptCipherBaseKeyId baseCipherId;
	EncryptCipherKeyCheckValue baseCipherKCV;
	EncryptCipherRandomSalt salt;
	StringRef baseCipher;

	static BlobGranuleCipherKey fromBlobCipherKey(Reference<BlobCipherKey> keyRef, Arena& arena) {
		BlobGranuleCipherKey cipherKey;
		cipherKey.encryptDomainId = keyRef->getDomainId();
		cipherKey.baseCipherId = keyRef->getBaseCipherId();
		cipherKey.baseCipherKCV = keyRef->getBaseCipherKCV();
		cipherKey.salt = keyRef->getSalt();
		cipherKey.baseCipher = makeString(keyRef->getBaseCipherLen(), arena);
		memcpy(mutateString(cipherKey.baseCipher), keyRef->rawBaseCipher(), keyRef->getBaseCipherLen());

		return cipherKey;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptDomainId, baseCipherId, baseCipherKCV, salt, baseCipher);
	}
};

// When updating this struct with new fields, you must update and add new api versioning for corresponding struct in
// fdb_c.h!
struct BlobGranuleCipherKeysCtx {
	constexpr static FileIdentifier file_identifier = 1278718;
	BlobGranuleCipherKey textCipherKey;
	BlobGranuleCipherKey headerCipherKey;
	StringRef ivRef;

	static BlobGranuleCipherKeysMeta toCipherKeysMeta(const BlobGranuleCipherKeysCtx& ctx) {
		return BlobGranuleCipherKeysMeta(ctx.textCipherKey.encryptDomainId,
		                                 ctx.textCipherKey.baseCipherId,
		                                 ctx.textCipherKey.baseCipherKCV,
		                                 ctx.textCipherKey.salt,
		                                 ctx.headerCipherKey.encryptDomainId,
		                                 ctx.headerCipherKey.baseCipherId,
		                                 ctx.headerCipherKey.baseCipherKCV,
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
	BlobGranuleCipherKeysMetaRef(Arena& to, BlobGranuleCipherKeysMeta cipherKeysMeta)
	  : textDomainId(cipherKeysMeta.textDomainId), textBaseCipherId(cipherKeysMeta.textBaseCipherId),
	    textSalt(cipherKeysMeta.textSalt), headerDomainId(cipherKeysMeta.headerDomainId),
	    headerBaseCipherId(cipherKeysMeta.headerBaseCipherId), headerSalt(cipherKeysMeta.headerSalt),
	    ivRef(StringRef(to, cipherKeysMeta.ivStr)) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, textDomainId, textBaseCipherId, textSalt, headerDomainId, headerBaseCipherId, headerSalt, ivRef);
	}
};

struct BlobFilePointerRef {
	constexpr static FileIdentifier file_identifier = 5253554;
	// Serializable fields
	StringRef filename;
	int64_t offset;
	int64_t length;
	int64_t fullFileLength;
	Version fileVersion;
	Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx;

	// Non-serializable fields
	Optional<BlobGranuleCipherKeysMetaRef>
	    cipherKeysMetaRef; // Placeholder to cache information sufficient to lookup encryption ciphers

	BlobFilePointerRef() {}

	BlobFilePointerRef(Arena& to,
	                   const std::string& filename,
	                   int64_t offset,
	                   int64_t length,
	                   int64_t fullFileLength,
	                   Version fileVersion)
	  : filename(to, filename), offset(offset), length(length), fullFileLength(fullFileLength),
	    fileVersion(fileVersion) {}

	BlobFilePointerRef(Arena& to,
	                   const std::string& filename,
	                   int64_t offset,
	                   int64_t length,
	                   int64_t fullFileLength,
	                   Version fileVersion,
	                   Optional<BlobGranuleCipherKeysCtx> ciphKeysCtx)
	  : filename(to, filename), offset(offset), length(length), fullFileLength(fullFileLength),
	    fileVersion(fileVersion), cipherKeysCtx(ciphKeysCtx) {}

	BlobFilePointerRef(Arena& to,
	                   const std::string& filename,
	                   int64_t offset,
	                   int64_t length,
	                   int64_t fullFileLength,
	                   Version fileVersion,
	                   Optional<BlobGranuleCipherKeysMeta> ciphKeysMeta)
	  : filename(to, filename), offset(offset), length(length), fullFileLength(fullFileLength),
	    fileVersion(fileVersion) {
		if (ciphKeysMeta.present()) {
			cipherKeysMetaRef = BlobGranuleCipherKeysMetaRef(to, ciphKeysMeta.get());
		}
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, filename, offset, length, fullFileLength, fileVersion, cipherKeysCtx);
	}

	std::string toString() const {
		std::stringstream ss;
		ss << filename.toString() << ":" << offset << ":" << length << ":" << fullFileLength << "@" << fileVersion;
		if (cipherKeysCtx.present()) {
			ss << ":CipherKeysCtx:TextCipher:" << cipherKeysCtx.get().textCipherKey.encryptDomainId << ":"
			   << cipherKeysCtx.get().textCipherKey.baseCipherId << ":" << cipherKeysCtx.get().textCipherKey.salt
			   << ":HeaderCipher:" << cipherKeysCtx.get().headerCipherKey.encryptDomainId << ":"
			   << cipherKeysCtx.get().headerCipherKey.baseCipherId << ":" << cipherKeysCtx.get().headerCipherKey.salt;
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
	// FIXME: remove snapshotVersion, it is deprecated with fileVersion in BlobFilePointerRef
	Version snapshotVersion;
	Optional<BlobFilePointerRef> snapshotFile; // not set if it's an incremental read
	VectorRef<BlobFilePointerRef> deltaFiles;
	GranuleDeltas newDeltas;
	Optional<KeyRef> tenantPrefix;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, includedVersion, snapshotVersion, snapshotFile, deltaFiles, newDeltas, tenantPrefix);
	}
};

struct BlobGranuleSummaryRef {
	constexpr static FileIdentifier file_identifier = 9774587;
	KeyRangeRef keyRange;
	Version snapshotVersion;
	int64_t snapshotSize;
	Version deltaVersion;
	int64_t deltaSize;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, keyRange, snapshotVersion, snapshotSize, deltaVersion, deltaSize);
	}
};

BlobGranuleSummaryRef summarizeGranuleChunk(Arena& ar, const BlobGranuleChunkRef& chunk);

enum BlobGranuleSplitState { Unknown = 0, Initialized = 1, Assigned = 2, Done = 3 };

// Boundary metadata for each range indexed by the beginning of the range.
struct BlobGranuleMergeBoundary {
	constexpr static FileIdentifier file_identifier = 557861;

	// Hard boundaries represent backing regions we want to keep separate.
	bool buddy;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, buddy);
	}
};

struct BlobGranuleHistoryValue {
	constexpr static FileIdentifier file_identifier = 991434;
	UID granuleID;
	VectorRef<KeyRef> parentBoundaries;
	VectorRef<Version> parentVersions;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, granuleID, parentBoundaries, parentVersions);
	}
};

struct GranuleHistory {
	KeyRange range;
	Version version;
	Standalone<BlobGranuleHistoryValue> value;

	GranuleHistory() {}

	GranuleHistory(KeyRange range, Version version, Standalone<BlobGranuleHistoryValue> value)
	  : range(range), version(version), value(value) {}
};

// A manifest to assist full fdb restore from blob granule files
struct BlobManifestTailer {
	constexpr static FileIdentifier file_identifier = 379431;
	int64_t totalRows;
	int64_t totalSegments;
	int64_t totalBytes;
	// All manifest files are currently encrypted using default_domain
	// and with a single encryption key.
	// TODO: Extend domain_aware encryption semantics to manifest file(s)
	Optional<BlobGranuleCipherKeysMeta> cipherKeysMeta;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, totalRows, totalSegments, totalBytes, cipherKeysMeta);
	}
};

// Value of blob range change log.
struct BlobRangeChangeLogRef {
	constexpr static FileIdentifier file_identifier = 9774587;

	KeyRangeRef range;
	ValueRef value;

	BlobRangeChangeLogRef() {}
	BlobRangeChangeLogRef(KeyRangeRef range, ValueRef value) : range(range), value(value) {}
	BlobRangeChangeLogRef(Arena& to, KeyRangeRef range, ValueRef value) : range(to, range), value(to, value) {}
	BlobRangeChangeLogRef(Arena& to, const BlobRangeChangeLogRef& from)
	  : range(to, from.range), value(to, from.value) {}

	int expectedSize() const { return range.expectedSize() + value.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, range, value);
	}
};

#endif
