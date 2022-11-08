/*
 * BlobGranuleFiles.cpp
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

#include "fdbclient/BlobGranuleFiles.h"

#include "fdbclient/BlobCipher.h"
#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h" // for allKeys unit test - could remove

#include "flow/Arena.h"
#include "flow/CompressionUtils.h"
#include "flow/DeterministicRandom.h"
#include "flow/EncryptUtils.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include "flow/UnitTest.h"
#include "flow/xxhash.h"

#include "fmt/format.h"

#include <cstring>
#include <fstream> // for perf microbenchmark
#include <limits>
#include <vector>

#define BG_READ_DEBUG false
#define BG_FILES_TEST_DEBUG false

// Implements granule file parsing and materialization with normal c++ functions (non-actors) so that this can be used
// outside the FDB network thread.

// File Format stuff

// Version info for file format of chunked files.
uint16_t LATEST_BG_FORMAT_VERSION = 1;
uint16_t MIN_SUPPORTED_BG_FORMAT_VERSION = 1;

// TODO combine with SystemData? These don't actually have to match though

const uint8_t SNAPSHOT_FILE_TYPE = 'S';
const uint8_t DELTA_FILE_TYPE = 'D';

// Deltas in key order

// For key-ordered delta files, the format for both sets and range clears is that you store boundaries ordered by key.
// Each boundary has a corresponding key, zero or more versioned updates (ValueAndVersionRef), and optionally a clear
// from keyAfter(key) to the next boundary, at a version.
// A streaming merge is more efficient than applying deltas one by one to restore to a later version from the snapshot.
// The concept of this versioned mutation boundaries is repurposed directly from a prior version of redwood, back when
// it supported versioned data.
struct ValueAndVersionRef {
	Version version;
	MutationRef::Type op; // only set/clear
	ValueRef value; // only present for set

	ValueAndVersionRef() {}
	// create clear
	explicit ValueAndVersionRef(Version version) : version(version), op(MutationRef::Type::ClearRange) {}
	// create set
	explicit ValueAndVersionRef(Version version, ValueRef value)
	  : version(version), op(MutationRef::Type::SetValue), value(value) {}
	ValueAndVersionRef(Arena& arena, const ValueAndVersionRef& copyFrom)
	  : version(copyFrom.version), op(copyFrom.op), value(arena, copyFrom.value) {}

	bool isSet() const { return op == MutationRef::SetValue; }
	bool isClear() const { return op == MutationRef::ClearRange; }

	int totalSize() const { return sizeof(ValueAndVersionRef) + value.size(); }
	int expectedSize() const { return value.size(); }

	struct OrderByVersion {
		bool operator()(ValueAndVersionRef const& a, ValueAndVersionRef const& b) const {
			return a.version < b.version;
		}
	};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, op, value);
	}
};

// Effectively the single DeltaBoundaryRef reduced to one update, but also with the key and clear after information.
// Sometimes at a given version, the boundary may only be necessary to represent a clear version after this key, or just
// an update/clear to this key, or both.
struct ParsedDeltaBoundaryRef {
	KeyRef key;
	MutationRef::Type op; // SetValue, ClearRange, or NoOp
	ValueRef value; // null unless op == SetValue
	bool clearAfter;

	// op constructor
	ParsedDeltaBoundaryRef() {}
	explicit ParsedDeltaBoundaryRef(KeyRef key, bool clearAfter, const ValueAndVersionRef& valueAndVersion)
	  : key(key), op(valueAndVersion.op), value(valueAndVersion.value), clearAfter(clearAfter) {}
	// noop constructor
	explicit ParsedDeltaBoundaryRef(KeyRef key, bool clearAfter)
	  : key(key), op(MutationRef::Type::NoOp), clearAfter(clearAfter) {}
	// from snapshot set constructor
	explicit ParsedDeltaBoundaryRef(const KeyValueRef& kv)
	  : key(kv.key), op(MutationRef::Type::SetValue), value(kv.value), clearAfter(false) {}

	ParsedDeltaBoundaryRef(Arena& arena, const ParsedDeltaBoundaryRef& copyFrom)
	  : key(arena, copyFrom.key), op(copyFrom.op), clearAfter(copyFrom.clearAfter) {
		if (copyFrom.isSet()) {
			value = StringRef(arena, copyFrom.value);
		}
	}

	bool isSet() const { return op == MutationRef::SetValue; }
	bool isClear() const { return op == MutationRef::ClearRange; }
	bool isNoOp() const { return op == MutationRef::NoOp; }
	bool redundant(bool prevClearAfter) const { return op == MutationRef::Type::NoOp && clearAfter == prevClearAfter; }
};

struct DeltaBoundaryRef {
	// key
	KeyRef key;
	// updates to exactly this key
	VectorRef<ValueAndVersionRef> values;
	// clear version from keyAfter(key) up to the next boundary
	Optional<Version> clearVersion;

	DeltaBoundaryRef() {}
	DeltaBoundaryRef(Arena& ar, const DeltaBoundaryRef& copyFrom)
	  : key(ar, copyFrom.key), values(ar, copyFrom.values), clearVersion(copyFrom.clearVersion) {}

	int totalSize() { return sizeof(DeltaBoundaryRef) + key.expectedSize() + values.expectedSize(); }
	int expectedSize() const { return key.expectedSize() + values.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, values, clearVersion);
	}
};

struct GranuleSortedDeltas {
	constexpr static FileIdentifier file_identifier = 8183903;

	VectorRef<DeltaBoundaryRef> boundaries;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, boundaries);
	}
};

struct ChildBlockPointerRef {
	StringRef key;
	uint32_t offset;

	ChildBlockPointerRef() {}
	explicit ChildBlockPointerRef(StringRef key, uint32_t offset) : key(key), offset(offset) {}
	explicit ChildBlockPointerRef(Arena& arena, StringRef key, uint32_t offset) : key(arena, key), offset(offset) {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, offset);
	}

	struct OrderByKey {
		bool operator()(ChildBlockPointerRef const& a, ChildBlockPointerRef const& b) const { return a.key < b.key; }
	};

	struct OrderByKeyCommonPrefix {
		int prefixLen;
		OrderByKeyCommonPrefix(int prefixLen) : prefixLen(prefixLen) {}
		bool operator()(ChildBlockPointerRef const& a, ChildBlockPointerRef const& b) const {
			return a.key.compareSuffix(b.key, prefixLen);
		}
	};
};

namespace {
BlobGranuleFileEncryptionKeys getEncryptBlobCipherKey(const BlobGranuleCipherKeysCtx cipherKeysCtx) {
	BlobGranuleFileEncryptionKeys eKeys;

	// Cipher key reconstructed is 'never' inserted into BlobCipherKey cache, choose 'neverExpire'
	eKeys.textCipherKey = makeReference<BlobCipherKey>(cipherKeysCtx.textCipherKey.encryptDomainId,
	                                                   cipherKeysCtx.textCipherKey.baseCipherId,
	                                                   cipherKeysCtx.textCipherKey.baseCipher.begin(),
	                                                   cipherKeysCtx.textCipherKey.baseCipher.size(),
	                                                   cipherKeysCtx.textCipherKey.salt,
	                                                   std::numeric_limits<int64_t>::max(),
	                                                   std::numeric_limits<int64_t>::max());
	eKeys.headerCipherKey = makeReference<BlobCipherKey>(cipherKeysCtx.headerCipherKey.encryptDomainId,
	                                                     cipherKeysCtx.headerCipherKey.baseCipherId,
	                                                     cipherKeysCtx.headerCipherKey.baseCipher.begin(),
	                                                     cipherKeysCtx.headerCipherKey.baseCipher.size(),
	                                                     cipherKeysCtx.headerCipherKey.salt,
	                                                     std::numeric_limits<int64_t>::max(),
	                                                     std::numeric_limits<int64_t>::max());

	return eKeys;
}

void validateEncryptionHeaderDetails(const BlobGranuleFileEncryptionKeys& eKeys,
                                     const BlobCipherEncryptHeader& header,
                                     const StringRef& ivRef) {
	// Validate encryption header 'cipherHeader' details sanity
	if (!(header.cipherHeaderDetails.baseCipherId == eKeys.headerCipherKey->getBaseCipherId() &&
	      header.cipherHeaderDetails.encryptDomainId == eKeys.headerCipherKey->getDomainId() &&
	      header.cipherHeaderDetails.salt == eKeys.headerCipherKey->getSalt())) {
		TraceEvent(SevError, "EncryptionHeader_CipherHeaderMismatch")
		    .detail("HeaderDomainId", eKeys.headerCipherKey->getDomainId())
		    .detail("ExpectedHeaderDomainId", header.cipherHeaderDetails.encryptDomainId)
		    .detail("HeaderBaseCipherId", eKeys.headerCipherKey->getBaseCipherId())
		    .detail("ExpectedHeaderBaseCipherId", header.cipherHeaderDetails.baseCipherId)
		    .detail("HeaderSalt", eKeys.headerCipherKey->getSalt())
		    .detail("ExpectedHeaderSalt", header.cipherHeaderDetails.salt);
		throw encrypt_header_metadata_mismatch();
	}
	// Validate encryption header 'cipherText' details sanity
	if (!(header.cipherTextDetails.baseCipherId == eKeys.textCipherKey->getBaseCipherId() &&
	      header.cipherTextDetails.encryptDomainId == eKeys.textCipherKey->getDomainId() &&
	      header.cipherTextDetails.salt == eKeys.textCipherKey->getSalt())) {
		TraceEvent(SevError, "EncryptionHeader_CipherTextMismatch")
		    .detail("TextDomainId", eKeys.textCipherKey->getDomainId())
		    .detail("ExpectedTextDomainId", header.cipherTextDetails.encryptDomainId)
		    .detail("TextBaseCipherId", eKeys.textCipherKey->getBaseCipherId())
		    .detail("ExpectedTextBaseCipherId", header.cipherTextDetails.baseCipherId)
		    .detail("TextSalt", eKeys.textCipherKey->getSalt())
		    .detail("ExpectedTextSalt", header.cipherTextDetails.salt);
		throw encrypt_header_metadata_mismatch();
	}
	// Validate 'Initialization Vector' sanity
	if (memcmp(ivRef.begin(), &header.iv[0], AES_256_IV_LENGTH) != 0) {
		TraceEvent(SevError, "EncryptionHeader_IVMismatch")
		    .detail("IVChecksum", XXH3_64bits(ivRef.begin(), ivRef.size()))
		    .detail("ExpectedIVChecksum", XXH3_64bits(&header.iv[0], AES_256_IV_LENGTH));
		throw encrypt_header_metadata_mismatch();
	}
}
} // namespace

struct IndexBlock {
	constexpr static FileIdentifier file_identifier = 6525412;

	// Serializable fields
	VectorRef<ChildBlockPointerRef> children;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, children);
	}
};

struct IndexBlockRef {
	constexpr static FileIdentifier file_identifier = 1945731;

	// Serialized fields
	Optional<StringRef> encryptHeaderRef;
	// Encrypted/unencrypted IndexBlock
	StringRef buffer;

	// Non-serializable fields
	IndexBlock block;

	void encrypt(const BlobGranuleCipherKeysCtx cipherKeysCtx, Arena& arena) {
		BlobGranuleFileEncryptionKeys eKeys = getEncryptBlobCipherKey(cipherKeysCtx);
		ASSERT(eKeys.headerCipherKey.isValid() && eKeys.textCipherKey.isValid());

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(buffer.begin(), buffer.size());
			TraceEvent(SevDebug, "IndexBlockEncrypt_Before").detail("Chksum", chksum);
		}

		EncryptBlobCipherAes265Ctr encryptor(
		    eKeys.textCipherKey,
		    eKeys.headerCipherKey,
		    cipherKeysCtx.ivRef.begin(),
		    AES_256_IV_LENGTH,
		    getEncryptAuthTokenMode(EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE),
		    BlobCipherMetrics::BLOB_GRANULE);
		Value serializedBuff = ObjectWriter::toValue(block, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
		BlobCipherEncryptHeader header;
		buffer = encryptor.encrypt(serializedBuff.contents().begin(), serializedBuff.contents().size(), &header, arena)
		             ->toStringRef();
		encryptHeaderRef = BlobCipherEncryptHeader::toStringRef(header, arena);

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(buffer.begin(), buffer.size());
			TraceEvent(SevDebug, "IndexBlockEncrypt_After").detail("Chksum", chksum);
		}
	}

	static void decrypt(const BlobGranuleCipherKeysCtx cipherKeysCtx, IndexBlockRef& idxRef, Arena& arena) {
		BlobGranuleFileEncryptionKeys eKeys = getEncryptBlobCipherKey(cipherKeysCtx);

		ASSERT(eKeys.headerCipherKey.isValid() && eKeys.textCipherKey.isValid());
		ASSERT(idxRef.encryptHeaderRef.present());

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(idxRef.buffer.begin(), idxRef.buffer.size());
			TraceEvent(SevDebug, "IndexBlockEncrypt_Before").detail("Chksum", chksum);
		}

		BlobCipherEncryptHeader header = BlobCipherEncryptHeader::fromStringRef(idxRef.encryptHeaderRef.get());

		validateEncryptionHeaderDetails(eKeys, header, cipherKeysCtx.ivRef);

		DecryptBlobCipherAes256Ctr decryptor(
		    eKeys.textCipherKey, eKeys.headerCipherKey, cipherKeysCtx.ivRef.begin(), BlobCipherMetrics::BLOB_GRANULE);
		StringRef decrypted =
		    decryptor.decrypt(idxRef.buffer.begin(), idxRef.buffer.size(), header, arena)->toStringRef();

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(decrypted.begin(), decrypted.size());
			TraceEvent(SevDebug, "IndexBlockEncrypt_After").detail("Chksum", chksum);
		}

		ObjectReader dataReader(decrypted.begin(), IncludeVersion());
		dataReader.deserialize(FileIdentifierFor<IndexBlock>::value, idxRef.block, arena);
	}

	void init(Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx, Arena& arena) {
		if (encryptHeaderRef.present()) {
			CODE_PROBE(true, "reading encrypted chunked file");
			ASSERT(cipherKeysCtx.present());

			decrypt(cipherKeysCtx.get(), *this, arena);
		} else {
			if (BG_ENCRYPT_COMPRESS_DEBUG) {
				TraceEvent("IndexBlockSize").detail("Sz", buffer.size());
			}

			ObjectReader dataReader(buffer.begin(), IncludeVersion());
			dataReader.deserialize(FileIdentifierFor<IndexBlock>::value, block, arena);
		}
	}

	void finalize(Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx, Arena& arena) {
		if (cipherKeysCtx.present()) {
			// IndexBlock childBlock pointers offsets are relative to IndexBlock endOffset instead of file start offset.
			// Compressing indexBlock will need offset recalculation (circular depedency). IndexBlock size is bounded by
			// number of chunks and sizeof(KeyPrefix), 'not' compressing IndexBlock shouldn't cause significant file
			// size bloat.

			ASSERT(cipherKeysCtx.present());
			encrypt(cipherKeysCtx.get(), arena);
		} else {
			encryptHeaderRef.reset();
			buffer = StringRef(
			    arena, ObjectWriter::toValue(block, IncludeVersion(ProtocolVersion::withBlobGranuleFile())).contents());
		}

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			TraceEvent(SevDebug, "IndexBlockSize")
			    .detail("Sz", buffer.size())
			    .detail("Encrypted", cipherKeysCtx.present());
		}
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, encryptHeaderRef, buffer);
	}
};

// On-disk and/or in-memory representation of a IndexBlobGranuleFile 'chunk'.
//
// Encryption: A 'chunk' gets encrypted before getting persisted if enabled. Encryption header is persisted along with
// the chunk data to assist decryption on reads.
//
// Compression: A 'chunk' gets compressed before getting persisted if enabled. Compression filter (algorithm)
// information is persisted as part of 'chunk metadata' to assist decompression on reads.

struct IndexBlobGranuleFileChunkRef {
	constexpr static FileIdentifier file_identifier = 2814019;

	// Serialized fields
	Optional<CompressionFilter> compressionFilter;
	Optional<StringRef> encryptHeaderRef;
	// encrypted and/or compressed chunk;
	StringRef buffer;

	// Non-serialized
	Optional<StringRef> chunkBytes;

	static void encrypt(const BlobGranuleCipherKeysCtx& cipherKeysCtx,
	                    IndexBlobGranuleFileChunkRef& chunkRef,
	                    Arena& arena) {
		BlobGranuleFileEncryptionKeys eKeys = getEncryptBlobCipherKey(cipherKeysCtx);

		ASSERT(eKeys.headerCipherKey.isValid() && eKeys.textCipherKey.isValid());

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(chunkRef.buffer.begin(), chunkRef.buffer.size());
			TraceEvent(SevDebug, "BlobChunkEncrypt_Before").detail("Chksum", chksum);
		}

		EncryptBlobCipherAes265Ctr encryptor(
		    eKeys.textCipherKey,
		    eKeys.headerCipherKey,
		    cipherKeysCtx.ivRef.begin(),
		    AES_256_IV_LENGTH,
		    getEncryptAuthTokenMode(EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE),
		    BlobCipherMetrics::BLOB_GRANULE);
		BlobCipherEncryptHeader header;
		chunkRef.buffer =
		    encryptor.encrypt(chunkRef.buffer.begin(), chunkRef.buffer.size(), &header, arena)->toStringRef();
		chunkRef.encryptHeaderRef = BlobCipherEncryptHeader::toStringRef(header, arena);

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(chunkRef.buffer.begin(), chunkRef.buffer.size());
			TraceEvent(SevDebug, "BlobChunkEncrypt_After").detail("Chksum", chksum);
		}
	}

	static StringRef decrypt(const BlobGranuleCipherKeysCtx& cipherKeysCtx,
	                         const IndexBlobGranuleFileChunkRef& chunkRef,
	                         Arena& arena) {
		BlobGranuleFileEncryptionKeys eKeys = getEncryptBlobCipherKey(cipherKeysCtx);

		ASSERT(eKeys.headerCipherKey.isValid() && eKeys.textCipherKey.isValid());
		ASSERT(chunkRef.encryptHeaderRef.present());

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(chunkRef.buffer.begin(), chunkRef.buffer.size());
			TraceEvent(SevDebug, "BlobChunkDecrypt_Before").detail("Chksum", chksum);
		}

		BlobCipherEncryptHeader header = BlobCipherEncryptHeader::fromStringRef(chunkRef.encryptHeaderRef.get());

		validateEncryptionHeaderDetails(eKeys, header, cipherKeysCtx.ivRef);

		DecryptBlobCipherAes256Ctr decryptor(
		    eKeys.textCipherKey, eKeys.headerCipherKey, cipherKeysCtx.ivRef.begin(), BlobCipherMetrics::BLOB_GRANULE);
		StringRef decrypted =
		    decryptor.decrypt(chunkRef.buffer.begin(), chunkRef.buffer.size(), header, arena)->toStringRef();

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(decrypted.begin(), decrypted.size());
			TraceEvent(SevDebug, "BlobChunkDecrypt_After").detail("Chksum", chksum);
		}

		return decrypted;
	}

	static void compress(IndexBlobGranuleFileChunkRef& chunkRef,
	                     const Value& chunk,
	                     const CompressionFilter compFilter,
	                     Arena& arena) {
		chunkRef.compressionFilter = compFilter;
		chunkRef.buffer = CompressionUtils::compress(chunkRef.compressionFilter.get(),
		                                             chunk.contents(),
		                                             CompressionUtils::getDefaultCompressionLevel(compFilter),
		                                             arena);

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chunkChksum = XXH3_64bits(chunk.contents().begin(), chunk.contents().size());
			XXH64_hash_t chksum = XXH3_64bits(chunkRef.buffer.begin(), chunkRef.buffer.size());
			TraceEvent("CompressBlobChunk")
			    .detail("Filter", CompressionUtils::toString(chunkRef.compressionFilter.get()))
			    .detail("ChkSumBefore", chunkChksum)
			    .detail("ChkSumAfter", chksum);
		}
	}

	static StringRef decompress(const IndexBlobGranuleFileChunkRef& chunkRef, Arena& arena) {
		ASSERT(chunkRef.compressionFilter.present());
		return CompressionUtils::decompress(chunkRef.compressionFilter.get(), chunkRef.chunkBytes.get(), arena);
	}

	static Value toBytes(Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
	                     Optional<CompressionFilter> compFilter,
	                     const Value& chunk,
	                     Arena& arena) {
		IndexBlobGranuleFileChunkRef chunkRef;

		if (compFilter.present()) {
			IndexBlobGranuleFileChunkRef::compress(chunkRef, chunk, compFilter.get(), arena);
		} else {
			chunkRef.buffer = StringRef(arena, chunk.contents());
		}

		if (cipherKeysCtx.present()) {
			IndexBlobGranuleFileChunkRef::encrypt(cipherKeysCtx.get(), chunkRef, arena);
		}

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			TraceEvent(SevDebug, "GenerateBlobGranuleFileChunk")
			    .detail("Encrypt", cipherKeysCtx.present())
			    .detail("Compress", compFilter.present())
			    .detail("CompFilter",
			            compFilter.present() ? CompressionUtils::toString(compFilter.get())
			                                 : CompressionUtils::toString(CompressionFilter::NONE));
		}

		return ObjectWriter::toValue(chunkRef, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
	}

	static IndexBlobGranuleFileChunkRef fromBytes(Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
	                                              StringRef buffer,
	                                              Arena& arena) {
		IndexBlobGranuleFileChunkRef chunkRef;
		ObjectReader dataReader(buffer.begin(), IncludeVersion());
		dataReader.deserialize(FileIdentifierFor<IndexBlobGranuleFileChunkRef>::value, chunkRef, arena);

		if (chunkRef.encryptHeaderRef.present()) {
			CODE_PROBE(true, "reading encrypted file chunk");
			ASSERT(cipherKeysCtx.present());
			chunkRef.chunkBytes = IndexBlobGranuleFileChunkRef::decrypt(cipherKeysCtx.get(), chunkRef, arena);
		} else {
			chunkRef.chunkBytes = chunkRef.buffer;
		}

		if (chunkRef.compressionFilter.present()) {
			CODE_PROBE(true, "reading compressed file chunk");
			chunkRef.chunkBytes = IndexBlobGranuleFileChunkRef::decompress(chunkRef, arena);
		} else if (!chunkRef.chunkBytes.present()) {
			// 'Encryption' & 'Compression' aren't enabled.
			chunkRef.chunkBytes = chunkRef.buffer;
		}

		ASSERT(chunkRef.chunkBytes.present());

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			TraceEvent(SevDebug, "ParseBlobGranuleFileChunk")
			    .detail("Encrypted", chunkRef.encryptHeaderRef.present())
			    .detail("Compressed", chunkRef.compressionFilter.present())
			    .detail("CompFilter",
			            chunkRef.compressionFilter.present()
			                ? CompressionUtils::toString(chunkRef.compressionFilter.get())
			                : CompressionUtils::toString(CompressionFilter::NONE));
		}

		return chunkRef;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, compressionFilter, encryptHeaderRef, buffer);
	}
};

/*
 * A file header for a key-ordered file that is chunked on disk, where each chunk is a disjoint key range of data.
 */
struct IndexedBlobGranuleFile {
	constexpr static FileIdentifier file_identifier = 3828201;
	// serialized fields
	uint16_t formatVersion;
	uint8_t fileType;
	Optional<StringRef> filter; // not used currently

	IndexBlockRef indexBlockRef;
	int chunkStartOffset;

	// Non-serialized member fields
	StringRef fileBytes;

	void init(uint8_t fType, const Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
		formatVersion = LATEST_BG_FORMAT_VERSION;
		fileType = fType;
		chunkStartOffset = -1;
	}

	void init(const StringRef& fBytes, Arena& arena, const Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
		ASSERT(chunkStartOffset > 0);

		fileBytes = fBytes;
		indexBlockRef.init(cipherKeysCtx, arena);
	}

	static Standalone<IndexedBlobGranuleFile> fromFileBytes(const StringRef& fileBytes,
	                                                        const Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
		// parse index block at head of file
		Arena arena;
		IndexedBlobGranuleFile file;
		ObjectReader dataReader(fileBytes.begin(), IncludeVersion());
		dataReader.deserialize(FileIdentifierFor<IndexedBlobGranuleFile>::value, file, arena);

		file.init(fileBytes, arena, cipherKeysCtx);

		// do sanity checks
		if (file.formatVersion > LATEST_BG_FORMAT_VERSION || file.formatVersion < MIN_SUPPORTED_BG_FORMAT_VERSION) {
			TraceEvent(SevWarn, "BlobGranuleFileInvalidFormatVersion")
			    .suppressFor(5.0)
			    .detail("FoundFormatVersion", file.formatVersion)
			    .detail("MinSupported", MIN_SUPPORTED_BG_FORMAT_VERSION)
			    .detail("LatestSupported", LATEST_BG_FORMAT_VERSION);
			throw unsupported_format_version();
		}
		ASSERT(file.fileType == SNAPSHOT_FILE_TYPE || file.fileType == DELTA_FILE_TYPE);

		return Standalone<IndexedBlobGranuleFile>(file, arena);
	}

	ChildBlockPointerRef* findStartBlock(const KeyRef& beginKey) const {
		ChildBlockPointerRef searchKey(beginKey, 0);
		ChildBlockPointerRef* startBlock = (ChildBlockPointerRef*)std::lower_bound(indexBlockRef.block.children.begin(),
		                                                                           indexBlockRef.block.children.end(),
		                                                                           searchKey,
		                                                                           ChildBlockPointerRef::OrderByKey());

		if (startBlock != indexBlockRef.block.children.end() && startBlock != indexBlockRef.block.children.begin() &&
		    beginKey < startBlock->key) {
			startBlock--;
		} else if (startBlock == indexBlockRef.block.children.end()) {
			startBlock--;
		}

		return startBlock;
	}

	// FIXME: implement some sort of iterator type interface?
	template <class ChildType>
	Standalone<ChildType> getChild(const ChildBlockPointerRef* childPointer,
	                               Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
	                               int startOffset) {
		ASSERT(childPointer != indexBlockRef.block.children.end());
		const ChildBlockPointerRef* nextPointer = childPointer + 1;
		ASSERT(nextPointer != indexBlockRef.block.children.end());

		size_t blockSize = nextPointer->offset - childPointer->offset;
		// Account for IndexBlockRef size for chunk offset computation
		StringRef childData(fileBytes.begin() + childPointer->offset + startOffset, blockSize);

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			TraceEvent(SevDebug, "GetChild")
			    .detail("BlkSize", blockSize)
			    .detail("Offset", childPointer->offset)
			    .detail("StartOffset", chunkStartOffset);
		}

		Arena childArena;
		IndexBlobGranuleFileChunkRef chunkRef =
		    IndexBlobGranuleFileChunkRef::fromBytes(cipherKeysCtx, childData, childArena);

		// TODO implement some sort of decrypted+decompressed+deserialized cache, if this object gets reused?

		BinaryReader br(chunkRef.chunkBytes.get(), IncludeVersion());
		Standalone<ChildType> child;
		br >> child;
		return child;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, formatVersion, fileType, filter, indexBlockRef, chunkStartOffset);
	}
};

// Since ObjectReader doesn't update read offset after reading, we have to make the block offsets absolute offsets by
// serializing once, adding the serialized size to each offset, and serializing again. This relies on the fact that
// ObjectWriter/flatbuffers uses fixed size integers instead of variable size.

Value serializeIndexBlock(Standalone<IndexedBlobGranuleFile>& file, Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	file.indexBlockRef.finalize(cipherKeysCtx, file.arena());

	Value serialized = ObjectWriter::toValue(file, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
	file.chunkStartOffset = serialized.contents().size();

	if (BG_ENCRYPT_COMPRESS_DEBUG) {
		TraceEvent(SevDebug, "SerializeIndexBlock").detail("StartOffset", file.chunkStartOffset);
	}

	return ObjectWriter::toValue(file, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
}

Value serializeFileFromChunks(Standalone<IndexedBlobGranuleFile>& file,
                              Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
                              std::vector<Value>& chunks,
                              int previousChunkBytes) {
	Value indexBlockBytes = serializeIndexBlock(file, cipherKeysCtx);
	int32_t indexSize = indexBlockBytes.size();
	chunks[0] = indexBlockBytes;

	// TODO: write this directly to stream to avoid extra copy?
	Arena ret;

	size_t size = indexSize + previousChunkBytes;
	uint8_t* buffer = new (ret) uint8_t[size];
	uint8_t* bufferStart = buffer;

	int idx = 0;
	for (auto& it : chunks) {
		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			TraceEvent(SevDebug, "SerializeFile")
			    .detail("ChunkIdx", idx++)
			    .detail("Size", it.size())
			    .detail("Offset", buffer - bufferStart);
		}
		buffer = it.copyTo(buffer);
	}
	ASSERT(size == buffer - bufferStart);

	return Standalone<StringRef>(StringRef(bufferStart, size), ret);
}

// TODO: this should probably be in actor file with yields? - move writing logic to separate actor file in server?
// TODO: optimize memory copying
// TODO: sanity check no oversized files
Value serializeChunkedSnapshot(const Standalone<StringRef>& fileNameRef,
                               const Standalone<GranuleSnapshot>& snapshot,
                               int targetChunkBytes,
                               Optional<CompressionFilter> compressFilter,
                               Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {

	if (BG_ENCRYPT_COMPRESS_DEBUG) {
		TraceEvent(SevDebug, "SerializeChunkedSnapshot")
		    .detail("FileName", fileNameRef.toString())
		    .detail("Encrypted", cipherKeysCtx.present())
		    .detail("Compressed", compressFilter.present());
	}

	CODE_PROBE(compressFilter.present(), "serializing compressed snapshot file");
	CODE_PROBE(cipherKeysCtx.present(), "serializing encrypted snapshot file");
	Standalone<IndexedBlobGranuleFile> file;

	file.init(SNAPSHOT_FILE_TYPE, cipherKeysCtx);

	size_t currentChunkBytesEstimate = 0;
	size_t previousChunkBytes = 0;

	std::vector<Value> chunks;
	chunks.push_back(Value()); // dummy value for index block
	Standalone<GranuleSnapshot> currentChunk;

	for (int i = 0; i < snapshot.size(); i++) {
		// TODO REMOVE sanity check
		if (i > 0) {
			ASSERT(snapshot[i - 1].key < snapshot[i].key);
		}

		currentChunk.push_back_deep(currentChunk.arena(), snapshot[i]);
		currentChunkBytesEstimate += snapshot[i].expectedSize();

		if (currentChunkBytesEstimate >= targetChunkBytes || i == snapshot.size() - 1) {
			Value serialized =
			    BinaryWriter::toValue(currentChunk, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
			Value chunkBytes =
			    IndexBlobGranuleFileChunkRef::toBytes(cipherKeysCtx, compressFilter, serialized, file.arena());
			chunks.push_back(chunkBytes);
			// TODO remove validation
			if (!file.indexBlockRef.block.children.empty()) {
				ASSERT(file.indexBlockRef.block.children.back().key < currentChunk.begin()->key);
			}
			file.indexBlockRef.block.children.emplace_back_deep(
			    file.arena(), currentChunk.begin()->key, previousChunkBytes);

			if (BG_ENCRYPT_COMPRESS_DEBUG) {
				TraceEvent(SevDebug, "ChunkSize")
				    .detail("ChunkBytes", chunkBytes.size())
				    .detail("PrvChunkBytes", previousChunkBytes);
			}

			previousChunkBytes += chunkBytes.size();
			currentChunkBytesEstimate = 0;
			currentChunk = Standalone<GranuleSnapshot>();
		}
	}
	ASSERT(currentChunk.empty());
	// push back dummy last chunk to get last chunk size, and to know last key in last block without having to read it
	if (!snapshot.empty()) {
		file.indexBlockRef.block.children.emplace_back_deep(
		    file.arena(), keyAfter(snapshot.back().key), previousChunkBytes);
	}

	return serializeFileFromChunks(file, cipherKeysCtx, chunks, previousChunkBytes);
}

// TODO: use redwood prefix trick to optimize cpu comparison
static Standalone<VectorRef<ParsedDeltaBoundaryRef>> loadSnapshotFile(
    const Standalone<StringRef>& fileName,
    const StringRef& snapshotData,
    const KeyRangeRef& keyRange,
    Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	Standalone<VectorRef<ParsedDeltaBoundaryRef>> results;

	if (BG_ENCRYPT_COMPRESS_DEBUG) {
		TraceEvent(SevDebug, "LoadChunkedSnapshot")
		    .detail("FileName", fileName.toString())
		    .detail("RangeBegin", keyRange.begin.printable())
		    .detail("RangeEnd", keyRange.end.printable())
		    .detail("Encrypted", cipherKeysCtx.present());
	}

	Standalone<IndexedBlobGranuleFile> file = IndexedBlobGranuleFile::fromFileBytes(snapshotData, cipherKeysCtx);

	ASSERT(file.fileType == SNAPSHOT_FILE_TYPE);
	ASSERT(file.chunkStartOffset > 0);

	// empty snapshot file
	if (file.indexBlockRef.block.children.empty()) {
		return results;
	}

	ASSERT(file.indexBlockRef.block.children.size() >= 2);

	// find range of blocks needed to read
	ChildBlockPointerRef* currentBlock = file.findStartBlock(keyRange.begin);

	if (currentBlock == (file.indexBlockRef.block.children.end() - 1) || keyRange.end <= currentBlock->key) {
		return results;
	}

	bool lastBlock = false;

	// FIXME: shared prefix for key comparison
	while (!lastBlock) {
		auto nextBlock = currentBlock;
		nextBlock++;
		lastBlock = (nextBlock == (file.indexBlockRef.block.children.end() - 1)) || (keyRange.end <= nextBlock->key);
		Standalone<GranuleSnapshot> dataBlock =
		    file.getChild<GranuleSnapshot>(currentBlock, cipherKeysCtx, file.chunkStartOffset);
		ASSERT(!dataBlock.empty());
		ASSERT(currentBlock->key == dataBlock.front().key);

		bool anyRows = false;
		for (auto& entry : dataBlock) {
			if (!results.empty() && !lastBlock) {
				// no key comparisons needed
				results.emplace_back(results.arena(), entry);
				anyRows = true;
			} else if ((!results.empty() || entry.key >= keyRange.begin) && (!lastBlock || entry.key < keyRange.end)) {
				results.emplace_back(results.arena(), entry);
				anyRows = true;
			} else if (!results.empty() && lastBlock) {
				break;
			}
		}
		if (anyRows) {
			results.arena().dependsOn(dataBlock.arena());
		}
		currentBlock++;
	}

	return results;
}

typedef std::map<Key, Standalone<DeltaBoundaryRef>> SortedDeltasT;

// FIXME: optimize all of this with common prefix comparison stuff
SortedDeltasT::iterator insertMutationBoundary(SortedDeltasT& deltasByKey, const KeyRef& boundary) {
	// Find the first split point in buffer that is >= key
	auto it = deltasByKey.lower_bound(boundary);

	// Since the map contains fileRange already, we had to have found something
	ASSERT(it != deltasByKey.end());
	if (it->first == boundary) {
		return it;
	}

	// new boundary, using find as insert hint
	it = deltasByKey.insert(it, { boundary, Standalone<DeltaBoundaryRef>() });

	// look back at previous entry to see if this boundary is already cleared to at a prior version
	ASSERT(it != deltasByKey.begin());
	auto itPrev = it;
	--itPrev;

	if (itPrev->second.clearVersion.present()) {
		it->second.clearVersion = itPrev->second.clearVersion;
		it->second.values.push_back(it->second.arena(), ValueAndVersionRef(it->second.clearVersion.get()));
	}

	return it;
}

void updateMutationBoundary(Standalone<DeltaBoundaryRef>& boundary, const ValueAndVersionRef& update) {
	if (update.isSet()) {
		if (boundary.values.empty() || boundary.values.back().version < update.version) {
			// duplicate same set even if it's the same as the last one, so beginVersion reads still get updates
			boundary.values.push_back(boundary.arena(), update);
		} else {
			CODE_PROBE(true, "multiple boundary updates at same version (set)");
			// preserve inter-mutation order by replacing this one
			boundary.values.back() = update;
		}
	} else {
		if (boundary.values.empty() ||
		    (boundary.values.back().isSet() && boundary.values.back().version < update.version)) {
			// don't duplicate single-key clears in order if previous was also a clear, since it's a no-op when starting
			// with beginVersion
			boundary.values.push_back(boundary.arena(), update);
		} else if (!boundary.values.empty() && boundary.values.back().version == update.version) {
			CODE_PROBE(true, "multiple boundary updates at same version (clear)");
			if (boundary.values.back().isSet()) {
				// if the last 2 updates were clear @ v1 and set @ v2, and we now have a clear at v2, just pop off the
				// set and leave the previous clear. Otherwise, just set the last set to a clear
				if (boundary.values.size() >= 2 && boundary.values[boundary.values.size() - 2].isClear()) {
					CODE_PROBE(true, "clear then set/clear at same version optimization");
					boundary.values.pop_back();
				} else {
					boundary.values.back() = update;
				}
			} // else we have 2 consecutive clears at this version, no-op
		}
	}
}

void insertSortedDelta(const MutationRef& m,
                       const Version version,
                       const KeyRangeRef& fileRange,
                       SortedDeltasT& deltasByKey) {
	// TODO REMOVE validation
	ASSERT(fileRange.contains(m.param1));
	if (m.type == MutationRef::ClearRange) {
		ASSERT(m.param2 <= fileRange.end);
		// handle single key clear more efficiently
		if (equalsKeyAfter(m.param1, m.param2)) {
			SortedDeltasT::iterator key = insertMutationBoundary(deltasByKey, m.param1);
			updateMutationBoundary(key->second, ValueAndVersionRef(version));
		} else {
			// Update each boundary in the cleared range
			SortedDeltasT::iterator begin = insertMutationBoundary(deltasByKey, m.param1);
			SortedDeltasT::iterator end = insertMutationBoundary(deltasByKey, m.param2);
			while (begin != end) {
				// Set the rangeClearedVersion if not set
				if (!begin->second.clearVersion.present()) {
					begin->second.clearVersion = version;
				}

				// Add a clear to values if it's empty or the last item is not a clear
				if (begin->second.values.empty() || begin->second.values.back().isSet()) {
					updateMutationBoundary(begin->second, ValueAndVersionRef(version));
				}
				++begin;
			}
		}
	} else {
		Standalone<DeltaBoundaryRef>& bound = insertMutationBoundary(deltasByKey, m.param1)->second;
		updateMutationBoundary(bound, ValueAndVersionRef(version, m.param2));
	}
}

// TODO: investigate more cpu-efficient sorting methods. Potential options:
// 1) Replace std::map with ART mutation buffer
// 2) sort updates and clear endpoints by (key, version), and keep track of active clears.
void sortDeltasByKey(const Standalone<GranuleDeltas>& deltasByVersion,
                     const KeyRangeRef& fileRange,
                     SortedDeltasT& deltasByKey) {
	if (deltasByVersion.empty()) {
		return;
	}
	if (deltasByKey.empty()) {
		deltasByKey.insert({ fileRange.begin, Standalone<DeltaBoundaryRef>() });
		deltasByKey.insert({ fileRange.end, Standalone<DeltaBoundaryRef>() });
	}
	for (auto& it : deltasByVersion) {
		for (auto& m : it.mutations) {
			insertSortedDelta(m, it.version, fileRange, deltasByKey);
		}
	}

	// TODO: could do a scan through map and coalesce clears (if any boundaries with exactly 1 mutation (clear) and same
	// clearVersion as previous guy)
}

// FIXME: Could maybe reduce duplicated code between this and chunkedSnapshot for chunking
Value serializeChunkedDeltaFile(const Standalone<StringRef>& fileNameRef,
                                const Standalone<GranuleDeltas>& deltas,
                                const KeyRangeRef& fileRange,
                                int chunkSize,
                                Optional<CompressionFilter> compressFilter,
                                Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	if (BG_ENCRYPT_COMPRESS_DEBUG) {
		TraceEvent(SevDebug, "SerializeChunkedDelta")
		    .detail("Filename", fileNameRef.toString())
		    .detail("RangeBegin", fileRange.begin.printable())
		    .detail("RangeEnd", fileRange.end.printable())
		    .detail("Encrypted", cipherKeysCtx.present())
		    .detail("Compressed", compressFilter.present());
	}

	CODE_PROBE(compressFilter.present(), "serializing compressed delta file");
	CODE_PROBE(cipherKeysCtx.present(), "serializing encrypted delta file");
	Standalone<IndexedBlobGranuleFile> file;

	file.init(DELTA_FILE_TYPE, cipherKeysCtx);

	// build in-memory version of boundaries - TODO separate functions
	SortedDeltasT boundaries;
	sortDeltasByKey(deltas, fileRange, boundaries);

	std::vector<Value> chunks;
	chunks.push_back(Value()); // dummy value for index block

	Standalone<GranuleSortedDeltas> currentChunk;
	size_t currentChunkBytesEstimate = 0;
	size_t previousChunkBytes = 0;

	// TODO REMOVE - for validation
	KeyRef lastKey;
	int i = 0;
	for (auto& it : boundaries) {
		// TODO REMOVE sanity check
		if (i > 0) {
			ASSERT(lastKey < it.first);
		}
		lastKey = it.first;
		it.second.key = it.first;

		currentChunk.boundaries.push_back_deep(currentChunk.arena(), it.second);
		currentChunkBytesEstimate += it.second.totalSize();

		if (currentChunkBytesEstimate >= chunkSize || i == boundaries.size() - 1) {
			Value serialized =
			    BinaryWriter::toValue(currentChunk, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
			Value chunkBytes =
			    IndexBlobGranuleFileChunkRef::toBytes(cipherKeysCtx, compressFilter, serialized, file.arena());
			chunks.push_back(chunkBytes);

			// TODO remove validation
			if (!file.indexBlockRef.block.children.empty()) {
				ASSERT(file.indexBlockRef.block.children.back().key < currentChunk.boundaries.begin()->key);
			}
			file.indexBlockRef.block.children.emplace_back_deep(
			    file.arena(), currentChunk.boundaries.begin()->key, previousChunkBytes);

			if (BG_ENCRYPT_COMPRESS_DEBUG) {
				TraceEvent(SevDebug, "ChunkSize")
				    .detail("ChunkBytes", chunkBytes.size())
				    .detail("PrvChunkBytes", previousChunkBytes);
			}

			previousChunkBytes += chunkBytes.size();
			currentChunkBytesEstimate = 0;
			currentChunk = Standalone<GranuleSortedDeltas>();
		}
		i++;
	}
	ASSERT(currentChunk.boundaries.empty());
	if (!deltas.empty()) {
		file.indexBlockRef.block.children.emplace_back_deep(file.arena(), fileRange.end, previousChunkBytes);
	}

	return serializeFileFromChunks(file, cipherKeysCtx, chunks, previousChunkBytes);
}

ParsedDeltaBoundaryRef deltaAtVersion(const DeltaBoundaryRef& delta, Version beginVersion, Version readVersion) {
	bool clearAfter = delta.clearVersion.present() && readVersion >= delta.clearVersion.get() &&
	                  beginVersion <= delta.clearVersion.get();
	if (delta.values.empty()) {
		return ParsedDeltaBoundaryRef(delta.key, clearAfter);
	} else if (readVersion >= delta.values.back().version && beginVersion <= delta.values.back().version) {
		// for all but zero or one delta files, readVersion >= the entire delta file. optimize this case
		return ParsedDeltaBoundaryRef(delta.key, clearAfter, delta.values.back());
	}
	auto valueAtVersion = std::lower_bound(delta.values.begin(),
	                                       delta.values.end(),
	                                       ValueAndVersionRef(readVersion),
	                                       ValueAndVersionRef::OrderByVersion());
	if (valueAtVersion == delta.values.begin() && readVersion < valueAtVersion->version) {
		// deltas are all higher than read version
		return ParsedDeltaBoundaryRef(delta.key, clearAfter);
	}
	// lower_bound() found version >= readVersion, so if we're at the end or it's not equal, go back one
	if (valueAtVersion == delta.values.end() || valueAtVersion->version > readVersion) {
		valueAtVersion--;
	}
	ASSERT(readVersion >= valueAtVersion->version);
	// now, handle beginVersion (if update < beginVersion, it's a noop)
	if (valueAtVersion->version < beginVersion) {
		return ParsedDeltaBoundaryRef(delta.key, clearAfter);
	} else {
		return ParsedDeltaBoundaryRef(delta.key, clearAfter, *valueAtVersion);
	}
}

// The arena owns the BoundaryDeltaRef struct data but the StringRef pointers point to data in deltaData, to avoid extra
// copying
Standalone<VectorRef<ParsedDeltaBoundaryRef>> loadChunkedDeltaFile(const Standalone<StringRef>& fileNameRef,
                                                                   const StringRef& deltaData,
                                                                   const KeyRangeRef& keyRange,
                                                                   Version beginVersion,
                                                                   Version readVersion,
                                                                   Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx,
                                                                   bool& startClear) {
	Standalone<VectorRef<ParsedDeltaBoundaryRef>> deltas;
	Standalone<IndexedBlobGranuleFile> file = IndexedBlobGranuleFile::fromFileBytes(deltaData, cipherKeysCtx);

	ASSERT(file.fileType == DELTA_FILE_TYPE);
	ASSERT(file.chunkStartOffset > 0);

	// empty delta file
	if (file.indexBlockRef.block.children.empty()) {
		return deltas;
	}

	ASSERT(file.indexBlockRef.block.children.size() >= 2);

	// find range of blocks needed to read
	ChildBlockPointerRef* currentBlock = file.findStartBlock(keyRange.begin);

	if (currentBlock == (file.indexBlockRef.block.children.end() - 1) || keyRange.end <= currentBlock->key) {
		// empty, done
		return deltas;
	}

	// FIXME: shared prefix for key comparison
	// FIXME: could cpu optimize first block a bit more by seeking right to start
	bool lastBlock = false;
	bool prevClearAfter = false;
	while (!lastBlock) {
		auto nextBlock = currentBlock;
		nextBlock++;
		lastBlock = (nextBlock == file.indexBlockRef.block.children.end() - 1) || keyRange.end <= nextBlock->key;

		Standalone<GranuleSortedDeltas> deltaBlock =
		    file.getChild<GranuleSortedDeltas>(currentBlock, cipherKeysCtx, file.chunkStartOffset);
		ASSERT(!deltaBlock.boundaries.empty());
		ASSERT(currentBlock->key == deltaBlock.boundaries.front().key);

		// TODO refactor this into function to share with memory deltas
		bool blockMemoryUsed = false;

		for (auto& entry : deltaBlock.boundaries) {
			ParsedDeltaBoundaryRef boundary = deltaAtVersion(entry, beginVersion, readVersion);
			if (deltas.empty() && entry.key < keyRange.begin) {
				startClear = boundary.clearAfter;
				prevClearAfter = boundary.clearAfter;
			} else if (!lastBlock || entry.key < keyRange.end) {
				if (!boundary.redundant(prevClearAfter)) {
					deltas.push_back(deltas.arena(), boundary);
					blockMemoryUsed = true;
					prevClearAfter = boundary.clearAfter;
				}
			} else {
				// TODO REMOVE validation
				ASSERT(lastBlock);
				break;
			}
		}
		if (blockMemoryUsed) {
			deltas.arena().dependsOn(deltaBlock.arena());
		}
		currentBlock++;
	}

	// TODO REMOVE eventually? order sanity check for parsed deltas
	for (int i = 0; i < deltas.size() - 1; i++) {
		ASSERT(deltas[i].key < deltas[i + 1].key);
	}

	return deltas;
}

static void applyDelta(const KeyRangeRef& keyRange, const MutationRef& m, std::map<KeyRef, ValueRef>& dataMap) {
	if (m.type == MutationRef::ClearRange) {
		if (m.param2 <= keyRange.begin || m.param1 >= keyRange.end) {
			return;
		}
		// keyRange is inclusive on start, lower_bound is inclusive with the argument, and erase is inclusive for the
		// begin. So if lower bound didn't find the exact key, we need to go up one so it doesn't erase an extra key
		// outside the range.
		std::map<KeyRef, ValueRef>::iterator itStart = dataMap.lower_bound(m.param1);
		if (itStart != dataMap.end() && itStart->first < m.param1) {
			itStart++;
		}

		// keyRange is exclusive on end, lower bound is inclusive with the argument, and erase is exclusive for the end
		// key. So if lower bound didn't find the exact key, we need to go up one so it doesn't skip the last key it
		// should erase
		std::map<KeyRef, ValueRef>::iterator itEnd = dataMap.lower_bound(m.param2);
		if (itEnd != dataMap.end() && itEnd->first < m.param2) {
			itEnd++;
		}
		dataMap.erase(itStart, itEnd);
	} else {
		// We don't need atomics here since eager reads handles it
		ASSERT(m.type == MutationRef::SetValue);
		if (m.param1 < keyRange.begin || m.param1 >= keyRange.end) {
			return;
		}

		std::map<KeyRef, ValueRef>::iterator it = dataMap.find(m.param1);
		if (it == dataMap.end()) {
			dataMap.insert({ m.param1, m.param2 });
		} else {
			it->second = m.param2;
		}
	}
}

static void applyDeltasByVersion(const GranuleDeltas& deltas,
                                 const KeyRangeRef& keyRange,
                                 Version beginVersion,
                                 Version readVersion,
                                 Version& lastFileEndVersion,
                                 std::map<KeyRef, ValueRef>& dataMap) {
	if (deltas.empty()) {
		return;
	}
	// check that consecutive delta file versions are disjoint
	ASSERT(lastFileEndVersion < deltas.front().version);

	const MutationsAndVersionRef* mutationIt = deltas.begin();
	// prune beginVersion if necessary
	if (beginVersion > deltas.front().version) {
		if (beginVersion > deltas.back().version) {
			// can happen with force flush
			mutationIt = deltas.end();
		} else {
			// binary search for beginVersion
			mutationIt = std::lower_bound(deltas.begin(),
			                              deltas.end(),
			                              MutationsAndVersionRef(beginVersion, 0),
			                              MutationsAndVersionRef::OrderByVersion());
		}
	}

	while (mutationIt != deltas.end()) {
		if (mutationIt->version > readVersion) {
			lastFileEndVersion = readVersion;
			return;
		}
		for (auto& m : mutationIt->mutations) {
			applyDelta(keyRange, m, dataMap);
		}
		mutationIt++;
	}
	lastFileEndVersion = deltas.back().version;
}

// TODO: could optimize this slightly to avoid tracking multiple updates for the same key at all since it's always then
// collapsed to the last one
Standalone<VectorRef<ParsedDeltaBoundaryRef>> sortMemoryDeltas(const GranuleDeltas& memoryDeltas,
                                                               const KeyRangeRef& granuleRange,
                                                               const KeyRangeRef& readRange,
                                                               Version beginVersion,
                                                               Version readVersion) {
	ASSERT(!memoryDeltas.empty());

	// filter by request range first
	SortedDeltasT versionedBoundaries;
	if (versionedBoundaries.empty()) {
		versionedBoundaries.insert({ readRange.begin, Standalone<DeltaBoundaryRef>() });
		versionedBoundaries.insert({ readRange.end, Standalone<DeltaBoundaryRef>() });
	}
	for (auto& it : memoryDeltas) {
		for (auto& m : it.mutations) {
			if (m.type == MutationRef::ClearRange) {
				if (m.param2 > readRange.begin && m.param1 < readRange.end) {
					KeyRangeRef clearRangeClipped = readRange & KeyRangeRef(m.param1, m.param2);
					MutationRef clearClipped(
					    MutationRef::Type::ClearRange, clearRangeClipped.begin, clearRangeClipped.end);
					insertSortedDelta(clearClipped, it.version, granuleRange, versionedBoundaries);
				}
			} else {
				ASSERT(m.type == MutationRef::SetValue);
				if (readRange.contains(m.param1)) {
					insertSortedDelta(m, it.version, granuleRange, versionedBoundaries);
				}
			}
		}
	}

	// parse and collapse based on version
	bool prevClearAfter = false;
	Standalone<VectorRef<ParsedDeltaBoundaryRef>> deltas;

	// remove extra ranges inserted from clears that partially overlap read range
	auto itBegin = versionedBoundaries.begin();
	while (itBegin->first < readRange.begin) {
		++itBegin;
	}
	auto itEnd = versionedBoundaries.end();
	itEnd--;
	while (itEnd->first > readRange.end) {
		itEnd--;
	}
	itEnd++;

	while (itBegin != itEnd) {
		itBegin->second.key = itBegin->first;
		ParsedDeltaBoundaryRef boundary = deltaAtVersion(itBegin->second, beginVersion, readVersion);
		if (!boundary.redundant(prevClearAfter)) {
			deltas.push_back_deep(deltas.arena(), boundary);
			prevClearAfter = boundary.clearAfter;
		}
		++itBegin;
	}

	return deltas;
}

// does a sorted merge of the delta streams.
// In terms of write precedence, streams[i] < streams[i+1]
// Handles range clears by tracking the active clears when they start
struct MergeStreamNext {
	KeyRef key;
	int16_t streamIdx;
	int dataIdx;
};

// the sort order is logically lower by key, and then higher by streamIdx
// because a priority queue is backwards, we invert that
struct OrderForPriorityQueue {
	int commonPrefixLen;
	OrderForPriorityQueue(int commonPrefixLen) : commonPrefixLen(commonPrefixLen) {}

	bool operator()(MergeStreamNext const& a, MergeStreamNext const& b) const {
		int keyCmp = a.key.compareSuffix(b.key, commonPrefixLen);
		if (keyCmp != 0) {
			return keyCmp > 0; // reverse
		}
		return a.streamIdx < b.streamIdx;
	}
};

typedef std::priority_queue<MergeStreamNext, std::vector<MergeStreamNext>, OrderForPriorityQueue> MergePQ;

static RangeResult mergeDeltaStreams(const BlobGranuleChunkRef& chunk,
                                     const std::vector<Standalone<VectorRef<ParsedDeltaBoundaryRef>>>& streams,
                                     const std::vector<bool> startClears,
                                     GranuleMaterializeStats& stats) {
	ASSERT(streams.size() < std::numeric_limits<int16_t>::max());
	ASSERT(startClears.size() == streams.size());

	int prefixLen = commonPrefixLength(chunk.keyRange.begin, chunk.keyRange.end);

	// next element for each stream
	MergePQ next = MergePQ(OrderForPriorityQueue(prefixLen));

	// efficiently find the highest stream's active clear
	std::set<int16_t, std::greater<int16_t>> activeClears;
	int16_t maxActiveClear = -1;

	// trade off memory for cpu performance by assuming all inserts
	RangeResult result;
	int maxExpectedSize = 0;

	// check if a given stream is actively clearing
	bool clearActive[streams.size()];
	for (int16_t i = 0; i < streams.size(); i++) {
		clearActive[i] = startClears[i];
		if (startClears[i]) {
			activeClears.insert(i);
			maxActiveClear = i;
		}
		if (streams[i].empty()) {
			// single clear that entirely encases partial read bounds
			ASSERT(clearActive[i]);
		} else {
			MergeStreamNext item;
			item.key = streams[i][0].key;
			item.streamIdx = i;
			item.dataIdx = 0;
			next.push(item);
			maxExpectedSize += streams[i].size();
			result.arena().dependsOn(streams[i].arena());
		}
	}
	result.reserve(result.arena(), maxExpectedSize);

	std::vector<MergeStreamNext> cur;
	cur.reserve(streams.size());
	while (!next.empty()) {
		cur.clear();
		cur.push_back(next.top());
		next.pop();

		// next.top().key == cur.front().key but with suffix comparison
		while (!next.empty() && cur.front().key.compareSuffix(next.top().key, prefixLen) == 0) {
			cur.push_back(next.top());
			next.pop();
		}

		// un-set clears and find latest value for key (if present)
		bool foundValue = false;
		bool includesSnapshot = cur.back().streamIdx == 0 && chunk.snapshotFile.present();
		for (auto& it : cur) {
			auto& v = streams[it.streamIdx][it.dataIdx];
			if (clearActive[it.streamIdx]) {
				clearActive[it.streamIdx] = false;
				activeClears.erase(it.streamIdx);
				if (it.streamIdx == maxActiveClear) {
					// re-get max active clear
					maxActiveClear = activeClears.empty() ? -1 : *activeClears.begin();
				}
			}

			// find value for this key (if any)
			if (!foundValue && !v.isNoOp()) {
				foundValue = true;
				// if it's a clear, or maxActiveClear is higher, no value for this key
				if (v.isSet() && maxActiveClear < it.streamIdx) {
					KeyRef finalKey =
					    chunk.tenantPrefix.present() ? v.key.removePrefix(chunk.tenantPrefix.get()) : v.key;
					result.push_back(result.arena(), KeyValueRef(finalKey, v.value));
					if (!includesSnapshot) {
						stats.rowsInserted++;
					} else if (it.streamIdx > 0) {
						stats.rowsUpdated++;
					}
				} else if (includesSnapshot) {
					stats.rowsCleared++;
				}
			}
		}

		// advance streams and start clearAfter
		for (auto& it : cur) {
			if (streams[it.streamIdx][it.dataIdx].clearAfter) {
				clearActive[it.streamIdx] = true;
				activeClears.insert(it.streamIdx);
				maxActiveClear = std::max(maxActiveClear, it.streamIdx);
			}
			// TODO: implement skipping if large clear!!
			// if (maxClearIdx > it.streamIdx) - skip
			it.dataIdx++;
			if (it.dataIdx < streams[it.streamIdx].size()) {
				it.key = streams[it.streamIdx][it.dataIdx].key;
				next.push(it);
			}
		}
	}

	// FIXME: if memory assumption was wrong and result is significantly smaller than total input size, could copy it
	// with push_back_deep to a new result. This is rare though

	stats.outputBytes += result.expectedSize();

	return result;
}

RangeResult materializeJustSnapshot(const BlobGranuleChunkRef& chunk,
                                    Optional<StringRef> snapshotData,
                                    const KeyRange& requestRange,
                                    GranuleMaterializeStats& stats) {
	stats.inputBytes += snapshotData.get().size();

	Standalone<VectorRef<ParsedDeltaBoundaryRef>> snapshotRows = loadSnapshotFile(
	    chunk.snapshotFile.get().filename, snapshotData.get(), requestRange, chunk.snapshotFile.get().cipherKeysCtx);
	RangeResult result;
	if (!snapshotRows.empty()) {
		result.arena().dependsOn(snapshotRows.arena());
		result.reserve(result.arena(), snapshotRows.size());
		for (auto& it : snapshotRows) {
			// TODO REMOVE validation
			ASSERT(it.op == MutationRef::Type::SetValue);
			KeyRef finalKey = chunk.tenantPrefix.present() ? it.key.removePrefix(chunk.tenantPrefix.get()) : it.key;
			result.push_back(result.arena(), KeyValueRef(finalKey, it.value));
		}
		stats.outputBytes += result.expectedSize();
		stats.snapshotRows += result.size();
	}

	return result;
}

RangeResult materializeBlobGranule(const BlobGranuleChunkRef& chunk,
                                   KeyRangeRef keyRange,
                                   Version beginVersion,
                                   Version readVersion,
                                   Optional<StringRef> snapshotData,
                                   StringRef deltaFileData[],
                                   GranuleMaterializeStats& stats) {
	// TODO REMOVE with early replying
	ASSERT(readVersion == chunk.includedVersion);

	// Arena to hold all allocations for applying deltas. Most of it, and the arenas produced by reading the files,
	// will likely be tossed if there are a significant number of mutations, so we copy at the end instead of doing a
	// dependsOn.
	// FIXME: probably some threshold of a small percentage of the data is actually changed, where it makes sense to
	// just to dependsOn instead of copy, to use a little extra memory footprint to help cpu?
	Arena arena;
	KeyRange requestRange;
	if (chunk.tenantPrefix.present()) {
		requestRange = keyRange.withPrefix(chunk.tenantPrefix.get());
	} else {
		requestRange = keyRange;
	}

	// fast case for only-snapshot read
	if (chunk.snapshotFile.present() && chunk.deltaFiles.empty() && chunk.newDeltas.empty()) {
		return materializeJustSnapshot(chunk, snapshotData, requestRange, stats);
	}

	std::vector<Standalone<VectorRef<ParsedDeltaBoundaryRef>>> streams;
	std::vector<bool> startClears;
	// +1 for possible snapshot, +1 for possible memory deltas
	streams.reserve(chunk.deltaFiles.size() + 2);

	if (snapshotData.present()) {
		stats.inputBytes += snapshotData.get().size();
		ASSERT(chunk.snapshotFile.present());
		Standalone<VectorRef<ParsedDeltaBoundaryRef>> snapshotRows =
		    loadSnapshotFile(chunk.snapshotFile.get().filename,
		                     snapshotData.get(),
		                     requestRange,
		                     chunk.snapshotFile.get().cipherKeysCtx);
		if (!snapshotRows.empty()) {
			streams.push_back(snapshotRows);
			startClears.push_back(false);
			arena.dependsOn(streams.back().arena());
			stats.snapshotRows += snapshotRows.size();
		}
	} else {
		ASSERT(!chunk.snapshotFile.present());
	}

	if (BG_READ_DEBUG) {
		fmt::print("Applying {} delta files\n", chunk.deltaFiles.size());
	}
	for (int deltaIdx = 0; deltaIdx < chunk.deltaFiles.size(); deltaIdx++) {
		stats.inputBytes += deltaFileData[deltaIdx].size();
		bool startClear = false;
		auto deltaRows = loadChunkedDeltaFile(chunk.deltaFiles[deltaIdx].filename,
		                                      deltaFileData[deltaIdx],
		                                      requestRange,
		                                      beginVersion,
		                                      readVersion,
		                                      chunk.deltaFiles[deltaIdx].cipherKeysCtx,
		                                      startClear);
		if (startClear || !deltaRows.empty()) {
			streams.push_back(deltaRows);
			startClears.push_back(startClear);
			arena.dependsOn(streams.back().arena());
		}
		arena.dependsOn(deltaRows.arena());
	}
	if (BG_READ_DEBUG) {
		fmt::print("Applying {} memory deltas\n", chunk.newDeltas.size());
	}
	if (!chunk.newDeltas.empty()) {
		stats.inputBytes += chunk.newDeltas.expectedSize();
		// TODO REMOVE validation
		ASSERT(beginVersion <= chunk.newDeltas.front().version);
		ASSERT(readVersion >= chunk.newDeltas.back().version);
		auto memoryRows = sortMemoryDeltas(chunk.newDeltas, chunk.keyRange, requestRange, beginVersion, readVersion);
		if (!memoryRows.empty()) {
			streams.push_back(memoryRows);
			startClears.push_back(false);
			arena.dependsOn(streams.back().arena());
		}
	}

	return mergeDeltaStreams(chunk, streams, startClears, stats);
}

struct GranuleLoadFreeHandle : NonCopyable, ReferenceCounted<GranuleLoadFreeHandle> {
	const ReadBlobGranuleContext* granuleContext;
	int64_t loadId;

	GranuleLoadFreeHandle(const ReadBlobGranuleContext* granuleContext, int64_t loadId)
	  : granuleContext(granuleContext), loadId(loadId) {}

	~GranuleLoadFreeHandle() { granuleContext->free_load_f(loadId, granuleContext->userContext); }
};

struct GranuleLoadIds {
	Optional<int64_t> snapshotId;
	std::vector<int64_t> deltaIds;
	std::vector<Reference<GranuleLoadFreeHandle>> freeHandles;
};

static void startLoad(const ReadBlobGranuleContext* granuleContext,
                      const BlobGranuleChunkRef& chunk,
                      GranuleLoadIds& loadIds) {

	// Start load process for all files in chunk
	if (chunk.snapshotFile.present()) {
		std::string snapshotFname = chunk.snapshotFile.get().filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.snapshotFile.get().offset == 0);
		ASSERT(chunk.snapshotFile.get().length == chunk.snapshotFile.get().fullFileLength);
		loadIds.snapshotId = granuleContext->start_load_f(snapshotFname.c_str(),
		                                                  snapshotFname.size(),
		                                                  chunk.snapshotFile.get().offset,
		                                                  chunk.snapshotFile.get().length,
		                                                  chunk.snapshotFile.get().fullFileLength,
		                                                  granuleContext->userContext);
		loadIds.freeHandles.push_back(makeReference<GranuleLoadFreeHandle>(granuleContext, loadIds.snapshotId.get()));
	}
	loadIds.deltaIds.reserve(chunk.deltaFiles.size());
	for (int deltaFileIdx = 0; deltaFileIdx < chunk.deltaFiles.size(); deltaFileIdx++) {
		std::string deltaFName = chunk.deltaFiles[deltaFileIdx].filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.deltaFiles[deltaFileIdx].offset == 0);
		ASSERT(chunk.deltaFiles[deltaFileIdx].length == chunk.deltaFiles[deltaFileIdx].fullFileLength);
		int64_t deltaLoadId = granuleContext->start_load_f(deltaFName.c_str(),
		                                                   deltaFName.size(),
		                                                   chunk.deltaFiles[deltaFileIdx].offset,
		                                                   chunk.deltaFiles[deltaFileIdx].length,
		                                                   chunk.deltaFiles[deltaFileIdx].fullFileLength,
		                                                   granuleContext->userContext);
		loadIds.deltaIds.push_back(deltaLoadId);
		loadIds.freeHandles.push_back(makeReference<GranuleLoadFreeHandle>(granuleContext, deltaLoadId));
	}
}

ErrorOr<RangeResult> loadAndMaterializeBlobGranules(const Standalone<VectorRef<BlobGranuleChunkRef>>& files,
                                                    const KeyRangeRef& keyRange,
                                                    Version beginVersion,
                                                    Version readVersion,
                                                    ReadBlobGranuleContext granuleContext,
                                                    GranuleMaterializeStats& stats) {
	int64_t parallelism = granuleContext.granuleParallelism;
	if (parallelism < 1) {
		parallelism = 1;
	}
	if (parallelism >= CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM) {
		parallelism = CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM;
	}

	GranuleLoadIds loadIds[files.size()];

	try {
		// Kick off first file reads if parallelism > 1
		for (int i = 0; i < parallelism - 1 && i < files.size(); i++) {
			startLoad(&granuleContext, files[i], loadIds[i]);
		}
		RangeResult results;
		for (int chunkIdx = 0; chunkIdx < files.size(); chunkIdx++) {
			// Kick off files for this granule if parallelism == 1, or future granule if parallelism > 1
			if (chunkIdx + parallelism - 1 < files.size()) {
				startLoad(&granuleContext, files[chunkIdx + parallelism - 1], loadIds[chunkIdx + parallelism - 1]);
			}

			RangeResult chunkRows;

			// once all loads kicked off, load data for chunk
			Optional<StringRef> snapshotData;
			if (files[chunkIdx].snapshotFile.present()) {
				snapshotData =
				    StringRef(granuleContext.get_load_f(loadIds[chunkIdx].snapshotId.get(), granuleContext.userContext),
				              files[chunkIdx].snapshotFile.get().length);
				if (!snapshotData.get().begin()) {
					return ErrorOr<RangeResult>(blob_granule_file_load_error());
				}
			}

			// +1 to avoid UBSAN variable length array of size zero
			StringRef deltaData[files[chunkIdx].deltaFiles.size() + 1];
			for (int i = 0; i < files[chunkIdx].deltaFiles.size(); i++) {
				deltaData[i] =
				    StringRef(granuleContext.get_load_f(loadIds[chunkIdx].deltaIds[i], granuleContext.userContext),
				              files[chunkIdx].deltaFiles[i].length);
				// null data is error
				if (!deltaData[i].begin()) {
					return ErrorOr<RangeResult>(blob_granule_file_load_error());
				}
			}

			// materialize rows from chunk
			chunkRows = materializeBlobGranule(
			    files[chunkIdx], keyRange, beginVersion, readVersion, snapshotData, deltaData, stats);

			results.arena().dependsOn(chunkRows.arena());
			results.append(results.arena(), chunkRows.begin(), chunkRows.size());

			// free once done by forcing FreeHandles to trigger
			loadIds[chunkIdx].freeHandles.clear();
		}
		return ErrorOr<RangeResult>(results);
	} catch (Error& e) {
		return ErrorOr<RangeResult>(e);
	}
}

std::string randomBGFilename(UID blobWorkerID, UID granuleID, Version version, std::string suffix) {
	// Start with random bytes to avoid metadata hotspotting
	// Worker ID for uniqueness and attribution
	// Granule ID for uniqueness and attribution
	// Version for uniqueness and possible future use
	return deterministicRandom()->randomUniqueID().shortString().substr(0, 8) + "_" +
	       blobWorkerID.shortString().substr(0, 8) + "_" + granuleID.shortString() + "_V" + std::to_string(version) +
	       suffix;
}

namespace {
const EncryptCipherDomainId encryptDomainId = deterministicRandom()->randomInt64(786, 7860);
const EncryptCipherBaseKeyId encryptBaseCipherId = deterministicRandom()->randomUInt64();
const EncryptCipherRandomSalt encryptSalt = deterministicRandom()->randomUInt64();

Standalone<StringRef> getBaseCipher() {
	Standalone<StringRef> baseCipher = makeString(AES_256_KEY_LENGTH);
	deterministicRandom()->randomBytes(mutateString(baseCipher), baseCipher.size());
	return baseCipher;
}

Standalone<StringRef> encryptBaseCipher = getBaseCipher();

BlobGranuleCipherKeysCtx getCipherKeysCtx(Arena& arena) {
	BlobGranuleCipherKeysCtx cipherKeysCtx;

	cipherKeysCtx.textCipherKey.encryptDomainId = encryptDomainId;
	cipherKeysCtx.textCipherKey.baseCipherId = encryptBaseCipherId;
	cipherKeysCtx.textCipherKey.salt = encryptSalt;
	cipherKeysCtx.textCipherKey.baseCipher = StringRef(arena, encryptBaseCipher);

	cipherKeysCtx.headerCipherKey.encryptDomainId = SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID;
	cipherKeysCtx.headerCipherKey.baseCipherId = encryptBaseCipherId;
	cipherKeysCtx.headerCipherKey.salt = encryptSalt;
	cipherKeysCtx.headerCipherKey.baseCipher = StringRef(arena, encryptBaseCipher);

	cipherKeysCtx.ivRef = makeString(AES_256_IV_LENGTH, arena);
	deterministicRandom()->randomBytes(mutateString(cipherKeysCtx.ivRef), AES_256_IV_LENGTH);

	return cipherKeysCtx;
}

} // namespace

TEST_CASE("/blobgranule/files/applyDelta") {
	printf("Testing blob granule delta applying\n");
	Arena a;

	StringRef k_a = StringRef(a, "A"_sr);
	StringRef k_ab = StringRef(a, "AB"_sr);
	StringRef k_b = StringRef(a, "B"_sr);
	StringRef k_c = StringRef(a, "C"_sr);
	StringRef k_z = StringRef(a, "Z"_sr);
	StringRef val1 = StringRef(a, "1"_sr);
	StringRef val2 = StringRef(a, "2"_sr);

	std::map<KeyRef, ValueRef> data;
	data.insert({ k_a, val1 });
	data.insert({ k_ab, val1 });
	data.insert({ k_b, val1 });

	std::map<KeyRef, ValueRef> correctData = data;
	std::map<KeyRef, ValueRef> originalData = data;

	ASSERT(data == correctData);

	// test all clear permutations

	MutationRef mClearEverything(MutationRef::ClearRange, allKeys.begin, allKeys.end);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything2(MutationRef::ClearRange, allKeys.begin, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything2, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything3(MutationRef::ClearRange, k_a, allKeys.end);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything3, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearEverything4(MutationRef::ClearRange, k_a, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearEverything, data);
	correctData.clear();
	ASSERT(data == correctData);

	MutationRef mClearFirst(MutationRef::ClearRange, k_a, k_ab);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearFirst, data);
	correctData.erase(k_a);
	ASSERT(data == correctData);

	MutationRef mClearSecond(MutationRef::ClearRange, k_ab, k_b);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearSecond, data);
	correctData.erase(k_ab);
	ASSERT(data == correctData);

	MutationRef mClearThird(MutationRef::ClearRange, k_b, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearThird, data);
	correctData.erase(k_b);
	ASSERT(data == correctData);

	MutationRef mClearFirst2(MutationRef::ClearRange, k_a, k_b);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearFirst2, data);
	correctData.erase(k_a);
	correctData.erase(k_ab);
	ASSERT(data == correctData);

	MutationRef mClearLast2(MutationRef::ClearRange, k_ab, k_c);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mClearLast2, data);
	correctData.erase(k_ab);
	correctData.erase(k_b);
	ASSERT(data == correctData);

	// test set data
	MutationRef mSetA(MutationRef::SetValue, k_a, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetA, data);
	correctData[k_a] = val2;
	ASSERT(data == correctData);

	MutationRef mSetAB(MutationRef::SetValue, k_ab, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetAB, data);
	correctData[k_ab] = val2;
	ASSERT(data == correctData);

	MutationRef mSetB(MutationRef::SetValue, k_b, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetB, data);
	correctData[k_b] = val2;
	ASSERT(data == correctData);

	MutationRef mSetC(MutationRef::SetValue, k_c, val2);
	data = originalData;
	correctData = originalData;
	applyDelta(allKeys, mSetC, data);
	correctData[k_c] = val2;
	ASSERT(data == correctData);

	// test pruning deltas that are outside of the key range

	MutationRef mSetZ(MutationRef::SetValue, k_z, val2);
	data = originalData;
	applyDelta(KeyRangeRef(k_a, k_c), mSetZ, data);
	ASSERT(data == originalData);

	applyDelta(KeyRangeRef(k_ab, k_c), mSetA, data);
	ASSERT(data == originalData);

	applyDelta(KeyRangeRef(k_ab, k_c), mClearFirst, data);
	ASSERT(data == originalData);

	applyDelta(KeyRangeRef(k_a, k_ab), mClearThird, data);
	ASSERT(data == originalData);

	return Void();
}

void checkDeltaAtVersion(const ParsedDeltaBoundaryRef& expected,
                         const DeltaBoundaryRef& boundary,
                         Version beginVersion,
                         Version readVersion) {
	ParsedDeltaBoundaryRef actual = deltaAtVersion(boundary, beginVersion, readVersion);
	ASSERT(expected.clearAfter == actual.clearAfter);
	ASSERT(expected.op == actual.op);
	if (expected.isSet()) {
		ASSERT(expected.value == actual.value);
	} else {
		ASSERT(actual.value.empty());
	}
}

TEST_CASE("/blobgranule/files/deltaAtVersion") {
	Arena ar;
	std::string keyStr = "k";
	std::string aStr = "a";

	KeyRef key(ar, keyStr);
	ValueAndVersionRef vv_a_3(3, ValueRef(ar, aStr));
	ValueAndVersionRef vv_clear_5(5);

	ParsedDeltaBoundaryRef resultEmpty(key, false);
	ParsedDeltaBoundaryRef resultEmptyWithClear(key, true);
	ParsedDeltaBoundaryRef resultSetA(key, false, vv_a_3);
	ParsedDeltaBoundaryRef resultClearA(key, true, vv_clear_5);

	// test empty boundary ref
	DeltaBoundaryRef boundaryEmpty;
	boundaryEmpty.key = key;
	checkDeltaAtVersion(resultEmpty, boundaryEmpty, 0, 2);

	// test empty boundary with clear
	DeltaBoundaryRef boundaryEmptyWithClear;
	boundaryEmptyWithClear.key = key;
	boundaryEmptyWithClear.clearVersion = 5;

	// higher read version includes clear
	checkDeltaAtVersion(resultEmptyWithClear, boundaryEmptyWithClear, 0, 5);
	checkDeltaAtVersion(resultEmptyWithClear, boundaryEmptyWithClear, 0, 10);
	checkDeltaAtVersion(resultEmptyWithClear, boundaryEmptyWithClear, 2, 5);
	checkDeltaAtVersion(resultEmptyWithClear, boundaryEmptyWithClear, 2, 10);
	checkDeltaAtVersion(resultEmptyWithClear, boundaryEmptyWithClear, 5, 10);
	checkDeltaAtVersion(resultEmptyWithClear, boundaryEmptyWithClear, 5, 5);

	// lower read version does not include clear
	checkDeltaAtVersion(resultEmpty, boundaryEmptyWithClear, 0, 4);
	checkDeltaAtVersion(resultEmpty, boundaryEmptyWithClear, 3, 4);

	// higher read version but also higher beginVersion does not include clear
	checkDeltaAtVersion(resultEmpty, boundaryEmptyWithClear, 6, 10);

	// check values
	DeltaBoundaryRef fullBoundary;
	fullBoundary.key = key;
	fullBoundary.values.push_back(ar, vv_a_3);
	fullBoundary.values.push_back(ar, vv_clear_5);
	fullBoundary.clearVersion = 5;

	checkDeltaAtVersion(resultEmpty, fullBoundary, 0, 2);
	checkDeltaAtVersion(resultEmpty, fullBoundary, 6, 10);
	checkDeltaAtVersion(resultEmpty, fullBoundary, 4, 4);

	checkDeltaAtVersion(resultSetA, fullBoundary, 0, 3);
	checkDeltaAtVersion(resultSetA, fullBoundary, 3, 4);

	checkDeltaAtVersion(resultClearA, fullBoundary, 0, 5);
	checkDeltaAtVersion(resultClearA, fullBoundary, 0, 10);
	checkDeltaAtVersion(resultClearA, fullBoundary, 3, 5);
	checkDeltaAtVersion(resultClearA, fullBoundary, 4, 5);

	return Void();
}

void checkSnapshotEmpty(const Value& serialized, Key begin, Key end, Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	Standalone<StringRef> fileNameRef = StringRef();
	Standalone<VectorRef<ParsedDeltaBoundaryRef>> result =
	    loadSnapshotFile(fileNameRef, serialized, KeyRangeRef(begin, end), cipherKeysCtx);
	ASSERT(result.empty());
}

// endIdx is exclusive
void checkSnapshotRead(const Standalone<StringRef>& fileNameRef,
                       const Standalone<GranuleSnapshot>& snapshot,
                       const Value& serialized,
                       int beginIdx,
                       int endIdx,
                       Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	ASSERT(beginIdx < endIdx);
	ASSERT(endIdx <= snapshot.size());
	KeyRef beginKey = snapshot[beginIdx].key;
	Key endKey = endIdx == snapshot.size() ? keyAfter(snapshot.back().key) : snapshot[endIdx].key;
	KeyRangeRef range(beginKey, endKey);

	fmt::print("Reading [{0} - {1})\n", beginKey.printable(), endKey.printable());

	Standalone<VectorRef<ParsedDeltaBoundaryRef>> result =
	    loadSnapshotFile(fileNameRef, serialized, range, cipherKeysCtx);

	if (result.size() != endIdx - beginIdx) {
		fmt::print("Read {0} rows != {1}\n", result.size(), endIdx - beginIdx);
	}

	if (BG_FILES_TEST_DEBUG) {
		fmt::print("Expected Data {0}:\n", result.size());
		for (auto& it : result) {
			fmt::print("  {0}=\n", it.key.printable());
		}
		fmt::print("Actual Data {0}:\n", endIdx - beginIdx);
		for (int i = beginIdx; i < endIdx; i++) {
			fmt::print("  {0}=\n", snapshot[i].key.printable());
		}
	}

	ASSERT(result.size() == endIdx - beginIdx);
	for (auto& it : result) {
		ASSERT(it.isSet());
		if (it.key != snapshot[beginIdx].key) {
			fmt::print("Key {0} != {1}\n", it.key.printable(), snapshot[beginIdx].key.printable());
		}
		ASSERT(it.key == snapshot[beginIdx].key);
		if (it.key != snapshot[beginIdx].key) {
			fmt::print("Value {0} != {1} for Key {2}\n",
			           it.value.printable(),
			           snapshot[beginIdx].value.printable(),
			           it.key.printable());
		}
		ASSERT(it.value == snapshot[beginIdx].value);
		beginIdx++;
	}
}

namespace {

size_t uidSize = 32;

struct KeyValueGen {
	Arena ar;
	std::string sharedPrefix;
	int targetKeyLength;
	int targetValueLength;
	std::set<std::string> usedKeys;
	std::vector<StringRef> usedKeysList;
	double clearFrequency;
	double clearUnsetFrequency;
	double updateExistingKeyFrequency;
	int minVersionIncrease;
	int maxVersionIncrease;
	int targetMutationsPerDelta;
	KeyRange allRange;

	Version version = 0;

	// encryption/compression settings
	// TODO: possibly different cipher keys or meta context per file?
	Optional<BlobGranuleCipherKeysCtx> cipherKeys;
	Optional<CompressionFilter> compressFilter;

	KeyValueGen() {
		sharedPrefix = deterministicRandom()->randomUniqueID().toString();
		ASSERT(sharedPrefix.size() == uidSize);
		int sharedPrefixLen = deterministicRandom()->randomInt(0, uidSize);
		targetKeyLength = deterministicRandom()->randomInt(4, uidSize);
		sharedPrefix = sharedPrefix.substr(0, sharedPrefixLen) + "_";
		targetValueLength = deterministicRandom()->randomExp(0, 12);
		allRange = KeyRangeRef(StringRef(sharedPrefix),
		                       sharedPrefix.size() == 0 ? "\xff"_sr : strinc(StringRef(sharedPrefix)));

		if (deterministicRandom()->coinflip()) {
			clearFrequency = 0.0;
			clearUnsetFrequency = 0.0;
		} else {
			clearFrequency = deterministicRandom()->random01() / 2;
			// clearing an unset value has no effect on the results, we mostly just want to make sure the format doesn't
			// barf
			clearUnsetFrequency = deterministicRandom()->random01() / 10;
		}
		if (deterministicRandom()->random01() < 0.2) {
			// no updates, only new writes
			updateExistingKeyFrequency = 0.0;
		} else {
			updateExistingKeyFrequency = deterministicRandom()->random01();
		}
		if (deterministicRandom()->coinflip()) {
			// sequential versions
			minVersionIncrease = 1;
			maxVersionIncrease = 2;
		} else {
			minVersionIncrease = deterministicRandom()->randomExp(0, 25);
			maxVersionIncrease = minVersionIncrease + deterministicRandom()->randomExp(0, 25);
		}
		if (deterministicRandom()->coinflip()) {
			targetMutationsPerDelta = 1;
		} else {
			targetMutationsPerDelta = deterministicRandom()->randomExp(1, 5);
		}

		if (deterministicRandom()->coinflip()) {
			cipherKeys = getCipherKeysCtx(ar);
		}
		if (deterministicRandom()->coinflip()) {
			compressFilter = CompressionUtils::getRandomFilter();
		}
	}

	Optional<StringRef> newKey() {
		for (int nAttempt = 0; nAttempt < 1000; nAttempt++) {
			size_t keySize = deterministicRandom()->randomInt(targetKeyLength / 2, targetKeyLength * 3 / 2);
			keySize = std::min(keySize, uidSize);
			std::string key = sharedPrefix + deterministicRandom()->randomUniqueID().toString().substr(0, keySize);
			if (usedKeys.insert(key).second) {
				StringRef k(ar, key);
				usedKeysList.push_back(k);
				return k;
			}
		}
		return {};
	}

	StringRef value() {
		int valueSize = deterministicRandom()->randomInt(targetValueLength / 2, targetValueLength * 3 / 2);
		std::string value = deterministicRandom()->randomUniqueID().toString();
		if (value.size() > valueSize) {
			value = value.substr(0, valueSize);
		}
		if (value.size() < valueSize) {
			// repeated string so it's compressible
			value += std::string(valueSize - value.size(), 'x');
		}
		return StringRef(ar, value);
	}

	KeyRef randomUsedKey() const { return usedKeysList[deterministicRandom()->randomInt(0, usedKeysList.size())]; }

	KeyRange randomKeyRange() const {
		ASSERT(!usedKeysList.empty());
		Key begin = randomUsedKey();
		if (deterministicRandom()->coinflip()) {
			begin = keyAfter(begin);
		}
		if (usedKeysList.size() == 1) {
			return KeyRange(KeyRangeRef(begin, keyAfter(begin)));
		} else {
			Key end = begin;
			while (end == begin) {
				end = randomUsedKey();
			}
			if (deterministicRandom()->coinflip()) {
				end = keyAfter(end);
			}
			if (begin < end) {
				return KeyRangeRef(begin, end);
			} else {
				return KeyRangeRef(end, begin);
			}
		}
	}

	StringRef keyForUpdate(double probUseExisting) {
		if (!usedKeysList.empty() && deterministicRandom()->random01() < probUseExisting) {
			return randomUsedKey();
		} else {
			auto k = newKey();
			if (k.present()) {
				return k.get();
			} else {
				// use existing key instead
				ASSERT(!usedKeysList.empty());
				return randomUsedKey();
			}
		}
	}

	Version nextVersion() {
		Version jump = deterministicRandom()->randomInt(minVersionIncrease, maxVersionIncrease);
		version += jump;
		return version;
	}

	MutationRef newMutation() {
		if (deterministicRandom()->random01() < clearFrequency) {
			// The algorithm for generating clears of varying sizes is, to generate clear sizes based on an exponential
			// distribution, such that the expected value of the clear size is 2.
			int clearWidth = 1;
			while (clearWidth < usedKeys.size() && deterministicRandom()->coinflip()) {
				clearWidth *= 2;
			}
			bool clearPastEnd = deterministicRandom()->coinflip();
			if (clearPastEnd) {
				clearWidth--;
			}
			StringRef begin = keyForUpdate(1.0 - clearUnsetFrequency);
			std::string beginStr = begin.toString();
			auto it = usedKeys.find(beginStr);
			ASSERT(it != usedKeys.end());
			while (it != usedKeys.end() && clearWidth > 0) {
				it++;
				clearWidth--;
			}
			if (it == usedKeys.end()) {
				it--;
				clearPastEnd = true;
			}
			std::string endKey = *it;
			if (clearPastEnd) {
				Key end = keyAfter(StringRef(ar, endKey));
				ar.dependsOn(end.arena());
				return MutationRef(MutationRef::ClearRange, begin, end);
			} else {
				// clear up to end
				return MutationRef(MutationRef::ClearRange, begin, StringRef(ar, endKey));
			}

		} else {
			return MutationRef(MutationRef::SetValue, keyForUpdate(updateExistingKeyFrequency), value());
		}
	}

	MutationsAndVersionRef newDelta() {
		Version v = nextVersion();
		int mutationCount = deterministicRandom()->randomInt(1, targetMutationsPerDelta * 2);
		MutationsAndVersionRef ret(v, v);
		for (int i = 0; i < mutationCount; i++) {
			ret.mutations.push_back(ar, newMutation());
		}
		return ret;
	}
};

} // namespace

Standalone<GranuleSnapshot> genSnapshot(KeyValueGen& kvGen, int targetDataBytes) {
	Standalone<GranuleSnapshot> data;
	int totalDataBytes = 0;
	while (totalDataBytes < targetDataBytes) {
		Optional<StringRef> key = kvGen.newKey();
		if (!key.present()) {
			break;
		}
		StringRef value = kvGen.value();

		data.push_back_deep(data.arena(), KeyValueRef(KeyRef(key.get()), ValueRef(value)));
		totalDataBytes += key.get().size() + value.size();
	}

	std::sort(data.begin(), data.end(), KeyValueRef::OrderByKey());
	return data;
}

Standalone<GranuleDeltas> genDeltas(KeyValueGen& kvGen, int targetBytes) {
	Standalone<GranuleDeltas> data;
	int totalDataBytes = 0;
	while (totalDataBytes < targetBytes) {
		data.push_back(data.arena(), kvGen.newDelta());
		totalDataBytes += data.back().expectedSize();
	}
	return data;
}

TEST_CASE("/blobgranule/files/validateEncryptionCompression") {
	KeyValueGen kvGen;

	int targetSnapshotChunks = deterministicRandom()->randomExp(0, 9);
	int targetDeltaChunks = deterministicRandom()->randomExp(0, 8);
	int targetDataBytes = deterministicRandom()->randomExp(12, 25);
	int targetSnapshotBytes = (int)(deterministicRandom()->randomInt(0, targetDataBytes));
	int targetDeltaBytes = targetDataBytes - targetSnapshotBytes;

	int targetSnapshotChunkSize = targetSnapshotBytes / targetSnapshotChunks;
	int targetDeltaChunkSize = targetDeltaBytes / targetDeltaChunks;

	Standalone<GranuleSnapshot> snapshotData = genSnapshot(kvGen, targetSnapshotBytes);
	Standalone<GranuleDeltas> deltaData = genDeltas(kvGen, targetDeltaBytes);
	fmt::print("{0} snapshot rows and {1} deltas\n", snapshotData.size(), deltaData.size());

	Standalone<StringRef> fileNameRef = StringRef();

	Arena ar;
	BlobGranuleCipherKeysCtx cipherKeys = getCipherKeysCtx(ar);
	std::vector<bool> encryptionModes = { false, true };
	std::vector<Optional<CompressionFilter>> compressionModes;
	compressionModes.insert(
	    compressionModes.end(), CompressionUtils::supportedFilters.begin(), CompressionUtils::supportedFilters.end());

	std::vector<Value> snapshotValues;
	for (bool encryptionMode : encryptionModes) {
		Optional<BlobGranuleCipherKeysCtx> keys = encryptionMode ? cipherKeys : Optional<BlobGranuleCipherKeysCtx>();
		for (auto& compressionMode : compressionModes) {
			Value v =
			    serializeChunkedSnapshot(fileNameRef, snapshotData, targetSnapshotChunkSize, compressionMode, keys);
			fmt::print("snapshot({0}, {1}): {2}\n",
			           encryptionMode,
			           compressionMode.present() ? CompressionUtils::toString(compressionMode.get()) : "",
			           v.size());
			for (auto& v2 : snapshotValues) {
				ASSERT(v != v2);
			}
			snapshotValues.push_back(v);
		}
	}
	fmt::print("Validated {0} encryption/compression combos for snapshot\n", snapshotValues.size());

	std::vector<Value> deltaValues;
	for (bool encryptionMode : encryptionModes) {
		Optional<BlobGranuleCipherKeysCtx> keys = encryptionMode ? cipherKeys : Optional<BlobGranuleCipherKeysCtx>();
		for (auto& compressionMode : compressionModes) {
			Value v = serializeChunkedDeltaFile(
			    fileNameRef, deltaData, kvGen.allRange, targetDeltaChunkSize, compressionMode, keys);
			fmt::print("delta({0}, {1}): {2}\n",
			           encryptionMode,
			           compressionMode.present() ? CompressionUtils::toString(compressionMode.get()) : "",
			           v.size());
			for (auto& v2 : deltaValues) {
				ASSERT(v != v2);
			}
			deltaValues.push_back(v);
		}
	}
	fmt::print("Validated {0} encryption/compression combos for delta\n", deltaValues.size());

	return Void();
}

TEST_CASE("/blobgranule/files/snapshotFormatUnitTest") {
	// snapshot files are likely to have a non-trivial shared prefix since they're for a small contiguous key range
	KeyValueGen kvGen;

	int targetChunks = deterministicRandom()->randomExp(0, 9);
	int targetDataBytes = deterministicRandom()->randomExp(0, 25);
	int targetChunkSize = targetDataBytes / targetChunks;
	Standalone<StringRef> fnameRef = StringRef(std::string("test"));

	Standalone<GranuleSnapshot> data = genSnapshot(kvGen, targetDataBytes);

	int maxExp = 0;
	while (1 << maxExp < data.size()) {
		maxExp++;
	}
	maxExp--;

	fmt::print("Validating snapshot data is sorted\n");
	for (int i = 0; i < data.size() - 1; i++) {
		ASSERT(data[i].key < data[i + 1].key);
	}

	fmt::print("Constructing snapshot with {0} rows, {1} chunks\n", data.size(), targetChunks);

	Value serialized =
	    serializeChunkedSnapshot(fnameRef, data, targetChunkSize, kvGen.compressFilter, kvGen.cipherKeys);

	fmt::print("Snapshot serialized! {0} bytes\n", serialized.size());

	fmt::print("Validating snapshot data is sorted again\n");
	for (int i = 0; i < data.size() - 1; i++) {
		ASSERT(data[i].key < data[i + 1].key);
	}

	fmt::print("Initial read starting\n");

	checkSnapshotRead(fnameRef, data, serialized, 0, data.size(), kvGen.cipherKeys);

	fmt::print("Initial read complete\n");

	if (data.size() > 1) {
		for (int i = 0; i < std::min(100, data.size() * 2); i++) {
			int width = deterministicRandom()->randomExp(0, maxExp);
			ASSERT(width <= data.size());
			int start = deterministicRandom()->randomInt(0, data.size() - width);
			checkSnapshotRead(fnameRef, data, serialized, start, start + width, kvGen.cipherKeys);
		}

		fmt::print("Doing empty checks\n");
		int randomIdx = deterministicRandom()->randomInt(0, data.size() - 1);
		checkSnapshotEmpty(serialized, keyAfter(data[randomIdx].key), data[randomIdx + 1].key, kvGen.cipherKeys);
	} else {
		fmt::print("Doing empty checks\n");
	}

	checkSnapshotEmpty(serialized, normalKeys.begin, data.front().key, kvGen.cipherKeys);
	checkSnapshotEmpty(serialized, normalKeys.begin, "\x00"_sr, kvGen.cipherKeys);
	checkSnapshotEmpty(serialized, keyAfter(data.back().key), normalKeys.end, kvGen.cipherKeys);
	checkSnapshotEmpty(serialized, "\xfe"_sr, normalKeys.end, kvGen.cipherKeys);

	fmt::print("Snapshot format test done!\n");

	return Void();
}

void checkDeltaRead(const KeyValueGen& kvGen,
                    const KeyRangeRef& range,
                    Version beginVersion,
                    Version readVersion,
                    const Standalone<GranuleDeltas>& data,
                    StringRef* serialized) {
	// expected answer
	std::map<KeyRef, ValueRef> expectedData;
	Version lastFileEndVersion = 0;
	GranuleMaterializeStats stats;

	fmt::print("Delta Read [{0} - {1}) @ {2} - {3}\n",
	           range.begin.printable(),
	           range.end.printable(),
	           beginVersion,
	           readVersion);

	applyDeltasByVersion(data, range, beginVersion, readVersion, lastFileEndVersion, expectedData);

	// actual answer
	std::string filename = randomBGFilename(
	    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUniqueID(), readVersion, ".delta");
	Standalone<BlobGranuleChunkRef> chunk;
	chunk.deltaFiles.emplace_back_deep(
	    chunk.arena(), filename, 0, serialized->size(), serialized->size(), kvGen.cipherKeys);
	chunk.keyRange = kvGen.allRange;
	chunk.includedVersion = readVersion;
	chunk.snapshotVersion = invalidVersion;

	RangeResult actualData = materializeBlobGranule(chunk, range, beginVersion, readVersion, {}, serialized, stats);

	if (expectedData.size() != actualData.size()) {
		fmt::print("Expected Data {0}:\n", expectedData.size());
		/*for (auto& it : expectedData) {
		    fmt::print("  {0}=\n", it.first.printable());
		}*/
		fmt::print("Actual Data {0}:\n", actualData.size());
		/*for (auto& it : actualData) {
		    fmt::print("  {0}=\n", it.key.printable());
		}*/
	}

	ASSERT(expectedData.size() == actualData.size());
	int i = 0;
	for (auto& it : expectedData) {
		ASSERT(it.first == actualData[i].key);
		ASSERT(it.second == actualData[i].value);
		i++;
	}
}

static std::tuple<KeyRange, Version, Version> randomizeKeyAndVersions(const KeyValueGen& kvGen,
                                                                      const Standalone<GranuleDeltas> data) {
	// either randomize just keyrange, just version range, or both
	double rand = deterministicRandom()->randomInt(0, 3);
	bool randomizeKeyRange = rand == 0 || rand == 2;
	bool randomizeVersionRange = rand == 1 || rand == 2;
	KeyRange readRange = kvGen.allRange;
	Version beginVersion = 0;
	Version readVersion = data.back().version;

	if (randomizeKeyRange) {
		readRange = kvGen.randomKeyRange();
	}

	if (randomizeVersionRange) {
		if (deterministicRandom()->coinflip()) {
			beginVersion = 0;
		} else {
			beginVersion = data[deterministicRandom()->randomInt(0, data.size())].version;
			beginVersion += deterministicRandom()->randomInt(0, 3) - 1; // randomize between -1, 0, and +1
		}
		readVersion = data[deterministicRandom()->randomInt(0, data.size())].version;
		readVersion += deterministicRandom()->randomInt(0, 3) - 1; // randomize between -1, 0, and +1
		if (readVersion < beginVersion) {
			std::swap(beginVersion, readVersion);
		}
	}

	return { readRange, beginVersion, readVersion };
}

TEST_CASE("/blobgranule/files/deltaFormatUnitTest") {
	KeyValueGen kvGen;
	Standalone<StringRef> fileNameRef = StringRef(std::string("test"));

	int targetChunks = deterministicRandom()->randomExp(0, 8);
	int targetDataBytes = deterministicRandom()->randomExp(0, 21);
	int targetChunkSize = targetDataBytes / targetChunks;

	Standalone<GranuleDeltas> data = genDeltas(kvGen, targetDataBytes);

	fmt::print("Deltas ({0})\n", data.size());
	/*for (auto& it : data) {
	    fmt::print("  {0}) ({1})\n", it.version, it.mutations.size());
	    for (auto& it2 : it.mutations) {
	        if (it2.type == MutationRef::Type::SetValue) {
	            fmt::print("    {0}=\n", it2.param1.printable());
	        } else {
	            fmt::print("    {0} - {1}\n", it2.param1.printable(), it2.param2.printable());
	        }
	    }
	}*/
	Value serialized = serializeChunkedDeltaFile(
	    fileNameRef, data, kvGen.allRange, targetChunkSize, kvGen.compressFilter, kvGen.cipherKeys);

	// check whole file
	checkDeltaRead(kvGen, kvGen.allRange, 0, data.back().version, data, &serialized);

	for (int i = 0; i < std::min((size_t)100, kvGen.usedKeysList.size() * data.size()); i++) {
		auto params = randomizeKeyAndVersions(kvGen, data);
		checkDeltaRead(kvGen, std::get<0>(params), std::get<1>(params), std::get<2>(params), data, &serialized);
	}

	return Void();
}

void checkGranuleRead(const KeyValueGen& kvGen,
                      const KeyRangeRef& range,
                      Version beginVersion,
                      Version readVersion,
                      const Standalone<GranuleSnapshot>& snapshotData,
                      const Standalone<GranuleDeltas>& deltaData,
                      const Value& serializedSnapshot,
                      const std::vector<std::pair<Version, Value>>& serializedDeltas,
                      const Standalone<GranuleDeltas>& inMemoryDeltas) {
	// expected answer
	std::map<KeyRef, ValueRef> expectedData;
	if (beginVersion == 0) {
		for (auto& it : snapshotData) {
			if (range.contains(it.key)) {
				expectedData.insert({ it.key, it.value });
			}
		}
	}
	Version lastFileEndVersion = 0;
	applyDeltasByVersion(deltaData, range, beginVersion, readVersion, lastFileEndVersion, expectedData);
	GranuleMaterializeStats stats;

	// actual answer
	Standalone<BlobGranuleChunkRef> chunk;
	if (beginVersion == 0) {
		std::string snapshotFilename = randomBGFilename(
		    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUniqueID(), 0, ".snapshot");
		chunk.snapshotFile = BlobFilePointerRef(
		    chunk.arena(), snapshotFilename, 0, serializedSnapshot.size(), serializedSnapshot.size(), kvGen.cipherKeys);
	}
	int deltaIdx = 0;
	while (deltaIdx < serializedDeltas.size() && serializedDeltas[deltaIdx].first < beginVersion) {
		deltaIdx++;
	}
	std::vector<StringRef> deltaPtrsVector;
	while (deltaIdx < serializedDeltas.size()) {
		std::string deltaFilename = randomBGFilename(
		    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUniqueID(), readVersion, ".delta");
		size_t fsize = serializedDeltas[deltaIdx].second.size();
		chunk.deltaFiles.emplace_back_deep(chunk.arena(), deltaFilename, 0, fsize, fsize, kvGen.cipherKeys);
		deltaPtrsVector.push_back(serializedDeltas[deltaIdx].second);

		if (serializedDeltas[deltaIdx].first >= readVersion) {
			break;
		}
		deltaIdx++;
	}
	StringRef deltaPtrs[deltaPtrsVector.size()];
	for (int i = 0; i < deltaPtrsVector.size(); i++) {
		deltaPtrs[i] = deltaPtrsVector[i];
	}

	// add in memory deltas
	chunk.arena().dependsOn(inMemoryDeltas.arena());
	for (auto& it : inMemoryDeltas) {
		if (beginVersion <= it.version && it.version <= readVersion) {
			chunk.newDeltas.push_back(chunk.arena(), it);
		}
	}

	chunk.keyRange = kvGen.allRange;
	chunk.includedVersion = readVersion;
	chunk.snapshotVersion = (beginVersion == 0) ? 0 : invalidVersion;

	Optional<StringRef> snapshotPtr;
	if (beginVersion == 0) {
		snapshotPtr = serializedSnapshot;
	}
	RangeResult actualData =
	    materializeBlobGranule(chunk, range, beginVersion, readVersion, snapshotPtr, deltaPtrs, stats);

	if (expectedData.size() != actualData.size()) {
		fmt::print("Expected Size {0} != Actual Size {1}\n", expectedData.size(), actualData.size());
	}
	if (BG_FILES_TEST_DEBUG) {
		fmt::print("Expected Data {0}:\n", expectedData.size());
		for (auto& it : expectedData) {
			fmt::print("  {0}=\n", it.first.printable());
		}
		fmt::print("Actual Data {0}:\n", actualData.size());
		for (auto& it : actualData) {
			fmt::print("  {0}=\n", it.key.printable());
		}
	}

	ASSERT(expectedData.size() == actualData.size());
	int i = 0;
	for (auto& it : expectedData) {
		if (it.first != actualData[i].key) {
			fmt::print("expected {0} != actual {1}\n", it.first.printable(), actualData[i].key.printable());
		}
		ASSERT(it.first == actualData[i].key);
		ASSERT(it.second == actualData[i].value);
		i++;
	}
}

TEST_CASE("/blobgranule/files/granuleReadUnitTest") {
	KeyValueGen kvGen;
	Standalone<StringRef> fileNameRef = StringRef(std::string("testSnap"));

	int targetSnapshotChunks = deterministicRandom()->randomExp(0, 9);
	int targetDeltaChunks = deterministicRandom()->randomExp(0, 8);
	int targetDataBytes = deterministicRandom()->randomExp(12, 25);
	int targetSnapshotBytes = (int)(deterministicRandom()->randomInt(0, targetDataBytes));
	int targetDeltaBytes = targetDataBytes - targetSnapshotBytes;

	if (BG_FILES_TEST_DEBUG) {
		fmt::print("Snapshot Chunks: {0}\nDelta Chunks: {1}\nSnapshot Bytes: {2}\nDelta Bytes: {3}\n",
		           targetSnapshotChunks,
		           targetDeltaChunks,
		           targetSnapshotBytes,
		           targetDeltaBytes);
	}

	int targetSnapshotChunkSize = targetSnapshotBytes / targetSnapshotChunks;
	int targetDeltaChunkSize = targetDeltaBytes / targetDeltaChunks;

	Standalone<GranuleSnapshot> snapshotData = genSnapshot(kvGen, targetSnapshotBytes);
	if (BG_FILES_TEST_DEBUG) {
		fmt::print("Snapshot data: {0}\n", snapshotData.size());
		for (auto& it : snapshotData) {
			fmt::print("  {0}=\n", it.key.printable());
		}
	}
	Standalone<GranuleDeltas> deltaData = genDeltas(kvGen, targetDeltaBytes);
	fmt::print("{0} snapshot rows and {1} deltas\n", snapshotData.size(), deltaData.size());

	if (BG_FILES_TEST_DEBUG) {
		fmt::print("Delta data: {0}\n", deltaData.size());
		for (auto& it : deltaData) {
			fmt::print("  {0}) ({1})\n", it.version, it.mutations.size());
			for (auto& it2 : it.mutations) {
				if (it2.type == MutationRef::Type::SetValue) {
					fmt::print("    {0}=\n", it2.param1.printable());
				} else {
					fmt::print("    {0} - {1}\n", it2.param1.printable(), it2.param2.printable());
				}
			}
		}
	}

	Value serializedSnapshot = serializeChunkedSnapshot(
	    fileNameRef, snapshotData, targetSnapshotChunkSize, kvGen.compressFilter, kvGen.cipherKeys);

	// split deltas up across multiple files
	int deltaFiles = std::min(deltaData.size(), deterministicRandom()->randomInt(1, 21));
	int deltasPerFile = deltaData.size() / deltaFiles + 1;
	std::vector<std::pair<Version, Value>> serializedDeltaFiles;
	Standalone<GranuleDeltas> inMemoryDeltas;
	serializedDeltaFiles.reserve(deltaFiles);
	for (int i = 0; i < deltaFiles; i++) {
		Standalone<GranuleDeltas> fileData;
		int j;
		for (j = i * deltasPerFile; j < (i + 1) * deltasPerFile && j < deltaData.size(); j++) {
			fileData.push_back_deep(fileData.arena(), deltaData[j]);
		}
		if (!fileData.empty()) {
			if (j == deltaData.size() && deterministicRandom()->coinflip()) {
				// if it's the last set of deltas, sometimes make them the memory deltas instead
				fmt::print("Memory Deltas {0} - {1}\n", fileData.front().version, fileData.back().version);
				inMemoryDeltas = fileData;
			} else {
				fmt::print("Delta file {0} - {1}\n", fileData.front().version, fileData.back().version);
				Standalone<StringRef> fileNameRef = StringRef("delta" + std::to_string(i));
				Value serializedDelta = serializeChunkedDeltaFile(fileNameRef,
				                                                  fileData,
				                                                  kvGen.allRange,
				                                                  targetDeltaChunkSize,
				                                                  kvGen.compressFilter,
				                                                  kvGen.cipherKeys);
				serializedDeltaFiles.emplace_back(fileData.back().version, serializedDelta);
			}
		}
	}

	fmt::print("Full test\n");
	checkGranuleRead(kvGen,
	                 kvGen.allRange,
	                 0,
	                 deltaData.back().version,
	                 snapshotData,
	                 deltaData,
	                 serializedSnapshot,
	                 serializedDeltaFiles,
	                 inMemoryDeltas);

	// prevent overflow by doing min before multiply
	int maxRuns = 100;
	int snapshotAndDeltaSize = 5 + std::min(maxRuns, snapshotData.size()) * std::min(maxRuns, deltaData.size());
	int lim = std::min(maxRuns, snapshotAndDeltaSize);
	for (int i = 0; i < lim; i++) {
		auto params = randomizeKeyAndVersions(kvGen, deltaData);
		fmt::print("Partial test {0}: [{1} - {2}) @ {3} - {4}\n",
		           i,
		           std::get<0>(params).begin.printable(),
		           std::get<0>(params).end.printable(),
		           std::get<1>(params),
		           std::get<2>(params));
		checkGranuleRead(kvGen,
		                 std::get<0>(params),
		                 std::get<1>(params),
		                 std::get<2>(params),
		                 snapshotData,
		                 deltaData,
		                 serializedSnapshot,
		                 serializedDeltaFiles,
		                 inMemoryDeltas);
	}

	return Void();
}

// performance micro-benchmarks

struct FileSet {
	std::tuple<std::string, Version, Value, Standalone<GranuleSnapshot>> snapshotFile;
	std::vector<std::tuple<std::string, Version, Value, Standalone<GranuleDeltas>>> deltaFiles;
	Key commonPrefix;
	KeyRange range;
};

std::pair<std::string, Version> parseFilename(const std::string& fname) {
	auto dotPos = fname.find(".");
	ASSERT(dotPos > 0);
	std::string type = fname.substr(dotPos + 1);
	ASSERT(type == "snapshot" || type == "delta");
	auto lastUnderscorePos = fname.rfind("_");
	ASSERT('V' == fname[lastUnderscorePos + 1]);
	std::string versionString = fname.substr(lastUnderscorePos + 2, dotPos);
	Version version = std::stoll(versionString);
	return { type, version };
}

Value loadFileData(std::string filename) {
	std::ifstream input(filename, std::ios::binary);
	ASSERT(input.good());

	// copies all data into buffer
	std::vector<uint8_t> buffer(std::istreambuf_iterator<char>(input), {});
	Value v(StringRef(&buffer[0], buffer.size()));
	fmt::print("Loaded {0} file bytes from {1}\n", v.size(), filename);

	input.close();
	return v;
}

struct CommonPrefixStats {
	// for computing common prefix details and stats
	Key key;
	int len = -1;
	int64_t totalKeySize = 0;
	int totalKeys = 0;
	int minKeySize = 1000000000;
	int maxKeySize = 0;
	int64_t logicalBytes = 0;
	int64_t totalLogicalBytes = 0;

	int deltas = 0;
	int deltasSet = 0;
	int deltasClear = 0;
	int deltasNoOp = 0;
	int deltasClearAfter = 0;

	void addKey(const KeyRef& k) {
		if (len == -1) {
			key = k;
			len = k.size();
		} else {
			len = std::min(len, commonPrefixLength(k, key));
		}
		totalKeys++;
		totalKeySize += k.size();
		minKeySize = std::min(minKeySize, k.size());
		maxKeySize = std::max(maxKeySize, k.size());
	}

	void addKeyValue(const KeyRef& k, const ValueRef& v) {
		addKey(k);
		logicalBytes += k.size();
		logicalBytes += v.size();
	}

	void addBoundary(const ParsedDeltaBoundaryRef& d) {
		addKey(d.key);

		deltas++;
		if (d.isSet()) {
			deltasSet++;
			logicalBytes += d.value.size();
		} else if (d.isClear()) {
			deltasClear++;
		} else {
			ASSERT(d.isNoOp());
			deltasNoOp++;
		}
		if (d.clearAfter) {
			deltasClearAfter++;
		}
	}

	void doneFile() {
		totalLogicalBytes += logicalBytes;
		fmt::print("Logical Size: {0}\n", logicalBytes);
		logicalBytes = 0;
	}

	Key done() {
		doneFile();
		ASSERT(len >= 0);
		fmt::print("Common prefix: {0}\nCommon Prefix Length: {1}\nAverage Key Size: {2}\nMin Key Size: {3}, Max Key "
		           "Size: {4}\n",
		           key.substr(0, len).printable(),
		           len,
		           totalKeySize / totalKeys,
		           minKeySize,
		           maxKeySize);

		if (deltas > 0) {
			fmt::print("Delta stats: {0} deltas, {1} sets, {2} clears, {3} noops, {4} clearAfters\n",
			           deltas,
			           deltasSet,
			           deltasClear,
			           deltasNoOp,
			           deltasClearAfter);
		}
		fmt::print("Logical Size: {0}\n", totalLogicalBytes);
		return key.substr(0, len);
	}
};

FileSet loadFileSet(std::string basePath, const std::vector<std::string>& filenames, bool newFormat) {
	FileSet files;
	CommonPrefixStats stats;
	for (int i = 0; i < filenames.size(); i++) {
		auto parts = parseFilename(filenames[i]);
		std::string type = parts.first;
		Version version = parts.second;
		if (type == "snapshot") {
			std::string fpath = basePath + filenames[i];
			Value data = loadFileData(fpath);

			Standalone<GranuleSnapshot> parsed;
			if (!newFormat) {
				Arena arena;
				GranuleSnapshot file;
				ObjectReader dataReader(data.begin(), Unversioned());
				dataReader.deserialize(FileIdentifierFor<GranuleSnapshot>::value, file, arena);
				parsed = Standalone<GranuleSnapshot>(file, arena);
				fmt::print("Loaded {0} rows from snapshot file\n", parsed.size());

				for (auto& it : parsed) {
					stats.addKeyValue(it.key, it.value);
				}
			} else {
				Standalone<VectorRef<ParsedDeltaBoundaryRef>> res = loadSnapshotFile(""_sr, data, normalKeys, {});
				fmt::print("Loaded {0} rows from snapshot file\n", res.size());
				for (auto& it : res) {
					stats.addKeyValue(it.key, it.value);
				}
			}

			files.snapshotFile = { filenames[i], version, data, parsed };

		} else {
			std::string fpath = basePath + filenames[i];
			Value data = loadFileData(fpath);

			if (!newFormat) {
				Arena arena;
				GranuleDeltas file;
				ObjectReader dataReader(data.begin(), Unversioned());
				dataReader.deserialize(FileIdentifierFor<GranuleDeltas>::value, file, arena);
				Standalone<GranuleDeltas> parsed(file, arena);

				fmt::print("Loaded {0} deltas from delta file\n", parsed.size());
				files.deltaFiles.push_back({ filenames[i], version, data, parsed });

				for (auto& it : parsed) {
					for (auto& it2 : it.mutations) {
						stats.addKey(it2.param1);
						if (it2.type == MutationRef::Type::ClearRange) {
							stats.addKey(it2.param2);
						}
					}
				}
			} else {
				bool startClear = false;
				Standalone<VectorRef<ParsedDeltaBoundaryRef>> res =
				    loadChunkedDeltaFile(""_sr, data, normalKeys, 0, version, {}, startClear);
				ASSERT(!startClear);

				Standalone<GranuleDeltas> parsed;
				fmt::print("Loaded {0} boundaries from delta file\n", res.size());
				files.deltaFiles.push_back({ filenames[i], version, data, parsed });

				for (auto& it : res) {
					stats.addBoundary(it);
				}
			}
		}
		stats.doneFile();
	}

	files.commonPrefix = stats.done();
	if (files.commonPrefix.size() == 0) {
		files.range = normalKeys;
	} else {
		files.range = KeyRangeRef(files.commonPrefix, strinc(files.commonPrefix));
	}
	fmt::print("Range: [{0} - {1})\n", files.range.begin.printable(), files.range.end.printable());

	return files;
}

int WRITE_RUNS = 5;

std::pair<int64_t, double> doSnapshotWriteBench(const Standalone<GranuleSnapshot>& data,
                                                bool chunked,
                                                Optional<BlobGranuleCipherKeysCtx> cipherKeys,
                                                Optional<CompressionFilter> compressionFilter) {
	Standalone<StringRef> fileNameRef = StringRef();
	int64_t serializedBytes = 0;
	double elapsed = -timer_monotonic();
	for (int runI = 0; runI < WRITE_RUNS; runI++) {
		if (!chunked) {
			serializedBytes = ObjectWriter::toValue(data, Unversioned()).size();
		} else {
			serializedBytes =
			    serializeChunkedSnapshot(fileNameRef, data, 64 * 1024, compressionFilter, cipherKeys).size();
		}
	}
	elapsed += timer_monotonic();
	elapsed /= WRITE_RUNS;
	return { serializedBytes, elapsed };
}

std::pair<int64_t, double> doDeltaWriteBench(const Standalone<GranuleDeltas>& data,
                                             const KeyRangeRef& fileRange,
                                             bool chunked,
                                             Optional<BlobGranuleCipherKeysCtx> cipherKeys,
                                             Optional<CompressionFilter> compressionFilter) {
	Standalone<StringRef> fileNameRef = StringRef();
	int64_t serializedBytes = 0;
	double elapsed = -timer_monotonic();
	for (int runI = 0; runI < WRITE_RUNS; runI++) {
		if (!chunked) {
			serializedBytes = ObjectWriter::toValue(data, Unversioned()).size();
		} else {
			serializedBytes =
			    serializeChunkedDeltaFile(fileNameRef, data, fileRange, 32 * 1024, compressionFilter, cipherKeys)
			        .size();
		}
	}
	elapsed += timer_monotonic();
	elapsed /= WRITE_RUNS;
	return { serializedBytes, elapsed };
}

void chunkFromFileSet(const FileSet& fileSet,
                      Standalone<BlobGranuleChunkRef>& chunk,
                      StringRef* deltaPtrs,
                      Version readVersion,
                      Optional<BlobGranuleCipherKeysCtx> keys,
                      int numDeltaFiles) {
	size_t snapshotSize = std::get<3>(fileSet.snapshotFile).size();
	chunk.snapshotFile =
	    BlobFilePointerRef(chunk.arena(), std::get<0>(fileSet.snapshotFile), 0, snapshotSize, snapshotSize, keys);

	for (int i = 0; i < numDeltaFiles; i++) {
		size_t deltaSize = std::get<3>(fileSet.deltaFiles[i]).size();
		chunk.deltaFiles.emplace_back_deep(
		    chunk.arena(), std::get<0>(fileSet.deltaFiles[i]), 0, deltaSize, deltaSize, keys);
		deltaPtrs[i] = std::get<2>(fileSet.deltaFiles[i]);
	}

	chunk.keyRange = fileSet.range;
	chunk.includedVersion = readVersion;
	chunk.snapshotVersion = std::get<1>(fileSet.snapshotFile);
}

FileSet rewriteChunkedFileSet(const FileSet& fileSet,
                              Optional<BlobGranuleCipherKeysCtx> keys,
                              Optional<CompressionFilter> compressionFilter) {
	Standalone<StringRef> fileNameRef = StringRef();
	FileSet newFiles;
	newFiles.snapshotFile = fileSet.snapshotFile;
	newFiles.deltaFiles = fileSet.deltaFiles;
	newFiles.commonPrefix = fileSet.commonPrefix;
	newFiles.range = fileSet.range;

	std::get<2>(newFiles.snapshotFile) =
	    serializeChunkedSnapshot(fileNameRef, std::get<3>(newFiles.snapshotFile), 64 * 1024, compressionFilter, keys);
	for (auto& deltaFile : newFiles.deltaFiles) {
		std::get<2>(deltaFile) = serializeChunkedDeltaFile(
		    fileNameRef, std::get<3>(deltaFile), fileSet.range, 32 * 1024, compressionFilter, keys);
	}

	return newFiles;
}

int READ_RUNS = 20;
std::pair<int64_t, double> doReadBench(const FileSet& fileSet,
                                       bool chunked,
                                       KeyRange readRange,
                                       bool clearAllAtEnd,
                                       Optional<BlobGranuleCipherKeysCtx> keys,
                                       int numDeltaFiles,
                                       bool printStats = false) {
	Version readVersion = std::get<1>(fileSet.deltaFiles.back());

	Standalone<BlobGranuleChunkRef> chunk;
	GranuleMaterializeStats stats;
	ASSERT(numDeltaFiles >= 0 && numDeltaFiles <= fileSet.deltaFiles.size());
	StringRef deltaPtrs[numDeltaFiles];

	MutationRef clearAllAtEndMutation;
	if (clearAllAtEnd) {
		clearAllAtEndMutation = MutationRef(MutationRef::Type::ClearRange, readRange.begin, readRange.end);
	}
	if (chunked) {
		chunkFromFileSet(fileSet, chunk, deltaPtrs, readVersion, keys, numDeltaFiles);
		if (clearAllAtEnd) {
			readVersion++;
			MutationsAndVersionRef lastDelta;
			lastDelta.version = readVersion;
			lastDelta.mutations.push_back(chunk.arena(), clearAllAtEndMutation);
			chunk.includedVersion = readVersion;

			chunk.newDeltas.push_back_deep(chunk.arena(), lastDelta);
		}
	}

	int64_t serializedBytes = 0;
	double elapsed = -timer_monotonic();
	for (int runI = 0; runI < READ_RUNS; runI++) {
		if (!chunked) {
			std::map<KeyRef, ValueRef> data;
			for (auto& it : std::get<3>(fileSet.snapshotFile)) {
				data.insert({ it.key, it.value });
			}
			Version lastFileEndVersion = 0;
			for (auto& deltaFile : fileSet.deltaFiles) {
				applyDeltasByVersion(std::get<3>(deltaFile), readRange, 0, readVersion, lastFileEndVersion, data);
			}
			if (clearAllAtEnd) {
				applyDelta(readRange, clearAllAtEndMutation, data);
			}
			RangeResult actualData;
			for (auto& it : data) {
				actualData.push_back_deep(actualData.arena(), KeyValueRef(it.first, it.second));
			}
			serializedBytes += actualData.expectedSize();
		} else {
			RangeResult actualData = materializeBlobGranule(
			    chunk, readRange, 0, readVersion, std::get<2>(fileSet.snapshotFile), deltaPtrs, stats);
			serializedBytes += actualData.expectedSize();
		}
	}
	elapsed += timer_monotonic();
	elapsed /= READ_RUNS;
	serializedBytes /= READ_RUNS;

	if (printStats) {
		fmt::print("Materialize stats:\n");
		fmt::print("  Input bytes:  {0}\n", stats.inputBytes / READ_RUNS);
		fmt::print("  Output bytes: {0}\n", stats.outputBytes / READ_RUNS);
		fmt::print("    Write Amp:  {0}\n", (1.0 * stats.inputBytes) / stats.outputBytes);
		fmt::print("  Snapshot Rows: {0}\n", stats.snapshotRows / READ_RUNS);
		fmt::print("  Rows Cleared:  {0}\n", stats.rowsCleared / READ_RUNS);
		fmt::print("  Rows Inserted: {0}\n", stats.rowsInserted / READ_RUNS);
		fmt::print("  Rows Updated:  {0}\n", stats.rowsUpdated / READ_RUNS);
	}

	return { serializedBytes, elapsed };
}

void printMetrics(int64_t diskBytes, double elapsed, int64_t processesBytes, int64_t logicalSize) {
	double storageAmp = (1.0 * diskBytes) / logicalSize;

	double MBperCPUsec = (elapsed == 0.0) ? 0.0 : (processesBytes / 1024.0 / 1024.0) / elapsed;
	fmt::print("{}", fmt::format(" {:.6} {:.6}", storageAmp, MBperCPUsec));
}

TEST_CASE("!/blobgranule/files/benchFromFiles") {
	std::string basePath = "SET_ME";
	std::vector<std::vector<std::string>> fileSetNames = { { "SET_ME" } };
	Arena ar;
	BlobGranuleCipherKeysCtx cipherKeys = getCipherKeysCtx(ar);
	std::vector<bool> chunkModes = { false, true };
	std::vector<bool> encryptionModes = { false, true };
	std::vector<Optional<CompressionFilter>> compressionModes;
	compressionModes.push_back({});
	compressionModes.insert(
	    compressionModes.end(), CompressionUtils::supportedFilters.begin(), CompressionUtils::supportedFilters.end());

	std::vector<std::string> runNames = { "logical" };
	std::vector<std::pair<int64_t, double>> snapshotMetrics;
	std::vector<std::pair<int64_t, double>> deltaMetrics;

	std::vector<FileSet> fileSets;
	int64_t logicalSnapshotSize = 0;
	int64_t logicalDeltaSize = 0;
	for (auto& it : fileSetNames) {
		FileSet fileSet = loadFileSet(basePath, it, false);
		fileSets.push_back(fileSet);
		logicalSnapshotSize += std::get<3>(fileSet.snapshotFile).expectedSize();
		for (auto& deltaFile : fileSet.deltaFiles) {
			logicalDeltaSize += std::get<3>(deltaFile).expectedSize();
		}
	}
	snapshotMetrics.push_back({ logicalSnapshotSize, 0.0 });
	deltaMetrics.push_back({ logicalDeltaSize, 0.0 });

	for (bool chunk : chunkModes) {
		for (bool encrypt : encryptionModes) {
			if (!chunk && encrypt) {
				continue;
			}
			Optional<BlobGranuleCipherKeysCtx> keys = encrypt ? cipherKeys : Optional<BlobGranuleCipherKeysCtx>();
			for (auto& compressionFilter : compressionModes) {
				if (!chunk && compressionFilter.present()) {
					continue;
				}
				if (compressionFilter.present() && CompressionFilter::NONE == compressionFilter.get()) {
					continue;
				}

				std::string name;
				if (!chunk) {
					name = "old";
				} else {
					if (encrypt) {
						name += "ENC";
					}
					if (compressionFilter.present() && compressionFilter.get() != CompressionFilter::NONE) {
						name += "CMP";
					}
					if (name.empty()) {
						name = "chunked";
					}
				}
				runNames.push_back(name);
				int64_t snapshotTotalBytes = 0;
				double snapshotTotalElapsed = 0.0;
				for (auto& fileSet : fileSets) {
					auto res = doSnapshotWriteBench(std::get<3>(fileSet.snapshotFile), chunk, keys, compressionFilter);
					snapshotTotalBytes += res.first;
					snapshotTotalElapsed += res.second;
				}
				snapshotMetrics.push_back({ snapshotTotalBytes, snapshotTotalElapsed });

				int64_t deltaTotalBytes = 0;
				double deltaTotalElapsed = 0.0;
				for (auto& fileSet : fileSets) {
					for (auto& deltaFile : fileSet.deltaFiles) {
						auto res =
						    doDeltaWriteBench(std::get<3>(deltaFile), fileSet.range, chunk, keys, compressionFilter);
						deltaTotalBytes += res.first;
						deltaTotalElapsed += res.second;
					}
				}
				deltaMetrics.push_back({ deltaTotalBytes, deltaTotalElapsed });
			}
		}
	}

	fmt::print("\n\n\n\nWrite Results:\n");

	ASSERT(runNames.size() == snapshotMetrics.size());
	ASSERT(runNames.size() == deltaMetrics.size());
	for (int i = 0; i < runNames.size(); i++) {
		fmt::print("{0}", runNames[i]);

		printMetrics(
		    snapshotMetrics[i].first, snapshotMetrics[i].second, snapshotMetrics[i].first, snapshotMetrics[0].first);
		printMetrics(deltaMetrics[i].first, deltaMetrics[i].second, deltaMetrics[i].first, deltaMetrics[0].first);

		int64_t logicalTotalBytes = snapshotMetrics[0].first + deltaMetrics[0].first;
		int64_t totalBytes = deltaMetrics[i].first + snapshotMetrics[i].first;
		double logicalTotalElapsed = (snapshotMetrics[i].second == 0.0 || deltaMetrics[i].second == 0.0)
		                                 ? 0.0
		                                 : snapshotMetrics[i].second + deltaMetrics[i].second;
		printMetrics(totalBytes, logicalTotalElapsed, deltaMetrics[i].first, logicalTotalBytes);

		fmt::print("\n");
	}

	std::vector<std::string> readRunNames = {};
	std::vector<std::pair<int64_t, double>> readMetrics;

	bool doEdgeCaseReadTests = false;
	bool doVaryingDeltaTests = false;
	std::vector<double> clearAllReadMetrics;
	std::vector<double> readSingleKeyMetrics;
	std::vector<std::vector<std::pair<int64_t, double>>> varyingDeltaMetrics;

	size_t maxDeltaFiles = 100000;
	for (auto& f : fileSets) {
		maxDeltaFiles = std::min(maxDeltaFiles, f.deltaFiles.size());
	}

	for (bool chunk : chunkModes) {
		for (bool encrypt : encryptionModes) {
			if (!chunk && encrypt) {
				continue;
			}

			Optional<BlobGranuleCipherKeysCtx> keys = encrypt ? cipherKeys : Optional<BlobGranuleCipherKeysCtx>();
			for (auto& compressionFilter : compressionModes) {
				if (!chunk && compressionFilter.present()) {
					continue;
				}
				if (compressionFilter.present() && CompressionFilter::NONE == compressionFilter.get()) {
					continue;
				}
				std::string name;
				if (!chunk) {
					name = "old";
				} else {
					if (encrypt) {
						name += "ENC";
					}
					if (compressionFilter.present() && compressionFilter.get() != CompressionFilter::NONE) {
						name += "CMP";
					}
					if (name.empty()) {
						name = "chunked";
					}
				}
				readRunNames.push_back(name);

				int64_t totalBytesRead = 0;
				double totalElapsed = 0.0;
				double totalElapsedClearAll = 0.0;
				double totalElapsedSingleKey = 0.0;
				std::vector<std::pair<int64_t, double>> varyingDeltas;
				for (int i = 0; i <= maxDeltaFiles; i++) {
					varyingDeltas.push_back({ 0, 0.0 });
				}
				for (auto& fileSet : fileSets) {
					FileSet newFileSet;
					if (!chunk) {
						newFileSet = fileSet;
					} else {
						newFileSet = rewriteChunkedFileSet(fileSet, keys, compressionFilter);
					}

					auto res = doReadBench(newFileSet, chunk, fileSet.range, false, keys, newFileSet.deltaFiles.size());
					totalBytesRead += res.first;
					totalElapsed += res.second;

					if (doEdgeCaseReadTests) {
						totalElapsedClearAll +=
						    doReadBench(newFileSet, chunk, fileSet.range, true, keys, newFileSet.deltaFiles.size())
						        .second;
						Key k = std::get<3>(fileSet.snapshotFile).front().key;
						KeyRange singleKeyRange(KeyRangeRef(k, keyAfter(k)));
						totalElapsedSingleKey +=
						    doReadBench(newFileSet, chunk, singleKeyRange, false, keys, newFileSet.deltaFiles.size())
						        .second;
					}

					if (doVaryingDeltaTests && chunk) {
						for (int i = 0; i <= maxDeltaFiles; i++) {
							auto r = doReadBench(newFileSet, chunk, fileSet.range, false, keys, i);
							varyingDeltas[i].first += r.first;
							varyingDeltas[i].second += r.second;
						}
					}
				}
				readMetrics.push_back({ totalBytesRead, totalElapsed });

				if (doEdgeCaseReadTests) {
					clearAllReadMetrics.push_back(totalElapsedClearAll);
					readSingleKeyMetrics.push_back(totalElapsedSingleKey);
				}
				if (doVaryingDeltaTests) {
					varyingDeltaMetrics.push_back(varyingDeltas);
				}
			}
		}
	}

	fmt::print("\n\nRead Results:\n");

	ASSERT(readRunNames.size() == readMetrics.size());
	for (int i = 0; i < readRunNames.size(); i++) {
		fmt::print("{0}", readRunNames[i]);

		double MBperCPUsec = (readMetrics[i].first / 1024.0 / 1024.0) / readMetrics[i].second;
		fmt::print(" {:.6}", MBperCPUsec);

		fmt::print("\n");
	}

	if (doEdgeCaseReadTests) {
		ASSERT(readRunNames.size() == clearAllReadMetrics.size());
		ASSERT(readRunNames.size() == readSingleKeyMetrics.size());
		fmt::print("\n\nEdge Case Read Results:\n");

		for (int i = 0; i < readRunNames.size(); i++) {
			fmt::print("{0}", readRunNames[i]);

			// use MB from full read test but elapsed from these tests so the numbers make sense relatively
			double MBperCPUsecClearAll = (readMetrics[i].first / 1024.0 / 1024.0) / clearAllReadMetrics[i];
			double MBperCPUsecSingleKey = (readMetrics[i].first / 1024.0 / 1024.0) / readSingleKeyMetrics[i];
			fmt::print(" {:.6} {:.6}", MBperCPUsecClearAll, MBperCPUsecSingleKey);

			fmt::print("\n");
		}
	}

	if (doVaryingDeltaTests) {
		ASSERT(readRunNames.size() == varyingDeltaMetrics.size());
		fmt::print("\n\nVarying Deltas Read Results:\nDF#\t");
		for (int i = 0; i <= maxDeltaFiles; i++) {
			fmt::print("{0}\t", i);
		}
		fmt::print("\n");

		for (int i = 0; i < readRunNames.size(); i++) {
			fmt::print("{0}", readRunNames[i]);

			for (auto& it : varyingDeltaMetrics[i]) {
				double MBperCPUsec = (it.first / 1024.0 / 1024.0) / it.second;
				fmt::print("\t{:.6}", MBperCPUsec);
			}
			fmt::print("\n");
		}
	}

	fmt::print("\n\nCombined Results:\n");
	ASSERT(readRunNames.size() == runNames.size() - 1);
	for (int i = 0; i < readRunNames.size(); i++) {
		fmt::print("{0}", readRunNames[i]);
		int64_t logicalBytes = deltaMetrics[i + 1].first;
		double totalElapsed = snapshotMetrics[i + 1].second + deltaMetrics[i + 1].second + readMetrics[i].second;
		double MBperCPUsec = (logicalBytes / 1024.0 / 1024.0) / totalElapsed;
		fmt::print(" {:.6}", MBperCPUsec);

		fmt::print("\n");
	}

	fmt::print("\n\nBenchmark Complete!\n");

	return Void();
}

TEST_CASE("!/blobgranule/files/repeatFromFiles") {
	std::string basePath = "SET_ME";
	std::vector<std::vector<std::string>> fileSetNames = { { "SET_ME" } };

	int64_t totalBytesRead = 0;
	double totalElapsed = 0.0;
	for (auto& it : fileSetNames) {
		FileSet fileSet = loadFileSet(basePath, it, true);
		auto res = doReadBench(fileSet, true, fileSet.range, false, {}, fileSet.deltaFiles.size(), true);
		totalBytesRead += res.first;
		totalElapsed += res.second;
	}

	double MBperCPUsec = (totalBytesRead / 1024.0 / 1024.0) / totalElapsed;
	fmt::print("Read Results: {:.6} MB/cpusec\n", MBperCPUsec);

	return Void();
}
