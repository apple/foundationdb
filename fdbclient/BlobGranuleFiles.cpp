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

#include "fdbclient/BlobGranuleCommon.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h" // for allKeys unit test - could remove

#include "flow/BlobCipher.h"
#include "flow/CompressionUtils.h"
#include "flow/DeterministicRandom.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/serialize.h"
#include "flow/UnitTest.h"
#include "flow/xxhash.h"

#include "fmt/format.h"

#include <cstring>
#include <vector>

#define BG_READ_DEBUG false

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

	eKeys.textCipherKey = makeReference<BlobCipherKey>(cipherKeysCtx.textCipherKey.encryptDomainId,
	                                                   cipherKeysCtx.textCipherKey.baseCipherId,
	                                                   cipherKeysCtx.textCipherKey.baseCipher.begin(),
	                                                   cipherKeysCtx.textCipherKey.baseCipher.size(),
	                                                   cipherKeysCtx.textCipherKey.salt);
	eKeys.headerCipherKey = makeReference<BlobCipherKey>(cipherKeysCtx.headerCipherKey.encryptDomainId,
	                                                     cipherKeysCtx.headerCipherKey.baseCipherId,
	                                                     cipherKeysCtx.headerCipherKey.baseCipher.begin(),
	                                                     cipherKeysCtx.headerCipherKey.baseCipher.size(),
	                                                     cipherKeysCtx.headerCipherKey.salt);

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
	// Validate encryption header 'cipherHeader' details sanity
	if (!(header.cipherHeaderDetails.baseCipherId == eKeys.headerCipherKey->getBaseCipherId() &&
	      header.cipherHeaderDetails.encryptDomainId == eKeys.headerCipherKey->getDomainId() &&
	      header.cipherHeaderDetails.salt == eKeys.headerCipherKey->getSalt())) {
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

		EncryptBlobCipherAes265Ctr encryptor(eKeys.textCipherKey,
		                                     eKeys.headerCipherKey,
		                                     cipherKeysCtx.ivRef.begin(),
		                                     AES_256_IV_LENGTH,
		                                     ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
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

		DecryptBlobCipherAes256Ctr decryptor(eKeys.textCipherKey, eKeys.headerCipherKey, cipherKeysCtx.ivRef.begin());
		StringRef decrypted =
		    decryptor.decrypt(idxRef.buffer.begin(), idxRef.buffer.size(), header, arena)->toStringRef();

		if (BG_ENCRYPT_COMPRESS_DEBUG) {
			XXH64_hash_t chksum = XXH3_64bits(decrypted.begin(), decrypted.size());
			TraceEvent(SevDebug, "IndexBlockEncrypt_After").detail("Chksum", chksum);
		}

		// TODO: Add version?
		ObjectReader dataReader(decrypted.begin(), IncludeVersion());
		dataReader.deserialize(FileIdentifierFor<IndexBlock>::value, idxRef.block, arena);
	}

	void init(Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx, Arena& arena) {
		if (encryptHeaderRef.present()) {
			CODE_PROBE(true, "reading encrypted chunked file");
			ASSERT(cipherKeysCtx.present());
			decrypt(cipherKeysCtx.get(), *this, arena);
		} else {
			TraceEvent("IndexBlockSize").detail("Sz", buffer.size());

			// TODO: Add version?
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

		TraceEvent(SevDebug, "IndexBlockSize").detail("Sz", buffer.size()).detail("Encrypted", cipherKeysCtx.present());
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

		EncryptBlobCipherAes265Ctr encryptor(eKeys.textCipherKey,
		                                     eKeys.headerCipherKey,
		                                     cipherKeysCtx.ivRef.begin(),
		                                     AES_256_IV_LENGTH,
		                                     ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
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

		DecryptBlobCipherAes256Ctr decryptor(eKeys.textCipherKey, eKeys.headerCipherKey, cipherKeysCtx.ivRef.begin());
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
		chunkRef.buffer = CompressionUtils::compress(chunkRef.compressionFilter.get(), chunk.contents(), arena);

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

		ChildType child;
		ObjectReader dataReader(chunkRef.chunkBytes.get().begin(), IncludeVersion());
		dataReader.deserialize(FileIdentifierFor<ChildType>::value, child, childArena);

		// TODO implement some sort of decrypted+decompressed+deserialized cache, if this object gets reused?
		return Standalone<ChildType>(child, childArena);
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
Value serializeChunkedSnapshot(Standalone<GranuleSnapshot> snapshot,
                               int targetChunkBytes,
                               Optional<CompressionFilter> compressFilter,
                               Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
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
			    ObjectWriter::toValue(currentChunk, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
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
static Arena loadSnapshotFile(const StringRef& snapshotData,
                              const KeyRangeRef& keyRange,
                              std::map<KeyRef, ValueRef>& dataMap,
                              Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	Arena rootArena;

	Standalone<IndexedBlobGranuleFile> file = IndexedBlobGranuleFile::fromFileBytes(snapshotData, cipherKeysCtx);

	ASSERT(file.fileType == SNAPSHOT_FILE_TYPE);
	ASSERT(file.chunkStartOffset > 0);

	// empty snapshot file
	if (file.indexBlockRef.block.children.empty()) {
		return rootArena;
	}

	ASSERT(file.indexBlockRef.block.children.size() >= 2);

	// TODO: refactor this out of delta tree
	// int commonPrefixLen = commonPrefixLength(index.dataBlockOffsets.front().first,
	// index.dataBlockOffsets.back().first);

	// find range of blocks needed to read
	ChildBlockPointerRef* currentBlock = file.findStartBlock(keyRange.begin);

	// FIXME: optimize cpu comparisons here in first/last partial blocks, doing entire blocks at once based on
	// comparison, and using shared prefix for key comparison
	while (currentBlock != (file.indexBlockRef.block.children.end() - 1) && keyRange.end > currentBlock->key) {
		Standalone<GranuleSnapshot> dataBlock =
		    file.getChild<GranuleSnapshot>(currentBlock, cipherKeysCtx, file.chunkStartOffset);
		ASSERT(!dataBlock.empty());
		ASSERT(currentBlock->key == dataBlock.front().key);

		bool anyRows = false;
		for (auto& entry : dataBlock) {
			if (entry.key >= keyRange.begin && entry.key < keyRange.end) {
				dataMap.insert({ entry.key, entry.value });
				anyRows = true;
			}
		}
		if (anyRows) {
			rootArena.dependsOn(dataBlock.arena());
		}
		currentBlock++;
	}

	return rootArena;
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
			// TODO REMOVE validation
			ASSERT(fileRange.contains(m.param1));
			if (m.type == MutationRef::ClearRange) {
				ASSERT(m.param2 <= fileRange.end);
				// handle single key clear more efficiently
				if (equalsKeyAfter(m.param1, m.param2)) {
					SortedDeltasT::iterator key = insertMutationBoundary(deltasByKey, m.param1);
					updateMutationBoundary(key->second, ValueAndVersionRef(it.version));
				} else {
					// Update each boundary in the cleared range
					SortedDeltasT::iterator begin = insertMutationBoundary(deltasByKey, m.param1);
					SortedDeltasT::iterator end = insertMutationBoundary(deltasByKey, m.param2);
					while (begin != end) {
						// Set the rangeClearedVersion if not set
						if (!begin->second.clearVersion.present()) {
							begin->second.clearVersion = it.version;
						}

						// Add a clear to values if it's empty or the last item is not a clear
						if (begin->second.values.empty() || begin->second.values.back().isSet()) {
							updateMutationBoundary(begin->second, ValueAndVersionRef(it.version));
						}
						++begin;
					}
				}
			} else {
				Standalone<DeltaBoundaryRef>& bound = insertMutationBoundary(deltasByKey, m.param1)->second;
				updateMutationBoundary(bound, ValueAndVersionRef(it.version, m.param2));
			}
		}
	}

	// TODO: could do a scan through map and coalesce clears (if any boundaries with exactly 1 mutation (clear) and same
	// clearVersion as previous guy)
}

// FIXME: Could maybe reduce duplicated code between this and chunkedSnapshot for chunking
Value serializeChunkedDeltaFile(Standalone<GranuleDeltas> deltas,
                                const KeyRangeRef& fileRange,
                                int chunkSize,
                                Optional<CompressionFilter> compressFilter,
                                Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
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
			    ObjectWriter::toValue(currentChunk, IncludeVersion(ProtocolVersion::withBlobGranuleFile()));
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
	ParsedDeltaBoundaryRef(Arena& arena, const ParsedDeltaBoundaryRef& copyFrom)
	  : key(arena, copyFrom.key), op(copyFrom.op), clearAfter(copyFrom.clearAfter) {
		if (copyFrom.isSet()) {
			value = StringRef(arena, copyFrom.value);
		}
	}

	bool isSet() const { return op == MutationRef::SetValue; }
	bool isClear() const { return op == MutationRef::ClearRange; }
	bool redundant(bool prevClearAfter) const { return op == MutationRef::Type::NoOp && clearAfter == prevClearAfter; }
};

// TODO could move ParsedDeltaBoundaryRef struct type up to granule common and make this a member of DeltaBoundaryRef?
ParsedDeltaBoundaryRef deltaAtVersion(const DeltaBoundaryRef& delta, Version beginVersion, Version readVersion) {
	bool clearAfter = delta.clearVersion.present() && readVersion >= delta.clearVersion.get() &&
	                  beginVersion <= delta.clearVersion.get();
	if (delta.values.empty()) {
		return ParsedDeltaBoundaryRef(delta.key, clearAfter);
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

void applyDeltasSorted(const Standalone<VectorRef<ParsedDeltaBoundaryRef>>& sortedDeltas,
                       bool startClear,
                       std::map<KeyRef, ValueRef>& dataMap) {
	if (sortedDeltas.empty() && !startClear) {
		return;
	}

	// sorted merge of 2 iterators
	bool prevClear = startClear;
	auto deltaIt = sortedDeltas.begin();
	auto snapshotIt = dataMap.begin();

	while (deltaIt != sortedDeltas.end() && snapshotIt != dataMap.end()) {
		if (deltaIt->key < snapshotIt->first) {
			// Delta is lower than snapshot. Insert new row, if the delta is a set. Ignore point clear and noop
			if (deltaIt->isSet()) {
				snapshotIt = dataMap.insert(snapshotIt, { deltaIt->key, deltaIt->value });
				snapshotIt++;
			}
			prevClear = deltaIt->clearAfter;
			deltaIt++;
		} else if (snapshotIt->first < deltaIt->key) {
			// Snapshot is lower than delta. Erase the current entry if the previous delta was a clearAfter
			if (prevClear) {
				snapshotIt = dataMap.erase(snapshotIt);
			} else {
				snapshotIt++;
			}
		} else {
			// Delta and snapshot are for the same key. The delta is newer, so if it is a set, update the value, else if
			// it's a clear, delete the value (ignore noop)
			if (deltaIt->isSet()) {
				snapshotIt->second = deltaIt->value;
			} else if (deltaIt->isClear()) {
				snapshotIt = dataMap.erase(snapshotIt);
			}
			if (!deltaIt->isClear()) {
				snapshotIt++;
			}
			prevClear = deltaIt->clearAfter;
			deltaIt++;
		}
	}
	// Either we are out of deltas or out of snapshots.
	// if snapshot remaining and prevClear last delta set, clear the rest of the map
	if (prevClear && snapshotIt != dataMap.end()) {
		CODE_PROBE(true, "last delta range cleared end of snapshot");
		dataMap.erase(snapshotIt, dataMap.end());
	}
	// Apply remaining sets from delta, with no remaining snapshot
	while (deltaIt != sortedDeltas.end()) {
		if (deltaIt->isSet()) {
			CODE_PROBE(true, "deltas past end of snapshot");
			snapshotIt = dataMap.insert(snapshotIt, { deltaIt->key, deltaIt->value });
		}
		deltaIt++;
	}
}

// The arena owns the BoundaryDeltaRef struct data but the StringRef pointers point to data in deltaData, to avoid extra
// copying
Arena loadChunkedDeltaFile(const StringRef& deltaData,
                           const KeyRangeRef& keyRange,
                           Version beginVersion,
                           Version readVersion,
                           std::map<KeyRef, ValueRef>& dataMap,
                           Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	Standalone<VectorRef<ParsedDeltaBoundaryRef>> deltas;
	Standalone<IndexedBlobGranuleFile> file = IndexedBlobGranuleFile::fromFileBytes(deltaData, cipherKeysCtx);

	ASSERT(file.fileType == DELTA_FILE_TYPE);
	ASSERT(file.chunkStartOffset > 0);

	// empty delta file
	if (file.indexBlockRef.block.children.empty()) {
		return deltas.arena();
	}

	ASSERT(file.indexBlockRef.block.children.size() >= 2);

	// TODO: refactor this out of delta tree
	// int commonPrefixLen = commonPrefixLength(index.dataBlockOffsets.front().first,
	// index.dataBlockOffsets.back().first);

	// find range of blocks needed to read
	ChildBlockPointerRef* currentBlock = file.findStartBlock(keyRange.begin);

	// TODO cpu optimize (key check per block, prefixes, optimize start of first block)
	bool startClear = false;
	bool prevClearAfter = false;
	while (currentBlock != (file.indexBlockRef.block.children.end() - 1) && keyRange.end > currentBlock->key) {
		Standalone<GranuleSortedDeltas> deltaBlock =
		    file.getChild<GranuleSortedDeltas>(currentBlock, cipherKeysCtx, file.chunkStartOffset);
		ASSERT(!deltaBlock.boundaries.empty());
		ASSERT(currentBlock->key == deltaBlock.boundaries.front().key);

		// TODO refactor this into function to share with memory deltas
		bool blockMemoryUsed = false;

		for (auto& entry : deltaBlock.boundaries) {
			ParsedDeltaBoundaryRef boundary = deltaAtVersion(entry, beginVersion, readVersion);
			if (entry.key < keyRange.begin) {
				startClear = boundary.clearAfter;
				prevClearAfter = boundary.clearAfter;
			} else if (entry.key < keyRange.end) {
				if (!boundary.redundant(prevClearAfter)) {
					deltas.push_back(deltas.arena(), boundary);
					blockMemoryUsed = true;
					prevClearAfter = boundary.clearAfter;
				}
			} else {
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

	applyDeltasSorted(deltas, startClear, dataMap);

	return deltas.arena();
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

RangeResult materializeBlobGranule(const BlobGranuleChunkRef& chunk,
                                   KeyRangeRef keyRange,
                                   Version beginVersion,
                                   Version readVersion,
                                   Optional<StringRef> snapshotData,
                                   StringRef deltaFileData[]) {
	// TODO REMOVE with early replying
	ASSERT(readVersion == chunk.includedVersion);

	// Arena to hold all allocations for applying deltas. Most of it, and the arenas produced by reading the files,
	// will likely be tossed if there are a significant number of mutations, so we copy at the end instead of doing a
	// dependsOn.
	// FIXME: probably some threshold of a small percentage of the data is actually changed, where it makes sense to
	// just to dependsOn instead of copy, to use a little extra memory footprint to help cpu?
	Arena arena;
	std::map<KeyRef, ValueRef> dataMap;
	Version lastFileEndVersion = invalidVersion;
	KeyRange requestRange;
	if (chunk.tenantPrefix.present()) {
		requestRange = keyRange.withPrefix(chunk.tenantPrefix.get());
	} else {
		requestRange = keyRange;
	}

	if (snapshotData.present()) {
		Arena snapshotArena = loadSnapshotFile(snapshotData.get(), requestRange, dataMap, chunk.cipherKeysCtx);
		arena.dependsOn(snapshotArena);
	}

	if (BG_READ_DEBUG) {
		fmt::print("Applying {} delta files\n", chunk.deltaFiles.size());
	}
	for (int deltaIdx = 0; deltaIdx < chunk.deltaFiles.size(); deltaIdx++) {
		Arena deltaArena = loadChunkedDeltaFile(
		    deltaFileData[deltaIdx], requestRange, beginVersion, readVersion, dataMap, chunk.cipherKeysCtx);
		arena.dependsOn(deltaArena);
	}
	if (BG_READ_DEBUG) {
		fmt::print("Applying {} memory deltas\n", chunk.newDeltas.size());
	}
	// TODO: also sort these and do merge
	applyDeltasByVersion(chunk.newDeltas, requestRange, beginVersion, readVersion, lastFileEndVersion, dataMap);

	RangeResult ret;
	for (auto& it : dataMap) {
		ret.push_back_deep(
		    ret.arena(),
		    KeyValueRef(chunk.tenantPrefix.present() ? it.first.removePrefix(chunk.tenantPrefix.get()) : it.first,
		                it.second));
	}

	return ret;
}

struct GranuleLoadIds {
	Optional<int64_t> snapshotId;
	std::vector<int64_t> deltaIds;
};

static void startLoad(const ReadBlobGranuleContext granuleContext,
                      const BlobGranuleChunkRef& chunk,
                      GranuleLoadIds& loadIds) {

	// Start load process for all files in chunk
	if (chunk.snapshotFile.present()) {
		std::string snapshotFname = chunk.snapshotFile.get().filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.snapshotFile.get().offset == 0);
		ASSERT(chunk.snapshotFile.get().length == chunk.snapshotFile.get().fullFileLength);
		loadIds.snapshotId = granuleContext.start_load_f(snapshotFname.c_str(),
		                                                 snapshotFname.size(),
		                                                 chunk.snapshotFile.get().offset,
		                                                 chunk.snapshotFile.get().length,
		                                                 chunk.snapshotFile.get().fullFileLength,
		                                                 granuleContext.userContext);
	}
	loadIds.deltaIds.reserve(chunk.deltaFiles.size());
	for (int deltaFileIdx = 0; deltaFileIdx < chunk.deltaFiles.size(); deltaFileIdx++) {
		std::string deltaFName = chunk.deltaFiles[deltaFileIdx].filename.toString();
		// FIXME: remove when we implement file multiplexing
		ASSERT(chunk.deltaFiles[deltaFileIdx].offset == 0);
		ASSERT(chunk.deltaFiles[deltaFileIdx].length == chunk.deltaFiles[deltaFileIdx].fullFileLength);
		int64_t deltaLoadId = granuleContext.start_load_f(deltaFName.c_str(),
		                                                  deltaFName.size(),
		                                                  chunk.deltaFiles[deltaFileIdx].offset,
		                                                  chunk.deltaFiles[deltaFileIdx].length,
		                                                  chunk.deltaFiles[deltaFileIdx].fullFileLength,
		                                                  granuleContext.userContext);
		loadIds.deltaIds.push_back(deltaLoadId);
	}
}

ErrorOr<RangeResult> loadAndMaterializeBlobGranules(const Standalone<VectorRef<BlobGranuleChunkRef>>& files,
                                                    const KeyRangeRef& keyRange,
                                                    Version beginVersion,
                                                    Version readVersion,
                                                    ReadBlobGranuleContext granuleContext) {
	int64_t parallelism = granuleContext.granuleParallelism;
	if (parallelism < 1) {
		parallelism = 1;
	}
	if (parallelism >= CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM) {
		parallelism = CLIENT_KNOBS->BG_MAX_GRANULE_PARALLELISM;
	}

	GranuleLoadIds loadIds[files.size()];

	// Kick off first file reads if parallelism > 1
	for (int i = 0; i < parallelism - 1 && i < files.size(); i++) {
		startLoad(granuleContext, files[i], loadIds[i]);
	}

	try {
		RangeResult results;
		for (int chunkIdx = 0; chunkIdx < files.size(); chunkIdx++) {
			// Kick off files for this granule if parallelism == 1, or future granule if parallelism > 1
			if (chunkIdx + parallelism - 1 < files.size()) {
				startLoad(granuleContext, files[chunkIdx + parallelism - 1], loadIds[chunkIdx + parallelism - 1]);
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

			StringRef deltaData[files[chunkIdx].deltaFiles.size()];
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
			chunkRows =
			    materializeBlobGranule(files[chunkIdx], keyRange, beginVersion, readVersion, snapshotData, deltaData);

			results.arena().dependsOn(chunkRows.arena());
			results.append(results.arena(), chunkRows.begin(), chunkRows.size());

			if (loadIds[chunkIdx].snapshotId.present()) {
				granuleContext.free_load_f(loadIds[chunkIdx].snapshotId.get(), granuleContext.userContext);
			}
			for (int i = 0; i < loadIds[chunkIdx].deltaIds.size(); i++) {
				granuleContext.free_load_f(loadIds[chunkIdx].deltaIds[i], granuleContext.userContext);
			}
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
	generateRandomData(mutateString(baseCipher), baseCipher.size());
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
	generateRandomData(mutateString(cipherKeysCtx.ivRef), AES_256_IV_LENGTH);

	return cipherKeysCtx;
}

} // namespace

TEST_CASE("/blobgranule/files/applyDelta") {
	printf("Testing blob granule delta applying\n");
	Arena a;

	// do this 2 phase arena creation of string refs instead of LiteralStringRef because there is no char* StringRef
	// constructor, and valgrind might complain if the stringref data isn't in the arena
	std::string sk_a = "A";
	std::string sk_ab = "AB";
	std::string sk_b = "B";
	std::string sk_c = "C";
	std::string sk_z = "Z";
	std::string sval1 = "1";
	std::string sval2 = "2";

	StringRef k_a = StringRef(a, sk_a);
	StringRef k_ab = StringRef(a, sk_ab);
	StringRef k_b = StringRef(a, sk_b);
	StringRef k_c = StringRef(a, sk_c);
	StringRef k_z = StringRef(a, sk_z);
	StringRef val1 = StringRef(a, sval1);
	StringRef val2 = StringRef(a, sval2);

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
	std::map<KeyRef, ValueRef> result;
	Arena ar = loadSnapshotFile(serialized, KeyRangeRef(begin, end), result, cipherKeysCtx);
	ASSERT(result.empty());
}

// endIdx is exclusive
void checkSnapshotRead(const Standalone<GranuleSnapshot>& snapshot,
                       const Value& serialized,
                       int beginIdx,
                       int endIdx,
                       Optional<BlobGranuleCipherKeysCtx> cipherKeysCtx) {
	ASSERT(beginIdx < endIdx);
	ASSERT(endIdx <= snapshot.size());
	std::map<KeyRef, ValueRef> result;
	KeyRef beginKey = snapshot[beginIdx].key;
	Key endKey = endIdx == snapshot.size() ? keyAfter(snapshot.back().key) : snapshot[endIdx].key;
	KeyRangeRef range(beginKey, endKey);

	Arena ar = loadSnapshotFile(serialized, range, result, cipherKeysCtx);

	if (result.size() != endIdx - beginIdx) {
		fmt::print("Read {0} rows != {1}\n", result.size(), endIdx - beginIdx);
	}
	ASSERT(result.size() == endIdx - beginIdx);
	for (auto& it : result) {
		if (it.first != snapshot[beginIdx].key) {
			fmt::print("Key {0} != {1}\n", it.first.printable(), snapshot[beginIdx].key.printable());
		}
		ASSERT(it.first == snapshot[beginIdx].key);
		if (it.first != snapshot[beginIdx].key) {
			fmt::print("Value {0} != {1} for Key {2}\n",
			           it.second.printable(),
			           snapshot[beginIdx].value.printable(),
			           it.first.printable());
		}
		ASSERT(it.second == snapshot[beginIdx].value);
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
		allRange = KeyRangeRef(StringRef(sharedPrefix), LiteralStringRef("\xff"));

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
#ifdef ZLIB_LIB_SUPPORTED
			compressFilter = CompressionFilter::GZIP;
#else
			compressFilter = CompressionFilter::NONE;
#endif
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
			CODE_PROBE(true, "snapshot unit test keyspace full");
			break;
		}
		StringRef value = kvGen.value();

		data.push_back_deep(data.arena(), KeyValueRef(KeyRef(key.get()), ValueRef(value)));
		totalDataBytes += key.get().size() + value.size();
	}

	std::sort(data.begin(), data.end(), KeyValueRef::OrderByKey());
	return data;
}

TEST_CASE("/blobgranule/files/snapshotFormatUnitTest") {
	// snapshot files are likely to have a non-trivial shared prefix since they're for a small contiguous key range
	KeyValueGen kvGen;

	int targetChunks = deterministicRandom()->randomExp(0, 9);
	int targetDataBytes = deterministicRandom()->randomExp(0, 25);
	int targetChunkSize = targetDataBytes / targetChunks;

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

	Value serialized = serializeChunkedSnapshot(data, targetChunkSize, kvGen.compressFilter, kvGen.cipherKeys);

	fmt::print("Snapshot serialized! {0} bytes\n", serialized.size());

	fmt::print("Validating snapshot data is sorted again\n");
	for (int i = 0; i < data.size() - 1; i++) {
		ASSERT(data[i].key < data[i + 1].key);
	}

	fmt::print("Initial read starting\n");

	checkSnapshotRead(data, serialized, 0, data.size(), kvGen.cipherKeys);

	fmt::print("Initial read complete\n");

	if (data.size() > 1) {
		for (int i = 0; i < std::min(100, data.size() * 2); i++) {
			int width = deterministicRandom()->randomExp(0, maxExp);
			ASSERT(width <= data.size());
			int start = deterministicRandom()->randomInt(0, data.size() - width);
			checkSnapshotRead(data, serialized, start, start + width, kvGen.cipherKeys);
		}

		fmt::print("Doing empty checks\n");
		int randomIdx = deterministicRandom()->randomInt(0, data.size() - 1);
		checkSnapshotEmpty(serialized, keyAfter(data[randomIdx].key), data[randomIdx + 1].key, kvGen.cipherKeys);
	} else {
		fmt::print("Doing empty checks\n");
	}

	checkSnapshotEmpty(serialized, normalKeys.begin, data.front().key, kvGen.cipherKeys);
	checkSnapshotEmpty(serialized, normalKeys.begin, LiteralStringRef("\x00"), kvGen.cipherKeys);
	checkSnapshotEmpty(serialized, keyAfter(data.back().key), normalKeys.end, kvGen.cipherKeys);
	checkSnapshotEmpty(serialized, LiteralStringRef("\xfe"), normalKeys.end, kvGen.cipherKeys);

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

	applyDeltasByVersion(data, range, beginVersion, readVersion, lastFileEndVersion, expectedData);

	// actual answer
	std::string filename = randomBGFilename(
	    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUniqueID(), readVersion, ".delta");
	Standalone<BlobGranuleChunkRef> chunk;
	// TODO need to add cipher keys meta
	chunk.deltaFiles.emplace_back_deep(chunk.arena(), filename, 0, serialized->size(), serialized->size());
	chunk.cipherKeysCtx = kvGen.cipherKeys;
	chunk.keyRange = kvGen.allRange;
	chunk.includedVersion = readVersion;
	chunk.snapshotVersion = invalidVersion;

	RangeResult actualData = materializeBlobGranule(chunk, range, beginVersion, readVersion, {}, serialized);

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

	// TODO randomize begin and read version to sometimes +/- 1 and readRange begin and end to keyAfter sometimes
	return { readRange, beginVersion, readVersion };
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

TEST_CASE("/blobgranule/files/deltaFormatUnitTest") {
	KeyValueGen kvGen;

	int targetChunks = deterministicRandom()->randomExp(0, 8);
	int targetDataBytes = deterministicRandom()->randomExp(0, 21);

	int targetChunkSize = targetDataBytes / targetChunks;

	Standalone<GranuleDeltas> data = genDeltas(kvGen, targetDataBytes);

	fmt::print("Deltas ({0})\n", data.size());
	Value serialized =
	    serializeChunkedDeltaFile(data, kvGen.allRange, targetChunkSize, kvGen.compressFilter, kvGen.cipherKeys);

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

	// actual answer
	Standalone<BlobGranuleChunkRef> chunk;
	if (beginVersion == 0) {
		std::string snapshotFilename = randomBGFilename(
		    deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUniqueID(), 0, ".snapshot");
		chunk.snapshotFile = BlobFilePointerRef(
		    chunk.arena(), snapshotFilename, 0, serializedSnapshot.size(), serializedSnapshot.size());
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
		chunk.deltaFiles.emplace_back_deep(chunk.arena(), deltaFilename, 0, fsize, fsize);
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

	// TODO need to add cipher keys meta
	chunk.cipherKeysCtx = kvGen.cipherKeys;
	chunk.keyRange = kvGen.allRange;
	chunk.includedVersion = readVersion;
	chunk.snapshotVersion = (beginVersion == 0) ? 0 : invalidVersion;

	Optional<StringRef> snapshotPtr;
	if (beginVersion == 0) {
		snapshotPtr = serializedSnapshot;
	}
	RangeResult actualData = materializeBlobGranule(chunk, range, beginVersion, readVersion, snapshotPtr, deltaPtrs);

	ASSERT(expectedData.size() == actualData.size());
	int i = 0;
	for (auto& it : expectedData) {
		ASSERT(it.first == actualData[i].key);
		ASSERT(it.second == actualData[i].value);
		i++;
	}
}

TEST_CASE("/blobgranule/files/granuleReadUnitTest") {
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

	Value serializedSnapshot =
	    serializeChunkedSnapshot(snapshotData, targetSnapshotChunkSize, kvGen.compressFilter, kvGen.cipherKeys);

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
				inMemoryDeltas = fileData;
			} else {
				Value serializedDelta = serializeChunkedDeltaFile(
				    fileData, kvGen.allRange, targetDeltaChunkSize, kvGen.compressFilter, kvGen.cipherKeys);
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

	for (int i = 0; i < std::min(100, 5 + snapshotData.size() * deltaData.size()); i++) {
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