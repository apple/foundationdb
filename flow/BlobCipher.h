/*
 * BlobCipher.h
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
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#if (!defined(TLS_DISABLED) && !defined(_WIN32))
#define ENCRYPTION_ENABLED 1
#else
#define ENCRYPTION_ENABLED 0
#endif

#if ENCRYPTION_ENABLED

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/xxhash.h"

#include <openssl/aes.h>
#include <openssl/engine.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#define AES_256_KEY_LENGTH 32
#define AES_256_IV_LENGTH 16
#define INVALID_DOMAIN_ID 0
#define INVALID_CIPHER_KEY_ID 0

using BlobCipherDomainId = uint64_t;
using BlobCipherRandomSalt = uint64_t;
using BlobCipherBaseKeyId = uint64_t;
using BlobCipherChecksum = uint64_t;

typedef enum { BLOB_CIPHER_ENCRYPT_MODE_NONE = 0, BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR = 1 } BlockCipherEncryptMode;

// Encryption operations buffer management
// Approach limits number of copies needed during encryption or decryption operations.
// For encryption EncryptBuf is allocated using client supplied Arena and provided to AES library to capture
// the ciphertext. Similarly, on decryption EncryptBuf is allocated using client supplied Arena and provided
// to the AES library to capture decipher text and passed back to the clients. Given the object passed around
// is reference-counted, it gets freed once refrenceCount goes to 0.

class EncryptBuf : public ReferenceCounted<EncryptBuf>, NonCopyable {
public:
	EncryptBuf(int size, Arena& arena) : allocSize(size), logicalSize(size) {
		if (size > 0) {
			buffer = new (arena) uint8_t[size];
		} else {
			buffer = nullptr;
		}
	}

	int getLogicalSize() { return logicalSize; }
	void setLogicalSize(int value) {
		ASSERT(value <= allocSize);
		logicalSize = value;
	}
	uint8_t* begin() { return buffer; }

private:
	int allocSize;
	int logicalSize;
	uint8_t* buffer;
};

// BlobCipher Encryption header format
// This header is persisted along with encrypted buffer, it contains information necessary
// to assist decrypting the buffers to serve read requests.
//
// The total space overhead is 56 bytes.

#pragma pack(push, 1) // exact fit - no padding
typedef struct BlobCipherEncryptHeader {
	union {
		struct {
			uint8_t size; // reading first byte is sufficient to determine header
			              // length. ALWAYS THE FIRST HEADER ELEMENT.
			uint8_t headerVersion{};
			uint8_t encryptMode{};
			uint8_t _reserved[5]{};
		} flags;
		uint64_t _padding{};
	};
	// Encyrption domain boundary identifier.
	BlobCipherDomainId encryptDomainId{};
	// BaseCipher encryption key identifier
	BlobCipherBaseKeyId baseCipherId{};
	// Random salt
	BlobCipherRandomSalt salt{};
	// Checksum of the encrypted buffer. It protects against 'tampering' of ciphertext as well 'bit rots/flips'.
	BlobCipherChecksum ciphertextChecksum{};
	// Initialization vector used to encrypt the payload.
	uint8_t iv[AES_256_IV_LENGTH];

	BlobCipherEncryptHeader();
} BlobCipherEncryptHeader;
#pragma pack(pop)

// This interface is in-memory representation of CipherKey used for encryption/decryption information.
// It caches base encryption key properties as well as caches the 'derived encryption' key obtained by applying
// HMAC-SHA-256 derivation technique.

class BlobCipherKey : public ReferenceCounted<BlobCipherKey>, NonCopyable {
public:
	BlobCipherKey(const BlobCipherDomainId& domainId,
	              const BlobCipherBaseKeyId& baseCiphId,
	              const uint8_t* baseCiph,
	              int baseCiphLen);

	uint8_t* data() const { return cipher.get(); }
	uint64_t getCreationTime() const { return creationTime; }
	BlobCipherDomainId getDomainId() const { return encryptDomainId; }
	BlobCipherRandomSalt getSalt() const { return randomSalt; }
	BlobCipherBaseKeyId getBaseCipherId() const { return baseCipherId; }
	int getBaseCipherLen() const { return baseCipherLen; }
	uint8_t* rawCipher() const { return cipher.get(); }
	uint8_t* rawBaseCipher() const { return baseCipher.get(); }
	bool isEqual(const Reference<BlobCipherKey> toCompare) {
		return encryptDomainId == toCompare->getDomainId() && baseCipherId == toCompare->getBaseCipherId() &&
		       randomSalt == toCompare->getSalt() && baseCipherLen == toCompare->getBaseCipherLen() &&
		       memcmp(cipher.get(), toCompare->rawCipher(), AES_256_KEY_LENGTH) == 0 &&
		       memcmp(baseCipher.get(), toCompare->rawBaseCipher(), baseCipherLen) == 0;
	}
	void reset();

private:
	// Encryption domain boundary identifier
	BlobCipherDomainId encryptDomainId;
	// Base encryption cipher key properties
	std::unique_ptr<uint8_t[]> baseCipher;
	int baseCipherLen;
	BlobCipherBaseKeyId baseCipherId;
	// Random salt used for encryption cipher key derivation
	BlobCipherRandomSalt randomSalt;
	// Creation timestamp for the derived encryption cipher key
	uint64_t creationTime;
	// Derived encryption cipher key
	std::unique_ptr<uint8_t[]> cipher;

	void initKey(const BlobCipherDomainId& domainId,
	             const uint8_t* baseCiph,
	             int baseCiphLen,
	             const BlobCipherBaseKeyId& baseCiphId,
	             const BlobCipherRandomSalt& salt);
	void applyHmacSha256Derivation();
};

// This interface allows FDB processes participating in encryption to store and
// index recently used encyption cipher keys. FDB encryption has two dimensions:
// 1. Mapping on cipher encryption keys per "encryption domains"
// 2. Per encryption domain, the cipher keys are index using "baseCipherKeyId".
//
// The design supports NIST recommendation of limiting lifetime of an encryption
// key. For details refer to:
// https://csrc.nist.gov/publications/detail/sp/800-57-part-1/rev-3/archive/2012-07-10
//
// Below gives a pictoral representation of in-memory datastructure implemented
// to index encryption keys:
//                  { encryptionDomain -> { baseCipherId -> cipherKey } }
//
// Supported cache lookups schemes:
// 1. Lookup cipher based on { encryptionDomainId, baseCipherKeyId } tuple.
// 2. Lookup latest cipher key for a given encryptionDomainId.
//
// Client is responsible to handle cache-miss usecase, the corrective operation
// might vary based on the calling process, for instance: EncryptKeyServer
// cache-miss shall invoke RPC to external Encryption Key Manager to fetch the
// required encryption key, however, CPs/SSs cache-miss would result in RPC to
// EncryptKeyServer to refresh the desired encryption key.

using BlobCipherKeyIdCacheMap = std::unordered_map<BlobCipherBaseKeyId, Reference<BlobCipherKey>>;
using BlobCipherKeyIdCacheMapCItr = std::unordered_map<BlobCipherBaseKeyId, Reference<BlobCipherKey>>::const_iterator;

struct BlobCipherKeyIdCache : ReferenceCounted<BlobCipherKeyIdCache> {
public:
	BlobCipherKeyIdCache();
	explicit BlobCipherKeyIdCache(BlobCipherDomainId dId);

	// API returns the last inserted cipherKey.
	// If none exists, 'encrypt_key_not_found' is thrown.
	Reference<BlobCipherKey> getLatestCipherKey();
	// API returns cipherKey corresponding to input 'baseCipherKeyId'.
	// If none exists, 'encrypt_key_not_found' is thrown.
	Reference<BlobCipherKey> getCipherByBaseCipherId(BlobCipherBaseKeyId baseCipherKeyId);
	// API enables inserting base encryption cipher details to the BlobCipherKeyIdCache.
	// Given cipherKeys are immutable, attempting to re-insert same 'identical' cipherKey
	// is treated as a NOP (success), however, an attempt to update cipherKey would throw
	// 'encrypt_update_cipher' exception.
	void insertBaseCipherKey(BlobCipherBaseKeyId baseCipherId, const uint8_t* baseCipher, int baseCipherLen);
	// API cleanup the cache by dropping all cached cipherKeys
	void cleanup();
	// API returns list of all 'cached' cipherKeys
	std::vector<Reference<BlobCipherKey>> getAllCipherKeys();

private:
	BlobCipherDomainId domainId;
	BlobCipherKeyIdCacheMap keyIdCache;
	BlobCipherBaseKeyId latestBaseCipherKeyId;
};

using BlobCipherDomainCacheMap = std::unordered_map<BlobCipherDomainId, Reference<BlobCipherKeyIdCache>>;

class BlobCipherKeyCache : NonCopyable {
public:
	// Enable clients to insert base encryption cipher details to the BlobCipherKeyCache.
	// The cipherKeys are indexed using 'baseCipherId', given cipherKeys are immutable,
	// attempting to re-insert same 'identical' cipherKey is treated as a NOP (success),
	// however, an attempt to update cipherKey would throw 'encrypt_update_cipher' exception.
	void insertCipherKey(const BlobCipherDomainId& domainId,
	                     const BlobCipherBaseKeyId& baseCipherId,
	                     const uint8_t* baseCipher,
	                     int baseCipherLen);
	// API returns the last insert cipherKey for a given encyryption domain Id.
	// If none exists, it would throw 'encrypt_key_not_found' exception.
	Reference<BlobCipherKey> getLatestCipherKey(const BlobCipherDomainId& domainId);
	// API returns cipherKey corresponding to {encryptionDomainId, baseCipherId} tuple.
	// If none exists, it would throw 'encrypt_key_not_found' exception.
	Reference<BlobCipherKey> getCipherKey(const BlobCipherDomainId& domainId, const BlobCipherBaseKeyId& baseCipherId);
	// API returns point in time list of all 'cached' cipherKeys for a given encryption domainId.
	std::vector<Reference<BlobCipherKey>> getAllCiphers(const BlobCipherDomainId& domainId);
	// API enables dropping all 'cached' cipherKeys for a given encryption domain Id.
	// Useful to cleanup cache if an encryption domain gets removed/destroyed etc.
	void resetEncyrptDomainId(const BlobCipherDomainId domainId);

	static BlobCipherKeyCache& getInstance() {
		static BlobCipherKeyCache instance;
		return instance;
	}
	// Ensures cached encryption key(s) (plaintext) never gets persisted as part
	// of FDB process/core dump.
	static void cleanup() noexcept;

private:
	BlobCipherDomainCacheMap domainCacheMap;
	static constexpr uint64_t CIPHER_KEY_CACHE_TTL_SEC = 10 * 60L;

	BlobCipherKeyCache() {}
};

// This interface enables data block encryption. An invocation to encrypt() will
// do two things:
// 1) generate encrypted ciphertext for given plaintext input.
// 2) generate BlobCipherEncryptHeader (including the 'header checksum') and persit for decryption on reads.

class EncryptBlobCipherAes265Ctr final : NonCopyable, public ReferenceCounted<EncryptBlobCipherAes265Ctr> {
public:
	static constexpr uint8_t ENCRYPT_HEADER_VERSION = 1;

	EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> key, const uint8_t* iv, const int ivLen);
	~EncryptBlobCipherAes265Ctr();
	Reference<EncryptBuf> encrypt(const uint8_t* plaintext,
	                              const int plaintextLen,
	                              BlobCipherEncryptHeader* header,
	                              Arena&);

private:
	EVP_CIPHER_CTX* ctx;
	Reference<BlobCipherKey> cipherKey;
	uint8_t iv[AES_256_IV_LENGTH];
};

// This interface enable data block decryption. An invocation to decrypt() would generate
// 'plaintext' for a given 'ciphertext' input, the caller needs to supply BlobCipherEncryptHeader.

class DecryptBlobCipherAes256Ctr final : NonCopyable, public ReferenceCounted<DecryptBlobCipherAes256Ctr> {
public:
	DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> key, const uint8_t* iv);
	~DecryptBlobCipherAes256Ctr();
	Reference<EncryptBuf> decrypt(const uint8_t* ciphertext,
	                              const int ciphertextLen,
	                              const BlobCipherEncryptHeader& header,
	                              Arena&);

private:
	EVP_CIPHER_CTX* ctx;

	void verifyEncryptBlobHeader(const uint8_t* cipherText,
	                             const int ciphertextLen,
	                             const BlobCipherEncryptHeader& header,
	                             Arena& arena);
};

class HmacSha256DigestGen final : NonCopyable {
public:
	HmacSha256DigestGen(const unsigned char* key, size_t len);
	~HmacSha256DigestGen();
	HMAC_CTX* getCtx() const { return ctx; }
	StringRef digest(unsigned char const* data, size_t len, Arena&);

private:
	HMAC_CTX* ctx;
};

BlobCipherChecksum computeEncryptChecksum(const uint8_t* payload,
                                          const int payloadLen,
                                          const BlobCipherRandomSalt& salt,
                                          Arena& arena);

#endif // ENCRYPTION_ENABLED
