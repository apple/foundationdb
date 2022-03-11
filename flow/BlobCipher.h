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
#include <string>
#include <vector>

#define AES_256_KEY_LENGTH 32
#define AES_256_TAG_LENGTH 16
#define AES_256_IV_LENGTH 16

using BlobCipherDomainId = uint64_t;
using BlobCipherRandomSalt = uint64_t;
using BlobCipherBaseKeyId = uint64_t;
using BlobCipherIV = std::array<unsigned char, AES_256_IV_LENGTH>;
using BlobCipherChecksum = XXH64_hash_t;

typedef enum { BLOB_CIPHER_ENCRYPT_MODE_NONE = 0, BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR = 1 } BlockCipherEncryptMode;

// BlobCipher Encryption header format
// The header is persisted as 'plaintext' for encrypted block containing sufficient information for encryption key
// regeneration to assit decryption on reads. The total space overhead is 40 bytes.

#pragma pack(push, 1) // exact fit - no padding
typedef struct BlobCipherEncryptHeader {
	union {
		struct {
			uint8_t
			    size; // reading first byte is sufficient to determine header length. ALWAYS THE FIRST HEADER ELEMENT.
			uint8_t headerVersion{};
			uint8_t encryptMode{};
			uint8_t _reserved[5]{};
		} flags;
		uint64_t _padding{};
	};
	BlobCipherDomainId encryptDomainId{};
	BlobCipherBaseKeyId baseCipherId{};
	BlobCipherRandomSalt salt{};
	BlobCipherChecksum checksum{};

	BlobCipherEncryptHeader();
} BlobCipherEncryptHeader;
#pragma pack(pop)

// This interface is in-memory representation of CipherKey used for encryption/decryption information. It caches base
// encryption key properties as well as apply HMAC_SHA_256 derivation technique to generate a new encryption key.

class BlobCipherKey : public ReferenceCounted<BlobCipherKey>, NonCopyable {
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
	void reset();
};

// This interface allows FDB processes participating in encryption to store and index recently used encyption cipher
// keys. FDB encryption has three dimensions:
// 1. Mapping on cipher encryption keys per "encryption domains"
// 2. Per encryption domain, the cipher keys are index using "baseCipherKeyId"
// 3. Within baseCipherKey ids indexed encryption keys, cipher keys are indexed based on the "randomSalt" used to apply
// HMAC-SHA256 derivation.
//                  { encryptionDomain -> { baseCipherId -> { randomSalt, cipherKey } } }
//
// Supported cache lookups schemes:
// 1. Lookup cipher based on { encryptionDomainId, baseCipherKeyId } tuple.
// 2. Lookup cipher based on BlobCipherEncryptionHeader
//
// Client is responsible to handle cache-miss usecase, the corrective operation might vary based on the
// calling process, for instance: EncryptKeyServer cache-miss shall invoke RPC to external Encryption Key Manager to
// fetch the required encryption key, however, CPs/SSs cache-miss would result in RPC to EncryptKeyServer to refresh the
// desired encryption key.

using BlobCipherKeySaltCache = std::unordered_map<BlobCipherRandomSalt, Reference<BlobCipherKey>>;

class BlobCipherKeyItem : public ReferenceCounted<BlobCipherKeyItem>, NonCopyable {
	Reference<BlobCipherKey> latest;
	BlobCipherKeySaltCache keyCache;

public:
	BlobCipherKeyItem(Reference<BlobCipherKey>& cipher) { updateLatest(cipher); }

	Reference<BlobCipherKey> getLatest() { return latest; }

	void updateLatest(Reference<BlobCipherKey> cipher) {
		latest = cipher;
		keyCache.emplace(cipher.getPtr()->getSalt(), cipher);
	}
	Reference<BlobCipherKey> findCipher(const BlobCipherRandomSalt& salt) {
		auto itr = keyCache.find(salt);
		if (itr == keyCache.end()) {
			return Reference<BlobCipherKey>();
		}
		return itr->second;
	}
	void reset() {
		for (auto& keyItr : keyCache) {
			keyItr.second.getPtr()->reset();
		}
	}
	std::vector<Reference<BlobCipherKey>> getAllCiphers() {
		std::vector<Reference<BlobCipherKey>> ciphers;
		for (auto itr : keyCache) {
			ciphers.emplace_back(itr.second);
		}
		return ciphers;
	}
};

using BlobCipherKeyIdCacheMap = std::unordered_map<BlobCipherBaseKeyId, Reference<BlobCipherKeyItem>>;
using BlobCipherDomainCacheMap = std::unordered_map<BlobCipherDomainId, BlobCipherKeyIdCacheMap>;

class BlobCipherKeyCache : NonCopyable {
	BlobCipherDomainCacheMap domainCacheMap;
	static constexpr uint64_t CIPHER_KEY_CACHE_TTL_SEC = 10 * 60L;

	BlobCipherKeyCache() {}

public:
	void insertCipherKey(const BlobCipherDomainId& domainId,
	                     const BlobCipherBaseKeyId& baseCipherId,
	                     const uint8_t* baseCipher,
	                     int baseCipherLen);
	Reference<BlobCipherKey> getLatestCipherKey(const BlobCipherDomainId& domainId,
	                                            const BlobCipherBaseKeyId& baseKeyId);
	Reference<BlobCipherKey> getCipherKey(const BlobCipherEncryptHeader& header);
	std::vector<Reference<BlobCipherKey>> getAllCiphers(const BlobCipherDomainId& domainId,
	                                                    const BlobCipherBaseKeyId& baseKeyId);
	static BlobCipherKeyCache& getInstance() {
		static BlobCipherKeyCache instance;
		return instance;
	}
	// Ensures cached encryption key(s) (plaintext) never gets persisted as part of FDB
	// process/core dump.
	static void cleanup() noexcept;
};

// This interface enables data block encryption. An invocation to encrypt() will do two things: a) generate
// encrypted ciphertext for given plaintext input. b) generate BlobCipherEncryptHeader (including the 'tag') persiting
// for decryption on reads.

class EncryptBlobCipherAes265Ctr final : NonCopyable, public ReferenceCounted<EncryptBlobCipherAes265Ctr> {
	EVP_CIPHER_CTX* ctx;
	Reference<BlobCipherKey> cipherKey;

public:
	static constexpr uint8_t ENCRYPT_HEADER_VERSION = 1;

	EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> key, const BlobCipherIV& iv);
	~EncryptBlobCipherAes265Ctr();
	StringRef encrypt(unsigned char const* plaintext, const int plaintextLen, BlobCipherEncryptHeader* header, Arena&);
};

// This interface enable data block decryption. An invocation to decrypt() would generate 'plaintext' for a given
// 'ciphertext' input, the caller needs to supply BlobCipherEncryptHeader.

class DecryptBlobCipherAes256Ctr final : NonCopyable, public ReferenceCounted<DecryptBlobCipherAes256Ctr> {
	EVP_CIPHER_CTX* ctx;

public:
	DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> key, const BlobCipherIV& iv);
	~DecryptBlobCipherAes256Ctr();
	StringRef decrypt(unsigned char const* ciphertext,
	                  const int ciphertextLen,
	                  const BlobCipherEncryptHeader& header,
	                  Arena&);
};

class HmacSha256DigestGen final : NonCopyable {
	HMAC_CTX* ctx;

public:
	HmacSha256DigestGen(const unsigned char* key, size_t len);
	~HmacSha256DigestGen();
	HMAC_CTX* getCtx() const { return ctx; }
	StringRef digest(unsigned char const* data, size_t len, Arena&);
};

#endif // ENCRYPTION_ENABLED
