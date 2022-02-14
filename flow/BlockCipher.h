/*
 * BlockCipher.h
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

using BlockCipherDomainId = uint64_t;
using BlockCipherRandomSalt = uint64_t;
using BlockCipherBaseKeyId = uint64_t;
using BlockCipherIV = std::array<unsigned char, AES_256_IV_LENGTH>;
using BlockCipherTag = std::unique_ptr<uint8_t[]>;

// BlockCipher Encryption header format
// The header is persisted as 'plaintext' for encrypted block containing sufficient information for encyrption key
// regeneration to assit decryption on reads. The total space overhead is 48 bytes.

#pragma pack(push, 1) // exact fit - no padding
typedef struct BlockCipherEncryptHeader {
	union {
		struct {
			uint8_t headerVersion;
			uint8_t _reserved[7];
		} flags;
		uint64_t _padding;
	};
	uint8_t headerVersion;
	BlockCipherDomainId encryptDomainId;
	BlockCipherBaseKeyId baseCipherId;
	BlockCipherRandomSalt salt;
	BlockCipherTag tag;
	uint64_t _reserved;

	BlockCipherEncryptHeader();
} BlockCipherEncryptHeader;
#pragma pack(pop)

// This interface is in-memory representation of CipherKey used for encryption/decryption information. It caches base
// encyrption key properties as well as apply HMAC_SHA_256 derivation technique to generate a new encryption key.

class BlockCipherKey : public ReferenceCounted<BlockCipherKey>, NonCopyable {
	// Encryption domain boundary identifier
	BlockCipherDomainId encryptDomainId;
	// Base encyrption cipher key properties
	std::unique_ptr<uint8_t[]> baseCipher;
	int baseCipherLen;
	BlockCipherBaseKeyId baseCipherId;
	// Random salt used for encryption cipher key derivation
	BlockCipherRandomSalt randomSalt;
	// Creation timestamp for the derived encryption cipher key
	uint64_t creationTime;
	// Derived encyrption cipher key
	std::unique_ptr<uint8_t[]> cipher;

	void initKey(const BlockCipherDomainId& domainId,
	             const uint8_t* baseCiph,
	             int baseCiphLen,
	             const BlockCipherBaseKeyId& baseCiphId,
	             const BlockCipherRandomSalt& salt);
	void applyHmacSha256Derivation();

public:
	BlockCipherKey(const BlockCipherDomainId& domainId,
	               const BlockCipherBaseKeyId& baseCiphId,
	               const uint8_t* baseCiph,
	               int baseCiphLen);

	uint8_t* data() const { return cipher.get(); }
	uint64_t getCreationTime() const { return creationTime; }
	BlockCipherDomainId getDomainId() const { return encryptDomainId; }
	BlockCipherRandomSalt getSalt() const { return randomSalt; }
	BlockCipherBaseKeyId getBaseCipherId() const { return baseCipherId; }
	int getBaseCipherLen() const { return baseCipherLen; }
	uint8_t* rawCipher() const { return cipher.get(); }
	uint8_t* rawBaseCipher() const { return baseCipher.get(); }
	void reset();
};

// This interface allows FDB processes participating in encyrption to store and index recently used encyption cipher
// keys. FDB encryption has three dimensions:
// 1. Mapping on cipher encyrption keys per "encryption domains"
// 2. Per encryption domain, the cipher keys are index using "baseCipherKeyId"
// 3. Within baseCipherKey ids indexed encryption keys, cipher keys are indexed based on the "randomSalt" used to apply
// HMAC-SHA256 derivation.
//                  { encryptionDomain -> { baseCipherId -> { randomSalt, cipherKey } } }
//
// Supported cache lookups schemes:
// 1. Lookup cipher based on { encyrptionDomainId, baseCipherKeyId } tuple.
// 2. Lookup cipher based on BlockCipherEncryptionHeader
//
// Client is responsible to handle cache-miss usecase, the corrective operation might vary based on the
// calling process, for instance: EncryptKeyServer cache-miss shall invoke RPC to external Encryption Key Manager to
// fetch the required encryption key, however, CPs/SSs cache-miss would result in RPC to EncryptKeyServer to refresh the
// desired encryption key.

using BlockCipherKeySaltCache = std::unordered_map<BlockCipherRandomSalt, Reference<BlockCipherKey>>;

class BlockCipherKeyItem : public ReferenceCounted<BlockCipherKeyItem>, NonCopyable {
	Reference<BlockCipherKey> latest;
	BlockCipherKeySaltCache keyCache;

public:
	BlockCipherKeyItem(Reference<BlockCipherKey>& cipher) { updateLatest(cipher); }

	Reference<BlockCipherKey> getLatest() { return latest; }

	void updateLatest(Reference<BlockCipherKey> cipher) {
		latest = cipher;
		keyCache.emplace(cipher.getPtr()->getSalt(), cipher);
	}
	Reference<BlockCipherKey> findCipher(const BlockCipherRandomSalt& salt) {
		auto itr = keyCache.find(salt);
		if (itr == keyCache.end()) {
			return Reference<BlockCipherKey>();
		}
		return itr->second;
	}
	void reset() {
		for (auto& keyItr : keyCache) {
			keyItr.second.getPtr()->reset();
		}
	}
	std::vector<Reference<BlockCipherKey>> getAllCiphers() {
		std::vector<Reference<BlockCipherKey>> ciphers;
		for (auto itr : keyCache) {
			ciphers.emplace_back(itr.second);
		}
		return ciphers;
	}
};

using BlockCipherKeyIdCacheMap = std::unordered_map<BlockCipherBaseKeyId, Reference<BlockCipherKeyItem>>;
using BlockCipherDomainCacheMap = std::unordered_map<BlockCipherDomainId, BlockCipherKeyIdCacheMap>;

class BlockCipherKeyCache : NonCopyable {
	BlockCipherDomainCacheMap domainCacheMap;
	static uint64_t CIPHER_KEY_CACHE_TTL_NS;

	BlockCipherKeyCache() {}

public:
	void insertCipherKey(const BlockCipherDomainId& domainId,
	                     const BlockCipherBaseKeyId& baseCipherId,
	                     const uint8_t* baseCipher,
	                     int baseCipherLen);
	Reference<BlockCipherKey> getLatestCipherKey(const BlockCipherDomainId& domainId,
	                                             const BlockCipherBaseKeyId& baseKeyId);
	Reference<BlockCipherKey> getCipherKey(const BlockCipherEncryptHeader& header);
	std::vector<Reference<BlockCipherKey>> getAllCiphers(const BlockCipherDomainId& domainId,
	                                                     const BlockCipherBaseKeyId& baseKeyId);
	static BlockCipherKeyCache& getInstance() {
		static BlockCipherKeyCache instance;
		return instance;
	}
	// Ensures cached encryption key(s) (plaintext) never gets persisted as part of FDB
	// process/core dump.
	static void cleanup() noexcept;
};

// This interface enables data block encyrption. An invocation to encrypt() will do two things: a) generate
// encrypted ciphertext for given plaintext input. b) generate BlockCipherEncryptHeader (including the 'tag') persiting
// for decryption on reads.

class EncryptBlockCipher final : NonCopyable, public ReferenceCounted<EncryptBlockCipher> {
	EVP_CIPHER_CTX* ctx;
	Reference<BlockCipherKey> cipherKey;

public:
	static uint8_t ENCRYPT_HEADER_VERSION;

	EncryptBlockCipher(Reference<BlockCipherKey> key, const BlockCipherIV& iv);
	~EncryptBlockCipher();
	StringRef encrypt(unsigned char const* plaintext, int len, BlockCipherEncryptHeader* header, Arena&);
};

// This interface enable data block decryption. An invocation to decrypt() would generate 'plaintext' for a given
// 'ciphertext' input, the caller needs to supply BlockCipherEncryptHeader.

class DecryptBlockCipher final : NonCopyable, public ReferenceCounted<DecryptBlockCipher> {
	EVP_CIPHER_CTX* ctx;

public:
	DecryptBlockCipher(Reference<BlockCipherKey> key, const BlockCipherIV& iv);
	~DecryptBlockCipher();
	StringRef decrypt(unsigned char const* ciphertext, int len, const BlockCipherEncryptHeader& header, Arena&);
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
