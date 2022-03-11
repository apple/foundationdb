/*
 * BlobCipher.cpp
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

#include "flow/BlobCipher.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/network.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include <cstring>
#include <memory>

#if ENCRYPTION_ENABLED

// BlobCipherEncryptHeader
BlobCipherEncryptHeader::BlobCipherEncryptHeader() {
	flags.encryptMode = BLOB_CIPHER_ENCRYPT_MODE_NONE;
}

// BlobCipherKey class methods

BlobCipherKey::BlobCipherKey(const BlobCipherDomainId& domainId,
                             const BlobCipherBaseKeyId& baseCiphId,
                             const uint8_t* baseCiph,
                             int baseCiphLen) {
	BlobCipherRandomSalt salt;
	if (g_network->isSimulated()) {
		salt = deterministicRandom()->randomUInt64();
	} else {
		salt = nondeterministicRandom()->randomUInt64();
	}
	initKey(domainId, baseCiph, baseCiphLen, baseCiphId, salt);
	/*TraceEvent("BlobCipherKey")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("BaseCipherLen", baseCipherLen)
	    .detail("RandomSalt", randomSalt)
	    .detail("CreationTime", creationTime);*/
}

void BlobCipherKey::initKey(const BlobCipherDomainId& domainId,
                            const uint8_t* baseCiph,
                            int baseCiphLen,
                            const BlobCipherBaseKeyId& baseCiphId,
                            const BlobCipherRandomSalt& salt) {
	// Set the base encryption key properties
	baseCipher = std::make_unique<uint8_t[]>(AES_256_KEY_LENGTH);
	memset(baseCipher.get(), 0, AES_256_KEY_LENGTH);
	memcpy(baseCipher.get(), baseCiph, std::min<int>(baseCiphLen, AES_256_KEY_LENGTH));
	baseCipherLen = baseCiphLen;
	baseCipherId = baseCiphId;
	// Set the encryption domain for the base encryption key
	encryptDomainId = domainId;
	randomSalt = salt;
	// derive the encryption key
	cipher = std::make_unique<uint8_t[]>(AES_256_KEY_LENGTH);
	memset(cipher.get(), 0, AES_256_KEY_LENGTH);
	applyHmacSha256Derivation();
	// update the key creation time
	creationTime = now();
}

void BlobCipherKey::applyHmacSha256Derivation() {
	Arena arena;
	uint8_t buf[baseCipherLen + sizeof(BlobCipherRandomSalt)];
	memcpy(&buf[0], baseCipher.get(), baseCipherLen);
	memcpy(&buf[0] + baseCipherLen, &randomSalt, sizeof(BlobCipherRandomSalt));
	HmacSha256DigestGen hmacGen(baseCipher.get(), baseCipherLen);
	StringRef digest = hmacGen.digest(&buf[0], baseCipherLen + sizeof(BlobCipherRandomSalt), arena);
	std::copy(digest.begin(), digest.end(), cipher.get());
	if (digest.size() < AES_256_KEY_LENGTH) {
		memcpy(cipher.get() + digest.size(), buf, AES_256_KEY_LENGTH - digest.size());
	}
}

void BlobCipherKey::reset() {
	memset(baseCipher.get(), 0, baseCipherLen);
	memset(cipher.get(), 0, AES_256_KEY_LENGTH);
}

// BlobCipherKeyCache class methods

void BlobCipherKeyCache::insertCipherKey(const BlobCipherDomainId& domainId,
                                         const BlobCipherBaseKeyId& baseCipherId,
                                         const uint8_t* baseCipher,
                                         int baseCipherLen) {
	Reference<BlobCipherKey> cipherKey =
	    makeReference<BlobCipherKey>(domainId, baseCipherId, baseCipher, baseCipherLen);

	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		domainCacheMap[domainId].emplace(baseCipherId, makeReference<BlobCipherKeyItem>(cipherKey));
		return;
	}
	auto keyIdItr = domainItr->second.find(baseCipherId);
	if (keyIdItr == domainItr->second.end()) {
		domainCacheMap[domainId].emplace(baseCipherId, makeReference<BlobCipherKeyItem>(cipherKey));
		return;
	}
	Reference<BlobCipherKeyItem> cipherKeyItem = keyIdItr->second;
	cipherKeyItem->updateLatest(cipherKey);
}

Reference<BlobCipherKey> BlobCipherKeyCache::getLatestCipherKey(const BlobCipherDomainId& domainId,
                                                                const BlobCipherBaseKeyId& baseKeyId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return Reference<BlobCipherKey>();
	}
	auto keyIdItr = domainItr->second.find(baseKeyId);
	if (keyIdItr == domainItr->second.end()) {
		return Reference<BlobCipherKey>();
	}
	Reference<BlobCipherKeyItem> cipherKeyItem = keyIdItr->second;
	Reference<BlobCipherKey> cipherKey = cipherKeyItem.getPtr()->getLatest();
	if ((now() - cipherKey->getCreationTime()) > BlobCipherKeyCache::CIPHER_KEY_CACHE_TTL_SEC) {
		return Reference<BlobCipherKey>();
	}
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::getCipherKey(const BlobCipherEncryptHeader& header) {
	auto domainItr = domainCacheMap.find(header.encryptDomainId);
	if (domainItr == domainCacheMap.end()) {
		return Reference<BlobCipherKey>();
	}
	auto keyIdItr = domainItr->second.find(header.baseCipherId);
	if (keyIdItr == domainItr->second.end()) {
		return Reference<BlobCipherKey>();
	}
	Reference<BlobCipherKeyItem> cipherKeyItem = keyIdItr->second;
	return cipherKeyItem.getPtr()->findCipher(header.salt);
}

void BlobCipherKeyCache::cleanup() noexcept {
	BlobCipherKeyCache& instance = BlobCipherKeyCache::getInstance();
	for (auto& domainItr : instance.domainCacheMap) {
		for (auto& keyIdItr : domainItr.second) {
			keyIdItr.second->reset();
		}
	}
}

std::vector<Reference<BlobCipherKey>> BlobCipherKeyCache::getAllCiphers(const BlobCipherDomainId& domainId,
                                                                        const BlobCipherBaseKeyId& baseKeyId) {
	std::vector<Reference<BlobCipherKey>> ciphers;
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return ciphers;
	}
	auto keyIdItr = domainItr->second.find(baseKeyId);
	if (keyIdItr == domainItr->second.end()) {
		return ciphers;
	}
	Reference<BlobCipherKeyItem> cipherKeyItem = keyIdItr->second;
	return cipherKeyItem->getAllCiphers();
}

// EncryptBlobCipher class methods

EncryptBlobCipherAes265Ctr::EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> key, const BlobCipherIV& iv)
  : ctx(EVP_CIPHER_CTX_new()), cipherKey(key) {
	if (ctx == nullptr) {
		throw internal_error();
	}
	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr) != 1) {
		throw internal_error();
	}
	if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.getPtr()->data(), iv.data()) != 1) {
		throw internal_error();
	}
}

StringRef EncryptBlobCipherAes265Ctr::encrypt(unsigned char const* plaintext,
                                              const int plaintextLen,
                                              BlobCipherEncryptHeader* header,
                                              Arena& arena) {
	TEST(true); // Encrypting data with BlobCipher
	// FIXME:
	auto ciphertext = new (arena) unsigned char[plaintextLen + AES_BLOCK_SIZE];
	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, ciphertext, &bytes, plaintext, plaintextLen) != 1) {
		throw encrypt_ops_error();
	}
	int finalBytes{ 0 };
	if (EVP_EncryptFinal_ex(ctx, ciphertext + bytes, &finalBytes) != 1) {
		throw encrypt_ops_error();
	}
	// populate header details for the encrypted blob.
	header->flags.size = sizeof(BlobCipherEncryptHeader);
	header->flags.headerVersion = EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION;
	header->flags.encryptMode = BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR;
	header->baseCipherId = cipherKey->getBaseCipherId();
	header->encryptDomainId = cipherKey->getDomainId();
	header->salt = cipherKey->getSalt();

	// Preserve checksum of encrypted bytes in the header; approach protects against disk induced bit-rot/flip
	// scenarios. AES CTR mode doesn't generate 'tag' by default as with schemes such as: AES 256 GCM.
	//
	// Possible to use XXH3_64bitsWithSeed() where 'seed' could be cipherKey->randomSalt; for performance reasons
	// choosing 'default seed'.

	header->checksum = XXH3_64bits(ciphertext, bytes + finalBytes);
	return StringRef(ciphertext, bytes + finalBytes);
}

EncryptBlobCipherAes265Ctr::~EncryptBlobCipherAes265Ctr() {
	EVP_CIPHER_CTX_free(ctx);
}

// DecryptBlobCipher class methods

DecryptBlobCipherAes256Ctr::DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> key, const BlobCipherIV& iv)
  : ctx(EVP_CIPHER_CTX_new()) {
	if (ctx == nullptr) {
		throw internal_error();
	}
	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
		EVP_CIPHER_CTX_free(ctx);
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.getPtr()->data(), iv.data())) {
		EVP_CIPHER_CTX_free(ctx);
		throw encrypt_ops_error();
	}
}

StringRef DecryptBlobCipherAes256Ctr::decrypt(unsigned char const* ciphertext,
                                              const int ciphertextLen,
                                              const BlobCipherEncryptHeader& header,
                                              Arena& arena) {
	TEST(true); // Decrypting data with BlobCipher
	// validate header flag sanity; for now only one valid header version exists.
	if (header.flags.headerVersion != EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION ||
	    header.flags.encryptMode != BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR) {
		throw internal_error();
	}
	// encrypted byte checksum sanity; protection against data bit-rot/flip.
	BlobCipherChecksum checksum = XXH3_64bits(ciphertext, ciphertextLen);
	if (checksum != header.checksum) {
		throw checksum_failed();
	}
	auto plaintext = new (arena) unsigned char[ciphertextLen + AES_BLOCK_SIZE];
	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, plaintext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		EVP_CIPHER_CTX_free(ctx);
		throw encrypt_ops_error();
	}
	int finalBlobBytes{ 0 };
	if (EVP_DecryptFinal_ex(ctx, plaintext + bytesDecrypted, &finalBlobBytes) <= 0) {
		EVP_CIPHER_CTX_free(ctx);
		throw encrypt_ops_error();
	}
	return StringRef(plaintext, bytesDecrypted + finalBlobBytes);
}

DecryptBlobCipherAes256Ctr::~DecryptBlobCipherAes256Ctr() {
	EVP_CIPHER_CTX_free(ctx);
}

// HmacSha256DigestGen class methods

HmacSha256DigestGen::HmacSha256DigestGen(const unsigned char* key, size_t len) : ctx(HMAC_CTX_new()) {
	if (!HMAC_Init_ex(ctx, key, len, EVP_sha256(), nullptr)) {
		throw internal_error();
	}
}

HmacSha256DigestGen::~HmacSha256DigestGen() {
	HMAC_CTX_free(ctx);
}

StringRef HmacSha256DigestGen::digest(const unsigned char* data, size_t len, Arena& arena) {
	TEST(true); // Digest generation
	unsigned int digestLen = HMAC_size(ctx);
	auto digest = new (arena) unsigned char[digestLen];
	if (HMAC_Update(ctx, data, len) != 1) {
		throw internal_error();
	}
	if (HMAC_Final(ctx, digest, &digestLen) != 1) {
		throw internal_error();
	}
	return StringRef(digest, digestLen);
}

// Only used to link unit tests
void forceLinkBlobCipherTests() {}

// Test BlobCipherKey caching mechanism, tests cases includes:
// 1. Insert & retrieval of latest cipher for multiple encryption domains
// 2. Insert new cipherKeys for already inserted baseCipherKeyId
TEST_CASE("flow/BlobCipher") {
	// Construct a dummy External Key Manager representation and populate with some keys
	class BaseCipher : public ReferenceCounted<BaseCipher>, NonCopyable {
	public:
		BlobCipherDomainId domainId;
		int len;
		BlobCipherBaseKeyId keyId;
		std::unique_ptr<uint8_t[]> key;

		BaseCipher(const BlobCipherDomainId& dId, const BlobCipherBaseKeyId& kId)
		  : domainId(dId), len(deterministicRandom()->randomInt(AES_256_KEY_LENGTH / 2, AES_256_KEY_LENGTH + 1)),
		    keyId(kId), key(std::make_unique<uint8_t[]>(len)) {
			generateRandomData(key.get(), len);
		}
	};

	using BaseKeyMap = std::unordered_map<BlobCipherBaseKeyId, Reference<BaseCipher>>;
	using DomainKeyMap = std::unordered_map<BlobCipherDomainId, BaseKeyMap>;
	DomainKeyMap domainKeyMap;
	for (int dId = 0; dId < 10; dId++) {
		for (int kId = 100; kId < 120; kId++) {
			domainKeyMap[dId].emplace(kId, makeReference<BaseCipher>(dId, kId));
		}
	}

	// case-I: insert BlobCipher keys into BlobCipherKeyCache map and validate
	BlobCipherKeyCache& cipherKeyCache = BlobCipherKeyCache::getInstance();
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;

			cipherKeyCache.insertCipherKey(
			    baseCipher->domainId, baseCipher->keyId, baseCipher->key.get(), baseCipher->len);
		}
	}
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;
			Reference<BlobCipherKey> cipherKey =
			    cipherKeyCache.getLatestCipherKey(baseCipher->domainId, baseCipher->keyId);
			ASSERT(cipherKey.isValid());
			// validate common cipher properties - domainId, baseCipherId, baseCipherLen, rawBaseCipher
			ASSERT(cipherKey->getBaseCipherId() == baseCipher->keyId);
			ASSERT(cipherKey->getDomainId() == baseCipher->domainId);
			ASSERT(cipherKey->getBaseCipherLen() == baseCipher->len);
			// ensure that baseCipher matches with the cached information
			ASSERT(std::memcmp(cipherKey->rawBaseCipher(), baseCipher->key.get(), cipherKey->getBaseCipherLen()) == 0);
			// validate the encryption derivation
			ASSERT(std::memcmp(cipherKey->rawCipher(), baseCipher->key.get(), cipherKey->getBaseCipherLen()) != 0);
		}
	}

	// case-II: simulate baseKeyId are unchanged, forces generate new cipher keys(with different salt).
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;
			cipherKeyCache.insertCipherKey(
			    baseCipher->domainId, baseCipher->keyId, baseCipher->key.get(), baseCipher->len);
		}
	}
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;
			std::vector<Reference<BlobCipherKey>> ciphers =
			    cipherKeyCache.getAllCiphers(baseCipher->domainId, baseCipher->keyId);
			// ensure more one than one cipher is cached
			ASSERT(ciphers.size() == 2);
			ASSERT(ciphers[0].isValid() && ciphers[1].isValid());
			// validate common cipher properties - domainId, baseCipherId, baseCipherLen, rawBaseCipher
			ASSERT(ciphers[0]->getDomainId() == ciphers[1]->getDomainId());
			ASSERT(ciphers[0]->getBaseCipherId() == ciphers[1]->getBaseCipherId());
			ASSERT(ciphers[0]->getBaseCipherLen() == ciphers[1]->getBaseCipherLen());
			ASSERT(memcmp(ciphers[0]->rawBaseCipher(), ciphers[1]->rawBaseCipher(), ciphers[0]->getBaseCipherLen()) ==
			       0);
			// validate unique cipher properties - cipher derivation, randomSalt
			ASSERT(memcmp(ciphers[0]->rawCipher(), ciphers[1]->rawCipher(), ciphers[0]->getBaseCipherLen()) != 0);
			ASSERT(ciphers[0]->getSalt() != ciphers[1]->getSalt());
			// TODO: creationTime validation can't be done due to use of simulated time
		}
	}
	return Void();
}

#endif // ENCRYPTION_ENABLED
