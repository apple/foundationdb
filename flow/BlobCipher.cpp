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
#include "flow/ITrace.h"
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

// BlobKeyIdCache class methods

BlobCipherKeyIdCache::BlobCipherKeyIdCache()
  : domainId(INVALID_DOMAIN_ID), latestBaseCipherKeyId(INVALID_CIPHER_KEY_ID) {}

BlobCipherKeyIdCache::BlobCipherKeyIdCache(BlobCipherDomainId dId)
  : domainId(dId), latestBaseCipherKeyId(INVALID_CIPHER_KEY_ID) {
	TraceEvent("Init_BlobCipherKeyIdCache").detail("DomainId", domainId);
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::getLatestCipherKey() {
	return getCipherByBaseCipherId(latestBaseCipherKeyId);
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::getCipherByBaseCipherId(BlobCipherBaseKeyId baseCipherKeyId) {
	BlobCipherKeyIdCacheMapCItr itr = keyIdCache.find(baseCipherKeyId);
	if (itr == keyIdCache.end()) {
		throw encrypt_key_not_found();
	}
	return itr->second;
}

void BlobCipherKeyIdCache::insertBaseCipherKey(BlobCipherBaseKeyId baseCipherId,
                                               const uint8_t* baseCipher,
                                               int baseCipherLen) {
	ASSERT(baseCipherId > INVALID_CIPHER_KEY_ID);

	// BaseCipherKeys are immutable, ensure that cached value doesn't get updated.
	BlobCipherKeyIdCacheMapCItr itr = keyIdCache.find(baseCipherId);
	if (itr != keyIdCache.end()) {
		if (memcmp(itr->second->rawBaseCipher(), baseCipher, baseCipherLen) == 0) {
			TraceEvent("InsertBaseCipherKey_AlreadyPresent")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
			// Key is already present; nothing more to do.
			return;
		} else {
			TraceEvent("InsertBaseCipherKey_UpdateCipher")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
			throw encrypt_update_cipher();
		}
	}

	keyIdCache.emplace(baseCipherId, makeReference<BlobCipherKey>(domainId, baseCipherId, baseCipher, baseCipherLen));
	// Update the latest BaseCipherKeyId for the given encryption domain
	latestBaseCipherKeyId = baseCipherId;
}

void BlobCipherKeyIdCache::cleanup() {
	for (auto& keyItr : keyIdCache) {
		keyItr.second->reset();
	}

	keyIdCache.clear();
}

std::vector<Reference<BlobCipherKey>> BlobCipherKeyIdCache::getAllCipherKeys() {
	std::vector<Reference<BlobCipherKey>> cipherKeys;
	for (auto& keyItr : keyIdCache) {
		cipherKeys.push_back(keyItr.second);
	}
	return cipherKeys;
}

// BlobCipherKeyCache class methods

void BlobCipherKeyCache::insertCipherKey(const BlobCipherDomainId& domainId,
                                         const BlobCipherBaseKeyId& baseCipherId,
                                         const uint8_t* baseCipher,
                                         int baseCipherLen) {
	if (domainId == INVALID_DOMAIN_ID || baseCipherId == INVALID_CIPHER_KEY_ID) {
		throw encrypt_invalid_id();
	}

	try {
		auto domainItr = domainCacheMap.find(domainId);
		if (domainItr == domainCacheMap.end()) {
			// Add mapping to track new encryption domain
			Reference<BlobCipherKeyIdCache> keyIdCache = makeReference<BlobCipherKeyIdCache>(domainId);
			keyIdCache->insertBaseCipherKey(baseCipherId, baseCipher, baseCipherLen);
			domainCacheMap.emplace(domainId, keyIdCache);
		} else {
			// Track new baseCipher keys
			Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
			keyIdCache->insertBaseCipherKey(baseCipherId, baseCipher, baseCipherLen);
		}

		TraceEvent("InsertCipherKey").detail("DomainId", domainId).detail("BaseCipherKeyId", baseCipherId);
	} catch (Error& e) {
		TraceEvent("InsertCipherKey_Failed").detail("BaseCipherKeyId", baseCipherId).detail("DomainId", domainId);
		throw;
	}
}

Reference<BlobCipherKey> BlobCipherKeyCache::getLatestCipherKey(const BlobCipherDomainId& domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		TraceEvent("GetLatestCipherKey_DomainNotFound").detail("DomainId", domainId);
		throw encrypt_key_not_found();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	Reference<BlobCipherKey> cipherKey = keyIdCache->getLatestCipherKey();
	if ((now() - cipherKey->getCreationTime()) > BlobCipherKeyCache::CIPHER_KEY_CACHE_TTL_SEC) {
		TraceEvent("GetLatestCipherKey_ExpiredTTL")
		    .detail("DomainId", domainId)
		    .detail("BaseCipherId", cipherKey->getBaseCipherId());
		throw encrypt_key_ttl_expired();
	}

	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::getCipherKey(const BlobCipherDomainId& domainId,
                                                          const BlobCipherBaseKeyId& baseCipherId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		throw encrypt_key_not_found();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	return keyIdCache->getCipherByBaseCipherId(baseCipherId);
}

void BlobCipherKeyCache::resetEncyrptDomainId(const BlobCipherDomainId domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		throw encrypt_key_not_found();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	keyIdCache->cleanup();
	TraceEvent("ResetEncryptDomainId").detail("DomainId", domainId);
}

void BlobCipherKeyCache::cleanup() noexcept {
	BlobCipherKeyCache& instance = BlobCipherKeyCache::getInstance();
	for (auto& domainItr : instance.domainCacheMap) {
		Reference<BlobCipherKeyIdCache> keyIdCache = domainItr.second;
		keyIdCache->cleanup();
		TraceEvent("BlobCipherKeyCache_Cleanup").detail("DomainId", domainItr.first);
	}

	instance.domainCacheMap.clear();
}

std::vector<Reference<BlobCipherKey>> BlobCipherKeyCache::getAllCiphers(const BlobCipherDomainId& domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return {};
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	return keyIdCache->getAllCipherKeys();
}

// EncryptBlobCipher class methods

EncryptBlobCipherAes265Ctr::EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> key,
                                                       const uint8_t* cipherIV,
                                                       const int ivLen)
  : ctx(EVP_CIPHER_CTX_new()), cipherKey(key) {
	ASSERT(ivLen == AES_256_IV_LENGTH);
	memcpy(&iv[0], cipherIV, ivLen);

	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr) != 1) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, key.getPtr()->data(), cipherIV) != 1) {
		throw encrypt_ops_error();
	}
}

Reference<EncryptBuf> EncryptBlobCipherAes265Ctr::encrypt(const uint8_t* plaintext,
                                                          const int plaintextLen,
                                                          BlobCipherEncryptHeader* header,
                                                          Arena& arena) {
	TEST(true); // Encrypting data with BlobCipher

	Reference<EncryptBuf> encryptBuf = makeReference<EncryptBuf>(plaintextLen + AES_BLOCK_SIZE, arena);
	uint8_t* ciphertext = encryptBuf->begin();
	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, ciphertext, &bytes, plaintext, plaintextLen) != 1) {
		TraceEvent("Encrypt_UpdateFailed")
		    .detail("BaseCipherId", cipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", cipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	int finalBytes{ 0 };
	if (EVP_EncryptFinal_ex(ctx, ciphertext + bytes, &finalBytes) != 1) {
		TraceEvent("Encrypt_FinalFailed")
		    .detail("BaseCipherId", cipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", cipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	if ((bytes + finalBytes) != plaintextLen) {
		TraceEvent("Encrypt_UnexpectedCipherLen")
		    .detail("PlaintextLen", plaintextLen)
		    .detail("EncryptedBufLen", bytes + finalBytes);
		throw encrypt_ops_error();
	}

	// populate header details for the encrypted blob.
	header->flags.size = sizeof(BlobCipherEncryptHeader);
	header->flags.headerVersion = EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION;
	header->flags.encryptMode = BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR;
	header->baseCipherId = cipherKey->getBaseCipherId();
	header->encryptDomainId = cipherKey->getDomainId();
	header->salt = cipherKey->getSalt();
	memcpy(&header->iv[0], &iv[0], AES_256_IV_LENGTH);

	// Preserve checksum of encrypted bytes in the header; approach protects against disk induced bit-rot/flip
	// scenarios. AES CTR mode doesn't generate 'tag' by default as with schemes such as: AES 256 GCM.

	header->ciphertextChecksum = computeEncryptChecksum(ciphertext, bytes + finalBytes, cipherKey->getSalt(), arena);

	encryptBuf->setLogicalSize(plaintextLen);
	return encryptBuf;
}

EncryptBlobCipherAes265Ctr::~EncryptBlobCipherAes265Ctr() {
	if (ctx != nullptr) {
		EVP_CIPHER_CTX_free(ctx);
	}
}

// DecryptBlobCipher class methods

DecryptBlobCipherAes256Ctr::DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> key, const uint8_t* iv)
  : ctx(EVP_CIPHER_CTX_new()) {
	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, nullptr, nullptr, key.getPtr()->data(), iv)) {
		throw encrypt_ops_error();
	}
}

void DecryptBlobCipherAes256Ctr::verifyEncryptBlobHeader(const uint8_t* ciphertext,
                                                         const int ciphertextLen,
                                                         const BlobCipherEncryptHeader& header,
                                                         Arena& arena) {
	// validate header flag sanity
	if (header.flags.headerVersion != EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION ||
	    header.flags.encryptMode != BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR) {
		TraceEvent("VerifyEncryptBlobHeader")
		    .detail("HeaderVersion", header.flags.headerVersion)
		    .detail("HeaderMode", header.flags.encryptMode)
		    .detail("ExpectedVersion", EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION)
		    .detail("ExpectedMode", BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR);
		throw encrypt_header_metadata_mismatch();
	}

	// encrypted byte checksum sanity; protection against data bit-rot/flip.
	BlobCipherChecksum computed = computeEncryptChecksum(ciphertext, ciphertextLen, header.salt, arena);
	if (computed != header.ciphertextChecksum) {
		TraceEvent("VerifyEncryptBlobHeader_ChecksumMismatch")
		    .detail("HeaderVersion", header.flags.headerVersion)
		    .detail("HeaderMode", header.flags.encryptMode)
		    .detail("CiphertextChecksum", header.ciphertextChecksum)
		    .detail("ComputedCiphertextChecksum", computed);
		throw encrypt_header_checksum_mismatch();
	}
}

Reference<EncryptBuf> DecryptBlobCipherAes256Ctr::decrypt(const uint8_t* ciphertext,
                                                          const int ciphertextLen,
                                                          const BlobCipherEncryptHeader& header,
                                                          Arena& arena) {
	TEST(true); // Decrypting data with BlobCipher

	verifyEncryptBlobHeader(ciphertext, ciphertextLen, header, arena);

	Reference<EncryptBuf> decrypted = makeReference<EncryptBuf>(ciphertextLen + AES_BLOCK_SIZE, arena);
	uint8_t* plaintext = decrypted->begin();
	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, plaintext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		TraceEvent("Decrypt_UpdateFailed")
		    .detail("BaseCipherId", header.baseCipherId)
		    .detail("EncryptDomainId", header.encryptDomainId);
		throw encrypt_ops_error();
	}

	int finalBlobBytes{ 0 };
	if (EVP_DecryptFinal_ex(ctx, plaintext + bytesDecrypted, &finalBlobBytes) <= 0) {
		TraceEvent("Decrypt_FinalFailed")
		    .detail("BaseCipherId", header.baseCipherId)
		    .detail("EncryptDomainId", header.encryptDomainId);
		throw encrypt_ops_error();
	}

	if ((bytesDecrypted + finalBlobBytes) != ciphertextLen) {
		TraceEvent("Encrypt_UnexpectedPlaintextLen")
		    .detail("CiphertextLen", ciphertextLen)
		    .detail("DecryptedBufLen", bytesDecrypted + finalBlobBytes);
		throw encrypt_ops_error();
	}

	decrypted->setLogicalSize(ciphertextLen);
	return decrypted;
}

DecryptBlobCipherAes256Ctr::~DecryptBlobCipherAes256Ctr() {
	if (ctx != nullptr) {
		EVP_CIPHER_CTX_free(ctx);
	}
}

// HmacSha256DigestGen class methods

HmacSha256DigestGen::HmacSha256DigestGen(const unsigned char* key, size_t len) : ctx(HMAC_CTX_new()) {
	if (!HMAC_Init_ex(ctx, key, len, EVP_sha256(), nullptr)) {
		throw encrypt_ops_error();
	}
}

HmacSha256DigestGen::~HmacSha256DigestGen() {
	if (ctx != nullptr) {
		HMAC_CTX_free(ctx);
	}
}

StringRef HmacSha256DigestGen::digest(const unsigned char* data, size_t len, Arena& arena) {
	TEST(true); // Digest generation
	unsigned int digestLen = HMAC_size(ctx);
	auto digest = new (arena) unsigned char[digestLen];
	if (HMAC_Update(ctx, data, len) != 1) {
		throw encrypt_ops_error();
	}

	if (HMAC_Final(ctx, digest, &digestLen) != 1) {
		throw encrypt_ops_error();
	}
	return StringRef(digest, digestLen);
}

// Only used to link unit tests
void forceLinkBlobCipherTests() {}

// Tests cases includes:
// 1. Populate cache by inserting 'baseCipher' details for new encryptionDomainIds
// 2. Random lookup for cipherKeys and content validation
// 3. Inserting of 'identical' cipherKey (already cached) more than once works as desired.
// 4. Inserting of 'non-identical' cipherKey (already cached) more than once works as desired.
// 5. Validation encryption ops (correctness):
//  5.1. Encyrpt a buffer followed by decryption of the buffer, validate the contents.
//  5.2. Simulate anomolies such as: EncyrptionHeader corruption, checkSum mismatch / encryptionMode mismatch etc.
// 6. Cache cleanup
//  6.1  cleanup cipherKeys by given encryptDomainId
//  6.2. Cleanup all cached cipherKeys
TEST_CASE("flow/BlobCipher") {
	TraceEvent("BlobCipherTest_Start").log();
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
	const BlobCipherDomainId minDomainId = 1;
	const BlobCipherDomainId maxDomainId = deterministicRandom()->randomInt(minDomainId, minDomainId + 10) + 5;
	const BlobCipherBaseKeyId minBaseCipherKeyId = 100;
	const BlobCipherBaseKeyId maxBaseCipherKeyId =
	    deterministicRandom()->randomInt(minBaseCipherKeyId, minBaseCipherKeyId + 50) + 15;
	for (int dId = minDomainId; dId <= maxDomainId; dId++) {
		for (int kId = minBaseCipherKeyId; kId <= maxBaseCipherKeyId; kId++) {
			domainKeyMap[dId].emplace(kId, makeReference<BaseCipher>(dId, kId));
		}
	}
	ASSERT(domainKeyMap.size() == maxDomainId);

	// insert BlobCipher keys into BlobCipherKeyCache map and validate
	TraceEvent("BlobCipherTest_InsertKeys").log();
	BlobCipherKeyCache& cipherKeyCache = BlobCipherKeyCache::getInstance();
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;

			cipherKeyCache.insertCipherKey(
			    baseCipher->domainId, baseCipher->keyId, baseCipher->key.get(), baseCipher->len);
		}
	}
	TraceEvent("BlobCipherTest_InsertKeysDone").log();

	// validate the cipherKey lookups work as desired
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;
			Reference<BlobCipherKey> cipherKey = cipherKeyCache.getCipherKey(baseCipher->domainId, baseCipher->keyId);
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
	TraceEvent("BlobCipherTest_LooksupDone").log();

	// Ensure attemtping to insert existing cipherKey (identical) more than once is treated as a NOP
	try {
		Reference<BaseCipher> baseCipher = domainKeyMap[minDomainId][minBaseCipherKeyId];
		cipherKeyCache.insertCipherKey(baseCipher->domainId, baseCipher->keyId, baseCipher->key.get(), baseCipher->len);
	} catch (Error& e) {
		throw;
	}
	TraceEvent("BlobCipherTest_ReinsertIdempotentKeyDone").log();

	// Ensure attemtping to insert an existing cipherKey (modified) fails with appropriate error
	try {
		Reference<BaseCipher> baseCipher = domainKeyMap[minDomainId][minBaseCipherKeyId];
		uint8_t rawCipher[baseCipher->len];
		memcpy(rawCipher, baseCipher->key.get(), baseCipher->len);
		// modify few bytes in the cipherKey
		for (int i = 2; i < 5; i++) {
			rawCipher[i]++;
		}
		cipherKeyCache.insertCipherKey(baseCipher->domainId, baseCipher->keyId, &rawCipher[0], baseCipher->len);
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_update_cipher) {
			throw;
		}
	}
	TraceEvent("BlobCipherTest_ReinsertNonIdempotentKeyDone").log();

	// Validate Encyrption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache.getLatestCipherKey(minDomainId);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	generateRandomData(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	generateRandomData(&iv[0], AES_256_IV_LENGTH);

	// validate basic encrypt followed by decrypt operation
	EncryptBlobCipherAes265Ctr encryptor(cipherKey, iv, AES_256_IV_LENGTH);
	BlobCipherEncryptHeader header;
	Reference<EncryptBuf> encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);

	ASSERT(encrypted->getLogicalSize() == bufLen);
	ASSERT(memcmp(&orgData[0], encrypted->begin(), bufLen) != 0);
	ASSERT(header.flags.headerVersion == EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION);
	ASSERT(header.flags.encryptMode == BLOB_CIPHER_ENCRYPT_MODE_AES_256_CTR);

	TraceEvent("BlobCipherTest_EncryptDone")
	    .detail("HeaderVersion", header.flags.headerVersion)
	    .detail("HeaderEncryptMode", header.flags.encryptMode)
	    .detail("DomainId", header.encryptDomainId)
	    .detail("BaseCipherId", header.baseCipherId)
	    .detail("HeaderChecksum", header.ciphertextChecksum);

	Reference<BlobCipherKey> encyrptKey = cipherKeyCache.getCipherKey(header.encryptDomainId, header.baseCipherId);
	ASSERT(encyrptKey->isEqual(cipherKey));
	DecryptBlobCipherAes256Ctr decryptor(encyrptKey, &header.iv[0]);
	Reference<EncryptBuf> decrypted = decryptor.decrypt(encrypted->begin(), bufLen, header, arena);

	ASSERT(decrypted->getLogicalSize() == bufLen);
	ASSERT(memcmp(decrypted->begin(), &orgData[0], bufLen) == 0);

	TraceEvent("BlobCipherTest_DecryptDone").log();

	// induce encryption header corruption - headerVersion corrupted
	header.flags.headerVersion += 1;
	try {
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, header, arena);
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		header.flags.headerVersion -= 1;
	}

	// induce encryption header corruption - encryptionMode corrupted
	header.flags.encryptMode += 1;
	try {
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, header, arena);
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		header.flags.encryptMode -= 1;
	}

	// induce encryption header corruption - checksum mismatch
	header.ciphertextChecksum += 1;
	try {
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, header, arena);
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_checksum_mismatch) {
			throw;
		}
		header.ciphertextChecksum -= 1;
	}

	// Validate dropping encyrptDomainId cached keys
	const BlobCipherDomainId candidate = deterministicRandom()->randomInt(minDomainId, maxDomainId);
	cipherKeyCache.resetEncyrptDomainId(candidate);
	std::vector<Reference<BlobCipherKey>> cachedKeys = cipherKeyCache.getAllCiphers(candidate);
	ASSERT(cachedKeys.empty());

	// Validate dropping all cached cipherKeys
	cipherKeyCache.cleanup();
	for (int dId = minDomainId; dId < maxDomainId; dId++) {
		std::vector<Reference<BlobCipherKey>> cachedKeys = cipherKeyCache.getAllCiphers(dId);
		ASSERT(cachedKeys.empty());
	}

	TraceEvent("BlobCipherTest_Done").log();
	return Void();
}

BlobCipherChecksum computeEncryptChecksum(const uint8_t* payload,
                                          const int payloadLen,
                                          const BlobCipherRandomSalt& salt,
                                          Arena& arena) {
	// FIPS compliance recommendation is to leverage cryptographic digest mechanism to generate checksum
	// Leverage HMAC_SHA256 using header.randomSalt as the initialization 'key' for the hmac digest.

	HmacSha256DigestGen hmacGenerator((const uint8_t*)&salt, sizeof(salt));
	StringRef digest = hmacGenerator.digest(payload, payloadLen, arena);
	ASSERT(digest.size() >= sizeof(BlobCipherChecksum));

	BlobCipherChecksum checksum;
	memcpy((uint8_t*)&checksum, digest.begin(), sizeof(BlobCipherChecksum));
	return checksum;
}

#endif // ENCRYPTION_ENABLED
