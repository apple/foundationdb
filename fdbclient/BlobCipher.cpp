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

#include "fdbclient/BlobCipher.h"

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FileIdentifier.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/ObjectSerializer.h"
#include "flow/Platform.h"
#include "flow/ProtocolVersion.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/xxhash.h"
#include "include/fdbclient/BlobCipher.h"

#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>

#ifndef _WIN32
#include <unistd.h>
#else
#include <io.h>
#endif

#define BLOB_CIPHER_DEBUG false
#define BLOB_CIPHER_SERIALIZATION_CHECKS false

namespace {
void validateEncryptHeaderFlagVersion(const int flagsVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	if (flagsVersion > CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION) {
		TraceEvent("EncryptHeaderUnsupportedFlagVersion")
		    .detail("MaxSupportedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION)
		    .detail("Version", flagsVersion);
		throw not_implemented();
	}
}

void validateEncryptHeaderAlgoHeaderVersion(const EncryptCipherMode cipherMode,
                                            const EncryptAuthTokenMode authMode,
                                            const EncryptAuthTokenAlgo authAlgo,
                                            const int version) {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	if (cipherMode != ENCRYPT_CIPHER_MODE_AES_256_CTR) {
		TraceEvent("EncryptHeaderUnsupportedEncryptCipherMode")
		    .detail("MaxSupportedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION)
		    .detail("CipherMode", cipherMode);
		throw not_implemented();
	}

	int maxSupportedVersion = -1;
	if (authMode == ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION;
	} else {
		ASSERT_EQ(authMode, ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);

		if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA) {
			maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION;
		} else if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
			maxSupportedVersion = CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;
		} else {
			// Unknown encryption authentication algo
		}
	}

	if (version > maxSupportedVersion || maxSupportedVersion == -1) {
		TraceEvent("EncryptHeaderUnsupportedEncryptAuthToken")
		    .detail("CipherMode", cipherMode)
		    .detail("AuthMode", authMode)
		    .detail("AuthAlgo", authAlgo)
		    .detail("AlgoHeaderVersion", version)
		    .detail("MaxSsupportedVersion", maxSupportedVersion);
		throw not_implemented();
	}
}
} // namespace

// BlobCipherEncryptHeaderRef

uint32_t BlobCipherEncryptHeaderRef::getHeaderSize(const int flagVersion,
                                                   const int algoVersion,
                                                   const EncryptCipherMode cipherMode,
                                                   const EncryptAuthTokenMode authMode,
                                                   const EncryptAuthTokenAlgo authAlgo) {
	if (flagVersion != 1) {
		TraceEvent("BlobCipherGetHeaderSizeInvalidFlagVersion").detail("FlagVersion", flagVersion);
		throw not_implemented();
	}
	if (algoVersion != 1) {
		TraceEvent("BlobCipherGetHeaderSizeInvalidAlgoVersion").detail("AlgoVersion", algoVersion);
		throw not_implemented();
	}

	uint32_t total = sizeof(BlobCipherEncryptHeaderFlagsV1) + 2; // 2 bytes of std::variant index

	if (cipherMode != ENCRYPT_CIPHER_MODE_AES_256_CTR) {
		throw not_implemented();
	}

	if (authMode == ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		total += AesCtrNoAuth::getSize();
	} else {
		if (authAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA) {
			total += AesCtrWithHmac::getSize();
		} else {
			ASSERT_EQ(authAlgo, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC);
			total += AesCtrWithCmac::getSize();
		}
	}
	return total;
}

const uint8_t* BlobCipherEncryptHeaderRef::getIV() const {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	return std::visit([](auto& h) { return h.v1.iv; }, algoHeader);
}

template <class>
inline constexpr bool always_false_v = false;

const EncryptHeaderCipherDetails BlobCipherEncryptHeaderRef::getCipherDetails() const {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [](auto&& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesCtrNoAuth>) {
			    return EncryptHeaderCipherDetails(h.v1.cipherTextDetails);
		    } else if constexpr (std::is_same_v<T, AesCtrWithHmac> || std::is_same_v<T, AesCtrWithCmac>) {
			    return EncryptHeaderCipherDetails(h.v1.cipherTextDetails, h.v1.cipherHeaderDetails);
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption authentication");
		    }
	    },
	    algoHeader);
}

EncryptAuthTokenMode BlobCipherEncryptHeaderRef::getAuthTokenMode() const {
	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [](auto&& f) {
		    using T = std::decay_t<decltype(f)>;
		    if constexpr (std::is_same_v<T, BlobCipherEncryptHeaderFlagsV1>) {
			    return (EncryptAuthTokenMode)f.authTokenMode;
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption flag header");
		    }
	    },
	    flags);
}

void BlobCipherEncryptHeaderRef::validateEncryptionHeaderDetails(const BlobCipherDetails& textCipherDetails,
                                                                 const BlobCipherDetails& headerCipherDetails,
                                                                 const StringRef& ivRef) const {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	validateEncryptHeaderFlagVersion(flagsVersion());
	ASSERT_EQ(flagsVersion(), 1);

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(this->flags);

	validateEncryptHeaderAlgoHeaderVersion((EncryptCipherMode)flags.encryptMode,
	                                       (EncryptAuthTokenMode)flags.authTokenMode,
	                                       (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                                       algoHeaderVersion());
	ASSERT_EQ(algoHeaderVersion(), 1);

	BlobCipherDetails persistedTextCipherDetails;
	BlobCipherDetails persistedHeaderCipherDetails;
	uint8_t* persistedIV = nullptr;

	// TODO: Replace with "Overload visitor pattern" someday.
	return std::visit(
	    [&persistedTextCipherDetails, &persistedHeaderCipherDetails, &persistedIV](auto&& h) {
		    using T = std::decay_t<decltype(h)>;
		    if constexpr (std::is_same_v<T, AesCtrNoAuth>) {
			    persistedTextCipherDetails = h.v1.cipherTextDetails;
			    persistedIV = (uint8_t*)&h.v1.iv[0];
		    } else if constexpr (std::is_same_v<T, AesCtrWithHmac> || std::is_same_v<T, AesCtrWithCmac>) {
			    persistedTextCipherDetails = h.v1.cipherTextDetails;
			    persistedHeaderCipherDetails = h.v1.cipherHeaderDetails;
			    persistedIV = (uint8_t*)&h.v1.iv[0];
		    } else {
			    static_assert(always_false_v<T>, "Unknown encryption authentication");
		    }
	    },
	    algoHeader);

	// Validate encryption header 'cipherHeader' details sanity
	if (flags.authTokenMode != ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE &&
	    headerCipherDetails != persistedHeaderCipherDetails) {
		TraceEvent(SevError, "ValidateEncryptHeaderMismatch")
		    .detail("HeaderDomainId", headerCipherDetails.encryptDomainId)
		    .detail("PersistedHeaderDomainId", persistedHeaderCipherDetails.encryptDomainId)
		    .detail("HeaderBaseCipherId", headerCipherDetails.baseCipherId)
		    .detail("ExpectedHeaderBaseCipherId", persistedHeaderCipherDetails.baseCipherId)
		    .detail("HeaderSalt", headerCipherDetails.salt)
		    .detail("ExpectedHeaderSalt", persistedHeaderCipherDetails.salt);
		throw encrypt_header_metadata_mismatch();
	}
	// Validate encryption header 'cipherText' details sanity
	if (textCipherDetails != persistedTextCipherDetails) {
		TraceEvent(SevError, "ValidateEncryptHeaderMismatch")
		    .detail("TextDomainId", textCipherDetails.encryptDomainId)
		    .detail("PersistedTextDomainId", persistedTextCipherDetails.encryptDomainId)
		    .detail("TextBaseCipherId", textCipherDetails.baseCipherId)
		    .detail("PersistedTextBaseCipherId", persistedTextCipherDetails.encryptDomainId)
		    .detail("TextSalt", textCipherDetails.salt)
		    .detail("PersistedTextSalt", persistedTextCipherDetails.salt);
		throw encrypt_header_metadata_mismatch();
	}
	// Validate 'Initialization Vector' sanity
	if (memcmp(ivRef.begin(), persistedIV, AES_256_IV_LENGTH) != 0) {
		TraceEvent(SevError, "EncryptionHeader_IVMismatch")
		    .detail("IVChecksum", XXH3_64bits(ivRef.begin(), ivRef.size()))
		    .detail("ExpectedIVChecksum", XXH3_64bits(persistedIV, AES_256_IV_LENGTH));
		throw encrypt_header_metadata_mismatch();
	}
}

// BlobCipherMetrics methods

BlobCipherMetrics::CounterSet::CounterSet(CounterCollection& cc, std::string name)
  : encryptCPUTimeNS(name + "EncryptCPUTimeNS", cc), decryptCPUTimeNS(name + "DecryptCPUTimeNS", cc),
    getCipherKeysLatency(name + "GetCipherKeysLatency",
                         UID(),
                         FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL,
                         FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY),
    getLatestCipherKeysLatency(name + "GetLatestCipherKeysLatency",
                               UID(),
                               FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL,
                               FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY) {}

BlobCipherMetrics::BlobCipherMetrics()
  : cc("BlobCipher"), cipherKeyCacheHit("CipherKeyCacheHit", cc), cipherKeyCacheMiss("CipherKeyCacheMiss", cc),
    cipherKeyCacheExpired("CipherKeyCacheExpired", cc), latestCipherKeyCacheHit("LatestCipherKeyCacheHit", cc),
    latestCipherKeyCacheMiss("LatestCipherKeyCacheMiss", cc),
    latestCipherKeyCacheNeedsRefresh("LatestCipherKeyCacheNeedsRefresh", cc),
    getCipherKeysLatency("GetCipherKeysLatency",
                         UID(),
                         FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL,
                         FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY),
    getLatestCipherKeysLatency("GetLatestCipherKeysLatency",
                               UID(),
                               FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL,
                               FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY),
    getBlobMetadataLatency("GetBlobMetadataLatency",
                           UID(),
                           FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL,
                           FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_SKETCH_ACCURACY),
    counterSets({ CounterSet(cc, "TLog"),
                  CounterSet(cc, "TLogPostResolution"),
                  CounterSet(cc, "KVMemory"),
                  CounterSet(cc, "KVRedwood"),
                  CounterSet(cc, "BlobGranule"),
                  CounterSet(cc, "Backup"),
                  CounterSet(cc, "Restore"),
                  CounterSet(cc, "Test") }) {
	specialCounter(cc, "CacheSize", []() { return BlobCipherKeyCache::getInstance()->getSize(); });
	traceFuture = cc.traceCounters("BlobCipherMetrics", UID(), FLOW_KNOBS->ENCRYPT_KEY_CACHE_LOGGING_INTERVAL);
}

std::string toString(BlobCipherMetrics::UsageType type) {
	switch (type) {
	case BlobCipherMetrics::UsageType::TLOG:
		return "TLog";
	case BlobCipherMetrics::UsageType::TLOG_POST_RESOLUTION:
		return "TLogPostResolution";
	case BlobCipherMetrics::UsageType::KV_MEMORY:
		return "KVMemory";
	case BlobCipherMetrics::UsageType::KV_REDWOOD:
		return "KVRedwood";
	case BlobCipherMetrics::UsageType::BLOB_GRANULE:
		return "BlobGranule";
	case BlobCipherMetrics::UsageType::BACKUP:
		return "Backup";
	case BlobCipherMetrics::UsageType::RESTORE:
		return "Restore";
	case BlobCipherMetrics::UsageType::TEST:
		return "Test";
	default:
		ASSERT(false);
		return "";
	}
}

// BlobCipherKey class methods

BlobCipherKey::BlobCipherKey(const EncryptCipherDomainId& domainId,
                             const EncryptCipherBaseKeyId& baseCiphId,
                             const uint8_t* baseCiph,
                             int baseCiphLen,
                             const int64_t refreshAt,
                             const int64_t expireAt) {
	// Salt generated is used while applying HMAC Key derivation, hence, not using crypto-secure hash algorithm is
	// ok. Further, 'deterministic' salt generation is used to preserve simulation determinism properties.
	EncryptCipherRandomSalt salt;
	if (g_network->isSimulated()) {
		salt = deterministicRandom()->randomUInt64();
	} else {
		salt = nondeterministicRandom()->randomUInt64();
	}

	// Support two type of CipherKeys: 'revocable' & 'non-revocable' ciphers.
	// In all cases, either cipherKey never expires i.e. refreshAt == infinite, or, refreshAt needs <= expireAt
	// timestamp.
	ASSERT(refreshAt == std::numeric_limits<int64_t>::max() || (refreshAt <= expireAt));

	initKey(domainId, baseCiph, baseCiphLen, baseCiphId, salt, refreshAt, expireAt);
}

BlobCipherKey::BlobCipherKey(const EncryptCipherDomainId& domainId,
                             const EncryptCipherBaseKeyId& baseCiphId,
                             const uint8_t* baseCiph,
                             int baseCiphLen,
                             const EncryptCipherRandomSalt& salt,
                             const int64_t refreshAt,
                             const int64_t expireAt) {
	initKey(domainId, baseCiph, baseCiphLen, baseCiphId, salt, refreshAt, expireAt);
}

void BlobCipherKey::initKey(const EncryptCipherDomainId& domainId,
                            const uint8_t* baseCiph,
                            int baseCiphLen,
                            const EncryptCipherBaseKeyId& baseCiphId,
                            const EncryptCipherRandomSalt& salt,
                            const int64_t refreshAt,
                            const int64_t expireAt) {
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
	// update cipher 'refresh' and 'expire' TS
	refreshAtTS = refreshAt;
	expireAtTS = expireAt;

#if BLOB_CIPHER_DEBUG
	TraceEvent(SevDebug, "BlobCipherKeyInit")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("BaseCipherLen", baseCipherLen)
	    .detail("RandomSalt", randomSalt)
	    .detail("RefreshAt", refreshAtTS)
	    .detail("ExpireAtTS", expireAtTS);
#endif
}

void BlobCipherKey::applyHmacSha256Derivation() {
	Arena arena;
	uint8_t buf[baseCipherLen + sizeof(EncryptCipherRandomSalt)];
	memcpy(&buf[0], baseCipher.get(), baseCipherLen);
	memcpy(&buf[0] + baseCipherLen, &randomSalt, sizeof(EncryptCipherRandomSalt));
	HmacSha256DigestGen hmacGen(baseCipher.get(), baseCipherLen);
	unsigned int digestLen = hmacGen.digest(
	    { { &buf[0], baseCipherLen + sizeof(EncryptCipherRandomSalt) } }, cipher.get(), AUTH_TOKEN_HMAC_SHA_SIZE);
	if (digestLen < AES_256_KEY_LENGTH) {
		memcpy(cipher.get() + digestLen, buf, AES_256_KEY_LENGTH - digestLen);
	}
}

void BlobCipherKey::reset() {
	memset(baseCipher.get(), 0, baseCipherLen);
	memset(cipher.get(), 0, AES_256_KEY_LENGTH);
}

// BlobKeyIdCache class methods

BlobCipherKeyIdCache::BlobCipherKeyIdCache(EncryptCipherDomainId dId, size_t* sizeStat)
  : domainId(dId), latestBaseCipherKeyId(), latestRandomSalt(), sizeStat(sizeStat) {
	ASSERT(sizeStat != nullptr);
	TraceEvent(SevInfo, "BlobCipherKeyIdCacheInit").detail("DomainId", domainId);
}

BlobCipherKeyIdCacheKey BlobCipherKeyIdCache::getCacheKey(const EncryptCipherBaseKeyId& baseCipherKeyId,
                                                          const EncryptCipherRandomSalt& salt) {
	if (baseCipherKeyId == INVALID_ENCRYPT_CIPHER_KEY_ID || salt == INVALID_ENCRYPT_RANDOM_SALT) {
		throw encrypt_invalid_id();
	}
	return std::make_pair(baseCipherKeyId, salt);
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::getLatestCipherKey() {
	if (!latestBaseCipherKeyId.present()) {
		return Reference<BlobCipherKey>();
	}
	ASSERT_NE(latestBaseCipherKeyId.get(), INVALID_ENCRYPT_CIPHER_KEY_ID);
	ASSERT(latestRandomSalt.present());
	ASSERT_NE(latestRandomSalt.get(), INVALID_ENCRYPT_RANDOM_SALT);

	return getCipherByBaseCipherId(latestBaseCipherKeyId.get(), latestRandomSalt.get());
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::getCipherByBaseCipherId(const EncryptCipherBaseKeyId& baseCipherKeyId,
                                                                       const EncryptCipherRandomSalt& salt) {
	BlobCipherKeyIdCacheMapCItr itr = keyIdCache.find(getCacheKey(baseCipherKeyId, salt));
	if (itr == keyIdCache.end()) {
		return Reference<BlobCipherKey>();
	}
	return itr->second;
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::insertBaseCipherKey(const EncryptCipherBaseKeyId& baseCipherId,
                                                                   const uint8_t* baseCipher,
                                                                   int baseCipherLen,
                                                                   const int64_t refreshAt,
                                                                   const int64_t expireAt) {
	ASSERT_GT(baseCipherId, INVALID_ENCRYPT_CIPHER_KEY_ID);

	// BaseCipherKeys are immutable, given the routine invocation updates 'latestCipher',
	// ensure no key-tampering is done
	Reference<BlobCipherKey> latestCipherKey = getLatestCipherKey();
	if (latestCipherKey.isValid() && latestCipherKey->getBaseCipherId() == baseCipherId) {
		if (memcmp(latestCipherKey->rawBaseCipher(), baseCipher, baseCipherLen) == 0) {
#if BLOB_CIPHER_DEBUG
			TraceEvent(SevDebug, "InsertBaseCipherKeyAlreadyPresent")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
#endif

			// Key is already present; nothing more to do.
			return latestCipherKey;
		} else {
			TraceEvent(SevInfo, "BlobCipherUpdatetBaseCipherKey")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
			throw encrypt_update_cipher();
		}
	}

	TraceEvent(SevInfo, "BlobCipherKeyInsertBaseCipherKeyLatest")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("RefreshAt", refreshAt)
	    .detail("ExpireAt", expireAt);

	Reference<BlobCipherKey> cipherKey =
	    makeReference<BlobCipherKey>(domainId, baseCipherId, baseCipher, baseCipherLen, refreshAt, expireAt);
	BlobCipherKeyIdCacheKey cacheKey = getCacheKey(cipherKey->getBaseCipherId(), cipherKey->getSalt());
	keyIdCache.emplace(cacheKey, cipherKey);

	// Update the latest BaseCipherKeyId for the given encryption domain
	latestBaseCipherKeyId = baseCipherId;
	latestRandomSalt = cipherKey->getSalt();

	(*sizeStat)++;
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyIdCache::insertBaseCipherKey(const EncryptCipherBaseKeyId& baseCipherId,
                                                                   const uint8_t* baseCipher,
                                                                   int baseCipherLen,
                                                                   const EncryptCipherRandomSalt& salt,
                                                                   const int64_t refreshAt,
                                                                   const int64_t expireAt) {
	ASSERT_NE(baseCipherId, INVALID_ENCRYPT_CIPHER_KEY_ID);
	ASSERT_NE(salt, INVALID_ENCRYPT_RANDOM_SALT);

	BlobCipherKeyIdCacheKey cacheKey = getCacheKey(baseCipherId, salt);

	// BaseCipherKeys are immutable, ensure that cached value doesn't get updated.
	BlobCipherKeyIdCacheMapCItr itr = keyIdCache.find(cacheKey);
	if (itr != keyIdCache.end()) {
		if (memcmp(itr->second->rawBaseCipher(), baseCipher, baseCipherLen) == 0) {
#if BLOB_CIPHER_DEBUG
			TraceEvent(SevDebug, "InsertBaseCipherKeyAlreadyPresent")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
#endif

			// Key is already present; nothing more to do.
			return itr->second;
		} else {
			TraceEvent(SevInfo, "BlobCipherUpdateBaseCipherKey")
			    .detail("BaseCipherKeyId", baseCipherId)
			    .detail("DomainId", domainId);
			throw encrypt_update_cipher();
		}
	}

	TraceEvent(SevInfo, "BlobCipherKeyInsertBaseCipherKey")
	    .detail("DomainId", domainId)
	    .detail("BaseCipherId", baseCipherId)
	    .detail("Salt", salt)
	    .detail("RefreshAt", refreshAt)
	    .detail("ExpireAt", expireAt);

	Reference<BlobCipherKey> cipherKey =
	    makeReference<BlobCipherKey>(domainId, baseCipherId, baseCipher, baseCipherLen, salt, refreshAt, expireAt);
	keyIdCache.emplace(cacheKey, cipherKey);
	(*sizeStat)++;
	return cipherKey;
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

Reference<BlobCipherKey> BlobCipherKeyCache::insertCipherKey(const EncryptCipherDomainId& domainId,
                                                             const EncryptCipherBaseKeyId& baseCipherId,
                                                             const uint8_t* baseCipher,
                                                             int baseCipherLen,
                                                             const int64_t refreshAt,
                                                             const int64_t expireAt) {
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID || baseCipherId == INVALID_ENCRYPT_CIPHER_KEY_ID) {
		throw encrypt_invalid_id();
	}

	Reference<BlobCipherKey> cipherKey;

	try {
		auto domainItr = domainCacheMap.find(domainId);
		if (domainItr == domainCacheMap.end()) {
			// Add mapping to track new encryption domain
			Reference<BlobCipherKeyIdCache> keyIdCache = makeReference<BlobCipherKeyIdCache>(domainId, &size);
			cipherKey = keyIdCache->insertBaseCipherKey(baseCipherId, baseCipher, baseCipherLen, refreshAt, expireAt);
			domainCacheMap.emplace(domainId, keyIdCache);
		} else {
			// Track new baseCipher keys
			Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
			cipherKey = keyIdCache->insertBaseCipherKey(baseCipherId, baseCipher, baseCipherLen, refreshAt, expireAt);
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "BlobCipherInsertCipherKeyFailed")
		    .detail("BaseCipherKeyId", baseCipherId)
		    .detail("DomainId", domainId);
		throw;
	}
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::insertCipherKey(const EncryptCipherDomainId& domainId,
                                                             const EncryptCipherBaseKeyId& baseCipherId,
                                                             const uint8_t* baseCipher,
                                                             int baseCipherLen,
                                                             const EncryptCipherRandomSalt& salt,
                                                             const int64_t refreshAt,
                                                             const int64_t expireAt) {
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID || baseCipherId == INVALID_ENCRYPT_CIPHER_KEY_ID ||
	    salt == INVALID_ENCRYPT_RANDOM_SALT) {
		throw encrypt_invalid_id();
	}

	Reference<BlobCipherKey> cipherKey;
	try {
		auto domainItr = domainCacheMap.find(domainId);
		if (domainItr == domainCacheMap.end()) {
			// Add mapping to track new encryption domain
			Reference<BlobCipherKeyIdCache> keyIdCache = makeReference<BlobCipherKeyIdCache>(domainId, &size);
			cipherKey =
			    keyIdCache->insertBaseCipherKey(baseCipherId, baseCipher, baseCipherLen, salt, refreshAt, expireAt);
			domainCacheMap.emplace(domainId, keyIdCache);
		} else {
			// Track new baseCipher keys
			Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
			cipherKey =
			    keyIdCache->insertBaseCipherKey(baseCipherId, baseCipher, baseCipherLen, salt, refreshAt, expireAt);
		}
	} catch (Error& e) {
		TraceEvent(SevWarn, "BlobCipherInsertCipherKey_Failed")
		    .detail("BaseCipherKeyId", baseCipherId)
		    .detail("DomainId", domainId)
		    .detail("Salt", salt);
		throw;
	}
	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::getLatestCipherKey(const EncryptCipherDomainId& domainId) {
	if (domainId == INVALID_ENCRYPT_DOMAIN_ID) {
		TraceEvent(SevWarn, "BlobCipherGetLatestCipherKeyInvalidID").detail("DomainId", domainId);
		throw encrypt_invalid_id();
	}
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		TraceEvent(SevInfo, "BlobCipherGetLatestCipherKeyDomainNotFound").detail("DomainId", domainId);
		return Reference<BlobCipherKey>();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	Reference<BlobCipherKey> cipherKey = keyIdCache->getLatestCipherKey();

	// Ensure 'freshness' guarantees for the latestCipher
	if (cipherKey.isValid()) {
		if (cipherKey->needsRefresh()) {
#if BLOB_CIPHER_DEBUG
			TraceEvent("SevDebug, BlobCipherGetLatestNeedsRefresh")
			    .detail("DomainId", domainId)
			    .detail("Now", now())
			    .detail("RefreshAt", cipherKey->getRefreshAtTS());
#endif
			++BlobCipherMetrics::getInstance()->latestCipherKeyCacheNeedsRefresh;
			return Reference<BlobCipherKey>();
		}
		++BlobCipherMetrics::getInstance()->latestCipherKeyCacheHit;
	} else {
		++BlobCipherMetrics::getInstance()->latestCipherKeyCacheMiss;
	}

	return cipherKey;
}

Reference<BlobCipherKey> BlobCipherKeyCache::getCipherKey(const EncryptCipherDomainId& domainId,
                                                          const EncryptCipherBaseKeyId& baseCipherId,
                                                          const EncryptCipherRandomSalt& salt) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return Reference<BlobCipherKey>();
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	Reference<BlobCipherKey> cipherKey = keyIdCache->getCipherByBaseCipherId(baseCipherId, salt);

	// Ensure 'liveness' guarantees for the cipher
	if (cipherKey.isValid()) {
		if (cipherKey->isExpired()) {
#if BLOB_CIPHER_DEBUG
			TraceEvent(SevDebug, "BlobCipherGetCipherExpired")
			    .detail("DomainId", domainId)
			    .detail("BaseCipherId", baseCipherId)
			    .detail("Now", now())
			    .detail("ExpireAt", cipherKey->getExpireAtTS());
#endif
			++BlobCipherMetrics::getInstance()->cipherKeyCacheExpired;
			return Reference<BlobCipherKey>();
		}
		++BlobCipherMetrics::getInstance()->cipherKeyCacheHit;
	} else {
		++BlobCipherMetrics::getInstance()->cipherKeyCacheMiss;
	}

	return cipherKey;
}

void BlobCipherKeyCache::resetEncryptDomainId(const EncryptCipherDomainId domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return;
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	ASSERT(keyIdCache->getSize() <= size);
	size -= keyIdCache->getSize();
	keyIdCache->cleanup();
	TraceEvent(SevInfo, "BlobCipherResetEncryptDomainId").detail("DomainId", domainId);
}

void BlobCipherKeyCache::cleanup() noexcept {
	Reference<BlobCipherKeyCache> instance = BlobCipherKeyCache::getInstance();

	TraceEvent(SevInfo, "BlobCipherKeyCacheCleanup").log();

	for (auto& domainItr : instance->domainCacheMap) {
		Reference<BlobCipherKeyIdCache> keyIdCache = domainItr.second;
		keyIdCache->cleanup();
		TraceEvent(SevInfo, "BlobCipherKeyCacheCleanup").detail("DomainId", domainItr.first);
	}

	instance->domainCacheMap.clear();
	instance->size = 0;
}

std::vector<Reference<BlobCipherKey>> BlobCipherKeyCache::getAllCiphers(const EncryptCipherDomainId& domainId) {
	auto domainItr = domainCacheMap.find(domainId);
	if (domainItr == domainCacheMap.end()) {
		return {};
	}

	Reference<BlobCipherKeyIdCache> keyIdCache = domainItr->second;
	return keyIdCache->getAllCipherKeys();
}

int getEncryptAlgoHeaderVersion(const EncryptAuthTokenMode mode, const EncryptAuthTokenAlgo algo) {
	if (mode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_NO_AUTH_VERSION;
	} else {
		ASSERT_EQ(mode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
		if (algo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
			return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;
		} else {
			ASSERT_EQ(algo, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA);
			return CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION;
		}
	}
}

// EncryptBlobCipherAes265Ctr class methods

EncryptBlobCipherAes265Ctr::EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* cipherIV,
                                                       const int ivLen,
                                                       const EncryptAuthTokenMode mode,
                                                       BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()), textCipherKey(tCipherKey), headerCipherKeyOpt(hCipherKeyOpt), authTokenMode(mode),
    usageType(usageType) {
	ASSERT_EQ(ivLen, AES_256_IV_LENGTH);
	authTokenAlgo = getAuthTokenAlgoFromMode(authTokenMode);
	memcpy(&iv[0], cipherIV, ivLen);
	init();
}

EncryptBlobCipherAes265Ctr::EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* cipherIV,
                                                       const int ivLen,
                                                       const EncryptAuthTokenMode mode,
                                                       const EncryptAuthTokenAlgo algo,
                                                       BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()), textCipherKey(tCipherKey), headerCipherKeyOpt(hCipherKeyOpt), authTokenMode(mode),
    authTokenAlgo(algo), usageType(usageType) {
	ASSERT_EQ(ivLen, AES_256_IV_LENGTH);
	memcpy(&iv[0], cipherIV, ivLen);
	init();
}

EncryptBlobCipherAes265Ctr::EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const EncryptAuthTokenMode mode,
                                                       BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()), textCipherKey(tCipherKey), headerCipherKeyOpt(hCipherKeyOpt), authTokenMode(mode),
    usageType(usageType) {
	authTokenAlgo = getAuthTokenAlgoFromMode(authTokenMode);
	deterministicRandom()->randomBytes(iv, AES_256_IV_LENGTH);
	init();
}

EncryptBlobCipherAes265Ctr::EncryptBlobCipherAes265Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const EncryptAuthTokenMode mode,
                                                       const EncryptAuthTokenAlgo algo,
                                                       BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()), textCipherKey(tCipherKey), headerCipherKeyOpt(hCipherKeyOpt), authTokenMode(mode),
    authTokenAlgo(algo), usageType(usageType) {
	deterministicRandom()->randomBytes(iv, AES_256_IV_LENGTH);
	init();
}

void EncryptBlobCipherAes265Ctr::init() {
	ASSERT(textCipherKey.isValid());
	if (FLOW_KNOBS->ENCRYPT_HEADER_AUTH_TOKEN_ENABLED) {
		ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());
	}

	if (!isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo)) {
		TraceEvent(SevWarn, "InvalidAuthTokenDetails")
		    .detail("TokenMode", authTokenMode)
		    .detail("TokenAlgo", authTokenAlgo);
		throw internal_error();
	}

	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr) != 1) {
		throw encrypt_ops_error();
	}
	if (EVP_EncryptInit_ex(ctx, nullptr, nullptr, textCipherKey.getPtr()->data(), iv) != 1) {
		throw encrypt_ops_error();
	}
}

template <class Params>
void EncryptBlobCipherAes265Ctr::setCipherAlgoHeaderWithAuthV1(const uint8_t* ciphertext,
                                                               const int ciphertextLen,
                                                               const BlobCipherEncryptHeaderFlagsV1& flags,
                                                               BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());

	// Construct algorithm specific details except 'authToken', serialize the details into 'headerRef' to allow
	// authToken generation
	AesCtrWithAuthV1<Params> algoHeader(
	    BlobCipherDetails(textCipherKey->getDomainId(), textCipherKey->getBaseCipherId(), textCipherKey->getSalt()),
	    BlobCipherDetails(headerCipherKeyOpt.get()->getDomainId(),
	                      headerCipherKeyOpt.get()->getBaseCipherId(),
	                      headerCipherKeyOpt.get()->getSalt()),
	    iv,
	    AES_256_IV_LENGTH);
	headerRef->algoHeader = AesCtrWithAuth(algoHeader);
	// compute the authentication token
	Standalone<StringRef> serialized = BlobCipherEncryptHeaderRef::toStringRef(*headerRef);
	uint8_t computed[Params::authTokenSize];
	computeAuthToken({ { ciphertext, ciphertextLen }, { serialized.begin(), serialized.size() } },
	                 headerCipherKeyOpt.get()->rawCipher(),
	                 AES_256_KEY_LENGTH,
	                 &computed[0],
	                 (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                 AUTH_TOKEN_MAX_SIZE);
	memcpy(&algoHeader.authToken[0], &computed[0], Params::authTokenSize);

	// Populate headerRef algorithm specific header details
	headerRef->algoHeader = algoHeader;
}

void EncryptBlobCipherAes265Ctr::setCipherAlgoHeaderNoAuthV1(const BlobCipherEncryptHeaderFlagsV1& flags,
                                                             BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE);

	AesCtrNoAuthV1 aesCtrNoAuth(
	    BlobCipherDetails(textCipherKey->getDomainId(), textCipherKey->getBaseCipherId(), textCipherKey->getSalt()),
	    iv,
	    AES_256_IV_LENGTH);
	headerRef->algoHeader = AesCtrNoAuth(aesCtrNoAuth);
}

void EncryptBlobCipherAes265Ctr::setCipherAlgoHeaderV1(const uint8_t* ciphertext,
                                                       const int ciphertextLen,
                                                       const BlobCipherEncryptHeaderFlagsV1& flags,
                                                       BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_EQ(1,
	          getEncryptAlgoHeaderVersion((EncryptAuthTokenMode)flags.authTokenMode,
	                                      (EncryptAuthTokenAlgo)flags.authTokenAlgo));

	if (flags.authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		setCipherAlgoHeaderNoAuthV1(flags, headerRef);
	} else if (flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
		setCipherAlgoHeaderWithAuthV1<AesCtrWithCmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	} else {
		ASSERT_EQ(flags.authTokenAlgo, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA);
		setCipherAlgoHeaderWithAuthV1<AesCtrWithHmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	}
}

void EncryptBlobCipherAes265Ctr::updateEncryptHeaderFlagsV1(BlobCipherEncryptHeaderRef* headerRef,
                                                            BlobCipherEncryptHeaderFlagsV1* flags) {

	// Populate encryption header flags details
	flags->encryptMode = ENCRYPT_CIPHER_MODE_AES_256_CTR;
	flags->authTokenMode = authTokenMode;
	flags->authTokenAlgo = authTokenAlgo;
	headerRef->flags = *flags;
}

void EncryptBlobCipherAes265Ctr::updateEncryptHeader(const uint8_t* ciphertext,
                                                     const int ciphertextLen,
                                                     BlobCipherEncryptHeaderRef* headerRef) {
	ASSERT_LE(CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION, std::numeric_limits<uint8_t>::max());
	ASSERT_EQ(1, CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);

	// update header flags
	BlobCipherEncryptHeaderFlagsV1 flags;
	updateEncryptHeaderFlagsV1(headerRef, &flags);

	// update cipher algo header
	int algoHeaderVersion = getEncryptAlgoHeaderVersion(authTokenMode, authTokenAlgo);
	ASSERT_EQ(algoHeaderVersion, 1);
	setCipherAlgoHeaderV1(ciphertext, ciphertextLen, flags, headerRef);
}

StringRef EncryptBlobCipherAes265Ctr::encrypt(const uint8_t* plaintext,
                                              const int plaintextLen,
                                              BlobCipherEncryptHeaderRef* headerRef,
                                              Arena& arena) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		startTime = timer_monotonic();
	}

	const int allocSize = plaintextLen + AES_BLOCK_SIZE;
	StringRef encryptBuf = makeString(allocSize, arena);
	uint8_t* ciphertext = mutateString(encryptBuf);

	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, ciphertext, &bytes, plaintext, plaintextLen) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	int finalBytes{ 0 };
	if (EVP_EncryptFinal_ex(ctx, ciphertext + bytes, &finalBytes) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptFinalFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}
	if ((bytes + finalBytes) != plaintextLen) {
		TraceEvent(SevWarn, "BlobCipherEncryptUnexpectedCipherLen")
		    .detail("PlaintextLen", plaintextLen)
		    .detail("EncryptedBufLen", bytes + finalBytes);
		throw encrypt_ops_error();
	}

	// Ensure encryption header authToken details sanity
	ASSERT(isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo));
	updateEncryptHeader(ciphertext, plaintextLen, headerRef);
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		BlobCipherMetrics::counters(usageType).encryptCPUTimeNS += int64_t((timer_monotonic() - startTime) * 1e9);
	}

	CODE_PROBE(authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "ConfigurableEncryption: Encryption with Auth token generation disabled");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "ConfigurableEncryption: Encryption with HMAC_SHA Auth token generation");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "ConfigurableEncryption: Encryption with AES_CMAC Auth token generation");

	// discard the extra buffer allocated to account for AES_BLOCK_SIZE encryption assist
	return encryptBuf.substr(0, plaintextLen);
}

Reference<EncryptBuf> EncryptBlobCipherAes265Ctr::encrypt(const uint8_t* plaintext,
                                                          const int plaintextLen,
                                                          BlobCipherEncryptHeader* header,
                                                          Arena& arena) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		startTime = timer_monotonic();
	}

	memset(reinterpret_cast<uint8_t*>(header), 0, sizeof(BlobCipherEncryptHeader));

	// Alloc buffer computation accounts for 'header authentication' generation scheme. If single-auth-token needs
	// to be generated, allocate buffer sufficient to append header to the cipherText optimizing memcpy cost.

	const int allocSize = plaintextLen + AES_BLOCK_SIZE;
	Reference<EncryptBuf> encryptBuf = makeReference<EncryptBuf>(allocSize, arena);
	uint8_t* ciphertext = encryptBuf->begin();

	int bytes{ 0 };
	if (EVP_EncryptUpdate(ctx, ciphertext, &bytes, plaintext, plaintextLen) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	int finalBytes{ 0 };
	if (EVP_EncryptFinal_ex(ctx, ciphertext + bytes, &finalBytes) != 1) {
		TraceEvent(SevWarn, "BlobCipherEncryptFinalFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	if ((bytes + finalBytes) != plaintextLen) {
		TraceEvent(SevWarn, "BlobCipherEncryptUnexpectedCipherLen")
		    .detail("PlaintextLen", plaintextLen)
		    .detail("EncryptedBufLen", bytes + finalBytes);
		throw encrypt_ops_error();
	}

	// Populate encryption header flags details
	header->flags.size = sizeof(BlobCipherEncryptHeader);
	header->flags.headerVersion = EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION;
	header->flags.encryptMode = ENCRYPT_CIPHER_MODE_AES_256_CTR;
	header->flags.authTokenMode = authTokenMode;
	header->flags.authTokenAlgo = authTokenAlgo;

	// Ensure encryption header authToken details sanity
	ASSERT(isEncryptHeaderAuthTokenDetailsValid(authTokenMode, authTokenAlgo));

	// Populate cipherText encryption-key details
	header->cipherTextDetails = textCipherKey->details();
	// Populate header encryption-key details
	if (authTokenMode != ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		header->cipherHeaderDetails = headerCipherKeyOpt.get()->details();
	} else {
		header->cipherHeaderDetails.encryptDomainId = INVALID_ENCRYPT_DOMAIN_ID;
		header->cipherHeaderDetails.baseCipherId = INVALID_ENCRYPT_CIPHER_KEY_ID;
		header->cipherHeaderDetails.salt = INVALID_ENCRYPT_RANDOM_SALT;
	}

	memcpy(&header->iv[0], &iv[0], AES_256_IV_LENGTH);

	if (authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		// No header 'authToken' generation needed.
	} else {

		// Populate header authToken details
		ASSERT_EQ(header->flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
		ASSERT_GE(allocSize, (bytes + finalBytes));
		ASSERT_GE(encryptBuf->getLogicalSize(), (bytes + finalBytes));

		computeAuthToken({ { ciphertext, bytes + finalBytes },
		                   { reinterpret_cast<const uint8_t*>(header), sizeof(BlobCipherEncryptHeader) } },
		                 headerCipherKeyOpt.get()->rawCipher(),
		                 AES_256_KEY_LENGTH,
		                 &header->singleAuthToken.authToken[0],
		                 (EncryptAuthTokenAlgo)header->flags.authTokenAlgo,
		                 AUTH_TOKEN_MAX_SIZE);
	}

	encryptBuf->setLogicalSize(plaintextLen);

	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		BlobCipherMetrics::counters(usageType).encryptCPUTimeNS += int64_t((timer_monotonic() - startTime) * 1e9);
	}

	CODE_PROBE(true, "BlobCipher data encryption");
	CODE_PROBE(header->flags.authTokenAlgo == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "Encryption authentication disabled");
	CODE_PROBE(header->flags.authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "HMAC_SHA Auth token generation");
	CODE_PROBE(header->flags.authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "AES_CMAC Auth token generation");

	return encryptBuf;
}

EncryptBlobCipherAes265Ctr::~EncryptBlobCipherAes265Ctr() {
	if (ctx != nullptr) {
		EVP_CIPHER_CTX_free(ctx);
	}
}

// DecryptBlobCipherAes256Ctr class methods

DecryptBlobCipherAes256Ctr::DecryptBlobCipherAes256Ctr(Reference<BlobCipherKey> tCipherKey,
                                                       Optional<Reference<BlobCipherKey>> hCipherKeyOpt,
                                                       const uint8_t* iv,
                                                       BlobCipherMetrics::UsageType usageType)
  : ctx(EVP_CIPHER_CTX_new()), usageType(usageType), textCipherKey(tCipherKey), headerCipherKeyOpt(hCipherKeyOpt),
    authTokensValidationDone(false) {
	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
		throw encrypt_ops_error();
	}
	if (!EVP_DecryptInit_ex(ctx, nullptr, nullptr, tCipherKey.getPtr()->data(), iv)) {
		throw encrypt_ops_error();
	}
}

template <class Params>
void DecryptBlobCipherAes256Ctr::validateAuthTokenV1(const uint8_t* ciphertext,
                                                     const int ciphertextLen,
                                                     const BlobCipherEncryptHeaderFlagsV1& flags,
                                                     const BlobCipherEncryptHeaderRef& headerRef) {
	ASSERT_EQ(flags.encryptMode, ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_LE(Params::authTokenSize, AUTH_TOKEN_MAX_SIZE);
	ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());

	Arena tmpArena;
	uint8_t persited[Params::authTokenSize];
	uint8_t computed[Params::authTokenSize];

	// prepare the payload {cipherText + encryptionHeader}
	// ensure the 'authToken' is reset before computing the 'authentication token'
	BlobCipherEncryptHeaderRef headerRefCopy = BlobCipherEncryptHeaderRef(headerRef);

	AesCtrWithAuth<Params> algoHeaderCopy = std::get<AesCtrWithAuth<Params>>(headerRefCopy.algoHeader);
	// preserve the 'persisted' token for future validation before reseting the field
	memcpy(&persited[0], &algoHeaderCopy.v1.authToken[0], Params::authTokenSize);
	memset(&algoHeaderCopy.v1.authToken[0], 0, Params::authTokenSize);

	headerRefCopy.algoHeader = algoHeaderCopy;
	Standalone<StringRef> serializedHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRefCopy);
	computeAuthToken({ { ciphertext, ciphertextLen }, { serializedHeader.begin(), serializedHeader.size() } },
	                 headerCipherKeyOpt.get()->rawCipher(),
	                 AES_256_KEY_LENGTH,
	                 &computed[0],
	                 (EncryptAuthTokenAlgo)flags.authTokenAlgo,
	                 AUTH_TOKEN_MAX_SIZE);

	if (memcmp(&persited[0], &computed[0], Params::authTokenSize) != 0) {
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeaderAuthTokenMismatch")
		    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
		    .detail("HeaderMode", flags.encryptMode)
		    .detail("SingleAuthToken", StringRef(tmpArena, persited, Params::authTokenSize))
		    .detail("ComputedSingleAuthToken", StringRef(tmpArena, computed, Params::authTokenSize));

		CODE_PROBE(flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
		           "ConfigurableEncryption: AuthToken value mismatch - HMAC_SHA auth token generation");
		CODE_PROBE(flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
		           "ConfigurableEncryption: AuthToken value mismatch - AES_CMAC auth token generation");

		throw encrypt_header_authtoken_mismatch();
	}
}

void DecryptBlobCipherAes256Ctr::validateHeaderSingleAuthTokenV1(const uint8_t* ciphertext,
                                                                 const int ciphertextLen,
                                                                 const BlobCipherEncryptHeaderFlagsV1& flags,
                                                                 const BlobCipherEncryptHeaderRef& headerRef) {
	// prepare the payload {cipherText + encryptionHeader}
	// ensure the 'authToken' is reset before computing the 'authentication token'

	if (flags.authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
		validateAuthTokenV1<AesCtrWithCmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	} else {
		ASSERT_EQ(flags.authTokenAlgo, EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA);
		validateAuthTokenV1<AesCtrWithHmacParams>(ciphertext, ciphertextLen, flags, headerRef);
	}
}

void DecryptBlobCipherAes256Ctr::validateAuthTokensV1(const uint8_t* ciphertext,
                                                      const int ciphertextLen,
                                                      const BlobCipherEncryptHeaderFlagsV1& flags,
                                                      const BlobCipherEncryptHeaderRef& headerRef) {
	if (flags.authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		// No embedded 'authToken'; do nothing
		return;
	}

	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	validateHeaderSingleAuthTokenV1(ciphertext, ciphertextLen, flags, headerRef);
	authTokensValidationDone = true;
}

void DecryptBlobCipherAes256Ctr::validateEncryptHeaderFlagsV1(const uint32_t headerVersion,
                                                              const BlobCipherEncryptHeaderFlagsV1& flags) {
	// validate header flag sanity
	if (flags.encryptMode != EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR ||
	    !isEncryptHeaderAuthTokenModeValid((EncryptAuthTokenMode)flags.authTokenMode)) {
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeader")
		    .detail("HeaderVersion", headerVersion)
		    .detail("ExpectedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION)
		    .detail("EncryptCipherMode", flags.encryptMode)
		    .detail("ExpectedCipherMode", EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR)
		    .detail("EncryptHeaderAuthTokenMode", flags.authTokenMode);

		CODE_PROBE(true, "ConfigurableEncryption: Encryption header metadata mismatch");

		throw encrypt_header_metadata_mismatch();
	}
}

void DecryptBlobCipherAes256Ctr::validateEncryptHeader(const uint8_t* ciphertext,
                                                       const int ciphertextLen,
                                                       const BlobCipherEncryptHeaderRef& headerRef,
                                                       EncryptAuthTokenMode* authTokenMode,
                                                       EncryptAuthTokenAlgo* authTokenAlgo) {
	if (headerRef.flagsVersion() != 1) {
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeader")
		    .detail("HeaderVersion", headerRef.flagsVersion())
		    .detail("MaxSupportedVersion", CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);

		CODE_PROBE(true, "ConfigurableEncryption: Encryption header version unsupported");

		throw encrypt_header_metadata_mismatch();
	}

	if (headerRef.flagsVersion() != 1) {
		throw not_implemented();
	}

	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	validateEncryptHeaderFlagsV1(headerRef.flagsVersion(), flags);
	validateAuthTokensV1(ciphertext, ciphertextLen, flags, headerRef);

	*authTokenMode = (EncryptAuthTokenMode)flags.authTokenMode;
	*authTokenAlgo = (EncryptAuthTokenAlgo)flags.authTokenAlgo;
}

StringRef DecryptBlobCipherAes256Ctr::decrypt(const uint8_t* ciphertext,
                                              const int ciphertextLen,
                                              const BlobCipherEncryptHeaderRef& headerRef,
                                              Arena& arena) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		startTime = timer_monotonic();
	}

	EncryptAuthTokenMode authTokenMode;
	EncryptAuthTokenAlgo authTokenAlgo;
	validateEncryptHeader(ciphertext, ciphertextLen, headerRef, &authTokenMode, &authTokenAlgo);

	const int allocSize = ciphertextLen + AES_BLOCK_SIZE;
	StringRef decrypted = makeString(allocSize, arena);

	uint8_t* plaintext = mutateString(decrypted);
	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, plaintext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		TraceEvent(SevWarn, "BlobCipherDecryptUpdateFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	int finalBlobBytes{ 0 };
	if (EVP_DecryptFinal_ex(ctx, plaintext + bytesDecrypted, &finalBlobBytes) <= 0) {
		TraceEvent(SevWarn, "BlobCipherDecryptFinalFailed")
		    .detail("BaseCipherId", textCipherKey->getBaseCipherId())
		    .detail("EncryptDomainId", textCipherKey->getDomainId());
		throw encrypt_ops_error();
	}

	if ((bytesDecrypted + finalBlobBytes) != ciphertextLen) {
		TraceEvent(SevWarn, "BlobCipherEncryptUnexpectedPlaintextLen")
		    .detail("CiphertextLen", ciphertextLen)
		    .detail("DecryptedBufLen", bytesDecrypted + finalBlobBytes);
		throw encrypt_ops_error();
	}

	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		BlobCipherMetrics::counters(usageType).decryptCPUTimeNS += int64_t((timer_monotonic() - startTime) * 1e9);
	}

	CODE_PROBE(authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "ConfigurableEncryption: Decryption with Auth token generation disabled");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "ConfigurableEncryption: Decryption with HMAC_SHA Auth token generation");
	CODE_PROBE(authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "ConfigurableEncryption: Decryption with AES_CMAC Auth token generation");

	// discard the extra buffer allocated to account AES_BLOCK_SIZE decryption assist
	return decrypted.substr(0, ciphertextLen);
}

void DecryptBlobCipherAes256Ctr::verifyHeaderSingleAuthToken(const uint8_t* ciphertext,
                                                             const int ciphertextLen,
                                                             const BlobCipherEncryptHeader& header) {
	ASSERT(headerCipherKeyOpt.present() && headerCipherKeyOpt.get().isValid());
	// prepare the payload {cipherText + encryptionHeader}
	// ensure the 'authToken' is reset before computing the 'authentication token'
	BlobCipherEncryptHeader headerCopy;
	memcpy(reinterpret_cast<uint8_t*>(&headerCopy),
	       reinterpret_cast<const uint8_t*>(&header),
	       sizeof(BlobCipherEncryptHeader));
	memset(reinterpret_cast<uint8_t*>(&headerCopy.singleAuthToken), 0, AUTH_TOKEN_MAX_SIZE);
	uint8_t computed[AUTH_TOKEN_MAX_SIZE];
	computeAuthToken({ { ciphertext, ciphertextLen },
	                   { reinterpret_cast<const uint8_t*>(&headerCopy), sizeof(BlobCipherEncryptHeader) } },
	                 headerCipherKeyOpt.get()->rawCipher(),
	                 AES_256_KEY_LENGTH,
	                 &computed[0],
	                 (EncryptAuthTokenAlgo)header.flags.authTokenAlgo,
	                 AUTH_TOKEN_MAX_SIZE);

	int authTokenSize = getEncryptHeaderAuthTokenSize(header.flags.authTokenAlgo);
	ASSERT_LE(authTokenSize, AUTH_TOKEN_MAX_SIZE);
	if (memcmp(&header.singleAuthToken.authToken[0], &computed[0], authTokenSize) != 0) {
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeaderAuthTokenMismatch")
		    .detail("HeaderVersion", header.flags.headerVersion)
		    .detail("HeaderMode", header.flags.encryptMode)
		    .detail("SingleAuthToken", StringRef(&header.singleAuthToken.authToken[0], AUTH_TOKEN_MAX_SIZE).toString())
		    .detail("ComputedSingleAuthToken", StringRef(computed, AUTH_TOKEN_MAX_SIZE));

		CODE_PROBE(header.flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
		           "AuthToken size mismatch - HMAC_SHA auth token generation");
		CODE_PROBE(header.flags.authTokenAlgo == ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
		           "AuthToken size mismatch - AES_CMAC auth token generation");

		throw encrypt_header_authtoken_mismatch();
	}
}

void DecryptBlobCipherAes256Ctr::verifyAuthTokens(const uint8_t* ciphertext,
                                                  const int ciphertextLen,
                                                  const BlobCipherEncryptHeader& header) {
	ASSERT_EQ(header.flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	verifyHeaderSingleAuthToken(ciphertext, ciphertextLen, header);

	authTokensValidationDone = true;
}

void DecryptBlobCipherAes256Ctr::verifyEncryptHeaderMetadata(const BlobCipherEncryptHeader& header) {
	// validate header flag sanity
	if (header.flags.headerVersion != EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION ||
	    header.flags.encryptMode != EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR ||
	    !isEncryptHeaderAuthTokenModeValid((EncryptAuthTokenMode)header.flags.authTokenMode)) {
		TraceEvent(SevWarn, "BlobCipherVerifyEncryptBlobHeader")
		    .detail("HeaderVersion", header.flags.headerVersion)
		    .detail("ExpectedVersion", EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION)
		    .detail("EncryptCipherMode", header.flags.encryptMode)
		    .detail("ExpectedCipherMode", EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR)
		    .detail("EncryptHeaderAuthTokenMode", header.flags.authTokenMode);

		CODE_PROBE(true, "Encryption header metadata mismatch");

		throw encrypt_header_metadata_mismatch();
	}
}

Reference<EncryptBuf> DecryptBlobCipherAes256Ctr::decrypt(const uint8_t* ciphertext,
                                                          const int ciphertextLen,
                                                          const BlobCipherEncryptHeader& header,
                                                          Arena& arena) {
	double startTime = 0.0;
	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		startTime = timer_monotonic();
	}

	verifyEncryptHeaderMetadata(header);

	if (header.flags.authTokenMode != EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE &&
	    (!headerCipherKeyOpt.present() || !headerCipherKeyOpt.get().isValid())) {
		TraceEvent(SevWarn, "BlobCipherDecryptInvalidHeaderCipherKey")
		    .detail("AuthTokenMode", header.flags.authTokenMode);
		throw encrypt_ops_error();
	}

	const int allocSize = ciphertextLen + AES_BLOCK_SIZE;
	Reference<EncryptBuf> decrypted = makeReference<EncryptBuf>(allocSize, arena);

	if (header.flags.authTokenMode != EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE) {
		verifyAuthTokens(ciphertext, ciphertextLen, header);
		ASSERT(authTokensValidationDone);
	}

	uint8_t* plaintext = decrypted->begin();
	int bytesDecrypted{ 0 };
	if (!EVP_DecryptUpdate(ctx, plaintext, &bytesDecrypted, ciphertext, ciphertextLen)) {
		TraceEvent(SevWarn, "BlobCipherDecryptUpdateFailed")
		    .detail("BaseCipherId", header.cipherTextDetails.baseCipherId)
		    .detail("EncryptDomainId", header.cipherTextDetails.encryptDomainId);
		throw encrypt_ops_error();
	}

	int finalBlobBytes{ 0 };
	if (EVP_DecryptFinal_ex(ctx, plaintext + bytesDecrypted, &finalBlobBytes) <= 0) {
		TraceEvent(SevWarn, "BlobCipherDecryptFinalFailed")
		    .detail("BaseCipherId", header.cipherTextDetails.baseCipherId)
		    .detail("EncryptDomainId", header.cipherTextDetails.encryptDomainId);
		throw encrypt_ops_error();
	}

	if ((bytesDecrypted + finalBlobBytes) != ciphertextLen) {
		TraceEvent(SevWarn, "BlobCipherEncryptUnexpectedPlaintextLen")
		    .detail("CiphertextLen", ciphertextLen)
		    .detail("DecryptedBufLen", bytesDecrypted + finalBlobBytes);
		throw encrypt_ops_error();
	}

	decrypted->setLogicalSize(ciphertextLen);

	if (CLIENT_KNOBS->ENABLE_ENCRYPTION_CPU_TIME_LOGGING) {
		BlobCipherMetrics::counters(usageType).decryptCPUTimeNS += int64_t((timer_monotonic() - startTime) * 1e9);
	}

	CODE_PROBE(true, "BlobCipher data decryption");
	CODE_PROBE(header.flags.authTokenAlgo == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	           "Decryption authentication disabled");
	CODE_PROBE(header.flags.authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
	           "Decryption HMAC_SHA Auth token verification");
	CODE_PROBE(header.flags.authTokenAlgo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	           "Decryption AES_CMAC Auth token verification");

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

unsigned int HmacSha256DigestGen::digest(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                                         unsigned char* buf,
                                         unsigned int bufLen) {
	ASSERT_EQ(bufLen, HMAC_size(ctx));

	for (const auto& p : payload) {
		if (HMAC_Update(ctx, p.first, p.second) != 1) {
			throw encrypt_ops_error();
		}
	}

	unsigned int digestLen = 0;
	if (HMAC_Final(ctx, buf, &digestLen) != 1) {
		throw encrypt_ops_error();
	}

	CODE_PROBE(true, "HMAC_SHA Digest generation");

	return digestLen;
}

// Aes256CtrCmacDigestGen methods
Aes256CmacDigestGen::Aes256CmacDigestGen(const unsigned char* key, size_t keylen) : ctx(CMAC_CTX_new()) {
	ASSERT_EQ(keylen, AES_256_KEY_LENGTH);

	if (ctx == nullptr) {
		throw encrypt_ops_error();
	}
	if (!CMAC_Init(ctx, key, keylen, EVP_aes_256_cbc(), NULL)) {
		throw encrypt_ops_error();
	}
}

size_t Aes256CmacDigestGen::digest(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                                   uint8_t* digest,
                                   int digestlen) {
	ASSERT(ctx != nullptr);
	ASSERT_GE(digestlen, AUTH_TOKEN_AES_CMAC_SIZE);

	for (const auto& p : payload) {
		if (!CMAC_Update(ctx, p.first, p.second)) {
			throw encrypt_ops_error();
		}
	}
	size_t ret;
	if (!CMAC_Final(ctx, digest, &ret)) {
		throw encrypt_ops_error();
	}

	return ret;
}

Aes256CmacDigestGen::~Aes256CmacDigestGen() {
	if (ctx != nullptr) {
		CMAC_CTX_free(ctx);
	}
}

void computeAuthToken(const std::vector<std::pair<const uint8_t*, size_t>>& payload,
                      const uint8_t* key,
                      const int keyLen,
                      unsigned char* digestBuf,
                      const EncryptAuthTokenAlgo algo,
                      unsigned int digestBufMaxSz) {
	ASSERT_EQ(digestBufMaxSz, AUTH_TOKEN_MAX_SIZE);
	ASSERT_EQ(keyLen, AES_256_KEY_LENGTH);
	ASSERT(isEncryptHeaderAuthTokenAlgoValid(algo));

	int authTokenSz = getEncryptHeaderAuthTokenSize(algo);
	ASSERT_LE(authTokenSz, AUTH_TOKEN_MAX_SIZE);

	if (algo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA) {
		ASSERT_EQ(authTokenSz, AUTH_TOKEN_HMAC_SHA_SIZE);

		HmacSha256DigestGen hmacGenerator(key, keyLen);
		unsigned int digestLen = hmacGenerator.digest(payload, digestBuf, authTokenSz);

		ASSERT_EQ(digestLen, authTokenSz);
	} else if (algo == EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC) {
		ASSERT_EQ(authTokenSz, AUTH_TOKEN_AES_CMAC_SIZE);

		Aes256CmacDigestGen cmacGenerator(key, keyLen);
		size_t digestLen = cmacGenerator.digest(payload, digestBuf, authTokenSz);

		ASSERT_EQ(digestLen, authTokenSz);
	} else {
		throw not_implemented();
	}
}

EncryptAuthTokenMode getEncryptAuthTokenMode(const EncryptAuthTokenMode mode) {
	// Override mode if authToken isn't enabled
	return FLOW_KNOBS->ENCRYPT_HEADER_AUTH_TOKEN_ENABLED ? mode
	                                                     : EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE;
}

// Only used to link unit tests
void forceLinkBlobCipherTests() {}

// Tests cases includes:
// 1. Populate cache by inserting 'baseCipher' details for new encryptionDomainIds
// 2. Random lookup for cipherKeys and content validation
// 3. Inserting of 'identical' cipherKey (already cached) more than once works as desired.
// 4. Inserting of 'non-identical' cipherKey (already cached) more than once works as desired.
// 5. Validation encryption ops (correctness):
//  5.1. Encrypt a buffer followed by decryption of the buffer, validate the contents.
//  5.2. Simulate anomalies such as: EncryptionHeader corruption, authToken mismatch / encryptionMode mismatch etc.
// 6. Cache cleanup
//  6.1  cleanup cipherKeys by given encryptDomainId
//  6.2. Cleanup all cached cipherKeys

namespace {
// Construct a dummy External Key Manager representation and populate with some keys
class BaseCipher : public ReferenceCounted<BaseCipher>, NonCopyable {
public:
	EncryptCipherDomainId domainId;
	int len;
	EncryptCipherBaseKeyId keyId;
	std::unique_ptr<uint8_t[]> key;
	int64_t refreshAt;
	int64_t expireAt;
	EncryptCipherRandomSalt generatedSalt;

	BaseCipher(const EncryptCipherDomainId& dId,
	           const EncryptCipherBaseKeyId& kId,
	           const int64_t rAt,
	           const int64_t eAt)
	  : domainId(dId), len(deterministicRandom()->randomInt(AES_256_KEY_LENGTH / 2, AES_256_KEY_LENGTH + 1)),
	    keyId(kId), key(std::make_unique<uint8_t[]>(len)), refreshAt(rAt), expireAt(eAt) {
		deterministicRandom()->randomBytes(key.get(), len);
	}
};

using BaseKeyMap = std::unordered_map<EncryptCipherBaseKeyId, Reference<BaseCipher>>;
using DomainKeyMap = std::unordered_map<EncryptCipherDomainId, BaseKeyMap>;

} // namespace

void testKeyCacheEssentials(DomainKeyMap& domainKeyMap,
                            const int minDomainId,
                            const int maxDomainId,
                            const int minBaseCipherKeyId) {
	TraceEvent("BlobCipherCacheEssentialsStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// validate getLatestCipherKey return empty when there's no cipher key
	TraceEvent("BlobCipherTestLatestKeyNotExists").log();
	Reference<BlobCipherKey> latestKeyNonexists =
	    cipherKeyCache->getLatestCipherKey(deterministicRandom()->randomInt(minDomainId, maxDomainId));
	ASSERT(!latestKeyNonexists.isValid());
	try {
		cipherKeyCache->getLatestCipherKey(INVALID_ENCRYPT_DOMAIN_ID);
		ASSERT(false); // shouldn't get here
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_encrypt_invalid_id);
	}

	// insert BlobCipher keys into BlobCipherKeyCache map and validate
	TraceEvent("BlobCipherTestInsertKeys").log();
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;

			cipherKeyCache->insertCipherKey(baseCipher->domainId,
			                                baseCipher->keyId,
			                                baseCipher->key.get(),
			                                baseCipher->len,
			                                baseCipher->refreshAt,
			                                baseCipher->expireAt);
			Reference<BlobCipherKey> fetchedKey = cipherKeyCache->getLatestCipherKey(baseCipher->domainId);
			baseCipher->generatedSalt = fetchedKey->getSalt();
		}
	}
	// insert EncryptHeader BlobCipher key
	Reference<BaseCipher> headerBaseCipher = makeReference<BaseCipher>(
	    ENCRYPT_HEADER_DOMAIN_ID, 1, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max());
	cipherKeyCache->insertCipherKey(headerBaseCipher->domainId,
	                                headerBaseCipher->keyId,
	                                headerBaseCipher->key.get(),
	                                headerBaseCipher->len,
	                                headerBaseCipher->refreshAt,
	                                headerBaseCipher->expireAt);

	TraceEvent("BlobCipherTestInsertKeysDone").log();

	// validate the cipherKey lookups work as desired
	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;
			Reference<BlobCipherKey> cipherKey =
			    cipherKeyCache->getCipherKey(baseCipher->domainId, baseCipher->keyId, baseCipher->generatedSalt);
			ASSERT(cipherKey.isValid());
			// validate common cipher properties - domainId, baseCipherId, baseCipherLen, rawBaseCipher
			ASSERT_EQ(cipherKey->getBaseCipherId(), baseCipher->keyId);
			ASSERT_EQ(cipherKey->getDomainId(), baseCipher->domainId);
			ASSERT_EQ(cipherKey->getBaseCipherLen(), baseCipher->len);
			// ensure that baseCipher matches with the cached information
			ASSERT_EQ(std::memcmp(cipherKey->rawBaseCipher(), baseCipher->key.get(), cipherKey->getBaseCipherLen()), 0);
			// validate the encryption derivation
			ASSERT_NE(std::memcmp(cipherKey->rawCipher(), baseCipher->key.get(), cipherKey->getBaseCipherLen()), 0);
		}
	}
	TraceEvent("BlobCipherTestLooksupDone").log();

	// Ensure attemtping to insert existing cipherKey (identical) more than once is treated as a NOP
	try {
		Reference<BaseCipher> baseCipher = domainKeyMap[minDomainId][minBaseCipherKeyId];
		cipherKeyCache->insertCipherKey(baseCipher->domainId,
		                                baseCipher->keyId,
		                                baseCipher->key.get(),
		                                baseCipher->len,
		                                std::numeric_limits<int64_t>::max(),
		                                std::numeric_limits<int64_t>::max());
	} catch (Error& e) {
		throw;
	}
	TraceEvent("BlobCipherTestReinsertIdempotentKeyDone").log();

	// Ensure attemtping to insert an existing cipherKey (modified) fails with appropriate error
	try {
		Reference<BaseCipher> baseCipher = domainKeyMap[minDomainId][minBaseCipherKeyId];
		uint8_t rawCipher[baseCipher->len];
		memcpy(rawCipher, baseCipher->key.get(), baseCipher->len);
		// modify few bytes in the cipherKey
		for (int i = 2; i < 5; i++) {
			rawCipher[i]++;
		}
		cipherKeyCache->insertCipherKey(baseCipher->domainId,
		                                baseCipher->keyId,
		                                &rawCipher[0],
		                                baseCipher->len,
		                                std::numeric_limits<int64_t>::max(),
		                                std::numeric_limits<int64_t>::max());
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_update_cipher) {
			throw;
		}
	}
	TraceEvent("BlobCipherTestReinsertNonIdempotentKeyDone");

	TraceEvent("BlobCipherCacheEssentialsEnd");
}

void testNoAuthMode(const int minDomainId) {
	TraceEvent("BlobCipherTestInsertKeysDone").log();

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                     BlobCipherMetrics::TEST);
	BlobCipherEncryptHeader header;
	Reference<EncryptBuf> encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);

	ASSERT_EQ(encrypted->getLogicalSize(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encrypted->begin(), bufLen), 0);
	ASSERT_EQ(header.flags.headerVersion, EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION);
	ASSERT_EQ(header.flags.encryptMode, EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR);
	ASSERT_EQ(header.flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE);

	TraceEvent("BlobCipherTestEncryptDone")
	    .detail("HeaderVersion", header.flags.headerVersion)
	    .detail("HeaderEncryptMode", header.flags.encryptMode)
	    .detail("HeaderEncryptAuthTokenMode", header.flags.authTokenMode)
	    .detail("HeaderEncryptAuthTokenAlgo", header.flags.authTokenAlgo)
	    .detail("DomainId", header.cipherTextDetails.encryptDomainId)
	    .detail("BaseCipherId", header.cipherTextDetails.baseCipherId);

	Reference<BlobCipherKey> tCipherKeyKey = cipherKeyCache->getCipherKey(
	    header.cipherTextDetails.encryptDomainId, header.cipherTextDetails.baseCipherId, header.cipherTextDetails.salt);
	ASSERT(tCipherKeyKey->isEqual(cipherKey));
	DecryptBlobCipherAes256Ctr decryptor(
	    tCipherKeyKey, Reference<BlobCipherKey>(), &header.iv[0], BlobCipherMetrics::TEST);

	Reference<EncryptBuf> decrypted = decryptor.decrypt(encrypted->begin(), bufLen, header, arena);
	ASSERT_EQ(decrypted->getLogicalSize(), bufLen);
	ASSERT_EQ(memcmp(decrypted->begin(), &orgData[0], bufLen), 0);

	TraceEvent("BlobCipherTestDecryptDone");

	// induce encryption header corruption - headerVersion corrupted
	BlobCipherEncryptHeader headerCopy;
	memcpy(reinterpret_cast<uint8_t*>(&headerCopy),
	       reinterpret_cast<const uint8_t*>(&header),
	       sizeof(BlobCipherEncryptHeader));
	headerCopy.flags.headerVersion += 1;
	try {
		encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
		DecryptBlobCipherAes256Ctr decryptor(
		    tCipherKeyKey, Reference<BlobCipherKey>(), header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, headerCopy, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
	}

	// induce encryption header corruption - encryptionMode corrupted
	memcpy(reinterpret_cast<uint8_t*>(&headerCopy),
	       reinterpret_cast<const uint8_t*>(&header),
	       sizeof(BlobCipherEncryptHeader));
	headerCopy.flags.encryptMode += 1;
	try {
		encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
		DecryptBlobCipherAes256Ctr decryptor(
		    tCipherKeyKey, Reference<BlobCipherKey>(), header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, headerCopy, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
	}

	// induce encrypted buffer payload corruption
	try {
		encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
		uint8_t temp[bufLen];
		memcpy(encrypted->begin(), &temp[0], bufLen);
		int tIdx = deterministicRandom()->randomInt(0, bufLen - 1);
		temp[tIdx] += 1;
		DecryptBlobCipherAes256Ctr decryptor(
		    tCipherKeyKey, Reference<BlobCipherKey>(), header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(&temp[0], bufLen, header, arena);
	} catch (Error& e) {
		// No authToken, hence, no corruption detection supported
		ASSERT(false);
	}

	TraceEvent("BlobCipherTestNoAuthModeDone");
}

void testConfigurableEncryptionBlobCipherHeaderFlagsV1Ser() {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	Arena arena;

	// Version-1
	BlobCipherEncryptHeaderFlagsV1 flags(
	    ENCRYPT_CIPHER_MODE_AES_256_CTR, getRandomAuthTokenMode(), getRandomAuthTokenAlgo());
	Standalone<StringRef> ser = BlobCipherEncryptHeaderFlagsV1::toStringRef(flags, arena);
	ASSERT_EQ(ser.size(), sizeof(flags));
}

void testConfigurableEncryptionAesCtrNoAuthV1Ser(const int minDomainId) {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	Arena arena;
	BlobCipherEncryptHeaderRef headerRef;
	uint32_t size = 0;

	BlobCipherEncryptHeaderFlagsV1 flags = BlobCipherEncryptHeaderFlagsV1(
	    ENCRYPT_CIPHER_MODE_AES_256_CTR, ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE, ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE);
	size += sizeof(BlobCipherEncryptHeaderFlagsV1) + 2;
	headerRef.flags = flags;

	AesCtrNoAuth noAuth;
	noAuth.v1.cipherTextDetails = BlobCipherDetails(1, 2, 23);
	deterministicRandom()->randomBytes(&noAuth.v1.iv[0], AES_256_IV_LENGTH);
	Standalone<StringRef> serAlgo = AesCtrNoAuth::toStringRef(noAuth);
	ASSERT_EQ(serAlgo.size(), sizeof(noAuth));

	size += AesCtrNoAuth::getSize();

	headerRef.algoHeader = noAuth;
	Standalone<StringRef> serHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	ASSERT_EQ(serHeader.size(), size);
	ASSERT_EQ(size,
	          BlobCipherEncryptHeaderRef::getHeaderSize(headerRef.flagsVersion(),
	                                                    headerRef.algoHeaderVersion(),
	                                                    (EncryptCipherMode)flags.encryptMode,
	                                                    (EncryptAuthTokenMode)flags.authTokenMode,
	                                                    (EncryptAuthTokenAlgo)flags.authTokenAlgo));
}

template <class Params>
void testConfigurableEncryptionAesCtrWithAuthSer(const int minDomainId) {
	constexpr bool isHmac = std::is_same_v<Params, AesCtrWithHmacParams>;
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	Arena arena;
	BlobCipherEncryptHeaderRef headerRef;
	uint32_t size = 0;

	BlobCipherEncryptHeaderFlagsV1 flags = BlobCipherEncryptHeaderFlagsV1(
	    ENCRYPT_CIPHER_MODE_AES_256_CTR,
	    ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	    isHmac ? ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA : ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC);
	size += sizeof(BlobCipherEncryptHeaderFlagsV1) + 2;

	headerRef.flags = flags;

	AesCtrWithAuth<Params> withAuth;
	withAuth.v1.cipherTextDetails = BlobCipherDetails(1, 2, 23);
	withAuth.v1.cipherHeaderDetails = BlobCipherDetails(ENCRYPT_HEADER_DOMAIN_ID, 2, 23);
	deterministicRandom()->randomBytes(&withAuth.v1.iv[0], AES_256_IV_LENGTH);
	deterministicRandom()->randomBytes(&withAuth.v1.authToken[0], Params::authTokenSize);
	Standalone<StringRef> serAlgo = AesCtrWithAuth<Params>::toStringRef(withAuth);
	ASSERT_EQ(serAlgo.size(), sizeof(withAuth));

	size += AesCtrWithAuth<Params>::getSize();

	headerRef.algoHeader = withAuth;
	Standalone<StringRef> serHeader = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	ASSERT_EQ(serHeader.size(), size);
	ASSERT_EQ(size,
	          BlobCipherEncryptHeaderRef::getHeaderSize(headerRef.flagsVersion(),
	                                                    headerRef.algoHeaderVersion(),
	                                                    (EncryptCipherMode)flags.encryptMode,
	                                                    (EncryptAuthTokenMode)flags.authTokenMode,
	                                                    (EncryptAuthTokenAlgo)flags.authTokenAlgo));
}

void testConfigurableEncryptionHeaderNoAuthMode(const int minDomainId) {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	TraceEvent("TestConfigurableEncryptionHeader").detail("Mode", "No-Auth");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                     BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(headerRef.flagsVersion(), 1);
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.authTokenMode, headerRef.getAuthTokenMode());
	AesCtrNoAuth noAuth = std::get<AesCtrNoAuth>(headerRef.algoHeader);

	const uint8_t* headerIV = headerRef.getIV();
	ASSERT_EQ(memcmp(&headerIV[0], &iv[0], AES_256_IV_LENGTH), 0);

	EncryptHeaderCipherDetails validateDetails = headerRef.getCipherDetails();
	ASSERT(validateDetails.textCipherDetails.isValid() &&
	       validateDetails.textCipherDetails ==
	           BlobCipherDetails(cipherKey->getDomainId(), cipherKey->getBaseCipherId(), cipherKey->getSalt()));
	ASSERT(!validateDetails.headerCipherDetails.present());

	Standalone<StringRef> serHeaderRef = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	BlobCipherEncryptHeaderRef validateHeader = BlobCipherEncryptHeaderRef::fromStringRef(serHeaderRef);
	BlobCipherEncryptHeaderFlagsV1 validateFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(validateHeader.flags);
	ASSERT(validateFlags == flags);

	AesCtrNoAuth validateAlgo = std::get<AesCtrNoAuth>(validateHeader.algoHeader);
	ASSERT(validateAlgo.v1.cipherTextDetails == noAuth.v1.cipherTextDetails);
	ASSERT_EQ(memcmp(&validateAlgo.v1.iv[0], &noAuth.v1.iv[0], AES_256_IV_LENGTH), 0);

	TraceEvent("NoAuthHeaderSize")
	    .detail("Flags", sizeof(flags))
	    .detail("AlgoHeader", noAuth.getSize())
	    .detail("TotalHeader", serHeaderRef.size());

	TraceEvent("TestConfigurableEncryptionHeader").detail("Mode", "No-Auth");
}

void testConfigurableEncryptionNoAuthMode(const int minDomainId) {
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	TraceEvent("BlobCipherTestConfigurableEncryptionNoAuthModeStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                     BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);

	// validate header version details
	AesCtrNoAuth noAuth = std::get<AesCtrNoAuth>(headerRef.algoHeader);
	Reference<BlobCipherKey> tCipherKeyKey = cipherKeyCache->getCipherKey(noAuth.v1.cipherTextDetails.encryptDomainId,
	                                                                      noAuth.v1.cipherTextDetails.baseCipherId,
	                                                                      noAuth.v1.cipherTextDetails.salt);
	ASSERT(tCipherKeyKey->isEqual(cipherKey));
	DecryptBlobCipherAes256Ctr decryptor(
	    tCipherKeyKey, Reference<BlobCipherKey>(), &noAuth.v1.iv[0], BlobCipherMetrics::TEST);

	StringRef decryptedBuf = decryptor.decrypt(encryptedBuf.begin(), encryptedBuf.size(), headerRef, arena);
	ASSERT_EQ(decryptedBuf.size(), bufLen);
	ASSERT_EQ(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);

	TraceEvent("BlobCipherTestEncryptDecryptDone")
	    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
	    .detail("AlgoHeaderVersion", headerRef.algoHeaderVersion())
	    .detail("HeaderEncryptMode", ENCRYPT_CIPHER_MODE_AES_256_CTR)
	    .detail("HeaderEncryptAuthTokenMode", ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE)
	    .detail("HeaderEncryptAuthTokenAlgo", ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE)
	    .detail("DomainId", noAuth.v1.cipherTextDetails.encryptDomainId)
	    .detail("BaseCipherId", noAuth.v1.cipherTextDetails.baseCipherId)
	    .detail("Salt", noAuth.v1.cipherTextDetails.salt);

	// induce encryption header corruption - encryptionMode corrupted
	BlobCipherEncryptHeaderRef corruptedHeaderRef = BlobCipherEncryptHeaderRef(headerRef);
	BlobCipherEncryptHeaderFlagsV1 corruptedFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	corruptedFlags.encryptMode += 1;
	corruptedHeaderRef.flags = corruptedFlags;
	try {
		encryptedBuf = encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);
		DecryptBlobCipherAes256Ctr decryptor(
		    tCipherKeyKey, Reference<BlobCipherKey>(), &iv[0], BlobCipherMetrics::TEST);
		decryptedBuf = decryptor.decrypt(encryptedBuf.begin(), bufLen, corruptedHeaderRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		TraceEvent("ConfigurableEncryptionNoAuthCorruptEncryptModeDone");
	}

	// induce encrypted buffer payload corruption
	try {
		encryptedBuf = encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);
		uint8_t temp[bufLen];
		memcpy((void*)encryptedBuf.begin(), &temp[0], bufLen);
		int tIdx = deterministicRandom()->randomInt(0, bufLen - 1);
		temp[tIdx] += 1;
		DecryptBlobCipherAes256Ctr decryptor(
		    tCipherKeyKey, Reference<BlobCipherKey>(), &iv[0], BlobCipherMetrics::TEST);
		decryptedBuf = decryptor.decrypt(&temp[0], bufLen, headerRef, arena);
		ASSERT_NE(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);
	} catch (Error& e) {
		// No authToken, hence, no corruption detection supported
		ASSERT(false);
	}

	TraceEvent("ConfigurableEncryptionNoAuthDone");
}

// validate basic encrypt followed by decrypt operation for AUTH_TOKEN_MODE_SINGLE
// HMAC_SHA authToken algorithm
template <class Params>
void testSingleAuthMode(const int minDomainId) {
	constexpr bool isHmac = std::is_same_v<Params, AesCtrWithHmacParams>;
	const std::string authAlgoStr = isHmac ? "HMAC-SHA" : "AES-CMAC";
	const EncryptAuthTokenAlgo authAlgo = isHmac ? EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA
	                                             : EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC;

	TraceEvent("BlobCipherTestSingleAuthTokenStart").detail("Mode", authAlgoStr);

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                     authAlgo,
	                                     BlobCipherMetrics::TEST);
	BlobCipherEncryptHeader header;
	Reference<EncryptBuf> encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);

	ASSERT_EQ(encrypted->getLogicalSize(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encrypted->begin(), bufLen), 0);
	ASSERT_EQ(header.flags.headerVersion, EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION);
	ASSERT_EQ(header.flags.encryptMode, ENCRYPT_CIPHER_MODE_AES_256_CTR);
	ASSERT_EQ(header.flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(header.flags.authTokenAlgo, authAlgo);

	TraceEvent("BlobCipherTestEncryptDone")
	    .detail("HeaderVersion", header.flags.headerVersion)
	    .detail("HeaderEncryptMode", header.flags.encryptMode)
	    .detail("HeaderEncryptAuthTokenMode", header.flags.authTokenMode)
	    .detail("HeaderEncryptAuthTokenAlgo", header.flags.authTokenAlgo)
	    .detail("DomainId", header.cipherTextDetails.encryptDomainId)
	    .detail("BaseCipherId", header.cipherTextDetails.baseCipherId)
	    .detail("HeaderAuthToken",
	            StringRef(arena, &header.singleAuthToken.authToken[0], Params::authTokenSize).toString());

	Reference<BlobCipherKey> tCipherKeyKey = cipherKeyCache->getCipherKey(
	    header.cipherTextDetails.encryptDomainId, header.cipherTextDetails.baseCipherId, header.cipherTextDetails.salt);
	Reference<BlobCipherKey> hCipherKey = cipherKeyCache->getCipherKey(header.cipherHeaderDetails.encryptDomainId,
	                                                                   header.cipherHeaderDetails.baseCipherId,
	                                                                   header.cipherHeaderDetails.salt);
	ASSERT(tCipherKeyKey->isEqual(cipherKey));
	DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, header.iv, BlobCipherMetrics::TEST);
	Reference<EncryptBuf> decrypted = decryptor.decrypt(encrypted->begin(), bufLen, header, arena);

	ASSERT_EQ(decrypted->getLogicalSize(), bufLen);
	ASSERT_EQ(memcmp(decrypted->begin(), &orgData[0], bufLen), 0);

	TraceEvent("BlobCipherTestDecryptDone");

	// induce encryption header corruption - headerVersion corrupted
	BlobCipherEncryptHeader headerCopy;
	encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
	memcpy(reinterpret_cast<uint8_t*>(&headerCopy),
	       reinterpret_cast<const uint8_t*>(&header),
	       sizeof(BlobCipherEncryptHeader));
	headerCopy.flags.headerVersion += 1;
	try {
		DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, headerCopy, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
	}

	// induce encryption header corruption - encryptionMode corrupted
	encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
	memcpy(reinterpret_cast<uint8_t*>(&headerCopy),
	       reinterpret_cast<const uint8_t*>(&header),
	       sizeof(BlobCipherEncryptHeader));
	headerCopy.flags.encryptMode += 1;
	try {
		DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, headerCopy, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
	}

	// induce encryption header corruption - authToken mismatch
	encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
	memcpy(reinterpret_cast<uint8_t*>(&headerCopy),
	       reinterpret_cast<const uint8_t*>(&header),
	       sizeof(BlobCipherEncryptHeader));
	int hIdx = deterministicRandom()->randomInt(0, Params::authTokenSize - 1);
	headerCopy.singleAuthToken.authToken[hIdx] += 1;
	try {
		DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(encrypted->begin(), bufLen, headerCopy, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_authtoken_mismatch) {
			throw;
		}
	}

	// induce encrypted buffer payload corruption
	try {
		encrypted = encryptor.encrypt(&orgData[0], bufLen, &header, arena);
		uint8_t temp[bufLen];
		memcpy(encrypted->begin(), &temp[0], bufLen);
		int tIdx = deterministicRandom()->randomInt(0, bufLen - 1);
		temp[tIdx] += 1;
		DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, header.iv, BlobCipherMetrics::TEST);
		decrypted = decryptor.decrypt(&temp[0], bufLen, header, arena);
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_authtoken_mismatch) {
			throw;
		}
	}

	TraceEvent("BlobCipherTestSingleAuthTokenEnd").detail("Mode", authAlgoStr);
}

template <class Params>
void testConfigurableEncryptionHeaderSingleAuthMode(int minDomainId) {
	constexpr bool isHmac = std::is_same_v<Params, AesCtrWithHmac>;
	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	TraceEvent("TestEncryptionHeaderStart").detail("Mode", isHmac ? "HMAC_SHA" : "AES-CMAC");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                     std::is_same_v<Params, AesCtrWithHmacParams>
	                                         ? EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA
	                                         : EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC,
	                                     BlobCipherMetrics::TEST);
	BlobCipherEncryptHeaderRef headerRef;
	encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(headerRef.flagsVersion(), 1);
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.authTokenMode, headerRef.getAuthTokenMode());
	AesCtrWithAuth<Params> algoHeader = std::get<AesCtrWithAuth<Params>>(headerRef.algoHeader);

	const uint8_t* headerIV = headerRef.getIV();
	ASSERT_EQ(memcmp(&headerIV[0], &iv[0], AES_256_IV_LENGTH), 0);

	EncryptHeaderCipherDetails validateDetails = headerRef.getCipherDetails();
	ASSERT(validateDetails.textCipherDetails.isValid() &&
	       validateDetails.textCipherDetails ==
	           BlobCipherDetails(cipherKey->getDomainId(), cipherKey->getBaseCipherId(), cipherKey->getSalt()));
	ASSERT(validateDetails.headerCipherDetails.present() && validateDetails.headerCipherDetails.get().isValid() &&
	       validateDetails.headerCipherDetails.get() == BlobCipherDetails(headerCipherKey->getDomainId(),
	                                                                      headerCipherKey->getBaseCipherId(),
	                                                                      headerCipherKey->getSalt()));

	Standalone<StringRef> serHeaderRef = BlobCipherEncryptHeaderRef::toStringRef(headerRef);
	BlobCipherEncryptHeaderRef validateHeader = BlobCipherEncryptHeaderRef::fromStringRef(serHeaderRef);
	BlobCipherEncryptHeaderFlagsV1 validateFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(validateHeader.flags);
	ASSERT(validateFlags == flags);

	AesCtrWithAuth<Params> validateAlgo = std::get<AesCtrWithAuth<Params>>(validateHeader.algoHeader);
	ASSERT(validateAlgo.v1.cipherTextDetails == algoHeader.v1.cipherTextDetails);
	ASSERT(validateAlgo.v1.cipherHeaderDetails == algoHeader.v1.cipherHeaderDetails);
	ASSERT_EQ(memcmp(&iv[0], &validateAlgo.v1.iv[0], AES_256_IV_LENGTH), 0);
	ASSERT_EQ(memcmp(&algoHeader.v1.authToken[0], &validateAlgo.v1.authToken[0], Params::authTokenSize), 0);

	TraceEvent("HeaderSize")
	    .detail("Flags", sizeof(flags))
	    .detail("AlgoHeader", algoHeader.getSize())
	    .detail("TotalHeader", serHeaderRef.size());

	TraceEvent("TestEncryptionHeaderEnd").detail("Mode", isHmac ? "HMAC_SHA" : "AES-CMAC");
}

// validate basic encrypt followed by decrypt operation for AUTH_TOKEN_MODE_SINGLE
template <class Params>
void testConfigurableEncryptionSingleAuthMode(const int minDomainId) {
	constexpr bool isHmac = std::is_same_v<Params, AesCtrWithHmacParams>;
	const std::string authAlgoStr = isHmac ? "HMAC-SHA" : "AES-CMAC";
	const EncryptAuthTokenAlgo authAlgo = isHmac ? EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA
	                                             : EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_AES_CMAC;
	const int algoHeaderVersion = isHmac ? CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_HMAC_SHA_AUTH_VERSION
	                                     : CLIENT_KNOBS->ENCRYPT_HEADER_AES_CTR_AES_CMAC_AUTH_VERSION;

	ASSERT(CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION);

	TraceEvent("BlobCipherTestEncryptionHeaderStart").detail("Mode", authAlgoStr);

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	// Validate Encryption ops
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	const int bufLen = deterministicRandom()->randomInt(786, 2127) + 512;
	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);
	uint8_t orgData[bufLen];
	deterministicRandom()->randomBytes(&orgData[0], bufLen);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE,
	                                     authAlgo,
	                                     BlobCipherMetrics::TEST);
	BlobCipherEncryptHeaderRef headerRef;
	StringRef encryptedBuf = encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);

	ASSERT_EQ(encryptedBuf.size(), bufLen);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	ASSERT_EQ(headerRef.flagsVersion(), CLIENT_KNOBS->ENCRYPT_HEADER_FLAGS_VERSION);
	ASSERT_EQ(headerRef.algoHeaderVersion(), algoHeaderVersion);

	// validate flags
	BlobCipherEncryptHeaderFlagsV1 flags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	ASSERT_EQ(flags.encryptMode, EncryptCipherMode::ENCRYPT_CIPHER_MODE_AES_256_CTR);
	ASSERT_EQ(flags.authTokenMode, EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_SINGLE);
	ASSERT_EQ(flags.authTokenAlgo, authAlgo);

	// validate IV
	AesCtrWithAuth<Params> withAuth = std::get<AesCtrWithAuth<Params>>(headerRef.algoHeader);
	ASSERT_EQ(memcmp(&iv[0], &withAuth.v1.iv[0], AES_256_IV_LENGTH), 0);
	ASSERT_NE(memcmp(&orgData[0], encryptedBuf.begin(), bufLen), 0);
	// validate cipherKey details
	ASSERT_EQ(withAuth.v1.cipherTextDetails.encryptDomainId, cipherKey->getDomainId());
	ASSERT_EQ(withAuth.v1.cipherTextDetails.baseCipherId, cipherKey->getBaseCipherId());
	ASSERT_EQ(withAuth.v1.cipherTextDetails.salt, cipherKey->getSalt());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.encryptDomainId, headerCipherKey->getDomainId());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.baseCipherId, headerCipherKey->getBaseCipherId());
	ASSERT_EQ(withAuth.v1.cipherHeaderDetails.salt, headerCipherKey->getSalt());

	Reference<BlobCipherKey> tCipherKeyKey = cipherKeyCache->getCipherKey(withAuth.v1.cipherTextDetails.encryptDomainId,
	                                                                      withAuth.v1.cipherTextDetails.baseCipherId,
	                                                                      withAuth.v1.cipherTextDetails.salt);
	Reference<BlobCipherKey> hCipherKey = cipherKeyCache->getCipherKey(withAuth.v1.cipherHeaderDetails.encryptDomainId,
	                                                                   withAuth.v1.cipherHeaderDetails.baseCipherId,
	                                                                   withAuth.v1.cipherHeaderDetails.salt);
	ASSERT(tCipherKeyKey->isEqual(cipherKey));
	ASSERT(hCipherKey->isEqual(headerCipherKey));
	DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, &withAuth.v1.iv[0], BlobCipherMetrics::TEST);
	StringRef decryptedBuf = decryptor.decrypt(encryptedBuf.begin(), bufLen, headerRef, arena);

	ASSERT_EQ(decryptedBuf.size(), bufLen);
	ASSERT_EQ(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);

	TraceEvent("BlobCipherTestEncryptDecryptDone")
	    .detail("HeaderFlagsVersion", headerRef.flagsVersion())
	    .detail("AlgoHeaderVersion", headerRef.algoHeaderVersion())
	    .detail("HeaderEncryptMode", flags.encryptMode)
	    .detail("HeaderEncryptAuthTokenMode", flags.authTokenMode)
	    .detail("HeaderEncryptAuthTokenAlgo", flags.authTokenAlgo)
	    .detail("TextDomainId", withAuth.v1.cipherTextDetails.encryptDomainId)
	    .detail("TextBaseCipherId", withAuth.v1.cipherTextDetails.baseCipherId)
	    .detail("TextSalt", withAuth.v1.cipherTextDetails.salt)
	    .detail("HeaderDomainId", withAuth.v1.cipherHeaderDetails.encryptDomainId)
	    .detail("HeaderBaseCipherId", withAuth.v1.cipherHeaderDetails.baseCipherId)
	    .detail("HeaderSalt", withAuth.v1.cipherHeaderDetails.salt);

	// induce encryption header corruption - encryptionMode corrupted
	BlobCipherEncryptHeaderRef corruptedHeaderRef = BlobCipherEncryptHeaderRef(headerRef);
	BlobCipherEncryptHeaderFlagsV1 corruptedFlags = std::get<BlobCipherEncryptHeaderFlagsV1>(headerRef.flags);
	corruptedFlags.encryptMode += 1;
	corruptedHeaderRef.flags = corruptedFlags;
	try {
		encryptedBuf = encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);
		DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, &iv[0], BlobCipherMetrics::TEST);
		decryptedBuf = decryptor.decrypt(encryptedBuf.begin(), bufLen, corruptedHeaderRef, arena);
		ASSERT(false); // error expected
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_metadata_mismatch) {
			throw;
		}
		TraceEvent("ConfigurableEncryptionCorruptEncryptModeDone").detail("Mode", authAlgoStr);
	}

	// induce encrypted buffer payload corruption
	try {
		encryptedBuf = encryptor.encrypt(&orgData[0], bufLen, &headerRef, arena);
		uint8_t temp[bufLen];
		memcpy((void*)encryptedBuf.begin(), &temp[0], bufLen);
		int tIdx = deterministicRandom()->randomInt(0, bufLen - 1);
		temp[tIdx] += 1;
		DecryptBlobCipherAes256Ctr decryptor(tCipherKeyKey, hCipherKey, &iv[0], BlobCipherMetrics::TEST);
		decryptedBuf = decryptor.decrypt(&temp[0], bufLen, headerRef, arena);
		ASSERT_NE(memcmp(decryptedBuf.begin(), &orgData[0], bufLen), 0);
	} catch (Error& e) {
		if (e.code() != error_code_encrypt_header_authtoken_mismatch) {
			throw;
		}
		TraceEvent("ConfigurableEncryptionCorruptPayloadDone").detail("Mode", authAlgoStr);
	}

	TraceEvent("TestSingleAuthTokenConfigurableEncryptionEnd").detail("Mode", authAlgoStr);
}

void testKeyCacheCleanup(const int minDomainId, const int maxDomainId) {
	TraceEvent("BlobCipherTestKeyCacheCleanupStart");

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	// Validate dropping encryptDomainId cached keys
	const EncryptCipherDomainId candidate = deterministicRandom()->randomInt(minDomainId, maxDomainId);
	cipherKeyCache->resetEncryptDomainId(candidate);
	std::vector<Reference<BlobCipherKey>> cachedKeys = cipherKeyCache->getAllCiphers(candidate);
	ASSERT(cachedKeys.empty());

	// Validate dropping all cached cipherKeys
	cipherKeyCache->cleanup();
	for (int dId = minDomainId; dId < maxDomainId; dId++) {
		std::vector<Reference<BlobCipherKey>> cachedKeys = cipherKeyCache->getAllCiphers(dId);
		ASSERT(cachedKeys.empty());
	}

	TraceEvent("BlobCipherTestKeyCacheCleanupDone");
}

TEST_CASE("/blobCipher") {
	DomainKeyMap domainKeyMap;
	auto& g_knobs = IKnobCollection::getMutableGlobalKnobCollection();
	g_knobs.setKnob("enable_configurable_encryption", KnobValueRef::create(bool{ true }));

	const EncryptCipherDomainId minDomainId = 1;
	const EncryptCipherDomainId maxDomainId = deterministicRandom()->randomInt(minDomainId, minDomainId + 10) + 5;
	const EncryptCipherBaseKeyId minBaseCipherKeyId = 100;
	const EncryptCipherBaseKeyId maxBaseCipherKeyId =
	    deterministicRandom()->randomInt(minBaseCipherKeyId, minBaseCipherKeyId + 50) + 15;
	for (int dId = minDomainId; dId <= maxDomainId; dId++) {
		for (int kId = minBaseCipherKeyId; kId <= maxBaseCipherKeyId; kId++) {
			domainKeyMap[dId].emplace(
			    kId,
			    makeReference<BaseCipher>(
			        dId, kId, std::numeric_limits<int64_t>::max(), std::numeric_limits<int64_t>::max()));
		}
	}
	ASSERT_EQ(domainKeyMap.size(), maxDomainId);

	testKeyCacheEssentials(domainKeyMap, minDomainId, maxDomainId, minBaseCipherKeyId);

	testConfigurableEncryptionBlobCipherHeaderFlagsV1Ser();
	testConfigurableEncryptionAesCtrNoAuthV1Ser(minDomainId);
	testConfigurableEncryptionAesCtrWithAuthSer<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionAesCtrWithAuthSer<AesCtrWithCmacParams>(minDomainId);

	testConfigurableEncryptionHeaderNoAuthMode(minDomainId);
	testConfigurableEncryptionHeaderSingleAuthMode<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionHeaderSingleAuthMode<AesCtrWithCmacParams>(minDomainId);

	testNoAuthMode(minDomainId);
	testSingleAuthMode<AesCtrWithHmacParams>(minDomainId);
	testSingleAuthMode<AesCtrWithCmacParams>(minDomainId);

	testConfigurableEncryptionNoAuthMode(minDomainId);
	testConfigurableEncryptionSingleAuthMode<AesCtrWithHmacParams>(minDomainId);
	testConfigurableEncryptionSingleAuthMode<AesCtrWithCmacParams>(minDomainId);
	testKeyCacheCleanup(minDomainId, maxDomainId);

	return Void();
}