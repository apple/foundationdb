/*
 * IPageEncryptionKeyProvider.actor.h
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

#include "fdbclient/TenantManagement.actor.h"
#include "fdbrpc/TenantInfo.h"
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_IPAGEENCRYPTIONKEYPROVIDER_ACTOR_G_H)
#define FDBSERVER_IPAGEENCRYPTIONKEYPROVIDER_ACTOR_G_H
#include "fdbserver/IPageEncryptionKeyProvider.actor.g.h"
#elif !defined(FDBSERVER_IPAGEENCRYPTIONKEYPROVIDER_ACTOR_H)
#define FDBSERVER_IPAGEENCRYPTIONKEYPROVIDER_ACTOR_H

#include "fdbclient/BlobCipher.h"
#include "fdbclient/GetEncryptCipherKeys.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/Tenant.h"

#include "fdbserver/EncryptionOpsUtils.h"
#include "fdbserver/IPager.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#define XXH_INLINE_ALL
#include "flow/xxhash.h"

#include <functional>
#include <limits>
#include <tuple>

#include "flow/actorcompiler.h" // This must be the last #include.

// Interface used by pager to get encryption keys reading pages from disk
// and by the BTree to get encryption keys to use for new pages.
//
// Cipher key rotation:
// The key provider can rotate encryption keys, potentially per encryption domain (see below). Each of the new pages
// are encrypted using the latest encryption keys.
//
// Encryption domains:
// The key provider can specify how the page split the full key range into encryption domains by key prefixes.
// Encryption domains are expected to have their own set of encryption keys, which is managed by the key provider.
// The pager will ensure that data from different encryption domain won't fall in the same page, to make
// sure it can use one single encryption key to encrypt the whole page.
// The key provider needs to provide a default encryption domain, which is used to encrypt pages contain only
// full or partial encryption domain prefixes.
class IPageEncryptionKeyProvider : public ReferenceCounted<IPageEncryptionKeyProvider> {
public:
	using EncryptionKey = ArenaPage::EncryptionKey;

	virtual ~IPageEncryptionKeyProvider() = default;

	// Expected encoding type being used with the encryption key provider.
	virtual EncodingType expectedEncodingType() const = 0;

	// Whether domain-aware encryption is enabled.
	virtual bool isDomainAware() const { return false; }

	// Get an encryption key from given encoding header.
	virtual Future<EncryptionKey> getEncryptionKey(const void* encodingHeader) { throw not_implemented(); }

	// Get latest encryption key. If encryption domain is enabled, get encryption key for the default domain.
	virtual Future<EncryptionKey> getLatestDefaultEncryptionKey() {
		return getLatestEncryptionKey(getDefaultEncryptionDomainId());
	}

	// Get latest encryption key for data in given encryption domain.
	virtual Future<EncryptionKey> getLatestEncryptionKey(int64_t domainId) { throw not_implemented(); }

	// Return the default encryption domain.
	virtual int64_t getDefaultEncryptionDomainId() const { throw not_implemented(); }

	// Get encryption domain from a key. Return the domain id, and the size of the encryption domain prefix.
	virtual std::tuple<int64_t, size_t> getEncryptionDomain(const KeyRef& key) { throw not_implemented(); }

	// Get encryption domain of a page given encoding header.
	virtual int64_t getEncryptionDomainIdFromHeader(const void* encodingHeader) { throw not_implemented(); }

	// Helper methods.

	// Check if a key fits in an encryption domain.
	bool keyFitsInDomain(int64_t domainId, const KeyRef& key, bool canUseDefaultDomain) {
		ASSERT(isDomainAware());
		int64_t keyDomainId;
		size_t prefixLength;
		std::tie(keyDomainId, prefixLength) = getEncryptionDomain(key);
		return keyDomainId == domainId ||
		       (canUseDefaultDomain && (domainId == getDefaultEncryptionDomainId() && key.size() == prefixLength));
	}
};

// The null key provider is useful to simplify page decoding.
// It throws an error for any key info requested.
class NullEncryptionKeyProvider : public IPageEncryptionKeyProvider {
public:
	virtual ~NullEncryptionKeyProvider() {}
	EncodingType expectedEncodingType() const override { return EncodingType::XXHash64; }
};

// Key provider for dummy XOR encryption scheme
class XOREncryptionKeyProvider_TestOnly : public IPageEncryptionKeyProvider {
public:
	using EncodingHeader = ArenaPage::XOREncryptionEncoder::Header;

	XOREncryptionKeyProvider_TestOnly(std::string filename) {
		ASSERT(g_network->isSimulated());

		// Choose a deterministic random filename (without path) byte for secret generation
		// Remove any leading directory names
		size_t lastSlash = filename.find_last_of("\\/");
		if (lastSlash != filename.npos) {
			filename.erase(0, lastSlash);
		}
		xorWith = filename.empty() ? 0x5e
		                           : (uint8_t)filename[XXH3_64bits(filename.data(), filename.size()) % filename.size()];
	}

	virtual ~XOREncryptionKeyProvider_TestOnly() {}

	EncodingType expectedEncodingType() const override { return EncodingType::XOREncryption_TestOnly; }

	Future<EncryptionKey> getEncryptionKey(const void* encodingHeader) override {

		const EncodingHeader* h = reinterpret_cast<const EncodingHeader*>(encodingHeader);
		EncryptionKey s;
		s.xorKey = h->xorKey;
		s.xorWith = xorWith;
		return s;
	}

	Future<EncryptionKey> getLatestDefaultEncryptionKey() override {
		EncryptionKey s;
		s.xorKey = static_cast<uint8_t>(deterministicRandom()->randomInt(0, std::numeric_limits<uint8_t>::max() + 1));
		s.xorWith = xorWith;
		return s;
	}

	uint8_t xorWith;
};

// Key provider to provider cipher keys randomly from a pre-generated pool. It does not maintain encryption domains.
// Use for testing.
template <EncodingType encodingType,
          typename std::enable_if<encodingType == AESEncryption || encodingType == AESEncryptionWithAuth, bool>::type =
              true>
class RandomEncryptionKeyProvider : public IPageEncryptionKeyProvider {
public:
	enum EncryptionDomainMode : unsigned int {
		DISABLED = 0, // disable encryption domain
		RANDOM, // for each key prefix, deterministic randomly decide if there's an encryption domain for it.
		ALL, // all key prefixes has an encryption domain assigned to it.
		MAX,
	};

	explicit RandomEncryptionKeyProvider(EncryptionDomainMode mode = DISABLED) : mode(mode) {
		ASSERT(mode < EncryptionDomainMode::MAX);
		for (unsigned i = 0; i < NUM_CIPHER; i++) {
			BlobCipherDetails cipherDetails;
			cipherDetails.encryptDomainId = 0;
			cipherDetails.baseCipherId = i;
			cipherDetails.salt = deterministicRandom()->randomUInt64();
			cipherKeys[i] = generateCipherKey(cipherDetails);
		}
	}
	virtual ~RandomEncryptionKeyProvider() = default;

	EncodingType expectedEncodingType() const override { return encodingType; }

	bool isDomainAware() const override { return mode > 0; }

	Future<EncryptionKey> getEncryptionKey(const void* encodingHeader) override {
		using Header = typename ArenaPage::AESEncryptionEncoder<encodingType>::Header;
		const Header* h = reinterpret_cast<const Header*>(encodingHeader);
		EncryptionKey s;
		s.aesKey.cipherTextKey =
		    getCipherKey(h->encryption.cipherTextDetails.encryptDomainId, h->encryption.cipherTextDetails.baseCipherId);
		s.aesKey.cipherHeaderKey = getCipherKey(h->encryption.cipherHeaderDetails.encryptDomainId,
		                                        h->encryption.cipherHeaderDetails.baseCipherId);
		return s;
	}

	Future<EncryptionKey> getLatestEncryptionKey(int64_t domainId) override {
		domainId = checkDomainId(domainId);
		EncryptionKey s;
		s.aesKey.cipherTextKey = getCipherKey(domainId, deterministicRandom()->randomInt(0, NUM_CIPHER));
		s.aesKey.cipherHeaderKey =
		    getCipherKey(ENCRYPT_HEADER_DOMAIN_ID, deterministicRandom()->randomInt(0, NUM_CIPHER));
		return s;
	}

	int64_t getDefaultEncryptionDomainId() const override { return FDB_DEFAULT_ENCRYPT_DOMAIN_ID; }

	std::tuple<int64_t, size_t> getEncryptionDomain(const KeyRef& key) override {
		int64_t domainId;
		if (key.size() < PREFIX_LENGTH) {
			domainId = getDefaultEncryptionDomainId();
		} else {
			// Use first 4 bytes as a 32-bit int for the domain id.
			domainId = checkDomainId(static_cast<int64_t>(*reinterpret_cast<const int32_t*>(key.begin())));
		}
		return { domainId, (domainId == getDefaultEncryptionDomainId() ? 0 : PREFIX_LENGTH) };
	}

	int64_t getEncryptionDomainIdFromHeader(const void* encodingHeader) override {
		ASSERT(encodingHeader != nullptr);
		using Header = typename ArenaPage::AESEncryptionEncoder<encodingType>::Header;
		const Header* h = reinterpret_cast<const Header*>(encodingHeader);
		return h->encryption.cipherTextDetails.encryptDomainId;
	}

private:
	Reference<BlobCipherKey> generateCipherKey(const BlobCipherDetails& cipherDetails) {
		static unsigned char SHA_KEY[] = "3ab9570b44b8315fdb261da6b1b6c13b";
		Arena arena;
		uint8_t digest[AUTH_TOKEN_HMAC_SHA_SIZE];
		computeAuthToken(
		    { { reinterpret_cast<const uint8_t*>(&cipherDetails.baseCipherId), sizeof(EncryptCipherBaseKeyId) } },
		    SHA_KEY,
		    AES_256_KEY_LENGTH,
		    &digest[0],
		    EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
		    AUTH_TOKEN_HMAC_SHA_SIZE);
		ASSERT_EQ(AUTH_TOKEN_HMAC_SHA_SIZE, AES_256_KEY_LENGTH);
		return makeReference<BlobCipherKey>(cipherDetails.encryptDomainId,
		                                    cipherDetails.baseCipherId,
		                                    &digest[0],
		                                    AES_256_KEY_LENGTH,
		                                    cipherDetails.salt,
		                                    std::numeric_limits<int64_t>::max() /* refreshAt */,
		                                    std::numeric_limits<int64_t>::max() /* expireAt */);
	}

	int64_t checkDomainId(int64_t domainId) {
		std::hash<int64_t> hasher;
		if (mode == DISABLED || (mode == RANDOM && hasher(domainId) % 2 == 0)) {
			return getDefaultEncryptionDomainId();
		}
		return domainId;
	}

	Reference<BlobCipherKey> getCipherKey(EncryptCipherDomainId domainId, EncryptCipherBaseKeyId cipherId) {
		// Create a new cipher key by replacing the domain id.
		return makeReference<BlobCipherKey>(domainId,
		                                    cipherId,
		                                    cipherKeys[cipherId]->rawBaseCipher(),
		                                    AES_256_KEY_LENGTH,
		                                    cipherKeys[cipherId]->getSalt(),
		                                    std::numeric_limits<int64_t>::max() /* refreshAt */,
		                                    std::numeric_limits<int64_t>::max() /* expireAt */);
	}

	static constexpr int NUM_CIPHER = 1000;
	static constexpr size_t PREFIX_LENGTH = 4;
	EncryptionDomainMode mode;
	Reference<BlobCipherKey> cipherKeys[NUM_CIPHER];
};

// Key provider which extract tenant id from range key prefixes, and fetch tenant specific encryption keys from
// EncryptKeyProxy.
template <EncodingType encodingType,
          typename std::enable_if<encodingType == AESEncryption || encodingType == AESEncryptionWithAuth, bool>::type =
              true>
class AESEncryptionKeyProvider : public IPageEncryptionKeyProvider {
public:
	using EncodingHeader = typename ArenaPage::AESEncryptionEncoder<encodingType>::Header;

	const StringRef systemKeysPrefix = systemKeys.begin;

	AESEncryptionKeyProvider(Reference<AsyncVar<ServerDBInfo> const> db, EncryptionAtRestMode encryptionMode)
	  : db(db), encryptionMode(encryptionMode) {
		ASSERT(encryptionMode != EncryptionAtRestMode::DISABLED);
		ASSERT(db.isValid());
	}

	virtual ~AESEncryptionKeyProvider() = default;

	EncodingType expectedEncodingType() const override { return encodingType; }

	bool isDomainAware() const override {
		// Regardless of encryption mode, system keys always encrypted using system key space domain.
		// Because of this, AESEncryptionKeyProvider always appears to be domain-aware.
		return true;
	}

	ACTOR static Future<EncryptionKey> getEncryptionKey(AESEncryptionKeyProvider* self, const void* encodingHeader) {
		const BlobCipherEncryptHeader& header = reinterpret_cast<const EncodingHeader*>(encodingHeader)->encryption;
		TextAndHeaderCipherKeys cipherKeys =
		    wait(getEncryptCipherKeys(self->db, header, BlobCipherMetrics::KV_REDWOOD));
		EncryptionKey encryptionKey;
		encryptionKey.aesKey = cipherKeys;
		return encryptionKey;
	}

	Future<EncryptionKey> getEncryptionKey(const void* encodingHeader) override {
		return getEncryptionKey(this, encodingHeader);
	}

	Future<EncryptionKey> getLatestDefaultEncryptionKey() override {
		return getLatestEncryptionKey(getDefaultEncryptionDomainId());
	}

	ACTOR static Future<EncryptionKey> getLatestEncryptionKey(AESEncryptionKeyProvider* self, int64_t domainId) {
		ASSERT(self->encryptionMode == EncryptionAtRestMode::DOMAIN_AWARE || domainId < 0);
		TextAndHeaderCipherKeys cipherKeys =
		    wait(getLatestEncryptCipherKeysForDomain(self->db, domainId, BlobCipherMetrics::KV_REDWOOD));
		EncryptionKey encryptionKey;
		encryptionKey.aesKey = cipherKeys;
		return encryptionKey;
	}

	Future<EncryptionKey> getLatestEncryptionKey(int64_t domainId) override {
		return getLatestEncryptionKey(this, domainId);
	}

	int64_t getDefaultEncryptionDomainId() const override { return FDB_DEFAULT_ENCRYPT_DOMAIN_ID; }

	std::tuple<int64_t, size_t> getEncryptionDomain(const KeyRef& key) override {
		// System key.
		if (key.startsWith(systemKeysPrefix)) {
			return { SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID, systemKeysPrefix.size() };
		}
		// Cluster-aware encryption.
		if (encryptionMode == EncryptionAtRestMode::CLUSTER_AWARE) {
			return { FDB_DEFAULT_ENCRYPT_DOMAIN_ID, 0 };
		}
		// Key smaller than tenant prefix in size belongs to the default domain.
		if (key.size() < TenantAPI::PREFIX_SIZE) {
			return { FDB_DEFAULT_ENCRYPT_DOMAIN_ID, 0 };
		}
		int64_t tenantId = TenantAPI::extractTenantIdFromKeyRef(key);
		if (tenantId == TenantInfo::INVALID_TENANT) {
			return { FDB_DEFAULT_ENCRYPT_DOMAIN_ID, 0 };
		}
		return { tenantId, TenantAPI::PREFIX_SIZE };
	}

	int64_t getEncryptionDomainIdFromHeader(const void* encodingHeader) override {
		ASSERT(encodingHeader != nullptr);
		const BlobCipherEncryptHeader& header = reinterpret_cast<const EncodingHeader*>(encodingHeader)->encryption;
		return header.cipherTextDetails.encryptDomainId;
	}

private:
	Reference<AsyncVar<ServerDBInfo> const> db;
	EncryptionAtRestMode encryptionMode;
};

#include "flow/unactorcompiler.h"
#endif