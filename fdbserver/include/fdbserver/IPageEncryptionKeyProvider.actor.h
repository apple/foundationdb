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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"
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

#include "fdbserver/IPager.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#define XXH_INLINE_ALL
#include "flow/xxhash.h"

#include <array>
#include <functional>
#include <limits>
#include <tuple>
#include <type_traits>

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

	struct EncryptionDomain {
		int64_t domainId;
		size_t prefixLength;
	};

	struct EncryptionDomainInfo {
		EncryptionDomain domain;
		Optional<EncryptionDomain> outerDomain;

		EncryptionDomainInfo(const EncryptionDomain& dom) : domain(dom){};

		EncryptionDomainInfo(const KeyRef& key, const EncryptionDomain& dom, const EncryptionDomain& outer) {
			domain = dom;
			if (key.size() == dom.prefixLength) {
				outerDomain = outer;
			}
		}
	};

	virtual ~IPageEncryptionKeyProvider() = default;

	// Expected encoding type being used with the encryption key provider.
	virtual EncodingType expectedEncodingType() const = 0;

	// Whether encryption domain is enabled.
	virtual bool enableEncryptionDomain() const = 0;

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
	// If the key belong to a key range that needs to stay unencrypted, INVALID_ENCRYPT_DOMAIN_ID is returned as domain
	// id.
	virtual EncryptionDomainInfo getEncryptionDomain(const KeyRef& key) { throw not_implemented(); }

	// Get encryption domain of a page given encoding header.
	virtual int64_t getEncryptionDomainIdFromHeader(const void* encodingHeader) { throw not_implemented(); }

	// Whether the key range between the two keys (inclusive) may cross multiple encryption domain.
	virtual bool mayCrossEncryptionDomains(const KeyRef& begin, const KeyRef& end) { throw not_implemented(); }

	// Helper methods.

	// Check if a key fits in an encryption domain.
	bool keyFitsInDomain(int64_t domainId, const KeyRef& key, bool canUseOuterDomain) {
		ASSERT(enableEncryptionDomain());
		auto info = getEncryptionDomain(key);
		return info.domain.domainId == domainId ||
		       (canUseOuterDomain && info.outerDomain.present() && info.outerDomain.get().domainId == domainId);
	}
};

inline bool operator==(const IPageEncryptionKeyProvider::EncryptionDomain& a,
                       const IPageEncryptionKeyProvider::EncryptionDomain& b) {
	return a.domainId == b.domainId;
}

// The null key provider is useful to simplify page decoding.
// It throws an error for any key info requested.
class NullEncryptionKeyProvider : public IPageEncryptionKeyProvider {
public:
	virtual ~NullEncryptionKeyProvider() {}
	EncodingType expectedEncodingType() const override { return EncodingType::XXHash64; }
	bool enableEncryptionDomain() const override { return false; }
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

	bool enableEncryptionDomain() const override { return false; }

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

namespace {
template <EncodingType encodingType>
int64_t getEncryptionDomainIdFromAesEncryptionHeader(const void* encodingHeader) {
	using Encoder = typename ArenaPage::AESEncryptionEncoder<encodingType>;
	using EncodingHeader = typename Encoder::Header;
	ASSERT(encodingHeader != nullptr);
	if (CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION) {
		BlobCipherEncryptHeaderRef headerRef = Encoder::getEncryptionHeaderRef(encodingHeader);
		return headerRef.getCipherDetails().textCipherDetails.encryptDomainId;
	} else {
		const BlobCipherEncryptHeader& header = reinterpret_cast<const EncodingHeader*>(encodingHeader)->encryption;
		return header.cipherTextDetails.encryptDomainId;
	}
}
} // anonymous namespace

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

	bool enableEncryptionDomain() const override { return mode > 0; }

	Future<EncryptionKey> getEncryptionKey(const void* encodingHeader) override {
		using Encoder = typename ArenaPage::AESEncryptionEncoder<encodingType>;
		EncryptionKey s;
		if (CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION) {
			const BlobCipherEncryptHeaderRef headerRef = Encoder::getEncryptionHeaderRef(encodingHeader);
			EncryptHeaderCipherDetails details = headerRef.getCipherDetails();
			ASSERT(details.textCipherDetails.isValid());
			s.aesKey.cipherTextKey =
			    getCipherKey(details.textCipherDetails.encryptDomainId, details.textCipherDetails.baseCipherId);
			if (details.headerCipherDetails.present()) {
				ASSERT(details.headerCipherDetails.get().isValid());
				s.aesKey.cipherHeaderKey = getCipherKey(details.headerCipherDetails.get().encryptDomainId,
				                                        details.headerCipherDetails.get().baseCipherId);
			}
		} else {
			const typename Encoder::Header* h = reinterpret_cast<const typename Encoder::Header*>(encodingHeader);
			s.aesKey.cipherTextKey = getCipherKey(h->encryption.cipherTextDetails.encryptDomainId,
			                                      h->encryption.cipherTextDetails.baseCipherId);
			if (h->encryption.cipherHeaderDetails.isValid()) {
				s.aesKey.cipherHeaderKey = getCipherKey(h->encryption.cipherHeaderDetails.encryptDomainId,
				                                        h->encryption.cipherHeaderDetails.baseCipherId);
			}
		}
		return s;
	}

	Future<EncryptionKey> getLatestEncryptionKey(int64_t domainId) override {
		domainId = checkDomainId(domainId);
		EncryptionKey s;
		s.aesKey.cipherTextKey = getCipherKey(domainId, deterministicRandom()->randomInt(1, NUM_CIPHER + 1));
		s.aesKey.cipherHeaderKey =
		    getCipherKey(ENCRYPT_HEADER_DOMAIN_ID, deterministicRandom()->randomInt(1, NUM_CIPHER + 1));
		return s;
	}

	int64_t getDefaultEncryptionDomainId() const override { return FDB_DEFAULT_ENCRYPT_DOMAIN_ID; }

	EncryptionDomainInfo getEncryptionDomain(const KeyRef& key) override {
		static const EncryptionDomain defaultDomain{ FDB_DEFAULT_ENCRYPT_DOMAIN_ID, 0 };
		if (key.size() < PREFIX_LENGTH) {
			return EncryptionDomainInfo(defaultDomain);
		} else {
			// Use first 4 bytes as a 32-bit int for the domain id.
			int64_t domainId = checkDomainId(static_cast<int64_t>(*reinterpret_cast<const int32_t*>(key.begin())));
			EncryptionDomain dom{ domainId, PREFIX_LENGTH };
			return EncryptionDomainInfo(key, dom, defaultDomain);
		}
	}

	int64_t getEncryptionDomainIdFromHeader(const void* encodingHeader) override {
		return getEncryptionDomainIdFromAesEncryptionHeader<encodingType>(encodingHeader);
	}

	bool mayCrossEncryptionDomains(const KeyRef& begin, const KeyRef& end) override {
		if (begin.size() < PREFIX_LENGTH || end.size() < PREFIX_LENGTH) {
			return true;
		}
		int32_t beginDomainId = *reinterpret_cast<const int32_t*>(begin.begin());
		int32_t endDomainId = *reinterpret_cast<const int32_t*>(end.begin());
		return beginDomainId != endDomainId;
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
		ASSERT(cipherId > 0 && cipherId <= NUM_CIPHER);
		return makeReference<BlobCipherKey>(domainId,
		                                    cipherId,
		                                    cipherKeys[cipherId - 1]->rawBaseCipher(),
		                                    AES_256_KEY_LENGTH,
		                                    cipherKeys[cipherId - 1]->getSalt(),
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
	using Encoder = typename ArenaPage::AESEncryptionEncoder<encodingType>;
	using EncodingHeader = typename Encoder::Header;

	const KeyRef systemKeysPrefix = systemKeys.begin;

	const EncryptionDomain defaultDomain{ FDB_DEFAULT_ENCRYPT_DOMAIN_ID, 0 };
	const EncryptionDomain systemKeyDomain{ SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID,
		                                    static_cast<size_t>(systemKeysPrefix.size()) };

	const EncryptCipherDomainId whitelistedDomainIdBegin = MIN_ENCRYPT_DOMAIN_ID - 1;
	const std::array<KeyRef, 3> whitelistedKeyPrefixes = { applyLogKeys.begin,
		                                                   backupLogKeys.begin,
		                                                   changeFeedDurableKeys.begin };
	const std::array<EncryptionDomain, 3> whitelistedDomain = {
		EncryptionDomain{ whitelistedDomainIdBegin, static_cast<size_t>(whitelistedKeyPrefixes[0].size()) },
		EncryptionDomain{ whitelistedDomainIdBegin - 1, static_cast<size_t>(whitelistedKeyPrefixes[1].size()) },
		EncryptionDomain{ whitelistedDomainIdBegin - 2, static_cast<size_t>(whitelistedKeyPrefixes[2].size()) },
	};

	AESEncryptionKeyProvider(Reference<AsyncVar<ServerDBInfo> const> db, EncryptionAtRestMode encryptionMode)
	  : db(db), encryptionMode(encryptionMode) {
		ASSERT(encryptionMode != EncryptionAtRestMode::DISABLED);
		ASSERT(db.isValid());
		// We assume all whitelisted key ranges are within the system key range. Otherwise getEncryptionDomain() needs
		// to be updated.
		for (auto& whitelisted : whitelistedKeyPrefixes) {
			ASSERT(whitelisted.startsWith(systemKeysPrefix));
		}
	}

	virtual ~AESEncryptionKeyProvider() = default;

	EncodingType expectedEncodingType() const override { return encodingType; }

	bool enableEncryptionDomain() const override {
		// Regardless of encryption mode, system keys always encrypted using system key space domain.
		// Because of this, AESEncryptionKeyProvider always appears to be domain-aware.
		return true;
	}

	ACTOR static Future<EncryptionKey> getEncryptionKey(AESEncryptionKeyProvider* self, const void* encodingHeader) {
		state TextAndHeaderCipherKeys cipherKeys;
		if (CLIENT_KNOBS->ENABLE_CONFIGURABLE_ENCRYPTION) {
			BlobCipherEncryptHeaderRef headerRef = Encoder::getEncryptionHeaderRef(encodingHeader);
			TextAndHeaderCipherKeys cks =
			    wait(getEncryptCipherKeys(self->db, headerRef, BlobCipherMetrics::KV_REDWOOD));
			cipherKeys = cks;
		} else {
			const BlobCipherEncryptHeader& header = reinterpret_cast<const EncodingHeader*>(encodingHeader)->encryption;
			TextAndHeaderCipherKeys cks = wait(getEncryptCipherKeys(self->db, header, BlobCipherMetrics::KV_REDWOOD));
			cipherKeys = cks;
		}
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

	EncryptionDomainInfo getEncryptionDomain(const KeyRef& key) override {
		// System key. We assume whitelisted key ranges are all within the system key space to make the check more
		// efficient.
		if (key.startsWith(systemKeysPrefix)) {
			for (int i = 0; i < whitelistedKeyPrefixes.size(); i++) {
				if (key.startsWith(whitelistedKeyPrefixes[i])) {
					return EncryptionDomainInfo(key, whitelistedDomain[i], systemKeyDomain);
				}
			}
			return EncryptionDomainInfo(key, systemKeyDomain, defaultDomain);
		}
		// Cluster-aware encryption.
		if (encryptionMode == EncryptionAtRestMode::CLUSTER_AWARE) {
			return EncryptionDomainInfo(defaultDomain);
		}
		// Key smaller than tenant prefix in size belongs to the default domain.
		if (key.size() < TenantAPI::PREFIX_SIZE) {
			return EncryptionDomainInfo(defaultDomain);
		}
		int64_t tenantId = TenantAPI::extractTenantIdFromKeyRef(key);
		if (tenantId == TenantInfo::INVALID_TENANT) {
			return EncryptionDomainInfo(defaultDomain);
		}
		EncryptionDomain dom{ tenantId, TenantAPI::PREFIX_SIZE };
		return EncryptionDomainInfo(key, dom, defaultDomain);
	}

	int64_t getEncryptionDomainIdFromHeader(const void* encodingHeader) override {
		return getEncryptionDomainIdFromAesEncryptionHeader<encodingType>(encodingHeader);
	}

	bool mayCrossEncryptionDomains(const KeyRef& begin, const KeyRef& end) override {
		int64_t beginDomainId = getEncryptionDomain(begin).domain.domainId;
		int64_t endDomainId = getEncryptionDomain(end).domain.domainId;
		// For simplicity, return true if both of the end keys falls in one of the special encryption domain.
		// For example if both keys falls in system key space, the key range may still cross one of the
		// whitelisted key ranges.
		return beginDomainId != endDomainId || beginDomainId == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID ||
		       (beginDomainId == FDB_DEFAULT_ENCRYPT_DOMAIN_ID &&
		        encryptionMode != EncryptionAtRestMode::CLUSTER_AWARE);
	}

private:
	Reference<AsyncVar<ServerDBInfo> const> db;
	EncryptionAtRestMode encryptionMode;
};

#include "flow/unactorcompiler.h"
#endif