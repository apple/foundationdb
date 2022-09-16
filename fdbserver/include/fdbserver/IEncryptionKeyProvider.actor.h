/*
 * IEncryptionKeyProvider.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_IENCRYPTIONKEYPROVIDER_ACTOR_G_H)
#define FDBSERVER_IENCRYPTIONKEYPROVIDER_ACTOR_G_H
#include "fdbserver/IEncryptionKeyProvider.actor.g.h"
#elif !defined(FDBSERVER_IENCRYPTIONKEYPROVIDER_ACTOR_H)
#define FDBSERVER_IENCRYPTIONKEYPROVIDER_ACTOR_H

#include "fdbclient/GetEncryptCipherKeys.actor.h"
#include "fdbclient/Tenant.h"

#include "fdbserver/EncryptionOpsUtils.h"
#include "fdbserver/ServerDBInfo.h"

#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#define XXH_INLINE_ALL
#include "flow/xxhash.h"

#include "flow/actorcompiler.h" // This must be the last #include.

typedef uint64_t XOREncryptionKeyID;

// EncryptionKeyRef is somewhat multi-variant, it will contain members representing the union
// of all fields relevant to any implemented encryption scheme.  They are generally of
// the form
//   Page Fields - fields which come from or are stored in the Page
//   Secret Fields - fields which are only known by the Key Provider
// but it is up to each encoding and provider which fields are which and which ones are used
//
// TODO(yiwu): Rename and/or refactor this struct. It doesn't sound like an encryption key should
// contain page fields like encryption header.
struct EncryptionKeyRef {

	EncryptionKeyRef(){};
	EncryptionKeyRef(Arena& arena, const EncryptionKeyRef& toCopy)
	  : cipherKeys(toCopy.cipherKeys), secret(arena, toCopy.secret), id(toCopy.id) {}
	int expectedSize() const { return secret.size(); }

	// Fields for AESEncryptionV1
	TextAndHeaderCipherKeys cipherKeys;
	Optional<BlobCipherEncryptHeader> cipherHeader;
	// Fields for XOREncryption_TestOnly
	StringRef secret;
	Optional<XOREncryptionKeyID> id;
};
typedef Standalone<EncryptionKeyRef> EncryptionKey;

// Interface used by pager to get encryption keys reading pages from disk
// and by the BTree to get encryption keys to use for new pages
class IEncryptionKeyProvider : public ReferenceCounted<IEncryptionKeyProvider> {
public:
	virtual ~IEncryptionKeyProvider() {}

	// Get an EncryptionKey with Secret Fields populated based on the given Page Fields.
	// It is up to the implementation which fields those are.
	// The output Page Fields must match the input Page Fields.
	virtual Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) = 0;

	// Get encryption key that should be used for a given user Key-Value range
	virtual Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) = 0;

	// Setting tenant prefix to tenant name map.
	virtual void setTenantPrefixIndex(Reference<TenantPrefixIndex> tenantPrefixIndex) {}

	virtual bool shouldEnableEncryption() const = 0;
};

// The null key provider is useful to simplify page decoding.
// It throws an error for any key info requested.
class NullKeyProvider : public IEncryptionKeyProvider {
public:
	virtual ~NullKeyProvider() {}
	bool shouldEnableEncryption() const override { return true; }
	Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) override { throw encryption_key_not_found(); }
	Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) override {
		throw encryption_key_not_found();
	}
};

// Key provider for dummy XOR encryption scheme
class XOREncryptionKeyProvider_TestOnly : public IEncryptionKeyProvider {
public:
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

	bool shouldEnableEncryption() const override { return true; }

	Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) override {
		if (!key.id.present()) {
			throw encryption_key_not_found();
		}
		EncryptionKey s = key;
		uint8_t secret = ~(uint8_t)key.id.get() ^ xorWith;
		s.secret = StringRef(s.arena(), &secret, 1);
		return s;
	}

	Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) override {
		EncryptionKeyRef k;
		k.id = end.empty() ? 0 : *(end.end() - 1);
		return getSecrets(k);
	}

	uint8_t xorWith;
};

// Key provider to provider cipher keys randomly from a pre-generated pool. Use for testing.
class RandomEncryptionKeyProvider : public IEncryptionKeyProvider {
public:
	RandomEncryptionKeyProvider() {
		for (unsigned i = 0; i < NUM_CIPHER; i++) {
			BlobCipherDetails cipherDetails;
			cipherDetails.encryptDomainId = i;
			cipherDetails.baseCipherId = deterministicRandom()->randomUInt64();
			cipherDetails.salt = deterministicRandom()->randomUInt64();
			cipherKeys[i] = generateCipherKey(cipherDetails);
		}
	}
	virtual ~RandomEncryptionKeyProvider() = default;

	bool shouldEnableEncryption() const override { return true; }

	Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) override {
		ASSERT(key.cipherHeader.present());
		EncryptionKey s = key;
		s.cipherKeys.cipherTextKey = cipherKeys[key.cipherHeader.get().cipherTextDetails.encryptDomainId];
		s.cipherKeys.cipherHeaderKey = cipherKeys[key.cipherHeader.get().cipherHeaderDetails.encryptDomainId];
		return s;
	}

	Future<EncryptionKey> getByRange(const KeyRef& /*begin*/, const KeyRef& /*end*/) override {
		EncryptionKey s;
		s.cipherKeys.cipherTextKey = getRandomCipherKey();
		s.cipherKeys.cipherHeaderKey = getRandomCipherKey();
		return s;
	}

private:
	Reference<BlobCipherKey> generateCipherKey(const BlobCipherDetails& cipherDetails) {
		static unsigned char SHA_KEY[] = "3ab9570b44b8315fdb261da6b1b6c13b";
		Arena arena;
		StringRef digest = computeAuthToken(reinterpret_cast<const unsigned char*>(&cipherDetails.baseCipherId),
		                                    sizeof(EncryptCipherBaseKeyId),
		                                    SHA_KEY,
		                                    AES_256_KEY_LENGTH,
		                                    arena);
		return makeReference<BlobCipherKey>(cipherDetails.encryptDomainId,
		                                    cipherDetails.baseCipherId,
		                                    digest.begin(),
		                                    AES_256_KEY_LENGTH,
		                                    cipherDetails.salt,
		                                    std::numeric_limits<int64_t>::max() /* refreshAt */,
		                                    std::numeric_limits<int64_t>::max() /* expireAt */);
	}

	Reference<BlobCipherKey> getRandomCipherKey() {
		return cipherKeys[deterministicRandom()->randomInt(0, NUM_CIPHER)];
	}

	static constexpr int NUM_CIPHER = 1000;
	Reference<BlobCipherKey> cipherKeys[NUM_CIPHER];
};

// Key provider which extract tenant id from range key prefixes, and fetch tenant specific encryption keys from
// EncryptKeyProxy.
class TenantAwareEncryptionKeyProvider : public IEncryptionKeyProvider {
public:
	TenantAwareEncryptionKeyProvider(Reference<AsyncVar<ServerDBInfo> const> db) : db(db) {}

	virtual ~TenantAwareEncryptionKeyProvider() = default;

	bool shouldEnableEncryption() const override {
		return isEncryptionOpSupported(EncryptOperationType::STORAGE_SERVER_ENCRYPTION, db->get().client);
	}

	ACTOR static Future<EncryptionKey> getSecrets(TenantAwareEncryptionKeyProvider* self, EncryptionKeyRef key) {
		if (!key.cipherHeader.present()) {
			TraceEvent("TenantAwareEncryptionKeyProvider_CipherHeaderMissing");
			throw encrypt_ops_error();
		}
		TextAndHeaderCipherKeys cipherKeys =
		    wait(getEncryptCipherKeys(self->db, key.cipherHeader.get(), BlobCipherMetrics::KV_REDWOOD));
		EncryptionKey s = key;
		s.cipherKeys = cipherKeys;
		return s;
	}

	Future<EncryptionKey> getSecrets(const EncryptionKeyRef& key) override { return getSecrets(this, key); }

	ACTOR static Future<EncryptionKey> getByRange(TenantAwareEncryptionKeyProvider* self, KeyRef begin, KeyRef end) {
		EncryptCipherDomainNameRef domainName;
		EncryptCipherDomainId domainId = self->getEncryptionDomainId(begin, end, &domainName);
		TextAndHeaderCipherKeys cipherKeys =
		    wait(getLatestEncryptCipherKeysForDomain(self->db, domainId, domainName, BlobCipherMetrics::KV_REDWOOD));
		EncryptionKey s;
		s.cipherKeys = cipherKeys;
		return s;
	}

	Future<EncryptionKey> getByRange(const KeyRef& begin, const KeyRef& end) override {
		return getByRange(this, begin, end);
	}

	void setTenantPrefixIndex(Reference<TenantPrefixIndex> tenantPrefixIndex) override {
		ASSERT(tenantPrefixIndex.isValid());
		this->tenantPrefixIndex = tenantPrefixIndex;
	}

private:
	EncryptCipherDomainId getEncryptionDomainId(const KeyRef& begin,
	                                            const KeyRef& end,
	                                            EncryptCipherDomainNameRef* domainName) {
		int64_t domainId = SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID;
		int64_t beginTenantId = getTenantId(begin, true /*inclusive*/);
		int64_t endTenantId = getTenantId(end, false /*inclusive*/);
		if (beginTenantId == endTenantId && beginTenantId != SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) {
			ASSERT(tenantPrefixIndex.isValid());
			Key tenantPrefix = TenantMapEntry::idToPrefix(beginTenantId);
			auto view = tenantPrefixIndex->atLatest();
			auto itr = view.find(tenantPrefix);
			if (itr != view.end()) {
				*domainName = *itr;
				domainId = beginTenantId;
			} else {
				// No tenant with the same tenant id. We could be in optional or disabled tenant mode.
			}
		}
		if (domainId == SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID) {
			*domainName = FDB_SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_NAME;
		}
		return domainId;
	}

	int64_t getTenantId(const KeyRef& key, bool inclusive) {
		// A valid tenant id is always a valid encrypt domain id.
		static_assert(INVALID_ENCRYPT_DOMAIN_ID == -1);

		if (key.size() && key >= systemKeys.begin) {
			return SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID;
		}

		if (key.size() < TENANT_PREFIX_SIZE) {
			// Encryption domain information not available, leverage 'default encryption domain'
			return FDB_DEFAULT_ENCRYPT_DOMAIN_ID;
		}

		StringRef prefix = key.substr(0, TENANT_PREFIX_SIZE);
		int64_t tenantId = TenantMapEntry::prefixToId(prefix, EnforceValidTenantId::False);
		if (tenantId == TenantInfo::INVALID_TENANT) {
			// Encryption domain information not available, leverage 'default encryption domain'
			return FDB_DEFAULT_ENCRYPT_DOMAIN_ID;
		}

		if (!inclusive && key.size() == TENANT_PREFIX_SIZE) {
			tenantId = tenantId - 1;
		}
		ASSERT(tenantId >= 0);
		return tenantId;
	}

	Reference<AsyncVar<ServerDBInfo> const> db;
	Reference<TenantPrefixIndex> tenantPrefixIndex;
};

#include "flow/unactorcompiler.h"
#endif