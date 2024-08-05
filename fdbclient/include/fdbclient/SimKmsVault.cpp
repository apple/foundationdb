/*
 * SimKmsVault.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/SimKmsVault.h"
#include "fdbclient/BlobCipher.h"
#include "fdbclient/BlobMetadataUtils.h"
#include "fdbclient/ClientKnobs.h"

#include "fdbclient/Knobs.h"
#include "flow/Arena.h"
#include "flow/EncryptUtils.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/network.h"

// The credentials may be allowed to change, but the storage locations and partitioning cannot change, even across
// restarts. Keep it as global static state in simulation.
static std::unordered_map<BlobMetadataDomainId, Standalone<BlobMetadataDetailsRef>> simBlobMetadataStore;

SimKmsVaultKeyCtx::SimKmsVaultKeyCtx(EncryptCipherBaseKeyId kId, const uint8_t* data, const int dataLen)
  : id(kId), keyLen(dataLen) {
	key = makeString(keyLen);
	memcpy(mutateString(key), data, keyLen);
	kcv = Sha256KCV().computeKCV((const uint8_t*)data, dataLen);
	if (DEBUG_SIM_KEY_CIPHER) {
		TraceEvent(SevDebug, "SimKmsVaultKeyCtxInit")
		    .detail("BaseCipherId", kId)
		    .detail("BaseCipherLen", dataLen)
		    .detail("KCV", kcv);
	}
}

int SimKmsVaultKeyCtx::getKeyLen(const EncryptCipherBaseKeyId id) {
	ASSERT_GT(id, INVALID_ENCRYPT_CIPHER_KEY_ID);

	int ret = AES_256_KEY_LENGTH;
	if ((id % 2) == 0) {
		ret += (id % (MAX_BASE_CIPHER_LEN - AES_256_KEY_LENGTH));
	}
	CODE_PROBE(ret == AES_256_KEY_LENGTH, "SimKmsVault BaseCipherKeyLen AES_256_KEY_LENGTH");
	CODE_PROBE(ret != AES_256_KEY_LENGTH, "SimKmsVault BaseCipherKeyLen variable length");

	return ret;
}

class SimKmsVaultCtx : NonCopyable, public ReferenceCounted<SimKmsVaultCtx> {
public:
	// Public visibility constructior ONLY to assist FlowSingleton instance creation.
	// API Note: Constructor is expected to be instantiated only in simulation mode.

	explicit SimKmsVaultCtx(bool ignored) {
		ASSERT(g_network->isSimulated());
		init();
	}

	static Reference<SimKmsVaultCtx> getInstance() {
		if (g_network->isSimulated()) {
			return FlowSingleton<SimKmsVaultCtx>::getInstance(
			    []() { return makeReference<SimKmsVaultCtx>(g_network->isSimulated()); });
		} else {
			static SimKmsVaultCtx instance;
			return Reference<SimKmsVaultCtx>::addRef(&instance);
		}
	}

	Reference<SimKmsVaultKeyCtx> getByBaseCipherId(const EncryptCipherBaseKeyId baseCipherId) {
		auto itr = keyvault.find(baseCipherId);
		if (itr == keyvault.end()) {
			return Reference<SimKmsVaultKeyCtx>();
		}
		return itr->second;
	}

	EncryptCipherBaseKeyId getBaseCipherIdFromDomainId(const EncryptCipherDomainId domainId) const {
		return 1 + abs(domainId) % maxEncryptionKeys;
	}

	uint32_t maxKeys() const { return maxEncryptionKeys; }

private:
	SimKmsVaultCtx() { init(); }

	void init() {
		maxEncryptionKeys = CLIENT_KNOBS->SIM_KMS_VAULT_MAX_KEYS;
		// Vault needs to ensure 'stable encryption key semantics' across process restarts, the encryption keys are
		// generated using following scheme:
		// a. HMAC_SHA algorithm is used to generate the key (digest)
		// b. HMAC_SHA uses an 'deterministic' seed (SHA_KEY) and 'data' buffer to generate a vault key
		// c. To generate variable length vault-key, a known 'char' is used for padding
		const unsigned char SHA_KEY[] = "0c39e7906db6d51ac0573d328ce1b6be";

		// Construct encryption keyStore.
		// Note the keys generated must be the same after restart.
		for (int i = 1; i <= maxEncryptionKeys; i++) {
			const int keyLen = SimKmsVaultKeyCtx::getKeyLen(i);
			uint8_t key[keyLen];
			uint8_t digest[AUTH_TOKEN_HMAC_SHA_SIZE];

			// TODO: Allow baseCipherKeyLen < AES_256_KEY_LENGTH
			ASSERT_EQ(AES_256_KEY_LENGTH, AUTH_TOKEN_HMAC_SHA_SIZE);
			computeAuthToken({ { reinterpret_cast<const uint8_t*>(&i), sizeof(i) } },
			                 SHA_KEY,
			                 AES_256_KEY_LENGTH,
			                 &digest[0],
			                 EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_HMAC_SHA,
			                 AUTH_TOKEN_HMAC_SHA_SIZE);
			memcpy(&key[0], &digest[0], std::min(keyLen, AUTH_TOKEN_HMAC_SHA_SIZE));
			// Simulate variable length 'baseCipher' returned by external KMS
			if (keyLen > AUTH_TOKEN_HMAC_SHA_SIZE) {
				// pad it with known value
				memset(&key[AUTH_TOKEN_HMAC_SHA_SIZE], 'b', (keyLen - AUTH_TOKEN_HMAC_SHA_SIZE));
			}
			keyvault[i] = makeReference<SimKmsVaultKeyCtx>(i, &key[0], keyLen);
		}
	}

	uint32_t maxEncryptionKeys;
	std::unordered_map<EncryptCipherBaseKeyId, Reference<SimKmsVaultKeyCtx>> keyvault;
};

namespace SimKmsVault {

Reference<SimKmsVaultKeyCtx> getByBaseCipherId(const EncryptCipherBaseKeyId baseCipherId) {
	Reference<SimKmsVaultCtx> ctx = SimKmsVaultCtx::getInstance();
	return ctx->getByBaseCipherId(baseCipherId);
}

Reference<SimKmsVaultKeyCtx> getByDomainId(const EncryptCipherDomainId domainId) {
	Reference<SimKmsVaultCtx> ctx = SimKmsVaultCtx::getInstance();
	const EncryptCipherBaseKeyId baseCipherId = ctx->getBaseCipherIdFromDomainId(domainId);
	return ctx->getByBaseCipherId(baseCipherId);
}

uint32_t maxSimKeys() {
	Reference<SimKmsVaultCtx> vaultCtx = SimKmsVaultCtx::getInstance();
	return vaultCtx->maxKeys();
}

Standalone<BlobMetadataDetailsRef> getBlobMetadata(const BlobMetadataDomainId domainId, const std::string& bgUrl) {
	auto it = simBlobMetadataStore.find(domainId);
	if (it == simBlobMetadataStore.end()) {
		// construct new blob metadata
		it = simBlobMetadataStore.insert({ domainId, createRandomTestBlobMetadata(bgUrl, domainId) }).first;
	} else if (now() >= it->second.expireAt) {
		// update random refresh and expire time
		it->second.refreshAt = now() + deterministicRandom()->random01() * 30;
		it->second.expireAt = it->second.refreshAt + deterministicRandom()->random01() * 10;
	}
	return it->second;
}

} // namespace SimKmsVault

// Only used to link unit tests
void forceLinkSimKmsVaultTests() {}

TEST_CASE("/simKmsVault") {
	Reference<SimKmsVaultCtx> vaultCtx = SimKmsVaultCtx::getInstance();
	ASSERT_GT(vaultCtx->maxKeys(), 0);
	ASSERT_EQ(vaultCtx->maxKeys(), CLIENT_KNOBS->SIM_KMS_VAULT_MAX_KEYS);

	// Test non-existing baseCiphers
	Reference<SimKmsVaultKeyCtx> keyCtx = SimKmsVault::getByBaseCipherId(vaultCtx->maxKeys() + 1);
	ASSERT(!keyCtx.isValid());

	const int nIterations = deterministicRandom()->randomInt(512, 786);
	for (int i = 0; i < nIterations; i++) {
		Reference<SimKmsVaultKeyCtx> keyCtx;
		if (deterministicRandom()->coinflip()) {
			const EncryptCipherBaseKeyId baseCipherId = deterministicRandom()->randomInt(1, vaultCtx->maxKeys() + 1);
			keyCtx = SimKmsVault::getByBaseCipherId(baseCipherId);
			ASSERT(keyCtx.isValid());
			ASSERT_EQ(keyCtx->id, baseCipherId);
		} else {
			const EncryptCipherDomainId domainId = deterministicRandom()->randomInt64(1, 100001);
			keyCtx = SimKmsVault::getByDomainId(domainId);
			ASSERT(keyCtx.isValid());
			ASSERT_EQ(keyCtx->id, vaultCtx->getBaseCipherIdFromDomainId(domainId));
		}
		ASSERT_GT(keyCtx->kcv, 0);
		ASSERT(!keyCtx->key.empty());
	}
	return Void();
}
