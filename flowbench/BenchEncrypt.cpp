/*
 * BenchEncrypt.cpp
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

#include "benchmark/benchmark.h"

#include "fdbclient/BlobCipher.h"
#include "flow/EncryptUtils.h"
#include "flow/StreamCipher.h"
#include "flowbench/GlobalData.h"

static StreamCipher::IV getRandomIV() {
	StreamCipher::IV iv;
	deterministicRandom()->randomBytes(iv.data(), iv.size());
	return iv;
}

static inline Standalone<StringRef> encrypt(const StreamCipherKey* const key,
                                            const StreamCipher::IV& iv,
                                            unsigned char const* data,
                                            size_t len) {
	EncryptionStreamCipher encryptor(key, iv);
	Arena arena;
	auto encrypted = encryptor.encrypt(data, len, arena);
	return Standalone<StringRef>(encrypted, arena);
}

static void bench_encrypt(benchmark::State& state) {
	auto bytes = state.range(0);
	auto chunks = state.range(1);
	auto chunkSize = bytes / chunks;
	StreamCipherKey::initializeGlobalRandomTestKey();
	auto key = StreamCipherKey::getGlobalCipherKey();
	auto iv = getRandomIV();
	auto data = getKey(bytes);
	for (auto _ : state) {
		for (int chunk = 0; chunk < chunks; ++chunk) {
			benchmark::DoNotOptimize(encrypt(key, iv, data.begin() + chunk * chunkSize, chunkSize));
		}
	}
	state.SetBytesProcessed(bytes * static_cast<long>(state.iterations()));
}

static void bench_decrypt(benchmark::State& state) {
	auto bytes = state.range(0);
	auto chunks = state.range(1);
	auto chunkSize = bytes / chunks;
	StreamCipherKey::initializeGlobalRandomTestKey();
	auto key = StreamCipherKey::getGlobalCipherKey();
	auto iv = getRandomIV();
	auto data = getKey(bytes);
	auto encrypted = encrypt(key, iv, data.begin(), data.size());
	for (auto _ : state) {
		Arena arena;
		DecryptionStreamCipher decryptor(key, iv);
		for (int chunk = 0; chunk < chunks; ++chunk) {
			benchmark::DoNotOptimize(
			    Standalone<StringRef>(decryptor.decrypt(encrypted.begin() + chunk * chunkSize, chunkSize, arena)));
		}
	}
	state.SetBytesProcessed(bytes * static_cast<long>(state.iterations()));
}

BENCHMARK(bench_encrypt)->Ranges({ { 1 << 12, 1 << 20 }, { 1, 1 << 12 } });
BENCHMARK(bench_decrypt)->Ranges({ { 1 << 12, 1 << 20 }, { 1, 1 << 12 } });

// DEPRECATED -- Use EncryptionOps for benchmarking purposes.

// blob_chipher* benchmarks are following the encrypt and decrypt unittests from BlobCipher.cpp
// Construct a dummy External Key Manager representation and populate with some keys
class BaseCipher : public ReferenceCounted<BaseCipher>, NonCopyable {
public:
	EncryptCipherDomainId domainId;
	int len;
	EncryptCipherBaseKeyId keyId;
	EncryptCipherKeyCheckValue kcv;
	std::unique_ptr<uint8_t[]> key;
	int64_t refreshAt;
	int64_t expireAt;
	EncryptCipherRandomSalt generatedSalt;

	BaseCipher(const EncryptCipherDomainId& dId,
	           const EncryptCipherBaseKeyId& kId,
	           const int64_t rAt,
	           const int64_t eAt)
	  : domainId(dId), len(deterministicRandom()->randomInt(4, MAX_BASE_CIPHER_LEN + 1)), keyId(kId),
	    key(std::make_unique<uint8_t[]>(len)), refreshAt(rAt), expireAt(eAt) {
		deterministicRandom()->randomBytes(key.get(), len);
		kcv = Sha256KCV().computeKCV(key.get(), len);
	}
};

using BaseKeyMap = std::unordered_map<EncryptCipherBaseKeyId, Reference<BaseCipher>>;
using DomainKeyMap = std::unordered_map<EncryptCipherDomainId, BaseKeyMap>;

void static SetupEncryptCipher() {
	DomainKeyMap domainKeyMap;
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

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

	for (auto& domainItr : domainKeyMap) {
		for (auto& baseKeyItr : domainItr.second) {
			Reference<BaseCipher> baseCipher = baseKeyItr.second;

			cipherKeyCache->insertCipherKey(baseCipher->domainId,
			                                baseCipher->keyId,
			                                baseCipher->key.get(),
			                                baseCipher->len,
			                                baseCipher->kcv,
			                                baseCipher->refreshAt,
			                                baseCipher->expireAt);
			Reference<BlobCipherKey> fetchedKey = cipherKeyCache->getLatestCipherKey(baseCipher->domainId);
			baseCipher->generatedSalt = fetchedKey->getSalt();
		}
	}
}

static void blob_chipher_encrypt(benchmark::State& state) {
	const EncryptCipherDomainId minDomainId = 1;
	const int pageLen = state.range(0);
	const bool isInplace = state.range(1);

	SetupEncryptCipher();

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);
	uint8_t orgData[pageLen];
	deterministicRandom()->randomBytes(&orgData[0], pageLen);

	for (auto _ : state) {
		// create a new encryptor for each encrypt operation to simulate AESEncryptionEncoder.encode()
		EncryptBlobCipherAes265Ctr encryptor(cipherKey,
		                                     headerCipherKey,
		                                     iv,
		                                     AES_256_IV_LENGTH,
		                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
		                                     BlobCipherMetrics::TEST);

		BlobCipherEncryptHeader header;
		if (isInplace) {
			encryptor.encryptInplace(&orgData[0], pageLen, &header);
		} else {
			StringRef ciphertext = encryptor.encrypt(&orgData[0], pageLen, &header, arena)->toStringRef();
			memcpy(orgData, ciphertext.begin(), pageLen);
		}
	}
}

static void blob_chipher_decrypt(benchmark::State& state) {
	const EncryptCipherDomainId minDomainId = 1;
	const int pageLen = state.range(0);
	const bool isInplace = state.range(1);

	SetupEncryptCipher();

	Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
	Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(minDomainId);
	Reference<BlobCipherKey> headerCipherKey = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
	Arena arena;
	uint8_t iv[AES_256_IV_LENGTH];
	deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);
	uint8_t orgData[pageLen];
	deterministicRandom()->randomBytes(&orgData[0], pageLen);

	EncryptBlobCipherAes265Ctr encryptor(cipherKey,
	                                     headerCipherKey,
	                                     iv,
	                                     AES_256_IV_LENGTH,
	                                     EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE,
	                                     BlobCipherMetrics::TEST);

	BlobCipherEncryptHeaderRef headerRef;
	encryptor.encryptInplace(&orgData[0], pageLen, &headerRef);

	AesCtrNoAuth noAuth = std::get<AesCtrNoAuth>(headerRef.algoHeader);
	Reference<BlobCipherKey> tCipherKeyKey = cipherKeyCache->getCipherKey(noAuth.v1.cipherTextDetails.encryptDomainId,
	                                                                      noAuth.v1.cipherTextDetails.baseCipherId,
	                                                                      noAuth.v1.cipherTextDetails.salt);

	for (auto _ : state) {
		// create decryptor for every decrypt operation to simulate AESEncryptionEncoder.decode()
		DecryptBlobCipherAes256Ctr decryptor(
		    tCipherKeyKey, Reference<BlobCipherKey>(), &noAuth.v1.iv[0], BlobCipherMetrics::TEST);
		if (isInplace) {
			decryptor.decryptInplace(&orgData[0], pageLen, headerRef);
		} else {
			StringRef decrypted = decryptor.decrypt(&orgData[0], pageLen, headerRef, arena);
			memcpy(orgData, decrypted.begin(), pageLen);
		}
	}
}

static void blob_chipher_args(benchmark::internal::Benchmark* b) {
	for (int pageLen : { 8000, 16000 }) {
		for (bool isInplace : { false, true }) {
			b->Args({ pageLen, isInplace });
		}
	}
	b->ArgNames({ "pageLen", "isInplace" });
}

BENCHMARK(blob_chipher_encrypt)->Apply(blob_chipher_args);
BENCHMARK(blob_chipher_decrypt)->Apply(blob_chipher_args);
