/*
 * EncryptionOps.actor.cpp
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
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/EncryptUtils.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/flow.h"
#include "flow/ITrace.h"
#include "flow/Trace.h"

#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <random>

#include "flow/actorcompiler.h" // This must be the last #include.

#define MEGA_BYTES (1024 * 1024)
#define NANO_SECOND (1000 * 1000 * 1000)

struct WorkloadMetrics {
	double totalEncryptTimeNS;
	double totalDecryptTimeNS;
	double totalKeyDerivationTimeNS;
	int64_t totalBytes;

	void reset() {
		totalEncryptTimeNS = 0;
		totalDecryptTimeNS = 0;
		totalKeyDerivationTimeNS = 0;
		totalBytes = 0;
	}

	WorkloadMetrics() { reset(); }

	double computeEncryptThroughputMBPS() {
		// convert bytes -> MBs & nano-seonds -> seconds
		return (totalBytes * NANO_SECOND) / (totalEncryptTimeNS * MEGA_BYTES);
	}

	double computeDecryptThroughputMBPS() {
		// convert bytes -> MBs & nano-seonds -> seconds
		return (totalBytes * NANO_SECOND) / (totalDecryptTimeNS * MEGA_BYTES);
	}

	void updateKeyDerivationTime(double val) { totalKeyDerivationTimeNS += val; }
	void updateEncryptionTime(double val) { totalEncryptTimeNS += val; }
	void updateDecryptionTime(double val) { totalDecryptTimeNS += val; }
	void updateBytes(int64_t val) { totalBytes += val; }

	void recordMetrics(const std::string& mode, const int numIterations) {
		TraceEvent("EncryptionOpsWorkload")
		    .detail("Mode", mode)
		    .detail("EncryptTimeMS", totalEncryptTimeNS / 1000)
		    .detail("DecryptTimeMS", totalDecryptTimeNS / 1000)
		    .detail("EncryptMBPS", computeEncryptThroughputMBPS())
		    .detail("DecryptMBPS", computeDecryptThroughputMBPS())
		    .detail("KeyDerivationTimeMS", totalKeyDerivationTimeNS / 1000)
		    .detail("TotalBytes", totalBytes)
		    .detail("AvgCommitSize", totalBytes / numIterations);
	}
};

// Workload generator for encryption/decryption operations.
// 1. For every client run, it generate unique random encryptionDomainId range and simulate encryption of
//    either fixed size or variable size payload.
// 2. For each encryption run, it would interact with BlobCipherKeyCache to fetch the desired encryption key,
//    which then is used for encrypting the plaintext payload.
// 3. Encryption operation generates 'encryption header', it is leveraged to decrypt the ciphertext obtained from
//    step#2 (simulate real-world scenario)
//
// Correctness validations:
// -----------------------
// Correctness invariants are validated at various steps:
// 1. Encryption key correctness: as part of performing decryption, BlobCipherKeyCache lookup is done to procure
//    desired encrytion key based on: {encryptionDomainId, baseCipherId}; the obtained key is validated against
//    the encryption key used for encrypting the data.
// 2. After encryption, generated 'encryption header' fields are validated, encrypted buffer size and contents are
//    validated.
// 3. After decryption, the obtained deciphertext is validated against the orginal plaintext payload.
//
// Performance metrics:
// -------------------
// The workload generator profiles below operations across the iterations and logs the details at the end, they are:
// 1. Time spent in encryption key fetch (and derivation) operations.
// 2. Time spent encrypting the buffer (doesn't incude key lookup time); also records the throughput in MB/sec.
// 3. Time spent decrypting the buffer (doesn't incude key lookup time); also records the throughput in MB/sec.

struct EncryptionOpsWorkload : TestWorkload {
	static constexpr auto NAME = "EncryptionOps";
	int mode;
	int64_t numIterations;
	int pageSize;
	int maxBufSize;
	std::unique_ptr<uint8_t[]> buff;
	int enableTTLTest;

	Arena arena;
	std::unique_ptr<WorkloadMetrics> metrics;

	EncryptCipherDomainId minDomainId;
	EncryptCipherDomainId maxDomainId;
	EncryptCipherBaseKeyId minBaseCipherId;
	EncryptCipherBaseKeyId headerBaseCipherId;
	EncryptCipherRandomSalt headerRandomSalt;

	EncryptionOpsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), enableTTLTest(false) {
		mode = getOption(options, "fixedSize"_sr, 1);
		numIterations = getOption(options, "numIterations"_sr, 10);
		pageSize = getOption(options, "pageSize"_sr, 4096);
		maxBufSize = getOption(options, "maxBufSize"_sr, 512 * 1024);
		buff = std::make_unique<uint8_t[]>(maxBufSize);

		// assign unique encryptionDomainId range per workload clients
		minDomainId = wcx.clientId * 100 + mode * 30 + 1;
		maxDomainId = deterministicRandom()->randomInt(minDomainId, minDomainId + 10) + 5;
		minBaseCipherId = 100;
		headerBaseCipherId = wcx.clientId * 100 + 1;

		metrics = std::make_unique<WorkloadMetrics>();

		if (wcx.clientId == 0 && mode == 1) {
			enableTTLTest = true;
		}

		TraceEvent("EncryptionOpsWorkload")
		    .detail("Mode", getModeStr())
		    .detail("MinDomainId", minDomainId)
		    .detail("MaxDomainId", maxDomainId)
		    .detail("EnableTTL", enableTTLTest);
	}

	~EncryptionOpsWorkload() { TraceEvent("EncryptionOpsWorkloadDone").log(); }

	bool isFixedSizePayload() const { return mode == 1; }

	std::string getModeStr() const {
		if (mode == 1) {
			return "FixedSize";
		} else if (mode == 0) {
			return "VariableSize";
		}
		// no other mode supported
		throw internal_error();
	}

	static void generateRandomBaseCipher(const int maxLen, uint8_t* buff, int* retLen) {
		memset(buff, 0, maxLen);
		*retLen = deterministicRandom()->randomInt(maxLen / 2, maxLen);
		deterministicRandom()->randomBytes(buff, *retLen);
	}

	void setupCipherEssentials() {
		Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

		TraceEvent("SetupCipherEssentialsStart").detail("MinDomainId", minDomainId).detail("MaxDomainId", maxDomainId);

		uint8_t buff[AES_256_KEY_LENGTH];
		std::vector<Reference<BlobCipherKey>> cipherKeys;
		int cipherLen = 0;
		for (EncryptCipherDomainId id = minDomainId; id <= maxDomainId; id++) {
			generateRandomBaseCipher(AES_256_KEY_LENGTH, &buff[0], &cipherLen);
			cipherKeyCache->insertCipherKey(id,
			                                minBaseCipherId,
			                                buff,
			                                cipherLen,
			                                std::numeric_limits<int64_t>::max(),
			                                std::numeric_limits<int64_t>::max());

			ASSERT(cipherLen > 0 && cipherLen <= AES_256_KEY_LENGTH);

			cipherKeys = cipherKeyCache->getAllCiphers(id);
			ASSERT_EQ(cipherKeys.size(), 1);
		}

		// insert the Encrypt Header cipherKey; record cipherDetails as getLatestCipher() may not work with multiple
		// test clients
		generateRandomBaseCipher(AES_256_KEY_LENGTH, &buff[0], &cipherLen);
		cipherKeyCache->insertCipherKey(ENCRYPT_HEADER_DOMAIN_ID,
		                                headerBaseCipherId,
		                                buff,
		                                cipherLen,
		                                std::numeric_limits<int64_t>::max(),
		                                std::numeric_limits<int64_t>::max());
		Reference<BlobCipherKey> latestCipher = cipherKeyCache->getLatestCipherKey(ENCRYPT_HEADER_DOMAIN_ID);
		ASSERT_EQ(latestCipher->getBaseCipherId(), headerBaseCipherId);
		ASSERT_EQ(memcmp(latestCipher->rawBaseCipher(), buff, cipherLen), 0);
		headerRandomSalt = latestCipher->getSalt();

		TraceEvent("SetupCipherEssentialsDone")
		    .detail("MinDomainId", minDomainId)
		    .detail("MaxDomainId", maxDomainId)
		    .detail("HeaderBaseCipherId", headerBaseCipherId)
		    .detail("HeaderRandomSalt", headerRandomSalt);
	}

	void resetCipherEssentials() {
		TraceEvent("ResetCipherEssentialsStart").detail("Min", minDomainId).detail("Max", maxDomainId);

		Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
		for (EncryptCipherDomainId id = minDomainId; id <= maxDomainId; id++) {
			cipherKeyCache->resetEncryptDomainId(id);
		}

		cipherKeyCache->resetEncryptDomainId(FDB_DEFAULT_ENCRYPT_DOMAIN_ID);
		cipherKeyCache->resetEncryptDomainId(SYSTEM_KEYSPACE_ENCRYPT_DOMAIN_ID);
		cipherKeyCache->resetEncryptDomainId(ENCRYPT_HEADER_DOMAIN_ID);

		TraceEvent("ResetCipherEssentialsDone");
	}

	void updateLatestBaseCipher(const EncryptCipherDomainId encryptDomainId,
	                            uint8_t* baseCipher,
	                            int* baseCipherLen,
	                            EncryptCipherBaseKeyId* nextBaseCipherId) {
		Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
		Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(encryptDomainId);
		*nextBaseCipherId = cipherKey->getBaseCipherId() + 1;

		generateRandomBaseCipher(AES_256_KEY_LENGTH, baseCipher, baseCipherLen);

		ASSERT(*baseCipherLen > 0 && *baseCipherLen <= AES_256_KEY_LENGTH);
		TraceEvent("UpdateBaseCipher").detail("DomainId", encryptDomainId).detail("BaseCipherId", *nextBaseCipherId);
	}

	Reference<BlobCipherKey> getEncryptionKey(const EncryptCipherDomainId& domainId,
	                                          const EncryptCipherBaseKeyId& baseCipherId,
	                                          const EncryptCipherRandomSalt& salt) {
		const bool simCacheMiss = deterministicRandom()->randomInt(1, 100) < 15;

		Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();
		Reference<BlobCipherKey> cipherKey = cipherKeyCache->getCipherKey(domainId, baseCipherId, salt);

		if (simCacheMiss) {
			TraceEvent("SimKeyCacheMiss").detail("EncryptDomainId", domainId).detail("BaseCipherId", baseCipherId);
			// simulate KeyCache miss that may happen during decryption; insert a CipherKey with known 'salt'
			cipherKeyCache->insertCipherKey(domainId,
			                                baseCipherId,
			                                cipherKey->rawBaseCipher(),
			                                cipherKey->getBaseCipherLen(),
			                                cipherKey->getSalt(),
			                                std::numeric_limits<int64_t>::max(),
			                                std::numeric_limits<int64_t>::max());
			// Ensure the update was a NOP
			Reference<BlobCipherKey> cKey = cipherKeyCache->getCipherKey(domainId, baseCipherId, salt);
			ASSERT(cKey->isEqual(cipherKey));
		}
		return cipherKey;
	}

	Reference<EncryptBuf> doEncryption(Reference<BlobCipherKey> textCipherKey,
	                                   Reference<BlobCipherKey> headerCipherKey,
	                                   uint8_t* payload,
	                                   int len,
	                                   const EncryptAuthTokenMode authMode,
	                                   const EncryptAuthTokenAlgo authAlgo,
	                                   BlobCipherEncryptHeader* header) {
		uint8_t iv[AES_256_IV_LENGTH];
		deterministicRandom()->randomBytes(&iv[0], AES_256_IV_LENGTH);
		EncryptBlobCipherAes265Ctr encryptor(
		    textCipherKey, headerCipherKey, &iv[0], AES_256_IV_LENGTH, authMode, authAlgo, BlobCipherMetrics::TEST);

		auto start = std::chrono::high_resolution_clock::now();
		Reference<EncryptBuf> encrypted = encryptor.encrypt(payload, len, header, arena);
		auto end = std::chrono::high_resolution_clock::now();

		// validate encrypted buffer size and contents (not matching with plaintext)
		ASSERT_EQ(encrypted->getLogicalSize(), len);
		ASSERT_NE(memcmp(encrypted->begin(), payload, len), 0);
		ASSERT_EQ(header->flags.headerVersion, EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION);

		metrics->updateEncryptionTime(std::chrono::duration<double, std::nano>(end - start).count());
		return encrypted;
	}

	void doDecryption(Reference<EncryptBuf> encrypted,
	                  int len,
	                  const BlobCipherEncryptHeader& header,
	                  uint8_t* originalPayload,
	                  Reference<BlobCipherKey> orgCipherKey) {
		ASSERT_EQ(header.flags.headerVersion, EncryptBlobCipherAes265Ctr::ENCRYPT_HEADER_VERSION);
		ASSERT_EQ(header.flags.encryptMode, ENCRYPT_CIPHER_MODE_AES_256_CTR);

		Reference<BlobCipherKey> cipherKey = getEncryptionKey(header.cipherTextDetails.encryptDomainId,
		                                                      header.cipherTextDetails.baseCipherId,
		                                                      header.cipherTextDetails.salt);
		Reference<BlobCipherKey> headerCipherKey = getEncryptionKey(header.cipherHeaderDetails.encryptDomainId,
		                                                            header.cipherHeaderDetails.baseCipherId,
		                                                            header.cipherHeaderDetails.salt);
		ASSERT(cipherKey.isValid());
		ASSERT(cipherKey->isEqual(orgCipherKey));
		ASSERT(headerCipherKey.isValid() ||
		       header.flags.authTokenMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE);

		DecryptBlobCipherAes256Ctr decryptor(cipherKey, headerCipherKey, header.iv, BlobCipherMetrics::TEST);

		auto start = std::chrono::high_resolution_clock::now();
		Reference<EncryptBuf> decrypted = decryptor.decrypt(encrypted->begin(), len, header, arena);
		auto end = std::chrono::high_resolution_clock::now();

		// validate decrypted buffer size and contents (matching with original plaintext)
		ASSERT_EQ(decrypted->getLogicalSize(), len);
		ASSERT_EQ(memcmp(decrypted->begin(), originalPayload, len), 0);

		metrics->updateDecryptionTime(std::chrono::duration<double, std::nano>(end - start).count());
	}

	void testBlobCipherKeyCacheOps() {
		uint8_t baseCipher[AES_256_KEY_LENGTH];
		int baseCipherLen = 0;
		EncryptCipherBaseKeyId nextBaseCipherId;

		// Setup encryptDomainIds and corresponding baseCipher details
		setupCipherEssentials();

		for (int i = 0; i < numIterations; i++) {
			bool updateBaseCipher = deterministicRandom()->randomInt(1, 100) < 5;

			// Step-1: Encryption key derivation, caching the cipher for later use
			Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

			// randomly select a domainId
			const EncryptCipherDomainId encryptDomainId = deterministicRandom()->randomInt(minDomainId, maxDomainId);
			ASSERT(encryptDomainId >= minDomainId && encryptDomainId <= maxDomainId);

			if (updateBaseCipher) {
				// simulate baseCipherId getting refreshed/updated
				updateLatestBaseCipher(encryptDomainId, &baseCipher[0], &baseCipherLen, &nextBaseCipherId);
				cipherKeyCache->insertCipherKey(encryptDomainId,
				                                nextBaseCipherId,
				                                &baseCipher[0],
				                                baseCipherLen,
				                                std::numeric_limits<int64_t>::max(),
				                                std::numeric_limits<int64_t>::max());
			}

			auto start = std::chrono::high_resolution_clock::now();
			Reference<BlobCipherKey> cipherKey = cipherKeyCache->getLatestCipherKey(encryptDomainId);
			// Each client working with their own version of encryptHeaderCipherKey, avoid using getLatest()
			Reference<BlobCipherKey> headerCipherKey =
			    cipherKeyCache->getCipherKey(ENCRYPT_HEADER_DOMAIN_ID, headerBaseCipherId, headerRandomSalt);
			auto end = std::chrono::high_resolution_clock::now();
			metrics->updateKeyDerivationTime(std::chrono::duration<double, std::nano>(end - start).count());

			// Validate sanity of "getLatestCipher", especially when baseCipher gets updated
			if (updateBaseCipher) {
				ASSERT_EQ(cipherKey->getBaseCipherId(), nextBaseCipherId);
				ASSERT_EQ(cipherKey->getBaseCipherLen(), baseCipherLen);
				ASSERT_EQ(memcmp(cipherKey->rawBaseCipher(), baseCipher, baseCipherLen), 0);
			}

			int dataLen = isFixedSizePayload() ? pageSize : deterministicRandom()->randomInt(100, maxBufSize);
			deterministicRandom()->randomBytes(buff.get(), dataLen);

			// Encrypt the payload - generates BlobCipherEncryptHeader to assist decryption later
			BlobCipherEncryptHeader header;
			const EncryptAuthTokenMode authMode = getRandomAuthTokenMode();
			const EncryptAuthTokenAlgo authAlgo = authMode == EncryptAuthTokenMode::ENCRYPT_HEADER_AUTH_TOKEN_MODE_NONE
			                                          ? EncryptAuthTokenAlgo::ENCRYPT_HEADER_AUTH_TOKEN_ALGO_NONE
			                                          : getRandomAuthTokenAlgo();

			try {
				Reference<EncryptBuf> encrypted =
				    doEncryption(cipherKey, headerCipherKey, buff.get(), dataLen, authMode, authAlgo, &header);

				// Decrypt the payload - parses the BlobCipherEncryptHeader, fetch corresponding cipherKey and
				// decrypt
				doDecryption(encrypted, dataLen, header, buff.get(), cipherKey);
			} catch (Error& e) {
				TraceEvent("Failed")
				    .detail("DomainId", encryptDomainId)
				    .detail("BaseCipherId", cipherKey->getBaseCipherId())
				    .detail("AuthTokenMode", authMode)
				    .detail("AuthTokenAlgo", authAlgo);
				throw;
			}

			metrics->updateBytes(dataLen);
		}

		// Cleanup cipherKeys
		resetCipherEssentials();
	}

	static void compareCipherDetails(Reference<BlobCipherKey> cipherKey,
	                                 const EncryptCipherDomainId domId,
	                                 const EncryptCipherBaseKeyId baseCipherId,
	                                 const uint8_t* baseCipher,
	                                 const int baseCipherLen,
	                                 const int64_t refreshAt,
	                                 const int64_t expAt) {
		ASSERT(cipherKey.isValid());
		ASSERT_EQ(cipherKey->getDomainId(), domId);
		ASSERT_EQ(cipherKey->getBaseCipherId(), baseCipherId);
		ASSERT_EQ(memcmp(cipherKey->rawBaseCipher(), baseCipher, baseCipherLen), 0);
		ASSERT_EQ(cipherKey->getRefreshAtTS(), refreshAt);
		ASSERT_EQ(cipherKey->getExpireAtTS(), expAt);
	}

	ACTOR Future<Void> testBlobCipherKeyCacheTTL(EncryptionOpsWorkload* self) {
		state Reference<BlobCipherKeyCache> cipherKeyCache = BlobCipherKeyCache::getInstance();

		state EncryptCipherDomainId domId = deterministicRandom()->randomInt(120000, 150000);
		state EncryptCipherBaseKeyId baseCipherId = deterministicRandom()->randomInt(786, 1024);
		state std::unique_ptr<uint8_t[]> baseCipher = std::make_unique<uint8_t[]>(AES_256_KEY_LENGTH);
		state Reference<BlobCipherKey> cipherKey;
		state EncryptCipherRandomSalt salt;
		state int64_t refreshAt;
		state int64_t expAt;

		TraceEvent("TestBlobCipherCacheTTLStart").detail("DomId", domId);

		deterministicRandom()->randomBytes(baseCipher.get(), AES_256_KEY_LENGTH);

		// Validate 'non-revocable' cipher with no expiration
		refreshAt = std::numeric_limits<int64_t>::max();
		expAt = std::numeric_limits<int64_t>::max();
		cipherKeyCache->insertCipherKey(domId, baseCipherId, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);
		cipherKey = cipherKeyCache->getLatestCipherKey(domId);
		compareCipherDetails(cipherKey, domId, baseCipherId, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);

		TraceEvent("TestBlobCipherCacheTTLNonRevocableNoExpiry").detail("DomId", domId);

		// Validate 'non-revocable' cipher with expiration
		state EncryptCipherBaseKeyId baseCipherId_1 = baseCipherId + 1;
		refreshAt = now() + 5;
		cipherKeyCache->insertCipherKey(domId, baseCipherId_1, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);
		cipherKey = cipherKeyCache->getLatestCipherKey(domId);
		ASSERT(cipherKey.isValid());
		compareCipherDetails(cipherKey, domId, baseCipherId_1, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);
		salt = cipherKey->getSalt();
		wait(delayUntil(refreshAt));
		// Ensure that latest cipherKey needs refresh, however, cipher lookup works (non-revocable)
		cipherKey = cipherKeyCache->getLatestCipherKey(domId);
		ASSERT(!cipherKey.isValid());
		cipherKey = cipherKeyCache->getCipherKey(domId, baseCipherId_1, salt);
		ASSERT(cipherKey.isValid());
		compareCipherDetails(cipherKey, domId, baseCipherId_1, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);

		TraceEvent("TestBlobCipherCacheTTLNonRevocableWithExpiry").detail("DomId", domId);

		// Validate 'revocable' cipher with expiration
		state EncryptCipherBaseKeyId baseCipherId_2 = baseCipherId + 2;
		refreshAt = now() + 5;
		expAt = refreshAt + 5;
		cipherKeyCache->insertCipherKey(domId, baseCipherId_2, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);
		cipherKey = cipherKeyCache->getLatestCipherKey(domId);
		ASSERT(cipherKey.isValid());
		compareCipherDetails(cipherKey, domId, baseCipherId_2, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);
		salt = cipherKey->getSalt();
		wait(delayUntil(refreshAt));
		// Ensure that latest cipherKey needs refresh, however, cipher lookup works (non-revocable)
		cipherKey = cipherKeyCache->getLatestCipherKey(domId);
		ASSERT(!cipherKey.isValid());
		cipherKey = cipherKeyCache->getCipherKey(domId, baseCipherId_2, salt);
		ASSERT(cipherKey.isValid());
		compareCipherDetails(cipherKey, domId, baseCipherId_2, baseCipher.get(), AES_256_KEY_LENGTH, refreshAt, expAt);
		wait(delayUntil(expAt));
		// Ensure that cipherKey lookup doesn't work after expiry
		cipherKey = cipherKeyCache->getLatestCipherKey(domId);
		ASSERT(!cipherKey.isValid());
		cipherKey = cipherKeyCache->getCipherKey(domId, baseCipherId_2, salt);
		ASSERT(!cipherKey.isValid());

		TraceEvent("TestBlobCipherCacheTTLEnd").detail("DomId", domId);
		return Void();
	}

	Future<Void> setup(Database const& ctx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	ACTOR Future<Void> _start(Database cx, EncryptionOpsWorkload* self) {
		self->testBlobCipherKeyCacheOps();
		if (self->enableTTLTest) {
			wait(self->testBlobCipherKeyCacheTTL(self));
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override { metrics->recordMetrics(getModeStr(), numIterations); }
};

WorkloadFactory<EncryptionOpsWorkload> EncryptionOpsWorkloadFactory;
