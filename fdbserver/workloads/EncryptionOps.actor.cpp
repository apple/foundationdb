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

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/NativeAPI.actor.h"
#include "flow/IRandom.h"
#include "flow/BlockCipher.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

#if ENCRYPTION_ENABLED

#include <chrono>
#include <cstring>
#include <memory>
#include <random>

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

struct EncryptionOpsWorkload : TestWorkload {
	int mode;
	int64_t numIterations;
	int pageSize;
	int maxBufSize;
	std::unique_ptr<uint8_t[]> buff;
	std::unique_ptr<uint8_t[]> validationBuff;

	BlockCipherIV iv;
	std::unique_ptr<uint8_t[]> parentCipher;
	Arena arena;
	std::unique_ptr<WorkloadMetrics> metrics;

	EncryptionOpsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		mode = getOption(options, LiteralStringRef("fixedSize"), 1);
		numIterations = getOption(options, LiteralStringRef("numIterations"), 10);
		pageSize = getOption(options, LiteralStringRef("pageSize"), 4096);
		maxBufSize = getOption(options, LiteralStringRef("maxBufSize"), 512 * 1024);
		buff = std::make_unique<uint8_t[]>(maxBufSize);
		validationBuff = std::make_unique<uint8_t[]>(maxBufSize);

		iv = getRandomIV();
		parentCipher = std::make_unique<uint8_t[]>(AES_256_KEY_LENGTH);
		generateRandomData(parentCipher.get(), AES_256_KEY_LENGTH);

		metrics = std::make_unique<WorkloadMetrics>();

		TraceEvent("EncryptionOpsWorkload").detail("Mode", getModeStr());
	}

	bool isFixedSizePayload() { return mode == 1; }

	BlockCipherIV getRandomIV() {
		generateRandomData(iv.data(), iv.size());
		return iv;
	}

	std::string getModeStr() const {
		if (mode == 1) {
			return "FixedSize";
		} else if (mode == 0) {
			return "VariableSize";
		}
		// no other mode supported
		throw internal_error();
	}

	StringRef doEncryption(Reference<BlockCipherKey> key, uint8_t* payload, int len, BlockCipherEncryptHeader* header) {
		EncryptBlockCipher encryptor(key, iv);

		auto start = std::chrono::high_resolution_clock::now();
		auto encrypted = encryptor.encrypt(buff.get(), len, header, arena);
		auto end = std::chrono::high_resolution_clock::now();

		// validate encrypted buffer size and contents (not matching with plaintext)
		ASSERT(encrypted.size() == len);
		std::copy(encrypted.begin(), encrypted.end(), validationBuff.get());
		ASSERT(memcmp(validationBuff.get(), buff.get(), len) != 0);
		ASSERT(header->flags.headerVersion == EncryptBlockCipher::ENCRYPT_HEADER_VERSION);

		metrics->updateEncryptionTime(std::chrono::duration<double, std::nano>(end - start).count());
		return encrypted;
	}

	void doDecryption(const StringRef& encrypted,
	                  int len,
	                  const BlockCipherEncryptHeader& header,
	                  uint8_t* originalPayload,
	                  uint8_t* validationBuff) {
		ASSERT(header.flags.headerVersion == EncryptBlockCipher::ENCRYPT_HEADER_VERSION);

		auto& cipherKeyCache = BlockCipherKeyCache::getInstance();
		Reference<BlockCipherKey> cipherKey = cipherKeyCache.getCipherKey(header);
		assert(cipherKey != nullptr);
		DecryptBlockCipher decryptor(cipherKey, iv);

		auto start = std::chrono::high_resolution_clock::now();
		Standalone<StringRef> decrypted = decryptor.decrypt(encrypted.begin(), len, header, arena);
		auto end = std::chrono::high_resolution_clock::now();

		// validate decrypted buffer size and contents (matching with original plaintext)
		ASSERT(decrypted.size() == len);
		std::copy(decrypted.begin(), decrypted.end(), validationBuff);
		ASSERT(memcmp(validationBuff, originalPayload, len) == 0);

		metrics->updateDecryptionTime(std::chrono::duration<double, std::nano>(end - start).count());
	}

	Future<Void> setup(Database const& ctx) override { return Void(); }

	std::string description() const override { return "EncryptionOps"; }

	Future<Void> start(Database const& cx) override {
		// use uniform distribution random data generator
		std::random_device rd; // random seed gen.
		std::mt19937 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
		std::uniform_int_distribution<> distrib(100, maxBufSize);

		BlockCipherDomainId domainId{ 1 };
		for (int i = 0; i < numIterations; i++, domainId++) {
			// Step-1: Encryption key derivation, caching the cipher for later use
			auto start = std::chrono::high_resolution_clock::now();
			auto& cipherKeyCache = BlockCipherKeyCache::getInstance();
			cipherKeyCache.insertCipherKey(domainId, domainId, parentCipher.get(), AES_256_KEY_LENGTH);
			Reference<BlockCipherKey> cipherKey = cipherKeyCache.getLatestCipherKey(domainId, domainId);
			auto end = std::chrono::high_resolution_clock::now();
			metrics->updateKeyDerivationTime(std::chrono::duration<double, std::nano>(end - start).count());

			int dataLen = isFixedSizePayload() ? pageSize : distrib(gen);

			// Encrypt the payload - generates BlockCipherEncryptHeader to assist decryption later
			BlockCipherEncryptHeader header;
			const auto& encrypted = doEncryption(cipherKey, buff.get(), dataLen, &header);

			// Decrypt the payload - parses the BlockCipherEncryptHeader, fetch corresponding cipherKey and decrypt
			doDecryption(encrypted, dataLen, header, buff.get(), validationBuff.get());

			metrics->updateBytes(dataLen);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override { metrics->recordMetrics(getModeStr(), numIterations); }
};

WorkloadFactory<EncryptionOpsWorkload> EncryptionOpsWorkloadFactory("EncryptionOps");

#endif // ENCRYPTION_ENABLED
