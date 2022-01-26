/*
 * EncryptionOps.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
#include "flow/StreamCipher.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.
#include "flow/genericactors.actor.g.h"
#include <chrono>
#include <cstring>
#include <memory>

#define MEGA_BYTES (1024 * 1024)
#define NANO_SECOND (1000 * 1000 * 1000)

struct EncryptionOpsWorkload : TestWorkload {
	int mode;
	int64_t numIterations;
	int pageSize;
	int maxBufSize;
	std::unique_ptr<uint8_t[]> buff;
	std::unique_ptr<uint8_t[]> validationBuff;

	StreamCipher::IV iv;
	std::unique_ptr<EncryptionStreamCipher> encryptor;
	std::unique_ptr<DecryptionStreamCipher> decryptor;
	Arena arena;

	double totalEncryptTimeNS;
	double totalDecryptTimeNS;
	int64_t totalBytes;

	EncryptionOpsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		mode = getOption(options, LiteralStringRef("fixedSize"), 1);
		numIterations = getOption(options, LiteralStringRef("numIterations"), 10);
		pageSize = getOption(options, LiteralStringRef("pageSize"), 4096);
		maxBufSize = getOption(options, LiteralStringRef("maxBufSize"), 512 * 1024);
		buff = std::make_unique<uint8_t[]>(maxBufSize);
		validationBuff = std::make_unique<uint8_t[]>(maxBufSize);

		iv = getRandomIV();
		StreamCipher::Key::initializeRandomTestKey();
		const auto& key = StreamCipher::Key::getKey();
		encryptor = std::make_unique<EncryptionStreamCipher>(key, iv);
		decryptor = std::make_unique<DecryptionStreamCipher>(key, iv);

		totalDecryptTimeNS = totalEncryptTimeNS = totalBytes = 0;

		TraceEvent("EncryptionOpsWorkload").detail("Mode", getModeStr());
	}

	bool isFixedSizePayload() { return mode == 1; }

	StreamCipher::IV getRandomIV() {
		generateRandomData(iv.data(), iv.size());
		return iv;
	}

	std::string getModeStr() {
		if (mode == 1) {
			return "FixedSize";
		} else if (mode == 0) {
			return "VariableSize";
		}
		// no other mode supported
		throw internal_error();
	}

	double computeThroughputMBPS(double timeNS, double bytes) {
		// convert bytes -> MBs & nano-seonds -> seconds
		return (bytes * NANO_SECOND) / (timeNS * MEGA_BYTES);
	}

	Future<Void> setup(Database const& ctx) override { return Void(); }

	std::string description() const override { return "EncryptionOps"; }

	Future<Void> start(Database const& cx) override {
		for (int i = 0; i < numIterations; i++) {
			int dataLen = isFixedSizePayload() ? pageSize : deterministicRandom()->randomInt(100, maxBufSize);
			generateRandomData(buff.get(), dataLen);

			// encrypt the payload
			auto start = std::chrono::high_resolution_clock::now();
			auto encrypted = encryptor->encrypt(buff.get(), dataLen, arena);
			auto end = std::chrono::high_resolution_clock::now();
			totalEncryptTimeNS += std::chrono::duration<double, std::nano>(end - start).count();
			// validate encrypted buffer size and contents (not matching with plaintext)
			ASSERT(encrypted.size() == dataLen);
			std::copy(encrypted.begin(), encrypted.end(), validationBuff.get());
			ASSERT(memcmp(validationBuff.get(), buff.get(), dataLen) != 0);

			// decrypt the payload
			start = std::chrono::high_resolution_clock::now();
			Standalone<StringRef> decrypted = decryptor->decrypt(encrypted.begin(), dataLen, arena);
			end = std::chrono::high_resolution_clock::now();
			totalDecryptTimeNS += std::chrono::duration<double, std::nano>(end - start).count();
			// validate decrypted buffer size and contents (matching with original plaintext)
			ASSERT(decrypted.size() == dataLen);
			std::copy(decrypted.begin(), decrypted.end(), validationBuff.get());
			ASSERT(memcmp(validationBuff.get(), buff.get(), dataLen) == 0);

			totalBytes += dataLen;
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		TraceEvent("EncryptionOpsWorkload")
		    .detail("Mode", getModeStr())
		    .detail("TimeEncryptTimeMS", totalEncryptTimeNS / 1000)
		    .detail("TimeDecryptTimeMS", totalDecryptTimeNS / 1000)
		    .detail("EncryptMBPS", computeThroughputMBPS(totalEncryptTimeNS, totalBytes))
		    .detail("DecryptMBPS", computeThroughputMBPS(totalDecryptTimeNS, totalBytes))
		    .detail("TotalBytes", totalBytes)
		    .detail("AvgCommitSize", totalBytes / numIterations);
		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override { // Not implemented }
	}
};

WorkloadFactory<EncryptionOpsWorkload> EncryptionOpsWorkloadFactory("EncryptionOps");
