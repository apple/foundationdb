/*
 * BenchEncrypt.cpp
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

#include "benchmark/benchmark.h"

#include "flow/StreamCipher.h"
#include "flowbench/GlobalData.h"

static StreamCipher::IV getRandomIV() {
	StreamCipher::IV iv;
	generateRandomData(iv.data(), iv.size());
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
	while (state.KeepRunning()) {
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
	while (state.KeepRunning()) {
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
