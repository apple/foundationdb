/*
 * StreamCipher.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#pragma once

#if (!defined(TLS_DISABLED) && !defined(_WIN32))
#define ENCRYPTION_ENABLED 1
#else
#define ENCRYPTION_ENABLED 0
#endif

#if ENCRYPTION_ENABLED

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <string>
#include <unordered_set>
#include <vector>

#define AES_256_KEY_LENGTH 32

// Wrapper class for openssl implementation of AES GCM
// encryption/decryption
class StreamCipherKey : NonCopyable {
	static std::unique_ptr<StreamCipherKey> globalKey;
	std::unique_ptr<uint8_t[]> arr;
	int keySize;

public:
	StreamCipherKey(int size) : arr(std::make_unique<uint8_t[]>(size)), keySize(size) { memset(arr.get(), 0, keySize); }
	~StreamCipherKey() { cleanup(); }
	int size() const { return keySize; }
	uint8_t* data() const { return arr.get(); }
	void initializeKey(uint8_t* data, int len) {
		memset(arr.get(), 0, keySize);
		int copyLen = std::min(keySize, len);
		memcpy(arr.get(), data, copyLen);
	}
	void initializeRandomTestKey() { generateRandomData(arr.get(), keySize); }
	void cleanup() noexcept { memset(arr.get(), 0, keySize); }
	static StreamCipherKey* getGlobalCipherKey();
	static void setGlobalCipherKey(uint8_t* const data, int len);
	static void globalKeyCleanup();
};

class StreamCipher final : NonCopyable {
	static std::unordered_set<EVP_CIPHER_CTX*> ctxs;
	EVP_CIPHER_CTX* ctx;
	HMAC_CTX* hmacCtx;
	std::unique_ptr<StreamCipherKey> cipherKey;

public:
	StreamCipher();
	StreamCipher(int KeySize);
	~StreamCipher();
	EVP_CIPHER_CTX* getCtx();
	HMAC_CTX* getHmacCtx();

	static void cleanup() noexcept;
	using IV = std::array<unsigned char, 16>;
};

class EncryptionStreamCipher final : NonCopyable, public ReferenceCounted<EncryptionStreamCipher> {
	StreamCipher cipher;

public:
	EncryptionStreamCipher(const StreamCipherKey* const key, const StreamCipher::IV& iv);
	StringRef encrypt(unsigned char const* plaintext, int len, Arena&);
	StringRef finish(Arena&);
};

class DecryptionStreamCipher final : NonCopyable, public ReferenceCounted<DecryptionStreamCipher> {
	StreamCipher cipher;

public:
	DecryptionStreamCipher(const StreamCipherKey* const key, const StreamCipher::IV& iv);
	StringRef decrypt(unsigned char const* ciphertext, int len, Arena&);
	StringRef finish(Arena&);
};

class HmacSha256StreamCipher final : NonCopyable, public ReferenceCounted<HmacSha256StreamCipher> {
	StreamCipher cipher;

public:
	HmacSha256StreamCipher();
	StringRef digest(unsigned char const* data, int len, Arena&);
	StringRef finish(Arena&);
};

void applyHmacKeyDerivationFunc(StreamCipherKey* const cipherKey,
                                HmacSha256StreamCipher* const hmacGenerator,
                                Arena& arena);

#endif // ENCRYPTION_ENABLED
