/*
 * StreamCipher.h
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

#pragma once

#if (!defined(TLS_DISABLED))
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
	static UID globalKeyId;
	static std::unique_ptr<StreamCipherKey> globalKey;
	static std::unordered_map<UID, StreamCipherKey*> cipherKeys;
	UID id;
	std::unique_ptr<uint8_t[]> arr;
	int keySize;

public:
	StreamCipherKey(int size);
	~StreamCipherKey();

	int size() const { return keySize; }
	uint8_t* data() const { return arr.get(); }
	void initializeKey(uint8_t* data, int len);
	void initializeRandomTestKey() { generateRandomData(arr.get(), keySize); }
	void reset() { memset(arr.get(), 0, keySize); }

	static bool isGlobalKeyPresent();
	static void allocGlobalCipherKey();
	static void initializeGlobalRandomTestKey();
	static StreamCipherKey const* getGlobalCipherKey();
	static void cleanup() noexcept;
};

class StreamCipher final : NonCopyable {
	UID id;
	static std::unordered_map<UID, EVP_CIPHER_CTX*> ctxs;
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
	EncryptionStreamCipher(const StreamCipherKey* key, const StreamCipher::IV& iv);
	StringRef encrypt(unsigned char const* plaintext, int len, Arena&);
	StringRef finish(Arena&);
};

class DecryptionStreamCipher final : NonCopyable, public ReferenceCounted<DecryptionStreamCipher> {
	StreamCipher cipher;

public:
	DecryptionStreamCipher(const StreamCipherKey* key, const StreamCipher::IV& iv);
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

void applyHmacKeyDerivationFunc(StreamCipherKey* cipherKey, HmacSha256StreamCipher* hmacGenerator, Arena& arena);

#endif // ENCRYPTION_ENABLED
