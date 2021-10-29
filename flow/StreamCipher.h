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

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <string>
#include <unordered_set>
#include <vector>

// Wrapper class for openssl implementation of AES-128-GCM
// encryption/decryption
class StreamCipher final : NonCopyable {
	static std::unordered_set<EVP_CIPHER_CTX*> ctxs;
	EVP_CIPHER_CTX* ctx;

public:
	StreamCipher();
	~StreamCipher();
	EVP_CIPHER_CTX* getCtx();
	class Key : NonCopyable {
		std::array<unsigned char, 16> arr;
		static std::unique_ptr<Key> globalKey;
		struct ConstructorTag {};

	public:
		using RawKeyType = decltype(arr);
		Key(ConstructorTag) {}
		Key(Key&&);
		Key& operator=(Key&&);
		~Key();
		unsigned char const* data() const { return arr.data(); }
		static void initializeKey(RawKeyType&&);
		static void initializeRandomTestKey();
		static const Key& getKey();
		static void cleanup() noexcept;
	};
	static void cleanup() noexcept;
	using IV = std::array<unsigned char, 16>;
};

class EncryptionStreamCipher final : NonCopyable, public ReferenceCounted<EncryptionStreamCipher> {
	StreamCipher cipher;

public:
	EncryptionStreamCipher(const StreamCipher::Key& key, const StreamCipher::IV& iv);
	StringRef encrypt(unsigned char const* plaintext, int len, Arena&);
	StringRef finish(Arena&);
};

class DecryptionStreamCipher final : NonCopyable, public ReferenceCounted<DecryptionStreamCipher> {
	StreamCipher cipher;

public:
	DecryptionStreamCipher(const StreamCipher::Key& key, const StreamCipher::IV& iv);
	StringRef decrypt(unsigned char const* ciphertext, int len, Arena&);
	StringRef finish(Arena&);
};
