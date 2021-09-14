/*
 * StreamCipher.actor.cpp
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

#include "flow/StreamCipher.h"
#include "flow/UnitTest.h"

std::unordered_set<EVP_CIPHER_CTX*> StreamCipher::ctxs;
std::unique_ptr<StreamCipher::Key> StreamCipher::Key::globalKey;

StreamCipher::StreamCipher() : ctx(EVP_CIPHER_CTX_new()) {
	ctxs.insert(ctx);
}

StreamCipher::~StreamCipher() {
	EVP_CIPHER_CTX_free(ctx);
	ctxs.erase(ctx);
}

EVP_CIPHER_CTX* StreamCipher::getCtx() {
	return ctx;
}

void StreamCipher::cleanup() noexcept {
	Key::cleanup();
	for (auto ctx : ctxs) {
		EVP_CIPHER_CTX_free(ctx);
	}
}

void StreamCipher::Key::initializeKey(RawKeyType&& arr) {
	if (globalKey) {
		ASSERT(globalKey->arr == arr);
	}
	globalKey = std::make_unique<Key>(ConstructorTag{});
	globalKey->arr = std::move(arr);
	memset(arr.data(), 0, arr.size());
}

void StreamCipher::Key::initializeRandomTestKey() {
	ASSERT(g_network->isSimulated());
	if (globalKey)
		return;
	globalKey = std::make_unique<Key>(ConstructorTag{});
	generateRandomData(globalKey->arr.data(), globalKey->arr.size());
}

const StreamCipher::Key& StreamCipher::Key::getKey() {
	ASSERT(globalKey);
	return *globalKey;
}

StreamCipher::Key::Key(Key&& rhs) : arr(std::move(rhs.arr)) {
	memset(arr.data(), 0, arr.size());
}

StreamCipher::Key& StreamCipher::Key::operator=(Key&& rhs) {
	arr = std::move(rhs.arr);
	memset(arr.data(), 0, arr.size());
	return *this;
}

StreamCipher::Key::~Key() {
	memset(arr.data(), 0, arr.size());
}

void StreamCipher::Key::cleanup() noexcept {
	globalKey.reset();
}

EncryptionStreamCipher::EncryptionStreamCipher(const StreamCipher::Key& key, const StreamCipher::IV& iv) {
	EVP_EncryptInit_ex(cipher.getCtx(), EVP_aes_128_gcm(), nullptr, nullptr, nullptr);
	EVP_CIPHER_CTX_ctrl(cipher.getCtx(), EVP_CTRL_AEAD_SET_IVLEN, iv.size(), nullptr);
	EVP_EncryptInit_ex(cipher.getCtx(), nullptr, nullptr, key.data(), iv.data());
}

StringRef EncryptionStreamCipher::encrypt(unsigned char const* plaintext, int len, Arena& arena) {
	TEST(true); // Encrypting data with StreamCipher
	auto ciphertext = new (arena) unsigned char[len + AES_BLOCK_SIZE];
	int bytes{ 0 };
	EVP_EncryptUpdate(cipher.getCtx(), ciphertext, &bytes, plaintext, len);
	return StringRef(ciphertext, bytes);
}

StringRef EncryptionStreamCipher::finish(Arena& arena) {
	auto ciphertext = new (arena) unsigned char[AES_BLOCK_SIZE];
	int bytes{ 0 };
	EVP_EncryptFinal_ex(cipher.getCtx(), ciphertext, &bytes);
	return StringRef(ciphertext, bytes);
}

DecryptionStreamCipher::DecryptionStreamCipher(const StreamCipher::Key& key, const StreamCipher::IV& iv) {
	EVP_DecryptInit_ex(cipher.getCtx(), EVP_aes_128_gcm(), nullptr, nullptr, nullptr);
	EVP_CIPHER_CTX_ctrl(cipher.getCtx(), EVP_CTRL_AEAD_SET_IVLEN, iv.size(), nullptr);
	EVP_DecryptInit_ex(cipher.getCtx(), nullptr, nullptr, key.data(), iv.data());
}

StringRef DecryptionStreamCipher::decrypt(unsigned char const* ciphertext, int len, Arena& arena) {
	TEST(true); // Decrypting data with StreamCipher
	auto plaintext = new (arena) unsigned char[len];
	int bytesDecrypted{ 0 };
	EVP_DecryptUpdate(cipher.getCtx(), plaintext, &bytesDecrypted, ciphertext, len);
	int finalBlockBytes{ 0 };
	EVP_DecryptFinal_ex(cipher.getCtx(), plaintext + bytesDecrypted, &finalBlockBytes);
	return StringRef(plaintext, bytesDecrypted + finalBlockBytes);
}

StringRef DecryptionStreamCipher::finish(Arena& arena) {
	auto plaintext = new (arena) unsigned char[AES_BLOCK_SIZE];
	int finalBlockBytes{ 0 };
	EVP_DecryptFinal_ex(cipher.getCtx(), plaintext, &finalBlockBytes);
	return StringRef(plaintext, finalBlockBytes);
}

// Only used to link unit tests
void forceLinkStreamCipherTests() {}

// Tests both encryption and decryption of random data
// using the StreamCipher class
TEST_CASE("flow/StreamCipher") {
	StreamCipher::Key::initializeRandomTestKey();
	const auto& key = StreamCipher::Key::getKey();

	StreamCipher::IV iv;
	generateRandomData(iv.data(), iv.size());

	Arena arena;
	std::vector<unsigned char> plaintext(deterministicRandom()->randomInt(0, 10001));
	generateRandomData(&plaintext.front(), plaintext.size());
	std::vector<unsigned char> ciphertext(plaintext.size() + AES_BLOCK_SIZE);
	std::vector<unsigned char> decryptedtext(plaintext.size() + AES_BLOCK_SIZE);

	TraceEvent("StreamCipherTestStart")
	    .detail("PlaintextSize", plaintext.size())
	    .detail("AESBlockSize", AES_BLOCK_SIZE);
	{
		EncryptionStreamCipher encryptor(key, iv);
		int index = 0;
		int encryptedOffset = 0;
		while (index < plaintext.size()) {
			const auto chunkSize = std::min<int>(deterministicRandom()->randomInt(1, 101), plaintext.size() - index);
			const auto encrypted = encryptor.encrypt(&plaintext[index], chunkSize, arena);
			TraceEvent("StreamCipherTestEcryptedChunk")
			    .detail("EncryptedSize", encrypted.size())
			    .detail("EncryptedOffset", encryptedOffset)
			    .detail("Index", index);
			std::copy(encrypted.begin(), encrypted.end(), &ciphertext[encryptedOffset]);
			encryptedOffset += encrypted.size();
			index += chunkSize;
		}
		const auto encrypted = encryptor.finish(arena);
		std::copy(encrypted.begin(), encrypted.end(), &ciphertext[encryptedOffset]);
		ciphertext.resize(encryptedOffset + encrypted.size());
	}

	{
		DecryptionStreamCipher decryptor(key, iv);
		int index = 0;
		int decryptedOffset = 0;
		while (index < plaintext.size()) {
			const auto chunkSize = std::min<int>(deterministicRandom()->randomInt(1, 101), plaintext.size() - index);
			const auto decrypted = decryptor.decrypt(&ciphertext[index], chunkSize, arena);
			TraceEvent("StreamCipherTestDecryptedChunk")
			    .detail("DecryptedSize", decrypted.size())
			    .detail("DecryptedOffset", decryptedOffset)
			    .detail("Index", index);
			std::copy(decrypted.begin(), decrypted.end(), &decryptedtext[decryptedOffset]);
			decryptedOffset += decrypted.size();
			index += chunkSize;
		}
		const auto decrypted = decryptor.finish(arena);
		std::copy(decrypted.begin(), decrypted.end(), &decryptedtext[decryptedOffset]);
		ASSERT_EQ(decryptedOffset + decrypted.size(), plaintext.size());
		decryptedtext.resize(decryptedOffset + decrypted.size());
	}

	ASSERT(plaintext == decryptedtext);
	return Void();
}
