/*
 * StreamCipher.actor.cpp
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

#include "flow/StreamCipher.h"
#include "flow/Arena.h"
#include "flow/IRandom.h"
#include "flow/ITrace.h"
#include "flow/UnitTest.h"
#include <memory>

UID StreamCipherKey::globalKeyId;
std::unordered_map<UID, EVP_CIPHER_CTX*> StreamCipher::ctxs;
std::unordered_map<UID, StreamCipherKey*> StreamCipherKey::cipherKeys;
std::unique_ptr<StreamCipherKey> StreamCipherKey::globalKey;

bool StreamCipherKey::isGlobalKeyPresent() {
	return StreamCipherKey::globalKey.get() != nullptr;
}

void StreamCipherKey::allocGlobalCipherKey() {
	if (StreamCipherKey::isGlobalKeyPresent()) {
		return;
	}
	StreamCipherKey::globalKeyId = deterministicRandom()->randomUniqueID();
	StreamCipherKey::globalKey = std::make_unique<StreamCipherKey>(AES_256_KEY_LENGTH);
	StreamCipherKey::cipherKeys[StreamCipherKey::globalKeyId] = StreamCipherKey::globalKey.get();
}

void StreamCipherKey::initializeGlobalRandomTestKey() {
	if (!StreamCipherKey::isGlobalKeyPresent()) {
		StreamCipherKey::allocGlobalCipherKey();
	}
	StreamCipherKey::globalKey.get()->initializeRandomTestKey();
}

StreamCipherKey const* StreamCipherKey::getGlobalCipherKey() {
	if (!StreamCipherKey::isGlobalKeyPresent()) {
		StreamCipherKey::allocGlobalCipherKey();
	}
	ASSERT(StreamCipherKey::isGlobalKeyPresent());
	return globalKey.get();
}

void StreamCipherKey::cleanup() noexcept {
	for (const auto& itr : cipherKeys) {
		itr.second->reset();
	}
}

void StreamCipherKey::initializeKey(uint8_t* data, int len) {
	memset(arr.get(), 0, keySize);
	int copyLen = std::min(keySize, len);
	memcpy(arr.get(), data, copyLen);
}

StreamCipherKey::StreamCipherKey(int size)
  : id(deterministicRandom()->randomUniqueID()), arr(std::make_unique<uint8_t[]>(size)), keySize(size) {
	memset(arr.get(), 0, keySize);
	cipherKeys[id] = this;
}

StreamCipherKey::~StreamCipherKey() {
	reset();
	cipherKeys.erase(this->id);
}

StreamCipher::StreamCipher(int keySize)
  : id(deterministicRandom()->randomUniqueID()), ctx(EVP_CIPHER_CTX_new()), hmacCtx(HMAC_CTX_new()),
    cipherKey(std::make_unique<StreamCipherKey>(keySize)) {
	ctxs[id] = ctx;
}

StreamCipher::StreamCipher()
  : id(deterministicRandom()->randomUniqueID()), ctx(EVP_CIPHER_CTX_new()), hmacCtx(HMAC_CTX_new()),
    cipherKey(std::make_unique<StreamCipherKey>(AES_256_KEY_LENGTH)) {
	ctxs[id] = ctx;
}

StreamCipher::~StreamCipher() {
	HMAC_CTX_free(hmacCtx);
	EVP_CIPHER_CTX_free(ctx);
	ctxs.erase(id);
}

EVP_CIPHER_CTX* StreamCipher::getCtx() {
	return ctx;
}

HMAC_CTX* StreamCipher::getHmacCtx() {
	return hmacCtx;
}

void StreamCipher::cleanup() noexcept {
	for (auto itr : ctxs) {
		EVP_CIPHER_CTX_free(itr.second);
	}
}

void applyHmacKeyDerivationFunc(StreamCipherKey* cipherKey, HmacSha256StreamCipher* hmacGenerator, Arena& arena) {
	uint8_t buf[cipherKey->size() + sizeof(uint64_t)];
	memcpy(&buf[0], cipherKey->data(), cipherKey->size());
	uint64_t seed = deterministicRandom()->randomUInt64();
	memcpy(&buf[0] + cipherKey->size(), &seed, sizeof(uint64_t));
	StringRef digest = hmacGenerator->digest(&buf[0], cipherKey->size() + sizeof(uint64_t), arena);
	std::copy(digest.begin(), digest.end(), &buf[0]);
	cipherKey->initializeKey(&buf[0], cipherKey->size());
}

EncryptionStreamCipher::EncryptionStreamCipher(const StreamCipherKey* key, const StreamCipher::IV& iv)
  : cipher(StreamCipher(key->size())) {
	EVP_EncryptInit_ex(cipher.getCtx(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr);
	EVP_CIPHER_CTX_ctrl(cipher.getCtx(), EVP_CTRL_AEAD_SET_IVLEN, iv.size(), nullptr);
	EVP_EncryptInit_ex(cipher.getCtx(), nullptr, nullptr, key->data(), iv.data());
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

DecryptionStreamCipher::DecryptionStreamCipher(const StreamCipherKey* key, const StreamCipher::IV& iv)
  : cipher(key->size()) {
	EVP_DecryptInit_ex(cipher.getCtx(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr);
	EVP_CIPHER_CTX_ctrl(cipher.getCtx(), EVP_CTRL_AEAD_SET_IVLEN, iv.size(), nullptr);
	EVP_DecryptInit_ex(cipher.getCtx(), nullptr, nullptr, key->data(), iv.data());
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

HmacSha256StreamCipher::HmacSha256StreamCipher() : cipher(EVP_MAX_KEY_LENGTH) {
	HMAC_Init_ex(cipher.getHmacCtx(), NULL, 0, EVP_sha256(), nullptr);
}

StringRef HmacSha256StreamCipher::digest(unsigned char const* data, int len, Arena& arena) {
	TEST(true); // Digest using StreamCipher
	unsigned int digestLen = HMAC_size(cipher.getHmacCtx());
	auto digest = new (arena) unsigned char[digestLen];
	HMAC_Update(cipher.getHmacCtx(), data, len);
	HMAC_Final(cipher.getHmacCtx(), digest, &digestLen);
	return StringRef(digest, digestLen);
}

StringRef HmacSha256StreamCipher::finish(Arena& arena) {
	unsigned int digestLen = HMAC_size(cipher.getHmacCtx());
	auto digest = new (arena) unsigned char[digestLen];
	HMAC_Final(cipher.getHmacCtx(), digest, &digestLen);
	return StringRef(digest, digestLen);
}

// Only used to link unit tests
void forceLinkStreamCipherTests() {}

// Tests both encryption and decryption of random data
// using the StreamCipher class
TEST_CASE("flow/StreamCipher") {
	StreamCipherKey::initializeGlobalRandomTestKey();
	StreamCipherKey const* key = StreamCipherKey::getGlobalCipherKey();

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
