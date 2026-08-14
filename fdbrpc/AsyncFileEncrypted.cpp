/*
 * AsyncFileEncrypted.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/AsyncFileEncrypted.h"
#include "flow/StreamCipher.h"
#include "flow/UnitTest.h"
#include "flow/xxhash.h"

class AsyncFileEncryptedImpl {
public:
	// Determine the initialization for the first block of a file based on a hash of
	// the filename.
	static auto getFirstBlockIV(const std::string& filename) {
		StreamCipher::IV iv;
		auto slashPos = filename.rfind('/');
		auto salt = (slashPos == std::string::npos) ? filename : filename.substr(slashPos + 1);
		auto pos = salt.find('.');
		salt = salt.substr(0, pos);
		auto hash = XXH3_128bits(salt.c_str(), salt.size());
		auto pHigh = reinterpret_cast<unsigned char*>(&hash.high64);
		auto pLow = reinterpret_cast<unsigned char*>(&hash.low64);
		std::copy(pHigh, pHigh + 8, &iv[0]);
		std::copy(pLow, pLow + 4, &iv[8]);
		uint32_t blockZero = 0;
		auto pBlock = reinterpret_cast<unsigned char*>(&blockZero);
		std::copy(pBlock, pBlock + 4, &iv[12]);
		return iv;
	}

	// Read a single block of encryptionBlockSize bytes plus the trailing GCM tag, verify tag, and decrypt.
	static Future<Standalone<StringRef>> readBlock(AsyncFileEncrypted* self, uint32_t block) {
		Arena arena;
		const int rawBlockSize = self->encryptionBlockSize + GCM_TAG_LEN;
		auto* encrypted = new (arena) unsigned char[rawBlockSize];
		int bytes = co_await uncancellable(
		    holdWhile(arena, self->file->read(encrypted, rawBlockSize, int64_t(rawBlockSize) * block)));
		if (bytes < GCM_TAG_LEN) {
			throw restore_corrupted_data();
		}
		const int ciphertextLen = bytes - GCM_TAG_LEN;
		const uint8_t* tag = encrypted + ciphertextLen;
		StreamCipherKey const* cipherKey = StreamCipherKey::getGlobalCipherKey();
		DecryptionStreamCipher decryptor(cipherKey, self->getIV(block));
		auto decrypted = decryptor.decrypt(encrypted, ciphertextLen, arena);
		// Throws restore_corrupted_data if the tag does not verify.
		decryptor.finish(tag, arena);
		co_return Standalone<StringRef>(decrypted, arena);
	}

	static Future<int> read(Reference<AsyncFileEncrypted> self, void* data, int length, int64_t offset) {
		if (self->fileSize == -1) {
			int64_t rawSize = co_await self->file->size();
			self->fileSize = AsyncFileEncrypted::rawToLogicalSize(rawSize, self->encryptionBlockSize);
		}
		if (offset >= self->fileSize) {
			co_return 0;
		}
		if (offset + length > self->fileSize) {
			length = self->fileSize - offset;
		}
		uint32_t firstBlock = offset / self->encryptionBlockSize;
		uint32_t lastBlock = (offset + length - 1) / self->encryptionBlockSize;
		uint32_t block{ 0 };
		auto* output = reinterpret_cast<unsigned char*>(data);
		int bytesRead = 0;
		ASSERT(self->mode == AsyncFileEncrypted::Mode::READ_ONLY);
		for (block = firstBlock; block <= lastBlock; ++block) {
			Standalone<StringRef> plaintext = co_await readBlock(self.getPtr(), block);
			auto start =
			    (block == firstBlock) ? plaintext.begin() + (offset % self->encryptionBlockSize) : plaintext.begin();
			auto end = (block == lastBlock) ? plaintext.begin() + ((offset + length) % self->encryptionBlockSize)
			                                : plaintext.end();
			if ((offset + length) % self->encryptionBlockSize == 0) {
				end = plaintext.end();
			}

			// The block could be short if it includes or is after the end of the file.
			end = std::min(end, plaintext.end());
			// If the start position is at or after the end of the block, the read is complete.
			if (start == end || start >= plaintext.end()) {
				break;
			}

			std::copy(start, end, output);
			output += (end - start);
			bytesRead += (end - start);
		}
		co_return bytesRead;
	}

	static Future<Void> write(Reference<AsyncFileEncrypted> self, void const* data, int length, int64_t offset) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		// All writes must append to the end of the file:
		ASSERT_EQ(offset, self->currentBlock * self->encryptionBlockSize + self->offsetInBlock);
		auto const* input = reinterpret_cast<unsigned char const*>(data);
		while (length > 0) {
			const auto chunkSize = std::min(length, self->encryptionBlockSize - self->offsetInBlock);
			std::copy(input, input + chunkSize, &self->writeBuffer[self->offsetInBlock]);
			self->offsetInBlock += chunkSize;
			length -= chunkSize;
			input += chunkSize;
			if (self->offsetInBlock == self->encryptionBlockSize) {
				co_await self->writeLastBlockToFile();
				self->offsetInBlock = 0;
				ASSERT_LT(self->currentBlock, std::numeric_limits<uint32_t>::max());
				++self->currentBlock;
			}
		}
	}

	static Future<Void> sync(Reference<AsyncFileEncrypted> self) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		if (self->offsetInBlock > 0) {
			co_await self->writeLastBlockToFile();
		}
		co_await self->file->sync();
	}

	static Future<Void> zeroRange(AsyncFileEncrypted* self, int64_t offset, int64_t length) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		// TODO: Could optimize this
		Arena arena;
		auto zeroes = new (arena) unsigned char[length];
		memset(zeroes, 0, length);
		co_await uncancellable(holdWhile(arena, self->write(zeroes, length, offset)));
	}
};

AsyncFileEncrypted::AsyncFileEncrypted(Reference<IAsyncFile> file, Mode mode, int encryptionBlockSize)
  : file(file), mode(mode), currentBlock(0), encryptionBlockSize(encryptionBlockSize) {
	ASSERT(encryptionBlockSize > 0);
	firstBlockIV = AsyncFileEncryptedImpl::getFirstBlockIV(file->getFilename());
	if (mode == Mode::APPEND_ONLY) {
		writeBuffer = std::vector<unsigned char>(encryptionBlockSize, 0);
	}
}

void AsyncFileEncrypted::addref() {
	ReferenceCounted<AsyncFileEncrypted>::addref();
}

void AsyncFileEncrypted::delref() {
	ReferenceCounted<AsyncFileEncrypted>::delref();
}

int64_t AsyncFileEncrypted::rawToLogicalSize(int64_t rawSize, int blockSize) {
	const int64_t rawBlockSize = int64_t(blockSize) + GCM_TAG_LEN;
	const int64_t fullBlocks = rawSize / rawBlockSize;
	const int64_t trailing = rawSize % rawBlockSize;
	int64_t logical = fullBlocks * blockSize;
	if (trailing > 0) {
		ASSERT(trailing > GCM_TAG_LEN);
		logical += trailing - GCM_TAG_LEN;
	}
	return logical;
}

int64_t AsyncFileEncrypted::logicalToRawSize(int64_t logicalSize, int blockSize) {
	const int64_t rawBlockSize = int64_t(blockSize) + GCM_TAG_LEN;
	const int64_t fullBlocks = logicalSize / blockSize;
	const int64_t trailing = logicalSize % blockSize;
	int64_t raw = fullBlocks * rawBlockSize;
	if (trailing > 0) {
		raw += trailing + GCM_TAG_LEN;
	}
	return raw;
}

Future<int> AsyncFileEncrypted::read(void* data, int length, int64_t offset) {
	return AsyncFileEncryptedImpl::read(Reference<AsyncFileEncrypted>::addRef(this), data, length, offset);
}

Future<Void> AsyncFileEncrypted::write(void const* data, int length, int64_t offset) {
	return AsyncFileEncryptedImpl::write(Reference<AsyncFileEncrypted>::addRef(this), data, length, offset);
}

Future<Void> AsyncFileEncrypted::zeroRange(int64_t offset, int64_t length) {
	return AsyncFileEncryptedImpl::zeroRange(this, offset, length);
}

Future<Void> AsyncFileEncrypted::truncate(int64_t size) {
	ASSERT(mode == Mode::APPEND_ONLY);
	return file->truncate(logicalToRawSize(size, encryptionBlockSize));
}

Future<Void> AsyncFileEncrypted::sync() {
	ASSERT(mode == Mode::APPEND_ONLY);
	return AsyncFileEncryptedImpl::sync(Reference<AsyncFileEncrypted>::addRef(this));
}

Future<Void> AsyncFileEncrypted::flush() {
	ASSERT(mode == Mode::APPEND_ONLY);
	return Void();
}

Future<int64_t> AsyncFileEncrypted::size() const {
	ASSERT(mode == Mode::READ_ONLY);
	int blockSize = encryptionBlockSize;
	return map(file->size(), [blockSize](int64_t raw) { return rawToLogicalSize(raw, blockSize); });
}

std::string AsyncFileEncrypted::getFilename() const {
	return file->getFilename();
}

Future<Void> AsyncFileEncrypted::readZeroCopy(void** data, int* length, int64_t offset) {
	throw io_error();
	return Void();
}

void AsyncFileEncrypted::releaseZeroCopy(void* data, int length, int64_t offset) {
	throw io_error();
}

int64_t AsyncFileEncrypted::debugFD() const {
	return file->debugFD();
}

StreamCipher::IV AsyncFileEncrypted::getIV(uint32_t block) const {
	auto iv = firstBlockIV;

	auto pBlock = reinterpret_cast<unsigned char*>(&block);
	std::copy(pBlock, pBlock + 4, &iv[12]);

	return iv;
}

Future<Void> AsyncFileEncrypted::writeLastBlockToFile() {
	Arena arena;
	EncryptionStreamCipher encryptor(StreamCipherKey::getGlobalCipherKey(), getIV(currentBlock));
	auto ciphertext = encryptor.encrypt(&writeBuffer[0], offsetInBlock, arena);
	auto tag = encryptor.finish(arena);
	const int rawBlockSize = encryptionBlockSize + GCM_TAG_LEN;
	auto* outBuf = new (arena) unsigned char[offsetInBlock + GCM_TAG_LEN];
	std::copy(ciphertext.begin(), ciphertext.end(), outBuf);
	std::copy(tag.begin(), tag.end(), outBuf + ciphertext.size());
	return uncancellable(holdWhile(
	    Reference<AsyncFileEncrypted>::addRef(this),
	    holdWhile(arena, file->write(outBuf, offsetInBlock + GCM_TAG_LEN, int64_t(rawBlockSize) * currentBlock))));
}

// This test writes random data into an encrypted file in random increments,
// then reads this data back from the file in random increments, then confirms that
// the bytes read match the bytes written.
TEST_CASE("fdbrpc/AsyncFileEncrypted") {
	int encryptionBlockSize = 4096;
	const int bytes = encryptionBlockSize * deterministicRandom()->randomInt(0, 1000);
	std::vector<unsigned char> writeBuffer(bytes, 0);
	if (bytes > 0) {
		deterministicRandom()->randomBytes(writeBuffer.data(), bytes);
	}
	std::vector<unsigned char> readBuffer(bytes, 0);
	StreamCipherKey::initializeGlobalRandomTestKey();
	int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
	            IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
	Reference<IAsyncFile> rawFile = co_await IAsyncFileSystem::filesystem()->open(
	    joinPath(params.getDataDir(), "test-encrypted-file"), flags, 0600);
	Reference<IAsyncFile> file =
	    makeReference<AsyncFileEncrypted>(rawFile, AsyncFileEncrypted::Mode::APPEND_ONLY, encryptionBlockSize);
	int bytesWritten = 0;
	int chunkSize;
	while (bytesWritten < bytes) {
		chunkSize = std::min(deterministicRandom()->randomInt(0, 100), bytes - bytesWritten);
		co_await file->write(&writeBuffer[bytesWritten], chunkSize, bytesWritten);
		bytesWritten += chunkSize;
	}
	co_await file->sync();
	file = makeReference<AsyncFileEncrypted>(rawFile, AsyncFileEncrypted::Mode::READ_ONLY, encryptionBlockSize);
	int bytesRead = 0;
	while (bytesRead < bytes) {
		chunkSize = std::min(deterministicRandom()->randomInt(0, 100), bytes - bytesRead);
		int bytesReadInChunk = co_await file->read(&readBuffer[bytesRead], chunkSize, bytesRead);
		ASSERT_EQ(bytesReadInChunk, chunkSize);
		bytesRead += bytesReadInChunk;
	}
	ASSERT(writeBuffer == readBuffer);
}
