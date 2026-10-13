/*
 * AsyncFileEncrypted.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "flow/actorcompiler.h" // must be last include

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
	ACTOR static Future<Standalone<StringRef>> readBlock(AsyncFileEncrypted* self, uint32_t block) {
		state Arena arena;
		state int rawBlockSize = self->encryptionBlockSize + GCM_TAG_LEN;
		state unsigned char* encrypted = new (arena) unsigned char[rawBlockSize];
		int bytes = wait(
		    uncancellable(holdWhile(arena, self->file->read(encrypted, rawBlockSize, int64_t(rawBlockSize) * block))));
		if (bytes < GCM_TAG_LEN) {
			throw restore_corrupted_data();
		}
		state int ciphertextLen = bytes - GCM_TAG_LEN;
		state const uint8_t* tag = encrypted + ciphertextLen;
		StreamCipherKey const* cipherKey = StreamCipherKey::getGlobalCipherKey();
		DecryptionStreamCipher decryptor(cipherKey, self->getIV(block));
		auto decrypted = decryptor.decrypt(encrypted, ciphertextLen, arena);
		// Throws restore_corrupted_data if the tag does not verify.
		decryptor.finish(tag, arena);
		return Standalone<StringRef>(decrypted, arena);
	}

	ACTOR static Future<int> read(Reference<AsyncFileEncrypted> self, void* data, int length, int64_t offset) {
		if (self->fileSize == -1) {
			state int64_t rawSize = wait(self->file->size());
			self->fileSize = AsyncFileEncrypted::rawToLogicalSize(rawSize, self->encryptionBlockSize);
		}
		if (offset >= self->fileSize) {
			return 0;
		}
		if (offset + length > self->fileSize) {
			length = self->fileSize - offset;
		}
		state uint32_t firstBlock = offset / self->encryptionBlockSize;
		state uint32_t lastBlock = (offset + length - 1) / self->encryptionBlockSize;
		state uint32_t block;
		state unsigned char* output = reinterpret_cast<unsigned char*>(data);
		state int bytesRead = 0;
		ASSERT(self->mode == AsyncFileEncrypted::Mode::READ_ONLY);
		for (block = firstBlock; block <= lastBlock; ++block) {
			state Standalone<StringRef> plaintext;
			wait(store(plaintext, readBlock(self.getPtr(), block)));
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
		return bytesRead;
	}

	ACTOR static Future<Void> write(Reference<AsyncFileEncrypted> self, void const* data, int length, int64_t offset) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		// All writes must append to the end of the file:
		ASSERT_EQ(offset, self->currentBlock * self->encryptionBlockSize + self->offsetInBlock);
		state unsigned char const* input = reinterpret_cast<unsigned char const*>(data);
		while (length > 0) {
			const auto chunkSize = std::min(length, self->encryptionBlockSize - self->offsetInBlock);
			std::copy(input, input + chunkSize, &self->writeBuffer[self->offsetInBlock]);
			self->offsetInBlock += chunkSize;
			length -= chunkSize;
			input += chunkSize;
			if (self->offsetInBlock == self->encryptionBlockSize) {
				wait(self->writeLastBlockToFile());
				self->offsetInBlock = 0;
				ASSERT_LT(self->currentBlock, std::numeric_limits<uint32_t>::max());
				++self->currentBlock;
			}
		}
		return Void();
	}

	ACTOR static Future<Void> sync(Reference<AsyncFileEncrypted> self) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		if (self->offsetInBlock > 0) {
			wait(self->writeLastBlockToFile());
		}
		wait(self->file->sync());
		return Void();
	}

	ACTOR static Future<Void> zeroRange(AsyncFileEncrypted* self, int64_t offset, int64_t length) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		// TODO: Could optimize this
		Arena arena;
		auto zeroes = new (arena) unsigned char[length];
		memset(zeroes, 0, length);
		wait(uncancellable(holdWhile(arena, self->write(zeroes, length, offset))));
		return Void();
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
	state int encryptionBlockSize = 4096;
	state const int bytes = encryptionBlockSize * deterministicRandom()->randomInt(0, 1000);
	state std::vector<unsigned char> writeBuffer(bytes, 0);
	deterministicRandom()->randomBytes(&writeBuffer.front(), bytes);
	state std::vector<unsigned char> readBuffer(bytes, 0);
	ASSERT(g_network->isSimulated());
	StreamCipherKey::initializeGlobalRandomTestKey();
	int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
	            IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
	state Reference<IAsyncFile> raw_file =
	    wait(IAsyncFileSystem::filesystem()->open(joinPath(params.getDataDir(), "test-encrypted-file"), flags, 0600));
	state Reference<AsyncFileEncrypted> file =
	    makeReference<AsyncFileEncrypted>(raw_file, AsyncFileEncrypted::Mode::APPEND_ONLY, encryptionBlockSize);
	state int bytesWritten = 0;
	state int chunkSize;
	while (bytesWritten < bytes) {
		chunkSize = std::min(deterministicRandom()->randomInt(0, 100), bytes - bytesWritten);
		wait(file->write(&writeBuffer[bytesWritten], chunkSize, bytesWritten));
		bytesWritten += chunkSize;
	}
	wait(file->sync());
	state int bytesRead = 0;
	while (bytesRead < bytes) {
		chunkSize = std::min(deterministicRandom()->randomInt(0, 100), bytes - bytesRead);
		int bytesReadInChunk = wait(file->read(&readBuffer[bytesRead], chunkSize, bytesRead));
		ASSERT_EQ(bytesReadInChunk, chunkSize);
		bytesRead += bytesReadInChunk;
	}
	ASSERT(writeBuffer == readBuffer);
	return Void();
}
