/*
 * AsyncFileEncrypted.actor.cpp
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

#include "fdbrpc/AsyncFileEncrypted.h"
#include "flow/StreamCipher.h"
#include "flow/UnitTest.h"
#include "flow/xxhash.h"
#include "flow/actorcompiler.h" // must be last include

class AsyncFileEncryptedImpl {
public:
	static auto getFirstBlockIV(const std::string& filename) {
		StreamCipher::IV iv;
		auto hash = XXH3_128bits(filename.c_str(), filename.size());
		auto high = reinterpret_cast<unsigned char*>(&hash.high64);
		auto low = reinterpret_cast<unsigned char*>(&hash.low64);
		std::copy(high, high + 8, &iv[0]);
		std::copy(low, low + 6, &iv[8]);
		iv[14] = iv[15] = 0; // last 16 bits identify block
		return iv;
	}

	ACTOR static Future<Standalone<StringRef>> readBlock(AsyncFileEncrypted* self, uint16_t block) {
		state Arena arena;
		state unsigned char* encrypted = new (arena) unsigned char[FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE];
		int bytes = wait(
		    self->file->read(encrypted, FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE, FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE * block));
		DecryptionStreamCipher decryptor(AsyncFileEncrypted::getKey(), self->getIV(block));
		auto decrypted = decryptor.decrypt(encrypted, bytes, arena);
		return Standalone<StringRef>(decrypted, arena);
	}

	ACTOR static Future<int> read(AsyncFileEncrypted* self, void* data, int length, int offset) {
		state const uint16_t firstBlock = offset / FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE;
		state const uint16_t lastBlock = (offset + length - 1) / FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE;
		state uint16_t block;
		state unsigned char* output = reinterpret_cast<unsigned char*>(data);
		state int bytesRead = 0;
		for (block = firstBlock; block <= lastBlock; ++block) {
			state StringRef plaintext;
			auto it = self->readBuffers.find(block);
			if (it != self->readBuffers.end()) {
				plaintext = it->second;
			} else {
				Standalone<StringRef> _plaintext = wait(readBlock(self, block));
				ASSERT(_plaintext.size() == FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE);
				if (self->readBuffers.size() == FLOW_KNOBS->MAX_DECRYPTED_BLOCKS) {
					// TODO: Improve eviction policy
					self->readBuffers.erase(self->readBuffers.begin());
				}
				self->readBuffers[block] = _plaintext;
				plaintext = _plaintext;
			}
			ASSERT(plaintext.size() == FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE);
			auto start = (block == firstBlock) ? plaintext.begin() + (offset % FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE)
			                                   : plaintext.begin();
			auto end = (block == lastBlock)
			               ? plaintext.begin() + ((offset + length) % FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE)
			               : plaintext.end();
			if ((offset + length) % FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE == 0) {
				end = plaintext.end();
			}
			std::copy(start, end, output);
			output += (end - start);
			bytesRead += (end - start);
		}
		return bytesRead;
	}

	ACTOR static Future<Void> write(AsyncFileEncrypted* self, void const* data, int length, int64_t offset) {
		ASSERT(self->canWrite);
		ASSERT(offset == self->currentBlock * FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE + self->offsetInBlock);
		state unsigned char const* input = reinterpret_cast<unsigned char const*>(data);
		while (length > 0) {
			const auto chunkSize = std::min(length, FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE - self->offsetInBlock);
			Arena arena;
			auto encrypted = self->encryptor->encrypt(input, chunkSize, arena);
			std::copy(encrypted.begin(), encrypted.end(), &self->writeBuffer[self->offsetInBlock]);
			offset += encrypted.size();
			self->offsetInBlock += chunkSize;
			length -= chunkSize;
			input += chunkSize;
			if (self->offsetInBlock == FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE) {
				wait(self->writeLastBlockToFile());
				self->offsetInBlock = 0;
				ASSERT(self->currentBlock < std::numeric_limits<uint16_t>::max());
				++self->currentBlock;
				self->encryptor = std::make_unique<EncryptionStreamCipher>(AsyncFileEncrypted::getKey(), self->getIV(self->currentBlock));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> sync(AsyncFileEncrypted* self) {
		ASSERT(self->canWrite);
		wait(self->writeLastBlockToFile());
		wait(self->file->sync());
		return Void();
	}

	ACTOR static Future<Void> initializeKey(Reference<IAsyncFile> keyFile, int64_t offset) {
		ASSERT(!AsyncFileEncrypted::key.present());
		AsyncFileEncrypted::key = StreamCipher::Key{};
		state int keySize = AsyncFileEncrypted::key.get().size();
		if (g_network->isSimulated()) {
			generateRandomData(AsyncFileEncrypted::key.get().data(), keySize);
			return Void();
		} else {
			int bytesRead = wait(keyFile->read(AsyncFileEncrypted::key.get().data(), keySize, offset));
			ASSERT(bytesRead == keySize);
			return Void();
		}
	}

	ACTOR static Future<Void> zeroRange(AsyncFileEncrypted* self, int64_t offset, int64_t length) {
		// TODO: Could optimize this
		Arena arena;
		auto zeroes = new (arena) unsigned char[length];
		memset(zeroes, 0, length);
		wait(self->write(zeroes, length, offset));
		return Void();
	}
};

AsyncFileEncrypted::AsyncFileEncrypted(Reference<IAsyncFile> file, bool canWrite)
  : file(file), canWrite(canWrite), currentBlock(0) {
	firstBlockIV = AsyncFileEncryptedImpl::getFirstBlockIV(file->getFilename());
	if (canWrite) {
		encryptor = std::make_unique<EncryptionStreamCipher>(AsyncFileEncrypted::getKey(), getIV(currentBlock));
		writeBuffer = std::vector<unsigned char>(FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE, 0);
	}
}

void AsyncFileEncrypted::addref() {
	ReferenceCounted<AsyncFileEncrypted>::addref();
}

void AsyncFileEncrypted::delref() {
	ReferenceCounted<AsyncFileEncrypted>::delref();
}

Future<int> AsyncFileEncrypted::read(void* data, int length, int64_t offset) {
	return AsyncFileEncryptedImpl::read(this, data, length, offset);
}

Future<Void> AsyncFileEncrypted::write(void const* data, int length, int64_t offset) {
	return AsyncFileEncryptedImpl::write(this, data, length, offset);
}

Future<Void> AsyncFileEncrypted::zeroRange(int64_t offset, int64_t length) {
	return AsyncFileEncryptedImpl::zeroRange(this, offset, length);
}

Future<Void> AsyncFileEncrypted::truncate(int64_t size) {
	ASSERT(false); // TODO: Not yet implemented
	return Void();
}

Future<Void> AsyncFileEncrypted::sync() {
	return AsyncFileEncryptedImpl::sync(this);
}

Future<Void> AsyncFileEncrypted::flush() {
	return Void();
}

Future<int64_t> AsyncFileEncrypted::size() const {
	return currentBlock * FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE + offsetInBlock;
}

std::string AsyncFileEncrypted::getFilename() const {
	return file->getFilename();
}

Future<Void> AsyncFileEncrypted::readZeroCopy(void** data, int* length, int64_t offset) {
	ASSERT(false); // Not implemented
	return Void();
}

void AsyncFileEncrypted::releaseZeroCopy(void* data, int length, int64_t offset) {
	ASSERT(false); // Not implemented
}

int64_t AsyncFileEncrypted::debugFD() const {
	return 0;
}

StreamCipher::IV AsyncFileEncrypted::getIV(uint16_t block) const {
	auto iv = firstBlockIV;
	iv[14] = block / 256;
	iv[15] = block % 256;
	return iv;
}

Future<Void> AsyncFileEncrypted::writeLastBlockToFile() {
	return file->write(&writeBuffer[0], offsetInBlock, currentBlock * FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE);
}

Optional<StreamCipher::Key> AsyncFileEncrypted::key;

StreamCipher::Key AsyncFileEncrypted::getKey() {
	return key.get();
}

Future<Void> AsyncFileEncrypted::initializeKey(const Reference<IAsyncFile>& keyFile, int64_t offset) {
	return AsyncFileEncryptedImpl::initializeKey(keyFile, offset);
}

TEST_CASE("fdbrpc/AsyncFileEncrypted") {
	state const int bytes = FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE * deterministicRandom()->randomInt(0, 1000);
	state std::vector<unsigned char> writeBuffer(bytes, 0);
	generateRandomData(&writeBuffer.front(), bytes);
	state std::vector<unsigned char> readBuffer(bytes, 0);
	ASSERT(g_network->isSimulated());
	wait(AsyncFileEncrypted::initializeKey(Reference<IAsyncFile>{}));
	int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
	            IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_ENCRYPTED | IAsyncFile::OPEN_UNCACHED |
	            IAsyncFile::OPEN_NO_AIO;
	state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open("/tmp/test", flags, 0600));
	state int bytesWritten = 0;
	while (bytesWritten < bytes) {
		chunkSize = std::min(deterministicRandom()->randomInt(0, 100), bytes - bytesWritten);
		wait(file->write(&writeBuffer[bytesWritten], chunkSize, bytesWritten));
		bytesWritten += chunkSize;
	}
	wait(file->sync());
	state int bytesRead = 0;
	state int chunkSize;
	while (bytesRead < bytes) {
		chunkSize = std::min(deterministicRandom()->randomInt(0, 100), bytes - bytesRead);
		int bytesReadInChunk = wait(file->read(&readBuffer[bytesRead], chunkSize, bytesRead));
		ASSERT(bytesReadInChunk == chunkSize);
		bytesRead += bytesReadInChunk;
	}
	ASSERT(writeBuffer == readBuffer);
	return Void();
}
