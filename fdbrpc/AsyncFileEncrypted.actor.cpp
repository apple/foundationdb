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
#include "fdbrpc/AsyncFileReadAhead.actor.h"
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
		auto salt = basename(filename);
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

	// Read a single block of size ENCRYPTION_BLOCK_SIZE bytes, and decrypt.
	ACTOR static Future<Standalone<StringRef>> readBlock(AsyncFileEncrypted* self, uint32_t block) {
		state Arena arena;
		state unsigned char* encrypted = new (arena) unsigned char[FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE];
		int bytes = wait(uncancellable(holdWhile(arena,
		                                         self->file->read(encrypted,
		                                                          FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE,
		                                                          FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE * block))));
		StreamCipherKey const* cipherKey = StreamCipherKey::getGlobalCipherKey();
		DecryptionStreamCipher decryptor(cipherKey, self->getIV(block));
		auto decrypted = decryptor.decrypt(encrypted, bytes, arena);
		return Standalone<StringRef>(decrypted, arena);
	}

	ACTOR static Future<int> read(Reference<AsyncFileEncrypted> self, void* data, int length, int64_t offset) {
		state const uint32_t firstBlock = offset / FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE;
		state const uint32_t lastBlock = (offset + length - 1) / FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE;
		state uint32_t block;
		state unsigned char* output = reinterpret_cast<unsigned char*>(data);
		state int bytesRead = 0;
		// ASSERT(self->mode == AsyncFileEncrypted::Mode::READ_ONLY);
		for (block = firstBlock; block <= lastBlock; ++block) {
			state Standalone<StringRef> plaintext;

			auto cachedBlock = self->readBuffers.get(block);
			if (cachedBlock.present()) {
				plaintext = cachedBlock.get();
			} else {
				wait(store(plaintext, readBlock(self.getPtr(), block)));
				self->readBuffers.insert(block, plaintext);
			}
			auto start = (block == firstBlock) ? plaintext.begin() + (offset % FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE)
			                                   : plaintext.begin();
			auto end = (block == lastBlock)
			               ? plaintext.begin() + ((offset + length) % FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE)
			               : plaintext.end();
			if ((offset + length) % FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE == 0) {
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
		ASSERT_EQ(offset, self->currentBlock * FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE + self->offsetInBlock);
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
				ASSERT_LT(self->currentBlock, std::numeric_limits<uint32_t>::max());
				++self->currentBlock;
				self->encryptor = std::make_unique<EncryptionStreamCipher>(StreamCipherKey::getGlobalCipherKey(),
				                                                           self->getIV(self->currentBlock));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> sync(Reference<AsyncFileEncrypted> self) {
		ASSERT(self->mode == AsyncFileEncrypted::Mode::APPEND_ONLY);
		wait(self->writeLastBlockToFile());
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

AsyncFileEncrypted::AsyncFileEncrypted(Reference<IAsyncFile> file, Mode mode)
  : file(file), mode(mode), readBuffers(FLOW_KNOBS->MAX_DECRYPTED_BLOCKS), currentBlock(0) {
	firstBlockIV = AsyncFileEncryptedImpl::getFirstBlockIV(file->getFilename());
	if (mode == Mode::APPEND_ONLY) {
		encryptor =
		    std::make_unique<EncryptionStreamCipher>(StreamCipherKey::getGlobalCipherKey(), getIV(currentBlock));
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
	return file->truncate(size);
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
	return file->size();
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
	// The source buffer for the write is owned by *this so this must be kept alive by reference count until the write
	// is finished.
	return uncancellable(
	    holdWhile(Reference<AsyncFileEncrypted>::addRef(this),
	              file->write(&writeBuffer[0], offsetInBlock, currentBlock * FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE)));
}

size_t AsyncFileEncrypted::RandomCache::evict() {
	ASSERT_EQ(vec.size(), maxSize);
	auto index = deterministicRandom()->randomInt(0, maxSize);
	hashMap.erase(vec[index]);
	return index;
}

AsyncFileEncrypted::RandomCache::RandomCache(size_t maxSize) : maxSize(maxSize) {
	vec.reserve(maxSize);
}

void AsyncFileEncrypted::RandomCache::insert(uint32_t block, const Standalone<StringRef>& value) {
	auto [_, found] = hashMap.insert({ block, value });
	if (found) {
		return;
	} else if (vec.size() < maxSize) {
		vec.push_back(block);
	} else {
		auto index = evict();
		vec[index] = block;
	}
}

Optional<Standalone<StringRef>> AsyncFileEncrypted::RandomCache::get(uint32_t block) const {
	auto it = hashMap.find(block);
	if (it == hashMap.end()) {
		return {};
	} else {
		return it->second;
	}
}

// This test writes random data into an encrypted file in random increments,
// then reads this data back from the file in random increments, then confirms that
// the bytes read match the bytes written.
TEST_CASE("fdbrpc/AsyncFileEncrypted") {
	state const int bytes = FLOW_KNOBS->ENCRYPTION_BLOCK_SIZE * deterministicRandom()->randomInt(0, 1000);
	state std::vector<unsigned char> writeBuffer(bytes, 0);
	deterministicRandom()->randomBytes(&writeBuffer.front(), bytes);
	state std::vector<unsigned char> readBuffer(bytes, 0);
	// ASSERT(g_network->isSimulated());
	StreamCipherKey::initializeGlobalRandomTestKey();
	int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
	            IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_ENCRYPTED | IAsyncFile::OPEN_UNCACHED |
	            IAsyncFile::OPEN_NO_AIO;
	state Reference<IAsyncFile> file =
	    wait(IAsyncFileSystem::filesystem()->open(joinPath(params.getDataDir(), "test-encrypted-file"), flags, 0600));
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

// Mock file wrapper that simulates S3-like behavior by failing when reads go beyond file size
class MockS3LikeFile : public IAsyncFile, public ReferenceCounted<MockS3LikeFile> {
private:
	Reference<IAsyncFile> underlying;
	int64_t fileSize;

public:
	MockS3LikeFile(Reference<IAsyncFile> file, int64_t size) : underlying(file), fileSize(size) {}
	
	void addref() override { ReferenceCounted<MockS3LikeFile>::addref(); }
	void delref() override { ReferenceCounted<MockS3LikeFile>::delref(); }
	
	StringRef getClassName() override { return "MockS3LikeFile"_sr; }
	
	Future<int> read(void* data, int length, int64_t offset) override {
		// Simulate S3 behavior: fail if trying to read beyond file size
		if (offset >= fileSize) {
			throw io_error();
		}
		// // Also fail if the read would extend beyond the file size (this is the key behavior)
		// if (offset + length > fileSize) {
		// 	throw io_error();
		// }
		return underlying->read(data, length, offset);
	}
	
	Future<Void> write(void const* data, int length, int64_t offset) override {
		return underlying->write(data, length, offset);
	}
	
	Future<Void> zeroRange(int64_t offset, int64_t length) override {
		return underlying->zeroRange(offset, length);
	}
	
	Future<Void> truncate(int64_t size) override {
		fileSize = size;
		return underlying->truncate(size);
	}
	
	Future<Void> sync() override { return underlying->sync(); }
	Future<Void> flush() override { return underlying->flush(); }
	Future<int64_t> size() const override { return fileSize; }
	std::string getFilename() const override { return underlying->getFilename(); }
	
	Future<Void> readZeroCopy(void** data, int* length, int64_t offset) override {
		return underlying->readZeroCopy(data, length, offset);
	}
	
	void releaseZeroCopy(void* data, int length, int64_t offset) override {
		underlying->releaseZeroCopy(data, length, offset);
	}
	
	int64_t debugFD() const override { return underlying->debugFD(); }
};

// Test case to reproduce the bug where AsyncFileReadAhead seeks to offset 4096 
// when reading a small file through AsyncFileEncrypted, which can cause issues 
// with certain filesystems like S3
TEST_CASE("fdbrpc/AsyncFileEncryptedReadAheadBug") {
	// ASSERT(g_network->isSimulated());
	StreamCipherKey::initializeGlobalRandomTestKey();
	
	// Create a small file (50 bytes) to reproduce the issue
	state int smallFileSize = 50;
	state std::vector<unsigned char> writeBuffer(smallFileSize, 0);
	deterministicRandom()->randomBytes(&writeBuffer.front(), smallFileSize);
	
	// Create the encrypted file
	state int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE |
	            IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_ENCRYPTED | IAsyncFile::OPEN_UNCACHED |
	            IAsyncFile::OPEN_NO_AIO;
	state Reference<IAsyncFile> baseFile =
	    wait(IAsyncFileSystem::filesystem()->open(joinPath(params.getDataDir(), "test-small-encrypted-file"), flags, 0600));
	
	// Write the small file
	wait(baseFile->write(&writeBuffer[0], smallFileSize, 0));
	wait(baseFile->sync());
	
	// Close and reopen for reading to ensure data is persisted
	baseFile.clear();
	
	// flags = IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_ENCRYPTED | 
	//         IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
	state Reference<IAsyncFile> realEncryptedFile =
	    wait(IAsyncFileSystem::filesystem()->open(joinPath(params.getDataDir(), "test-small-encrypted-file"), 0, 0600));
	
	// Wrap the encrypted file with our mock S3-like file that fails on reads beyond file size
	state Reference<MockS3LikeFile> s3LikeFile(new MockS3LikeFile(realEncryptedFile, smallFileSize));
	
	// Wrap with AsyncFileReadAheadCache to reproduce the bug
	// Use typical read-ahead parameters that would cause the issue
	state Reference<AsyncFileReadAheadCache> readAheadFile(new AsyncFileReadAheadCache(
	    s3LikeFile,
	    1024*1024,  // block size - this is the problematic value that causes seeks to 4096
	    0,     // read ahead blocks
	    3,     // max concurrent reads  
	    3     // cache size blocks
	));
	
	// Verify file size is correct
	int64_t fileSize = wait(readAheadFile->size());
	ASSERT_EQ(fileSize, smallFileSize);
	
	// This read should only access the first 50 bytes, but due to the bug,
	// AsyncFileReadAhead will try to read a full 4096-byte block (from offset 0 to 4096),
	// which will cause MockS3LikeFile to throw an io_error() because the read extends
	// beyond the 50-byte file size, simulating S3 behavior
	state std::vector<unsigned char> readBuffer(smallFileSize, 0);
	
	// This should fail due to the bug - AsyncFileReadAhead will attempt to read 4096 bytes
	// starting from offset 0, but our MockS3LikeFile will reject this because it goes beyond
	// the 50-byte file size
	try {
		int bytesRead = wait(readAheadFile->read(&readBuffer[0], smallFileSize, 0));
		// If we get here, the bug is NOT present (the read succeeded)
		ASSERT(false); // This should not be reached if the bug exists
	} catch (Error& e) {
		// Expected: the read should fail due to AsyncFileReadAhead trying to read beyond file size
		ASSERT(e.code() == error_code_io_error);
		printf("SUCCESS: Test correctly reproduced the AsyncFileReadAhead bug - read failed as expected\n");
	}
	
	return Void();
}
