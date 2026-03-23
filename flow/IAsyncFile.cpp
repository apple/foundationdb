/*
 * IAsyncFile.cpp
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

#include "flow/IAsyncFile.h"
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/UnitTest.h"
#include <iostream>

IAsyncFile::~IAsyncFile() = default;

const static unsigned int ONE_MEGABYTE = 1 << 20;
const static unsigned int FOUR_KILOBYTES = 4 << 10;

static Future<Void> zeroRangeHelper(Reference<IAsyncFile> f, int64_t offset, int64_t length, int fixedbyte) {
	int64_t pos = offset;
	void* zeros = aligned_alloc(ONE_MEGABYTE, ONE_MEGABYTE);
	memset(zeros, fixedbyte, ONE_MEGABYTE);

	while (pos < offset + length) {
		int len = std::min<int64_t>(ONE_MEGABYTE, offset + length - pos);
		co_await f->write(zeros, len, pos);
		pos += len;
		co_await yield();
	}

	aligned_free(zeros);
}

Future<Void> IAsyncFile::zeroRange(int64_t offset, int64_t length) {
	return uncancellable(zeroRangeHelper(Reference<IAsyncFile>::addRef(this), offset, length, 0));
}

TEST_CASE("/fileio/zero") {
	std::string filename = "/tmp/__ZEROJUNK__";
	Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0);

	// Verify that we can grow a file with zero().
	co_await f->sync();
	co_await f->zeroRange(0, ONE_MEGABYTE);
	int64_t size = co_await f->size();
	ASSERT(ONE_MEGABYTE == size);

	// Verify that zero() does, in fact, zero.
	co_await zeroRangeHelper(f, 0, ONE_MEGABYTE, 0xff);
	co_await f->zeroRange(0, ONE_MEGABYTE);
	uint8_t* page = (uint8_t*)malloc(FOUR_KILOBYTES);
	int n = co_await f->read(page, FOUR_KILOBYTES, 0);
	ASSERT(n == FOUR_KILOBYTES);
	for (int i = 0; i < FOUR_KILOBYTES; i++) {
		ASSERT(page[i] == 0);
	}
	free(page);

	// Destruct our file and remove it.
	f.clear();
	co_await IAsyncFileSystem::filesystem()->deleteFile(filename, true);
}

static Future<Void> incrementalDeleteHelper(std::string filename,
                                            bool mustBeDurable,
                                            int64_t truncateAmt,
                                            double interval) {
	Reference<IAsyncFile> file;
	int64_t remainingFileSize{ 0 };
	bool exists = fileExists(filename);

	if (exists) {
		Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
		    filename, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_UNBUFFERED, 0);
		file = f;

		int64_t fileSize = co_await file->size();
		remainingFileSize = fileSize;
	}

	co_await IAsyncFileSystem::filesystem()->deleteFile(filename, mustBeDurable);

	if (exists) {
		for (; remainingFileSize > 0; remainingFileSize -= truncateAmt) {
			co_await file->truncate(remainingFileSize);
			co_await file->sync();
			co_await delay(interval);
		}
	}
}

Future<Void> IAsyncFileSystem::incrementalDeleteFile(const std::string& filename, bool mustBeDurable) {
	return uncancellable(incrementalDeleteHelper(filename,
	                                             mustBeDurable,
	                                             FLOW_KNOBS->INCREMENTAL_DELETE_TRUNCATE_AMOUNT,
	                                             FLOW_KNOBS->INCREMENTAL_DELETE_INTERVAL));
}

TEST_CASE("/fileio/incrementalDelete") {
	// about 5GB
	int64_t fileSize = 5e9;
	std::string filename = "/tmp/__JUNK__";
	Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0);
	co_await f->sync();
	co_await f->truncate(fileSize);
	// close the file by deleting the reference
	f.clear();
	co_await IAsyncFileSystem::filesystem()->incrementalDeleteFile(filename, true);
}

TEST_CASE("/fileio/rename") {
	// create a file
	int64_t fileSize = 100e6;
	std::string filename = "/tmp/__JUNK__." + deterministicRandom()->randomUniqueID().toString();
	std::string renamedFile = "/tmp/__RENAMED_JUNK__." + deterministicRandom()->randomUniqueID().toString();
	std::unique_ptr<char[]> data(new char[4096]);
	std::unique_ptr<char[]> readData(new char[4096]);
	Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
	    filename,
	    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE,
	    0644);
	co_await f->sync();
	co_await f->truncate(fileSize);
	memset(data.get(), 0, 4096);
	// write a random string at the beginning of the file which we can verify after rename
	for (int i = 0; i < 16; ++i) {
		data[i] = deterministicRandom()->randomAlphaNumeric();
	}
	// write first and block
	co_await f->write(data.get(), 4096, 0);
	co_await f->write(data.get(), 4096, fileSize - 4096);
	co_await f->sync();
	// close file
	f.clear();
	co_await IAsyncFileSystem::filesystem()->renameFile(filename, renamedFile);
	f = co_await IAsyncFileSystem::filesystem()->open(renamedFile, IAsyncFile::OPEN_READONLY, 0);

	// verify rename happened
	bool renamedExists = false;
	auto bName = basename(renamedFile);
	auto files = platform::listFiles("/tmp/");
	for (const auto& file : files) {
		if (file == bName) {
			renamedExists = true;
		}
		ASSERT(file != filename);
	}
	ASSERT(renamedExists);

	// verify magic string at beginning of file
	int length = co_await f->read(readData.get(), 4096, 0);
	ASSERT(length == 4096);
	ASSERT(memcmp(readData.get(), data.get(), 4096) == 0);
	// close the file
	f.clear();

	// clean up
	co_await IAsyncFileSystem::filesystem()->deleteFile(renamedFile, true);
}

// Truncating to extend size should zero the new data
TEST_CASE("/fileio/truncateAndRead") {
	std::string filename = "/tmp/__JUNK__";
	Reference<IAsyncFile> f = co_await IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0);
	std::array<char, 4096> data;
	co_await f->sync();
	co_await f->truncate(4096);
	int length = co_await f->read(&data[0], 4096, 0);
	ASSERT(length == 4096);
	for (auto c : data) {
		ASSERT(c == '\0');
	}
	// close the file by deleting the reference
	f.clear();
	co_await IAsyncFileSystem::filesystem()->incrementalDeleteFile(filename, true);
}
