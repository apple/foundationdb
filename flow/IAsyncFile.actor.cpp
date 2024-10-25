/*
 * IAsyncFile.actor.cpp
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

#include "flow/IAsyncFile.h"
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/UnitTest.h"
#include <iostream>
#include "flow/actorcompiler.h" // has to be last include

IAsyncFile::~IAsyncFile() = default;

const static unsigned int ONE_MEGABYTE = 1 << 20;
const static unsigned int FOUR_KILOBYTES = 4 << 10;

ACTOR static Future<Void> zeroRangeHelper(Reference<IAsyncFile> f, int64_t offset, int64_t length, int fixedbyte) {
	state int64_t pos = offset;
	state void* zeros = aligned_alloc(ONE_MEGABYTE, ONE_MEGABYTE);
	memset(zeros, fixedbyte, ONE_MEGABYTE);

	while (pos < offset + length) {
		state int len = std::min<int64_t>(ONE_MEGABYTE, offset + length - pos);
		wait(f->write(zeros, len, pos));
		pos += len;
		wait(yield());
	}

	aligned_free(zeros);
	return Void();
}

Future<Void> IAsyncFile::zeroRange(int64_t offset, int64_t length) {
	return uncancellable(zeroRangeHelper(Reference<IAsyncFile>::addRef(this), offset, length, 0));
}

TEST_CASE("/fileio/zero") {
	state std::string filename = "/tmp/__ZEROJUNK__";
	state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0));

	// Verify that we can grow a file with zero().
	wait(f->sync());
	wait(f->zeroRange(0, ONE_MEGABYTE));
	int64_t size = wait(f->size());
	ASSERT(ONE_MEGABYTE == size);

	// Verify that zero() does, in fact, zero.
	wait(zeroRangeHelper(f, 0, ONE_MEGABYTE, 0xff));
	wait(f->zeroRange(0, ONE_MEGABYTE));
	state uint8_t* page = (uint8_t*)malloc(FOUR_KILOBYTES);
	int n = wait(f->read(page, FOUR_KILOBYTES, 0));
	ASSERT(n == FOUR_KILOBYTES);
	for (int i = 0; i < FOUR_KILOBYTES; i++) {
		ASSERT(page[i] == 0);
	}
	free(page);

	// Destruct our file and remove it.
	f.clear();
	wait(IAsyncFileSystem::filesystem()->deleteFile(filename, true));
	return Void();
}

ACTOR static Future<Void> incrementalDeleteHelper(std::string filename,
                                                  bool mustBeDurable,
                                                  int64_t truncateAmt,
                                                  double interval) {
	state Reference<IAsyncFile> file;
	state int64_t remainingFileSize;
	state bool exists = fileExists(filename);

	if (exists) {
		Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
		    filename, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_UNBUFFERED, 0));
		file = f;

		int64_t fileSize = wait(file->size());
		remainingFileSize = fileSize;
	}

	wait(IAsyncFileSystem::filesystem()->deleteFile(filename, mustBeDurable));

	if (exists) {
		for (; remainingFileSize > 0; remainingFileSize -= truncateAmt) {
			wait(file->truncate(remainingFileSize));
			wait(file->sync());
			wait(delay(interval));
		}
	}

	return Void();
}

Future<Void> IAsyncFileSystem::incrementalDeleteFile(const std::string& filename, bool mustBeDurable) {
	return uncancellable(incrementalDeleteHelper(filename,
	                                             mustBeDurable,
	                                             FLOW_KNOBS->INCREMENTAL_DELETE_TRUNCATE_AMOUNT,
	                                             FLOW_KNOBS->INCREMENTAL_DELETE_INTERVAL));
}

TEST_CASE("/fileio/incrementalDelete") {
	// about 5GB
	state int64_t fileSize = 5e9;
	state std::string filename = "/tmp/__JUNK__";
	state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0));
	wait(f->sync());
	wait(f->truncate(fileSize));
	// close the file by deleting the reference
	f.clear();
	wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(filename, true));
	return Void();
}

TEST_CASE("/fileio/rename") {
	// create a file
	state int64_t fileSize = 100e6;
	state std::filesystem::path filename =
	    std::filesystem::path("/tmp/__JUNK__." + deterministicRandom()->randomUniqueID().toString());
	state std::filesystem::path renamedFile =
	    std::filesystem::path("/tmp/__RENAMED_JUNK__." + deterministicRandom()->randomUniqueID().toString());
	state std::unique_ptr<char[]> data(new char[4096]);
	state std::unique_ptr<char[]> readData(new char[4096]);
	state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
	    filename,
	    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE,
	    0644));
	;
	wait(f->sync());
	wait(f->truncate(fileSize));
	memset(data.get(), 0, 4096);
	// write a random string at the beginning of the file which we can verify after rename
	for (int i = 0; i < 16; ++i) {
		data[i] = deterministicRandom()->randomAlphaNumeric();
	}
	// write first and block
	wait(f->write(data.get(), 4096, 0));
	wait(f->write(data.get(), 4096, fileSize - 4096));
	wait(f->sync());
	// close file
	f.clear();
	wait(IAsyncFileSystem::filesystem()->renameFile(filename, renamedFile));
	Reference<IAsyncFile> _f = wait(IAsyncFileSystem::filesystem()->open(renamedFile, IAsyncFile::OPEN_READONLY, 0));
	f = _f;

	// verify rename happened
	bool renamedExists = false;
	auto bName = renamedFile.filename();
	auto files = platform::listFiles("/tmp/");
	for (const auto& file : files) {
		if (file == bName) {
			renamedExists = true;
		}
		ASSERT(file != filename);
	}
	ASSERT(renamedExists);

	// verify magic string at beginning of file
	int length = wait(f->read(readData.get(), 4096, 0));
	ASSERT(length == 4096);
	ASSERT(memcmp(readData.get(), data.get(), 4096) == 0);
	// close the file
	f.clear();

	// clean up
	wait(IAsyncFileSystem::filesystem()->deleteFile(renamedFile, true));
	return Void();
}

// Truncating to extend size should zero the new data
TEST_CASE("/fileio/truncateAndRead") {
	state std::string filename = "/tmp/__JUNK__";
	state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(
	    filename, IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE, 0));
	state std::array<char, 4096> data;
	wait(f->sync());
	wait(f->truncate(4096));
	int length = wait(f->read(&data[0], 4096, 0));
	ASSERT(length == 4096);
	for (auto c : data) {
		ASSERT(c == '\0');
	}
	// close the file by deleting the reference
	f.clear();
	wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(filename, true));
	return Void();
}
