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

IAsyncFile::~IAsyncFile() = default;

const static unsigned int ONE_MEGABYTE = 1 << 20;

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
