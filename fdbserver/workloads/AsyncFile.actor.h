/*
 * AsyncFile.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(WORKLOADS_ASYNCFILE_ACTOR_G_H)
#define WORKLOADS_ASYNCFILE_ACTOR_G_H
#include "fdbserver/workloads/AsyncFile.actor.g.h"
#elif !defined(WORKLOADS_ASYNCFILE_ACTOR_H)
#define WORKLOADS_ASYNCFILE_ACTOR_H

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class RandomByteGenerator {
private:
	char* b1;
	int BUF_SIZE;

public:
	RandomByteGenerator();
	~RandomByteGenerator();
	void writeRandomBytesToBuffer(void* buf, int bytes);
};

struct AsyncFileBuffer : public ReferenceCounted<AsyncFileBuffer> {
	AsyncFileBuffer(size_t size, bool aligned);
	virtual ~AsyncFileBuffer();

	unsigned char* buffer;
	bool aligned;
};

struct AsyncFileHandle : public ReferenceCounted<AsyncFileHandle> {
	AsyncFileHandle(Reference<IAsyncFile> file, std::string path, bool temporary);
	virtual ~AsyncFileHandle();

	Reference<IAsyncFile> file;
	std::string path;
	bool temporary;
};

struct AsyncFileWorkload : TestWorkload {
	static const int _PAGE_SIZE;

	// If true, then the underlying AsyncFile will be assumed to be performing unbuffered IO, which requires special
	// alignments
	bool unbufferedIO;
	bool uncachedIO;
	bool fillRandom;
	bool enabled;
	double testDuration;

	Reference<AsyncFileHandle> fileHandle;
	int64_t fileSize;

	std::string path;

	AsyncFileWorkload(WorkloadContext const&);
	~AsyncFileWorkload() override {}

	// Allocates a buffer of a given size.  If necessary, the buffer will be aligned to 4K
	Reference<AsyncFileBuffer> allocateBuffer(size_t size);

	Future<bool> check(Database const& cx) override;

	// Opens a file for AsyncFile operations.  If the path is empty, then creates a file and fills it with random data
	ACTOR Future<Void> openFile(AsyncFileWorkload* self,
	                            int64_t flags,
	                            int64_t mode,
	                            uint64_t size,
	                            bool fillFile = false) {
		state RandomByteGenerator rbg;

		if (self->fileHandle.getPtr() != nullptr) {
			self->fileHandle->file = Reference<IAsyncFile>(nullptr);
			wait(delay(0.1));
		}

		state bool fileCreated = self->path.length() == 0;
		if (fileCreated) {
			self->path = "asyncfile." + deterministicRandom()->randomUniqueID().toString();
			flags &= ~IAsyncFile::OPEN_READONLY;
			flags |= IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE;
		} else if (fillFile) {
			flags &= ~IAsyncFile::OPEN_READONLY;
			flags |= IAsyncFile::OPEN_READWRITE;
		}

		if (self->unbufferedIO)
			flags |= IAsyncFile::OPEN_UNBUFFERED;
		if (self->uncachedIO)
			flags |= IAsyncFile::OPEN_UNCACHED;

		try {
			state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(self->path, flags, 0666));
			if (self->fileHandle.getPtr() == nullptr)
				self->fileHandle = makeReference<AsyncFileHandle>(file, self->path, fileCreated);
			else
				self->fileHandle->file = file;

			if (fileCreated || fillFile) {
				state int64_t oldSize = wait(file->size());
				state int64_t newSize = (size + _PAGE_SIZE - 1) & ~(int64_t(_PAGE_SIZE - 1)); // align size up

				if (!fileCreated)
					wait(file->truncate(newSize));

				state int chunkSize = 4 << 16;
				state Reference<AsyncFileBuffer> data = self->allocateBuffer(chunkSize);
				state int64_t i;
				state Future<Void> lastWrite = Void();
				for (i = oldSize & ~(chunkSize - 1); i < newSize; i += chunkSize) {
					if (i >> 30 != (i + (chunkSize)) >> 30) // each GB
						printf("Building test file: %d GB\n", int((i + (chunkSize)) >> 30));
					if (self->fillRandom)
						rbg.writeRandomBytesToBuffer(data->buffer, chunkSize);
					auto w = lastWrite;
					lastWrite = file->write(data->buffer, chunkSize, i);
					wait(w);
				}
				wait(lastWrite);
			}
		} catch (Error& error) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "Could not open file");
			throw;
		}

		return Void();
	}
};

#include "flow/unactorcompiler.h"
#endif
