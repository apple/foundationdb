/*
 * DiskDurability.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "flow/SystemMonitor.h"
#include "fdbrpc/IAsyncFile.h"
#include "fdbserver/workloads/AsyncFile.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct DiskDurabilityWorkload : public AsyncFileWorkload {
	struct FileBlock {
		FileBlock(int blockNum) : blockNum(blockNum), lastData(0), lock(new FlowLock(1)) {}
		~FileBlock() {}
		int blockNum;
		int64_t lastData;
		Reference<FlowLock> lock;

		ACTOR static Future<Void> test_impl(FileBlock* self,
		                                    Reference<AsyncFileHandle> file,
		                                    int pages,
		                                    Reference<AsyncFileBuffer> buffer) {
			wait(self->lock->take());

			state int64_t offset = (int64_t)self->blockNum * pages * _PAGE_SIZE;
			state int size = pages * _PAGE_SIZE;

			state int64_t newData;
			if (self->lastData == 0)
				newData = deterministicRandom()->randomInt64(std::numeric_limits<int64_t>::min(),
				                                             std::numeric_limits<int64_t>::max());
			else {
				++newData;
				int readBytes = wait(file->file->read(buffer->buffer, size, offset));
				ASSERT(readBytes == size);
			}

			if (newData == 0)
				newData = 1;
			int64_t* arr = (int64_t*)buffer->buffer;
			for (int i = 0, imax = size / sizeof(int64_t); i < imax; ++i) {
				if (self->lastData != 0 && arr[i] != self->lastData) {
					TraceEvent(SevError, "WriteWasNotDurable")
					    .detail("Filename", file->path)
					    .detail("Offset", offset)
					    .detail("OpSize", size)
					    .detail("Expected", self->lastData)
					    .detail("Found", arr[i])
					    .detail("Index", i);
					throw io_error();
				}
				arr[i] = newData;
			}

			wait(file->file->write(buffer->buffer, size, offset));
			self->lock->release(1);
			self->lastData = newData;
			return Void();
		}

		Future<Void> test(Reference<AsyncFileHandle> file, int pages, Reference<AsyncFileBuffer> buffer) {
			return test_impl(this, file, pages, buffer);
		}
	};

	std::vector<FileBlock> blocks;
	int pagesPerWrite;
	int filePages;
	int writers;
	double syncInterval;

	DiskDurabilityWorkload(WorkloadContext const& wcx) : AsyncFileWorkload(wcx) {
		writers = getOption(options, LiteralStringRef("writers"), 1);
		filePages = getOption(options, LiteralStringRef("filePages"), 1000000);
		fileSize = filePages * _PAGE_SIZE;
		unbufferedIO = true;
		uncachedIO = true;
		fillRandom = false;
		pagesPerWrite = getOption(options, LiteralStringRef("pagesPerWrite"), 1);
		syncInterval = (double)(getOption(options, LiteralStringRef("syncIntervalMs"), 2000)) / 1000;
	}

	~DiskDurabilityWorkload() override {}

	std::string description() const override { return "DiskDurability"; }

	Future<Void> setup(Database const& cx) override {
		if (enabled)
			return _setup(this);

		return Void();
	}

	ACTOR Future<Void> _setup(DiskDurabilityWorkload* self) {
		ASSERT(!self->path.empty());

		int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE;

		if (self->unbufferedIO)
			flags |= IAsyncFile::OPEN_UNBUFFERED;
		if (self->uncachedIO)
			flags |= IAsyncFile::OPEN_UNCACHED;

		try {
			state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(self->path, flags, 0666));
			if (self->fileHandle.getPtr() == nullptr)
				self->fileHandle = makeReference<AsyncFileHandle>(file, self->path, false);
			else
				self->fileHandle->file = file;
		} catch (Error& error) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "Could not open file");
			throw;
		}

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (enabled)
			return _start(this);

		return Void();
	}

	static unsigned int intHash(unsigned int x) {
		x = ((x >> 16) ^ x) * 0x45d9f3b;
		x = ((x >> 16) ^ x) * 0x45d9f3b;
		x = (x >> 16) ^ x;
		return x;
	}

	ACTOR static Future<Void> worker(DiskDurabilityWorkload* self) {
		state Reference<AsyncFileBuffer> buffer = makeReference<AsyncFileBuffer>(_PAGE_SIZE, true);
		state int logfp = (int)ceil(log2(self->filePages));
		loop {
			int block = intHash(std::min<int>(
			                deterministicRandom()->randomInt(0, 1 << deterministicRandom()->randomInt(0, logfp)),
			                self->filePages - 1)) %
			            self->filePages;
			wait(self->blocks[block].test(self->fileHandle, self->pagesPerWrite, buffer));
		}
	}

	ACTOR static Future<Void> syncLoop(DiskDurabilityWorkload* self) {
		loop {
			wait(delay(deterministicRandom()->random01() * self->syncInterval));
			wait(self->fileHandle->file->sync());
		}
	}

	ACTOR Future<Void> _start(DiskDurabilityWorkload* self) {
		self->blocks.reserve(self->filePages);
		for (int i = 0; i < self->filePages; ++i)
			self->blocks.push_back(FileBlock(i));

		state std::vector<Future<Void>> tasks;
		tasks.push_back(syncLoop(self));

		for (int i = 0; i < self->writers; ++i)
			tasks.push_back(worker(self));

		wait(timeout(waitForAll(tasks), self->testDuration, Void()));

		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DiskDurabilityWorkload> DiskDurabilityWorkloadFactory("DiskDurability");
