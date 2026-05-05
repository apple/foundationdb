/*
 * DiskDurability.cpp
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

#include "fdbserver/tester/workloads.h"
#include "flow/ActorCollection.h"
#include "flow/SystemMonitor.h"
#include "flow/IAsyncFile.h"
#include "AsyncFile.h"

struct DiskDurabilityWorkload : public AsyncFileWorkload {
	static constexpr auto NAME = "DiskDurability";
	struct FileBlock {
		explicit FileBlock(int blockNum) : blockNum(blockNum), lastData(0), lock(new FlowLock(1)) {}
		~FileBlock() = default;
		int blockNum;
		int64_t lastData;
		Reference<FlowLock> lock;

		Future<Void> test_impl(Reference<AsyncFileHandle> file, int pages, Reference<AsyncFileBuffer> buffer) {
			co_await lock->take();

			int64_t offset = (int64_t)blockNum * pages * _PAGE_SIZE;
			int size = pages * _PAGE_SIZE;

			int64_t newData = 0;
			if (lastData == 0)
				newData = deterministicRandom()->randomInt64(std::numeric_limits<int64_t>::min(),
				                                             std::numeric_limits<int64_t>::max());
			else {
				++newData;
				int readBytes = co_await file->file->read(buffer->buffer, size, offset);
				ASSERT(readBytes == size);
			}

			if (newData == 0)
				newData = 1;
			int64_t* arr = (int64_t*)buffer->buffer;
			for (int i = 0, imax = size / sizeof(int64_t); i < imax; ++i) {
				if (lastData != 0 && arr[i] != lastData) {
					TraceEvent(SevError, "WriteWasNotDurable")
					    .detail("Filename", file->path)
					    .detail("Offset", offset)
					    .detail("OpSize", size)
					    .detail("Expected", lastData)
					    .detail("Found", arr[i])
					    .detail("Index", i);
					throw io_error();
				}
				arr[i] = newData;
			}

			co_await file->file->write(buffer->buffer, size, offset);
			lock->release(1);
			lastData = newData;
		}

		Future<Void> test(Reference<AsyncFileHandle> file, int pages, Reference<AsyncFileBuffer> buffer) {
			return test_impl(file, pages, buffer);
		}
	};

	std::vector<FileBlock> blocks;
	int pagesPerWrite;
	int filePages;
	int writers;
	double syncInterval;

	explicit DiskDurabilityWorkload(WorkloadContext const& wcx) : AsyncFileWorkload(wcx) {
		writers = getOption(options, "writers"_sr, 1);
		filePages = getOption(options, "filePages"_sr, 1000000);
		fileSize = filePages * _PAGE_SIZE;
		unbufferedIO = true;
		uncachedIO = true;
		fillRandom = false;
		pagesPerWrite = getOption(options, "pagesPerWrite"_sr, 1);
		syncInterval = (double)(getOption(options, "syncIntervalMs"_sr, 2000)) / 1000;
	}

	~DiskDurabilityWorkload() override = default;

	Future<Void> setup(Database const& cx) override {
		if (enabled)
			return _setup();

		return Void();
	}

	Future<Void> _setup() {
		ASSERT(!path.empty());

		int flags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE;

		if (unbufferedIO)
			flags |= IAsyncFile::OPEN_UNBUFFERED;
		if (uncachedIO)
			flags |= IAsyncFile::OPEN_UNCACHED;

		try {
			Reference<IAsyncFile> file = co_await IAsyncFileSystem::filesystem()->open(path, flags, 0666);
			if (fileHandle.getPtr() == nullptr)
				fileHandle = makeReference<AsyncFileHandle>(file, path, false);
			else
				fileHandle->file = file;
		} catch (Error& error) {
			TraceEvent(SevError, "TestFailure").detail("Reason", "Could not open file");
			throw;
		}
	}

	Future<Void> start(Database const& cx) override {
		if (enabled)
			return _start();

		return Void();
	}

	static unsigned int intHash(unsigned int x) {
		x = ((x >> 16) ^ x) * 0x45d9f3b;
		x = ((x >> 16) ^ x) * 0x45d9f3b;
		x = (x >> 16) ^ x;
		return x;
	}

	Future<Void> worker() {
		auto buffer = makeReference<AsyncFileBuffer>(_PAGE_SIZE, true);
		int logfp = (int)ceil(log2(filePages));
		while (true) {
			int block = intHash(std::min<int>(
			                deterministicRandom()->randomInt(0, 1 << deterministicRandom()->randomInt(0, logfp)),
			                filePages - 1)) %
			            filePages;
			co_await blocks[block].test(fileHandle, pagesPerWrite, buffer);
		}
	}

	Future<Void> syncLoop() {
		while (true) {
			co_await delay(deterministicRandom()->random01() * syncInterval);
			co_await fileHandle->file->sync();
		}
	}

	Future<Void> _start() {
		blocks.reserve(filePages);
		for (int i = 0; i < filePages; ++i)
			blocks.push_back(FileBlock(i));

		std::vector<Future<Void>> tasks;
		tasks.push_back(syncLoop());

		for (int i = 0; i < writers; ++i)
			tasks.push_back(worker());

		co_await timeout(waitForAll(tasks), testDuration, Void());
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<DiskDurabilityWorkload> DiskDurabilityWorkloadFactory;
