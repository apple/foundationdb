/*
 * AsyncFileWrite.actor.cpp
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

struct AsyncFileWriteWorkload : public AsyncFileWorkload {
	// Buffer used to store what is being written
	Reference<AsyncFileBuffer> writeBuffer;

	// The futures for the asynchronous write futures
	std::vector<Future<Void>> writeFutures;

	// Number of writes to perform in parallel.  Write tests are performed only if this is greater than zero and
	// numParallelReads is zero
	int numParallelWrites;

	// The number of bytes written in each call of write
	int writeSize;

	// Whether or not writes should be performed sequentially
	bool sequential;

	double averageCpuUtilization;
	PerfIntCounter bytesWritten;

	AsyncFileWriteWorkload(WorkloadContext const& wcx)
	  : AsyncFileWorkload(wcx), writeBuffer(nullptr), bytesWritten("Bytes Written") {
		numParallelWrites = getOption(options, LiteralStringRef("numParallelWrites"), 0);
		writeSize = getOption(options, LiteralStringRef("writeSize"), _PAGE_SIZE);
		fileSize = getOption(options, LiteralStringRef("fileSize"), 10002432);
		sequential = getOption(options, LiteralStringRef("sequential"), true);
	}

	std::string description() const override { return "AsyncFileWrite"; }

	Future<Void> setup(Database const& cx) override {
		if (enabled)
			return _setup(this);

		return Void();
	}

	ACTOR Future<Void> _setup(AsyncFileWriteWorkload* self) {
		// Allow only 4K aligned writes if using unbuffered IO
		if (self->unbufferedIO && self->writeSize % AsyncFileWorkload::_PAGE_SIZE != 0)
			self->writeSize = std::max(AsyncFileWorkload::_PAGE_SIZE,
			                           self->writeSize - self->writeSize % AsyncFileWorkload::_PAGE_SIZE);

		// Allocate the write buffer
		self->writeBuffer = self->allocateBuffer(self->writeSize);

		int64_t initialSize = self->fileSize;
		if (self->sequential)
			initialSize = 0;

		wait(self->openFile(self, IAsyncFile::OPEN_READWRITE, 0666, initialSize));

		int64_t fileSize = wait(self->fileHandle->file->size());
		if (fileSize != 0)
			self->fileSize = fileSize;

		return Void();
	}

	Future<Void> start(Database const& cx) override {
		if (enabled)
			return _start(this);

		return Void();
	}

	ACTOR Future<Void> _start(AsyncFileWriteWorkload* self) {
		state StatisticsState statState;
		customSystemMonitor("AsyncFile Metrics", &statState);

		wait(timeout(self->runWriteTest(self), self->testDuration, Void()));

		SystemStatistics stats = customSystemMonitor("AsyncFile Metrics", &statState);
		self->averageCpuUtilization = stats.processCPUSeconds / stats.elapsed;

		// Try to let the IO complete so we can clean up after them
		wait(timeout(waitForAll(self->writeFutures), 10, Void()));

		return Void();
	}

	ACTOR Future<Void> runWriteTest(AsyncFileWriteWorkload* self) {
		state int64_t offset = self->fileSize;
		state Future<Void> prevSync = Void();
		loop {
			// Write chunks of the file using different actors
			for (int i = 0; i < self->numParallelWrites; i++) {
				// Perform the write.  Don't allow it to be cancelled (because the underlying IO may not be cancellable)
				// and don't allow objects that the write uses to be deleted
				self->writeFutures.push_back(uncancellable(holdWhile(
				    self->fileHandle,
				    holdWhile(self->writeBuffer,
				              self->fileHandle->file->write(self->writeBuffer->buffer,
				                                            std::min((int64_t)self->writeSize, self->fileSize - offset),
				                                            offset)))));

				if (self->sequential) {
					offset += self->writeSize;

					// If the file is exhausted, start over at the beginning
					if (offset >= self->fileSize)
						offset = 0;
				} else if (self->unbufferedIO)
					offset = (int64_t)(deterministicRandom()->random01() * (self->fileSize - 1) /
					                   AsyncFileWorkload::_PAGE_SIZE) *
					         AsyncFileWorkload::_PAGE_SIZE;
				else
					offset = (int64_t)(deterministicRandom()->random01() * (self->fileSize - 1));
			}

			wait(waitForAll(self->writeFutures));
			wait(prevSync);
			prevSync = self->fileHandle->file->sync();

			self->writeFutures.clear();

			self->bytesWritten += self->writeSize * self->numParallelWrites;
		}
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (enabled) {
			m.emplace_back("Bytes written/sec", bytesWritten.getValue() / testDuration, Averaged::False);
			m.emplace_back("Average CPU Utilization (Percentage)", averageCpuUtilization * 100, Averaged::False);
		}
	}
};

WorkloadFactory<AsyncFileWriteWorkload> AsyncFileWriteWorkloadFactory("AsyncFileWrite");
