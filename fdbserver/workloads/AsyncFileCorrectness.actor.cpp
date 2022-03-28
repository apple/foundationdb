/*
 * AsyncFileCorrectness.actor.cpp
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

#include <cinttypes>

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "flow/IRandom.h"
#include "flow/SystemMonitor.h"
#include "fdbserver/workloads/AsyncFile.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// An enumeration representing the type of operation to be performed in a correctness test operation
enum OperationType { READ, WRITE, SYNC, REOPEN, TRUNCATE };

// Stores information about an operation that is executed on the file
struct OperationInfo {
	Reference<AsyncFileBuffer> data;

	uint64_t offset;
	uint64_t length;

	bool flushOperations;
	OperationType operation;
	int index;
};

struct AsyncFileCorrectnessWorkload : public AsyncFileWorkload {
	// Maximum number of bytes operated on by a file operation
	int maxOperationSize;

	// The number of simultaneous outstanding operations on a file
	int numSimultaneousOperations;

	// The futures for asynchronous IO operations
	std::vector<Future<OperationInfo>> operations;

	// Our in memory representation of what the file should be
	Reference<AsyncFileBuffer> memoryFile;

	// A vector holding a lock for each byte in the file. 0xFFFFFFFF means that the byte is being written, any other
	// number means that it is being read that many times
	std::vector<uint32_t> fileLock;

	// A mask designating whether each byte in the file has been explicitly written (bytes which weren't explicitly
	// written have no guarantees about content)
	std::vector<unsigned char> fileValidityMask;

	// Whether or not the correctness test succeeds
	bool success;

	// The targetted size of the file (the actual file can be anywhere in size from 1 byte to 2 * targetFileSize)
	int64_t targetFileSize;

	double averageCpuUtilization;
	PerfIntCounter numOperations;

	AsyncFileCorrectnessWorkload(WorkloadContext const& wcx)
	  : AsyncFileWorkload(wcx), memoryFile(nullptr), success(true), numOperations("Num Operations") {
		maxOperationSize = getOption(options, LiteralStringRef("maxOperationSize"), 4096);
		numSimultaneousOperations = getOption(options, LiteralStringRef("numSimultaneousOperations"), 10);
		targetFileSize = getOption(options, LiteralStringRef("targetFileSize"), (uint64_t)163840);

		if (unbufferedIO)
			maxOperationSize = std::max(_PAGE_SIZE, maxOperationSize);

		if (maxOperationSize * numSimultaneousOperations > targetFileSize * 0.25) {
			targetFileSize *= (int)ceil((maxOperationSize * numSimultaneousOperations * 4.0) / targetFileSize);
			fmt::print(
			    "Target file size is insufficient to support {0} simultaneous operations of size {1}; changing to "
			    "{2}\n",
			    numSimultaneousOperations,
			    maxOperationSize,
			    targetFileSize);
		}
	}

	~AsyncFileCorrectnessWorkload() override {}

	std::string description() const override { return "AsyncFileCorrectness"; }

	Future<Void> setup(Database const& cx) override {
		if (enabled)
			return _setup(this);

		return Void();
	}

	ACTOR Future<Void> _setup(AsyncFileCorrectnessWorkload* self) {
		// Create the memory version of the file, the file locks, and the valid mask
		self->memoryFile = self->allocateBuffer(self->targetFileSize);
		self->fileLock.resize(self->targetFileSize, 0);
		self->fileValidityMask.resize(self->targetFileSize, 0);
		self->fileSize = 0;

		// Create or open the file being used for testing
		wait(self->openFile(self, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE, 0666, self->fileSize, true));

		return Void();
	}

	// Updates the memory buffer, locks, and validity mask to a new file size
	void updateMemoryBuffer(int64_t newFileSize) {
		int64_t oldBufferSize = std::max(fileSize, targetFileSize);
		int64_t newBufferSize = std::max(newFileSize, targetFileSize);

		if (oldBufferSize != newBufferSize) {
			Reference<AsyncFileBuffer> newFile = allocateBuffer(newBufferSize);
			memcpy(newFile->buffer, memoryFile->buffer, std::min(newBufferSize, oldBufferSize));

			if (newBufferSize > oldBufferSize)
				memset(&newFile->buffer[oldBufferSize], 0, newBufferSize - oldBufferSize);

			memoryFile = newFile;

			fileLock.resize(newBufferSize, 0);
			fileValidityMask.resize(newBufferSize, 0xFF);
		}

		fileSize = newFileSize;
	}

	Future<Void> start(Database const& cx) override {
		if (enabled)
			return _start(this);

		return Void();
	}

	ACTOR Future<Void> _start(AsyncFileCorrectnessWorkload* self) {
		state StatisticsState statState;
		customSystemMonitor("AsyncFile Metrics", &statState);

		wait(timeout(self->runCorrectnessTest(self), self->testDuration, Void()));

		SystemStatistics stats = customSystemMonitor("AsyncFile Metrics", &statState);
		self->averageCpuUtilization = stats.processCPUSeconds / stats.elapsed;

		// Try to let the IO operations finish so we can clean up after them
		wait(timeout(waitForAll(self->operations), 10, Void()));

		return Void();
	}

	ACTOR Future<Void> runCorrectnessTest(AsyncFileCorrectnessWorkload* self) {
		state std::vector<OperationInfo> postponedOperations;
		state int validOperations = 0;

		loop {
			wait(delay(0));

			// Fill the operations buffer with random operations
			while (self->operations.size() < self->numSimultaneousOperations && postponedOperations.size() == 0) {
				self->operations.push_back(
				    self->processOperation(self, self->generateOperation(self->operations.size(), false)));
				validOperations++;
			}

			// Get the first operation that finishes
			OperationInfo info = wait(waitForFirst(self->operations));

			// If it is a read, check that it matches what our memory representation has
			if (info.operation == READ) {
				int start = 0;
				bool isValid = true;
				int length = std::min(info.length, self->fileLock.size() - info.offset);

				// Scan the entire read range for sections that we know (fileValidityMask > 0) and those that we don't
				for (int i = 0; i < length; i++) {
					bool currentValid = self->fileValidityMask[i] > 0;
					if (start == 0)
						isValid = currentValid;
					else if (isValid != currentValid || i == length - 1) {
						// If we know what data should be in a particular range, then compare the result with what we
						// know
						if (isValid && memcmp(&self->fileValidityMask[info.offset + start],
						                      &info.data->buffer[start],
						                      i - start)) {
							printf("Read returned incorrect results at %" PRIu64 " of length %" PRIu64 "\n",
							       info.offset,
							       info.length);

							self->success = false;
							return Void();
						}
						// Otherwise, skip the comparison and just update what we know
						else if (!isValid) {
							memcpy(
							    &self->memoryFile->buffer[info.offset + start], &info.data->buffer[start], i - start);
							memset(&self->fileValidityMask[info.offset + start], 0xFF, i - start);
						}

						start = i;
					}

					isValid = currentValid;
				}

				// Decrement the read count for each byte that was read
				int lockEnd = std::min(info.offset + info.length, (uint64_t)self->fileLock.size());
				if (lockEnd > self->fileSize)
					lockEnd = self->fileLock.size();

				for (int i = info.offset; i < lockEnd; i++)
					self->fileLock[i]--;
			}

			// If it is a write, clear the write locks
			else if (info.operation == WRITE)
				memset(&self->fileLock[info.offset], 0, info.length * sizeof(uint32_t));

			// Only generate new operations if we don't have a postponed operation in queue
			if (postponedOperations.size() == 0) {
				// Insert a new operation into the operations buffer
				OperationInfo newOperation = self->generateOperation(info.index);

				// If we need to flush existing operations, postpone this operation
				if (newOperation.flushOperations)
					postponedOperations.push_back(newOperation);
				// Otherwise, add it to our operations queue
				else
					self->operations[info.index] = self->processOperation(self, newOperation);
			}

			// If there is a postponed operation, clear the queue so that we can run it
			if (postponedOperations.size() > 0) {
				self->operations[info.index] = Never();
				validOperations--;
			}

			// If there are no operations being processed and postponed operations are waiting, run them now
			while (validOperations == 0 && postponedOperations.size() > 0) {
				self->operations.clear();
				self->operations.push_back(self->processOperation(self, postponedOperations.front()));
				OperationInfo info = wait(self->operations.front());
				postponedOperations.erase(postponedOperations.begin());
				self->operations.clear();
			}
		}
	}

	// Generates a random operation
	OperationInfo generateOperation(int index, bool allowFlushingOperations = true) {
		OperationInfo info;

		do {
			info.flushOperations = false;

			// Cumulative density function for the different operations
			int cdfArray[] = { 0, 1000, 2000, 2100, 2101, 2102 };
			std::vector<int> cdf = std::vector<int>(cdfArray, cdfArray + 6);

			// Choose a random operation type (READ, WRITE, SYNC, REOPEN, TRUNCATE).
			int random = deterministicRandom()->randomInt(0, cdf.back());
			for (int i = 0; i < cdf.size() - 1; i++) {
				if (cdf[i] <= random && random < cdf[i + 1]) {
					info.operation = (OperationType)i;
					break;
				}
			}

			if (info.operation == READ || info.operation == WRITE) {
				int64_t maxOffset;

				// Reads should not exceed the extent of written data
				if (info.operation == READ) {
					maxOffset = fileSize - 1;
					if (maxOffset < 0)
						info.operation = WRITE;
					// Only allow reads once the file has gotten large enough (to prevent blocking on locks)
					if (maxOffset < targetFileSize / 2)
						info.operation = WRITE;
				}

				// Writes can be up to the target file size or the current file size (the current file size could be
				// larger than the target as a result of a truncate)
				if (info.operation == WRITE)
					maxOffset = std::max(fileSize, targetFileSize) - 1;

				// Choose a random offset and length, retrying if that section is already locked
				do {
					// Generate random length and offset
					if (unbufferedIO) {
						info.length =
						    deterministicRandom()->randomInt(1, maxOperationSize / _PAGE_SIZE + 1) * _PAGE_SIZE;
						info.offset =
						    (int64_t)(deterministicRandom()->random01() * maxOffset / _PAGE_SIZE) * _PAGE_SIZE;
					} else {
						info.length = deterministicRandom()->randomInt(1, maxOperationSize);
						info.offset = (int64_t)(deterministicRandom()->random01() * maxOffset);
					}

				} while (checkFileLocked(info.operation, info.offset, info.length));

				// If the operation is a read, increment the read count for each byte
				if (info.operation == READ) {
					// If the read extends past the end of the file, then we have to lock all bytes beyond the end of
					// the file This is so that we can accurately determine if the read count is correct
					int lockEnd = std::min(info.offset + info.length, (uint64_t)fileLock.size());
					if (lockEnd > fileSize)
						lockEnd = fileLock.size();

					for (int i = info.offset; i < lockEnd; i++)
						fileLock[i]++;
				}

				// If the operation is a write, set the write lock for each byte
				else if (info.operation == WRITE) {
					// Don't write past the end of the file
					info.length = std::min(info.length, std::max(targetFileSize, fileSize) - info.offset);
					memset(&fileLock[info.offset], 0xFF, info.length * sizeof(uint32_t));
				}
			} else if (info.operation == REOPEN)
				info.flushOperations = true;
			else if (info.operation == TRUNCATE) {
				info.flushOperations = true;

				// Choose a random length to truncate to
				if (unbufferedIO)
					info.offset =
					    (int64_t)(deterministicRandom()->random01() * (2 * targetFileSize) / _PAGE_SIZE) * _PAGE_SIZE;
				else
					info.offset = (int64_t)(deterministicRandom()->random01() * (2 * targetFileSize));
			}

		} while (!allowFlushingOperations && info.flushOperations);

		info.index = index;
		return info;
	}

	// Checks if a file is already locked for a given set of bytes.  The file is locked if it is being written
	// (fileLock[i] = 0xFFFFFFFF) or if we are trying to perform a write and the read count is nonzero (fileLock[i] !=
	// 0)
	bool checkFileLocked(int operation, int offset, int length) const {
		for (int i = offset; i < offset + length && i < fileLock.size(); i++)
			if (fileLock[i] == 0xFFFFFFFF || (fileLock[i] != 0 && operation == WRITE))
				return true;

		return false;
	}

	// Performs an operation on a file and the memory representation of that file
	ACTOR Future<OperationInfo> processOperation(AsyncFileCorrectnessWorkload* self, OperationInfo info) {
		if (info.operation == READ) {
			info.data = self->allocateBuffer(info.length);

			// Perform the read.  Don't allow it to be cancelled (because the underlying IO may not be cancellable) and
			// don't allow objects that the read uses to be deleted
			int numRead = wait(uncancellable(
			    holdWhile(self->fileHandle,
			              holdWhile(info, self->fileHandle->file->read(info.data->buffer, info.length, info.offset)))));

			if (numRead != std::min(info.length, self->fileSize - info.offset)) {
				printf("Read reported incorrect number of bytes at %" PRIu64 " of length %" PRIu64 "\n",
				       info.offset,
				       info.length);
				self->success = false;
			}
		} else if (info.operation == WRITE) {
			info.data = self->allocateBuffer(info.length);
			generateRandomData(reinterpret_cast<uint8_t*>(info.data->buffer), info.length);
			memcpy(&self->memoryFile->buffer[info.offset], info.data->buffer, info.length);
			memset(&self->fileValidityMask[info.offset], 0xFF, info.length);

			// Perform the write.  Don't allow it to be cancelled (because the underlying IO may not be cancellable) and
			// don't allow objects that the write uses to be deleted
			wait(uncancellable(holdWhile(
			    self->fileHandle,
			    holdWhile(info, self->fileHandle->file->write(info.data->buffer, info.length, info.offset)))));

			// If we wrote past the end of the file, update the size of the file
			self->fileSize = std::max((int64_t)(info.offset + info.length), self->fileSize);
		} else if (info.operation == SYNC) {
			info.data = Reference<AsyncFileBuffer>(nullptr);
			wait(self->fileHandle->file->sync());
		} else if (info.operation == REOPEN) {
			// Will fail if the file does not exist
			wait(self->openFile(self, IAsyncFile::OPEN_READWRITE, 0666, 0, false));
			int64_t fileSize = wait(self->fileHandle->file->size());
			int64_t fileSizeChange = fileSize - self->fileSize;
			if (fileSizeChange >= _PAGE_SIZE) {
				fmt::print("Reopened file increased in size by {0} bytes (at most {1} allowed)\n",
				           fileSizeChange,
				           _PAGE_SIZE - 1);
				self->success = false;
			} else if (fileSizeChange < 0) {
				fmt::print("Reopened file decreased in size by {} bytes\n", -fileSizeChange);
				self->success = false;
			}

			self->updateMemoryBuffer(fileSize);
		} else if (info.operation == TRUNCATE) {
			// Perform the truncate.  Don't allow it to be cancelled (because the underlying IO may not be cancellable)
			// and don't allow file handle to be deleted
			wait(uncancellable(holdWhile(self->fileHandle, self->fileHandle->file->truncate(info.offset))));

			int64_t fileSize = wait(self->fileHandle->file->size());
			if (fileSize != info.offset) {
				printf("Incorrect file size reported after truncate\n");
				self->success = false;
			}

			self->updateMemoryBuffer(fileSize);
		}

		++self->numOperations;
		return info;
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (enabled) {
			m.emplace_back("Number of Operations Performed", numOperations.getValue(), Averaged::False);
			m.emplace_back("Average CPU Utilization (Percentage)", averageCpuUtilization * 100, Averaged::False);
		}
	}
};

WorkloadFactory<AsyncFileCorrectnessWorkload> AsyncFileCorrectnessWorkloadFactory("AsyncFileCorrectness");
