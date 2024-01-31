/*
 * AsyncFileWriteChecker.actor.h
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
#if defined(NO_INTELLISENSE) && !defined(ASYNC_FILE_WRITE_CHECKER_ACTOR_G_H)
#define ASYNC_FILE_WRITE_CHECKER_ACTOR_G_H
#include "fdbrpc/AsyncFileWriteChecker.actor.g.h"
#elif !defined(ASYNC_FILE_WRITE_CHECKER_ACTOR_H)
#define ASYNC_FILE_WRITE_CHECKER_ACTOR_H

#include "flow/IAsyncFile.h"
#include "crc32/crc32c.h"

#if VALGRIND
#include <memcheck.h>
#endif

#include "flow/actorcompiler.h"
struct ChecksumRequest {
	void const* data;
	int length;
	int64_t offset;
	ChecksumRequest(void const* initData, int initLength, int64_t initOffset)
	  : data(initData), length(initLength), offset(initOffset) {}
};

class AsyncFileWriteChecker : public IAsyncFile, public ReferenceCounted<AsyncFileWriteChecker> {
public:
	void addref() override { ReferenceCounted<AsyncFileWriteChecker>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileWriteChecker>::delref(); }

	virtual StringRef getClassName() override { return "AsyncFileWriteChecker"_sr; }

	// For read() and write(), the data buffer must remain valid until the future is ready
	Future<int> read(void* data, int length, int64_t offset) override {
		// Lambda must hold a reference to this to keep it alive until after the read
		Future<int> readSize = m_f->read(data, length, offset);
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(success(readSize) && success(self->fileSize), [self, data, offset, readSize](Void v) {
			if (!self->reserved) {
				self->reserved = true;
				// This would make the vector have a large VMM footprint, but the RSS footprint would only increase
				// as the space is actually used. This would avoid the copy operation when resize is needed.
				// However, resize later to a smaller number would not free memory.
				self->checksumHistory.reserve(std::min<int>(std::max<int>(checksumHistoryBudget.get(), 0),
				                                            self->fileSize.get() / checksumHistoryPageSize));
			}
			int r = readSize.get();
			self->updateChecksumHistory(false, offset, r, (uint8_t*)data);
			return r;
		});
	}
	Future<Void> readZeroCopy(void** data, int* length, int64_t offset) override {
		// Lambda must hold a reference to this to keep it alive until after the read
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(success(m_f->readZeroCopy(data, length, offset)) && success(self->fileSize),
		           [self, data, length, offset](Void r) {
			           if (!self->reserved) {
				           self->reserved = true;
				           self->checksumHistory.reserve(std::min<int>(std::max<int>(checksumHistoryBudget.get(), 0),
				                                                       self->fileSize.get() / checksumHistoryPageSize));
			           }
			           self->updateChecksumHistory(false, offset, *length, (uint8_t*)data);
			           return r;
		           });
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		updateChecksumHistory(true, offset, length, (uint8_t*)data);
		return map(success(m_f->write(data, length, offset)) && success(self->fileSize),
		           [self, data, length, offset](Void r) {
			           if (!self->reserved) {
				           self->reserved = true;
				           self->checksumHistory.reserve(std::min<int>(std::max<int>(checksumHistoryBudget.get(), 0),
				                                                       self->fileSize.get() / checksumHistoryPageSize));
			           }
			           // try validate checksum by read after write, but not guarantee when it will be done
			           self->ps.send(ChecksumRequest(data, length, offset));
			           pendingChecksumCount += 1;
			           return r;
		           });
	}

	Future<Void> truncate(int64_t size) override {
		// Lambda must hold a reference to this to keep it alive until after the read
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(m_f->truncate(size), [self, size](Void r) {
			// Truncate the page checksum history if it is in use
			if ((size / checksumHistoryPageSize) < self->checksumHistory.size()) {
				int oldCapacity = self->checksumHistory.capacity();
				self->checksumHistory.resize(size / checksumHistoryPageSize);
				checksumHistoryBudget.get() -= (self->checksumHistory.capacity() - oldCapacity);
			}
			return r;
		});
	}

	Future<Void> sync() override { return m_f->sync(); }
	Future<Void> flush() override { return m_f->flush(); }
	Future<int64_t> size() const override { return m_f->size(); }
	std::string getFilename() const override { return m_f->getFilename(); }
	void releaseZeroCopy(void* data, int length, int64_t offset) override {
		return m_f->releaseZeroCopy(data, length, offset);
	}
	int64_t debugFD() const override { return m_f->debugFD(); }

	AsyncFileWriteChecker(Reference<IAsyncFile> f) : m_f(f) {
		// Initialize the static history budget the first time (and only the first time) a file is opened.
		if (!checksumHistoryBudget.present()) {
			checksumHistoryBudget = FLOW_KNOBS->PAGE_WRITE_CHECKSUM_HISTORY;
		}
		fileSize = m_f->size();
		// Adjust the budget by the initial capacity of history, which should be 0 but maybe not for some
		// implementations.
		checksumHistoryBudget.get() -= checksumHistory.capacity();
	}

	~AsyncFileWriteChecker() override { checksumHistoryBudget.get() += checksumHistory.capacity(); }

private:
	Reference<IAsyncFile> m_f;
	PromiseStream<struct ChecksumRequest> ps;
	Future<Void> checksumWorker;
	Future<Void> checksumLogger;

	struct WriteInfo {
		WriteInfo() : checksum(0), timestamp(0) {}
		uint32_t checksum;
		uint32_t timestamp;
	};

	std::vector<WriteInfo> checksumHistory;
	Future<int64_t> fileSize;
	bool reserved = false;
	bool checksumWorkerStart = false;
	// This is the most page checksum history blocks we will use across all files.
	static Optional<int> checksumHistoryBudget;
	static int checksumHistoryPageSize;
	// add a queue and an actor, to have the actor sequentially process the queue, log active queue size periodically
	static int pendingChecksumCount;

	ACTOR Future<Void> runChecksumWorker(AsyncFileWriteChecker* self, FutureStream<ChecksumRequest> checksumStream) {
		loop {
			ChecksumRequest req = waitNext(checksumStream);
			void const* data = req.data;
			int length = req.length;
			int64_t offset = req.offset;
			self->updateChecksumHistory(false, offset, length, (uint8_t*)data);
			pendingChecksumCount -= 1;
		}
	}

	ACTOR Future<Void> runChecksumLogger(AsyncFileWriteChecker* self) {
		state double delayDuration = FLOW_KNOBS->ASYNC_FILE_WRITE_CHEKCER_LOGGING_INTERVAL;
		loop {
			wait(delay(delayDuration));
			TraceEvent("AsyncFileWriteChecker")
			    .detail("Delay", delayDuration)
			    .detail("Filename", self->getFilename())
			    .detail("PendingChecksumCount", pendingChecksumCount);
		}
	}

	// Update or check checksum(s) in history for any full pages covered by this operation
	void updateChecksumHistory(bool updateChecksum, int64_t offset, int len, uint8_t* buf) {
		if (!checksumWorkerStart) {
			checksumWorkerStart = true;
			checksumWorker = runChecksumWorker(this, ps.getFuture());
			checksumLogger = runChecksumLogger(this);
		}
		// Check or set each full block in the the range
		int page = offset / checksumHistoryPageSize; // First page number
		int slack = offset % checksumHistoryPageSize; // Bytes after most recent page boundary
		uint8_t* start = buf; // Position in buffer to start checking from
		// If offset is not page-aligned, move to next page and adjust start
		if (slack != 0) {
			++page;
			start += (checksumHistoryPageSize - slack);
		}
		int pageEnd = (offset + len) / checksumHistoryPageSize; // Last page plus 1

		// Make sure history is large enough or limit pageEnd
		if (checksumHistory.size() < pageEnd) {
			if (checksumHistoryBudget.get() > 0) {
				// Resize history and update budget based on capacity change
				auto initialCapacity = checksumHistory.capacity();
				checksumHistory.resize(checksumHistory.size() +
				                       std::min<int>(checksumHistoryBudget.get(), pageEnd - checksumHistory.size()));
				checksumHistoryBudget.get() -= (checksumHistory.capacity() - initialCapacity);
			}

			// Limit pageEnd to end of history, which works whether or not all of the desired
			// history slots were allocated.
			pageEnd = checksumHistory.size();
		}

		while (page < pageEnd) {
			uint32_t checksum = crc32c_append(0xab12fd93, start, checksumHistoryPageSize);
			WriteInfo& history = checksumHistory[page];
			// printf("%d %d %u %u\n", updateChecksum, page, checksum, history.checksum);

#if VALGRIND
			// It's possible we'll read or write a page where not all of the data is defined, but the checksum of the
			// page is still valid
			VALGRIND_MAKE_MEM_DEFINED_IF_ADDRESSABLE(&checksum, sizeof(uint32_t));
#endif

			// when updateChecksum is true, just update the stored sum
			double millisecondsPerSecond = 1000;
			if (updateChecksum) {
				history.timestamp = (uint32_t)(now() * millisecondsPerSecond);
				history.checksum = checksum;
			} else {
				if (history.checksum != 0 && history.checksum != checksum) {
					// For reads, verify the stored sum if it is not 0.  If it fails, clear it.
					TraceEvent(SevError, "AsyncFileLostWriteDetected")
					    .error(checksum_failed())
					    .detail("Filename", m_f->getFilename())
					    .detail("PageNumber", page)
					    .detail("ChecksumOfPage", checksum)
					    .detail("ChecksumHistory", history.checksum)
					    .detail("LastWriteTime", history.timestamp / millisecondsPerSecond);
					history.checksum = 0;
				}
			}

			start += checksumHistoryPageSize;
			++page;
		}
	}
};

#include "flow/unactorcompiler.h"
#endif