/*
 * AsyncFileWriteChecker.h
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

#include "fdbrpc/IAsyncFile.h"
#include "flow/crc32c.h"

#if VALGRIND
#include <memcheck.h>
#endif

class AsyncFileWriteChecker : public IAsyncFile, public ReferenceCounted<AsyncFileWriteChecker> {
public:
	void addref() override { ReferenceCounted<AsyncFileWriteChecker>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileWriteChecker>::delref(); }

	// For read() and write(), the data buffer must remain valid until the future is ready
	Future<int> read(void* data, int length, int64_t offset) override {
		return map(m_f->read(data, length, offset), [=](int r) {
			updateChecksumHistory(false, offset, r, (uint8_t*)data);
			return r;
		});
	}
	Future<Void> readZeroCopy(void** data, int* length, int64_t offset) override {
		return map(m_f->readZeroCopy(data, length, offset), [=](Void r) {
			updateChecksumHistory(false, offset, *length, (uint8_t*)data);
			return r;
		});
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		updateChecksumHistory(true, offset, length, (uint8_t*)data);
		return m_f->write(data, length, offset);
	}

	Future<Void> truncate(int64_t size) override {
		return map(m_f->truncate(size), [=](Void r) {
			// Truncate the page checksum history if it is in use
			if ((size / checksumHistoryPageSize) < checksumHistory.size()) {
				int oldCapacity = checksumHistory.capacity();
				checksumHistory.resize(size / checksumHistoryPageSize);
				checksumHistoryBudget.get() -= (checksumHistory.capacity() - oldCapacity);
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

		// Adjust the budget by the initial capacity of history, which should be 0 but maybe not for some
		// implementations.
		checksumHistoryBudget.get() -= checksumHistory.capacity();
	}

	~AsyncFileWriteChecker() override { checksumHistoryBudget.get() += checksumHistory.capacity(); }

private:
	Reference<IAsyncFile> m_f;

	struct WriteInfo {
		WriteInfo() : checksum(0), timestamp(0) {}
		uint32_t checksum;
		uint32_t timestamp;
	};

	std::vector<WriteInfo> checksumHistory;
	// This is the most page checksum history blocks we will use across all files.
	static Optional<int> checksumHistoryBudget;
	static int checksumHistoryPageSize;

	// Update or check checksum(s) in history for any full pages covered by this operation
	void updateChecksumHistory(bool write, int64_t offset, int len, uint8_t* buf) {
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
			// printf("%d %d %u %u\n", write, page, checksum, history.checksum);

#if VALGRIND
			// It's possible we'll read or write a page where not all of the data is defined, but the checksum of the
			// page is still valid
			VALGRIND_MAKE_MEM_DEFINED_IF_ADDRESSABLE(&checksum, sizeof(uint32_t));
#endif

			// For writes, just update the stored sum
			if (write) {
				history.timestamp = (uint32_t)now();
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
					    .detail("LastWriteTime", history.timestamp);
					history.checksum = 0;
				}
			}

			start += checksumHistoryPageSize;
			++page;
		}
	}
};
