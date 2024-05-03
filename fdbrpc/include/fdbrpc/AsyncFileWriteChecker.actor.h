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
static double millisecondsPerSecond = 1000;

// this class does checksum for the wrapped IAsyncFile in read and writes opertions.
// it maintains a dynamic data structure to store the recently written page and its checksum.
// it has an actor to continuously read and verify checksums for the recently written page,
// and also deletes the corresponding entry upon a successful to avoid using too much memory.
class AsyncFileWriteChecker : public IAsyncFile, public ReferenceCounted<AsyncFileWriteChecker> {
public:
	void addref() override { ReferenceCounted<AsyncFileWriteChecker>::addref(); }
	void delref() override { ReferenceCounted<AsyncFileWriteChecker>::delref(); }

	virtual StringRef getClassName() override { return "AsyncFileWriteChecker"_sr; }

	// For read() and write(), the data buffer must remain valid until the future is ready
	Future<int> read(void* data, int length, int64_t offset) override {
		// Lambda must hold a reference to this to keep it alive until after the read
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(m_f->read(data, length, offset), [self, data, offset](int r) {
			self->updateChecksumHistory(false, offset, r, (uint8_t*)data);
			return r;
		});
	}
	Future<Void> readZeroCopy(void** data, int* length, int64_t offset) override {
		// Lambda must hold a reference to this to keep it alive until after the read
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(m_f->readZeroCopy(data, length, offset), [self, data, length, offset](Void r) {
			self->updateChecksumHistory(false, offset, *length, (uint8_t*)data);
			return r;
		});
	}

	Future<Void> write(void const* data, int length, int64_t offset) override {
		auto pages = updateChecksumHistory(true, offset, length, (uint8_t*)data);
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(m_f->write(data, length, offset), [self, pages](Void r) {
			for (uint32_t page : pages) {
				self->writing.erase(page);
			}
			return r;
		});
	}

	Future<Void> truncate(int64_t size) override {
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(m_f->truncate(size), [self, size](Void r) {
			int maxFullPage = size / checksumHistoryPageSize;
			int oldSize = self->lru.size();
			self->lru.truncate(maxFullPage);
			checksumHistoryBudget.get() += oldSize - self->lru.size();
			return r;
		});
	}

	Future<Void> sync() override {
		auto self = Reference<AsyncFileWriteChecker>::addRef(this);
		return map(m_f->sync(), [self](Void r) {
			self->syncedTime = AsyncFileWriteChecker::transformTime(now());
			return r;
		});
	}

	Future<Void> flush() override { return m_f->flush(); }
	Future<int64_t> size() const override { return m_f->size(); }
	std::string getFilename() const override { return m_f->getFilename(); }
	void releaseZeroCopy(void* data, int length, int64_t offset) override {
		return m_f->releaseZeroCopy(data, length, offset);
	}
	int64_t debugFD() const override { return m_f->debugFD(); }

	struct WriteInfo {
		WriteInfo() : checksum(0), timestamp(0) {}
		uint32_t checksum;
		uint64_t timestamp; // keep a precision of ms
	};

	static uint64_t transformTime(double unixTime) { return (uint64_t)(unixTime * millisecondsPerSecond); }

	class LRU {
	private:
		uint64_t step;
		std::string fileName;
		std::map<uint64_t, uint32_t> stepToKey;
		std::map<uint32_t, uint64_t> keyToStep; // std::map is to support ::truncate
		std::unordered_map<uint32_t, AsyncFileWriteChecker::WriteInfo> pageContents;

	public:
		LRU(std::string _fileName) {
			step = 0;
			fileName = _fileName;
		}

		void update(uint32_t page, AsyncFileWriteChecker::WriteInfo writeInfo) {
			if (keyToStep.find(page) != keyToStep.end()) {
				// remove its old entry in stepToKey
				stepToKey.erase(keyToStep[page]);
			}
			keyToStep[page] = step;
			stepToKey[step] = page;
			pageContents[page] = writeInfo;
			step++;
		}

		void truncate(uint32_t page) {
			auto it = keyToStep.lower_bound(page);
			// iterate through keyToStep, to find corresponding entries in stepToKey
			while (it != keyToStep.end()) {
				uint64_t step = it->second;
				auto next = it;
				next++;
				keyToStep.erase(it);
				stepToKey.erase(step);
				it = next;
			}
		}

		uint32_t randomPage() {
			if (keyToStep.size() == 0) {
				return 0;
			}
			auto it = keyToStep.begin();
			std::advance(it, deterministicRandom()->randomInt(0, (int)keyToStep.size()));
			return it->first;
		}

		int size() { return keyToStep.size(); }

		bool exist(uint32_t page) { return keyToStep.find(page) != keyToStep.end(); }

		AsyncFileWriteChecker::WriteInfo find(uint32_t page) {
			auto it = keyToStep.find(page);
			if (it == keyToStep.end()) {
				TraceEvent(SevError, "LRUCheckerTryFindingPageNotExist")
				    .detail("FileName", fileName)
				    .detail("Page", page)
				    .log();
				return AsyncFileWriteChecker::WriteInfo();
			}
			return pageContents[page];
		}

		uint32_t leastRecentlyUsedPage() {
			if (stepToKey.size() == 0) {
				return 0;
			}
			return stepToKey.begin()->second;
		}

		void remove(uint32_t page) {
			if (keyToStep.find(page) == keyToStep.end()) {
				return;
			}
			pageContents.erase(page);
			stepToKey.erase(keyToStep[page]);
			keyToStep.erase(page);
		}
	};

	AsyncFileWriteChecker(Reference<IAsyncFile> f) : m_f(f), lru(f->getFilename()) {
		// Initialize the static history budget the first time (and only the first time) a file is opened.
		if (!checksumHistoryBudget.present()) {
			checksumHistoryBudget = FLOW_KNOBS->PAGE_WRITE_CHECKSUM_HISTORY;
		}
		pageBuffer = (void*)new char[checksumHistoryPageSize];
		totalCheckedSucceed = 0;
		totalCheckedFail = 0;
		lru = LRU(m_f->getFilename());
		checksumWorker = AsyncFileWriteChecker::sweep(this);
		checksumLogger = runChecksumLogger(this);
	}

	~AsyncFileWriteChecker() override {
		checksumHistoryBudget.get() += lru.size();
		delete[] reinterpret_cast<char*>(pageBuffer);
	}

private:
	Reference<IAsyncFile> m_f;
	Future<Void> checksumWorker;
	Future<Void> checksumLogger;
	LRU lru;
	void* pageBuffer;
	uint64_t totalCheckedFail, totalCheckedSucceed;
	// transform from unixTime(double) to uint64_t, to retain ms precision.
	uint64_t syncedTime;
	// to avoid concurrent operation, so that the continuous reader will skip a page if it is being written
	std::unordered_set<uint32_t> writing;
	// This is the most page checksum history blocks we will use across all files.
	static Optional<int> checksumHistoryBudget;
	static int checksumHistoryPageSize;

	ACTOR Future<Void> sweep(AsyncFileWriteChecker* self) {
		loop {
			// for each page, read and do checksum
			// scan from the least recently used, thus it is safe to quit if data has not been synced
			state uint32_t page = self->lru.leastRecentlyUsedPage();
			while (self->writing.find(page) != self->writing.end() || page == 0) {
				// avoid concurrent ops
				wait(delay(FLOW_KNOBS->ASYNC_FILE_WRITE_CHEKCER_CHECKING_DELAY));
				continue;
			}
			int64_t offset = page * checksumHistoryPageSize;
			// perform a read to verify checksum, it will remove the entry upon success
			wait(success(self->read(self->pageBuffer, checksumHistoryPageSize, offset)));
		}
	}

	ACTOR Future<Void> runChecksumLogger(AsyncFileWriteChecker* self) {
		state double delayDuration = FLOW_KNOBS->ASYNC_FILE_WRITE_CHEKCER_LOGGING_INTERVAL;
		loop {
			wait(delay(delayDuration));
			// TODO: add more stats, such as total checked, current entries, budget
			TraceEvent("AsyncFileWriteChecker")
			    .detail("Delay", delayDuration)
			    .detail("Filename", self->getFilename())
			    .detail("TotalCheckedSucceed", self->totalCheckedSucceed)
			    .detail("TotalCheckedFail", self->totalCheckedFail)
			    .detail("CurrentSize", self->lru.size());
		}
	}

	// return true if there are still remaining valid synced pages to check, otherwise false
	// this method removes the page entry from checksum history upon a successful check
	bool verifyChecksum(uint32_t page, uint32_t checksum, uint8_t* start) {
		if (!lru.exist(page)) {
			// it has already been verified succesfully and removed by checksumWorker
			return true;
		}
		WriteInfo history = lru.find(page);
		// only verify checksum for pages have been synced
		if (history.timestamp < syncedTime) {
			if (history.checksum != checksum) {
				TraceEvent(SevError, "AsyncFileLostWriteDetected")
				    .error(checksum_failed())
				    .detail("Filename", getFilename())
				    .detail("PageNumber", page)
				    .detail("Size", lru.size())
				    .detail("Start", (long)start)
				    .detail("ChecksumOfPage", checksum)
				    .detail("ChecksumHistory", history.checksum)
				    .detail("SyncedTime", syncedTime / millisecondsPerSecond)
				    .detail("LastWriteTime", history.timestamp / millisecondsPerSecond);
				totalCheckedFail += 1;
			} else {
				checksumHistoryBudget.get() += 1;
				lru.remove(page);
				totalCheckedSucceed += 1;
			}
			return true;
		} else {
			return false;
		}
	}

	// Update or check checksum(s) in history for any full pages covered by this operation
	// return the updated pages when updateChecksum is true
	std::vector<uint32_t> updateChecksumHistory(bool updateChecksum, int64_t offset, int len, uint8_t* buf) {
		std::vector<uint32_t> pages;
		// Check or set each full block in the the range
		// page number starts at 1, as we use 0 to indicate invalid page
		uint32_t page = offset / checksumHistoryPageSize + 1; // First page number
		int slack = offset % checksumHistoryPageSize; // Bytes after most recent page boundary
		uint8_t* start = buf; // Position in buffer to start checking from
		// If offset is not page-aligned, move to next page and adjust start
		if (slack != 0) {
			++page;
			start += (checksumHistoryPageSize - slack);
		}
		uint32_t startPage = page;
		uint32_t pageEnd = (offset + len) / checksumHistoryPageSize; // Last page plus 1
		while (page < pageEnd) {
			uint32_t checksum = crc32c_append(0xab12fd93, start, checksumHistoryPageSize);
#if VALGRIND
			// It's possible we'll read or write a page where not all of the data is defined, but the checksum of the
			// page is still valid
			VALGRIND_MAKE_MEM_DEFINED_IF_ADDRESSABLE(&checksum, sizeof(uint32_t));
#endif
			// when updateChecksum is true, just update the stored sum and skip checking
			if (updateChecksum) {
				writing.insert(page);
				pages.push_back(page);
				WriteInfo history;
				if (!lru.exist(page)) {
					if (checksumHistoryBudget.get() > 0) {
						checksumHistoryBudget.get() -= 1;
					} else {
						TraceEvent("SkippedPagesDuringUpdateChecksum")
						    .detail("Filename", getFilename())
						    .detail("StartPage", startPage)
						    .detail("CheckedPage", page)
						    .detail("TotalPage", pageEnd);
						break;
					}
				}
				history.timestamp = AsyncFileWriteChecker::transformTime(now());
				history.checksum = checksum;
				lru.update(page, history);
			} else {
				if (!verifyChecksum(page, checksum, start)) {
					break;
				}
			}
			start += checksumHistoryPageSize;
			++page;
		}
		return pages;
	}
};

#include "flow/unactorcompiler.h"
#endif