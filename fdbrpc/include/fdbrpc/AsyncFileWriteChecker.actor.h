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
		uint32_t timestamp; // keep a precision of ms
	};

	struct node {
		uint32_t page;
		WriteInfo writeInfo;
		node *next, *prev;
		node(uint32_t _page, WriteInfo _writeInfo) {
			page = _page;
			writeInfo = _writeInfo;
			page = _page;
			next = NULL;
			prev = NULL;
		}
	};

	class LRU {
	private:
		node* start;
		node* end;
		std::unordered_map<uint32_t, node*> m;
		std::string fileName;
		int maxFullPage;

		void insertHead(node* n) {
			n->next = start->next;
			start->next->prev = n;
			n->prev = start;
			start->next = n;
		}

		void removeFromList(node* n) {
			n->prev->next = n->next;
			n->next->prev = n->prev;
		}

	public:
		LRU(std::string _fileName) {
			fileName = _fileName;
			maxFullPage = 0;
			start = new node(0, WriteInfo());
			end = new node(0, WriteInfo());
			start->next = end;
			end->prev = start;
		}

		void update(uint32_t page, WriteInfo writeInfo) {
			if (m.find(page) != m.end()) {
				node* n = m[page];
				removeFromList(n);
				insertHead(n);
				n->writeInfo = writeInfo;
				return;
			}
			node* n = new node(page, writeInfo);
			insertHead(n);
			m[page] = n;
			if (page >= maxFullPage) {
				maxFullPage = page + 1;
			}
		}

		int maxPage() { return maxFullPage; }

		void truncate(int newMaxFullPage) {
			for (int i = newMaxFullPage; i < maxFullPage; i++) {
				remove(i);
			}
			if (maxFullPage > newMaxFullPage) {
				maxFullPage = newMaxFullPage;
			}
		}

		int size() { return m.size(); }

		bool exist(uint32_t page) { return m.find(page) != m.end(); }

		WriteInfo find(uint32_t page) {
			if (!exist(page)) {
				TraceEvent(SevError, "CheckerTryFindingPageNotExist").log();
				return WriteInfo();
			}
			return m[page]->writeInfo;
		}

		uint32_t leastRecentlyUsedPage() {
			if (m.size() == 0) {
				return -1;
			}
			return end->prev->page;
		}

		void remove(uint32_t page) {
			if (m.find(page) == m.end()) {
				return;
			}
			node* n = m[page];
			removeFromList(n);
			m.erase(page);
			delete n;
		}
	};

	AsyncFileWriteChecker(Reference<IAsyncFile> f) : m_f(f), lru("aa") {
		// Initialize the static history budget the first time (and only the first time) a file is opened.
		if (!checksumHistoryBudget.present()) {
			checksumHistoryBudget = FLOW_KNOBS->PAGE_WRITE_CHECKSUM_HISTORY;
		}
		totalCheckedSucceed = 0;
		totalCheckedFail = 0;
		lru = LRU(m_f->getFilename());
	}

	~AsyncFileWriteChecker() override { checksumHistoryBudget.get() += lru.size(); }

private:
	// transform from unixTime(double) to uint32_t, to retain ms precision.
	static uint32_t transformTime(double unixTime) { return (uint32_t)(unixTime * millisecondsPerSecond); }
	Reference<IAsyncFile> m_f;
	Future<Void> checksumWorker;
	Future<Void> checksumLogger;
	LRU lru;

	uint64_t totalCheckedFail, totalCheckedSucceed;
	uint32_t syncedTime;
	bool checksumWorkerStart = false;
	std::unordered_set<uint32_t> writing;
	// This is the most page checksum history blocks we will use across all files.
	static Optional<int> checksumHistoryBudget;
	static int checksumHistoryPageSize;

	ACTOR static Future<Void> sweep(AsyncFileWriteChecker* self) {
		state void* data = (void*)new char[checksumHistoryPageSize];
		loop {
			// for each page, read and do checksum
			// scan from the least recently used, thus it is safe to quit if data has not been synced
			state uint32_t page = self->lru.leastRecentlyUsedPage();
			while (self->writing.find(page) != self->writing.end()) {
				// avoid concurrent ops
				wait(delay(3.0));
				continue;
			}
			if (page == -1) {
				// no more available pages
				break;
			}
			int64_t offset = page * checksumHistoryPageSize;
			// perform a read to verify checksum
			wait(success(self->read(data, checksumHistoryPageSize, offset)));
			self->lru.remove(page);
		}
		delete[] reinterpret_cast<char*>(data);
		return Void();
	}

	ACTOR Future<Void> runChecksumWorker(AsyncFileWriteChecker* self) {
		// periodically scan the LRU of checksum history, remove them after check the items that have been synced
		loop {
			choose {
				when(wait(AsyncFileWriteChecker::sweep(self))) {}
				when(wait(delay(FLOW_KNOBS->ASYNC_FILE_WRITE_CHEKCER_CHECKING_INTERVAL))) {}
			}
			wait(delay(FLOW_KNOBS->ASYNC_FILE_WRITE_CHEKCER_CHECKING_INTERVAL));
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
	bool verifyChecksum(int page, uint32_t checksum, uint8_t* start, bool sweep) {
		if (!lru.exist(page)) {
			// it has already been verified succesfully and removed by checksumWorker
			return true;
		}
		WriteInfo history = lru.find(page);
		// only verify checksum for pages have been sycned
		if (history.timestamp < syncedTime) {
			if (history.checksum != checksum) {
				TraceEvent(SevError, "AsyncFileLostWriteDetected")
				    .error(checksum_failed())
				    .detail("Filename", getFilename())
				    .detail("Sweep", sweep)
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
	std::vector<uint32_t> updateChecksumHistory(bool updateChecksum,
	                                            int64_t offset,
	                                            int len,
	                                            uint8_t* buf,
	                                            bool sweep = false) {
		std::vector<uint32_t> pages;
		if (!checksumWorkerStart) {
			checksumWorkerStart = true;
			checksumWorker = runChecksumWorker(this);
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
		int startPage = page;
		int pageEnd = (offset + len) / checksumHistoryPageSize; // Last page plus 1
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
				if (FLOW_KNOBS->ASYNC_FILE_WRITE_CHEKCER_ENABLE_CHECKSUM) {
					if (!verifyChecksum(page, checksum, start, sweep)) {
						break;
					}
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