/*
 * VersionedBTree.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "flow/flow.h"
#include "fdbserver/IVersionedStore.h"
#include "fdbserver/IPager.h"
#include "fdbclient/Tuple.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"
#include "fdbserver/IPager.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/ActorCollection.h"
#include <map>
#include <vector>
#include "fdbclient/CommitTransaction.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/DeltaTree.h"
#include <string.h>
#include "flow/actorcompiler.h"
#include <cinttypes>
#include <boost/intrusive/list.hpp>

#define REDWOOD_DEBUG 0

// Only print redwood debug statements for a certain address. Useful in simulation with many redwood processes to reduce
// log size.
#define REDWOOD_DEBUG_ADDR 0
// example addr:  "[abcd::4:0:1:4]:1"
#define REDWOOD_DEBUG_ADDR_VAL "";

#define debug_printf_stream stdout
#define debug_printf_always(...)                                                                                       \
	{                                                                                                                  \
		std::string prefix = format("%s %f %04d ", g_network->getLocalAddress().toString().c_str(), now(), __LINE__);  \
		std::string msg = format(__VA_ARGS__);                                                                         \
		writePrefixedLines(debug_printf_stream, prefix, msg);                                                          \
		fflush(debug_printf_stream);                                                                                   \
	}

#define debug_printf_addr(...)                                                                                         \
	{                                                                                                                  \
		std::string addr = REDWOOD_DEBUG_ADDR_VAL;                                                                     \
		if (!memcmp(addr.c_str(), g_network->getLocalAddress().toString().c_str(), addr.size())) {                     \
			std::string prefix =                                                                                       \
			    format("%s %f %04d ", g_network->getLocalAddress().toString().c_str(), now(), __LINE__);               \
			std::string msg = format(__VA_ARGS__);                                                                     \
			writePrefixedLines(debug_printf_stream, prefix, msg);                                                      \
			fflush(debug_printf_stream);                                                                               \
		}                                                                                                              \
	}

#define debug_printf_noop(...)

#if defined(NO_INTELLISENSE)
#if REDWOOD_DEBUG
#define debug_printf debug_printf_always
#elif REDWOOD_DEBUG_ADDR
#define debug_printf debug_printf_addr
#else
#define debug_printf debug_printf_noop
#endif
#else
// To get error-checking on debug_printf statements in IDE
#define debug_printf printf
#endif

#define BEACON debug_printf_always("HERE\n")
#define TRACE                                                                                                          \
	debug_printf_always("%s: %s line %d %s\n", __FUNCTION__, __FILE__, __LINE__, platform::get_backtrace().c_str());

// Writes prefix:line for each line in msg to fout
void writePrefixedLines(FILE* fout, std::string prefix, std::string msg) {
	StringRef m = msg;
	while (m.size() != 0) {
		StringRef line = m.eat("\n");
		fprintf(fout, "%s %s\n", prefix.c_str(), line.toString().c_str());
	}
}

// Some convenience functions for debugging to stringify various structures
// Classes can add compatibility by either specializing toString<T> or implementing
//   std::string toString() const;
template <typename T>
std::string toString(const T& o) {
	return o.toString();
}

std::string toString(StringRef s) {
	return s.printable();
}

std::string toString(LogicalPageID id) {
	if (id == invalidLogicalPageID) {
		return "LogicalPageID{invalid}";
	}
	return format("LogicalPageID{%u}", id);
}

std::string toString(Version v) {
	if (v == invalidVersion) {
		return "invalidVersion";
	}
	return format("@%" PRId64, v);
}

std::string toString(bool b) {
	return b ? "true" : "false";
}

template <typename T>
std::string toString(const Standalone<T>& s) {
	return toString((T)s);
}

template <typename T>
std::string toString(const T* begin, const T* end) {
	std::string r = "{";

	bool comma = false;
	while (begin != end) {
		if (comma) {
			r += ", ";
		} else {
			comma = true;
		}
		r += toString(*begin++);
	}

	r += "}";
	return r;
}

template <typename T>
std::string toString(const std::vector<T>& v) {
	return toString(&v.front(), &v.back() + 1);
}

template <typename T>
std::string toString(const VectorRef<T>& v) {
	return toString(v.begin(), v.end());
}

template <typename T>
std::string toString(const Optional<T>& o) {
	if (o.present()) {
		return toString(o.get());
	}
	return "<not present>";
}

template <typename F, typename S>
std::string toString(const std::pair<F, S>& o) {
	return format("{%s, %s}", toString(o.first).c_str(), toString(o.second).c_str());
}

// A FIFO queue of T stored as a linked list of pages.
// Main operations are pop(), pushBack(), pushFront(), and flush().
//
// flush() will ensure all queue pages are written to the pager and move the unflushed
// pushFront()'d records onto the front of the queue, in FIFO order.
//
// pop() will only return records that have been flushed, and pops
// from the front of the queue.
//
// Each page contains some number of T items and a link to the next page and starting position on that page.
// When the queue is flushed, the last page in the chain is ended and linked to a newly allocated
// but not-yet-written-to pageID, which future writes after the flush will write to.
// Items pushed onto the front of the queue are written to a separate linked list until flushed,
// at which point that list becomes the new front of the queue.
//
// The write pattern is designed such that no page is ever expected to be valid after
// being written to or updated but not fsync'd.  This is why a new unused page is added
// to the queue, linked to by the last data page, before commit.  The new page can't be
// added and filled with data as part of the next commit because that would mean modifying
// the previous tail page to update its next link, which risks corrupting it and losing
// data that was not yet popped if that write is never fsync'd.
//
// Requirements on T
//   - must be trivially copyable
//     OR have a specialization for FIFOQueueCodec<T>
//     OR have the following methods
//       // Deserialize from src into *this, return number of bytes from src consumed
//       int readFromBytes(const uint8_t *src);
//       // Return the size of *this serialized
//       int bytesNeeded() const;
//       // Serialize *this to dst, return number of bytes written to dst
//       int writeToBytes(uint8_t *dst) const;
//  - must be supported by toString(object) (see above)
template <typename T, typename Enable = void>
struct FIFOQueueCodec {
	static T readFromBytes(const uint8_t* src, int& bytesRead) {
		T x;
		bytesRead = x.readFromBytes(src);
		return x;
	}
	static int bytesNeeded(const T& x) { return x.bytesNeeded(); }
	static int writeToBytes(uint8_t* dst, const T& x) { return x.writeToBytes(dst); }
};

template <typename T>
struct FIFOQueueCodec<T, typename std::enable_if<std::is_trivially_copyable<T>::value>::type> {
	static_assert(std::is_trivially_copyable<T>::value);
	static T readFromBytes(const uint8_t* src, int& bytesRead) {
		bytesRead = sizeof(T);
		return *(T*)src;
	}
	static int bytesNeeded(const T& x) { return sizeof(T); }
	static int writeToBytes(uint8_t* dst, const T& x) {
		*(T*)dst = x;
		return sizeof(T);
	}
};

template <typename T, typename Codec = FIFOQueueCodec<T>>
class FIFOQueue {
public:
#pragma pack(push, 1)
	struct QueueState {
		bool operator==(const QueueState& rhs) const { return memcmp(this, &rhs, sizeof(QueueState)) == 0; }
		LogicalPageID headPageID = invalidLogicalPageID;
		LogicalPageID tailPageID = invalidLogicalPageID;
		uint16_t headOffset;
		// Note that there is no tail index because the tail page is always never-before-written and its index will
		// start at 0
		int64_t numPages;
		int64_t numEntries;
		std::string toString() const {
			return format("{head: %s:%d  tail: %s  numPages: %" PRId64 "  numEntries: %" PRId64 "}",
			              ::toString(headPageID).c_str(),
			              (int)headOffset,
			              ::toString(tailPageID).c_str(),
			              numPages,
			              numEntries);
		}
	};
#pragma pack(pop)

	struct Cursor {
		// Queue mode
		enum Mode { NONE, POP, READONLY, WRITE };
		Mode mode;

		// Queue this cursor is accessing
		FIFOQueue* queue;

		// The current page and pageID being read or written to
		LogicalPageID pageID;
		Reference<ArenaPage> page;

		// The first page ID to be written to the pager, if this cursor has written anything
		LogicalPageID firstPageIDWritten;

		// Offset after RawPage header in page to next read from or write to
		int offset;

		// A read cursor will not read this page (or beyond)
		LogicalPageID endPageID;

		// Page future and corresponding page ID for the expected next page to be used.  It may not
		// match the current page's next page link because queues can prepended with new front pages.
		Future<Reference<ArenaPage>> nextPageReader;
		LogicalPageID nextPageID;

		// Future that represents all outstanding write operations previously issued
		// This exists because writing the queue returns void, not a future
		Future<Void> writeOperations;

		FlowLock mutex;
		Future<Void> killMutex;

		Cursor() : mode(NONE) {}

		// Initialize a cursor.
		void init(FIFOQueue* q = nullptr,
		          Mode m = NONE,
		          LogicalPageID initialPageID = invalidLogicalPageID,
		          int readOffset = 0,
		          LogicalPageID endPage = invalidLogicalPageID) {
			queue = q;

			// If the pager gets an error, which includes shutdown, kill the mutex so any waiters can no longer run.
			// This avoids having every mutex wait also wait on pagerError.
			killMutex = map(ready(queue->pagerError), [=](Void e) {
				mutex.kill();
				return Void();
			});

			mode = m;
			firstPageIDWritten = invalidLogicalPageID;
			offset = readOffset;
			endPageID = endPage;
			page.clear();
			writeOperations = Void();

			if (mode == POP || mode == READONLY) {
				// If cursor is not pointed at the end page then start loading it.
				// The end page will not have been written to disk yet.
				pageID = initialPageID;
				if (pageID != endPageID) {
					startNextPageLoad(pageID);
				} else {
					nextPageID = invalidLogicalPageID;
				}
			} else {
				pageID = invalidLogicalPageID;
				ASSERT(mode == WRITE ||
				       (initialPageID == invalidLogicalPageID && readOffset == 0 && endPage == invalidLogicalPageID));
			}

			debug_printf("FIFOQueue::Cursor(%s) initialized\n", toString().c_str());

			if (mode == WRITE && initialPageID != invalidLogicalPageID) {
				addNewPage(initialPageID, 0, true);
			}
		}

		// Since cursors can have async operations pending which modify their state they can't be copied cleanly
		Cursor(const Cursor& other) = delete;

		~Cursor() { writeOperations.cancel(); }

		// A read cursor can be initialized from a pop cursor
		void initReadOnly(const Cursor& c) {
			ASSERT(c.mode == READONLY || c.mode == POP);
			init(c.queue, READONLY, c.pageID, c.offset, c.endPageID);
		}

		std::string toString() const {
			if (mode == WRITE) {
				return format("{WriteCursor %s:%p pos=%s:%d rawEndOffset=%d}",
				              queue->name.c_str(),
				              this,
				              ::toString(pageID).c_str(),
				              offset,
				              page ? raw()->endOffset : -1);
			}
			if (mode == POP || mode == READONLY) {
				return format("{ReadCursor %s:%p pos=%s:%d rawEndOffset=%d endPage=%s nextPage=%s}",
				              queue->name.c_str(),
				              this,
				              ::toString(pageID).c_str(),
				              offset,
				              page ? raw()->endOffset : -1,
				              ::toString(endPageID).c_str(),
				              ::toString(nextPageID).c_str());
			}
			ASSERT(mode == NONE);
			return format("{NullCursor=%p}", this);
		}

#pragma pack(push, 1)
		struct RawPage {
			// The next page of the queue after this one
			LogicalPageID nextPageID;
			// The start offset of the next page
			uint16_t nextOffset;
			// The end offset of the current page
			uint16_t endOffset;
			// Get pointer to data after page header
			uint8_t* begin() { return (uint8_t*)(this + 1); }
		};
#pragma pack(pop)

		// Returns true if the mutex cannot be immediately taken.
		bool isBusy() { return mutex.activePermits() != 0; }

		// Wait for all operations started before now to be ready, which is done by
		// obtaining and releasing the mutex.
		Future<Void> notBusy() {
			return isBusy() ? map(mutex.take(),
			                      [&](Void) {
				                      mutex.release();
				                      return Void();
			                      })
			                : Void();
		}

		// Returns true if any items have been written to the last page
		bool pendingTailWrites() const { return mode == WRITE && offset != 0; }

		RawPage* raw() const { return ((RawPage*)(page->begin())); }

		void setNext(LogicalPageID pageID, int offset) {
			ASSERT(mode == WRITE);
			RawPage* p = raw();
			p->nextPageID = pageID;
			p->nextOffset = offset;
		}

		void startNextPageLoad(LogicalPageID id) {
			nextPageID = id;
			debug_printf(
			    "FIFOQueue::Cursor(%s) loadPage start id=%s\n", toString().c_str(), ::toString(nextPageID).c_str());
			nextPageReader = waitOrError(queue->pager->readPage(nextPageID, true), queue->pagerError);
		}

		void writePage() {
			ASSERT(mode == WRITE);
			debug_printf("FIFOQueue::Cursor(%s) writePage\n", toString().c_str());
			VALGRIND_MAKE_MEM_DEFINED(raw()->begin(), offset);
			VALGRIND_MAKE_MEM_DEFINED(raw()->begin() + offset, queue->dataBytesPerPage - raw()->endOffset);
			queue->pager->updatePage(pageID, page);
			if (firstPageIDWritten == invalidLogicalPageID) {
				firstPageIDWritten = pageID;
			}
		}

		// Link the current page to newPageID:newOffset and then write it to the pager.
		// The link destination could be a new page at the end of the queue, or the beginning of
		// an existing chain of queue pages.
		// If initializeNewPage is true a page buffer will be allocated for the new page and it will be initialized
		// as a new tail page.
		void addNewPage(LogicalPageID newPageID, int newOffset, bool initializeNewPage) {
			ASSERT(mode == WRITE);
			ASSERT(newPageID != invalidLogicalPageID);
			debug_printf("FIFOQueue::Cursor(%s) Adding page %s init=%d\n",
			             toString().c_str(),
			             ::toString(newPageID).c_str(),
			             initializeNewPage);

			// Update existing page and write, if it exists
			if (page) {
				setNext(newPageID, newOffset);
				debug_printf("FIFOQueue::Cursor(%s) Linked new page %s:%d\n",
				             toString().c_str(),
				             ::toString(newPageID).c_str(),
				             newOffset);
				writePage();
			}

			pageID = newPageID;
			offset = newOffset;

			if (BUGGIFY) {
				// Randomly change the byte limit for queue pages.  The min here must be large enough for at least one
				// queue item of any type.  This change will suddenly make some pages being written to seem overfilled
				// but this won't break anything, the next write will just be detected as not fitting and the page will
				// end.
				queue->dataBytesPerPage = deterministicRandom()->randomInt(
				    50, queue->pager->getUsablePageSize() - sizeof(typename Cursor::RawPage));
			}

			if (initializeNewPage) {
				debug_printf("FIFOQueue::Cursor(%s) Initializing new page\n", toString().c_str());
				page = queue->pager->newPageBuffer();
				setNext(0, 0);
				auto p = raw();
				ASSERT(newOffset == 0);
				p->endOffset = 0;
			} else {
				page.clear();
			}
		}

		// Write item to the next position in the current page or, if it won't fit, add a new page and write it there.
		ACTOR static Future<Void> write_impl(Cursor* self, T item) {
			ASSERT(self->mode == WRITE);

			state bool mustWait = self->isBusy();
			state int bytesNeeded = Codec::bytesNeeded(item);
			state bool needNewPage =
			    self->pageID == invalidLogicalPageID || self->offset + bytesNeeded > self->queue->dataBytesPerPage;

			debug_printf("FIFOQueue::Cursor(%s) write(%s) mustWait=%d needNewPage=%d\n",
			             self->toString().c_str(),
			             ::toString(item).c_str(),
			             mustWait,
			             needNewPage);

			// If we have to wait for the mutex because it's busy, or we need a new page, then wait for the mutex.
			if (mustWait || needNewPage) {
				wait(self->mutex.take());

				// If we had to wait because the mutex was busy, then update needNewPage as another writer
				// would have changed the cursor state
				// Otherwise, taking the mutex would be immediate so no other writer could have run
				if (mustWait) {
					needNewPage = self->pageID == invalidLogicalPageID ||
					              self->offset + bytesNeeded > self->queue->dataBytesPerPage;
				}
			}

			// If we need a new page, add one.
			if (needNewPage) {
				debug_printf("FIFOQueue::Cursor(%s) write(%s) page is full, adding new page\n",
				             self->toString().c_str(),
				             ::toString(item).c_str());
				LogicalPageID newPageID = wait(self->queue->pager->newPageID());
				self->addNewPage(newPageID, 0, true);
				++self->queue->numPages;
			}

			debug_printf(
			    "FIFOQueue::Cursor(%s) write(%s) writing\n", self->toString().c_str(), ::toString(item).c_str());
			auto p = self->raw();
			Codec::writeToBytes(p->begin() + self->offset, item);
			self->offset += bytesNeeded;
			p->endOffset = self->offset;
			++self->queue->numEntries;

			if (mustWait || needNewPage) {
				self->mutex.release();
			}

			return Void();
		}

		void write(const T& item) {
			// Start the write.  It may complete immediately if no IO was being waited on
			Future<Void> w = write_impl(this, item);
			// If it didn't complete immediately, then store the future in operation
			if (!w.isReady()) {
				writeOperations = writeOperations && w;
			}
		}

		// If readNext() cannot complete immediately, it will route to here
		// The mutex will be taken if locked is false
		// The next page will be waited for if load is true
		// Only mutex holders will wait on the page read.
		ACTOR static Future<Optional<T>> waitThenReadNext(Cursor* self,
		                                                  Optional<T> upperBound,
		                                                  bool locked,
		                                                  bool load) {
			// Lock the mutex if it wasn't already
			if (!locked) {
				debug_printf("FIFOQueue::Cursor(%s) waitThenReadNext locking mutex\n", self->toString().c_str());
				wait(self->mutex.take());
			}

			if (load) {
				debug_printf("FIFOQueue::Cursor(%s) waitThenReadNext waiting for page load\n",
				             self->toString().c_str());
				wait(success(self->nextPageReader));
			}

			Optional<T> result = wait(self->readNext(upperBound, true));

			// If this actor instance locked the mutex, then unlock it.
			if (!locked) {
				debug_printf("FIFOQueue::Cursor(%s) waitThenReadNext unlocking mutex\n", self->toString().c_str());
				self->mutex.release();
			}

			return result;
		}

		// Read the next item at the cursor (if < upperBound), moving to a new page first if the current page is
		// exhausted If locked is true, this call owns the mutex, which would have been locked by readNext() before a
		// recursive call
		Future<Optional<T>> readNext(const Optional<T>& upperBound = {}, bool locked = false) {
			if ((mode != POP && mode != READONLY) || pageID == invalidLogicalPageID || pageID == endPageID) {
				debug_printf("FIFOQueue::Cursor(%s) readNext returning nothing\n", toString().c_str());
				return Optional<T>();
			}

			// If we don't own the mutex and it's not available then acquire it
			if (!locked && isBusy()) {
				return waitThenReadNext(this, upperBound, false, false);
			}

			// We now know pageID is valid and should be used, but page might not point to it yet
			if (!page) {
				debug_printf("FIFOQueue::Cursor(%s) loading\n", toString().c_str());

				// If the next pageID loading or loaded is not the page we should be reading then restart the load
				// nextPageID coud be different because it could be invalid or it could be no longer relevant
				// if the previous commit added new pages to the front of the queue.
				if (pageID != nextPageID) {
					debug_printf("FIFOQueue::Cursor(%s) reloading\n", toString().c_str());
					startNextPageLoad(pageID);
				}

				if (!nextPageReader.isReady()) {
					return waitThenReadNext(this, upperBound, locked, true);
				}

				page = nextPageReader.get();

				// Start loading the next page if it's not the end page
				auto p = raw();
				if (p->nextPageID != endPageID) {
					startNextPageLoad(p->nextPageID);
				} else {
					// Prevent a future next page read from reusing the same result as page would have to be updated
					// before the queue would read it again
					nextPageID = invalidLogicalPageID;
				}
			}

			auto p = raw();
			debug_printf("FIFOQueue::Cursor(%s) readNext reading at current position\n", toString().c_str());
			ASSERT(offset < p->endOffset);
			int bytesRead;
			const T result = Codec::readFromBytes(p->begin() + offset, bytesRead);

			if (upperBound.present() && upperBound.get() < result) {
				debug_printf("FIFOQueue::Cursor(%s) not popping %s, exceeds upper bound %s\n",
				             toString().c_str(),
				             ::toString(result).c_str(),
				             ::toString(upperBound.get()).c_str());

				return Optional<T>();
			}

			offset += bytesRead;
			if (mode == POP) {
				--queue->numEntries;
			}
			debug_printf("FIFOQueue::Cursor(%s) after read of %s\n", toString().c_str(), ::toString(result).c_str());
			ASSERT(offset <= p->endOffset);

			// If this page is exhausted, start reading the next page for the next readNext() to use, unless it's the
			// tail page
			if (offset == p->endOffset) {
				debug_printf("FIFOQueue::Cursor(%s) Page exhausted\n", toString().c_str());
				LogicalPageID oldPageID = pageID;
				pageID = p->nextPageID;
				offset = p->nextOffset;

				// If pageID isn't the tail page and nextPageID isn't pageID then start loading the next page
				if (pageID != endPageID && nextPageID != pageID) {
					startNextPageLoad(pageID);
				}

				if (mode == POP) {
					--queue->numPages;
				}
				page.clear();
				debug_printf("FIFOQueue::Cursor(%s) readNext page exhausted, moved to new page\n", toString().c_str());

				if (mode == POP) {
					// Freeing the old page must happen after advancing the cursor and clearing the page reference
					// because freePage() could cause a push onto a queue that causes a newPageID() call which could
					// pop() from this very same queue. Queue pages are freed at version 0 because they can be reused
					// after the next commit.
					queue->pager->freePage(oldPageID, 0);
				}
			}

			debug_printf("FIFOQueue(%s) %s(upperBound=%s) -> %s\n",
			             queue->name.c_str(),
			             (mode == POP ? "pop" : "peek"),
			             ::toString(upperBound).c_str(),
			             ::toString(result).c_str());
			return Optional<T>(result);
		}
	};

public:
	FIFOQueue() : pager(nullptr) {}

	~FIFOQueue() { newTailPage.cancel(); }

	FIFOQueue(const FIFOQueue& other) = delete;
	void operator=(const FIFOQueue& rhs) = delete;

	// Create a new queue at newPageID
	void create(IPager2* p, LogicalPageID newPageID, std::string queueName) {
		debug_printf("FIFOQueue(%s) create from page %s\n", queueName.c_str(), toString(newPageID).c_str());
		pager = p;
		pagerError = pager->getError();
		name = queueName;
		numPages = 1;
		numEntries = 0;
		dataBytesPerPage = pager->getUsablePageSize() - sizeof(typename Cursor::RawPage);
		headReader.init(this, Cursor::POP, newPageID, 0, newPageID);
		tailWriter.init(this, Cursor::WRITE, newPageID);
		headWriter.init(this, Cursor::WRITE);
		newTailPage = invalidLogicalPageID;
		debug_printf("FIFOQueue(%s) created\n", queueName.c_str());
	}

	// Load an existing queue from its queue state
	void recover(IPager2* p, const QueueState& qs, std::string queueName) {
		debug_printf("FIFOQueue(%s) recover from queue state %s\n", queueName.c_str(), qs.toString().c_str());
		pager = p;
		pagerError = pager->getError();
		name = queueName;
		numPages = qs.numPages;
		numEntries = qs.numEntries;
		dataBytesPerPage = pager->getUsablePageSize() - sizeof(typename Cursor::RawPage);
		headReader.init(this, Cursor::POP, qs.headPageID, qs.headOffset, qs.tailPageID);
		tailWriter.init(this, Cursor::WRITE, qs.tailPageID);
		headWriter.init(this, Cursor::WRITE);
		newTailPage = invalidLogicalPageID;
		debug_printf("FIFOQueue(%s) recovered\n", queueName.c_str());
	}

	ACTOR static Future<Standalone<VectorRef<T>>> peekAll_impl(FIFOQueue* self) {
		state Standalone<VectorRef<T>> results;
		state Cursor c;
		c.initReadOnly(self->headReader);
		results.reserve(results.arena(), self->numEntries);

		loop {
			Optional<T> x = wait(c.readNext());
			if (!x.present()) {
				break;
			}
			results.push_back(results.arena(), x.get());
		}

		return results;
	}

	Future<Standalone<VectorRef<T>>> peekAll() { return peekAll_impl(this); }

	ACTOR static Future<Optional<T>> peek_impl(FIFOQueue* self) {
		state Cursor c;
		c.initReadOnly(self->headReader);

		Optional<T> x = wait(c.readNext());
		return x;
	}

	Future<Optional<T>> peek() { return peek_impl(this); }

	// Pop the next item on front of queue if it is <= upperBound or if upperBound is not present
	Future<Optional<T>> pop(Optional<T> upperBound = {}) { return headReader.readNext(upperBound); }

	QueueState getState() const {
		QueueState s;
		s.headOffset = headReader.offset;
		s.headPageID = headReader.pageID;
		s.tailPageID = tailWriter.pageID;
		s.numEntries = numEntries;
		s.numPages = numPages;

		debug_printf("FIFOQueue(%s) getState(): %s\n", name.c_str(), s.toString().c_str());
		return s;
	}

	void pushBack(const T& item) {
		debug_printf("FIFOQueue(%s) pushBack(%s)\n", name.c_str(), toString(item).c_str());
		tailWriter.write(item);
	}

	void pushFront(const T& item) {
		debug_printf("FIFOQueue(%s) pushFront(%s)\n", name.c_str(), toString(item).c_str());
		headWriter.write(item);
	}

	bool isBusy() {
		return headWriter.isBusy() || headReader.isBusy() || tailWriter.isBusy() || !newTailPage.isReady();
	}

	// Wait until all previously started operations on each cursor are done and the new tail page is ready
	Future<Void> notBusy() {
		auto f = headWriter.notBusy() && headReader.notBusy() && tailWriter.notBusy() && ready(newTailPage);
		debug_printf("FIFOQueue(%s) notBusy future ready=%d\n", name.c_str(), f.isReady());
		return f;
	}

	// preFlush() prepares this queue to be flushed to disk, but doesn't actually do it so the queue can still
	// be pushed and popped after this operation. It returns whether or not any operations were pending or
	// started during execution.
	//
	// If one or more queues are used by their pager in newPageID() or freePage() operations, then preFlush()
	// must be called on each of them inside a loop that runs until each of the preFlush() calls have returned
	// false twice in a row.
	//
	// The reason for all this is that:
	//   - queue pop() can call pager->freePage() which can call push() on the same or another queue
	//   - queue push() can call pager->newPageID() which can call pop() on the same or another queue
	// This creates a circular dependency with 1 or more queues when those queues are used by the pager
	// to manage free page IDs.
	ACTOR static Future<bool> preFlush_impl(FIFOQueue* self) {
		debug_printf("FIFOQueue(%s) preFlush begin\n", self->name.c_str());
		wait(self->notBusy());

		// Completion of the pending operations as of the start of notBusy() could have began new operations,
		// so see if any work is pending now.
		bool workPending = self->isBusy();

		if (!workPending) {
			// A newly created or flushed queue starts out in a state where its tail page to be written to is empty.
			// After pushBack() is called, this is no longer the case and never will be again until the queue is
			// flushed. Before the non-empty tail page is written it must be linked to a new empty page for use after
			// the next flush.  (This is explained more at the top of FIFOQueue but it is because queue pages can only
			// be written once because once they contain durable data a second write to link to a new page could corrupt
			// the existing data if the subsequent commit never succeeds.)
			//
			// If the newTailPage future is ready but it's an invalid page and the tail page we are currently pointed to
			// has had items added to it, then get a new tail page ID.
			if (self->newTailPage.isReady() && self->newTailPage.get() == invalidLogicalPageID &&
			    self->tailWriter.pendingTailWrites()) {
				debug_printf("FIFOQueue(%s) preFlush starting to get new page ID\n", self->name.c_str());
				self->newTailPage = self->pager->newPageID();
				workPending = true;
			}
		}

		debug_printf("FIFOQueue(%s) preFlush returning %d\n", self->name.c_str(), workPending);
		return workPending;
	}

	Future<bool> preFlush() { return preFlush_impl(this); }

	void finishFlush() {
		debug_printf("FIFOQueue(%s) finishFlush start\n", name.c_str());
		ASSERT(!isBusy());

		// If a new tail page was allocated, link the last page of the tail writer to it.
		if (newTailPage.get() != invalidLogicalPageID) {
			tailWriter.addNewPage(newTailPage.get(), 0, false);
			// The flush sequence allocated a page and added it to the queue so increment numPages
			++numPages;

			// newPage() should be ready immediately since a pageID is being explicitly passed.
			ASSERT(!tailWriter.isBusy());

			newTailPage = invalidLogicalPageID;
		}

		// If the headWriter wrote anything, link its tail page to the headReader position and point the headReader
		// to the start of the headWriter
		if (headWriter.pendingTailWrites()) {
			headWriter.addNewPage(headReader.pageID, headReader.offset, false);
			headReader.pageID = headWriter.firstPageIDWritten;
			headReader.offset = 0;
			headReader.page.clear();
		}

		// Update headReader's end page to the new tail page
		headReader.endPageID = tailWriter.pageID;

		// Reset the write cursors
		tailWriter.init(this, Cursor::WRITE, tailWriter.pageID);
		headWriter.init(this, Cursor::WRITE);

		debug_printf("FIFOQueue(%s) finishFlush end\n", name.c_str());
	}

	ACTOR static Future<Void> flush_impl(FIFOQueue* self) {
		loop {
			bool notDone = wait(self->preFlush());
			if (!notDone) {
				break;
			}
		}
		self->finishFlush();
		return Void();
	}

	Future<Void> flush() { return flush_impl(this); }

	IPager2* pager;
	Future<Void> pagerError;

	int64_t numPages;
	int64_t numEntries;
	int dataBytesPerPage;

	Cursor headReader;
	Cursor tailWriter;
	Cursor headWriter;

	Future<LogicalPageID> newTailPage;

	// For debugging
	std::string name;
};

int nextPowerOf2(uint32_t x) {
	return 1 << (32 - clz(x - 1));
}

struct RedwoodMetrics {
	static constexpr int btreeLevels = 5;

	RedwoodMetrics() { clear(); }

	void clear() {
		memset(this, 0, sizeof(RedwoodMetrics));
		for (auto& level : levels) {
			level = {};
		}
		startTime = g_network ? now() : 0;
	}

	struct Level {
		unsigned int pageRead;
		unsigned int pageReadExt;
		unsigned int pageBuild;
		unsigned int pageBuildExt;
		unsigned int pageCommitStart;
		unsigned int pageModify;
		unsigned int pageModifyExt;
		unsigned int lazyClearRequeue;
		unsigned int lazyClearRequeueExt;
		unsigned int lazyClearFree;
		unsigned int lazyClearFreeExt;
		unsigned int forceUpdate;
		unsigned int detachChild;
		double buildStoredPct;
		double buildFillPct;
		unsigned int buildItemCount;
		double modifyStoredPct;
		double modifyFillPct;
		unsigned int modifyItemCount;
	};

	Level levels[btreeLevels];

	unsigned int opSet;
	unsigned int opSetKeyBytes;
	unsigned int opSetValueBytes;
	unsigned int opClear;
	unsigned int opClearKey;
	unsigned int opCommit;
	unsigned int opGet;
	unsigned int opGetRange;
	unsigned int pagerDiskWrite;
	unsigned int pagerDiskRead;
	unsigned int pagerRemapFree;
	unsigned int pagerRemapCopy;
	unsigned int pagerRemapSkip;
	unsigned int pagerCacheHit;
	unsigned int pagerCacheMiss;
	unsigned int pagerProbeHit;
	unsigned int pagerProbeMiss;
	unsigned int pagerEvictUnhit;
	unsigned int pagerEvictFail;
	unsigned int btreeLeafPreload;
	unsigned int btreeLeafPreloadExt;

	// Return number of pages read or written, from cache or disk
	unsigned int pageOps() const {
		// All page reads are either a cache hit, probe hit, or a disk read
		return pagerDiskWrite + pagerDiskRead + pagerCacheHit + pagerProbeHit;
	}

	double startTime;

	Level& level(unsigned int level) {
		static Level outOfBound;
		if (level == 0 || level > btreeLevels) {
			return outOfBound;
		}
		return levels[level - 1];
	}

	// This will populate a trace event and/or a string with Redwood metrics.
	// The string is a reasonably well formatted page of information
	void getFields(TraceEvent* e, std::string* s = nullptr, bool skipZeroes = false) {
		std::pair<const char*, unsigned int> metrics[] = { { "BTreePreload", btreeLeafPreload },
			                                               { "BTreePreloadExt", btreeLeafPreloadExt },
			                                               { "", 0 },
			                                               { "OpSet", opSet },
			                                               { "OpSetKeyBytes", opSetKeyBytes },
			                                               { "OpSetValueBytes", opSetValueBytes },
			                                               { "OpClear", opClear },
			                                               { "OpClearKey", opClearKey },
			                                               { "", 0 },
			                                               { "OpGet", opGet },
			                                               { "OpGetRange", opGetRange },
			                                               { "OpCommit", opCommit },
			                                               { "", 0 },
			                                               { "PagerDiskWrite", pagerDiskWrite },
			                                               { "PagerDiskRead", pagerDiskRead },
			                                               { "PagerCacheHit", pagerCacheHit },
			                                               { "PagerCacheMiss", pagerCacheMiss },
			                                               { "", 0 },
			                                               { "PagerProbeHit", pagerProbeHit },
			                                               { "PagerProbeMiss", pagerProbeMiss },
			                                               { "PagerEvictUnhit", pagerEvictUnhit },
			                                               { "PagerEvictFail", pagerEvictFail },
			                                               { "", 0 },
			                                               { "PagerRemapFree", pagerRemapFree },
			                                               { "PagerRemapCopy", pagerRemapCopy },
			                                               { "PagerRemapSkip", pagerRemapSkip } };
		double elapsed = now() - startTime;

		if (e != nullptr) {
			for (auto& m : metrics) {
				char c = m.first[0];
				if (c != 0 && (!skipZeroes || m.second != 0)) {
					e->detail(m.first, m.second);
				}
			}
		}

		if (s != nullptr) {
			for (auto& m : metrics) {
				if (*m.first == '\0') {
					*s += "\n";
				} else if (!skipZeroes || m.second != 0) {
					*s += format("%-15s %-8u %8" PRId64 "/s  ", m.first, m.second, int64_t(m.second / elapsed));
				}
			}
		}

		for (int i = 0; i < btreeLevels; ++i) {
			auto& level = levels[i];
			std::pair<const char*, unsigned int> metrics[] = {
				{ "PageBuild", level.pageBuild },
				{ "PageBuildExt", level.pageBuildExt },
				{ "PageModify", level.pageModify },
				{ "PageModifyExt", level.pageModifyExt },
				{ "", 0 },
				{ "PageRead", level.pageRead },
				{ "PageReadExt", level.pageReadExt },
				{ "PageCommitStart", level.pageCommitStart },
				{ "", 0 },
				{ "LazyClearInt", level.lazyClearRequeue },
				{ "LazyClearIntExt", level.lazyClearRequeueExt },
				{ "LazyClear", level.lazyClearFree },
				{ "LazyClearExt", level.lazyClearFreeExt },
				{ "", 0 },
				{ "ForceUpdate", level.forceUpdate },
				{ "DetachChild", level.detachChild },
				{ "", 0 },
				{ "-BldAvgCount", level.pageBuild ? level.buildItemCount / level.pageBuild : 0 },
				{ "-BldAvgFillPct", level.pageBuild ? level.buildFillPct / level.pageBuild * 100 : 0 },
				{ "-BldAvgStoredPct", level.pageBuild ? level.buildStoredPct / level.pageBuild * 100 : 0 },
				{ "", 0 },
				{ "-ModAvgCount", level.pageModify ? level.modifyItemCount / level.pageModify : 0 },
				{ "-ModAvgFillPct", level.pageModify ? level.modifyFillPct / level.pageModify * 100 : 0 },
				{ "-ModAvgStoredPct", level.pageModify ? level.modifyStoredPct / level.pageModify * 100 : 0 },
				{ "", 0 },
			};

			if (e != nullptr) {
				for (auto& m : metrics) {
					char c = m.first[0];
					if (c != 0 && (!skipZeroes || m.second != 0)) {
						e->detail(format("L%d%s", i + 1, m.first + (c == '-' ? 1 : 0)), m.second);
					}
				}
			}

			if (s != nullptr) {
				*s += format("\nLevel %d\n\t", i + 1);

				for (auto& m : metrics) {
					const char* name = m.first;
					bool rate = elapsed != 0;
					if (*name == '-') {
						++name;
						rate = false;
					}

					if (*name == '\0') {
						*s += "\n\t";
					} else if (!skipZeroes || m.second != 0) {
						*s += format("%-15s %8u %8u/s  ", name, m.second, rate ? int(m.second / elapsed) : 0);
					}
				}
			}
		}
	}

	std::string toString(bool clearAfter) {
		std::string s;
		getFields(nullptr, &s);

		if (clearAfter) {
			clear();
		}

		return s;
	}
};

// Using a global for Redwood metrics because a single process shouldn't normally have multiple storage engines
RedwoodMetrics g_redwoodMetrics = {};
Future<Void> g_redwoodMetricsActor;

ACTOR Future<Void> redwoodMetricsLogger() {
	g_redwoodMetrics.clear();

	loop {
		wait(delay(SERVER_KNOBS->REDWOOD_LOGGING_INTERVAL));

		TraceEvent e("RedwoodMetrics");
		double elapsed = now() - g_redwoodMetrics.startTime;
		e.detail("Elapsed", elapsed);
		g_redwoodMetrics.getFields(&e);
		g_redwoodMetrics.clear();
	}
}

// Holds an index of recently used objects.
// ObjectType must have the methods
//   bool evictable() const;            // return true if the entry can be evicted
//   Future<Void> onEvictable() const;  // ready when entry can be evicted
template <class IndexType, class ObjectType>
class ObjectCache : NonCopyable {

	struct Entry : public boost::intrusive::list_base_hook<> {
		Entry() : hits(0) {}
		IndexType index;
		ObjectType item;
		int hits;
	};

	typedef std::unordered_map<IndexType, Entry> CacheT;
	typedef boost::intrusive::list<Entry> EvictionOrderT;

public:
	ObjectCache(int sizeLimit = 1) : sizeLimit(sizeLimit) {}

	void setSizeLimit(int n) {
		ASSERT(n > 0);
		sizeLimit = n;
	}

	// Get the object for i if it exists, else return nullptr.
	// If the object exists, its eviction order will NOT change as this is not a cache hit.
	ObjectType* getIfExists(const IndexType& index) {
		auto i = cache.find(index);
		if (i != cache.end()) {
			++i->second.hits;
			++g_redwoodMetrics.pagerProbeHit;
			return &i->second.item;
		}
		++g_redwoodMetrics.pagerProbeMiss;
		return nullptr;
	}

	// Try to evict the item at index from cache
	// Returns true if item is evicted or was not present in cache
	bool tryEvict(const IndexType& index) {
		auto i = cache.find(index);
		if (i == cache.end() || !i->second.item.evictable()) {
			return false;
		}
		Entry& toEvict = i->second;
		if (toEvict.hits == 0) {
			++g_redwoodMetrics.pagerEvictUnhit;
		}
		evictionOrder.erase(evictionOrder.iterator_to(toEvict));
		cache.erase(toEvict.index);
		return true;
	}

	// Get the object for i or create a new one.
	// After a get(), the object for i is the last in evictionOrder.
	// If noHit is set, do not consider this access to be cache hit if the object is present
	// If noMiss is set, do not consider this access to be a cache miss if the object is not present
	ObjectType& get(const IndexType& index, bool noHit = false, bool noMiss = false) {
		Entry& entry = cache[index];

		// If entry is linked into evictionOrder then move it to the back of the order
		if (entry.is_linked()) {
			if (!noHit) {
				++entry.hits;
				++g_redwoodMetrics.pagerCacheHit;

				// Move the entry to the back of the eviction order
				evictionOrder.erase(evictionOrder.iterator_to(entry));
				evictionOrder.push_back(entry);
			}
		} else {
			if (!noMiss) {
				++g_redwoodMetrics.pagerCacheMiss;
			}
			// Finish initializing entry
			entry.index = index;
			entry.hits = 0;
			// Insert the newly created Entry at the back of the eviction order
			evictionOrder.push_back(entry);

			// While the cache is too big, evict the oldest entry until the oldest entry can't be evicted.
			while (cache.size() > sizeLimit) {
				Entry& toEvict = evictionOrder.front();

				// It's critical that we do not evict the item we just added because it would cause the reference
				// returned to be invalid.  An eviction could happen with a no-hit access to a cache resident page
				// that is currently evictable and exists in the oversized portion of the cache eviction order due
				// to previously failed evictions.
				if (&entry == &toEvict) {
					debug_printf("Cannot evict target index %s\n", toString(index).c_str());
					break;
				}

				debug_printf("Trying to evict %s to make room for %s\n",
				             toString(toEvict.index).c_str(),
				             toString(index).c_str());

				if (!toEvict.item.evictable()) {
					evictionOrder.erase(evictionOrder.iterator_to(toEvict));
					evictionOrder.push_back(toEvict);
					++g_redwoodMetrics.pagerEvictFail;
					break;
				} else {
					if (toEvict.hits == 0) {
						++g_redwoodMetrics.pagerEvictUnhit;
					}
					debug_printf(
					    "Evicting %s to make room for %s\n", toString(toEvict.index).c_str(), toString(index).c_str());
					evictionOrder.pop_front();
					cache.erase(toEvict.index);
				}
			}
		}

		return entry.item;
	}

	// Clears the cache, saving the entries, and then waits for eachWaits for each item to be evictable and evicts it.
	// The cache should not be Evicts all evictable entries
	ACTOR static Future<Void> clear_impl(ObjectCache* self) {
		state ObjectCache::CacheT cache;
		state EvictionOrderT evictionOrder;

		// Swap cache contents to local state vars
		// After this, no more entries will be added to or read from these
		// structures so we know for sure that no page will become unevictable
		// after it is either evictable or onEvictable() is ready.
		cache.swap(self->cache);
		evictionOrder.swap(self->evictionOrder);

		state typename EvictionOrderT::iterator i = evictionOrder.begin();
		state typename EvictionOrderT::iterator iEnd = evictionOrder.begin();

		while (i != iEnd) {
			if (!i->item.evictable()) {
				wait(i->item.onEvictable());
			}
			++i;
		}

		evictionOrder.clear();
		cache.clear();

		return Void();
	}

	Future<Void> clear() {
		ASSERT(evictionOrder.size() == cache.size());
		return clear_impl(this);
	}

	int count() const { return evictionOrder.size(); }

private:
	int64_t sizeLimit;

	CacheT cache;
	EvictionOrderT evictionOrder;
};

ACTOR template <class T>
Future<T> forwardError(Future<T> f, Promise<Void> target) {
	try {
		T x = wait(f);
		return x;
	} catch (Error& e) {
		if (e.code() != error_code_actor_cancelled && target.canBeSet()) {
			target.sendError(e);
		}

		throw e;
	}
}

class DWALPagerSnapshot;

// An implementation of IPager2 that supports atomicUpdate() of a page without forcing a change to new page ID.
// It does this internally mapping the original page ID to alternate page IDs by write version.
// The page id remaps are kept in memory and also logged to a "remap queue" which must be reloaded on cold start.
// To prevent the set of remaps from growing unboundedly, once a remap is old enough to be at or before the
// oldest pager version being maintained the remap can be "undone" by popping it from the remap queue,
// copying the alternate page ID's data over top of the original page ID's data, and deleting the remap from memory.
// This process basically describes a "Delayed" Write-Ahead-Log (DWAL) because the remap queue and the newly allocated
// alternate pages it references basically serve as a write ahead log for pages that will eventially be copied
// back to their original location once the original version is no longer needed.
class DWALPager : public IPager2 {
public:
	typedef FIFOQueue<LogicalPageID> LogicalPageQueueT;
	typedef std::map<Version, LogicalPageID> VersionToPageMapT;
	typedef std::unordered_map<LogicalPageID, VersionToPageMapT> PageToVersionedMapT;

#pragma pack(push, 1)
	struct DelayedFreePage {
		Version version;
		LogicalPageID pageID;

		bool operator<(const DelayedFreePage& rhs) const { return version < rhs.version; }

		std::string toString() const {
			return format("DelayedFreePage{%s @%" PRId64 "}", ::toString(pageID).c_str(), version);
		}
	};

	struct RemappedPage {
		enum Type { NONE = 'N', REMAP = 'R', FREE = 'F', DETACH = 'D' };
		RemappedPage(Version v = invalidVersion,
		             LogicalPageID o = invalidLogicalPageID,
		             LogicalPageID n = invalidLogicalPageID)
		  : version(v), originalPageID(o), newPageID(n) {}

		Version version;
		LogicalPageID originalPageID;
		LogicalPageID newPageID;

		static Type getTypeOf(LogicalPageID newPageID) {
			if (newPageID == invalidLogicalPageID) {
				return FREE;
			}
			if (newPageID == 0) {
				return DETACH;
			}
			return REMAP;
		}

		Type getType() const { return getTypeOf(newPageID); }

		bool operator<(const RemappedPage& rhs) const { return version < rhs.version; }

		std::string toString() const {
			return format("RemappedPage(%c: %s -> %s %s}",
			              getType(),
			              ::toString(originalPageID).c_str(),
			              ::toString(newPageID).c_str(),
			              ::toString(version).c_str());
		}
	};

#pragma pack(pop)

	typedef FIFOQueue<DelayedFreePage> DelayedFreePageQueueT;
	typedef FIFOQueue<RemappedPage> RemapQueueT;

	// If the file already exists, pageSize might be different than desiredPageSize
	// Use pageCacheSizeBytes == 0 to use default from flow knobs
	// If filename is empty, the pager will exist only in memory and once the cache is full writes will fail.
	DWALPager(int desiredPageSize,
	          std::string filename,
	          int64_t pageCacheSizeBytes,
	          Version remapCleanupWindow,
	          bool memoryOnly = false)
	  : desiredPageSize(desiredPageSize), filename(filename), pHeader(nullptr), pageCacheBytes(pageCacheSizeBytes),
	    memoryOnly(memoryOnly), remapCleanupWindow(remapCleanupWindow) {

		if (!g_redwoodMetricsActor.isValid()) {
			g_redwoodMetricsActor = redwoodMetricsLogger();
		}

		commitFuture = Void();
		recoverFuture = forwardError(recover(this), errorPromise);
	}

	void setPageSize(int size) {
		logicalPageSize = size;
		// Physical page size is the total size of the smallest number of physical blocks needed to store
		// logicalPageSize bytes
		int blocks = 1 + ((logicalPageSize - 1) / smallestPhysicalBlock);
		physicalPageSize = blocks * smallestPhysicalBlock;
		if (pHeader != nullptr) {
			pHeader->pageSize = logicalPageSize;
		}
		pageCache.setSizeLimit(1 + ((pageCacheBytes - 1) / physicalPageSize));
	}

	void updateCommittedHeader() {
		memcpy(lastCommittedHeaderPage->mutate(), headerPage->begin(), smallestPhysicalBlock);
	}

	ACTOR static Future<Void> recover(DWALPager* self) {
		ASSERT(!self->recoverFuture.isValid());

		state bool exists = false;

		if (!self->memoryOnly) {
			int64_t flags = IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_UNBUFFERED | IAsyncFile::OPEN_READWRITE |
			                IAsyncFile::OPEN_LOCK;
			exists = fileExists(self->filename);
			if (!exists) {
				flags |= IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE;
			}

			wait(store(self->pageFile, IAsyncFileSystem::filesystem()->open(self->filename, flags, 0644)));
		}

		// Header page is always treated as having a page size of smallestPhysicalBlock
		self->setPageSize(smallestPhysicalBlock);
		self->lastCommittedHeaderPage = self->newPageBuffer();
		self->pLastCommittedHeader = (Header*)self->lastCommittedHeaderPage->begin();

		state int64_t fileSize = 0;
		if (exists) {
			wait(store(fileSize, self->pageFile->size()));
		}

		debug_printf(
		    "DWALPager(%s) recover exists=%d fileSize=%" PRId64 "\n", self->filename.c_str(), exists, fileSize);
		// TODO:  If the file exists but appears to never have been successfully committed is this an error or
		// should recovery proceed with a new pager instance?

		// If there are at least 2 pages then try to recover the existing file
		if (exists && fileSize >= (self->smallestPhysicalBlock * 2)) {
			debug_printf("DWALPager(%s) recovering using existing file\n", self->filename.c_str());

			state bool recoveredHeader = false;

			// Read physical page 0 directly
			wait(store(self->headerPage, self->readHeaderPage(self, 0)));

			// If the checksum fails for the header page, try to recover committed header backup from page 1
			if (!self->headerPage->verifyChecksum(0)) {
				TraceEvent(SevWarn, "DWALPagerRecoveringHeader").detail("Filename", self->filename);

				wait(store(self->headerPage, self->readHeaderPage(self, 1)));

				if (!self->headerPage->verifyChecksum(1)) {
					if (g_network->isSimulated()) {
						// TODO: Detect if process is being restarted and only throw injected if so?
						throw io_error().asInjectedFault();
					}

					Error e = checksum_failed();
					TraceEvent(SevError, "DWALPagerRecoveryFailed").detail("Filename", self->filename).error(e);
					throw e;
				}
				recoveredHeader = true;
			}

			self->pHeader = (Header*)self->headerPage->begin();

			if (self->pHeader->formatVersion != Header::FORMAT_VERSION) {
				Error e = internal_error(); // TODO:  Something better?
				TraceEvent(SevError, "DWALPagerRecoveryFailedWrongVersion")
				    .detail("Filename", self->filename)
				    .detail("Version", self->pHeader->formatVersion)
				    .detail("ExpectedVersion", Header::FORMAT_VERSION)
				    .error(e);
				throw e;
			}

			self->setPageSize(self->pHeader->pageSize);
			if (self->logicalPageSize != self->desiredPageSize) {
				TraceEvent(SevWarn, "DWALPagerPageSizeNotDesired")
				    .detail("Filename", self->filename)
				    .detail("ExistingPageSize", self->logicalPageSize)
				    .detail("DesiredPageSize", self->desiredPageSize);
			}

			self->freeList.recover(self, self->pHeader->freeList, "FreeListRecovered");
			self->delayedFreeList.recover(self, self->pHeader->delayedFreeList, "DelayedFreeListRecovered");
			self->remapQueue.recover(self, self->pHeader->remapQueue, "RemapQueueRecovered");

			Standalone<VectorRef<RemappedPage>> remaps = wait(self->remapQueue.peekAll());
			for (auto& r : remaps) {
				self->remappedPages[r.originalPageID][r.version] = r.newPageID;
			}

			// If the header was recovered from the backup at Page 1 then write and sync it to Page 0 before continuing.
			// If this fails, the backup header is still in tact for the next recovery attempt.
			if (recoveredHeader) {
				// Write the header to page 0
				wait(self->writeHeaderPage(0, self->headerPage));

				// Wait for all outstanding writes to complete
				wait(self->operations.signalAndCollapse());

				// Sync header
				wait(self->pageFile->sync());
				debug_printf("DWALPager(%s) Header recovery complete.\n", self->filename.c_str());
			}

			// Update the last committed header with the one that was recovered (which is the last known committed
			// header)
			self->updateCommittedHeader();
			self->addLatestSnapshot();
			self->remapCleanupFuture = remapCleanup(self);
		} else {
			// Note: If the file contains less than 2 pages but more than 0 bytes then the pager was never successfully
			// committed. A new pager will be created in its place.
			// TODO:  Is the right behavior?

			debug_printf("DWALPager(%s) creating new pager\n", self->filename.c_str());

			self->headerPage = self->newPageBuffer();
			self->pHeader = (Header*)self->headerPage->begin();

			// Now that the header page has been allocated, set page size to desired
			self->setPageSize(self->desiredPageSize);

			// Write new header using desiredPageSize
			self->pHeader->formatVersion = Header::FORMAT_VERSION;
			self->pHeader->committedVersion = 1;
			self->pHeader->oldestVersion = 1;
			// No meta key until a user sets one and commits
			self->pHeader->setMetaKey(Key());

			// There are 2 reserved pages:
			//   Page 0 - header
			//   Page 1 - header backup
			self->pHeader->pageCount = 2;

			// Create queues
			self->freeList.create(self, self->newLastPageID(), "FreeList");
			self->delayedFreeList.create(self, self->newLastPageID(), "DelayedFreeList");
			self->remapQueue.create(self, self->newLastPageID(), "RemapQueue");

			// The first commit() below will flush the queues and update the queue states in the header,
			// but since the queues will not be used between now and then their states will not change.
			// In order to populate lastCommittedHeader, update the header now with the queue states.
			self->pHeader->freeList = self->freeList.getState();
			self->pHeader->delayedFreeList = self->delayedFreeList.getState();
			self->pHeader->remapQueue = self->remapQueue.getState();

			// Set remaining header bytes to \xff
			memset(self->headerPage->mutate() + self->pHeader->size(),
			       0xff,
			       self->headerPage->size() - self->pHeader->size());

			// Since there is no previously committed header use the initial header for the initial commit.
			self->updateCommittedHeader();

			self->remapCleanupFuture = Void();
			wait(self->commit());
		}

		debug_printf("DWALPager(%s) recovered.  committedVersion=%" PRId64 " logicalPageSize=%d physicalPageSize=%d\n",
		             self->filename.c_str(),
		             self->pHeader->committedVersion,
		             self->logicalPageSize,
		             self->physicalPageSize);
		return Void();
	}

	Reference<ArenaPage> newPageBuffer() override {
		return Reference<ArenaPage>(new ArenaPage(logicalPageSize, physicalPageSize));
	}

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	// TODO: This is abstraction breaking.  This should probably be stored as a member, calculated once on construction
	// by creating an ArenaPage and getting its usable size.
	int getUsablePageSize() const override { return logicalPageSize - sizeof(ArenaPage::Checksum); }

	// Get a new, previously available page ID.  The page will be considered in-use after the next commit
	// regardless of whether or not it was written to, until it is returned to the pager via freePage()
	ACTOR static Future<LogicalPageID> newPageID_impl(DWALPager* self) {
		// First try the free list
		Optional<LogicalPageID> freePageID = wait(self->freeList.pop());
		if (freePageID.present()) {
			debug_printf("DWALPager(%s) newPageID() returning %s from free list\n",
			             self->filename.c_str(),
			             toString(freePageID.get()).c_str());
			return freePageID.get();
		}

		// Try to reuse pages up to the earlier of the oldest version set by the user or the oldest snapshot still in
		// the snapshots list
		ASSERT(!self->snapshots.empty());
		Optional<DelayedFreePage> delayedFreePageID =
		    wait(self->delayedFreeList.pop(DelayedFreePage{ self->effectiveOldestVersion(), 0 }));
		if (delayedFreePageID.present()) {
			debug_printf("DWALPager(%s) newPageID() returning %s from delayed free list\n",
			             self->filename.c_str(),
			             toString(delayedFreePageID.get()).c_str());
			return delayedFreePageID.get().pageID;
		}

		// Lastly, add a new page to the pager
		LogicalPageID id = self->newLastPageID();
		debug_printf(
		    "DWALPager(%s) newPageID() returning %s at end of file\n", self->filename.c_str(), toString(id).c_str());
		return id;
	};

	// Grow the pager file by pone page and return it
	LogicalPageID newLastPageID() {
		LogicalPageID id = pHeader->pageCount;
		++pHeader->pageCount;
		return id;
	}

	Future<LogicalPageID> newPageID() override { return newPageID_impl(this); }

	Future<Void> writePhysicalPage(PhysicalPageID pageID, Reference<ArenaPage> page, bool header = false) {
		debug_printf("DWALPager(%s) op=%s %s ptr=%p\n",
		             filename.c_str(),
		             (header ? "writePhysicalHeader" : "writePhysical"),
		             toString(pageID).c_str(),
		             page->begin());

		++g_redwoodMetrics.pagerDiskWrite;
		VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
		page->updateChecksum(pageID);

		if (memoryOnly) {
			return Void();
		}

		// Note:  Not using forwardError here so a write error won't be discovered until commit time.
		int blockSize = header ? smallestPhysicalBlock : physicalPageSize;
		Future<Void> f =
		    holdWhile(page, map(pageFile->write(page->begin(), blockSize, (int64_t)pageID * blockSize), [=](Void) {
			              debug_printf("DWALPager(%s) op=%s %s ptr=%p\n",
			                           filename.c_str(),
			                           (header ? "writePhysicalHeaderComplete" : "writePhysicalComplete"),
			                           toString(pageID).c_str(),
			                           page->begin());
			              return Void();
		              }));
		operations.add(f);
		return f;
	}

	Future<Void> writeHeaderPage(PhysicalPageID pageID, Reference<ArenaPage> page) {
		return writePhysicalPage(pageID, page, true);
	}

	void updatePage(LogicalPageID pageID, Reference<ArenaPage> data) override {
		// Get the cache entry for this page, without counting it as a cache hit as we're replacing its contents now
		// or as a cache miss because there is no benefit to the page already being in cache
		PageCacheEntry& cacheEntry = pageCache.get(pageID, true, true);
		debug_printf("DWALPager(%s) op=write %s cached=%d reading=%d writing=%d\n",
		             filename.c_str(),
		             toString(pageID).c_str(),
		             cacheEntry.initialized(),
		             cacheEntry.initialized() && cacheEntry.reading(),
		             cacheEntry.initialized() && cacheEntry.writing());

		// If the page is still being read then it's not also being written because a write places
		// the new content into readFuture when the write is launched, not when it is completed.
		// Read/write ordering is being enforced so waiting readers will not see the new write.  This
		// is necessary for remap erasure to work correctly since the oldest version of a page, located
		// at the original page ID, could have a pending read when that version is expired (after which
		// future reads of the version are not allowed) and the write of the next newest version over top
		// of the original page begins.
		if (!cacheEntry.initialized()) {
			cacheEntry.writeFuture = writePhysicalPage(pageID, data);
		} else if (cacheEntry.reading()) {
			// Wait for the read to finish, then start the write.
			cacheEntry.writeFuture = map(success(cacheEntry.readFuture), [=](Void) {
				writePhysicalPage(pageID, data);
				return Void();
			});
		}
		// If the page is being written, wait for this write before issuing the new write to ensure the
		// writes happen in the correct order
		else if (cacheEntry.writing()) {
			cacheEntry.writeFuture = map(cacheEntry.writeFuture, [=](Void) {
				writePhysicalPage(pageID, data);
				return Void();
			});
		} else {
			cacheEntry.writeFuture = writePhysicalPage(pageID, data);
		}

		// Always update the page contents immediately regardless of what happened above.
		cacheEntry.readFuture = data;
	}

	Future<LogicalPageID> atomicUpdatePage(LogicalPageID pageID, Reference<ArenaPage> data, Version v) override {
		debug_printf("DWALPager(%s) op=writeAtomic %s @%" PRId64 "\n", filename.c_str(), toString(pageID).c_str(), v);
		Future<LogicalPageID> f = map(newPageID(), [=](LogicalPageID newPageID) {
			updatePage(newPageID, data);
			// TODO:  Possibly limit size of remap queue since it must be recovered on cold start
			RemappedPage r{ v, pageID, newPageID };
			remapQueue.pushBack(r);
			remappedPages[pageID][v] = newPageID;
			debug_printf("DWALPager(%s) pushed %s\n", filename.c_str(), RemappedPage(r).toString().c_str());
			return pageID;
		});

		// No need for forwardError here because newPageID() is already wrapped in forwardError
		return f;
	}

	void freeUnmappedPage(LogicalPageID pageID, Version v) {
		// If v is older than the oldest version still readable then mark pageID as free as of the next commit
		if (v < effectiveOldestVersion()) {
			debug_printf("DWALPager(%s) op=freeNow %s @%" PRId64 " oldestVersion=%" PRId64 "\n",
			             filename.c_str(),
			             toString(pageID).c_str(),
			             v,
			             pLastCommittedHeader->oldestVersion);
			freeList.pushBack(pageID);
		} else {
			// Otherwise add it to the delayed free list
			debug_printf("DWALPager(%s) op=freeLater %s @%" PRId64 " oldestVersion=%" PRId64 "\n",
			             filename.c_str(),
			             toString(pageID).c_str(),
			             v,
			             pLastCommittedHeader->oldestVersion);
			delayedFreeList.pushBack({ v, pageID });
		}
	}

	LogicalPageID detachRemappedPage(LogicalPageID pageID, Version v) override {
		auto i = remappedPages.find(pageID);
		if (i == remappedPages.end()) {
			// Page is not remapped
			return invalidLogicalPageID;
		}

		// Get the page that id was most recently remapped to
		auto iLast = i->second.rbegin();
		LogicalPageID newID = iLast->second;
		ASSERT(RemappedPage::getTypeOf(newID) == RemappedPage::REMAP);

		// If the last change remap was also at v then change the remap to a delete, as it's essentially
		// the same as the original page being deleted at that version and newID being used from then on.
		if (iLast->first == v) {
			debug_printf("DWALPager(%s) op=detachDelete originalID=%s newID=%s @%" PRId64 " oldestVersion=%" PRId64
			             "\n",
			             filename.c_str(),
			             toString(pageID).c_str(),
			             toString(newID).c_str(),
			             v,
			             pLastCommittedHeader->oldestVersion);
			iLast->second = invalidLogicalPageID;
			remapQueue.pushBack(RemappedPage{ v, pageID, invalidLogicalPageID });
		} else {
			debug_printf("DWALPager(%s) op=detach originalID=%s newID=%s @%" PRId64 " oldestVersion=%" PRId64 "\n",
			             filename.c_str(),
			             toString(pageID).c_str(),
			             toString(newID).c_str(),
			             v,
			             pLastCommittedHeader->oldestVersion);
			// Mark id as converted to its last remapped location as of v
			i->second[v] = 0;
			remapQueue.pushBack(RemappedPage{ v, pageID, 0 });
		}
		return newID;
	}

	void freePage(LogicalPageID pageID, Version v) override {
		// If pageID has been remapped, then it can't be freed until all existing remaps for that page have been undone,
		// so queue it for later deletion
		auto i = remappedPages.find(pageID);
		if (i != remappedPages.end()) {
			debug_printf("DWALPager(%s) op=freeRemapped %s @%" PRId64 " oldestVersion=%" PRId64 "\n",
			             filename.c_str(),
			             toString(pageID).c_str(),
			             v,
			             pLastCommittedHeader->oldestVersion);
			remapQueue.pushBack(RemappedPage{ v, pageID, invalidLogicalPageID });
			i->second[v] = invalidLogicalPageID;
			return;
		}

		freeUnmappedPage(pageID, v);
	};

	// Read a physical page from the page file.  Note that header pages use a page size of smallestPhysicalBlock
	// If the user chosen physical page size is larger, then there will be a gap of unused space after the header pages
	// and before the user-chosen sized pages.
	ACTOR static Future<Reference<ArenaPage>> readPhysicalPage(DWALPager* self,
	                                                           PhysicalPageID pageID,
	                                                           bool header = false) {
		ASSERT(!self->memoryOnly);
		++g_redwoodMetrics.pagerDiskRead;

		if (g_network->getCurrentTask() > TaskPriority::DiskRead) {
			wait(delay(0, TaskPriority::DiskRead));
		}

		state Reference<ArenaPage> page =
		    header ? Reference<ArenaPage>(new ArenaPage(smallestPhysicalBlock, smallestPhysicalBlock))
		           : self->newPageBuffer();
		debug_printf("DWALPager(%s) op=readPhysicalStart %s ptr=%p\n",
		             self->filename.c_str(),
		             toString(pageID).c_str(),
		             page->begin());

		int blockSize = header ? smallestPhysicalBlock : self->physicalPageSize;
		// TODO:  Could a dispatched read try to write to page after it has been destroyed if this actor is cancelled?
		int readBytes = wait(self->pageFile->read(page->mutate(), blockSize, (int64_t)pageID * blockSize));
		debug_printf("DWALPager(%s) op=readPhysicalComplete %s ptr=%p bytes=%d\n",
		             self->filename.c_str(),
		             toString(pageID).c_str(),
		             page->begin(),
		             readBytes);

		// Header reads are checked explicitly during recovery
		if (!header) {
			if (!page->verifyChecksum(pageID)) {
				debug_printf(
				    "DWALPager(%s) checksum failed for %s\n", self->filename.c_str(), toString(pageID).c_str());
				Error e = checksum_failed();
				TraceEvent(SevError, "DWALPagerChecksumFailed")
				    .detail("Filename", self->filename.c_str())
				    .detail("PageID", pageID)
				    .detail("PageSize", self->physicalPageSize)
				    .detail("Offset", pageID * self->physicalPageSize)
				    .detail("CalculatedChecksum", page->calculateChecksum(pageID))
				    .detail("ChecksumInPage", page->getChecksum())
				    .error(e);
				ASSERT(false);
				throw e;
			}
		}
		return page;
	}

	static Future<Reference<ArenaPage>> readHeaderPage(DWALPager* self, PhysicalPageID pageID) {
		return readPhysicalPage(self, pageID, true);
	}

	bool tryEvictPage(LogicalPageID logicalID, Version v) {
		PhysicalPageID physicalID = getPhysicalPageID(logicalID, v);
		return pageCache.tryEvict(physicalID);
	}

	// Reads the most recent version of pageID, either previously committed or written using updatePage()
	// in the current commit
	// If cacheable is false then if fromCache is valid it will be set to true if the page is from cache, otherwise
	// false. If cacheable is true, fromCache is ignored as the result is automatically from cache by virtue of being
	// cacheable.
	Future<Reference<ArenaPage>> readPage(LogicalPageID pageID,
	                                      bool cacheable,
	                                      bool noHit = false,
	                                      bool* fromCache = nullptr) override {
		// Use cached page if present, without triggering a cache hit.
		// Otherwise, read the page and return it but don't add it to the cache
		if (!cacheable) {
			debug_printf("DWALPager(%s) op=readUncached %s\n", filename.c_str(), toString(pageID).c_str());
			PageCacheEntry* pCacheEntry = pageCache.getIfExists(pageID);
			if (fromCache != nullptr) {
				*fromCache = pCacheEntry != nullptr;
			}

			if (pCacheEntry != nullptr) {
				debug_printf("DWALPager(%s) op=readUncachedHit %s\n", filename.c_str(), toString(pageID).c_str());
				return pCacheEntry->readFuture;
			}

			debug_printf("DWALPager(%s) op=readUncachedMiss %s\n", filename.c_str(), toString(pageID).c_str());
			return forwardError(readPhysicalPage(this, (PhysicalPageID)pageID), errorPromise);
		}

		PageCacheEntry& cacheEntry = pageCache.get(pageID, noHit);
		debug_printf("DWALPager(%s) op=read %s cached=%d reading=%d writing=%d noHit=%d\n",
		             filename.c_str(),
		             toString(pageID).c_str(),
		             cacheEntry.initialized(),
		             cacheEntry.initialized() && cacheEntry.reading(),
		             cacheEntry.initialized() && cacheEntry.writing(),
		             noHit);

		if (!cacheEntry.initialized()) {
			debug_printf("DWALPager(%s) issuing actual read of %s\n", filename.c_str(), toString(pageID).c_str());
			cacheEntry.readFuture = forwardError(readPhysicalPage(this, (PhysicalPageID)pageID), errorPromise);
			cacheEntry.writeFuture = Void();
		}

		return cacheEntry.readFuture;
	}

	PhysicalPageID getPhysicalPageID(LogicalPageID pageID, Version v) {
		auto i = remappedPages.find(pageID);

		if (i != remappedPages.end()) {
			auto j = i->second.upper_bound(v);
			if (j != i->second.begin()) {
				--j;
				debug_printf("DWALPager(%s) op=lookupRemapped %s @%" PRId64 " -> %s\n",
				             filename.c_str(),
				             toString(pageID).c_str(),
				             v,
				             toString(j->second).c_str());
				pageID = j->second;
				ASSERT(pageID != invalidLogicalPageID);
			}
		} else {
			debug_printf("DWALPager(%s) op=lookupNotRemapped %s @%" PRId64 " (not remapped)\n",
			             filename.c_str(),
			             toString(pageID).c_str(),
			             v);
		}

		return (PhysicalPageID)pageID;
	}

	Future<Reference<ArenaPage>> readPageAtVersion(LogicalPageID logicalID,
	                                               Version v,
	                                               bool cacheable,
	                                               bool noHit,
	                                               bool* fromCache) {
		PhysicalPageID physicalID = getPhysicalPageID(logicalID, v);
		return readPage(physicalID, cacheable, noHit, fromCache);
	}

	// Get snapshot as of the most recent committed version of the pager
	Reference<IPagerSnapshot> getReadSnapshot(Version v) override;
	void addLatestSnapshot();

	// Set the pending oldest versiont to keep as of the next commit
	void setOldestVersion(Version v) override {
		ASSERT(v >= pHeader->oldestVersion);
		ASSERT(v <= pHeader->committedVersion);
		pHeader->oldestVersion = v;
		expireSnapshots(v);
	};

	// Get the oldest *readable* version, which is not the same as the oldest retained version as the version
	// returned could have been set as the oldest version in the pending commit
	Version getOldestVersion() const override { return pHeader->oldestVersion; };

	// Calculate the *effective* oldest version, which can be older than the one set in the last commit since we
	// are allowing active snapshots to temporarily delay page reuse.
	Version effectiveOldestVersion() {
		return std::min(pLastCommittedHeader->oldestVersion, snapshots.front().version);
	}

	ACTOR static Future<Void> removeRemapEntry(DWALPager* self, RemappedPage p, Version oldestRetainedVersion) {
		// Get iterator to the versioned page map entry for the original page
		state PageToVersionedMapT::iterator iPageMapPair = self->remappedPages.find(p.originalPageID);
		// The iterator must be valid and not empty and its first page map entry must match p's version
		ASSERT(iPageMapPair != self->remappedPages.end());
		ASSERT(!iPageMapPair->second.empty());
		state VersionToPageMapT::iterator iVersionPagePair = iPageMapPair->second.find(p.version);
		ASSERT(iVersionPagePair != iPageMapPair->second.end());

		RemappedPage::Type firstType = p.getType();
		state RemappedPage::Type secondType;
		bool secondAfterOldestRetainedVersion = false;
		state bool deleteAtSameVersion = false;
		if (p.newPageID == iVersionPagePair->second) {
			auto nextEntry = iVersionPagePair;
			++nextEntry;
			if (nextEntry == iPageMapPair->second.end()) {
				secondType = RemappedPage::NONE;
			} else {
				secondType = RemappedPage::getTypeOf(nextEntry->second);
				secondAfterOldestRetainedVersion = nextEntry->first > oldestRetainedVersion;
			}
		} else {
			ASSERT(iVersionPagePair->second == invalidLogicalPageID);
			secondType = RemappedPage::FREE;
			deleteAtSameVersion = true;
		}
		ASSERT(firstType == RemappedPage::REMAP || secondType == RemappedPage::NONE);

		// Scenarios and actions to take:
		//
		// The first letter (firstType) is the type of the entry just popped from the remap queue.
		// The second letter (secondType) is the type of the next item in the queue for the same
		// original page ID, if present.  If not present, secondType will be NONE.
		//
		// Since the next item can be arbitrarily ahead in the queue, secondType is determined by
		// looking at the remappedPages structure.
		//
		// R == Remap    F == Free   D == Detach   | == oldestRetaineedVersion
		//
		//   R R |  free new ID
		//   R F |  free new ID if R and D are at different versions
		//   R D |  do nothing
		//   R | R  copy new to original ID, free new ID
		//   R | F  copy new to original ID, free new ID
		//   R | D  copy new to original ID
		//   R |    copy new to original ID, free new ID
		//   F |    free original ID
		//   D |    free original ID
		//
		// Note that
		//
		// Special case:  Page is detached while it is being read in remapCopyAndFree()
		//   Initial state:  R |
		//   Start remapCopyAndFree(), intending to copy new, ID to originalID and free newID
		//   New state:  R | D
		//   Read of newID completes.
		//   Copy new contents over original, do NOT free new ID
		//   Later popped state:  D |
		//   free original ID
		//
		state bool freeNewID =
		    (firstType == RemappedPage::REMAP && secondType != RemappedPage::DETACH && !deleteAtSameVersion);
		state bool copyNewToOriginal = (firstType == RemappedPage::REMAP &&
		                                (secondAfterOldestRetainedVersion || secondType == RemappedPage::NONE));
		state bool freeOriginalID = (firstType == RemappedPage::FREE || firstType == RemappedPage::DETACH);

		debug_printf("DWALPager(%s) remapCleanup %s secondType=%c mapEntry=%s oldestRetainedVersion=%" PRId64 " \n",
		             self->filename.c_str(),
		             p.toString().c_str(),
		             secondType,
		             ::toString(*iVersionPagePair).c_str(),
		             oldestRetainedVersion);

		if (copyNewToOriginal) {
			if (g_network->isSimulated()) {
				ASSERT(self->remapDestinationsSimOnly.count(p.originalPageID) == 0);
				self->remapDestinationsSimOnly.insert(p.originalPageID);
			}
			debug_printf("DWALPager(%s) remapCleanup copy %s\n", self->filename.c_str(), p.toString().c_str());

			// Read the data from the page that the original was mapped to
			Reference<ArenaPage> data = wait(self->readPage(p.newPageID, false, true));

			// Write the data to the original page so it can be read using its original pageID
			self->updatePage(p.originalPageID, data);
			++g_redwoodMetrics.pagerRemapCopy;
		} else if (firstType == RemappedPage::REMAP) {
			++g_redwoodMetrics.pagerRemapSkip;
		}

		// Now that the page contents have been copied to the original page, if the corresponding map entry
		// represented the remap and there wasn't a delete later in the queue at p for the same version then
		// erase the entry.
		if (!deleteAtSameVersion) {
			debug_printf(
			    "DWALPager(%s) remapCleanup deleting map entry %s\n", self->filename.c_str(), p.toString().c_str());
			// Erase the entry and set iVersionPagePair to the next entry or end
			iVersionPagePair = iPageMapPair->second.erase(iVersionPagePair);

			// If the map is now empty, delete it
			if (iPageMapPair->second.empty()) {
				debug_printf(
				    "DWALPager(%s) remapCleanup deleting empty map %s\n", self->filename.c_str(), p.toString().c_str());
				self->remappedPages.erase(iPageMapPair);
			} else if (freeNewID && secondType == RemappedPage::NONE &&
			           iVersionPagePair != iPageMapPair->second.end() &&
			           RemappedPage::getTypeOf(iVersionPagePair->second) == RemappedPage::DETACH) {
				// If we intend to free the new ID and there was no map entry, one could have been added during the wait
				// above. If so, and if it was a detach operation, then we can't free the new page ID as its lifetime
				// will be managed by the client starting at some later version.
				freeNewID = false;
			}
		}

		if (freeNewID) {
			debug_printf("DWALPager(%s) remapCleanup freeNew %s\n", self->filename.c_str(), p.toString().c_str());
			self->freeUnmappedPage(p.newPageID, 0);
			++g_redwoodMetrics.pagerRemapFree;
		}

		if (freeOriginalID) {
			debug_printf("DWALPager(%s) remapCleanup freeOriginal %s\n", self->filename.c_str(), p.toString().c_str());
			self->freeUnmappedPage(p.originalPageID, 0);
			++g_redwoodMetrics.pagerRemapFree;
		}

		return Void();
	}

	ACTOR static Future<Void> remapCleanup(DWALPager* self) {
		state ActorCollection tasks(true);
		state Promise<Void> signal;
		tasks.add(signal.getFuture());

		self->remapCleanupStop = false;

		// The oldest retained version cannot change during the cleanup run as this would allow multiple read/copy
		// operations with the same original page ID destination to be started and they could complete out of order.
		state Version oldestRetainedVersion = self->effectiveOldestVersion();

		// Cutoff is the version we can pop to
		state RemappedPage cutoff(oldestRetainedVersion - self->remapCleanupWindow);

		// Minimum version we must pop to before obeying stop command.
		state Version minStopVersion =
		    cutoff.version - (BUGGIFY ? deterministicRandom()->randomInt(0, 10)
		                              : (self->remapCleanupWindow * SERVER_KNOBS->REDWOOD_REMAP_CLEANUP_LAG));
		self->remapDestinationsSimOnly.clear();

		state int sinceYield = 0;
		loop {
			state Optional<RemappedPage> p = wait(self->remapQueue.pop(cutoff));
			debug_printf("DWALPager(%s) remapCleanup popped %s\n", self->filename.c_str(), ::toString(p).c_str());

			// Stop if we have reached the cutoff version, which is the start of the cleanup coalescing window
			if (!p.present()) {
				break;
			}

			Future<Void> task = removeRemapEntry(self, p.get(), oldestRetainedVersion);
			if (!task.isReady()) {
				tasks.add(task);
			}

			// If the stop flag is set and we've reached the minimum stop version according the the allowed lag then
			// stop.
			if (self->remapCleanupStop && p.get().version >= minStopVersion) {
				break;
			}

			if (++sinceYield >= 100) {
				sinceYield = 0;
				wait(yield());
			}
		}

		debug_printf("DWALPager(%s) remapCleanup stopped (stop=%d)\n", self->filename.c_str(), self->remapCleanupStop);
		signal.send(Void());
		wait(tasks.getResult());
		return Void();
	}

	// Flush all queues so they have no operations pending.
	ACTOR static Future<Void> flushQueues(DWALPager* self) {
		ASSERT(self->remapCleanupFuture.isReady());

		// Flush remap queue separately, it's not involved in free page management
		wait(self->remapQueue.flush());

		// Flush the free list and delayed free list queues together as they are used by freePage() and newPageID()
		// Since each queue's preFlush can create work for the other, we must see preflush return false for both
		// twice in row.
		state int clear = 0;
		loop {
			state bool freeBusy = wait(self->freeList.preFlush());
			state bool delayedFreeBusy = wait(self->delayedFreeList.preFlush());
			debug_printf("DWALPager(%s) flushQueues freeBusy=%d delayedFreeBusy=%d\n",
			             self->filename.c_str(),
			             freeBusy,
			             delayedFreeBusy);

			// Once preFlush() returns false for both queues then there are no more operations pending
			// on either queue.  If preFlush() returns true for either queue in one loop execution then
			// it could have generated new work for itself or the other queue.
			if (!freeBusy && !delayedFreeBusy) {
				if (++clear == 2) {
					break;
				}
			} else {
				clear = 0;
			}
		}
		self->freeList.finishFlush();
		self->delayedFreeList.finishFlush();

		return Void();
	}

	ACTOR static Future<Void> commit_impl(DWALPager* self) {
		debug_printf("DWALPager(%s) commit begin\n", self->filename.c_str());

		// Write old committed header to Page 1
		self->writeHeaderPage(1, self->lastCommittedHeaderPage);

		// Trigger the remap eraser to stop and then wait for it.
		self->remapCleanupStop = true;
		wait(self->remapCleanupFuture);

		wait(flushQueues(self));

		self->pHeader->remapQueue = self->remapQueue.getState();
		self->pHeader->freeList = self->freeList.getState();
		self->pHeader->delayedFreeList = self->delayedFreeList.getState();

		// Wait for all outstanding writes to complete
		debug_printf("DWALPager(%s) waiting for outstanding writes\n", self->filename.c_str());
		wait(self->operations.signalAndCollapse());
		debug_printf("DWALPager(%s) Syncing\n", self->filename.c_str());

		// Sync everything except the header
		if (g_network->getCurrentTask() > TaskPriority::DiskWrite) {
			wait(delay(0, TaskPriority::DiskWrite));
		}

		if (!self->memoryOnly) {
			wait(self->pageFile->sync());
			debug_printf("DWALPager(%s) commit version %" PRId64 " sync 1\n",
			             self->filename.c_str(),
			             self->pHeader->committedVersion);
		}

		// Update header on disk and sync again.
		wait(self->writeHeaderPage(0, self->headerPage));
		if (g_network->getCurrentTask() > TaskPriority::DiskWrite) {
			wait(delay(0, TaskPriority::DiskWrite));
		}

		if (!self->memoryOnly) {
			wait(self->pageFile->sync());
			debug_printf("DWALPager(%s) commit version %" PRId64 " sync 2\n",
			             self->filename.c_str(),
			             self->pHeader->committedVersion);
		}

		// Update the last committed header for use in the next commit.
		self->updateCommittedHeader();
		self->addLatestSnapshot();

		// Try to expire snapshots up to the oldest version, in case some were being kept around due to being in use,
		// because maybe some are no longer in use.
		self->expireSnapshots(self->pHeader->oldestVersion);

		// Start unmapping pages for expired versions
		self->remapCleanupFuture = remapCleanup(self);

		return Void();
	}

	Future<Void> commit() override {
		// Can't have more than one commit outstanding.
		ASSERT(commitFuture.isReady());
		commitFuture = forwardError(commit_impl(this), errorPromise);
		return commitFuture;
	}

	Key getMetaKey() const override { return pHeader->getMetaKey(); }

	void setCommitVersion(Version v) override { pHeader->committedVersion = v; }

	void setMetaKey(KeyRef metaKey) override { pHeader->setMetaKey(metaKey); }

	ACTOR void shutdown(DWALPager* self, bool dispose) {
		debug_printf("DWALPager(%s) shutdown cancel recovery\n", self->filename.c_str());
		self->recoverFuture.cancel();
		debug_printf("DWALPager(%s) shutdown cancel commit\n", self->filename.c_str());
		self->commitFuture.cancel();
		debug_printf("DWALPager(%s) shutdown cancel remap\n", self->filename.c_str());
		self->remapCleanupFuture.cancel();

		if (self->errorPromise.canBeSet()) {
			debug_printf("DWALPager(%s) shutdown sending error\n", self->filename.c_str());
			self->errorPromise.sendError(actor_cancelled()); // Ideally this should be shutdown_in_progress
		}

		// Must wait for pending operations to complete, canceling them can cause a crash because the underlying
		// operations may be uncancellable and depend on memory from calling scope's page reference
		debug_printf("DWALPager(%s) shutdown wait for operations\n", self->filename.c_str());
		wait(self->operations.signal());

		debug_printf("DWALPager(%s) shutdown destroy page cache\n", self->filename.c_str());
		wait(self->pageCache.clear());

		// Unreference the file and clear
		self->pageFile.clear();
		if (dispose) {
			if (!self->memoryOnly) {
				debug_printf("DWALPager(%s) shutdown deleting file\n", self->filename.c_str());
				wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(self->filename, true));
			}
		}

		self->closedPromise.send(Void());
		delete self;
	}

	void dispose() override { shutdown(this, true); }

	void close() override { shutdown(this, false); }

	Future<Void> getError() override { return errorPromise.getFuture(); }

	Future<Void> onClosed() override { return closedPromise.getFuture(); }

	StorageBytes getStorageBytes() const override {
		ASSERT(recoverFuture.isReady());
		int64_t free;
		int64_t total;
		if (memoryOnly) {
			total = pageCacheBytes;
			free = pageCacheBytes - ((int64_t)pageCache.count() * physicalPageSize);
		} else {
			g_network->getDiskBytes(parentDirectory(filename), free, total);
		}
		int64_t pagerSize = pHeader->pageCount * physicalPageSize;

		// It is not exactly known how many pages on the delayed free list are usable as of right now.  It could be
		// known, if each commit delayed entries that were freeable were shuffled from the delayed free queue to the
		// free queue, but this doesn't seem necessary.
		int64_t reusable = (freeList.numEntries + delayedFreeList.numEntries) * physicalPageSize;
		int64_t temp = remapQueue.numEntries * physicalPageSize;

		return StorageBytes(free, total, pagerSize - reusable, free + reusable, temp);
	}

	ACTOR static Future<Void> getUserPageCount_cleanup(DWALPager* self) {
		// Wait for the remap eraser to finish all of its work (not triggering stop)
		wait(self->remapCleanupFuture);

		// Flush queues so there are no pending freelist operations
		wait(flushQueues(self));

		return Void();
	}

	// Get the number of pages in use by the pager's user
	Future<int64_t> getUserPageCount() override {
		return map(getUserPageCount_cleanup(this), [=](Void) {
			int64_t userPages = pHeader->pageCount - 2 - freeList.numPages - freeList.numEntries -
			                    delayedFreeList.numPages - delayedFreeList.numEntries - remapQueue.numPages;

			debug_printf("DWALPager(%s) userPages=%" PRId64 " totalPageCount=%" PRId64 " freeQueuePages=%" PRId64
			             " freeQueueCount=%" PRId64 " delayedFreeQueuePages=%" PRId64 " delayedFreeQueueCount=%" PRId64
			             " remapQueuePages=%" PRId64 " remapQueueCount=%" PRId64 "\n",
			             filename.c_str(),
			             userPages,
			             pHeader->pageCount,
			             freeList.numPages,
			             freeList.numEntries,
			             delayedFreeList.numPages,
			             delayedFreeList.numEntries,
			             remapQueue.numPages,
			             remapQueue.numEntries);
			return userPages;
		});
	}

	Future<Void> init() override { return recoverFuture; }

	Version getLatestVersion() const override { return pLastCommittedHeader->committedVersion; }

private:
	~DWALPager() {}

	// Try to expire snapshots up to but not including v, but do not expire any snapshots that are in use.
	void expireSnapshots(Version v);

#pragma pack(push, 1)
	// Header is the format of page 0 of the database
	struct Header {
		static constexpr int FORMAT_VERSION = 2;
		uint16_t formatVersion;
		uint32_t pageSize;
		int64_t pageCount;
		FIFOQueue<LogicalPageID>::QueueState freeList;
		FIFOQueue<DelayedFreePage>::QueueState delayedFreeList;
		FIFOQueue<RemappedPage>::QueueState remapQueue;
		Version committedVersion;
		Version oldestVersion;
		int32_t metaKeySize;

		KeyRef getMetaKey() const { return KeyRef((const uint8_t*)(this + 1), metaKeySize); }

		void setMetaKey(StringRef key) {
			ASSERT(key.size() < (smallestPhysicalBlock - sizeof(Header)));
			metaKeySize = key.size();
			if (key.size() > 0) {
				memcpy(this + 1, key.begin(), key.size());
			}
		}

		int size() const { return sizeof(Header) + metaKeySize; }

	private:
		Header();
	};
#pragma pack(pop)

	struct PageCacheEntry {
		Future<Reference<ArenaPage>> readFuture;
		Future<Void> writeFuture;

		bool initialized() const { return readFuture.isValid(); }

		bool reading() const { return !readFuture.isReady(); }

		bool writing() const { return !writeFuture.isReady(); }

		bool evictable() const {
			// Don't evict if a page is still being read or written
			return !reading() && !writing();
		}

		Future<Void> onEvictable() const { return ready(readFuture) && writeFuture; }
	};

	// Physical page sizes will always be a multiple of 4k because AsyncFileNonDurable requires
	// this in simulation, and it also makes sense for current SSDs.
	// Allowing a smaller 'logical' page size is very useful for testing.
	static constexpr int smallestPhysicalBlock = 4096;
	int physicalPageSize;
	int logicalPageSize; // In simulation testing it can be useful to use a small logical page size

	int64_t pageCacheBytes;

	// The header will be written to / read from disk as a smallestPhysicalBlock sized chunk.
	Reference<ArenaPage> headerPage;
	Header* pHeader;

	int desiredPageSize;

	Reference<ArenaPage> lastCommittedHeaderPage;
	Header* pLastCommittedHeader;

	std::string filename;
	bool memoryOnly;

	typedef ObjectCache<LogicalPageID, PageCacheEntry> PageCacheT;
	PageCacheT pageCache;

	Promise<Void> closedPromise;
	Promise<Void> errorPromise;
	Future<Void> commitFuture;
	SignalableActorCollection operations;
	Future<Void> recoverFuture;
	Future<Void> remapCleanupFuture;
	bool remapCleanupStop;

	Reference<IAsyncFile> pageFile;

	LogicalPageQueueT freeList;

	// The delayed free list will be approximately in Version order.
	// TODO: Make this an ordered container some day.
	DelayedFreePageQueueT delayedFreeList;

	RemapQueueT remapQueue;
	Version remapCleanupWindow;
	std::unordered_set<PhysicalPageID> remapDestinationsSimOnly;

	struct SnapshotEntry {
		Version version;
		Promise<Void> expired;
		Reference<DWALPagerSnapshot> snapshot;
	};

	struct SnapshotEntryLessThanVersion {
		bool operator()(Version v, const SnapshotEntry& snapshot) { return v < snapshot.version; }

		bool operator()(const SnapshotEntry& snapshot, Version v) { return snapshot.version < v; }
	};

	// TODO: Better data structure
	PageToVersionedMapT remappedPages;

	std::deque<SnapshotEntry> snapshots;
};

// Prevents pager from reusing freed pages from version until the snapshot is destroyed
class DWALPagerSnapshot : public IPagerSnapshot, public ReferenceCounted<DWALPagerSnapshot> {
public:
	DWALPagerSnapshot(DWALPager* pager, Key meta, Version version, Future<Void> expiredFuture)
	  : pager(pager), metaKey(meta), version(version), expired(expiredFuture) {}
	~DWALPagerSnapshot() override {}

	Future<Reference<const ArenaPage>> getPhysicalPage(LogicalPageID pageID,
	                                                   bool cacheable,
	                                                   bool noHit,
	                                                   bool* fromCache) override {
		if (expired.isError()) {
			throw expired.getError();
		}
		return map(pager->readPageAtVersion(pageID, version, cacheable, noHit, fromCache),
		           [=](Reference<ArenaPage> p) { return Reference<const ArenaPage>(std::move(p)); });
	}

	bool tryEvictPage(LogicalPageID id) override { return pager->tryEvictPage(id, version); }

	Key getMetaKey() const override { return metaKey; }

	Version getVersion() const override { return version; }

	void addref() override { ReferenceCounted<DWALPagerSnapshot>::addref(); }

	void delref() override { ReferenceCounted<DWALPagerSnapshot>::delref(); }

	DWALPager* pager;
	Future<Void> expired;
	Version version;
	Key metaKey;
};

void DWALPager::expireSnapshots(Version v) {
	debug_printf("DWALPager(%s) expiring snapshots through %" PRId64 " snapshot count %d\n",
	             filename.c_str(),
	             v,
	             (int)snapshots.size());
	while (snapshots.size() > 1 && snapshots.front().version < v && snapshots.front().snapshot->isSoleOwner()) {
		debug_printf("DWALPager(%s) expiring snapshot for %" PRId64 " soleOwner=%d\n",
		             filename.c_str(),
		             snapshots.front().version,
		             snapshots.front().snapshot->isSoleOwner());
		// The snapshot contract could be made such that the expired promise isn't need anymore.  In practice it
		// probably is already not needed but it will gracefully handle the case where a user begins a page read
		// with a snapshot reference, keeps the page read future, and drops the snapshot reference.
		snapshots.front().expired.sendError(transaction_too_old());
		snapshots.pop_front();
	}
}

Reference<IPagerSnapshot> DWALPager::getReadSnapshot(Version v) {
	ASSERT(!snapshots.empty());

	auto i = std::upper_bound(snapshots.begin(), snapshots.end(), v, SnapshotEntryLessThanVersion());
	if (i == snapshots.begin()) {
		throw version_invalid();
	}
	--i;
	return i->snapshot;
}

void DWALPager::addLatestSnapshot() {
	Promise<Void> expired;
	snapshots.push_back(
	    { pLastCommittedHeader->committedVersion,
	      expired,
	      makeReference<DWALPagerSnapshot>(
	          this, pLastCommittedHeader->getMetaKey(), pLastCommittedHeader->committedVersion, expired.getFuture()) });
}

// TODO: Move this to a flow header once it is mature.
struct SplitStringRef {
	StringRef a;
	StringRef b;

	SplitStringRef(StringRef a = StringRef(), StringRef b = StringRef()) : a(a), b(b) {}

	SplitStringRef(Arena& arena, const SplitStringRef& toCopy) : a(toStringRef(arena)), b() {}

	SplitStringRef prefix(int len) const {
		if (len <= a.size()) {
			return SplitStringRef(a.substr(0, len));
		}
		len -= a.size();
		return SplitStringRef(a, b.substr(0, len));
	}

	StringRef toStringRef(Arena& arena) const {
		StringRef c = makeString(size(), arena);
		memcpy(mutateString(c), a.begin(), a.size());
		memcpy(mutateString(c) + a.size(), b.begin(), b.size());
		return c;
	}

	Standalone<StringRef> toStringRef() const {
		Arena a;
		return Standalone<StringRef>(toStringRef(a), a);
	}

	int size() const { return a.size() + b.size(); }

	int expectedSize() const { return size(); }

	std::string toString() const { return format("%s%s", a.toString().c_str(), b.toString().c_str()); }

	std::string toHexString() const { return format("%s%s", a.toHexString().c_str(), b.toHexString().c_str()); }

	struct const_iterator {
		const uint8_t* ptr;
		const uint8_t* end;
		const uint8_t* next;

		inline bool operator==(const const_iterator& rhs) const { return ptr == rhs.ptr; }
		inline bool operator!=(const const_iterator& rhs) const { return !(*this == rhs); }

		inline const_iterator& operator++() {
			++ptr;
			if (ptr == end) {
				ptr = next;
			}
			return *this;
		}

		inline const_iterator& operator+(int n) {
			ptr += n;
			if (ptr >= end) {
				ptr = next + (ptr - end);
			}
			return *this;
		}

		inline uint8_t operator*() const { return *ptr; }
	};

	inline const_iterator begin() const { return { a.begin(), a.end(), b.begin() }; }

	inline const_iterator end() const { return { b.end() }; }

	template <typename StringT>
	int compare(const StringT& rhs) const {
		auto j = begin();
		auto k = rhs.begin();
		auto jEnd = end();
		auto kEnd = rhs.end();

		while (j != jEnd && k != kEnd) {
			int cmp = *j - *k;
			if (cmp != 0) {
				return cmp;
			}
		}

		// If we've reached the end of *this, then values are equal if rhs is also exhausted, otherwise *this is less
		// than rhs
		if (j == jEnd) {
			return k == kEnd ? 0 : -1;
		}

		return 1;
	}
};

// A BTree "page id" is actually a list of LogicalPageID's whose contents should be concatenated together.
// NOTE: Uses host byte order
typedef VectorRef<LogicalPageID> BTreePageIDRef;
constexpr LogicalPageID maxPageID = (LogicalPageID)-1;

std::string toString(BTreePageIDRef id) {
	return std::string("BTreePageID") + toString(id.begin(), id.end());
}

#define STR(x) LiteralStringRef(x)
struct RedwoodRecordRef {
	typedef uint8_t byte;

	RedwoodRecordRef(KeyRef key = KeyRef(), Version ver = 0, Optional<ValueRef> value = {})
	  : key(key), version(ver), value(value) {}

	RedwoodRecordRef(Arena& arena, const RedwoodRecordRef& toCopy) : key(arena, toCopy.key), version(toCopy.version) {
		if (toCopy.value.present()) {
			value = ValueRef(arena, toCopy.value.get());
		}
	}

	KeyValueRef toKeyValueRef() const { return KeyValueRef(key, value.get()); }

	// RedwoodRecordRefs are used for both internal and leaf pages of the BTree.
	// Boundary records in internal pages are made from leaf records.
	// These functions make creating and working with internal page records more convenient.
	inline BTreePageIDRef getChildPage() const {
		ASSERT(value.present());
		return BTreePageIDRef((LogicalPageID*)value.get().begin(), value.get().size() / sizeof(LogicalPageID));
	}

	inline void setChildPage(BTreePageIDRef id) {
		value = ValueRef((const uint8_t*)id.begin(), id.size() * sizeof(LogicalPageID));
	}

	inline void setChildPage(Arena& arena, BTreePageIDRef id) {
		value = ValueRef(arena, (const uint8_t*)id.begin(), id.size() * sizeof(LogicalPageID));
	}

	inline RedwoodRecordRef withPageID(BTreePageIDRef id) const {
		return RedwoodRecordRef(key, version, ValueRef((const uint8_t*)id.begin(), id.size() * sizeof(LogicalPageID)));
	}

	inline RedwoodRecordRef withoutValue() const { return RedwoodRecordRef(key, version); }

	inline RedwoodRecordRef withMaxPageID() const {
		return RedwoodRecordRef(key, version, StringRef((uint8_t*)&maxPageID, sizeof(maxPageID)));
	}

	// Truncate (key, version, part) tuple to len bytes.
	void truncate(int len) {
		ASSERT(len <= key.size());
		key = key.substr(0, len);
		version = 0;
	}

	// Find the common key prefix between two records, assuming that the first skipLen bytes are the same
	inline int getCommonPrefixLen(const RedwoodRecordRef& other, int skipLen = 0) const {
		return skipLen + commonPrefixLength(key, other.key, skipLen);
	}

	// Compares and orders by key, version, chunk.total, chunk.start, value
	// This is the same order that delta compression uses for prefix borrowing
	int compare(const RedwoodRecordRef& rhs, int skip = 0) const {
		int keySkip = std::min(skip, key.size());
		int cmp = key.compareSuffix(rhs.key, keySkip);

		if (cmp == 0) {
			cmp = version - rhs.version;
			if (cmp == 0) {
				cmp = value.compare(rhs.value);
			}
		}
		return cmp;
	}

	bool sameUserKey(const StringRef& k, int skipLen) const {
		// Keys are the same if the sizes are the same and either the skipLen is longer or the non-skipped suffixes are
		// the same.
		return (key.size() == k.size()) && (key.substr(skipLen) == k.substr(skipLen));
	}

	bool sameExceptValue(const RedwoodRecordRef& rhs, int skipLen = 0) const {
		return sameUserKey(rhs.key, skipLen) && version == rhs.version;
	}

	// TODO: Use SplitStringRef (unless it ends up being slower)
	KeyRef key;
	Optional<ValueRef> value;
	Version version;

	int expectedSize() const { return key.expectedSize() + value.expectedSize(); }
	int kvBytes() const { return expectedSize(); }

	class Reader {
	public:
		Reader(const void* ptr) : rptr((const byte*)ptr) {}

		const byte* rptr;

		StringRef readString(int len) {
			StringRef s(rptr, len);
			rptr += len;
			return s;
		}
	};

#pragma pack(push, 1)
	struct Delta {

		uint8_t flags;

		// Four field sizing schemes ranging from 3 to 8 bytes, with 3 being the most common.
		union {
			struct {
				uint8_t prefixLength;
				uint8_t suffixLength;
				uint8_t valueLength;
			} LengthFormat0;

			struct {
				uint8_t prefixLength;
				uint8_t suffixLength;
				uint16_t valueLength;
			} LengthFormat1;

			struct {
				uint8_t prefixLength;
				uint8_t suffixLength;
				uint32_t valueLength;
			} LengthFormat2;

			struct {
				uint16_t prefixLength;
				uint16_t suffixLength;
				uint32_t valueLength;
			} LengthFormat3;
		};

		struct int48_t {
			static constexpr int64_t MASK = 0xFFFFFFFFFFFFLL;
			int32_t high;
			int16_t low;
		};

		static constexpr int LengthFormatSizes[] = { sizeof(LengthFormat0),
			                                         sizeof(LengthFormat1),
			                                         sizeof(LengthFormat2),
			                                         sizeof(LengthFormat3) };
		static constexpr int VersionDeltaSizes[] = { 0, sizeof(int32_t), sizeof(int48_t), sizeof(int64_t) };

		// Serialized Format
		//
		// Flags - 1 byte
		//    1 bit - borrow source is prev ancestor (otherwise next ancestor)
		//    1 bit - item is deleted
		//    1 bit - has value (different from zero-length value, if 0 value len will be 0)
		//    1 bits - has nonzero version
		//    2 bits - version delta integer size code, maps to 0, 4, 6, 8
		//    2 bits - length fields format
		//
		// Length fields using 3 to 8 bytes total depending on length fields format
		//
		// Byte strings
		//    Key suffix bytes
		//    Value bytes
		//    Version delta bytes
		//

		enum EFlags {
			PREFIX_SOURCE_PREV = 0x80,
			IS_DELETED = 0x40,
			HAS_VALUE = 0x20,
			HAS_VERSION = 0x10,
			VERSION_DELTA_SIZE = 0xC,
			LENGTHS_FORMAT = 0x03
		};

		static inline int determineLengthFormat(int prefixLength, int suffixLength, int valueLength) {
			// Large prefix or suffix length, which should be rare, is format 3
			if (prefixLength > 0xFF || suffixLength > 0xFF) {
				return 3;
			} else if (valueLength < 0x100) {
				return 0;
			} else if (valueLength < 0x10000) {
				return 1;
			} else {
				return 2;
			}
		}

		// Large prefix or suffix length, which should be rare, is format 3
		byte* data() const {
			switch (flags & LENGTHS_FORMAT) {
			case 0:
				return (byte*)(&LengthFormat0 + 1);
			case 1:
				return (byte*)(&LengthFormat1 + 1);
			case 2:
				return (byte*)(&LengthFormat2 + 1);
			case 3:
			default:
				return (byte*)(&LengthFormat3 + 1);
			}
		}

		int getKeyPrefixLength() const {
			switch (flags & LENGTHS_FORMAT) {
			case 0:
				return LengthFormat0.prefixLength;
			case 1:
				return LengthFormat1.prefixLength;
			case 2:
				return LengthFormat2.prefixLength;
			case 3:
			default:
				return LengthFormat3.prefixLength;
			}
		}

		int getKeySuffixLength() const {
			switch (flags & LENGTHS_FORMAT) {
			case 0:
				return LengthFormat0.suffixLength;
			case 1:
				return LengthFormat1.suffixLength;
			case 2:
				return LengthFormat2.suffixLength;
			case 3:
			default:
				return LengthFormat3.suffixLength;
			}
		}

		int getValueLength() const {
			switch (flags & LENGTHS_FORMAT) {
			case 0:
				return LengthFormat0.valueLength;
			case 1:
				return LengthFormat1.valueLength;
			case 2:
				return LengthFormat2.valueLength;
			case 3:
			default:
				return LengthFormat3.valueLength;
			}
		}

		StringRef getKeySuffix() const { return StringRef(data(), getKeySuffixLength()); }

		StringRef getValue() const { return StringRef(data() + getKeySuffixLength(), getValueLength()); }

		bool hasVersion() const { return flags & HAS_VERSION; }

		int getVersionDeltaSizeBytes() const {
			int code = (flags & VERSION_DELTA_SIZE) >> 2;
			return VersionDeltaSizes[code];
		}

		static int getVersionDeltaSizeBytes(Version d) {
			if (d == 0) {
				return 0;
			} else if (d == (int32_t)d) {
				return sizeof(int32_t);
			} else if (d == (d & int48_t::MASK)) {
				return sizeof(int48_t);
			}
			return sizeof(int64_t);
		}

		int getVersionDelta(const uint8_t* r) const {
			int code = (flags & VERSION_DELTA_SIZE) >> 2;
			switch (code) {
			case 0:
				return 0;
			case 1:
				return *(int32_t*)r;
			case 2:
				return ((int64_t) static_cast<uint32_t>(reinterpret_cast<const int48_t*>(r)->high) << 16) |
				       (((int48_t*)r)->low & 0xFFFF);
			case 3:
			default:
				return *(int64_t*)r;
			}
		}

		// Version delta size should be 0 before calling
		int setVersionDelta(Version d, uint8_t* w) {
			flags |= HAS_VERSION;
			if (d == 0) {
				return 0;
			} else if (d == (int32_t)d) {
				flags |= 1 << 2;
				*(uint32_t*)w = d;
				return sizeof(uint32_t);
			} else if (d == (d & int48_t::MASK)) {
				flags |= 2 << 2;
				((int48_t*)w)->high = d >> 16;
				((int48_t*)w)->low = d;
				return sizeof(int48_t);
			} else {
				flags |= 3 << 2;
				*(int64_t*)w = d;
				return sizeof(int64_t);
			}
		}

		bool hasValue() const { return flags & HAS_VALUE; }

		void setPrefixSource(bool val) {
			if (val) {
				flags |= PREFIX_SOURCE_PREV;
			} else {
				flags &= ~PREFIX_SOURCE_PREV;
			}
		}

		bool getPrefixSource() const { return flags & PREFIX_SOURCE_PREV; }

		void setDeleted(bool val) {
			if (val) {
				flags |= IS_DELETED;
			} else {
				flags &= ~IS_DELETED;
			}
		}

		bool getDeleted() const { return flags & IS_DELETED; }

		RedwoodRecordRef apply(const RedwoodRecordRef& base, Arena& arena) const {
			int keyPrefixLen = getKeyPrefixLength();
			int keySuffixLen = getKeySuffixLength();
			int valueLen = hasValue() ? getValueLength() : 0;

			StringRef k;

			Reader r(data());
			// If there is a key suffix, reconstitute the complete key into a contiguous string
			if (keySuffixLen > 0) {
				StringRef keySuffix = r.readString(keySuffixLen);
				k = makeString(keyPrefixLen + keySuffixLen, arena);
				memcpy(mutateString(k), base.key.begin(), keyPrefixLen);
				memcpy(mutateString(k) + keyPrefixLen, keySuffix.begin(), keySuffixLen);
			} else {
				// Otherwise just reference the base key's memory
				k = base.key.substr(0, keyPrefixLen);
			}

			Optional<ValueRef> value;
			if (hasValue()) {
				value = r.readString(valueLen);
			}

			Version v = 0;
			if (hasVersion()) {
				v = base.version + getVersionDelta(r.rptr);
			}

			return RedwoodRecordRef(k, v, value);
		}

		int size() const {
			int size = 1 + getVersionDeltaSizeBytes();
			switch (flags & LENGTHS_FORMAT) {
			case 0:
				return size + sizeof(LengthFormat0) + LengthFormat0.suffixLength + LengthFormat0.valueLength;
			case 1:
				return size + sizeof(LengthFormat1) + LengthFormat1.suffixLength + LengthFormat1.valueLength;
			case 2:
				return size + sizeof(LengthFormat2) + LengthFormat2.suffixLength + LengthFormat2.valueLength;
			case 3:
			default:
				return size + sizeof(LengthFormat3) + LengthFormat3.suffixLength + LengthFormat3.valueLength;
			}
		}

		std::string toString() const {
			std::string flagString = " ";
			if (flags & PREFIX_SOURCE_PREV) {
				flagString += "PrefixSource|";
			}
			if (flags & IS_DELETED) {
				flagString += "IsDeleted|";
			}
			if (hasValue()) {
				flagString += "HasValue|";
			}
			if (hasVersion()) {
				flagString += "HasVersion|";
			}
			int lengthFormat = flags & LENGTHS_FORMAT;

			Reader r(data());
			int prefixLen = getKeyPrefixLength();
			int keySuffixLen = getKeySuffixLength();
			int valueLen = getValueLength();

			return format("lengthFormat: %d  totalDeltaSize: %d  flags: %s  prefixLen: %d  keySuffixLen: %d  "
			              "versionDeltaSizeBytes: %d  valueLen %d  raw: %s",
			              lengthFormat,
			              size(),
			              flagString.c_str(),
			              prefixLen,
			              keySuffixLen,
			              getVersionDeltaSizeBytes(),
			              valueLen,
			              StringRef((const uint8_t*)this, size()).toHexString().c_str());
		}
	};

	// Using this class as an alternative for Delta enables reading a DeltaTree<RecordRef> while only decoding
	// its values, so the Reader does not require the original prev/next ancestors.
	struct DeltaValueOnly : Delta {
		RedwoodRecordRef apply(const RedwoodRecordRef& base, Arena& arena) const {
			Optional<ValueRef> value;

			if (hasValue()) {
				value = getValue();
			}

			return RedwoodRecordRef(StringRef(), 0, value);
		}
	};
#pragma pack(pop)

	bool operator==(const RedwoodRecordRef& rhs) const { return compare(rhs) == 0; }

	bool operator!=(const RedwoodRecordRef& rhs) const { return compare(rhs) != 0; }

	bool operator<(const RedwoodRecordRef& rhs) const { return compare(rhs) < 0; }

	bool operator>(const RedwoodRecordRef& rhs) const { return compare(rhs) > 0; }

	bool operator<=(const RedwoodRecordRef& rhs) const { return compare(rhs) <= 0; }

	bool operator>=(const RedwoodRecordRef& rhs) const { return compare(rhs) >= 0; }

	// Worst case overhead means to assume that either the prefix length or the suffix length
	// could contain the full key size
	int deltaSize(const RedwoodRecordRef& base, int skipLen, bool worstCaseOverhead) const {
		int prefixLen = getCommonPrefixLen(base, skipLen);
		int keySuffixLen = key.size() - prefixLen;
		int valueLen = value.present() ? value.get().size() : 0;

		int formatType;
		int versionBytes;
		if (worstCaseOverhead) {
			formatType = Delta::determineLengthFormat(key.size(), key.size(), valueLen);
			versionBytes = version == 0 ? 0 : Delta::getVersionDeltaSizeBytes(version << 1);
		} else {
			formatType = Delta::determineLengthFormat(prefixLen, keySuffixLen, valueLen);
			versionBytes = version == 0 ? 0 : Delta::getVersionDeltaSizeBytes(version - base.version);
		}

		return 1 + Delta::LengthFormatSizes[formatType] + keySuffixLen + valueLen + versionBytes;
	}

	// commonPrefix between *this and base can be passed if known
	int writeDelta(Delta& d, const RedwoodRecordRef& base, int keyPrefixLen = -1) const {
		d.flags = value.present() ? Delta::HAS_VALUE : 0;

		if (keyPrefixLen < 0) {
			keyPrefixLen = getCommonPrefixLen(base, 0);
		}

		StringRef keySuffix = key.substr(keyPrefixLen);
		int valueLen = value.present() ? value.get().size() : 0;

		int formatType = Delta::determineLengthFormat(keyPrefixLen, keySuffix.size(), valueLen);
		d.flags |= formatType;

		switch (formatType) {
		case 0:
			d.LengthFormat0.prefixLength = keyPrefixLen;
			d.LengthFormat0.suffixLength = keySuffix.size();
			d.LengthFormat0.valueLength = valueLen;
			break;
		case 1:
			d.LengthFormat1.prefixLength = keyPrefixLen;
			d.LengthFormat1.suffixLength = keySuffix.size();
			d.LengthFormat1.valueLength = valueLen;
			break;
		case 2:
			d.LengthFormat2.prefixLength = keyPrefixLen;
			d.LengthFormat2.suffixLength = keySuffix.size();
			d.LengthFormat2.valueLength = valueLen;
			break;
		case 3:
		default:
			d.LengthFormat3.prefixLength = keyPrefixLen;
			d.LengthFormat3.suffixLength = keySuffix.size();
			d.LengthFormat3.valueLength = valueLen;
			break;
		}

		uint8_t* wptr = d.data();
		// Write key suffix string
		wptr = keySuffix.copyTo(wptr);

		// Write value bytes
		if (value.present()) {
			wptr = value.get().copyTo(wptr);
		}

		if (version != 0) {
			wptr += d.setVersionDelta(version - base.version, wptr);
		}

		return wptr - (uint8_t*)&d;
	}

	static std::string kvformat(StringRef s, int hexLimit = -1) {
		bool hex = false;

		for (auto c : s) {
			if (!isprint(c)) {
				hex = true;
				break;
			}
		}

		return hex ? s.toHexString(hexLimit) : s.toString();
	}

	std::string toString(bool leaf = true) const {
		std::string r;
		r += format("'%s'@%" PRId64 " => ", key.printable().c_str(), version);
		if (value.present()) {
			if (leaf) {
				r += format("'%s'", kvformat(value.get()).c_str());
			} else {
				r += format("[%s]", ::toString(getChildPage()).c_str());
			}
		} else {
			r += "(absent)";
		}
		return r;
	}
};

struct BTreePage {
	typedef DeltaTree<RedwoodRecordRef> BinaryTree;
	typedef DeltaTree<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly> ValueTree;

#pragma pack(push, 1)
	struct {
		uint8_t height;
		uint32_t kvBytes;
	};
#pragma pack(pop)

	int size() const {
		auto& t = tree();
		return (uint8_t*)&t - (uint8_t*)this + t.size();
	}

	bool isLeaf() const { return height == 1; }

	BinaryTree& tree() { return *(BinaryTree*)(this + 1); }

	const BinaryTree& tree() const { return *(const BinaryTree*)(this + 1); }

	const ValueTree& valueTree() const { return *(const ValueTree*)(this + 1); }

	// TODO:  boundaries are for decoding, but upper
	std::string toString(bool write,
	                     BTreePageIDRef id,
	                     Version ver,
	                     const RedwoodRecordRef* lowerBound,
	                     const RedwoodRecordRef* upperBound) const {
		std::string r;
		r += format("BTreePage op=%s %s @%" PRId64
		            " ptr=%p height=%d count=%d kvBytes=%d\n  lowerBound: %s\n  upperBound: %s\n",
		            write ? "write" : "read",
		            ::toString(id).c_str(),
		            ver,
		            this,
		            height,
		            (int)tree().numItems,
		            (int)kvBytes,
		            lowerBound->toString(false).c_str(),
		            upperBound->toString(false).c_str());
		try {
			if (tree().numItems > 0) {
				// This doesn't use the cached reader for the page because it is only for debugging purposes,
				// a cached reader may not exist
				BinaryTree::Mirror reader(&tree(), lowerBound, upperBound);
				BinaryTree::Cursor c = reader.getCursor();

				c.moveFirst();
				ASSERT(c.valid());

				bool anyOutOfRange = false;
				do {
					r += "  ";
					r += c.get().toString(height == 1);

					bool tooLow = c.get().withoutValue() < lowerBound->withoutValue();
					bool tooHigh = c.get().withoutValue() >= upperBound->withoutValue();
					if (tooLow || tooHigh) {
						anyOutOfRange = true;
						if (tooLow) {
							r += " (below decode lower bound)";
						}
						if (tooHigh) {
							r += " (at or above decode upper bound)";
						}
					}
					r += "\n";

				} while (c.moveNext());

				// Out of range entries are actually okay now and the result of subtree deletion followed by
				// incremental insertions of records in the deleted range being added to an adjacent subtree
				// which is logically expanded encompass the deleted range but still is using the original
				// subtree boundaries as DeltaTree boundaries.
				// ASSERT(!anyOutOfRange);
			}
		} catch (Error& e) {
			debug_printf("BTreePage::toString ERROR: %s\n", e.what());
			debug_printf("BTreePage::toString partial result: %s\n", r.c_str());
			throw;
		}

		// All appends to r end in a linefeed, remove the final one.
		r.resize(r.size() - 1);
		return r;
	}
};

static void makeEmptyRoot(Reference<ArenaPage> page) {
	BTreePage* btpage = (BTreePage*)page->begin();
	btpage->height = 1;
	btpage->kvBytes = 0;
	btpage->tree().build(page->size(), nullptr, nullptr, nullptr, nullptr);
}

BTreePage::BinaryTree::Cursor getCursor(const Reference<const ArenaPage>& page) {
	return ((BTreePage::BinaryTree::Mirror*)page->userData)->getCursor();
}

struct BoundaryRefAndPage {
	Standalone<RedwoodRecordRef> lowerBound;
	Reference<ArenaPage> firstPage;
	std::vector<Reference<ArenaPage>> extPages;

	std::string toString() const {
		return format("[%s, %d pages]", lowerBound.toString().c_str(), extPages.size() + (firstPage ? 1 : 0));
	}
};

#define NOT_IMPLEMENTED UNSTOPPABLE_ASSERT(false)

#pragma pack(push, 1)
template <typename T, typename SizeT = int8_t>
struct InPlaceArray {
	SizeT count;

	const T* begin() const { return (T*)(this + 1); }

	T* begin() { return (T*)(this + 1); }

	const T* end() const { return begin() + count; }

	T* end() { return begin() + count; }

	VectorRef<T> get() { return VectorRef<T>(begin(), count); }

	void set(VectorRef<T> v, int availableSpace) {
		ASSERT(sizeof(T) * v.size() <= availableSpace);
		count = v.size();
		memcpy(begin(), v.begin(), sizeof(T) * v.size());
	}

	int extraSize() const { return count * sizeof(T); }
};
#pragma pack(pop)

class VersionedBTree final : public IVersionedStore {
public:
	// The first possible internal record possible in the tree
	static RedwoodRecordRef dbBegin;
	// A record which is greater than the last possible record in the tree
	static RedwoodRecordRef dbEnd;

	struct LazyClearQueueEntry {
		Version version;
		Standalone<BTreePageIDRef> pageID;

		bool operator<(const LazyClearQueueEntry& rhs) const { return version < rhs.version; }

		int readFromBytes(const uint8_t* src) {
			version = *(Version*)src;
			src += sizeof(Version);
			int count = *src++;
			pageID = BTreePageIDRef((LogicalPageID*)src, count);
			return bytesNeeded();
		}

		int bytesNeeded() const { return sizeof(Version) + 1 + (pageID.size() * sizeof(LogicalPageID)); }

		int writeToBytes(uint8_t* dst) const {
			*(Version*)dst = version;
			dst += sizeof(Version);
			*dst++ = pageID.size();
			memcpy(dst, pageID.begin(), pageID.size() * sizeof(LogicalPageID));
			return bytesNeeded();
		}

		std::string toString() const { return format("{%s @%" PRId64 "}", ::toString(pageID).c_str(), version); }
	};

	typedef FIFOQueue<LazyClearQueueEntry> LazyClearQueueT;

	struct ParentInfo {
		ParentInfo() {
			count = 0;
			bits = 0;
		}
		void clear() {
			count = 0;
			bits = 0;
		}

		static uint32_t mask(LogicalPageID id) { return 1 << (id & 31); }

		void pageUpdated(LogicalPageID child) {
			auto m = mask(child);
			if ((bits & m) == 0) {
				bits |= m;
				++count;
			}
		}

		bool maybeUpdated(LogicalPageID child) { return (mask(child) & bits) != 0; }

		uint32_t bits;
		int count;
	};

	typedef std::unordered_map<LogicalPageID, ParentInfo> ParentInfoMapT;

#pragma pack(push, 1)
	struct MetaKey {
		static constexpr int FORMAT_VERSION = 8;
		// This serves as the format version for the entire tree, individual pages will not be versioned
		uint16_t formatVersion;
		uint8_t height;
		LazyClearQueueT::QueueState lazyDeleteQueue;
		InPlaceArray<LogicalPageID> root;

		KeyRef asKeyRef() const { return KeyRef((uint8_t*)this, sizeof(MetaKey) + root.extraSize()); }

		void fromKeyRef(KeyRef k) {
			memcpy(this, k.begin(), k.size());
			ASSERT(formatVersion == FORMAT_VERSION);
		}

		std::string toString() {
			return format("{height=%d  formatVersion=%d  root=%s  lazyDeleteQueue=%s}",
			              (int)height,
			              (int)formatVersion,
			              ::toString(root.get()).c_str(),
			              lazyDeleteQueue.toString().c_str());
		}
	};
#pragma pack(pop)

	// All async opts on the btree are based on pager reads, writes, and commits, so
	// we can mostly forward these next few functions to the pager
	Future<Void> getError() override { return m_pager->getError(); }

	Future<Void> onClosed() override { return m_pager->onClosed(); }

	void close_impl(bool dispose) {
		auto* pager = m_pager;
		delete this;
		if (dispose)
			pager->dispose();
		else
			pager->close();
	}

	void dispose() override { return close_impl(true); }

	void close() override { return close_impl(false); }

	KeyValueStoreType getType() const override { NOT_IMPLEMENTED; }
	bool supportsMutation(int op) const override { NOT_IMPLEMENTED; }
	StorageBytes getStorageBytes() const override { return m_pager->getStorageBytes(); }

	// Writes are provided in an ordered stream.
	// A write is considered part of (a change leading to) the version determined by the previous call to
	// setWriteVersion() A write shall not become durable until the following call to commit() begins, and shall be
	// durable once the following call to commit() returns
	void set(KeyValueRef keyValue) override {
		++g_redwoodMetrics.opSet;
		g_redwoodMetrics.opSetKeyBytes += keyValue.key.size();
		g_redwoodMetrics.opSetValueBytes += keyValue.value.size();
		m_pBuffer->insert(keyValue.key).mutation().setBoundaryValue(m_pBuffer->copyToArena(keyValue.value));
	}

	void clear(KeyRangeRef clearedRange) override {
		// Optimization for single key clears to create just one mutation boundary instead of two
		if (clearedRange.begin.size() == clearedRange.end.size() - 1 &&
		    clearedRange.end[clearedRange.end.size() - 1] == 0 && clearedRange.end.startsWith(clearedRange.begin)) {
			++g_redwoodMetrics.opClear;
			++g_redwoodMetrics.opClearKey;
			m_pBuffer->insert(clearedRange.begin).mutation().clearBoundary();
			return;
		}

		++g_redwoodMetrics.opClear;
		MutationBuffer::iterator iBegin = m_pBuffer->insert(clearedRange.begin);
		MutationBuffer::iterator iEnd = m_pBuffer->insert(clearedRange.end);

		iBegin.mutation().clearAll();
		++iBegin;
		m_pBuffer->erase(iBegin, iEnd);
	}

	void mutate(int op, StringRef param1, StringRef param2) override { NOT_IMPLEMENTED; }

	void setOldestVersion(Version v) override { m_newOldestVersion = v; }

	Version getOldestVersion() const override { return m_pager->getOldestVersion(); }

	Version getLatestVersion() const override {
		if (m_writeVersion != invalidVersion)
			return m_writeVersion;
		return m_pager->getLatestVersion();
	}

	Version getWriteVersion() const { return m_writeVersion; }

	Version getLastCommittedVersion() const { return m_lastCommittedVersion; }

	VersionedBTree(IPager2* pager, std::string name)
	  : m_pager(pager), m_writeVersion(invalidVersion), m_lastCommittedVersion(invalidVersion), m_pBuffer(nullptr),
	    m_commitReadLock(new FlowLock(SERVER_KNOBS->REDWOOD_COMMIT_CONCURRENT_READS)), m_name(name) {

		m_lazyClearActor = 0;
		m_init = init_impl(this);
		m_latestCommit = m_init;
	}

	ACTOR static Future<int> incrementalLazyClear(VersionedBTree* self) {
		ASSERT(self->m_lazyClearActor.isReady());
		self->m_lazyClearStop = false;

		// TODO: Is it contractually okay to always to read at the latest version?
		state Reference<IPagerSnapshot> snapshot = self->m_pager->getReadSnapshot(self->m_pager->getLatestVersion());
		state int freedPages = 0;

		loop {
			state int toPop = SERVER_KNOBS->REDWOOD_LAZY_CLEAR_BATCH_SIZE_PAGES;
			state std::vector<std::pair<LazyClearQueueEntry, Future<Reference<const ArenaPage>>>> entries;
			entries.reserve(toPop);

			// Take up to batchSize pages from front of queue
			while (toPop > 0) {
				Optional<LazyClearQueueEntry> q = wait(self->m_lazyClearQueue.pop());
				debug_printf("LazyClear: popped %s\n", toString(q).c_str());
				if (!q.present()) {
					break;
				}
				// Start reading the page, without caching
				entries.push_back(
				    std::make_pair(q.get(), self->readPage(snapshot, q.get().pageID, nullptr, nullptr, true, false)));

				--toPop;
			}

			state int i;
			for (i = 0; i < entries.size(); ++i) {
				Reference<const ArenaPage> p = wait(entries[i].second);
				const LazyClearQueueEntry& entry = entries[i].first;
				const BTreePage& btPage = *(BTreePage*)p->begin();
				auto& metrics = g_redwoodMetrics.level(btPage.height);

				debug_printf("LazyClear: processing %s\n", toString(entry).c_str());

				// Level 1 (leaf) nodes should never be in the lazy delete queue
				ASSERT(btPage.height > 1);

				// Iterate over page entries, skipping key decoding using BTreePage::ValueTree which uses
				// RedwoodRecordRef::DeltaValueOnly as the delta type type to skip key decoding
				BTreePage::ValueTree::Mirror reader(&btPage.valueTree(), &dbBegin, &dbEnd);
				auto c = reader.getCursor();
				ASSERT(c.moveFirst());
				Version v = entry.version;
				while (1) {
					if (c.get().value.present()) {
						BTreePageIDRef btChildPageID = c.get().getChildPage();
						// If this page is height 2, then the children are leaves so free them directly
						if (btPage.height == 2) {
							debug_printf("LazyClear: freeing child %s\n", toString(btChildPageID).c_str());
							self->freeBTreePage(btChildPageID, v);
							freedPages += btChildPageID.size();
							metrics.lazyClearFree += 1;
							metrics.lazyClearFreeExt += (btChildPageID.size() - 1);
						} else {
							// Otherwise, queue them for lazy delete.
							debug_printf("LazyClear: queuing child %s\n", toString(btChildPageID).c_str());
							self->m_lazyClearQueue.pushFront(LazyClearQueueEntry{ v, btChildPageID });
							metrics.lazyClearRequeue += 1;
							metrics.lazyClearRequeueExt += (btChildPageID.size() - 1);
						}
					}
					if (!c.moveNext()) {
						break;
					}
				}

				// Free the page, now that its children have either been freed or queued
				debug_printf("LazyClear: freeing queue entry %s\n", toString(entry.pageID).c_str());
				self->freeBTreePage(entry.pageID, v);
				freedPages += entry.pageID.size();
				metrics.lazyClearFree += 1;
				metrics.lazyClearFreeExt += entry.pageID.size() - 1;
			}

			// Stop if
			//   - the poppable items in the queue have already been exhausted
			//   - stop flag is set and we've freed the minimum number of pages required
			//   - maximum number of pages to free met or exceeded
			if (toPop > 0 || (freedPages >= SERVER_KNOBS->REDWOOD_LAZY_CLEAR_MIN_PAGES && self->m_lazyClearStop) ||
			    (freedPages >= SERVER_KNOBS->REDWOOD_LAZY_CLEAR_MAX_PAGES)) {
				break;
			}
		}

		debug_printf("LazyClear: freed %d pages, %s has %" PRId64 " entries\n",
		             freedPages,
		             self->m_lazyClearQueue.name.c_str(),
		             self->m_lazyClearQueue.numEntries);
		return freedPages;
	}

	ACTOR static Future<Void> init_impl(VersionedBTree* self) {
		wait(self->m_pager->init());

		self->m_blockSize = self->m_pager->getUsablePageSize();
		state Version latest = self->m_pager->getLatestVersion();
		self->m_newOldestVersion = self->m_pager->getOldestVersion();

		debug_printf("Recovered pager to version %" PRId64 ", oldest version is %" PRId64 "\n",
		             self->m_newOldestVersion);

		state Key meta = self->m_pager->getMetaKey();
		if (meta.size() == 0) {
			self->m_header.formatVersion = MetaKey::FORMAT_VERSION;
			LogicalPageID id = wait(self->m_pager->newPageID());
			BTreePageIDRef newRoot((LogicalPageID*)&id, 1);
			debug_printf("new root %s\n", toString(newRoot).c_str());
			self->m_header.root.set(newRoot, sizeof(headerSpace) - sizeof(m_header));
			self->m_header.height = 1;
			++latest;
			Reference<ArenaPage> page = self->m_pager->newPageBuffer();
			makeEmptyRoot(page);
			self->m_pager->updatePage(id, page);
			self->m_pager->setCommitVersion(latest);

			LogicalPageID newQueuePage = wait(self->m_pager->newPageID());
			self->m_lazyClearQueue.create(self->m_pager, newQueuePage, "LazyClearQueue");
			self->m_header.lazyDeleteQueue = self->m_lazyClearQueue.getState();
			self->m_pager->setMetaKey(self->m_header.asKeyRef());
			wait(self->m_pager->commit());
			debug_printf("Committed initial commit.\n");
		} else {
			self->m_header.fromKeyRef(meta);
			self->m_lazyClearQueue.recover(self->m_pager, self->m_header.lazyDeleteQueue, "LazyClearQueueRecovered");
		}

		debug_printf("Recovered btree at version %" PRId64 ": %s\n", latest, self->m_header.toString().c_str());

		self->m_lastCommittedVersion = latest;
		self->m_lazyClearActor = incrementalLazyClear(self);
		return Void();
	}

	Future<Void> init() override { return m_init; }

	virtual ~VersionedBTree() {
		// This probably shouldn't be called directly (meaning deleting an instance directly) but it should be safe,
		// it will cancel init and commit and leave the pager alive but with potentially an incomplete set of
		// uncommitted writes so it should not be committed.
		m_init.cancel();
		m_latestCommit.cancel();
	}

	Reference<IStoreCursor> readAtVersion(Version v) override {
		// Only committed versions can be read.
		ASSERT(v <= m_lastCommittedVersion);
		Reference<IPagerSnapshot> snapshot = m_pager->getReadSnapshot(v);

		// This is a ref because snapshot will continue to hold the metakey value memory
		KeyRef m = snapshot->getMetaKey();

		// Currently all internal records generated in the write path are at version 0
		return Reference<IStoreCursor>(new Cursor(snapshot, ((MetaKey*)m.begin())->root.get(), (Version)0));
	}

	// Must be nondecreasing
	void setWriteVersion(Version v) override {
		ASSERT(v > m_lastCommittedVersion);
		// If there was no current mutation buffer, create one in the buffer map and update m_pBuffer
		if (m_pBuffer == nullptr) {
			// When starting a new mutation buffer its start version must be greater than the last write version
			ASSERT(v > m_writeVersion);
			m_pBuffer = &m_mutationBuffers[v];
		} else {
			// It's OK to set the write version to the same version repeatedly so long as m_pBuffer is not null
			ASSERT(v >= m_writeVersion);
		}
		m_writeVersion = v;
	}

	Future<Void> commit() override {
		if (m_pBuffer == nullptr)
			return m_latestCommit;
		return commit_impl(this);
	}

	ACTOR static Future<Void> clearAllAndCheckSanity_impl(VersionedBTree* self) {
		ASSERT(g_network->isSimulated());

		debug_printf("Clearing tree.\n");
		self->setWriteVersion(self->getLatestVersion() + 1);
		self->clear(KeyRangeRef(dbBegin.key, dbEnd.key));
		wait(self->commit());

		// Loop commits until the the lazy delete queue is completely processed.
		loop {
			wait(self->commit());

			// If the lazy delete queue is completely processed then the last time the lazy delete actor
			// was started it, after the last commit, it would exist immediately and do no work, so its
			// future would be ready and its value would be 0.
			if (self->m_lazyClearActor.isReady() && self->m_lazyClearActor.get() == 0) {
				break;
			}
			self->setWriteVersion(self->getLatestVersion() + 1);
		}

		// Forget all but the latest version of the tree.
		debug_printf("Discarding all old versions.\n");
		self->setOldestVersion(self->getLastCommittedVersion());
		self->setWriteVersion(self->getLatestVersion() + 1);
		wait(self->commit());

		// The lazy delete queue should now be empty and contain only the new page to start writing to
		// on the next commit.
		LazyClearQueueT::QueueState s = self->m_lazyClearQueue.getState();
		ASSERT(s.numEntries == 0);
		ASSERT(s.numPages == 1);

		// The btree should now be a single non-oversized root page.
		ASSERT(self->m_header.height == 1);
		ASSERT(self->m_header.root.count == 1);

		// From the pager's perspective the only pages that should be in use are the btree root and
		// the previously mentioned lazy delete queue page.
		int64_t userPageCount = wait(self->m_pager->getUserPageCount());
		ASSERT(userPageCount == 2);

		return Void();
	}

	Future<Void> clearAllAndCheckSanity() { return clearAllAndCheckSanity_impl(this); }

private:
	// Represents a change to a single key - set, clear, or atomic op
	struct SingleKeyMutation {
		// Clear
		SingleKeyMutation() : op(MutationRef::ClearRange) {}
		// Set
		SingleKeyMutation(Value val) : op(MutationRef::SetValue), value(val) {}
		// Atomic Op
		SingleKeyMutation(MutationRef::Type op, Value val) : op(op), value(val) {}

		MutationRef::Type op;
		Value value;

		inline bool isClear() const { return op == MutationRef::ClearRange; }
		inline bool isSet() const { return op == MutationRef::SetValue; }
		inline bool isAtomicOp() const { return !isSet() && !isClear(); }

		inline bool equalToSet(ValueRef val) { return isSet() && value == val; }

		inline RedwoodRecordRef toRecord(KeyRef userKey, Version version) const {
			// No point in serializing an atomic op, it needs to be coalesced to a real value.
			ASSERT(!isAtomicOp());

			if (isClear())
				return RedwoodRecordRef(userKey, version);

			return RedwoodRecordRef(userKey, version, value);
		}

		std::string toString() const { return format("op=%d val='%s'", op, printable(value).c_str()); }
	};

	struct RangeMutation {
		RangeMutation() : boundaryChanged(false), clearAfterBoundary(false) {}

		bool boundaryChanged;
		Optional<ValueRef> boundaryValue; // Not present means cleared
		bool clearAfterBoundary;

		bool boundaryCleared() const { return boundaryChanged && !boundaryValue.present(); }

		// Returns true if this RangeMutation doesn't actually mutate anything
		bool noChanges() const { return !boundaryChanged && !clearAfterBoundary; }

		void clearBoundary() {
			boundaryChanged = true;
			boundaryValue.reset();
		}

		void clearAll() {
			clearBoundary();
			clearAfterBoundary = true;
		}

		void setBoundaryValue(ValueRef v) {
			boundaryChanged = true;
			boundaryValue = v;
		}

		bool boundarySet() const { return boundaryChanged && boundaryValue.present(); }

		std::string toString() const {
			return format("boundaryChanged=%d clearAfterBoundary=%d boundaryValue=%s",
			              boundaryChanged,
			              clearAfterBoundary,
			              ::toString(boundaryValue).c_str());
		}
	};

public:
#include "fdbserver/ArtMutationBuffer.h"
	struct MutationBufferStdMap {
		MutationBufferStdMap() {
			// Create range representing the entire keyspace.  This reduces edge cases to applying mutations
			// because now all existing keys are within some range in the mutation map.
			mutations[dbBegin.key];
			// Setting the dbEnd key to be cleared prevents having to treat a range clear to dbEnd as a special
			// case in order to avoid traversing down the rightmost edge of the tree.
			mutations[dbEnd.key].clearBoundary();
		}

	private:
		typedef std::map<KeyRef, RangeMutation> MutationsT;
		Arena arena;
		MutationsT mutations;

	public:
		struct iterator : public MutationsT::iterator {
			typedef MutationsT::iterator Base;
			iterator() = default;
			iterator(const MutationsT::iterator& i) : Base(i) {}

			const KeyRef& key() { return (*this)->first; }

			RangeMutation& mutation() { return (*this)->second; }
		};

		struct const_iterator : public MutationsT::const_iterator {
			typedef MutationsT::const_iterator Base;
			const_iterator() = default;
			const_iterator(const MutationsT::const_iterator& i) : Base(i) {}
			const_iterator(const MutationsT::iterator& i) : Base(i) {}

			const KeyRef& key() { return (*this)->first; }

			const RangeMutation& mutation() { return (*this)->second; }
		};

		// Return a T constructed in arena
		template <typename T>
		T copyToArena(const T& object) {
			return T(arena, object);
		}

		const_iterator upper_bound(const KeyRef& k) const { return mutations.upper_bound(k); }

		const_iterator lower_bound(const KeyRef& k) const { return mutations.lower_bound(k); }

		// erase [begin, end) from the mutation map
		void erase(const const_iterator& begin, const const_iterator& end) { mutations.erase(begin, end); }

		// Find or create a mutation buffer boundary for bound and return an iterator to it
		iterator insert(KeyRef boundary) {
			// Find the first split point in buffer that is >= key
			// Since the initial state of the mutation buffer contains the range '' through
			// the maximum possible key, our search had to have found something so we
			// can assume the iterator is valid.
			iterator ib = mutations.lower_bound(boundary);

			// If we found the boundary we are looking for, return its iterator
			if (ib.key() == boundary) {
				return ib;
			}

			// ib is our insert hint.  Copy boundary into arena and insert boundary into buffer
			boundary = KeyRef(arena, boundary);
			ib = mutations.insert(ib, { boundary, RangeMutation() });

			// ib is certainly > begin() because it is guaranteed that the empty string
			// boundary exists and the only way to have found that is to look explicitly
			// for it in which case we would have returned above.
			iterator iPrevious = ib;
			--iPrevious;
			// If the range we just divided was being cleared, then the dividing boundary key and range after it must
			// also be cleared
			if (iPrevious.mutation().clearAfterBoundary) {
				ib.mutation().clearAll();
			}

			return ib;
		}
	};
#define USE_ART_MUTATION_BUFFER 1

#ifdef USE_ART_MUTATION_BUFFER
	typedef struct MutationBufferART MutationBuffer;
#else
	typedef struct MutationBufferStdMap MutationBuffer;
#endif

private:
	/* Mutation Buffer Overview
	 *
	 * This structure's organization is meant to put pending updates for the btree in an order
	 * that makes it efficient to query all pending mutations across all pending versions which are
	 * relevant to a particular subtree of the btree.
	 *
	 * At the top level, it is a map of the start of a range being modified to a RangeMutation.
	 * The end of the range is map key (which is the next range start in the map).
	 *
	 * - The buffer starts out with keys '' and endKVV.key already populated.
	 *
	 * - When a new key is inserted into the buffer map, it is by definition
	 *   splitting an existing range so it should take on the rangeClearVersion of
	 *   the immediately preceding key which is the start of that range
	 *
	 * - Keys are inserted into the buffer map for every individual operation (set/clear/atomic)
	 *   key and for both the start and end of a range clear.
	 *
	 * - To apply a single clear, add it to the individual ops only if the last entry is not also a clear.
	 *
	 * - To apply a range clear, after inserting the new range boundaries do the following to the start
	 *   boundary and all successive boundaries < end
	 *      - set the range clear version if not already set
	 *      - add a clear to the startKeyMutations if the final entry is not a clear.
	 *
	 * - Note that there are actually TWO valid ways to represent
	 *       set c = val1 at version 1
	 *       clear c\x00 to z at version 2
	 *   with this model.  Either
	 *      c =     { rangeClearVersion = 2, startKeyMutations = { 1 => val1 }
	 *      z =     { rangeClearVersion = <not present>, startKeyMutations = {}
	 *   OR
	 *      c =     { rangeClearVersion = <not present>, startKeyMutations = { 1 => val1 }
	 *      c\x00 = { rangeClearVersion = 2, startKeyMutations = { 2 => <not present> }
	 *      z =     { rangeClearVersion = <not present>, startKeyMutations = {}
	 *
	 *   This is because the rangeClearVersion applies to a range begining with the first
	 *   key AFTER the start key, so that the logic for reading the start key is more simple
	 *   as it only involves consulting startKeyMutations.  When adding a clear range, the
	 *   boundary key insert/split described above is valid, and is what is currently done,
	 *   but it would also be valid to see if the last key before startKey is equal to
	 *   keyBefore(startKey), and if so that mutation buffer boundary key can be used instead
	 *   without adding an additional key to the buffer.

	 * TODO: A possible optimization here could be to only use existing btree leaf page boundaries as keys,
	 * with mutation point keys being stored in an unsorted strucutre under those boundary map keys,
	 * to be sorted later just before being merged into the existing leaf page.
	 */

	IPager2* m_pager;
	MutationBuffer* m_pBuffer;
	std::map<Version, MutationBuffer> m_mutationBuffers;

	Version m_writeVersion;
	Version m_lastCommittedVersion;
	Version m_newOldestVersion;
	Reference<FlowLock> m_commitReadLock;
	Future<Void> m_latestCommit;
	Future<Void> m_init;
	std::string m_name;
	int m_blockSize;
	std::unordered_map<LogicalPageID, ParentInfo> parents;
	ParentInfoMapT childUpdateTracker;

	// MetaKey changes size so allocate space for it to expand into. FIXME: Steve is fixing this to be dynamically
	// sized.
	union {
		uint8_t headerSpace[sizeof(MetaKey) + sizeof(LogicalPageID) * 200];
		MetaKey m_header;
	};

	LazyClearQueueT m_lazyClearQueue;
	Future<int> m_lazyClearActor;
	bool m_lazyClearStop;

	// Describes a range of a vector of records that should be built into a BTreePage
	struct PageToBuild {
		PageToBuild(int index, int blockSize)
		  : startIndex(index), count(0), pageSize(blockSize),
		    bytesLeft(blockSize - sizeof(BTreePage) - sizeof(BTreePage::BinaryTree)),
		    largeDeltaTree(pageSize > BTreePage::BinaryTree::SmallSizeLimit), blockSize(blockSize), blockCount(1),
		    kvBytes(0) {}

		int startIndex; // Index of the first record
		int count; // Number of records added to the page
		int pageSize; // Page size required to hold a BTreePage of the added records, which is a multiple of blockSize
		int bytesLeft; // Bytes in pageSize that are unused by the BTreePage so far
		bool largeDeltaTree; // Whether or not the DeltaTree in the generated page is in the 'large' size range
		int blockSize; // Base block size by which pageSize can be incremented
		int blockCount; // The number of blocks in pageSize
		int kvBytes; // The amount of user key/value bytes added to the page

		// Number of bytes used by the generated/serialized BTreePage
		int size() const { return pageSize - bytesLeft; }

		// Used fraction of pageSize bytes
		double usedFraction() const { return (double)size() / pageSize; }

		// Unused fraction of pageSize bytes
		double slackFraction() const { return (double)bytesLeft / pageSize; }

		// Fraction of PageSize in use by key or value string bytes, disregarding all overhead including string sizes
		double kvFraction() const { return (double)kvBytes / pageSize; }

		// Index of the last record to be included in this page
		int lastIndex() const { return endIndex() - 1; }

		// Index of the first record NOT included in this page
		int endIndex() const { return startIndex + count; }

		std::string toString() const {
			return format(
			    "{start=%d count=%d used %d/%d bytes (%.2f%% slack) kvBytes=%d blocks=%d blockSize=%d large=%d}",
			    startIndex,
			    count,
			    size(),
			    pageSize,
			    slackFraction() * 100,
			    kvBytes,
			    blockCount,
			    blockSize,
			    largeDeltaTree);
		}

		// Move an item from a to b if a has 2 or more items and the item fits in b
		// a and b must be consecutive pages from the same array of records
		static bool shiftItem(PageToBuild& a, PageToBuild& b, int deltaSize, int kvBytes) {
			if (a.count < 2) {
				return false;
			}

			// Size of the nodes in A and B, respectively
			int aNodeSize = deltaSize + BTreePage::BinaryTree::Node::headerSize(a.largeDeltaTree);
			int bNodeSize = deltaSize + BTreePage::BinaryTree::Node::headerSize(b.largeDeltaTree);

			if (b.bytesLeft < bNodeSize) {
				return false;
			}

			--a.count;
			++b.count;
			--b.startIndex;
			a.bytesLeft += aNodeSize;
			b.bytesLeft -= bNodeSize;
			a.kvBytes -= kvBytes;
			b.kvBytes += kvBytes;

			return true;
		}

		// Try to add a record of the given delta size to the page.
		// If force is true, the page will be expanded to make the record fit if needed.
		// Return value is whether or not the record was added to the page.
		bool addRecord(const RedwoodRecordRef& rec, int deltaSize, bool force) {
			int nodeSize = deltaSize + BTreePage::BinaryTree::Node::headerSize(largeDeltaTree);

			// If the record doesn't fit and the page can't be expanded then return false
			if (nodeSize > bytesLeft && !force) {
				return false;
			}

			++count;
			bytesLeft -= nodeSize;
			kvBytes += rec.kvBytes();

			// If needed, expand page so that record fits.
			// This is a loop because the first expansion may increase per-node overhead which could
			// then require a second expansion.
			while (bytesLeft < 0) {
				int newBlocks = (-bytesLeft + blockSize - 1) / blockSize;
				int extraSpace = newBlocks * blockSize;
				blockCount += newBlocks;
				bytesLeft += extraSpace;
				pageSize += extraSpace;

				// If size has moved into the "large" range then every node has gotten bigger so adjust bytesLeft
				if (!largeDeltaTree && pageSize > BTreePage::BinaryTree::SmallSizeLimit) {
					largeDeltaTree = true;
					bytesLeft -= (count * BTreePage::BinaryTree::LargeTreePerNodeExtraOverhead);
				}
			}
			return true;
		}
	};

	// Scans a vector of records and decides on page split points, returning a vector of 1+ pages to build
	static std::vector<PageToBuild> splitPages(const RedwoodRecordRef* lowerBound,
	                                           const RedwoodRecordRef* upperBound,
	                                           int prefixLen,
	                                           VectorRef<RedwoodRecordRef> records,
	                                           int height,
	                                           int blockSize) {
		debug_printf("splitPages height=%d records=%d lowerBound=%s upperBound=%s\n",
		             height,
		             records.size(),
		             lowerBound->toString(false).c_str(),
		             upperBound->toString(false).c_str());
		ASSERT(!records.empty());

		// Leaves can have just one record if it's large, but internal pages should have at least 4
		int minRecords = height == 1 ? 1 : 4;
		double maxSlack = SERVER_KNOBS->REDWOOD_PAGE_REBUILD_MAX_SLACK;
		std::vector<PageToBuild> pages;

		// deltaSizes contains pair-wise delta sizes for [lowerBound, records..., upperBound]
		std::vector<int> deltaSizes(records.size() + 1);
		deltaSizes.front() = records.front().deltaSize(*lowerBound, prefixLen, true);
		deltaSizes.back() = records.back().deltaSize(*upperBound, prefixLen, true);
		for (int i = 1; i < records.size(); ++i) {
			deltaSizes[i] = records[i].deltaSize(records[i - 1], prefixLen, true);
		}

		PageToBuild p(0, blockSize);

		for (int i = 0; i < records.size(); ++i) {
			bool force = p.count < minRecords || p.slackFraction() > maxSlack;
			debug_printf(
			    "  before addRecord  i=%d  records=%d  deltaSize=%d  kvSize=%d  force=%d  pageToBuild=%s  record=%s",
			    i,
			    records.size(),
			    deltaSizes[i],
			    records[i].kvBytes(),
			    force,
			    p.toString().c_str(),
			    records[i].toString(height == 1).c_str());

			if (!p.addRecord(records[i], deltaSizes[i], force)) {
				pages.push_back(p);
				p = PageToBuild(p.endIndex(), blockSize);
				p.addRecord(records[i], deltaSizes[i], true);
			}
		}

		if (p.count > 0) {
			pages.push_back(p);
		}

		debug_printf("  Before shift: %s\n", ::toString(pages).c_str());

		// If page count is > 1, try to balance slack between last two pages
		// The buggify disables this balancing as this will result in more edge
		// cases of pages with very few records.
		if (pages.size() > 1 && !BUGGIFY) {
			PageToBuild& a = pages[pages.size() - 2];
			PageToBuild& b = pages.back();

			// While the last page page has too much slack and the second to last page
			// has more than the minimum record count, shift a record from the second
			// to last page to the last page.
			while (b.slackFraction() > maxSlack && a.count > minRecords) {
				int i = a.lastIndex();
				if (!PageToBuild::shiftItem(a, b, deltaSizes[i], records[i].kvBytes())) {
					break;
				}
				debug_printf("  After shifting i=%d: a=%s b=%s\n", i, a.toString().c_str(), b.toString().c_str());
			}
		}

		return pages;
	}

	// Writes entries to 1 or more pages and return a vector of boundary keys with their ArenaPage(s)
	ACTOR static Future<Standalone<VectorRef<RedwoodRecordRef>>> writePages(VersionedBTree* self,
	                                                                        const RedwoodRecordRef* lowerBound,
	                                                                        const RedwoodRecordRef* upperBound,
	                                                                        VectorRef<RedwoodRecordRef> entries,
	                                                                        int height,
	                                                                        Version v,
	                                                                        BTreePageIDRef previousID) {
		ASSERT(entries.size() > 0);

		state Standalone<VectorRef<RedwoodRecordRef>> records;

		// All records share the prefix shared by the lower and upper boundaries
		state int prefixLen = lowerBound->getCommonPrefixLen(*upperBound);

		state std::vector<PageToBuild> pagesToBuild =
		    splitPages(lowerBound, upperBound, prefixLen, entries, height, self->m_blockSize);
		debug_printf("splitPages returning %s\n", toString(pagesToBuild).c_str());

		// Lower bound of the page being added to
		state RedwoodRecordRef pageLowerBound = lowerBound->withoutValue();
		state RedwoodRecordRef pageUpperBound;

		state int pageIndex;

		for (pageIndex = 0; pageIndex < pagesToBuild.size(); ++pageIndex) {
			auto& p = pagesToBuild[pageIndex];
			debug_printf("building page %d of %d %s\n", pageIndex + 1, pagesToBuild.size(), p.toString().c_str());
			ASSERT(p.count != 0);

			// For internal pages, skip first entry if child link is null.  Such links only exist
			// to maintain a borrow-able prefix for the previous subtree after a subtree deletion.
			// If the null link falls on a new page post-split, then the pageLowerBound of the page
			// being built now will serve as the previous subtree's upper boundary as it is the same
			// key as entries[p.startIndex] and there is no need to actually store the null link in
			// the new page.
			if (height != 1 && !entries[p.startIndex].value.present()) {
				p.kvBytes -= entries[p.startIndex].key.size();
				++p.startIndex;
				--p.count;
				debug_printf("Skipping first null record, new count=%d\n", p.count);

				// If the page is now empty then it must be the last page in pagesToBuild, otherwise there would
				// be more than 1 item since internal pages need to have multiple children. While there is no page
				// to be built here, a record must be added to the output set because the upper boundary of the last
				// page built does not match the upper boundary of the original page that this call to writePages() is
				// replacing.  Put another way, the upper boundary of the rightmost page of the page set that was just
				// built does not match the upper boundary of the original page that the page set is replacing, so
				// adding the extra null link fixes this.
				if (p.count == 0) {
					ASSERT(pageIndex == pagesToBuild.size() - 1);
					records.push_back_deep(records.arena(), pageUpperBound);
					break;
				}
			}

			// Use the next entry as the upper bound, or upperBound if there are no more entries beyond this page
			int endIndex = p.endIndex();
			bool lastPage = endIndex == entries.size();
			pageUpperBound = lastPage ? upperBound->withoutValue() : entries[endIndex].withoutValue();

			// If this is a leaf page, and not the last one to be written, shorten the upper boundary
			if (!lastPage && height == 1) {
				int commonPrefix = pageUpperBound.getCommonPrefixLen(entries[endIndex - 1], prefixLen);
				pageUpperBound.truncate(commonPrefix + 1);
			}

			state std::vector<Reference<ArenaPage>> pages;
			BTreePage* btPage;

			if (p.blockCount == 1) {
				Reference<ArenaPage> page = self->m_pager->newPageBuffer();
				btPage = (BTreePage*)page->mutate();
				pages.push_back(std::move(page));
			} else {
				ASSERT(p.blockCount > 1);
				btPage = (BTreePage*)new uint8_t[p.pageSize];
			}

			btPage->height = height;
			btPage->kvBytes = p.kvBytes;

			debug_printf("Building tree for %s\nlower: %s\nupper: %s\n",
			             p.toString().c_str(),
			             pageLowerBound.toString(false).c_str(),
			             pageUpperBound.toString(false).c_str());

			int deltaTreeSpace = p.pageSize - sizeof(BTreePage);
			state int written = btPage->tree().build(
			    deltaTreeSpace, &entries[p.startIndex], &entries[endIndex], &pageLowerBound, &pageUpperBound);

			if (written > deltaTreeSpace) {
				debug_printf("ERROR:  Wrote %d bytes to page %s deltaTreeSpace=%d\n",
				             written,
				             p.toString().c_str(),
				             deltaTreeSpace);
				TraceEvent(SevError, "RedwoodDeltaTreeOverflow")
				    .detail("PageSize", p.pageSize)
				    .detail("BytesWritten", written);
				ASSERT(false);
			}

			auto& metrics = g_redwoodMetrics.level(btPage->height);
			metrics.pageBuild += 1;
			metrics.pageBuildExt += p.blockCount - 1;
			metrics.buildFillPct += p.usedFraction();
			metrics.buildStoredPct += p.kvFraction();
			metrics.buildItemCount += p.count;

			// Create chunked pages
			// TODO: Avoid copying page bytes, but this is not trivial due to how pager checksums are currently handled.
			if (p.blockCount != 1) {
				// Mark the slack in the page buffer as defined
				VALGRIND_MAKE_MEM_DEFINED(((uint8_t*)btPage) + written, (p.blockCount * p.blockSize) - written);
				const uint8_t* rptr = (const uint8_t*)btPage;
				for (int b = 0; b < p.blockCount; ++b) {
					Reference<ArenaPage> page = self->m_pager->newPageBuffer();
					memcpy(page->mutate(), rptr, p.blockSize);
					rptr += p.blockSize;
					pages.push_back(std::move(page));
				}
				delete[](uint8_t*) btPage;
			}

			// Write this btree page, which is made of 1 or more pager pages.
			state BTreePageIDRef childPageID;
			state int k;

			// If we are only writing 1 page and it has the same BTreePageID size as the original then try to reuse the
			// LogicalPageIDs in previousID and try to update them atomically.
			if (pagesToBuild.size() == 1 && previousID.size() == pages.size()) {
				for (k = 0; k < pages.size(); ++k) {
					LogicalPageID id = wait(self->m_pager->atomicUpdatePage(previousID[k], pages[k], v));
					childPageID.push_back(records.arena(), id);
				}
			} else {
				// Either the original page is being split, or it's not but it has changed BTreePageID size.
				// Either way, there is no point in reusing any of the original page IDs because the parent
				// must be rewritten anyway to count for the change in child count or child links.
				// Free the old IDs, but only once (before the first output record is added).
				if (records.empty()) {
					self->freeBTreePage(previousID, v);
				}
				for (k = 0; k < pages.size(); ++k) {
					LogicalPageID id = wait(self->m_pager->newPageID());
					self->m_pager->updatePage(id, pages[k]);
					childPageID.push_back(records.arena(), id);
				}
			}

			wait(yield());

			if (REDWOOD_DEBUG) {
				auto& p = pagesToBuild[pageIndex];
				debug_printf("Wrote %s original=%s deltaTreeSize=%d for %s\nlower: %s\nupper: %s\n",
				             toString(childPageID).c_str(),
				             toString(previousID).c_str(),
				             written,
				             p.toString().c_str(),
				             pageLowerBound.toString(false).c_str(),
				             pageUpperBound.toString(false).c_str());
				for (int j = p.startIndex; j < p.endIndex(); ++j) {
					debug_printf(" %3d: %s\n", j, entries[j].toString(height == 1).c_str());
				}
				ASSERT(pageLowerBound.key <= pageUpperBound.key);
			}

			// Push a new record onto the results set, without the child page, copying it into the records arena
			records.push_back_deep(records.arena(), pageLowerBound.withoutValue());
			// Set the child page value of the inserted record to childPageID, which has already been allocated in
			// records.arena() above
			records.back().setChildPage(childPageID);

			pageLowerBound = pageUpperBound;
		}

		return records;
	}

	ACTOR static Future<Standalone<VectorRef<RedwoodRecordRef>>>
	buildNewRoot(VersionedBTree* self, Version version, Standalone<VectorRef<RedwoodRecordRef>> records, int height) {
		debug_printf("buildNewRoot start version %" PRId64 ", %lu records\n", version, records.size());

		// While there are multiple child pages for this version we must write new tree levels.
		while (records.size() > 1) {
			self->m_header.height = ++height;
			Standalone<VectorRef<RedwoodRecordRef>> newRecords =
			    wait(writePages(self, &dbBegin, &dbEnd, records, height, version, BTreePageIDRef()));
			debug_printf("Wrote a new root level at version %" PRId64 " height %d size %lu pages\n",
			             version,
			             height,
			             newRecords.size());
			records = newRecords;
		}

		return records;
	}

	// Try to evict a BTree page from the pager cache.
	// Returns true if, at the end of the call, the page is no longer in cache,
	// so the caller can assume its ArenaPage reference is the only one.
	bool tryEvictPage(IPagerSnapshot* pager, BTreePageIDRef id) {
		// If it's an oversized page, currently it cannot be in the cache
		if (id.size() > 0) {
			return true;
		}
		return pager->tryEvictPage(id.front());
	}

	ACTOR static Future<Reference<const ArenaPage>> readPage(Reference<IPagerSnapshot> snapshot,
	                                                         BTreePageIDRef id,
	                                                         const RedwoodRecordRef* lowerBound,
	                                                         const RedwoodRecordRef* upperBound,
	                                                         bool forLazyClear = false,
	                                                         bool cacheable = true,
	                                                         bool* fromCache = nullptr) {
		if (!forLazyClear) {
			debug_printf("readPage() op=read %s @%" PRId64 " lower=%s upper=%s\n",
			             toString(id).c_str(),
			             snapshot->getVersion(),
			             lowerBound->toString(false).c_str(),
			             upperBound->toString(false).c_str());
		} else {
			debug_printf(
			    "readPage() op=readForDeferredClear %s @%" PRId64 " \n", toString(id).c_str(), snapshot->getVersion());
		}

		wait(yield());

		state Reference<const ArenaPage> page;

		if (id.size() == 1) {
			Reference<const ArenaPage> p = wait(snapshot->getPhysicalPage(id.front(), cacheable, false, fromCache));
			page = std::move(p);
		} else {
			ASSERT(!id.empty());
			std::vector<Future<Reference<const ArenaPage>>> reads;
			for (auto& pageID : id) {
				reads.push_back(snapshot->getPhysicalPage(pageID, cacheable, false));
			}
			std::vector<Reference<const ArenaPage>> pages = wait(getAll(reads));
			// TODO:  Cache reconstituted super pages somehow, perhaps with help from the Pager.
			page = ArenaPage::concatPages(pages);

			// In the current implementation, SuperPages are never present in the cache
			if (fromCache != nullptr) {
				*fromCache = false;
			}
		}

		debug_printf("readPage() op=readComplete %s @%" PRId64 " \n", toString(id).c_str(), snapshot->getVersion());
		const BTreePage* pTreePage = (const BTreePage*)page->begin();
		auto& metrics = g_redwoodMetrics.level(pTreePage->height);
		metrics.pageRead += 1;
		metrics.pageReadExt += (id.size() - 1);

		if (!forLazyClear && page->userData == nullptr) {
			debug_printf("readPage() Creating Mirror for %s @%" PRId64 " lower=%s upper=%s\n",
			             toString(id).c_str(),
			             snapshot->getVersion(),
			             lowerBound->toString(false).c_str(),
			             upperBound->toString(false).c_str());
			page->userData = new BTreePage::BinaryTree::Mirror(&pTreePage->tree(), lowerBound, upperBound);
			page->userDataDestructor = [](void* ptr) { delete (BTreePage::BinaryTree::Mirror*)ptr; };
		}

		if (!forLazyClear) {
			debug_printf("readPage() %s\n",
			             pTreePage->toString(false, id, snapshot->getVersion(), lowerBound, upperBound).c_str());
		}

		return std::move(page);
	}

	static void preLoadPage(IPagerSnapshot* snapshot, BTreePageIDRef id) {
		g_redwoodMetrics.btreeLeafPreload += 1;
		g_redwoodMetrics.btreeLeafPreloadExt += (id.size() - 1);

		for (auto pageID : id) {
			snapshot->getPhysicalPage(pageID, true, true);
		}
	}

	void freeBTreePage(BTreePageIDRef btPageID, Version v) {
		// Free individual pages at v
		for (LogicalPageID id : btPageID) {
			m_pager->freePage(id, v);
		}
	}

	// Write new version of pageID at version v using page as its data.
	// Attempts to reuse original id(s) in btPageID, returns BTreePageID.
	ACTOR static Future<BTreePageIDRef> updateBTreePage(VersionedBTree* self,
	                                                    BTreePageIDRef oldID,
	                                                    Arena* arena,
	                                                    Reference<ArenaPage> page,
	                                                    Version writeVersion) {
		state BTreePageIDRef newID;
		newID.resize(*arena, oldID.size());

		if (oldID.size() == 1) {
			LogicalPageID id = wait(self->m_pager->atomicUpdatePage(oldID.front(), page, writeVersion));
			newID.front() = id;
		} else {
			state std::vector<Reference<ArenaPage>> pages;
			const uint8_t* rptr = page->begin();
			int bytesLeft = page->size();
			while (bytesLeft > 0) {
				Reference<ArenaPage> p = self->m_pager->newPageBuffer();
				int blockSize = p->size();
				memcpy(p->mutate(), rptr, blockSize);
				rptr += blockSize;
				bytesLeft -= blockSize;
				pages.push_back(p);
			}
			ASSERT(pages.size() == oldID.size());

			// Write pages, trying to reuse original page IDs
			state int i = 0;
			for (; i < pages.size(); ++i) {
				LogicalPageID id = wait(self->m_pager->atomicUpdatePage(oldID[i], pages[i], writeVersion));
				newID[i] = id;
			}
		}

		return newID;
	}

	// Copy page and initialize a Mirror for reading it.
	Reference<ArenaPage> cloneForUpdate(Reference<const ArenaPage> page) {
		Reference<ArenaPage> newPage = page->cloneContents();

		auto oldMirror = (const BTreePage::BinaryTree::Mirror*)page->userData;
		auto newBTPage = (BTreePage*)newPage->mutate();

		newPage->userData =
		    new BTreePage::BinaryTree::Mirror(&newBTPage->tree(), oldMirror->lowerBound(), oldMirror->upperBound());
		newPage->userDataDestructor = [](void* ptr) { delete (BTreePage::BinaryTree::Mirror*)ptr; };
		return newPage;
	}

	// Each call to commitSubtree() will pass most of its arguments via a this structure because the caller
	// will need access to these parameters after commitSubtree() is done.
	struct InternalPageSliceUpdate {
		// The logical range for the subtree's contents.  Due to subtree clears, these boundaries may not match
		// the lower/upper bounds needed to decode the page.
		// Subtree clears can cause the boundaries for decoding the page to be more restrictive than the subtree's
		// logical boundaries.  When a subtree is fully cleared, the link to it is replaced with a null link, but
		// the key boundary remains in tact to support decoding of the previous subtree.
		const RedwoodRecordRef* subtreeLowerBound;
		const RedwoodRecordRef* subtreeUpperBound;

		// The lower/upper bound for decoding the root of the subtree
		const RedwoodRecordRef* decodeLowerBound;
		const RedwoodRecordRef* decodeUpperBound;

		bool boundariesNormal() const {
			// If the decode upper boundary is the subtree upper boundary the pointers will be the same
			// For the lower boundary, if the pointers are not the same there is still a possibility
			// that the keys are the same.  This happens for the first remaining subtree of an internal page
			// after the prior subtree(s) were cleared.
			return (decodeUpperBound == subtreeUpperBound) &&
			       (decodeLowerBound == subtreeLowerBound || decodeLowerBound->sameExceptValue(*subtreeLowerBound));
		}

		// The record range of the subtree slice is cBegin to cEnd
		// cBegin.get().getChildPage() is guaranteed to be valid
		// cEnd can be
		//   - the next record which also has a child page
		//   - the next-next record which has a child page because the next record does not and
		//     only existed to provide the correct upper bound for decoding cBegin's child page
		//   - a later record with a valid child page, because this slice represents a range of
		//     multiple subtrees that are either all unchanged or all cleared.
		//   - invalid, because cBegin is the last child entry in the page or because the range
		//     being cleared or unchanged extends to the end of the page's entries
		BTreePage::BinaryTree::Cursor cBegin;
		BTreePage::BinaryTree::Cursor cEnd;

		// The prefix length common to the entire logical subtree.  Might be shorter than the length common to all
		// actual items in the page.
		int skipLen;

		// Members below this point are "output" members, set by function calls from commitSubtree() once it decides
		// what is happening with this slice of the tree.

		// If true, present, the contents of newLinks should replace [cBegin, cEnd)
		bool childrenChanged;
		Standalone<VectorRef<RedwoodRecordRef>> newLinks;

		// The upper boundary expected, if any, by the last child in either [cBegin, cEnd) or newLinks
		// If the last record in the range has a null link then this will be null.
		const RedwoodRecordRef* expectedUpperBound;

		bool inPlaceUpdate;

		// CommitSubtree will call one of the following three functions based on its exit path

		// Subtree was cleared.
		void cleared() {
			inPlaceUpdate = false;
			childrenChanged = true;
			expectedUpperBound = nullptr;
		}

		// Page was updated in-place through edits and written to maybeNewID
		void updatedInPlace(BTreePageIDRef maybeNewID, BTreePage* btPage, int capacity) {
			inPlaceUpdate = true;
			auto& metrics = g_redwoodMetrics.level(btPage->height);
			metrics.pageModify += 1;
			metrics.pageModifyExt += (maybeNewID.size() - 1);
			metrics.modifyFillPct += (double)btPage->size() / capacity;
			metrics.modifyStoredPct += (double)btPage->kvBytes / capacity;
			metrics.modifyItemCount += btPage->tree().numItems;

			// The boundaries can't have changed, but the child page link may have.
			if (maybeNewID != decodeLowerBound->getChildPage()) {
				// Add page's decode lower bound to newLinks set without its child page, intially
				newLinks.push_back_deep(newLinks.arena(), decodeLowerBound->withoutValue());

				// Set the child page ID, which has already been allocated in result.arena()
				newLinks.back().setChildPage(maybeNewID);
				childrenChanged = true;
			} else {
				childrenChanged = false;
			}

			// Expected upper bound remains unchanged.
		}

		// writePages() was used to build 1 or more replacement pages.
		void rebuilt(Standalone<VectorRef<RedwoodRecordRef>> newRecords) {
			inPlaceUpdate = false;
			newLinks = newRecords;
			childrenChanged = true;

			// If the replacement records ended on a non-null child page, then the expect upper bound is
			// the subtree upper bound since that is what would have been used for the page(s) rebuild,
			// otherwise it is null.
			expectedUpperBound = newLinks.back().value.present() ? subtreeUpperBound : nullptr;
		}

		// Get the first record for this range AFTER applying whatever changes were made
		const RedwoodRecordRef* getFirstBoundary() const {
			if (childrenChanged) {
				if (newLinks.empty()) {
					return nullptr;
				}
				return &newLinks.front();
			}
			return decodeLowerBound;
		}

		std::string toString() const {
			std::string s;
			s += format("SubtreeSlice: addr=%p skipLen=%d subtreeCleared=%d childrenChanged=%d inPlaceUpdate=%d\n",
			            this,
			            skipLen,
			            childrenChanged && newLinks.empty(),
			            childrenChanged,
			            inPlaceUpdate);
			s += format("SubtreeLower: %s\n", subtreeLowerBound->toString(false).c_str());
			s += format(" DecodeLower: %s\n", decodeLowerBound->toString(false).c_str());
			s += format(" DecodeUpper: %s\n", decodeUpperBound->toString(false).c_str());
			s += format("SubtreeUpper: %s\n", subtreeUpperBound->toString(false).c_str());
			s += format("expectedUpperBound: %s\n",
			            expectedUpperBound ? expectedUpperBound->toString(false).c_str() : "(null)");
			for (int i = 0; i < newLinks.size(); ++i) {
				s += format("  %i: %s\n", i, newLinks[i].toString(false).c_str());
			}
			s.resize(s.size() - 1);
			return s;
		}
	};

	struct InternalPageModifier {
		InternalPageModifier() {}
		InternalPageModifier(BTreePage* p, BTreePage::BinaryTree::Mirror* m, bool updating, ParentInfo* parentInfo)
		  : btPage(p), m(m), updating(updating), changesMade(false), parentInfo(parentInfo) {}

		bool updating;
		BTreePage* btPage;
		BTreePage::BinaryTree::Mirror* m;
		Standalone<VectorRef<RedwoodRecordRef>> rebuild;
		bool changesMade;
		ParentInfo* parentInfo;

		bool empty() const {
			if (updating) {
				return m->tree->numItems == 0;
			} else {
				return rebuild.empty();
			}
		}

		// end is the cursor position of the first record of the unvisited child link range, which
		// is needed if the insert requires switching from update to rebuild mode.
		void insert(BTreePage::BinaryTree::Cursor end, const VectorRef<RedwoodRecordRef>& recs) {
			int i = 0;
			if (updating) {
				// TODO: insert recs in a random order to avoid new subtree being entirely right child links
				while (i != recs.size()) {
					const RedwoodRecordRef& rec = recs[i];
					debug_printf("internal page (updating) insert: %s\n", rec.toString(false).c_str());

					if (!m->insert(rec)) {
						debug_printf("internal page: failed to insert %s, switching to rebuild\n",
						             rec.toString(false).c_str());
						// Update failed, so populate rebuild vector with everything up to but not including end, which
						// may include items from recs that were already added.
						auto c = end;
						if (c.moveFirst()) {
							rebuild.reserve(rebuild.arena(), c.mirror->tree->numItems);
							while (c != end) {
								debug_printf("  internal page rebuild: add %s\n", c.get().toString(false).c_str());
								rebuild.push_back(rebuild.arena(), c.get());
								c.moveNext();
							}
						}
						updating = false;
						break;
					}
					btPage->kvBytes += rec.kvBytes();
					++i;
				}
			}

			// Not updating existing page so just add recs to rebuild vector
			if (!updating) {
				rebuild.reserve(rebuild.arena(), rebuild.size() + recs.size());
				while (i != recs.size()) {
					const RedwoodRecordRef& rec = recs[i];
					debug_printf("internal page (rebuilding) insert: %s\n", rec.toString(false).c_str());
					rebuild.push_back(rebuild.arena(), rec);
					++i;
				}
			}
		}

		void keep(BTreePage::BinaryTree::Cursor begin, BTreePage::BinaryTree::Cursor end) {
			if (!updating) {
				while (begin != end) {
					debug_printf("internal page (rebuilding) keeping: %s\n", begin.get().toString(false).c_str());
					rebuild.push_back(rebuild.arena(), begin.get());
					begin.moveNext();
				}
			} else if (REDWOOD_DEBUG) {
				while (begin != end) {
					debug_printf("internal page (updating) keeping: %s\n", begin.get().toString(false).c_str());
					begin.moveNext();
				}
			}
		}

		// This must be called for each of the InternalPageSliceUpdates in sorted order.
		void applyUpdate(InternalPageSliceUpdate& u, const RedwoodRecordRef* nextBoundary) {
			debug_printf("applyUpdate nextBoundary=(%p) %s  %s\n",
			             nextBoundary,
			             (nextBoundary != nullptr) ? nextBoundary->toString(false).c_str() : "",
			             u.toString().c_str());

			// If the children changed, replace [cBegin, cEnd) with newLinks
			if (u.childrenChanged) {
				if (updating) {
					auto c = u.cBegin;
					while (c != u.cEnd) {
						debug_printf("internal page (updating) erasing: %s\n", c.get().toString(false).c_str());
						btPage->kvBytes -= c.get().kvBytes();
						c.erase();
					}
					// [cBegin, cEnd) is now erased, and cBegin is invalid, so cEnd represents the end
					// of the range that comes before any part of newLinks that can't be added if there
					// is not enough space.
					insert(u.cEnd, u.newLinks);
				} else {
					// Already in rebuild mode so the cursor parameter is meaningless
					insert({}, u.newLinks);
				}

				// cBegin has been erased so interating from the first entry forward will never see cBegin to use as an
				// endpoint.
				changesMade = true;
			} else {

				if (u.inPlaceUpdate) {
					for (auto id : u.decodeLowerBound->getChildPage()) {
						parentInfo->pageUpdated(id);
					}
				}

				keep(u.cBegin, u.cEnd);
			}

			// If there is an expected upper boundary for the next range after u
			if (u.expectedUpperBound != nullptr) {
				// Then if it does not match the next boundary then insert a dummy record
				if (nextBoundary == nullptr ||
				    (nextBoundary != u.expectedUpperBound && !nextBoundary->sameExceptValue(*u.expectedUpperBound))) {
					RedwoodRecordRef rec = u.expectedUpperBound->withoutValue();
					debug_printf("applyUpdate adding dummy record %s\n", rec.toString(false).c_str());
					insert(u.cEnd, { &rec, 1 });
					changesMade = true;
				}
			}
		}
	};

	ACTOR static Future<Void> commitSubtree(
	    VersionedBTree* self,
	    Reference<IPagerSnapshot> snapshot,
	    MutationBuffer* mutationBuffer,
	    BTreePageIDRef rootID,
	    bool isLeaf,
	    MutationBuffer::const_iterator mBegin, // greatest mutation boundary <= subtreeLowerBound->key
	    MutationBuffer::const_iterator mEnd, // least boundary >= subtreeUpperBound->key
	    InternalPageSliceUpdate* update) {

		state std::string context;
		if (REDWOOD_DEBUG) {
			context = format("CommitSubtree(root=%s): ", toString(rootID).c_str());
		}
		debug_printf("%s %s\n", context.c_str(), update->toString().c_str());
		if (REDWOOD_DEBUG) {
			debug_printf("%s ---------MUTATION BUFFER SLICE ---------------------\n", context.c_str());
			auto begin = mBegin;
			while (1) {
				debug_printf("%s Mutation: '%s':  %s\n",
				             context.c_str(),
				             printable(begin.key()).c_str(),
				             begin.mutation().toString().c_str());
				if (begin == mEnd) {
					break;
				}
				++begin;
			}
			debug_printf("%s -------------------------------------\n", context.c_str());
		}

		state Version writeVersion = self->getLastCommittedVersion() + 1;

		state Reference<FlowLock> commitReadLock = self->m_commitReadLock;
		wait(commitReadLock->take());
		state FlowLock::Releaser readLock(*commitReadLock);
		state bool fromCache = false;
		state Reference<const ArenaPage> page = wait(
		    readPage(snapshot, rootID, update->decodeLowerBound, update->decodeUpperBound, false, false, &fromCache));
		readLock.release();

		state BTreePage* btPage = (BTreePage*)page->begin();
		ASSERT(isLeaf == btPage->isLeaf());
		g_redwoodMetrics.level(btPage->height).pageCommitStart += 1;

		// TODO:  Decide if it is okay to update if the subtree boundaries are expanded.  It can result in
		// records in a DeltaTree being outside its decode boundary range, which isn't actually invalid
		// though it is awkward to reason about.
		state bool tryToUpdate = btPage->tree().numItems > 0 && update->boundariesNormal();

		// If trying to update the page and the page reference points into the cache,
		// we need to clone it so we don't modify the original version of the page.
		// TODO: Refactor DeltaTree::Mirror so it can be shared between different versions of pages
		if (tryToUpdate && fromCache) {
			page = self->cloneForUpdate(page);
			btPage = (BTreePage*)page->begin();
			fromCache = false;
		}

		debug_printf(
		    "%s commitSubtree(): %s\n",
		    context.c_str(),
		    btPage->toString(false, rootID, snapshot->getVersion(), update->decodeLowerBound, update->decodeUpperBound)
		        .c_str());

		state BTreePage::BinaryTree::Cursor cursor = getCursor(page);

		if (REDWOOD_DEBUG) {
			debug_printf("%s ---------MUTATION BUFFER SLICE ---------------------\n", context.c_str());
			auto begin = mBegin;
			while (1) {
				debug_printf("%s Mutation: '%s':  %s\n",
				             context.c_str(),
				             printable(begin.key()).c_str(),
				             begin.mutation().toString().c_str());
				if (begin == mEnd) {
					break;
				}
				++begin;
			}
			debug_printf("%s -------------------------------------\n", context.c_str());
		}

		// Leaf Page
		if (isLeaf) {
			bool updating = tryToUpdate;
			bool changesMade = false;

			// Couldn't make changes in place, so now do a linear merge and build new pages.
			state Standalone<VectorRef<RedwoodRecordRef>> merged;

			auto switchToLinearMerge = [&]() {
				updating = false;
				auto c = cursor;
				c.moveFirst();
				while (c != cursor) {
					debug_printf("%s catch-up adding %s\n", context.c_str(), c.get().toString().c_str());
					merged.push_back(merged.arena(), c.get());
					c.moveNext();
				}
			};

			// The first mutation buffer boundary has a key <= the first key in the page.

			cursor.moveFirst();
			debug_printf("%s Leaf page, applying changes.\n", context.c_str());

			// Now, process each mutation range and merge changes with existing data.
			bool firstMutationBoundary = true;
			while (mBegin != mEnd) {
				debug_printf("%s New mutation boundary: '%s': %s\n",
				             context.c_str(),
				             printable(mBegin.key()).c_str(),
				             mBegin.mutation().toString().c_str());

				// Apply the change to the mutation buffer start boundary key only if
				//   - there actually is a change (whether a set or a clear, old records are to be removed)
				//   - either this is not the first boundary or it is but its key matches our lower bound key
				bool applyBoundaryChange = mBegin.mutation().boundaryChanged &&
				                           (!firstMutationBoundary || mBegin.key() == update->subtreeLowerBound->key);
				firstMutationBoundary = false;

				// Iterate over records for the mutation boundary key, keep them unless the boundary key was changed or
				// we are not applying it
				while (cursor.valid() && cursor.get().key == mBegin.key()) {
					// If there were no changes to the key or we're not applying it
					if (!applyBoundaryChange) {
						// If not updating, add to the output set, otherwise skip ahead past the records for the
						// mutation boundary
						if (!updating) {
							merged.push_back(merged.arena(), cursor.get());
							debug_printf("%s Added %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());
						}
						cursor.moveNext();
					} else {
						changesMade = true;
						// If updating, erase from the page, otherwise do not add to the output set
						if (updating) {
							debug_printf("%s Erasing %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());
							btPage->kvBytes -= cursor.get().kvBytes();
							cursor.erase();
						} else {
							debug_printf("%s Skipped %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());
							cursor.moveNext();
						}
					}
				}

				constexpr int maxHeightAllowed = 8;

				// Write the new record(s) for the mutation boundary start key if its value has been set
				// Clears of this key will have been processed above by not being erased from the updated page or
				// excluded from the merge output
				if (applyBoundaryChange && mBegin.mutation().boundarySet()) {
					RedwoodRecordRef rec(mBegin.key(), 0, mBegin.mutation().boundaryValue.get());
					changesMade = true;

					// If updating, add to the page, else add to the output set
					if (updating) {
						if (cursor.mirror->insert(rec, update->skipLen, maxHeightAllowed)) {
							btPage->kvBytes += rec.kvBytes();
							debug_printf(
							    "%s Inserted %s [mutation, boundary start]\n", context.c_str(), rec.toString().c_str());
						} else {
							debug_printf("%s Insert failed for %s [mutation, boundary start]\n",
							             context.c_str(),
							             rec.toString().c_str());
							switchToLinearMerge();
						}
					}

					if (!updating) {
						merged.push_back(merged.arena(), rec);
						debug_printf(
						    "%s Added %s [mutation, boundary start]\n", context.c_str(), rec.toString().c_str());
					}
				}

				// Before advancing the iterator, get whether or not the records in the following range must be removed
				bool remove = mBegin.mutation().clearAfterBoundary;
				// Advance to the next boundary because we need to know the end key for the current range.
				++mBegin;
				if (mBegin == mEnd) {
					update->skipLen = 0;
				}

				debug_printf("%s Mutation range end: '%s'\n", context.c_str(), printable(mBegin.key()).c_str());

				// Now handle the records up through but not including the next mutation boundary key
				RedwoodRecordRef end(mBegin.key());

				// If the records are being removed and we're not doing an in-place update
				// OR if we ARE doing an update but the records are NOT being removed, then just skip them.
				if (remove != updating) {
					// If not updating, then the records, if any exist, are being removed.  We don't know if there
					// actually are any but we must assume there are.
					if (!updating) {
						changesMade = true;
					}

					debug_printf("%s Seeking forward to next boundary (remove=%d updating=%d) %s\n",
					             context.c_str(),
					             remove,
					             updating,
					             mBegin.key().toString().c_str());
					cursor.seekGreaterThanOrEqual(end, update->skipLen);
				} else {
					// Otherwise we must visit the records.  If updating, the visit is to erase them, and if doing a
					// linear merge than the visit is to add them to the output set.
					while (cursor.valid() && cursor.get().compare(end, update->skipLen) < 0) {
						if (updating) {
							debug_printf("%s Erasing %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());
							btPage->kvBytes -= cursor.get().kvBytes();
							cursor.erase();
							changesMade = true;
						} else {
							merged.push_back(merged.arena(), cursor.get());
							debug_printf(
							    "%s Added %s [existing, middle]\n", context.c_str(), merged.back().toString().c_str());
							cursor.moveNext();
						}
					}
				}
			}

			// If there are still more records, they have the same key as the end boundary
			if (cursor.valid()) {
				// If the end boundary is changing, we must remove the remaining records in this page
				bool remove = mEnd.mutation().boundaryChanged;
				if (remove) {
					changesMade = true;
				}

				// If we don't have to remove the records and we are updating, do nothing.
				// If we do have to remove the records and we are not updating, do nothing.
				if (remove != updating) {
					debug_printf(
					    "%s Ignoring remaining records, remove=%d updating=%d\n", context.c_str(), remove, updating);
				} else {
					// If updating and the key is changing, we must visit the records to erase them.
					// If not updating and the key is not changing, we must visit the records to add them to the output
					// set.
					while (cursor.valid()) {
						if (updating) {
							debug_printf(
							    "%s Erasing %s and beyond [existing, matches changed upper mutation boundary]\n",
							    context.c_str(),
							    cursor.get().toString().c_str());
							btPage->kvBytes -= cursor.get().kvBytes();
							cursor.erase();
						} else {
							merged.push_back(merged.arena(), cursor.get());
							debug_printf(
							    "%s Added %s [existing, tail]\n", context.c_str(), merged.back().toString().c_str());
							cursor.moveNext();
						}
					}
				}
			} else {
				debug_printf("%s No records matching mutation buffer end boundary key\n", context.c_str());
			}

			// No changes were actually made.  This could happen if the only mutations are clear ranges which do not
			// match any records.
			if (!changesMade) {
				debug_printf("%s No changes were made during mutation merge, returning %s\n",
				             context.c_str(),
				             toString(*update).c_str());
				return Void();
			} else {
				debug_printf(
				    "%s Changes were made, writing, but subtree may still be unchanged from parent's perspective.\n",
				    context.c_str());
			}

			writeVersion = self->getLastCommittedVersion() + 1;

			if (updating) {
				const BTreePage::BinaryTree& deltaTree = btPage->tree();
				// If the tree is now empty, delete the page
				if (deltaTree.numItems == 0) {
					update->cleared();
					self->freeBTreePage(rootID, writeVersion);
					debug_printf("%s Page updates cleared all entries, returning %s\n",
					             context.c_str(),
					             toString(*update).c_str());
				} else {
					// Otherwise update it.
					BTreePageIDRef newID = wait(self->updateBTreePage(
					    self, rootID, &update->newLinks.arena(), page.castTo<ArenaPage>(), writeVersion));

					update->updatedInPlace(newID, btPage, newID.size() * self->m_blockSize);
					debug_printf(
					    "%s Page updated in-place, returning %s\n", context.c_str(), toString(*update).c_str());
				}
				return Void();
			}

			// If everything in the page was deleted then this page should be deleted as of the new version
			if (merged.empty()) {
				update->cleared();
				self->freeBTreePage(rootID, writeVersion);

				debug_printf("%s All leaf page contents were cleared, returning %s\n",
				             context.c_str(),
				             toString(*update).c_str());
				return Void();
			}

			// Rebuild new page(s).
			state Standalone<VectorRef<RedwoodRecordRef>> entries = wait(writePages(self,
			                                                                        update->subtreeLowerBound,
			                                                                        update->subtreeUpperBound,
			                                                                        merged,
			                                                                        btPage->height,
			                                                                        writeVersion,
			                                                                        rootID));

			// Put new links into update and tell update that pages were rebuilt
			update->rebuilt(entries);

			debug_printf("%s Merge complete, returning %s\n", context.c_str(), toString(*update).c_str());
			return Void();
		} else {
			// Internal Page
			std::vector<Future<Void>> recursions;
			state std::vector<InternalPageSliceUpdate*> slices;
			state Arena arena;

			cursor.moveFirst();

			bool first = true;

			while (cursor.valid()) {
				InternalPageSliceUpdate& u = *new (arena) InternalPageSliceUpdate();
				slices.push_back(&u);

				// At this point we should never be at a null child page entry because the first entry of a page
				// can't be null and this loop will skip over null entries that come after non-null entries.
				ASSERT(cursor.get().value.present());

				// Subtree lower boundary is this page's subtree lower bound or cursor
				u.cBegin = cursor;
				u.decodeLowerBound = &cursor.get();
				if (first) {
					u.subtreeLowerBound = update->subtreeLowerBound;
					first = false;
					// mbegin is already the first mutation that could affect this subtree described by update
				} else {
					u.subtreeLowerBound = u.decodeLowerBound;
					mBegin = mEnd;
					// mBegin is either at or greater than subtreeLowerBound->key, which was the subtreeUpperBound->key
					// for the previous subtree slice.  But we need it to be at or *before* subtreeLowerBound->key
					// so if mBegin.key() is not exactly the subtree lower bound key then decrement it.
					if (mBegin.key() != u.subtreeLowerBound->key) {
						--mBegin;
					}
				}

				BTreePageIDRef pageID = cursor.get().getChildPage();
				ASSERT(!pageID.empty());

				// The decode upper bound is always the next key after the child link, or the decode upper bound for
				// this page
				if (cursor.moveNext()) {
					u.decodeUpperBound = &cursor.get();
					// If cursor record has a null child page then it exists only to preserve a previous
					// subtree boundary that is now needed for reading the subtree at cBegin.
					if (!cursor.get().value.present()) {
						// If the upper bound is provided by a dummy record in [cBegin, cEnd) then there is no
						// requirement on the next subtree range or the parent page to have a specific upper boundary
						// for decoding the subtree.
						u.expectedUpperBound = nullptr;
						cursor.moveNext();
						// If there is another record after the null child record, it must have a child page value
						ASSERT(!cursor.valid() || cursor.get().value.present());
					} else {
						u.expectedUpperBound = u.decodeUpperBound;
					}
				} else {
					u.decodeUpperBound = update->decodeUpperBound;
					u.expectedUpperBound = update->decodeUpperBound;
				}
				u.subtreeUpperBound = cursor.valid() ? &cursor.get() : update->subtreeUpperBound;
				u.cEnd = cursor;
				u.skipLen = 0; // TODO: set this

				// Find the mutation buffer range that includes all changes to the range described by u
				mEnd = mutationBuffer->lower_bound(u.subtreeUpperBound->key);

				// If the mutation range described by mBegin extends to mEnd, then see if the part of that range
				// that overlaps with u's subtree range is being fully cleared or fully unchanged.
				auto next = mBegin;
				++next;
				if (next == mEnd) {
					// Check for uniform clearedness or unchangedness for the range mutation where it overlaps u's
					// subtree
					const KeyRef& mutationBoundaryKey = mBegin.key();
					const RangeMutation& range = mBegin.mutation();
					bool uniform;
					if (range.clearAfterBoundary) {
						// If the mutation range after the boundary key is cleared, then the mutation boundary key must
						// be cleared or must be different than the subtree lower bound key so that it doesn't matter
						uniform = range.boundaryCleared() || mutationBoundaryKey != u.subtreeLowerBound->key;
					} else {
						// If the mutation range after the boundary key is unchanged, then the mutation boundary key
						// must be also unchanged or must be different than the subtree lower bound key so that it
						// doesn't matter
						uniform = !range.boundaryChanged || mutationBoundaryKey != u.subtreeLowerBound->key;
					}

					// If u's subtree is either all cleared or all unchanged
					if (uniform) {
						// We do not need to recurse to this subtree.  Next, let's see if we can embiggen u's range to
						// include sibling subtrees also covered by (mBegin, mEnd) so we can not recurse to those, too.
						// If the cursor is valid, u.subtreeUpperBound is the cursor's position, which is >= mEnd.key().
						// If equal, no range expansion is possible.
						if (cursor.valid() && mEnd.key() != u.subtreeUpperBound->key) {
							cursor.seekLessThanOrEqual(mEnd.key(), update->skipLen, &cursor, 1);

							// If this seek moved us ahead, to something other than cEnd, then update subtree range
							// boundaries
							if (cursor != u.cEnd) {
								// If the cursor is at a record with a null child, back up one step because it is in the
								// middle of the next logical subtree, as null child records are not subtree boundaries.
								ASSERT(cursor.valid());
								if (!cursor.get().value.present()) {
									cursor.movePrev();
								}

								u.cEnd = cursor;
								u.subtreeUpperBound = &cursor.get();
								u.skipLen = 0; // TODO: set this

								// The new decode upper bound is either cEnd or the record before it if it has no child
								// link
								auto c = u.cEnd;
								c.movePrev();
								ASSERT(c.valid());
								if (!c.get().value.present()) {
									u.decodeUpperBound = &c.get();
									u.expectedUpperBound = nullptr;
								} else {
									u.decodeUpperBound = u.subtreeUpperBound;
									u.expectedUpperBound = u.subtreeUpperBound;
								}
							}
						}

						// The subtree range is either fully cleared or unchanged.
						if (range.clearAfterBoundary) {
							// Cleared
							u.cleared();
							auto c = u.cBegin;
							while (c != u.cEnd) {
								const RedwoodRecordRef& rec = c.get();
								if (rec.value.present()) {
									if (btPage->height == 2) {
										debug_printf("%s: freeing child page in cleared subtree range: %s\n",
										             context.c_str(),
										             ::toString(rec.getChildPage()).c_str());
										self->freeBTreePage(rec.getChildPage(), writeVersion);
									} else {
										debug_printf("%s: queuing subtree deletion cleared subtree range: %s\n",
										             context.c_str(),
										             ::toString(rec.getChildPage()).c_str());
										self->m_lazyClearQueue.pushFront(
										    LazyClearQueueEntry{ writeVersion, rec.getChildPage() });
									}
								}
								c.moveNext();
							}
						} else {
							// Subtree range unchanged
						}

						debug_printf("%s: MutationBuffer covers this range in a single mutation, not recursing: %s\n",
						             context.c_str(),
						             u.toString().c_str());

						// u has already been initialized with the correct result, no recursion needed, so restart the
						// loop.
						continue;
					}
				}

				// If this page has height of 2 then its children are leaf nodes
				recursions.push_back(
				    self->commitSubtree(self, snapshot, mutationBuffer, pageID, btPage->height == 2, mBegin, mEnd, &u));
			}

			debug_printf(
			    "%s Recursions from internal page started. pageSize=%d level=%d children=%d slices=%d recursions=%d\n",
			    context.c_str(),
			    btPage->size(),
			    btPage->height,
			    btPage->tree().numItems,
			    slices.size(),
			    recursions.size());

			wait(waitForAll(recursions));
			debug_printf("%s Recursions done, processing slice updates.\n", context.c_str());

			// Note:  parentInfo could be invalid after a wait and must be re-initialized.
			// All uses below occur before waits so no reinitialization is done.
			state ParentInfo* parentInfo = &self->childUpdateTracker[rootID.front()];
			state InternalPageModifier m(btPage, cursor.mirror, tryToUpdate, parentInfo);

			// Apply the possible changes for each subtree range recursed to, except the last one.
			// For each range, the expected next record, if any, is checked against the first boundary
			// of the next range, if any.
			for (int i = 0, iEnd = slices.size() - 1; i < iEnd; ++i) {
				m.applyUpdate(*slices[i], slices[i + 1]->getFirstBoundary());
			}

			// The expected next record for the final range is checked against one of the upper boundaries passed to
			// this commitSubtree() instance.  If changes have already been made, then the subtree upper boundary is
			// passed, so in the event a different upper boundary is needed it will be added to the already-modified
			// page.  Otherwise, the decode boundary is used which will prevent this page from being modified for the
			// sole purpose of adding a dummy upper bound record.
			debug_printf("%s Applying final child range update. changesMade=%d  Parent update is: %s\n",
			             context.c_str(),
			             m.changesMade,
			             update->toString().c_str());
			m.applyUpdate(*slices.back(), m.changesMade ? update->subtreeUpperBound : update->decodeUpperBound);

			state bool detachChildren = (parentInfo->count > 2);
			state bool forceUpdate = false;

			// If no changes were made, but we should rewrite it to point directly to remapped child pages
			if (!m.changesMade && detachChildren) {
				debug_printf(
				    "%s Internal page forced rewrite because at least %d children have been updated in-place.\n",
				    context.c_str(),
				    parentInfo->count);
				forceUpdate = true;
				if (!m.updating) {
					m.updating = true;

					// Copy the page before modification if the page references the cache
					if (fromCache) {
						page = self->cloneForUpdate(page);
						cursor = getCursor(page);
						btPage = (BTreePage*)page->begin();
						m.btPage = btPage;
						m.m = cursor.mirror;
						fromCache = false;
					}
				}
				++g_redwoodMetrics.level(btPage->height).forceUpdate;
			}

			// If page contents have changed
			if (m.changesMade || forceUpdate) {
				if (m.empty()) {
					update->cleared();
					debug_printf("%s All internal page children were deleted so deleting this page too, returning %s\n",
					             context.c_str(),
					             toString(*update).c_str());
					self->freeBTreePage(rootID, writeVersion);
					self->childUpdateTracker.erase(rootID.front());
				} else {
					if (m.updating) {
						// Page was updated in place (or being forced to be updated in place to update child page ids)
						debug_printf(
						    "%s Internal page modified in-place tryToUpdate=%d forceUpdate=%d detachChildren=%d\n",
						    context.c_str(),
						    tryToUpdate,
						    forceUpdate,
						    detachChildren);

						if (detachChildren) {
							int detached = 0;
							cursor.moveFirst();
							auto& stats = g_redwoodMetrics.level(btPage->height);
							while (cursor.valid()) {
								if (cursor.get().value.present()) {
									for (auto& p : cursor.get().getChildPage()) {
										if (parentInfo->maybeUpdated(p)) {
											LogicalPageID newID = self->m_pager->detachRemappedPage(p, writeVersion);
											if (newID != invalidLogicalPageID) {
												debug_printf("%s Detach updated %u -> %u\n", context.c_str(), p, newID);
												p = newID;
												++stats.detachChild;
												++detached;
											}
										}
									}
								}
								cursor.moveNext();
							}
							parentInfo->clear();
							if (forceUpdate && detached == 0) {
								debug_printf("%s No children detached during forced update, returning %s\n",
								             context.c_str(),
								             toString(*update).c_str());
								return Void();
							}
						}

						BTreePageIDRef newID = wait(self->updateBTreePage(
						    self, rootID, &update->newLinks.arena(), page.castTo<ArenaPage>(), writeVersion));
						debug_printf(
						    "%s commitSubtree(): Internal page updated in-place at version %s, new contents: %s\n",
						    context.c_str(),
						    toString(writeVersion).c_str(),
						    btPage
						        ->toString(false,
						                   newID,
						                   snapshot->getVersion(),
						                   update->decodeLowerBound,
						                   update->decodeUpperBound)
						        .c_str());

						update->updatedInPlace(newID, btPage, newID.size() * self->m_blockSize);
						debug_printf("%s Internal page updated in-place, returning %s\n",
						             context.c_str(),
						             toString(*update).c_str());
					} else {
						// Page was rebuilt, possibly split.
						debug_printf("%s Internal page could not be modified, rebuilding replacement(s).\n",
						             context.c_str());

						if (detachChildren) {
							auto& stats = g_redwoodMetrics.level(btPage->height);
							for (auto& rec : m.rebuild) {
								if (rec.value.present()) {
									BTreePageIDRef oldPages = rec.getChildPage();
									BTreePageIDRef newPages;
									for (int i = 0; i < oldPages.size(); ++i) {
										LogicalPageID p = oldPages[i];
										if (parentInfo->maybeUpdated(p)) {
											LogicalPageID newID = self->m_pager->detachRemappedPage(p, writeVersion);
											if (newID != invalidLogicalPageID) {
												// Rebuild record values reference original page memory so make a copy
												if (newPages.empty()) {
													newPages = BTreePageIDRef(m.rebuild.arena(), oldPages);
													rec.setChildPage(newPages);
												}
												debug_printf("%s Detach updated %u -> %u\n", context.c_str(), p, newID);
												newPages[i] = newID;
												++stats.detachChild;
											}
										}
									}
								}
							}
							parentInfo->clear();
						}

						Standalone<VectorRef<RedwoodRecordRef>> newChildEntries =
						    wait(writePages(self,
						                    update->subtreeLowerBound,
						                    update->subtreeUpperBound,
						                    m.rebuild,
						                    btPage->height,
						                    writeVersion,
						                    rootID));
						update->rebuilt(newChildEntries);

						debug_printf(
						    "%s Internal page rebuilt, returning %s\n", context.c_str(), toString(*update).c_str());
					}
				}
			} else {
				debug_printf("%s Page has no changes, returning %s\n", context.c_str(), toString(*update).c_str());
			}
			return Void();
		}
	}

	ACTOR static Future<Void> commit_impl(VersionedBTree* self) {
		state MutationBuffer* mutations = self->m_pBuffer;

		// No more mutations are allowed to be written to this mutation buffer we will commit
		// at m_writeVersion, which we must save locally because it could change during commit.
		self->m_pBuffer = nullptr;
		state Version writeVersion = self->m_writeVersion;

		// The latest mutation buffer start version is the one we will now (or eventually) commit.
		state Version mutationBufferStartVersion = self->m_mutationBuffers.rbegin()->first;

		// Replace the lastCommit future with a new one and then wait on the old one
		state Promise<Void> committed;
		Future<Void> previousCommit = self->m_latestCommit;
		self->m_latestCommit = committed.getFuture();

		// Wait for the latest commit to be finished.
		wait(previousCommit);

		self->m_pager->setOldestVersion(self->m_newOldestVersion);
		debug_printf("%s: Beginning commit of version %" PRId64 ", new oldest version set to %" PRId64 "\n",
		             self->m_name.c_str(),
		             writeVersion,
		             self->m_newOldestVersion);

		// Get the latest version from the pager, which is what we will read at
		state Version latestVersion = self->m_pager->getLatestVersion();
		debug_printf("%s: pager latestVersion %" PRId64 "\n", self->m_name.c_str(), latestVersion);

		state Standalone<BTreePageIDRef> rootPageID = self->m_header.root.get();
		state InternalPageSliceUpdate all;
		state RedwoodRecordRef rootLink = dbBegin.withPageID(rootPageID);
		all.subtreeLowerBound = &rootLink;
		all.decodeLowerBound = &rootLink;
		all.subtreeUpperBound = &dbEnd;
		all.decodeUpperBound = &dbEnd;
		all.skipLen = 0;

		MutationBuffer::const_iterator mBegin = mutations->upper_bound(all.subtreeLowerBound->key);
		--mBegin;
		MutationBuffer::const_iterator mEnd = mutations->lower_bound(all.subtreeUpperBound->key);

		wait(commitSubtree(self,
		                   self->m_pager->getReadSnapshot(latestVersion),
		                   mutations,
		                   rootPageID,
		                   self->m_header.height == 1,
		                   mBegin,
		                   mEnd,
		                   &all));

		// If the old root was deleted, write a new empty tree root node and free the old roots
		if (all.childrenChanged) {
			if (all.newLinks.empty()) {
				debug_printf("Writing new empty root.\n");
				LogicalPageID newRootID = wait(self->m_pager->newPageID());
				Reference<ArenaPage> page = self->m_pager->newPageBuffer();
				makeEmptyRoot(page);
				self->m_header.height = 1;
				self->m_pager->updatePage(newRootID, page);
				rootPageID = BTreePageIDRef((LogicalPageID*)&newRootID, 1);
			} else {
				Standalone<VectorRef<RedwoodRecordRef>> newRootLevel(all.newLinks, all.newLinks.arena());
				if (newRootLevel.size() == 1) {
					rootPageID = newRootLevel.front().getChildPage();
				} else {
					// If the new root level's size is not 1 then build new root level(s)
					Standalone<VectorRef<RedwoodRecordRef>> newRootPage =
					    wait(buildNewRoot(self, latestVersion, newRootLevel, self->m_header.height));
					rootPageID = newRootPage.front().getChildPage();
				}
			}
		}

		self->m_header.root.set(rootPageID, sizeof(headerSpace) - sizeof(m_header));

		self->m_lazyClearStop = true;
		wait(success(self->m_lazyClearActor));
		debug_printf("Lazy delete freed %u pages\n", self->m_lazyClearActor.get());

		self->m_pager->setCommitVersion(writeVersion);

		wait(self->m_lazyClearQueue.flush());
		self->m_header.lazyDeleteQueue = self->m_lazyClearQueue.getState();

		debug_printf("Setting metakey\n");
		self->m_pager->setMetaKey(self->m_header.asKeyRef());

		debug_printf("%s: Committing pager %" PRId64 "\n", self->m_name.c_str(), writeVersion);
		wait(self->m_pager->commit());
		debug_printf("%s: Committed version %" PRId64 "\n", self->m_name.c_str(), writeVersion);

		// Now that everything is committed we must delete the mutation buffer.
		// Our buffer's start version should be the oldest mutation buffer version in the map.
		ASSERT(mutationBufferStartVersion == self->m_mutationBuffers.begin()->first);
		self->m_mutationBuffers.erase(self->m_mutationBuffers.begin());

		self->m_lastCommittedVersion = writeVersion;
		++g_redwoodMetrics.opCommit;
		self->m_lazyClearActor = incrementalLazyClear(self);

		committed.send(Void());
		return Void();
	}

public:
	// InternalCursor is for seeking to and iterating over the leaf-level RedwoodRecordRef records in the tree.
	// The records could represent multiple values for the same key at different versions, including a non-present value
	// representing a clear. Currently, however, all records are at version 0 and no clears are present in the tree.
	struct InternalCursor {
	private:
		// Each InternalCursor's position is represented by a reference counted PageCursor, which links
		// to its parent PageCursor, up to a PageCursor representing a cursor on the root page.
		// PageCursors can be shared by many InternalCursors, making InternalCursor copying low overhead
		struct PageCursor : ReferenceCounted<PageCursor>, FastAllocated<PageCursor> {
			Reference<PageCursor> parent;
			BTreePageIDRef pageID; // Only needed for debugging purposes
			Reference<const ArenaPage> page;
			BTreePage::BinaryTree::Cursor cursor;

			// id will normally reference memory owned by the parent, which is okay because a reference to the parent
			// will be held in the cursor
			PageCursor(BTreePageIDRef id, Reference<const ArenaPage> page, Reference<PageCursor> parent = {})
			  : pageID(id), page(page), parent(parent), cursor(getCursor(page)) {}

			PageCursor(const PageCursor& toCopy)
			  : parent(toCopy.parent), pageID(toCopy.pageID), page(toCopy.page), cursor(toCopy.cursor) {}

			// Convenience method for copying a PageCursor
			Reference<PageCursor> copy() const { return makeReference<PageCursor>(*this); }

			const BTreePage* btPage() const { return (const BTreePage*)page->begin(); }

			bool isLeaf() const { return btPage()->isLeaf(); }

			Future<Reference<PageCursor>> getChild(Reference<IPagerSnapshot> pager, int readAheadBytes = 0) {
				ASSERT(!isLeaf());
				BTreePage::BinaryTree::Cursor next = cursor;
				next.moveNext();
				const RedwoodRecordRef& rec = cursor.get();
				BTreePageIDRef id = rec.getChildPage();
				Future<Reference<const ArenaPage>> child = readPage(pager, id, &rec, &next.getOrUpperBound());

				// Read ahead siblings at level 2
				// TODO:  Application of readAheadBytes is not taking into account the size of the current page or any
				// of the adjacent pages it is preloading.
				if (readAheadBytes > 0 && btPage()->height == 2 && next.valid()) {
					do {
						debug_printf("preloading %s %d bytes left\n",
						             ::toString(next.get().getChildPage()).c_str(),
						             readAheadBytes);
						// If any part of the page was already loaded then stop
						if (next.get().value.present()) {
							preLoadPage(pager.getPtr(), next.get().getChildPage());
							readAheadBytes -= page->size();
						}
					} while (readAheadBytes > 0 && next.moveNext());
				}

				return map(child, [=](Reference<const ArenaPage> page) {
					return makeReference<PageCursor>(id, page, Reference<PageCursor>::addRef(this));
				});
			}

			std::string toString() const {
				return format("%s, %s",
				              ::toString(pageID).c_str(),
				              cursor.valid() ? cursor.get().toString(isLeaf()).c_str() : "<invalid>");
			}
		};

		Standalone<BTreePageIDRef> rootPageID;
		Reference<IPagerSnapshot> pager;
		Reference<PageCursor> pageCursor;

	public:
		InternalCursor() {}

		InternalCursor(Reference<IPagerSnapshot> pager, BTreePageIDRef root) : pager(pager), rootPageID(root) {}

		std::string toString() const {
			std::string r;

			Reference<PageCursor> c = pageCursor;
			int maxDepth = 0;
			while (c) {
				c = c->parent;
				++maxDepth;
			}

			c = pageCursor;
			int depth = maxDepth;
			while (c) {
				r = format("[%d/%d: %s] ", depth--, maxDepth, c->toString().c_str()) + r;
				c = c->parent;
			}
			return r;
		}

		// Returns true if cursor position is a valid leaf page record
		bool valid() const { return pageCursor && pageCursor->isLeaf() && pageCursor->cursor.valid(); }

		// Returns true if cursor position is valid() and has a present record value
		bool present() const { return valid() && pageCursor->cursor.get().value.present(); }

		// Returns true if cursor position is present() and has an effective version <= v
		bool presentAtVersion(Version v) { return present() && pageCursor->cursor.get().version <= v; }

		// This is to enable an optimization for the case where all internal records are at the
		// same version and there are no implicit clears
		// *this MUST be valid()
		bool presentAtExactVersion(Version v) const { return present() && pageCursor->cursor.get().version == v; }

		// Returns true if cursor position is present() and has an effective version <= v
		bool validAtVersion(Version v) { return valid() && pageCursor->cursor.get().version <= v; }

		const RedwoodRecordRef& get() const { return pageCursor->cursor.get(); }

		// Ensure that pageCursor is not shared with other cursors so we can modify it
		void ensureUnshared() {
			if (!pageCursor->isSoleOwner()) {
				pageCursor = pageCursor->copy();
			}
		}

		Future<Void> moveToRoot() {
			// If pageCursor exists follow parent links to the root
			if (pageCursor) {
				while (pageCursor->parent) {
					pageCursor = pageCursor->parent;
				}
				return Void();
			}

			// Otherwise read the root page
			Future<Reference<const ArenaPage>> root = readPage(pager, rootPageID, &dbBegin, &dbEnd);
			return map(root, [=](Reference<const ArenaPage> p) {
				pageCursor = makeReference<PageCursor>(rootPageID, p);
				return Void();
			});
		}

		ACTOR Future<bool> seekLessThan_impl(InternalCursor* self, RedwoodRecordRef query, int prefetchBytes) {
			Future<Void> f = self->moveToRoot();
			// f will almost always be ready
			if (!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();
			loop {
				bool isLeaf = self->pageCursor->isLeaf();
				bool success = self->pageCursor->cursor.seekLessThan(query);

				// Skip backwards over internal page entries that do not link to child pages
				if (!isLeaf) {
					// While record has no value, move again
					while (success && !self->pageCursor->cursor.get().value.present()) {
						success = self->pageCursor->cursor.movePrev();
					}
				}

				if (success) {
					// If we found a record < query at a leaf page then return success
					if (isLeaf) {
						return true;
					}

					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, prefetchBytes));
					self->pageCursor = child;
				} else {
					// No records < query on this page, so move to immediate previous record at leaf level
					bool success = wait(self->move(false));
					return success;
				}
			}
		}

		Future<bool> seekLessThan(RedwoodRecordRef query, int prefetchBytes) {
			return seekLessThan_impl(this, query, prefetchBytes);
		}

		ACTOR Future<bool> move_impl(InternalCursor* self, bool forward) {
			// Try to move pageCursor, if it fails to go parent, repeat until it works or root cursor can't be moved
			while (1) {
				self->ensureUnshared();
				bool success = self->pageCursor->cursor.valid() &&
				               (forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev());

				// Skip over internal page entries that do not link to child pages
				if (!self->pageCursor->isLeaf()) {
					// While record has no value, move again
					while (success && !self->pageCursor->cursor.get().value.present()) {
						success = forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					}
				}

				// Stop if successful or there's no parent to move to
				if (success || !self->pageCursor->parent) {
					break;
				}

				// Move to parent
				self->pageCursor = self->pageCursor->parent;
			}

			// If pageCursor not valid we've reached an end of the tree
			if (!self->pageCursor->cursor.valid()) {
				return false;
			}

			// While not on a leaf page, move down to get to one.
			while (!self->pageCursor->isLeaf()) {
				// Skip over internal page entries that do not link to child pages
				while (!self->pageCursor->cursor.get().value.present()) {
					bool success = forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					if (!success) {
						return false;
					}
				}

				Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager));
				forward ? child->cursor.moveFirst() : child->cursor.moveLast();
				self->pageCursor = child;
			}

			return true;
		}

		Future<bool> move(bool forward) { return move_impl(this, forward); }

		// Move to the first or last record of the database.
		ACTOR Future<bool> move_end(InternalCursor* self, bool begin) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if (!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				// Move to first or last record in the page
				bool success = begin ? self->pageCursor->cursor.moveFirst() : self->pageCursor->cursor.moveLast();

				// Skip over internal page entries that do not link to child pages
				if (!self->pageCursor->isLeaf()) {
					// While record has no value, move past it
					while (success && !self->pageCursor->cursor.get().value.present()) {
						success = begin ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					}
				}

				// If it worked, return true if we've reached a leaf page otherwise go to the next child
				if (success) {
					if (self->pageCursor->isLeaf()) {
						return true;
					}

					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager));
					self->pageCursor = child;
				} else {
					return false;
				}
			}
		}

		Future<bool> moveFirst() { return move_end(this, true); }
		Future<bool> moveLast() { return move_end(this, false); }
	};

	// Cursor designed for short lifespans.
	// Holds references to all pages touched.
	// All record references returned from it are valid until the cursor is destroyed.
	class BTreeCursor {
	public:
		struct PathEntry {
			Reference<const ArenaPage> page;
			BTreePage::BinaryTree::Cursor cursor;

			const BTreePage* btPage() const { return (BTreePage*)page->begin(); };
		};

	private:
		VersionedBTree* btree;
		Reference<IPagerSnapshot> pager;
		bool valid;
		std::vector<PathEntry> path;

	public:
		BTreeCursor() {}

		bool isValid() const { return valid; }

		std::string toString() const {
			std::string r = format("{ptr=%p %s ", this, ::toString(pager->getVersion()).c_str());
			for (int i = 0; i < path.size(); ++i) {
				r += format("[%d/%d: %s] ",
				            i + 1,
				            path.size(),
				            path[i].cursor.valid() ? path[i].cursor.get().toString(path[i].btPage()->isLeaf()).c_str()
				                                   : "<invalid>");
			}
			if (!valid) {
				r += " (invalid) ";
			}
			r += "}";
			return r;
		}

		const RedwoodRecordRef& get() { return path.back().cursor.get(); }

		bool inRoot() const { return path.size() == 1; }

		// To enable more efficient range scans, caller can read the lowest page
		// of the cursor and pop it.
		PathEntry& back() { return path.back(); }
		void popPath() { path.pop_back(); }

		Future<Void> pushPage(BTreePageIDRef id,
		                      const RedwoodRecordRef& lowerBound,
		                      const RedwoodRecordRef& upperBound) {

			return map(readPage(pager, id, &lowerBound, &upperBound), [this, id](Reference<const ArenaPage> p) {
				path.push_back({ p, getCursor(p) });
				return Void();
			});
		}

		Future<Void> pushPage(BTreePage::BinaryTree::Cursor c) {
			const RedwoodRecordRef& rec = c.get();
			auto next = c;
			next.moveNext();
			BTreePageIDRef id = rec.getChildPage();
			return pushPage(id, rec, next.getOrUpperBound());
		}

		Future<Void> init(VersionedBTree* btree_in, Reference<IPagerSnapshot> pager_in, BTreePageIDRef root) {
			btree = btree_in;
			pager = pager_in;
			path.reserve(6);
			valid = false;
			return pushPage(root, dbBegin, dbEnd);
		}

		// Seeks cursor to query if it exists, the record before or after it, or an undefined and invalid
		// position between those records
		// If 0 is returned, then
		//   If the cursor is valid then it points to query
		//   If the cursor is not valid then the cursor points to some place in the btree such that
		//     If there is a record in the tree < query then movePrev() will move to it, and
		//     If there is a record in the tree > query then moveNext() will move to it.
		// If non-zero is returned then the cursor is valid and the return value is logically equivalent
		// to query.compare(cursor.get())
		ACTOR Future<int> seek_impl(BTreeCursor* self, RedwoodRecordRef query, int prefetchBytes) {
			state RedwoodRecordRef internalPageQuery = query.withMaxPageID();
			self->path.resize(1);
			debug_printf(
			    "seek(%s, %d) start cursor = %s\n", query.toString().c_str(), prefetchBytes, self->toString().c_str());

			loop {
				auto& entry = self->path.back();
				if (entry.btPage()->isLeaf()) {
					int cmp = entry.cursor.seek(query);
					self->valid = entry.cursor.valid() && !entry.cursor.node->isDeleted();
					debug_printf("seek(%s, %d) loop exit cmp=%d cursor=%s\n",
					             query.toString().c_str(),
					             prefetchBytes,
					             cmp,
					             self->toString().c_str());
					return self->valid ? cmp : 0;
				}

				// Internal page, so seek to the branch where query must be
				// Currently, after a subtree deletion internal page boundaries are still strictly adhered
				// to and will be updated if anything is inserted into the cleared range, so if the seek fails
				// or it finds an entry with a null child page then query does not exist in the BTree.
				if (entry.cursor.seekLessThan(internalPageQuery) && entry.cursor.get().value.present()) {
					debug_printf("seek(%s, %d) loop seek success cursor=%s\n",
					             query.toString().c_str(),
					             prefetchBytes,
					             self->toString().c_str());
					Future<Void> f = self->pushPage(entry.cursor);

					// Prefetch siblings, at least prefetchBytes, at level 2 but without jumping to another level 2
					// sibling
					if (prefetchBytes != 0 && entry.btPage()->height == 2) {
						auto c = entry.cursor;
						bool fwd = prefetchBytes > 0;
						prefetchBytes = abs(prefetchBytes);
						// While we should still preload more bytes and a move in the target direction is successful
						while (prefetchBytes > 0 && (fwd ? c.moveNext() : c.movePrev())) {
							// If there is a page link, preload it.
							if (c.get().value.present()) {
								BTreePageIDRef childPage = c.get().getChildPage();
								preLoadPage(self->pager.getPtr(), childPage);
								prefetchBytes -= self->btree->m_blockSize * childPage.size();
							}
						}
					}

					wait(f);
				} else {
					self->valid = false;
					debug_printf("seek(%s, %d) loop exit cmp=0 cursor=%s\n",
					             query.toString().c_str(),
					             prefetchBytes,
					             self->toString().c_str());
					return 0;
				}
			}
		}

		Future<int> seek(RedwoodRecordRef query, int prefetchBytes) { return seek_impl(this, query, prefetchBytes); }

		ACTOR Future<Void> seekGTE_impl(BTreeCursor* self, RedwoodRecordRef query, int prefetchBytes) {
			debug_printf("seekGTE(%s, %d) start\n", query.toString().c_str(), prefetchBytes);
			int cmp = wait(self->seek(query, prefetchBytes));
			if (cmp > 0 || (cmp == 0 && !self->isValid())) {
				wait(self->moveNext());
			}
			return Void();
		}

		Future<Void> seekGTE(RedwoodRecordRef query, int prefetchBytes) {
			return seekGTE_impl(this, query, prefetchBytes);
		}

		ACTOR Future<Void> seekLT_impl(BTreeCursor* self, RedwoodRecordRef query, int prefetchBytes) {
			debug_printf("seekLT(%s, %d) start\n", query.toString().c_str(), prefetchBytes);
			int cmp = wait(self->seek(query, prefetchBytes));
			if (cmp <= 0) {
				wait(self->movePrev());
			}
			return Void();
		}

		Future<Void> seekLT(RedwoodRecordRef query, int prefetchBytes) {
			return seekLT_impl(this, query, -prefetchBytes);
		}

		ACTOR Future<Void> move_impl(BTreeCursor* self, bool forward) {
			// Try to the move cursor at the end of the path in the correct direction
			debug_printf("move%s() start cursor=%s\n", forward ? "Next" : "Prev", self->toString().c_str());
			while (1) {
				debug_printf("move%s() first loop cursor=%s\n", forward ? "Next" : "Prev", self->toString().c_str());
				auto& entry = self->path.back();
				bool success;
				if (entry.cursor.valid()) {
					success = forward ? entry.cursor.moveNext() : entry.cursor.movePrev();
				} else {
					success = forward ? entry.cursor.moveFirst() : false;
				}

				// Skip over internal page entries that do not link to child pages.  There should never be two in a row.
				if (success && !entry.btPage()->isLeaf() && !entry.cursor.get().value.present()) {
					success = forward ? entry.cursor.moveNext() : entry.cursor.movePrev();
					ASSERT(!success || entry.cursor.get().value.present());
				}

				// Stop if successful
				if (success) {
					break;
				}

				if (self->path.size() == 1) {
					self->valid = false;
					return Void();
				}

				// Move to parent
				self->path.pop_back();
			}

			// While not on a leaf page, move down to get to one.
			while (1) {
				debug_printf("move%s() second loop cursor=%s\n", forward ? "Next" : "Prev", self->toString().c_str());
				auto& entry = self->path.back();
				if (entry.btPage()->isLeaf()) {
					break;
				}

				// The last entry in an internal page could be a null link, if so move back
				if (!forward && !entry.cursor.get().value.present()) {
					ASSERT(entry.cursor.movePrev());
					ASSERT(entry.cursor.get().value.present());
				}

				wait(self->pushPage(entry.cursor));
				auto& newEntry = self->path.back();
				ASSERT(forward ? newEntry.cursor.moveFirst() : newEntry.cursor.moveLast());
			}

			self->valid = true;

			debug_printf("move%s() exit cursor=%s\n", forward ? "Next" : "Prev", self->toString().c_str());
			return Void();
		}

		Future<Void> moveNext() { return move_impl(this, true); }
		Future<Void> movePrev() { return move_impl(this, false); }
	};

	Future<Void> initBTreeCursor(BTreeCursor* cursor, Version snapshotVersion) {
		// Only committed versions can be read.
		ASSERT(snapshotVersion <= m_lastCommittedVersion);
		Reference<IPagerSnapshot> snapshot = m_pager->getReadSnapshot(snapshotVersion);

		// This is a ref because snapshot will continue to hold the metakey value memory
		KeyRef m = snapshot->getMetaKey();

		return cursor->init(this, snapshot, ((MetaKey*)m.begin())->root.get());
	}

	// Cursor is for reading and interating over user visible KV pairs at a specific version
	// KeyValueRefs returned become invalid once the cursor is moved
	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor>, public FastAllocated<Cursor>, NonCopyable {
	public:
		Cursor(Reference<IPagerSnapshot> pageSource, BTreePageIDRef root, Version internalRecordVersion)
		  : m_version(internalRecordVersion), m_cur1(pageSource, root), m_cur2(m_cur1) {}

		void addref() override { ReferenceCounted<Cursor>::addref(); }
		void delref() override { ReferenceCounted<Cursor>::delref(); }

	private:
		Version m_version;
		// If kv is valid
		//   - kv.key references memory held by cur1
		//   - If cur1 points to a non split KV pair
		//       - kv.value references memory held by cur1
		//       - cur2 points to the next internal record after cur1
		//     Else
		//       - kv.value references memory in arena
		//       - cur2 points to the first internal record of the split KV pair
		InternalCursor m_cur1;
		InternalCursor m_cur2;
		Arena m_arena;
		Optional<KeyValueRef> m_kv;

	public:
		Future<Void> findEqual(KeyRef key) override { return find_impl(this, key, 0); }
		Future<Void> findFirstEqualOrGreater(KeyRef key, int prefetchBytes) override {
			return find_impl(this, key, 1, prefetchBytes);
		}
		Future<Void> findLastLessOrEqual(KeyRef key, int prefetchBytes) override {
			return find_impl(this, key, -1, prefetchBytes);
		}

		Future<Void> next() override { return move(this, true); }
		Future<Void> prev() override { return move(this, false); }

		bool isValid() override { return m_kv.present(); }

		KeyRef getKey() override { return m_kv.get().key; }

		ValueRef getValue() override { return m_kv.get().value; }

		std::string toString(bool includePaths = true) const {
			std::string r;
			r += format("Cursor(%p) ver: %" PRId64 " ", this, m_version);
			if (m_kv.present()) {
				r += format(
				    "  KV: '%s' -> '%s'", m_kv.get().key.printable().c_str(), m_kv.get().value.printable().c_str());
			} else {
				r += "  KV: <np>";
			}
			if (includePaths) {
				r += format("\n Cur1: %s", m_cur1.toString().c_str());
				r += format("\n Cur2: %s", m_cur2.toString().c_str());
			} else {
				if (m_cur1.valid()) {
					r += format("\n Cur1: %s", m_cur1.get().toString().c_str());
				}
				if (m_cur2.valid()) {
					r += format("\n Cur2: %s", m_cur2.get().toString().c_str());
				}
			}

			return r;
		}

	private:
		// find key in tree closest to or equal to key (at this cursor's version)
		// for less than or equal use cmp < 0
		// for greater than or equal use cmp > 0
		// for equal use cmp == 0
		ACTOR static Future<Void> find_impl(Cursor* self, KeyRef key, int cmp, int prefetchBytes = 0) {
			state RedwoodRecordRef query(key, self->m_version + 1);
			self->m_kv.reset();

			wait(success(self->m_cur1.seekLessThan(query, prefetchBytes)));
			debug_printf("find%sE(%s): %s\n",
			             cmp > 0 ? "GT" : (cmp == 0 ? "" : "LT"),
			             query.toString().c_str(),
			             self->toString().c_str());

			// If we found the target key with a present value then return it as it is valid for any cmp type
			if (self->m_cur1.present() && self->m_cur1.get().key == key) {
				debug_printf("Target key found.  Cursor: %s\n", self->toString().c_str());
				self->m_kv = self->m_cur1.get().toKeyValueRef();
				return Void();
			}

			// If cmp type is Equal and we reached here, we didn't find it
			if (cmp == 0) {
				return Void();
			}

			// cmp mode is GreaterThanOrEqual, so if we've reached here an equal key was not found and cur1 either
			// points to a lesser key or is invalid.
			if (cmp > 0) {
				// If cursor is invalid, query was less than the first key in database so go to the first record
				if (!self->m_cur1.valid()) {
					bool valid = wait(self->m_cur1.moveFirst());
					if (!valid) {
						self->m_kv.reset();
						return Void();
					}
				} else {
					// Otherwise, move forward until we find a key greater than the target key.
					// If multiversion data is present, the next record could have the same key as the initial
					// record found but be at a newer version.
					loop {
						bool valid = wait(self->m_cur1.move(true));
						if (!valid) {
							self->m_kv.reset();
							return Void();
						}

						if (self->m_cur1.get().key > key) {
							break;
						}
					}
				}

				// Get the next present key at the target version.  Handles invalid cursor too.
				wait(self->next());
			} else if (cmp < 0) {
				// cmp mode is LessThanOrEqual.  An equal key to the target key was already checked above, and the
				// search was for LessThan query, so cur1 is already in the right place.
				if (!self->m_cur1.valid()) {
					self->m_kv.reset();
					return Void();
				}

				// Move to previous present kv pair at the target version
				wait(self->prev());
			}

			return Void();
		}

		ACTOR static Future<Void> move(Cursor* self, bool fwd) {
			debug_printf("Cursor::move(%d): Start %s\n", fwd, self->toString().c_str());
			ASSERT(self->m_cur1.valid());

			// If kv is present then the key/version at cur1 was already returned so move to a new key
			// Move cur1 until failure or a new key is found, keeping prior record visited in cur2
			if (self->m_kv.present()) {
				ASSERT(self->m_cur1.valid());
				loop {
					self->m_cur2 = self->m_cur1;
					debug_printf("Cursor::move(%d): Advancing cur1 %s\n", fwd, self->toString().c_str());
					bool valid = wait(self->m_cur1.move(fwd));
					if (!valid || self->m_cur1.get().key != self->m_cur2.get().key) {
						break;
					}
				}
			}

			// Given two consecutive cursors c1 and c2, c1 represents a returnable record if
			//    c1 is present at exactly version v
			//  OR
			//    c1 is.presentAtVersion(v) && (!c2.validAtVersion() || c2.get().key != c1.get().key())
			// Note the distinction between 'present' and 'valid'.  Present means the value for the key
			// exists at the version (but could be the empty string) while valid just means the internal
			// record is in effect at that version but it could indicate that the key was cleared and
			// no longer exists from the user's perspective at that version
			if (self->m_cur1.valid()) {
				self->m_cur2 = self->m_cur1;
				debug_printf("Cursor::move(%d): Advancing cur2 %s\n", fwd, self->toString().c_str());
				wait(success(self->m_cur2.move(true)));
			}

			while (self->m_cur1.valid()) {

				if (self->m_cur1.get().version == self->m_version ||
				    (self->m_cur1.presentAtVersion(self->m_version) &&
				     (!self->m_cur2.validAtVersion(self->m_version) ||
				      self->m_cur2.get().key != self->m_cur1.get().key))) {
					self->m_kv = self->m_cur1.get().toKeyValueRef();
					return Void();
				}

				if (fwd) {
					// Moving forward, move cur2 forward and keep cur1 pointing to the prior (predecessor) record
					debug_printf("Cursor::move(%d): Moving forward %s\n", fwd, self->toString().c_str());
					self->m_cur1 = self->m_cur2;
					wait(success(self->m_cur2.move(true)));
				} else {
					// Moving backward, move cur1 backward and keep cur2 pointing to the prior (successor) record
					debug_printf("Cursor::move(%d): Moving backward %s\n", fwd, self->toString().c_str());
					self->m_cur2 = self->m_cur1;
					wait(success(self->m_cur1.move(false)));
				}
			}

			debug_printf("Cursor::move(%d): Exit, end of db reached.  Cursor = %s\n", fwd, self->toString().c_str());
			self->m_kv.reset();

			return Void();
		}
	};
};

#include "fdbserver/art_impl.h"

RedwoodRecordRef VersionedBTree::dbBegin(LiteralStringRef(""));
RedwoodRecordRef VersionedBTree::dbEnd(LiteralStringRef("\xff\xff\xff\xff\xff"));

class KeyValueStoreRedwoodUnversioned : public IKeyValueStore {
public:
	KeyValueStoreRedwoodUnversioned(std::string filePrefix, UID logID)
	  : m_filePrefix(filePrefix), m_concurrentReads(new FlowLock(SERVER_KNOBS->REDWOOD_KVSTORE_CONCURRENT_READS)) {
		// TODO: This constructor should really just take an IVersionedStore

		int pageSize =
		    BUGGIFY ? deterministicRandom()->randomInt(1000, 4096 * 4) : SERVER_KNOBS->REDWOOD_DEFAULT_PAGE_SIZE;
		int64_t pageCacheBytes =
		    g_network->isSimulated()
		        ? (BUGGIFY ? deterministicRandom()->randomInt(pageSize, FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_4K)
		                   : FLOW_KNOBS->SIM_PAGE_CACHE_4K)
		        : FLOW_KNOBS->PAGE_CACHE_4K;
		Version remapCleanupWindow =
		    BUGGIFY ? deterministicRandom()->randomInt64(0, 1000) : SERVER_KNOBS->REDWOOD_REMAP_CLEANUP_WINDOW;

		IPager2* pager = new DWALPager(pageSize, filePrefix, pageCacheBytes, remapCleanupWindow);
		m_tree = new VersionedBTree(pager, filePrefix);
		m_init = catchError(init_impl(this));
	}

	Future<Void> init() override { return m_init; }

	ACTOR Future<Void> init_impl(KeyValueStoreRedwoodUnversioned* self) {
		TraceEvent(SevInfo, "RedwoodInit").detail("FilePrefix", self->m_filePrefix);
		wait(self->m_tree->init());
		Version v = self->m_tree->getLatestVersion();
		self->m_tree->setWriteVersion(v + 1);
		TraceEvent(SevInfo, "RedwoodInitComplete").detail("FilePrefix", self->m_filePrefix);
		return Void();
	}

	ACTOR void shutdown(KeyValueStoreRedwoodUnversioned* self, bool dispose) {
		TraceEvent(SevInfo, "RedwoodShutdown").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		if (self->m_error.canBeSet()) {
			self->m_error.sendError(actor_cancelled()); // Ideally this should be shutdown_in_progress
		}
		self->m_init.cancel();
		Future<Void> closedFuture = self->m_tree->onClosed();
		if (dispose)
			self->m_tree->dispose();
		else
			self->m_tree->close();
		wait(closedFuture);
		self->m_closed.send(Void());
		TraceEvent(SevInfo, "RedwoodShutdownComplete")
		    .detail("FilePrefix", self->m_filePrefix)
		    .detail("Dispose", dispose);
		delete self;
	}

	void close() override { shutdown(this, false); }

	void dispose() override { shutdown(this, true); }

	Future<Void> onClosed() override { return m_closed.getFuture(); }

	Future<Void> commit(bool sequential = false) override {
		Future<Void> c = m_tree->commit();
		m_tree->setOldestVersion(m_tree->getLatestVersion());
		m_tree->setWriteVersion(m_tree->getWriteVersion() + 1);
		return catchError(c);
	}

	KeyValueStoreType getType() const override { return KeyValueStoreType::SSD_REDWOOD_V1; }

	StorageBytes getStorageBytes() const override { return m_tree->getStorageBytes(); }

	Future<Void> getError() override { return delayed(m_error.getFuture()); };

	void clear(KeyRangeRef range, const Arena* arena = 0) override {
		debug_printf("CLEAR %s\n", printable(range).c_str());
		m_tree->clear(range);
	}

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		debug_printf("SET %s\n", printable(keyValue).c_str());
		m_tree->set(keyValue);
	}

	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit = 1 << 30, int byteLimit = 1 << 30) override {
		debug_printf("READRANGE %s\n", printable(keys).c_str());
		return catchError(readRange_impl(this, keys, rowLimit, byteLimit));
	}

	ACTOR static Future<RangeResult> readRange_impl(KeyValueStoreRedwoodUnversioned* self,
	                                                KeyRange keys,
	                                                int rowLimit,
	                                                int byteLimit) {
		state VersionedBTree::BTreeCursor cur;
		wait(self->m_tree->initBTreeCursor(&cur, self->m_tree->getLastCommittedVersion()));

		state Reference<FlowLock> readLock = self->m_concurrentReads;
		wait(readLock->take());
		state FlowLock::Releaser releaser(*readLock);
		++g_redwoodMetrics.opGetRange;

		state RangeResult result;
		state int accumulatedBytes = 0;
		ASSERT(byteLimit > 0);

		if (rowLimit == 0) {
			return result;
		}

		// Prefetch is disabled for now pending some decent logic for deciding how much to fetch
		state int prefetchBytes = 0;

		if (rowLimit > 0) {
			wait(cur.seekGTE(keys.begin, prefetchBytes));
			while (cur.isValid()) {
				// Read page contents without using waits
				BTreePage::BinaryTree::Cursor leafCursor = cur.back().cursor;

				// we can bypass the bounds check for each key in the leaf if the entire leaf is in range
				// > because both query end and page upper bound are exclusive of the query results and page contents,
				// respectively
				bool boundsCheck = leafCursor.upperBound() > keys.end;
				// Whether or not any results from this page were added to results
				bool usedPage = false;

				while (leafCursor.valid()) {
					KeyValueRef kv = leafCursor.get().toKeyValueRef();
					if (boundsCheck && kv.key.compare(keys.end) >= 0) {
						break;
					}
					accumulatedBytes += kv.expectedSize();
					result.push_back(result.arena(), kv);
					usedPage = true;
					if (--rowLimit == 0 || accumulatedBytes >= byteLimit) {
						break;
					}
					leafCursor.moveNext();
				}

				// If the page was used, results must depend on the ArenaPage arena and the Mirror arena.
				// This must be done after visiting all the results in case the Mirror arena changes.
				if (usedPage) {
					result.arena().dependsOn(leafCursor.mirror->arena);
					result.arena().dependsOn(cur.back().page->getArena());
				}

				// Stop if the leaf cursor is still valid which means we hit a key or size limit or
				// if the cursor is in the root page, in which case there are no more pages.
				if (leafCursor.valid() || cur.inRoot()) {
					break;
				}
				cur.popPath();
				wait(cur.moveNext());
			}
		} else {
			wait(cur.seekLT(keys.end, prefetchBytes));
			while (cur.isValid()) {
				// Read page contents without using waits
				BTreePage::BinaryTree::Cursor leafCursor = cur.back().cursor;

				// we can bypass the bounds check for each key in the leaf if the entire leaf is in range
				// < because both query begin and page lower bound are inclusive of the query results and page contents,
				// respectively
				bool boundsCheck = leafCursor.lowerBound() < keys.begin;
				// Whether or not any results from this page were added to results
				bool usedPage = false;

				while (leafCursor.valid()) {
					KeyValueRef kv = leafCursor.get().toKeyValueRef();
					if (boundsCheck && kv.key.compare(keys.begin) < 0) {
						break;
					}
					accumulatedBytes += kv.expectedSize();
					result.push_back(result.arena(), kv);
					usedPage = true;
					if (++rowLimit == 0 || accumulatedBytes >= byteLimit) {
						break;
					}
					leafCursor.movePrev();
				}

				// If the page was used, results must depend on the ArenaPage arena and the Mirror arena.
				// This must be done after visiting all the results in case the Mirror arena changes.
				if (usedPage) {
					result.arena().dependsOn(leafCursor.mirror->arena);
					result.arena().dependsOn(cur.back().page->getArena());
				}

				// Stop if the leaf cursor is still valid which means we hit a key or size limit or
				// if we started in the root page
				if (leafCursor.valid() || cur.inRoot()) {
					break;
				}
				cur.popPath();
				wait(cur.movePrev());
			}
		}

		result.more = rowLimit == 0 || accumulatedBytes >= byteLimit;
		if (result.more) {
			ASSERT(result.size() > 0);
			result.readThrough = result[result.size() - 1].key;
		}
		return result;
	}

	ACTOR static Future<Optional<Value>> readValue_impl(KeyValueStoreRedwoodUnversioned* self,
	                                                    Key key,
	                                                    Optional<UID> debugID) {
		state VersionedBTree::BTreeCursor cur;
		wait(self->m_tree->initBTreeCursor(&cur, self->m_tree->getLastCommittedVersion()));

		state Reference<FlowLock> readLock = self->m_concurrentReads;
		wait(readLock->take());
		state FlowLock::Releaser releaser(*readLock);
		++g_redwoodMetrics.opGet;

		wait(cur.seekGTE(key, 0));
		if (cur.isValid() && cur.get().key == key) {
			// Return a Value whose arena depends on the source page arena
			Value v;
			v.arena().dependsOn(cur.back().page->getArena());
			v.contents() = cur.get().value.get();
			return v;
		}

		return Optional<Value>();
	}

	Future<Optional<Value>> readValue(KeyRef key, Optional<UID> debugID = Optional<UID>()) override {
		return catchError(readValue_impl(this, key, debugID));
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        Optional<UID> debugID = Optional<UID>()) override {
		return catchError(map(readValue_impl(this, key, debugID), [maxLength](Optional<Value> v) {
			if (v.present() && v.get().size() > maxLength) {
				v.get().contents() = v.get().substr(0, maxLength);
			}
			return v;
		}));
	}

	~KeyValueStoreRedwoodUnversioned() override{};

private:
	std::string m_filePrefix;
	VersionedBTree* m_tree;
	Future<Void> m_init;
	Promise<Void> m_closed;
	Promise<Void> m_error;
	Reference<FlowLock> m_concurrentReads;

	template <typename T>
	inline Future<T> catchError(Future<T> f) {
		return forwardError(f, m_error);
	}
};

IKeyValueStore* keyValueStoreRedwoodV1(std::string const& filename, UID logID) {
	return new KeyValueStoreRedwoodUnversioned(filename, logID);
}

int randomSize(int max) {
	int n = pow(deterministicRandom()->random01(), 3) * max;
	return n;
}

StringRef randomString(Arena& arena, int len, char firstChar = 'a', char lastChar = 'z') {
	++lastChar;
	StringRef s = makeString(len, arena);
	for (int i = 0; i < len; ++i) {
		*(uint8_t*)(s.begin() + i) = (uint8_t)deterministicRandom()->randomInt(firstChar, lastChar);
	}
	return s;
}

Standalone<StringRef> randomString(int len, char firstChar = 'a', char lastChar = 'z') {
	Standalone<StringRef> s;
	(StringRef&)s = randomString(s.arena(), len, firstChar, lastChar);
	return s;
}

KeyValue randomKV(int maxKeySize = 10, int maxValueSize = 5) {
	int kLen = randomSize(1 + maxKeySize);
	int vLen = maxValueSize > 0 ? randomSize(maxValueSize) : 0;

	KeyValue kv;

	kv.key = randomString(kv.arena(), kLen, 'a', 'm');
	for (int i = 0; i < kLen; ++i)
		mutateString(kv.key)[i] = (uint8_t)deterministicRandom()->randomInt('a', 'm');

	if (vLen > 0) {
		kv.value = randomString(kv.arena(), vLen, 'n', 'z');
		for (int i = 0; i < vLen; ++i)
			mutateString(kv.value)[i] = (uint8_t)deterministicRandom()->randomInt('o', 'z');
	}

	return kv;
}

// Verify a range using a BTreeCursor.
// Assumes that the BTree holds a single data version and the version is 0.
ACTOR Future<int> verifyRangeBTreeCursor(VersionedBTree* btree,
                                         Key start,
                                         Key end,
                                         Version v,
                                         std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                                         int* pErrorCount) {
	state int errors = 0;
	if (end <= start)
		end = keyAfter(start);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i =
	    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd =
	    written->upper_bound(std::make_pair(end.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iLast;

	state VersionedBTree::BTreeCursor cur;
	wait(btree->initBTreeCursor(&cur, v));
	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Start\n", v, start.printable().c_str(), end.printable().c_str());

	// Randomly use the cursor for something else first.
	if (deterministicRandom()->coinflip()) {
		state Key randomKey = randomKV().key;
		debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Dummy seek to '%s'\n",
		             v,
		             start.printable().c_str(),
		             end.printable().c_str(),
		             randomKey.toString().c_str());
		wait(success(cur.seek(randomKey, 0)));
	}

	debug_printf(
	    "VerifyRange(@%" PRId64 ", %s, %s): Actual seek\n", v, start.printable().c_str(), end.printable().c_str());
	wait(cur.seekGTE(start, 0));

	state Standalone<VectorRef<KeyValueRef>> results;

	while (cur.isValid() && cur.get().key < end) {
		// Find the next written kv pair that would be present at this version
		while (1) {
			iLast = i;
			if (i == iEnd)
				break;
			++i;

			if (iLast->first.second <= v && iLast->second.present() &&
			    (i == iEnd || i->first.first != iLast->first.first || i->first.second > v)) {
				debug_printf("VerifyRange(@%" PRId64 ", %s, %s) Found key in written map: %s\n",
				             v,
				             start.printable().c_str(),
				             end.printable().c_str(),
				             iLast->first.first.c_str());
				break;
			}
		}

		if (iLast == iEnd) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str());
			break;
		}

		if (cur.get().key != iLast->first.first) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       iLast->first.first.c_str());
			break;
		}
		if (cur.get().value.get() != iLast->second.get()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       cur.get().value.get().toString().c_str(),
			       iLast->second.get().c_str());
			break;
		}

		ASSERT(errors == 0);

		results.push_back(results.arena(), cur.get().toKeyValueRef());
		results.arena().dependsOn(cur.back().cursor.mirror->arena);
		results.arena().dependsOn(cur.back().page->getArena());

		wait(cur.moveNext());
	}

	// Make sure there are no further written kv pairs that would be present at this version.
	while (1) {
		iLast = i;
		if (i == iEnd)
			break;
		++i;
		if (iLast->first.second <= v && iLast->second.present() &&
		    (i == iEnd || i->first.first != iLast->first.first || i->first.second > v))
			break;
	}

	if (iLast != iEnd) {
		++errors;
		++*pErrorCount;
		printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has @%" PRId64 " '%s'\n",
		       v,
		       start.printable().c_str(),
		       end.printable().c_str(),
		       iLast->first.second,
		       iLast->first.first.c_str());
	}

	debug_printf(
	    "VerifyRangeReverse(@%" PRId64 ", %s, %s): start\n", v, start.printable().c_str(), end.printable().c_str());

	// Randomly use a new cursor at the same version for the reverse range read, if the version is still available for
	// opening new cursors
	if (v >= btree->getOldestVersion() && deterministicRandom()->coinflip()) {
		cur = VersionedBTree::BTreeCursor();
		wait(btree->initBTreeCursor(&cur, v));
	}

	// Now read the range from the tree in reverse order and compare to the saved results
	wait(cur.seekLT(end, 0));

	state std::reverse_iterator<const KeyValueRef*> r = results.rbegin();

	while (cur.isValid() && cur.get().key >= start) {
		if (r == results.rend()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str());
			break;
		}

		if (cur.get().key != r->key) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       r->key.toString().c_str());
			break;
		}
		if (cur.get().value.get() != r->value) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64
			       ", %s, %s) ERROR: Tree key '%s' has tree value '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       cur.get().value.get().toString().c_str(),
			       r->value.toString().c_str());
			break;
		}

		++r;
		wait(cur.movePrev());
	}

	if (r != results.rend()) {
		++errors;
		++*pErrorCount;
		printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has '%s'\n",
		       v,
		       start.printable().c_str(),
		       end.printable().c_str(),
		       r->key.toString().c_str());
	}

	return errors;
}

ACTOR Future<int> verifyRange(VersionedBTree* btree,
                              Key start,
                              Key end,
                              Version v,
                              std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                              int* pErrorCount) {
	state int errors = 0;
	if (end <= start)
		end = keyAfter(start);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i =
	    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd =
	    written->upper_bound(std::make_pair(end.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iLast;

	state Reference<IStoreCursor> cur = btree->readAtVersion(v);
	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Start cur=%p\n",
	             v,
	             start.printable().c_str(),
	             end.printable().c_str(),
	             cur.getPtr());

	// Randomly use the cursor for something else first.
	if (deterministicRandom()->coinflip()) {
		state Key randomKey = randomKV().key;
		debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Dummy seek to '%s'\n",
		             v,
		             start.printable().c_str(),
		             end.printable().c_str(),
		             randomKey.toString().c_str());
		wait(deterministicRandom()->coinflip() ? cur->findFirstEqualOrGreater(randomKey)
		                                       : cur->findLastLessOrEqual(randomKey));
	}

	debug_printf(
	    "VerifyRange(@%" PRId64 ", %s, %s): Actual seek\n", v, start.printable().c_str(), end.printable().c_str());
	wait(cur->findFirstEqualOrGreater(start));

	state std::vector<KeyValue> results;

	while (cur->isValid() && cur->getKey() < end) {
		// Find the next written kv pair that would be present at this version
		while (1) {
			iLast = i;
			if (i == iEnd)
				break;
			++i;

			if (iLast->first.second <= v && iLast->second.present() &&
			    (i == iEnd || i->first.first != iLast->first.first || i->first.second > v)) {
				debug_printf("VerifyRange(@%" PRId64 ", %s, %s) Found key in written map: %s\n",
				             v,
				             start.printable().c_str(),
				             end.printable().c_str(),
				             iLast->first.first.c_str());
				break;
			}
		}

		if (iLast == iEnd) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur->getKey().toString().c_str());
			break;
		}

		if (cur->getKey() != iLast->first.first) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur->getKey().toString().c_str(),
			       iLast->first.first.c_str());
			break;
		}
		if (cur->getValue() != iLast->second.get()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur->getKey().toString().c_str(),
			       cur->getValue().toString().c_str(),
			       iLast->second.get().c_str());
			break;
		}

		ASSERT(errors == 0);

		results.push_back(KeyValue(KeyValueRef(cur->getKey(), cur->getValue())));
		wait(cur->next());
	}

	// Make sure there are no further written kv pairs that would be present at this version.
	while (1) {
		iLast = i;
		if (i == iEnd)
			break;
		++i;
		if (iLast->first.second <= v && iLast->second.present() &&
		    (i == iEnd || i->first.first != iLast->first.first || i->first.second > v))
			break;
	}

	if (iLast != iEnd) {
		++errors;
		++*pErrorCount;
		printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has @%" PRId64 " '%s'\n",
		       v,
		       start.printable().c_str(),
		       end.printable().c_str(),
		       iLast->first.second,
		       iLast->first.first.c_str());
	}

	debug_printf(
	    "VerifyRangeReverse(@%" PRId64 ", %s, %s): start\n", v, start.printable().c_str(), end.printable().c_str());

	// Randomly use a new cursor at the same version for the reverse range read, if the version is still available for
	// opening new cursors
	if (v >= btree->getOldestVersion() && deterministicRandom()->coinflip()) {
		cur = btree->readAtVersion(v);
	}

	// Now read the range from the tree in reverse order and compare to the saved results
	wait(cur->findLastLessOrEqual(end));
	if (cur->isValid() && cur->getKey() == end)
		wait(cur->prev());

	state std::vector<KeyValue>::const_reverse_iterator r = results.rbegin();

	while (cur->isValid() && cur->getKey() >= start) {
		if (r == results.rend()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur->getKey().toString().c_str());
			break;
		}

		if (cur->getKey() != r->key) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur->getKey().toString().c_str(),
			       r->key.toString().c_str());
			break;
		}
		if (cur->getValue() != r->value) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64
			       ", %s, %s) ERROR: Tree key '%s' has tree value '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur->getKey().toString().c_str(),
			       cur->getValue().toString().c_str(),
			       r->value.toString().c_str());
			break;
		}

		++r;
		wait(cur->prev());
	}

	if (r != results.rend()) {
		++errors;
		++*pErrorCount;
		printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has '%s'\n",
		       v,
		       start.printable().c_str(),
		       end.printable().c_str(),
		       r->key.toString().c_str());
	}

	return errors;
}

// Verify the result of point reads for every set or cleared key at the given version
ACTOR Future<int> seekAll(VersionedBTree* btree,
                          Version v,
                          std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                          int* pErrorCount) {
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->cend();
	state int errors = 0;
	state Reference<IStoreCursor> cur = btree->readAtVersion(v);

	while (i != iEnd) {
		state std::string key = i->first.first;
		state Version ver = i->first.second;
		if (ver == v) {
			state Optional<std::string> val = i->second;
			debug_printf("Verifying @%" PRId64 " '%s'\n", ver, key.c_str());
			state Arena arena;
			wait(cur->findEqual(KeyRef(arena, key)));

			if (val.present()) {
				if (!(cur->isValid() && cur->getKey() == key && cur->getValue() == val.get())) {
					++errors;
					++*pErrorCount;
					if (!cur->isValid())
						printf("Verify ERROR: key_not_found: '%s' -> '%s' @%" PRId64 "\n",
						       key.c_str(),
						       val.get().c_str(),
						       ver);
					else if (cur->getKey() != key)
						printf("Verify ERROR: key_incorrect: found '%s' expected '%s' @%" PRId64 "\n",
						       cur->getKey().toString().c_str(),
						       key.c_str(),
						       ver);
					else if (cur->getValue() != val.get())
						printf("Verify ERROR: value_incorrect: for '%s' found '%s' expected '%s' @%" PRId64 "\n",
						       cur->getKey().toString().c_str(),
						       cur->getValue().toString().c_str(),
						       val.get().c_str(),
						       ver);
				}
			} else {
				if (cur->isValid() && cur->getKey() == key) {
					++errors;
					++*pErrorCount;
					printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%" PRId64 "\n",
					       key.c_str(),
					       cur->getValue().toString().c_str(),
					       ver);
				}
			}
		}
		++i;
	}
	return errors;
}

// Verify the result of point reads for every set or cleared key at the given version
ACTOR Future<int> seekAllBTreeCursor(VersionedBTree* btree,
                                     Version v,
                                     std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                                     int* pErrorCount) {
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->cend();
	state int errors = 0;
	state VersionedBTree::BTreeCursor cur;

	wait(btree->initBTreeCursor(&cur, v));

	while (i != iEnd) {
		state std::string key = i->first.first;
		state Version ver = i->first.second;
		if (ver == v) {
			state Optional<std::string> val = i->second;
			debug_printf("Verifying @%" PRId64 " '%s'\n", ver, key.c_str());
			state Arena arena;
			wait(cur.seekGTE(RedwoodRecordRef(KeyRef(arena, key), 0), 0));
			bool foundKey = cur.isValid() && cur.get().key == key;
			bool hasValue = foundKey && cur.get().value.present();

			if (val.present()) {
				bool valueMatch = hasValue && cur.get().value.get() == val.get();
				if (!foundKey || !hasValue || !valueMatch) {
					++errors;
					++*pErrorCount;
					if (!foundKey) {
						printf("Verify ERROR: key_not_found: '%s' -> '%s' @%" PRId64 "\n",
						       key.c_str(),
						       val.get().c_str(),
						       ver);
					} else if (!hasValue) {
						printf("Verify ERROR: value_not_found: '%s' -> '%s' @%" PRId64 "\n",
						       key.c_str(),
						       val.get().c_str(),
						       ver);
					} else if (!valueMatch) {
						printf("Verify ERROR: value_incorrect: for '%s' found '%s' expected '%s' @%" PRId64 "\n",
						       key.c_str(),
						       cur.get().value.get().toString().c_str(),
						       val.get().c_str(),
						       ver);
					}
				}
			} else if (foundKey && hasValue) {
				++errors;
				++*pErrorCount;
				printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%" PRId64 "\n",
				       key.c_str(),
				       cur.get().value.get().toString().c_str(),
				       ver);
			}
		}
		++i;
	}
	return errors;
}

ACTOR Future<Void> verify(VersionedBTree* btree,
                          FutureStream<Version> vStream,
                          std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                          int* pErrorCount,
                          bool serial) {
	state Future<int> fRangeAll;
	state Future<int> fRangeRandom;
	state Future<int> fSeekAll;

	// Queue of committed versions still readable from btree
	state std::deque<Version> committedVersions;

	try {
		loop {
			state Version v = waitNext(vStream);
			committedVersions.push_back(v);

			// Remove expired versions
			while (!committedVersions.empty() && committedVersions.front() < btree->getOldestVersion()) {
				committedVersions.pop_front();
			}

			// Continue if the versions list is empty, which won't wait until it reaches the oldest readable
			// btree version which will already be in vStream.
			if (committedVersions.empty()) {
				continue;
			}

			// Choose a random committed version.
			v = committedVersions[deterministicRandom()->randomInt(0, committedVersions.size())];

			debug_printf("Using committed version %" PRId64 "\n", v);
			// Get a cursor at v so that v doesn't get expired between the possibly serial steps below.
			state Reference<IStoreCursor> cur = btree->readAtVersion(v);

			debug_printf("Verifying entire key range at version %" PRId64 "\n", v);
			if (deterministicRandom()->coinflip()) {
				fRangeAll =
				    verifyRange(btree, LiteralStringRef(""), LiteralStringRef("\xff\xff"), v, written, pErrorCount);
			} else {
				fRangeAll = verifyRangeBTreeCursor(
				    btree, LiteralStringRef(""), LiteralStringRef("\xff\xff"), v, written, pErrorCount);
			}
			if (serial) {
				wait(success(fRangeAll));
			}

			Key begin = randomKV().key;
			Key end = randomKV().key;
			debug_printf(
			    "Verifying range (%s, %s) at version %" PRId64 "\n", toString(begin).c_str(), toString(end).c_str(), v);
			if (deterministicRandom()->coinflip()) {
				fRangeRandom = verifyRange(btree, begin, end, v, written, pErrorCount);
			} else {
				fRangeRandom = verifyRangeBTreeCursor(btree, begin, end, v, written, pErrorCount);
			}
			if (serial) {
				wait(success(fRangeRandom));
			}

			debug_printf("Verifying seeks to each changed key at version %" PRId64 "\n", v);
			if (deterministicRandom()->coinflip()) {
				fSeekAll = seekAll(btree, v, written, pErrorCount);
			} else {
				fSeekAll = seekAllBTreeCursor(btree, v, written, pErrorCount);
			}
			if (serial) {
				wait(success(fSeekAll));
			}

			wait(success(fRangeAll) && success(fRangeRandom) && success(fSeekAll));

			printf("Verified version %" PRId64 ", %d errors\n", v, *pErrorCount);

			if (*pErrorCount != 0)
				break;
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream && e.code() != error_code_transaction_too_old) {
			throw;
		}
	}
	return Void();
}

// Does a random range read, doesn't trap/report errors
ACTOR Future<Void> randomReader(VersionedBTree* btree) {
	try {
		state Reference<IStoreCursor> cur;
		loop {
			wait(yield());
			if (!cur || deterministicRandom()->random01() > .01) {
				Version v = btree->getLastCommittedVersion();
				cur = btree->readAtVersion(v);
			}

			state KeyValue kv = randomKV(10, 0);
			wait(cur->findFirstEqualOrGreater(kv.key));
			state int c = deterministicRandom()->randomInt(0, 100);
			while (cur->isValid() && c-- > 0) {
				wait(success(cur->next()));
				wait(yield());
			}
		}
	} catch (Error& e) {
		if (e.code() != error_code_transaction_too_old) {
			throw e;
		}
	}

	return Void();
}

struct IntIntPair {
	IntIntPair() {}
	IntIntPair(int k, int v) : k(k), v(v) {}

	IntIntPair(Arena& arena, const IntIntPair& toCopy) { *this = toCopy; }

	struct Delta {
		bool prefixSource;
		bool deleted;
		int dk;
		int dv;

		IntIntPair apply(const IntIntPair& base, Arena& arena) { return { base.k + dk, base.v + dv }; }

		void setPrefixSource(bool val) { prefixSource = val; }

		bool getPrefixSource() const { return prefixSource; }

		void setDeleted(bool val) { deleted = val; }

		bool getDeleted() const { return deleted; }

		int size() const { return sizeof(Delta); }

		std::string toString() const {
			return format(
			    "DELTA{prefixSource=%d deleted=%d dk=%d(0x%x) dv=%d(0x%x)}", prefixSource, deleted, dk, dk, dv, dv);
		}
	};

	// For IntIntPair, skipLen will be in units of fields, not bytes
	int getCommonPrefixLen(const IntIntPair& other, int skip = 0) const {
		if (k == other.k) {
			if (v == other.v) {
				return 2;
			}
			return 1;
		}
		return 0;
	}

	int compare(const IntIntPair& rhs, int skip = 0) const {
		if (skip == 2) {
			return 0;
		}
		int cmp = (skip > 0) ? 0 : (k - rhs.k);

		if (cmp == 0) {
			cmp = v - rhs.v;
		}
		return cmp;
	}

	bool operator==(const IntIntPair& rhs) const { return compare(rhs) == 0; }
	bool operator!=(const IntIntPair& rhs) const { return compare(rhs) != 0; }

	bool operator<(const IntIntPair& rhs) const { return compare(rhs) < 0; }
	bool operator>(const IntIntPair& rhs) const { return compare(rhs) > 0; }
	bool operator<=(const IntIntPair& rhs) const { return compare(rhs) <= 0; }
	bool operator>=(const IntIntPair& rhs) const { return compare(rhs) >= 0; }

	int deltaSize(const IntIntPair& base, int skipLen, bool worstcase) const { return sizeof(Delta); }

	int writeDelta(Delta& d, const IntIntPair& base, int commonPrefix = -1) const {
		d.prefixSource = false;
		d.deleted = false;
		d.dk = k - base.k;
		d.dv = v - base.v;
		return sizeof(Delta);
	}

	int k;
	int v;

	std::string toString() const { return format("{k=%d(0x%x) v=%d(0x%x)}", k, k, v, v); }
};

int deltaTest(RedwoodRecordRef rec, RedwoodRecordRef base) {
	std::vector<uint8_t> buf(rec.key.size() + rec.value.orDefault(StringRef()).size() + 20);
	RedwoodRecordRef::Delta& d = *(RedwoodRecordRef::Delta*)&buf.front();

	Arena mem;
	int expectedSize = rec.deltaSize(base, 0, false);
	int deltaSize = rec.writeDelta(d, base);
	RedwoodRecordRef decoded = d.apply(base, mem);

	if (decoded != rec || expectedSize != deltaSize || d.size() != deltaSize) {
		printf("\n");
		printf("Base:                %s\n", base.toString().c_str());
		printf("Record:              %s\n", rec.toString().c_str());
		printf("Decoded:             %s\n", decoded.toString().c_str());
		printf("deltaSize():         %d\n", expectedSize);
		printf("writeDelta():        %d\n", deltaSize);
		printf("d.size():            %d\n", d.size());
		printf("DeltaToString:       %s\n", d.toString().c_str());
		printf("RedwoodRecordRef::Delta test failure!\n");
		ASSERT(false);
	}

	return deltaSize;
}

RedwoodRecordRef randomRedwoodRecordRef(const std::string& keyBuffer, const std::string& valueBuffer) {
	RedwoodRecordRef rec;
	rec.key = StringRef((uint8_t*)keyBuffer.data(), deterministicRandom()->randomInt(0, keyBuffer.size()));
	if (deterministicRandom()->coinflip()) {
		rec.value = StringRef((uint8_t*)valueBuffer.data(), deterministicRandom()->randomInt(0, valueBuffer.size()));
	}

	int versionIntSize = deterministicRandom()->randomInt(0, 8) * 8;
	if (versionIntSize > 0) {
		--versionIntSize;
		int64_t max = ((int64_t)1 << versionIntSize) - 1;
		rec.version = deterministicRandom()->randomInt64(0, max);
	}

	return rec;
}

TEST_CASE("/redwood/correctness/unit/RedwoodRecordRef") {
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[0] == 3);
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[1] == 4);
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[2] == 6);
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[3] == 8);

	ASSERT(RedwoodRecordRef::Delta::VersionDeltaSizes[0] == 0);
	ASSERT(RedwoodRecordRef::Delta::VersionDeltaSizes[1] == 4);
	ASSERT(RedwoodRecordRef::Delta::VersionDeltaSizes[2] == 6);
	ASSERT(RedwoodRecordRef::Delta::VersionDeltaSizes[3] == 8);

	// Test pageID stuff.
	{
		LogicalPageID ids[] = { 1, 5 };
		BTreePageIDRef id(ids, 2);
		RedwoodRecordRef r;
		r.setChildPage(id);
		ASSERT(r.getChildPage() == id);
		ASSERT(r.getChildPage().begin() == id.begin());

		Standalone<RedwoodRecordRef> r2 = r;
		ASSERT(r2.getChildPage() == id);
		ASSERT(r2.getChildPage().begin() != id.begin());
	}

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 0, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), 0, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef("abcd"), 0, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef("abcd"), 2, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(std::string(300, 'k'), 2, std::string(1e6, 'v')),
	          RedwoodRecordRef(std::string(300, 'k'), 2, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 2, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), 1, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 0xffff, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), 1, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 1, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), 0xffff, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 0xffffff, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), 1, LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 1, LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), 0xffffff, LiteralStringRef("")));

	Arena mem;
	double start;
	uint64_t total;
	uint64_t count;
	uint64_t i;
	int64_t bytes;

	std::string keyBuffer(30000, 'k');
	std::string valueBuffer(70000, 'v');
	start = timer();
	count = 1000;
	bytes = 0;
	for (i = 0; i < count; ++i) {
		RedwoodRecordRef a = randomRedwoodRecordRef(keyBuffer, valueBuffer);
		RedwoodRecordRef b = randomRedwoodRecordRef(keyBuffer, valueBuffer);
		bytes += deltaTest(a, b);
	}
	double elapsed = timer() - start;
	printf("DeltaTest() on random large records %f M/s  %f MB/s\n", count / elapsed / 1e6, bytes / elapsed / 1e6);

	keyBuffer.resize(30);
	valueBuffer.resize(100);
	start = timer();
	count = 1e6;
	bytes = 0;
	for (i = 0; i < count; ++i) {
		RedwoodRecordRef a = randomRedwoodRecordRef(keyBuffer, valueBuffer);
		RedwoodRecordRef b = randomRedwoodRecordRef(keyBuffer, valueBuffer);
		bytes += deltaTest(a, b);
	}
	printf("DeltaTest() on random small records %f M/s  %f MB/s\n", count / elapsed / 1e6, bytes / elapsed / 1e6);

	RedwoodRecordRef rec1;
	RedwoodRecordRef rec2;

	rec1.key = LiteralStringRef("alksdfjaklsdfjlkasdjflkasdjfklajsdflk;ajsdflkajdsflkjadsf1");
	rec2.key = LiteralStringRef("alksdfjaklsdfjlkasdjflkasdjfklajsdflk;ajsdflkajdsflkjadsf234");

	rec1.version = deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max());
	rec2.version = deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max());

	start = timer();
	total = 0;
	count = 100e6;
	for (i = 0; i < count; ++i) {
		total += rec1.getCommonPrefixLen(rec2, 50);
	}
	printf("%" PRId64 " getCommonPrefixLen(skip=50) %f M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 100e6;
	for (i = 0; i < count; ++i) {
		total += rec1.getCommonPrefixLen(rec2, 0);
	}
	printf("%" PRId64 " getCommonPrefixLen(skip=0) %f M/s\n", total, count / (timer() - start) / 1e6);

	char buf[1000];
	RedwoodRecordRef::Delta& d = *(RedwoodRecordRef::Delta*)buf;

	start = timer();
	total = 0;
	count = 100e6;
	int commonPrefix = rec1.getCommonPrefixLen(rec2, 0);

	for (i = 0; i < count; ++i) {
		total += rec1.writeDelta(d, rec2, commonPrefix);
	}
	printf("%" PRId64 " writeDelta(commonPrefix=%d) %f M/s\n", total, commonPrefix, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 10e6;
	for (i = 0; i < count; ++i) {
		total += rec1.writeDelta(d, rec2);
	}
	printf("%" PRId64 " writeDelta() %f M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 10e6;
	for (i = 0; i < count; ++i) {
		total += rec1.compare(rec2, 0);
	}
	printf("%" PRId64 " compare(skip=0) %f M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 10e6;
	for (i = 0; i < count; ++i) {
		total += rec1.compare(rec2, 50);
	}
	printf("%" PRId64 " compare(skip=50) %f M/s\n", total, count / (timer() - start) / 1e6);

	return Void();
}

TEST_CASE("/redwood/correctness/unit/deltaTree/RedwoodRecordRef") {
	// Sanity check on delta tree node format
	ASSERT(DeltaTree<RedwoodRecordRef>::Node::headerSize(false) == 4);
	ASSERT(DeltaTree<RedwoodRecordRef>::Node::headerSize(true) == 8);

	const int N = deterministicRandom()->randomInt(200, 1000);

	RedwoodRecordRef prev;
	RedwoodRecordRef next(LiteralStringRef("\xff\xff\xff\xff"));

	Arena arena;
	std::set<RedwoodRecordRef> uniqueItems;

	// Add random items to uniqueItems until its size is N
	while (uniqueItems.size() < N) {
		std::string k = deterministicRandom()->randomAlphaNumeric(30);
		std::string v = deterministicRandom()->randomAlphaNumeric(30);
		RedwoodRecordRef rec;
		rec.key = StringRef(arena, k);
		rec.version = deterministicRandom()->coinflip()
		                  ? deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max())
		                  : invalidVersion;
		if (deterministicRandom()->coinflip()) {
			rec.value = StringRef(arena, v);
		}
		if (uniqueItems.count(rec) == 0) {
			uniqueItems.insert(rec);
		}
	}
	std::vector<RedwoodRecordRef> items(uniqueItems.begin(), uniqueItems.end());

	int bufferSize = N * 100;
	bool largeTree = bufferSize > DeltaTree<RedwoodRecordRef>::SmallSizeLimit;
	DeltaTree<RedwoodRecordRef>* tree = (DeltaTree<RedwoodRecordRef>*)new uint8_t[bufferSize];

	tree->build(bufferSize, &items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialHeight=%d  largeTree=%d\n",
	       (int)items.size(),
	       (int)tree->size(),
	       (int)tree->initialHeight,
	       largeTree);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t*)tree, tree->size()).toHexString().c_str());

	DeltaTree<RedwoodRecordRef>::Mirror r(tree, &prev, &next);

	// Test delete/insert behavior for each item, making no net changes
	printf("Testing seek/delete/insert for existing keys with random values\n");
	ASSERT(tree->numItems == items.size());
	for (auto rec : items) {
		// Insert existing should fail
		ASSERT(!r.insert(rec));
		ASSERT(tree->numItems == items.size());

		// Erase existing should succeed
		ASSERT(r.erase(rec));
		ASSERT(tree->numItems == items.size() - 1);

		// Erase deleted should fail
		ASSERT(!r.erase(rec));
		ASSERT(tree->numItems == items.size() - 1);

		// Insert deleted should succeed
		ASSERT(r.insert(rec));
		ASSERT(tree->numItems == items.size());

		// Insert existing should fail
		ASSERT(!r.insert(rec));
		ASSERT(tree->numItems == items.size());
	}

	DeltaTree<RedwoodRecordRef>::Cursor fwd = r.getCursor();
	DeltaTree<RedwoodRecordRef>::Cursor rev = r.getCursor();

	DeltaTree<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>::Mirror rValuesOnly(tree, &prev, &next);
	DeltaTree<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>::Cursor fwdValueOnly = rValuesOnly.getCursor();

	printf("Verifying tree contents using forward, reverse, and value-only iterators\n");
	ASSERT(fwd.moveFirst());
	ASSERT(fwdValueOnly.moveFirst());
	ASSERT(rev.moveLast());

	int i = 0;
	while (1) {
		if (fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n",
			       i,
			       fwd.get().toString().c_str(),
			       items[i].toString().c_str());
			printf("Delta: %s\n", fwd.node->raw->delta(largeTree).toString().c_str());
			ASSERT(false);
		}
		if (rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n",
			       i,
			       rev.get().toString().c_str(),
			       items[items.size() - 1 - i].toString().c_str());
			printf("Delta: %s\n", rev.node->raw->delta(largeTree).toString().c_str());
			ASSERT(false);
		}
		if (fwdValueOnly.get().value != items[i].value) {
			printf("forward values-only iterator i=%d\n  %s found\n  %s expected\n",
			       i,
			       fwdValueOnly.get().toString().c_str(),
			       items[i].toString().c_str());
			printf("Delta: %s\n", fwdValueOnly.node->raw->delta(largeTree).toString().c_str());
			ASSERT(false);
		}
		++i;

		bool more = fwd.moveNext();
		ASSERT(fwdValueOnly.moveNext() == more);
		ASSERT(rev.movePrev() == more);

		ASSERT(fwd.valid() == more);
		ASSERT(fwdValueOnly.valid() == more);
		ASSERT(rev.valid() == more);

		if (!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	{
		DeltaTree<RedwoodRecordRef>::Mirror mirror(tree, &prev, &next);
		DeltaTree<RedwoodRecordRef>::Cursor c = mirror.getCursor();

		printf("Doing 20M random seeks using the same cursor from the same mirror.\n");
		double start = timer();

		for (int i = 0; i < 20000000; ++i) {
			const RedwoodRecordRef& query = items[deterministicRandom()->randomInt(0, items.size())];
			if (!c.seekLessThanOrEqual(query)) {
				printf("Not found!  query=%s\n", query.toString().c_str());
				ASSERT(false);
			}
			if (c.get() != query) {
				printf("Found incorrect node!  query=%s  found=%s\n",
				       query.toString().c_str(),
				       c.get().toString().c_str());
				ASSERT(false);
			}
		}
		double elapsed = timer() - start;
		printf("Elapsed %f\n", elapsed);
	}

	{
		printf("Doing 5M random seeks using 10k random cursors, each from a different mirror.\n");
		double start = timer();
		std::vector<DeltaTree<RedwoodRecordRef>::Mirror*> mirrors;
		std::vector<DeltaTree<RedwoodRecordRef>::Cursor> cursors;
		for (int i = 0; i < 10000; ++i) {
			mirrors.push_back(new DeltaTree<RedwoodRecordRef>::Mirror(tree, &prev, &next));
			cursors.push_back(mirrors.back()->getCursor());
		}

		for (int i = 0; i < 5000000; ++i) {
			const RedwoodRecordRef& query = items[deterministicRandom()->randomInt(0, items.size())];
			DeltaTree<RedwoodRecordRef>::Cursor& c = cursors[deterministicRandom()->randomInt(0, cursors.size())];
			if (!c.seekLessThanOrEqual(query)) {
				printf("Not found!  query=%s\n", query.toString().c_str());
				ASSERT(false);
			}
			if (c.get() != query) {
				printf("Found incorrect node!  query=%s  found=%s\n",
				       query.toString().c_str(),
				       c.get().toString().c_str());
				ASSERT(false);
			}
		}
		double elapsed = timer() - start;
		printf("Elapsed %f\n", elapsed);
	}

	return Void();
}

TEST_CASE("/redwood/correctness/unit/deltaTree/IntIntPair") {
	const int N = 200;
	IntIntPair prev = { 1, 0 };
	IntIntPair next = { 10000, 10000 };

	state std::function<IntIntPair()> randomPair = [&]() {
		return IntIntPair(
		    { deterministicRandom()->randomInt(prev.k, next.k), deterministicRandom()->randomInt(prev.v, next.v) });
	};

	// Build a set of N unique items, where no consecutive items are in the set, a requirement of the seek behavior tests.
	std::set<IntIntPair> uniqueItems;
	while (uniqueItems.size() < N) {
		IntIntPair p = randomPair();
		auto nextP = p; // also check if next highest/lowest key is not in set
		nextP.v++;
		auto prevP = p;
		prevP.v--;
		if (uniqueItems.count(p) == 0 && uniqueItems.count(nextP) == 0 && uniqueItems.count(prevP) == 0) {
			uniqueItems.insert(p);
		}
	}

	// Build tree of items
	std::vector<IntIntPair> items(uniqueItems.begin(), uniqueItems.end());
	int bufferSize = N * 2 * 20;
	DeltaTree<IntIntPair>* tree = (DeltaTree<IntIntPair>*)new uint8_t[bufferSize];
	int builtSize = tree->build(bufferSize, &items[0], &items[items.size()], &prev, &next);
	ASSERT(builtSize <= bufferSize);

	DeltaTree<IntIntPair>::Mirror r(tree, &prev, &next);

	// Grow uniqueItems until tree is full, adding half of new items to toDelete
	std::vector<IntIntPair> toDelete;
	while (1) {
		IntIntPair p = randomPair();
		auto nextP = p; // also check if next highest/lowest key is not in the set
		nextP.v++;
		auto prevP = p;
		prevP.v--;
		if (uniqueItems.count(p) == 0 && uniqueItems.count(nextP) == 0 && uniqueItems.count(prevP) == 0) {
			if (!r.insert(p)) {
				break;
			};
			uniqueItems.insert(p);
			if (deterministicRandom()->coinflip()) {
				toDelete.push_back(p);
			}
			// printf("Inserted %s  size=%d\n", items.back().toString().c_str(), tree->size());
		}
	}

	ASSERT(tree->numItems > 2 * N);
	ASSERT(tree->size() <= bufferSize);

	// Update items vector
	items = std::vector<IntIntPair>(uniqueItems.begin(), uniqueItems.end());

	auto printItems = [&] {
		for (int k = 0; k < items.size(); ++k) {
			printf("%d %s\n", k, items[k].toString().c_str());
		}
	};

	printf("Count=%d  Size=%d  InitialHeight=%d  MaxHeight=%d\n",
	       (int)items.size(),
	       (int)tree->size(),
	       (int)tree->initialHeight,
	       (int)tree->maxHeight);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t*)tree, tree->size()).toHexString().c_str());

	// Iterate through items and tree forward and backward, verifying tree contents.
	auto scanAndVerify = [&]() {
		printf("Verify tree contents.\n");

		DeltaTree<IntIntPair>::Cursor fwd = r.getCursor();
		DeltaTree<IntIntPair>::Cursor rev = r.getCursor();
		ASSERT(fwd.moveFirst());
		ASSERT(rev.moveLast());

		for (int i = 0; i < items.size(); ++i) {
			if (fwd.get() != items[i]) {
				printItems();
				printf("forward iterator i=%d\n  %s found\n  %s expected\n",
				       i,
				       fwd.get().toString().c_str(),
				       items[i].toString().c_str());
				ASSERT(false);
			}
			if (rev.get() != items[items.size() - 1 - i]) {
				printItems();
				printf("reverse iterator i=%d\n  %s found\n  %s expected\n",
				       i,
				       rev.get().toString().c_str(),
				       items[items.size() - 1 - i].toString().c_str());
				ASSERT(false);
			}

			// Advance iterator, check scanning cursors for correct validity state
			int j = i + 1;
			bool end = j == items.size();

			ASSERT(fwd.moveNext() == !end);
			ASSERT(rev.movePrev() == !end);
			ASSERT(fwd.valid() == !end);
			ASSERT(rev.valid() == !end);

			if (end) {
				break;
			}
		}
	};

	// Verify tree contents
	scanAndVerify();

	// Create a new mirror, decoding the tree from scratch since insert() modified both the tree and the mirror
	r = DeltaTree<IntIntPair>::Mirror(tree, &prev, &next);
	scanAndVerify();

	// For each randomly selected new item to be deleted, delete it from the DeltaTree and from uniqueItems
	printf("Deleting some items\n");
	for (auto p : toDelete) {
		uniqueItems.erase(p);
		DeltaTree<IntIntPair>::Cursor c = r.getCursor();
		ASSERT(c.seekLessThanOrEqual(p));
		c.erase();
	}
	// Update items vector
	items = std::vector<IntIntPair>(uniqueItems.begin(), uniqueItems.end());

	// Verify tree contents after deletions
	scanAndVerify();

	printf("Verifying insert/erase behavior for existing items\n");
	// Test delete/insert behavior for each item, making no net changes
	for (auto p : items) {
		// Insert existing should fail
		ASSERT(!r.insert(p));

		// Erase existing should succeed
		ASSERT(r.erase(p));

		// Erase deleted should fail
		ASSERT(!r.erase(p));

		// Insert deleted should succeed
		ASSERT(r.insert(p));

		// Insert existing should fail
		ASSERT(!r.insert(p));
	}

	// Tree contents should still match items vector
	scanAndVerify();

	printf("Verifying seek behaviors\n");
	DeltaTree<IntIntPair>::Cursor s = r.getCursor();

	// SeekLTE to each element
	for (int i = 0; i < items.size(); ++i) {
		IntIntPair p = items[i];
		IntIntPair q = p;
		ASSERT(s.seekLessThanOrEqual(q));
		if (s.get() != p) {
			printItems();
			printf("seekLessThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s.get().toString().c_str(),
			       p.toString().c_str());
			ASSERT(false);
		}
	}

	// SeekGTE to each element
	for (int i = 0; i < items.size(); ++i) {
		IntIntPair p = items[i];
		IntIntPair q = p;
		ASSERT(s.seekGreaterThanOrEqual(q));
		if (s.get() != p) {
			printItems();
			printf("seekGreaterThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s.get().toString().c_str(),
			       p.toString().c_str());
			ASSERT(false);
		}
	}

	// SeekLTE to the next possible int pair value after each element to make sure the base element is found
	// Assumes no consecutive items are present in the set
	for (int i = 0; i < items.size(); ++i) {
		IntIntPair p = items[i];
		IntIntPair q = p;
		q.v++;
		ASSERT(s.seekLessThanOrEqual(q));
		if (s.get() != p) {
			printItems();
			printf("seekLessThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s.get().toString().c_str(),
			       p.toString().c_str());
			ASSERT(false);
		}
	}

	// SeekGTE to the previous possible int pair value after each element to make sure the base element is found
	// Assumes no consecutive items are present in the set
	for (int i = 0; i < items.size(); ++i) {
		IntIntPair p = items[i];
		IntIntPair q = p;
		q.v--;
		ASSERT(s.seekGreaterThanOrEqual(q));
		if (s.get() != p) {
			printItems();
			printf("seekGreaterThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s.get().toString().c_str(),
			       p.toString().c_str());
			ASSERT(false);
		}
	}

	// SeekLTE to each element N times, using every element as a hint
	for (int i = 0; i < items.size(); ++i) {
		IntIntPair p = items[i];
		IntIntPair q = p;
		for (int j = 0; j < items.size(); ++j) {
			ASSERT(s.seekLessThanOrEqual(items[j]));
			ASSERT(s.seekLessThanOrEqual(q, 0, &s));
			if (s.get() != p) {
				printItems();
				printf("i=%d  j=%d\n", i, j);
				printf("seekLessThanOrEqual(%s) found %s expected %s\n",
				       q.toString().c_str(),
				       s.get().toString().c_str(),
				       p.toString().c_str());
				ASSERT(false);
			}
		}
	}

	// SeekLTE to each element's next possible value, using each element as a hint
	// Assumes no consecutive items are present in the set
	for (int i = 0; i < items.size(); ++i) {
		IntIntPair p = items[i];
		IntIntPair q = p;
		q.v++;
		for (int j = 0; j < items.size(); ++j) {
			ASSERT(s.seekLessThanOrEqual(items[j]));
			ASSERT(s.seekLessThanOrEqual(q, 0, &s));
			if (s.get() != p) {
				printItems();
				printf("i=%d  j=%d\n", i, j);
				ASSERT(false);
			}
		}
	}

	auto skipSeekPerformance = [&](int jumpMax, bool old, bool useHint, int count) {
		// Skip to a series of increasing items, jump by up to jumpMax units forward in the
		// items, wrapping around to 0.
		double start = timer();
		s.moveFirst();
		auto first = s;
		int pos = 0;
		for (int c = 0; c < count; ++c) {
			int jump = deterministicRandom()->randomInt(0, jumpMax);
			int newPos = pos + jump;
			if (newPos >= items.size()) {
				pos = 0;
				newPos = jump;
				s = first;
			}
			IntIntPair q = items[newPos];
			++q.v;
			if (old) {
				if (useHint) {
					s.seekLessThanOrEqualOld(q, 0, &s, newPos - pos);
				} else {
					s.seekLessThanOrEqualOld(q, 0, nullptr, 0);
				}
			} else {
				if (useHint) {
					s.seekLessThanOrEqual(q, 0, &s, newPos - pos);
				} else {
					s.seekLessThanOrEqual(q);
				}
			}
			pos = newPos;
		}
		double elapsed = timer() - start;
		printf("Seek/skip test, count=%d jumpMax=%d, items=%d, oldSeek=%d useHint=%d:  Elapsed %f seconds  %.2f M/s\n",
		       count,
		       jumpMax,
		       items.size(),
		       old,
		       useHint,
		       elapsed,
		       double(count) / elapsed / 1e6);
	};

	// Compare seeking to nearby elements with and without hints, using the old and new SeekLessThanOrEqual methods.
	// TODO:  Once seekLessThanOrEqual() with a hint is as fast as seekLessThanOrEqualOld, remove it.
	skipSeekPerformance(8, true, false, 80e6);
	skipSeekPerformance(8, true, true, 80e6);
	skipSeekPerformance(8, false, false, 80e6);
	skipSeekPerformance(8, false, true, 80e6);

	// Repeatedly seek for one of a set of pregenerated random pairs and time it.
	std::vector<IntIntPair> randomPairs;
	randomPairs.reserve(10 * N);
	for (int i = 0; i < 10 * N; ++i) {
		randomPairs.push_back(randomPair());
	}

	// Random seeks
	double start = timer();
	for (int i = 0; i < 20000000; ++i) {
		IntIntPair p = randomPairs[i % randomPairs.size()];
		// Verify the result is less than or equal, and if seek fails then p must be lower than lowest (first) item
		if (!s.seekLessThanOrEqual(p)) {
			if (p >= items.front()) {
				printf("Seek failed!  query=%s  front=%s\n", p.toString().c_str(), items.front().toString().c_str());
				ASSERT(false);
			}
		} else if (s.get() > p) {
			printf("Found incorrect node!  query=%s  found=%s\n", p.toString().c_str(), s.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Random seek test: Elapsed %f\n", elapsed);

	return Void();
}

struct SimpleCounter {
	SimpleCounter() : x(0), xt(0), t(timer()), start(t) {}
	void operator+=(int n) { x += n; }
	void operator++() { x++; }
	int64_t get() { return x; }
	double rate() {
		double t2 = timer();
		int r = (x - xt) / (t2 - t);
		xt = x;
		t = t2;
		return r;
	}
	double avgRate() { return x / (timer() - start); }
	int64_t x;
	double t;
	double start;
	int64_t xt;
	std::string toString() { return format("%" PRId64 "/%.2f/%.2f", x, rate() / 1e6, avgRate() / 1e6); }
};

TEST_CASE(":/redwood/performance/mutationBuffer") {
	// This test uses pregenerated short random keys
	int count = 10e6;

	printf("Generating %d strings...\n", count);
	Arena arena;
	std::vector<KeyRef> strings;
	while (strings.size() < count) {
		strings.push_back(randomString(arena, 5));
	}

	printf("Inserting and then finding each string...\n", count);
	double start = timer();
	VersionedBTree::MutationBuffer m;
	for (int i = 0; i < count; ++i) {
		KeyRef key = strings[i];
		auto a = m.insert(key);
		auto b = m.lower_bound(key);
		ASSERT(a == b);
		m.erase(a, b);
	}

	double elapsed = timer() - start;
	printf("count=%d elapsed=%f\n", count, elapsed);

	return Void();
}

// This test is only useful with Arena debug statements which show when aligned buffers are allocated and freed.
TEST_CASE(":/redwood/pager/ArenaPage") {
	Arena x;
	printf("Making p\n");
	Reference<ArenaPage> p(new ArenaPage(4096, 4096));
	printf("Made p=%p\n", p->begin());
	printf("Clearing p\n");
	p.clear();
	printf("Making p\n");
	p = Reference<ArenaPage>(new ArenaPage(4096, 4096));
	printf("Made p=%p\n", p->begin());
	printf("Making x depend on p\n");
	x.dependsOn(p->getArena());
	printf("Clearing p\n");
	p.clear();
	printf("Clearing x\n");
	x = Arena();
	printf("Pointer should be freed\n");
	return Void();
}

TEST_CASE("/redwood/correctness/btree") {
	g_redwoodMetricsActor = Void(); // Prevent trace event metrics from starting
	g_redwoodMetrics.clear();

	state std::string fileName = params.get("fileName").orDefault("unittest_pageFile.redwood");
	IPager2* pager;

	state bool serialTest = params.getInt("serialTest").orDefault(deterministicRandom()->random01() < 0.25);
	state bool shortTest = params.getInt("shortTest").orDefault(deterministicRandom()->random01() < 0.25);

	state int pageSize =
	    shortTest ? 200 : (deterministicRandom()->coinflip() ? 4096 : deterministicRandom()->randomInt(200, 400));

	state int64_t targetPageOps = params.getInt("targetPageOps").orDefault(shortTest ? 50000 : 1000000);
	state bool pagerMemoryOnly =
	    params.getInt("pagerMemoryOnly").orDefault(shortTest && (deterministicRandom()->random01() < .001));
	state int maxKeySize = params.getInt("maxKeySize").orDefault(deterministicRandom()->randomInt(1, pageSize * 2));
	state int maxValueSize = params.getInt("maxValueSize").orDefault(randomSize(pageSize * 25));
	state int maxCommitSize =
	    params.getInt("maxCommitSize")
	        .orDefault(shortTest ? 1000 : randomSize(std::min<int>((maxKeySize + maxValueSize) * 20000, 10e6)));
	state double clearProbability =
	    params.getDouble("clearProbability").orDefault(deterministicRandom()->random01() * .1);
	state double clearSingleKeyProbability =
	    params.getDouble("clearSingleKeyProbability").orDefault(deterministicRandom()->random01());
	state double clearPostSetProbability =
	    params.getDouble("clearPostSetProbability").orDefault(deterministicRandom()->random01() * .1);
	state double coldStartProbability =
	    params.getDouble("coldStartProbability").orDefault(pagerMemoryOnly ? 0 : (deterministicRandom()->random01()));
	state double advanceOldVersionProbability =
	    params.getDouble("advanceOldVersionProbability").orDefault(deterministicRandom()->random01());
	state int64_t cacheSizeBytes =
	    params.getInt("cacheSizeBytes")
	        .orDefault(pagerMemoryOnly ? 2e9
	                                   : (pageSize * deterministicRandom()->randomInt(1, (BUGGIFY ? 2 : 10000) + 1)));
	state Version versionIncrement =
	    params.getInt("versionIncrement").orDefault(deterministicRandom()->randomInt64(1, 1e8));
	state Version remapCleanupWindow =
	    params.getInt("remapCleanupWindow")
	        .orDefault(BUGGIFY ? 0 : deterministicRandom()->randomInt64(1, versionIncrement * 50));
	state int maxVerificationMapEntries = params.getInt("maxVerificationMapEntries").orDefault(300e3);

	printf("\n");
	printf("targetPageOps: %" PRId64 "\n", targetPageOps);
	printf("pagerMemoryOnly: %d\n", pagerMemoryOnly);
	printf("serialTest: %d\n", serialTest);
	printf("shortTest: %d\n", shortTest);
	printf("pageSize: %d\n", pageSize);
	printf("maxKeySize: %d\n", maxKeySize);
	printf("maxValueSize: %d\n", maxValueSize);
	printf("maxCommitSize: %d\n", maxCommitSize);
	printf("clearProbability: %f\n", clearProbability);
	printf("clearSingleKeyProbability: %f\n", clearSingleKeyProbability);
	printf("clearPostSetProbability: %f\n", clearPostSetProbability);
	printf("coldStartProbability: %f\n", coldStartProbability);
	printf("advanceOldVersionProbability: %f\n", advanceOldVersionProbability);
	printf("cacheSizeBytes: %s\n", cacheSizeBytes == 0 ? "default" : format("%" PRId64, cacheSizeBytes).c_str());
	printf("versionIncrement: %" PRId64 "\n", versionIncrement);
	printf("remapCleanupWindow: %" PRId64 "\n", remapCleanupWindow);
	printf("maxVerificationMapEntries: %d\n", maxVerificationMapEntries);
	printf("\n");

	printf("Deleting existing test data...\n");
	deleteFile(fileName);

	printf("Initializing...\n");
	pager = new DWALPager(pageSize, fileName, cacheSizeBytes, remapCleanupWindow, pagerMemoryOnly);
	state VersionedBTree* btree = new VersionedBTree(pager, fileName);
	wait(btree->init());

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state std::set<Key> keys;

	state Version lastVer = btree->getLatestVersion();
	printf("Starting from version: %" PRId64 "\n", lastVer);

	state Version version = lastVer + 1;
	btree->setWriteVersion(version);

	state SimpleCounter mutationBytes;
	state SimpleCounter keyBytesInserted;
	state SimpleCounter valueBytesInserted;
	state SimpleCounter sets;
	state SimpleCounter rangeClears;
	state SimpleCounter keyBytesCleared;
	state int errorCount;
	state int mutationBytesThisCommit = 0;
	state int mutationBytesTargetThisCommit = randomSize(maxCommitSize);

	state PromiseStream<Version> committedVersions;
	state Future<Void> verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
	state Future<Void> randomTask = serialTest ? Void() : (randomReader(btree) || btree->getError());
	committedVersions.send(lastVer);

	state Future<Void> commit = Void();
	state int64_t totalPageOps = 0;

	while (totalPageOps < targetPageOps && written.size() < maxVerificationMapEntries) {
		// Sometimes increment the version
		if (deterministicRandom()->random01() < 0.10) {
			++version;
			btree->setWriteVersion(version);
		}

		// Sometimes do a clear range
		if (deterministicRandom()->random01() < clearProbability) {
			Key start = randomKV(maxKeySize, 1).key;
			Key end = (deterministicRandom()->random01() < .01) ? keyAfter(start) : randomKV(maxKeySize, 1).key;

			// Sometimes replace start and/or end with a close actual (previously used) value
			if (deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(start);
				if (i != keys.end())
					start = *i;
			}
			if (deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(end);
				if (i != keys.end())
					end = *i;
			}

			// Do a single key clear based on probability or end being randomly chosen to be the same as begin
			// (unlikely)
			if (deterministicRandom()->random01() < clearSingleKeyProbability || end == start) {
				end = keyAfter(start);
			} else if (end < start) {
				std::swap(end, start);
			}

			// Apply clear range to verification map
			++rangeClears;
			KeyRangeRef range(start, end);
			debug_printf("      Mutation:  Clear '%s' to '%s' @%" PRId64 "\n",
			             start.toString().c_str(),
			             end.toString().c_str(),
			             version);
			auto e = written.lower_bound(std::make_pair(start.toString(), 0));
			if (e != written.end()) {
				auto last = e;
				auto eEnd = written.lower_bound(std::make_pair(end.toString(), 0));
				while (e != eEnd) {
					auto w = *e;
					++e;
					// If e key is different from last and last was present then insert clear for last's key at version
					if (last != eEnd &&
					    ((e == eEnd || e->first.first != last->first.first) && last->second.present())) {
						debug_printf(
						    "      Mutation:    Clearing key '%s' @%" PRId64 "\n", last->first.first.c_str(), version);

						keyBytesCleared += last->first.first.size();
						mutationBytes += last->first.first.size();
						mutationBytesThisCommit += last->first.first.size();

						// If the last set was at version then just make it not present
						if (last->first.second == version) {
							last->second.reset();
						} else {
							written[std::make_pair(last->first.first, version)].reset();
						}
					}
					last = e;
				}
			}

			btree->clear(range);

			// Sometimes set the range start after the clear
			if (deterministicRandom()->random01() < clearPostSetProbability) {
				KeyValue kv = randomKV(0, maxValueSize);
				kv.key = range.begin;
				btree->set(kv);
				written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			}
		} else {
			// Set a key
			KeyValue kv = randomKV(maxKeySize, maxValueSize);
			// Sometimes change key to a close previously used key
			if (deterministicRandom()->random01() < .01) {
				auto i = keys.upper_bound(kv.key);
				if (i != keys.end())
					kv.key = StringRef(kv.arena(), *i);
			}

			debug_printf("      Mutation:  Set '%s' -> '%s' @%" PRId64 "\n",
			             kv.key.toString().c_str(),
			             kv.value.toString().c_str(),
			             version);

			++sets;
			keyBytesInserted += kv.key.size();
			valueBytesInserted += kv.value.size();
			mutationBytes += (kv.key.size() + kv.value.size());
			mutationBytesThisCommit += (kv.key.size() + kv.value.size());

			btree->set(kv);
			written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			keys.insert(kv.key);
		}

		// Commit after any limits for this commit or the total test are reached
		if (totalPageOps >= targetPageOps || written.size() >= maxVerificationMapEntries ||
		    mutationBytesThisCommit >= mutationBytesTargetThisCommit) {
			// Wait for previous commit to finish
			wait(commit);
			printf("Last commit complete.  Next commit %d bytes, %" PRId64 " bytes committed so far.",
			       mutationBytesThisCommit,
			       mutationBytes.get() - mutationBytesThisCommit);
			printf("  Stats:  Insert %.2f MB/s  ClearedKeys %.2f MB/s  Total %.2f\n",
			       (keyBytesInserted.rate() + valueBytesInserted.rate()) / 1e6,
			       keyBytesCleared.rate() / 1e6,
			       mutationBytes.rate() / 1e6);

			Version v = version; // Avoid capture of version as a member of *this

			// Sometimes advance the oldest version to close the gap between the oldest and latest versions by a random
			// amount.
			if (deterministicRandom()->random01() < advanceOldVersionProbability) {
				btree->setOldestVersion(btree->getLastCommittedVersion() -
				                        deterministicRandom()->randomInt64(
				                            0, btree->getLastCommittedVersion() - btree->getOldestVersion() + 1));
			}

			commit = map(btree->commit(), [=, &ops = totalPageOps](Void) {
				// Update pager ops before clearing metrics
				ops += g_redwoodMetrics.pageOps();
				printf("Committed %s PageOps %" PRId64 "/%" PRId64 " (%.2f%%) VerificationMapEntries %d/%d (%.2f%%)\n",
				       toString(v).c_str(),
				       ops,
				       targetPageOps,
				       ops * 100.0 / targetPageOps,
				       written.size(),
				       maxVerificationMapEntries,
				       written.size() * 100.0 / maxVerificationMapEntries);
				printf("Committed:\n%s\n", g_redwoodMetrics.toString(true).c_str());

				// Notify the background verifier that version is committed and therefore readable
				committedVersions.send(v);
				return Void();
			});

			if (serialTest) {
				// Wait for commit, wait for verification, then start new verification
				wait(commit);
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
			}

			mutationBytesThisCommit = 0;
			mutationBytesTargetThisCommit = randomSize(maxCommitSize);

			// Recover from disk at random
			if (!pagerMemoryOnly && deterministicRandom()->random01() < coldStartProbability) {
				printf("Recovering from disk after next commit.\n");

				// Wait for outstanding commit
				debug_printf("Waiting for outstanding commit\n");
				wait(commit);

				// Stop and wait for the verifier task
				committedVersions.sendError(end_of_stream());
				debug_printf("Waiting for verification to complete.\n");
				wait(verifyTask);

				debug_printf("Closing btree\n");
				Future<Void> closedFuture = btree->onClosed();
				btree->close();
				wait(closedFuture);

				printf("Reopening btree from disk.\n");
				IPager2* pager = new DWALPager(pageSize, fileName, cacheSizeBytes, remapCleanupWindow);
				btree = new VersionedBTree(pager, fileName);
				wait(btree->init());

				Version v = btree->getLatestVersion();
				printf("Recovered from disk.  Latest recovered version %" PRId64 " highest written version %" PRId64
				       "\n",
				       v,
				       version);
				ASSERT(v == version);

				// Create new promise stream and start the verifier again
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
				randomTask = randomReader(btree) || btree->getError();
				committedVersions.send(v);
			}

			version += versionIncrement;
			btree->setWriteVersion(version);
		}

		// Check for errors
		ASSERT(errorCount == 0);
	}

	debug_printf("Waiting for outstanding commit\n");
	wait(commit);
	committedVersions.sendError(end_of_stream());
	randomTask.cancel();
	debug_printf("Waiting for verification to complete.\n");
	wait(verifyTask);

	// Check for errors
	ASSERT(errorCount == 0);

	// Reopen pager and btree with a remap cleanup window of 0 to reclaim all old pages
	state Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);
	btree = new VersionedBTree(new DWALPager(pageSize, fileName, cacheSizeBytes, 0), fileName);
	wait(btree->init());

	wait(btree->clearAllAndCheckSanity());

	closedFuture = btree->onClosed();
	btree->close();
	debug_printf("Closing.\n");
	wait(closedFuture);

	return Void();
}

ACTOR Future<Void> randomSeeks(VersionedBTree* btree, int count, char firstChar, char lastChar) {
	state Version readVer = btree->getLatestVersion();
	state int c = 0;
	state double readStart = timer();
	printf("Executing %d random seeks\n", count);
	state Reference<IStoreCursor> cur = btree->readAtVersion(readVer);
	while (c < count) {
		state Key k = randomString(20, firstChar, lastChar);
		wait(success(cur->findFirstEqualOrGreater(k)));
		++c;
	}
	double elapsed = timer() - readStart;
	printf("Random seek speed %d/s\n", int(count / elapsed));
	return Void();
}

ACTOR Future<Void> randomScans(VersionedBTree* btree,
                               int count,
                               int width,
                               int readAhead,
                               char firstChar,
                               char lastChar) {
	state Version readVer = btree->getLatestVersion();
	state int c = 0;
	state double readStart = timer();
	printf("Executing %d random scans\n", count);
	state Reference<IStoreCursor> cur = btree->readAtVersion(readVer);
	state bool adaptive = readAhead < 0;
	state int totalScanBytes = 0;
	while (c++ < count) {
		state Key k = randomString(20, firstChar, lastChar);
		wait(success(cur->findFirstEqualOrGreater(k, readAhead)));
		if (adaptive) {
			readAhead = totalScanBytes / c;
		}
		state int w = width;
		while (w > 0 && cur->isValid()) {
			totalScanBytes += cur->getKey().size();
			totalScanBytes += cur->getValue().size();
			wait(cur->next());
			--w;
		}
	}
	double elapsed = timer() - readStart;
	printf("Completed %d scans: readAhead=%d width=%d bytesRead=%d scansRate=%d/s\n",
	       count,
	       readAhead,
	       width,
	       totalScanBytes,
	       int(count / elapsed));
	return Void();
}

TEST_CASE(":/redwood/correctness/pager/cow") {
	state std::string pagerFile = "unittest_pageFile.redwood";
	printf("Deleting old test data\n");
	deleteFile(pagerFile);

	int pageSize = 4096;
	state IPager2* pager = new DWALPager(pageSize, pagerFile, 0, 0);

	wait(success(pager->init()));
	state LogicalPageID id = wait(pager->newPageID());
	Reference<ArenaPage> p = pager->newPageBuffer();
	memset(p->mutate(), (char)id, p->size());
	pager->updatePage(id, p);
	pager->setMetaKey(LiteralStringRef("asdfasdf"));
	wait(pager->commit());
	Reference<ArenaPage> p2 = wait(pager->readPage(id, true));
	printf("%s\n", StringRef(p2->begin(), p2->size()).toHexString().c_str());

	// TODO: Verify reads, do more writes and reads to make this a real pager validator

	Future<Void> onClosed = pager->onClosed();
	pager->close();
	wait(onClosed);

	return Void();
}

TEST_CASE(":/redwood/performance/set") {
	state SignalableActorCollection actors;

	g_redwoodMetricsActor = Void(); // Prevent trace event metrics from starting
	g_redwoodMetrics.clear();

	state std::string fileName = params.get("fileName").orDefault("unittest.redwood");
	state int pageSize = params.getInt("pageSize").orDefault(SERVER_KNOBS->REDWOOD_DEFAULT_PAGE_SIZE);
	state int64_t pageCacheBytes = params.getInt("pageCacheBytes").orDefault(FLOW_KNOBS->PAGE_CACHE_4K);
	state int nodeCount = params.getInt("nodeCount").orDefault(1e9);
	state int maxRecordsPerCommit = params.getInt("maxRecordsPerCommit").orDefault(20000);
	state int maxKVBytesPerCommit = params.getInt("maxKVBytesPerCommit").orDefault(20e6);
	state int64_t kvBytesTarget = params.getInt("kvBytesTarget").orDefault(4e9);
	state int minKeyPrefixBytes = params.getInt("minKeyPrefixBytes").orDefault(25);
	state int maxKeyPrefixBytes = params.getInt("maxKeyPrefixBytes").orDefault(25);
	state int minValueSize = params.getInt("minValueSize").orDefault(100);
	state int maxValueSize = params.getInt("maxValueSize").orDefault(500);
	state int minConsecutiveRun = params.getInt("minConsecutiveRun").orDefault(1);
	state int maxConsecutiveRun = params.getInt("maxConsecutiveRun").orDefault(100);
	state char firstKeyChar = params.get("firstKeyChar").orDefault("a")[0];
	state char lastKeyChar = params.get("lastKeyChar").orDefault("m")[0];
	state Version remapCleanupWindow =
	    params.getInt("remapCleanupWindow").orDefault(SERVER_KNOBS->REDWOOD_REMAP_CLEANUP_WINDOW);
	state bool openExisting = params.getInt("openExisting").orDefault(0);
	state bool insertRecords = !openExisting || params.getInt("insertRecords").orDefault(0);
	state int concurrentSeeks = params.getInt("concurrentSeeks").orDefault(64);
	state int concurrentScans = params.getInt("concurrentScans").orDefault(64);
	state int seeks = params.getInt("seeks").orDefault(1000000);
	state int scans = params.getInt("scans").orDefault(20000);

	printf("pageSize: %d\n", pageSize);
	printf("pageCacheBytes: %" PRId64 "\n", pageCacheBytes);
	printf("trailingIntegerIndexRange: %d\n", nodeCount);
	printf("maxChangesPerCommit: %d\n", maxRecordsPerCommit);
	printf("minKeyPrefixBytes: %d\n", minKeyPrefixBytes);
	printf("maxKeyPrefixBytes: %d\n", maxKeyPrefixBytes);
	printf("minConsecutiveRun: %d\n", minConsecutiveRun);
	printf("maxConsecutiveRun: %d\n", maxConsecutiveRun);
	printf("minValueSize: %d\n", minValueSize);
	printf("maxValueSize: %d\n", maxValueSize);
	printf("maxCommitSize: %d\n", maxKVBytesPerCommit);
	printf("kvBytesTarget: %" PRId64 "\n", kvBytesTarget);
	printf("KeyLexicon '%c' to '%c'\n", firstKeyChar, lastKeyChar);
	printf("remapCleanupWindow: %" PRId64 "\n", remapCleanupWindow);
	printf("concurrentScans: %d\n", concurrentScans);
	printf("concurrentSeeks: %d\n", concurrentSeeks);
	printf("seeks: %d\n", seeks);
	printf("scans: %d\n", scans);
	printf("fileName: %s\n", fileName.c_str());
	printf("openExisting: %d\n", openExisting);
	printf("insertRecords: %d\n", insertRecords);

	if (!openExisting) {
		printf("Deleting old test data\n");
		deleteFile(fileName);
	}

	DWALPager* pager = new DWALPager(pageSize, fileName, pageCacheBytes, remapCleanupWindow);
	state VersionedBTree* btree = new VersionedBTree(pager, fileName);
	wait(btree->init());
	printf("Initialized.  StorageBytes=%s\n", btree->getStorageBytes().toString().c_str());

	state int64_t kvBytesThisCommit = 0;
	state int64_t kvBytesTotal = 0;
	state int recordsThisCommit = 0;
	state Future<Void> commit = Void();
	state std::string value(maxValueSize, 'v');

	printf("Starting.\n");
	state double intervalStart = timer();
	state double start = intervalStart;

	if (insertRecords) {
		while (kvBytesTotal < kvBytesTarget) {
			wait(yield());

			Version lastVer = btree->getLatestVersion();
			state Version version = lastVer + 1;
			btree->setWriteVersion(version);
			state int changesThisVersion =
			    deterministicRandom()->randomInt(0, maxRecordsPerCommit - recordsThisCommit + 1);

			while (changesThisVersion > 0 && kvBytesThisCommit < maxKVBytesPerCommit) {
				KeyValue kv;
				kv.key = randomString(kv.arena(),
				                      deterministicRandom()->randomInt(minKeyPrefixBytes + sizeof(uint32_t),
				                                                       maxKeyPrefixBytes + sizeof(uint32_t) + 1),
				                      firstKeyChar,
				                      lastKeyChar);
				int32_t index = deterministicRandom()->randomInt(0, nodeCount);
				int runLength = deterministicRandom()->randomInt(minConsecutiveRun, maxConsecutiveRun + 1);

				while (runLength > 0 && changesThisVersion > 0) {
					*(uint32_t*)(kv.key.end() - sizeof(uint32_t)) = bigEndian32(index++);
					kv.value = StringRef((uint8_t*)value.data(),
					                     deterministicRandom()->randomInt(minValueSize, maxValueSize + 1));

					btree->set(kv);

					--runLength;
					--changesThisVersion;
					kvBytesThisCommit += kv.key.size() + kv.value.size();
					++recordsThisCommit;
				}

				wait(yield());
			}

			if (kvBytesThisCommit >= maxKVBytesPerCommit || recordsThisCommit >= maxRecordsPerCommit) {
				btree->setOldestVersion(btree->getLastCommittedVersion());
				wait(commit);
				printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n",
				       kvBytesTotal / 1e6,
				       kvBytesTotal / (timer() - start) / 1e6);

				// Avoid capturing via this to freeze counter values
				int recs = recordsThisCommit;
				int kvb = kvBytesThisCommit;

				// Capturing invervalStart via this->intervalStart makes IDE's unhappy as they do not know about the
				// actor state object
				double* pIntervalStart = &intervalStart;

				commit = map(btree->commit(), [=](Void result) {
					printf("Committed:\n%s\n", g_redwoodMetrics.toString(true).c_str());
					double elapsed = timer() - *pIntervalStart;
					printf("Committed %d keyValueBytes in %d records in %f seconds, %.2f MB/s\n",
					       kvb,
					       recs,
					       elapsed,
					       kvb / elapsed / 1e6);
					*pIntervalStart = timer();
					return Void();
				});

				kvBytesTotal += kvBytesThisCommit;
				kvBytesThisCommit = 0;
				recordsThisCommit = 0;
			}
		}

		wait(commit);
		printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n",
		       kvBytesTotal / 1e6,
		       kvBytesTotal / (timer() - start) / 1e6);
		printf("StorageBytes=%s\n", btree->getStorageBytes().toString().c_str());
	}

	printf("Warming cache with seeks\n");
	for (int x = 0; x < concurrentSeeks; ++x) {
		actors.add(randomSeeks(btree, seeks / concurrentSeeks, firstKeyChar, lastKeyChar));
	}
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Serial scans with adaptive readAhead...\n");
	actors.add(randomScans(btree, scans, 50, -1, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Serial scans with readAhead 3 pages...\n");
	actors.add(randomScans(btree, scans, 50, 12000, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Serial scans with readAhead 2 pages...\n");
	actors.add(randomScans(btree, scans, 50, 8000, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Serial scans with readAhead 1 page...\n");
	actors.add(randomScans(btree, scans, 50, 4000, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Serial scans...\n");
	actors.add(randomScans(btree, scans, 50, 0, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Parallel scans, concurrency=%d, no readAhead ...\n", concurrentScans);
	for (int x = 0; x < concurrentScans; ++x) {
		actors.add(randomScans(btree, scans / concurrentScans, 50, 0, firstKeyChar, lastKeyChar));
	}
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Serial seeks...\n");
	actors.add(randomSeeks(btree, seeks, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	printf("Parallel seeks, concurrency=%d ...\n", concurrentSeeks);
	for (int x = 0; x < concurrentSeeks; ++x) {
		actors.add(randomSeeks(btree, seeks / concurrentSeeks, firstKeyChar, lastKeyChar));
	}
	wait(actors.signalAndReset());
	printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str());

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	return Void();
}

struct PrefixSegment {
	int length;
	int cardinality;

	std::string toString() const { return format("{%d bytes, %d choices}", length, cardinality); }
};

// Utility class for generating kv pairs under a prefix pattern
// It currently uses std::string in an abstraction breaking way.
struct KVSource {
	KVSource() {}

	typedef VectorRef<uint8_t> PrefixRef;
	typedef Standalone<PrefixRef> Prefix;

	std::vector<PrefixSegment> desc;
	std::vector<std::vector<std::string>> segments;
	std::vector<Prefix> prefixes;
	std::vector<Prefix*> prefixesSorted;
	std::string valueData;
	int prefixLen;
	int lastIndex;
	// TODO there is probably a better way to do this
	Prefix extraRangePrefix;

	KVSource(const std::vector<PrefixSegment>& desc, int numPrefixes = 0) : desc(desc) {
		if (numPrefixes == 0) {
			numPrefixes = 1;
			for (auto& p : desc) {
				numPrefixes *= p.cardinality;
			}
		}

		prefixLen = 0;
		for (auto& s : desc) {
			prefixLen += s.length;
			std::vector<std::string> parts;
			while (parts.size() < s.cardinality) {
				parts.push_back(deterministicRandom()->randomAlphaNumeric(s.length));
			}
			segments.push_back(std::move(parts));
		}

		while (prefixes.size() < numPrefixes) {
			std::string p;
			for (auto& s : segments) {
				p.append(s[deterministicRandom()->randomInt(0, s.size())]);
			}
			prefixes.push_back(PrefixRef((uint8_t*)p.data(), p.size()));
		}

		for (auto& p : prefixes) {
			prefixesSorted.push_back(&p);
		}
		std::sort(prefixesSorted.begin(), prefixesSorted.end(), [](const Prefix* a, const Prefix* b) {
			return KeyRef((uint8_t*)a->begin(), a->size()) < KeyRef((uint8_t*)b->begin(), b->size());
		});

		valueData = deterministicRandom()->randomAlphaNumeric(100000);
		lastIndex = 0;
	}

	// Expands the chosen prefix in the prefix list to hold suffix,
	// fills suffix with random bytes, and returns a reference to the string
	KeyRef getKeyRef(int suffixLen) { return makeKey(randomPrefix(), suffixLen); }

	// Like getKeyRef but uses the same prefix as the last randomly chosen prefix
	KeyRef getAnotherKeyRef(int suffixLen, bool sorted = false) {
		Prefix& p = sorted ? *prefixesSorted[lastIndex] : prefixes[lastIndex];
		return makeKey(p, suffixLen);
	}

	// Like getKeyRef but gets a KeyRangeRef. If samePrefix, it returns a range from the same prefix,
	// otherwise it returns a random range from the entire keyspace
	// Can technically return an empty range with low probability
	KeyRangeRef getKeyRangeRef(bool samePrefix, int suffixLen, bool sorted = false) {
		KeyRef a, b;

		a = getKeyRef(suffixLen);
		// Copy a so that b's Prefix Arena allocation doesn't overwrite a if using the same prefix
		extraRangePrefix.reserve(extraRangePrefix.arena(), a.size());
		a.copyTo((uint8_t*)extraRangePrefix.begin());
		a = KeyRef(extraRangePrefix.begin(), a.size());

		if (samePrefix) {
			b = getAnotherKeyRef(suffixLen, sorted);
		} else {
			b = getKeyRef(suffixLen);
		}

		if (a < b) {
			return KeyRangeRef(a, b);
		} else {
			return KeyRangeRef(b, a);
		}
	}

	// TODO unused, remove?
	// Like getKeyRef but gets a KeyRangeRef for two keys covering the given number of sorted adjacent prefixes
	KeyRangeRef getRangeRef(int prefixesCovered, int suffixLen) {
		prefixesCovered = std::min<int>(prefixesCovered, prefixes.size());
		int i = deterministicRandom()->randomInt(0, prefixesSorted.size() - prefixesCovered);
		Prefix* begin = prefixesSorted[i];
		Prefix* end = prefixesSorted[i + prefixesCovered];
		return KeyRangeRef(makeKey(*begin, suffixLen), makeKey(*end, suffixLen));
	}

	KeyRef getValue(int len) { return KeyRef(valueData).substr(0, len); }

	// Move lastIndex to the next position, wrapping around to 0
	void nextPrefix() {
		++lastIndex;
		if (lastIndex == prefixes.size()) {
			lastIndex = 0;
		}
	}

	Prefix& randomPrefix() {
		lastIndex = deterministicRandom()->randomInt(0, prefixes.size());
		return prefixes[lastIndex];
	}

	static KeyRef makeKey(Prefix& p, int suffixLen) {
		p.reserve(p.arena(), p.size() + suffixLen);
		uint8_t* wptr = p.end();
		for (int i = 0; i < suffixLen; ++i) {
			*wptr++ = (uint8_t)deterministicRandom()->randomAlphaNumeric();
		}
		return KeyRef(p.begin(), p.size() + suffixLen);
	}

	int numPrefixes() const { return prefixes.size(); };

	std::string toString() const {
		return format("{prefixLen=%d prefixes=%d format=%s}", prefixLen, numPrefixes(), ::toString(desc).c_str());
	}
};

ACTOR Future<StorageBytes> getStableStorageBytes(IKeyValueStore* kvs) {
	state StorageBytes sb = kvs->getStorageBytes();

	// Wait for StorageBytes used metric to stabilize
	loop {
		wait(kvs->commit());
		StorageBytes sb2 = kvs->getStorageBytes();
		bool stable = sb2.used == sb.used;
		sb = sb2;
		if (stable) {
			break;
		}
	}

	return sb;
}

ACTOR Future<Void> prefixClusteredInsert(IKeyValueStore* kvs,
                                         int suffixSize,
                                         int valueSize,
                                         KVSource source,
                                         int recordCountTarget,
                                         bool usePrefixesInOrder,
                                         bool clearAfter) {
	state int commitTarget = 5e6;

	state int recordSize = source.prefixLen + suffixSize + valueSize;
	state int64_t kvBytesTarget = (int64_t)recordCountTarget * recordSize;
	state int recordsPerPrefix = recordCountTarget / source.numPrefixes();

	printf("\nstoreType: %d\n", kvs->getType());
	printf("commitTarget: %d\n", commitTarget);
	printf("prefixSource: %s\n", source.toString().c_str());
	printf("usePrefixesInOrder: %d\n", usePrefixesInOrder);
	printf("suffixSize: %d\n", suffixSize);
	printf("valueSize: %d\n", valueSize);
	printf("recordSize: %d\n", recordSize);
	printf("recordsPerPrefix: %d\n", recordsPerPrefix);
	printf("recordCountTarget: %d\n", recordCountTarget);
	printf("kvBytesTarget: %" PRId64 "\n", kvBytesTarget);

	state int64_t kvBytes = 0;
	state int64_t kvBytesTotal = 0;
	state int records = 0;
	state Future<Void> commit = Void();
	state std::string value = deterministicRandom()->randomAlphaNumeric(1e6);

	wait(kvs->init());

	state double intervalStart = timer();
	state double start = intervalStart;

	state std::function<void()> stats = [&]() {
		double elapsed = timer() - start;
		printf("Cumulative stats: %.2f seconds  %.2f MB keyValue bytes  %d records  %.2f MB/s  %.2f rec/s\r",
		       elapsed,
		       kvBytesTotal / 1e6,
		       records,
		       kvBytesTotal / elapsed / 1e6,
		       records / elapsed);
		fflush(stdout);
	};

	while (kvBytesTotal < kvBytesTarget) {
		wait(yield());

		state int i;
		for (i = 0; i < recordsPerPrefix; ++i) {
			KeyValueRef kv(source.getAnotherKeyRef(suffixSize, usePrefixesInOrder), source.getValue(valueSize));
			kvs->set(kv);
			kvBytes += kv.expectedSize();
			++records;

			if (kvBytes >= commitTarget) {
				wait(commit);
				stats();
				commit = kvs->commit();
				kvBytesTotal += kvBytes;
				if (kvBytesTotal >= kvBytesTarget) {
					break;
				}
				kvBytes = 0;
			}
		}

		// Use every prefix, one at a time
		source.nextPrefix();
	}

	wait(commit);
	// TODO is it desired that not all records are committed? This could commit again to ensure any records set() since
	// the last commit are persisted. For the purposes of how this is used currently, I don't think it matters though
	stats();
	printf("\n");

	intervalStart = timer();
	StorageBytes sb = wait(getStableStorageBytes(kvs));
	printf("storageBytes: %s (stable after %.2f seconds)\n", toString(sb).c_str(), timer() - intervalStart);

	if (clearAfter) {
		printf("Clearing all keys\n");
		intervalStart = timer();
		kvs->clear(KeyRangeRef(LiteralStringRef(""), LiteralStringRef("\xff")));
		state StorageBytes sbClear = wait(getStableStorageBytes(kvs));
		printf("Cleared all keys in %.2f seconds, final storageByte: %s\n",
		       timer() - intervalStart,
		       toString(sbClear).c_str());
	}

	return Void();
}

ACTOR Future<Void> sequentialInsert(IKeyValueStore* kvs, int prefixLen, int valueSize, int recordCountTarget) {
	state int commitTarget = 5e6;

	state KVSource source({ { prefixLen, 1 } });
	state int recordSize = source.prefixLen + sizeof(uint64_t) + valueSize;
	state int64_t kvBytesTarget = (int64_t)recordCountTarget * recordSize;

	printf("\nstoreType: %d\n", kvs->getType());
	printf("commitTarget: %d\n", commitTarget);
	printf("valueSize: %d\n", valueSize);
	printf("recordSize: %d\n", recordSize);
	printf("recordCountTarget: %d\n", recordCountTarget);
	printf("kvBytesTarget: %" PRId64 "\n", kvBytesTarget);

	state int64_t kvBytes = 0;
	state int64_t kvBytesTotal = 0;
	state int records = 0;
	state Future<Void> commit = Void();
	state std::string value = deterministicRandom()->randomAlphaNumeric(1e6);

	wait(kvs->init());

	state double intervalStart = timer();
	state double start = intervalStart;

	state std::function<void()> stats = [&]() {
		double elapsed = timer() - start;
		printf("Cumulative stats: %.2f seconds  %.2f MB keyValue bytes  %d records  %.2f MB/s  %.2f rec/s\r",
		       elapsed,
		       kvBytesTotal / 1e6,
		       records,
		       kvBytesTotal / elapsed / 1e6,
		       records / elapsed);
		fflush(stdout);
	};

	state uint64_t c = 0;
	state Key key = source.getKeyRef(sizeof(uint64_t));

	while (kvBytesTotal < kvBytesTarget) {
		wait(yield());
		*(uint64_t*)(key.end() - sizeof(uint64_t)) = bigEndian64(c);
		KeyValueRef kv(key, source.getValue(valueSize));
		kvs->set(kv);
		kvBytes += kv.expectedSize();
		++records;

		if (kvBytes >= commitTarget) {
			wait(commit);
			stats();
			commit = kvs->commit();
			kvBytesTotal += kvBytes;
			if (kvBytesTotal >= kvBytesTarget) {
				break;
			}
			kvBytes = 0;
		}
		++c;
	}

	wait(commit);
	stats();
	printf("\n");

	return Void();
}

Future<Void> closeKVS(IKeyValueStore* kvs) {
	Future<Void> closed = kvs->onClosed();
	kvs->close();
	return closed;
}

ACTOR Future<Void> doPrefixInsertComparison(int suffixSize,
                                            int valueSize,
                                            int recordCountTarget,
                                            bool usePrefixesInOrder,
                                            KVSource source) {

	deleteFile("test.redwood");
	wait(delay(5));
	state IKeyValueStore* redwood = openKVStore(KeyValueStoreType::SSD_REDWOOD_V1, "test.redwood", UID(), 0);
	wait(prefixClusteredInsert(redwood, suffixSize, valueSize, source, recordCountTarget, usePrefixesInOrder, true));
	wait(closeKVS(redwood));
	printf("\n");

	deleteFile("test.sqlite");
	deleteFile("test.sqlite-wal");
	wait(delay(5));
	state IKeyValueStore* sqlite = openKVStore(KeyValueStoreType::SSD_BTREE_V2, "test.sqlite", UID(), 0);
	wait(prefixClusteredInsert(sqlite, suffixSize, valueSize, source, recordCountTarget, usePrefixesInOrder, true));
	wait(closeKVS(sqlite));
	printf("\n");

	return Void();
}

TEST_CASE(":/redwood/performance/prefixSizeComparison") {
	state int suffixSize = params.getInt("suffixSize").orDefault(12);
	state int valueSize = params.getInt("valueSize").orDefault(100);
	state int recordCountTarget = params.getInt("recordCountTarget").orDefault(100e6);
	state bool usePrefixesInOrder = params.getInt("usePrefixesInOrder").orDefault(0);

	wait(doPrefixInsertComparison(
	    suffixSize, valueSize, recordCountTarget, usePrefixesInOrder, KVSource({ { 10, 100000 } })));
	wait(doPrefixInsertComparison(
	    suffixSize, valueSize, recordCountTarget, usePrefixesInOrder, KVSource({ { 16, 100000 } })));
	wait(doPrefixInsertComparison(
	    suffixSize, valueSize, recordCountTarget, usePrefixesInOrder, KVSource({ { 32, 100000 } })));
	wait(doPrefixInsertComparison(suffixSize,
	                              valueSize,
	                              recordCountTarget,
	                              usePrefixesInOrder,
	                              KVSource({ { 4, 5 }, { 12, 1000 }, { 8, 5 }, { 8, 4 } })));

	return Void();
}

TEST_CASE(":/redwood/performance/sequentialInsert") {
	state int prefixLen = params.getInt("prefixLen").orDefault(30);
	state int valueSize = params.getInt("valueSize").orDefault(100);
	state int recordCountTarget = params.getInt("recordCountTarget").orDefault(100e6);

	deleteFile("test.redwood");
	wait(delay(5));
	state IKeyValueStore* redwood = openKVStore(KeyValueStoreType::SSD_REDWOOD_V1, "test.redwood", UID(), 0);
	wait(sequentialInsert(redwood, prefixLen, valueSize, recordCountTarget));
	wait(closeKVS(redwood));
	printf("\n");

	return Void();
}

// singlePrefix forces the range read to have the start and end key with the same prefix
ACTOR Future<Void> randomRangeScans(IKeyValueStore* kvs,
                                    int suffixSize,
                                    KVSource source,
                                    int valueSize,
                                    int recordCountTarget,
                                    bool singlePrefix,
                                    int rowLimit) {
	printf("\nstoreType: %d\n", kvs->getType());
	printf("prefixSource: %s\n", source.toString().c_str());
	printf("suffixSize: %d\n", suffixSize);
	printf("recordCountTarget: %d\n", recordCountTarget);
	printf("singlePrefix: %d\n", singlePrefix);
	printf("rowLimit: %d\n", rowLimit);

	state int64_t recordSize = source.prefixLen + suffixSize + valueSize;
	state int64_t bytesRead = 0;
	state int64_t recordsRead = 0;
	state int queries = 0;
	state int64_t nextPrintRecords = 1e5;

	state double start = timer();
	state std::function<void()> stats = [&]() {
		double elapsed = timer() - start;
		printf("Cumulative stats: %.2f seconds  %d queries %.2f MB %d records  %.2f qps %.2f MB/s  %.2f rec/s\r\n",
		       elapsed,
		       queries,
		       bytesRead / 1e6,
		       recordsRead,
		       queries / elapsed,
		       bytesRead / elapsed / 1e6,
		       recordsRead / elapsed);
		fflush(stdout);
	};

	while (recordsRead < recordCountTarget) {
		KeyRangeRef range = source.getKeyRangeRef(singlePrefix, suffixSize);
		int rowLim = (deterministicRandom()->randomInt(0, 2) != 0) ? rowLimit : -rowLimit;

		RangeResult result = wait(kvs->readRange(range, rowLim));

		recordsRead += result.size();
		bytesRead += result.size() * recordSize;
		++queries;

		// log stats with exponential backoff
		if (recordsRead >= nextPrintRecords) {
			stats();
			nextPrintRecords *= 2;
		}
	}

	stats();
	printf("\n");

	return Void();
}

TEST_CASE("!/redwood/performance/randomRangeScans") {
	state int prefixLen = 30;
	state int suffixSize = 12;
	state int valueSize = 100;

	// TODO change to 100e8 after figuring out no-disk redwood mode
	state int writeRecordCountTarget = 1e6;
	state int queryRecordTarget = 1e7;
	state int writePrefixesInOrder = false;

	state KVSource source({ { prefixLen, 1000 } });

	deleteFile("test.redwood");
	wait(delay(5));
	state IKeyValueStore* redwood = openKVStore(KeyValueStoreType::SSD_REDWOOD_V1, "test.redwood", UID(), 0);
	wait(prefixClusteredInsert(
	    redwood, suffixSize, valueSize, source, writeRecordCountTarget, writePrefixesInOrder, false));

	// divide targets for tiny queries by 10 because they are much slower
	wait(randomRangeScans(redwood, suffixSize, source, valueSize, queryRecordTarget / 10, true, 10));
	wait(randomRangeScans(redwood, suffixSize, source, valueSize, queryRecordTarget, true, 1000));
	wait(randomRangeScans(redwood, suffixSize, source, valueSize, queryRecordTarget / 10, false, 100));
	wait(randomRangeScans(redwood, suffixSize, source, valueSize, queryRecordTarget, false, 10000));
	wait(randomRangeScans(redwood, suffixSize, source, valueSize, queryRecordTarget, false, 1000000));
	wait(closeKVS(redwood));
	printf("\n");

	return Void();
}
