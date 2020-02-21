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
#include "fdbrpc/crc32c.h"
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

// Some convenience functions for debugging to stringify various structures
// Classes can add compatibility by either specializing toString<T> or implementing
//   std::string toString() const;
template<typename T>
std::string toString(const T &o) {
	return o.toString();
}

std::string toString(StringRef s) {
	return s.printable();
}

std::string toString(LogicalPageID id) {
	if(id == invalidLogicalPageID) {
		return "LogicalPageID{invalid}";
	}
	return format("LogicalPageID{%" PRId64 "}", id);
}

template<typename T>
std::string toString(const Standalone<T> &s) {
	return toString((T)s);
}

template<typename T>
std::string toString(const T *begin, const T *end) {
	std::string r = "{";

	bool comma = false;
	while(begin != end) {
		if(comma) {
			r += ", ";
		}
		else {
			comma = true;
		}
		r += toString(*begin++);
	}

	r += "}";
	return r;
}

template<typename T>
std::string toString(const std::vector<T> &v) {
	return toString(v.begin(), v.end());
}

template<typename T>
std::string toString(const VectorRef<T> &v) {
	return toString(v.begin(), v.end());
}

template<typename T>
std::string toString(const Optional<T> &o) {
	if(o.present()) {
		return toString(o.get());
	}
	return "<not present>";
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
template<typename T, typename Enable = void>
struct FIFOQueueCodec {
	static T readFromBytes(const uint8_t *src, int &bytesRead) {
		T x;
		bytesRead = x.readFromBytes(src);
		return x;
	}
	static int bytesNeeded(const T &x) {
		return x.bytesNeeded();
	}
	static int writeToBytes(uint8_t *dst, const T &x) {
		return x.writeToBytes(dst);
	}
};

template<typename T>
struct FIFOQueueCodec<T, typename std::enable_if<std::is_trivially_copyable<T>::value>::type> {
	static_assert(std::is_trivially_copyable<T>::value);
	static T readFromBytes(const uint8_t *src, int &bytesRead) {
		bytesRead = sizeof(T);
		return *(T *)src;
	}
	static int bytesNeeded(const T &x) {
		return sizeof(T);
	}
	static int writeToBytes(uint8_t *dst, const T &x) {
		*(T *)dst = x;
		return sizeof(T);
	}
};

template<typename T, typename Codec = FIFOQueueCodec<T>>
class FIFOQueue {
public:
#pragma pack(push, 1)
	struct QueueState {
		bool operator==(const QueueState &rhs) const {
			return memcmp(this, &rhs, sizeof(QueueState)) == 0;
		}
		LogicalPageID headPageID = invalidLogicalPageID;
		LogicalPageID tailPageID = invalidLogicalPageID;
		uint16_t headOffset;
		// Note that there is no tail index because the tail page is always never-before-written and its index will start at 0
		int64_t numPages;
		int64_t numEntries;
		std::string toString() const {
			return format("{head: %s:%d  tail: %s  numPages: %" PRId64 "  numEntries: %" PRId64 "}", ::toString(headPageID).c_str(), (int)headOffset, ::toString(tailPageID).c_str(), numPages, numEntries);
		}
	};
#pragma pack(pop)

	struct Cursor {
		enum Mode {
			NONE,
			POP,
			READONLY,
			WRITE
		};

		// The current page being read or written to
		LogicalPageID pageID;

		// The first page ID to be written to the pager, if this cursor has written anything
		LogicalPageID firstPageIDWritten;

		// Offset after RawPage header to next read from or write to 
		int offset;

		// A read cursor will not read this page (or beyond)
		LogicalPageID endPageID;

		Reference<IPage> page;
		FIFOQueue *queue;
		Future<Void> operation;
		Mode mode;

		Cursor() : mode(NONE) {
		}

		// Initialize a cursor.  
		void init(FIFOQueue *q = nullptr, Mode m = NONE, LogicalPageID initialPageID = invalidLogicalPageID, int readOffset = 0, LogicalPageID endPage = invalidLogicalPageID) {
			if(operation.isValid()) {
				operation.cancel();
			}
			queue = q;
			mode = m;
			firstPageIDWritten = invalidLogicalPageID;
			offset = readOffset;
			endPageID = endPage;
			page.clear();

			if(mode == POP || mode == READONLY) {
				// If cursor is not pointed at the end page then start loading it.
				// The end page will not have been written to disk yet.
				pageID = initialPageID;
				operation = (pageID == endPageID) ? Void() : loadPage();
			}
			else {
				pageID = invalidLogicalPageID;
				ASSERT(mode == WRITE || (initialPageID == invalidLogicalPageID && readOffset == 0 && endPage == invalidLogicalPageID));
				operation = Void();
			}

			debug_printf("FIFOQueue::Cursor(%s) initialized\n", toString().c_str());

			if(mode == WRITE && initialPageID != invalidLogicalPageID) {
				addNewPage(initialPageID, 0, true);
			}
		}

		// Since cursors can have async operations pending which modify their state they can't be copied cleanly
		Cursor(const Cursor &other) = delete;

		// A read cursor can be initialized from a pop cursor
		void initReadOnly(const Cursor &c) {
			ASSERT(c.mode == READONLY || c.mode == POP);
			init(c.queue, READONLY, c.pageID, c.offset, c.endPageID);
		}

		~Cursor() {
			operation.cancel();
		}

		std::string toString() const {
			if(mode == WRITE) {
				return format("{WriteCursor %s:%p pos=%s:%d endOffset=%d}", queue->name.c_str(), this, ::toString(pageID).c_str(), offset, page ? raw()->endOffset : -1);
			}
			if(mode == POP || mode == READONLY) {
				return format("{ReadCursor %s:%p pos=%s:%d endOffset=%d endPage=%s}", queue->name.c_str(), this, ::toString(pageID).c_str(), offset, page ? raw()->endOffset : -1, ::toString(endPageID).c_str());
			}
			ASSERT(mode == NONE);
			return format("{NullCursor=%p}", this);
		}

#pragma pack(push, 1)
		struct RawPage {
			LogicalPageID nextPageID;
			uint16_t nextOffset;
			uint16_t endOffset;
			uint8_t * begin() {
				return (uint8_t *)(this + 1);
			}
		};
#pragma pack(pop)

		Future<Void> notBusy() {
			return operation;
		}

		// Returns true if any items have been written to the last page
		bool pendingWrites() const {
			return mode == WRITE && offset != 0;
		}

		RawPage * raw() const {
			return ((RawPage *)(page->begin()));
		}

		void setNext(LogicalPageID pageID, int offset) {
			ASSERT(mode == WRITE);
			RawPage *p = raw();
			p->nextPageID = pageID;
			p->nextOffset = offset;
		}

		Future<Void> loadPage() {
			ASSERT(mode == POP | mode == READONLY);
			debug_printf("FIFOQueue::Cursor(%s) loadPage\n", toString().c_str());
			return map(queue->pager->readPage(pageID, true), [=](Reference<IPage> p) {
				page = p;
				debug_printf("FIFOQueue::Cursor(%s) loadPage done\n", toString().c_str());
				return Void();
			});
		}

		void writePage() {
			ASSERT(mode == WRITE);
			debug_printf("FIFOQueue::Cursor(%s) writePage\n", toString().c_str());
			VALGRIND_MAKE_MEM_DEFINED(raw()->begin(), offset);
			VALGRIND_MAKE_MEM_DEFINED(raw()->begin() + offset, queue->dataBytesPerPage - raw()->endOffset);
			queue->pager->updatePage(pageID, page);
			if(firstPageIDWritten == invalidLogicalPageID) {
				firstPageIDWritten = pageID;
			}
		}

		// Link the current page to newPageID:newOffset and then write it to the pager.
		// If initializeNewPage is true a page buffer will be allocated for the new page and it will be initialized 
		// as a new tail page.
		void addNewPage(LogicalPageID newPageID, int newOffset, bool initializeNewPage) {
			ASSERT(mode == WRITE);
			ASSERT(newPageID != invalidLogicalPageID);
			debug_printf("FIFOQueue::Cursor(%s) Adding page %s init=%d\n", toString().c_str(), ::toString(newPageID).c_str(), initializeNewPage);

			// Update existing page and write, if it exists
			if(page) {
				setNext(newPageID, newOffset);
				debug_printf("FIFOQueue::Cursor(%s) Linked new page\n", toString().c_str());
				writePage();
			}

			pageID = newPageID;
			offset = newOffset;

			if(initializeNewPage) {
				debug_printf("FIFOQueue::Cursor(%s) Initializing new page\n", toString().c_str());
				page = queue->pager->newPageBuffer();
				setNext(0, 0);
				auto p = raw();
				ASSERT(newOffset == 0);
				p->endOffset = 0;
			}
			else {
				page.clear();
			}
		}

		// Write item to the next position in the current page or, if it won't fit, add a new page and write it there.
		ACTOR static Future<Void> write_impl(Cursor *self, T item, Future<Void> start) {
			ASSERT(self->mode == WRITE);

			// Wait for the previous operation to finish
			state Future<Void> previous = self->operation;
			wait(start);
			wait(previous);

			state int bytesNeeded = Codec::bytesNeeded(item);
			if(self->pageID == invalidLogicalPageID || self->offset + bytesNeeded > self->queue->dataBytesPerPage) {
				debug_printf("FIFOQueue::Cursor(%s) write(%s) page is full, adding new page\n", self->toString().c_str(), ::toString(item).c_str());
				LogicalPageID newPageID = wait(self->queue->pager->newPageID());
				self->addNewPage(newPageID, 0, true);
				++self->queue->numPages;
				wait(yield());
			}
			debug_printf("FIFOQueue::Cursor(%s) before write(%s)\n", self->toString().c_str(), ::toString(item).c_str());
			auto p = self->raw();
			Codec::writeToBytes(p->begin() + self->offset, item);
			self->offset += bytesNeeded;
			p->endOffset = self->offset;
			++self->queue->numEntries;
			return Void();
		}

		void write(const T &item) {
			Promise<Void> p;
			operation = write_impl(this, item, p.getFuture());
			p.send(Void());
		}

		// Read the next item at the cursor (if <= upperBound), moving to a new page first if the current page is exhausted
		ACTOR static Future<Optional<T>> readNext_impl(Cursor *self, Optional<T> upperBound, Future<Void> start) {
			ASSERT(self->mode == POP || self->mode == READONLY);

			// Wait for the previous operation to finish
			state Future<Void> previous = self->operation;
			wait(start);
			wait(previous);

			debug_printf("FIFOQueue::Cursor(%s) readNext begin\n", self->toString().c_str());
			if(self->pageID == invalidLogicalPageID || self->pageID == self->endPageID) {
				debug_printf("FIFOQueue::Cursor(%s) readNext returning nothing\n", self->toString().c_str());
				return Optional<T>();
			}

			// We now know we are pointing to PageID and it should be read and used, but it may not be loaded yet.
			if(!self->page) {
				wait(self->loadPage());
				wait(yield());
			}

			auto p = self->raw();
			debug_printf("FIFOQueue::Cursor(%s) readNext reading at current position\n", self->toString().c_str());
			ASSERT(self->offset < p->endOffset);
			int bytesRead;
			T result = Codec::readFromBytes(p->begin() + self->offset, bytesRead);

			if(upperBound.present() && upperBound.get() < result) {
				debug_printf("FIFOQueue::Cursor(%s) not popping %s, exceeds upper bound %s\n",
					self->toString().c_str(), ::toString(result).c_str(), ::toString(upperBound.get()).c_str());
				return Optional<T>();
			}

			self->offset += bytesRead;
			if(self->mode == POP) {
				--self->queue->numEntries;
			}
			debug_printf("FIFOQueue::Cursor(%s) after read of %s\n", self->toString().c_str(), ::toString(result).c_str());
			ASSERT(self->offset <= p->endOffset);

			if(self->offset == p->endOffset) {
				debug_printf("FIFOQueue::Cursor(%s) Page exhausted\n", self->toString().c_str());
				LogicalPageID oldPageID = self->pageID;
				self->pageID = p->nextPageID;
				self->offset = p->nextOffset;
				if(self->mode == POP) {
					--self->queue->numPages;
				}
				self->page.clear();
				debug_printf("FIFOQueue::Cursor(%s) readNext page exhausted, moved to new page\n", self->toString().c_str());

				if(self->mode == POP) {
					// Freeing the old page must happen after advancing the cursor and clearing the page reference because
					// freePage() could cause a push onto a queue that causes a newPageID() call which could pop() from this
					// very same queue.
					// Queue pages are freed at page 0 because they can be reused after the next commit.
					self->queue->pager->freePage(oldPageID, 0);
				}
			}

			debug_printf("FIFOQueue(%s) %s(upperBound=%s) -> %s\n", self->queue->name.c_str(), (self->mode == POP ? "pop" : "peek"), ::toString(upperBound).c_str(), ::toString(result).c_str());
			return result;
		}

		// Read and move past the next item if is <= upperBound or if upperBound is not present
		Future<Optional<T>> readNext(const Optional<T> &upperBound = {}) {
			if(mode == NONE) {
				return Optional<T>();
			}
			Promise<Void> p;
			Future<Optional<T>> read = readNext_impl(this, upperBound, p.getFuture());
			operation = success(read);
			p.send(Void());
			return read;
		}
	};

public:
	FIFOQueue() : pager(nullptr) {
	}

	~FIFOQueue() {
		newTailPage.cancel();
	}

	FIFOQueue(const FIFOQueue &other) = delete;
	void operator=(const FIFOQueue &rhs) = delete;

	// Create a new queue at newPageID
	void create(IPager2 *p, LogicalPageID newPageID, std::string queueName) {
		debug_printf("FIFOQueue(%s) create from page %s\n", queueName.c_str(), toString(newPageID).c_str());
		pager = p;
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
	void recover(IPager2 *p, const QueueState &qs, std::string queueName) {
		debug_printf("FIFOQueue(%s) recover from queue state %s\n", queueName.c_str(), qs.toString().c_str());
		pager = p;
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

	ACTOR static Future<Standalone<VectorRef<T>>> peekAll_impl(FIFOQueue *self) {
		state Standalone<VectorRef<T>> results;
		state Cursor c;
		c.initReadOnly(self->headReader);
		results.reserve(results.arena(), self->numEntries);

		loop {
			Optional<T> x = wait(c.readNext());
			if(!x.present()) {
				break;
			}
			results.push_back(results.arena(), x.get());
		}

		return results;
	}

	Future<Standalone<VectorRef<T>>> peekAll() {
		return peekAll_impl(this);
	}

	// Pop the next item on front of queue if it is <= upperBound or if upperBound is not present
	Future<Optional<T>> pop(Optional<T> upperBound = {}) {
		return headReader.readNext(upperBound);
	}

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

	void pushBack(const T &item) {
		debug_printf("FIFOQueue(%s) pushBack(%s)\n", name.c_str(), toString(item).c_str());
		tailWriter.write(item);
	}

	void pushFront(const T &item) {
		debug_printf("FIFOQueue(%s) pushFront(%s)\n", name.c_str(), toString(item).c_str());
		headWriter.write(item);
	}

	// Wait until the most recently started operations on each cursor as of now are ready
	Future<Void> notBusy() {
		return headWriter.notBusy() && headReader.notBusy() && tailWriter.notBusy() && ready(newTailPage);
	}

	// Returns true if any most recently started operations on any cursors are not ready
	bool busy() {
		return !headWriter.notBusy().isReady() || !headReader.notBusy().isReady() || !tailWriter.notBusy().isReady() || !newTailPage.isReady();
	}

	// preFlush() prepares this queue to be flushed to disk, but doesn't actually do it so the queue can still
	// be pushed and popped after this operation. It returns whether or not any operations were pending or
	// started during execution.
	//
	// If one or more queues are used by their pager in newPageID() or freePage() operations, then preFlush()
	// must be called on each of them inside a loop that runs until each of the preFlush() calls have returned
	// false.
	//
	// The reason for all this is that:
	//   - queue pop() can call pager->freePage() which can call push() on the same or another queue
	//   - queue push() can call pager->newPageID() which can call pop() on the same or another queue
	// This creates a circular dependency with 1 or more queues when those queues are used by the pager
	// to manage free page IDs.
	ACTOR static Future<bool> preFlush_impl(FIFOQueue *self) {
		debug_printf("FIFOQueue(%s) preFlush begin\n", self->name.c_str());
		wait(self->notBusy());

		// Completion of the pending operations as of the start of notBusy() could have began new operations,
		// so see if any work is pending now.
		bool workPending = self->busy();

		if(!workPending) {
			// A newly created or flushed queue starts out in a state where its tail page to be written to is empty.
			// After pushBack() is called, this is no longer the case and never will be again until the queue is flushed.
			// Before the non-empty tail page is written it must be linked to a new empty page for use after the next
			// flush.  (This is explained more at the top of FIFOQueue but it is because queue pages can only be written
			// once because once they contain durable data a second write to link to a new page could corrupt the existing
			// data if the subsequent commit never succeeds.)
			if(self->newTailPage.isReady() && self->newTailPage.get() == invalidLogicalPageID && self->tailWriter.pendingWrites()) {
				self->newTailPage = self->pager->newPageID();
				workPending = true;
			}
		}

		debug_printf("FIFOQueue(%s) preFlush returning %d\n", self->name.c_str(), workPending);
		return workPending;
	}

	Future<bool> preFlush() {
		return preFlush_impl(this);
	}

	void finishFlush() {
		debug_printf("FIFOQueue(%s) finishFlush start\n", name.c_str());
		ASSERT(!busy());

		// If a new tail page was allocated, link the last page of the tail writer to it.
		if(newTailPage.get() != invalidLogicalPageID) {
			tailWriter.addNewPage(newTailPage.get(), 0, false);
			// The flush sequence allocated a page and added it to the queue so increment numPages
			++numPages;

			// newPage() should be ready immediately since a pageID is being explicitly passed.
			ASSERT(tailWriter.notBusy().isReady());

			newTailPage = invalidLogicalPageID;
		}

		// If the headWriter wrote anything, link its tail page to the headReader position and point the headReader
		// to the start of the headWriter
		if(headWriter.pendingWrites()) {
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

	ACTOR static Future<Void> flush_impl(FIFOQueue *self) {
		loop {
			bool notDone = wait(self->preFlush());
			if(!notDone) {
				break;
			}
		}
		self->finishFlush();
		return Void();
	}

	Future<Void> flush() {
		return flush_impl(this);
	}

	IPager2 *pager;
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

class FastAllocatedPage : public IPage, public FastAllocated<FastAllocatedPage>, ReferenceCounted<FastAllocatedPage> {
public:
	// Create a fast-allocated page with size total bytes INCLUDING checksum
	FastAllocatedPage(int size, int bufferSize) : logicalSize(size), bufferSize(bufferSize) {
		buffer = (uint8_t *)allocateFast(bufferSize);
		// Mark any unused page portion defined
		VALGRIND_MAKE_MEM_DEFINED(buffer + logicalSize, bufferSize - logicalSize);
	};

	virtual ~FastAllocatedPage() {
		freeFast(bufferSize, buffer);
	}

	// Usable size, without checksum
	int size() const {
		return logicalSize - sizeof(Checksum);
	}

	uint8_t const* begin() const {
		return buffer;
	}

	uint8_t* mutate() {
		return buffer;
	}

	void addref() const {
		ReferenceCounted<FastAllocatedPage>::addref();
	}

	void delref() const {
		ReferenceCounted<FastAllocatedPage>::delref();
	}
	
	typedef uint32_t Checksum;

	Checksum & getChecksum() {
		return *(Checksum *)(buffer + size());
	}

	Checksum calculateChecksum(LogicalPageID pageID) {
		return crc32c_append(pageID, buffer, size());
	}

	void updateChecksum(LogicalPageID pageID) {
		getChecksum() = calculateChecksum(pageID);
	}

	bool verifyChecksum(LogicalPageID pageID) {
		return getChecksum() == calculateChecksum(pageID);
	}
private:
	int logicalSize;
	int bufferSize;
	uint8_t *buffer;
};

// Holds an index of recently used objects.
// ObjectType must have the method
//   bool evictable() const;            // return true if the entry can be evicted
//   Future<Void> onEvictable() const;  // ready when entry can be evicted
// indicating if it is safe to evict.
template<class IndexType, class ObjectType>
class ObjectCache : NonCopyable {

	struct Entry : public boost::intrusive::list_base_hook<> {
		Entry() : hits(0) {
		}
		IndexType index;
		ObjectType item;
		int hits;
	};

public:
	ObjectCache(int sizeLimit = 0) : sizeLimit(sizeLimit), cacheHits(0), cacheMisses(0), noHitEvictions(0) {
	}

	void setSizeLimit(int n) {
		sizeLimit = n;
	}

	// Get the object for i if it exists, else return nullptr.
	// If the object exists, its eviction order will NOT change as this is not a cache hit.
	ObjectType * getIfExists(const IndexType &index) {
		auto i = cache.find(index);
		if(i != cache.end()) {
			++i->second.hits;
			return &i->second.item;
		}
		return nullptr;
	}

	// Get the object for i or create a new one.
	// After a get(), the object for i is the last in evictionOrder.
	ObjectType & get(const IndexType &index, bool noHit = false) {
		Entry &entry = cache[index];

		// If entry is linked into evictionOrder then move it to the back of the order
		if(entry.is_linked()) {
			if(!noHit) {
				++entry.hits;
				++cacheHits;
			}
			// Move the entry to the back of the eviction order
			evictionOrder.erase(evictionOrder.iterator_to(entry));
			evictionOrder.push_back(entry);
		}
		else {
			++cacheMisses;
			// Finish initializing entry
			entry.index = index;
			entry.hits = noHit ? 0 : 1;
			// Insert the newly created Entry at the back of the eviction order
			evictionOrder.push_back(entry);

			// If the cache is too big, try to evict the first Entry in the eviction order
			if(cache.size() > sizeLimit) {
				Entry &toEvict = evictionOrder.front();
				debug_printf("Trying to evict %s to make room for %s\n", toString(toEvict.index).c_str(), toString(index).c_str());
				// Don't evict the entry that was just added as then we can't return a reference to it.
				if(toEvict.index != index && toEvict.item.evictable()) {
					if(toEvict.hits == 0) {
						++noHitEvictions;
					}
					debug_printf("Evicting %s to make room for %s\n", toString(toEvict.index).c_str(), toString(index).c_str());
					evictionOrder.pop_front();
					cache.erase(toEvict.index);
				}
			}
		}

		return entry.item;
	}

	// Clears the cache, saving the entries, and then waits for eachWaits for each item to be evictable and evicts it.
	// The cache should not be Evicts all evictable entries
	ACTOR static Future<Void> clear_impl(ObjectCache *self) {
		state std::unordered_map<IndexType, Entry> cache;
		state boost::intrusive::list<Entry> evictionOrder;

		// Swap cache contents to local state vars
		cache.swap(self->cache);
		evictionOrder.swap(self->evictionOrder);

		state typename boost::intrusive::list<Entry>::iterator i = evictionOrder.begin();
		state typename boost::intrusive::list<Entry>::iterator iEnd = evictionOrder.begin();

		while(i != iEnd) {
			if(!i->item.evictable()) {
				wait(i->item.onEvictable());
			}
			++i;
		}

		evictionOrder.clear();
		cache.clear();

		return Void();
	}

	Future<Void> clear() {
		return clear_impl(this);
	}

	int count() const {
		ASSERT(evictionOrder.size() == cache.size());
		return evictionOrder.size();
	}

private:
	int64_t sizeLimit;
	int64_t cacheHits;
	int64_t cacheMisses;
	int64_t noHitEvictions;

	// TODO:  Use boost intrusive unordered set instead, with a comparator that only considers entry.index
	std::unordered_map<IndexType, Entry> cache;
	boost::intrusive::list<Entry> evictionOrder;
};

ACTOR template<class T> Future<T> forwardError(Future<T> f, Promise<Void> target) {
	try {
		T x = wait(f);
		return x;
	}
	catch(Error &e) {
		if(e.code() != error_code_actor_cancelled && target.canBeSet()) {
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
	typedef FastAllocatedPage Page;
	typedef FIFOQueue<LogicalPageID> LogicalPageQueueT;

#pragma pack(push, 1)
	struct DelayedFreePage {
		Version version;
		LogicalPageID pageID;

		bool operator<(const DelayedFreePage &rhs) const {
			return version < rhs.version;
		}

		std::string toString() const {
			return format("DelayedFreePage{%s @%" PRId64 "}", ::toString(pageID).c_str(), version);
		}
	};

	struct RemappedPage {
		Version version;
		LogicalPageID originalPageID;
		LogicalPageID newPageID;

		bool operator<(const RemappedPage &rhs) {
			return version < rhs.version;
		}

		std::string toString() const {
			return format("RemappedPage(%s -> %s @%" PRId64 "}", ::toString(originalPageID).c_str(), ::toString(newPageID).c_str(), version);
		}
	};

#pragma pack(pop)

	typedef FIFOQueue<DelayedFreePage> DelayedFreePageQueueT;
	typedef FIFOQueue<RemappedPage> RemapQueueT;

	// If the file already exists, pageSize might be different than desiredPageSize
	// Use pageCacheSizeBytes == 0 for default
	DWALPager(int desiredPageSize, std::string filename, int64_t pageCacheSizeBytes)
		: desiredPageSize(desiredPageSize), filename(filename), pHeader(nullptr), pageCacheBytes(pageCacheSizeBytes)
 	{
		if(pageCacheBytes == 0) {
			pageCacheBytes = g_network->isSimulated() ? (BUGGIFY ? FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_4K : FLOW_KNOBS->SIM_PAGE_CACHE_4K) : FLOW_KNOBS->PAGE_CACHE_4K;
		}
		commitFuture = Void();
		recoverFuture = forwardError(recover(this), errorPromise);
	}

	void setPageSize(int size) {
		logicalPageSize = size;
		physicalPageSize = smallestPhysicalBlock;
		while(logicalPageSize > physicalPageSize) {
			physicalPageSize += smallestPhysicalBlock;
		}
		if(pHeader != nullptr) {
			pHeader->pageSize = logicalPageSize;
		}
		pageCache.setSizeLimit(pageCacheBytes / physicalPageSize);
	}

	void updateCommittedHeader() {
		memcpy(lastCommittedHeaderPage->mutate(), headerPage->begin(), smallestPhysicalBlock);
	}

	ACTOR static Future<Void> recover(DWALPager *self) {
		ASSERT(!self->recoverFuture.isValid());

		self->remapUndoFuture = Void();

		int64_t flags = IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_LOCK;
		state bool exists = fileExists(self->filename);
		if(!exists) {
			flags |= IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE;
		}

		wait(store(self->pageFile, IAsyncFileSystem::filesystem()->open(self->filename, flags, 0644)));

		// Header page is always treated as having a page size of smallestPhysicalBlock
		self->setPageSize(smallestPhysicalBlock);
		self->lastCommittedHeaderPage = self->newPageBuffer();
		self->pLastCommittedHeader = (Header *)self->lastCommittedHeaderPage->begin();

		state int64_t fileSize = 0;
		if(exists) {
			wait(store(fileSize, self->pageFile->size()));
		}

		debug_printf("DWALPager(%s) recover exists=%d fileSize=%" PRId64 "\n", self->filename.c_str(), exists, fileSize);
		// TODO:  If the file exists but appears to never have been successfully committed is this an error or
		// should recovery proceed with a new pager instance?

		// If there are at least 2 pages then try to recover the existing file
		if(exists && fileSize >= (self->smallestPhysicalBlock * 2)) {
			debug_printf("DWALPager(%s) recovering using existing file\n");

			state bool recoveredHeader = false;

			// Read physical page 0 directly
			wait(store(self->headerPage, self->readHeaderPage(self, 0)));

			// If the checksum fails for the header page, try to recover committed header backup from page 1
			if(!self->headerPage.castTo<Page>()->verifyChecksum(0)) {
				TraceEvent(SevWarn, "DWALPagerRecoveringHeader").detail("Filename", self->filename);
	
				wait(store(self->headerPage, self->readHeaderPage(self, 1)));

				if(!self->headerPage.castTo<Page>()->verifyChecksum(1)) {
					if(g_network->isSimulated()) {
						// TODO: Detect if process is being restarted and only throw injected if so?
						throw io_error().asInjectedFault();
					}

					Error e = checksum_failed();
					TraceEvent(SevError, "DWALPagerRecoveryFailed")
						.detail("Filename", self->filename)
						.error(e);
					throw e;
				}
				recoveredHeader = true;
			}

			self->pHeader = (Header *)self->headerPage->begin();

			if(self->pHeader->formatVersion != Header::FORMAT_VERSION) {
				Error e = internal_error();  // TODO:  Something better?
				TraceEvent(SevError, "DWALPagerRecoveryFailedWrongVersion")
					.detail("Filename", self->filename)
					.detail("Version", self->pHeader->formatVersion)
					.detail("ExpectedVersion", Header::FORMAT_VERSION)
					.error(e);
				throw e;
			}

			self->setPageSize(self->pHeader->pageSize);
			if(self->logicalPageSize != self->desiredPageSize) {
				TraceEvent(SevWarn, "DWALPagerPageSizeNotDesired")
					.detail("Filename", self->filename)
					.detail("ExistingPageSize", self->logicalPageSize)
					.detail("DesiredPageSize", self->desiredPageSize);
			}

			self->freeList.recover(self, self->pHeader->freeList, "FreeListRecovered");
			self->delayedFreeList.recover(self, self->pHeader->delayedFreeList, "DelayedFreeListRecovered");
			self->remapQueue.recover(self, self->pHeader->remapQueue, "RemapQueueRecovered");

			Standalone<VectorRef<RemappedPage>> remaps = wait(self->remapQueue.peekAll());
			for(auto &r : remaps) {
				if(r.newPageID != invalidLogicalPageID) {
					self->remappedPages[r.originalPageID][r.version] = r.newPageID;
				}
			}

			// If the header was recovered from the backup at Page 1 then write and sync it to Page 0 before continuing.
			// If this fails, the backup header is still in tact for the next recovery attempt.
			if(recoveredHeader) {
				// Write the header to page 0
				wait(self->writeHeaderPage(0, self->headerPage));

				// Wait for all outstanding writes to complete
				wait(self->operations.signalAndCollapse());

				// Sync header
				wait(self->pageFile->sync());
				debug_printf("DWALPager(%s) Header recovery complete.\n", self->filename.c_str());
			}

			// Update the last committed header with the one that was recovered (which is the last known committed header)
			self->updateCommittedHeader();
			self->addLatestSnapshot();
		}
		else {
			// Note: If the file contains less than 2 pages but more than 0 bytes then the pager was never successfully committed.
			// A new pager will be created in its place.
			// TODO:  Is the right behavior?

			debug_printf("DWALPager(%s) creating new pager\n");

			self->headerPage = self->newPageBuffer();
			self->pHeader = (Header *)self->headerPage->begin();

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
			self->delayedFreeList.create(self, self->newLastPageID(), "delayedFreeList");
			self->remapQueue.create(self, self->newLastPageID(), "remapQueue");

			// The first commit() below will flush the queues and update the queue states in the header,
			// but since the queues will not be used between now and then their states will not change.
			// In order to populate lastCommittedHeader, update the header now with the queue states.
			self->pHeader->freeList = self->freeList.getState();
			self->pHeader->delayedFreeList = self->delayedFreeList.getState();
			self->pHeader->remapQueue = self->remapQueue.getState();

			// Set remaining header bytes to \xff
			memset(self->headerPage->mutate() + self->pHeader->size(), 0xff, self->headerPage->size() - self->pHeader->size());

			// Since there is no previously committed header use the initial header for the initial commit.
			self->updateCommittedHeader();

			wait(self->commit());
		}

		debug_printf("DWALPager(%s) recovered.  committedVersion=%" PRId64 " logicalPageSize=%d physicalPageSize=%d\n", self->filename.c_str(), self->pHeader->committedVersion, self->logicalPageSize, self->physicalPageSize);
		return Void();
	}

	Reference<IPage> newPageBuffer() override {
		return Reference<IPage>(new FastAllocatedPage(logicalPageSize, physicalPageSize));
	}

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	int getUsablePageSize() override {
		return logicalPageSize - sizeof(FastAllocatedPage::Checksum);
	}

	// Get a new, previously available page ID.  The page will be considered in-use after the next commit
	// regardless of whether or not it was written to, until it is returned to the pager via freePage()
	ACTOR static Future<LogicalPageID> newPageID_impl(DWALPager *self) {
		// First try the free list
		Optional<LogicalPageID> freePageID = wait(self->freeList.pop());
		if(freePageID.present()) {
			debug_printf("DWALPager(%s) newPageID() returning %s from free list\n", self->filename.c_str(), toString(freePageID.get()).c_str());
			return freePageID.get();
		}

		// Try to reuse pages up to the earlier of the oldest version set by the user or the oldest snapshot still in the snapshots list
		ASSERT(!self->snapshots.empty());
		Optional<DelayedFreePage> delayedFreePageID = wait(self->delayedFreeList.pop(DelayedFreePage{self->effectiveOldestVersion(), 0}));
		if(delayedFreePageID.present()) {
			debug_printf("DWALPager(%s) newPageID() returning %s from delayed free list\n", self->filename.c_str(), toString(delayedFreePageID.get()).c_str());
			return delayedFreePageID.get().pageID;
		}

		// Lastly, add a new page to the pager
		LogicalPageID id = self->newLastPageID();
		debug_printf("DWALPager(%s) newPageID() returning %s at end of file\n", self->filename.c_str(), toString(id).c_str());
		return id;
	};

	// Grow the pager file by pone page and return it
	LogicalPageID newLastPageID() {
		LogicalPageID id = pHeader->pageCount;
		++pHeader->pageCount;
		return id;
	}

	Future<LogicalPageID> newPageID() override {
		return newPageID_impl(this);
	}

	Future<Void> writePhysicalPage(PhysicalPageID pageID, Reference<IPage> page, bool header = false) {
		debug_printf("DWALPager(%s) op=%s %s ptr=%p\n", filename.c_str(), (header ? "writePhysicalHeader" : "writePhysical"), toString(pageID).c_str(), page->begin());

		VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
		((Page *)page.getPtr())->updateChecksum(pageID);

		// Note:  Not using forwardError here so a write error won't be discovered until commit time.
		int blockSize = header ? smallestPhysicalBlock : physicalPageSize;
		Future<Void> f = holdWhile(page, map(pageFile->write(page->begin(), blockSize, (int64_t)pageID * blockSize), [=](Void) {
			debug_printf("DWALPager(%s) op=%s %s ptr=%p\n", filename.c_str(), (header ? "writePhysicalHeaderComplete" : "writePhysicalComplete"), toString(pageID).c_str(), page->begin());
			return Void();
		}));
		operations.add(f);
		return f;
	}

	Future<Void> writeHeaderPage(PhysicalPageID pageID, Reference<IPage> page) {
		return writePhysicalPage(pageID, page, true);
	}

	void updatePage(LogicalPageID pageID, Reference<IPage> data) override {
		// Get the cache entry for this page, without counting it as a cache hit as we're replacing its contents now
		PageCacheEntry &cacheEntry = pageCache.get(pageID, true);
		debug_printf("DWALPager(%s) op=write %s cached=%d reading=%d writing=%d\n", filename.c_str(), toString(pageID).c_str(), cacheEntry.initialized(), cacheEntry.initialized() && cacheEntry.reading(), cacheEntry.initialized() && cacheEntry.writing());

		// If the page is still being read then it's not also being written because a write places
		// the new content into readFuture when the write is launched, not when it is completed.
		// Read/write ordering is being enforced waiting readers will not see the new write.  This
		// is necessary for remap erasure to work correctly since the oldest version of a page, located
		// at the original page ID, could have a pending read when that version is expired and the write
		// of the next newest version over top of the original page begins.
		if(!cacheEntry.initialized()) {
			cacheEntry.writeFuture = writePhysicalPage(pageID, data);
		}
		else if(cacheEntry.reading()) {
			// Wait for the read to finish, then start the write.
			cacheEntry.writeFuture = map(success(cacheEntry.readFuture), [=](Void) {
				writePhysicalPage(pageID, data);
				return Void();
			});
		} 
		// If the page is being written, wait for this write before issuing the new write to ensure the
		// writes happen in the correct order
		else if(cacheEntry.writing()) {
			cacheEntry.writeFuture = map(cacheEntry.writeFuture, [=](Void) {
				writePhysicalPage(pageID, data);
				return Void();
			});
		}
		else {
			cacheEntry.writeFuture = writePhysicalPage(pageID, data);
		}

		// Always update the page contents immediately regardless of what happened above.
		cacheEntry.readFuture = data;
	}

	Future<LogicalPageID> atomicUpdatePage(LogicalPageID pageID, Reference<IPage> data, Version v) override {
		debug_printf("DWALPager(%s) op=writeAtomic %s @%" PRId64 "\n", filename.c_str(), toString(pageID).c_str(), v);
		// This pager does not support atomic update, so it always allocates and uses a new pageID
		Future<LogicalPageID> f = map(newPageID(), [=](LogicalPageID newPageID) {
			updatePage(newPageID, data);
			// TODO:  Possibly limit size of remap queue since it must be recovered on cold start
			RemappedPage r{v, pageID, newPageID};
			remapQueue.pushBack(r);
			remappedPages[pageID][v] = newPageID;
			debug_printf("DWALPager(%s) pushed %s\n", filename.c_str(), RemappedPage(r).toString().c_str());
			return pageID;
		});

		// No need for forwardError here because newPageID() is already wrapped in forwardError
		return f;
	}

	void freePage(LogicalPageID pageID, Version v) override {
		// If pageID has been remapped, then it can't be freed until all existing remaps for that page have been undone, so queue it for later deletion
		if(remappedPages.find(pageID) != remappedPages.end()) {
			debug_printf("DWALPager(%s) op=freeRemapped %s @%" PRId64 " oldestVersion=%" PRId64 "\n", filename.c_str(), toString(pageID).c_str(), v, pLastCommittedHeader->oldestVersion);
			remapQueue.pushBack(RemappedPage{v, pageID, invalidLogicalPageID});
			return;
		}

		// If v is older than the oldest version still readable then mark pageID as free as of the next commit
		if(v < effectiveOldestVersion()) {
			debug_printf("DWALPager(%s) op=freeNow %s @%" PRId64 " oldestVersion=%" PRId64 "\n", filename.c_str(), toString(pageID).c_str(), v, pLastCommittedHeader->oldestVersion);
			freeList.pushBack(pageID);
		}
		else {
			// Otherwise add it to the delayed free list
			debug_printf("DWALPager(%s) op=freeLater %s @%" PRId64 " oldestVersion=%" PRId64 "\n", filename.c_str(), toString(pageID).c_str(), v, pLastCommittedHeader->oldestVersion);
			delayedFreeList.pushBack({v, pageID});
		}
	};

	// Read a physical page from the page file.  Note that header pages use a page size of smallestPhysicalBlock
	// If the user chosen physical page size is larger, then there will be a gap of unused space after the header pages
	// and before the user-chosen sized pages.
	ACTOR static Future<Reference<IPage>> readPhysicalPage(DWALPager *self, PhysicalPageID pageID, bool header = false) {
		if(g_network->getCurrentTask() > TaskPriority::DiskRead) {
			wait(delay(0, TaskPriority::DiskRead));
		}

		state Reference<IPage> page = header ? Reference<IPage>(new FastAllocatedPage(smallestPhysicalBlock, smallestPhysicalBlock)) : self->newPageBuffer();
		debug_printf("DWALPager(%s) op=readPhysicalStart %s ptr=%p\n", self->filename.c_str(), toString(pageID).c_str(), page->begin());

		int blockSize = header ? smallestPhysicalBlock : self->physicalPageSize;
		// TODO:  Could a dispatched read try to write to page after it has been destroyed if this actor is cancelled?
		int readBytes = wait(self->pageFile->read(page->mutate(), blockSize, (int64_t)pageID * blockSize));
		debug_printf("DWALPager(%s) op=readPhysicalComplete %s ptr=%p bytes=%d\n", self->filename.c_str(), toString(pageID).c_str(), page->begin(), readBytes);

		// Header reads are checked explicitly during recovery
		if(!header) {
			Page *p = (Page *)page.getPtr();
			if(!p->verifyChecksum(pageID)) {
				debug_printf("DWALPager(%s) checksum failed for %s\n", self->filename.c_str(), toString(pageID).c_str());
				Error e = checksum_failed();
				TraceEvent(SevError, "DWALPagerChecksumFailed")
					.detail("Filename", self->filename.c_str())
					.detail("PageID", pageID)
					.detail("PageSize", self->physicalPageSize)
					.detail("Offset", pageID * self->physicalPageSize)
					.detail("CalculatedChecksum", p->calculateChecksum(pageID))
					.detail("ChecksumInPage", p->getChecksum())
					.error(e);
				throw e;
			}
		}
		return page;
	}

	static Future<Reference<IPage>> readHeaderPage(DWALPager *self, PhysicalPageID pageID) {
		return readPhysicalPage(self, pageID, true);
	}

	// Reads the most recent version of pageID either committed or written using updatePage()
	Future<Reference<IPage>> readPage(LogicalPageID pageID, bool cacheable, bool noHit = false) override {
		// Use cached page if present, without triggering a cache hit.
		// Otherwise, read the page and return it but don't add it to the cache
		if(!cacheable) {
			debug_printf("DWALPager(%s) op=readUncached %s\n", filename.c_str(), toString(pageID).c_str());
			PageCacheEntry *pCacheEntry = pageCache.getIfExists(pageID);
			if(pCacheEntry != nullptr) {
				debug_printf("DWALPager(%s) op=readUncachedHit %s\n", filename.c_str(), toString(pageID).c_str());
				return pCacheEntry->readFuture;
			}

			debug_printf("DWALPager(%s) op=readUncachedMiss %s\n", filename.c_str(), toString(pageID).c_str());
			return forwardError(readPhysicalPage(this, (PhysicalPageID)pageID), errorPromise);
		}

		PageCacheEntry &cacheEntry = pageCache.get(pageID, noHit);
		debug_printf("DWALPager(%s) op=read %s cached=%d reading=%d writing=%d noHit=%d\n", filename.c_str(), toString(pageID).c_str(), cacheEntry.initialized(), cacheEntry.initialized() && cacheEntry.reading(), cacheEntry.initialized() && cacheEntry.writing(), noHit);

		if(!cacheEntry.initialized()) {
			debug_printf("DWALPager(%s) issuing actual read of %s\n", filename.c_str(), toString(pageID).c_str());
			cacheEntry.readFuture = readPhysicalPage(this, (PhysicalPageID)pageID);
			cacheEntry.writeFuture = Void();
		}

		cacheEntry.readFuture = forwardError(cacheEntry.readFuture, errorPromise);
		return cacheEntry.readFuture;
	}

	Future<Reference<IPage>> readPageAtVersion(LogicalPageID pageID, Version v, bool cacheable, bool noHit) {
		auto i = remappedPages.find(pageID);

		if(i != remappedPages.end()) {
			auto j = i->second.upper_bound(v);
			if(j != i->second.begin()) {
				--j;
				debug_printf("DWALPager(%s) read %s @%" PRId64 " -> %s\n", filename.c_str(), toString(pageID).c_str(), v, toString(j->second).c_str());
				pageID = j->second;
			}
		}
		else {
			debug_printf("DWALPager(%s) read %s @%" PRId64 " (not remapped)\n", filename.c_str(), toString(pageID).c_str(), v);
		}

		return readPage(pageID, cacheable, noHit);
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

	// Get the oldest version set as of the last commit.
	Version getOldestVersion() override {
		return pLastCommittedHeader->oldestVersion;
	};

	// Calculate the *effective* oldest version, which can be older than the one set in the last commit since we
	// are allowing active snapshots to temporarily delay page reuse.
	Version effectiveOldestVersion() {
		return std::min(pLastCommittedHeader->oldestVersion, snapshots.front().version);
	}

	ACTOR static Future<Void> undoRemaps(DWALPager *self) {
		state RemappedPage cutoff;
		cutoff.version = self->effectiveOldestVersion();

		// TODO:  Use parallel reads
		// TODO:  One run of this actor might write to the same original page more than once, in which case just unmap the latest
		loop {
			if(self->remapUndoStop) {
				break;
			}
			state Optional<RemappedPage> p = wait(self->remapQueue.pop(cutoff));
			if(!p.present()) {
				break;
			}
			debug_printf("DWALPager(%s) undoRemaps popped %s\n", self->filename.c_str(), p.get().toString().c_str());

			if(p.get().newPageID == invalidLogicalPageID) {
				debug_printf("DWALPager(%s) undoRemaps freeing %s\n", self->filename.c_str(), p.get().toString().c_str());
				self->freePage(p.get().originalPageID, p.get().version);
			}
			else {
				// Read the data from the page that the original was mapped to
				Reference<IPage> data = wait(self->readPage(p.get().newPageID, false));

				// Write the data to the original page so it can be read using its original pageID
				self->updatePage(p.get().originalPageID, data);

				// Remove the remap from this page, deleting the entry for the pageID if its map becomes empty
				auto i = self->remappedPages.find(p.get().originalPageID);
				if(i->second.size() == 1) {
					self->remappedPages.erase(i);
				}
				else {
					i->second.erase(p.get().version);
				}

				// Now that the remap has been undone nothing will read this page so it can be freed as of the next commit.
				self->freePage(p.get().newPageID, 0);
			}
		}

		debug_printf("DWALPager(%s) undoRemaps stopped, remapQueue size is %d\n", self->filename.c_str(), self->remapQueue.numEntries);
		return Void();
	}

	// Flush all queues so they have no operations pending.
	ACTOR static Future<Void> flushQueues(DWALPager *self) {
		ASSERT(self->remapUndoFuture.isReady());

		// Flush remap queue separately, it's not involved in free page management
		wait(self->remapQueue.flush());

		// Flush the free list and delayed free list queues together as they are used by freePage() and newPageID()
		loop {
			state bool freeBusy = wait(self->freeList.preFlush());
			state bool delayedFreeBusy = wait(self->delayedFreeList.preFlush());

			// Once preFlush() returns false for both queues then there are no more operations pending
			// on either queue.  If preFlush() returns true for either queue in one loop execution then
			// it could have generated new work for itself or the other queue.
			if(!freeBusy && !delayedFreeBusy) {
				break;
			}
		}
		self->freeList.finishFlush();
		self->delayedFreeList.finishFlush();

		return Void();
	}

	ACTOR static Future<Void> commit_impl(DWALPager *self) {
		debug_printf("DWALPager(%s) commit begin\n", self->filename.c_str());

		// Write old committed header to Page 1
		self->writeHeaderPage(1, self->lastCommittedHeaderPage);

		// Trigger the remap eraser to stop and then wait for it.
		self->remapUndoStop = true;
		wait(self->remapUndoFuture);

		wait(flushQueues(self));

		self->pHeader->remapQueue = self->remapQueue.getState();
		self->pHeader->freeList = self->freeList.getState();
		self->pHeader->delayedFreeList = self->delayedFreeList.getState();

		// Wait for all outstanding writes to complete
		debug_printf("DWALPager(%s) waiting for outstanding writes\n", self->filename.c_str());
		wait(self->operations.signalAndCollapse());
		debug_printf("DWALPager(%s) Syncing\n", self->filename.c_str());

		// Sync everything except the header
		if(g_network->getCurrentTask() > TaskPriority::DiskWrite) {
			wait(delay(0, TaskPriority::DiskWrite));
		}
		wait(self->pageFile->sync());
		debug_printf("DWALPager(%s) commit version %" PRId64 " sync 1\n", self->filename.c_str(), self->pHeader->committedVersion);

		// Update header on disk and sync again.
		wait(self->writeHeaderPage(0, self->headerPage));
		if(g_network->getCurrentTask() > TaskPriority::DiskWrite) {
			wait(delay(0, TaskPriority::DiskWrite));
		}
		wait(self->pageFile->sync());
		debug_printf("DWALPager(%s) commit version %" PRId64 " sync 2\n", self->filename.c_str(), self->pHeader->committedVersion);

		// Update the last committed header for use in the next commit.
		self->updateCommittedHeader();
		self->addLatestSnapshot();

		// Try to expire snapshots up to the oldest version, in case some were being kept around due to being in use,
		// because maybe some are no longer in use.
		self->expireSnapshots(self->pHeader->oldestVersion);

		// Start unmapping pages for expired versions
		self->remapUndoStop = false;
		self->remapUndoFuture = undoRemaps(self);

		return Void();
	}

	Future<Void> commit() override {
		// Can't have more than one commit outstanding.
		ASSERT(commitFuture.isReady());
		commitFuture = forwardError(commit_impl(this), errorPromise);
		return commitFuture;
	}

	Key getMetaKey() const override {
		return pHeader->getMetaKey();
	}

	void setCommitVersion(Version v) override {
		pHeader->committedVersion = v;
	}

	void setMetaKey(KeyRef metaKey) override {
		pHeader->setMetaKey(metaKey);
	}
	
	ACTOR void shutdown(DWALPager *self, bool dispose) {
		debug_printf("DWALPager(%s) shutdown cancel recovery\n", self->filename.c_str());
		self->recoverFuture.cancel();
		debug_printf("DWALPager(%s) shutdown cancel commit\n", self->filename.c_str());
		self->commitFuture.cancel();
		debug_printf("DWALPager(%s) shutdown cancel remap\n", self->filename.c_str());
		self->remapUndoFuture.cancel();

		if(self->errorPromise.canBeSet()) {
			debug_printf("DWALPager(%s) shutdown sending error\n", self->filename.c_str());
			self->errorPromise.sendError(actor_cancelled());  // Ideally this should be shutdown_in_progress
		}

		// Must wait for pending operations to complete, canceling them can cause a crash because the underlying
		// operations may be uncancellable and depend on memory from calling scope's page reference
		debug_printf("DWALPager(%s) shutdown wait for operations\n", self->filename.c_str());
		wait(self->operations.signal());

		debug_printf("DWALPager(%s) shutdown destroy page cache\n", self->filename.c_str());
		wait(self->pageCache.clear());

		// Unreference the file and clear
		self->pageFile.clear();
		if(dispose) {
			debug_printf("DWALPager(%s) shutdown deleting file\n", self->filename.c_str());
			wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(self->filename, true));
		}

		self->closedPromise.send(Void());
		delete self;
	}

	void dispose() override {
		shutdown(this, true);
	}

	void close() override {
		shutdown(this, false);
	}

	Future<Void> getError() override {
		return errorPromise.getFuture();
	}
	
	Future<Void> onClosed() override {
		return closedPromise.getFuture();
	}

	StorageBytes getStorageBytes() override {
		ASSERT(recoverFuture.isReady());
		int64_t free;
		int64_t total;
		g_network->getDiskBytes(parentDirectory(filename), free, total);
		int64_t pagerSize = pHeader->pageCount * physicalPageSize;

		// It is not exactly known how many pages on the delayed free list are usable as of right now.  It could be,
		// if each commit delayed entries that were freeable were shuffled from the delayed free queue to the free queue.
		// but this doesn't seem necessary most of the time.
		int64_t reusable = (freeList.numEntries + delayedFreeList.numEntries) * physicalPageSize;

		return StorageBytes(free, total, pagerSize, free + reusable);
	}

	ACTOR static Future<Void> getUserPageCount_cleanup(DWALPager *self) {
		// Wait for the remap eraser to finish all of its work (not triggering stop)
		wait(self->remapUndoFuture);

		// Flush queues so there are no pending freelist operations
		wait(flushQueues(self));
	
		return Void();
	}

	// Get the number of pages in use by the pager's user
	Future<int64_t> getUserPageCount() override {
		return map(getUserPageCount_cleanup(this), [=](Void) {
			int64_t userPages = pHeader->pageCount - 2 - freeList.numPages - freeList.numEntries - delayedFreeList.numPages - delayedFreeList.numEntries - remapQueue.numPages;
			debug_printf("DWALPager(%s) userPages=%" PRId64 " totalPageCount=%" PRId64 " freeQueuePages=%" PRId64 " freeQueueCount=%" PRId64 " delayedFreeQueuePages=%" PRId64 " delayedFreeQueueCount=%" PRId64 " remapQueuePages=%" PRId64 " remapQueueCount=%" PRId64 "\n",
				filename.c_str(), userPages, pHeader->pageCount, freeList.numPages, freeList.numEntries, delayedFreeList.numPages, delayedFreeList.numEntries, remapQueue.numPages, remapQueue.numEntries);
			return userPages;
		});
	}

	Future<Void> init() override {
		return recoverFuture;
	}

	Version getLatestVersion() override {
		return pLastCommittedHeader->committedVersion;
	}

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

		KeyRef getMetaKey() const {
			return KeyRef((const uint8_t *)(this + 1), metaKeySize);
		}

		void setMetaKey(StringRef key) {
			ASSERT(key.size() < (smallestPhysicalBlock - sizeof(Header)));
			metaKeySize = key.size();
			if (key.size() > 0) {
				memcpy(this + 1, key.begin(), key.size());
			}
		}

		int size() const {
			return sizeof(Header) + metaKeySize;
		}

	private:
		Header();
	};
#pragma pack(pop)

	struct PageCacheEntry {
		Future<Reference<IPage>> readFuture;
		Future<Void> writeFuture;

		bool initialized() const {
			return readFuture.isValid();
		}

		bool reading() const {
			return !readFuture.isReady();
		}

		bool writing() const {
			return !writeFuture.isReady();
		}

		bool evictable() const {
			// Don't evict if a page is still being read or written
			return !reading() && !writing();
		}

		Future<Void> onEvictable() const {
			return ready(readFuture) && writeFuture;
		}
	};

	// Physical page sizes will always be a multiple of 4k because AsyncFileNonDurable requires
	// this in simulation, and it also makes sense for current SSDs.
	// Allowing a smaller 'logical' page size is very useful for testing.
	static constexpr int smallestPhysicalBlock = 4096;
	int physicalPageSize;
	int logicalPageSize;  // In simulation testing it can be useful to use a small logical page size

	int64_t pageCacheBytes;

	// The header will be written to / read from disk as a smallestPhysicalBlock sized chunk.
	Reference<IPage> headerPage;
	Header *pHeader;

	int desiredPageSize;

	Reference<IPage> lastCommittedHeaderPage;
	Header *pLastCommittedHeader;

	std::string filename;

	typedef ObjectCache<LogicalPageID, PageCacheEntry> PageCacheT;
	PageCacheT pageCache;

	Promise<Void> closedPromise;
	Promise<Void> errorPromise; 
	Future<Void> commitFuture;
	SignalableActorCollection operations;
	Future<Void> recoverFuture;
	Future<Void> remapUndoFuture;
	bool remapUndoStop;

	Reference<IAsyncFile> pageFile;

	LogicalPageQueueT freeList;

	// The delayed free list will be approximately in Version order.
	// TODO: Make this an ordered container some day.
	DelayedFreePageQueueT delayedFreeList;

	RemapQueueT remapQueue;

	struct SnapshotEntry {
		Version version;
		Promise<Void> expired;
		Reference<DWALPagerSnapshot> snapshot;
	};

	struct SnapshotEntryLessThanVersion {
		bool operator() (Version v, const SnapshotEntry &snapshot) {
			return v < snapshot.version;
		}

		bool operator() (const SnapshotEntry &snapshot, Version v) {
			return snapshot.version < v;
		}
	};

	// TODO: Better data structure
	std::unordered_map<LogicalPageID, std::map<Version, LogicalPageID>> remappedPages;

	std::deque<SnapshotEntry> snapshots;
};

// Prevents pager from reusing freed pages from version until the snapshot is destroyed
class DWALPagerSnapshot : public IPagerSnapshot, public ReferenceCounted<DWALPagerSnapshot> {
public:
	DWALPagerSnapshot(DWALPager *pager, Key meta, Version version, Future<Void> expiredFuture) : pager(pager), metaKey(meta), version(version), expired(expiredFuture) {
	}
	virtual ~DWALPagerSnapshot() {
	}

	Future<Reference<const IPage>> getPhysicalPage(LogicalPageID pageID, bool cacheable, bool noHit) override {
		if(expired.isError()) {
			throw expired.getError();
		}
		return map(pager->readPageAtVersion(pageID, version, cacheable, noHit), [=](Reference<IPage> p) {
			return Reference<const IPage>(p);
		});
	}

	Key getMetaKey() const override {
		return metaKey;
	}

	Version getVersion() const override {
		return version;
	}

	void addref() override {
		ReferenceCounted<DWALPagerSnapshot>::addref();
	}

	void delref() override {
		ReferenceCounted<DWALPagerSnapshot>::delref();
	}

	DWALPager *pager;
	Future<Void> expired;
	Version version;
	Key metaKey;
};

void DWALPager::expireSnapshots(Version v) {
	debug_printf("DWALPager(%s) expiring snapshots through %" PRId64 " snapshot count %d\n", filename.c_str(), v, (int)snapshots.size());
	while(snapshots.size() > 1 && snapshots.front().version < v && snapshots.front().snapshot->isSoleOwner()) {
		debug_printf("DWALPager(%s) expiring snapshot for %" PRId64 " soleOwner=%d\n", filename.c_str(), snapshots.front().version, snapshots.front().snapshot->isSoleOwner());
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
	if(i == snapshots.begin()) {
		throw version_invalid();
	}
	--i;
	return i->snapshot;
}

void DWALPager::addLatestSnapshot() {
	Promise<Void> expired;
	snapshots.push_back({
		pLastCommittedHeader->committedVersion,
		expired,
		Reference<DWALPagerSnapshot>(new DWALPagerSnapshot(this, pLastCommittedHeader->getMetaKey(), pLastCommittedHeader->committedVersion, expired.getFuture()))
	});
}


// TODO: Move this to a flow header once it is mature.
struct SplitStringRef {
	StringRef a;
	StringRef b;

	SplitStringRef(StringRef a = StringRef(), StringRef b = StringRef()) : a(a), b(b) {
	}

	SplitStringRef(Arena &arena, const SplitStringRef &toCopy)
	  : a(toStringRef(arena)), b() {
	}

	SplitStringRef prefix(int len) const {
		if(len <= a.size()) {
			return SplitStringRef(a.substr(0, len));
		}
		len -= a.size();
		return SplitStringRef(a, b.substr(0, len));
	}

	StringRef toStringRef(Arena &arena) const {
		StringRef c = makeString(size(), arena);
		memcpy(mutateString(c), a.begin(), a.size());
		memcpy(mutateString(c) + a.size(), b.begin(), b.size());
		return c;
	}

	Standalone<StringRef> toStringRef() const {
		Arena a;
		return Standalone<StringRef>(toStringRef(a), a);
	}

	int size() const {
		return a.size() + b.size();
	}

	int expectedSize() const {
		return size();
	}

	std::string toString() const {
		return format("%s%s", a.toString().c_str(), b.toString().c_str());
	}

	std::string toHexString() const {
		return format("%s%s", a.toHexString().c_str(), b.toHexString().c_str());
	}

	struct const_iterator {
		const uint8_t *ptr;
		const uint8_t *end;
		const uint8_t *next;

		inline bool operator==(const const_iterator &rhs) const {
			return ptr == rhs.ptr;
		}

		inline const_iterator & operator++() {
			++ptr;
			if(ptr == end) {
				ptr = next;
			}
			return *this;
		}

		inline const_iterator & operator+(int n) {
			ptr += n;
			if(ptr >= end) {
				ptr = next + (ptr - end);
			}
			return *this;
		}

		inline uint8_t operator *() const {
			return *ptr;
		}
	};

	inline const_iterator begin() const {
		return {a.begin(), a.end(), b.begin()};
	}

	inline const_iterator end() const {
		return {b.end()};
	}

	template<typename StringT>
	int compare(const StringT &rhs) const {
		auto j = begin();
		auto k = rhs.begin();
		auto jEnd = end();
		auto kEnd = rhs.end();

		while(j != jEnd && k != kEnd) {
			int cmp = *j - *k;
			if(cmp != 0) {
				return cmp;
			}
		}

		// If we've reached the end of *this, then values are equal if rhs is also exhausted, otherwise *this is less than rhs
		if(j == jEnd) {
			return k == kEnd ? 0 : -1;
		}

		return 1;
	}

};

// A BTree "page id" is actually a list of LogicalPageID's whose contents should be concatenated together.
// NOTE: Uses host byte order
typedef VectorRef<LogicalPageID> BTreePageID;

std::string toString(BTreePageID id) {
	return std::string("BTreePageID") + toString(id.begin(), id.end());
}

#define STR(x) LiteralStringRef(x)
struct RedwoodRecordRef {
	typedef uint8_t byte;

	RedwoodRecordRef(KeyRef key = KeyRef(), Version ver = 0, Optional<ValueRef> value = {}, uint32_t chunkTotal = 0, uint32_t chunkStart = 0)
		: key(key), version(ver), value(value), chunk({chunkTotal, chunkStart})
	{
	}

	RedwoodRecordRef(Arena &arena, const RedwoodRecordRef &toCopy)
	  : key(arena, toCopy.key), version(toCopy.version), chunk(toCopy.chunk)
	{
		if(toCopy.value.present()) {
			value = ValueRef(arena, toCopy.value.get());
		}
	}

	RedwoodRecordRef(KeyRef key, Optional<ValueRef> value, const byte intFields[14])
		: key(key), value(value)
	{
		deserializeIntFields(intFields);
	}

	// RedwoodRecordRefs are used for both internal and leaf pages of the BTree.
	// Boundary records in internal pages are made from leaf records.
	// These functions make creating and working with internal page records more convenient.
	inline BTreePageID getChildPage() const {
		ASSERT(value.present());
		return BTreePageID((LogicalPageID *)value.get().begin(), value.get().size() / sizeof(LogicalPageID));
	}

	inline void setChildPage(BTreePageID id) {
		value = ValueRef((const uint8_t *)id.begin(), id.size() * sizeof(LogicalPageID));
	}

	inline void setChildPage(Arena &arena, BTreePageID id) {
		value = ValueRef(arena, (const uint8_t *)id.begin(), id.size() * sizeof(LogicalPageID));
	}

	inline RedwoodRecordRef withPageID(BTreePageID id) const {
		return RedwoodRecordRef(key, version, ValueRef((const uint8_t *)id.begin(), id.size() * sizeof(LogicalPageID)), chunk.total, chunk.start);
	}

	inline RedwoodRecordRef withoutValue() const {
		return RedwoodRecordRef(key, version, {}, chunk.total, chunk.start);
	}

	// Returns how many bytes are in common between the integer fields of *this and other, assuming that 
	// all values are BigEndian, version is 64 bits, chunk total is 24 bits, and chunk start is 24 bits
	int getCommonIntFieldPrefix(const RedwoodRecordRef &other) const {
		if(version != other.version) {
			return clzll(version ^ other.version) >> 3;
		}

		if(chunk.total != other.chunk.total) {
			// the -1 is because we are only considering the lower 3 bytes
			return 8 + (clz(chunk.total ^ other.chunk.total) >> 3) - 1;
		}
		
		if(chunk.start != other.chunk.start) {
			// the -1 is because we are only considering the lower 3 bytes
			return 11 + (clz(chunk.start ^ other.chunk.start) >> 3) - 1;
		}

		return 14;
	}

	// Truncate (key, version, chunk.total, chunk.start) tuple to len bytes.
	void truncate(int len) {
		if(len <= key.size()) {
			key = key.substr(0, len);
			version = 0;
			chunk.total = 0;
			chunk.start = 0;
		}
		else {
			byte fields[intFieldArraySize];
			serializeIntFields(fields);
			int end = len - key.size();
			for(int i = intFieldArraySize - 1; i >= end; --i) {
				fields[i] = 0;
			}
		}
	}

	// Find the common prefix between two records, assuming that the first
	// skip bytes are the same.
	inline int getCommonPrefixLen(const RedwoodRecordRef &other, int skip) const {
		int skipStart = std::min(skip, key.size());
		int common = skipStart + commonPrefixLength(key.begin() + skipStart, other.key.begin() + skipStart, std::min(other.key.size(), key.size()) - skipStart);

		if(common == key.size() && key.size() == other.key.size()) {
			common += getCommonIntFieldPrefix(other);
		}

		return common;
	}

	static const int intFieldArraySize = 14;

	// Write big endian values of version (64 bits), total (24 bits), and start (24 bits) fields
	// to an array of 14 bytes
	void serializeIntFields(byte *dst) const {
		*(uint32_t *)(dst + 10) = bigEndian32(chunk.start);
		*(uint32_t *)(dst + 7) = bigEndian32(chunk.total);
		*(uint64_t *)dst = bigEndian64(version);
	}

	// Initialize int fields from the array format that serializeIntFields produces
	void deserializeIntFields(const byte *src) {
		version = bigEndian64(*(uint64_t *)src);
		chunk.total = bigEndian32(*(uint32_t *)(src + 7)) & 0xffffff;
		chunk.start = bigEndian32(*(uint32_t *)(src + 10)) & 0xffffff;
	}

	// TODO: Use SplitStringRef (unless it ends up being slower)
	KeyRef key;
	Optional<ValueRef> value;
	Version version;
	struct {
		uint32_t total;
		// TODO:  Change start to chunk number?
		uint32_t start;
	} chunk;

	int expectedSize() const {
		return key.expectedSize() + value.expectedSize();
	}

	bool isMultiPart() const {
		return chunk.total != 0;
	}

	// Generate a kv shard from a complete kv
	RedwoodRecordRef split(int start, int len) {
		ASSERT(!isMultiPart());
		return RedwoodRecordRef(key, version, value.get().substr(start, len), value.get().size(), start);
	}

	class Writer {
	public:
		Writer(byte *ptr) : wptr(ptr) {}

		byte *wptr;

		template<typename T> void write(const T &in) {
			*(T *)wptr = in;
			wptr += sizeof(T);
		}

		// Write a big endian 1 or 2 byte integer using the high bit of the first byte as an "extension" bit.
		// Values > 15 bits in length are not valid input but this is not checked for.
		void writeVarInt(int x) {
			if(x >= 128) {
				*wptr++ = (uint8_t)( (x >> 8) | 0x80 );
			}
			*wptr++ = (uint8_t)x;
		}

		void writeString(StringRef s) {
			memcpy(wptr, s.begin(), s.size());
			wptr += s.size();
		}

	};

	class Reader {
	public:
		Reader(const void *ptr) : rptr((const byte *)ptr) {}

		const byte *rptr;

		template<typename T> T read() {
			T r = *(const T *)rptr;
			rptr += sizeof(T);
			return r;
		}

		// Read a big endian 1 or 2 byte integer using the high bit of the first byte as an "extension" bit.
		int readVarInt() {
			int x = *rptr++;
			// If the high bit is set
			if(x & 0x80) {
				// Clear the high bit
				x &= 0x7f;
				// Shift low byte left
				x <<= 8;
				// Read the new low byte and OR it in
				x |= *rptr++;
			}

			return x;
		}

		StringRef readString(int len) {
			StringRef s(rptr, len);
			rptr += len;
			return s;
		}

		const byte * readBytes(int len) {
			const byte *b = rptr;
			rptr += len;
			return b;
		}
	};

#pragma pack(push,1)
	struct Delta {

		// Serialized Format
		//
		// 1 byte for Flags + a 4 bit length
		//    borrow source is prev ancestor - 0 or 1
		//    has_key_suffix
		//    has_value
		//    has_version
		//    other_fields suffix len - 4 bits
		//
		// If has value and value is not 4 bytes
		//    1 byte value length
		//
		// 1 or 2 bytes for Prefix Borrow Length (hi bit indicates presence of second byte)
		//
		// IF has_key_suffix is set
		//    1 or 2 bytes for Key Suffix Length
		//
		// Key suffix bytes
		// Meta suffix bytes
		// Value bytes
		//
		// For a series of RedwoodRecordRef's containing shards of the same KV pair where the key size is < 104 bytes,
		// the overhead per middle chunk is 7 bytes:
		//   4 bytes of child pointers in the DeltaTree Node
		//   1 flag byte
		//   1 prefix borrow length byte
		//   1 meta suffix byte describing chunk start position
 
		enum EFlags {
			PREFIX_SOURCE = 0x80,
			HAS_KEY_SUFFIX = 0x40,
			HAS_VALUE = 0x20,
			HAS_VERSION = 0x10,
			INT_FIELD_SUFFIX_BITS = 0x0f
		};

		uint8_t flags;

		inline byte * data() {
			return (byte *)(this + 1);
		}

		inline const byte * data() const {
			return (const byte *)(this + 1);
		}

		void setPrefixSource(bool val) {
			if(val) {
				flags |= PREFIX_SOURCE;
			}
			else {
				flags &= ~PREFIX_SOURCE;
			}
		}

		bool getPrefixSource() const {
			return flags & PREFIX_SOURCE;
		}

		RedwoodRecordRef apply(const RedwoodRecordRef &base, Arena &arena) const {
			Reader r(data());

			int intFieldSuffixLen = flags & INT_FIELD_SUFFIX_BITS;
			int prefixLen = r.readVarInt();
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;

			StringRef k;

			// Separate the borrowed key string byte count from the borrowed int field byte count
			int keyPrefixLen = std::min(prefixLen, base.key.size());
			int intFieldPrefixLen = prefixLen - keyPrefixLen;
			int keySuffixLen = (flags & HAS_KEY_SUFFIX) ? r.readVarInt() : 0;

			// If there is a key suffix, reconstitute the complete key into a contiguous string
			if(keySuffixLen > 0) {
				k = makeString(keyPrefixLen + keySuffixLen, arena);
				memcpy(mutateString(k), base.key.begin(), keyPrefixLen);
				memcpy(mutateString(k) + keyPrefixLen, r.readString(keySuffixLen).begin(), keySuffixLen);
			}
			else {
				k = base.key.substr(0, keyPrefixLen);
			}

			// Now decode the integer fields
			const byte *intFieldSuffix = r.readBytes(intFieldSuffixLen);

			// Create big endian array in which to reassemble the integer fields from prefix and suffix bytes
			byte intFields[intFieldArraySize];

			// If borrowing any bytes, get the source's integer field array
			if(intFieldPrefixLen > 0) {
				base.serializeIntFields(intFields);
			}
			else {
				memset(intFields, 0, intFieldArraySize);
			}

			// Version offset is used to skip the version bytes in the int field array when version is missing (aka 0)
			int versionOffset = ( (intFieldPrefixLen == 0) && (~flags & HAS_VERSION) ) ? 8 : 0;

			// If there are suffix bytes, copy those into place after the prefix
			if(intFieldSuffixLen > 0) {
				memcpy(intFields + versionOffset + intFieldPrefixLen, intFieldSuffix, intFieldSuffixLen);
			}

			// Zero out any remaining bytes if the array was initialized from base
			if(intFieldPrefixLen > 0) {
				for(int i = versionOffset + intFieldPrefixLen + intFieldSuffixLen; i < intFieldArraySize; ++i) {
					intFields[i] = 0;
				}
			}

			return RedwoodRecordRef(k, flags & HAS_VALUE ? r.readString(valueLen) : Optional<ValueRef>(), intFields);
		}

		int size() const {
			Reader r(data());

			int intFieldSuffixLen = flags & INT_FIELD_SUFFIX_BITS;
			r.readVarInt();  // prefixlen
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;
			int keySuffixLen = (flags & HAS_KEY_SUFFIX) ? r.readVarInt() : 0;

			return sizeof(Delta) + r.rptr - data() + intFieldSuffixLen + valueLen + keySuffixLen;
		}

		// Delta can't be determined without the RedwoodRecordRef upon which the Delta is based.
		std::string toString() const {
			Reader r(data());

			std::string flagString = " ";
			if(flags & PREFIX_SOURCE) flagString += "prefixSource ";
			if(flags & HAS_KEY_SUFFIX) flagString += "keySuffix ";
			if(flags & HAS_VERSION) flagString += "Version ";
			if(flags & HAS_VALUE) flagString += "Value ";

			int intFieldSuffixLen = flags & INT_FIELD_SUFFIX_BITS;
			int prefixLen = r.readVarInt();
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;
			int keySuffixLen = (flags & HAS_KEY_SUFFIX) ? r.readVarInt() : 0;

			return format("len: %d  flags: %s prefixLen: %d  keySuffixLen: %d  intFieldSuffix: %d  valueLen %d  raw: %s",
				size(), flagString.c_str(), prefixLen, keySuffixLen, intFieldSuffixLen, valueLen, StringRef((const uint8_t *)this, size()).toHexString().c_str());
		}
	};

	// Using this class as an alternative for Delta enables reading a DeltaTree<RecordRef> while only decoding
	// its values, so the Reader does not require the original prev/next ancestors.
	struct DeltaValueOnly : Delta {
		RedwoodRecordRef apply(const RedwoodRecordRef &base, Arena &arena) const {
			Reader r(data());

			// Skip prefix length
			r.readVarInt();

			// Get value length
			int valueLen = (flags & HAS_VALUE) ? r.read<uint8_t>() : 0;

			// Skip key suffix length and bytes if exists
			if(flags & HAS_KEY_SUFFIX) {
				r.readString(r.readVarInt());
			}

			// Skip int field suffix if present
			r.readBytes(flags & INT_FIELD_SUFFIX_BITS);

			return RedwoodRecordRef(StringRef(), 0, (flags & HAS_VALUE ? r.readString(valueLen) : Optional<ValueRef>()) );
		}
	};
#pragma pack(pop)

	// Compares and orders by key, version, chunk.start, chunk.total.
	// Value is not considered, as it is does not make sense for a container
	// to have two records which differ only in value.
	int compare(const RedwoodRecordRef &rhs) const {
		int cmp = key.compare(rhs.key);
		if(cmp == 0) {
			cmp = version - rhs.version;
			if(cmp == 0) {
				// It is assumed that in any data set there will never be more than one
				// unique chunk total size for the same key and version, so sort by start, total 
				// Chunked (represented by chunk.total > 0) sorts higher than whole
				cmp = chunk.start - rhs.chunk.start;
				if(cmp == 0) {
					cmp = chunk.total - rhs.chunk.total;
				}
			}
		}
		return cmp;
	}

	// Compares key fields and value for equality
	bool identical(const RedwoodRecordRef &rhs) const {
		return compare(rhs) == 0 && value == rhs.value;
	}

	bool operator==(const RedwoodRecordRef &rhs) const {
		return compare(rhs) == 0;
	}

	bool operator!=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) != 0;
	}

		bool operator<(const RedwoodRecordRef &rhs) const {
		return compare(rhs) < 0;
	}

	bool operator>(const RedwoodRecordRef &rhs) const {
		return compare(rhs) > 0;
	}

	bool operator<=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) <= 0;
	}

	bool operator>=(const RedwoodRecordRef &rhs) const {
		return compare(rhs) >= 0;
	}

	int deltaSize(const RedwoodRecordRef &base, bool worstCase = true) const {
		int size = sizeof(Delta);

		if(value.present()) {
			size += value.get().size();
			++size;
		}

		int prefixLen = getCommonPrefixLen(base, 0);
		size += (worstCase || prefixLen >= 128) ? 2 : 1;

		int intFieldPrefixLen;

		// Currently using a worst-guess guess where int fields in suffix are stored in their entirety if nonzero.
		if(prefixLen < key.size()) {
			int keySuffixLen = key.size() - prefixLen;
			size += (worstCase || keySuffixLen >= 128) ? 2 : 1;
			size += keySuffixLen;
			intFieldPrefixLen = 0;
		}
		else {
			intFieldPrefixLen = prefixLen - key.size();
			if(worstCase) {
				size += 2;
			}
		}

		if(version == 0 && chunk.total == 0 && chunk.start == 0) {
			// No int field suffix needed
		}
		else {
			byte fields[intFieldArraySize];
			serializeIntFields(fields);

			const byte *end = fields + intFieldArraySize - 1;
			int trailingNulls = 0;
			while(*end-- == 0) {
				++trailingNulls;
			}

			size += std::max(0, intFieldArraySize - intFieldPrefixLen - trailingNulls);
			if(intFieldPrefixLen == 0 && version == 0) {
				size -= 8;
			}
		}

		return size;
	}

	// commonPrefix between *this and base can be passed if known
	int writeDelta(Delta &d, const RedwoodRecordRef &base, int commonPrefix = -1) const {
		d.flags = version == 0 ? 0 : Delta::HAS_VERSION;

		if(commonPrefix < 0) {
			commonPrefix = getCommonPrefixLen(base, 0);
		}

		Writer w(d.data());

		// prefixLen
		w.writeVarInt(commonPrefix);

		// valueLen
		if(value.present()) {
			d.flags |= Delta::HAS_VALUE;
			w.write<uint8_t>(value.get().size());
		}

		// keySuffixLen
		if(key.size() > commonPrefix) {
			d.flags |= Delta::HAS_KEY_SUFFIX;

			StringRef keySuffix = key.substr(commonPrefix);
			w.writeVarInt(keySuffix.size());

			// keySuffix
			w.writeString(keySuffix);
		}

		// This is a common case, where no int suffix is needed
		if(version == 0 && chunk.total == 0 && chunk.start == 0) {
			// The suffixLen bits in flags are already zero, so nothing to do here.
		}
		else {
			byte fields[intFieldArraySize];
			serializeIntFields(fields);

			// Find the position of the first null byte from the right
			// This for loop has no endPos > 0 check because it is known that the array contains non-null bytes
			int endPos;
			for(endPos = intFieldArraySize; fields[endPos - 1] == 0; --endPos);

			// Start copying after any prefix bytes that matched the int fields of the base
			int intFieldPrefixLen = std::max(0, commonPrefix - key.size());
			int startPos = intFieldPrefixLen + (intFieldPrefixLen == 0 && version == 0 ? 8 : 0);
			int suffixLen = std::max(0, endPos - startPos);

			if(suffixLen > 0) {
				w.writeString(StringRef(fields + startPos, suffixLen));
				d.flags |= suffixLen;
			}
		}

		if(value.present()) {
			w.writeString(value.get());
		}

		return w.wptr - d.data() + sizeof(Delta);
	}

	template<typename StringRefT>
	static std::string kvformat(StringRefT s, int hexLimit = -1) {
		bool hex = false;

		for(auto c : s) {
			if(!isprint(c)) {
				hex = true;
				break;
			}
		}

		return hex ? s.toHexString(hexLimit) : s.toString();
	}

	std::string toString(int hexLimit = 15) const {
		std::string r;
		r += format("'%s'@%" PRId64, kvformat(key, hexLimit).c_str(), version);
		r += format("[%u/%u]->", chunk.start, chunk.total);
		if(value.present()) {
			// Assume that values the size of a page ID are page IDs.  It's not perfect but it's just for debugging.
			if(value.get().size() == sizeof(LogicalPageID)) {
				r += format("[%s]", ::toString(getChildPage()).c_str());
			}
			else {
				r += format("'%s'", kvformat(value.get(), hexLimit).c_str());
			}
		}
		else {
			r += "null";
		}
		return r;
	}
};

struct BTreePage {
	typedef DeltaTree<RedwoodRecordRef> BinaryTree;
	typedef DeltaTree<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly> ValueTree;

#pragma pack(push,1)
	struct {
		uint8_t height;
		uint16_t itemCount;
		uint32_t kvBytes;
	};
#pragma pack(pop)

	int size() const {
		const BinaryTree *t = &tree();
		return (uint8_t *)t - (uint8_t *)this + t->size();
	}

	bool isLeaf() const {
		return height == 1;
	}

	BinaryTree & tree() {
		return *(BinaryTree *)(this + 1);
	}

	const BinaryTree & tree() const {
		return *(const BinaryTree *)(this + 1);
	}

	const ValueTree & valueTree() const {
		return *(const ValueTree *)(this + 1);
	}

	std::string toString(bool write, BTreePageID id, Version ver, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound) const {
		std::string r;
		r += format("BTreePage op=%s %s @%" PRId64 " ptr=%p height=%d count=%d kvBytes=%d\n  lowerBound: %s\n  upperBound: %s\n",
					write ? "write" : "read", ::toString(id).c_str(), ver, this, height, (int)itemCount, (int)kvBytes,
					lowerBound->toString().c_str(), upperBound->toString().c_str());
		try {
			if(itemCount > 0) {
				// This doesn't use the cached reader for the page but it is only for debugging purposes
				BinaryTree::Reader reader(&tree(), lowerBound, upperBound);
				BinaryTree::Cursor c = reader.getCursor();

				c.moveFirst();
				ASSERT(c.valid());

				bool anyOutOfRange = false;
				do {
					r += "  ";
					r += c.get().toString();

					bool tooLow = c.get().key < lowerBound->key;
					bool tooHigh = c.get().key > upperBound->key;
					if(tooLow || tooHigh) {
						anyOutOfRange = true;
						if(tooLow) {
							r += " (too low)";
						}
						if(tooHigh) {
							r += " (too high)";
						}
					}
					r += "\n";

				} while(c.moveNext());
				ASSERT(!anyOutOfRange);
			}
		} catch (Error& e) {
			debug_printf("BTreePage::toString ERROR: %s\n", e.what());
			debug_printf("BTreePage::toString partial result: %s\n", r.c_str());
			throw;
		}

		return r;
	}
};

static void makeEmptyRoot(Reference<IPage> page) {
	BTreePage *btpage = (BTreePage *)page->begin();
	btpage->height = 1;
	btpage->kvBytes = 0;
	btpage->itemCount = 0;
	btpage->tree().build(nullptr, nullptr, nullptr, nullptr);
}

BTreePage::BinaryTree::Reader * getReader(Reference<const IPage> page) {
	return (BTreePage::BinaryTree::Reader *)page->userData;
}

struct BoundaryRefAndPage {
	Standalone<RedwoodRecordRef> lowerBound;
	Reference<IPage> firstPage;
	std::vector<Reference<IPage>> extPages;

	std::string toString() const {
		return format("[%s, %d pages]", lowerBound.toString().c_str(), extPages.size() + (firstPage ? 1 : 0));
	}
};

#define NOT_IMPLEMENTED { UNSTOPPABLE_ASSERT(false); }

#pragma pack(push, 1)
template<typename T, typename SizeT = int8_t>
struct InPlaceArray {
	SizeT count;

	const T * begin() const {
		return (T *)(this + 1);
	}
	
	T * begin() {
		return (T *)(this + 1);
	}

	const T * end() const {
		return begin() + count;
	}
	
	T * end() {
		return begin() + count;
	}

	VectorRef<T> get() {
		return VectorRef<T>(begin(), count);
	}

	void set(VectorRef<T> v, int availableSpace) {
		ASSERT(sizeof(T) * v.size() <= availableSpace);
		count = v.size();
		memcpy(begin(), v.begin(), sizeof(T) * v.size());
	}

	int extraSize() const {
		return count * sizeof(T);
	}
};
#pragma pack(pop)

class VersionedBTree : public IVersionedStore {
public:
	// The first possible internal record possible in the tree
	static RedwoodRecordRef dbBegin;
	// A record which is greater than the last possible record in the tree
	static RedwoodRecordRef dbEnd;

	struct LazyDeleteQueueEntry {
		Version version;
		Standalone<BTreePageID> pageID;

		bool operator< (const LazyDeleteQueueEntry &rhs) const {
			return version < rhs.version;
		}

		int readFromBytes(const uint8_t *src) {
			version = *(Version *)src;
			src += sizeof(Version);
			int count = *src++;
			pageID = BTreePageID((LogicalPageID *)src, count);
			return bytesNeeded();
		}

		int bytesNeeded() const {
			return sizeof(Version) + 1 + (pageID.size() * sizeof(LogicalPageID));
		}

		int writeToBytes(uint8_t *dst) const {
			*(Version *)dst = version;
			dst += sizeof(Version);
			*dst++ = pageID.size();
			memcpy(dst, pageID.begin(), pageID.size() * sizeof(LogicalPageID));
			return bytesNeeded();
		}

		std::string toString() const {
			return format("{%s @%" PRId64 "}", ::toString(pageID).c_str(), version);
		}
	};

	typedef FIFOQueue<LazyDeleteQueueEntry> LazyDeleteQueueT;

#pragma pack(push, 1)
	struct MetaKey {
		static constexpr int FORMAT_VERSION = 2;
		// This serves as the format version for the entire tree, individual pages will not be versioned
		uint16_t formatVersion;
		uint8_t height;
		LazyDeleteQueueT::QueueState lazyDeleteQueue;
		InPlaceArray<LogicalPageID> root;

		KeyRef asKeyRef() const {
			return KeyRef((uint8_t *)this, sizeof(MetaKey) + root.extraSize());
		}

		void fromKeyRef(KeyRef k) {
			memcpy(this, k.begin(), k.size());
			ASSERT(formatVersion == FORMAT_VERSION);
		}

		std::string toString() {
			return format("{height=%d  formatVersion=%d  root=%s  lazyDeleteQueue=%s}", (int)height, (int)formatVersion, ::toString(root.get()).c_str(), lazyDeleteQueue.toString().c_str());
		}

	};
#pragma pack(pop)

	struct Counts {
		Counts() {
			memset(this, 0, sizeof(Counts));
			startTime = g_network ? now() : 0;
		}

		void clear() {
			*this = Counts();
		}

		int64_t pageReads;
		int64_t extPageReads;
		int64_t pagePreloads;
		int64_t extPagePreloads;
		int64_t setBytes;
		int64_t pageWrites;
		int64_t extPageWrites;
		int64_t sets;
		int64_t clears;
		int64_t commits;
		int64_t gets;
		int64_t getRanges;
		int64_t commitToPage;
		int64_t commitToPageStart;
		double startTime;

		std::string toString(bool clearAfter = false) {
			const char *labels[] = {"set", "clear", "get", "getRange", "commit", "pageReads", "extPageRead", "pagePreloads", "extPagePreloads", "pageWrite", "extPageWrite", "commitPage", "commitPageStart"};
			const int64_t values[] = {sets, clears, gets, getRanges, commits, pageReads, extPageReads, pagePreloads, extPagePreloads, pageWrites, extPageWrites, commitToPage, commitToPageStart};

			double elapsed = now() - startTime;
			std::string s;
			for(int i = 0; i < sizeof(values) / sizeof(int64_t); ++i) {
				s += format("%s=%" PRId64 " (%d/s)  ", labels[i], values[i], int(values[i] / elapsed));
			}

			if(clearAfter) {
				clear();
			}

			return s;
		}
	};

	// Using a static for metrics because a single process shouldn't normally have multiple storage engines
	static Counts counts;

	// All async opts on the btree are based on pager reads, writes, and commits, so
	// we can mostly forward these next few functions to the pager
	Future<Void> getError() {
		return m_pager->getError();
	}

	Future<Void> onClosed() {
		return m_pager->onClosed();
	}

	void close_impl(bool dispose) {
		auto *pager = m_pager;
		delete this;
		if(dispose)
			pager->dispose();
		else
			pager->close();
	}

	void dispose() {
		return close_impl(true);
	}

	void close() {
		return close_impl(false);
	}

	KeyValueStoreType getType() NOT_IMPLEMENTED
	bool supportsMutation(int op) NOT_IMPLEMENTED
	StorageBytes getStorageBytes() {
		return m_pager->getStorageBytes();
	}

	// Writes are provided in an ordered stream.
	// A write is considered part of (a change leading to) the version determined by the previous call to setWriteVersion()
	// A write shall not become durable until the following call to commit() begins, and shall be durable once the following call to commit() returns
	void set(KeyValueRef keyValue) {
		++counts.sets;
		SingleKeyMutationsByVersion &changes = insertMutationBoundary(keyValue.key)->second.startKeyMutations;

		if(singleVersion) {
			if(changes.empty()) {
				changes[0] = SingleKeyMutation(keyValue.value);
			}
			else {
				changes.begin()->second = SingleKeyMutation(keyValue.value);
			}
		}
		else {
			// Add the set if the changes set is empty or the last entry isn't a set to exactly the same value
			if(changes.empty() || !changes.rbegin()->second.equalToSet(keyValue.value)) {
				changes[m_writeVersion] = SingleKeyMutation(keyValue.value);
			}
		}
	}
	void clear(KeyRangeRef range) {
		++counts.clears;
		MutationBufferT::iterator iBegin = insertMutationBoundary(range.begin);
		MutationBufferT::iterator iEnd = insertMutationBoundary(range.end);

		// In single version mode, clear all pending updates in the affected range
		if(singleVersion) {
			RangeMutation &range = iBegin->second;
			range.startKeyMutations.clear();
			range.startKeyMutations[0] = SingleKeyMutation();
			range.rangeClearVersion = 0;
			++iBegin;
			m_pBuffer->erase(iBegin, iEnd);
		}
		else {
			// For each boundary in the cleared range
			while(iBegin != iEnd) {
				RangeMutation &range = iBegin->second;

				// Set the rangeClearedVersion if not set
				if(!range.rangeClearVersion.present())
					range.rangeClearVersion = m_writeVersion;

				// Add a clear to the startKeyMutations map if it's empty or the last item is not a clear
				if(range.startKeyMutations.empty() || !range.startKeyMutations.rbegin()->second.isClear())
					range.startKeyMutations[m_writeVersion] = SingleKeyMutation();

				++iBegin;
			}
		}
	}

	void mutate(int op, StringRef param1, StringRef param2) NOT_IMPLEMENTED

	void setOldestVersion(Version v) {
		m_newOldestVersion = v;
	}

	Version getOldestVersion() {
		return m_pager->getOldestVersion();
	}

	Version getLatestVersion() {
		if(m_writeVersion != invalidVersion)
			return m_writeVersion;
		return m_pager->getLatestVersion();
	}

	Version getWriteVersion() {
		return m_writeVersion;
	}

	Version getLastCommittedVersion() {
		return m_lastCommittedVersion;
	}

	VersionedBTree(IPager2 *pager, std::string name, bool singleVersion = false)
	  : m_pager(pager),
		m_writeVersion(invalidVersion),
		m_lastCommittedVersion(invalidVersion),
		m_pBuffer(nullptr),
		m_name(name),
		singleVersion(singleVersion)
	{
		m_init = init_impl(this);
		m_latestCommit = m_init;
	}

	ACTOR static Future<int> incrementalSubtreeClear(VersionedBTree *self, bool *pStop = nullptr, unsigned int minPages = 0, int maxPages = std::numeric_limits<int>::max()) {
		// TODO: Is it contractually okay to always to read at the latest version?
		state Reference<IPagerSnapshot> snapshot = self->m_pager->getReadSnapshot(self->m_pager->getLatestVersion());
		state int freedPages = 0;
		loop {
			// take a page from front of queue
			state Optional<LazyDeleteQueueEntry> q = wait(self->m_lazyDeleteQueue.pop());
			debug_printf("LazyDelete: popped %s\n", toString(q).c_str());
			if(!q.present()) {
				break;
			}

			// Read the page without caching
			Reference<const IPage> p = wait(self->readPage(snapshot, q.get().pageID, nullptr, nullptr, true));
			const BTreePage &btPage = *(BTreePage *)p->begin();

			// Level 1 (leaf) nodes should never be in the lazy delete queue
			ASSERT(btPage.height > 1);
			
			// Iterate over page entries, skipping key decoding using BTreePage::ValueTree which uses
			// RedwoodRecordRef::DeltaValueOnly as the delta type type to skip key decoding
			BTreePage::ValueTree::Reader reader(&btPage.valueTree(), &dbBegin, &dbEnd);
			auto c = reader.getCursor();
			ASSERT(c.moveFirst());
			Version v = q.get().version;
			while(1) {
				if(c.get().value.present()) {
					BTreePageID btChildPageID = c.get().getChildPage();
					// If this page is height 2, then the children are leaves so free
					if(btPage.height == 2) {
						debug_printf("LazyDelete: freeing child %s\n", toString(btChildPageID).c_str());
						self->freeBtreePage(btChildPageID, v);
						freedPages += btChildPageID.size();
					}
					else {
						// Otherwise, queue them for lazy delete.
						debug_printf("LazyDelete: queuing child %s\n", toString(btChildPageID).c_str());
						self->m_lazyDeleteQueue.pushFront(LazyDeleteQueueEntry{v, btChildPageID});
					}
				}
				if(!c.moveNext()) {
					break;
				}
			}

			// Free the page, now that its children have either been freed or queued
			debug_printf("LazyDelete: freeing queue entry %s\n", toString(q.get().pageID).c_str());
			self->freeBtreePage(q.get().pageID, v);
			freedPages += q.get().pageID.size();

			// If stop is set and we've freed the minimum number of pages required, or the maximum is exceeded, return.
			if((freedPages >= minPages && pStop != nullptr && *pStop) || freedPages >= maxPages) {
				break;
			}
		}

		debug_printf("LazyDelete: freed %d pages, %s has %" PRId64 " entries\n", freedPages, self->m_lazyDeleteQueue.name.c_str(), self->m_lazyDeleteQueue.numEntries);
		return freedPages;
	}

	ACTOR static Future<Void> init_impl(VersionedBTree *self) {
		wait(self->m_pager->init());

		state Version latest = self->m_pager->getLatestVersion();
		self->m_newOldestVersion = self->m_pager->getOldestVersion();

		debug_printf("Recovered pager to version %" PRId64 ", oldest version is %" PRId64 "\n", self->m_newOldestVersion);

		state Key meta = self->m_pager->getMetaKey();
		if(meta.size() == 0) {
			self->m_header.formatVersion = MetaKey::FORMAT_VERSION;
			LogicalPageID id = wait(self->m_pager->newPageID());
			BTreePageID newRoot((LogicalPageID *)&id, 1);
			debug_printf("new root %s\n", toString(newRoot).c_str());
			self->m_header.root.set(newRoot, sizeof(headerSpace) - sizeof(m_header));
			self->m_header.height = 1;
			++latest;
			Reference<IPage> page = self->m_pager->newPageBuffer();
			makeEmptyRoot(page);
			self->m_pager->updatePage(id, page);
			self->m_pager->setCommitVersion(latest);

			LogicalPageID newQueuePage = wait(self->m_pager->newPageID());
			self->m_lazyDeleteQueue.create(self->m_pager, newQueuePage, "LazyDeleteQueue");
			self->m_header.lazyDeleteQueue = self->m_lazyDeleteQueue.getState();
			self->m_pager->setMetaKey(self->m_header.asKeyRef());
			wait(self->m_pager->commit());
			debug_printf("Committed initial commit.\n");
		}
		else {
			self->m_header.fromKeyRef(meta);
			self->m_lazyDeleteQueue.recover(self->m_pager, self->m_header.lazyDeleteQueue, "LazyDeleteQueueRecovered");
		}

		debug_printf("Recovered btree at version %" PRId64 ": %s\n", latest, self->m_header.toString().c_str());

		self->m_maxPartSize = std::min(255, self->m_pager->getUsablePageSize() / 5);
		self->m_lastCommittedVersion = latest;
		return Void();
	}

	Future<Void> init() override {
		return m_init;
	}

	virtual ~VersionedBTree() {
		// This probably shouldn't be called directly (meaning deleting an instance directly) but it should be safe,
		// it will cancel init and commit and leave the pager alive but with potentially an incomplete set of 
		// uncommitted writes so it should not be committed.
		m_init.cancel();
		m_latestCommit.cancel();
	}

	Reference<IStoreCursor> readAtVersion(Version v) {
		// Only committed versions can be read.
		Version recordVersion = singleVersion ? 0 : v;
		ASSERT(v <= m_lastCommittedVersion);
		if(singleVersion) {
			ASSERT(v == m_lastCommittedVersion);
		}
		Reference<IPagerSnapshot> snapshot = m_pager->getReadSnapshot(v);

		// Snapshot will continue to hold the metakey value memory
		KeyRef m = snapshot->getMetaKey();

		return Reference<IStoreCursor>(new Cursor(snapshot, ((MetaKey *)m.begin())->root.get(), recordVersion));
	}

	// Must be nondecreasing
	void setWriteVersion(Version v) {
		ASSERT(v > m_lastCommittedVersion);
		// If there was no current mutation buffer, create one in the buffer map and update m_pBuffer
		if(m_pBuffer == nullptr) {
			// When starting a new mutation buffer its start version must be greater than the last write version
			ASSERT(v > m_writeVersion);
			m_pBuffer = &m_mutationBuffers[v];

			// Create range representing the entire keyspace.  This reduces edge cases to applying mutations
			// because now all existing keys are within some range in the mutation map.
			(*m_pBuffer)[dbBegin.key] = RangeMutation();
			// Setting the dbEnd key to be cleared prevents having to treat a range clear to dbEnd as a special
			// case in order to avoid traversing down the rightmost edge of the tree.
			(*m_pBuffer)[dbEnd.key].startKeyMutations[0] = SingleKeyMutation();
		}
		else {
			// It's OK to set the write version to the same version repeatedly so long as m_pBuffer is not null
			ASSERT(v >= m_writeVersion);
		}
		m_writeVersion = v;
	}

	Future<Void> commit() {
		if(m_pBuffer == nullptr)
			return m_latestCommit;
		return commit_impl(this);
	}

	ACTOR static Future<Void> destroyAndCheckSanity_impl(VersionedBTree *self) {
		ASSERT(g_network->isSimulated());

		debug_printf("Clearing tree.\n");
		self->setWriteVersion(self->getLatestVersion() + 1);
		self->clear(KeyRangeRef(dbBegin.key, dbEnd.key));

		loop {
			state int freedPages = wait(self->incrementalSubtreeClear(self));
			wait(self->commit());
			// Keep looping until the last commit doesn't do anything at all
			if(self->m_lazyDeleteQueue.numEntries == 0 && freedPages == 0) {
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
		LazyDeleteQueueT::QueueState s = self->m_lazyDeleteQueue.getState();
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

	Future<Void> destroyAndCheckSanity() {
		return destroyAndCheckSanity_impl(this);
	}

	bool isSingleVersion() const {
		return singleVersion;
	}

private:
	struct VersionAndChildrenRef {
		VersionAndChildrenRef(Version v, VectorRef<RedwoodRecordRef> children, RedwoodRecordRef upperBound)
		 : version(v), children(children), upperBound(upperBound) {
		}

		VersionAndChildrenRef(Arena &arena, const VersionAndChildrenRef &toCopy)
		 : version(toCopy.version), children(arena, toCopy.children), upperBound(arena, toCopy.upperBound) {
		}

		int expectedSize() const {
			return children.expectedSize() + upperBound.expectedSize();
		}

		std::string toString() const {
			return format("{version=%" PRId64 " children=%s upperbound=%s}", version, ::toString(children).c_str(), upperBound.toString().c_str());
		}

		Version version;
		VectorRef<RedwoodRecordRef> children;
		RedwoodRecordRef upperBound;
	};

	typedef VectorRef<VersionAndChildrenRef> VersionedChildrenT;

	// Utility class for building a vector of internal page entries.
	// Entries must be added in version order.  Modified will be set to true
	// if any entries differ from the original ones.  Additional entries will be
	// added when necessary to reconcile differences between the upper and lower
	// boundaries of consecutive entries.
	struct InternalPageBuilder {
		// Cursor must be at first entry in page
		InternalPageBuilder(const BTreePage::BinaryTree::Cursor &c)
		 : cursor(c), modified(false), childPageCount(0)
		{
		}

	private:
		// This must be called internally, on records whose arena has already been added to the entries arena
		inline void addEntry(const RedwoodRecordRef &rec) {
			if(rec.value.present()) {
				++childPageCount;
			}

			// If no modification detected yet then check that this record is identical to the next
			// record from the original page which is at the current cursor position.
			if(!modified) {
				if(cursor.valid()) {
					if(!rec.identical(cursor.get())) {
						debug_printf("InternalPageBuilder: Found internal page difference.  new: %s  old: %s\n", rec.toString().c_str(), cursor.get().toString().c_str());
						modified = true;
					}
					else {
						cursor.moveNext();
					}
				}
				else {
					debug_printf("InternalPageBuilder: Found internal page difference.  new: %s  old: <end>\n", rec.toString().c_str());
					modified = true;
				}
			}

			entries.push_back(entries.arena(), rec);
		}
	public:
		// Add the child entries from newSet into entries
		void addEntries(VersionAndChildrenRef newSet) {
			// If there are already entries, the last one links to a child page, and its upper bound is not the same
			// as the first lowerBound in newSet (or newSet is empty, as the next newSet is necessarily greater)
			// then add the upper bound of the previous set as a value-less record so that on future reads
			// the previous child page can be decoded correctly.
			if(!entries.empty() && entries.back().value.present()
				&& (newSet.children.empty() || newSet.children.front() != lastUpperBound))
			{
				debug_printf("InternalPageBuilder: Added placeholder %s\n", lastUpperBound.withoutValue().toString().c_str());
				addEntry(lastUpperBound.withoutValue());
			}

			for(auto &child : newSet.children) {
				debug_printf("InternalPageBuilder: Adding child entry %s\n", child.toString().c_str());
				addEntry(child);
			}

			lastUpperBound = newSet.upperBound;
			debug_printf("InternalPageBuilder: New upper bound: %s\n", lastUpperBound.toString().c_str());
		}

		// Finish comparison to existing data if necesary.
		// Handle possible page upper bound changes.
		// If modified is set (see below) and our rightmost entry has a child page and its upper bound
		// (currently in lastUpperBound) does not match the new desired page upper bound, passed as newUpperBound,
		// then write lastUpperBound with no value to allow correct decoding of the rightmost entry.
		// This is only done if modified is set to avoid rewriting this page for this purpose only.
		//
		// After this call, lastUpperBound is internal page's upper bound.
		void finalize(const RedwoodRecordRef &upperBound, const RedwoodRecordRef &decodeUpperBound) {
			debug_printf("InternalPageBuilder::end  modified=%d  upperBound=%s  decodeUpperBound=%s  lastUpperBound=%s\n", modified, upperBound.toString().c_str(), decodeUpperBound.toString().c_str(), lastUpperBound.toString().c_str());
			modified = modified || cursor.valid();
			debug_printf("InternalPageBuilder::end  modified=%d after cursor check\n", modified);

			// If there are boundary key entries and the last one has a child page then the 
			// upper bound for this internal page must match the required upper bound for
			// the last child entry.
			if(!entries.empty() && entries.back().value.present()) {
				debug_printf("InternalPageBuilder::end  last entry is not null\n");

				// If the page contents were not modified so far and the upper bound required
				// for the last child page (lastUpperBound) does not match what the page
				// was encoded with then the page must be modified.
				if(!modified && lastUpperBound != decodeUpperBound) {
					debug_printf("InternalPageBuilder::end  modified set true because lastUpperBound does not match decodeUpperBound\n");
					modified = true;
				}

				if(modified && lastUpperBound != upperBound) {
					debug_printf("InternalPageBuilder::end  Modified is true but lastUpperBound does not match upperBound so adding placeholder\n");
					addEntry(lastUpperBound.withoutValue());
					lastUpperBound = upperBound;
				}
			}
			debug_printf("InternalPageBuilder::end  exit.  modified=%d  upperBound=%s  decodeUpperBound=%s  lastUpperBound=%s\n", modified, upperBound.toString().c_str(), decodeUpperBound.toString().c_str(), lastUpperBound.toString().c_str());
		}

		BTreePage::BinaryTree::Cursor cursor;
		Standalone<VectorRef<RedwoodRecordRef>> entries;
		RedwoodRecordRef lastUpperBound;
		bool modified;
		int childPageCount;
	};

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

			if(isClear())
				return RedwoodRecordRef(userKey, version);

			return RedwoodRecordRef(userKey, version, value);
		}

		std::string toString() const {
			return format("op=%d val='%s'", op, printable(value).c_str());
		}
	};

	// Represents mutations on a single key and a possible clear to a range that begins
	// immediately after that key
	typedef std::map<Version, SingleKeyMutation> SingleKeyMutationsByVersion;
	struct RangeMutation {
		// Mutations for exactly the start key
		SingleKeyMutationsByVersion startKeyMutations;
		// A clear range version, if cleared, for the range starting immediately AFTER the start key
		Optional<Version> rangeClearVersion;

		bool keyCleared() const {
			return startKeyMutations.size() == 1 && startKeyMutations.begin()->second.isClear();
		}

		bool keyChanged() const {
			return !startKeyMutations.empty();
		}

		bool rangeCleared() const {
			return rangeClearVersion.present();
		}

		// Returns true if this RangeMutation doesn't actually mutate anything
		bool noChanges() const {
			return !rangeClearVersion.present() && startKeyMutations.empty();
		}

		std::string toString() const {
			std::string result;
			result.append("rangeClearVersion: ");
			if(rangeClearVersion.present())
				result.append(format("%" PRId64 "", rangeClearVersion.get()));
			else
				result.append("<not present>");
			result.append("  startKeyMutations: ");
			for(SingleKeyMutationsByVersion::value_type const &m : startKeyMutations)
				result.append(format("[%" PRId64 " => %s] ", m.first, m.second.toString().c_str()));
			return result;
		}
	};

	typedef std::map<Key, RangeMutation> MutationBufferT;

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

	IPager2 *m_pager;
	MutationBufferT *m_pBuffer;
	std::map<Version, MutationBufferT> m_mutationBuffers;

	Version m_writeVersion;
	Version m_lastCommittedVersion;
	Version m_newOldestVersion;
	Future<Void> m_latestCommit;
	Future<Void> m_init;
	std::string m_name;
	bool singleVersion;

	// MetaKey changes size so allocate space for it to expand into
	union {
		uint8_t headerSpace[sizeof(MetaKey) + sizeof(LogicalPageID) * 20];
		MetaKey m_header;
	};

	LazyDeleteQueueT m_lazyDeleteQueue;
	int m_maxPartSize;

	// Find or create a mutation buffer boundary for bound and return an iterator to it
	MutationBufferT::iterator insertMutationBoundary(Key boundary) {
		ASSERT(m_pBuffer != nullptr);

		// Find the first split point in buffer that is >= key
		MutationBufferT::iterator ib = m_pBuffer->lower_bound(boundary);

		// Since the initial state of the mutation buffer contains the range '' through
		// the maximum possible key, our search had to have found something.
		ASSERT(ib != m_pBuffer->end());

		// If we found the boundary we are looking for, return its iterator
		if(ib->first == boundary)
			return ib;

		// ib is our insert hint.  Insert the new boundary and set ib to its entry
		ib = m_pBuffer->insert(ib, {boundary, RangeMutation()});

		// ib is certainly > begin() because it is guaranteed that the empty string
		// boundary exists and the only way to have found that is to look explicitly
		// for it in which case we would have returned above.
		MutationBufferT::iterator iPrevious = ib;
		--iPrevious;
		if(iPrevious->second.rangeClearVersion.present()) {
			ib->second.rangeClearVersion = iPrevious->second.rangeClearVersion;
			ib->second.startKeyMutations[iPrevious->second.rangeClearVersion.get()] = SingleKeyMutation();
		}

		return ib;
	}

	// Writes entries to 1 or more pages and return a vector of boundary keys with their IPage(s)
	ACTOR static Future<Standalone<VectorRef<RedwoodRecordRef>>> writePages(VersionedBTree *self, bool minimalBoundaries, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound, VectorRef<RedwoodRecordRef> entries, int height, Version v, BTreePageID previousID) {
		ASSERT(entries.size() > 0);
		state Standalone<VectorRef<RedwoodRecordRef>> records;

		// This is how much space for the binary tree exists in the page, after the header
		state int blockSize = self->m_pager->getUsablePageSize();
		state int pageSize = blockSize - sizeof(BTreePage);
		state int blockCount = 1;

		state int kvBytes = 0;
		state int compressedBytes = BTreePage::BinaryTree::GetTreeOverhead();

		state int start = 0;
		state int i = 0;
		state bool end;

		// For leaf level where minimal boundaries are used require at least 1 entry, otherwise require 4 to enforce a minimum branching factor
		state int minimumEntries = minimalBoundaries ? 1 : 4;
					
		// Lower bound of the page being added to
		state RedwoodRecordRef pageLowerBound = lowerBound->withoutValue();
		state RedwoodRecordRef pageUpperBound;

		while(i <= entries.size()) {
			end = i == entries.size();
			bool flush = end;

			// If not the end, add i to the page if necessary
			if(end) {
				pageUpperBound = upperBound->withoutValue();
			}
			else {
				// Get delta from previous record
				const RedwoodRecordRef &entry = entries[i];
				int deltaSize = entry.deltaSize((i == start) ? pageLowerBound : entries[i - 1]);
				int keySize = entry.key.size();
				int valueSize = entry.value.present() ? entry.value.get().size() : 0;

				int spaceNeeded = sizeof(BTreePage::BinaryTree::Node) + deltaSize;

				debug_printf("Trying to add record %3d of %3lu (i=%3d) klen %4d  vlen %3d  deltaSize %4d  spaceNeeded %4d  compressed %4d / page %4d bytes  %s\n",
					i + 1, entries.size(), i, keySize, valueSize, deltaSize,
					spaceNeeded, compressedBytes, pageSize, entry.toString().c_str());

				int spaceAvailable = pageSize - compressedBytes;

				// Does it fit?
				bool fits = spaceAvailable >= spaceNeeded;

				// If it doesn't fit, either end the current page or increase the page size
				if(!fits) {
					int count = i - start;

					// If not enough entries or page less than half full, increase page size to make the entry fit
					if(count < minimumEntries || spaceAvailable > pageSize / 2) {
						// Figure out how many additional whole or partial blocks are needed
						// newBlocks = ceil ( additional space needed / block size)
						int newBlocks = 1 + (spaceNeeded - spaceAvailable - 1) / blockSize;
						int newPageSize = pageSize + (newBlocks * blockSize);
						if(newPageSize <= BTreePage::BinaryTree::MaximumTreeSize()) {
							blockCount += newBlocks;
							pageSize = newPageSize;
							fits = true;
						}
					}
					if(!fits) {
						pageUpperBound = entry.withoutValue();
					}
				}

				// If the record fits then add it to the page set
				if(fits) {
					kvBytes += keySize + valueSize;
					compressedBytes += spaceNeeded;
					++i;
				}

				flush = !fits;
			}

			// If flush then write a page using records from start to i.  It's guaranteed that pageUpperBound has been set above.
			if(flush) {
				int remaining = entries.size() - i;
				end = remaining == 0;  // i could have been moved above
				int count = i - start;

				// If
				//    - this is not the last page
				//    - the number of entries remaining after this page is less than the count of the current page
				//    - the page that would be written ends on a user key boundary
				// Then adjust the current page item count to half the amount remaining after the start position.
				if(!end && remaining < count && entries[i - 1].key != entries[i].key) {
					i = (start + entries.size()) / 2;
					pageUpperBound = entries[i].withoutValue();
				}

				// If this isn't the final page, shorten the upper boundary
				if(!end && minimalBoundaries) {
					int commonPrefix = pageUpperBound.getCommonPrefixLen(entries[i - 1], 0);
					pageUpperBound.truncate(commonPrefix + 1);
				}

				state std::vector<Reference<IPage>> pages;
				BTreePage *btPage;

				if(blockCount == 1) {
					Reference<IPage> page = self->m_pager->newPageBuffer();
					btPage = (BTreePage *)page->mutate();
					pages.push_back(std::move(page));
				}
				else {
					ASSERT(blockCount > 1);
					int size = blockSize * blockCount;
					btPage = (BTreePage *)new uint8_t[size];
				}

				btPage->height = height;
				btPage->kvBytes = kvBytes;
				btPage->itemCount = i - start;

				int written = btPage->tree().build(&entries[start], &entries[i], &pageLowerBound, &pageUpperBound);
				if(written > pageSize) {
					fprintf(stderr, "ERROR:  Wrote %d bytes to %d byte page (%d blocks). recs %d  kvBytes %d  compressed %d\n", written, pageSize, blockCount, i - start, kvBytes, compressedBytes);
					ASSERT(false);
				}

				// Create chunked pages
				// TODO: Avoid copying page bytes, but this is not trivial due to how pager checksums are currently handled.
				if(blockCount != 1) {
					// Mark the slack in the page buffer as defined
					VALGRIND_MAKE_MEM_DEFINED(((uint8_t *)btPage) + written, (blockCount * blockSize) - written);
					const uint8_t *rptr = (const uint8_t *)btPage;
					for(int b = 0; b < blockCount; ++b) {
						Reference<IPage> page = self->m_pager->newPageBuffer();
						memcpy(page->mutate(), rptr, blockSize);
						rptr += blockSize;
						pages.push_back(std::move(page));
					}
					delete [] (uint8_t *)btPage;
				}

				// Write this btree page, which is made of 1 or more pager pages.
				state int p;
				state BTreePageID childPageID;

				// If we are only writing 1 page and it has the same BTreePageID size as the original they try to reuse the
				// LogicalPageIDs in previousID and try to update them atomically.
				if(end && records.empty() && previousID.size() == pages.size()) {
					for(p = 0; p < pages.size(); ++p) {
						LogicalPageID id = wait(self->m_pager->atomicUpdatePage(previousID[p], pages[p], v));
						childPageID.push_back(records.arena(), id);
					}
				}
				else {
					// Either the original page is being split, or it's not but it has changed BTreePageID size.
					// Either way, there is no point in reusing any of the original page IDs because the parent
					// must be rewritten anyway to count for the change in child count or child links.
					// Free the old IDs, but only once (before the first output record is added).
					if(records.empty()) {
						self->freeBtreePage(previousID, v);
					}
					for(p = 0; p < pages.size(); ++p) {
						LogicalPageID id = wait(self->m_pager->newPageID());
						self->m_pager->updatePage(id, pages[p]);
						childPageID.push_back(records.arena(), id);
					}
				}
				wait(yield());

				// Update activity counts
				++counts.pageWrites;
				if(pages.size() > 1) {
					counts.extPageWrites += pages.size() - 1;
				}

				debug_printf("Flushing %s original=%s start=%d i=%d count=%d\nlower: %s\nupper: %s\n", toString(childPageID).c_str(), toString(previousID).c_str(), start, i, i - start, pageLowerBound.toString().c_str(), pageUpperBound.toString().c_str());
				if(REDWOOD_DEBUG) {
					for(int j = start; j < i; ++j) {
						debug_printf(" %3d: %s\n", j, entries[j].toString().c_str());
					}
					ASSERT(pageLowerBound.key <= pageUpperBound.key);
				}

				// Push a new record onto the results set, without the child page, copying it into the records arena
				records.push_back_deep(records.arena(), pageLowerBound.withoutValue());
				// Set the child page value of the inserted record to childPageID, which has already been allocated in records.arena() above
				records.back().setChildPage(childPageID);

				if(end) {
					break;
				}

				start = i;
				kvBytes = 0;
				compressedBytes = BTreePage::BinaryTree::GetTreeOverhead();
				pageLowerBound = pageUpperBound.withoutValue();
			}
		}

		return records;
	}

	ACTOR static Future<Standalone<VectorRef<RedwoodRecordRef>>> buildNewRoot(VersionedBTree *self, Version version, Standalone<VectorRef<RedwoodRecordRef>> records, int height) {
		debug_printf("buildNewRoot start version %" PRId64 ", %lu records\n", version, records.size());

		// While there are multiple child pages for this version we must write new tree levels.
		while(records.size() > 1) {
			self->m_header.height = ++height;
			Standalone<VectorRef<RedwoodRecordRef>> newRecords = wait(writePages(self, false, &dbBegin, &dbEnd, records, height, version, BTreePageID()));
			debug_printf("Wrote a new root level at version %" PRId64 " height %d size %lu pages\n", version, height, newRecords.size());
			records = newRecords;
		}

		return records;
	}

	class SuperPage : public IPage, ReferenceCounted<SuperPage>, public FastAllocated<SuperPage>{
	public:
		SuperPage(std::vector<Reference<const IPage>> pages) {
			int blockSize = pages.front()->size();
			m_size = blockSize * pages.size();
			m_data = new uint8_t[m_size];
			uint8_t *wptr = m_data;
			for(auto &p : pages) {
				ASSERT(p->size() == blockSize);
				memcpy(wptr, p->begin(), blockSize);
				wptr += blockSize;
			}
		}

		virtual ~SuperPage() {
			delete [] m_data;
		}

		void addref() const {
			ReferenceCounted<SuperPage>::addref();
		}

		void delref() const {
			ReferenceCounted<SuperPage>::delref();
		}

		int size() const {
			return m_size;
		}

		uint8_t const* begin() const {
			return m_data;
		}

		uint8_t* mutate() {
			return m_data;
		}

	private:
		uint8_t *m_data;
		int m_size;
	};

	ACTOR static Future<Reference<const IPage>> readPage(Reference<IPagerSnapshot> snapshot, BTreePageID id, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound, bool forLazyDelete = false) {
		if(!forLazyDelete) {
			debug_printf("readPage() op=read %s @%" PRId64 " lower=%s upper=%s\n", toString(id).c_str(), snapshot->getVersion(), lowerBound->toString().c_str(), upperBound->toString().c_str());
		}
		else {
			debug_printf("readPage() op=readForDeferredClear %s @%" PRId64 " \n", toString(id).c_str(), snapshot->getVersion());
		}

		wait(yield());

		state Reference<const IPage> page;

		++counts.pageReads;
		if(id.size() == 1) {
			Reference<const IPage> p = wait(snapshot->getPhysicalPage(id.front(), !forLazyDelete, false));
			page = p;
		}
		else {
			ASSERT(!id.empty());
			counts.extPageReads += (id.size() - 1);
			std::vector<Future<Reference<const IPage>>> reads;
			for(auto &pageID : id) {
				reads.push_back(snapshot->getPhysicalPage(pageID, !forLazyDelete, false));
			}
			std::vector<Reference<const IPage>> pages = wait(getAll(reads));
			// TODO:  Cache reconstituted super pages somehow, perhaps with help from the Pager.
			page = Reference<const IPage>(new SuperPage(pages));
		}

		debug_printf("readPage() op=readComplete %s @%" PRId64 " \n", toString(id).c_str(), snapshot->getVersion());
		const BTreePage *pTreePage = (const BTreePage *)page->begin();

		if(!forLazyDelete && page->userData == nullptr) {
			debug_printf("readPage() Creating Reader for %s @%" PRId64 " lower=%s upper=%s\n", toString(id).c_str(), snapshot->getVersion(), lowerBound->toString().c_str(), upperBound->toString().c_str());
			page->userData = new BTreePage::BinaryTree::Reader(&pTreePage->tree(), lowerBound, upperBound);
			page->userDataDestructor = [](void *ptr) { delete (BTreePage::BinaryTree::Reader *)ptr; };
		}

		if(!forLazyDelete) {
			debug_printf("readPage() %s\n", pTreePage->toString(false, id, snapshot->getVersion(), lowerBound, upperBound).c_str());
		}

		return page;
	}

	static void preLoadPage(IPagerSnapshot *snapshot, BTreePageID id) {
		++counts.pagePreloads;
		counts.extPagePreloads += (id.size() - 1);
	
		for(auto pageID : id) {
			snapshot->getPhysicalPage(pageID, true, true);
		}
	}

	void freeBtreePage(BTreePageID btPageID, Version v) {
		// Free individual pages at v
		for(LogicalPageID id : btPageID) {
			m_pager->freePage(id, v);
		}
	}

	// Returns list of (version, internal page records, required upper bound)
	ACTOR static Future<Standalone<VersionedChildrenT>> commitSubtree(VersionedBTree *self, MutationBufferT *mutationBuffer, Reference<IPagerSnapshot> snapshot, BTreePageID rootID, bool isLeaf, const RedwoodRecordRef *lowerBound, const RedwoodRecordRef *upperBound, const RedwoodRecordRef *decodeLowerBound, const RedwoodRecordRef *decodeUpperBound) {
		state std::string context;
		if(REDWOOD_DEBUG) {
			context = format("CommitSubtree(root=%s): ", toString(rootID).c_str());
		}

		state Standalone<VersionedChildrenT> results;

		debug_printf("%s lower=%s upper=%s\n", context.c_str(), lowerBound->toString().c_str(), upperBound->toString().c_str());
		debug_printf("%s decodeLower=%s decodeUpper=%s\n", context.c_str(), decodeLowerBound->toString().c_str(), decodeUpperBound->toString().c_str());
		self->counts.commitToPageStart++;

		// Find the slice of the mutation buffer that is relevant to this subtree
		// TODO:  Rather than two lower_bound searches, perhaps just compare each mutation to the upperBound key while iterating
		state MutationBufferT::const_iterator iMutationBoundary = mutationBuffer->upper_bound(lowerBound->key);
		--iMutationBoundary;
		state MutationBufferT::const_iterator iMutationBoundaryEnd = mutationBuffer->lower_bound(upperBound->key);

		if(REDWOOD_DEBUG) {
			debug_printf("%s ---------MUTATION BUFFER SLICE ---------------------\n", context.c_str());
			auto begin = iMutationBoundary;
			while(1) {
				debug_printf("%s Mutation: '%s':  %s\n", context.c_str(), printable(begin->first).c_str(), begin->second.toString().c_str());
				if(begin == iMutationBoundaryEnd) {
					break;
				}
				++begin;
			}
			debug_printf("%s -------------------------------------\n", context.c_str());
		}

		// iMutationBoundary is greatest boundary <= lowerBound->key
		// iMutationBoundaryEnd is least boundary >= upperBound->key

		// If the boundary range iterators are the same then this subtree only has one unique key, which is the same key as the boundary
		// record the iterators are pointing to.  There only two outcomes possible:  Clearing the subtree or leaving it alone.
		// If there are any changes to the one key then the entire subtree should be deleted as the changes for the key
		// do not go into this subtree.
		if(iMutationBoundary == iMutationBoundaryEnd) {
			if(iMutationBoundary->second.keyChanged()) {
				debug_printf("%s lower and upper bound key/version match and key is modified so deleting page, returning %s\n", context.c_str(), toString(results).c_str());
				Version firstKeyChangeVersion = self->singleVersion ? self->getLastCommittedVersion() + 1 : iMutationBoundary->second.startKeyMutations.begin()->first;
				if(isLeaf) {
					self->freeBtreePage(rootID, firstKeyChangeVersion);
				}
				else {
					self->m_lazyDeleteQueue.pushBack(LazyDeleteQueueEntry{firstKeyChangeVersion, rootID});
				}
				return results;
			}

			// Otherwise, no changes to this subtree
			results.push_back_deep(results.arena(), VersionAndChildrenRef(0, VectorRef<RedwoodRecordRef>((RedwoodRecordRef *)decodeLowerBound, 1), *decodeUpperBound));
			debug_printf("%s page contains a single key '%s' which is not changing, returning %s\n", context.c_str(), lowerBound->key.toString().c_str(), toString(results).c_str());
			return results;
		}

		// If one mutation range covers the entire subtree, then check if the entire subtree is modified,
		// unmodified, or possibly/partially modified.
		MutationBufferT::const_iterator iMutationBoundaryNext = iMutationBoundary;
		++iMutationBoundaryNext;
		if(iMutationBoundaryNext == iMutationBoundaryEnd) {
			// Cleared means the entire range covering the subtree was cleared.  It is assumed true
			// if the range starting after the lower mutation boundary was cleared, and then proven false
			// below if possible.
			bool cleared = iMutationBoundary->second.rangeCleared();
			// Unchanged means the entire range covering the subtree was unchanged, it is assumed to be the
			// opposite of cleared() and then proven false below if possible.
			bool unchanged = !cleared;
			debug_printf("%s cleared=%d unchanged=%d\n", context.c_str(), cleared, unchanged);

			// If the lower mutation boundary key is the same as the subtree lower bound then whether or not
			// that key is being changed or cleared affects this subtree.
			if(iMutationBoundary->first == lowerBound->key) {
				// If subtree will be cleared (so far) but the lower boundary key is not cleared then the subtree is not cleared
				if(cleared && !iMutationBoundary->second.keyCleared()) {
					cleared = false;
					debug_printf("%s cleared=%d unchanged=%d\n", context.c_str(), cleared, unchanged);
				}
				// If the subtree looked unchanged (so far) but the lower boundary is is changed then the subtree is changed
				if(unchanged && iMutationBoundary->second.keyChanged()) {
					unchanged = false;
					debug_printf("%s cleared=%d unchanged=%d\n", context.c_str(), cleared, unchanged);
				}
			}

			// If the higher mutation boundary key is the same as the subtree upper bound key then whether 
			// or not it is being changed or cleared affects this subtree.
			if((cleared || unchanged) && iMutationBoundaryEnd->first == upperBound->key) {
				// If the key is being changed then the records in this subtree with the same key must be removed
				// so the subtree is definitely not unchanged, though it may be cleared to achieve the same effect.
				if(iMutationBoundaryEnd->second.keyChanged()) {
					unchanged = false;
					debug_printf("%s cleared=%d unchanged=%d\n", context.c_str(), cleared, unchanged);
				}
				else {
					// If the key is not being changed then the records in this subtree can't be removed so the
					// subtree is not being cleared.
					cleared = false;
					debug_printf("%s cleared=%d unchanged=%d\n", context.c_str(), cleared, unchanged);
				}
			}

			// The subtree cannot be both cleared and unchanged.
			ASSERT(!(cleared && unchanged));

			// If no changes in subtree
			if(unchanged) {
				results.push_back_deep(results.arena(), VersionAndChildrenRef(0, VectorRef<RedwoodRecordRef>((RedwoodRecordRef *)decodeLowerBound, 1), *decodeUpperBound));
				debug_printf("%s no changes on this subtree, returning %s\n", context.c_str(), toString(results).c_str());
				return results;
			}

			// If subtree is cleared
			if(cleared) {
				debug_printf("%s %s cleared, deleting it, returning %s\n", context.c_str(), isLeaf ? "Page" : "Subtree", toString(results).c_str());
				Version clearVersion = self->singleVersion ? self->getLastCommittedVersion() + 1 : iMutationBoundary->second.rangeClearVersion.get();
				if(isLeaf) {
					self->freeBtreePage(rootID, clearVersion);
				}
				else {
					self->m_lazyDeleteQueue.pushBack(LazyDeleteQueueEntry{clearVersion, rootID});
				}
				return results;
			}
		}

		self->counts.commitToPage++;
		state Reference<const IPage> rawPage = wait(readPage(snapshot, rootID, decodeLowerBound, decodeUpperBound));
		state BTreePage *page = (BTreePage *) rawPage->begin();
		ASSERT(isLeaf == page->isLeaf());
		debug_printf("%s commitSubtree(): %s\n", context.c_str(), page->toString(false, rootID, snapshot->getVersion(), decodeLowerBound, decodeUpperBound).c_str());

		state BTreePage::BinaryTree::Cursor cursor = getReader(rawPage)->getCursor();
		cursor.moveFirst();

		state Version writeVersion;

		// Leaf Page
		if(isLeaf) {
			state Standalone<VectorRef<RedwoodRecordRef>> merged;

			debug_printf("%s Leaf page, merging changes.\n", context.c_str());

			// If replacement pages are written they will be at the minimum version seen in the mutations for this leaf
			Version minVersion = invalidVersion;

			// Now, process each mutation range and merge changes with existing data.
			bool firstMutationBoundary = true;
			while(iMutationBoundary != iMutationBoundaryEnd) {
				debug_printf("%s New mutation boundary: '%s': %s\n", context.c_str(), printable(iMutationBoundary->first).c_str(), iMutationBoundary->second.toString().c_str());

				SingleKeyMutationsByVersion::const_iterator iMutations;

				// For the first mutation boundary only, if the boundary key is less than the lower bound for the page
				// then skip startKeyMutations for this boundary, we're only processing this mutation range here to apply
				// a possible clear to existing data.
				if(firstMutationBoundary && iMutationBoundary->first < lowerBound->key) {
					iMutations = iMutationBoundary->second.startKeyMutations.end();
				}
				else {
					iMutations = iMutationBoundary->second.startKeyMutations.begin();
				}
				firstMutationBoundary = false;

				SingleKeyMutationsByVersion::const_iterator iMutationsEnd = iMutationBoundary->second.startKeyMutations.end();

				// Iterate over old versions of the mutation boundary key, outputting if necessary
				bool boundaryKeyWritten = false;
				while(cursor.valid() && cursor.get().key == iMutationBoundary->first) {
					// If not in single version mode or there were no changes to the key
					if(!self->singleVersion || iMutationBoundary->second.noChanges()) {
						merged.push_back(merged.arena(), cursor.get());
						debug_printf("%s Added %s [existing, boundary start]\n", context.c_str(), merged.back().toString().c_str());
						boundaryKeyWritten = true;
					}
					else {
						ASSERT(self->singleVersion);
						debug_printf("%s Skipped %s [existing, boundary start, singleVersion mode]\n", context.c_str(), cursor.get().toString().c_str());
						minVersion = 0;
					}
					cursor.moveNext();
				}

				// TODO:  If a mutation set is equal to the previous existing value of the key, maybe don't write it.
				// Output mutations for the mutation boundary start key
				while(iMutations != iMutationsEnd) {
					const SingleKeyMutation &m = iMutations->second;
					if(m.isClear() || m.value.size() <= self->m_maxPartSize) {
						// If the boundary key was not yet written to the merged list then clears can be skipped.
						// Note that in a more complex scenario where there are multiple sibling pages for the same key, with different
						// versions and/or part numbers, this is still a valid thing to do.  This is because a changing boundary 
						// key (set or clear) will result in any instances (different versions, split parts) of this key
						// on sibling pages to the left of this page to be removed, so an explicit clear need only be stored
						// if a record with the mutation boundary key was already written to this page.
						if(!boundaryKeyWritten && iMutations->second.isClear()) {
							debug_printf("%s Skipped %s [mutation, unnecessary boundary key clear]\n", context.c_str(), m.toRecord(iMutationBoundary->first, iMutations->first).toString().c_str());
						}
						else {
							merged.push_back(merged.arena(), m.toRecord(iMutationBoundary->first, iMutations->first));
							debug_printf("%s Added non-split %s [mutation, boundary start]\n", context.c_str(), merged.back().toString().c_str());
							if(iMutations->first < minVersion || minVersion == invalidVersion)
								minVersion = iMutations->first;
							boundaryKeyWritten = true;
						}
					}
					else {
						if(iMutations->first < minVersion || minVersion == invalidVersion)
							minVersion = iMutations->first;
						int bytesLeft = m.value.size();
						int start = 0;
						RedwoodRecordRef whole(iMutationBoundary->first, iMutations->first, m.value);
						while(bytesLeft > 0) {
							int partSize = std::min(bytesLeft, self->m_maxPartSize);
							// Don't copy the value chunk because this page will stay in memory until after we've built new version(s) of it
							merged.push_back(merged.arena(), whole.split(start, partSize));
							bytesLeft -= partSize;
							start += partSize;
							debug_printf("%s Added split %s [mutation, boundary start] bytesLeft %d\n", context.c_str(), merged.back().toString().c_str(), bytesLeft);
						}
						boundaryKeyWritten = true;
					}
					++iMutations;
				}

				// Get the clear version for this range, which is the last thing that we need from it,
				Optional<Version> clearRangeVersion = iMutationBoundary->second.rangeClearVersion;
				// Advance to the next boundary because we need to know the end key for the current range.
				++iMutationBoundary;

				debug_printf("%s Mutation range end: '%s'\n", context.c_str(), printable(iMutationBoundary->first).c_str());

				// Write existing keys which are less than the next mutation boundary key, clearing if needed.
				while(cursor.valid() && cursor.get().key < iMutationBoundary->first) {
					// TODO:  Remove old versions that are too old

					bool remove = self->singleVersion && clearRangeVersion.present();
					if(!remove) {
						merged.push_back(merged.arena(), cursor.get());
						debug_printf("%s Added %s [existing, middle]\n", context.c_str(), merged.back().toString().c_str());
					}
					else {
						ASSERT(self->singleVersion);
						debug_printf("%s Skipped %s [existing, boundary start, singleVersion mode]\n", context.c_str(), cursor.get().toString().c_str());
						Version clearVersion = clearRangeVersion.get();
						if(clearVersion < minVersion || minVersion == invalidVersion)
							minVersion = clearVersion;
					}

					// If keeping version history, write clears for records that exist in this range if the range was cleared
					if(!self->singleVersion) {
						// Write a clear of this key if needed.  A clear is required if clearRangeVersion is set and the next cursor
						// key is different than the current one.  If the last cursor key in the page is different from the
						// first key in the right sibling page then the page's upper bound will reflect that.
						auto nextCursor = cursor;
						nextCursor.moveNext();

						if(clearRangeVersion.present() && cursor.get().key != nextCursor.getOrUpperBound().key) {
							Version clearVersion = clearRangeVersion.get();
							if(clearVersion < minVersion || minVersion == invalidVersion)
								minVersion = clearVersion;
							merged.push_back(merged.arena(), RedwoodRecordRef(cursor.get().key, clearVersion));
							debug_printf("%s Added %s [existing, middle clear]\n", context.c_str(), merged.back().toString().c_str());
						}
						cursor = nextCursor;
					}
					else {
						cursor.moveNext();
					}
				}
			}

			// Write any remaining existing keys, which are not subject to clears as they are beyond the cleared range.
			bool upperMutationBoundaryKeyChanged = iMutationBoundaryEnd->second.keyChanged();
			while(cursor.valid()) {
				// If the upper mutation boundary is being changed and the cursor's key matches it then stop because none of the earlier
				// versions or fragments of that key should be written.
				if(upperMutationBoundaryKeyChanged && cursor.get().key == iMutationBoundaryEnd->first) {
					debug_printf("%s Skipped %s and beyond [existing, matches changed upper mutation boundary]\n", context.c_str(), cursor.get().toString().c_str());
					Version changedVersion = iMutationBoundaryEnd->second.startKeyMutations.begin()->first;
					if(changedVersion < minVersion || minVersion == invalidVersion)
						minVersion = changedVersion;
					break;
				}
				merged.push_back(merged.arena(), cursor.get());
				debug_printf("%s Added %s [existing, tail]\n", context.c_str(), merged.back().toString().c_str());
				cursor.moveNext();
			}

			// No changes were actually made.  This could happen if the only mutations are clear ranges which do not match any records.
			if(minVersion == invalidVersion) {
				results.push_back_deep(results.arena(), VersionAndChildrenRef(0, VectorRef<RedwoodRecordRef>((RedwoodRecordRef *)decodeLowerBound, 1), *decodeUpperBound));
				debug_printf("%s No changes were made during mutation merge, returning %s\n", context.c_str(), toString(results).c_str());
				return results;
			}
			else {
				debug_printf("%s Changes were made, writing.\n", context.c_str());
			}

			// TODO: Make version and key splits based on contents of merged list, if keeping history

			writeVersion = self->singleVersion ? self->getLastCommittedVersion() + 1 : minVersion;
			// If everything in the page was deleted then this page should be deleted as of the new version
			// Note that if a single range clear covered the entire page then we should not get this far
			if(merged.empty()) {
				debug_printf("%s All leaf page contents were cleared, returning %s\n", context.c_str(), toString(results).c_str());
				self->freeBtreePage(rootID, writeVersion);
				return results;
			}

			state Standalone<VectorRef<RedwoodRecordRef>> entries = wait(writePages(self, true, lowerBound, upperBound, merged, page->height, writeVersion, rootID));
			results.arena().dependsOn(entries.arena());
			results.push_back(results.arena(), VersionAndChildrenRef(writeVersion, entries, *upperBound));
			debug_printf("%s Merge complete, returning %s\n", context.c_str(), toString(results).c_str());
			return results;
		}
		else {
			// Internal Page
			ASSERT(!isLeaf);
			state std::vector<Future<Standalone<VersionedChildrenT>>> futureChildren;

			bool first = true;
			while(cursor.valid()) {
				// The lower bound for the first child is the lowerBound arg
				const RedwoodRecordRef &childLowerBound = first ? *lowerBound : cursor.get();
				first = false;

				// Skip over any children that do not link to a page.  They exist to preserve the ancestors from
				// which adjacent children can borrow prefix bytes.
				// If there are any, then the first valid child page will incur a boundary change to move
				// its lower bound to the left so we can delete the non-linking entry from this page to free up space.
				while(!cursor.get().value.present()) {
					// There should never be an internal page written that has no valid child pages. This loop will find
					// the first valid child link, and if there are no more then execution will not return to this loop.
					ASSERT(cursor.moveNext());
				}

				ASSERT(cursor.valid());

				const RedwoodRecordRef &decodeChildLowerBound = cursor.get();

				BTreePageID pageID = cursor.get().getChildPage();
				ASSERT(!pageID.empty());

				const RedwoodRecordRef &decodeChildUpperBound = cursor.moveNext() ? cursor.get() : *decodeUpperBound;

				// Skip over any next-children which do not actually link to child pages
				while(cursor.valid() && !cursor.get().value.present()) {
					cursor.moveNext();
				}

				const RedwoodRecordRef &childUpperBound = cursor.valid() ? cursor.get() : *upperBound;

				debug_printf("%s recursing to %s lower=%s upper=%s decodeLower=%s decodeUpper=%s\n",
					context.c_str(), toString(pageID).c_str(), childLowerBound.toString().c_str(), childUpperBound.toString().c_str(), decodeChildLowerBound.toString().c_str(), decodeChildUpperBound.toString().c_str());

				/*
				// TODO: If lower bound and upper bound have the same key, do something intelligent if possible
				// 
				if(childLowerBound.key == childUpperBound.key) {
					if(key is modified or cleared) {
						if(self->singleVersion) {
							// In single version mode, don't keep any records with the old key if the key is modified, so return
							// an empty page set to replace the child page
							futureChildren.push_back(VersionedChildrenT({ {0,{} } }));
						}
						else {
							// In versioned mode, there is no need to recurse to this page because new versions of key
							// will go in the right most page that has the same lowerBound key, but since the key is
							// being changed the new version of this page should exclude the old subtree

						}
						else {
							// Return the child page as-is, no need to visit it
							futureChildren.push_back(VersionedChildrenT({ {0,{{childLowerBound, pageID}}} }));
						}
					}
					else {
						// No changes
						futureChildren.push_back(VersionedChildrenT({ {0,{{childLowerBound, pageID}}} }));
					}
				}
				else {
					futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, &childLowerBound, &childUpperBound));
				}
				*/
				// If this page has height of 2 then its children are leaf nodes
				futureChildren.push_back(self->commitSubtree(self, mutationBuffer, snapshot, pageID, page->height == 2, &childLowerBound, &childUpperBound, &decodeChildLowerBound, &decodeChildUpperBound));
			}

			// Waiting one at a time makes debugging easier
			// TODO:  Is it better to use waitForAll()?
			state int k;
			for(k = 0; k < futureChildren.size(); ++k) {
				wait(success(futureChildren[k]));
			}

			if(REDWOOD_DEBUG) {
 				debug_printf("%s Subtree update results\n", context.c_str());
				for(int i = 0; i < futureChildren.size(); ++i) {
					debug_printf("%s subtree result %s\n", context.c_str(), toString(futureChildren[i].get()).c_str());
				}
			}

			// TODO:  Either handle multi-versioned results or change commitSubtree interface to return a single child set.
			ASSERT(self->singleVersion);
			writeVersion = self->getLastCommittedVersion() + 1;
			cursor.moveFirst();
			// All of the things added to pageBuilder will exist in the arenas inside futureChildren or will be upperBound
			InternalPageBuilder pageBuilder(cursor);

			for(int i = 0; i < futureChildren.size(); ++i) {
				VersionedChildrenT versionedChildren = futureChildren[i].get();
				ASSERT(versionedChildren.size() <= 1);

				if(!versionedChildren.empty()) {
					pageBuilder.addEntries(versionedChildren.front());
				}
			}

			pageBuilder.finalize(*upperBound, *decodeUpperBound);

			// If page contents have changed
			if(pageBuilder.modified) {
				// If the page now has no children
				if(pageBuilder.childPageCount == 0) {
					debug_printf("%s All internal page children were deleted so deleting this page too, returning %s\n", context.c_str(), toString(results).c_str());
					self->freeBtreePage(rootID, writeVersion);
					return results;
				}
				else {
					debug_printf("%s Internal page modified, creating replacements.\n", context.c_str());
					debug_printf("%s newChildren=%s  lastUpperBound=%s  upperBound=%s\n", context.c_str(), toString(pageBuilder.entries).c_str(), pageBuilder.lastUpperBound.toString().c_str(), upperBound->toString().c_str());

					ASSERT(pageBuilder.lastUpperBound == *upperBound);

 					Standalone<VectorRef<RedwoodRecordRef>> childEntries = wait(holdWhile(pageBuilder.entries, writePages(self, false, lowerBound, upperBound, pageBuilder.entries, page->height, writeVersion, rootID)));

					results.arena().dependsOn(childEntries.arena());
					results.push_back(results.arena(), VersionAndChildrenRef(0, childEntries, *upperBound));
					debug_printf("%s Internal modified, returning %s\n", context.c_str(), toString(results).c_str());
					return results;
				}
			}
			else {
				results.push_back_deep(results.arena(), VersionAndChildrenRef(0, VectorRef<RedwoodRecordRef>((RedwoodRecordRef *)decodeLowerBound, 1), *decodeUpperBound));
				debug_printf("%s Page has no changes, returning %s\n", context.c_str(), toString(results).c_str());
				return results;
			}
		}
	}

	ACTOR static Future<Void> commit_impl(VersionedBTree *self) {
		state MutationBufferT *mutations = self->m_pBuffer;

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

		// Wait for the latest commit that started to be finished.
		wait(previousCommit);

		self->m_pager->setOldestVersion(self->m_newOldestVersion);
		debug_printf("%s: Beginning commit of version %" PRId64 ", new oldest version set to %" PRId64 "\n", self->m_name.c_str(), writeVersion, self->m_newOldestVersion);

		state bool lazyDeleteStop = false;
		state Future<int> lazyDelete = incrementalSubtreeClear(self, &lazyDeleteStop);

		// Get the latest version from the pager, which is what we will read at
		state Version latestVersion = self->m_pager->getLatestVersion();
		debug_printf("%s: pager latestVersion %" PRId64 "\n", self->m_name.c_str(), latestVersion);

		state Standalone<BTreePageID> rootPageID = self->m_header.root.get();
		state RedwoodRecordRef lowerBound = dbBegin.withPageID(rootPageID);
		Standalone<VersionedChildrenT> versionedRoots = wait(commitSubtree(self, mutations, self->m_pager->getReadSnapshot(latestVersion), rootPageID, self->m_header.height == 1, &lowerBound, &dbEnd, &lowerBound, &dbEnd));
		debug_printf("CommitSubtree(root %s) returned %s\n", toString(rootPageID).c_str(), toString(versionedRoots).c_str());

		// CommitSubtree on the root can only return 1 child at most because the pager interface only supports writing
		// one meta record (which contains the root page) per commit.
		ASSERT(versionedRoots.size() <= 1);

		// If the old root was deleted, write a new empty tree root node and free the old roots
		if(versionedRoots.empty()) {
			debug_printf("Writing new empty root.\n");
			LogicalPageID newRootID = wait(self->m_pager->newPageID());
			Reference<IPage> page = self->m_pager->newPageBuffer();
			makeEmptyRoot(page);
			self->m_header.height = 1;
			self->m_pager->updatePage(newRootID, page);
			rootPageID = BTreePageID((LogicalPageID *)&newRootID, 1);
		}
		else {
        	Standalone<VectorRef<RedwoodRecordRef>> newRootLevel(versionedRoots.front().children, versionedRoots.arena());
			if(newRootLevel.size() == 1) {
				rootPageID = newRootLevel.front().getChildPage();
			}
			else {
				// If the new root level's size is not 1 then build new root level(s)
				Standalone<VectorRef<RedwoodRecordRef>> newRootPage = wait(buildNewRoot(self, latestVersion, newRootLevel, self->m_header.height));
				rootPageID = newRootPage.front().getChildPage();
			}
		}

		self->m_header.root.set(rootPageID, sizeof(headerSpace) - sizeof(m_header));

		lazyDeleteStop = true;
		wait(success(lazyDelete));
		debug_printf("Lazy delete freed %u pages\n", lazyDelete.get());

		self->m_pager->setCommitVersion(writeVersion);

		wait(self->m_lazyDeleteQueue.flush());
		self->m_header.lazyDeleteQueue = self->m_lazyDeleteQueue.getState();

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
		++counts.commits;
		committed.send(Void());

		return Void();
	}

	// InternalCursor is for seeking to and iterating over the 'internal' records (not user-visible) in the Btree.
	// These records are versioned and they can represent deletedness or partial values.
	struct InternalCursor {
	private:
		// Each InternalCursor's position is represented by a reference counted PageCursor, which links
		// to its parent PageCursor, up to a PageCursor representing a cursor on the root page.
		// PageCursors can be shared by many InternalCursors, making InternalCursor copying low overhead
		struct PageCursor : ReferenceCounted<PageCursor>, FastAllocated<PageCursor> {
			Reference<PageCursor> parent;
			BTreePageID pageID;       // Only needed for debugging purposes
			Reference<const IPage> page;
			BTreePage::BinaryTree::Cursor cursor;

			// id will normally reference memory owned by the parent, which is okay because a reference to the parent
			// will be held in the cursor
			PageCursor(BTreePageID id, Reference<const IPage> page, Reference<PageCursor> parent = {})
				: pageID(id), page(page), parent(parent), cursor(getReader().getCursor())
			{
			}

			PageCursor(const PageCursor &toCopy) : parent(toCopy.parent), pageID(toCopy.pageID), page(toCopy.page), cursor(toCopy.cursor) {
			}

			// Convenience method for copying a PageCursor
			Reference<PageCursor> copy() const {
				return Reference<PageCursor>(new PageCursor(*this));
			}

			const BTreePage * btPage() const {
				return (const BTreePage *)page->begin();
			}

			// Multiple InternalCursors can share a Page 
			BTreePage::BinaryTree::Reader & getReader() const {
				return *(BTreePage::BinaryTree::Reader *)page->userData;
			}

			bool isLeaf() const {
				return btPage()->isLeaf();
			}

			Future<Reference<PageCursor>> getChild(Reference<IPagerSnapshot> pager, int readAheadBytes = 0) {
				ASSERT(!isLeaf());
				BTreePage::BinaryTree::Cursor next = cursor;
				next.moveNext();
				const RedwoodRecordRef &rec = cursor.get();
				BTreePageID id = rec.getChildPage();
				Future<Reference<const IPage>> child = readPage(pager, id, &rec, &next.getOrUpperBound());

				// Read ahead siblings at level 2
				if(readAheadBytes > 0 && btPage()->height == 2 && next.valid()) {
					do {
						debug_printf("preloading %s %d bytes left\n", ::toString(next.get().getChildPage()).c_str(), readAheadBytes);
						// If any part of the page was already loaded then stop
						if(next.get().value.present()) {
							preLoadPage(pager.getPtr(), next.get().getChildPage());
							readAheadBytes -= page->size();
						}
					} while(readAheadBytes > 0 && next.moveNext());
				}

				return map(child, [=](Reference<const IPage> page) {
					return Reference<PageCursor>(new PageCursor(id, page, Reference<PageCursor>::addRef(this)));
				});
			}

			std::string toString() const {
				return format("%s, %s", ::toString(pageID).c_str(), cursor.valid() ? cursor.get().toString().c_str() : "<invalid>");
			}
		};

		Standalone<BTreePageID> rootPageID;
		Reference<IPagerSnapshot> pager;
		Reference<PageCursor> pageCursor;

	public:
		InternalCursor() {
		}

		InternalCursor(Reference<IPagerSnapshot> pager, BTreePageID root)
			: pager(pager), rootPageID(root) {
		}

		std::string toString() const {
			std::string r;

			Reference<PageCursor> c = pageCursor;
			int maxDepth = 0;
			while(c) {
				c = c->parent;
				++maxDepth;
			}

			c = pageCursor;
			int depth = maxDepth;
			while(c) {
				r = format("[%d/%d: %s] ", depth--, maxDepth, c->toString().c_str()) + r;
				c = c->parent;
			}
			return r;
		}

		// Returns true if cursor position is a valid leaf page record
		bool valid() const {
			return pageCursor && pageCursor->isLeaf() && pageCursor->cursor.valid();
		}

		// Returns true if cursor position is valid() and has a present record value
		bool present() {
			return valid() && pageCursor->cursor.get().value.present();
		}

		// Returns true if cursor position is present() and has an effective version <= v
		bool presentAtVersion(Version v) {
			return present() && pageCursor->cursor.get().version <= v;
		}

		// Returns true if cursor position is present() and has an effective version <= v
		bool validAtVersion(Version v) {
			return valid() && pageCursor->cursor.get().version <= v;
		}

		const RedwoodRecordRef & get() const {
			return pageCursor->cursor.get();
		}

		// Ensure that pageCursor is not shared with other cursors so we can modify it
		void ensureUnshared() {
			if(!pageCursor->isSoleOwner()) {
				pageCursor = pageCursor->copy();
			}
		}

		Future<Void> moveToRoot() {
			// If pageCursor exists follow parent links to the root
			if(pageCursor) {
				while(pageCursor->parent) {
					pageCursor = pageCursor->parent;
				}
				return Void();
			}

			// Otherwise read the root page
			Future<Reference<const IPage>> root = readPage(pager, rootPageID, &dbBegin, &dbEnd);
			return map(root, [=](Reference<const IPage> p) {
				pageCursor = Reference<PageCursor>(new PageCursor(rootPageID, p));
				return Void();
			});
		}

		ACTOR Future<bool> seekLessThanOrEqual_impl(InternalCursor *self, RedwoodRecordRef query, int prefetchBytes) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if(!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				bool success = self->pageCursor->cursor.seekLessThanOrEqual(query);

				// Skip backwards over internal page entries that do not link to child pages
				if(!self->pageCursor->isLeaf()) {
					// While record has no value, move again
					while(success && !self->pageCursor->cursor.get().value.present()) {
						success = self->pageCursor->cursor.movePrev();
					}
				}

				if(success) {
					// If we found a record <= query at a leaf page then return success
					if(self->pageCursor->isLeaf()) {
						return true;
					}

					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager, prefetchBytes));
					self->pageCursor = child;
				}
				else {
					// No records <= query on this page, so move to immediate previous record at leaf level
					bool success = wait(self->move(false));
					return success;
				}
			}
		}

		Future<bool> seekLTE(RedwoodRecordRef query, int prefetchBytes) {
			return seekLessThanOrEqual_impl(this, query, prefetchBytes);
		}

		ACTOR Future<bool> move_impl(InternalCursor *self, bool forward) {
			// Try to move pageCursor, if it fails to go parent, repeat until it works or root cursor can't be moved
			while(1) {
				self->ensureUnshared();
				bool success = self->pageCursor->cursor.valid() && (forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev());

				// Skip over internal page entries that do not link to child pages
				if(!self->pageCursor->isLeaf()) {
					// While record has no value, move again
					while(success && !self->pageCursor->cursor.get().value.present()) {
						success = forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					}
				}

				// Stop if successful or there's no parent to move to
				if(success || !self->pageCursor->parent) {
					break;
				}

				// Move to parent
				self->pageCursor = self->pageCursor->parent;
			}

			// If pageCursor not valid we've reached an end of the tree
			if(!self->pageCursor->cursor.valid()) {
				return false;
			}

			// While not on a leaf page, move down to get to one.
			while(!self->pageCursor->isLeaf()) {
				// Skip over internal page entries that do not link to child pages
				while(!self->pageCursor->cursor.get().value.present()) {
					bool success = forward ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					if(!success) {
						return false;
					}
				}

				Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager));
				forward ? child->cursor.moveFirst() : child->cursor.moveLast();
				self->pageCursor = child;
			}

			return true;
		}

		Future<bool> move(bool forward) {
			return move_impl(this, forward);
		}

		// Move to the first or last record of the database.
		ACTOR Future<bool> move_end(InternalCursor *self, bool begin) {
			Future<Void> f = self->moveToRoot();

			// f will almost always be ready
			if(!f.isReady()) {
				wait(f);
			}

			self->ensureUnshared();

			loop {
				// Move to first or last record in the page
				bool success = begin ? self->pageCursor->cursor.moveFirst() : self->pageCursor->cursor.moveLast();

				// Skip over internal page entries that do not link to child pages
				if(!self->pageCursor->isLeaf()) {
					// While record has no value, move past it
					while(success && !self->pageCursor->cursor.get().value.present()) {
						success = begin ? self->pageCursor->cursor.moveNext() : self->pageCursor->cursor.movePrev();
					}
				}

				// If it worked, return true if we've reached a leaf page otherwise go to the next child
				if(success) {
					if(self->pageCursor->isLeaf()) {
						return true;
					}

					Reference<PageCursor> child = wait(self->pageCursor->getChild(self->pager));
					self->pageCursor = child;
				}
				else {
					return false;
				}
			}
		}

		Future<bool> moveFirst() {
			return move_end(this, true);
		}
		Future<bool> moveLast() {
			return move_end(this, false);
		}

	};

	// Cursor is for reading and interating over user visible KV pairs at a specific version
	// KeyValueRefs returned become invalid once the cursor is moved
	class Cursor : public IStoreCursor, public ReferenceCounted<Cursor>, public FastAllocated<Cursor>, NonCopyable  {
	public:
		Cursor(Reference<IPagerSnapshot> pageSource, BTreePageID root, Version recordVersion)
			: m_version(recordVersion),
			m_cur1(pageSource, root),
			m_cur2(m_cur1)
		{
		}

		void addref() { ReferenceCounted<Cursor>::addref(); }
		void delref() { ReferenceCounted<Cursor>::delref(); }

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
		Future<Void> findEqual(KeyRef key) override {
			 return find_impl(this, key, 0);
		}
		Future<Void> findFirstEqualOrGreater(KeyRef key, int prefetchBytes) override {
			return find_impl(this, key, 1, prefetchBytes);
		}
		Future<Void> findLastLessOrEqual(KeyRef key, int prefetchBytes) override {
			return find_impl(this, key, -1, prefetchBytes);
		}

		Future<Void> next() override {
			return move(this, true);
		}
		Future<Void> prev() override {
			return move(this, false);
		}

		bool isValid() override {
			return m_kv.present();
		}

		KeyRef getKey() override {
			return m_kv.get().key;
		}

		ValueRef getValue() override {
			return m_kv.get().value;
		}

		std::string toString(bool includePaths = false) const {
			std::string r;
			r += format("Cursor(%p) ver: %" PRId64 " ", this, m_version);
			if(m_kv.present()) {
				r += format("  KV: '%s' -> '%s'", m_kv.get().key.printable().c_str(), m_kv.get().value.printable().c_str());
			}
			else {
				r += "  KV: <np>";
			}
			if(includePaths) {
				r += format("\n Cur1: %s", m_cur1.toString().c_str());
				r += format("\n Cur2: %s", m_cur2.toString().c_str());
			}
			else {
				if(m_cur1.valid()) {
					r += format("\n Cur1: %s", m_cur1.get().toString().c_str());
				}
				if(m_cur2.valid()) {
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
		ACTOR static Future<Void> find_impl(Cursor *self, KeyRef key, int cmp, int prefetchBytes = 0) {
			// Search for the last key at or before (key, version, \xff)
			state RedwoodRecordRef query(key, self->m_version, {}, 0, std::numeric_limits<int32_t>::max());
			self->m_kv.reset();

			wait(success(self->m_cur1.seekLTE(query, prefetchBytes)));
			debug_printf("find%sE(%s): %s\n", cmp > 0 ? "GT" : (cmp == 0 ? "" : "LT"), query.toString().c_str(), self->toString().c_str());

			// If we found the target key with a present value then return it as it is valid for any cmp type
			if(self->m_cur1.present() && self->m_cur1.get().key == key) {
				debug_printf("Target key found, reading full KV pair.  Cursor: %s\n", self->toString().c_str());
				wait(self->readFullKVPair(self));
				return Void();
			}

			// Mode is ==, so if we're still here we didn't find it.
			if(cmp == 0) {
				return Void();
			}

			// Mode is >=, so if we're here we have to go to the next present record at the target version
			// because the seek done above was <= query
			if(cmp > 0) {
				// icur is at a record < query or invalid.

				// If cursor is invalid, try to go to start of tree
				if(!self->m_cur1.valid()) {
					bool valid = wait(self->m_cur1.moveFirst());
					if(!valid) {
						self->m_kv.reset();
						return Void();
					}
				}
				else {
					loop {
						bool valid = wait(self->m_cur1.move(true));
						if(!valid) {
							self->m_kv.reset();
							return Void();
						}

						if(self->m_cur1.get().key > key) {
							break;
						}
					}
				}

				// Get the next present key at the target version.  Handles invalid cursor too.
				wait(self->next());
			}
			else if(cmp < 0) {
				// Mode is <=, which is the same as the seekLTE(query)
				if(!self->m_cur1.valid()) {
					self->m_kv.reset();
					return Void();
				}

				// Move to previous present kv pair at the target version
				wait(self->prev());
			}

			return Void();
		}

		ACTOR static Future<Void> move(Cursor *self, bool fwd) {
			debug_printf("Cursor::move(%d): Start %s\n", fwd, self->toString().c_str());
			ASSERT(self->m_cur1.valid());

			// If kv is present then the key/version at cur1 was already returned so move to a new key
			// Move cur1 until failure or a new key is found, keeping prior record visited in cur2
			if(self->m_kv.present()) {
				ASSERT(self->m_cur1.valid());
				loop {
					self->m_cur2 = self->m_cur1;
					debug_printf("Cursor::move(%d): Advancing cur1 %s\n", fwd, self->toString().c_str());
					bool valid = wait(self->m_cur1.move(fwd));
					if(!valid || self->m_cur1.get().key != self->m_cur2.get().key) {
						break;
					}
				}
			}

			// Given two consecutive cursors c1 and c2, c1 represents a returnable record if
			//    c1.presentAtVersion(v) || (!c2.validAtVersion() || c2.get().key != c1.get().key())
			// Note the distinction between 'present' and 'valid'.  Present means the value for the key
			// exists at the version (but could be the empty string) while valid just means the internal
			// record is in effect at that version but it could indicate that the key was cleared and
			// no longer exists from the user's perspective at that version
			//
			// cur2 must be the record immediately after cur1
			// TODO:  This may already be the case, store state to track this condition and avoid the reset here
			if(self->m_cur1.valid()) {
				self->m_cur2 = self->m_cur1;
				debug_printf("Cursor::move(%d): Advancing cur2 %s\n", fwd, self->toString().c_str());
				wait(success(self->m_cur2.move(true)));
			}

			self->m_kv.reset();
			while(self->m_cur1.valid()) {

				if(self->m_cur1.presentAtVersion(self->m_version) &&
					(!self->m_cur2.validAtVersion(self->m_version) ||
					self->m_cur2.get().key != self->m_cur1.get().key)
				) {
					wait(readFullKVPair(self));
					return Void();
				}

				if(fwd) {
					// Moving forward, move cur2 forward and keep cur1 pointing to the prior (predecessor) record
					debug_printf("Cursor::move(%d): Moving forward %s\n", fwd, self->toString().c_str());
					self->m_cur1 = self->m_cur2;
					wait(success(self->m_cur2.move(true)));
				}
				else {
					// Moving backward, move cur1 backward and keep cur2 pointing to the prior (successor) record
					debug_printf("Cursor::move(%d): Moving backward %s\n", fwd, self->toString().c_str());
					self->m_cur2 = self->m_cur1;
					wait(success(self->m_cur1.move(false)));
				}

			}

			debug_printf("Cursor::move(%d): Exit, end of db reached.  Cursor = %s\n", fwd, self->toString().c_str());
			return Void();
		}

		// Read all of the current key-value record starting at cur1 into kv
		ACTOR static Future<Void> readFullKVPair(Cursor *self) {
			self->m_arena = Arena();
			const RedwoodRecordRef &rec = self->m_cur1.get();
	
			self->m_kv.reset();
			debug_printf("readFullKVPair:  Starting at %s\n", self->toString().c_str());

			// Unsplit value, cur1 will hold the key and value memory
			if(!rec.isMultiPart()) {
				self->m_kv = KeyValueRef(rec.key, rec.value.get());
				debug_printf("readFullKVPair:  Unsplit, exit.  %s\n", self->toString().c_str());

				return Void();
			}

			debug_printf("readFullKVPair:  Split, first record %s\n", rec.toString().c_str());

			// Split value, need to coalesce split value parts into a buffer in arena,
			// after which cur1 will point to the first part and kv.key will reference its key
			ASSERT(rec.chunk.start + rec.value.get().size() == rec.chunk.total);

			// Allocate space for the entire value in the same arena as the key
			state int bytesLeft = rec.chunk.total;
			state StringRef dst = makeString(bytesLeft, self->m_arena);

			loop {
				const RedwoodRecordRef &rec = self->m_cur1.get();

				debug_printf("readFullKVPair:  Adding chunk %s\n", rec.toString().c_str());

				int partSize = rec.value.get().size();
				memcpy(mutateString(dst) + rec.chunk.start, rec.value.get().begin(), partSize);
				bytesLeft -= partSize;
				if(bytesLeft == 0) {
					self->m_kv = KeyValueRef(rec.key, dst);
					return Void();
				}
				ASSERT(bytesLeft > 0);
				// Move backward
				bool success = wait(self->m_cur1.move(false));
				ASSERT(success);
			}
		}
	};

};

RedwoodRecordRef VersionedBTree::dbBegin(StringRef(), 0);
RedwoodRecordRef VersionedBTree::dbEnd(LiteralStringRef("\xff\xff\xff\xff\xff"));
VersionedBTree::Counts VersionedBTree::counts;

class KeyValueStoreRedwoodUnversioned : public IKeyValueStore {
public:
	KeyValueStoreRedwoodUnversioned(std::string filePrefix, UID logID) : m_filePrefix(filePrefix) {
		// TODO: This constructor should really just take an IVersionedStore
		IPager2 *pager = new DWALPager(4096, filePrefix, 0);
		m_tree = new VersionedBTree(pager, filePrefix, true);
		m_init = catchError(init_impl(this));
	}

	Future<Void> init() {
		return m_init;
	}

	ACTOR Future<Void> init_impl(KeyValueStoreRedwoodUnversioned *self) {
		TraceEvent(SevInfo, "RedwoodInit").detail("FilePrefix", self->m_filePrefix);
		wait(self->m_tree->init());
		Version v = self->m_tree->getLatestVersion();
		self->m_tree->setWriteVersion(v + 1);
		TraceEvent(SevInfo, "RedwoodInitComplete").detail("FilePrefix", self->m_filePrefix);
		return Void();
	}

	ACTOR void shutdown(KeyValueStoreRedwoodUnversioned *self, bool dispose) {
		TraceEvent(SevInfo, "RedwoodShutdown").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		if(self->m_error.canBeSet()) {
			self->m_error.sendError(actor_cancelled());  // Ideally this should be shutdown_in_progress
		}
		self->m_init.cancel();
		Future<Void> closedFuture = self->m_tree->onClosed();
		if(dispose)
			self->m_tree->dispose();
		else
			self->m_tree->close();
		wait(closedFuture);
		self->m_closed.send(Void());
		TraceEvent(SevInfo, "RedwoodShutdownComplete").detail("FilePrefix", self->m_filePrefix).detail("Dispose", dispose);
		delete self;
	}

	void close() {
		shutdown(this, false);
	}

	void dispose() {
		shutdown(this, true);
	}

	Future< Void > onClosed() {
		return m_closed.getFuture();
	}

	Future<Void> commit(bool sequential = false) {
		Future<Void> c = m_tree->commit();
		m_tree->setOldestVersion(m_tree->getLatestVersion());
		m_tree->setWriteVersion(m_tree->getWriteVersion() + 1);
		return catchError(c);
	}

	KeyValueStoreType getType() {
		return KeyValueStoreType::SSD_REDWOOD_V1;
	}

	StorageBytes getStorageBytes() {
		return m_tree->getStorageBytes();
	}

	Future< Void > getError() {
		return delayed(m_error.getFuture());
	};

	void clear(KeyRangeRef range, const Arena* arena = 0) {
		debug_printf("CLEAR %s\n", printable(range).c_str());
		m_tree->clear(range);
	}

    void set( KeyValueRef keyValue, const Arena* arena = NULL ) {
		debug_printf("SET %s\n", keyValue.key.printable().c_str());
		m_tree->set(keyValue);
	}

	Future< Standalone< RangeResultRef > > readRange(KeyRangeRef keys, int rowLimit = 1<<30, int byteLimit = 1<<30) {
		debug_printf("READRANGE %s\n", printable(keys).c_str());
		return catchError(readRange_impl(this, keys, rowLimit, byteLimit));
	}

	ACTOR static Future< Standalone< RangeResultRef > > readRange_impl(KeyValueStoreRedwoodUnversioned *self, KeyRange keys, int rowLimit, int byteLimit) {
		self->m_tree->counts.getRanges++;
		state Standalone<RangeResultRef> result;
		state int accumulatedBytes = 0;
		ASSERT( byteLimit > 0 );

		if(rowLimit == 0) {
			return result;
		}

		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());
		// Prefetch is currently only done in the forward direction
		state int prefetchBytes = rowLimit > 1 ? byteLimit : 0;

		if(rowLimit > 0) {
			wait(cur->findFirstEqualOrGreater(keys.begin, prefetchBytes));
			while(cur->isValid() && cur->getKey() < keys.end) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(--rowLimit == 0 || accumulatedBytes >= byteLimit) {
					break;
				}
				wait(cur->next());
			}
		} else {
			wait(cur->findLastLessOrEqual(keys.end));
			if(cur->isValid() && cur->getKey() == keys.end)
				wait(cur->prev());

			while(cur->isValid() && cur->getKey() >= keys.begin) {
				KeyValueRef kv(KeyRef(result.arena(), cur->getKey()), ValueRef(result.arena(), cur->getValue()));
				accumulatedBytes += kv.expectedSize();
				result.push_back(result.arena(), kv);
				if(++rowLimit == 0 || accumulatedBytes >= byteLimit) {
					break;
				}
				wait(cur->prev());
			}
		}

		result.more = rowLimit == 0 || accumulatedBytes >= byteLimit;
		if(result.more) {
			ASSERT(result.size() > 0);
			result.readThrough = result[result.size()-1].key;
		}
		return result;
	}

	ACTOR static Future< Optional<Value> > readValue_impl(KeyValueStoreRedwoodUnversioned *self, Key key, Optional< UID > debugID) {
		self->m_tree->counts.gets++;
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		wait(cur->findEqual(key));
		if(cur->isValid()) {
			return cur->getValue();
		}
		return Optional<Value>();
	}

	Future< Optional< Value > > readValue(KeyRef key, Optional< UID > debugID = Optional<UID>()) {
		return catchError(readValue_impl(this, key, debugID));
	}

	ACTOR static Future< Optional<Value> > readValuePrefix_impl(KeyValueStoreRedwoodUnversioned *self, Key key, int maxLength, Optional< UID > debugID) {
		self->m_tree->counts.gets++;
		state Reference<IStoreCursor> cur = self->m_tree->readAtVersion(self->m_tree->getLastCommittedVersion());

		wait(cur->findEqual(key));
		if(cur->isValid()) {
			Value v = cur->getValue();
			int len = std::min(v.size(), maxLength);
			return Value(cur->getValue().substr(0, len));
		}
		return Optional<Value>();
	}

	Future< Optional< Value > > readValuePrefix(KeyRef key, int maxLength, Optional< UID > debugID = Optional<UID>()) {
		return catchError(readValuePrefix_impl(this, key, maxLength, debugID));
	}

	virtual ~KeyValueStoreRedwoodUnversioned() {
	};

private:
	std::string m_filePrefix;
	VersionedBTree *m_tree;
	Future<Void> m_init;
	Promise<Void> m_closed;
	Promise<Void> m_error;

	template <typename T> inline Future<T> catchError(Future<T> f) {
		return forwardError(f, m_error);
	}
};

IKeyValueStore* keyValueStoreRedwoodV1( std::string const& filename, UID logID) {
	return new KeyValueStoreRedwoodUnversioned(filename, logID);
}

int randomSize(int max) {
	int n = pow(deterministicRandom()->random01(), 3) * max;
	return n;
}

StringRef randomString(Arena &arena, int len, char firstChar = 'a', char lastChar = 'z') {
	++lastChar;
	StringRef s = makeString(len, arena);
	for(int i = 0; i < len; ++i) {
		*(uint8_t *)(s.begin() + i) = (uint8_t)deterministicRandom()->randomInt(firstChar, lastChar);
	}
	return s;
}

Standalone<StringRef> randomString(int len, char firstChar = 'a', char lastChar = 'z') {
	Standalone<StringRef> s;
	(StringRef &)s = randomString(s.arena(), len, firstChar, lastChar);
	return s;
}

KeyValue randomKV(int maxKeySize = 10, int maxValueSize = 5) {
	int kLen = randomSize(1 + maxKeySize);
	int vLen = maxValueSize > 0 ? randomSize(maxValueSize) : 0;

	KeyValue kv;

	kv.key = randomString(kv.arena(), kLen, 'a', 'm');
	for(int i = 0; i < kLen; ++i)
		mutateString(kv.key)[i] = (uint8_t)deterministicRandom()->randomInt('a', 'm');

	if(vLen > 0) {
		kv.value = randomString(kv.arena(), vLen, 'n', 'z');
		for(int i = 0; i < vLen; ++i)
			mutateString(kv.value)[i] = (uint8_t)deterministicRandom()->randomInt('o', 'z');
	}

	return kv;
}

ACTOR Future<int> verifyRange(VersionedBTree *btree, Key start, Key end, Version v, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount) {
	state int errors = 0;
	if(end <= start)
		end = keyAfter(start);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i =    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->upper_bound(std::make_pair(end.toString(),   0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iLast;

	state Reference<IStoreCursor> cur = btree->readAtVersion(v);
	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Start cur=%p\n", v, start.toString().c_str(), end.toString().c_str(), cur.getPtr());

	// Randomly use the cursor for something else first.
	if(deterministicRandom()->coinflip()) {
		state Key randomKey = randomKV().key;
		debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Dummy seek to '%s'\n", v, start.toString().c_str(), end.toString().c_str(), randomKey.toString().c_str());
		wait(deterministicRandom()->coinflip() ? cur->findFirstEqualOrGreater(randomKey) : cur->findLastLessOrEqual(randomKey));
	}

	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Actual seek\n", v, start.toString().c_str(), end.toString().c_str());
	wait(cur->findFirstEqualOrGreater(start));

	state std::vector<KeyValue> results;

	while(cur->isValid() && cur->getKey() < end) {
		// Find the next written kv pair that would be present at this version
		while(1) {
			iLast = i;
			if(i == iEnd)
				break;
			++i;

			if(iLast->first.second <= v
				&& iLast->second.present()
				&& (
					i == iEnd
					|| i->first.first != iLast->first.first
					|| i->first.second > v
				)
			) {
				debug_printf("VerifyRange(@%" PRId64 ", %s, %s) Found key in written map: %s\n", v, start.toString().c_str(), end.toString().c_str(), iLast->first.first.c_str());
				break;
			}
		}

		if(iLast == iEnd) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str());
			break;
		}

		if(cur->getKey() != iLast->first.first) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), iLast->first.first.c_str());
			break;
		}
		if(cur->getValue() != iLast->second.get()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), iLast->second.get().c_str());
			break;
		}

		ASSERT(errors == 0);

		results.push_back(KeyValue(KeyValueRef(cur->getKey(), cur->getValue())));
		wait(cur->next());
	}

	// Make sure there are no further written kv pairs that would be present at this version.
	while(1) {
		iLast = i;
		if(i == iEnd)
			break;
		++i;
		if(iLast->first.second <= v
			&& iLast->second.present()
			&& (
				i == iEnd
				|| i->first.first != iLast->first.first
				|| i->first.second > v
			)
		)
			break;
	}

	if(iLast != iEnd) {
		++errors;
		++*pErrorCount;
		printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has @%" PRId64 " '%s'\n", v, start.toString().c_str(), end.toString().c_str(), iLast->first.second, iLast->first.first.c_str());
	}

	debug_printf("VerifyRangeReverse(@%" PRId64 ", %s, %s): start\n", v, start.toString().c_str(), end.toString().c_str());

	// Randomly use a new cursor for the reverse range read but only if version history is available
	if(!btree->isSingleVersion() && deterministicRandom()->coinflip()) {
		cur = btree->readAtVersion(v);
	}

	// Now read the range from the tree in reverse order and compare to the saved results
	wait(cur->findLastLessOrEqual(end));
	if(cur->isValid() && cur->getKey() == end)
		wait(cur->prev());

	state std::vector<KeyValue>::const_reverse_iterator r = results.rbegin();

	while(cur->isValid() && cur->getKey() >= start) {
		if(r == results.rend()) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs nothing in written map.\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str());
			break;
		}

		if(cur->getKey() != r->key) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), r->key.toString().c_str());
			break;
		}
		if(cur->getValue() != r->value) {
			++errors;
			++*pErrorCount;
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree key '%s' has tree value '%s' vs written '%s'\n", v, start.toString().c_str(), end.toString().c_str(), cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), r->value.toString().c_str());
			break;
		}

		++r;
		wait(cur->prev());
	}

	if(r != results.rend()) {
		++errors;
		++*pErrorCount;
		printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: Tree range ended but written has '%s'\n", v, start.toString().c_str(), end.toString().c_str(), r->key.toString().c_str());
	}

	return errors;
}

ACTOR Future<int> verifyAll(VersionedBTree *btree, Version maxCommittedVersion, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount) {
	// Read back every key at every version set or cleared and verify the result.
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->cend();
	state int errors = 0;

	while(i != iEnd) {
		state std::string key = i->first.first;
		state Version ver = i->first.second;
		if(ver <= maxCommittedVersion) {
			state Optional<std::string> val = i->second;

			state Reference<IStoreCursor> cur = btree->readAtVersion(ver);

			debug_printf("Verifying @%" PRId64 " '%s'\n", ver, key.c_str());
			state Arena arena;
			wait(cur->findEqual(KeyRef(arena, key)));

			if(val.present()) {
				if(!(cur->isValid() && cur->getKey() == key && cur->getValue() == val.get())) {
					++errors;
					++*pErrorCount;
					if(!cur->isValid())
						printf("Verify ERROR: key_not_found: '%s' -> '%s' @%" PRId64 "\n", key.c_str(), val.get().c_str(), ver);
					else if(cur->getKey() != key)
						printf("Verify ERROR: key_incorrect: found '%s' expected '%s' @%" PRId64 "\n", cur->getKey().toString().c_str(), key.c_str(), ver);
					else if(cur->getValue() != val.get())
						printf("Verify ERROR: value_incorrect: for '%s' found '%s' expected '%s' @%" PRId64 "\n", cur->getKey().toString().c_str(), cur->getValue().toString().c_str(), val.get().c_str(), ver);
				}
			} else {
				if(cur->isValid() && cur->getKey() == key) {
					++errors;
					++*pErrorCount;
					printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%" PRId64 "\n", key.c_str(), cur->getValue().toString().c_str(), ver);
				}
			}
		}
		++i;
	}
	return errors;
}

ACTOR Future<Void> verify(VersionedBTree *btree, FutureStream<Version> vStream, std::map<std::pair<std::string, Version>, Optional<std::string>> *written, int *pErrorCount, bool serial) {
	state Future<int> vall;
	state Future<int> vrange;

	try {
		loop {
			state Version v = waitNext(vStream);

			if(btree->isSingleVersion()) {
				v = btree->getLastCommittedVersion();
				debug_printf("Verifying at latest committed version %" PRId64 "\n", v);
				vall = verifyRange(btree, LiteralStringRef(""), LiteralStringRef("\xff\xff"), v, written, pErrorCount);
				if(serial) {
					wait(success(vall));
				}
				vrange = verifyRange(btree, randomKV().key, randomKV().key, v, written, pErrorCount);
				if(serial) {
					wait(success(vrange));
				}
			}
			else {
				debug_printf("Verifying through version %" PRId64 "\n", v);
				vall = verifyAll(btree, v, written, pErrorCount);
				if(serial) {
					wait(success(vall));
				}
				vrange = verifyRange(btree, randomKV().key, randomKV().key, deterministicRandom()->randomInt(1, v + 1), written, pErrorCount);
				if(serial) {
					wait(success(vrange));
				}
			}
			wait(success(vall) && success(vrange));

			debug_printf("Verified through version %" PRId64 ", %d errors\n", v, *pErrorCount);

			if(*pErrorCount != 0)
				break;
		}
	} catch(Error &e) {
		if(e.code() != error_code_end_of_stream && e.code() != error_code_transaction_too_old) {
			throw;
		}
	}
	return Void();
}

// Does a random range read, doesn't trap/report errors
ACTOR Future<Void> randomReader(VersionedBTree *btree) {
	try {
		state Reference<IStoreCursor> cur;
		loop {
			wait(yield());
			if(!cur || deterministicRandom()->random01() > .1) {
				Version v = btree->getLastCommittedVersion();
				if(!btree->isSingleVersion()) {
					v = deterministicRandom()->randomInt(1, v + 1);
				}
				cur = btree->readAtVersion(v);
			}

			state KeyValue kv = randomKV(10, 0);
			wait(cur->findFirstEqualOrGreater(kv.key));
			state int c = deterministicRandom()->randomInt(0, 100);
			while(cur->isValid() && c-- > 0) {
				wait(success(cur->next()));
				wait(yield());
			}
		}
	}
	catch(Error &e) {
		if(e.code() != error_code_transaction_too_old) {
			throw e;
		}
	}

	return Void();
}

struct IntIntPair {
	IntIntPair() {}
	IntIntPair(int k, int v) : k(k), v(v) {}

	IntIntPair(Arena &arena, const IntIntPair &toCopy) {
		*this = toCopy;
	}

	struct Delta {
		bool prefixSource;
		int dk;
		int dv;

		IntIntPair apply(const IntIntPair &base, Arena &arena) {
			return {base.k + dk, base.v + dv};
		}

		void setPrefixSource(bool val) {
			prefixSource = val;
		}

		bool getPrefixSource() const {
			return prefixSource;
		}

		int size() const {
			return sizeof(Delta);
		}

		std::string toString() const {
			return format("DELTA{prefixSource=%d dk=%d(0x%x) dv=%d(0x%x)}", prefixSource, dk, dk, dv, dv);
		}
	};

	int compare(const IntIntPair &rhs) const {
		//printf("compare %s to %s\n", toString().c_str(), rhs.toString().c_str());
		return k - rhs.k;
	}

	bool operator==(const IntIntPair &rhs) const {
		return k == rhs.k;
	}

	int getCommonPrefixLen(const IntIntPair &other, int skip) const {
		return 0;
	}

	int deltaSize(const IntIntPair &base) const {
		return sizeof(Delta);
	}

	int writeDelta(Delta &d, const IntIntPair &base, int commonPrefix = -1) const {
		d.dk = k - base.k;
		d.dv = v - base.v;
		return sizeof(Delta);
	}

	int k;
	int v;

	std::string toString() const {
		return format("{k=%d(0x%x) v=%d(0x%x)}", k, k, v, v);
	}
};

int getCommonIntFieldPrefix2(const RedwoodRecordRef &a, const RedwoodRecordRef &b) {
	RedwoodRecordRef::byte aFields[RedwoodRecordRef::intFieldArraySize];
	RedwoodRecordRef::byte bFields[RedwoodRecordRef::intFieldArraySize];

	a.serializeIntFields(aFields);
	b.serializeIntFields(bFields);

	//printf("a: %s\n", StringRef(aFields, RedwoodRecordRef::intFieldArraySize).toHexString().c_str());
	//printf("b: %s\n", StringRef(bFields, RedwoodRecordRef::intFieldArraySize).toHexString().c_str());

	int i = 0;
	while(i < RedwoodRecordRef::intFieldArraySize && aFields[i] == bFields[i]) {
		++i;
	}

	//printf("%d\n", i);
	return i;
}

void deltaTest(RedwoodRecordRef rec, RedwoodRecordRef base) {
	char buf[500];
	RedwoodRecordRef::Delta &d = *(RedwoodRecordRef::Delta *)buf;

	Arena mem;
	int expectedSize = rec.deltaSize(base, false);
	int deltaSize = rec.writeDelta(d, base);
	RedwoodRecordRef decoded = d.apply(base, mem);

	if(decoded != rec || expectedSize != deltaSize) {
		printf("\n");
		printf("Base:         %s\n", base.toString().c_str());
		printf("ExpectedSize: %d\n", expectedSize);
		printf("DeltaSize:    %d\n", deltaSize);
		printf("Delta:        %s\n", d.toString().c_str());
		printf("Record:       %s\n", rec.toString().c_str());
		printf("Decoded:      %s\n", decoded.toString().c_str());
		printf("RedwoodRecordRef::Delta test failure!\n");
		ASSERT(false);
	}
}

Standalone<RedwoodRecordRef> randomRedwoodRecordRef(int maxKeySize = 3, int maxValueSize = 255) {
	RedwoodRecordRef rec;
	KeyValue kv = randomKV(3, 10);
	rec.key = kv.key;

	if(deterministicRandom()->random01() < .9) {
		rec.value = kv.value;
	}

	rec.version = deterministicRandom()->coinflip() ? 0 : deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max());

	if(deterministicRandom()->coinflip()) {
		rec.chunk.total = deterministicRandom()->randomInt(1, 100000);
		rec.chunk.start = deterministicRandom()->randomInt(0, rec.chunk.total);
	}

	return Standalone<RedwoodRecordRef>(rec, kv.arena());
}

TEST_CASE("!/redwood/correctness/unit/RedwoodRecordRef") {

	// Test pageID stuff.
	{
		LogicalPageID ids[] = {1, 5};
		BTreePageID id(ids, 2);
		RedwoodRecordRef r;
		r.setChildPage(id);
		ASSERT(r.getChildPage() == id);
		ASSERT(r.getChildPage().begin() == id.begin());

		Standalone<RedwoodRecordRef> r2 = r;
		ASSERT(r2.getChildPage() == id);
		ASSERT(r2.getChildPage().begin() != id.begin());
	}

	// Testing common prefix calculation for integer fields using the member function that calculates this directly
	// and by serializing the integer fields to arrays and finding the common prefix length of the two arrays

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), 0, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef(""), 0, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("abc"), 0, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"),  0, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("abcd"), 0, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 0, 0)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 0, 0),
			  RedwoodRecordRef(LiteralStringRef("ab"),  2, LiteralStringRef(""), 1, 3)
	);

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 5, 0),
			  RedwoodRecordRef(LiteralStringRef("abc"), 2, LiteralStringRef(""), 5, 1)
	);

	RedwoodRecordRef::byte varInts[100];
	RedwoodRecordRef::Writer w(varInts);
	RedwoodRecordRef::Reader r(varInts);
	w.writeVarInt(1);
	w.writeVarInt(128);
	w.writeVarInt(32000);
	ASSERT(r.readVarInt() == 1);
	ASSERT(r.readVarInt() == 128);
	ASSERT(r.readVarInt() == 32000);

	RedwoodRecordRef rec1;
	RedwoodRecordRef rec2;

	rec1.version = 0x12345678;
	rec2.version = 0x12995678;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 5);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 14);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = invalidVersion;
	rec2.version = 0;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 0);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.total = 4;
	rec2.chunk.total = 4;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 14);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.start = 4;
	rec2.chunk.start = 4;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 14);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.start = 4;
	rec2.chunk.start = 5;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 13);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	rec1.version = 0x12345678;
	rec2.version = 0x12345678;
	rec1.chunk.total = 256;
	rec2.chunk.total = 512;
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == 9);
	ASSERT(rec1.getCommonIntFieldPrefix(rec2) == getCommonIntFieldPrefix2(rec1, rec2));

	Arena mem;
	double start;
	uint64_t total;
	uint64_t count;
	uint64_t i;

	start = timer();
	total = 0;
	count = 1e9;
	for(i = 0; i < count; ++i) {
		rec1.chunk.total = i & 0xffffff;
		rec2.chunk.total = i & 0xffffff;
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.getCommonIntFieldPrefix(rec2);
	}
	printf("%" PRId64 " getCommonIntFieldPrefix() %g M/s\n", total, count / (timer() - start) / 1e6);

	rec1.key = LiteralStringRef("alksdfjaklsdfjlkasdjflkasdjfklajsdflk;ajsdflkajdsflkjadsf");
	rec2.key = LiteralStringRef("alksdfjaklsdfjlkasdjflkasdjfklajsdflk;ajsdflkajdsflkjadsf");

	start = timer();
	total = 0;
	count = 1e9;
	for(i = 0; i < count; ++i) {
		RedwoodRecordRef::byte fields[RedwoodRecordRef::intFieldArraySize];
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		rec1.serializeIntFields(fields);
		total += fields[RedwoodRecordRef::intFieldArraySize - 1];
	}
	printf("%" PRId64 " serializeIntFields() %g M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 100e6;
	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.getCommonPrefixLen(rec2, 50);
	}
	printf("%" PRId64 " getCommonPrefixLen(skip=50) %g M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 100e6;
	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.getCommonPrefixLen(rec2, 0);
	}
	printf("%" PRId64 " getCommonPrefixLen(skip=0) %g M/s\n", total, count / (timer() - start) / 1e6);

	char buf[1000];
	RedwoodRecordRef::Delta &d = *(RedwoodRecordRef::Delta *)buf;

	start = timer();
	total = 0;
	count = 100e6;
	int commonPrefix = rec1.getCommonPrefixLen(rec2, 0);

	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.writeDelta(d, rec2, commonPrefix);
	}
	printf("%" PRId64 " writeDelta(commonPrefix=%d) %g M/s\n", total, commonPrefix, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 10e6;
	for(i = 0; i < count; ++i) {
		rec1.chunk.start = i & 0xffffff;
		rec2.chunk.start = (i + 1) & 0xffffff;
		total += rec1.writeDelta(d, rec2);
	}
	printf("%" PRId64 " writeDelta() %g M/s\n", total, count / (timer() - start) / 1e6);

	start = timer();
	total = 0;
	count = 1e6;
	for(i = 0; i < count; ++i) {
		Standalone<RedwoodRecordRef> a = randomRedwoodRecordRef();
		Standalone<RedwoodRecordRef> b = randomRedwoodRecordRef();
		deltaTest(a, b);
	}
	printf("Random deltaTest() %g M/s\n", count / (timer() - start) / 1e6);

	return Void();
}

TEST_CASE("!/redwood/correctness/unit/deltaTree/RedwoodRecordRef") {
	const int N = 200;

	RedwoodRecordRef prev;
	RedwoodRecordRef next(LiteralStringRef("\xff\xff\xff\xff"));

	Arena arena;
	std::vector<RedwoodRecordRef> items;
	for(int i = 0; i < N; ++i) {
		std::string k = deterministicRandom()->randomAlphaNumeric(30);
		std::string v = deterministicRandom()->randomAlphaNumeric(30);
		RedwoodRecordRef rec;
		rec.key = StringRef(arena, k);
		rec.version = deterministicRandom()->coinflip() ? deterministicRandom()->randomInt64(0, std::numeric_limits<Version>::max()) : invalidVersion;
		if(deterministicRandom()->coinflip()) {
			rec.value = StringRef(arena, v);
			if(deterministicRandom()->coinflip()) {
				rec.chunk.start = deterministicRandom()->randomInt(0, 100000);
				rec.chunk.total = rec.chunk.start + v.size() + deterministicRandom()->randomInt(0, 100000);
			}
		}
		items.push_back(rec);
		//printf("i=%d %s\n", i, items.back().toString().c_str());
	}
	std::sort(items.begin(), items.end());

	DeltaTree<RedwoodRecordRef> *tree = (DeltaTree<RedwoodRecordRef> *) new uint8_t[N * 100];

	tree->build(&items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialDepth=%d\n", (int)items.size(), (int)tree->size(), (int)tree->initialDepth);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t *)tree, tree->size()).toHexString().c_str());

	DeltaTree<RedwoodRecordRef>::Reader r(tree, &prev, &next);
	DeltaTree<RedwoodRecordRef>::Cursor fwd = r.getCursor();
	DeltaTree<RedwoodRecordRef>::Cursor rev = r.getCursor();

	DeltaTree<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>::Reader rValuesOnly(tree, &prev, &next);
	DeltaTree<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>::Cursor fwdValueOnly = rValuesOnly.getCursor();

	ASSERT(fwd.moveFirst());
	ASSERT(fwdValueOnly.moveFirst());
	ASSERT(rev.moveLast());
	int i = 0;
	while(1) {
		if(fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n", i, fwd.get().toString().c_str(), items[i].toString().c_str());
			printf("Delta: %s\n", fwd.node->raw->delta().toString().c_str());
			ASSERT(false);
		}
		if(rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n", i, rev.get().toString().c_str(), items[items.size() - 1 - i].toString().c_str());
			printf("Delta: %s\n", rev.node->raw->delta().toString().c_str());
			ASSERT(false);
		}
		if(fwdValueOnly.get().value != items[i].value) {
			printf("forward values-only iterator i=%d\n  %s found\n  %s expected\n", i, fwdValueOnly.get().toString().c_str(), items[i].toString().c_str());
			printf("Delta: %s\n", fwdValueOnly.node->raw->delta().toString().c_str());
			ASSERT(false);
		}
		++i;
	
		bool more = fwd.moveNext();
		ASSERT(fwdValueOnly.moveNext() == more);
		ASSERT(rev.movePrev() == more);

		ASSERT(fwd.valid() == more);
		ASSERT(fwdValueOnly.valid() == more);
		ASSERT(rev.valid() == more);

		if(!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	double start = timer();
	DeltaTree<RedwoodRecordRef>::Cursor c = r.getCursor();

	for(int i = 0; i < 20000000; ++i) {
		const RedwoodRecordRef &query = items[deterministicRandom()->randomInt(0, items.size())];
		if(!c.seekLessThanOrEqual(query)) {
			printf("Not found!  query=%s\n", query.toString().c_str());
			ASSERT(false);
		}
		if(c.get() != query) {
			printf("Found incorrect node!  query=%s  found=%s\n", query.toString().c_str(), c.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Elapsed %f\n", elapsed);

	return Void();
}

TEST_CASE("!/redwood/correctness/unit/deltaTree/IntIntPair") {
	const int N = 200;
	IntIntPair prev = {0, 0};
	IntIntPair next = {1000, 0};

	std::vector<IntIntPair> items;
	for(int i = 0; i < N; ++i) {
		items.push_back({i*10, i*1000});
		//printf("i=%d %s\n", i, items.back().toString().c_str());
	}

	DeltaTree<IntIntPair> *tree = (DeltaTree<IntIntPair> *) new uint8_t[10000];

	tree->build(&items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialDepth=%d\n", (int)items.size(), (int)tree->size(), (int)tree->initialDepth);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t *)tree, tree->size()).toHexString().c_str());

	DeltaTree<IntIntPair>::Reader r(tree, &prev, &next);
	DeltaTree<IntIntPair>::Cursor fwd = r.getCursor();
	DeltaTree<IntIntPair>::Cursor rev = r.getCursor();

	ASSERT(fwd.moveFirst());
	ASSERT(rev.moveLast());
	int i = 0;
	while(1) {
		if(fwd.get() != items[i]) {
			printf("forward iterator i=%d\n  %s found\n  %s expected\n", i, fwd.get().toString().c_str(), items[i].toString().c_str());
			ASSERT(false);
		}
		if(rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n", i, rev.get().toString().c_str(), items[items.size() - 1 - i].toString().c_str());
			ASSERT(false);
		}
		++i;
		ASSERT(fwd.moveNext() == rev.movePrev());
		ASSERT(fwd.valid() == rev.valid());
		if(!fwd.valid()) {
			break;
		}
	}
	ASSERT(i == items.size());

	DeltaTree<IntIntPair>::Cursor c = r.getCursor();

	double start = timer();
	for(int i = 0; i < 20000000; ++i) {
		IntIntPair p({deterministicRandom()->randomInt(0, items.size() * 10), 0});
		if(!c.seekLessThanOrEqual(p)) {
			printf("Not found!  query=%s\n", p.toString().c_str());
			ASSERT(false);
		}
		if(c.get().k != (p.k - (p.k % 10))) {
			printf("Found incorrect node!  query=%s  found=%s\n", p.toString().c_str(), c.get().toString().c_str());
			ASSERT(false);
		}
	}
	double elapsed = timer() - start;
	printf("Elapsed %f\n", elapsed);

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

TEST_CASE("!/redwood/correctness/btree") {
	state std::string pagerFile = "unittest_pageFile.redwood";
	IPager2 *pager;

	state bool serialTest = deterministicRandom()->coinflip();
	state bool shortTest = deterministicRandom()->coinflip();
	state bool singleVersion = true; // Multi-version mode is broken / not finished

	state int pageSize = shortTest ? 200 : (deterministicRandom()->coinflip() ? 4096 : deterministicRandom()->randomInt(200, 400));

	// We must be able to fit at least two any two keys plus overhead in a page to prevent
	// a situation where the tree cannot be grown upward with decreasing level size.
	state int maxKeySize = deterministicRandom()->randomInt(4, pageSize * 2);
	state int maxValueSize = deterministicRandom()->randomInt(0, pageSize * 4);
	state int maxCommitSize = shortTest ? 1000 : randomSize(std::min<int>((maxKeySize + maxValueSize) * 20000, 10e6));
	state int mutationBytesTarget = shortTest ? 5000 : randomSize(std::min<int>(maxCommitSize * 100, 100e6));
	state double clearProbability = deterministicRandom()->random01() * .1;
	state double clearPostSetProbability = deterministicRandom()->random01() * .1;
	state double coldStartProbability = deterministicRandom()->random01();
	state double advanceOldVersionProbability = deterministicRandom()->random01();
	state double maxDuration = 60;

	printf("\n");
	printf("serialTest: %d\n", serialTest);
	printf("shortTest: %d\n", shortTest);
	printf("singleVersion: %d\n", serialTest);
	printf("pageSize: %d\n", pageSize);
	printf("maxKeySize: %d\n", maxKeySize);
	printf("maxValueSize: %d\n", maxValueSize);
	printf("maxCommitSize: %d\n", maxCommitSize);
	printf("mutationBytesTarget: %d\n", mutationBytesTarget);
	printf("clearProbability: %f\n", clearProbability);
	printf("clearPostSetProbability: %f\n", clearPostSetProbability);
	printf("coldStartProbability: %f\n", coldStartProbability);
	printf("advanceOldVersionProbability: %f\n", advanceOldVersionProbability);
	printf("\n");

	printf("Deleting existing test data...\n");
	deleteFile(pagerFile);

	printf("Initializing...\n");
	state double startTime = now();
	pager = new DWALPager(pageSize, pagerFile, 0);
	state VersionedBTree *btree = new VersionedBTree(pager, pagerFile, singleVersion);
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

	state Future<Void> commit = Void();

	while(mutationBytes.get() < mutationBytesTarget && (now() - startTime) < maxDuration) {
		if(now() - startTime > 600) {
			mutationBytesTarget = mutationBytes.get();
		}

		// Sometimes advance the version
		if(deterministicRandom()->random01() < 0.10) {
			++version;
			btree->setWriteVersion(version);
		}

		// Sometimes do a clear range
		if(deterministicRandom()->random01() < clearProbability) {
			Key start = randomKV(maxKeySize, 1).key;
			Key end = (deterministicRandom()->random01() < .01) ? keyAfter(start) : randomKV(maxKeySize, 1).key;

			// Sometimes replace start and/or end with a close actual (previously used) value
			if(deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(start);
				if(i != keys.end())
					start = *i;
			}
			if(deterministicRandom()->random01() < .10) {
				auto i = keys.upper_bound(end);
				if(i != keys.end())
					end = *i;
			}

			if(end == start)
				end = keyAfter(start);
			else if(end < start) {
				std::swap(end, start);
			}

			++rangeClears;
			KeyRangeRef range(start, end);
			debug_printf("      Mutation:  Clear '%s' to '%s' @%" PRId64 "\n", start.toString().c_str(), end.toString().c_str(), version);
			auto e = written.lower_bound(std::make_pair(start.toString(), 0));
			if(e != written.end()) {
				auto last = e;
				auto eEnd = written.lower_bound(std::make_pair(end.toString(), 0));
				while(e != eEnd) {
					auto w = *e;
					++e;
					// If e key is different from last and last was present then insert clear for last's key at version
					if(last != eEnd && ((e == eEnd || e->first.first != last->first.first) && last->second.present())) {
						debug_printf("      Mutation:    Clearing key '%s' @%" PRId64 "\n", last->first.first.c_str(), version);

						keyBytesCleared += last->first.first.size();
						mutationBytes += last->first.first.size();
						mutationBytesThisCommit += last->first.first.size();

						// If the last set was at version then just make it not present
						if(last->first.second == version) {
							last->second.reset();
						}
						else {
							written[std::make_pair(last->first.first, version)].reset();
						}
					}
					last = e;
				}
			}

			btree->clear(range);

			// Sometimes set the range start after the clear
			if(deterministicRandom()->random01() < clearPostSetProbability) {
				KeyValue kv = randomKV(0, maxValueSize);
				kv.key = range.begin;
				btree->set(kv);
				written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			}

			// Sometimes set the range end after the clear
			if(deterministicRandom()->random01() < clearPostSetProbability) {
				KeyValue kv = randomKV(0, maxValueSize);
				kv.key = range.end;
				btree->set(kv);
				written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			}
		}
		else {
			// Set a key
			KeyValue kv = randomKV(maxKeySize, maxValueSize);
			// Sometimes change key to a close previously used key
			if(deterministicRandom()->random01() < .01) {
				auto i = keys.upper_bound(kv.key);
				if(i != keys.end())
					kv.key = StringRef(kv.arena(), *i);
			}

			debug_printf("      Mutation:  Set '%s' -> '%s' @%" PRId64 "\n", kv.key.toString().c_str(), kv.value.toString().c_str(), version);

			++sets;
			keyBytesInserted += kv.key.size();
			valueBytesInserted += kv.value.size();
			mutationBytes += (kv.key.size() + kv.value.size());
			mutationBytesThisCommit += (kv.key.size() + kv.value.size());

			btree->set(kv);
			written[std::make_pair(kv.key.toString(), version)] = kv.value.toString();
			keys.insert(kv.key);
		}

		// Commit at end or after this commit's mutation bytes are reached
		if(mutationBytes.get() >= mutationBytesTarget || mutationBytesThisCommit >= mutationBytesTargetThisCommit) {
			// Wait for previous commit to finish
			wait(commit);
			printf("Committed.  Next commit %d bytes, %" PRId64 "/%d (%.2f%%)  Stats: Insert %.2f MB/s  ClearedKeys %.2f MB/s  Total %.2f\n",
				mutationBytesThisCommit,
				mutationBytes.get(),
				mutationBytesTarget,
				(double)mutationBytes.get() / mutationBytesTarget * 100,
				(keyBytesInserted.rate() + valueBytesInserted.rate()) / 1e6,
				keyBytesCleared.rate() / 1e6,
				mutationBytes.rate() / 1e6
			);

			Version v = version;  // Avoid capture of version as a member of *this

			// Sometimes advance the oldest version to close the gap between the oldest and latest versions by a random amount.
			if(deterministicRandom()->random01() < advanceOldVersionProbability) {
				btree->setOldestVersion(btree->getLastCommittedVersion() - deterministicRandom()->randomInt(0, btree->getLastCommittedVersion() - btree->getOldestVersion() + 1));
			}

			commit = map(btree->commit(), [=](Void) {
				printf("Committed: %s\n", VersionedBTree::counts.toString(true).c_str());
				// Notify the background verifier that version is committed and therefore readable
				committedVersions.send(v);
				return Void();
			});

			if(serialTest) {
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
			if(!serialTest && deterministicRandom()->random01() < coldStartProbability) {
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
				IPager2 *pager = new DWALPager(pageSize, pagerFile, 0);
				btree = new VersionedBTree(pager, pagerFile, singleVersion);
				wait(btree->init());

				Version v = btree->getLatestVersion();
				ASSERT(v == version);
				printf("Recovered from disk.  Latest version %" PRId64 "\n", v);

				// Create new promise stream and start the verifier again
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &errorCount, serialTest);
				randomTask = randomReader(btree) || btree->getError();
			}

			++version;
			btree->setWriteVersion(version);
		}

		// Check for errors
		if(errorCount != 0)
			throw internal_error();
	}

	debug_printf("Waiting for outstanding commit\n");
	wait(commit);
	committedVersions.sendError(end_of_stream());
	randomTask.cancel();
	debug_printf("Waiting for verification to complete.\n");
	wait(verifyTask);

	// Check for errors
	if(errorCount != 0)
		throw internal_error();

	wait(btree->destroyAndCheckSanity());

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	debug_printf("Closing.\n");
	wait(closedFuture);

	return Void();
}

ACTOR Future<Void> randomSeeks(VersionedBTree *btree, int count, char firstChar, char lastChar) {
	state Version readVer = btree->getLatestVersion();
	state int c = 0;
	state double readStart = timer();
	printf("Executing %d random seeks\n", count);
	state Reference<IStoreCursor> cur = btree->readAtVersion(readVer);
	while(c < count) {
		state Key k = randomString(20, firstChar, lastChar);
		wait(success(cur->findFirstEqualOrGreater(k)));
		++c;
	}
	double elapsed = timer() - readStart;
	printf("Random seek speed %d/s\n", int(count / elapsed));
	return Void();
}

ACTOR Future<Void> randomScans(VersionedBTree *btree, int count, int width, int readAhead, char firstChar, char lastChar) {
	state Version readVer = btree->getLatestVersion();
	state int c = 0;
	state double readStart = timer();
	printf("Executing %d random scans\n", count);
	state Reference<IStoreCursor> cur = btree->readAtVersion(readVer);
	state bool adaptive = readAhead < 0;
	state int totalScanBytes = 0;
	while(c++ < count) {
		state Key k = randomString(20, firstChar, lastChar);
		wait(success(cur->findFirstEqualOrGreater(k, readAhead)));
		if(adaptive) {
			readAhead = totalScanBytes / c;
		}
		state int w = width;
		while(w > 0 && cur->isValid()) {
			totalScanBytes += cur->getKey().size();
			totalScanBytes += cur->getValue().size();
			wait(cur->next());
			--w;
		}
	}
	double elapsed = timer() - readStart;
	printf("Completed %d scans: readAhead=%d width=%d bytesRead=%d scansRate=%d/s\n", count, readAhead, width, totalScanBytes, int(count / elapsed));
	return Void();
}

TEST_CASE("!/redwood/correctness/pager/cow") {
	state std::string pagerFile = "unittest_pageFile.redwood";
	printf("Deleting old test data\n");
	deleteFile(pagerFile);

	int pageSize = 4096;
	state IPager2 *pager = new DWALPager(pageSize, pagerFile, 0);

	wait(success(pager->init()));
	state LogicalPageID id = wait(pager->newPageID());
	Reference<IPage> p = pager->newPageBuffer();
	memset(p->mutate(), (char)id, p->size());
	pager->updatePage(id, p);
	pager->setMetaKey(LiteralStringRef("asdfasdf"));
	wait(pager->commit());
	Reference<IPage> p2 = wait(pager->readPage(id, true));
	printf("%s\n", StringRef(p2->begin(), p2->size()).toHexString().c_str());

	// TODO: Verify reads, do more writes and reads to make this a real pager validator

	Future<Void> onClosed = pager->onClosed();
	pager->close();
	wait(onClosed);

	return Void();
}

TEST_CASE("!/redwood/performance/set") {
	state SignalableActorCollection actors;
	VersionedBTree::counts.clear();

	// If a test file is passed in by environment then don't write new data to it.
	state bool reload = getenv("TESTFILE") == nullptr;
	state std::string pagerFile = reload ? "unittest.redwood" : getenv("TESTFILE");

	if(reload) {
		printf("Deleting old test data\n");
		deleteFile(pagerFile);
	}

	state int pageSize = 4096;
	state int64_t pageCacheBytes = FLOW_KNOBS->PAGE_CACHE_4K;
	DWALPager *pager = new DWALPager(pageSize, pagerFile, pageCacheBytes);
	state bool singleVersion = true;
	state VersionedBTree *btree = new VersionedBTree(pager, pagerFile, singleVersion);
	wait(btree->init());

	state int nodeCount = 1e9;
	state int maxChangesPerVersion = 5000;
	state int64_t kvBytesTarget = 4e9;
	state int commitTarget = 20e6;
	state int minKeyPrefixBytes = 0;
	state int maxKeyPrefixBytes = 25;
	state int minValueSize = 0;
	state int maxValueSize = 500;
	state int maxConsecutiveRun = 10;
	state char firstKeyChar = 'a';
	state char lastKeyChar = 'b';

	printf("pageSize: %d\n", pageSize);
	printf("pageCacheBytes: %" PRId64 "\n", pageCacheBytes);
	printf("trailingIntegerIndexRange: %d\n", nodeCount);
	printf("maxChangesPerVersion: %d\n", maxChangesPerVersion);
	printf("minKeyPrefixBytes: %d\n", minKeyPrefixBytes);
	printf("maxKeyPrefixBytes: %d\n", maxKeyPrefixBytes);
	printf("maxConsecutiveRun: %d\n", maxConsecutiveRun);
	printf("minValueSize: %d\n", minValueSize);
	printf("maxValueSize: %d\n", maxValueSize);
	printf("commitTarget: %d\n", commitTarget);
	printf("kvBytesTarget: %" PRId64 "\n", kvBytesTarget);
	printf("KeyLexicon '%c' to '%c'\n", firstKeyChar, lastKeyChar);

	state int64_t kvBytes = 0;
	state int64_t kvBytesTotal = 0;
	state int records = 0;
	state Future<Void> commit = Void();
	state std::string value(maxValueSize, 'v');

	printf("Starting.\n");
	state double intervalStart = timer();
	state double start = intervalStart;

	if(reload) {
		while(kvBytesTotal < kvBytesTarget) {
			wait(yield());

			Version lastVer = btree->getLatestVersion();
			state Version version = lastVer + 1;
			btree->setWriteVersion(version);
			int changes = deterministicRandom()->randomInt(0, maxChangesPerVersion);

			while(changes > 0 && kvBytes < commitTarget) {
				KeyValue kv;
				kv.key = randomString(kv.arena(), deterministicRandom()->randomInt(minKeyPrefixBytes + sizeof(uint32_t), maxKeyPrefixBytes + sizeof(uint32_t) + 1), firstKeyChar, lastKeyChar);
				int32_t index = deterministicRandom()->randomInt(0, nodeCount);
				int runLength = deterministicRandom()->randomInt(1, maxConsecutiveRun + 1);

				while(runLength > 0 && changes > 0) {
					*(uint32_t *)(kv.key.end() - sizeof(uint32_t)) = bigEndian32(index++);
					kv.value = StringRef((uint8_t *)value.data(), deterministicRandom()->randomInt(minValueSize, maxValueSize + 1));

					btree->set(kv);

					--runLength;
					--changes;
					kvBytes += kv.key.size() + kv.value.size();
					++records;
				}
			}

			if(kvBytes >= commitTarget) {
				btree->setOldestVersion(btree->getLastCommittedVersion());
				wait(commit);
				printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n", kvBytesTotal / 1e6, kvBytesTotal / (timer() - start) / 1e6);

				// Avoid capturing via this to freeze counter values
				int recs = records;
				int kvb = kvBytes;

				// Capturing invervalStart via this->intervalStart makes IDE's unhappy as they do not know about the actor state object
				double *pIntervalStart = &intervalStart;

				commit = map(btree->commit(), [=](Void result) {
					printf("Committed: %s\n", VersionedBTree::counts.toString(true).c_str());
					double elapsed = timer() - *pIntervalStart;
					printf("Committed %d kvBytes in %d records in %f seconds, %.2f MB/s\n", kvb, recs, elapsed, kvb / elapsed / 1e6);
					*pIntervalStart = timer();
					return Void();
				});

				kvBytesTotal += kvBytes;
				kvBytes = 0;
				records = 0;
			}
		}

		wait(commit);
		printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n", kvBytesTotal / 1e6, kvBytesTotal / (timer() - start) / 1e6);
	}

	int seeks = 1e6;
	printf("Warming cache with seeks\n");
	actors.add(randomSeeks(btree, seeks/3, firstKeyChar, lastKeyChar));
	actors.add(randomSeeks(btree, seeks/3, firstKeyChar, lastKeyChar));
	actors.add(randomSeeks(btree, seeks/3, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	state int ops = 10000;

	printf("Serial scans with adaptive readAhead...\n");
	actors.add(randomScans(btree, ops, 50, -1, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	printf("Serial scans with readAhead 3 pages...\n");
	actors.add(randomScans(btree, ops, 50, 12000, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	printf("Serial scans with readAhead 2 pages...\n");
	actors.add(randomScans(btree, ops, 50, 8000, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	printf("Serial scans with readAhead 1 page...\n");
	actors.add(randomScans(btree, ops, 50, 4000, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	printf("Serial scans...\n");
	actors.add(randomScans(btree, ops, 50, 0, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	printf("Serial seeks...\n");
	actors.add(randomSeeks(btree, ops, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	printf("Parallel seeks...\n");
	actors.add(randomSeeks(btree, ops, firstKeyChar, lastKeyChar));
	actors.add(randomSeeks(btree, ops, firstKeyChar, lastKeyChar));
	actors.add(randomSeeks(btree, ops, firstKeyChar, lastKeyChar));
	wait(actors.signalAndReset());
	printf("Stats: %s\n", VersionedBTree::counts.toString(true).c_str());

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	return Void();
}
