/*
 * VersionedBTree.actor.cpp
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

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/Knobs.h"
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/flow.h"
#include "flow/Histogram.h"
#include <limits>
#include <random>
#include "fdbrpc/ContinuousSample.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/IPager.h"
#include "fdbclient/Tuple.h"
#include "flow/serialize.h"
#include "flow/genericactors.actor.h"
#include "flow/UnitTest.h"
#include "fdbserver/IPager.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/ActorCollection.h"
#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include "fdbclient/CommitTransaction.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/DeltaTree.h"
#include <string.h>
#include <cinttypes>
#include <boost/intrusive/list.hpp>
#include "flow/actorcompiler.h" // must be last include

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

#define PRIORITYMULTILOCK_DEBUG 0

// A multi user lock with a concurrent holder limit where waiters are granted the lock according to
// an integer priority from 0 to maxPriority, inclusive, where higher integers are given priority.
//
// The interface is similar to FlowMutex except that lock holders can drop the lock to release it.
//
// Usage:
//   Lock lock = wait(prioritylock.lock(priorityLevel));
//   lock.release();  // Explicit release, or
//   // let lock and all copies of lock go out of scope to release
class PriorityMultiLock {

public:
	// Waiting on the lock returns a Lock, which is really just a Promise<Void>
	// Calling release() is not necessary, it exists in case the Lock holder wants to explicitly release
	// the Lock before it goes out of scope.
	struct Lock {
		void release() { promise.send(Void()); }

		// This is exposed in case the caller wants to use/copy it directly
		Promise<Void> promise;
	};

private:
	struct Waiter {
		Waiter() : queuedTime(now()) {}
		Promise<Lock> lockPromise;
		double queuedTime;
	};

	typedef Deque<Waiter> Queue;

#if PRIORITYMULTILOCK_DEBUG
#define prioritylock_printf(...) printf(__VA_ARGS__)
#else
#define prioritylock_printf(...)
#endif

public:
	PriorityMultiLock(int concurrency, int maxPriority, int launchLimit = std::numeric_limits<int>::max())
	  : concurrency(concurrency), available(concurrency), waiting(0), launchLimit(launchLimit) {
		waiters.resize(maxPriority + 1);
		fRunner = runner(this);
	}

	~PriorityMultiLock() { prioritylock_printf("destruct"); }

	Future<Lock> lock(int priority = 0) {
		prioritylock_printf("lock begin %s\n", toString().c_str());

		// This shortcut may enable a waiter to jump the line when the releaser loop yields
		if (available > 0) {
			--available;
			Lock p;
			addRunner(p);
			prioritylock_printf("lock exit immediate %s\n", toString().c_str());
			return p;
		}

		Waiter w;
		waiters[priority].push_back(w);
		++waiting;
		prioritylock_printf("lock exit queued %s\n", toString().c_str());
		return w.lockPromise.getFuture();
	}

	std::string toString() const {
		int runnersDone = 0;
		for (int i = 0; i < runners.size(); ++i) {
			if (runners[i].isReady()) {
				++runnersDone;
			}
		}

		std::string s =
		    format("{ ptr=%p concurrency=%d available=%d running=%d waiting=%d runnersQueue=%d runnersDone=%d ",
		           this,
		           concurrency,
		           available,
		           concurrency - available,
		           waiting,
		           runners.size(),
		           runnersDone);

		for (int i = 0; i < waiters.size(); ++i) {
			s += format("p%d_waiters=%u ", i, waiters[i].size());
		}

		s += "}";
		return s;
	}

private:
	void addRunner(Lock& lock) {
		runners.push_back(map(ready(lock.promise.getFuture()), [=](Void) {
			prioritylock_printf("Lock released\n");
			++available;
			if (waiting > 0 || runners.size() > 100) {
				release.trigger();
			}
			return Void();
		}));
	}

	ACTOR static Future<Void> runner(PriorityMultiLock* self) {
		state int sinceYield = 0;
		state Future<Void> error = self->brokenOnDestruct.getFuture();
		state int maxPriority = self->waiters.size() - 1;

		// Priority to try to run tasks from next
		state int priority = maxPriority;
		state Queue* pQueue = &self->waiters[maxPriority];

		// Track the number of waiters unlocked at the same priority in a row
		state int lastPriorityCount = 0;

		loop {
			// Cleanup finished runner futures at the front of the runner queue.
			while (!self->runners.empty() && self->runners.front().isReady()) {
				self->runners.pop_front();
			}

			// Wait for a runner to release its lock
			wait(self->release.onTrigger());
			prioritylock_printf("runner wakeup %s\n", self->toString().c_str());

			if (++sinceYield == 1000) {
				sinceYield = 0;
				wait(delay(0));
			}

			// While there are available slots and there are waiters, launch tasks
			while (self->available > 0 && self->waiting > 0) {
				prioritylock_printf("Checking priority=%d lastPriorityCount=%d %s\n",
				                    priority,
				                    lastPriorityCount,
				                    self->toString().c_str());

				while (!pQueue->empty() && ++lastPriorityCount < self->launchLimit) {
					Waiter w = pQueue->front();
					pQueue->pop_front();
					--self->waiting;
					Lock lock;
					prioritylock_printf("  Running waiter priority=%d wait=%f %s\n",
					                    priority,
					                    now() - w.queuedTime,
					                    self->toString().c_str());
					w.lockPromise.send(lock);

					// Self may have been destructed during the lock callback
					if (error.isReady()) {
						throw error.getError();
					}

					// If the lock was not already released, add it to the runners future queue
					if (lock.promise.canBeSet()) {
						self->addRunner(lock);

						// A slot has been consumed, so stop reading from this queue if there aren't any more
						if (--self->available == 0) {
							break;
						}
					}
				}

				// If there are no more slots available, then don't move to the next priority
				if (self->available == 0) {
					break;
				}

				// Decrease priority, wrapping around to max from 0
				if (priority == 0) {
					priority = maxPriority;
				} else {
					--priority;
				}

				pQueue = &self->waiters[priority];
				lastPriorityCount = 0;
			}
		}
	}

	int concurrency;
	int available;
	int waiting;
	int launchLimit;
	std::vector<Queue> waiters;
	Deque<Future<Void>> runners;
	Future<Void> fRunner;
	AsyncTrigger release;
	Promise<Void> brokenOnDestruct;
};

// Some convenience functions for debugging to stringify various structures
// Classes can add compatibility by either specializing toString<T> or implementing
//   std::string toString() const;
template <typename T>
std::string toString(const T& o) {
	return o.toString();
}

std::string toString(const std::string& s) {
	return s;
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

template <typename K, typename V>
std::string toString(const std::map<K, V>& m) {
	std::string r = "{";
	bool comma = false;
	for (const auto& [key, value] : m) {
		if (comma) {
			r += ", ";
		} else {
			comma = true;
		}
		r += toString(value);
		r += " ";
		r += toString(key);
	}
	r += "}\n";
	return r;
}

template <typename K, typename V>
std::string toString(const std::unordered_map<K, V>& u) {
	std::string r = "{";
	bool comma = false;
	for (const auto& n : u) {
		if (comma) {
			r += ", ";
		} else {
			comma = true;
		}
		r += toString(n.first);
		r += " => ";
		r += toString(n.second);
	}
	r += "}";
	return r;
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

static constexpr int ioMinPriority = 0;
static constexpr int ioLeafPriority = 1;
static constexpr int ioMaxPriority = 3;

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
		QueueID queueID = invalidQueueID;
		LogicalPageID headPageID = invalidLogicalPageID;
		LogicalPageID tailPageID = invalidLogicalPageID;
		uint16_t headOffset;
		// Note that there is no tail index because the tail page is always never-before-written and its index will
		// start at 0
		int64_t numPages;
		int64_t numEntries;
		bool usesExtents = false; // Is this an extent based queue?
		LogicalPageID prevExtentEndPageID = invalidLogicalPageID;
		bool tailPageNewExtent = false;
		KeyRef asKeyRef() const { return KeyRef((uint8_t*)this, sizeof(QueueState)); }

		void fromKeyRef(KeyRef k) { memcpy(this, k.begin(), k.size()); }

		std::string toString() const {
			return format("{queueID: %u head: %s:%d  tail: %s  numPages: %" PRId64 "  numEntries: %" PRId64
			              "  usesExtents:%d}",
			              queueID,
			              ::toString(headPageID).c_str(),
			              (int)headOffset,
			              ::toString(tailPageID).c_str(),
			              numPages,
			              numEntries,
			              usesExtents);
		}
	};

	struct RawPage {
		// The next page of the queue after this one
		LogicalPageID nextPageID;
		// The start offset of the next page
		uint16_t nextOffset;
		// The end offset of the current page
		uint16_t endOffset;
		// Current page within the extent
		LogicalPageID extentCurPageID;
		// The nd page within the extent
		LogicalPageID extentEndPageID;
		// Get pointer to data after page header
		uint8_t* begin() { return (uint8_t*)(this + 1); }
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

		FlowMutex mutex;

		Cursor() : mode(NONE) {}

		// Initialize a cursor.
		void init(FIFOQueue* q = nullptr,
		          Mode m = NONE,
		          LogicalPageID initialPageID = invalidLogicalPageID,
		          bool initExtentInfo = true,
		          bool tailPageNewExtent = false,
		          LogicalPageID endPage = invalidLogicalPageID,
		          int readOffset = 0,
		          LogicalPageID prevExtentEndPageID = invalidLogicalPageID) {
			queue = q;
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
					// For extent based queues, we loads extents at a time during recovery
					if (queue->usesExtents && initExtentInfo)
						loadExtent();
					else
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
				debug_printf("FIFOQueue::Cursor(%s) init. Adding new page %u\n", toString().c_str(), initialPageID);
				addNewPage(initialPageID, 0, true, initExtentInfo, tailPageNewExtent, prevExtentEndPageID);
			}
		}

		// Reset the read cursor (this is used only for extent based remap queue after recovering the remap
		// queue contents via fastpath extent reads)
		void resetRead() {
			ASSERT(mode == POP || mode == READONLY);
			page.clear();
			if (pageID != endPageID) {
				startNextPageLoad(pageID);
			}
		}

		// Since cursors can have async operations pending which modify their state they can't be copied cleanly
		Cursor(const Cursor& other) = delete;

		~Cursor() { writeOperations.cancel(); }

		// A read cursor can be initialized from a pop cursor
		void initReadOnly(const Cursor& c, bool readExtents = false) {
			ASSERT(c.mode == READONLY || c.mode == POP);
			init(c.queue, READONLY, c.pageID, readExtents, false, c.endPageID, c.offset);
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

		// Returns true if the mutex cannot be immediately taken.
		bool isBusy() { return !mutex.available(); }

		// Wait for all operations started before now to be ready, which is done by
		// obtaining and releasing the mutex.
		Future<Void> notBusy() {
			return isBusy() ? map(mutex.take(),
			                      [&](FlowMutex::Lock lock) {
				                      lock.release();
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
			nextPageReader = queue->pager->readPage(
			    PagerEventReasons::MetaData, nonBtreeLevel, nextPageID, ioMaxPriority, true, false);
			if (!nextPageReader.isReady()) {
				nextPageReader = waitOrError(nextPageReader, queue->pagerError);
			}
		}

		Future<Void> loadExtent() {
			ASSERT(mode == POP | mode == READONLY);
			debug_printf("FIFOQueue::Cursor(%s) loadExtent\n", toString().c_str());
			return map(queue->pager->readExtent(pageID), [=](Reference<ArenaPage> p) {
				page = p;
				debug_printf("FIFOQueue::Cursor(%s) loadExtent done. Page: %p\n", toString().c_str(), page->begin());
				return Void();
			});
		}

		void writePage() {
			ASSERT(mode == WRITE);
			debug_printf("FIFOQueue::Cursor(%s) writePage\n", toString().c_str());
			VALGRIND_MAKE_MEM_DEFINED(raw()->begin(), offset);
			VALGRIND_MAKE_MEM_DEFINED(raw()->begin() + offset, queue->dataBytesPerPage - raw()->endOffset);
			queue->pager->updatePage(
			    PagerEventReasons::MetaData, nonBtreeLevel, VectorRef<LogicalPageID>(&pageID, 1), page);
			if (firstPageIDWritten == invalidLogicalPageID) {
				firstPageIDWritten = pageID;
			}
		}

		// Link the current page to newPageID:newOffset and then write it to the pager.
		// The link destination could be a new page at the end of the queue, or the beginning of
		// an existing chain of queue pages.
		// If initializeNewPage is true a page buffer will be allocated for the new page and it will be initialized
		// as a new tail page.
		// if initializeExtentInfo is true in addition to initializeNewPage, update the extentEndPageID info
		// in the mew page being added using newExtentPage and prevExtentEndPageID parameters
		void addNewPage(LogicalPageID newPageID,
		                int newOffset,
		                bool initializeNewPage,
		                bool initializeExtentInfo = false,
		                bool newExtentPage = false,
		                LogicalPageID prevExtentEndPageID = invalidLogicalPageID) {
			ASSERT(mode == WRITE);
			ASSERT(newPageID != invalidLogicalPageID);
			debug_printf("FIFOQueue::Cursor(%s) Adding page %s initPage=%d initExtentInfo=%d newExtentPage=%d\n",
			             toString().c_str(),
			             ::toString(newPageID).c_str(),
			             initializeNewPage,
			             initializeExtentInfo,
			             newExtentPage);

			// Update existing page/newLastPageID and write, if it exists
			if (page) {
				setNext(newPageID, newOffset);
				debug_printf("FIFOQueue::Cursor(%s) Linked new page %s:%d\n",
				             toString().c_str(),
				             ::toString(newPageID).c_str(),
				             newOffset);
				writePage();
				auto p = raw();
				prevExtentEndPageID = p->extentEndPageID;
				if (pageID == prevExtentEndPageID)
					newExtentPage = true;
				debug_printf(
				    "FIFOQueue::Cursor(%s) Linked new page. pageID %u, newPageID %u,  prevExtentEndPageID %u\n",
				    toString().c_str(),
				    pageID,
				    newPageID,
				    prevExtentEndPageID);
			}

			pageID = newPageID;
			offset = newOffset;

			if (BUGGIFY) {
				// Randomly change the byte limit for queue pages.  The min here must be large enough for at least one
				// queue item of any type.  This change will suddenly make some pages being written to seem overfilled
				// but this won't break anything, the next write will just be detected as not fitting and the page will
				// end.
				queue->dataBytesPerPage =
				    deterministicRandom()->randomInt(50, queue->pager->getUsablePageSize() - sizeof(RawPage));
			}

			if (initializeNewPage) {
				debug_printf("FIFOQueue::Cursor(%s) Initializing new page. usesExtents: %d, initializeExtentInfo: %d\n",
				             toString().c_str(),
				             queue->usesExtents,
				             initializeExtentInfo);
				page = queue->pager->newPageBuffer();
				setNext(0, 0);
				auto p = raw();
				ASSERT(newOffset == 0);
				p->endOffset = 0;
				// For extent based queue, update the index of current page within the extent
				if (queue->usesExtents) {
					debug_printf("FIFOQueue::Cursor(%s) Adding page %s init=%d pageCount %d\n",
					             toString().c_str(),
					             ::toString(newPageID).c_str(),
					             initializeNewPage,
					             queue->pager->getPageCount());
					p->extentCurPageID = newPageID;
					if (initializeExtentInfo) {
						int pagesPerExtent = queue->pagesPerExtent;
						if (newExtentPage) {
							p->extentEndPageID = newPageID + pagesPerExtent - 1;
							debug_printf("FIFOQueue::Cursor(%s) newExtentPage. newPageID %u, pagesPerExtent %d, "
							             "ExtentEndPageID: %s\n",
							             toString().c_str(),
							             newPageID,
							             pagesPerExtent,
							             ::toString(p->extentEndPageID).c_str());
						} else {
							p->extentEndPageID = prevExtentEndPageID;
							debug_printf("FIFOQueue::Cursor(%s) Copied ExtentEndPageID: %s\n",
							             toString().c_str(),
							             ::toString(p->extentEndPageID).c_str());
						}
					}
				}
			} else {
				debug_printf("FIFOQueue::Cursor(%s) Clearing new page\n", toString().c_str());
				page.clear();
			}
		}

		// Write item to the next position in the current page or, if it won't fit, add a new page and write it there.
		ACTOR static Future<Void> write_impl(Cursor* self, T item) {
			ASSERT(self->mode == WRITE);

			state FlowMutex::Lock lock;
			state bool mustWait = self->isBusy();
			state int bytesNeeded = Codec::bytesNeeded(item);
			state bool needNewPage =
			    self->pageID == invalidLogicalPageID || self->offset + bytesNeeded > self->queue->dataBytesPerPage;

			if (BUGGIFY) {
				// Sometimes (1% probability) decide a new page is needed as long as at least 1 item has been
				// written (indicated by non-zero offset) to the current page.
				if ((self->offset > 0) && deterministicRandom()->random01() < 0.01) {
					needNewPage = true;
				}
			}

			debug_printf("FIFOQueue::Cursor(%s) write(%s) mustWait=%d needNewPage=%d\n",
			             self->toString().c_str(),
			             ::toString(item).c_str(),
			             mustWait,
			             needNewPage);

			// If we have to wait for the mutex because it's busy, or we need a new page, then wait for the mutex.
			if (mustWait || needNewPage) {
				FlowMutex::Lock _lock = wait(self->mutex.take());
				lock = _lock;

				// If we had to wait because the mutex was busy, then update needNewPage as another writer
				// would have changed the cursor state
				// Otherwise, taking the mutex would be immediate so no other writer could have run
				if (mustWait) {
					needNewPage = self->pageID == invalidLogicalPageID ||
					              self->offset + bytesNeeded > self->queue->dataBytesPerPage;
					if (BUGGIFY) {
						// Sometimes (1% probability) decide a new page is needed as long as at least 1 item has been
						// written (indicated by non-zero offset) to the current page.
						if ((self->offset > 0) && deterministicRandom()->random01() < 0.01) {
							needNewPage = true;
						}
					}
				}
			}

			// If we need a new page, add one.
			if (needNewPage) {
				debug_printf("FIFOQueue::Cursor(%s) write(%s) page is full, adding new page\n",
				             self->toString().c_str(),
				             ::toString(item).c_str(),
				             ::toString(self->pageID).c_str(),
				             bytesNeeded,
				             self->queue->dataBytesPerPage);
				state LogicalPageID newPageID;
				// If this is an extent based queue, check if there is an available page in current extent
				if (self->queue->usesExtents) {
					bool allocateNewExtent = false;
					if (self->pageID != invalidLogicalPageID) {
						auto praw = self->raw();
						if (praw->extentCurPageID < praw->extentEndPageID) {
							newPageID = praw->extentCurPageID + 1;
						} else {
							allocateNewExtent = true;
						}
					} else
						allocateNewExtent = true;
					if (allocateNewExtent) {
						LogicalPageID newPID = wait(self->queue->pager->newExtentPageID(self->queue->queueID));
						newPageID = newPID;
					}
				} else {
					LogicalPageID newPID = wait(self->queue->pager->newPageID());
					newPageID = newPID;
				}
				self->addNewPage(newPageID, 0, true, true);

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
				// Prevent possible stack overflow if too many waiters which require no IO are queued up
				// Using static because multiple Cursors can be involved
				static int sinceYield = 0;
				if (++sinceYield == 1000) {
					sinceYield = 0;
					wait(delay(0));
				}

				lock.release();
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
		                                                  FlowMutex::Lock* lock,
		                                                  bool load) {
			state FlowMutex::Lock localLock;

			// Lock the mutex if it wasn't already locked, so we didn't get a lock pointer
			if (lock == nullptr) {
				debug_printf("FIFOQueue::Cursor(%s) waitThenReadNext locking mutex\n", self->toString().c_str());
				FlowMutex::Lock newLock = wait(self->mutex.take());
				localLock = newLock;
			}

			if (load) {
				debug_printf("FIFOQueue::Cursor(%s) waitThenReadNext waiting for page load\n",
				             self->toString().c_str());
				wait(success(self->nextPageReader));
			}

			state Optional<T> result = wait(self->readNext(upperBound, &localLock));

			// If a lock was not passed in, so this actor locked the mutex above, then unlock it
			if (lock == nullptr) {
				// Prevent possible stack overflow if too many waiters which require no IO are queued up
				// Using static because multiple Cursors can be involved
				static int sinceYield = 0;
				if (++sinceYield == 1000) {
					sinceYield = 0;
					wait(delay(0));
				}

				debug_printf("FIFOQueue::Cursor(%s) waitThenReadNext unlocking mutex\n", self->toString().c_str());
				localLock.release();
			}

			return result;
		}

		// Read the next item at the cursor (if < upperBound), moving to a new page first if the current page is
		// exhausted If locked is true, this call owns the mutex, which would have been locked by readNext() before a
		// recursive call
		Future<Optional<T>> readNext(const Optional<T>& upperBound = {}, FlowMutex::Lock* lock = nullptr) {
			if ((mode != POP && mode != READONLY) || pageID == invalidLogicalPageID || pageID == endPageID) {
				debug_printf("FIFOQueue::Cursor(%s) readNext returning nothing\n", toString().c_str());
				return Optional<T>();
			}

			// If we don't have a lock and the mutex isn't available then acquire it
			if (lock == nullptr && isBusy()) {
				return waitThenReadNext(this, upperBound, lock, false);
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
					return waitThenReadNext(this, upperBound, lock, true);
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
				LogicalPageID extentCurPageID = p->extentCurPageID;
				LogicalPageID extentEndPageID = p->extentEndPageID;
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
					if (!queue->usesExtents) {
						// Freeing the old page must happen after advancing the cursor and clearing the page reference
						// because freePage() could cause a push onto a queue that causes a newPageID() call which could
						// pop() from this very same queue. Queue pages are freed at version 0 because they can be
						// reused after the next commit.
						queue->pager->freePage(oldPageID, 0);
					} else if (extentCurPageID == extentEndPageID) {
						// Figure out the beginning of the extent
						int pagesPerExtent = queue->pagesPerExtent;
						queue->pager->freeExtent(oldPageID - pagesPerExtent + 1);
					}
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
	void create(IPager2* p, LogicalPageID newPageID, std::string queueName, QueueID id, bool extent) {
		debug_printf("FIFOQueue(%s) create from page %s. usesExtents %d\n",
		             queueName.c_str(),
		             toString(newPageID).c_str(),
		             extent);
		pager = p;
		pagerError = pager->getError();
		name = queueName;
		queueID = id;
		numPages = 1;
		numEntries = 0;
		dataBytesPerPage = pager->getUsablePageSize() - sizeof(RawPage);
		usesExtents = extent;
		pagesPerExtent = pager->getPagesPerExtent();
		headReader.init(this, Cursor::POP, newPageID, false, false, newPageID, 0);
		tailWriter.init(this, Cursor::WRITE, newPageID, true, true);
		headWriter.init(this, Cursor::WRITE);
		newTailPage = invalidLogicalPageID;
		debug_printf("FIFOQueue(%s) created\n", queueName.c_str());
	}

	// Load an existing queue from its queue state
	void recover(IPager2* p, const QueueState& qs, std::string queueName, bool loadExtents = true) {
		debug_printf("FIFOQueue(%s) recover from queue state %s\n", queueName.c_str(), qs.toString().c_str());
		pager = p;
		pagerError = pager->getError();
		name = queueName;
		queueID = qs.queueID;
		numPages = qs.numPages;
		numEntries = qs.numEntries;
		dataBytesPerPage = pager->getUsablePageSize() - sizeof(RawPage);
		usesExtents = qs.usesExtents;
		pagesPerExtent = pager->getPagesPerExtent();
		headReader.init(this, Cursor::POP, qs.headPageID, loadExtents, false, qs.tailPageID, qs.headOffset);
		tailWriter.init(this,
		                Cursor::WRITE,
		                qs.tailPageID,
		                true,
		                qs.tailPageNewExtent,
		                invalidLogicalPageID,
		                0,
		                qs.prevExtentEndPageID);
		headWriter.init(this, Cursor::WRITE);
		newTailPage = invalidLogicalPageID;
		debug_printf("FIFOQueue(%s) recovered\n", queueName.c_str());
	}

	// Reset the head reader (this is used only for extent based remap queue after recovering the remap
	// queue contents via fastpath extent reads)
	void resetHeadReader() {
		headReader.resetRead();
		debug_printf("FIFOQueue(%s) read cursor reset\n", name.c_str());
	}

	// Fast path extent peekAll (this zooms through the queue reading extents at a time)
	// Output interface is a promise stream and one vector of results per extent found is sent to the promise stream
	// Once we are finished reading all the extents of the queue, end_of_stream() is sent to mark completion
	ACTOR static Future<Void> peekAll_ext(FIFOQueue* self, PromiseStream<Standalone<VectorRef<T>>> res) {
		state Cursor c;
		c.initReadOnly(self->headReader, true);

		debug_printf("FIFOQueue::Cursor(%s) peekAllExt begin\n", c.toString().c_str());
		if (c.pageID == invalidLogicalPageID || c.pageID == c.endPageID) {
			debug_printf("FIFOQueue::Cursor(%s) peekAllExt returning nothing\n", c.toString().c_str());
			res.sendError(end_of_stream());
			return Void();
		}

		state int entriesRead = 0;
		// Loop over all the extents in this queue
		loop {
			// We now know we are pointing to PageID and it should be read and used, but it may not be loaded yet.
			if (!c.page) {
				debug_printf("FIFOQueue::Cursor(%s) peekAllExt going to Load Extent %s.\n",
				             c.toString().c_str(),
				             ::toString(c.pageID).c_str());
				wait(c.loadExtent());
				wait(yield());
			}

			state Standalone<VectorRef<T>> results;
			results.reserve(results.arena(), self->pagesPerExtent * self->pager->getPhysicalPageSize() / sizeof(T));

			// Loop over all the pages in this extent
			int pageIdx = 0;
			loop {
				// Position the page pointer to current page in the extent
				Reference<ArenaPage> page =
				    c.page->subPage(pageIdx++ * self->pager->getPhysicalPageSize(), self->pager->getLogicalPageSize());
				debug_printf("FIFOQueue::Cursor(%s) peekALLExt %s. Offset %d, CalculateChecksum %d ChecksumInPage %d\n",
				             c.toString().c_str(),
				             toString(c.pageID).c_str(),
				             c.pageID * self->pager->getPhysicalPageSize(),
				             page->calculateChecksum(c.pageID),
				             page->getChecksum());
				if (!page->verifyChecksum(c.pageID)) {
					debug_printf("FIFOQueue::Cursor(%s) peekALLExt checksum failed for %s. Offset %d, "
					             "CalculateChecksum %d ChecksumInPage %d\n",
					             c.toString().c_str(),
					             toString(c.pageID).c_str(),
					             c.pageID * self->pager->getPhysicalPageSize(),
					             page->calculateChecksum(c.pageID),
					             page->getChecksum());
					Error e = checksum_failed();
					TraceEvent(SevError, "RedwoodChecksumFailed")
					    .error(e)
					    .detail("PageID", c.pageID)
					    .detail("PageSize", self->pager->getPhysicalPageSize())
					    .detail("Offset", c.pageID * self->pager->getPhysicalPageSize())
					    .detail("CalculatedChecksum", page->calculateChecksum(c.pageID))
					    .detail("ChecksumInPage", page->getChecksum());
					throw e;
				}

				RawPage* p = (RawPage*)(page->begin());
				int bytesRead;

				// Now loop over all entries inside the current page
				loop {
					ASSERT(c.offset < p->endOffset);
					T result = Codec::readFromBytes(p->begin() + c.offset, bytesRead);
					debug_printf(
					    "FIFOQueue::Cursor(%s) after read of %s\n", c.toString().c_str(), ::toString(result).c_str());
					results.push_back(results.arena(), result);
					entriesRead++;

					c.offset += bytesRead;
					ASSERT(c.offset <= p->endOffset);

					if (c.offset == p->endOffset) {
						c.pageID = p->nextPageID;
						c.offset = p->nextOffset;
						debug_printf("FIFOQueue::Cursor(%s) peekAllExt page exhausted, moved to new page\n",
						             c.toString().c_str());
						debug_printf("FIFOQueue:: nextPageID=%s, extentCurPageID=%s, extentEndPageID=%s\n",
						             ::toString(p->nextPageID).c_str(),
						             ::toString(p->extentCurPageID).c_str(),
						             ::toString(p->extentEndPageID).c_str());
						break;
					}
				} // End of Page

				// Check if we have reached the end of the queue
				if (c.pageID == invalidLogicalPageID || c.pageID == c.endPageID) {
					debug_printf("FIFOQueue::Cursor(%s) peekAllExt Queue exhausted\n", c.toString().c_str());
					res.send(results);

					// Since we have reached the end of the queue, verify that the number of entries read matches
					// the queue metadata. If it does, send end_of_stream() to mark completion, else throw an error
					if (entriesRead != self->numEntries) {
						Error e = internal_error(); // TODO:  Something better?
						TraceEvent(SevError, "RedwoodQueueNumEntriesMisMatch")
						    .error(e)
						    .detail("EntriesRead", entriesRead)
						    .detail("ExpectedEntries", self->numEntries);
						throw e;
					}
					res.sendError(end_of_stream());
					return Void();
				}

				// Check if we have reached the end of current extent
				if (p->extentCurPageID == p->extentEndPageID) {
					c.page.clear();
					debug_printf("FIFOQueue::Cursor(%s) peekAllExt extent exhausted, moved to new extent\n",
					             c.toString().c_str());

					// send an extent worth of entries to the promise stream
					res.send(results);
					self->pager->releaseExtentReadLock();
					break;
				}
			} // End of Extent
		} // End of Queue
	}

	Future<Void> peekAllExt(PromiseStream<Standalone<VectorRef<T>>> resStream) { return peekAll_ext(this, resStream); }

	ACTOR static Future<Standalone<VectorRef<T>>> peekAll_impl(FIFOQueue* self) {
		state Standalone<VectorRef<T>> results;
		state Cursor c;
		c.initReadOnly(self->headReader);
		results.reserve(results.arena(), self->numEntries);

		state int sinceYield = 0;
		loop {
			Optional<T> x = wait(c.readNext());
			if (!x.present()) {
				break;
			}
			results.push_back(results.arena(), x.get());

			// yield periodically to avoid overflowing the stack
			if (++sinceYield >= 100) {
				sinceYield = 0;
				wait(yield());
			}
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
		s.queueID = queueID;
		s.headOffset = headReader.offset;
		s.headPageID = headReader.pageID;
		s.tailPageID = tailWriter.pageID;
		s.numEntries = numEntries;
		s.numPages = numPages;
		s.usesExtents = usesExtents;
		s.tailPageNewExtent = tailPageNewExtent;
		s.prevExtentEndPageID = prevExtentEndPageID;

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
			if (self->newTailPage.isReady() && self->newTailPage.get() == invalidLogicalPageID) {
				if (self->tailWriter.pendingTailWrites()) {
					debug_printf("FIFOQueue(%s) preFlush starting to get new page ID\n", self->name.c_str());
					if (self->usesExtents) {
						if (self->tailWriter.pageID == invalidLogicalPageID) {
							self->newTailPage = self->pager->newExtentPageID(self->queueID);
							self->tailPageNewExtent = true;
							self->prevExtentEndPageID = invalidLogicalPageID;
						} else {
							auto p = self->tailWriter.raw();
							debug_printf(
							    "FIFOQueue(%s) newTailPage tailWriterPage %u extentCurPageID %u, extentEndPageID %u\n",
							    self->name.c_str(),
							    self->tailWriter.pageID,
							    p->extentCurPageID,
							    p->extentEndPageID);
							if (p->extentCurPageID < p->extentEndPageID) {
								self->newTailPage = p->extentCurPageID + 1;
								self->tailPageNewExtent = false;
								self->prevExtentEndPageID = p->extentEndPageID;
							} else {
								self->newTailPage = self->pager->newExtentPageID(self->queueID);
								self->tailPageNewExtent = true;
								self->prevExtentEndPageID = invalidLogicalPageID;
							}
						}
						debug_printf("FIFOQueue(%s) newTailPage tailPageNewExtent:%d prevExtentEndPageID: %u "
						             "tailWriterPage %u\n",
						             self->name.c_str(),
						             self->tailPageNewExtent,
						             self->prevExtentEndPageID,
						             self->tailWriter.pageID);
					} else
						self->newTailPage = self->pager->newPageID();
					workPending = true;
				} else {
					if (self->usesExtents) {
						auto p = self->tailWriter.raw();
						self->prevExtentEndPageID = p->extentEndPageID;
						self->tailPageNewExtent = false;
						debug_printf("FIFOQueue(%s) newTailPage tailPageNewExtent: %d prevExtentEndPageID: %u "
						             "tailWriterPage %u\n",
						             self->name.c_str(),
						             self->tailPageNewExtent,
						             self->prevExtentEndPageID,
						             self->tailWriter.pageID);
					}
				}
			}
		}

		debug_printf("FIFOQueue(%s) preFlush returning %d\n", self->name.c_str(), workPending);
		return workPending;
	}

	Future<bool> preFlush() { return preFlush_impl(this); }

	void finishFlush() {
		debug_printf("FIFOQueue(%s) finishFlush start\n", name.c_str());
		ASSERT(!isBusy());
		bool initTailWriter = true;

		// If a new tail page was allocated, link the last page of the tail writer to it.
		if (newTailPage.get() != invalidLogicalPageID) {
			tailWriter.addNewPage(newTailPage.get(), 0, false, false);
			// The flush sequence allocated a page and added it to the queue so increment numPages
			++numPages;

			// newPage() should be ready immediately since a pageID is being explicitly passed.
			ASSERT(!tailWriter.isBusy());

			newTailPage = invalidLogicalPageID;
			initTailWriter = true;
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
		debug_printf("FIFOQueue(%s) Reset tailWriter cursor. tailPageNewExtent: %d\n", name.c_str(), tailPageNewExtent);
		tailWriter.init(this,
		                Cursor::WRITE,
		                tailWriter.pageID,
		                initTailWriter /*false*/,
		                tailPageNewExtent,
		                invalidLogicalPageID,
		                0,
		                prevExtentEndPageID);
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
	QueueID queueID;
	Future<Void> pagerError;

	int64_t numPages;
	int64_t numEntries;
	int dataBytesPerPage;
	int pagesPerExtent;
	bool usesExtents;
	bool tailPageNewExtent;
	LogicalPageID prevExtentEndPageID;

	Cursor headReader;
	Cursor tailWriter;
	Cursor headWriter;

	Future<LogicalPageID> newTailPage;

	// For debugging
	std::string name;

	void toTraceEvent(TraceEvent& e, const char* prefix) const {
		e.detail(format("%sRecords", prefix), numEntries);
		e.detail(format("%sPages", prefix), numPages);
		e.detail(format("%sRecordsPerPage", prefix), numPages > 0 ? (double)numEntries / numPages : 0);
	}
};

int nextPowerOf2(uint32_t x) {
	return 1 << (32 - clz(x - 1));
}

struct RedwoodMetrics {
	static constexpr unsigned int btreeLevels = 5;
	static int maxRecordCount;

	struct EventReasonsArray {
		unsigned int eventReasons[(size_t)PagerEvents::MAXEVENTS][(size_t)PagerEventReasons::MAXEVENTREASONS];

		EventReasonsArray() { clear(); }
		void clear() { memset(eventReasons, 0, sizeof(eventReasons)); }

		void addEventReason(PagerEvents event, PagerEventReasons reason) {
			eventReasons[(size_t)event][(size_t)reason] += 1;
		}

		unsigned int getEventReason(PagerEvents event, PagerEventReasons reason) const {
			return eventReasons[(size_t)event][(size_t)reason];
		}

		std::string toString(unsigned int level, double elapsed) const {
			std::string result;

			const auto& pairs = (level == 0 ? L0PossibleEventReasonPairs : possibleEventReasonPairs);
			PagerEvents prevEvent = pairs.front().first;
			std::string lineStart = (level == 0) ? "" : "\t";

			for (const auto& p : pairs) {
				if (p.first != prevEvent) {
					result += "\n";
					result += lineStart;
				}

				std::string name =
				    format("%s%s", PagerEventsStrings[(int)p.first], PagerEventReasonsStrings[(int)p.second]);
				int count = getEventReason(p.first, p.second);
				result += format("%-15s %8u %8u/s  ", name.c_str(), count, int(count / elapsed));

				prevEvent = p.first;
			}

			return result;
		}

		void toTraceEvent(TraceEvent* t, unsigned int level) const {
			const auto& pairs = (level == 0 ? L0PossibleEventReasonPairs : possibleEventReasonPairs);
			for (const auto& p : pairs) {
				std::string name =
				    format(level == 0 ? "" : "L%d", level) +
				    format("%s%s", PagerEventsStrings[(int)p.first], PagerEventReasonsStrings[(int)p.second]);
				int count = getEventReason(p.first, p.second);
				t->detail(std::move(name), count);
			}
		}
	};

	// Metrics by level
	struct Level {
		struct Counters {
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
			EventReasonsArray events;
		};
		Counters metrics;
		Reference<Histogram> buildFillPctSketch;
		Reference<Histogram> modifyFillPctSketch;
		Reference<Histogram> buildStoredPctSketch;
		Reference<Histogram> modifyStoredPctSketch;
		Reference<Histogram> buildItemCountSketch;
		Reference<Histogram> modifyItemCountSketch;

		Level() { metrics = {}; }

		void clear() { metrics = {}; }
	};

	struct metrics {
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
	};

	RedwoodMetrics() {
		// All histograms have reset their buckets to 0 in the constructor.
		kvSizeWritten = Reference<Histogram>(
		    new Histogram(Reference<HistogramRegistry>(), "kvSize", "Written", Histogram::Unit::bytes));
		kvSizeReadByGet = Reference<Histogram>(
		    new Histogram(Reference<HistogramRegistry>(), "kvSize", "ReadByGet", Histogram::Unit::bytes));
		kvSizeReadByGetRange = Reference<Histogram>(
		    new Histogram(Reference<HistogramRegistry>(), "kvSize", "ReadByGetRange", Histogram::Unit::bytes));

		// These histograms are used for Btree events, hence level > 0
		unsigned int levelCounter = 0;
		for (RedwoodMetrics::Level& level : levels) {
			if (levelCounter > 0) {
				std::string levelString = "L" + std::to_string(levelCounter);
				level.buildFillPctSketch = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "buildFillPct", levelString, Histogram::Unit::percentageLinear));
				level.modifyFillPctSketch = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "modifyFillPct", levelString, Histogram::Unit::percentageLinear));
				level.buildStoredPctSketch = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "buildStoredPct", levelString, Histogram::Unit::percentageLinear));
				level.modifyStoredPctSketch = Reference<Histogram>(new Histogram(
				    Reference<HistogramRegistry>(), "modifyStoredPct", levelString, Histogram::Unit::percentageLinear));
				level.buildItemCountSketch = Reference<Histogram>(new Histogram(Reference<HistogramRegistry>(),
				                                                                "buildItemCount",
				                                                                levelString,
				                                                                Histogram::Unit::countLinear,
				                                                                0,
				                                                                maxRecordCount));
				level.modifyItemCountSketch = Reference<Histogram>(new Histogram(Reference<HistogramRegistry>(),
				                                                                 "modifyItemCount",
				                                                                 levelString,
				                                                                 Histogram::Unit::countLinear,
				                                                                 0,
				                                                                 maxRecordCount));
			}
			++levelCounter;
		}
		clear();
	}

	void clear() {
		for (RedwoodMetrics::Level& level : levels) {
			level.clear();
		}
		metric = {};
		startTime = g_network ? now() : 0;
	}

	// btree levels and one extra level for non btree level.
	Level levels[btreeLevels + 1];
	metrics metric;

	Reference<Histogram> kvSizeWritten;
	Reference<Histogram> kvSizeReadByGet;
	Reference<Histogram> kvSizeReadByGetRange;
	double startTime;

	// Return number of pages read or written, from cache or disk
	unsigned int pageOps() const {
		// All page reads are either a cache hit, probe hit, or a disk read
		return metric.pagerDiskWrite + metric.pagerDiskRead + metric.pagerCacheHit + metric.pagerProbeHit;
	}

	Level& level(unsigned int level) {
		// Valid levels are from 0 - btreeLevels
		// Level 0 is for operations that are not BTree level specific, as many of the metrics are the same
		// Level 0 - btreeLevels correspond to BTree node height, however heights above btreeLevels are combined
		//           into the level at btreeLevels
		return levels[std::min(level, btreeLevels)];
	}

	void updateMaxRecordCount(int maxRecords) {
		if (maxRecordCount != maxRecords) {
			maxRecordCount = maxRecords;
			for (int i = 1; i <= btreeLevels; ++i) {
				auto& level = levels[i];
				level.buildItemCountSketch->updateUpperBound(maxRecordCount);
				level.modifyItemCountSketch->updateUpperBound(maxRecordCount);
			}
		}
	}

	void logHistograms(double elapsed) {
		// All histograms have reset their buckets to 0 after writeToLog.
		kvSizeWritten->writeToLog(elapsed);
		kvSizeReadByGet->writeToLog(elapsed);
		kvSizeReadByGetRange->writeToLog(elapsed);
		unsigned int levelCounter = 0;
		for (RedwoodMetrics::Level& level : levels) {
			if (levelCounter > 0) {
				level.buildFillPctSketch->writeToLog(elapsed);
				level.modifyFillPctSketch->writeToLog(elapsed);
				level.buildStoredPctSketch->writeToLog(elapsed);
				level.modifyStoredPctSketch->writeToLog(elapsed);
				level.buildItemCountSketch->writeToLog(elapsed);
				level.modifyItemCountSketch->writeToLog(elapsed);
			}
			++levelCounter;
		}
	}

	// This will populate a trace event and/or a string with Redwood metrics.
	// The string is a reasonably well formatted page of information
	void getFields(TraceEvent* e, std::string* s = nullptr, bool skipZeroes = false);

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
int RedwoodMetrics::maxRecordCount = 315;
RedwoodMetrics g_redwoodMetrics = {};
Future<Void> g_redwoodMetricsActor;

ACTOR Future<Void> redwoodHistogramsLogger(double interval) {
	state double currTime;
	loop {
		currTime = now();
		wait(delay(interval));
		double elapsed = now() - currTime;
		g_redwoodMetrics.logHistograms(elapsed);
	}
}

ACTOR Future<Void> redwoodMetricsLogger() {
	g_redwoodMetrics.clear();
	state Future<Void> loggingFuture = redwoodHistogramsLogger(SERVER_KNOBS->REDWOOD_HISTOGRAM_INTERVAL);
	loop {
		wait(delay(SERVER_KNOBS->REDWOOD_METRICS_INTERVAL));

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
	struct Entry;
	typedef std::unordered_map<IndexType, Entry> CacheT;

	struct Entry : public boost::intrusive::list_base_hook<> {
		Entry() : hits(0), size(0) {}
		IndexType index;
		ObjectType item;
		int hits;
		int size;
		bool ownedByEvictor;
		CacheT* pCache;
	};

	typedef boost::intrusive::list<Entry> EvictionOrderT;

public:
	// Object evictor, manages the eviction order for one or more ObjectCaches
	// Not all objects tracked by the Evictor are in its evictionOrder, as ObjectCaches
	// using this Evictor can temporarily remove entries to an external order but they
	// must eventually give them back with moveIn() or remove them with reclaim().
	class Evictor : NonCopyable {
	public:
		Evictor(int64_t sizeLimit = 0) : sizeLimit(sizeLimit) {}

		// Evictors are normally singletons, either one per real process or one per virtual process in simulation
		static Evictor* getEvictor() {
			static Evictor nonSimEvictor;
			static std::map<NetworkAddress, Evictor> simEvictors;

			if (g_network->isSimulated()) {
				return &simEvictors[g_network->getLocalAddress()];
			} else {
				return &nonSimEvictor;
			}
		}

		// Move an entry to a different eviction order, stored outside of the Evictor,
		// but the entry size is still counted against the evictor
		void moveOut(Entry& e, EvictionOrderT& dest) {
			ASSERT(e.ownedByEvictor);
			dest.splice(dest.end(), evictionOrder, EvictionOrderT::s_iterator_to(e));
			e.ownedByEvictor = false;
			++movedOutCount;
		}

		// Move an entry to the back of the eviction order if it is in the eviction order
		void moveToBack(Entry& e) {
			ASSERT(e.ownedByEvictor);
			evictionOrder.splice(evictionOrder.end(), evictionOrder, EvictionOrderT::s_iterator_to(e));
		}

		// Move entire contents of an external eviction order containing entries whose size is part of
		// this Evictor to the front of its eviction order.
		void moveIn(EvictionOrderT& otherOrder) {
			for (auto& e : otherOrder) {
				ASSERT(!e.ownedByEvictor);
				e.ownedByEvictor = true;
				--movedOutCount;
			}
			evictionOrder.splice(evictionOrder.begin(), otherOrder);
		}

		// Add a new item to the back of the eviction order
		void addNew(Entry& e) {
			sizeUsed += e.size;
			evictionOrder.push_back(e);
			e.ownedByEvictor = true;
		}

		// Claim ownership of an entry, removing its size from the current size and removing it
		// from the eviction order if it exists there
		void reclaim(Entry& e) {
			sizeUsed -= e.size;
			// If e is in evictionOrder then remove it
			if (e.ownedByEvictor) {
				evictionOrder.erase(EvictionOrderT::s_iterator_to(e));
				e.ownedByEvictor = false;
			} else {
				// Otherwise, it wasn't so it had to be a movedOut item so decrement the count
				--movedOutCount;
			}
		}

		void trim(int additionalSpaceNeeded = 0) {
			int attemptsLeft = FLOW_KNOBS->MAX_EVICT_ATTEMPTS;
			// While the cache is too big, evict the oldest entry until the oldest entry can't be evicted.
			while (attemptsLeft-- > 0 && sizeUsed > (sizeLimit - reservedSize - additionalSpaceNeeded) &&
			       !evictionOrder.empty()) {
				Entry& toEvict = evictionOrder.front();

				debug_printf("Evictor count=%" PRId64 " sizeUsed=%" PRId64 " sizeLimit=%" PRId64 " sizePenalty=%" PRId64
				             " needed=%d  Trying to evict %s evictable %d\n",
				             evictionOrder.size(),
				             sizeUsed,
				             sizeLimit,
				             reservedSize,
				             additionalSpaceNeeded,
				             ::toString(toEvict.index).c_str(),
				             toEvict.item.evictable());

				if (!toEvict.item.evictable()) {
					// shift the front to the back
					evictionOrder.shift_forward(1);
					++g_redwoodMetrics.metric.pagerEvictFail;
					break;
				} else {
					if (toEvict.hits == 0) {
						++g_redwoodMetrics.metric.pagerEvictUnhit;
					}
					sizeUsed -= toEvict.size;
					debug_printf("Evicting %s\n", ::toString(toEvict.index).c_str());
					evictionOrder.pop_front();
					toEvict.pCache->erase(toEvict.index);
				}
			}
		}

		int64_t getCountUsed() const { return evictionOrder.size() + movedOutCount; }
		int64_t getCountMoved() const { return movedOutCount; }
		int64_t getSizeUsed() const { return sizeUsed + reservedSize; }

		// Only to be used in tests at a point where all ObjectCache instances should be destroyed.
		bool empty() const { return reservedSize == 0 && sizeUsed == 0 && getCountUsed() == 0; }

		std::string toString() const {
			std::string s = format("Evictor {sizeLimit=%" PRId64 " sizeUsed=%" PRId64 " countUsed=%" PRId64
			                       " sizePenalty=%" PRId64 " movedOutCount=%" PRId64,
			                       sizeLimit,
			                       sizeUsed,
			                       getCountUsed(),
			                       reservedSize,
			                       movedOutCount);
			for (auto& entry : evictionOrder) {
				s += format("\n\tindex %s  size %d  evictable %d\n",
				            ::toString(entry.index).c_str(),
				            entry.size,
				            entry.item.evictable());
			}
			s += "}\n";
			return s;
		}

		// Any external data strutures whose memory usage should be counted as part of the object cache
		// budget should add their usage to this total and keep it updated.
		int64_t reservedSize = 0;
		int64_t sizeLimit;

	private:
		EvictionOrderT evictionOrder;
		// Size of all entries in the eviction order or held in external eviction orders
		int64_t sizeUsed = 0;
		// Number of items that have been moveOut()'d to other evictionOrders and aren't back yet
		int64_t movedOutCount = 0;
	};

	ObjectCache(Evictor* evictor = nullptr) : pEvictor(evictor) {
		if (pEvictor == nullptr) {
			pEvictor = Evictor::getEvictor();
		}
	}

	Evictor& evictor() const { return *pEvictor; }

	int64_t getCount() const { return cache.size(); }

	void reserveCount(int count) { cache.reserve(count); }

	// Get the object for i if it exists, else return nullptr.
	// If the object exists, its eviction order will NOT change as this is not a cache hit.
	ObjectType* getIfExists(const IndexType& index) {
		auto i = cache.find(index);
		if (i != cache.end()) {
			++i->second.hits;
			return &i->second.item;
		}
		return nullptr;
	}

	// If index is in cache and not on the prioritized eviction order list, move it there.
	void prioritizeEviction(const IndexType& index) {
		auto i = cache.find(index);
		if (i != cache.end() && i->second.ownedByEvictor) {
			pEvictor->moveOut(i->second, prioritizedEvictions);
		}
	}

	// Get the object for i or create a new one.
	// After a get(), the object for i is the last in evictionOrder.
	// If noHit is set, do not consider this access to be cache hit if the object is present
	// If noMiss is set, do not consider this access to be a cache miss if the object is not present
	ObjectType& get(const IndexType& index, int size, bool noHit = false) {
		Entry& entry = cache[index];

		// If entry is linked into an evictionOrder
		if (entry.is_linked()) {
			// If this access is meant to be a hit
			if (!noHit) {
				++entry.hits;
				// If item eviction is not prioritized, move to end of eviction order
				if (entry.ownedByEvictor) {
					pEvictor->moveToBack(entry);
				}
			}
		} else {
			// Otherwise it was a cache miss

			// Finish initializing entry
			entry.index = index;
			entry.pCache = &cache;
			entry.hits = 0;
			entry.size = size;

			pEvictor->trim(entry.size);
			pEvictor->addNew(entry);
		}

		return entry.item;
	}

	// Clears the cache, saving the entries to second cache, then waits for each item to be evictable and evicts it.
	ACTOR static Future<Void> clear_impl(ObjectCache* self) {
		// Claim ownership of all of our cached items, removing them from the evictor's control and quota.
		for (auto& ie : self->cache) {
			self->pEvictor->reclaim(ie.second);
		}

		// All items are in the cache so we don't need the prioritized eviction order anymore, and the cache is about
		// to be destroyed so the prioritizedEvictions head/tail will become invalid.
		self->prioritizedEvictions.clear();

		state typename CacheT::iterator i = self->cache.begin();
		while (i != self->cache.end()) {
			if (!i->second.item.evictable()) {
				wait(i->second.item.onEvictable());
			}
			++i;
		}

		return Void();
	}

	Future<Void> clear() { return clear_impl(this); }

	// Move the prioritized evictions queued to the front of the eviction order
	void flushPrioritizedEvictions() { pEvictor->moveIn(prioritizedEvictions); }

private:
	Evictor* pEvictor;
	CacheT cache;
	EvictionOrderT prioritizedEvictions;
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

constexpr int initialVersion = invalidVersion;

class DWALPagerSnapshot;

// An implementation of IPager2 that supports atomicUpdate() of a page without forcing a change to new page ID.
// It does this internally mapping the original page ID to alternate page IDs by write version.
// The page id remaps are kept in memory and also logged to a "remap queue" which must be reloaded on cold start.
// To prevent the set of remaps from growing unboundedly, once a remap is old enough to be at or before the
// oldest pager version being maintained the remap can be "undone" by popping it from the remap queue,
// copying the alternate page ID's data over top of the original page ID's data, and deleting the remap from memory.
// This process basically describes a "Delayed" Write-Ahead-Log (DWAL) because the remap queue and the newly allocated
// alternate pages it references basically serve as a write ahead log for pages that will eventually be copied
// back to their original location once the original version is no longer needed.
class DWALPager final : public IPager2 {
public:
	typedef FIFOQueue<LogicalPageID> LogicalPageQueueT;
	typedef std::map<Version, LogicalPageID> VersionToPageMapT;
	typedef std::unordered_map<LogicalPageID, VersionToPageMapT> PageToVersionedMapT;
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
	typedef ObjectCache<LogicalPageID, PageCacheEntry> PageCacheT;

	int64_t* getPageCachePenaltySource() override { return &pageCache.evictor().reservedSize; }

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

	struct ExtentUsedListEntry {
		QueueID queueID;
		LogicalPageID extentID;

		bool operator<(const ExtentUsedListEntry& rhs) const { return queueID < rhs.queueID; }

		std::string toString() const {
			return format("ExtentUsedListEntry{%s @%s}", ::toString(extentID).c_str(), ::toString(queueID).c_str());
		}
	};

#pragma pack(pop)

	typedef FIFOQueue<DelayedFreePage> DelayedFreePageQueueT;
	typedef FIFOQueue<RemappedPage> RemapQueueT;
	typedef FIFOQueue<ExtentUsedListEntry> ExtentUsedListQueueT;

	// If the file already exists, pageSize might be different than desiredPageSize
	// Use pageCacheSizeBytes == 0 to use default from flow knobs
	// If memoryOnly is true, the pager will exist only in memory and once the cache is full writes will fail.
	DWALPager(int desiredPageSize,
	          int desiredExtentSize,
	          std::string filename,
	          int64_t pageCacheSizeBytes,
	          int64_t remapCleanupWindowBytes,
	          int concurrentExtentReads,
	          bool memoryOnly = false,
	          Promise<Void> errorPromise = {})
	  : ioLock(FLOW_KNOBS->MAX_OUTSTANDING, ioMaxPriority, FLOW_KNOBS->MAX_OUTSTANDING / 2),
	    pageCacheBytes(pageCacheSizeBytes), pHeader(nullptr), desiredPageSize(desiredPageSize),
	    desiredExtentSize(desiredExtentSize), filename(filename), memoryOnly(memoryOnly), errorPromise(errorPromise),
	    remapCleanupWindowBytes(remapCleanupWindowBytes), concurrentExtentReads(new FlowLock(concurrentExtentReads)) {

		// This sets the page cache size for all PageCacheT instances using the same evictor
		pageCache.evictor().sizeLimit = pageCacheBytes;

		if (!g_redwoodMetricsActor.isValid()) {
			g_redwoodMetricsActor = redwoodMetricsLogger();
		}

		commitFuture = Void();
		recoverFuture = forwardError(recover(this), errorPromise);
	}

	void setPageSize(int size) {
		// Conservative maximum for number of records that can fit in this page size
		g_redwoodMetrics.updateMaxRecordCount(315.0 * size / 4096);

		logicalPageSize = size;
		// Physical page size is the total size of the smallest number of physical blocks needed to store
		// logicalPageSize bytes
		int blocks = 1 + ((logicalPageSize - 1) / smallestPhysicalBlock);
		physicalPageSize = blocks * smallestPhysicalBlock;
		if (pHeader != nullptr) {
			pHeader->pageSize = logicalPageSize;
		}
	}

	void setExtentSize(int size) {
		// if the specified extent size is smaller than the physical page size, round it off to one physical page size
		// physical extent size has to be a multiple of physical page size
		if (size <= physicalPageSize) {
			pagesPerExtent = 1;
		} else {
			pagesPerExtent = 1 + ((size - 1) / physicalPageSize);
		}
		physicalExtentSize = pagesPerExtent * physicalPageSize;

		if (pHeader != nullptr) {
			pHeader->extentSize = size;
		}
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

		self->fileExtension = Void();

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
				TraceEvent(SevWarn, "RedwoodRecoveringHeader").detail("Filename", self->filename);

				wait(store(self->headerPage, self->readHeaderPage(self, 1)));

				if (!self->headerPage->verifyChecksum(1)) {
					if (g_network->isSimulated()) {
						// TODO: Detect if process is being restarted and only throw injected if so?
						throw io_error().asInjectedFault();
					}

					Error e = checksum_failed();
					TraceEvent(SevError, "RedwoodRecoveryFailed").error(e).detail("Filename", self->filename);
					throw e;
				}
				recoveredHeader = true;
			}

			self->pHeader = (Header*)self->headerPage->begin();

			if (self->pHeader->formatVersion != Header::FORMAT_VERSION) {
				Error e = unsupported_format_version();
				TraceEvent(SevWarn, "RedwoodRecoveryFailedWrongVersion")
				    .error(e)
				    .detail("Filename", self->filename)
				    .detail("Version", self->pHeader->formatVersion)
				    .detail("ExpectedVersion", Header::FORMAT_VERSION);
				throw e;
			}

			self->setPageSize(self->pHeader->pageSize);
			self->filePageCount = fileSize / self->physicalPageSize;
			self->filePageCountPending = self->filePageCount;

			if (self->logicalPageSize != self->desiredPageSize) {
				TraceEvent(SevWarn, "RedwoodPageSizeNotDesired")
				    .detail("Filename", self->filename)
				    .detail("ExistingPageSize", self->logicalPageSize)
				    .detail("DesiredPageSize", self->desiredPageSize);
			}

			self->setExtentSize(self->pHeader->extentSize);

			self->freeList.recover(self, self->pHeader->freeList, "FreeListRecovered");
			self->extentFreeList.recover(self, self->pHeader->extentFreeList, "ExtentFreeListRecovered");
			self->delayedFreeList.recover(self, self->pHeader->delayedFreeList, "DelayedFreeListRecovered");
			self->extentUsedList.recover(self, self->pHeader->extentUsedList, "ExtentUsedListRecovered");
			self->remapQueue.recover(self, self->pHeader->remapQueue, "RemapQueueRecovered");

			debug_printf("DWALPager(%s) Queue recovery complete.\n", self->filename.c_str());

			// remapQueue entries are recovered using a fast path reading extents at a time
			// we first issue disk reads for remapQueue extents obtained from extentUsedList
			Standalone<VectorRef<ExtentUsedListEntry>> extents = wait(self->extentUsedList.peekAll());
			debug_printf("DWALPager(%s) ExtentUsedList size: %d.\n", self->filename.c_str(), extents.size());
			if (extents.size() > 1) {
				QueueID remapQueueID = self->remapQueue.queueID;
				for (int i = 1; i < extents.size() - 1; i++) {
					if (extents[i].queueID == remapQueueID) {
						LogicalPageID extID = extents[i].extentID;
						debug_printf("DWALPager Extents: ID: %s ", toString(extID).c_str());
						self->readExtent(extID);
					}
				}
			}

			// And here we consume results of the disk reads and populate the remappedPages map
			// Using a promiseStream for the peeked results ensures that we use the CPU to populate the map
			// and the disk concurrently
			state PromiseStream<Standalone<VectorRef<RemappedPage>>> remapStream;
			state Future<Void> remapRecoverActor;
			remapRecoverActor = self->remapQueue.peekAllExt(remapStream);
			try {
				loop choose {
					when(Standalone<VectorRef<RemappedPage>> remaps = waitNext(remapStream.getFuture())) {
						debug_printf("DWALPager(%s) recovery. remaps size: %d, queueEntries: %d\n",
						             self->filename.c_str(),
						             remaps.size(),
						             self->remapQueue.numEntries);
						for (auto& r : remaps) {
							self->remappedPages[r.originalPageID][r.version] = r.newPageID;
						}
					}
					when(wait(remapRecoverActor)) { remapRecoverActor = Never(); }
				}
			} catch (Error& e) {
				if (e.code() != error_code_end_of_stream) {
					throw;
				}
			}

			debug_printf("DWALPager(%s) recovery complete. RemappedPagesMap: %s\n",
			             self->filename.c_str(),
			             toString(self->remappedPages).c_str());

			debug_printf("DWALPager(%s) recovery complete. destroy extent cache\n", self->filename.c_str());
			wait(self->extentCache.clear());

			// If the header was recovered from the backup at Page 1 then write and sync it to Page 0 before continuing.
			// If this fails, the backup header is still in tact for the next recovery attempt.
			if (recoveredHeader) {
				// Write the header to page 0
				wait(self->writeHeaderPage(0, self->headerPage));

				// Wait for all outstanding writes to complete
				wait(waitForAll(self->operations));
				self->operations.clear();
				// Sync header
				wait(self->pageFile->sync());
				debug_printf("DWALPager(%s) Header recovery complete.\n", self->filename.c_str());
			}

			// Update the last committed header with the one that was recovered (which is the last known committed
			// header)
			self->updateCommittedHeader();
			self->addSnapshot(self->pHeader->committedVersion, self->pHeader->getMetaKey());

			// Reset the remapQueue head reader for normal reads
			self->remapQueue.resetHeadReader();

			self->remapCleanupFuture = remapCleanup(self);
		} else {
			// Note: If the file contains less than 2 pages but more than 0 bytes then the pager was never successfully
			// committed. A new pager will be created in its place.
			// TODO:  Is the right behavior?
			exists = false;

			debug_printf("DWALPager(%s) creating new pager\n", self->filename.c_str());

			self->headerPage = self->newPageBuffer();
			self->pHeader = (Header*)self->headerPage->begin();

			// Now that the header page has been allocated, set page size to desired
			self->setPageSize(self->desiredPageSize);
			self->filePageCount = 0;
			self->filePageCountPending = 0;

			// Now set the extent size, do this always after setting the page size as
			// extent size is a multiple of page size
			self->setExtentSize(self->desiredExtentSize);

			// Write new header using desiredPageSize
			self->pHeader->formatVersion = Header::FORMAT_VERSION;
			self->pHeader->committedVersion = initialVersion;
			self->pHeader->oldestVersion = initialVersion;
			// No meta key until a user sets one and commits
			self->pHeader->setMetaKey(Key());

			// There are 2 reserved pages:
			//   Page 0 - header
			//   Page 1 - header backup
			self->pHeader->pageCount = 2;

			// Create queues
			self->pHeader->queueCount = 0;
			self->freeList.create(self, self->newLastPageID(), "FreeList", self->newLastQueueID(), false);
			self->delayedFreeList.create(self, self->newLastPageID(), "DelayedFreeList", self->newLastQueueID(), false);
			self->extentFreeList.create(self, self->newLastPageID(), "ExtentFreeList", self->newLastQueueID(), false);
			self->extentUsedList.create(self, self->newLastPageID(), "ExtentUsedList", self->newLastQueueID(), false);
			LogicalPageID extID = self->newLastExtentID();
			self->remapQueue.create(self, extID, "RemapQueue", self->newLastQueueID(), true);
			self->extentUsedList.pushBack({ self->remapQueue.queueID, extID });

			// The first commit() below will flush the queues and update the queue states in the header,
			// but since the queues will not be used between now and then their states will not change.
			// In order to populate lastCommittedHeader, update the header now with the queue states.
			self->pHeader->freeList = self->freeList.getState();
			self->pHeader->delayedFreeList = self->delayedFreeList.getState();
			self->pHeader->extentFreeList = self->extentFreeList.getState();
			self->pHeader->extentUsedList = self->extentUsedList.getState();
			self->pHeader->remapQueue = self->remapQueue.getState();

			// Set remaining header bytes to \xff
			memset(self->headerPage->mutate() + self->pHeader->size(),
			       0xff,
			       self->headerPage->size() - self->pHeader->size());

			// There is no previously committed header, but the current header state is sufficient to use as the backup
			// header for the next commit, which if recovered would result in a valid empty pager at version 0.
			self->updateCommittedHeader();
			self->addSnapshot(initialVersion, KeyRef());

			self->remapCleanupFuture = Void();
		}

		if (!self->memoryOnly) {
			wait(store(fileSize, self->pageFile->size()));
		}

		TraceEvent e(SevInfo, "RedwoodRecoveredPager");
		e.detail("OpenedExisting", exists);
		self->toTraceEvent(e);
		e.log();

		self->recoveryVersion = self->pHeader->committedVersion;
		debug_printf("DWALPager(%s) recovered.  recoveryVersion=%" PRId64 " oldestVersion=%" PRId64
		             " logicalPageSize=%d physicalPageSize=%d\n",
		             self->filename.c_str(),
		             self->recoveryVersion,
		             self->pHeader->oldestVersion,
		             self->logicalPageSize,
		             self->physicalPageSize,
		             self->pHeader->pageCount,
		             self->filePageCount);

		return Void();
	}

	void toTraceEvent(TraceEvent& e) const override {
		e.detail("FileName", filename.c_str());
		e.detail("LogicalFileSize", pHeader->pageCount * physicalPageSize);
		e.detail("PhysicalFileSize", filePageCountPending * physicalPageSize);
		e.detail("CommittedVersion", pHeader->committedVersion);
		e.detail("LogicalPageSize", logicalPageSize);
		e.detail("PhysicalPageSize", physicalPageSize);

		remapQueue.toTraceEvent(e, "RemapQueue");
		delayedFreeList.toTraceEvent(e, "FreeQueue");
		freeList.toTraceEvent(e, "DelayedFreeQueue");
		extentUsedList.toTraceEvent(e, "UsedExtentQueue");
		extentFreeList.toTraceEvent(e, "FreeExtentQueue");
		getStorageBytes().toTraceEvent(e);
	}

	ACTOR static void extentCacheClear_impl(DWALPager* self) { wait(self->extentCache.clear()); }

	void extentCacheClear() override { extentCacheClear_impl(this); }

	// get a list of used extents for a given extent based queue (for testing purpose)
	ACTOR static Future<Standalone<VectorRef<LogicalPageID>>> getUsedExtents_impl(DWALPager* self, QueueID queueID) {
		state Standalone<VectorRef<LogicalPageID>> extentIDs;
		extentIDs.reserve(extentIDs.arena(),
		                  self->extentUsedList.numEntries); // TODO this is overreserving. is that a problem?

		Standalone<VectorRef<ExtentUsedListEntry>> extents = wait(self->extentUsedList.peekAll());
		debug_printf("DWALPager(%s) ExtentUsedList size: %d.\n", self->filename.c_str(), extents.size());
		if (extents.size() > 1) {
			for (int i = 1; i < extents.size() - 1; i++) {
				if (extents[i].queueID == queueID) {
					LogicalPageID extID = extents[i].extentID;
					debug_printf("DWALPager Extents: ID: %s ", toString(extID).c_str());
					extentIDs.push_back(extentIDs.arena(), extID);
				}
			}
		}
		return extentIDs;
	}

	Future<Standalone<VectorRef<LogicalPageID>>> getUsedExtents(QueueID queueID) override {
		return getUsedExtents_impl(this, queueID);
	}

	void pushExtentUsedList(QueueID queueID, LogicalPageID extID) override {
		extentUsedList.pushBack({ queueID, extID });
	}

	// Allocate a new queueID
	QueueID newLastQueueID() override {
		QueueID id = pHeader->queueCount;
		++pHeader->queueCount;
		return id;
	}

	Reference<ArenaPage> newPageBuffer(size_t size = 1) override {
		return Reference<ArenaPage>(new ArenaPage(logicalPageSize * size, physicalPageSize * size));
	}

	// Returns the usable size of pages returned by the pager (i.e. the size of the page that isn't pager overhead).
	// For a given pager instance, separate calls to this function must return the same value.
	// TODO: This is abstraction breaking.  This should probably be stored as a member, calculated once on construction
	// by creating an ArenaPage and getting its usable size.
	int getUsablePageSize() const override { return logicalPageSize - sizeof(ArenaPage::Checksum); }
	int getPhysicalPageSize() const override { return physicalPageSize; }
	int getLogicalPageSize() const override { return logicalPageSize; }
	int getPagesPerExtent() const override { return pagesPerExtent; }

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

	// Grow the pager file by one page and return it
	LogicalPageID newLastPageID() {
		LogicalPageID id = pHeader->pageCount;
		growPager(1);
		return id;
	}

	Future<LogicalPageID> newPageID() override { return newPageID_impl(this); }

	void growPager(int64_t pages) { pHeader->pageCount += pages; }

	// Get a new, previously available extent and it's first page ID.  The page will be considered in-use after the next
	// commit regardless of whether or not it was written to, until it is returned to the pager via freePage()
	ACTOR static Future<LogicalPageID> newExtentPageID_impl(DWALPager* self, QueueID queueID) {
		// First try the free list
		Optional<LogicalPageID> freeExtentID = wait(self->extentFreeList.pop());
		if (freeExtentID.present()) {
			debug_printf("DWALPager(%s) remapQueue newExtentPageID() returning %s from free list\n",
			             self->filename.c_str(),
			             toString(freeExtentID.get()).c_str());
			self->extentUsedList.pushBack({ queueID, freeExtentID.get() });
			self->extentUsedList.getState();
			return freeExtentID.get();
		}

		// Lastly, add a new extent to the pager
		LogicalPageID id = self->newLastExtentID();
		debug_printf("DWALPager(%s) remapQueue newExtentPageID() returning %s at end of file\n",
		             self->filename.c_str(),
		             toString(id).c_str());
		self->extentUsedList.pushBack({ queueID, id });
		self->extentUsedList.getState();
		return id;
	}

	// Grow the pager file by one extent and return it
	// We reserve all the pageIDs within the extent during this step
	// That translates to extentID being same as the return first pageID
	LogicalPageID newLastExtentID() {
		LogicalPageID id = pHeader->pageCount;
		growPager(pagesPerExtent);
		return id;
	}

	Future<LogicalPageID> newExtentPageID(QueueID queueID) override { return newExtentPageID_impl(this, queueID); }

	ACTOR static Future<Void> writePhysicalPage_impl(DWALPager* self,
	                                                 Void* data,
	                                                 PagerEventReasons reason,
	                                                 unsigned int level,
	                                                 PhysicalPageID pageID,
	                                                 int blockSize,
	                                                 bool header) {

		state PriorityMultiLock::Lock lock = wait(self->ioLock.lock(header ? ioMaxPriority : ioMinPriority));
		++g_redwoodMetrics.metric.pagerDiskWrite;
		g_redwoodMetrics.level(level).metrics.events.addEventReason(PagerEvents::PageWrite, reason);
		if (self->memoryOnly) {
			return Void();
		}

		// If a truncation up to include pageID has not yet been completed
		if (pageID >= self->filePageCount) {
			// And no extension pending will include pageID
			if (pageID >= self->filePageCountPending) {
				// Update extension to a new one that waits on the old one and extends further
				self->fileExtension = extendToCover(self, pageID, self->fileExtension);
			}

			// Wait for extension that covers pageID to complete;
			wait(self->fileExtension);
		}

		// Note:  Not using forwardError here so a write error won't be discovered until commit time.
		wait(self->pageFile->write(data, blockSize, (int64_t)pageID * blockSize));
		return Void();
	}

	ACTOR static Future<Void> extendToCover(DWALPager* self, uint64_t pageID, Future<Void> previousExtension) {
		// Calculate new page count, round up to nearest multiple of growth size > pageID
		state int64_t newPageCount = pageID + SERVER_KNOBS->REDWOOD_PAGEFILE_GROWTH_SIZE_PAGES -
		                             (pageID % SERVER_KNOBS->REDWOOD_PAGEFILE_GROWTH_SIZE_PAGES);

		// Indicate that extension to this new count has been started
		self->filePageCountPending = newPageCount;

		// Wait for any previous extensions to complete
		wait(previousExtension);

		// Grow the file
		wait(self->pageFile->truncate(newPageCount * self->physicalPageSize));

		// Indicate that extension to the new count has been completed
		self->filePageCount = newPageCount;

		return Void();
	}

	Future<Void> writePhysicalPage(PagerEventReasons reason,
	                               unsigned int level,
	                               Standalone<VectorRef<PhysicalPageID>> pageIDs,
	                               Reference<ArenaPage> page,
	                               bool header = false) {
		debug_printf("DWALPager(%s) op=%s %s ptr=%p\n",
		             filename.c_str(),
		             (header ? "writePhysicalHeader" : "writePhysical"),
		             toString(pageIDs).c_str(),
		             page->begin());
		VALGRIND_MAKE_MEM_DEFINED(page->begin(), page->size());
		page->updateChecksum(pageIDs.front());
		debug_printf("DWALPager(%s) writePhysicalPage %s CalculatedChecksum=%d ChecksumInPage=%d\n",
		             filename.c_str(),
		             toString(pageIDs).c_str(),
		             page->calculateChecksum(pageIDs.front()),
		             page->getChecksum());
		int blockSize = header ? smallestPhysicalBlock : physicalPageSize;
		Future<Void> f;
		if (pageIDs.size() == 1) {
			f = writePhysicalPage_impl(this, (Void*)page->mutate(), reason, level, pageIDs.front(), blockSize, header);
			operations.push_back(f);
			return f;
		}
		std::vector<Future<Void>> writers;
		for (int i = 0; i < pageIDs.size(); ++i) {
			Future<Void> p = writePhysicalPage_impl(
			    this, (Void*)page->mutate() + i * blockSize, reason, level, pageIDs[i], blockSize, header);
			writers.push_back(p);
		}
		f = waitForAll(writers);
		operations.push_back(f);
		return f;
	}

	Future<Void> writeHeaderPage(PhysicalPageID pageID, Reference<ArenaPage> page) {
		return writePhysicalPage(
		    PagerEventReasons::MetaData, nonBtreeLevel, VectorRef<PhysicalPageID>(&pageID, 1), page, true);
	}

	void updatePage(PagerEventReasons reason,
	                unsigned int level,
	                Standalone<VectorRef<LogicalPageID>> pageIDs,
	                Reference<ArenaPage> data) override {
		// Get the cache entry for this page, without counting it as a cache hit as we're replacing its contents now
		// or as a cache miss because there is no benefit to the page already being in cache
		// Similarly, this does not count as a point lookup for reason.
		ASSERT(pageIDs.front() != invalidLogicalPageID);
		PageCacheEntry& cacheEntry = pageCache.get(pageIDs.front(), pageIDs.size() * physicalPageSize, true);
		debug_printf("DWALPager(%s) op=write %s cached=%d reading=%d writing=%d\n",
		             filename.c_str(),
		             toString(pageIDs).c_str(),
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
			cacheEntry.writeFuture = detach(writePhysicalPage(reason, level, pageIDs, data));
		} else if (cacheEntry.reading()) {
			// Wait for the read to finish, then start the write.
			cacheEntry.writeFuture = map(success(cacheEntry.readFuture), [=](Void) {
				writePhysicalPage(reason, level, pageIDs, data);
				return Void();
			});
		}
		// If the page is being written, wait for this write before issuing the new write to ensure the
		// writes happen in the correct order
		else if (cacheEntry.writing()) {
			cacheEntry.writeFuture = map(cacheEntry.writeFuture, [=](Void) {
				writePhysicalPage(reason, level, pageIDs, data);
				return Void();
			});
		} else {
			cacheEntry.writeFuture = detach(writePhysicalPage(reason, level, pageIDs, data));
		}

		// Always update the page contents immediately regardless of what happened above.
		cacheEntry.readFuture = data;
	}

	Future<LogicalPageID> atomicUpdatePage(PagerEventReasons reason,
	                                       unsigned int level,
	                                       LogicalPageID pageID,
	                                       Reference<ArenaPage> data,
	                                       Version v) override {
		debug_printf("DWALPager(%s) op=writeAtomic %s @%" PRId64 "\n", filename.c_str(), toString(pageID).c_str(), v);
		Future<LogicalPageID> f = map(newPageID(), [=](LogicalPageID newPageID) {
			updatePage(reason, level, VectorRef<LogicalPageID>(&newPageID, 1), data);
			// TODO:  Possibly limit size of remap queue since it must be recovered on cold start
			RemappedPage r{ v, pageID, newPageID };
			remapQueue.pushBack(r);
			auto& versionedMap = remappedPages[pageID];

			if (SERVER_KNOBS->REDWOOD_EVICT_UPDATED_PAGES) {
				// An update page is unlikely to have its old version read again soon, so prioritize its cache eviction
				// If the versioned map is empty for this page then the prior version of the page is at stored at the
				// PhysicalPageID pageID, otherwise it is the last mapped value in the version-ordered map.
				pageCache.prioritizeEviction(versionedMap.empty() ? pageID : versionedMap.rbegin()->second);
			}
			versionedMap[v] = newPageID;

			debug_printf("DWALPager(%s) pushed %s\n", filename.c_str(), RemappedPage(r).toString().c_str());
			return pageID;
		});

		// No need for forwardError here because newPageID() is already wrapped in forwardError
		return f;
	}

	void freeUnmappedPage(PhysicalPageID pageID, Version v) {
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

		// A freed page is unlikely to be read again soon so prioritize its cache eviction
		if (SERVER_KNOBS->REDWOOD_EVICT_UPDATED_PAGES) {
			pageCache.prioritizeEviction(pageID);
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

			// A freed page is unlikely to be read again soon so prioritize its cache eviction
			if (SERVER_KNOBS->REDWOOD_EVICT_UPDATED_PAGES) {
				PhysicalPageID previousPhysicalPage = i->second.rbegin()->second;
				pageCache.prioritizeEviction(previousPhysicalPage);
			}

			i->second[v] = invalidLogicalPageID;
			return;
		}

		freeUnmappedPage(pageID, v);
	};

	ACTOR static void freeExtent_impl(DWALPager* self, LogicalPageID pageID) {
		self->extentFreeList.pushBack(pageID);
		Optional<ExtentUsedListEntry> freeExtent = wait(self->extentUsedList.pop());
		// Optional<LogicalPageID> freeExtentPageID = wait(self->extentUsedList.pop());
		if (freeExtent.present()) {
			debug_printf("DWALPager(%s) freeExtentPageID() popped %s from used list\n",
			             self->filename.c_str(),
			             toString(freeExtent.get().extentID).c_str());
		}
	}
	void freeExtent(LogicalPageID pageID) override { freeExtent_impl(this, pageID); }

	ACTOR static Future<int> readPhysicalPage_impl(DWALPager* self,
	                                               uint8_t* data,
	                                               int blockSize,
	                                               int64_t offset,
	                                               int priority) {
		state PriorityMultiLock::Lock lock = wait(self->ioLock.lock(std::min(priority, ioMaxPriority)));
		int reader = wait(self->pageFile->read(data, blockSize, offset));
		return reader;
	}

	// Read a physical page from the page file.  Note that header pages use a page size of smallestPhysicalBlock
	// If the user chosen physical page size is larger, then there will be a gap of unused space after the header pages
	// and before the user-chosen sized pages.
	ACTOR static Future<Reference<ArenaPage>> readPhysicalPage(DWALPager* self,
	                                                           PhysicalPageID pageID,
	                                                           int priority,
	                                                           bool header) {
		ASSERT(!self->memoryOnly);

		// if (g_network->getCurrentTask() > TaskPriority::DiskRead) {
		// 	wait(delay(0, TaskPriority::DiskRead));
		// }

		state Reference<ArenaPage> page =
		    header ? Reference<ArenaPage>(new ArenaPage(smallestPhysicalBlock, smallestPhysicalBlock))
		           : self->newPageBuffer();
		debug_printf("DWALPager(%s) op=readPhysicalStart %s ptr=%p\n",
		             self->filename.c_str(),
		             toString(pageID).c_str(),
		             page->begin());

		state PriorityMultiLock::Lock lock = wait(self->ioLock.lock(std::min(priority, ioMaxPriority)));
		++g_redwoodMetrics.metric.pagerDiskRead;

		// TODO:  Could a dispatched read try to write to page after it has been destroyed if this actor is cancelled?
		int blockSize = header ? smallestPhysicalBlock : self->physicalPageSize;
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
				if (g_network->isSimulated() && g_simulator.checkInjectedCorruption())
					e = e.asInjectedFault();
				TraceEvent(SevError, "RedwoodChecksumFailed")
				    .error(e)
				    .detail("Filename", self->filename.c_str())
				    .detail("PageID", pageID)
				    .detail("PageSize", self->physicalPageSize)
				    .detail("Offset", pageID * self->physicalPageSize)
				    .detail("CalculatedChecksum", page->calculateChecksum(pageID))
				    .detail("ChecksumInPage", page->getChecksum());
				ASSERT(false);
				throw e;
			}
		}
		return page;
	}

	ACTOR static Future<Reference<ArenaPage>> readPhysicalMultiPage(DWALPager* self,
	                                                                Standalone<VectorRef<PhysicalPageID>> pageIDs,
	                                                                int priority) {
		ASSERT(!self->memoryOnly);

		// if (g_network->getCurrentTask() > TaskPriority::DiskRead) {
		// 	wait(delay(0, TaskPriority::DiskRead));
		// }

		state Reference<ArenaPage> page = self->newPageBuffer(pageIDs.size());
		debug_printf("DWALPager(%s) op=readPhysicalStart %s ptr=%p\n",
		             self->filename.c_str(),
		             toString(pageIDs).c_str(),
		             page->begin());

		++g_redwoodMetrics.metric.pagerDiskRead;
		// TODO:  Could a dispatched read try to write to page after it has been destroyed if this actor is cancelled?
		state int blockSize = self->physicalPageSize;
		state uint8_t* data = page->mutate();
		std::vector<Future<int>> reads;
		for (int i = 0; i < pageIDs.size(); ++i) {
			reads.push_back(readPhysicalPage_impl(self, data, blockSize, ((int64_t)pageIDs[i]) * blockSize, priority));
			data += blockSize;
		}
		// wait for all the parallel read futures
		wait(waitForAll(reads));

		debug_printf("DWALPager(%s) op=readPhysicalComplete %s ptr=%p bytes=%d\n",
		             self->filename.c_str(),
		             toString(pageIDs).c_str(),
		             page->begin(),
		             pageIDs.size() * blockSize);
		// Header reads are checked explicitly during recovery
		if (!page->verifyChecksum(pageIDs.front())) {
			debug_printf("DWALPager(%s) checksum failed for %s\n", self->filename.c_str(), toString(pageIDs).c_str());
			Error e = checksum_failed();
			TraceEvent(SevError, "RedwoodChecksumFailed")
			    .error(e)
			    .detail("Filename", self->filename.c_str())
			    .detail("PageID", pageIDs)
			    .detail("PageSize", self->physicalPageSize)
			    .detail("Offset", pageIDs.front() * self->physicalPageSize)
			    .detail("CalculatedChecksum", page->calculateChecksum(pageIDs.front()))
			    .detail("ChecksumInPage", page->getChecksum());
			ASSERT(false);
			throw e;
		}
		return page;
	}

	static Future<Reference<ArenaPage>> readHeaderPage(DWALPager* self, PhysicalPageID pageID) {
		debug_printf("DWALPager(%s) readHeaderPage %s\n", self->filename.c_str(), toString(pageID).c_str());
		return readPhysicalPage(self, pageID, ioMaxPriority, true);
	}

	// Reads the most recent version of pageID, either previously committed or written using updatePage()
	// in the current commit
	Future<Reference<ArenaPage>> readPage(PagerEventReasons reason,
	                                      unsigned int level,
	                                      PhysicalPageID pageID,
	                                      int priority,
	                                      bool cacheable,
	                                      bool noHit) override {
		// Use cached page if present, without triggering a cache hit.
		// Otherwise, read the page and return it but don't add it to the cache
		debug_printf("DWALPager(%s) op=read %s noHit=%d\n", filename.c_str(), toString(pageID).c_str(), noHit);
		auto& eventReasons = g_redwoodMetrics.level(level).metrics.events;
		eventReasons.addEventReason(PagerEvents::CacheLookup, reason);
		if (!cacheable) {
			debug_printf("DWALPager(%s) op=readUncached %s\n", filename.c_str(), toString(pageID).c_str());
			PageCacheEntry* pCacheEntry = pageCache.getIfExists(pageID);
			if (pCacheEntry != nullptr) {
				++g_redwoodMetrics.metric.pagerProbeHit;
				debug_printf("DWALPager(%s) op=readUncachedHit %s\n", filename.c_str(), toString(pageID).c_str());
				return pCacheEntry->readFuture;
			}
			++g_redwoodMetrics.metric.pagerProbeMiss;
			debug_printf("DWALPager(%s) op=readUncachedMiss %s\n", filename.c_str(), toString(pageID).c_str());
			return forwardError(readPhysicalPage(this, pageID, priority, false), errorPromise);
		}
		PageCacheEntry& cacheEntry = pageCache.get(pageID, physicalPageSize, noHit);
		debug_printf("DWALPager(%s) op=read %s cached=%d reading=%d writing=%d noHit=%d\n",
		             filename.c_str(),
		             toString(pageID).c_str(),
		             cacheEntry.initialized(),
		             cacheEntry.initialized() && cacheEntry.reading(),
		             cacheEntry.initialized() && cacheEntry.writing(),
		             noHit);
		if (!cacheEntry.initialized()) {
			debug_printf("DWALPager(%s) issuing actual read of %s\n", filename.c_str(), toString(pageID).c_str());
			cacheEntry.readFuture = forwardError(readPhysicalPage(this, pageID, priority, false), errorPromise);
			cacheEntry.writeFuture = Void();

			++g_redwoodMetrics.metric.pagerCacheMiss;
			eventReasons.addEventReason(PagerEvents::CacheMiss, reason);
		} else {
			++g_redwoodMetrics.metric.pagerCacheHit;
			eventReasons.addEventReason(PagerEvents::CacheHit, reason);
		}
		return cacheEntry.readFuture;
	}

	Future<Reference<ArenaPage>> readMultiPage(PagerEventReasons reason,
	                                           unsigned int level,
	                                           Standalone<VectorRef<PhysicalPageID>> pageIDs,
	                                           int priority,
	                                           bool cacheable,
	                                           bool noHit) override {
		// Use cached page if present, without triggering a cache hit.
		// Otherwise, read the page and return it but don't add it to the cache
		debug_printf("DWALPager(%s) op=read %s noHit=%d\n", filename.c_str(), toString(pageIDs).c_str(), noHit);
		auto& eventReasons = g_redwoodMetrics.level(level).metrics.events;
		eventReasons.addEventReason(PagerEvents::CacheLookup, reason);
		if (!cacheable) {
			debug_printf("DWALPager(%s) op=readUncached %s\n", filename.c_str(), toString(pageIDs).c_str());
			PageCacheEntry* pCacheEntry = pageCache.getIfExists(pageIDs.front());
			if (pCacheEntry != nullptr) {
				++g_redwoodMetrics.metric.pagerProbeHit;
				debug_printf("DWALPager(%s) op=readUncachedHit %s\n", filename.c_str(), toString(pageIDs).c_str());
				return pCacheEntry->readFuture;
			}
			++g_redwoodMetrics.metric.pagerProbeMiss;
			debug_printf("DWALPager(%s) op=readUncachedMiss %s\n", filename.c_str(), toString(pageIDs).c_str());
			return forwardError(readPhysicalMultiPage(this, pageIDs, priority), errorPromise);
		}

		PageCacheEntry& cacheEntry = pageCache.get(pageIDs.front(), pageIDs.size() * physicalPageSize, noHit);
		debug_printf("DWALPager(%s) op=read %s cached=%d reading=%d writing=%d noHit=%d\n",
		             filename.c_str(),
		             toString(pageIDs).c_str(),
		             cacheEntry.initialized(),
		             cacheEntry.initialized() && cacheEntry.reading(),
		             cacheEntry.initialized() && cacheEntry.writing(),
		             noHit);
		if (!cacheEntry.initialized()) {
			debug_printf("DWALPager(%s) issuing actual read of %s\n", filename.c_str(), toString(pageIDs).c_str());
			cacheEntry.readFuture = forwardError(readPhysicalMultiPage(this, pageIDs, priority), errorPromise);
			cacheEntry.writeFuture = Void();

			++g_redwoodMetrics.metric.pagerCacheMiss;
			eventReasons.addEventReason(PagerEvents::CacheMiss, reason);
		} else {
			++g_redwoodMetrics.metric.pagerCacheHit;
			eventReasons.addEventReason(PagerEvents::CacheHit, reason);
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
				if (pageID == invalidLogicalPageID)
					debug_printf(
					    "DWALPager(%s) remappedPagesMap: %s\n", filename.c_str(), toString(remappedPages).c_str());

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

	Future<Reference<ArenaPage>> readPageAtVersion(PagerEventReasons reason,
	                                               unsigned int level,
	                                               LogicalPageID logicalID,
	                                               int priority,
	                                               Version v,
	                                               bool cacheable,
	                                               bool noHit) {
		PhysicalPageID physicalID = getPhysicalPageID(logicalID, v);
		return readPage(reason, level, physicalID, priority, cacheable, noHit);
	}

	Future<Reference<ArenaPage>> readMultiPageAtVersion(PagerEventReasons reason,
	                                                    unsigned int level,
	                                                    VectorRef<LogicalPageID> logicalIDs,
	                                                    int priority,
	                                                    Version v,
	                                                    bool cacheable,
	                                                    bool noHit) {
		Standalone<VectorRef<PhysicalPageID>> ids;
		ids.resize(ids.arena(), logicalIDs.size());
		for (int i = 0; i < logicalIDs.size(); ++i) {
			ids[i] = getPhysicalPageID(logicalIDs[i], v);
		}
		debug_printf("op=readMultiPageAtVersion, from logicalIDs %s to phsyicalIDs %s\n",
		             toString(logicalIDs).c_str(),
		             toString(ids).c_str());
		return readMultiPage(reason, level, ids, priority, cacheable, noHit);
	}

	void releaseExtentReadLock() override { concurrentExtentReads->release(); }

	// Read the physical extent at given pageID
	// NOTE that we use the same interface (<ArenaPage>) for the extent as the page
	ACTOR static Future<Reference<ArenaPage>> readPhysicalExtent(DWALPager* self,
	                                                             PhysicalPageID pageID,
	                                                             int readSize = 0) {
		// First take the concurrentExtentReads lock to avoid issuing too many reads concurrently
		wait(self->concurrentExtentReads->take());

		ASSERT(!self->memoryOnly);

		if (g_network->getCurrentTask() > TaskPriority::DiskRead) {
			wait(delay(0, TaskPriority::DiskRead));
		}

		// readSize may not be equal to the physical extent size (for the first and last extents)
		if (!readSize)
			readSize = self->physicalExtentSize;

		state Reference<ArenaPage> extent = Reference<ArenaPage>(new ArenaPage(self->logicalPageSize, readSize));

		// physicalReadSize is the size of disk read we intend to issue
		auto physicalReadSize = SERVER_KNOBS->REDWOOD_DEFAULT_EXTENT_READ_SIZE;
		auto parallelReads = readSize / physicalReadSize;
		auto lastReadSize = readSize % physicalReadSize;

		debug_printf(
		    "DWALPager(%s) op=readPhysicalExtentStart %s readSize %d offset %d physicalReadSize %d parallelReads %d\n",
		    self->filename.c_str(),
		    toString(pageID).c_str(),
		    readSize,
		    (int64_t)pageID * (self->physicalPageSize),
		    physicalReadSize,
		    parallelReads);

		// we split the extent read into a number of parallel disk reads based on the determined physical
		// disk read size. All those reads are issued in parallel and their futures are stored into the following
		// reads vector
		std::vector<Future<int>> reads;
		int i;
		int64_t startOffset = (int64_t)pageID * (self->physicalPageSize);
		int64_t currentOffset;
		for (i = 0; i < parallelReads; i++) {
			currentOffset = i * physicalReadSize;
			debug_printf("DWALPager(%s) current offset %d\n", self->filename.c_str(), currentOffset);
			++g_redwoodMetrics.metric.pagerDiskRead;
			reads.push_back(
			    self->pageFile->read(extent->mutate() + currentOffset, physicalReadSize, startOffset + currentOffset));
		}

		// Handle the last read separately as it may be smaller than physicalReadSize
		if (lastReadSize) {
			currentOffset = i * physicalReadSize;
			debug_printf("DWALPager(%s) iter %d current offset %d lastReadSize %d\n",
			             self->filename.c_str(),
			             i,
			             currentOffset,
			             lastReadSize);
			++g_redwoodMetrics.metric.pagerDiskRead;
			reads.push_back(
			    self->pageFile->read(extent->mutate() + currentOffset, lastReadSize, startOffset + currentOffset));
		}

		// wait for all the parallel read futures for the given extent
		wait(waitForAll(reads));

		debug_printf("DWALPager(%s) op=readPhysicalExtentComplete %s ptr=%p bytes=%d file offset=%d\n",
		             self->filename.c_str(),
		             toString(pageID).c_str(),
		             extent->begin(),
		             readSize,
		             (pageID * self->physicalPageSize));

		return extent;
	}

	Future<Reference<ArenaPage>> readExtent(LogicalPageID pageID) override {
		debug_printf("DWALPager(%s) op=readExtent %s\n", filename.c_str(), toString(pageID).c_str());
		PageCacheEntry* pCacheEntry = extentCache.getIfExists(pageID);
		auto& eventReasons = g_redwoodMetrics.level(nonBtreeLevel).metrics.events;
		if (pCacheEntry != nullptr) {
			eventReasons.addEventReason(PagerEvents::CacheLookup, PagerEventReasons::MetaData);
			debug_printf("DWALPager(%s) Cache Entry exists for %s\n", filename.c_str(), toString(pageID).c_str());
			return pCacheEntry->readFuture;
		}
		eventReasons.addEventReason(PagerEvents::CacheLookup, PagerEventReasons::MetaData);

		LogicalPageID headPageID = pHeader->remapQueue.headPageID;
		LogicalPageID tailPageID = pHeader->remapQueue.tailPageID;
		int readSize = physicalExtentSize;
		bool headExt = false;
		bool tailExt = false;
		debug_printf("DWALPager(%s) #extentPages: %d, headPageID: %s, tailPageID: %s\n",
		             filename.c_str(),
		             pagesPerExtent,
		             toString(headPageID).c_str(),
		             toString(tailPageID).c_str());

		if (headPageID >= pageID && ((headPageID - pageID) < pagesPerExtent))
			headExt = true;
		if ((tailPageID - pageID) < pagesPerExtent)
			tailExt = true;
		if (headExt && tailExt) {
			readSize = (tailPageID - headPageID + 1) * physicalPageSize;
		} else if (headExt) {
			readSize = (pagesPerExtent - (headPageID - pageID)) * physicalPageSize;
		} else if (tailExt) {
			readSize = (tailPageID - pageID + 1) * physicalPageSize;
		}

		PageCacheEntry& cacheEntry = extentCache.get(pageID, 1);
		if (!cacheEntry.initialized()) {
			cacheEntry.writeFuture = Void();
			cacheEntry.readFuture =
			    forwardError(readPhysicalExtent(this, (PhysicalPageID)pageID, readSize), errorPromise);
			debug_printf("DWALPager(%s) Set the cacheEntry readFuture for page: %s\n",
			             filename.c_str(),
			             toString(pageID).c_str());

			++g_redwoodMetrics.metric.pagerCacheMiss;
			eventReasons.addEventReason(PagerEvents::CacheMiss, PagerEventReasons::MetaData);
			eventReasons.addEventReason(PagerEvents::CacheLookup, PagerEventReasons::MetaData);
		} else {
			++g_redwoodMetrics.metric.pagerCacheHit;
			eventReasons.addEventReason(PagerEvents::CacheHit, PagerEventReasons::MetaData);
			eventReasons.addEventReason(PagerEvents::CacheLookup, PagerEventReasons::MetaData);
		}
		return cacheEntry.readFuture;
	}

	// Get snapshot as of the most recent committed version of the pager
	Reference<IPagerSnapshot> getReadSnapshot(Version v) override;
	void addSnapshot(Version version, KeyRef meta) {
		ASSERT(snapshots.empty() || snapshots.back().version != version);

		Promise<Void> expired;
		snapshots.push_back(
		    { version, expired, makeReference<DWALPagerSnapshot>(this, meta, version, expired.getFuture()) });
	}

	// Set the pending oldest versiont to keep as of the next commit
	void setOldestReadableVersion(Version v) override {
		ASSERT(v >= pHeader->oldestVersion);
		ASSERT(v <= pHeader->committedVersion);
		pHeader->oldestVersion = v;
		expireSnapshots(v);
	};

	// Get the oldest *readable* version, which is not the same as the oldest retained version as the version
	// returned could have been set as the oldest version in the pending commit
	Version getOldestReadableVersion() const override { return pHeader->oldestVersion; };

	// Calculate the *effective* oldest version, which can be older than the one set in the last commit since we
	// are allowing active snapshots to temporarily delay page reuse.
	Version effectiveOldestVersion() {
		if (snapshots.empty()) {
			debug_printf("DWALPager(%s) snapshots list empty\n", filename.c_str());
			return pLastCommittedHeader->oldestVersion;
		}
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
			Reference<ArenaPage> data = wait(
			    self->readPage(PagerEventReasons::MetaData, nonBtreeLevel, p.newPageID, ioLeafPriority, false, true));

			// Write the data to the original page so it can be read using its original pageID
			self->updatePage(
			    PagerEventReasons::MetaData, nonBtreeLevel, VectorRef<LogicalPageID>(&p.originalPageID, 1), data);
			++g_redwoodMetrics.metric.pagerRemapCopy;
		} else if (firstType == RemappedPage::REMAP) {
			++g_redwoodMetrics.metric.pagerRemapSkip;
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
			++g_redwoodMetrics.metric.pagerRemapFree;
		}

		if (freeOriginalID) {
			debug_printf("DWALPager(%s) remapCleanup freeOriginal %s\n", self->filename.c_str(), p.toString().c_str());
			self->freeUnmappedPage(p.originalPageID, 0);
			++g_redwoodMetrics.metric.pagerRemapFree;
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
		state RemappedPage cutoff(oldestRetainedVersion);

		// Maximum number of remaining remap entries to keep before obeying stop command.
		double toleranceRatio = BUGGIFY ? deterministicRandom()->randomInt(0, 10) / 100.0
		                                : SERVER_KNOBS->REDWOOD_REMAP_CLEANUP_TOLERANCE_RATIO;
		// For simplicity, we assume each entry in the remap queue corresponds to one remapped page.
		uint64_t remapCleanupWindowEntries =
		    static_cast<uint64_t>(self->remapCleanupWindowBytes / self->pHeader->pageSize);
		state uint64_t minRemapEntries = static_cast<uint64_t>(remapCleanupWindowEntries * (1.0 - toleranceRatio));
		state uint64_t maxRemapEntries = static_cast<uint64_t>(remapCleanupWindowEntries * (1.0 + toleranceRatio));

		debug_printf("DWALPager(%s) remapCleanup oldestRetainedVersion=%" PRId64 " remapCleanupWindowBytes=%" PRId64
		             " pageSize=%" PRIu32 " minRemapEntries=%" PRId64 " maxRemapEntries=%" PRId64 " items=%" PRId64
		             "\n",
		             self->filename.c_str(),
		             oldestRetainedVersion,
		             self->remapCleanupWindowBytes,
		             self->pHeader->pageSize,
		             minRemapEntries,
		             maxRemapEntries,
		             self->remapQueue.numEntries);

		if (g_network->isSimulated()) {
			self->remapDestinationsSimOnly.clear();
		}

		state int sinceYield = 0;
		loop {
			// Stop if we have cleanup enough remap entries, or if the stop flag is set and the remaining remap
			// entries are less than that allowed by the lag.
			int64_t remainingEntries = self->remapQueue.numEntries;
			if (remainingEntries <= minRemapEntries ||
			    (self->remapCleanupStop && remainingEntries <= maxRemapEntries)) {
				debug_printf("DWALPager(%s) remapCleanup finished remainingEntries=%" PRId64 " minRemapEntries=%" PRId64
				             " maxRemapEntries=%" PRId64,
				             self->filename.c_str(),
				             remainingEntries,
				             minRemapEntries,
				             maxRemapEntries);
				break;
			}
			state Optional<RemappedPage> p = wait(self->remapQueue.pop(cutoff));
			debug_printf("DWALPager(%s) remapCleanup popped %s items=%" PRId64 "\n",
			             self->filename.c_str(),
			             ::toString(p).c_str(),
			             self->remapQueue.numEntries);

			// Stop if we have reached the cutoff version, which is the start of the cleanup coalescing window
			if (!p.present()) {
				debug_printf("DWALPager(%s) remapCleanup pop failed cutoffVer=%" PRId64 " items=%" PRId64 "\n",
				             self->filename.c_str(),
				             cutoff.version,
				             self->remapQueue.numEntries);
				break;
			}

			Future<Void> task = removeRemapEntry(self, p.get(), oldestRetainedVersion);
			if (!task.isReady()) {
				tasks.add(task);
			}

			// Yield to prevent slow task in case no IO waits are encountered
			if (++sinceYield >= 100) {
				sinceYield = 0;
				wait(yield());
			}
		}

		debug_printf("DWALPager(%s) remapCleanup stopped stopSignal=%d remap=%" PRId64 " free=%" PRId64
		             " delayedFree=%" PRId64 "\n",
		             self->filename.c_str(),
		             self->remapCleanupStop,
		             self->remapQueue.numEntries,
		             self->freeList.numEntries,
		             self->delayedFreeList.numEntries);
		signal.send(Void());
		wait(tasks.getResult());
		return Void();
	}

	// Flush all queues so they have no operations pending.
	ACTOR static Future<Void> flushQueues(DWALPager* self) {
		ASSERT(self->remapCleanupFuture.isReady());

		// Flush remap queue and related queues separately, they are not involved in free page management
		wait(self->remapQueue.flush());
		wait(self->extentFreeList.flush());
		wait(self->extentUsedList.flush());

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

	ACTOR static Future<Void> commit_impl(DWALPager* self, Version v) {
		debug_printf("DWALPager(%s) commit begin %s\n", self->filename.c_str(), ::toString(v).c_str());

		// Write old committed header to Page 1
		self->writeHeaderPage(1, self->lastCommittedHeaderPage);

		// Trigger the remap eraser to stop and then wait for it.
		self->remapCleanupStop = true;
		wait(self->remapCleanupFuture);

		wait(flushQueues(self));

		self->pHeader->committedVersion = v;
		self->pHeader->remapQueue = self->remapQueue.getState();
		self->pHeader->extentFreeList = self->extentFreeList.getState();
		self->pHeader->extentUsedList = self->extentUsedList.getState();
		self->pHeader->freeList = self->freeList.getState();
		self->pHeader->delayedFreeList = self->delayedFreeList.getState();

		// Wait for all outstanding writes to complete
		debug_printf("DWALPager(%s) waiting for outstanding writes\n", self->filename.c_str());
		wait(waitForAll(self->operations));
		self->operations.clear();
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
		self->addSnapshot(v, self->pHeader->getMetaKey());

		// Try to expire snapshots up to the oldest version, in case some were being kept around due to being in use,
		// because maybe some are no longer in use.
		self->expireSnapshots(self->pHeader->oldestVersion);

		// Start unmapping pages for expired versions
		self->remapCleanupFuture = remapCleanup(self);

		// If there are prioritized evictions queued, flush them to the regular eviction order.
		self->pageCache.flushPrioritizedEvictions();

		return Void();
	}

	Future<Void> commit(Version v) override {
		// Can't have more than one commit outstanding.
		ASSERT(commitFuture.isReady());
		ASSERT(v > pLastCommittedHeader->committedVersion);
		commitFuture = forwardError(commit_impl(this, v), errorPromise);
		return commitFuture;
	}

	Key getMetaKey() const override { return pHeader->getMetaKey(); }

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
		wait(waitForAll(self->operations));
		self->operations.clear();

		debug_printf("DWALPager(%s) shutdown destroy page cache\n", self->filename.c_str());
		wait(self->pageCache.clear());
		wait(delay(0));

		debug_printf("DWALPager(%s) shutdown remappedPagesMap: %s\n",
		             self->filename.c_str(),
		             toString(self->remappedPages).c_str());

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

	Future<Void> getError() const override { return errorPromise.getFuture(); }

	Future<Void> onClosed() const override { return closedPromise.getFuture(); }

	StorageBytes getStorageBytes() const override {
		int64_t free;
		int64_t total;
		if (memoryOnly) {
			total = pageCache.evictor().sizeLimit;
			free = pageCache.evictor().getSizeUsed();
		} else {
			g_network->getDiskBytes(parentDirectory(filename), free, total);
		}
		int64_t pagerSize = pHeader->pageCount * physicalPageSize;

		// It is not exactly known how many pages on the delayed free list are usable as of right now.  It could be
		// known, if each commit delayed entries that were freeable were shuffled from the delayed free queue to the
		// free queue, but this doesn't seem necessary.

		// Amount of space taken up by all of the items in the free lists
		int64_t reusablePageSpace = (freeList.numEntries + delayedFreeList.numEntries) * physicalPageSize;
		// Amount of space taken up by the free list queues themselves, as if we were to pop and use
		// items on the free lists the space the items are stored in would also become usable
		int64_t reusableQueueSpace = (freeList.numPages + delayedFreeList.numPages) * physicalPageSize;
		int64_t reusable = reusablePageSpace + reusableQueueSpace;

		// Space currently in used by old page versions have have not yet been freed due to the remap cleanup window.
		int64_t temp = remapQueue.numEntries * physicalPageSize;

		return StorageBytes(free, total, pagerSize - reusable, free + reusable, temp);
	}

	int64_t getPageCacheCount() override { return pageCache.getCount(); }
	int64_t getPageCount() override { return pHeader->pageCount; }
	int64_t getExtentCacheCount() override { return extentCache.getCount(); }

	ACTOR static Future<Void> getUserPageCount_cleanup(DWALPager* self) {
		// Wait for the remap eraser to finish all of its work (not triggering stop)
		wait(self->remapCleanupFuture);

		// Flush queues so there are no pending freelist operations
		wait(flushQueues(self));

		debug_printf("DWALPager getUserPageCount_cleanup\n");
		self->freeList.getState();
		self->delayedFreeList.getState();
		self->extentFreeList.getState();
		self->extentUsedList.getState();
		self->remapQueue.getState();
		return Void();
	}

	// Get the number of pages in use by the pager's user
	Future<int64_t> getUserPageCount() override {
		return map(getUserPageCount_cleanup(this), [=](Void) {
			int64_t userPages =
			    pHeader->pageCount - 2 - freeList.numPages - freeList.numEntries - delayedFreeList.numPages -
			    delayedFreeList.numEntries - ((((remapQueue.numPages - 1) / pagesPerExtent) + 1) * pagesPerExtent) -
			    extentFreeList.numPages - (pagesPerExtent * extentFreeList.numEntries) - extentUsedList.numPages;

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

	Version getLastCommittedVersion() const override { return pLastCommittedHeader->committedVersion; }

private:
	~DWALPager() {}

	// Try to expire snapshots up to but not including v, but do not expire any snapshots that are in use.
	void expireSnapshots(Version v);

#pragma pack(push, 1)
	// Header is the format of page 0 of the database
	struct Header {
		static constexpr int FORMAT_VERSION = 8;
		uint16_t formatVersion;
		uint32_t queueCount;
		uint32_t pageSize;
		int64_t pageCount;
		uint32_t extentSize;
		FIFOQueue<LogicalPageID>::QueueState freeList;
		FIFOQueue<LogicalPageID>::QueueState extentFreeList; // free list for extents
		FIFOQueue<ExtentUsedListEntry>::QueueState extentUsedList; // in-use list for extents
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

	ACTOR static Future<Void> clearRemapQueue_impl(DWALPager* self) {
		// Wait for outstanding commit.
		wait(self->commitFuture);

		// Set remap cleanup window to 0 to allow the remap queue to drain.
		state int64_t remapCleanupWindowBytes = self->remapCleanupWindowBytes;
		self->remapCleanupWindowBytes = 0;

		// Try twice to commit and advance version. The first commit should trigger a remap cleanup actor, which picks
		// up the new remap cleanup window being 0. The second commit waits for the remap cleanup actor to finish.
		state int attempt = 0;
		for (attempt = 0; attempt < 2; attempt++) {
			self->setOldestReadableVersion(self->getLastCommittedVersion());
			wait(self->commit(self->getLastCommittedVersion() + 1));
		}
		ASSERT(self->remapQueue.numEntries == 0);

		// Restore remap cleanup window.
		if (remapCleanupWindowBytes != 0)
			self->remapCleanupWindowBytes = remapCleanupWindowBytes;

		TraceEvent e("RedwoodClearRemapQueue");
		self->toTraceEvent(e);
		e.log();
		return Void();
	}

	Future<Void> clearRemapQueue() override { return clearRemapQueue_impl(this); }

private:
	// Physical page sizes will always be a multiple of 4k because AsyncFileNonDurable requires
	// this in simulation, and it also makes sense for current SSDs.
	// Allowing a smaller 'logical' page size is very useful for testing.
	static constexpr int smallestPhysicalBlock = 4096;
	int physicalPageSize;
	int logicalPageSize; // In simulation testing it can be useful to use a small logical page size

	// Extents are multi-page blocks used by the FIFO queues
	int physicalExtentSize;
	int pagesPerExtent;

	PriorityMultiLock ioLock;

	int64_t pageCacheBytes;

	// The header will be written to / read from disk as a smallestPhysicalBlock sized chunk.
	Reference<ArenaPage> headerPage;
	Header* pHeader;

	// Pages - pages known to be in the file, truncations complete to that size
	int64_t filePageCount;
	// Pages that will be in file once fileExtension is ready
	int64_t filePageCountPending;
	// Future representing the end of all pending truncations
	Future<Void> fileExtension;

	int desiredPageSize;
	int desiredExtentSize;

	Version recoveryVersion;
	Reference<ArenaPage> lastCommittedHeaderPage;
	Header* pLastCommittedHeader;

	std::string filename;
	bool memoryOnly;

	PageCacheT pageCache;

	// The extent cache isn't a normal cache, it isn't allowed to evict things.  It is populated
	// during recovery with remap queue extents and then cleared.
	PageCacheT::Evictor extentCacheDummyEvictor{ std::numeric_limits<int64_t>::max() };
	PageCacheT extentCache{ &extentCacheDummyEvictor };

	Promise<Void> closedPromise;
	Promise<Void> errorPromise;
	Future<Void> commitFuture;
	std::vector<Future<Void>> operations;
	Future<Void> recoverFuture;
	Future<Void> remapCleanupFuture;
	bool remapCleanupStop;

	Reference<IAsyncFile> pageFile;

	LogicalPageQueueT freeList;

	// The delayed free list will be approximately in Version order.
	// TODO: Make this an ordered container some day.
	DelayedFreePageQueueT delayedFreeList;

	RemapQueueT remapQueue;
	LogicalPageQueueT extentFreeList;
	ExtentUsedListQueueT extentUsedList;
	uint64_t remapCleanupWindowBytes;
	Reference<FlowLock> concurrentExtentReads;
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
	  : pager(pager), expired(expiredFuture), version(version), metaKey(meta) {}
	~DWALPagerSnapshot() override {}

	Future<Reference<const ArenaPage>> getPhysicalPage(PagerEventReasons reason,
	                                                   unsigned int level,
	                                                   LogicalPageID pageID,
	                                                   int priority,
	                                                   bool cacheable,
	                                                   bool noHit) override {
		if (expired.isError()) {
			throw expired.getError();
		}
		return map(pager->readPageAtVersion(reason, level, pageID, priority, version, cacheable, noHit),
		           [=](Reference<ArenaPage> p) { return Reference<const ArenaPage>(std::move(p)); });
	}

	Future<Reference<const ArenaPage>> getMultiPhysicalPage(PagerEventReasons reason,
	                                                        unsigned int level,
	                                                        VectorRef<LogicalPageID> pageIDs,
	                                                        int priority,
	                                                        bool cacheable,
	                                                        bool noHit) override {
		if (expired.isError()) {
			throw expired.getError();
		}
		return map(pager->readMultiPageAtVersion(reason, level, pageIDs, priority, version, cacheable, noHit),
		           [=](Reference<ArenaPage> p) { return Reference<const ArenaPage>(std::move(p)); });
	}

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
		const uint8_t* end{ nullptr };
		const uint8_t* next{ nullptr };

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

	RedwoodRecordRef(KeyRef key = KeyRef(), Optional<ValueRef> value = {}) : key(key), value(value) {}

	RedwoodRecordRef(Arena& arena, const RedwoodRecordRef& toCopy) : key(arena, toCopy.key) {
		if (toCopy.value.present()) {
			value = ValueRef(arena, toCopy.value.get());
		}
	}

	typedef KeyRef Partial;

	void updateCache(Optional<Partial>& cache, Arena& arena) const { cache = KeyRef(arena, key); }

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
		return RedwoodRecordRef(key, ValueRef((const uint8_t*)id.begin(), id.size() * sizeof(LogicalPageID)));
	}

	inline RedwoodRecordRef withoutValue() const { return RedwoodRecordRef(key); }

	inline RedwoodRecordRef withMaxPageID() const {
		return RedwoodRecordRef(key, StringRef((uint8_t*)&maxPageID, sizeof(maxPageID)));
	}

	// Truncate (key, version, part) tuple to len bytes.
	void truncate(int len) {
		ASSERT(len <= key.size());
		key = key.substr(0, len);
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
			cmp = value.compare(rhs.value);
		}
		return cmp;
	}

	bool sameUserKey(const StringRef& k, int skipLen) const {
		// Keys are the same if the sizes are the same and either the skipLen is longer or the non-skipped suffixes are
		// the same.
		return (key.size() == k.size()) && (key.substr(skipLen) == k.substr(skipLen));
	}

	bool sameExceptValue(const RedwoodRecordRef& rhs, int skipLen = 0) const { return sameUserKey(rhs.key, skipLen); }

	// TODO: Use SplitStringRef (unless it ends up being slower)
	KeyRef key;
	Optional<ValueRef> value;

	int expectedSize() const { return key.expectedSize() + value.expectedSize(); }
	int kvBytes() const { return expectedSize(); }

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

		static constexpr int LengthFormatSizes[] = { sizeof(LengthFormat0),
			                                         sizeof(LengthFormat1),
			                                         sizeof(LengthFormat2),
			                                         sizeof(LengthFormat3) };

		// Serialized Format
		//
		// Flags - 1 byte
		//    1 bit - borrow source is prev ancestor (otherwise next ancestor)
		//    1 bit - item is deleted
		//    1 bit - has value (different from a zero-length value, which is still a value)
		//    3 unused bits
		//    2 bits - length fields format
		//
		// Length fields using 3 to 8 bytes total depending on length fields format
		//
		// Byte strings
		//    Value bytes
		//    Key suffix bytes

		enum EFlags {
			PREFIX_SOURCE_PREV = 0x80,
			IS_DELETED = 0x40,
			HAS_VALUE = 0x20,
			// 3 unused bits
			LENGTHS_FORMAT = 0x03
		};

		// Figure out which length format must be used for the given lengths
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

		StringRef getKeySuffix() const { return StringRef(data() + getValueLength(), getKeySuffixLength()); }

		StringRef getValue() const { return StringRef(data(), getValueLength()); }

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

		// DeltaTree interface
		RedwoodRecordRef apply(const RedwoodRecordRef& base, Arena& arena) const {
			int keyPrefixLen = getKeyPrefixLength();
			int keySuffixLen = getKeySuffixLength();
			int valueLen = hasValue() ? getValueLength() : 0;
			byte* pData = data();

			StringRef k;
			// If there is a key suffix, reconstitute the complete key into a contiguous string
			if (keySuffixLen > 0) {
				k = makeString(keyPrefixLen + keySuffixLen, arena);
				memcpy(mutateString(k), base.key.begin(), keyPrefixLen);
				memcpy(mutateString(k) + keyPrefixLen, pData + valueLen, keySuffixLen);
			} else {
				// Otherwise just reference the base key's memory
				k = base.key.substr(0, keyPrefixLen);
			}

			return RedwoodRecordRef(k, hasValue() ? ValueRef(pData, valueLen) : Optional<ValueRef>());
		}

		// DeltaTree interface
		RedwoodRecordRef apply(const Partial& cache) {
			return RedwoodRecordRef(cache, hasValue() ? Optional<ValueRef>(getValue()) : Optional<ValueRef>());
		}

		RedwoodRecordRef apply(Arena& arena, const Partial& baseKey, Optional<Partial>& cache) {
			int keyPrefixLen = getKeyPrefixLength();
			int keySuffixLen = getKeySuffixLength();
			int valueLen = hasValue() ? getValueLength() : 0;
			byte* pData = data();

			StringRef k;
			// If there is a key suffix, reconstitute the complete key into a contiguous string
			if (keySuffixLen > 0) {
				k = makeString(keyPrefixLen + keySuffixLen, arena);
				memcpy(mutateString(k), baseKey.begin(), keyPrefixLen);
				memcpy(mutateString(k) + keyPrefixLen, pData + valueLen, keySuffixLen);
			} else {
				// Otherwise just reference the base key's memory
				k = baseKey.substr(0, keyPrefixLen);
			}
			cache = k;

			return RedwoodRecordRef(k, hasValue() ? ValueRef(pData, valueLen) : Optional<ValueRef>());
		}

		RedwoodRecordRef apply(Arena& arena, const RedwoodRecordRef& base, Optional<Partial>& cache) {
			return apply(arena, base.key, cache);
		}

		int size() const {
			int size = 1;
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
			int lengthFormat = flags & LENGTHS_FORMAT;

			int prefixLen = getKeyPrefixLength();
			int keySuffixLen = getKeySuffixLength();
			int valueLen = getValueLength();

			return format("lengthFormat: %d  totalDeltaSize: %d  flags: %s  prefixLen: %d  keySuffixLen: %d  "
			              "valueLen %d  raw: %s",
			              lengthFormat,
			              size(),
			              flagString.c_str(),
			              prefixLen,
			              keySuffixLen,
			              valueLen,
			              StringRef((const uint8_t*)this, size()).toHexString().c_str());
		}
	};

	// Using this class as an alternative for Delta enables reading a DeltaTree2<RecordRef> while only decoding
	// its values, so the Reader does not require the original prev/next ancestors.
	struct DeltaValueOnly : Delta {
		RedwoodRecordRef apply(const RedwoodRecordRef& base, Arena& arena) const {
			return RedwoodRecordRef(KeyRef(), hasValue() ? Optional<ValueRef>(getValue()) : Optional<ValueRef>());
		}

		RedwoodRecordRef apply(const Partial& cache) {
			return RedwoodRecordRef(KeyRef(), hasValue() ? Optional<ValueRef>(getValue()) : Optional<ValueRef>());
		}

		RedwoodRecordRef apply(Arena& arena, const RedwoodRecordRef& base, Optional<Partial>& cache) {
			cache = KeyRef();
			return RedwoodRecordRef(KeyRef(), hasValue() ? Optional<ValueRef>(getValue()) : Optional<ValueRef>());
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
		if (worstCaseOverhead) {
			formatType = Delta::determineLengthFormat(key.size(), key.size(), valueLen);
		} else {
			formatType = Delta::determineLengthFormat(prefixLen, keySuffixLen, valueLen);
		}

		return 1 + Delta::LengthFormatSizes[formatType] + keySuffixLen + valueLen;
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

		// Write value bytes
		if (valueLen > 0) {
			wptr = value.get().copyTo(wptr);
		}

		// Write key suffix string
		wptr = keySuffix.copyTo(wptr);

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
		r += format("'%s' => ", key.printable().c_str());
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
	typedef DeltaTree2<RedwoodRecordRef> BinaryTree;
	typedef DeltaTree2<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly> ValueTree;

#pragma pack(push, 1)
	struct {
		uint8_t height;
		uint32_t kvBytes;
	};
#pragma pack(pop)

	int size() const {
		const BinaryTree* t = tree();
		return (uint8_t*)t - (uint8_t*)this + t->size();
	}

	bool isLeaf() const { return height == 1; }

	BinaryTree* tree() { return (BinaryTree*)(this + 1); }

	BinaryTree* tree() const { return (BinaryTree*)(this + 1); }

	ValueTree* valueTree() const { return (ValueTree*)(this + 1); }

	std::string toString(bool write,
	                     BTreePageIDRef id,
	                     Version ver,
	                     const RedwoodRecordRef& lowerBound,
	                     const RedwoodRecordRef& upperBound) const {
		std::string r;
		r += format("BTreePage op=%s %s @%" PRId64
		            " ptr=%p height=%d count=%d kvBytes=%d\n  lowerBound: %s\n  upperBound: %s\n",
		            write ? "write" : "read",
		            ::toString(id).c_str(),
		            ver,
		            this,
		            height,
		            (int)tree()->numItems,
		            (int)kvBytes,
		            lowerBound.toString(false).c_str(),
		            upperBound.toString(false).c_str());
		try {
			if (tree()->numItems > 0) {
				// This doesn't use the cached reader for the page because it is only for debugging purposes,
				// a cached reader may not exist
				BinaryTree::DecodeCache cache(lowerBound, upperBound);
				BinaryTree::Cursor c(&cache, tree());

				c.moveFirst();
				ASSERT(c.valid());

				do {
					r += "  ";
					r += c.get().toString(height == 1);

					// Out of range entries are annotated but can actually be valid, as they can be the result of
					// subtree deletion followed by incremental insertions of records in the deleted range being added
					// to an adjacent subtree which is logically expanded encompass the deleted range but still is using
					// the original subtree boundaries as DeltaTree2 boundaries.
					bool tooLow = c.get().withoutValue() < lowerBound.withoutValue();
					bool tooHigh = c.get().withoutValue() >= upperBound.withoutValue();
					if (tooLow || tooHigh) {
						if (tooLow) {
							r += " (below decode lower bound)";
						}
						if (tooHigh) {
							r += " (at or above decode upper bound)";
						}
					}
					r += "\n";

				} while (c.moveNext());
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

struct BoundaryRefAndPage {
	Standalone<RedwoodRecordRef> lowerBound;
	Reference<ArenaPage> firstPage;
	std::vector<Reference<ArenaPage>> extPages;

	std::string toString() const {
		return format("[%s, %d pages]", lowerBound.toString().c_str(), extPages.size() + (firstPage ? 1 : 0));
	}
};

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

	int size() const { return count; }
	int sizeBytes() const { return count * sizeof(T); }
};
#pragma pack(pop)

// DecodeBoundaryVerifier provides simulation-only verification of DeltaTree boundaries between
// reads and writes by using a static structure to track boundaries used during DeltaTree generation
// for all writes and updates across cold starts and virtual process restarts.
struct DecodeBoundaryVerifier {
	struct DecodeBoundaries {
		Key lower;
		Key upper;
		bool empty() const { return lower.empty() && upper.empty(); }
	};

	typedef std::map<Version, DecodeBoundaries> BoundariesByVersion;
	std::unordered_map<LogicalPageID, BoundariesByVersion> boundariesByPageID;

	static DecodeBoundaryVerifier* getVerifier(std::string name) {
		static std::map<std::string, DecodeBoundaryVerifier> verifiers;
		// Verifier disabled due to not being finished
		//
		// Only use verifier in a non-restarted simulation so that all page writes are captured
		// if (g_network->isSimulated() && !g_simulator.restarted) {
		// 	return &verifiers[name];
		// }
		return nullptr;
	}

	void update(BTreePageIDRef id, Version v, Key lowerBound, Key upperBound) {
		debug_printf("decodeBoundariesUpdate %s %s '%s' to '%s'\n",
		             ::toString(id).c_str(),
		             ::toString(v).c_str(),
		             lowerBound.toString().c_str(),
		             upperBound.toString().c_str());

		auto& b = boundariesByPageID[id.front()][v];
		ASSERT(b.empty());
		b = { lowerBound, upperBound };
	}

	bool verify(LogicalPageID id, Version v, Key lowerBound, Key upperBound) {
		auto i = boundariesByPageID.find(id);
		ASSERT(i != boundariesByPageID.end());
		ASSERT(!i->second.empty());

		auto b = i->second.upper_bound(v);
		--b;
		if (b->second.lower != lowerBound || b->second.upper != upperBound) {
			fprintf(stderr,
			        "Boundary mismatch on %s %s\nFound   :%s %s\nExpected:%s %s\n",
			        ::toString(id).c_str(),
			        ::toString(v).c_str(),
			        lowerBound.toString().c_str(),
			        upperBound.toString().c_str(),
			        b->second.lower.toString().c_str(),
			        b->second.upper.toString().c_str());
			return false;
		}
		return true;
	}

	void update(Version v, LogicalPageID oldID, LogicalPageID newID) {
		debug_printf("decodeBoundariesUpdate copy %s %s to %s\n",
		             ::toString(v).c_str(),
		             ::toString(oldID).c_str(),
		             ::toString(newID).c_str());
		auto& old = boundariesByPageID[oldID];
		ASSERT(!old.empty());
		auto i = old.end();
		--i;
		boundariesByPageID[newID][v] = i->second;
	}
};

class VersionedBTree {
public:
	// The first possible internal record possible in the tree
	static RedwoodRecordRef dbBegin;
	// A record which is greater than the last possible record in the tree
	static RedwoodRecordRef dbEnd;

	struct LazyClearQueueEntry {
		uint8_t height;
		Version version;
		Standalone<BTreePageIDRef> pageID;

		bool operator<(const LazyClearQueueEntry& rhs) const { return version < rhs.version; }

		int readFromBytes(const uint8_t* src) {
			height = *(uint8_t*)src;
			src += sizeof(uint8_t);
			version = *(Version*)src;
			src += sizeof(Version);
			int count = *src++;
			pageID = BTreePageIDRef((LogicalPageID*)src, count);
			return bytesNeeded();
		}

		int bytesNeeded() const {
			return sizeof(uint8_t) + sizeof(Version) + 1 + (pageID.size() * sizeof(LogicalPageID));
		}

		int writeToBytes(uint8_t* dst) const {
			*(uint8_t*)dst = height;
			dst += sizeof(uint8_t);
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

		bool maybeUpdated(LogicalPageID child) const { return (mask(child) & bits) != 0; }

		uint32_t bits;
		int count;
	};

	typedef std::unordered_map<LogicalPageID, ParentInfo> ParentInfoMapT;

#pragma pack(push, 1)
	struct MetaKey {
		static constexpr int FORMAT_VERSION = 15;
		// This serves as the format version for the entire tree, individual pages will not be versioned
		uint16_t formatVersion;
		uint8_t height;
		LazyClearQueueT::QueueState lazyDeleteQueue;
		InPlaceArray<LogicalPageID> root;

		KeyRef asKeyRef() const { return KeyRef((uint8_t*)this, sizeof(MetaKey) + root.sizeBytes()); }

		void fromKeyRef(KeyRef k) {
			memcpy(this, k.begin(), k.size());
			ASSERT(formatVersion == FORMAT_VERSION);
		}

		std::string toString() {
			return format("{formatVersion=%d  height=%d  root=%s  lazyDeleteQueue=%s}",
			              (int)formatVersion,
			              (int)height,
			              ::toString(root.get()).c_str(),
			              lazyDeleteQueue.toString().c_str());
		}
	};
#pragma pack(pop)

	// All async opts on the btree are based on pager reads, writes, and commits, so
	// we can mostly forward these next few functions to the pager
	Future<Void> getError() const { return m_pager->getError(); }

	Future<Void> onClosed() const { return m_pager->onClosed(); }

	void close_impl(bool dispose) {
		auto* pager = m_pager;
		delete this;
		if (dispose)
			pager->dispose();
		else
			pager->close();
	}

	void dispose() { return close_impl(true); }

	void close() { return close_impl(false); }

	StorageBytes getStorageBytes() const { return m_pager->getStorageBytes(); }

	// Set key to value as of the next commit
	// The new value is not readable until after the next commit is completed.
	void set(KeyValueRef keyValue) {
		++m_mutationCount;
		++g_redwoodMetrics.metric.opSet;
		g_redwoodMetrics.metric.opSetKeyBytes += keyValue.key.size();
		g_redwoodMetrics.metric.opSetValueBytes += keyValue.value.size();
		m_pBuffer->insert(keyValue.key).mutation().setBoundaryValue(m_pBuffer->copyToArena(keyValue.value));
	}

	void clear(KeyRangeRef clearedRange) {
		++m_mutationCount;
		// Optimization for single key clears to create just one mutation boundary instead of two
		if (clearedRange.begin.size() == clearedRange.end.size() - 1 &&
		    clearedRange.end[clearedRange.end.size() - 1] == 0 && clearedRange.end.startsWith(clearedRange.begin)) {
			++g_redwoodMetrics.metric.opClear;
			++g_redwoodMetrics.metric.opClearKey;
			m_pBuffer->insert(clearedRange.begin).mutation().clearBoundary();
			return;
		}

		++g_redwoodMetrics.metric.opClear;
		MutationBuffer::iterator iBegin = m_pBuffer->insert(clearedRange.begin);
		MutationBuffer::iterator iEnd = m_pBuffer->insert(clearedRange.end);

		iBegin.mutation().clearAll();
		++iBegin;
		m_pBuffer->erase(iBegin, iEnd);
	}

	void setOldestReadableVersion(Version v) { m_newOldestVersion = v; }

	Version getOldestReadableVersion() const { return m_pager->getOldestReadableVersion(); }

	Version getLastCommittedVersion() const { return m_pager->getLastCommittedVersion(); }

	VersionedBTree(IPager2* pager, std::string name)
	  : m_pager(pager), m_pBuffer(nullptr), m_mutationCount(0), m_name(name), m_pHeader(nullptr), m_headerSpace(0) {

		m_pBoundaryVerifier = DecodeBoundaryVerifier::getVerifier(name);
		m_pDecodeCacheMemory = m_pager->getPageCachePenaltySource();
		m_lazyClearActor = 0;
		m_init = init_impl(this);
		m_latestCommit = m_init;
	}

	void toTraceEvent(TraceEvent& e) const {
		m_pager->toTraceEvent(e);
		m_lazyClearQueue.toTraceEvent(e, "LazyClearQueue");
	}

	ACTOR static Future<int> incrementalLazyClear(VersionedBTree* self) {
		ASSERT(self->m_lazyClearActor.isReady());
		self->m_lazyClearStop = false;

		// TODO: Is it contractually okay to always to read at the latest version?
		state Reference<IPagerSnapshot> snapshot =
		    self->m_pager->getReadSnapshot(self->m_pager->getLastCommittedVersion());
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
				entries.emplace_back(q.get(),
				                     self->readPage(PagerEventReasons::LazyClear,
				                                    q.get().height,
				                                    snapshot.getPtr(),
				                                    q.get().pageID,
				                                    ioLeafPriority,
				                                    true,
				                                    false));
				--toPop;
			}

			state int i;
			for (i = 0; i < entries.size(); ++i) {
				Reference<const ArenaPage> p = wait(entries[i].second);
				const LazyClearQueueEntry& entry = entries[i].first;
				const BTreePage& btPage = *(BTreePage*)p->begin();
				ASSERT(btPage.height == entry.height);
				auto& metrics = g_redwoodMetrics.level(entry.height).metrics;

				debug_printf("LazyClear: processing %s\n", toString(entry).c_str());

				// Level 1 (leaf) nodes should never be in the lazy delete queue
				ASSERT(entry.height > 1);

				// Iterate over page entries, skipping key decoding using BTreePage::ValueTree which uses
				// RedwoodRecordRef::DeltaValueOnly as the delta type type to skip key decoding
				BTreePage::ValueTree::DecodeCache cache(dbBegin, dbEnd);
				BTreePage::ValueTree::Cursor c(&cache, btPage.valueTree());
				ASSERT(c.moveFirst());
				Version v = entry.version;
				while (1) {
					if (c.get().value.present()) {
						BTreePageIDRef btChildPageID = c.get().getChildPage();
						// If this page is height 2, then the children are leaves so free them directly
						if (entry.height == 2) {
							debug_printf("LazyClear: freeing leaf child %s\n", toString(btChildPageID).c_str());
							self->freeBTreePage(1, btChildPageID, v);
							freedPages += btChildPageID.size();
							metrics.lazyClearFree += 1;
							metrics.lazyClearFreeExt += (btChildPageID.size() - 1);
						} else {
							// Otherwise, queue them for lazy delete.
							debug_printf("LazyClear: queuing child %s\n", toString(btChildPageID).c_str());
							self->m_lazyClearQueue.pushFront(
							    LazyClearQueueEntry{ (uint8_t)(entry.height - 1), v, btChildPageID });
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
				self->freeBTreePage(entry.height, entry.pageID, v);
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
		self->m_pBuffer.reset(new MutationBuffer());
		// TODO: Get actual max MetaKey size limit from Pager
		self->m_headerSpace = self->m_pager->getUsablePageSize();
		self->m_pHeader = (MetaKey*)new uint8_t[self->m_headerSpace];

		self->m_blockSize = self->m_pager->getUsablePageSize();
		self->m_newOldestVersion = self->m_pager->getOldestReadableVersion();

		debug_printf("Recovered pager to version %" PRId64 ", oldest version is %" PRId64 "\n",
		             self->m_newOldestVersion);

		state Key meta = self->m_pager->getMetaKey();
		if (meta.size() == 0) {
			// Create new BTree
			self->m_pHeader->formatVersion = MetaKey::FORMAT_VERSION;
			LogicalPageID id = wait(self->m_pager->newPageID());
			BTreePageIDRef newRoot((LogicalPageID*)&id, 1);
			debug_printf("new root %s\n", toString(newRoot).c_str());
			self->m_pHeader->root.set(newRoot, self->m_headerSpace - sizeof(MetaKey));
			self->m_pHeader->height = 1;
			Reference<ArenaPage> page = self->m_pager->newPageBuffer();
			self->makeEmptyRoot(page);
			self->m_pager->updatePage(PagerEventReasons::MetaData, nonBtreeLevel, newRoot, page);

			LogicalPageID newQueuePage = wait(self->m_pager->newPageID());
			self->m_lazyClearQueue.create(
			    self->m_pager, newQueuePage, "LazyClearQueue", self->m_pager->newLastQueueID(), false);
			self->m_pHeader->lazyDeleteQueue = self->m_lazyClearQueue.getState();
			self->m_pager->setMetaKey(self->m_pHeader->asKeyRef());

			debug_printf("BTree created (but not committed)\n");
		} else {
			self->m_pHeader->fromKeyRef(meta);
			self->m_lazyClearQueue.recover(self->m_pager, self->m_pHeader->lazyDeleteQueue, "LazyClearQueueRecovered");
			debug_printf("BTree recovered.\n");
		}

		self->m_lazyClearActor = 0;

		TraceEvent e(SevInfo, "RedwoodRecoveredBTree");
		e.detail("FileName", self->m_name);
		e.detail("OpenedExisting", meta.size() != 0);
		e.detail("LatestVersion", self->m_pager->getLastCommittedVersion());
		self->m_lazyClearQueue.toTraceEvent(e, "LazyClearQueue");
		e.log();

		debug_printf("Recovered btree at version %" PRId64 ": %s\n",
		             self->m_pager->getLastCommittedVersion(),
		             self->m_pHeader->toString().c_str());

		return Void();
	}

	Future<Void> init() { return m_init; }

	virtual ~VersionedBTree() {
		// This probably shouldn't be called directly (meaning deleting an instance directly) but it should be safe,
		// it will cancel init and commit and leave the pager alive but with potentially an incomplete set of
		// uncommitted writes so it should not be committed.
		m_init.cancel();
		m_latestCommit.cancel();

		if (m_pHeader != nullptr) {
			delete[](uint8_t*) m_pHeader;
		}
	}

	Future<Void> commit(Version v) { return commit_impl(this, v); }

	ACTOR static Future<Void> clearAllAndCheckSanity_impl(VersionedBTree* self) {
		// Clear and commit
		debug_printf("Clearing tree.\n");
		self->clear(KeyRangeRef(dbBegin.key, dbEnd.key));
		wait(self->commit(self->getLastCommittedVersion() + 1));

		// Loop commits until the the lazy delete queue is completely processed.
		loop {
			wait(self->commit(self->getLastCommittedVersion() + 1));

			// If the lazy delete queue is completely processed then the last time the lazy delete actor
			// was started it, after the last commit, it would exist immediately and do no work, so its
			// future would be ready and its value would be 0.
			if (self->m_lazyClearActor.isReady() && self->m_lazyClearActor.get() == 0) {
				break;
			}
		}

		// The lazy delete queue should now be empty and contain only the new page to start writing to
		// on the next commit.
		LazyClearQueueT::QueueState s = self->m_lazyClearQueue.getState();
		ASSERT(s.numEntries == 0);
		ASSERT(s.numPages == 1);

		// The btree should now be a single non-oversized root page.
		ASSERT(self->m_pHeader->height == 1);
		ASSERT(self->m_pHeader->root.count == 1);

		// Let pager do more commits to finish all cleanup of old pages
		wait(self->m_pager->clearRemapQueue());

		TraceEvent e("RedwoodDestructiveSanityCheck");
		self->toTraceEvent(e);
		e.log();

		// From the pager's perspective the only pages that should be in use are the btree root and
		// the previously mentioned lazy delete queue page.
		int64_t userPageCount = wait(self->m_pager->getUserPageCount());
		debug_printf("clearAllAndCheckSanity: userPageCount: %d\n", userPageCount);
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

		inline RedwoodRecordRef toRecord(KeyRef userKey) const {
			// No point in serializing an atomic op, it needs to be coalesced to a real value.
			ASSERT(!isAtomicOp());

			if (isClear())
				return RedwoodRecordRef(userKey);

			return RedwoodRecordRef(userKey, value);
		}

		std::string toString() const { return format("op=%d val='%s'", op, printable(value).c_str()); }
	};

	struct RangeMutation {
		RangeMutation() : boundaryChanged(false), clearAfterBoundary(false) {}

		bool boundaryChanged;
		Optional<ValueRef> boundaryValue; // Not present means cleared
		bool clearAfterBoundary;

		bool boundaryCleared() const { return boundaryChanged && !boundaryValue.present(); }
		bool boundarySet() const { return boundaryChanged && boundaryValue.present(); }

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

	// Counter to update with DecodeCache memory usage
	int64_t* m_pDecodeCacheMemory = nullptr;

	// The mutation buffer currently being written to
	std::unique_ptr<MutationBuffer> m_pBuffer;
	int64_t m_mutationCount;
	DecodeBoundaryVerifier* m_pBoundaryVerifier;

	struct CommitBatch {
		Version readVersion;
		Version writeVersion;
		std::unique_ptr<MutationBuffer> mutations;
		int64_t mutationCount;
		Reference<IPagerSnapshot> snapshot;
	};

	Version m_newOldestVersion;
	Future<Void> m_latestCommit;
	Future<Void> m_init;
	std::string m_name;
	int m_blockSize;
	ParentInfoMapT childUpdateTracker;

	// MetaKey has a variable size, it can be as large as m_headerSpace
	MetaKey* m_pHeader;
	int m_headerSpace;

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
		bool largeDeltaTree; // Whether or not the tree in the generated page is in the 'large' size range
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
	                                           unsigned int height,
	                                           int blockSize) {
		debug_printf("splitPages height=%d records=%d\n\tlowerBound=%s\n\tupperBound=%s\n",
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

	void makeEmptyRoot(Reference<ArenaPage> page) {
		BTreePage* btpage = (BTreePage*)page->begin();
		btpage->height = 1;
		btpage->kvBytes = 0;
		btpage->tree()->build(page->size(), nullptr, nullptr, nullptr, nullptr);
	}

	// Writes entries to 1 or more pages and return a vector of boundary keys with their ArenaPage(s)
	ACTOR static Future<Standalone<VectorRef<RedwoodRecordRef>>> writePages(VersionedBTree* self,
	                                                                        const RedwoodRecordRef* lowerBound,
	                                                                        const RedwoodRecordRef* upperBound,
	                                                                        VectorRef<RedwoodRecordRef> entries,
	                                                                        unsigned int height,
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
		state int sinceYield = 0;

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

			state Reference<ArenaPage> pages;
			BTreePage* btPage;

			if (p.blockCount == 1) {
				Reference<ArenaPage> page = self->m_pager->newPageBuffer();
				btPage = (BTreePage*)page->mutate();
				pages = std::move(page);
			} else {
				ASSERT(p.blockCount > 1);
				btPage = (BTreePage*)new uint8_t[p.pageSize];
			}

			btPage->height = height;
			btPage->kvBytes = p.kvBytes;
			g_redwoodMetrics.kvSizeWritten->sample(p.kvBytes);

			debug_printf("Building tree for %s\nlower: %s\nupper: %s\n",
			             p.toString().c_str(),
			             pageLowerBound.toString(false).c_str(),
			             pageUpperBound.toString(false).c_str());

			int deltaTreeSpace = p.pageSize - sizeof(BTreePage);
			state int written = btPage->tree()->build(
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
			auto& metrics = g_redwoodMetrics.level(height);
			metrics.metrics.pageBuild += 1;
			metrics.metrics.pageBuildExt += p.blockCount - 1;

			metrics.buildFillPctSketch->samplePercentage(p.usedFraction());
			metrics.buildStoredPctSketch->samplePercentage(p.kvFraction());
			metrics.buildItemCountSketch->sampleRecordCounter(p.count);

			// Create chunked pages
			// TODO: Avoid copying page bytes, but this is not trivial due to how pager checksums are currently handled.
			if (p.blockCount != 1) {
				// Mark the slack in the page buffer as defined
				VALGRIND_MAKE_MEM_DEFINED(((uint8_t*)btPage) + written, (p.blockCount * p.blockSize) - written);
				Reference<ArenaPage> page = self->m_pager->newPageBuffer(p.blockCount);
				const uint8_t* rptr = (const uint8_t*)btPage;
				for (int b = 0; b < p.blockCount; ++b) {
					memcpy(page->mutate() + b * p.blockSize, rptr, p.blockSize);
					rptr += p.blockSize;
				}
				pages = std::move(page);
				delete[](uint8_t*) btPage;
			}

			// Write this btree page, which is made of 1 or more pager pages.
			state BTreePageIDRef childPageID;

			// If we are only writing 1 BTree node and its block count is 1 and the original node also had 1 block
			// then try to update the page atomically so its logical page ID does not change
			if (pagesToBuild.size() == 1 && p.blockCount == 1 && previousID.size() == 1) {
				LogicalPageID id = wait(
				    self->m_pager->atomicUpdatePage(PagerEventReasons::Commit, height, previousID.front(), pages, v));
				childPageID.push_back(records.arena(), id);
			} else {
				// Either the original node is being split, or it's not but it has changed BTreePageID size or
				// it is now a multi-page node (and maybe was before as well)
				// Either way, there is no point in reusing any of the original page IDs because either
				// the parent must be rewritten anyway to account for the change in child link count or size,
				// or the parent would be rewritten because it the same cost or cheaper than incurring the second
				// write of 2 or more pages of a multi-page node.

				// Free the old IDs, but only once (before the first output record is added).
				if (records.empty()) {
					self->freeBTreePage(height, previousID, v);
				}

				state Standalone<VectorRef<LogicalPageID>> emptyPages;
				emptyPages.resize(emptyPages.arena(), p.blockCount);
				state int i = 0;
				for (i = 0; i < emptyPages.size(); ++i) {
					LogicalPageID id = wait(self->m_pager->newPageID());
					emptyPages[i] = id;
				}
				debug_printf("writePages: newPages %s", toString(emptyPages).c_str());
				self->m_pager->updatePage(PagerEventReasons::Commit, height, emptyPages, pages);
				for (const LogicalPageID id : emptyPages) {
					childPageID.push_back(records.arena(), id);
				}
			}

			if (self->m_pBoundaryVerifier != nullptr) {
				self->m_pBoundaryVerifier->update(childPageID, v, pageLowerBound.key, pageUpperBound.key);
			}

			if (++sinceYield > 100) {
				sinceYield = 0;
				wait(yield());
			}

			if (REDWOOD_DEBUG) {
				auto& p = pagesToBuild[pageIndex];
				debug_printf("Wrote %s %s original=%s deltaTreeSize=%d for %s\nlower: %s\nupper: %s\n",
				             toString(v).c_str(),
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

	ACTOR static Future<Standalone<VectorRef<RedwoodRecordRef>>> buildNewRoot(
	    VersionedBTree* self,
	    Version version,
	    Standalone<VectorRef<RedwoodRecordRef>> records,
	    unsigned int height) {
		debug_printf("buildNewRoot start version %" PRId64 ", %lu records\n", version, records.size());

		// While there are multiple child pages for this version we must write new tree levels.
		while (records.size() > 1) {
			self->m_pHeader->height = ++height;
			ASSERT(height < std::numeric_limits<int8_t>::max());
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

	ACTOR static Future<Reference<const ArenaPage>> readPage(PagerEventReasons reason,
	                                                         unsigned int level,
	                                                         IPagerSnapshot* snapshot,
	                                                         BTreePageIDRef id,
	                                                         int priority,
	                                                         bool forLazyClear,
	                                                         bool cacheable) {

		debug_printf("readPage() op=read%s %s @%" PRId64 "\n",
		             forLazyClear ? "ForDeferredClear" : "",
		             toString(id).c_str(),
		             snapshot->getVersion());

		state Reference<const ArenaPage> page;
		if (id.size() == 1) {
			Reference<const ArenaPage> p =
			    wait(snapshot->getPhysicalPage(reason, level, id.front(), priority, cacheable, false));
			page = std::move(p);
		} else {
			ASSERT(!id.empty());
			Reference<const ArenaPage> p =
			    wait(snapshot->getMultiPhysicalPage(reason, level, id, priority, cacheable, false));
			page = std::move(p);
		}
		debug_printf("readPage() op=readComplete %s @%" PRId64 " \n", toString(id).c_str(), snapshot->getVersion());
		const BTreePage* btPage = (const BTreePage*)page->begin();
		auto& metrics = g_redwoodMetrics.level(btPage->height).metrics;
		metrics.pageRead += 1;
		metrics.pageReadExt += (id.size() - 1);

		return std::move(page);
	}

	// Get cursor into a BTree node, creating decode cache from boundaries if needed
	inline BTreePage::BinaryTree::Cursor getCursor(const ArenaPage* page,
	                                               const RedwoodRecordRef& lowerBound,
	                                               const RedwoodRecordRef& upperBound) {
		if (page->userData == nullptr) {
			debug_printf("Creating DecodeCache for ptr=%p lower=%s upper=%s\n",
			             page->begin(),
			             lowerBound.toString(false).c_str(),
			             upperBound.toString(false).c_str());

			BTreePage::BinaryTree::DecodeCache* cache =
			    new BTreePage::BinaryTree::DecodeCache(lowerBound, upperBound, m_pDecodeCacheMemory);
			page->userData = cache;
			page->userDataDestructor = [](void* cache) { ((BTreePage::BinaryTree::DecodeCache*)cache)->delref(); };
		}

		return BTreePage::BinaryTree::Cursor((BTreePage::BinaryTree::DecodeCache*)page->userData,
		                                     ((BTreePage*)page->begin())->tree());
	}

	// Get cursor into a BTree node from a child link
	inline BTreePage::BinaryTree::Cursor getCursor(const ArenaPage* page, const BTreePage::BinaryTree::Cursor& link) {
		if (page->userData == nullptr) {
			return getCursor(page, link.get(), link.next().getOrUpperBound());
		}

		return BTreePage::BinaryTree::Cursor((BTreePage::BinaryTree::DecodeCache*)page->userData,
		                                     ((BTreePage*)page->begin())->tree());
	}

	static void preLoadPage(IPagerSnapshot* snapshot, BTreePageIDRef pageIDs, int priority) {
		g_redwoodMetrics.metric.btreeLeafPreload += 1;
		g_redwoodMetrics.metric.btreeLeafPreloadExt += (pageIDs.size() - 1);
		if (pageIDs.size() == 1) {
			snapshot->getPhysicalPage(
			    PagerEventReasons::RangePrefetch, nonBtreeLevel, pageIDs.front(), priority, true, true);
		} else {
			snapshot->getMultiPhysicalPage(
			    PagerEventReasons::RangePrefetch, nonBtreeLevel, pageIDs, priority, true, true);
		}
	}

	void freeBTreePage(int height, BTreePageIDRef btPageID, Version v) {
		// Free individual pages at v
		for (LogicalPageID id : btPageID) {
			m_pager->freePage(id, v);
		}

		// Stop tracking child updates for deleted internal nodes
		if (height > 1 && !btPageID.empty()) {
			childUpdateTracker.erase(btPageID.front());
		}
	}

	// Write new version of pageID at version v using page as its data.
	// If oldID size is 1, attempts to keep logical page ID via an atomic page update.
	// Returns resulting BTreePageID which might be the same as the input
	// updateBTreePage is only called from commitSubTree function so write reason is always btree commit
	ACTOR static Future<BTreePageIDRef> updateBTreePage(VersionedBTree* self,
	                                                    BTreePageIDRef oldID,
	                                                    Arena* arena,
	                                                    Reference<ArenaPage> page,
	                                                    Version writeVersion) {
		state BTreePageIDRef newID;
		newID.resize(*arena, oldID.size());

		if (REDWOOD_DEBUG) {
			BTreePage* btPage = (BTreePage*)page->begin();
			BTreePage::BinaryTree::DecodeCache* cache = (BTreePage::BinaryTree::DecodeCache*)page->userData;
			debug_printf_always(
			    "updateBTreePage(%s, %s) %s\n",
			    ::toString(oldID).c_str(),
			    ::toString(writeVersion).c_str(),
			    cache == nullptr
			        ? "<noDecodeCache>"
			        : btPage->toString(true, oldID, writeVersion, cache->lowerBound, cache->upperBound).c_str());
		}

		state unsigned int height = (unsigned int)((BTreePage*)page->begin())->height;
		if (oldID.size() == 1) {
			LogicalPageID id = wait(
			    self->m_pager->atomicUpdatePage(PagerEventReasons::Commit, height, oldID.front(), page, writeVersion));
			newID.front() = id;
			return newID;
		}
		state Standalone<VectorRef<LogicalPageID>> emptyPages;
		state int i = 0;
		emptyPages.resize(emptyPages.arena(), oldID.size());
		for (i = 0; i < oldID.size(); ++i) {
			LogicalPageID id = wait(self->m_pager->newPageID());
			emptyPages[i] = id;
		}
		debug_printf("updateBTreePage: newPages %s", toString(emptyPages).c_str());
		self->m_pager->updatePage(PagerEventReasons::Commit, height, emptyPages, page);
		i = 0;
		for (const LogicalPageID id : emptyPages) {
			newID[i] = id;
			++i;
		}

		if (self->m_pBoundaryVerifier != nullptr) {
			self->m_pBoundaryVerifier->update(writeVersion, oldID.front(), newID.front());
		}

		self->freeBTreePage(height, oldID, writeVersion);
		return newID;
	}

	// Copy page to a new page which shares the same DecodeCache with the old page
	static Reference<ArenaPage> clonePageForUpdate(Reference<const ArenaPage> page) {
		Reference<ArenaPage> newPage = page->cloneContents();

		BTreePage::BinaryTree::DecodeCache* cache = (BTreePage::BinaryTree::DecodeCache*)page->userData;
		cache->addref();
		newPage->userData = cache;
		newPage->userDataDestructor = [](void* cache) { ((BTreePage::BinaryTree::DecodeCache*)cache)->delref(); };

		debug_printf("cloneForUpdate(%p -> %p  size=%d\n", page->begin(), newPage->begin(), page->size());
		return newPage;
	}

	// Each call to commitSubtree() will pass most of its arguments via a this structure because the caller
	// will need access to these parameters after commitSubtree() is done.
	struct InternalPageSliceUpdate : public FastAllocated<InternalPageSliceUpdate> {
		// The logical range for the subtree's contents.  Due to subtree clears, these boundaries may not match
		// the lower/upper bounds needed to decode the page.
		// Subtree clears can cause the boundaries for decoding the page to be more restrictive than the subtree's
		// logical boundaries.  When a subtree is fully cleared, the link to it is replaced with a null link, but
		// the key boundary remains in tact to support decoding of the previous subtree.
		RedwoodRecordRef subtreeLowerBound;
		RedwoodRecordRef subtreeUpperBound;

		// The lower/upper bound for decoding the root of the subtree
		RedwoodRecordRef decodeLowerBound;
		RedwoodRecordRef decodeUpperBound;

		bool boundariesNormal() const {
			// If the decode upper boundary is the subtree upper boundary the pointers will be the same
			// For the lower boundary, if the pointers are not the same there is still a possibility
			// that the keys are the same.  This happens for the first remaining subtree of an internal page
			// after the prior subtree(s) were cleared.
			return (decodeUpperBound == subtreeUpperBound) &&
			       (decodeLowerBound == subtreeLowerBound || decodeLowerBound.sameExceptValue(subtreeLowerBound));
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
		Optional<RedwoodRecordRef> expectedUpperBound;

		bool inPlaceUpdate;

		// CommitSubtree will call one of the following three functions based on its exit path

		// Subtree was cleared.
		void cleared() {
			inPlaceUpdate = false;
			childrenChanged = true;
			expectedUpperBound.reset();
		}

		// Page was updated in-place through edits and written to maybeNewID
		void updatedInPlace(BTreePageIDRef maybeNewID, BTreePage* btPage, int capacity) {
			inPlaceUpdate = true;

			auto& metrics = g_redwoodMetrics.level(btPage->height);
			metrics.metrics.pageModify += 1;
			metrics.metrics.pageModifyExt += (maybeNewID.size() - 1);

			metrics.modifyFillPctSketch->samplePercentage((double)btPage->size() / capacity);
			metrics.modifyStoredPctSketch->samplePercentage((double)btPage->kvBytes / capacity);
			metrics.modifyItemCountSketch->sampleRecordCounter(btPage->tree()->numItems);

			g_redwoodMetrics.kvSizeWritten->sample(btPage->kvBytes);

			// The boundaries can't have changed, but the child page link may have.
			if (maybeNewID != decodeLowerBound.getChildPage()) {
				// Add page's decode lower bound to newLinks set without its child page, intially
				newLinks.push_back_deep(newLinks.arena(), decodeLowerBound.withoutValue());

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
			expectedUpperBound = newLinks.back().value.present() ? subtreeUpperBound : Optional<RedwoodRecordRef>();
		}

		// Get the first record for this range AFTER applying whatever changes were made
		const RedwoodRecordRef* getFirstBoundary() const {
			if (childrenChanged) {
				if (newLinks.empty()) {
					return nullptr;
				}
				return &newLinks.front();
			}
			return &decodeLowerBound;
		}

		std::string toString() const {
			std::string s;
			s += format("SubtreeSlice: addr=%p skipLen=%d subtreeCleared=%d childrenChanged=%d inPlaceUpdate=%d\n",
			            this,
			            skipLen,
			            childrenChanged && newLinks.empty(),
			            childrenChanged,
			            inPlaceUpdate);
			s += format("SubtreeLower: %s\n", subtreeLowerBound.toString(false).c_str());
			s += format(" DecodeLower: %s\n", decodeLowerBound.toString(false).c_str());
			s += format(" DecodeUpper: %s\n", decodeUpperBound.toString(false).c_str());
			s += format("SubtreeUpper: %s\n", subtreeUpperBound.toString(false).c_str());
			s += format("expectedUpperBound: %s\n",
			            expectedUpperBound.present() ? expectedUpperBound.get().toString(false).c_str() : "(null)");
			for (int i = 0; i < newLinks.size(); ++i) {
				s += format("  %i: %s\n", i, newLinks[i].toString(false).c_str());
			}
			s.resize(s.size() - 1);
			return s;
		}
	};

	struct InternalPageModifier {
		InternalPageModifier() {}
		InternalPageModifier(Reference<const ArenaPage> p, bool alreadyCloned, bool updating, ParentInfo* parentInfo)
		  : updating(updating), page(p), clonedPage(alreadyCloned), changesMade(false), parentInfo(parentInfo) {}

		// Whether updating the existing page is allowed
		bool updating;
		Reference<const ArenaPage> page;

		// Whether or not page has been cloned for update
		bool clonedPage;

		Standalone<VectorRef<RedwoodRecordRef>> rebuild;

		// Whether there are any changes to the page, either made in place or staged in rebuild
		bool changesMade;
		ParentInfo* parentInfo;

		BTreePage* btPage() const { return (BTreePage*)page->begin(); }

		bool empty() const {
			if (updating) {
				return btPage()->tree()->numItems == 0;
			} else {
				return rebuild.empty();
			}
		}

		void cloneForUpdate() {
			if (!clonedPage) {
				page = clonePageForUpdate(page);
				clonedPage = true;
			}
		}

		// end is the cursor position of the first record of the unvisited child link range, which
		// is needed if the insert requires switching from update to rebuild mode.
		void insert(BTreePage::BinaryTree::Cursor end, const VectorRef<RedwoodRecordRef>& recs) {
			int i = 0;
			if (updating) {
				// Update must be done in the new tree, not the original tree where the end cursor will be from
				end.tree = btPage()->tree();

				// TODO: insert recs in a random order to avoid new subtree being entirely right child links
				while (i != recs.size()) {
					const RedwoodRecordRef& rec = recs[i];
					debug_printf("internal page (updating) insert: %s\n", rec.toString(false).c_str());

					if (!end.insert(rec)) {
						debug_printf("internal page: failed to insert %s, switching to rebuild\n",
						             rec.toString(false).c_str());

						// Update failed, so populate rebuild vector with everything up to but not including end, which
						// may include items from recs that were already added.
						auto c = end;
						if (c.moveFirst()) {
							rebuild.reserve(rebuild.arena(), c.tree->numItems);
							while (c != end) {
								debug_printf("  internal page rebuild: add %s\n", c.get().toString(false).c_str());
								rebuild.push_back(rebuild.arena(), c.get());
								c.moveNext();
							}
						}
						updating = false;
						break;
					}
					btPage()->kvBytes += rec.kvBytes();
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

					if (c != u.cEnd) {
						cloneForUpdate();
						// must point c to the tree to erase from
						c.tree = btPage()->tree();
					}

					while (c != u.cEnd) {
						debug_printf("internal page (updating) erasing: %s\n", c.get().toString(false).c_str());
						btPage()->kvBytes -= c.get().kvBytes();
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

				// If this was an in-place update, where the child page IDs do not change, notify the
				// parentInfo that those pages have been updated so it can possibly eliminate their
				// second writes later.
				if (u.inPlaceUpdate) {
					for (auto id : u.decodeLowerBound.getChildPage()) {
						parentInfo->pageUpdated(id);
					}
				}

				keep(u.cBegin, u.cEnd);
			}

			// If there is an expected upper boundary for the next range after u
			if (u.expectedUpperBound.present()) {
				// Then if it does not match the next boundary then insert a dummy record
				if (nextBoundary == nullptr || (nextBoundary != &u.expectedUpperBound.get() &&
				                                !nextBoundary->sameExceptValue(u.expectedUpperBound.get()))) {
					RedwoodRecordRef rec = u.expectedUpperBound.get().withoutValue();
					debug_printf("applyUpdate adding dummy record %s\n", rec.toString(false).c_str());

					cloneForUpdate();
					insert(u.cEnd, { &rec, 1 });
					changesMade = true;
				}
			}
		}
	};

	ACTOR static Future<Void> commitSubtree(
	    VersionedBTree* self,
	    CommitBatch* batch,
	    BTreePageIDRef rootID,
	    unsigned int height,
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

		state Reference<const ArenaPage> page =
		    wait(readPage(PagerEventReasons::Commit, height, batch->snapshot.getPtr(), rootID, height, false, true));

		// If the page exists in the cache, it must be copied before modification.
		// That copy will be referenced by pageCopy, as page must stay in scope in case anything references its
		// memory and it gets evicted from the cache.
		// If the page is not in the cache, then no copy is needed so we will initialize pageCopy to page
		state Reference<const ArenaPage> pageCopy;

		state BTreePage* btPage = (BTreePage*)page->begin();
		ASSERT(height == btPage->height);
		++g_redwoodMetrics.level(height).metrics.pageCommitStart;

		// TODO:  Decide if it is okay to update if the subtree boundaries are expanded.  It can result in
		// records in a DeltaTree being outside its decode boundary range, which isn't actually invalid
		// though it is awkward to reason about.
		// TryToUpdate indicates insert and erase operations should be tried on the existing page first
		state bool tryToUpdate = btPage->tree()->numItems > 0 && update->boundariesNormal();

		debug_printf(
		    "%s commitSubtree(): %s\n",
		    context.c_str(),
		    btPage
		        ->toString(
		            false, rootID, batch->snapshot->getVersion(), update->decodeLowerBound, update->decodeUpperBound)
		        .c_str());

		state BTreePage::BinaryTree::Cursor cursor = update->cBegin.valid()
		                                                 ? self->getCursor(page.getPtr(), update->cBegin)
		                                                 : self->getCursor(page.getPtr(), dbBegin, dbEnd);

		if (self->m_pBoundaryVerifier != nullptr) {
			if (update->cBegin.valid()) {
				ASSERT(self->m_pBoundaryVerifier->verify(rootID.front(),
				                                         batch->snapshot->getVersion(),
				                                         update->cBegin.get().key,
				                                         update->cBegin.next().getOrUpperBound().key));
			}
		}

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
		if (btPage->isLeaf()) {
			// When true, we are modifying the existing DeltaTree
			// When false, we are accumulating retained and added records in merged vector to build pages from them.
			bool updatingDeltaTree = tryToUpdate;
			bool changesMade = false;

			// Copy page for modification if not already copied
			auto copyForUpdate = [&]() {
				if (!pageCopy.isValid()) {
					pageCopy = clonePageForUpdate(page);
					btPage = (BTreePage*)pageCopy->begin();
					cursor.switchTree(btPage->tree());
				}
			};

			state Standalone<VectorRef<RedwoodRecordRef>> merged;

			// The first mutation buffer boundary has a key <= the first key in the page.

			cursor.moveFirst();
			debug_printf("%s Leaf page, applying changes.\n", context.c_str());

			// Now, process each mutation range and merge changes with existing data.
			bool firstMutationBoundary = true;
			constexpr int maxHeightAllowed = 8;

			while (mBegin != mEnd) {
				// Apply the change to the mutation buffer start boundary key only if
				//   - there actually is a change (clear or set to new value)
				//   - either this is not the first boundary or it is but its key matches our lower bound key
				bool applyBoundaryChange = mBegin.mutation().boundaryChanged &&
				                           (!firstMutationBoundary || mBegin.key() == update->subtreeLowerBound.key);
				bool boundaryExists = cursor.valid() && cursor.get().key == mBegin.key();

				debug_printf("%s New mutation boundary: '%s': %s  applyBoundaryChange=%d  boundaryExists=%d "
				             "updatingDeltaTree=%d\n",
				             context.c_str(),
				             printable(mBegin.key()).c_str(),
				             mBegin.mutation().toString().c_str(),
				             applyBoundaryChange,
				             boundaryExists,
				             updatingDeltaTree);

				firstMutationBoundary = false;

				if (applyBoundaryChange) {
					// If the boundary is being set to a value, the new KV record will be inserted
					bool shouldInsertBoundary = mBegin.mutation().boundarySet();

					// Optimization:  In-place value update of new same-sized value
					// If the boundary exists in the page and we're in update mode and the boundary is being set to a
					// new value of the same length as the old value then just update the value bytes.
					if (boundaryExists && updatingDeltaTree && shouldInsertBoundary &&
					    mBegin.mutation().boundaryValue.get().size() == cursor.get().value.get().size()) {
						changesMade = true;
						shouldInsertBoundary = false;

						debug_printf("%s In-place value update for %s [existing, boundary start]\n",
						             context.c_str(),
						             cursor.get().toString().c_str());

						copyForUpdate();
						memcpy((uint8_t*)cursor.get().value.get().begin(),
						       mBegin.mutation().boundaryValue.get().begin(),
						       cursor.get().value.get().size());
						cursor.moveNext();
					} else if (boundaryExists) {
						// An in place update can't be done, so if the boundary exists then erase or skip the record
						changesMade = true;

						// If updating, erase from the page, otherwise do not add to the output set
						if (updatingDeltaTree) {
							debug_printf("%s Erasing %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());

							copyForUpdate();
							btPage->kvBytes -= cursor.get().kvBytes();
							cursor.erase();
						} else {
							debug_printf("%s Skipped %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());
							cursor.moveNext();
						}
					}

					// If the boundary value is being set and we must insert it, add it to the page or the output set
					if (shouldInsertBoundary) {
						RedwoodRecordRef rec(mBegin.key(), mBegin.mutation().boundaryValue.get());
						changesMade = true;

						// If updating, first try to add the record to the page
						if (updatingDeltaTree) {
							copyForUpdate();
							if (cursor.insert(rec, update->skipLen, maxHeightAllowed)) {
								btPage->kvBytes += rec.kvBytes();
								debug_printf("%s Inserted %s [mutation, boundary start]\n",
								             context.c_str(),
								             rec.toString().c_str());
							} else {
								debug_printf("%s Insert failed for %s [mutation, boundary start]\n",
								             context.c_str(),
								             rec.toString().c_str());

								// Since the insert failed we must switch to a linear merge of existing data and
								// mutations, accumulating the new record set in the merge vector and build new pages
								// from it. First, we must populate the merged vector with all the records up to but not
								// including the current mutation boundary key.
								auto c = cursor;
								c.moveFirst();
								while (c != cursor) {
									debug_printf(
									    "%s catch-up adding %s\n", context.c_str(), c.get().toString().c_str());
									merged.push_back(merged.arena(), c.get());
									c.moveNext();
								}
								updatingDeltaTree = false;
							}
						}

						// If not updating, possibly due to insert failure above, then add record to the output set
						if (!updatingDeltaTree) {
							merged.push_back(merged.arena(), rec);
							debug_printf(
							    "%s Added %s [mutation, boundary start]\n", context.c_str(), rec.toString().c_str());
						}
					}
				} else if (boundaryExists) {
					// If the boundary exists in the page but there is no pending change,
					// then if updating move past it, otherwise add it to the output set.
					if (updatingDeltaTree) {
						cursor.moveNext();
					} else {
						merged.push_back(merged.arena(), cursor.get());
						debug_printf("%s Added %s [existing, boundary start]\n",
						             context.c_str(),
						             cursor.get().toString().c_str());
						cursor.moveNext();
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
				if (remove != updatingDeltaTree) {
					// If not updating, then the records, if any exist, are being removed.  We don't know if there
					// actually are any but we must assume there are.
					if (!updatingDeltaTree) {
						changesMade = true;
					}

					debug_printf("%s Seeking forward to next boundary (remove=%d updating=%d) %s\n",
					             context.c_str(),
					             remove,
					             updatingDeltaTree,
					             mBegin.key().toString().c_str());
					cursor.seekGreaterThanOrEqual(end, update->skipLen);
				} else {
					// Otherwise we must visit the records.  If updating, the visit is to erase them, and if doing a
					// linear merge than the visit is to add them to the output set.
					while (cursor.valid() && cursor.get().compare(end, update->skipLen) < 0) {
						if (updatingDeltaTree) {
							debug_printf("%s Erasing %s [existing, boundary start]\n",
							             context.c_str(),
							             cursor.get().toString().c_str());

							copyForUpdate();
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
				if (remove != updatingDeltaTree) {
					debug_printf("%s Ignoring remaining records, remove=%d updating=%d\n",
					             context.c_str(),
					             remove,
					             updatingDeltaTree);
				} else {
					// If updating and the key is changing, we must visit the records to erase them.
					// If not updating and the key is not changing, we must visit the records to add them to the output
					// set.
					while (cursor.valid()) {
						if (updatingDeltaTree) {
							debug_printf(
							    "%s Erasing %s and beyond [existing, matches changed upper mutation boundary]\n",
							    context.c_str(),
							    cursor.get().toString().c_str());

							copyForUpdate();
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

			if (updatingDeltaTree) {
				// If the tree is now empty, delete the page
				if (cursor.tree->numItems == 0) {
					update->cleared();
					self->freeBTreePage(height, rootID, batch->writeVersion);
					debug_printf("%s Page updates cleared all entries, returning %s\n",
					             context.c_str(),
					             toString(*update).c_str());
				} else {
					// Otherwise update it.
					BTreePageIDRef newID = wait(self->updateBTreePage(
					    self, rootID, &update->newLinks.arena(), pageCopy.castTo<ArenaPage>(), batch->writeVersion));

					update->updatedInPlace(newID, btPage, newID.size() * self->m_blockSize);
					debug_printf(
					    "%s Page updated in-place, returning %s\n", context.c_str(), toString(*update).c_str());
				}
				return Void();
			}

			// If everything in the page was deleted then this page should be deleted as of the new version
			if (merged.empty()) {
				update->cleared();
				self->freeBTreePage(height, rootID, batch->writeVersion);

				debug_printf("%s All leaf page contents were cleared, returning %s\n",
				             context.c_str(),
				             toString(*update).c_str());
				return Void();
			}

			// Rebuild new page(s).
			state Standalone<VectorRef<RedwoodRecordRef>> entries = wait(writePages(self,
			                                                                        &update->subtreeLowerBound,
			                                                                        &update->subtreeUpperBound,
			                                                                        merged,
			                                                                        height,
			                                                                        batch->writeVersion,
			                                                                        rootID));

			// Put new links into update and tell update that pages were rebuilt
			update->rebuilt(entries);

			debug_printf("%s Merge complete, returning %s\n", context.c_str(), toString(*update).c_str());
			return Void();
		} else {
			// Internal Page
			std::vector<Future<Void>> recursions;
			state std::vector<std::unique_ptr<InternalPageSliceUpdate>> slices;

			cursor.moveFirst();

			bool first = true;

			while (cursor.valid()) {
				slices.emplace_back(new InternalPageSliceUpdate());
				InternalPageSliceUpdate& u = *slices.back();

				// At this point we should never be at a null child page entry because the first entry of a page
				// can't be null and this loop will skip over null entries that come after non-null entries.
				ASSERT(cursor.get().value.present());

				// Subtree lower boundary is this page's subtree lower bound or cursor
				u.cBegin = cursor;
				u.decodeLowerBound = cursor.get();
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
					if (mBegin.key() != u.subtreeLowerBound.key) {
						--mBegin;
					}
				}

				BTreePageIDRef pageID = cursor.get().getChildPage();
				ASSERT(!pageID.empty());

				// The decode upper bound is always the next key after the child link, or the decode upper bound for
				// this page
				if (cursor.moveNext()) {
					u.decodeUpperBound = cursor.get();
					// If cursor record has a null child page then it exists only to preserve a previous
					// subtree boundary that is now needed for reading the subtree at cBegin.
					if (!cursor.get().value.present()) {
						// If the upper bound is provided by a dummy record in [cBegin, cEnd) then there is no
						// requirement on the next subtree range or the parent page to have a specific upper boundary
						// for decoding the subtree.
						u.expectedUpperBound.reset();
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
				u.subtreeUpperBound = cursor.valid() ? cursor.get() : update->subtreeUpperBound;
				u.cEnd = cursor;
				u.skipLen = 0; // TODO: set this

				// Find the mutation buffer range that includes all changes to the range described by u
				mEnd = batch->mutations->lower_bound(u.subtreeUpperBound.key);

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
						uniform = range.boundaryCleared() || mutationBoundaryKey != u.subtreeLowerBound.key;
					} else {
						// If the mutation range after the boundary key is unchanged, then the mutation boundary key
						// must be also unchanged or must be different than the subtree lower bound key so that it
						// doesn't matter
						uniform = !range.boundaryChanged || mutationBoundaryKey != u.subtreeLowerBound.key;
					}

					// If u's subtree is either all cleared or all unchanged
					if (uniform) {
						// We do not need to recurse to this subtree.  Next, let's see if we can embiggen u's range to
						// include sibling subtrees also covered by (mBegin, mEnd) so we can not recurse to those, too.
						// If the cursor is valid, u.subtreeUpperBound is the cursor's position, which is >= mEnd.key().
						// If equal, no range expansion is possible.
						if (cursor.valid() && mEnd.key() != u.subtreeUpperBound.key) {
							// TODO:  If cursor hints are available, use (cursor, 1)
							cursor.seekLessThanOrEqual(mEnd.key(), update->skipLen);

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
								u.subtreeUpperBound = cursor.get();
								u.skipLen = 0; // TODO: set this

								// The new decode upper bound is either cEnd or the record before it if it has no child
								// link
								auto c = u.cEnd;
								c.movePrev();
								ASSERT(c.valid());
								if (!c.get().value.present()) {
									u.decodeUpperBound = c.get();
									u.expectedUpperBound.reset();
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
								RedwoodRecordRef rec = c.get();
								if (rec.value.present()) {
									if (height == 2) {
										debug_printf("%s: freeing child page in cleared subtree range: %s\n",
										             context.c_str(),
										             ::toString(rec.getChildPage()).c_str());
										self->freeBTreePage(height, rec.getChildPage(), batch->writeVersion);
									} else {
										debug_printf("%s: queuing subtree deletion cleared subtree range: %s\n",
										             context.c_str(),
										             ::toString(rec.getChildPage()).c_str());
										self->m_lazyClearQueue.pushBack(LazyClearQueueEntry{
										    (uint8_t)(height - 1), batch->writeVersion, rec.getChildPage() });
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
				recursions.push_back(self->commitSubtree(self, batch, pageID, height - 1, mBegin, mEnd, &u));
			}

			debug_printf(
			    "%s Recursions from internal page started. pageSize=%d level=%d children=%d slices=%d recursions=%d\n",
			    context.c_str(),
			    btPage->size(),
			    btPage->height,
			    btPage->tree()->numItems,
			    slices.size(),
			    recursions.size());

			wait(waitForAll(recursions));
			debug_printf("%s Recursions done, processing slice updates.\n", context.c_str());

			// ParentInfo could be invalid after a wait and must be re-initialized.
			// All uses below occur before waits so no reinitialization is done.
			state ParentInfo* parentInfo = &self->childUpdateTracker[rootID.front()];

			// InternalPageModifier takes the results of the recursive commitSubtree() calls in order
			// and makes changes to page as needed, copying as needed, and generating an array from
			// which to build new page(s) if modification is not possible or not allowed.
			// If pageCopy is already set it was initialized to page above so the modifier doesn't need
			// to copy it
			state InternalPageModifier modifier(page, pageCopy.isValid(), tryToUpdate, parentInfo);

			// Apply the possible changes for each subtree range recursed to, except the last one.
			// For each range, the expected next record, if any, is checked against the first boundary
			// of the next range, if any.
			for (int i = 0, iEnd = slices.size() - 1; i < iEnd; ++i) {
				modifier.applyUpdate(*slices[i], slices[i + 1]->getFirstBoundary());
			}

			// The expected next record for the final range is checked against one of the upper boundaries passed to
			// this commitSubtree() instance.  If changes have already been made, then the subtree upper boundary is
			// passed, so in the event a different upper boundary is needed it will be added to the already-modified
			// page.  Otherwise, the decode boundary is used which will prevent this page from being modified for the
			// sole purpose of adding a dummy upper bound record.
			debug_printf("%s Applying final child range update. changesMade=%d  Parent update is: %s\n",
			             context.c_str(),
			             modifier.changesMade,
			             update->toString().c_str());
			modifier.applyUpdate(*slices.back(),
			                     modifier.changesMade ? &update->subtreeUpperBound : &update->decodeUpperBound);

			state bool detachChildren = (parentInfo->count > 2);
			state bool forceUpdate = false;

			// If no changes were made, but we should rewrite it to point directly to remapped child pages
			if (!modifier.changesMade && detachChildren) {
				debug_printf(
				    "%s Internal page forced rewrite because at least %d children have been updated in-place.\n",
				    context.c_str(),
				    parentInfo->count);

				forceUpdate = true;
				modifier.updating = true;

				// Make sure the modifier cloned the page so we can update the child links in-place below.
				modifier.cloneForUpdate();
				++g_redwoodMetrics.level(height).metrics.forceUpdate;
			}

			// If the modifier cloned the page for updating, then update our local pageCopy, btPage, and cursor
			if (modifier.clonedPage) {
				pageCopy = modifier.page;
				btPage = modifier.btPage();
				cursor.switchTree(btPage->tree());
			}

			// If page contents have changed
			if (modifier.changesMade || forceUpdate) {
				if (modifier.empty()) {
					update->cleared();
					debug_printf("%s All internal page children were deleted so deleting this page too, returning %s\n",
					             context.c_str(),
					             toString(*update).c_str());
					self->freeBTreePage(height, rootID, batch->writeVersion);
				} else {
					if (modifier.updating) {
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
							auto& stats = g_redwoodMetrics.level(height);
							while (cursor.valid()) {
								if (cursor.get().value.present()) {
									for (auto& p : cursor.get().getChildPage()) {
										if (parentInfo->maybeUpdated(p)) {
											LogicalPageID newID =
											    self->m_pager->detachRemappedPage(p, batch->writeVersion);
											if (newID != invalidLogicalPageID) {
												debug_printf("%s Detach updated %u -> %u\n", context.c_str(), p, newID);
												if (self->m_pBoundaryVerifier != nullptr) {
													self->m_pBoundaryVerifier->update(batch->writeVersion, p, newID);
												}
												p = newID;
												++stats.metrics.detachChild;
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

						BTreePageIDRef newID = wait(self->updateBTreePage(self,
						                                                  rootID,
						                                                  &update->newLinks.arena(),
						                                                  pageCopy.castTo<ArenaPage>(),
						                                                  batch->writeVersion));
						debug_printf(
						    "%s commitSubtree(): Internal page updated in-place at version %s, new contents: %s\n",
						    context.c_str(),
						    toString(batch->writeVersion).c_str(),
						    btPage
						        ->toString(false,
						                   newID,
						                   batch->snapshot->getVersion(),
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
							auto& stats = g_redwoodMetrics.level(height);
							for (auto& rec : modifier.rebuild) {
								if (rec.value.present()) {
									BTreePageIDRef oldPages = rec.getChildPage();
									BTreePageIDRef newPages;
									for (int i = 0; i < oldPages.size(); ++i) {
										LogicalPageID p = oldPages[i];
										if (parentInfo->maybeUpdated(p)) {
											LogicalPageID newID =
											    self->m_pager->detachRemappedPage(p, batch->writeVersion);
											if (newID != invalidLogicalPageID) {
												// Rebuild record values reference original page memory so make a copy
												if (newPages.empty()) {
													newPages = BTreePageIDRef(modifier.rebuild.arena(), oldPages);
													rec.setChildPage(newPages);
												}
												debug_printf("%s Detach updated %u -> %u\n", context.c_str(), p, newID);
												if (self->m_pBoundaryVerifier != nullptr) {
													self->m_pBoundaryVerifier->update(batch->writeVersion, p, newID);
												}
												newPages[i] = newID;
												++stats.metrics.detachChild;
											}
										}
									}
								}
							}
							parentInfo->clear();
						}

						Standalone<VectorRef<RedwoodRecordRef>> newChildEntries =
						    wait(writePages(self,
						                    &update->subtreeLowerBound,
						                    &update->subtreeUpperBound,
						                    modifier.rebuild,
						                    height,
						                    batch->writeVersion,
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

	ACTOR static Future<Void> commit_impl(VersionedBTree* self, Version writeVersion) {
		// Take ownership of the current mutation buffer and make a new one
		state CommitBatch batch;
		batch.mutations = std::move(self->m_pBuffer);
		self->m_pBuffer.reset(new MutationBuffer());
		batch.mutationCount = self->m_mutationCount;
		self->m_mutationCount = 0;

		batch.writeVersion = writeVersion;

		// Replace the lastCommit future with a new one and then wait on the old one
		state Promise<Void> committed;
		Future<Void> previousCommit = self->m_latestCommit;
		self->m_latestCommit = committed.getFuture();

		// Wait for the latest commit to be finished.
		wait(previousCommit);

		// If the write version has not advanced then there can be no changes pending.
		// If there are no changes, then the commit is a no-op.
		if (writeVersion == self->m_pager->getLastCommittedVersion()) {
			ASSERT(batch.mutationCount == 0);
			debug_printf("%s: Empty commit at repeat version %" PRId64 "\n", self->m_name.c_str(), batch.writeVersion);
			committed.send(Void());
			return Void();
		}

		// For this commit, use the latest snapshot that was just committed.
		batch.readVersion = self->m_pager->getLastCommittedVersion();

		self->m_pager->setOldestReadableVersion(self->m_newOldestVersion);
		debug_printf("%s: Beginning commit of version %" PRId64 ", read version %" PRId64
		             ", new oldest version set to %" PRId64 "\n",
		             self->m_name.c_str(),
		             batch.writeVersion,
		             batch.readVersion,
		             self->m_newOldestVersion);

		batch.snapshot = self->m_pager->getReadSnapshot(batch.readVersion);

		state Standalone<BTreePageIDRef> rootPageID = self->m_pHeader->root.get();
		state InternalPageSliceUpdate all;
		state RedwoodRecordRef rootLink = dbBegin.withPageID(rootPageID);
		all.subtreeLowerBound = rootLink;
		all.decodeLowerBound = rootLink;
		all.subtreeUpperBound = dbEnd;
		all.decodeUpperBound = dbEnd;
		all.skipLen = 0;

		MutationBuffer::const_iterator mBegin = batch.mutations->upper_bound(all.subtreeLowerBound.key);
		--mBegin;
		MutationBuffer::const_iterator mEnd = batch.mutations->lower_bound(all.subtreeUpperBound.key);

		wait(commitSubtree(self, &batch, rootPageID, self->m_pHeader->height, mBegin, mEnd, &all));

		// If the old root was deleted, write a new empty tree root node and free the old roots
		if (all.childrenChanged) {
			if (all.newLinks.empty()) {
				debug_printf("Writing new empty root.\n");
				LogicalPageID newRootID = wait(self->m_pager->newPageID());
				Reference<ArenaPage> page = self->m_pager->newPageBuffer();
				self->makeEmptyRoot(page);
				self->m_pHeader->height = 1;
				VectorRef<LogicalPageID> rootID((LogicalPageID*)&newRootID, 1);
				self->m_pager->updatePage(PagerEventReasons::Commit, self->m_pHeader->height, rootID, page);
				rootPageID = BTreePageIDRef((LogicalPageID*)&newRootID, 1);
			} else {
				Standalone<VectorRef<RedwoodRecordRef>> newRootRecords(all.newLinks, all.newLinks.arena());
				if (newRootRecords.size() == 1) {
					rootPageID = newRootRecords.front().getChildPage();
				} else {
					// If the new root level's size is not 1 then build new root level(s)
					Standalone<VectorRef<RedwoodRecordRef>> newRootPage =
					    wait(buildNewRoot(self, batch.writeVersion, newRootRecords, self->m_pHeader->height));
					rootPageID = newRootPage.front().getChildPage();
				}
			}
		}

		debug_printf("new root %s\n", toString(rootPageID).c_str());
		self->m_pHeader->root.set(rootPageID, self->m_headerSpace - sizeof(MetaKey));

		self->m_lazyClearStop = true;
		wait(success(self->m_lazyClearActor));
		debug_printf("Lazy delete freed %u pages\n", self->m_lazyClearActor.get());

		wait(self->m_lazyClearQueue.flush());
		self->m_pHeader->lazyDeleteQueue = self->m_lazyClearQueue.getState();

		debug_printf("Setting metakey\n");
		self->m_pager->setMetaKey(self->m_pHeader->asKeyRef());

		debug_printf("%s: Committing pager %" PRId64 "\n", self->m_name.c_str(), writeVersion);
		wait(self->m_pager->commit(writeVersion));
		debug_printf("%s: Committed version %" PRId64 "\n", self->m_name.c_str(), writeVersion);

		++g_redwoodMetrics.metric.opCommit;
		self->m_lazyClearActor = incrementalLazyClear(self);

		committed.send(Void());
		return Void();
	}

public:
	// Cursor into BTree which enables seeking and iteration in the BTree as a whole, or
	// iteration within a specific page and movement across levels for more efficient access.
	// Cursor record's memory is only guaranteed to be valid until cursor moves to a different page.
	class BTreeCursor {
	public:
		struct PathEntry {
			Reference<const ArenaPage> page;
			BTreePage::BinaryTree::Cursor cursor;
#if REDWOOD_DEBUG
			Standalone<BTreePageIDRef> id;
#endif

			const BTreePage* btPage() const { return (BTreePage*)page->begin(); };
		};

	private:
		PagerEventReasons reason;
		VersionedBTree* btree;
		Reference<IPagerSnapshot> pager;
		bool valid;
		std::vector<PathEntry> path;

	public:
		BTreeCursor() : reason(PagerEventReasons::MAXEVENTREASONS) {}

		bool intialized() const { return pager.isValid(); }
		bool isValid() const { return valid; }

		// path entries at dumpHeight or below will have their entire pages printed
		std::string toString(int dumpHeight = 0) const {
			std::string r = format("{ptr=%p reason=%s %s ",
			                       this,
			                       PagerEventReasonsStrings[(int)reason],
			                       ::toString(pager->getVersion()).c_str());
			for (int i = 0; i < path.size(); ++i) {
				std::string id = "<debugOnly>";
#if REDWOOD_DEBUG
				id = ::toString(path[i].id);
#endif
				int height = path[i].btPage()->height;
				r += format("\n\t[Level=%d ID=%s ptr=%p Cursor=%s]   ",
				            height,
				            id.c_str(),
				            path[i].page->begin(),
				            path[i].cursor.valid() ? path[i].cursor.get().toString(path[i].btPage()->isLeaf()).c_str()
				                                   : "<invalid>");
				if (height <= dumpHeight) {
					BTreePage::BinaryTree::Cursor c = path[i].cursor;
					c.moveFirst();
					int i = 0;
					while (c.valid()) {
						r += format("\n\t\%7d: %s", ++i, c.get().toString(height == 1).c_str());
						c.moveNext();
					}
				}
			}
			r += "\n";
			if (!valid) {
				r += " (invalid) ";
			}
			r += "}";
			return r;
		}

		const RedwoodRecordRef get() { return path.back().cursor.get(); }

		bool inRoot() const { return path.size() == 1; }

		// To enable more efficient range scans, caller can read the lowest page
		// of the cursor and pop it.
		PathEntry& back() { return path.back(); }
		void popPath() { path.pop_back(); }

		Future<Void> pushPage(const BTreePage::BinaryTree::Cursor& link) {
			debug_printf("pushPage(link=%s)\n", link.get().toString(false).c_str());
			return map(readPage(reason,
			                    path.back().btPage()->height - 1,
			                    pager.getPtr(),
			                    link.get().getChildPage(),
			                    ioMaxPriority,
			                    false,
			                    true),
			           [=](Reference<const ArenaPage> p) {
#if REDWOOD_DEBUG
				           path.push_back({ p, btree->getCursor(p.getPtr(), link), link.get().getChildPage() });
#else
							path.push_back({ p, btree->getCursor(p.getPtr(), link) });
#endif

				           if (btree->m_pBoundaryVerifier != nullptr) {
					           ASSERT(btree->m_pBoundaryVerifier->verify(link.get().getChildPage().front(),
					                                                     pager->getVersion(),
					                                                     link.get().key,
					                                                     link.next().getOrUpperBound().key));
				           }
				           return Void();
			           });
		}

		Future<Void> pushPage(BTreePageIDRef id) {
			debug_printf("pushPage(root=%s)\n", ::toString(id).c_str());
			return map(readPage(reason, btree->m_pHeader->height, pager.getPtr(), id, ioMaxPriority, false, true),
			           [=](Reference<const ArenaPage> p) {
#if REDWOOD_DEBUG
				           path.push_back({ p, btree->getCursor(p.getPtr(), dbBegin, dbEnd), id });
#else
				path.push_back({ p, btree->getCursor(p.getPtr(), dbBegin, dbEnd) });
#endif
				           return Void();
			           });
		}

		// Initialize or reinitialize cursor
		Future<Void> init(VersionedBTree* btree_in,
		                  PagerEventReasons reason_in,
		                  Reference<IPagerSnapshot> pager_in,
		                  BTreePageIDRef root) {
			btree = btree_in;
			reason = reason_in;
			pager = pager_in;
			path.clear();
			path.reserve(6);
			valid = false;
			return root.empty() ? Void() : pushPage(root);
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
		ACTOR Future<int> seek_impl(BTreeCursor* self, RedwoodRecordRef query) {
			state RedwoodRecordRef internalPageQuery = query.withMaxPageID();
			self->path.resize(1);
			debug_printf("seek(%s) start cursor = %s\n", query.toString().c_str(), self->toString().c_str());

			loop {
				auto& entry = self->path.back();
				if (entry.btPage()->isLeaf()) {
					int cmp = entry.cursor.seek(query);
					self->valid = entry.cursor.valid() && !entry.cursor.isErased();
					debug_printf("seek(%s) loop exit cmp=%d cursor=%s\n",
					             query.toString().c_str(),
					             cmp,
					             self->toString().c_str());
					return self->valid ? cmp : 0;
				}

				// Internal page, so seek to the branch where query must be
				// Currently, after a subtree deletion internal page boundaries are still strictly adhered
				// to and will be updated if anything is inserted into the cleared range, so if the seek fails
				// or it finds an entry with a null child page then query does not exist in the BTree.
				if (entry.cursor.seekLessThan(internalPageQuery) && entry.cursor.get().value.present()) {
					debug_printf(
					    "seek(%s) loop seek success cursor=%s\n", query.toString().c_str(), self->toString().c_str());
					Future<Void> f = self->pushPage(entry.cursor);
					wait(f);
				} else {
					self->valid = false;
					debug_printf(
					    "seek(%s) loop exit cmp=0 cursor=%s\n", query.toString().c_str(), self->toString().c_str());
					return 0;
				}
			}
		}

		Future<int> seek(RedwoodRecordRef query) { return path.empty() ? 0 : seek_impl(this, query); }

		ACTOR Future<Void> seekGTE_impl(BTreeCursor* self, RedwoodRecordRef query) {
			debug_printf("seekGTE(%s) start\n", query.toString().c_str());
			int cmp = wait(self->seek(query));
			if (cmp > 0 || (cmp == 0 && !self->isValid())) {
				wait(self->moveNext());
			}
			return Void();
		}

		Future<Void> seekGTE(RedwoodRecordRef query) { return seekGTE_impl(this, query); }

		// Start fetching sibling nodes in the forward or backward direction, stopping after recordLimit or byteLimit
		void prefetch(KeyRef rangeEnd, bool directionForward, int recordLimit, int byteLimit) {
			// Prefetch scans level 2 so if there are less than 2 nodes in the path there is no level 2
			if (path.size() < 2) {
				return;
			}

			auto firstLeaf = path.back().btPage();

			// We know the first leaf's record count, so assume they are all relevant to the query,
			// even though some may not be.
			int recordsRead = firstLeaf->tree()->numItems;

			// We can't know for sure how many records are in a node without reading it, so just guess
			// that siblings have about the same record count as the first leaf.
			int estRecordsPerPage = recordsRead;

			// Use actual KVBytes stored for the first leaf, but use node capacity for siblings below
			int bytesRead = firstLeaf->kvBytes;

			// Cursor for moving through siblings.
			// Note that only immediate siblings under the same parent are considered for prefetch so far.
			BTreePage::BinaryTree::Cursor c = path[path.size() - 2].cursor;
			ASSERT(path[path.size() - 2].btPage()->height == 2);

			// The loop conditions are split apart into different if blocks for readability.
			// While query limits are not exceeded
			while (recordsRead < recordLimit && bytesRead < byteLimit) {
				// If prefetching right siblings
				if (directionForward) {
					// If there is no right sibling or its lower boundary is greater
					// or equal to than the range end then stop.
					if (!c.moveNext() || c.get().key >= rangeEnd) {
						break;
					}
				} else {
					// Prefetching left siblings
					// If the current leaf lower boundary is less than or equal to the range end
					// or there is no left sibling then stop
					if (c.get().key <= rangeEnd || !c.movePrev()) {
						break;
					}
				}

				// Prefetch the sibling if the link is not null
				if (c.get().value.present()) {
					BTreePageIDRef childPage = c.get().getChildPage();
					if (childPage.size() > 0)
						preLoadPage(pager.getPtr(), childPage, ioLeafPriority);
					recordsRead += estRecordsPerPage;
					// Use sibling node capacity as an estimate of bytes read.
					bytesRead += childPage.size() * this->btree->m_blockSize;
				}
			}
		}

		ACTOR Future<Void> seekLT_impl(BTreeCursor* self, RedwoodRecordRef query) {
			debug_printf("seekLT(%s) start\n", query.toString().c_str());
			int cmp = wait(self->seek(query));
			if (cmp <= 0) {
				wait(self->movePrev());
			}
			return Void();
		}

		Future<Void> seekLT(RedwoodRecordRef query) { return seekLT_impl(this, query); }

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
					debug_printf("move%s() exit cursor=%s\n", forward ? "Next" : "Prev", self->toString(1).c_str());
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

			debug_printf("move%s() exit cursor=%s\n", forward ? "Next" : "Prev", self->toString(1).c_str());
			return Void();
		}

		Future<Void> moveNext() { return path.empty() ? Void() : move_impl(this, true); }
		Future<Void> movePrev() { return path.empty() ? Void() : move_impl(this, false); }
	};

	Future<Void> initBTreeCursor(BTreeCursor* cursor, Version snapshotVersion, PagerEventReasons reason) {
		Reference<IPagerSnapshot> snapshot = m_pager->getReadSnapshot(snapshotVersion);

		// This is a ref because snapshot will continue to hold the metakey value memory
		KeyRef m = snapshot->getMetaKey();

		return cursor->init(
		    this, reason, snapshot, m.size() == 0 ? BTreePageIDRef() : ((MetaKey*)m.begin())->root.get());
	}
};

#include "fdbserver/art_impl.h"

RedwoodRecordRef VersionedBTree::dbBegin(LiteralStringRef(""));
RedwoodRecordRef VersionedBTree::dbEnd(LiteralStringRef("\xff\xff\xff\xff\xff"));

class KeyValueStoreRedwood : public IKeyValueStore {
public:
	KeyValueStoreRedwood(std::string filePrefix, UID logID)
	  : m_filename(filePrefix), m_concurrentReads(SERVER_KNOBS->REDWOOD_KVSTORE_CONCURRENT_READS, 0),
	    prefetch(SERVER_KNOBS->REDWOOD_KVSTORE_RANGE_PREFETCH) {

		int pageSize =
		    BUGGIFY ? deterministicRandom()->randomInt(1000, 4096 * 4) : SERVER_KNOBS->REDWOOD_DEFAULT_PAGE_SIZE;
		int extentSize = SERVER_KNOBS->REDWOOD_DEFAULT_EXTENT_SIZE;
		int64_t pageCacheBytes =
		    g_network->isSimulated()
		        ? (BUGGIFY ? deterministicRandom()->randomInt(pageSize, FLOW_KNOBS->BUGGIFY_SIM_PAGE_CACHE_4K)
		                   : FLOW_KNOBS->SIM_PAGE_CACHE_4K)
		        : FLOW_KNOBS->PAGE_CACHE_4K;
		// Rough size of pages to keep in remap cleanup queue before being cleanup.
		int64_t remapCleanupWindowBytes =
		    g_network->isSimulated()
		        ? (BUGGIFY ? (deterministicRandom()->coinflip()
		                          ? deterministicRandom()->randomInt64(0, 100 * 1024) // small window
		                          : deterministicRandom()->randomInt64(0, 100 * 1024 * 1024)) // large window
		                   : 100 * 1024 * 1024) // 100M
		        : SERVER_KNOBS->REDWOOD_REMAP_CLEANUP_WINDOW_BYTES;

		IPager2* pager = new DWALPager(pageSize,
		                               extentSize,
		                               filePrefix,
		                               pageCacheBytes,
		                               remapCleanupWindowBytes,
		                               SERVER_KNOBS->REDWOOD_EXTENT_CONCURRENT_READS,
		                               false,
		                               m_error);
		m_tree = new VersionedBTree(pager, filePrefix);
		m_init = catchError(init_impl(this));
	}

	Future<Void> init() override { return m_init; }

	ACTOR Future<Void> init_impl(KeyValueStoreRedwood* self) {
		TraceEvent(SevInfo, "RedwoodInit").detail("FilePrefix", self->m_filename);
		wait(self->m_tree->init());
		TraceEvent(SevInfo, "RedwoodInitComplete")
		    .detail("Filename", self->m_filename)
		    .detail("Version", self->m_tree->getLastCommittedVersion());
		self->m_nextCommitVersion = self->m_tree->getLastCommittedVersion() + 1;
		return Void();
	}

	ACTOR void shutdown(KeyValueStoreRedwood* self, bool dispose) {
		TraceEvent(SevInfo, "RedwoodShutdown").detail("Filename", self->m_filename).detail("Dispose", dispose);
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
		TraceEvent(SevInfo, "RedwoodShutdownComplete").detail("Filename", self->m_filename).detail("Dispose", dispose);
		delete self;
	}

	void close() override { shutdown(this, false); }

	void dispose() override { shutdown(this, true); }

	Future<Void> onClosed() const override { return m_closed.getFuture(); }

	Future<Void> commit(bool sequential = false) override {
		Future<Void> c = m_tree->commit(m_nextCommitVersion);
		// Currently not keeping history
		m_tree->setOldestReadableVersion(m_nextCommitVersion);
		++m_nextCommitVersion;
		return catchError(c);
	}

	KeyValueStoreType getType() const override { return KeyValueStoreType::SSD_REDWOOD_V1; }

	StorageBytes getStorageBytes() const override { return m_tree->getStorageBytes(); }

	Future<Void> getError() const override { return delayed(m_error.getFuture()); };

	void clear(KeyRangeRef range, const Arena* arena = 0) override {
		debug_printf("CLEAR %s\n", printable(range).c_str());
		m_tree->clear(range);
	}

	void set(KeyValueRef keyValue, const Arena* arena = nullptr) override {
		debug_printf("SET %s\n", printable(keyValue).c_str());
		m_tree->set(keyValue);
	}

	Future<RangeResult> readRange(KeyRangeRef keys, int rowLimit, int byteLimit, IKeyValueStore::ReadType) override {
		debug_printf("READRANGE %s\n", printable(keys).c_str());
		return catchError(readRange_impl(this, keys, rowLimit, byteLimit));
	}

	ACTOR static Future<RangeResult> readRange_impl(KeyValueStoreRedwood* self,
	                                                KeyRange keys,
	                                                int rowLimit,
	                                                int byteLimit) {
		state VersionedBTree::BTreeCursor cur;
		wait(
		    self->m_tree->initBTreeCursor(&cur, self->m_tree->getLastCommittedVersion(), PagerEventReasons::RangeRead));
		state PriorityMultiLock::Lock lock;
		state Future<Void> f;
		++g_redwoodMetrics.metric.opGetRange;

		state RangeResult result;
		state int accumulatedBytes = 0;
		ASSERT(byteLimit > 0);

		if (rowLimit == 0) {
			return result;
		}

		if (rowLimit > 0) {
			f = cur.seekGTE(keys.begin);
			if (f.isReady()) {
				TEST(true); // Cached forward range read seek
				f.get();
			} else {
				TEST(true); // Uncached forward range read seek
				wait(store(lock, self->m_concurrentReads.lock()));
				wait(f);
			}

			if (self->prefetch) {
				cur.prefetch(keys.end, true, rowLimit, byteLimit);
			}

			while (cur.isValid()) {
				// Read leaf page contents without using waits by using the leaf page cursor directly
				// and advancing it until it is no longer valid
				BTreePage::BinaryTree::Cursor& leafCursor = cur.back().cursor;

				// we can bypass the bounds check for each key in the leaf if the entire leaf is in range
				// > because both query end and page upper bound are exclusive of the query results and page contents,
				// respectively
				bool checkBounds = leafCursor.cache->upperBound > keys.end;
				// Whether or not any results from this page were added to results
				bool usedPage = false;

				while (leafCursor.valid()) {
					KeyValueRef kv = leafCursor.get().toKeyValueRef();
					if (checkBounds && kv.key.compare(keys.end) >= 0) {
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
					result.arena().dependsOn(leafCursor.cache->arena);
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
			f = cur.seekLT(keys.end);
			if (f.isReady()) {
				TEST(true); // Cached reverse range read seek
				f.get();
			} else {
				TEST(true); // Uncached reverse range read seek
				wait(store(lock, self->m_concurrentReads.lock()));
				wait(f);
			}

			if (self->prefetch) {
				cur.prefetch(keys.begin, false, -rowLimit, byteLimit);
			}

			while (cur.isValid()) {
				// Read leaf page contents without using waits by using the leaf page cursor directly
				// and advancing it until it is no longer valid
				BTreePage::BinaryTree::Cursor& leafCursor = cur.back().cursor;

				// we can bypass the bounds check for each key in the leaf if the entire leaf is in range
				// < because both query begin and page lower bound are inclusive of the query results and page contents,
				// respectively
				bool checkBounds = leafCursor.cache->lowerBound < keys.begin;
				// Whether or not any results from this page were added to results
				bool usedPage = false;

				while (leafCursor.valid()) {
					KeyValueRef kv = leafCursor.get().toKeyValueRef();
					if (checkBounds && kv.key.compare(keys.begin) < 0) {
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
					result.arena().dependsOn(leafCursor.cache->arena);
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
		g_redwoodMetrics.kvSizeReadByGetRange->sample(accumulatedBytes);
		return result;
	}

	ACTOR static Future<Optional<Value>> readValue_impl(KeyValueStoreRedwood* self, Key key, Optional<UID> debugID) {
		state VersionedBTree::BTreeCursor cur;
		wait(
		    self->m_tree->initBTreeCursor(&cur, self->m_tree->getLastCommittedVersion(), PagerEventReasons::PointRead));

		// Not locking for point reads, instead relying on IO priority lock
		// state PriorityMultiLock::Lock lock = wait(self->m_concurrentReads.lock());

		++g_redwoodMetrics.metric.opGet;
		wait(cur.seekGTE(key));
		if (cur.isValid() && cur.get().key == key) {
			// Return a Value whose arena depends on the source page arena
			Value v;
			v.arena().dependsOn(cur.back().page->getArena());
			v.contents() = cur.get().value.get();
			g_redwoodMetrics.kvSizeReadByGet->sample(cur.get().kvBytes());
			return v;
		}

		return Optional<Value>();
	}

	Future<Optional<Value>> readValue(KeyRef key, IKeyValueStore::ReadType, Optional<UID> debugID) override {
		return catchError(readValue_impl(this, key, debugID));
	}

	Future<Optional<Value>> readValuePrefix(KeyRef key,
	                                        int maxLength,
	                                        IKeyValueStore::ReadType,
	                                        Optional<UID> debugID) override {
		return catchError(map(readValue_impl(this, key, debugID), [maxLength](Optional<Value> v) {
			if (v.present() && v.get().size() > maxLength) {
				v.get().contents() = v.get().substr(0, maxLength);
			}
			return v;
		}));
	}

	~KeyValueStoreRedwood() override{};

private:
	std::string m_filename;
	VersionedBTree* m_tree;
	Future<Void> m_init;
	Promise<Void> m_closed;
	Promise<Void> m_error;
	PriorityMultiLock m_concurrentReads;
	bool prefetch;
	Version m_nextCommitVersion;

	template <typename T>
	inline Future<T> catchError(Future<T> f) {
		return forwardError(f, m_error);
	}
};

IKeyValueStore* keyValueStoreRedwoodV1(std::string const& filename, UID logID) {
	return new KeyValueStoreRedwood(filename, logID);
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
ACTOR Future<Void> verifyRangeBTreeCursor(VersionedBTree* btree,
                                          Key start,
                                          Key end,
                                          Version v,
                                          std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                                          int64_t* pRecordsRead) {
	if (end <= start)
		end = keyAfter(start);

	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i =
	    written->lower_bound(std::make_pair(start.toString(), 0));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd =
	    written->upper_bound(std::make_pair(end.toString(), initialVersion));
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iLast;

	state VersionedBTree::BTreeCursor cur;
	wait(btree->initBTreeCursor(&cur, v, PagerEventReasons::RangeRead));
	debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Start\n", v, start.printable().c_str(), end.printable().c_str());

	// Randomly use the cursor for something else first.
	if (deterministicRandom()->coinflip()) {
		state Key randomKey = randomKV().key;
		debug_printf("VerifyRange(@%" PRId64 ", %s, %s): Dummy seek to '%s'\n",
		             v,
		             start.printable().c_str(),
		             end.printable().c_str(),
		             randomKey.toString().c_str());
		wait(success(cur.seek(randomKey)));
	}

	debug_printf(
	    "VerifyRange(@%" PRId64 ", %s, %s): Actual seek\n", v, start.printable().c_str(), end.printable().c_str());

	wait(cur.seekGTE(start));

	state Standalone<VectorRef<KeyValueRef>> results;

	while (cur.isValid() && cur.get().key < end) {
		// Find the next written kv pair that would be present at this version
		while (1) {
			// Since the written map grows, range scans become less efficient so count all records written
			// at any version against the records read count
			++*pRecordsRead;
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
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR:BTree key '%s' vs nothing in written map.\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str());
			ASSERT(false);
		}

		if (cur.get().key != iLast->first.first) {
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR:BTree key '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       iLast->first.first.c_str());
			ASSERT(false);
		}
		if (cur.get().value.get() != iLast->second.get()) {
			printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR:BTree key '%s' has tree value '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       cur.get().value.get().toString().c_str(),
			       iLast->second.get().c_str());
			ASSERT(false);
		}

		results.push_back(results.arena(), cur.get().toKeyValueRef());
		results.arena().dependsOn(cur.back().cursor.cache->arena);
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
		printf("VerifyRange(@%" PRId64 ", %s, %s) ERROR: BTree range ended but written has @%" PRId64 " '%s'\n",
		       v,
		       start.printable().c_str(),
		       end.printable().c_str(),
		       iLast->first.second,
		       iLast->first.first.c_str());
		ASSERT(false);
	}

	debug_printf(
	    "VerifyRangeReverse(@%" PRId64 ", %s, %s): start\n", v, start.printable().c_str(), end.printable().c_str());

	// Randomly use a new cursor at the same version for the reverse range read, if the version is still available for
	// opening new cursors
	if (v >= btree->getOldestReadableVersion() && deterministicRandom()->coinflip()) {
		cur = VersionedBTree::BTreeCursor();
		wait(btree->initBTreeCursor(&cur, v, PagerEventReasons::RangeRead));
	}

	// Now read the range from the tree in reverse order and compare to the saved results
	wait(cur.seekLT(end));

	state std::reverse_iterator<const KeyValueRef*> r = results.rbegin();

	while (cur.isValid() && cur.get().key >= start) {
		++*pRecordsRead;
		if (r == results.rend()) {
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR:BTree key '%s' vs nothing in written map.\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str());
			ASSERT(false);
		}

		if (cur.get().key != r->key) {
			printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR:BTree key '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       r->key.toString().c_str());
			ASSERT(false);
		}
		if (cur.get().value.get() != r->value) {
			printf("VerifyRangeReverse(@%" PRId64
			       ", %s, %s) ERROR:BTree key '%s' has tree value '%s' but expected '%s'\n",
			       v,
			       start.printable().c_str(),
			       end.printable().c_str(),
			       cur.get().key.toString().c_str(),
			       cur.get().value.get().toString().c_str(),
			       r->value.toString().c_str());
			ASSERT(false);
		}

		++r;
		wait(cur.movePrev());
	}

	if (r != results.rend()) {
		printf("VerifyRangeReverse(@%" PRId64 ", %s, %s) ERROR: BTree range ended but written has '%s'\n",
		       v,
		       start.printable().c_str(),
		       end.printable().c_str(),
		       r->key.toString().c_str());
		ASSERT(false);
	}

	return Void();
}

// Verify the result of point reads for every set or cleared key change made at exactly v
ACTOR Future<Void> seekAllBTreeCursor(VersionedBTree* btree,
                                      Version v,
                                      std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                                      int64_t* pRecordsRead) {
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator i = written->cbegin();
	state std::map<std::pair<std::string, Version>, Optional<std::string>>::const_iterator iEnd = written->cend();
	state VersionedBTree::BTreeCursor cur;

	wait(btree->initBTreeCursor(&cur, v, PagerEventReasons::RangeRead));

	while (i != iEnd) {
		// Since the written map gets larger and takes longer to scan each time, count visits to all written recs
		++*pRecordsRead;
		state std::string key = i->first.first;
		state Version ver = i->first.second;
		if (ver == v) {
			state Optional<std::string> val = i->second;
			debug_printf("Verifying @%" PRId64 " '%s'\n", ver, key.c_str());
			state Arena arena;
			wait(cur.seekGTE(RedwoodRecordRef(KeyRef(arena, key))));
			bool foundKey = cur.isValid() && cur.get().key == key;
			bool hasValue = foundKey && cur.get().value.present();

			if (val.present()) {
				bool valueMatch = hasValue && cur.get().value.get() == val.get();
				if (!foundKey || !hasValue || !valueMatch) {
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
					ASSERT(false);
				}
			} else if (foundKey && hasValue) {
				printf("Verify ERROR: cleared_key_found: '%s' -> '%s' @%" PRId64 "\n",
				       key.c_str(),
				       cur.get().value.get().toString().c_str(),
				       ver);
				ASSERT(false);
			}
		}
		++i;
	}
	return Void();
}

ACTOR Future<Void> verify(VersionedBTree* btree,
                          FutureStream<Version> vStream,
                          std::map<std::pair<std::string, Version>, Optional<std::string>>* written,
                          int64_t* pRecordsRead,
                          bool serial) {

	// Queue of committed versions still readable from btree
	state std::deque<Version> committedVersions;

	try {
		loop {
			state Version v = waitNext(vStream);
			committedVersions.push_back(v);

			// Remove expired versions
			while (!committedVersions.empty() && committedVersions.front() < btree->getOldestReadableVersion()) {
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
			state VersionedBTree::BTreeCursor cur;
			wait(btree->initBTreeCursor(&cur, v, PagerEventReasons::RangeRead));

			debug_printf("Verifying entire key range at version %" PRId64 "\n", v);
			state Future<Void> fRangeAll = verifyRangeBTreeCursor(
			    btree, LiteralStringRef(""), LiteralStringRef("\xff\xff"), v, written, pRecordsRead);
			if (serial) {
				wait(fRangeAll);
			}

			Key begin = randomKV().key;
			Key end = randomKV().key;

			debug_printf(
			    "Verifying range (%s, %s) at version %" PRId64 "\n", toString(begin).c_str(), toString(end).c_str(), v);
			state Future<Void> fRangeRandom = verifyRangeBTreeCursor(btree, begin, end, v, written, pRecordsRead);
			if (serial) {
				wait(fRangeRandom);
			}

			debug_printf("Verifying seeks to each changed key at version %" PRId64 "\n", v);
			state Future<Void> fSeekAll = seekAllBTreeCursor(btree, v, written, pRecordsRead);
			if (serial) {
				wait(fSeekAll);
			}

			wait(fRangeAll && fRangeRandom && fSeekAll);

			printf("Verified version %" PRId64 "\n", v);
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream && e.code() != error_code_transaction_too_old) {
			throw;
		}
	}
	return Void();
}

// Does a random range read, doesn't trap/report errors
ACTOR Future<Void> randomReader(VersionedBTree* btree, int64_t* pRecordsRead) {
	try {
		state VersionedBTree::BTreeCursor cur;

		loop {
			wait(yield());
			if (!cur.intialized() || deterministicRandom()->random01() > .01) {
				wait(btree->initBTreeCursor(&cur, btree->getLastCommittedVersion(), PagerEventReasons::RangeRead));
			}

			state KeyValue kv = randomKV(10, 0);
			wait(cur.seekGTE(kv.key));
			state int c = deterministicRandom()->randomInt(0, 100);
			state bool direction = deterministicRandom()->coinflip();
			while (cur.isValid() && c-- > 0) {
				++*pRecordsRead;
				wait(success(direction ? cur.moveNext() : cur.movePrev()));
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

	typedef IntIntPair Partial;

	void updateCache(Optional<Partial> cache, Arena& arena) const {}
	struct Delta {
		bool prefixSource;
		bool deleted;
		int dk;
		int dv;

		IntIntPair apply(const IntIntPair& base, Arena& arena) { return { base.k + dk, base.v + dv }; }

		IntIntPair apply(const Partial& cache) { return cache; }

		IntIntPair apply(Arena& arena, const IntIntPair& base, Optional<Partial>& cache) {
			cache = IntIntPair(base.k + dk, base.v + dv);
			return cache.get();
		}

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

	return rec;
}

void RedwoodMetrics::getFields(TraceEvent* e, std::string* s, bool skipZeroes) {
	std::pair<const char*, unsigned int> metrics[] = { { "BTreePreload", metric.btreeLeafPreload },
		                                               { "BTreePreloadExt", metric.btreeLeafPreloadExt },
		                                               { "", 0 },
		                                               { "OpSet", metric.opSet },
		                                               { "OpSetKeyBytes", metric.opSetKeyBytes },
		                                               { "OpSetValueBytes", metric.opSetValueBytes },
		                                               { "OpClear", metric.opClear },
		                                               { "OpClearKey", metric.opClearKey },
		                                               { "", 0 },
		                                               { "OpGet", metric.opGet },
		                                               { "OpGetRange", metric.opGetRange },
		                                               { "OpCommit", metric.opCommit },
		                                               { "", 0 },
		                                               { "PagerDiskWrite", metric.pagerDiskWrite },
		                                               { "PagerDiskRead", metric.pagerDiskRead },
		                                               { "PagerCacheHit", metric.pagerCacheHit },
		                                               { "PagerCacheMiss", metric.pagerCacheMiss },
		                                               { "", 0 },
		                                               { "PagerProbeHit", metric.pagerProbeHit },
		                                               { "PagerProbeMiss", metric.pagerProbeMiss },
		                                               { "PagerEvictUnhit", metric.pagerEvictUnhit },
		                                               { "PagerEvictFail", metric.pagerEvictFail },
		                                               { "", 0 },
		                                               { "PagerRemapFree", metric.pagerRemapFree },
		                                               { "PagerRemapCopy", metric.pagerRemapCopy },
		                                               { "PagerRemapSkip", metric.pagerRemapSkip },
		                                               { "", 0 } };

	double elapsed = now() - startTime;

	if (e != nullptr) {
		for (auto& m : metrics) {
			char c = m.first[0];
			if (c != 0 && (!skipZeroes || m.second != 0)) {
				e->detail(m.first, m.second);
			}
		}
		levels[0].metrics.events.toTraceEvent(e, 0);
	}

	if (s != nullptr) {
		for (auto& m : metrics) {
			if (*m.first == '\0') {
				*s += "\n";
			} else if (!skipZeroes || m.second != 0) {
				*s += format("%-15s %-8u %8" PRId64 "/s  ", m.first, m.second, int64_t(m.second / elapsed));
			}
		}
		*s += levels[0].metrics.events.toString(0, elapsed);
	}

	auto const& evictor = DWALPager::PageCacheT::Evictor::getEvictor();

	std::pair<const char*, int64_t> cacheMetrics[] = { { "PageCacheCount", evictor->getCountUsed() },
		                                               { "PageCacheMoved", evictor->getCountMoved() },
		                                               { "PageCacheSize", evictor->getSizeUsed() },
		                                               { "DecodeCacheSize", evictor->reservedSize } };

	if (e != nullptr) {
		for (auto& m : cacheMetrics) {
			e->detail(m.first, m.second);
		}
	}

	if (s != nullptr) {
		for (auto& m : cacheMetrics) {
			*s += format("%-15s %-14" PRId64 "       ", m.first, m.second);
		}
		*s += "\n";
	}

	for (int i = 1; i < btreeLevels + 1; ++i) {
		auto& metric = levels[i].metrics;

		std::pair<const char*, unsigned int> metrics[] = {
			{ "PageBuild", metric.pageBuild },
			{ "PageBuildExt", metric.pageBuildExt },
			{ "PageModify", metric.pageModify },
			{ "PageModifyExt", metric.pageModifyExt },
			{ "", 0 },
			{ "PageRead", metric.pageRead },
			{ "PageReadExt", metric.pageReadExt },
			{ "PageCommitStart", metric.pageCommitStart },
			{ "", 0 },
			{ "LazyClearInt", metric.lazyClearRequeue },
			{ "LazyClearIntExt", metric.lazyClearRequeueExt },
			{ "LazyClear", metric.lazyClearFree },
			{ "LazyClearExt", metric.lazyClearFreeExt },
			{ "", 0 },
			{ "ForceUpdate", metric.forceUpdate },
			{ "DetachChild", metric.detachChild },
			{ "", 0 },
		};

		if (e != nullptr) {
			for (auto& m : metrics) {
				char c = m.first[0];
				if (c != 0 && (!skipZeroes || m.second != 0)) {
					e->detail(format("L%d%s", i, m.first + (c == '-' ? 1 : 0)), m.second);
				}
			}
			metric.events.toTraceEvent(e, i);
		}

		if (s != nullptr) {
			*s += format("\nLevel %d\n\t", i);

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
			*s += metric.events.toString(i, elapsed);
		}
	}
}

TEST_CASE("/redwood/correctness/unit/RedwoodRecordRef") {
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[0] == 3);
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[1] == 4);
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[2] == 6);
	ASSERT(RedwoodRecordRef::Delta::LengthFormatSizes[3] == 8);

	fmt::print("sizeof(RedwoodRecordRef) = {}\n", sizeof(RedwoodRecordRef));

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

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef("abc"), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef("abc"), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef("abcd"), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef("abcd"), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef("abc"), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(std::string(300, 'k'), std::string(1e6, 'v')),
	          RedwoodRecordRef(std::string(300, 'k'), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")));

	deltaTest(RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")),
	          RedwoodRecordRef(LiteralStringRef(""), LiteralStringRef("")));

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

TEST_CASE("Lredwood/correctness/unit/deltaTree/RedwoodRecordRef") {
	// Sanity check on delta tree node format
	ASSERT(DeltaTree2<RedwoodRecordRef>::Node::headerSize(false) == 4);
	ASSERT(DeltaTree2<RedwoodRecordRef>::Node::headerSize(true) == 8);

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

TEST_CASE("Lredwood/correctness/unit/deltaTree/RedwoodRecordRef2") {
	// Sanity check on delta tree node format
	ASSERT(DeltaTree2<RedwoodRecordRef>::Node::headerSize(false) == 4);
	ASSERT(DeltaTree2<RedwoodRecordRef>::Node::headerSize(true) == 8);
	ASSERT(sizeof(DeltaTree2<RedwoodRecordRef>::DecodedNode) == 28);

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
		if (deterministicRandom()->coinflip()) {
			rec.value = StringRef(arena, v);
		}
		if (uniqueItems.count(rec) == 0) {
			uniqueItems.insert(rec);
		}
	}
	std::vector<RedwoodRecordRef> items(uniqueItems.begin(), uniqueItems.end());

	int bufferSize = N * 100;
	bool largeTree = bufferSize > DeltaTree2<RedwoodRecordRef>::SmallSizeLimit;
	DeltaTree2<RedwoodRecordRef>* tree = (DeltaTree2<RedwoodRecordRef>*)new uint8_t[bufferSize];

	tree->build(bufferSize, &items[0], &items[items.size()], &prev, &next);

	printf("Count=%d  Size=%d  InitialHeight=%d  largeTree=%d\n",
	       (int)items.size(),
	       (int)tree->size(),
	       (int)tree->initialHeight,
	       largeTree);
	debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t*)tree, tree->size()).toHexString().c_str());

	DeltaTree2<RedwoodRecordRef>::DecodeCache cache(prev, next);
	DeltaTree2<RedwoodRecordRef>::Cursor c(&cache, tree);

	// Test delete/insert behavior for each item, making no net changes
	printf("Testing seek/delete/insert for existing keys with random values\n");
	ASSERT(tree->numItems == items.size());
	for (auto rec : items) {
		// Insert existing should fail
		ASSERT(!c.insert(rec));
		ASSERT(tree->numItems == items.size());

		// Erase existing should succeed
		ASSERT(c.erase(rec));
		ASSERT(tree->numItems == items.size() - 1);

		// Erase deleted should fail
		ASSERT(!c.erase(rec));
		ASSERT(tree->numItems == items.size() - 1);

		// Insert deleted should succeed
		ASSERT(c.insert(rec));
		ASSERT(tree->numItems == items.size());

		// Insert existing should fail
		ASSERT(!c.insert(rec));
		ASSERT(tree->numItems == items.size());
	}

	DeltaTree2<RedwoodRecordRef>::Cursor fwd = c;
	DeltaTree2<RedwoodRecordRef>::Cursor rev = c;

	DeltaTree2<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>::DecodeCache cacheValuesOnly(prev, next);
	DeltaTree2<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>::Cursor fwdValueOnly(
	    &cacheValuesOnly, (DeltaTree2<RedwoodRecordRef, RedwoodRecordRef::DeltaValueOnly>*)tree);

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
			printf("Cursor: %s\n", fwd.toString().c_str());
			ASSERT(false);
		}
		if (rev.get() != items[items.size() - 1 - i]) {
			printf("reverse iterator i=%d\n  %s found\n  %s expected\n",
			       i,
			       rev.get().toString().c_str(),
			       items[items.size() - 1 - i].toString().c_str());
			printf("Cursor: %s\n", rev.toString().c_str());
			ASSERT(false);
		}
		if (fwdValueOnly.get().value != items[i].value) {
			printf("forward values-only iterator i=%d\n  %s found\n  %s expected\n",
			       i,
			       fwdValueOnly.get().toString().c_str(),
			       items[i].toString().c_str());
			printf("Cursor: %s\n", fwdValueOnly.toString().c_str());
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
		DeltaTree2<RedwoodRecordRef>::DecodeCache cache(prev, next);
		DeltaTree2<RedwoodRecordRef>::Cursor c(&cache, tree);

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

	// {
	// 	printf("Doing 5M random seeks using 10k random cursors, each from a different mirror.\n");
	// 	double start = timer();
	// 	std::vector<DeltaTree2<RedwoodRecordRef>::Mirror*> mirrors;
	// 	std::vector<DeltaTree2<RedwoodRecordRef>::Cursor> cursors;
	// 	for (int i = 0; i < 10000; ++i) {
	// 		mirrors.push_back(new DeltaTree2<RedwoodRecordRef>::Mirror(tree, &prev, &next));
	// 		cursors.push_back(mirrors.back()->getCursor());
	// 	}

	// 	for (int i = 0; i < 5000000; ++i) {
	// 		const RedwoodRecordRef& query = items[deterministicRandom()->randomInt(0, items.size())];
	// 		DeltaTree2<RedwoodRecordRef>::Cursor& c = cursors[deterministicRandom()->randomInt(0, cursors.size())];
	// 		if (!c.seekLessThanOrEqual(query)) {
	// 			printf("Not found!  query=%s\n", query.toString().c_str());
	// 			ASSERT(false);
	// 		}
	// 		if (c.get() != query) {
	// 			printf("Found incorrect node!  query=%s  found=%s\n",
	// 			       query.toString().c_str(),
	// 			       c.get().toString().c_str());
	// 			ASSERT(false);
	// 		}
	// 	}
	// 	double elapsed = timer() - start;
	// 	printf("Elapsed %f\n", elapsed);
	// }

	return Void();
}

TEST_CASE("Lredwood/correctness/unit/deltaTree/IntIntPair") {
	const int N = 200;
	IntIntPair lowerBound = { 0, 0 };
	IntIntPair upperBound = { 1000, 1000 };

	state std::function<IntIntPair()> randomPair = [&]() {
		// Generate a pair >= lowerBound and < upperBound
		int k = deterministicRandom()->randomInt(lowerBound.k, upperBound.k + 1);
		int v = deterministicRandom()->randomInt(lowerBound.v, upperBound.v);

		// Only generate even values so the tests below can approach and find each
		// key with a directional seek of the adjacent absent value on either side.
		v -= v % 2;

		return IntIntPair(k, v);
	};

	// Build a set of N unique items, where no consecutive items are in the set, a requirement of the seek behavior
	// tests.
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
	int bufferSize = N * 2 * 30;

	DeltaTree<IntIntPair>* tree = (DeltaTree<IntIntPair>*)new uint8_t[bufferSize];
	int builtSize = tree->build(bufferSize, &items[0], &items[items.size()], &lowerBound, &upperBound);
	ASSERT(builtSize <= bufferSize);
	DeltaTree<IntIntPair>::Mirror r(tree, &lowerBound, &upperBound);

	DeltaTree2<IntIntPair>* tree2 = (DeltaTree2<IntIntPair>*)new uint8_t[bufferSize];
	int builtSize2 = tree2->build(bufferSize, &items[0], &items[0] + items.size(), &lowerBound, &upperBound);
	ASSERT(builtSize2 <= bufferSize);
	DeltaTree2<IntIntPair>::DecodeCache cache(lowerBound, upperBound);
	DeltaTree2<IntIntPair>::Cursor cur2(&cache, tree2);

	auto printItems = [&] {
		for (int k = 0; k < items.size(); ++k) {
			debug_printf("%d/%d %s\n", k + 1, items.size(), items[k].toString().c_str());
		}
	};

	auto printTrees = [&] {
		printf("DeltaTree: Count=%d  Size=%d  InitialHeight=%d  MaxHeight=%d\n",
		       (int)tree->numItems,
		       (int)tree->size(),
		       (int)tree->initialHeight,
		       (int)tree->maxHeight);
		debug_printf("Data(%p): %s\n", tree, StringRef((uint8_t*)tree, tree->size()).toHexString().c_str());

		printf("DeltaTree2: Count=%d  Size=%d  InitialHeight=%d  MaxHeight=%d\n",
		       (int)tree2->numItems,
		       (int)tree2->size(),
		       (int)tree2->initialHeight,
		       (int)tree2->maxHeight);
		debug_printf("Data(%p): %s\n", tree2, StringRef((uint8_t*)tree2, tree2->size()).toHexString().c_str());
	};

	// Iterate through items and tree forward and backward, verifying tree contents.
	auto scanAndVerify = [&]() {
		printf("Verify DeltaTree contents.\n");

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

	// Iterate through items and tree forward and backward, verifying tree contents.
	auto scanAndVerify2 = [&]() {
		printf("Verify DeltaTree2 contents.\n");

		DeltaTree2<IntIntPair>::Cursor fwd(&cache, tree2);
		DeltaTree2<IntIntPair>::Cursor rev(&cache, tree2);

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

	printItems();
	printTrees();

	// Verify tree contents
	scanAndVerify();
	scanAndVerify2();

	// Grow uniqueItems until tree is full, adding half of new items to toDelete
	std::vector<IntIntPair> toDelete;
	int maxInsert = 9999999;
	bool shouldBeFull = false;
	while (maxInsert-- > 0) {
		IntIntPair p = randomPair();
		// Insert record if it, its predecessor, and its successor are not present.
		// Test data is intentionally sparse to test finding each record with a directional
		// seek from each adjacent possible but not present record.
		if (uniqueItems.count(p) == 0 && uniqueItems.count(IntIntPair(p.k, p.v - 1)) == 0 &&
		    uniqueItems.count(IntIntPair(p.k, p.v + 1)) == 0) {
			if (!cur2.insert(p)) {
				shouldBeFull = true;
				break;
			};
			ASSERT(r.insert(p));
			uniqueItems.insert(p);
			if (deterministicRandom()->coinflip()) {
				toDelete.push_back(p);
			}
			// printf("Inserted %s  size=%d\n", items.back().toString().c_str(), tree->size());
		}
	}

	// If the tree refused to insert an item, the count should be at least 2*N
	ASSERT(!shouldBeFull || tree->numItems > 2 * N);
	ASSERT(tree->size() <= bufferSize);

	// Update items vector
	items = std::vector<IntIntPair>(uniqueItems.begin(), uniqueItems.end());

	printItems();
	printTrees();

	// Verify tree contents
	scanAndVerify();
	scanAndVerify2();

	// Create a new mirror, decoding the tree from scratch since insert() modified both the tree and the mirror
	r = DeltaTree<IntIntPair>::Mirror(tree, &lowerBound, &upperBound);
	cache.clear();
	scanAndVerify();
	scanAndVerify2();

	// For each randomly selected new item to be deleted, delete it from the DeltaTree2 and from uniqueItems
	printf("Deleting some items\n");
	for (auto p : toDelete) {
		uniqueItems.erase(p);

		DeltaTree<IntIntPair>::Cursor c = r.getCursor();
		ASSERT(c.seekLessThanOrEqual(p));
		c.erase();

		ASSERT(cur2.seekLessThanOrEqual(p));
		cur2.erase();
	}
	// Update items vector
	items = std::vector<IntIntPair>(uniqueItems.begin(), uniqueItems.end());

	printItems();
	printTrees();

	// Verify tree contents after deletions
	scanAndVerify();
	scanAndVerify2();

	printf("Verifying insert/erase behavior for existing items\n");
	// Test delete/insert behavior for each item, making no net changes
	for (auto p : items) {
		// Insert existing should fail
		ASSERT(!r.insert(p));
		ASSERT(!cur2.insert(p));

		// Erase existing should succeed
		ASSERT(r.erase(p));
		ASSERT(cur2.erase(p));

		// Erase deleted should fail
		ASSERT(!r.erase(p));
		ASSERT(!cur2.erase(p));

		// Insert deleted should succeed
		ASSERT(r.insert(p));
		ASSERT(cur2.insert(p));

		// Insert existing should fail
		ASSERT(!r.insert(p));
		ASSERT(!cur2.insert(p));
	}

	printItems();
	printTrees();

	// Tree contents should still match items vector
	scanAndVerify();
	scanAndVerify2();

	printf("Verifying seek behaviors\n");
	DeltaTree<IntIntPair>::Cursor s = r.getCursor();
	DeltaTree2<IntIntPair>::Cursor s2(&cache, tree2);

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

		ASSERT(s2.seekLessThanOrEqual(q));
		if (s2.get() != p) {
			printItems();
			printf("seekLessThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s2.get().toString().c_str(),
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

		ASSERT(s2.seekGreaterThanOrEqual(q));
		if (s2.get() != p) {
			printItems();
			printf("seekGreaterThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s2.get().toString().c_str(),
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

		ASSERT(s2.seekLessThanOrEqual(q));
		if (s2.get() != p) {
			printItems();
			printf("seekLessThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s2.get().toString().c_str(),
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

		ASSERT(s2.seekGreaterThanOrEqual(q));
		if (s2.get() != p) {
			printItems();
			printf("seekGreaterThanOrEqual(%s) found %s expected %s\n",
			       q.toString().c_str(),
			       s2.get().toString().c_str(),
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
		fmt::print("Seek/skip test, count={0} jumpMax={1}, items={2}, oldSeek={3} useHint={4}:  Elapsed {5} seconds "
		           "{6:.2f} M/s\n",
		           count,
		           jumpMax,
		           items.size(),
		           old,
		           useHint,
		           elapsed,
		           double(count) / elapsed / 1e6);
	};

	auto skipSeekPerformance2 = [&](int jumpMax, bool old, bool useHint, int count) {
		// Skip to a series of increasing items, jump by up to jumpMax units forward in the
		// items, wrapping around to 0.
		double start = timer();
		s2.moveFirst();
		auto first = s2;
		int pos = 0;
		for (int c = 0; c < count; ++c) {
			int jump = deterministicRandom()->randomInt(0, jumpMax);
			int newPos = pos + jump;
			if (newPos >= items.size()) {
				pos = 0;
				newPos = jump;
				s2 = first;
			}
			IntIntPair q = items[newPos];
			++q.v;
			if (old) {
				if (useHint) {
					// s.seekLessThanOrEqualOld(q, 0, &s, newPos - pos);
				} else {
					// s.seekLessThanOrEqualOld(q, 0, nullptr, 0);
				}
			} else {
				if (useHint) {
					// s.seekLessThanOrEqual(q, 0, &s, newPos - pos);
				} else {
					s2.seekLessThanOrEqual(q);
				}
			}
			pos = newPos;
		}
		double elapsed = timer() - start;
		fmt::print("DeltaTree2 Seek/skip test, count={0} jumpMax={1}, items={2}, oldSeek={3} useHint={4}:  Elapsed {5} "
		           "seconds  "
		           "{6:.2f} M/s\n",
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
	skipSeekPerformance(8, false, false, 80e6);
	skipSeekPerformance2(8, false, false, 80e6);
	skipSeekPerformance(8, true, false, 80e6);
	skipSeekPerformance(8, true, true, 80e6);
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
	SimpleCounter() : x(0), t(timer()), start(t), xt(0) {}
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

	fmt::print("Inserting {} elements and then finding each string...\n", count);
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

TEST_CASE("Lredwood/correctness/btree") {
	g_redwoodMetricsActor = Void(); // Prevent trace event metrics from starting
	g_redwoodMetrics.clear();

	state std::string file = params.get("file").orDefault("unittest_pageFile.redwood-v1");
	IPager2* pager;

	state bool serialTest = params.getInt("serialTest").orDefault(deterministicRandom()->random01() < 0.25);
	state bool shortTest = params.getInt("shortTest").orDefault(deterministicRandom()->random01() < 0.25);

	state int pageSize =
	    shortTest ? 200 : (deterministicRandom()->coinflip() ? 4096 : deterministicRandom()->randomInt(200, 400));
	state int extentSize =
	    params.getInt("extentSize")
	        .orDefault(deterministicRandom()->coinflip() ? SERVER_KNOBS->REDWOOD_DEFAULT_EXTENT_SIZE
	                                                     : deterministicRandom()->randomInt(4096, 32768));
	state bool pagerMemoryOnly =
	    params.getInt("pagerMemoryOnly").orDefault(shortTest && (deterministicRandom()->random01() < .001));
	state int maxKeySize = params.getInt("maxKeySize").orDefault(deterministicRandom()->randomInt(1, pageSize * 2));
	state int maxValueSize = params.getInt("maxValueSize").orDefault(randomSize(pageSize * 25));
	state int maxCommitSize =
	    params.getInt("maxCommitSize")
	        .orDefault(shortTest ? 1000 : randomSize(std::min<int>((maxKeySize + maxValueSize) * 20000, 10e6)));
	state double setExistingKeyProbability =
	    params.getDouble("setExistingKeyProbability").orDefault(deterministicRandom()->random01() * .5);
	state double clearProbability =
	    params.getDouble("clearProbability").orDefault(deterministicRandom()->random01() * .1);
	state double clearExistingBoundaryProbability =
	    params.getDouble("clearProbability").orDefault(deterministicRandom()->random01() * .5);
	state double clearSingleKeyProbability =
	    params.getDouble("clearSingleKeyProbability").orDefault(deterministicRandom()->random01());
	state double clearPostSetProbability =
	    params.getDouble("clearPostSetProbability").orDefault(deterministicRandom()->random01() * .1);
	state double coldStartProbability =
	    params.getDouble("coldStartProbability").orDefault(pagerMemoryOnly ? 0 : (deterministicRandom()->random01()));
	state double advanceOldVersionProbability =
	    params.getDouble("advanceOldVersionProbability").orDefault(deterministicRandom()->random01());
	state int64_t pageCacheBytes =
	    params.getInt("pageCacheBytes")
	        .orDefault(pagerMemoryOnly ? 2e9
	                                   : (pageSize * deterministicRandom()->randomInt(1, (BUGGIFY ? 10 : 10000) + 1)));
	state Version versionIncrement =
	    params.getInt("versionIncrement").orDefault(deterministicRandom()->randomInt64(1, 1e8));
	state int64_t remapCleanupWindowBytes =
	    params.getInt("remapCleanupWindowBytes")
	        .orDefault(BUGGIFY ? 0 : deterministicRandom()->randomInt64(1, 100) * 1024 * 1024);
	state int concurrentExtentReads =
	    params.getInt("concurrentExtentReads").orDefault(SERVER_KNOBS->REDWOOD_EXTENT_CONCURRENT_READS);

	// These settings are an attempt to keep the test execution real reasonably short
	state int64_t maxPageOps = params.getInt("maxPageOps").orDefault((shortTest || serialTest) ? 50e3 : 1e6);
	state int maxVerificationMapEntries =
	    params.getInt("maxVerificationMapEntries").orDefault((1.0 - coldStartProbability) * 300e3);
	// Max number of records in the BTree or the versioned written map to visit
	state int64_t maxRecordsRead = 300e6;

	printf("\n");
	printf("file: %s\n", file.c_str());
	printf("maxPageOps: %" PRId64 "\n", maxPageOps);
	printf("maxVerificationMapEntries: %d\n", maxVerificationMapEntries);
	printf("maxRecordsRead: %" PRId64 "\n", maxRecordsRead);
	printf("pagerMemoryOnly: %d\n", pagerMemoryOnly);
	printf("serialTest: %d\n", serialTest);
	printf("shortTest: %d\n", shortTest);
	printf("pageSize: %d\n", pageSize);
	printf("extentSize: %d\n", extentSize);
	printf("maxKeySize: %d\n", maxKeySize);
	printf("maxValueSize: %d\n", maxValueSize);
	printf("maxCommitSize: %d\n", maxCommitSize);
	printf("setExistingKeyProbability: %f\n", setExistingKeyProbability);
	printf("clearProbability: %f\n", clearProbability);
	printf("clearExistingBoundaryProbability: %f\n", clearExistingBoundaryProbability);
	printf("clearSingleKeyProbability: %f\n", clearSingleKeyProbability);
	printf("clearPostSetProbability: %f\n", clearPostSetProbability);
	printf("coldStartProbability: %f\n", coldStartProbability);
	printf("advanceOldVersionProbability: %f\n", advanceOldVersionProbability);
	printf("pageCacheBytes: %s\n", pageCacheBytes == 0 ? "default" : format("%" PRId64, pageCacheBytes).c_str());
	printf("versionIncrement: %" PRId64 "\n", versionIncrement);
	printf("remapCleanupWindowBytes: %" PRId64 "\n", remapCleanupWindowBytes);
	printf("\n");

	printf("Deleting existing test data...\n");
	deleteFile(file);

	printf("Initializing...\n");
	pager = new DWALPager(
	    pageSize, extentSize, file, pageCacheBytes, remapCleanupWindowBytes, concurrentExtentReads, pagerMemoryOnly);
	state VersionedBTree* btree = new VersionedBTree(pager, file);
	wait(btree->init());

	state std::map<std::pair<std::string, Version>, Optional<std::string>> written;
	state int64_t totalRecordsRead = 0;
	state std::set<Key> keys;

	state Version lastVer = btree->getLastCommittedVersion();
	printf("Starting from version: %" PRId64 "\n", lastVer);

	state Version version = lastVer + 1;

	state SimpleCounter mutationBytes;
	state SimpleCounter keyBytesInserted;
	state SimpleCounter valueBytesInserted;
	state SimpleCounter sets;
	state SimpleCounter rangeClears;
	state SimpleCounter keyBytesCleared;
	state int mutationBytesThisCommit = 0;
	state int mutationBytesTargetThisCommit = randomSize(maxCommitSize);

	state PromiseStream<Version> committedVersions;
	state Future<Void> verifyTask =
	    verify(btree, committedVersions.getFuture(), &written, &totalRecordsRead, serialTest);
	state Future<Void> randomTask = serialTest ? Void() : (randomReader(btree, &totalRecordsRead) || btree->getError());
	committedVersions.send(lastVer);

	// Sometimes do zero-change commit at last version
	if (deterministicRandom()->coinflip()) {
		wait(btree->commit(lastVer));
	}

	state Future<Void> commit = Void();
	state int64_t totalPageOps = 0;

	// Check test op limits
	state std::function<bool()> testFinished = [=]() {
		return !(totalPageOps < maxPageOps && written.size() < maxVerificationMapEntries &&
		         totalRecordsRead < maxRecordsRead);
	};

	while (!testFinished()) {
		// Sometimes increment the version
		if (deterministicRandom()->random01() < 0.10) {
			++version;
		}

		// Sometimes do a clear range
		if (deterministicRandom()->random01() < clearProbability) {
			Key start = randomKV(maxKeySize, 1).key;
			Key end = (deterministicRandom()->random01() < .01) ? keyAfter(start) : randomKV(maxKeySize, 1).key;

			// Sometimes replace start and/or end with a close actual (previously used) value
			if (deterministicRandom()->random01() < clearExistingBoundaryProbability) {
				auto i = keys.upper_bound(start);
				if (i != keys.end())
					start = *i;
			}
			if (deterministicRandom()->random01() < clearExistingBoundaryProbability) {
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
			if (deterministicRandom()->random01() < setExistingKeyProbability) {
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
		if (mutationBytesThisCommit >= mutationBytesTargetThisCommit || testFinished()) {
			// Wait for previous commit to finish
			wait(commit);
			printf("Commit complete.  Next commit %d bytes, %" PRId64 " bytes committed so far.",
			       mutationBytesThisCommit,
			       mutationBytes.get() - mutationBytesThisCommit);
			printf("  Stats:  Insert %.2f MB/s  ClearedKeys %.2f MB/s  Total %.2f\n",
			       (keyBytesInserted.rate() + valueBytesInserted.rate()) / 1e6,
			       keyBytesCleared.rate() / 1e6,
			       mutationBytes.rate() / 1e6);

			// Sometimes advance the oldest version to close the gap between the oldest and latest versions by a random
			// amount.
			if (deterministicRandom()->random01() < advanceOldVersionProbability) {
				btree->setOldestReadableVersion(
				    btree->getLastCommittedVersion() -
				    deterministicRandom()->randomInt64(
				        0, btree->getLastCommittedVersion() - btree->getOldestReadableVersion() + 1));
			}

			commit = map(btree->commit(version), [&, v = version](Void) {
				// Update pager ops before clearing metrics
				totalPageOps += g_redwoodMetrics.pageOps();
				fmt::print("Committed {0} PageOps {1}/{2} ({3:.2f}%)  VerificationMapEntries {4}/{5} ({6:.2f}%)  "
				           "RecordsRead {7}/{8} ({9:.2f}%)\n",
				           toString(v).c_str(),
				           totalPageOps,
				           maxPageOps,
				           totalPageOps * 100.0 / maxPageOps,
				           written.size(),
				           maxVerificationMapEntries,
				           written.size() * 100.0 / maxVerificationMapEntries,
				           totalRecordsRead,
				           maxRecordsRead,
				           totalRecordsRead * 100.0 / maxRecordsRead);
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
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &totalRecordsRead, serialTest);
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
				IPager2* pager = new DWALPager(
				    pageSize, extentSize, file, pageCacheBytes, remapCleanupWindowBytes, concurrentExtentReads);
				btree = new VersionedBTree(pager, file);
				wait(btree->init());

				Version v = btree->getLastCommittedVersion();
				printf("Recovered from disk.  Latest recovered version %" PRId64 " highest written version %" PRId64
				       "\n",
				       v,
				       version);
				ASSERT(v == version);

				// Sometimes do zero-change commit at last version
				if (deterministicRandom()->coinflip()) {
					wait(btree->commit(version));
				}

				// Create new promise stream and start the verifier again
				committedVersions = PromiseStream<Version>();
				verifyTask = verify(btree, committedVersions.getFuture(), &written, &totalRecordsRead, serialTest);
				if (!serialTest) {
					randomTask = randomReader(btree, &totalRecordsRead) || btree->getError();
				}
				committedVersions.send(version);
			}

			version += versionIncrement;
		}
	}

	debug_printf("Waiting for outstanding commit\n");
	wait(commit);
	committedVersions.sendError(end_of_stream());
	randomTask.cancel();
	debug_printf("Waiting for verification to complete.\n");
	wait(verifyTask);

	// Reopen pager and btree with a remap cleanup window of 0 to reclaim all old pages
	state Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);
	// If buggify, test starting with empty remap cleanup window.
	btree = new VersionedBTree(
	    new DWALPager(
	        pageSize, extentSize, file, pageCacheBytes, (BUGGIFY ? 0 : remapCleanupWindowBytes), concurrentExtentReads),
	    file);
	wait(btree->init());

	wait(btree->clearAllAndCheckSanity());

	closedFuture = btree->onClosed();
	btree->close();
	debug_printf("Closing.\n");
	wait(closedFuture);

	wait(delay(0));
	ASSERT(DWALPager::PageCacheT::Evictor::getEvictor()->empty());

	return Void();
}

ACTOR Future<Void> randomSeeks(VersionedBTree* btree, int count, char firstChar, char lastChar) {
	state Version readVer = btree->getLastCommittedVersion();
	state int c = 0;
	state double readStart = timer();
	state VersionedBTree::BTreeCursor cur;
	wait(btree->initBTreeCursor(&cur, readVer, PagerEventReasons::PointRead));
	while (c < count) {
		state Key k = randomString(20, firstChar, lastChar);
		wait(cur.seekGTE(k));
		++c;
	}
	double elapsed = timer() - readStart;
	printf("Random seek speed %d/s\n", int(count / elapsed));
	return Void();
}

ACTOR Future<Void> randomScans(VersionedBTree* btree,
                               int count,
                               int width,
                               int prefetchBytes,
                               char firstChar,
                               char lastChar) {
	state Version readVer = btree->getLastCommittedVersion();
	state int c = 0;
	state double readStart = timer();
	state VersionedBTree::BTreeCursor cur;
	wait(btree->initBTreeCursor(&cur, readVer, PagerEventReasons::RangeRead));

	state int totalScanBytes = 0;
	while (c++ < count) {
		state Key k = randomString(20, firstChar, lastChar);
		wait(cur.seekGTE(k));
		state int w = width;
		state bool directionFwd = deterministicRandom()->coinflip();

		if (prefetchBytes > 0) {
			cur.prefetch(directionFwd ? VersionedBTree::dbEnd.key : VersionedBTree::dbBegin.key,
			             directionFwd,
			             width,
			             prefetchBytes);
		}

		while (w > 0 && cur.isValid()) {
			totalScanBytes += cur.get().expectedSize();
			wait(success(directionFwd ? cur.moveNext() : cur.movePrev()));
			--w;
		}
	}
	double elapsed = timer() - readStart;
	fmt::print(
	    "Completed {0} scans: width={1} totalbytesRead={2} prefetchBytes={3} scansRate={4} scans/s  {5:.2f} MB/s\n",
	    count,
	    width,
	    totalScanBytes,
	    prefetchBytes,
	    int(count / elapsed),
	    double(totalScanBytes) / 1e6 / elapsed);
	return Void();
}

TEST_CASE(":/redwood/correctness/pager/cow") {
	state std::string pagerFile = "unittest_pageFile.redwood-v1";
	printf("Deleting old test data\n");
	deleteFile(pagerFile);

	int pageSize = 4096;
	int extentSize = SERVER_KNOBS->REDWOOD_DEFAULT_EXTENT_SIZE;
	state IPager2* pager =
	    new DWALPager(pageSize, extentSize, pagerFile, 0, 0, SERVER_KNOBS->REDWOOD_EXTENT_CONCURRENT_READS);

	wait(success(pager->init()));
	state LogicalPageID id = wait(pager->newPageID());
	state VectorRef<LogicalPageID> pageID(&id, 1);
	Reference<ArenaPage> p = pager->newPageBuffer();
	memset(p->mutate(), (char)id, p->size());
	pager->updatePage(PagerEventReasons::MetaData, nonBtreeLevel, pageID, p);
	pager->setMetaKey(LiteralStringRef("asdfasdf"));
	wait(pager->commit(pager->getLastCommittedVersion() + 1));
	Reference<ArenaPage> p2 =
	    wait(pager->readPage(PagerEventReasons::PointRead, nonBtreeLevel, id, ioMinPriority, true, false));
	printf("%s\n", StringRef(p2->begin(), p2->size()).toHexString().c_str());

	// TODO: Verify reads, do more writes and reads to make this a real pager validator

	Future<Void> onClosed = pager->onClosed();
	pager->close();
	wait(onClosed);

	return Void();
}

template <int size>
struct ExtentQueueEntry {
	uint8_t entry[size];

	bool operator<(const ExtentQueueEntry& rhs) const { return entry < rhs.entry; }

	std::string toString() const {
		return format("{%s}", StringRef((const uint8_t*)entry, size).toHexString().c_str());
	}
};

typedef FIFOQueue<ExtentQueueEntry<16>> ExtentQueueT;
TEST_CASE(":/redwood/performance/extentQueue") {
	state ExtentQueueT m_extentQueue;
	state ExtentQueueT::QueueState extentQueueState;

	state DWALPager* pager;
	// If a test file is passed in by environment then don't write new data to it.
	state bool reload = getenv("TESTFILE") == nullptr;
	state std::string fileName = reload ? "unittest.redwood-v1" : getenv("TESTFILE");

	if (reload) {
		printf("Deleting old test data\n");
		deleteFile(fileName);
	}

	printf("Filename: %s\n", fileName.c_str());
	state int pageSize = params.getInt("pageSize").orDefault(SERVER_KNOBS->REDWOOD_DEFAULT_PAGE_SIZE);
	state int extentSize = params.getInt("extentSize").orDefault(SERVER_KNOBS->REDWOOD_DEFAULT_EXTENT_SIZE);
	state int64_t cacheSizeBytes = params.getInt("cacheSizeBytes").orDefault(FLOW_KNOBS->PAGE_CACHE_4K);
	// Choose a large remapCleanupWindowBytes to avoid popping the queue
	state int64_t remapCleanupWindowBytes = params.getInt("remapCleanupWindowBytes").orDefault(1e16);
	state int numEntries = params.getInt("numEntries").orDefault(10e6);
	state int concurrentExtentReads =
	    params.getInt("concurrentExtentReads").orDefault(SERVER_KNOBS->REDWOOD_EXTENT_CONCURRENT_READS);
	state int targetCommitSize = deterministicRandom()->randomInt(2e6, 30e6);
	state int currentCommitSize = 0;
	state int64_t cumulativeCommitSize = 0;

	printf("pageSize: %d\n", pageSize);
	printf("extentSize: %d\n", extentSize);
	printf("cacheSizeBytes: %" PRId64 "\n", cacheSizeBytes);
	printf("remapCleanupWindowBytes: %" PRId64 "\n", remapCleanupWindowBytes);

	// Do random pushes into the queue and commit periodically
	if (reload) {
		pager = new DWALPager(
		    pageSize, extentSize, fileName, cacheSizeBytes, remapCleanupWindowBytes, concurrentExtentReads);

		wait(success(pager->init()));

		LogicalPageID extID = pager->newLastExtentID();
		m_extentQueue.create(pager, extID, "ExtentQueue", pager->newLastQueueID(), true);
		pager->pushExtentUsedList(m_extentQueue.queueID, extID);

		state int v;
		state ExtentQueueEntry<16> e;
		generateRandomData(e.entry, 16);
		state int sinceYield = 0;
		for (v = 1; v <= numEntries; ++v) {
			// Sometimes do a commit
			if (currentCommitSize >= targetCommitSize) {
				fmt::print("currentCommitSize: {0}, cumulativeCommitSize: {1}, pageCacheCount: {2}\n",
				           currentCommitSize,
				           cumulativeCommitSize,
				           pager->getPageCacheCount());
				wait(m_extentQueue.flush());
				wait(pager->commit(pager->getLastCommittedVersion() + 1));
				cumulativeCommitSize += currentCommitSize;
				targetCommitSize = deterministicRandom()->randomInt(2e6, 30e6);
				currentCommitSize = 0;
			}

			// push a random entry into the queue
			m_extentQueue.pushBack(e);
			currentCommitSize += 16;

			// yield periodically to avoid overflowing the stack
			if (++sinceYield >= 100) {
				sinceYield = 0;
				wait(yield());
			}
		}
		cumulativeCommitSize += currentCommitSize;
		fmt::print(
		    "Final cumulativeCommitSize: {0}, pageCacheCount: {1}\n", cumulativeCommitSize, pager->getPageCacheCount());
		wait(m_extentQueue.flush());
		extentQueueState = m_extentQueue.getState();
		printf("Commit ExtentQueue getState(): %s\n", extentQueueState.toString().c_str());
		pager->setMetaKey(extentQueueState.asKeyRef());
		wait(pager->commit(pager->getLastCommittedVersion() + 1));

		Future<Void> onClosed = pager->onClosed();
		pager->close();
		wait(onClosed);
	}

	printf("Reopening pager file from disk.\n");
	pager =
	    new DWALPager(pageSize, extentSize, fileName, cacheSizeBytes, remapCleanupWindowBytes, concurrentExtentReads);
	wait(success(pager->init()));

	printf("Starting ExtentQueue FastPath Recovery from Disk.\n");

	// reopen the pager from disk
	state Key meta = pager->getMetaKey();
	memcpy(&extentQueueState, meta.begin(), meta.size());
	extentQueueState.fromKeyRef(meta);
	printf("Recovered ExtentQueue getState(): %s\n", extentQueueState.toString().c_str());
	m_extentQueue.recover(pager, extentQueueState, "ExtentQueueRecovered");

	state double intervalStart = timer();
	state double start = intervalStart;
	state Standalone<VectorRef<LogicalPageID>> extentIDs = wait(pager->getUsedExtents(m_extentQueue.queueID));

	// fire read requests for all used extents
	state int i;
	for (i = 1; i < extentIDs.size() - 1; i++) {
		LogicalPageID extID = extentIDs[i];
		pager->readExtent(extID);
	}

	state PromiseStream<Standalone<VectorRef<ExtentQueueEntry<16>>>> resultStream;
	state Future<Void> queueRecoverActor;
	queueRecoverActor = m_extentQueue.peekAllExt(resultStream);
	state int entriesRead = 0;
	try {
		loop choose {
			when(Standalone<VectorRef<ExtentQueueEntry<16>>> entries = waitNext(resultStream.getFuture())) {
				entriesRead += entries.size();
				if (entriesRead == m_extentQueue.numEntries)
					break;
			}
			when(wait(queueRecoverActor)) { queueRecoverActor = Never(); }
		}
	} catch (Error& e) {
		if (e.code() != error_code_end_of_stream) {
			throw;
		}
	}

	state double elapsed = timer() - start;
	printf("Completed fastpath extent queue recovery: elapsed=%f entriesRead=%d recoveryRate=%f MB/s\n",
	       elapsed,
	       entriesRead,
	       cumulativeCommitSize / elapsed / 1e6);

	fmt::print("pageCacheCount: {0} extentCacheCount: {1}\n", pager->getPageCacheCount(), pager->getExtentCacheCount());

	pager->extentCacheClear();
	m_extentQueue.resetHeadReader();

	printf("Starting ExtentQueue SlowPath Recovery from Disk.\n");
	intervalStart = timer();
	start = intervalStart;
	// peekAll the queue using regular slow path
	Standalone<VectorRef<ExtentQueueEntry<16>>> entries = wait(m_extentQueue.peekAll());

	elapsed = timer() - start;
	printf("Completed slowpath extent queue recovery: elapsed=%f entriesRead=%d recoveryRate=%f MB/s\n",
	       elapsed,
	       entries.size(),
	       cumulativeCommitSize / elapsed / 1e6);

	return Void();
}

TEST_CASE(":/redwood/performance/set") {
	state SignalableActorCollection actors;

	state std::string file = params.get("file").orDefault("unittest.redwood-v1");
	state int pageSize = params.getInt("pageSize").orDefault(SERVER_KNOBS->REDWOOD_DEFAULT_PAGE_SIZE);
	state int extentSize = params.getInt("extentSize").orDefault(SERVER_KNOBS->REDWOOD_DEFAULT_EXTENT_SIZE);
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
	state int64_t remapCleanupWindowBytes = params.getInt("remapCleanupWindowBytes").orDefault(100LL * 1024 * 1024);
	state int concurrentExtentReads =
	    params.getInt("concurrentExtentReads").orDefault(SERVER_KNOBS->REDWOOD_EXTENT_CONCURRENT_READS);
	state bool openExisting = params.getInt("openExisting").orDefault(0);
	state bool insertRecords = !openExisting || params.getInt("insertRecords").orDefault(0);
	state int concurrentSeeks = params.getInt("concurrentSeeks").orDefault(64);
	state int concurrentScans = params.getInt("concurrentScans").orDefault(64);
	state int seeks = params.getInt("seeks").orDefault(1000000);
	state int scans = params.getInt("scans").orDefault(20000);
	state int scanWidth = params.getInt("scanWidth").orDefault(50);
	state int scanPrefetchBytes = params.getInt("scanPrefetchBytes").orDefault(0);
	state bool pagerMemoryOnly = params.getInt("pagerMemoryOnly").orDefault(0);
	state bool traceMetrics = params.getInt("traceMetrics").orDefault(0);
	state bool destructiveSanityCheck = params.getInt("destructiveSanityCheck").orDefault(0);

	printf("file: %s\n", file.c_str());
	printf("openExisting: %d\n", openExisting);
	printf("insertRecords: %d\n", insertRecords);
	printf("destructiveSanityCheck: %d\n", destructiveSanityCheck);
	printf("pagerMemoryOnly: %d\n", pagerMemoryOnly);
	printf("pageSize: %d\n", pageSize);
	printf("extentSize: %d\n", extentSize);
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
	printf("remapCleanupWindowBytes: %" PRId64 "\n", remapCleanupWindowBytes);
	printf("concurrentScans: %d\n", concurrentScans);
	printf("concurrentSeeks: %d\n", concurrentSeeks);
	printf("seeks: %d\n", seeks);
	printf("scans: %d\n", scans);
	printf("scanWidth: %d\n", scanWidth);
	printf("scanPrefetchBytes: %d\n", scanPrefetchBytes);

	// If using stdout for metrics, prevent trace event metrics logger from starting
	if (!traceMetrics) {
		g_redwoodMetricsActor = Void();
		g_redwoodMetrics.clear();
	}

	if (!openExisting) {
		printf("Deleting old test data\n");
		deleteFile(file);
	}

	DWALPager* pager = new DWALPager(
	    pageSize, extentSize, file, pageCacheBytes, remapCleanupWindowBytes, concurrentExtentReads, pagerMemoryOnly);
	state VersionedBTree* btree = new VersionedBTree(pager, file);
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
	state int sinceYield = 0;
	state Version version = btree->getLastCommittedVersion();

	if (insertRecords) {
		while (kvBytesTotal < kvBytesTarget) {
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

				if (++sinceYield >= 100) {
					sinceYield = 0;
					wait(yield());
				}
			}

			if (kvBytesThisCommit >= maxKVBytesPerCommit || recordsThisCommit >= maxRecordsPerCommit) {
				btree->setOldestReadableVersion(btree->getLastCommittedVersion());
				wait(commit);
				TraceEvent e("RedwoodState");
				btree->toTraceEvent(e);
				e.log();

				printf("Cumulative %.2f MB keyValue bytes written at %.2f MB/s\n",
				       kvBytesTotal / 1e6,
				       kvBytesTotal / (timer() - start) / 1e6);

				// Avoid capturing via this to freeze counter values
				int recs = recordsThisCommit;
				int kvb = kvBytesThisCommit;

				// Capturing invervalStart via this->intervalStart makes IDE's unhappy as they do not know about the
				// actor state object
				double* pIntervalStart = &intervalStart;

				commit = map(btree->commit(++version), [=](Void result) {
					if (!traceMetrics) {
						printf("%s\n", g_redwoodMetrics.toString(true).c_str());
					}
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

	state Future<Void> stats =
	    traceMetrics ? Void()
	                 : repeatEvery(1.0, [&]() { printf("Stats:\n%s\n", g_redwoodMetrics.toString(true).c_str()); });

	if (scans > 0) {
		printf("Parallel scans, concurrency=%d, scans=%d, scanWidth=%d, scanPreftchBytes=%d ...\n",
		       concurrentScans,
		       scans,
		       scanWidth,
		       scanPrefetchBytes);
		for (int x = 0; x < concurrentScans; ++x) {
			actors.add(
			    randomScans(btree, scans / concurrentScans, scanWidth, scanPrefetchBytes, firstKeyChar, lastKeyChar));
		}
		wait(actors.signalAndReset());
	}

	if (seeks > 0) {
		printf("Parallel seeks, concurrency=%d, seeks=%d ...\n", concurrentSeeks, seeks);
		for (int x = 0; x < concurrentSeeks; ++x) {
			actors.add(randomSeeks(btree, seeks / concurrentSeeks, firstKeyChar, lastKeyChar));
		}
		wait(actors.signalAndReset());
	}

	stats.cancel();

	if (destructiveSanityCheck) {
		wait(btree->clearAllAndCheckSanity());
	}

	Future<Void> closedFuture = btree->onClosed();
	btree->close();
	wait(closedFuture);

	wait(delay(0));
	ASSERT(DWALPager::PageCacheT::Evictor::getEvictor()->empty());

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

	fmt::print("\nstoreType: {}\n", static_cast<int>(kvs->getType()));
	fmt::print("commitTarget: {}\n", commitTarget);
	fmt::print("prefixSource: {}\n", source.toString());
	fmt::print("usePrefixesInOrder: {}\n", usePrefixesInOrder);
	fmt::print("suffixSize: {}\n", suffixSize);
	fmt::print("valueSize: {}\n", valueSize);
	fmt::print("recordSize: {}\n", recordSize);
	fmt::print("recordsPerPrefix: {}\n", recordsPerPrefix);
	fmt::print("recordCountTarget: {}\n", recordCountTarget);
	fmt::print("kvBytesTarget: {}\n", kvBytesTarget);

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

	fmt::print("\nstoreType: {}\n", static_cast<int>(kvs->getType()));
	fmt::print("commitTarget: {}\n", commitTarget);
	fmt::print("valueSize: {}\n", valueSize);
	fmt::print("recordSize: {}\n", recordSize);
	fmt::print("recordCountTarget: {}\n", recordCountTarget);
	fmt::print("kvBytesTarget: {}\n", kvBytesTarget);

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

	deleteFile("test.redwood-v1");
	wait(delay(5));
	state IKeyValueStore* redwood = openKVStore(KeyValueStoreType::SSD_REDWOOD_V1, "test.redwood-v1", UID(), 0);
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

	deleteFile("test.redwood-v1");
	wait(delay(5));
	state IKeyValueStore* redwood = openKVStore(KeyValueStoreType::SSD_REDWOOD_V1, "test.redwood-v1", UID(), 0);
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
	fmt::print("\nstoreType: {}\n", static_cast<int>(kvs->getType()));
	fmt::print("prefixSource: {}\n", source.toString());
	fmt::print("suffixSize: {}\n", suffixSize);
	fmt::print("recordCountTarget: {}\n", recordCountTarget);
	fmt::print("singlePrefix: {}\n", singlePrefix);
	fmt::print("rowLimit: {}\n", rowLimit);

	state int64_t recordSize = source.prefixLen + suffixSize + valueSize;
	state int64_t bytesRead = 0;
	state int64_t recordsRead = 0;
	state int queries = 0;
	state int64_t nextPrintRecords = 1e5;

	state double start = timer();
	state std::function<void()> stats = [&]() {
		double elapsed = timer() - start;
		fmt::print("Cumulative stats: {0:.2f} seconds  {1} queries {2:.2f} MB {3} records  {4:.2f} qps {5:.2f} MB/s  "
		           "{6:.2f} rec/s\r\n",
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

TEST_CASE(":/redwood/performance/randomRangeScans") {
	state int prefixLen = 30;
	state int suffixSize = 12;
	state int valueSize = 100;

	// TODO change to 100e8 after figuring out no-disk redwood mode
	state int writeRecordCountTarget = 1e6;
	state int queryRecordTarget = 1e7;
	state int writePrefixesInOrder = false;

	state KVSource source({ { prefixLen, 1000 } });

	deleteFile("test.redwood-v1");
	wait(delay(5));
	state IKeyValueStore* redwood = openKVStore(KeyValueStoreType::SSD_REDWOOD_V1, "test.redwood-v1", UID(), 0);
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

TEST_CASE(":/redwood/performance/histogramThroughput") {
	std::default_random_engine generator;
	std::uniform_int_distribution<uint32_t> distribution(0, UINT32_MAX);
	state size_t inputSize = pow(10, 8);
	state std::vector<uint32_t> uniform;
	for (int i = 0; i < inputSize; i++) {
		uniform.push_back(distribution(generator));
	}
	std::cout << "size of input: " << uniform.size() << std::endl;
	{
		// Time needed to log 33 histograms.
		std::vector<Reference<Histogram>> histograms;
		for (int i = 0; i < 33; i++) {
			std::string levelString = "L" + std::to_string(i);
			histograms.push_back(Histogram::getHistogram(
			    LiteralStringRef("histogramTest"), LiteralStringRef("levelString"), Histogram::Unit::bytes));
		}
		for (int i = 0; i < 33; i++) {
			for (int j = 0; j < 32; j++) {
				histograms[i]->sample(std::pow(2, j));
			}
		}
		auto t_start = std::chrono::high_resolution_clock::now();
		for (int i = 0; i < 33; i++) {
			histograms[i]->writeToLog(30.0);
		}
		auto t_end = std::chrono::high_resolution_clock::now();
		double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
		std::cout << "Time needed to log 33 histograms (millisecond): " << elapsed_time_ms << std::endl;
	}
	{
		std::cout << "Histogram Unit bytes" << std::endl;
		auto t_start = std::chrono::high_resolution_clock::now();
		Reference<Histogram> h = Histogram::getHistogram(
		    LiteralStringRef("histogramTest"), LiteralStringRef("counts"), Histogram::Unit::bytes);
		ASSERT(uniform.size() == inputSize);
		for (size_t i = 0; i < uniform.size(); i++) {
			h->sample(uniform[i]);
		}
		auto t_end = std::chrono::high_resolution_clock::now();
		std::cout << h->drawHistogram();
		double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
		std::cout << "Time in millisecond: " << elapsed_time_ms << std::endl;

		Reference<Histogram> hCopy = Histogram::getHistogram(
		    LiteralStringRef("histogramTest"), LiteralStringRef("counts"), Histogram::Unit::bytes);
		std::cout << hCopy->drawHistogram();
		GetHistogramRegistry().logReport();
	}
	{
		std::cout << "Histogram Unit percentage: " << std::endl;
		auto t_start = std::chrono::high_resolution_clock::now();
		Reference<Histogram> h = Histogram::getHistogram(
		    LiteralStringRef("histogramTest"), LiteralStringRef("counts"), Histogram::Unit::percentageLinear);
		ASSERT(uniform.size() == inputSize);
		for (size_t i = 0; i < uniform.size(); i++) {
			h->samplePercentage((double)uniform[i] / UINT32_MAX);
		}
		auto t_end = std::chrono::high_resolution_clock::now();
		std::cout << h->drawHistogram();
		GetHistogramRegistry().logReport();
		double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
		std::cout << "Time in millisecond: " << elapsed_time_ms << std::endl;
	}
	return Void();
}
TEST_CASE(":/redwood/performance/continuousSmapleThroughput") {
	std::default_random_engine generator;
	std::uniform_int_distribution<uint32_t> distribution(0, UINT32_MAX);
	state size_t inputSize = pow(10, 8);
	state std::vector<uint32_t> uniform;
	for (int i = 0; i < inputSize; i++) {
		uniform.push_back(distribution(generator));
	}

	{
		ContinuousSample<uint32_t> s = ContinuousSample<uint32_t>(pow(10, 3));
		auto t_start = std::chrono::high_resolution_clock::now();
		ASSERT(uniform.size() == inputSize);
		for (size_t i = 0; i < uniform.size(); i++) {
			s.addSample(uniform[i]);
		}
		auto t_end = std::chrono::high_resolution_clock::now();
		double elapsed_time_ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
		std::cout << "size of input: " << uniform.size() << std::endl;
		std::cout << "Time in millisecond: " << elapsed_time_ms << std::endl;
	}
	return Void();
}
